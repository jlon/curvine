// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_fs_api::{Path, RpcCode};
use curvine_model::{MkdirOpts, ProtoUtils};
use curvine_proto::{GetFileStatusRequest, GetFilesystemInfoRequest, MkdirRequest};
use curvine_raft::proto::raft::RaftRequest;
use curvine_raft::raft::RaftCode;
use curvine_rpc::client::RpcClient;
use curvine_rpc::message::Builder;
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use raft::eraftpb::{Message as RaftMessage, MessageType};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn start_cluster() -> Arc<MiniCluster> {
    let test_id = Utils::rand_str(8);
    let base_dir = Utils::test_sub_dir(format!("leader-transition-rpc-{test_id}"));
    let mut conf = ClusterConf::default();
    conf.master.meta_dir = format!("{base_dir}/meta");
    conf.journal.journal_dir = format!("{base_dir}/journal");
    conf.journal.raft_tick_interval_ms = 50;
    conf.journal.raft_election_tick = 5;
    conf.journal.raft_min_election_ticks = 5;
    conf.journal.raft_max_election_ticks = 6;
    conf.client.short_circuit = false;

    let cluster = Arc::new(MiniCluster::with_num(&conf, 3, 0));
    cluster.start_cluster();
    cluster
}

async fn request_master_info(conf: &ClusterConf) -> CommonResult<Result<(), FsError>> {
    let client = RpcClient::with_raw(&conf.master_addr(), &conf.client_rpc_conf()).await?;
    let response = client
        .rpc(
            Builder::new_rpc(RpcCode::GetFilesystemInfo)
                .proto_header(GetFilesystemInfoRequest::default())
                .build(),
        )
        .await?;
    Ok(response.check_error_ext::<FsError>())
}

async fn active_master_index(cluster: &MiniCluster) -> CommonResult<Option<usize>> {
    for (index, conf) in cluster.master_conf.iter().enumerate() {
        match request_master_info(conf).await? {
            Ok(()) => return Ok(Some(index)),
            Err(FsError::NotLeaderMaster(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(None)
}

async fn wait_for_active_master(cluster: &MiniCluster) -> CommonResult<usize> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(index) = active_master_index(cluster).await? {
            return Ok(index);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("no active master after Raft election".into())
}

async fn wait_for_all_master_rpcs(cluster: &MiniCluster) -> CommonResult<()> {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        let mut all_ready = true;
        for conf in &cluster.master_conf {
            match request_master_info(conf).await {
                Ok(Ok(())) | Ok(Err(FsError::NotLeaderMaster(_))) => {}
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => {
                    all_ready = false;
                    break;
                }
            }
        }
        if all_ready {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err("not every master RPC server became ready".into())
}

async fn wait_for_standby_master(cluster: &MiniCluster, index: usize) -> CommonResult<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match request_master_info(&cluster.master_conf[index]).await? {
            Err(FsError::NotLeaderMaster(_)) => return Ok(()),
            Ok(()) => tokio::time::sleep(Duration::from_millis(10)).await,
            Err(error) => return Err(error.into()),
        }
    }
    Err(format!("master {index} did not step down after a higher-term Raft message").into())
}

async fn expect_not_leader<T>(conf: &ClusterConf, code: RpcCode, request: T) -> CommonResult<()>
where
    T: prost::Message + Default,
{
    let client = RpcClient::with_raw(&conf.master_addr(), &conf.client_rpc_conf()).await?;
    let response = client
        .rpc(Builder::new_rpc(code).proto_header(request).build())
        .await?;
    assert!(matches!(
        response.check_error_ext::<FsError>(),
        Err(FsError::NotLeaderMaster(_))
    ));
    Ok(())
}

#[test]
fn multi_master_demotion_fences_stale_rpc_and_recovers() -> CommonResult<()> {
    let cluster = start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async move {
        wait_for_all_master_rpcs(&cluster).await?;
        let fs = cluster.new_fs();
        let root = Path::from_str("/leader-transition")?;
        let before = Path::from_str("/leader-transition/before")?;
        let stale = Path::from_str("/leader-transition/stale")?;
        let after = Path::from_str("/leader-transition/after")?;

        assert!(fs.mkdir(&root, true).await?);
        assert!(fs.mkdir(&before, false).await?);
        let leader = wait_for_active_master(&cluster).await?;
        let leader_conf = &cluster.master_conf[leader];
        let source = if leader == 0 { 2 } else { 1 };

        let journal_client = RpcClient::with_raw(
            &leader_conf.journal.local_addr(),
            &leader_conf.journal.new_client_conf(),
        )
        .await?;
        let mut message = RaftMessage::default();
        message.set_msg_type(MessageType::MsgAppend);
        message.from = source;
        message.to = (leader + 1) as u64;
        message.term = 1_000_000;
        journal_client
            .rpc(
                Builder::new_rpc(RaftCode::Raft)
                    .proto_header(RaftRequest { message })
                    .build(),
            )
            .await?;

        wait_for_standby_master(&cluster, leader).await?;
        expect_not_leader(
            leader_conf,
            RpcCode::FileStatus,
            GetFileStatusRequest {
                path: before.to_string(),
            },
        )
        .await?;
        expect_not_leader(
            leader_conf,
            RpcCode::Mkdir,
            MkdirRequest {
                path: stale.to_string(),
                opts: ProtoUtils::mkdir_opts_to_pb(MkdirOpts::with_create(false)),
            },
        )
        .await?;

        wait_for_active_master(&cluster).await?;
        assert!(fs.mkdir(&after, false).await?);
        assert!(fs.exists(&before).await?);
        assert!(fs.exists(&after).await?);
        assert!(!fs.exists(&stale).await?);
        Ok(())
    })
}
