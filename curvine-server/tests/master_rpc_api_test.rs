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

use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_fs_api::{Path, RpcCode};
use curvine_job_client::JobMasterClient;
use curvine_model::{
    FileAllocOpts, FileLock, HeartbeatStatus, JobTaskProgress, JobTaskState, ListOptions,
    LoadJobCommand, LockFlags, LockType, MountOptions, OpenFlags, ProtoUtils, SetAttrOptsBuilder,
    StorageType,
};
use curvine_proto::{
    BlockReportListRequest, BlockReportListResponse, MetricsReportRequest, MetricsReportResponse,
    ReportBlockReplicationRequest, ReportBlockReplicationResponse, WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
};
use curvine_rpc::client::RpcClient;
use curvine_rpc::message::Builder;
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use std::collections::HashSet;
use std::sync::Arc;

fn path(value: impl AsRef<str>) -> CommonResult<Path> {
    Path::from_str(value.as_ref())
}

fn start_cluster() -> Arc<MiniCluster> {
    let test_id = Utils::rand_str(8);
    let base_dir = Utils::test_sub_dir(format!("master-rpc-api-{test_id}"));
    let mut conf = ClusterConf::default();
    conf.master.meta_dir = format!("{base_dir}/meta");
    conf.journal.journal_dir = format!("{base_dir}/journal");
    conf.worker.data_dir = vec![format!("[MEM:128MB]{base_dir}/worker")];
    conf.journal.raft_tick_interval_ms = 100;
    conf.client.short_circuit = false;
    conf.client.replicas = 1;
    conf.client.block_size_str = "1MB".to_string();
    conf.client.storage_type = StorageType::Mem;
    conf.client.storage_type_str = "mem".to_string();

    let cluster = Arc::new(MiniCluster::with_num(&conf, 1, 1));
    cluster.start_cluster();
    cluster
}

async fn raw_rpc<T, R>(client: &RpcClient, code: RpcCode, request: T) -> CommonResult<R>
where
    T: prost::Message + Default,
    R: prost::Message + Default,
{
    let response = client
        .rpc(Builder::new_rpc(code).proto_header(request).build())
        .await?;
    response.check_error_ext::<FsError>()?;
    response.parse_header()
}

async fn verify_namespace_and_metadata_rpcs(fs: &CurvineFileSystem) -> CommonResult<()> {
    let root = path("/rpc-api")?;
    let data = path("/rpc-api/data")?;
    let renamed = path("/rpc-api/data-renamed")?;
    let hard_link = path("/rpc-api/data-hard-link")?;
    let symbolic_link = path("/rpc-api/data-symbolic-link")?;

    assert!(fs.mkdir(&root, true).await?);
    fs.write_string(&data, "master-rpc-api-e2e").await?;
    assert_eq!(fs.read_string(&data).await?, "master-rpc-api-e2e");
    assert!(fs.exists(&data).await?);

    let client = fs.fs_client();
    let opened = client
        .open_with_opts(
            &data,
            fs.create_opts_builder().create_parent(false).build(),
            OpenFlags::new_read_only(),
        )
        .await?;
    assert_eq!(opened.status.path, data.to_string());

    let status = fs.get_status(&data).await?;
    assert_eq!(status.path, data.to_string());
    assert!(!fs.get_status_bytes(&data).await?.is_empty());
    assert!(fs
        .list_status(&root)
        .await?
        .iter()
        .any(|entry| entry.name == "data"));
    assert_eq!(
        fs.list_options(
            &root,
            ListOptions {
                limit: Some(1),
                start_after: None,
            },
        )
        .await?
        .len(),
        1
    );
    assert!(!fs.get_block_locations(&data).await?.block_locs.is_empty());

    assert!(fs.rename(&data, &renamed).await?);
    fs.link(&renamed, &hard_link).await?;
    fs.symlink(&renamed.to_string(), &symbolic_link, true)
        .await?;
    assert_eq!(
        fs.get_status(&symbolic_link).await?.target.as_deref(),
        Some(renamed.to_string().as_str())
    );

    let attrs = SetAttrOptsBuilder::new()
        .owner("rpc-api")
        .group("rpc-api")
        .add_x_attr("rpc-api", b"enabled".to_vec())
        .build();
    let updated = fs.set_attr(&hard_link, attrs).await?;
    assert_eq!(updated.owner, "rpc-api");
    assert_eq!(updated.x_attr.get("rpc-api"), Some(&b"enabled".to_vec()));

    let read_lock = FileLock {
        client_id: "rpc-api-reader".to_string(),
        owner_id: 1,
        pid: 1,
        lock_type: LockType::ReadLock,
        lock_flags: LockFlags::Plock,
        start: 0,
        end: 1,
        ..Default::default()
    };
    assert!(fs.set_lock(&hard_link, read_lock).await?.is_none());
    let write_lock = FileLock {
        client_id: "rpc-api-writer".to_string(),
        owner_id: 2,
        pid: 2,
        lock_type: LockType::WriteLock,
        lock_flags: LockFlags::Plock,
        start: 0,
        end: 1,
        ..Default::default()
    };
    assert!(fs.get_lock(&hard_link, write_lock).await?.is_some());

    Ok(())
}

async fn verify_block_and_batch_rpcs(fs: &CurvineFileSystem) -> CommonResult<()> {
    let client = fs.fs_client();
    let first = path("/rpc-api/batch/first")?;
    let second = path("/rpc-api/batch/second")?;
    let opts = fs.create_opts_builder().create_parent(true).build();
    let flags = OpenFlags::new_create().set_overwrite(true);

    let created = client
        .create_files_batch(vec![
            (first.to_string(), opts.clone(), flags),
            (second.to_string(), opts, flags),
        ])
        .await?;
    assert_eq!(created.len(), 2);

    let direct = path("/rpc-api/direct")?;
    let direct_status = client
        .create_with_opts(
            &direct,
            fs.create_opts_builder().create_parent(true).build(),
            false,
        )
        .await?;
    assert_eq!(direct_status.path, direct.to_string());
    let direct_block = client.add_block(&direct, vec![], 0, None).await?;
    assert!(!direct_block.locs.is_empty());
    assert!(client
        .complete_file(&direct, 0, vec![], false, None)
        .await?
        .is_none());
    assert!(fs.exists(&direct).await?);

    let write_open = path("/rpc-api/open-write")?;
    let opened = client
        .open_with_opts(
            &write_open,
            fs.create_opts_builder().create_parent(true).build(),
            OpenFlags::new_write_only().set_create(true),
        )
        .await?;
    assert_eq!(opened.status.path, write_open.to_string());
    assert!(client
        .complete_file(&write_open, 0, vec![], false, None)
        .await?
        .is_none());
    assert!(fs.exists(&write_open).await?);

    let blocks = client
        .add_blocks_batch(vec![first.to_string(), second.to_string()])
        .await?;
    assert_eq!(blocks.len(), 2);
    assert!(!blocks[0].locs.is_empty());
    let assigned = client
        .assign_worker(&first, blocks[0].block.clone())
        .await?;
    assert_eq!(assigned.block.id, blocks[0].block.id);

    let client_name = client.context().clone_client_name();
    let completed = client
        .complete_files_batch(vec![
            (first.to_string(), 0, vec![], client_name.clone(), false),
            (second.to_string(), 0, vec![], client_name, false),
        ])
        .await?;
    assert_eq!(completed, vec![true, true]);

    let resized = client
        .resize(&first, FileAllocOpts::with_truncate(0))
        .await?;
    assert_eq!(resized.status.path, first.to_string());
    let free_result = fs.free(&second, false).await?;
    assert_eq!(
        free_result.inodes, 0,
        "free must not delete CV-only metadata"
    );
    assert!(fs.exists(&second).await?);
    fs.delete(&second, false).await?;
    assert!(!fs.exists(&second).await?);

    Ok(())
}

async fn verify_metadata_replica_rpcs(fs: &CurvineFileSystem) -> CommonResult<()> {
    let mut snapshot_paths = HashSet::new();
    let mut page_token = None;
    let mut snapshot_epoch = None;
    loop {
        let page = fs
            .get_cv_metadata_snapshot_page(page_token.take(), Some(1))
            .await?;
        let epoch = snapshot_epoch.get_or_insert(page.epoch);
        assert_eq!(*epoch, page.epoch);
        for entry in page.entries {
            assert!(
                snapshot_paths.insert(entry.status.path.clone()),
                "snapshot page repeated path {}",
                entry.status.path
            );
        }
        let Some(next_page_token) = page.next_page_token else {
            break;
        };
        page_token = Some(next_page_token);
    }
    assert!(snapshot_paths.contains("/rpc-api"));
    let snapshot_epoch = snapshot_epoch.expect("metadata snapshot must contain the root entry");
    assert!(fs
        .get_cv_metadata_snapshot_page(None, Some(0))
        .await
        .is_err());

    let first_snapshot_page = fs.get_cv_metadata_snapshot_page(None, Some(1)).await?;
    let first_snapshot_epoch = first_snapshot_page.epoch;
    let next_page_token = first_snapshot_page
        .next_page_token
        .expect("fixture must require multiple snapshot pages");
    fs.mkdir(&path("/rpc-api/metadata-epoch-change")?, false)
        .await?;
    let next_snapshot_page = fs
        .get_cv_metadata_snapshot_page(Some(next_page_token), Some(1))
        .await?;
    assert!(next_snapshot_page.epoch > first_snapshot_epoch);

    let source = path("/rpc-api/metadata-source")?;
    let target = path("/rpc-api/metadata-target")?;
    fs.write_string(&source, "metadata replica delta").await?;
    assert!(fs.rename(&source, &target).await?);
    fs.delete(&target, false).await?;

    let mut delta_paths = HashSet::new();
    let mut tombstones = HashSet::new();
    let mut page_token = None;
    let mut target_epoch = None;
    loop {
        let page = fs
            .get_cv_metadata_delta_page(snapshot_epoch, target_epoch, page_token.take(), Some(1))
            .await?;
        assert!(!page.full_snapshot_required);
        assert_eq!(page.from_epoch, snapshot_epoch);
        let epoch = target_epoch.get_or_insert(page.to_epoch);
        assert_eq!(*epoch, page.to_epoch);
        for entry in page.entries {
            if entry.entry.is_none() {
                tombstones.insert(entry.path.clone());
            }
            assert!(
                delta_paths.insert(entry.path.clone()),
                "delta page repeated path {}",
                entry.path
            );
        }
        let Some(next_page_token) = page.next_page_token else {
            break;
        };
        page_token = Some(next_page_token);
    }
    assert!(delta_paths.contains(&target.to_string()));
    assert!(delta_paths.contains(&source.to_string()));
    assert!(tombstones.contains(&target.to_string()));
    assert!(tombstones.contains(&source.to_string()));

    let full_snapshot = fs
        .get_cv_metadata_delta_page(snapshot_epoch, Some(snapshot_epoch), None, Some(1))
        .await?;
    assert!(full_snapshot.full_snapshot_required);
    assert!(fs
        .get_cv_metadata_delta_page(
            snapshot_epoch.saturating_add(1),
            Some(snapshot_epoch),
            None,
            Some(1)
        )
        .await
        .is_err());
    Ok(())
}

async fn verify_mount_and_control_rpcs(
    cluster: &MiniCluster,
    fs: &CurvineFileSystem,
    mount_dir: &str,
) -> CommonResult<()> {
    let mount = path("/rpc-api/mount")?;
    let ufs = path(format!("file://{mount_dir}"))?;
    fs.mount(&ufs, &mount, MountOptions::builder().build())
        .await?;
    assert!(fs.get_mount_info(&mount).await?.is_some());
    assert!(fs
        .get_mount_table()
        .await?
        .iter()
        .any(|entry| entry.cv_path == mount.to_string()));

    let source_path = format!("{mount_dir}/report-task-source");
    std::fs::write(&source_path, [])?;
    let job_client = JobMasterClient::new(fs.fs_client());
    let job = job_client
        .submit_load_job(LoadJobCommand::builder(format!("file://{source_path}")).build())
        .await?;
    job_client
        .report_task(
            &job.job_id,
            "stale-task-report",
            JobTaskProgress {
                state: JobTaskState::Loading,
                loaded_size: 0,
                total_size: 0,
                update_time: 1,
                message: "master-rpc-api stale task report".to_string(),
            },
        )
        .await?;
    assert_eq!(
        job_client.get_job_status(&job.job_id).await?.job_id,
        job.job_id
    );
    job_client.cancel_job(&job.job_id).await?;

    fs.umount(&mount).await?;
    assert!(fs.get_mount_info(&mount).await?.is_none());

    fs.metrics_report().await?;
    let master_info = fs.get_master_info().await?;
    assert_eq!(master_info.live_workers.len(), 1);
    let worker = master_info.live_workers[0].clone();
    let raw_client = cluster.master_client().await?;

    let _: MetricsReportResponse = raw_rpc(
        &raw_client,
        RpcCode::MetricsReport,
        MetricsReportRequest {
            instance: "master-rpc-api-test".to_string(),
            source: "test".to_string(),
            metrics: vec![],
        },
    )
    .await?;

    let _: WorkerHeartbeatResponse = raw_rpc(
        &raw_client,
        RpcCode::WorkerHeartbeat,
        WorkerHeartbeatRequest {
            cluster_id: fs.conf().cluster_id.clone(),
            worker_id: worker.address.worker_id,
            fs_ctime: 0,
            address: ProtoUtils::worker_address_to_pb(&worker.address),
            failed_dirs: 0,
            status: HeartbeatStatus::Running.into(),
            software_version: "master-rpc-api-test".to_string(),
            storages: worker
                .storage_map
                .values()
                .cloned()
                .map(ProtoUtils::storage_info_to_pb)
                .collect(),
            blocks: vec![],
            weight: Some(worker.weight),
            ..Default::default()
        },
    )
    .await?;
    let _: BlockReportListResponse = raw_rpc(
        &raw_client,
        RpcCode::WorkerBlockReport,
        BlockReportListRequest {
            cluster_id: fs.conf().cluster_id.clone(),
            worker_id: worker.address.worker_id,
            full_report: false,
            total_len: 0,
            blocks: vec![],
            full_report_start: Some(false),
        },
    )
    .await?;
    let replication: ReportBlockReplicationResponse = raw_rpc(
        &raw_client,
        RpcCode::ReportBlockReplicationResult,
        ReportBlockReplicationRequest {
            block_id: i64::MAX,
            storage_type: StorageType::Disk.into(),
            success: false,
            message: Some("master-rpc-api control-plane probe".to_string()),
        },
    )
    .await?;
    assert!(replication.success);

    Ok(())
}

#[test]
fn master_filesystem_rpcs_complete_through_a_real_cluster() -> CommonResult<()> {
    let cluster = start_cluster();
    let mount_dir = Utils::test_sub_dir(format!("master-rpc-api-ufs-{}", Utils::rand_str(8)));
    std::fs::create_dir_all(&mount_dir)?;
    let rt = cluster.clone_client_rt();

    rt.block_on(async move {
        let fs = cluster.new_fs();
        verify_namespace_and_metadata_rpcs(&fs).await?;
        verify_block_and_batch_rpcs(&fs).await?;
        verify_metadata_replica_rpcs(&fs).await?;
        verify_mount_and_control_rpcs(&cluster, &fs, &mount_dir).await?;
        Ok(())
    })
}
