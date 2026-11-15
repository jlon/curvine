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

//! Cluster-level load check for follower reads: three masters, three workers,
//! `master.follower_reads_enabled` on the servers, and a fan of reader threads
//! issuing open+seek+read loops like a metadata-heavy randread job. Success is
//! measured by the master's own `master_follower_read_accepted_total` counter
//! increasing, not by direct-RPC assertions.

#![cfg(any(target_os = "linux", target_os = "macos"))]

#[path = "common/follower_daemon.rs"]
mod daemon;

use curvine_client_core::file::{CurvineFileSystem, FsReader};
use curvine_config::ClusterConf;
use curvine_fs_api::{Path, Reader, Writer};
use curvine_net::net::InetAddr;
use curvine_raft::raft::RaftPeer;
use curvine_rpc::client::RpcClient;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use daemon::{Daemon, TestDirectory};
use std::fs;
use std::io::Write;
use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const FILE_LEN: usize = 16 * 1024 * 1024; // 16 MB across 1 MB blocks
const READERS: usize = 8;
const READS_PER_READER: usize = 200;
const CHUNK: usize = 64 * 1024;

fn three_worker_cluster() -> (TestDirectory, Vec<ClusterConf>, Vec<ClusterConf>) {
    // 3 masters: rpc+web+raft each; 3 workers: rpc+web each.
    let sockets: Vec<_> = (0..15)
        .map(|_| TcpListener::bind(("127.0.0.1", 0)).unwrap())
        .collect();
    let ports: Vec<_> = sockets
        .iter()
        .map(|s| s.local_addr().unwrap().port())
        .collect();
    drop(sockets);
    let root = TestDirectory(std::env::temp_dir().join(format!(
        "curvine-follower-load-{}-{}",
        std::process::id(),
        ports[0]
    )));
    fs::create_dir_all(&root.0).unwrap();

    let mut base = ClusterConf {
        format_master: true,
        ..Default::default()
    };
    base.master.hostname = "127.0.0.1".to_string();
    base.master.io_threads = 1;
    base.master.worker_threads = 2;
    base.master.log.log_dir = "stderr".to_string();
    base.master.heartbeat_interval = "500ms".to_string();
    base.master.follower_reads_enabled = true;
    base.journal.hostname = "127.0.0.1".to_string();
    base.journal.io_threads = 1;
    base.journal.worker_threads = 2;
    base.journal.raft_tick_interval_ms = 100;
    base.journal.raft_heartbeat_tick = 1;
    base.journal.journal_addrs = (0..3)
        .map(|i| RaftPeer::new(i as u64 + 1, "127.0.0.1", ports[3 * i + 2]))
        .collect();
    base.client.master_addrs = (0..3)
        .map(|i| InetAddr::new("127.0.0.1", ports[3 * i]))
        .collect();
    base.client.block_size_str = "1MB".to_string();
    base.client.hostname = "127.0.0.1".to_string();
    base.client.io_threads = 1;
    base.client.worker_threads = 2;
    base.client.init().unwrap();
    base.worker.hostname = "127.0.0.1".to_string();
    base.worker.io_threads = 1;
    base.worker.worker_threads = 2;
    base.worker.log.log_dir = "stderr".to_string();
    base.worker.follower_reports_enabled = true;

    let master_confs: Vec<ClusterConf> = (0..3)
        .map(|i| {
            let mut conf = base.clone();
            conf.master.rpc_port = ports[3 * i];
            conf.master.web_port = ports[3 * i + 1];
            conf.journal.rpc_port = ports[3 * i + 2];
            conf.master.meta_dir = root
                .0
                .join(format!("meta-{i}"))
                .to_str()
                .unwrap()
                .to_string();
            conf.journal.journal_dir = root
                .0
                .join(format!("journal-{i}"))
                .to_str()
                .unwrap()
                .to_string();
            conf
        })
        .collect();

    let worker_confs: Vec<ClusterConf> = (0..3)
        .map(|i| {
            let mut conf = base.clone();
            conf.worker.rpc_port = ports[9 + 2 * i];
            conf.worker.web_port = ports[9 + 2 * i + 1];
            conf.worker.data_dir = vec![format!(
                "[SSD:64MB]{}",
                root.0.join(format!("data-{i}")).display()
            )];
            conf
        })
        .collect();

    (root, master_confs, worker_confs)
}

async fn follower_reads_served(confs: &[ClusterConf]) -> u64 {
    let mut total = 0;
    for conf in confs {
        if let Ok(mut socket) =
            tokio::net::TcpStream::connect(("127.0.0.1", conf.master.web_port)).await
        {
            if socket
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .is_err()
            {
                continue;
            }
            let mut body = String::new();
            if tokio::time::timeout(Duration::from_secs(3), socket.read_to_string(&mut body))
                .await
                .is_err()
            {
                continue;
            }
            total += body
                .lines()
                .filter(|line| {
                    !line.starts_with('#') && line.contains("master_follower_read_accepted_total{")
                })
                .map(|line| {
                    line.split_whitespace()
                        .last()
                        .unwrap()
                        .parse::<u64>()
                        .unwrap()
                })
                .sum::<u64>();
        }
    }
    total
}

fn spawn_reader_threads(
    confs: &[ClusterConf],
    path: Path,
    ready: Arc<AtomicU64>,
    ops: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
) -> Vec<std::thread::JoinHandle<()>> {
    (0..READERS)
        .map(|t| {
            let conf = {
                let mut c = confs[0].clone();
                c.client.follower_reads_enabled = true;
                c
            };
            let path = path.clone();
            let ready = ready.clone();
            let ops = ops.clone();
            let bytes = bytes.clone();
            let errors = errors.clone();
            std::thread::spawn(move || {
                let rt = Arc::new(AsyncRuntime::single());
                rt.block_on({
                    let fs_rt = rt.clone();
                    async move {
                        let fs = CurvineFileSystem::with_rt(conf, fs_rt).unwrap();
                        let mut seed = 0x9e3779b97f4a7c15u64
                            .wrapping_add((t as u64).wrapping_mul(0x517cc1b727220a95));
                        let mut rng = move || {
                            seed ^= seed << 13;
                            seed ^= seed >> 7;
                            seed ^= seed << 17;
                            seed
                        };
                        let mut buf = vec![0u8; CHUNK];
                        for _ in 0..READS_PER_READER {
                            let res: Result<(), String> = async {
                                let mut reader: FsReader =
                                    fs.open(&path).await.map_err(|e| e.to_string())?;
                                let max_off = (FILE_LEN - CHUNK) as u64;
                                let off = (rng() % max_off) as i64;
                                reader.seek(off).await.map_err(|e| e.to_string())?;
                                let n = reader.read(&mut buf).await.map_err(|e| e.to_string())?;
                                if n == 0 {
                                    return Err("short read".into());
                                }
                                bytes.fetch_add(n as u64, Ordering::Relaxed);
                                Ok(())
                            }
                            .await;
                            match res {
                                Ok(()) => {
                                    ops.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    if errors.fetch_add(1, Ordering::Relaxed) < 5 {
                                        eprintln!("reader {t} error: {e}");
                                    }
                                }
                            }
                            ready.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            })
        })
        .collect()
}

#[test]
fn follower_read_offload_under_randread_load() {
    let (root, master_confs, worker_confs) = three_worker_cluster();
    let mut daemons = Vec::new();
    for (i, conf) in master_confs.iter().enumerate() {
        let path = root.0.join(format!("master-{i}.toml"));
        fs::write(&path, conf.to_pretty_toml().unwrap()).unwrap();
        daemons.push(Daemon::spawn("master", &path, &root));
    }
    let rt = Arc::new(AsyncRuntime::single());
    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(master_confs[0].clone(), rt.clone()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            for daemon in &mut daemons {
                daemon.assert_running();
            }
            if tokio::time::timeout(Duration::from_secs(2), fs.get_filesystem_info())
                .await
                .is_ok_and(|r| r.is_ok())
            {
                break;
            }
            assert!(Instant::now() < deadline, "no elected master became ready");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for (index, address) in master_confs[0].client.master_addrs.iter().enumerate() {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                daemons[index].assert_running();
                if RpcClient::with_raw(address, &master_confs[0].client_rpc_conf())
                    .await
                    .is_ok()
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "master {index} RPC never started"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        let mut workers = Vec::new();
        for (i, conf) in worker_confs.iter().enumerate() {
            let path = root.0.join(format!("worker-{i}.toml"));
            let mut w = std::io::BufWriter::new(fs::File::create(&path).unwrap());
            writeln!(w, "{}", conf.to_pretty_toml().unwrap()).unwrap();
            drop(w);
            workers.push(Daemon::spawn("worker", &path, &root));
        }

        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let live = fs.get_filesystem_info().await.unwrap().live_workers.len();
            if live == 3 {
                break;
            }
            for w in &mut workers {
                w.assert_running();
            }
            assert!(
                Instant::now() < deadline,
                "only {live}/3 workers registered"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Seed data once.
        let path = Path::from_str("/load-file").unwrap();
        let mut writer = fs.create(&path, true).await.unwrap();
        let payload = vec![0xabu8; 1024 * 1024];
        for _ in 0..(FILE_LEN / payload.len()) {
            writer.write(&payload).await.unwrap();
        }
        writer.complete().await.unwrap();

        // Wait until followers actually carry the written view (bounded
        // staleness is expected, but they must catch up, not stay stale).
        let before = follower_reads_served(&master_confs).await;
        {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                let leader_index = fs
                    .fs_client()
                    .master_peer_leader_id()
                    .and_then(|id| {
                        fs.fs_client()
                            .master_peer_snapshot()
                            .iter()
                            .position(|p| p.id == id)
                    })
                    .unwrap_or(0);
                let follower_index = (leader_index + 1) % 3;
                let rpc = RpcClient::with_raw(
                    &master_confs[0].client.master_addrs[follower_index],
                    &master_confs[0].client_rpc_conf(),
                )
                .await
                .unwrap();
                use curvine_fs_api::RpcCode;
                use curvine_model::ProtoUtils;
                use curvine_proto::GetBlockLocationsRequest;
                use curvine_proto::GetBlockLocationsResponse;
                use curvine_rpc::message::{MessageBuilder, FOLLOWER_READ_SEQ_ID};
                let request = MessageBuilder::new_rpc(RpcCode::GetBlockLocations)
                    .seq_id(FOLLOWER_READ_SEQ_ID)
                    .proto_header(GetBlockLocationsRequest {
                        path: path.encode(),
                    })
                    .build();
                let reply = rpc
                    .timeout_rpc(Duration::from_secs(3), request)
                    .await
                    .unwrap();
                let ok = reply.is_success()
                    && reply
                        .parse_header::<GetBlockLocationsResponse>()
                        .is_ok_and(|h| {
                            let blocks = ProtoUtils::file_blocks_from_pb(h.blocks);
                            blocks.status.len == FILE_LEN as i64 && !blocks.block_locs.is_empty()
                        });
                if ok {
                    eprintln!("follower {follower_index} caught up");
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "follower never caught up with the written file"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        let ready = Arc::new(AtomicU64::new(0));
        let ops = Arc::new(AtomicU64::new(0));
        let bytes = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let handles = spawn_reader_threads(
            &master_confs,
            path.clone(),
            ready.clone(),
            ops.clone(),
            bytes.clone(),
            errors.clone(),
        );

        // Watch progress; finish early if every reader finished its quota.
        let deadline = Instant::now() + Duration::from_secs(240);
        loop {
            let done = ready.load(Ordering::Relaxed) as usize;
            if done == READERS * READS_PER_READER {
                break;
            }
            assert!(Instant::now() < deadline, "readers stalled at {done}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        for h in handles {
            h.join().unwrap();
        }

        let total_ops = ops.load(Ordering::Relaxed);
        let total_errors = errors.load(Ordering::Relaxed);
        let after = follower_reads_served(&master_confs).await;
        eprintln!(
            "load: ops={} errors={} bytes={} follower_reads_before={} after={}",
            total_ops,
            total_errors,
            bytes.load(Ordering::Relaxed),
            before,
            after
        );
        assert_eq!(total_errors, 0, "read errors under load");
        assert_eq!(total_ops as usize, READERS * READS_PER_READER);
        assert!(
            after > before,
            "follower masters served no reads under load ({before} -> {after})"
        );

        for w in &mut workers {
            w.terminate().await;
        }
        for d in &mut daemons {
            d.terminate().await;
        }
    });
}
