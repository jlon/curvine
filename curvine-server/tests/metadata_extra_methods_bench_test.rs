// Copyright 2025 OPPO.
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

//! Extra per-method metadata QPS benchmark covering MasterFilesystem
//! operations not exercised by metadata_edge_qps_bench_test:
//! exists / list_status / set_attr / file-lock cycle / write lifecycle
//! (create -> open -> add_block -> complete -> get_file_blocks -> delete).
//!
//! Same fixture style as the edge bench: in-process MasterFilesystem with a
//! disabled journal, one test worker, data dirs under CURVINE_METADATA_BENCH_DIR.

use curvine_config::{ClusterConf, JournalConf, MasterConf};
use curvine_model::ClientAddress;
use curvine_model::{
    BlockLocation, CommitBlock, FileLock, FileStatus, LockFlags, LockType, SetAttrOptsBuilder,
    WorkerInfo,
};
use curvine_runtime::common::Utils;
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::Master;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const LIST_WORKSET: usize = 2048;

#[derive(Clone, Copy)]
enum ExtraOperation {
    Exists,
    ListStatus,
    SetAttr,
    FileLockCycle,
    WriteLifecycle,
}

impl ExtraOperation {
    fn name(self) -> &'static str {
        match self {
            Self::Exists => "exists",
            Self::ListStatus => "list_status",
            Self::SetAttr => "set_attr",
            Self::FileLockCycle => "file_lock_cycle",
            Self::WriteLifecycle => "write_lifecycle",
        }
    }
}

fn selected_operation() -> ExtraOperation {
    let workload =
        std::env::var("CURVINE_EXTRA_BENCH_WORKLOAD").unwrap_or_else(|_| "exists".to_string());
    match workload.as_str() {
        "exists" => ExtraOperation::Exists,
        "list_status" => ExtraOperation::ListStatus,
        "set_attr" => ExtraOperation::SetAttr,
        "file_lock_cycle" => ExtraOperation::FileLockCycle,
        "write_lifecycle" => ExtraOperation::WriteLifecycle,
        other => panic!("unknown CURVINE_EXTRA_BENCH_WORKLOAD: {other}"),
    }
}

fn test_dir(name: &str) -> PathBuf {
    let mut path = PathBuf::from(
        std::env::var("CURVINE_METADATA_BENCH_DIR").unwrap_or_else(|_| "/dev/shm".to_string()),
    );
    path.push(format!("curvine-extra-qps-{name}-{}", Utils::rand_str(8)));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn new_fs(name: &str) -> (MasterFilesystem, JournalSystem, Vec<PathBuf>) {
    Master::init_test_metrics();
    let meta_dir = test_dir(&format!("meta-{name}"));
    let journal_dir = test_dir(&format!("journal-{name}"));
    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: meta_dir.display().to_string(),
            io_threads: 2,
            worker_threads: 2,
            actor_threads: 1,
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: journal_dir.display().to_string(),
            io_threads: 2,
            worker_threads: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let js = JournalSystem::from_conf(&conf).unwrap();
    let fs = MasterFilesystem::with_js(&conf, &js);
    fs.add_test_worker(WorkerInfo::default());
    (fs, js, vec![meta_dir, journal_dir])
}

fn prepare_worksets(fs: &MasterFilesystem, threads: usize) {
    fs.mkdir("/bench", true).unwrap();
    // Large-directory workset for list_status (per-thread parent keeps the
    // same-parent vs distinct-parents distinction moot; this bench isolates
    // per-method cost, not edge contention).
    for worker in 0..threads {
        let directory = format!("/bench/list-{worker:02}");
        fs.mkdir(&directory, true).unwrap();
        for index in 0..LIST_WORKSET {
            fs.create(format!("{directory}/f-{index:05}"), false).unwrap();
        }
    }
    // Hot file workset for exists/set_attr/lock/write-lifecycle.
    for worker in 0..threads {
        let directory = format!("/bench/hot-{worker:02}");
        fs.mkdir(&directory, true).unwrap();
        fs.create(format!("{directory}/probe"), false).unwrap();
    }
}

fn measure(operation: ExtraOperation, threads: usize, duration: Duration) {
    let (fs, _js, dirs) = new_fs(operation.name());
    prepare_worksets(&fs, threads);
    let fs = Arc::new(fs);
    let start = Arc::new(Barrier::new(threads));
    let mut handles = Vec::with_capacity(threads);

    for worker in 0..threads {
        let fs = fs.clone();
        let start = start.clone();
        handles.push(thread::spawn(move || {
            let hot = format!("/bench/hot-{worker:02}");
            let probe = format!("{hot}/probe");
            let list_dir = format!("/bench/list-{worker:02}");
            let client = format!("bench-client-{worker:02}");
            let mut index = 0u64;
            let mut locked = false;
            start.wait();
            let begin = Instant::now();
            while begin.elapsed() < duration {
                match operation {
                    ExtraOperation::Exists => {
                        assert!(fs.exists(&probe).unwrap());
                    }
                    ExtraOperation::ListStatus => {
                        let statuses: Vec<FileStatus> = fs.list_status(&list_dir).unwrap();
                        assert_eq!(statuses.len(), LIST_WORKSET);
                    }
                    ExtraOperation::SetAttr => {
                        let opts = SetAttrOptsBuilder::new()
                            .mode(0o600 + (index % 8) as u32)
                            .build();
                        fs.set_attr(&probe, opts).unwrap();
                    }
                    ExtraOperation::FileLockCycle => {
                        if locked {
                            // Release by clearing the byte range we own.
                            let release = FileLock {
                                client_id: client.clone(),
                                owner_id: worker as u64 + 1,
                                lock_type: LockType::WriteLock,
                                lock_flags: LockFlags::Plock,
                                start: 0,
                                end: 0,
                                ..Default::default()
                            };
                            fs.set_lock(&probe, release).unwrap();
                            locked = false;
                        } else {
                            let lock = FileLock {
                                client_id: client.clone(),
                                owner_id: worker as u64 + 1,
                                lock_type: LockType::WriteLock,
                                lock_flags: LockFlags::Plock,
                                start: 0,
                                end: u64::MAX,
                                ..Default::default()
                            };
                            let conflict = fs.set_lock(&probe, lock).unwrap();
                            assert!(conflict.is_none());
                            let held = fs.get_lock(
                                &probe,
                                FileLock {
                                    client_id: client.clone(),
                                    owner_id: worker as u64 + 1,
                                    lock_type: LockType::ReadLock,
                                    lock_flags: LockFlags::Plock,
                                    start: 0,
                                    end: 1,
                                    ..Default::default()
                                },
                            );
                            let _ = held.unwrap().is_some();
                            locked = true;
                        }
                    }
                    ExtraOperation::WriteLifecycle => {
                        let path = format!("{hot}/wl-{index}");
                        fs.create(&path, false).unwrap();
                        let addr = ClientAddress::default();
                        let status = fs.file_status(&path).unwrap();
                        let first = fs
                            .add_block(&path, None, addr.clone(), vec![], vec![], 0, None)
                            .unwrap();
                        let first_commit = CommitBlock {
                            block_id: first.block.id,
                            block_len: status.block_size,
                            locations: vec![BlockLocation::with_id(100)],
                        };
                        let second = fs
                            .add_block(
                                &path,
                                None,
                                addr,
                                vec![first_commit],
                                vec![],
                                status.block_size,
                                Some(first.block.clone()),
                            )
                            .unwrap();
                        fs.complete_file(
                            &path,
                            None,
                            (status.block_size * 2) as i64,
                            vec![
                                CommitBlock {
                                    block_id: first.block.id,
                                    block_len: status.block_size,
                                    locations: vec![BlockLocation::with_id(100)],
                                },
                                CommitBlock {
                                    block_id: second.block.id,
                                    block_len: status.block_size,
                                    locations: vec![BlockLocation::with_id(100)],
                                },
                            ],
                            &client,
                            true,
                            None,
                        )
                        .unwrap();
                        let blocks = fs.get_block_locations(&path).unwrap();
                        assert_eq!(blocks.block_locs.len(), 2);
                        fs.delete(&path, false).unwrap();
                    }
                }
                index += 1;
            }
            (index, begin.elapsed())
        }));
    }

    let mut operations = 0u64;
    let mut elapsed = Duration::ZERO;
    for handle in handles {
        let (count, took) = handle.join().unwrap();
        operations += count;
        elapsed = elapsed.max(took);
    }

    println!(
        "EXTRA_METHOD_QPS operation={} operations={} elapsed_ms={} qps={:.2}",
        operation.name(),
        operations,
        elapsed.as_millis(),
        operations as f64 / elapsed.as_secs_f64()
    );

    drop(fs);
    for dir in dirs {
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
#[ignore = "manual per-method QPS comparison; use CURVINE_EXTRA_BENCH_WORKLOAD to isolate one method per process"]
fn measure_extra_method_qps() {
    let seconds = std::env::var("CURVINE_EXTRA_BENCH_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5);
    let threads = std::env::var("CURVINE_EXTRA_BENCH_THREADS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16);
    assert!(seconds > 0);
    assert!(threads > 0);
    measure(selected_operation(), threads, Duration::from_secs(seconds));
}
