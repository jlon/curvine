use curvine_config::{ClusterConf, MasterConf};
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_model::{
    BlockReportInfo, BlockReportList, BlockReportStatus, ClientAddress, ListOptions, RenameFlags,
    SetAttrOptsBuilder, StorageType, WorkerInfo,
};
use curvine_raft::conf::JournalConf;
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_runtime::common::{SerdeUtils, Utils};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::{JournalBatch, JournalEntry, JournalLoader, JournalSystem};
use curvine_server::master::meta::CommitGate;
use curvine_server::master::Master;
use fxhash::FxHasher;
use raft::eraftpb::Entry;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc, Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};

fn new_fs(name: &str) -> (MasterFilesystem, JournalSystem) {
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: temp_stress_dir(&format!("meta-{}", name)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: temp_stress_dir(&format!("journal-{}", name)),
            ..Default::default()
        },
        ..Default::default()
    };

    let js = JournalSystem::from_conf(&conf).unwrap();
    let fs = MasterFilesystem::with_js(&conf, &js);
    fs.add_test_worker(WorkerInfo::default());
    (fs, js)
}

fn assert_mem_store_consistent(fs: &MasterFilesystem) {
    let fs_dir = fs.fs_dir.read();
    let mem_hash = fs_dir.root_dir().sum_hash().unwrap();
    let state_hash = fs_dir.create_tree().unwrap().sum_hash().unwrap();
    assert_eq!(mem_hash, state_hash);
}

fn temp_stress_dir(name: &str) -> String {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "curvine-lock-order-deadlock-stress-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path.display().to_string()
}

fn new_journal_pair(
    name: &str,
) -> (
    MasterFilesystem,
    JournalSystem,
    JournalLoader,
    JournalSystem,
    MasterFilesystem,
) {
    Master::init_test_metrics();
    let suffix = Utils::rand_str(6);
    let mut conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: temp_stress_dir(&format!("meta-{}-leader-{}", name, suffix)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: true,
            journal_dir: temp_stress_dir(&format!("journal-{}-leader-{}", name, suffix)),
            ..Default::default()
        },
        ..Default::default()
    };
    let worker = WorkerInfo::default();

    let leader_js = JournalSystem::from_conf(&conf).unwrap();
    let leader_fs = MasterFilesystem::with_js(&conf, &leader_js);
    leader_fs.add_test_worker(worker.clone());

    conf.master.meta_dir = temp_stress_dir(&format!("meta-{}-follower-{}", name, suffix));
    conf.journal.journal_dir = temp_stress_dir(&format!("journal-{}-follower-{}", name, suffix));
    let follower_js = JournalSystem::from_conf(&conf).unwrap();
    let follower_fs = MasterFilesystem::with_js(&conf, &follower_js);
    follower_fs.add_test_worker(worker);
    let loader = follower_js.journal_loader();

    (leader_fs, leader_js, loader, follower_js, follower_fs)
}

fn apply_journal_entries(loader: &JournalLoader, entries: &[JournalEntry]) -> CommonResult<()> {
    let rt = AsyncRuntime::single();
    rt.block_on(async {
        for (offset, entry) in entries.iter().cloned().enumerate() {
            let index = offset as u64 + 1;
            let op_id = entry.op_id();
            let window_start = offset.saturating_sub(8);
            let window_end = (offset + 8).min(entries.len().saturating_sub(1));
            let mut batch = JournalBatch::new(index);
            batch.push(entry);
            let raft_entry = Entry {
                term: 1,
                index,
                data: SerdeUtils::serialize(&batch)?,
                ..Default::default()
            };
            loader
                .apply(true, ApplyMsg::new_entry(raft_entry))
                .await
                .map_err(|error| {
                    format!(
                        "replay failed at journal index {index}, op_id={op_id}, entry={:?}, nearby entries={:?}: {error}",
                        entries[offset],
                        &entries[window_start..=window_end]
                    )
                })?;
        }
        Ok(())
    })
}

fn assert_journal_entries_consistent(entries: &[JournalEntry], expected_entries: usize) {
    assert_eq!(
        entries.len(),
        expected_entries,
        "journal entry count changed"
    );

    let mut op_ids = HashSet::new();
    let mut inode_ids = HashSet::new();
    for entry in entries {
        assert!(op_ids.insert(entry.op_id()), "duplicate op_id in journal");
        if let Some(inode_id) = entry.inode_id() {
            assert!(inode_ids.insert(inode_id), "duplicate inode_id in journal");
        }
    }
}

fn assert_journal_replay_matches(
    leader_fs: &MasterFilesystem,
    leader_js: &JournalSystem,
    loader: &JournalLoader,
    follower_fs: &MasterFilesystem,
    expected_entries: usize,
) {
    assert_mem_store_consistent(leader_fs);
    let entries = leader_js.fs().fs_dir.read().take_entries();
    assert!(!entries.is_empty(), "journal must contain replay entries");
    assert_journal_entries_consistent(&entries, expected_entries);
    apply_journal_entries(loader, &entries).unwrap();
    assert!(
        follower_fs.fs_dir.read().take_entries().is_empty(),
        "journal replay must not enqueue new journal entries"
    );
    assert_mem_store_consistent(follower_fs);
    assert_eq!(leader_fs.last_inode_id(), follower_fs.last_inode_id());
    assert_eq!(
        leader_fs.sum_hash().unwrap(),
        follower_fs.sum_hash().unwrap()
    );
}

fn run_same_parent_metadata_mutation_stress(
    fs: Arc<MasterFilesystem>,
    thread_count: usize,
    files_per_thread: usize,
) {
    let barrier = Arc::new(Barrier::new(thread_count));
    let mut handles = Vec::new();

    for thread_id in 0..thread_count {
        let fs = fs.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for index in 0..files_per_thread {
                let base_path = format!("/stress/hot/t{:02}-{:04}.log", thread_id, index);
                let final_path = if index % 2 == 0 {
                    let renamed_path = format!("/stress/hot/t{:02}-{:04}.done", thread_id, index);
                    fs.create(&base_path, false).unwrap();
                    fs.rename(&base_path, &renamed_path, RenameFlags::empty())
                        .unwrap();
                    renamed_path
                } else {
                    fs.create(&base_path, false).unwrap();
                    base_path
                };

                if index % 5 == 0 {
                    fs.delete(&final_path, false).unwrap();
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for thread_id in 0..thread_count {
        for index in 0..files_per_thread {
            let base_path = format!("/stress/hot/t{:02}-{:04}.log", thread_id, index);
            let final_path = if index % 2 == 0 {
                format!("/stress/hot/t{:02}-{:04}.done", thread_id, index)
            } else {
                base_path.clone()
            };

            assert_eq!(
                fs.exists(&final_path).unwrap(),
                index % 5 != 0,
                "unexpected final path state for {}",
                final_path
            );
            if index % 2 == 0 {
                assert!(
                    !fs.exists(&base_path).unwrap(),
                    "renamed source path must not remain: {}",
                    base_path
                );
            }
        }
    }

    let kept_files = fs.list_status("/stress/hot").unwrap();
    assert_eq!(
        kept_files.len(),
        thread_count * files_per_thread - thread_count * files_per_thread.div_ceil(5)
    );
    assert!(
        kept_files
            .windows(2)
            .all(|pair| pair[0].name <= pair[1].name),
        "sharded directory listing must keep lexical order"
    );
}

fn run_parent_recreate_create_pressure(
    fs: Arc<MasterFilesystem>,
    creator_count: usize,
    files_per_creator: usize,
    archive_count: usize,
) {
    let barrier = Arc::new(Barrier::new(creator_count + 1));
    let mut handles = Vec::new();

    for creator_id in 0..creator_count {
        let fs = fs.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for index in 0..files_per_creator {
                let path = format!("/volatile/hot/c{:02}-{:04}.log", creator_id, index);
                fs.create(path, true).unwrap();
            }
        }));
    }

    let fs_for_rename = fs.clone();
    let barrier_for_rename = barrier.clone();
    handles.push(thread::spawn(move || {
        barrier_for_rename.wait();
        for index in 0..archive_count {
            let archive_path = format!("/volatile/archive-{:04}", index);
            fs_for_rename
                .rename("/volatile/hot", &archive_path, RenameFlags::empty())
                .unwrap();
            fs_for_rename.mkdir("/volatile/hot", true).unwrap();
        }
    }));

    for handle in handles {
        handle.join().unwrap();
    }

    assert!(fs.exists("/volatile/hot").unwrap());
    for index in 0..archive_count {
        assert!(
            fs.exists(format!("/volatile/archive-{:04}", index))
                .unwrap(),
            "archive dir missing after parent churn: {}",
            index
        );
    }

    let files = fs.list_status("/volatile/*/*.log").unwrap();
    assert_eq!(files.len(), creator_count * files_per_creator + 1);
}

#[test]
fn commit_gate_keeps_writes_blocked_until_all_barriers_drop() {
    let gate = Arc::new(CommitGate::new());
    let barrier1 = gate.close_and_wait();
    let barrier2 = gate.close_and_wait();

    let (entered_tx, entered_rx) = mpsc::channel();
    let gate_for_writer = gate.clone();
    let writer = thread::spawn(move || {
        let _guard = gate_for_writer.enter();
        entered_tx.send(()).unwrap();
    });

    assert!(entered_rx.recv_timeout(Duration::from_millis(100)).is_err());
    drop(barrier1);
    assert!(entered_rx.recv_timeout(Duration::from_millis(100)).is_err());
    drop(barrier2);
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    writer.join().unwrap();
}

#[test]
fn commit_gate_barrier_wakes_after_active_writer_leaves() {
    let gate = Arc::new(CommitGate::new());
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let gate_for_writer = gate.clone();
    let writer = thread::spawn(move || {
        let _guard = gate_for_writer.enter();
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let (closed_tx, closed_rx) = mpsc::channel();
    let gate_for_barrier = gate.clone();
    let barrier = thread::spawn(move || {
        let _barrier = gate_for_barrier.close_and_wait();
        closed_tx.send(()).unwrap();
    });

    assert!(closed_rx.recv_timeout(Duration::from_millis(100)).is_err());
    release_tx.send(()).unwrap();
    closed_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    writer.join().unwrap();
    barrier.join().unwrap();
}

#[test]
fn commit_gate_exclusive_writer_blocks_later_barrier() {
    let gate = Arc::new(CommitGate::new());
    let exclusive = gate.close_and_enter_if_open().unwrap();

    let (closed_tx, closed_rx) = mpsc::channel();
    let gate_for_barrier = gate.clone();
    let barrier = thread::spawn(move || {
        let _barrier = gate_for_barrier.close_and_wait();
        closed_tx.send(()).unwrap();
    });

    assert!(closed_rx.recv_timeout(Duration::from_millis(100)).is_err());
    drop(exclusive);
    closed_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    barrier.join().unwrap();
}

#[test]
fn commit_gate_try_enter_never_bypasses_a_barrier() {
    let gate = CommitGate::new();
    let barrier = gate.close_and_wait();
    assert!(gate.try_enter().is_none());
    drop(barrier);
    assert!(gate.try_enter().is_some());
}

#[test]
fn owned_commit_gate_barrier_keeps_writes_blocked_after_creator_returns() {
    let gate = Arc::new(CommitGate::new());
    let barrier = gate.close_and_wait_owned();

    let (entered_tx, entered_rx) = mpsc::channel();
    let gate_for_writer = gate.clone();
    let writer = thread::spawn(move || {
        let _guard = gate_for_writer.enter();
        entered_tx.send(()).unwrap();
    });

    assert!(entered_rx.recv_timeout(Duration::from_millis(100)).is_err());
    drop(barrier);
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    writer.join().unwrap();
}

#[test]
fn topology_quiescence_does_not_block_unrelated_metadata_commits() {
    let metadata_gate = Arc::new(CommitGate::new());
    let topology_gate = Arc::new(CommitGate::new());
    let topology_barrier = topology_gate.close_and_wait();

    let (metadata_tx, metadata_rx) = mpsc::channel();
    let metadata_writer_gate = metadata_gate.clone();
    let metadata_writer = thread::spawn(move || {
        let _guard = metadata_writer_gate.enter();
        metadata_tx.send(()).unwrap();
    });

    metadata_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let (topology_tx, topology_rx) = mpsc::channel();
    let topology_writer_gate = topology_gate.clone();
    let topology_writer = thread::spawn(move || {
        let _guard = topology_writer_gate.enter();
        topology_tx.send(()).unwrap();
    });

    assert!(topology_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    drop(topology_barrier);
    topology_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    metadata_writer.join().unwrap();
    topology_writer.join().unwrap();
}

#[test]
fn stress_add_block_vs_block_report_no_hang() {
    let (fs, _js) = new_fs("api");
    fs.create("/deadlock/file.log", true).unwrap();

    let fs = Arc::new(fs);
    let loops = 4000usize;
    let timeout = Duration::from_secs(20);
    let start = Instant::now();

    let (tx, rx) = mpsc::channel::<&'static str>();
    let add_progress = Arc::new(AtomicUsize::new(0));
    let report_progress = Arc::new(AtomicUsize::new(0));

    let fs_a = fs.clone();
    let tx_a = tx.clone();
    let add_progress_a = add_progress.clone();
    let t1 = thread::spawn(move || {
        let client = ClientAddress {
            client_name: "deadlock-repro".into(),
            hostname: "localhost".into(),
            ip_addr: "127.0.0.1".into(),
            port: 0,
        };

        let mut ok = 0usize;
        let mut err = 0usize;
        for i in 0..loops {
            let res = fs_a.add_block(
                "/deadlock/file.log",
                None,
                client.clone(),
                vec![],
                vec![],
                0,
                None,
            );
            if res.is_ok() {
                ok += 1;
            } else {
                err += 1;
            }
            if i % 10 == 0 {
                add_progress_a.store(i, Ordering::Relaxed);
            }
        }
        add_progress_a.store(loops, Ordering::Relaxed);
        let _ = tx_a.send("add_block_done");
        (ok, err)
    });

    let fs_b = fs.clone();
    let tx_b = tx.clone();
    let report_progress_b = report_progress.clone();
    let t2 = thread::spawn(move || {
        let mut ok = 0usize;
        let mut err = 0usize;
        for i in 0..loops {
            let report = BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: 100,
                full_report: false,
                full_report_start: false,
                total_len: 0,
                blocks: vec![BlockReportInfo::new(
                    9_000_000 + i as i64,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    0,
                )],
            };

            let res = fs_b.block_report(report, None);
            if res.is_ok() {
                ok += 1;
            } else {
                err += 1;
            }
            if i % 10 == 0 {
                report_progress_b.store(i, Ordering::Relaxed);
            }
        }
        report_progress_b.store(loops, Ordering::Relaxed);
        let _ = tx_b.send("block_report_done");
        (ok, err)
    });

    let first = rx.recv_timeout(timeout).unwrap_or_else(|_| {
        panic!(
            "timeout waiting first worker, add_progress={}, report_progress={}",
            add_progress.load(Ordering::Relaxed),
            report_progress.load(Ordering::Relaxed)
        )
    });
    let second = rx.recv_timeout(timeout).unwrap_or_else(|_| {
        panic!(
            "timeout waiting second worker, add_progress={}, report_progress={}",
            add_progress.load(Ordering::Relaxed),
            report_progress.load(Ordering::Relaxed)
        )
    });

    let (a_ok, a_err) = t1.join().unwrap();
    let (b_ok, b_err) = t2.join().unwrap();

    eprintln!(
        "finished in {:?}, done=({}, {}), add_block ok/err={}/{}, block_report ok/err={}/{}",
        start.elapsed(),
        first,
        second,
        a_ok,
        a_err,
        b_ok,
        b_err
    );
}

#[test]
fn sanity_single_thread_paths_progress() {
    let (fs, _js) = new_fs("sanity");
    fs.create("/deadlock/file.log", true).unwrap();

    let client = ClientAddress {
        client_name: "sanity".into(),
        hostname: "localhost".into(),
        ip_addr: "127.0.0.1".into(),
        port: 0,
    };

    for i in 0..500 {
        let _ = fs.add_block(
            "/deadlock/file.log",
            None,
            client.clone(),
            vec![],
            vec![],
            0,
            None,
        );
        let report = BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: false,
            full_report_start: false,
            total_len: 0,
            blocks: vec![BlockReportInfo::new(
                8_000_000 + i as i64,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                0,
            )],
        };
        let _ = fs.block_report(report, None);
    }
}

#[test]
fn disjoint_namespace_writes_preserve_mem_store_consistency() {
    let (fs, _js) = new_fs("parallel-create");
    for dir in ["a", "b", "c", "d"] {
        fs.mkdir(format!("/parallel/{}", dir), true).unwrap();
    }

    let fs = Arc::new(fs);
    let mut handles = Vec::new();
    for dir in ["a", "b", "c", "d"] {
        let fs = fs.clone();
        let dir = dir.to_string();
        handles.push(thread::spawn(move || {
            for index in 0..200 {
                fs.create(format!("/parallel/{}/file-{}.log", dir, index), false)
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for dir in ["a", "b", "c", "d"] {
        for index in 0..200 {
            assert!(fs
                .exists(format!("/parallel/{}/file-{}.log", dir, index))
                .unwrap());
        }
    }

    assert_mem_store_consistent(&fs);
}

#[test]
fn concurrent_same_parent_metadata_mutations_preserve_consistency() {
    let (fs, _js) = new_fs("same-parent-metadata-stress");
    fs.mkdir("/stress/hot", true).unwrap();

    let fs = Arc::new(fs);
    let thread_count = 16usize;
    let files_per_thread = 160usize;
    run_same_parent_metadata_mutation_stress(fs.clone(), thread_count, files_per_thread);
    assert_mem_store_consistent(&fs);
}

#[test]
fn concurrent_topology_reads_do_not_observe_partial_namespace_updates() {
    let (fs, _js) = new_fs("concurrent-topology-reads");
    fs.mkdir("/reads/hot", true).unwrap();
    for index in 0..32 {
        fs.create(format!("/reads/stable-{index:02}"), true)
            .unwrap();
    }

    let fs = Arc::new(fs);
    let reader_count = 8usize;
    let start = Arc::new(Barrier::new(reader_count + 1));
    let done = Arc::new(AtomicUsize::new(0));
    let mut readers = Vec::with_capacity(reader_count);
    for reader_id in 0..reader_count {
        let fs = fs.clone();
        let start = start.clone();
        let done = done.clone();
        readers.push(thread::spawn(move || {
            start.wait();
            let mut reads = 0usize;
            while done.load(Ordering::Acquire) == 0 {
                let stable_path = format!("/reads/stable-{:02}", reader_id % 32);
                fs.file_status(stable_path).unwrap();
                fs.list_status("/reads/hot").unwrap();
                reads += 1;
            }
            reads
        }));
    }

    start.wait();
    for index in 0..500 {
        let source = format!("/reads/hot/file-{index:04}");
        let target = format!("/reads/hot/file-{index:04}.done");
        fs.create(&source, false).unwrap();
        fs.rename(&source, &target, RenameFlags::empty()).unwrap();
        if index % 2 == 0 {
            fs.delete(&target, false).unwrap();
        }
    }
    done.store(1, Ordering::Release);

    assert!(
        readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .sum::<usize>()
            > 0
    );
    assert_mem_store_consistent(&fs);
}

#[test]
fn concurrent_block_location_reads_survive_path_churn() {
    let (fs, _js) = new_fs("concurrent-block-location-path-churn");
    fs.create("/blocks/hot", true).unwrap();

    let fs = Arc::new(fs);
    let reader_count = 8usize;
    let start = Arc::new(Barrier::new(reader_count + 1));
    let done = Arc::new(AtomicUsize::new(0));
    let mut readers = Vec::with_capacity(reader_count);
    for _ in 0..reader_count {
        let fs = fs.clone();
        let start = start.clone();
        let done = done.clone();
        readers.push(thread::spawn(move || {
            start.wait();
            let mut reads = 0usize;
            while done.load(Ordering::Acquire) == 0 {
                match fs.get_block_locations("/blocks/hot") {
                    Ok(blocks) => assert_eq!(blocks.status.path, "/blocks/hot"),
                    Err(FsError::FileNotFound(_)) => {}
                    Err(error) => {
                        panic!("unexpected block-location error during path churn: {error}")
                    }
                }
                reads += 1;
            }
            reads
        }));
    }

    let (finished_tx, finished_rx) = mpsc::channel();
    let writer_fs = fs.clone();
    let writer_done = done.clone();
    let writer_start = start.clone();
    let writer = thread::spawn(move || {
        writer_start.wait();
        for _ in 0..4_000 {
            writer_fs
                .rename("/blocks/hot", "/blocks/parked", RenameFlags::empty())
                .unwrap();
            writer_fs
                .rename("/blocks/parked", "/blocks/hot", RenameFlags::empty())
                .unwrap();
        }
        writer_done.store(1, Ordering::Release);
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("block-location path churn deadlocked");
    writer.join().unwrap();
    assert!(
        readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .sum::<usize>()
            > 0
    );
    assert_mem_store_consistent(&fs);
}

#[test]
fn concurrent_parent_recreate_reads_do_not_surface_intermediate_store_errors() {
    let (fs, _js) = new_fs("concurrent-parent-recreate-reads");
    fs.mkdir("/reads/parent/hot", true).unwrap();
    fs.create("/reads/parent/hot/file", false).unwrap();

    let fs = Arc::new(fs);
    let reader = {
        let fs = fs.clone();
        thread::spawn(move || {
            for _ in 0..1_000 {
                match fs.list_status("/reads/parent/hot") {
                    Ok(_) | Err(FsError::FileNotFound(_)) => {}
                    Err(error) => {
                        panic!("unexpected read error while parent is recreated: {error}")
                    }
                }
            }
        })
    };

    for _ in 0..100 {
        fs.delete("/reads/parent", true).unwrap();
        fs.mkdir("/reads/parent/hot", true).unwrap();
        fs.create("/reads/parent/hot/file", false).unwrap();
    }

    reader.join().unwrap();
    assert_mem_store_consistent(&fs);
}

#[test]
fn rename_overwrite_keeps_destination_visible_to_concurrent_readers() {
    let (fs, _js) = new_fs("rename-overwrite-visibility");
    fs.create("/destination", true).unwrap();

    let fs = Arc::new(fs);
    let destination = "/destination".to_string();
    let reader_count = 8usize;
    let start = Arc::new(Barrier::new(reader_count + 1));
    let done = Arc::new(AtomicUsize::new(0));
    let mut readers = Vec::with_capacity(reader_count);
    for _ in 0..reader_count {
        let fs = fs.clone();
        let start = start.clone();
        let done = done.clone();
        readers.push(thread::spawn(move || {
            start.wait();
            let mut reads = 0usize;
            while done.load(Ordering::Acquire) == 0 {
                fs.file_status("/destination")
                    .expect("rename overwrite must not make destination disappear");
                reads += 1;
            }
            reads
        }));
    }

    start.wait();
    for index in 0..1_000 {
        let source = format!("/source-{index:04}");
        fs.create(&source, false).unwrap();
        fs.rename(&source, &destination, RenameFlags::empty())
            .unwrap();
    }
    done.store(1, Ordering::Release);

    assert!(
        readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .sum::<usize>()
            > 0
    );
    assert_mem_store_consistent(&fs);
}

#[test]
fn moved_directory_path_reads_do_not_deadlock_with_cross_parent_rename() {
    let (fs, _js) = new_fs("moved-directory-read-lock-order");
    fs.mkdir("/old/hot", true).unwrap();
    fs.create("/old/hot/file", true).unwrap();
    fs.mkdir("/new", true).unwrap();
    fs.rename("/old", "/new/old", RenameFlags::empty()).unwrap();

    let fs = Arc::new(fs);
    let reader_count = 8usize;
    let start = Arc::new(Barrier::new(reader_count + 1));
    let done = Arc::new(AtomicUsize::new(0));
    let mut readers = Vec::with_capacity(reader_count);
    for _ in 0..reader_count {
        let fs = fs.clone();
        let start = start.clone();
        let done = done.clone();
        readers.push(thread::spawn(move || {
            start.wait();
            let mut reads = 0usize;
            while done.load(Ordering::Acquire) == 0 {
                match fs.file_status("/new/old/hot/file") {
                    Ok(_) | Err(FsError::FileNotFound(_)) => reads += 1,
                    Err(error) => panic!("unexpected moved-directory read error: {error}"),
                }
            }
            reads
        }));
    }

    let (complete_tx, complete_rx) = mpsc::channel();
    let writer_fs = fs.clone();
    let writer_done = done.clone();
    let writer_start = start.clone();
    let writer = thread::spawn(move || {
        writer_start.wait();
        for _ in 0..4_000 {
            writer_fs
                .rename("/new/old/hot", "/new/parked", RenameFlags::empty())
                .unwrap();
            writer_fs
                .rename("/new/parked", "/new/old/hot", RenameFlags::empty())
                .unwrap();
        }
        writer_done.store(1, Ordering::Release);
        complete_tx.send(()).unwrap();
    });

    complete_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("cross-parent rename and descendant reads deadlocked");
    writer.join().unwrap();
    assert!(
        readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .sum::<usize>()
            > 0
    );
    assert_mem_store_consistent(&fs);
}

#[test]
fn concurrent_file_status_reads_do_not_regress_after_updates() {
    let (fs, _js) = new_fs("concurrent-file-status-cache");
    fs.create("/reads/file", true).unwrap();
    let initial_mtime = fs.file_status("/reads/file").unwrap().mtime;

    let fs = Arc::new(fs);
    let reader_count = 8usize;
    let updates = 1_000i64;
    let start = Arc::new(Barrier::new(reader_count + 1));
    let done = Arc::new(AtomicUsize::new(0));
    let mut readers = Vec::with_capacity(reader_count);
    for _ in 0..reader_count {
        let fs = fs.clone();
        let start = start.clone();
        let done = done.clone();
        readers.push(thread::spawn(move || {
            start.wait();
            let mut last_mtime = initial_mtime;
            let mut reads = 0usize;
            while done.load(Ordering::Acquire) == 0 {
                let status = fs.file_status("/reads/file").unwrap();
                assert!(
                    status.mtime >= last_mtime,
                    "file status regressed from {} to {}",
                    last_mtime,
                    status.mtime
                );
                last_mtime = status.mtime;
                reads += 1;
            }
            reads
        }));
    }

    start.wait();
    for offset in 1..=updates {
        fs.set_attr(
            "/reads/file",
            SetAttrOptsBuilder::new()
                .mtime(initial_mtime.saturating_add(offset))
                .build(),
        )
        .unwrap();
    }
    done.store(1, Ordering::Release);

    assert!(
        readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .sum::<usize>()
            > 0
    );
    assert_eq!(
        fs.file_status("/reads/file").unwrap().mtime,
        initial_mtime.saturating_add(updates)
    );
    assert_mem_store_consistent(&fs);
}

#[test]
fn force_symlink_replacement_keeps_parent_child_count_stable() {
    const PREFILL_COUNT: usize = 256;
    const REPLACEMENTS: usize = 2_000;
    const READER_COUNT: usize = 4;

    let (fs, _js) = new_fs("force-symlink-directory-status");
    fs.mkdir("/status/hot", true).unwrap();
    for index in 0..PREFILL_COUNT {
        fs.create(format!("/status/hot/file-{index:04}"), false)
            .unwrap();
    }
    fs.symlink("initial", "/status/hot/link", false, 0o777)
        .unwrap();

    let expected_child_count = (PREFILL_COUNT + 1) as i32;
    let fs = Arc::new(fs);
    let start = Arc::new(Barrier::new(READER_COUNT + 1));
    let done = Arc::new(AtomicUsize::new(0));
    let mut readers = Vec::with_capacity(READER_COUNT);
    for _ in 0..READER_COUNT {
        let fs = fs.clone();
        let start = start.clone();
        let done = done.clone();
        readers.push(thread::spawn(move || {
            start.wait();
            while done.load(Ordering::Acquire) == 0 {
                let status = fs.file_status("/status/hot").unwrap();
                assert_eq!(status.children_num, expected_child_count);
            }
        }));
    }

    start.wait();
    for index in 0..REPLACEMENTS {
        let target = if index % 2 == 0 { "left" } else { "right" };
        fs.symlink(target, "/status/hot/link", true, 0o777).unwrap();
    }
    done.store(1, Ordering::Release);
    for reader in readers {
        reader.join().unwrap();
    }

    assert_eq!(
        fs.file_status("/status/hot").unwrap().children_num,
        expected_child_count
    );
    assert_mem_store_consistent(&fs);
}

#[test]
fn concurrent_same_parent_metadata_mutations_replay_from_journal() {
    let (leader_fs, leader_js, loader, _follower_js, follower_fs) =
        new_journal_pair("same-parent-journal-stress");
    leader_fs.mkdir("/stress/hot", true).unwrap();

    let leader_fs = Arc::new(leader_fs);
    let thread_count = 12usize;
    let files_per_thread = 120usize;
    run_same_parent_metadata_mutation_stress(leader_fs.clone(), thread_count, files_per_thread);

    let expected_entries = 2 + thread_count
        * (files_per_thread + files_per_thread.div_ceil(2) + files_per_thread.div_ceil(5));
    assert_journal_replay_matches(
        &leader_fs,
        &leader_js,
        &loader,
        &follower_fs,
        expected_entries,
    );
}

#[test]
fn concurrent_sharded_directory_edge_mutations_replay_from_journal() {
    const PREFILL_COUNT: usize = 2048;
    const WRITER_COUNT: usize = 8;
    const OPERATIONS_PER_WRITER: usize = 32;

    let (leader_fs, leader_js, loader, _follower_js, follower_fs) =
        new_journal_pair("sharded-directory-edge-mutations");
    leader_fs.mkdir("/edges/hot", true).unwrap();

    for index in 0..PREFILL_COUNT {
        leader_fs
            .create(format!("/edges/hot/prefill-{index:04}"), false)
            .unwrap();
    }
    for writer_id in 0..WRITER_COUNT {
        leader_fs
            .create(format!("/edges/hot/source-{writer_id:02}"), false)
            .unwrap();
        for index in 0..OPERATIONS_PER_WRITER {
            leader_fs
                .create(
                    format!("/edges/hot/delete-{writer_id:02}-{index:02}"),
                    false,
                )
                .unwrap();
        }
    }

    let fs = Arc::new(leader_fs);
    let reader_count = 4usize;
    let writer_count = WRITER_COUNT * 3;
    let start = Arc::new(Barrier::new(writer_count + reader_count));
    let done = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(writer_count + reader_count);

    for writer_id in 0..WRITER_COUNT {
        let fs = fs.clone();
        let start = start.clone();
        handles.push(thread::spawn(move || {
            start.wait();
            for index in 0..OPERATIONS_PER_WRITER {
                fs.link(
                    format!("/edges/hot/source-{writer_id:02}"),
                    format!("/edges/hot/link-{writer_id:02}-{index:02}"),
                )
                .unwrap();
            }
        }));
    }

    for writer_id in 0..WRITER_COUNT {
        let fs = fs.clone();
        let start = start.clone();
        handles.push(thread::spawn(move || {
            start.wait();
            for index in 0..OPERATIONS_PER_WRITER {
                fs.symlink(
                    format!("target-{writer_id:02}-{index:02}"),
                    format!("/edges/hot/symlink-{writer_id:02}-{index:02}"),
                    false,
                    0o777,
                )
                .unwrap();
            }
        }));
    }

    for writer_id in 0..WRITER_COUNT {
        let fs = fs.clone();
        let start = start.clone();
        handles.push(thread::spawn(move || {
            start.wait();
            for index in 0..OPERATIONS_PER_WRITER {
                fs.delete(
                    format!("/edges/hot/delete-{writer_id:02}-{index:02}"),
                    false,
                )
                .unwrap();
            }
        }));
    }

    for _ in 0..reader_count {
        let fs = fs.clone();
        let start = start.clone();
        let done = done.clone();
        let read_count = read_count.clone();
        handles.push(thread::spawn(move || {
            start.wait();
            let mut reads = 0usize;
            while done.load(Ordering::Acquire) == 0 {
                fs.file_status("/edges/hot/prefill-0000").unwrap();
                let page = fs
                    .list_options(
                        "/edges/hot",
                        ListOptions {
                            limit: Some(64),
                            start_after: Some("prefill-1000".to_string()),
                        },
                    )
                    .unwrap();
                assert_eq!(page.len(), 64);
                assert!(page.windows(2).all(|pair| pair[0].name <= pair[1].name));
                reads += 1;
            }
            read_count.fetch_add(reads, Ordering::Release);
        }));
    }

    for handle in handles.drain(..writer_count) {
        handle.join().unwrap();
    }
    done.store(1, Ordering::Release);
    for handle in handles {
        handle.join().unwrap();
    }
    assert!(read_count.load(Ordering::Acquire) > 0);

    for writer_id in 0..WRITER_COUNT {
        let source = format!("/edges/hot/source-{writer_id:02}");
        assert_eq!(
            fs.file_status(&source).unwrap().nlink,
            (OPERATIONS_PER_WRITER + 1) as u32
        );
        for index in 0..OPERATIONS_PER_WRITER {
            let link = format!("/edges/hot/link-{writer_id:02}-{index:02}");
            assert_eq!(
                fs.file_status(&link).unwrap().id,
                fs.file_status(&source).unwrap().id
            );
            assert_eq!(
                fs.file_status(format!("/edges/hot/symlink-{writer_id:02}-{index:02}"))
                    .unwrap()
                    .target
                    .as_deref(),
                Some(format!("target-{writer_id:02}-{index:02}").as_str())
            );
            assert!(!fs
                .exists(format!("/edges/hot/delete-{writer_id:02}-{index:02}"))
                .unwrap());
        }
    }
    assert_mem_store_consistent(&fs);

    let setup_entries = 2 + PREFILL_COUNT + WRITER_COUNT * (OPERATIONS_PER_WRITER + 1);
    let mutation_entries = WRITER_COUNT * OPERATIONS_PER_WRITER * 3;
    let expected_entries = setup_entries + mutation_entries;
    assert_journal_replay_matches(&fs, &leader_js, &loader, &follower_fs, expected_entries);
}

#[test]
fn concurrent_same_target_edge_mutations_leave_no_persistent_side_effects() {
    const PREFILL_COUNT: usize = 2048;
    const ROUNDS: usize = 64;

    let (fs, _js) = new_fs("same-target-edge-race");
    fs.mkdir("/race/hot", true).unwrap();
    for index in 0..PREFILL_COUNT {
        fs.create(format!("/race/hot/prefill-{index:04}"), false)
            .unwrap();
    }

    let fs = Arc::new(fs);
    for index in 0..ROUNDS {
        let left = format!("/race/hot/left-{index:04}");
        let right = format!("/race/hot/right-{index:04}");
        let destination = format!("/race/hot/link-{index:04}");
        fs.create(&left, false).unwrap();
        fs.create(&right, false).unwrap();

        let start = Arc::new(Barrier::new(2));
        let left_fs = fs.clone();
        let left_start = start.clone();
        let left_path = left.clone();
        let left_destination = destination.clone();
        let left_result = thread::spawn(move || {
            left_start.wait();
            left_fs.link(left_path, left_destination)
        });
        let right_fs = fs.clone();
        let right_path = right.clone();
        let right_destination = destination.clone();
        let right_result = thread::spawn(move || {
            start.wait();
            right_fs.link(right_path, right_destination)
        });

        let left_ok = left_result.join().unwrap().is_ok();
        let right_ok = right_result.join().unwrap().is_ok();
        assert_ne!(
            left_ok, right_ok,
            "exactly one hard link must win for {destination}"
        );
        let winner = if left_ok { &left } else { &right };
        let loser = if left_ok { &right } else { &left };
        assert_eq!(
            fs.file_status(&destination).unwrap().id,
            fs.file_status(winner).unwrap().id
        );
        assert_eq!(fs.file_status(winner).unwrap().nlink, 2);
        assert_eq!(fs.file_status(loser).unwrap().nlink, 1);
    }

    for index in 0..ROUNDS {
        let destination = format!("/race/hot/symlink-{index:04}");
        let first_target = format!("target-a-{index:04}");
        let second_target = format!("target-b-{index:04}");
        let start = Arc::new(Barrier::new(2));
        let first_fs = fs.clone();
        let first_start = start.clone();
        let first_destination = destination.clone();
        let first_target_for_thread = first_target.clone();
        let first_result = thread::spawn(move || {
            first_start.wait();
            first_fs.symlink(first_target_for_thread, first_destination, false, 0o777)
        });
        let second_fs = fs.clone();
        let second_destination = destination.clone();
        let second_target_for_thread = second_target.clone();
        let second_result = thread::spawn(move || {
            start.wait();
            second_fs.symlink(second_target_for_thread, second_destination, false, 0o777)
        });

        let first_ok = first_result.join().unwrap().is_ok();
        let second_ok = second_result.join().unwrap().is_ok();
        assert_ne!(
            first_ok, second_ok,
            "exactly one symlink must win for {destination}"
        );
        let expected_target = if first_ok {
            &first_target
        } else {
            &second_target
        };
        assert_eq!(
            fs.file_status(&destination).unwrap().target.as_deref(),
            Some(expected_target.as_str())
        );
    }

    assert_mem_store_consistent(&fs);
    fs.restore_from_rocksdb().unwrap();
    assert_mem_store_consistent(&fs);
}

#[test]
fn sharded_list_snapshot_excludes_cross_shard_rename_duplicates() {
    const PREFILL_COUNT: usize = 2048;
    const WARMUP_WRITERS: usize = 8;
    const WARMUP_FILES_PER_WRITER: usize = 16;
    const RENAME_ROUNDS: usize = 1_000;
    const READER_COUNT: usize = 4;

    let (fs, _js) = new_fs("sharded-list-rename-snapshot");
    fs.mkdir("/list/hot", true).unwrap();
    for index in 0..PREFILL_COUNT {
        fs.create(format!("/list/hot/prefill-{index:04}"), false)
            .unwrap();
    }

    let fs = Arc::new(fs);
    let warmup_start = Arc::new(Barrier::new(WARMUP_WRITERS));
    let mut warmup = Vec::with_capacity(WARMUP_WRITERS);
    for writer_id in 0..WARMUP_WRITERS {
        let fs = fs.clone();
        let start = warmup_start.clone();
        warmup.push(thread::spawn(move || {
            start.wait();
            for index in 0..WARMUP_FILES_PER_WRITER {
                fs.create(format!("/list/hot/warm-{writer_id:02}-{index:02}"), false)
                    .unwrap();
            }
        }));
    }
    for writer in warmup {
        writer.join().unwrap();
    }

    let (source_name, destination_name) = distinct_shard_names();
    let source = format!("/list/hot/{source_name}");
    let destination = format!("/list/hot/{destination_name}");
    fs.create(&source, false).unwrap();

    let expected_entries = PREFILL_COUNT + WARMUP_WRITERS * WARMUP_FILES_PER_WRITER + 1;
    let start = Arc::new(Barrier::new(READER_COUNT + 1));
    let done = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let mut readers = Vec::with_capacity(READER_COUNT);
    for _ in 0..READER_COUNT {
        let fs = fs.clone();
        let start = start.clone();
        let done = done.clone();
        let read_count = read_count.clone();
        let source_name = source_name.clone();
        let destination_name = destination_name.clone();
        readers.push(thread::spawn(move || {
            start.wait();
            let mut reads = 0usize;
            while done.load(Ordering::Acquire) == 0 {
                let entries = fs.list_status("/list/hot").unwrap();
                assert_eq!(entries.len(), expected_entries);
                let inode_ids = entries.iter().map(|entry| entry.id).collect::<HashSet<_>>();
                assert_eq!(inode_ids.len(), entries.len());
                let renamed_entries = entries
                    .iter()
                    .filter(|entry| entry.name == source_name || entry.name == destination_name)
                    .count();
                assert_eq!(renamed_entries, 1);
                reads += 1;
            }
            read_count.fetch_add(reads, Ordering::Release);
        }));
    }

    start.wait();
    for _ in 0..RENAME_ROUNDS {
        fs.rename(&source, &destination, RenameFlags::empty())
            .unwrap();
        fs.rename(&destination, &source, RenameFlags::empty())
            .unwrap();
    }
    done.store(1, Ordering::Release);
    for reader in readers {
        reader.join().unwrap();
    }
    assert!(read_count.load(Ordering::Acquire) > 0);
    assert_mem_store_consistent(&fs);
}

fn distinct_shard_names() -> (String, String) {
    let shard_count = std::thread::available_parallelism()
        .map(|count| count.get().saturating_mul(2))
        .unwrap_or(8)
        .next_power_of_two()
        .clamp(8, 64);
    let mut first_by_shard: Vec<Option<(usize, String)>> = vec![None; shard_count];
    for index in 0..shard_count * 2 {
        let name = format!("rename-{index:04}");
        let mut hasher = FxHasher::default();
        name.hash(&mut hasher);
        let shard = (hasher.finish() as usize) & (shard_count - 1);
        if let Some(first) = first_by_shard.iter().flatten().next() {
            if first.0 != shard {
                return (first.1.clone(), name);
            }
        }
        if first_by_shard[shard].is_none() {
            first_by_shard[shard] = Some((shard, name));
        }
    }
    panic!("failed to construct distinct child shard names");
}

#[test]
fn concurrent_parent_recreate_create_pressure_preserves_consistency() {
    let (fs, _js) = new_fs("parent-recreate-create-stress");
    fs.mkdir("/volatile/hot", true).unwrap();
    fs.create("/volatile/hot/warmup.log", false).unwrap();

    let fs = Arc::new(fs);
    run_parent_recreate_create_pressure(fs.clone(), 8, 160, 48);
    assert_mem_store_consistent(&fs);
}

#[test]
fn exchange_is_atomic_to_concurrent_directory_readers() {
    let (fs, _js) = new_fs("exchange-atomic-read");
    fs.mkdir("/exchange", true).unwrap();
    let left = fs.create("/exchange/left", false).unwrap().id;
    let right = fs.create("/exchange/right", false).unwrap().id;
    let fs = Arc::new(fs);
    let stop = Arc::new(AtomicBool::new(false));
    let violated = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(5));
    let mut readers = Vec::new();

    for _ in 0..4 {
        let fs = fs.clone();
        let stop = stop.clone();
        let violated = violated.clone();
        let start = start.clone();
        readers.push(thread::spawn(move || {
            start.wait();
            while !stop.load(Ordering::Acquire) {
                let statuses = fs.list_status("/exchange").unwrap();
                let ids = statuses
                    .into_iter()
                    .map(|status| status.id)
                    .collect::<HashSet<_>>();
                if ids != HashSet::from([left, right]) {
                    violated.store(true, Ordering::Release);
                    break;
                }
            }
        }));
    }

    start.wait();
    for _ in 0..20_000 {
        if violated.load(Ordering::Acquire) {
            break;
        }
        fs.rename("/exchange/left", "/exchange/right", RenameFlags::EXCHANGE)
            .unwrap();
    }
    stop.store(true, Ordering::Release);
    for reader in readers {
        reader.join().unwrap();
    }
    assert!(!violated.load(Ordering::Acquire));
    assert_mem_store_consistent(&fs);
}

#[test]
fn same_parent_rename_race_to_missing_destination_replays() {
    let (leader_fs, leader_js, loader, _follower_js, follower_fs) =
        new_journal_pair("same-parent-rename-destination-race");
    leader_fs.mkdir("/race", true).unwrap();

    let rounds = 64usize;
    for index in 0..rounds {
        leader_fs
            .create(format!("/race/left-{index:04}"), false)
            .unwrap();
        leader_fs
            .create(format!("/race/right-{index:04}"), false)
            .unwrap();
    }

    let fs = Arc::new(leader_fs);
    let start = Arc::new(Barrier::new(2));
    let left_fs = fs.clone();
    let left_start = start.clone();
    let left = thread::spawn(move || {
        for index in 0..rounds {
            left_start.wait();
            left_fs
                .rename(
                    format!("/race/left-{index:04}"),
                    format!("/race/destination-{index:04}"),
                    RenameFlags::empty(),
                )
                .unwrap();
        }
    });
    let right_fs = fs.clone();
    let right = thread::spawn(move || {
        for index in 0..rounds {
            start.wait();
            right_fs
                .rename(
                    format!("/race/right-{index:04}"),
                    format!("/race/destination-{index:04}"),
                    RenameFlags::empty(),
                )
                .unwrap();
        }
    });
    left.join().unwrap();
    right.join().unwrap();

    for index in 0..rounds {
        assert!(!fs.exists(format!("/race/left-{index:04}")).unwrap());
        assert!(!fs.exists(format!("/race/right-{index:04}")).unwrap());
        assert!(fs.exists(format!("/race/destination-{index:04}")).unwrap());
    }

    let expected_entries = 1 + rounds * 4;
    assert_journal_replay_matches(&fs, &leader_js, &loader, &follower_fs, expected_entries);
}

#[test]
fn concurrent_parent_recreate_create_pressure_replay_from_journal() {
    let (leader_fs, leader_js, loader, _follower_js, follower_fs) =
        new_journal_pair("parent-recreate-journal-stress");
    leader_fs.mkdir("/volatile/hot", true).unwrap();
    leader_fs.create("/volatile/hot/warmup.log", false).unwrap();

    let leader_fs = Arc::new(leader_fs);
    let creator_count = 6usize;
    let files_per_creator = 120usize;
    let archive_count = 36usize;
    run_parent_recreate_create_pressure(
        leader_fs.clone(),
        creator_count,
        files_per_creator,
        archive_count,
    );

    let expected_entries = 3 + creator_count * files_per_creator + archive_count * 2;
    assert_journal_replay_matches(
        &leader_fs,
        &leader_js,
        &loader,
        &follower_fs,
        expected_entries,
    );
}

#[test]
fn stale_create_parent_lock_cache_does_not_write_to_renamed_parent() {
    let (fs, _js) = new_fs("stale-create-cache-rename");
    fs.mkdir("/cache/a", true).unwrap();
    fs.create("/cache/a/warmup.log", false).unwrap();

    fs.rename("/cache/a", "/cache/b", RenameFlags::empty())
        .unwrap();
    fs.create("/cache/a/new.log", true).unwrap();

    assert!(fs.exists("/cache/a/new.log").unwrap());
    assert!(fs.exists("/cache/b/warmup.log").unwrap());
    assert!(!fs.exists("/cache/b/new.log").unwrap());
    assert_mem_store_consistent(&fs);
}

#[test]
fn create_parent_lock_cache_relocks_when_target_exists() {
    let (fs, _js) = new_fs("stale-create-cache-target");
    fs.mkdir("/cache/parent", true).unwrap();
    fs.create("/cache/parent/warmup.log", false).unwrap();

    fs.mkdir("/cache/parent/existing", false).unwrap();
    assert!(fs.mkdir("/cache/parent/existing", false).is_err());
    assert!(fs.exists("/cache/parent/existing").unwrap());
    assert_mem_store_consistent(&fs);
}

#[test]
fn set_attr_keeps_tree_and_store_consistent() {
    let (fs, _js) = new_fs("setattr-consistency");
    fs.mkdir("/d", true).unwrap();
    fs.mkdir("/d/sub", true).unwrap();
    fs.create("/d/f1", false).unwrap();
    fs.create("/d/sub/f2", false).unwrap();

    let mk = || curvine_model::SetAttrOpts {
        recursive: false,
        replicas: None,
        owner: Some("alice".to_string()),
        group: Some("staff".to_string()),
        mode: Some(0o750),
        atime: None,
        mtime: Some(1700000000000),
        ttl_ms: None,
        ttl_action: None,
        add_x_attr: std::collections::HashMap::new(),
        remove_x_attr: vec![],
        ufs_mtime: None,
    };

    // Directory set_attr must reach the in-tree node, not only RocksDB.
    fs.set_attr("/d/f1", mk()).unwrap();
    assert_mem_store_consistent(&fs);
    fs.set_attr("/d", mk()).unwrap();
    assert_mem_store_consistent(&fs);

    // Recursive set_attr rewrites every directory level.
    let mut recursive = mk();
    recursive.recursive = true;
    recursive.owner = Some("bob".to_string());
    fs.set_attr("/d", recursive).unwrap();
    assert_mem_store_consistent(&fs);
}
