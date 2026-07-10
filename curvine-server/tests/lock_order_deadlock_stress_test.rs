use curvine_common::conf::{ClusterConf, JournalConf, MasterConf};
use curvine_common::raft::storage::{AppStorage, ApplyMsg};
use curvine_common::state::RenameFlags;
use curvine_common::state::{
    BlockReportInfo, BlockReportList, BlockReportStatus, ClientAddress, StorageType, WorkerInfo,
};
use curvine_common::utils::SerdeUtils;
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::{JournalBatch, JournalEntry, JournalLoader, JournalSystem};
use curvine_server::master::meta::NamespaceCommitGate;
use curvine_server::master::Master;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use raft::eraftpb::Entry;
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
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
            let mut batch = JournalBatch::new(index);
            batch.push(entry);
            let raft_entry = Entry {
                term: 1,
                index,
                data: SerdeUtils::serialize(&batch)?,
                ..Default::default()
            };
            loader.apply(true, ApplyMsg::new_entry(raft_entry)).await?;
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
fn namespace_commit_gate_keeps_writes_blocked_until_all_barriers_drop() {
    let gate = Arc::new(NamespaceCommitGate::new());
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

            let res = fs_b.block_report(report);
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
        let _ = fs.block_report(report);
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
fn concurrent_parent_recreate_create_pressure_preserves_consistency() {
    let (fs, _js) = new_fs("parent-recreate-create-stress");
    fs.mkdir("/volatile/hot", true).unwrap();
    fs.create("/volatile/hot/warmup.log", false).unwrap();

    let fs = Arc::new(fs);
    run_parent_recreate_create_pressure(fs.clone(), 8, 160, 48);
    assert_mem_store_consistent(&fs);
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
