// TEMPORARY diagnostic harness for the parent-recreate replay divergence.
// Not intended to be committed. Reproduces
// concurrent_parent_recreate_create_pressure_replay_from_journal in a loop
// inside one process and, on the first leader/follower sum_hash mismatch,
// dumps both in-memory trees plus a path-keyed diff.

use curvine_config::{ClusterConf, MasterConf};
use curvine_core_error::CommonResult;
use curvine_model::{RenameFlags, WorkerInfo};
use curvine_raft::conf::JournalConf;
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_runtime::common::{SerdeUtils, Utils};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::{JournalBatch, JournalEntry, JournalLoader, JournalSystem};
use curvine_server::master::meta::inode::InodeView;
use curvine_server::master::Master;
use raft::eraftpb::Entry;
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;

fn temp_dir(name: &str) -> String {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "cv-parent-recreate-diag-{}-{}",
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
    MasterFilesystem,
) {
    Master::init_test_metrics();
    let suffix = Utils::rand_str(6);
    let mut conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: temp_dir(&format!("{}-leader-{}", name, suffix)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: true,
            journal_dir: temp_dir(&format!("{}-journal-leader-{}", name, suffix)),
            ..Default::default()
        },
        ..Default::default()
    };
    let worker = WorkerInfo::default();

    let leader_js = JournalSystem::from_conf(&conf).unwrap();
    let leader_fs = MasterFilesystem::with_js(&conf, &leader_js);
    leader_fs.add_test_worker(worker.clone());

    conf.master.meta_dir = temp_dir(&format!("{}-follower-{}", name, suffix));
    conf.journal.journal_dir = temp_dir(&format!("{}-journal-follower-{}", name, suffix));
    let follower_js = JournalSystem::from_conf(&conf).unwrap();
    let follower_fs = MasterFilesystem::with_js(&conf, &follower_js);
    follower_fs.add_test_worker(worker);
    let loader = follower_js.journal_loader();

    (leader_fs, leader_js, loader, follower_fs)
}

fn apply_journal_entries(loader: &JournalLoader, entries: &[JournalEntry]) -> CommonResult<()> {
    let rt = AsyncRuntime::single();
    rt.block_on(async {
        for (offset, entry) in entries.iter().cloned().enumerate() {
            let index = offset as u64 + 1;
            let mut batch = JournalBatch::new(index);
            batch.push(entry.clone());
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
}

fn walk_tree(view: &InodeView, parent: &str, out: &mut BTreeMap<String, String>) {
    let raw_name = view.name();
    let path = if parent.is_empty() {
        "/".to_string()
    } else if parent == "/" {
        format!("/{}", raw_name)
    } else {
        format!("{}/{}", parent, raw_name)
    };

    let kind;
    if view.is_dir() {
        kind = "DIR";
        if let Ok(dir) = view.as_dir_ref() {
            for child in dir.children_iter() {
                walk_tree(&child, &path, out);
            }
        }
    } else {
        kind = "FILE";
    }

    out.insert(
        path,
        format!(
            "kind={} id={} mtime={} ctime={} nlink={}",
            kind,
            view.id(),
            view.mtime(),
            view.ctime(),
            view.nlink().unwrap_or(0),
        ),
    );
}

fn dump_tree(fs: &MasterFilesystem) -> BTreeMap<String, String> {
    let fs_dir = fs.fs_dir.read();
    let mut out = BTreeMap::new();
    walk_tree(fs_dir.root_dir(), "", &mut out);
    out
}

#[test]
fn diag_parent_recreate_replay_divergence() {
    let max_attempts = 300;
    for attempt in 0..max_attempts {
        let (leader_fs, leader_js, loader, follower_fs) =
            new_journal_pair(&format!("diag-{:03}", attempt));

        leader_fs.mkdir("/volatile/hot", true).unwrap();
        leader_fs.create("/volatile/hot/warmup.log", false).unwrap();

        let leader_fs = Arc::new(leader_fs);
        run_parent_recreate_create_pressure(leader_fs.clone(), 6, 120, 36);

        // Leader self-check first (mirrors the flaky test's preconditions).
        {
            let fs_dir = leader_fs.fs_dir.read();
            let mem_hash = fs_dir.root_dir().sum_hash().unwrap();
            let state_hash = fs_dir.create_tree().unwrap().sum_hash().unwrap();
            assert_eq!(mem_hash, state_hash, "attempt {}: leader mem/store diverged; this is a DIFFERENT failure mode than the one under diagnosis", attempt);
        }

        let entries = leader_js.fs().fs_dir.read().take_entries();
        assert!(!entries.is_empty(), "attempt {}: journal empty", attempt);
        apply_journal_entries(&loader, &entries).unwrap();

        assert!(
            follower_fs.fs_dir.read().take_entries().is_empty(),
            "attempt {}: follower replay enqueued journal entries",
            attempt
        );
        {
            let fs_dir = follower_fs.fs_dir.read();
            let mem_hash = fs_dir.root_dir().sum_hash().unwrap();
            let state_hash = fs_dir.create_tree().unwrap().sum_hash().unwrap();
            assert_eq!(
                mem_hash, state_hash,
                "attempt {}: follower mem/store diverged",
                attempt
            );
        }
        assert_eq!(
            leader_fs.last_inode_id(),
            follower_fs.last_inode_id(),
            "attempt {}: last_inode_id mismatch",
            attempt
        );

        let leader_hash = leader_fs.sum_hash().unwrap();
        let follower_hash = follower_fs.sum_hash().unwrap();
        if leader_hash == follower_hash {
            continue;
        }

        // Divergence captured. Dump and diff.
        println!(
            "=== DIVERGENCE at attempt {} after {} entries ===",
            attempt,
            entries.len()
        );
        println!("leader   sum_hash = {:#034x}", leader_hash);
        println!("follower sum_hash = {:#034x}", follower_hash);

        let leader_tree = dump_tree(&leader_fs);
        let follower_tree = dump_tree(&follower_fs);

        let mut only_leader = 0usize;
        let mut only_follower = 0usize;
        let mut mismatched = 0usize;
        let mut diff_lines: Vec<String> = Vec::new();
        for (path, l) in &leader_tree {
            match follower_tree.get(path) {
                None => {
                    only_leader += 1;
                    diff_lines.push(format!("ONLY-LEADER  {} | {}", path, l));
                }
                Some(f) if f != l => {
                    mismatched += 1;
                    diff_lines.push(format!(
                        "MISMATCH     {} | leader {} | follower {}",
                        path, l, f
                    ));
                }
                _ => {}
            }
        }
        for (path, f) in &follower_tree {
            if !leader_tree.contains_key(path) {
                only_follower += 1;
                diff_lines.push(format!("ONLY-FOLLOWER {} | {}", path, f));
            }
        }

        let report_path = std::env::temp_dir().join(format!(
            "parent-recreate-diag-report-{}",
            Utils::rand_str(6)
        ));
        let mut report = String::new();
        report.push_str(&format!("attempt {}\n", attempt));
        report.push_str(&format!("leader   sum_hash = {:#034x}\n", leader_hash));
        report.push_str(&format!("follower sum_hash = {:#034x}\n", follower_hash));
        report.push_str(&format!(
            "diff summary: only_leader={} only_follower={} mismatched={}\n\n",
            only_leader, only_follower, mismatched
        ));
        for line in &diff_lines {
            report.push_str(line);
            report.push('\n');
        }
        report.push_str("\n===== JOURNAL ENTRIES =====\n");
        for entry in &entries {
            report.push_str(&format!("{:?}\n", entry));
        }
        std::fs::write(&report_path, report).unwrap();

        println!(
            "diff summary: only_leader={} only_follower={} mismatched={}",
            only_leader, only_follower, mismatched
        );
        println!("first 40 diff lines:");
        for line in diff_lines.iter().take(40) {
            println!("{}", line);
        }
        println!("full report: {}", report_path.display());
        panic!(
            "divergence reproduced on attempt {}; report at {}",
            attempt,
            report_path.display()
        );
    }
    panic!(
        "no divergence observed in {} attempts — try again under load",
        max_attempts
    );
}
