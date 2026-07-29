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

use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_fs_api::CurvineURI;
use curvine_model::{
    BlockLocation, BlockReportInfo, BlockReportList, BlockReportStatus, ClientAddress, CommitBlock,
    CreateFileOpts, FileAllocOpts, FileLock, HeartbeatStatus, LockFlags, LockType, MountOptions,
    OpenFlags, RenameFlags, SetAttrOptsBuilder, StorageType, WorkerCommand, WorkerInfo, WriteType,
};
use curvine_net::net::NetUtils;
use curvine_raft::proto::raft::{AppliedIndex, FsmState, SnapshotData, SnapshotFileList};
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_raft::raft::{NodeId, RaftPeer};
use curvine_runtime::common::SerdeUtils;
use curvine_runtime::common::{FileUtils, Logger, TimeSpent, Utils};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::{
    JournalBatch, JournalCommandBatch, JournalEntry, JournalEnvelope, JournalLoader, JournalSystem,
    UfsLoader,
};
use curvine_server::master::{Master, MountManager};
use log::info;
use raft::{eraftpb::Entry, StateRole};
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

fn replay_entries(loader: &JournalLoader, entries: Vec<JournalEntry>) -> CommonResult<()> {
    let rt = AsyncRuntime::single();
    rt.block_on(async move {
        for (offset, entry) in entries.into_iter().enumerate() {
            let entry = raft_entry(offset as u64 + 1, entry)?;
            loader.apply(true, ApplyMsg::new_entry(entry)).await?;
        }
        Ok(())
    })
}

fn raft_entry(index: u64, entry: JournalEntry) -> CommonResult<Entry> {
    let mut batch = JournalBatch::new(index);
    batch.push(entry);
    Ok(Entry {
        term: 1,
        index,
        data: SerdeUtils::serialize(&batch)?,
        ..Default::default()
    })
}

fn reopen_journal_system(conf: &ClusterConf) -> CommonResult<JournalSystem> {
    for _ in 0..50 {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            JournalSystem::from_conf(conf)
        })) {
            Ok(Ok(js)) => return Ok(js),
            Ok(Err(e)) if e.to_string().contains("lock hold by current process") => {
                thread::sleep(Duration::from_millis(100));
            }
            Ok(Err(e)) => return Err(e.into()),
            Err(panic) if panic_message(&panic).contains("lock hold by current process") => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
    JournalSystem::from_conf(conf).map_err(|e| e.into())
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(msg) = payload.downcast_ref::<String>() {
        return msg.clone();
    }
    if let Some(msg) = payload.downcast_ref::<&str>() {
        return (*msg).to_string();
    }
    String::new()
}

fn new_test_ufs_uri(name: &str) -> CommonResult<CurvineURI> {
    let dir = std::env::temp_dir().join(format!(
        "curvine-journal-{name}-{}-{}",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    std::fs::create_dir_all(&dir)?;
    CurvineURI::new(format!("file://{}/", dir.display()))
}

#[test]
fn replay_accepts_versioned_legacy_journal_batch() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut source_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    source_conf.change_test_meta_dir(format!("versioned-source-{}", Utils::rand_str(6)));
    let source_fs = JournalSystem::fs_only_for_test(&source_conf)?;
    source_fs.mkdir("/versioned-legacy", false)?;
    let entry = source_fs
        .fs_dir
        .read()
        .take_entries()
        .into_iter()
        .next()
        .expect("mkdir must emit a journal entry");

    let mut target_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    target_conf.change_test_meta_dir(format!("versioned-target-{}", Utils::rand_str(6)));
    let target = JournalSystem::from_conf(&target_conf)?;
    let target_fs = target.fs();
    let loader = target.journal_loader();

    let mut batch = JournalCommandBatch::new(1);
    batch.push_legacy(entry);
    let raft_entry = Entry {
        term: 1,
        index: 1,
        data: JournalEnvelope::encode(batch)?,
        ..Default::default()
    };

    AsyncRuntime::single()
        .block_on(async { loader.apply(true, ApplyMsg::new_entry(raft_entry)).await })?;

    assert!(target_fs.file_status("/versioned-legacy").is_ok());
    Ok(())
}

#[test]
fn promotion_applies_committed_metadata_before_returning() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut source_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    source_conf.change_test_meta_dir(format!("promotion-source-{}", Utils::rand_str(6)));
    let source_fs = JournalSystem::fs_only_for_test(&source_conf)?;
    source_fs.mkdir("/committed-before-promotion", false)?;
    let entry = source_fs
        .fs_dir
        .read()
        .take_entries()
        .into_iter()
        .next()
        .expect("mkdir must emit a journal entry");

    let mut target_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    target_conf.change_test_meta_dir(format!("promotion-target-{}", Utils::rand_str(6)));
    let target = JournalSystem::from_conf(&target_conf)?;
    let target_fs = target.fs();
    let loader = target.journal_loader();
    let raft_entry = raft_entry(1, entry)?;
    target.set_committed_entries_for_test(&[raft_entry], 1)?;

    AsyncRuntime::single().block_on(async { loader.role_change(StateRole::Leader).await })?;

    assert!(target_fs.file_status("/committed-before-promotion").is_ok());
    Ok(())
}

#[test]
fn replay_rejects_duplicate_allocated_inode_id() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut first_source_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    first_source_conf.change_test_meta_dir(format!("duplicate-id-source-a-{}", Utils::rand_str(6)));
    let first_source_fs = JournalSystem::fs_only_for_test(&first_source_conf)?;
    first_source_fs.mkdir("/first", false)?;
    let first = first_source_fs
        .fs_dir
        .read()
        .take_entries()
        .into_iter()
        .next()
        .expect("mkdir must emit a journal entry");

    let mut second_source_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    second_source_conf
        .change_test_meta_dir(format!("duplicate-id-source-b-{}", Utils::rand_str(6)));
    let second_source_fs = JournalSystem::fs_only_for_test(&second_source_conf)?;
    second_source_fs.mkdir("/second", false)?;
    let second = second_source_fs
        .fs_dir
        .read()
        .take_entries()
        .into_iter()
        .next()
        .expect("mkdir must emit a journal entry");

    assert_eq!(first.allocated_inode_id(), second.allocated_inode_id());

    let mut target_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    target_conf.change_test_meta_dir(format!("duplicate-id-target-{}", Utils::rand_str(6)));
    let target = JournalSystem::from_conf(&target_conf)?;
    let loader = target.journal_loader();
    let first_entry = raft_entry(1, first)?;
    let second_entry = raft_entry(2, second)?;

    let result = AsyncRuntime::single().block_on(async {
        loader.apply(true, ApplyMsg::new_entry(first_entry)).await?;
        loader.apply(true, ApplyMsg::new_entry(second_entry)).await
    });
    assert!(result.is_err());
    Ok(())
}

#[test]
fn replay_scan_rejects_committed_log_gap() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("journal-gap-{}", Utils::rand_str(6)));
    let journal_system = JournalSystem::from_conf(&conf)?;
    let loader = journal_system.journal_loader();

    let entry1 = Entry {
        term: 1,
        index: 1,
        ..Default::default()
    };
    let entry3 = Entry {
        term: 1,
        index: 3,
        ..Default::default()
    };
    journal_system.set_committed_entries_for_test(&[entry1, entry3], 3)?;

    let result = AsyncRuntime::single().block_on(async {
        loader
            .apply(true, ApplyMsg::new_scan(AppliedIndex::default()))
            .await
    });

    let err = result.expect_err("replay must reject committed journal gaps");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("committed entry gap") && err_msg.contains("hard_state.commit=3"),
        "unexpected error: {}",
        err_msg
    );
    Ok(())
}

#[test]
fn active_namespace_changes_replicate_without_legacy_writer_queue() -> CommonResult<()> {
    Master::init_test_metrics();

    let port1 = NetUtils::hold_available_port();
    let port2 = NetUtils::hold_available_port();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.journal.writer_flush_batch_size = 1;
    conf.journal.writer_flush_batch_ms = 10;
    conf.journal.raft_tick_interval_ms = 100;
    conf.journal.raft_check_quorum = false;
    conf.journal.journal_addrs = vec![
        RaftPeer::new(port1 as NodeId, &conf.master.hostname, port1),
        RaftPeer::new(port2 as NodeId, &conf.master.hostname, port2),
    ];

    conf.change_test_meta_dir("active-committed-namespace-1");
    conf.journal.rpc_port = port1;
    let js1 = JournalSystem::from_conf(&conf)?;
    let fs1 = MasterFilesystem::with_js(&conf, &js1);
    let mnt1 = js1.mount_manager();
    let monitor1 = js1.master_monitor();

    conf.change_test_meta_dir("active-committed-namespace-2");
    conf.journal.rpc_port = port2;
    let js2 = JournalSystem::from_conf(&conf)?;
    let fs2 = MasterFilesystem::with_js(&conf, &js2);
    let mnt2 = js2.mount_manager();
    let monitor2 = js2.master_monitor();

    js1.start_blocking()?;
    js2.start_blocking()?;

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let (active, standby, active_mnt, standby_mnt) = loop {
        if monitor1.is_active() {
            break (fs1.clone(), fs2.clone(), mnt1.clone(), mnt2.clone());
        }
        if monitor2.is_active() {
            break (fs2.clone(), fs1.clone(), mnt2.clone(), mnt1.clone());
        }
        if std::time::Instant::now() >= deadline {
            return err_box!("Not found active master");
        }
        thread::sleep(Duration::from_millis(100));
    };

    let worker = WorkerInfo::default();
    active.add_test_worker(worker.clone());
    standby.add_test_worker(worker.clone());
    let mount_opts = MountOptions::builder().build();
    let mount_ufs = new_test_ufs_uri("active-committed-mount")?;
    active_mnt.mount(
        None,
        "/committed-mount",
        mount_ufs.encode_uri().as_ref(),
        &mount_opts,
    )?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::Mount(_))),
        "active mount must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    let umount_ufs = new_test_ufs_uri("active-committed-umount")?;
    active_mnt.mount(
        None,
        "/committed-umount",
        umount_ufs.encode_uri().as_ref(),
        &mount_opts,
    )?;
    let _ = active.fs_dir.read().take_entries();
    active_mnt.umount("/committed-umount")?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::UnMount(_))),
        "active umount must not emit legacy local-first journal entries: {legacy_entries:?}"
    );

    let barrier = Arc::new(Barrier::new(2));
    let mut handles = vec![];
    for _ in 0..2 {
        let fs = active.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            fs.create_with_opts(
                "/exclusive-race",
                CreateFileOpts::with_create(false),
                OpenFlags::new_create().set_exclusive(true),
            )
            .map(|_| ())
        }));
    }
    let mut successes = 0;
    let mut failures = 0;
    for handle in handles {
        match handle
            .join()
            .expect("exclusive create thread must not panic")
        {
            Ok(()) => successes += 1,
            Err(_) => failures += 1,
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(failures, 1);

    active.mkdir("/committed-dir", false)?;
    active.mkdir("/deleted-dir", false)?;
    active.create("/reopen-file", false)?;
    active.open_file(
        "/reopen-file",
        CreateFileOpts::with_create(false),
        OpenFlags::new_write_only(),
    )?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::ReopenFile(_))),
        "active reopen_file must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    active.symlink("/target", "/committed-symlink", false, 0o777)?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::Symlink(_))),
        "active symlink must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    active.create("/hardlink-source", false)?;
    active.link("/hardlink-source", "/hardlink-dst")?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::Link(_))),
        "active link must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    active.create("/lockfile", false)?;
    active.set_lock(
        "/lockfile",
        FileLock {
            client_id: "client1".to_string(),
            owner_id: 1,
            lock_type: LockType::WriteLock,
            lock_flags: LockFlags::Plock,
            start: 0,
            end: 100,
            ..Default::default()
        },
    )?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::SetLocks(_))),
        "active set_lock must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    active.create("/block-metadata-file", false)?;
    let block_metadata = active.add_block(
        "/block-metadata-file",
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    )?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::AddBlock(_))),
        "active add_block must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    active.complete_file(
        "/block-metadata-file",
        None,
        9,
        vec![CommitBlock {
            block_id: block_metadata.block.id,
            block_len: 9,
            locations: vec![BlockLocation::with_id(worker.worker_id())],
        }],
        "",
        false,
        None,
    )?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::CompleteFile(_))),
        "active complete_file must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    active.create("/rename-victim", false)?;
    let old_block = active.add_block(
        "/rename-victim",
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    )?;
    active.complete_file(
        "/rename-victim",
        None,
        10,
        vec![CommitBlock {
            block_id: old_block.block.id,
            block_len: 10,
            locations: vec![BlockLocation::with_id(worker.worker_id())],
        }],
        "",
        false,
        None,
    )?;
    active.block_report(
        BlockReportList {
            cluster_id: conf.cluster_id.clone(),
            worker_id: worker.worker_id(),
            full_report: false,
            total_len: 0,
            blocks: vec![BlockReportInfo::new(
                old_block.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                old_block.block.len,
            )],
        },
        None,
    )?;
    active.create("/rename-source", false)?;
    active.rename("/rename-source", "/rename-victim", RenameFlags::empty())?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::Rename(_))),
        "active overwrite rename must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    active.create("/delete-file", false)?;
    let delete_block = active.add_block(
        "/delete-file",
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    )?;
    active.complete_file(
        "/delete-file",
        None,
        11,
        vec![CommitBlock {
            block_id: delete_block.block.id,
            block_len: 11,
            locations: vec![BlockLocation::with_id(worker.worker_id())],
        }],
        "",
        false,
        None,
    )?;
    active.block_report(
        BlockReportList {
            cluster_id: conf.cluster_id.clone(),
            worker_id: worker.worker_id(),
            full_report: false,
            total_len: 0,
            blocks: vec![BlockReportInfo::new(
                delete_block.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                delete_block.block.len,
            )],
        },
        None,
    )?;
    active.create("/post-delete-flush", false)?;
    active.delete("/delete-file", false)?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::Delete(_))),
        "active file delete must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    active.create("/free-file", false)?;
    let free_block = active.add_block(
        "/free-file",
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    )?;
    active.complete_file(
        "/free-file",
        None,
        12,
        vec![CommitBlock {
            block_id: free_block.block.id,
            block_len: 12,
            locations: vec![BlockLocation::with_id(worker.worker_id())],
        }],
        "",
        false,
        None,
    )?;
    active.block_report(
        BlockReportList {
            cluster_id: conf.cluster_id.clone(),
            worker_id: worker.worker_id(),
            full_report: false,
            total_len: 0,
            blocks: vec![BlockReportInfo::new(
                free_block.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                free_block.block.len,
            )],
        },
        None,
    )?;
    active.create("/post-free-flush", false)?;
    active.free("/free-file", false)?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::Free(_))),
        "active free must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    active.create("/overwrite-file", false)?;
    let overwrite_block = active.add_block(
        "/overwrite-file",
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    )?;
    active.complete_file(
        "/overwrite-file",
        None,
        13,
        vec![CommitBlock {
            block_id: overwrite_block.block.id,
            block_len: 13,
            locations: vec![BlockLocation::with_id(worker.worker_id())],
        }],
        "",
        false,
        None,
    )?;
    active.block_report(
        BlockReportList {
            cluster_id: conf.cluster_id.clone(),
            worker_id: worker.worker_id(),
            full_report: false,
            total_len: 0,
            blocks: vec![BlockReportInfo::new(
                overwrite_block.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                overwrite_block.block.len,
            )],
        },
        None,
    )?;
    active.create("/post-overwrite-flush", false)?;
    let _ = active.fs_dir.read().take_entries();
    active.create_with_opts(
        "/overwrite-file",
        CreateFileOpts::with_create(false),
        OpenFlags::new_create().set_overwrite(true),
    )?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::OverWriteFile(_))),
        "active overwrite create must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    active.create("/resize-file", false)?;
    let resize_block = active.add_block(
        "/resize-file",
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    )?;
    active.complete_file(
        "/resize-file",
        None,
        14,
        vec![CommitBlock {
            block_id: resize_block.block.id,
            block_len: 14,
            locations: vec![BlockLocation::with_id(worker.worker_id())],
        }],
        "",
        false,
        None,
    )?;
    active.block_report(
        BlockReportList {
            cluster_id: conf.cluster_id.clone(),
            worker_id: worker.worker_id(),
            full_report: false,
            total_len: 0,
            blocks: vec![BlockReportInfo::new(
                resize_block.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                resize_block.block.len,
            )],
        },
        None,
    )?;
    active.create("/post-resize-flush", false)?;
    let _ = active.fs_dir.read().take_entries();
    active.resize("/resize-file", FileAllocOpts::with_truncate(0))?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::CompleteFile(_))),
        "active resize must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    active.create("/assign-worker-file", false)?;
    let assign_blocks = active.resize("/assign-worker-file", FileAllocOpts::with_truncate(16))?;
    let assign_block = assign_blocks
        .block_locs
        .into_iter()
        .find(|block| block.should_assign())
        .expect("resize-created block must require worker assignment");
    let _ = active.fs_dir.read().take_entries();
    active.assign_worker(
        "/assign-worker-file",
        assign_block.block,
        ClientAddress::default(),
        vec![],
    )?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries
            .iter()
            .any(|entry| matches!(entry, JournalEntry::AddBlock(_))),
        "active assign_worker must not emit legacy local-first journal entries: {legacy_entries:?}"
    );
    let worker_cmds = active.worker_manager.write().heartbeat(
        &conf.cluster_id,
        HeartbeatStatus::Running,
        worker.address.clone(),
        worker.weight,
        vec![],
    )?;
    assert!(
        worker_cmds.iter().any(|cmd| matches!(
            cmd,
            WorkerCommand::DeleteBlock(delete) if delete.blocks.contains(&delete_block.block.id)
        )),
        "file delete must schedule old block deletion"
    );
    assert!(
        worker_cmds.iter().any(|cmd| matches!(
            cmd,
            WorkerCommand::DeleteBlock(delete) if delete.blocks.contains(&old_block.block.id)
        )),
        "overwrite rename must schedule old block deletion"
    );
    assert!(
        worker_cmds.iter().any(|cmd| matches!(
            cmd,
            WorkerCommand::DeleteBlock(delete) if delete.blocks.contains(&overwrite_block.block.id)
        )),
        "overwrite create must schedule old block deletion"
    );
    assert!(
        worker_cmds.iter().any(|cmd| matches!(
            cmd,
            WorkerCommand::DeleteBlock(delete) if delete.blocks.contains(&resize_block.block.id)
        )),
        "resize truncate must schedule old block deletion"
    );
    active.create("/committed-file", false)?;
    active.rename("/committed-file", "/renamed-file", RenameFlags::empty())?;
    active.set_attr(
        "/renamed-file",
        SetAttrOptsBuilder::new().owner("committed-owner").build(),
    )?;
    active.delete("/deleted-dir", false)?;
    let legacy_entries = active.fs_dir.read().take_entries();
    assert!(
        !legacy_entries.iter().any(|entry| matches!(
            entry,
            JournalEntry::Mkdir(_)
                | JournalEntry::CreateFile(_)
                | JournalEntry::ReopenFile(_)
                | JournalEntry::Rename(_)
                | JournalEntry::SetAttr(_)
                | JournalEntry::Delete(_)
                | JournalEntry::Symlink(_)
                | JournalEntry::Link(_)
                | JournalEntry::SetLocks(_)
                | JournalEntry::AddBlock(_)
                | JournalEntry::CompleteFile(_)
                | JournalEntry::OverWriteFile(_)
                | JournalEntry::Mount(_)
                | JournalEntry::UnMount(_)
        )),
        "active namespace changes must not emit legacy local-first namespace journal entries: {legacy_entries:?}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if standby.file_status("/committed-dir").is_ok()
            && standby.file_status("/committed-mount").is_ok()
            && standby.file_status("/committed-umount").is_ok()
            && standby.file_status("/committed-file").is_err()
            && standby.file_status("/deleted-dir").is_err()
            && standby.file_status("/exclusive-race").is_ok()
            && standby.file_status("/reopen-file").is_ok()
            && standby.file_status("/committed-symlink").is_ok()
            && standby.file_status("/hardlink-dst").is_ok()
            && standby.file_status("/lockfile").is_ok()
            && standby.file_status("/block-metadata-file").is_ok()
            && standby.file_status("/rename-victim").is_ok()
            && standby.file_status("/rename-source").is_err()
            && standby.file_status("/delete-file").is_err()
            && standby.file_status("/free-file").is_ok()
            && standby.file_status("/overwrite-file").is_ok()
            && standby.file_status("/assign-worker-file").is_ok()
        {
            let standby_mounts = standby_mnt.get_mount_table().unwrap_or_default();
            let mount_converged = standby_mounts
                .iter()
                .any(|mount| mount.cv_path == "/committed-mount")
                && !standby_mounts
                    .iter()
                    .any(|mount| mount.cv_path == "/committed-umount");
            if let (Ok(status), Ok(resized)) = (
                standby.file_status("/renamed-file"),
                standby.file_status("/resize-file"),
            ) {
                if status.owner == "committed-owner" && resized.len == 0 && mount_converged {
                    return Ok(());
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            return err_box!("standby did not apply committed namespace changes");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

// First start a master and perform the operation; then start 1 stand by, manually replay the log to check consistency.
#[test]
fn test_journal_replay_consistency_between_leader_and_follower() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    let worker = WorkerInfo::default();

    conf.change_test_meta_dir("meta-js1");
    let journal_system = JournalSystem::from_conf(&conf)?;
    let fs_leader = MasterFilesystem::with_js(&conf, &journal_system);
    let mnt_mgr1 = journal_system.mount_manager();
    fs_leader.add_test_worker(worker.clone());

    run(&fs_leader, &worker)?;
    run_mnt(mnt_mgr1.clone())?;

    /************* Replay log from node **************/
    conf.change_test_meta_dir("meta-js2");
    let follower_journal_system = JournalSystem::from_conf(&conf)?;
    let fs_follower = MasterFilesystem::with_js(&conf, &follower_journal_system);
    let mnt_mgr2 = follower_journal_system.mount_manager();
    let journal_loader = follower_journal_system.journal_loader();
    let entries = journal_system.fs().fs_dir.read().take_entries();
    info!("entries size {}", entries.len());
    replay_entries(&journal_loader, entries)?;

    fs_leader.print_tree();
    fs_follower.print_tree();
    assert_eq!(fs_leader.last_inode_id(), fs_follower.last_inode_id());
    assert_eq!(fs_leader.sum_hash()?, fs_follower.sum_hash()?);

    let leader_mnt = mnt_mgr1.get_mount_table().unwrap();
    let follower_mnt = mnt_mgr2.get_mount_table().unwrap();
    assert_eq!(leader_mnt.len(), 1);
    assert_eq!(leader_mnt.len(), follower_mnt.len());
    assert_eq!(leader_mnt[0], follower_mnt[0]);

    Ok(())
}

// Start 2 masters at the same time to check the correctness of log playback.
#[test]
fn test_raft_consensus_and_state_synchronization_between_two_masters() -> CommonResult<()> {
    Logger::default();
    Master::init_test_metrics();

    // hold_available_port keeps each socket bound until the Raft server claims it,
    // preventing TOCTOU races when nextest runs tests in parallel.
    let port1 = NetUtils::hold_available_port();
    let port2 = NetUtils::hold_available_port();

    let mut conf = ClusterConf::default();
    conf.journal.writer_flush_batch_size = 1;
    conf.journal.writer_flush_batch_ms = 10;
    conf.journal.raft_tick_interval_ms = 100;
    conf.journal.raft_check_quorum = false;
    conf.journal.journal_addrs = vec![
        RaftPeer::new(port1 as NodeId, &conf.master.hostname, port1),
        RaftPeer::new(port2 as NodeId, &conf.master.hostname, port2),
    ];
    let worker = WorkerInfo::default();

    conf.change_test_meta_dir("raft-1");
    conf.journal.rpc_port = port1;
    let js1 = JournalSystem::from_conf(&conf).unwrap();
    let fs1 = MasterFilesystem::with_js(&conf, &js1);
    let mnt_mgr1 = js1.mount_manager();
    fs1.add_test_worker(worker.clone());
    let fs_monitor1 = js1.master_monitor();

    conf.change_test_meta_dir("raft-2");
    conf.journal.rpc_port = port2;
    let js2 = JournalSystem::from_conf(&conf).unwrap();
    let fs2 = MasterFilesystem::with_js(&conf, &js2);
    let mnt_mgr2 = js2.mount_manager();
    fs2.add_test_worker(worker.clone());
    let fs_monitor2 = js2.master_monitor();

    js1.start_blocking()?;
    js2.start_blocking()?;

    // Wait for the success of the choice of the owner.
    let mut wait = 30 * 1000;
    while wait > 0 {
        let start = TimeSpent::new();
        if fs_monitor1.is_active() || fs_monitor2.is_active() {
            break;
        }
        wait -= start.used_ms();
        thread::sleep(Duration::from_millis(100));
    }

    let (active, standby, mnt_mgr) = {
        if fs_monitor1.is_active() {
            (fs1, fs2, mnt_mgr1.clone())
        } else if fs_monitor2.is_active() {
            (fs2, fs1, mnt_mgr2.clone())
        } else {
            return err_box!("Not found active master");
        }
    };

    info!("state 1 {:?}", fs_monitor1.journal_state());
    info!("state 2 {:?}", fs_monitor2.journal_state());

    run(&active, &worker)?;
    run_mnt(mnt_mgr.clone())?;

    // Poll until the standby's filesystem state AND mount table converge with
    // the active node, rather than using a fixed sleep that may be insufficient
    // under load. Both inode state and mount table are replicated via separate
    // Raft log entries, so we must wait for all of them to be applied.
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    loop {
        let leader_mnt = mnt_mgr1.get_mount_table().unwrap_or_default();
        let follower_mnt = mnt_mgr2.get_mount_table().unwrap_or_default();
        let hash_converged = match (active.sum_hash(), standby.sum_hash()) {
            (Ok(active_hash), Ok(standby_hash)) => active_hash == standby_hash,
            _ => false,
        };
        if active.last_inode_id() == standby.last_inode_id()
            && hash_converged
            && leader_mnt.len() == follower_mnt.len()
        {
            break;
        }
        if std::time::Instant::now() >= deadline {
            active.print_tree();
            standby.print_tree();
            assert_eq!(active.last_inode_id(), standby.last_inode_id());
            assert_eq!(active.sum_hash()?, standby.sum_hash()?);
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }

    let leader_mnt = mnt_mgr1.get_mount_table().unwrap();
    let follower_mnt = mnt_mgr2.get_mount_table().unwrap();
    assert_eq!(leader_mnt.len(), 1);
    assert_eq!(leader_mnt.len(), follower_mnt.len());
    assert_eq!(leader_mnt[0], follower_mnt[0]);

    Ok(())
}

fn run(fs_leader: &MasterFilesystem, worker: &WorkerInfo) -> CommonResult<()> {
    let address = ClientAddress::default();
    /************* Master node execution log **************/
    // Create a directory
    fs_leader.mkdir("/journal/a", true)?;
    fs_leader.mkdir("/journal_1/a", true)?;
    fs_leader.mkdir("/journal_1/b", true)?;
    fs_leader.mkdir("/journal_2/a", true)?;
    fs_leader.mkdir("/journal_2/b", true)?;

    // Create a file.
    let status = fs_leader.create("/journal/b/test.log", true)?;

    // Assign block
    let block =
        fs_leader.add_block(&status.path, None, address.clone(), vec![], vec![], 0, None)?;

    // Complete the file.
    let commit = CommitBlock {
        block_id: block.block.id,
        block_len: 10,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader.complete_file(
        &status.path,
        None,
        10,
        vec![commit],
        &address.client_name,
        false,
        None,
    )?;

    // File renaming
    fs_leader.rename(
        "/journal/b/test.log",
        "/journal/a/test.log",
        RenameFlags::empty(),
    )?;

    // delete
    fs_leader.delete("/journal_2", true)?;

    let path = "/journal/append.log";
    fs_leader.create(path, true)?;

    let block = fs_leader.add_block(path, None, address.clone(), vec![], vec![], 0, None)?;
    let commit = CommitBlock {
        block_id: block.block.id,
        block_len: 10,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader.complete_file(path, None, 10, vec![commit], "", false, None)?;

    let commit = CommitBlock {
        block_id: block.block.id,
        block_len: 13,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader.open_file(
        path,
        CreateFileOpts::with_create(true),
        OpenFlags::new_create(),
    )?;
    fs_leader.complete_file(path, None, 13, vec![commit], "", false, None)?;

    Ok(())
}

fn run_mnt(mnt_mgr: Arc<MountManager>) -> CommonResult<()> {
    /************* Master node execution log **************/
    //mount file:///... -> /x/y/z
    let mgr = mnt_mgr;
    let mount_uri = CurvineURI::new("/x/y/z")?;
    let ufs_uri = new_test_ufs_uri("mnt-1")?;
    let mut config = HashMap::new();
    config.insert("k1".to_string(), "v1".to_string());
    let mnt_opt = MountOptions::builder().set_properties(config).build();
    mgr.mount(
        None,
        mount_uri.path(),
        ufs_uri.encode_uri().as_ref(),
        &mnt_opt,
    )?;

    //mount file:///... -> /x/z/y
    let mount_uri = CurvineURI::new("/x/z/y")?;
    let ufs_uri = new_test_ufs_uri("mnt-2")?;
    let mut config = HashMap::new();
    config.insert("k2".to_string(), "v1".to_string());
    let mnt_opt = MountOptions::builder().build();
    mgr.mount(
        None,
        mount_uri.path(),
        ufs_uri.encode_uri().as_ref(),
        &mnt_opt,
    )?;

    // umount
    let mount_uri = CurvineURI::new("/x/z/y")?;
    mgr.umount(mount_uri.path())?;

    Ok(())
}

#[test]
fn test_ufs_loader_mkdir_recreates_missing_ufs_parent() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "ufs-loader-mkdir-parent-{}",
        curvine_runtime::common::LocalTime::mills()
    ));

    let journal_system = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &journal_system);
    let mount_manager = journal_system.mount_manager();

    let ufs_dir = std::env::temp_dir().join(format!(
        "curvine-ufs-loader-mkdir-{}-{}",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let _ = std::fs::remove_dir_all(&ufs_dir);
    std::fs::create_dir_all(&ufs_dir)?;

    let mount_opts = MountOptions::builder()
        .write_type(WriteType::FsMode)
        .build();
    mount_manager.mount(
        None,
        "/mnt",
        format!("file://{}/", ufs_dir.display()).as_ref(),
        &mount_opts,
    )?;

    fs.mkdir("/mnt/db/table", true)?;
    journal_system.fs().fs_dir.read().take_entries();

    fs.mkdir("/mnt/db/table/log", false)?;
    let mkdir_entry = match journal_system
        .fs()
        .fs_dir
        .read()
        .take_entries()
        .into_iter()
        .find_map(|entry| match entry {
            JournalEntry::Mkdir(e) => Some(e),
            _ => None,
        }) {
        Some(entry) => entry,
        None => return err_box!("missing mkdir journal entry"),
    };

    assert!(!ufs_dir.join("db/table").exists());

    let loader = UfsLoader::new(journal_system.job_manager(), &conf.journal);
    let rt = AsyncRuntime::single();
    rt.block_on(async { loader.mkdir(&mkdir_entry).await })?;

    assert!(ufs_dir.join("db/table/log").is_dir());
    let _ = std::fs::remove_dir_all(&ufs_dir);

    Ok(())
}

// Test snapshot restart
#[test]
fn test_master_restart_with_snapshot_recovery() -> CommonResult<()> {
    Logger::default();
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    let worker = WorkerInfo::default();

    conf.change_test_meta_dir("meta-test-restart");
    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &js);
    let mnt_mgr = js.mount_manager();
    fs.add_test_worker(worker.clone());

    fs.mkdir("/a", false)?;
    run_mnt(mnt_mgr.clone())?;

    assert!(fs.exists("/a")?);

    let leader_mnt = mnt_mgr.get_mount_table().unwrap();
    assert_eq!(leader_mnt.len(), 1);

    // Create a snapshot manually.
    js.create_snapshot()?;

    drop(fs);
    drop(mnt_mgr);
    js.shutdown();

    conf.format_master = false;
    let js = reopen_journal_system(&conf)?;
    js.apply_snapshot()?;
    let fs = MasterFilesystem::with_js(&conf, &js);
    let mnt_mgr = js.mount_manager();
    fs.add_test_worker(worker.clone());
    assert!(fs.exists("/a")?);
    let leader_mnt = mnt_mgr.get_mount_table().unwrap();
    assert_eq!(leader_mnt.len(), 1);

    drop(fs);
    drop(mnt_mgr);
    js.shutdown();

    Ok(())
}

fn empty_checkpoint_snapshot(empty_dir: &str) -> SnapshotData {
    let fsm_state = FsmState {
        applied: AppliedIndex {
            term: 1,
            index: 1,
            op_id: 0,
            rpc_id: 0,
        },
        ufs_applied: AppliedIndex {
            term: 1,
            index: 1,
            op_id: 0,
            rpc_id: 0,
        },
    };
    SnapshotData {
        snapshot_id: 1,
        node_id: 1,
        create_time: 0,
        bytes_data: None,
        files_data: Some(SnapshotFileList {
            dir: empty_dir.to_string(),
            files: vec![],
        }),
        fsm_state,
    }
}

// Refuse empty checkpoint over a FS that already has files; allow when file_count == 0
// even if directories exist (tuple order: get_file_counts -> (dir_count, file_count)).
#[test]
fn test_apply_snapshot_refuses_empty_over_populated_files() -> CommonResult<()> {
    Logger::default();
    Master::init_test_metrics();

    let test_id = Utils::rand_str(8);
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("meta-empty-snap-refuse-{}", test_id));

    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &js);
    fs.add_test_worker(WorkerInfo::default());
    let loader = js.journal_loader();
    let rt = AsyncRuntime::single();

    // Directories only: guard must not refuse (file_count == 0).
    fs.mkdir("/only-dirs/nested", true)?;
    let (dir_count, file_count) = fs.get_file_counts();
    assert!(
        dir_count > 0,
        "expected dirs after mkdir, got {}",
        dir_count
    );
    assert_eq!(file_count, 0);

    let empty_dirs = Utils::test_sub_dir(format!("empty-snap-dirs-{}", test_id));
    FileUtils::create_dir(&empty_dirs, true)?;
    assert_eq!(FileUtils::dir_size(&empty_dirs).unwrap_or(1), 0);

    let dirs_only = rt.block_on(loader.apply_snapshot(empty_checkpoint_snapshot(&empty_dirs)));
    if let Err(e) = &dirs_only {
        let msg = e.to_string();
        assert!(
            !msg.contains("refusing to apply empty snapshot"),
            "dirs-only FS must not hit the empty-snapshot guard: {}",
            msg
        );
    }

    // Rebuild a populated FS with files: empty checkpoint must be refused.
    fs.mkdir("/with-files", true)?;
    fs.create("/with-files/file.log", false)?;
    let (dir_count, file_count) = fs.get_file_counts();
    assert!(file_count > 0, "expected files after create");

    let empty_files = Utils::test_sub_dir(format!("empty-snap-files-{}", test_id));
    FileUtils::create_dir(&empty_files, true)?;
    assert_eq!(FileUtils::dir_size(&empty_files).unwrap_or(1), 0);

    let err = rt
        .block_on(loader.apply_snapshot(empty_checkpoint_snapshot(&empty_files)))
        .expect_err("empty snapshot over populated files must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("refusing to apply empty snapshot"),
        "unexpected error: {}",
        msg
    );
    assert!(
        msg.contains(&format!("{} files", file_count))
            && msg.contains(&format!("{} dirs", dir_count)),
        "error should report (file_count, dir_count)=({}, {}); got: {}",
        file_count,
        dir_count,
        msg
    );

    drop(fs);
    js.shutdown();
    Ok(())
}
