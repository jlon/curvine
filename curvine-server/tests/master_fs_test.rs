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

use curvine_config::{ClusterConf, MasterConf};
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_fs_api::RpcCode;
use curvine_fs_api::{CurvineURI, Path};
use curvine_model::ListOptions;
use curvine_model::MountOptions;
use curvine_model::ProtoUtils;
use curvine_model::{
    BlockLocation, BlockReportInfo, BlockReportList, BlockReportStatus, ClientAddress, CommitBlock,
    CreateFileOpts, CreateFileOptsBuilder, DirectoryAttributes, FileAllocOpts, FileLock,
    LocatedBlock, LockFlags, LockType, MkdirOptsBuilder, OpenFlags, RenameFlags,
    SetAttrOptsBuilder, StorageType, TtlAction, WorkerAddress, WorkerInfo,
};
use curvine_proto::{
    CompleteFileRequest, CompleteFileResponse, CreateFileRequest, DeleteRequest,
    GetMasterInfoRequest, MkdirOptsProto, MkdirRequest, OpenFileRequest, RenameRequest,
};
use curvine_raft::conf::JournalConf;
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_rpc::handler::MessageHandler;
use curvine_rpc::message::Builder;
#[cfg(feature = "fault-injection")]
use curvine_rpc::message::ResponseStatus;
use curvine_runtime::common::LocalTime;
use curvine_runtime::common::SerdeUtils;
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_server::master::fs::{FsRetryCache, MasterFilesystem, OperationStatus};
use curvine_server::master::journal::{JournalBatch, JournalEntry, JournalLoader, JournalSystem};
use curvine_server::master::meta::inode::ttl::InodeTtlExecutor;
use curvine_server::master::meta::InodeId;
use curvine_server::master::replication::master_replication_manager::MasterReplicationManager;
use curvine_server::master::{JobHandler, JobManager, Master, MasterHandler, RpcContext};
use prost::Message as ProtoMessage;
use raft::eraftpb::Entry;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Semaphore;

#[cfg(feature = "fault-injection")]
use curvine_fault::{FaultRuleBuilder, FaultRuntime};

#[cfg(feature = "fault-injection")]
use curvine_fault::{FaultRuleBuilder, FaultRuntime};

// Master metrics gauges are process-wide; master_fs_test cases must not run in parallel or
// inode_file_num / inode_dir_num race with other tests' format/init.
static MASTER_FS_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

fn master_fs_test_serial() -> std::sync::MutexGuard<'static, ()> {
    MASTER_FS_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// Use a lightweight filesystem-only setup for tests that do not need the full
// journal runtime lifecycle.
fn new_fs(format: bool, name: &str) -> MasterFilesystem {
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: format,
        testing: true, // Enable testing mode to prevent background thread spawn
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("master-fs-test/meta-{}", name)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: Utils::test_sub_dir(format!(
                "master-fs-test/journal-{}-{}",
                name,
                Utils::rand_str(6)
            )),
            ..Default::default()
        },
        ..Default::default()
    };

    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.add_test_worker(WorkerInfo::default());
    fs
}

fn new_fs_with_journal(
    format: bool,
    name: &str,
) -> CommonResult<(MasterFilesystem, JournalSystem)> {
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: format,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("master-fs-test/meta-{}", name)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            // Reuse the same journal_dir across reopen phases so the test hits
            // the real RocksDB reopen path instead of a fresh directory.
            journal_dir: Utils::test_sub_dir(format!("master-fs-test/journal-{}", name)),
            ..Default::default()
        },
        ..Default::default()
    };

    let journal_system = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &journal_system);
    fs.add_test_worker(WorkerInfo::default());
    Ok((fs, journal_system))
}

fn reopen_fs_with_journal(
    format: bool,
    name: &str,
) -> CommonResult<(MasterFilesystem, JournalSystem)> {
    for _ in 0..50 {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            new_fs_with_journal(format, name)
        })) {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(e)) if e.to_string().contains("lock hold by current process") => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Ok(Err(e)) => return Err(e),
            Err(panic) if panic_message(&panic).contains("lock hold by current process") => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
    new_fs_with_journal(format, name)
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

fn file_counts(fs: &MasterFilesystem) -> (i64, i64) {
    fs.get_file_counts()
}

fn new_handler() -> MasterHandler {
    new_handler_for_test("retry")
}

fn full_commit(block: &LocatedBlock, len: i64) -> CommitBlock {
    CommitBlock {
        block_id: block.id,
        block_len: len,
        locations: block
            .locs
            .iter()
            .map(|worker| BlockLocation::new(worker.worker_id, block.storage_type))
            .collect(),
    }
}

fn prepare_flush_file(
    fs: &MasterFilesystem,
    path: &str,
    client: &ClientAddress,
    blocks: usize,
) -> CommonResult<(i64, CommitBlock)> {
    let status = fs.create(path, true)?;
    let mut last: Option<LocatedBlock> = None;

    for index in 0..blocks {
        let commits = last
            .as_ref()
            .map(|block| vec![full_commit(block, status.block_size)])
            .unwrap_or_default();
        let last_block = last.as_ref().map(|block| block.block.clone());
        last = Some(fs.add_block(
            path,
            None,
            client.clone(),
            commits,
            vec![],
            index as i64 * status.block_size,
            last_block,
        )?);
    }

    let last = last.expect("benchmark requires at least one block");
    Ok((
        blocks as i64 * status.block_size,
        full_commit(&last, status.block_size),
    ))
}

fn new_handler_for_test(test_name: &str) -> MasterHandler {
    Master::init_test_metrics();

    let test_id = Utils::rand_str(8);
    let mut conf = ClusterConf::format();
    conf.journal.enable = false;

    conf.master.meta_dir =
        Utils::test_sub_dir(format!("master-fs-test/meta-{test_name}-{test_id}"));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("master-fs-test/journal-{test_name}-{test_id}"));

    let journal_system = JournalSystem::from_conf(&conf).unwrap();
    let fs = MasterFilesystem::with_js(&conf, &journal_system);
    fs.add_test_worker(WorkerInfo::default());
    let retry_cache = FsRetryCache::with_conf(&conf.master)
        .expect("test master retry cache configuration should be valid");

    let mount_manager = journal_system.mount_manager();
    let rt = Arc::new(AsyncRuntime::single());
    let replication_manager =
        MasterReplicationManager::new(&fs, &conf, &rt, &journal_system.worker_manager())
            .expect("test master replication manager should initialize");
    let job_manager = Arc::new(JobManager::from_cluster_conf(
        fs.clone(),
        mount_manager.clone(),
        rt.clone(),
        &conf,
    ));
    let control_rpc_rt = Arc::new(AsyncRuntime::single());
    let metadata_read_rt = Arc::new(AsyncRuntime::single());
    MasterHandler::new(
        &conf,
        fs,
        retry_cache,
        None,
        mount_manager,
        JobHandler::new(job_manager),
        control_rpc_rt,
        Arc::new(Semaphore::new(1)),
        Arc::new(Semaphore::new(3)),
        Some(Arc::new(Semaphore::new(4))),
        Arc::new(Semaphore::new(2)),
        metadata_read_rt,
        Arc::new(Semaphore::new(1)),
        replication_manager,
        rt,
        Master::get_metrics().expect("test master metrics should initialize"),
    )
}

#[cfg(feature = "fault-injection")]
#[test]
fn test_master_sync_and_async_rpc_points_follow_dispatch_paths() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let faults = FaultRuntime::process();
    faults.clear()?;
    for (id, point, code) in [
        (
            "sync-dispatch",
            "master.rpc.before_sync_dispatch",
            RpcCode::Mkdir,
        ),
        (
            "async-dispatch",
            "master.rpc.before_async_dispatch",
            RpcCode::SubmitJob,
        ),
    ] {
        let rule = FaultRuleBuilder::named(point)
            .matches("rpc_code", code as i32)?
            .times(1)?
            .return_error(id)?;
        faults.configure(id, rule)?;
    }

    let handler = new_handler_for_test("fault-dispatch");
    let sync_request = Builder::new_rpc(RpcCode::Mkdir).build();
    let sync_response = handler.handle(&sync_request)?;

    let async_request = Builder::new_rpc(RpcCode::SubmitJob).build();
    let rt = AsyncRuntime::single();
    let async_response = rt.block_on(handler.async_handle(async_request))?;
    for response in [sync_response, async_response] {
        assert_eq!(response.response_status(), ResponseStatus::Error);
        assert!(matches!(
            response.check_error_ext::<FsError>(),
            Err(FsError::Common(_))
        ));
    }

    let status = faults.status();
    assert!(status.rules.iter().all(|rule| rule.executions == 1));
    faults.clear()?;
    Ok(())
}

#[test]
fn control_plane_requests_use_the_async_handler() {
    let _serial = master_fs_test_serial();
    let handler = new_handler();

    for code in [
        RpcCode::SubmitJob,
        RpcCode::GetJobStatus,
        RpcCode::CancelJob,
        RpcCode::ReportTask,
        RpcCode::FileStatus,
        RpcCode::Exists,
        RpcCode::ListStatus,
        RpcCode::ListOptions,
        RpcCode::GetBlockLocations,
        RpcCode::GetLock,
        RpcCode::GetMasterInfo,
        RpcCode::GetCvMetadataSnapshotPage,
        RpcCode::GetCvMetadataDeltaPage,
    ] {
        let msg = Builder::new_rpc(code).build();
        assert!(
            !handler.is_sync(&msg),
            "{code:?} must use the async handler"
        );
    }

    for code in [
        RpcCode::WorkerHeartbeat,
        RpcCode::WorkerBlockReport,
        RpcCode::ReportBlockReplicationResult,
    ] {
        let msg = Builder::new_rpc(code).build();
        assert!(handler.is_sync(&msg), "{code:?} must use the sync handler");
        assert!(
            handler.get_rt(&msg).is_some(),
            "{code:?} must use actor runtime"
        );
        assert_eq!(
            handler.request_admission(&msg).unwrap().available_permits(),
            2,
            "{code:?} must use control admission"
        );
    }

    let master_info = Builder::new_rpc(RpcCode::GetMasterInfo).build();
    assert!(!handler.is_sync(&master_info));
    assert_eq!(
        handler
            .request_admission(&master_info)
            .unwrap()
            .available_permits(),
        3,
        "GetMasterInfo must use client admission"
    );

    let mkdir = Builder::new_rpc(RpcCode::Mkdir).build();
    assert!(handler.is_sync(&mkdir));
    assert_eq!(
        handler
            .request_admission(&mkdir)
            .unwrap()
            .available_permits(),
        4,
        "blocking client requests must use blocking admission"
    );

    let file_status = Builder::new_rpc(RpcCode::FileStatus).build();
    assert!(!handler.is_sync(&file_status));
    assert_eq!(
        handler
            .request_admission(&file_status)
            .unwrap()
            .available_permits(),
        3,
        "async client requests must use client admission"
    );

    let read_open = Builder::new_rpc(RpcCode::OpenFile)
        .proto_header(OpenFileRequest {
            flags: OpenFlags::new_read_only().value(),
            ..Default::default()
        })
        .build();
    assert!(!handler.is_sync(&read_open));

    let write_open = Builder::new_rpc(RpcCode::OpenFile)
        .proto_header(OpenFileRequest {
            flags: OpenFlags::new_create().value(),
            ..Default::default()
        })
        .build();
    assert!(handler.is_sync(&write_open));
}

#[test]
fn sync_rpc_to_standby_returns_rpc_error_response() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let handler = new_handler();
    let msg = Builder::new_rpc(RpcCode::GetMasterInfo)
        .proto_header(GetMasterInfoRequest::default())
        .build();

    let response = handler.handle(&msg)?;

    assert!(!response.is_success());
    let err = response.check_error_ext::<FsError>().unwrap_err();
    assert!(matches!(err, FsError::NotLeaderMaster(_)));
    Ok(())
}

#[test]
fn test_master_filesystem_core_operations() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "fs_test");

    mkdir(&fs)?;
    delete(&fs)?;
    rename(&fs)?;
    create_file(&fs)?;
    get_file_info(&fs)?;
    list_status(&fs)?;
    state(&fs)?;

    Ok(())
}

#[test]
fn block_report_for_non_file_inode_schedules_worker_delete() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "block-report-non-file");
    fs.mkdir("/dir-block", true)?;
    let dir_status = fs.file_status("/dir-block")?;
    let block_id = InodeId::create_block_id(dir_status.id, 0)?;

    let result = fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 0,
            full_report: false,
            full_report_start: false,
            total_len: 1,
            blocks: vec![BlockReportInfo::new(
                block_id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                1,
            )],
        },
        None,
    )?;

    assert_eq!(result.delete_blocks, vec![block_id]);
    Ok(())
}

#[test]
fn block_report_for_writing_non_file_inode_defers_worker_delete() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "block-report-writing-non-file");
    fs.mkdir("/dir-block", true)?;
    let dir_status = fs.file_status("/dir-block")?;
    let block_id = InodeId::create_block_id(dir_status.id, 0)?;

    let result = fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 0,
            full_report: false,
            full_report_start: false,
            total_len: 1,
            blocks: vec![BlockReportInfo::new(
                block_id,
                BlockReportStatus::Writing,
                StorageType::Disk,
                1,
            )],
        },
        None,
    )?;

    assert!(result.delete_blocks.is_empty());
    Ok(())
}

#[test]
fn block_report_for_writing_missing_inode_defers_worker_delete() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "block-report-writing-missing");
    let file = fs.create("/missing", false)?;
    fs.delete("/missing", false)?;
    let block_id = InodeId::create_block_id(file.id, 0)?;

    let result = fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 0,
            full_report: false,
            full_report_start: false,
            total_len: 1,
            blocks: vec![BlockReportInfo::new(
                block_id,
                BlockReportStatus::Writing,
                StorageType::Disk,
                1,
            )],
        },
        None,
    )?;

    assert!(result.delete_blocks.is_empty());
    Ok(())
}

#[test]
fn full_block_report_for_writing_missing_inode_schedules_worker_delete() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "full-block-report-writing-missing");
    let file = fs.create("/missing", false)?;
    fs.delete("/missing", false)?;
    let block_id = InodeId::create_block_id(file.id, 0)?;

    let result = fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 0,
            full_report: true,
            full_report_start: false,
            total_len: 1,
            blocks: vec![BlockReportInfo::new(
                block_id,
                BlockReportStatus::Writing,
                StorageType::Disk,
                1,
            )],
        },
        None,
    )?;

    assert_eq!(result.delete_blocks, vec![block_id]);
    Ok(())
}

#[test]
fn full_block_report_reconcile_removes_stale_location_async() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "full-block-reconcile-async");
    let path = "/full-block-reconcile.log";
    let addr = ClientAddress::default();
    let status = fs.create(path, false)?;

    let first = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;
    let first_commit = CommitBlock {
        block_id: first.block.id,
        block_len: status.block_size,
        locations: vec![BlockLocation::with_id(100)],
    };
    let second = fs.add_block(
        path,
        None,
        addr,
        vec![first_commit],
        vec![],
        status.block_size,
        Some(first.block.clone()),
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: false,
            full_report_start: false,
            total_len: 0,
            blocks: vec![
                BlockReportInfo::new(
                    first.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    first.block.len,
                ),
                BlockReportInfo::new(
                    second.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    second.block.len,
                ),
            ],
        },
        None,
    )?;

    let before = fs.get_block_locations(path)?;
    assert_eq!(before.block_locs.len(), 2);
    assert!(!before.block_locs[1].locs.is_empty());

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            full_report_start: false,
            total_len: 1,
            blocks: vec![BlockReportInfo::new(
                first.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                first.block.len,
            )],
        },
        None,
    )?;

    for _ in 0..50 {
        if fs.get_block_locations_by_id(second.block.id)?.is_empty() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    panic!(
        "stale worker location for block {} was not reconciled: {:?}",
        second.block.id,
        fs.get_block_locations_by_id(second.block.id)?
    );
}

#[test]
fn incremental_report_invalidates_incomplete_full_report_session() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "full-block-report-invalidate-session");
    let path = "/full-block-report-invalidate-session.log";
    let addr = ClientAddress::default();
    let status = fs.create(path, false)?;

    let first = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;
    let first_commit = CommitBlock {
        block_id: first.block.id,
        block_len: status.block_size,
        locations: vec![BlockLocation::with_id(100)],
    };
    let second = fs.add_block(
        path,
        None,
        addr.clone(),
        vec![first_commit],
        vec![],
        status.block_size,
        Some(first.block.clone()),
    )?;
    let second_commit = CommitBlock {
        block_id: second.block.id,
        block_len: status.block_size,
        locations: vec![BlockLocation::with_id(100)],
    };
    let third = fs.add_block(
        path,
        None,
        addr,
        vec![second_commit],
        vec![],
        status.block_size * 2,
        Some(second.block.clone()),
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: false,
            full_report_start: false,
            total_len: 0,
            blocks: vec![
                BlockReportInfo::new(
                    first.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    first.block.len,
                ),
                BlockReportInfo::new(
                    second.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    second.block.len,
                ),
                BlockReportInfo::new(
                    third.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    third.block.len,
                ),
            ],
        },
        None,
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            full_report_start: false,
            total_len: 2,
            blocks: vec![BlockReportInfo::new(
                first.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                first.block.len,
            )],
        },
        None,
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: false,
            full_report_start: false,
            total_len: 0,
            blocks: vec![BlockReportInfo::new(
                second.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                second.block.len,
            )],
        },
        None,
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            full_report_start: false,
            total_len: 2,
            blocks: vec![BlockReportInfo::new(
                third.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                third.block.len,
            )],
        },
        None,
    )?;

    for _ in 0..50 {
        let blocks = fs.get_block_locations(path)?;
        let protected = blocks
            .block_locs
            .iter()
            .find(|block| block.block.id == second.block.id)
            .expect("second block metadata should remain");
        assert!(
            !protected.locs.is_empty(),
            "incremental report should protect block {} from stale full-report reconciliation",
            second.block.id
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    Ok(())
}

#[test]
fn ttl_executor_deletes_nested_expired_inode() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "ttl-executor-nested-delete");
    let opts = CreateFileOptsBuilder::new()
        .create_parent(true)
        .ttl_ms(1)
        .ttl_action(TtlAction::Delete)
        .build();
    let status = fs.create_with_opts(
        "/ttl/a/b/file.log",
        opts,
        OpenFlags::new_create().set_overwrite(true),
    )?;

    std::thread::sleep(Duration::from_millis(10));
    let executor = InodeTtlExecutor::with_managers(fs.clone());
    let (processed, inode) = executor.execute_by_id(status.id)?;

    assert!(
        processed,
        "expired inode should be processed by TTL executor"
    );
    assert_eq!(inode.id(), status.id);
    assert!(
        fs.file_status("/ttl/a/b/file.log").is_err(),
        "TTL delete should remove the nested file path resolved from inode id"
    );

    Ok(())
}

// Regression: TTL path resolution must not re-acquire the fs_dir read lock while
// already holding it. std::sync::RwLock is writer-preferring, so a reentrant read
// deadlocks once a writer is queued. This reproduces the 2026-07-08 freeze shape:
// a deep path resolved under continuous concurrent writers. It must complete
// within the timeout.
#[test]
fn ttl_path_resolution_no_reentrant_deadlock_under_writers() -> CommonResult<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "ttl-reentrant-deadlock");

    let mut deep_path = String::new();
    for level in 0..12 {
        deep_path.push_str(&format!("/d{}", level));
    }
    deep_path.push_str("/file.log");

    let opts = CreateFileOptsBuilder::new()
        .create_parent(true)
        .ttl_ms(1)
        .ttl_action(TtlAction::Delete)
        .build();
    let status = fs.create_with_opts(
        &deep_path,
        opts,
        OpenFlags::new_create().set_overwrite(true),
    )?;
    std::thread::sleep(Duration::from_millis(10));

    let stop = Arc::new(AtomicBool::new(false));
    let writer_fs = fs.clone();
    let writer_stop = stop.clone();
    let writer = std::thread::spawn(move || {
        let mut i = 0u64;
        while !writer_stop.load(Ordering::Relaxed) {
            let _ = writer_fs.mkdir(format!("/writer/{}", i), true);
            i += 1;
        }
    });

    let (tx, rx) = mpsc::channel();
    let exec_fs = fs.clone();
    let inode_id = status.id;
    let resolver = std::thread::spawn(move || {
        let executor = InodeTtlExecutor::with_managers(exec_fs);
        let _ = tx.send(executor.execute_by_id(inode_id));
    });

    let result = rx.recv_timeout(Duration::from_secs(10));
    stop.store(true, Ordering::Relaxed);
    let _ = writer.join();
    let _ = resolver.join();

    let (processed, inode) = result
        .expect("TTL path resolution deadlocked: no result within 10s under concurrent writers")?;
    assert!(processed, "expired inode should be processed");
    assert_eq!(inode.id(), status.id);
    assert!(
        fs.file_status(&deep_path).is_err(),
        "TTL delete should remove the deep path resolved from inode id"
    );

    Ok(())
}

#[test]
fn test_rename_posix_semantics() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "rename-posix");
    rename_posix_semantics(&fs)?;
    Ok(())
}

#[test]
fn test_rpc_retry_cache_for_idempotent_operations() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let mut handler = new_handler();
    let fs = handler.clone_fs();

    create_file_retry(&mut handler).unwrap();
    add_block_retry(&fs).unwrap();
    complete_file_retry(&fs).unwrap();
    delete_file_retry(&mut handler).unwrap();
    rename_retry(&mut handler).unwrap();

    Ok(())
}

#[test]
fn test_filesystem_metadata_persistence_and_restore() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    // First phase: create metadata and persist to RocksDB
    let hash1 = {
        let fs = new_fs(true, "restore");
        fs.mkdir("/a", false)?;
        fs.mkdir("/x1/x2/x3", true)?;
        let hash = fs.sum_hash()?;
        drop(fs);
        hash
    }; // Scope ensures all resources are dropped before reopening DB

    // Second phase: restore from persisted metadata
    let fs = new_fs(false, "restore");
    fs.restore_from_rocksdb()?;

    assert!(fs.exists("/a")?);
    assert!(fs.exists("/x1/x2/x3")?);
    let hash2 = fs.sum_hash()?;
    assert_eq!(hash1, hash2);

    Ok(())
}

#[test]
fn directory_attributes_survive_rocksdb_restore() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let test_name = format!("directory-attributes-{}", Utils::rand_str(6));
    let (root_before, parent_before) = {
        let fs = new_fs(true, &test_name);
        fs.mkdir("/parent/first", true)?;
        fs.mkdir("/parent/second", false)?;
        fs.delete("/parent/first", false)?;
        (fs.file_status("/")?, fs.file_status("/parent")?)
    };

    let fs = new_fs(false, &test_name);
    fs.restore_from_rocksdb()?;
    let root_after = fs.file_status("/")?;
    let parent_after = fs.file_status("/parent")?;

    assert_eq!(root_after.mtime, root_before.mtime);
    assert_eq!(root_after.nlink, root_before.nlink);
    assert_eq!(parent_after.mtime, parent_before.mtime);
    assert_eq!(parent_after.nlink, parent_before.nlink);
    Ok(())
}

#[test]
fn test_filesystem_metadata_restore_with_full_journal_system_reopen() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let test_name = format!("restore-full-{}", Utils::rand_str(6));
    let hash1 = {
        let (fs, js) = new_fs_with_journal(true, &test_name)?;
        fs.mkdir("/a", false)?;
        fs.mkdir("/x1/x2/x3", true)?;
        let hash = fs.sum_hash()?;
        drop(fs);
        js.shutdown();
        hash
    };

    let (fs, js) = reopen_fs_with_journal(false, &test_name)?;
    fs.restore_from_rocksdb()?;

    assert!(fs.exists("/a")?);
    assert!(fs.exists("/x1/x2/x3")?);
    assert_eq!(hash1, fs.sum_hash()?);

    drop(fs);
    js.shutdown();

    Ok(())
}

#[test]
fn test_exchange_parent_attributes_survive_rocksdb_reopen() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let test_name = format!("exchange-parent-attrs-{}", Utils::rand_str(6));
    let expected = {
        let fs = new_fs(true, &test_name);
        fs.mkdir("/left/dir_entry", true)?;
        fs.mkdir("/right", true)?;
        fs.create("/right/file_entry", false)?;
        std::thread::sleep(Duration::from_millis(2));
        fs.rename(
            "/left/dir_entry",
            "/right/file_entry",
            RenameFlags::EXCHANGE,
        )?;
        let left = fs.file_status("/left")?;
        let right = fs.file_status("/right")?;
        let expected = (left.mtime, left.nlink, right.mtime, right.nlink);
        drop(fs);
        expected
    };

    let fs = new_fs(false, &test_name);
    fs.restore_from_rocksdb()?;
    let left = fs.file_status("/left")?;
    let right = fs.file_status("/right")?;
    assert_eq!((left.mtime, left.nlink, right.mtime, right.nlink), expected);
    assert!(!fs.file_status("/left/dir_entry")?.is_dir);
    assert!(fs.file_status("/right/file_entry")?.is_dir);
    Ok(())
}

#[test]
fn metadata_replica_reader_tracks_namespace_updates_and_restore() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let test_name = format!("metadata-replica-{}", Utils::rand_str(6));
    let fs = new_fs(true, &test_name);

    fs.mkdir("/source", true)?;
    fs.mkdir("/target", true)?;
    let created = fs.create("/source/file", false)?;
    assert_eq!(fs.file_status("/source/file")?.id, created.id);
    assert_eq!(fs.list_status("/source")?.len(), 1);

    assert!(fs.rename("/source/file", "/target/file", RenameFlags::NO_REPLACE)?);
    assert!(!fs.exists("/source/file")?);
    assert_eq!(fs.file_status("/target/file")?.id, created.id);
    let listed = fs.list_options("/target", ListOptions::with_limit(1))?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "file");

    fs.restore_from_rocksdb()?;
    assert!(fs.exists("/target/file")?);
    assert_eq!(fs.file_status("/target/file")?.id, created.id);

    assert!(fs.delete("/target/file", false)?);
    assert!(!fs.exists("/target/file")?);
    assert!(fs.list_status("/target")?.is_empty());
    Ok(())
}

#[test]
fn metadata_status_cache_tracks_file_mutations_and_hard_links() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let test_name = format!("metadata-status-cache-{}", Utils::rand_str(6));
    let fs = new_fs(true, &test_name);

    fs.create("/source/file", true)?;
    let initial = fs.file_status("/source/file")?;

    let mtime = initial.mtime.saturating_add(1);
    let updated = fs.set_attr(
        "/source/file",
        SetAttrOptsBuilder::new().mtime(mtime).build(),
    )?;
    assert_eq!(updated.mtime, mtime);
    assert_eq!(fs.file_status("/source/file")?.mtime, mtime);

    fs.link("/source/file", "/links/alias")?;
    let source = fs.file_status("/source/file")?;
    let alias = fs.file_status("/links/alias")?;
    assert_eq!(source.id, alias.id);
    assert_eq!(source.nlink, 2);
    assert_eq!(alias.nlink, 2);
    assert_eq!(alias.name, "alias");

    assert!(fs.delete("/links/alias", false)?);
    assert_eq!(fs.file_status("/source/file")?.nlink, 1);

    fs.mkdir("/target", false)?;
    assert!(fs.rename("/source/file", "/target/renamed", RenameFlags::NO_REPLACE)?);
    assert_eq!(fs.file_status("/target/renamed")?.name, "renamed");

    fs.mkdir("/directory", false)?;
    let initial_directory = fs.file_status("/directory")?;
    fs.create("/directory/child", false)?;
    let directory = fs.file_status("/directory")?;
    assert_eq!(directory.children_num, 1);
    assert!(directory.mtime >= initial_directory.mtime);

    fs.set_attr(
        "/directory",
        SetAttrOptsBuilder::new().owner("directory_owner").build(),
    )?;
    let updated_directory = fs.file_status("/directory")?;
    assert_eq!(updated_directory.owner, "directory_owner");
    assert_eq!(
        updated_directory.ctime(),
        fs.file_status("/directory")?.ctime()
    );

    fs.create("/cached-source", true)?;
    fs.mkdir("/cached-tree", false)?;
    fs.link("/cached-source", "/cached-tree/alias")?;
    assert_eq!(fs.file_status("/cached-source")?.nlink, 2);
    assert!(fs.delete("/cached-tree", true)?);
    assert_eq!(fs.file_status("/cached-source")?.nlink, 1);

    fs.restore_from_rocksdb()?;
    let restored = fs.file_status("/target/renamed")?;
    assert_eq!(restored.id, initial.id);
    assert_eq!(restored.mtime, mtime);
    assert_eq!(restored.nlink, 1);
    Ok(())
}

#[test]
fn metadata_file_cache_invalidates_block_report_locations() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "metadata-file-cache-block-report");
    let path = "/cached-block.log";
    fs.create(path, false)?;

    let block = fs.add_block(
        path,
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    )?;
    let worker_id = block.locs[0].worker_id;

    // Populate the immutable inode cache before the worker changes its location.
    assert!(!fs.get_block_locations(path)?.block_locs[0].has_spdk);

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id,
            full_report: true,
            full_report_start: false,
            total_len: 1,
            blocks: vec![BlockReportInfo::new(
                block.block.id,
                BlockReportStatus::Finalized,
                StorageType::SpdkDisk,
                block.block.len,
            )],
        },
        None,
    )?;
    assert!(fs.get_block_locations(path)?.block_locs[0].has_spdk);

    let cleanup = fs.delete_locations(worker_id)?;
    assert_eq!(cleanup.removed_block_ids, vec![block.block.id]);
    assert!(fs.get_block_locations(path).is_err());
    Ok(())
}

#[test]
fn metadata_replica_reader_preserves_large_directory_order_after_updates() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let test_name = format!("metadata-replica-large-{}", Utils::rand_str(6));
    let fs = new_fs(true, &test_name);
    fs.mkdir("/directory", true)?;

    // Cross the bounded pending-edge threshold so listing exercises both the
    // merged ordered index and a live delta.
    for index in 0..4097 {
        fs.create(format!("/directory/file-{index:04}"), false)?;
    }

    let first_page = fs.list_options("/directory", ListOptions::with_limit(3))?;
    assert_eq!(
        first_page
            .iter()
            .map(|status| status.name.as_str())
            .collect::<Vec<_>>(),
        ["file-0000", "file-0001", "file-0002"]
    );

    assert!(fs.delete("/directory/file-1024", false)?);
    let page_after_delete = fs.list_options(
        "/directory",
        ListOptions {
            limit: Some(3),
            start_after: Some("file-1023".to_string()),
        },
    )?;
    assert_eq!(
        page_after_delete
            .iter()
            .map(|status| status.name.as_str())
            .collect::<Vec<_>>(),
        ["file-1025", "file-1026", "file-1027"]
    );

    fs.create("/directory/file-1024", false)?;
    let restored_page = fs.list_options(
        "/directory",
        ListOptions {
            limit: Some(3),
            start_after: Some("file-1023".to_string()),
        },
    )?;
    assert_eq!(
        restored_page
            .iter()
            .map(|status| status.name.as_str())
            .collect::<Vec<_>>(),
        ["file-1024", "file-1025", "file-1026"]
    );
    assert_eq!(fs.file_status("/directory")?.children_num, 4097);

    fs.restore_from_rocksdb()?;
    assert_eq!(fs.list_status("/directory")?.len(), 4097);
    Ok(())
}

#[test]
fn metadata_replica_reader_invalidates_cached_deleted_directory() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let test_name = format!("metadata-replica-cache-{}", Utils::rand_str(6));
    let fs = new_fs(true, &test_name);

    fs.create("/cached/directory/file", true)?;
    let first = fs.file_status("/cached/directory/file")?;
    assert!(fs.delete("/cached/directory", true)?);
    assert!(!fs.exists("/cached/directory/file")?);

    let recreated = fs.create("/cached/directory/file", true)?;
    assert_ne!(first.id, recreated.id);
    assert_eq!(fs.file_status("/cached/directory/file")?.id, recreated.id);
    Ok(())
}

fn mkdir(fs: &MasterFilesystem) -> CommonResult<()> {
    let res1 = fs.mkdir("/a/b", false);
    assert!(res1.is_err());

    let _ = fs.mkdir("/a1", true)?;
    let _ = fs.mkdir("/a2", true)?;

    let res2 = fs.mkdir("/a3/b/c", true);
    assert!(res2.is_ok());

    // Verify directories exist after creation
    assert!(fs.exists("/a1")?);
    assert!(fs.exists("/a2")?);
    assert!(fs.exists("/a3")?);
    assert!(fs.exists("/a3/b")?);
    assert!(fs.exists("/a3/b/c")?);

    let list = fs.list_status("/")?;
    assert_eq!(list.len(), 3);

    fs.print_tree();

    Ok(())
}

#[test]
fn mkdir_inherits_setgid_parent_group_and_mode() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "mkdir-setgid-inherit");

    let parent_opts = MkdirOptsBuilder::new()
        .owner("parent-owner".to_string())
        .group("parent-group".to_string())
        .mode(0o2775)
        .build();
    fs.mkdir_with_opts("/parent", parent_opts)?;

    let child_opts = MkdirOptsBuilder::new()
        .owner("child-owner".to_string())
        .group("child-group".to_string())
        .mode(0o775)
        .build();
    fs.mkdir_with_opts("/parent/child", child_opts)?;

    let child = fs.file_status("/parent/child")?;
    assert_eq!("parent-group", child.group);
    assert_eq!(0o2000, child.mode & 0o2000);
    assert_eq!(0o775, child.mode & 0o777);

    Ok(())
}

#[test]
fn mkdir_inherits_setgid_after_parent_set_attr() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "mkdir-setgid-after-setattr");
    fs.mkdir("/parent", true)?;

    fs.set_attr(
        "/parent",
        SetAttrOptsBuilder::new()
            .group("parent-group".to_string())
            .mode(0o2775)
            .build(),
    )?;
    fs.mkdir_with_opts(
        "/parent/child",
        MkdirOptsBuilder::new()
            .group("child-group".to_string())
            .mode(0o775)
            .build(),
    )?;

    let child = fs.file_status("/parent/child")?;
    assert_eq!("parent-group", child.group);
    assert_eq!(0o2000, child.mode & 0o2000);
    assert_eq!(0o775, child.mode & 0o777);

    Ok(())
}

fn delete(fs: &MasterFilesystem) -> CommonResult<()> {
    let res1 = fs.delete("/a", false);
    assert!(res1.is_err());

    fs.mkdir("/a/b/c/d", true)?;

    // Verify directory structure exists before deletion
    assert!(fs.exists("/a")?);
    assert!(fs.exists("/a/b")?);
    assert!(fs.exists("/a/b/c")?);
    assert!(fs.exists("/a/b/c/d")?);

    fs.delete("/a/b/c", true)?;

    // Verify deletion results
    assert!(!fs.exists("/a/b/c")?);
    assert!(!fs.exists("/a/b/c/d")?);
    // Parent directories should still exist
    assert!(fs.exists("/a")?);
    assert!(fs.exists("/a/b")?);

    fs.print_tree();
    Ok(())
}

fn rename(fs: &MasterFilesystem) -> CommonResult<()> {
    // Test directory rename
    fs.mkdir("/a/b/c", true)?;
    println!("=== Before directory rename ===");
    fs.print_tree();

    // Verify original paths exist
    assert!(fs.exists("/a/b/c")?);
    assert!(fs.exists("/a/b")?);
    assert!(fs.exists("/a")?);

    // Execute rename operation
    fs.rename("/a/b/c", "/a/x", RenameFlags::empty())?;

    println!("=== After directory rename ===");
    fs.print_tree();

    // Verify rename results
    // Original path should not exist
    assert!(!fs.exists("/a/b/c")?);
    // New path should exist
    assert!(fs.exists("/a/x")?);
    // Parent directory should still exist
    assert!(fs.exists("/a")?);
    // Intermediate directory b should still exist (since rename only moved c)
    assert!(fs.exists("/a/b")?);

    // Test file rename
    fs.create("/a.txt", true)?;

    println!("=== Before file rename ===");
    fs.print_tree();

    // Verify original file exists
    assert!(fs.exists("/a.txt")?);

    // Execute file rename operation
    fs.rename("/a.txt", "/aaa.txt", RenameFlags::empty())?;

    println!("=== After file rename ===");
    fs.print_tree();

    // Verify file rename results
    // Original file should not exist
    assert!(!fs.exists("/a.txt")?);
    // New file should exist
    assert!(fs.exists("/aaa.txt")?);

    Ok(())
}

fn rename_posix_semantics(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.mkdir("/a/b", true)?;

    // Test file rename to existing directory: POSIX rename must fail with EISDIR.
    fs.create("/a/1.log", true)?;

    let err = fs
        .rename("/a/1.log", "/a/b", RenameFlags::empty())
        .expect_err("rename file to directory must fail");
    assert!(matches!(err, FsError::IsADirectory(_)));
    assert!(fs.exists("/a/1.log")?);
    assert!(fs.exists("/a/b")?);
    assert!(!fs.exists("/a/b/1.log")?);

    // src file, dst file -> overwrite existing file.
    fs.create("/a/old.log", true)?;
    fs.create("/a/new.log", true)?;
    fs.rename("/a/old.log", "/a/new.log", RenameFlags::empty())?;
    assert!(!fs.exists("/a/old.log")?);
    assert!(fs.exists("/a/new.log")?);

    // src directory, dst empty directory -> overwrite empty directory.
    fs.mkdir("/a/src_dir", true)?;
    fs.create("/a/src_dir/child.txt", true)?;
    fs.mkdir("/a/empty_dir", true)?;
    fs.rename("/a/src_dir", "/a/empty_dir", RenameFlags::empty())?;
    assert!(!fs.exists("/a/src_dir")?);
    assert!(fs.exists("/a/empty_dir")?);
    assert!(fs.exists("/a/empty_dir/child.txt")?);

    // src directory, dst non-empty directory -> ENOTEMPTY.
    fs.mkdir("/a/rename_src", true)?;
    fs.create("/a/rename_src/file.txt", true)?;
    fs.mkdir("/a/rename_dst", true)?;
    fs.create("/a/rename_dst/keep.txt", true)?;

    let err = fs
        .rename("/a/rename_src", "/a/rename_dst", RenameFlags::empty())
        .expect_err("rename to non-empty directory must fail");
    assert!(matches!(err, FsError::DirNotEmpty(_)));
    assert!(fs.exists("/a/rename_src")?);
    assert!(fs.exists("/a/rename_src/file.txt")?);
    assert!(fs.exists("/a/rename_dst/keep.txt")?);
    assert!(!fs.exists("/a/rename_dst/rename_src")?);

    // src directory, dst file -> ENOTDIR.
    fs.mkdir("/a/dir_src", true)?;
    fs.create("/a/file_dst", true)?;
    let err = fs
        .rename("/a/dir_src", "/a/file_dst", RenameFlags::empty())
        .expect_err("rename directory to file must fail");
    assert!(matches!(err, FsError::NotADirectory(_)));
    assert!(fs.exists("/a/dir_src")?);
    assert!(fs.exists("/a/file_dst")?);

    // src directory -> dst under src: POSIX EINVAL.
    fs.mkdir("/a/rename_parent", true)?;
    let err = fs
        .rename(
            "/a/rename_parent",
            "/a/rename_parent/child",
            RenameFlags::empty(),
        )
        .expect_err("rename into subdirectory must fail");
    assert!(matches!(err, FsError::InvalidArgument(_)));
    assert!(fs.exists("/a/rename_parent")?);
    assert!(!fs.exists("/a/rename_parent/child")?);

    // same-path rename is a no-op for files and directories (including non-empty dirs).
    fs.create("/a/same_file.txt", true)?;
    fs.rename("/a/same_file.txt", "/a/same_file.txt", RenameFlags::empty())?;
    assert!(fs.exists("/a/same_file.txt")?);

    fs.mkdir("/a/same_empty_dir", true)?;
    fs.rename(
        "/a/same_empty_dir",
        "/a/same_empty_dir",
        RenameFlags::empty(),
    )?;
    assert!(fs.exists("/a/same_empty_dir")?);

    fs.mkdir("/a/same_full_dir", true)?;
    fs.create("/a/same_full_dir/child.txt", true)?;
    fs.rename("/a/same_full_dir", "/a/same_full_dir", RenameFlags::empty())?;
    assert!(fs.exists("/a/same_full_dir")?);
    assert!(fs.exists("/a/same_full_dir/child.txt")?);

    // RENAME_NOREPLACE: fail when dst exists, succeed when absent.
    fs.create("/a/noreplace_dst", true)?;
    fs.create("/a/noreplace_src", true)?;
    let err = fs
        .rename(
            "/a/noreplace_src",
            "/a/noreplace_dst",
            RenameFlags::NO_REPLACE,
        )
        .expect_err("no_replace must fail when dst exists");
    assert!(matches!(err, FsError::FileAlreadyExists(_)));
    assert!(fs.exists("/a/noreplace_src")?);
    assert!(fs.exists("/a/noreplace_dst")?);

    fs.rename(
        "/a/noreplace_src",
        "/a/noreplace_new",
        RenameFlags::NO_REPLACE,
    )?;
    assert!(!fs.exists("/a/noreplace_src")?);
    assert!(fs.exists("/a/noreplace_new")?);

    fs.create("/a/exchange_a", true)?;
    fs.create("/a/exchange_b", true)?;
    let id_a = fs.file_status("/a/exchange_a")?.id;
    let id_b = fs.file_status("/a/exchange_b")?.id;
    fs.rename("/a/exchange_a", "/a/exchange_b", RenameFlags::EXCHANGE)?;
    assert_eq!(fs.file_status("/a/exchange_a")?.id, id_b);
    assert_eq!(fs.file_status("/a/exchange_b")?.id, id_a);

    // EXCHANGE rejects src under dst (/a/b <-> /a would make /a its own descendant).
    fs.mkdir("/a/ex_parent", true)?;
    fs.mkdir("/a/ex_parent/child", true)?;
    let err = fs
        .rename("/a/ex_parent/child", "/a/ex_parent", RenameFlags::EXCHANGE)
        .expect_err("exchange with src under dst must fail");
    assert!(matches!(err, FsError::InvalidArgument(_)));
    assert!(fs.exists("/a/ex_parent")?);
    assert!(fs.exists("/a/ex_parent/child")?);

    // Cross-parent EXCHANGE between directory and file adjusts parent nlink.
    fs.mkdir("/a/ex_dir_parent", true)?;
    fs.mkdir("/a/ex_file_parent", true)?;
    fs.mkdir("/a/ex_dir_parent/dir_entry", true)?;
    fs.create("/a/ex_file_parent/file_entry", true)?;
    let dir_parent_nlink = fs.file_status("/a/ex_dir_parent")?.nlink;
    let file_parent_nlink = fs.file_status("/a/ex_file_parent")?.nlink;
    fs.rename(
        "/a/ex_dir_parent/dir_entry",
        "/a/ex_file_parent/file_entry",
        RenameFlags::EXCHANGE,
    )?;
    assert_eq!(
        fs.file_status("/a/ex_dir_parent")?.nlink,
        dir_parent_nlink - 1
    );
    assert_eq!(
        fs.file_status("/a/ex_file_parent")?.nlink,
        file_parent_nlink + 1
    );

    fs.create("/a/unsupported_src", true)?;
    let err = fs
        .rename(
            "/a/unsupported_src",
            "/a/unsupported_dst",
            RenameFlags::WHITEOUT,
        )
        .expect_err("whiteout must not degrade into a normal rename");
    assert!(matches!(err, FsError::Unsupported(_)));
    assert!(fs.exists("/a/unsupported_src")?);
    assert!(!fs.exists("/a/unsupported_dst")?);

    let err = fs
        .rename(
            "/a/unsupported_src",
            "/a/unsupported_dst",
            RenameFlags::EXCHANGE | RenameFlags::NO_REPLACE,
        )
        .expect_err("rename exchange combinations must be rejected");
    assert!(matches!(err, FsError::Unsupported(_)));
    assert!(fs.exists("/a/unsupported_src")?);
    assert!(!fs.exists("/a/unsupported_dst")?);

    // src symlink, dst symlink -> overwrite existing symlink and keep dst deletable.
    fs.symlink("nobody", "/a/symbolic", false, 0o777)?;
    fs.rename("/a/symbolic", "/a/asymbolic", RenameFlags::empty())?;
    assert!(!fs.exists("/a/symbolic")?);
    assert!(fs.exists("/a/asymbolic")?);

    fs.create("/a/object", true)?;
    fs.symlink("object", "/a/symbolic", false, 0o777)?;
    fs.rename("/a/symbolic", "/a/asymbolic", RenameFlags::empty())?;
    assert!(!fs.exists("/a/symbolic")?);
    assert!(fs.exists("/a/asymbolic")?);
    assert_eq!(
        fs.file_status("/a/asymbolic")?.target.as_deref(),
        Some("object")
    );
    assert!(fs.exists("/a/object")?);
    fs.delete("/a/asymbolic", false)?;
    assert!(!fs.exists("/a/asymbolic")?);
    fs.delete("/a/object", false)?;

    Ok(())
}

fn create_file(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.mkdir("/test_dir/subdir", true)?;

    // Verify directory exists before file creation
    assert!(fs.exists("/test_dir/subdir")?);

    fs.create("/test_dir/subdir/file1.log", false)?;
    fs.create("/test_dir/subdir/file2.log", false)?;

    // Verify files exist after creation
    assert!(fs.exists("/test_dir/subdir/file1.log")?);
    assert!(fs.exists("/test_dir/subdir/file2.log")?);
    // Verify directory still exists
    assert!(fs.exists("/test_dir/subdir")?);

    fs.print_tree();

    // overwrite file
    let oldid = fs.file_status("/test_dir/subdir/file1.log")?.id;
    let opts = CreateFileOpts::with_create(false);
    fs.create_with_opts(
        "/test_dir/subdir/file1.log",
        opts.clone(),
        OpenFlags::new_create().set_overwrite(true),
    )?;
    assert_eq!(oldid, fs.file_status("/test_dir/subdir/file1.log")?.id);

    fs.print_tree();
    Ok(())
}

fn get_file_info(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.create("/a/b/xx.log", true)?;
    fs.print_tree();

    let info = fs.file_status("/a/b/xx.log")?;
    println!("info = {:#?}", info);
    Ok(())
}

fn list_status_with_glob(fs: &MasterFilesystem) -> CommonResult<()> {
    // test 1
    let list_1 = fs
        .list_status("/*/*.log")
        .expect("list_1 failed to get status");
    assert_eq!(list_1.len(), 2, "Should find exactly 2 log files");

    // Sort for consistent ordering (if order not guaranteed)
    let mut sorted_list_1 = list_1.clone();
    sorted_list_1.sort_by(|a, b| a.name.cmp(&b.name));

    // Verify first file: /a/1.log
    assert_eq!(sorted_list_1[0].path, "/a/b1.log", "file path mismatch");
    assert_eq!(sorted_list_1[0].name, "b1.log", "file name mismatch");

    // Verify second file: /a/2.log
    assert_eq!(sorted_list_1[1].path, "/a/b2.log", "file path mismatch");
    assert_eq!(sorted_list_1[1].name, "b2.log", "file name mismatch");

    // test 2
    let list_2 = fs
        .list_status("/a/[ac]2.*")
        .expect("list_2 failed to get status");
    assert_eq!(list_2.len(), 1, "Should find exactly 1 log files");

    // Sort for consistent ordering (if order not guaranteed)
    let mut sorted_list_2 = list_2.clone();
    sorted_list_2.sort_by(|a, b| a.name.cmp(&b.name));

    // Verify second file: /a/c2.txt
    assert_eq!(sorted_list_2[0].path, "/a/c2.txt", "file path mismatch");
    assert_eq!(sorted_list_2[0].name, "c2.txt", "file name mismatch");

    // test 3: /a/* matches direct children; list_status expands matched dirs (e.g. /a/b -> xx.log).
    let list_3 = fs.list_status("/a/*").expect("list_3 failed to get status");
    assert_eq!(
        list_3.len(),
        5,
        "Should find exactly 5 entries for /a/* glob"
    );
    // Sort for consistent ordering (if order not guaranteed)
    let mut sorted_list_3 = list_3.clone();
    sorted_list_3.sort_by(|a, b| a.name.cmp(&b.name));

    assert_eq!(sorted_list_3[0].path, "/a/b1.log", "file path mismatch");
    assert_eq!(sorted_list_3[0].name, "b1.log", "file name mismatch");

    assert_eq!(sorted_list_3[1].path, "/a/b2.log", "file path mismatch");
    assert_eq!(sorted_list_3[1].name, "b2.log", "file name mismatch");

    assert_eq!(sorted_list_3[2].path, "/a/c1.txt", "file path mismatch");
    assert_eq!(sorted_list_3[2].name, "c1.txt", "file name mismatch");

    assert_eq!(sorted_list_3[3].path, "/a/c2.txt", "file path mismatch");
    assert_eq!(sorted_list_3[3].name, "c2.txt", "file name mismatch");

    assert_eq!(sorted_list_3[4].path, "/a/b/xx.log", "file path mismatch");
    assert_eq!(sorted_list_3[4].name, "xx.log", "file name mismatch");

    // test 4
    assert!(fs.list_status("/a/[a").is_err());

    let list_5 = fs.list_status("/*").expect("list_5 failed to get status");
    assert_eq!(list_5.len(), 11, "should find exactly 11 log files");

    Ok(())
}

fn list_status_without_glob(fs: &MasterFilesystem) -> CommonResult<()> {
    // Verify directories exist after creation
    assert!(fs.exists("/a1")?);
    assert!(fs.exists("/a2")?);
    assert!(fs.exists("/a3")?);
    assert!(fs.exists("/a3/b")?);
    assert!(fs.exists("/a3/b/c")?);

    let list = fs.list_status("/")?;
    assert_eq!(list.len(), 6);

    Ok(())
}

fn list_status(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.create("/a/b1.log", true)?;
    fs.create("/a/b2.log", true)?;
    fs.create("/a/c1.txt", true)?;
    fs.create("/a/c2.txt", true)?;

    fs.mkdir("/a/d1", true)?;
    fs.mkdir("/a/d2", true)?;

    assert!(fs.mkdir("/a/b", false).is_err());

    fs.mkdir("/a1", true)?;
    fs.mkdir("/a2", true)?;

    assert!(fs.mkdir("/a3/b/c", true).is_ok());

    fs.print_tree();

    let _ = list_status_with_glob(fs);
    let _ = list_status_without_glob(fs);
    Ok(())
}

#[test]
fn test_hardlink_to_dangling_symlink_inode() -> CommonResult<()> {
    // LTP link01 case 2: hard-link a symlink whose target does not exist.
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "link_dangling_symlink");
    fs.mkdir("/a", true)?;
    fs.symlink("object", "/a/symbolic", false, 0o777)?;
    assert!(fs.exists("/a/symbolic")?);
    assert!(!fs.exists("/a/object")?);

    fs.link("/a/symbolic", "/a/nick")?;
    assert!(fs.exists("/a/nick")?);

    let symlink = fs.file_status("/a/symbolic")?;
    let nick = fs.file_status("/a/nick")?;
    assert_eq!(symlink.id, nick.id);
    assert_eq!(symlink.nlink, 2);
    assert_eq!(nick.nlink, 2);
    assert_eq!(symlink.file_type, curvine_model::FileType::Link);
    assert_eq!(nick.file_type, curvine_model::FileType::Link);
    Ok(())
}

#[test]
fn list_options_respects_start_after_and_limit() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "list-options-page");
    fs.create("/page/a.log", true)?;
    fs.create("/page/b.log", true)?;
    fs.create("/page/c.log", true)?;
    fs.create("/page/d.log", true)?;
    fs.create("/page/e.log", true)?;

    let page = fs.list_options(
        "/page",
        ListOptions {
            limit: Some(2),
            start_after: Some("b.log".to_string()),
        },
    )?;

    let names = page
        .iter()
        .map(|status| status.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["c.log", "d.log"]);
    Ok(())
}

#[test]
fn test_hardlink_creation_and_nlink_counting() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "link_test");
    fs.mkdir("/a/b", true)?;
    fs.create("/a/b/file.log", true)?;
    fs.print_tree();
    fs.link("/a/b/file.log", "/a/b/file2.log")?;
    assert!(fs.exists("/a/b/file2.log")?);
    assert_eq!(fs.file_status("/a/b/file2.log")?.name, "file2.log");
    fs.print_tree();

    fs.link("/a/b/file.log", "/a/d/file.log")?;
    assert!(fs.exists("/a/d/file.log")?);
    fs.print_tree();

    let inode1 = fs.file_status("/a/b/file.log")?.id;
    let inode2 = fs.file_status("/a/b/file2.log")?.id;
    let inode3 = fs.file_status("/a/d/file.log")?.id;
    assert_eq!(inode1, inode2);
    assert_eq!(inode1, inode3);

    //update to check all linked file attr is same
    let time = LocalTime::mills() as i64;
    let opts = SetAttrOptsBuilder::new().mtime(time).build();
    fs.set_attr("/a/b/file2.log", opts)?;
    fs.print_tree();
    let mtime_t = fs.file_status("/a/b/file2.log")?.mtime;
    assert_eq!(mtime_t, fs.file_status("/a/b/file.log")?.mtime);
    assert_eq!(mtime_t, fs.file_status("/a/d/file.log")?.mtime);

    let nlink_t = fs.file_status("/a/b/file.log")?.nlink;
    assert_eq!(nlink_t, 3);
    let nlink_t = fs.file_status("/a/b/file2.log")?.nlink;
    assert_eq!(nlink_t, 3);
    let nlink_t = fs.file_status("/a/d/file.log")?.nlink;
    assert_eq!(nlink_t, 3);

    fs.delete("/a/b/file.log", true)?;
    assert!(!fs.exists("/a/b/file.log")?);
    assert!(fs.exists("/a/b/file2.log")?);
    assert!(fs.exists("/a/d/file.log")?);
    fs.print_tree();

    let nlink_t = fs.file_status("/a/b/file2.log")?.nlink;
    assert_eq!(nlink_t, 2);
    let nlink_t = fs.file_status("/a/d/file.log")?.nlink;
    assert_eq!(nlink_t, 2);

    //rename file2.log
    fs.rename("/a/b/file2.log", "/a/b/file3.log", RenameFlags::empty())?;
    assert!(!fs.exists("/a/b/file2.log")?);
    assert!(fs.exists("/a/b/file3.log")?);
    fs.print_tree();

    //let nlink_t = fs.file_status("/a/b/file3.log")?.nlink;
    //assert_eq!(nlink_t, 2);

    Ok(())
}

#[test]
fn rename_between_hard_links_keeps_both_directory_entries() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "rename-hard-link-same-inode");

    fs.create("/source", true)?;
    fs.link("/source", "/alias")?;
    let source = fs.file_status("/source")?;
    let alias = fs.file_status("/alias")?;
    assert_eq!(source.id, alias.id);
    assert_eq!(source.nlink, 2);

    assert!(!fs.rename("/source", "/alias", RenameFlags::empty())?);
    let err = fs
        .rename("/source", "/alias", RenameFlags::NO_REPLACE)
        .expect_err("RENAME_NOREPLACE must reject an existing hard-link path");
    assert!(matches!(err, FsError::FileAlreadyExists(_)));
    let source = fs.file_status("/source")?;
    let alias = fs.file_status("/alias")?;
    assert_eq!(source.id, alias.id);
    assert_eq!(source.nlink, 2);
    assert_eq!(alias.nlink, 2);

    fs.restore_from_rocksdb()?;
    assert_eq!(fs.file_status("/source")?.nlink, 2);
    assert_eq!(fs.file_status("/alias")?.nlink, 2);

    fs.mkdir("/left", false)?;
    fs.mkdir("/right", false)?;
    fs.create("/left/source", false)?;
    fs.link("/left/source", "/right/alias")?;
    let err = fs
        .rename("/left/source", "/right/alias", RenameFlags::NO_REPLACE)
        .expect_err("RENAME_NOREPLACE must reject an existing cross-parent hard-link path");
    assert!(matches!(err, FsError::FileAlreadyExists(_)));
    assert_eq!(
        fs.file_status("/left/source")?.id,
        fs.file_status("/right/alias")?.id
    );
    Ok(())
}

#[test]
fn rename_directory_between_parents_updates_nlink() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "rename-directory-nlink");

    fs.mkdir("/source/child", true)?;
    fs.mkdir("/destination", false)?;
    assert_eq!(fs.file_status("/source")?.nlink, 3);
    assert_eq!(fs.file_status("/destination")?.nlink, 2);

    assert!(fs.rename(
        "/source/child",
        "/destination/child",
        RenameFlags::NO_REPLACE,
    )?);
    assert_eq!(fs.file_status("/source")?.nlink, 2);
    assert_eq!(fs.file_status("/destination")?.nlink, 3);

    fs.mkdir("/source/next", false)?;
    fs.mkdir("/destination/replaced", false)?;
    assert_eq!(fs.file_status("/source")?.nlink, 3);
    assert_eq!(fs.file_status("/destination")?.nlink, 4);
    assert!(fs.rename(
        "/source/next",
        "/destination/replaced",
        RenameFlags::empty(),
    )?);
    assert_eq!(fs.file_status("/source")?.nlink, 2);
    assert_eq!(fs.file_status("/destination")?.nlink, 4);

    fs.mkdir("/destination/left", false)?;
    fs.mkdir("/destination/right", false)?;
    assert_eq!(fs.file_status("/destination")?.nlink, 6);
    assert!(fs.rename(
        "/destination/left",
        "/destination/right",
        RenameFlags::empty(),
    )?);
    assert_eq!(fs.file_status("/destination")?.nlink, 5);

    fs.restore_from_rocksdb()?;
    assert_eq!(fs.file_status("/source")?.nlink, 2);
    assert_eq!(fs.file_status("/destination")?.nlink, 5);
    Ok(())
}

#[test]
fn rename_hard_link_preserves_backing_inode() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "rename-hard-link-backing-inode");

    fs.create("/source", true)?;
    fs.link("/source", "/alias")?;
    fs.rename("/alias", "/renamed", RenameFlags::NO_REPLACE)?;

    let source = fs.file_status("/source")?;
    let renamed = fs.file_status("/renamed")?;
    assert_eq!(source.id, renamed.id);
    assert_eq!(source.name, "source");
    assert_eq!(renamed.name, "renamed");
    assert_eq!(source.nlink, 2);
    assert_eq!(renamed.nlink, 2);

    fs.restore_from_rocksdb()?;
    let source = fs.file_status("/source")?;
    let renamed = fs.file_status("/renamed")?;
    assert_eq!(source.id, renamed.id);
    assert_eq!(source.name, "source");
    assert_eq!(renamed.name, "renamed");
    assert_eq!(source.nlink, 2);
    assert_eq!(renamed.nlink, 2);
    Ok(())
}

#[test]
fn hard_link_chain_and_unlink_keep_live_nlink_consistent() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "hard-link-chain-live-nlink");

    fs.create("/source", true)?;
    fs.link("/source", "/alias")?;
    fs.link("/alias", "/second")?;
    assert_eq!(fs.file_status("/source")?.nlink, 3);
    assert_eq!(fs.file_status("/alias")?.nlink, 3);
    assert_eq!(fs.file_status("/second")?.nlink, 3);

    fs.delete("/alias", false)?;
    assert_eq!(fs.file_status("/source")?.nlink, 2);
    assert_eq!(fs.file_status("/second")?.nlink, 2);

    fs.delete("/second", false)?;
    assert_eq!(fs.file_status("/source")?.nlink, 1);

    fs.restore_from_rocksdb()?;
    assert_eq!(fs.file_status("/source")?.nlink, 1);
    Ok(())
}

#[test]
fn forced_symlink_rewrite_updates_live_and_restored_target() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "forced-symlink-rewrite-target");

    fs.symlink("target-a", "/link", false, 0o777)?;
    assert_eq!(fs.file_status("/link")?.target.as_deref(), Some("target-a"));

    fs.symlink("target-b", "/link", true, 0o777)?;
    assert_eq!(fs.file_status("/link")?.target.as_deref(), Some("target-b"));

    fs.restore_from_rocksdb()?;
    assert_eq!(fs.file_status("/link")?.target.as_deref(), Some("target-b"));
    Ok(())
}

#[test]
fn forced_symlink_rewrite_preserves_hard_link_alias() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "forced-symlink-rewrite-hard-link");

    fs.symlink("target-a", "/link", false, 0o777)?;
    fs.link("/link", "/alias")?;

    fs.symlink("target-b", "/link", true, 0o777)?;
    let link = fs.file_status("/link")?;
    let alias = fs.file_status("/alias")?;
    assert_eq!(link.target.as_deref(), Some("target-b"));
    assert_eq!(alias.target.as_deref(), Some("target-a"));
    assert_ne!(link.id, alias.id);
    assert_eq!(link.nlink, 1);
    assert_eq!(alias.nlink, 1);

    fs.restore_from_rocksdb()?;
    assert_eq!(fs.file_status("/link")?.target.as_deref(), Some("target-b"));
    assert_eq!(
        fs.file_status("/alias")?.target.as_deref(),
        Some("target-a")
    );
    Ok(())
}

fn state(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.mkdir("/a/b", true)?;
    fs.mkdir("/a/c", true)?;
    fs.create("/a/file/1.log", true)?;
    fs.create("/a/file/2.log", true)?;

    fs.create("/a/rename/old.log", true)?;
    fs.rename("/a/rename/old.log", "/a/c/new.log", RenameFlags::empty())?;
    fs.delete("/a/file/2.log", true)?;

    fs.print_tree();
    let fs_dir = fs.fs_dir.read();
    let mem_hash = fs_dir.root_dir().sum_hash()?;

    let state_tree = fs_dir.create_tree()?;
    state_tree.print_tree();
    let state_hash = state_tree.sum_hash()?;

    println!("mem_hash = {}, state_hash = {}", mem_hash, state_hash);
    assert_eq!(mem_hash, state_hash);

    Ok(())
}

fn create_file_retry(handler: &mut MasterHandler) -> CommonResult<()> {
    let req = CreateFileRequest {
        path: "/create_file_retry.log".to_string(),
        flags: OpenFlags::new_create().value(),
        ..Default::default()
    };
    let req_id = Utils::req_id();

    let msg = Builder::new_rpc(RpcCode::CreateFile)
        .req_id(req_id)
        .proto_header(req.clone())
        .build();

    assert!(handler.get_req_cache(req_id).is_none());

    let mut ctx = RpcContext::new(&msg);
    let _ = handler.retry_check_create_file(&mut ctx)?;

    assert_eq!(
        handler.get_req_cache(req_id).unwrap(),
        OperationStatus::Success
    );
    let is_retry = handler.check_is_retry(req_id)?;
    assert!(is_retry);

    // Retry request is normal
    let _ = handler.retry_check_create_file(&mut ctx)?;

    Ok(())
}

fn add_block_retry(fs: &MasterFilesystem) -> CommonResult<()> {
    let path = "/add_block_retry.log";
    let addr = ClientAddress::default();
    let status = fs.create(path, false).unwrap();

    let b1 = fs
        .add_block(path, None, addr.clone(), vec![], vec![], 0, None)
        .unwrap();
    let b2 = fs
        .add_block(path, None, addr.clone(), vec![], vec![], 0, None)
        .unwrap();

    assert_eq!(b1.block.id, b2.block.id);

    let locs = fs.get_block_locations(path).unwrap();
    println!("locs = {:?}", locs);
    assert_eq!(locs.block_locs.len(), 1);

    // Get the first block info to use as last_block parameter
    let first_block = b1.block.clone();

    let commit = CommitBlock {
        block_id: first_block.id,
        block_len: status.block_size,
        locations: vec![BlockLocation {
            worker_id: b1.locs[0].worker_id,
            storage_type: Default::default(),
        }],
    };

    // Add second block with first block as last_block parameter
    let b1 = fs
        .add_block(
            path,
            None,
            addr.clone(),
            vec![commit.clone()],
            vec![],
            status.block_size,
            Some(first_block.clone()), // Specify we want block after first_block
        )
        .unwrap();
    let b2 = fs
        .add_block(
            path,
            None,
            addr.clone(),
            vec![commit],
            vec![],
            status.block_size,
            Some(first_block), // Retry with same parameters (should return b1)
        )
        .unwrap();
    assert_eq!(b1.block.id, b2.block.id);

    let locs = fs.get_block_locations(path).unwrap();
    println!("locs = {:?}", locs);
    assert_eq!(locs.block_locs.len(), 2);

    Ok(())
}

fn complete_file_retry(fs: &MasterFilesystem) -> CommonResult<()> {
    let path = "/complete_file_retry.log";
    let addr = ClientAddress::default();
    fs.create(path, false)?;

    let b1 = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;

    let commit = CommitBlock {
        block_id: b1.block.id,
        block_len: b1.block.len,
        locations: vec![BlockLocation {
            worker_id: b1.locs[0].worker_id,
            storage_type: Default::default(),
        }],
    };

    let f1 = fs.complete_file(
        path,
        None,
        b1.block.len,
        vec![commit.clone()],
        &addr.client_name,
        false,
        None,
    );
    assert!(f1.is_ok());

    let f2 = fs.complete_file(
        path,
        None,
        b1.block.len,
        vec![commit.clone()],
        &addr.client_name,
        false,
        None,
    );
    assert!(f2.is_ok());

    let status = fs.file_status(path)?;
    println!("status = {:?}", status);
    assert!(status.is_complete);

    Ok(())
}

fn delete_file_retry(handler: &mut MasterHandler) -> CommonResult<()> {
    let msg = Builder::new_rpc(RpcCode::Mkdir)
        .proto_header(MkdirRequest {
            path: "/delete_file_retry".to_string(),
            opts: MkdirOptsProto {
                create_parent: false,
                ..Default::default()
            },
        })
        .build();

    let mut ctx = RpcContext::new(&msg);
    handler.mkdir(&mut ctx)?;

    let id = Utils::req_id();
    let req = DeleteRequest {
        path: "/delete_file_retry".to_string(),
        recursive: false,
    };

    let f1 = handler.delete0(id, req.clone())?;
    assert!(f1);

    let f2 = handler.delete0(id, req.clone())?;
    assert!(f2);

    Ok(())
}

fn rename_retry(handler: &mut MasterHandler) -> CommonResult<()> {
    let msg = Builder::new_rpc(RpcCode::Mkdir)
        .proto_header(MkdirRequest {
            path: "/rename_retry".to_string(),
            opts: MkdirOptsProto {
                create_parent: false,
                ..Default::default()
            },
        })
        .build();
    println!("msg: {:?}", msg);
    let mut ctx = RpcContext::new(&msg);
    handler.mkdir(&mut ctx)?;

    let id = Utils::req_id();
    let req = RenameRequest {
        src: "/rename_retry".to_string(),
        dst: "/rename_retry1".to_string(),
        flags: RenameFlags::empty().value(),
    };

    let f1 = handler.rename0(id, req.clone())?;
    println!("f1: {:?}", f1);
    assert!(f1);

    let f2 = handler.rename0(id, req.clone())?;
    println!("f2: {:?}", f2);
    assert!(f2);

    Ok(())
}

// Helper: creates a leader + follower pair, returns (leader_fs, leader_js, loader, follower_js)
fn setup_pair(
    name: &str,
) -> (
    MasterFilesystem,
    JournalSystem,
    JournalLoader,
    JournalSystem,
    MasterFilesystem,
) {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    let worker = WorkerInfo::default();

    conf.change_test_meta_dir(format!("idem-{}-leader", name));
    let js1 = JournalSystem::from_conf(&conf).unwrap();
    let fs1 = MasterFilesystem::with_js(&conf, &js1);
    fs1.add_test_worker(worker.clone());

    conf.change_test_meta_dir(format!("idem-{}-follower", name));
    let js2 = JournalSystem::from_conf(&conf).unwrap();
    let fs2 = MasterFilesystem::with_js(&conf, &js2);
    fs2.add_test_worker(worker);
    let loader = js2.journal_loader();

    (fs1, js1, loader, js2, fs2)
}

fn apply_entries(
    loader: &JournalLoader,
    entries: &[JournalEntry],
    start_index: u64,
) -> CommonResult<()> {
    let rt = AsyncRuntime::single();
    rt.block_on(async {
        for (offset, entry) in entries.iter().cloned().enumerate() {
            let index = start_index + offset as u64;
            let mut batch = JournalBatch::new(index);
            batch.push(entry);
            let entry = Entry {
                term: 1,
                index,
                data: SerdeUtils::serialize(&batch)?,
                ..Default::default()
            };
            loader.apply(true, ApplyMsg::new_entry(entry)).await?;
        }
        Ok(())
    })
}

/// Simulate follower replay through the real AppStorage apply path.
fn replay_all_then_duplicate_last(js: &JournalSystem, loader: &JournalLoader) -> CommonResult<()> {
    let entries = js.fs().fs_dir.read().take_entries();
    assert!(!entries.is_empty());

    apply_entries(loader, &entries, 1)?;

    let dup_start = entries.len() - 1;
    apply_entries(loader, &entries[dup_start..], entries.len() as u64)
}

#[test]
fn test_auto_snapshot_entry_after_journal_threshold() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir("master-fs-test/meta-auto-snapshot"),
            ..Default::default()
        },
        journal: JournalConf {
            enable: true,
            snapshot_entries: 1,
            writer_channel_size: 8,
            journal_dir: Utils::test_sub_dir("master-fs-test/journal-auto-snapshot"),
            ..Default::default()
        },
        ..Default::default()
    };

    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &js);
    fs.mkdir("/snapshot-trigger", false)?;

    let entries = js.fs().fs_dir.read().take_entries();
    assert_eq!(entries.len(), 2);
    assert!(matches!(entries[0], JournalEntry::Mkdir(_)));
    let JournalEntry::Snapshot(_) = &entries[1] else {
        panic!(
            "expected Snapshot entry after threshold, got {:?}",
            entries[1]
        );
    };
    assert!(entries[1].op_id() > entries[0].op_id());
    let checkpoint_path = js
        .fs()
        .fs_dir
        .read()
        .get_checkpoint_path(entries[1].op_id());
    assert!(std::path::Path::new(&checkpoint_path).exists());
    Ok(())
}

#[test]
fn checkpoint_restores_hot_directory_attributes() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "checkpoint-directory-attributes");
    fs.mkdir("/hot", true)?;
    for index in 0..256 {
        let path = format!("/hot/child-{index:04}");
        fs.mkdir(&path, false)?;
        fs.delete(&path, false)?;
    }

    let checkpoint = fs.fs_dir.read().create_checkpoint(1)?;
    fs.fs_dir.write().restore(&checkpoint, 0)?;

    let status = fs.file_status("/hot")?;
    assert_eq!(status.children_num, 0);
    assert_eq!(status.nlink, 2);
    Ok(())
}

#[test]
fn checkpoint_keeps_empty_directory_attributes_sparse() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "checkpoint-empty-directory-attributes");
    fs.mkdir("/empty", false)?;

    let before = fs.file_status("/empty")?;
    let checkpoint = fs.fs_dir.read().create_checkpoint(1)?;
    assert_eq!(
        fs.fs_dir
            .read()
            .get_rocks_store()
            .get_directory_attributes(before.id)?,
        None
    );

    fs.fs_dir.write().restore(&checkpoint, 0)?;
    let after = fs.file_status("/empty")?;
    assert_eq!(after.id, before.id);
    assert_eq!(after.nlink, before.nlink);
    Ok(())
}

#[test]
fn checkpoint_restore_initializes_empty_directory_attributes_on_first_child() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "checkpoint-empty-directory-first-child");
    fs.mkdir("/empty", false)?;

    let checkpoint = fs.fs_dir.read().create_checkpoint(1)?;
    fs.fs_dir.write().restore(&checkpoint, 0)?;
    fs.mkdir("/empty/child", false)?;

    let second_checkpoint = fs.fs_dir.read().create_checkpoint(2)?;
    fs.fs_dir.write().restore(&second_checkpoint, 0)?;

    let status = fs.file_status("/empty")?;
    assert_eq!(status.children_num, 1);
    assert_eq!(status.nlink, 3);
    assert!(fs.exists("/empty/child")?);
    Ok(())
}

#[test]
fn rename_empty_directory_persists_directory_attributes() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "rename-empty-directory-attributes");
    fs.mkdir("/source", false)?;
    fs.rename("/source", "/renamed", RenameFlags::empty())?;

    let status = fs.file_status("/renamed")?;
    let attributes = fs
        .fs_dir
        .read()
        .get_rocks_store()
        .get_directory_attributes(status.id)?;
    assert_eq!(
        attributes,
        Some(DirectoryAttributes::new(
            status.mtime,
            status.ctime(),
            status.nlink as u32,
        ))
    );
    Ok(())
}

#[test]
fn cross_parent_rename_preserves_directory_attributes_after_restore() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "cross-parent-rename-directory-attributes");
    fs.mkdir("/source/empty", true)?;
    fs.mkdir("/target", false)?;
    fs.rename("/source/empty", "/target/renamed", RenameFlags::empty())?;
    fs.mkdir("/target/renamed/child", false)?;

    fs.fs_dir.write().restore_from_rocksdb()?;

    let status = fs.file_status("/target/renamed")?;
    assert_eq!(status.children_num, 1);
    assert_eq!(status.nlink, 3);
    assert!(fs.exists("/target/renamed/child")?);
    Ok(())
}

#[test]
fn test_single_entry_journal_permit_uses_one_queue_slot() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir("master-fs-test/meta-journal-single-slot"),
            ..Default::default()
        },
        journal: JournalConf {
            enable: true,
            snapshot_entries: 0,
            writer_channel_size: 1,
            journal_dir: Utils::test_sub_dir("master-fs-test/journal-single-slot"),
            ..Default::default()
        },
        ..Default::default()
    };

    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &js);

    fs.mkdir("/single-slot", false)?;

    let err = fs.mkdir("/queue-full", false).unwrap_err();
    assert!(err.to_string().contains("journal writer queue is full"));
    assert!(matches!(
        fs.file_status("/queue-full").unwrap_err(),
        FsError::FileNotFound(_)
    ));

    let entries = js.fs().fs_dir.read().take_entries();
    assert_eq!(entries.len(), 1);
    assert!(matches!(entries[0], JournalEntry::Mkdir(_)));
    Ok(())
}

#[test]
fn test_namespace_write_fails_before_mutation_when_journal_queue_is_full() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir("master-fs-test/meta-journal-full"),
            ..Default::default()
        },
        journal: JournalConf {
            enable: true,
            snapshot_entries: 0,
            writer_channel_size: 3,
            journal_dir: Utils::test_sub_dir("master-fs-test/journal-full"),
            ..Default::default()
        },
        ..Default::default()
    };

    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &js);

    let mut blocked_path = None;
    for index in 0..16 {
        let path = format!("/queued-{index}");
        match fs.mkdir(&path, false) {
            Ok(_) => {}
            Err(err) => {
                assert!(err.to_string().contains("journal writer queue is full"));
                blocked_path = Some(path);
                break;
            }
        }
    }
    let blocked_path = blocked_path.expect("journal writer queue should become full");

    let err = fs.file_status(&blocked_path).unwrap_err();
    assert!(matches!(err, FsError::FileNotFound(_)));

    let entries = js.fs().fs_dir.read().take_entries();
    assert!(entries
        .iter()
        .all(|entry| matches!(entry, JournalEntry::Mkdir(_))));
    Ok(())
}

#[test]
fn test_mount_commit_failure_does_not_mutate_mount_table() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir("master-fs-test/meta-mount-journal-full"),
            ..Default::default()
        },
        journal: JournalConf {
            enable: true,
            snapshot_entries: 0,
            writer_channel_size: 4,
            journal_dir: Utils::test_sub_dir("master-fs-test/journal-mount-full"),
            ..Default::default()
        },
        ..Default::default()
    };

    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &js);
    let mnt_mgr = js.mount_manager();
    let mnt_opt = MountOptions::builder().build();
    fs.mkdir("/mnt", true)?;
    let mut queue_full = false;
    let mut filler_success = 0usize;
    for id in 1..16 {
        let cv_path = format!("/filler-{}", id);
        let ufs_path = format!("oss://filler-{}/", id);
        let filler = mnt_opt.clone().to_info(id, &cv_path, &ufs_path);
        if fs.commit_mount(filler).is_err() {
            queue_full = true;
            break;
        }
        filler_success += 1;
    }
    assert!(queue_full, "test setup should fill the journal queue");

    let err = mnt_mgr
        .mount(None, "/mnt", "oss://bucket/", &mnt_opt)
        .unwrap_err();
    assert!(err.to_string().contains("journal writer queue is full"));

    let mount_path = Path::from_str("/mnt")?;
    assert!(mnt_mgr.get_mount_info(&mount_path)?.is_none());

    let entries = js.fs().fs_dir.read().take_entries();
    assert_eq!(entries.len(), 1 + filler_success);
    Ok(())
}

#[test]
fn test_link_with_created_parents_reserves_enough_journal_permits() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir("master-fs-test/meta-link-journal-permits"),
            ..Default::default()
        },
        journal: JournalConf {
            enable: true,
            snapshot_entries: 0,
            writer_channel_size: 6,
            journal_dir: Utils::test_sub_dir("master-fs-test/journal-link-permits"),
            ..Default::default()
        },
        ..Default::default()
    };

    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &js);
    fs.create("/source.log", true)?;
    js.fs().fs_dir.read().take_entries();

    fs.link("/source.log", "/links/nested/source.log")?;

    let entries = js.fs().fs_dir.read().take_entries();
    assert_eq!(entries.len(), 3);
    assert!(matches!(entries[0], JournalEntry::Mkdir(_)));
    assert!(matches!(entries[1], JournalEntry::Mkdir(_)));
    assert!(matches!(entries[2], JournalEntry::Link(_)));
    assert!(fs.file_status("/links/nested/source.log").is_ok());
    Ok(())
}

#[test]
fn test_delete_locations_removes_worker_block_locations() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "delete-locations-batched");
    fs.create("/location-cleanup.log", true)?;
    let client = ClientAddress {
        client_name: "delete-locations-test".into(),
        hostname: "localhost".into(),
        ip_addr: "127.0.0.1".into(),
        port: 0,
    };

    let located = fs.add_block(
        "/location-cleanup.log",
        None,
        client,
        vec![],
        vec![],
        0,
        None,
    )?;
    let commit = CommitBlock {
        block_id: located.block.id,
        block_len: located.block.len,
        locations: located
            .locs
            .iter()
            .map(|worker| BlockLocation::new(worker.worker_id, located.block.storage_type))
            .collect(),
    };
    fs.complete_file(
        "/location-cleanup.log",
        None,
        located.block.len,
        vec![commit],
        "delete-locations-test",
        false,
        None,
    )?;
    let before = fs.get_block_locations("/location-cleanup.log")?;
    assert_eq!(before.block_locs.len(), 1);

    let deleted = fs.delete_locations(100)?;
    assert!(deleted.removed_block_ids.contains(&located.block.id));
    let err = fs.get_block_locations("/location-cleanup.log").unwrap_err();
    assert!(err.to_string().contains("Lost"));
    fs.add_block_location(
        located.block.id,
        BlockLocation::new(100, located.block.storage_type),
    )?;
    let restored = fs.get_block_locations("/location-cleanup.log")?;
    assert_eq!(restored.block_locs.len(), 1);
    Ok(())
}

#[test]
fn test_block_report_ignores_missing_block_locations() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "block-report-missing-block");
    let missing_block_id = InodeId::create_block_id(900_001, 0)?;

    let stale = BlockReportList {
        cluster_id: "curvine".into(),
        worker_id: 100,
        full_report: false,
        full_report_start: false,
        total_len: 0,
        blocks: vec![BlockReportInfo::new(
            missing_block_id,
            BlockReportStatus::Finalized,
            StorageType::Disk,
            0,
        )],
    };

    let result = fs.block_report(stale, None)?;
    assert_eq!(result.delete_blocks, vec![missing_block_id]);
    assert!(fs.get_block_locations_by_id(missing_block_id)?.is_empty());
    Ok(())
}

#[test]
fn test_full_block_report_does_not_delete_locations_committed_after_report_start(
) -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "full-block-report-candidate-race");
    let client = ClientAddress {
        client_name: "full-report-race".into(),
        hostname: "localhost".into(),
        ip_addr: "127.0.0.1".into(),
        port: 0,
    };

    let first_status = fs.create("/old.log", true)?;
    let first = fs.add_block("/old.log", None, client.clone(), vec![], vec![], 0, None)?;
    let first_commit = CommitBlock {
        block_id: first.block.id,
        block_len: first_status.block_size,
        locations: first
            .locs
            .iter()
            .map(|worker| BlockLocation::new(worker.worker_id, first.block.storage_type))
            .collect(),
    };
    let second = fs.add_block(
        "/old.log",
        None,
        client.clone(),
        vec![first_commit.clone()],
        vec![],
        first_status.block_size,
        Some(first.block.clone()),
    )?;
    let second_commit = CommitBlock {
        block_id: second.block.id,
        block_len: second.block.len,
        locations: second
            .locs
            .iter()
            .map(|worker| BlockLocation::new(worker.worker_id, second.block.storage_type))
            .collect(),
    };
    fs.complete_file(
        "/old.log",
        None,
        first_status.block_size + second.block.len,
        vec![second_commit],
        &client.client_name,
        false,
        None,
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            full_report_start: true,
            total_len: 0,
            blocks: vec![],
        },
        None,
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            full_report_start: false,
            total_len: 2,
            blocks: vec![BlockReportInfo::new(
                first.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                0,
            )],
        },
        None,
    )?;

    fs.create("/new.log", true)?;
    let new_block = fs.add_block("/new.log", None, client.clone(), vec![], vec![], 0, None)?;
    let new_commit = CommitBlock {
        block_id: new_block.block.id,
        block_len: new_block.block.len,
        locations: new_block
            .locs
            .iter()
            .map(|worker| BlockLocation::new(worker.worker_id, new_block.block.storage_type))
            .collect(),
    };
    fs.complete_file(
        "/new.log",
        None,
        new_block.block.len,
        vec![new_commit],
        &client.client_name,
        false,
        None,
    )?;

    let result = fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            full_report_start: false,
            total_len: 2,
            blocks: vec![BlockReportInfo::new(
                second.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                0,
            )],
        },
        None,
    )?;

    assert!(result.delete_blocks.is_empty());
    assert_eq!(fs.get_block_locations("/new.log")?.block_locs.len(), 1);
    Ok(())
}

#[test]
fn test_full_block_report_finish_ignores_newer_generation() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "full-block-report-generation-race");
    let client = ClientAddress {
        client_name: "full-report-generation".into(),
        hostname: "localhost".into(),
        ip_addr: "127.0.0.1".into(),
        port: 0,
    };

    fs.create("/generation-race.log", true)?;
    let located = fs.add_block(
        "/generation-race.log",
        None,
        client.clone(),
        vec![],
        vec![],
        0,
        None,
    )?;
    let commit = CommitBlock {
        block_id: located.block.id,
        block_len: located.block.len,
        locations: located
            .locs
            .iter()
            .map(|worker| BlockLocation::new(worker.worker_id, located.block.storage_type))
            .collect(),
    };
    fs.complete_file(
        "/generation-race.log",
        None,
        located.block.len,
        vec![commit],
        &client.client_name,
        false,
        None,
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            full_report_start: true,
            total_len: 0,
            blocks: vec![],
        },
        None,
    )?;

    let mut blocks = vec![BlockReportInfo::new(
        located.block.id,
        BlockReportStatus::Finalized,
        StorageType::Disk,
        0,
    )];
    for id in 10_000_000..10_020_000 {
        blocks.push(BlockReportInfo::new(
            InodeId::create_block_id(id, 0)?,
            BlockReportStatus::Finalized,
            StorageType::Disk,
            0,
        ));
    }
    let total_len = blocks.len() as u64;

    let keep_starting = Arc::new(AtomicBool::new(true));
    let sent_start = Arc::new(AtomicBool::new(false));
    let start_fs = fs.clone();
    let start_flag = Arc::clone(&keep_starting);
    let sent_flag = Arc::clone(&sent_start);
    let start_thread = std::thread::spawn(move || {
        while start_flag.load(Ordering::Acquire) {
            let _ = start_fs.block_report(
                BlockReportList {
                    cluster_id: "curvine".into(),
                    worker_id: 100,
                    full_report: true,
                    full_report_start: true,
                    total_len: 0,
                    blocks: vec![],
                },
                None,
            );
            sent_flag.store(true, Ordering::Release);
        }
    });

    let result = fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            full_report_start: false,
            total_len,
            blocks,
        },
        None,
    )?;
    keep_starting.store(false, Ordering::Release);
    start_thread.join().expect("start thread should not panic");

    assert!(sent_start.load(Ordering::Acquire));
    assert!(
        !result.delete_blocks.contains(&located.block.id),
        "a newer full report generation must not let an older finish delete live block locations"
    );
    assert_eq!(
        fs.get_block_locations("/generation-race.log")?
            .block_locs
            .len(),
        1
    );
    Ok(())
}

#[test]
fn test_legacy_full_block_report_after_worker_start_reconciles_stale_locations() -> CommonResult<()>
{
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "legacy-full-block-report-start");
    let client = ClientAddress {
        client_name: "legacy-full-report".into(),
        hostname: "localhost".into(),
        ip_addr: "127.0.0.1".into(),
        port: 0,
    };

    fs.create("/stale.log", true)?;
    let located = fs.add_block("/stale.log", None, client.clone(), vec![], vec![], 0, None)?;
    let commit = CommitBlock {
        block_id: located.block.id,
        block_len: located.block.len,
        locations: located
            .locs
            .iter()
            .map(|worker| BlockLocation::new(worker.worker_id, located.block.storage_type))
            .collect(),
    };
    fs.complete_file(
        "/stale.log",
        None,
        located.block.len,
        vec![commit],
        &client.client_name,
        false,
        None,
    )?;

    fs.begin_full_block_report(100);
    let result = fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            full_report_start: false,
            total_len: 0,
            blocks: vec![],
        },
        None,
    )?;

    assert!(result.delete_blocks.is_empty());
    for _ in 0..50 {
        if fs.get_block_locations_by_id(located.block.id)?.is_empty() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "stale location for block {} was not reconciled",
        located.block.id
    );
}

#[test]
fn test_idempotent_mkdir() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("mkdir");
    fs.mkdir("/data", false)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_create_file() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("create-file");
    fs.create("/file.log", true)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_delete() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("delete");
    fs.mkdir("/data", false)?;
    let after_mkdir = file_counts(&fs);
    eprintln!("delete counts after mkdir = {:?}", after_mkdir);
    fs.delete("/data", true)?;
    let after_delete = file_counts(&fs);
    eprintln!("delete counts after delete = {:?}", after_delete);
    replay_all_then_duplicate_last(&js, &loader)?;
    let leader_counts = file_counts(&fs);
    let follower_counts = file_counts(&fs2);
    eprintln!("delete leader counts after replay = {:?}", leader_counts);
    eprintln!(
        "delete follower counts after replay = {:?}",
        follower_counts
    );
    assert!(
        leader_counts.0 >= 0 && leader_counts.1 >= 0,
        "leader file counts must stay non-negative: {:?}",
        leader_counts
    );
    assert!(
        follower_counts.0 >= 0 && follower_counts.1 >= 0,
        "follower file counts must stay non-negative: {:?}",
        follower_counts
    );
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_rename() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("rename");
    fs.mkdir("/src", false)?;
    fs.rename("/src", "/dst", RenameFlags::empty())?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_exchange_rename() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("exchange-rename");
    fs.mkdir("/a", true)?;
    fs.create("/a/ex_a", true)?;
    fs.create("/a/ex_b", true)?;
    fs.rename("/a/ex_a", "/a/ex_b", RenameFlags::EXCHANGE)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_rename_link_and_forced_symlink() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("rename-link-symlink");

    fs.mkdir("/source/child", true)?;
    fs.mkdir("/destination", false)?;
    fs.rename(
        "/source/child",
        "/destination/child",
        RenameFlags::NO_REPLACE,
    )?;

    fs.create("/file", true)?;
    fs.link("/file", "/alias")?;
    fs.rename("/alias", "/renamed", RenameFlags::NO_REPLACE)?;

    fs.symlink("target-a", "/link", false, 0o777)?;
    fs.link("/link", "/link-alias")?;
    fs.symlink("target-b", "/link", true, 0o777)?;

    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    assert_eq!(fs.file_status("/source")?.nlink, 2);
    assert_eq!(fs2.file_status("/source")?.nlink, 2);
    assert_eq!(fs.file_status("/destination")?.nlink, 3);
    assert_eq!(fs2.file_status("/destination")?.nlink, 3);
    assert_eq!(fs.file_status("/file")?.nlink, 2);
    assert_eq!(fs2.file_status("/file")?.nlink, 2);
    assert_eq!(fs.file_status("/renamed")?.id, fs.file_status("/file")?.id);
    assert_eq!(
        fs2.file_status("/renamed")?.id,
        fs2.file_status("/file")?.id
    );
    assert_eq!(fs.file_status("/renamed")?.nlink, 2);
    assert_eq!(fs2.file_status("/renamed")?.nlink, 2);
    assert_eq!(fs.file_status("/link")?.target.as_deref(), Some("target-b"));
    assert_eq!(
        fs.file_status("/link-alias")?.target.as_deref(),
        Some("target-a")
    );
    assert_eq!(fs.file_status("/link-alias")?.nlink, 1);
    assert_eq!(
        fs2.file_status("/link")?.target.as_deref(),
        Some("target-b")
    );
    assert_eq!(
        fs2.file_status("/link-alias")?.target.as_deref(),
        Some("target-a")
    );
    assert_eq!(fs2.file_status("/link-alias")?.nlink, 1);
    Ok(())
}

#[test]
fn test_idempotent_free() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("free");
    fs.create("/file.log", true)?;
    // Set ufs_mtime > 0 so the free function passes the ufs_exists() check
    let set_opts = SetAttrOptsBuilder::new().ufs_mtime(1).build();
    fs.set_attr("/file.log", set_opts)?;
    fs.free("/file.log", false)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_set_attr() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("set-attr");
    fs.mkdir("/data", false)?;
    let before = fs.file_status("/data")?;
    let opts = SetAttrOptsBuilder::new().owner("test_owner").build();
    let status = fs.set_attr("/data", opts)?;
    assert_eq!(status.owner, "test_owner");
    let current = fs.file_status("/data")?;
    assert_eq!(current.owner, "test_owner");
    assert!(status.ctime() >= before.ctime());
    assert_eq!(current.ctime(), status.ctime());
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_unmount() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("unmount");
    let mnt_mgr = js.mount_manager();
    let mnt_opt = MountOptions::builder().build();
    mnt_mgr.mount(None, "/mnt/test", "oss://bucket/", &mnt_opt)?;
    mnt_mgr.umount("/mnt/test")?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_reject_s3_mount_without_region_before_persist() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, _loader, _js2, _fs2) = setup_pair("reject-s3-mount-without-region");
    let mnt_mgr = js.mount_manager();
    let mount_path = "/mnt/s3";
    let mount = Path::from_str(mount_path)?;
    let opts = MountOptions::builder()
        .add_property("s3.endpoint_url", "http://s3.example.com")
        .add_property("s3.credentials.access", "access-key")
        .add_property("s3.credentials.secret", "secret-key")
        .build();

    let err = mnt_mgr
        .mount(None, mount_path, "s3://bucket/path", &opts)
        .expect_err("S3 mount without s3.region_name must be rejected");

    assert!(err.to_string().contains("s3.region_name"));
    assert!(mnt_mgr.get_mount_info(&mount)?.is_none());
    assert!(!fs.exists(mount_path)?);
    Ok(())
}

#[test]
fn test_legacy_s3_mount_properties_are_canonicalized_before_persist() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, _loader, _js2, _fs2) = setup_pair("canonicalize-legacy-s3-mount");
    let mnt_mgr = js.mount_manager();
    let mount_path = "/mnt/s3";
    let mount = Path::from_str(mount_path)?;
    let opts = MountOptions::builder()
        .add_property("s3.endpoint_url", "http://s3.example.com")
        .add_property("s3.access_key", "access-key")
        .add_property("s3.secret_key", "secret-key")
        .add_property("s3.region", "cn-test-1")
        .build();

    mnt_mgr.mount(None, mount_path, "s3://bucket/path", &opts)?;

    let properties = &mnt_mgr
        .get_mount_info(&mount)?
        .expect("mount must be stored")
        .properties;
    assert_eq!(
        properties.get("s3.region_name").map(String::as_str),
        Some("cn-test-1")
    );
    assert_eq!(
        properties.get("s3.credentials.access").map(String::as_str),
        Some("access-key")
    );
    assert_eq!(
        properties.get("s3.credentials.secret").map(String::as_str),
        Some("secret-key")
    );
    assert!(!properties.contains_key("s3.region"));
    assert!(!properties.contains_key("s3.access_key"));
    assert!(!properties.contains_key("s3.secret_key"));
    assert!(fs.exists(mount_path)?);
    Ok(())
}

#[test]
fn test_reject_invalid_s3_mount_config_before_persist() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, _loader, _js2, _fs2) = setup_pair("reject-invalid-s3-mount-config");
    let mnt_mgr = js.mount_manager();
    let mount_path = "/mnt/s3";
    let mount = Path::from_str(mount_path)?;
    let opts = MountOptions::builder()
        .add_property("s3.endpoint_url", "http://s3.example.com")
        .add_property("s3.credentials.access", "access-key")
        .add_property("s3.credentials.secret", "secret-key")
        .add_property("s3.region_name", "cn-test-1")
        .add_property("s3.list_objects_version", "v3")
        .build();

    let err = mnt_mgr
        .mount(None, mount_path, "s3://bucket/path", &opts)
        .expect_err("invalid S3 provider configuration must be rejected");

    assert!(err.to_string().contains("s3.list_objects_version"));
    assert!(mnt_mgr.get_mount_info(&mount)?.is_none());
    assert!(!fs.exists(mount_path)?);
    Ok(())
}

#[test]
fn test_reject_invalid_s3_mount_update_before_persist() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (_fs, js, _loader, _js2, _fs2) = setup_pair("reject-invalid-s3-mount-update");
    let mnt_mgr = js.mount_manager();
    let mount_path = "/mnt/s3";
    let mount = Path::from_str(mount_path)?;
    let region = "cn-test-1";
    let opts = MountOptions::builder()
        .add_property("s3.endpoint_url", "http://s3.example.com")
        .add_property("s3.credentials.access", "access-key")
        .add_property("s3.credentials.secret", "secret-key")
        .add_property("s3.region_name", region)
        .build();

    mnt_mgr.unprotected_add_mount(opts.clone().to_info(1, mount_path, "s3://bucket/path"))?;

    let update = MountOptions::builder()
        .update(true)
        .add_property("s3.list_objects_version", "v3")
        .build();
    let err = mnt_mgr
        .mount(None, mount_path, "s3://bucket/path", &update)
        .expect_err("invalid S3 mount update must be rejected");

    assert!(err.to_string().contains("s3.list_objects_version"));
    assert_eq!(
        mnt_mgr
            .get_mount_info(&mount)?
            .expect("valid S3 mount must remain stored")
            .properties
            .get("s3.region_name")
            .map(String::as_str),
        Some(region)
    );
    Ok(())
}

#[test]
fn test_inode_file_num_stays_non_negative_for_symlink_create_delete() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "inode-file-num-symlink");

    let (dir_count, file_count) = file_counts(&fs);
    assert_eq!(dir_count, 0);
    assert_eq!(file_count, 0);

    fs.mkdir("/dir", false)?;
    let (dir_count, file_count) = file_counts(&fs);
    assert_eq!(dir_count, 1);
    assert_eq!(file_count, 0);

    fs.symlink("/target", "/dir/link", false, 0o777)?;
    let (dir_count_after_create, file_count_after_create) = file_counts(&fs);

    fs.delete("/dir/link", false)?;
    let (dir_count_after_delete, file_count_after_delete) = file_counts(&fs);

    assert_eq!(dir_count_after_create, dir_count_after_delete);
    assert_eq!(
        file_count_after_create,
        file_count_after_delete + 1,
        "symlink create/delete should change file count symmetrically"
    );
    assert!(
        file_count_after_delete >= 0,
        "inode_file_num must never be negative, got {}",
        file_count_after_delete
    );
    Ok(())
}

#[test]
fn test_inode_file_num_stable_on_forced_symlink_rewrite() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "inode-file-num-symlink-force");

    fs.mkdir("/dir", false)?;
    fs.symlink("/target-a", "/dir/link", false, 0o777)?;
    let file_count_after_first = file_counts(&fs).1;

    fs.symlink("/target-b", "/dir/link", true, 0o777)?;
    fs.symlink("/target-c", "/dir/link", true, 0o777)?;
    let file_count_after_rewrites = file_counts(&fs).1;

    assert_eq!(
        file_count_after_first, file_count_after_rewrites,
        "force symlink replace must not inflate inode_file_num (was {}, after rewrites {})",
        file_count_after_first, file_count_after_rewrites
    );
    Ok(())
}

#[test]
fn test_inode_file_num_stays_non_negative_when_renaming_over_symlink() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "inode-file-num-rename-over-link");

    fs.mkdir("/dir", false)?;
    fs.create("/dir/file.log", true)?;
    fs.symlink("/target", "/dir/link", false, 0o777)?;

    fs.rename("/dir/file.log", "/dir/link", RenameFlags::empty())?;

    let (_dir_count, file_count) = file_counts(&fs);
    assert!(
        file_count >= 0,
        "inode_file_num must never be negative after rename-overwrite, got {}",
        file_count
    );
    Ok(())
}

#[test]
fn test_idempotent_symlink() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("symlink");
    fs.mkdir("/dir", false)?;
    let after_mkdir = file_counts(&fs);
    eprintln!("symlink counts after mkdir = {:?}", after_mkdir);
    fs.symlink("/target", "/dir/link", false, 0o777)?;
    let after_create = file_counts(&fs);
    eprintln!("symlink counts after create = {:?}", after_create);
    replay_all_then_duplicate_last(&js, &loader)?;
    let leader_counts = file_counts(&fs);
    let follower_counts = file_counts(&fs2);
    eprintln!("symlink leader counts after replay = {:?}", leader_counts);
    eprintln!(
        "symlink follower counts after replay = {:?}",
        follower_counts
    );
    assert!(
        leader_counts.0 >= 0 && leader_counts.1 >= 0,
        "leader file counts must stay non-negative: {:?}",
        leader_counts
    );
    assert!(
        follower_counts.0 >= 0 && follower_counts.1 >= 0,
        "follower file counts must stay non-negative: {:?}",
        follower_counts
    );
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_link() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("link");
    fs.create("/original.txt", true)?;
    let after_create = file_counts(&fs);
    eprintln!("link counts after create = {:?}", after_create);
    fs.link("/original.txt", "/hardlink.txt")?;
    let after_hardlink = file_counts(&fs);
    eprintln!("link counts after hardlink = {:?}", after_hardlink);
    replay_all_then_duplicate_last(&js, &loader)?;

    let leader_counts = file_counts(&fs);
    let follower_counts = file_counts(&fs2);
    let file_count_drift = follower_counts.0 - leader_counts.0;
    let dir_count_drift = follower_counts.1 - leader_counts.1;
    eprintln!("link leader counts after replay = {:?}", leader_counts);
    eprintln!("link follower counts after replay = {:?}", follower_counts);
    eprintln!(
        "link count drift after replay: files={}, dirs={}",
        file_count_drift, dir_count_drift
    );
    assert!(
        leader_counts.0 >= 0 && leader_counts.1 >= 0,
        "leader file counts must stay non-negative: {:?}",
        leader_counts
    );
    assert!(
        follower_counts.0 >= 0 && follower_counts.1 >= 0,
        "follower file counts must stay non-negative: {:?}",
        follower_counts
    );
    assert_eq!(
        dir_count_drift, 0,
        "hardlink replay should not change directory counts: leader={:?}, follower={:?}",
        leader_counts, follower_counts
    );
    assert_eq!(
        file_count_drift, 0,
        "hardlink replay should preserve file counts: leader={:?}, follower={:?}",
        leader_counts, follower_counts
    );

    let original = fs.file_status("/original.txt")?;
    let hardlink = fs.file_status("/hardlink.txt")?;
    let replay_original = fs2.file_status("/original.txt")?;
    let replay_hardlink = fs2.file_status("/hardlink.txt")?;

    assert_eq!(original.id, hardlink.id);
    assert_eq!(replay_original.id, replay_hardlink.id);
    assert_eq!(original.id, replay_original.id);

    assert_eq!(original.nlink, 2);
    assert_eq!(hardlink.nlink, 2);
    assert_eq!(replay_original.nlink, 2);
    assert_eq!(replay_hardlink.nlink, 2);
    Ok(())
}

#[test]
fn test_idempotent_mount() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("mount");
    let mnt_mgr = js.mount_manager();
    let mount_uri = CurvineURI::new("/mnt/test")?;
    let ufs_uri = CurvineURI::new("oss://bucket1/")?;
    let mnt_opt = MountOptions::builder().build();
    mnt_mgr.mount(
        None,
        mount_uri.path(),
        ufs_uri.encode_uri().as_ref(),
        &mnt_opt,
    )?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_set_locks() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("set-locks");
    fs.create("/lockfile.log", true)?;
    let lock = curvine_model::FileLock {
        client_id: "client1".to_string(),
        owner_id: 1,
        lock_type: curvine_model::LockType::WriteLock,
        lock_flags: curvine_model::LockFlags::Plock,
        start: 0,
        end: 100,
        ..Default::default()
    };
    fs.set_lock("/lockfile.log", lock)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn lock_operations_reject_partial_paths() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "lock-partial-path");
    fs.create("/lockfile.log", true)?;

    let lock = FileLock {
        client_id: "client1".to_string(),
        owner_id: 1,
        lock_type: LockType::WriteLock,
        lock_flags: LockFlags::Plock,
        start: 0,
        end: 100,
        ..Default::default()
    };

    assert!(matches!(
        fs.get_lock("/lockfile.log/missing", lock.clone())
            .unwrap_err(),
        FsError::FileNotFound(_)
    ));
    assert!(matches!(
        fs.set_lock("/lockfile.log/missing", lock.clone())
            .unwrap_err(),
        FsError::FileNotFound(_)
    ));
    assert!(fs.get_lock("/lockfile.log", lock)?.is_none());
    Ok(())
}

#[test]
fn resize_rejects_extreme_file_size() {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "resize-extreme");
    fs.create("/extreme.log", true).unwrap();

    let err = fs
        .resize("/extreme.log", FileAllocOpts::with_truncate(1_i64 << 60))
        .unwrap_err();
    assert!(matches!(err, FsError::InvalidFileSize(_)));

    let status = fs.file_status("/extreme.log").unwrap();
    assert_eq!(status.len, 0);
}

#[test]
fn only_flush_persists_block_without_returning_file_snapshot() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "only-flush-no-snapshot");
    let path = "/only-flush-no-snapshot.log";
    let client = ClientAddress::default();
    let status = fs.create(path, false)?;
    let block = fs.add_block(path, None, client.clone(), vec![], vec![], 0, None)?;
    let commit = full_commit(&block, status.block_size);

    fs.flush_file(
        path,
        None,
        status.block_size,
        vec![commit],
        client.client_name.as_str(),
    )?;

    let file_blocks = fs.get_block_locations(path)?;
    assert_eq!(file_blocks.block_locs.len(), 1);
    assert_eq!(file_blocks.block_locs[0].block.len, status.block_size);

    let legacy_response = fs.complete_file(
        path,
        None,
        status.block_size,
        vec![],
        client.client_name.as_str(),
        true,
        None,
    )?;
    assert_eq!(legacy_response.unwrap().block_locs.len(), 1);
    Ok(())
}

#[test]
fn handler_only_flush_without_snapshot_persists_block() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let handler = new_handler_for_test("handler-only-flush-no-snapshot");
    let fs = handler.clone_fs();
    let path = "/handler-only-flush-no-snapshot.log";
    let client = ClientAddress::default();
    let status = fs.create(path, false)?;
    let block = fs.add_block(path, None, client.clone(), vec![], vec![], 0, None)?;
    let req = CompleteFileRequest {
        path: path.to_string(),
        len: status.block_size,
        client_name: client.client_name.clone(),
        commit_blocks: vec![ProtoUtils::commit_block_to_pb(full_commit(
            &block,
            status.block_size,
        ))],
        only_flush: true,
        inode_id: None,
        set_attr_opts: None,
        return_file_blocks: Some(false),
    };
    let msg = Builder::new_rpc(RpcCode::CompleteFile)
        .proto_header(req)
        .build();
    let mut ctx = RpcContext::new(&msg);

    let response = handler.complete_file(&mut ctx)?;
    let header: CompleteFileResponse = response.parse_header()?;
    assert!(header.result);
    assert!(header.file_blocks.is_none());

    let file_blocks = fs.get_block_locations(path)?;
    assert_eq!(file_blocks.block_locs.len(), 1);
    assert_eq!(file_blocks.block_locs[0].block.len, status.block_size);
    Ok(())
}

#[test]
#[ignore = "manual benchmark: run with --ignored --nocapture"]
fn measure_only_flush_file_blocks_snapshot() -> CommonResult<()> {
    const BLOCKS: usize = 4096;

    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "only-flush-snapshot-bench");
    let client = ClientAddress::default();

    let (legacy_len, legacy_commit) =
        prepare_flush_file(&fs, "/flush-bench/legacy", &client, BLOCKS)?;
    let legacy_started = std::time::Instant::now();
    let legacy_response = fs.complete_file(
        "/flush-bench/legacy",
        None,
        legacy_len,
        vec![legacy_commit],
        client.client_name.as_str(),
        true,
        None,
    )?;
    let legacy_elapsed = legacy_started.elapsed();
    let legacy_blocks = legacy_response
        .as_ref()
        .map(|blocks| blocks.block_locs.len())
        .unwrap_or_default();
    let legacy_bytes = legacy_response
        .as_ref()
        .map(|blocks| ProtoUtils::file_blocks_to_pb(blocks.clone()).encoded_len())
        .unwrap_or_default();

    let (opt_in_len, opt_in_commit) =
        prepare_flush_file(&fs, "/flush-bench/opt-in", &client, BLOCKS)?;
    let opt_in_started = std::time::Instant::now();
    let opt_in_result = fs.flush_file(
        "/flush-bench/opt-in",
        None,
        opt_in_len,
        vec![opt_in_commit],
        client.client_name.as_str(),
    );
    let opt_in_elapsed = opt_in_started.elapsed();
    opt_in_result?;
    let opt_in_blocks = 0;
    let opt_in_bytes = 0;

    assert_eq!(legacy_blocks, BLOCKS);
    assert_eq!(opt_in_blocks, 0);
    eprintln!(
        "ONLY_FLUSH_FILE_BLOCKS_BENCH blocks={BLOCKS} legacy_us={} legacy_bytes={legacy_bytes} opt_in_us={} opt_in_bytes={opt_in_bytes}",
        legacy_elapsed.as_micros(),
        opt_in_elapsed.as_micros(),
    );
    Ok(())
}

#[test]
fn located_block_has_spdk_reflects_worker_reported_storage_type() -> CommonResult<()> {
    let _serial = master_fs_test_serial();

    // Scenario A: worker reports SpdkDisk -> has_spdk should be true
    {
        let fs = new_fs(true, "has-spdk-spdk");
        let path = "/has-spdk-spdk.log";
        let addr = ClientAddress::default();
        fs.create(path, false)?;

        let block = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;

        fs.block_report(
            BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: block.locs[0].worker_id,
                full_report: true,
                full_report_start: false,
                total_len: 1,
                blocks: vec![BlockReportInfo::new(
                    block.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::SpdkDisk,
                    block.block.len,
                )],
            },
            None,
        )?;

        let fb = fs.get_block_locations(path)?;
        assert_eq!(fb.block_locs.len(), 1);
        assert!(
            fb.block_locs[0].has_spdk,
            "has_spdk should be true when worker reports SpdkDisk"
        );
    }

    // Scenario B: worker reports Mem -> has_spdk should be false
    {
        let fs = new_fs(true, "has-spdk-mem");
        let path = "/has-spdk-mem.log";
        let addr = ClientAddress::default();
        fs.create(path, false)?;

        let block = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;

        fs.block_report(
            BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: block.locs[0].worker_id,
                full_report: true,
                full_report_start: false,
                total_len: 1,
                blocks: vec![BlockReportInfo::new(
                    block.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Mem,
                    block.block.len,
                )],
            },
            None,
        )?;

        let fb = fs.get_block_locations(path)?;
        assert_eq!(fb.block_locs.len(), 1);
        assert!(
            !fb.block_locs[0].has_spdk,
            "has_spdk should be false when worker reports Mem"
        );
    }

    // Scenario C: worker reports Disk -> has_spdk should be false
    {
        let fs = new_fs(true, "has-spdk-disk");
        let path = "/has-spdk-disk.log";
        let addr = ClientAddress::default();
        fs.create(path, false)?;

        let block = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;

        fs.block_report(
            BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: block.locs[0].worker_id,
                full_report: true,
                full_report_start: false,
                total_len: 1,
                blocks: vec![BlockReportInfo::new(
                    block.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    block.block.len,
                )],
            },
            None,
        )?;

        let fb = fs.get_block_locations(path)?;
        assert_eq!(fb.block_locs.len(), 1);
        assert!(
            !fb.block_locs[0].has_spdk,
            "has_spdk should be false when worker reports Disk"
        );
    }

    // Scenario D: mixed replicas across 2 workers — one SpdkDisk, one Disk -> has_spdk should be true
    {
        let fs = new_fs(true, "has-spdk-mixed");
        let path = "/has-spdk-mixed.log";
        let addr = ClientAddress::default();
        fs.create(path, false)?;

        // Add second worker (worker_id=200) so we can have 2 replicas on different workers
        let worker2_addr = WorkerAddress {
            worker_id: 200,
            ip_addr: "127.0.0.2".to_string(),
            rpc_port: 667,
            ..Default::default()
        };
        let worker2 = WorkerInfo::new(worker2_addr, 0);
        fs.add_test_worker(worker2);

        let block = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;

        // Worker 100 (default) reports block as SpdkDisk
        fs.block_report(
            BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: 100,
                full_report: true,
                full_report_start: false,
                total_len: 1,
                blocks: vec![BlockReportInfo::new(
                    block.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::SpdkDisk,
                    block.block.len,
                )],
            },
            None,
        )?;

        // Worker 200 reports same block as Disk
        fs.block_report(
            BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: 200,
                full_report: false,
                full_report_start: false,
                total_len: 0,
                blocks: vec![BlockReportInfo::new(
                    block.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    block.block.len,
                )],
            },
            None,
        )?;

        let fb = fs.get_block_locations(path)?;
        assert_eq!(fb.block_locs.len(), 1);
        assert_eq!(fb.block_locs[0].locs.len(), 2, "should have 2 replicas");
        assert!(
            fb.block_locs[0].has_spdk,
            "has_spdk should be true when any replica reports SpdkDisk"
        );
    }

    Ok(())
}

#[test]
fn complete_file_with_set_attr_applies_attributes() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "complete-with-attr");
    let path = "/complete_with_attr.log";
    let addr = ClientAddress::default();

    // Create file and add a block
    let _status = fs.create(path, false)?;
    let block = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;

    let commit = CommitBlock {
        block_id: block.block.id,
        block_len: block.block.len,
        locations: vec![BlockLocation::with_id(block.locs[0].worker_id)],
    };

    // Complete the file with SetAttrOpts — owner, group, mode, mtime, xattr
    let custom_mtime: i64 = 1_000_000;
    let opts = SetAttrOptsBuilder::new()
        .owner("alice")
        .group("dev")
        .mode(0o644)
        .mtime(custom_mtime)
        .add_x_attr("user.tag".to_string(), b"v1".to_vec())
        .build();

    fs.complete_file(
        path,
        None,
        block.block.len,
        vec![commit],
        &addr.client_name,
        false,
        Some(opts),
    )?;

    // Verify the attributes were applied
    let result = fs.file_status(path)?;
    assert!(result.is_complete, "file should be complete");
    assert_eq!(
        result.owner, "alice",
        "owner should be set by complete_file"
    );
    assert_eq!(result.group, "dev", "group should be set by complete_file");
    assert_eq!(
        result.mode & 0o777,
        0o644,
        "mode should be set by complete_file"
    );
    assert_eq!(
        result.mtime, custom_mtime,
        "mtime should be overridden by set_attr_opts"
    );
    assert_eq!(
        result.x_attr.get("user.tag"),
        Some(&b"v1".to_vec()),
        "xattr should be set by complete_file"
    );

    Ok(())
}
