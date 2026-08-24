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

use crate::master::fs::context::ValidateAddBlock;
use crate::master::fs::policy::ChooseContext;
use crate::master::journal::{
    JournalCommitScope, JournalPermitScope, JournalSystem, JournalWriter, SnapshotManifest,
};
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::inode::{
    Inode, InodeDir, InodeFile, InodePath, InodePtr, InodeView, PATH_SEPARATOR, ROOT_INODE_ID,
};
use crate::master::meta::{FsDir, SameParentRename};

use crate::master::fs::DeleteResult as FsDeleteResult;
use crate::master::meta::parse_glob_pattern;
use crate::master::meta::store::{
    RocksInodeStore, RocksInodeStoreSnapshot, RocksStoreHandle, RocksStoreReadGuard,
    StorePathResolver, StoreResolvedPath, StoreSubtreeSummary,
};
use crate::master::meta::{
    BlockLocationLockManager, CacheInvalidationResult, CommitGate, FileSystemStats, InodeId,
    InodeLockManager, InodeLockMode, InodeLockRequest, InodeLockSet, MetadataReplicaPath,
    MetadataReplicaPathEntry, MetadataReplicaReader, SameParentRenamePlan, StablePathRead,
};
use crate::master::replication::master_replication_handler::MasterReplicationHandler;
use crate::master::{Master, MasterMonitor, SyncFsDir, SyncWorkerManager};
use curvine_config::{ClusterConf, MasterConf};
use curvine_core_error::{err_box, err_ext, try_option, CommonError, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::DeleteResult;
use curvine_model::*;
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::GroupExecutor;
use curvine_runtime::sync::{ArcRwLock, AtomicCounter};
use log::{error, info, warn};
use parking_lot::Mutex;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

#[derive(Default)]
pub struct LostWorkerLocationCleanup {
    pub removed_block_ids: Vec<i64>,
    pub replication_block_ids: Vec<i64>,
}

pub struct CvMetadataSnapshotEntry {
    pub status: FileStatus,
    pub blocks: Option<FileBlocks>,
}

pub struct CvMetadataSnapshotPage {
    pub entries: Vec<CvMetadataSnapshotEntry>,
    pub next_page_token: Option<String>,
    pub epoch: u64,
}

pub struct CvMetadataDeltaEntry {
    pub path: String,
    pub entry: Option<CvMetadataSnapshotEntry>,
}

pub struct CvMetadataDeltaPage {
    pub entries: Vec<CvMetadataDeltaEntry>,
    pub next_page_token: Option<String>,
    pub from_epoch: u64,
    pub to_epoch: u64,
    pub full_snapshot_required: bool,
}

struct CompleteFileOptions {
    only_flush: bool,
    return_file_blocks: bool,
    set_attr_opts: Option<SetAttrOpts>,
}

#[derive(Clone)]
pub struct MasterFilesystem {
    pub fs_dir: SyncFsDir,
    pub worker_manager: SyncWorkerManager,
    pub master_monitor: MasterMonitor,
    pub conf: Arc<MasterConf>,
    journal_writer: Arc<JournalWriter>,
    store_handle: Arc<RocksStoreHandle>,
    metadata_reader: Arc<MetadataReplicaReader>,
    ttl_bucket_list: Arc<TtlBucketList>,
    fs_stats: Arc<FileSystemStats>,
    op_id: Arc<AtomicCounter>,
    full_block_reports: Arc<Mutex<HashMap<u32, FullBlockReportState>>>,
    full_block_reconciles: Arc<Mutex<HashMap<u32, FullBlockReconcileState>>>,
    full_block_reconcile_executor: Arc<GroupExecutor>,
    full_block_report_seq: Arc<AtomicCounter>,
    block_location_locks: Arc<BlockLocationLockManager>,
    inode_locks: Arc<InodeLockManager>,
    metadata_commit_gate: Arc<CommitGate>,
    namespace_topology_gate: Arc<CommitGate>,
}

pub struct BlockReportResult {
    pub delete_blocks: Vec<i64>,
}

fn child_snapshot_path(parent: &str, child_name: &str) -> String {
    if parent == "/" {
        format!("/{child_name}")
    } else {
        format!("{parent}/{child_name}")
    }
}

fn snapshot_token_in_subtree(token: &str, subtree_path: &str) -> bool {
    token == subtree_path
        || (subtree_path != "/"
            && token
                .strip_prefix(subtree_path)
                .map(|rest| rest.starts_with(PATH_SEPARATOR))
                .unwrap_or(false))
}
struct FullBlockReportState {
    generation: u64,
    total_len: Option<u64>,
    update_time_ms: u64,
    reported_blocks: HashSet<i64>,
    protected_blocks: HashSet<i64>,
    can_reconcile: bool,
}

struct FullBlockReconcileState {
    running: bool,
    pending: Option<FullBlockReconcileJob>,
}

struct FullBlockReconcileJob {
    generation: u64,
}

#[derive(Clone, Copy)]
enum BlockInodeState {
    Missing,
    File,
    NotFile,
}

struct MetadataRenameLockPlan {
    requests: Vec<InodeLockRequest>,
    same_parent: Option<SameParentRenamePlan>,
}

struct RenameLockSet<'a> {
    _inode_locks: InodeLockSet<'a>,
    replaced_block_ids: Vec<i64>,
    same_parent: Option<SameParentRenamePlan>,
}

struct LockedPath<'a> {
    inode_locks: InodeLockSet<'a>,
    path: InodePath,
}

enum MetadataRead<T> {
    Ready(T),
    Missing,
    Retry,
}

const FULL_BLOCK_REPORT_TTL_MS: u64 = 60 * 60 * 1000;
const MAX_FULL_BLOCK_REPORT_BLOCKS: u64 = 100_000_000;
const FULL_BLOCK_RECONCILE_BATCH_SIZE: usize = 4096;
const FULL_BLOCK_RECONCILE_THREADS: usize = 2;
const FULL_BLOCK_RECONCILE_QUEUE_SIZE: usize = 128;
const NAMESPACE_LOCK_RETRY_LIMIT: usize = 128;
const METADATA_READ_FALLBACK_RETRY_LIMIT: usize = 32;

impl MasterFilesystem {
    const LOST_WORKER_INVALIDATION_CHUNK: usize = 1024;

    fn validate_alloc_capacity(
        current_len: i64,
        replicas: u8,
        opts: &FileAllocOpts,
        available: i64,
    ) -> FsResult<()> {
        if opts.truncate || opts.len <= current_len {
            return Ok(());
        }

        let logical_growth = opts.len - current_len;
        let required = logical_growth.saturating_mul(i64::from(replicas));
        if required > available {
            return err_ext!(FsError::disk_out_of_space(format!(
                "fallocate requires {} bytes for {} replicas, but only {} bytes are available",
                logical_growth, replicas, available
            )));
        }

        Ok(())
    }
    pub fn new(
        conf: &ClusterConf,
        fs_dir: SyncFsDir,
        worker_manager: SyncWorkerManager,
        master_monitor: MasterMonitor,
    ) -> Self {
        let (journal_writer, store_handle, metadata_reader, ttl_bucket_list, fs_stats, op_id) = {
            let fs_dir_guard = fs_dir.read();
            (
                fs_dir_guard.journal_writer.clone(),
                fs_dir_guard.store_handle.clone(),
                fs_dir_guard.metadata_reader(),
                fs_dir_guard.store.get_ttl_bucket_list(),
                fs_dir_guard.store.fs_stats.clone(),
                fs_dir_guard.op_id_counter(),
            )
        };
        Self {
            fs_dir,
            worker_manager,
            master_monitor,
            conf: Arc::new(conf.master.clone()),
            journal_writer,
            store_handle,
            metadata_reader,
            ttl_bucket_list,
            fs_stats,
            op_id,
            full_block_reports: Default::default(),
            full_block_reconciles: Default::default(),
            full_block_reconcile_executor: Arc::new(GroupExecutor::new(
                "master-full-block-reconcile",
                FULL_BLOCK_RECONCILE_THREADS,
                FULL_BLOCK_RECONCILE_QUEUE_SIZE,
            )),
            full_block_report_seq: Arc::new(AtomicCounter::new(0)),
            block_location_locks: Default::default(),
            inode_locks: Default::default(),
            metadata_commit_gate: Default::default(),
            namespace_topology_gate: Default::default(),
        }
    }

    pub fn with_js(conf: &ClusterConf, js: &JournalSystem) -> Self {
        let _ = conf;
        js.fs()
    }

    pub fn check_parent(path: &InodePath) -> FsResult<()> {
        // The root directory must exist.All /a does not require verification
        if path.len() > 2 {
            if let Some(v) = path.get_inode(-2) {
                if !v.is_dir() {
                    err_box!(
                        "Parent path is not a directory:: {}",
                        path.get_parent_path()
                    )
                } else {
                    Ok(())
                }
            } else {
                err_box!("Parent directory doesn't exist: {}", path.get_parent_path())
            }
        } else {
            Ok(())
        }
    }

    pub fn print_tree(&self) {
        let fs_dir = self.fs_dir.read();
        fs_dir.print_tree();
    }

    pub(crate) fn ensure_metadata_current(&self) -> FsResult<()> {
        self.journal_writer.ensure_metadata_current()
    }

    pub(crate) fn metadata_commit_gate(&self) -> Arc<CommitGate> {
        self.metadata_commit_gate.clone()
    }

    fn run_metadata_write<R>(&self, f: impl FnOnce() -> FsResult<R>) -> FsResult<R> {
        self.ensure_metadata_current()?;
        let (result, commit_scope) = {
            let _commit_guard = self.metadata_commit_gate.enter();
            // A role change can begin after the pre-entry check and before the
            // gate is acquired. Recheck inside the serialization boundary so
            // an old request cannot write after follower recovery starts.
            let commit_scope = self.journal_writer.begin_commit_scope();
            let result = self.ensure_metadata_current().and_then(|_| f());
            (result, commit_scope)
        };

        commit_scope.wait()?;

        if result.is_ok() {
            self.emit_snapshot_if_requested();
        }
        result
    }

    fn run_namespace_topology_write<R>(&self, f: impl FnOnce() -> FsResult<R>) -> FsResult<R> {
        self.run_metadata_write(|| {
            let _topology_guard = self.namespace_topology_gate.enter();
            f()
        })
    }

    fn try_fast_namespace_write<R>(
        &self,
        f: impl FnOnce() -> FsResult<Option<R>>,
    ) -> FsResult<Option<R>> {
        let (result, commit_scope): (FsResult<Option<R>>, JournalCommitScope) = {
            let Some(_commit_guard) = self.metadata_commit_gate.try_enter() else {
                return Ok(None);
            };
            let Some(_topology_guard) = self.namespace_topology_gate.try_enter() else {
                return Ok(None);
            };
            let commit_scope = self.journal_writer.begin_commit_scope();
            let result = self.ensure_metadata_current().and_then(|_| f());
            (result, commit_scope)
        };

        commit_scope.wait()?;
        let result = result?;
        if result.is_some() {
            self.emit_snapshot_if_requested();
        }
        Ok(result)
    }

    fn emit_snapshot_if_requested(&self) {
        let writer = self.journal_writer.clone();
        if !writer.try_begin_snapshot() {
            return;
        }

        let snapshot_result = (|| {
            let _snapshot_guard = self.metadata_commit_gate.close_and_enter_if_open();
            let Some(_snapshot_guard) = _snapshot_guard else {
                return err_box!("metadata gate is already closed");
            };
            self.ensure_metadata_current()?;
            let permit = writer.reserve()?;
            let now = LocalTime::mills();
            let op_id = self.next_op_id();
            let (inode_id, dir) = {
                let fs_dir = self.fs_dir.read();
                let inode_id = fs_dir.last_inode_id();
                let dir = fs_dir.create_checkpoint(op_id)?;
                SnapshotManifest::write_checkpoint(&dir, op_id, writer.node_id())?;
                (inode_id, dir)
            };
            info!(
                "create leader snapshot, dir {}, op_id {}, cost {} ms, inode_id {}",
                dir,
                op_id,
                LocalTime::mills() - now,
                inode_id
            );
            writer.enqueue_snapshot_with_permit(permit, op_id, dir)
        })();

        if let Err(error) = &snapshot_result {
            warn!("defer leader snapshot after failure: {}", error);
        }
        writer.finish_snapshot(snapshot_result.is_ok());
    }

    pub fn mkdir_with_opts<T: AsRef<str>>(&self, path: T, opts: MkdirOpts) -> FsResult<FileStatus> {
        let path = path.as_ref();
        if let Some(status) = self.try_fast_mkdir(path, &opts)? {
            return Ok(status);
        }
        self.run_namespace_topology_write(|| self.mkdir_with_locks(path, opts))
    }

    fn mkdir_with_locks(&self, path: &str, opts: MkdirOpts) -> FsResult<FileStatus> {
        let mut parent_write = false;
        for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let LockedPath {
                inode_locks,
                path: inp,
            } = self.lock_resolved_path_for_write(path, InodeLockMode::Write, parent_write)?;
            let fs_dir = self.fs_dir.read();
            let requires_parent_write =
                !inp.is_full() && !Self::path_has_existing_parent_only(&inp);
            if requires_parent_write && !parent_write {
                drop(fs_dir);
                drop(inode_locks);
                parent_write = true;
                continue;
            }
            if !Self::create_inode_locks_cover(
                &inode_locks,
                &inp,
                InodeLockMode::Write,
                parent_write,
            ) {
                drop(fs_dir);
                drop(inode_locks);
                Self::retry_namespace_lock(path, attempt)?;
                continue;
            }

            let _inode_locks = inode_locks;
            let _journal_scope = self.reserve_journal_scope(
                Self::create_entries_for_resolved_path(&inp, opts.create_parent),
            )?;

            // Creation of root directory is not allowed
            if inp.is_root() {
                return err_box!("Not allowed to create existing root path: {}", inp.path());
            }

            if inp.is_full() {
                if opts.create_parent {
                    if let Some(last_inode) = inp.get_last_inode() {
                        if last_inode.is_dir() {
                            let status = last_inode.to_file_status(inp.path())?;
                            return Ok(status);
                        }
                    }
                }
                return err_ext!(FsError::file_exists(inp.path()));
            }

            // Check whether the directory can be created recursively.
            if !opts.create_parent {
                Self::check_parent(&inp)?;
            }

            let inp = match fs_dir.mkdir(inp, opts.clone()) {
                Ok(inp) => inp,
                Err(FsError::FileAlreadyExists(_)) => {
                    drop(fs_dir);
                    drop(_inode_locks);
                    continue;
                }
                Err(error) => return Err(error),
            };
            let last = try_option!(
                inp.get_last_inode(),
                "Path {} has no inode after mkdir",
                inp.path()
            );
            let status = last.to_file_status(inp.path())?;
            return Ok(status);
        }

        err_box!(
            "namespace path {} changed while acquiring mkdir locks after {} retries",
            path,
            NAMESPACE_LOCK_RETRY_LIMIT
        )
    }

    fn try_fast_mkdir(&self, path: &str, opts: &MkdirOpts) -> FsResult<Option<FileStatus>> {
        self.try_fast_namespace_write(|| {
            let Ok(fs_dir) = self.fs_dir.try_write() else {
                return Ok(None);
            };
            let inp = Self::resolve_exclusive_path(&fs_dir, path)?;
            let _journal_scope = self.reserve_journal_scope(
                Self::create_entries_for_resolved_path(&inp, opts.create_parent),
            )?;

            if inp.is_root() {
                return err_box!("Not allowed to create existing root path: {}", inp.path());
            }
            if inp.is_full() {
                if opts.create_parent {
                    if let Some(last_inode) = inp.get_last_inode() {
                        if last_inode.is_dir() {
                            return Ok(Some(last_inode.to_file_status(inp.path())?));
                        }
                    }
                }
                return Err(FsError::file_exists(inp.path()));
            }
            if !opts.create_parent {
                Self::check_parent(&inp)?;
            }

            let inp = fs_dir.mkdir_uncontended(inp, opts.clone())?;
            let last = try_option!(
                inp.get_last_inode(),
                "Path {} has no inode after mkdir",
                inp.path()
            );
            Ok(Some(last.to_file_status(inp.path())?))
        })
    }

    pub fn mkdir<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let opts = MkdirOpts::with_create(create_parent);
        self.mkdir_with_opts(path, opts)
    }

    pub fn delete<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<DeleteResult> {
        let path = path.as_ref();
        if !recursive {
            if let Some(result) = self.try_fast_delete(path)? {
                return Ok(result);
            }
        }
        self.run_namespace_topology_write(|| self.delete_with_locks(path, recursive))
    }

    fn delete_with_locks(&self, path: &str, recursive: bool) -> FsResult<DeleteResult> {
        let _journal_scope = self.reserve_journal_scope(1)?;
        if recursive {
            let (_inode_locks, block_ids) = self.lock_delete_path(path, true)?;
            let _block_locks = self.block_location_locks.write_blocks(&block_ids);
            let delete_result = {
                let fs_dir = self.fs_dir.read();
                let inp = Self::resolve_path(&fs_dir, path)?;
                fs_dir.delete(&inp, true)?
            };
            self.worker_manager
                .write()
                .remove_blocks(&Self::to_model_delete_result(&delete_result));
            return Ok(Self::to_model_delete_result(delete_result));
        }

        let LockedPath {
            inode_locks: _inode_locks,
            path: inp,
        } = self.lock_resolved_path_for_write(path, InodeLockMode::Write, false)?;
        let fs_dir = self.fs_dir.read();
        let block_ids = Self::path_block_ids(&fs_dir, &inp)?;
        let _block_locks = self.block_location_locks.write_blocks(&block_ids);
        let delete_result = fs_dir.delete(&inp, false)?;
        drop(fs_dir);
        self.worker_manager
            .write()
            .remove_blocks(&Self::to_model_delete_result(&delete_result));

        Ok(Self::to_model_delete_result(delete_result))
    }

    /// Convert the namespace-internal delete summary into the wire-level
    /// model type (block locations keyed by worker id).
    fn to_model_delete_result(res: impl AsModelDelete) -> DeleteResult {
        res.as_model_delete()
    }

    fn try_fast_delete(&self, path: &str) -> FsResult<Option<DeleteResult>> {
        self.try_fast_namespace_write(|| {
            let Ok(fs_dir) = self.fs_dir.try_write() else {
                return Ok(None);
            };
            let inp = Self::resolve_exclusive_path(&fs_dir, path)?;
            let _journal_scope = self.reserve_journal_scope(1)?;
            let block_ids = Self::path_block_ids(&fs_dir, &inp)?;
            let _block_locks = self.block_location_locks.write_blocks(&block_ids);
            let delete_result = fs_dir.delete_uncontended(&inp, false)?;
            drop(fs_dir);
            self.worker_manager
                .write()
                .remove_blocks(&Self::to_model_delete_result(&delete_result));
            Ok(Some(Self::to_model_delete_result(delete_result)))
        })
    }

    pub fn free<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<FreeResult> {
        let path = path.as_ref();
        self.run_metadata_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let (_inode_locks, block_ids) = self.lock_free_path(path, recursive)?;
            let _block_locks = self.block_location_locks.write_blocks(&block_ids);
            let fs_dir = self.fs_dir.read();
            let inp = Self::resolve_path(&fs_dir, path)?;

            let mut free_res = fs_dir.free(&inp, recursive)?;
            drop(fs_dir);

            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(&DeleteResult {
                inodes: 0,
                bytes: 0,
                blocks: std::mem::take(&mut free_res.blocks),
            });

            Ok(free_res)
        })
    }

    pub fn rename<T: AsRef<str>>(&self, src: T, dst: T, flags: RenameFlags) -> FsResult<bool> {
        let src = src.as_ref();
        let dst = dst.as_ref();
        if !flags.is_supported() {
            return err_ext!(FsError::unsupported(format!(
                "unsupported rename flags: {:#x}",
                flags.value()
            )));
        }
        if let Some(result) = self.try_fast_rename(src, dst, flags)? {
            return Ok(result);
        }
        self.run_namespace_topology_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;

            if src == dst {
                return Ok(false);
            }

            for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
                let outcome = {
                    let rename_locks = self.lock_rename_paths(src, dst, flags, true)?;
                    let _block_locks = self
                        .block_location_locks
                        .write_blocks(&rename_locks.replaced_block_ids);
                    let fs_dir = self.fs_dir.read();
                    match rename_locks.same_parent.as_ref() {
                        Some(plan) => fs_dir.rename_same_parent(plan, src, dst, flags)?,
                        None => {
                            let src_inp = Self::resolve_path(&fs_dir, src)?;
                            let dst_inp = Self::resolve_path(&fs_dir, dst)?;

                            if src_inp.is_root() {
                                return err_box!("Cannot rename root path");
                            }

                            if let (Some(src_inode), Some(dst_inode)) =
                                (src_inp.get_last_inode(), dst_inp.get_last_inode())
                            {
                                if src_inode.id() == dst_inode.id() {
                                    return Ok(false);
                                }
                            }

                            // dst cannot be in the src directory, /a/b -> /a/b/c is not allowed (POSIX EINVAL).
                            if let Some(rest) = dst.strip_prefix(src) {
                                if rest.starts_with(PATH_SEPARATOR) {
                                    return err_ext!(FsError::invalid_argument(format!(
                                        "cannot rename {} to {}: destination is under source",
                                        src, dst
                                    )));
                                }
                            }

                            if flags.exchange_mode() {
                                if let Some(rest) = src.strip_prefix(dst) {
                                    if rest.starts_with(PATH_SEPARATOR) {
                                        return err_ext!(FsError::invalid_argument(format!(
                                            "cannot exchange {} with {}: source is under destination",
                                            src, dst
                                        )));
                                    }
                                }
                            }

                            SameParentRename::Renamed(fs_dir.rename(&src_inp, &dst_inp, flags)?)
                        }
                    }
                };

                match outcome {
                    SameParentRename::Renamed(Some(del_res)) => {
                        let mut worker_manager = self.worker_manager.write();
                        worker_manager.remove_blocks(&Self::to_model_delete_result(&del_res));
                        return Ok(true);
                    }
                    SameParentRename::Renamed(None) => return Ok(true),
                    SameParentRename::Noop => return Ok(false),
                    SameParentRename::Retry | SameParentRename::NotApplicable => {
                        Self::retry_namespace_lock(src, attempt)?;
                    }
                }
            }

            err_box!(
                "namespace paths {} -> {} changed while executing rename after {} retries",
                src,
                dst,
                NAMESPACE_LOCK_RETRY_LIMIT
            )
        })
    }

    fn try_fast_rename(&self, src: &str, dst: &str, flags: RenameFlags) -> FsResult<Option<bool>> {
        self.try_fast_namespace_write(|| {
            let Ok(fs_dir) = self.fs_dir.try_write() else {
                return Ok(None);
            };
            let _journal_scope = self.reserve_journal_scope(1)?;

            if src == dst {
                return Ok(Some(false));
            }

            let src_path = Self::resolve_exclusive_path(&fs_dir, src)?;
            let dst_path = Self::resolve_exclusive_path(&fs_dir, dst)?;
            if src_path.is_root() {
                return err_box!("Cannot rename root path");
            }
            if flags.no_replace() && dst_path.is_full() {
                return Err(FsError::file_exists(dst));
            }
            if let (Some(src_inode), Some(dst_inode)) =
                (src_path.get_last_inode(), dst_path.get_last_inode())
            {
                if src_inode.id() == dst_inode.id() {
                    return Ok(Some(false));
                }
            }
            if let Some(rest) = dst.strip_prefix(src) {
                if rest.starts_with(PATH_SEPARATOR) {
                    return err_ext!(FsError::invalid_argument(format!(
                        "cannot rename {} to {}: destination is under source",
                        src, dst
                    )));
                }
            }

            // Replacing an existing entry can remove blocks. Preserve the
            // normal path's block-location locking for that case.
            if dst_path.is_full() {
                return Ok(None);
            }

            let delete_result = fs_dir.rename(&src_path, &dst_path, flags)?;
            debug_assert!(
                delete_result.is_none(),
                "rename without an existing destination must not delete metadata"
            );
            Ok(Some(true))
        })
    }

    pub fn create<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let ctx = CreateFileOpts::with_create(create_parent);
        self.create_with_opts(path, ctx, OpenFlags::new_create().set_overwrite(true))
    }

    fn truncate(
        &self,
        fs_dir: &FsDir,
        inp: &InodePath,
        opts: CreateFileOpts,
    ) -> FsResult<FsDeleteResult> {
        fs_dir.overwrite_file(inp, opts)
    }

    pub fn create_with_opts<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        let path = path.as_ref();
        if let Some(status) = self.try_fast_create(path, &opts, flags)? {
            return Ok(status);
        }
        self.run_metadata_write(|| self.create_with_locks(path, &opts, flags))
    }

    fn create_with_locks(
        &self,
        path: &str,
        opts: &CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        self.validate_create_request(path, opts, flags)?;

        let mut parent_write = false;
        for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let LockedPath {
                inode_locks,
                path: inp,
            } = self.lock_resolved_path_for_write(path, InodeLockMode::Write, parent_write)?;
            let fs_dir = self.fs_dir.read();
            let requires_parent_write =
                !inp.is_full() && !Self::path_has_existing_parent_only(&inp);
            if requires_parent_write && !parent_write {
                drop(fs_dir);
                drop(inode_locks);
                parent_write = true;
                continue;
            }
            if !Self::create_inode_locks_cover(
                &inode_locks,
                &inp,
                InodeLockMode::Write,
                parent_write,
            ) {
                drop(fs_dir);
                drop(inode_locks);
                Self::retry_namespace_lock(path, attempt)?;
                continue;
            }

            let _inode_locks = inode_locks;
            let _journal_scope = self.reserve_journal_scope(
                Self::create_entries_for_resolved_path(&inp, opts.create_parent),
            )?;

            let last_inode = inp.get_last_inode();
            if let Some(inode) = &last_inode {
                if inode.is_dir() {
                    return err_box!("{}  already exists as a dir", inp.path());
                }

                if flags.exclusive() {
                    return err_ext!(FsError::file_exists(inp.path()));
                }
            }

            let clean_result;
            if !opts.create_parent {
                Self::check_parent(&inp)?;
            }

            let inp = if let Some(existing_inode) = &last_inode {
                if flags.overwrite() {
                    let overwrite_block_ids = existing_inode.as_file_ref()?.block_ids();
                    let _block_locks = self.block_location_locks.write_blocks(&overwrite_block_ids);
                    clean_result = Some(self.truncate(&fs_dir, &inp, opts.clone())?);
                } else {
                    return err_ext!(FsError::file_exists(inp.path()));
                }
                inp
            } else {
                clean_result = None;
                // Only creation changes namespace topology. Existing-file
                // opens and truncates stay outside the topology gate so a
                // path-read progress fallback never waits on them.
                let _topology_guard = self.namespace_topology_gate.enter();
                match fs_dir.create_file(inp, opts.clone()) {
                    Ok(inp) => inp,
                    Err(FsError::FileAlreadyExists(_)) => {
                        drop(fs_dir);
                        drop(_inode_locks);
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            };

            let status = fs_dir.file_status(&inp)?;
            if let Some(clean_result) = clean_result {
                if !clean_result.blocks.is_empty() {
                    self.worker_manager
                        .write()
                        .remove_blocks(&Self::to_model_delete_result(&clean_result));
                }
            }

            return Ok(status);
        }

        err_box!(
            "namespace path {} changed while acquiring create locks after {} retries",
            path,
            NAMESPACE_LOCK_RETRY_LIMIT
        )
    }

    fn try_fast_create(
        &self,
        path: &str,
        opts: &CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<Option<FileStatus>> {
        self.try_fast_namespace_write(|| {
            self.validate_create_request(path, opts, flags)?;
            let Ok(fs_dir) = self.fs_dir.try_write() else {
                return Ok(None);
            };
            let inp = Self::resolve_exclusive_path(&fs_dir, path)?;
            let _journal_scope = self.reserve_journal_scope(
                Self::create_entries_for_resolved_path(&inp, opts.create_parent),
            )?;

            if let Some(inode) = inp.get_last_inode() {
                if inode.is_dir() {
                    return err_box!("{}  already exists as a dir", inp.path());
                }
                if flags.exclusive() || !flags.overwrite() {
                    return Err(FsError::file_exists(inp.path()));
                }
                // Overwrite can release blocks, so retain the normal path's
                // block-location locking and worker cleanup.
                return Ok(None);
            }
            if !opts.create_parent {
                Self::check_parent(&inp)?;
            }

            match fs_dir.create_file_uncontended(inp, opts.clone()) {
                Ok(inp) => Ok(Some(fs_dir.file_status(&inp)?)),
                Err(FsError::FileAlreadyExists(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }

    fn validate_create_request(
        &self,
        path: &str,
        opts: &CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<()> {
        if !flags.create() {
            return err_box!("create_with_opts requires O_CREAT flag");
        }
        self.check_path_length(path)?;
        if opts.replicas < self.conf.min_replication || opts.replicas >= self.conf.max_replication {
            return err_box!(
                "The replica number {} needs to be between {} and {}",
                opts.replicas,
                self.conf.min_replication,
                self.conf.max_replication
            );
        }
        if opts.block_size < self.conf.min_block_size || opts.block_size >= self.conf.max_block_size
        {
            return err_box!(
                "Block size needs to be between {} and {}",
                self.conf.min_block_size,
                self.conf.max_block_size
            );
        }
        Ok(())
    }

    pub fn open_file<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileBlocks> {
        let path = path.as_ref();

        if flags.read_only() {
            if flags.truncate() {
                return err_box!("cannot combine O_RDONLY with O_TRUNC");
            }
            if flags.create() {
                return err_box!("cannot combine O_RDONLY with O_CREAT");
            }
            return self.get_block_locations(path);
        }

        if flags.create() {
            self.ensure_metadata_current()?;
            let missing = {
                let store = self.rocks_store()?;
                let resolved = StorePathResolver::new(&store).resolve(path)?;
                resolved.target().is_none()
            };
            if missing {
                let status = self.create_with_opts(path, opts, flags)?;
                return Ok(FileBlocks::new(status, vec![]));
            }
        }

        let existing = self.run_metadata_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let _inode_locks = self.lock_path_for_write(path, InodeLockMode::Write, false)?;
            let truncate_block_ids = if flags.truncate() {
                let store = self.rocks_store()?;
                let block_ids = StorePathResolver::new(&store).collect_block_ids(path, false)?;
                block_ids
            } else {
                Vec::new()
            };
            let _block_locks = self.block_location_locks.write_blocks(&truncate_block_ids);
            let (blocks, clean_result): (Option<FileBlocks>, Option<DeleteResult>) = {
                let fs_dir = self.fs_dir.read();
                let inp = Self::resolve_path(&fs_dir, path)?;

                let inode = match inp.get_last_inode() {
                    None if flags.create() => return Ok(None),
                    None => return err_ext!(FsError::file_not_found(inp.path())),
                    Some(inode) => {
                        if inode.is_dir() {
                            return err_box!("{} is a directory", inp.path());
                        }
                        inode
                    }
                };

                if flags.truncate() {
                    let clean_result = self.truncate(&fs_dir, &inp, opts.clone())?;
                    if !clean_result.blocks.is_empty() {
                        self.worker_manager
                            .write()
                            .remove_blocks(&Self::to_model_delete_result(&clean_result));
                    }
                    let status = fs_dir.file_status(&inp)?;
                    (
                        Some(FileBlocks::new(status, vec![])),
                        Some(Self::to_model_delete_result(clean_result)),
                    )
                } else {
                    let status = fs_dir.reopen_file(&inp, opts.client_name.clone())?;
                    let file = inode.as_file_ref()?;
                    let blocks = if !file.blocks.is_empty() {
                        let block_ids = file.blocks.iter().map(|meta| meta.id).collect::<Vec<_>>();
                        let _block_locks = self.block_location_locks.read_blocks(&block_ids);
                        self.get_block_locs(path, &fs_dir, file)?
                    } else {
                        vec![]
                    };
                    (Some(FileBlocks::new(status, blocks)), None)
                }
            };

            Ok(blocks)
        })?;

        match existing {
            Some(blocks) => Ok(blocks),
            None => {
                let status = self.create_with_opts(path, opts, flags)?;
                Ok(FileBlocks::new(status, vec![]))
            }
        }
    }

    pub fn file_status<T: AsRef<str>>(&self, path: T) -> FsResult<FileStatus> {
        self.ensure_metadata_current()?;
        self.file_status_unchecked(path)
    }

    pub(crate) fn file_status_unchecked<T: AsRef<str>>(&self, path: T) -> FsResult<FileStatus> {
        let path = path.as_ref();
        if let Some(MetadataRead::Ready(status)) = self
            .metadata_reader
            .with_resolved_path(path, |target| {
                Ok(self.replica_file_status_for_target(target, path)?)
            })
            .map_err(Self::fs_error_from_common)?
        {
            return Ok(status);
        }

        for _ in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let (component_count, resolved) = self.metadata_reader.resolve(path)?;
            if !resolved.is_full(component_count) {
                if self.metadata_reader.validate(&resolved) {
                    return err_ext!(FsError::file_not_found(path));
                }
                Self::retry_metadata_read();
                continue;
            }
            let target = try_option!(resolved.target().cloned(), "File {} not exists", path);
            let Some(status) = self.read_if_path_current(
                &resolved,
                self.replica_file_status_for_target(&target, path),
            )?
            else {
                Self::retry_metadata_read();
                continue;
            };
            match status {
                MetadataRead::Ready(status) if self.metadata_reader.validate(&resolved) => {
                    return Ok(status)
                }
                MetadataRead::Missing if self.metadata_reader.validate(&resolved) => {
                    return err_ext!(FsError::file_not_found(path));
                }
                _ => {}
            }
            Self::retry_metadata_read();
        }

        Self::require_metadata_read(path, self.read_file_status_until_path_current(path)?)
    }

    pub fn exists<T: AsRef<str>>(&self, path: T) -> FsResult<bool> {
        self.ensure_metadata_current()?;
        let path = path.as_ref();
        if let Some(Some(())) = self
            .metadata_reader
            .with_resolved_path(path, |_| Ok(Some(())))?
        {
            return Ok(true);
        }

        for _ in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let (component_count, resolved) = self.metadata_reader.resolve(path)?;
            if !resolved.is_full(component_count) {
                if self.metadata_reader.validate(&resolved) {
                    return Ok(false);
                }
                Self::retry_metadata_read();
                continue;
            }
            if self.metadata_reader.validate(&resolved) {
                return Ok(true);
            }
            Self::retry_metadata_read();
        }

        match self.read_until_path_current(path, |_| Ok(MetadataRead::Ready(())))? {
            MetadataRead::Ready(()) => Ok(true),
            MetadataRead::Missing => Ok(false),
            MetadataRead::Retry => Err(FsError::in_progress_msg(format!(
                "metadata path {} is changing; retry the request",
                path
            ))),
        }
    }

    pub fn list_status<T: AsRef<str>>(&self, path: T) -> FsResult<Vec<FileStatus>> {
        self.ensure_metadata_current()?;
        self.list_status_unchecked(path)
    }

    pub(crate) fn list_status_unchecked<T: AsRef<str>>(
        &self,
        path: T,
    ) -> FsResult<Vec<FileStatus>> {
        let path = path.as_ref();
        let (is_glob_pattern, _) = parse_glob_pattern(path);
        if is_glob_pattern {
            let store = self.rocks_store()?;
            let resolver = StorePathResolver::new(&store);
            resolver.list_status_glob(path)
        } else {
            self.list_status_from_replica(path, None)
        }
    }

    pub fn list_options<T: AsRef<str>>(
        &self,
        path: T,
        opts: ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        self.ensure_metadata_current()?;
        let path = path.as_ref();
        let (is_glob_pattern, _) = parse_glob_pattern(path);
        if is_glob_pattern {
            err_box!("list_options does not support glob pattern, path {}", path)
        } else {
            self.list_status_from_replica(path, Some(&opts))
        }
    }

    fn replica_inode(
        store: &RocksInodeStore,
        entry: &MetadataReplicaPathEntry,
    ) -> FsResult<Option<InodeView>> {
        let mut inode = match store.get_inode(entry.inode_id)? {
            Some(inode) => inode,
            None if entry.inode_id == ROOT_INODE_ID => {
                InodeView::new_dir(entry.name.to_string(), InodeDir::new(ROOT_INODE_ID, 0))
            }
            None => return Ok(None),
        };
        inode.change_name(entry.name.to_string());
        Ok(Some(inode))
    }

    fn replica_snapshot_inode(
        snapshot: &RocksInodeStoreSnapshot<'_>,
        entry: &MetadataReplicaPathEntry,
    ) -> FsResult<Option<InodeView>> {
        let mut inode = match snapshot.get_inode(entry.inode_id)? {
            Some(inode) => inode,
            None if entry.inode_id == ROOT_INODE_ID => {
                let inode =
                    InodeView::new_dir(entry.name.to_string(), InodeDir::new(ROOT_INODE_ID, 0));
                if let Some(attributes) = snapshot.get_directory_attributes(ROOT_INODE_ID)? {
                    inode.set_directory_attributes(attributes);
                }
                inode
            }
            None => return Ok(None),
        };
        inode.change_name(entry.name.to_string());
        Ok(Some(inode))
    }

    fn replica_file_status(
        inode: &InodeView,
        path: &str,
        child_count: Option<usize>,
    ) -> FsResult<FileStatus> {
        let mut status = inode.to_file_status(path)?;
        if let Some(child_count) = child_count {
            let child_count = i32::try_from(child_count).unwrap_or(i32::MAX);
            status.children_num = child_count;
            status.len = i64::from(child_count);
        }
        Ok(status)
    }

    fn replica_file_status_for_entry(
        inode: &InodeView,
        target: &MetadataReplicaPathEntry,
        path: &str,
    ) -> FsResult<FileStatus> {
        let mut status = Self::replica_file_status(inode, path, None)?;
        // A hard-link edge has its own name while sharing the backing inode.
        status.name = target.name.to_string();
        Ok(status)
    }

    fn replica_file_status_for_target(
        &self,
        target: &MetadataReplicaPathEntry,
        path: &str,
    ) -> FsResult<MetadataRead<FileStatus>> {
        if !target.is_dir {
            if let Some(status) =
                self.metadata_reader
                    .cached_file_status(target.inode_id, path, target.name.as_ref())
            {
                return Ok(MetadataRead::Ready(status));
            }
        }

        if target.is_dir {
            return self.replica_directory_file_status(target, path);
        }

        for _ in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let version = self.metadata_reader.file_status_version(target.inode_id);
            if let Some(inode) = self
                .metadata_reader
                .cached_file_inode(target.inode_id, version)
            {
                return Ok(MetadataRead::Ready(Self::replica_file_status_for_entry(
                    &inode, target, path,
                )?));
            }
            let store = self.rocks_store()?;
            let Some(inode) = Self::replica_inode(&store, target)? else {
                return Ok(MetadataRead::Missing);
            };
            if self.metadata_reader.file_status_version(target.inode_id) != version {
                continue;
            }
            if !self
                .metadata_reader
                .cache_file_status_if_current(&inode, version)?
            {
                continue;
            }
            if !self
                .metadata_reader
                .cache_file_inode_if_current(&inode, version)
            {
                continue;
            }
            let status = Self::replica_file_status_for_entry(&inode, target, path)?;
            if self.metadata_reader.file_status_version(target.inode_id) == version {
                return Ok(MetadataRead::Ready(status));
            }
        }

        Ok(MetadataRead::Retry)
    }

    /// The stable-path fallback holds both the target inode and every path
    /// edge lock. Those locks are the correctness fence; the cache version is
    /// intentionally not consulted because its fixed hash shards may also be
    /// invalidated by unrelated inodes.
    fn replica_file_status_from_locked_target(
        &self,
        target: &MetadataReplicaPathEntry,
        path: &str,
    ) -> FsResult<MetadataRead<FileStatus>> {
        let version = self.metadata_reader.file_status_version(target.inode_id);
        if let Some(inode) = self
            .metadata_reader
            .cached_file_inode(target.inode_id, version)
        {
            return Ok(MetadataRead::Ready(Self::replica_file_status_for_entry(
                &inode, target, path,
            )?));
        }
        let store = self.rocks_store()?;
        let Some(inode) = Self::replica_inode(&store, target)? else {
            return Ok(MetadataRead::Missing);
        };
        let status = Self::replica_file_status_for_entry(&inode, target, path)?;
        self.metadata_reader.cache_file_status(&inode)?;
        self.metadata_reader.cache_file_inode(&inode);
        Ok(MetadataRead::Ready(status))
    }

    fn replica_directory_file_status(
        &self,
        target: &MetadataReplicaPathEntry,
        path: &str,
    ) -> FsResult<MetadataRead<FileStatus>> {
        let directory = self
            .metadata_reader
            .directory_handle(target)
            .map_err(Self::fs_error_from_common)?;
        let status_snapshot = {
            let Some(status_snapshot) = directory.status_snapshot() else {
                return Ok(MetadataRead::Retry);
            };
            status_snapshot
        };
        self.replica_directory_file_status_with_snapshot(target, path, &status_snapshot)
    }

    /// Reads immutable inode state before the pessimistic directory read. The
    /// caller rechecks the inode version while holding the local directory
    /// locks, so RocksDB I/O never extends the write-blocking slow path.
    fn replica_directory_status_base(
        &self,
        target: &MetadataReplicaPathEntry,
        path: &str,
    ) -> FsResult<MetadataRead<(FileStatus, u64)>> {
        for _ in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let version = self.metadata_reader.file_status_version(target.inode_id);
            if let Some(status) = self.metadata_reader.cached_file_status_at_version(
                target.inode_id,
                version,
                path,
                target.name.as_ref(),
            ) {
                return Ok(MetadataRead::Ready((status, version)));
            }

            let store = self.rocks_store()?;
            let snapshot = store.snapshot();
            let Some(mut inode) = Self::replica_snapshot_inode(&snapshot, target)? else {
                return Ok(MetadataRead::Missing);
            };
            if self.metadata_reader.file_status_version(target.inode_id) != version
                || !self
                    .metadata_reader
                    .cache_file_status_if_current(&inode, version)?
            {
                continue;
            }

            inode.change_name(target.name.to_string());
            let status = Self::replica_file_status(&inode, path, None)?;
            if self.metadata_reader.file_status_version(target.inode_id) == version {
                return Ok(MetadataRead::Ready((status, version)));
            }
        }

        Ok(MetadataRead::Retry)
    }

    fn replica_directory_file_status_with_snapshot(
        &self,
        target: &MetadataReplicaPathEntry,
        path: &str,
        status_snapshot: &crate::master::meta::inode::DirectoryStatusSnapshot,
    ) -> FsResult<MetadataRead<FileStatus>> {
        if let Some(status) =
            self.metadata_reader
                .cached_file_status(target.inode_id, path, target.name.as_ref())
        {
            return Ok(status_snapshot
                .is_current()
                .then(|| Self::with_directory_status(status, status_snapshot))
                .map_or(MetadataRead::Retry, MetadataRead::Ready));
        }

        let version = self.metadata_reader.file_status_version(target.inode_id);
        let store = self.rocks_store()?;
        let snapshot = store.snapshot();
        let inode = Self::replica_snapshot_inode(&snapshot, target)?;
        let Some(mut inode) = inode else {
            return Ok(MetadataRead::Missing);
        };
        if self.metadata_reader.file_status_version(target.inode_id) != version
            || !self
                .metadata_reader
                .cache_file_status_if_current(&inode, version)?
            || !status_snapshot.is_current()
        {
            return Ok(MetadataRead::Retry);
        }
        inode.change_name(target.name.to_string());
        let status = Self::replica_file_status(&inode, path, None)?;
        Ok(MetadataRead::Ready(Self::with_directory_status(
            status,
            status_snapshot,
        )))
    }

    fn with_directory_status(
        mut status: FileStatus,
        snapshot: &crate::master::meta::inode::DirectoryStatusSnapshot,
    ) -> FileStatus {
        if let Some(attributes) = snapshot.attributes {
            status.mtime = attributes.mtime;
            status.nlink = attributes.nlink;
            status.x_attr.insert(
                INTERNAL_CTIME_XATTR.to_string(),
                attributes.ctime.to_le_bytes().to_vec(),
            );
        }
        let child_count = i32::try_from(snapshot.child_count).unwrap_or(i32::MAX);
        status.children_num = child_count;
        status.len = i64::from(child_count);
        status
    }

    fn replica_directory_statuses(
        &self,
        target: &MetadataReplicaPathEntry,
        path: &str,
        opts: &ListOptions,
    ) -> FsResult<MetadataRead<Vec<FileStatus>>> {
        let store = self.rocks_store()?;
        let children = self
            .metadata_reader
            .directory_entries(target, opts)
            .map_err(Self::fs_error_from_common)?;
        let statuses = store
            .batched_file_statuses_skip_missing(path, children.entries.iter().collect())
            .map_err(Self::fs_error_from_common)?;
        if children.is_current() {
            Ok(MetadataRead::Ready(statuses))
        } else {
            Ok(MetadataRead::Retry)
        }
    }

    fn list_status_from_replica(
        &self,
        path: &str,
        opts: Option<&ListOptions>,
    ) -> FsResult<Vec<FileStatus>> {
        let opts = ListOptions {
            limit: opts.and_then(|opts| opts.limit),
            start_after: opts.and_then(|opts| opts.start_after.as_ref()).cloned(),
        };
        if let Some(MetadataRead::Ready(statuses)) = self
            .metadata_reader
            .with_resolved_path(path, |target| {
                Ok(self.replica_list_status_for_target(target, path, &opts)?)
            })
            .map_err(Self::fs_error_from_common)?
        {
            return Ok(statuses);
        }

        for _ in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let (component_count, resolved) = self.metadata_reader.resolve(path)?;
            if !resolved.is_full(component_count) {
                if self.metadata_reader.validate(&resolved) {
                    return err_ext!(FsError::file_not_found(path));
                }
                Self::retry_metadata_read();
                continue;
            }
            let target = try_option!(resolved.target().cloned(), "File {} not exists", path);
            let Some(statuses) = self.read_if_path_current(
                &resolved,
                self.replica_list_status_for_target(&target, path, &opts),
            )?
            else {
                Self::retry_metadata_read();
                continue;
            };
            match statuses {
                MetadataRead::Ready(statuses) if self.metadata_reader.validate(&resolved) => {
                    return Ok(statuses)
                }
                MetadataRead::Missing if self.metadata_reader.validate(&resolved) => {
                    return err_ext!(FsError::file_not_found(path));
                }
                _ => {}
            }
            Self::retry_metadata_read();
        }

        Self::require_metadata_read(
            path,
            self.read_until_path_current(path, |target| {
                self.replica_list_status_for_target(target, path, &opts)
            })?,
        )
    }

    fn replica_list_status_for_target(
        &self,
        target: &MetadataReplicaPathEntry,
        path: &str,
        opts: &ListOptions,
    ) -> FsResult<MetadataRead<Vec<FileStatus>>> {
        if target.is_dir {
            return self.replica_directory_statuses(target, path, opts);
        }

        self.replica_file_status_for_target(target, path)
            .map(|status| match status {
                MetadataRead::Ready(status) => {
                    MetadataRead::Ready(Self::list_replica_single_file(status, opts))
                }
                MetadataRead::Missing => MetadataRead::Missing,
                MetadataRead::Retry => MetadataRead::Retry,
            })
    }

    fn list_replica_single_file(status: FileStatus, opts: &ListOptions) -> Vec<FileStatus> {
        if matches!(opts.limit, Some(0)) {
            return vec![];
        }
        if let Some(start_after) = opts.start_after.as_deref() {
            if status.name.as_str() <= start_after {
                return vec![];
            }
        }
        vec![status]
    }

    fn resolve_path(fs_dir: &FsDir, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store)
    }

    fn resolve_exclusive_path(fs_dir: &FsDir, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve_exclusive(fs_dir.root_ptr(), path, &fs_dir.store)
    }

    fn lock_path_for_write(
        &self,
        path: &str,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> CommonResult<InodeLockSet<'_>> {
        Ok(self
            .lock_resolved_path_for_write(path, target_mode, parent_write)?
            .inode_locks)
    }

    fn lock_resolved_path_for_write(
        &self,
        path: &str,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> CommonResult<LockedPath<'_>> {
        for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let (component_count, resolved, views) =
                self.metadata_reader.resolve_for_write(path)?;
            let requests = Self::metadata_inode_lock_requests(
                &resolved,
                component_count,
                target_mode,
                parent_write,
            );
            let locks = self.inode_locks.lock_many(&requests);
            let covered = {
                let fs_dir = self.fs_dir.read();
                let current = if self.metadata_reader.validate(&resolved) {
                    InodePath::from_views(path, views, &fs_dir.store)?
                } else {
                    Self::resolve_path(&fs_dir, path)?
                };
                Self::inode_locks_cover_path(&locks, &current, target_mode, parent_write)
                    .then_some(current)
            };
            if let Some(path) = covered {
                return Ok(LockedPath {
                    inode_locks: locks,
                    path,
                });
            }
            drop(locks);
            Self::retry_namespace_lock(path, attempt)?;
        }

        err_box!(
            "namespace path {} changed while acquiring locks after {} retries",
            path,
            NAMESPACE_LOCK_RETRY_LIMIT
        )
    }

    fn lock_path_and_inode_for_write(
        &self,
        path: &str,
        inode_id: Option<i64>,
    ) -> CommonResult<InodeLockSet<'_>> {
        for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let requests = {
                let store = self.rocks_store()?;
                let resolved = StorePathResolver::new(&store).resolve(path)?;
                Self::path_and_inode_lock_requests(&resolved, inode_id)
            };
            let locks = self.inode_locks.lock_many(&requests);
            let current_requests = {
                let store = self.rocks_store()?;
                let current = StorePathResolver::new(&store).resolve(path)?;
                Self::path_and_inode_lock_requests(&current, inode_id)
            };
            if locks.covers_requests(&current_requests) {
                return Ok(locks);
            }
            drop(locks);
            Self::retry_namespace_lock(path, attempt)?;
        }

        err_box!(
            "namespace path {} changed while acquiring path+inode locks after {} retries",
            path,
            NAMESPACE_LOCK_RETRY_LIMIT
        )
    }

    fn path_and_inode_lock_requests(
        resolved: &StoreResolvedPath,
        inode_id: Option<i64>,
    ) -> Vec<InodeLockRequest> {
        let mut requests = Self::store_inode_lock_requests(resolved, InodeLockMode::Write, false);
        if let Some(inode_id) = inode_id.filter(|id| *id > 0) {
            requests.push(InodeLockRequest::write(0, inode_id));
        }
        requests
    }

    fn lock_delete_path(
        &self,
        path: &str,
        recursive: bool,
    ) -> CommonResult<(InodeLockSet<'_>, Vec<i64>)> {
        for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let requests = {
                let store = self.rocks_store()?;
                let resolver = StorePathResolver::new(&store);
                let resolved = resolver.resolve(path)?;
                let subtree = resolver.collect_resolved_subtree(&resolved, recursive)?;
                Self::delete_lock_requests(&resolved, recursive, &subtree)
            };
            let locks = self.inode_locks.lock_many(&requests);
            let (current_requests, block_ids) = {
                let store = self.rocks_store()?;
                let resolver = StorePathResolver::new(&store);
                let resolved = resolver.resolve(path)?;
                let subtree = resolver.collect_resolved_subtree(&resolved, recursive)?;
                (
                    Self::delete_lock_requests(&resolved, recursive, &subtree),
                    subtree.block_ids,
                )
            };
            if locks.covers_requests(&current_requests) {
                return Ok((locks, block_ids));
            }
            drop(locks);
            Self::retry_namespace_lock(path, attempt)?;
        }

        err_box!(
            "namespace path {} changed while acquiring delete locks after {} retries",
            path,
            NAMESPACE_LOCK_RETRY_LIMIT
        )
    }

    fn delete_lock_requests(
        resolved: &StoreResolvedPath,
        recursive: bool,
        subtree: &StoreSubtreeSummary,
    ) -> Vec<InodeLockRequest> {
        let mut requests = Self::store_inode_lock_requests(resolved, InodeLockMode::Write, false);
        if recursive {
            requests.extend(
                subtree
                    .inodes
                    .iter()
                    .map(|inode| InodeLockRequest::write(inode.depth, inode.inode_id)),
            );
        }
        requests
    }

    fn lock_free_path(
        &self,
        path: &str,
        recursive: bool,
    ) -> CommonResult<(InodeLockSet<'_>, Vec<i64>)> {
        for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let requests = {
                let store = self.rocks_store()?;
                let resolver = StorePathResolver::new(&store);
                let resolved = resolver.resolve(path)?;
                let subtree = resolver.collect_resolved_subtree(&resolved, recursive)?;
                Self::free_lock_requests(&resolved, recursive, &subtree)
            };
            let locks = self.inode_locks.lock_many(&requests);
            let (current_requests, block_ids) = {
                let store = self.rocks_store()?;
                let resolver = StorePathResolver::new(&store);
                let resolved = resolver.resolve(path)?;
                let subtree = resolver.collect_resolved_subtree(&resolved, recursive)?;
                (
                    Self::free_lock_requests(&resolved, recursive, &subtree),
                    subtree.block_ids,
                )
            };
            if locks.covers_requests(&current_requests) {
                return Ok((locks, block_ids));
            }
            drop(locks);
            Self::retry_namespace_lock(path, attempt)?;
        }

        err_box!(
            "namespace path {} changed while acquiring free locks after {} retries",
            path,
            NAMESPACE_LOCK_RETRY_LIMIT
        )
    }

    fn free_lock_requests(
        resolved: &StoreResolvedPath,
        recursive: bool,
        subtree: &StoreSubtreeSummary,
    ) -> Vec<InodeLockRequest> {
        if !recursive {
            return Self::store_inode_lock_requests(resolved, InodeLockMode::Write, false);
        }

        let mut requests = Self::store_inode_lock_requests(resolved, InodeLockMode::Read, false);
        requests.extend(subtree.inodes.iter().map(|inode| {
            if inode.is_file() {
                InodeLockRequest::write(inode.depth, inode.inode_id)
            } else {
                InodeLockRequest::read(inode.depth, inode.inode_id)
            }
        }));
        requests
    }

    fn lock_link_paths(&self, src: &str, dst: &str) -> CommonResult<InodeLockSet<'_>> {
        for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let (src_component_count, src_path) = self.metadata_reader.resolve(src)?;
            let (dst_component_count, dst_path) = self.metadata_reader.resolve(dst)?;
            let requests = Self::metadata_link_lock_requests(
                &src_path,
                src_component_count,
                &dst_path,
                dst_component_count,
            );
            let locks = self.inode_locks.lock_many(&requests);
            let covered = {
                let fs_dir = self.fs_dir.read();
                let current_src = Self::resolve_path(&fs_dir, src)?;
                let current_dst = Self::resolve_path(&fs_dir, dst)?;
                Self::inode_locks_cover_link_paths(&locks, &current_src, &current_dst)
            };
            if covered {
                return Ok(locks);
            }
            drop(locks);
            Self::retry_namespace_lock(dst, attempt)?;
        }

        err_box!(
            "namespace paths {} -> {} changed while acquiring link locks after {} retries",
            src,
            dst,
            NAMESPACE_LOCK_RETRY_LIMIT
        )
    }

    fn metadata_link_lock_requests(
        src: &MetadataReplicaPath,
        src_component_count: usize,
        dst: &MetadataReplicaPath,
        dst_component_count: usize,
    ) -> Vec<InodeLockRequest> {
        let mut requests = Self::metadata_inode_lock_requests(
            src,
            src_component_count,
            InodeLockMode::Write,
            false,
        );
        let dst_parent_write = !dst.is_full(dst_component_count)
            && !Self::metadata_path_has_existing_parent_only(dst, dst_component_count);
        requests.extend(Self::metadata_inode_lock_requests(
            dst,
            dst_component_count,
            InodeLockMode::Write,
            dst_parent_write,
        ));
        requests
    }

    fn metadata_path_has_existing_parent_only(
        path: &MetadataReplicaPath,
        component_count: usize,
    ) -> bool {
        !path.is_full(component_count) && path.entries.len().saturating_add(1) == component_count
    }

    fn lock_rename_paths(
        &self,
        src: &str,
        dst: &str,
        flags: RenameFlags,
        allow_same_parent_read: bool,
    ) -> FsResult<RenameLockSet<'_>> {
        for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            // The candidate copies namespace edges under directory read locks.
            // Same-parent renames validate those exact edges again while holding
            // their child-shard write lock, avoiding seqlock starvation under a
            // write-hot directory before an inode lock can be acquired.
            let candidate = self.metadata_rename_lock_candidates(
                src,
                dst,
                allow_same_parent_read && !flags.exchange_mode(),
            )?;
            let locks = self.inode_locks.lock_many(&candidate.requests);
            if let Some(same_parent) = candidate.same_parent {
                // The parent read lock now prevents a subsequent parent move.
                // Validate only the ancestor edges that lead to that parent;
                // source/destination child edges are checked by rename_same_parent
                // under their shard write guard and may legitimately change.
                let parent_components = same_parent.src_component_count.saturating_sub(1);
                if self
                    .metadata_reader
                    .validate_prefix(&same_parent.src_path, parent_components)
                {
                    let replaced_block_ids = if flags.exchange_mode() {
                        vec![]
                    } else {
                        self.metadata_replaced_block_ids(&same_parent)?
                    };
                    return Ok(RenameLockSet {
                        _inode_locks: locks,
                        replaced_block_ids,
                        same_parent: Some(same_parent),
                    });
                }
                drop(locks);
                Self::retry_namespace_lock(src, attempt)?;
                continue;
            }

            let Some(current) = self.stable_metadata_rename_lock_plan(
                src,
                dst,
                flags,
                allow_same_parent_read && !flags.exchange_mode(),
            )?
            else {
                drop(locks);
                Self::retry_namespace_lock(src, attempt)?;
                continue;
            };
            if locks.covers_requests(&current.requests) {
                let replaced_block_ids = if flags.exchange_mode() {
                    vec![]
                } else {
                    match &current.same_parent {
                        Some(same_parent) => self.metadata_replaced_block_ids(same_parent)?,
                        None => self.rename_replaced_block_ids_after_locks(dst)?,
                    }
                };
                return Ok(RenameLockSet {
                    _inode_locks: locks,
                    replaced_block_ids,
                    same_parent: current.same_parent,
                });
            }
            drop(locks);
            Self::retry_namespace_lock(src, attempt)?;
        }

        err_box!(
            "namespace paths {} -> {} changed while acquiring rename locks after {} retries",
            src,
            dst,
            NAMESPACE_LOCK_RETRY_LIMIT
        )
    }

    fn metadata_rename_lock_candidates(
        &self,
        src: &str,
        dst: &str,
        allow_same_parent_read: bool,
    ) -> CommonResult<MetadataRenameLockPlan> {
        let (src_component_count, src_path) = self.metadata_reader.resolve(src)?;
        let (dst_component_count, dst_path) = self.metadata_reader.resolve(dst)?;
        Ok(Self::metadata_rename_lock_plan(
            src_path,
            src_component_count,
            dst_path,
            dst_component_count,
            allow_same_parent_read,
        ))
    }

    fn stable_metadata_rename_lock_plan(
        &self,
        src: &str,
        dst: &str,
        flags: RenameFlags,
        allow_same_parent_read: bool,
    ) -> FsResult<Option<MetadataRenameLockPlan>> {
        let (src_component_count, src_path) = self.metadata_reader.resolve(src)?;
        let (dst_component_count, dst_path) = self.metadata_reader.resolve(dst)?;
        if !self.metadata_reader.validate(&src_path) || !self.metadata_reader.validate(&dst_path) {
            return Ok(None);
        }

        Self::validate_metadata_rename_target(
            &src_path,
            src_component_count,
            &dst_path,
            dst_component_count,
            dst,
            flags,
            &self.metadata_reader,
        )?;

        if !self.metadata_reader.validate(&src_path) || !self.metadata_reader.validate(&dst_path) {
            return Ok(None);
        }

        Ok(Some(Self::metadata_rename_lock_plan(
            src_path,
            src_component_count,
            dst_path,
            dst_component_count,
            allow_same_parent_read,
        )))
    }

    fn metadata_rename_lock_plan(
        src_path: MetadataReplicaPath,
        src_component_count: usize,
        dst_path: MetadataReplicaPath,
        dst_component_count: usize,
        allow_same_parent_read: bool,
    ) -> MetadataRenameLockPlan {
        let same_parent = allow_same_parent_read
            .then(|| {
                Self::same_parent_rename_plan(
                    &src_path,
                    src_component_count,
                    &dst_path,
                    dst_component_count,
                )
            })
            .flatten();
        let parent_write = same_parent.is_none();
        let mut requests = Self::metadata_inode_lock_requests(
            &src_path,
            src_component_count,
            InodeLockMode::Write,
            parent_write,
        );
        requests.extend(Self::metadata_inode_lock_requests(
            &dst_path,
            dst_component_count,
            InodeLockMode::Write,
            parent_write,
        ));
        requests.sort_by_key(|request| (request.depth, request.inode_id));
        MetadataRenameLockPlan {
            requests,
            same_parent,
        }
    }

    fn same_parent_rename_plan(
        src_path: &MetadataReplicaPath,
        src_component_count: usize,
        dst_path: &MetadataReplicaPath,
        dst_component_count: usize,
    ) -> Option<SameParentRenamePlan> {
        if !src_path.is_full(src_component_count)
            || (!dst_path.is_full(dst_component_count)
                && dst_path.entries.len() + 1 != dst_component_count)
        {
            return None;
        }
        let src_parent = Self::metadata_rename_parent(src_path, src_component_count)?;
        let dst_parent = Self::metadata_rename_parent(dst_path, dst_component_count)?;
        (src_parent.inode_id == dst_parent.inode_id).then(|| SameParentRenamePlan {
            src_path: src_path.clone(),
            src_component_count,
            dst_path: dst_path.clone(),
            dst_component_count,
        })
    }

    fn metadata_rename_parent(
        path: &MetadataReplicaPath,
        component_count: usize,
    ) -> Option<&MetadataReplicaPathEntry> {
        if path.is_full(component_count) {
            return path.entries.get(path.entries.len().checked_sub(2)?);
        }
        (path.entries.len() + 1 == component_count)
            .then(|| path.entries.last())
            .flatten()
    }

    fn validate_metadata_rename_target(
        src_path: &MetadataReplicaPath,
        src_component_count: usize,
        dst_path: &MetadataReplicaPath,
        dst_component_count: usize,
        dst: &str,
        flags: RenameFlags,
        metadata_reader: &MetadataReplicaReader,
    ) -> FsResult<()> {
        // Exchange updates only the two source/destination directory edges.
        // The caller locks both resolved paths; no destination subtree is removed.
        if flags.exchange_mode() {
            return Ok(());
        }

        let Some(src_inode) = src_path.target() else {
            return Ok(());
        };
        let Some(dst_inode) = dst_path.target() else {
            return Ok(());
        };
        if !src_path.is_full(src_component_count) || !dst_path.is_full(dst_component_count) {
            return Ok(());
        }

        if flags.no_replace() {
            return Err(FsError::file_exists(dst));
        }

        if !src_inode.is_dir && dst_inode.is_dir {
            return Err(FsError::is_a_directory(dst));
        }
        if src_inode.is_dir && !dst_inode.is_dir {
            return Err(FsError::not_a_directory(dst));
        }
        if src_inode.is_dir && dst_inode.is_dir && !metadata_reader.directory_is_empty(dst_inode)? {
            return Err(FsError::dir_not_empty(dst));
        }

        Ok(())
    }

    fn metadata_inode_lock_requests(
        path: &MetadataReplicaPath,
        component_count: usize,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> Vec<InodeLockRequest> {
        let last_index = path.entries.len().saturating_sub(1);
        let parent_index = if path.is_full(component_count) {
            last_index.saturating_sub(1)
        } else {
            last_index
        };

        path.entries
            .iter()
            .enumerate()
            .map(|(index, inode)| {
                let mode = if parent_write && index == parent_index {
                    InodeLockMode::Write
                } else if path.is_full(component_count) && index == last_index {
                    target_mode
                } else {
                    InodeLockMode::Read
                };
                InodeLockRequest {
                    depth: index,
                    inode_id: inode.inode_id,
                    mode,
                }
            })
            .collect()
    }

    fn rename_replaced_block_ids_after_locks(&self, dst: &str) -> FsResult<Vec<i64>> {
        let fs_dir = self.fs_dir.read();
        let dst_path = Self::resolve_path(&fs_dir, dst)?;
        Self::rename_replaced_block_ids(&fs_dir, &dst_path)
    }

    fn metadata_replaced_block_ids(&self, plan: &SameParentRenamePlan) -> FsResult<Vec<i64>> {
        if !plan.dst_path.is_full(plan.dst_component_count) {
            return Ok(vec![]);
        }
        let dst = match plan.dst_path.target() {
            Some(dst) => dst,
            None => return err_box!("Destination metadata path has no target"),
        };
        if dst.is_dir {
            return Ok(vec![]);
        }
        let store = self.rocks_store()?;
        let Some(inode) = store.get_inode(dst.inode_id)? else {
            // The candidate may have observed a destination which a preceding
            // rename removed before this operation acquired its inode lock.
            // The child-edge check in rename_same_parent will reject that plan.
            return Ok(vec![]);
        };
        match inode {
            InodeView::File(file) => Ok(file.blocks.iter().map(|block| block.id).collect()),
            InodeView::Dir(_) => Ok(vec![]),
            InodeView::FileEntry(entry) => err_box!(
                "Destination inode {} unexpectedly resolves to file entry {}",
                dst.inode_id,
                entry.id()
            ),
        }
    }

    fn path_block_ids(fs_dir: &FsDir, path: &InodePath) -> FsResult<Vec<i64>> {
        if !path.is_full() {
            return Ok(vec![]);
        }
        let Some(inode) = path.get_last_inode() else {
            return Ok(vec![]);
        };

        let mut inode = inode;
        loop {
            match inode.as_ref() {
                InodeView::File(file) => {
                    return Ok(file.blocks.iter().map(|block| block.id).collect())
                }
                // A directory replacement is legal only after the lock plan
                // observes it empty, so it has no block locations to protect.
                InodeView::Dir(_) => return Ok(vec![]),
                InodeView::FileEntry(entry) => {
                    let resolved = try_option!(
                        fs_dir.store.get_inode(entry.id(), Some(entry.name()))?,
                        "Failed to load linked inode {} while collecting block ids",
                        entry.id()
                    );
                    inode = InodePtr::from_owned(resolved);
                }
            }
        }
    }

    fn rename_replaced_block_ids(fs_dir: &FsDir, dst_path: &InodePath) -> FsResult<Vec<i64>> {
        Self::path_block_ids(fs_dir, dst_path)
    }

    fn lock_set_attr_path(&self, path: &str, recursive: bool) -> CommonResult<InodeLockSet<'_>> {
        for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let requests = {
                let store = self.rocks_store()?;
                let resolver = StorePathResolver::new(&store);
                let resolved = resolver.resolve(path)?;
                let subtree = resolver.collect_resolved_subtree(&resolved, recursive)?;
                Self::set_attr_lock_requests(&resolved, recursive, &subtree)
            };
            let locks = self.inode_locks.lock_many(&requests);
            let current_requests = {
                let store = self.rocks_store()?;
                let resolver = StorePathResolver::new(&store);
                let resolved = resolver.resolve(path)?;
                let subtree = resolver.collect_resolved_subtree(&resolved, recursive)?;
                Self::set_attr_lock_requests(&resolved, recursive, &subtree)
            };
            if locks.covers_requests(&current_requests) {
                return Ok(locks);
            }
            drop(locks);
            Self::retry_namespace_lock(path, attempt)?;
        }

        err_box!(
            "namespace path {} changed while acquiring set_attr locks after {} retries",
            path,
            NAMESPACE_LOCK_RETRY_LIMIT
        )
    }

    fn set_attr_lock_requests(
        resolved: &StoreResolvedPath,
        recursive: bool,
        subtree: &StoreSubtreeSummary,
    ) -> Vec<InodeLockRequest> {
        let mut requests = Self::store_inode_lock_requests(resolved, InodeLockMode::Write, false);
        if recursive {
            requests.extend(
                subtree
                    .inodes
                    .iter()
                    .map(|inode| InodeLockRequest::write(inode.depth, inode.inode_id)),
            );
        }
        requests
    }

    fn store_inode_lock_requests(
        path: &StoreResolvedPath,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> Vec<InodeLockRequest> {
        let last_index = path.inodes.len().saturating_sub(1);
        let parent_index = if path.is_full() {
            last_index.saturating_sub(1)
        } else {
            last_index
        };

        path.inodes
            .iter()
            .enumerate()
            .map(|(index, inode)| {
                let mode = if parent_write && index == parent_index {
                    InodeLockMode::Write
                } else if path.is_full() && index == last_index {
                    target_mode
                } else {
                    InodeLockMode::Read
                };
                InodeLockRequest {
                    depth: index,
                    inode_id: inode.id(),
                    mode,
                }
            })
            .collect()
    }

    fn inode_path_lock_requests(
        path: &InodePath,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> Vec<InodeLockRequest> {
        let last_index = path.inodes.len().saturating_sub(1);
        let parent_index = if path.is_full() {
            last_index.saturating_sub(1)
        } else {
            last_index
        };

        path.inodes
            .iter()
            .enumerate()
            .map(|(index, inode)| {
                let mode = if parent_write && index == parent_index {
                    InodeLockMode::Write
                } else if path.is_full() && index == last_index {
                    target_mode
                } else {
                    InodeLockMode::Read
                };
                InodeLockRequest {
                    depth: index,
                    inode_id: inode.id(),
                    mode,
                }
            })
            .collect()
    }

    fn create_inode_locks_cover(
        locks: &InodeLockSet,
        path: &InodePath,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> bool {
        Self::inode_locks_cover_path(locks, path, target_mode, parent_write)
    }

    fn inode_locks_cover_path(
        locks: &InodeLockSet,
        path: &InodePath,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> bool {
        locks.covers_requests(&Self::inode_path_lock_requests(
            path,
            target_mode,
            parent_write,
        ))
    }

    fn inode_locks_cover_link_paths(
        locks: &InodeLockSet,
        src: &InodePath,
        dst: &InodePath,
    ) -> bool {
        let mut requests = Self::inode_path_lock_requests(src, InodeLockMode::Write, false);
        let dst_parent_write = !dst.is_full() && !Self::path_has_existing_parent_only(dst);
        requests.extend(Self::inode_path_lock_requests(
            dst,
            InodeLockMode::Write,
            dst_parent_write,
        ));
        locks.covers_requests(&requests)
    }

    fn read_until_path_current<R>(
        &self,
        path: &str,
        read: impl Fn(&MetadataReplicaPathEntry) -> FsResult<MetadataRead<R>>,
    ) -> FsResult<MetadataRead<R>> {
        self.record_metadata_read_fallback("stable_path");
        for _ in 0..METADATA_READ_FALLBACK_RETRY_LIMIT {
            let (component_count, resolved) = self.metadata_reader.resolve(path)?;
            let expected_target_id = resolved
                .is_full(component_count)
                .then(|| resolved.target().map(|target| target.inode_id))
                .flatten();
            let _target_lock =
                expected_target_id.map(|inode_id| self.inode_locks.read_inode(inode_id));

            match self.metadata_reader.with_stable_path(
                path,
                &resolved,
                component_count,
                |target| Ok(read(target)?),
            )? {
                StablePathRead::Ready(result @ MetadataRead::Ready(_))
                | StablePathRead::Ready(result @ MetadataRead::Missing) => return Ok(result),
                StablePathRead::Missing => return Ok(MetadataRead::Missing),
                StablePathRead::Ready(MetadataRead::Retry) | StablePathRead::Retry => {
                    Self::retry_metadata_read()
                }
            }
        }
        self.read_after_namespace_quiesce(path, read)
    }

    /// The directory status fallback obtains its immutable inode value before
    /// acquiring path edge locks. The locked section only validates that value
    /// and combines it with the current directory attributes, so RocksDB I/O
    /// cannot block concurrent child-edge mutations.
    fn read_file_status_until_path_current(
        &self,
        path: &str,
    ) -> FsResult<MetadataRead<FileStatus>> {
        self.record_metadata_read_fallback("file_status");
        for _ in 0..METADATA_READ_FALLBACK_RETRY_LIMIT {
            let (component_count, resolved) = self.metadata_reader.resolve(path)?;
            if !resolved.is_full(component_count) {
                if self.metadata_reader.validate(&resolved) {
                    return Ok(MetadataRead::Missing);
                }
                Self::retry_metadata_read();
                continue;
            }
            let target = try_option!(resolved.target().cloned(), "File {} not exists", path);
            if !target.is_dir {
                return self.read_until_path_current(path, |stable_target| {
                    self.replica_file_status_from_locked_target(stable_target, path)
                });
            }

            let (status, version) = match self.replica_directory_status_base(&target, path)? {
                MetadataRead::Ready(status) => status,
                MetadataRead::Missing if self.metadata_reader.validate(&resolved) => {
                    return Ok(MetadataRead::Missing)
                }
                MetadataRead::Missing | MetadataRead::Retry => {
                    Self::retry_metadata_read();
                    continue;
                }
            };

            let expected_inode_id = target.inode_id;
            let _target_lock = self.inode_locks.read_inode(expected_inode_id);
            match self.metadata_reader.with_stable_path(
                path,
                &resolved,
                component_count,
                |stable_target| {
                    if !stable_target.is_dir || stable_target.inode_id != expected_inode_id {
                        return Ok(MetadataRead::Retry);
                    }
                    let directory = self
                        .metadata_reader
                        .directory_handle(stable_target)
                        .map_err(Self::fs_error_from_common)?;
                    Ok(directory.with_status_read(|status_snapshot| {
                        if self.metadata_reader.file_status_version(expected_inode_id) != version {
                            return MetadataRead::Retry;
                        }
                        MetadataRead::Ready(Self::with_directory_status(status, status_snapshot))
                    }))
                },
            )? {
                StablePathRead::Ready(result @ MetadataRead::Ready(_))
                | StablePathRead::Ready(result @ MetadataRead::Missing) => return Ok(result),
                StablePathRead::Missing => return Ok(MetadataRead::Missing),
                StablePathRead::Ready(MetadataRead::Retry) | StablePathRead::Retry => {
                    Self::retry_metadata_read()
                }
            }
        }

        self.read_after_namespace_quiesce(path, |target| {
            self.replica_file_status_for_target(target, path)
        })
    }

    /// A path that is repeatedly created and removed has no stable local lock
    /// set until the missing edge is observed. After both optimistic and local
    /// conditional reads have exhausted their bounded attempts, temporarily
    /// quiesce namespace commits to give the read a real linearization point.
    /// This is a progress guarantee, not the normal metadata read path.
    fn read_after_namespace_quiesce<R>(
        &self,
        path: &str,
        read: impl Fn(&MetadataReplicaPathEntry) -> FsResult<MetadataRead<R>>,
    ) -> FsResult<MetadataRead<R>> {
        if let Ok(metrics) = Master::get_metrics() {
            metrics.metadata_topology_quiesce_total.inc();
        }
        let _barrier = self.namespace_topology_gate.close_and_wait();
        let (component_count, resolved) = self.metadata_reader.resolve(path)?;
        if !resolved.is_full(component_count) {
            return Ok(MetadataRead::Missing);
        }
        let target = try_option!(resolved.target(), "File {} not exists", path);
        let _target_lock = self.inode_locks.read_inode(target.inode_id);
        let result = read(target)?;
        if self.metadata_reader.validate(&resolved) {
            Ok(result)
        } else {
            Err(FsError::in_progress_msg(format!(
                "metadata path {} changed while namespace commits were quiesced",
                path
            )))
        }
    }

    fn require_metadata_read<T>(path: &str, result: MetadataRead<T>) -> FsResult<T> {
        match result {
            MetadataRead::Ready(value) => Ok(value),
            MetadataRead::Missing => err_ext!(FsError::file_not_found(path)),
            MetadataRead::Retry => Err(FsError::in_progress_msg(format!(
                "metadata path {} is changing; retry the request",
                path
            ))),
        }
    }

    fn retry_metadata_read() {
        std::thread::yield_now();
    }

    fn record_metadata_read_fallback(&self, stage: &str) {
        if let Ok(metrics) = Master::get_metrics() {
            metrics
                .metadata_read_fallback_total
                .with_label_values(&[stage])
                .inc();
        }
    }

    fn retry_namespace_lock(path: &str, attempt: usize) -> CommonResult<()> {
        if attempt + 1 >= NAMESPACE_LOCK_RETRY_LIMIT {
            return err_box!(
                "namespace path {} changed while acquiring locks after {} retries",
                path,
                NAMESPACE_LOCK_RETRY_LIMIT
            );
        }
        std::thread::yield_now();
        Ok(())
    }

    fn fs_error_from_common(error: CommonError) -> FsError {
        match error.downcast::<FsError>() {
            Ok(error) => *error,
            Err(error) => FsError::from(error),
        }
    }

    fn read_if_path_current<T>(
        &self,
        resolved: &MetadataReplicaPath,
        result: FsResult<T>,
    ) -> FsResult<Option<T>> {
        match result {
            Ok(value) => Ok(Some(value)),
            Err(FsError::FileNotFound(_)) if !self.metadata_reader.validate(resolved) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn commit_block_ids(commit_blocks: &[CommitBlock]) -> Vec<i64> {
        let mut block_ids = commit_blocks
            .iter()
            .map(|block| block.block_id)
            .collect::<Vec<_>>();
        block_ids.sort_unstable();
        block_ids.dedup();
        block_ids
    }

    fn commit_block_worker_ids(commit_blocks: &[CommitBlock]) -> Vec<u32> {
        let mut worker_ids = commit_blocks
            .iter()
            .flat_map(|block| block.locations.iter().map(|location| location.worker_id))
            .collect::<Vec<_>>();
        worker_ids.sort_unstable();
        worker_ids.dedup();
        worker_ids
    }

    fn commit_block_location_pairs(commit_blocks: &[CommitBlock]) -> Vec<(u32, i64)> {
        commit_blocks
            .iter()
            .flat_map(|block| {
                block
                    .locations
                    .iter()
                    .map(move |location| (location.worker_id, block.block_id))
            })
            .collect()
    }

    fn protect_committed_block_locations(&self, locations: &[(u32, i64)]) {
        let mut reports = self.full_block_reports.lock();
        if reports.is_empty() {
            return;
        }

        for (worker_id, block_id) in locations {
            if let Some(report) = reports.get_mut(worker_id) {
                report.protected_blocks.insert(*block_id);
            }
        }
    }

    fn reserve_journal_scope(&self, entry_count: usize) -> FsResult<JournalPermitScope> {
        self.journal_writer.reserve_scope(entry_count.max(1))
    }

    fn rocks_store(&self) -> CommonResult<RocksStoreReadGuard<'_>> {
        self.store_handle.read()
    }

    pub fn get_rocksdb_metrics(&self) -> CommonResult<HashMap<String, u64>> {
        self.rocks_store()?.get_rocksdb_metrics()
    }

    pub fn ttl_bucket_list(&self) -> Arc<TtlBucketList> {
        self.ttl_bucket_list.clone()
    }

    pub fn ttl_bucket_counts(&self) -> (usize, u64) {
        (
            self.ttl_bucket_list.buckets_len(),
            self.ttl_bucket_list.total_inodes(),
        )
    }

    fn next_op_id(&self) -> u64 {
        self.op_id.next()
    }

    pub fn get_inode_by_id(&self, inode_id: i64) -> FsResult<Option<InodeView>> {
        self.ensure_metadata_current()?;
        Ok(self.rocks_store()?.snapshot().get_inode(inode_id)?)
    }

    pub fn get_inodes_by_id(&self, inode_ids: &[i64]) -> FsResult<Vec<Option<InodeView>>> {
        self.ensure_metadata_current()?;
        Ok(self
            .rocks_store()?
            .snapshot()
            .get_inodes(inode_ids.iter().copied())?)
    }

    pub fn resolve_inode_path_by_id(&self, inode_id: i64) -> FsResult<String> {
        self.ensure_metadata_current()?;
        let store = self.rocks_store()?;
        let snapshot = store.snapshot();
        let path = self.build_inode_path_from_store(&snapshot, inode_id)?;
        Ok(path)
    }

    fn build_inode_path_from_store(
        &self,
        snapshot: &RocksInodeStoreSnapshot<'_>,
        inode_id: i64,
    ) -> CommonResult<String> {
        if inode_id == ROOT_INODE_ID {
            return Ok(PATH_SEPARATOR.to_string());
        }

        let mut current_id = inode_id;
        let mut names = Vec::new();
        for _ in 0..self.conf.max_path_depth {
            if current_id == ROOT_INODE_ID {
                names.reverse();
                return Ok(format!("{}{}", PATH_SEPARATOR, names.join(PATH_SEPARATOR)));
            }

            let inode = try_option!(
                snapshot.get_inode(current_id)?,
                "Cannot resolve path for inode {}",
                current_id
            );
            match inode {
                InodeView::File(file) => {
                    names.push(file.name.clone());
                    current_id = file.parent_id();
                }
                InodeView::Dir(dir) => {
                    names.push(dir.name.clone());
                    current_id = dir.parent_id();
                }
                InodeView::FileEntry(entry) => {
                    names.push(entry.name().to_string());
                    names.reverse();
                    return Ok(format!("{}{}", PATH_SEPARATOR, names.join(PATH_SEPARATOR)));
                }
            }
        }

        err_box!(
            "Cannot resolve path for inode {}: path depth exceeds {}",
            inode_id,
            self.conf.max_path_depth
        )
    }

    fn create_entries_for_resolved_path(path: &InodePath, create_parent: bool) -> usize {
        if path.is_full() || !create_parent {
            return 1;
        }

        path.components
            .len()
            .saturating_sub(path.inodes.len())
            .max(1)
    }

    fn path_has_existing_parent_only(path: &InodePath) -> bool {
        !path.is_full() && path.existing_len().saturating_add(1) == path.len()
    }

    fn estimate_link_entries(&self, path: &str) -> CommonResult<usize> {
        let store = self.rocks_store()?;
        let resolved = StorePathResolver::new(&store).resolve(path)?;
        let parent_entries = resolved
            .components
            .len()
            .saturating_sub(resolved.inodes.len().saturating_add(1));
        Ok(parent_entries.saturating_add(1).max(1))
    }

    pub fn check_path_length(&self, path: &str) -> CommonResult<()> {
        if path.len() > self.conf.max_path_len {
            return err_box!(
                "create: Path too long, limit {} characters",
                self.conf.max_path_len
            );
        }

        let depth = path.split(PATH_SEPARATOR).count();
        if depth > self.conf.max_path_depth {
            return err_box!(
                "create: Path too long, limit {} levels",
                self.conf.max_path_depth
            );
        }

        Ok(())
    }

    pub fn validate_add_block(
        file: &InodeFile,
        client_addr: &ClientAddress,
        previous: Option<&CommitBlock>,
    ) -> FsResult<ValidateAddBlock> {
        if let Some(v) = previous {
            if v.block_len != file.block_size as i64 {
                return err_box!(
                    "The block size is incorrect, block size: {}, commit block length: {}",
                    file.block_size,
                    v.block_len
                );
            }
        }

        let res = ValidateAddBlock {
            replicas: file.replicas as u16,
            block_size: file.block_size as i64,
            storage_policy: file.storage_policy.clone(),
            client_host: client_addr.hostname.clone(),
        };

        Ok(res)
    }

    pub fn choose_worker(
        &self,
        inp: &InodePath,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let mut inode = try_option!(inp.get_last_inode(), "File {} not exists", inp.path());
        let file = inode.as_file_mut()?;
        self.choose_worker_for_file(file, client_addr, exclude_workers)
    }

    pub fn choose_worker_for_file(
        &self,
        file: &InodeFile,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let wm = self.worker_manager.read();
        let validate_block = Self::validate_add_block(file, &client_addr, None)?;
        let choose_ctx = ChooseContext::with_block(validate_block, exclude_workers);
        Ok(wm.choose_worker(choose_ctx)?)
    }

    pub fn create_locate_block(
        &self,
        path: impl AsRef<str>,
        block: ExtendedBlock,
        locs: &[BlockLocation],
    ) -> FsResult<LocatedBlock> {
        self.worker_manager
            .read()
            .create_locate_block(path, block, locs)
    }

    pub fn resolve_file_inode(
        fs_dir: &FsDir,
        path: &str,
        inode_id: Option<i64>,
    ) -> FsResult<InodePtr> {
        match inode_id {
            Some(v) if v > 0 => match fs_dir.store.get_inode(v, None)? {
                Some(view) => Ok(InodePtr::from_owned(view)),
                None => err_box!("File inode {} not exists", v),
            },

            _ => {
                let inp = Self::resolve_path(fs_dir, path)?;
                match inp.task_last() {
                    Some(ptr) => Ok(ptr),
                    None => err_box!("File {} not exists", path),
                }
            }
        }
    }

    /// Document application to allocate a new block.
    #[allow(clippy::too_many_arguments)]
    pub fn add_block<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        client_addr: ClientAddress,
        commit_blocks: Vec<CommitBlock>,
        exclude_workers: Vec<u32>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        self.run_metadata_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let _inode_locks = self.lock_path_and_inode_for_write(path, inode_id)?;
            let commit_block_ids = Self::commit_block_ids(&commit_blocks);
            let commit_worker_ids = Self::commit_block_worker_ids(&commit_blocks);
            let commit_location_pairs = Self::commit_block_location_pairs(&commit_blocks);
            let fs_dir = self.fs_dir.read();
            let inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
            let file = inode.as_file_ref()?;

            // File allows concurrent writes, 'previous' is the previous block,
            // need to check if the next block has already been allocated。
            // If it has been allocated, return that block
            if let Some(next) = file.search_next_block(last_block.map(|v| v.id)) {
                let mut block_ids = commit_block_ids.clone();
                block_ids.push(next.id);
                block_ids.sort_unstable();
                block_ids.dedup();
                let _block_locks = self
                    .block_location_locks
                    .write_workers_blocks(&commit_worker_ids, &block_ids);
                let locs = fs_dir.get_block_locations(next.id)?;
                let extend_block = ExtendedBlock {
                    id: next.id,
                    len: next.len(),
                    storage_type: file.storage_policy.storage_type,
                    file_type: file.file_type,
                    alloc_opts: next.alloc_opts.clone(),
                };

                return self.create_locate_block(path, extend_block, &locs);
            }

            let _block_locks = self
                .block_location_locks
                .write_workers_blocks(&commit_worker_ids, &commit_block_ids);
            let choose_workers = self.choose_worker_for_file(file, client_addr, exclude_workers)?;
            let has_spdk = {
                let wm = self.worker_manager.read();
                wm.workers_have_spdk(&choose_workers)
            };
            let block =
                fs_dir.acquire_new_block(path, inode, commit_blocks, &choose_workers, file_len)?;
            self.protect_committed_block_locations(&commit_location_pairs);
            let located = LocatedBlock {
                block,
                locs: choose_workers,
                has_spdk,
            };

            Ok(located)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<Option<FileBlocks>> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush,
                return_file_blocks: true,
                set_attr_opts,
            },
        )
    }

    /// Flushes file metadata without building the full block-location snapshot.
    pub fn flush_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
    ) -> FsResult<()> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush: true,
                return_file_blocks: false,
                set_attr_opts: None,
            },
        )
        .map(|_| ())
    }

    fn complete_file0<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        options: CompleteFileOptions,
    ) -> FsResult<Option<FileBlocks>> {
        let path = path.as_ref();
        self.run_metadata_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let _inode_locks = self.lock_path_and_inode_for_write(path, inode_id)?;
            let commit_block_ids = Self::commit_block_ids(&commit_blocks);
            let commit_worker_ids = Self::commit_block_worker_ids(&commit_blocks);
            let commit_location_pairs = Self::commit_block_location_pairs(&commit_blocks);
            let fs_dir = self.fs_dir.read();
            let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
            let mut block_ids = commit_block_ids.clone();
            if options.only_flush {
                block_ids.extend(inode.as_file_ref()?.blocks.iter().map(|block| block.id));
                block_ids.sort_unstable();
                block_ids.dedup();
            }
            let _block_locks = self
                .block_location_locks
                .write_workers_blocks(&commit_worker_ids, &block_ids);
            fs_dir.complete_file(
                path,
                &mut inode,
                len,
                commit_blocks,
                client_name,
                options.only_flush,
                options.set_attr_opts,
            )?;
            self.protect_committed_block_locations(&commit_location_pairs);

            if options.only_flush && options.return_file_blocks {
                let file = inode.as_file_ref()?;
                let locs = self.get_block_locs(path, &fs_dir, file)?;
                let status = inode.to_file_status(path)?;
                Ok(Some(FileBlocks::new(status, locs)))
            } else {
                Ok(None)
            }
        })
    }

    pub fn get_file_blocks(
        &self,
        path: &str,
        fs_dir: &FsDir,
        inp: &InodePath,
    ) -> FsResult<FileBlocks> {
        let inode = try_option!(inp.get_last_inode(), "File {} not exists", path);
        let file = inode.as_file_ref()?;
        let blocks = self.get_block_locs(path, fs_dir, file)?;
        Ok(FileBlocks::new(inode.to_file_status(path)?, blocks))
    }

    fn get_block_locs(
        &self,
        path: &str,
        fs_dir: &FsDir,
        file: &InodeFile,
    ) -> FsResult<Vec<LocatedBlock>> {
        let wm = self.worker_manager.read();
        let file_locs = fs_dir.get_file_locations(file)?;
        let mut block_locs = Vec::with_capacity(file_locs.len());

        for (index, meta) in file.blocks.iter().enumerate() {
            if index + 1 < file.blocks.len() && meta.len() != file.block_size as i64 {
                return err_box!(
                    "block status abnormal, block id {}, block len {}, expected block size {}",
                    meta.id,
                    meta.len(),
                    file.block_size
                );
            }

            let extend_block = ExtendedBlock {
                id: meta.id,
                len: meta.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: meta.alloc_opts.clone(),
            };

            let lc = try_option!(
                file_locs.get(&meta.id),
                "File {}, block {} Lost (no worker can read)",
                path,
                meta.id
            );
            let lb = wm.create_locate_block(path, extend_block, lc)?;
            block_locs.push(lb);
        }

        Ok(block_locs)
    }

    pub fn get_block_locations<T: AsRef<str>>(&self, path: T) -> FsResult<FileBlocks> {
        self.ensure_metadata_current()?;
        self.get_block_locations_unchecked(path)
    }

    pub(crate) fn get_block_locations_unchecked<T: AsRef<str>>(
        &self,
        path: T,
    ) -> FsResult<FileBlocks> {
        let path = path.as_ref();
        if let Some(MetadataRead::Ready(blocks)) = self
            .metadata_reader
            .with_resolved_path(path, |target| {
                Ok(self.replica_file_blocks_for_target(target, path)?)
            })
            .map_err(Self::fs_error_from_common)?
        {
            return Ok(blocks);
        }

        for _ in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let (component_count, resolved) = self.metadata_reader.resolve(path)?;
            if !resolved.is_full(component_count) {
                if self.metadata_reader.validate(&resolved) {
                    return err_ext!(FsError::file_not_found(path));
                }
                Self::retry_metadata_read();
                continue;
            }
            let target = try_option!(resolved.target().cloned(), "File {} not exists", path);
            let Some(blocks) = self.read_if_path_current(
                &resolved,
                self.replica_file_blocks_for_target(&target, path),
            )?
            else {
                Self::retry_metadata_read();
                continue;
            };
            match blocks {
                MetadataRead::Ready(blocks) if self.metadata_reader.validate(&resolved) => {
                    return Ok(blocks)
                }
                MetadataRead::Missing if self.metadata_reader.validate(&resolved) => {
                    return err_ext!(FsError::file_not_found(path));
                }
                _ => {}
            }
            Self::retry_metadata_read();
        }

        Self::require_metadata_read(
            path,
            self.read_until_path_current(path, |target| {
                self.replica_file_blocks_from_locked_target(target, path)
            })?,
        )
    }

    fn replica_file_blocks_for_target(
        &self,
        target: &MetadataReplicaPathEntry,
        path: &str,
    ) -> FsResult<MetadataRead<FileBlocks>> {
        if target.is_dir {
            return Ok(MetadataRead::Missing);
        }

        let _inode_lock = self.inode_locks.read_inode(target.inode_id);
        self.replica_file_blocks_from_locked_target(target, path)
    }

    /// Reads block metadata while the caller holds the target inode lock.
    ///
    /// `read_until_path_current` and `read_after_namespace_quiesce` acquire
    /// that lock before validating the path. Reacquiring it here would violate
    /// the inode lock manager's non-reentrancy invariant and can self-deadlock
    /// behind a waiting writer.
    fn replica_file_blocks_from_locked_target(
        &self,
        target: &MetadataReplicaPathEntry,
        path: &str,
    ) -> FsResult<MetadataRead<FileBlocks>> {
        if target.is_dir {
            return Ok(MetadataRead::Missing);
        }

        let version = self.metadata_reader.file_status_version(target.inode_id);
        let inode = match self
            .metadata_reader
            .cached_file_inode(target.inode_id, version)
        {
            Some(inode) => inode,
            None => {
                let store = self.rocks_store()?;
                let Some(inode) = Self::replica_inode(&store, target)? else {
                    return Ok(MetadataRead::Missing);
                };
                if !self
                    .metadata_reader
                    .cache_file_status_if_current(&inode, version)?
                {
                    return Ok(MetadataRead::Retry);
                }
                if !self
                    .metadata_reader
                    .cache_file_inode_if_current(&inode, version)
                {
                    return Ok(MetadataRead::Retry);
                }
                Arc::new(inode)
            }
        };
        let file = inode.as_file_ref()?.clone();
        let external_location_block_ids = file
            .blocks
            .iter()
            .filter(|meta| meta.locs.is_none())
            .map(|meta| meta.id)
            .collect::<Vec<_>>();
        let status = Self::replica_file_status_for_entry(&inode, target, path)?;

        if file.blocks.is_empty() {
            return Ok(MetadataRead::Ready(FileBlocks {
                status,
                block_locs: vec![],
            }));
        }

        // Inline locations are part of the same inode value as the block list. Only
        // locations stored in CF_BLOCK need a block lock to avoid cross-report reads.
        let _block_locks = self
            .block_location_locks
            .read_blocks(&external_location_block_ids);
        let store = self.rocks_store()?;
        let block_locs = self.get_block_locs_from_store_locked(path, &store, &file)?;
        Ok(MetadataRead::Ready(FileBlocks { status, block_locs }))
    }

    pub fn cv_metadata_snapshot_page(
        &self,
        page_token: Option<String>,
        page_size: usize,
    ) -> FsResult<CvMetadataSnapshotPage> {
        if page_size == 0 {
            return err_box!("cv metadata snapshot page_size must be greater than 0");
        }

        let fs_dir = self.fs_dir.read();
        let epoch = fs_dir.op_id.get();
        let start_after = page_token.filter(|token| !token.is_empty());
        let mut entries = Vec::with_capacity(page_size.saturating_add(1));
        self.collect_cv_metadata_snapshot_page(
            &fs_dir,
            fs_dir.root_dir(),
            "/",
            start_after.as_deref(),
            page_size.saturating_add(1),
            &mut entries,
        )?;

        let next_page_token = if entries.len() > page_size {
            let token = entries
                .get(page_size.saturating_sub(1))
                .map(|entry| entry.status.path.clone());
            entries.truncate(page_size);
            token
        } else {
            None
        };

        Ok(CvMetadataSnapshotPage {
            entries,
            next_page_token,
            epoch,
        })
    }

    pub fn cv_metadata_delta_page(
        &self,
        from_epoch: u64,
        target_epoch: Option<u64>,
        page_token: Option<String>,
        page_size: usize,
    ) -> FsResult<CvMetadataDeltaPage> {
        if page_size == 0 {
            return err_box!("cv metadata delta page_size must be greater than 0");
        }

        let fs_dir = self.fs_dir.read();
        let current_epoch = fs_dir.op_id.get();
        let to_epoch = target_epoch.unwrap_or(current_epoch);
        if to_epoch > current_epoch {
            return err_box!(
                "cv metadata delta target_epoch {} is newer than current epoch {}",
                to_epoch,
                current_epoch
            );
        }
        if from_epoch > to_epoch {
            return err_box!(
                "cv metadata delta from_epoch {} is newer than target epoch {}",
                from_epoch,
                to_epoch
            );
        }
        if target_epoch.is_some() && to_epoch < current_epoch {
            return Ok(CvMetadataDeltaPage {
                entries: Vec::new(),
                next_page_token: None,
                from_epoch,
                to_epoch,
                full_snapshot_required: true,
            });
        }
        if from_epoch == to_epoch {
            return Ok(CvMetadataDeltaPage {
                entries: Vec::new(),
                next_page_token: None,
                from_epoch,
                to_epoch,
                full_snapshot_required: false,
            });
        }

        let Some(changes) = fs_dir
            .journal_writer
            .cv_metadata_changes_since(from_epoch, to_epoch)
        else {
            return Ok(CvMetadataDeltaPage {
                entries: Vec::new(),
                next_page_token: None,
                from_epoch,
                to_epoch,
                full_snapshot_required: true,
            });
        };

        let mut changed_paths = BTreeMap::new();
        for change in changes {
            changed_paths
                .entry(change.path)
                .and_modify(|include_subtree| *include_subtree |= change.include_subtree)
                .or_insert(change.include_subtree);
        }

        let mut delta_entries = BTreeMap::new();
        for (path, include_subtree) in changed_paths {
            if include_subtree {
                self.collect_cv_metadata_delta_subtree(&fs_dir, &path, &mut delta_entries)?;
            } else {
                let entry = self.cv_metadata_entry_for_path(&fs_dir, &path)?;
                delta_entries.insert(path, entry);
            }
        }

        let start_after = page_token.filter(|token| !token.is_empty());
        let mut page_entries = Vec::with_capacity(page_size.saturating_add(1));
        for (path, entry) in delta_entries {
            if start_after
                .as_deref()
                .map(|token| path.as_str() <= token)
                .unwrap_or(false)
            {
                continue;
            }
            page_entries.push(CvMetadataDeltaEntry { path, entry });
            if page_entries.len() > page_size {
                break;
            }
        }

        let next_page_token = if page_entries.len() > page_size {
            let token = page_entries
                .get(page_size.saturating_sub(1))
                .map(|entry| entry.path.clone());
            page_entries.truncate(page_size);
            token
        } else {
            None
        };

        Ok(CvMetadataDeltaPage {
            entries: page_entries,
            next_page_token,
            from_epoch,
            to_epoch,
            full_snapshot_required: false,
        })
    }

    fn collect_cv_metadata_delta_subtree(
        &self,
        fs_dir: &FsDir,
        path: &str,
        entries: &mut BTreeMap<String, Option<CvMetadataSnapshotEntry>>,
    ) -> FsResult<()> {
        let Some(entry) = self.cv_metadata_entry_for_path(fs_dir, path)? else {
            entries.insert(path.to_string(), None);
            return Ok(());
        };

        let is_dir = entry.status.is_dir;
        entries.insert(path.to_string(), Some(entry));
        if !is_dir {
            return Ok(());
        }

        let inp = Self::resolve_path(fs_dir, path)?;
        let Some(inode) = inp.get_last_inode() else {
            return Ok(());
        };
        let resolved = self.resolve_snapshot_inode(fs_dir, &inode)?;
        if let InodeView::Dir(dir) = resolved {
            for child in dir.children_iter() {
                let child_path = child_snapshot_path(path, child.name());
                self.collect_cv_metadata_delta_subtree(fs_dir, &child_path, entries)?;
            }
        }
        Ok(())
    }

    fn cv_metadata_entry_for_path(
        &self,
        fs_dir: &FsDir,
        path: &str,
    ) -> FsResult<Option<CvMetadataSnapshotEntry>> {
        let inp = match Self::resolve_path(fs_dir, path) {
            Ok(inp) => inp,
            Err(_) => return Ok(None),
        };
        let Some(inode) = inp.get_last_inode() else {
            return Ok(None);
        };
        let resolved = self.resolve_snapshot_inode(fs_dir, &inode)?;
        let status = resolved.to_file_status(path)?;
        let blocks = if let Ok(file) = resolved.as_file_ref() {
            Some(FileBlocks::new(
                status.clone(),
                self.get_block_locs(path, fs_dir, file)?,
            ))
        } else {
            None
        };
        Ok(Some(CvMetadataSnapshotEntry { status, blocks }))
    }

    fn collect_cv_metadata_snapshot_page(
        &self,
        fs_dir: &FsDir,
        inode: &InodeView,
        path: &str,
        start_after: Option<&str>,
        limit: usize,
        entries: &mut Vec<CvMetadataSnapshotEntry>,
    ) -> FsResult<()> {
        if entries.len() >= limit {
            return Ok(());
        }
        if let Some(token) = start_after {
            if path != "/" && token > path && !snapshot_token_in_subtree(token, path) {
                return Ok(());
            }
        }

        let resolved = self.resolve_snapshot_inode(fs_dir, inode)?;
        if start_after.map(|token| path > token).unwrap_or(true) {
            let status = resolved.to_file_status(path)?;
            let blocks = if let Ok(file) = resolved.as_file_ref() {
                Some(FileBlocks::new(
                    status.clone(),
                    self.get_block_locs(path, fs_dir, file)?,
                ))
            } else {
                None
            };
            entries.push(CvMetadataSnapshotEntry { status, blocks });
            if entries.len() >= limit {
                return Ok(());
            }
        }

        if let InodeView::Dir(dir) = resolved {
            for child in dir.children_iter() {
                let child_path = child_snapshot_path(path, child.name());
                self.collect_cv_metadata_snapshot_page(
                    fs_dir,
                    &child,
                    &child_path,
                    start_after,
                    limit,
                    entries,
                )?;
                if entries.len() >= limit {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn resolve_snapshot_inode(&self, fs_dir: &FsDir, inode: &InodeView) -> FsResult<InodeView> {
        if let InodeView::FileEntry(entry) = inode {
            return fs_dir
                .store
                .get_inode(entry.id, Some(&entry.name))?
                .ok_or_else(|| FsError::file_not_found(entry.name.clone()));
        }
        Ok(inode.clone())
    }

    fn get_block_locs_from_store_locked(
        &self,
        path: &str,
        store: &RocksInodeStore,
        file: &InodeFile,
    ) -> FsResult<Vec<LocatedBlock>> {
        let wm = self.worker_manager.read();
        let mut block_locs = Vec::with_capacity(file.blocks.len());

        for (index, meta) in file.blocks.iter().enumerate() {
            if index + 1 < file.blocks.len() && meta.len() != file.block_size as i64 {
                return err_box!(
                    "block status abnormal, block id {}, block len {}, expected block size {}",
                    meta.id,
                    meta.len(),
                    file.block_size
                );
            }

            let extend_block = ExtendedBlock {
                id: meta.id,
                len: meta.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: meta.alloc_opts.clone(),
            };

            let locs = match &meta.locs {
                Some(locs) => locs.clone(),
                None => store.get_locations(meta.id)?,
            };
            if locs.is_empty() {
                return err_box!("File {}, block {} Lost (no worker can read)", path, meta.id);
            }
            let lb = wm.create_locate_block(path, extend_block, &locs)?;
            block_locs.push(lb);
        }

        Ok(block_locs)
    }

    pub fn get_block_locations_by_id(&self, block_id: i64) -> FsResult<Vec<BlockLocation>> {
        self.ensure_metadata_current()?;
        let _block_locks = self.block_location_locks.read_blocks(&[block_id]);
        Ok(self.rocks_store()?.get_locations(block_id)?)
    }

    pub fn filesystem_info(&self) -> FsResult<FilesystemInfo> {
        self.ensure_metadata_current()?;
        let metrics = Master::get_metrics()?;
        let mut info = FilesystemInfo {
            inode_dir_num: metrics.inode_dir_num.get(),
            inode_file_num: metrics.inode_file_num.get(),
            ..Default::default()
        };

        let wm = self.worker_manager.read();

        // Requests can only reach active master
        info.active_master = wm.conf.master_addr().to_string();
        for peer in &wm.conf.journal.journal_addrs {
            info.journal_nodes.push(peer.to_string())
        }

        for (_, worker) in wm.worker_map.workers() {
            info.capacity += worker.capacity;
            info.available += worker.available;
            info.fs_used += worker.fs_used;
            info.non_fs_used += worker.non_fs_used;
            info.reserved_bytes += worker.reserved_bytes;
            info.block_num += worker.block_num;

            match worker.status {
                WorkerStatus::Live => {
                    info.live_workers.push(worker.clone());
                    // Only Live workers are eligible for new allocations, so the
                    // allocatable view mirrors the allocation policy. Failed
                    // storage dirs are already excluded from worker.capacity.
                    info.allocatable_capacity += worker.capacity;
                    info.allocatable_available += worker.available;
                }
                WorkerStatus::Blacklist => info.blacklist_workers.push(worker.clone()),
                WorkerStatus::Decommission => info.decommission_workers.push(worker.clone()),
                _ => (),
            }
        }

        for (_, worker) in wm.worker_map.lost_workers() {
            info.lost_workers.push(worker.clone());
        }

        Ok(info)
    }

    pub fn fs_dir(&self) -> ArcRwLock<FsDir> {
        self.fs_dir.clone()
    }

    // Add a test worker and unit tests will use it.
    pub fn add_test_worker(&self, worker: WorkerInfo) {
        let mut wm = self.worker_manager.write();
        wm.add_test_worker(worker);
    }

    pub fn sum_hash(&self) -> CommonResult<u128> {
        let fs_dir = self.fs_dir.read();
        fs_dir.sum_hash()
    }

    pub fn last_inode_id(&self) -> i64 {
        let fs_dir = self.fs_dir.read();
        fs_dir.last_inode_id()
    }

    pub fn get_file_counts(&self) -> (i64, i64) {
        self.fs_stats.counts()
    }

    pub fn get_file_counts_current(&self) -> FsResult<(i64, i64)> {
        self.ensure_metadata_current()?;
        Ok(self.get_file_counts())
    }

    // Create a directory number based on rocksdb data for testing.
    pub fn create_tree(&self) -> CommonResult<InodeView> {
        let fs_dir = self.fs_dir.read();
        fs_dir.create_tree()
    }

    // Restore in-memory tree from RocksDB (for testing without Raft).
    // In production, Raft automatically restores via apply_snapshot().
    pub fn restore_from_rocksdb(&self) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        fs_dir.restore_from_rocksdb()
    }

    fn block_inode_state(&self, block_id: i64) -> FsResult<BlockInodeState> {
        let store = self.rocks_store()?;
        let snapshot = store.snapshot();
        let file_id = InodeId::get_id(block_id);
        match snapshot.get_inode(file_id)? {
            None => Ok(BlockInodeState::Missing),
            Some(inode) if inode.is_file() => Ok(BlockInodeState::File),
            Some(_) => Ok(BlockInodeState::NotFile),
        }
    }

    fn block_exists_in_snapshot(
        snapshot: &RocksInodeStoreSnapshot<'_>,
        file_blocks: &mut HashMap<i64, HashSet<i64>>,
        id: i64,
    ) -> FsResult<bool> {
        let file_id = InodeId::get_id(id);
        if let std::collections::hash_map::Entry::Vacant(entry) = file_blocks.entry(file_id) {
            let blocks = match snapshot.get_inode(file_id)? {
                Some(InodeView::File(file)) => file.blocks.iter().map(|block| block.id).collect(),
                _ => HashSet::new(),
            };
            entry.insert(blocks);
        }

        Ok(file_blocks
            .get(&file_id)
            .is_some_and(|blocks| blocks.contains(&id)))
    }

    fn update_preallocated_block_location(
        snapshot: &RocksInodeStoreSnapshot<'_>,
        file_inodes: &mut HashMap<i64, InodeView>,
        dirty_inodes: &mut HashSet<i64>,
        add: bool,
        block_id: i64,
        location: &BlockLocation,
    ) -> FsResult<()> {
        let file_id = InodeId::get_id(block_id);
        if let Entry::Vacant(entry) = file_inodes.entry(file_id) {
            match snapshot.get_inode(file_id)? {
                Some(inode @ InodeView::File(_)) => {
                    entry.insert(inode);
                }
                _ => return Ok(()),
            }
        }

        let Some(inode) = file_inodes.get_mut(&file_id) else {
            return Ok(());
        };
        let file = inode.as_file_mut()?;
        let Some(meta) = file.search_block_mut(block_id) else {
            return Ok(());
        };
        let Some(locs) = meta.locs.as_mut() else {
            return Ok(());
        };

        if add {
            match locs
                .iter_mut()
                .find(|loc| loc.worker_id == location.worker_id)
            {
                Some(existing) => existing.storage_type = location.storage_type,
                None => locs.push(location.clone()),
            }
        } else {
            locs.retain(|loc| loc.worker_id != location.worker_id);
        }
        dirty_inodes.insert(file_id);
        Ok(())
    }

    fn collect_full_block_report(&self, list: &BlockReportList) -> FsResult<Option<u64>> {
        let now = LocalTime::mills();
        let mut reports = self.full_block_reports.lock();
        reports.retain(|_, report| {
            now.saturating_sub(report.update_time_ms) <= FULL_BLOCK_REPORT_TTL_MS
        });

        if !list.full_report {
            return Ok(None);
        }

        if list.total_len > MAX_FULL_BLOCK_REPORT_BLOCKS {
            return err_box!(
                "full block report for worker {} rejected because total_len {} exceeds {}",
                list.worker_id,
                list.total_len,
                MAX_FULL_BLOCK_REPORT_BLOCKS
            );
        }

        if list.full_report_start {
            let generation = self.next_full_block_report_generation();
            Self::begin_full_block_report_state(&mut reports, list.worker_id, now, generation);
            drop(reports);
            self.invalidate_full_block_reconcile(list.worker_id);
            return Ok(None);
        }

        if list.blocks.len() as u64 > list.total_len {
            return err_box!(
                "full block report for worker {} has chunk size {} greater than total_len {}",
                list.worker_id,
                list.blocks.len(),
                list.total_len
            );
        }

        let report = match reports.entry(list.worker_id) {
            Entry::Vacant(entry) => entry.insert(FullBlockReportState {
                generation: self.next_full_block_report_generation(),
                total_len: Some(list.total_len),
                update_time_ms: now,
                reported_blocks: HashSet::new(),
                protected_blocks: HashSet::new(),
                can_reconcile: true,
            }),
            Entry::Occupied(entry) => entry.into_mut(),
        };

        if let Some(total_len) = report.total_len {
            if total_len != list.total_len {
                warn!(
                    "full block report for worker {} restarted because total_len changed from {} to {}; discarding {} accumulated block ids",
                    list.worker_id,
                    total_len,
                    list.total_len,
                    report.reported_blocks.len()
                );
                *report = FullBlockReportState {
                    generation: self.next_full_block_report_generation(),
                    total_len: Some(list.total_len),
                    update_time_ms: now,
                    reported_blocks: HashSet::new(),
                    protected_blocks: HashSet::new(),
                    can_reconcile: true,
                };
            }
        } else {
            report.total_len = Some(list.total_len);
        }

        report.update_time_ms = now;

        for block in &list.blocks {
            report.reported_blocks.insert(block.id);
        }

        let total_len = report
            .total_len
            .expect("full block report total_len is initialized before reporting blocks");
        if report.reported_blocks.len() as u64 >= total_len {
            Ok(Some(report.generation))
        } else {
            Ok(None)
        }
    }

    fn next_full_block_report_generation(&self) -> u64 {
        self.full_block_report_seq.next()
    }

    fn begin_full_block_report_state(
        reports: &mut HashMap<u32, FullBlockReportState>,
        worker_id: u32,
        now: u64,
        generation: u64,
    ) {
        reports.insert(
            worker_id,
            FullBlockReportState {
                generation,
                total_len: None,
                update_time_ms: now,
                reported_blocks: HashSet::new(),
                protected_blocks: HashSet::new(),
                can_reconcile: true,
            },
        );
    }

    pub fn begin_full_block_report(&self, worker_id: u32) {
        let now = LocalTime::mills();
        let generation = self.next_full_block_report_generation();
        let mut reports = self.full_block_reports.lock();
        Self::begin_full_block_report_state(&mut reports, worker_id, now, generation);
        drop(reports);
        self.invalidate_full_block_reconcile(worker_id);
    }

    pub fn reset_full_block_report(&self, worker_id: u32) {
        self.full_block_reports.lock().remove(&worker_id);
        self.invalidate_full_block_reconcile(worker_id);
    }

    fn invalidate_full_block_reconcile(&self, worker_id: u32) {
        let mut reconciles = self.full_block_reconciles.lock();
        if let Some(state) = reconciles.get_mut(&worker_id) {
            state.pending = None;
            if !state.running {
                reconciles.remove(&worker_id);
            }
        }
    }

    fn apply_block_report_batch(
        &self,
        worker_id: u32,
        batch: Vec<(bool, i64, BlockLocation)>,
        protect_adds_during_full_report: bool,
    ) -> FsResult<Vec<i64>> {
        self.ensure_metadata_current()?;
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let block_ids = batch
            .iter()
            .map(|(_, block_id, _)| *block_id)
            .collect::<Vec<_>>();
        let result = (|| {
            let _commit_guard = self.metadata_commit_gate.enter();
            self.ensure_metadata_current()?;
            let _block_locks = self
                .block_location_locks
                .write_worker_blocks(worker_id, &block_ids);
            let store = self.rocks_store()?;
            let snapshot = store.snapshot();
            let mut file_blocks = HashMap::new();
            let mut file_inodes = HashMap::new();
            let mut dirty_inodes = HashSet::new();
            let mut filtered = Vec::with_capacity(batch.len());
            let mut missing_adds = Vec::new();
            let mut full_report_updates = Vec::new();
            for (add, block_id, location) in batch {
                if add && !Self::block_exists_in_snapshot(&snapshot, &mut file_blocks, block_id)? {
                    missing_adds.push(block_id);
                    full_report_updates.push((false, block_id));
                    filtered.push((false, block_id, location));
                    continue;
                }
                if !add || protect_adds_during_full_report {
                    full_report_updates.push((add, block_id));
                }
                Self::update_preallocated_block_location(
                    &snapshot,
                    &mut file_inodes,
                    &mut dirty_inodes,
                    add,
                    block_id,
                    &location,
                )?;
                filtered.push((add, block_id, location));
            }
            let mut write_batch = store.new_batch();
            for inode_id in &dirty_inodes {
                if let Some(inode) = file_inodes.get(inode_id) {
                    write_batch.write_inode(inode)?;
                }
            }
            for (add, block_id, location) in filtered {
                if add {
                    write_batch.add_location(block_id, &location)?;
                } else {
                    write_batch.delete_location(block_id, location.worker_id)?;
                }
            }
            write_batch.commit()?;
            for inode_id in dirty_inodes {
                self.metadata_reader.invalidate_file_status(inode_id);
            }
            self.update_full_block_report_protected_blocks(
                worker_id,
                &full_report_updates,
                protect_adds_during_full_report,
            );
            Ok(missing_adds)
        })();

        if result.is_ok() {
            self.emit_snapshot_if_requested();
        }
        result
    }

    fn update_full_block_report_protected_blocks(
        &self,
        worker_id: u32,
        updates: &[(bool, i64)],
        protect_adds: bool,
    ) {
        let mut reports = self.full_block_reports.lock();
        let Some(report) = reports.get_mut(&worker_id) else {
            return;
        };

        for (add, block_id) in updates {
            if *add {
                if protect_adds {
                    report.protected_blocks.insert(*block_id);
                }
            } else {
                report.reported_blocks.remove(block_id);
                report.protected_blocks.remove(block_id);
            }
        }
    }

    fn delete_worker_block_locations(
        &self,
        worker_id: u32,
        should_delete: impl Fn(i64) -> bool,
    ) -> FsResult<Vec<i64>> {
        self.ensure_metadata_current()?;
        let result = {
            let _commit_guard = self.metadata_commit_gate.enter();
            self.ensure_metadata_current()?;
            let _worker_lock = self.block_location_locks.write_worker(worker_id);
            self.delete_worker_block_locations_locked(worker_id, should_delete)
        };

        if result.is_ok() {
            self.emit_snapshot_if_requested();
        }
        result
    }

    fn finish_full_block_report(&self, worker_id: u32, generation: u64) -> FsResult<Vec<i64>> {
        self.ensure_metadata_current()?;
        let result = {
            let _commit_guard = self.metadata_commit_gate.enter();
            self.ensure_metadata_current()?;
            let _worker_lock = self.block_location_locks.write_worker(worker_id);
            let report = {
                let mut reports = self.full_block_reports.lock();
                match reports.get(&worker_id) {
                    Some(report) if report.generation == generation => reports.remove(&worker_id),
                    _ => None,
                }
            };
            let Some(report) = report else {
                return Ok(Vec::new());
            };

            if !report.can_reconcile {
                warn!(
                    "full block report for worker {} has no start marker; skip stale reconciliation for compatibility",
                    worker_id
                );
                return Ok(Vec::new());
            }

            self.delete_worker_block_locations_locked(worker_id, |block_id| {
                !report.reported_blocks.contains(&block_id)
                    && !report.protected_blocks.contains(&block_id)
            })
        };

        if result.is_ok() {
            self.emit_snapshot_if_requested();
        }
        result
    }

    fn delete_worker_block_locations_locked(
        &self,
        worker_id: u32,
        should_delete: impl Fn(i64) -> bool,
    ) -> FsResult<Vec<i64>> {
        let block_ids = self.rocks_store()?.get_block_ids(worker_id)?;
        let mut deleted_block_ids = Vec::new();
        let mut batch_ids = Vec::with_capacity(FULL_BLOCK_RECONCILE_BATCH_SIZE);

        for block_id in block_ids {
            if !should_delete(block_id) {
                continue;
            }

            batch_ids.push(block_id);
            deleted_block_ids.push(block_id);

            if batch_ids.len() >= FULL_BLOCK_RECONCILE_BATCH_SIZE {
                self.delete_worker_block_location_batch(worker_id, &batch_ids)?;
                batch_ids.clear();
                std::thread::yield_now();
            }
        }

        self.delete_worker_block_location_batch(worker_id, &batch_ids)?;
        Ok(deleted_block_ids)
    }

    fn delete_worker_block_location_batch(
        &self,
        worker_id: u32,
        block_ids: &[i64],
    ) -> FsResult<()> {
        if block_ids.is_empty() {
            return Ok(());
        }

        let _block_locks = self.block_location_locks.write_blocks(block_ids);
        let store = self.rocks_store()?;
        let snapshot = store.snapshot();
        let mut file_inodes = HashMap::new();
        let mut dirty_inodes = HashSet::new();
        let mut write_batch = store.new_batch();
        let location = BlockLocation::with_id(worker_id);
        for block_id in block_ids {
            Self::update_preallocated_block_location(
                &snapshot,
                &mut file_inodes,
                &mut dirty_inodes,
                false,
                *block_id,
                &location,
            )?;
            write_batch.delete_location(*block_id, worker_id)?;
        }
        for inode_id in &dirty_inodes {
            if let Some(inode) = file_inodes.get(inode_id) {
                write_batch.write_inode(inode)?;
            }
        }
        write_batch.commit()?;
        for inode_id in dirty_inodes {
            self.metadata_reader.invalidate_file_status(inode_id);
        }
        Ok(())
    }

    pub fn add_block_location(&self, block_id: i64, location: BlockLocation) -> FsResult<()> {
        self.apply_block_report_batch(location.worker_id, vec![(true, block_id, location)], true)?;
        Ok(())
    }

    pub fn commit_mount(&self, info: MountInfo) -> FsResult<()> {
        self.run_metadata_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let op_id = self.next_op_id();
            self.rocks_store()?.add_mountpoint(info.mount_id, &info)?;
            self.journal_writer.log_mount_by_id(op_id, info)?;
            Ok(())
        })
    }

    pub fn commit_unmount(&self, mount_id: u32) -> FsResult<()> {
        self.run_metadata_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let op_id = self.next_op_id();
            self.rocks_store()?.remove_mountpoint(mount_id)?;
            self.journal_writer.log_unmount_by_id(op_id, mount_id)?;
            Ok(())
        })
    }

    /// Process block reports
    pub fn block_report(
        &self,
        list: BlockReportList,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<BlockReportResult> {
        self.ensure_metadata_current()?;
        // @todo check cluster.
        let worker_id = list.worker_id;
        let is_full_report = list.full_report;
        let completed_full_report = self.collect_full_block_report(&list)?;
        if list.blocks.is_empty() && completed_full_report.is_none() {
            return Ok(BlockReportResult {
                delete_blocks: Vec::new(),
            });
        }

        //(Whether to increase, block id, block location)
        let mut checked = Vec::with_capacity(list.blocks.len());
        let mut delete_blocks = Vec::new();
        let mut missing_blocks = 0usize;
        let mut not_file_blocks = 0usize;
        for item in list.blocks {
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let defer_writing_delete =
                        item.status == BlockReportStatus::Writing && !list.full_report;
                    let state = match self.block_inode_state(item.id) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("block_report {item:?}: {e}");
                            continue;
                        }
                    };
                    match state {
                        BlockInodeState::File => checked.push((item, Some(BlockInodeState::File))),
                        BlockInodeState::Missing if defer_writing_delete => {
                            warn!(
                                "block_report deferred deletion for writing block {} on worker {} because its inode is missing",
                                item.id, list.worker_id
                            );
                        }
                        BlockInodeState::NotFile if defer_writing_delete => {
                            warn!(
                                "block_report deferred deletion for writing block {} on worker {} because its inode is not a file",
                                item.id, list.worker_id
                            );
                        }
                        BlockInodeState::Missing => {
                            missing_blocks += 1;
                            delete_blocks.push(item.id);
                            checked.push((item, Some(BlockInodeState::Missing)));
                        }
                        BlockInodeState::NotFile => {
                            not_file_blocks += 1;
                            delete_blocks.push(item.id);
                            checked.push((item, Some(BlockInodeState::NotFile)));
                        }
                    }
                }
                BlockReportStatus::Deleted => checked.push((item, None)),
            }
        }
        if missing_blocks > 0 || not_file_blocks > 0 {
            warn!(
                "block_report found {} missing-inode and {} non-file-inode blocks for worker {}; scheduling worker deletion",
                missing_blocks, not_file_blocks, list.worker_id
            );
        }

        let mut batch: Vec<(bool, i64, BlockLocation)> = vec![];
        let mut wm = self.worker_manager.write();
        for (item, exists) in checked {
            let loc = BlockLocation::new(worker_id, item.storage_type);
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let state = match exists {
                        Some(v) => v,
                        None => {
                            warn!(
                                "block_report invariant violated: missing inode state for block {}",
                                item.id
                            );
                            continue;
                        }
                    };
                    match state {
                        BlockInodeState::File => batch.push((true, item.id, loc)),
                        BlockInodeState::Missing | BlockInodeState::NotFile => {
                            batch.push((false, item.id, loc));
                            wm.remove_block(worker_id, item.id);
                        }
                    }
                }
                BlockReportStatus::Deleted => {
                    batch.push((false, item.id, loc));
                    wm.deleted_block(worker_id, item.id);
                }
            }
        }
        drop(wm);

        let missing_after_lock =
            self.apply_block_report_batch(worker_id, batch, !is_full_report)?;
        if !missing_after_lock.is_empty() {
            let mut wm = self.worker_manager.write();
            for block_id in &missing_after_lock {
                wm.remove_block(worker_id, *block_id);
            }
        }

        if let Some(generation) = completed_full_report {
            self.submit_full_block_reconcile(worker_id, generation, replication_handler)?;
        }

        delete_blocks.extend(missing_after_lock);
        Ok(BlockReportResult { delete_blocks })
    }

    fn submit_full_block_reconcile(
        &self,
        worker_id: u32,
        generation: u64,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<()> {
        let should_spawn = {
            let mut reconciles = self.full_block_reconciles.lock();
            let state = reconciles
                .entry(worker_id)
                .or_insert_with(|| FullBlockReconcileState {
                    running: false,
                    pending: None,
                });
            state.pending = Some(FullBlockReconcileJob { generation });
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };

        if !should_spawn {
            return Ok(());
        }

        let fs = self.clone();
        let res = self
            .full_block_reconcile_executor
            .fixed_spawn(worker_id as i64, move || {
                fs.run_full_block_reconcile(worker_id, replication_handler);
            });
        if let Err(e) = &res {
            self.full_block_reconciles.lock().remove(&worker_id);
            error!("submit full block report reconcile for worker {worker_id} failed: {e}");
        }
        res?;
        Ok(())
    }

    fn run_full_block_reconcile(
        &self,
        worker_id: u32,
        replication_handler: Option<MasterReplicationHandler>,
    ) {
        loop {
            let job = {
                let mut reconciles = self.full_block_reconciles.lock();
                match reconciles.get_mut(&worker_id) {
                    Some(state) => match state.pending.take() {
                        Some(v) => v,
                        None => {
                            reconciles.remove(&worker_id);
                            return;
                        }
                    },
                    None => return,
                }
            };

            match self.finish_full_block_report(worker_id, job.generation) {
                Ok(stale_block_ids) => {
                    let stale_block_count = stale_block_ids.len();
                    if stale_block_count > 0 {
                        info!(
                            "full block report reconciled {} stale block locations for worker {}",
                            stale_block_count, worker_id
                        );
                        if let Some(replication_handler) = &replication_handler {
                            if let Err(e) = replication_handler
                                .report_under_replicated_blocks(worker_id, stale_block_ids)
                            {
                                error!(
                                    "Errors on reporting under-replicated {} blocks from full block report reconciliation. err: {:?}",
                                    stale_block_count, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "full block report reconcile for worker {} failed: {}",
                        worker_id, e
                    );
                }
            };
        }
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<LostWorkerLocationCleanup> {
        let removed_block_ids = self.delete_worker_block_locations(worker_id, |_| true)?;
        let mut invalidated = CacheInvalidationResult::default();

        for chunk in removed_block_ids.chunks(Self::LOST_WORKER_INVALIDATION_CHUNK) {
            let result = {
                let mut fs_dir = self.fs_dir.write();
                fs_dir.invalidate_lost_cache_files(chunk)
            };
            match result {
                Ok(result) => invalidated.extend(result),
                Err(e) => warn!(
                    "failed to invalidate lost cache files for worker {} ({} block ids); continuing with normal replica recovery: {}",
                    worker_id,
                    chunk.len(),
                    e
                ),
            }
        }

        let replication_block_ids = removed_block_ids
            .iter()
            .copied()
            .filter(|block_id| !invalidated.invalidated_block_ids.contains(block_id))
            .collect();

        if !invalidated.delete_result.blocks.is_empty() {
            self.worker_manager
                .write()
                .remove_blocks(&Self::to_model_delete_result(&invalidated.delete_result));
        }

        Ok(LostWorkerLocationCleanup {
            removed_block_ids,
            replication_block_ids,
        })
    }

    pub fn set_attr<T: AsRef<str>>(&self, path: T, opts: SetAttrOpts) -> FsResult<FileStatus> {
        let path = path.as_ref();
        self.run_metadata_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let _inode_locks = self.lock_set_attr_path(path, opts.recursive)?;
            let fs_dir = self.fs_dir.read();
            let inp = Self::resolve_path(&fs_dir, path)?;
            fs_dir.set_attr(inp, opts)
        })
    }

    pub fn symlink<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
    ) -> FsResult<()> {
        self.symlink_with_owner_group(target, link, force, mode, None, None)
    }

    pub fn symlink_with_owner_group<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        let target = target.as_ref();
        let link = link.as_ref();
        if self
            .try_fast_symlink(
                target,
                link,
                force,
                mode,
                owner.as_deref(),
                group.as_deref(),
            )?
            .is_some()
        {
            return Ok(());
        }
        self.run_namespace_topology_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let _inode_locks = self.lock_path_for_write(link, InodeLockMode::Write, false)?;
            let fs_dir = self.fs_dir.read();
            let target = target.to_string();
            let link = Self::resolve_path(&fs_dir, link)?;
            fs_dir.symlink(target, link, force, mode, owner, group)
        })
    }

    fn try_fast_symlink(
        &self,
        target: &str,
        link: &str,
        force: bool,
        mode: u32,
        owner: Option<&str>,
        group: Option<&str>,
    ) -> FsResult<Option<()>> {
        self.try_fast_namespace_write(|| {
            let Ok(fs_dir) = self.fs_dir.try_write() else {
                return Ok(None);
            };
            let _journal_scope = self.reserve_journal_scope(1)?;
            let link = Self::resolve_exclusive_path(&fs_dir, link)?;
            fs_dir.symlink_uncontended(
                target.to_string(),
                link,
                force,
                mode,
                owner.map(str::to_string),
                group.map(str::to_string),
            )?;
            Ok(Some(()))
        })
    }

    pub fn link<T: AsRef<str>>(&self, src_path: T, dst_path: T) -> FsResult<()> {
        let src_path = src_path.as_ref();
        let dst_path = dst_path.as_ref();
        if self.try_fast_link(src_path, dst_path)?.is_some() {
            return Ok(());
        }
        self.run_namespace_topology_write(|| {
            let _inode_locks = self.lock_link_paths(src_path, dst_path)?;
            let _journal_scope =
                self.reserve_journal_scope(self.estimate_link_entries(dst_path)?)?;
            let fs_dir = self.fs_dir.read();
            let src_path = Self::resolve_path(&fs_dir, src_path)?;
            let dst_path = Self::resolve_path(&fs_dir, dst_path)?;
            fs_dir.link(src_path, dst_path)
        })
    }

    fn try_fast_link(&self, src_path: &str, dst_path: &str) -> FsResult<Option<()>> {
        self.try_fast_namespace_write(|| {
            let Ok(fs_dir) = self.fs_dir.try_write() else {
                return Ok(None);
            };
            let src_path = Self::resolve_exclusive_path(&fs_dir, src_path)?;
            let dst_path = Self::resolve_exclusive_path(&fs_dir, dst_path)?;
            let _journal_scope = self
                .reserve_journal_scope(Self::create_entries_for_resolved_path(&dst_path, true))?;
            fs_dir.link(src_path, dst_path)?;
            Ok(Some(()))
        })
    }

    pub fn resize<T: AsRef<str>>(&self, path: T, opts: FileAllocOpts) -> FsResult<FileBlocks> {
        self.run_metadata_write(|| {
            opts.validate()?;
            let path = path.as_ref();
            // This snapshot only rejects individually impossible requests; it is not a
            // reservation, so concurrent fallocates may observe the same capacity.
            // Worker-side block allocation remains the hard enforcement point.
            let available = if opts.truncate {
                i64::MAX
            } else {
                self.worker_manager.read().available_bytes()
            };
            let _journal_scope = self.reserve_journal_scope(1)?;
            let _inode_locks = self.lock_path_for_write(path, InodeLockMode::Write, false)?;
            let current_block_ids = {
                let store = self.rocks_store()?;
                let block_ids = StorePathResolver::new(&store).collect_block_ids(path, false)?;
                block_ids
            };
            let _block_locks = self.block_location_locks.write_blocks(&current_block_ids);
            let (del_res, blocks) = {
                let fs_dir = self.fs_dir.read();
                let inp = Self::resolve_path(&fs_dir, path)?;
                let inode = try_option!(inp.get_last_inode(), "File {} not exists", path);
                let file = inode.as_file_ref()?;
                Self::validate_alloc_capacity(file.len, file.replicas, &opts, available)?;
                let del_res = fs_dir.resize(&inp, opts)?;
                let blocks = self.get_file_blocks(path, &fs_dir, &inp)?;
                (del_res, blocks)
            };

            if !del_res.blocks.is_empty() {
                self.worker_manager
                    .write()
                    .remove_blocks(&Self::to_model_delete_result(&del_res));
            }

            Ok(blocks)
        })
    }

    pub fn assign_worker<T: AsRef<str>>(
        &self,
        path: T,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        self.run_metadata_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let _inode_locks = self.lock_path_for_write(path, InodeLockMode::Write, false)?;
            let fs_dir = self.fs_dir.read();
            let inp = Self::resolve_path(&fs_dir, path)?;

            let choose_workers = self.choose_worker(&inp, client_addr, exclude_workers)?;
            let has_spdk = {
                let wm = self.worker_manager.read();
                wm.workers_have_spdk(&choose_workers)
            };
            let block = fs_dir.assign_worker(inp, block.id, &choose_workers)?;

            Ok(LocatedBlock {
                block,
                locs: choose_workers,
                has_spdk,
            })
        })
    }

    pub fn get_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        self.ensure_metadata_current()?;
        let path = path.as_ref();
        let store = self.rocks_store()?;
        let resolver = StorePathResolver::new(&store);
        let resolved = resolver.resolve(path)?;
        if !resolved.is_full() {
            return err_ext!(FsError::file_not_found(path));
        }
        let inode = match resolved.target() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(path)),
        };
        let expire_ms = self.conf.lock_expire_time_ms();
        let mut meta = store.get_locks(inode.id())?;

        Ok(meta.check_conflict(&lock, expire_ms))
    }

    pub fn set_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        let path = path.as_ref();
        self.run_metadata_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let _inode_locks = self.lock_path_for_write(path, InodeLockMode::Write, false)?;
            let writer = self.journal_writer.clone();
            let op_id = self.next_op_id();
            let store = self.rocks_store()?;
            let resolver = StorePathResolver::new(&store);
            let resolved = resolver.resolve(path)?;
            if !resolved.is_full() {
                return err_ext!(FsError::file_not_found(path));
            }
            let inode = match resolved.target() {
                Some(v) => v,
                None => return err_ext!(FsError::file_not_found(path)),
            };

            let mut meta = store.get_locks(inode.id())?;
            let conflict = meta.set_lock(lock, self.conf.lock_expire_time_ms());
            let locks = meta.to_vec();
            store.set_locks(inode.id(), &locks)?;
            writer.log_set_locks_by_id(op_id, inode.id(), locks)?;
            Ok(conflict)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallocate_rejects_growth_larger_than_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        let err = MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 359).unwrap_err();
        assert!(matches!(err, FsError::DiskOutOfSpace(_)));
    }

    #[test]
    fn fallocate_accepts_exact_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 360).is_ok());
    }

    #[test]
    fn truncate_growth_does_not_require_physical_capacity() {
        let opts = FileAllocOpts::with_truncate(200);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 0).is_ok());
    }
}

/// Bridge between the namespace-internal delete summary and the wire-level
/// model type, accepting both owned and borrowed values.
trait AsModelDelete {
    fn as_model_delete(&self) -> DeleteResult;
}

impl AsModelDelete for FsDeleteResult {
    fn as_model_delete(&self) -> DeleteResult {
        DeleteResult {
            inodes: self.inodes,
            bytes: 0,
            blocks: self.blocks.clone(),
        }
    }
}

impl AsModelDelete for &FsDeleteResult {
    fn as_model_delete(&self) -> DeleteResult {
        (*self).as_model_delete()
    }
}
