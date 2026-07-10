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
use crate::master::journal::{JournalPermitScope, JournalSystem, JournalWriter, SnapshotManifest};
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::inode::{
    Inode, InodeFile, InodePath, InodePtr, InodeView, PATH_SEPARATOR, ROOT_INODE_ID,
};
use crate::master::meta::FsDir;

use crate::master::fs::DeleteResult;
use crate::master::meta::parse_glob_pattern;
use crate::master::meta::store::{
    RocksInodeStoreSnapshot, RocksStoreHandle, RocksStoreReadGuard, StorePathResolver,
    StoreResolvedPath, StoreSubtreeSummary,
};
use crate::master::meta::{
    BlockLocationLockManager, FileSystemStats, InodeId, InodeLockManager, InodeLockMode,
    InodeLockRequest, InodeLockSet, NamespaceCommitGate,
};
use crate::master::{Master, MasterMonitor, SyncFsDir, SyncWorkerManager};
use curvine_common::conf::{ClusterConf, MasterConf};
use curvine_common::error::FsError;
use curvine_common::state::*;
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::{LocalTime, Utils};
use orpc::sync::{ArcRwLock, AtomicCounter};
use orpc::{err_box, err_ext, try_option, CommonResult};
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone)]
pub struct MasterFilesystem {
    pub fs_dir: SyncFsDir,
    pub worker_manager: SyncWorkerManager,
    pub master_monitor: MasterMonitor,
    pub conf: Arc<MasterConf>,
    journal_writer: Arc<JournalWriter>,
    store_handle: Arc<RocksStoreHandle>,
    ttl_bucket_list: Arc<TtlBucketList>,
    fs_stats: Arc<FileSystemStats>,
    op_id: Arc<AtomicCounter>,
    full_block_reports: Arc<Mutex<HashMap<u32, FullBlockReportState>>>,
    full_block_report_seq: Arc<AtomicCounter>,
    block_location_locks: Arc<BlockLocationLockManager>,
    inode_locks: Arc<InodeLockManager>,
    namespace_commit_gate: Arc<NamespaceCommitGate>,
    metadata_read_bypass_token: Arc<String>,
}

struct FullBlockReportState {
    generation: u64,
    total_len: Option<u64>,
    update_time_ms: u64,
    reported_blocks: HashSet<i64>,
    protected_blocks: HashSet<i64>,
    can_reconcile: bool,
}

const FULL_BLOCK_REPORT_TTL_MS: u64 = 60 * 60 * 1000;
const MAX_FULL_BLOCK_REPORT_BLOCKS: u64 = 100_000_000;
const FULL_BLOCK_RECONCILE_BATCH_SIZE: usize = 4096;
const NAMESPACE_LOCK_RETRY_LIMIT: usize = 128;
const CREATE_PARENT_LOCK_CACHE_LIMIT: usize = 256;

thread_local! {
    static CREATE_PARENT_LOCK_CACHE: RefCell<HashMap<String, Vec<InodeLockRequest>>> =
        RefCell::new(HashMap::new());
}

impl MasterFilesystem {
    pub fn new(
        conf: &ClusterConf,
        fs_dir: SyncFsDir,
        worker_manager: SyncWorkerManager,
        master_monitor: MasterMonitor,
    ) -> Self {
        let (journal_writer, store_handle, ttl_bucket_list, fs_stats, op_id) = {
            let fs_dir_guard = fs_dir.read();
            (
                fs_dir_guard.journal_writer.clone(),
                fs_dir_guard.store_handle.clone(),
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
            ttl_bucket_list,
            fs_stats,
            op_id,
            full_block_reports: Default::default(),
            full_block_report_seq: Arc::new(AtomicCounter::new(0)),
            block_location_locks: Default::default(),
            inode_locks: Default::default(),
            namespace_commit_gate: Default::default(),
            metadata_read_bypass_token: Arc::new(Utils::uuid()),
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

    pub(crate) fn metadata_read_bypass_token(&self) -> String {
        self.metadata_read_bypass_token.as_ref().clone()
    }

    pub(crate) fn can_bypass_metadata_read_barrier(&self, token: Option<&str>) -> bool {
        token
            .filter(|value| !value.is_empty())
            .is_some_and(|value| value == self.metadata_read_bypass_token.as_str())
    }

    pub(crate) fn namespace_commit_gate(&self) -> Arc<NamespaceCommitGate> {
        self.namespace_commit_gate.clone()
    }

    fn run_namespace_write<R>(&self, f: impl FnOnce() -> FsResult<R>) -> FsResult<R> {
        self.ensure_metadata_current()?;
        let result = {
            let _commit_guard = self.namespace_commit_gate.enter();
            f()
        };

        if result.is_ok() {
            self.emit_snapshot_if_requested();
        }
        result
    }

    fn emit_snapshot_if_requested(&self) {
        let writer = self.journal_writer.clone();
        if !writer.try_begin_snapshot() {
            return;
        }

        let snapshot_result = (|| {
            let _barrier = self.namespace_commit_gate.close_and_wait();
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
        self.run_namespace_write(|| {
            for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
                let (inode_locks, parent_cache_key, used_cached_locks) =
                    self.lock_create_path_for_write(path, InodeLockMode::Write, true)?;
                let fs_dir = self.fs_dir.read();
                let inp = Self::resolve_path(&fs_dir, path)?;
                if !Self::create_inode_locks_cover(&inode_locks, &inp, InodeLockMode::Write, true) {
                    if used_cached_locks {
                        if let Some(parent_path) = &parent_cache_key {
                            Self::remove_create_parent_lock_cache(parent_path);
                        }
                    }
                    drop(fs_dir);
                    drop(inode_locks);
                    Self::retry_namespace_lock(path, attempt)?;
                    continue;
                }

                if !used_cached_locks && Self::path_has_existing_parent_only(&inp) {
                    if let Some(parent_path) = &parent_cache_key {
                        let requests = Self::create_inode_lock_requests_from_memory(
                            &inp,
                            InodeLockMode::Write,
                            true,
                        );
                        Self::put_create_parent_lock_cache(parent_path, requests);
                    }
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

                let inp = fs_dir.mkdir(inp, opts)?;
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
        })
    }

    pub fn mkdir<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let opts = MkdirOpts::with_create(create_parent);
        self.mkdir_with_opts(path, opts)
    }

    pub fn delete<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<bool> {
        let path = path.as_ref();
        self.run_namespace_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let (_inode_locks, block_ids) = self.lock_delete_path(path, recursive)?;
            let _block_locks = self.block_location_locks.write_blocks(&block_ids);
            let delete_result = {
                let fs_dir = self.fs_dir.read();
                let inp = Self::resolve_path(&fs_dir, path)?;
                fs_dir.delete(&inp, recursive)?
            };

            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(&delete_result);

            Ok(true)
        })
    }

    pub fn free<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<FreeResult> {
        let path = path.as_ref();
        self.run_namespace_write(|| {
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
                blocks: std::mem::take(&mut free_res.blocks),
            });

            Ok(free_res)
        })
    }

    pub fn rename<T: AsRef<str>>(&self, src: T, dst: T, flags: RenameFlags) -> FsResult<bool> {
        let src = src.as_ref();
        let dst = dst.as_ref();
        self.run_namespace_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;

            if src == dst {
                return Ok(false);
            }

            let (_inode_locks, replaced_block_ids) = self.lock_rename_paths(src, dst, flags)?;
            let _block_locks = self.block_location_locks.write_blocks(&replaced_block_ids);
            let delete_result = {
                let fs_dir = self.fs_dir.read();
                let src_inp = Self::resolve_path(&fs_dir, src)?;
                let dst_inp = Self::resolve_path(&fs_dir, dst)?;

                if src_inp.is_root() {
                    return err_box!("Cannot rename root path");
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

                fs_dir.rename(&src_inp, &dst_inp, flags)?
            };
            if let Some(del_res) = delete_result {
                let mut worker_manager = self.worker_manager.write();
                worker_manager.remove_blocks(&del_res);
            }

            Ok(true)
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
    ) -> FsResult<DeleteResult> {
        fs_dir.overwrite_file(inp, opts)
    }

    pub fn create_with_opts<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        self.run_namespace_write(|| {
            if !flags.create() {
                return err_box!("create_with_opts requires O_CREAT flag");
            }
            let path = path.as_ref();
            // Check the path length
            self.check_path_length(path)?;

            if opts.replicas < self.conf.min_replication
                || opts.replicas >= self.conf.max_replication
            {
                return err_box!(
                    "The replica number {} needs to be between {} and {}",
                    opts.replicas,
                    self.conf.min_replication,
                    self.conf.max_replication
                );
            }

            if opts.block_size < self.conf.min_block_size
                || opts.block_size >= self.conf.max_block_size
            {
                return err_box!(
                    "Block size needs to be between {} and {}",
                    self.conf.min_block_size,
                    self.conf.max_block_size
                );
            }

            for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
                let (inode_locks, parent_cache_key, used_cached_locks) =
                    self.lock_create_path_for_write(path, InodeLockMode::Write, true)?;
                let fs_dir = self.fs_dir.read();
                let inp = Self::resolve_path(&fs_dir, path)?;
                if !Self::create_inode_locks_cover(&inode_locks, &inp, InodeLockMode::Write, true) {
                    if used_cached_locks {
                        if let Some(parent_path) = &parent_cache_key {
                            Self::remove_create_parent_lock_cache(parent_path);
                        }
                    }
                    drop(fs_dir);
                    drop(inode_locks);
                    Self::retry_namespace_lock(path, attempt)?;
                    continue;
                }

                if !used_cached_locks && Self::path_has_existing_parent_only(&inp) {
                    if let Some(parent_path) = &parent_cache_key {
                        let requests = Self::create_inode_lock_requests_from_memory(
                            &inp,
                            InodeLockMode::Write,
                            true,
                        );
                        Self::put_create_parent_lock_cache(parent_path, requests);
                    }
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
                        let _block_locks =
                            self.block_location_locks.write_blocks(&overwrite_block_ids);
                        clean_result = Some(self.truncate(&fs_dir, &inp, opts)?);
                    } else {
                        return err_ext!(FsError::file_exists(inp.path()));
                    }
                    inp
                } else {
                    clean_result = None;
                    fs_dir.create_file(inp, opts)?
                };

                let status = fs_dir.file_status(&inp)?;
                if let Some(clean_result) = clean_result {
                    if !clean_result.blocks.is_empty() {
                        self.worker_manager.write().remove_blocks(&clean_result);
                    }
                }

                return Ok(status);
            }

            err_box!(
                "namespace path {} changed while acquiring create locks after {} retries",
                path,
                NAMESPACE_LOCK_RETRY_LIMIT
            )
        })
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

        let existing = self.run_namespace_write(|| {
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
                    let status = fs_dir.file_status(&inp)?;
                    (Some(FileBlocks::new(status, vec![])), Some(clean_result))
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

            if let Some(clean_result) = clean_result {
                if !clean_result.blocks.is_empty() {
                    self.worker_manager.write().remove_blocks(&clean_result);
                }
            }

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
        let store = self.rocks_store()?;
        let resolver = StorePathResolver::new(&store);
        let resolved = resolver.resolve(path)?;
        if !resolved.is_full() {
            return err_ext!(FsError::file_not_found(path));
        }

        let inode = try_option!(resolved.target(), "File {} not exists", path);
        Ok(inode.to_file_status(path)?)
    }

    pub fn exists<T: AsRef<str>>(&self, path: T) -> FsResult<bool> {
        self.ensure_metadata_current()?;
        let path = path.as_ref();
        let store = self.rocks_store()?;
        let resolver = StorePathResolver::new(&store);
        Ok(resolver.resolve(path)?.is_full())
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
        let store = self.rocks_store()?;
        let resolver = StorePathResolver::new(&store);
        if is_glob_pattern {
            resolver.list_status_glob(path)
        } else {
            resolver.list_status(path)
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
            let store = self.rocks_store()?;
            let resolver = StorePathResolver::new(&store);
            resolver.list_options(path, &opts)
        }
    }

    fn resolve_path(fs_dir: &FsDir, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store)
    }

    fn lock_path_for_write(
        &self,
        path: &str,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> CommonResult<InodeLockSet<'_>> {
        for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let requests = {
                let store = self.rocks_store()?;
                let resolved = StorePathResolver::new(&store).resolve(path)?;
                Self::store_inode_lock_requests(&resolved, target_mode, parent_write)
            };
            let locks = self.inode_locks.lock_many(&requests);
            let current_requests = {
                let store = self.rocks_store()?;
                let current = StorePathResolver::new(&store).resolve(path)?;
                Self::store_inode_lock_requests(&current, target_mode, parent_write)
            };
            if locks.covers_requests(&current_requests) {
                return Ok(locks);
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

    fn lock_path_for_write_unchecked(
        &self,
        path: &str,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> CommonResult<InodeLockSet<'_>> {
        let store = self.rocks_store()?;
        let resolved = StorePathResolver::new(&store).resolve(path)?;
        let requests =
            Self::create_inode_lock_requests_from_store(&resolved, target_mode, parent_write);
        Ok(self.inode_locks.lock_many(&requests))
    }

    fn lock_create_path_for_write(
        &self,
        path: &str,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> CommonResult<(InodeLockSet<'_>, Option<String>, bool)> {
        let parent_cache_key = if parent_write {
            Self::parent_path_for_create_cache(path)
        } else {
            None
        };

        if let Some(parent_path) = &parent_cache_key {
            if let Some(requests) = Self::get_create_parent_lock_cache(parent_path) {
                return Ok((
                    self.inode_locks.lock_many(&requests),
                    parent_cache_key,
                    true,
                ));
            }
        }

        Ok((
            self.lock_path_for_write_unchecked(path, target_mode, parent_write)?,
            parent_cache_key,
            false,
        ))
    }

    fn parent_path_for_create_cache(path: &str) -> Option<String> {
        let path = path.trim_end_matches(PATH_SEPARATOR);
        if path.is_empty() || path == PATH_SEPARATOR {
            return None;
        }

        let split = path.rfind(PATH_SEPARATOR)?;
        if split == 0 {
            Some(PATH_SEPARATOR.to_string())
        } else {
            Some(path[..split].to_string())
        }
    }

    fn get_create_parent_lock_cache(parent_path: &str) -> Option<Vec<InodeLockRequest>> {
        CREATE_PARENT_LOCK_CACHE.with(|cache| cache.borrow().get(parent_path).cloned())
    }

    fn put_create_parent_lock_cache(parent_path: &str, requests: Vec<InodeLockRequest>) {
        CREATE_PARENT_LOCK_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if cache.len() >= CREATE_PARENT_LOCK_CACHE_LIMIT && !cache.contains_key(parent_path) {
                cache.clear();
            }
            cache.insert(parent_path.to_string(), requests);
        });
    }

    fn remove_create_parent_lock_cache(parent_path: &str) {
        CREATE_PARENT_LOCK_CACHE.with(|cache| {
            cache.borrow_mut().remove(parent_path);
        });
    }

    fn create_inode_lock_requests_from_store(
        path: &StoreResolvedPath,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> Vec<InodeLockRequest> {
        Self::store_inode_lock_requests(path, target_mode, parent_write)
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
        let mut requests = Self::store_inode_lock_requests(resolved, InodeLockMode::Write, true);
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

    fn lock_rename_paths(
        &self,
        src: &str,
        dst: &str,
        flags: RenameFlags,
    ) -> FsResult<(InodeLockSet<'_>, Vec<i64>)> {
        for attempt in 0..NAMESPACE_LOCK_RETRY_LIMIT {
            let requests = {
                let store = self.rocks_store()?;
                let resolver = StorePathResolver::new(&store);
                let src_resolved = resolver.resolve(src)?;
                let dst_resolved = resolver.resolve(dst)?;
                Self::validate_rename_target(&resolver, &src_resolved, &dst_resolved, flags)?;
                let replaced_subtree = resolver.collect_resolved_subtree(&dst_resolved, true)?;
                Self::rename_lock_requests(&src_resolved, &dst_resolved, &replaced_subtree)
            };
            let locks = self.inode_locks.lock_many(&requests);
            let (current_requests, replaced_block_ids) = {
                let store = self.rocks_store()?;
                let resolver = StorePathResolver::new(&store);
                let src_resolved = resolver.resolve(src)?;
                let dst_resolved = resolver.resolve(dst)?;
                Self::validate_rename_target(&resolver, &src_resolved, &dst_resolved, flags)?;
                let replaced_subtree = resolver.collect_resolved_subtree(&dst_resolved, true)?;
                (
                    Self::rename_lock_requests(&src_resolved, &dst_resolved, &replaced_subtree),
                    replaced_subtree.block_ids,
                )
            };
            if locks.covers_requests(&current_requests) {
                return Ok((locks, replaced_block_ids));
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

    fn validate_rename_target(
        resolver: &StorePathResolver<'_>,
        src_resolved: &StoreResolvedPath,
        dst_resolved: &StoreResolvedPath,
        flags: RenameFlags,
    ) -> FsResult<()> {
        if flags.exchange_mode() {
            return err_ext!(FsError::unsupported(
                "rename exchange mode is not supported"
            ));
        }

        let Some(src_inode) = src_resolved.target() else {
            return Ok(());
        };
        let Some(dst_inode) = dst_resolved.target() else {
            return Ok(());
        };
        if !src_resolved.is_full() || !dst_resolved.is_full() {
            return Ok(());
        }

        if flags.no_replace() {
            return Err(FsError::file_exists(dst_resolved.path()));
        }

        let src_is_file = src_inode.is_file();
        let dst_is_file = dst_inode.is_file();
        if src_is_file && !dst_is_file {
            return Err(FsError::is_a_directory(dst_resolved.path()));
        }
        if !src_is_file && dst_is_file {
            return Err(FsError::not_a_directory(dst_resolved.path()));
        }
        if !src_is_file && !dst_is_file && resolver.dir_has_children(dst_inode.id())? {
            return Err(FsError::dir_not_empty(dst_resolved.path()));
        }

        Ok(())
    }

    fn rename_lock_requests(
        src_resolved: &StoreResolvedPath,
        dst_resolved: &StoreResolvedPath,
        replaced_subtree: &StoreSubtreeSummary,
    ) -> Vec<InodeLockRequest> {
        let mut requests =
            Self::store_inode_lock_requests(src_resolved, InodeLockMode::Write, true);
        requests.extend(Self::store_inode_lock_requests(
            dst_resolved,
            InodeLockMode::Write,
            true,
        ));
        requests.extend(
            replaced_subtree
                .inodes
                .iter()
                .map(|inode| InodeLockRequest::write(inode.depth, inode.inode_id)),
        );
        requests.sort_by_key(|request| (request.depth, request.inode_id));
        requests
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

    fn create_inode_lock_requests_from_memory(
        path: &InodePath,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> Vec<InodeLockRequest> {
        Self::inode_path_lock_requests(path, target_mode, parent_write)
    }

    fn create_inode_locks_cover(
        locks: &InodeLockSet,
        path: &InodePath,
        target_mode: InodeLockMode,
        parent_write: bool,
    ) -> bool {
        if let Some(request) =
            Self::single_create_inode_lock_request(path, target_mode, parent_write)
        {
            return locks.covers_request(request);
        }

        let requests =
            Self::create_inode_lock_requests_from_memory(path, target_mode, parent_write);
        locks.covers_requests(&requests)
    }

    fn single_create_inode_lock_request(
        path: &InodePath,
        target_mode: InodeLockMode,
        _parent_write: bool,
    ) -> Option<InodeLockRequest> {
        if path.inodes.len() == 1 && path.is_full() {
            let inode = path.inodes.first()?;
            return Some(InodeLockRequest {
                depth: 0,
                inode_id: inode.id(),
                mode: target_mode,
            });
        }

        None
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
        self.run_namespace_write(|| {
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
            let block =
                fs_dir.acquire_new_block(path, inode, commit_blocks, &choose_workers, file_len)?;
            self.protect_committed_block_locations(&commit_location_pairs);
            let located = LocatedBlock {
                block,
                locs: choose_workers,
            };

            Ok(located)
        })
    }

    pub fn complete_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        only_flush: bool,
    ) -> FsResult<Option<FileBlocks>> {
        let path = path.as_ref();
        self.run_namespace_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let _inode_locks = self.lock_path_and_inode_for_write(path, inode_id)?;
            let commit_block_ids = Self::commit_block_ids(&commit_blocks);
            let commit_worker_ids = Self::commit_block_worker_ids(&commit_blocks);
            let commit_location_pairs = Self::commit_block_location_pairs(&commit_blocks);
            let fs_dir = self.fs_dir.read();
            let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
            let mut block_ids = commit_block_ids.clone();
            if only_flush {
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
                only_flush,
            )?;
            self.protect_committed_block_locations(&commit_location_pairs);

            if only_flush {
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
        let _inode_locks = self.lock_path_for_write(path, InodeLockMode::Read, false)?;
        let (status, file, block_ids) = {
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
            let file = inode.as_file_ref()?.clone();
            let block_ids = file.blocks.iter().map(|meta| meta.id).collect::<Vec<_>>();
            (inode.to_file_status(path)?, file, block_ids)
        };

        let _block_locks = self.block_location_locks.read_blocks(&block_ids);
        let store = self.rocks_store()?;
        let resolver = StorePathResolver::new(&store);
        let block_locs = self.get_block_locs_from_store_locked(path, &resolver, &file)?;
        let locate_blocks = FileBlocks { status, block_locs };

        Ok(locate_blocks)
    }

    fn get_block_locs_from_store_locked(
        &self,
        path: &str,
        resolver: &StorePathResolver<'_>,
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
                None => resolver.get_locations(meta.id)?,
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

    pub fn master_info(&self) -> FsResult<MasterInfo> {
        self.ensure_metadata_current()?;
        let metrics = Master::get_metrics()?;
        let mut info = MasterInfo {
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
                WorkerStatus::Live => info.live_workers.push(worker.clone()),
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
                can_reconcile: false,
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
                    can_reconcile: false,
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
    }

    pub fn reset_full_block_report(&self, worker_id: u32) {
        self.full_block_reports.lock().remove(&worker_id);
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
            let _commit_guard = self.namespace_commit_gate.enter();
            let _block_locks = self
                .block_location_locks
                .write_worker_blocks(worker_id, &block_ids);
            let store = self.rocks_store()?;
            let snapshot = store.snapshot();
            let mut file_blocks = HashMap::new();
            let mut filtered = Vec::with_capacity(batch.len());
            let mut missing_adds = Vec::new();
            let mut full_report_updates = Vec::new();
            for (add, block_id, location) in batch {
                if add && !Self::block_exists_in_snapshot(&snapshot, &mut file_blocks, block_id)? {
                    missing_adds.push(block_id);
                    continue;
                }
                if !add || protect_adds_during_full_report {
                    full_report_updates.push((add, block_id));
                }
                filtered.push((add, block_id, location));
            }
            store.apply_block_locations(filtered)?;
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
            let _commit_guard = self.namespace_commit_gate.enter();
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
            let _commit_guard = self.namespace_commit_gate.enter();
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
        let batch = block_ids
            .iter()
            .map(|block_id| (false, *block_id, BlockLocation::with_id(worker_id)))
            .collect::<Vec<_>>();
        self.rocks_store()?.apply_block_locations(batch)?;
        Ok(())
    }

    pub fn add_block_location(&self, block_id: i64, location: BlockLocation) -> FsResult<()> {
        self.apply_block_report_batch(location.worker_id, vec![(true, block_id, location)], true)?;
        Ok(())
    }

    pub fn commit_mount(&self, info: MountInfo) -> FsResult<()> {
        self.run_namespace_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let op_id = self.next_op_id();
            self.rocks_store()?.add_mountpoint(info.mount_id, &info)?;
            self.journal_writer.log_mount_by_id(op_id, info)?;
            Ok(())
        })
    }

    pub fn commit_unmount(&self, mount_id: u32) -> FsResult<()> {
        self.run_namespace_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let op_id = self.next_op_id();
            self.rocks_store()?.remove_mountpoint(mount_id)?;
            self.journal_writer.log_unmount_by_id(op_id, mount_id)?;
            Ok(())
        })
    }

    /// Process block reports
    pub fn block_report(&self, list: BlockReportList) -> FsResult<Vec<i64>> {
        self.ensure_metadata_current()?;
        // @todo check cluster.
        let completed_full_report = self.collect_full_block_report(&list)?;
        if list.blocks.is_empty() && completed_full_report.is_none() {
            return Ok(Vec::new());
        }

        let mut batch: Vec<(bool, i64, BlockLocation)> = vec![];
        let mut wm = self.worker_manager.write();
        for item in list.blocks {
            let loc = BlockLocation::new(list.worker_id, item.storage_type);
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    batch.push((true, item.id, loc));
                }
                BlockReportStatus::Deleted => {
                    batch.push((false, item.id, loc));
                    wm.deleted_block(list.worker_id, item.id);
                }
            }
        }
        drop(wm);

        let missing_after_lock =
            self.apply_block_report_batch(list.worker_id, batch, !list.full_report)?;
        if !missing_after_lock.is_empty() {
            let mut wm = self.worker_manager.write();
            for block_id in missing_after_lock {
                wm.remove_block(list.worker_id, block_id);
            }
        }

        let mut stale_block_ids = Vec::new();
        if let Some(generation) = completed_full_report {
            stale_block_ids = self.finish_full_block_report(list.worker_id, generation)?;
        }

        if !stale_block_ids.is_empty() {
            warn!(
                "full block report reconciled {} stale block locations for worker {}",
                stale_block_ids.len(),
                list.worker_id
            );
        }

        Ok(stale_block_ids)
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        self.delete_worker_block_locations(worker_id, |_| true)
    }

    pub fn set_attr<T: AsRef<str>>(&self, path: T, opts: SetAttrOpts) -> FsResult<FileStatus> {
        let path = path.as_ref();
        self.run_namespace_write(|| {
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
        let link = link.as_ref();
        self.run_namespace_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let _inode_locks = self.lock_path_for_write(link, InodeLockMode::Write, true)?;
            let fs_dir = self.fs_dir.read();
            let target = target.as_ref().to_string();
            let link = Self::resolve_path(&fs_dir, link)?;
            fs_dir.symlink(target, link, force, mode)
        })
    }

    pub fn link<T: AsRef<str>>(&self, src_path: T, dst_path: T) -> FsResult<()> {
        let src_path = src_path.as_ref();
        let dst_path = dst_path.as_ref();
        self.run_namespace_write(|| {
            let (_inode_locks, _replaced_block_ids) =
                self.lock_rename_paths(src_path, dst_path, RenameFlags::NO_REPLACE)?;
            let _journal_scope =
                self.reserve_journal_scope(self.estimate_link_entries(dst_path)?)?;
            let fs_dir = self.fs_dir.read();
            let src_path = Self::resolve_path(&fs_dir, src_path)?;
            let dst_path = Self::resolve_path(&fs_dir, dst_path)?;
            fs_dir.link(src_path, dst_path)
        })
    }

    pub fn resize<T: AsRef<str>>(&self, path: T, opts: FileAllocOpts) -> FsResult<FileBlocks> {
        self.run_namespace_write(|| {
            opts.validate()?;

            let path = path.as_ref();
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
                let del_res = fs_dir.resize(&inp, opts)?;
                let blocks = self.get_file_blocks(path, &fs_dir, &inp)?;
                (del_res, blocks)
            };

            if !del_res.blocks.is_empty() {
                self.worker_manager.write().remove_blocks(&del_res);
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
        self.run_namespace_write(|| {
            let _journal_scope = self.reserve_journal_scope(1)?;
            let _inode_locks = self.lock_path_for_write(path, InodeLockMode::Write, false)?;
            let fs_dir = self.fs_dir.read();
            let inp = Self::resolve_path(&fs_dir, path)?;

            let choose_workers = self.choose_worker(&inp, client_addr, exclude_workers)?;
            let block = fs_dir.assign_worker(inp, block.id, &choose_workers)?;

            Ok(LocatedBlock {
                block,
                locs: choose_workers,
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
        self.run_namespace_write(|| {
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
