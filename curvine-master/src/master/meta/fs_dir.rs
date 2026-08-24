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

use crate::master::fs::DeleteResult;
use crate::master::journal::{JournalEntry, JournalWriter};
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::inode::InodeView::{Dir, File, FileEntry};
use crate::master::meta::inode::*;
use crate::master::meta::store::{
    InodeStore, RenameStoreRequest, RocksInodeStore, RocksStoreHandle,
};
use crate::master::meta::{
    BlockMeta, InodeId, MetadataReplicaPath, MetadataReplicaPathEntry, MetadataReplicaReader,
    SameParentRenamePlan,
};
use crate::master::quota::eviction::evictor::Evictor;
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, err_ext, try_option, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::{
    BlockLocation, CommitBlock, CreateFileOpts, DirectoryAttributeDelta, DirectoryAttributes,
    ExtendedBlock, FileAllocOpts, FileLock, FileStatus, FreeResult, ListOptions, MkdirOpts,
    MountInfo, RenameFlags, SetAttrOpts, TtlAction, WorkerAddress, INTERNAL_CTIME_XATTR,
};
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::sync::AtomicCounter;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet, LinkedList};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum DirectoryAttributeWrite {
    Merge,
    Replace,
}

enum DirectoryExchangeMutation<'a> {
    Same(DirectoryRenameMutation<'a>),
    Separate {
        source: DirectoryMutation<'a>,
        destination: DirectoryMutation<'a>,
    },
}

impl DirectoryExchangeMutation<'_> {
    fn exchange(
        &mut self,
        source_id: i64,
        source_name: &str,
        destination_id: i64,
        destination_name: &str,
        at_source: InodeView,
        at_destination: InodeView,
    ) -> CommonResult<()> {
        match self {
            Self::Same(mutation) => mutation.exchange_children(
                source_id,
                source_name,
                destination_id,
                destination_name,
                at_source,
                at_destination,
            ),
            Self::Separate {
                source,
                destination,
            } => {
                let _ = source.delete_child(source_id, source_name)?;
                let _ = destination.delete_child(destination_id, destination_name)?;
                let _ = source.add_child(at_source)?;
                let _ = destination.add_child(at_destination)?;
                Ok(())
            }
        }
    }
}

pub(crate) enum SameParentRename {
    NotApplicable,
    Retry,
    Noop,
    Renamed(Option<DeleteResult>),
}

/// Namespace mutation is protected by inode/block object locks. The in-memory
/// tree is updated through InodePtr while FsDir's outer lock acts as a restore
/// barrier, not as the normal write serialization point.
#[derive(Default)]
pub(crate) struct CacheInvalidationResult {
    pub delete_result: DeleteResult,
    pub invalidated_block_ids: HashSet<i64>,
}

impl CacheInvalidationResult {
    pub(crate) fn extend(&mut self, other: Self) {
        self.delete_result.inodes += other.delete_result.inodes;
        for (block_id, locations) in other.delete_result.blocks {
            self.delete_result
                .blocks
                .entry(block_id)
                .or_default()
                .extend(locations);
        }
        self.invalidated_block_ids
            .extend(other.invalidated_block_ids);
    }
}

pub struct FsDir {
    pub(crate) root_dir: InodeView,
    pub(crate) inode_id: InodeId,
    pub(crate) store: InodeStore,
    pub(crate) store_handle: Arc<RocksStoreHandle>,
    pub(crate) journal_writer: Arc<JournalWriter>,
    pub(crate) evictor: Arc<dyn Evictor>,
    pub(crate) op_id: Arc<AtomicCounter>,
    pub(crate) metadata_reader: Arc<MetadataReplicaReader>,
}

impl FsDir {
    pub fn new(
        conf: &ClusterConf,
        journal_writer: Arc<JournalWriter>,
        ttl_bucket_list: Arc<TtlBucketList>,
        evictor: Arc<dyn Evictor>,
    ) -> FsResult<Self> {
        let db_conf = conf.db_conf();

        let store = RocksInodeStore::new(db_conf, conf.format_master)?;
        let state = InodeStore::new(store, ttl_bucket_list)?;
        state.migrate_directory_attributes()?;
        state.initialize_root_directory_attributes()?;
        let store_handle = Arc::new(RocksStoreHandle::new(&state.store));
        let (last_inode_id, root_dir) = state.create_blank_tree()?;

        let metadata_reader = Arc::new(MetadataReplicaReader::new(
            root_dir.clone(),
            conf.master.metadata_read_cache_size.as_byte(),
        )?);
        let fs_dir = Self {
            metadata_reader,
            root_dir,
            inode_id: InodeId::new(),
            store: state,
            store_handle,
            journal_writer,
            evictor,
            op_id: Arc::new(AtomicCounter::new(0)),
        };
        fs_dir.update_last_inode_id(last_inode_id)?;

        Ok(fs_dir)
    }

    // Create root directory
    pub fn create_root() -> InodeView {
        InodeView::new_dir(ROOT_INODE_NAME.to_string(), InodeDir::new(ROOT_INODE_ID, 0))
    }

    pub fn root_ptr(&self) -> InodePtr {
        InodePtr::from_ref(&self.root_dir)
    }

    pub fn root_dir(&self) -> &InodeView {
        &self.root_dir
    }

    fn hydrate_inode(&self, inode: InodePtr) -> FsResult<InodePtr> {
        let FileEntry(entry) = inode.as_ref() else {
            return Ok(inode);
        };
        let mut stored = try_option!(
            self.store.get_inode(entry.id(), Some(entry.name()))?,
            "Failed to load inode {} from store",
            entry.id()
        );
        stored.change_name(entry.name().to_string());
        Ok(InodePtr::from_owned(stored))
    }

    pub(crate) fn metadata_reader(&self) -> Arc<MetadataReplicaReader> {
        self.metadata_reader.clone()
    }

    pub(crate) fn invalidate_file_status_for_inode(&self, inode: &InodeView) {
        if !inode.is_file_entry() {
            self.invalidate_file_status(inode.id());
        }
    }

    pub(crate) fn invalidate_file_status(&self, inode_id: i64) {
        self.metadata_reader.invalidate_file_status(inode_id);
    }

    fn next_inode_id(&self) -> FsResult<i64> {
        let id = self.inode_id.next()?;
        Ok(id)
    }

    pub fn next_op_id(&self) -> u64 {
        self.op_id.next()
    }

    pub fn op_id_counter(&self) -> Arc<AtomicCounter> {
        self.op_id.clone()
    }

    pub fn update_op_id(&self, op_id: u64) {
        if op_id > self.op_id.get() {
            self.op_id.set(op_id);
        }
    }

    pub fn get_ttl_bucket_list(&self) -> Arc<TtlBucketList> {
        self.store.get_ttl_bucket_list()
    }

    pub fn mkdir(&self, inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
        self.mkdir_with_attribute_write(inp, opts, DirectoryAttributeWrite::Merge)
    }

    pub(crate) fn mkdir_uncontended(&self, inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
        self.mkdir_with_attribute_write(inp, opts, DirectoryAttributeWrite::Replace)
    }

    fn mkdir_with_attribute_write(
        &self,
        mut inp: InodePath,
        opts: MkdirOpts,
        attribute_write: DirectoryAttributeWrite,
    ) -> FsResult<InodePath> {
        // Create parent directory
        inp = self.create_parent_dir(inp, opts.parent_opts(), attribute_write)?;

        // Create the final directory.
        inp = self.create_single_dir(inp, opts, attribute_write)?;
        Ok(inp)
    }

    // Create the first subdirectory that does not exist.
    // 1. If all directories on the path already exist, skip and return successful.
    // 2. If the parent directory does not exist, an error is returned.
    fn create_single_dir(
        &self,
        mut inp: InodePath,
        mut opts: MkdirOpts,
        attribute_write: DirectoryAttributeWrite,
    ) -> FsResult<InodePath> {
        if inp.is_full() || inp.is_root() {
            return Ok(inp);
        }

        let pos = inp.existing_len() - 1;
        let name = inp.get_component(pos + 1)?.to_string();

        self.apply_setgid_directory_inheritance(&inp, &mut opts)?;

        let dir = InodeDir::with_opts(self.next_inode_id()?, LocalTime::mills() as i64, opts);
        let dir_path = inp.get_path(inp.existing_len() + 1);
        inp = self.add_last_inode_after_store(
            inp,
            InodeView::new_dir(name, dir.clone()),
            attribute_write,
            |_| self.journal_writer.log_mkdir(self, &dir_path, &dir),
        )?;

        Ok(inp)
    }

    fn apply_setgid_directory_inheritance(
        &self,
        inp: &InodePath,
        opts: &mut MkdirOpts,
    ) -> FsResult<()> {
        if inp.existing_len() == 0 {
            return Ok(());
        }
        let parent_pos = inp.existing_len() as i32 - 1;
        let parent = match inp.get_inode(parent_pos) {
            Some(parent) => parent,
            None => return Ok(()),
        };
        if let Some(group) = parent.as_dir_ref()?.inherited_setgid_group() {
            opts.group = group;
            opts.mode |= MODE_SETGID;
        }
        Ok(())
    }

    fn directory_attribute_replacement(
        parent: &InodeView,
        delta: DirectoryAttributeDelta,
        attribute_write: DirectoryAttributeWrite,
    ) -> CommonResult<Option<DirectoryAttributes>> {
        match attribute_write {
            DirectoryAttributeWrite::Merge => Ok(None),
            DirectoryAttributeWrite::Replace => {
                parent.updated_directory_attributes(delta).map(Some)
            }
        }
    }

    // Create all previous directories that may be missing on the path.
    fn create_parent_dir(
        &self,
        mut inp: InodePath,
        opts: MkdirOpts,
        attribute_write: DirectoryAttributeWrite,
    ) -> FsResult<InodePath> {
        let mut index = inp.existing_len();

        // The parent directory already exists and does not need to be created.
        if inp.is_full() || index + 1 >= inp.len() {
            return Ok(inp);
        }

        while index <= inp.len() - 2 {
            inp = self.create_single_dir(inp, opts.clone(), attribute_write)?;
            index += 1;
        }

        Ok(inp)
    }

    // Delete files or directories
    pub fn delete(&self, inp: &InodePath, recursive: bool) -> FsResult<DeleteResult> {
        self.delete_with_attribute_write(inp, recursive, DirectoryAttributeWrite::Merge)
    }

    pub(crate) fn delete_uncontended(
        &self,
        inp: &InodePath,
        recursive: bool,
    ) -> FsResult<DeleteResult> {
        self.delete_with_attribute_write(inp, recursive, DirectoryAttributeWrite::Replace)
    }

    fn delete_with_attribute_write(
        &self,
        inp: &InodePath,
        recursive: bool,
        attribute_write: DirectoryAttributeWrite,
    ) -> FsResult<DeleteResult> {
        let op_ms = LocalTime::mills();

        if inp.is_root() {
            return err_box!("The root is not allowed to be deleted");
        }

        if inp.is_empty() || inp.get_last_inode().is_none() {
            return err_ext!(FsError::file_not_found(inp.path()));
        }

        if !inp.is_empty_dir() && !recursive {
            return err_ext!(FsError::dir_not_empty(inp.path()));
        }

        let del_res =
            self.unprotected_delete_with_attribute_write(inp, op_ms as i64, attribute_write)?;
        self.journal_writer
            .log_delete(self, inp.path(), op_ms as i64)?;

        Ok(del_res)
    }

    pub(crate) fn unprotected_delete(&self, inp: &InodePath, mtime: i64) -> FsResult<DeleteResult> {
        self.unprotected_delete_with_attribute_write(inp, mtime, DirectoryAttributeWrite::Merge)
    }

    fn unprotected_delete_with_attribute_write(
        &self,
        inp: &InodePath,
        mtime: i64,
        attribute_write: DirectoryAttributeWrite,
    ) -> FsResult<DeleteResult> {
        let target = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_box!("Path not exists: {}", inp.path()),
        };

        let parent = match inp.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Abnormal data status"),
        };
        let child = target.as_ref();
        let child_name = inp.name();

        let parent_delta = DirectoryAttributeDelta::new(mtime, mtime, -i32::from(child.is_dir()));
        let parent_attributes =
            Self::directory_attribute_replacement(parent.as_ref(), parent_delta, attribute_write)?;
        let mut parent_children = parent.as_dir_ref()?.begin_child_mutation(child_name);

        let del_res = match child {
            File(f) => {
                if f.nlink() > 1 {
                    let target_inode = target.clone();
                    if let File(ref mut nf) = target_inode.as_mut() {
                        nf.decrement_nlink(mtime);
                    }
                    self.store.apply_unlink(
                        parent.as_ref(),
                        child,
                        child_name,
                        mtime,
                        parent_attributes,
                    )?
                } else {
                    // This is the last link, delete the inode
                    self.store.apply_delete(
                        parent.as_ref(),
                        child,
                        child_name,
                        mtime,
                        parent_attributes,
                    )?
                }
            }
            FileEntry(e) => {
                // This is a link entry, just remove the directory entry
                // The actual inode's nlink count should be decremented
                self.store.apply_unlink_file_entry(
                    parent.as_ref(),
                    child,
                    child_name,
                    e.id,
                    mtime,
                    parent_attributes,
                )?
            }
            Dir(_) => {
                // Directories are always deleted
                self.store.apply_delete(
                    parent.as_ref(),
                    child,
                    child_name,
                    mtime,
                    parent_attributes,
                )?
            }
        };

        parent.apply_directory_attribute_delta(parent_delta)?;

        if !child.is_dir() && !del_res.file_ids.contains(&child.id()) {
            self.invalidate_file_status(child.id());
        }
        for inode_id in &del_res.file_ids {
            self.invalidate_file_status(*inode_id);
        }

        // After deletion occurs, the target address cannot be used.
        let _ = parent_children.delete_child(child.id(), child_name)?;
        Ok(del_res)
    }

    pub fn free(&self, inp: &InodePath, recursive: bool) -> FsResult<FreeResult> {
        let op_ms = LocalTime::mills() as i64;

        if inp.is_root() {
            return err_box!("The root is not allowed to be free");
        }

        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let free_res = self.unprotected_free(inode, op_ms, recursive)?;
        self.journal_writer
            .log_free(self, inp.path(), op_ms, recursive)?;

        Ok(free_res)
    }

    pub(crate) fn unprotected_free(
        &self,
        inode: InodePtr,
        mtime: i64,
        recursive: bool,
    ) -> FsResult<FreeResult> {
        let mut free_res = FreeResult::default();
        let mut change_inodes = vec![];

        let mut stack = LinkedList::new();
        stack.push_back(inode);
        while let Some(inode) = stack.pop_front() {
            let inode = self.hydrate_inode(inode)?;
            match inode.as_mut() {
                FileEntry(e) => {
                    if let Some(store_inode) = self.store.get_inode(e.id, Some(&e.name))? {
                        stack.push_back(InodePtr::from_owned(store_inode));
                    }
                }

                Dir(d) => {
                    if recursive {
                        for child in d.child_ptrs() {
                            stack.push_back(child);
                        }
                    }
                }

                File(f) => {
                    let locs = f.get_locs(&self.store)?;
                    let bytes = f.get_locs_bytes(&locs);
                    if f.free(mtime) {
                        free_res.add(bytes, locs);
                        change_inodes.push(inode.as_ref().clone());
                    }
                }
            }
        }

        self.store.apply_free(change_inodes.clone())?;
        for inode in &change_inodes {
            self.invalidate_file_status_for_inode(inode);
        }
        Ok(free_res)
    }

    pub fn rename(
        &self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        flags: RenameFlags,
    ) -> FsResult<Option<DeleteResult>> {
        let op_ms = LocalTime::mills();
        let exchange_pre_swap_ids = if flags.exchange_mode() {
            let src_id = src_inp
                .get_last_inode()
                .map(|inode| inode.id())
                .unwrap_or(0);
            let dst_id = dst_inp
                .get_last_inode()
                .map(|inode| inode.id())
                .unwrap_or(0);
            Some((src_id, dst_id))
        } else {
            None
        };
        let res =
            self.unprotected_rename(src_inp, dst_inp, op_ms as i64, flags, exchange_pre_swap_ids)?;
        self.journal_writer.log_rename(
            self,
            src_inp.path(),
            dst_inp.path(),
            op_ms as i64,
            flags,
            exchange_pre_swap_ids,
        )?;
        Ok(res)
    }

    pub(crate) fn rename_same_parent(
        &self,
        plan: &SameParentRenamePlan,
        src: &str,
        dst: &str,
        flags: RenameFlags,
    ) -> FsResult<SameParentRename> {
        if flags.exchange_mode() {
            return Ok(SameParentRename::NotApplicable);
        }

        let src_entry = try_option!(plan.src_path.target(), "Source path {} has no target", src);
        let dst_entry = plan
            .dst_path
            .is_full(plan.dst_component_count)
            .then(|| plan.dst_path.target())
            .flatten();
        let dst_name = try_option!(
            InodeView::path_components(dst)?.last(),
            "Destination path {} has no components",
            dst
        )
        .to_string();
        let src_parent = try_option!(
            Self::replica_rename_parent(&plan.src_path, plan.src_component_count),
            "Source path {} has no parent",
            src
        );
        let dst_parent = try_option!(
            Self::replica_rename_parent(&plan.dst_path, plan.dst_component_count),
            "Destination path {} has no parent",
            dst
        );
        if src_parent.inode_id != dst_parent.inode_id {
            return Ok(SameParentRename::NotApplicable);
        }
        let parent_children = self.metadata_reader.directory_handle(src_parent)?;
        let mut children =
            parent_children.begin_rename_mutation(src_entry.name.as_ref(), &dst_name);
        if children
            .child_view(src_entry.name.as_ref())
            .is_none_or(|inode| inode.id() != src_entry.inode_id)
        {
            return Ok(SameParentRename::Retry);
        }
        let destination_matches = match (dst_entry, children.child_view(&dst_name)) {
            (Some(expected), Some(actual)) => actual.id() == expected.inode_id,
            (None, None) => true,
            _ => false,
        };
        if !destination_matches {
            return Ok(SameParentRename::Retry);
        }

        let mtime = LocalTime::mills() as i64;
        let src_inode = try_option!(
            self.store
                .get_inode(src_entry.inode_id, Some(src_entry.name.as_ref()))?,
            "Source inode {} not found for rename {}",
            src_entry.inode_id,
            src
        );
        let dst_inode = match dst_entry {
            Some(entry) => Some(try_option!(
                self.store
                    .get_inode(entry.inode_id, Some(entry.name.as_ref()))?,
                "Destination inode {} not found for rename {}",
                entry.inode_id,
                dst
            )),
            None => None,
        };

        if flags.no_replace() && dst_inode.is_some() {
            return Err(FsError::file_exists(dst));
        }
        if let Some(dst_inode) = &dst_inode {
            if src_inode.id() == dst_inode.id() {
                return Ok(SameParentRename::Noop);
            }
            if src_inode.is_file() && !dst_inode.is_file() {
                return Err(FsError::is_a_directory(dst));
            }
            if !src_inode.is_file() && dst_inode.is_file() {
                return Err(FsError::not_a_directory(dst));
            }
            if !src_inode.is_file() && !dst_inode.is_file() {
                let entry =
                    try_option!(dst_entry, "Destination path {} has no metadata entry", dst);
                if !self.metadata_reader.directory_is_empty(entry)? {
                    return Err(FsError::dir_not_empty(dst));
                }
            }
        }

        let mut renamed_inode = src_inode.clone();
        if src_entry.is_dir {
            renamed_inode
                .set_directory_children_handle(self.metadata_reader.directory_handle(src_entry)?)?;
        }
        renamed_inode.change_name(dst_name.clone());
        renamed_inode.set_parent_id(src_parent.inode_id);
        renamed_inode.update_ctime(mtime);
        let renamed_file_id = renamed_inode.is_file().then_some(renamed_inode.id());
        let replaced_file_id = dst_inode
            .as_ref()
            .filter(|inode| inode.is_file())
            .map(|inode| inode.id());
        let parent_delta = DirectoryAttributeDelta::new(
            mtime,
            mtime,
            -dst_inode
                .as_ref()
                .map_or(0, |inode| i32::from(inode.is_dir())),
        );
        let attributes = parent_children.attribute_state();
        let delete_result = self.store.apply_rename(RenameStoreRequest {
            src_parent_id: src_parent.inode_id,
            src_inode: &src_inode,
            src_name: src_inode.name(),
            dst_parent_id: src_parent.inode_id,
            dst_inode: &renamed_inode,
            replaced: dst_inode.as_ref().map(|inode| (inode, dst_name.as_str())),
            src_parent_delta: attributes.delta(
                try_option!(
                    attributes.current(),
                    "Directory {} has no attribute state",
                    src_parent.inode_id
                ),
                parent_delta,
            ),
            dst_parent_delta: None,
        })?;
        children.rename_child(
            src_inode.id(),
            src_inode.name(),
            dst_inode
                .as_ref()
                .map(|inode| (inode.id(), dst_name.as_str())),
            renamed_inode,
        )?;
        attributes.apply(parent_delta)?;
        attributes.mark_persisted();
        if let Some(inode_id) = renamed_file_id {
            self.invalidate_file_status(inode_id);
        }
        if let Some(inode_id) = replaced_file_id {
            self.invalidate_file_status(inode_id);
        }
        self.journal_writer
            .log_rename(self, src, dst, mtime, flags, None)?;
        Ok(SameParentRename::Renamed(
            dst_inode.is_some().then_some(delete_result),
        ))
    }

    fn replica_rename_parent(
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

    pub(crate) fn unprotected_rename(
        &self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        mtime: i64,
        flags: RenameFlags,
        exchange_pre_swap_ids: Option<(i64, i64)>,
    ) -> FsResult<Option<DeleteResult>> {
        let src_inode = match src_inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(src_inp.path())),
            Some(v) => v,
        };
        if !flags.is_supported() {
            return err_ext!(FsError::unsupported(format!(
                "unsupported rename flags: {:#x}",
                flags.value()
            )));
        }
        if flags.exchange_mode() {
            self.unprotected_exchange(src_inp, dst_inp, mtime, exchange_pre_swap_ids)?;
            return Ok(None);
        }

        let src_parent = match src_inp.get_inode(-2) {
            None => return err_box!("Parent not exists: {}", src_inp.path()),
            Some(v) => v,
        };

        let dst_inode = dst_inp.get_last_inode();
        if flags.no_replace() && dst_inode.is_some() {
            return err_ext!(FsError::file_exists(dst_inp.path()));
        }
        if let Some(dst_inode) = &dst_inode {
            let src_is_file = src_inode.is_file();
            let dst_is_file = dst_inode.is_file();
            if src_inode.id() == dst_inode.id() {
                return Ok(None);
            }
            if src_is_file && !dst_is_file {
                return err_ext!(FsError::is_a_directory(dst_inp.path()));
            }
            if !src_is_file && dst_is_file {
                return err_ext!(FsError::not_a_directory(dst_inp.path()));
            }
            if !src_is_file && !dst_is_file && !dst_inp.is_empty_dir() {
                return err_ext!(FsError::dir_not_empty(dst_inp.path()));
            }
        }

        let new_name = dst_inp.name().to_string();
        let dst_parent = match dst_inp.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Parent {} does not exist", dst_inp.get_parent_path()),
        };
        let replaced_file_id = dst_inode
            .as_ref()
            .filter(|inode| inode.is_file())
            .map(|inode| inode.id());

        // Modify the time and name of the rename node.
        let mut new_inode = src_inode.as_ref().clone();
        new_inode.change_name(new_name);
        new_inode.set_parent_id(dst_parent.id());
        new_inode.update_ctime(mtime);
        let cached_file = new_inode.is_file().then(|| new_inode.clone());

        let (src_parent_delta, dst_parent_delta) = if src_parent.id() == dst_parent.id() {
            (
                DirectoryAttributeDelta::new(
                    mtime,
                    mtime,
                    -dst_inode
                        .as_ref()
                        .map_or(0, |inode| i32::from(inode.is_dir())),
                ),
                None,
            )
        } else {
            (
                DirectoryAttributeDelta::new(mtime, mtime, -i32::from(src_inode.is_dir())),
                Some(DirectoryAttributeDelta::new(
                    mtime,
                    mtime,
                    i32::from(src_inode.is_dir())
                        - dst_inode
                            .as_ref()
                            .map_or(0, |inode| i32::from(inode.is_dir())),
                )),
            )
        };

        let src_parent_store_delta = src_parent
            .as_ref()
            .directory_attribute_delta(src_parent_delta)?;
        let dst_parent_store_delta = dst_parent_delta
            .map(|delta| dst_parent.as_ref().directory_attribute_delta(delta))
            .transpose()?;

        let src_id = src_parent.id();
        let dst_id = dst_parent.id();
        let src_children = src_parent.as_dir_ref()?.children_handle();
        let dst_children = dst_parent.as_dir_ref()?.children_handle();
        let apply_store = || {
            self.store.apply_rename(RenameStoreRequest {
                src_parent_id: src_parent.id(),
                src_inode: src_inode.as_ref(),
                src_name: src_inp.name(),
                dst_parent_id: dst_parent.id(),
                dst_inode: &new_inode,
                replaced: dst_inode
                    .as_ref()
                    .map(|inode| (inode.as_ref(), dst_inp.name())),
                src_parent_delta: src_parent_store_delta,
                dst_parent_delta: dst_parent_store_delta,
            })
        };
        let finish_memory_update = || -> CommonResult<()> {
            src_parent.apply_directory_attribute_delta(src_parent_delta)?;
            src_parent.mark_directory_attributes_persisted();
            if let Some(delta) = dst_parent_delta {
                dst_parent.apply_directory_attribute_delta(delta)?;
                dst_parent.mark_directory_attributes_persisted();
            }
            if let Some(inode) = cached_file.as_ref() {
                self.invalidate_file_status_for_inode(inode);
            }
            if let Some(inode_id) = replaced_file_id {
                self.invalidate_file_status(inode_id);
            }
            Ok(())
        };
        let delete_result = if src_id == dst_id {
            let mut children = src_parent
                .as_dir_ref()?
                .begin_rename_child_mutation(src_inode.name(), new_inode.name());
            let delete_result = apply_store()?;
            children.rename_child(
                src_inode.id(),
                src_inode.name(),
                dst_inode.as_ref().map(|inode| (inode.id(), dst_inp.name())),
                new_inode,
            )?;
            finish_memory_update()?;
            delete_result
        } else if src_id < dst_id {
            let mut src_children = src_children.begin_mutation(src_inode.name());
            let mut dst_children = dst_children.begin_mutation(new_inode.name());
            let delete_result = apply_store()?;
            if let Some(inode) = &dst_inode {
                let _ = dst_children.delete_child(inode.id(), dst_inp.name())?;
            }
            let _ = src_children.delete_child(src_inode.id(), src_inode.name())?;
            let _ = dst_children.add_child(new_inode)?;
            finish_memory_update()?;
            delete_result
        } else {
            let mut dst_children = dst_children.begin_mutation(new_inode.name());
            let mut src_children = src_children.begin_mutation(src_inode.name());
            let delete_result = apply_store()?;
            if let Some(inode) = &dst_inode {
                let _ = dst_children.delete_child(inode.id(), dst_inp.name())?;
            }
            let _ = src_children.delete_child(src_inode.id(), src_inode.name())?;
            let _ = dst_children.add_child(new_inode)?;
            finish_memory_update()?;
            delete_result
        };

        Ok(dst_inode.is_some().then_some(delete_result))
    }

    fn unprotected_exchange(
        &self,
        src_inp: &InodePath,
        dst_inp: &InodePath,
        mtime: i64,
        pre_swap_ids: Option<(i64, i64)>,
    ) -> FsResult<()> {
        let src_inode = match src_inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(src_inp.path())),
            Some(v) => v,
        };
        let dst_inode = match dst_inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(dst_inp.path())),
            Some(v) => v,
        };

        let src_id = src_inode.id();
        let dst_id = dst_inode.id();

        if src_id == dst_id {
            return Ok(());
        }

        if let Some((expected_src, expected_dst)) = pre_swap_ids {
            if expected_src != 0 && expected_dst != 0 {
                if src_id == expected_dst && dst_id == expected_src {
                    return Ok(());
                }
                if src_id != expected_src || dst_id != expected_dst {
                    warn!(
                        "Exchange replay inode id mismatch at {} and {}: current ({}, {}), expected ({}, {})",
                        src_inp.path(),
                        dst_inp.path(),
                        src_id,
                        dst_id,
                        expected_src,
                        expected_dst
                    );
                    return Ok(());
                }
            }
        }

        let mut src_parent = match src_inp.get_inode(-2) {
            None => return err_box!("Parent not exists: {}", src_inp.path()),
            Some(v) => v,
        };
        let mut dst_parent = match dst_inp.get_inode(-2) {
            None => return err_box!("Parent not exists: {}", dst_inp.path()),
            Some(v) => v,
        };

        let src_name = src_inp.name().to_string();
        let dst_name = dst_inp.name().to_string();

        let mut at_src = dst_inode.as_ref().clone();
        at_src.change_name(src_name.clone());
        at_src.set_parent_id(src_parent.id());

        let mut at_dst = src_inode.as_ref().clone();
        at_dst.change_name(dst_name.clone());
        at_dst.set_parent_id(dst_parent.id());

        let src_children = src_parent.as_dir_ref()?.children_handle();
        let dst_children = dst_parent.as_dir_ref()?.children_handle();
        let mut exchange_mutation = if src_parent.id() == dst_parent.id() {
            DirectoryExchangeMutation::Same(
                src_children.begin_rename_mutation(&src_name, &dst_name),
            )
        } else if src_parent.id() < dst_parent.id() {
            let source = src_children.begin_mutation(&src_name);
            let destination = dst_children.begin_mutation(&dst_name);
            DirectoryExchangeMutation::Separate {
                source,
                destination,
            }
        } else {
            let destination = dst_children.begin_mutation(&dst_name);
            let source = src_children.begin_mutation(&src_name);
            DirectoryExchangeMutation::Separate {
                source,
                destination,
            }
        };

        let src_nlink_delta = i32::from(dst_inode.is_dir()) - i32::from(src_inode.is_dir());
        let src_parent_delta = DirectoryAttributeDelta::new(mtime, mtime, src_nlink_delta);
        let dst_parent_delta = (src_parent.id() != dst_parent.id())
            .then(|| DirectoryAttributeDelta::new(mtime, mtime, -src_nlink_delta));

        src_parent.update_mtime(mtime);
        dst_parent.update_mtime(mtime);

        if src_parent.id() != dst_parent.id() {
            let src_was_dir = src_inode.is_dir();
            let dst_was_dir = dst_inode.is_dir();
            if src_was_dir && !dst_was_dir {
                src_parent.dec_nlink(mtime)?;
            } else if !src_was_dir && dst_was_dir {
                src_parent.incr_nlink(mtime)?;
            }
            if dst_was_dir && !src_was_dir {
                dst_parent.dec_nlink(mtime)?;
            } else if !dst_was_dir && src_was_dir {
                dst_parent.incr_nlink(mtime)?;
            }
        }

        self.store.apply_exchange(
            src_parent.as_ref(),
            &src_name,
            dst_parent.as_ref(),
            &dst_name,
            &at_src,
            &at_dst,
            src_parent_delta,
            dst_parent_delta,
        )?;

        exchange_mutation.exchange(
            src_inode.id(),
            &src_name,
            dst_inode.id(),
            &dst_name,
            at_src,
            at_dst,
        )?;

        Ok(())
    }

    pub fn create_file(&self, inp: InodePath, opts: CreateFileOpts) -> FsResult<InodePath> {
        self.create_file_with_attribute_write(inp, opts, DirectoryAttributeWrite::Merge)
    }

    pub(crate) fn create_file_uncontended(
        &self,
        inp: InodePath,
        opts: CreateFileOpts,
    ) -> FsResult<InodePath> {
        self.create_file_with_attribute_write(inp, opts, DirectoryAttributeWrite::Replace)
    }

    fn create_file_with_attribute_write(
        &self,
        mut inp: InodePath,
        opts: CreateFileOpts,
        attribute_write: DirectoryAttributeWrite,
    ) -> FsResult<InodePath> {
        if inp.get_last_inode().is_some() {
            return err_ext!(FsError::file_exists(inp.path()));
        }

        // Create a directory that does not exist.
        inp = self.create_parent_dir(inp, opts.dir_opts(), attribute_write)?;
        let name = inp.name().to_string();

        // Create an inode file node.
        let file = InodeFile::with_opts(self.next_inode_id()?, LocalTime::mills() as i64, opts);
        inp = self.add_last_inode_with_attribute_write(
            inp,
            InodeView::new_file(name, file),
            attribute_write,
        )?;
        self.journal_writer.log_create_file(self, &inp)?;

        Ok(inp)
    }

    pub(crate) fn add_last_inode(&self, inp: InodePath, child: InodeView) -> FsResult<InodePath> {
        self.add_last_inode_with_attribute_write(inp, child, DirectoryAttributeWrite::Merge)
    }

    fn add_last_inode_with_attribute_write(
        &self,
        inp: InodePath,
        child: InodeView,
        attribute_write: DirectoryAttributeWrite,
    ) -> FsResult<InodePath> {
        self.add_last_inode_after_store(inp, child, attribute_write, |_| Ok(()))
    }

    fn add_last_inode_after_store<F>(
        &self,
        mut inp: InodePath,
        child: InodeView,
        attribute_write: DirectoryAttributeWrite,
        after_store: F,
    ) -> FsResult<InodePath>
    where
        F: FnOnce(&InodePath) -> FsResult<()>,
    {
        if inp.is_full() || inp.is_root() {
            return Ok(inp);
        }

        let pos = inp.existing_len() as i32;

        // parent must be an existing directory.
        let parent = match inp.get_inode(pos - 1) {
            Some(v) => {
                if !v.is_dir() {
                    return err_box!("Parent path is not a directory: {}", inp.get_parent_path());
                } else {
                    v
                }
            }

            None => return err_box!("Parent path not exists: {}", inp.get_parent_path()),
        };

        let parent_delta =
            DirectoryAttributeDelta::new(child.mtime(), child.mtime(), i32::from(child.is_dir()));

        let mut child = child;
        child.set_parent_id(parent.id());
        let parent_attributes =
            Self::directory_attribute_replacement(parent.as_ref(), parent_delta, attribute_write)?;
        let mut parent_children = parent.as_dir_ref()?.begin_child_mutation(child.name());
        if parent_children.contains_child(child.name()) {
            return err_ext!(FsError::file_exists(inp.path()));
        }
        self.store
            .apply_add(parent.as_ref(), &child, parent_attributes)?;
        after_store(&inp)?;
        parent.apply_directory_attribute_delta(parent_delta)?;
        let added = parent_children.add_child(child)?;
        inp.append(added)?;

        Ok(inp)
    }

    pub fn file_status(&self, inp: &InodePath) -> FsResult<FileStatus> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let inode = self.hydrate_inode(inode)?;
        let status = match inode.as_ref() {
            File(..) | Dir(..) => inode.to_file_status(inp.path())?,
            FileEntry(..) => {
                return err_box!("FileEntry is not supported");
            }
        };

        Ok(status)
    }

    pub fn list_status(&self, inp: &InodePath) -> FsResult<Vec<FileStatus>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        match inode.as_ref() {
            File(_) => Ok(vec![inode.to_file_status(inp.path())?]),

            Dir(d) => {
                let children = d.children_vec();
                let res = self
                    .store
                    .batched_get_inodes(inp, children.iter().collect())?;
                Ok(res)
            }

            FileEntry(e) => {
                let inode_opt = self.store.get_inode(e.id, Some(&e.name))?;
                match inode_opt {
                    Some(inode_view) => Ok(vec![inode_view.to_file_status(inp.path())?]),
                    None => err_ext!(FsError::file_not_found(inp.path())),
                }
            }
        }
    }

    fn list_single_file(status: FileStatus, opts: &ListOptions) -> Vec<FileStatus> {
        if matches!(opts.limit, Some(0)) {
            return vec![];
        }
        if let Some(sa) = opts.start_after.as_deref() {
            if status.name.as_str() <= sa {
                return vec![];
            }
        }
        vec![status]
    }

    pub fn list_options(&self, inp: &InodePath, opts: &ListOptions) -> FsResult<Vec<FileStatus>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        match inode.as_ref() {
            File(_) => {
                let status = inode.to_file_status(inp.path())?;
                Ok(Self::list_single_file(status, opts))
            }

            Dir(d) => {
                let children = d.list_options(opts);
                let res = self
                    .store
                    .batched_get_inodes(inp, children.iter().collect())?;
                Ok(res)
            }

            FileEntry(e) => {
                let inode_opt = self.store.get_inode(e.id, Some(&e.name))?;
                match inode_opt {
                    Some(inode_view) => {
                        let status = inode_view.to_file_status(inp.path())?;
                        Ok(Self::list_single_file(status, opts))
                    }
                    None => err_ext!(FsError::file_not_found(inp.path())),
                }
            }
        }
    }

    pub fn acquire_new_block(
        &self,
        path: impl AsRef<str>,
        mut inode: InodePtr,
        commit_blocks: Vec<CommitBlock>,
        choose_workers: &[WorkerAddress],
        file_len: i64,
    ) -> FsResult<ExtendedBlock> {
        let file = inode.as_file_mut()?;

        let new_block_id = file.next_block_id()?;

        // flush file and commit block
        file.complete(file_len, &commit_blocks, "", true)?;

        // create block.
        file.add_block(BlockMeta::with_pre(new_block_id, choose_workers));

        let block = ExtendedBlock {
            id: new_block_id,
            len: 0,
            storage_type: file.storage_policy.storage_type,
            file_type: file.file_type,
            alloc_opts: None,
        };

        // state add block.
        self.store.apply_new_block(inode.as_ref(), &commit_blocks)?;
        self.invalidate_file_status_for_inode(inode.as_ref());
        self.journal_writer
            .log_add_block(self, path, inode.as_file_ref()?, commit_blocks)?;
        Ok(block)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_file(
        &self,
        path: impl AsRef<str>,
        inode: &mut InodePtr,
        len: i64,
        commit_block: Vec<CommitBlock>,
        client_name: impl AsRef<str>,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<bool> {
        let file = inode.as_file_mut()?;
        let id = file.id();
        file.complete(len, &commit_block, client_name, only_flush)?;

        // Only apply set_attr_opts on real complete/close, not on flush.
        // Flush semantics should remain narrow (durability only).
        if !only_flush {
            if let Some(opts) = set_attr_opts {
                inode.set_attr(opts)?;
            }
        }

        self.evictor.on_access(id);

        self.store
            .apply_complete_file(inode.as_ref(), &commit_block)?;
        self.invalidate_file_status_for_inode(inode.as_ref());
        self.journal_writer
            .log_complete_file(self, path, inode.as_file_ref()?, commit_block)?;

        Ok(true)
    }

    pub fn get_file_locations(
        &self,
        file: &InodeFile,
    ) -> FsResult<HashMap<i64, Vec<BlockLocation>>> {
        let locs = self.store.get_file_locations(file)?;
        self.evictor.on_access(file.id());
        Ok(locs)
    }

    pub fn add_block_location(&self, block_id: i64, location: BlockLocation) -> FsResult<()> {
        self.store.add_block_location(block_id, location)?;
        Ok(())
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        let block_ids = self.store.store.delete_locations(worker_id)?;
        Ok(block_ids)
    }

    pub(crate) fn invalidate_lost_cache_files(
        &mut self,
        block_ids: &[i64],
    ) -> FsResult<CacheInvalidationResult> {
        let inode_ids: HashSet<_> = block_ids.iter().map(|id| InodeId::get_id(*id)).collect();
        let mut result = CacheInvalidationResult::default();
        let mut changed_inodes = Vec::new();

        for inode_id in inode_ids {
            let Some(mut inode) = self.store.get_inode(inode_id, None)? else {
                continue;
            };
            let File(file) = &mut inode else {
                continue;
            };

            // Cache-mode load jobs use the Delete TTL action. Files in fs-mode
            // use Free instead and retain their normal replica-recovery path.
            if !file.storage_policy.both_exists()
                || file.storage_policy.ttl_action != TtlAction::Delete
            {
                continue;
            }

            // `get_locs` purposefully omits empty location lists, so inspect
            // each file block directly to find cache blocks with no replicas.
            let mut cache_unreadable = false;
            for block in &file.blocks {
                if self.store.get_block_locations(block.id)?.is_empty() {
                    cache_unreadable = true;
                    break;
                }
            }
            if !cache_unreadable {
                continue;
            }

            let locations = file.get_locs(&self.store)?;
            result
                .invalidated_block_ids
                .extend(file.blocks.iter().map(|block| block.id));
            if file.invalidate_cache() {
                result.delete_result.blocks.extend(locations);
                changed_inodes.push(inode);
            }
        }

        let journal_inodes = changed_inodes.clone();
        for inode in &changed_inodes {
            self.metadata_reader.invalidate_file_status(inode.id());
        }
        self.store.apply_cache_invalidations(changed_inodes)?;
        self.journal_writer
            .log_cache_invalidations(self, journal_inodes)?;
        Ok(result)
    }

    pub fn get_block_locations(&self, block_id: i64) -> FsResult<Vec<BlockLocation>> {
        Ok(self.store.get_block_locations(block_id)?)
    }

    pub fn reopen_file(
        &self,
        inp: &InodePath,
        client_name: impl AsRef<str>,
    ) -> FsResult<FileStatus> {
        let inode_ptr = match inp.get_last_inode() {
            None => return err_ext!(FsError::file_not_found(inp.path())),
            Some(v) => v,
        };

        let mut inode = match inode_ptr.as_ref() {
            File(..) => inode_ptr.as_ref().clone(),
            Dir(..) => {
                let err_msg = format!("Cannot append to already exists {} directory", inp.path());
                return err_ext!(FsError::file_exists(err_msg));
            }
            FileEntry(..) => {
                return err_box!("FileEntry is not supported");
            }
        };

        let file = inode.as_file_mut()?;
        let _ = file.reopen(client_name);
        let status = inode.to_file_status(inp.path())?;

        self.store.apply_reopen_file(&inode)?;
        self.invalidate_file_status_for_inode(&inode);
        self.journal_writer
            .log_reopen_file(self, inp.path(), inode.as_file_ref()?)?;

        Ok(status)
    }

    /// Overwrite a file by cleaning all blocks and updating metadata.
    /// If file doesn't exist, create a new one.
    /// Returns DeleteResult containing blocks that need to be removed from workers.
    pub fn overwrite_file(&self, inp: &InodePath, opts: CreateFileOpts) -> FsResult<DeleteResult> {
        let op_ms = LocalTime::mills();
        let mut delete_result = DeleteResult::new();

        match inp.get_last_inode() {
            Some(inode) => {
                if !inode.is_file() {
                    return err_box!("Path is not a file: {}", inp.path());
                }

                let file = inode.as_mut().as_file_mut()?;
                for block_meta in &file.blocks {
                    if let Ok(locations) = self.get_block_locations(block_meta.id) {
                        delete_result.blocks.insert(block_meta.id, locations);
                    }
                }
                file.overwrite(opts, op_ms as i64);

                self.store.apply_overwrite_file(inode.as_ref())?;
                self.invalidate_file_status_for_inode(inode.as_ref());
            }
            None => {
                return err_ext!(FsError::file_not_found(inp.path()));
            }
        }

        // Log the operation
        self.journal_writer.log_overwrite_file(self, inp)?;

        Ok(delete_result)
    }

    pub fn print_tree(&self) {
        self.root_dir.print_tree()
    }

    pub fn sum_hash(&self) -> CommonResult<u128> {
        let mut tree_hash = self.root_dir.sum_hash()?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_INODES)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_EDGES)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_LOCATION)?;
        tree_hash += self.store.cf_hash(RocksInodeStore::CF_BLOCK)?;
        Ok(tree_hash)
    }

    pub fn last_inode_id(&self) -> i64 {
        self.inode_id.current()
    }

    pub fn update_last_inode_id(&self, new_value: i64) -> CommonResult<()> {
        let old_value = self.last_inode_id();
        if new_value > old_value {
            self.inode_id.reset(new_value)
        } else {
            Ok(())
        }
    }

    // Read data from rocksdb to build a directory tree
    pub fn create_tree(&self) -> CommonResult<InodeView> {
        self.store.create_tree().map(|x| x.1)
    }

    // Restore in-memory tree from RocksDB without checkpoint (for testing only).
    // In production, use restore() with checkpoint path via Raft snapshot.
    pub fn restore_from_rocksdb(&mut self) -> CommonResult<()> {
        self.store.migrate_directory_attributes()?;
        self.store.initialize_root_directory_attributes()?;
        let (last_inode_id, root_dir) = self.store.create_tree()?;
        self.root_dir = root_dir;
        self.metadata_reader.replace_root(self.root_dir.clone())?;
        self.update_last_inode_id(last_inode_id)?;
        Ok(())
    }

    pub fn create_checkpoint(&self, id: u64) -> CommonResult<String> {
        let directories = self.directories_for_checkpoint();
        let attributes = directories
            .iter()
            .filter(|inode| inode.has_persisted_directory_attributes())
            .filter_map(|inode| {
                inode
                    .directory_attributes()
                    .map(|attributes| (inode.id(), attributes))
            })
            .collect();
        self.store.materialize_directory_attributes(attributes)?;
        for inode in directories
            .into_iter()
            .filter(|inode| inode.has_persisted_directory_attributes())
        {
            inode.mark_directory_attributes_persisted();
        }
        self.store.create_checkpoint(id)
    }

    fn directories_for_checkpoint(&self) -> Vec<InodeView> {
        let mut directories = vec![self.root_dir.clone()];
        let mut result = Vec::new();

        while let Some(inode) = directories.pop() {
            let Dir(directory) = inode else {
                continue;
            };
            for child in directory.children_vec() {
                if child.is_dir() {
                    directories.push(child);
                }
            }
            result.push(InodeView::Dir(directory));
        }

        result
    }

    pub fn restore<T: AsRef<str>>(&mut self, path: T, checkpoint_size: u64) -> CommonResult<()> {
        let mut spend = TimeSpent::new();
        let path = path.as_ref();
        let mut store_guard = self.store_handle.write();

        // Set to other value first to facilitate memory recycling.
        self.root_dir = Self::create_root();

        // Reset rocksdb
        self.store.restore(path)?;
        self.store.migrate_directory_attributes()?;
        self.store.initialize_root_directory_attributes()?;
        let time1 = spend.used_ms();
        spend.reset();

        // Update the directory tree
        let (last_inode_id, root_dir) = self.store.create_tree()?;
        self.root_dir = root_dir;
        self.metadata_reader.replace_root(self.root_dir.clone())?;
        self.update_last_inode_id(last_inode_id)?;
        store_guard.publish(&self.store.store);
        let time2 = spend.used_ms();

        info!(
            "restore from {}, checkpoint_size={} bytes, restore_rocksdb={} ms, \
        build_tree={} ms (see create_tree log for sub-phase breakdown), \
        last_inode_id={}",
            path, checkpoint_size, time1, time2, last_inode_id
        );
        Ok(())
    }

    pub fn get_checkpoint_path(&self, id: u64) -> String {
        self.store.get_checkpoint_path(id)
    }

    pub fn get_file_counts(&self) -> (i64, i64) {
        self.store.get_file_counts()
    }

    pub fn block_report(&self, blocks: Vec<(bool, i64, BlockLocation)>) -> FsResult<()> {
        let mut batch = self.store.new_batch();
        for (add, id, loc) in blocks {
            if add {
                batch.add_location(id, &loc)?;
            } else {
                batch.delete_location(id, loc.worker_id)?;
            }
        }

        batch.commit()?;
        Ok(())
    }

    pub fn get_rocks_store(&self) -> &RocksInodeStore {
        &self.store.store
    }

    pub fn get_worker_block_ids(&self, worker_id: u32) -> FsResult<Vec<i64>> {
        Ok(self.store.store.get_block_ids(worker_id)?)
    }

    // for testing
    pub fn take_entries(&self) -> Vec<JournalEntry> {
        self.journal_writer.take_entries()
    }

    pub fn get_mount_table(&self) -> CommonResult<Vec<MountInfo>> {
        self.store.get_mount_table()
    }

    pub fn get_mount_point(&self, id: u32) -> CommonResult<Option<MountInfo>> {
        self.store.get_mount_point(id)
    }

    pub fn set_attr(&self, inp: InodePath, mut opts: SetAttrOpts) -> FsResult<FileStatus> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        // Internal metadata is master-owned. Persist the operation timestamp in the
        // journal so replay restores it without changing the bincode inode layout.
        opts.add_x_attr.remove(INTERNAL_CTIME_XATTR);
        opts.remove_x_attr.retain(|key| key != INTERNAL_CTIME_XATTR);
        let ctime = LocalTime::mills() as i64;
        opts.add_x_attr.insert(
            INTERNAL_CTIME_XATTR.to_string(),
            ctime.to_le_bytes().to_vec(),
        );
        let target_is_root = inp.len() <= 1;
        let parent_view = if target_is_root {
            None
        } else {
            inp.get_inode(-2)
        };
        self.unprotected_set_attr(parent_view.as_deref(), inode.clone(), opts.clone())?;
        self.journal_writer.log_set_attr(self, &inp, opts)?;
        let inode = self.hydrate_inode(inode)?;
        Ok(inode.to_file_status(inp.path())?)
    }

    pub(crate) fn unprotected_set_attr(
        &self,
        parent: Option<&InodeView>,
        inode: InodePtr,
        opts: SetAttrOpts,
    ) -> FsResult<()> {
        if inode.is_file_entry() {
            return err_box!("set_attr is not supported on unresolved FileEntry inodes; resolve/load the full inode before calling set_attr");
        }

        let mut change_inodes = vec![];
        let mut directory_status_writes = vec![];
        let mut stack = LinkedList::new();
        let target_parent_handle = parent
            .and_then(|view| view.as_dir_ref().ok())
            .map(|dir| dir.children_handle());
        stack.push_back((inode.clone(), target_parent_handle));
        let child_opts = opts.child_opts();
        while let Some((cur_inode, cur_parent)) = stack.pop_front() {
            let cur_inode = self.hydrate_inode(cur_inode)?;
            if !cur_inode.is_file_entry() {
                let set_opts = if cur_inode.id() != inode.id() {
                    child_opts.clone()
                } else {
                    opts.clone()
                };
                if let Dir(directory) = cur_inode.as_ref() {
                    directory_status_writes.push(directory.begin_status_write());
                }
                cur_inode.as_mut().set_attr(set_opts)?;
                // Write the mutated directory view back into its parent
                // container under the shard lock. Path resolution hands out
                // owned clones, so without this write-back the in-tree node
                // keeps stale raw fields (acl/x_attr/ttl) while RocksDB gets
                // the new bytes - the same divergence class the exchange path
                // had. Files stay stubbed in-tree and hydrate from the store,
                // so they need no write-back. The root has no parent container
                // and keeps the legacy behavior.
                if cur_inode.is_dir() {
                    if let Some(parent_children) = cur_parent {
                        let mut mutation = parent_children.begin_mutation(cur_inode.name());
                        if let Err(e) =
                            mutation.replace_child(cur_inode.id(), cur_inode.as_ref().clone())
                        {
                            warn!(
                                "set_attr: skipping tree write-back for inode {}: {}",
                                cur_inode.id(),
                                e
                            );
                        }
                    }
                }
                change_inodes.push(cur_inode.as_ref().clone());
            }

            match cur_inode.as_ref() {
                Dir(dir) if opts.recursive => {
                    let child_handle = dir.children_handle();
                    for child in dir.child_ptrs() {
                        stack.push_back((child, Some(child_handle.clone())));
                    }
                }

                FileEntry(e) => {
                    if let Some(store_inode) = self.store.get_inode(e.id, Some(&e.name))? {
                        stack.push_back((InodePtr::from_owned(store_inode), None));
                    } else {
                        warn!(
                            "unprotected_set_attr: missing inode {} for FileEntry '{}'",
                            e.id, e.name
                        );
                    }
                }

                _ => (),
            }
        }

        self.store.apply_set_attr(change_inodes.clone())?;
        for inode in &change_inodes {
            self.invalidate_file_status_for_inode(inode);
        }
        Ok(())
    }

    pub fn symlink(
        &self,
        target: String,
        link: InodePath,
        force: bool,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        self.symlink_with_attribute_write(
            target,
            link,
            force,
            mode,
            owner,
            group,
            DirectoryAttributeWrite::Merge,
        )
    }

    pub(crate) fn symlink_uncontended(
        &self,
        target: String,
        link: InodePath,
        force: bool,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        self.symlink_with_attribute_write(
            target,
            link,
            force,
            mode,
            owner,
            group,
            DirectoryAttributeWrite::Replace,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn symlink_with_attribute_write(
        &self,
        target: String,
        link: InodePath,
        force: bool,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
        attribute_write: DirectoryAttributeWrite,
    ) -> FsResult<()> {
        let op_ms = LocalTime::mills();
        let new_inode = InodeFile::with_link(
            self.inode_id.next()?,
            op_ms as i64,
            target,
            mode,
            owner,
            group,
        );

        let link = self.unprotected_symlink_with_attribute_write(
            link,
            new_inode.clone(),
            force,
            attribute_write,
        )?;
        self.journal_writer
            .log_symlink(self, link.path(), new_inode, force)?;
        Ok(())
    }

    pub fn unprotected_symlink(
        &self,
        link: InodePath,
        new_inode: InodeFile,
        force: bool,
    ) -> FsResult<InodePath> {
        self.unprotected_symlink_with_attribute_write(
            link,
            new_inode,
            force,
            DirectoryAttributeWrite::Merge,
        )
    }

    fn unprotected_symlink_with_attribute_write(
        &self,
        mut link: InodePath,
        new_inode: InodeFile,
        force: bool,
        attribute_write: DirectoryAttributeWrite,
    ) -> FsResult<InodePath> {
        // check parent
        let parent = match link.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Directory does not exist"),
        };

        let name = link.name().to_string();
        let mut new_inode = InodeView::new_file(name, new_inode);
        new_inode.set_parent_id(parent.id());
        let mut parent_children = parent.as_dir_ref()?.begin_child_mutation(new_inode.name());
        let old_inode = match parent_children.child_view(new_inode.name()) {
            Some(InodeView::FileEntry(entry)) => Some(InodePtr::from_owned(try_option!(
                self.store.get_inode(entry.id(), Some(entry.name()))?,
                "Failed to load inode {} from store",
                entry.id()
            ))),
            Some(inode) => Some(InodePtr::from_owned(inode)),
            None => None,
        };
        if old_inode
            .as_ref()
            .is_some_and(|inode| !inode.is_link() || !force)
        {
            return err_ext!(FsError::file_exists(link.path()));
        }
        let parent_delta = DirectoryAttributeDelta::for_child(new_inode.mtime());
        let parent_attributes =
            Self::directory_attribute_replacement(parent.as_ref(), parent_delta, attribute_write)?;
        self.store.apply_symlink(
            parent.as_ref(),
            &new_inode,
            old_inode.as_ref().map(InodePtr::as_ref),
            parent_attributes,
        )?;
        if let Some(inode) = &old_inode {
            self.invalidate_file_status_for_inode(inode.as_ref());
        }
        parent.apply_directory_attribute_delta(parent_delta)?;
        match old_inode {
            Some(v) => {
                parent_children.replace_child(v.id(), new_inode)?;
            }
            None => {
                let added = parent_children.add_child(new_inode)?;
                link.append(added)?;
            }
        }
        Ok(link)
    }

    // Create a link to an existing file
    pub fn link(&self, src_path: InodePath, dst_path: InodePath) -> FsResult<()> {
        let op_ms = LocalTime::mills();
        // Get the original inode ID and update nlink in memory if it's a direct File
        let (original_inode_id, mut original_inode_ptr) = match src_path.get_last_inode() {
            Some(inode) => match inode.as_ref() {
                File(file) => {
                    // Hard links to regular files and symlinks are valid; directories are not.
                    if !matches!(
                        file.file_type,
                        curvine_model::FileType::File | curvine_model::FileType::Link
                    ) {
                        return err_ext!(FsError::common("Cannot create link to non-regular file"));
                    }
                    (file.id, Some(inode.clone()))
                }
                FileEntry(e) => (e.id, None), // FileEntry already points to an inode
                Dir(_) => return err_ext!(FsError::common("Cannot create link to directory")),
            },
            None => return err_ext!(FsError::file_not_found(src_path.path())),
        };

        // Create the link
        let dst_path_str = dst_path.path().to_string();
        self.unprotected_link(dst_path, original_inode_id, op_ms)?;
        if let Some(ref mut inode_ptr) = original_inode_ptr {
            if let File(_) = inode_ptr.as_mut() {
                inode_ptr.incr_nlink(op_ms as i64)?;
            }
        }

        // Log the operation
        self.journal_writer
            .log_link(self, src_path.path(), &dst_path_str, op_ms as i64)?;

        Ok(())
    }

    pub fn unprotected_link(
        &self,
        mut new_path: InodePath,
        original_inode_id: i64,
        op_ms: u64,
    ) -> FsResult<InodePath> {
        // Create parent directory if needed
        new_path = self.create_parent_dir(
            new_path,
            MkdirOpts::with_create(true),
            DirectoryAttributeWrite::Merge,
        )?;
        // Get the parent directory
        let parent = match new_path.get_inode(-2) {
            Some(v) => v,
            None => return err_box!("Parent directory does not exist"),
        };

        // Create a FileEntry that points to the original inode
        let name = new_path.name().to_string();
        let file_entry = InodeView::new_entry(name.clone(), original_inode_id);

        let mut parent_children = parent.as_dir_ref()?.begin_child_mutation(file_entry.name());
        if parent_children.contains_child(file_entry.name()) {
            return err_ext!(FsError::file_exists(new_path.path()));
        }
        // Apply changes to storage - this creates an edge pointing to the original inode
        self.store.apply_link(
            parent.as_ref(),
            &file_entry,
            original_inode_id,
            op_ms as i64,
        )?;
        parent.apply_directory_attribute_delta(DirectoryAttributeDelta::for_child(op_ms as i64))?;
        self.invalidate_file_status(original_inode_id);
        let added = parent_children.add_child(file_entry)?;
        new_path.append(added)?;
        Ok(new_path)
    }

    /// Resize a file to the specified length.
    ///
    /// This method changes the file size by either extending or truncating it.
    /// - If the new size is larger than the current size, new blocks are allocated.
    /// - If the new size is smaller, blocks beyond the new size are marked for deletion.
    ///
    /// # Arguments
    /// * `inp` - The inode path of the file to resize
    /// * `opts` - File allocation options containing the target length and allocation mode
    ///
    /// # Returns
    /// * `DeleteResult` - Contains blocks that need to be deleted from workers
    ///
    /// # Process
    /// 1. Resize the file metadata (extend or truncate blocks)
    /// 2. Complete the file operation to update metadata state
    /// 3. Collect locations of blocks to be deleted
    /// 4. Persist changes to store and write journal entry
    pub fn resize(&self, inp: &InodePath, opts: FileAllocOpts) -> FsResult<DeleteResult> {
        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };
        let file = inode.as_file_mut()?;

        if file.len == opts.len {
            return Ok(DeleteResult::new());
        }
        let del_blocks = file.resize(opts.clone())?;
        debug!("resize file {} success, opts: {:?}", inp.path(), opts);

        file.complete(file.len, &[], "", true)?;
        let mut del_res = DeleteResult::new();
        for meta in del_blocks {
            let locs = self.get_locations(&meta)?;
            if !locs.is_empty() {
                del_res.blocks.insert(meta.id, locs);
            }
        }

        self.store.apply_complete_file(inode.as_ref(), &[])?;
        self.invalidate_file_status_for_inode(inode.as_ref());
        self.journal_writer
            .log_complete_file(self, inp.path(), inode.as_file_ref()?, vec![])?;

        Ok(del_res)
    }

    pub fn assign_worker(
        &self,
        inp: InodePath,
        block_id: i64,
        workers: &[WorkerAddress],
    ) -> FsResult<ExtendedBlock> {
        let mut inode = try_option!(inp.get_last_inode(), "File {} not exists", inp.path());
        let file = inode.as_file_mut()?;

        let block = file.search_block_mut_check(block_id)?;
        let res = block.assign_worker(workers);
        let block = ExtendedBlock {
            id: block.id,
            len: block.len as i64,
            alloc_opts: block.alloc_opts.clone(),
            storage_type: file.storage_policy.storage_type,
            file_type: file.file_type,
        };

        if res {
            self.store.apply_new_block(inode.as_ref(), &[])?;
            self.invalidate_file_status_for_inode(inode.as_ref());
            self.journal_writer
                .log_add_block(self, inp.path(), inode.as_file_ref()?, vec![])?;
        }

        Ok(block)
    }

    pub fn get_locations(&self, meta: &BlockMeta) -> CommonResult<Vec<BlockLocation>> {
        if let Some(locs) = &meta.locs {
            Ok(locs.clone())
        } else {
            self.store.get_locations(meta.id)
        }
    }

    pub fn get_lock(
        &self,
        inp: InodePath,
        lock: &FileLock,
        expire_ms: u64,
    ) -> FsResult<Option<FileLock>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let mut meta = self.store.get_locks(inode.id())?;
        let conflict = meta.check_conflict(lock, expire_ms);
        Ok(conflict)
    }

    pub fn set_lock(
        &self,
        inp: InodePath,
        lock: FileLock,
        expire_ms: u64,
    ) -> FsResult<Option<FileLock>> {
        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(inp.path())),
        };

        let mut meta = self.store.get_locks(inode.id())?;
        let (conflict, changed) = meta.set_lock_with_change(lock, expire_ms);

        if !changed {
            return Ok(conflict);
        }

        let locks = meta.to_vec();
        self.store.apply_set_locks(inode.id(), &locks)?;
        self.journal_writer.log_set_locks(self, inode.id(), locks)?;

        Ok(conflict)
    }
}
