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

use crate::master::meta::inode::InodeView::{Dir, File, FileEntry};
use crate::master::meta::inode::{InodeDir, InodeView, ROOT_INODE_ID, ROOT_INODE_NAME};
use crate::master::meta::parse_glob_pattern;
use crate::master::meta::store::{RocksInodeStore, RocksInodeStoreSnapshot};
use curvine_core_error::{err_box, try_option, CommonResult};
use curvine_error::{FsError, FsResult};
use curvine_model::{BlockLocation, FileStatus, ListOptions};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};

const RESOLVER_CACHE_LIMIT: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreSubtreeInodeKind {
    File,
    Dir,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreSubtreeInode {
    pub depth: usize,
    pub inode_id: i64,
    pub kind: StoreSubtreeInodeKind,
}

impl StoreSubtreeInode {
    pub fn is_file(&self) -> bool {
        matches!(self.kind, StoreSubtreeInodeKind::File)
    }
}

#[derive(Debug, Default)]
pub struct StoreSubtreeSummary {
    pub inodes: Vec<StoreSubtreeInode>,
    pub block_ids: Vec<i64>,
}

impl StoreSubtreeSummary {
    fn normalize(&mut self) {
        self.inodes
            .sort_by_key(|inode| (inode.depth, inode.inode_id));
        self.inodes
            .dedup_by_key(|inode| (inode.depth, inode.inode_id));
        self.block_ids.sort_unstable();
        self.block_ids.dedup();
    }
}

pub struct StorePathResolver<'a> {
    snapshot: RocksInodeStoreSnapshot<'a>,
    inode_cache: RefCell<HashMap<i64, Option<InodeView>>>,
    edge_cache: RefCell<HashMap<(i64, String), Option<i64>>>,
    location_cache: RefCell<HashMap<i64, Vec<BlockLocation>>>,
}

impl<'a> StorePathResolver<'a> {
    pub fn new(store: &'a RocksInodeStore) -> Self {
        Self {
            snapshot: store.snapshot(),
            inode_cache: RefCell::new(HashMap::new()),
            edge_cache: RefCell::new(HashMap::new()),
            location_cache: RefCell::new(HashMap::new()),
        }
    }

    fn get_inode(&self, inode_id: i64) -> CommonResult<Option<InodeView>> {
        if let Some(inode) = self.inode_cache.borrow().get(&inode_id) {
            return Ok(inode.clone());
        }

        let inode = self.snapshot.get_inode_raw(inode_id)?;
        let mut cache = self.inode_cache.borrow_mut();
        if cache.len() < RESOLVER_CACHE_LIMIT {
            cache.insert(inode_id, inode.clone());
        }
        Ok(inode)
    }

    fn get_child_id(&self, parent_id: i64, child_name: &str) -> CommonResult<Option<i64>> {
        let key = (parent_id, child_name.to_string());
        if let Some(child_id) = self.edge_cache.borrow().get(&key) {
            return Ok(*child_id);
        }

        let child_id = self.snapshot.get_child_id(parent_id, child_name)?;
        let mut cache = self.edge_cache.borrow_mut();
        if cache.len() < RESOLVER_CACHE_LIMIT {
            cache.insert(key, child_id);
        }
        Ok(child_id)
    }

    fn get_locations_cached(&self, block_id: i64) -> CommonResult<Vec<BlockLocation>> {
        if let Some(locations) = self.location_cache.borrow().get(&block_id) {
            return Ok(locations.clone());
        }

        let locations = self.snapshot.get_locations(block_id)?;
        let mut cache = self.location_cache.borrow_mut();
        if cache.len() < RESOLVER_CACHE_LIMIT {
            cache.insert(block_id, locations.clone());
        }
        Ok(locations)
    }

    pub fn resolve<T: AsRef<str>>(&self, path: T) -> CommonResult<StoreResolvedPath> {
        let path = path.as_ref();
        let components = InodeView::path_components(path)?;
        let name = try_option!(components.last(), "Path {} has no components", path);

        if name.is_empty() {
            return err_box!("Path {} is invalid", path);
        }

        let mut inodes = Vec::with_capacity(components.len());
        let mut current = self.root_inode()?;

        let mut index = 0;
        while index < components.len() {
            let resolved = self.resolve_file_entry(current)?;
            let resolved_id = resolved.id();
            inodes.push(resolved.clone());

            if index == components.len() - 1 {
                break;
            }

            index += 1;
            let child_name = components[index].as_str();
            current = match resolved {
                Dir(_) => match self.get_child_id(resolved_id, child_name)? {
                    Some(child_id) => {
                        let mut child = try_option!(
                            self.get_inode(child_id)?,
                            "Edge {}/{} points to missing inode {}",
                            resolved_id,
                            child_name,
                            child_id
                        );
                        child.change_name(child_name.to_string());
                        child
                    }
                    None => break,
                },
                _ => break,
            };
        }

        Ok(StoreResolvedPath {
            path: path.to_string(),
            name: name.to_string(),
            components,
            inodes,
        })
    }

    fn resolve_file_entry(&self, inode: InodeView) -> CommonResult<InodeView> {
        match inode {
            FileEntry(entry) => {
                let mut inode = try_option!(
                    self.get_inode(entry.id())?,
                    "Failed to load inode {} from store",
                    entry.id()
                );
                inode.change_name(entry.name().to_string());
                Ok(inode)
            }
            other => Ok(other),
        }
    }

    fn root_inode(&self) -> CommonResult<InodeView> {
        match self.get_inode(ROOT_INODE_ID)? {
            Some(inode) => Ok(inode),
            None => {
                let inode = Self::default_root_inode();
                if let Some(attributes) = self.snapshot.get_directory_attributes(ROOT_INODE_ID)? {
                    inode.set_directory_attributes(attributes);
                }
                Ok(inode)
            }
        }
    }

    fn default_root_inode() -> InodeView {
        InodeView::new_dir(ROOT_INODE_NAME.to_string(), InodeDir::new(ROOT_INODE_ID, 0))
    }

    pub fn get_locations(&self, block_id: i64) -> CommonResult<Vec<BlockLocation>> {
        self.get_locations_cached(block_id)
    }

    pub fn dir_has_children(&self, inode_id: i64) -> CommonResult<bool> {
        Ok(!self
            .snapshot
            .get_child_ids(inode_id, None, Some(1))?
            .is_empty())
    }

    pub fn collect_block_ids<T: AsRef<str>>(
        &self,
        path: T,
        recursive: bool,
    ) -> CommonResult<Vec<i64>> {
        let resolved = self.resolve(path)?;
        Ok(self
            .collect_resolved_subtree(&resolved, recursive)?
            .block_ids)
    }

    pub fn collect_resolved_subtree(
        &self,
        resolved: &StoreResolvedPath,
        recursive: bool,
    ) -> CommonResult<StoreSubtreeSummary> {
        let mut summary = StoreSubtreeSummary::default();
        if !resolved.is_full() {
            return Ok(summary);
        }
        if let Some(inode) = resolved.target() {
            let depth = resolved.inodes.len().saturating_sub(1);
            self.collect_inode_subtree(inode.clone(), recursive, depth, &mut summary)?;
        }
        summary.normalize();
        Ok(summary)
    }

    fn collect_inode_subtree(
        &self,
        inode: InodeView,
        recursive: bool,
        depth: usize,
        summary: &mut StoreSubtreeSummary,
    ) -> CommonResult<()> {
        match self.resolve_file_entry(inode)? {
            File(file) => {
                summary.inodes.push(StoreSubtreeInode {
                    depth,
                    inode_id: file.id,
                    kind: StoreSubtreeInodeKind::File,
                });
                summary
                    .block_ids
                    .extend(file.blocks.iter().map(|block| block.id));
            }
            Dir(dir) => {
                summary.inodes.push(StoreSubtreeInode {
                    depth,
                    inode_id: dir.id,
                    kind: StoreSubtreeInodeKind::Dir,
                });
                if recursive {
                    for (_, child_id) in self.snapshot.get_child_ids(dir.id, None, None)? {
                        let child = try_option!(
                            self.get_inode(child_id)?,
                            "Directory edge points to missing inode {}",
                            child_id
                        );
                        self.collect_inode_subtree(child, recursive, depth + 1, summary)?;
                    }
                }
            }
            FileEntry(entry) => {
                let inode = try_option!(
                    self.get_inode(entry.id())?,
                    "Failed to load inode {} from store",
                    entry.id()
                );
                self.collect_inode_subtree(inode, recursive, depth, summary)?;
            }
        }
        Ok(())
    }

    pub fn list_status(&self, path: &str) -> FsResult<Vec<FileStatus>> {
        self.list_options(path, &ListOptions::default())
    }

    pub fn list_status_glob(&self, pattern: &str) -> FsResult<Vec<FileStatus>> {
        let paths = self.resolve_glob(pattern)?;
        let mut all_statuses = Vec::new();
        for path in paths {
            all_statuses.extend(self.list_status(&path)?);
        }
        Ok(all_statuses)
    }

    pub fn list_options(&self, path: &str, opts: &ListOptions) -> FsResult<Vec<FileStatus>> {
        let resolved = self.resolve(path)?;
        let inode = match resolved.target() {
            Some(inode) if resolved.is_full() => inode,
            _ => return Err(FsError::file_not_found(path)),
        };

        if inode.is_file() {
            return Ok(Self::list_single_file(inode.to_file_status(path)?, opts));
        }

        if !inode.is_dir() {
            return err_box!("FileEntry is not supported after snapshot path resolve");
        }

        let children =
            self.snapshot
                .get_child_ids(inode.id(), opts.start_after.as_deref(), opts.limit)?;
        let child_ids = children
            .iter()
            .map(|(_, child_id)| *child_id)
            .collect::<Vec<_>>();
        let child_inodes = self.snapshot.get_inodes_raw(child_ids)?;
        let mut statuses = Vec::with_capacity(children.len());
        for ((child_name, child_id), child) in children.into_iter().zip(child_inodes) {
            let mut child = try_option!(
                child,
                "Edge {}/{} points to missing inode {}",
                inode.id(),
                child_name,
                child_id
            );
            child = self.resolve_file_entry(child)?;
            self.snapshot.hydrate_directory_attributes(&mut child)?;
            child.change_name(child_name.clone());
            statuses.push(child.to_file_status(&resolved.child_path(&child_name))?);
        }
        Ok(statuses)
    }

    fn list_single_file(status: FileStatus, opts: &ListOptions) -> Vec<FileStatus> {
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

    fn resolve_glob(&self, pattern: &str) -> CommonResult<Vec<String>> {
        let components = InodeView::path_components(pattern)?;
        if components.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let mut queue = VecDeque::new();
        let root = self.root_inode()?;
        queue.push_back((0usize, root, String::from("/")));

        while let Some((index, inode, path)) = queue.pop_front() {
            if index + 1 == components.len() {
                results.push(path);
                continue;
            }

            let resolved = self.resolve_file_entry(inode)?;
            let Dir(dir) = resolved else {
                continue;
            };

            let child_pattern = &components[index + 1];
            let (is_glob_pattern, glob_pattern) = parse_glob_pattern(child_pattern);
            if is_glob_pattern {
                let glob_pattern = try_option!(
                    glob_pattern,
                    "Glob pattern {} failed to compile",
                    child_pattern
                );
                for (child_name, child_id) in self.snapshot.get_child_ids(dir.id, None, None)? {
                    if !glob_pattern.matches(&child_name) {
                        continue;
                    }
                    let child = try_option!(
                        self.get_inode(child_id)?,
                        "Edge {}/{} points to missing inode {}",
                        dir.id,
                        child_name,
                        child_id
                    );
                    queue.push_back((index + 1, child, Self::join_path(&path, &child_name)));
                }
            } else if let Some(child_id) = self.get_child_id(dir.id, child_pattern)? {
                let child = try_option!(
                    self.get_inode(child_id)?,
                    "Edge {}/{} points to missing inode {}",
                    dir.id,
                    child_pattern,
                    child_id
                );
                queue.push_back((index + 1, child, Self::join_path(&path, child_pattern)));
            }
        }

        Ok(results)
    }

    fn join_path(parent: &str, child_name: &str) -> String {
        if parent == "/" {
            format!("/{}", child_name)
        } else {
            format!("{}/{}", parent, child_name)
        }
    }
}

#[derive(Debug, Clone)]
pub struct StoreResolvedPath {
    path: String,
    name: String,
    pub components: Vec<String>,
    pub inodes: Vec<InodeView>,
}

impl StoreResolvedPath {
    pub fn is_full(&self) -> bool {
        self.components.len() == self.inodes.len()
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn target(&self) -> Option<&InodeView> {
        self.inodes.last()
    }

    pub fn child_path(&self, child_name: &str) -> String {
        if self.path == "/" {
            format!("/{}", child_name)
        } else {
            format!("{}/{}", self.path, child_name)
        }
    }
}
