// Copyright 2026 OPPO.
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

use crate::master::meta::inode::{
    DirectoryChildren, DirectoryReadVersion, InodeView, SequenceState, ROOT_INODE_ID,
};
use curvine_core_error::{err_box, CommonError, CommonResult};
use curvine_error::FsError;
use curvine_model::{BlockLocation, FileStatus, ListOptions};
use fxhash::FxHasher;
use parking_lot::RwLock;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasherDefault, Hasher};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

const PATH_CACHE_LIMIT: usize = 16_384;
const PATH_CACHE_MAX_BYTES: usize = 8 * 1024 * 1024;
const PATH_CACHE_STALE_QUEUE_LIMIT: usize = 1024;
const FILE_STATUS_CACHE_SLOTS: usize = 4096;
const FILE_STATUS_VERSION_SHARDS: usize = 4096;
const FILE_INODE_CACHE_SHARDS: usize = 64;
const ROOT_EPOCH_SPIN_LIMIT: usize = 64;

static NEXT_READER_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static PATH_CACHE: RefCell<PathCache> = RefCell::new(PathCache::default());
    static FILE_STATUS_CACHE: RefCell<FileStatusCache> = RefCell::new(FileStatusCache::new());
}

#[derive(Debug, Clone)]
pub(crate) struct MetadataReplicaPathEntry {
    pub inode_id: i64,
    pub name: Arc<str>,
    pub is_dir: bool,
    directory: Option<Arc<DirectoryChildren>>,
}

#[derive(Debug, Clone)]
struct DirectoryVersion {
    directory: Arc<DirectoryChildren>,
    version: DirectoryReadVersion,
}

#[derive(Clone)]
struct MetadataReplicaRoot {
    inode: InodeView,
    directory: Arc<DirectoryChildren>,
}

#[derive(Clone)]
struct CachedPathEntry {
    inode_id: i64,
    name: Arc<str>,
    is_dir: bool,
    children: Option<Weak<DirectoryChildren>>,
}

impl CachedPathEntry {
    fn from_entry(entry: &MetadataReplicaPathEntry) -> Self {
        Self {
            inode_id: entry.inode_id,
            name: entry.name.clone(),
            is_dir: entry.is_dir,
            children: entry.directory.as_ref().map(Arc::downgrade),
        }
    }

    fn upgrade(&self) -> Option<MetadataReplicaPathEntry> {
        let directory = match &self.children {
            Some(children) => Some(children.upgrade()?),
            None => None,
        };
        Some(MetadataReplicaPathEntry {
            inode_id: self.inode_id,
            name: self.name.clone(),
            is_dir: self.is_dir,
            directory,
        })
    }
}

struct CachedDirectoryVersion {
    directory: Weak<DirectoryChildren>,
    version: DirectoryReadVersion,
}

struct CachedPath {
    path: Arc<str>,
    epoch: u64,
    target: CachedPathEntry,
    versions: Vec<CachedDirectoryVersion>,
}

impl CachedPath {
    fn from_path(path: &str, metadata_path: &MetadataReplicaPath) -> Option<Self> {
        Some(Self {
            path: Arc::from(path),
            epoch: metadata_path.epoch,
            target: CachedPathEntry::from_entry(metadata_path.target()?),
            versions: metadata_path
                .versions
                .iter()
                .map(|version| CachedDirectoryVersion {
                    directory: Arc::downgrade(&version.directory),
                    version: version.version.clone(),
                })
                .collect(),
        })
    }

    fn is_current(&self, reader: &MetadataReplicaReader) -> bool {
        let epoch = reader.epoch.load(Ordering::Acquire);
        epoch.is_multiple_of(2)
            && epoch == self.epoch
            && self.versions.iter().all(|version| {
                version
                    .directory
                    .upgrade()
                    .is_some_and(|directory| directory.version_is(&version.version))
            })
    }
}

#[derive(Default)]
struct PathCache {
    reader_id: u64,
    paths: HashMap<String, Arc<CachedPath>>,
    insertion_order: VecDeque<Weak<CachedPath>>,
    weight: usize,
}

impl PathCache {
    fn reset_if_needed(&mut self, reader_id: u64) {
        if self.reader_id == reader_id {
            return;
        }
        self.reader_id = reader_id;
        self.paths.clear();
        self.insertion_order.clear();
        self.weight = 0;
    }

    fn insert(&mut self, cached_path: Arc<CachedPath>) {
        let path = cached_path.path.as_ref();
        let weight = cached_path_weight(&cached_path);
        if let Some(previous) = self.paths.insert(path.to_string(), cached_path.clone()) {
            self.weight = self.weight.saturating_sub(cached_path_weight(&previous));
        }
        self.weight = self.weight.saturating_add(weight);
        self.insertion_order.push_back(Arc::downgrade(&cached_path));

        while self.paths.len() > PATH_CACHE_LIMIT || self.weight > PATH_CACHE_MAX_BYTES {
            let Some(expected) = self.insertion_order.pop_front() else {
                break;
            };
            if let Some(entry) = expected.upgrade() {
                let path = entry.path.as_ref();
                let is_current = self
                    .paths
                    .get(path)
                    .is_some_and(|current| Arc::ptr_eq(current, &entry));
                if is_current {
                    if let Some(evicted) = self.paths.remove(path) {
                        self.weight = self.weight.saturating_sub(cached_path_weight(&evicted));
                    }
                }
            }
        }
        self.compact_insertion_order();
    }

    fn remove(&mut self, path: &str) {
        if let Some(removed) = self.paths.remove(path) {
            self.weight = self.weight.saturating_sub(cached_path_weight(&removed));
        }
        self.compact_insertion_order();
    }

    fn compact_insertion_order(&mut self) {
        if self.insertion_order.len()
            <= self
                .paths
                .len()
                .saturating_add(PATH_CACHE_STALE_QUEUE_LIMIT)
        {
            return;
        }
        self.insertion_order = self.paths.values().map(Arc::downgrade).collect();
    }
}

fn cached_path_weight(cached_path: &CachedPath) -> usize {
    let path_bytes = cached_path.path.len().saturating_mul(2);
    let target_bytes = cached_path.target.name.len();
    let version_bytes = cached_path.versions.iter().fold(0usize, |total, version| {
        let sequence_bytes =
            match &version.version {
                DirectoryReadVersion::Directory(_) => 0,
                DirectoryReadVersion::Shard { .. } => 0,
                DirectoryReadVersion::Shards(shards) => shards
                    .capacity()
                    .saturating_mul(size_of::<(Arc<SequenceState>, u64)>()),
            };
        total
            .saturating_add(size_of::<CachedDirectoryVersion>())
            .saturating_add(sequence_bytes)
    });
    // This is a conservative admission estimate, not allocator accounting.
    // It includes the map entry, FIFO record, Arc/Weak handles and owned path
    // bytes so the per-thread cap remains below the configured budget in practice.
    size_of::<CachedPath>()
        .saturating_add(size_of::<CachedPathEntry>())
        .saturating_add(size_of::<(String, Arc<CachedPath>)>())
        .saturating_add(size_of::<Weak<CachedPath>>())
        .saturating_add(path_bytes)
        .saturating_add(target_bytes)
        .saturating_add(version_bytes)
}

struct FileStatusCacheEntry {
    inode_id: i64,
    version: u64,
    status: FileStatus,
}

struct FileStatusCache {
    reader_id: u64,
    root_epoch: u64,
    entries: Vec<Option<FileStatusCacheEntry>>,
}

struct CachedFileInode {
    epoch: u64,
    version: u64,
    inode: Arc<InodeView>,
    weight: u32,
}

struct FileInodeCacheShard {
    entries: HashMap<i64, Arc<CachedFileInode>, BuildHasherDefault<FxHasher>>,
    insertion_order: VecDeque<(i64, u64, u64)>,
    weight: u64,
    max_weight: u64,
}

impl FileInodeCacheShard {
    fn new(max_weight: u64) -> Self {
        Self {
            entries: HashMap::with_hasher(BuildHasherDefault::default()),
            insertion_order: VecDeque::new(),
            weight: 0,
            max_weight,
        }
    }

    fn get(&self, inode_id: i64) -> Option<Arc<CachedFileInode>> {
        self.entries.get(&inode_id).cloned()
    }

    fn insert(&mut self, inode_id: i64, entry: Arc<CachedFileInode>) {
        let weight = u64::from(entry.weight);
        if weight > self.max_weight {
            self.remove(inode_id);
            return;
        }

        let insertion = (inode_id, entry.version, entry.epoch);
        match self.entries.insert(inode_id, entry) {
            Some(previous) => {
                self.weight = self.weight.saturating_sub(u64::from(previous.weight));
                self.insertion_order.push_back(insertion);
            }
            None => self.insertion_order.push_back(insertion),
        }
        self.weight = self.weight.saturating_add(weight);

        while self.weight > self.max_weight {
            let Some((evicted_inode_id, version, epoch)) = self.insertion_order.pop_front() else {
                break;
            };
            let matches_current = self
                .entries
                .get(&evicted_inode_id)
                .is_some_and(|entry| entry.version == version && entry.epoch == epoch);
            if matches_current {
                self.remove(evicted_inode_id);
            }
        }
    }

    fn remove(&mut self, inode_id: i64) {
        if let Some(entry) = self.entries.remove(&inode_id) {
            self.weight = self.weight.saturating_sub(u64::from(entry.weight));
        }
        self.compact_insertion_order();
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.insertion_order.clear();
        self.weight = 0;
    }

    fn compact_insertion_order(&mut self) {
        if self.insertion_order.len() <= self.entries.len().saturating_mul(2).saturating_add(1024) {
            return;
        }
        self.insertion_order = self
            .entries
            .iter()
            .map(|(inode_id, entry)| (*inode_id, entry.version, entry.epoch))
            .collect();
    }
}

struct FileInodeCache {
    shards: Box<[RwLock<FileInodeCacheShard>]>,
}

impl FileInodeCache {
    fn new(max_weight: u64) -> Self {
        let shard_weight = max_weight.saturating_add(FILE_INODE_CACHE_SHARDS as u64 - 1)
            / FILE_INODE_CACHE_SHARDS as u64;
        let shards = (0..FILE_INODE_CACHE_SHARDS)
            .map(|_| RwLock::new(FileInodeCacheShard::new(shard_weight)))
            .collect();
        Self { shards }
    }

    fn get(&self, inode_id: i64) -> Option<Arc<CachedFileInode>> {
        self.shards[self.shard_index(inode_id)].read().get(inode_id)
    }

    fn insert(&self, inode_id: i64, entry: Arc<CachedFileInode>) {
        self.shards[self.shard_index(inode_id)]
            .write()
            .insert(inode_id, entry);
    }

    fn invalidate(&self, inode_id: i64) {
        self.shards[self.shard_index(inode_id)]
            .write()
            .remove(inode_id);
    }

    fn invalidate_all(&self) {
        for shard in &self.shards {
            shard.write().clear();
        }
    }

    fn shard_index(&self, inode_id: i64) -> usize {
        file_status_version_shard(inode_id) % self.shards.len()
    }
}

impl FileStatusCache {
    fn new() -> Self {
        Self {
            reader_id: 0,
            root_epoch: 0,
            entries: (0..FILE_STATUS_CACHE_SLOTS).map(|_| None).collect(),
        }
    }

    fn reset_if_needed(&mut self, reader_id: u64, root_epoch: u64) {
        if self.reader_id == reader_id && self.root_epoch == root_epoch {
            return;
        }

        self.reader_id = reader_id;
        self.root_epoch = root_epoch;
        self.entries.iter_mut().for_each(|entry| *entry = None);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MetadataReplicaPath {
    pub entries: Vec<MetadataReplicaPathEntry>,
    epoch: u64,
    versions: Vec<DirectoryVersion>,
}

pub(crate) struct MetadataReplicaDirectoryEntries {
    pub entries: Vec<InodeView>,
    directory: Arc<DirectoryChildren>,
    version: DirectoryReadVersion,
}

impl MetadataReplicaDirectoryEntries {
    pub(crate) fn is_current(&self) -> bool {
        self.directory.version_is(&self.version)
    }
}

pub(crate) enum StablePathRead<T> {
    Ready(T),
    Missing,
    Retry,
}

struct StablePathEdge {
    parent_id: i64,
    directory: Arc<DirectoryChildren>,
    name: String,
    expected_inode_id: Option<i64>,
}

pub(crate) struct SameParentRenamePlan {
    pub src_path: MetadataReplicaPath,
    pub src_component_count: usize,
    pub dst_path: MetadataReplicaPath,
    pub dst_component_count: usize,
}

impl MetadataReplicaPath {
    pub fn is_full(&self, component_count: usize) -> bool {
        self.entries.len() == component_count
    }

    pub fn target(&self) -> Option<&MetadataReplicaPathEntry> {
        self.entries.last()
    }

    fn record_directory(
        &mut self,
        directory: Arc<DirectoryChildren>,
        version: DirectoryReadVersion,
    ) {
        self.versions.push(DirectoryVersion { directory, version });
    }
}

/// A lock-free-at-the-filesystem-level namespace reader.
///
/// This reader owns no directory edges. It walks the same per-directory child
/// index that `FsDir` mutates, so path reads do not take the global `fs_dir`
/// lock and namespace writes do not maintain a second directory tree.
pub(crate) struct MetadataReplicaReader {
    reader_id: u64,
    root: RwLock<MetadataReplicaRoot>,
    epoch: AtomicU64,
    file_status_versions: Box<[AtomicU64]>,
    file_inode_cache: Option<FileInodeCache>,
}

impl MetadataReplicaReader {
    pub(crate) fn new(root: InodeView, file_inode_cache_size: u64) -> CommonResult<Self> {
        let directory = root.as_dir_ref()?.children_handle();
        Ok(Self {
            reader_id: NEXT_READER_ID.fetch_add(1, Ordering::Relaxed),
            root: RwLock::new(MetadataReplicaRoot {
                inode: root,
                directory,
            }),
            epoch: AtomicU64::new(0),
            file_status_versions: (0..FILE_STATUS_VERSION_SHARDS)
                .map(|_| AtomicU64::new(0))
                .collect(),
            file_inode_cache: (file_inode_cache_size > 0)
                .then(|| FileInodeCache::new(file_inode_cache_size)),
        })
    }

    pub(crate) fn replace_root(&self, root: InodeView) -> CommonResult<()> {
        let directory = root.as_dir_ref()?.children_handle();
        let mut current_root = self.root.write();
        self.epoch.fetch_add(1, Ordering::AcqRel);
        if let Some(cache) = &self.file_inode_cache {
            cache.invalidate_all();
        }
        *current_root = MetadataReplicaRoot {
            inode: root,
            directory,
        };
        self.epoch.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub(crate) fn resolve(&self, path: &str) -> CommonResult<(usize, MetadataReplicaPath)> {
        let components = InodeView::path_components(path)?;
        let (root, epoch) = self.root_at_epoch();
        let mut resolved = MetadataReplicaPath {
            entries: vec![MetadataReplicaPathEntry {
                inode_id: ROOT_INODE_ID,
                name: Arc::from(components[0].as_str()),
                is_dir: true,
                directory: Some(root.directory.clone()),
            }],
            epoch,
            versions: vec![],
        };

        let mut directory = root.directory.clone();
        for name in components.iter().skip(1) {
            let (child, version) = directory.read_child(name);
            resolved.record_directory(directory.clone(), version);
            let Some(child) = child else {
                break;
            };
            let next_directory = child.children.clone();
            resolved.entries.push(MetadataReplicaPathEntry {
                inode_id: child.inode_id,
                name: Arc::from(name.as_str()),
                is_dir: child.is_dir,
                directory: next_directory.clone(),
            });
            if resolved.entries.len() == components.len() || !child.is_dir {
                break;
            }
            directory = match next_directory {
                Some(directory) => directory,
                None => return err_box!("directory {} has no child index", child.inode_id),
            };
        }

        Ok((components.len(), resolved))
    }

    /// Resolves a write path and keeps owned inode views for the optimistic
    /// fast path. The caller must validate the returned metadata path after
    /// acquiring inode locks before it uses these views.
    pub(crate) fn resolve_for_write(
        &self,
        path: &str,
    ) -> CommonResult<(usize, MetadataReplicaPath, Vec<InodeView>)> {
        let components = InodeView::path_components(path)?;
        let (root, epoch) = self.root_at_epoch();
        let mut resolved = MetadataReplicaPath {
            entries: vec![MetadataReplicaPathEntry {
                inode_id: ROOT_INODE_ID,
                name: Arc::from(components[0].as_str()),
                is_dir: true,
                directory: Some(root.directory.clone()),
            }],
            epoch,
            versions: vec![],
        };
        let mut views = vec![root.inode];
        let mut directory = root.directory;

        for name in components.iter().skip(1) {
            let (child, version) = directory.read_child_view(name);
            resolved.record_directory(directory.clone(), version);
            let Some(child) = child else {
                break;
            };
            let inode_id = child.id();
            let is_dir = child.is_dir();
            let next_directory = if is_dir {
                Some(child.as_dir_ref()?.children_handle())
            } else {
                None
            };
            views.push(child);
            resolved.entries.push(MetadataReplicaPathEntry {
                inode_id,
                name: Arc::from(name.as_str()),
                is_dir,
                directory: next_directory.clone(),
            });
            if resolved.entries.len() == components.len() || !is_dir {
                break;
            }
            directory = match next_directory {
                Some(directory) => directory,
                None => return err_box!("directory {} has no child index", inode_id),
            };
        }

        Ok((components.len(), resolved, views))
    }

    pub(crate) fn validate(&self, path: &MetadataReplicaPath) -> bool {
        let epoch = self.epoch.load(Ordering::Acquire);
        epoch.is_multiple_of(2)
            && epoch == path.epoch
            && path
                .versions
                .iter()
                .all(|version| version.directory.version_is(&version.version))
    }

    /// Validates the directory edges that lead to a path prefix. The last
    /// child edge is deliberately excluded so same-parent mutations can keep
    /// their per-child concurrency after the parent inode lock is held.
    pub(crate) fn validate_prefix(
        &self,
        path: &MetadataReplicaPath,
        component_count: usize,
    ) -> bool {
        let epoch = self.epoch.load(Ordering::Acquire);
        epoch.is_multiple_of(2)
            && epoch == path.epoch
            && path
                .versions
                .iter()
                .take(component_count.saturating_sub(1))
                .all(|version| version.directory.version_is(&version.version))
    }

    /// Runs a full-path read against a thread-local topology cache.
    ///
    /// The cache retains only topology handles. Regular-file status values use
    /// the bounded immutable cache below; directory status and cache misses
    /// still read the authoritative RocksDB inode. A cached path is valid only
    /// while all directory generations and the root epoch remain unchanged.
    pub(crate) fn with_resolved_path<R>(
        &self,
        path: &str,
        f: impl FnOnce(&MetadataReplicaPathEntry) -> CommonResult<R>,
    ) -> CommonResult<Option<R>> {
        let mut f = Some(f);
        if let Some((cached_path, target)) = self.cached_path(path) {
            let callback = match f.take() {
                Some(callback) => callback,
                None => return err_box!("metadata path callback is unavailable"),
            };
            let result = callback(&target);
            if cached_path.is_current(self) {
                return result.map(Some);
            }
            self.remove_cached_path(path);
            if result
                .as_ref()
                .err()
                .is_some_and(|error| !Self::is_file_not_found(error))
            {
                return result.map(Some);
            }
            return Ok(None);
        }

        let (component_count, resolved) = self.resolve(path)?;
        if !resolved.is_full(component_count) || !self.validate(&resolved) {
            return Ok(None);
        }
        let callback = match f.take() {
            Some(callback) => callback,
            None => return err_box!("metadata path callback is unavailable"),
        };
        let target = match resolved.target() {
            Some(target) => target,
            None => return err_box!("metadata path {} has no target", path),
        };
        let result = callback(target);
        if !self.validate(&resolved) {
            if result
                .as_ref()
                .err()
                .is_some_and(|error| !Self::is_file_not_found(error))
            {
                return result.map(Some);
            }
            return Ok(None);
        }
        let result = result?;
        PATH_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.reset_if_needed(self.reader_id);
            if let Some(cached_path) = CachedPath::from_path(path, &resolved) {
                cache.insert(Arc::new(cached_path));
            }
        });
        Ok(Some(result))
    }

    /// Resolves a path while holding just the directory edge locks required by
    /// that path. This is the progress fallback for a seqlock reader that is
    /// continuously invalidated by writers. It never takes the global FsDir
    /// lock and verifies that the target protected by the caller's inode lock
    /// is still the target reached through these edge locks.
    pub(crate) fn with_stable_path<R>(
        &self,
        path: &str,
        resolved: &MetadataReplicaPath,
        component_count: usize,
        read: impl FnOnce(&MetadataReplicaPathEntry) -> CommonResult<R>,
    ) -> CommonResult<StablePathRead<R>> {
        let components = InodeView::path_components(path)?;
        let mut read = Some(read);
        if components.len() != component_count {
            return Ok(StablePathRead::Retry);
        }
        let Some(edges) = Self::stable_path_edges(&components, resolved, component_count) else {
            return if self.validate(resolved) {
                Ok(StablePathRead::Missing)
            } else {
                Ok(StablePathRead::Retry)
            };
        };

        // Namespace writers lock multiple directory child indexes by inode id.
        // Acquire the same order here rather than path order: a directory may
        // have been moved under a newer directory, where those orders differ.
        let mut order = (0..edges.len()).collect::<Vec<_>>();
        order.sort_unstable_by_key(|index| edges[*index].parent_id);
        let mut guards = Vec::with_capacity(edges.len());
        for index in order {
            let edge = &edges[index];
            guards.push((index, edge.directory.lock_child_read(&edge.name)));
        }

        let mut target = None;
        for (index, edge) in edges.iter().enumerate() {
            let child = guards
                .iter()
                .find(|(guard_index, _)| *guard_index == index)
                .and_then(|(_, guard)| guard.child(&edge.name));
            match (edge.expected_inode_id, child) {
                (None, None) => return Ok(StablePathRead::Missing),
                (None, Some(_)) => return Ok(StablePathRead::Retry),
                // The edge was present during the optimistic resolve but is
                // absent while its directory lock is held. That is a valid
                // linearization point for FileNotFound, not a stale-plan
                // conflict: no writer can add it until this read completes.
                (Some(_), None) => return Ok(StablePathRead::Missing),
                (Some(expected), Some(child)) if expected == child.inode_id => {
                    if index + 1 == edges.len() {
                        target = Some(MetadataReplicaPathEntry {
                            inode_id: child.inode_id,
                            name: Arc::from(edge.name.as_str()),
                            is_dir: child.is_dir,
                            directory: child.children,
                        });
                    }
                }
                // A different inode has replaced the edge. Do not return a
                // status for the previous inode; resolve the new generation.
                (Some(_), Some(_)) => return Ok(StablePathRead::Retry),
            }
        }

        let Some(target) = target.or_else(|| resolved.target().cloned()) else {
            return Ok(StablePathRead::Missing);
        };
        // Keep every path edge guard alive through the callback. The callback
        // performs the final inode/version validation, so releasing these
        // guards first would reopen the path-change race this fallback exists
        // to close.
        let result = Self::finish_stable_path_read(self, resolved.epoch, &target, &mut read);
        drop(guards);
        result
    }

    fn stable_path_edges(
        components: &[String],
        resolved: &MetadataReplicaPath,
        component_count: usize,
    ) -> Option<Vec<StablePathEdge>> {
        let edge_count = if resolved.is_full(component_count) {
            component_count.checked_sub(1)?
        } else {
            resolved.entries.len()
        };
        let mut edges = Vec::with_capacity(edge_count);
        for index in 1..=edge_count {
            let parent = resolved.entries.get(index.checked_sub(1)?)?;
            edges.push(StablePathEdge {
                parent_id: parent.inode_id,
                directory: parent.directory.clone()?,
                name: components.get(index)?.clone(),
                expected_inode_id: resolved.entries.get(index).map(|entry| entry.inode_id),
            });
        }
        Some(edges)
    }

    fn finish_stable_path_read<R, F>(
        reader: &Self,
        epoch: u64,
        target: &MetadataReplicaPathEntry,
        read: &mut Option<F>,
    ) -> CommonResult<StablePathRead<R>>
    where
        F: FnOnce(&MetadataReplicaPathEntry) -> CommonResult<R>,
    {
        if reader.epoch.load(Ordering::Acquire) != epoch || !epoch.is_multiple_of(2) {
            return Ok(StablePathRead::Retry);
        }
        let callback = read
            .take()
            .ok_or_else(|| CommonError::from("metadata path callback is unavailable"))?;
        let value = callback(target)?;
        if reader.epoch.load(Ordering::Acquire) == epoch && epoch.is_multiple_of(2) {
            Ok(StablePathRead::Ready(value))
        } else {
            Ok(StablePathRead::Retry)
        }
    }

    fn is_file_not_found(error: &CommonError) -> bool {
        error
            .downcast_ref::<FsError>()
            .is_some_and(|error| matches!(error, FsError::FileNotFound(_)))
    }

    pub(crate) fn directory_entries(
        &self,
        directory: &MetadataReplicaPathEntry,
        opts: &ListOptions,
    ) -> CommonResult<MetadataReplicaDirectoryEntries> {
        let directory_handle = directory.directory.as_ref().ok_or_else(|| {
            CommonError::from(format!("inode {} is not a directory", directory.inode_id))
        })?;
        let (entries, version) = directory_handle.list_options_snapshot(opts);
        Ok(MetadataReplicaDirectoryEntries {
            entries,
            directory: directory_handle.clone(),
            version,
        })
    }

    pub(crate) fn directory_is_empty(
        &self,
        directory: &MetadataReplicaPathEntry,
    ) -> CommonResult<bool> {
        let directory_handle = directory.directory.as_ref().ok_or_else(|| {
            CommonError::from(format!("inode {} is not a directory", directory.inode_id))
        })?;
        Ok(directory_handle.is_empty())
    }

    pub(crate) fn directory_handle(
        &self,
        directory: &MetadataReplicaPathEntry,
    ) -> CommonResult<Arc<DirectoryChildren>> {
        directory.directory.clone().ok_or_else(|| {
            CommonError::from(format!("inode {} is not a directory", directory.inode_id))
        })
    }

    pub(crate) fn cached_file_status(
        &self,
        inode_id: i64,
        path: &str,
        name: &str,
    ) -> Option<FileStatus> {
        let version = self.file_status_version(inode_id);
        self.cached_file_status_at_version(inode_id, version, path, name)
    }

    pub(crate) fn cached_file_status_at_version(
        &self,
        inode_id: i64,
        expected_version: u64,
        path: &str,
        name: &str,
    ) -> Option<FileStatus> {
        if self.file_status_version(inode_id) != expected_version {
            return None;
        }

        let root_epoch = self.epoch.load(Ordering::Acquire);
        FILE_STATUS_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.reset_if_needed(self.reader_id, root_epoch);
            let entry = cache.entries[file_status_cache_slot(inode_id)].as_ref()?;
            if entry.inode_id != inode_id || entry.version != expected_version {
                return None;
            }
            if self.file_status_version(inode_id) != expected_version {
                return None;
            }

            let mut status = entry.status.clone();
            status.path = path.to_string();
            status.name = name.to_string();
            Some(status)
        })
    }

    pub(crate) fn cache_file_status(&self, inode: &InodeView) -> CommonResult<()> {
        if inode.is_file_entry() {
            return Ok(());
        }

        let root_epoch = self.epoch.load(Ordering::Acquire);
        let inode_id = inode.id();
        let version = self.file_status_version(inode_id);
        let status = inode.to_file_status("")?;
        FILE_STATUS_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.reset_if_needed(self.reader_id, root_epoch);
            cache.entries[file_status_cache_slot(inode_id)] = Some(FileStatusCacheEntry {
                inode_id,
                version,
                status,
            });
        });
        Ok(())
    }

    pub(crate) fn cache_file_status_if_current(
        &self,
        inode: &InodeView,
        expected_version: u64,
    ) -> CommonResult<bool> {
        if inode.is_file_entry() {
            return Ok(true);
        }

        let inode_id = inode.id();
        if self.file_status_version(inode_id) != expected_version {
            return Ok(false);
        }

        let root_epoch = self.epoch.load(Ordering::Acquire);
        let status = inode.to_file_status("")?;
        if self.file_status_version(inode_id) != expected_version {
            return Ok(false);
        }

        Ok(FILE_STATUS_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.reset_if_needed(self.reader_id, root_epoch);
            if self.file_status_version(inode_id) != expected_version {
                return false;
            }
            cache.entries[file_status_cache_slot(inode_id)] = Some(FileStatusCacheEntry {
                inode_id,
                version: expected_version,
                status,
            });
            true
        }))
    }

    pub(crate) fn invalidate_file_status(&self, inode_id: i64) {
        self.file_status_versions[file_status_version_shard(inode_id)]
            .fetch_add(1, Ordering::Release);
        if let Some(cache) = &self.file_inode_cache {
            cache.invalidate(inode_id);
        }
    }

    pub(crate) fn file_status_version(&self, inode_id: i64) -> u64 {
        self.file_status_versions[file_status_version_shard(inode_id)].load(Ordering::Acquire)
    }

    pub(crate) fn cached_file_inode(
        &self,
        inode_id: i64,
        expected_version: u64,
    ) -> Option<Arc<InodeView>> {
        if self.file_status_version(inode_id) != expected_version {
            return None;
        }

        let epoch = self.stable_epoch();
        let entry = self.file_inode_cache.as_ref()?.get(inode_id)?;
        if entry.epoch != epoch || entry.version != expected_version {
            return None;
        }
        if self.epoch.load(Ordering::Acquire) != epoch
            || self.file_status_version(inode_id) != expected_version
        {
            return None;
        }
        Some(entry.inode.clone())
    }

    pub(crate) fn cache_file_inode_if_current(
        &self,
        inode: &InodeView,
        expected_version: u64,
    ) -> bool {
        let Some(cache) = &self.file_inode_cache else {
            return true;
        };
        if !inode.is_file() || inode.is_file_entry() {
            return true;
        }

        let inode_id = inode.id();
        if self.file_status_version(inode_id) != expected_version {
            return false;
        }
        let epoch = self.stable_epoch();
        let entry = Arc::new(CachedFileInode {
            epoch,
            version: expected_version,
            inode: Arc::new(inode.clone()),
            weight: cached_file_inode_weight(inode),
        });
        if self.epoch.load(Ordering::Acquire) != epoch
            || self.file_status_version(inode_id) != expected_version
        {
            return false;
        }
        cache.insert(inode_id, entry);
        self.epoch.load(Ordering::Acquire) == epoch
            && self.file_status_version(inode_id) == expected_version
    }

    pub(crate) fn cache_file_inode(&self, inode: &InodeView) {
        let version = self.file_status_version(inode.id());
        let _ = self.cache_file_inode_if_current(inode, version);
    }

    fn root_at_epoch(&self) -> (MetadataReplicaRoot, u64) {
        loop {
            let epoch = self.wait_for_stable_epoch();
            let root = self.root.read().clone();
            if self.epoch.load(Ordering::Acquire) == epoch {
                return (root, epoch);
            }
        }
    }

    fn stable_epoch(&self) -> u64 {
        self.wait_for_stable_epoch()
    }

    fn wait_for_stable_epoch(&self) -> u64 {
        let mut spins = 0;
        loop {
            let epoch = self.epoch.load(Ordering::Acquire);
            if epoch.is_multiple_of(2) {
                return epoch;
            }
            if spins < ROOT_EPOCH_SPIN_LIMIT {
                spins += 1;
                std::hint::spin_loop();
            } else {
                spins = 0;
                std::thread::yield_now();
            }
        }
    }

    fn cached_path(&self, path: &str) -> Option<(Arc<CachedPath>, MetadataReplicaPathEntry)> {
        PATH_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.reset_if_needed(self.reader_id);

            let cached_path = cache.paths.get(path)?.clone();
            if let Some(target) = cached_path.target.upgrade() {
                return Some((cached_path, target));
            }
            cache.remove(path);
            None
        })
    }

    fn remove_cached_path(&self, path: &str) {
        PATH_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if cache.reader_id == self.reader_id {
                cache.remove(path);
            }
        });
    }
}

fn cached_file_inode_weight(inode: &InodeView) -> u32 {
    let InodeView::File(file) = inode else {
        return 1;
    };

    let mut bytes = size_of::<CachedFileInode>()
        .saturating_add(size_of::<InodeView>())
        .saturating_add(size_of::<crate::master::meta::inode::NamedFile>())
        .saturating_add(size_of::<(i64, Arc<CachedFileInode>)>())
        .saturating_add(size_of::<(i64, u64, u64)>())
        .saturating_add(file.name.capacity())
        .saturating_add(
            file.blocks
                .capacity()
                .saturating_mul(size_of::<crate::master::meta::BlockMeta>()),
        )
        .saturating_add(
            file.features
                .x_attr
                .capacity()
                .saturating_mul(size_of::<(String, Vec<u8>)>().saturating_mul(2)),
        )
        .saturating_add(file.features.acl.owner.capacity())
        .saturating_add(file.features.acl.group.capacity());
    for (key, value) in &file.features.x_attr {
        bytes = bytes
            .saturating_add(key.capacity())
            .saturating_add(value.capacity());
    }
    for block in &file.blocks {
        bytes = bytes.saturating_add(block.locs.as_ref().map_or(0, |locations| {
            locations
                .capacity()
                .saturating_mul(size_of::<BlockLocation>())
        }));
    }
    if let Some(write) = &file.features.file_write {
        bytes = bytes.saturating_add(
            write
                .clients
                .capacity()
                .saturating_mul(size_of::<String>().saturating_mul(2)),
        );
        for client in &write.clients {
            bytes = bytes.saturating_add(client.capacity());
        }
    }
    if let Some(target) = &file.target {
        bytes = bytes.saturating_add(target.capacity());
    }
    // The multiplier leaves room for allocator and hash-table control data
    // that cannot be measured without adding work to the metadata read path.
    u32::try_from(bytes.saturating_mul(2))
        .unwrap_or(u32::MAX)
        .max(1)
}

fn file_status_version_shard(inode_id: i64) -> usize {
    let mut hasher = FxHasher::default();
    hasher.write_i64(inode_id);
    (hasher.finish() as usize) % FILE_STATUS_VERSION_SHARDS
}

fn file_status_cache_slot(inode_id: i64) -> usize {
    let mut hasher = FxHasher::default();
    hasher.write_i64(inode_id);
    (hasher.finish() as usize) % FILE_STATUS_CACHE_SLOTS
}
