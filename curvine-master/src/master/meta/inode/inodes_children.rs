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

use crate::master::meta::inode::inode_dir::DirectoryAttributeState;
use crate::master::meta::inode::{InodePtr, InodeView};
use curvine_core_error::{err_box, CommonError, CommonResult};
use curvine_model::{DirectoryAttributes, ListOptions};
use fxhash::FxHasher;
use glob::Pattern;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::cmp::Reverse;
use std::collections::btree_map::{Entry, Values};
use std::collections::{BTreeMap, BinaryHeap};
use std::hash::{Hash, Hasher};
use std::ops::Bound;
use std::slice::Iter;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::vec;

const MIN_SHARDED_CHILDREN: usize = 256;
#[derive(Debug)]
pub(crate) struct SequenceState {
    value: AtomicU64,
}

impl SequenceState {
    fn begin(&self) {
        self.value.fetch_add(1, Ordering::AcqRel);
    }

    fn finish(&self) {
        self.value.fetch_add(1, Ordering::Release);
    }

    fn version(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    fn is_current(&self, version: u64) -> bool {
        let current = self.value.load(Ordering::Acquire);
        current.is_multiple_of(2) && current == version
    }
}

/// Aggregate directory status is written by independent shards concurrently.
/// It therefore needs an in-flight writer count, unlike per-edge sequences
/// whose writers hold the corresponding child-map write lock.
#[derive(Debug)]
struct StatusSequenceState {
    sequence: AtomicU64,
    writers: AtomicU32,
    child_count: AtomicUsize,
}

impl StatusSequenceState {
    fn begin(&self) {
        // A plain fetch_add instead of a compare-and-swap loop: writers only
        // bump the counter here, u32::MAX simultaneous writers is unreachable
        // (bounded by OS threads), and finish() still rejects underflow. The
        // odd/even sequence bump below publishes the mutation window; attribute
        // stores are ordered by finish()'s Release.
        let previous = self.writers.fetch_add(1, Ordering::Relaxed);
        assert!(
            previous != u32::MAX,
            "directory status writer count overflow"
        );
        if previous == 0 {
            self.sequence.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn finish(&self) {
        let previous = self.writers.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "directory status writer count underflow");
        if previous == 1 {
            self.sequence.fetch_add(1, Ordering::Release);
        }
    }

    fn version(&self) -> u64 {
        self.sequence.load(Ordering::Acquire)
    }

    fn is_current(&self, version: u64) -> bool {
        self.writers.load(Ordering::Acquire) == 0
            && self.sequence.load(Ordering::Acquire) == version
            && version.is_multiple_of(2)
    }
}

struct WriteSequence<'a>(&'a SequenceState);

impl<'a> WriteSequence<'a> {
    fn begin(state: &'a SequenceState) -> Self {
        state.begin();
        Self(state)
    }
}

impl Drop for WriteSequence<'_> {
    fn drop(&mut self) {
        self.0.finish();
    }
}

#[derive(Debug, Clone)]
pub enum InodeChildren {
    List(Vec<Box<InodeView>>),
    Map(BTreeMap<String, Box<InodeView>>),
}

/// The canonical, concurrently readable child index for one directory.
///
/// `FsDir` mutations and metadata reads use this same index.  The version is
/// advanced before a mutation so a path walk can detect a concurrent
/// rename, create, or delete after it releases an ancestor's read lock.
#[derive(Debug)]
pub struct DirectoryChildren {
    attributes: DirectoryAttributeState,
    sequence: SequenceState,
    children: RwLock<InodeChildren>,
    shards: OnceLock<ShardedChildren>,
    status: OnceLock<Arc<StatusSequenceState>>,
}

enum DirectoryMutationGuard<'a> {
    Single(RwLockWriteGuard<'a, InodeChildren>),
    Shard(RwLockWriteGuard<'a, InodeChildren>),
}

pub(crate) struct DirectoryMutation<'a> {
    children: DirectoryMutationGuard<'a>,
    _sequence: WriteSequence<'a>,
    _status: Option<DirectoryStatusWrite>,
}

#[derive(Debug)]
struct ShardedChildren {
    ready: std::sync::atomic::AtomicBool,
    shards: Box<[OnceLock<DirectoryShard>]>,
}

enum DirectoryRenameMutationGuard<'a> {
    Single(RwLockWriteGuard<'a, InodeChildren>),
    Shards {
        first: RwLockWriteGuard<'a, InodeChildren>,
        second: Option<RwLockWriteGuard<'a, InodeChildren>>,
        source_is_first: bool,
    },
}

pub(crate) struct DirectoryRenameMutation<'a> {
    children: DirectoryRenameMutationGuard<'a>,
    _sequences: DirectoryRenameSequences<'a>,
    _status: Option<DirectoryStatusWrite>,
}

enum DirectoryRenameSequences<'a> {
    One {
        _sequence: WriteSequence<'a>,
    },
    Two {
        _first: WriteSequence<'a>,
        _second: WriteSequence<'a>,
    },
}

#[derive(Debug)]
#[repr(align(64))]
struct DirectoryShard {
    children: RwLock<InodeChildren>,
    sequence: Arc<SequenceState>,
}

#[derive(Debug, Clone)]
pub(crate) enum DirectoryReadVersion {
    Directory(u64),
    Shard {
        sequence: Arc<SequenceState>,
        version: u64,
    },
    Shards(Vec<(Arc<SequenceState>, u64)>),
}

pub(crate) struct DirectoryStatusSnapshot {
    pub child_count: usize,
    pub attributes: Option<DirectoryAttributes>,
    source: Arc<StatusSequenceState>,
    version: u64,
}

pub(crate) struct DirectoryStatusWrite {
    source: Arc<StatusSequenceState>,
}

impl DirectoryStatusSnapshot {
    pub(crate) fn is_current(&self) -> bool {
        self.source.is_current(self.version)
    }
}

impl DirectoryStatusWrite {
    fn add_child(&self) {
        self.source.child_count.fetch_add(1, Ordering::Release);
    }

    fn remove_child(&self) {
        self.source.child_count.fetch_sub(1, Ordering::Release);
    }
}

impl Drop for DirectoryStatusWrite {
    fn drop(&mut self) {
        self.source.finish();
    }
}

pub(crate) enum DirectoryReadSnapshot<'a> {
    Single(RwLockReadGuard<'a, InodeChildren>),
    Shards(Vec<RwLockReadGuard<'a, InodeChildren>>),
}

#[derive(Debug, Clone)]
pub(crate) struct DirectoryChild {
    pub inode_id: i64,
    pub is_dir: bool,
    pub children: Option<Arc<DirectoryChildren>>,
}

pub(crate) enum DirectoryChildReadGuard<'a> {
    Single(RwLockReadGuard<'a, InodeChildren>),
    Shard(RwLockReadGuard<'a, InodeChildren>),
}

impl DirectoryChildReadGuard<'_> {
    pub(crate) fn child(&self, name: &str) -> Option<DirectoryChild> {
        match self {
            Self::Single(children) => children
                .get_child(name)
                .map(DirectoryChildren::directory_child),
            Self::Shard(children) => children
                .get_child(name)
                .map(DirectoryChildren::directory_child),
        }
    }
}

impl Default for DirectoryChildren {
    fn default() -> Self {
        Self {
            attributes: DirectoryAttributeState::default(),
            sequence: SequenceState {
                value: AtomicU64::new(0),
            },
            children: RwLock::new(InodeChildren::new_map()),
            shards: OnceLock::new(),
            status: OnceLock::new(),
        }
    }
}

impl DirectoryChildren {
    pub(crate) fn with_attributes(attributes: DirectoryAttributes) -> Self {
        Self {
            attributes: DirectoryAttributeState::new(attributes),
            ..Default::default()
        }
    }

    pub(crate) fn attribute_state(&self) -> &DirectoryAttributeState {
        &self.attributes
    }

    pub fn add_child(&self, inode: InodeView) -> CommonResult<InodePtr> {
        let name = inode.name().to_string();
        self.begin_mutation(&name).add_child(inode)
    }

    pub fn delete_child(&self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        self.begin_mutation(child_name)
            .delete_child(child_id, child_name)
    }

    pub(crate) fn begin_mutation(&self, child_name: &str) -> DirectoryMutation<'_> {
        if let Some(shards) = self.sharded() {
            return self.begin_shard_mutation(shards, child_name);
        }

        let (mut children, contended) = self.lock_children_for_mutation();
        if self.sharded().is_some() {
            drop(children);
            return self.begin_mutation(child_name);
        }

        if self.should_promote_to_shards(children.len(), contended) {
            self.promote_to_shards(&mut children);
            drop(children);
            return self.begin_mutation(child_name);
        }

        DirectoryMutation {
            children: DirectoryMutationGuard::Single(children),
            _sequence: WriteSequence::begin(&self.sequence),
            _status: self.begin_mutation_status_write(),
        }
    }

    pub fn child_ptr(&self, name: &str) -> Option<InodePtr> {
        self.child_view(name).map(InodePtr::from_owned)
    }

    /// Returns a borrowed child pointer while the caller exclusively owns the
    /// namespace tree. Concurrent namespace readers may run, but no writer may
    /// replace or remove this child until that tree lock is released.
    pub(crate) fn child_ptr_exclusive(&self, name: &str) -> Option<InodePtr> {
        self.with_children_for_name(name, |children, name| {
            children.get_child(name).map(InodePtr::from_ref)
        })
    }

    pub fn child_view(&self, name: &str) -> Option<InodeView> {
        self.with_children_for_name(name, |children, name| children.get_child(name).cloned())
    }

    pub fn child_ptrs_by_glob_pattern(&self, glob_pattern: &Pattern) -> Option<Vec<InodePtr>> {
        self.read_snapshot()
            .child_ptrs_by_glob_pattern(glob_pattern)
    }

    pub fn child_ptrs(&self) -> Vec<InodePtr> {
        self.read_snapshot().child_ptrs()
    }

    pub(crate) fn read_child(&self, name: &str) -> (Option<DirectoryChild>, DirectoryReadVersion) {
        if let Some(shards) = self.sharded() {
            let shard = shards.shard(name);
            let children = shard.children.read();
            let child = children.get_child(name).map(Self::directory_child);
            let version = DirectoryReadVersion::Shard {
                sequence: shard.sequence.clone(),
                version: shard.sequence.version(),
            };
            return (child, version);
        }

        let children = self.children.read();
        if self.sharded().is_some() {
            drop(children);
            return self.read_child(name);
        }
        let child = children.get_child(name).map(Self::directory_child);
        (
            child,
            DirectoryReadVersion::Directory(self.sequence.version()),
        )
    }

    /// Returns an owned child view for a write-path optimistic read.
    ///
    /// Readers that only need topology use `read_child` to avoid cloning inode
    /// metadata. Write planning needs the owned view so it can reuse the
    /// versioned traversal after its inode locks have been acquired.
    pub(crate) fn read_child_view(&self, name: &str) -> (Option<InodeView>, DirectoryReadVersion) {
        if let Some(shards) = self.sharded() {
            let shard = shards.shard(name);
            let children = shard.children.read();
            let child = children.get_child(name).cloned();
            let version = DirectoryReadVersion::Shard {
                sequence: shard.sequence.clone(),
                version: shard.sequence.version(),
            };
            return (child, version);
        }

        let children = self.children.read();
        if self.sharded().is_some() {
            drop(children);
            return self.read_child_view(name);
        }
        let child = children.get_child(name).cloned();
        (
            child,
            DirectoryReadVersion::Directory(self.sequence.version()),
        )
    }

    /// Holds the lock for one child edge. This is the pessimistic progress
    /// fallback after a versioned read is continuously invalidated by writers.
    pub(crate) fn lock_child_read(&self, name: &str) -> DirectoryChildReadGuard<'_> {
        loop {
            if let Some(shards) = self.sharded() {
                let shard = shards.shard(name);
                let children = shard.children.read();
                return DirectoryChildReadGuard::Shard(children);
            }

            let children = self.children.read();
            if self.sharded().is_none() {
                return DirectoryChildReadGuard::Single(children);
            }
            drop(children);
        }
    }

    pub(crate) fn version_is(&self, version: &DirectoryReadVersion) -> bool {
        match version {
            DirectoryReadVersion::Directory(version) => {
                self.sharded().is_none() && self.sequence.is_current(*version)
            }
            DirectoryReadVersion::Shard { sequence, version } => sequence.is_current(*version),
            DirectoryReadVersion::Shards(versions) => versions
                .iter()
                .all(|(sequence, version)| sequence.is_current(*version)),
        }
    }

    pub fn children_vec(&self) -> Vec<InodeView> {
        self.read_snapshot().children_vec()
    }

    pub fn len(&self) -> usize {
        self.read_snapshot().len()
    }

    pub(crate) fn status_snapshot(self: &Arc<Self>) -> Option<DirectoryStatusSnapshot> {
        self.status_snapshot_for(self.ensure_status_source())
    }

    /// Runs a status read while preventing child-edge mutations in this
    /// directory. Used only after an optimistic status read exhausts its retry
    /// budget, so normal sharded writes remain independent.
    pub(crate) fn with_status_read<R>(
        self: &Arc<Self>,
        read: impl FnOnce(&DirectoryStatusSnapshot) -> R,
    ) -> R {
        let source = self.ensure_status_source();
        loop {
            if let Some(shards) = self.sharded() {
                let _children = shards.read_all();
                if let Some(snapshot) = self.status_snapshot_for(source.clone()) {
                    return read(&snapshot);
                }
                continue;
            }

            let children = self.children.read();
            if self.sharded().is_some() {
                drop(children);
                continue;
            }
            if let Some(snapshot) = self.status_snapshot_for(source.clone()) {
                return read(&snapshot);
            }
        }
    }

    fn status_source(&self, child_count: usize) -> Arc<StatusSequenceState> {
        self.status
            .get_or_init(|| {
                Arc::new(StatusSequenceState {
                    sequence: AtomicU64::new(0),
                    writers: AtomicU32::new(0),
                    child_count: AtomicUsize::new(child_count),
                })
            })
            .clone()
    }

    /// Enables aggregate directory-status tracking only after a status reader
    /// needs it. Holding every child map read lock establishes a stable base:
    /// a namespace mutation either finishes before tracking begins or observes
    /// the initialized sidecar after it acquires its child write lock.
    fn ensure_status_source(&self) -> Arc<StatusSequenceState> {
        if let Some(source) = self.status.get() {
            return source.clone();
        }

        loop {
            if let Some(shards) = self.sharded() {
                let children = shards.read_all();
                let child_count = children.iter().map(|children| children.len()).sum();
                return self.status_source(child_count);
            }

            let children = self.children.read();
            if self.sharded().is_none() {
                return self.status_source(children.len());
            }
            drop(children);
        }
    }

    fn status_snapshot_for(
        self: &Arc<Self>,
        source: Arc<StatusSequenceState>,
    ) -> Option<DirectoryStatusSnapshot> {
        let version = source.version();
        let child_count = source.child_count.load(Ordering::Acquire);
        let attributes = self.attribute_state().current();
        source
            .is_current(version)
            .then_some(DirectoryStatusSnapshot {
                child_count,
                attributes,
                source,
                version,
            })
    }

    pub(crate) fn begin_status_write(&self) -> DirectoryStatusWrite {
        let source = self.ensure_status_source();
        source.begin();
        DirectoryStatusWrite { source }
    }

    fn begin_mutation_status_write(&self) -> Option<DirectoryStatusWrite> {
        let source = self.status.get()?.clone();
        source.begin();
        Some(DirectoryStatusWrite { source })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn list_options(&self, opts: &ListOptions) -> Vec<InodeView> {
        self.list_options_snapshot(opts).0
    }

    pub(crate) fn list_options_snapshot(
        &self,
        opts: &ListOptions,
    ) -> (Vec<InodeView>, DirectoryReadVersion) {
        loop {
            if let Some(shards) = self.sharded() {
                if let Some(limit) = opts.limit {
                    let mut candidates = Vec::with_capacity(shards.shards.len());
                    let mut versions = Vec::with_capacity(shards.shards.len());
                    for index in 0..shards.shards.len() {
                        let shard = shards.shard_at(index);
                        let children = shard.children.read();
                        candidates.push(
                            (limit != 0)
                                .then(|| children.next_after(opts.start_after.as_deref()))
                                .flatten(),
                        );
                        versions.push((shard.sequence.clone(), shard.sequence.version()));
                    }
                    return (
                        merge_sharded_page(shards, candidates, limit),
                        DirectoryReadVersion::Shards(versions),
                    );
                }

                let mut entries = Vec::with_capacity(shards.shards.len());
                let mut versions = Vec::with_capacity(shards.shards.len());
                for index in 0..shards.shards.len() {
                    let shard = shards.shard_at(index);
                    let children = shard.children.read();
                    entries.push(children.list_options(opts).into_iter().cloned().collect());
                    versions.push((shard.sequence.clone(), shard.sequence.version()));
                }
                return (
                    merge_sharded_options(entries),
                    DirectoryReadVersion::Shards(versions),
                );
            }

            let children = self.children.read();
            if self.sharded().is_none() {
                let entries = children.list_options(opts).into_iter().cloned().collect();
                return (
                    entries,
                    DirectoryReadVersion::Directory(self.sequence.version()),
                );
            }
            drop(children);
        }
    }

    fn begin_shard_mutation<'a>(
        &'a self,
        shards: &'a ShardedChildren,
        child_name: &str,
    ) -> DirectoryMutation<'a> {
        let shard = shards.shard(child_name);
        let children = shard.children.write();
        DirectoryMutation {
            children: DirectoryMutationGuard::Shard(children),
            _sequence: WriteSequence::begin(shard.sequence.as_ref()),
            _status: self.begin_mutation_status_write(),
        }
    }

    pub(crate) fn begin_rename_mutation<'a>(
        &'a self,
        source_name: &str,
        destination_name: &str,
    ) -> DirectoryRenameMutation<'a> {
        if let Some(shards) = self.sharded() {
            let source_index = shards.shard_index(source_name);
            let destination_index = shards.shard_index(destination_name);
            let source = shards.shard_at(source_index);
            let destination = shards.shard_at(destination_index);
            if source_index == destination_index {
                let first = source.children.write();
                return DirectoryRenameMutation {
                    children: DirectoryRenameMutationGuard::Shards {
                        first,
                        second: None,
                        source_is_first: true,
                    },
                    _sequences: DirectoryRenameSequences::One {
                        _sequence: WriteSequence::begin(source.sequence.as_ref()),
                    },
                    _status: self.begin_mutation_status_write(),
                };
            }
            if source_index < destination_index {
                let first = source.children.write();
                let second = destination.children.write();
                return DirectoryRenameMutation {
                    children: DirectoryRenameMutationGuard::Shards {
                        first,
                        second: Some(second),
                        source_is_first: true,
                    },
                    _sequences: DirectoryRenameSequences::Two {
                        _first: WriteSequence::begin(source.sequence.as_ref()),
                        _second: WriteSequence::begin(destination.sequence.as_ref()),
                    },
                    _status: self.begin_mutation_status_write(),
                };
            }
            let first = destination.children.write();
            let second = source.children.write();
            return DirectoryRenameMutation {
                children: DirectoryRenameMutationGuard::Shards {
                    first,
                    second: Some(second),
                    source_is_first: false,
                },
                _sequences: DirectoryRenameSequences::Two {
                    _first: WriteSequence::begin(destination.sequence.as_ref()),
                    _second: WriteSequence::begin(source.sequence.as_ref()),
                },
                _status: self.begin_mutation_status_write(),
            };
        }

        let (mut children, contended) = self.lock_children_for_mutation();
        if self.sharded().is_some() {
            drop(children);
            return self.begin_rename_mutation(source_name, destination_name);
        }
        if self.should_promote_to_shards(children.len(), contended) {
            self.promote_to_shards(&mut children);
            drop(children);
            return self.begin_rename_mutation(source_name, destination_name);
        }
        DirectoryRenameMutation {
            children: DirectoryRenameMutationGuard::Single(children),
            _sequences: DirectoryRenameSequences::One {
                _sequence: WriteSequence::begin(&self.sequence),
            },
            _status: self.begin_mutation_status_write(),
        }
    }

    fn with_children_for_name<R>(&self, name: &str, f: impl Fn(&InodeChildren, &str) -> R) -> R {
        if let Some(shards) = self.sharded() {
            let shard = shards.shard(name);
            let children = shard.children.read();
            return f(&children, name);
        }
        let children = self.children.read();
        if self.sharded().is_some() {
            drop(children);
            return self.with_children_for_name(name, f);
        }
        f(&children, name)
    }

    fn lock_children_for_mutation(&self) -> (RwLockWriteGuard<'_, InodeChildren>, bool) {
        match self.children.try_write() {
            Some(children) => (children, false),
            None => (self.children.write(), true),
        }
    }

    fn should_promote_to_shards(&self, child_count: usize, contended: bool) -> bool {
        contended && child_count >= min_sharded_children()
    }

    fn promote_to_shards(&self, children: &mut InodeChildren) {
        // Keep the compact single-map representation until writers actually
        // contend. Once contention is observed, promotion is permanent and
        // reuses the existing sharded reader and mutation protocol.
        let promotion = WriteSequence::begin(&self.sequence);
        let sharded = self.shards.get_or_init(ShardedChildren::new);
        sharded.insert_children(std::mem::take(children));
        sharded.ready.store(true, Ordering::Release);
        drop(promotion);
    }

    fn sharded(&self) -> Option<&ShardedChildren> {
        self.shards
            .get()
            .filter(|shards| shards.ready.load(Ordering::Acquire))
    }

    /// Pre-shards the index when a bulk load (restore/replay tree build) knows
    /// the final child count up front. Large restored directories then never
    /// take the O(children) `promote_to_shards` migration on their first
    /// contended mutation. Runtime-grown directories still promote lazily once
    /// contention appears, at which point the directory has just crossed the
    /// size threshold and the one-off migration stays bounded.
    ///
    /// Restore calls this while namespace mutations are quiesced, but it uses
    /// the same transition protocol as `promote_to_shards`: late readers and
    /// writers re-check `sharded()` after taking each fallback lock.
    pub(crate) fn seed_shards_for_bulk_load(&self, expected_children: usize) {
        if expected_children < min_sharded_children() || self.sharded().is_some() {
            return;
        }
        let sharded = self.shards.get_or_init(ShardedChildren::new);
        sharded.ready.store(true, Ordering::Release);
    }

    /// Test/introspection probe: whether this index currently uses shards.
    #[cfg(test)]
    pub(crate) fn is_sharded(&self) -> bool {
        self.sharded().is_some()
    }

    pub(crate) fn read_snapshot(&self) -> DirectoryReadSnapshot<'_> {
        loop {
            if let Some(shards) = self.sharded() {
                return DirectoryReadSnapshot::Shards(shards.read_all());
            }
            let children = self.children.read();
            if self.sharded().is_none() {
                return DirectoryReadSnapshot::Single(children);
            }
            drop(children);
        }
    }

    fn directory_child(inode: &InodeView) -> DirectoryChild {
        let children = match inode {
            InodeView::Dir(dir) => Some(dir.children_handle()),
            _ => None,
        };
        DirectoryChild {
            inode_id: inode.id(),
            is_dir: inode.is_dir(),
            children,
        }
    }
}

impl DirectoryMutation<'_> {
    pub fn contains_child(&self, child_name: &str) -> bool {
        match &self.children {
            DirectoryMutationGuard::Single(children) => children.get_child(child_name).is_some(),
            DirectoryMutationGuard::Shard(children) => children.get_child(child_name).is_some(),
        }
    }

    pub(crate) fn child_view(&self, child_name: &str) -> Option<InodeView> {
        match &self.children {
            DirectoryMutationGuard::Single(children) => children.get_child(child_name).cloned(),
            DirectoryMutationGuard::Shard(children) => children.get_child(child_name).cloned(),
        }
    }

    pub fn add_child(&mut self, inode: InodeView) -> CommonResult<InodePtr> {
        let result = match &mut self.children {
            DirectoryMutationGuard::Single(children) => children.add_child(inode),
            DirectoryMutationGuard::Shard(children) => children.add_child(inode),
        };
        if result.is_ok() {
            if let Some(status) = &self._status {
                status.add_child();
            }
        }
        result
    }

    pub fn replace_child(&mut self, child_id: i64, inode: InodeView) -> CommonResult<()> {
        match &mut self.children {
            DirectoryMutationGuard::Single(children) => children.replace_child(child_id, inode),
            DirectoryMutationGuard::Shard(children) => children.replace_child(child_id, inode),
        }
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        let result = match &mut self.children {
            DirectoryMutationGuard::Single(children) => children.delete_child(child_id, child_name),
            DirectoryMutationGuard::Shard(children) => children.delete_child(child_id, child_name),
        };
        if result.is_ok() {
            if let Some(status) = &self._status {
                status.remove_child();
            }
        }
        result
    }
}

impl DirectoryRenameMutation<'_> {
    pub(crate) fn child_view(&self, name: &str) -> Option<InodeView> {
        match &self.children {
            DirectoryRenameMutationGuard::Single(children) => children.get_child(name).cloned(),
            DirectoryRenameMutationGuard::Shards { first, second, .. } => {
                first.get_child(name).cloned().or_else(|| {
                    second
                        .as_ref()
                        .and_then(|children| children.get_child(name).cloned())
                })
            }
        }
    }

    pub fn rename_child(
        &mut self,
        source_id: i64,
        source_name: &str,
        replaced: Option<(i64, &str)>,
        destination: InodeView,
    ) -> CommonResult<()> {
        match &mut self.children {
            DirectoryRenameMutationGuard::Single(children) => {
                if let Some((replaced_id, replaced_name)) = replaced {
                    let _ = children.delete_child(replaced_id, replaced_name)?;
                }
                let _ = children.delete_child(source_id, source_name)?;
                let _ = children.add_child(destination)?;
            }
            DirectoryRenameMutationGuard::Shards {
                first,
                second,
                source_is_first,
            } => match second {
                None => {
                    if let Some((replaced_id, replaced_name)) = replaced {
                        let _ = first.delete_child(replaced_id, replaced_name)?;
                    }
                    let _ = first.delete_child(source_id, source_name)?;
                    let _ = first.add_child(destination)?;
                }
                Some(second) if *source_is_first => {
                    if let Some((replaced_id, replaced_name)) = replaced {
                        let _ = second.delete_child(replaced_id, replaced_name)?;
                    }
                    let _ = first.delete_child(source_id, source_name)?;
                    let _ = second.add_child(destination)?;
                }
                Some(second) => {
                    if let Some((replaced_id, replaced_name)) = replaced {
                        let _ = first.delete_child(replaced_id, replaced_name)?;
                    }
                    let _ = second.delete_child(source_id, source_name)?;
                    let _ = first.add_child(destination)?;
                }
            },
        }
        if replaced.is_some() {
            if let Some(status) = &self._status {
                status.remove_child();
            }
        }
        Ok(())
    }

    pub fn exchange_children(
        &mut self,
        source_id: i64,
        source_name: &str,
        destination_id: i64,
        destination_name: &str,
        at_source: InodeView,
        at_destination: InodeView,
    ) -> CommonResult<()> {
        match &mut self.children {
            DirectoryRenameMutationGuard::Single(children) => {
                let _ = children.delete_child(source_id, source_name)?;
                let _ = children.delete_child(destination_id, destination_name)?;
                let _ = children.add_child(at_source)?;
                let _ = children.add_child(at_destination)?;
            }
            DirectoryRenameMutationGuard::Shards {
                first,
                second,
                source_is_first,
            } => match second {
                None => {
                    let _ = first.delete_child(source_id, source_name)?;
                    let _ = first.delete_child(destination_id, destination_name)?;
                    let _ = first.add_child(at_source)?;
                    let _ = first.add_child(at_destination)?;
                }
                Some(second) if *source_is_first => {
                    let _ = first.delete_child(source_id, source_name)?;
                    let _ = second.delete_child(destination_id, destination_name)?;
                    let _ = first.add_child(at_source)?;
                    let _ = second.add_child(at_destination)?;
                }
                Some(second) => {
                    let _ = second.delete_child(source_id, source_name)?;
                    let _ = first.delete_child(destination_id, destination_name)?;
                    let _ = second.add_child(at_source)?;
                    let _ = first.add_child(at_destination)?;
                }
            },
        }
        Ok(())
    }
}

impl ShardedChildren {
    fn new() -> Self {
        let shard_count = shard_count();
        let shards = (0..shard_count)
            .map(|_| OnceLock::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            ready: std::sync::atomic::AtomicBool::new(false),
            shards,
        }
    }

    fn insert_children(&self, children: InodeChildren) {
        for child in children.into_map().into_values() {
            let shard = self.shard(child.name());
            let mut shard_children = shard.children.write();
            shard_children.insert_child_boxed(child);
        }
    }

    fn shard(&self, name: &str) -> &DirectoryShard {
        self.shard_at(self.shard_index(name))
    }

    fn shard_index(&self, name: &str) -> usize {
        shard_index(name, self.shards.len())
    }

    fn shard_at(&self, index: usize) -> &DirectoryShard {
        self.shards[index].get_or_init(|| DirectoryShard {
            children: RwLock::new(InodeChildren::new_map()),
            sequence: Arc::new(SequenceState {
                value: AtomicU64::new(0),
            }),
        })
    }

    fn read_all(&self) -> Vec<RwLockReadGuard<'_, InodeChildren>> {
        let mut children = Vec::with_capacity(self.shards.len());
        for index in 0..self.shards.len() {
            let shard = self.shard_at(index);
            children.push(shard.children.read());
        }
        children
    }
}

impl DirectoryReadSnapshot<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Single(children) => children.len(),
            Self::Shards(children) => children.iter().map(|children| children.len()).sum(),
        }
    }

    pub(crate) fn children_vec(&self) -> Vec<InodeView> {
        match self {
            Self::Single(children) => children.iter().cloned().collect(),
            Self::Shards(children) => {
                let mut result = children
                    .iter()
                    .flat_map(|children| children.iter().cloned())
                    .collect::<Vec<_>>();
                result.sort_unstable_by(|left, right| left.name().cmp(right.name()));
                result
            }
        }
    }

    pub(crate) fn child_ptrs_by_glob_pattern(
        &self,
        glob_pattern: &Pattern,
    ) -> Option<Vec<InodePtr>> {
        match self {
            Self::Single(children) => children.get_child_ptr_by_glob_pattern(glob_pattern),
            Self::Shards(children) => {
                let mut result = children
                    .iter()
                    .flat_map(|children| {
                        children
                            .get_child_ptr_by_glob_pattern(glob_pattern)
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>();
                result.sort_unstable_by(|left, right| left.name().cmp(right.name()));
                Some(result)
            }
        }
    }

    pub(crate) fn child_ptrs(&self) -> Vec<InodePtr> {
        match self {
            Self::Single(children) => children.iter().cloned().map(InodePtr::from_owned).collect(),
            Self::Shards(children) => children
                .iter()
                .flat_map(|children| children.iter().cloned().map(InodePtr::from_owned))
                .collect(),
        }
    }
}

fn merge_sharded_options(shards: Vec<Vec<InodeView>>) -> Vec<InodeView> {
    let total_len = shards
        .iter()
        .map(Vec::len)
        .fold(0usize, usize::saturating_add);
    let mut iterators = shards.into_iter().map(Vec::into_iter).collect::<Vec<_>>();
    let mut candidates = Vec::with_capacity(iterators.len());
    let mut heap = BinaryHeap::new();
    for (shard, iterator) in iterators.iter_mut().enumerate() {
        let child = iterator.next();
        if let Some(child) = &child {
            heap.push(Reverse((child.name().to_string(), shard)));
        }
        candidates.push(child);
    }

    let mut result = Vec::with_capacity(total_len);
    while let Some(Reverse((_, shard))) = heap.pop() {
        let child = match candidates[shard].take() {
            Some(child) => child,
            None => continue,
        };
        result.push(child);
        let next = iterators[shard].next();
        if let Some(child) = &next {
            heap.push(Reverse((child.name().to_string(), shard)));
        }
        candidates[shard] = next;
    }
    result
}

fn merge_sharded_page(
    shards: &ShardedChildren,
    mut candidates: Vec<Option<InodeView>>,
    limit: usize,
) -> Vec<InodeView> {
    let mut heap = BinaryHeap::new();
    for (shard, child) in candidates.iter().enumerate() {
        if let Some(child) = child {
            heap.push(Reverse((child.name().to_string(), shard)));
        }
    }

    let mut result = Vec::with_capacity(limit);
    while let Some(Reverse((_, shard_index))) = heap.pop() {
        let child = match candidates[shard_index].take() {
            Some(child) => child,
            None => continue,
        };
        let child_name = child.name().to_string();
        result.push(child);
        if result.len() == limit {
            break;
        }

        let shard = shards.shard_at(shard_index);
        let children = shard.children.read();
        let next = children.next_after(Some(&child_name));
        if let Some(child) = &next {
            heap.push(Reverse((child.name().to_string(), shard_index)));
        }
        candidates[shard_index] = next;
    }
    result
}

fn shard_count() -> usize {
    static SHARD_COUNT: OnceLock<usize> = OnceLock::new();
    *SHARD_COUNT.get_or_init(|| {
        std::thread::available_parallelism()
            .map(|count| count.get().saturating_mul(2))
            .unwrap_or(8)
            .next_power_of_two()
            .clamp(8, 64)
    })
}

pub(crate) fn min_sharded_children() -> usize {
    static SHARD_CHILDREN_THRESHOLD: OnceLock<usize> = OnceLock::new();
    *SHARD_CHILDREN_THRESHOLD.get_or_init(|| {
        std::thread::available_parallelism()
            .map(|count| count.get().saturating_mul(32))
            .unwrap_or(MIN_SHARDED_CHILDREN)
            .clamp(MIN_SHARDED_CHILDREN, 2048)
    })
}

fn shard_index(name: &str, count: usize) -> usize {
    // Shard selection is process-local and never persisted. Use the existing
    // non-cryptographic hasher to keep the per-operation routing path cheap.
    let mut hasher = FxHasher::default();
    name.hash(&mut hasher);
    (hasher.finish() as usize) & (count - 1)
}

impl InodeChildren {
    pub fn new_list() -> Self {
        InodeChildren::List(vec![])
    }

    pub fn new_map() -> Self {
        InodeChildren::Map(BTreeMap::new())
    }

    fn into_map(self) -> BTreeMap<String, Box<InodeView>> {
        match self {
            InodeChildren::List(children) => children
                .into_iter()
                .map(|child| (child.name().to_string(), child))
                .collect(),
            InodeChildren::Map(children) => children,
        }
    }

    fn insert_child_boxed(&mut self, inode: Box<InodeView>) {
        let name = inode.name().to_string();
        match self {
            InodeChildren::List(children) => match Self::search_by_name(children, &name) {
                Ok(index) => {
                    children[index] = inode;
                }
                Err(index) => children.insert(index, inode),
            },
            InodeChildren::Map(children) => {
                children.insert(name, inode);
            }
        }
    }

    // Search for whether the current inode name exists.
    fn search_by_name(list: &[Box<InodeView>], name: &str) -> Result<usize, usize> {
        list.binary_search_by(|f| f.name().cmp(name))
    }

    /// Get children matching glob pattern (e.g., "*.txt", "dir*")
    pub fn get_child_by_glob_pattern<'a>(
        &'a self,
        glob_pattern: &'a Pattern,
    ) -> Option<Vec<&'a InodeView>> {
        match self {
            InodeChildren::List(list) => {
                let mut matches: Vec<&'a InodeView> = Vec::new();
                for child in list {
                    if glob_pattern.matches(child.name()) {
                        matches.push(child.as_ref());
                    }
                }
                Some(matches)
            }
            InodeChildren::Map(map) => {
                let mut matches: Vec<&'a InodeView> = Vec::new();
                for child in map.values() {
                    if glob_pattern.matches(child.name()) {
                        matches.push(child.as_ref());
                    }
                }
                Some(matches)
            }
        }
    }

    pub fn get_child_ptr_by_glob_pattern(&self, glob_pattern: &Pattern) -> Option<Vec<InodePtr>> {
        self.get_child_by_glob_pattern(glob_pattern)
            .map(|children| {
                children
                    .iter()
                    .map(|child| InodePtr::from_owned((*child).clone()))
                    .collect()
            })
    }

    pub fn get_child(&self, name: &str) -> Option<&InodeView> {
        match self {
            InodeChildren::List(list) => {
                let index = Self::search_by_name(list, name);
                match index {
                    Err(_) => None,
                    Ok(v) => Some(&list[v]),
                }
            }
            InodeChildren::Map(map) => map.get(name).map(|x| x.as_ref()),
        }
    }

    fn next_after(&self, name: Option<&str>) -> Option<InodeView> {
        match self {
            InodeChildren::List(children) => {
                let index = name.map_or(0, |name| match Self::search_by_name(children, name) {
                    Ok(index) => index + 1,
                    Err(index) => index,
                });
                children.get(index).map(|child| child.as_ref().clone())
            }
            InodeChildren::Map(children) => {
                let mut range = name.map_or_else(
                    || children.range::<str, _>((Bound::Unbounded, Bound::Unbounded)),
                    |name| children.range::<str, _>((Bound::Excluded(name), Bound::Unbounded)),
                );
                range.next().map(|(_, child)| child.as_ref().clone())
            }
        }
    }

    pub fn get_child_ptr(&self, name: &str) -> Option<InodePtr> {
        self.get_child(name).cloned().map(InodePtr::from_owned)
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        let removed = match self {
            InodeChildren::List(list) => {
                let index = Self::search_by_name(list, child_name);
                match index {
                    Ok(v) => Some(list.remove(v)),
                    Err(_) => None,
                }
            }

            InodeChildren::Map(map) => map.remove(child_name),
        };

        match removed {
            None => err_box!("Child {} not exists", child_name),
            Some(r) => {
                if r.id() != child_id {
                    err_box!(
                        "Inode status error, expect id {}, actually delete {}",
                        child_id,
                        r.id()
                    )
                } else {
                    Ok(*r)
                }
            }
        }
    }

    pub fn list_options(&self, opts: &ListOptions) -> Vec<&InodeView> {
        match self {
            InodeChildren::List(list) => {
                let start = opts
                    .start_after
                    .as_ref()
                    .map(|a| match Self::search_by_name(list, a) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    })
                    .unwrap_or(0);
                let slice = &list[start..];
                let n = opts.limit.unwrap_or(slice.len());
                slice.iter().take(n).map(|b| b.as_ref()).collect()
            }

            InodeChildren::Map(map) => {
                let range = opts.start_after.as_ref().map_or(
                    map.range::<str, _>((Bound::Unbounded, Bound::Unbounded)),
                    |a| map.range::<str, _>((Bound::Excluded(a.as_str()), Bound::Unbounded)),
                );
                let n = opts.limit.unwrap_or(usize::MAX);
                range.take(n).map(|(_, v)| v.as_ref()).collect()
            }
        }
    }

    pub fn add_child(&mut self, inode: InodeView) -> CommonResult<InodePtr> {
        let result = InodePtr::from_owned(inode.clone());
        let inode = Self::stored_child(inode);
        match self {
            InodeChildren::List(list) => {
                let index = Self::search_by_name(list, inode.name());
                match index {
                    Err(v) => {
                        list.insert(v, inode);
                        Ok(result)
                    }

                    Ok(_) => {
                        err_box!("Child {} already exists", inode.name())
                    }
                }
            }

            InodeChildren::Map(map) => match map.entry(inode.name().to_owned()) {
                Entry::Vacant(v) => {
                    v.insert(inode);
                    Ok(result)
                }

                Entry::Occupied(_) => {
                    err_box!("Child {} already exists", inode.name())
                }
            },
        }
    }

    pub fn replace_child(&mut self, child_id: i64, inode: InodeView) -> CommonResult<()> {
        let child_name = inode.name().to_string();
        let inode = Self::stored_child(inode);
        match self {
            InodeChildren::List(list) => {
                let index = Self::search_by_name(list, &child_name)
                    .map_err(|_| CommonError::from(format!("Child {child_name} not exists")))?;
                let current = &list[index];
                if current.id() != child_id {
                    return err_box!(
                        "Inode status error, expect id {}, actually replace {}",
                        child_id,
                        current.id()
                    );
                }
                list[index] = inode;
            }
            InodeChildren::Map(map) => {
                let current = map
                    .get(&child_name)
                    .ok_or_else(|| CommonError::from(format!("Child {child_name} not exists")))?;
                if current.id() != child_id {
                    return err_box!(
                        "Inode status error, expect id {}, actually replace {}",
                        child_id,
                        current.id()
                    );
                }
                map.insert(child_name, inode);
            }
        }
        Ok(())
    }

    fn stored_child(inode: InodeView) -> Box<InodeView> {
        if inode.is_file() {
            Box::new(InodeView::new_entry(inode.name().to_string(), inode.id()))
        } else {
            Box::new(inode)
        }
    }

    pub fn iter(&self) -> ChildrenIter<'_> {
        match self {
            InodeChildren::List(list) => ChildrenIter {
                len: list.len(),
                inner: InnerIter::List(list.iter()),
            },

            InodeChildren::Map(map) => ChildrenIter {
                len: map.len(),
                inner: InnerIter::Map(map.values()),
            },
        }
    }

    pub fn len(&self) -> usize {
        match self {
            InodeChildren::List(list) => list.len(),
            InodeChildren::Map(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for InodeChildren {
    fn default() -> Self {
        Self::new_map()
    }
}

enum InnerIter<'a> {
    List(Iter<'a, Box<InodeView>>),
    Map(Values<'a, String, Box<InodeView>>),
}

pub struct ChildrenIter<'a> {
    len: usize,
    inner: InnerIter<'a>,
}

impl ChildrenIter<'_> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a> Iterator for ChildrenIter<'a> {
    type Item = &'a InodeView;

    fn next(&mut self) -> Option<Self::Item> {
        let next = match &mut self.inner {
            InnerIter::List(list) => list.next(),
            InnerIter::Map(map) => map.next(),
        };
        next.map(|x| x.as_ref())
    }
}
