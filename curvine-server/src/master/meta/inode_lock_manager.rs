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

use super::object_lock_pool::ObjectLockPool;
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard};
use std::collections::BTreeMap;

#[cfg(debug_assertions)]
use std::collections::BTreeSet;

const DEFAULT_INODE_LOCK_SHARDS: usize = 4096;

#[cfg(debug_assertions)]
thread_local! {
    static HELD_INODE_LOCKS: std::cell::RefCell<BTreeSet<i64>> =
        const { std::cell::RefCell::new(BTreeSet::new()) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeLockMode {
    Read,
    Write,
}

impl InodeLockMode {
    fn merge(self, other: Self) -> Self {
        if matches!(self, Self::Write) || matches!(other, Self::Write) {
            Self::Write
        } else {
            Self::Read
        }
    }

    fn covers(self, required: Self) -> bool {
        matches!(self, Self::Write) || self == required
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InodeLockRequest {
    pub depth: usize,
    pub inode_id: i64,
    pub mode: InodeLockMode,
}

impl InodeLockRequest {
    pub fn read(depth: usize, inode_id: i64) -> Self {
        Self {
            depth,
            inode_id,
            mode: InodeLockMode::Read,
        }
    }

    pub fn write(depth: usize, inode_id: i64) -> Self {
        Self {
            depth,
            inode_id,
            mode: InodeLockMode::Write,
        }
    }
}

pub struct InodeLockManager {
    locks: ObjectLockPool,
}

#[derive(Debug, Clone, Copy)]
struct NormalizedInodeLock {
    depth: usize,
    inode_id: i64,
    mode: InodeLockMode,
}

impl Default for InodeLockManager {
    fn default() -> Self {
        Self::new(DEFAULT_INODE_LOCK_SHARDS)
    }
}

impl InodeLockManager {
    pub fn new(shard_count: usize) -> Self {
        Self {
            locks: ObjectLockPool::new(shard_count),
        }
    }

    pub fn lock_many<'a>(&'a self, requests: &[InodeLockRequest]) -> InodeLockSet<'a> {
        let normalized = Self::normalize_requests(requests);
        let inode_ids = normalized
            .iter()
            .map(|request| request.inode_id)
            .collect::<Vec<_>>();
        assert_no_reentrant_locks(&inode_ids);

        let mut guards = Vec::with_capacity(normalized.len());
        for request in &normalized {
            let lock = self.locks.get_or_create_lock(inode_key(request.inode_id));
            let guard = match request.mode {
                InodeLockMode::Read => InodeGuard::Read {
                    _guard: lock.read_arc(),
                },
                InodeLockMode::Write => InodeGuard::Write {
                    _guard: lock.write_arc(),
                },
            };
            guards.push(guard);
        }

        mark_locks_held(&inode_ids);

        InodeLockSet {
            manager: self,
            guards,
            inode_ids,
            held_locks: normalized,
        }
    }

    fn normalize_requests(requests: &[InodeLockRequest]) -> Vec<NormalizedInodeLock> {
        let mut by_inode = BTreeMap::new();
        for request in requests {
            by_inode
                .entry(request.inode_id)
                .and_modify(|existing: &mut NormalizedInodeLock| {
                    existing.depth = existing.depth.min(request.depth);
                    existing.mode = existing.mode.merge(request.mode);
                })
                .or_insert(NormalizedInodeLock {
                    depth: request.depth,
                    inode_id: request.inode_id,
                    mode: request.mode,
                });
        }

        let mut normalized = by_inode.into_values().collect::<Vec<_>>();
        normalized.sort_by_key(|request| request.inode_id);
        normalized
    }
}

pub struct InodeLockSet<'a> {
    manager: &'a InodeLockManager,
    guards: Vec<InodeGuard>,
    inode_ids: Vec<i64>,
    held_locks: Vec<NormalizedInodeLock>,
}

impl InodeLockSet<'_> {
    pub fn covers_requests(&self, requests: &[InodeLockRequest]) -> bool {
        let required = InodeLockManager::normalize_requests(requests);
        required.into_iter().all(|required| {
            self.held_locks
                .iter()
                .any(|held| held.inode_id == required.inode_id && held.mode.covers(required.mode))
        })
    }
}

enum InodeGuard {
    Read {
        _guard: ArcRwLockReadGuard<parking_lot::RawRwLock, ()>,
    },
    Write {
        _guard: ArcRwLockWriteGuard<parking_lot::RawRwLock, ()>,
    },
}

impl Drop for InodeLockSet<'_> {
    fn drop(&mut self) {
        self.guards.clear();
        mark_locks_released(&self.inode_ids);
        let keys = self
            .inode_ids
            .iter()
            .map(|inode_id| inode_key(*inode_id))
            .collect::<Vec<_>>();
        self.manager.locks.cleanup_locks(&keys);
    }
}

fn inode_key(inode_id: i64) -> u64 {
    inode_id as u64
}

#[cfg(debug_assertions)]
fn assert_no_reentrant_locks(inode_ids: &[i64]) {
    HELD_INODE_LOCKS.with(|held| {
        let held = held.borrow();
        for inode_id in inode_ids {
            assert!(
                !held.contains(inode_id),
                "reentrant inode lock on inode {}",
                inode_id
            );
        }
    });
}

#[cfg(not(debug_assertions))]
fn assert_no_reentrant_locks(_inode_ids: &[i64]) {}

#[cfg(debug_assertions)]
fn mark_locks_held(inode_ids: &[i64]) {
    HELD_INODE_LOCKS.with(|held| {
        let mut held = held.borrow_mut();
        for inode_id in inode_ids {
            held.insert(*inode_id);
        }
    });
}

#[cfg(not(debug_assertions))]
fn mark_locks_held(_inode_ids: &[i64]) {}

#[cfg(debug_assertions)]
fn mark_locks_released(inode_ids: &[i64]) {
    HELD_INODE_LOCKS.with(|held| {
        let mut held = held.borrow_mut();
        for inode_id in inode_ids {
            held.remove(inode_id);
        }
    });
}

#[cfg(not(debug_assertions))]
fn mark_locks_released(_inode_ids: &[i64]) {}
