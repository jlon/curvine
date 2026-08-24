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
use smallvec::SmallVec;

#[cfg(debug_assertions)]
use std::collections::BTreeSet;

const DEFAULT_INODE_LOCK_SHARDS: usize = 4096;
const INLINE_INODE_LOCKS: usize = 4;

type InodeLocks<T> = SmallVec<[T; INLINE_INODE_LOCKS]>;

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
        if normalized.len() == 1 {
            return self.lock_normalized_one(normalized[0]);
        }

        let inode_ids = normalized
            .iter()
            .map(|request| request.inode_id)
            .collect::<InodeLocks<_>>();
        assert_no_reentrant_locks(&inode_ids);

        let mut guards = InodeLocks::with_capacity(normalized.len());
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
            _marker: std::marker::PhantomData,
            inner: InodeLockSetInner::Many {
                guards,
                inode_ids,
                held_locks: normalized,
            },
        }
    }

    pub(crate) fn read_inode(&self, inode_id: i64) -> InodeLockSet<'_> {
        self.lock_normalized_one(NormalizedInodeLock {
            depth: 0,
            inode_id,
            mode: InodeLockMode::Read,
        })
    }

    fn lock_normalized_one<'a>(&'a self, request: NormalizedInodeLock) -> InodeLockSet<'a> {
        assert_no_reentrant_lock(request.inode_id);
        let guard = self.lock_one_guard(request);
        mark_lock_held(request.inode_id);
        InodeLockSet {
            _marker: std::marker::PhantomData,
            inner: InodeLockSetInner::One {
                guard: Some(guard),
                held_lock: request,
            },
        }
    }

    fn lock_one_guard(&self, request: NormalizedInodeLock) -> InodeGuard {
        let lock = self.locks.get_or_create_lock(inode_key(request.inode_id));
        match request.mode {
            InodeLockMode::Read => InodeGuard::Read {
                _guard: lock.read_arc(),
            },
            InodeLockMode::Write => InodeGuard::Write {
                _guard: lock.write_arc(),
            },
        }
    }

    fn normalize_requests(requests: &[InodeLockRequest]) -> InodeLocks<NormalizedInodeLock> {
        if requests.is_empty() {
            return InodeLocks::new();
        }

        let mut normalized = requests
            .iter()
            .map(|request| NormalizedInodeLock {
                depth: request.depth,
                inode_id: request.inode_id,
                mode: request.mode,
            })
            .collect::<InodeLocks<_>>();
        normalized.sort_by_key(|request| request.inode_id);

        let mut write_index = 0;
        for read_index in 1..normalized.len() {
            if normalized[read_index].inode_id == normalized[write_index].inode_id {
                normalized[write_index].depth = normalized[write_index]
                    .depth
                    .min(normalized[read_index].depth);
                normalized[write_index].mode = normalized[write_index]
                    .mode
                    .merge(normalized[read_index].mode);
            } else {
                write_index += 1;
                if write_index != read_index {
                    normalized[write_index] = normalized[read_index];
                }
            }
        }
        normalized.truncate(write_index + 1);
        normalized
    }
}

pub struct InodeLockSet<'a> {
    _marker: std::marker::PhantomData<&'a InodeLockManager>,
    inner: InodeLockSetInner,
}

enum InodeLockSetInner {
    One {
        guard: Option<InodeGuard>,
        held_lock: NormalizedInodeLock,
    },
    Many {
        guards: InodeLocks<InodeGuard>,
        inode_ids: InodeLocks<i64>,
        held_locks: InodeLocks<NormalizedInodeLock>,
    },
}

impl InodeLockSet<'_> {
    pub fn covers_request(&self, request: InodeLockRequest) -> bool {
        self.covers_normalized(NormalizedInodeLock {
            depth: request.depth,
            inode_id: request.inode_id,
            mode: request.mode,
        })
    }

    pub fn covers_requests(&self, requests: &[InodeLockRequest]) -> bool {
        if requests.len() == 1 {
            let required = NormalizedInodeLock {
                depth: requests[0].depth,
                inode_id: requests[0].inode_id,
                mode: requests[0].mode,
            };
            return self.covers_normalized(required);
        }

        let required = InodeLockManager::normalize_requests(requests);
        required
            .into_iter()
            .all(|required| self.covers_normalized(required))
    }

    fn covers_normalized(&self, required: NormalizedInodeLock) -> bool {
        match &self.inner {
            InodeLockSetInner::One { held_lock, .. } => {
                held_lock.inode_id == required.inode_id && held_lock.mode.covers(required.mode)
            }
            InodeLockSetInner::Many { held_locks, .. } => held_locks
                .iter()
                .any(|held| held.inode_id == required.inode_id && held.mode.covers(required.mode)),
        }
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
        match &mut self.inner {
            InodeLockSetInner::One { guard, held_lock } => {
                guard.take();
                mark_lock_released(held_lock.inode_id);
            }
            InodeLockSetInner::Many {
                guards, inode_ids, ..
            } => {
                guards.clear();
                mark_locks_released(inode_ids);
            }
        }
    }
}

fn inode_key(inode_id: i64) -> u64 {
    inode_id as u64
}

#[cfg(debug_assertions)]
fn assert_no_reentrant_lock(inode_id: i64) {
    HELD_INODE_LOCKS.with(|held| {
        let held = held.borrow();
        assert!(
            !held.contains(&inode_id),
            "reentrant inode lock on inode {}",
            inode_id
        );
    });
}

#[cfg(not(debug_assertions))]
fn assert_no_reentrant_lock(_inode_id: i64) {}

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
fn mark_lock_held(inode_id: i64) {
    HELD_INODE_LOCKS.with(|held| {
        held.borrow_mut().insert(inode_id);
    });
}

#[cfg(not(debug_assertions))]
fn mark_lock_held(_inode_id: i64) {}

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
fn mark_lock_released(inode_id: i64) {
    HELD_INODE_LOCKS.with(|held| {
        held.borrow_mut().remove(&inode_id);
    });
}

#[cfg(not(debug_assertions))]
fn mark_lock_released(_inode_id: i64) {}

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
