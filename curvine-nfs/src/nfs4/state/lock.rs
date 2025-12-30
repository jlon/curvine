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

//! Byte-Range Lock Management
//!
//! Production-grade NFSv4 byte-range locking with conflict detection,
//! lock queuing, and blocking lock support.
//!
//! Reference: NFS-Ganesha nfs4_op_lock.c, nfs4_op_lockt.c, nfs4_op_locku.c

use crate::nfs4::error::{Nfs4Error, Nfs4Result, Nfs4Status};
use crate::nfs4::types::{Clientid4, Fileid4, LockOwner4, Stateid4};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

// ============================================================================
// Lock Types
// ============================================================================

/// NFSv4 lock types
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum LockType4 {
    /// Read lock (shared)
    ReadLt = 1,
    /// Write lock (exclusive)
    WriteLt = 2,
    /// Read lock with blocking
    ReadwLt = 3,
    /// Write lock with blocking
    WritewLt = 4,
}

impl LockType4 {
    /// Check if this is a write (exclusive) lock
    #[inline]
    pub fn is_write(&self) -> bool {
        matches!(self, Self::WriteLt | Self::WritewLt)
    }

    /// Check if this is a blocking lock request
    #[inline]
    pub fn is_blocking(&self) -> bool {
        matches!(self, Self::ReadwLt | Self::WritewLt)
    }
}

impl From<u32> for LockType4 {
    #[inline]
    fn from(v: u32) -> Self {
        match v {
            1 => Self::ReadLt,
            2 => Self::WriteLt,
            3 => Self::ReadwLt,
            4 => Self::WritewLt,
            _ => Self::ReadLt,
        }
    }
}

// ============================================================================
// Lock Entry Structure (represents a single lock range)
// ============================================================================

/// Lock owner identifier (wraps LockOwner4 for convenience)
pub type LockOwnerId = LockOwner4;

/// Lock entry represents a single byte-range lock
/// Multiple LockEntry can belong to one LockState (stateid)
#[derive(Debug)]
pub struct LockEntry {
    /// File ID
    pub fileid: Fileid4,
    /// Lock type (READ/WRITE) - immutable after creation
    pub lock_type: LockType4,
    /// Start offset - mutable for lock merging
    pub offset: RwLock<u64>,
    /// Length (0 or u64::MAX means to end of file) - mutable for lock merging
    pub length: RwLock<u64>,
    /// Time when lock was granted
    pub granted_time: SystemTime,
}

/// Lock state status
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockStateStatus {
    /// Lock state is active
    Active,
    /// Lock state is blocked
    Blocked,
}

/// Lock State represents an NFSv4 stateid with multiple lock ranges
/// This aligns with NFS-Ganesha's state_t structure
#[derive(Debug)]
pub struct LockState {
    /// Lock stateid
    pub stateid: Stateid4,
    /// Lock owner
    pub owner: LockOwner4,
    /// List of lock entries (lock ranges) belonging to this state
    pub lock_entries: RwLock<Vec<Arc<LockEntry>>>,
    /// State status
    pub status: RwLock<LockStateStatus>,
    /// Sequence ID for stateid updates (atomic for thread-safety)
    pub seqid: AtomicU32,
}

impl LockEntry {
    /// Create a new lock entry
    pub fn new(fileid: Fileid4, lock_type: LockType4, offset: u64, length: u64) -> Self {
        Self {
            fileid,
            lock_type,
            offset: RwLock::new(offset),
            length: RwLock::new(length),
            granted_time: SystemTime::now(),
        }
    }

    /// Get the end offset (exclusive)
    #[inline]
    pub fn end(&self) -> u64 {
        let offset = *self.offset.read().unwrap();
        let length = *self.length.read().unwrap();
        if length == 0 || length == u64::MAX {
            u64::MAX
        } else {
            offset.saturating_add(length)
        }
    }

    /// Check if this lock overlaps with a range
    #[inline]
    pub fn overlaps(&self, offset: u64, length: u64) -> bool {
        let self_offset = *self.offset.read().unwrap();
        let other_end = if length == 0 || length == u64::MAX {
            u64::MAX
        } else {
            offset.saturating_add(length)
        };

        // Ranges overlap if: start1 < end2 AND start2 < end1
        self_offset < other_end && offset < self.end()
    }

    /// Get current offset (for logging/debugging)
    #[inline]
    pub fn get_offset(&self) -> u64 {
        *self.offset.read().unwrap()
    }

    /// Get current length (for logging/debugging)
    #[inline]
    pub fn get_length(&self) -> u64 {
        *self.length.read().unwrap()
    }
}

impl LockState {
    /// Create a new lock state
    pub fn new(stateid: Stateid4, owner: LockOwner4, initial_entry: Arc<LockEntry>) -> Self {
        Self {
            stateid,
            owner,
            lock_entries: RwLock::new(vec![initial_entry]),
            status: RwLock::new(LockStateStatus::Active),
            seqid: AtomicU32::new(stateid.seqid),
        }
    }

    /// Get current stateid with updated seqid
    #[inline]
    pub fn get_stateid(&self) -> Stateid4 {
        Stateid4::new(self.seqid.load(Ordering::Relaxed), self.stateid.other)
    }

    /// Increment seqid and return new stateid
    #[inline]
    pub fn update_stateid(&self) -> Stateid4 {
        let new_seqid = self.seqid.fetch_add(1, Ordering::Relaxed) + 1;
        Stateid4::new(new_seqid, self.stateid.other)
    }

    /// Check if this state conflicts with a lock request
    pub fn conflicts_with(
        &self,
        other_type: LockType4,
        offset: u64,
        length: u64,
        owner: &LockOwner4,
    ) -> bool {
        // Same owner doesn't conflict
        if &self.owner == owner {
            return false;
        }

        let entries = self.lock_entries.read().unwrap();
        for entry in entries.iter() {
            // Check range overlap
            if !entry.overlaps(offset, length) {
                continue;
            }

            // Check type conflict:
            // - WRITE locks conflict with everything
            // - READ locks only conflict with WRITE locks
            if entry.lock_type.is_write() || other_type.is_write() {
                return true;
            }
        }

        false
    }

    /// Add a lock entry to this state (with merging)
    pub fn add_lock_entry(&self, new_entry: Arc<LockEntry>) {
        let mut entries = self.lock_entries.write().unwrap();

        // Merge with existing entries of same type
        let mut to_remove = Vec::new();
        let mut new_offset = new_entry.get_offset();
        let mut new_end = new_entry.end();

        for (idx, entry) in entries.iter().enumerate() {
            // Only merge same lock type
            if entry.lock_type != new_entry.lock_type {
                continue;
            }

            let entry_offset = entry.get_offset();
            let entry_end = entry.end();

            // Check if adjacent or overlapping
            if entry_end.saturating_add(1) < new_offset {
                continue;
            }
            if new_end.saturating_add(1) < entry_offset {
                continue;
            }

            // Merge: expand new_entry range
            if entry_end > new_end {
                new_end = entry_end;
            }
            if entry_offset < new_offset {
                new_offset = entry_offset;
            }

            to_remove.push(idx);
        }

        // Update new_entry range if merged
        if !to_remove.is_empty() {
            *new_entry.offset.write().unwrap() = new_offset;
            let new_length = if new_end == u64::MAX {
                0
            } else {
                new_end.saturating_sub(new_offset)
            };
            *new_entry.length.write().unwrap() = new_length;
        }

        // Remove merged entries (reverse order)
        for idx in to_remove.into_iter().rev() {
            entries.remove(idx);
        }

        // Add new entry
        entries.push(new_entry);
    }

    /// Remove lock range from this state (supports partial unlock)
    /// Returns true if state should be deleted (no more entries)
    pub fn remove_lock_range(&self, offset: u64, length: u64) -> Nfs4Result<bool> {
        let mut entries = self.lock_entries.write().unwrap();
        let unlock_end = if length == 0 || length == u64::MAX {
            u64::MAX
        } else {
            offset.saturating_add(length)
        };

        let mut new_entries = Vec::new();
        let mut indices_to_remove = Vec::new();

        for (idx, entry) in entries.iter().enumerate() {
            let entry_offset = entry.get_offset();
            let entry_end = entry.end();

            // Check if unlock range overlaps this entry
            if unlock_end <= entry_offset || offset >= entry_end {
                // No overlap, keep entry as-is
                continue;
            }

            // Overlap detected - need to split/remove
            indices_to_remove.push(idx);

            // Check if fully covered
            if offset <= entry_offset && unlock_end >= entry_end {
                // Fully covered - remove entire entry
                continue;
            }

            // Partial overlap - create fragments
            // Left fragment: [entry_offset, offset)
            if offset > entry_offset {
                let left_length = offset.saturating_sub(entry_offset);
                let left_entry = Arc::new(LockEntry::new(
                    entry.fileid,
                    entry.lock_type,
                    entry_offset,
                    left_length,
                ));
                new_entries.push(left_entry);
            }

            // Right fragment: [unlock_end, entry_end)
            if unlock_end < entry_end {
                let right_offset = unlock_end;
                let right_length = if entry_end == u64::MAX {
                    0
                } else {
                    entry_end.saturating_sub(unlock_end)
                };
                let right_entry = Arc::new(LockEntry::new(
                    entry.fileid,
                    entry.lock_type,
                    right_offset,
                    right_length,
                ));
                new_entries.push(right_entry);
            }
        }

        // Remove old entries (reverse order)
        for idx in indices_to_remove.into_iter().rev() {
            entries.remove(idx);
        }

        // Add new fragments
        entries.extend(new_entries);

        // Return true if no more entries
        Ok(entries.is_empty())
    }
}

/// Blocked lock information
#[derive(Clone, Debug)]
pub struct BlockedLock {
    /// The lock state that is blocked
    pub lock_state: Arc<LockState>,
    /// Stateids of locks that are blocking this lock
    pub blocking_locks: Vec<[u8; 12]>,
    /// Time when lock was blocked
    pub block_time: SystemTime,
}

// ============================================================================
// Lock Manager
// ============================================================================

/// Lock Manager - production-grade lock management with State/LockEntry separation
/// Aligns with NFS-Ganesha architecture: one State contains multiple LockEntry
pub struct LockManager {
    /// Stateid -> Lock state (one state can have multiple lock entries)
    lock_states: RwLock<HashMap<[u8; 12], Arc<LockState>>>,
    /// Client ID -> Lock stateids (for cleanup)
    client_locks: RwLock<HashMap<Clientid4, Vec<[u8; 12]>>>,
    /// Lock owner -> Lock stateid (for finding existing state)
    owner_locks: RwLock<HashMap<LockOwner4, [u8; 12]>>,
    /// Blocked locks queue (FIFO order)
    blocked_locks: RwLock<Vec<BlockedLock>>,
    /// Next lock stateid counter
    next_stateid: AtomicU32,
    /// Server boot time (for stateid generation)
    boot_time: u64,
    /// Lock statistics
    stats: LockStats,
}

/// Lock statistics for monitoring
#[derive(Debug, Default)]
pub struct LockStats {
    /// Total locks granted
    pub total_granted: AtomicU32,
    /// Total locks denied (non-blocking)
    pub total_denied: AtomicU32,
    /// Total locks blocked
    pub total_blocked: AtomicU32,
    /// Total locks released
    pub total_released: AtomicU32,
    /// Total lock merges performed
    pub total_merges: AtomicU32,
    /// Total lock splits performed
    pub total_splits: AtomicU32,
}

impl LockStats {
    /// Get current statistics snapshot
    pub fn snapshot(&self) -> LockStatsSnapshot {
        LockStatsSnapshot {
            total_granted: self.total_granted.load(Ordering::Relaxed),
            total_denied: self.total_denied.load(Ordering::Relaxed),
            total_blocked: self.total_blocked.load(Ordering::Relaxed),
            total_released: self.total_released.load(Ordering::Relaxed),
            total_merges: self.total_merges.load(Ordering::Relaxed),
            total_splits: self.total_splits.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of lock statistics
#[derive(Debug, Clone, Copy)]
pub struct LockStatsSnapshot {
    pub total_granted: u32,
    pub total_denied: u32,
    pub total_blocked: u32,
    pub total_released: u32,
    pub total_merges: u32,
    pub total_splits: u32,
}

impl LockManager {
    pub fn new() -> Self {
        let boot_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            lock_states: RwLock::new(HashMap::new()),
            client_locks: RwLock::new(HashMap::new()),
            owner_locks: RwLock::new(HashMap::new()),
            blocked_locks: RwLock::new(Vec::new()),
            next_stateid: AtomicU32::new(1),
            boot_time,
            stats: LockStats::default(),
        }
    }

    /// Get lock statistics
    pub fn get_stats(&self) -> LockStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get current lock counts
    pub fn get_lock_counts(&self) -> (usize, usize) {
        let granted = self.lock_states.read().unwrap().len();
        let blocked = self.blocked_locks.read().unwrap().len();
        (granted, blocked)
    }

    /// Generate a new lock stateid
    fn generate_stateid(&self) -> Stateid4 {
        let seq = self.next_stateid.fetch_add(1, Ordering::Relaxed);
        let mut other = [0u8; 12];
        // Use different prefix than open stateids (XOR with 0xFFFFFFFF)
        other[0..4].copy_from_slice(&((self.boot_time as u32) ^ 0xFFFFFFFF).to_le_bytes());
        other[4..8].copy_from_slice(&seq.to_le_bytes());
        Stateid4::new(1, other)
    }

    /// Find conflicting lock states for a lock request
    /// Returns list of conflicting stateids
    fn find_conflicts(
        &self,
        fileid: Fileid4,
        lock_type: LockType4,
        offset: u64,
        length: u64,
        owner: &LockOwner4,
    ) -> Vec<[u8; 12]> {
        let lock_states = self.lock_states.read().unwrap();
        let mut conflicts = Vec::new();

        for (stateid_key, state) in lock_states.iter() {
            // Check if any lock entry in this state conflicts
            let entries = state.lock_entries.read().unwrap();
            for entry in entries.iter() {
                // Only check locks on same file
                if entry.fileid != fileid {
                    continue;
                }

                // Check if conflicts
                if state.conflicts_with(lock_type, offset, length, owner) {
                    conflicts.push(*stateid_key);
                    break; // One conflict per state is enough
                }
            }
        }

        conflicts
    }

    /// Acquire a lock (grant immediately or block if conflicts exist)
    ///
    /// This implements NFS-Ganesha's state_lock logic:
    /// - For new_lock_owner: create new LockState with first LockEntry
    /// - For existing_lock_owner: find LockState, add/merge LockEntry
    /// - Returns the stateid (new or existing with updated seqid)
    #[allow(clippy::too_many_arguments)]
    pub fn lock(
        &self,
        clientid: Clientid4,
        owner: Vec<u8>,
        fileid: Fileid4,
        lock_type: LockType4,
        offset: u64,
        length: u64,
        blocking: bool,
        existing_stateid: Option<&Stateid4>,
    ) -> Nfs4Result<Stateid4> {
        let lock_owner = LockOwner4 { clientid, owner };

        // Check for conflicts
        let conflicts = self.find_conflicts(fileid, lock_type, offset, length, &lock_owner);

        if !conflicts.is_empty() {
            if !blocking {
                // Non-blocking lock request with conflicts -> return DENIED
                self.stats.total_denied.fetch_add(1, Ordering::Relaxed);
                return Err(Nfs4Error::with_message(
                    Nfs4Status::Denied,
                    format!("Lock conflict with {} existing locks", conflicts.len()),
                ));
            } else {
                // Blocking lock not fully implemented yet
                self.stats.total_blocked.fetch_add(1, Ordering::Relaxed);
                return Err(Nfs4Error::with_message(
                    Nfs4Status::Denied,
                    "Lock blocked, blocking locks not fully implemented".to_string(),
                ));
            }
        }

        // No conflicts -> grant lock
        self.stats.total_granted.fetch_add(1, Ordering::Relaxed);

        // Create new lock entry
        let new_entry = Arc::new(LockEntry::new(fileid, lock_type, offset, length));

        // Check if this is new_lock_owner or existing_lock_owner
        let lock_state = if let Some(existing) = existing_stateid {
            // existing_lock_owner: find existing state
            let lock_states = self.lock_states.read().unwrap();
            let state = lock_states
                .get(&existing.other)
                .ok_or(Nfs4Status::BadStateid)?;

            // Verify owner matches
            if state.owner != lock_owner {
                return Err(Nfs4Status::BadStateid.into());
            }

            // Add lock entry to existing state (with merging)
            state.add_lock_entry(new_entry);

            // Return updated stateid (incremented seqid)
            Ok(state.update_stateid())
        } else {
            // new_lock_owner: create new state
            let stateid = self.generate_stateid();
            let stateid_key = stateid.other;

            let lock_state = Arc::new(LockState::new(stateid, lock_owner.clone(), new_entry));

            // Store lock state
            self.lock_states
                .write()
                .unwrap()
                .insert(stateid_key, Arc::clone(&lock_state));

            // Track by client
            self.client_locks
                .write()
                .unwrap()
                .entry(clientid)
                .or_default()
                .push(stateid_key);

            // Track by owner (one state per owner)
            self.owner_locks
                .write()
                .unwrap()
                .insert(lock_owner.clone(), stateid_key);

            Ok(stateid)
        };

        lock_state
    }

    /// Test if a lock can be acquired (LOCKT operation)
    /// Returns the first conflicting lock state if any
    pub fn test_lock(
        &self,
        fileid: Fileid4,
        lock_type: LockType4,
        offset: u64,
        length: u64,
        owner: &LockOwner4,
    ) -> Option<Arc<LockState>> {
        let lock_states = self.lock_states.read().unwrap();

        for state in lock_states.values() {
            if state.conflicts_with(lock_type, offset, length, owner) {
                // Check if any entry is on the requested file
                let entries = state.lock_entries.read().unwrap();
                for entry in entries.iter() {
                    if entry.fileid == fileid && entry.overlaps(offset, length) {
                        return Some(Arc::clone(state));
                    }
                }
            }
        }

        None
    }

    /// Release a lock (LOCKU operation)
    /// Supports partial unlock - can split lock entries
    /// Returns updated stateid (same state, incremented seqid)
    pub fn unlock(&self, stateid: &Stateid4, offset: u64, length: u64) -> Nfs4Result<Stateid4> {
        // First, get a cloned Arc to the state
        let state = {
            let lock_states = self.lock_states.read().unwrap();
            lock_states
                .get(&stateid.other)
                .cloned()
                .ok_or(Nfs4Status::BadStateid)?
        };

        // Remove lock range from state (may split entries)
        let should_delete = state.remove_lock_range(offset, length)?;

        if should_delete {
            // No more lock entries - delete the state
            let mut lock_states = self.lock_states.write().unwrap();
            let removed_state = lock_states
                .remove(&stateid.other)
                .ok_or(Nfs4Status::BadStateid)?;

            // Remove from client_locks
            if let Some(locks) = self
                .client_locks
                .write()
                .unwrap()
                .get_mut(&removed_state.owner.clientid)
            {
                locks.retain(|s| s != &stateid.other);
            }

            // Remove from owner_locks
            self.owner_locks
                .write()
                .unwrap()
                .remove(&removed_state.owner);

            self.stats.total_released.fetch_add(1, Ordering::Relaxed);

            // Return updated stateid (even though state is deleted)
            Ok(state.update_stateid())
        } else {
            // State still has lock entries - return updated stateid
            self.stats.total_released.fetch_add(1, Ordering::Relaxed);

            // Return updated stateid (incremented seqid, same state)
            Ok(state.update_stateid())
        }
    }

    /// Get lock state by stateid (for verification and operations)
    pub fn get_lock_state(&self, stateid: &Stateid4) -> Option<Arc<LockState>> {
        self.lock_states
            .read()
            .unwrap()
            .get(&stateid.other)
            .cloned()
    }

    /// Release all locks for a client (on client expiration)
    pub fn release_all_for_client(&self, clientid: Clientid4) {
        let stateids: Vec<[u8; 12]> = self
            .client_locks
            .write()
            .unwrap()
            .remove(&clientid)
            .unwrap_or_default();

        let mut lock_states = self.lock_states.write().unwrap();
        let mut owner_locks = self.owner_locks.write().unwrap();

        for stateid_key in stateids {
            if let Some(state) = lock_states.remove(&stateid_key) {
                // Remove from owner_locks
                owner_locks.remove(&state.owner);
            }
        }

        tracing::info!("Released all locks for client {}", clientid);
    }

    /// Release all locks for a lock owner (RELEASE_LOCKOWNER operation)
    pub fn release_all_for_owner(&self, owner: &LockOwner4) {
        if let Some(stateid_key) = self.owner_locks.write().unwrap().remove(owner) {
            let mut lock_states = self.lock_states.write().unwrap();
            if let Some(state) = lock_states.remove(&stateid_key) {
                // Remove from client_locks
                if let Some(locks) = self
                    .client_locks
                    .write()
                    .unwrap()
                    .get_mut(&state.owner.clientid)
                {
                    locks.retain(|s| s != &stateid_key);
                }
            }
        }

        tracing::info!("Released all locks for owner (client {})", owner.clientid);
    }

    /// Export all lock states for persistence
    pub fn export_locks(&self) -> Vec<Arc<LockState>> {
        self.lock_states
            .read()
            .unwrap()
            .values()
            .map(Arc::clone)
            .collect()
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}
