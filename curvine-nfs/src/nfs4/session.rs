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

//! NFSv4.1 Session Management - Optimized for High Concurrency
//!
//! # Performance Optimizations (2025-12-31)
//!
//! 1. **Lock-free Slot Acquisition**: Use atomic CAS for slot state
//! 2. **Session Trunking**: Multiple TCP connections share one session
//! 3. **Increased Slot Count**: 128 slots for better parallelism
//! 4. **RwLock for Session Lookup**: Read-heavy workload optimization
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Session Manager                               │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  sessions: RwLock<HashMap<SessionId, Arc<Session>>>             │
//! │  client_sessions: RwLock<HashMap<ClientId, Vec<SessionId>>>     │
//! │  connection_sessions: RwLock<HashMap<ConnId, SessionId>>        │
//! └─────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       Session                                    │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  sessionid: [u8; 16]                                            │
//! │  clientid: u64                                                  │
//! │  slots: Vec<Slot>  (128 slots for parallel requests)            │
//! │  connections: AtomicU32 (trunking: multiple TCP connections)    │
//! └─────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        Slot (Lock-free)                          │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  sequence: AtomicU32                                            │
//! │  state: AtomicU8 (FREE=0, IN_USE=1, CACHED=2)                   │
//! │  cached_reply: Mutex<Option<CachedReply>>                       │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::types::{Clientid4, Sessionid4};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

// ============================================================================
// Constants
// ============================================================================

/// Default slot count per session
/// Aligned with NFS-Ganesha default: NFS41_NB_SLOTS_DEF = 64
/// Reference: nfs-ganesha/src/include/sal_data.h
pub const DEFAULT_SLOT_COUNT: u32 = 64;

/// Maximum slot count (RFC 5661 allows up to 2^32-1)
pub const MAX_SLOT_COUNT: u32 = 1024;

/// Slot states for lock-free state machine
const SLOT_FREE: u8 = 0;
const SLOT_IN_USE: u8 = 1;
const SLOT_CACHED: u8 = 2;

// ============================================================================
// Slot - Lock-free Request Sequencing Unit
// ============================================================================

/// Cached reply for replay detection
#[derive(Clone)]
pub struct CachedReply {
    pub sequence: u32,
    pub reply: Vec<u8>,
}

/// Slot for exactly-once semantics (Lock-free design)
///
/// # Performance Optimization
/// Uses atomic operations for state transitions to avoid mutex contention.
/// Only the cached_reply uses a Mutex (rarely accessed).
///
/// # State Machine
/// ```text
/// FREE ──acquire()──► IN_USE ──release()──► CACHED
///   ▲                                          │
///   └──────────────expire()───────────────────┘
/// ```
pub struct Slot {
    /// Slot ID
    pub slot_id: u32,
    /// Current sequence number (atomic for lock-free read)
    sequence: AtomicU32,
    /// Slot state: FREE(0), IN_USE(1), CACHED(2)
    state: AtomicU8,
    /// Cached reply for replay (only accessed on replay, rare)
    cached_reply: Mutex<Option<CachedReply>>,
}

impl Slot {
    pub fn new(slot_id: u32) -> Self {
        Self {
            slot_id,
            sequence: AtomicU32::new(1),
            state: AtomicU8::new(SLOT_FREE),
            cached_reply: Mutex::new(None),
        }
    }

    /// Get current sequence number (lock-free)
    #[inline]
    pub fn sequence(&self) -> u32 {
        self.sequence.load(Ordering::Acquire)
    }

    /// Check and acquire slot for a request (lock-free fast path)
    ///
    /// # Returns
    /// - Ok(None): Slot acquired, proceed with request
    /// - Ok(Some(reply)): Replay detected, return cached reply
    /// - Err(SeqMisordered): Sequence mismatch
    ///
    /// # Performance
    /// Fast path (normal case): single atomic CAS, no mutex
    /// Slow path (replay): mutex lock to read cached reply
    pub fn acquire(&self, seq: u32) -> Result<Option<CachedReply>, Nfs4Status> {
        let current_seq = self.sequence.load(Ordering::Acquire);

        match seq.cmp(&current_seq) {
            std::cmp::Ordering::Less => {
                // Potential replay - check if it's the previous sequence
                if seq == current_seq.saturating_sub(1) {
                    // Check if we have a cached reply (slow path)
                    let cached = self.cached_reply.lock().unwrap();
                    if let Some(ref reply) = *cached {
                        if reply.sequence == seq {
                            return Ok(Some(reply.clone()));
                        }
                    }
                }
                Err(Nfs4Status::SeqMisordered)
            }
            std::cmp::Ordering::Greater => {
                // Sequence too high
                Err(Nfs4Status::SeqMisordered)
            }
            std::cmp::Ordering::Equal => {
                // Correct sequence - try to acquire with CAS (lock-free)
                match self.state.compare_exchange(
                    SLOT_FREE,
                    SLOT_IN_USE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // Successfully acquired - increment sequence
                        self.sequence.fetch_add(1, Ordering::Release);
                        Ok(None)
                    }
                    Err(SLOT_IN_USE) => {
                        // Slot already in use - concurrent request
                        Err(Nfs4Status::SeqMisordered)
                    }
                    Err(SLOT_CACHED) => {
                        // Slot has cached reply - try to transition to IN_USE
                        match self.state.compare_exchange(
                            SLOT_CACHED,
                            SLOT_IN_USE,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => {
                                self.sequence.fetch_add(1, Ordering::Release);
                                Ok(None)
                            }
                            Err(_) => Err(Nfs4Status::SeqMisordered),
                        }
                    }
                    Err(_) => Err(Nfs4Status::SeqMisordered),
                }
            }
        }
    }

    /// Release slot and cache reply
    #[inline]
    pub fn release(&self, reply: Vec<u8>) {
        let seq = self.sequence.load(Ordering::Acquire).saturating_sub(1);

        // Cache the reply (mutex only for write, rare operation)
        *self.cached_reply.lock().unwrap() = Some(CachedReply {
            sequence: seq,
            reply,
        });

        // Transition to CACHED state
        self.state.store(SLOT_CACHED, Ordering::Release);
    }

    /// Release slot without caching (for errors)
    #[inline]
    pub fn release_no_cache(&self) {
        self.state.store(SLOT_FREE, Ordering::Release);
    }
}

// ============================================================================
// Session - Supports Trunking (Multiple Connections)
// ============================================================================

/// NFSv4.1 Session with Trunking Support
///
/// # Session Trunking (RFC 5661 Section 2.10.3)
/// Multiple TCP connections can share the same session, allowing:
/// - Higher aggregate throughput
/// - Better utilization of multi-core systems
/// - Resilience to connection failures
///
/// # Thread Safety
/// - slots: Each slot is independently lock-free
/// - connections: Atomic counter for trunking
pub struct Session {
    /// Session ID (16 bytes)
    pub sessionid: Sessionid4,
    /// Associated client ID
    pub clientid: Clientid4,
    /// Fore channel slots (lock-free)
    slots: Vec<Slot>,
    /// Number of active connections (trunking)
    connections: AtomicU32,
    /// Creation time
    pub created: Instant,
    /// Session flags (e.g., CONN_BACK_CHAN)
    pub flags: AtomicU32,
}

impl Session {
    pub fn new(sessionid: Sessionid4, clientid: Clientid4, slot_count: u32) -> Self {
        let slot_count = slot_count.min(MAX_SLOT_COUNT);
        let slots = (0..slot_count).map(Slot::new).collect();
        Self {
            sessionid,
            clientid,
            slots,
            connections: AtomicU32::new(1), // First connection
            created: Instant::now(),
            flags: AtomicU32::new(0),
        }
    }

    /// Get slot by ID (no lock needed)
    #[inline]
    pub fn get_slot(&self, slot_id: u32) -> Option<&Slot> {
        self.slots.get(slot_id as usize)
    }

    /// Get highest slot ID
    #[inline]
    pub fn highest_slot(&self) -> u32 {
        self.slots.len().saturating_sub(1) as u32
    }

    /// Get slot count
    #[inline]
    pub fn slot_count(&self) -> u32 {
        self.slots.len() as u32
    }

    /// Add a connection (trunking)
    #[inline]
    pub fn add_connection(&self) -> u32 {
        self.connections.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Remove a connection
    #[inline]
    pub fn remove_connection(&self) -> u32 {
        self.connections.fetch_sub(1, Ordering::AcqRel) - 1
    }

    /// Get connection count
    #[inline]
    pub fn connection_count(&self) -> u32 {
        self.connections.load(Ordering::Acquire)
    }

    /// Set session flags
    #[inline]
    pub fn set_flags(&self, flags: u32) {
        self.flags.store(flags, Ordering::Release);
    }

    /// Get session flags
    #[inline]
    pub fn get_flags(&self) -> u32 {
        self.flags.load(Ordering::Acquire)
    }
}

// ============================================================================
// SessionManager - Optimized for Read-Heavy Workloads
// ============================================================================

/// Session Manager - manages all sessions with trunking support
///
/// # Performance Optimizations
/// 1. RwLock for session lookup (read-heavy)
/// 2. Connection-to-session mapping for fast lookup
/// 3. Lazy cleanup of expired sessions
pub struct SessionManager {
    /// Session ID -> Session (RwLock for read-heavy access)
    sessions: RwLock<HashMap<Sessionid4, Arc<Session>>>,
    /// Client ID -> Session IDs
    client_sessions: RwLock<HashMap<Clientid4, Vec<Sessionid4>>>,
    /// Next session ID counter
    next_session_id: AtomicU64,
    /// Server boot time (for session ID generation)
    boot_time: u64,
    /// Default slot count
    default_slot_count: u32,
}

impl SessionManager {
    pub fn new() -> Self {
        Self::with_slot_count(DEFAULT_SLOT_COUNT)
    }

    pub fn with_slot_count(slot_count: u32) -> Self {
        let boot_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            sessions: RwLock::new(HashMap::new()),
            client_sessions: RwLock::new(HashMap::new()),
            next_session_id: AtomicU64::new(1),
            boot_time,
            default_slot_count: slot_count.min(MAX_SLOT_COUNT),
        }
    }

    /// Generate a new session ID
    fn generate_sessionid(&self) -> Sessionid4 {
        let seq = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let mut id = [0u8; 16];
        id[0..8].copy_from_slice(&self.boot_time.to_le_bytes());
        id[8..16].copy_from_slice(&seq.to_le_bytes());
        id
    }

    /// Create a new session for a client
    pub fn create_session(&self, clientid: Clientid4) -> Nfs4Result<Arc<Session>> {
        self.create_session_with_flags(clientid, 0)
    }

    /// Create a new session with flags
    pub fn create_session_with_flags(
        &self,
        clientid: Clientid4,
        flags: u32,
    ) -> Nfs4Result<Arc<Session>> {
        let sessionid = self.generate_sessionid();
        let session = Arc::new(Session::new(sessionid, clientid, self.default_slot_count));
        session.set_flags(flags);

        // Store session (write lock)
        self.sessions
            .write()
            .unwrap()
            .insert(sessionid, session.clone());

        // Associate with client
        self.client_sessions
            .write()
            .unwrap()
            .entry(clientid)
            .or_default()
            .push(sessionid);

        tracing::info!(
            "CREATE_SESSION: sessionid={:02x?} clientid={} slots={} flags={:#x}",
            &sessionid[..8],
            clientid,
            self.default_slot_count,
            flags
        );

        Ok(session)
    }

    /// Get session by ID (fast path with read lock)
    #[inline]
    pub fn get_session(&self, sessionid: &Sessionid4) -> Option<Arc<Session>> {
        self.sessions.read().unwrap().get(sessionid).cloned()
    }

    /// Destroy a session
    pub fn destroy_session(&self, sessionid: &Sessionid4) -> Nfs4Result<()> {
        let session = self
            .sessions
            .write()
            .unwrap()
            .remove(sessionid)
            .ok_or(Nfs4Status::BadSession)?;

        // Remove from client's session list
        if let Some(sessions) = self
            .client_sessions
            .write()
            .unwrap()
            .get_mut(&session.clientid)
        {
            sessions.retain(|s| s != sessionid);
        }

        tracing::info!(
            "DESTROY_SESSION: sessionid={:02x?} clientid={}",
            &sessionid[..8],
            session.clientid
        );

        Ok(())
    }

    /// Destroy all sessions for a client
    pub fn destroy_client_sessions(&self, clientid: Clientid4) {
        let session_ids: Vec<Sessionid4> = self
            .client_sessions
            .write()
            .unwrap()
            .remove(&clientid)
            .unwrap_or_default();

        let mut sessions = self.sessions.write().unwrap();
        for sid in session_ids {
            sessions.remove(&sid);
        }
    }

    /// Process SEQUENCE operation (optimized)
    ///
    /// # Performance
    /// - Read lock for session lookup (fast path)
    /// - Lock-free slot acquisition
    /// - No write lock in normal case
    #[allow(clippy::type_complexity)]
    pub fn sequence(
        &self,
        sessionid: &Sessionid4,
        slot_id: u32,
        sequence_id: u32,
    ) -> Nfs4Result<(Arc<Session>, Option<CachedReply>, u32, u32, u32)> {
        // Fast path: read lock for session lookup
        let session = self.get_session(sessionid).ok_or(Nfs4Status::BadSession)?;

        // Get slot (no lock)
        let slot = session.get_slot(slot_id).ok_or(Nfs4Status::BadSlot)?;

        // Lock-free slot acquisition
        let cached = slot.acquire(sequence_id)?;

        // Return session info
        Ok((
            session.clone(),
            cached,
            session.highest_slot(),
            session.highest_slot(),
            session.get_flags(),
        ))
    }

    /// Cache reply for a slot
    pub fn cache_reply(&self, sessionid: &Sessionid4, slot_id: u32, reply: Vec<u8>) {
        if let Some(session) = self.get_session(sessionid) {
            if let Some(slot) = session.get_slot(slot_id) {
                slot.release(reply);
            }
        }
    }

    /// Release slot without caching (for errors)
    pub fn release_slot(&self, sessionid: &Sessionid4, slot_id: u32) {
        if let Some(session) = self.get_session(sessionid) {
            if let Some(slot) = session.get_slot(slot_id) {
                slot.release_no_cache();
            }
        }
    }

    /// Get default slot count
    #[inline]
    pub fn default_slot_count(&self) -> u32 {
        self.default_slot_count
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_acquire_release() {
        let slot = Slot::new(0);

        // First acquire should succeed
        assert!(slot.acquire(1).unwrap().is_none());

        // Release with reply
        slot.release(vec![1, 2, 3]);

        // Replay should return cached reply
        let cached = slot.acquire(1).unwrap();
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().reply, vec![1, 2, 3]);

        // Next sequence should succeed
        assert!(slot.acquire(2).unwrap().is_none());
    }

    #[test]
    fn test_session_trunking() {
        let session = Session::new([0u8; 16], 1, 64);

        assert_eq!(session.connection_count(), 1);

        // Add connections (trunking)
        assert_eq!(session.add_connection(), 2);
        assert_eq!(session.add_connection(), 3);
        assert_eq!(session.connection_count(), 3);

        // Remove connection
        assert_eq!(session.remove_connection(), 2);
        assert_eq!(session.connection_count(), 2);
    }

    #[test]
    fn test_session_manager() {
        let manager = SessionManager::with_slot_count(64);

        // Create session
        let session = manager.create_session(12345).unwrap();
        assert_eq!(session.clientid, 12345);
        assert_eq!(session.slot_count(), 64);

        // Get session
        let retrieved = manager.get_session(&session.sessionid).unwrap();
        assert_eq!(retrieved.clientid, 12345);

        // Destroy session
        manager.destroy_session(&session.sessionid).unwrap();
        assert!(manager.get_session(&session.sessionid).is_none());
    }
}
