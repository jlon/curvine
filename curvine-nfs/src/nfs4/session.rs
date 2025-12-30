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

//! NFSv4.1 Session Management
//!
//! Implements session-based exactly-once semantics through:
//! - Session creation and destruction
//! - Slot-based request sequencing
//! - Reply caching for replay detection

use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::types::{Clientid4, Sessionid4};
use crate::nfs4::DEFAULT_SLOT_COUNT;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

// ============================================================================
// Slot - Request Sequencing Unit
// ============================================================================

/// Cached reply for replay detection
#[derive(Clone)]
pub struct CachedReply {
    pub sequence: u32,
    pub reply: Vec<u8>,
}

/// Slot for exactly-once semantics
pub struct Slot {
    /// Slot ID
    pub slot_id: u32,
    /// Current sequence number
    sequence: AtomicU32,
    /// Whether slot is in use
    in_use: std::sync::Mutex<bool>,
    /// Cached reply for replay
    cached_reply: std::sync::Mutex<Option<CachedReply>>,
}

impl Slot {
    pub fn new(slot_id: u32) -> Self {
        Self {
            slot_id,
            sequence: AtomicU32::new(1),
            in_use: std::sync::Mutex::new(false),
            cached_reply: std::sync::Mutex::new(None),
        }
    }

    /// Get current sequence number
    #[inline]
    pub fn sequence(&self) -> u32 {
        self.sequence.load(Ordering::Acquire)
    }

    /// Check and acquire slot for a request
    /// Returns Ok(()) if sequence matches and slot acquired
    /// Returns cached reply if this is a replay
    pub fn acquire(&self, seq: u32) -> Result<Option<CachedReply>, Nfs4Status> {
        let current_seq = self.sequence.load(Ordering::Acquire);

        match seq.cmp(&current_seq) {
            std::cmp::Ordering::Less => {
                // Old sequence - check if it's a replay
                if seq == current_seq.saturating_sub(1) {
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
                // Correct sequence - try to acquire
                let mut in_use = self.in_use.lock().unwrap();
                if *in_use {
                    return Err(Nfs4Status::SeqMisordered);
                }
                *in_use = true;
                // Increment sequence for next request
                self.sequence.fetch_add(1, Ordering::Release);
                Ok(None)
            }
        }
    }

    /// Release slot and cache reply
    pub fn release(&self, reply: Vec<u8>) {
        let seq = self.sequence.load(Ordering::Acquire).saturating_sub(1);
        *self.cached_reply.lock().unwrap() = Some(CachedReply {
            sequence: seq,
            reply,
        });
        *self.in_use.lock().unwrap() = false;
    }
}

// ============================================================================
// Session
// ============================================================================

/// NFSv4.1 Session
pub struct Session {
    /// Session ID (16 bytes)
    pub sessionid: Sessionid4,
    /// Associated client ID
    pub clientid: Clientid4,
    /// Fore channel slots
    slots: Vec<Slot>,
    /// Creation time
    pub created: Instant,
}

impl Session {
    pub fn new(sessionid: Sessionid4, clientid: Clientid4, slot_count: u32) -> Self {
        let slots = (0..slot_count).map(Slot::new).collect();
        Self {
            sessionid,
            clientid,
            slots,
            created: Instant::now(),
        }
    }

    /// Get slot by ID
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
}

// ============================================================================
// SessionManager
// ============================================================================

/// Session Manager - manages all sessions
pub struct SessionManager {
    /// Session ID -> Session
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
        let boot_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            sessions: RwLock::new(HashMap::new()),
            client_sessions: RwLock::new(HashMap::new()),
            next_session_id: AtomicU64::new(1),
            boot_time,
            default_slot_count: DEFAULT_SLOT_COUNT,
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
        let sessionid = self.generate_sessionid();
        let session = Arc::new(Session::new(sessionid, clientid, self.default_slot_count));

        // Store session
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

        Ok(session)
    }

    /// Get session by ID
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

    /// Process SEQUENCE operation
    /// Returns (highest_slot, target_highest_slot, status_flags)
    #[allow(clippy::type_complexity)]
    pub fn sequence(
        &self,
        sessionid: &Sessionid4,
        slot_id: u32,
        sequence_id: u32,
    ) -> Nfs4Result<(Arc<Session>, Option<CachedReply>, u32, u32, u32)> {
        let session = self.get_session(sessionid).ok_or(Nfs4Status::BadSession)?;

        let slot = session.get_slot(slot_id).ok_or(Nfs4Status::BadSlot)?;

        let cached = slot.acquire(sequence_id)?;

        Ok((
            session.clone(),
            cached,
            session.highest_slot(),
            session.highest_slot(),
            0, // status_flags
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
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}
