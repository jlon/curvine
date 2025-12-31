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

//! Open State Management
//!
//! Manages file open state including:
//! - Stateid generation and validation
//! - Share reservation (access/deny modes)
//! - Integration with Nfs4FileSystem for Reader/Writer lifecycle
//!
//! # Key Design (NFSv4.1 vs NFSv3)
//!
//! NFSv3: Stateless, io_cache manages Reader/Writer with TTL
//! NFSv4.1: Stateful, OpenState owns Reader/Writer, released on CLOSE

use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::types::{Clientid4, Fileid4, Stateid4};
use curvine_common::fs::Path;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

// ============================================================================
// Share Access/Deny Modes
// ============================================================================

/// Share access modes (what the opener wants to do)
pub mod share_access {
    pub const READ: u32 = 0x00000001;
    pub const WRITE: u32 = 0x00000002;
    pub const BOTH: u32 = READ | WRITE;
}

/// Share deny modes (what the opener denies to others)
pub mod share_deny {
    pub const NONE: u32 = 0x00000000;
    pub const READ: u32 = 0x00000001;
    pub const WRITE: u32 = 0x00000002;
    pub const BOTH: u32 = READ | WRITE;
}

// ============================================================================
// Open State
// ============================================================================

/// Open state for a file (NFS-Ganesha aligned: state_t)
///
/// NFS-Ganesha equivalent:
/// ```c
/// struct state_t {
///     stateid_t stateid;
///     struct state_share share;  // Only share access/deny info
///     state_owner_t *state_owner; // Owner (clientid + owner_val)
/// }
/// ```
///
/// Key design (NFS-Ganesha aligned):
/// - State is keyed by (file, owner) where owner = clientid + owner_val
/// - Each owner has its own state, each state has one OPEN/CLOSE pair
/// - Multiple states for the same file share the same OpenFile (Reader/Writer)
/// - OpenFile ref_count tracks how many states reference it
pub struct OpenState {
    /// State ID
    pub stateid: Stateid4,
    /// Client ID
    pub clientid: Clientid4,
    /// Owner value (NFS-Ganesha: so_owner_val)
    /// This is typically process ID or thread ID from the client
    pub owner_val: Vec<u8>,
    /// File ID
    pub fileid: Fileid4,
    /// File path
    pub path: Path,
    /// Access mode - protected by RwLock for OPEN_DOWNGRADE
    pub share_access: RwLock<u32>,
    /// Deny mode - protected by RwLock for OPEN_DOWNGRADE
    pub share_deny: RwLock<u32>,
    /// Sequence ID (incremented on each state change)
    seqid: AtomicU32,
    /// Confirmed flag (NFSv4.0 only - NFS-Ganesha: so_confirmed)
    /// In NFSv4.0, OPEN creates unconfirmed state, OPEN_CONFIRM sets this to true
    /// In NFSv4.1, this is always true (no OPEN_CONFIRM needed)
    confirmed: AtomicBool,
}

impl std::fmt::Debug for OpenState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenState")
            .field("stateid", &self.stateid)
            .field("clientid", &self.clientid)
            .field("owner_val", &format!("{:02x?}", &self.owner_val))
            .field("fileid", &self.fileid)
            .field("path", &self.path.path())
            .field("access", &self.get_access())
            .field("deny", &self.get_deny())
            .field("seqid", &self.seqid())
            .field("confirmed", &self.is_confirmed())
            .finish()
    }
}

impl OpenState {
    pub fn new(
        stateid: Stateid4,
        clientid: Clientid4,
        owner_val: Vec<u8>,
        fileid: Fileid4,
        path: Path,
        access: u32,
        deny: u32,
    ) -> Self {
        Self {
            stateid,
            clientid,
            owner_val,
            fileid,
            path,
            share_access: RwLock::new(access),
            share_deny: RwLock::new(deny),
            seqid: AtomicU32::new(1),
            confirmed: AtomicBool::new(false), // NFSv4.0: starts unconfirmed
        }
    }

    /// Get current sequence ID
    #[inline]
    pub fn seqid(&self) -> u32 {
        self.seqid.load(Ordering::Acquire)
    }

    /// Increment and get new sequence ID
    pub fn next_seqid(&self) -> u32 {
        self.seqid.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Check if state is confirmed (NFS-Ganesha: so_confirmed)
    #[inline]
    pub fn is_confirmed(&self) -> bool {
        self.confirmed.load(Ordering::Acquire)
    }

    /// Set confirmed flag (NFS-Ganesha: so_confirmed = true)
    /// Called by OPEN_CONFIRM in NFSv4.0
    pub fn set_confirmed(&self, confirmed: bool) {
        self.confirmed.store(confirmed, Ordering::Release);
    }

    /// Get current access mode
    #[inline]
    pub fn get_access(&self) -> u32 {
        *self.share_access.read().unwrap()
    }

    /// Get current deny mode
    #[inline]
    pub fn get_deny(&self) -> u32 {
        *self.share_deny.read().unwrap()
    }

    /// Check if access mode allows read
    #[inline]
    pub fn can_read(&self) -> bool {
        self.get_access() & share_access::READ != 0
    }

    /// Check if access mode allows write
    #[inline]
    pub fn can_write(&self) -> bool {
        self.get_access() & share_access::WRITE != 0
    }

    /// Downgrade access and deny modes (for OPEN_DOWNGRADE)
    ///
    /// The new modes must be a subset of the current modes.
    /// This is validated by the caller before calling this method.
    pub fn downgrade_access(&self, new_access: u32, new_deny: u32) {
        *self.share_access.write().unwrap() = new_access;
        *self.share_deny.write().unwrap() = new_deny;
        self.next_seqid();
    }
}

pub struct OpenManager {
    /// Stateid -> Open State
    states: RwLock<HashMap<[u8; 12], Arc<OpenState>>>,
    /// File ID -> Stateids (for share conflict checking)
    file_opens: RwLock<HashMap<Fileid4, Vec<[u8; 12]>>>,
    /// Client ID -> Stateids
    client_opens: RwLock<HashMap<Clientid4, Vec<[u8; 12]>>>,
    /// (File ID, Owner Val) -> Stateid (for state reuse - NFS-Ganesha aligned)
    /// This allows finding existing state for same file+owner combination
    /// NFS-Ganesha: nfs4_State_Get_Obj(file_obj, owner)
    #[allow(clippy::type_complexity)]
    file_owner_state: RwLock<HashMap<(Fileid4, Vec<u8>), [u8; 12]>>,
    /// Next stateid counter
    next_stateid: AtomicU32,
    /// Server boot time
    boot_time: u64,
}

impl OpenManager {
    pub fn new() -> Self {
        let boot_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            states: RwLock::new(HashMap::new()),
            file_opens: RwLock::new(HashMap::new()),
            client_opens: RwLock::new(HashMap::new()),
            file_owner_state: RwLock::new(HashMap::new()),
            next_stateid: AtomicU32::new(1),
            boot_time,
        }
    }

    /// Generate a new stateid
    fn generate_stateid(&self) -> Stateid4 {
        let seq = self.next_stateid.fetch_add(1, Ordering::Relaxed);
        let mut other = [0u8; 12];
        other[0..4].copy_from_slice(&(self.boot_time as u32).to_le_bytes());
        other[4..8].copy_from_slice(&seq.to_le_bytes());
        // Remaining 4 bytes are random/zero for uniqueness
        Stateid4::new(1, other)
    }

    /// Check for share reservation conflicts
    fn check_share_conflict(
        &self,
        fileid: Fileid4,
        access: u32,
        deny: u32,
        exclude_client: Option<Clientid4>,
    ) -> Nfs4Result<()> {
        let file_opens = self.file_opens.read().unwrap();
        let states = self.states.read().unwrap();

        if let Some(stateids) = file_opens.get(&fileid) {
            for stateid_other in stateids {
                if let Some(state) = states.get(stateid_other) {
                    // Skip same client's opens (they can upgrade)
                    if let Some(exclude) = exclude_client {
                        if state.clientid == exclude {
                            continue;
                        }
                    }

                    // Check: my access vs their deny
                    let state_deny = state.get_deny();
                    let state_access = state.get_access();
                    if (access & share_access::READ != 0) && (state_deny & share_deny::READ != 0) {
                        return Err(Nfs4Status::ShareDenied.into());
                    }
                    if (access & share_access::WRITE != 0) && (state_deny & share_deny::WRITE != 0)
                    {
                        return Err(Nfs4Status::ShareDenied.into());
                    }

                    // Check: my deny vs their access
                    if (deny & share_deny::READ != 0) && (state_access & share_access::READ != 0) {
                        return Err(Nfs4Status::ShareDenied.into());
                    }
                    if (deny & share_deny::WRITE != 0) && (state_access & share_access::WRITE != 0)
                    {
                        return Err(Nfs4Status::ShareDenied.into());
                    }
                }
            }
        }

        Ok(())
    }

    /// Open a file (NFS-Ganesha aligned: reuse existing state for same file+owner)
    ///
    /// # NFS-Ganesha Reference
    /// From nfs4_op_open.c:977:
    /// ```c
    /// /* Check if there is already a state for this entry and owner. */
    /// *file_state = nfs4_State_Get_Obj(file_obj, owner);
    /// ```
    ///
    /// If a state already exists for this (file, owner) pair:
    /// - Reuse the existing state
    /// - Upgrade access/deny modes if needed
    /// - Return the existing stateid
    /// - Return `new_state = false`
    ///
    /// Otherwise:
    /// - Create a new state
    /// - Generate a new stateid
    /// - Return `new_state = true`
    ///
    /// # Arguments
    /// - clientid: Client ID
    /// - owner_val: Owner value (typically process ID from client)
    /// - fileid: File ID
    /// - path: File path
    /// - access: Share access mode
    /// - deny: Share deny mode
    ///
    /// # Returns
    /// `(Arc<OpenState>, bool)` where bool indicates if this is a new state
    pub fn open(
        &self,
        clientid: Clientid4,
        owner_val: Vec<u8>,
        fileid: Fileid4,
        path: Path,
        access: u32,
        deny: u32,
    ) -> Nfs4Result<(Arc<OpenState>, bool)> {
        // NFS-Ganesha aligned: Check if there is already a state for this (file, owner)
        let existing_stateid = self
            .file_owner_state
            .read()
            .unwrap()
            .get(&(fileid, owner_val.clone()))
            .copied();

        if let Some(stateid_key) = existing_stateid {
            // Found existing state - reuse it (NFS-Ganesha behavior)
            if let Some(state) = self.states.read().unwrap().get(&stateid_key).cloned() {
                // Check if we need to upgrade access/deny modes
                let current_access = state.get_access();
                let current_deny = state.get_deny();
                let new_access = current_access | access;
                let new_deny = current_deny | deny;

                if new_access != current_access || new_deny != current_deny {
                    // Upgrade modes (check conflicts first)
                    self.check_share_conflict(fileid, new_access, new_deny, Some(clientid))?;

                    *state.share_access.write().unwrap() = new_access;
                    *state.share_deny.write().unwrap() = new_deny;
                } else {
                    // Check conflicts with current modes
                    self.check_share_conflict(fileid, access, deny, Some(clientid))?;
                }

                // Return existing state with new_state = false (NFS-Ganesha aligned)
                return Ok((state, false));
            }
        }

        // No existing state - create new one (NFS-Ganesha: alloc_state)

        // Check share conflicts
        self.check_share_conflict(fileid, access, deny, Some(clientid))?;

        // Generate stateid
        let stateid = self.generate_stateid();
        let stateid_key = stateid.other;

        // Create open state with owner_val
        let state = Arc::new(OpenState::new(
            stateid,
            clientid,
            owner_val.clone(),
            fileid,
            path,
            access,
            deny,
        ));

        // Store state
        self.states
            .write()
            .unwrap()
            .insert(stateid_key, state.clone());
        self.file_opens
            .write()
            .unwrap()
            .entry(fileid)
            .or_default()
            .push(stateid_key);
        self.client_opens
            .write()
            .unwrap()
            .entry(clientid)
            .or_default()
            .push(stateid_key);

        // Track (file, owner) -> stateid mapping for reuse
        self.file_owner_state
            .write()
            .unwrap()
            .insert((fileid, owner_val.clone()), stateid_key);

        // Return new state with new_state = true (NFS-Ganesha aligned)
        Ok((state, true))
    }

    /// Get open state by stateid
    pub fn get_state(&self, stateid: &Stateid4) -> Option<Arc<OpenState>> {
        // Handle special stateids
        if stateid.is_special() {
            return None;
        }
        self.states.read().unwrap().get(&stateid.other).cloned()
    }

    /// Verify stateid and optionally update sequence
    pub fn verify_stateid(&self, stateid: &Stateid4) -> Nfs4Result<Arc<OpenState>> {
        // Handle special stateids
        if *stateid == Stateid4::ANONYMOUS || *stateid == Stateid4::READ_BYPASS {
            return Err(Nfs4Status::BadStateid.into());
        }

        let state = self.get_state(stateid).ok_or(Nfs4Status::BadStateid)?;

        // Check sequence (0 means any sequence is OK)
        if stateid.seqid != 0 && stateid.seqid != state.seqid() {
            if stateid.seqid < state.seqid() {
                return Err(Nfs4Status::OldStateid.into());
            }
            return Err(Nfs4Status::BadStateid.into());
        }

        Ok(state)
    }

    /// Close an open state
    ///
    /// NFS-Ganesha aligned: Each state has exactly one CLOSE.
    /// When CLOSE is called, the state is deleted.
    /// Returns the closed state for the caller to handle OpenFile cleanup.
    pub fn close(&self, stateid: &Stateid4) -> Nfs4Result<Arc<OpenState>> {
        // Get and remove the state
        let state = self
            .states
            .write()
            .unwrap()
            .remove(&stateid.other)
            .ok_or(Nfs4Status::BadStateid)?;

        // Remove from file_opens
        if let Some(opens) = self.file_opens.write().unwrap().get_mut(&state.fileid) {
            opens.retain(|s| s != &stateid.other);
        }

        // Remove from client_opens
        if let Some(opens) = self.client_opens.write().unwrap().get_mut(&state.clientid) {
            opens.retain(|s| s != &stateid.other);
        }

        // Remove from file_owner_state mapping
        self.file_owner_state
            .write()
            .unwrap()
            .remove(&(state.fileid, state.owner_val.clone()));

        Ok(state)
    }

    /// Get all open states for a client (for cleanup)
    ///
    /// Returns a list of OpenState references for the client.
    /// Used by cleanup_client to close all OpenFiles before removing states.
    pub fn get_client_opens(&self, clientid: Clientid4) -> Vec<Arc<OpenState>> {
        let client_opens = self.client_opens.read().unwrap();
        let states = self.states.read().unwrap();

        client_opens
            .get(&clientid)
            .map(|stateids| {
                stateids
                    .iter()
                    .filter_map(|sid| states.get(sid).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Close all opens for a client
    pub fn close_all_for_client(&self, clientid: Clientid4) {
        let stateids: Vec<[u8; 12]> = self
            .client_opens
            .write()
            .unwrap()
            .remove(&clientid)
            .unwrap_or_default();

        let mut states = self.states.write().unwrap();
        let mut file_opens = self.file_opens.write().unwrap();

        for stateid_key in stateids {
            if let Some(state) = states.remove(&stateid_key) {
                if let Some(opens) = file_opens.get_mut(&state.fileid) {
                    opens.retain(|s| s != &stateid_key);
                }
            }
        }
    }

    /// Export all open states for persistence
    pub fn export_opens(&self) -> Vec<Arc<OpenState>> {
        self.states
            .read()
            .unwrap()
            .values()
            .map(Arc::clone)
            .collect()
    }

    /// Restore persisted open state (for recovery after server restart)
    ///
    /// This method is called during grace period to restore persisted opens.
    /// The state is marked as unconfirmed until client reclaims it with CLAIM_PREVIOUS.
    ///
    /// # NFS-Ganesha Reference
    /// From nfs4_recovery.c: state recovery during grace period
    ///
    /// # Arguments
    /// - stateid: Persisted stateid.other (12 bytes)
    /// - clientid: Client ID
    /// - fileid: File ID
    /// - path: File path
    /// - access: Share access mode
    /// - deny: Share deny mode
    /// - owner_val: Owner value (will be looked up from persisted state)
    pub fn restore_persisted_state(
        &self,
        stateid: [u8; 12],
        clientid: Clientid4,
        fileid: Fileid4,
        path: Path,
        access: u32,
        deny: u32,
        owner_val: Vec<u8>,
    ) -> Nfs4Result<Arc<OpenState>> {
        // Check if state already exists (should not happen during recovery)
        if self.states.read().unwrap().contains_key(&stateid) {
            tracing::warn!(
                "Restore: stateid {:02x?} already exists, skipping",
                &stateid[..4]
            );
            return Err(Nfs4Status::BadStateid.into());
        }

        // Create stateid from persisted stateid.other
        let stateid_obj = Stateid4::new(1, stateid);

        // Create open state (unconfirmed, will be confirmed on CLAIM_PREVIOUS)
        let state = Arc::new(OpenState::new(
            stateid_obj,
            clientid,
            owner_val.clone(),
            fileid,
            path,
            access,
            deny,
        ));

        // Store state
        self.states.write().unwrap().insert(stateid, state.clone());
        self.file_opens
            .write()
            .unwrap()
            .entry(fileid)
            .or_default()
            .push(stateid);
        self.client_opens
            .write()
            .unwrap()
            .entry(clientid)
            .or_default()
            .push(stateid);

        // Track (file, owner) -> stateid mapping for CLAIM_PREVIOUS lookup
        self.file_owner_state
            .write()
            .unwrap()
            .insert((fileid, owner_val), stateid);

        tracing::info!(
            "Restored persisted open state: stateid={:02x?} clientid={} fileid={}",
            &stateid[..4],
            clientid,
            fileid
        );

        Ok(state)
    }

    /// Find persisted state by (fileid, owner_val) for CLAIM_PREVIOUS
    ///
    /// This is used during CLAIM_PREVIOUS to find the state that was restored from persistence.
    pub fn find_persisted_state(
        &self,
        fileid: Fileid4,
        owner_val: &[u8],
    ) -> Option<Arc<OpenState>> {
        let stateid_key = self
            .file_owner_state
            .read()
            .unwrap()
            .get(&(fileid, owner_val.to_vec()))
            .copied()?;

        self.states.read().unwrap().get(&stateid_key).cloned()
    }
}

impl Default for OpenManager {
    fn default() -> Self {
        Self::new()
    }
}
