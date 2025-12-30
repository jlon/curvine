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

//! NFSv4.1 Delegation Management
//!
//! Delegation allows clients to cache file data locally without
//! constantly checking with the server. The server grants a delegation
//! and uses Backchannel to recall it when another client needs access.
//!
//! # Performance Note
//!
//! **Delegations are DISABLED by default** for maximum performance.
//! Enable only if your workload benefits from client-side caching.
//!
//! # Delegation Types
//!
//! - READ: Client can cache reads, server recalls on any write
//! - WRITE: Client can cache reads and writes, server recalls on any access
//!
//! # Architecture
//!
//! ```text
//! Client 1                    Server                    Client 2
//!    |                          |                          |
//!    |-- OPEN(WANT_DELEG) ----->|                          |
//!    |<-- OK + READ_DELEG ------|                          |
//!    |   (can cache locally)    |                          |
//!    |                          |<-- OPEN(file) -----------|
//!    |<-- CB_RECALL ------------|                          |
//!    |-- DELEGRETURN ---------->|                          |
//!    |                          |-- OK ------------------->|
//! ```

use crate::nfs4::backchannel::BackchannelManager;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::types::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

// ============================================================================
// Configuration
// ============================================================================

/// Delegation configuration
#[derive(Clone, Debug)]
pub struct DelegationConfig {
    /// Whether delegations are enabled (default: false for performance)
    pub enabled: bool,
    /// Recall timeout in seconds (default: 30)
    pub recall_timeout_secs: u64,
    /// Maximum number of delegations (default: 1000)
    pub max_delegations: usize,
    /// Reaper check interval in milliseconds (default: 5000)
    pub reaper_check_interval_ms: u64,
}

impl Default for DelegationConfig {
    fn default() -> Self {
        Self {
            // Disabled by default for maximum performance
            // Following NFS-Ganesha's conservative approach
            enabled: false,
            recall_timeout_secs: 30,
            max_delegations: 1000,
            reaper_check_interval_ms: 5000,
        }
    }
}

// ============================================================================
// Delegation Types
// ============================================================================

/// Delegation type
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum DelegationType {
    /// No delegation
    None = 0,
    /// Read delegation (client can cache reads)
    Read = 1,
    /// Write delegation (client can cache reads and writes)
    Write = 2,
}

impl From<u32> for DelegationType {
    fn from(v: u32) -> Self {
        match v {
            1 => Self::Read,
            2 => Self::Write,
            _ => Self::None,
        }
    }
}

/// OPEN4 flags for delegation
pub mod open4_share_access {
    /// Client wants a delegation
    pub const WANT_DELEG_ANY: u32 = 0x0100;
    /// Client wants read delegation
    pub const WANT_READ_DELEG: u32 = 0x0200;
    /// Client wants write delegation
    pub const WANT_WRITE_DELEG: u32 = 0x0400;
    /// Client doesn't want delegation
    pub const WANT_NO_DELEG: u32 = 0x0800;
    /// Mask for delegation want flags
    pub const WANT_DELEG_MASK: u32 = 0x0F00;
}

// ============================================================================
// Delegation State
// ============================================================================

/// Delegation state for a file
#[derive(Clone, Debug)]
pub struct Delegation {
    /// Delegation stateid
    pub stateid: Stateid4,
    /// Client ID holding the delegation
    pub clientid: Clientid4,
    /// File ID
    pub fileid: Fileid4,
    /// Delegation type
    pub deleg_type: DelegationType,
    /// When delegation was granted
    pub granted_at: Instant,
    /// Whether recall has been initiated
    pub recall_initiated: bool,
    /// Recall initiated time (for timeout)
    pub recall_time: Option<Instant>,
}

impl Delegation {
    pub fn new(
        stateid: Stateid4,
        clientid: Clientid4,
        fileid: Fileid4,
        deleg_type: DelegationType,
    ) -> Self {
        Self {
            stateid,
            clientid,
            fileid,
            deleg_type,
            granted_at: Instant::now(),
            recall_initiated: false,
            recall_time: None,
        }
    }

    /// Check if recall has timed out
    pub fn recall_timed_out(&self, timeout: Duration) -> bool {
        self.recall_time
            .map(|t| t.elapsed() > timeout)
            .unwrap_or(false)
    }
}

// ============================================================================
// Delegation Manager
// ============================================================================

/// Delegation Manager
///
/// Manages file delegations and coordinates with Backchannel for recalls.
///
/// **Performance Note**: Delegations are disabled by default. Enable via
/// configuration only if your workload benefits from client-side caching.
pub struct DelegationManager {
    /// File ID -> Delegation
    delegations: RwLock<HashMap<Fileid4, Arc<RwLock<Delegation>>>>,
    /// Stateid -> File ID (reverse lookup)
    stateid_to_file: RwLock<HashMap<[u8; 12], Fileid4>>,
    /// Client ID -> File IDs with delegations
    client_delegations: RwLock<HashMap<Clientid4, Vec<Fileid4>>>,
    /// Backchannel manager for recalls
    backchannel: Arc<BackchannelManager>,
    /// Next stateid counter
    next_stateid: AtomicU32,
    /// Server boot time
    #[allow(dead_code)]
    boot_time: u64,
    /// Recall timeout
    recall_timeout: Duration,
    /// Whether delegations are enabled
    enabled: bool,
    /// Maximum number of delegations
    max_delegations: usize,
}

impl DelegationManager {
    pub fn new(backchannel: Arc<BackchannelManager>) -> Self {
        Self::with_config(backchannel, DelegationConfig::default())
    }

    pub fn with_config(backchannel: Arc<BackchannelManager>, config: DelegationConfig) -> Self {
        let boot_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        tracing::debug!(
            "Delegation manager initialized: enabled={}, recall_timeout={}s, max_delegations={}",
            config.enabled,
            config.recall_timeout_secs,
            config.max_delegations
        );

        Self {
            delegations: RwLock::new(HashMap::new()),
            stateid_to_file: RwLock::new(HashMap::new()),
            client_delegations: RwLock::new(HashMap::new()),
            backchannel,
            next_stateid: AtomicU32::new(1),
            boot_time,
            recall_timeout: Duration::from_secs(config.recall_timeout_secs),
            enabled: config.enabled,
            max_delegations: config.max_delegations,
        }
    }

    /// Generate a new delegation stateid
    fn generate_stateid(&self) -> Stateid4 {
        let seq = self.next_stateid.fetch_add(1, Ordering::Relaxed);
        let mut other = [0u8; 12];
        // Use different prefix than open/lock stateids (0xDELE prefix)
        other[0..4].copy_from_slice(&0xDE1E0000u32.to_le_bytes());
        other[4..8].copy_from_slice(&seq.to_le_bytes());
        Stateid4::new(1, other)
    }

    /// Check if delegations are enabled
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Enable or disable delegations
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Get existing delegation for a file
    pub fn get_delegation(&self, fileid: Fileid4) -> Option<Arc<RwLock<Delegation>>> {
        self.delegations.read().unwrap().get(&fileid).cloned()
    }

    /// Check if a file has an active delegation
    pub fn has_delegation(&self, fileid: Fileid4) -> bool {
        self.delegations.read().unwrap().contains_key(&fileid)
    }

    /// Try to grant a delegation
    ///
    /// Returns Some(delegation) if granted, None if not possible.
    ///
    /// This follows NFS-Ganesha's two-stage check:
    /// 1. can_we_grant_deleg() - technical feasibility (locks, conflicts)
    /// 2. should_we_grant_deleg() - policy decision (heuristics, limits)
    pub fn try_grant(
        &self,
        clientid: Clientid4,
        fileid: Fileid4,
        want_flags: u32,
        _file_handle: Nfs4FileHandle,
    ) -> Option<Delegation> {
        // Fast path: delegations disabled
        if !self.enabled {
            return None;
        }

        // Check if client doesn't want delegation
        if want_flags & open4_share_access::WANT_NO_DELEG != 0 {
            return None;
        }

        // Stage 1: Can we grant? (technical feasibility)
        if !self.can_grant(fileid) {
            return None;
        }

        // Stage 2: Should we grant? (policy decision)
        if !self.should_grant(clientid, fileid, want_flags) {
            return None;
        }

        // Determine delegation type based on want flags
        let deleg_type = if want_flags & open4_share_access::WANT_WRITE_DELEG != 0 {
            DelegationType::Write
        } else if want_flags & open4_share_access::WANT_READ_DELEG != 0 {
            DelegationType::Read
        } else if want_flags & open4_share_access::WANT_DELEG_ANY != 0 {
            // Default to read delegation
            DelegationType::Read
        } else {
            return None;
        };

        // Generate stateid and create delegation
        let stateid = self.generate_stateid();
        let delegation = Delegation::new(stateid, clientid, fileid, deleg_type);

        // Store delegation
        let deleg_arc = Arc::new(RwLock::new(delegation.clone()));
        self.delegations.write().unwrap().insert(fileid, deleg_arc);
        self.stateid_to_file
            .write()
            .unwrap()
            .insert(stateid.other, fileid);
        self.client_delegations
            .write()
            .unwrap()
            .entry(clientid)
            .or_default()
            .push(fileid);

        info!(
            "Granted {:?} delegation for file {} to client {}, stateid={:?}",
            deleg_type,
            fileid,
            clientid,
            &stateid.other[..4]
        );

        Some(delegation)
    }

    /// Check if we CAN grant a delegation (technical feasibility)
    ///
    /// Following NFS-Ganesha's can_we_grant_deleg() logic:
    /// - No conflicting locks
    /// - No anonymous operations in progress
    /// - File doesn't already have a delegation
    fn can_grant(&self, fileid: Fileid4) -> bool {
        // Check if file already has a delegation
        if self.has_delegation(fileid) {
            debug!(
                "File {} already has delegation, not granting new one",
                fileid
            );
            return false;
        }

        // TODO: Check for conflicting NLM locks (when NLM support is added)
        // TODO: Check for anonymous operations in progress

        true
    }

    /// Check if we SHOULD grant a delegation (policy decision)
    ///
    /// Following NFS-Ganesha's should_we_grant_deleg() logic:
    /// - Respect maximum delegations limit
    /// - Check client behavior (revoke count)
    /// - Check recent recall history
    fn should_grant(&self, _clientid: Clientid4, _fileid: Fileid4, _want_flags: u32) -> bool {
        // Check maximum delegations limit
        if self.delegations.read().unwrap().len() >= self.max_delegations {
            debug!(
                "Maximum delegations limit reached ({}), not granting new one",
                self.max_delegations
            );
            return false;
        }

        // TODO: Check client revoke count (when client tracking is added)
        // TODO: Check recent recall history (RECALL2DELEG_TIME)
        // TODO: Check for write file descriptors (contention detection)

        true
    }

    /// Recall a delegation (async, via Backchannel)
    ///
    /// Called when another client needs access to a delegated file.
    /// Returns Ok(()) if recall was initiated, Err if no delegation exists.
    pub async fn recall(
        &self,
        fileid: Fileid4,
        file_handle: Nfs4FileHandle,
        truncate: bool,
    ) -> Nfs4Result<()> {
        let deleg = self
            .delegations
            .read()
            .unwrap()
            .get(&fileid)
            .cloned()
            .ok_or(Nfs4Status::BadStateid)?;

        let (clientid, stateid) = {
            let mut d = deleg.write().unwrap();
            if d.recall_initiated {
                // Already recalling
                return Ok(());
            }
            d.recall_initiated = true;
            d.recall_time = Some(Instant::now());
            (d.clientid, d.stateid)
        };

        // Send CB_RECALL via Backchannel
        match self
            .backchannel
            .recall_delegation(clientid, stateid, file_handle, truncate)
        {
            Ok(()) => {
                debug!(
                    "Initiated delegation recall for file {} from client {}",
                    fileid, clientid
                );
                Ok(())
            }
            Err(e) => {
                warn!("Failed to recall delegation for file {}: {:?}", fileid, e);
                // Mark as not recalling so we can retry
                deleg.write().unwrap().recall_initiated = false;
                deleg.write().unwrap().recall_time = None;
                Err(e)
            }
        }
    }

    /// Return a delegation (DELEGRETURN)
    ///
    /// Called when client voluntarily returns delegation or responds to recall.
    pub fn return_delegation(&self, stateid: &Stateid4) -> Nfs4Result<()> {
        let fileid = self
            .stateid_to_file
            .write()
            .unwrap()
            .remove(&stateid.other)
            .ok_or(Nfs4Status::BadStateid)?;

        let deleg = self
            .delegations
            .write()
            .unwrap()
            .remove(&fileid)
            .ok_or(Nfs4Status::BadStateid)?;

        let clientid = deleg.read().unwrap().clientid;

        // Remove from client's delegation list
        if let Some(files) = self.client_delegations.write().unwrap().get_mut(&clientid) {
            files.retain(|&f| f != fileid);
        }

        info!(
            "Delegation returned for file {} by client {}, stateid={:?}",
            fileid,
            clientid,
            &stateid.other[..4]
        );

        Ok(())
    }

    /// Revoke all delegations for a client (on lease expiry)
    pub fn revoke_all_for_client(&self, clientid: Clientid4) {
        let fileids: Vec<Fileid4> = self
            .client_delegations
            .write()
            .unwrap()
            .remove(&clientid)
            .unwrap_or_default();

        let mut delegations = self.delegations.write().unwrap();
        let mut stateid_to_file = self.stateid_to_file.write().unwrap();

        for fileid in fileids {
            if let Some(deleg) = delegations.remove(&fileid) {
                let stateid = deleg.read().unwrap().stateid.other;
                stateid_to_file.remove(&stateid);
            }
        }

        info!("Revoked all delegations for client {}", clientid);
    }

    /// Check for timed-out recalls and revoke them
    pub fn cleanup_timed_out_recalls(&self) -> Vec<(Fileid4, Clientid4)> {
        let mut revoked = Vec::new();
        let delegations = self.delegations.read().unwrap();

        for (&fileid, deleg) in delegations.iter() {
            let d = deleg.read().unwrap();
            if d.recall_timed_out(self.recall_timeout) {
                revoked.push((fileid, d.clientid));
            }
        }

        drop(delegations);

        // Revoke timed-out delegations
        for (fileid, clientid) in &revoked {
            if let Some(deleg) = self.delegations.write().unwrap().remove(fileid) {
                let stateid = deleg.read().unwrap().stateid.other;
                self.stateid_to_file.write().unwrap().remove(&stateid);
                if let Some(files) = self.client_delegations.write().unwrap().get_mut(clientid) {
                    files.retain(|&f| f != *fileid);
                }
                warn!(
                    "Revoked timed-out delegation for file {} from client {}",
                    fileid, clientid
                );
            }
        }

        revoked
    }

    /// Check if access to a file requires delegation recall
    ///
    /// Returns true if there's a conflicting delegation that needs recall.
    pub fn needs_recall(&self, fileid: Fileid4, clientid: Clientid4, is_write: bool) -> bool {
        if let Some(deleg) = self.delegations.read().unwrap().get(&fileid) {
            let d = deleg.read().unwrap();
            // Different client accessing
            if d.clientid != clientid {
                // Write delegation conflicts with any access
                if d.deleg_type == DelegationType::Write {
                    return true;
                }
                // Read delegation conflicts with write access
                if is_write && d.deleg_type == DelegationType::Read {
                    return true;
                }
            }
        }
        false
    }

    /// Check for delegation conflict and initiate recall if needed
    ///
    /// This should be called before granting access to a file.
    /// Returns Ok(true) if recall was initiated, Ok(false) if no conflict,
    /// or Err if recall failed.
    pub async fn check_and_recall_if_needed(
        &self,
        clientid: Clientid4,
        fileid: Fileid4,
        is_write: bool,
        file_handle: Nfs4FileHandle,
    ) -> Nfs4Result<bool> {
        // Fast path: delegations disabled
        if !self.enabled {
            return Ok(false);
        }

        if self.needs_recall(fileid, clientid, is_write) {
            debug!(
                "Delegation conflict detected for file {}, initiating recall",
                fileid
            );
            // Initiate recall (truncate=false for normal recall)
            self.recall(fileid, file_handle, false).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get delegation info for OPEN response
    pub fn get_delegation_info(&self, fileid: Fileid4) -> Option<(DelegationType, Stateid4)> {
        self.delegations.read().unwrap().get(&fileid).map(|d| {
            let deleg = d.read().unwrap();
            (deleg.deleg_type, deleg.stateid)
        })
    }
}

// ============================================================================
// Delegation Response Encoding
// ============================================================================

/// Encode delegation for OPEN response
pub fn encode_open_delegation(
    delegation: Option<&Delegation>,
    _file_handle: &Nfs4FileHandle,
) -> Nfs4Result<Vec<u8>> {
    use crate::protocol::xdr::XDR;
    let mut result = Vec::new();

    match delegation {
        None => {
            // OPEN_DELEGATE_NONE
            0u32.serialize(&mut result)?;
        }
        Some(deleg) => {
            match deleg.deleg_type {
                DelegationType::None => {
                    0u32.serialize(&mut result)?;
                }
                DelegationType::Read => {
                    // OPEN_DELEGATE_READ
                    1u32.serialize(&mut result)?;
                    // open_read_delegation4
                    deleg.stateid.serialize(&mut result)?;
                    false.serialize(&mut result)?; // recall = false
                                                   // nfsace4 (simplified - allow all)
                    0u32.serialize(&mut result)?; // type = ALLOW
                    0u32.serialize(&mut result)?; // flag
                    0u32.serialize(&mut result)?; // access_mask
                    let who: Vec<u8> = b"EVERYONE@".to_vec();
                    who.serialize(&mut result)?;
                }
                DelegationType::Write => {
                    // OPEN_DELEGATE_WRITE
                    2u32.serialize(&mut result)?;
                    // open_write_delegation4
                    deleg.stateid.serialize(&mut result)?;
                    false.serialize(&mut result)?; // recall = false
                                                   // space_limit4 (NFS4_LIMIT_SIZE)
                    1u32.serialize(&mut result)?;
                    u64::MAX.serialize(&mut result)?; // filesize
                                                      // nfsace4 (simplified - allow all)
                    0u32.serialize(&mut result)?;
                    0u32.serialize(&mut result)?;
                    0u32.serialize(&mut result)?;
                    let who: Vec<u8> = b"EVERYONE@".to_vec();
                    who.serialize(&mut result)?;
                }
            }
        }
    }

    Ok(result)
}
