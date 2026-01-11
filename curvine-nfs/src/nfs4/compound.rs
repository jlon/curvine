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

//! NFSv4.1 COMPOUND Operation Handler
//!
//! COMPOUND allows multiple operations in a single RPC call,
//! reducing network round-trips and improving performance.

use crate::nfs4::backchannel::BackchannelManager;
use crate::nfs4::delegation::DelegationManager;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::fs::Nfs4FileSystem;
use crate::nfs4::session::SessionManager;
use crate::nfs4::state::{
    ClientManager, GracePeriodManager, GracePeriodReaper, LockManager, OpenManager,
    StatePersistenceManager,
};
use crate::nfs4::types::*;
use curvine_common::executor::ScheduledExecutor;
use std::sync::Arc;

// ============================================================================
// NFSv4.1 Operation Codes
// ============================================================================

/// NFSv4.1 operation codes
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum Nfs4Op {
    Access = 3,
    Close = 4,
    Commit = 5,
    Create = 6,
    Delegpurge = 7,
    Delegreturn = 8,
    Getattr = 9,
    Getfh = 10,
    Link = 11,
    Lock = 12,
    Lockt = 13,
    Locku = 14,
    Lookup = 15,
    Lookupp = 16,
    Nverify = 17,
    Open = 18,
    Openattr = 19,
    OpenConfirm = 20, // NFSv4.0 only
    OpenDowngrade = 21,
    Putfh = 22,
    Putpubfh = 23,
    Putrootfh = 24,
    Read = 25,
    Readdir = 26,
    Readlink = 27,
    Remove = 28,
    Rename = 29,
    Renew = 30, // NFSv4.0 only
    Restorefh = 31,
    Savefh = 32,
    Secinfo = 33,
    Setattr = 34,
    Setclientid = 35,        // NFSv4.0 only
    SetclientidConfirm = 36, // NFSv4.0 only
    Verify = 37,
    Write = 38,
    ReleaseLockowner = 39, // NFSv4.0 only

    // NFSv4.1 operations
    BackchannelCtl = 40,
    BindConnToSession = 41,
    ExchangeId = 42,
    CreateSession = 43,
    DestroySession = 44,
    FreeStateid = 45,
    GetDirDelegation = 46,
    Getdeviceinfo = 47,
    Getdevicelist = 48,
    Layoutcommit = 49,
    Layoutget = 50,
    Layoutreturn = 51,
    SecinfoNoName = 52,
    Sequence = 53,
    SetSsv = 54,
    TestStateid = 55,
    WantDelegation = 56,
    DestroyClientid = 57,
    ReclaimComplete = 58,

    Illegal = 10044,
}

impl From<u32> for Nfs4Op {
    fn from(v: u32) -> Self {
        match v {
            3 => Self::Access,
            4 => Self::Close,
            5 => Self::Commit,
            6 => Self::Create,
            7 => Self::Delegpurge,
            8 => Self::Delegreturn,
            9 => Self::Getattr,
            10 => Self::Getfh,
            11 => Self::Link,
            12 => Self::Lock,
            13 => Self::Lockt,
            14 => Self::Locku,
            15 => Self::Lookup,
            16 => Self::Lookupp,
            17 => Self::Nverify,
            18 => Self::Open,
            19 => Self::Openattr,
            20 => Self::OpenConfirm,
            21 => Self::OpenDowngrade,
            22 => Self::Putfh,
            23 => Self::Putpubfh,
            24 => Self::Putrootfh,
            25 => Self::Read,
            26 => Self::Readdir,
            27 => Self::Readlink,
            28 => Self::Remove,
            29 => Self::Rename,
            30 => Self::Renew,
            31 => Self::Restorefh,
            32 => Self::Savefh,
            33 => Self::Secinfo,
            34 => Self::Setattr,
            35 => Self::Setclientid,
            36 => Self::SetclientidConfirm,
            37 => Self::Verify,
            38 => Self::Write,
            39 => Self::ReleaseLockowner,
            40 => Self::BackchannelCtl,
            41 => Self::BindConnToSession,
            42 => Self::ExchangeId,
            43 => Self::CreateSession,
            44 => Self::DestroySession,
            45 => Self::FreeStateid,
            46 => Self::GetDirDelegation,
            47 => Self::Getdeviceinfo,
            48 => Self::Getdevicelist,
            49 => Self::Layoutcommit,
            50 => Self::Layoutget,
            51 => Self::Layoutreturn,
            52 => Self::SecinfoNoName,
            53 => Self::Sequence,
            54 => Self::SetSsv,
            55 => Self::TestStateid,
            56 => Self::WantDelegation,
            57 => Self::DestroyClientid,
            58 => Self::ReclaimComplete,
            _ => Self::Illegal,
        }
    }
}

// ============================================================================
// Compound Context
// ============================================================================

/// Context maintained during COMPOUND execution
pub struct CompoundContext {
    /// Current file handle
    pub current_fh: Option<Nfs4FileHandle>,
    /// Saved file handle (for SAVEFH/RESTOREFH)
    pub saved_fh: Option<Nfs4FileHandle>,
    /// Current stateid (for operations that use "current" stateid)
    pub current_stateid: Option<Stateid4>,
    /// Session ID (set by SEQUENCE, NFSv4.1 only)
    pub sessionid: Option<Sessionid4>,
    /// Slot ID (set by SEQUENCE, NFSv4.1 only)
    pub slot_id: Option<u32>,
    /// Client ID (from SEQUENCE for v4.1, from SETCLIENTID for v4.0)
    pub clientid: Option<Clientid4>,
    /// Minor version (0 = NFSv4.0, 1 = NFSv4.1)
    pub minor_version: u32,
    pub cachethis: bool,
    /// Total number of operations in this COMPOUND (for validation)
    pub op_count: usize,
}

impl CompoundContext {
    pub fn new() -> Self {
        Self {
            current_fh: None,
            saved_fh: None,
            current_stateid: None,
            sessionid: None,
            slot_id: None,
            clientid: None,
            minor_version: 1, // Default to NFSv4.1
            cachethis: false,
            op_count: 0,
        }
    }

    /// Create context with specific minor version
    pub fn with_minor_version(minor_version: u32) -> Self {
        Self {
            current_fh: None,
            saved_fh: None,
            current_stateid: None,
            sessionid: None,
            slot_id: None,
            clientid: None,
            minor_version,
            cachethis: false,
            op_count: 0,
        }
    }

    /// Check if this is NFSv4.0
    pub fn is_v40(&self) -> bool {
        self.minor_version == 0
    }

    /// Check if this is NFSv4.1
    pub fn is_v41(&self) -> bool {
        self.minor_version == 1
    }

    /// Get current file handle or return error
    pub fn require_current_fh(&self) -> Nfs4Result<&Nfs4FileHandle> {
        self.current_fh
            .as_ref()
            .ok_or(Nfs4Status::Nofilehandle.into())
    }
}

impl Default for CompoundContext {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Compound Handler
// ============================================================================

/// COMPOUND operation handler
pub struct CompoundHandler {
    /// Session manager
    pub sessions: Arc<SessionManager>,
    /// Client manager
    pub clients: Arc<ClientManager>,
    /// Open state manager
    pub opens: Arc<OpenManager>,
    /// Lock manager
    pub locks: Arc<LockManager>,
    /// File system
    pub fs: Arc<Nfs4FileSystem>,
    /// Backchannel manager
    pub backchannel: Arc<BackchannelManager>,
    /// Delegation manager
    pub delegations: Arc<DelegationManager>,
    /// Grace period manager
    pub grace: Arc<GracePeriodManager>,
    /// State persistence manager
    pub persistence: Arc<StatePersistenceManager>,
    /// Server boot time (for write verifier)
    pub boot_time: u64,
}

impl CompoundHandler {
    pub fn new(
        sessions: Arc<SessionManager>,
        clients: Arc<ClientManager>,
        opens: Arc<OpenManager>,
        locks: Arc<LockManager>,
        fs: Arc<Nfs4FileSystem>,
        persistence: Arc<StatePersistenceManager>,
        nfs_config: &curvine_common::conf::NfsGatewayConf,
    ) -> Self {
        let backchannel = Arc::new(BackchannelManager::new());

        // Create delegation manager with configuration
        let deleg_config = crate::nfs4::DelegationConfig {
            enabled: nfs_config.delegation_enabled,
            recall_timeout_secs: nfs_config.delegation_recall_timeout_secs,
            max_delegations: nfs_config.delegation_max_count,
            reaper_check_interval_ms: 5000, // Fixed 5s interval
        };
        let delegations = Arc::new(DelegationManager::with_config(
            backchannel.clone(),
            deleg_config.clone(),
        ));

        let grace = Arc::new(GracePeriodManager::with_default_config());

        // Start grace period reaper thread (following NFS-Ganesha design)
        // Check every 5 seconds
        let reaper = GracePeriodReaper::new(grace.clone());
        let scheduler = ScheduledExecutor::new("grace-period-reaper", 5000);
        if let Err(e) = scheduler.start(reaper) {
            tracing::error!("Failed to start grace period reaper: {}", e);
        }

        // Start delegation recall timeout reaper only if delegations are enabled
        // Following performance-first principle: don't create unnecessary threads
        if delegations.is_enabled() {
            let deleg_reaper = crate::nfs4::state::DelegationReaperTask::new(delegations.clone());
            let deleg_scheduler = ScheduledExecutor::new("delegation-reaper", 5000);
            if let Err(e) = deleg_scheduler.start(deleg_reaper) {
                tracing::error!("Failed to start delegation reaper: {}", e);
            }
        }

        Self {
            sessions,
            clients,
            opens,
            locks,
            fs,
            backchannel,
            delegations,
            grace,
            persistence,
            boot_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        }
    }

    /// Check if operation requires SEQUENCE to be called first (NFSv4.1 only)
    #[allow(dead_code)]
    fn requires_sequence(op: Nfs4Op) -> bool {
        !matches!(
            op,
            Nfs4Op::ExchangeId
                | Nfs4Op::CreateSession
                | Nfs4Op::DestroySession
                | Nfs4Op::DestroyClientid
                | Nfs4Op::BindConnToSession
                | Nfs4Op::Illegal
        )
    }

    /// Check if SEQUENCE has been called (NFSv4.1 only)
    /// NFSv4.0 does not use SEQUENCE, so this check is skipped
    #[allow(dead_code)]
    fn check_sequence(&self, ctx: &CompoundContext, op: Nfs4Op) -> Nfs4Result<()> {
        // NFSv4.0 does not require SEQUENCE
        if ctx.is_v40() {
            return Ok(());
        }
        // NFSv4.1 requires SEQUENCE for most operations
        if Self::requires_sequence(op) && ctx.sessionid.is_none() {
            return Err(Nfs4Status::OpNotInSession.into());
        }
        Ok(())
    }

    /// Cleanup all state for a client (NFS-Ganesha: nfs41_single_client_cleanup)
    ///
    /// This method coordinates cleanup across all managers to prevent resource leaks.
    /// Called when:
    /// - Client lease expires
    /// - DESTROY_CLIENTID is received
    /// - Client reconnects with different verifier
    ///
    /// # Cleanup Order (following NFS-Ganesha)
    /// 1. Revoke delegations (CB_RECALL not needed, just revoke)
    /// 2. Close all open files (release OpenFile resources)
    /// 3. Release all locks
    /// 4. Destroy all sessions
    /// 5. Remove client state
    pub async fn cleanup_client(&self, clientid: Clientid4) {
        tracing::info!("CLEANUP_CLIENT: Starting cleanup for client {}", clientid);

        // Step 1: Revoke all delegations for this client
        self.delegations.revoke_all_for_client(clientid);
        tracing::info!(
            "CLEANUP_CLIENT: Revoked delegations for client {}",
            clientid
        );

        // Step 2: Close all open files for this client
        // First get all fileids that need to be closed
        let open_states = self.opens.get_client_opens(clientid);
        for state in &open_states {
            // Close the OpenFile (decrement ref_count, complete if last)
            if let Err(e) = self.fs.close_file(state.fileid).await {
                tracing::warn!(
                    "CLEANUP_CLIENT: Failed to close file {} for client {}: {:?}",
                    state.fileid,
                    clientid,
                    e
                );
            }
        }
        // Then remove all open states from OpenManager
        self.opens.close_all_for_client(clientid);
        tracing::info!(
            "CLEANUP_CLIENT: Closed {} opens for client {}",
            open_states.len(),
            clientid
        );

        // Step 3: Release all locks for this client
        self.locks.release_all_for_client(clientid);
        tracing::info!("CLEANUP_CLIENT: Released locks for client {}", clientid);

        // Step 4: Destroy all sessions for this client
        self.sessions.destroy_client_sessions(clientid);
        tracing::info!("CLEANUP_CLIENT: Destroyed sessions for client {}", clientid);

        // Step 5: Remove client state (done by caller or ClientManager)
        tracing::info!("CLEANUP_CLIENT: Completed cleanup for client {}", clientid);
    }
}
