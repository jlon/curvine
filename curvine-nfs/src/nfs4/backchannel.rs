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

//! NFSv4.1 Backchannel Manager
//!
//! Backchannel allows server to send callbacks to clients, which is essential for:
//! - Delegation recall (CB_RECALL)
//! - Layout recall for pNFS (CB_LAYOUTRECALL)
//! - Notify operations (CB_NOTIFY)
//!
//! # Architecture
//!
//! ```text
//! Server                          Client
//!   |                               |
//!   |  <-- Fore Channel (requests)  |
//!   |  --> Back Channel (callbacks) |
//!   |                               |
//! ```

use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::types::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tracing::{debug, warn};

// ============================================================================
// Callback Operations
// ============================================================================

/// Callback operation types
#[derive(Clone, Debug)]
pub enum CallbackOp {
    /// Recall a delegation
    Recall { stateid: Stateid4, truncate: bool },
    /// Recall a layout (pNFS)
    LayoutRecall {
        layout_type: u32,
        iomode: u32,
        changed: bool,
    },
    /// Notify of attribute changes
    Notify { changes: Vec<u32> },
    /// Get attributes (for write delegation)
    Getattr { bitmap: Vec<u32> },
}

/// Callback task to be sent to client
#[derive(Clone, Debug)]
pub struct CallbackTask {
    pub session_id: Sessionid4,
    pub op: CallbackOp,
    pub file_handle: Nfs4FileHandle,
}

// ============================================================================
// Backchannel State
// ============================================================================

/// Backchannel connection state
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackchannelState {
    /// Not yet established
    None,
    /// Established and working
    Up,
    /// Temporarily down, will retry
    Down,
    /// Permanently failed
    Failed,
}

/// Backchannel connection info for a session
#[derive(Debug)]
pub struct BackchannelConn {
    /// Session ID
    pub session_id: Sessionid4,
    /// Client ID
    pub client_id: Clientid4,
    /// Connection state
    pub state: BackchannelState,
    /// Callback program number
    pub cb_program: u32,
    /// Callback slots (for sequencing)
    pub slots: Vec<BackchannelSlot>,
    /// Next sequence ID
    next_seq: AtomicU32,
    /// Pending callbacks sender
    pub callback_tx: mpsc::UnboundedSender<CallbackTask>,
}

/// Backchannel slot for callback sequencing
#[derive(Debug, Default)]
pub struct BackchannelSlot {
    pub slot_id: u32,
    pub sequence: u32,
    pub in_use: bool,
}

impl BackchannelConn {
    pub fn new(
        session_id: Sessionid4,
        client_id: Clientid4,
        cb_program: u32,
        slot_count: u32,
        callback_tx: mpsc::UnboundedSender<CallbackTask>,
    ) -> Self {
        let slots = (0..slot_count)
            .map(|i| BackchannelSlot {
                slot_id: i,
                sequence: 1,
                in_use: false,
            })
            .collect();

        Self {
            session_id,
            client_id,
            state: BackchannelState::Up,
            cb_program,
            slots,
            next_seq: AtomicU32::new(1),
            callback_tx,
        }
    }

    /// Get next sequence ID
    pub fn next_sequence(&self) -> u32 {
        self.next_seq.fetch_add(1, Ordering::SeqCst)
    }
}

// ============================================================================
// Backchannel Manager
// ============================================================================

/// Backchannel Manager
///
/// Manages server-to-client callback connections for all sessions.
pub struct BackchannelManager {
    /// Session ID -> Backchannel connection
    channels: RwLock<HashMap<Sessionid4, Arc<BackchannelConn>>>,
    /// Client ID -> Session IDs (for finding client's backchannels)
    client_sessions: RwLock<HashMap<Clientid4, Vec<Sessionid4>>>,
}

impl BackchannelManager {
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            client_sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Check if backchannel is available for a client
    ///
    /// NFS-Ganesha: get_cb_chan_down(client) returns true if DOWN
    /// This function returns true if backchannel is UP (available).
    ///
    /// IMPORTANT: Without backchannel, server cannot recall delegations,
    /// which can cause client-side delays when closing files.
    pub fn is_available_for_client(&self, client_id: Clientid4) -> bool {
        let client_sessions = self.client_sessions.read().unwrap();
        if let Some(session_ids) = client_sessions.get(&client_id) {
            let channels = self.channels.read().unwrap();
            for session_id in session_ids {
                if let Some(conn) = channels.get(session_id) {
                    if conn.state == BackchannelState::Up {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Register a backchannel for a session
    pub fn register(
        &self,
        session_id: Sessionid4,
        client_id: Clientid4,
        cb_program: u32,
        slot_count: u32,
    ) -> mpsc::UnboundedReceiver<CallbackTask> {
        let (tx, rx) = mpsc::unbounded_channel();

        let conn = Arc::new(BackchannelConn::new(
            session_id, client_id, cb_program, slot_count, tx,
        ));

        self.channels.write().unwrap().insert(session_id, conn);
        self.client_sessions
            .write()
            .unwrap()
            .entry(client_id)
            .or_default()
            .push(session_id);

        debug!(
            "Registered backchannel for session {:?}, client {}",
            &session_id[..4],
            client_id
        );

        rx
    }

    /// Unregister a backchannel
    pub fn unregister(&self, session_id: &Sessionid4) {
        if let Some(conn) = self.channels.write().unwrap().remove(session_id) {
            let mut client_sessions = self.client_sessions.write().unwrap();
            if let Some(sessions) = client_sessions.get_mut(&conn.client_id) {
                sessions.retain(|s| s != session_id);
            }
        }
    }

    /// Get backchannel state for a session
    pub fn get_state(&self, session_id: &Sessionid4) -> BackchannelState {
        self.channels
            .read()
            .unwrap()
            .get(session_id)
            .map(|c| c.state)
            .unwrap_or(BackchannelState::None)
    }

    /// Send a callback to a client via any of its sessions
    pub fn send_callback(&self, client_id: Clientid4, task: CallbackTask) -> Nfs4Result<()> {
        let sessions = self.client_sessions.read().unwrap();
        let session_ids = sessions.get(&client_id).ok_or(Nfs4Status::BadSession)?;

        let channels = self.channels.read().unwrap();

        // Try to find an active backchannel
        for session_id in session_ids {
            if let Some(conn) = channels.get(session_id) {
                // Collapse nested if statements (clippy::collapsible_if)
                if conn.state == BackchannelState::Up && conn.callback_tx.send(task.clone()).is_ok()
                {
                    debug!(
                        "Sent callback to client {} via session {:?}",
                        client_id,
                        &session_id[..4]
                    );
                    return Ok(());
                }
            }
        }

        warn!("No active backchannel for client {}", client_id);
        Err(Nfs4Status::CbPathDown.into())
    }

    /// Recall a delegation from a client
    pub fn recall_delegation(
        &self,
        client_id: Clientid4,
        stateid: Stateid4,
        file_handle: Nfs4FileHandle,
        truncate: bool,
    ) -> Nfs4Result<()> {
        // Find a session for this client
        let sessions = self.client_sessions.read().unwrap();
        let session_ids = sessions.get(&client_id).ok_or(Nfs4Status::BadSession)?;
        // Dereference instead of clone for Copy type (clippy::clone_on_copy)
        let session_id = *session_ids.first().ok_or(Nfs4Status::BadSession)?;

        let task = CallbackTask {
            session_id,
            op: CallbackOp::Recall { stateid, truncate },
            file_handle,
        };

        self.send_callback(client_id, task)
    }

    /// Mark backchannel as down
    pub fn mark_down(&self, session_id: &Sessionid4) {
        // Note: In a real implementation, we'd update the state
        // For now, we just log it
        warn!(
            "Backchannel for session {:?} marked as down",
            &session_id[..4]
        );
    }
}

impl Default for BackchannelManager {
    fn default() -> Self {
        Self::new()
    }
}
