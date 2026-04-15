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
use crate::protocol::rpc::{auth_flavor, call_body, opaque_auth, rpc_body, rpc_msg};
use crate::protocol::xdr::XDR;
use crate::server::context::OutboundTx;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::bytes::{BufMut, Bytes, BytesMut};
use tracing::{debug, warn};

const NFS_CB_VERSION: u32 = 1;
const CB_COMPOUND_PROC: u32 = 1;
const NFS4_OP_CB_RECALL: u32 = 4;
const NFS4_OP_CB_SEQUENCE: u32 = 11;
const CB_TAG: &[u8] = b"curvine-cb";

fn write_opaque(out: &mut impl Write, bytes: &[u8]) -> std::io::Result<()> {
    (bytes.len() as u32).serialize(out)?;
    out.write_all(bytes)?;
    let pad = (4 - bytes.len() % 4) % 4;
    if pad != 0 {
        out.write_all(&[0u8; 4][..pad])?;
    }
    Ok(())
}

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
#[repr(u8)]
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
    state: AtomicU8,
    /// Callback program number
    pub cb_program: u32,
    /// Callback slots (for sequencing)
    slots: Mutex<Vec<BackchannelSlot>>,
    slot_cv: Condvar,
    /// Pending callbacks sender
    pub callback_tx: mpsc::UnboundedSender<CallbackTask>,
    /// Optional transport writer for on-wire callbacks on the same connection.
    wire_tx: RwLock<Option<OutboundTx>>,
    /// Callback credential negotiated in CREATE_SESSION.
    auth: RwLock<opaque_auth>,
    /// xid -> callback metadata for in-flight callback RPCs.
    inflight: Mutex<HashMap<u32, InflightCall>>,
}

/// Backchannel slot for callback sequencing
#[derive(Debug, Default)]
pub struct BackchannelSlot {
    pub slot_id: u32,
    pub sequence: u32,
    pub in_use: bool,
}

#[derive(Clone, Debug)]
struct InflightCall {
    slot_id: u32,
    op: CallbackOp,
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
            state: AtomicU8::new(BackchannelState::Up as u8),
            cb_program,
            slots: Mutex::new(slots),
            slot_cv: Condvar::new(),
            callback_tx,
            wire_tx: RwLock::new(None),
            auth: RwLock::new(opaque_auth {
                flavor: auth_flavor::AUTH_NULL,
                body: Vec::new(),
            }),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    #[inline]
    pub fn state(&self) -> BackchannelState {
        match self.state.load(Ordering::Acquire) {
            1 => BackchannelState::Up,
            2 => BackchannelState::Down,
            3 => BackchannelState::Failed,
            _ => BackchannelState::None,
        }
    }

    #[inline]
    pub fn set_state(&self, state: BackchannelState) {
        self.state.store(state as u8, Ordering::Release);
    }

    fn reserve_call(&self) -> Option<(u32, u32, u32)> {
        self.reserve_call_inner(false)
    }

    fn reserve_call_wait(&self) -> Option<(u32, u32, u32)> {
        self.reserve_call_inner(true)
    }

    fn reserve_call_inner(&self, wait: bool) -> Option<(u32, u32, u32)> {
        let mut slots = self.slots.lock().unwrap();
        loop {
            let highest = slots.len().saturating_sub(1) as u32;
            if let Some(slot) = slots.iter_mut().find(|slot| !slot.in_use) {
                let slot_id = slot.slot_id;
                let seq = slot.sequence;
                slot.sequence = slot.sequence.wrapping_add(1).max(1);
                slot.in_use = true;
                return Some((slot_id, seq, highest));
            }

            if !wait {
                return None;
            }

            let (guard, timeout) = self
                .slot_cv
                .wait_timeout(slots, Duration::from_millis(100))
                .unwrap();
            slots = guard;
            if timeout.timed_out() {
                return None;
            }
        }
    }

    fn release_call(&self, xid: u32) -> Option<CallbackOp> {
        let Some(call) = self.inflight.lock().unwrap().remove(&xid) else {
            return None;
        };

        if let Some(slot) = self
            .slots
            .lock()
            .unwrap()
            .iter_mut()
            .find(|slot| slot.slot_id == call.slot_id)
        {
            slot.in_use = false;
        }
        self.slot_cv.notify_all();
        Some(call.op)
    }

    fn attach_wire(&self, wire_tx: OutboundTx, auth: opaque_auth) {
        *self.wire_tx.write().unwrap() = Some(wire_tx);
        *self.auth.write().unwrap() = auth;
        self.set_state(BackchannelState::Up);
    }

    fn wire_tx(&self) -> Option<OutboundTx> {
        self.wire_tx.read().unwrap().clone()
    }

    fn auth(&self) -> opaque_auth {
        self.auth.read().unwrap().clone()
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
    /// Session ID -> in-process callback queue receiver
    receivers: Mutex<HashMap<Sessionid4, mpsc::UnboundedReceiver<CallbackTask>>>,
    /// Globally unique RPC xid for outbound callback calls.
    next_xid: AtomicU32,
    /// xid -> session for O(1) callback completion lookup.
    inflight: Mutex<HashMap<u32, Sessionid4>>,
}

impl BackchannelManager {
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            client_sessions: RwLock::new(HashMap::new()),
            receivers: Mutex::new(HashMap::new()),
            next_xid: AtomicU32::new(1),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    #[inline]
    fn next_xid(&self) -> u32 {
        self.next_xid.fetch_add(1, Ordering::SeqCst)
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
                    if conn.state() == BackchannelState::Up {
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
    ) {
        self.ensure_registered(session_id, client_id, cb_program, slot_count);
    }

    fn ensure_registered(
        &self,
        session_id: Sessionid4,
        client_id: Clientid4,
        cb_program: u32,
        slot_count: u32,
    ) {
        if let Some(conn) = self.channels.read().unwrap().get(&session_id) {
            conn.set_state(BackchannelState::Up);
            return;
        }

        let (tx, rx) = mpsc::unbounded_channel();

        let conn = Arc::new(BackchannelConn::new(
            session_id, client_id, cb_program, slot_count, tx,
        ));

        self.channels.write().unwrap().insert(session_id, conn);
        {
            let mut client_sessions = self.client_sessions.write().unwrap();
            let sessions = client_sessions.entry(client_id).or_default();
            if !sessions.contains(&session_id) {
                sessions.push(session_id);
            }
        }
        self.receivers.lock().unwrap().insert(session_id, rx);

        debug!(
            "Registered backchannel for session {:?}, client {}",
            &session_id[..4],
            client_id
        );
    }

    /// Unregister a backchannel
    pub fn unregister(&self, session_id: &Sessionid4) {
        if let Some(conn) = self.channels.write().unwrap().remove(session_id) {
            let mut client_sessions = self.client_sessions.write().unwrap();
            if let Some(sessions) = client_sessions.get_mut(&conn.client_id) {
                sessions.retain(|s| s != session_id);
            }
        }
        self.receivers.lock().unwrap().remove(session_id);
        self.inflight
            .lock()
            .unwrap()
            .retain(|_, sid| sid != session_id);
    }

    /// Unregister all backchannels for a client.
    pub fn unregister_client(&self, client_id: Clientid4) {
        let session_ids = self
            .client_sessions
            .write()
            .unwrap()
            .remove(&client_id)
            .unwrap_or_default();

        let mut channels = self.channels.write().unwrap();
        let mut receivers = self.receivers.lock().unwrap();
        let session_set: std::collections::HashSet<_> = session_ids.iter().copied().collect();
        for session_id in session_ids {
            channels.remove(&session_id);
            receivers.remove(&session_id);
        }
        self.inflight
            .lock()
            .unwrap()
            .retain(|_, session_id| !session_set.contains(session_id));
    }

    /// Get backchannel state for a session
    pub fn get_state(&self, session_id: &Sessionid4) -> BackchannelState {
        self.channels
            .read()
            .unwrap()
            .get(session_id)
            .map(|c| c.state())
            .unwrap_or(BackchannelState::None)
    }

    pub fn attach_transport(
        &self,
        session_id: &Sessionid4,
        wire_tx: OutboundTx,
        auth: opaque_auth,
    ) -> Nfs4Result<()> {
        let conn = self
            .channels
            .read()
            .unwrap()
            .get(session_id)
            .cloned()
            .ok_or(Nfs4Status::BadSession)?;
        conn.attach_wire(wire_tx, auth);
        Ok(())
    }

    pub fn bind_transport(
        &self,
        session_id: Sessionid4,
        client_id: Clientid4,
        cb_program: u32,
        slot_count: u32,
        wire_tx: OutboundTx,
        auth: opaque_auth,
    ) -> Nfs4Result<()> {
        if self.channels.read().unwrap().contains_key(&session_id) {
            return self.attach_transport(&session_id, wire_tx, auth);
        }
        self.ensure_registered(session_id, client_id, cb_program, slot_count);
        self.attach_transport(&session_id, wire_tx, auth)
    }

    pub fn complete_reply(&self, xid: u32, ok: bool) -> Option<CallbackOp> {
        let session_id = self.inflight.lock().unwrap().remove(&xid)?;
        let conn = self.channels.read().unwrap().get(&session_id).cloned()?;
        let op = conn.release_call(xid)?;
        if !ok {
            conn.set_state(BackchannelState::Down);
        }
        Some(op)
    }

    pub fn complete_compound_reply(&self, xid: u32) -> Option<CallbackOp> {
        let session_id = self.inflight.lock().unwrap().remove(&xid)?;
        let conn = self.channels.read().unwrap().get(&session_id).cloned()?;
        conn.release_call(xid)
    }

    fn encode_cb_sequence(
        &self,
        conn: &BackchannelConn,
        slot_id: u32,
        sequence_id: u32,
        highest_slot: u32,
        out: &mut impl Write,
    ) -> std::io::Result<()> {
        (NFS4_OP_CB_SEQUENCE as u32).serialize(out)?;
        conn.session_id.serialize(out)?;
        sequence_id.serialize(out)?;
        slot_id.serialize(out)?;
        highest_slot.serialize(out)?;
        false.serialize(out)?; // cachethis
        0u32.serialize(out)?; // referring_call_lists length
        Ok(())
    }

    fn encode_cb_recall(&self, task: &CallbackTask, out: &mut impl Write) -> std::io::Result<()> {
        (NFS4_OP_CB_RECALL as u32).serialize(out)?;
        match &task.op {
            CallbackOp::Recall { stateid, truncate } => {
                stateid.serialize(out)?;
                truncate.serialize(out)?;
                task.file_handle.serialize(out)?;
                Ok(())
            }
            _ => unreachable!("only recall callbacks are encoded on wire today"),
        }
    }

    fn encode_wire_callback(
        &self,
        conn: &BackchannelConn,
        xid: u32,
        slot_id: u32,
        sequence_id: u32,
        highest_slot: u32,
        task: &CallbackTask,
    ) -> Nfs4Result<Bytes> {
        let mut buf = BytesMut::with_capacity(256);
        {
            let mut out = (&mut buf).writer();
            rpc_msg {
                xid,
                body: rpc_body::CALL(call_body {
                    rpcvers: 2,
                    prog: conn.cb_program,
                    vers: NFS_CB_VERSION,
                    proc: CB_COMPOUND_PROC,
                    cred: conn.auth(),
                    verf: opaque_auth::default(),
                }),
            }
            .serialize(&mut out)?;

            write_opaque(&mut out, CB_TAG)?; // tag
            1u32.serialize(&mut out)?; // minorversion
            0u32.serialize(&mut out)?; // callback_ident
            2u32.serialize(&mut out)?; // argarray length
            self.encode_cb_sequence(conn, slot_id, sequence_id, highest_slot, &mut out)?;
            self.encode_cb_recall(task, &mut out)?;
        }
        Ok(buf.freeze())
    }

    fn select_sender(
        &self,
        client_id: Clientid4,
    ) -> Nfs4Result<(Arc<BackchannelConn>, mpsc::UnboundedSender<CallbackTask>)> {
        let sessions = self.client_sessions.read().unwrap();
        let session_ids = sessions.get(&client_id).ok_or(Nfs4Status::BadSession)?;

        let channels = self.channels.read().unwrap();

        for session_id in session_ids {
            if let Some(conn) = channels.get(session_id) {
                if conn.state() == BackchannelState::Up {
                    return Ok((Arc::clone(conn), conn.callback_tx.clone()));
                }
            }
        }

        warn!("No active backchannel for client {}", client_id);
        Err(Nfs4Status::CbPathDown.into())
    }

    /// Send a callback to a client via any of its sessions.
    pub fn send_callback(
        &self,
        client_id: Clientid4,
        op: CallbackOp,
        file_handle: Nfs4FileHandle,
    ) -> Nfs4Result<()> {
        let (conn, tx) = self.select_sender(client_id)?;
        let task = CallbackTask {
            session_id: conn.session_id,
            op,
            file_handle,
        };
        let wire = if let Some(wire_tx) = conn.wire_tx() {
            let (slot_id, sequence_id, highest_slot) = conn
                .reserve_call()
                .or_else(|| conn.reserve_call_wait())
                .ok_or(Nfs4Status::BackChanBusy)?;
            let xid = self.next_xid();
            self.inflight.lock().unwrap().insert(xid, conn.session_id);
            conn.inflight.lock().unwrap().insert(
                xid,
                InflightCall {
                    slot_id,
                    op: task.op.clone(),
                },
            );
            let bytes = match self.encode_wire_callback(
                &conn,
                xid,
                slot_id,
                sequence_id,
                highest_slot,
                &task,
            ) {
                Ok(bytes) => bytes,
                Err(err) => {
                    self.inflight.lock().unwrap().remove(&xid);
                    conn.release_call(xid);
                    return Err(err);
                }
            };
            Some((wire_tx, xid, bytes))
        } else {
            None
        };

        if tx.send(task).is_err() {
            if let Some((_, xid, _)) = wire {
                self.inflight.lock().unwrap().remove(&xid);
                conn.release_call(xid);
            }
            return Err(crate::nfs4::error::Nfs4Error::from(Nfs4Status::CbPathDown));
        }

        if let Some((wire_tx, xid, bytes)) = wire {
            if wire_tx.send(Ok(bytes)).is_err() {
                self.inflight.lock().unwrap().remove(&xid);
                conn.release_call(xid);
                return Err(Nfs4Status::CbPathDown.into());
            }
        }

        Ok(())
    }

    /// Recall a delegation from a client
    pub fn recall_delegation(
        &self,
        client_id: Clientid4,
        stateid: Stateid4,
        file_handle: Nfs4FileHandle,
        truncate: bool,
    ) -> Nfs4Result<()> {
        self.send_callback(
            client_id,
            CallbackOp::Recall { stateid, truncate },
            file_handle,
        )
    }

    /// Read one queued callback from a session backchannel.
    pub fn try_recv(&self, session_id: &Sessionid4) -> Option<CallbackTask> {
        let mut receivers = self.receivers.lock().unwrap();
        let receiver = receivers.get_mut(session_id)?;
        receiver.try_recv().ok()
    }

    /// Mark backchannel as down
    pub fn mark_down(&self, session_id: &Sessionid4) {
        if let Some(conn) = self.channels.read().unwrap().get(session_id) {
            conn.set_state(BackchannelState::Down);
        }
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
