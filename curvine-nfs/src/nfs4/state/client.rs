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

//! Client State Management

use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::session::SessionManager;
use crate::nfs4::types::{ClientOwner4, Clientid4};
use crate::nfs4::DEFAULT_LEASE_TIME;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Cached CREATE_SESSION response for replay detection
#[derive(Clone, Default)]
pub struct CreateSessionSlot {
    /// Cached response data
    pub response: Vec<u8>,
    /// Status of the cached response
    pub status: u32,
}

/// Client state
pub struct ClientState {
    /// Client ID assigned by server
    pub clientid: Clientid4,
    /// Client owner (from EXCHANGE_ID)
    pub owner: ClientOwner4,
    /// Whether client is confirmed (CREATE_SESSION completed)
    pub confirmed: std::sync::atomic::AtomicBool,
    /// Last lease renewal time
    pub last_renew: RwLock<Instant>,
    /// Lease duration
    pub lease_time: Duration,
    /// CREATE_SESSION sequence ID (starts at 1, incremented after each successful CREATE_SESSION)
    pub create_session_sequence: std::sync::atomic::AtomicU32,
    /// Cached CREATE_SESSION response for replay detection
    pub create_session_slot: RwLock<CreateSessionSlot>,
}

impl ClientState {
    pub fn new(clientid: Clientid4, owner: ClientOwner4) -> Self {
        Self {
            clientid,
            owner,
            confirmed: std::sync::atomic::AtomicBool::new(false),
            last_renew: RwLock::new(Instant::now()),
            lease_time: Duration::from_secs(DEFAULT_LEASE_TIME),
            // Per RFC 5661, CREATE_SESSION sequence starts at 1
            create_session_sequence: std::sync::atomic::AtomicU32::new(1),
            create_session_slot: RwLock::new(CreateSessionSlot::default()),
        }
    }

    /// Check if lease has expired
    pub fn is_expired(&self) -> bool {
        let last = *self.last_renew.read().unwrap();
        last.elapsed() > self.lease_time
    }

    /// Renew lease
    pub fn renew(&self) {
        *self.last_renew.write().unwrap() = Instant::now();
    }

    /// Confirm client (set confirmed flag)
    pub fn confirm(&self) {
        self.confirmed
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Check if client is confirmed
    pub fn is_confirmed(&self) -> bool {
        self.confirmed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get current CREATE_SESSION sequence ID
    pub fn get_create_session_sequence(&self) -> u32 {
        self.create_session_sequence
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Increment CREATE_SESSION sequence ID after successful session creation
    pub fn increment_create_session_sequence(&self) {
        self.create_session_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// Cache CREATE_SESSION response for replay detection
    pub fn cache_create_session_response(&self, response: Vec<u8>, status: u32) {
        let mut slot = self.create_session_slot.write().unwrap();
        slot.response = response;
        slot.status = status;
    }

    /// Get cached CREATE_SESSION response
    pub fn get_cached_create_session_response(&self) -> CreateSessionSlot {
        self.create_session_slot.read().unwrap().clone()
    }
}

/// Client Manager - manages all client state
pub struct ClientManager {
    /// Client ID -> Client State
    clients: RwLock<HashMap<Clientid4, Arc<ClientState>>>,
    /// Client Owner ID -> Client ID (for reconnection)
    owner_to_client: RwLock<HashMap<Vec<u8>, Clientid4>>,
    /// Next client ID
    next_clientid: AtomicU64,
    /// Server boot time (for client ID generation)
    boot_time: u64,
}

impl ClientManager {
    pub fn new() -> Self {
        let boot_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            clients: RwLock::new(HashMap::new()),
            owner_to_client: RwLock::new(HashMap::new()),
            next_clientid: AtomicU64::new(1),
            boot_time,
        }
    }

    /// Generate a new client ID
    fn generate_clientid(&self) -> Clientid4 {
        let seq = self.next_clientid.fetch_add(1, Ordering::Relaxed);
        // Combine boot time and sequence for uniqueness
        (self.boot_time << 32) | (seq & 0xFFFFFFFF)
    }

    /// EXCHANGE_ID - register or reconnect a client
    /// Returns (clientid, sequence_id, flags)
    pub fn exchange_id(&self, owner: ClientOwner4) -> Nfs4Result<(Clientid4, u32, u32)> {
        let owner_id = owner.co_ownerid.clone();

        // Check if client is reconnecting - use a separate scope to release locks early
        let existing_clientid = self.owner_to_client.read().unwrap().get(&owner_id).copied();

        if let Some(existing_clientid) = existing_clientid {
            let should_purge = {
                let clients = self.clients.read().unwrap();
                if let Some(client) = clients.get(&existing_clientid) {
                    // Same verifier = client reconnecting
                    if client.owner.co_verifier == owner.co_verifier {
                        client.renew();
                        let flags = if client.is_confirmed() {
                            0x1 // EXCHGID4_FLAG_CONFIRMED_R
                        } else {
                            0
                        };
                        // Return current create_session_sequence for this client
                        let seqid = client.get_create_session_sequence();
                        return Ok((existing_clientid, seqid, flags));
                    }
                    // Different verifier = client restarted, need to clean up
                    true
                } else {
                    false
                }
            }; // clients lock released here

            if should_purge {
                self.purge_client(existing_clientid);
            }
        }

        // New client
        let clientid = self.generate_clientid();
        let client = Arc::new(ClientState::new(clientid, owner));

        // Get the initial sequence ID (should be 1)
        let seqid = client.get_create_session_sequence();

        self.clients.write().unwrap().insert(clientid, client);
        self.owner_to_client
            .write()
            .unwrap()
            .insert(owner_id, clientid);

        tracing::info!("Registered new client {} with create_session_sequence={}", clientid, seqid);
        Ok((clientid, seqid, 0))
    }

    /// Get client by ID
    pub fn get_client(&self, clientid: Clientid4) -> Option<Arc<ClientState>> {
        self.clients.read().unwrap().get(&clientid).cloned()
    }

    /// Find client by owner ID (for NFSv4.0 SETCLIENTID)
    pub fn find_client_by_owner(&self, owner_id: &[u8]) -> Option<Clientid4> {
        self.owner_to_client.read().unwrap().get(owner_id).copied()
    }

    /// Get any confirmed client (for NFSv4.0 when context doesn't have clientid)
    pub fn get_any_confirmed_client(&self) -> Option<Clientid4> {
        let clients = self.clients.read().unwrap();
        for (&clientid, client) in clients.iter() {
            if client.is_confirmed() {
                tracing::info!("Found confirmed client {} for NFSv4.0 operation", clientid);
                return Some(clientid);
            }
        }
        tracing::warn!("No confirmed clients found for NFSv4.0 operation");
        None
    }

    /// Confirm client (after CREATE_SESSION)
    pub fn confirm_client(&self, clientid: Clientid4) -> Nfs4Result<()> {
        let clients = self.clients.read().unwrap();
        let client = clients.get(&clientid).ok_or(Nfs4Status::StaleClientid)?;

        client.confirm();
        client.renew();

        tracing::info!("Confirmed client {}", clientid);
        Ok(())
    }

    /// Renew client lease
    pub fn renew_lease(&self, clientid: Clientid4) -> Nfs4Result<()> {
        let clients = self.clients.read().unwrap();
        let client = clients.get(&clientid).ok_or(Nfs4Status::StaleClientid)?;
        client.renew();
        Ok(())
    }

    /// Validate whether DESTROY_CLIENTID can proceed for a client.
    ///
    /// NFS-Ganesha returns:
    /// - `NFS4ERR_STALE_CLIENTID` when the client does not exist
    /// - `NFS4ERR_CLIENTID_BUSY` when the client still has live sessions
    pub fn ensure_destroyable(
        &self,
        clientid: Clientid4,
        sessions: &SessionManager,
    ) -> Nfs4Result<()> {
        if self.get_client(clientid).is_none() {
            return Err(Nfs4Status::StaleClientid.into());
        }

        if sessions.has_client_sessions(clientid) {
            return Err(Nfs4Status::ClientidBusy.into());
        }

        Ok(())
    }

    /// Purge a client and all its state
    pub fn purge_client(&self, clientid: Clientid4) {
        if let Some(client) = self.clients.write().unwrap().remove(&clientid) {
            self.owner_to_client
                .write()
                .unwrap()
                .remove(&client.owner.co_ownerid);
            tracing::info!("Purged client {}", clientid);
        }
    }

    /// Check and clean up expired clients
    pub fn cleanup_expired(&self) -> Vec<Clientid4> {
        let expired: Vec<Clientid4> = self
            .clients
            .read()
            .unwrap()
            .iter()
            .filter(|(_, c)| c.is_expired())
            .map(|(&id, _)| id)
            .collect();

        for clientid in &expired {
            self.purge_client(*clientid);
        }

        expired
    }

    /// Export all client states for persistence
    pub fn export_clients(&self) -> Vec<(Clientid4, Arc<ClientState>)> {
        self.clients
            .read()
            .unwrap()
            .iter()
            .map(|(&id, state)| (id, Arc::clone(state)))
            .collect()
    }
}

impl Default for ClientManager {
    fn default() -> Self {
        Self::new()
    }
}
