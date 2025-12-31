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

//! NFSv4.1 Lease Management
//!
//! Implements lease management with reservation mechanism, aligned with NFS-Ganesha.
//!
//! # Key Concepts (from nfs-ganesha/src/SAL/nfs4_lease.c)
//!
//! 1. **Lease Reservation**: Prevents lease expiration during active operations.
//!    When a request starts, it reserves the lease. When it completes, it releases
//!    the reservation and updates the lease time.
//!
//! 2. **Lease Validation**: Checks if lease is still valid based on last_renew time.
//!
//! 3. **Lease Expiration**: When lease expires, all client state is cleaned up.
//!
//! # NFS-Ganesha Reference
//! - nfs-ganesha/src/SAL/nfs4_lease.c: valid_lease(), reserve_lease(), update_lease()
//! - Default lease time: LEASE_LIFETIME_DEFAULT = 60 seconds

use crate::nfs4::types::Clientid4;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Default lease time in seconds (aligned with NFS-Ganesha)
pub const DEFAULT_LEASE_TIME_SECS: u64 = 60;

/// Lease state for a client
///
/// # NFS-Ganesha Alignment
/// Corresponds to fields in nfs_client_id_t:
/// - cid_last_renew: last renewal time
/// - cid_lease_reservations: reservation counter
/// - cid_confirmed: confirmation status
#[derive(Debug)]
pub struct LeaseState {
    /// Client ID
    pub clientid: Clientid4,
    /// Last lease renewal time
    last_renew: RwLock<Instant>,
    /// Number of active lease reservations
    /// When > 0, lease cannot expire (operation in progress)
    reservations: AtomicU32,
    /// Whether client is confirmed (CREATE_SESSION completed)
    confirmed: std::sync::atomic::AtomicBool,
    /// Whether client is marked for delayed cleanup
    marked_for_cleanup: std::sync::atomic::AtomicBool,
}

impl LeaseState {
    pub fn new(clientid: Clientid4) -> Self {
        Self {
            clientid,
            last_renew: RwLock::new(Instant::now()),
            reservations: AtomicU32::new(0),
            confirmed: std::sync::atomic::AtomicBool::new(false),
            marked_for_cleanup: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Check if lease is valid
    ///
    /// # NFS-Ganesha Reference
    /// From _valid_lease() in nfs4_lease.c:
    /// - Returns remaining seconds if valid
    /// - Returns 0 if expired
    /// - Reservations prevent expiration
    #[inline]
    pub fn is_valid(&self, lease_lifetime: Duration) -> bool {
        // If there are active reservations, lease is always valid
        if self.reservations.load(Ordering::Acquire) > 0 {
            return true;
        }

        match self.last_renew.read() {
            Ok(last) => last.elapsed() < lease_lifetime,
            Err(_) => false,
        }
    }

    /// Get remaining lease time in seconds
    pub fn remaining_secs(&self, lease_lifetime: Duration) -> u64 {
        if self.reservations.load(Ordering::Acquire) > 0 {
            return lease_lifetime.as_secs();
        }

        match self.last_renew.read() {
            Ok(last) => {
                let elapsed = last.elapsed();
                if elapsed >= lease_lifetime {
                    0
                } else {
                    (lease_lifetime - elapsed).as_secs()
                }
            }
            Err(_) => 0,
        }
    }

    /// Reserve lease (prevents expiration during operation)
    ///
    /// # NFS-Ganesha Reference
    /// From reserve_lease() in nfs4_lease.c
    #[inline]
    pub fn reserve(&self) {
        self.reservations.fetch_add(1, Ordering::AcqRel);
    }

    /// Release reservation and update lease time
    ///
    /// # NFS-Ganesha Reference
    /// From update_lease() in nfs4_lease.c:
    /// - Decrements reservation counter
    /// - Updates last_renew when last reservation is released
    #[inline]
    pub fn release_and_renew(&self) {
        let prev = self.reservations.fetch_sub(1, Ordering::AcqRel);
        // Renew lease when last reservation is released
        if prev == 1 {
            if let Ok(mut last) = self.last_renew.write() {
                *last = Instant::now();
            }
        }
    }

    /// Simple renew without reservation
    #[inline]
    pub fn renew(&self) {
        if let Ok(mut last) = self.last_renew.write() {
            *last = Instant::now();
        }
    }

    /// Confirm client
    #[inline]
    pub fn confirm(&self) {
        self.confirmed.store(true, Ordering::Release);
    }

    /// Check if confirmed
    #[inline]
    pub fn is_confirmed(&self) -> bool {
        self.confirmed.load(Ordering::Acquire)
    }

    /// Mark for delayed cleanup
    #[inline]
    pub fn mark_for_cleanup(&self) {
        self.marked_for_cleanup.store(true, Ordering::Release);
    }

    /// Check if marked for cleanup
    #[inline]
    pub fn is_marked_for_cleanup(&self) -> bool {
        self.marked_for_cleanup.load(Ordering::Acquire)
    }

    /// Get reservation count
    #[inline]
    pub fn reservation_count(&self) -> u32 {
        self.reservations.load(Ordering::Acquire)
    }
}

/// RAII guard for lease reservation
///
/// Automatically releases reservation when dropped.
/// This ensures lease is properly released even on error paths.
pub struct LeaseReservation {
    state: Arc<LeaseState>,
}

impl LeaseReservation {
    /// Create a new lease reservation
    pub fn new(state: Arc<LeaseState>) -> Self {
        state.reserve();
        Self { state }
    }

    /// Get the client ID
    #[inline]
    pub fn clientid(&self) -> Clientid4 {
        self.state.clientid
    }
}

impl Drop for LeaseReservation {
    fn drop(&mut self) {
        self.state.release_and_renew();
    }
}

/// Lease Manager - manages all client leases
///
/// # NFS-Ganesha Alignment
/// This combines functionality from:
/// - nfs4_lease.c: lease validation and reservation
/// - nfs_reaper_thread.c: expired client cleanup
///
/// # Thread Safety
/// - LeaseState uses atomic operations for reservations
/// - RwLock for lease time updates
pub struct LeaseManager {
    /// Client ID -> Lease State
    leases: RwLock<HashMap<Clientid4, Arc<LeaseState>>>,
    /// Lease lifetime
    lease_lifetime: Duration,
}

impl LeaseManager {
    pub fn new() -> Self {
        Self::with_lifetime(Duration::from_secs(DEFAULT_LEASE_TIME_SECS))
    }

    pub fn with_lifetime(lease_lifetime: Duration) -> Self {
        Self {
            leases: RwLock::new(HashMap::new()),
            lease_lifetime,
        }
    }

    /// Get lease lifetime
    #[inline]
    pub fn lease_lifetime(&self) -> Duration {
        self.lease_lifetime
    }

    /// Get lease lifetime in seconds
    #[inline]
    pub fn lease_lifetime_secs(&self) -> u64 {
        self.lease_lifetime.as_secs()
    }

    /// Register a new client lease
    pub fn register(&self, clientid: Clientid4) -> Arc<LeaseState> {
        let state = Arc::new(LeaseState::new(clientid));
        if let Ok(mut leases) = self.leases.write() {
            leases.insert(clientid, state.clone());
        }
        tracing::debug!("Registered lease for client {}", clientid);
        state
    }

    /// Get lease state for a client
    pub fn get(&self, clientid: Clientid4) -> Option<Arc<LeaseState>> {
        self.leases.read().ok()?.get(&clientid).cloned()
    }

    /// Reserve lease for an operation (RAII guard)
    ///
    /// Returns None if client doesn't exist or lease is expired.
    pub fn reserve(&self, clientid: Clientid4) -> Option<LeaseReservation> {
        let state = self.get(clientid)?;
        if !state.is_valid(self.lease_lifetime) {
            return None;
        }
        Some(LeaseReservation::new(state))
    }

    /// Simple renew without reservation
    pub fn renew(&self, clientid: Clientid4) -> bool {
        if let Some(state) = self.get(clientid) {
            state.renew();
            true
        } else {
            false
        }
    }

    /// Check if lease is valid
    pub fn is_valid(&self, clientid: Clientid4) -> bool {
        self.get(clientid)
            .map(|s| s.is_valid(self.lease_lifetime))
            .unwrap_or(false)
    }

    /// Confirm client lease
    pub fn confirm(&self, clientid: Clientid4) -> bool {
        if let Some(state) = self.get(clientid) {
            state.confirm();
            true
        } else {
            false
        }
    }

    /// Remove a client lease
    pub fn remove(&self, clientid: Clientid4) {
        if let Ok(mut leases) = self.leases.write() {
            leases.remove(&clientid);
        }
        tracing::debug!("Removed lease for client {}", clientid);
    }

    /// Check and collect expired clients
    ///
    /// # NFS-Ganesha Reference
    /// Similar to the reaper thread logic in nfs_reaper_thread.c
    pub fn collect_expired(&self) -> Vec<Clientid4> {
        match self.leases.read() {
            Ok(leases) => leases
                .iter()
                .filter(|(_, state)| state.is_confirmed() && !state.is_valid(self.lease_lifetime))
                .map(|(&id, _)| id)
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Get all client IDs
    pub fn all_clients(&self) -> Vec<Clientid4> {
        match self.leases.read() {
            Ok(leases) => leases.keys().copied().collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Get client count
    pub fn client_count(&self) -> usize {
        self.leases.read().map(|l| l.len()).unwrap_or(0)
    }
}

impl Default for LeaseManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_lease_state_basic() {
        let state = LeaseState::new(12345);
        let lifetime = Duration::from_secs(60);

        // Initially valid
        assert!(state.is_valid(lifetime));
        assert!(!state.is_confirmed());

        // Confirm
        state.confirm();
        assert!(state.is_confirmed());
    }

    #[test]
    fn test_lease_reservation() {
        let state = Arc::new(LeaseState::new(12345));
        let lifetime = Duration::from_secs(60);

        assert_eq!(state.reservation_count(), 0);

        // Reserve
        {
            let _guard = LeaseReservation::new(state.clone());
            assert_eq!(state.reservation_count(), 1);
            assert!(state.is_valid(lifetime));
        }

        // After drop, reservation is released
        assert_eq!(state.reservation_count(), 0);
    }

    #[test]
    fn test_lease_manager() {
        let manager = LeaseManager::new();

        // Register
        let state = manager.register(12345);
        assert!(manager.is_valid(12345));

        // Confirm
        assert!(manager.confirm(12345));
        assert!(state.is_confirmed());

        // Renew
        assert!(manager.renew(12345));

        // Remove
        manager.remove(12345);
        assert!(!manager.is_valid(12345));
    }

    #[test]
    fn test_lease_expiration() {
        let manager = LeaseManager::with_lifetime(Duration::from_millis(50));

        let state = manager.register(12345);
        state.confirm();

        // Initially valid
        assert!(manager.is_valid(12345));

        // Wait for expiration
        thread::sleep(Duration::from_millis(100));

        // Now expired
        assert!(!manager.is_valid(12345));

        // Collect expired
        let expired = manager.collect_expired();
        assert_eq!(expired, vec![12345]);
    }
}
