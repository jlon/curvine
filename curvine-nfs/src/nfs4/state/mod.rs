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

//! NFSv4.1 State Management
//!
//! This module manages all stateful aspects of NFSv4.1:
//! - Client state (registration, lease)
//! - Open state (file opens, share reservations)
//! - Lock state (byte-range locks)
//! - Lease state (lease reservation and expiration)

pub mod client;
pub mod deleg_reaper;
pub mod grace;
pub mod lease;
pub mod lock;
pub mod open;
pub mod persistence;
pub mod saver;

pub use client::ClientManager;
pub use deleg_reaper::DelegationReaperTask;
pub use grace::{GracePeriodConfig, GracePeriodManager, GracePeriodReaper};
pub use lease::{LeaseManager, LeaseReservation, LeaseState, DEFAULT_LEASE_TIME_SECS};
pub use lock::{LockEntry, LockManager, LockState, LockType4};
pub use open::{OpenManager, OpenState};
pub use persistence::{
    PersistedClient, PersistedLock, PersistedOpen, PersistenceConfig, RecoveryMetadata,
    StatePersistenceManager,
};
pub use saver::StateSaverTask;
