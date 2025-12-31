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

//! NFSv4.1 Protocol Implementation
//!
//! This module implements NFSv4.1 protocol with the following key features:
//! - Session-based operation with exactly-once semantics
//! - COMPOUND operations for reduced network round-trips
//! - Stateful file operations (OPEN/CLOSE with stateid)
//! - Byte-range locking
//! - Delegation support (optional)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    NFSv4.1 Gateway                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Protocol Layer: COMPOUND Handler, XDR Codec                │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Session Layer: SessionManager, SlotManager, LeaseManager   │
//! ├─────────────────────────────────────────────────────────────┤
//! │  State Layer: ClientManager, OpenManager, LockManager       │
//! ├─────────────────────────────────────────────────────────────┤
//! │  FS Layer: Nfs4FileSystem (reuses FuseReader/FuseWriter)    │
//! └─────────────────────────────────────────────────────────────┘
//! ```

pub mod backchannel;
pub mod compound;
pub mod delegation;
pub mod error;
pub mod fs;
pub mod handlers;
pub mod ops; // NFSv4 operation handlers (mirrors nfs-ganesha/src/Protocols/NFS/)
pub mod session;
pub mod state;
pub mod types;
pub mod xdr;

// Re-export commonly used types
pub use backchannel::BackchannelManager;
pub use compound::CompoundHandler;
pub use delegation::{DelegationConfig, DelegationManager};
pub use error::{Nfs4Error, Nfs4Result};
pub use fs::Nfs4FileSystem;
pub use session::SessionManager;
pub use state::{ClientManager, LockManager, OpenManager};
pub use types::*;

/// NFSv4.1 program number
pub const NFS4_PROGRAM: u32 = 100003;

/// NFSv4.1 version number
pub const NFS4_VERSION: u32 = 4;

/// NFSv4.1 minor version
pub const NFS4_MINOR_VERSION: u32 = 1;

/// Default lease time in seconds
pub const DEFAULT_LEASE_TIME: u64 = 90;

/// Default number of slots per session (re-exported from session module)
/// Increased to 128 for better parallelism in NFSv4.1
pub use session::DEFAULT_SLOT_COUNT;

/// Maximum COMPOUND operations per request
pub const MAX_COMPOUND_OPS: usize = 64;
