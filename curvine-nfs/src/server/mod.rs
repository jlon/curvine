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

//! NFS Server infrastructure
//!
//! This module contains the TCP server, RPC context, and wire protocol handling.
//!
//! # Modules
//!
//! - `tcp`: TCP listener and connection handling
//! - `context`: RPC request context
//! - `wire`: RPC wire protocol (record marking, message handling)
//! - `transaction`: Transaction tracking for retransmission detection

pub mod context;
pub mod tcp;
pub mod tcp_tuning;
pub mod transaction;
pub mod wire;

// Re-export commonly used items
pub use context::RPCContext;
pub use tcp::{NFSTcp, NFSTcpListener};
pub use transaction::TransactionTracker;
