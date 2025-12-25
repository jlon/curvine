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

//! RPC Protocol definitions
//!
//! This module contains the core RPC protocol types and XDR serialization
//! macros used by NFS, Mount, and Portmap protocols.
//!
//! # Modules
//!
//! - `xdr`: XDR (External Data Representation) serialization (RFC 1014)
//! - `rpc`: RPC protocol types and helpers (RFC 1057)

pub mod rpc;
pub mod xdr;

// Re-export commonly used items
pub use rpc::*;
pub use xdr::{XDRBoolUnion, XDREndian, XDREnumSerde, XDRStruct, XDR};
