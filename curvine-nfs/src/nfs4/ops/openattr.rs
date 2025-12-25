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

//! NFSv4 OPENATTR Operation
//!
//! This file mirrors NFS-Ganesha's nfs4_op_openattr.c
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_openattr.c (82 lines)
//!
//! # Architecture Alignment
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_openattr()
//!   └─> return NFS4ERR_NOTSUPP
//!
//! Our Flow (same):
//! op_openattr()
//!   └─> return NFS4ERR_NOTSUPP
//! ```
//!
//! # Key Implementation Details
//!
//! OPENATTR is used to access named attributes directory.
//! Most implementations (including NFS-Ganesha) don't support this feature.
//! We return NOTSUPP to match NFS-Ganesha behavior.

use crate::nfs4::compound::{CompoundContext, CompoundHandler};
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;
use tracing::debug;

/// OPENATTR operation handler
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_openattr() at line 56
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// NFS4ERR_NOTSUPP (not supported)
pub async fn op_openattr(
    input: &mut impl Read,
    _ctx: &CompoundContext,
    _handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Read OPENATTR4args (line 57-58)
    let _createdir = input.read_u32::<BigEndian>()?;

    debug!("OPENATTR: not supported (same as NFS-Ganesha)");

    // Return NOTSUPP (line 62)
    Err(Nfs4Status::Notsupp.into())
}
