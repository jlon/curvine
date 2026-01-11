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

//! NFSv4 ACCESS Operation
//!
//! This file mirrors NFS-Ganesha's nfs4_op_access.c (134 lines)
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_access.c
//!
//! ## Function List (from NFS-Ganesha)
//! 1. `nfs4_op_access()` - Main ACCESS handler (line 67-134)
//! 2. `nfs4_op_access_Free()` - Free ACCESS result (line 136-143)
//!
//! ## ACCESS Operation (RFC 5661, Section 18.1)
//!
//! The ACCESS operation checks the access rights that a user has to an object.
//! It does not check access based on the access mode of the file.
//!
//! ### Access Bits (NFSv4.0/4.1)
//! - `ACCESS4_READ` (0x00000001): Read data from file or read directory
//! - `ACCESS4_LOOKUP` (0x00000002): Look up a name in a directory
//! - `ACCESS4_MODIFY` (0x00000004): Modify file data
//! - `ACCESS4_EXTEND` (0x00000008): Extend file (write beyond EOF)
//! - `ACCESS4_DELETE` (0x00000010): Delete file or directory
//! - `ACCESS4_EXECUTE` (0x00000020): Execute file
//!
//! ### Extended Attributes (NFSv4.2+)
//! - `ACCESS4_XAREAD` (0x00000040): Read extended attributes
//! - `ACCESS4_XAWRITE` (0x00000080): Write extended attributes
//! - `ACCESS4_XALIST` (0x00000100): List extended attributes
//!
//! # Architecture
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_access()
//!   ├─> nfs4_sanity_check_FH()  // Validate filehandle
//!   ├─> nfs_access_op()         // Check access permissions
//!   └─> return supported + access bits
//!
//! Our Flow (same logic):
//! op_access()
//!   ├─> ctx.require_current_fh()  // Validate filehandle
//!   ├─> check_access()            // Check access permissions
//!   └─> return supported + access bits
//! ```

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::types::*;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;

// Access mode constants (RFC 5661, Section 18.1.3)
pub mod access_mode {
    pub const READ: u32 = 0x00000001;
    pub const LOOKUP: u32 = 0x00000002;
    pub const MODIFY: u32 = 0x00000004;
    pub const EXTEND: u32 = 0x00000008;
    pub const DELETE: u32 = 0x00000010;
    pub const EXECUTE: u32 = 0x00000020;

    // NFSv4.2 extended attributes (RFC 7862)
    pub const XAREAD: u32 = 0x00000040;
    pub const XAWRITE: u32 = 0x00000080;
    pub const XALIST: u32 = 0x00000100;

    // All basic access modes
    pub const ALL_BASIC: u32 = READ | LOOKUP | MODIFY | EXTEND | DELETE | EXECUTE;

    // All extended modes (NFSv4.2+)
    pub const ALL_EXTENDED: u32 = XAREAD | XAWRITE | XALIST;
}

/// ACCESS operation handler
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_access() at line 67
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// Serialized ACCESS4res
pub async fn op_access(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let access = input.read_u32::<BigEndian>()?;

    let max_access = if ctx.minor_version >= 2 {
        access_mode::ALL_BASIC | access_mode::ALL_EXTENDED
    } else {
        access_mode::ALL_BASIC
    };

    if access > max_access {
        return Err(Nfs4Status::Inval.into());
    }

    let fh = ctx.require_current_fh()?;

    let fileid = handler.fs.fh_to_fileid(fh)?;

    let (supported, allowed) = check_access(handler, fileid, access).await?;

    let mut result = Vec::new();
    supported.serialize(&mut result)?;
    allowed.serialize(&mut result)?;

    Ok(result)
}

/// Check access permissions for a file
///
/// # NFS-Ganesha Reference
/// Function: nfs_access_op() (called at line 102)
///
/// This function checks what access rights the current user has to the file.
/// It returns both the supported access modes and the allowed access modes.
///
/// **IMPORTANT**: Even if FSAL returns ACCESS error, we should return NFS4_OK
/// (NFS-Ganesha line 106-109). The client will see the denied access in the
/// 'allowed' field.
///
/// # Arguments
/// - handler: NFS4 handler
/// - fileid: File ID to check
/// - requested: Requested access modes
///
/// # Returns
/// (supported, allowed) tuple
async fn check_access(
    handler: &CompoundHandler,
    fileid: Fileid4,
    requested: u32,
) -> Nfs4Result<(u32, u32)> {
    let status = handler.fs.get_status(fileid).await?;

    let supported = match status.file_type {
        curvine_common::state::FileType::Dir => {
            access_mode::READ
                | access_mode::LOOKUP
                | access_mode::MODIFY
                | access_mode::EXTEND
                | access_mode::DELETE
        }
        curvine_common::state::FileType::File => {
            access_mode::READ | access_mode::MODIFY | access_mode::EXTEND | access_mode::EXECUTE
        }
        curvine_common::state::FileType::Link => access_mode::READ,
        _ => access_mode::READ,
    };

    let allowed = requested & supported;

    Ok((supported, allowed))
}
