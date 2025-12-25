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

//! NFSv4 GETATTR Operation
//!
//! This file mirrors NFS-Ganesha's nfs4_op_getattr.c (250 lines)
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_getattr.c
//!
//! ## Function List (from NFS-Ganesha)
//! 1. `nfs4_op_getattr()` - Main GETATTR handler (line 70-230)
//! 2. `nfs4_op_getattr_Free()` - Free GETATTR result (line 240-250)
//!
//! ## GETATTR Operation (RFC 5661, Section 18.7)
//!
//! The GETATTR operation retrieves attributes for a file system object.
//!
//! ### Key Features (NFS-Ganesha)
//! - **Attribute Bitmap**: Client specifies which attributes to retrieve
//! - **Delegation Handling**: CB_GETATTR for write delegations (skipped per user)
//! - **Referral Support**: Returns FATTR4_RDATTR_ERROR for referrals
//! - **Response Size Check**: Ensures response fits in RPC limits
//!
//! # Architecture
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_getattr()
//!   ├─> nfs4_sanity_check_FH()    // Validate filehandle
//!   ├─> nfs4_Fattr_Check_Access() // Check attribute access
//!   ├─> bitmap4_to_attrmask_t()   // Convert bitmap to mask
//!   ├─> is_write_delegated()      // Check delegation (skipped)
//!   ├─> handle_deleg_getattr()    // CB_GETATTR if needed (skipped)
//!   ├─> file_To_Fattr()           // Get attributes from FSAL
//!   └─> check_resp_room()         // Check response size
//!
//! Our Flow (simplified):
//! op_getattr()
//!   ├─> ctx.require_current_fh()  // Validate filehandle
//!   ├─> parse_attr_request()      // Parse requested attributes
//!   ├─> get_file_attributes()     // Get attributes from fs
//!   └─> build_fattr4_response()   // Build response
//! ```

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::Nfs4Result;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;
use tracing::debug;

/// GETATTR operation handler
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_getattr() at line 70
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// Serialized GETATTR4res
pub async fn op_getattr(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Parse GETATTR4args - attribute request bitmap (NFS-Ganesha line 80)
    let bitmap_len = input.read_u32::<BigEndian>()?;
    let mut requested_attrs = Vec::new();
    for _ in 0..bitmap_len {
        requested_attrs.push(input.read_u32::<BigEndian>()?);
    }

    debug!(
        "GETATTR: bitmap_len={} attrs={:?}",
        bitmap_len, requested_attrs
    );

    // Sanity check: if no attributes requested, return empty (NFS-Ganesha line 90)
    if requested_attrs.is_empty() {
        let mut result = Vec::new();
        // Empty attrmask
        0u32.serialize(&mut result)?;
        // Empty attr_vals
        0u32.serialize(&mut result)?;
        return Ok(result);
    }

    // Get current filehandle (NFS-Ganesha: nfs4_sanity_check_FH at line 100)
    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    // Get file status (NFS-Ganesha: file_To_Fattr at line 180)
    let status = handler.fs.get_status_cached(fileid).await?;

    // Build fattr4 response using existing encode_fattr4 from handlers.rs
    // This function already handles all attribute encoding logic
    let attrs = crate::nfs4::types::FileAttrs::from_status(&status);
    let fattr = crate::nfs4::handlers::encode_fattr4(&attrs, &requested_attrs, Some(fh))?;

    // Serialize fattr4 to result
    let mut result = Vec::new();
    fattr.serialize(&mut result)?;

    debug!(
        "GETATTR: fileid={} result_len={} attrmask={:?}",
        fileid,
        result.len(),
        fattr.attrmask
    );

    Ok(result)
}
