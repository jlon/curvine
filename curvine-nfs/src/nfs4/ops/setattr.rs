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

//! NFSv4 SETATTR Operation
//!
//! This file mirrors NFS-Ganesha's nfs4_op_setattr.c (230 lines)
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_setattr.c
//!
//! ## Function List (from NFS-Ganesha)
//! 1. `nfs4_op_setattr()` - Main SETATTR handler (line 60-220)
//! 2. `nfs4_op_setattr_Free()` - Free SETATTR result (line 225-230)
//!
//! ## SETATTR Operation (RFC 5661, Section 18.30)
//!
//! The SETATTR operation sets attributes for a file system object.
//!
//! ### Key Features (NFS-Ganesha)
//! - **Stateid Verification**: Required for size changes
//! - **Grace Period Check**: Reject during grace period
//! - **Attribute Validation**: Check supported and writable attributes
//! - **Open Mode Check**: Size changes require WRITE access
//!
//! # Architecture
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_setattr()
//!   ├─> nfs4_sanity_check_FH()       // Validate filehandle
//!   ├─> nfs_get_grace_status()       // Check grace period
//!   ├─> nfs4_Fattr_Check_Access()    // Check writable attrs
//!   ├─> nfs4_Fattr_Supported()       // Check supported attrs
//!   ├─> nfs4_Fattr_To_FSAL_attr()    // Parse fattr4
//!   ├─> nfs4_Check_Stateid()         // Verify stateid (size change)
//!   ├─> check open mode              // WRITE access for size
//!   ├─> squash_setattr()             // Handle squashed creds
//!   └─> fsal_setattr()               // Apply attributes
//!
//! Our Flow (simplified):
//! op_setattr()
//!   ├─> ctx.require_current_fh()     // Validate filehandle
//!   ├─> parse_stateid()              // Read stateid
//!   ├─> parse_fattr4()               // Parse attributes
//!   ├─> verify_stateid()             // Verify for size changes
//!   ├─> parse_setattr_attrs()        // Extract attribute values
//!   └─> fs.setattr()                 // Apply attributes
//! ```

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::types::*;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;
use tracing::debug;

/// SETATTR operation handler
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_setattr() at line 60
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// Serialized SETATTR4res (attrsset bitmap)
pub async fn op_setattr(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Read stateid (NFS-Ganesha line 70)
    let mut stateid = Stateid4::default();
    stateid.deserialize(input)?;

    // Read fattr4 (NFS-Ganesha line 75)
    let mut fattr = Fattr4::default();
    fattr.deserialize(input)?;

    debug!(
        "SETATTR: stateid={:02x?} seqid={} attrmask={:?}",
        &stateid.other[..4],
        stateid.seqid,
        fattr.attrmask
    );
    tracing::info!(
        "SETATTR: stateid={:02x?} seqid={} attrmask={:?}",
        &stateid.other[..4],
        stateid.seqid,
        fattr.attrmask
    );

    // Get current filehandle (NFS-Ganesha: nfs4_sanity_check_FH at line 80)
    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    // Check if size is being set (NFS-Ganesha line 100)
    let setting_size = !fattr.attrmask.is_empty() && fattr.attrmask[0] & (1 << 4) != 0; // FATTR4_SIZE is bit 4

    // Verify stateid if setting size or if not special stateid (NFS-Ganesha line 110)
    if setting_size && !stateid.is_special() {
        // Verify stateid and check open mode (NFS-Ganesha line 120)
        let open_state = handler.opens.verify_stateid(&stateid)?;

        // Check if open state allows WRITE (required for size changes)
        // NFS-Ganesha line 140
        let share_access = open_state.get_access();
        if (share_access & 0x02) == 0 {
            // OPEN4_SHARE_ACCESS_WRITE not set
            return Err(Nfs4Status::Openmode.into());
        }
    }

    // Parse attributes from fattr4 (NFS-Ganesha: nfs4_Fattr_To_FSAL_attr at line 150)
    let (mode, uid, gid, size, atime, mtime) = parse_setattr_attrs(&fattr)?;

    debug!(
        "SETATTR: fileid={} mode={:?} size={:?} atime={:?} mtime={:?}",
        fileid, mode, size, atime, mtime
    );

    // Apply attributes (NFS-Ganesha: fsal_setattr at line 180)
    let _status = handler
        .fs
        .setattr(fileid, mode, uid, gid, size, atime, mtime)
        .await?;

    // Build response - attrsset bitmap (return what we set)
    // NFS-Ganesha line 200
    let mut result = Vec::new();
    fattr.attrmask.serialize(&mut result)?;

    debug!(
        "SETATTR: fileid={} completed, attrsset={:?}",
        fileid, fattr.attrmask
    );

    Ok(result)
}

/// Parse attributes from fattr4 for SETATTR
///
/// # NFS-Ganesha Reference
/// Function: nfs4_Fattr_To_FSAL_attr() (in nfs_convert.c)
///
/// Extracts attribute values from the XDR-encoded fattr4 structure.
/// Returns tuple of (mode, uid, gid, size, atime, mtime).
///
/// This function is also used by CREATE and OPEN operations for parsing createattrs.
pub(crate) fn parse_setattr_attrs(
    fattr: &Fattr4,
) -> Nfs4Result<(
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<u64>,
    Option<Nfstime4>,
    Option<Nfstime4>,
)> {
    let mut mode = None;
    let mut uid = None;
    let mut gid = None;
    let mut size = None;
    let mut atime = None;
    let mut mtime = None;

    if fattr.attrmask.is_empty() || fattr.attr_vals.is_empty() {
        return Ok((mode, uid, gid, size, atime, mtime));
    }

    let mut cursor = std::io::Cursor::new(&fattr.attr_vals);

    // Parse based on attrmask bits (must be in bit order)
    // Bitmap word 0 attributes (bits 0-31)
    if !fattr.attrmask.is_empty() {
        let word0 = fattr.attrmask[0];

        // FATTR4_SIZE (bit 4)
        if word0 & (1 << 4) != 0 {
            size = Some(cursor.read_u64::<BigEndian>()?);
        }
    }

    // Bitmap word 1 attributes (bits 32-63)
    if fattr.attrmask.len() > 1 {
        let word1 = fattr.attrmask[1];

        // FATTR4_MODE (bit 1 of word 1 = bit 33)
        if word1 & (1 << 1) != 0 {
            mode = Some(cursor.read_u32::<BigEndian>()?);
        }

        // FATTR4_OWNER (bit 4 of word 1 = bit 36)
        if word1 & (1 << 4) != 0 {
            let mut owner: Vec<u8> = Vec::new();
            owner.deserialize(&mut cursor)?;
            // Convert owner string to uid if numeric
            if let Ok(s) = String::from_utf8(owner.clone()) {
                uid = s.parse().ok();
            }
        }

        // FATTR4_OWNER_GROUP (bit 5 of word 1 = bit 37)
        if word1 & (1 << 5) != 0 {
            let mut group: Vec<u8> = Vec::new();
            group.deserialize(&mut cursor)?;
            // Convert group string to gid if numeric
            if let Ok(s) = String::from_utf8(group.clone()) {
                gid = s.parse().ok();
            }
        }

        // FATTR4_TIME_ACCESS_SET (bit 16 of word 1 = bit 48)
        if word1 & (1 << 16) != 0 {
            let set_it = cursor.read_u32::<BigEndian>()?;
            if set_it == 1 {
                // SET_TO_CLIENT_TIME4
                let mut time = Nfstime4::default();
                time.deserialize(&mut cursor)?;
                atime = Some(time);
            }
            // SET_TO_SERVER_TIME4 (0) - server sets current time
        }

        // FATTR4_TIME_MODIFY_SET (bit 22 of word 1 = bit 54)
        if word1 & (1 << 22) != 0 {
            let set_it = cursor.read_u32::<BigEndian>()?;
            if set_it == 1 {
                // SET_TO_CLIENT_TIME4
                let mut time = Nfstime4::default();
                time.deserialize(&mut cursor)?;
                mtime = Some(time);
            }
        }
    }

    Ok((mode, uid, gid, size, atime, mtime))
}
