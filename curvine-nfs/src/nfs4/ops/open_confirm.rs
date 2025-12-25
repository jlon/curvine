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

//! NFSv4 OPEN_CONFIRM Operation
//!
//! This file mirrors NFS-Ganesha's nfs4_op_open_confirm.c
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_open_confirm.c (169 lines)
//!
//! # Architecture Alignment
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_open_confirm()
//!   ├─> nfs4_sanity_check_FH()        // Check filehandle
//!   ├─> nfs4_Check_Stateid()          // Verify stateid
//!   ├─> Check_nfs4_seqid()            // Verify seqid
//!   ├─> Check so_confirmed flag       // Ensure not already confirmed
//!   ├─> Set so_confirmed = true       // Mark as confirmed
//!   └─> update_stateid()              // Increment seqid and return
//!
//! Our Flow (same logic):
//! op_open_confirm()
//!   ├─> ctx.require_current_fh()      // Same as nfs4_sanity_check_FH
//!   ├─> opens.get_state()             // Same as nfs4_Check_Stateid
//!   ├─> Verify seqid                  // Same as Check_nfs4_seqid
//!   ├─> Check confirmed flag          // Same logic
//!   ├─> Mark as confirmed             // Same as setting so_confirmed
//!   └─> Return updated stateid        // Same as update_stateid
//! ```
//!
//! # Key Implementation Details
//!
//! 1. **NFSv4.0 Only**: Returns NFS4ERR_NOTSUPP for NFSv4.1+
//! 2. **Stateid Handling**: Must accept stateid from OPEN response (any seqid)
//! 3. **Confirmation Flag**: Prevents double-confirmation
//! 4. **Seqid Increment**: Returns stateid with incremented seqid

use crate::nfs4::compound::{CompoundContext, CompoundHandler};
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::types::*;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;
use tracing::{debug, info};

/// OPEN_CONFIRM operation handler (NFSv4.0 only)
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_open_confirm() at line 56
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// Serialized OPEN_CONFIRM4res
pub async fn op_open_confirm(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // NFSv4.1+ does not use OPEN_CONFIRM (line 73-76)
    if ctx.minor_version > 0 {
        debug!("OPEN_CONFIRM not supported in NFSv4.{}", ctx.minor_version);
        return Err(Nfs4Status::Notsupp.into());
    }

    // Read OPEN_CONFIRM4args (line 57-58)
    let mut stateid = Stateid4::default();
    stateid.deserialize(input)?;
    let seqid = input.read_u32::<BigEndian>()?;

    info!(
        "OPEN_CONFIRM: stateid={:02x?} seqid={}",
        &stateid.other[..4],
        seqid
    );

    // Check filehandle (line 79-84)
    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    // Get state by stateid.other (line 87-96)
    // Note: We use get_state() not verify_stateid() because OPEN_CONFIRM
    // must accept the stateid from OPEN response regardless of seqid
    let open_state = handler
        .opens
        .get_state(&stateid)
        .ok_or(Nfs4Status::BadStateid)?;

    info!(
        "OPEN_CONFIRM: found state for file {} client {} access={:#x} deny={:#x}",
        open_state.fileid,
        open_state.clientid,
        open_state.get_access(),
        open_state.get_deny()
    );

    // Verify file matches (sanity check)
    if open_state.fileid != fileid {
        debug!(
            "OPEN_CONFIRM: fileid mismatch state={} current={}",
            open_state.fileid, fileid
        );
        return Err(Nfs4Status::BadStateid.into());
    }

    // Check if already confirmed (line 119-123)
    if open_state.is_confirmed() {
        info!("OPEN_CONFIRM: state already confirmed, returning BAD_STATEID");
        return Err(Nfs4Status::BadStateid.into());
    }

    // Mark as confirmed (line 126)
    open_state.set_confirmed(true);

    // Increment seqid and build response (line 129)
    let new_seqid = open_state.next_seqid();
    let confirmed_stateid = Stateid4::new(new_seqid, stateid.other);

    info!(
        "✅ OPEN_CONFIRM: confirmed file {} stateid={:02x?} new_seqid={}",
        fileid,
        &confirmed_stateid.other[..4],
        new_seqid
    );

    // Build response
    let mut result = Vec::new();
    confirmed_stateid.serialize(&mut result)?;

    Ok(result)
}
