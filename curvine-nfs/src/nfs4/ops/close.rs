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

//! NFSv4 CLOSE Operation
//!
//! This file mirrors NFS-Ganesha's nfs4_op_close.c
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_close.c
//!
//! Key functions:
//! - nfs4_op_close() - Main CLOSE handler (line 140)
//!
//! # Architecture
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_close()
//!   ├─> nfs4_Check_Stateid()      // Verify stateid
//!   ├─> state_unlock_all()        // Release all locks
//!   ├─> state_del_locked()        // Delete state
//!   └─> fsal_close2()             // Close fd (if last reference)
//!
//! Our Flow:
//! op_close()
//!   ├─> OpenManager::verify_stateid()  // Verify stateid
//!   ├─> OpenManager::close()           // Delete state
//!   └─> Nfs4FileSystem::close_file()   // Close OpenFile (if last ref)
//!         └─> OpenFile::complete()     // Commit data
//! ```

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::types::*;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;
use tracing::info;

/// CLOSE operation handler
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_close() at line 140
///
/// # Key Steps (from NFS-Ganesha)
/// 1. Check stateid correctness (nfs4_Check_Stateid)
/// 2. Clean all associated lock states (state_unlock_all)
/// 3. Delete the state (state_del_locked)
/// 4. FSAL closes fd if this is the last reference
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// Serialized CLOSE4res
pub async fn op_close(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Parse CLOSE4args
    let seqid = input.read_u32::<BigEndian>()?;
    let mut stateid = Stateid4::default();
    stateid.deserialize(input)?;

    info!(
        "CLOSE: seqid={} stateid={:02x?}",
        seqid,
        &stateid.other[..4]
    );

    // Step 1: Get state by stateid.other (NFS-Ganesha: nfs4_Check_Stateid with STATEID_SPECIAL_FOR_CLOSE)
    // For CLOSE, we accept any seqid - only check stateid.other
    let open_state = handler
        .opens
        .get_state(&stateid)
        .ok_or(Nfs4Status::BadStateid)?;

    let fileid = open_state.fileid;

    info!(
        "CLOSE: Verified stateid for fileid={} owner={:02x?}",
        fileid,
        &open_state.owner_val[..open_state.owner_val.len().min(8)]
    );

    // Step 2: Delete state (NFS-Ganesha: state_del_locked)
    // This removes the state from all HashMaps
    let closed_state = handler.opens.close(&stateid)?;

    // Step 3: Close file (NFS-Ganesha: fsal_close2)
    // This decrements OpenFile ref_count and calls complete() if last reference
    handler.fs.close_file(closed_state.fileid).await?;

    // Step 4: Build response
    let mut result = Vec::new();

    // Return special stateid for NFSv4.1+ (NFS-Ganesha line 264)
    if ctx.minor_version > 0 {
        // Special invalid stateid (all zeros with seqid=0xFFFFFFFF)
        let invalid_stateid = Stateid4::new(0xFFFFFFFF, [0u8; 12]);
        invalid_stateid.serialize(&mut result)?;
    } else {
        // NFSv4.0: increment seqid
        let mut response_stateid = stateid;
        response_stateid.seqid = response_stateid.seqid.wrapping_add(1);
        if response_stateid.seqid == 0 {
            response_stateid.seqid = 1;
        }
        response_stateid.serialize(&mut result)?;
    }

    info!(
        "CLOSE SUCCESS: fileid={} stateid={:02x?}",
        fileid,
        &stateid.other[..4]
    );

    Ok(result)
}
