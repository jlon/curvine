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
use crate::nfs4::error::Nfs4Result;
use crate::nfs4::types::*;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;

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
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let _seqid = input.read_u32::<BigEndian>()?;
    let mut stateid = Stateid4::default();
    stateid.deserialize(input)?;

    tracing::debug!("CLOSE: stateid={:?}", stateid);

    // Step 1: Check stateid correctness
    let open_state = handler.opens.verify_stateid(&stateid)?;

    let fileid = open_state.fileid;
    let clientid = open_state.clientid;
    let share_access = open_state.get_access();

    tracing::info!(
        "CLOSE: fileid={} path={} stateid={:?}",
        fileid,
        open_state.path.path(),
        stateid
    );

    // Update delegation heuristics (NFS-Ganesha: fds_num_opens tracking)
    let is_write_open = (share_access & 0x02) != 0;
    handler.delegations.record_close(fileid, is_write_open);

    // Step 2: Clean all associated lock states (NFS-Ganesha: state_unlock_all)
    // Release all locks held by this client on this file
    release_locks_for_open(handler, clientid, fileid);

    // Step 3: Delete the open state
    let closed_state = handler.opens.close(&stateid)?;

    // Step 4: Close file (FSAL closes fd if this is the last reference)
    tracing::debug!("CLOSE: fileid={} calling close_file", fileid);
    // ⏱️ PERF: Measure close_file time (includes complete/flush)
    let close_start = std::time::Instant::now();
    handler.fs.close_file(closed_state.fileid).await?;
    let close_elapsed = close_start.elapsed();
    tracing::warn!(
        "⏱️ PERF_CLOSE_FILE: fileid={} elapsed_us={}",
        fileid,
        close_elapsed.as_micros()
    );
    tracing::debug!("CLOSE: fileid={} completed successfully", fileid);

    // Build response
    let mut result = Vec::new();

    if ctx.minor_version > 0 {
        // NFSv4.1+: Return special invalid stateid to prevent re-use
        // NFS-Ganesha: memcpy all_zero + seqid = UINT32_MAX
        let invalid_stateid = Stateid4::new(0xFFFFFFFF, [0u8; 12]);
        ctx.current_stateid = None;
        invalid_stateid.serialize(&mut result)?;
    } else {
        // NFSv4.0: Return updated stateid with incremented seqid
        let mut response_stateid = stateid;
        response_stateid.seqid = response_stateid.seqid.wrapping_add(1);
        if response_stateid.seqid == 0 {
            response_stateid.seqid = 1;
        }
        ctx.current_stateid = Some(response_stateid);
        response_stateid.serialize(&mut result)?;
    }

    Ok(result)
}

/// Release all locks associated with an open state
///
/// NFS-Ganesha Reference: nfs4_op_close.c line 244-256
/// ```c
/// glist_for_each_safe(glist, glistn, &state_found->state_data.share.share_lockstates) {
///     state_t *lock_state = glist_entry(glist, state_t, state_data.lock.state_sharelist);
///     state_unlock_all(state_obj, lock_state);
///     state_del_locked(lock_state);
/// }
/// ```
fn release_locks_for_open(
    handler: &CompoundHandler,
    clientid: crate::nfs4::types::Clientid4,
    fileid: crate::nfs4::types::Fileid4,
) {
    // Get all lock states for this client
    let lock_states = handler.locks.export_locks();

    for lock_state in lock_states {
        // Only process locks belonging to this client
        if lock_state.owner.clientid != clientid {
            continue;
        }

        // Check if any lock entry is on this file
        let has_file_lock = {
            let entries = lock_state.lock_entries.read().unwrap();
            entries.iter().any(|e| e.fileid == fileid)
        };

        if has_file_lock {
            // Release all lock ranges on this file
            // NFS-Ganesha: state_unlock_all() releases all locks in the state
            match handler.locks.unlock(&lock_state.stateid, 0, u64::MAX) {
                Ok(updated_stateid) => {
                    let _ = handler.locks.free_stateid(&updated_stateid);
                    tracing::debug!(
                        "CLOSE: Released locks for file {} stateid={:02x?}",
                        fileid,
                        &updated_stateid.other[..4]
                    );
                }
                Err(e) => {
                    tracing::warn!("CLOSE: Failed to release lock for file {}: {:?}", fileid, e);
                }
            }
        }
    }
}
