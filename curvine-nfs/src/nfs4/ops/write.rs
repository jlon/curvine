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

//! NFSv4 WRITE Operation
//!
//! Writes data to a regular file. Supports stable write semantics:
//! - UNSTABLE4: Data cached, requires COMMIT for persistence
//! - DATA_SYNC4: Data synced, metadata may be delayed
//! - FILE_SYNC4: Both data and metadata synced
//!
//! Validates stateid, enforces write limits (MaxWrite/MaxOffsetWrite),
//! and returns write verifier for restart detection.

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Error, Nfs4Result, Nfs4Status};
use crate::nfs4::types::*;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;

struct AnonWriteGuard<'a> {
    delegations: &'a crate::nfs4::DelegationManager,
    fileid: Fileid4,
}

impl Drop for AnonWriteGuard<'_> {
    fn drop(&mut self) {
        self.delegations.end_anon_op(self.fileid);
    }
}

/// NFS4 stable write mode: UNSTABLE4
pub const UNSTABLE4: u32 = 0;

/// NFS4 stable write mode: DATA_SYNC4
pub const DATA_SYNC4: u32 = 1;

/// NFS4 stable write mode: FILE_SYNC4
pub const FILE_SYNC4: u32 = 2;

/// WRITE operation handler
pub async fn op_write(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut stateid = Stateid4::default();
    stateid.deserialize(input)?;
    let offset = input.read_u64::<BigEndian>()?;
    let stable = input.read_u32::<BigEndian>()?;

    let mut data: Vec<u8> = Vec::new();
    data.deserialize(input)?;

    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    tracing::info!(
        "WRITE: stateid={:?} offset={} len={} stable={} fileid={}",
        stateid,
        offset,
        data.len(),
        stable,
        fileid
    );

    let status = handler.fs.get_status(fileid).await?;
    if status.file_type != curvine_common::state::FileType::File {
        return Err(Nfs4Status::Inval.into());
    }

    let anon_guard = if stateid.is_special() {
        handler.delegations.begin_anon_op(fileid);
        Some(AnonWriteGuard {
            delegations: &handler.delegations,
            fileid,
        })
    } else {
        None
    };

    let adjusted_size = check_write_limits(offset, data.len() as u64, handler)?;
    if adjusted_size < data.len() {
        data.truncate(adjusted_size);
    }

    // Following nfs-ganesha: need_sync = (stable != UNSTABLE4)
    let need_sync = stable != UNSTABLE4;

    let (count, actual_synced) = if stateid.is_special() {
        let write_res = handler.fs.write(fileid, offset, data).await;
        let written = write_res?;
        // Invalidate small file data cache since file content has changed
        handler.fs.invalidate_file_data(fileid);
        (written, true) // Special stateid always syncs
    } else {
        let state = handler
            .opens
            .get_state(&stateid)
            .ok_or(Nfs4Status::BadStateid)?;

        tracing::info!(
            "WRITE: state.fileid={} state.path={} can_write={}",
            state.fileid,
            state.path.path(),
            state.can_write()
        );

        if !state.can_write() {
            return Err(Nfs4Status::Openmode.into());
        }

        let open_file = handler.fs.get_open_file(state.fileid).ok_or_else(|| {
            tracing::error!(
                "WRITE: OpenFile not found! state.fileid={} fileid_from_fh={} stateid={:?}",
                state.fileid,
                fileid,
                stateid
            );
            Nfs4Error::with_message(Nfs4Status::BadStateid, "OpenFile not found")
        })?;

        tracing::info!(
            "WRITE: Found OpenFile, calling write offset={} len={} need_sync={}",
            offset,
            data.len(),
            need_sync
        );

        let (written, actual_synced) = open_file.write(offset, data, need_sync).await?;

        // Invalidate small file data cache since file content has changed
        handler.fs.invalidate_file_data(state.fileid);

        tracing::info!(
            "WRITE: Successfully wrote {} bytes, synced={}",
            written,
            actual_synced
        );

        (written, actual_synced)
    };

    drop(anon_guard);
    build_write_response(count as usize, actual_synced, handler)
}

/// Check write limits and adjust size
///
/// Uses max_write_size from NfsGatewayConf instead of hardcoded value.
fn check_write_limits(_offset: u64, size: u64, handler: &CompoundHandler) -> Nfs4Result<usize> {
    // Use max_write_size from config
    let max_write = handler.fs.config().max_write_size as u64;

    let adjusted_size = if size > max_write {
        max_write as usize
    } else {
        size as usize
    };

    Ok(adjusted_size)
}

/// Build WRITE response with count, committed level, and write verifier
///
/// Following nfs-ganesha: committed is based on actual sync status, not requested stable
fn build_write_response(
    count: usize,
    actual_synced: bool,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut result = Vec::with_capacity(16);

    (count as u32).serialize(&mut result)?;

    // Following nfs-ganesha: return committed based on actual sync status
    let committed = if actual_synced { FILE_SYNC4 } else { UNSTABLE4 };
    committed.serialize(&mut result)?;

    let verifier = handler.boot_time.to_le_bytes();
    result.extend_from_slice(&verifier);

    Ok(result)
}
