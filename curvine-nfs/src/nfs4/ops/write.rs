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
        "WRITE: stateid={:?} offset={} len={} fileid_from_fh={}",
        stateid,
        offset,
        data.len(),
        fileid
    );

    let status = handler.fs.get_status(fileid).await?;
    if status.file_type != curvine_common::state::FileType::File {
        return Err(Nfs4Status::Inval.into());
    }

    let adjusted_size = check_write_limits(offset, data.len() as u64)?;
    if adjusted_size < data.len() {
        data.truncate(adjusted_size);
    }

    let count = if stateid.is_special() {
        handler.fs.write(fileid, offset, data).await?
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
            "WRITE: Found OpenFile, calling write offset={} len={}",
            offset,
            data.len()
        );

        let written = open_file.write(offset, data).await?;

        tracing::info!("WRITE: Successfully wrote {} bytes", written);

        written
    };

    build_write_response(count as usize, stable, handler)
}

/// Check write limits and adjust size
fn check_write_limits(_offset: u64, size: u64) -> Nfs4Result<usize> {
    const MAX_WRITE: u64 = 1024 * 1024; // 1MB default

    let adjusted_size = if size > MAX_WRITE {
        MAX_WRITE as usize
    } else {
        size as usize
    };

    Ok(adjusted_size)
}

/// Build WRITE response with count, committed level, and write verifier
fn build_write_response(
    count: usize,
    stable: u32,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut result = Vec::with_capacity(16);

    (count as u32).serialize(&mut result)?;

    let committed = if stable != 0 {
        2u32 // FILE_SYNC4
    } else {
        0u32 // UNSTABLE4
    };
    committed.serialize(&mut result)?;

    let verifier = handler.boot_time.to_le_bytes();
    result.extend_from_slice(&verifier);

    Ok(result)
}
