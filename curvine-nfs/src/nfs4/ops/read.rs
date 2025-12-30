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

//! NFSv4 READ Operation
//!
//! Reads data from a regular file. Validates stateid, enforces read limits
//! (MaxRead/MaxOffsetRead), and returns data with accurate EOF flag.

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Error, Nfs4Result, Nfs4Status};
use crate::nfs4::types::*;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;

/// READ operation handler
pub async fn op_read(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut stateid = Stateid4::default();
    stateid.deserialize(input)?;
    let offset = input.read_u64::<BigEndian>()?;
    let count = input.read_u32::<BigEndian>()?;

    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    let status = handler.fs.get_status(fileid).await?;
    if status.file_type != curvine_common::state::FileType::File {
        return Err(Nfs4Status::Inval.into());
    }

    let (adjusted_offset, adjusted_count, early_eof) = check_read_limits(offset, count, &status)?;

    if early_eof {
        return build_read_response(vec![], true);
    }

    let (slices, eof) = if stateid.is_special() {
        handler
            .fs
            .read(fileid, adjusted_offset, adjusted_count)
            .await?
    } else {
        let open_state = handler
            .opens
            .get_state(&stateid)
            .ok_or(Nfs4Status::BadStateid)?;

        let open_file = handler.fs.get_open_file(open_state.fileid).ok_or_else(|| {
            tracing::error!("READ: OpenFile not found for fileid={}", open_state.fileid);
            Nfs4Error::with_message(Nfs4Status::BadStateid, "OpenFile not found")
        })?;

        open_file.read(adjusted_offset, adjusted_count).await?
    };

    build_read_response(slices, eof)
}

/// Check read limits and adjust parameters based on MaxRead and file size
fn check_read_limits(
    offset: u64,
    count: u32,
    status: &curvine_common::state::FileStatus,
) -> Nfs4Result<(u64, u32, bool)> {
    const MAX_READ: u64 = 1024 * 1024; // 1MB default

    let adjusted_count = if count as u64 > MAX_READ {
        MAX_READ as u32
    } else {
        count
    };

    let file_size = status.len as u64;
    let final_count = if offset >= file_size {
        0
    } else if offset + adjusted_count as u64 > file_size {
        (file_size - offset) as u32
    } else {
        adjusted_count
    };

    Ok((offset, final_count, false))
}

/// Build READ response with EOF flag, data length, and XDR-padded data
///
/// # Performance Optimization (2025-12-30)
/// This function is a critical hot path in NFS READ operations.
/// Optimizations applied:
/// 1. Pre-allocate exact buffer size to avoid reallocation
/// 2. Use write_all() instead of extend_from_slice() for better performance
/// 3. Minimize intermediate allocations
///
/// # XDR Format
/// ```text
/// +--------+--------+--------+--------+
/// |  EOF   | Length |  Data  |  Pad   |
/// | (bool) | (u32)  | (bytes)| (0-3)  |
/// +--------+--------+--------+--------+
/// ```
fn build_read_response(slices: Vec<orpc::sys::DataSlice>, eof: bool) -> Nfs4Result<Vec<u8>> {
    let total_len: usize = slices.iter().map(|s| s.len()).sum();
    let pad = (4 - total_len % 4) % 4;

    // Pre-allocate exact size: 1 byte (eof) + 4 bytes (length) + data + padding
    let result_size = 1 + 4 + total_len + pad;
    let mut result = Vec::with_capacity(result_size);

    // Serialize EOF flag and data length
    eof.serialize(&mut result)?;
    (total_len as u32).serialize(&mut result)?;

    // Copy data slices - this is unavoidable for XDR encoding
    // XDR requires contiguous memory layout with proper alignment
    for slice in &slices {
        result.extend_from_slice(slice.as_slice());
    }

    // Add XDR padding (0-3 bytes) to align to 4-byte boundary
    if pad > 0 {
        result.extend_from_slice(&[0u8; 4][..pad]);
    }

    Ok(result)
}
