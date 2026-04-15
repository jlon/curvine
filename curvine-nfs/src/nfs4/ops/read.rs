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
use orpc::sys::DataSlice;
use std::io::Read;
use tokio_util::bytes::Bytes;

struct AnonReadGuard<'a> {
    delegations: &'a crate::nfs4::DelegationManager,
    fileid: Fileid4,
}

impl Drop for AnonReadGuard<'_> {
    fn drop(&mut self) {
        self.delegations.end_anon_op(self.fileid);
    }
}

/// READ operation handler
///
/// # Small File Cache (curvine-nfs extension for AI training)
///
/// This is NOT part of nfs-ganesha standard implementation. It's a curvine-nfs
/// extension optimized for AI training scenarios where many small files are
/// read repeatedly by a single client.
///
/// ## Cache Consistency Note
/// The cache uses TTL-based invalidation (default 10s). Within TTL period,
/// if another client modifies the file, this client may read stale data.
/// This is acceptable for AI training where files are typically read-only.
///
/// ## When to use
/// - AI training: reading many small config/metadata files repeatedly
/// - Single-client workloads where cache consistency is less critical
///
/// ## When NOT to use (disable via file_data_cache_size=0)
/// - Multi-writer workloads requiring strong consistency
/// - Files that change frequently
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
    if let Some((ds, handle)) = handler.pnfs_block_handle(fh)? {
        tracing::info!(
            "pNFS DS READ: worker_id={} block_id={} offset={} count={}",
            handle.worker_id,
            handle.block.id,
            offset,
            count
        );
        let (slices, eof) = ds.read(&handle, &stateid, offset, count).await?;
        return build_read_response(slices, eof);
    }

    let fileid = handler.fs.fh_to_fileid(fh)?;

    let status = handler.fs.get_status(fileid).await?;
    if status.file_type != curvine_common::state::FileType::File {
        return Err(Nfs4Status::Inval.into());
    }

    let anon_guard = if stateid.is_special() {
        handler.delegations.begin_anon_op(fileid);
        Some(AnonReadGuard {
            delegations: &handler.delegations,
            fileid,
        })
    } else {
        None
    };

    // Get max_cacheable_file_size from config
    let max_cacheable_size = handler.fs.config().max_cacheable_file_size;
    let file_size = status.len as u64;

    // Try small file cache first (only for full file reads of small files)
    // Cache hit: return data directly without backend I/O
    if offset == 0 && file_size <= max_cacheable_size {
        if let Some(cached_data) = handler.fs.get_file_data(fileid) {
            tracing::debug!(
                "READ: Small file cache hit for fileid={} size={}",
                fileid,
                cached_data.len()
            );
            let read_len = (count as usize).min(cached_data.len());
            let eof = read_len >= cached_data.len();
            let slice = DataSlice::bytes(Bytes::from(cached_data[..read_len].to_vec()));
            drop(anon_guard);
            return build_read_response(vec![slice], eof);
        }
    }

    let (adjusted_offset, adjusted_count, early_eof) =
        check_read_limits(offset, count, &status, handler)?;

    if early_eof {
        drop(anon_guard);
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

    // Cache small file data after read (only for full file reads starting at offset 0)
    // This benefits AI training workloads where small files are read repeatedly
    if offset == 0 && file_size <= max_cacheable_size && eof {
        let total_len: usize = slices.iter().map(|s| s.len()).sum();
        if total_len == file_size as usize {
            // Collect all slices into a single Vec for caching
            let mut cached_data = Vec::with_capacity(total_len);
            for slice in &slices {
                cached_data.extend_from_slice(slice.as_slice());
            }
            handler.fs.insert_file_data(fileid, cached_data);
            tracing::debug!(
                "READ: Cached small file fileid={} size={}",
                fileid,
                total_len
            );
        }
    }

    drop(anon_guard);
    build_read_response(slices, eof)
}

/// Check read limits and adjust parameters based on MaxRead and file size
///
/// Uses max_read_size from NfsGatewayConf instead of hardcoded value.
fn check_read_limits(
    offset: u64,
    count: u32,
    status: &curvine_common::state::FileStatus,
    handler: &CompoundHandler,
) -> Nfs4Result<(u64, u32, bool)> {
    // Use max_read_size from config
    let max_read = handler.fs.config().max_read_size as u64;
    let adjusted_count = if count as u64 > max_read {
        max_read as u32
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
