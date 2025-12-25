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

//! NFSv4 COMMIT Operation
//!
//! This file mirrors NFS-Ganesha's nfs4_op_commit.c (160 lines)
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_commit.c
//!
//! ## Function List (from NFS-Ganesha)
//! 1. `nfs4_op_commit()` - Main COMMIT handler (line 68-127)
//! 2. `nfs4_op_commit_Free()` - Free COMMIT result (line 129-137)
//! 3. `op_dscommit()` - pNFS data server commit (line 139-160)
//!
//! ## COMMIT Operation (RFC 5661, Section 18.3)
//!
//! The COMMIT operation forces or flushes data to stable storage that was
//! previously written with a WRITE operation which had the stable field set
//! to UNSTABLE4.
//!
//! ### Write Verifier
//! The server returns a write verifier that the client can use to determine
//! whether the server has restarted between the WRITE and COMMIT operations.
//!
//! # Architecture
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_commit()
//!   ├─> nfs4_sanity_check_FH()  // Validate filehandle (REGULAR_FILE)
//!   ├─> fsal_commit()           // Flush data to storage
//!   └─> get_write_verifier()    // Get server verifier
//!
//! Our Flow (same logic):
//! op_commit()
//!   ├─> ctx.require_current_fh()  // Validate filehandle
//!   ├─> commit_file()             // Flush data to storage
//!   └─> get_write_verifier()      // Get server verifier
//! ```
//!
//! ## NFSv4 Write Semantics
//!
//! 1. **WRITE (UNSTABLE4)**: Data buffered in Writer, not yet committed
//! 2. **COMMIT**: Calls `Writer::flush()` to flush all buffered data (keeps Writer open)
//! 3. **CLOSE**: Calls `Writer::complete()` to close Writer and release resources
//! 4. **Write Verifier**: Server boot time, used to detect server restarts

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Error, Nfs4Result, Nfs4Status};
use crate::nfs4::types::Fileid4;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;
use tracing::info;

/// COMMIT operation handler
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_commit() at line 68
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// Serialized COMMIT4res
pub async fn op_commit(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Parse COMMIT4args (NFS-Ganesha line 69-70)
    let offset = input.read_u64::<BigEndian>()?;
    let count = input.read_u32::<BigEndian>()?;
    
    info!("COMMIT: offset={} count={}", offset, count);

    // Get current filehandle (NFS-Ganesha: nfs4_sanity_check_FH at line 82)
    // COMMIT is done only on a file (REGULAR_FILE)
    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    // Verify it's a regular file
    let status = handler.fs.get_status(fileid).await?;
    if status.file_type != curvine_common::state::FileType::File {
        return Err(Nfs4Status::Inval.into());
    }

    // Perform the commit operation (NFS-Ganesha: fsal_commit at line 87-88)
    commit_file(handler, fileid, offset, count).await?;

    // Get write verifier (NFS-Ganesha: get_write_verifier at line 95-99)
    let verifier = get_write_verifier(handler);

    // Build response
    let mut result = Vec::new();
    verifier.serialize(&mut result)?;

    info!(
        "COMMIT SUCCESS: offset={} count={} verifier={:?}",
        offset, count, verifier
    );

    Ok(result)
}

/// Commit file data to stable storage
///
/// # NFS-Ganesha Reference
/// Function: fsal_commit() (called at line 87)
/// VFS implementation: vfs_commit2() in FSAL_VFS/file.c
///
/// NFS-Ganesha calls fsync() to flush data to stable storage WITHOUT closing
/// the file descriptor. This is critical - COMMIT should only flush, not close.
///
/// # Semantics
/// - flush(): Flushes buffered data to storage, keeps Writer open (like fsync)
/// - complete(): Closes Writer and releases resources (like close)
///
/// COMMIT must use flush(), not complete(). complete() is only called on CLOSE.
///
/// # Arguments
/// - handler: NFS4 handler
/// - fileid: File ID to commit
/// - offset: Starting offset (0 means entire file)
/// - count: Number of bytes (0 means to EOF)
///
/// # Returns
/// Ok(()) on success
async fn commit_file(
    handler: &CompoundHandler,
    fileid: Fileid4,
    offset: u64,
    count: u32,
) -> Nfs4Result<()> {
    // Get the OpenFile for this file
    let open_file = handler.fs.get_open_file(fileid);

    if let Some(open_file) = open_file {
        // File is currently open, flush the Writer
        // NFS-Ganesha: vfs_commit2() calls fsync(fd), NOT close(fd)
        // We use flush() which flushes data but keeps Writer open
        // Clone NfsWriter to avoid holding lock across await
        let writer = {
            let writer_guard = open_file.writer.read().unwrap();
            writer_guard.clone()
        };

        if let Some(writer) = writer {
            writer.flush().await.map_err(|e| {
                tracing::error!("Failed to flush file {}: {:?}", fileid, e);
                Nfs4Error::from(Nfs4Status::Io)
            })?;
            tracing::info!("✅ Flushed Writer for file {} (COMMIT)", fileid);
        }
    } else {
        // File is not currently open
        // For NFSv4, COMMIT without an open file is a no-op
        // (data should have been committed on CLOSE)
        tracing::debug!(
            "COMMIT on closed file {} (offset={} count={}) - no-op",
            fileid,
            offset,
            count
        );
    }

    Ok(())
}

/// Get write verifier
///
/// # NFS-Ganesha Reference
/// Function: get_write_verifier() (called at line 95)
///
/// The write verifier is used by clients to detect server restarts.
/// If the verifier changes between WRITE and COMMIT, the client knows
/// the server restarted and the data may have been lost.
///
/// # Implementation
/// We use the server boot time as the verifier (8 bytes).
///
/// # Arguments
/// - handler: NFS4 handler
///
/// # Returns
/// 8-byte write verifier
fn get_write_verifier(handler: &CompoundHandler) -> [u8; 8] {
    // Use server boot time as verifier
    // This is stored in the handler when the server starts
    let boot_time = handler.boot_time;
    boot_time.to_le_bytes()
}
