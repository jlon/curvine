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
//! Forces buffered data to stable storage. The COMMIT operation flushes data
//! that was previously written with UNSTABLE4 semantics.
//!
//! Key semantics:
//! - Uses `Writer::flush()` to persist data without closing the Writer
//! - Returns write verifier (server boot time) for restart detection
//! - Only valid on regular files

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Error, Nfs4Result, Nfs4Status};
use crate::nfs4::types::Fileid4;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;

/// COMMIT operation handler
pub async fn op_commit(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let offset = input.read_u64::<BigEndian>()?;
    let count = input.read_u32::<BigEndian>()?;

    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    let status = handler.fs.get_status(fileid).await?;
    if status.file_type != curvine_common::state::FileType::File {
        return Err(Nfs4Status::Inval.into());
    }

    commit_file(handler, fileid, offset, count).await?;

    let verifier = get_write_verifier(handler);
    let mut result = Vec::new();
    verifier.serialize(&mut result)?;

    Ok(result)
}

/// Commit file data to stable storage
///
/// Uses `Writer::flush()` to persist data without closing the Writer.
/// This matches fsync() semantics - data is persisted but the file remains open.
async fn commit_file(
    handler: &CompoundHandler,
    fileid: Fileid4,
    _offset: u64,
    _count: u32,
) -> Nfs4Result<()> {
    let open_file = handler.fs.get_open_file(fileid);

    if let Some(open_file) = open_file {
        // Clone writer to avoid holding lock across await
        let writer = {
            let writer_guard = open_file.writer.read().unwrap();
            writer_guard.clone()
        };

        if let Some(writer) = writer {
            writer.flush().await.map_err(|e| {
                tracing::error!("COMMIT: Failed to flush file {}: {:?}", fileid, e);
                Nfs4Error::from(Nfs4Status::Io)
            })?;
        }
    }

    Ok(())
}

/// Get write verifier for restart detection
///
/// Returns server boot time as 8-byte verifier. Clients compare this
/// between WRITE and COMMIT to detect server restarts.
fn get_write_verifier(handler: &CompoundHandler) -> [u8; 8] {
    handler.boot_time.to_le_bytes()
}
