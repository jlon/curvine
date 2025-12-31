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

//! NFSv4 GETATTR Operation
//!
//! Retrieves file attributes based on client-specified attribute bitmap.

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::Nfs4Result;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;

/// GETATTR operation handler
pub async fn op_getattr(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let bitmap_len = input.read_u32::<BigEndian>()?;
    let mut requested_attrs = Vec::new();
    for _ in 0..bitmap_len {
        requested_attrs.push(input.read_u32::<BigEndian>()?);
    }

    if requested_attrs.is_empty() {
        let mut result = Vec::new();
        0u32.serialize(&mut result)?;
        0u32.serialize(&mut result)?;
        return Ok(result);
    }

    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    let status = handler.fs.get_status_cached(fileid).await?;

    let mut attrs = crate::nfs4::types::FileAttrs::from_status(&status);

    // If file has an active writer, use its current position as file size
    // This ensures GETATTR returns the correct size for files being written
    // (NFS-Ganesha: fsal_getattrs checks fd->buffer for uncommitted data)
    if let Some(open_file) = handler.fs.get_open_file(fileid) {
        if let Some(writer_pos) = open_file.get_writer_pos().await {
            attrs.size = writer_pos as u64;
            attrs.used = ((writer_pos + 511) / 512 * 512) as u64;
        }
    }

    let fattr = crate::nfs4::handlers::encode_fattr4(&attrs, &requested_attrs, Some(fh))?;

    let mut result = Vec::new();
    fattr.serialize(&mut result)?;

    Ok(result)
}
