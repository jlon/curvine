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

//! NFSv4 READDIR Operation
//!
//! This file mirrors NFS-Ganesha's nfs4_op_readdir.c (700 lines)
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_readdir.c
//!
//! ## Function List (from NFS-Ganesha)
//! 1. `nfs4_readdir_callback()` - Entry callback (line 100-500)
//! 2. `nfs4_op_readdir()` - Main READDIR handler (line 550-700)
//! 3. `nfs4_op_readdir_Free()` - Free READDIR result (line 705-710)
//!
//! ## READDIR Operation (RFC 5661, Section 18.23)
//!
//! The READDIR operation reads directory entries.
//!
//! ### Key Features (NFS-Ganesha)
//! - **Cookie Mechanism**: Resume from previous position
//! - **Cookie Verifier**: Detect directory changes
//! - **Dircount/Maxcount**: Limit response size
//! - **Attribute Encoding**: Return requested attributes per entry
//! - **EOF Handling**: Indicate end of directory
//!
//! # Architecture
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_readdir()
//!   ├─> nfs4_sanity_check_FH()       // Validate directory
//!   ├─> bitmap4_to_attrmask_t()      // Parse requested attrs
//!   ├─> check cookie verifier        // Validate cookie
//!   ├─> fsal_readdir()               // Read entries
//!   │   └─> nfs4_readdir_callback()  // For each entry
//!   │       ├─> check space          // Ensure fits in response
//!   │       ├─> handle junction      // Cross export (simplified)
//!   │       ├─> xdr_encode_entry4()  // Encode entry
//!   │       └─> update counters      // Track dircount/maxcount
//!   └─> encode EOF                   // Final EOF marker
//!
//! Our Flow (simplified):
//! op_readdir()
//!   ├─> ctx.require_current_fh()     // Validate directory
//!   ├─> parse_readdir_args()         // Parse cookie, counts, attrs
//!   ├─> check_cookie_verifier()      // Validate cookie (optional)
//!   ├─> fs.readdir()                 // Read entries
//!   ├─> encode_entries()             // Encode each entry with attrs
//!   └─> encode_eof()                 // Final EOF marker
//! ```

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Read, Write};

/// READDIR operation handler
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_readdir() at line 550
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// Serialized READDIR4res (cookieverf + entries + eof)
pub async fn op_readdir(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let cookie = input.read_u64::<BigEndian>()?;
    let mut cookieverf: [u8; 8] = [0; 8];
    input.read_exact(&mut cookieverf)?;
    let dircount = input.read_u32::<BigEndian>()?;
    let maxcount = input.read_u32::<BigEndian>()?;

    let bitmap_len = input.read_u32::<BigEndian>()?;
    let mut attr_request = Vec::new();
    for _ in 0..bitmap_len {
        attr_request.push(input.read_u32::<BigEndian>()?);
    }

    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    if cookie == 1 || cookie == 2 {
        return Err(Nfs4Status::BadCookie.into());
    }

    let max_entries = if dircount > 0 {
        (dircount / 64).max(16) as usize
    } else {
        256
    };

    let (entries, eof) = handler.fs.readdir(fileid, cookie, max_entries).await?;

    let mut result = Vec::new();

    let response_verifier: [u8; 8] = [0; 8];
    result.write_all(&response_verifier)?;

    let mut total_size = 8;
    let max_response_size = maxcount as usize;

    for (entry_cookie, name, status) in entries {
        let entry_size_estimate = 8 + 4 + name.len() + 100 + 4;
        if total_size + entry_size_estimate > max_response_size {
            break;
        }

        if encode_entry(
            &mut result,
            entry_cookie,
            &name,
            &status,
            &attr_request,
            handler,
        )
        .is_err()
        {
            break;
        }

        total_size = result.len();
    }

    false.serialize(&mut result)?;

    eof.serialize(&mut result)?;

    Ok(result)
}

/// Encode a single directory entry
///
/// # NFS-Ganesha Reference
/// Function: xdr_encode_entry4() (in nfs4_xdr.c)
///
/// Encodes: nextentry(TRUE) + cookie + name + fattr4
fn encode_entry(
    output: &mut Vec<u8>,
    cookie: u64,
    name: &str,
    status: &curvine_common::state::FileStatus,
    attr_request: &[u32],
    _handler: &CompoundHandler,
) -> Nfs4Result<()> {
    true.serialize(output)?;

    cookie.serialize(output)?;

    name.as_bytes().to_vec().serialize(output)?;

    let attrs = crate::nfs4::types::FileAttrs::from_status(status);
    let fattr = crate::nfs4::handlers::encode_fattr4(&attrs, attr_request, None)?;
    fattr.serialize(output)?;

    Ok(())
}
