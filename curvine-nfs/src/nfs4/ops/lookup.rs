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

//! NFSv4 LOOKUP Operation
//!
//! Looks up a filename in a directory and sets the current filehandle to the result.

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::protocol::xdr::*;
use std::io::Read;

/// LOOKUP operation handler
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_lookup() at line 60
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context (mutable to update current FH)
/// - handler: NFS4 handler
///
/// # Returns
/// Empty result (success updates ctx.current_fh)
pub async fn op_lookup(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut name: Vec<u8> = Vec::new();
    name.deserialize(input)?;
    let name_str = String::from_utf8_lossy(&name).to_string();

    validate_filename(&name_str)?;

    let parent_fh = ctx.require_current_fh()?;
    let parent_id = handler.fs.fh_to_fileid(parent_fh)?;

    let (fileid, _status) = handler.fs.lookup(parent_id, &name_str).await?;

    let new_fh = handler.fs.fileid_to_fh(fileid);

    ctx.current_fh = Some(new_fh);

    Ok(Vec::new())
}

/// Validate filename for LOOKUP
///
/// # NFS-Ganesha Reference
/// Function: nfs4_utf8string_scan() with UTF8_SCAN_PATH_COMP
///
/// Checks for:
/// - Empty name
/// - Null bytes
/// - Path separators (/)
/// - Special names ("." and ".." are handled separately)
fn validate_filename(name: &str) -> Nfs4Result<()> {
    if name.is_empty() {
        return Err(Nfs4Status::Inval.into());
    }

    if name.contains('\0') {
        return Err(Nfs4Status::Inval.into());
    }

    if name.contains('/') {
        return Err(Nfs4Status::Inval.into());
    }

    if name == "." || name == ".." {
        return Err(Nfs4Status::Inval.into());
    }

    Ok(())
}
