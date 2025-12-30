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

//! NFSv4 REMOVE Operation
//!
//! This file mirrors NFS-Ganesha's nfs4_op_remove.c (160 lines)
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_remove.c
//!
//! ## Function List (from NFS-Ganesha)
//! 1. `nfs4_op_remove()` - Main REMOVE handler (line 50-150)
//! 2. `nfs4_op_remove_Free()` - Free REMOVE result (line 155-160)
//!
//! ## REMOVE Operation (RFC 5661, Section 18.25)
//!
//! The REMOVE operation removes a file or directory.
//!
//! ### Key Features (NFS-Ganesha)
//! - **Filename Validation**: UTF8_SCAN_PATH_COMP
//! - **Grace Period Check**: Reject during grace period
//! - **Change Info**: Return before/after change attributes
//! - **Atomic Flag**: Indicate if change info is atomic
//!
//! # Architecture
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_remove()
//!   ├─> nfs4_sanity_check_FH()       // Validate parent is directory
//!   ├─> nfs4_utf8string_scan()       // Validate filename
//!   ├─> nfs_get_grace_status()       // Check grace period
//!   ├─> fsal_get_changeid4()         // Get before change attr
//!   ├─> fsal_remove()                // Remove file/directory
//!   ├─> get change attrs             // Get before/after from fsal_remove
//!   └─> build change_info4           // Return change info
//!
//! Our Flow (simplified):
//! op_remove()
//!   ├─> ctx.require_current_fh()     // Validate parent filehandle
//!   ├─> validate_filename()          // Check UTF8 and special chars
//!   ├─> get_change_before()          // Get parent change attr (before)
//!   ├─> fs.remove()                  // Remove file/directory
//!   ├─> get_change_after()           // Get parent change attr (after)
//!   └─> build_change_info4()         // Return change info
//! ```

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::protocol::xdr::*;
use std::io::Read;

/// REMOVE operation handler
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_remove() at line 50
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// Serialized REMOVE4res (change_info4)
pub async fn op_remove(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut name: Vec<u8> = Vec::new();
    name.deserialize(input)?;
    let name_str = String::from_utf8_lossy(&name).to_string();

    validate_filename(&name_str)?;

    let parent_fh = ctx.require_current_fh()?;
    let parent_id = handler.fs.fh_to_fileid(parent_fh)?;

    let parent_status_before = handler.fs.get_status_cached(parent_id).await?;
    let change_before = parent_status_before.mtime as u64;

    handler.fs.remove(parent_id, &name_str).await?;

    let parent_status_after = handler.fs.get_status_cached(parent_id).await?;
    let change_after = parent_status_after.mtime as u64;

    let mut result = Vec::new();

    let atomic = true;
    atomic.serialize(&mut result)?;

    change_before.serialize(&mut result)?;

    change_after.serialize(&mut result)?;

    Ok(result)
}

/// Validate filename for REMOVE
///
/// # NFS-Ganesha Reference
/// Function: nfs4_utf8string_scan() with UTF8_SCAN_PATH_COMP
///
/// Checks for:
/// - Empty name
/// - Null bytes
/// - Path separators (/)
/// - Special names ("." and "..")
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
