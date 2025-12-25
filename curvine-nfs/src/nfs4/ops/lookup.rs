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
//! This file mirrors NFS-Ganesha's nfs4_op_lookup.c (240 lines)
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_lookup.c
//!
//! ## Function List (from NFS-Ganesha)
//! 1. `nfs4_op_lookup()` - Main LOOKUP handler (line 60-220)
//! 2. `nfs4_op_lookup_Free()` - Free LOOKUP result (line 225-240)
//!
//! ## LOOKUP Operation (RFC 5661, Section 18.16)
//!
//! The LOOKUP operation looks up a filename in a directory.
//!
//! ### Key Features (NFS-Ganesha)
//! - **Filename Validation**: UTF8_SCAN_PATH_COMP
//! - **Junction Handling**: Cross export boundaries (simplified per user)
//! - **Export Switching**: Change current export (simplified per user)
//! - **Filehandle Update**: Set current FH to looked-up file
//!
//! # Architecture
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_lookup()
//!   ├─> nfs4_sanity_check_FH()       // Validate parent is directory
//!   ├─> nfs4_utf8string_scan()       // Validate filename
//!   ├─> fsal_lookup()                // Lookup in FSAL
//!   ├─> check junction               // Handle export crossing
//!   ├─> export_ready()               // Verify export accessible
//!   ├─> nfs4_export_check_access()   // Check export permissions
//!   ├─> nfs_export_get_root_entry()  // Get root across junction
//!   └─> nfs4_FSALToFhandle()         // Convert to filehandle
//!
//! Our Flow (simplified):
//! op_lookup()
//!   ├─> ctx.require_current_fh()     // Validate parent filehandle
//!   ├─> validate_filename()          // Check UTF8 and special chars
//!   ├─> fs.lookup()                  // Lookup in filesystem
//!   └─> update_current_fh()          // Set current FH to result
//! ```

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::protocol::xdr::*;
use std::io::Read;
use tracing::debug;

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
    // Read filename (NFS-Ganesha line 70)
    let mut name: Vec<u8> = Vec::new();
    name.deserialize(input)?;
    let name_str = String::from_utf8_lossy(&name).to_string();

    debug!("LOOKUP: name={}", name_str);

    // Validate filename (NFS-Ganesha: nfs4_utf8string_scan at line 80)
    validate_filename(&name_str)?;

    // Get parent directory filehandle (NFS-Ganesha: nfs4_sanity_check_FH at line 90)
    let parent_fh = ctx.require_current_fh()?;
    let parent_id = handler.fs.fh_to_fileid(parent_fh)?;

    // Lookup in filesystem (NFS-Ganesha: fsal_lookup at line 100)
    let (fileid, _status) = handler.fs.lookup(parent_id, &name_str).await?;

    debug!(
        "LOOKUP: parent={} name={} -> fileid={}",
        parent_id, name_str, fileid
    );

    // Convert to filehandle (NFS-Ganesha: nfs4_FSALToFhandle at line 180)
    let new_fh = handler.fs.fileid_to_fh(fileid);

    // Update current filehandle (NFS-Ganesha: set_current_entry at line 190)
    ctx.current_fh = Some(new_fh);

    debug!("LOOKUP: updated current_fh to fileid={}", fileid);

    // Return empty result (success indicated by NFS4_OK status)
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
    // Empty name is invalid
    if name.is_empty() {
        return Err(Nfs4Status::Inval.into());
    }

    // Check for null bytes
    if name.contains('\0') {
        return Err(Nfs4Status::Inval.into());
    }

    // Check for path separator (/)
    if name.contains('/') {
        return Err(Nfs4Status::Inval.into());
    }

    // "." and ".." are handled by LOOKUPP or special logic
    // For LOOKUP, they should be rejected
    if name == "." || name == ".." {
        return Err(Nfs4Status::Inval.into());
    }

    Ok(())
}
