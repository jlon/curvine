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

//! NFSv4 RENAME Operation
//!
//! This file mirrors NFS-Ganesha's nfs4_op_rename.c (210 lines)
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_rename.c
//!
//! ## Function List (from NFS-Ganesha)
//! 1. `nfs4_op_rename()` - Main RENAME handler (line 60-200)
//! 2. `nfs4_op_rename_Free()` - Free RENAME result (line 205-210)
//!
//! ## RENAME Operation (RFC 5661, Section 18.26)
//!
//! The RENAME operation renames a file or directory.
//!
//! ### Key Features (NFS-Ganesha)
//! - **Filename Validation**: UTF8_SCAN_PATH_COMP for both names
//! - **Grace Period Check**: Reject during grace period
//! - **Cross-Export Check**: Source and target must be in same export
//! - **Change Info**: Return before/after for both source and target dirs
//! - **Atomic Flags**: Indicate if change info is atomic for each dir
//!
//! # Architecture
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_rename()
//!   ├─> nfs4_utf8string_scan()       // Validate oldname
//!   ├─> nfs4_utf8string_scan()       // Validate newname
//!   ├─> nfs4_sanity_check_FH()       // Validate current FH (target dir)
//!   ├─> nfs4_sanity_check_saved_FH() // Validate saved FH (source dir)
//!   ├─> check same export            // Ensure same export
//!   ├─> nfs_get_grace_status()       // Check grace period
//!   ├─> fsal_get_changeid4()         // Get before change attrs
//!   ├─> fsal_rename()                // Rename file/directory
//!   ├─> get change attrs             // Get before/after from fsal_rename
//!   └─> build change_info4 x2        // Return source and target change info
//!
//! Our Flow (simplified):
//! op_rename()
//!   ├─> validate_filenames()         // Check both oldname and newname
//!   ├─> get_source_and_target()      // Get saved FH and current FH
//!   ├─> get_change_before()          // Get both dirs' change attrs (before)
//!   ├─> fs.rename()                  // Rename file/directory
//!   ├─> get_change_after()           // Get both dirs' change attrs (after)
//!   └─> build_change_info4_pair()    // Return source and target change info
//! ```

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::protocol::xdr::*;
use std::io::Read;
use tracing::{debug, info};

/// RENAME operation handler
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_rename() at line 60
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// Serialized RENAME4res (source_cinfo + target_cinfo)
pub async fn op_rename(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Read oldname and newname (NFS-Ganesha line 70)
    let mut oldname: Vec<u8> = Vec::new();
    oldname.deserialize(input)?;
    let mut newname: Vec<u8> = Vec::new();
    newname.deserialize(input)?;

    let oldname_str = String::from_utf8_lossy(&oldname).to_string();
    let newname_str = String::from_utf8_lossy(&newname).to_string();

    info!("RENAME: {} -> {}", oldname_str, newname_str);

    // Validate both filenames (NFS-Ganesha: nfs4_utf8string_scan at line 80)
    validate_filename(&oldname_str)?;
    validate_filename(&newname_str)?;

    // Get source directory (saved FH) (NFS-Ganesha: nfs4_sanity_check_saved_FH at line 90)
    let src_fh = ctx.saved_fh.as_ref().ok_or(Nfs4Status::Nofilehandle)?;
    let src_parent = handler.fs.fh_to_fileid(src_fh)?;

    // Get target directory (current FH) (NFS-Ganesha: nfs4_sanity_check_FH at line 100)
    let dst_fh = ctx.require_current_fh()?;
    let dst_parent = handler.fs.fh_to_fileid(dst_fh)?;

    debug!(
        "RENAME: src_parent={} dst_parent={} oldname={} newname={}",
        src_parent, dst_parent, oldname_str, newname_str
    );

    // Get source directory change attribute before rename (NFS-Ganesha line 110)
    let src_status_before = handler.fs.get_status_cached(src_parent).await?;
    let src_change_before = src_status_before.mtime as u64;

    // Get target directory change attribute before rename (NFS-Ganesha line 120)
    let dst_status_before = handler.fs.get_status_cached(dst_parent).await?;
    let dst_change_before = dst_status_before.mtime as u64;

    // Rename file/directory (NFS-Ganesha: fsal_rename at line 130)
    handler
        .fs
        .rename(src_parent, &oldname_str, dst_parent, &newname_str)
        .await?;

    // Get source directory change attribute after rename (NFS-Ganesha line 140)
    let src_status_after = handler.fs.get_status_cached(src_parent).await?;
    let src_change_after = src_status_after.mtime as u64;

    // Get target directory change attribute after rename (NFS-Ganesha line 150)
    let dst_status_after = handler.fs.get_status_cached(dst_parent).await?;
    let dst_change_after = dst_status_after.mtime as u64;

    debug!(
        "RENAME: completed, src_change {} -> {}, dst_change {} -> {}",
        src_change_before, src_change_after, dst_change_before, dst_change_after
    );

    // Build response - source_cinfo + target_cinfo (NFS-Ganesha line 160)
    let mut result = Vec::new();

    // source_cinfo (change_info4)
    // atomic (bool) - TRUE if we got both before and after atomically
    let src_atomic = true; // Simplified: assume atomic
    src_atomic.serialize(&mut result)?;
    src_change_before.serialize(&mut result)?;
    src_change_after.serialize(&mut result)?;

    // target_cinfo (change_info4)
    let dst_atomic = true; // Simplified: assume atomic
    dst_atomic.serialize(&mut result)?;
    dst_change_before.serialize(&mut result)?;
    dst_change_after.serialize(&mut result)?;

    Ok(result)
}

/// Validate filename for RENAME
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

    // Cannot rename to/from "." or ".."
    if name == "." || name == ".." {
        return Err(Nfs4Status::Inval.into());
    }

    Ok(())
}
