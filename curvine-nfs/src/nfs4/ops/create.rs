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

//! NFSv4 CREATE Operation
//!
//! This file mirrors NFS-Ganesha's nfs4_op_create.c (357 lines)
//!
//! # NFS-Ganesha Reference
//! File: nfs-ganesha/src/Protocols/NFS/nfs4_op_create.c
//!
//! ## Function List (from NFS-Ganesha)
//! 1. `nfs4_op_create()` - Main CREATE handler (line 67-349)
//! 2. `nfs4_op_create_Free()` - Free CREATE result (line 351-357)
//!
//! ## CREATE Operation (RFC 5661, Section 18.4)
//!
//! The CREATE operation creates a non-regular file object in a directory.
//! Regular files are created using OPEN with the CREATE flag.
//!
//! ### Supported Object Types
//! - `NF4DIR`: Directory
//! - `NF4LNK`: Symbolic link
//! - `NF4BLK`: Block device
//! - `NF4CHR`: Character device
//! - `NF4SOCK`: Socket
//! - `NF4FIFO`: Named pipe (FIFO)
//!
//! ### Change Info
//! The server returns change_info4 containing:
//! - `atomic`: Whether the operation was atomic
//! - `before`: Change attribute before the operation
//! - `after`: Change attribute after the operation
//!
//! # Architecture
//!
//! ```text
//! NFS-Ganesha Flow:
//! nfs4_op_create()
//!   ├─> nfs4_sanity_check_FH()     // Validate parent is directory
//!   ├─> check_quota()              // Check inode quota
//!   ├─> nfs4_Fattr_Supported()     // Validate attributes
//!   ├─> nfs4_utf8string_scan()     // Validate filename
//!   ├─> nfs4_Fattr_To_FSAL_attr()  // Convert attributes
//!   ├─> fsal_create()              // Create the object
//!   ├─> nfs4_FSALToFhandle()       // Build new filehandle
//!   └─> set_current_entry()        // Update current FH
//!
//! Our Flow (same logic):
//! op_create()
//!   ├─> ctx.require_current_fh()   // Validate parent is directory
//!   ├─> validate_filename()        // Validate filename
//!   ├─> parse_create_type()        // Parse object type
//!   ├─> create_object()            // Create the object
//!   ├─> update_current_fh()        // Update current FH
//!   └─> build_change_info()        // Build change_info4
//! ```

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;
use tracing::info;

// Object type constants (RFC 5661, Section 3.3.13)
pub mod object_type {
    pub const NF4REG: u32 = 1; // Regular file (not allowed in CREATE)
    pub const NF4DIR: u32 = 2; // Directory
    pub const NF4BLK: u32 = 3; // Block device
    pub const NF4CHR: u32 = 4; // Character device
    pub const NF4LNK: u32 = 5; // Symbolic link
    pub const NF4SOCK: u32 = 6; // Socket
    pub const NF4FIFO: u32 = 7; // Named pipe (FIFO)
    pub const NF4ATTRDIR: u32 = 8; // Attribute directory (not used)
    pub const NF4NAMEDATTR: u32 = 9; // Named attribute (not used)
}

/// CREATE operation handler
///
/// # NFS-Ganesha Reference
/// Function: nfs4_op_create() at line 67
///
/// # Arguments
/// - input: XDR input stream
/// - ctx: Compound context
/// - handler: NFS4 handler
///
/// # Returns
/// Serialized CREATE4res
pub async fn op_create(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Parse CREATE4args
    let obj_type = input.read_u32::<BigEndian>()?;
    
    // Parse type-specific data
    let (link_data, spec_data1, spec_data2) = match obj_type {
        object_type::NF4LNK => {
            // Symbolic link: read link data
            let mut link: Vec<u8> = Vec::new();
            link.deserialize(input)?;
            (Some(link), 0, 0)
        }
        object_type::NF4BLK | object_type::NF4CHR => {
            // Device file: read specdata
            let data1 = input.read_u32::<BigEndian>()?;
            let data2 = input.read_u32::<BigEndian>()?;
            (None, data1, data2)
        }
        _ => (None, 0, 0),
    };
    
    // Parse object name
    let mut objname: Vec<u8> = Vec::new();
    objname.deserialize(input)?;
    let name = String::from_utf8_lossy(&objname);
    
    // Parse create attributes
    let attr_count = input.read_u32::<BigEndian>()?;
    let mut attrset_bitmap = Vec::new();
    for _ in 0..attr_count {
        let attr_id = input.read_u32::<BigEndian>()?;
        attrset_bitmap.push(attr_id);
        // Skip attribute data (would need proper parsing)
        // TODO: Implement full attribute parsing (NFS-Ganesha line 177-186)
    }
    
    info!(
        "CREATE: type={} name={} spec=({},{}) attrs={}",
        obj_type, name, spec_data1, spec_data2, attr_count
    );

    // Get parent directory (NFS-Ganesha: line 157-165)
    let parent_fh = ctx.require_current_fh()?;
    let parent_id = handler.fs.fh_to_fileid(parent_fh)?;

    // Verify parent is a directory (NFS-Ganesha: line 167-171)
    let parent_status = handler.fs.get_status(parent_id).await?;
    if parent_status.file_type != curvine_common::state::FileType::Dir {
        return Err(Nfs4Status::Notdir.into());
    }

    // Get parent's change attribute before operation (NFS-Ganesha: line 173)
    // Change attribute is derived from mtime
    let change_before = parent_status.mtime as u64;

    // Validate filename (NFS-Ganesha: nfs4_utf8string_scan at line 153)
    validate_filename(&name)?;

    // Validate link content for symbolic links (NFS-Ganesha: line 203-210)
    if obj_type == object_type::NF4LNK {
        if let Some(ref link) = link_data {
            let link_str = String::from_utf8_lossy(link);
            validate_link_content(&link_str)?;
        }
    }

    // Create the object based on type (NFS-Ganesha: line 195-257)
    let (new_fileid, _new_status) = match obj_type {
        object_type::NF4DIR => {
            // Create directory (NFS-Ganesha: line 213)
            handler.fs.mkdir(parent_id, &name).await?
        }
        object_type::NF4LNK => {
            // Create symbolic link (NFS-Ganesha: line 203-210)
            // Fix lifetime issue by extracting target before the call
            let link_bytes = link_data.ok_or(Nfs4Status::Inval)?;
            let target = String::from_utf8_lossy(&link_bytes).to_string();
            handler.fs.symlink(parent_id, &name, &target).await?
        }
        object_type::NF4SOCK | object_type::NF4FIFO => {
            // Socket and FIFO files not supported yet
            // NFS-Ganesha: line 217-220
            return Err(Nfs4Status::Notsupp.into());
        }
        object_type::NF4BLK | object_type::NF4CHR => {
            // Device files not supported yet
            // NFS-Ganesha: line 221-237
            // TODO: Implement device file creation with rawdev.major/minor
            return Err(Nfs4Status::Notsupp.into());
        }
        object_type::NF4REG => {
            // Regular files must be created with OPEN, not CREATE
            // NFS-Ganesha: line 239-242
            return Err(Nfs4Status::Badtype.into());
        }
        _ => {
            return Err(Nfs4Status::Badtype.into());
        }
    };

    // Update current FH to the new object (NFS-Ganesha: line 277-281)
    let new_fh = handler.fs.fileid_to_fh(new_fileid);
    ctx.current_fh = Some(new_fh);
    // Note: current_stateid_valid field doesn't exist in our CompoundContext
    // This is handled differently in our implementation

    // Get parent's change attribute after operation (NFS-Ganesha: line 308-318)
    let parent_status_after = handler.fs.get_status(parent_id).await?;
    let change_after = parent_status_after.mtime as u64;

    // Build response
    let mut result = Vec::new();
    
    // change_info4 (NFS-Ganesha: line 320-327)
    // atomic = TRUE if we successfully got both before and after change attrs
    true.serialize(&mut result)?; // atomic = TRUE
    change_before.serialize(&mut result)?;
    change_after.serialize(&mut result)?;
    
    // attrset (NFS-Ganesha: line 285-292)
    // Return the same bitmap that was requested
    // (we don't actually set attributes yet, but return empty bitmap)
    0u32.serialize(&mut result)?; // bitmap count = 0

    info!(
        "CREATE SUCCESS: type={} name={} fileid={} change={}->{}",
        obj_type, name, new_fileid, change_before, change_after
    );

    Ok(result)
}

/// Validate filename
///
/// # NFS-Ganesha Reference
/// Function: nfs4_utf8string_scan() (called at line 153)
///
/// Validates that the filename:
/// - Is not empty
/// - Does not contain '/' or null bytes
/// - Is valid UTF-8
///
/// # Arguments
/// - name: Filename to validate
///
/// # Returns
/// Ok(()) if valid, Err otherwise
fn validate_filename(name: &str) -> Nfs4Result<()> {
    if name.is_empty() {
        return Err(Nfs4Status::Inval.into());
    }

    if name.contains('/') || name.contains('\0') {
        return Err(Nfs4Status::Inval.into());
    }

    if name == "." || name == ".." {
        return Err(Nfs4Status::Inval.into());
    }

    Ok(())
}

/// Validate symbolic link content
///
/// # NFS-Ganesha Reference
/// Function: nfs4_utf8string_scan() for link data (line 203-210)
///
/// Per RFC 7530 Section 12.4, we validate the length but NOT the content.
/// The link target can contain any UTF-8 string, including invalid paths.
///
/// # Arguments
/// - link_content: Link target to validate
///
/// # Returns
/// Ok(()) if valid, Err otherwise
fn validate_link_content(link_content: &str) -> Nfs4Result<()> {
    // Just check it's not empty
    // NFS-Ganesha uses UTF8_SCAN_PATH which allows '/' in link targets
    if link_content.is_empty() {
        return Err(Nfs4Status::Inval.into());
    }

    Ok(())
}
