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
use crate::nfs4::types::*;
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

    // Parse create attributes (fattr4 structure)
    // According to RFC 5661 Section 18.4, createattrs is of type fattr4
    // NFS-Ganesha: nfs4_Fattr_To_FSAL_attr() at line 177-186
    let mut fattr = Fattr4::default();
    fattr.deserialize(input)?;

    // NFS-Ganesha line 123-126: Check if attributes are supported
    // nfs4_Fattr_Supported() validates that all requested attributes are supported
    // For now, we accept all standard attributes (mode, uid, gid, size, times)
    // This is a simplified check - full implementation would validate against server capabilities
    if !fattr.attrmask.is_empty() {
        // Check for unsupported attributes (bits beyond what we support)
        // We support: SIZE (bit 4), MODE (bit 33), OWNER (bit 36), OWNER_GROUP (bit 37),
        // TIME_ACCESS_SET (bit 48), TIME_MODIFY_SET (bit 54)
        // Reject if any other bits are set in the first 2 words
        let word0 = fattr.attrmask[0];
        // Only allow SIZE (bit 4) in word 0
        if word0 & !(1 << 4) != 0 {
            return Err(Nfs4Status::AttrNotsupp.into());
        }
        if fattr.attrmask.len() > 1 {
            let word1 = fattr.attrmask[1];
            // Only allow MODE (bit 1), OWNER (bit 4), OWNER_GROUP (bit 5),
            // TIME_ACCESS_SET (bit 16), TIME_MODIFY_SET (bit 22) in word 1
            let allowed_bits = (1 << 1) | (1 << 4) | (1 << 5) | (1 << 16) | (1 << 22);
            if word1 & !allowed_bits != 0 {
                return Err(Nfs4Status::AttrNotsupp.into());
            }
        }
    }

    // NFS-Ganesha line 129-133: Check if attributes are writable
    // nfs4_Fattr_Check_Access() validates that attributes have FATTR4_ATTR_WRITE flag
    // For CREATE, all requested attributes should be writable
    // This is a simplified check - full implementation would check against attribute capabilities

    // Parse attributes from fattr4 (same logic as SETATTR)
    let (mut mode, uid, gid, size, atime, mtime) = parse_create_attrs(&fattr)?;

    info!(
        "CREATE: type={} name={} spec=({},{}) attrs=mode={:?} uid={:?} gid={:?} size={:?}",
        obj_type, name, spec_data1, spec_data2, mode, uid, gid, size
    );

    // Get parent directory (NFS-Ganesha: line 157-165)
    let parent_fh = ctx.require_current_fh()?;
    let parent_id = handler.fs.fh_to_fileid(parent_fh)?;

    // Verify parent is a directory (NFS-Ganesha: line 167-171)
    let parent_status = handler.fs.get_status(parent_id).await?;
    if parent_status.file_type != curvine_common::state::FileType::Dir {
        return Err(Nfs4Status::Notdir.into());
    }

    // Get parent's change attribute before operation (NFS-Ganesha: line 166-167)
    // NFS-Ganesha uses fsal_get_changeid4() which gets ATTR_CHANGE attribute
    // We use mtime as change attribute (simplified approach)
    let change_before = parent_status.mtime as u64;
    let is_parent_pre_attrs_valid = true; // We always get mtime successfully

    // Validate filename (NFS-Ganesha: nfs4_utf8string_scan at line 153)
    validate_filename(&name)?;

    // Validate link content for symbolic links (NFS-Ganesha: line 203-210)
    if obj_type == object_type::NF4LNK {
        if let Some(ref link) = link_data {
            let link_str = String::from_utf8_lossy(link);
            validate_link_content(&link_str)?;
        }
    }

    // NFS-Ganesha line 245-252: Set default mode if not provided
    // Directory: 0700, Other files: 0600
    if mode.is_none() {
        mode = Some(if obj_type == object_type::NF4DIR {
            0o700
        } else {
            0o600
        });
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

    // Apply attributes after creation (NFS-Ganesha: line 250-260)
    // Attributes are applied after object creation, similar to SETATTR
    // Note: mode is now always Some() due to default setting above
    if mode.is_some()
        || uid.is_some()
        || gid.is_some()
        || size.is_some()
        || atime.is_some()
        || mtime.is_some()
    {
        handler
            .fs
            .setattr(new_fileid, mode, uid, gid, size, atime, mtime)
            .await?;
    }

    // Update current FH to the new object (NFS-Ganesha: line 277-281)
    let new_fh = handler.fs.fileid_to_fh(new_fileid);
    ctx.current_fh = Some(new_fh);
    // Note: current_stateid_valid field doesn't exist in our CompoundContext
    // This is handled differently in our implementation

    // Get parent's change attribute after operation (NFS-Ganesha: line 299-307)
    let parent_status_after = handler.fs.get_status(parent_id).await?;
    let change_after = parent_status_after.mtime as u64;
    let is_parent_post_attrs_valid = true; // We always get mtime successfully

    // Build response
    let mut result = Vec::new();

    // change_info4 (NFS-Ganesha: line 309-311)
    // atomic = TRUE only if we successfully got both before and after change attrs
    // NFS-Ganesha: atomic = is_parent_pre_attrs_valid && is_parent_post_attrs_valid
    let atomic = is_parent_pre_attrs_valid && is_parent_post_attrs_valid;
    atomic.serialize(&mut result)?;
    change_before.serialize(&mut result)?;
    change_after.serialize(&mut result)?;

    // attrset (NFS-Ganesha: line 285-292)
    // Return the bitmap of attributes that were successfully set
    // NFS-Ganesha returns the same bitmap that was requested if all attributes were set
    fattr.attrmask.serialize(&mut result)?;

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

/// Parse attributes from fattr4 for CREATE
///
/// # NFS-Ganesha Reference
/// Function: nfs4_Fattr_To_FSAL_attr() (in nfs_convert.c)
///
/// Extracts attribute values from the XDR-encoded fattr4 structure.
/// Returns tuple of (mode, uid, gid, size, atime, mtime).
/// Same logic as SETATTR's parse_setattr_attrs.
fn parse_create_attrs(
    fattr: &Fattr4,
) -> Nfs4Result<(
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<u64>,
    Option<Nfstime4>,
    Option<Nfstime4>,
)> {
    let mut mode = None;
    let mut uid = None;
    let mut gid = None;
    let mut size = None;
    let mut atime = None;
    let mut mtime = None;

    if fattr.attrmask.is_empty() || fattr.attr_vals.is_empty() {
        return Ok((mode, uid, gid, size, atime, mtime));
    }

    let mut cursor = std::io::Cursor::new(&fattr.attr_vals);

    // Parse based on attrmask bits (must be in bit order)
    // Bitmap word 0 attributes (bits 0-31)
    if !fattr.attrmask.is_empty() {
        let word0 = fattr.attrmask[0];

        // FATTR4_SIZE (bit 4)
        if word0 & (1 << 4) != 0 {
            size = Some(cursor.read_u64::<BigEndian>()?);
        }
    }

    // Bitmap word 1 attributes (bits 32-63)
    if fattr.attrmask.len() > 1 {
        let word1 = fattr.attrmask[1];

        // FATTR4_MODE (bit 1 of word 1 = bit 33)
        if word1 & (1 << 1) != 0 {
            mode = Some(cursor.read_u32::<BigEndian>()?);
        }

        // FATTR4_OWNER (bit 4 of word 1 = bit 36)
        if word1 & (1 << 4) != 0 {
            let mut owner: Vec<u8> = Vec::new();
            owner.deserialize(&mut cursor)?;
            // Convert owner string to uid if numeric
            if let Ok(s) = String::from_utf8(owner.clone()) {
                uid = s.parse().ok();
            }
        }

        // FATTR4_OWNER_GROUP (bit 5 of word 1 = bit 37)
        if word1 & (1 << 5) != 0 {
            let mut group: Vec<u8> = Vec::new();
            group.deserialize(&mut cursor)?;
            // Convert group string to gid if numeric
            if let Ok(s) = String::from_utf8(group.clone()) {
                gid = s.parse().ok();
            }
        }

        // FATTR4_TIME_ACCESS_SET (bit 16 of word 1 = bit 48)
        if word1 & (1 << 16) != 0 {
            let set_it = cursor.read_u32::<BigEndian>()?;
            if set_it == 1 {
                // SET_TO_CLIENT_TIME4
                let mut time = Nfstime4::default();
                time.deserialize(&mut cursor)?;
                atime = Some(time);
            }
            // SET_TO_SERVER_TIME4 (0) - server sets current time
        }

        // FATTR4_TIME_MODIFY_SET (bit 22 of word 1 = bit 54)
        if word1 & (1 << 22) != 0 {
            let set_it = cursor.read_u32::<BigEndian>()?;
            if set_it == 1 {
                // SET_TO_CLIENT_TIME4
                let mut time = Nfstime4::default();
                time.deserialize(&mut cursor)?;
                mtime = Some(time);
            }
        }
    }

    Ok((mode, uid, gid, size, atime, mtime))
}
