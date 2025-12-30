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
    let obj_type = input.read_u32::<BigEndian>()?;

    let (link_data, _spec_data1, _spec_data2) = match obj_type {
        object_type::NF4LNK => {
            let mut link: Vec<u8> = Vec::new();
            link.deserialize(input)?;
            (Some(link), 0, 0)
        }
        object_type::NF4BLK | object_type::NF4CHR => {
            let data1 = input.read_u32::<BigEndian>()?;
            let data2 = input.read_u32::<BigEndian>()?;
            (None, data1, data2)
        }
        _ => (None, 0, 0),
    };

    let mut objname: Vec<u8> = Vec::new();
    objname.deserialize(input)?;
    let name = String::from_utf8_lossy(&objname);

    let mut fattr = Fattr4::default();
    fattr.deserialize(input)?;

    if !fattr.attrmask.is_empty() {
        let word0 = fattr.attrmask[0];
        if word0 & !(1 << 4) != 0 {
            return Err(Nfs4Status::AttrNotsupp.into());
        }
        if fattr.attrmask.len() > 1 {
            let word1 = fattr.attrmask[1];
            let allowed_bits = (1 << 1) | (1 << 4) | (1 << 5) | (1 << 16) | (1 << 22);
            if word1 & !allowed_bits != 0 {
                return Err(Nfs4Status::AttrNotsupp.into());
            }
        }
    }

    let (mut mode, uid, gid, size, atime, mtime) = parse_create_attrs(&fattr)?;

    let parent_fh = ctx.require_current_fh()?;
    let parent_id = handler.fs.fh_to_fileid(parent_fh)?;

    let parent_status = handler.fs.get_status(parent_id).await?;
    if parent_status.file_type != curvine_common::state::FileType::Dir {
        return Err(Nfs4Status::Notdir.into());
    }

    let change_before = parent_status.mtime as u64;
    let is_parent_pre_attrs_valid = true;

    validate_filename(&name)?;

    if obj_type == object_type::NF4LNK {
        if let Some(ref link) = link_data {
            let link_str = String::from_utf8_lossy(link);
            validate_link_content(&link_str)?;
        }
    }

    if mode.is_none() {
        mode = Some(if obj_type == object_type::NF4DIR {
            0o700
        } else {
            0o600
        });
    }

    let (new_fileid, _new_status) = match obj_type {
        object_type::NF4DIR => handler.fs.mkdir(parent_id, &name).await?,
        object_type::NF4LNK => {
            let link_bytes = link_data.ok_or(Nfs4Status::Inval)?;
            let target = String::from_utf8_lossy(&link_bytes).to_string();
            handler.fs.symlink(parent_id, &name, &target).await?
        }
        object_type::NF4SOCK | object_type::NF4FIFO => {
            return Err(Nfs4Status::Notsupp.into());
        }
        object_type::NF4BLK | object_type::NF4CHR => {
            return Err(Nfs4Status::Notsupp.into());
        }
        object_type::NF4REG => {
            return Err(Nfs4Status::Badtype.into());
        }
        _ => {
            return Err(Nfs4Status::Badtype.into());
        }
    };

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

    let new_fh = handler.fs.fileid_to_fh(new_fileid);
    ctx.current_fh = Some(new_fh);

    let parent_status_after = handler.fs.get_status(parent_id).await?;
    let change_after = parent_status_after.mtime as u64;
    let is_parent_post_attrs_valid = true;

    let mut result = Vec::new();

    let atomic = is_parent_pre_attrs_valid && is_parent_post_attrs_valid;
    atomic.serialize(&mut result)?;
    change_before.serialize(&mut result)?;
    change_after.serialize(&mut result)?;

    fattr.attrmask.serialize(&mut result)?;

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
#[allow(clippy::type_complexity)]
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

    if !fattr.attrmask.is_empty() {
        let word0 = fattr.attrmask[0];

        if word0 & (1 << 4) != 0 {
            size = Some(cursor.read_u64::<BigEndian>()?);
        }
    }

    if fattr.attrmask.len() > 1 {
        let word1 = fattr.attrmask[1];

        if word1 & (1 << 1) != 0 {
            mode = Some(cursor.read_u32::<BigEndian>()?);
        }

        if word1 & (1 << 4) != 0 {
            let mut owner: Vec<u8> = Vec::new();
            owner.deserialize(&mut cursor)?;
            if let Ok(s) = String::from_utf8(owner.clone()) {
                uid = s.parse().ok();
            }
        }

        if word1 & (1 << 5) != 0 {
            let mut group: Vec<u8> = Vec::new();
            group.deserialize(&mut cursor)?;
            if let Ok(s) = String::from_utf8(group.clone()) {
                gid = s.parse().ok();
            }
        }

        if word1 & (1 << 16) != 0 {
            let set_it = cursor.read_u32::<BigEndian>()?;
            if set_it == 1 {
                let mut time = Nfstime4::default();
                time.deserialize(&mut cursor)?;
                atime = Some(time);
            }
        }

        if word1 & (1 << 22) != 0 {
            let set_it = cursor.read_u32::<BigEndian>()?;
            if set_it == 1 {
                let mut time = Nfstime4::default();
                time.deserialize(&mut cursor)?;
                mtime = Some(time);
            }
        }
    }

    Ok((mode, uid, gid, size, atime, mtime))
}
