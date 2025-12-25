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

//! NFSv3 request handlers

#![allow(clippy::upper_case_acronyms)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_local_definitions)]

use crate::nfs;
use crate::protocol::rpc::*;
use crate::protocol::xdr::*;
use crate::server::context::RPCContext;
use crate::vfs::VFSCapabilities;
use byteorder::{ReadBytesExt, WriteBytesExt};
use num_enum::{FromPrimitive, IntoPrimitive};
use std::io::{Read, Write};
use tracing::{debug, error, trace, warn};

/*
program NFS_PROGRAM {
 version NFS_V3 {
    void NFSPROC3_NULL(void) = 0;
    GETATTR3res NFSPROC3_GETATTR(GETATTR3args) = 1;
    SETATTR3res NFSPROC3_SETATTR(SETATTR3args) = 2;
    LOOKUP3res NFSPROC3_LOOKUP(LOOKUP3args) = 3;
    ACCESS3res NFSPROC3_ACCESS(ACCESS3args) = 4;
    READLINK3res NFSPROC3_READLINK(READLINK3args) = 5;
    READ3res NFSPROC3_READ(READ3args) = 6;
    WRITE3res NFSPROC3_WRITE(WRITE3args) = 7;
    CREATE3res NFSPROC3_CREATE(CREATE3args) = 8;
    MKDIR3res NFSPROC3_MKDIR(MKDIR3args) = 9;
    SYMLINK3res NFSPROC3_SYMLINK(SYMLINK3args) = 10;
    MKNOD3res NFSPROC3_MKNOD(MKNOD3args) = 11;
    REMOVE3res NFSPROC3_REMOVE(REMOVE3args) = 12;
    RMDIR3res NFSPROC3_RMDIR(RMDIR3args) = 13;
    RENAME3res NFSPROC3_RENAME(RENAME3args) = 14;
    LINK3res NFSPROC3_LINK(LINK3args) = 15;
    READDIR3res NFSPROC3_READDIR(READDIR3args) = 16;
    READDIRPLUS3res NFSPROC3_READDIRPLUS(READDIRPLUS3args) = 17;
    FSSTAT3res NFSPROC3_FSSTAT(FSSTAT3args) = 18;
    FSINFO3res NFSPROC3_FSINFO(FSINFO3args) = 19;
    PATHCONF3res NFSPROC3_PATHCONF(PATHCONF3args) = 20;
    COMMIT3res NFSPROC3_COMMIT(COMMIT3args) = 21;
 } = 3;
} = 100003;
*/

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Debug, Default, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
enum NFSProgram {
    NFSPROC3_NULL = 0,
    NFSPROC3_GETATTR = 1,
    NFSPROC3_SETATTR = 2,
    NFSPROC3_LOOKUP = 3,
    NFSPROC3_ACCESS = 4,
    NFSPROC3_READLINK = 5,
    NFSPROC3_READ = 6,
    NFSPROC3_WRITE = 7,
    NFSPROC3_CREATE = 8,
    NFSPROC3_MKDIR = 9,
    NFSPROC3_SYMLINK = 10,
    NFSPROC3_MKNOD = 11,
    NFSPROC3_REMOVE = 12,
    NFSPROC3_RMDIR = 13,
    NFSPROC3_RENAME = 14,
    NFSPROC3_LINK = 15,
    NFSPROC3_READDIR = 16,
    NFSPROC3_READDIRPLUS = 17,
    NFSPROC3_FSSTAT = 18,
    NFSPROC3_FSINFO = 19,
    NFSPROC3_PATHCONF = 20,
    NFSPROC3_COMMIT = 21,
    #[default]
    INVALID = 22,
}

/// Main NFS request dispatcher
pub async fn handle_nfs(
    xid: u32,
    call: call_body,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    if call.vers != nfs::VERSION {
        warn!(
            "Invalid NFS Version number {} != {}",
            call.vers,
            nfs::VERSION
        );
        prog_mismatch_reply_message(xid, nfs::VERSION).serialize(output)?;
        return Ok(());
    }
    let prog = NFSProgram::from(call.proc);

    match prog {
        NFSProgram::NFSPROC3_NULL => nfsproc3_null(xid, input, output)?,
        NFSProgram::NFSPROC3_GETATTR => nfsproc3_getattr(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_LOOKUP => nfsproc3_lookup(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_READ => nfsproc3_read(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_FSINFO => nfsproc3_fsinfo(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_ACCESS => nfsproc3_access(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_PATHCONF => nfsproc3_pathconf(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_FSSTAT => nfsproc3_fsstat(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_READDIR => nfsproc3_readdir(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_READDIRPLUS => {
            nfsproc3_readdirplus(xid, input, output, context).await?
        }
        NFSProgram::NFSPROC3_WRITE => nfsproc3_write(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_CREATE => nfsproc3_create(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_SETATTR => nfsproc3_setattr(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_REMOVE => nfsproc3_remove(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_RMDIR => nfsproc3_remove(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_RENAME => nfsproc3_rename(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_MKDIR => nfsproc3_mkdir(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_SYMLINK => nfsproc3_symlink(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_READLINK => nfsproc3_readlink(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_LINK => nfsproc3_link(xid, input, output, context).await?,
        NFSProgram::NFSPROC3_COMMIT => nfsproc3_commit(xid, input, output, context).await?,
        _ => {
            warn!("Unimplemented message {:?}", prog);
            proc_unavail_reply_message(xid).serialize(output)?;
        }
    }
    Ok(())
}

// ============================================================================
// NULL procedure
// ============================================================================

pub fn nfsproc3_null(
    xid: u32,
    _: &mut impl Read,
    output: &mut impl Write,
) -> Result<(), anyhow::Error> {
    debug!("nfsproc3_null({:?}) ", xid);
    let msg = make_success_reply(xid);
    debug!("\t{:?} --> {:?}", xid, msg);
    msg.serialize(output)?;
    Ok(())
}

// ============================================================================
// GETATTR procedure
// ============================================================================

pub async fn nfsproc3_getattr(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut handle = nfs::nfs_fh3::default();
    handle.deserialize(input)?;
    debug!("nfsproc3_getattr({:?},{:?}) ", xid, handle);

    let id = match context.vfs.fh_to_id(&handle) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            return Ok(());
        }
    };
    match context.vfs.getattr(id).await {
        Ok(fh) => {
            debug!(" {:?} --> {:?}", xid, fh);
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            fh.serialize(output)?;
        }
        Err(stat) => {
            error!("getattr error {:?} --> {:?}", xid, stat);
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
        }
    }
    Ok(())
}

// ============================================================================
// LOOKUP procedure
// ============================================================================

pub async fn nfsproc3_lookup(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut dirops = nfs::diropargs3::default();
    dirops.deserialize(input)?;
    debug!("nfsproc3_lookup({:?},{:?}) ", xid, dirops);

    let dirid = match context.vfs.fh_to_id(&dirops.dir) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            return Ok(());
        }
    };

    let dir_attr = match context.vfs.getattr(dirid).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };
    match context.vfs.lookup(dirid, &dirops.name).await {
        Ok(fid) => {
            let obj_attr = match context.vfs.getattr(fid).await {
                Ok(v) => nfs::post_op_attr::attributes(v),
                Err(_) => nfs::post_op_attr::Void,
            };

            debug!("lookup success {:?} --> {:?}", xid, obj_attr);
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            context.vfs.id_to_fh(fid).serialize(output)?;
            obj_attr.serialize(output)?;
            dir_attr.serialize(output)?;
        }
        Err(stat) => {
            debug!("lookup error {:?}({:?}) --> {:?}", xid, dirops.name, stat);
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            dir_attr.serialize(output)?;
        }
    }
    Ok(())
}

// ============================================================================
// READ procedure
// ============================================================================

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct READ3args {
    file: nfs::nfs_fh3,
    offset: nfs::offset3,
    count: nfs::count3,
}
XDRStruct!(READ3args, file, offset, count);

// Note: READ3resok is serialized manually to avoid data copy

pub async fn nfsproc3_read(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut args = READ3args::default();
    args.deserialize(input)?;
    debug!("nfsproc3_read({:?},{:?}) ", xid, args);

    let id = match context.vfs.fh_to_id(&args.file) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            return Ok(());
        }
    };

    let obj_attr = match context.vfs.getattr(id).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };

    match context.vfs.read(id, args.offset, args.count).await {
        Ok((slices, eof)) => {
            // Calculate total bytes
            let total_len: usize = slices.iter().map(|s| s.len()).sum();

            // Serialize response header
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            obj_attr.serialize(output)?;
            (total_len as u32).serialize(output)?; // count
            eof.serialize(output)?;

            // Serialize data length and slices directly (zero-copy)
            (total_len as u32).serialize(output)?;
            for slice in &slices {
                output.write_all(slice.as_slice())?;
            }

            // XDR 4-byte alignment padding
            let pad = (4 - total_len % 4) % 4;
            if pad > 0 {
                output.write_all(&[0u8; 4][..pad])?;
            }
        }
        Err(stat) => {
            error!("read error {:?} --> {:?}", xid, stat);
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            obj_attr.serialize(output)?;
        }
    }
    Ok(())
}

// ============================================================================
// FSINFO procedure
// ============================================================================

pub async fn nfsproc3_fsinfo(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut handle = nfs::nfs_fh3::default();
    handle.deserialize(input)?;
    debug!("nfsproc3_fsinfo({:?},{:?}) ", xid, handle);

    let id = match context.vfs.fh_to_id(&handle) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            return Ok(());
        }
    };

    match context.vfs.fsinfo(id).await {
        Ok(fsinfo) => {
            debug!(" {:?} --> {:?}", xid, fsinfo);
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            fsinfo.serialize(output)?;
        }
        Err(stat) => {
            error!("fsinfo error {:?} --> {:?}", xid, stat);
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
        }
    }
    Ok(())
}

// ============================================================================
// ACCESS procedure
// ============================================================================

const ACCESS3_READ: u32 = 0x0001;
const ACCESS3_LOOKUP: u32 = 0x0002;
const ACCESS3_MODIFY: u32 = 0x0004;
const ACCESS3_EXTEND: u32 = 0x0008;
const ACCESS3_DELETE: u32 = 0x0010;
const ACCESS3_EXECUTE: u32 = 0x0020;

pub async fn nfsproc3_access(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut handle = nfs::nfs_fh3::default();
    handle.deserialize(input)?;
    let mut access: u32 = 0;
    access.deserialize(input)?;
    debug!("nfsproc3_access({:?},{:?},{:?})", xid, handle, access);

    let id = match context.vfs.fh_to_id(&handle) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            return Ok(());
        }
    };

    let obj_attr = match context.vfs.getattr(id).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };
    if !matches!(context.vfs.capabilities(), VFSCapabilities::ReadWrite) {
        access &= ACCESS3_READ | ACCESS3_LOOKUP;
    }
    debug!(" {:?} ---> {:?}", xid, access);
    make_success_reply(xid).serialize(output)?;
    nfs::nfsstat3::NFS3_OK.serialize(output)?;
    obj_attr.serialize(output)?;
    access.serialize(output)?;
    Ok(())
}

// ============================================================================
// PATHCONF procedure
// ============================================================================

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct PATHCONF3resok {
    obj_attributes: nfs::post_op_attr,
    linkmax: u32,
    name_max: u32,
    no_trunc: bool,
    chown_restricted: bool,
    case_insensitive: bool,
    case_preserving: bool,
}
XDRStruct!(
    PATHCONF3resok,
    obj_attributes,
    linkmax,
    name_max,
    no_trunc,
    chown_restricted,
    case_insensitive,
    case_preserving
);

pub async fn nfsproc3_pathconf(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut handle = nfs::nfs_fh3::default();
    handle.deserialize(input)?;
    debug!("nfsproc3_pathconf({:?},{:?})", xid, handle);

    let id = match context.vfs.fh_to_id(&handle) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            return Ok(());
        }
    };

    let obj_attr = match context.vfs.getattr(id).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };
    let res = PATHCONF3resok {
        obj_attributes: obj_attr,
        linkmax: 0,
        name_max: 32768,
        no_trunc: true,
        chown_restricted: true,
        case_insensitive: false,
        case_preserving: true,
    };
    debug!(" {:?} ---> {:?}", xid, res);
    make_success_reply(xid).serialize(output)?;
    nfs::nfsstat3::NFS3_OK.serialize(output)?;
    res.serialize(output)?;
    Ok(())
}

// ============================================================================
// FSSTAT procedure
// ============================================================================

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct FSSTAT3resok {
    obj_attributes: nfs::post_op_attr,
    tbytes: nfs::size3,
    fbytes: nfs::size3,
    abytes: nfs::size3,
    tfiles: nfs::size3,
    ffiles: nfs::size3,
    afiles: nfs::size3,
    invarsec: u32,
}
XDRStruct!(
    FSSTAT3resok,
    obj_attributes,
    tbytes,
    fbytes,
    abytes,
    tfiles,
    ffiles,
    afiles,
    invarsec
);

pub async fn nfsproc3_fsstat(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut handle = nfs::nfs_fh3::default();
    handle.deserialize(input)?;
    debug!("nfsproc3_fsstat({:?},{:?}) ", xid, handle);
    let id = match context.vfs.fh_to_id(&handle) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            return Ok(());
        }
    };

    let obj_attr = match context.vfs.getattr(id).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };
    let res = FSSTAT3resok {
        obj_attributes: obj_attr,
        tbytes: 1024 * 1024 * 1024 * 1024,
        fbytes: 1024 * 1024 * 1024 * 1024,
        abytes: 1024 * 1024 * 1024 * 1024,
        tfiles: 1024 * 1024 * 1024,
        ffiles: 1024 * 1024 * 1024,
        afiles: 1024 * 1024 * 1024,
        invarsec: u32::MAX,
    };
    make_success_reply(xid).serialize(output)?;
    nfs::nfsstat3::NFS3_OK.serialize(output)?;
    debug!(" {:?} ---> {:?}", xid, res);
    res.serialize(output)?;
    Ok(())
}

// ============================================================================
// READDIR / READDIRPLUS procedures
// ============================================================================

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct READDIRPLUS3args {
    dir: nfs::nfs_fh3,
    cookie: nfs::cookie3,
    cookieverf: nfs::cookieverf3,
    dircount: nfs::count3,
    maxcount: nfs::count3,
}
XDRStruct!(
    READDIRPLUS3args,
    dir,
    cookie,
    cookieverf,
    dircount,
    maxcount
);

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct entry3 {
    fileid: nfs::fileid3,
    name: nfs::filename3,
    cookie: nfs::cookie3,
}
XDRStruct!(entry3, fileid, name, cookie);

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct READDIR3args {
    dir: nfs::nfs_fh3,
    cookie: nfs::cookie3,
    cookieverf: nfs::cookieverf3,
    dircount: nfs::count3,
}
XDRStruct!(READDIR3args, dir, cookie, cookieverf, dircount);

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct entryplus3 {
    fileid: nfs::fileid3,
    name: nfs::filename3,
    cookie: nfs::cookie3,
    name_attributes: nfs::post_op_attr,
    name_handle: nfs::post_op_fh3,
}
XDRStruct!(
    entryplus3,
    fileid,
    name,
    cookie,
    name_attributes,
    name_handle
);

pub async fn nfsproc3_readdirplus(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut args = READDIRPLUS3args::default();
    args.deserialize(input)?;
    debug!("nfsproc3_readdirplus({:?},{:?}) ", xid, args);

    let dirid = match context.vfs.fh_to_id(&args.dir) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            return Ok(());
        }
    };
    let dir_attr_maybe = context.vfs.getattr(dirid).await;

    let dir_attr = match dir_attr_maybe {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };

    let dirversion = if let Ok(ref dir_attr) = dir_attr_maybe {
        let cvf_version = (dir_attr.mtime.seconds as u64) << 32 | (dir_attr.mtime.nseconds as u64);
        cvf_version.to_be_bytes()
    } else {
        nfs::cookieverf3::default()
    };
    debug!(" -- Dir attr {:?}", dir_attr);
    debug!(" -- Dir version {:?}", dirversion);
    let has_version = args.cookieverf != nfs::cookieverf3::default();

    let max_bytes_allowed = args.maxcount as usize - 128;
    let estimated_max_results = args.dircount / 16;
    let max_dircount_bytes = args.dircount as usize;
    let mut ctr = 0;
    match context
        .vfs
        .readdir(dirid, args.cookie, estimated_max_results as usize)
        .await
    {
        Ok(result) => {
            let mut accumulated_dircount: usize = 0;
            let mut all_entries_written = true;

            let mut counting_output = crate::util::WriteCounter::new(output);

            make_success_reply(xid).serialize(&mut counting_output)?;
            nfs::nfsstat3::NFS3_OK.serialize(&mut counting_output)?;
            dir_attr.serialize(&mut counting_output)?;
            dirversion.serialize(&mut counting_output)?;
            for entry in result.entries {
                let obj_attr = entry.attr;
                let handle = nfs::post_op_fh3::handle(context.vfs.id_to_fh(entry.fileid));

                let entry = entryplus3 {
                    fileid: entry.fileid,
                    name: entry.name,
                    cookie: entry.fileid,
                    name_attributes: nfs::post_op_attr::attributes(obj_attr),
                    name_handle: handle,
                };
                let mut write_buf: Vec<u8> = Vec::new();
                let mut write_cursor = std::io::Cursor::new(&mut write_buf);
                true.serialize(&mut write_cursor)?;
                entry.serialize(&mut write_cursor)?;
                write_cursor.flush()?;
                let added_dircount = std::mem::size_of::<nfs::fileid3>()
                    + std::mem::size_of::<u32>()
                    + entry.name.len()
                    + std::mem::size_of::<nfs::cookie3>();
                let added_output_bytes = write_buf.len();
                if added_output_bytes + counting_output.bytes_written() < max_bytes_allowed
                    && added_dircount + accumulated_dircount < max_dircount_bytes
                {
                    trace!("  -- dirent {:?}", entry);
                    ctr += 1;
                    counting_output.write_all(&write_buf)?;
                    accumulated_dircount += added_dircount;
                    trace!(
                        "  -- lengths: {:?} / {:?} {:?} / {:?}",
                        accumulated_dircount,
                        max_dircount_bytes,
                        counting_output.bytes_written(),
                        max_bytes_allowed
                    );
                } else {
                    trace!(" -- insufficient space. truncating");
                    all_entries_written = false;
                    break;
                }
            }
            false.serialize(&mut counting_output)?;
            if all_entries_written {
                debug!("  -- readdir eof {:?}", result.end);
                result.end.serialize(&mut counting_output)?;
            } else {
                debug!("  -- readdir eof {:?}", false);
                false.serialize(&mut counting_output)?;
            }
            debug!(
                "readir {}, has_version {},  start at {}, flushing {} entries, complete {}",
                dirid, has_version, args.cookie, ctr, all_entries_written
            );
        }
        Err(stat) => {
            error!("readdir error {:?} --> {:?} ", xid, stat);
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            dir_attr.serialize(output)?;
        }
    };
    Ok(())
}

pub async fn nfsproc3_readdir(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut args = READDIR3args::default();
    args.deserialize(input)?;
    debug!("nfsproc3_readdirplus({:?},{:?}) ", xid, args);

    let dirid = match context.vfs.fh_to_id(&args.dir) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            return Ok(());
        }
    };
    let dir_attr_maybe = context.vfs.getattr(dirid).await;

    let dir_attr = match dir_attr_maybe {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };

    let dirversion = if let Ok(ref dir_attr) = dir_attr_maybe {
        let cvf_version = (dir_attr.mtime.seconds as u64) << 32 | (dir_attr.mtime.nseconds as u64);
        cvf_version.to_be_bytes()
    } else {
        nfs::cookieverf3::default()
    };
    debug!(" -- Dir attr {:?}", dir_attr);
    debug!(" -- Dir version {:?}", dirversion);
    let has_version = args.cookieverf != nfs::cookieverf3::default();
    let max_bytes_allowed = args.dircount as usize - 128;
    let estimated_max_results = args.dircount / 16;
    let mut ctr = 0;
    match context
        .vfs
        .readdir_simple(dirid, estimated_max_results as usize)
        .await
    {
        Ok(result) => {
            let mut accumulated_dircount: usize = 0;
            let mut all_entries_written = true;

            let mut counting_output = crate::util::WriteCounter::new(output);

            make_success_reply(xid).serialize(&mut counting_output)?;
            nfs::nfsstat3::NFS3_OK.serialize(&mut counting_output)?;
            dir_attr.serialize(&mut counting_output)?;
            dirversion.serialize(&mut counting_output)?;
            for entry in result.entries {
                let entry = entry3 {
                    fileid: entry.fileid,
                    name: entry.name,
                    cookie: entry.fileid,
                };
                let mut write_buf: Vec<u8> = Vec::new();
                let mut write_cursor = std::io::Cursor::new(&mut write_buf);
                true.serialize(&mut write_cursor)?;
                entry.serialize(&mut write_cursor)?;
                write_cursor.flush()?;
                let added_dircount = std::mem::size_of::<nfs::fileid3>()
                    + std::mem::size_of::<u32>()
                    + entry.name.len()
                    + std::mem::size_of::<nfs::cookie3>();
                let added_output_bytes = write_buf.len();
                if added_output_bytes + counting_output.bytes_written() < max_bytes_allowed {
                    trace!("  -- dirent {:?}", entry);
                    ctr += 1;
                    counting_output.write_all(&write_buf)?;
                    accumulated_dircount += added_dircount;
                    trace!(
                        "  -- lengths: {:?} / {:?} / {:?}",
                        accumulated_dircount,
                        counting_output.bytes_written(),
                        max_bytes_allowed
                    );
                } else {
                    trace!(" -- insufficient space. truncating");
                    all_entries_written = false;
                    break;
                }
            }
            false.serialize(&mut counting_output)?;
            if all_entries_written {
                debug!("  -- readdir eof {:?}", result.end);
                result.end.serialize(&mut counting_output)?;
            } else {
                debug!("  -- readdir eof {:?}", false);
                false.serialize(&mut counting_output)?;
            }
            debug!(
                "readir {}, has_version {},  start at {}, flushing {} entries, complete {}",
                dirid, has_version, args.cookie, ctr, all_entries_written
            );
        }
        Err(stat) => {
            error!("readdir error {:?} --> {:?} ", xid, stat);
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            dir_attr.serialize(output)?;
        }
    };
    Ok(())
}

// ============================================================================
// WRITE procedure
// ============================================================================

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Debug, Default, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum stable_how {
    #[default]
    UNSTABLE = 0,
    DATA_SYNC = 1,
    FILE_SYNC = 2,
}
XDREnumSerde!(stable_how);

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct WRITE3args {
    file: nfs::nfs_fh3,
    offset: nfs::offset3,
    count: nfs::count3,
    stable: u32,
    data: Vec<u8>,
}
XDRStruct!(WRITE3args, file, offset, count, stable, data);

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct WRITE3resok {
    file_wcc: nfs::wcc_data,
    count: nfs::count3,
    committed: stable_how,
    verf: nfs::writeverf3,
}
XDRStruct!(WRITE3resok, file_wcc, count, committed, verf);

pub async fn nfsproc3_write(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    if !matches!(context.vfs.capabilities(), VFSCapabilities::ReadWrite) {
        warn!("No write capabilities.");
        make_success_reply(xid).serialize(output)?;
        nfs::nfsstat3::NFS3ERR_ROFS.serialize(output)?;
        nfs::wcc_data::default().serialize(output)?;
        return Ok(());
    }

    let mut args = WRITE3args::default();
    args.deserialize(input)?;
    debug!("nfsproc3_write({:?},...) ", xid);
    if args.data.len() != args.count as usize {
        garbage_args_reply_message(xid).serialize(output)?;
        return Ok(());
    }

    let id = match context.vfs.fh_to_id(&args.file) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };

    // Get pre-op attributes for wcc_data
    let pre_obj_attr = match context.vfs.getattr(id).await {
        Ok(v) => {
            let wccattr = nfs::wcc_attr {
                size: v.size,
                mtime: v.mtime,
                ctime: v.ctime,
            };
            nfs::pre_op_attr::attributes(wccattr)
        }
        Err(_) => nfs::pre_op_attr::Void,
    };

    // Pass ownership of args.data to avoid copy (zero-copy optimization)
    match context.vfs.write(id, args.offset, args.data).await {
        Ok(bytes_written) => {
            debug!("write success {:?} --> {} bytes", xid, bytes_written);
            // For UNSTABLE writes, we don't need accurate post-op attributes
            // Use Void to avoid extra get_status RPC
            let res = WRITE3resok {
                file_wcc: nfs::wcc_data {
                    before: pre_obj_attr,
                    after: nfs::post_op_attr::Void,
                },
                count: bytes_written,
                // Return UNSTABLE to allow client to batch writes and call COMMIT later
                committed: stable_how::UNSTABLE,
                verf: context.vfs.serverid(),
            };
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            res.serialize(output)?;
        }
        Err(stat) => {
            error!("write error {:?} --> {:?}", xid, stat);
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
        }
    }
    Ok(())
}

// ============================================================================
// CREATE procedure
// ============================================================================

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Debug, Default, FromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum createmode3 {
    #[default]
    UNCHECKED = 0,
    GUARDED = 1,
    EXCLUSIVE = 2,
}
XDREnumSerde!(createmode3);

pub async fn nfsproc3_create(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    if !matches!(context.vfs.capabilities(), VFSCapabilities::ReadWrite) {
        warn!("No write capabilities.");
        make_success_reply(xid).serialize(output)?;
        nfs::nfsstat3::NFS3ERR_ROFS.serialize(output)?;
        nfs::wcc_data::default().serialize(output)?;
        return Ok(());
    }

    let mut dirops = nfs::diropargs3::default();
    dirops.deserialize(input)?;
    let mut createhow = createmode3::default();
    createhow.deserialize(input)?;

    debug!("nfsproc3_create({:?}, {:?}, {:?}) ", xid, dirops, createhow);

    let dirid = match context.vfs.fh_to_id(&dirops.dir) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            error!("Directory does not exist");
            return Ok(());
        }
    };

    let pre_dir_attr = match context.vfs.getattr(dirid).await {
        Ok(v) => {
            let wccattr = nfs::wcc_attr {
                size: v.size,
                mtime: v.mtime,
                ctime: v.ctime,
            };
            nfs::pre_op_attr::attributes(wccattr)
        }
        Err(stat) => {
            error!("Cannot stat directory");
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };
    let mut target_attributes = nfs::sattr3::default();

    match createhow {
        createmode3::UNCHECKED => {
            target_attributes.deserialize(input)?;
            debug!("create unchecked {:?}", target_attributes);
        }
        createmode3::GUARDED => {
            target_attributes.deserialize(input)?;
            debug!("create guarded {:?}", target_attributes);
            if context.vfs.lookup(dirid, &dirops.name).await.is_ok() {
                let post_dir_attr = match context.vfs.getattr(dirid).await {
                    Ok(v) => nfs::post_op_attr::attributes(v),
                    Err(_) => nfs::post_op_attr::Void,
                };

                make_success_reply(xid).serialize(output)?;
                nfs::nfsstat3::NFS3ERR_EXIST.serialize(output)?;
                nfs::wcc_data {
                    before: pre_dir_attr,
                    after: post_dir_attr,
                }
                .serialize(output)?;
                return Ok(());
            }
        }
        createmode3::EXCLUSIVE => {
            debug!("create exclusive");
        }
    }

    let fid: Result<nfs::fileid3, nfs::nfsstat3>;
    let postopattr: nfs::post_op_attr;
    if matches!(createhow, createmode3::EXCLUSIVE) {
        fid = context.vfs.create_exclusive(dirid, &dirops.name).await;
        postopattr = nfs::post_op_attr::Void;
    } else {
        let res = context
            .vfs
            .create(dirid, &dirops.name, target_attributes)
            .await;
        fid = res.map(|x| x.0);
        postopattr = if let Ok((_, fattr)) = res {
            nfs::post_op_attr::attributes(fattr)
        } else {
            nfs::post_op_attr::Void
        };
    }

    let post_dir_attr = match context.vfs.getattr(dirid).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };
    let wcc_res = nfs::wcc_data {
        before: pre_dir_attr,
        after: post_dir_attr,
    };

    match fid {
        Ok(fid) => {
            debug!("create success --> {:?}, {:?}", fid, postopattr);
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            let fh = context.vfs.id_to_fh(fid);
            nfs::post_op_fh3::handle(fh).serialize(output)?;
            postopattr.serialize(output)?;
            wcc_res.serialize(output)?;
        }
        Err(e) => {
            error!("create error --> {:?}", e);
            make_success_reply(xid).serialize(output)?;
            e.serialize(output)?;
            wcc_res.serialize(output)?;
        }
    }

    Ok(())
}

// ============================================================================
// SETATTR procedure
// ============================================================================

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Debug, Default)]
#[repr(u32)]
pub enum sattrguard3 {
    #[default]
    Void,
    obj_ctime(nfs::nfstime3),
}
XDRBoolUnion!(sattrguard3, obj_ctime, nfs::nfstime3);

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Default)]
struct SETATTR3args {
    object: nfs::nfs_fh3,
    new_attribute: nfs::sattr3,
    guard: sattrguard3,
}
XDRStruct!(SETATTR3args, object, new_attribute, guard);

pub async fn nfsproc3_setattr(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    if !matches!(context.vfs.capabilities(), VFSCapabilities::ReadWrite) {
        warn!("No write capabilities.");
        make_success_reply(xid).serialize(output)?;
        nfs::nfsstat3::NFS3ERR_ROFS.serialize(output)?;
        nfs::wcc_data::default().serialize(output)?;
        return Ok(());
    }
    let mut args = SETATTR3args::default();
    args.deserialize(input)?;
    debug!("nfsproc3_setattr({:?},{:?}) ", xid, args);

    let id = match context.vfs.fh_to_id(&args.object) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            return Ok(());
        }
    };

    let ctime;

    let pre_op_attr = match context.vfs.getattr(id).await {
        Ok(v) => {
            let wccattr = nfs::wcc_attr {
                size: v.size,
                mtime: v.mtime,
                ctime: v.ctime,
            };
            ctime = v.ctime;
            nfs::pre_op_attr::attributes(wccattr)
        }
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };
    match args.guard {
        sattrguard3::Void => {}
        sattrguard3::obj_ctime(c) => {
            if c.seconds != ctime.seconds || c.nseconds != ctime.nseconds {
                make_success_reply(xid).serialize(output)?;
                nfs::nfsstat3::NFS3ERR_NOT_SYNC.serialize(output)?;
                nfs::wcc_data::default().serialize(output)?;
            }
        }
    }

    match context.vfs.setattr(id, args.new_attribute).await {
        Ok(post_op_attr) => {
            debug!(" setattr success {:?} --> {:?}", xid, post_op_attr);
            let wcc_res = nfs::wcc_data {
                before: pre_op_attr,
                after: nfs::post_op_attr::attributes(post_op_attr),
            };
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            wcc_res.serialize(output)?;
        }
        Err(stat) => {
            error!("setattr error {:?} --> {:?}", xid, stat);
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
        }
    }
    Ok(())
}

// ============================================================================
// REMOVE / RMDIR procedure
// ============================================================================

pub async fn nfsproc3_remove(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    if !matches!(context.vfs.capabilities(), VFSCapabilities::ReadWrite) {
        warn!("No write capabilities.");
        make_success_reply(xid).serialize(output)?;
        nfs::nfsstat3::NFS3ERR_ROFS.serialize(output)?;
        nfs::wcc_data::default().serialize(output)?;
        return Ok(());
    }

    let mut dirops = nfs::diropargs3::default();
    dirops.deserialize(input)?;

    debug!("nfsproc3_remove({:?}, {:?}) ", xid, dirops);

    let dirid = match context.vfs.fh_to_id(&dirops.dir) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            error!("Directory does not exist");
            return Ok(());
        }
    };

    let pre_dir_attr = match context.vfs.getattr(dirid).await {
        Ok(v) => {
            let wccattr = nfs::wcc_attr {
                size: v.size,
                mtime: v.mtime,
                ctime: v.ctime,
            };
            nfs::pre_op_attr::attributes(wccattr)
        }
        Err(stat) => {
            error!("Cannot stat directory");
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };

    let res = context.vfs.remove(dirid, &dirops.name).await;

    let post_dir_attr = match context.vfs.getattr(dirid).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };
    let wcc_res = nfs::wcc_data {
        before: pre_dir_attr,
        after: post_dir_attr,
    };

    match res {
        Ok(()) => {
            debug!("remove success");
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            wcc_res.serialize(output)?;
        }
        Err(e) => {
            error!("remove error {:?} --> {:?}", xid, e);
            make_success_reply(xid).serialize(output)?;
            e.serialize(output)?;
            wcc_res.serialize(output)?;
        }
    }

    Ok(())
}

// ============================================================================
// RENAME procedure
// ============================================================================

pub async fn nfsproc3_rename(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    if !matches!(context.vfs.capabilities(), VFSCapabilities::ReadWrite) {
        warn!("No write capabilities.");
        make_success_reply(xid).serialize(output)?;
        nfs::nfsstat3::NFS3ERR_ROFS.serialize(output)?;
        nfs::wcc_data::default().serialize(output)?;
        return Ok(());
    }

    let mut fromdirops = nfs::diropargs3::default();
    let mut todirops = nfs::diropargs3::default();
    fromdirops.deserialize(input)?;
    todirops.deserialize(input)?;

    debug!(
        "nfsproc3_rename({:?}, {:?}, {:?}) ",
        xid, fromdirops, todirops
    );

    let from_dirid = match context.vfs.fh_to_id(&fromdirops.dir) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            error!("Directory does not exist");
            return Ok(());
        }
    };

    let to_dirid = match context.vfs.fh_to_id(&todirops.dir) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            error!("Directory does not exist");
            return Ok(());
        }
    };

    let pre_from_dir_attr = match context.vfs.getattr(from_dirid).await {
        Ok(v) => {
            let wccattr = nfs::wcc_attr {
                size: v.size,
                mtime: v.mtime,
                ctime: v.ctime,
            };
            nfs::pre_op_attr::attributes(wccattr)
        }
        Err(stat) => {
            error!("Cannot stat directory");
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };

    let pre_to_dir_attr = match context.vfs.getattr(to_dirid).await {
        Ok(v) => {
            let wccattr = nfs::wcc_attr {
                size: v.size,
                mtime: v.mtime,
                ctime: v.ctime,
            };
            nfs::pre_op_attr::attributes(wccattr)
        }
        Err(stat) => {
            error!("Cannot stat directory");
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };

    let res = context
        .vfs
        .rename(from_dirid, &fromdirops.name, to_dirid, &todirops.name)
        .await;

    let post_from_dir_attr = match context.vfs.getattr(from_dirid).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };
    let post_to_dir_attr = match context.vfs.getattr(to_dirid).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };
    let from_wcc_res = nfs::wcc_data {
        before: pre_from_dir_attr,
        after: post_from_dir_attr,
    };

    let to_wcc_res = nfs::wcc_data {
        before: pre_to_dir_attr,
        after: post_to_dir_attr,
    };

    match res {
        Ok(()) => {
            debug!("rename success");
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            from_wcc_res.serialize(output)?;
            to_wcc_res.serialize(output)?;
        }
        Err(e) => {
            error!("rename error {:?} --> {:?}", xid, e);
            make_success_reply(xid).serialize(output)?;
            e.serialize(output)?;
            from_wcc_res.serialize(output)?;
            to_wcc_res.serialize(output)?;
        }
    }

    Ok(())
}

// ============================================================================
// MKDIR procedure
// ============================================================================

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct MKDIR3args {
    dirops: nfs::diropargs3,
    attributes: nfs::sattr3,
}
XDRStruct!(MKDIR3args, dirops, attributes);

pub async fn nfsproc3_mkdir(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    if !matches!(context.vfs.capabilities(), VFSCapabilities::ReadWrite) {
        warn!("No write capabilities.");
        make_success_reply(xid).serialize(output)?;
        nfs::nfsstat3::NFS3ERR_ROFS.serialize(output)?;
        nfs::wcc_data::default().serialize(output)?;
        return Ok(());
    }
    let mut args = MKDIR3args::default();
    args.deserialize(input)?;

    debug!("nfsproc3_mkdir({:?}, {:?}) ", xid, args);

    let dirid = match context.vfs.fh_to_id(&args.dirops.dir) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            error!("Directory does not exist");
            return Ok(());
        }
    };

    let pre_dir_attr = match context.vfs.getattr(dirid).await {
        Ok(v) => {
            let wccattr = nfs::wcc_attr {
                size: v.size,
                mtime: v.mtime,
                ctime: v.ctime,
            };
            nfs::pre_op_attr::attributes(wccattr)
        }
        Err(stat) => {
            error!("Cannot stat directory");
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };

    let res = context.vfs.mkdir(dirid, &args.dirops.name).await;

    let post_dir_attr = match context.vfs.getattr(dirid).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };
    let wcc_res = nfs::wcc_data {
        before: pre_dir_attr,
        after: post_dir_attr,
    };

    match res {
        Ok((fid, fattr)) => {
            debug!("mkdir success --> {:?}, {:?}", fid, fattr);
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            let fh = context.vfs.id_to_fh(fid);
            nfs::post_op_fh3::handle(fh).serialize(output)?;
            nfs::post_op_attr::attributes(fattr).serialize(output)?;
            wcc_res.serialize(output)?;
        }
        Err(e) => {
            debug!("mkdir error {:?} --> {:?}", xid, e);
            make_success_reply(xid).serialize(output)?;
            e.serialize(output)?;
            wcc_res.serialize(output)?;
        }
    }

    Ok(())
}

// ============================================================================
// SYMLINK procedure
// ============================================================================

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct SYMLINK3args {
    dirops: nfs::diropargs3,
    symlink: nfs::symlinkdata3,
}
XDRStruct!(SYMLINK3args, dirops, symlink);

pub async fn nfsproc3_symlink(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    if !matches!(context.vfs.capabilities(), VFSCapabilities::ReadWrite) {
        warn!("No write capabilities.");
        make_success_reply(xid).serialize(output)?;
        nfs::nfsstat3::NFS3ERR_ROFS.serialize(output)?;
        nfs::wcc_data::default().serialize(output)?;
        return Ok(());
    }
    let mut args = SYMLINK3args::default();
    args.deserialize(input)?;

    debug!("nfsproc3_symlink({:?}, {:?}) ", xid, args);

    let dirid = match context.vfs.fh_to_id(&args.dirops.dir) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            error!("Directory does not exist");
            return Ok(());
        }
    };

    let pre_dir_attr = match context.vfs.getattr(dirid).await {
        Ok(v) => {
            let wccattr = nfs::wcc_attr {
                size: v.size,
                mtime: v.mtime,
                ctime: v.ctime,
            };
            nfs::pre_op_attr::attributes(wccattr)
        }
        Err(stat) => {
            error!("Cannot stat directory");
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };

    let res = context
        .vfs
        .symlink(
            dirid,
            &args.dirops.name,
            &args.symlink.symlink_data,
            &args.symlink.symlink_attributes,
        )
        .await;

    let post_dir_attr = match context.vfs.getattr(dirid).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };
    let wcc_res = nfs::wcc_data {
        before: pre_dir_attr,
        after: post_dir_attr,
    };

    match res {
        Ok((fid, fattr)) => {
            debug!("symlink success --> {:?}, {:?}", fid, fattr);
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            let fh = context.vfs.id_to_fh(fid);
            nfs::post_op_fh3::handle(fh).serialize(output)?;
            nfs::post_op_attr::attributes(fattr).serialize(output)?;
            wcc_res.serialize(output)?;
        }
        Err(e) => {
            debug!("symlink error --> {:?}", e);
            make_success_reply(xid).serialize(output)?;
            e.serialize(output)?;
            wcc_res.serialize(output)?;
        }
    }

    Ok(())
}

// ============================================================================
// READLINK procedure
// ============================================================================

pub async fn nfsproc3_readlink(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut handle = nfs::nfs_fh3::default();
    handle.deserialize(input)?;
    debug!("nfsproc3_readlink({:?},{:?}) ", xid, handle);

    let id = match context.vfs.fh_to_id(&handle) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            return Ok(());
        }
    };
    let symlink_attr = match context.vfs.getattr(id).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            return Ok(());
        }
    };
    match context.vfs.readlink(id).await {
        Ok(path) => {
            debug!(" {:?} --> {:?}", xid, path);
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            symlink_attr.serialize(output)?;
            path.serialize(output)?;
        }
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            symlink_attr.serialize(output)?;
        }
    }
    Ok(())
}

// ============================================================================
// LINK procedure
// ============================================================================

/// LINK3args structure: file handle + diropargs3 (target directory + name)
#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct LINK3args {
    file: nfs::nfs_fh3,
    link: nfs::diropargs3,
}
XDRStruct!(LINK3args, file, link);

pub async fn nfsproc3_link(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    // Check write capability
    if !matches!(context.vfs.capabilities(), VFSCapabilities::ReadWrite) {
        warn!("No write capabilities for LINK.");
        make_success_reply(xid).serialize(output)?;
        nfs::nfsstat3::NFS3ERR_ROFS.serialize(output)?;
        nfs::post_op_attr::Void.serialize(output)?;
        nfs::wcc_data::default().serialize(output)?;
        return Ok(());
    }

    // Parse LINK3args
    let mut args = LINK3args::default();
    args.deserialize(input)?;
    debug!("nfsproc3_link({:?}, {:?})", xid, args);

    // Validate source file handle
    let file_id = match context.vfs.fh_to_id(&args.file) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };

    // Validate target directory handle
    let dir_id = match context.vfs.fh_to_id(&args.link.dir) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };

    // Get source file attributes (for post_op_attr)
    let file_attr = match context.vfs.getattr(file_id).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };

    // Get pre-op attributes for target directory (for wcc_data)
    let pre_dir_attr = match context.vfs.getattr(dir_id).await {
        Ok(v) => {
            let wccattr = nfs::wcc_attr {
                size: v.size,
                mtime: v.mtime,
                ctime: v.ctime,
            };
            nfs::pre_op_attr::attributes(wccattr)
        }
        Err(stat) => {
            error!("Cannot stat target directory");
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::post_op_attr::Void.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };

    // Perform link operation
    let res = context.vfs.link(file_id, dir_id, &args.link.name).await;

    // Get post-op attributes for target directory
    let post_dir_attr = match context.vfs.getattr(dir_id).await {
        Ok(v) => nfs::post_op_attr::attributes(v),
        Err(_) => nfs::post_op_attr::Void,
    };

    let wcc_res = nfs::wcc_data {
        before: pre_dir_attr,
        after: post_dir_attr,
    };

    match res {
        Ok((_fid, fattr)) => {
            debug!("link success --> {:?}", fattr);
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            // Return updated source file attributes (nlink should be incremented)
            nfs::post_op_attr::attributes(fattr).serialize(output)?;
            wcc_res.serialize(output)?;
        }
        Err(e) => {
            error!("link error {:?} --> {:?}", xid, e);
            make_success_reply(xid).serialize(output)?;
            e.serialize(output)?;
            file_attr.serialize(output)?;
            wcc_res.serialize(output)?;
        }
    }

    Ok(())
}

// ============================================================================
// COMMIT procedure - Commit cached data to stable storage
// ============================================================================

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct COMMIT3args {
    file: nfs::nfs_fh3,
    offset: nfs::offset3,
    count: nfs::count3,
}
XDRStruct!(COMMIT3args, file, offset, count);

#[allow(non_camel_case_types)]
#[derive(Debug, Default)]
struct COMMIT3resok {
    file_wcc: nfs::wcc_data,
    verf: nfs::writeverf3,
}
XDRStruct!(COMMIT3resok, file_wcc, verf);

pub async fn nfsproc3_commit(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    let mut args = COMMIT3args::default();
    args.deserialize(input)?;
    debug!(
        "nfsproc3_commit({:?}, offset={}, count={}) ",
        xid, args.offset, args.count
    );

    let id = match context.vfs.fh_to_id(&args.file) {
        Ok(id) => id,
        Err(stat) => {
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
            return Ok(());
        }
    };

    // Get pre-op attributes
    let pre_obj_attr = match context.vfs.getattr(id).await {
        Ok(v) => {
            let wccattr = nfs::wcc_attr {
                size: v.size,
                mtime: v.mtime,
                ctime: v.ctime,
            };
            nfs::pre_op_attr::attributes(wccattr)
        }
        Err(_) => nfs::pre_op_attr::Void,
    };

    // Commit cached writes
    match context.vfs.commit(id).await {
        Ok(()) => {
            // Get post-op attributes
            let post_obj_attr = match context.vfs.getattr(id).await {
                Ok(v) => nfs::post_op_attr::attributes(v),
                Err(_) => nfs::post_op_attr::Void,
            };

            let res = COMMIT3resok {
                file_wcc: nfs::wcc_data {
                    before: pre_obj_attr,
                    after: post_obj_attr,
                },
                verf: context.vfs.serverid(),
            };

            debug!("commit success {:?}", xid);
            make_success_reply(xid).serialize(output)?;
            nfs::nfsstat3::NFS3_OK.serialize(output)?;
            res.serialize(output)?;
        }
        Err(stat) => {
            error!("commit error {:?} --> {:?}", xid, stat);
            make_success_reply(xid).serialize(output)?;
            stat.serialize(output)?;
            nfs::wcc_data::default().serialize(output)?;
        }
    }

    Ok(())
}
