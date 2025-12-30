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

//! NFSv4 OPEN Operation
//!
//! Opens a file and creates state. Validates claim type, manages open owner,
//! reuses existing state when possible, and optionally grants delegation.
//! Supports CREATE operation within OPEN.

use crate::nfs4::compound::CompoundContext;
use crate::nfs4::compound::CompoundHandler;
use crate::nfs4::delegation::encode_open_delegation;
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::ops::setattr::parse_setattr_attrs;
use crate::nfs4::types::*;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;

/// OPEN operation handler
pub async fn op_open(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let _seqid = input.read_u32::<BigEndian>()?;
    let share_access = input.read_u32::<BigEndian>()?;
    let share_deny = input.read_u32::<BigEndian>()?;

    let clientid = input.read_u64::<BigEndian>()?;
    let mut owner_data: Vec<u8> = Vec::new();
    owner_data.deserialize(input)?;

    let opentype = input.read_u32::<BigEndian>()?;

    #[allow(clippy::type_complexity)]
    let mut create_attrs: Option<(
        Option<u32>,
        Option<u32>,
        Option<u32>,
        Option<u64>,
        Option<Nfstime4>,
        Option<Nfstime4>,
    )> = None;

    if opentype == 1 {
        let createmode = input.read_u32::<BigEndian>()?;

        match createmode {
            0 | 1 => {
                let mut fattr = Fattr4::default();
                fattr.deserialize(input)?;
                let attrs = parse_setattr_attrs(&fattr)?;
                create_attrs = Some(attrs);
            }
            2 => {
                // TODO: Implement EXCLUSIVE4 verifier check when needed
                let mut v = [0u8; 8];
                input.read_exact(&mut v)?;
                let _verifier = v;
            }
            3 => {
                // TODO: Implement EXCLUSIVE4_1 verifier check when needed
                let mut v = [0u8; 8];
                input.read_exact(&mut v)?;
                let _verifier = v;
                let mut fattr = Fattr4::default();
                fattr.deserialize(input)?;
                let attrs = parse_setattr_attrs(&fattr)?;
                create_attrs = Some(attrs);
            }
            _ => {
                return Err(Nfs4Status::Inval.into());
            }
        }
    }

    let claim_type = input.read_u32::<BigEndian>()?;
    let mut filename: Option<Vec<u8>> = None;

    if claim_type == 0 {
        let mut name: Vec<u8> = Vec::new();
        name.deserialize(input)?;
        filename = Some(name);
    }

    validate_claim(claim_type, ctx)?;

    let is_claim_previous = claim_type == 1;

    let fh = ctx.require_current_fh()?;
    let parent_id = handler.fs.fh_to_fileid(fh)?;

    let (fileid, is_create) = if let Some(name) = filename {
        let name_str = String::from_utf8_lossy(&name);

        if opentype == 1 {
            let (fid, _status) = handler.fs.create_file(parent_id, &name_str).await?;

            if let Some((mode, uid, gid, size, atime, mtime)) = create_attrs {
                if mode.is_some()
                    || uid.is_some()
                    || gid.is_some()
                    || size.is_some()
                    || atime.is_some()
                    || mtime.is_some()
                {
                    handler
                        .fs
                        .setattr(fid, mode, uid, gid, size, atime, mtime)
                        .await?;
                }
            }

            (fid, true)
        } else {
            let (fid, _status) = handler.fs.lookup(parent_id, &name_str).await?;
            (fid, false)
        }
    } else {
        (parent_id, false)
    };

    let open_state = open_ex(
        handler,
        clientid,
        owner_data,
        fileid,
        share_access,
        share_deny,
        is_create,
    )
    .await?;

    if is_claim_previous {
        open_state.set_confirmed(true);
    }

    ctx.current_fh = Some(handler.fs.fileid_to_fh(fileid));

    let mut result = Vec::new();

    let new_seqid = open_state.next_seqid();
    let response_stateid = Stateid4::new(new_seqid, open_state.stateid.other);
    response_stateid.serialize(&mut result)?;

    1u32.serialize(&mut result)?;
    0u64.serialize(&mut result)?;
    0u64.serialize(&mut result)?;

    let mut rflags = 0u32;
    if ctx.minor_version == 0 {
        if !open_state.is_confirmed() {
            rflags |= 0x00000002; // OPEN4_RESULT_CONFIRM
        }
    } else {
        open_state.set_confirmed(true);
    }
    rflags |= 0x00000001; // OPEN4_RESULT_LOCKTYPE_POSIX
    rflags.serialize(&mut result)?;

    0u32.serialize(&mut result)?;

    if handler.delegations.is_enabled() && !is_create {
        // Try to grant delegation
        let delegation = handler.delegations.try_grant(
            clientid,
            fileid,
            share_access,
            handler.fs.fileid_to_fh(fileid),
        );

        let deleg_bytes =
            encode_open_delegation(delegation.as_ref(), &handler.fs.fileid_to_fh(fileid))?;
        result.extend_from_slice(&deleg_bytes);
    } else {
        0u32.serialize(&mut result)?;
    }

    Ok(result)
}

/// Extended OPEN operation
///
/// Checks if state exists for (file, owner), reuses existing state or creates new one,
/// and opens/reopens the file accordingly.
async fn open_ex(
    handler: &CompoundHandler,
    clientid: Clientid4,
    owner_val: Vec<u8>,
    fileid: Fileid4,
    access: u32,
    deny: u32,
    _is_create: bool,
) -> Nfs4Result<std::sync::Arc<crate::nfs4::state::open::OpenState>> {
    let path = handler.fs.get_path(fileid)?;

    let (open_state, new_state) = handler.opens.open(
        clientid,
        owner_val.clone(),
        fileid,
        path.clone(),
        access,
        deny,
    )?;

    if new_state {
        handler.fs.open_file(fileid, access).await?;
    } else {
        handler.fs.reopen_file_ex(fileid, access, false).await?;
    }

    Ok(open_state)
}

/// Validate claim type
fn validate_claim(claim_type: u32, _ctx: &CompoundContext) -> Nfs4Result<()> {
    match claim_type {
        0 => Ok(()),                          // CLAIM_NULL
        1 => Ok(()),                          // CLAIM_PREVIOUS
        2 => Err(Nfs4Status::Notsupp.into()), // CLAIM_DELEGATE_CUR
        3 => Err(Nfs4Status::Notsupp.into()), // CLAIM_DELEGATE_PREV
        _ => Err(Nfs4Status::Inval.into()),
    }
}
