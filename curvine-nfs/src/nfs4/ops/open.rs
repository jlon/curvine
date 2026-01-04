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

    // Debug: log raw share_access to see delegation want flags
    // OPEN4_SHARE_ACCESS_WANT_DELEG_MASK = 0x0F00
    tracing::info!(
        "OPEN request: share_access={:#x} (want_deleg_mask={:#x}) share_deny={:#x}",
        share_access,
        share_access & 0x0F00,
        share_deny
    );

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

    // Validate claim type and check grace period (NFS-Ganesha aligned)
    validate_claim(claim_type, ctx, handler, clientid)?;

    let is_claim_previous = claim_type == 1;

    let fh = ctx.require_current_fh()?;
    let parent_id = handler.fs.fh_to_fileid(fh)?;

    // Check delegation conflict BEFORE opening file (NFS-Ganesha aligned)
    // NFS-Ganesha: state_deleg_conflict_impl() check in open4_ex()
    let is_write_open = (share_access & 0x02) != 0;
    if let Some(fileid_to_check) = match filename.as_ref() {
        Some(name) => {
            let name_str = String::from_utf8_lossy(name);
            match handler.fs.lookup(parent_id, &name_str).await {
                Ok((fid, _)) => Some(fid),
                Err(_) => None, // File doesn't exist yet
            }
        }
        None => Some(parent_id), // Using current filehandle
    } {
        // Check if this access conflicts with existing delegation
        if handler.delegations.needs_recall(fileid_to_check, clientid, is_write_open) {
            // Return NFS4ERR_DELAY to tell client to retry later
            // NFS-Ganesha: state_deleg_conflict_impl() returns NFS4ERR_DELAY
            tracing::warn!(
                "OPEN: delegation conflict detected for file {}, client {} retry later",
                fileid_to_check, clientid
            );
            return Err(Nfs4Status::Delay.into());
        }
    }

    // Variables that will be set by either branch
    let fileid: u64;
    let is_create: bool;

    // Handle CLAIM_PREVIOUS: find and restore persisted state
    let open_state = if is_claim_previous {
        // CLAIM_PREVIOUS: use current filehandle (NFS-Ganesha aligned)
        fileid = parent_id;
        is_create = false; // CLAIM_PREVIOUS is never a create

        // Find persisted state by (fileid, owner_val)
        match handler.opens.find_persisted_state(fileid, &owner_data) {
            Some(persisted_state) => {
                // Found persisted state - confirm it (NFS-Ganesha: so_confirmed = true)
                persisted_state.set_confirmed(true);

                // Reopen file (NFS-Ganesha: fsal_reopen2 with FSAL_O_RECLAIM)
                handler
                    .fs
                    .reopen_file_ex(fileid, persisted_state.get_access() | share_access, true)
                    .await?;

                tracing::info!(
                    "OPEN CLAIM_PREVIOUS: reclaimed stateid={:?} fileid={}",
                    persisted_state.stateid,
                    fileid
                );

                persisted_state
            }
            None => {
                // No persisted state found - return error (NFS-Ganesha: NFS4ERR_RECLAIM_BAD)
                tracing::warn!(
                    "OPEN CLAIM_PREVIOUS: no persisted state found for fileid={} owner={:02x?}",
                    fileid,
                    &owner_data[..owner_data.len().min(8)]
                );
                return Err(Nfs4Status::ReclaimBad.into());
            }
        }
    } else {
        // CLAIM_NULL: normal open path
        let (fid, created) = if let Some(name) = filename {
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

        fileid = fid;
        is_create = created;

        open_ex(
            handler,
            clientid,
            owner_data,
            fileid,
            share_access,
            share_deny,
            is_create,
        )
        .await?
    };

    let path = handler.fs.get_path(fileid).ok();
    tracing::info!(
        "OPEN: fileid={} path={} stateid={:?} seqid={} access={:#x} deny={:#x} is_create={}",
        fileid,
        path.as_ref().map(|p| p.path()).unwrap_or("unknown"),
        open_state.stateid,
        open_state.seqid(),
        share_access,
        share_deny,
        is_create
    );

    // CLAIM_PREVIOUS already confirmed the state above
    // For CLAIM_NULL, confirmation is handled in open_ex

    ctx.current_fh = Some(handler.fs.fileid_to_fh(open_state.fileid));

    let mut result = Vec::new();

    let new_seqid = open_state.next_seqid();
    let response_stateid = Stateid4::new(new_seqid, open_state.stateid.other);
    response_stateid.serialize(&mut result)?;

    // change_info4: atomic=true, before=0, after=0
    1u32.serialize(&mut result)?; // atomic
    0u64.serialize(&mut result)?; // before
    0u64.serialize(&mut result)?; // after

    let mut rflags = 0u32;
    if ctx.minor_version == 0 {
        if !open_state.is_confirmed() {
            rflags |= 0x00000002; // OPEN4_RESULT_CONFIRM
        }
    } else {
        open_state.set_confirmed(true);
    }
    // NFS-Ganesha: OPEN4_RESULT_LOCKTYPE_POSIX = 0x00000004 (nfsv41.h line 1667)
    rflags |= 0x00000004; // OPEN4_RESULT_LOCKTYPE_POSIX
    rflags.serialize(&mut result)?;

    tracing::info!(
        "OPEN response: stateid={:?} rflags={:#x} minor_version={}",
        response_stateid,
        rflags,
        ctx.minor_version
    );

    // attrset bitmap (empty)
    0u32.serialize(&mut result)?;

    // Delegation handling (NFS-Ganesha aligned: nfs4_op_open.c line 605-609)
    // Record open for delegation heuristics (NFS-Ganesha: fds_num_opens tracking)
    let is_write_open = (share_access & 0x02) != 0;
    handler.delegations.record_open(fileid, is_write_open);

    // OPEN4_SHARE_ACCESS_WANT_DELEG_MASK = 0x0F00
    let want_deleg_mask: u32 = 0x0F00;
    let client_wants_deleg = (share_access & want_deleg_mask) != 0;

    let deleg_enabled = handler.delegations.is_enabled();
    tracing::info!(
        "OPEN delegation: enabled={} is_create={} is_write={} want_deleg={} minor_version={}",
        deleg_enabled,
        is_create,
        is_write_open,
        client_wants_deleg,
        ctx.minor_version
    );

    if deleg_enabled && !is_create {
        // Try to grant delegation
        let delegation = handler.delegations.try_grant(
            clientid,
            fileid,
            share_access,
            handler.fs.fileid_to_fh(fileid),
        );

        tracing::info!(
            "OPEN delegation result: {:?}",
            delegation.as_ref().map(|d| d.deleg_type)
        );

        let deleg_bytes = encode_open_delegation(
            delegation.as_ref(),
            &handler.fs.fileid_to_fh(fileid),
            ctx.minor_version,
            client_wants_deleg,
        )?;
        tracing::info!("OPEN delegation bytes: len={}", deleg_bytes.len());
        result.extend_from_slice(&deleg_bytes);
    } else {
        // No delegation - encode based on minor version and client request
        // NFS-Ganesha: nfs4_op_open.c line 605-609
        let deleg_bytes = encode_open_delegation(
            None,
            &handler.fs.fileid_to_fh(fileid),
            ctx.minor_version,
            client_wants_deleg,
        )?;
        tracing::info!("OPEN delegation: NONE, bytes len={}", deleg_bytes.len());
        result.extend_from_slice(&deleg_bytes);
    }

    tracing::info!("OPEN response total bytes: len={}", result.len());

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

    tracing::debug!(
        "OPEN: fileid={} path={} access={:#x} deny={:#x} clientid={} owner={:02x?}",
        fileid,
        path.path(),
        access,
        deny,
        clientid,
        &owner_val[..owner_val.len().min(8)]
    );

    let (open_state, new_state) = handler.opens.open(
        clientid,
        owner_val.clone(),
        fileid,
        path.clone(),
        access,
        deny,
    )?;

    tracing::debug!(
        "OPEN: fileid={} new_state={} stateid={:?}",
        fileid,
        new_state,
        open_state.stateid
    );

    if new_state {
        // New state: create OpenFile with ref_count = 1
        // NFS-Ganesha: fsal_open2() for new state
        tracing::debug!("OPEN: fileid={} calling open_file (new state)", fileid);
        handler.fs.open_file(fileid, access).await?;
    } else {
        // Existing state reused: increment ref_count
        // NFS-Ganesha: fsal_reopen2() for existing state, increments fd_work
        // Each OPEN (new or reused state) represents a file handle that will be CLOSEd
        tracing::debug!(
            "OPEN: fileid={} calling reopen_file_ex with add_ref=true (reused state)",
            fileid
        );
        handler.fs.reopen_file_ex(fileid, access, true).await?;
    }

    tracing::debug!("OPEN: fileid={} completed successfully", fileid);

    Ok(open_state)
}

/// Validate claim type and check grace period (NFS-Ganesha aligned)
///
/// Reference: nfs4_op_open.c:open4_validate_claim()
fn validate_claim(
    claim_type: u32,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
    clientid: Clientid4,
) -> Nfs4Result<()> {
    match claim_type {
        0 => {
            // CLAIM_NULL: normal open
            // NFS-Ganesha: nfs4_op_open.c line 137-140
            // In NFSv4.1, client MUST call RECLAIM_COMPLETE before CLAIM_NULL opens
            if ctx.minor_version > 0 {
                // Get client state
                let client = handler
                    .clients
                    .get_client(clientid)
                    .ok_or(Nfs4Status::StaleClientid)?;

                // Check if reclaim complete (NFS-Ganesha: cid_cb.v41.cid_reclaim_complete)
                if !client.is_reclaim_complete() {
                    tracing::debug!(
                        "CLAIM_NULL: client {} has not completed RECLAIM_COMPLETE, returning GRACE",
                        clientid
                    );
                    return Err(Nfs4Status::Grace.into());
                }
            }
            Ok(())
        }
        1 => {
            // CLAIM_PREVIOUS: reclaim, must be in grace period
            // Check grace period (NFS-Ganesha: nfs_get_grace_status(want_grace=true))
            let _guard = handler
                .grace
                .acquire_grace_status(true)
                .map_err(|_| Nfs4Status::NoGrace)?;

            // Get client state
            let client = handler
                .clients
                .get_client(clientid)
                .ok_or(Nfs4Status::StaleClientid)?;

            // Check if client allows reclaim (NFS-Ganesha: cid_allow_reclaim)
            if !client.allow_reclaim() {
                tracing::warn!("CLAIM_PREVIOUS: client {} does not allow reclaim", clientid);
                return Err(Nfs4Status::NoGrace.into());
            }

            // Check if reclaim already complete (NFSv4.1 only)
            // NFS-Ganesha: nfs4_op_open.c line 155-158
            if ctx.minor_version > 0 && client.is_reclaim_complete() {
                tracing::warn!(
                    "CLAIM_PREVIOUS: client {} already completed reclaim",
                    clientid
                );
                return Err(Nfs4Status::NoGrace.into());
            }

            Ok(())
        }
        2 => Err(Nfs4Status::Notsupp.into()), // CLAIM_DELEGATE_CUR
        3 => Err(Nfs4Status::Notsupp.into()), // CLAIM_DELEGATE_PREV
        _ => Err(Nfs4Status::Inval.into()),
    }
}
