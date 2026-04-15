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
use crate::nfs4::delegation::{encode_open_delegation, Grant};
use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::ops::setattr::parse_setattr_attrs;
use crate::nfs4::types::*;
use crate::protocol::xdr::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;

enum OpenClaim {
    Null(Vec<u8>),
    Previous,
    DelegCur { stateid: Stateid4, name: Vec<u8> },
    DelegPrev,
    Fh,
    DelegPrevFh,
    DelegCurFh(Stateid4),
}

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

    tracing::debug!("OPEN: opentype={} (0=NOCREATE, 1=CREATE)", opentype);

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
                tracing::debug!("OPEN CREATE: createmode={} (UNCHECKED/GUARDED)", createmode);
                let mut fattr = Fattr4::default();
                fattr.deserialize(input)?;
                let attrs = parse_setattr_attrs(&fattr)?;
                create_attrs = Some(attrs);
            }
            2 => {
                tracing::debug!("OPEN CREATE: createmode=2 (EXCLUSIVE4)");
                // TODO: Implement EXCLUSIVE4 verifier check when needed
                let mut v = [0u8; 8];
                input.read_exact(&mut v)?;
                let _verifier = v;
            }
            3 => {
                tracing::debug!("OPEN CREATE: createmode=3 (EXCLUSIVE4_1)");
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
                tracing::error!("OPEN CREATE: invalid createmode={}", createmode);
                return Err(Nfs4Status::Inval.into());
            }
        }
    }

    let claim_type = input.read_u32::<BigEndian>()?;
    let claim = parse_open_claim(input, claim_type)?;

    // Validate claim type and check grace period (NFS-Ganesha aligned)
    validate_claim(&claim, ctx, handler, clientid)?;

    let is_claim_previous = matches!(claim, OpenClaim::Previous);

    let fh = ctx.require_current_fh()?;
    let current_id = handler.fs.fh_to_fileid(fh)?;
    let delegated_target = match &claim {
        OpenClaim::DelegCur { stateid, name } => {
            let name = std::str::from_utf8(name).map_err(|_| Nfs4Status::Badchar)?;
            let (fileid, _) = handler.fs.lookup(current_id, name).await?;
            let deleg = handler.delegations.verify_stateid(stateid)?;
            if deleg.fileid != fileid {
                return Err(Nfs4Status::BadStateid.into());
            }
            Some(fileid)
        }
        OpenClaim::DelegCurFh(stateid) => {
            let deleg = handler.delegations.verify_stateid(stateid)?;
            if deleg.fileid != current_id {
                return Err(Nfs4Status::BadStateid.into());
            }
            Some(current_id)
        }
        _ => None,
    };
    let object_id = delegated_target.unwrap_or(current_id);

    // Get parent directory's change attribute BEFORE operation (NFS-Ganesha aligned)
    // NFS-Ganesha: nfs4_op_open.c line 1456-1457, 1543-1563
    // obj_change is set to current_obj (parent directory) and changeid is retrieved
    let parent_status_before = handler.fs.get_status(object_id).await?;
    let change_before = parent_status_before.mtime as u64;
    let is_parent_pre_attrs_valid = true; // We always have valid change attribute

    // Check delegation conflict BEFORE opening file (NFS-Ganesha aligned)
    // NFS-Ganesha: state_deleg_conflict_impl() check in open4_ex()
    let is_write_open = (share_access & 0x02) != 0;
    let fileid_to_check = if !matches!(claim, OpenClaim::DelegCur { .. } | OpenClaim::DelegCurFh(_))
    {
        match &claim {
            OpenClaim::Null(name) => {
                let name_str = String::from_utf8_lossy(name);
                match handler.fs.lookup(current_id, &name_str).await {
                    Ok((fid, _)) => Some(fid),
                    Err(_) => None,
                }
            }
            _ => Some(object_id),
        }
    } else {
        None
    };
    if let Some(fileid_to_check) = fileid_to_check {
        // Check if this access conflicts with existing delegation. Ganesha
        // schedules CB_RECALL first, then returns NFS4ERR_DELAY.
        if handler
            .delegations
            .check_and_recall_if_needed(
                clientid,
                fileid_to_check,
                is_write_open,
                handler.fs.fileid_to_fh(fileid_to_check),
            )
            .await?
        {
            // Return NFS4ERR_DELAY to tell client to retry later
            // NFS-Ganesha: state_deleg_conflict_impl() returns NFS4ERR_DELAY
            tracing::warn!(
                "OPEN: delegation conflict detected for file {}, client {} retry later",
                fileid_to_check,
                clientid
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
        fileid = object_id;
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
        let (fid, created) = if let OpenClaim::Null(name) = &claim {
            let name_str = String::from_utf8_lossy(&name);

            if opentype == 1 {
                let (fid, _status) = handler.fs.create_file(current_id, &name_str).await?;

                // Extract client-provided attributes (if any)
                let (mode, uid, gid, size, atime, mtime) = if let Some(attrs) = create_attrs {
                    attrs
                } else {
                    (None, None, None, None, None, None)
                };

                // Use RPC auth credentials if client didn't specify owner/group
                // This aligns with nfs-ganesha behavior and CREATE operation
                let effective_uid = uid.or(Some(ctx.auth.uid));
                let effective_gid = gid.or(Some(ctx.auth.gid));

                // Always call setattr to ensure owner/group are set
                if mode.is_some()
                    || effective_uid.is_some()
                    || effective_gid.is_some()
                    || size.is_some()
                    || atime.is_some()
                    || mtime.is_some()
                {
                    handler
                        .fs
                        .setattr(fid, mode, effective_uid, effective_gid, size, atime, mtime)
                        .await?;
                }

                (fid, true)
            } else {
                let (fid, _status) = handler.fs.lookup(current_id, &name_str).await?;
                (fid, false)
            }
        } else {
            (object_id, false)
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
    ctx.current_stateid = Some(response_stateid);
    response_stateid.serialize(&mut result)?;

    // change_info4: Build from parent directory's change attributes (NFS-Ganesha aligned)
    // NFS-Ganesha: nfs4_op_open.c line 1543-1563
    // Get parent directory's change attribute AFTER operation
    let parent_status_after = handler.fs.get_status(object_id).await?;
    let change_after = parent_status_after.mtime as u64;
    let is_parent_post_attrs_valid = true; // We always have valid change attribute

    // Debug: log change_info values
    tracing::info!(
        "OPEN change_info: before={} after={} atomic={}",
        change_before,
        change_after,
        is_parent_pre_attrs_valid && is_parent_post_attrs_valid
    );

    // atomic = true only if both pre and post attrs are valid (NFS-Ganesha line 1561-1563)
    let atomic = is_parent_pre_attrs_valid && is_parent_post_attrs_valid;
    atomic.serialize(&mut result)?; // atomic
    change_before.serialize(&mut result)?; // before
    change_after.serialize(&mut result)?; // after

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

    // attrset bitmap (empty)
    0u32.serialize(&mut result)?;

    // Delegation handling (NFS-Ganesha aligned: nfs4_op_open.c line 605-609)
    // Record open for delegation heuristics (NFS-Ganesha: fds_num_opens tracking)
    let is_write_open = (share_access & 0x02) != 0;
    handler.delegations.record_open(fileid, is_write_open);

    // OPEN4_SHARE_ACCESS_WANT_DELEG_MASK = 0x0F00
    let want_deleg_mask: u32 = 0x0F00;
    let client_wants_deleg = (share_access & want_deleg_mask) != 0;

    // FIXED: Disabled forced delegation grant
    // Without real RPC backchannel, forced delegation causes client confusion
    // and performance degradation (20-50% slower due to client retries)
    let force_grant_delegation = false;

    let file_status = if !is_create && client_wants_deleg {
        Some(handler.fs.get_status(fileid).await?)
    } else {
        None
    };

    let (delegation, why_none) = if !is_create {
        if force_grant_delegation && !handler.delegations.is_enabled() {
            tracing::warn!("EXPERIMENTAL: Force granting READ delegation without backchannel!");
            (
                Some(crate::nfs4::delegation::Delegation::new(
                    handler.delegations.generate_stateid_unsafe(),
                    clientid,
                    fileid,
                    crate::nfs4::delegation::DelegationType::Read,
                )),
                None,
            )
        } else if file_status
            .as_ref()
            .map(|status| status.file_type != curvine_common::state::FileType::File)
            .unwrap_or(false)
        {
            (
                None,
                Some(crate::nfs4::delegation::why_no_delegation::WND4_NOT_SUPP_FTYPE),
            )
        } else {
            match handler.delegations.grant_or_reason(
                clientid,
                fileid,
                share_access,
                handler.fs.fileid_to_fh(fileid),
            ) {
                Grant::Granted(delegation) => (Some(delegation), None),
                Grant::Denied(why_none) => (None, Some(why_none)),
            }
        }
    } else {
        (
            None,
            Some(crate::nfs4::delegation::why_no_delegation::WND4_NOT_SUPP_FTYPE),
        )
    };

    let deleg_bytes = encode_open_delegation(
        delegation.as_ref(),
        why_none,
        &handler.fs.fileid_to_fh(fileid),
        ctx.minor_version,
        client_wants_deleg,
    )?;
    result.extend_from_slice(&deleg_bytes);

    Ok(result)
}

fn parse_open_claim(input: &mut impl Read, claim_type: u32) -> Nfs4Result<OpenClaim> {
    match claim_type {
        0 => {
            let mut name = Vec::new();
            name.deserialize(input)?;
            Ok(OpenClaim::Null(name))
        }
        1 => Ok(OpenClaim::Previous),
        2 => {
            let mut stateid = Stateid4::default();
            stateid.deserialize(input)?;
            let mut name = Vec::new();
            name.deserialize(input)?;
            Ok(OpenClaim::DelegCur { stateid, name })
        }
        3 => {
            let mut name: Vec<u8> = Vec::new();
            name.deserialize(input)?;
            let _ = name;
            Ok(OpenClaim::DelegPrev)
        }
        4 => Ok(OpenClaim::Fh),
        5 => {
            let mut stateid = Stateid4::default();
            stateid.deserialize(input)?;
            Ok(OpenClaim::DelegCurFh(stateid))
        }
        6 => Ok(OpenClaim::DelegPrevFh),
        _ => Err(Nfs4Status::Inval.into()),
    }
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
    claim: &OpenClaim,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
    clientid: Clientid4,
) -> Nfs4Result<()> {
    match claim {
        OpenClaim::Null(_) => {
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
        OpenClaim::Previous => {
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
        OpenClaim::DelegCur { .. } => {
            if ctx.minor_version == 0 {
                Err(Nfs4Status::Notsupp.into())
            } else if handler.delegations.is_enabled() {
                Ok(())
            } else {
                Err(Nfs4Status::Notsupp.into())
            }
        }
        OpenClaim::DelegCurFh(_) => {
            if ctx.minor_version == 0 {
                Err(Nfs4Status::Notsupp.into())
            } else if !handler.delegations.is_enabled() {
                Err(Nfs4Status::Notsupp.into())
            } else {
                Ok(())
            }
        }
        OpenClaim::DelegPrev | OpenClaim::DelegPrevFh => Err(Nfs4Status::Notsupp.into()),
        OpenClaim::Fh => Ok(()),
    }
}
