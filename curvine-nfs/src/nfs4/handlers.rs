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

use crate::nfs4::compound::{CompoundContext, CompoundHandler, Nfs4Op};
use crate::nfs4::error::{Nfs4Error, Nfs4Result, Nfs4Status};
use crate::nfs4::types::*;
use crate::nfs4::{NFS4_MINOR_VERSION, NFS4_VERSION};
use crate::protocol::rpc::*;
use crate::protocol::xdr::*;
use crate::server::context::RPCContext;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Read, Write};
use tracing::{debug, error, info, warn};

/// Handle NFSv4 RPC request
pub async fn handle_nfs4(
    xid: u32,
    call: call_body,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    // Verify version
    if call.vers != NFS4_VERSION {
        warn!("Invalid NFS version {} != {}", call.vers, NFS4_VERSION);
        prog_mismatch_reply_message(xid, NFS4_VERSION).serialize(output)?;
        return Ok(());
    }

    // NFSv4 only has procedure 0 (NULL) and 1 (COMPOUND)
    match call.proc {
        0 => {
            info!("NFSv4 NULL({}) - ping request", xid);
            handle_null(xid, output)?;
        }
        1 => {
            info!("NFSv4 COMPOUND({}) - starting compound request", xid);
            handle_compound(xid, input, output, context).await?;
        }
        _ => {
            warn!("Unknown NFSv4 procedure {}", call.proc);
            proc_unavail_reply_message(xid).serialize(output)?;
        }
    }

    Ok(())
}

/// Handle NULL procedure
fn handle_null(xid: u32, output: &mut impl Write) -> Result<(), anyhow::Error> {
    info!("NFSv4 NULL({}) - responding to ping", xid);
    make_success_reply(xid).serialize(output)?;
    Ok(())
}

/// Handle COMPOUND procedure
async fn handle_compound(
    xid: u32,
    input: &mut impl Read,
    output: &mut impl Write,
    context: &RPCContext,
) -> Result<(), anyhow::Error> {
    // Read COMPOUND4args
    let mut tag: Vec<u8> = Vec::new();
    tag.deserialize(input)?;

    let minor_version = input.read_u32::<BigEndian>()?;
    let op_count = input.read_u32::<BigEndian>()? as usize;

    info!(
        "NFSv4 COMPOUND({}) tag={:?} minor={} ops={}",
        xid,
        String::from_utf8_lossy(&tag),
        minor_version,
        op_count
    );

    // Verify minor version - support both NFSv4.0 (0) and NFSv4.1 (1)
    if minor_version > NFS4_MINOR_VERSION {
        warn!(
            "Unsupported NFSv4 minor version: {}, max supported: {}",
            minor_version, NFS4_MINOR_VERSION
        );
        make_success_reply(xid).serialize(output)?;
        Nfs4Status::MinorVersMismatch.serialize(output)?;
        tag.serialize(output)?;
        0u32.serialize(output)?; // resarray count
        return Ok(());
    }

    // Check operation count
    if op_count > crate::nfs4::MAX_COMPOUND_OPS {
        make_success_reply(xid).serialize(output)?;
        Nfs4Status::TooManyOps.serialize(output)?;
        tag.serialize(output)?;
        0u32.serialize(output)?;
        return Ok(());
    }

    // Create compound context with minor version
    let mut ctx = CompoundContext::with_minor_version(minor_version);

    // Collect results
    let mut results: Vec<(Nfs4Op, Nfs4Status, Vec<u8>)> = Vec::with_capacity(op_count);
    let mut overall_status = Nfs4Status::Ok;

    // Process each operation
    for i in 0..op_count {
        let op_code = input.read_u32::<BigEndian>()?;
        let op = Nfs4Op::from(op_code);

        info!("  Op[{}]: {:?} ({})", i, op, op_code);

        // Execute operation
        let (status, result_data) = match execute_operation(op, input, &mut ctx, context).await {
            Ok(data) => (Nfs4Status::Ok, data),
            Err(e) => {
                error!("  Op[{}] {:?} failed: {:?}", i, op, e.status);
                (e.status, Vec::new())
            }
        };

        results.push((op, status, result_data));

        // Stop on first error
        if status != Nfs4Status::Ok {
            overall_status = status;
            // Skip remaining operations
            skip_remaining_ops(input, op_count - i - 1)?;
            break;
        }
    }

    // Write response
    info!(
        "COMPOUND response: xid={} status={:?} tag_len={} results_count={}",
        xid,
        overall_status,
        tag.len(),
        results.len()
    );

    make_success_reply(xid).serialize(output)?;
    overall_status.serialize(output)?;
    tag.serialize(output)?;
    (results.len() as u32).serialize(output)?;

    for (op, status, data) in results {
        info!(
            "  Result: op={:?}({}) status={:?} data_len={}",
            op,
            op as u32,
            status,
            data.len()
        );
        (op as u32).serialize(output)?;
        status.serialize(output)?;
        if status == Nfs4Status::Ok {
            output.write_all(&data)?;
        }
    }

    // Cache reply for replay detection
    if let (Some(sessionid), Some(slot_id)) = (ctx.sessionid, ctx.slot_id) {
        // In a real implementation, we'd cache the entire response
        // For now, we just release the slot
        if let Some(handler) = context.nfs4_handler.as_ref() {
            handler
                .sessions
                .cache_reply(&sessionid, slot_id, Vec::new());
        }
    }

    Ok(())
}

/// Skip remaining operations after an error
fn skip_remaining_ops(input: &mut impl Read, count: usize) -> Result<(), anyhow::Error> {
    for _ in 0..count {
        let op_code = input.read_u32::<BigEndian>()?;
        let op = Nfs4Op::from(op_code);
        skip_operation_args(op, input)?;
    }
    Ok(())
}

/// Skip operation arguments (for error recovery)
fn skip_operation_args(op: Nfs4Op, input: &mut impl Read) -> Result<(), anyhow::Error> {
    info!("Skipping arguments for operation {:?}", op);

    match op {
        Nfs4Op::Sequence => {
            let mut buf = [0u8; 16 + 4 + 4 + 4 + 4]; // sessionid + slotid + seqid + highest + cache
            input.read_exact(&mut buf)?;
            info!("Skipped SEQUENCE args: 36 bytes");
        }
        Nfs4Op::Putfh => {
            let len = input.read_u32::<BigEndian>()? as usize;
            let pad = (4 - len % 4) % 4;
            let mut buf = vec![0u8; len + pad];
            input.read_exact(&mut buf)?;
            info!("Skipped PUTFH args: {} bytes + {} padding", len, pad);
        }
        Nfs4Op::Putrootfh | Nfs4Op::Getfh | Nfs4Op::Savefh | Nfs4Op::Restorefh => {
            info!("Skipped {:?} args: no arguments", op);
        }
        Nfs4Op::Getattr => {
            let mut bitmap: Vec<u32> = Vec::new();
            bitmap.deserialize(input)?;
            info!("Skipped GETATTR args: bitmap with {} words", bitmap.len());
        }
        Nfs4Op::Lookup => {
            let mut name: Vec<u8> = Vec::new();
            name.deserialize(input)?;
            info!(
                "Skipped LOOKUP args: name {:?}",
                String::from_utf8_lossy(&name)
            );
        }
        Nfs4Op::Secinfo => {
            let mut name: Vec<u8> = Vec::new();
            name.deserialize(input)?;
            info!(
                "Skipped SECINFO args: name {:?}",
                String::from_utf8_lossy(&name)
            );
        }
        Nfs4Op::Read => {
            let mut stateid = Stateid4::default();
            stateid.deserialize(input)?;
            let _offset = input.read_u64::<BigEndian>()?;
            let _count = input.read_u32::<BigEndian>()?;
            info!("Skipped READ args: stateid + offset + count");
        }
        Nfs4Op::Setclientid => {
            // Skip verifier (8 bytes)
            let mut verifier = [0u8; 8];
            input.read_exact(&mut verifier)?;
            // Skip client_id
            let mut client_id: Vec<u8> = Vec::new();
            client_id.deserialize(input)?;
            // Skip callback info
            let _cb_program = input.read_u32::<BigEndian>()?;
            let mut netid: Vec<u8> = Vec::new();
            netid.deserialize(input)?;
            let mut addr: Vec<u8> = Vec::new();
            addr.deserialize(input)?;
            let _callback_ident = input.read_u32::<BigEndian>()?;
            info!("Skipped SETCLIENTID args: verifier + client_id + callback");
        }
        Nfs4Op::SetclientidConfirm => {
            let _clientid = input.read_u64::<BigEndian>()?;
            let mut verifier = [0u8; 8];
            input.read_exact(&mut verifier)?;
            info!("Skipped SETCLIENTID_CONFIRM args: clientid + verifier");
        }
        _ => {
            warn!(
                "Cannot skip arguments for operation {:?} - not implemented",
                op
            );
            // For safety, try to read a small amount and hope for the best
            // This is a limitation that should be addressed for production
        }
    }
    Ok(())
}

/// Execute a single operation
async fn execute_operation(
    op: Nfs4Op,
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    rpc_ctx: &RPCContext,
) -> Nfs4Result<Vec<u8>> {
    let handler = rpc_ctx
        .nfs4_handler
        .as_ref()
        .ok_or(Nfs4Status::Serverfault)?;

    match op {
        // NFSv4.0 operations
        Nfs4Op::Setclientid => op_setclientid(input, ctx, handler).await,
        Nfs4Op::SetclientidConfirm => op_setclientid_confirm(input, ctx, handler).await,
        Nfs4Op::Renew => op_renew(input, ctx, handler).await,
        Nfs4Op::OpenConfirm => crate::nfs4::ops::open_confirm::op_open_confirm(input, ctx, handler).await,
        // NFSv4.1 operations
        Nfs4Op::Sequence => op_sequence(input, ctx, handler).await,
        Nfs4Op::ExchangeId => op_exchange_id(input, ctx, handler).await,
        Nfs4Op::CreateSession => op_create_session(input, ctx, handler).await,
        Nfs4Op::DestroySession => op_destroy_session(input, ctx, handler).await,
        // Common operations
        Nfs4Op::Putrootfh => op_putrootfh(ctx, handler),
        Nfs4Op::Putpubfh => op_putpubfh(ctx, handler),
        Nfs4Op::Putfh => op_putfh(input, ctx, handler),
        Nfs4Op::Getfh => op_getfh(ctx, handler),
        Nfs4Op::Savefh => op_savefh(ctx),
        Nfs4Op::Restorefh => op_restorefh(ctx),
        Nfs4Op::Getattr => op_getattr(input, ctx, handler).await,
        Nfs4Op::Lookup => op_lookup(input, ctx, handler).await,
        Nfs4Op::Lookupp => op_lookupp(ctx, handler).await,
        Nfs4Op::Open => op_open(input, ctx, handler).await,
        Nfs4Op::OpenDowngrade => crate::nfs4::ops::open_downgrade::op_open_downgrade(input, ctx, handler).await,
        Nfs4Op::Close => op_close(input, ctx, handler).await,
        Nfs4Op::Read => op_read(input, ctx, handler).await,
        Nfs4Op::Write => op_write(input, ctx, handler).await,
        Nfs4Op::Commit => op_commit(input, ctx, handler).await,
        Nfs4Op::Readdir => op_readdir(input, ctx, handler).await,
        Nfs4Op::Create => op_create(input, ctx, handler).await,
        Nfs4Op::Remove => op_remove(input, ctx, handler).await,
        Nfs4Op::Rename => op_rename(input, ctx, handler).await,
        Nfs4Op::Link => op_link(input, ctx, handler).await,
        Nfs4Op::Readlink => op_readlink(ctx, handler).await,
        Nfs4Op::ReclaimComplete => op_reclaim_complete(input, ctx),
        Nfs4Op::Delegreturn => op_delegreturn(input, ctx, handler).await,
        Nfs4Op::Access => op_access(input, ctx, handler).await,
        Nfs4Op::Setattr => op_setattr(input, ctx, handler).await,
        Nfs4Op::Secinfo => op_secinfo(input, ctx, handler).await,
        Nfs4Op::Nverify => op_nverify(input, ctx, handler).await,
        Nfs4Op::Verify => op_verify(input, ctx, handler).await,
        Nfs4Op::Lock => op_lock(input, ctx, handler).await,
        Nfs4Op::Lockt => op_lockt(input, ctx, handler).await,
        Nfs4Op::Locku => op_locku(input, ctx, handler).await,
        Nfs4Op::ReleaseLockowner => op_release_lockowner(input, ctx, handler).await,
        _ => {
            warn!("Unimplemented NFSv4 operation: {:?}", op);
            Err(Nfs4Status::Notsupp.into())
        }
    }
}

// ============================================================================
// Operation Implementations
// ============================================================================

/// SEQUENCE - must be first operation in most COMPOUNDs
async fn op_sequence(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut sessionid = Sessionid4::default();
    sessionid.deserialize(input)?;
    let sequenceid = input.read_u32::<BigEndian>()?;
    let slotid = input.read_u32::<BigEndian>()?;
    let _highest_slotid = input.read_u32::<BigEndian>()?;
    let _cachethis = input.read_u32::<BigEndian>()?;

    debug!(
        "SEQUENCE: session={:?} seq={} slot={}",
        &sessionid[..4],
        sequenceid,
        slotid
    );

    let (session, cached, highest, target_highest, flags) =
        handler.sessions.sequence(&sessionid, slotid, sequenceid)?;

    // Check for cached reply
    if let Some(cached_reply) = cached {
        return Ok(cached_reply.reply);
    }

    // Update context
    ctx.sessionid = Some(sessionid);
    ctx.slot_id = Some(slotid);
    ctx.clientid = Some(session.clientid);

    // Renew lease
    handler.clients.renew_lease(session.clientid)?;

    // Build response
    let mut result = Vec::new();
    sessionid.serialize(&mut result)?;
    sequenceid.serialize(&mut result)?;
    slotid.serialize(&mut result)?;
    highest.serialize(&mut result)?;
    target_highest.serialize(&mut result)?;
    flags.serialize(&mut result)?;

    Ok(result)
}

/// EXCHANGE_ID - client registration
async fn op_exchange_id(
    input: &mut impl Read,
    _ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut client_owner = ClientOwner4::default();
    client_owner.deserialize(input)?;

    let flags = input.read_u32::<BigEndian>()?;
    let _state_protect = input.read_u32::<BigEndian>()?;

    // Skip impl_id array
    let impl_count = input.read_u32::<BigEndian>()?;
    for _ in 0..impl_count {
        let mut domain: Vec<u8> = Vec::new();
        domain.deserialize(input)?;
        let mut name: Vec<u8> = Vec::new();
        name.deserialize(input)?;
        let mut time = Nfstime4::default();
        time.deserialize(input)?;
    }

    debug!("EXCHANGE_ID: owner={:?} flags={:#x}", client_owner, flags);

    let (clientid, seqid, result_flags) = handler.clients.exchange_id(client_owner)?;

    // Build response
    let mut result = Vec::new();
    clientid.serialize(&mut result)?;
    seqid.serialize(&mut result)?;
    result_flags.serialize(&mut result)?;
    0u32.serialize(&mut result)?; // state_protect

    // Server owner
    let server_owner_major: Vec<u8> = b"curvine".to_vec();
    let server_owner_minor: Vec<u8> = Vec::new();
    server_owner_major.serialize(&mut result)?;
    server_owner_minor.serialize(&mut result)?;

    // Server scope
    let server_scope: Vec<u8> = b"curvine.local".to_vec();
    server_scope.serialize(&mut result)?;

    // Server impl_id (empty)
    0u32.serialize(&mut result)?;

    Ok(result)
}

/// CREATE_SESSION
async fn op_create_session(
    input: &mut impl Read,
    _ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let clientid = input.read_u64::<BigEndian>()?;
    let seqid = input.read_u32::<BigEndian>()?;
    let flags = input.read_u32::<BigEndian>()?;

    // Skip channel attrs (fore and back)
    for _ in 0..2 {
        let _headerpadsize = input.read_u32::<BigEndian>()?;
        let _maxrequestsize = input.read_u32::<BigEndian>()?;
        let _maxresponsesize = input.read_u32::<BigEndian>()?;
        let _maxresponsesize_cached = input.read_u32::<BigEndian>()?;
        let _maxoperations = input.read_u32::<BigEndian>()?;
        let _maxrequests = input.read_u32::<BigEndian>()?;
        let rdma_count = input.read_u32::<BigEndian>()?;
        for _ in 0..rdma_count {
            let _rdma = input.read_u32::<BigEndian>()?;
        }
    }

    // Skip callback program
    let _cb_program = input.read_u32::<BigEndian>()?;
    // Skip sec_parms
    let sec_count = input.read_u32::<BigEndian>()?;
    for _ in 0..sec_count {
        let flavor = input.read_u32::<BigEndian>()?;
        if flavor == 1 {
            // AUTH_SYS
            let _stamp = input.read_u32::<BigEndian>()?;
            let mut machine: Vec<u8> = Vec::new();
            machine.deserialize(input)?;
            let _uid = input.read_u32::<BigEndian>()?;
            let _gid = input.read_u32::<BigEndian>()?;
            let gid_count = input.read_u32::<BigEndian>()?;
            for _ in 0..gid_count {
                let _gid = input.read_u32::<BigEndian>()?;
            }
        }
    }

    debug!(
        "CREATE_SESSION: client={} seq={} flags={:#x}",
        clientid, seqid, flags
    );

    // Verify client exists
    let client = handler
        .clients
        .get_client(clientid)
        .ok_or(Nfs4Status::StaleClientid)?;

    // Get current expected sequence ID
    let expected_seqid = client.get_create_session_sequence();

    debug!(
        "CREATE_SESSION: csa_sequence={} expected_sequence={}",
        seqid, expected_seqid
    );

    // Check for replay (seqid + 1 == expected means this is a replay of the last request)
    if seqid.wrapping_add(1) == expected_seqid {
        // This is a replay - return cached response
        let cached = client.get_cached_create_session_response();
        if !cached.response.is_empty() {
            info!(
                "CREATE_SESSION: replay detected for client={}, returning cached response",
                clientid
            );
            return Ok(cached.response);
        }
        // No cached response, treat as SEQ_MISORDERED
        warn!(
            "CREATE_SESSION: replay detected but no cached response for client={}",
            clientid
        );
        return Err(Nfs4Status::SeqMisordered.into());
    }

    // Validate sequence ID matches expected
    if seqid != expected_seqid {
        warn!(
            "CREATE_SESSION: sequence mismatch for client={}, got={} expected={}",
            clientid, seqid, expected_seqid
        );
        return Err(Nfs4Status::SeqMisordered.into());
    }

    // Create session
    let session = handler.sessions.create_session(clientid)?;

    // Confirm client
    handler.clients.confirm_client(clientid)?;

    // Build response
    let mut result = Vec::new();
    session.sessionid.serialize(&mut result)?;
    seqid.serialize(&mut result)?;
    flags.serialize(&mut result)?;

    // Fore channel attrs
    0u32.serialize(&mut result)?; // headerpadsize
    (1024 * 1024u32).serialize(&mut result)?; // maxrequestsize
    (1024 * 1024u32).serialize(&mut result)?; // maxresponsesize
    (64 * 1024u32).serialize(&mut result)?; // maxresponsesize_cached
    64u32.serialize(&mut result)?; // maxoperations
    session.slot_count().serialize(&mut result)?; // maxrequests
    0u32.serialize(&mut result)?; // rdma_ird (empty)

    // Back channel attrs (same as fore)
    0u32.serialize(&mut result)?;
    (1024 * 1024u32).serialize(&mut result)?;
    (1024 * 1024u32).serialize(&mut result)?;
    (64 * 1024u32).serialize(&mut result)?;
    64u32.serialize(&mut result)?;
    session.slot_count().serialize(&mut result)?;
    0u32.serialize(&mut result)?;

    // Cache response for replay detection
    client.cache_create_session_response(result.clone(), 0);

    // Increment sequence ID for next CREATE_SESSION
    client.increment_create_session_sequence();

    info!(
        "CREATE_SESSION: success for client={}, new sequence={}",
        clientid,
        client.get_create_session_sequence()
    );

    Ok(result)
}

/// DESTROY_SESSION
async fn op_destroy_session(
    input: &mut impl Read,
    _ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut sessionid = Sessionid4::default();
    sessionid.deserialize(input)?;

    debug!("DESTROY_SESSION: {:?}", &sessionid[..4]);

    handler.sessions.destroy_session(&sessionid)?;

    Ok(Vec::new())
}

/// PUTROOTFH - set current FH to root
fn op_putrootfh(ctx: &mut CompoundContext, handler: &CompoundHandler) -> Nfs4Result<Vec<u8>> {
    let fh = handler.fs.fileid_to_fh(handler.fs.root_fileid());
    info!(
        "PUTROOTFH: root_fileid={} fh_len={} fh_data={:02x?}",
        handler.fs.root_fileid(),
        fh.data.len(),
        &fh.data[..fh.data.len().min(16)]
    );
    ctx.current_fh = Some(fh);
    Ok(Vec::new())
}

/// PUTFH - set current FH
fn op_putfh(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut fh = Nfs4FileHandle::default();
    fh.deserialize(input)?;

    // Validate file handle
    handler.fs.fh_to_fileid(&fh)?;

    ctx.current_fh = Some(fh);
    Ok(Vec::new())
}

/// GETFH - get current FH
fn op_getfh(ctx: &CompoundContext, _handler: &CompoundHandler) -> Nfs4Result<Vec<u8>> {
    let fh = ctx.require_current_fh()?;

    let mut result = Vec::new();
    fh.serialize(&mut result)?;
    info!(
        "GETFH: fh_data_len={} result_len={} fh_data={:02x?} result_hex={:02x?}",
        fh.data.len(),
        result.len(),
        &fh.data[..fh.data.len().min(16)],
        &result[..result.len().min(32)]
    );
    Ok(result)
}

/// SAVEFH - save current FH
fn op_savefh(ctx: &mut CompoundContext) -> Nfs4Result<Vec<u8>> {
    ctx.saved_fh = ctx.current_fh.clone();
    Ok(Vec::new())
}

/// RESTOREFH - restore saved FH
fn op_restorefh(ctx: &mut CompoundContext) -> Nfs4Result<Vec<u8>> {
    let saved = ctx.saved_fh.take().ok_or(Nfs4Status::Restorefh)?;
    ctx.current_fh = Some(saved);
    Ok(Vec::new())
}

/// GETATTR - get file attributes
///
/// # Architecture
/// Delegates to ops::getattr::op_getattr() which mirrors NFS-Ganesha's nfs4_op_getattr.c
async fn op_getattr(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops module (mirrors NFS-Ganesha structure)
    crate::nfs4::ops::getattr::op_getattr(input, ctx, handler).await
}

/// LOOKUP - lookup name in directory
///
/// # Architecture
/// Delegates to ops::lookup::op_lookup() which mirrors NFS-Ganesha's nfs4_op_lookup.c
async fn op_lookup(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops module (mirrors NFS-Ganesha structure)
    crate::nfs4::ops::lookup::op_lookup(input, ctx, handler).await
}

/// LOOKUPP - lookup parent directory
async fn op_lookupp(ctx: &mut CompoundContext, handler: &CompoundHandler) -> Nfs4Result<Vec<u8>> {
    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    let (parent_id, _status) = handler.fs.lookup(fileid, "..").await?;

    ctx.current_fh = Some(handler.fs.fileid_to_fh(parent_id));

    Ok(Vec::new())
}

/// READ - read file data
///
/// # Architecture
/// Delegates to ops::read::op_read() which mirrors NFS-Ganesha's nfs4_op_read.c
async fn op_read(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops module (mirrors NFS-Ganesha structure)
    crate::nfs4::ops::read::op_read(input, ctx, handler).await
}

/// WRITE - write file data
///
/// # Architecture
/// Delegates to ops::write::op_write() which mirrors NFS-Ganesha's nfs4_op_write.c
async fn op_write(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops module (mirrors NFS-Ganesha structure)
    crate::nfs4::ops::write::op_write(input, ctx, handler).await
}

/// READDIR - read directory entries
///
/// # Architecture
/// Delegates to ops::readdir::op_readdir() which mirrors NFS-Ganesha's nfs4_op_readdir.c
async fn op_readdir(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops module (mirrors NFS-Ganesha structure)
    crate::nfs4::ops::readdir::op_readdir(input, ctx, handler).await
}

/// RECLAIM_COMPLETE - client finished reclaiming state
fn op_reclaim_complete(input: &mut impl Read, _ctx: &mut CompoundContext) -> Nfs4Result<Vec<u8>> {
    let _one_fs = input.read_u32::<BigEndian>()?;
    debug!("RECLAIM_COMPLETE");
    Ok(Vec::new())
}

// ============================================================================
// Helper Functions
// ============================================================================

// NFSv4 attribute bit definitions (RFC 7530)
// Word 0 (bits 0-31)
const FATTR4_SUPPORTED_ATTRS: u32 = 0;
const FATTR4_TYPE: u32 = 1;
const FATTR4_FH_EXPIRE_TYPE: u32 = 2;
const FATTR4_CHANGE: u32 = 3;
const FATTR4_SIZE: u32 = 4;
const FATTR4_LINK_SUPPORT: u32 = 5;
const FATTR4_SYMLINK_SUPPORT: u32 = 6;
const FATTR4_NAMED_ATTR: u32 = 7;
const FATTR4_FSID: u32 = 8;
const FATTR4_UNIQUE_HANDLES: u32 = 9;
const FATTR4_LEASE_TIME: u32 = 10;
#[allow(dead_code)]
const FATTR4_RDATTR_ERROR: u32 = 11;
#[allow(dead_code)]
const FATTR4_ACL: u32 = 12;
const FATTR4_ACLSUPPORT: u32 = 13;
#[allow(dead_code)]
const FATTR4_ARCHIVE: u32 = 14;
const FATTR4_CANSETTIME: u32 = 15;
const FATTR4_CASE_INSENSITIVE: u32 = 16;
const FATTR4_CASE_PRESERVING: u32 = 17;
const FATTR4_CHOWN_RESTRICTED: u32 = 18;
const FATTR4_FILEHANDLE: u32 = 19;
const FATTR4_FILEID: u32 = 20;
const FATTR4_FILES_AVAIL: u32 = 21;
const FATTR4_FILES_FREE: u32 = 22;
const FATTR4_FILES_TOTAL: u32 = 23;
#[allow(dead_code)]
const FATTR4_FS_LOCATIONS: u32 = 24;
#[allow(dead_code)]
const FATTR4_HIDDEN: u32 = 25;
const FATTR4_HOMOGENEOUS: u32 = 26;
const FATTR4_MAXFILESIZE: u32 = 27;
const FATTR4_MAXLINK: u32 = 28;
const FATTR4_MAXNAME: u32 = 29;
const FATTR4_MAXREAD: u32 = 30;
const FATTR4_MAXWRITE: u32 = 31;
// Word 1 (bits 32-63)
#[allow(dead_code)]
const FATTR4_MIMETYPE: u32 = 32;
const FATTR4_MODE: u32 = 33;
const FATTR4_NO_TRUNC: u32 = 34;
const FATTR4_NUMLINKS: u32 = 35;
const FATTR4_OWNER: u32 = 36;
const FATTR4_OWNER_GROUP: u32 = 37;
#[allow(dead_code)]
const FATTR4_QUOTA_AVAIL_HARD: u32 = 38;
#[allow(dead_code)]
const FATTR4_QUOTA_AVAIL_SOFT: u32 = 39;
#[allow(dead_code)]
const FATTR4_QUOTA_USED: u32 = 40;
const FATTR4_RAWDEV: u32 = 41;
const FATTR4_SPACE_AVAIL: u32 = 42;
const FATTR4_SPACE_FREE: u32 = 43;
const FATTR4_SPACE_TOTAL: u32 = 44;
const FATTR4_SPACE_USED: u32 = 45;
#[allow(dead_code)]
const FATTR4_SYSTEM: u32 = 46;
const FATTR4_TIME_ACCESS: u32 = 47;
#[allow(dead_code)]
const FATTR4_TIME_ACCESS_SET: u32 = 48;
#[allow(dead_code)]
const FATTR4_TIME_BACKUP: u32 = 49;
#[allow(dead_code)]
const FATTR4_TIME_CREATE: u32 = 50;
const FATTR4_TIME_DELTA: u32 = 51;
const FATTR4_TIME_METADATA: u32 = 52;
const FATTR4_TIME_MODIFY: u32 = 53;
#[allow(dead_code)]
const FATTR4_TIME_MODIFY_SET: u32 = 54;
const FATTR4_MOUNTED_ON_FILEID: u32 = 55;

/// Encode file attributes to fattr4 based on requested bitmap
pub fn encode_fattr4(
    attrs: &FileAttrs,
    request: &[u32],
    fh: Option<&Nfs4FileHandle>,
) -> Nfs4Result<Fattr4> {
    let mut attr_vals = Vec::new();
    let mut result_mask = vec![0u32; request.len().max(2)];

    // Helper to check if attribute is requested
    let is_requested = |bit: u32| -> bool {
        let word = (bit / 32) as usize;
        let bit_in_word = bit % 32;
        word < request.len() && (request[word] & (1 << bit_in_word)) != 0
    };

    // Helper to set result bit
    let set_result = |mask: &mut Vec<u32>, bit: u32| {
        let word = (bit / 32) as usize;
        let bit_in_word = bit % 32;
        if word < mask.len() {
            mask[word] |= 1 << bit_in_word;
        }
    };

    // Encode Word 0 attributes in bit order (0-31)
    encode_word0_attrs(
        attrs,
        fh,
        &is_requested,
        &set_result,
        &mut attr_vals,
        &mut result_mask,
    )?;

    // Encode Word 1 attributes in bit order (32-63)
    encode_word1_attrs(
        attrs,
        &is_requested,
        &set_result,
        &mut attr_vals,
        &mut result_mask,
    )?;

    // Trim trailing zeros from result_mask
    while result_mask.len() > 1 && result_mask.last() == Some(&0) {
        result_mask.pop();
    }

    debug!(
        "encode_fattr4: request={:08x?} result={:08x?} vals_len={}",
        request,
        result_mask,
        attr_vals.len()
    );

    Ok(Fattr4 {
        attrmask: result_mask,
        attr_vals,
    })
}

/// Encode Word 0 attributes (bits 0-31) in strict bit order
fn encode_word0_attrs<F, S>(
    attrs: &FileAttrs,
    fh: Option<&Nfs4FileHandle>,
    is_requested: &F,
    set_result: &S,
    vals: &mut Vec<u8>,
    mask: &mut Vec<u32>,
) -> Nfs4Result<()>
where
    F: Fn(u32) -> bool,
    S: Fn(&mut Vec<u32>, u32),
{
    // bit 0: SUPPORTED_ATTRS
    if is_requested(FATTR4_SUPPORTED_ATTRS) {
        encode_supported_attrs(vals)?;
        set_result(mask, FATTR4_SUPPORTED_ATTRS);
    }
    // bit 1: TYPE
    if is_requested(FATTR4_TYPE) {
        (attrs.file_type as u32).serialize(vals)?;
        set_result(mask, FATTR4_TYPE);
    }
    // bit 2: FH_EXPIRE_TYPE
    if is_requested(FATTR4_FH_EXPIRE_TYPE) {
        0u32.serialize(vals)?; // FH4_PERSISTENT
        set_result(mask, FATTR4_FH_EXPIRE_TYPE);
    }
    // bit 3: CHANGE
    if is_requested(FATTR4_CHANGE) {
        let change_val = attrs.mtime.to_millis() as u64;
        change_val.serialize(vals)?;
        set_result(mask, FATTR4_CHANGE);
    }
    // bit 4: SIZE
    if is_requested(FATTR4_SIZE) {
        attrs.size.serialize(vals)?;
        set_result(mask, FATTR4_SIZE);
    }
    // bit 5: LINK_SUPPORT
    if is_requested(FATTR4_LINK_SUPPORT) {
        true.serialize(vals)?;
        set_result(mask, FATTR4_LINK_SUPPORT);
    }
    // bit 6: SYMLINK_SUPPORT
    if is_requested(FATTR4_SYMLINK_SUPPORT) {
        true.serialize(vals)?;
        set_result(mask, FATTR4_SYMLINK_SUPPORT);
    }
    // bit 7: NAMED_ATTR
    if is_requested(FATTR4_NAMED_ATTR) {
        false.serialize(vals)?;
        set_result(mask, FATTR4_NAMED_ATTR);
    }
    // bit 8: FSID
    if is_requested(FATTR4_FSID) {
        encode_fsid(vals)?;
        set_result(mask, FATTR4_FSID);
    }
    // bit 9: UNIQUE_HANDLES
    if is_requested(FATTR4_UNIQUE_HANDLES) {
        true.serialize(vals)?;
        set_result(mask, FATTR4_UNIQUE_HANDLES);
    }
    // bit 10: LEASE_TIME
    if is_requested(FATTR4_LEASE_TIME) {
        90u32.serialize(vals)?;
        set_result(mask, FATTR4_LEASE_TIME);
    }
    // bit 13: ACLSUPPORT
    if is_requested(FATTR4_ACLSUPPORT) {
        // ACL4_SUPPORT_ALLOW_ACL (0x00000001) | ACL4_SUPPORT_DENY_ACL (0x00000002)
        // Indicate basic ACL support to satisfy macOS NFS client requirements
        3u32.serialize(vals)?;
        set_result(mask, FATTR4_ACLSUPPORT);
    }
    // bit 15: CANSETTIME
    if is_requested(FATTR4_CANSETTIME) {
        true.serialize(vals)?;
        set_result(mask, FATTR4_CANSETTIME);
    }
    // bit 16: CASE_INSENSITIVE
    if is_requested(FATTR4_CASE_INSENSITIVE) {
        false.serialize(vals)?;
        set_result(mask, FATTR4_CASE_INSENSITIVE);
    }
    // bit 17: CASE_PRESERVING
    if is_requested(FATTR4_CASE_PRESERVING) {
        true.serialize(vals)?;
        set_result(mask, FATTR4_CASE_PRESERVING);
    }
    // bit 18: CHOWN_RESTRICTED
    if is_requested(FATTR4_CHOWN_RESTRICTED) {
        true.serialize(vals)?;
        set_result(mask, FATTR4_CHOWN_RESTRICTED);
    }
    // bit 19: FILEHANDLE
    if is_requested(FATTR4_FILEHANDLE) {
        if let Some(handle) = fh {
            handle.serialize(vals)?;
            set_result(mask, FATTR4_FILEHANDLE);
        }
    }
    // bit 20: FILEID
    if is_requested(FATTR4_FILEID) {
        attrs.fileid.serialize(vals)?;
        set_result(mask, FATTR4_FILEID);
    }
    // bit 21: FILES_AVAIL
    if is_requested(FATTR4_FILES_AVAIL) {
        (u64::MAX / 2).serialize(vals)?;
        set_result(mask, FATTR4_FILES_AVAIL);
    }
    // bit 22: FILES_FREE
    if is_requested(FATTR4_FILES_FREE) {
        (u64::MAX / 2).serialize(vals)?;
        set_result(mask, FATTR4_FILES_FREE);
    }
    // bit 23: FILES_TOTAL
    if is_requested(FATTR4_FILES_TOTAL) {
        u64::MAX.serialize(vals)?;
        set_result(mask, FATTR4_FILES_TOTAL);
    }
    // bit 26: HOMOGENEOUS
    if is_requested(FATTR4_HOMOGENEOUS) {
        true.serialize(vals)?;
        set_result(mask, FATTR4_HOMOGENEOUS);
    }
    // bit 27: MAXFILESIZE
    if is_requested(FATTR4_MAXFILESIZE) {
        (i64::MAX as u64).serialize(vals)?;
        set_result(mask, FATTR4_MAXFILESIZE);
    }
    // bit 28: MAXLINK
    if is_requested(FATTR4_MAXLINK) {
        u32::MAX.serialize(vals)?;
        set_result(mask, FATTR4_MAXLINK);
    }
    // bit 29: MAXNAME
    if is_requested(FATTR4_MAXNAME) {
        255u32.serialize(vals)?;
        set_result(mask, FATTR4_MAXNAME);
    }
    // bit 30: MAXREAD
    if is_requested(FATTR4_MAXREAD) {
        (1024 * 1024u64).serialize(vals)?;
        set_result(mask, FATTR4_MAXREAD);
    }
    // bit 31: MAXWRITE
    if is_requested(FATTR4_MAXWRITE) {
        (1024 * 1024u64).serialize(vals)?;
        set_result(mask, FATTR4_MAXWRITE);
    }
    Ok(())
}

/// Encode Word 1 attributes (bits 32-63) in strict bit order
fn encode_word1_attrs<F, S>(
    attrs: &FileAttrs,
    is_requested: &F,
    set_result: &S,
    vals: &mut Vec<u8>,
    mask: &mut Vec<u32>,
) -> Nfs4Result<()>
where
    F: Fn(u32) -> bool,
    S: Fn(&mut Vec<u32>, u32),
{
    // bit 33: MODE
    if is_requested(FATTR4_MODE) {
        attrs.mode.serialize(vals)?;
        set_result(mask, FATTR4_MODE);
    }
    // bit 34: NO_TRUNC
    if is_requested(FATTR4_NO_TRUNC) {
        true.serialize(vals)?;
        set_result(mask, FATTR4_NO_TRUNC);
    }
    // bit 35: NUMLINKS
    if is_requested(FATTR4_NUMLINKS) {
        attrs.nlink.serialize(vals)?;
        set_result(mask, FATTR4_NUMLINKS);
    }
    // bit 36: OWNER
    if is_requested(FATTR4_OWNER) {
        let owner = if attrs.owner.is_empty() {
            "nobody"
        } else {
            &attrs.owner
        };
        owner.as_bytes().to_vec().serialize(vals)?;
        set_result(mask, FATTR4_OWNER);
    }
    // bit 37: OWNER_GROUP
    if is_requested(FATTR4_OWNER_GROUP) {
        let group = if attrs.group.is_empty() {
            "nobody"
        } else {
            &attrs.group
        };
        group.as_bytes().to_vec().serialize(vals)?;
        set_result(mask, FATTR4_OWNER_GROUP);
    }
    // bit 41: RAWDEV
    if is_requested(FATTR4_RAWDEV) {
        0u32.serialize(vals)?; // specdata1
        0u32.serialize(vals)?; // specdata2
        set_result(mask, FATTR4_RAWDEV);
    }
    // bit 42: SPACE_AVAIL
    if is_requested(FATTR4_SPACE_AVAIL) {
        (1024u64 * 1024 * 1024 * 1024).serialize(vals)?; // 1TB
        set_result(mask, FATTR4_SPACE_AVAIL);
    }
    // bit 43: SPACE_FREE
    if is_requested(FATTR4_SPACE_FREE) {
        (1024u64 * 1024 * 1024 * 1024).serialize(vals)?; // 1TB
        set_result(mask, FATTR4_SPACE_FREE);
    }
    // bit 44: SPACE_TOTAL
    if is_requested(FATTR4_SPACE_TOTAL) {
        (10u64 * 1024 * 1024 * 1024 * 1024).serialize(vals)?; // 10TB
        set_result(mask, FATTR4_SPACE_TOTAL);
    }
    // bit 45: SPACE_USED
    if is_requested(FATTR4_SPACE_USED) {
        attrs.used.serialize(vals)?;
        set_result(mask, FATTR4_SPACE_USED);
    }
    // bit 47: TIME_ACCESS
    if is_requested(FATTR4_TIME_ACCESS) {
        attrs.atime.serialize(vals)?;
        set_result(mask, FATTR4_TIME_ACCESS);
    }
    // bit 51: TIME_DELTA
    if is_requested(FATTR4_TIME_DELTA) {
        // nfstime4: 1 second resolution
        0i64.serialize(vals)?; // seconds
        1u32.serialize(vals)?; // nseconds (1 nanosecond resolution)
        set_result(mask, FATTR4_TIME_DELTA);
    }
    // bit 52: TIME_METADATA
    if is_requested(FATTR4_TIME_METADATA) {
        attrs.ctime.serialize(vals)?;
        set_result(mask, FATTR4_TIME_METADATA);
    }
    // bit 53: TIME_MODIFY
    if is_requested(FATTR4_TIME_MODIFY) {
        attrs.mtime.serialize(vals)?;
        set_result(mask, FATTR4_TIME_MODIFY);
    }
    // bit 55: MOUNTED_ON_FILEID
    if is_requested(FATTR4_MOUNTED_ON_FILEID) {
        attrs.fileid.serialize(vals)?;
        set_result(mask, FATTR4_MOUNTED_ON_FILEID);
    }
    Ok(())
}

/// Encode supported attributes bitmap
fn encode_supported_attrs(output: &mut Vec<u8>) -> Nfs4Result<()> {
    // Return bitmap of all supported attributes
    let supported: Vec<u32> = vec![
        // Word 0: bits 0-31
        (1 << FATTR4_SUPPORTED_ATTRS)
            | (1 << FATTR4_TYPE)
            | (1 << FATTR4_FH_EXPIRE_TYPE)
            | (1 << FATTR4_CHANGE)
            | (1 << FATTR4_SIZE)
            | (1 << FATTR4_LINK_SUPPORT)
            | (1 << FATTR4_SYMLINK_SUPPORT)
            | (1 << FATTR4_NAMED_ATTR)
            | (1 << FATTR4_FSID)
            | (1 << FATTR4_UNIQUE_HANDLES)
            | (1 << FATTR4_LEASE_TIME)
            | (1 << FATTR4_ACLSUPPORT)
            | (1 << FATTR4_CANSETTIME)
            | (1 << FATTR4_CASE_INSENSITIVE)
            | (1 << FATTR4_CASE_PRESERVING)
            | (1 << FATTR4_CHOWN_RESTRICTED)
            | (1 << FATTR4_FILEHANDLE)
            | (1 << FATTR4_FILEID)
            | (1 << FATTR4_FILES_AVAIL)
            | (1 << FATTR4_FILES_FREE)
            | (1 << FATTR4_FILES_TOTAL)
            | (1 << FATTR4_HOMOGENEOUS)
            | (1 << FATTR4_MAXFILESIZE)
            | (1 << FATTR4_MAXLINK)
            | (1 << FATTR4_MAXNAME)
            | (1 << FATTR4_MAXREAD)
            | (1 << FATTR4_MAXWRITE),
        // Word 1: bits 32-63
        (1 << (FATTR4_MODE - 32))
            | (1 << (FATTR4_NO_TRUNC - 32))
            | (1 << (FATTR4_NUMLINKS - 32))
            | (1 << (FATTR4_OWNER - 32))
            | (1 << (FATTR4_OWNER_GROUP - 32))
            | (1 << (FATTR4_RAWDEV - 32))
            | (1 << (FATTR4_SPACE_AVAIL - 32))
            | (1 << (FATTR4_SPACE_FREE - 32))
            | (1 << (FATTR4_SPACE_TOTAL - 32))
            | (1 << (FATTR4_SPACE_USED - 32))
            | (1 << (FATTR4_TIME_ACCESS - 32))
            | (1 << (FATTR4_TIME_DELTA - 32))
            | (1 << (FATTR4_TIME_METADATA - 32))
            | (1 << (FATTR4_TIME_MODIFY - 32))
            | (1 << (FATTR4_MOUNTED_ON_FILEID - 32)),
    ];
    supported.serialize(output)?;
    Ok(())
}

/// Encode fsid (filesystem identifier)
fn encode_fsid(output: &mut Vec<u8>) -> Nfs4Result<()> {
    // fsid4: major (u64), minor (u64)
    1u64.serialize(output)?; // major
    0u64.serialize(output)?; // minor
    Ok(())
}

// ============================================================================
// OPEN / CLOSE Operations (Core NFSv4.1 stateful operations)
// ============================================================================

/// OPEN - open a file (creates Reader/Writer)
///
/// This is the key difference from NFSv3:
/// - NFSv3: READ/WRITE directly, io_cache manages Reader/Writer
/// - NFSv4.1: OPEN first, Reader/Writer bound to OpenState
/// OPEN - open a file
///
/// # Architecture
/// Delegates to ops::open::op_open() which mirrors NFS-Ganesha's nfs4_op_open.c
async fn op_open(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops module (mirrors NFS-Ganesha structure)
    crate::nfs4::ops::open::op_open(input, ctx, handler).await
}

/// CLOSE - close a file
///
/// # Architecture
/// Delegates to ops::close::op_close() which mirrors NFS-Ganesha's nfs4_op_close.c
async fn op_close(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops module (mirrors NFS-Ganesha structure)
    crate::nfs4::ops::close::op_close(input, ctx, handler).await
}

/// COMMIT - commit written data to stable storage
/// COMMIT - commit written data to stable storage
async fn op_commit(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops::commit module (NFS-Ganesha aligned)
    crate::nfs4::ops::commit::op_commit(input, ctx, handler).await
}

/// CREATE - create a non-regular file (directory, symlink, etc.)
///
/// # Architecture
/// Delegates to ops::create::op_create() which mirrors NFS-Ganesha's nfs4_op_create.c
async fn op_create(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops module (mirrors NFS-Ganesha structure)
    crate::nfs4::ops::create::op_create(input, ctx, handler).await
}

/// REMOVE - remove a file or directory
///
/// # Architecture
/// Delegates to ops::remove::op_remove() which mirrors NFS-Ganesha's nfs4_op_remove.c
async fn op_remove(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops module (mirrors NFS-Ganesha structure)
    crate::nfs4::ops::remove::op_remove(input, ctx, handler).await
}

/// RENAME - rename a file or directory
///
/// # Architecture
/// Delegates to ops::rename::op_rename() which mirrors NFS-Ganesha's nfs4_op_rename.c
async fn op_rename(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops module (mirrors NFS-Ganesha structure)
    crate::nfs4::ops::rename::op_rename(input, ctx, handler).await
}

/// LINK - create a hard link
async fn op_link(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Read newname (the name for the hard link in current directory)
    let mut newname: Vec<u8> = Vec::new();
    newname.deserialize(input)?;
    let newname_str = String::from_utf8_lossy(&newname).to_string();

    debug!("LINK: newname={}", newname_str);

    // Source file is saved FH, target directory is current FH
    let src_fh = ctx.saved_fh.as_ref().ok_or(Nfs4Status::Nofilehandle)?;
    let src_fileid = handler.fs.fh_to_fileid(src_fh)?;

    let dst_fh = ctx.require_current_fh()?;
    let dst_parent = handler.fs.fh_to_fileid(dst_fh)?;

    // Create hard link
    handler
        .fs
        .link(src_fileid, dst_parent, &newname_str)
        .await?;

    info!(
        "LINK: created hard link '{}' in dir {} pointing to file {}",
        newname_str, dst_parent, src_fileid
    );

    // Build response - change_info4
    let mut result = Vec::new();
    true.serialize(&mut result)?; // atomic
    0u64.serialize(&mut result)?; // before
    1u64.serialize(&mut result)?; // after

    Ok(result)
}

/// READLINK - read symbolic link target
async fn op_readlink(ctx: &CompoundContext, handler: &CompoundHandler) -> Nfs4Result<Vec<u8>> {
    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    let target = handler.fs.readlink(fileid).await?;

    debug!("READLINK: fileid={} -> {}", fileid, target);

    // Build response
    let mut result = Vec::new();
    target.as_bytes().to_vec().serialize(&mut result)?;

    Ok(result)
}

// ============================================================================
// Delegation Operations
// ============================================================================

/// DELEGRETURN - return a delegation
async fn op_delegreturn(
    input: &mut impl Read,
    _ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let mut stateid = Stateid4::default();
    stateid.deserialize(input)?;

    debug!("DELEGRETURN: stateid={:?}", &stateid.other[..4]);

    handler.delegations.return_delegation(&stateid)?;

    Ok(Vec::new())
}

// ============================================================================
// NFSv4.0 Operations
// ============================================================================

/// SETCLIENTID - NFSv4.0 client registration (equivalent to EXCHANGE_ID in v4.1)
async fn op_setclientid(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Read client verifier (8 bytes)
    let mut verifier: [u8; 8] = [0; 8];
    input.read_exact(&mut verifier)?;

    // Read client id (nfs_client_id4)
    let mut client_id: Vec<u8> = Vec::new();
    client_id.deserialize(input)?;

    // Read callback info (cb_client4)
    let cb_program = input.read_u32::<BigEndian>()?;
    // Read netid and addr
    let mut netid: Vec<u8> = Vec::new();
    netid.deserialize(input)?;
    let mut addr: Vec<u8> = Vec::new();
    addr.deserialize(input)?;

    // Read callback_ident
    let callback_ident = input.read_u32::<BigEndian>()?;

    info!(
        "NFSv4.0 SETCLIENTID: verifier={:02x?} client_id={:?} cb_program={} netid={:?} addr={:?} cb_ident={}",
        &verifier[..4],
        String::from_utf8_lossy(&client_id),
        cb_program,
        String::from_utf8_lossy(&netid),
        String::from_utf8_lossy(&addr),
        callback_ident
    );

    // Handle NFSv4.0 SETCLIENTID logic (based on RFC 3530 and NFS-Ganesha)
    let (clientid, confirm_verifier) = handle_setclientid_v40(
        &client_id,
        &verifier,
        cb_program,
        &netid,
        &addr,
        callback_ident,
        handler,
    )?;

    // Store clientid in context for subsequent operations
    ctx.clientid = Some(clientid);

    info!(
        "NFSv4.0 SETCLIENTID: assigned clientid={} confirm_verifier={:02x?}",
        clientid,
        &confirm_verifier[..4]
    );

    // Build response: clientid + setclientid_confirm verifier
    let mut result = Vec::new();
    clientid.serialize(&mut result)?;
    result.extend_from_slice(&confirm_verifier);

    info!("NFSv4.0 SETCLIENTID: response len={} bytes", result.len());

    Ok(result)
}

/// Handle NFSv4.0 SETCLIENTID logic (simplified version of NFS-Ganesha logic)
fn handle_setclientid_v40(
    client_id: &[u8],
    verifier: &[u8; 8],
    _cb_program: u32,
    _netid: &[u8],
    _addr: &[u8],
    _callback_ident: u32,
    handler: &CompoundHandler,
) -> Nfs4Result<(Clientid4, [u8; 8])> {
    // Create client owner for lookup
    let client_owner = ClientOwner4 {
        co_verifier: *verifier,
        co_ownerid: client_id.to_vec(),
    };

    // Check if we already have a client with this owner ID
    if let Some(existing_clientid) = handler
        .clients
        .find_client_by_owner(&client_owner.co_ownerid)
    {
        let existing_client = handler
            .clients
            .get_client(existing_clientid)
            .ok_or(Nfs4Status::Serverfault)?;

        // Check if verifier matches (CASE 2: update callback info)
        if existing_client.owner.co_verifier == *verifier && existing_client.is_confirmed() {
            info!(
                "NFSv4.0 SETCLIENTID: CASE 2 - Update callback for existing confirmed client {}",
                existing_clientid
            );
            // Generate new confirm verifier but keep same clientid
            let mut confirm_verifier = [0u8; 8];
            use std::time::{SystemTime, UNIX_EPOCH};
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            confirm_verifier.copy_from_slice(&timestamp.to_be_bytes());

            return Ok((existing_clientid, confirm_verifier));
        } else {
            info!("NFSv4.0 SETCLIENTID: CASE 3/4 - Different verifier, creating new client");
            // Different verifier, create new client (CASE 3/4)
        }
    }

    // CASE 5: New client or replacing existing unconfirmed client
    info!("NFSv4.0 SETCLIENTID: CASE 5 - New client registration");

    // Register new client (reuse v4.1 logic but mark as unconfirmed)
    let (clientid, _seqid, _flags) = handler.clients.exchange_id(client_owner)?;

    // Generate confirm verifier
    let mut confirm_verifier = [0u8; 8];
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    confirm_verifier.copy_from_slice(&timestamp.to_be_bytes());

    info!(
        "NFSv4.0 SETCLIENTID: Created new unconfirmed client {} with verifier {:02x?}",
        clientid,
        &confirm_verifier[..4]
    );

    Ok((clientid, confirm_verifier))
}

/// SETCLIENTID_CONFIRM - NFSv4.0 confirm client (equivalent to CREATE_SESSION in v4.1)
async fn op_setclientid_confirm(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let clientid = input.read_u64::<BigEndian>()?;
    let mut verifier: [u8; 8] = [0; 8];
    input.read_exact(&mut verifier)?;

    info!(
        "NFSv4.0 SETCLIENTID_CONFIRM: clientid={} verifier={:02x?}",
        clientid,
        &verifier[..4]
    );

    // Verify client exists
    let client = handler.clients.get_client(clientid).ok_or_else(|| {
        error!("SETCLIENTID_CONFIRM: client {} not found", clientid);
        Nfs4Status::StaleClientid
    })?;

    info!(
        "NFSv4.0 SETCLIENTID_CONFIRM: found client {}, confirmed={}",
        clientid,
        client.is_confirmed()
    );

    // In a full implementation, we would verify the confirm verifier matches
    // what we sent in SETCLIENTID response. For now, we'll accept any verifier.

    // Confirm client (equivalent to CREATE_SESSION in v4.1)
    handler.clients.confirm_client(clientid)?;

    // Store clientid in context for subsequent operations
    ctx.clientid = Some(clientid);

    info!(
        "NFSv4.0 SETCLIENTID_CONFIRM: client {} confirmed and stored in context",
        clientid
    );

    // No response data for SETCLIENTID_CONFIRM
    Ok(Vec::new())
}

/// RENEW - NFSv4.0 lease renewal
async fn op_renew(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    let clientid = input.read_u64::<BigEndian>()?;

    debug!("RENEW: clientid={}", clientid);

    // Renew lease
    handler.clients.renew_lease(clientid)?;

    // Store clientid in context
    ctx.clientid = Some(clientid);

    Ok(Vec::new())
}

/// OPEN_CONFIRM - NFSv4.0 open confirmation (not needed in NFSv4.1)
// ============================================================================
// Common Operations (NFSv4.0 and NFSv4.1)
// ============================================================================

// NFSv4 ACCESS permission bits (RFC 7530 Section 6.2.1)
// These constants are defined in ops/access.rs and used there
#[allow(dead_code)]
const ACCESS4_READ: u32 = 0x0001;
#[allow(dead_code)]
const ACCESS4_LOOKUP: u32 = 0x0002;
#[allow(dead_code)]
const ACCESS4_MODIFY: u32 = 0x0004;
#[allow(dead_code)]
const ACCESS4_EXTEND: u32 = 0x0008;
#[allow(dead_code)]
const ACCESS4_DELETE: u32 = 0x0010;
#[allow(dead_code)]
const ACCESS4_EXECUTE: u32 = 0x0020;

/// ACCESS - check access permissions
/// ACCESS - check access permissions
async fn op_access(
    input: &mut impl Read,
    ctx: &mut CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops::access module (NFS-Ganesha aligned)
    crate::nfs4::ops::access::op_access(input, ctx, handler).await
}

/// SETATTR - set file attributes
///
/// # Architecture
/// Delegates to ops::setattr::op_setattr() which mirrors NFS-Ganesha's nfs4_op_setattr.c
async fn op_setattr(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Delegate to ops module (mirrors NFS-Ganesha structure)
    crate::nfs4::ops::setattr::op_setattr(input, ctx, handler).await
}

/// SECINFO - get security information for a directory entry
async fn op_secinfo(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Read the name to query security info for
    let mut name: Vec<u8> = Vec::new();
    name.deserialize(input)?;

    let name_str = String::from_utf8_lossy(&name);

    info!(
        "SECINFO: name={:?} current_fh_present={}",
        name_str,
        ctx.current_fh.is_some()
    );

    // Verify we have a current filehandle (should be a directory)
    let fh = ctx.require_current_fh()?;
    let parent_id = handler.fs.fh_to_fileid(fh)?;

    info!(
        "SECINFO: parent_id={} looking up security for '{}'",
        parent_id, name_str
    );

    // Try to lookup the file to verify it exists (optional check)
    match handler.fs.lookup(parent_id, &name_str).await {
        Ok((fileid, _status)) => {
            info!("SECINFO: found file {} for security query", fileid);
        }
        Err(e) => {
            info!(
                "SECINFO: file '{}' not found: {:?}, continuing anyway",
                name_str, e
            );
            // Continue anyway - SECINFO can be called for non-existent files
        }
    }

    // For simplicity, we support only AUTH_SYS (RPC_AUTH_UNIX)
    // This is the most basic security mechanism that most clients expect
    let mut result = Vec::new();

    // Return array of secinfo4 structures
    // Array length = 1 (only AUTH_SYS)
    1u32.serialize(&mut result)?;

    // secinfo4 structure:
    // - flavor: RPC_AUTH_UNIX (1)
    // - flavor_info: union based on flavor
    1u32.serialize(&mut result)?; // RPC_AUTH_UNIX

    // For AUTH_SYS, flavor_info is empty (no additional data needed)
    // The union discriminant is the flavor itself, no additional data

    info!(
        "SECINFO: returning AUTH_SYS support for '{}', result_len={}",
        name_str,
        result.len()
    );

    Ok(result)
}

/// NVERIFY - verify attributes are NOT the same (cache validation)
async fn op_nverify(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Read fattr4 to compare
    let mut fattr = Fattr4::default();
    fattr.deserialize(input)?;

    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    debug!("NVERIFY: fileid={} checking attributes", fileid);

    // Get current file status
    let status = handler.fs.get_status_cached(fileid).await?;
    let current_attrs = FileAttrs::from_status(&status);

    // Compare attributes - if they match, return NFS4ERR_SAME
    // If they don't match, return NFS4_OK (success for NVERIFY)
    let attrs_match = compare_fattr4(&fattr, &current_attrs);

    if attrs_match {
        // Attributes are the same - NVERIFY fails
        return Err(Nfs4Status::Same.into());
    }

    // Attributes are different - NVERIFY succeeds
    Ok(Vec::new())
}

/// VERIFY - verify attributes are the same (cache validation)
async fn op_verify(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Read fattr4 to compare
    let mut fattr = Fattr4::default();
    fattr.deserialize(input)?;

    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    debug!("VERIFY: fileid={} checking attributes", fileid);

    // Get current file status
    let status = handler.fs.get_status_cached(fileid).await?;
    let current_attrs = FileAttrs::from_status(&status);

    // Compare attributes - if they don't match, return NFS4ERR_NOT_SAME
    // If they match, return NFS4_OK (success for VERIFY)
    let attrs_match = compare_fattr4(&fattr, &current_attrs);

    if !attrs_match {
        // Attributes are different - VERIFY fails
        return Err(Nfs4Status::NotSame.into());
    }

    // Attributes are the same - VERIFY succeeds
    Ok(Vec::new())
}

/// Compare fattr4 with current file attributes (simplified comparison)
fn compare_fattr4(fattr: &Fattr4, _current: &FileAttrs) -> bool {
    // Simplified comparison - check size and mtime if present in bitmap
    // In production, should compare all requested attributes
    if fattr.attrmask.is_empty() {
        return true;
    }

    // For now, just check if the change attribute matches
    // This is the most commonly used attribute for cache validation
    // A full implementation would decode fattr.attr_vals and compare each attribute
    true // Simplified: assume match for now
}

/// LOCK - request a byte-range lock
async fn op_lock(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    use crate::nfs4::state::lock::LockType4;

    // Read LOCK4args
    let locktype = input.read_u32::<BigEndian>()?;
    let reclaim = input.read_u32::<BigEndian>()? != 0;
    let offset = input.read_u64::<BigEndian>()?;
    let length = input.read_u64::<BigEndian>()?;
    let locker_type = input.read_u32::<BigEndian>()?;

    // Smart grace period check (following NFS-Ganesha):
    // - reclaim=true: requires grace period (want_grace=true)
    // - reclaim=false: requires NO grace period (want_grace=false)
    // Only check for NEW lock creation, not for existing locks
    let want_grace = reclaim;
    let need_check = locker_type == 1 || reclaim; // new_lock_owner or reclaim

    // Acquire grace status with RAII guard (NFS-Ganesha compatible)
    // The guard automatically releases the reference when dropped
    let _grace_guard = if need_check {
        Some(handler.grace.acquire_grace_status(want_grace)?)
    } else {
        None
    };

    // Read locker union based on type
    let (new_lock_owner, open_stateid, existing_lock_stateid, lock_seqid, lock_owner) =
        if locker_type == 1 {
            // new_lock_owner - open_to_lock_owner4
            let _open_seqid = input.read_u32::<BigEndian>()?;
            let mut open_stateid = Stateid4::default();
            open_stateid.deserialize(input)?;
            let lock_seqid = input.read_u32::<BigEndian>()?;
            let lock_owner_clientid = input.read_u64::<BigEndian>()?;
            let mut lock_owner_data: Vec<u8> = Vec::new();
            lock_owner_data.deserialize(input)?;

            let lock_owner = LockOwner4 {
                clientid: lock_owner_clientid,
                owner: lock_owner_data,
            };

            (true, Some(open_stateid), None, lock_seqid, lock_owner)
        } else {
            // existing_lock_owner - exist_lock_owner4
            let mut lock_stateid = Stateid4::default();
            lock_stateid.deserialize(input)?;
            let lock_seqid = input.read_u32::<BigEndian>()?;

            // Get lock owner from existing lock state
            let lock_state = handler
                .locks
                .get_lock_state(&lock_stateid)
                .ok_or(Nfs4Status::BadStateid)?;

            (
                false,
                None,
                Some(lock_stateid),
                lock_seqid,
                lock_state.owner.clone(),
            )
        };

    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    // Convert lock type
    let lock_type = LockType4::from(locktype);
    let blocking = lock_type.is_blocking();

    info!(
        "LOCK: fileid={} type={:?} reclaim={} offset={} length={} seqid={} blocking={}",
        fileid, lock_type, reclaim, offset, length, lock_seqid, blocking
    );

    // Validate lock parameters
    if length == 0 {
        return Err(Nfs4Status::Inval.into());
    }

    // Check for range overflow (offset + length > 2^64-1)
    if length != u64::MAX && offset.checked_add(length).is_none() {
        return Err(Nfs4Status::Inval.into());
    }

    // If new lock owner, verify open stateid
    if new_lock_owner {
        if let Some(ref open_stateid) = open_stateid {
            let open_state = handler.opens.verify_stateid(open_stateid)?;

            // Check if open state allows the lock type
            let share_access = open_state.get_access();
            if lock_type.is_write() && (share_access & 0x02) == 0 {
                // WRITE lock requires OPEN4_SHARE_ACCESS_WRITE
                return Err(Nfs4Status::Openmode.into());
            }
            if !lock_type.is_write() && (share_access & 0x01) == 0 {
                // READ lock requires OPEN4_SHARE_ACCESS_READ
                return Err(Nfs4Status::Openmode.into());
            }
        }
    }

    // Determine which stateid to use for lock operation
    let existing_stateid = if new_lock_owner {
        None
    } else {
        existing_lock_stateid.as_ref()
    };

    // Try to acquire the lock
    let lock_stateid = handler.locks.lock(
        lock_owner.clientid,
        lock_owner.owner.clone(),
        fileid,
        lock_type,
        offset,
        length,
        blocking,
        existing_stateid,
    )?;

    info!(
        "LOCK: granted lock stateid={:02x?} for file {} at {}+{}",
        &lock_stateid.other[..4],
        fileid,
        offset,
        length
    );

    // Build response - LOCK4resok
    let mut result = Vec::new();
    lock_stateid.serialize(&mut result)?;

    Ok(result)
}

/// LOCKT - test for lock conflict
async fn op_lockt(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    use crate::nfs4::state::lock::LockType4;

    // Read LOCKT4args
    let locktype = input.read_u32::<BigEndian>()?;
    let offset = input.read_u64::<BigEndian>()?;
    let length = input.read_u64::<BigEndian>()?;
    let owner_clientid = input.read_u64::<BigEndian>()?;
    let mut owner_data: Vec<u8> = Vec::new();
    owner_data.deserialize(input)?;

    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    let lock_type = LockType4::from(locktype);

    info!(
        "LOCKT: fileid={} type={:?} offset={} length={}",
        fileid, lock_type, offset, length
    );

    // Validate lock parameters
    if length == 0 {
        return Err(Nfs4Status::Inval.into());
    }

    // Check for range overflow
    if length != u64::MAX && offset.checked_add(length).is_none() {
        return Err(Nfs4Status::Inval.into());
    }

    // Create lock owner
    let lock_owner = LockOwner4 {
        clientid: owner_clientid,
        owner: owner_data,
    };

    // Test for conflicts
    if let Some(conflict_state) =
        handler
            .locks
            .test_lock(fileid, lock_type, offset, length, &lock_owner)
    {
        // Find the specific conflicting entry on this file
        let entries = conflict_state.lock_entries.read().unwrap();
        let conflict_entry = entries
            .iter()
            .find(|e| e.fileid == fileid && e.overlaps(offset, length))
            .ok_or(Nfs4Status::Serverfault)?;

        let conflict_offset = conflict_entry.get_offset();
        let conflict_length = conflict_entry.get_length();

        info!(
            "LOCKT: conflict found with lock at {}+{} type={:?}",
            conflict_offset, conflict_length, conflict_entry.lock_type
        );

        // Build LOCK4denied response
        let mut result = Vec::new();

        // offset
        conflict_offset.serialize(&mut result)?;
        // length
        conflict_length.serialize(&mut result)?;
        // locktype
        let denied_type = if conflict_entry.lock_type.is_write() {
            2u32
        } else {
            1u32
        };
        denied_type.serialize(&mut result)?;
        // owner (LockOwner4 structure)
        conflict_state.owner.serialize(&mut result)?;

        // Return NFS4ERR_DENIED with conflict info
        return Err(Nfs4Error::with_data(Nfs4Status::Denied, result));
    }

    info!("LOCKT: no conflicts found");

    // No conflicts - return success with empty response
    Ok(Vec::new())
}

/// LOCKU - unlock a byte-range lock
async fn op_locku(
    input: &mut impl Read,
    ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    use crate::nfs4::state::lock::LockType4;

    // Read LOCKU4args
    let locktype = input.read_u32::<BigEndian>()?;
    let seqid = input.read_u32::<BigEndian>()?;
    let mut lock_stateid = Stateid4::default();
    lock_stateid.deserialize(input)?;
    let offset = input.read_u64::<BigEndian>()?;
    let length = input.read_u64::<BigEndian>()?;

    let fh = ctx.require_current_fh()?;
    let fileid = handler.fs.fh_to_fileid(fh)?;

    let lock_type = LockType4::from(locktype);

    info!(
        "LOCKU: fileid={} type={:?} seqid={} stateid={:?} offset={} length={}",
        fileid,
        lock_type,
        seqid,
        &lock_stateid.other[..4],
        offset,
        length
    );

    // Validate lock parameters
    if length == 0 {
        return Err(Nfs4Status::Inval.into());
    }

    // Check for range overflow
    if length != u64::MAX && offset.checked_add(length).is_none() {
        return Err(Nfs4Status::Inval.into());
    }

    // Verify lock state exists
    let lock_state = handler
        .locks
        .get_lock_state(&lock_stateid)
        .ok_or(Nfs4Status::BadStateid)?;

    // Verify the unlock request is for the correct file
    // (check if any entry in this state is for this file)
    let entries = lock_state.lock_entries.read().unwrap();
    let has_file_lock = entries.iter().any(|e| e.fileid == fileid);
    drop(entries);

    if !has_file_lock {
        return Err(Nfs4Status::BadStateid.into());
    }

    // Release the lock (supports partial unlock)
    // This returns updated stateid with incremented seqid
    let new_stateid = handler.locks.unlock(&lock_stateid, offset, length)?;

    info!(
        "LOCKU: released lock on file {} at {}+{}, new stateid seqid={}",
        fileid, offset, length, new_stateid.seqid
    );

    // Return updated stateid
    let mut result = Vec::new();
    new_stateid.serialize(&mut result)?;

    Ok(result)
}

/// RELEASE_LOCKOWNER - NFSv4.0 release lock owner (not needed in NFSv4.1)
async fn op_release_lockowner(
    input: &mut impl Read,
    _ctx: &CompoundContext,
    handler: &CompoundHandler,
) -> Nfs4Result<Vec<u8>> {
    // Read lock_owner4
    let clientid = input.read_u64::<BigEndian>()?;
    let mut owner_data: Vec<u8> = Vec::new();
    owner_data.deserialize(input)?;

    tracing::info!(
        "RELEASE_LOCKOWNER: clientid={}, owner={:?}",
        clientid,
        String::from_utf8_lossy(&owner_data)
    );

    // Verify client exists
    handler
        .clients
        .get_client(clientid)
        .ok_or(Nfs4Status::StaleClientid)?;

    // Create lock owner
    let lock_owner = LockOwner4 {
        clientid,
        owner: owner_data,
    };

    // Release all locks held by this lock owner
    handler.locks.release_all_for_owner(&lock_owner);

    tracing::info!("RELEASE_LOCKOWNER: completed for clientid={}", clientid);
    Ok(Vec::new())
}

/// PUTPUBFH - set current filehandle to public filehandle
///
/// Per RFC 7530/RFC 8881, PUTPUBFH sets the current filehandle to the
/// public filehandle. In most implementations (including NFS-Ganesha),
/// this is equivalent to PUTROOTFH since there's no separate public
/// namespace.
fn op_putpubfh(ctx: &mut CompoundContext, handler: &CompoundHandler) -> Nfs4Result<Vec<u8>> {
    // PUTPUBFH is functionally equivalent to PUTROOTFH
    // Set current filehandle to root
    let root_fh = handler.fs.fileid_to_fh(handler.fs.root_fileid());
    ctx.current_fh = Some(root_fh);
    Ok(Vec::new())
}

