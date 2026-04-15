use std::io::{Cursor, Read};
use std::sync::Arc;
use std::time::Duration;

use curvine_common::conf::NfsGatewayConf;
use curvine_common::fs::{Path, Writer};
use curvine_nfs::gateway::CurvineNfsFileSystem;
use curvine_nfs::nfs;
use curvine_nfs::nfs4::error::Nfs4Status;
use curvine_nfs::nfs4::handlers::handle_nfs4;
use curvine_nfs::nfs4::state::{
    ClientManager, LockManager, OpenManager, PersistenceConfig, StatePersistenceManager,
};
use curvine_nfs::nfs4::{CompoundHandler, Nfs4FileSystem, SessionManager, Sessionid4, Stateid4};
use curvine_nfs::protocol::rpc::{
    auth_unix, call_body, opaque_auth, reply_body, rpc_body, rpc_msg,
};
use curvine_nfs::protocol::xdr::XDR;
use curvine_nfs::server::context::RPCContext;
use curvine_nfs::server::transaction::TransactionTracker;
use curvine_tests::Testing;
use orpc::runtime::RpcRuntime;

fn compound_call() -> call_body {
    call_body {
        rpcvers: 2,
        prog: nfs::PROGRAM,
        vers: 4,
        proc: 1,
        cred: opaque_auth::default(),
        verf: opaque_auth::default(),
    }
}

fn sequence_body(sessionid: Sessionid4, seq: u32) -> Vec<u8> {
    let mut body = Vec::new();
    53u32.serialize(&mut body).unwrap();
    sessionid.serialize(&mut body).unwrap();
    seq.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    body
}

fn putfh_body(fh: &curvine_nfs::nfs4::Nfs4FileHandle) -> Vec<u8> {
    let mut body = Vec::new();
    22u32.serialize(&mut body).unwrap();
    fh.serialize(&mut body).unwrap();
    body
}

fn putrootfh_body() -> Vec<u8> {
    let mut body = Vec::new();
    24u32.serialize(&mut body).unwrap();
    body
}

fn layoutget_body(stateid: Stateid4, length: u64) -> Vec<u8> {
    layoutget_body_with(stateid, 1, length, 8192)
}

fn layoutget_body_with(stateid: Stateid4, layout_type: u32, length: u64, maxcount: u32) -> Vec<u8> {
    let mut body = Vec::new();
    50u32.serialize(&mut body).unwrap();
    false.serialize(&mut body).unwrap();
    layout_type.serialize(&mut body).unwrap();
    1u32.serialize(&mut body).unwrap(); // READ
    0u64.serialize(&mut body).unwrap();
    length.serialize(&mut body).unwrap();
    0u64.serialize(&mut body).unwrap();
    stateid.serialize(&mut body).unwrap();
    maxcount.serialize(&mut body).unwrap();
    body
}

fn getdeviceinfo_body(deviceid: [u8; 16]) -> Vec<u8> {
    getdeviceinfo_body_with(deviceid, 1, 8192)
}

fn getdeviceinfo_body_with(deviceid: [u8; 16], layout_type: u32, maxcount: u32) -> Vec<u8> {
    let mut body = Vec::new();
    47u32.serialize(&mut body).unwrap();
    body.extend_from_slice(&deviceid);
    layout_type.serialize(&mut body).unwrap();
    maxcount.serialize(&mut body).unwrap();
    Vec::<u32>::new().serialize(&mut body).unwrap();
    body
}

fn layoutreturn_body(stateid: Stateid4) -> Vec<u8> {
    let mut body = Vec::new();
    51u32.serialize(&mut body).unwrap();
    false.serialize(&mut body).unwrap();
    1u32.serialize(&mut body).unwrap(); // file layout
    1u32.serialize(&mut body).unwrap(); // READ
    1u32.serialize(&mut body).unwrap(); // LAYOUTRETURN4_FILE
    0u64.serialize(&mut body).unwrap();
    u64::MAX.serialize(&mut body).unwrap();
    stateid.serialize(&mut body).unwrap();
    Vec::<u8>::new().serialize(&mut body).unwrap();
    body
}

fn layoutcommit_body(stateid: Stateid4) -> Vec<u8> {
    let mut body = Vec::new();
    49u32.serialize(&mut body).unwrap();
    0u64.serialize(&mut body).unwrap();
    u64::MAX.serialize(&mut body).unwrap();
    false.serialize(&mut body).unwrap();
    stateid.serialize(&mut body).unwrap();
    false.serialize(&mut body).unwrap();
    false.serialize(&mut body).unwrap();
    1u32.serialize(&mut body).unwrap();
    Vec::<u8>::new().serialize(&mut body).unwrap();
    body
}

fn destroy_clientid_body(clientid: u64) -> Vec<u8> {
    let mut body = Vec::new();
    57u32.serialize(&mut body).unwrap();
    clientid.serialize(&mut body).unwrap();
    body
}

fn wrap_compound(minor: u32, ops: &[Vec<u8>]) -> Vec<u8> {
    let mut body = Vec::new();
    Vec::<u8>::new().serialize(&mut body).unwrap();
    minor.serialize(&mut body).unwrap();
    (ops.len() as u32).serialize(&mut body).unwrap();
    for op in ops {
        body.extend_from_slice(op);
    }
    body
}

fn parse_reply(bytes: &[u8]) -> (Nfs4Status, Vec<(u32, Nfs4Status, Vec<u8>)>) {
    let mut cur = Cursor::new(bytes);
    let mut reply = rpc_msg::default();
    reply.deserialize(&mut cur).unwrap();
    match reply.body {
        rpc_body::REPLY(reply_body::MSG_ACCEPTED(_)) => {}
        other => panic!("unexpected rpc reply: {other:?}"),
    }

    let mut overall = Nfs4Status::default();
    overall.deserialize(&mut cur).unwrap();
    let mut tag = Vec::<u8>::new();
    tag.deserialize(&mut cur).unwrap();
    let mut count = 0u32;
    count.deserialize(&mut cur).unwrap();
    let mut out = Vec::new();

    for idx in 0..count {
        let mut op = 0u32;
        op.deserialize(&mut cur).unwrap();
        let mut status = Nfs4Status::default();
        status.deserialize(&mut cur).unwrap();
        let before = cur.position() as usize;
        let data = if status == Nfs4Status::Ok {
            if idx == 0 && op == 53 {
                let mut skip = vec![0u8; 36];
                cur.read_exact(&mut skip).unwrap();
                skip
            } else if op == 22 {
                Vec::new()
            } else if op == 50 {
                let mut return_on_close = false;
                return_on_close.deserialize(&mut cur).unwrap();
                let mut stateid = Stateid4::default();
                stateid.deserialize(&mut cur).unwrap();
                let mut len = 0u32;
                len.deserialize(&mut cur).unwrap();
                assert_eq!(len, 1);
                let mut offset = 0u64;
                offset.deserialize(&mut cur).unwrap();
                let mut length = 0u64;
                length.deserialize(&mut cur).unwrap();
                let mut iomode = 0u32;
                iomode.deserialize(&mut cur).unwrap();
                let mut layout_type = 0u32;
                layout_type.deserialize(&mut cur).unwrap();
                let mut body = Vec::<u8>::new();
                body.deserialize(&mut cur).unwrap();
                let mut data = Vec::new();
                return_on_close.serialize(&mut data).unwrap();
                stateid.serialize(&mut data).unwrap();
                len.serialize(&mut data).unwrap();
                offset.serialize(&mut data).unwrap();
                length.serialize(&mut data).unwrap();
                iomode.serialize(&mut data).unwrap();
                layout_type.serialize(&mut data).unwrap();
                body.serialize(&mut data).unwrap();
                data
            } else if op == 47 {
                let mut layout_type = 0u32;
                layout_type.deserialize(&mut cur).unwrap();
                let mut body = Vec::<u8>::new();
                body.deserialize(&mut cur).unwrap();
                let mut notify = Vec::<u32>::new();
                notify.deserialize(&mut cur).unwrap();
                let mut data = Vec::new();
                layout_type.serialize(&mut data).unwrap();
                body.serialize(&mut data).unwrap();
                notify.serialize(&mut data).unwrap();
                data
            } else if op == 51 {
                let mut present = false;
                present.deserialize(&mut cur).unwrap();
                let mut data = Vec::new();
                present.serialize(&mut data).unwrap();
                data
            } else {
                bytes[before..cur.position() as usize].to_vec()
            }
        } else {
            Vec::new()
        };
        out.push((op, status, data));
    }

    (overall, out)
}

fn build_handler(
    cluster: curvine_common::conf::ClusterConf,
    gateway: NfsGatewayConf,
    rt: Arc<orpc::runtime::Runtime>,
) -> Result<
    (
        Arc<CurvineNfsFileSystem>,
        Arc<CompoundHandler>,
        Arc<ClientManager>,
        Arc<SessionManager>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let v3 = Arc::new(CurvineNfsFileSystem::new(
        cluster.clone(),
        gateway.clone(),
        rt.clone(),
    )?);
    let nfs4 = Arc::new(Nfs4FileSystem::new(
        cluster.clone(),
        gateway.clone(),
        rt.clone(),
    )?);
    let persistence = Arc::new(StatePersistenceManager::new(
        nfs4.clone(),
        PersistenceConfig {
            enabled: false,
            ..Default::default()
        },
    ));
    let sessions = Arc::new(SessionManager::new());
    let clients = Arc::new(ClientManager::new());
    let opens = Arc::new(OpenManager::new());
    let locks = Arc::new(LockManager::new());
    let handler = Arc::new(CompoundHandler::new(
        sessions.clone(),
        clients.clone(),
        opens,
        locks,
        nfs4,
        persistence,
        &gateway,
    ));
    Ok((v3, handler, clients, sessions))
}

fn make_context(v3: Arc<CurvineNfsFileSystem>, handler: Arc<CompoundHandler>) -> RPCContext {
    RPCContext {
        local_port: 2049,
        client_addr: Arc::<str>::from("127.0.0.1:2049"),
        auth: auth_unix::default(),
        vfs: v3,
        nfs4_handler: Some(handler),
        mount_signal: None,
        export_name: Arc::new("/".to_string()),
        transaction_tracker: Arc::new(TransactionTracker::new(Duration::from_secs(60))),
        outbound_tx: None,
    }
}

#[test]
fn test_pnfs_layoutget_getdeviceinfo_and_layoutreturn(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.block_size = 1024;
        })
        .build()?;
    testing.start_cluster()?;
    let cluster = testing.get_active_cluster_conf()?;
    let rt = Arc::new(cluster.client_rpc_conf().create_runtime());
    let mut gateway = NfsGatewayConf::default();
    gateway.pnfs_ds_secret = Some("pnfs-test-secret".to_string());
    let (v3, handler, clients, sessions) = build_handler(cluster.clone(), gateway, rt.clone())?;
    let fs = testing.get_fs(Some(rt.clone()), Some(cluster.clone()))?;
    let path = Path::from_str("/pnfs-meta.txt")?;

    rt.block_on(async {
        let mut writer = fs.create(&path, true).await?;
        writer.write(b"0123456789abcdef0123456789abcdef").await?;
        writer.complete().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    let owner = curvine_nfs::nfs4::ClientOwner4 {
        co_verifier: [1u8; 8],
        co_ownerid: b"pnfs-client".to_vec(),
    };
    let (clientid, _, _) = clients.exchange_id(owner)?;
    clients.confirm_client(clientid)?;
    let session = sessions.create_session(clientid)?;

    let ctx = make_context(v3, handler.clone());

    rt.block_on(async {
        let root_id = handler.fs.root_fileid();
        let (fileid, _status) = handler.fs.lookup(root_id, "pnfs-meta.txt").await?;
        let file_fh = handler.fs.fileid_to_fh(fileid);
        let path = handler.fs.get_path(fileid)?;
        let (open_state, _) =
            handler
                .opens
                .open(clientid, b"pnfs-owner".to_vec(), fileid, path, 1, 0)?;

        let body = wrap_compound(
            1,
            &[
                sequence_body(session.sessionid, 1),
                putfh_body(&file_fh),
                layoutget_body(open_state.stateid, u64::MAX),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(21, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Ok);
        assert_eq!(results.len(), 3);
        assert_eq!(results[2].0, 50);
        assert_eq!(results[2].1, Nfs4Status::Ok);

        let mut cur = Cursor::new(&results[2].2);
        let mut return_on_close = false;
        return_on_close.deserialize(&mut cur).unwrap();
        assert!(!return_on_close);
        let mut layout_stateid = Stateid4::default();
        layout_stateid.deserialize(&mut cur).unwrap();
        let mut segs = 0u32;
        segs.deserialize(&mut cur).unwrap();
        assert_eq!(segs, 1);
        let mut offset = 0u64;
        offset.deserialize(&mut cur).unwrap();
        let mut length = 0u64;
        length.deserialize(&mut cur).unwrap();
        let mut iomode = 0u32;
        iomode.deserialize(&mut cur).unwrap();
        let mut layout_type = 0u32;
        layout_type.deserialize(&mut cur).unwrap();
        assert_eq!(layout_type, 1);
        let mut layout_body = Vec::<u8>::new();
        layout_body.deserialize(&mut cur).unwrap();
        let mut body_cur = Cursor::new(&layout_body);
        let mut deviceid = [0u8; 16];
        body_cur.read_exact(&mut deviceid).unwrap();
        let mut util = 0u32;
        util.deserialize(&mut body_cur).unwrap();
        let _ = util;
        let mut first_stripe = 0u32;
        first_stripe.deserialize(&mut body_cur).unwrap();
        assert_eq!(first_stripe, 0);
        let mut pattern_offset = 0u64;
        pattern_offset.deserialize(&mut body_cur).unwrap();
        assert_eq!(pattern_offset, 0);
        let mut fh_count = 0u32;
        fh_count.deserialize(&mut body_cur).unwrap();
        assert!(fh_count >= 1);

        let body = wrap_compound(
            1,
            &[
                sequence_body(session.sessionid, 2),
                getdeviceinfo_body(deviceid),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(22, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Ok);
        assert_eq!(results[1].0, 47);
        assert_eq!(results[1].1, Nfs4Status::Ok);

        let body = wrap_compound(
            1,
            &[
                sequence_body(session.sessionid, 3),
                getdeviceinfo_body_with(deviceid, 99, 8192),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(23, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Notsupp);
        assert_eq!(results[1].1, Nfs4Status::Notsupp);

        let body = wrap_compound(
            1,
            &[
                sequence_body(session.sessionid, 4),
                putfh_body(&file_fh),
                layoutreturn_body(layout_stateid),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(24, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Ok);
        assert_eq!(results[2].0, 51);
        assert_eq!(results[2].1, Nfs4Status::Ok);

        let body = wrap_compound(
            1,
            &[
                sequence_body(session.sessionid, 5),
                putfh_body(&file_fh),
                layoutreturn_body(layout_stateid),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(25, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::BadStateid);
        assert_eq!(results[2].1, Nfs4Status::BadStateid);

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    Ok(())
}

#[test]
fn test_pnfs_metadata_errors_and_unsupported_paths(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.block_size = 1024;
        })
        .build()?;
    testing.start_cluster()?;
    let cluster = testing.get_active_cluster_conf()?;
    let rt = Arc::new(cluster.client_rpc_conf().create_runtime());
    let mut gateway = NfsGatewayConf::default();
    gateway.pnfs_ds_secret = Some("pnfs-test-secret".to_string());
    let (v3, handler, clients, sessions) = build_handler(cluster.clone(), gateway, rt.clone())?;
    let fs = testing.get_fs(Some(rt.clone()), Some(cluster.clone()))?;
    let path = Path::from_str("/pnfs-errors.txt")?;

    rt.block_on(async {
        let mut writer = fs.create(&path, true).await?;
        writer.write(b"0123456789abcdef0123456789abcdef").await?;
        writer.complete().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    let owner = curvine_nfs::nfs4::ClientOwner4 {
        co_verifier: [2u8; 8],
        co_ownerid: b"pnfs-errors-client".to_vec(),
    };
    let (clientid, _, _) = clients.exchange_id(owner)?;
    clients.confirm_client(clientid)?;
    let session = sessions.create_session(clientid)?;

    let ctx = make_context(v3, handler.clone());

    rt.block_on(async {
        let root_id = handler.fs.root_fileid();
        let (fileid, _status) = handler.fs.lookup(root_id, "pnfs-errors.txt").await?;
        let file_fh = handler.fs.fileid_to_fh(fileid);
        let path = handler.fs.get_path(fileid)?;
        let (open_state, _) =
            handler
                .opens
                .open(clientid, b"pnfs-errors-owner".to_vec(), fileid, path, 1, 0)?;

        let body = wrap_compound(
            1,
            &[
                sequence_body(session.sessionid, 1),
                putrootfh_body(),
                layoutget_body(open_state.stateid, u64::MAX),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(31, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Inval);
        assert_eq!(results[2].1, Nfs4Status::Inval);

        let body = wrap_compound(
            1,
            &[
                sequence_body(session.sessionid, 2),
                putfh_body(&file_fh),
                layoutget_body_with(open_state.stateid, 1, u64::MAX, 8),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(32, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Toosmall);
        assert_eq!(results[2].1, Nfs4Status::Toosmall);

        let body = wrap_compound(
            1,
            &[
                sequence_body(session.sessionid, 3),
                putfh_body(&file_fh),
                layoutget_body(open_state.stateid, u64::MAX),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(33, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Ok);
        assert_eq!(results[2].1, Nfs4Status::Ok);
        let mut cur = Cursor::new(&results[2].2);
        let mut return_on_close = false;
        return_on_close.deserialize(&mut cur).unwrap();
        assert!(!return_on_close);
        let mut layout_stateid = Stateid4::default();
        layout_stateid.deserialize(&mut cur).unwrap();
        let mut segs = 0u32;
        segs.deserialize(&mut cur).unwrap();
        assert_eq!(segs, 1);
        let mut offset = 0u64;
        offset.deserialize(&mut cur).unwrap();
        assert_eq!(offset, 0);
        let mut length = 0u64;
        length.deserialize(&mut cur).unwrap();
        assert_eq!(length, u64::MAX);
        let mut iomode = 0u32;
        iomode.deserialize(&mut cur).unwrap();
        assert_eq!(iomode, 1);
        let mut layout_type = 0u32;
        layout_type.deserialize(&mut cur).unwrap();
        assert_eq!(layout_type, 1);
        let mut layout_body = Vec::<u8>::new();
        layout_body.deserialize(&mut cur).unwrap();
        let mut body_cur = Cursor::new(&layout_body);
        let mut deviceid = [0u8; 16];
        body_cur.read_exact(&mut deviceid).unwrap();

        let body = wrap_compound(
            1,
            &[
                sequence_body(session.sessionid, 4),
                layoutreturn_body(layout_stateid),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(34, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Nofilehandle);
        assert_eq!(results[1].1, Nfs4Status::Nofilehandle);

        let body = wrap_compound(
            1,
            &[
                sequence_body(session.sessionid, 5),
                putfh_body(&file_fh),
                layoutcommit_body(layout_stateid),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(35, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Notsupp);
        assert_eq!(results[2].1, Nfs4Status::Notsupp);

        handler.sessions.destroy_session(&session.sessionid)?;
        let body = wrap_compound(1, &[destroy_clientid_body(clientid)]);
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(36, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Ok);
        assert_eq!(results[0].1, Nfs4Status::Ok);

        let owner = curvine_nfs::nfs4::ClientOwner4 {
            co_verifier: [3u8; 8],
            co_ownerid: b"pnfs-errors-client-2".to_vec(),
        };
        let (clientid2, _, _) = clients.exchange_id(owner)?;
        clients.confirm_client(clientid2)?;
        let session2 = sessions.create_session(clientid2)?;

        let body = wrap_compound(
            1,
            &[
                sequence_body(session2.sessionid, 1),
                getdeviceinfo_body(deviceid),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(37, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Noent);
        assert_eq!(results[1].1, Nfs4Status::Noent);

        let body = wrap_compound(
            1,
            &[
                sequence_body(session2.sessionid, 2),
                putfh_body(&file_fh),
                layoutreturn_body(layout_stateid),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(38, compound_call(), &mut input, &mut output, &ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::BadStateid);
        assert_eq!(results[2].1, Nfs4Status::BadStateid);

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    Ok(())
}
