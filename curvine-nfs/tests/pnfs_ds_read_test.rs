use std::io::{Cursor, Read};
use std::sync::Arc;
use std::time::Duration;

use curvine_common::conf::NfsGatewayConf;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::WorkerAddress;
use curvine_nfs::gateway::CurvineNfsFileSystem;
use curvine_nfs::nfs;
use curvine_nfs::nfs4::error::Nfs4Status;
use curvine_nfs::nfs4::handlers::handle_nfs4;
use curvine_nfs::nfs4::state::{
    ClientManager, LockManager, OpenManager, PersistenceConfig, StatePersistenceManager,
};
use curvine_nfs::nfs4::{
    CompoundHandler, Fattr4, Nfs4FileHandle, Nfs4FileSystem, Nfs4FileType, SessionManager,
    Sessionid4, Stateid4,
};
use curvine_nfs::protocol::rpc::{
    auth_unix, call_body, opaque_auth, reply_body, rpc_body, rpc_msg,
};
use curvine_nfs::protocol::xdr::XDR;
use curvine_nfs::server::context::RPCContext;
use curvine_nfs::server::transaction::TransactionTracker;
use curvine_tests::Testing;
use orpc::runtime::RpcRuntime;

const FATTR4_TYPE: u32 = 1;
const FATTR4_SIZE: u32 = 4;

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

fn putfh_body(fh: &Nfs4FileHandle) -> Vec<u8> {
    let mut body = Vec::new();
    22u32.serialize(&mut body).unwrap();
    fh.serialize(&mut body).unwrap();
    body
}

fn layoutget_body(stateid: Stateid4, length: u64) -> Vec<u8> {
    let mut body = Vec::new();
    50u32.serialize(&mut body).unwrap();
    false.serialize(&mut body).unwrap();
    1u32.serialize(&mut body).unwrap();
    1u32.serialize(&mut body).unwrap();
    0u64.serialize(&mut body).unwrap();
    length.serialize(&mut body).unwrap();
    0u64.serialize(&mut body).unwrap();
    stateid.serialize(&mut body).unwrap();
    8192u32.serialize(&mut body).unwrap();
    body
}

fn getattr_body() -> Vec<u8> {
    let mut body = Vec::new();
    9u32.serialize(&mut body).unwrap();
    vec![(1u32 << FATTR4_TYPE) | (1u32 << FATTR4_SIZE)]
        .serialize(&mut body)
        .unwrap();
    body
}

fn read_body(stateid: Stateid4, offset: u64, count: u32) -> Vec<u8> {
    let mut body = Vec::new();
    25u32.serialize(&mut body).unwrap();
    stateid.serialize(&mut body).unwrap();
    offset.serialize(&mut body).unwrap();
    count.serialize(&mut body).unwrap();
    body
}

fn commit_body(offset: u64, count: u32) -> Vec<u8> {
    let mut body = Vec::new();
    5u32.serialize(&mut body).unwrap();
    offset.serialize(&mut body).unwrap();
    count.serialize(&mut body).unwrap();
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
        let data = if status == Nfs4Status::Ok {
            if idx == 0 && op == 53 {
                let mut skip = vec![0u8; 36];
                cur.read_exact(&mut skip).unwrap();
                skip
            } else if op == 9 {
                let mut fattr = Fattr4::default();
                fattr.deserialize(&mut cur).unwrap();
                let mut data = Vec::new();
                fattr.serialize(&mut data).unwrap();
                data
            } else if op == 22 || op == 5 {
                Vec::new()
            } else if op == 25 {
                let mut eof = false;
                eof.deserialize(&mut cur).unwrap();
                let mut read_data = Vec::<u8>::new();
                read_data.deserialize(&mut cur).unwrap();
                let mut data = Vec::new();
                eof.serialize(&mut data).unwrap();
                read_data.serialize(&mut data).unwrap();
                data
            } else if op == 50 {
                let mut return_on_close = false;
                return_on_close.deserialize(&mut cur).unwrap();
                let mut stateid = Stateid4::default();
                stateid.deserialize(&mut cur).unwrap();
                let mut len = 0u32;
                len.deserialize(&mut cur).unwrap();
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
            } else {
                Vec::new()
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

fn first_alternate_worker(workers: &[WorkerAddress], worker_id: u32) -> WorkerAddress {
    workers
        .iter()
        .find(|worker| worker.worker_id != worker_id)
        .cloned()
        .expect("expected an alternate worker")
}

#[test]
fn test_pnfs_ds_read_path_from_layout_block_fh(
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

    let (v3_mds, mds_handler, mds_clients, mds_sessions) =
        build_handler(cluster.clone(), gateway.clone(), rt.clone())?;
    let (v3_ds, ds_handler, ds_clients, ds_sessions) =
        build_handler(cluster.clone(), gateway, rt.clone())?;
    let fs = testing.get_fs(Some(rt.clone()), Some(cluster.clone()))?;
    let path = Path::from_str("/pnfs-ds.txt")?;
    let contents = b"pnfs-data-server-read-path";

    rt.block_on(async {
        let mut writer = fs.create(&path, true).await?;
        writer.write(contents).await?;
        writer.complete().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    let workers = rt.block_on(async {
        Ok::<Vec<WorkerAddress>, Box<dyn std::error::Error + Send + Sync>>(
            fs.get_master_info()
                .await?
                .live_workers
                .into_iter()
                .map(|worker| worker.address)
                .collect(),
        )
    })?;

    let owner = curvine_nfs::nfs4::ClientOwner4 {
        co_verifier: [9u8; 8],
        co_ownerid: b"pnfs-mds-client".to_vec(),
    };
    let (clientid, _, _) = mds_clients.exchange_id(owner)?;
    mds_clients.confirm_client(clientid)?;
    let session = mds_sessions.create_session(clientid)?;
    let mds_ctx = make_context(v3_mds, mds_handler.clone());

    let (block_fh, ds_worker, file_fh) = rt.block_on(async {
        let root_id = mds_handler.fs.root_fileid();
        let (fileid, _status) = mds_handler.fs.lookup(root_id, "pnfs-ds.txt").await?;
        let file_fh = mds_handler.fs.fileid_to_fh(fileid);
        let file_path = mds_handler.fs.get_path(fileid)?;
        let (open_state, _) = mds_handler.opens.open(
            clientid,
            b"pnfs-layout-owner".to_vec(),
            fileid,
            file_path,
            1,
            0,
        )?;

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
        handle_nfs4(101, compound_call(), &mut input, &mut output, &mds_ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Ok);
        assert_eq!(results[2].1, Nfs4Status::Ok);

        let mut cur = Cursor::new(&results[2].2);
        let mut return_on_close = false;
        return_on_close.deserialize(&mut cur).unwrap();
        assert!(!return_on_close);
        let mut layout_stateid = Stateid4::default();
        layout_stateid.deserialize(&mut cur).unwrap();
        let _ = layout_stateid;
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
        let mut layout_cur = Cursor::new(&layout_body);
        let mut deviceid = [0u8; 16];
        layout_cur.read_exact(&mut deviceid).unwrap();
        let _ = deviceid;
        let mut util = 0u32;
        util.deserialize(&mut layout_cur).unwrap();
        let _ = util;
        let mut first_stripe = 0u32;
        first_stripe.deserialize(&mut layout_cur).unwrap();
        assert_eq!(first_stripe, 0);
        let mut pattern_offset = 0u64;
        pattern_offset.deserialize(&mut layout_cur).unwrap();
        assert_eq!(pattern_offset, 0);
        let mut fh_count = 0u32;
        fh_count.deserialize(&mut layout_cur).unwrap();
        assert!(fh_count >= 1);
        let mut block_fh = Nfs4FileHandle::default();
        block_fh.deserialize(&mut layout_cur).unwrap();

        let block_locations = fs.get_block_locations(&path).await?;
        let ds_worker = block_locations.block_locs[0].locs[0].clone();

        Ok::<
            (Nfs4FileHandle, WorkerAddress, Nfs4FileHandle),
            Box<dyn std::error::Error + Send + Sync>,
        >((block_fh, ds_worker, file_fh))
    })?;

    ds_handler.enable_pnfs_ds(ds_worker.clone());
    let ds_ctx = make_context(v3_ds, ds_handler.clone());
    let ds_owner = curvine_nfs::nfs4::ClientOwner4 {
        co_verifier: [10u8; 8],
        co_ownerid: b"pnfs-ds-client".to_vec(),
    };
    let (ds_clientid, _, _) = ds_clients.exchange_id(ds_owner)?;
    ds_clients.confirm_client(ds_clientid)?;
    let ds_session = ds_sessions.create_session(ds_clientid)?;

    rt.block_on(async {
        let body = wrap_compound(
            1,
            &[
                sequence_body(ds_session.sessionid, 1),
                putfh_body(&block_fh),
                getattr_body(),
                read_body(Stateid4::READ_BYPASS, 0, contents.len() as u32),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(201, compound_call(), &mut input, &mut output, &ds_ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Ok);
        assert_eq!(results[2].1, Nfs4Status::Ok);
        assert_eq!(results[3].1, Nfs4Status::Ok);

        let mut fattr_cur = Cursor::new(&results[2].2);
        let mut fattr = Fattr4::default();
        fattr.deserialize(&mut fattr_cur).unwrap();
        let mut attr_vals = Cursor::new(&fattr.attr_vals);
        let mut file_type = 0u32;
        file_type.deserialize(&mut attr_vals).unwrap();
        let mut size = 0u64;
        size.deserialize(&mut attr_vals).unwrap();
        assert_eq!(file_type, Nfs4FileType::Regular as u32);
        assert_eq!(size, contents.len() as u64);

        let mut read_cur = Cursor::new(&results[3].2);
        let mut eof = false;
        eof.deserialize(&mut read_cur).unwrap();
        assert!(eof);
        let mut data = Vec::<u8>::new();
        data.deserialize(&mut read_cur).unwrap();
        assert_eq!(data, contents);

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    rt.block_on(async {
        let body = wrap_compound(
            1,
            &[
                sequence_body(ds_session.sessionid, 2),
                putfh_body(&block_fh),
                commit_body(0, contents.len() as u32),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(202, compound_call(), &mut input, &mut output, &ds_ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Notsupp);
        assert_eq!(results[2].1, Nfs4Status::Notsupp);

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    let (v3_wrong, wrong_handler, wrong_clients, wrong_sessions) = build_handler(
        cluster.clone(),
        NfsGatewayConf {
            pnfs_ds_secret: Some("pnfs-test-secret".to_string()),
            ..Default::default()
        },
        rt.clone(),
    )?;
    wrong_handler.enable_pnfs_ds(first_alternate_worker(&workers, ds_worker.worker_id));
    let wrong_ctx = make_context(v3_wrong, wrong_handler.clone());
    let wrong_owner = curvine_nfs::nfs4::ClientOwner4 {
        co_verifier: [11u8; 8],
        co_ownerid: b"pnfs-ds-wrong-worker".to_vec(),
    };
    let (wrong_clientid, _, _) = wrong_clients.exchange_id(wrong_owner)?;
    wrong_clients.confirm_client(wrong_clientid)?;
    let wrong_session = wrong_sessions.create_session(wrong_clientid)?;

    rt.block_on(async {
        let body = wrap_compound(
            1,
            &[
                sequence_body(wrong_session.sessionid, 1),
                putfh_body(&block_fh),
            ],
        );
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        handle_nfs4(203, compound_call(), &mut input, &mut output, &wrong_ctx).await?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Stale);
        assert_eq!(results[1].1, Nfs4Status::Stale);

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    let _ = file_fh;
    Ok(())
}
