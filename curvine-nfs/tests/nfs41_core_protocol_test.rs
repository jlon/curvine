use std::io::{Cursor, Read};
use std::sync::Arc;
use std::time::Duration;

use curvine_common::conf::NfsGatewayConf;
use curvine_nfs::gateway::CurvineNfsFileSystem;
use curvine_nfs::nfs;
use curvine_nfs::nfs4::delegation::delegation_type;
use curvine_nfs::nfs4::error::Nfs4Status;
use curvine_nfs::nfs4::handlers::handle_nfs4;
use curvine_nfs::nfs4::state::{
    ClientManager, LockManager, OpenManager, PersistenceConfig, StatePersistenceManager,
};
use curvine_nfs::nfs4::{ClientOwner4, CompoundHandler, Nfs4FileHandle, Nfs4FileSystem, Nfstime4};
use curvine_nfs::nfs4::{SessionManager, Sessionid4, Stateid4};
use curvine_nfs::protocol::rpc::{
    auth_unix, call_body, opaque_auth, reply_body, rpc_body, rpc_msg,
};
use curvine_nfs::protocol::xdr::XDR;
use curvine_nfs::server::context::{OutboundTx, RPCContext};
use curvine_nfs::server::transaction::TransactionTracker;
use curvine_tests::Testing;
use tokio::sync::mpsc;

const EXCHGID4_FLAG_USE_NON_PNFS: u32 = 0x0001_0000;
const EXCHGID4_FLAG_USE_PNFS_MDS: u32 = 0x0002_0000;
const CREATE_SESSION4_FLAG_CONN_BACK_CHAN: u32 = 0x0000_0002;
const OPEN4_SHARE_ACCESS_READ: u32 = 0x0000_0001;
const OPEN4_SHARE_ACCESS_WRITE: u32 = 0x0000_0002;
const WANT_READ_DELEG: u32 = 0x0000_0200;

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

fn exchange_id_body(owner: ClientOwner4, flags: u32) -> Vec<u8> {
    let mut body = Vec::new();
    42u32.serialize(&mut body).unwrap();
    owner.serialize(&mut body).unwrap();
    flags.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    body
}

fn create_session_body(clientid: u64, seqid: u32, flags: u32, cb_program: u32) -> Vec<u8> {
    fn encode_channel_attrs(out: &mut Vec<u8>, max_requests: u32) {
        0u32.serialize(out).unwrap();
        1_048_576u32.serialize(out).unwrap();
        1_048_576u32.serialize(out).unwrap();
        0u32.serialize(out).unwrap();
        64u32.serialize(out).unwrap();
        max_requests.serialize(out).unwrap();
        0u32.serialize(out).unwrap();
    }

    let mut body = Vec::new();
    43u32.serialize(&mut body).unwrap();
    clientid.serialize(&mut body).unwrap();
    seqid.serialize(&mut body).unwrap();
    flags.serialize(&mut body).unwrap();
    encode_channel_attrs(&mut body, 64);
    encode_channel_attrs(&mut body, 8);
    cb_program.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    body
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

fn bind_body(sessionid: Sessionid4, dir: u32) -> Vec<u8> {
    let mut body = Vec::new();
    41u32.serialize(&mut body).unwrap();
    sessionid.serialize(&mut body).unwrap();
    dir.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    body
}

fn putfh_body(fh: &Nfs4FileHandle) -> Vec<u8> {
    let mut body = Vec::new();
    22u32.serialize(&mut body).unwrap();
    fh.serialize(&mut body).unwrap();
    body
}

fn open_body(clientid: u64, owner: &[u8], share_access: u32, name: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    18u32.serialize(&mut body).unwrap();
    1u32.serialize(&mut body).unwrap();
    share_access.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    clientid.serialize(&mut body).unwrap();
    owner.to_vec().serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    name.to_vec().serialize(&mut body).unwrap();
    body
}

fn delegreturn_body(stateid: Stateid4) -> Vec<u8> {
    let mut body = Vec::new();
    8u32.serialize(&mut body).unwrap();
    stateid.serialize(&mut body).unwrap();
    body
}

fn reclaim_complete_body(one_fs: bool) -> Vec<u8> {
    let mut body = Vec::new();
    58u32.serialize(&mut body).unwrap();
    one_fs.serialize(&mut body).unwrap();
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
            match op {
                53 if idx == 0 => {
                    let mut skip = vec![0u8; 36];
                    cur.read_exact(&mut skip).unwrap();
                    skip
                }
                42 => {
                    let mut clientid = 0u64;
                    clientid.deserialize(&mut cur).unwrap();
                    let mut seqid = 0u32;
                    seqid.deserialize(&mut cur).unwrap();
                    let mut flags = 0u32;
                    flags.deserialize(&mut cur).unwrap();
                    let mut spr_how = 0u32;
                    spr_how.deserialize(&mut cur).unwrap();
                    let mut minor = 0u64;
                    minor.deserialize(&mut cur).unwrap();
                    let mut major = Vec::<u8>::new();
                    major.deserialize(&mut cur).unwrap();
                    let mut scope = Vec::<u8>::new();
                    scope.deserialize(&mut cur).unwrap();
                    let mut impl_count = 0u32;
                    impl_count.deserialize(&mut cur).unwrap();
                    for _ in 0..impl_count {
                        let mut domain = Vec::<u8>::new();
                        domain.deserialize(&mut cur).unwrap();
                        let mut name = Vec::<u8>::new();
                        name.deserialize(&mut cur).unwrap();
                        let mut time = Nfstime4::default();
                        time.deserialize(&mut cur).unwrap();
                    }

                    let mut data = Vec::new();
                    clientid.serialize(&mut data).unwrap();
                    seqid.serialize(&mut data).unwrap();
                    flags.serialize(&mut data).unwrap();
                    spr_how.serialize(&mut data).unwrap();
                    minor.serialize(&mut data).unwrap();
                    major.serialize(&mut data).unwrap();
                    scope.serialize(&mut data).unwrap();
                    data
                }
                43 => {
                    fn read_channel_attrs(cur: &mut Cursor<&[u8]>, out: &mut Vec<u8>) {
                        for _ in 0..6 {
                            let mut val = 0u32;
                            val.deserialize(cur).unwrap();
                            val.serialize(out).unwrap();
                        }
                        let mut len = 0u32;
                        len.deserialize(cur).unwrap();
                        len.serialize(out).unwrap();
                        for _ in 0..len {
                            let mut rdma = 0u32;
                            rdma.deserialize(cur).unwrap();
                            rdma.serialize(out).unwrap();
                        }
                    }

                    let mut sessionid = Sessionid4::default();
                    sessionid.deserialize(&mut cur).unwrap();
                    let mut seqid = 0u32;
                    seqid.deserialize(&mut cur).unwrap();
                    let mut flags = 0u32;
                    flags.deserialize(&mut cur).unwrap();

                    let mut data = Vec::new();
                    sessionid.serialize(&mut data).unwrap();
                    seqid.serialize(&mut data).unwrap();
                    flags.serialize(&mut data).unwrap();
                    read_channel_attrs(&mut cur, &mut data);
                    read_channel_attrs(&mut cur, &mut data);
                    data
                }
                41 => {
                    let mut sessionid = Sessionid4::default();
                    sessionid.deserialize(&mut cur).unwrap();
                    let mut dir = 0u32;
                    dir.deserialize(&mut cur).unwrap();
                    let mut rdma = 0u32;
                    rdma.deserialize(&mut cur).unwrap();
                    let mut data = Vec::new();
                    sessionid.serialize(&mut data).unwrap();
                    dir.serialize(&mut data).unwrap();
                    rdma.serialize(&mut data).unwrap();
                    data
                }
                18 => {
                    let mut stateid = Stateid4::default();
                    stateid.deserialize(&mut cur).unwrap();
                    let mut atomic = false;
                    atomic.deserialize(&mut cur).unwrap();
                    let mut before = 0u64;
                    before.deserialize(&mut cur).unwrap();
                    let mut after = 0u64;
                    after.deserialize(&mut cur).unwrap();
                    let mut rflags = 0u32;
                    rflags.deserialize(&mut cur).unwrap();
                    let mut attrset_len = 0u32;
                    attrset_len.deserialize(&mut cur).unwrap();
                    for _ in 0..attrset_len {
                        let mut word = 0u32;
                        word.deserialize(&mut cur).unwrap();
                        let _ = word;
                    }
                    let mut deleg_type = 0u32;
                    deleg_type.deserialize(&mut cur).unwrap();

                    let mut data = Vec::new();
                    stateid.serialize(&mut data).unwrap();
                    atomic.serialize(&mut data).unwrap();
                    before.serialize(&mut data).unwrap();
                    after.serialize(&mut data).unwrap();
                    rflags.serialize(&mut data).unwrap();
                    attrset_len.serialize(&mut data).unwrap();
                    deleg_type.serialize(&mut data).unwrap();

                    match deleg_type {
                        delegation_type::OPEN_DELEGATE_READ => {
                            let mut deleg_stateid = Stateid4::default();
                            deleg_stateid.deserialize(&mut cur).unwrap();
                            let mut recall = false;
                            recall.deserialize(&mut cur).unwrap();
                            let mut ace_type = 0u32;
                            ace_type.deserialize(&mut cur).unwrap();
                            let mut ace_flag = 0u32;
                            ace_flag.deserialize(&mut cur).unwrap();
                            let mut access = 0u32;
                            access.deserialize(&mut cur).unwrap();
                            let mut who = Vec::<u8>::new();
                            who.deserialize(&mut cur).unwrap();
                            deleg_stateid.serialize(&mut data).unwrap();
                            recall.serialize(&mut data).unwrap();
                            ace_type.serialize(&mut data).unwrap();
                            ace_flag.serialize(&mut data).unwrap();
                            access.serialize(&mut data).unwrap();
                            who.serialize(&mut data).unwrap();
                        }
                        delegation_type::OPEN_DELEGATE_NONE_EXT => {
                            let mut why = 0u32;
                            why.deserialize(&mut cur).unwrap();
                            why.serialize(&mut data).unwrap();
                        }
                        _ => {}
                    }
                    data
                }
                22 | 8 | 58 => Vec::new(),
                _ => Vec::new(),
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

fn make_context(
    v3: Arc<CurvineNfsFileSystem>,
    handler: Arc<CompoundHandler>,
    outbound_tx: Option<OutboundTx>,
) -> RPCContext {
    RPCContext {
        local_port: 2049,
        client_addr: Arc::<str>::from("127.0.0.1:2049"),
        auth: auth_unix::default(),
        vfs: v3,
        nfs4_handler: Some(handler),
        mount_signal: None,
        export_name: Arc::new("/".to_string()),
        transaction_tracker: Arc::new(TransactionTracker::new(Duration::from_secs(60))),
        outbound_tx,
    }
}

fn protocol_exchange_and_create_session(
    xid_base: u32,
    owner_name: &[u8],
    request_pnfs_flags: u32,
    create_flags: u32,
    cb_program: u32,
    ctx: &RPCContext,
) -> Result<(u64, Sessionid4, u32), Box<dyn std::error::Error + Send + Sync>> {
    let owner = ClientOwner4 {
        co_verifier: [owner_name[0]; 8],
        co_ownerid: owner_name.to_vec(),
    };

    let body = wrap_compound(1, &[exchange_id_body(owner, request_pnfs_flags)]);
    let mut input = Cursor::new(body);
    let mut output = Vec::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(handle_nfs4(
        xid_base,
        compound_call(),
        &mut input,
        &mut output,
        ctx,
    ))?;
    let (overall, results) = parse_reply(&output);
    assert_eq!(overall, Nfs4Status::Ok);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 42);
    assert_eq!(results[0].1, Nfs4Status::Ok);

    let mut cur = Cursor::new(&results[0].2);
    let mut clientid = 0u64;
    clientid.deserialize(&mut cur).unwrap();
    let mut seqid = 0u32;
    seqid.deserialize(&mut cur).unwrap();
    let mut flags = 0u32;
    flags.deserialize(&mut cur).unwrap();

    let body = wrap_compound(
        1,
        &[create_session_body(
            clientid,
            seqid,
            create_flags,
            cb_program,
        )],
    );
    let mut input = Cursor::new(body);
    let mut output = Vec::new();
    rt.block_on(handle_nfs4(
        xid_base + 1,
        compound_call(),
        &mut input,
        &mut output,
        ctx,
    ))?;
    let (overall, results) = parse_reply(&output);
    assert_eq!(overall, Nfs4Status::Ok);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 43);
    assert_eq!(results[0].1, Nfs4Status::Ok);

    let mut cur = Cursor::new(&results[0].2);
    let mut sessionid = Sessionid4::default();
    sessionid.deserialize(&mut cur).unwrap();

    Ok((clientid, sessionid, flags))
}

#[test]
fn test_nfs41_protocol_exchange_create_session_sequence_and_reclaim_complete(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .build()?;
    testing.start_cluster()?;
    let cluster = testing.get_active_cluster_conf()?;
    let rt = Arc::new(cluster.client_rpc_conf().create_runtime());
    let gateway = NfsGatewayConf {
        pnfs_ds_secret: Some("nfs41-core-test-secret".to_string()),
        ..Default::default()
    };
    let (v3, handler, _clients, _sessions) = build_handler(cluster, gateway, rt)?;
    let ctx = make_context(v3, handler, None);

    let (_clientid, sessionid, exchange_flags) =
        protocol_exchange_and_create_session(100, b"core-session-client", 0, 0, 0, &ctx)?;
    assert_ne!(exchange_flags & EXCHGID4_FLAG_USE_PNFS_MDS, 0);
    assert_eq!(exchange_flags & EXCHGID4_FLAG_USE_NON_PNFS, 0);

    let body = wrap_compound(1, &[sequence_body(sessionid, 1)]);
    let mut input = Cursor::new(body);
    let mut output = Vec::new();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(handle_nfs4(
        102,
        compound_call(),
        &mut input,
        &mut output,
        &ctx,
    ))?;
    let (overall, results) = parse_reply(&output);
    assert_eq!(overall, Nfs4Status::Ok);
    assert_eq!(results[0].0, 53);
    assert_eq!(results[0].1, Nfs4Status::Ok);

    let body = wrap_compound(
        1,
        &[sequence_body(sessionid, 2), reclaim_complete_body(false)],
    );
    let mut input = Cursor::new(body);
    let mut output = Vec::new();
    runtime.block_on(handle_nfs4(
        103,
        compound_call(),
        &mut input,
        &mut output,
        &ctx,
    ))?;
    let (overall, results) = parse_reply(&output);
    assert_eq!(overall, Nfs4Status::Ok);
    assert_eq!(results[1].0, 58);
    assert_eq!(results[1].1, Nfs4Status::Ok);

    let body = wrap_compound(
        1,
        &[sequence_body(sessionid, 3), reclaim_complete_body(false)],
    );
    let mut input = Cursor::new(body);
    let mut output = Vec::new();
    runtime.block_on(handle_nfs4(
        104,
        compound_call(),
        &mut input,
        &mut output,
        &ctx,
    ))?;
    let (overall, results) = parse_reply(&output);
    assert_eq!(overall, Nfs4Status::CompleteAlready);
    assert_eq!(results[1].1, Nfs4Status::CompleteAlready);

    Ok(())
}

#[test]
fn test_nfs41_protocol_delegation_recall_and_return(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .build()?;
    testing.start_cluster()?;
    let cluster = testing.get_active_cluster_conf()?;
    let rt = Arc::new(cluster.client_rpc_conf().create_runtime());
    let gateway = NfsGatewayConf {
        delegation_enabled: true,
        pnfs_ds_secret: Some("nfs41-core-test-secret".to_string()),
        ..Default::default()
    };
    let (v3, handler, _clients, _sessions) = build_handler(cluster, gateway, rt.clone())?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let ctx1 = make_context(v3.clone(), handler.clone(), Some(tx));
    let ctx2 = make_context(v3, handler.clone(), None);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let root_fh = handler.fs.fileid_to_fh(handler.fs.root_fileid());
    let file_fh = runtime.block_on(async {
        let (fileid, _) = handler
            .fs
            .create_file(handler.fs.root_fileid(), "deleg-proto.txt")
            .await?;
        Ok::<Nfs4FileHandle, Box<dyn std::error::Error + Send + Sync>>(
            handler.fs.fileid_to_fh(fileid),
        )
    })?;
    let file_name = b"deleg-proto.txt";

    let (client1, session1, _flags1) = protocol_exchange_and_create_session(
        200,
        b"deleg-client-1",
        0,
        CREATE_SESSION4_FLAG_CONN_BACK_CHAN,
        0x4000_0000,
        &ctx1,
    )?;
    let (_client2, session2, _flags2) =
        protocol_exchange_and_create_session(210, b"deleg-client-2", 0, 0, 0, &ctx2)?;

    for (xid, sessionid, ctx) in [(212, session1, &ctx1), (213, session2, &ctx2)] {
        let body = wrap_compound(1, &[sequence_body(sessionid, 1), reclaim_complete_body(false)]);
        let mut input = Cursor::new(body);
        let mut output = Vec::new();
        runtime.block_on(handle_nfs4(
            xid,
            compound_call(),
            &mut input,
            &mut output,
            ctx,
        ))?;
        let (overall, results) = parse_reply(&output);
        assert_eq!(overall, Nfs4Status::Ok);
        assert_eq!(results[1].0, 58);
        assert_eq!(results[1].1, Nfs4Status::Ok);
    }

    let body = wrap_compound(1, &[bind_body(session1, 3)]);
    let mut input = Cursor::new(body);
    let mut output = Vec::new();
    runtime.block_on(handle_nfs4(
        202,
        compound_call(),
        &mut input,
        &mut output,
        &ctx1,
    ))?;
    let (overall, results) = parse_reply(&output);
    assert_eq!(overall, Nfs4Status::Ok);
    assert_eq!(results[0].0, 41);
    assert_eq!(results[0].1, Nfs4Status::Ok);

    let body = wrap_compound(
        1,
        &[
            sequence_body(session1, 2),
            putfh_body(&root_fh),
            open_body(
                client1,
                b"deleg-open-owner-1",
                OPEN4_SHARE_ACCESS_READ | WANT_READ_DELEG,
                file_name,
            ),
        ],
    );
    let mut input = Cursor::new(body);
    let mut output = Vec::new();
    runtime.block_on(handle_nfs4(
        203,
        compound_call(),
        &mut input,
        &mut output,
        &ctx1,
    ))?;
    let (overall, results) = parse_reply(&output);
    assert_eq!(overall, Nfs4Status::Ok);
    assert_eq!(results[2].0, 18);
    assert_eq!(results[2].1, Nfs4Status::Ok);

    let mut cur = Cursor::new(&results[2].2);
    let mut open_stateid = Stateid4::default();
    open_stateid.deserialize(&mut cur).unwrap();
    let mut atomic = false;
    atomic.deserialize(&mut cur).unwrap();
    assert!(atomic);
    let mut before = 0u64;
    before.deserialize(&mut cur).unwrap();
    let _ = before;
    let mut after = 0u64;
    after.deserialize(&mut cur).unwrap();
    let _ = after;
    let mut rflags = 0u32;
    rflags.deserialize(&mut cur).unwrap();
    let _ = rflags;
    let mut attrset_len = 0u32;
    attrset_len.deserialize(&mut cur).unwrap();
    assert_eq!(attrset_len, 0);
    let mut deleg_type = 0u32;
    deleg_type.deserialize(&mut cur).unwrap();
    assert_eq!(deleg_type, delegation_type::OPEN_DELEGATE_READ);
    let mut deleg_stateid = Stateid4::default();
    deleg_stateid.deserialize(&mut cur).unwrap();

    let body = wrap_compound(
        1,
        &[
            sequence_body(session2, 2),
            putfh_body(&root_fh),
            open_body(
                client1 + 1,
                b"deleg-open-owner-2",
                OPEN4_SHARE_ACCESS_WRITE,
                file_name,
            ),
        ],
    );
    let mut input = Cursor::new(body);
    let mut output = Vec::new();
    runtime.block_on(handle_nfs4(
        204,
        compound_call(),
        &mut input,
        &mut output,
        &ctx2,
    ))?;
    let (overall, results) = parse_reply(&output);
    assert_eq!(overall, Nfs4Status::Delay);
    assert_eq!(results[2].1, Nfs4Status::Delay);

    let callback = rx
        .try_recv()
        .expect("delegation recall should be emitted")
        .expect("encoded callback bytes");
    let mut cur = Cursor::new(&callback[..]);
    let mut msg = rpc_msg::default();
    msg.deserialize(&mut cur).unwrap();
    match msg.body {
        rpc_body::CALL(call) => {
            assert_eq!(call.proc, 1);
        }
        other => panic!("unexpected callback body: {other:?}"),
    }
    let mut tag = Vec::<u8>::new();
    tag.deserialize(&mut cur).unwrap();
    assert_eq!(tag, b"curvine-cb".to_vec());
    let mut minor = 0u32;
    minor.deserialize(&mut cur).unwrap();
    assert_eq!(minor, 1);
    let mut callback_ident = 0u32;
    callback_ident.deserialize(&mut cur).unwrap();
    assert_eq!(callback_ident, 0);
    let mut op_count = 0u32;
    op_count.deserialize(&mut cur).unwrap();
    assert_eq!(op_count, 2);
    let mut cb_sequence_op = 0u32;
    cb_sequence_op.deserialize(&mut cur).unwrap();
    assert_eq!(cb_sequence_op, 11);
    let mut reply_session = Sessionid4::default();
    reply_session.deserialize(&mut cur).unwrap();
    assert_eq!(reply_session, session1);
    let mut cb_seq = 0u32;
    cb_seq.deserialize(&mut cur).unwrap();
    let _ = cb_seq;
    let mut slot = 0u32;
    slot.deserialize(&mut cur).unwrap();
    let _ = slot;
    let mut highest = 0u32;
    highest.deserialize(&mut cur).unwrap();
    let _ = highest;
    let mut cache = false;
    cache.deserialize(&mut cur).unwrap();
    let _ = cache;
    let mut referring_len = 0u32;
    referring_len.deserialize(&mut cur).unwrap();
    let _ = referring_len;
    let mut cb_recall_op = 0u32;
    cb_recall_op.deserialize(&mut cur).unwrap();
    assert_eq!(cb_recall_op, 4);
    let mut recalled = Stateid4::default();
    recalled.deserialize(&mut cur).unwrap();
    assert_eq!(recalled, deleg_stateid);

    let body = wrap_compound(
        1,
        &[
            sequence_body(session1, 3),
            putfh_body(&file_fh),
            delegreturn_body(deleg_stateid),
        ],
    );
    let mut input = Cursor::new(body);
    let mut output = Vec::new();
    runtime.block_on(handle_nfs4(
        205,
        compound_call(),
        &mut input,
        &mut output,
        &ctx1,
    ))?;
    let (overall, results) = parse_reply(&output);
    assert_eq!(overall, Nfs4Status::Ok);
    assert_eq!(results[2].0, 8);
    assert_eq!(results[2].1, Nfs4Status::Ok);

    let body = wrap_compound(
        1,
        &[
            sequence_body(session2, 3),
            putfh_body(&root_fh),
            open_body(
                client1 + 1,
                b"deleg-open-owner-2-retry",
                OPEN4_SHARE_ACCESS_WRITE,
                file_name,
            ),
        ],
    );
    let mut input = Cursor::new(body);
    let mut output = Vec::new();
    runtime.block_on(handle_nfs4(
        206,
        compound_call(),
        &mut input,
        &mut output,
        &ctx2,
    ))?;
    let (overall, results) = parse_reply(&output);
    assert_eq!(overall, Nfs4Status::Ok);
    assert_eq!(results[2].0, 18);
    assert_eq!(results[2].1, Nfs4Status::Ok);

    let _ = open_stateid;
    Ok(())
}
