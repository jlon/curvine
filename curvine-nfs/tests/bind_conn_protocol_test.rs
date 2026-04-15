use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use curvine_common::conf::NfsGatewayConf;
use curvine_nfs::gateway::CurvineNfsFileSystem;
use curvine_nfs::nfs;
use curvine_nfs::nfs4::error::Nfs4Status;
use curvine_nfs::nfs4::handlers::handle_nfs4;
use curvine_nfs::nfs4::state::{
    ClientManager, LockManager, OpenManager, PersistenceConfig, StatePersistenceManager,
};
use curvine_nfs::nfs4::{CompoundHandler, Nfs4FileSystem, SessionManager, Sessionid4};
use curvine_nfs::protocol::rpc::{
    auth_unix, call_body, opaque_auth, reply_body, rpc_body, rpc_msg,
};
use curvine_nfs::protocol::xdr::XDR;
use curvine_nfs::server::context::RPCContext;
use curvine_nfs::server::transaction::TransactionTracker;
use curvine_tests::Testing;
use orpc::runtime::RpcRuntime;
use tokio::sync::mpsc;

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

fn bind_body(sessionid: Sessionid4, dir: u32) -> Vec<u8> {
    let mut body = Vec::new();
    Vec::<u8>::new().serialize(&mut body).unwrap(); // tag
    1u32.serialize(&mut body).unwrap(); // minor
    1u32.serialize(&mut body).unwrap(); // op count
    41u32.serialize(&mut body).unwrap(); // BIND_CONN_TO_SESSION
    sessionid.serialize(&mut body).unwrap();
    dir.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap(); // no rdma
    body
}

fn sequence_body(sessionid: Sessionid4, seq: u32) -> Vec<u8> {
    let mut body = Vec::new();
    Vec::<u8>::new().serialize(&mut body).unwrap(); // tag
    1u32.serialize(&mut body).unwrap(); // minor
    1u32.serialize(&mut body).unwrap(); // op count
    53u32.serialize(&mut body).unwrap(); // SEQUENCE
    sessionid.serialize(&mut body).unwrap();
    seq.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap(); // slotid
    0u32.serialize(&mut body).unwrap(); // highest slotid
    0u32.serialize(&mut body).unwrap(); // cachethis
    body
}

fn backchannel_ctl_body(cb_program: u32) -> Vec<u8> {
    let mut body = Vec::new();
    Vec::<u8>::new().serialize(&mut body).unwrap(); // tag
    1u32.serialize(&mut body).unwrap(); // minor
    1u32.serialize(&mut body).unwrap(); // op count
    40u32.serialize(&mut body).unwrap(); // BACKCHANNEL_CTL
    cb_program.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap(); // sec parms length
    body
}

fn parse_compound(bytes: &[u8]) -> (Nfs4Status, u32, Nfs4Status, Vec<u8>) {
    let mut cur = Cursor::new(bytes);
    let mut reply = rpc_msg::default();
    reply.deserialize(&mut cur).unwrap();
    match reply.body {
        rpc_body::REPLY(reply_body::MSG_ACCEPTED(_)) => {}
        other => panic!("unexpected rpc reply: {other:?}"),
    }

    let mut status = Nfs4Status::default();
    status.deserialize(&mut cur).unwrap();
    let mut tag = Vec::<u8>::new();
    tag.deserialize(&mut cur).unwrap();
    let mut count = 0u32;
    count.deserialize(&mut cur).unwrap();
    let mut op = 0u32;
    op.deserialize(&mut cur).unwrap();
    let mut op_status = Nfs4Status::default();
    op_status.deserialize(&mut cur).unwrap();
    let rest = bytes[cur.position() as usize..].to_vec();
    (status, op, op_status, rest)
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
    outbound_tx: Option<curvine_nfs::server::context::OutboundTx>,
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

#[test]
fn test_bind_conn_to_session_clears_cb_path_down(
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
        ..Default::default()
    };

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

    let owner = curvine_nfs::nfs4::ClientOwner4 {
        co_verifier: [1u8; 8],
        co_ownerid: b"bind-client".to_vec(),
    };
    let (clientid, _, _) = clients.exchange_id(owner)?;
    clients.confirm_client(clientid)?;
    let session = sessions.create_session(clientid)?;
    session.set_cb_program(0x4000_0000);

    let (tx, _rx) = mpsc::unbounded_channel();
    let ctx = RPCContext {
        local_port: 2049,
        client_addr: Arc::<str>::from("127.0.0.1:2049"),
        auth: auth_unix::default(),
        vfs: v3,
        nfs4_handler: Some(handler.clone()),
        mount_signal: None,
        export_name: Arc::new("/".to_string()),
        transaction_tracker: Arc::new(TransactionTracker::new(Duration::from_secs(60))),
        outbound_tx: Some(tx),
    };

    let mut bind_in = Cursor::new(bind_body(session.sessionid, 3));
    let mut bind_out = Vec::new();
    rt.block_on(handle_nfs4(
        1,
        compound_call(),
        &mut bind_in,
        &mut bind_out,
        &ctx,
    ))?;
    let (status, op, op_status, bind_rest) = parse_compound(&bind_out);
    assert_eq!(status, Nfs4Status::Ok);
    assert_eq!(op, 41);
    assert_eq!(op_status, Nfs4Status::Ok);
    let mut bind_cur = Cursor::new(bind_rest);
    let mut reply_session = Sessionid4::default();
    reply_session.deserialize(&mut bind_cur).unwrap();
    assert_eq!(reply_session, session.sessionid);
    let mut reply_dir = 0u32;
    reply_dir.deserialize(&mut bind_cur).unwrap();
    assert_eq!(reply_dir, 3);
    assert!(session.is_backchannel_up());

    let mut seq_in = Cursor::new(sequence_body(session.sessionid, 1));
    let mut seq_out = Vec::new();
    rt.block_on(handle_nfs4(
        2,
        compound_call(),
        &mut seq_in,
        &mut seq_out,
        &ctx,
    ))?;
    let (status, op, op_status, seq_rest) = parse_compound(&seq_out);
    assert_eq!(status, Nfs4Status::Ok);
    assert_eq!(op, 53);
    assert_eq!(op_status, Nfs4Status::Ok);
    let mut seq_cur = Cursor::new(seq_rest);
    let mut seq_session = Sessionid4::default();
    seq_session.deserialize(&mut seq_cur).unwrap();
    assert_eq!(seq_session, session.sessionid);
    let mut resp_seq = 0u32;
    resp_seq.deserialize(&mut seq_cur).unwrap();
    assert_eq!(resp_seq, 1);
    let mut slot = 0u32;
    slot.deserialize(&mut seq_cur).unwrap();
    assert_eq!(slot, 0);
    let mut highest = 0u32;
    highest.deserialize(&mut seq_cur).unwrap();
    let mut target_highest = 0u32;
    target_highest.deserialize(&mut seq_cur).unwrap();
    let mut flags = 0u32;
    flags.deserialize(&mut seq_cur).unwrap();
    assert_eq!(flags & 0x1, 0); // SEQ4_STATUS_CB_PATH_DOWN cleared

    Ok(())
}

#[test]
fn test_bind_conn_fore_or_both_falls_back_without_transport(
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
        ..Default::default()
    };

    let (v3, handler, clients, sessions) = build_handler(cluster, gateway, rt.clone())?;
    let owner = curvine_nfs::nfs4::ClientOwner4 {
        co_verifier: [2u8; 8],
        co_ownerid: b"bind-fore".to_vec(),
    };
    let (clientid, _, _) = clients.exchange_id(owner)?;
    clients.confirm_client(clientid)?;
    let session = sessions.create_session(clientid)?;
    session.set_cb_program(0x4000_0000);
    let ctx = make_context(v3, handler, None);

    let mut bind_in = Cursor::new(bind_body(session.sessionid, 3));
    let mut bind_out = Vec::new();
    rt.block_on(handle_nfs4(
        10,
        compound_call(),
        &mut bind_in,
        &mut bind_out,
        &ctx,
    ))?;
    let (status, op, op_status, bind_rest) = parse_compound(&bind_out);
    assert_eq!(status, Nfs4Status::Ok);
    assert_eq!(op, 41);
    assert_eq!(op_status, Nfs4Status::Ok);
    let mut cur = Cursor::new(bind_rest);
    let mut reply_session = Sessionid4::default();
    reply_session.deserialize(&mut cur).unwrap();
    assert_eq!(reply_session, session.sessionid);
    let mut reply_dir = 0u32;
    reply_dir.deserialize(&mut cur).unwrap();
    assert_eq!(reply_dir, 1);
    assert!(!session.is_backchannel_up());
    Ok(())
}

#[test]
fn test_bind_conn_back_requires_transport() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .build()?;
    testing.start_cluster()?;
    let cluster = testing.get_active_cluster_conf()?;
    let rt = Arc::new(cluster.client_rpc_conf().create_runtime());
    let gateway = NfsGatewayConf {
        delegation_enabled: true,
        ..Default::default()
    };

    let (v3, handler, clients, sessions) = build_handler(cluster, gateway, rt.clone())?;
    let owner = curvine_nfs::nfs4::ClientOwner4 {
        co_verifier: [3u8; 8],
        co_ownerid: b"bind-back".to_vec(),
    };
    let (clientid, _, _) = clients.exchange_id(owner)?;
    clients.confirm_client(clientid)?;
    let session = sessions.create_session(clientid)?;
    session.set_cb_program(0x4000_0000);
    let ctx = make_context(v3, handler, None);

    let mut bind_in = Cursor::new(bind_body(session.sessionid, 2));
    let mut bind_out = Vec::new();
    rt.block_on(handle_nfs4(
        11,
        compound_call(),
        &mut bind_in,
        &mut bind_out,
        &ctx,
    ))?;
    let (status, op, op_status, _) = parse_compound(&bind_out);
    assert_eq!(status, Nfs4Status::CbPathDown);
    assert_eq!(op, 41);
    assert_eq!(op_status, Nfs4Status::CbPathDown);
    assert!(!session.is_backchannel_up());
    Ok(())
}

#[test]
fn test_backchannel_ctl_matches_ganesha_op_illegal(
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
        ..Default::default()
    };

    let (v3, handler, _clients, _sessions) = build_handler(cluster, gateway, rt.clone())?;
    let ctx = make_context(v3, handler, None);

    let mut input = Cursor::new(backchannel_ctl_body(0x4000_0000));
    let mut output = Vec::new();
    rt.block_on(handle_nfs4(
        12,
        compound_call(),
        &mut input,
        &mut output,
        &ctx,
    ))?;

    let (status, op, op_status, _) = parse_compound(&output);
    assert_eq!(status, Nfs4Status::OpIllegal);
    assert_eq!(op, 10044);
    assert_eq!(op_status, Nfs4Status::OpIllegal);
    Ok(())
}
