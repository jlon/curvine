use std::io::Cursor;
use std::sync::Arc;

use curvine_common::conf::NfsGatewayConf;
use curvine_nfs::gateway::CurvineNfsFileSystem;
use curvine_nfs::nfs4::compound::CompoundContext;
use curvine_nfs::nfs4::ops::op_open;
use curvine_nfs::nfs4::state::{
    ClientManager, LockManager, OpenManager, PersistenceConfig, StatePersistenceManager,
};
use curvine_nfs::nfs4::{CompoundHandler, Nfs4FileSystem, SessionManager, Stateid4};
use curvine_nfs::protocol::xdr::XDR;
use curvine_tests::Testing;
use orpc::runtime::RpcRuntime;

fn open_input(
    clientid: u64,
    owner: &[u8],
    share_access: u32,
    claim_type: u32,
    name: Option<&[u8]>,
    deleg_stateid: Option<Stateid4>,
) -> Vec<u8> {
    let mut input = Vec::new();
    1u32.serialize(&mut input).unwrap(); // seqid
    share_access.serialize(&mut input).unwrap();
    0u32.serialize(&mut input).unwrap(); // share_deny
    clientid.serialize(&mut input).unwrap();
    owner.to_vec().serialize(&mut input).unwrap();
    0u32.serialize(&mut input).unwrap(); // opentype = NOCREATE
    claim_type.serialize(&mut input).unwrap();
    match claim_type {
        0 => name.unwrap().to_vec().serialize(&mut input).unwrap(),
        2 => {
            deleg_stateid.unwrap().serialize(&mut input).unwrap();
            name.unwrap().to_vec().serialize(&mut input).unwrap();
        }
        5 => {
            deleg_stateid.unwrap().serialize(&mut input).unwrap();
        }
        _ => {}
    }
    input
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

#[test]
fn test_open_supports_claim_delegate_cur_and_cur_fh(
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
    let fs = Arc::new(Nfs4FileSystem::new(
        cluster.clone(),
        gateway.clone(),
        rt.clone(),
    )?);
    let persistence = Arc::new(StatePersistenceManager::new(
        fs.clone(),
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
        fs.clone(),
        persistence,
        &gateway,
    ));

    rt.block_on(async move {
        let root_id = fs.root_fileid();
        let (fileid, _) = fs.create_file(root_id, "deleg-open.txt").await?;
        let root_fh = fs.fileid_to_fh(root_id);
        let file_fh = fs.fileid_to_fh(fileid);

        let owner = curvine_nfs::nfs4::ClientOwner4 {
            co_verifier: [1u8; 8],
            co_ownerid: b"deleg-open-client".to_vec(),
        };
        let (clientid, _, _) = clients.exchange_id(owner)?;
        let session = sessions.create_session(clientid)?;
        handler
            .backchannel
            .register(session.sessionid, clientid, 1234, session.slot_count());

        let delegation = handler
            .delegations
            .try_grant(
                clientid,
                fileid,
                curvine_nfs::nfs4::delegation::open4_share_access::WANT_READ_DELEG,
                file_fh.clone(),
            )
            .expect("delegation should be granted");

        let mut by_name = CompoundContext::with_minor_version(1);
        by_name.current_fh = Some(root_fh.clone());
        let input = open_input(
            clientid,
            b"owner-by-name",
            0x01,
            2,
            Some(b"deleg-open.txt"),
            Some(delegation.stateid),
        );
        let res = op_open(&mut Cursor::new(input), &mut by_name, &handler).await?;
        assert!(!res.is_empty());
        assert_eq!(by_name.current_fh.unwrap().data, file_fh.data);

        let mut by_fh = CompoundContext::with_minor_version(1);
        by_fh.current_fh = Some(file_fh.clone());
        let input = open_input(
            clientid,
            b"owner-by-fh",
            0x01,
            5,
            None,
            Some(delegation.stateid),
        );
        let res = op_open(&mut Cursor::new(input), &mut by_fh, &handler).await?;
        assert!(!res.is_empty());
        assert_eq!(by_fh.current_fh.unwrap().data, file_fh.data);

        let mut wrong_name = CompoundContext::with_minor_version(1);
        wrong_name.current_fh = Some(root_fh.clone());
        let input = open_input(
            clientid,
            b"owner-wrong-name",
            0x01,
            2,
            Some(b"not-deleg-open.txt"),
            Some(delegation.stateid),
        );
        let err = op_open(&mut Cursor::new(input), &mut wrong_name, &handler)
            .await
            .unwrap_err();
        assert_eq!(err.status, curvine_nfs::nfs4::error::Nfs4Status::Noent);

        let mut wrong_fh = CompoundContext::with_minor_version(1);
        wrong_fh.current_fh = Some(root_fh.clone());
        let input = open_input(
            clientid,
            b"owner-wrong-fh",
            0x01,
            5,
            None,
            Some(delegation.stateid),
        );
        let err = op_open(&mut Cursor::new(input), &mut wrong_fh, &handler)
            .await
            .unwrap_err();
        assert_eq!(err.status, curvine_nfs::nfs4::error::Nfs4Status::BadStateid);

        let mut invalid_name = CompoundContext::with_minor_version(1);
        invalid_name.current_fh = Some(root_fh.clone());
        let input = open_input(
            clientid,
            b"owner-invalid-name",
            0x01,
            2,
            Some(&[0xff, 0xfe]),
            Some(delegation.stateid),
        );
        let err = op_open(&mut Cursor::new(input), &mut invalid_name, &handler)
            .await
            .unwrap_err();
        assert_eq!(err.status, curvine_nfs::nfs4::error::Nfs4Status::Badchar);

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    Ok(())
}

#[test]
fn test_open_rejects_delegated_claims_when_delegation_disabled(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .build()?;
    testing.start_cluster()?;
    let cluster = testing.get_active_cluster_conf()?;
    let rt = Arc::new(cluster.client_rpc_conf().create_runtime());
    let gateway = NfsGatewayConf {
        delegation_enabled: false,
        ..Default::default()
    };

    let (_v3, handler, clients, sessions) = build_handler(cluster, gateway, rt.clone())?;
    let owner = curvine_nfs::nfs4::ClientOwner4 {
        co_verifier: [9u8; 8],
        co_ownerid: b"deleg-disabled".to_vec(),
    };
    let (clientid, _, _) = clients.exchange_id(owner)?;
    clients.confirm_client(clientid)?;
    let _session = sessions.create_session(clientid)?;

    rt.block_on(async move {
        let root_fh = handler.fs.fileid_to_fh(handler.fs.root_fileid());
        let mut ctx = CompoundContext::with_minor_version(1);
        ctx.current_fh = Some(root_fh);
        let input = open_input(
            clientid,
            b"owner-disabled",
            0x01,
            5,
            None,
            Some(Stateid4::new(1, [7u8; 12])),
        );
        let err = op_open(&mut Cursor::new(input), &mut ctx, &handler)
            .await
            .unwrap_err();
        assert_eq!(err.status, curvine_nfs::nfs4::error::Nfs4Status::Notsupp);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    Ok(())
}

#[test]
fn test_open_rejects_delegated_claim_cur_on_v40(
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

    let (_v3, handler, clients, sessions) = build_handler(cluster, gateway, rt.clone())?;
    let owner = curvine_nfs::nfs4::ClientOwner4 {
        co_verifier: [4u8; 8],
        co_ownerid: b"deleg-v40".to_vec(),
    };
    let (clientid, _, _) = clients.exchange_id(owner)?;
    clients.confirm_client(clientid)?;
    let session = sessions.create_session(clientid)?;
    handler
        .backchannel
        .register(session.sessionid, clientid, 1234, session.slot_count());

    rt.block_on(async move {
        let root_id = handler.fs.root_fileid();
        let (fileid, _) = handler.fs.create_file(root_id, "deleg-v40.txt").await?;
        let file_fh = handler.fs.fileid_to_fh(fileid);
        let delegation = handler
            .delegations
            .try_grant(
                clientid,
                fileid,
                curvine_nfs::nfs4::delegation::open4_share_access::WANT_READ_DELEG,
                file_fh,
            )
            .expect("delegation should be granted");

        let mut ctx = CompoundContext::with_minor_version(0);
        ctx.current_fh = Some(handler.fs.fileid_to_fh(root_id));
        let input = open_input(
            clientid,
            b"owner-v40",
            0x01,
            2,
            Some(b"deleg-v40.txt"),
            Some(delegation.stateid),
        );
        let err = op_open(&mut Cursor::new(input), &mut ctx, &handler)
            .await
            .unwrap_err();
        assert_eq!(err.status, curvine_nfs::nfs4::error::Nfs4Status::Notsupp);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    Ok(())
}
