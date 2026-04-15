use std::io::Cursor;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use curvine_nfs::nfs4::backchannel::{BackchannelManager, BackchannelState, CallbackOp};
use curvine_nfs::nfs4::delegation::open4_share_access::WANT_READ_DELEG;
use curvine_nfs::nfs4::delegation::{why_no_delegation, Grant};
use curvine_nfs::nfs4::error::Nfs4Status;
use curvine_nfs::nfs4::{DelegationConfig, DelegationManager, Nfs4FileHandle, Stateid4};
use curvine_nfs::protocol::rpc::{
    accept_body, accepted_reply, auth_flavor, opaque_auth, reply_body, rpc_body, rpc_msg,
};
use curvine_nfs::protocol::xdr::XDR;
use curvine_nfs::server::wire::handle_callback_reply;
use tokio::sync::mpsc;

#[test]
fn test_delegation_stateid_verification_matches_ganesha_seqid_rules() {
    let backchannel = Arc::new(BackchannelManager::new());
    backchannel.register([7u8; 16], 55, 1234, 4);
    let config = DelegationConfig {
        enabled: true,
        ..Default::default()
    };
    let delegations = DelegationManager::with_config(backchannel.clone(), config);

    let delegation = delegations
        .try_grant(55, 11, WANT_READ_DELEG, Nfs4FileHandle::default())
        .expect("delegation should be granted in the unconstrained test case");

    assert!(delegations.verify_stateid(&delegation.stateid).is_ok());
    assert!(delegations
        .verify_stateid(&Stateid4::new(0, delegation.stateid.other))
        .is_ok());
    assert_eq!(
        delegations
            .verify_stateid(&Stateid4::new(
                delegation.stateid.seqid + 1,
                delegation.stateid.other
            ))
            .unwrap_err()
            .status,
        Nfs4Status::BadStateid
    );
}

#[test]
fn test_delegation_recall_timeout_becomes_revoked_until_free_stateid() {
    let backchannel = Arc::new(BackchannelManager::new());
    let session_id = [9u8; 16];
    backchannel.register(session_id, 55, 4321, 4);
    let config = DelegationConfig {
        enabled: true,
        recall_timeout_secs: 0,
        ..Default::default()
    };
    let delegations = DelegationManager::with_config(backchannel.clone(), config);

    let delegation = delegations
        .try_grant(55, 21, WANT_READ_DELEG, Nfs4FileHandle::default())
        .expect("delegation should be granted");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(runtime
        .block_on(delegations.check_and_recall_if_needed(77, 21, true, Nfs4FileHandle::default(),))
        .unwrap());

    let task = backchannel
        .try_recv(&session_id)
        .expect("recall callback should be queued");
    match task.op {
        CallbackOp::Recall { stateid, truncate } => {
            assert_eq!(stateid, delegation.stateid);
            assert!(!truncate);
        }
        other => panic!("unexpected callback op: {other:?}"),
    }

    thread::sleep(Duration::from_millis(1));
    let revoked = delegations.cleanup_timed_out_recalls();
    assert_eq!(revoked, vec![(21, 55)]);
    assert_eq!(
        delegations
            .verify_stateid(&delegation.stateid)
            .unwrap_err()
            .status,
        Nfs4Status::DelegRevoked
    );

    assert!(delegations.free_stateid(&delegation.stateid).is_ok());
    assert_eq!(
        delegations
            .verify_stateid(&delegation.stateid)
            .unwrap_err()
            .status,
        Nfs4Status::BadStateid
    );
}

#[test]
fn test_backchannel_register_deliver_and_mark_down() {
    let manager = BackchannelManager::new();
    let session_id = [3u8; 16];

    manager.register(session_id, 88, 9000, 8);
    assert_eq!(manager.get_state(&session_id), BackchannelState::Up);

    manager
        .recall_delegation(
            88,
            Stateid4::new(1, [4u8; 12]),
            Nfs4FileHandle::default(),
            false,
        )
        .unwrap();

    let task = manager.try_recv(&session_id).expect("queued callback");
    match task.op {
        CallbackOp::Recall { truncate, .. } => assert!(!truncate),
        other => panic!("unexpected callback op: {other:?}"),
    }

    manager.mark_down(&session_id);
    assert_eq!(manager.get_state(&session_id), BackchannelState::Down);
}

#[test]
fn test_backchannel_transport_emits_callback_rpc() {
    let manager = BackchannelManager::new();
    let session_id = [5u8; 16];
    let (tx, mut rx) = mpsc::unbounded_channel();

    manager.register(session_id, 99, 0x4000_0000, 8);
    manager
        .attach_transport(
            &session_id,
            tx,
            opaque_auth {
                flavor: auth_flavor::AUTH_NULL,
                body: Vec::new(),
            },
        )
        .unwrap();

    manager
        .recall_delegation(
            99,
            Stateid4::new(7, [8u8; 12]),
            Nfs4FileHandle::default(),
            false,
        )
        .unwrap();

    let bytes = rx
        .try_recv()
        .expect("wire callback")
        .expect("encoded callback");
    let mut cur = Cursor::new(&bytes[..]);
    let mut msg = rpc_msg::default();
    msg.deserialize(&mut cur).unwrap();

    match msg.body {
        rpc_body::CALL(call) => {
            assert_eq!(call.rpcvers, 2);
            assert_eq!(call.prog, 0x4000_0000);
            assert_eq!(call.vers, 1);
            assert_eq!(call.proc, 1);
        }
        rpc_body::REPLY(_) => panic!("expected callback call"),
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

    let mut cb_sequence = 0u32;
    cb_sequence.deserialize(&mut cur).unwrap();
    assert_eq!(cb_sequence, 11);
    let mut reply_session = [0u8; 16];
    reply_session.deserialize(&mut cur).unwrap();
    assert_eq!(reply_session, session_id);
    let mut seq = 0u32;
    seq.deserialize(&mut cur).unwrap();
    let mut slot = 0u32;
    slot.deserialize(&mut cur).unwrap();
    let mut highest = 0u32;
    highest.deserialize(&mut cur).unwrap();
    let mut cache = false;
    cache.deserialize(&mut cur).unwrap();
    let mut ref_len = 0u32;
    ref_len.deserialize(&mut cur).unwrap();

    let mut cb_recall = 0u32;
    cb_recall.deserialize(&mut cur).unwrap();
    assert_eq!(cb_recall, 4);
    let mut recalled = Stateid4::default();
    recalled.deserialize(&mut cur).unwrap();
    assert_eq!(recalled, Stateid4::new(7, [8u8; 12]));
    let mut truncate = false;
    truncate.deserialize(&mut cur).unwrap();
    assert!(!truncate);
    let mut fh = Nfs4FileHandle::default();
    fh.deserialize(&mut cur).unwrap();
    assert_eq!(fh.data, Nfs4FileHandle::default().data);
}

#[test]
fn test_backchannel_reply_reopens_slot() {
    let manager = BackchannelManager::new();
    let session_id = [6u8; 16];
    let (tx, mut rx) = mpsc::unbounded_channel();

    manager.register(session_id, 77, 0x4000_0000, 1);
    manager
        .attach_transport(
            &session_id,
            tx,
            opaque_auth {
                flavor: auth_flavor::AUTH_NULL,
                body: Vec::new(),
            },
        )
        .unwrap();

    manager
        .recall_delegation(
            77,
            Stateid4::new(1, [1u8; 12]),
            Nfs4FileHandle::default(),
            false,
        )
        .unwrap();
    assert_eq!(
        manager
            .recall_delegation(
                77,
                Stateid4::new(2, [2u8; 12]),
                Nfs4FileHandle::default(),
                false
            )
            .unwrap_err()
            .status,
        Nfs4Status::BackChanBusy
    );

    let bytes = rx
        .try_recv()
        .expect("wire callback")
        .expect("encoded callback");
    let mut cur = Cursor::new(&bytes[..]);
    let mut msg = rpc_msg::default();
    msg.deserialize(&mut cur).unwrap();
    let xid = msg.xid;

    manager.complete_reply(xid, true);
    manager
        .recall_delegation(
            77,
            Stateid4::new(3, [3u8; 12]),
            Nfs4FileHandle::default(),
            false,
        )
        .unwrap();
}

#[test]
fn test_backchannel_waits_briefly_for_slot_release() {
    let manager = Arc::new(BackchannelManager::new());
    let session_id = [12u8; 16];
    let (tx, mut rx) = mpsc::unbounded_channel();

    manager.register(session_id, 77, 0x4000_0000, 1);
    manager
        .attach_transport(
            &session_id,
            tx,
            opaque_auth {
                flavor: auth_flavor::AUTH_NULL,
                body: Vec::new(),
            },
        )
        .unwrap();

    manager
        .recall_delegation(
            77,
            Stateid4::new(1, [1u8; 12]),
            Nfs4FileHandle::default(),
            false,
        )
        .unwrap();

    let manager2 = manager.clone();
    let waiter = std::thread::spawn(move || {
        manager2.recall_delegation(
            77,
            Stateid4::new(2, [2u8; 12]),
            Nfs4FileHandle::default(),
            false,
        )
    });

    let first = rx
        .try_recv()
        .expect("first wire callback")
        .expect("encoded callback");
    let mut cur = Cursor::new(&first[..]);
    let mut msg = rpc_msg::default();
    msg.deserialize(&mut cur).unwrap();
    manager.complete_reply(msg.xid, true);

    waiter.join().unwrap().unwrap();
    assert!(rx.try_recv().is_ok());
}

#[test]
fn test_backchannel_error_reply_marks_channel_down() {
    let manager = BackchannelManager::new();
    let session_id = [7u8; 16];
    let (tx, mut rx) = mpsc::unbounded_channel();

    manager.register(session_id, 66, 0x4000_0000, 1);
    manager
        .attach_transport(
            &session_id,
            tx,
            opaque_auth {
                flavor: auth_flavor::AUTH_NULL,
                body: Vec::new(),
            },
        )
        .unwrap();

    manager
        .recall_delegation(
            66,
            Stateid4::new(1, [9u8; 12]),
            Nfs4FileHandle::default(),
            false,
        )
        .unwrap();

    let bytes = rx
        .try_recv()
        .expect("wire callback")
        .expect("encoded callback");
    let mut cur = Cursor::new(&bytes[..]);
    let mut msg = rpc_msg::default();
    msg.deserialize(&mut cur).unwrap();
    manager.complete_reply(msg.xid, false);

    assert_eq!(manager.get_state(&session_id), BackchannelState::Down);
}

#[test]
fn test_failed_compound_reply_allows_recall_retry() {
    let backchannel = Arc::new(BackchannelManager::new());
    let session_id = [8u8; 16];
    let (tx, mut rx) = mpsc::unbounded_channel();
    backchannel.register(session_id, 55, 0x4000_0000, 1);
    backchannel
        .attach_transport(
            &session_id,
            tx,
            opaque_auth {
                flavor: auth_flavor::AUTH_NULL,
                body: Vec::new(),
            },
        )
        .unwrap();

    let config = DelegationConfig {
        enabled: true,
        ..Default::default()
    };
    let delegations = DelegationManager::with_config(backchannel.clone(), config);
    let _delegation = delegations
        .try_grant(55, 31, WANT_READ_DELEG, Nfs4FileHandle::default())
        .expect("delegation should be granted");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(runtime
        .block_on(delegations.check_and_recall_if_needed(77, 31, true, Nfs4FileHandle::default(),))
        .unwrap());

    let first = rx
        .try_recv()
        .expect("wire callback")
        .expect("encoded callback");
    let mut cur = Cursor::new(&first[..]);
    let mut msg = rpc_msg::default();
    msg.deserialize(&mut cur).unwrap();
    let xid = msg.xid;

    let mut body = Vec::new();
    Nfs4Status::Delay.serialize(&mut body).unwrap();
    b"curvine-cb".to_vec().serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    let reply = reply_body::MSG_ACCEPTED(accepted_reply {
        verf: opaque_auth::default(),
        reply_data: accept_body::SUCCESS,
    });
    let mut cur = Cursor::new(&body[..]);
    handle_callback_reply(&mut cur, &backchannel, &delegations, xid, &reply).unwrap();

    assert!(runtime
        .block_on(delegations.check_and_recall_if_needed(77, 31, true, Nfs4FileHandle::default(),))
        .unwrap());
    assert!(rx.try_recv().is_ok());
}

#[test]
fn test_missing_recall_resop_allows_retry() {
    let backchannel = Arc::new(BackchannelManager::new());
    let session_id = [13u8; 16];
    let (tx, mut rx) = mpsc::unbounded_channel();
    backchannel.register(session_id, 55, 0x4000_0000, 1);
    backchannel
        .attach_transport(
            &session_id,
            tx,
            opaque_auth {
                flavor: auth_flavor::AUTH_NULL,
                body: Vec::new(),
            },
        )
        .unwrap();

    let config = DelegationConfig {
        enabled: true,
        ..Default::default()
    };
    let delegations = DelegationManager::with_config(backchannel.clone(), config);
    let delegation = delegations
        .try_grant(55, 71, WANT_READ_DELEG, Nfs4FileHandle::default())
        .expect("delegation should be granted");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(runtime
        .block_on(delegations.check_and_recall_if_needed(77, 71, true, Nfs4FileHandle::default(),))
        .unwrap());

    let first = rx
        .try_recv()
        .expect("wire callback")
        .expect("encoded callback");
    let mut cur = Cursor::new(&first[..]);
    let mut msg = rpc_msg::default();
    msg.deserialize(&mut cur).unwrap();
    let xid = msg.xid;

    let mut body = Vec::new();
    Nfs4Status::Ok.serialize(&mut body).unwrap();
    b"curvine-cb".to_vec().serialize(&mut body).unwrap();
    1u32.serialize(&mut body).unwrap();
    11u32.serialize(&mut body).unwrap();
    Nfs4Status::Ok.serialize(&mut body).unwrap();
    session_id.serialize(&mut body).unwrap();
    1u32.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    0u32.serialize(&mut body).unwrap();
    let reply = reply_body::MSG_ACCEPTED(accepted_reply {
        verf: opaque_auth::default(),
        reply_data: accept_body::SUCCESS,
    });
    let mut cur = Cursor::new(&body[..]);
    handle_callback_reply(&mut cur, &backchannel, &delegations, xid, &reply).unwrap();

    assert!(runtime
        .block_on(delegations.check_and_recall_if_needed(77, 71, true, Nfs4FileHandle::default(),))
        .unwrap());
    let second = rx
        .try_recv()
        .expect("wire callback")
        .expect("encoded callback");
    let mut cur2 = Cursor::new(&second[..]);
    let mut msg2 = rpc_msg::default();
    msg2.deserialize(&mut cur2).unwrap();
    assert_ne!(msg2.xid, msg.xid);
    assert_eq!(
        delegations
            .verify_stateid(&delegation.stateid)
            .unwrap()
            .stateid,
        delegation.stateid
    );
}

#[test]
fn test_client_revoke_history_blocks_new_delegations() {
    let backchannel = Arc::new(BackchannelManager::new());
    let session_id = [11u8; 16];
    backchannel.register(session_id, 55, 4321, 4);
    let config = DelegationConfig {
        enabled: true,
        recall_timeout_secs: 0,
        ..Default::default()
    };
    let delegations = DelegationManager::with_config(backchannel.clone(), config);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    for fileid in [51u64, 52, 53] {
        let delegation = delegations
            .try_grant(55, fileid, WANT_READ_DELEG, Nfs4FileHandle::default())
            .expect("delegation should be granted before revoke threshold");
        assert!(runtime
            .block_on(delegations.check_and_recall_if_needed(
                77,
                fileid,
                true,
                Nfs4FileHandle::default(),
            ))
            .unwrap());
        thread::sleep(Duration::from_millis(1));
        let revoked = delegations.cleanup_timed_out_recalls();
        assert_eq!(revoked, vec![(fileid, 55)]);
        assert!(delegations.free_stateid(&delegation.stateid).is_ok());
    }

    match delegations.grant_or_reason(55, 60, WANT_READ_DELEG, Nfs4FileHandle::default()) {
        Grant::Denied(why) => assert_eq!(why, why_no_delegation::WND4_RESOURCE),
        Grant::Granted(_) => panic!("delegation should be denied after repeated revokes"),
    }
}

#[test]
fn test_anonymous_operation_blocks_delegation_grant() {
    let backchannel = Arc::new(BackchannelManager::new());
    backchannel.register([14u8; 16], 55, 1234, 4);
    let config = DelegationConfig {
        enabled: true,
        ..Default::default()
    };
    let delegations = DelegationManager::with_config(backchannel, config);

    delegations.begin_anon_op(81);
    match delegations.grant_or_reason(55, 81, WANT_READ_DELEG, Nfs4FileHandle::default()) {
        Grant::Denied(why) => assert_eq!(why, why_no_delegation::WND4_CONTENTION),
        Grant::Granted(_) => panic!("delegation should be denied while anonymous I/O is active"),
    }
    delegations.end_anon_op(81);

    assert!(matches!(
        delegations.grant_or_reason(55, 81, WANT_READ_DELEG, Nfs4FileHandle::default()),
        Grant::Granted(_)
    ));
}

#[test]
fn test_malformed_success_reply_frees_slot_and_allows_retry_after_rebind() {
    let backchannel = Arc::new(BackchannelManager::new());
    let session_id = [10u8; 16];
    let (tx, mut rx) = mpsc::unbounded_channel();
    backchannel.register(session_id, 55, 0x4000_0000, 1);
    backchannel
        .attach_transport(
            &session_id,
            tx,
            opaque_auth {
                flavor: auth_flavor::AUTH_NULL,
                body: Vec::new(),
            },
        )
        .unwrap();

    let config = DelegationConfig {
        enabled: true,
        ..Default::default()
    };
    let delegations = DelegationManager::with_config(backchannel.clone(), config);
    let delegation = delegations
        .try_grant(55, 41, WANT_READ_DELEG, Nfs4FileHandle::default())
        .expect("delegation should be granted");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(runtime
        .block_on(delegations.check_and_recall_if_needed(77, 41, true, Nfs4FileHandle::default(),))
        .unwrap());

    let first = rx
        .try_recv()
        .expect("wire callback")
        .expect("encoded callback");
    let mut cur = Cursor::new(&first[..]);
    let mut msg = rpc_msg::default();
    msg.deserialize(&mut cur).unwrap();

    let reply = reply_body::MSG_ACCEPTED(accepted_reply {
        verf: opaque_auth::default(),
        reply_data: accept_body::SUCCESS,
    });
    let mut malformed = Cursor::new(&[][..]);
    handle_callback_reply(&mut malformed, &backchannel, &delegations, msg.xid, &reply).unwrap();
    assert_eq!(backchannel.get_state(&session_id), BackchannelState::Down);

    let (tx2, mut rx2) = mpsc::unbounded_channel();
    backchannel.register(session_id, 55, 0x4000_0000, 1);
    backchannel
        .attach_transport(
            &session_id,
            tx2,
            opaque_auth {
                flavor: auth_flavor::AUTH_NULL,
                body: Vec::new(),
            },
        )
        .unwrap();

    assert!(runtime
        .block_on(delegations.check_and_recall_if_needed(77, 41, true, Nfs4FileHandle::default(),))
        .unwrap());
    let second = rx2
        .try_recv()
        .expect("wire callback")
        .expect("encoded callback");
    let mut cur2 = Cursor::new(&second[..]);
    let mut msg2 = rpc_msg::default();
    msg2.deserialize(&mut cur2).unwrap();
    assert_ne!(msg2.xid, msg.xid);
    assert_eq!(
        delegations
            .verify_stateid(&delegation.stateid)
            .unwrap()
            .stateid,
        delegation.stateid
    );
}
