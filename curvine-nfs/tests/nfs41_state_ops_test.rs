use curvine_common::fs::Path;
use curvine_nfs::nfs4::error::Nfs4Status;
use curvine_nfs::nfs4::state::open::{share_access, share_deny};
use curvine_nfs::nfs4::state::LockType4;
use curvine_nfs::nfs4::types::ClientOwner4;
use curvine_nfs::nfs4::{ClientManager, OpenManager, SessionManager, Stateid4};

fn owner(id: &[u8], verifier: u8) -> ClientOwner4 {
    ClientOwner4 {
        co_verifier: [verifier; 8],
        co_ownerid: id.to_vec(),
    }
}

#[test]
fn test_destroy_clientid_matches_ganesha_busy_and_stale_semantics() {
    let clients = ClientManager::new();
    let sessions = SessionManager::new();

    assert_eq!(
        clients
            .ensure_destroyable(42, &sessions)
            .unwrap_err()
            .status,
        Nfs4Status::StaleClientid
    );

    let (clientid, _, _) = clients.exchange_id(owner(b"client-a", 1)).unwrap();
    assert!(clients.ensure_destroyable(clientid, &sessions).is_ok());

    let session = sessions.create_session(clientid).unwrap();
    assert_eq!(
        clients
            .ensure_destroyable(clientid, &sessions)
            .unwrap_err()
            .status,
        Nfs4Status::ClientidBusy
    );

    sessions.destroy_session(&session.sessionid).unwrap();
    assert!(clients.ensure_destroyable(clientid, &sessions).is_ok());
}

#[test]
fn test_open_stateid_verification_matches_ganesha_seqid_rules() {
    let opens = OpenManager::new();
    let path = Path::new("/stateid-open").unwrap();
    let (state, _) = opens
        .open(
            100,
            b"open-owner".to_vec(),
            7,
            path,
            share_access::READ,
            share_deny::NONE,
        )
        .unwrap();

    let current = Stateid4::new(state.seqid(), state.stateid.other);
    assert!(opens.verify_stateid(&current).is_ok());
    assert!(opens
        .verify_stateid(&Stateid4::new(0, state.stateid.other))
        .is_ok());

    state.next_seqid();

    assert_eq!(
        opens.verify_stateid(&current).unwrap_err().status,
        Nfs4Status::OldStateid
    );
    assert_eq!(
        opens
            .verify_stateid(&Stateid4::new(state.seqid() + 1, state.stateid.other))
            .unwrap_err()
            .status,
        Nfs4Status::BadStateid
    );
}

#[test]
fn test_lock_stateid_verification_matches_ganesha_seqid_rules() {
    let locks = curvine_nfs::nfs4::state::LockManager::new();
    let owner = b"lock-owner".to_vec();

    let stateid = locks
        .lock(
            100,
            owner.clone(),
            9,
            LockType4::WriteLt,
            0,
            64,
            false,
            None,
        )
        .unwrap();
    assert!(locks.verify_stateid(&stateid).is_ok());
    assert!(locks
        .verify_stateid(&Stateid4::new(0, stateid.other))
        .is_ok());

    let updated = locks
        .lock(
            100,
            owner,
            9,
            LockType4::WriteLt,
            128,
            64,
            false,
            Some(&stateid),
        )
        .unwrap();

    assert_eq!(
        locks.verify_stateid(&stateid).unwrap_err().status,
        Nfs4Status::OldStateid
    );
    assert_eq!(
        locks
            .verify_stateid(&Stateid4::new(updated.seqid + 1, updated.other))
            .unwrap_err()
            .status,
        Nfs4Status::BadStateid
    );
}

#[test]
fn test_free_stateid_succeeds_only_after_lock_state_is_empty() {
    let locks = curvine_nfs::nfs4::state::LockManager::new();
    let owner = b"free-stateid-owner".to_vec();

    let stateid = locks
        .lock(100, owner, 13, LockType4::WriteLt, 0, 64, false, None)
        .unwrap();

    assert_eq!(
        locks.free_stateid(&stateid).unwrap_err().status,
        Nfs4Status::LocksHeld
    );

    let emptied = locks.unlock(&stateid, 0, u64::MAX).unwrap();
    assert!(locks.get_lock_state(&emptied).is_some());
    assert!(locks.free_stateid(&emptied).is_ok());
    assert!(locks.get_lock_state(&emptied).is_none());
}
