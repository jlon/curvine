// Copyright 2025 OPPO.
// NFSv4.1 Session and Slot Deep Testing
//
// Test cases aligned with NFS-Ganesha behavior

use std::sync::Arc;
use std::thread;
use std::time::Duration;

// Import from curvine-nfs
use curvine_nfs::nfs4::session::{
    Session, SessionManager, Slot, SlotAcquireResult, DEFAULT_SLOT_COUNT, MAX_SLOT_COUNT,
};
use curvine_nfs::nfs4::state::lease::LeaseManager;

/// Test 1: Slot sequence number validation
#[test]
fn test_slot_sequence_validation() {
    let slot = Slot::new(0);

    // Case 1: First request with seq=1 should succeed
    match slot.acquire(1).unwrap() {
        SlotAcquireResult::Acquired { new_sequenceid } => assert_eq!(new_sequenceid, 1),
        _ => panic!("unexpected acquire result"),
    }
    slot.release(vec![1, 2, 3]);

    // Case 2: Replay with same seq should return cached reply
    match slot.acquire(1).unwrap() {
        SlotAcquireResult::Replay { cached_reply } => assert_eq!(cached_reply, vec![1, 2, 3]),
        _ => panic!("unexpected replay result"),
    }

    // Case 3: Next seq=2 should succeed
    match slot.acquire(2).unwrap() {
        SlotAcquireResult::Acquired { new_sequenceid } => assert_eq!(new_sequenceid, 2),
        _ => panic!("unexpected acquire result"),
    }
    slot.release(vec![4, 5, 6]);

    // Case 4: Old seq=1 should fail (too old)
    assert!(slot.acquire(1).is_err());

    // Case 5: Skip seq=4 should fail (too high)
    assert!(slot.acquire(4).is_err());

    // Case 6: Correct seq=3 should succeed
    match slot.acquire(3).unwrap() {
        SlotAcquireResult::Acquired { new_sequenceid } => assert_eq!(new_sequenceid, 3),
        _ => panic!("unexpected acquire result"),
    }
}

/// Test 2: Concurrent slot access
#[test]
fn test_concurrent_slot_access() {
    let manager = Arc::new(SessionManager::new());
    let session = manager.create_session(12345).unwrap();
    let sessionid = session.sessionid;

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let mgr = manager.clone();
            let sid = sessionid;
            thread::spawn(move || {
                // Each thread uses a different slot
                let slot_id = i as u32;
                for seq in 1..=100 {
                    let result = mgr.sequence(&sid, slot_id, seq);
                    if result.is_ok() {
                        mgr.cache_reply(&sid, slot_id, vec![seq as u8]);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify all slots processed correctly
    for slot_id in 0..10 {
        let slot = session.get_slot(slot_id).unwrap();
        assert_eq!(slot.sequence(), 100);
    }
}

/// Test 3: Session trunking (multiple connections)
#[test]
fn test_session_trunking() {
    let session = Session::new([0u8; 16], 12345, 64);

    // Initial connection count
    assert_eq!(session.connection_count(), 1);

    // Add connections (simulating trunking)
    assert_eq!(session.add_connection(), 2);
    assert_eq!(session.add_connection(), 3);
    assert_eq!(session.add_connection(), 4);
    assert_eq!(session.connection_count(), 4);

    // Remove connections
    assert_eq!(session.remove_connection(), 3);
    assert_eq!(session.remove_connection(), 2);
    assert_eq!(session.connection_count(), 2);
}

/// Test 4: Session manager - create and destroy
#[test]
fn test_session_lifecycle() {
    let manager = SessionManager::new();

    // Create session
    let session1 = manager.create_session(111).unwrap();
    let session2 = manager.create_session(222).unwrap();

    // Verify sessions exist
    assert!(manager.get_session(&session1.sessionid).is_some());
    assert!(manager.get_session(&session2.sessionid).is_some());

    // Destroy session1
    manager.destroy_session(&session1.sessionid).unwrap();
    assert!(manager.get_session(&session1.sessionid).is_none());
    assert!(manager.get_session(&session2.sessionid).is_some());

    // Destroy all sessions for client 222
    manager.destroy_client_sessions(222);
    assert!(manager.get_session(&session2.sessionid).is_none());
}

/// Test 5: Lease reservation mechanism
#[test]
fn test_lease_reservation() {
    let manager = LeaseManager::new();
    let state = manager.register(12345);

    // Initial state
    assert!(manager.is_valid(12345));
    assert_eq!(state.reservation_count(), 0);

    // Reserve lease
    {
        let _guard1 = manager.reserve(12345).unwrap();
        assert_eq!(state.reservation_count(), 1);

        let _guard2 = manager.reserve(12345).unwrap();
        assert_eq!(state.reservation_count(), 2);
    }

    // After guards drop, reservations released
    assert_eq!(state.reservation_count(), 0);
}

/// Test 6: Lease expiration
#[test]
fn test_lease_expiration() {
    let manager = LeaseManager::with_lifetime(Duration::from_millis(100));
    let state = manager.register(12345);
    state.confirm();

    // Initially valid
    assert!(manager.is_valid(12345));

    // Wait for expiration
    thread::sleep(Duration::from_millis(150));

    // Now expired
    assert!(!manager.is_valid(12345));

    // Collect expired
    let expired = manager.collect_expired();
    assert_eq!(expired, vec![12345]);
}

/// Test 7: Lease reservation prevents expiration
#[test]
fn test_lease_reservation_prevents_expiration() {
    let manager = LeaseManager::with_lifetime(Duration::from_millis(100));
    let state = manager.register(12345);
    state.confirm();

    // Reserve lease
    let _guard = manager.reserve(12345).unwrap();

    // Wait past expiration time
    thread::sleep(Duration::from_millis(150));

    // Still valid because of reservation
    assert!(state.is_valid(Duration::from_millis(100)));

    // Drop guard
    drop(_guard);

    // Now should be expired (after renew from release)
    // Actually, release_and_renew() renews the lease, so it's valid again
    assert!(manager.is_valid(12345));
}

/// Test 8: Slot count validation
#[test]
fn test_slot_count() {
    // Default slot count should be 64 (aligned with NFS-Ganesha)
    assert_eq!(DEFAULT_SLOT_COUNT, 64);

    // Session with default slots
    let session = Session::new([0u8; 16], 1, DEFAULT_SLOT_COUNT);
    assert_eq!(session.slot_count(), 64);
    assert_eq!(session.highest_slot(), 63);

    // Session with custom slots (capped at MAX)
    let session2 = Session::new([0u8; 16], 2, 2000);
    assert_eq!(session2.slot_count(), MAX_SLOT_COUNT);
}

/// Test 9: SEQUENCE replay detection
#[test]
fn test_sequence_replay_detection() {
    let manager = SessionManager::new();
    let session = manager.create_session(12345).unwrap();
    let sid = session.sessionid;

    // First SEQUENCE
    manager.sequence(&sid, 0, 1).unwrap();

    // Cache reply
    manager.cache_reply(&sid, 0, vec![0xAB, 0xCD]);

    // Replay same sequence - should return cached
    assert!(manager.sequence(&sid, 0, 1).is_err());
    assert_eq!(manager.replay_reply(&sid, 0, 1), Some(vec![0xAB, 0xCD]));

    // Next sequence should work
    manager.sequence(&sid, 0, 2).unwrap();
}

/// Test 10: Multiple clients with separate sessions
#[test]
fn test_multiple_clients() {
    let manager = SessionManager::new();

    // Create sessions for different clients
    let s1 = manager.create_session(111).unwrap();
    let s2 = manager.create_session(222).unwrap();
    let s3 = manager.create_session(111).unwrap(); // Same client, different session

    // Each session is independent
    assert_ne!(s1.sessionid, s2.sessionid);
    assert_ne!(s1.sessionid, s3.sessionid);
    assert_ne!(s2.sessionid, s3.sessionid);

    // Destroy all sessions for client 111
    manager.destroy_client_sessions(111);

    // Client 111's sessions gone
    assert!(manager.get_session(&s1.sessionid).is_none());
    assert!(manager.get_session(&s3.sessionid).is_none());

    // Client 222's session still exists
    assert!(manager.get_session(&s2.sessionid).is_some());
}
