use curvine_server::master::meta::BlockLocationLockManager;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

#[test]
fn same_shard_different_blocks_do_not_serialize() {
    let locks = Arc::new(BlockLocationLockManager::new(1, 1));
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();

    let holder_locks = locks.clone();
    let holder = thread::spawn(move || {
        let _guard = holder_locks.write_blocks(&[1001]);
        held_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });

    held_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let waiter_locks = locks.clone();
    let waiter = thread::spawn(move || {
        let _guard = waiter_locks.write_blocks(&[1002]);
        acquired_tx.send(()).unwrap();
    });

    acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    waiter.join().unwrap();
}

#[test]
fn same_block_write_locks_are_mutually_exclusive() {
    let locks = Arc::new(BlockLocationLockManager::new(1, 1));
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();

    let holder_locks = locks.clone();
    let holder = thread::spawn(move || {
        let _guard = holder_locks.write_blocks(&[1001]);
        held_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });

    held_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let waiter_locks = locks.clone();
    let waiter = thread::spawn(move || {
        let _guard = waiter_locks.write_blocks(&[1001]);
        acquired_tx.send(()).unwrap();
    });

    assert!(acquired_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    release_tx.send(()).unwrap();
    acquired_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    holder.join().unwrap();
    waiter.join().unwrap();
}

#[test]
fn same_shard_different_workers_do_not_serialize() {
    let locks = Arc::new(BlockLocationLockManager::new(1, 1));
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();

    let holder_locks = locks.clone();
    let holder = thread::spawn(move || {
        let _guard = holder_locks.write_worker_blocks(1001, &[]);
        held_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });

    held_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let waiter_locks = locks.clone();
    let waiter = thread::spawn(move || {
        let _guard = waiter_locks.write_worker_blocks(1002, &[]);
        acquired_tx.send(()).unwrap();
    });

    acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    waiter.join().unwrap();
}
