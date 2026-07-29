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

use curvine_raft::raft::storage::RocksStorageCore;
use curvine_raft::rocksdb::{DBConf, DBEngine, RocksUtils};
use curvine_runtime::common::{FileUtils, Utils};
use prost::Message;
use raft::eraftpb::{Entry, HardState, Snapshot};

fn test_conf(name: &str) -> DBConf {
    DBConf::new(Utils::test_sub_dir(format!(
        "rocks-storage-recovery-{}-{}",
        name,
        Utils::rand_str(6)
    )))
}

fn assert_reseed_required(error: impl ToString, commit: u64, lower: u64, upper: u64) {
    let message = error.to_string();
    assert!(
        message.contains(&format!("hard_state.commit={commit}")),
        "missing commit in error: {message}"
    );
    assert!(
        message.contains(&format!("[{lower}, {upper}]")),
        "missing durable range in error: {message}"
    );
    assert!(
        message.contains("Restore a consistent master meta+journal pair from a healthy voter"),
        "missing operator hint in error: {message}"
    );
}

#[test]
fn hard_state_cannot_commit_past_the_durable_log_tail() {
    let conf = test_conf("commit-tail");
    let mut storage = RocksStorageCore::new(conf, true);
    let hard_state = HardState {
        term: 7,
        vote: 1,
        commit: 42,
    };

    let error = storage.set_hard_state(hard_state).unwrap_err();
    assert_reseed_required(error, 42, 0, 0);
}

#[test]
fn startup_rejects_a_hard_state_beyond_the_durable_log_tail() {
    let conf = test_conf("startup");
    FileUtils::delete_path(&conf.base_dir, true).unwrap();

    {
        let db = DBEngine::new(
            conf.clone()
                .add_cf(RocksStorageCore::CF_ENTRIES)
                .add_cf(RocksStorageCore::CF_META),
            true,
        )
        .unwrap();
        let hard_state = HardState {
            term: 7,
            vote: 1,
            commit: 42,
        };
        db.put_cf(
            RocksStorageCore::CF_META,
            RocksStorageCore::STATE_KEY,
            hard_state.encode_to_vec(),
        )
        .unwrap();
    }

    let mut storage = RocksStorageCore::new(conf, false);
    let error = storage.init_state().unwrap_err();
    assert_reseed_required(error, 42, 0, 0);
}

#[test]
fn startup_rejects_a_hard_state_below_the_compacted_prefix() {
    let conf = test_conf("startup-lower-bound");
    FileUtils::delete_path(&conf.base_dir, true).unwrap();

    {
        let db = DBEngine::new(
            conf.clone()
                .add_cf(RocksStorageCore::CF_ENTRIES)
                .add_cf(RocksStorageCore::CF_META),
            true,
        )
        .unwrap();
        let hard_state = HardState {
            term: 7,
            vote: 1,
            commit: 99,
        };
        db.put_cf(
            RocksStorageCore::CF_META,
            RocksStorageCore::INDEX_KEY,
            RocksUtils::u64_u64_to_bytes(101, 110),
        )
        .unwrap();
        db.put_cf(
            RocksStorageCore::CF_META,
            RocksStorageCore::STATE_KEY,
            hard_state.encode_to_vec(),
        )
        .unwrap();
    }

    let mut storage = RocksStorageCore::new(conf, false);
    let error = storage.init_state().unwrap_err();
    assert_reseed_required(error, 99, 100, 110);
}

#[test]
fn startup_accepts_a_hard_state_at_the_snapshot_index_without_entries() {
    let conf = test_conf("startup-snapshot-only");
    FileUtils::delete_path(&conf.base_dir, true).unwrap();

    {
        let db = DBEngine::new(
            conf.clone()
                .add_cf(RocksStorageCore::CF_ENTRIES)
                .add_cf(RocksStorageCore::CF_META),
            true,
        )
        .unwrap();
        let hard_state = HardState {
            term: 7,
            vote: 1,
            commit: 100,
        };
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 100;
        snapshot.mut_metadata().term = 7;
        db.put_cf(
            RocksStorageCore::CF_META,
            RocksStorageCore::SNAP_KEY,
            snapshot.encode_to_vec(),
        )
        .unwrap();
        db.put_cf(
            RocksStorageCore::CF_META,
            RocksStorageCore::STATE_KEY,
            hard_state.encode_to_vec(),
        )
        .unwrap();
    }

    let mut storage = RocksStorageCore::new(conf, false);
    assert!(storage.init_state().is_ok());
}

#[test]
fn apply_snapshot_persists_the_matching_hard_state_commit() {
    let conf = test_conf("apply-snapshot-hard-state");
    FileUtils::delete_path(&conf.base_dir, true).unwrap();

    {
        let mut storage = RocksStorageCore::new(conf.clone(), true);
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 100;
        snapshot.mut_metadata().term = 7;
        storage.apply_snapshot(snapshot).unwrap();
    }

    let mut storage = RocksStorageCore::new(conf, false);
    let state = storage.init_state().unwrap();
    assert_eq!(state.hard_state.commit, 100);
}

#[test]
fn apply_snapshot_compacts_the_durable_log_range() {
    let conf = test_conf("apply-snapshot-compacts-range");
    FileUtils::delete_path(&conf.base_dir, true).unwrap();

    {
        let mut storage = RocksStorageCore::new(conf.clone(), true);
        let entries: Vec<_> = (1..=3)
            .map(|index| Entry {
                term: 7,
                index,
                ..Default::default()
            })
            .collect();
        storage.append(&entries).unwrap();

        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 3;
        snapshot.mut_metadata().term = 7;
        storage.apply_snapshot(snapshot).unwrap();
    }

    let mut storage = RocksStorageCore::new(conf, false);
    let state = storage.init_state().unwrap();
    assert_eq!(state.hard_state.commit, 3);
    assert_eq!(storage.first_index(), 4);
    assert_eq!(storage.last_index(), 3);
}
