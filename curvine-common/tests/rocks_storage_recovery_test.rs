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

use curvine_common::raft::storage::RocksStorageCore;
use curvine_common::rocksdb::{DBConf, DBEngine};
use orpc::common::{FileUtils, Utils};
use prost::Message;
use raft::eraftpb::HardState;
use raft::Storage;

fn test_conf(name: &str) -> DBConf {
    DBConf::new(Utils::test_sub_dir(format!(
        "rocks-storage-recovery-{}-{}",
        name,
        Utils::rand_str(6)
    )))
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

    assert!(storage.set_hard_state(hard_state).is_err());
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
    assert!(storage.init_state().is_err());
}
