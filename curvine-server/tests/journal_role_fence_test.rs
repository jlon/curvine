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

use curvine_config::ClusterConf;
use curvine_raft::raft::storage::AppStorage;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::Master;
use raft::StateRole;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[test]
fn follower_role_holds_metadata_writes_until_leader_is_current() {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir("journal-role-fence");

    let journal_system = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal_system.journal_loader();
    let rt = AsyncRuntime::single();
    rt.block_on(loader.role_change(StateRole::Follower))
        .unwrap();

    let fs = journal_system.fs();
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        started_tx.send(()).unwrap();
        finished_tx.send(fs.mkdir("/role-fenced", false)).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(finished_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());

    rt.block_on(loader.role_change(StateRole::Leader)).unwrap();
    finished_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    writer.join().unwrap();
}
