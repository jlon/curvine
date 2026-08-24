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
use curvine_error::FsError;
use curvine_raft::raft::RaftClient;
use curvine_server::master::journal::JournalWriter;
use curvine_server::master::Master;

#[test]
fn metadata_read_barrier_rejects_requests_until_catch_up_finishes() {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir("journal-read-barrier");

    let rt = conf.journal.create_runtime();
    let client = RaftClient::from_conf(rt, &conf.journal);
    let writer = JournalWriter::new(true, client, &conf.journal).unwrap();

    writer.ensure_metadata_current().unwrap();

    writer.begin_metadata_catch_up(10, 12);
    assert!(matches!(
        writer.ensure_metadata_current().unwrap_err(),
        FsError::NotLeaderMaster(_)
    ));

    writer.advance_metadata_applied(11);
    assert!(matches!(
        writer.ensure_metadata_current().unwrap_err(),
        FsError::NotLeaderMaster(_)
    ));

    writer.advance_metadata_applied(12);
    assert!(matches!(
        writer.ensure_metadata_current().unwrap_err(),
        FsError::NotLeaderMaster(_)
    ));

    writer.advance_metadata_catch_up(12);
    writer.ensure_metadata_current().unwrap();

    writer.begin_metadata_catch_up(5, 6);
    assert!(matches!(
        writer.ensure_metadata_current().unwrap_err(),
        FsError::NotLeaderMaster(_)
    ));
    writer.advance_metadata_catch_up(6);
    writer.ensure_metadata_current().unwrap();
}
