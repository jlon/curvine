// Copyright 2026 OPPO.
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

use bytes::BytesMut;
use curvine_common::fs::{Path, Reader, Writer};
use curvine_tests::Testing;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use std::sync::Arc;

#[test]
fn cached_random_writer_is_committed_before_append() {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(|conf| {
            conf.log.level = "WARN".to_string();
            conf.master.log.level = "WARN".to_string();
            conf.master.audit_log.level = "WARN".to_string();
            conf.worker.log.level = "WARN".to_string();
            conf.fuse.log.level = "WARN".to_string();
            conf.client.short_circuit = false;
            conf.client.replicas = 1;
            conf.client.block_size = 64;
            conf.client.block_size_str = "64B".to_string();
            conf.client.write_chunk_size = 64;
            conf.client.write_chunk_size_str = "64B".to_string();
            conf.client.max_cache_block_handles = 2;
        })
        .build()
        .unwrap();
    testing.start_cluster().unwrap();
    let conf = testing.get_active_cluster_conf().unwrap();

    let rt = Arc::new(AsyncRuntime::single());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf)).unwrap();

    rt.block_on(async move {
        let path = Path::from_str("/cached-writer-append.data").unwrap();
        let mut writer = fs.create(&path, true).await.unwrap();

        writer.write(&[0x5a; 128]).await.unwrap();
        writer.seek(0).await.unwrap();
        writer.seek(128).await.unwrap();
        writer.write(b"tail").await.unwrap();
        writer.complete().await.unwrap();

        let mut reader = fs.open(&path).await.unwrap();
        let mut actual = BytesMut::zeroed(132);
        assert_eq!(reader.read_full(&mut actual).await.unwrap(), 132);
        reader.complete().await.unwrap();

        assert_eq!(&actual[..128], &[0x5a; 128]);
        assert_eq!(&actual[128..], b"tail");
    });
}
