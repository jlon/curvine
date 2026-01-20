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

use curvine_client::block::{BlockWriter, BlockWriterRemote};
use curvine_client::file::CurvineFileSystem;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Reader, Writer};
use curvine_common::state::{CreateFileOptsBuilder, OpenFlags};
use curvine_server::test::MiniCluster;
use curvine_tests::Testing;
use log::{info, warn};
use orpc::common::LogConf;
use orpc::runtime::RpcRuntime;
use orpc::sys::DataSlice;
use orpc::CommonResult;
use std::env;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::time::{sleep, Duration};

fn init_logger() {
    static LOGGER: OnceLock<()> = OnceLock::new();
    LOGGER.get_or_init(|| {
        orpc::common::Logger::init(LogConf::default());
    });
}

#[test]
fn test_pipeline_single_write_debug() -> CommonResult<()> {
    init_logger();

    info!("=== Starting single write debug test ===");
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
        })
        .build()?;
    testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let file_path = Path::from("/test_debug_single.txt");
        let data = vec![42u8; 100 * 1024];

        info!("Creating file with 3 replicas");
        let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
            .create_parent(true)
            .replicas(3)
            .build();

        let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
        info!("Writer created");

        writer.write(&data).await?;
        info!("Data written");

        writer.complete().await?;
        info!("Write completed");

        let status = fs.get_status(&file_path).await?;
        info!("File status: len={}", status.len);
        assert_eq!(status.len, data.len() as i64);

        let block_locs = fs.get_block_locations(&file_path).await?;
        info!("Block locations: {} blocks", block_locs.block_locs.len());
        for (i, bloc) in block_locs.block_locs.iter().enumerate() {
            info!(
                "  Block {}: id={}, {} replicas",
                i,
                bloc.block.id,
                bloc.locs.len()
            );
            assert_eq!(bloc.locs.len(), 3, "Should have 3 replicas");
        }

        info!("Reading file");
        let mut reader = fs.open(&file_path).await?;
        let mut read_data = vec![0u8; data.len()];
        let bytes_read = reader.read_full(&mut read_data).await?;
        reader.complete().await?;
        info!("Read {} bytes", bytes_read);

        assert_eq!(bytes_read, data.len());
        assert_eq!(read_data, data);

        fs.delete(&file_path, false).await?;
        info!("=== Single write debug test passed ===");
        Ok(())
    })
}

#[test]
fn test_pipeline_replication_basic() -> CommonResult<()> {
    init_logger();

    info!("Starting Pipeline replication basic test");
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
        })
        .build()?;
    testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        test_single_block_write(&fs).await?;
        test_multiple_blocks_write(&fs).await?;
        test_large_file_write(&fs).await?;
        test_concurrent_writes(&fs).await?;

        info!("Pipeline replication basic test completed successfully");
        Ok(())
    })
}

#[test]
fn test_pipeline_min_replicas_enforced() -> CommonResult<()> {
    init_logger();

    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
            conf.master.min_replication = 3;
        })
        .build()?;
    let cluster = testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let file_path = Path::from("/test_pipeline_min_replicas.txt");
        let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
            .create_parent(true)
            .replicas(3)
            .build();

        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(true);
        let fs_client = fs.fs_client();
        let fs_context = fs.fs_context();
        let _ = fs_client
            .open_with_opts(&file_path, opts.clone(), flags)
            .await?;
        let located = fs_client.add_block(&file_path, vec![], 0, None).await?;

        let middle_id = located
            .locs
            .get(1)
            .ok_or_else(|| FsError::common("Missing block locations for middle worker"))?
            .worker_id;
        let tail_id = located
            .locs
            .get(2)
            .ok_or_else(|| FsError::common("Missing block locations for tail worker"))?
            .worker_id;
        cluster.stop_worker_by_id(middle_id)?;
        cluster.stop_worker_by_id(tail_id)?;
        sleep(Duration::from_millis(300)).await;

        let writer = BlockWriter::new(fs_context.clone(), located.clone(), 0).await;
        assert!(
            writer.is_err(),
            "min_replicas should reject degraded pipeline"
        );

        let _ = fs.delete(&file_path, false).await;
        Ok(())
    })
}

#[test]
fn test_pipeline_established_degrade_min1() -> CommonResult<()> {
    init_logger();

    let testing = Testing::builder()
        .workers(1)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
            conf.master.min_replication = 1;
        })
        .build()?;
    testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let file_path = Path::from("/test_pipeline_established_degrade_min1.txt");
        let data = vec![7u8; 128 * 1024];

        let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
            .create_parent(true)
            .replicas(3)
            .build();

        let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
        writer.write(&data).await?;
        writer.complete().await?;

        let block_locs = fs.get_block_locations(&file_path).await?;
        for bloc in &block_locs.block_locs {
            assert_eq!(bloc.locs.len(), 1, "Expected degrade to 1 replica");
        }

        let _ = fs.delete(&file_path, false).await;
        Ok(())
    })
}

#[test]
fn test_pipeline_established_zero_min1_fail() -> CommonResult<()> {
    init_logger();

    let testing = Testing::builder()
        .workers(1)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
            conf.master.min_replication = 1;
        })
        .build()?;
    let cluster = testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let file_path = Path::from("/test_pipeline_established_zero_min1.txt");
        let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
            .create_parent(true)
            .replicas(3)
            .build();
        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(true);
        let fs_client = fs.fs_client();
        let _ = fs_client
            .open_with_opts(&file_path, opts.clone(), flags)
            .await?;
        let worker_id = fs
            .get_master_info()
            .await?
            .live_workers
            .first()
            .ok_or_else(|| FsError::common("Missing live worker"))?
            .worker_id();
        cluster.stop_worker_by_id(worker_id)?;
        sleep(Duration::from_millis(300)).await;

        let add_result = fs_client.add_block(&file_path, vec![], 0, None).await;
        assert!(
            add_result.is_err(),
            "Expected add_block to fail with 0 workers"
        );

        let _ = fs.delete(&file_path, false).await;
        Ok(())
    })
}

#[test]
fn test_pipeline_established_one_min2_fail() -> CommonResult<()> {
    init_logger();

    let testing = Testing::builder()
        .workers(1)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
            conf.master.min_replication = 2;
        })
        .build()?;
    testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let file_path = Path::from("/test_pipeline_established_one_min2.txt");
        let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
            .create_parent(true)
            .replicas(3)
            .build();
        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(true);
        let fs_client = fs.fs_client();
        let fs_context = fs.fs_context();
        let _ = fs_client
            .open_with_opts(&file_path, opts.clone(), flags)
            .await?;
        let located = fs_client.add_block(&file_path, vec![], 0, None).await?;

        let writer = BlockWriter::new(fs_context.clone(), located.clone(), 0).await;
        assert!(writer.is_err(), "min_replicas=2 should reject 1 replica");

        let _ = fs.delete(&file_path, false).await;
        Ok(())
    })
}

#[test]
fn test_pipeline_min_replicas_strict_write_fail() -> CommonResult<()> {
    init_logger();

    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
            conf.master.min_replication = 3;
        })
        .build()?;
    let cluster = testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let file_path = Path::from("/test_pipeline_min_replicas_strict_write.txt");
        let data = vec![13u8; 256 * 1024];
        let chunk_size = 64 * 1024;

        let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
            .create_parent(true)
            .replicas(3)
            .build();

        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(true);
        let fs_client = fs.fs_client();
        let fs_context = fs.fs_context();
        let _ = fs_client
            .open_with_opts(&file_path, opts.clone(), flags)
            .await?;
        let located = fs_client.add_block(&file_path, vec![], 0, None).await?;

        let mut writer = BlockWriter::new(fs_context.clone(), located.clone(), 0).await?;
        writer
            .write(DataSlice::mem_slice(&data[..chunk_size]))
            .await?;
        writer.flush().await?;

        let tail_id = located
            .locs
            .last()
            .ok_or_else(|| FsError::common("Missing block locations for tail worker"))?
            .worker_id;
        cluster.stop_worker_by_id(tail_id)?;
        sleep(Duration::from_millis(300)).await;

        let write_result = writer
            .write(DataSlice::mem_slice(&data[chunk_size..]))
            .await;
        assert!(
            write_result.is_err(),
            "min_replicas=3 should fail after replica loss"
        );
        if let Err(e) = write_result {
            assert!(
                matches!(e, FsError::MinReplicasNotMet(_)),
                "expected MinReplicasNotMet, got {:?}",
                e
            );
        }

        let _ = writer.cancel().await;
        let _ = fs.delete(&file_path, false).await;
        Ok(())
    })
}

async fn test_single_block_write(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("Testing single block write with 3 replicas");

    let file_path = Path::from("/test_pipeline_single_block.txt");
    let data = vec![0u8; 512 * 1024];

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
    writer.write(&data).await?;
    writer.complete().await?;

    let status = fs.get_status(&file_path).await?;
    assert_eq!(status.len, data.len() as i64);

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; data.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, data.len());
    assert_eq!(read_data, data);

    fs.delete(&file_path, false).await?;
    info!("Single block write test passed");
    Ok(())
}

async fn test_multiple_blocks_write(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("Testing multiple blocks write with 3 replicas");

    let file_path = Path::from("/test_pipeline_multiple_blocks.txt");
    let data = vec![1u8; 2 * 1024 * 1024];

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
    writer.write(&data).await?;
    writer.complete().await?;

    let status = fs.get_status(&file_path).await?;
    assert_eq!(status.len, data.len() as i64);

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; data.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, data.len());
    assert_eq!(read_data, data);

    fs.delete(&file_path, false).await?;
    info!("Multiple blocks write test passed");
    Ok(())
}

async fn test_large_file_write(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("Testing large file write with 3 replicas");

    let file_path = Path::from("/test_pipeline_large_file.txt");
    let data = vec![2u8; 10 * 1024 * 1024];

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
    writer.write(&data).await?;
    writer.complete().await?;

    let status = fs.get_status(&file_path).await?;
    assert_eq!(status.len, data.len() as i64);

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; data.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, data.len());
    assert_eq!(read_data, data);

    fs.delete(&file_path, false).await?;
    info!("Large file write test passed");
    Ok(())
}

async fn test_concurrent_writes(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("Testing concurrent writes with 3 replicas");

    let file_count = 5;
    let mut handles = Vec::new();

    for i in 0..file_count {
        let fs_clone = fs.clone();
        let handle = tokio::spawn(async move {
            let file_path = Path::from(format!("/test_pipeline_concurrent_{}.txt", i).as_str());
            let data = vec![i as u8; 1024 * 1024];

            let opts = CreateFileOptsBuilder::with_conf(&fs_clone.conf().client)
                .create_parent(true)
                .replicas(3)
                .build();

            let mut writer = fs_clone.create_with_opts(&file_path, opts, true).await?;
            writer.write(&data).await?;
            writer.complete().await?;

            let status = fs_clone.get_status(&file_path).await?;
            assert_eq!(status.len, data.len() as i64);

            let mut reader = fs_clone.open(&file_path).await?;
            let mut read_data = vec![0u8; data.len()];
            let bytes_read = reader.read_full(&mut read_data).await?;
            reader.complete().await?;
            assert_eq!(bytes_read, data.len());
            assert_eq!(read_data, data);

            fs_clone.delete(&file_path, false).await?;
            Ok::<(), FsError>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap()?;
    }

    info!("Concurrent writes test passed");
    Ok(())
}

#[test]
fn test_pipeline_replication_edge_cases() -> CommonResult<()> {
    init_logger();

    info!("Starting Pipeline replication edge cases test");
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
        })
        .build()?;
    testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        test_empty_file(&fs).await?;
        test_single_byte_write(&fs).await?;
        test_exact_block_size(&fs).await?;
        test_block_boundary(&fs).await?;
        test_small_chunk_writes(&fs).await?;
        test_flush_mid_write(&fs).await?;
        test_random_write(&fs).await?;

        info!("Pipeline replication edge cases test completed successfully");
        Ok(())
    })
}

async fn test_empty_file(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("Testing empty file with 3 replicas");

    let file_path = Path::from("/test_pipeline_empty.txt");

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
    writer.complete().await?;

    let status = fs.get_status(&file_path).await?;
    assert_eq!(status.len, 0);

    fs.delete(&file_path, false).await?;
    info!("Empty file test passed");
    Ok(())
}

async fn test_single_byte_write(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("Testing single byte write with 3 replicas");

    let file_path = Path::from("/test_pipeline_single_byte.txt");
    let data = vec![42u8; 1];

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
    writer.write(&data).await?;
    writer.complete().await?;

    let status = fs.get_status(&file_path).await?;
    assert_eq!(status.len, 1);

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; 1];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, 1);
    assert_eq!(read_data[0], 42);

    fs.delete(&file_path, false).await?;
    info!("Single byte write test passed");
    Ok(())
}

async fn test_exact_block_size(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("Testing exact block size write with 3 replicas");

    let file_path = Path::from("/test_pipeline_exact_block.txt");
    let data = vec![3u8; 1024 * 1024];

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
    writer.write(&data).await?;
    writer.complete().await?;

    let status = fs.get_status(&file_path).await?;
    assert_eq!(status.len, data.len() as i64);

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; data.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, data.len());
    assert_eq!(read_data, data);

    fs.delete(&file_path, false).await?;
    info!("Exact block size write test passed");
    Ok(())
}

async fn test_block_boundary(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("Testing block boundary write with 3 replicas");

    let file_path = Path::from("/test_pipeline_boundary.txt");
    let data = vec![4u8; 1024 * 1024 + 1];

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
    writer.write(&data).await?;
    writer.complete().await?;

    let status = fs.get_status(&file_path).await?;
    assert_eq!(status.len, data.len() as i64);

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; data.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, data.len());
    assert_eq!(read_data, data);

    fs.delete(&file_path, false).await?;
    info!("Block boundary write test passed");
    Ok(())
}

async fn test_small_chunk_writes(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("Testing small chunk writes with 3 replicas");

    let file_path = Path::from("/test_pipeline_small_chunks.txt");
    let data = vec![7u8; 128 * 1024];
    let chunk_size = 1024;

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
    let mut offset = 0;
    while offset < data.len() {
        let end = (offset + chunk_size).min(data.len());
        writer.write(&data[offset..end]).await?;
        offset = end;
    }
    writer.complete().await?;

    let status = fs.get_status(&file_path).await?;
    assert_eq!(status.len, data.len() as i64);

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; data.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, data.len());
    assert_eq!(read_data, data);

    fs.delete(&file_path, false).await?;
    info!("Small chunk writes test passed");
    Ok(())
}

async fn test_flush_mid_write(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("Testing flush mid write with 3 replicas");

    let file_path = Path::from("/test_pipeline_flush_mid.txt");
    let data = vec![9u8; 256 * 1024];
    let mid = data.len() / 2;

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
    writer.write(&data[..mid]).await?;
    writer.flush().await?;
    writer.write(&data[mid..]).await?;
    writer.flush().await?;
    writer.complete().await?;

    let status = fs.get_status(&file_path).await?;
    assert_eq!(status.len, data.len() as i64);

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; data.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, data.len());
    assert_eq!(read_data, data);

    fs.delete(&file_path, false).await?;
    info!("Flush mid write test passed");
    Ok(())
}

async fn test_random_write(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("Testing random write with 3 replicas");

    let file_path = Path::from("/test_pipeline_random_write.txt");
    let total_size = 256 * 1024;
    let base = vec![0x11u8; total_size];
    let patch1_offset = 64 * 1024;
    let patch1_len = 16 * 1024;
    let patch2_offset = 128 * 1024;
    let patch2_len = 8 * 1024;
    let patch1 = vec![0x22u8; patch1_len];
    let patch2 = vec![0x33u8; patch2_len];

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
    writer.write(&base).await?;
    writer.seek(patch1_offset as i64).await?;
    writer.write(&patch1).await?;
    writer.seek(patch2_offset as i64).await?;
    writer.write(&patch2).await?;
    writer.complete().await?;

    let mut expected = base;
    expected[patch1_offset..patch1_offset + patch1_len].copy_from_slice(&patch1);
    expected[patch2_offset..patch2_offset + patch2_len].copy_from_slice(&patch2);

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; expected.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, expected.len());
    assert_eq!(read_data, expected);

    fs.delete(&file_path, false).await?;
    info!("Random write test passed");
    Ok(())
}

#[test]
fn test_pipeline_failure_scenarios() -> CommonResult<()> {
    init_logger();

    info!("Starting Pipeline failure scenarios test");
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
        })
        .build()?;
    testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        test_pipeline_with_different_replica_counts(&fs).await?;
        test_pipeline_data_integrity(&fs).await?;
        test_pipeline_concurrent_stress(&fs).await?;
        test_pipeline_parallel_pressure(&fs).await?;
        info!("Pipeline failure scenarios test completed successfully");
        Ok(())
    })
}

async fn test_pipeline_with_different_replica_counts(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Testing pipeline with different replica counts ===");

    for replicas in [1, 2, 3] {
        info!("Testing with {} replicas", replicas);

        let file_path = Path::from(format!("/test_pipeline_replicas_{}.txt", replicas).as_str());
        let data = vec![replicas as u8; 512 * 1024];

        let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
            .create_parent(true)
            .replicas(replicas)
            .build();

        let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
        writer.write(&data).await?;
        writer.complete().await?;

        let status = fs.get_status(&file_path).await?;
        assert_eq!(status.len, data.len() as i64);

        let block_locs = fs.get_block_locations(&file_path).await?;
        for bloc in &block_locs.block_locs {
            info!("  Block {}: {} replicas", bloc.block.id, bloc.locs.len());
            assert_eq!(
                bloc.locs.len(),
                replicas as usize,
                "Block should have exactly {} replicas",
                replicas
            );
        }

        let mut reader = fs.open(&file_path).await?;
        let mut read_data = vec![0u8; data.len()];
        let bytes_read = reader.read_full(&mut read_data).await?;
        reader.complete().await?;
        assert_eq!(bytes_read, data.len());
        assert_eq!(read_data, data);

        fs.delete(&file_path, false).await?;
        info!("Test with {} replicas passed", replicas);
    }

    Ok(())
}

async fn test_pipeline_data_integrity(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Testing pipeline data integrity with various patterns ===");

    let test_cases = vec![
        ("zeros", vec![0u8; 1024 * 1024]),
        ("ones", vec![1u8; 1024 * 1024]),
        (
            "sequential",
            (0..=255).cycle().take(1024 * 1024).collect::<Vec<u8>>(),
        ),
        (
            "random_pattern",
            (0..1024 * 1024)
                .map(|i| ((i * 7 + 13) % 256) as u8)
                .collect::<Vec<u8>>(),
        ),
    ];

    for (name, data) in test_cases {
        info!("Testing data pattern: {}", name);

        let file_path = Path::from(format!("/test_pipeline_integrity_{}.txt", name).as_str());

        let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
            .create_parent(true)
            .replicas(3)
            .build();

        let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
        writer.write(&data).await?;
        writer.complete().await?;

        let mut reader = fs.open(&file_path).await?;
        let mut read_data = vec![0u8; data.len()];
        let bytes_read = reader.read_full(&mut read_data).await?;
        reader.complete().await?;
        assert_eq!(bytes_read, data.len());
        assert_eq!(
            read_data, data,
            "Data integrity check failed for pattern: {}",
            name
        );

        fs.delete(&file_path, false).await?;
        info!("Data integrity test for {} passed", name);
    }

    Ok(())
}

async fn test_pipeline_concurrent_stress(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Testing concurrent pipeline writes (stress test) ===");

    let file_count = 10;
    let mut handles = Vec::new();

    for i in 0..file_count {
        let fs_clone = fs.clone();
        let handle = tokio::spawn(async move {
            let file_path = Path::from(format!("/test_pipeline_stress_{}.txt", i).as_str());
            let data = vec![i as u8; 1024 * 1024];

            let opts = CreateFileOptsBuilder::with_conf(&fs_clone.conf().client)
                .create_parent(true)
                .replicas(3)
                .build();

            let mut writer = fs_clone.create_with_opts(&file_path, opts, true).await?;

            let chunk_size = 256 * 1024;
            for chunk_idx in 0..(data.len() / chunk_size) {
                let start = chunk_idx * chunk_size;
                let end = (start + chunk_size).min(data.len());
                let chunk = &data[start..end];
                writer.write(chunk).await?;
            }

            writer.complete().await?;

            let status = fs_clone.get_status(&file_path).await?;
            assert_eq!(status.len, data.len() as i64);

            let mut reader = fs_clone.open(&file_path).await?;
            let mut read_data = vec![0u8; data.len()];
            let bytes_read = reader.read_full(&mut read_data).await?;
            reader.complete().await?;
            assert_eq!(bytes_read, data.len());
            assert_eq!(read_data, data);

            fs_clone.delete(&file_path, false).await?;
            Ok::<(), FsError>(())
        });
        handles.push(handle);
    }

    let mut success_count = 0;
    let mut failure_count = 0;

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {
                success_count += 1;
            }
            Ok(Err(e)) => {
                warn!("Concurrent write failed: {}", e);
                failure_count += 1;
            }
            Err(e) => {
                warn!("Task panicked: {}", e);
                failure_count += 1;
            }
        }
    }

    info!(
        "Concurrent stress test: {} succeeded, {} failed",
        success_count, failure_count
    );
    assert_eq!(
        success_count, file_count,
        "All concurrent writes should succeed"
    );
    assert_eq!(failure_count, 0, "No failures should occur");

    info!("Concurrent stress test passed");
    Ok(())
}

async fn test_pipeline_parallel_pressure(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Testing parallel pressure writes ===");

    let file_count = 20;
    let data_size = 512 * 1024;
    let mut handles = Vec::new();

    for i in 0..file_count {
        let fs_clone = fs.clone();
        let handle = tokio::spawn(async move {
            let file_path = Path::from(format!("/test_pipeline_pressure_{}.txt", i).as_str());
            let data = vec![i as u8; data_size];

            let opts = CreateFileOptsBuilder::with_conf(&fs_clone.conf().client)
                .create_parent(true)
                .replicas(3)
                .build();

            let mut writer = fs_clone.create_with_opts(&file_path, opts, true).await?;
            let chunk_size = 32 * 1024;
            let mut offset = 0;
            while offset < data.len() {
                let end = (offset + chunk_size).min(data.len());
                writer.write(&data[offset..end]).await?;
                offset = end;
            }
            writer.complete().await?;

            let status = fs_clone.get_status(&file_path).await?;
            assert_eq!(status.len, data.len() as i64);

            let mut reader = fs_clone.open(&file_path).await?;
            let mut read_data = vec![0u8; data.len()];
            let bytes_read = reader.read_full(&mut read_data).await?;
            reader.complete().await?;
            assert_eq!(bytes_read, data.len());
            assert_eq!(read_data, data);

            fs_clone.delete(&file_path, false).await?;
            Ok::<(), FsError>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap()?;
    }

    info!("Parallel pressure test passed");
    Ok(())
}

async fn test_pipeline_tail_worker_down(
    fs: &CurvineFileSystem,
    cluster: Arc<MiniCluster>,
) -> CommonResult<()> {
    info!("=== Testing tail worker down during write ===");

    let file_path = Path::from("/test_pipeline_tail_down.txt");
    let data = vec![10u8; 256 * 1024];
    let chunk_size = 64 * 1024;

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let flags = OpenFlags::new_write_only()
        .set_create(true)
        .set_overwrite(true);
    let fs_client = fs.fs_client();
    let fs_context = fs.fs_context();
    let _ = fs_client
        .open_with_opts(&file_path, opts.clone(), flags)
        .await?;
    let located = fs_client.add_block(&file_path, vec![], 0, None).await?;

    let mut writer = BlockWriter::new(fs_context.clone(), located.clone(), 0).await?;
    writer
        .write(DataSlice::mem_slice(&data[..chunk_size]))
        .await?;
    writer.flush().await?;

    let tail_id = located
        .locs
        .last()
        .ok_or_else(|| FsError::common("Missing block locations for tail worker"))?
        .worker_id;
    cluster.stop_worker_by_id(tail_id)?;
    sleep(Duration::from_millis(300)).await;

    writer
        .write(DataSlice::mem_slice(&data[chunk_size..]))
        .await?;
    let commit = writer.complete().await?;
    fs_client
        .complete_file(&file_path, data.len() as i64, vec![commit], false)
        .await?;

    let block_locs = fs.get_block_locations(&file_path).await?;
    for bloc in &block_locs.block_locs {
        assert_eq!(bloc.locs.len(), 2, "Tail down should degrade to 2 replicas");
    }

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; data.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, data.len());
    assert_eq!(read_data, data);

    let _ = fs.delete(&file_path, false).await;
    info!("Tail worker down test passed");
    Ok(())
}

async fn test_pipeline_middle_worker_down(
    fs: &CurvineFileSystem,
    cluster: Arc<MiniCluster>,
) -> CommonResult<()> {
    info!("=== Testing middle worker down during write ===");

    let file_path = Path::from("/test_pipeline_middle_down.txt");
    let data = vec![11u8; 256 * 1024];
    let chunk_size = 64 * 1024;

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let flags = OpenFlags::new_write_only()
        .set_create(true)
        .set_overwrite(true);
    let fs_client = fs.fs_client();
    let fs_context = fs.fs_context();
    let _ = fs_client
        .open_with_opts(&file_path, opts.clone(), flags)
        .await?;
    let located = fs_client.add_block(&file_path, vec![], 0, None).await?;

    let mut writer = BlockWriter::new(fs_context.clone(), located.clone(), 0).await?;
    writer
        .write(DataSlice::mem_slice(&data[..chunk_size]))
        .await?;
    writer.flush().await?;

    let middle_id = located
        .locs
        .get(1)
        .ok_or_else(|| FsError::common("Missing block locations for middle worker"))?
        .worker_id;
    cluster.stop_worker_by_id(middle_id)?;
    sleep(Duration::from_millis(300)).await;

    writer
        .write(DataSlice::mem_slice(&data[chunk_size..]))
        .await?;
    let commit = writer.complete().await?;
    fs_client
        .complete_file(&file_path, data.len() as i64, vec![commit], false)
        .await?;

    let block_locs = fs.get_block_locations(&file_path).await?;
    for bloc in &block_locs.block_locs {
        assert_eq!(
            bloc.locs.len(),
            2,
            "Middle down should degrade to 2 replicas"
        );
    }

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; data.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, data.len());
    assert_eq!(read_data, data);

    let _ = fs.delete(&file_path, false).await;
    info!("Middle worker down test passed");
    Ok(())
}

async fn test_pipeline_head_worker_down(
    fs: &CurvineFileSystem,
    cluster: Arc<MiniCluster>,
) -> CommonResult<()> {
    info!("=== Testing head worker down during write ===");

    let file_path = Path::from("/test_pipeline_head_down.txt");
    let data = vec![12u8; 256 * 1024];
    let chunk_size = 64 * 1024;

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let flags = OpenFlags::new_write_only()
        .set_create(true)
        .set_overwrite(true);
    let fs_client = fs.fs_client();
    let fs_context = fs.fs_context();
    let _ = fs_client
        .open_with_opts(&file_path, opts.clone(), flags)
        .await?;
    let located = fs_client.add_block(&file_path, vec![], 0, None).await?;

    let mut writer = BlockWriter::new(fs_context.clone(), located.clone(), 0).await?;
    writer
        .write(DataSlice::mem_slice(&data[..chunk_size]))
        .await?;
    writer.flush().await?;

    let head_id = located
        .locs
        .first()
        .ok_or_else(|| FsError::common("Missing block locations for head worker"))?
        .worker_id;
    cluster.stop_worker_by_id(head_id)?;
    sleep(Duration::from_millis(300)).await;

    writer
        .write(DataSlice::mem_slice(&data[chunk_size..]))
        .await?;
    let commit = writer.complete().await?;
    fs_client
        .complete_file(&file_path, data.len() as i64, vec![commit], false)
        .await?;

    let block_locs = fs.get_block_locations(&file_path).await?;
    for bloc in &block_locs.block_locs {
        assert_eq!(bloc.locs.len(), 2, "Head down should degrade to 2 replicas");
    }

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; data.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, data.len());
    assert_eq!(read_data, data);

    let _ = fs.delete(&file_path, false).await;
    info!("Head worker down test passed");
    Ok(())
}

async fn test_pipeline_replacement_worker(
    fs: &CurvineFileSystem,
    cluster: Arc<MiniCluster>,
) -> CommonResult<()> {
    info!("=== Testing replacement worker during write ===");

    let file_path = Path::from("/test_pipeline_replacement.txt");
    let data = vec![12u8; 256 * 1024];
    let chunk_size = 64 * 1024;

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let flags = OpenFlags::new_write_only()
        .set_create(true)
        .set_overwrite(true);
    let fs_client = fs.fs_client();
    let fs_context = fs.fs_context();
    let _ = fs_client
        .open_with_opts(&file_path, opts.clone(), flags)
        .await?;
    let located = fs_client.add_block(&file_path, vec![], 0, None).await?;

    let mut writer = BlockWriter::new(fs_context.clone(), located.clone(), 0).await?;
    writer
        .write(DataSlice::mem_slice(&data[..chunk_size]))
        .await?;
    writer.flush().await?;

    let stop_id = located
        .locs
        .get(1)
        .ok_or_else(|| FsError::common("Missing block locations for worker"))?
        .worker_id;
    cluster.stop_worker_by_id(stop_id)?;
    sleep(Duration::from_millis(300)).await;

    writer
        .write(DataSlice::mem_slice(&data[chunk_size..]))
        .await?;
    let commit = writer.complete().await?;
    fs_client
        .complete_file(&file_path, data.len() as i64, vec![commit], false)
        .await?;

    let block_locs = fs.get_block_locations(&file_path).await?;
    for bloc in &block_locs.block_locs {
        assert_eq!(bloc.locs.len(), 3, "Replacement should keep 3 replicas");
        assert!(
            !bloc.locs.iter().any(|w| w.worker_id == stop_id),
            "Stopped worker should not be in locations"
        );
    }

    let mut reader = fs.open(&file_path).await?;
    let mut read_data = vec![0u8; data.len()];
    let bytes_read = reader.read_full(&mut read_data).await?;
    reader.complete().await?;
    assert_eq!(bytes_read, data.len());
    assert_eq!(read_data, data);

    let _ = fs.delete(&file_path, false).await;
    info!("Replacement worker test passed");
    Ok(())
}

#[test]
fn test_pipeline_tail_worker_down_case() -> CommonResult<()> {
    init_logger();

    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move { test_pipeline_tail_worker_down(&fs, cluster.clone()).await })
}

#[test]
fn test_pipeline_middle_worker_down_case() -> CommonResult<()> {
    init_logger();

    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move { test_pipeline_middle_worker_down(&fs, cluster.clone()).await })
}

#[test]
fn test_pipeline_head_worker_down_case() -> CommonResult<()> {
    init_logger();

    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move { test_pipeline_head_worker_down(&fs, cluster.clone()).await })
}

#[test]
fn test_pipeline_stream_timeout_case() -> CommonResult<()> {
    init_logger();

    env::set_var("CURVINE_PIPELINE_WRITE_DELAY_MS", "200");
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
            conf.client.pipeline_timeout_ms = 5;
        })
        .build()?;
    testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let file_path = Path::from("/test_pipeline_stream_timeout.txt");
        let data = vec![14u8; 64 * 1024];

        let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
            .create_parent(true)
            .replicas(3)
            .build();

        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(true);
        let fs_client = fs.fs_client();
        let fs_context = fs.fs_context();
        let _ = fs_client
            .open_with_opts(&file_path, opts.clone(), flags)
            .await?;
        let located = fs_client.add_block(&file_path, vec![], 0, None).await?;
        let head = located
            .locs
            .first()
            .ok_or_else(|| FsError::common("Missing block locations for head worker"))?
            .clone();
        let pipeline_stream = located.locs[1..].to_vec();

        let mut writer = BlockWriterRemote::new_with_pipeline(
            &fs_context,
            located.block.clone(),
            head,
            0,
            pipeline_stream,
        )
        .await?;

        let write_result = writer.write(DataSlice::mem_slice(&data)).await;
        assert!(
            write_result.is_err(),
            "Expected pipeline_stream timeout error"
        );
        if let Err(e) = write_result {
            warn!(
                "Pipeline stream timeout error: {:?}, kind={:?}",
                e,
                e.kind()
            );
            assert!(e.is_pipeline_error(), "Expected pipeline timeout error");
        }

        let _ = writer.cancel().await;
        let _ = fs.delete(&file_path, false).await;
        env::remove_var("CURVINE_PIPELINE_WRITE_DELAY_MS");
        Ok(())
    })
}

#[test]
fn test_pipeline_replacement_worker_case() -> CommonResult<()> {
    init_logger();

    let testing = Testing::builder()
        .workers(4)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
            conf.master.min_replication = 3;
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move { test_pipeline_replacement_worker(&fs, cluster.clone()).await })
}

#[test]
fn test_pipeline_advanced_scenarios() -> CommonResult<()> {
    init_logger();

    info!("Starting Pipeline advanced scenarios test");
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.replicas = 3;
            conf.client.block_size_str = "1MB".to_string();
            conf.client.write_chunk_size_str = "64KB".to_string();
            conf.client.short_circuit = false;
        })
        .build()?;
    testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        test_pipeline_block_locations(&fs).await?;
        test_pipeline_multiple_blocks(&fs).await?;
        test_pipeline_large_file_streaming(&fs).await?;

        info!("Pipeline advanced scenarios test completed successfully");
        Ok(())
    })
}

async fn test_pipeline_block_locations(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Testing pipeline block location distribution ===");

    let file_path = Path::from("/test_pipeline_block_locations.txt");
    let data = vec![1u8; 3 * 1024 * 1024];

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;
    writer.write(&data).await?;
    writer.complete().await?;

    let block_locs = fs.get_block_locations(&file_path).await?;
    info!("File has {} blocks", block_locs.block_locs.len());

    for (i, bloc) in block_locs.block_locs.iter().enumerate() {
        info!(
            "Block {}: id={}, {} replicas",
            i,
            bloc.block.id,
            bloc.locs.len()
        );
        assert_eq!(bloc.locs.len(), 3, "Each block should have 3 replicas");

        for (j, loc) in bloc.locs.iter().enumerate() {
            info!("  Replica {}: worker_id={}", j, loc.worker_id);
        }

        let worker_ids: Vec<u32> = bloc.locs.iter().map(|l| l.worker_id).collect();
        let unique_workers: std::collections::HashSet<_> = worker_ids.iter().collect();
        assert_eq!(
            unique_workers.len(),
            3,
            "Replicas should be on different workers"
        );
    }

    fs.delete(&file_path, false).await?;
    info!("Block location distribution test passed");
    Ok(())
}

async fn test_pipeline_multiple_blocks(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Testing pipeline with multiple blocks ===");

    let file_path = Path::from("/test_pipeline_multi_blocks.txt");
    let block_size = 1024 * 1024;
    let num_blocks = 5;
    let total_size = block_size * num_blocks;

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;

    for block_idx in 0..num_blocks {
        let data = vec![block_idx as u8; block_size];
        writer.write(&data).await?;
        info!("Written block {}/{}", block_idx + 1, num_blocks);
    }

    writer.complete().await?;

    let status = fs.get_status(&file_path).await?;
    assert_eq!(status.len, total_size as i64);

    let block_locs = fs.get_block_locations(&file_path).await?;
    assert_eq!(
        block_locs.block_locs.len(),
        num_blocks,
        "Should have {} blocks",
        num_blocks
    );

    let mut reader = fs.open(&file_path).await?;
    for block_idx in 0..num_blocks {
        let mut block_data = vec![0u8; block_size];
        let bytes_read = reader.read_full(&mut block_data).await?;
        assert_eq!(bytes_read, block_size);
        assert_eq!(
            block_data,
            vec![block_idx as u8; block_size],
            "Block {} data mismatch",
            block_idx
        );
    }
    reader.complete().await?;

    fs.delete(&file_path, false).await?;
    info!("Multiple blocks test passed");
    Ok(())
}

async fn test_pipeline_large_file_streaming(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Testing pipeline with large file streaming write ===");

    let file_path = Path::from("/test_pipeline_streaming.txt");
    let chunk_size = 128 * 1024;
    let num_chunks = 100;
    let total_size = chunk_size * num_chunks;

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .replicas(3)
        .build();

    let mut writer = fs.create_with_opts(&file_path, opts, true).await?;

    for chunk_idx in 0..num_chunks {
        let data = vec![(chunk_idx % 256) as u8; chunk_size];
        writer.write(&data).await?;

        if (chunk_idx + 1) % 20 == 0 {
            info!("Written {}/{} chunks", chunk_idx + 1, num_chunks);
        }
    }

    writer.complete().await?;

    let status = fs.get_status(&file_path).await?;
    assert_eq!(status.len, total_size as i64);

    let mut reader = fs.open(&file_path).await?;
    for chunk_idx in 0..num_chunks {
        let mut chunk_data = vec![0u8; chunk_size];
        let bytes_read = reader.read_full(&mut chunk_data).await?;
        assert_eq!(bytes_read, chunk_size);
        assert_eq!(
            chunk_data,
            vec![(chunk_idx % 256) as u8; chunk_size],
            "Chunk {} data mismatch",
            chunk_idx
        );
    }
    reader.complete().await?;

    fs.delete(&file_path, false).await?;
    info!("Large file streaming test passed");
    Ok(())
}
