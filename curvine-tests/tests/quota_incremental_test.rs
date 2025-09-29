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

//! 增量配额功能完整集成测试
//!
//! 测试覆盖：
//! 1. 基本文件读写的 subtree_bytes 更新
//! 2. 硬链接（link/unlink）的配额影响
//! 3. 目录重命名的配额传播
//! 4. 目录删除的配额清理
//! 5. 文件覆写的配额更新
//! 6. O(1) 配额查询性能验证

use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::CreateFileOptsBuilder;
use curvine_server::test::MiniCluster;
use log::info;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::sync::Arc;

#[test]
fn quota_incremental_integration_test() -> CommonResult<()> {
    // 创建集群配置
    let mut conf = ClusterConf::default();
    conf.client.block_size = 64 * 1024;
    conf.master.min_block_size = 64 * 1024;

    // 配置 worker 数据目录
    conf.worker.data_dir = vec!["/tmp/curvine-test-data".to_string()];

    // 启动 MiniCluster (1 master, 1 worker)
    let cluster = MiniCluster::with_num(&conf, 1, 1);
    let conf = cluster.master_conf().clone();

    cluster.start_cluster();

    // 等待集群启动
    Utils::sleep(10000);

    let rt = Arc::new(AsyncRuntime::single());

    let rt1 = rt.clone();
    let res: CommonResult<()> = rt.block_on(async move {
        // 创建文件系统客户端
        let fs = CurvineFileSystem::with_rt(conf, rt1)?;

        // 清理测试环境
        let test_root = Path::from_str("/quota_test")?;
        let _ = fs.delete(&test_root, true).await;

        // 测试1: 基本文件读写的 subtree_bytes 更新
        test_basic_file_operations(&fs).await?;

        // 测试2: 硬链接的配额影响
        test_hardlink_quota_impact(&fs).await?;

        // 测试3: 目录重命名的配额传播
        test_directory_rename_quota_propagation(&fs).await?;

        // 测试4: 目录删除的配额清理
        test_directory_deletion_quota_cleanup(&fs).await?;

        // 测试5: 文件覆写的配额更新
        test_file_overwrite_quota_update(&fs).await?;

        // 测试6: O(1) 配额查询性能验证
        test_quota_query_performance(&fs).await?;

        Ok(())
    });

    res
}

/// 测试1: 基本文件读写的 subtree_bytes 更新
async fn test_basic_file_operations(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 1: Basic File Operations ===");

    // 创建测试目录并设置配额
    let quota_dir = Path::from_str("/quota_test/basic")?;
    fs.mkdir(&quota_dir, true).await?;
    fs.fs_client()
        .add_quota("/quota_test/basic", 1024 * 1024)
        .await?; // 1MB 配额

    // 验证初始配额状态
    let quota_info = fs.fs_client().get_quota_info("/quota_test/basic").await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(quota_info.used_size, 0, "初始配额使用应为0");
    assert_eq!(quota_info.quota_size, 1024 * 1024, "配额大小应为1MB");

    // 创建文件并写入数据
    let file_path = Path::from_str("/quota_test/basic/test_file.txt")?;
    let test_data = "Hello, Curvine Quota!";
    let expected_size = test_data.len() as i64;

    fs.write_string(&file_path, test_data).await?;

    // 验证配额更新
    let quota_info = fs.fs_client().get_quota_info("/quota_test/basic").await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(
        quota_info.used_size, expected_size,
        "配额使用应等于文件大小: expected={}, actual={}",
        expected_size, quota_info.used_size
    );

    // 追加写入数据
    let additional_data = " Additional content.";
    let total_expected_size = expected_size + additional_data.len() as i64;

    fs.append_string(&file_path, additional_data).await?;

    // 验证配额更新
    let quota_info = fs.fs_client().get_quota_info("/quota_test/basic").await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(
        quota_info.used_size, total_expected_size,
        "追加后配额使用应正确更新: expected={}, actual={}",
        total_expected_size, quota_info.used_size
    );

    info!("✓ Basic file operations quota tracking works correctly");
    Ok(())
}

/// 测试2: 硬链接的配额影响
async fn test_hardlink_quota_impact(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 2: Hardlink Quota Impact ===");

    // 创建测试目录并设置配额
    let quota_dir = Path::from_str("/quota_test/hardlink")?;
    fs.mkdir(&quota_dir, true).await?;
    fs.fs_client()
        .add_quota("/quota_test/hardlink", 2 * 1024 * 1024)
        .await?; // 2MB 配额

    // 创建原始文件
    let original_file = Path::from_str("/quota_test/hardlink/original.txt")?;
    let test_data = "Hardlink test data content.";
    let file_size = test_data.len() as i64;

    fs.write_string(&original_file, test_data).await?;

    // 验证初始配额
    let quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/hardlink")
        .await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(quota_info.used_size, file_size, "原始文件创建后配额正确");

    // 创建硬链接
    let hardlink_file = Path::from_str("/quota_test/hardlink/hardlink.txt")?;
    fs.link(&original_file, &hardlink_file).await?;

    // 验证硬链接创建后配额 (应该增加一份文件大小，因为每个目录条目都计入父目录)
    let quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/hardlink")
        .await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(
        quota_info.used_size,
        file_size * 2,
        "硬链接创建后配额应增加: expected={}, actual={}",
        file_size * 2,
        quota_info.used_size
    );

    // 删除硬链接
    fs.delete(&hardlink_file, false).await?;

    // 验证硬链接删除后配额恢复
    let quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/hardlink")
        .await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(
        quota_info.used_size, file_size,
        "硬链接删除后配额应恢复: expected={}, actual={}",
        file_size, quota_info.used_size
    );

    // 删除原始文件
    fs.delete(&original_file, false).await?;

    // 验证所有文件删除后配额为0
    let quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/hardlink")
        .await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(quota_info.used_size, 0, "所有文件删除后配额应为0");

    info!("✓ Hardlink quota tracking works correctly");
    Ok(())
}

/// 测试3: 目录重命名的配额传播
async fn test_directory_rename_quota_propagation(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 3: Directory Rename Quota Propagation ===");

    // 创建源目录和目标目录，分别设置配额
    let source_quota_dir = Path::from_str("/quota_test/rename_src")?;
    let target_quota_dir = Path::from_str("/quota_test/rename_dst")?;

    fs.mkdir(&source_quota_dir, true).await?;
    fs.mkdir(&target_quota_dir, true).await?;

    fs.fs_client()
        .add_quota("/quota_test/rename_src", 1024 * 1024)
        .await?; // 1MB 配额
    fs.fs_client()
        .add_quota("/quota_test/rename_dst", 2 * 1024 * 1024)
        .await?; // 2MB 配额

    // 在源目录中创建子目录和文件
    let subdir = Path::from_str("/quota_test/rename_src/subdir")?;
    fs.mkdir(&subdir, true).await?;

    let file1 = Path::from_str("/quota_test/rename_src/subdir/file1.txt")?;
    let file2 = Path::from_str("/quota_test/rename_src/subdir/file2.txt")?;

    let test_data = "Directory rename test data.";
    let file_size = test_data.len() as i64;

    fs.write_string(&file1, test_data).await?;
    fs.write_string(&file2, test_data).await?;

    let total_size = file_size * 2;

    // 验证源目录配额
    let src_quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/rename_src")
        .await?;
    assert!(src_quota_info.is_some(), "源配额信息应该存在");
    let src_quota_info = src_quota_info.unwrap();
    assert_eq!(src_quota_info.used_size, total_size, "源目录配额应正确");

    // 验证目标目录配额（应为0）
    let dst_quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/rename_dst")
        .await?;
    assert!(dst_quota_info.is_some(), "目标配额信息应该存在");
    let dst_quota_info = dst_quota_info.unwrap();
    assert_eq!(dst_quota_info.used_size, 0, "目标目录配额应为0");

    // 将子目录从源目录移动到目标目录
    let old_subdir = Path::from_str("/quota_test/rename_src/subdir")?;
    let new_subdir = Path::from_str("/quota_test/rename_dst/subdir")?;
    fs.rename(&old_subdir, &new_subdir).await?;

    // 验证移动后的配额变化
    let src_quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/rename_src")
        .await?;
    assert!(src_quota_info.is_some(), "源配额信息应该存在");
    let src_quota_info = src_quota_info.unwrap();
    assert_eq!(
        src_quota_info.used_size, 0,
        "源目录移动后配额应为0: actual={}",
        src_quota_info.used_size
    );

    let dst_quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/rename_dst")
        .await?;
    assert!(dst_quota_info.is_some(), "目标配额信息应该存在");
    let dst_quota_info = dst_quota_info.unwrap();
    assert_eq!(
        dst_quota_info.used_size, total_size,
        "目标目录移动后配额应增加: expected={}, actual={}",
        total_size, dst_quota_info.used_size
    );

    info!("✓ Directory rename quota propagation works correctly");
    Ok(())
}

/// 测试4: 目录删除的配额清理
async fn test_directory_deletion_quota_cleanup(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 4: Directory Deletion Quota Cleanup ===");

    // 创建测试目录并设置配额
    let quota_dir = Path::from_str("/quota_test/deletion")?;
    fs.mkdir(&quota_dir, true).await?;
    fs.fs_client()
        .add_quota("/quota_test/deletion", 5 * 1024 * 1024)
        .await?; // 5MB 配额

    // 创建复杂的目录结构
    let subdir1 = Path::from_str("/quota_test/deletion/subdir1")?;
    let subdir2 = Path::from_str("/quota_test/deletion/subdir1/subdir2")?;
    fs.mkdir(&subdir1, true).await?;
    fs.mkdir(&subdir2, true).await?;

    // 创建多个文件
    let files = vec![
        "/quota_test/deletion/file1.txt",
        "/quota_test/deletion/subdir1/file2.txt",
        "/quota_test/deletion/subdir1/subdir2/file3.txt",
    ];

    let test_data = "Directory deletion test data content.";
    let file_size = test_data.len() as i64;
    let total_expected_size = file_size * files.len() as i64;

    for file_path_str in &files {
        let file_path = Path::from_str(*file_path_str)?;
        fs.write_string(&file_path, test_data).await?;
    }

    // 验证所有文件创建后的配额
    let quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/deletion")
        .await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(
        quota_info.used_size, total_expected_size,
        "所有文件创建后配额应正确: expected={}, actual={}",
        total_expected_size, quota_info.used_size
    );

    // 删除子目录（递归删除）
    fs.delete(&subdir1, true).await?;

    // 验证删除后配额更新
    let quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/deletion")
        .await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(
        quota_info.used_size, file_size,
        "子目录删除后配额应正确: expected={}, actual={}",
        file_size, quota_info.used_size
    );

    // 删除剩余文件
    let remaining_file = Path::from_str("/quota_test/deletion/file1.txt")?;
    fs.delete(&remaining_file, false).await?;

    // 验证最终配额为0
    let quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/deletion")
        .await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(quota_info.used_size, 0, "所有内容删除后配额应为0");

    info!("✓ Directory deletion quota cleanup works correctly");
    Ok(())
}

/// 测试5: 文件覆写的配额更新
async fn test_file_overwrite_quota_update(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 5: File Overwrite Quota Update ===");

    // 创建测试目录并设置配额
    let quota_dir = Path::from_str("/quota_test/overwrite")?;
    fs.mkdir(&quota_dir, true).await?;
    fs.fs_client()
        .add_quota("/quota_test/overwrite", 1024 * 1024)
        .await?; // 1MB 配额

    // 创建初始文件
    let file_path = Path::from_str("/quota_test/overwrite/test_file.txt")?;
    let initial_data = "Initial content for overwrite test.";
    let initial_size = initial_data.len() as i64;

    fs.write_string(&file_path, initial_data).await?;

    // 验证初始配额
    let quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/overwrite")
        .await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(quota_info.used_size, initial_size, "初始文件配额正确");

    // 覆写文件为更大的内容
    let overwrite_data = "This is a much longer content for overwrite testing. It should be significantly larger than the original content.";
    let overwrite_size = overwrite_data.len() as i64;

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .overwrite(true)
        .build();
    let mut writer = fs.create_with_opts(&file_path, opts).await?;
    writer.write(overwrite_data.as_bytes()).await?;
    writer.complete().await?;

    // 验证覆写后配额更新
    let quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/overwrite")
        .await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(
        quota_info.used_size, overwrite_size,
        "覆写后配额应更新: expected={}, actual={}",
        overwrite_size, quota_info.used_size
    );

    // 覆写文件为更小的内容
    let smaller_data = "Small.";
    let smaller_size = smaller_data.len() as i64;

    fs.write_string(&file_path, smaller_data).await?;

    // 验证缩小后配额更新
    let quota_info = fs
        .fs_client()
        .get_quota_info("/quota_test/overwrite")
        .await?;
    assert!(quota_info.is_some(), "配额信息应该存在");
    let quota_info = quota_info.unwrap();
    assert_eq!(
        quota_info.used_size, smaller_size,
        "缩小后配额应更新: expected={}, actual={}",
        smaller_size, quota_info.used_size
    );

    info!("✓ File overwrite quota update works correctly");
    Ok(())
}

/// 测试6: O(1) 配额查询性能验证
async fn test_quota_query_performance(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 6: O(1) Quota Query Performance ===");

    // 创建深层目录结构来验证O(1)查询
    let quota_dir = Path::from_str("/quota_test/performance")?;
    fs.mkdir(&quota_dir, true).await?;
    fs.fs_client()
        .add_quota("/quota_test/performance", 100 * 1024 * 1024)
        .await?; // 100MB 配额

    // 创建大量子目录和文件
    let num_subdirs = 10;
    let num_files_per_dir = 5;
    let file_data = "Performance test data for O(1) quota query verification.";
    let file_size = file_data.len() as i64;

    for i in 0..num_subdirs {
        let subdir = Path::from_str(format!("/quota_test/performance/subdir_{}", i))?;
        fs.mkdir(&subdir, true).await?;

        for j in 0..num_files_per_dir {
            let file_path = Path::from_str(format!(
                "/quota_test/performance/subdir_{}/file_{}.txt",
                i, j
            ))?;
            fs.write_string(&file_path, file_data).await?;
        }
    }

    let expected_total_size = file_size * (num_subdirs * num_files_per_dir) as i64;

    // 多次查询配额，验证一致性和性能
    let num_queries = 10;
    let start_time = std::time::Instant::now();

    for i in 0..num_queries {
        let quota_info = fs
            .fs_client()
            .get_quota_info("/quota_test/performance")
            .await?;
        assert!(quota_info.is_some(), "配额信息查询失败 (iteration {})", i);
        let quota_info = quota_info.unwrap();
        assert_eq!(
            quota_info.used_size, expected_total_size,
            "配额查询结果不一致 (iteration {}): expected={}, actual={}",
            i, expected_total_size, quota_info.used_size
        );
    }

    let elapsed = start_time.elapsed();
    let avg_query_time = elapsed / num_queries;

    info!(
        "✓ O(1) quota query performance: {} queries in {:?}, avg: {:?}",
        num_queries, elapsed, avg_query_time
    );

    // 验证查询时间应该很短（小于50ms，因为网络延迟）
    assert!(
        avg_query_time.as_millis() < 50,
        "配额查询平均时间过长: {:?}",
        avg_query_time
    );

    // 验证配额表查询
    let quota_table = fs.fs_client().get_quota_table().await?;
    let test_quotas: Vec<_> = quota_table
        .iter()
        .filter(|q| q.path.starts_with("/quota_test/"))
        .collect();

    assert!(!test_quotas.is_empty(), "配额表应包含测试配额");
    info!("✓ Found {} quota entries in quota table", test_quotas.len());

    for quota in test_quotas {
        info!(
            "Quota: path={}, used={}, limit={}, state={:?}",
            quota.path, quota.used_size, quota.quota_size, quota.state
        );
    }

    info!("✓ O(1) quota query performance verification passed");
    Ok(())
}
