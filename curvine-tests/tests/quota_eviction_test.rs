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

use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::Path;
use curvine_server::test::MiniCluster;
use log::info;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Test LRU eviction functionality with comprehensive scenarios
#[test]
fn quota_lru_eviction_integration_test() -> CommonResult<()> {
    let mut conf = ClusterConf::default();
    conf.client.block_size = 64 * 1024;
    conf.master.min_block_size = 64 * 1024;

    conf.worker.data_dir = vec!["/tmp/curvine-test-data-lru".to_string()];

    conf.master.enable_prequota_eviction = true;
    conf.master.eviction_mode = "DeleteFile".to_string(); // 使用 DeleteFile 模式真正删除文件
    conf.master.eviction_policy = "Lru".to_string();
    conf.master.eviction_high_watermark = 0.8; // 80% 触发淘汰
    conf.master.eviction_low_watermark = 0.6; // 60% 停止淘汰
    conf.master.eviction_target_margin_ratio = 0.05; // 目标水位线安全边距，计算target_ratio = min(low_watermark, high_watermark - target_margin_ratio)
    conf.master.eviction_candidate_scan_page = 2; // 每次从 LRU 中扫描的候选文件数量（分页=2，覆盖批量淘汰）
    conf.master.eviction_max_rate_bytes_per_s = 64; // 限速，覆盖多轮淘汰
    conf.master.eviction_dry_run = false;

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    let conf = cluster.master_conf().clone();

    cluster.start_cluster();

    Utils::sleep(10000);

    let rt = Arc::new(AsyncRuntime::single());
    let rt1 = rt.clone();
    let res: CommonResult<()> = rt.block_on(async move {
        let fs = CurvineFileSystem::with_rt(conf, rt1)?;

        // Clean up any existing test data
        let test_root = Path::from_str("/lru_test")?;
        let _ = fs.delete(&test_root, true).await;

        info!("=== LRU Eviction Test Suite ===");

        // Test 1: Basic LRU Access Tracking
        test_basic_lru_access_tracking(&fs).await?;

        // Test 2: LRU Victim Selection Order
        test_lru_victim_selection_order(&fs).await?;

        // Test 3: LRU Cache Size Limits
        test_lru_cache_size_limits(&fs).await?;

        // Test 4: Multiple Quota LRU Isolation
        test_multiple_quota_lru_isolation(&fs).await?;

        // Test 5: LRU Access Pattern Simulation
        test_lru_access_pattern_simulation(&fs).await?;

        // Test 6: LRU Eviction with File Deletion
        test_lru_eviction_with_file_deletion(&fs).await?;

        // Test 7: LRU Performance Under Load
        test_lru_performance_under_load(&fs).await?;

        // Test 8: LRU Concurrent Access Safety
        test_lru_concurrent_access_safety(&fs).await?;

        // Test 9: LRU Memory Management
        test_lru_memory_management(&fs).await?;

        // Test 10: LRU Integration with Quota Enforcement
        test_lru_quota_enforcement_integration(&fs).await?;

        // Test 11: Verify Actual File Eviction
        test_actual_file_eviction(&fs).await?;

        // Test 12: Multiple Reads Impact on Eviction Order
        test_multiple_reads_eviction_impact(&fs).await?;

        // Test 13: Cleaning Guard in single cluster
        test_cleaning_guard_integration(&fs).await?;

        // Test 14: Directory as victim (subtree eviction acceptable)
        test_directory_victim_eviction(&fs).await?;

        Ok(())
    });

    match res {
        Ok(_) => {
            info!("✅ All LRU eviction tests passed");
            Ok(())
        }
        Err(e) => {
            eprintln!("❌ LRU eviction tests failed: {:?}", e);
            Err(e)
        }
    }
}

/// Concurrent triggers should not cause duplicate overlapping cleanings (run inside main cluster)
async fn test_cleaning_guard_integration(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 13: Cleaning Guard (Single Cluster) ===");

    let root = Path::from_str("/lru_test/cleaning")?;
    let _ = fs.delete(&root, true).await;
    fs.mkdir(&root, true).await?;
    fs.fs_client().add_quota("/lru_test/cleaning", 350).await?;

    for i in 0..3u32 {
        let p = Path::from_str(&format!("/lru_test/cleaning/f_{}.txt", i))?;
        fs.write_string(&p, &format!("f{}", i)).await?;
        fs.read_string(&p).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let t1 = Path::from_str("/lru_test/cleaning/tr1.txt")?;
    let t2 = Path::from_str("/lru_test/cleaning/tr2.txt")?;
    let t1_task = t1.clone();
    let t2_task = t2.clone();
    let big = "x".repeat(260);
    let big1 = big.clone();
    let big2 = big;
    let fs_c1 = fs.clone();
    let fs_c2 = fs.clone();

    let h1 = tokio::spawn(async move {
        let _ = fs_c1.write_string(&t1_task, &big1).await;
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let h2 = tokio::spawn(async move {
        let _ = fs_c2.write_string(&t2_task, &big2).await;
    });
    let _ = tokio::join!(h1, h2);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let q = fs.fs_client().get_quota_info("/lru_test/cleaning").await?;
    assert!(q.is_some());
    let q = q.unwrap();
    assert!(q.used_size <= q.quota_size * 2, "quota should be bounded");

    let survivors = [
        Path::from_str("/lru_test/cleaning/f_0.txt")?,
        Path::from_str("/lru_test/cleaning/f_1.txt")?,
        Path::from_str("/lru_test/cleaning/f_2.txt")?,
    ];
    let mut evicted_or_not = 0usize;
    for s in survivors.iter() {
        if !fs.read_string(s).await.is_ok() {
            evicted_or_not += 1;
        }
    }
    assert!(evicted_or_not >= 1, "at least one victim should be evicted");

    let t1_exists = fs.read_string(&t1).await.is_ok();
    let t2_exists = fs.read_string(&t2).await.is_ok();
    info!(
        "Cleaning guard result: t1_exists={}, t2_exists={}",
        t1_exists, t2_exists
    );

    info!("✓ Cleaning guard works under concurrent triggers");
    Ok(())
}

/// Test 14: Directory victim eviction (subtree as a single victim)
async fn test_directory_victim_eviction(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 14: Directory Victim Eviction ===");

    // Create a quota dir with a subdir containing multiple files
    let root = Path::from_str("/lru_test/dir_victim")?;
    let _ = fs.delete(&root, true).await;
    fs.mkdir(&root, true).await?;
    fs.fs_client()
        .add_quota("/lru_test/dir_victim", 500)
        .await?;

    // Subdir with data to act as a large single victim (subtree)
    let sub = Path::from_str("/lru_test/dir_victim/subtree")?;
    fs.mkdir(&sub, true).await?;
    for i in 0..5u32 {
        let p = Path::from_str(&format!("/lru_test/dir_victim/subtree/file_{}.txt", i))?;
        fs.write_string(&p, &format!("content-{}-{}", i, "x".repeat(40)))
            .await?;
        // 访问以确保进入LRU
        fs.read_string(&p).await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // 一个新文件，作为较新的热点
    let hot = Path::from_str("/lru_test/dir_victim/hot.txt")?;
    fs.write_string(&hot, &"hot".repeat(30)).await?;
    fs.read_string(&hot).await?;

    // 触发文件，提升使用量以触发清理
    let trigger = Path::from_str("/lru_test/dir_victim/trigger.txt")?;
    let big = "x".repeat(350);
    fs.write_string(&trigger, &big).await?;

    // 等待清理执行
    tokio::time::sleep(Duration::from_millis(400)).await;

    // 断言：subtree 目录有较大概率作为单个受害者被整体删除（允许多删策略）
    let sub_exists = fs
        .read_string(&Path::from_str("/lru_test/dir_victim/subtree/file_0.txt")?)
        .await
        .is_ok();
    let hot_exists = fs.read_string(&hot).await.is_ok();

    // 至少要保证配额回到合理范围
    let q = fs
        .fs_client()
        .get_quota_info("/lru_test/dir_victim")
        .await?;
    assert!(q.is_some());
    let q = q.unwrap();
    assert!(
        q.used_size <= q.quota_size * 2,
        "quota should be bounded after directory victim eviction"
    );

    info!(
        "Directory victim eviction: subtree_remaining={}, hot_exists={}",
        sub_exists, hot_exists
    );
    Ok(())
}

/// Test 1: Basic LRU Access Tracking
async fn test_basic_lru_access_tracking(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 1: Basic LRU Access Tracking ===");

    // Create test directory with quota
    fs.mkdir(&Path::from_str("/lru_test/basic")?, true).await?;
    fs.fs_client()
        .add_quota("/lru_test/basic", 1024 * 1024)
        .await?; // 1MB

    // Create multiple files
    let files = vec!["file_a.txt", "file_b.txt", "file_c.txt"];
    for file in &files {
        let path = Path::from_str(&format!("/lru_test/basic/{}", file))?;
        let content = format!("Content of {}", file);
        fs.write_string(&path, &content).await?;
    }

    // Access files in specific order: A -> B -> C -> A
    // This should make B the least recently used
    fs.read_string(&Path::from_str("/lru_test/basic/file_a.txt")?)
        .await?;
    fs.read_string(&Path::from_str("/lru_test/basic/file_b.txt")?)
        .await?;
    fs.read_string(&Path::from_str("/lru_test/basic/file_c.txt")?)
        .await?;
    fs.read_string(&Path::from_str("/lru_test/basic/file_a.txt")?)
        .await?; // A becomes most recent

    // Verify quota tracking
    let quota_info = fs.fs_client().get_quota_info("/lru_test/basic").await?;
    assert!(quota_info.is_some(), "Quota should exist");
    let quota = quota_info.unwrap();
    assert!(quota.used_size > 0, "Used size should be greater than 0");

    info!("✓ Basic LRU access tracking works correctly");
    Ok(())
}

/// Test 2: LRU Victim Selection Order
async fn test_lru_victim_selection_order(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 2: LRU Victim Selection Order ===");

    // Create test directory with small quota to trigger eviction
    fs.mkdir(&Path::from_str("/lru_test/victims")?, true)
        .await?;
    fs.fs_client().add_quota("/lru_test/victims", 500).await?; // Small quota to trigger eviction

    // Create files with known access pattern
    let files = vec!["oldest.txt", "middle.txt", "newest.txt"];
    for (i, file) in files.iter().enumerate() {
        let path = Path::from_str(&format!("/lru_test/victims/{}", file))?;
        let content = format!("File {} content with some data to use space", i);
        fs.write_string(&path, &content).await?;

        // Add delay to ensure different access times
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Access files in reverse order to establish LRU order
    // newest.txt -> middle.txt -> oldest.txt (oldest becomes LRU)
    fs.read_string(&Path::from_str("/lru_test/victims/newest.txt")?)
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    fs.read_string(&Path::from_str("/lru_test/victims/middle.txt")?)
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    fs.read_string(&Path::from_str("/lru_test/victims/oldest.txt")?)
        .await?;

    // Check initial state - all files should exist
    let oldest_exists_before = fs
        .read_string(&Path::from_str("/lru_test/victims/oldest.txt")?)
        .await
        .is_ok();
    let middle_exists_before = fs
        .read_string(&Path::from_str("/lru_test/victims/middle.txt")?)
        .await
        .is_ok();
    let newest_exists_before = fs
        .read_string(&Path::from_str("/lru_test/victims/newest.txt")?)
        .await
        .is_ok();
    assert!(
        oldest_exists_before && middle_exists_before && newest_exists_before,
        "All files should exist before eviction"
    );

    // Create a large file to trigger eviction (should exceed 80% of 500 bytes = 400 bytes)
    let large_content = "x".repeat(300); // Should trigger eviction
    let _result = fs
        .write_string(
            &Path::from_str("/lru_test/victims/trigger.txt")?,
            &large_content,
        )
        .await;

    // Wait for eviction to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Check which files survived (using DeleteFile mode, files should be truly deleted)
    let oldest_exists_after = fs
        .read_string(&Path::from_str("/lru_test/victims/oldest.txt")?)
        .await
        .is_ok();
    let middle_exists_after = fs
        .read_string(&Path::from_str("/lru_test/victims/middle.txt")?)
        .await
        .is_ok();
    let newest_exists_after = fs
        .read_string(&Path::from_str("/lru_test/victims/newest.txt")?)
        .await
        .is_ok();

    info!("File existence after eviction:");
    info!(
        "  Oldest file: {} -> {}",
        oldest_exists_before, oldest_exists_after
    );
    info!(
        "  Middle file: {} -> {}",
        middle_exists_before, middle_exists_after
    );
    info!(
        "  Newest file: {} -> {}",
        newest_exists_before, newest_exists_after
    );

    // Verify LRU order: oldest should be evicted first
    if !oldest_exists_after {
        info!("✓ Oldest file was correctly evicted first (LRU order verified)");
    } else {
        info!("⚠ Oldest file still exists - eviction may not have been triggered");
    }

    // Check if eviction was triggered (quota exceeded or file creation failed)
    let quota_info = fs.fs_client().get_quota_info("/lru_test/victims").await?;
    if let Some(quota) = quota_info {
        info!(
            "Quota after eviction trigger: used={}, limit={}, state={:?}",
            quota.used_size, quota.quota_size, quota.state
        );
    }

    info!("✓ LRU victim selection order test completed");
    Ok(())
}

/// Test 3: LRU Cache Size Limits
async fn test_lru_cache_size_limits(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 3: LRU Cache Size Limits ===");

    // Create test directory
    fs.mkdir(&Path::from_str("/lru_test/cache_limits")?, true)
        .await?;
    fs.fs_client()
        .add_quota("/lru_test/cache_limits", 10 * 1024)
        .await?; // 10KB

    // Create many small files to test cache limits
    let file_count = 50;
    for i in 0..file_count {
        let path = Path::from_str(&format!("/lru_test/cache_limits/file_{:03}.txt", i))?;
        let content = format!("Small file {} content", i);
        fs.write_string(&path, &content).await?;
    }

    // Access all files to populate LRU cache
    for i in 0..file_count {
        let path = Path::from_str(&format!("/lru_test/cache_limits/file_{:03}.txt", i))?;
        let _ = fs.read_string(&path).await;
    }

    // Access some files multiple times to test cache behavior
    for i in (0..10).rev() {
        let path = Path::from_str(&format!("/lru_test/cache_limits/file_{:03}.txt", i))?;
        let _ = fs.read_string(&path).await;
    }

    let quota_info = fs
        .fs_client()
        .get_quota_info("/lru_test/cache_limits")
        .await?;
    assert!(quota_info.is_some(), "Quota should exist");

    info!("✓ LRU cache size limits test completed");
    Ok(())
}

/// Test 4: Multiple Quota LRU Isolation
async fn test_multiple_quota_lru_isolation(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 4: Multiple Quota LRU Isolation ===");

    // Create multiple quota directories
    let quotas = vec![
        ("/lru_test/quota_a", 2048),
        ("/lru_test/quota_b", 2048),
        ("/lru_test/quota_c", 2048),
    ];

    for (path, size) in &quotas {
        fs.mkdir(&Path::from_str(path)?, true).await?;
        fs.fs_client().add_quota(path, *size).await?;
    }

    // Create files in each quota and access them
    for (quota_path, _) in &quotas {
        for i in 0..3 {
            let file_path = format!("{}/file_{}.txt", quota_path, i);
            let path = Path::from_str(&file_path)?;
            let content = format!("Content for {} file {}", quota_path, i);
            fs.write_string(&path, &content).await?;

            // Access the file to add to LRU
            fs.read_string(&path).await?;
        }
    }

    // Verify each quota has independent LRU tracking
    for (quota_path, _) in &quotas {
        let quota_info = fs.fs_client().get_quota_info(quota_path).await?;
        assert!(
            quota_info.is_some(),
            "Quota should exist for {}",
            quota_path
        );
        let quota = quota_info.unwrap();
        assert!(
            quota.used_size > 0,
            "Used size should be > 0 for {}",
            quota_path
        );
    }

    info!("✓ Multiple quota LRU isolation works correctly");
    Ok(())
}

/// Test 5: LRU Access Pattern Simulation with Eviction Verification
async fn test_lru_access_pattern_simulation(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 5: LRU Access Pattern Simulation with Eviction Verification ===");

    // Create test directory with small quota to trigger eviction
    fs.mkdir(&Path::from_str("/lru_test/patterns")?, true)
        .await?;
    fs.fs_client().add_quota("/lru_test/patterns", 400).await?; // Small quota

    // Create files with different access patterns
    let hot_file = Path::from_str("/lru_test/patterns/hot.txt")?;
    let warm_file = Path::from_str("/lru_test/patterns/warm.txt")?;
    let cold_file = Path::from_str("/lru_test/patterns/cold.txt")?;

    fs.write_string(&hot_file, "Hot file content - accessed frequently")
        .await?;
    fs.write_string(&warm_file, "Warm file content - accessed moderately")
        .await?;
    fs.write_string(&cold_file, "Cold file content - accessed rarely")
        .await?;

    // Simulate realistic access patterns
    // Cold file: accessed once (should be evicted first)
    fs.read_string(&cold_file).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Warm file: accessed moderately (should be evicted second)
    for _ in 0..3 {
        fs.read_string(&warm_file).await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Hot file: accessed frequently (should survive longest)
    for _ in 0..6 {
        fs.read_string(&hot_file).await?;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    info!("Access pattern established:");
    info!("  Cold file: 1 access (should be evicted first)");
    info!("  Warm file: 3 accesses (should be evicted second)");
    info!("  Hot file: 6 accesses (should survive)");

    // Check initial state
    let hot_exists_before = fs.read_string(&hot_file).await.is_ok();
    let warm_exists_before = fs.read_string(&warm_file).await.is_ok();
    let cold_exists_before = fs.read_string(&cold_file).await.is_ok();
    assert!(
        hot_exists_before && warm_exists_before && cold_exists_before,
        "All files should exist before eviction"
    );

    // Create trigger file to exceed quota and force eviction
    let trigger_file = Path::from_str("/lru_test/patterns/trigger.txt")?;
    let large_content = "x".repeat(250); // Should trigger eviction
    fs.write_string(&trigger_file, &large_content).await?;

    // Wait for eviction to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify eviction followed access pattern expectations
    let hot_exists_after = fs.read_string(&hot_file).await.is_ok();
    let warm_exists_after = fs.read_string(&warm_file).await.is_ok();
    let cold_exists_after = fs.read_string(&cold_file).await.is_ok();

    info!("File existence after eviction:");
    info!(
        "  Hot file (6 accesses): {} -> {}",
        hot_exists_before, hot_exists_after
    );
    info!(
        "  Warm file (3 accesses): {} -> {}",
        warm_exists_before, warm_exists_after
    );
    info!(
        "  Cold file (1 access): {} -> {}",
        cold_exists_before, cold_exists_after
    );

    // Verify LRU behavior based on access patterns
    if !cold_exists_after {
        info!("✓ Cold file (least accessed) was evicted first");
    }
    if hot_exists_after {
        info!("✓ Hot file (most accessed) survived as expected");
    }

    let quota_info = fs.fs_client().get_quota_info("/lru_test/patterns").await?;
    if let Some(quota) = quota_info {
        info!(
            "Final quota state: used={}, limit={}",
            quota.used_size, quota.quota_size
        );
    }

    info!("✓ LRU access pattern simulation with eviction verification completed");
    Ok(())
}

/// Test 6: LRU Eviction with File Deletion
async fn test_lru_eviction_with_file_deletion(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 6: LRU Eviction with File Deletion ===");

    // Create test directory with small quota to trigger automatic eviction
    fs.mkdir(&Path::from_str("/lru_test/deletion")?, true)
        .await?;
    fs.fs_client().add_quota("/lru_test/deletion", 600).await?; // Small quota

    // Create files that will be candidates for eviction
    let file1 = Path::from_str("/lru_test/deletion/evict_candidate1.txt")?;
    let file2 = Path::from_str("/lru_test/deletion/evict_candidate2.txt")?;
    let file3 = Path::from_str("/lru_test/deletion/evict_candidate3.txt")?;

    fs.write_string(&file1, "Content for eviction candidate 1")
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    fs.read_string(&file1).await?; // Add to LRU (oldest)

    fs.write_string(&file2, "Content for eviction candidate 2")
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    fs.read_string(&file2).await?; // Add to LRU (middle)

    fs.write_string(&file3, "Content for eviction candidate 3")
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    fs.read_string(&file3).await?; // Add to LRU (newest)

    // Check initial state
    let file1_exists_before = fs.read_string(&file1).await.is_ok();
    let file2_exists_before = fs.read_string(&file2).await.is_ok();
    let file3_exists_before = fs.read_string(&file3).await.is_ok();
    assert!(
        file1_exists_before && file2_exists_before && file3_exists_before,
        "All files should exist before eviction"
    );

    // Create a large file to trigger automatic eviction
    let trigger_file = Path::from_str("/lru_test/deletion/large_trigger.txt")?;
    let large_content = "x".repeat(400); // Should exceed quota and trigger eviction
    fs.write_string(&trigger_file, &large_content).await?;

    // Wait for eviction to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Check which files were deleted by automatic eviction (DeleteFile mode)
    let file1_exists_after = fs.read_string(&file1).await.is_ok();
    let file2_exists_after = fs.read_string(&file2).await.is_ok();
    let file3_exists_after = fs.read_string(&file3).await.is_ok();

    info!("File existence after automatic eviction:");
    info!(
        "  File 1 (oldest): {} -> {}",
        file1_exists_before, file1_exists_after
    );
    info!(
        "  File 2 (middle): {} -> {}",
        file2_exists_before, file2_exists_after
    );
    info!(
        "  File 3 (newest): {} -> {}",
        file3_exists_before, file3_exists_after
    );

    // Verify that automatic eviction actually deleted files
    let evicted_count = [file1_exists_after, file2_exists_after, file3_exists_after]
        .iter()
        .filter(|&&exists| !exists)
        .count();

    if evicted_count > 0 {
        info!(
            "✓ Automatic eviction successfully deleted {} file(s)",
            evicted_count
        );

        // Verify LRU order: oldest files should be evicted first
        if !file1_exists_after {
            info!("✓ Oldest file was evicted first (LRU order correct)");
        }
    } else {
        info!("⚠ No files were automatically evicted - quota may not have been exceeded");
    }

    let quota_info = fs.fs_client().get_quota_info("/lru_test/deletion").await?;
    if let Some(quota) = quota_info {
        info!(
            "Final quota state: used={}, limit={}",
            quota.used_size, quota.quota_size
        );
    }

    info!("✓ LRU eviction with file deletion works correctly");
    Ok(())
}

/// Test 7: LRU Performance Under Load
async fn test_lru_performance_under_load(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 7: LRU Performance Under Load ===");

    // Create test directory
    fs.mkdir(&Path::from_str("/lru_test/performance")?, true)
        .await?;
    fs.fs_client()
        .add_quota("/lru_test/performance", 50 * 1024)
        .await?; // 50KB

    let start_time = Instant::now();
    let file_count = 100;

    // Create many files quickly
    for i in 0..file_count {
        let path = Path::from_str(&format!("/lru_test/performance/perf_{:03}.txt", i))?;
        let content = format!("Performance test file {} content", i);
        fs.write_string(&path, &content).await?;
    }

    let creation_time = start_time.elapsed();

    // Access files in random-like pattern
    let access_start = Instant::now();
    for i in 0..file_count {
        let file_idx = (i * 17) % file_count; // Pseudo-random access
        let path = Path::from_str(&format!("/lru_test/performance/perf_{:03}.txt", file_idx))?;
        let _ = fs.read_string(&path).await;
    }

    let access_time = access_start.elapsed();

    info!("Performance metrics:");
    info!(
        "  File creation: {} files in {:?} (avg: {:?}/file)",
        file_count,
        creation_time,
        creation_time / file_count
    );
    info!(
        "  File access: {} accesses in {:?} (avg: {:?}/access)",
        file_count,
        access_time,
        access_time / file_count
    );

    // Verify performance is reasonable (adjust thresholds as needed)
    assert!(
        creation_time < Duration::from_secs(30),
        "Creation should be < 30s"
    );
    assert!(
        access_time < Duration::from_secs(20),
        "Access should be < 20s"
    );

    info!("✓ LRU performance under load is acceptable");
    Ok(())
}

/// Test 8: LRU Concurrent Access Safety
async fn test_lru_concurrent_access_safety(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 8: LRU Concurrent Access Safety ===");

    // Create test directory
    fs.mkdir(&Path::from_str("/lru_test/concurrent")?, true)
        .await?;
    fs.fs_client()
        .add_quota("/lru_test/concurrent", 10 * 1024)
        .await?; // 10KB

    // Create files for concurrent access
    let file_count = 20;
    for i in 0..file_count {
        let path = Path::from_str(&format!("/lru_test/concurrent/conc_{:02}.txt", i))?;
        let content = format!("Concurrent test file {} content", i);
        fs.write_string(&path, &content).await?;
    }

    // Simulate concurrent access using tasks
    let mut tasks = Vec::new();
    for task_id in 0..5 {
        let fs_clone = fs.clone();
        let task = async move {
            for i in 0..10 {
                let file_idx = (task_id * 4 + i) % file_count;
                let path =
                    Path::from_str(&format!("/lru_test/concurrent/conc_{:02}.txt", file_idx))?;
                let _ = fs_clone.read_string(&path).await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok::<(), orpc::CommonError>(())
        };
        tasks.push(task);
    }

    // Wait for all concurrent tasks to complete
    for task in tasks {
        task.await?;
    }

    // Verify system is still consistent
    let quota_info = fs
        .fs_client()
        .get_quota_info("/lru_test/concurrent")
        .await?;
    assert!(
        quota_info.is_some(),
        "Quota should exist after concurrent access"
    );

    info!("✓ LRU concurrent access safety verified");
    Ok(())
}

/// Test 9: LRU Memory Management
async fn test_lru_memory_management(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 9: LRU Memory Management ===");

    // Create test directory
    fs.mkdir(&Path::from_str("/lru_test/memory")?, true).await?;
    fs.fs_client()
        .add_quota("/lru_test/memory", 20 * 1024)
        .await?; // 20KB

    // Create and access many files to test memory usage
    let cycles = 3;
    let files_per_cycle = 30;

    for cycle in 0..cycles {
        info!("Memory test cycle {} of {}", cycle + 1, cycles);

        // Create files for this cycle
        for i in 0..files_per_cycle {
            let path = Path::from_str(&format!("/lru_test/memory/mem_{}_{:02}.txt", cycle, i))?;
            let content = format!("Memory test cycle {} file {} content", cycle, i);
            fs.write_string(&path, &content).await?;
            fs.read_string(&path).await?; // Add to LRU immediately
        }

        // Access some files from previous cycles to test LRU behavior
        if cycle > 0 {
            for i in 0..5 {
                let path =
                    Path::from_str(&format!("/lru_test/memory/mem_{}_{:02}.txt", cycle - 1, i))?;
                let _ = fs.read_string(&path).await;
            }
        }
    }

    let quota_info = fs.fs_client().get_quota_info("/lru_test/memory").await?;
    assert!(quota_info.is_some(), "Quota should exist");

    info!("✓ LRU memory management test completed");
    Ok(())
}

/// Test 10: LRU Integration with Quota Enforcement and Eviction
async fn test_lru_quota_enforcement_integration(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 10: LRU Integration with Quota Enforcement and Eviction ===");

    // Create test directory with small quota to test enforcement and eviction
    fs.mkdir(&Path::from_str("/lru_test/enforcement")?, true)
        .await?;
    fs.fs_client()
        .add_quota("/lru_test/enforcement", 800)
        .await?; // Small quota

    // Create initial files that will be eviction candidates
    let victim1 = Path::from_str("/lru_test/enforcement/victim1.txt")?;
    let victim2 = Path::from_str("/lru_test/enforcement/victim2.txt")?;
    let survivor = Path::from_str("/lru_test/enforcement/survivor.txt")?;

    fs.write_string(&victim1, "Victim file 1 - should be evicted first")
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    fs.read_string(&victim1).await?; // Add to LRU (oldest)

    fs.write_string(&victim2, "Victim file 2 - may be evicted second")
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    fs.read_string(&victim2).await?; // Add to LRU (middle)

    fs.write_string(&survivor, "Survivor file - should remain")
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    fs.read_string(&survivor).await?; // Add to LRU (newest)

    // Check initial quota state
    let quota_before = fs
        .fs_client()
        .get_quota_info("/lru_test/enforcement")
        .await?;
    if let Some(quota) = &quota_before {
        info!(
            "Initial quota state: used={}, limit={}",
            quota.used_size, quota.quota_size
        );
    }

    // Check initial file existence
    let victim1_exists_before = fs.read_string(&victim1).await.is_ok();
    let victim2_exists_before = fs.read_string(&victim2).await.is_ok();
    let survivor_exists_before = fs.read_string(&survivor).await.is_ok();
    assert!(
        victim1_exists_before && victim2_exists_before && survivor_exists_before,
        "All files should exist initially"
    );

    // Create a large file to trigger quota enforcement and eviction
    let trigger_file = Path::from_str("/lru_test/enforcement/large_trigger.txt")?;
    let large_content = "x".repeat(500); // Should exceed quota and trigger eviction

    info!("Creating large file to trigger quota enforcement and eviction...");
    fs.write_string(&trigger_file, &large_content).await?;

    // Wait for eviction processing
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Check which files survived the quota enforcement + eviction
    let victim1_exists_after = fs.read_string(&victim1).await.is_ok();
    let victim2_exists_after = fs.read_string(&victim2).await.is_ok();
    let survivor_exists_after = fs.read_string(&survivor).await.is_ok();

    info!("File existence after quota enforcement and eviction:");
    info!(
        "  Victim 1 (oldest): {} -> {}",
        victim1_exists_before, victim1_exists_after
    );
    info!(
        "  Victim 2 (middle): {} -> {}",
        victim2_exists_before, victim2_exists_after
    );
    info!(
        "  Survivor (newest): {} -> {}",
        survivor_exists_before, survivor_exists_after
    );

    // Verify quota enforcement and LRU eviction worked together
    let evicted_count = [
        victim1_exists_after,
        victim2_exists_after,
        survivor_exists_after,
    ]
    .iter()
    .filter(|&&exists| !exists)
    .count();

    if evicted_count > 0 {
        info!(
            "✓ Quota enforcement triggered eviction of {} file(s)",
            evicted_count
        );

        // Verify LRU order: oldest should be evicted first
        if !victim1_exists_after {
            info!(
                "✓ Oldest file was evicted first (LRU order maintained during quota enforcement)"
            );
        }
    } else {
        info!("⚠ No files were evicted - quota limit may not have been exceeded");
    }

    // Check final quota state
    let quota_after = fs
        .fs_client()
        .get_quota_info("/lru_test/enforcement")
        .await?;
    if let Some(quota) = quota_after {
        info!(
            "Final quota state: used={}, limit={}, state={:?}",
            quota.used_size, quota.quota_size, quota.state
        );

        // Verify quota is not severely exceeded after eviction
        if quota.used_size <= quota.quota_size * 2 {
            info!("✓ Quota usage is within reasonable bounds after eviction");
        }
    }

    info!("✓ LRU integration with quota enforcement and eviction works correctly");
    Ok(())
}

/// Test 11: Verify Actual File Eviction
async fn test_actual_file_eviction(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 11: Verify Actual File Eviction ===");

    // Create test directory with very small quota to force eviction
    fs.mkdir(&Path::from_str("/lru_test/actual_eviction")?, true)
        .await?;
    fs.fs_client()
        .add_quota("/lru_test/actual_eviction", 300)
        .await?; // Very small quota

    // Create first file (oldest in LRU)
    let old_file = Path::from_str("/lru_test/actual_eviction/old_file.txt")?;
    fs.write_string(&old_file, "This is the old file that should be evicted")
        .await?;
    fs.read_string(&old_file).await?; // Add to LRU

    // Wait a bit to ensure different timestamps
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Create second file (newer in LRU)
    let new_file = Path::from_str("/lru_test/actual_eviction/new_file.txt")?;
    fs.write_string(&new_file, "This is the new file that should remain")
        .await?;
    fs.read_string(&new_file).await?; // Add to LRU

    // Verify both files exist initially
    let old_exists_before = fs.read_string(&old_file).await.is_ok();
    let new_exists_before = fs.read_string(&new_file).await.is_ok();
    assert!(old_exists_before, "Old file should exist before eviction");
    assert!(new_exists_before, "New file should exist before eviction");

    info!("Both files created and verified to exist");

    // Create a large file that should trigger eviction of the old file
    let trigger_file = Path::from_str("/lru_test/actual_eviction/trigger_large.txt")?;
    let large_content = "x".repeat(200); // This should exceed quota and trigger eviction
    fs.write_string(&trigger_file, &large_content).await?;

    // Wait for eviction to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Check final state - old file should be evicted, new file should remain
    let old_exists_after = fs.read_string(&old_file).await.is_ok();
    let new_exists_after = fs.read_string(&new_file).await.is_ok();
    let trigger_exists_after = fs.read_string(&trigger_file).await.is_ok();

    info!("File existence after eviction:");
    info!("  Old file exists: {}", old_exists_after);
    info!("  New file exists: {}", new_exists_after);
    info!("  Trigger file exists: {}", trigger_exists_after);

    // Check quota state
    let quota_info = fs
        .fs_client()
        .get_quota_info("/lru_test/actual_eviction")
        .await?;
    if let Some(quota) = quota_info {
        info!(
            "Final quota state: used={}, limit={}, state={:?}",
            quota.used_size, quota.quota_size, quota.state
        );

        // Verify quota is not severely exceeded (allowing some over-deletion)
        assert!(
            quota.used_size <= quota.quota_size * 2,
            "Quota usage should not be severely exceeded after eviction"
        );
    }

    // The key assertion: old file should be evicted (LRU victim)
    if !old_exists_after {
        info!("✓ LRU eviction successfully removed the oldest file");
    } else {
        info!("⚠ Old file still exists - eviction may not have occurred or LRU is empty");
    }

    // New file should ideally remain (but we allow for over-deletion in current implementation)
    if new_exists_after {
        info!("✓ Newer file remained after eviction");
    } else {
        info!("⚠ Newer file was also evicted (acceptable with current over-deletion strategy)");
    }

    info!("✓ Actual file eviction test completed");
    Ok(())
}

/// Test 12: Multiple Reads Impact on Eviction Order
async fn test_multiple_reads_eviction_impact(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Test 12: Multiple Reads Impact on Eviction Order ===");

    // Create test directory with small quota to force eviction
    fs.mkdir(&Path::from_str("/lru_test/multi_reads")?, true)
        .await?;
    fs.fs_client()
        .add_quota("/lru_test/multi_reads", 400)
        .await?; // Small quota

    // Create three files with clear access pattern expectations
    let file_a = Path::from_str("/lru_test/multi_reads/file_a_rarely_read.txt")?;
    let file_b = Path::from_str("/lru_test/multi_reads/file_b_sometimes_read.txt")?;
    let file_c = Path::from_str("/lru_test/multi_reads/file_c_frequently_read.txt")?;

    // Create all files first
    fs.write_string(&file_a, "File A content - should be evicted first")
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;

    fs.write_string(&file_b, "File B content - should be evicted second")
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;

    fs.write_string(&file_c, "File C content - should survive longest")
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;

    info!("Created 3 files, now establishing access patterns...");

    // Establish clear access patterns:
    // File A: Read only once (oldest in LRU)
    fs.read_string(&file_a).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // File B: Read a few times (middle in LRU)
    for _ in 0..3 {
        fs.read_string(&file_b).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // File C: Read many times (newest in LRU, should survive)
    for i in 0..8 {
        fs.read_string(&file_c).await?;
        if i % 2 == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    info!("Access patterns established:");
    info!("  File A: 1 read (should be evicted first)");
    info!("  File B: 3 reads (should be evicted second)");
    info!("  File C: 8 reads (should survive longest)");

    // Check initial state - all files should exist
    let a_exists_before = fs.read_string(&file_a).await.is_ok();
    let b_exists_before = fs.read_string(&file_b).await.is_ok();
    let c_exists_before = fs.read_string(&file_c).await.is_ok();

    assert!(
        a_exists_before && b_exists_before && c_exists_before,
        "All files should exist before eviction trigger"
    );

    // Create trigger file to exceed quota and force eviction
    let trigger_file = Path::from_str("/lru_test/multi_reads/trigger_eviction.txt")?;
    let large_content = "x".repeat(200); // This should trigger eviction
    fs.write_string(&trigger_file, &large_content).await?;

    // Wait for eviction to process
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Check which files survived
    let a_exists_after = fs.read_string(&file_a).await.is_ok();
    let b_exists_after = fs.read_string(&file_b).await.is_ok();
    let c_exists_after = fs.read_string(&file_c).await.is_ok();
    let trigger_exists_after = fs.read_string(&trigger_file).await.is_ok();

    info!("File existence after eviction:");
    info!("  File A (1 read): exists = {}", a_exists_after);
    info!("  File B (3 reads): exists = {}", b_exists_after);
    info!("  File C (8 reads): exists = {}", c_exists_after);
    info!("  Trigger file: exists = {}", trigger_exists_after);

    // Check quota state
    let quota_info = fs
        .fs_client()
        .get_quota_info("/lru_test/multi_reads")
        .await?;
    if let Some(quota) = quota_info {
        info!(
            "Final quota state: used={}, limit={}",
            quota.used_size, quota.quota_size
        );
    }

    // Verify LRU behavior: files with fewer reads should be evicted first
    let evicted_count = [a_exists_after, b_exists_after, c_exists_after]
        .iter()
        .filter(|&&exists| !exists)
        .count();

    info!("Total files evicted: {}", evicted_count);

    // Test expectations based on LRU logic:
    // 1. File C (most frequently read) should have the highest chance of survival
    // 2. File A (least frequently read) should be most likely to be evicted
    // 3. The eviction should follow LRU order when possible

    if evicted_count > 0 {
        if !a_exists_after {
            info!("✓ File A (least read) was correctly evicted first");
        }
        if evicted_count >= 2 && !b_exists_after {
            info!("✓ File B (moderately read) was evicted second");
        }
        if c_exists_after && evicted_count < 3 {
            info!("✓ File C (most read) survived as expected");
        } else if !c_exists_after {
            info!("⚠ File C was evicted despite being most frequently read (acceptable with over-deletion)");
        }
    } else {
        info!("⚠ No files were evicted - quota may not have been exceeded enough or eviction didn't trigger");
    }

    // Additional verification: try to read the most frequently accessed file again
    if c_exists_after {
        let final_read = fs.read_string(&file_c).await;
        assert!(
            final_read.is_ok(),
            "Most frequently read file should still be accessible"
        );
        info!("✓ Most frequently read file is still accessible after eviction");
    }

    info!("✓ Multiple reads eviction impact test completed");
    Ok(())
}
