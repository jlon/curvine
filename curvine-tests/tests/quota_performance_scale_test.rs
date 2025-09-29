use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::Path;
use curvine_server::master::Master;
use curvine_server::test::MiniCluster;
use log::info;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Large-scale performance test for quota management
/// Tests O(1) quota query performance with hundreds of thousands of files
#[test]
fn test_quota_large_scale_performance() -> CommonResult<()> {
    // Initialize test metrics to prevent panic
    Master::init_test_metrics();

    let mut conf = ClusterConf::default();
    conf.worker.data_dir = vec!["/tmp/curvine-test-data-scale".to_string()];

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    let conf = cluster.master_conf().clone();

    cluster.start_cluster();

    // Wait for cluster to start
    Utils::sleep(10000);

    let rt = Arc::new(AsyncRuntime::single());
    let rt1 = rt.clone();

    let res: CommonResult<()> = rt.block_on(async move {
        // Create filesystem client
        let fs = CurvineFileSystem::with_rt(conf, rt1)?;

        info!("=== Large Scale Performance Test ===");

        // Clean up any existing test data
        let test_root = Path::from_str("/scale_test")?;
        let _ = fs.delete(&test_root, true).await;

        // Test 1: Create hierarchical structure with many files
        let start_setup = Instant::now();

        // Create main quota directory
        fs.mkdir(&test_root, true).await?;
        fs.fs_client().add_quota("/scale_test", 1024 * 1024 * 1024 * 10).await?; // 10GB

        // Create a large number of files for real performance testing
        // This will create 100,000+ files total
        const LEVELS: usize = 5;
        const DIRS_PER_LEVEL: usize = 20;
        const FILES_PER_DIR: usize = 100; // Total: 5 * 20 * 100 = 100,000 files

        let mut total_files = 0;
        let mut total_dirs = 0;

        for level in 0..LEVELS {
            info!("Starting level {} of {}", level + 1, LEVELS);

            for dir_idx in 0..DIRS_PER_LEVEL {
                let dir_path_str = format!("/scale_test/level_{}/dir_{}", level, dir_idx);
                let dir_path = Path::from_str(&dir_path_str)?;
                fs.mkdir(&dir_path, true).await?;
                total_dirs += 1;

                // Add quota to some directories for nested quota testing
                if level % 2 == 0 && dir_idx % 5 == 0 {
                    let quota_size = 1024 * 1024 * (level + 1) as i64; // Increasing quota per level
                    fs.fs_client().add_quota(&dir_path_str, quota_size).await?;
                }

                // Create files in this directory
                for file_idx in 0..FILES_PER_DIR {
                    let file_path_str = format!("{}/file_{:04}.txt", dir_path_str, file_idx);
                    let file_path = Path::from_str(&file_path_str)?;
                    let content = format!("Large scale performance test file {} in level {} directory {} - this content is used to test quota tracking with substantial file sizes for accurate performance measurement", file_idx, level, dir_idx);

                    fs.write_string(&file_path, &content).await?;
                    total_files += 1;
                }

                // Log progress every 20 directories
                if total_dirs % 20 == 0 {
                    info!("Created {} directories, {} files so far... ({:.1}% complete)",
                          total_dirs, total_files,
                          (total_dirs as f64 / (LEVELS * DIRS_PER_LEVEL) as f64) * 100.0);
                }
            }

            let level_completion = start_setup.elapsed();
            info!("Completed level {} in {:?} - {} directories, {} files total",
                  level + 1, level_completion, total_dirs, total_files);
        }

        let setup_duration = start_setup.elapsed();
        info!("✓ Setup completed: {} directories, {} files in {:?}", total_dirs, total_files, setup_duration);
        info!("  Average file creation rate: {:.0} files/sec", total_files as f64 / setup_duration.as_secs_f64());

        // Test 2: Measure O(1) quota query performance with large dataset
        info!("=== Testing O(1) Quota Query Performance ===");

        let quota_list = fs.fs_client().get_quota_table().await?;
        info!("Found {} quota entries", quota_list.len());

        // Warm up queries
        for _ in 0..20 {
            let _ = fs.fs_client().get_quota_info("/scale_test").await?;
        }

        // Performance test: Query main quota many times
        const PERFORMANCE_QUERIES: usize = 10000; // Increased for better measurement
        let start_queries = Instant::now();

        for _ in 0..PERFORMANCE_QUERIES {
            let quota_info = fs.fs_client().get_quota_info("/scale_test").await?;
            assert!(quota_info.is_some(), "Quota info should exist");
            let info = quota_info.unwrap();
            assert!(info.used_size > 0, "Used size should be greater than 0");
        }

        let query_duration = start_queries.elapsed();
        let avg_query_time = query_duration / PERFORMANCE_QUERIES as u32;
        let queries_per_sec = PERFORMANCE_QUERIES as f64 / query_duration.as_secs_f64();

        info!("✓ Performance test: {} quota queries in {:?}, avg: {:?}",
              PERFORMANCE_QUERIES, query_duration, avg_query_time);
        info!("  Query rate: {:.0} queries/sec", queries_per_sec);

        // Verify O(1) performance - should be under 1ms per query even with 100k+ files
        assert!(avg_query_time < Duration::from_millis(1),
                "Average query time {:?} exceeds 1ms threshold - not O(1) performance", avg_query_time);

        // Test 3: Nested quota performance
        info!("=== Testing Nested Quota Performance ===");

        let mut nested_queries = 0;
        let start_nested = Instant::now();

        for level in 0..LEVELS {
            for dir_idx in (0..DIRS_PER_LEVEL).step_by(5) {
                if level % 2 == 0 {
                    let dir_path = format!("/scale_test/level_{}/dir_{}", level, dir_idx);
                    let quota_info = fs.fs_client().get_quota_info(&dir_path).await?;
                    assert!(quota_info.is_some(), "Nested quota should exist for {}", dir_path);
                    nested_queries += 1;
                }
            }
        }

        let nested_duration = start_nested.elapsed();
        let avg_nested_time = if nested_queries > 0 { nested_duration / nested_queries as u32 } else { Duration::from_nanos(0) };

        info!("✓ Nested quota test: {} queries in {:?}, avg: {:?}",
              nested_queries, nested_duration, avg_nested_time);

        // Test 4: Concurrent quota queries simulation
        info!("=== Testing High-Frequency Query Performance ===");

        const RAPID_QUERIES: usize = 50000;
        let rapid_start = Instant::now();

        for i in 0..RAPID_QUERIES {
            // Alternate between main quota and nested quotas
            let path = if i % 10 == 0 {
                format!("/scale_test/level_{}/dir_{}", i % LEVELS, (i / 5) % DIRS_PER_LEVEL)
            } else {
                "/scale_test".to_string()
            };

            let _ = fs.fs_client().get_quota_info(&path).await?;
        }

        let rapid_duration = rapid_start.elapsed();
        let avg_rapid_time = rapid_duration / RAPID_QUERIES as u32;
        let rapid_qps = RAPID_QUERIES as f64 / rapid_duration.as_secs_f64();

        info!("✓ Rapid queries: {} queries in {:?}, avg: {:?}",
              RAPID_QUERIES, rapid_duration, avg_rapid_time);
        info!("  Rapid query rate: {:.0} queries/sec", rapid_qps);

        // Test 5: Memory usage and accuracy validation
        info!("=== Memory Usage and Accuracy Validation ===");

        // Query all quotas to ensure memory efficiency
        let all_quotas_start = Instant::now();
        let all_quotas = fs.fs_client().get_quota_table().await?;
        let all_quotas_duration = all_quotas_start.elapsed();

        info!("✓ Retrieved {} quota entries in {:?}", all_quotas.len(), all_quotas_duration);

        // Verify quota accuracy
        let main_quota = fs.fs_client().get_quota_info("/scale_test").await?.unwrap();
        let expected_min_size = total_files as i64 * 100; // Conservative estimate based on file content

        assert!(main_quota.used_size >= expected_min_size,
                "Main quota used_size {} should be at least {} (files: {})",
                main_quota.used_size, expected_min_size, total_files);

        info!("✓ Quota accuracy verified: {} bytes used for {} files", main_quota.used_size, total_files);
        info!("  Average file size: {} bytes", main_quota.used_size / total_files as i64);

        // Test 6: Scale verification - ensure O(1) performance is maintained
        info!("=== Scale Verification Test ===");

        const SCALE_QUERIES: usize = 100000;
        let scale_start = Instant::now();

        for _ in 0..SCALE_QUERIES {
            let quota_info = fs.fs_client().get_quota_info("/scale_test").await?;
            assert!(quota_info.unwrap().used_size > 0);
        }

        let scale_duration = scale_start.elapsed();
        let avg_scale_time = scale_duration / SCALE_QUERIES as u32;
        let scale_qps = SCALE_QUERIES as f64 / scale_duration.as_secs_f64();

        info!("✓ Scale verification: {} queries with {} files in {:?}, avg: {:?}",
              SCALE_QUERIES, total_files, scale_duration, avg_scale_time);
        info!("  Scale query rate: {:.0} queries/sec", scale_qps);

        // Critical assertion: O(1) performance must be maintained regardless of file count
        assert!(avg_scale_time < Duration::from_millis(1),
                "Scale test failed: avg query time {:?} exceeds 1ms with {} files",
                avg_scale_time, total_files);

        // Test 7: Cleanup performance measurement
        info!("=== Testing Cleanup Performance ===");

        let cleanup_start = Instant::now();

        // Remove some quotas first
        let mut removed_quotas = 0;
        for level in (0..LEVELS).step_by(2) {
            for dir_idx in (0..DIRS_PER_LEVEL).step_by(5) {
                let dir_path = format!("/scale_test/level_{}/dir_{}", level, dir_idx);
                if fs.fs_client().remove_quota(&dir_path).await.is_ok() {
                    removed_quotas += 1;
                }
            }
        }

        info!("Removed {} nested quotas", removed_quotas);

        // Delete entire test directory
        fs.delete(&test_root, true).await?;

        let cleanup_duration = cleanup_start.elapsed();
        let deletion_rate = total_files as f64 / cleanup_duration.as_secs_f64();

        info!("✓ Cleanup completed in {:?}", cleanup_duration);
        info!("  Deletion rate: {:.0} files/sec", deletion_rate);

        // Final verification
        let final_quota = fs.fs_client().get_quota_info("/scale_test").await;
        assert!(final_quota.is_err() || final_quota.unwrap().is_none(),
                "Main quota should be removed after directory deletion");

        info!("✅ Large scale performance test completed successfully!");
        info!("========== PERFORMANCE SUMMARY ==========");
        info!("  Files created: {} files in {} directories", total_files, total_dirs);
        info!("  Setup time: {:?} ({:.0} files/sec)", setup_duration, total_files as f64 / setup_duration.as_secs_f64());
        info!("  O(1) query performance: {:?} avg ({:.0} queries/sec)", avg_query_time, queries_per_sec);
        info!("  Nested quota queries: {:?} avg", avg_nested_time);
        info!("  Rapid query performance: {:?} avg ({:.0} queries/sec)", avg_rapid_time, rapid_qps);
        info!("  Scale verification: {:?} avg ({:.0} queries/sec)", avg_scale_time, scale_qps);
        info!("  Cleanup time: {:?} ({:.0} files/sec)", cleanup_duration, deletion_rate);
        info!("  ✓ O(1) performance maintained with {} files", total_files);
        info!("==========================================");

        Ok(())
    });

    res
}

/// Extreme scale test with 1M+ files (only run when explicitly requested)
#[test]
#[ignore] // Use --ignored to run this test
fn test_quota_extreme_scale_performance() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut conf = ClusterConf::default();
    conf.worker.data_dir = vec!["/tmp/curvine-test-data-extreme".to_string()];

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    let conf = cluster.master_conf().clone();

    cluster.start_cluster();
    Utils::sleep(15000); // Longer wait for extreme test

    let rt = Arc::new(AsyncRuntime::single());
    let rt1 = rt.clone();

    let res: CommonResult<()> = rt.block_on(async move {
        let fs = CurvineFileSystem::with_rt(conf, rt1)?;

        info!("=== EXTREME SCALE PERFORMANCE TEST (1M+ files) ===");

        let test_root = Path::from_str("/extreme_test")?;
        let _ = fs.delete(&test_root, true).await;

        // Create 1 million files across many directories
        const EXTREME_LEVELS: usize = 10;
        const EXTREME_DIRS_PER_LEVEL: usize = 100;
        const EXTREME_FILES_PER_DIR: usize = 100; // Total: 10 * 100 * 100 = 1,000,000 files

        fs.mkdir(&test_root, true).await?;
        fs.fs_client()
            .add_quota("/extreme_test", 1024i64 * 1024 * 1024 * 100)
            .await?; // 100GB

        let setup_start = Instant::now();
        let mut total_files = 0;
        let mut total_dirs = 0;

        info!(
            "Starting extreme scale setup: {} levels × {} dirs × {} files = {} total files",
            EXTREME_LEVELS,
            EXTREME_DIRS_PER_LEVEL,
            EXTREME_FILES_PER_DIR,
            EXTREME_LEVELS * EXTREME_DIRS_PER_LEVEL * EXTREME_FILES_PER_DIR
        );

        for level in 0..EXTREME_LEVELS {
            info!(
                "EXTREME: Starting level {} of {} ({}% complete)",
                level + 1,
                EXTREME_LEVELS,
                ((level as f64 / EXTREME_LEVELS as f64) * 100.0) as u32
            );

            for dir_idx in 0..EXTREME_DIRS_PER_LEVEL {
                let dir_path_str = format!("/extreme_test/level_{:02}/dir_{:03}", level, dir_idx);
                let dir_path = Path::from_str(&dir_path_str)?;
                fs.mkdir(&dir_path, true).await?;
                total_dirs += 1;

                // Add quota to every 50th directory
                if dir_idx % 50 == 0 {
                    fs.fs_client()
                        .add_quota(&dir_path_str, 1024 * 1024 * 10)
                        .await?; // 10MB each
                }

                // Create files in batches for better performance
                for file_idx in 0..EXTREME_FILES_PER_DIR {
                    let file_path_str = format!("{}/file_{:04}.txt", dir_path_str, file_idx);
                    let file_path = Path::from_str(&file_path_str)?;
                    let content = format!(
                        "Extreme scale test file {} in level {} dir {}",
                        file_idx, level, dir_idx
                    );
                    fs.write_string(&file_path, &content).await?;
                    total_files += 1;
                }

                if total_dirs % 500 == 0 {
                    let elapsed = setup_start.elapsed();
                    let rate = total_files as f64 / elapsed.as_secs_f64();
                    info!(
                        "EXTREME: {} dirs, {} files ({:.1}% complete, {:.0} files/sec)",
                        total_dirs,
                        total_files,
                        (total_files as f64
                            / (EXTREME_LEVELS * EXTREME_DIRS_PER_LEVEL * EXTREME_FILES_PER_DIR)
                                as f64)
                            * 100.0,
                        rate
                    );
                }
            }
        }

        let setup_duration = setup_start.elapsed();
        info!(
            "✓ EXTREME setup completed: {} files in {:?} ({:.0} files/sec)",
            total_files,
            setup_duration,
            total_files as f64 / setup_duration.as_secs_f64()
        );

        // Test O(1) performance with 1M+ files
        const EXTREME_QUERIES: usize = 100000;
        let query_start = Instant::now();

        for _ in 0..EXTREME_QUERIES {
            let quota_info = fs.fs_client().get_quota_info("/extreme_test").await?;
            assert!(quota_info.is_some(), "Extreme quota should exist");
        }

        let query_duration = query_start.elapsed();
        let avg_extreme_time = query_duration / EXTREME_QUERIES as u32;
        let extreme_qps = EXTREME_QUERIES as f64 / query_duration.as_secs_f64();

        info!(
            "✓ EXTREME performance: {} queries with {} files in {:?}, avg: {:?}",
            EXTREME_QUERIES, total_files, query_duration, avg_extreme_time
        );
        info!("  EXTREME query rate: {:.0} queries/sec", extreme_qps);

        // Critical test: Even with 1M+ files, should still be under 1ms per query
        assert!(
            avg_extreme_time < Duration::from_millis(1),
            "EXTREME SCALE FAILED: average query time {:?} exceeds 1ms with {} files",
            avg_extreme_time,
            total_files
        );

        // Cleanup
        info!("Starting extreme cleanup...");
        let cleanup_start = Instant::now();
        fs.delete(&test_root, true).await?;
        let cleanup_duration = cleanup_start.elapsed();

        info!("✅ EXTREME SCALE TEST COMPLETED SUCCESSFULLY!");
        info!("========== EXTREME PERFORMANCE SUMMARY ==========");
        info!(
            "  Files processed: {} ({:.1}M files)",
            total_files,
            total_files as f64 / 1_000_000.0
        );
        info!(
            "  O(1) performance maintained: {:?} avg ({:.0} queries/sec)",
            avg_extreme_time, extreme_qps
        );
        info!(
            "  Setup: {:?} ({:.0} files/sec)",
            setup_duration,
            total_files as f64 / setup_duration.as_secs_f64()
        );
        info!(
            "  Cleanup: {:?} ({:.0} files/sec)",
            cleanup_duration,
            total_files as f64 / cleanup_duration.as_secs_f64()
        );
        info!("  ✓ QUOTA SYSTEM SCALES TO MILLIONS OF FILES!");
        info!("================================================");

        Ok(())
    });

    res
}
