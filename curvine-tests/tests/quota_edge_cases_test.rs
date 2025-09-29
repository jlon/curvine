use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::Path;
use curvine_server::test::MiniCluster;
use log::info;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::sync::Arc;

#[test]
fn quota_edge_cases_test() -> CommonResult<()> {
    // 创建集群配置
    let mut conf = ClusterConf::default();
    conf.client.block_size = 64 * 1024;
    conf.master.min_block_size = 64 * 1024;

    // 配置 worker 数据目录
    conf.worker.data_dir = vec!["/tmp/curvine-test-data-edge".to_string()];

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
        let test_root = Path::from_str("/quota_edge_test")?;
        let _ = fs.delete(&test_root, true).await;

        info!("=== Test 1: 配额跟踪验证 ===");

        // 创建配额目录
        let quota_dir = Path::from_str("/quota_edge_test/tracking_quota")?;
        fs.mkdir(&quota_dir, true).await?;

        // 设置配额
        fs.fs_client()
            .add_quota(quota_dir.path(), 10 * 1024)
            .await?; // 10KB

        // 写入文件并验证配额跟踪
        let test_file = Path::from_str("/quota_edge_test/tracking_quota/test_file.txt")?;
        let test_data = "Test quota tracking data";
        fs.write_string(&test_file, test_data).await?;

        // 验证配额跟踪正确
        let quota_info = fs.fs_client().get_quota_info(quota_dir.path()).await?;
        if let Some(info) = quota_info {
            assert_eq!(
                info.used_size,
                test_data.len() as i64,
                "配额跟踪不正确: expected={}, actual={}",
                test_data.len(),
                info.used_size
            );
            info!("✓ 配额跟踪工作正常，使用量: {} bytes", info.used_size);
        } else {
            panic!("无法获取配额信息");
        }

        info!("=== Test 2: 深层嵌套目录配额传播 ===");

        // 创建20层深的目录结构
        let mut deep_path = "/quota_edge_test/deep".to_string();
        for i in 1..=20 {
            deep_path = format!("{}/level_{}", deep_path, i);
        }
        let deep_dir = Path::from_str(&deep_path)?;
        fs.mkdir(&deep_dir, true).await?;

        // 在根目录设置配额
        let quota_root = Path::from_str("/quota_edge_test/deep")?;
        fs.fs_client()
            .add_quota(quota_root.path(), 10 * 1024 * 1024)
            .await?; // 10MB

        // 在最深层创建文件
        let deep_file = Path::from_str(&format!("{}/deep_file.txt", deep_path))?;
        let deep_data = "Deep file content";
        fs.write_string(&deep_file, deep_data).await?;

        // 验证配额传播到根目录
        let quota_info = fs.fs_client().get_quota_info(quota_root.path()).await?;
        if let Some(info) = quota_info {
            assert_eq!(
                info.used_size,
                deep_data.len() as i64,
                "深层嵌套目录的配额传播失败: expected={}, actual={}",
                deep_data.len(),
                info.used_size
            );
            info!("✓ 深层嵌套目录配额传播正确: {} bytes", info.used_size);
        } else {
            panic!("无法获取深层目录配额信息");
        }

        info!("=== Test 3: 跨配额目录的文件移动 ===");

        // 创建两个配额目录
        let quota_a = Path::from_str("/quota_edge_test/quota_a")?;
        let quota_b = Path::from_str("/quota_edge_test/quota_b")?;
        fs.mkdir(&quota_a, true).await?;
        fs.mkdir(&quota_b, true).await?;

        fs.fs_client()
            .add_quota(quota_a.path(), 5 * 1024 * 1024)
            .await?; // 5MB
        fs.fs_client()
            .add_quota(quota_b.path(), 5 * 1024 * 1024)
            .await?; // 5MB

        // 在quota_a中创建文件
        let file_in_a = Path::from_str("/quota_edge_test/quota_a/movable_file.txt")?;
        let file_data = "This file will be moved";
        fs.write_string(&file_in_a, file_data).await?;

        // 验证quota_a的使用量
        let quota_a_info_before = fs.fs_client().get_quota_info(quota_a.path()).await?;
        let quota_b_info_before = fs.fs_client().get_quota_info(quota_b.path()).await?;

        assert_eq!(
            quota_a_info_before.as_ref().unwrap().used_size,
            file_data.len() as i64
        );
        assert_eq!(quota_b_info_before.as_ref().unwrap().used_size, 0);

        // 将文件移动到quota_b
        let file_in_b = Path::from_str("/quota_edge_test/quota_b/moved_file.txt")?;
        fs.rename(&file_in_a, &file_in_b).await?;

        // 验证移动后的配额变化
        let quota_a_info_after = fs.fs_client().get_quota_info(quota_a.path()).await?;
        let quota_b_info_after = fs.fs_client().get_quota_info(quota_b.path()).await?;

        assert_eq!(
            quota_a_info_after.as_ref().unwrap().used_size,
            0,
            "源配额目录应该减少: expected=0, actual={}",
            quota_a_info_after.as_ref().unwrap().used_size
        );
        assert_eq!(
            quota_b_info_after.as_ref().unwrap().used_size,
            file_data.len() as i64,
            "目标配额目录应该增加: expected={}, actual={}",
            file_data.len(),
            quota_b_info_after.as_ref().unwrap().used_size
        );

        info!("✓ 跨配额目录文件移动配额更新正确");

        info!("=== Test 4: 嵌套配额目录 ===");

        // 创建父子配额目录
        let parent_quota = Path::from_str("/quota_edge_test/parent_quota")?;
        let child_quota = Path::from_str("/quota_edge_test/parent_quota/child_quota")?;
        fs.mkdir(&parent_quota, true).await?;
        fs.mkdir(&child_quota, true).await?;

        fs.fs_client()
            .add_quota(parent_quota.path(), 10 * 1024 * 1024)
            .await?; // 10MB
        fs.fs_client()
            .add_quota(child_quota.path(), 2 * 1024 * 1024)
            .await?; // 2MB

        // 在子配额目录中创建文件
        let child_file =
            Path::from_str("/quota_edge_test/parent_quota/child_quota/nested_file.txt")?;
        let nested_data = "Nested quota file content";
        fs.write_string(&child_file, nested_data).await?;

        // 验证父子配额都被正确更新
        let parent_info = fs.fs_client().get_quota_info(parent_quota.path()).await?;
        let child_info = fs.fs_client().get_quota_info(child_quota.path()).await?;

        assert_eq!(
            child_info.as_ref().unwrap().used_size,
            nested_data.len() as i64,
            "子配额目录使用量不正确"
        );
        assert_eq!(
            parent_info.as_ref().unwrap().used_size,
            nested_data.len() as i64,
            "父配额目录使用量不正确"
        );

        info!("✓ 嵌套配额目录更新正确");

        info!("=== Test 5: 并发文件操作的配额一致性 ===");

        // 创建并发测试目录
        let concurrent_quota = Path::from_str("/quota_edge_test/concurrent")?;
        fs.mkdir(&concurrent_quota, true).await?;
        fs.fs_client()
            .add_quota(concurrent_quota.path(), 50 * 1024 * 1024)
            .await?; // 50MB

        // 模拟并发创建100个小文件
        let mut tasks = Vec::new();
        for i in 0..100 {
            let fs_clone = fs.clone();
            let task = async move {
                let file_path = Path::from_str(&format!(
                    "/quota_edge_test/concurrent/concurrent_file_{}.txt",
                    i
                ))?;
                let file_content = format!("Concurrent file {} content", i);
                fs_clone.write_string(&file_path, &file_content).await
            };
            tasks.push(task);
        }

        // 等待所有任务完成
        for task in tasks {
            task.await?;
        }

        // 验证最终配额使用量的一致性
        let final_quota_info = fs
            .fs_client()
            .get_quota_info(concurrent_quota.path())
            .await?;
        if let Some(info) = final_quota_info {
            // 每个文件大约25字节，100个文件约2500字节
            let expected_min = 2000; // 允许一些误差
            let expected_max = 3000;

            assert!(
                info.used_size >= expected_min && info.used_size <= expected_max,
                "并发操作后配额使用量异常: expected=[{}-{}], actual={}",
                expected_min,
                expected_max,
                info.used_size
            );

            info!("✓ 并发操作后配额一致性正确: {} bytes", info.used_size);
        }

        Ok(())
    });

    match res {
        Ok(_) => {
            info!("✅ 所有边缘情况测试通过");
            Ok(())
        }
        Err(e) => {
            eprintln!("❌ 边缘情况测试失败: {:?}", e);
            Err(e)
        }
    }
}
