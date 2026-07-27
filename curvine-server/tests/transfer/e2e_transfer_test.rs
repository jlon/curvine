use std::collections::HashSet;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use curvine_client::file::CurvineFileSystem;
use curvine_client::rpc::{JobMasterClient, TransferClient};
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::{ClusterConf, TransferCvMetadataReaderType, TransferStoreType};
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::proto::{
    GetTransferStatusResponse, SubmitTransferRequest, TransferKindProto, TransferStateProto,
    TransferTaskStateProto,
};
use curvine_common::state::{
    JobTaskProgress, JobTaskState, LoadJobCommand, LoadJobInfo, LoadTaskInfo, MountInfo,
    MountOptions, StorageType, TaskAttemptStart, TransferCommand, TransferJobRecord, TransferKind,
    TransferProgress, TransferTaskRecord, TransferTaskReportInfo, TransferTaskState, TtlAction,
    WorkerAddress, WorkerInfo,
};
use curvine_common::utils::ProtoUtils;
use curvine_server::common::UfsFactory;
use curvine_server::test::MiniCluster;
use curvine_server::transfer::{
    ClusterMetadataCache, CvMetadataReader, MemoryTransferStore, MetadataReplicaReader,
    MysqlTransferStore, SqliteTransferStore, TransferServer, TransferServerShutdown,
    TransferService, TransferStore,
};
use curvine_server::worker::task::{LoadTaskRunner, TaskContext};
use mysql::params;
use mysql::prelude::*;
use orpc::io::net::NetUtils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};

#[test]
fn test_transfer_server_requires_enabled_config() {
    let conf = ClusterConf::default();
    let err = match TransferServer::with_conf(conf) {
        Ok(_) => panic!("transfer server should reject disabled transfer config"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("curvine-transfer requires transfer.enabled=true"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_transfer_server_infers_local_report_endpoint() {
    let mut conf = ClusterConf::default();
    conf.transfer.enabled = true;
    conf.transfer.endpoints.clear();
    conf.transfer.init().unwrap();

    assert_eq!(conf.transfer.endpoints, vec!["localhost:9010"]);
}

#[test]
fn test_transfer_readyz_rejects_stale_cluster_snapshot() {
    let test_id = format!(
        "transfer-readyz-stale-snapshot-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let _ = fs::remove_dir_all(&base_dir);

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.cluster_snapshot_max_staleness_str = "200ms".to_string();
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();
    rt.block_on(async {
        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        let ready = read_http_path(cluster.cluster_conf.transfer.web_port, "/readyz");
        assert!(
            ready.contains("200 OK") && ready.ends_with("ok\n"),
            "transfer readiness should be ok after initial snapshot, response: {ready}"
        );

        tokio::time::sleep(Duration::from_millis(350)).await;
        let stale = read_http_path(cluster.cluster_conf.transfer.web_port, "/readyz");
        assert!(
            stale.contains("503 Service Unavailable")
                && stale.contains("Cluster metadata snapshot is stale"),
            "transfer readiness should reject stale cluster snapshot, response: {stale}"
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_server_does_not_fallback_when_mysql_store_is_unavailable() {
    let mut conf = ClusterConf::default();
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url =
        "mysql://root:curvine@127.0.0.1:1/curvine_transfer_unavailable".into();
    conf.transfer.endpoints = vec!["localhost:9010".to_string()];
    conf.transfer.init().unwrap();

    let err = match TransferServer::with_conf(conf) {
        Ok(_) => panic!("transfer server should fail when mysql store is unavailable"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("Transfer metadata store is unavailable"),
        "unexpected mysql unavailable error: {err}"
    );
}

#[test]
fn test_master_submit_job_is_disabled_when_transfer_is_enabled() {
    let test_id = format!(
        "transfer-master-submit-disabled-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = wait_master_and_create_fs(&cluster).await;
        let client = JobMasterClient::new(fs.fs_client());
        let err = client
            .submit_load_job(LoadJobCommand::builder("/mnt/file.txt").build())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Legacy Master SubmitJob is disabled because transfer.enabled=true"),
            "unexpected error: {err}"
        );

        let err = client
            .submit_export_job(LoadJobCommand::builder("/mnt/file.txt").build())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Legacy Master SubmitJob is disabled because transfer.enabled=true"),
            "legacy Master Export SubmitJob must be disabled: {err}"
        );

        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_submit_transfer_refreshes_missing_mount_snapshot() {
    let test_id = format!(
        "transfer-local-snapshot-submit-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let cache = ClusterMetadataCache::new(fs);
        let service = TransferService::with_cache(
            Arc::new(MemoryTransferStore::new()),
            cache.clone(),
            cluster.cluster_conf.transfer.task_stale_timeout,
        );
        let request = SubmitTransferRequest {
            kind: TransferKindProto::TransferLoad as i32,
            source_path: format!("file://{}/snapshot.txt", ufs_dir.display()),
            target_path: "/mnt/snapshot.txt".to_string(),
            client_request_id: test_id.clone(),
            submitter: "snapshot-submit".to_string(),
            tenant: "test".to_string(),
            command: Vec::new(),
            protocol_version: Some(1),
        };

        let job = service.submit_transfer(request).unwrap();
        assert_eq!(job.target_path, "/mnt/snapshot.txt");
        assert!(
            job.cluster_snapshot_version > 0,
            "SubmitTransfer should persist the refreshed mount snapshot version"
        );
    });

    let _ = fs::remove_dir_all(base_dir);
}

#[test]
fn test_submit_transfer_rejects_stale_cluster_snapshot_by_default() {
    let test_id = format!(
        "transfer-stale-cluster-snapshot-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.cluster_snapshot_max_staleness_str = "1ms".to_string();
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let request = SubmitTransferRequest {
            kind: TransferKindProto::TransferLoad as i32,
            source_path: format!("file://{}/stale.txt", ufs_dir.display()),
            target_path: "/mnt/stale.txt".to_string(),
            client_request_id: format!("{test_id}-stale"),
            submitter: "stale-submit".to_string(),
            tenant: "test".to_string(),
            command: Vec::new(),
            protocol_version: Some(1),
        };

        let cache = ClusterMetadataCache::with_snapshot_policy(
            fs.clone(),
            cluster.cluster_conf.transfer.cluster_snapshot_max_staleness,
            false,
        );
        cache.refresh().await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let service = TransferService::with_cache(
            Arc::new(MemoryTransferStore::new()),
            cache,
            cluster.cluster_conf.transfer.task_stale_timeout,
        );
        let err = service.submit_transfer(request.clone()).unwrap_err();
        assert!(
            err.to_string()
                .contains("Cluster metadata snapshot is stale"),
            "SubmitTransfer should reject stale cluster snapshot by default: {err}"
        );

        let stale_allowed_cache = ClusterMetadataCache::with_snapshot_policy(
            fs,
            cluster.cluster_conf.transfer.cluster_snapshot_max_staleness,
            true,
        );
        stale_allowed_cache.refresh().await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let service = TransferService::with_cache(
            Arc::new(MemoryTransferStore::new()),
            stale_allowed_cache,
            cluster.cluster_conf.transfer.task_stale_timeout,
        );
        let mut allowed_request = request;
        allowed_request.client_request_id = format!("{test_id}-allowed");
        let job = service.submit_transfer(allowed_request).unwrap();
        assert_eq!(job.target_path, "/mnt/stale.txt");
    });

    let _ = fs::remove_dir_all(base_dir);
}

#[test]
fn test_transfer_load_file_end_to_end() {
    let test_id = format!(
        "transfer-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("hello.txt"), b"hello-transfer").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let mut transfer_server =
            InProcessTransferServer::start_liveness(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;
        let healthz = read_http_path(cluster.cluster_conf.transfer.web_port, "/healthz");
        assert!(
            healthz.ends_with("ok\n"),
            "transfer health endpoint should return ok, response: {healthz}"
        );
        let readyz = read_http_path(cluster.cluster_conf.transfer.web_port, "/readyz");
        assert!(
            readyz.ends_with("ok\n"),
            "transfer readiness endpoint should return ok after startup, response: {readyz}"
        );
        let startup_metrics = read_http_metrics(cluster.cluster_conf.transfer.web_port);
        assert!(
            startup_metrics.contains("transfer_task_report_queue_len"),
            "transfer metrics endpoint should expose transfer metrics"
        );
        assert!(
            startup_metrics.contains("transfer_task_report_queue_len_by_lane"),
            "transfer metrics endpoint should expose report lane metrics"
        );
        assert!(
            startup_metrics.contains("transfer_acquire_total"),
            "transfer metrics endpoint should expose scheduler metrics"
        );

        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let source = Path::from_str(format!("file://{}/hello.txt", ufs_dir.display())).unwrap();
        let target = Path::from_str("/mnt/hello.txt").unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: source.clone_uri(),
                target_path: target.clone_uri(),
                client_request_id: test_id.clone(),
                submitter: "e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let mut final_state = submit.state;
        for _ in 0..80 {
            let status = client
                .status_page(&submit.job_id, Some(10), None)
                .await
                .unwrap();
            final_state = status.state;
            if final_state == TransferStateProto::TransferCompleted as i32
                || final_state == TransferStateProto::TransferFailed as i32
            {
                assert!(!status.tasks.is_empty(), "status should return task page");
                assert_eq!(
                    status.cv_metadata_epoch, None,
                    "load planning should not persist a CV metadata snapshot epoch"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        assert_eq!(final_state, TransferStateProto::TransferCompleted as i32);
        let content = fs.read_string(&target).await.unwrap();
        assert_eq!(content, "hello-transfer");
        let ufs_content = fs::read_to_string(ufs_dir.join("hello.txt")).unwrap();
        assert_eq!(ufs_content, "hello-transfer");
        let completed_metrics = read_http_metrics(cluster.cluster_conf.transfer.web_port);
        assert!(
            completed_metrics.contains("transfer_store_operation_duration_us"),
            "transfer metrics endpoint should expose store operation latency"
        );
        assert!(
            completed_metrics.contains("transfer_store_unavailable"),
            "transfer metrics endpoint should expose store availability"
        );
        assert!(
            completed_metrics.contains("transfer_store_unavailable_duration_us_total"),
            "transfer metrics endpoint should expose store unavailable duration"
        );
        assert!(
            completed_metrics.contains("transfer_metadata_operation_duration_us"),
            "transfer metrics endpoint should expose metadata operation latency"
        );
        assert!(
            completed_metrics.contains("source=\"ufs\""),
            "transfer metadata metrics should record UFS planning operations"
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_unified_async_cache_waits_for_transfer_job() {
    let test_id = format!(
        "transfer-unified-wait-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("cached.txt"), b"unified-transfer-cache").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let unified = UnifiedFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        let target = Path::from_str("/mnt/cached.txt").unwrap();
        unified
            .mount(
                &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
                &Path::from_str("/mnt").unwrap(),
                MountOptions::builder().build(),
            )
            .await
            .unwrap();

        let mut transfer_server =
            InProcessTransferServer::start_liveness(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;

        unified.async_cache(&target).unwrap();
        unified.wait_job_complete(&target, true).await.unwrap();
        assert_eq!(
            unified.cv().read_string(&target).await.unwrap(),
            "unified-transfer-cache"
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_load_directory_uses_multiple_workers_end_to_end() {
    const FILE_COUNT: usize = 36;
    const PAYLOAD_BYTES: usize = 32 * 1024;

    let test_id = format!(
        "transfer-multi-worker-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    for shard in 0..3 {
        fs::create_dir_all(ufs_dir.join(format!("nested-{shard}"))).unwrap();
    }
    let mut expected_total_size = 0i64;
    for index in 0..FILE_COUNT {
        let shard = index % 3;
        let path = ufs_dir
            .join(format!("nested-{shard}"))
            .join(format!("file-{index}.txt"));
        let payload = format!(
            "multi-worker-content-{index}\n{}",
            "x".repeat(PAYLOAD_BYTES)
        );
        expected_total_size += payload.len() as i64;
        fs::write(path, payload).unwrap();
    }

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.cv_metadata_reader = TransferCvMetadataReaderType::Master;
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 3);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = wait_master_and_create_fs(&cluster).await;
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let mut transfer_server =
            InProcessTransferServer::start_liveness(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;
        let startup_metrics = read_http_metrics(cluster.cluster_conf.transfer.web_port);
        assert!(
            startup_metrics.contains("transfer_cluster_snapshot_version"),
            "transfer metrics should expose cluster snapshot version"
        );
        assert!(
            startup_metrics.contains("transfer_cluster_snapshot_staleness_ms"),
            "transfer metrics should expose cluster snapshot staleness"
        );
        assert!(
            startup_metrics.contains("transfer_cluster_snapshot_capable_workers"),
            "transfer metrics should expose capable worker count"
        );

        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}", ufs_dir.display()),
                target_path: "/mnt/multi-worker-load".to_string(),
                client_request_id: test_id.clone(),
                submitter: "multi-worker-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let status = wait_transfer_state(
            &client,
            &submit.job_id,
            TransferStateProto::TransferCompleted,
            Duration::from_secs(60),
        )
        .await;
        assert_eq!(
            status.tasks.len(),
            FILE_COUNT,
            "directory load should create one task per source file"
        );
        let worker_sessions = status
            .tasks
            .iter()
            .map(|task| (task.worker_id, task.worker_session_id.clone()))
            .collect::<HashSet<_>>();
        assert!(
            worker_sessions.len() == 3,
            "multi-worker load should execute on all three workers, got {:?}",
            worker_sessions
        );
        assert!(
            status
                .tasks
                .iter()
                .all(|task| task.state == TransferTaskStateProto::TransferTaskCompleted as i32),
            "all directory load tasks must complete: {:?}",
            status.tasks
        );

        assert_eq!(
            status.progress.total_size, expected_total_size,
            "transfer total size should match source bytes"
        );
        assert_eq!(
            status.progress.loaded_size, expected_total_size,
            "transfer loaded size should match source bytes"
        );

        for index in 0..FILE_COUNT {
            let shard = index % 3;
            let target = format!("/mnt/multi-worker-load/nested-{shard}/file-{index}.txt");
            let content = fs
                .read_string(&Path::from_str(target).unwrap())
                .await
                .unwrap();
            assert!(
                content.starts_with(&format!("multi-worker-content-{index}\n")),
                "unexpected copied content for file {index}"
            );
            assert_eq!(
                content.len(),
                "multi-worker-content-\n".len() + index.to_string().len() + PAYLOAD_BYTES
            );
        }

        let metrics = read_http_metrics(cluster.cluster_conf.transfer.web_port);
        assert!(
            metrics.contains("transfer_task_report_total"),
            "transfer metrics should include task report counters after multi-worker load"
        );
        assert!(
            metrics.contains("transfer_dispatch_total"),
            "transfer metrics should include dispatch counters after multi-worker load"
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
#[ignore = "stress e2e: run explicitly to validate load storm backlog, dispatch, reports, and data consistency"]
fn test_transfer_load_storm_keeps_jobs_pending_and_completes_without_master_job_path() {
    const JOB_COUNT: usize = 48;
    const FILES_PER_JOB: usize = 6;
    const PAYLOAD_BYTES: usize = 64 * 1024;

    let test_id = format!(
        "transfer-load-storm-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    for job_index in 0..JOB_COUNT {
        let job_dir = ufs_dir.join(format!("job-{job_index}"));
        fs::create_dir_all(&job_dir).unwrap();
        for file_index in 0..FILES_PER_JOB {
            let payload = format!(
                "load-storm-job-{job_index}-file-{file_index}\n{}",
                "x".repeat(PAYLOAD_BYTES)
            );
            fs::write(job_dir.join(format!("file-{file_index}.bin")), payload).unwrap();
        }
    }

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.cv_metadata_reader = TransferCvMetadataReaderType::Master;
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.max_running_transfers = 2;
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 3);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = wait_master_and_create_fs(&cluster).await;
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let store = Arc::new(SqliteTransferStore::open(&cluster.cluster_conf.transfer.sqlite_path).unwrap());
        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;
        let client = Arc::new(TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap());

        let submit_started = std::time::Instant::now();
        let mut handles = Vec::with_capacity(JOB_COUNT);
        for job_index in 0..JOB_COUNT {
            let client = client.clone();
            let source_path = format!("file://{}/job-{job_index}", ufs_dir.display());
            let target_path = format!("/mnt/load-storm/job-{job_index}");
            let client_request_id = format!("{test_id}-{job_index}");
            handles.push(tokio::spawn(async move {
                client
                    .submit(TransferCommand {
                        kind: TransferKind::Load,
                        source_path,
                        target_path,
                        client_request_id,
                        submitter: "load-storm-e2e".to_string(),
                        tenant: "stress".to_string(),
                        options: Default::default(),
                    })
                    .await
                    .unwrap()
                    .job_id
            }));
        }
        let mut job_ids = Vec::with_capacity(JOB_COUNT);
        for handle in handles {
            job_ids.push(handle.await.unwrap());
        }
        eprintln!(
            "submitted {JOB_COUNT} load jobs with {} total files in {:?}",
            JOB_COUNT * FILES_PER_JOB,
            submit_started.elapsed()
        );

        let legacy_client = JobMasterClient::new(fs.fs_client());
        let legacy_err = legacy_client
            .submit_load_job(LoadJobCommand::builder("/mnt/load-storm/legacy.txt").build())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            legacy_err.contains("Legacy Master SubmitJob is disabled because transfer.enabled=true"),
            "legacy Master SubmitJob must stay blocked during load storm: {legacy_err}"
        );

        let mut max_observed_executing = 0u64;
        let mut observed_pending_backlog = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(180);
        loop {
            let active = store.count_active_transfers().unwrap();
            let executing = store.count_executing_transfers().unwrap();
            max_observed_executing = max_observed_executing.max(executing);
            if active > executing {
                observed_pending_backlog = true;
            }

            let mut completed = 0usize;
            for job_id in &job_ids {
                let status = client.status_page(job_id, Some(10), None).await.unwrap();
                if status.state == TransferStateProto::TransferCompleted as i32 {
                    completed += 1;
                }
            }
            if completed == JOB_COUNT {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "load storm did not complete in time: completed={completed}/{JOB_COUNT}, active={active}, executing={executing}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        assert!(
            observed_pending_backlog,
            "load storm should create Pending backlog when max_running_transfers is lower than submitted jobs"
        );
        assert!(
            max_observed_executing <= cluster.cluster_conf.transfer.max_running_transfers as u64,
            "executing transfers exceeded configured window: observed={}, limit={}",
            max_observed_executing,
            cluster.cluster_conf.transfer.max_running_transfers
        );

        let mut worker_sessions = HashSet::new();
        for (job_index, job_id) in job_ids.iter().enumerate() {
            let status = client
                .status_page(job_id, Some(FILES_PER_JOB as u32 + 1), None)
                .await
                .unwrap();
            assert_eq!(
                status.state,
                TransferStateProto::TransferCompleted as i32,
                "job {job_id} should complete"
            );
            assert_eq!(
                status.tasks.len(),
                FILES_PER_JOB,
                "job {job_id} should have one task per source file"
            );
            for task in &status.tasks {
                assert_eq!(
                    task.state,
                    TransferTaskStateProto::TransferTaskCompleted as i32
                );
                worker_sessions.insert((task.worker_id, task.worker_session_id.clone()));
            }
            for file_index in 0..FILES_PER_JOB {
                let target =
                    Path::from_str(format!("/mnt/load-storm/job-{job_index}/file-{file_index}.bin"))
                        .unwrap();
                let content = fs.read_string(&target).await.unwrap();
                assert!(
                    content.starts_with(&format!("load-storm-job-{job_index}-file-{file_index}\n")),
                    "unexpected copied content for {target}"
                );
                assert_eq!(
                    content.len(),
                    format!("load-storm-job-{job_index}-file-{file_index}\n").len()
                        + PAYLOAD_BYTES
                );
            }
        }
        assert!(
            worker_sessions.len() >= 2,
            "load storm should use multiple workers, got {:?}",
            worker_sessions
        );

        let metrics = read_http_metrics(cluster.cluster_conf.transfer.web_port);
        for metric in [
            "transfer_submit_total",
            "transfer_pending_jobs",
            "transfer_executing_jobs",
            "transfer_dispatch_total",
            "transfer_task_report_total",
        ] {
            assert!(
                metrics.contains(metric),
                "transfer metrics should expose {metric} after load storm"
            );
        }

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_recovers_completed_task_when_final_report_is_lost() {
    let test_id = format!(
        "transfer-lost-final-report-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("lost-report.txt"), b"lost-final-report").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.instance_id = "transfer-child-recover".to_string();
    let actual_transfer_endpoint = format!("{}:{}", conf.transfer.hostname, conf.transfer.rpc_port);
    conf.transfer.endpoints = vec!["localhost:1".to_string()];
    conf.transfer.lease_timeout_str = "1s".to_string();
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut client_conf = cluster.cluster_conf.clone();
        client_conf.transfer.endpoints = vec![actual_transfer_endpoint];
        let client = TransferClient::with_rt(&client_conf, rt.clone()).unwrap();
        let target = Path::from_str("/mnt/lost-report.txt").unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}/lost-report.txt", ufs_dir.display()),
                target_path: target.clone_uri(),
                client_request_id: test_id.clone(),
                submitter: "lost-final-report-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let mut final_state = submit.state;
        let mut task_state = 0;
        for _ in 0..120 {
            let status = client
                .status_page(&submit.job_id, Some(10), None)
                .await
                .unwrap();
            final_state = status.state;
            if let Some(task) = status.tasks.first() {
                task_state = task.state;
            }
            if final_state == TransferStateProto::TransferCompleted as i32
                || final_state == TransferStateProto::TransferFailed as i32
            {
                assert!(!status.tasks.is_empty(), "status should return task page");
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        assert_eq!(final_state, TransferStateProto::TransferCompleted as i32);
        assert_eq!(
            task_state,
            TransferTaskStateProto::TransferTaskCompleted as i32
        );
        assert_eq!(fs.read_string(&target).await.unwrap(), "lost-final-report");

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_cancel_pending_job_end_to_end() {
    let test_id = format!(
        "transfer-cancel-pending-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("pending.txt"), b"cancel-pending").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.instance_id = "transfer-cancel-pending".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 0);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}/pending.txt", ufs_dir.display()),
                target_path: "/mnt/pending.txt".to_string(),
                client_request_id: test_id.clone(),
                submitter: "cancel-pending-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let cancel = client.cancel(&submit.job_id, None).await.unwrap();
        assert_eq!(cancel.job_id, submit.job_id);
        assert!(
            cancel.state == TransferStateProto::TransferCanceling as i32
                || cancel.state == TransferStateProto::TransferCanceled as i32,
            "cancel RPC should return a cancel state, got {}",
            cancel.state
        );
        let status = wait_transfer_state(
            &client,
            &submit.job_id,
            TransferStateProto::TransferCanceled,
            Duration::from_secs(20),
        )
        .await;
        assert_eq!(status.state, TransferStateProto::TransferCanceled as i32);
        assert!(
            status.tasks.iter().all(|task| {
                task.state == TransferTaskStateProto::TransferTaskCanceled as i32
                    || task.state == TransferTaskStateProto::TransferTaskPending as i32
            }),
            "pending cancel should not start running task after cancel, tasks: {:?}",
            status.tasks
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_cancel_running_worker_task_does_not_commit_output() {
    let test_id = format!(
        "transfer-cancel-running-worker-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    let source_path = ufs_dir.join("large-source.bin");
    let source = fs::File::create(&source_path).unwrap();
    source.set_len(2 * 1024 * 1024 * 1024).unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.instance_id = "transfer-cancel-running-worker".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let target = Path::from_str("/mnt/cancelled-large.bin").unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}", source_path.display()),
                target_path: target.clone_uri(),
                client_request_id: test_id.clone(),
                submitter: "cancel-running-worker-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let _running = wait_transfer_task_state(
            &client,
            &submit.job_id,
            TransferTaskStateProto::TransferTaskRunning,
            Duration::from_secs(20),
        )
        .await;

        let cancel = client.cancel(&submit.job_id, None).await.unwrap();
        assert!(
            cancel.state == TransferStateProto::TransferCanceling as i32
                || cancel.state == TransferStateProto::TransferCanceled as i32,
            "cancel RPC should return a cancel state, got {}",
            cancel.state
        );
        let status = wait_transfer_state(
            &client,
            &submit.job_id,
            TransferStateProto::TransferCanceled,
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(status.state, TransferStateProto::TransferCanceled as i32);
        assert!(
            status
                .tasks
                .iter()
                .all(|task| { task.state == TransferTaskStateProto::TransferTaskCanceled as i32 }),
            "running worker cancel should leave only canceled tasks: {:?}",
            status.tasks
        );
        assert!(
            !fs.exists(&target).await.unwrap(),
            "canceled running transfer must not commit final target"
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_runner_keeps_pre_start_cancel_terminal() {
    let rt = Arc::new(AsyncRuntime::single());
    let conf = ClusterConf::default();
    let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone()).unwrap();
    let task = Arc::new(TaskContext::new(LoadTaskInfo {
        job: LoadJobInfo {
            job_id: "pre_start_cancel_job".to_string(),
            source_path: "file:///unused-source".to_string(),
            target_path: "/unused-target".to_string(),
            block_size: 4096,
            replicas: 1,
            storage_type: StorageType::default(),
            ttl_ms: 0,
            ttl_action: TtlAction::default(),
            mount_info: MountInfo::default(),
            create_time: 0,
            overwrite: None,
        },
        task_id: "pre_start_cancel_task".to_string(),
        worker: WorkerAddress::default(),
        source_path: "file:///unused-source".to_string(),
        target_path: "/unused-target".to_string(),
        create_time: 0,
        source_read_plan_json: String::new(),
        transfer_report: None,
    }));
    task.set_canceled("queued task canceled before runner start");

    let runner = LoadTaskRunner::new(
        task.clone(),
        fs,
        Arc::new(UfsFactory::with_rt(&conf.client, rt.clone())),
        None,
        10_000,
        60_000,
    );

    let remove_task = rt.block_on(async { runner.run().await });
    assert!(remove_task, "pre-start canceled task should be removable");
    assert_eq!(task.get_state(), JobTaskState::Canceled);
    assert_eq!(
        task.progress().message,
        "queued task canceled before runner start"
    );
}

#[test]
fn test_transfer_does_not_dispatch_to_worker_without_transfer_capabilities() {
    let test_id = format!(
        "transfer-worker-capability-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("capability.txt"), b"capability-gated").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.instance_id = "transfer-worker-capability".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 0);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    {
        let active_master = cluster.get_active_master_fs();
        let mut worker_manager = active_master.worker_manager.write();
        let mut legacy_worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: 424242,
                hostname: "legacy-worker".to_string(),
                ip_addr: "127.0.0.1".to_string(),
                rpc_port: NetUtils::hold_available_port() as u32,
                web_port: NetUtils::hold_available_port() as u32,
            },
            WorkerInfo::default_weight(),
        );
        legacy_worker.worker_session_id = "legacy-session-without-transfer-capability".to_string();
        worker_manager.add_test_worker(legacy_worker);
    }

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}/capability.txt", ufs_dir.display()),
                target_path: "/mnt/capability.txt".to_string(),
                client_request_id: test_id.clone(),
                submitter: "capability-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;
        let status = client
            .status_page(&submit.job_id, Some(10), None)
            .await
            .unwrap();
        assert_ne!(
            status.state,
            TransferStateProto::TransferCompleted as i32,
            "legacy worker without transfer capabilities must not complete transfer"
        );
        assert!(
            !status.tasks.is_empty(),
            "scheduler should have planned tasks before dispatch capability gating"
        );
        assert!(
            status.tasks.iter().all(|task| {
                task.state == TransferTaskStateProto::TransferTaskPending as i32
                    && task.worker_id == 0
                    && task.worker_session_id.is_empty()
            }),
            "tasks must remain unassigned when only legacy workers are live: {:?}",
            status.tasks
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_cancel_running_job_end_to_end() {
    let test_id = format!(
        "transfer-cancel-running-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    fs::create_dir_all(&base_dir).unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.instance_id = "transfer-cancel-running".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let now_ms = orpc::common::LocalTime::mills() as i64;
    let store = SqliteTransferStore::open(conf.transfer.sqlite_path.clone()).unwrap();
    let job_id = format!("{test_id}-job");
    let mut job = TransferJobRecord {
        job_key: format!("Load:file:///{test_id}:/cancel/{test_id}"),
        job_id: job_id.clone(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: format!("file:///{test_id}"),
        target_path: format!("/cancel/{test_id}"),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        cv_metadata_epoch: None,
        state: curvine_common::state::TransferState::Running,
        owner: "transfer-cancel-running".to_string(),
        lease_epoch: 1,
        lease_expire_at: now_ms + 120_000,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("{test_id}-request"),
        submitter: "cancel-running-e2e".to_string(),
        tenant: "test".to_string(),
        created_at: now_ms - 1_000,
        updated_at: now_ms - 1_000,
    };
    job.summary.message = "preloaded running job".to_string();
    store.create_or_get_by_request_id(job).unwrap();
    store
        .insert_tasks(vec![TransferTaskRecord {
            job_id: job_id.clone(),
            run_id: 1,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            source_path: format!("file:///{test_id}/source"),
            target_path: format!("/cancel/{test_id}/target"),
            worker_id: 0,
            worker_session_id: String::new(),
            source_read_plan_json: String::new(),
            report_target_json: "{}".to_string(),
            state: TransferTaskState::Running,
            progress: TransferProgress::default(),
            retry_count: 0,
            attempt_started_at: now_ms - 1_000,
            last_report_at: now_ms - 1_000,
            stale_deadline_at: now_ms + 120_000,
            updated_at: now_ms - 1_000,
        }])
        .unwrap();
    drop(store);

    let cluster = MiniCluster::with_num(&conf, 1, 0);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let cancel = client.cancel(&job_id, None).await.unwrap();
        assert_eq!(cancel.job_id, job_id);
        assert!(
            cancel.state == TransferStateProto::TransferCanceling as i32
                || cancel.state == TransferStateProto::TransferCanceled as i32,
            "cancel RPC should return a cancel state, got {}",
            cancel.state
        );
        let status = wait_transfer_state(
            &client,
            &job_id,
            TransferStateProto::TransferCanceled,
            Duration::from_secs(20),
        )
        .await;
        assert_eq!(status.state, TransferStateProto::TransferCanceled as i32);
        assert_eq!(status.tasks.len(), 1);
        assert_eq!(
            status.tasks[0].state,
            TransferTaskStateProto::TransferTaskCanceled as i32
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_reports_partial_success_after_task_failure() {
    let test_id = format!(
        "transfer-partial-success-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    fs::create_dir_all(&base_dir).unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.instance_id = "transfer-partial-success".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let now_ms = orpc::common::LocalTime::mills() as i64;
    let store = SqliteTransferStore::open(conf.transfer.sqlite_path.clone()).unwrap();
    let job_id = format!("{test_id}-job");
    let mut job = TransferJobRecord {
        job_key: format!("Load:file:///{test_id}:/partial/{test_id}"),
        job_id: job_id.clone(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: format!("file:///{test_id}"),
        target_path: format!("/partial/{test_id}"),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        cv_metadata_epoch: None,
        state: curvine_common::state::TransferState::Running,
        owner: "transfer-partial-success".to_string(),
        lease_epoch: 1,
        lease_expire_at: now_ms + 120_000,
        cancel_requested: false,
        summary: TransferProgress {
            loaded_size: 1_024,
            total_size: 3_584,
            update_time: now_ms,
            message: "one task failed".to_string(),
        },
        client_request_id: format!("{test_id}-request"),
        submitter: "partial-success-e2e".to_string(),
        tenant: "test".to_string(),
        created_at: now_ms - 1_000,
        updated_at: now_ms - 1_000,
    };
    job.summary.message = "one task failed".to_string();
    store.create_or_get_by_request_id(job).unwrap();
    store
        .insert_tasks(vec![
            TransferTaskRecord {
                job_id: job_id.clone(),
                run_id: 1,
                task_id: "completed".to_string(),
                attempt_id: 1,
                source_path: format!("file:///{test_id}/completed"),
                target_path: format!("/partial/{test_id}/completed"),
                worker_id: 0,
                worker_session_id: String::new(),
                source_read_plan_json: String::new(),
                report_target_json: "{}".to_string(),
                state: TransferTaskState::Completed,
                progress: TransferProgress {
                    loaded_size: 1_024,
                    total_size: 1_024,
                    update_time: now_ms,
                    message: String::new(),
                },
                retry_count: 0,
                attempt_started_at: now_ms - 1_000,
                last_report_at: now_ms - 1_000,
                stale_deadline_at: now_ms + 120_000,
                updated_at: now_ms - 1_000,
            },
            TransferTaskRecord {
                job_id: job_id.clone(),
                run_id: 1,
                task_id: "failed".to_string(),
                attempt_id: 1,
                source_path: format!("file:///{test_id}/failed"),
                target_path: format!("/partial/{test_id}/failed"),
                worker_id: 0,
                worker_session_id: String::new(),
                source_read_plan_json: String::new(),
                report_target_json: "{}".to_string(),
                state: TransferTaskState::Failed,
                progress: TransferProgress {
                    loaded_size: 0,
                    total_size: 2_048,
                    update_time: now_ms,
                    message: "source object not found".to_string(),
                },
                retry_count: 0,
                attempt_started_at: now_ms - 1_000,
                last_report_at: now_ms - 1_000,
                stale_deadline_at: now_ms + 120_000,
                updated_at: now_ms - 1_000,
            },
            TransferTaskRecord {
                job_id: job_id.clone(),
                run_id: 1,
                task_id: "pending".to_string(),
                attempt_id: 0,
                source_path: format!("file:///{test_id}/pending"),
                target_path: format!("/partial/{test_id}/pending"),
                worker_id: 0,
                worker_session_id: String::new(),
                source_read_plan_json: String::new(),
                report_target_json: "{}".to_string(),
                state: TransferTaskState::Pending,
                progress: TransferProgress {
                    loaded_size: 0,
                    total_size: 512,
                    update_time: now_ms,
                    message: String::new(),
                },
                retry_count: 0,
                attempt_started_at: 0,
                last_report_at: 0,
                stale_deadline_at: 0,
                updated_at: now_ms - 1_000,
            },
        ])
        .unwrap();
    drop(store);

    let cluster = MiniCluster::with_num(&conf, 1, 0);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();
    rt.block_on(async {
        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let status = wait_transfer_state(
            &client,
            &job_id,
            TransferStateProto::TransferPartialSuccess,
            Duration::from_secs(20),
        )
        .await;

        assert_eq!(status.progress.loaded_size, 1_024);
        assert_eq!(status.progress.total_size, 3_584);
        assert!(status.progress.message.contains("source object not found"));
        let summary = status
            .task_summary
            .expect("status must include task summary");
        assert_eq!(summary.completed, 1);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.canceled, 1);
        assert_eq!(summary.completed_size, 1_024);
        assert!(status.tasks.iter().any(|task| {
            task.task_id == "pending"
                && task.state == TransferTaskStateProto::TransferTaskCanceled as i32
        }));

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_rejects_unsafe_target_overwrite() {
    let test_id = format!(
        "transfer-target-policy-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::create_dir_all(ufs_dir.join("dir-source")).unwrap();
    fs::write(ufs_dir.join("existing-source.txt"), b"new-content").unwrap();
    fs::write(ufs_dir.join("dir-source.txt"), b"dir-content").unwrap();
    fs::write(ufs_dir.join("dir-source").join("child.txt"), b"dir-child").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.cv_metadata_reader = TransferCvMetadataReaderType::Master;
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();
        fs.write_string(&Path::from_str("/mnt/existing.txt").unwrap(), "old-content")
            .await
            .unwrap();
        fs.mkdir(&Path::from_str("/mnt/existing-dir").unwrap(), true)
            .await
            .unwrap();

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;
        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();

        let mut no_overwrite = TransferCommand {
            kind: TransferKind::Load,
            source_path: format!("file://{}/existing-source.txt", ufs_dir.display()),
            target_path: "/mnt/existing.txt".to_string(),
            client_request_id: format!("{test_id}-no-overwrite"),
            submitter: "target-policy-e2e".to_string(),
            tenant: "test".to_string(),
            options: Default::default(),
        };
        no_overwrite.set_overwrite(false);
        let no_overwrite_job = client.submit(no_overwrite).await.unwrap();
        assert_transfer_failed(&client, &no_overwrite_job.job_id).await;
        assert_eq!(
            fs.read_string(&Path::from_str("/mnt/existing.txt").unwrap())
                .await
                .unwrap(),
            "old-content"
        );

        let dir_target_job = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}/dir-source.txt", ufs_dir.display()),
                target_path: "/mnt/existing-dir".to_string(),
                client_request_id: format!("{test_id}-dir-target"),
                submitter: "target-policy-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();
        assert_transfer_failed(&client, &dir_target_job.job_id).await;
        assert!(
            fs.get_status(&Path::from_str("/mnt/existing-dir").unwrap())
                .await
                .unwrap()
                .is_dir,
            "directory target should remain a directory"
        );

        let directory_into_file_job = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}/dir-source", ufs_dir.display()),
                target_path: "/mnt/existing.txt".to_string(),
                client_request_id: format!("{test_id}-directory-into-file"),
                submitter: "target-policy-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();
        let directory_into_file_status =
            assert_transfer_failed(&client, &directory_into_file_job.job_id).await;
        assert!(
            directory_into_file_status
                .progress
                .message
                .contains("refusing to transfer directory into file"),
            "directory source should fail on target type preflight, got: {}",
            directory_into_file_status.progress.message
        );
        assert_eq!(
            fs.read_string(&Path::from_str("/mnt/existing.txt").unwrap())
                .await
                .unwrap(),
            "old-content"
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_load_file_end_to_end_with_mysql_store() {
    let Some(mysql_url) = mysql_transfer_store_url("mysql-e2e") else {
        eprintln!("skip mysql transfer e2e: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let test_id = format!(
        "transfer-mysql-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("hello.txt"), b"mysql-transfer").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url;
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.instance_id = "transfer-child-recover".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let target = Path::from_str("/mnt/hello.txt").unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}/hello.txt", ufs_dir.display()),
                target_path: target.clone_uri(),
                client_request_id: test_id.clone(),
                submitter: "mysql-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let mut final_state = submit.state;
        for _ in 0..80 {
            let status = client
                .status_page(&submit.job_id, Some(10), None)
                .await
                .unwrap();
            final_state = status.state;
            if final_state == TransferStateProto::TransferCompleted as i32
                || final_state == TransferStateProto::TransferFailed as i32
            {
                assert!(!status.tasks.is_empty(), "status should return task page");
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        assert_eq!(final_state, TransferStateProto::TransferCompleted as i32);
        assert_eq!(fs.read_string(&target).await.unwrap(), "mysql-transfer");

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_mysql_server_recovers_after_store_outage() {
    let Some((mysql_url, base_url, db_name)) = mysql_transfer_store_url_with_db("mysql-outage")
    else {
        eprintln!("skip mysql store outage recovery e2e: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let test_id = format!(
        "transfer-mysql-store-outage-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("after-outage.txt"), b"mysql-store-recovered").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url.clone();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.instance_id = "transfer-store-outage".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;
        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();

        drop_mysql_database(&base_url, &db_name);
        let outage = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}/after-outage.txt", ufs_dir.display()),
                target_path: "/mnt/after-outage-outage.txt".to_string(),
                client_request_id: format!("{test_id}-outage"),
                submitter: "mysql-store-outage-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap_err()
            .to_string();
        assert!(
            outage.contains("transfer_jobs")
                || outage.contains("Unknown database")
                || outage.contains("doesn't exist")
                || outage.contains("No database selected"),
            "unexpected store outage error: {outage}"
        );

        create_mysql_database(&base_url, &db_name);
        let restored_store = MysqlTransferStore::open(&mysql_url).unwrap();
        drop(restored_store);

        let target = Path::from_str("/mnt/after-outage.txt").unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}/after-outage.txt", ufs_dir.display()),
                target_path: target.clone_uri(),
                client_request_id: format!("{test_id}-recovered"),
                submitter: "mysql-store-outage-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let status = wait_transfer_state(
            &client,
            &submit.job_id,
            TransferStateProto::TransferCompleted,
            Duration::from_secs(30),
        )
        .await;
        assert!(!status.tasks.is_empty(), "status should return task page");
        assert_eq!(
            fs.read_string(&target).await.unwrap(),
            "mysql-store-recovered"
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_readyz_rejects_idle_mysql_store_outage() {
    let Some((mysql_url, proxy)) = mysql_transfer_store_url_with_proxy("readyz-store-outage")
    else {
        eprintln!(
            "skip mysql readyz store outage e2e: CURVINE_TRANSFER_MYSQL_URL must use host:port"
        );
        return;
    };
    let test_id = format!(
        "transfer-readyz-store-outage-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url;
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.instance_id = "transfer-readyz-store-outage".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 0);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        let ready = read_http_path(cluster.cluster_conf.transfer.web_port, "/readyz");
        assert!(
            ready.ends_with("ok\n"),
            "transfer readiness should be ok before store outage, response: {ready}"
        );

        proxy.set_available(false);
        let not_ready = read_http_path(cluster.cluster_conf.transfer.web_port, "/readyz");
        assert!(
            not_ready.contains("503 Service Unavailable")
                && not_ready.contains("store unavailable"),
            "transfer readiness should actively reject idle mysql outage, response: {not_ready}"
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_external_transfer_readyz_rejects_idle_mysql_store_outage() {
    let Some((mysql_url, proxy)) = mysql_transfer_store_url_with_proxy("external-readyz-outage")
    else {
        eprintln!(
            "skip external mysql readyz store outage e2e: CURVINE_TRANSFER_MYSQL_URL must use host:port"
        );
        return;
    };
    let binary = match curvine_server_binary() {
        Some(binary) => binary,
        None => {
            eprintln!("skip external mysql readyz store outage e2e: curvine-server binary missing");
            return;
        }
    };
    let test_id = format!(
        "transfer-external-readyz-store-outage-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url;
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.instance_id = "transfer-external-readyz-store-outage".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 0);
    cluster.start_cluster();

    let (transfer_conf, mut transfer_child) = spawn_transfer_child_until_healthy(
        &binary,
        cluster.cluster_conf.clone(),
        &base_dir,
        "external-readyz",
    );
    let ready = read_http_path(transfer_conf.transfer.web_port, "/readyz");
    assert!(
        ready.ends_with("ok\n"),
        "external transfer readiness should be ok before store outage, response: {ready}"
    );

    proxy.set_available(false);
    let not_ready = read_http_path(transfer_conf.transfer.web_port, "/readyz");
    assert!(
        not_ready.contains("503 Service Unavailable") && not_ready.contains("store unavailable"),
        "external transfer readiness should reject idle mysql outage, response: {not_ready}"
    );

    proxy.set_available(true);
    let restored = wait_http_path(
        transfer_conf.transfer.web_port,
        "/readyz",
        Duration::from_secs(10),
    );
    assert!(
        restored,
        "external transfer readiness should recover after mysql proxy restore"
    );

    transfer_child.stop();
    let _ = fs::remove_dir_all(base_dir);
}

#[test]
fn test_transfer_mysql_recovers_completed_running_task_after_transient_store_disconnect() {
    let Some((mysql_url, proxy)) =
        mysql_transfer_store_url_with_proxy("mysql-running-store-disconnect")
    else {
        eprintln!(
            "skip mysql transient store disconnect e2e: CURVINE_TRANSFER_MYSQL_URL must use host:port"
        );
        return;
    };
    let test_id = format!(
        "transfer-mysql-running-store-disconnect-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    let mut source = fs::File::create(ufs_dir.join("disconnect.bin")).unwrap();
    let block = vec![7u8; 1024 * 1024];
    for _ in 0..64 {
        source.write_all(&block).unwrap();
    }
    drop(source);

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url.clone();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.instance_id = "transfer-store-disconnect".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.lease_timeout_str = "1s".to_string();
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let target = Path::from_str("/mnt/disconnect.bin").unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}/disconnect.bin", ufs_dir.display()),
                target_path: target.clone_uri(),
                client_request_id: test_id.clone(),
                submitter: "mysql-store-disconnect-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let store = MysqlTransferStore::open(&mysql_url).unwrap();
        let mut running_task = None;
        for _ in 0..1200 {
            let tasks = store
                .list_transfer_tasks(&submit.job_id, submit.run_id)
                .unwrap();
            running_task = tasks
                .into_iter()
                .find(|task| task.state == TransferTaskState::Running);
            if running_task.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let running_task = running_task.expect("task must reach running before store disconnect");
        assert!(
            wait_worker_task_visible(&fs, &cluster.cluster_conf, rt.clone(), &running_task).await,
            "worker should accept task before store disconnect"
        );

        proxy.set_available(false);
        let mut committed_while_disconnected = false;
        for _ in 0..600 {
            if fs.get_status(&target).await.is_ok() {
                committed_while_disconnected = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            committed_while_disconnected,
            "worker should commit output locally while transfer store is disconnected"
        );
        tokio::time::sleep(Duration::from_millis(800)).await;
        let state_during_disconnect = client.status(&submit.job_id).await;
        assert!(
            state_during_disconnect.is_err(),
            "status should fail while transfer store connection is disconnected"
        );

        proxy.set_available(true);
        let status = wait_transfer_state(
            &client,
            &submit.job_id,
            TransferStateProto::TransferCompleted,
            Duration::from_secs(30),
        )
        .await;
        assert!(!status.tasks.is_empty(), "status should return task page");

        let tasks = store
            .list_transfer_tasks(&submit.job_id, submit.run_id)
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].state, TransferTaskState::Completed);
        assert_eq!(
            tasks[0].attempt_id, 1,
            "store recovery should complete the original task by probe, not by retry"
        );
        assert_eq!(
            tasks[0].retry_count, 0,
            "store recovery should not mark the completed local task stale"
        );
        assert_eq!(fs.get_status(&target).await.unwrap().len, 64 * 1024 * 1024);

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_mysql_recovers_planning_job_after_owner_loss() {
    let Some(mysql_url) = mysql_transfer_store_url("mysql-recover-planning") else {
        eprintln!("skip mysql transfer recovery e2e: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let test_id = format!(
        "transfer-mysql-recover-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("recover.txt"), b"mysql-recovery").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url.clone();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.instance_id = "transfer-child-recover".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let store = Arc::new(MysqlTransferStore::open(&mysql_url).unwrap());
        let cache = ClusterMetadataCache::new(fs.clone());
        cache.refresh().await.unwrap();
        let service = TransferService::with_cache(
            store.clone(),
            cache,
            cluster.cluster_conf.transfer.task_stale_timeout,
        );
        let job = service
            .submit_transfer(SubmitTransferRequest {
                kind: TransferKindProto::TransferLoad as i32,
                source_path: format!("file://{}/recover.txt", ufs_dir.display()),
                target_path: "/mnt/recover.txt".to_string(),
                client_request_id: test_id.clone(),
                submitter: "mysql-recovery-e2e".to_string(),
                tenant: "test".to_string(),
                command: Vec::new(),
                protocol_version: Some(1),
            })
            .unwrap();

        let dead_lease = store
            .acquire_runnable_transfer("dead-transfer-owner", 1, 1, 1)
            .unwrap()
            .unwrap();
        assert_eq!(dead_lease.job_id, job.job_id);
        assert_eq!(
            store.get_transfer(&job.job_id).unwrap().unwrap().state,
            curvine_common::state::TransferState::Planning
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
        let binary = curvine_server_binary()
            .expect("external transfer restart e2e requires target/debug/curvine-server");
        let (transfer_conf, mut transfer_child) = spawn_transfer_child_until_healthy(
            &binary,
            cluster.cluster_conf.clone(),
            &base_dir,
            "transfer-child",
        );

        let client = TransferClient::with_rt(&transfer_conf, rt.clone()).unwrap();
        let mut final_state = 0;
        let mut owner = String::new();
        for _ in 0..100 {
            let status = client
                .status_page(&job.job_id, Some(10), None)
                .await
                .unwrap();
            final_state = status.state;
            owner = status.owner.unwrap_or_default();
            if final_state == TransferStateProto::TransferCompleted as i32
                || final_state == TransferStateProto::TransferFailed as i32
            {
                assert!(!status.tasks.is_empty(), "status should return task page");
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        assert_ne!(owner, "dead-transfer-owner");
        assert_eq!(final_state, TransferStateProto::TransferCompleted as i32);
        assert_eq!(
            fs.read_string(&Path::from_str("/mnt/recover.txt").unwrap())
                .await
                .unwrap(),
            "mysql-recovery"
        );

        transfer_child.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_mysql_recovers_running_task_after_owner_loss() {
    let Some(mysql_url) = mysql_transfer_store_url("mysql-recover-running") else {
        eprintln!(
            "skip mysql transfer running recovery e2e: CURVINE_TRANSFER_MYSQL_URL is not set"
        );
        return;
    };
    let test_id = format!(
        "transfer-mysql-running-recover-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("running.txt"), b"mysql-running-recovery").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url.clone();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.instance_id = "transfer-running-owner-a".to_string();
    conf.transfer.endpoints = vec!["localhost:1".to_string()];
    conf.transfer.lease_timeout_str = "700ms".to_string();
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let binary = curvine_server_binary()
            .expect("external transfer restart e2e requires target/debug/curvine-server");
        let (first_conf, mut first_child) = spawn_transfer_child_until_healthy(
            &binary,
            cluster.cluster_conf.clone(),
            &base_dir,
            "transfer-running-owner-a",
        );
        let first_endpoint = format!(
            "{}:{}",
            first_conf.transfer.hostname, first_conf.transfer.rpc_port
        );
        let mut client_conf = cluster.cluster_conf.clone();
        client_conf.transfer.endpoints = vec![first_endpoint];
        let client = TransferClient::with_rt(&client_conf, rt.clone()).unwrap();
        let target = Path::from_str("/mnt/running.txt").unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}/running.txt", ufs_dir.display()),
                target_path: target.clone_uri(),
                client_request_id: test_id.clone(),
                submitter: "mysql-running-recovery-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let store = MysqlTransferStore::open(&mysql_url).unwrap();
        let running_task = wait_for_mysql_running_task_after_local_commit(
            &fs,
            &store,
            &submit.job_id,
            submit.run_id,
            &target,
            "mysql-running-recovery",
            Duration::from_secs(30),
        )
        .await;
        assert!(
            running_task.is_some(),
            "first owner should leave a running task with completed local output before it dies"
        );
        assert_ne!(
            store.get_transfer(&submit.job_id).unwrap().unwrap().state,
            curvine_common::state::TransferState::Completed,
            "first owner must not complete the job before failover"
        );
        first_child.stop();

        let mut second_conf = cluster.cluster_conf.clone();
        second_conf.transfer.rpc_port = NetUtils::get_available_port();
        second_conf.transfer.web_port = NetUtils::get_available_port();
        second_conf.transfer.instance_id = "transfer-running-owner-b".to_string();
        second_conf.transfer.endpoints = vec![format!(
            "{}:{}",
            second_conf.transfer.hostname, second_conf.transfer.rpc_port
        )];
        second_conf.transfer.init().unwrap();
        let (second_conf, mut second_child) = spawn_transfer_child_until_healthy(
            &binary,
            second_conf,
            &base_dir,
            "transfer-running-owner-b",
        );
        let second_client = TransferClient::with_rt(&second_conf, rt.clone()).unwrap();

        let mut final_state = 0;
        let mut owner = String::new();
        for _ in 0..120 {
            let status = second_client
                .status_page(&submit.job_id, Some(10), None)
                .await
                .unwrap();
            final_state = status.state;
            owner = status.owner.unwrap_or_default();
            if final_state == TransferStateProto::TransferCompleted as i32
                || final_state == TransferStateProto::TransferFailed as i32
            {
                assert!(!status.tasks.is_empty(), "status should return task page");
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        assert_eq!(owner, "transfer-running-owner-b");
        assert_eq!(final_state, TransferStateProto::TransferCompleted as i32);
        let final_tasks = store
            .list_transfer_tasks(&submit.job_id, submit.run_id)
            .unwrap();
        assert_eq!(final_tasks.len(), 1);
        assert_eq!(
            final_tasks[0].state,
            curvine_common::state::TransferTaskState::Completed
        );
        assert_eq!(
            final_tasks[0].attempt_id, 1,
            "running recovery must complete by probing the original attempt, not by retrying"
        );
        assert_eq!(
            final_tasks[0].retry_count, 0,
            "running recovery must not mark stale and redispatch the task"
        );
        assert_eq!(
            fs.read_string(&target).await.unwrap(),
            "mysql-running-recovery"
        );

        second_child.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_mysql_retry_accepts_already_committed_output_after_lost_report() {
    let Some(mysql_url) = mysql_transfer_store_url("mysql-retry-commit") else {
        eprintln!("skip mysql transfer retry commit e2e: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let test_id = format!(
        "transfer-mysql-retry-commit-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("retry.txt"), b"mysql-retry-commit").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url.clone();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.instance_id = "transfer-retry-owner-a".to_string();
    conf.transfer.endpoints = vec!["localhost:1".to_string()];
    conf.transfer.lease_timeout_str = "1s".to_string();
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let binary = curvine_server_binary()
            .expect("external transfer retry e2e requires target/debug/curvine-server");
        let (first_conf, mut first_child) = spawn_transfer_child_until_healthy(
            &binary,
            cluster.cluster_conf.clone(),
            &base_dir,
            "transfer-retry-owner-a",
        );
        let first_endpoint = format!(
            "{}:{}",
            first_conf.transfer.hostname, first_conf.transfer.rpc_port
        );
        let mut client_conf = cluster.cluster_conf.clone();
        client_conf.transfer.endpoints = vec![first_endpoint];
        let client = TransferClient::with_rt(&client_conf, rt.clone()).unwrap();
        let target = Path::from_str("/mnt/retry.txt").unwrap();
        let mut command = TransferCommand {
            kind: TransferKind::Load,
            source_path: format!("file://{}/retry.txt", ufs_dir.display()),
            target_path: target.clone_uri(),
            client_request_id: test_id.clone(),
            submitter: "mysql-retry-commit-e2e".to_string(),
            tenant: "test".to_string(),
            options: Default::default(),
        };
        command.set_overwrite(false);
        let submit = client.submit(command).await.unwrap();

        let store = MysqlTransferStore::open(&mysql_url).unwrap();
        let mut task = wait_for_mysql_running_task_after_local_commit(
            &fs,
            &store,
            &submit.job_id,
            submit.run_id,
            &target,
            "mysql-retry-commit",
            Duration::from_secs(30),
        )
        .await
        .expect("first owner should leave running task after committing output locally");
        first_child.stop();

        task.worker_session_id = "lost-report-session".to_string();
        task.stale_deadline_at = 1;
        force_mysql_task_state(&mysql_url, &task);

        let mut second_conf = cluster.cluster_conf.clone();
        second_conf.transfer.rpc_port = NetUtils::get_available_port();
        second_conf.transfer.web_port = NetUtils::get_available_port();
        second_conf.transfer.instance_id = "transfer-retry-owner-b".to_string();
        second_conf.transfer.endpoints = vec![format!(
            "{}:{}",
            second_conf.transfer.hostname, second_conf.transfer.rpc_port
        )];
        second_conf.transfer.init().unwrap();
        let (second_conf, mut second_child) = spawn_transfer_child_until_healthy(
            &binary,
            second_conf,
            &base_dir,
            "transfer-retry-owner-b",
        );
        let second_client = TransferClient::with_rt(&second_conf, rt.clone()).unwrap();

        let mut final_state = 0;
        for _ in 0..120 {
            let status = second_client
                .status_page(&submit.job_id, Some(10), None)
                .await
                .unwrap();
            final_state = status.state;
            if final_state == TransferStateProto::TransferCompleted as i32
                || final_state == TransferStateProto::TransferFailed as i32
            {
                assert!(!status.tasks.is_empty(), "status should return task page");
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        assert_eq!(final_state, TransferStateProto::TransferCompleted as i32);
        let final_tasks = store
            .list_transfer_tasks(&submit.job_id, submit.run_id)
            .unwrap();
        assert_eq!(final_tasks.len(), 1);
        assert_eq!(final_tasks[0].state, TransferTaskState::Completed);
        assert_eq!(
            final_tasks[0].attempt_id, 2,
            "retry commit recovery must dispatch a second attempt"
        );
        assert!(
            final_tasks[0].retry_count > 0,
            "retry commit recovery must mark the stale first attempt"
        );
        assert_eq!(fs.read_string(&target).await.unwrap(), "mysql-retry-commit");

        second_child.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_mysql_retries_running_task_after_worker_restart() {
    let Some(mysql_url) = mysql_transfer_store_url("mysql-worker-restart") else {
        eprintln!("skip mysql worker restart e2e: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let test_id = format!(
        "transfer-mysql-worker-restart-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("worker-restart.txt"), b"worker-restart").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url.clone();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.instance_id = "transfer-worker-restart-owner".to_string();
    conf.transfer.endpoints = vec!["localhost:1".to_string()];
    conf.transfer.lease_timeout_str = "2s".to_string();
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 0);
    cluster.start_master();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = wait_master_and_create_fs(&cluster).await;
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let binary = curvine_server_binary()
            .expect("external worker restart e2e requires target/debug/curvine-server");
        let mut worker_conf = cluster.cluster_conf.clone();
        worker_conf.worker.rpc_port = NetUtils::get_available_port();
        worker_conf.worker.web_port = NetUtils::get_available_port();
        worker_conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
        let worker_conf_path = base_dir.join("worker-restart.toml");
        std::fs::write(&worker_conf_path, toml::to_string(&worker_conf).unwrap()).unwrap();
        let first_worker_log = base_dir.join("worker-restart-a.log");
        let mut first_worker_child =
            ServiceChild::spawn(&binary, "worker", &worker_conf_path, &first_worker_log);
        let first_worker = wait_for_registered_worker_session(
            &fs,
            None,
            Duration::from_secs(20),
            &first_worker_log,
            &mut first_worker_child,
        )
        .await;

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut client_conf = cluster.cluster_conf.clone();
        client_conf.transfer.endpoints = vec![format!(
            "{}:{}",
            client_conf.transfer.hostname, client_conf.transfer.rpc_port
        )];
        let client = TransferClient::with_rt(&client_conf, rt.clone()).unwrap();
        let target = Path::from_str("/mnt/worker-restart.txt").unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Load,
                source_path: format!("file://{}/worker-restart.txt", ufs_dir.display()),
                target_path: target.clone_uri(),
                client_request_id: test_id.clone(),
                submitter: "mysql-worker-restart-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let store = MysqlTransferStore::open(&mysql_url).unwrap();
        let first_task = wait_for_mysql_running_task_after_local_commit(
            &fs,
            &store,
            &submit.job_id,
            submit.run_id,
            &target,
            "worker-restart",
            Duration::from_secs(30),
        )
        .await
        .expect("first worker should leave running task after local commit");
        assert_eq!(first_task.worker_session_id, first_worker.worker_session_id);

        first_worker_child.stop();
        let second_worker_log = base_dir.join("worker-restart-b.log");
        let mut second_worker_child =
            ServiceChild::spawn(&binary, "worker", &worker_conf_path, &second_worker_log);
        let second_worker = wait_for_registered_worker_session(
            &fs,
            Some(&first_worker.worker_session_id),
            Duration::from_secs(20),
            &second_worker_log,
            &mut second_worker_child,
        )
        .await;
        assert_ne!(
            second_worker.worker_session_id, first_worker.worker_session_id,
            "worker restart must publish a new session"
        );

        let mut final_state = 0;
        for _ in 0..120 {
            let status = client
                .status_page(&submit.job_id, Some(10), None)
                .await
                .unwrap();
            final_state = status.state;
            if final_state == TransferStateProto::TransferCompleted as i32
                || final_state == TransferStateProto::TransferFailed as i32
            {
                assert!(!status.tasks.is_empty(), "status should return task page");
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        assert_eq!(final_state, TransferStateProto::TransferCompleted as i32);
        let final_tasks = store
            .list_transfer_tasks(&submit.job_id, submit.run_id)
            .unwrap();
        assert_eq!(final_tasks.len(), 1);
        assert_eq!(final_tasks[0].state, TransferTaskState::Completed);
        assert_eq!(
            final_tasks[0].worker_id, second_worker.worker_id,
            "retry must use the restarted worker identity"
        );
        assert_eq!(
            final_tasks[0].worker_session_id, second_worker.worker_session_id,
            "retry must run on the restarted worker session"
        );
        assert!(
            final_tasks[0].attempt_id > 1,
            "worker restart must retry instead of probing the old session as completed"
        );
        assert!(
            final_tasks[0].retry_count > 0,
            "worker restart must mark the old running attempt stale"
        );
        assert_eq!(fs.read_string(&target).await.unwrap(), "worker-restart");

        transfer_server.stop();
        second_worker_child.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_mysql_two_servers_share_store_without_duplicate_tasks() {
    let Some(mysql_url) = mysql_transfer_store_url("mysql-two-server") else {
        eprintln!("skip mysql transfer two-server e2e: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let test_id = format!(
        "transfer-mysql-two-server-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    for index in 0..6 {
        fs::write(
            ufs_dir.join(format!("file-{index}.txt")),
            format!("two-server-{index}"),
        )
        .unwrap();
    }

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url.clone();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.instance_id = "transfer-e2e-a".to_string();
    conf.transfer.max_running_transfers = 2;
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let store = Arc::new(MysqlTransferStore::open(&mysql_url).unwrap());
        let cache = ClusterMetadataCache::new(fs.clone());
        cache.refresh().await.unwrap();
        let service = TransferService::with_cache(
            store.clone(),
            cache,
            cluster.cluster_conf.transfer.task_stale_timeout,
        );
        let mut job_ids = Vec::new();
        for index in 0..6 {
            let job = service
                .submit_transfer(SubmitTransferRequest {
                    kind: TransferKindProto::TransferLoad as i32,
                    source_path: format!("file://{}/file-{index}.txt", ufs_dir.display()),
                    target_path: format!("/mnt/file-{index}.txt"),
                    client_request_id: format!("{test_id}-{index}"),
                    submitter: "mysql-two-server-e2e".to_string(),
                    tenant: "test".to_string(),
                    command: Vec::new(),
                    protocol_version: Some(1),
                })
                .unwrap();
            job_ids.push(job.job_id);
        }

        let mut transfer_server_a = InProcessTransferServer::start(cluster.cluster_conf.clone());

        let mut second_conf = cluster.cluster_conf.clone();
        second_conf.transfer.rpc_port = NetUtils::hold_available_port();
        second_conf.transfer.web_port = NetUtils::hold_available_port();
        second_conf.transfer.instance_id = "transfer-e2e-b".to_string();
        second_conf.transfer.endpoints = vec![format!(
            "{}:{}",
            second_conf.transfer.hostname, second_conf.transfer.rpc_port
        )];
        second_conf.transfer.init().unwrap();
        let mut transfer_server_b = InProcessTransferServer::start(second_conf);
        tokio::time::sleep(Duration::from_millis(500)).await;

        for _ in 0..120 {
            let mut completed = 0;
            for job_id in &job_ids {
                let job = store.get_transfer(job_id).unwrap().unwrap();
                if job.state == curvine_common::state::TransferState::Completed {
                    completed += 1;
                }
            }
            if completed == job_ids.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        for (index, job_id) in job_ids.iter().enumerate() {
            let job = store.get_transfer(job_id).unwrap().unwrap();
            assert_eq!(
                job.state,
                curvine_common::state::TransferState::Completed,
                "job {} should complete, actual {:?}",
                job_id,
                job.state
            );
            assert!(
                job.owner == "transfer-e2e-a" || job.owner == "transfer-e2e-b",
                "unexpected job owner: {}",
                job.owner
            );
            let tasks = store.list_transfer_tasks(job_id, job.run_id).unwrap();
            assert_eq!(
                tasks.len(),
                1,
                "job {} should have one planned task",
                job_id
            );
            assert_eq!(
                tasks[0].state,
                curvine_common::state::TransferTaskState::Completed
            );
            assert_eq!(
                fs.read_string(&Path::from_str(format!("/mnt/file-{index}.txt")).unwrap())
                    .await
                    .unwrap(),
                format!("two-server-{index}")
            );
        }

        transfer_server_b.stop();
        transfer_server_a.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_mysql_external_process_rolling_upgrade_recovers_backlog() {
    let Some(mysql_url) = mysql_transfer_store_url("mysql-rolling-upgrade") else {
        eprintln!("skip mysql transfer rolling upgrade e2e: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let test_id = format!(
        "transfer-mysql-rolling-upgrade-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    let job_count = 24usize;
    let payload = "x".repeat(1024 * 1024);
    for index in 0..job_count {
        fs::write(
            ufs_dir.join(format!("rolling-{index}.bin")),
            payload.as_bytes(),
        )
        .unwrap();
    }

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url.clone();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.instance_id = "transfer-rolling-a".to_string();
    conf.transfer.max_running_transfers = 1;
    conf.transfer.lease_timeout_str = "1500ms".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let store = Arc::new(MysqlTransferStore::open(&mysql_url).unwrap());
        let cache = ClusterMetadataCache::new(fs.clone());
        cache.refresh().await.unwrap();
        let service = TransferService::with_cache(
            store.clone(),
            cache,
            cluster.cluster_conf.transfer.task_stale_timeout,
        );
        let mut job_ids = Vec::with_capacity(job_count);
        for index in 0..job_count {
            let job = service
                .submit_transfer(SubmitTransferRequest {
                    kind: TransferKindProto::TransferLoad as i32,
                    source_path: format!("file://{}/rolling-{index}.bin", ufs_dir.display()),
                    target_path: format!("/mnt/rolling-{index}.bin"),
                    client_request_id: format!("{test_id}-{index}"),
                    submitter: "mysql-rolling-upgrade-e2e".to_string(),
                    tenant: "test".to_string(),
                    command: Vec::new(),
                    protocol_version: Some(1),
                })
                .unwrap();
            job_ids.push(job.job_id);
        }

        let binary = curvine_server_binary()
            .expect("external transfer rolling upgrade e2e requires target/debug/curvine-server");
        let (first_conf, mut first_child) = spawn_transfer_child_until_healthy(
            &binary,
            cluster.cluster_conf.clone(),
            &base_dir,
            "transfer-rolling-a",
        );
        let mut first_owned = 0usize;
        for _ in 0..80 {
            first_owned = count_jobs_with_owner(&store, &job_ids, "transfer-rolling-a");
            if first_owned > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            first_owned > 0,
            "first transfer process should acquire at least one job before rolling stop"
        );
        first_child.stop();
        assert!(
            !wait_http_path(
                first_conf.transfer.web_port,
                "/healthz",
                Duration::from_millis(300)
            ),
            "old transfer health endpoint should be unavailable after rolling stop"
        );

        let mut second_conf = cluster.cluster_conf.clone();
        second_conf.transfer.rpc_port = NetUtils::get_available_port();
        second_conf.transfer.web_port = NetUtils::get_available_port();
        second_conf.transfer.instance_id = "transfer-rolling-b".to_string();
        second_conf.transfer.endpoints = vec![format!(
            "{}:{}",
            second_conf.transfer.hostname, second_conf.transfer.rpc_port
        )];
        second_conf.transfer.init().unwrap();
        let (second_conf, mut second_child) = spawn_transfer_child_until_healthy(
            &binary,
            second_conf,
            &base_dir,
            "transfer-rolling-b",
        );
        let second_client = TransferClient::with_rt(&second_conf, rt.clone()).unwrap();

        for _ in 0..480 {
            let completed = job_ids
                .iter()
                .filter(|job_id| {
                    store.get_transfer(job_id).unwrap().unwrap().state
                        == curvine_common::state::TransferState::Completed
                })
                .count();
            if completed == job_count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let mut second_owned = 0usize;
        let state_summary = transfer_state_summary(&store, &job_ids);
        for (index, job_id) in job_ids.iter().enumerate() {
            let status = second_client
                .status_page(job_id, Some(10), None)
                .await
                .unwrap();
            assert_eq!(
                status.state,
                TransferStateProto::TransferCompleted as i32,
                "job {job_id} should complete after rolling upgrade; state summary: {state_summary}"
            );
            let owner = status.owner.unwrap_or_default();
            assert!(
                owner == "transfer-rolling-a" || owner == "transfer-rolling-b",
                "unexpected owner after rolling upgrade: {owner}"
            );
            if owner == "transfer-rolling-b" {
                second_owned += 1;
            }
            assert_eq!(status.tasks.len(), 1, "job {job_id} should have one task");
            assert_eq!(
                status.tasks[0].state,
                TransferTaskStateProto::TransferTaskCompleted as i32
            );
            assert_eq!(
                fs.read_string(&Path::from_str(format!("/mnt/rolling-{index}.bin")).unwrap())
                    .await
                    .unwrap(),
                payload
            );
        }
        assert!(
            second_owned > 0,
            "second transfer process should take over at least one unfinished job"
        );

        second_child.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_mysql_external_process_multi_instance_rolling_upgrade() {
    let Some(mysql_url) = mysql_transfer_store_url("mysql-multi-rolling") else {
        eprintln!("skip mysql multi-instance rolling e2e: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let binary = match curvine_server_binary() {
        Some(binary) => binary,
        None => {
            eprintln!("skip mysql multi-instance rolling e2e: curvine-server binary missing");
            return;
        }
    };
    let test_id = format!(
        "transfer-mysql-multi-rolling-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    let job_count = 18usize;
    let payload = "multi-rolling-".repeat(64 * 1024);
    for index in 0..job_count {
        fs::write(
            ufs_dir.join(format!("multi-rolling-{index}.bin")),
            payload.as_bytes(),
        )
        .unwrap();
    }

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url.clone();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.instance_id = "transfer-multi-rolling-a".to_string();
    conf.transfer.max_running_transfers = 2;
    conf.transfer.lease_timeout_str = "1500ms".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 2);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let store = Arc::new(MysqlTransferStore::open(&mysql_url).unwrap());
        let cache = ClusterMetadataCache::new(fs.clone());
        cache.refresh().await.unwrap();
        let service = TransferService::with_cache(
            store.clone(),
            cache,
            cluster.cluster_conf.transfer.task_stale_timeout,
        );
        let mut job_ids = Vec::with_capacity(job_count);
        for index in 0..job_count {
            let job = service
                .submit_transfer(SubmitTransferRequest {
                    kind: TransferKindProto::TransferLoad as i32,
                    source_path: format!(
                        "file://{}/multi-rolling-{index}.bin",
                        ufs_dir.display()
                    ),
                    target_path: format!("/mnt/multi-rolling-{index}.bin"),
                    client_request_id: format!("{test_id}-{index}"),
                    submitter: "mysql-multi-rolling-e2e".to_string(),
                    tenant: "test".to_string(),
                    command: Vec::new(),
                    protocol_version: Some(1),
                })
                .unwrap();
            job_ids.push(job.job_id);
        }

        let (first_conf, mut first_child) = spawn_transfer_child_until_healthy(
            &binary,
            cluster.cluster_conf.clone(),
            &base_dir,
            "transfer-multi-rolling-a",
        );
        let mut second_conf = cluster.cluster_conf.clone();
        second_conf.transfer.rpc_port = NetUtils::get_available_port();
        second_conf.transfer.web_port = NetUtils::get_available_port();
        second_conf.transfer.instance_id = "transfer-multi-rolling-b".to_string();
        second_conf.transfer.endpoints = vec![format!(
            "{}:{}",
            second_conf.transfer.hostname, second_conf.transfer.rpc_port
        )];
        second_conf.transfer.init().unwrap();
        let (second_conf, mut second_child) = spawn_transfer_child_until_healthy(
            &binary,
            second_conf,
            &base_dir,
            "transfer-multi-rolling-b",
        );

        for _ in 0..120 {
            let first_owned =
                count_jobs_with_owner(&store, &job_ids, "transfer-multi-rolling-a");
            let second_owned =
                count_jobs_with_owner(&store, &job_ids, "transfer-multi-rolling-b");
            if first_owned > 0 && second_owned > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            count_jobs_with_owner(&store, &job_ids, "transfer-multi-rolling-a") > 0,
            "first transfer process should own jobs before rolling replacement"
        );
        assert!(
            count_jobs_with_owner(&store, &job_ids, "transfer-multi-rolling-b") > 0,
            "second transfer process should own jobs before rolling replacement"
        );

        first_child.stop();
        assert!(
            !wait_http_path(
                first_conf.transfer.web_port,
                "/healthz",
                Duration::from_millis(300)
            ),
            "old rolling instance should be unavailable after stop"
        );

        let mut third_conf = cluster.cluster_conf.clone();
        third_conf.transfer.rpc_port = NetUtils::get_available_port();
        third_conf.transfer.web_port = NetUtils::get_available_port();
        third_conf.transfer.instance_id = "transfer-multi-rolling-c".to_string();
        third_conf.transfer.endpoints = vec![format!(
            "{}:{}",
            third_conf.transfer.hostname, third_conf.transfer.rpc_port
        )];
        third_conf.transfer.init().unwrap();
        let (third_conf, mut third_child) = spawn_transfer_child_until_healthy(
            &binary,
            third_conf,
            &base_dir,
            "transfer-multi-rolling-c",
        );
        let third_client = TransferClient::with_rt(&third_conf, rt.clone()).unwrap();

        for _ in 0..480 {
            let completed = job_ids
                .iter()
                .filter(|job_id| {
                    store.get_transfer(job_id).unwrap().unwrap().state
                        == curvine_common::state::TransferState::Completed
                })
                .count();
            if completed == job_count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let state_summary = transfer_state_summary(&store, &job_ids);
        let mut second_owned = 0usize;
        let mut third_owned = 0usize;
        for (index, job_id) in job_ids.iter().enumerate() {
            let status = third_client
                .status_page(job_id, Some(10), None)
                .await
                .unwrap();
            assert_eq!(
                status.state,
                TransferStateProto::TransferCompleted as i32,
                "job {job_id} should complete after multi-instance rolling upgrade; state summary: {state_summary}"
            );
            let owner = status.owner.unwrap_or_default();
            assert!(
                matches!(
                    owner.as_str(),
                    "transfer-multi-rolling-a"
                        | "transfer-multi-rolling-b"
                        | "transfer-multi-rolling-c"
                ),
                "unexpected owner after multi-instance rolling upgrade: {owner}"
            );
            if owner == "transfer-multi-rolling-b" {
                second_owned += 1;
            }
            if owner == "transfer-multi-rolling-c" {
                third_owned += 1;
            }
            assert_eq!(status.tasks.len(), 1, "job {job_id} should have one task");
            assert_eq!(
                status.tasks[0].state,
                TransferTaskStateProto::TransferTaskCompleted as i32
            );
            assert_eq!(
                fs.read_string(&Path::from_str(format!("/mnt/multi-rolling-{index}.bin")).unwrap())
                    .await
                    .unwrap(),
                payload
            );
        }
        assert!(
            second_owned > 0,
            "existing peer should keep making progress during rolling replacement"
        );
        assert!(
            third_owned > 0,
            "new transfer process should acquire unfinished backlog after rolling replacement"
        );

        third_child.stop();
        second_child.stop();
        let _ = fs::remove_dir_all(base_dir);
        let _ = second_conf;
    });
}

#[test]
fn test_transfer_mysql_external_process_rolls_after_store_outage() {
    let Some((mysql_url, proxy)) = mysql_transfer_store_url_with_proxy("rolling-store-outage")
    else {
        eprintln!(
            "skip mysql rolling store outage e2e: CURVINE_TRANSFER_MYSQL_URL must use host:port"
        );
        return;
    };
    let binary = match curvine_server_binary() {
        Some(binary) => binary,
        None => {
            eprintln!("skip mysql rolling store outage e2e: curvine-server binary missing");
            return;
        }
    };
    let test_id = format!(
        "transfer-mysql-rolling-store-outage-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    let job_count = 8usize;
    let payload = "rolling-store-outage";
    for index in 0..job_count {
        fs::write(
            ufs_dir.join(format!("outage-{index}.txt")),
            format!("{payload}-{index}"),
        )
        .unwrap();
    }

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url.clone();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::get_available_port();
    conf.transfer.web_port = NetUtils::get_available_port();
    conf.transfer.instance_id = "transfer-rolling-outage-a".to_string();
    conf.transfer.max_running_transfers = 1;
    conf.transfer.lease_timeout_str = "1500ms".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let store = Arc::new(MysqlTransferStore::open(&mysql_url).unwrap());
        let cache = ClusterMetadataCache::new(fs.clone());
        cache.refresh().await.unwrap();
        let service = TransferService::with_cache(
            store.clone(),
            cache,
            cluster.cluster_conf.transfer.task_stale_timeout,
        );
        let mut job_ids = Vec::with_capacity(job_count);
        for index in 0..job_count {
            let job = service
                .submit_transfer(SubmitTransferRequest {
                    kind: TransferKindProto::TransferLoad as i32,
                    source_path: format!("file://{}/outage-{index}.txt", ufs_dir.display()),
                    target_path: format!("/mnt/outage-{index}.txt"),
                    client_request_id: format!("{test_id}-{index}"),
                    submitter: "mysql-rolling-store-outage-e2e".to_string(),
                    tenant: "test".to_string(),
                    command: Vec::new(),
                    protocol_version: Some(1),
                })
                .unwrap();
            job_ids.push(job.job_id);
        }

        let (first_conf, mut first_child) = spawn_transfer_child_until_healthy(
            &binary,
            cluster.cluster_conf.clone(),
            &base_dir,
            "transfer-rolling-outage-a",
        );
        let mut first_owned = 0usize;
        for _ in 0..80 {
            first_owned = count_jobs_with_owner(&store, &job_ids, "transfer-rolling-outage-a");
            if first_owned > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            first_owned > 0,
            "first transfer process should acquire at least one job before outage"
        );

        proxy.set_available(false);
        first_child.stop();
        assert!(
            !wait_http_path(
                first_conf.transfer.web_port,
                "/healthz",
                Duration::from_millis(300)
            ),
            "old transfer health endpoint should be unavailable after kill"
        );

        let mut second_conf = cluster.cluster_conf.clone();
        second_conf.transfer.rpc_port = NetUtils::get_available_port();
        second_conf.transfer.web_port = NetUtils::get_available_port();
        second_conf.transfer.instance_id = "transfer-rolling-outage-b".to_string();
        second_conf.transfer.endpoints = vec![format!(
            "{}:{}",
            second_conf.transfer.hostname, second_conf.transfer.rpc_port
        )];
        second_conf.transfer.init().unwrap();
        let second_conf_path = base_dir.join("transfer-rolling-outage-b-unavailable.toml");
        fs::write(&second_conf_path, toml::to_string(&second_conf).unwrap()).unwrap();
        let second_log_path = base_dir.join("transfer-rolling-outage-b-unavailable.log");
        let mut second_child = TransferChild::spawn(&binary, &second_conf_path, &second_log_path);
        assert!(
            !wait_http_path(second_conf.transfer.web_port, "/readyz", Duration::from_secs(2)),
            "new transfer process must not become ready while mysql store is unavailable"
        );
        second_child.stop();

        proxy.set_available(true);
        let (second_conf, mut second_child) = spawn_transfer_child_until_healthy(
            &binary,
            second_conf,
            &base_dir,
            "transfer-rolling-outage-b",
        );
        let second_client = TransferClient::with_rt(&second_conf, rt.clone()).unwrap();

        for _ in 0..240 {
            let completed = job_ids
                .iter()
                .filter(|job_id| {
                    store.get_transfer(job_id).unwrap().unwrap().state
                        == curvine_common::state::TransferState::Completed
                })
                .count();
            if completed == job_count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let state_summary = transfer_state_summary(&store, &job_ids);
        let mut second_owned = 0usize;
        for (index, job_id) in job_ids.iter().enumerate() {
            let status = second_client
                .status_page(job_id, Some(10), None)
                .await
                .unwrap();
            assert_eq!(
                status.state,
                TransferStateProto::TransferCompleted as i32,
                "job {job_id} should complete after rolling store outage; state summary: {state_summary}"
            );
            if status.owner.as_deref() == Some("transfer-rolling-outage-b") {
                second_owned += 1;
            }
            assert_eq!(
                fs.read_string(&Path::from_str(format!("/mnt/outage-{index}.txt")).unwrap())
                    .await
                    .unwrap(),
                format!("{payload}-{index}")
            );
        }
        assert!(
            second_owned > 0,
            "new transfer process should own at least one job after store outage recovery"
        );

        second_child.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_mysql_rpc_report_storm_does_not_starve_terminal_reports() {
    let Some(mysql_url) = mysql_transfer_store_url("mysql-rpc-report-storm") else {
        eprintln!("skip mysql rpc report storm e2e: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let test_id = format!(
        "transfer-mysql-rpc-report-storm-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    fs::create_dir_all(&base_dir).unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Mysql;
    conf.transfer.mysql_url = mysql_url.clone();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.instance_id = "transfer-report-storm-rpc".to_string();
    conf.transfer.worker_threads = 4;
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 0);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let store = Arc::new(MysqlTransferStore::open(&mysql_url).unwrap());
        let now_ms = orpc::common::LocalTime::mills() as i64;
        let job_id = format!("{test_id}-job");
        let task_count = 16usize;
        let running_reports = 512usize;

        store
            .create_or_get_by_request_id(TransferJobRecord {
                job_key: format!("Load:file:///{test_id}:/storm/{test_id}"),
                job_id: job_id.clone(),
                run_id: 1,
                kind: TransferKind::Load,
                source_path: format!("file:///{test_id}"),
                target_path: format!("/storm/{test_id}"),
                command_json: "{}".to_string(),
                mount_snapshot_json: "{}".to_string(),
                secret_ref_json: "{}".to_string(),
                cluster_snapshot_version: 1,
                cv_metadata_epoch: None,
                state: curvine_common::state::TransferState::Pending,
                owner: String::new(),
                lease_epoch: 0,
                lease_expire_at: 0,
                cancel_requested: false,
                summary: TransferProgress::default(),
                client_request_id: format!("{test_id}-request"),
                submitter: "rpc-report-storm-e2e".to_string(),
                tenant: "default".to_string(),
                created_at: now_ms,
                updated_at: now_ms,
            })
            .unwrap();

        let tasks = (0..task_count)
            .map(|index| TransferTaskRecord {
                job_id: job_id.clone(),
                run_id: 1,
                task_id: format!("task-{index}"),
                attempt_id: 0,
                source_path: format!("file:///{test_id}/source-{index}"),
                target_path: format!("/storm/{test_id}/target-{index}"),
                worker_id: 0,
                worker_session_id: String::new(),
                source_read_plan_json: String::new(),
                report_target_json: String::new(),
                state: TransferTaskState::Pending,
                progress: TransferProgress::default(),
                retry_count: 0,
                attempt_started_at: 0,
                last_report_at: 0,
                stale_deadline_at: 0,
                updated_at: now_ms,
            })
            .collect::<Vec<_>>();
        store.insert_tasks(tasks).unwrap();
        let lease = store
            .acquire_runnable_transfer("report-storm-owner", 600_000, now_ms, 100)
            .unwrap()
            .expect("preloaded transfer should be acquirable");
        for index in 0..task_count {
            assert!(store
                .start_task_attempt(TaskAttemptStart {
                    job_id: job_id.clone(),
                    run_id: 1,
                    owner: lease.owner.clone(),
                    lease_epoch: lease.lease_epoch,
                    task_id: format!("task-{index}"),
                    attempt_id: 1,
                    worker_id: 10_000 + index as u32,
                    worker_session_id: format!("storm-session-{index}"),
                    report_target_json: "{}".to_string(),
                    now_ms,
                    stale_deadline_at: now_ms + 600_000,
                })
                .unwrap());
        }

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;
        let client = Arc::new(TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap());

        let workers = 16usize;
        let reports_per_worker = running_reports / workers;
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let client = client.clone();
            let job_id = job_id.clone();
            handles.push(tokio::spawn(async move {
                for index in 0..reports_per_worker {
                    let task_index = (worker * reports_per_worker + index) % task_count;
                    let info = TransferTaskReportInfo {
                        run_id: 1,
                        attempt_id: 1,
                        worker_id: 10_000 + task_index as u32,
                        worker_session_id: format!("storm-session-{task_index}"),
                        report_target: String::new(),
                        report_endpoints: Vec::new(),
                    };
                    let progress = JobTaskProgress {
                        state: JobTaskState::Loading,
                        loaded_size: index as i64,
                        total_size: running_reports as i64,
                        update_time: orpc::common::LocalTime::mills() as i64,
                        message: format!("running-{worker}-{index}"),
                    };
                    assert!(
                        client
                            .report_task(&job_id, format!("task-{task_index}"), &info, progress)
                            .await
                            .unwrap(),
                        "running report should be accepted by transfer rpc queue"
                    );
                }
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }

        for index in 0..task_count {
            let info = TransferTaskReportInfo {
                run_id: 1,
                attempt_id: 1,
                worker_id: 10_000 + index as u32,
                worker_session_id: format!("storm-session-{index}"),
                report_target: String::new(),
                report_endpoints: Vec::new(),
            };
            let progress = JobTaskProgress {
                state: JobTaskState::Completed,
                loaded_size: 1,
                total_size: 1,
                update_time: orpc::common::LocalTime::mills() as i64,
                message: format!("completed-{index}"),
            };
            assert!(
                client
                    .report_task(&job_id, format!("task-{index}"), &info, progress)
                    .await
                    .unwrap(),
                "terminal report should not be starved by previous report storm"
            );
        }

        let final_job = store.get_transfer(&job_id).unwrap().unwrap();
        assert_eq!(
            final_job.state,
            curvine_common::state::TransferState::Completed
        );
        assert_eq!(final_job.summary.loaded_size, task_count as i64);
        assert_eq!(final_job.summary.total_size, task_count as i64);
        let final_tasks = store.list_transfer_tasks(&job_id, 1).unwrap();
        assert_eq!(final_tasks.len(), task_count);
        assert!(
            final_tasks
                .iter()
                .all(|task| task.state == TransferTaskState::Completed),
            "all tasks should be completed after terminal report burst"
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_master_metadata_reader_is_development_only() {
    let mut conf = ClusterConf::default();
    conf.transfer.enabled = true;
    conf.transfer.store_url = "mysql://root:curvine@127.0.0.1:3306/curvine_transfer".to_string();
    conf.transfer.cv_metadata_reader = TransferCvMetadataReaderType::Master;

    let err = conf.transfer.init().unwrap_err();
    assert!(
        err.to_string()
            .contains("production transfer requires transfer.cv_metadata_reader=replica"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_transfer_export_file_end_to_end() {
    let test_id = format!(
        "transfer-export-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.cv_metadata_reader = TransferCvMetadataReaderType::Master;
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let source = Path::from_str("/mnt/export.txt").unwrap();
        fs.write_string(&source, "cv-to-ufs").await.unwrap();

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let target = Path::from_str(format!("file://{}/export.txt", ufs_dir.display())).unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Export,
                source_path: source.clone_uri(),
                target_path: target.clone_uri(),
                client_request_id: test_id.clone(),
                submitter: "e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let mut final_state = submit.state;
        let mut task_state = 0;
        for _ in 0..80 {
            let status = client
                .status_page(&submit.job_id, Some(10), None)
                .await
                .unwrap();
            final_state = status.state;
            if let Some(task) = status.tasks.first() {
                task_state = task.state;
            }
            if final_state == TransferStateProto::TransferCompleted as i32
                || final_state == TransferStateProto::TransferFailed as i32
            {
                assert!(!status.tasks.is_empty(), "status should return task page");
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        assert_eq!(final_state, TransferStateProto::TransferCompleted as i32);
        assert_eq!(
            task_state,
            TransferTaskStateProto::TransferTaskCompleted as i32
        );
        let content = fs::read_to_string(ufs_dir.join("export.txt")).unwrap();
        assert_eq!(content, "cv-to-ufs");

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_export_ufs_overwrite_policy() {
    let test_id = format!(
        "transfer-export-ufs-overwrite-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::write(ufs_dir.join("existing.txt"), b"old-ufs-content").unwrap();
    fs::create_dir_all(ufs_dir.join("existing-dir")).unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.cv_metadata_reader = TransferCvMetadataReaderType::Master;
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let source = Path::from_str("/mnt/export-overwrite.txt").unwrap();
        fs.write_string(&source, "new-ufs-content").await.unwrap();

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;
        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();

        let mut no_overwrite = TransferCommand {
            kind: TransferKind::Export,
            source_path: source.clone_uri(),
            target_path: format!("file://{}/existing.txt", ufs_dir.display()),
            client_request_id: format!("{test_id}-no-overwrite"),
            submitter: "export-ufs-overwrite-e2e".to_string(),
            tenant: "test".to_string(),
            options: Default::default(),
        };
        no_overwrite.set_overwrite(false);
        let no_overwrite_job = client.submit(no_overwrite).await.unwrap();
        let no_overwrite_status = assert_transfer_failed(&client, &no_overwrite_job.job_id).await;
        assert!(
            no_overwrite_status
                .progress
                .message
                .contains("already exists"),
            "overwrite=false should fail because UFS target exists, got: {}",
            no_overwrite_status.progress.message
        );
        assert_eq!(
            fs::read_to_string(ufs_dir.join("existing.txt")).unwrap(),
            "old-ufs-content"
        );

        let dir_target_job = client
            .submit(TransferCommand {
                kind: TransferKind::Export,
                source_path: source.clone_uri(),
                target_path: format!("file://{}/existing-dir", ufs_dir.display()),
                client_request_id: format!("{test_id}-dir-target"),
                submitter: "export-ufs-overwrite-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();
        let dir_target_status = assert_transfer_failed(&client, &dir_target_job.job_id).await;
        assert!(
            dir_target_status
                .progress
                .message
                .contains("refusing to overwrite directory with file"),
            "file export into UFS directory should fail clearly, got: {}",
            dir_target_status.progress.message
        );
        assert!(
            ufs_dir.join("existing-dir").is_dir(),
            "directory target should remain a directory"
        );

        let overwrite_job = client
            .submit(TransferCommand {
                kind: TransferKind::Export,
                source_path: source.clone_uri(),
                target_path: format!("file://{}/existing.txt", ufs_dir.display()),
                client_request_id: format!("{test_id}-overwrite"),
                submitter: "export-ufs-overwrite-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();
        let overwrite_status = wait_transfer_state(
            &client,
            &overwrite_job.job_id,
            TransferStateProto::TransferCompleted,
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(overwrite_status.tasks.len(), 1);
        assert_eq!(
            overwrite_status.tasks[0].state,
            TransferTaskStateProto::TransferTaskCompleted as i32
        );
        assert_eq!(
            fs::read_to_string(ufs_dir.join("existing.txt")).unwrap(),
            "new-ufs-content"
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_export_file_with_metadata_replica_reader() {
    let test_id = format!(
        "transfer-export-replica-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.cv_metadata_reader = TransferCvMetadataReaderType::Replica;
    conf.transfer.metadata_replica_refresh_interval_str = "1h".to_string();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let source = Path::from_str("/mnt/export-replica.txt").unwrap();
        fs.write_string(&source, "cv-to-ufs-replica").await.unwrap();

        let mut transfer_server = InProcessTransferServer::start(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;
        let metrics = read_http_metrics(cluster.cluster_conf.transfer.web_port);
        assert!(
            metrics.contains("transfer_metadata_replica_version"),
            "transfer metrics should expose metadata replica version"
        );
        assert!(
            metrics.contains("transfer_metadata_replica_entries"),
            "transfer metrics should expose metadata replica entry count"
        );
        assert!(
            metrics.contains("transfer_metadata_replica_page_size"),
            "transfer metrics should expose metadata replica page size"
        );
        assert!(
            metrics.contains("transfer_metadata_replica_pages"),
            "transfer metrics should expose metadata replica page count"
        );
        assert!(
            metrics.contains("transfer_metadata_replica_refresh_total"),
            "transfer metrics should expose metadata replica refresh attempts"
        );

        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let target =
            Path::from_str(format!("file://{}/export-replica.txt", ufs_dir.display())).unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Export,
                source_path: source.clone_uri(),
                target_path: target.clone_uri(),
                client_request_id: test_id.clone(),
                submitter: "replica-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let mut final_state = submit.state;
        for _ in 0..80 {
            let status = client
                .status_page(&submit.job_id, Some(10), None)
                .await
                .unwrap();
            final_state = status.state;
            if final_state == TransferStateProto::TransferCompleted as i32
                || final_state == TransferStateProto::TransferFailed as i32
            {
                assert!(!status.tasks.is_empty(), "status should return task page");
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        assert_eq!(final_state, TransferStateProto::TransferCompleted as i32);
        let content = fs::read_to_string(ufs_dir.join("export-replica.txt")).unwrap();
        assert_eq!(content, "cv-to-ufs-replica");

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_transfer_export_rejects_stale_metadata_replica() {
    let test_id = format!(
        "transfer-export-stale-replica-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    fs::create_dir_all(&ufs_dir).unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_type = TransferStoreType::Sqlite;
    conf.transfer.sqlite_path = base_dir.join("transfer.db").display().to_string();
    conf.transfer.cv_metadata_reader = TransferCvMetadataReaderType::Replica;
    conf.transfer.metadata_replica_refresh_interval_str = "1h".to_string();
    conf.transfer.metadata_replica_max_staleness_str = "1ms".to_string();
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();

        let source = Path::from_str("/mnt/stale-replica.txt").unwrap();
        fs.write_string(&source, "stale-replica").await.unwrap();

        let mut transfer_server =
            InProcessTransferServer::start_liveness(cluster.cluster_conf.clone());
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = TransferClient::with_rt(&cluster.cluster_conf, rt.clone()).unwrap();
        let target =
            Path::from_str(format!("file://{}/stale-replica.txt", ufs_dir.display())).unwrap();
        let submit = client
            .submit(TransferCommand {
                kind: TransferKind::Export,
                source_path: source.clone_uri(),
                target_path: target.clone_uri(),
                client_request_id: test_id.clone(),
                submitter: "stale-replica-e2e".to_string(),
                tenant: "test".to_string(),
                options: Default::default(),
            })
            .await
            .unwrap();

        assert_transfer_failed(&client, &submit.job_id).await;
        assert!(
            !ufs_dir.join("stale-replica.txt").exists(),
            "stale replica export must not create target data"
        );

        transfer_server.stop();
        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_metadata_replica_entry_limit_allows_exact_limit_only() {
    let test_id = format!(
        "transfer-replica-limit-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        let reader = MetadataReplicaReader::new(fs.clone(), 1, 1, 2, Duration::from_secs(60));
        reader
            .refresh()
            .await
            .expect("root-only namespace should fit exact entry limit");

        fs.mkdir(&Path::from_str("/limit-child").unwrap(), false)
            .await
            .unwrap();
        let err = reader.refresh().await.unwrap_err().to_string();
        assert!(
            err.contains("Metadata replica entry limit exceeded"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_metadata_replica_reader_serves_retained_epoch() {
    let test_id = format!(
        "transfer-replica-history-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mkdir(&Path::from_str("/history").unwrap(), false)
            .await
            .unwrap();
        fs.write_string(&Path::from_str("/history/old.txt").unwrap(), "old")
            .await
            .unwrap();

        let reader = MetadataReplicaReader::new(fs.clone(), 100, 1, 2, Duration::from_secs(60));
        let old_epoch = reader.refresh().await.unwrap();

        fs.write_string(&Path::from_str("/history/new.txt").unwrap(), "new")
            .await
            .unwrap();
        let new_epoch = reader.refresh().await.unwrap();
        assert_ne!(old_epoch, new_epoch);

        let old_path = Path::from_str("/history/old.txt").unwrap();
        let new_path = Path::from_str("/history/new.txt").unwrap();
        assert!(reader
            .get_status_at_epoch(&old_path, Some(old_epoch))
            .await
            .is_ok());
        assert!(matches!(
            reader
                .get_status_at_epoch(&new_path, Some(old_epoch))
                .await
                .unwrap_err(),
            FsError::FileNotFound(_)
        ));
        assert!(reader
            .get_status_at_epoch(&new_path, Some(new_epoch))
            .await
            .is_ok());

        fs.mkdir(&Path::from_str("/history/tree").unwrap(), false)
            .await
            .unwrap();
        fs.write_string(&Path::from_str("/history/tree/child.txt").unwrap(), "child")
            .await
            .unwrap();
        let tree_epoch = reader.refresh().await.unwrap();
        let tree_child = Path::from_str("/history/tree/child.txt").unwrap();
        assert!(reader
            .get_status_at_epoch(&tree_child, Some(tree_epoch))
            .await
            .is_ok());

        let moved_child = Path::from_str("/history/moved/child.txt").unwrap();
        fs.rename(
            &Path::from_str("/history/tree").unwrap(),
            &Path::from_str("/history/moved").unwrap(),
        )
        .await
        .unwrap();
        let moved_epoch = reader.refresh().await.unwrap();
        assert_ne!(tree_epoch, moved_epoch);
        assert!(matches!(
            reader
                .get_status_at_epoch(&tree_child, Some(moved_epoch))
                .await
                .unwrap_err(),
            FsError::FileNotFound(_)
        ));
        assert!(reader
            .get_status_at_epoch(&moved_child, Some(moved_epoch))
            .await
            .is_ok());

        fs.delete(&Path::from_str("/history/moved").unwrap(), true)
            .await
            .unwrap();
        let deleted_epoch = reader.refresh().await.unwrap();
        assert_ne!(moved_epoch, deleted_epoch);
        assert!(matches!(
            reader
                .get_status_at_epoch(&moved_child, Some(deleted_epoch))
                .await
                .unwrap_err(),
            FsError::FileNotFound(_)
        ));

        let evicting_reader =
            MetadataReplicaReader::new(fs.clone(), 100, 1, 1, Duration::from_secs(60));
        let evicted_epoch = evicting_reader.refresh().await.unwrap();
        fs.write_string(&Path::from_str("/history/latest.txt").unwrap(), "latest")
            .await
            .unwrap();
        let latest_epoch = evicting_reader.refresh().await.unwrap();
        assert_ne!(evicted_epoch, latest_epoch);
        let err = evicting_reader
            .get_status_at_epoch(&old_path, Some(evicted_epoch))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("is no longer available"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_metadata_replica_falls_back_when_delta_window_is_lost() {
    let test_id = format!(
        "transfer-replica-delta-fallback-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.journal.metadata_delta_log_capacity = 1;
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        let reader = MetadataReplicaReader::new(fs.clone(), 100, 1, 2, Duration::from_secs(60));
        let base_epoch = reader.refresh().await.unwrap();

        let evicted_path = Path::from_str("/delta-evicted").unwrap();
        let retained_path = Path::from_str("/delta-retained").unwrap();
        fs.mkdir(&evicted_path, false).await.unwrap();
        fs.mkdir(&retained_path, false).await.unwrap();

        let refreshed_epoch = reader.refresh().await.unwrap();
        assert_ne!(base_epoch, refreshed_epoch);
        assert!(reader.get_status(&evicted_path).await.is_ok());
        assert!(reader.get_status(&retained_path).await.is_ok());

        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_metadata_delta_requires_full_snapshot_when_target_epoch_advances_between_pages() {
    let test_id = format!(
        "transfer-replica-delta-epoch-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        let base_epoch = fs
            .get_cv_metadata_snapshot_page(None, Some(100))
            .await
            .unwrap()
            .epoch;

        fs.mkdir(&Path::from_str("/delta").unwrap(), false)
            .await
            .unwrap();
        fs.mkdir(&Path::from_str("/delta/child").unwrap(), false)
            .await
            .unwrap();

        let first = fs
            .get_cv_metadata_delta_page(base_epoch, None, None, Some(1))
            .await
            .unwrap();
        assert_eq!(first.entries.len(), 1);
        let page_token = first
            .next_page_token
            .clone()
            .expect("delta should span more than one page");

        fs.delete(&Path::from_str("/delta/child").unwrap(), true)
            .await
            .unwrap();

        let next = fs
            .get_cv_metadata_delta_page(base_epoch, Some(first.to_epoch), Some(page_token), Some(1))
            .await
            .unwrap();
        assert!(
            next.full_snapshot_required,
            "a fixed-epoch delta page must not be built from newer metadata"
        );
        assert!(next.entries.is_empty());

        let _ = fs::remove_dir_all(base_dir);
    });
}

#[test]
fn test_cv_metadata_snapshot_page_returns_statuses_and_blocks() {
    let test_id = format!(
        "transfer-replica-page-e2e-{}-{}",
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(cluster.cluster_conf.clone(), rt.clone()).unwrap();
        fs.mkdir(&Path::from_str("/snapshot").unwrap(), false)
            .await
            .unwrap();
        fs.mkdir(&Path::from_str("/snapshot/dir").unwrap(), false)
            .await
            .unwrap();
        fs.write_string(&Path::from_str("/snapshot/dir/file.txt").unwrap(), "page")
            .await
            .unwrap();

        let first = fs
            .get_cv_metadata_snapshot_page(None, Some(2))
            .await
            .unwrap();
        assert_eq!(first.entries.len(), 2);
        let token = first
            .next_page_token
            .clone()
            .expect("first page should have a next token");
        let second = fs
            .get_cv_metadata_snapshot_page(Some(token), Some(2))
            .await
            .unwrap();
        assert_eq!(
            first.epoch, second.epoch,
            "metadata snapshot pages must share one epoch"
        );

        let mut entries = first.entries;
        entries.extend(second.entries);
        let statuses = entries
            .iter()
            .map(|entry| ProtoUtils::file_status_from_pb(entry.status.clone()))
            .collect::<Vec<_>>();
        let paths = statuses
            .iter()
            .map(|status| status.path.as_str())
            .collect::<HashSet<_>>();
        assert!(paths.contains("/"));
        assert!(paths.contains("/snapshot"));
        assert!(paths.contains("/snapshot/dir"));
        assert!(paths.contains("/snapshot/dir/file.txt"));

        let file_entry = entries
            .iter()
            .find(|entry| entry.status.path == "/snapshot/dir/file.txt")
            .expect("snapshot page should include file entry");
        let blocks = file_entry
            .blocks
            .clone()
            .map(ProtoUtils::file_blocks_from_pb)
            .expect("file entry should carry block locations");
        assert_eq!(blocks.status.path, "/snapshot/dir/file.txt");
        assert!(!blocks.block_locs.is_empty());

        let _ = fs::remove_dir_all(base_dir);
    });
}

fn mysql_transfer_store_url(name: &str) -> Option<String> {
    mysql_transfer_store_url_with_db(name).map(|(store_url, _, _)| store_url)
}

struct MysqlTcpProxy {
    listen_addr: SocketAddr,
    available: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
    streams: Arc<Mutex<Vec<TcpStream>>>,
}

impl MysqlTcpProxy {
    fn start(target_addr: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let available = Arc::new(AtomicBool::new(true));
        let running = Arc::new(AtomicBool::new(true));
        let streams = Arc::new(Mutex::new(Vec::new()));
        let accept_available = available.clone();
        let accept_running = running.clone();
        let accept_streams = streams.clone();
        thread::spawn(move || {
            while accept_running.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((client, _)) if accept_available.load(Ordering::SeqCst) => {
                        let Ok(server) = TcpStream::connect(target_addr) else {
                            let _ = client.shutdown(Shutdown::Both);
                            continue;
                        };
                        if let Ok(mut guard) = accept_streams.lock() {
                            if let Ok(stream) = client.try_clone() {
                                guard.push(stream);
                            }
                            if let Ok(stream) = server.try_clone() {
                                guard.push(stream);
                            }
                        }
                        proxy_bidirectional(client, server);
                    }
                    Ok((client, _)) => {
                        let _ = client.shutdown(Shutdown::Both);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            listen_addr,
            available,
            running,
            streams,
        }
    }

    fn set_available(&self, available: bool) {
        self.available.store(available, Ordering::SeqCst);
        if !available {
            if let Ok(mut streams) = self.streams.lock() {
                for stream in streams.iter() {
                    let _ = stream.shutdown(Shutdown::Both);
                }
                streams.clear();
            }
        }
    }
}

impl Drop for MysqlTcpProxy {
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        self.set_available(false);
        let _ = TcpStream::connect(self.listen_addr);
    }
}

fn proxy_bidirectional(client: TcpStream, server: TcpStream) {
    let mut client_reader = client.try_clone().unwrap();
    let mut client_writer = client;
    let mut server_reader = server.try_clone().unwrap();
    let mut server_writer = server;
    thread::spawn(move || {
        let _ = std::io::copy(&mut client_reader, &mut server_writer);
        let _ = server_writer.shutdown(Shutdown::Write);
    });
    thread::spawn(move || {
        let _ = std::io::copy(&mut server_reader, &mut client_writer);
        let _ = client_writer.shutdown(Shutdown::Write);
    });
}

fn mysql_transfer_store_url_with_proxy(name: &str) -> Option<(String, MysqlTcpProxy)> {
    let (store_url, _, _) = mysql_transfer_store_url_with_db(name)?;
    let target_addr = mysql_url_host_port(&store_url)?
        .parse::<SocketAddr>()
        .ok()?;
    let proxy = MysqlTcpProxy::start(target_addr);
    let proxied_url = store_url.replace(
        &target_addr.to_string(),
        &format!("127.0.0.1:{}", proxy.listen_addr.port()),
    );
    Some((proxied_url, proxy))
}

fn mysql_url_host_port(url: &str) -> Option<String> {
    let after_at = url.split_once('@')?.1;
    let host_port = after_at
        .split(['/', '?'])
        .next()
        .filter(|value| value.contains(':'))?;
    Some(host_port.to_string())
}

fn mysql_transfer_store_url_with_db(name: &str) -> Option<(String, String, String)> {
    let base_url = std::env::var("CURVINE_TRANSFER_MYSQL_URL").ok()?;
    let safe_name = name.replace('-', "_");
    let safe_name = &safe_name[..safe_name.len().min(20)];
    let db_name = format!(
        "cv_transfer_{}_{}_{}",
        safe_name,
        std::process::id(),
        orpc::common::LocalTime::mills()
    );
    let pool = mysql::Pool::new(limited_mysql_pool_url(&base_url).as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(format!("create database `{}`", db_name))
        .unwrap();
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let store_url = format!(
        "{}/{}{}pool_min=0&pool_max=1",
        base_url.trim_end_matches('/'),
        db_name,
        separator
    );
    Some((store_url, base_url, db_name))
}

fn drop_mysql_database(base_url: &str, db_name: &str) {
    let pool = mysql::Pool::new(limited_mysql_pool_url(base_url).as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(format!("drop database if exists `{}`", db_name))
        .unwrap();
}

fn create_mysql_database(base_url: &str, db_name: &str) {
    let pool = mysql::Pool::new(limited_mysql_pool_url(base_url).as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(format!("create database `{}`", db_name))
        .unwrap();
}

fn limited_mysql_pool_url(url: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}pool_min=0&pool_max=1")
}

fn force_mysql_task_state(store_url: &str, task: &TransferTaskRecord) {
    let pool = mysql::Pool::new(store_url).unwrap();
    let mut conn = pool.get_conn().unwrap();
    let record_json = serde_json::to_string(task).unwrap();
    conn.exec_drop(
        "update transfer_tasks
         set state = :state, attempt_id = :attempt_id, worker_id = :worker_id,
             worker_session_id = :worker_session_id, stale_deadline_at = :stale_deadline_at,
             record_json = :record_json, updated_at = :updated_at
         where job_id = :job_id and run_id = :run_id and task_id = :task_id",
        params! {
            "state" => task.state as i32,
            "attempt_id" => task.attempt_id,
            "worker_id" => task.worker_id,
            "worker_session_id" => &task.worker_session_id,
            "stale_deadline_at" => task.stale_deadline_at,
            "record_json" => record_json,
            "updated_at" => task.updated_at,
            "job_id" => &task.job_id,
            "run_id" => task.run_id,
            "task_id" => &task.task_id,
        },
    )
    .unwrap();
}

async fn wait_for_mysql_running_task_after_local_commit(
    fs: &CurvineFileSystem,
    store: &MysqlTransferStore,
    job_id: &str,
    run_id: u64,
    target: &Path,
    expected_content: &str,
    timeout: Duration,
) -> Option<TransferTaskRecord> {
    let deadline = Instant::now() + timeout;
    loop {
        if fs.read_string(target).await.ok().as_deref() == Some(expected_content) {
            let tasks = store.list_transfer_tasks(job_id, run_id).unwrap();
            if let Some(task) = tasks
                .into_iter()
                .find(|task| task.state == TransferTaskState::Running)
            {
                return Some(task);
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_master_and_create_fs(cluster: &MiniCluster) -> CurvineFileSystem {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let fs = cluster.new_fs();
    while std::time::Instant::now() < deadline {
        if fs.get_master_info().await.is_ok() {
            return fs;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("master did not become ready within 60 seconds");
}

struct RegisteredWorker {
    worker_id: u32,
    worker_session_id: String,
}

async fn wait_for_registered_worker_session(
    fs: &CurvineFileSystem,
    previous_session_id: Option<&str>,
    timeout: Duration,
    log_path: &std::path::Path,
    child: &mut ServiceChild,
) -> RegisteredWorker {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(info) = fs.get_master_info().await {
            if let Some(worker) = info.live_workers.into_iter().find(|worker| {
                !worker.worker_session_id.is_empty()
                    && previous_session_id
                        .map(|previous| worker.worker_session_id != previous)
                        .unwrap_or(true)
            }) {
                return RegisteredWorker {
                    worker_id: worker.worker_id(),
                    worker_session_id: worker.worker_session_id,
                };
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    child.stop();
    panic!(
        "worker did not register with expected session; child log: {}",
        fs::read_to_string(log_path).unwrap_or_default()
    );
}

async fn wait_worker_task_visible(
    fs: &CurvineFileSystem,
    conf: &ClusterConf,
    rt: Arc<orpc::runtime::Runtime>,
    task: &TransferTaskRecord,
) -> bool {
    let factory = UfsFactory::with_rt(&conf.client, rt);
    for _ in 0..120 {
        if let Ok(info) = fs.get_master_info().await {
            if let Some(worker) = info
                .live_workers
                .into_iter()
                .find(|worker| worker.worker_id() == task.worker_id)
            {
                if let Ok(client) = factory.get_worker_client(&worker.address).await {
                    if let Ok(response) = client
                        .query_transfer_task(
                            &task.job_id,
                            task.run_id,
                            &task.task_id,
                            task.attempt_id,
                            &task.worker_session_id,
                        )
                        .await
                    {
                        if response.found {
                            return true;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn assert_transfer_failed(
    client: &TransferClient,
    job_id: &str,
) -> GetTransferStatusResponse {
    let mut final_status = None;
    for _ in 0..80 {
        let status = client.status_page(job_id, Some(10), None).await.unwrap();
        let state = status.state;
        final_status = Some(status);
        if state == TransferStateProto::TransferCompleted as i32
            || state == TransferStateProto::TransferFailed as i32
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let final_status = final_status.expect("transfer status should be available");
    assert_eq!(
        final_status.state,
        TransferStateProto::TransferFailed as i32
    );
    final_status
}

async fn wait_transfer_state(
    client: &TransferClient,
    job_id: &str,
    expected: TransferStateProto,
    timeout: Duration,
) -> GetTransferStatusResponse {
    let deadline = std::time::Instant::now() + timeout;
    let mut last = None;
    while std::time::Instant::now() < deadline {
        let status = client.status_page(job_id, Some(100), None).await.unwrap();
        if status.state == expected as i32 {
            return status;
        }
        last = Some(status);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!(
        "transfer {job_id} did not reach {:?} within {:?}; last status: {:?}",
        expected, timeout, last
    );
}

async fn wait_transfer_task_state(
    client: &TransferClient,
    job_id: &str,
    expected: TransferTaskStateProto,
    timeout: Duration,
) -> GetTransferStatusResponse {
    let deadline = std::time::Instant::now() + timeout;
    let mut last = None;
    while std::time::Instant::now() < deadline {
        let status = client.status_page(job_id, Some(100), None).await.unwrap();
        if status
            .tasks
            .iter()
            .any(|task| task.state == expected as i32)
        {
            return status;
        }
        last = Some(status);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "transfer {job_id} did not have task state {:?} within {:?}; last status: {:?}",
        expected, timeout, last
    );
}

fn count_jobs_with_owner(store: &MysqlTransferStore, job_ids: &[String], owner: &str) -> usize {
    job_ids
        .iter()
        .filter(|job_id| {
            store
                .get_transfer(job_id)
                .unwrap()
                .map(|job| job.owner == owner)
                .unwrap_or(false)
        })
        .count()
}

fn transfer_state_summary(store: &MysqlTransferStore, job_ids: &[String]) -> String {
    let mut states = std::collections::BTreeMap::new();
    for job_id in job_ids {
        let state = store
            .get_transfer(job_id)
            .unwrap()
            .map(|job| format!("{:?}", job.state))
            .unwrap_or_else(|| "Missing".to_string());
        *states.entry(state).or_insert(0usize) += 1;
    }
    states
        .into_iter()
        .map(|(state, count)| format!("{state}={count}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn spawn_transfer_child_until_healthy(
    binary: &str,
    mut conf: ClusterConf,
    base_dir: &std::path::Path,
    name: &str,
) -> (ClusterConf, TransferChild) {
    for attempt in 0..5 {
        if attempt > 0 {
            conf.transfer.rpc_port = NetUtils::get_available_port();
            conf.transfer.web_port = NetUtils::get_available_port();
            conf.transfer.endpoints = vec![format!(
                "{}:{}",
                conf.transfer.hostname, conf.transfer.rpc_port
            )];
            conf.transfer.init().unwrap();
        }
        let conf_path = base_dir.join(format!("{name}-{attempt}.toml"));
        fs::write(&conf_path, toml::to_string(&conf).unwrap()).unwrap();
        let child_log_path = base_dir.join(format!("{name}-{attempt}.log"));
        let mut child = TransferChild::spawn(binary, &conf_path, &child_log_path);
        if wait_http_path(conf.transfer.web_port, "/readyz", Duration::from_secs(90)) {
            return (conf, child);
        }
        child.stop();
        let child_log = fs::read_to_string(&child_log_path).unwrap_or_default();
        if attempt == 4 {
            panic!("external transfer process did not become healthy; child log: {child_log}");
        }
        assert!(
            child_log.contains("Address already in use"),
            "external transfer process failed for non-port-conflict reason; child log: {child_log}"
        );
    }
    unreachable!("transfer child health retry loop must return or panic")
}

struct TransferChild {
    child: Child,
}

impl TransferChild {
    fn spawn(binary: &str, conf_path: &std::path::Path, log_path: &std::path::Path) -> Self {
        let stdout = fs::File::create(log_path)
            .unwrap_or_else(|err| panic!("failed to create transfer child log: {err}"));
        let stderr = stdout
            .try_clone()
            .unwrap_or_else(|err| panic!("failed to clone transfer child log: {err}"));
        let child = Command::new(binary)
            .arg("--service")
            .arg("transfer")
            .arg("--conf")
            .arg(conf_path)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap_or_else(|err| panic!("failed to spawn external transfer process: {err}"));
        Self { child }
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for TransferChild {
    fn drop(&mut self) {
        self.stop();
    }
}

struct InProcessTransferServer {
    shutdown: Option<TransferServerShutdown>,
    handle: Option<thread::JoinHandle<()>>,
}

impl InProcessTransferServer {
    fn start(conf: ClusterConf) -> Self {
        Self::start_with_probe(conf, "/readyz")
    }

    fn start_liveness(conf: ClusterConf) -> Self {
        Self::start_with_probe(conf, "/healthz")
    }

    fn start_with_probe(conf: ClusterConf, probe_path: &'static str) -> Self {
        let web_port = conf.transfer.web_port;
        let transfer = TransferServer::with_conf(conf).unwrap();
        let shutdown = transfer.shutdown_handle();
        let handle = thread::spawn(move || {
            if let Err(err) = transfer.block_on_start() {
                panic!("in-process transfer server failed: {err}");
            }
        });
        let mut server = Self {
            shutdown: Some(shutdown),
            handle: Some(handle),
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            if wait_http_path(web_port, probe_path, Duration::from_millis(200)) {
                return server;
            }
            if server
                .handle
                .as_ref()
                .map(|handle| handle.is_finished())
                .unwrap_or(false)
            {
                server.stop();
                panic!("in-process transfer server exited before it became healthy");
            }
        }
        server.stop();
        panic!("in-process transfer server did not pass {probe_path} within 30 seconds");
    }

    fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.shutdown();
        }
        if let Some(handle) = self.handle.take() {
            handle
                .join()
                .unwrap_or_else(|err| panic!("in-process transfer server panicked: {err:?}"));
        }
    }
}

impl Drop for InProcessTransferServer {
    fn drop(&mut self) {
        self.stop();
    }
}

struct ServiceChild {
    child: Child,
}

impl ServiceChild {
    fn spawn(
        binary: &str,
        service: &str,
        conf_path: &std::path::Path,
        log_path: &std::path::Path,
    ) -> Self {
        let stdout = fs::File::create(log_path)
            .unwrap_or_else(|err| panic!("failed to create {service} child log: {err}"));
        let stderr = stdout
            .try_clone()
            .unwrap_or_else(|err| panic!("failed to clone {service} child log: {err}"));
        let child = Command::new(binary)
            .arg("--service")
            .arg(service)
            .arg("--conf")
            .arg(conf_path)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap_or_else(|err| panic!("failed to spawn external {service} process: {err}"));
        Self { child }
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ServiceChild {
    fn drop(&mut self) {
        self.stop();
    }
}

fn curvine_server_binary() -> Option<String> {
    if let Some(path) = option_env!("CARGO_BIN_EXE_curvine-server") {
        return Some(path.to_string());
    }
    let current = std::env::current_exe().ok()?;
    let debug_dir = current.parent()?.parent()?;
    let candidate: PathBuf = debug_dir.join("curvine-server");
    candidate.exists().then(|| candidate.display().to_string())
}

fn wait_http_path(port: u16, path: &str, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(response) = try_read_http_path(port, path) {
            if response.contains("200 OK") {
                return true;
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn read_http_metrics(port: u16) -> String {
    read_http_path(port, "/metrics")
}

fn read_http_path(port: u16, path: &str) -> String {
    try_read_http_path(port, path).unwrap()
}

fn try_read_http_path(port: u16, path: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}
