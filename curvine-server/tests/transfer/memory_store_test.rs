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

use curvine_common::conf::{JournalConf, TransferConf, TransferCvMetadataReaderType};
use curvine_common::error::ErrorKind;
use curvine_common::proto::{
    SubmitTransferRequest, TransferKindProto, TransferProgressProto, TransferTaskReportRequest,
    TransferTaskStateProto,
};
use curvine_common::state::{
    TaskAttemptStart, TransferCommand, TransferJobRecord, TransferKind, TransferListFilter,
    TransferProgress, TransferState, TransferStateUpdate, TransferTaskRecord, TransferTaskReport,
    TransferTaskState,
};
use curvine_server::transfer::{
    MemoryTransferStore, TransferPlannedTasks, TransferService, TransferStore,
};
use std::sync::Arc;
use std::time::Duration;

fn job(job_id: &str, request_id: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: "Load:s3://bucket/a:/a".to_string(),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: "s3://bucket/a".to_string(),
        target_path: "/a".to_string(),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: request_id.to_string(),
        submitter: "sr".to_string(),
        tenant: "default".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

fn task(job_id: &str) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: job_id.to_string(),
        run_id: 1,
        task_id: "task-1".to_string(),
        attempt_id: 0,
        source_path: "s3://bucket/a".to_string(),
        target_path: "/a".to_string(),
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
        updated_at: 0,
    }
}

fn submit_request(request_id: &str, source: &str, target: &str) -> SubmitTransferRequest {
    SubmitTransferRequest {
        kind: TransferKindProto::TransferLoad as i32,
        source_path: source.to_string(),
        target_path: target.to_string(),
        client_request_id: request_id.to_string(),
        submitter: "sr".to_string(),
        tenant: "default".to_string(),
        command: Vec::new(),
        protocol_version: Some(1),
    }
}

fn submit_request_with_kind(
    request_id: &str,
    kind: TransferKindProto,
    source: &str,
    target: &str,
) -> SubmitTransferRequest {
    SubmitTransferRequest {
        kind: kind as i32,
        source_path: source.to_string(),
        target_path: target.to_string(),
        client_request_id: request_id.to_string(),
        submitter: "sr".to_string(),
        tenant: "default".to_string(),
        command: Vec::new(),
        protocol_version: Some(1),
    }
}

#[test]
fn retry_transfer_creates_a_new_job_with_the_original_command() {
    let store = Arc::new(MemoryTransferStore::new());
    let service = TransferService::new(store.clone());
    let mut command = TransferCommand {
        kind: TransferKind::Load,
        source_path: "s3://bucket/retry-source".to_string(),
        target_path: "/retry-target".to_string(),
        client_request_id: "retry-original".to_string(),
        submitter: "operator".to_string(),
        tenant: "tenant-a".to_string(),
        options: Default::default(),
    };
    command.set_overwrite(false);
    let submitted = service
        .submit_transfer(SubmitTransferRequest {
            kind: TransferKindProto::TransferLoad as i32,
            source_path: command.source_path.clone(),
            target_path: command.target_path.clone(),
            client_request_id: command.client_request_id.clone(),
            submitter: command.submitter.clone(),
            tenant: command.tenant.clone(),
            command: serde_json::to_vec(&command).unwrap(),
            protocol_version: Some(1),
        })
        .unwrap();
    let lease = store
        .acquire_runnable_transfer("retry-test", 1_000, 10, 100)
        .unwrap()
        .unwrap();
    assert!(store
        .update_transfer_state(TransferStateUpdate {
            job_id: submitted.job_id.clone(),
            run_id: submitted.run_id,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            from_states: vec![TransferState::Planning],
            to_state: TransferState::Failed,
            message: "source object not found".to_string(),
            now_ms: 11,
        })
        .unwrap());

    let retried = service.retry_transfer(&submitted.job_id).unwrap();
    let retried_command: TransferCommand = serde_json::from_str(&retried.command_json).unwrap();

    assert_ne!(retried.job_id, submitted.job_id);
    assert_ne!(retried.client_request_id, submitted.client_request_id);
    assert_eq!(retried.state, TransferState::Pending);
    assert_eq!(retried_command.kind, command.kind);
    assert_eq!(retried_command.source_path, command.source_path);
    assert_eq!(retried_command.target_path, command.target_path);
    assert_eq!(retried_command.submitter, command.submitter);
    assert_eq!(retried_command.tenant, command.tenant);
    assert_eq!(retried_command.options, command.options);
}

#[test]
fn transfer_conf_rejects_zero_scheduler_limits() {
    let mut conf = TransferConf {
        enabled: true,
        ufs_max_concurrency_per_endpoint: 0,
        ..Default::default()
    };
    let err = conf.init().unwrap_err().to_string();
    assert!(
        err.contains("transfer.ufs_max_concurrency_per_endpoint must be greater than 0"),
        "unexpected error: {err}"
    );

    let mut conf = TransferConf {
        enabled: true,
        metadata_replica_refresh_interval_str: "0ms".to_string(),
        ..Default::default()
    };
    let err = conf.init().unwrap_err().to_string();
    assert!(
        err.contains("transfer.metadata_replica_refresh_interval must be greater than 0"),
        "unexpected error: {err}"
    );
}

#[test]
fn transfer_conf_derives_recovery_timeouts_from_lease() {
    let mut conf = TransferConf {
        enabled: true,
        lease_timeout_str: "6s".to_string(),
        ..Default::default()
    };
    conf.init().unwrap();
    assert_eq!(conf.lease_timeout, Duration::from_secs(6));
    assert_eq!(conf.task_stale_timeout, Duration::from_secs(3));
    assert_eq!(conf.task_report_interval, Duration::from_millis(500));

    let mut conf = TransferConf {
        enabled: true,
        lease_timeout_str: "0ms".to_string(),
        ..Default::default()
    };
    let err = conf.init().unwrap_err().to_string();
    assert!(
        err.contains("transfer.lease_timeout must be greater than 0"),
        "unexpected error: {err}"
    );

    let mut conf = TransferConf {
        enabled: true,
        cluster_snapshot_max_staleness_str: "0ms".to_string(),
        ..Default::default()
    };
    let err = conf.init().unwrap_err().to_string();
    assert!(
        err.contains("transfer.cluster_snapshot_max_staleness must be greater than 0"),
        "unexpected error: {err}"
    );
}

#[test]
fn transfer_conf_rejects_multiple_endpoints_with_local_sqlite() {
    let mut conf = TransferConf {
        enabled: true,
        endpoints: vec!["transfer-a:9010".to_string(), "transfer-b:9010".to_string()],
        ..Default::default()
    };

    let err = conf.init().unwrap_err().to_string();

    assert!(
        err.contains("multiple transfer.endpoints require transfer.store_url=mysql://"),
        "unexpected error: {err}"
    );
}

#[test]
fn submit_transfer_rejects_command_identity_mismatch() {
    let service = TransferService::new(Arc::new(MemoryTransferStore::new()));
    let command = TransferCommand {
        kind: TransferKind::Load,
        source_path: "s3://bucket/source".to_string(),
        target_path: "/target".to_string(),
        client_request_id: "payload-request".to_string(),
        submitter: "payload-submitter".to_string(),
        tenant: "payload-tenant".to_string(),
        options: Default::default(),
    };

    let err = service
        .submit_transfer(SubmitTransferRequest {
            kind: TransferKindProto::TransferLoad as i32,
            source_path: command.source_path.clone(),
            target_path: command.target_path.clone(),
            client_request_id: "header-request".to_string(),
            submitter: command.submitter.clone(),
            tenant: command.tenant.clone(),
            command: serde_json::to_vec(&command).unwrap(),
            protocol_version: Some(1),
        })
        .unwrap_err();

    assert!(err.to_string().contains("client request id"));
}

#[test]
fn expired_owner_cannot_advance_transfer_state() {
    let store = MemoryTransferStore::new();
    store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();

    assert!(!store
        .update_transfer_state(TransferStateUpdate {
            job_id: lease.job_id,
            run_id: lease.run_id,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            from_states: vec![TransferState::Planning],
            to_state: TransferState::Dispatching,
            message: "stale planning result".to_string(),
            now_ms: 110,
        })
        .unwrap());
}

#[test]
fn expired_owner_cannot_persist_planned_tasks() {
    let store = MemoryTransferStore::new();
    store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();

    assert!(!store
        .persist_planned_tasks(TransferPlannedTasks {
            job_id: lease.job_id.clone(),
            run_id: lease.run_id,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            tasks: vec![task("job-1")],
            message: "planned total_size=0".to_string(),
            now_ms: 110,
        })
        .unwrap());
    assert!(store.list_transfer_tasks("job-1", 1).unwrap().is_empty());
    assert_eq!(
        store.get_transfer("job-1").unwrap().unwrap().state,
        TransferState::Planning
    );
}

#[test]
fn task_report_updates_progress_without_terminalizing_transfer() {
    let store = MemoryTransferStore::new();
    store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();
    store.insert_tasks(vec![task("job-1")]).unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: "job-1".to_string(),
            run_id: 1,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 1,
            worker_session_id: "worker-a".to_string(),
            report_target_json: "[]".to_string(),
            now_ms: 20,
            stale_deadline_at: 70,
        })
        .unwrap());

    assert!(store
        .update_task_report(TransferTaskReport {
            job_id: "job-1".to_string(),
            run_id: 1,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 1,
            worker_session_id: "worker-a".to_string(),
            state: TransferTaskState::Completed,
            progress: TransferProgress {
                loaded_size: 10,
                total_size: 10,
                update_time: 30,
                message: "copied".to_string(),
            },
            now_ms: 30,
            stale_deadline_at: 80,
        })
        .unwrap());

    let job = store.get_transfer("job-1").unwrap().unwrap();
    assert_eq!(job.state, TransferState::Planning);
    assert_eq!(job.summary.loaded_size, 10);
    assert_eq!(job.summary.total_size, 10);
}

#[test]
fn terminal_report_cannot_override_canceling_transfer() {
    let store = MemoryTransferStore::new();
    store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();
    store.insert_tasks(vec![task("job-1")]).unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: "job-1".to_string(),
            run_id: 1,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 1,
            worker_session_id: "worker-a".to_string(),
            report_target_json: "[]".to_string(),
            now_ms: 20,
            stale_deadline_at: 70,
        })
        .unwrap());
    assert!(store.request_cancel("job-1", 1, 21).unwrap());

    assert!(store
        .update_task_report(TransferTaskReport {
            job_id: "job-1".to_string(),
            run_id: 1,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 1,
            worker_session_id: "worker-a".to_string(),
            state: TransferTaskState::Completed,
            progress: TransferProgress {
                loaded_size: 10,
                total_size: 10,
                update_time: 30,
                message: "completed after cancel request".to_string(),
            },
            now_ms: 30,
            stale_deadline_at: 80,
        })
        .unwrap());

    assert_eq!(
        store.get_transfer("job-1").unwrap().unwrap().state,
        TransferState::Canceling
    );
}

#[test]
fn transfer_conf_keeps_internal_knobs_out_of_config_surface() {
    let toml = r#"
enabled = true
io_threads = 0
worker_threads = 0
metadata_replica_max_entries = 0
metadata_replica_page_size = 0
metadata_replica_history_size = 0
allow_submit_with_stale_snapshot = true
max_planning_transfers = 0
max_active_transfers = 0
max_queued_transfers = 0
max_tasks_per_transfer = 0
ufs_max_concurrency_per_endpoint = 0
task_max_retries = 0
task_report_interval = "0ms"
task_stale_timeout = "0ms"
planning_batch_size = 0
worker_dispatch_concurrency = 0
task_report_queue_size = 0
task_probe_concurrency = 0
worker_report_retry_queue_size = 0
client_pending_queue_size = 0
status_page_size = 0
watch_queue_size = 0
cleanup_batch_size = 0
cluster_snapshot_max_staleness = "0ms"
metadata_replica_refresh_interval = "0ms"
metadata_replica_max_staleness = "0ms"
"#;

    let mut conf: TransferConf = toml::from_str(toml).unwrap();
    conf.init().unwrap();

    assert_eq!(
        conf.metadata_replica_page_size(),
        TransferConf::DEFAULT_METADATA_REPLICA_PAGE_SIZE
    );
    assert_eq!(
        conf.metadata_replica_history_size(),
        TransferConf::DEFAULT_METADATA_REPLICA_HISTORY_SIZE
    );
    assert_eq!(
        conf.scheduler_workers(),
        TransferConf::DEFAULT_MAX_PLANNING_TRANSFERS
    );
    assert_eq!(
        conf.planning_batch_size(),
        TransferConf::DEFAULT_PLANNING_BATCH_SIZE
    );
    assert_eq!(
        conf.worker_dispatch_concurrency(),
        TransferConf::DEFAULT_WORKER_DISPATCH_CONCURRENCY
    );
    assert_eq!(
        conf.task_report_queue_size(),
        TransferConf::DEFAULT_TASK_REPORT_QUEUE_SIZE
    );
    assert_eq!(
        conf.task_probe_concurrency(),
        TransferConf::DEFAULT_TASK_PROBE_CONCURRENCY
    );
    assert_eq!(
        conf.cleanup_batch_size(),
        TransferConf::DEFAULT_CLEANUP_BATCH_SIZE
    );
    assert_eq!(conf.io_threads, TransferConf::DEFAULT_IO_THREADS);
    assert_eq!(conf.worker_threads, TransferConf::DEFAULT_WORKER_THREADS);
    assert_eq!(
        conf.metadata_replica_max_entries,
        TransferConf::DEFAULT_METADATA_REPLICA_MAX_ENTRIES
    );
    assert!(!conf.allow_submit_with_stale_snapshot);
    assert_eq!(
        conf.max_tasks_per_transfer,
        TransferConf::DEFAULT_MAX_TASKS_PER_TRANSFER
    );
    assert_eq!(
        conf.ufs_max_concurrency_per_endpoint,
        TransferConf::DEFAULT_UFS_MAX_CONCURRENCY_PER_ENDPOINT
    );
    assert_eq!(
        conf.task_max_retries,
        TransferConf::DEFAULT_TASK_MAX_RETRIES
    );
    assert_eq!(conf.lease_timeout, Duration::from_secs(120));
    assert_eq!(conf.task_stale_timeout, Duration::from_secs(60));
    assert_eq!(conf.task_report_interval, Duration::from_secs(10));
    assert_eq!(
        conf.cluster_snapshot_max_staleness_str,
        TransferConf::DEFAULT_CLUSTER_SNAPSHOT_MAX_STALENESS
    );
    assert_eq!(
        conf.metadata_replica_refresh_interval_str,
        TransferConf::DEFAULT_METADATA_REPLICA_REFRESH_INTERVAL
    );
    assert_eq!(
        conf.metadata_replica_max_staleness_str,
        TransferConf::DEFAULT_METADATA_REPLICA_MAX_STALENESS
    );

    let serialized = toml::to_string(&conf).unwrap();
    for hidden in [
        "io_threads",
        "worker_threads",
        "allow_memory_store",
        "allow_master_metadata_reader",
        "metadata_replica_max_entries",
        "metadata_replica_page_size",
        "metadata_replica_history_size",
        "allow_submit_with_stale_snapshot",
        "max_planning_transfers",
        "max_active_transfers",
        "max_queued_transfers",
        "max_tasks_per_transfer",
        "ufs_max_concurrency_per_endpoint",
        "task_max_retries",
        "task_report_interval",
        "task_stale_timeout",
        "planning_batch_size",
        "worker_dispatch_concurrency",
        "task_report_queue_size",
        "task_probe_concurrency",
        "worker_report_retry_queue_size",
        "client_pending_queue_size",
        "status_page_size",
        "watch_queue_size",
        "cleanup_batch_size",
        "cluster_snapshot_max_staleness",
        "metadata_replica_refresh_interval",
        "metadata_replica_max_staleness",
    ] {
        assert!(
            !serialized.contains(hidden),
            "internal knob leaked into transfer config: {hidden}\n{serialized}"
        );
    }
}

#[test]
fn journal_conf_keeps_transfer_delta_capacity_out_of_config_surface() {
    let toml = r#"
metadata_delta_log_capacity = 0
"#;

    let conf: JournalConf = toml::from_str(toml).unwrap();
    assert_eq!(
        conf.metadata_delta_log_capacity,
        JournalConf::DEFAULT_METADATA_DELTA_LOG_CAPACITY
    );

    let serialized = toml::to_string(&conf).unwrap();
    assert!(
        !serialized.contains("metadata_delta_log_capacity"),
        "internal transfer delta capacity leaked into journal config: {serialized}"
    );
}

#[test]
fn mysql_store_enforces_safe_defaults() {
    let mut conf = TransferConf {
        enabled: true,
        store_url: "mysql://root:curvine@127.0.0.1:3306/curvine_transfer".to_string(),
        cv_metadata_reader: TransferCvMetadataReaderType::Disabled,
        ..Default::default()
    };
    let err = conf.init().unwrap_err().to_string();
    assert!(
        err.contains("production transfer requires transfer.cv_metadata_reader=replica"),
        "unexpected error: {err}"
    );

    let mut conf = TransferConf {
        enabled: true,
        store_url: "mysql://root:curvine@127.0.0.1:3306/curvine_transfer".to_string(),
        ..Default::default()
    };
    conf.init().unwrap();

    let mut conf = TransferConf {
        enabled: true,
        store_url: "mysql://root:curvine@127.0.0.1:3306/curvine_transfer".to_string(),
        allow_submit_with_stale_snapshot: true,
        ..Default::default()
    };
    let err = conf.init().unwrap_err().to_string();
    assert!(
        err.contains("production transfer forbids transfer.allow_submit_with_stale_snapshot=true"),
        "unexpected error: {err}"
    );
}

#[test]
fn submit_is_idempotent_by_submitter_and_request_id() {
    let store = MemoryTransferStore::new();

    let first = store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();
    let second = store
        .create_or_get_by_request_id(job("job-2", "req-1"))
        .unwrap();

    assert_eq!(first.job_id, "job-1");
    assert_eq!(second.job_id, "job-1");
}

#[test]
fn submit_rejects_conflicting_target_tree() {
    let store = Arc::new(MemoryTransferStore::new());
    let service = TransferService::new(store);

    let first = service
        .submit_transfer(submit_request("req-1", "s3://bucket/a", "/a"))
        .unwrap();
    let replay = service
        .submit_transfer(submit_request("req-1", "s3://bucket/a", "/a"))
        .unwrap();
    assert_eq!(first.job_id, replay.job_id);

    let conflict = service
        .submit_transfer(submit_request("req-2", "s3://bucket/b", "/a/child"))
        .unwrap_err();
    assert!(
        matches!(conflict.kind(), ErrorKind::TransferTargetConflict),
        "unexpected conflict error: {conflict}"
    );

    let root_conflict = service
        .submit_transfer(submit_request("req-3", "s3://bucket/root", "/"))
        .unwrap_err();
    assert!(
        matches!(root_conflict.kind(), ErrorKind::TransferTargetConflict),
        "unexpected root conflict error: {root_conflict}"
    );
}

#[test]
fn submit_rejects_same_job_key_with_different_command() {
    let store = Arc::new(MemoryTransferStore::new());
    let service = TransferService::new(store);

    service
        .submit_transfer(submit_request("req-1", "s3://bucket/a", "/a"))
        .unwrap();
    let mut request = submit_request("req-2", "s3://bucket/a", "/a");
    let mut command = curvine_common::state::TransferCommand {
        kind: TransferKind::Load,
        source_path: "s3://bucket/a".to_string(),
        target_path: "/a".to_string(),
        client_request_id: "req-2".to_string(),
        submitter: "sr".to_string(),
        tenant: "default".to_string(),
        options: Default::default(),
    };
    command
        .options
        .insert("overwrite".to_string(), "true".to_string());
    request.command = serde_json::to_vec(&command).unwrap();

    let err = service.submit_transfer(request).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::TransferAlreadyRunning));
}

#[test]
fn submit_accepts_pending_backlog_and_keeps_idempotency() {
    let store = Arc::new(MemoryTransferStore::new());
    let service = TransferService::with_task_stale_timeout(store.clone(), Duration::from_secs(60));

    let first = service
        .submit_transfer(submit_request("req-1", "s3://bucket/a", "/a"))
        .unwrap();
    let replay = service
        .submit_transfer(submit_request("req-1", "s3://bucket/a", "/a"))
        .unwrap();
    assert_eq!(first.job_id, replay.job_id);

    let second = service
        .submit_transfer(submit_request("req-2", "s3://bucket/b", "/b"))
        .unwrap();
    assert_ne!(first.job_id, second.job_id);
    assert_eq!(store.count_active_transfers().unwrap(), 2);
    assert_eq!(store.count_executing_transfers().unwrap(), 0);
}

#[test]
fn acquire_can_skip_pending_backlog_without_blocking_executing_jobs() {
    let store = MemoryTransferStore::new();
    store
        .create_or_get_by_request_id(job("pending", "req-pending"))
        .unwrap();

    let mut running = job("running", "req-running");
    running.job_key = "Load:s3://bucket/running:/running".to_string();
    running.source_path = "s3://bucket/running".to_string();
    running.target_path = "/running".to_string();
    running.state = TransferState::Running;
    running.updated_at = 1;
    store.create_or_get_by_request_id(running).unwrap();

    let lease = store
        .acquire_runnable_transfer("owner-a", 1000, 10, 1)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, "running");

    let pending = store.get_transfer("pending").unwrap().unwrap();
    assert_eq!(pending.state, TransferState::Pending);
    assert_eq!(store.count_active_transfers().unwrap(), 2);
    assert_eq!(store.count_executing_transfers().unwrap(), 1);
}

#[test]
fn acquire_pending_prefers_tenant_with_fewer_executing_jobs() {
    let store = MemoryTransferStore::new();

    let mut running = job("running-noisy", "req-running-noisy");
    running.job_key = "Load:s3://bucket/running-noisy:/running-noisy".to_string();
    running.source_path = "s3://bucket/running-noisy".to_string();
    running.target_path = "/running-noisy".to_string();
    running.tenant = "noisy".to_string();
    running.state = TransferState::Running;
    running.owner = "owner-running".to_string();
    running.lease_expire_at = 1_000;
    running.updated_at = 10;
    store.create_or_get_by_request_id(running).unwrap();

    let mut noisy_pending = job("pending-noisy", "req-pending-noisy");
    noisy_pending.job_key = "Load:s3://bucket/pending-noisy:/pending-noisy".to_string();
    noisy_pending.source_path = "s3://bucket/pending-noisy".to_string();
    noisy_pending.target_path = "/pending-noisy".to_string();
    noisy_pending.tenant = "noisy".to_string();
    noisy_pending.updated_at = 1;
    store.create_or_get_by_request_id(noisy_pending).unwrap();

    let mut quiet_pending = job("pending-quiet", "req-pending-quiet");
    quiet_pending.job_key = "Load:s3://bucket/pending-quiet:/pending-quiet".to_string();
    quiet_pending.source_path = "s3://bucket/pending-quiet".to_string();
    quiet_pending.target_path = "/pending-quiet".to_string();
    quiet_pending.tenant = "quiet".to_string();
    quiet_pending.updated_at = 100;
    store.create_or_get_by_request_id(quiet_pending).unwrap();

    let lease = store
        .acquire_runnable_transfer("owner-a", 1000, 200, 2)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, "pending-quiet");
}

#[test]
fn acquire_pending_with_builtin_fairness_prefers_tenant_without_executing_jobs() {
    let store = MemoryTransferStore::new();

    let mut running = job("running-noisy", "req-running-noisy");
    running.job_key = "Load:s3://bucket/running-noisy:/running-noisy".to_string();
    running.source_path = "s3://bucket/running-noisy".to_string();
    running.target_path = "/running-noisy".to_string();
    running.tenant = "noisy".to_string();
    running.state = TransferState::Running;
    running.owner = "owner-running".to_string();
    running.lease_expire_at = 1_000;
    running.updated_at = 10;
    store.create_or_get_by_request_id(running).unwrap();

    let mut noisy_pending = job("pending-noisy", "req-pending-noisy");
    noisy_pending.job_key = "Load:s3://bucket/pending-noisy:/pending-noisy".to_string();
    noisy_pending.source_path = "s3://bucket/pending-noisy".to_string();
    noisy_pending.target_path = "/pending-noisy".to_string();
    noisy_pending.tenant = "noisy".to_string();
    noisy_pending.updated_at = 1;
    store.create_or_get_by_request_id(noisy_pending).unwrap();

    let mut quiet_pending = job("pending-quiet", "req-pending-quiet");
    quiet_pending.job_key = "Load:s3://bucket/pending-quiet:/pending-quiet".to_string();
    quiet_pending.source_path = "s3://bucket/pending-quiet".to_string();
    quiet_pending.target_path = "/pending-quiet".to_string();
    quiet_pending.tenant = "quiet".to_string();
    quiet_pending.updated_at = 100;
    store.create_or_get_by_request_id(quiet_pending).unwrap();

    let lease = store
        .acquire_runnable_transfer("owner-a", 1000, 200, 2)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, "pending-quiet");
}

#[test]
fn list_tenant_summaries_counts_states_and_orders_active_tenants() {
    let store = MemoryTransferStore::new();
    for (job_id, request_id, tenant, state) in [
        (
            "job-a-pending",
            "req-a-pending",
            "tenant-a",
            TransferState::Pending,
        ),
        (
            "job-a-running",
            "req-a-running",
            "tenant-a",
            TransferState::Running,
        ),
        (
            "job-a-completed",
            "req-a-completed",
            "tenant-a",
            TransferState::Completed,
        ),
        (
            "job-b-failed",
            "req-b-failed",
            "tenant-b",
            TransferState::Failed,
        ),
        (
            "job-b-canceled",
            "req-b-canceled",
            "tenant-b",
            TransferState::Canceled,
        ),
        (
            "job-c-pending",
            "req-c-pending",
            "tenant-c",
            TransferState::Pending,
        ),
    ] {
        let mut job = job(job_id, request_id);
        job.tenant = tenant.to_string();
        job.state = state;
        job.job_key = format!("Load:s3://bucket/{job_id}:/{job_id}");
        job.source_path = format!("s3://bucket/{job_id}");
        job.target_path = format!("/{job_id}");
        store.create_or_get_by_request_id(job).unwrap();
    }

    let summaries = store.list_tenant_summaries(10, 0).unwrap();
    assert_eq!(summaries.len(), 3);
    assert_eq!(summaries[0].tenant, "tenant-a");
    assert_eq!(summaries[0].pending, 1);
    assert_eq!(summaries[0].executing, 1);
    assert_eq!(summaries[0].completed, 1);
    assert_eq!(summaries[0].total, 3);
    assert_eq!(summaries[1].tenant, "tenant-c");
    assert_eq!(summaries[1].pending, 1);
    assert_eq!(summaries[2].tenant, "tenant-b");
    assert_eq!(summaries[2].failed, 1);
    assert_eq!(summaries[2].canceled, 1);

    let page = store.list_tenant_summaries(1, 1).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].tenant, "tenant-c");
}

#[test]
fn submit_rejects_unsupported_protocol_version() {
    let store = Arc::new(MemoryTransferStore::new());
    let service = TransferService::new(store);
    let mut request = submit_request("req-1", "s3://bucket/a", "/a");
    request.protocol_version = Some(2);

    let error = service.submit_transfer(request).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Unsupported transfer protocol version"),
        "unexpected protocol error: {error}"
    );
}

#[test]
fn submit_rejects_invalid_transfer_direction() {
    let store = Arc::new(MemoryTransferStore::new());
    let service = TransferService::new(store);

    let invalid_load = service
        .submit_transfer(submit_request_with_kind(
            "req-load",
            TransferKindProto::TransferLoad,
            "/cv/file",
            "s3://bucket/file",
        ))
        .unwrap_err();
    assert!(
        invalid_load.to_string().contains("Invalid Load direction"),
        "unexpected load direction error: {invalid_load}"
    );

    let invalid_export = service
        .submit_transfer(submit_request_with_kind(
            "req-export",
            TransferKindProto::TransferExport,
            "s3://bucket/file",
            "/cv/file",
        ))
        .unwrap_err();
    assert!(
        invalid_export
            .to_string()
            .contains("Invalid Export direction"),
        "unexpected export direction error: {invalid_export}"
    );
}

#[test]
fn submit_rejects_relative_path_segments() {
    let store = Arc::new(MemoryTransferStore::new());
    let service = TransferService::new(store);

    let error = service
        .submit_transfer(submit_request("req-1", "s3://bucket/a/../b", "/b"))
        .unwrap_err();
    assert!(
        error.to_string().contains("relative segment"),
        "unexpected relative path error: {error}"
    );
}

#[test]
fn list_transfers_filters_and_paginates_by_updated_time() {
    let store = MemoryTransferStore::new();
    let mut first = job("job-1", "req-1");
    first.job_key = "Load:s3://bucket/a:/a".to_string();
    first.source_path = "s3://bucket/a".to_string();
    first.target_path = "/a".to_string();
    first.updated_at = 10;
    store.create_or_get_by_request_id(first).unwrap();

    let mut second = job("job-2", "req-2");
    second.job_key = "Load:s3://bucket/b:/b".to_string();
    second.source_path = "s3://bucket/b".to_string();
    second.target_path = "/b".to_string();
    second.submitter = "flink".to_string();
    second.tenant = "tenant-a".to_string();
    second.updated_at = 30;
    second.state = TransferState::Completed;
    store.create_or_get_by_request_id(second).unwrap();

    let mut third = job("job-3", "req-3");
    third.updated_at = 20;
    third.kind = TransferKind::Export;
    third.job_key = "Export:/a:s3://bucket/a".to_string();
    third.source_path = "/a".to_string();
    third.target_path = "s3://bucket/a".to_string();
    store.create_or_get_by_request_id(third).unwrap();

    let page = store
        .list_transfers(TransferListFilter {
            limit: 2,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|job| job.job_id.as_str())
            .collect::<Vec<_>>(),
        vec!["job-2", "job-3"]
    );

    let exports = store
        .list_transfers(TransferListFilter {
            kind: Some(TransferKind::Export),
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(exports.len(), 1);
    assert_eq!(exports[0].job_id, "job-3");

    let completed = store
        .list_transfers(TransferListFilter {
            state: Some(TransferState::Completed),
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].job_id, "job-2");

    let tenant_jobs = store
        .list_transfers(TransferListFilter {
            tenant: Some("tenant-a".to_string()),
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(tenant_jobs.len(), 1);
    assert_eq!(tenant_jobs[0].job_id, "job-2");

    let submitter_jobs = store
        .list_transfers(TransferListFilter {
            submitter: Some("flink".to_string()),
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(submitter_jobs.len(), 1);
    assert_eq!(submitter_jobs[0].job_id, "job-2");
}

#[test]
fn purge_terminal_transfers_keeps_active_jobs() {
    let store = MemoryTransferStore::new();
    let mut active = job("job-active", "req-active");
    active.updated_at = 1;
    store.create_or_get_by_request_id(active).unwrap();

    let mut completed = job("job-completed", "req-completed");
    completed.job_key = "Load:s3://bucket/completed:/completed".to_string();
    completed.source_path = "s3://bucket/completed".to_string();
    completed.target_path = "/completed".to_string();
    completed.state = TransferState::Completed;
    completed.updated_at = 1;
    store.create_or_get_by_request_id(completed).unwrap();
    store.insert_tasks(vec![task("job-completed")]).unwrap();

    assert_eq!(store.purge_terminal_transfers(10, 100).unwrap(), 1);
    assert!(store.get_transfer("job-active").unwrap().is_some());
    assert!(store.get_transfer("job-completed").unwrap().is_none());
    assert!(store
        .list_transfer_tasks("job-completed", 1)
        .unwrap()
        .is_empty());
}

#[test]
fn lease_owner_and_epoch_guard_state_transition() {
    let store = MemoryTransferStore::new();
    store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();

    let lease = store
        .acquire_runnable_transfer("owner-a", 1000, 10, 100)
        .unwrap()
        .unwrap();

    let stale_owner_update = TransferStateUpdate {
        job_id: "job-1".to_string(),
        run_id: 1,
        owner: "owner-b".to_string(),
        lease_epoch: lease.lease_epoch,
        from_states: vec![TransferState::Pending],
        to_state: TransferState::Planning,
        message: "planning".to_string(),
        now_ms: 11,
    };
    assert!(!store.update_transfer_state(stale_owner_update).unwrap());

    let valid_update = TransferStateUpdate {
        job_id: "job-1".to_string(),
        run_id: 1,
        owner: lease.owner,
        lease_epoch: lease.lease_epoch,
        from_states: vec![TransferState::Planning],
        to_state: TransferState::Planning,
        message: "planning".to_string(),
        now_ms: 12,
    };
    assert!(store.update_transfer_state(valid_update).unwrap());
    assert_eq!(
        store.get_transfer("job-1").unwrap().unwrap().state,
        TransferState::Planning
    );
}

#[test]
fn owner_can_reacquire_running_transfer_before_lease_expiry() {
    let store = MemoryTransferStore::new();
    store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();

    let lease = store
        .acquire_runnable_transfer("owner-a", 1000, 10, 100)
        .unwrap()
        .unwrap();
    assert!(store
        .update_transfer_state(TransferStateUpdate {
            job_id: "job-1".to_string(),
            run_id: 1,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            from_states: vec![TransferState::Planning],
            to_state: TransferState::Running,
            message: "running".to_string(),
            now_ms: 11,
        })
        .unwrap());

    assert!(store
        .acquire_runnable_transfer("owner-b", 1000, 20, 100)
        .unwrap()
        .is_none());

    let reacquired = store
        .acquire_runnable_transfer("owner-a", 1000, 20, 100)
        .unwrap()
        .unwrap();
    assert_eq!(reacquired.job_id, "job-1");
    assert_eq!(reacquired.owner, "owner-a");
    assert!(reacquired.lease_epoch > lease.lease_epoch);
}

#[test]
fn task_report_requires_current_attempt_and_worker_session() {
    let store = MemoryTransferStore::new();
    store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();
    store.insert_tasks(vec![task("job-1")]).unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();

    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: "job-1".to_string(),
            run_id: 1,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 7,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 10,
            stale_deadline_at: 70,
        })
        .unwrap());

    let mut report = TransferTaskReport {
        job_id: "job-1".to_string(),
        run_id: 1,
        task_id: "task-1".to_string(),
        attempt_id: 7,
        worker_id: 10,
        worker_session_id: "session-old".to_string(),
        state: TransferTaskState::Running,
        progress: TransferProgress {
            loaded_size: 1,
            total_size: 10,
            update_time: 20,
            message: String::new(),
        },
        now_ms: 20,
        stale_deadline_at: 80,
    };
    assert!(!store.update_task_report(report.clone()).unwrap());

    report.worker_session_id = "session-a".to_string();
    report.state = TransferTaskState::Failed;
    report.progress.message = "copy failed\npermission denied".to_string();
    assert!(store.update_task_report(report).unwrap());
    let job = store.get_transfer("job-1").unwrap().unwrap();
    assert_eq!(job.state, TransferState::Planning);
    assert_eq!(job.summary.loaded_size, 1);
    assert_eq!(job.summary.total_size, 10);
    assert_eq!(job.summary.message, "copy failed\npermission denied");
}

#[test]
fn start_task_attempt_rejects_cancel_requested_job() {
    let store = MemoryTransferStore::new();
    store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();
    store.insert_tasks(vec![task("job-1")]).unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert!(store.request_cancel("job-1", 1, 11).unwrap());
    assert_eq!(
        store.get_transfer("job-1").unwrap().unwrap().state,
        TransferState::Canceling
    );

    assert!(!store
        .start_task_attempt(TaskAttemptStart {
            job_id: "job-1".to_string(),
            run_id: 1,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 12,
            stale_deadline_at: 72,
        })
        .unwrap());

    let task = store.list_transfer_tasks("job-1", 1).unwrap().remove(0);
    assert_eq!(task.state, TransferTaskState::Pending);
}

#[test]
fn queued_task_report_returns_store_acceptance() {
    let store = Arc::new(MemoryTransferStore::new());
    store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();
    store.insert_tasks(vec![task("job-1")]).unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: "job-1".to_string(),
            run_id: 1,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 7,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 10,
            stale_deadline_at: 70,
        })
        .unwrap());
    let service =
        TransferService::with_report_queue(store, Duration::from_secs(60), 16, 1).unwrap();

    let mut request = transfer_task_report_request("session-old");
    assert!(!service.report_task(request.clone()).unwrap());

    request.worker_session_id = "session-a".to_string();
    assert!(service.report_task(request).unwrap());
}

#[test]
fn queued_progress_report_returns_after_enqueue_without_state_pollution() {
    let store = Arc::new(MemoryTransferStore::new());
    store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();
    store.insert_tasks(vec![task("job-1")]).unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: "job-1".to_string(),
            run_id: 1,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 7,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 10,
            stale_deadline_at: 70,
        })
        .unwrap());
    let service =
        TransferService::with_report_queue(store.clone(), Duration::from_secs(60), 16, 1).unwrap();

    let mut request = transfer_task_report_request("session-old");
    request.state = TransferTaskStateProto::TransferTaskRunning as i32;
    request.progress.message = "stale progress".to_string();
    assert!(
        service.report_task(request).unwrap(),
        "non-terminal progress report should acknowledge enqueue"
    );

    std::thread::sleep(Duration::from_millis(100));
    let task = store.list_transfer_tasks("job-1", 1).unwrap().remove(0);
    assert_eq!(task.state, TransferTaskState::Running);
    assert_ne!(task.progress.message, "stale progress");
}

#[test]
fn queued_progress_report_storm_does_not_block_terminal_report() {
    let store = Arc::new(MemoryTransferStore::new());
    store
        .create_or_get_by_request_id(job("job-1", "req-1"))
        .unwrap();
    store.insert_tasks(vec![task("job-1")]).unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: "job-1".to_string(),
            run_id: 1,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 7,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 10,
            stale_deadline_at: 70,
        })
        .unwrap());
    let service = Arc::new(
        TransferService::with_report_queue(store.clone(), Duration::from_secs(60), 4096, 4)
            .unwrap(),
    );
    let workers = 16;
    let reports_per_worker = 100;
    let barrier = Arc::new(std::sync::Barrier::new(workers));

    let handles = (0..workers)
        .map(|worker| {
            let service = service.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for index in 0..reports_per_worker {
                    let mut request = transfer_task_report_request("session-a");
                    request.state = TransferTaskStateProto::TransferTaskRunning as i32;
                    request.progress.loaded_size = (worker * reports_per_worker + index) as i64;
                    request.progress.message = format!("progress-{worker}-{index}");
                    assert!(service.report_task(request).unwrap());
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }

    let mut terminal = transfer_task_report_request("session-a");
    terminal.state = TransferTaskStateProto::TransferTaskCompleted as i32;
    terminal.progress.message = "done-after-storm".to_string();
    assert!(
        service.report_task(terminal).unwrap(),
        "terminal report should still wait for and observe store acceptance"
    );

    let task = store.list_transfer_tasks("job-1", 1).unwrap().remove(0);
    assert_eq!(task.state, TransferTaskState::Completed);
    assert_eq!(task.progress.message, "done-after-storm");
}

fn transfer_task_report_request(worker_session_id: &str) -> TransferTaskReportRequest {
    TransferTaskReportRequest {
        job_id: "job-1".to_string(),
        run_id: 1,
        task_id: "task-1".to_string(),
        attempt_id: 7,
        worker_id: 10,
        worker_session_id: worker_session_id.to_string(),
        state: TransferTaskStateProto::TransferTaskCompleted as i32,
        progress: TransferProgressProto {
            loaded_size: 10,
            total_size: 10,
            update_time: 20,
            message: "done".to_string(),
        },
        protocol_version: Some(1),
    }
}
