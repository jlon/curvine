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

use curvine_common::error::ErrorKind;
use curvine_common::proto::{SubmitTransferRequest, TransferKindProto};
use curvine_common::state::{
    TaskAttemptStart, TransferJobRecord, TransferKind, TransferProgress, TransferState,
    TransferStateUpdate, TransferTaskRecord, TransferTaskReport, TransferTaskState,
};
use curvine_server::transfer::{
    MysqlTransferStore, TransferService, TransferStore, TransferStoreBackend,
};
use mysql::params;
use mysql::prelude::*;
use orpc::common::Metrics;
use std::sync::{Arc, Barrier};
use std::thread;
use uuid::Uuid;

fn mysql_store(name: &str) -> Option<(MysqlTransferStore, String, String, String)> {
    let base_url = std::env::var("CURVINE_TRANSFER_MYSQL_URL").ok()?;
    let safe_name = name.replace('-', "_");
    let safe_name = &safe_name[..safe_name.len().min(20)];
    let suffix = Uuid::new_v4().simple().to_string();
    let db_name = format!(
        "cv_transfer_{}_{}_{}",
        safe_name,
        std::process::id(),
        &suffix[..8]
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
    Some((
        MysqlTransferStore::open(&store_url).unwrap(),
        store_url,
        base_url,
        db_name,
    ))
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

fn metric_value(output: &str, metric: &str, labels: &[&str]) -> Option<f64> {
    output.lines().find_map(|line| {
        if !line.starts_with(metric) || labels.iter().any(|label| !line.contains(label)) {
            return None;
        }
        line.split_whitespace().last()?.parse::<f64>().ok()
    })
}

fn job(job_id: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("Load:s3://bucket/{job_id}:/{job_id}"),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: format!("s3://bucket/{job_id}"),
        target_path: format!("/{job_id}"),
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
        client_request_id: format!("req-{job_id}"),
        submitter: "mysql-test".to_string(),
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
        source_path: format!("s3://bucket/{job_id}"),
        target_path: format!("/{job_id}"),
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

#[test]
fn mysql_finds_conflicting_active_transfer_by_target_path() {
    let Some((store, _store_url, base_url, db_name)) = mysql_store("target-conflict") else {
        return;
    };
    let mut parent = job("parent");
    parent.target_path = "/a".to_string();
    store.create_or_get_by_request_id(parent).unwrap();

    let conflict = store
        .find_conflicting_active_transfer("/a/child", "mysql-test", "req-other")
        .unwrap()
        .expect("child target should conflict with active parent target");
    assert_eq!(conflict.job_id, "parent");

    let replay = store
        .find_conflicting_active_transfer("/a", "mysql-test", "req-parent")
        .unwrap();
    assert!(
        replay.is_none(),
        "same submitter request should be treated as idempotent replay"
    );
    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_root_target_conflicts_with_any_active_transfer() {
    let Some((store, _store_url, base_url, db_name)) = mysql_store("root-target-conflict") else {
        return;
    };
    let mut root = job("root");
    root.target_path = "/".to_string();
    store.create_or_get_by_request_id(root).unwrap();

    let root_conflict = store
        .find_conflicting_active_transfer("/any/path", "mysql-test", "req-third")
        .unwrap()
        .expect("root target should conflict with any target");
    assert_eq!(root_conflict.job_id, "root");
    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_submit_rejects_root_when_child_target_is_active() {
    let Some((store, _store_url, base_url, db_name)) = mysql_store("child-before-root-conflict")
    else {
        return;
    };
    let mut parent = job("parent");
    parent.target_path = "/a".to_string();
    store.create_or_get_by_request_id(parent).unwrap();

    let mut root = job("root");
    root.target_path = "/".to_string();
    let error = store.create_or_get_by_request_id(root).unwrap_err();
    assert!(
        matches!(error.kind(), ErrorKind::TransferTargetConflict),
        "unexpected conflict error: {error}"
    );
    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_store_supports_backlog_lease_report_and_cleanup() {
    let Some((store, store_url, base_url, db_name)) = mysql_store("store_semantics") else {
        eprintln!("skip mysql store test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let second_store = MysqlTransferStore::open(&store_url).unwrap();

    store.create_or_get_by_request_id(job("pending")).unwrap();
    let mut running = job("running");
    running.state = TransferState::Running;
    store.create_or_get_by_request_id(running).unwrap();
    assert_eq!(store.count_active_transfers().unwrap(), 2);
    assert_eq!(store.count_executing_transfers().unwrap(), 1);

    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 1)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, "running");
    assert_eq!(
        store.get_transfer("pending").unwrap().unwrap().state,
        TransferState::Pending
    );
    assert!(store
        .acquire_runnable_transfer("owner-a", 100, 10, 1)
        .unwrap()
        .is_none());

    let lease_a = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert_eq!(lease_a.job_id, "pending");
    assert!(second_store
        .acquire_runnable_transfer("owner-b", 100, 50, 100)
        .unwrap()
        .is_none());
    let lease_b = second_store
        .acquire_runnable_transfer("owner-b", 100, 111, 100)
        .unwrap()
        .unwrap();
    assert_eq!(lease_b.job_id, "pending");
    assert_eq!(lease_b.lease_epoch, lease_a.lease_epoch + 1);

    assert!(!store
        .update_transfer_state(TransferStateUpdate {
            job_id: lease_a.job_id.clone(),
            run_id: lease_a.run_id,
            owner: lease_a.owner,
            lease_epoch: lease_a.lease_epoch,
            from_states: vec![TransferState::Pending],
            to_state: TransferState::Planning,
            message: "stale owner should not update".to_string(),
            now_ms: 120,
        })
        .unwrap());
    assert!(second_store
        .update_transfer_state(TransferStateUpdate {
            job_id: lease_b.job_id.clone(),
            run_id: lease_b.run_id,
            owner: lease_b.owner.clone(),
            lease_epoch: lease_b.lease_epoch,
            from_states: vec![TransferState::Planning],
            to_state: TransferState::Planning,
            message: "current owner may update".to_string(),
            now_ms: 121,
        })
        .unwrap());

    store.insert_tasks(vec![task("pending")]).unwrap();
    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: "pending".to_string(),
            run_id: 1,
            owner: lease_b.owner.clone(),
            lease_epoch: lease_b.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 130,
            stale_deadline_at: 190,
        })
        .unwrap());
    assert!(!store
        .update_task_report(TransferTaskReport {
            job_id: "pending".to_string(),
            run_id: 1,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 10,
            worker_session_id: "old-session".to_string(),
            state: TransferTaskState::Running,
            progress: TransferProgress::default(),
            now_ms: 140,
            stale_deadline_at: 200,
        })
        .unwrap());
    assert!(store
        .update_task_report(TransferTaskReport {
            job_id: "pending".to_string(),
            run_id: 1,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            state: TransferTaskState::Completed,
            progress: TransferProgress {
                loaded_size: 10,
                total_size: 10,
                update_time: 150,
                message: String::new(),
            },
            now_ms: 150,
            stale_deadline_at: 210,
        })
        .unwrap());
    assert_eq!(
        store.get_transfer("pending").unwrap().unwrap().state,
        TransferState::Completed
    );

    assert_eq!(store.purge_terminal_transfers(200, 100).unwrap(), 1);
    assert!(store.get_transfer("pending").unwrap().is_none());
    assert!(store.list_transfer_tasks("pending", 1).unwrap().is_empty());

    drop(second_store);
    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_acquire_pending_prefers_tenant_with_fewer_executing_jobs() {
    let Some((store, _store_url, base_url, db_name)) = mysql_store("tenant_fairness") else {
        eprintln!("skip mysql tenant fairness test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };

    let mut running = job("running-noisy");
    running.target_path = "/running-noisy".to_string();
    running.job_key = "Load:s3://bucket/running-noisy:/running-noisy".to_string();
    running.tenant = "noisy".to_string();
    running.state = TransferState::Running;
    running.owner = "owner-running".to_string();
    running.lease_expire_at = 1_000;
    running.updated_at = 10;
    store.create_or_get_by_request_id(running).unwrap();

    let mut noisy_pending = job("pending-noisy");
    noisy_pending.target_path = "/pending-noisy".to_string();
    noisy_pending.job_key = "Load:s3://bucket/pending-noisy:/pending-noisy".to_string();
    noisy_pending.tenant = "noisy".to_string();
    noisy_pending.updated_at = 1;
    store.create_or_get_by_request_id(noisy_pending).unwrap();

    let mut quiet_pending = job("pending-quiet");
    quiet_pending.target_path = "/pending-quiet".to_string();
    quiet_pending.job_key = "Load:s3://bucket/pending-quiet:/pending-quiet".to_string();
    quiet_pending.tenant = "quiet".to_string();
    quiet_pending.updated_at = 100;
    store.create_or_get_by_request_id(quiet_pending).unwrap();

    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 200, 2)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, "pending-quiet");

    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_acquire_pending_with_builtin_fairness_prefers_tenant_without_executing_jobs() {
    let Some((store, _store_url, base_url, db_name)) = mysql_store("tenant_fifo") else {
        eprintln!("skip mysql tenant fifo test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };

    let mut running = job("running-noisy");
    running.target_path = "/running-noisy".to_string();
    running.job_key = "Load:s3://bucket/running-noisy:/running-noisy".to_string();
    running.tenant = "noisy".to_string();
    running.state = TransferState::Running;
    running.owner = "owner-running".to_string();
    running.lease_expire_at = 1_000;
    running.updated_at = 10;
    store.create_or_get_by_request_id(running).unwrap();

    let mut noisy_pending = job("pending-noisy");
    noisy_pending.target_path = "/pending-noisy".to_string();
    noisy_pending.job_key = "Load:s3://bucket/pending-noisy:/pending-noisy".to_string();
    noisy_pending.tenant = "noisy".to_string();
    noisy_pending.updated_at = 1;
    store.create_or_get_by_request_id(noisy_pending).unwrap();

    let mut quiet_pending = job("pending-quiet");
    quiet_pending.target_path = "/pending-quiet".to_string();
    quiet_pending.job_key = "Load:s3://bucket/pending-quiet:/pending-quiet".to_string();
    quiet_pending.tenant = "quiet".to_string();
    quiet_pending.updated_at = 100;
    store.create_or_get_by_request_id(quiet_pending).unwrap();

    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 200, 2)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, "pending-quiet");

    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_list_tenant_summaries_counts_states_and_orders_active_tenants() {
    let Some((store, _store_url, base_url, db_name)) = mysql_store("tenant_summary") else {
        eprintln!("skip mysql tenant summary test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };

    for (job_id, tenant, state) in [
        ("a-pending", "tenant-a", TransferState::Pending),
        ("a-running", "tenant-a", TransferState::Running),
        ("a-completed", "tenant-a", TransferState::Completed),
        ("b-failed", "tenant-b", TransferState::Failed),
        ("b-canceled", "tenant-b", TransferState::Canceled),
        ("c-pending", "tenant-c", TransferState::Pending),
    ] {
        let mut job = job(job_id);
        job.tenant = tenant.to_string();
        job.state = state;
        store.create_or_get_by_request_id(job).unwrap();
    }

    let summaries = store.list_tenant_summaries(10, 0).unwrap();
    assert_eq!(summaries.len(), 3);
    assert_eq!(summaries[0].tenant, "tenant-a");
    assert_eq!(summaries[0].pending, 1);
    assert_eq!(summaries[0].executing, 1);
    assert_eq!(summaries[0].completed, 1);
    assert_eq!(summaries[1].tenant, "tenant-c");
    assert_eq!(summaries[2].tenant, "tenant-b");
    assert_eq!(summaries[2].failed, 1);
    assert_eq!(summaries[2].canceled, 1);

    let page = store.list_tenant_summaries(1, 1).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].tenant, "tenant-c");

    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_start_task_attempt_rejects_cancel_requested_job() {
    let Some((store, _store_url, base_url, db_name)) = mysql_store("cancel_start") else {
        eprintln!("skip mysql cancel start test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    store.create_or_get_by_request_id(job("job-1")).unwrap();
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

    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_list_transfers_filters_by_submitter_and_tenant() {
    let Some((store, _store_url, base_url, db_name)) = mysql_store("list_filter_tenant") else {
        eprintln!("skip mysql list filter test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let mut flink = job("flink");
    flink.submitter = "flink".to_string();
    flink.tenant = "tenant-a".to_string();
    flink.updated_at = 30;
    store.create_or_get_by_request_id(flink).unwrap();

    let mut starrocks = job("starrocks");
    starrocks.submitter = "starrocks".to_string();
    starrocks.tenant = "tenant-b".to_string();
    starrocks.updated_at = 20;
    store.create_or_get_by_request_id(starrocks).unwrap();

    let tenant_jobs = store
        .list_transfers(curvine_common::state::TransferListFilter {
            tenant: Some("tenant-a".to_string()),
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(tenant_jobs.len(), 1);
    assert_eq!(tenant_jobs[0].job_id, "flink");

    let submitter_jobs = store
        .list_transfers(curvine_common::state::TransferListFilter {
            submitter: Some("starrocks".to_string()),
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(submitter_jobs.len(), 1);
    assert_eq!(submitter_jobs[0].job_id, "starrocks");

    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_migrates_v2_schema_through_v3_and_v4() {
    let base_url = match std::env::var("CURVINE_TRANSFER_MYSQL_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skip mysql v2 migration test: CURVINE_TRANSFER_MYSQL_URL is not set");
            return;
        }
    };
    let db_name = format!(
        "cv_transfer_migrate_v2_{}_{}",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    );
    create_mysql_database(&base_url, &db_name);
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let store_url = format!(
        "{}/{}{}pool_min=0&pool_max=1",
        base_url.trim_end_matches('/'),
        db_name,
        separator
    );
    let pool = mysql::Pool::new(store_url.as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(
        "create table transfer_schema_version (
            id tinyint unsigned primary key,
            version bigint unsigned not null,
            updated_at bigint not null
        )",
    )
    .unwrap();
    conn.query_drop(
        "insert into transfer_schema_version(id, version, updated_at) values (1, 2, 1)",
    )
    .unwrap();
    conn.query_drop(
        "create table transfer_jobs (
            job_id varchar(128) primary key,
            submitter varchar(255) not null,
            client_request_id varchar(255) not null,
            job_key varchar(1024) not null,
            run_id bigint unsigned not null,
            kind int not null,
            state int not null,
            owner varchar(255) not null,
            lease_epoch bigint unsigned not null,
            lease_expire_at bigint not null,
            cancel_requested tinyint not null,
            record_json longtext not null,
            created_at bigint not null,
            updated_at bigint not null,
            unique key transfer_jobs_request_idx(submitter, client_request_id)
        )",
    )
    .unwrap();
    let mut legacy = job("legacy-v2");
    legacy.submitter = "legacy-submitter".to_string();
    legacy.tenant = "legacy-tenant".to_string();
    legacy.target_path = "/legacy-target".to_string();
    legacy.job_key = format!("Load:{}:{}", legacy.source_path, legacy.target_path);
    let legacy_json = serde_json::to_string(&legacy).unwrap();
    conn.exec_drop(
        "insert into transfer_jobs (
            job_id, submitter, client_request_id, job_key, run_id, kind, state,
            owner, lease_epoch, lease_expire_at, cancel_requested, record_json, created_at, updated_at
        ) values (
            :job_id, :submitter, :client_request_id, :job_key, :run_id, :kind, :state,
            :owner, :lease_epoch, :lease_expire_at, :cancel_requested, :record_json, :created_at, :updated_at
        )",
        params! {
            "job_id" => &legacy.job_id,
            "submitter" => &legacy.submitter,
            "client_request_id" => &legacy.client_request_id,
            "job_key" => &legacy.job_key,
            "run_id" => legacy.run_id,
            "kind" => legacy.kind as i32,
            "state" => legacy.state as i32,
            "owner" => &legacy.owner,
            "lease_epoch" => legacy.lease_epoch,
            "lease_expire_at" => legacy.lease_expire_at,
            "cancel_requested" => legacy.cancel_requested,
            "record_json" => legacy_json,
            "created_at" => legacy.created_at,
            "updated_at" => legacy.updated_at,
        },
    )
    .unwrap();
    drop(conn);

    let store = MysqlTransferStore::open(&store_url).unwrap();
    let migrated = store.get_transfer("legacy-v2").unwrap().unwrap();
    assert_eq!(migrated.target_path, "/legacy-target");
    let tenant_jobs = store
        .list_transfers(curvine_common::state::TransferListFilter {
            tenant: Some("legacy-tenant".to_string()),
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(tenant_jobs.len(), 1);
    assert_eq!(tenant_jobs[0].job_id, "legacy-v2");
    assert!(store
        .find_conflicting_active_transfer("/legacy-target/child", "other", "other-req")
        .unwrap()
        .is_some());

    let mut conn = pool.get_conn().unwrap();
    let version: u64 = conn
        .exec_first(
            "select version from transfer_schema_version where id = 1",
            mysql::Params::Empty,
        )
        .unwrap()
        .unwrap();
    assert_eq!(version, 4);
    assert!(mysql_column_exists(
        &mut conn,
        "transfer_jobs",
        "target_path"
    ));
    assert!(mysql_column_exists(&mut conn, "transfer_jobs", "tenant"));

    drop(store);
    drop(conn);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_migrates_v3_schema_and_backfills_tenant() {
    let base_url = match std::env::var("CURVINE_TRANSFER_MYSQL_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skip mysql v3 migration test: CURVINE_TRANSFER_MYSQL_URL is not set");
            return;
        }
    };
    let db_name = format!(
        "cv_transfer_migrate_v3_{}_{}",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    );
    create_mysql_database(&base_url, &db_name);
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let store_url = format!(
        "{}/{}{}pool_min=0&pool_max=1",
        base_url.trim_end_matches('/'),
        db_name,
        separator
    );
    let pool = mysql::Pool::new(store_url.as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(
        "create table transfer_schema_version (
            id tinyint unsigned primary key,
            version bigint unsigned not null,
            updated_at bigint not null
        )",
    )
    .unwrap();
    conn.query_drop(
        "insert into transfer_schema_version(id, version, updated_at) values (1, 3, 1)",
    )
    .unwrap();
    conn.query_drop(
        "create table transfer_jobs (
            job_id varchar(128) primary key,
            submitter varchar(255) not null,
            client_request_id varchar(255) not null,
            job_key varchar(1024) not null,
            target_path varchar(2048) not null,
            run_id bigint unsigned not null,
            kind int not null,
            state int not null,
            owner varchar(255) not null,
            lease_epoch bigint unsigned not null,
            lease_expire_at bigint not null,
            cancel_requested tinyint not null,
            record_json longtext not null,
            created_at bigint not null,
            updated_at bigint not null,
            unique key transfer_jobs_request_idx(submitter, client_request_id)
        )",
    )
    .unwrap();
    let mut legacy = job("legacy");
    legacy.submitter = "legacy-submitter".to_string();
    legacy.tenant = "legacy-tenant".to_string();
    let legacy_json = serde_json::to_string(&legacy).unwrap();
    conn.exec_drop(
        "insert into transfer_jobs (
            job_id, submitter, client_request_id, job_key, target_path, run_id, kind, state,
            owner, lease_epoch, lease_expire_at, cancel_requested, record_json, created_at, updated_at
        ) values (
            :job_id, :submitter, :client_request_id, :job_key, :target_path, :run_id, :kind, :state,
            :owner, :lease_epoch, :lease_expire_at, :cancel_requested, :record_json, :created_at, :updated_at
        )",
        params! {
            "job_id" => &legacy.job_id,
            "submitter" => &legacy.submitter,
            "client_request_id" => &legacy.client_request_id,
            "job_key" => &legacy.job_key,
            "target_path" => &legacy.target_path,
            "run_id" => legacy.run_id,
            "kind" => legacy.kind as i32,
            "state" => legacy.state as i32,
            "owner" => &legacy.owner,
            "lease_epoch" => legacy.lease_epoch,
            "lease_expire_at" => legacy.lease_expire_at,
            "cancel_requested" => legacy.cancel_requested,
            "record_json" => legacy_json,
            "created_at" => legacy.created_at,
            "updated_at" => legacy.updated_at,
        },
    )
    .unwrap();
    drop(conn);

    let store = MysqlTransferStore::open(&store_url).unwrap();
    let tenant_jobs = store
        .list_transfers(curvine_common::state::TransferListFilter {
            tenant: Some("legacy-tenant".to_string()),
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(tenant_jobs.len(), 1);
    assert_eq!(tenant_jobs[0].job_id, "legacy");

    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_rejects_future_schema_without_creating_current_tables() {
    let base_url = match std::env::var("CURVINE_TRANSFER_MYSQL_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skip mysql future schema test: CURVINE_TRANSFER_MYSQL_URL is not set");
            return;
        }
    };
    let db_name = format!(
        "cv_transfer_future_schema_{}_{}",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    );
    create_mysql_database(&base_url, &db_name);
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let store_url = format!(
        "{}/{}{}pool_min=0&pool_max=1",
        base_url.trim_end_matches('/'),
        db_name,
        separator
    );
    let pool = mysql::Pool::new(store_url.as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(
        "create table transfer_schema_version (
            id tinyint unsigned primary key,
            version bigint unsigned not null,
            updated_at bigint not null
        )",
    )
    .unwrap();
    conn.query_drop(
        "insert into transfer_schema_version(id, version, updated_at) values (1, 999, 1)",
    )
    .unwrap();
    drop(conn);

    let err = match MysqlTransferStore::open(&store_url) {
        Ok(_) => panic!("future mysql schema unexpectedly opened"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("Unsupported mysql transfer schema version 999"),
        "unexpected error: {err}"
    );

    let mut conn = pool.get_conn().unwrap();
    assert!(!mysql_table_exists(&mut conn, "transfer_jobs"));
    assert!(!mysql_table_exists(&mut conn, "transfer_tasks"));
    drop(conn);
    drop_mysql_database(&base_url, &db_name);
}

fn mysql_table_exists(conn: &mut mysql::PooledConn, table: &str) -> bool {
    conn.exec_first::<String, _, _>(
        "select table_name from information_schema.tables
         where table_schema = database() and table_name = :table",
        params! {
            "table" => table,
        },
    )
    .unwrap()
    .is_some()
}

fn mysql_column_exists(conn: &mut mysql::PooledConn, table: &str, column: &str) -> bool {
    conn.exec_first::<String, _, _>(
        "select column_name from information_schema.columns
         where table_schema = database()
           and table_name = :table
           and column_name = :column",
        params! {
            "table" => table,
            "column" => column,
        },
    )
    .unwrap()
    .is_some()
}

#[test]
fn mysql_acquire_enforces_execution_window_across_concurrent_store_handles() {
    let Some((store, store_url, base_url, db_name)) = mysql_store("concurrent_acquire") else {
        eprintln!("skip mysql concurrent acquire test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };

    for index in 0..8 {
        store
            .create_or_get_by_request_id(job(&format!("pending-{index}")))
            .unwrap();
    }
    drop(store);

    let workers = 8;
    let barrier = Arc::new(Barrier::new(workers));
    let mut handles = Vec::new();
    for index in 0..workers {
        let barrier = barrier.clone();
        let store_url = store_url.clone();
        handles.push(thread::spawn(move || {
            let store = MysqlTransferStore::open(&store_url).unwrap();
            barrier.wait();
            store
                .acquire_runnable_transfer(&format!("owner-{index}"), 1000, 10, 1)
                .unwrap()
                .map(|lease| lease.job_id)
        }));
    }

    let acquired: Vec<_> = handles
        .into_iter()
        .filter_map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(
        acquired.len(),
        1,
        "only one pending job may enter the execution window"
    );

    let reopened = MysqlTransferStore::open(&store_url).unwrap();
    assert_eq!(reopened.count_active_transfers().unwrap(), 8);
    assert_eq!(reopened.count_executing_transfers().unwrap(), 1);
    let jobs = reopened
        .list_transfers(curvine_common::state::TransferListFilter {
            kind: None,
            state: None,
            limit: 100,
            offset: 0,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        jobs.iter()
            .filter(|job| job.state == TransferState::Planning)
            .count(),
        1
    );
    assert_eq!(
        jobs.iter()
            .filter(|job| job.state == TransferState::Pending)
            .count(),
        7
    );

    drop(reopened);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_submit_accepts_concurrent_unique_backlog() {
    let Some((store, store_url, base_url, db_name)) = mysql_store("submit_backlog") else {
        eprintln!("skip mysql submit backlog test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let workers = 8;
    let jobs_per_worker = 8;
    let service = Arc::new(TransferService::new(Arc::new(store)));
    let barrier = Arc::new(Barrier::new(workers));

    let handles = (0..workers)
        .map(|worker| {
            let service = service.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for index in 0..jobs_per_worker {
                    let request_id = format!("submit-backlog-{worker}-{index}");
                    service
                        .submit_transfer(SubmitTransferRequest {
                            kind: TransferKindProto::TransferLoad as i32,
                            source_path: format!("s3://bucket/{request_id}"),
                            target_path: format!("/submit-backlog/{worker}/{index}"),
                            client_request_id: request_id,
                            submitter: "mysql-test".to_string(),
                            tenant: "default".to_string(),
                            command: Vec::new(),
                            protocol_version: Some(1),
                        })
                        .unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }

    drop(service);
    let reopened = MysqlTransferStore::open(&store_url).unwrap();
    assert_eq!(
        reopened.count_active_transfers().unwrap(),
        (workers * jobs_per_worker) as u64
    );
    drop(reopened);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_submit_is_atomic_for_concurrent_idempotent_request() {
    let Some((store, store_url, base_url, db_name)) = mysql_store("submit_idempotent") else {
        eprintln!("skip mysql submit idempotent test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let workers = 16;
    let service = Arc::new(TransferService::new(Arc::new(store)));
    let barrier = Arc::new(Barrier::new(workers));

    let handles = (0..workers)
        .map(|_| {
            let service = service.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                service
                    .submit_transfer(SubmitTransferRequest {
                        kind: TransferKindProto::TransferLoad as i32,
                        source_path: "s3://bucket/idempotent".to_string(),
                        target_path: "/submit-idempotent".to_string(),
                        client_request_id: "same-request".to_string(),
                        submitter: "mysql-test".to_string(),
                        tenant: "default".to_string(),
                        command: Vec::new(),
                        protocol_version: Some(1),
                    })
                    .unwrap()
                    .job_id
            })
        })
        .collect::<Vec<_>>();
    let job_ids = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    assert!(job_ids.windows(2).all(|pair| pair[0] == pair[1]));
    drop(service);
    let reopened = MysqlTransferStore::open(&store_url).unwrap();
    assert_eq!(reopened.count_active_transfers().unwrap(), 1);
    drop(reopened);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_submit_is_atomic_for_concurrent_target_conflict() {
    let Some((store, store_url, base_url, db_name)) = mysql_store("submit_target_conflict") else {
        eprintln!("skip mysql submit target conflict test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let workers = 16;
    let service = Arc::new(TransferService::new(Arc::new(store)));
    let barrier = Arc::new(Barrier::new(workers));

    let handles = (0..workers)
        .map(|worker| {
            let service = service.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                service.submit_transfer(SubmitTransferRequest {
                    kind: TransferKindProto::TransferLoad as i32,
                    source_path: format!("s3://bucket/conflict-{worker}"),
                    target_path: "/submit-conflict".to_string(),
                    client_request_id: format!("conflict-request-{worker}"),
                    submitter: "mysql-test".to_string(),
                    tenant: "default".to_string(),
                    command: Vec::new(),
                    protocol_version: Some(1),
                })
            })
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    let accepted = results.iter().filter(|result| result.is_ok()).count();
    let rejected = results
        .iter()
        .filter(|result| match result {
            Ok(_) => false,
            Err(err) => matches!(err.kind(), ErrorKind::TransferTargetConflict),
        })
        .count();
    assert_eq!(accepted, 1);
    assert_eq!(rejected, workers - 1);

    drop(service);
    let reopened = MysqlTransferStore::open(&store_url).unwrap();
    assert_eq!(reopened.count_active_transfers().unwrap(), 1);
    drop(reopened);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_submit_is_atomic_for_concurrent_parent_child_target_conflict() {
    let Some((store, store_url, base_url, db_name)) = mysql_store("submit_parent_child_conflict")
    else {
        eprintln!(
            "skip mysql submit parent child conflict test: CURVINE_TRANSFER_MYSQL_URL is not set"
        );
        return;
    };
    let workers = 16;
    let service = Arc::new(TransferService::new(Arc::new(store)));
    let barrier = Arc::new(Barrier::new(workers));

    let handles = (0..workers)
        .map(|worker| {
            let service = service.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                let target_path = if worker % 2 == 0 {
                    "/submit-parent-child".to_string()
                } else {
                    "/submit-parent-child/child".to_string()
                };
                service.submit_transfer(SubmitTransferRequest {
                    kind: TransferKindProto::TransferLoad as i32,
                    source_path: format!("s3://bucket/parent-child-{worker}"),
                    target_path,
                    client_request_id: format!("parent-child-request-{worker}"),
                    submitter: "mysql-test".to_string(),
                    tenant: "default".to_string(),
                    command: Vec::new(),
                    protocol_version: Some(1),
                })
            })
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    let accepted = results.iter().filter(|result| result.is_ok()).count();
    let rejected = results
        .iter()
        .filter(|result| match result {
            Ok(_) => false,
            Err(err) => matches!(err.kind(), ErrorKind::TransferTargetConflict),
        })
        .count();
    assert_eq!(accepted, 1);
    assert_eq!(rejected, workers - 1);

    drop(service);
    let reopened = MysqlTransferStore::open(&store_url).unwrap();
    assert_eq!(reopened.count_active_transfers().unwrap(), 1);
    drop(reopened);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_target_conflict_detects_deep_ancestor_without_reverse_like() {
    let Some((store, _store_url, base_url, db_name)) = mysql_store("deep_ancestor_conflict") else {
        eprintln!("skip mysql deep ancestor conflict test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let mut parent = job("parent");
    parent.target_path = "/a/b".to_string();
    parent.job_key = "Load:s3://bucket/parent:/a/b".to_string();
    store.create_or_get_by_request_id(parent).unwrap();

    let conflict = store
        .find_conflicting_active_transfer("/a/b/c/d", "test", "req-other")
        .unwrap()
        .expect("deep child target should conflict with active ancestor target");
    assert_eq!(conflict.job_id, "parent");

    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_rejects_same_job_key_with_different_command() {
    let Some((store, _store_url, base_url, db_name)) = mysql_store("already_running") else {
        eprintln!("skip mysql already running test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    store.create_or_get_by_request_id(job("first")).unwrap();

    let mut second = job("second");
    second.job_key = "Load:s3://bucket/first:/first".to_string();
    second.source_path = "s3://bucket/first".to_string();
    second.target_path = "/first".to_string();
    second.client_request_id = "req-second".to_string();
    second.command_json = "{\"overwrite\":true}".to_string();

    let err = store.create_or_get_by_request_id(second).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::TransferAlreadyRunning));

    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_service_returns_store_error_when_database_disappears_at_runtime() {
    let Some((store, store_url, base_url, db_name)) = mysql_store("runtime_unavailable") else {
        eprintln!("skip mysql runtime unavailable test: CURVINE_TRANSFER_MYSQL_URL is not set");
        return;
    };
    let service = TransferService::new(Arc::new(TransferStoreBackend::Mysql(store)));

    drop_mysql_database(&base_url, &db_name);

    let err = service
        .submit_transfer(SubmitTransferRequest {
            kind: TransferKindProto::TransferLoad as i32,
            source_path: "s3://bucket/runtime-unavailable".to_string(),
            target_path: "/runtime-unavailable".to_string(),
            client_request_id: "runtime-unavailable".to_string(),
            submitter: "mysql-test".to_string(),
            tenant: "default".to_string(),
            command: Vec::new(),
            protocol_version: Some(1),
        })
        .unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::TransferStoreUnavailable));
    let err = err.to_string();
    assert!(
        err.contains("transfer_jobs")
            || err.contains("Unknown database")
            || err.contains("doesn't exist")
            || err.contains("No database selected"),
        "unexpected runtime store error: {err}"
    );
    let unavailable_metrics = Metrics::text_output().unwrap();
    assert_eq!(
        metric_value(
            &unavailable_metrics,
            "transfer_store_unavailable",
            &["backend=\"mysql\""]
        ),
        Some(1.0)
    );
    assert!(
        metric_value(
            &unavailable_metrics,
            "transfer_store_unavailable_total",
            &["backend=\"mysql\""]
        )
        .unwrap_or_default()
            >= 1.0
    );

    create_mysql_database(&base_url, &db_name);
    let restored_store = MysqlTransferStore::open(&store_url).unwrap();
    drop(restored_store);

    let recovered = service
        .submit_transfer(SubmitTransferRequest {
            kind: TransferKindProto::TransferLoad as i32,
            source_path: "s3://bucket/runtime-recovered".to_string(),
            target_path: "/runtime-recovered".to_string(),
            client_request_id: "runtime-recovered".to_string(),
            submitter: "mysql-test".to_string(),
            tenant: "default".to_string(),
            command: Vec::new(),
            protocol_version: Some(1),
        })
        .unwrap();
    assert_eq!(recovered.target_path, "/runtime-recovered");

    let reopened = MysqlTransferStore::open(&store_url).unwrap();
    assert_eq!(reopened.count_active_transfers().unwrap(), 1);
    assert!(reopened.get_transfer(&recovered.job_id).unwrap().is_some());
    let recovered_metrics = Metrics::text_output().unwrap();
    assert!(
        metric_value(
            &recovered_metrics,
            "transfer_store_unavailable_duration_us_total",
            &["backend=\"mysql\""]
        )
        .unwrap_or_default()
            > 0.0
    );

    let conflict = service
        .submit_transfer(SubmitTransferRequest {
            kind: TransferKindProto::TransferLoad as i32,
            source_path: "s3://bucket/runtime-conflict".to_string(),
            target_path: "/runtime-recovered".to_string(),
            client_request_id: "runtime-conflict".to_string(),
            submitter: "mysql-test".to_string(),
            tenant: "default".to_string(),
            command: Vec::new(),
            protocol_version: Some(1),
        })
        .unwrap_err();
    assert!(
        matches!(conflict.kind(), ErrorKind::TransferTargetConflict),
        "unexpected conflict error: {conflict}"
    );
    drop_mysql_database(&base_url, &db_name);
}
