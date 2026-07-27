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

use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path as StdPath;
use std::process::Command;
use std::thread;
use std::time::Duration;

use curvine_common::conf::ClusterConf;
use curvine_common::fs::Path;
use curvine_common::proto::TransferStateProto;
use curvine_common::state::{
    MountOptions, TransferCommand, TransferJobRecord, TransferKind, TransferProgress,
    TransferState, TransferTaskRecord, TransferTaskState,
};
use curvine_server::test::MiniCluster;
use curvine_server::transfer::{SqliteTransferStore, TransferServer, TransferStore};
use orpc::common::LocalTime;
use orpc::io::net::NetUtils;
use orpc::runtime::RpcRuntime;
use serde_json::Value;

#[test]
fn transfer_cli_reads_real_transfer_service_state() {
    let test_id = format!(
        "transfer-cli-e2e-{}-{}",
        std::process::id(),
        LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    let ufs_dir = base_dir.join("ufs");
    let cli_mount_dir = base_dir.join("cli-mount");
    fs::create_dir_all(&ufs_dir).unwrap();
    fs::create_dir_all(&cli_mount_dir).unwrap();
    fs::write(ufs_dir.join("hello.txt"), b"hello-from-cli").unwrap();
    fs::write(
        ufs_dir.join("no-overwrite-load-source.txt"),
        b"new-load-content",
    )
    .unwrap();
    fs::write(cli_mount_dir.join("watch-load.txt"), b"watch-load-content").unwrap();

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_url = format!("sqlite://{}", base_dir.join("transfer.db").display());
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.instance_id = "transfer-cli-e2e".to_string();
    conf.transfer.metadata_replica_refresh_interval_str = "1s".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    let runtime_conf = cluster.cluster_conf.clone();
    let conf_path = base_dir.join("curvine-cluster.toml");
    fs::write(&conf_path, toml::to_string(&runtime_conf).unwrap()).unwrap();

    cluster.start_cluster();
    let rt = cluster.clone_client_rt();
    rt.block_on(async {
        let fs = cluster.new_fs();
        fs.mount(
            &Path::from_str(format!("file://{}", ufs_dir.display())).unwrap(),
            &Path::from_str("/mnt").unwrap(),
            MountOptions::builder().build(),
        )
        .await
        .unwrap();
        fs.write_string(
            &Path::from_str("/mnt/export-cli.txt").unwrap(),
            "export-from-cli",
        )
        .await
        .unwrap();
        fs.write_string(
            &Path::from_str("/mnt/export-watch.txt").unwrap(),
            "export-watch-content",
        )
        .await
        .unwrap();
        fs.write_string(
            &Path::from_str("/mnt/no-overwrite-load-target.txt").unwrap(),
            "existing-load-content",
        )
        .await
        .unwrap();
        fs.write_string(
            &Path::from_str("/mnt/no-overwrite-export.txt").unwrap(),
            "new-export-content",
        )
        .await
        .unwrap();
    });
    fs::write(
        ufs_dir.join("no-overwrite-export.txt"),
        b"existing-export-content",
    )
    .unwrap();

    let transfer = TransferServer::with_conf(runtime_conf.clone()).unwrap();
    thread::spawn(move || transfer.block_on_start());
    thread::sleep(Duration::from_millis(500));

    run_cli(
        &conf_path,
        &[
            "mount",
            &format!("file://{}", cli_mount_dir.display()),
            "/cli",
            "--check-path-consist",
            "false",
            "--auto-cache",
            "false",
        ],
    );

    let watched_load = run_cli(
        &conf_path,
        &[
            "load",
            &format!("file://{}/watch-load.txt", cli_mount_dir.display()),
            "/cli/watch-load.txt",
            "--watch",
        ],
    );
    let watched_load_job_id = extract_job_id(&watched_load.stdout);
    let watched_load_status = wait_cli_state(
        &conf_path,
        &watched_load_job_id,
        TransferStateProto::TransferCompleted,
    );
    assert_eq!(
        watched_load_status["state"].as_i64(),
        Some(TransferStateProto::TransferCompleted as i64)
    );

    let load = run_cli(
        &conf_path,
        &["load", &format!("file://{}/hello.txt", ufs_dir.display())],
    );
    let job_id = extract_job_id(&load.stdout);

    let completed_status =
        wait_cli_state(&conf_path, &job_id, TransferStateProto::TransferCompleted);
    assert_eq!(
        completed_status["job_id"].as_str(),
        Some(job_id.as_str()),
        "status json should describe the submitted job: {completed_status}"
    );

    let load_status = run_cli(&conf_path, &["load-status", &job_id]);
    assert!(
        load_status.stdout.contains("Completed"),
        "load-status should query Transfer state: {}",
        load_status.stdout
    );

    let tasks = run_cli_json(
        &conf_path,
        &["transfer", "tasks", &job_id, "--format", "json"],
    );
    assert!(
        tasks
            .as_array()
            .map(|items| !items.is_empty())
            .unwrap_or(false),
        "tasks json should contain at least one task: {tasks}"
    );

    let detailed_status = run_cli(
        &conf_path,
        &["transfer", "status", &job_id, "--verbose", "--full-id"],
    );
    assert!(
        detailed_status.stdout.contains(&job_id),
        "verbose status should include the full job id: {}",
        detailed_status.stdout
    );
    assert!(
        detailed_status.stdout.contains("Tasks"),
        "transfer status should display task counts: {}",
        detailed_status.stdout
    );

    let list = run_cli_json(&conf_path, &["transfer", "list", "--format", "json"]);
    assert!(
        list["jobs"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|job| job["job_id"].as_str() == Some(job_id.as_str())),
        "list json should contain submitted job {job_id}: {list}"
    );

    let same_path_export = run_cli(&conf_path, &["export", "/mnt/hello.txt"]);
    let same_path_export_job_id = extract_job_id(&same_path_export.stdout);
    assert_ne!(
        same_path_export_job_id, job_id,
        "load and export for the same Curvine path must not share one idempotency key"
    );
    let same_path_export_status = wait_cli_state(
        &conf_path,
        &same_path_export_job_id,
        TransferStateProto::TransferCompleted,
    );
    assert_eq!(
        same_path_export_status["kind"].as_i64(),
        Some(TransferKind::Export as i32 as i64),
        "same-path export must create an Export job: {same_path_export_status}"
    );

    let export = run_cli(&conf_path, &["export", "/mnt/export-cli.txt"]);
    let export_job_id = extract_job_id(&export.stdout);
    let export_status = wait_cli_state(
        &conf_path,
        &export_job_id,
        TransferStateProto::TransferCompleted,
    );
    assert_eq!(
        export_status["job_id"].as_str(),
        Some(export_job_id.as_str()),
        "export status json should describe the submitted job: {export_status}"
    );
    assert_eq!(
        fs::read_to_string(ufs_dir.join("export-cli.txt")).unwrap(),
        "export-from-cli"
    );

    let watched_export = run_cli(&conf_path, &["export", "/mnt/export-watch.txt", "--watch"]);
    let watched_export_job_id = extract_job_id(&watched_export.stdout);
    wait_cli_state(
        &conf_path,
        &watched_export_job_id,
        TransferStateProto::TransferCompleted,
    );
    assert_eq!(
        fs::read_to_string(ufs_dir.join("export-watch.txt")).unwrap(),
        "export-watch-content"
    );

    let no_overwrite_load = run_cli(
        &conf_path,
        &[
            "load",
            &format!("file://{}/no-overwrite-load-source.txt", ufs_dir.display()),
            "/mnt/no-overwrite-load-target.txt",
            "--no-overwrite",
        ],
    );
    let no_overwrite_load_job_id = extract_job_id(&no_overwrite_load.stdout);
    wait_cli_state(
        &conf_path,
        &no_overwrite_load_job_id,
        TransferStateProto::TransferFailed,
    );

    let no_overwrite_export = run_cli(
        &conf_path,
        &["export", "/mnt/no-overwrite-export.txt", "--no-overwrite"],
    );
    let no_overwrite_export_job_id = extract_job_id(&no_overwrite_export.stdout);
    wait_cli_state(
        &conf_path,
        &no_overwrite_export_job_id,
        TransferStateProto::TransferFailed,
    );
    assert_eq!(
        fs::read_to_string(ufs_dir.join("no-overwrite-export.txt")).unwrap(),
        "existing-export-content"
    );

    let first_page = run_cli_json(
        &conf_path,
        &["transfer", "list", "--limit", "1", "--format", "json"],
    );
    assert_eq!(
        first_page["jobs"].as_array().map(Vec::len),
        Some(1),
        "single-page list should honor limit: {first_page}"
    );
    assert!(
        first_page["next_page_token"].as_str().is_some(),
        "single-page list should expose next_page_token: {first_page}"
    );

    let all_pages = run_cli_json(
        &conf_path,
        &[
            "transfer", "list", "--limit", "1", "--all", "--format", "json",
        ],
    );
    let empty_jobs = Vec::new();
    let all_jobs = all_pages["jobs"].as_array().unwrap_or(&empty_jobs);
    assert!(
        all_jobs
            .iter()
            .any(|job| job["job_id"].as_str() == Some(job_id.as_str())),
        "--all list should include load job {job_id}: {all_pages}"
    );
    assert!(
        all_jobs
            .iter()
            .any(|job| job["job_id"].as_str() == Some(export_job_id.as_str())),
        "--all list should include export job {export_job_id}: {all_pages}"
    );
    assert!(
        all_pages["next_page_token"].as_str().is_none(),
        "--all list should consume every page: {all_pages}"
    );

    let load_jobs = run_cli_json(
        &conf_path,
        &[
            "transfer",
            "list",
            "--kind",
            "load",
            "--state",
            "completed",
            "--all",
            "--format",
            "json",
        ],
    );
    let empty_load_jobs = Vec::new();
    let load_job_items = load_jobs["jobs"].as_array().unwrap_or(&empty_load_jobs);
    assert!(
        load_job_items
            .iter()
            .all(|job| job["kind"].as_i64() == Some(TransferKind::Load as i32 as i64)),
        "kind filter should only return Load jobs: {load_jobs}"
    );
    assert!(
        load_job_items
            .iter()
            .any(|job| job["job_id"].as_str() == Some(job_id.as_str())),
        "kind/state filter should include completed load job {job_id}: {load_jobs}"
    );

    let tenants = run_cli_json(
        &conf_path,
        &["transfer", "tenants", "--all", "--format", "json"],
    );
    let empty_tenants = Vec::new();
    let tenant_items = tenants["tenants"].as_array().unwrap_or(&empty_tenants);
    assert!(
        !tenant_items.is_empty(),
        "transfer tenants should return server data: {tenants}"
    );

    let cancel_job_id = "cli-cancel-load-pending-job";
    let store = SqliteTransferStore::open(runtime_conf.transfer.sqlite_store_path()).unwrap();
    store
        .create_or_get_by_request_id(cancel_job(
            cancel_job_id,
            &runtime_conf.transfer.instance_id,
        ))
        .unwrap();
    store
        .insert_tasks(vec![cancel_task(cancel_job_id)])
        .unwrap();
    let cancel = run_cli(&conf_path, &["cancel-load", cancel_job_id]);
    assert!(
        cancel.stdout.contains(cancel_job_id),
        "cancel output should include full job id: {}",
        cancel.stdout
    );
    assert!(
        cancel.stdout.contains("Canceling") || cancel.stdout.contains("Canceled"),
        "cancel output should show server cancel state: {}",
        cancel.stdout
    );
    wait_cli_state(
        &conf_path,
        cancel_job_id,
        TransferStateProto::TransferCanceled,
    );

    let _ = fs::remove_dir_all(base_dir);
}

#[test]
#[ignore = "requires Docker to run MinIO"]
fn transfer_cli_loads_from_and_exports_to_minio() {
    let minio = MinioServer::start();
    let test_id = format!(
        "transfer-cli-minio-e2e-{}-{}",
        std::process::id(),
        LocalTime::mills()
    );
    let base_dir = std::env::temp_dir().join(&test_id);
    fs::create_dir_all(&base_dir).unwrap();
    let payload = "minio-transfer-payload";
    let source_file = base_dir.join("source.txt");
    fs::write(&source_file, payload).unwrap();

    let bucket = format!("curvine-transfer-e2e-{}", LocalTime::mills());
    minio.create_bucket(&bucket);
    minio.put_file(&base_dir, "source.txt", &format!("{bucket}/input/load.txt"));

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = base_dir.join("meta").display().to_string();
    conf.journal.journal_dir = base_dir.join("journal").display().to_string();
    conf.worker.data_dir = vec![base_dir.join("worker").display().to_string()];
    conf.transfer.enabled = true;
    conf.transfer.store_url = format!("sqlite://{}", base_dir.join("transfer.db").display());
    conf.transfer.hostname = "localhost".to_string();
    conf.transfer.rpc_port = NetUtils::hold_available_port();
    conf.transfer.web_port = NetUtils::hold_available_port();
    conf.transfer.instance_id = "transfer-cli-minio-e2e".to_string();
    conf.transfer.metadata_replica_refresh_interval_str = "1s".to_string();
    conf.transfer.endpoints = vec![format!(
        "{}:{}",
        conf.transfer.hostname, conf.transfer.rpc_port
    )];
    conf.transfer.init().unwrap();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    let runtime_conf = cluster.cluster_conf.clone();
    let conf_path = base_dir.join("curvine-cluster.toml");
    fs::write(&conf_path, toml::to_string(&runtime_conf).unwrap()).unwrap();
    cluster.start_cluster();

    let transfer = TransferServer::with_conf(runtime_conf).unwrap();
    thread::spawn(move || transfer.block_on_start());
    thread::sleep(Duration::from_millis(500));

    let cv_mount = "/minio";
    let endpoint_conf = format!("s3.endpoint_url={}", minio.endpoint);
    let access_conf = format!("s3.credentials.access={}", MinioServer::ROOT_USER);
    let secret_conf = format!("s3.credentials.secret={}", MinioServer::ROOT_PASSWORD);
    let bucket_uri = format!("s3://{bucket}");
    run_cli(
        &conf_path,
        &[
            "mount",
            &bucket_uri,
            cv_mount,
            "-c",
            &endpoint_conf,
            "-c",
            "s3.region_name=us-east-1",
            "-c",
            &access_conf,
            "-c",
            &secret_conf,
            "--check-path-consist",
            "false",
            "--auto-cache",
            "false",
        ],
    );

    let source_uri = format!("s3://{bucket}/input/load.txt");
    let load = run_cli(
        &conf_path,
        &["load", &source_uri, "/minio/load.txt", "--watch"],
    );
    let load_job_id = extract_job_id(&load.stdout);
    wait_cli_state(
        &conf_path,
        &load_job_id,
        TransferStateProto::TransferCompleted,
    );

    let export = run_cli(&conf_path, &["export", "/minio/load.txt", "--watch"]);
    let export_job_id = extract_job_id(&export.stdout);
    wait_cli_state(
        &conf_path,
        &export_job_id,
        TransferStateProto::TransferCompleted,
    );
    assert_eq!(minio.read_object(&format!("{bucket}/load.txt")), payload);

    let _ = fs::remove_dir_all(base_dir);
}

struct CliOutput {
    stdout: String,
}

fn run_cli(conf_path: &StdPath, args: &[&str]) -> CliOutput {
    let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .env("CURVINE_CONF_FILE", conf_path)
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("run curvine-cli {args:?}: {err}"));
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "curvine-cli {args:?} failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    CliOutput { stdout }
}

fn run_cli_json(conf_path: &StdPath, args: &[&str]) -> Value {
    let output = run_cli(conf_path, args);
    serde_json::from_str(&output.stdout)
        .unwrap_or_else(|err| panic!("parse cli json from {args:?}: {err}\n{}", output.stdout))
}

fn wait_cli_state(conf_path: &StdPath, job_id: &str, expected: TransferStateProto) -> Value {
    for _ in 0..80 {
        let status = run_cli_json(
            conf_path,
            &["transfer", "status", job_id, "--format", "json"],
        );
        if status["state"].as_i64() == Some(expected as i64) {
            return status;
        }
        thread::sleep(Duration::from_millis(250));
    }
    panic!("transfer {job_id} did not reach {expected:?}");
}

const MINIO_IMAGE: &str =
    "minio/minio@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e";
const MINIO_MC_IMAGE: &str =
    "minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727";

struct MinioServer {
    name: String,
    endpoint: String,
}

impl MinioServer {
    const ROOT_USER: &str = "minioadmin";
    const ROOT_PASSWORD: &str = "minioadmin";

    fn start() -> Self {
        let port = NetUtils::get_available_port();
        let name = format!(
            "curvine-transfer-minio-{}-{}",
            std::process::id(),
            LocalTime::mills()
        );
        let endpoint = format!("http://127.0.0.1:{port}");
        let port_mapping = format!("127.0.0.1:{port}:9000");
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &name,
                "-p",
                &port_mapping,
                "-e",
                "MINIO_ROOT_USER=minioadmin",
                "-e",
                "MINIO_ROOT_PASSWORD=minioadmin",
                MINIO_IMAGE,
                "server",
                "/data",
                "--address",
                ":9000",
            ])
            .output()
            .unwrap_or_else(|err| panic!("start MinIO container: {err}"));
        assert!(
            output.status.success(),
            "start MinIO container failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            wait_minio_ready(port, Duration::from_secs(30)),
            "MinIO did not become ready"
        );
        Self { name, endpoint }
    }

    fn create_bucket(&self, bucket: &str) {
        self.run_mc(
            None,
            &["mb", "--ignore-existing", &format!("minio/{bucket}")],
        );
    }

    fn put_file(&self, source_dir: &StdPath, source_name: &str, object: &str) {
        self.run_mc(
            Some(source_dir),
            &[
                "cp",
                &format!("/data/{source_name}"),
                &format!("minio/{object}"),
            ],
        );
    }

    fn read_object(&self, object: &str) -> String {
        let output = self.run_mc_output(None, &["cat", &format!("minio/{object}")]);
        String::from_utf8(output.stdout).unwrap()
    }

    fn run_mc(&self, source_dir: Option<&StdPath>, args: &[&str]) {
        let output = self.run_mc_output(source_dir, args);
        assert!(
            output.status.success(),
            "MinIO client {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn run_mc_output(&self, source_dir: Option<&StdPath>, args: &[&str]) -> std::process::Output {
        let host = format!(
            "http://{}:{}@{}",
            Self::ROOT_USER,
            Self::ROOT_PASSWORD,
            self.endpoint.trim_start_matches("http://")
        );
        let mut command = Command::new("docker");
        command
            .args(["run", "--rm", "--network", "host", "-e"])
            .arg(format!("MC_HOST_minio={host}"));
        if let Some(source_dir) = source_dir {
            command
                .arg("-v")
                .arg(format!("{}:/data:ro", source_dir.display()));
        }
        command
            .arg(MINIO_MC_IMAGE)
            .args(args)
            .output()
            .unwrap_or_else(|err| panic!("run MinIO client {args:?}: {err}"))
    }
}

impl Drop for MinioServer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .status();
    }
}

fn wait_minio_ready(port: u16, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
            if stream
                .write_all(
                    b"GET /minio/health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .is_ok()
            {
                let mut response = String::new();
                if stream.read_to_string(&mut response).is_ok() && response.contains("200 OK") {
                    return true;
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn extract_job_id(stdout: &str) -> String {
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("Job ID: "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("load output did not contain Job ID: {stdout}"))
        .to_string()
}

fn cancel_job(job_id: &str, owner: &str) -> TransferJobRecord {
    let command = TransferCommand {
        kind: TransferKind::Load,
        source_path: "file:///cancel.txt".to_string(),
        target_path: "/mnt/cancel.txt".to_string(),
        client_request_id: format!("{job_id}-request"),
        submitter: "cli-e2e".to_string(),
        tenant: "test".to_string(),
        options: Default::default(),
    };
    let now = LocalTime::mills() as i64;
    TransferJobRecord {
        job_key: command.client_request_id.clone(),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: command.kind,
        source_path: command.source_path.clone(),
        target_path: command.target_path.clone(),
        command_json: serde_json::to_string(&command).unwrap(),
        mount_snapshot_json: String::new(),
        secret_ref_json: String::new(),
        cluster_snapshot_version: 1,
        cv_metadata_epoch: None,
        state: TransferState::Running,
        owner: owner.to_string(),
        lease_epoch: 1,
        lease_expire_at: now + 120_000,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: command.client_request_id,
        submitter: command.submitter,
        tenant: command.tenant,
        created_at: now - 1_000,
        updated_at: now - 1_000,
    }
}

fn cancel_task(job_id: &str) -> TransferTaskRecord {
    let now = LocalTime::mills() as i64;
    TransferTaskRecord {
        job_id: job_id.to_string(),
        run_id: 1,
        task_id: "cancel-task".to_string(),
        attempt_id: 1,
        source_path: "file:///cancel.txt".to_string(),
        target_path: "/mnt/cancel.txt".to_string(),
        worker_id: 0,
        worker_session_id: String::new(),
        source_read_plan_json: String::new(),
        report_target_json: "{}".to_string(),
        state: TransferTaskState::Running,
        progress: TransferProgress::default(),
        retry_count: 0,
        attempt_started_at: now - 1_000,
        last_report_at: now - 1_000,
        stale_deadline_at: now + 120_000,
        updated_at: now - 1_000,
    }
}
