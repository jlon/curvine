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

use std::process::Command;
use std::process::Stdio;
use std::thread;
use std::time::{Duration, Instant};

use curvine_cli::cmds::transfer::{render_cancel_response, OutputFormat};
use curvine_common::proto::CancelTransferResponse;

#[test]
fn mount_accepts_short_config_without_clap_debug_assert() {
    let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .args([
            "mount",
            "s3://bucket/path",
            "/bucket/path",
            "-c",
            "s3.endpoint_url=http://example.invalid",
            "--help",
        ])
        .output()
        .expect("run curvine-cli mount --help");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("Short option names must be unique"),
        "mount -c triggered clap debug assert: {stderr}"
    );
    assert!(output.status.success(), "unexpected stderr: {stderr}");
}

#[test]
fn mount_redacts_sensitive_config_values() {
    let secret = "plain-secret-value";
    let access = "plain-access-value";
    let mut child = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .args([
            "--master-addrs",
            "127.0.0.1:1",
            "mount",
            "s3://bucket/path",
            "/path",
            "--check-path-consist=false",
            "-c",
            "s3.endpoint_url=http://example.invalid",
            "-c",
            "s3.credentials.secret=plain-secret-value",
            "-c",
            "s3.credentials.access=plain-access-value",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run curvine-cli mount");

    let start = Instant::now();
    loop {
        if child.try_wait().expect("poll curvine-cli mount").is_some() {
            break;
        }
        if start.elapsed() >= Duration::from_secs(2) {
            child.kill().expect("kill curvine-cli mount");
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let output = child.wait_with_output().expect("collect curvine-cli mount");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("s3.credentials.secret = ******"),
        "secret was not redacted: {stdout}"
    );
    assert!(
        stdout.contains("s3.credentials.access = ******"),
        "access credential was not redacted: {stdout}"
    );
    assert!(
        !stdout.contains(secret),
        "secret leaked in stdout: {stdout}"
    );
    assert!(
        !stdout.contains(access),
        "access leaked in stdout: {stdout}"
    );
}

#[test]
fn transfer_commands_are_registered() {
    for command in ["export", "transfer-status", "cancel-transfer", "transfer"] {
        let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
            .args([command, "--help"])
            .output()
            .unwrap_or_else(|err| panic!("run curvine-cli {command} --help: {err}"));

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "curvine-cli {command} --help failed: {stderr}"
        );
    }
}

#[test]
fn legacy_load_commands_remain_available_during_rolling_upgrade() {
    for command in ["load", "load-status", "cancel-load"] {
        let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
            .args([command, "--help"])
            .output()
            .expect("run curvine-cli legacy load command");

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "curvine-cli {command} --help failed: {stderr}"
        );
    }
}

#[test]
fn transfer_subcommands_are_registered() {
    for subcommand in ["list", "status", "tasks", "cancel", "retry", "tenants"] {
        let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
            .args(["transfer", subcommand, "--help"])
            .output()
            .unwrap_or_else(|err| panic!("run curvine-cli transfer {subcommand} --help: {err}"));

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "curvine-cli transfer {subcommand} --help failed: {stderr}"
        );
    }
}

#[test]
fn transfer_tenants_command_accepts_pagination_and_format() {
    let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .args([
            "transfer",
            "tenants",
            "--limit",
            "10",
            "--page-token",
            "20",
            "--format",
            "json",
            "--help",
        ])
        .output()
        .expect("run curvine-cli transfer tenants --help");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "curvine-cli transfer tenants --help failed: {stderr}"
    );
}

#[test]
fn transfer_cancel_command_accepts_run_id_and_format() {
    let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .args([
            "transfer", "cancel", "job-123", "--run-id", "7", "--format", "json", "--help",
        ])
        .output()
        .expect("run curvine-cli transfer cancel --help");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "curvine-cli transfer cancel --help failed: {stderr}"
    );
}

#[test]
fn transfer_list_accepts_submitter_and_tenant_filters() {
    let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .args([
            "transfer",
            "list",
            "--submitter",
            "flink",
            "--tenant",
            "tenant-a",
            "--limit",
            "10",
            "--help",
        ])
        .output()
        .expect("run curvine-cli transfer list --help");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "curvine-cli transfer list --help failed: {stderr}"
    );
}

#[test]
fn transfer_cancel_table_output_uses_server_state() {
    let response = CancelTransferResponse {
        job_id: "transfer-job-1234567890".to_string(),
        state: 5,
    };

    let output = render_cancel_response(&response, OutputFormat::Table).unwrap();

    assert!(output.contains("transfer-job-1234567890"));
    assert!(output.contains("Canceling"));
    assert!(!output.contains("Canceled"));
}

#[test]
fn transfer_cancel_json_output_preserves_response() {
    let response = CancelTransferResponse {
        job_id: "transfer-job-terminal".to_string(),
        state: 8,
    };

    let output = render_cancel_response(&response, OutputFormat::Json).unwrap();
    let parsed: CancelTransferResponse = serde_json::from_str(&output).unwrap();

    assert_eq!(parsed.job_id, response.job_id);
    assert_eq!(parsed.state, response.state);
}
