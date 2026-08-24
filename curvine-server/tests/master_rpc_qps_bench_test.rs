use curvine_config::ClusterConf;
use curvine_error::FsError;
use curvine_fs_api::RpcCode;
use curvine_proto::{GetFileStatusRequest, GetFileStatusResponse};
use curvine_rpc::client::RpcClient;
use curvine_rpc::message::Builder;
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

const TOPOLOGY_CACHE_ENTRY_LIMIT: usize = 16_384;
const DEFAULT_FILE_COUNT: usize = TOPOLOGY_CACHE_ENTRY_LIMIT * 2;
const DEFAULT_CONNECTIONS: usize = 16;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[test]
#[ignore = "manual end-to-end Master RPC QPS comparison"]
fn measure_master_rpc_file_status_qps() {
    let files = env_usize("CURVINE_MASTER_RPC_QPS_FILES", DEFAULT_FILE_COUNT);
    let connections = env_usize("CURVINE_MASTER_RPC_QPS_CONNECTIONS", DEFAULT_CONNECTIONS);
    let seconds = env_usize("CURVINE_MASTER_RPC_QPS_SECONDS", 5);
    assert!(
        files > TOPOLOGY_CACHE_ENTRY_LIMIT,
        "working set must exceed the topology cache entry limit"
    );
    assert!(connections > 0);
    assert!(seconds > 0);

    let test_id = Utils::rand_str(8);
    let base_dir = Utils::test_sub_dir(format!("master-rpc-qps-{test_id}"));
    let mut conf = ClusterConf::default();
    conf.master.meta_dir = format!("{base_dir}/meta");
    conf.journal.journal_dir = format!("{base_dir}/journal");
    conf.worker.data_dir = vec![format!("[MEM:128MB]{base_dir}/worker")];
    conf.client.short_circuit = false;
    conf.master.audit_logging_enabled = false;
    conf.master.log.level = "WARN".to_string();
    conf.worker.log.level = "WARN".to_string();
    // This benchmark measures steady-state metadata reads. Populating the
    // fixture must not turn it into a sequential Raft write-latency benchmark.
    // The FileStatus RPC path does not branch on journal enablement once the
    // leader is metadata-current.
    conf.journal.enable = false;

    let cluster = Arc::new(MiniCluster::with_num(&conf, 1, 1));
    cluster.start_cluster();
    let master_fs = cluster.get_active_master_fs();
    master_fs.mkdir("/rpc-qps", true).unwrap();
    let paths = (0..files)
        .map(|index| format!("/rpc-qps/file-{index:05}"))
        .collect::<Vec<_>>();
    for path in &paths {
        master_fs.create(path, false).unwrap();
    }

    let rt = cluster.clone_client_rt();
    let master_addr = cluster.master_conf().master_addr();
    let client_conf = cluster.master_conf().client_rpc_conf();
    let paths = Arc::new(paths);
    let duration = Duration::from_secs(seconds as u64);
    let (reads, mut samples, elapsed) = rt.block_on(async {
        let start = Arc::new(Barrier::new(connections));
        let mut tasks = Vec::with_capacity(connections);
        for connection in 0..connections {
            let client = RpcClient::with_buffer(rt.clone(), &master_addr, &client_conf)
                .await
                .unwrap();
            let start = start.clone();
            let paths = paths.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                let begin = Instant::now();
                let mut reads = 0u64;
                let mut samples = Vec::new();
                while begin.elapsed() < duration {
                    let path = paths[(reads as usize + connection) % paths.len()].clone();
                    let started = reads.is_multiple_of(128).then(Instant::now);
                    let response = client
                        .rpc(
                            Builder::new_rpc(RpcCode::FileStatus)
                                .proto_header(GetFileStatusRequest { path: path.clone() })
                                .build(),
                        )
                        .await
                        .unwrap();
                    response.check_error_ext::<FsError>().unwrap();
                    let status: GetFileStatusResponse = response.parse_header().unwrap();
                    assert_eq!(status.status.path, path);
                    if let Some(started) = started {
                        samples.push(started.elapsed().as_micros() as u64);
                    }
                    reads += 1;
                }
                (reads, samples, begin.elapsed())
            }));
        }

        let mut reads = 0u64;
        let mut samples = Vec::new();
        let mut elapsed = Duration::ZERO;
        for task in tasks {
            let (count, mut task_samples, took) = task.await.unwrap();
            reads += count;
            samples.append(&mut task_samples);
            elapsed = elapsed.max(took);
        }
        (reads, samples, elapsed)
    });

    samples.sort_unstable();
    let p99 = samples
        .get(samples.len().saturating_sub(1) * 99 / 100)
        .copied()
        .unwrap_or_default();
    println!(
        "MASTER_RPC_FILE_STATUS_QPS files={} connections={} reads={} qps={:.2} sampled_p99_us={} samples={}",
        files,
        connections,
        reads,
        reads as f64 / elapsed.as_secs_f64(),
        p99,
        samples.len(),
    );
}
