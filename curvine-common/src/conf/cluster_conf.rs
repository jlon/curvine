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

use crate::conf::CliConf;
use crate::conf::{ClientConf, FuseConf, JobConf, JournalConf, MasterConf, WorkerConf};
use crate::rocksdb::DBConf;
use crate::version;
use log::info;
use orpc::client::{ClientConf as RpcConf, ClientFactory, SyncClient};
use orpc::common::{LogConf, Utils};
use orpc::io::net::{InetAddr, NodeAddr};
use orpc::io::retry::TimeBondedRetryBuilder;
use orpc::server::ServerConf;
use orpc::{err_box, try_err, CommonResult};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::{Display, Formatter};
use std::fs::read_to_string;
use std::time::Duration;

// Cluster configuration files.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConf {
    pub format_master: bool,

    pub format_worker: bool,

    // Whether it is in unit test state.In this state, the data will not flow normally, which facilitates unit tests to obtain data.
    pub testing: bool,

    pub cluster_id: String,

    pub master: MasterConf,

    // Log synchronization configuration.
    pub journal: JournalConf,

    pub worker: WorkerConf,

    pub log: LogConf,

    pub client: ClientConf,

    pub fuse: FuseConf,

    pub s3_gateway: S3GatewayConf,

    pub nfs_gateway: NfsGatewayConf,

    pub job: JobConf,

    pub cli: CliConf,
}

impl ClusterConf {
    pub const DEFAULT_HOSTNAME: &'static str = "localhost";
    pub const DEFAULT_MASTER_PORT: u16 = 8995;
    pub const DEFAULT_RAFT_PORT: u16 = 8996;
    pub const DEFAULT_WORKER_PORT: u16 = 8997;
    pub const DEFAULT_MASTER_WEB_PORT: u16 = 9000;
    pub const DEFAULT_WORKER_WEB_PORT: u16 = 9001;

    pub const ENV_MASTER_HOSTNAME: &'static str = "CURVINE_MASTER_HOSTNAME";
    pub const ENV_WORKER_HOSTNAME: &'static str = "CURVINE_WORKER_HOSTNAME";
    pub const ENV_CLIENT_HOSTNAME: &'static str = "CURVINE_CLIENT_HOSTNAME";
    pub const ENV_CONF_FILE: &'static str = "CURVINE_CONF_FILE";

    pub fn from<T: AsRef<str>>(path: T) -> CommonResult<Self> {
        let str = try_err!(read_to_string(path.as_ref()));
        let mut conf = try_err!(toml::from_str::<Self>(&str));

        if let Ok(v) = env::var(Self::ENV_MASTER_HOSTNAME) {
            conf.master.hostname = v.to_owned();
            conf.journal.hostname = v;
        }

        // Apply worker hostname from environment variable (used by worker process)
        if let Ok(v) = env::var(Self::ENV_WORKER_HOSTNAME) {
            conf.worker.hostname = v;
        }

        // Apply client hostname from environment variable
        if let Ok(v) = env::var(Self::ENV_CLIENT_HOSTNAME) {
            conf.client.hostname = v;
        }

        conf.master.init()?;
        conf.client.init()?;
        conf.fuse.init()?;
        conf.job.init()?;

        if conf.client.master_addrs.is_empty() {
            for peer in &mut conf.journal.journal_addrs {
                let node = InetAddr::new(&peer.hostname, conf.master.rpc_port);
                conf.client.master_addrs.push(node);
            }
        }

        Ok(conf)
    }

    pub fn check_master_hostname(&mut self) -> CommonResult<()> {
        let hostname_exists = self
            .journal
            .journal_addrs
            .iter()
            .any(|peer| peer.hostname == self.master.hostname);

        if !hostname_exists {
            return err_box!(
                "hostname '{}' from {} is not found in journal_addrs. Available hostnames: [{}]",
                self.master.hostname,
                Self::ENV_MASTER_HOSTNAME,
                self.journal
                    .journal_addrs
                    .iter()
                    .map(|peer| peer.hostname.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        Ok(())
    }

    // Master service starts configuration.
    pub fn master_server_conf(&self) -> ServerConf {
        let mut conf = ServerConf::with_hostname(&self.master.hostname, self.master.rpc_port);
        conf.name = format!("{}-master", self.cluster_id);
        conf.io_threads = self.master.io_threads;
        conf.worker_threads = self.master.worker_threads;
        // master will automatically close the idle connection, and the customer service will automatically maintain a heartbeat.
        conf.close_idle = self.master.io_close_idle;
        conf.timeout_ms = self.master.io_timeout_ms();
        conf
    }

    pub fn master_web_conf(&self) -> ServerConf {
        let mut web_conf = ServerConf::with_hostname(&self.master.hostname, self.master.web_port);
        web_conf.name = format!("{}-master", self.cluster_id);
        web_conf.io_threads = self.master.io_threads;
        web_conf.worker_threads = self.master.worker_threads;
        web_conf
    }

    pub fn worker_addr(&self) -> InetAddr {
        InetAddr::new(self.worker.hostname.clone(), self.worker.rpc_port)
    }

    pub fn master_addr(&self) -> InetAddr {
        InetAddr::new(&self.master.hostname, self.master.rpc_port)
    }

    // Get all master nodes
    pub fn master_nodes(&self) -> Vec<NodeAddr> {
        let mut map = vec![];

        let start = 100;
        if self.client.master_addrs.is_empty() {
            map.push(NodeAddr::from_addr(start, self.master_addr()));
        } else {
            for (index, addr) in self.client.master_addrs.iter().enumerate() {
                let id = start + index as u64;
                map.push(NodeAddr::from_addr(id, addr.clone()));
            }
        }
        map
    }

    pub fn masters_string(&self) -> String {
        let res: Vec<String> = self
            .master_nodes()
            .iter()
            .map(|x| format!("{}", x.addr))
            .collect();
        res.join(",")
    }

    pub fn worker_server_conf(&self) -> ServerConf {
        let mut conf = ServerConf::with_hostname(&self.worker.hostname, self.worker.rpc_port);
        conf.name = format!("{}-worker", self.cluster_id);
        conf.io_threads = self.worker.io_threads;
        conf.worker_threads = self.worker.worker_threads;

        // The raw client used by the worker does not currently implement heartbeat checks, so the default server does not actively close the connection.
        conf.close_idle = self.worker.io_close_idle;
        conf.timeout_ms = self.worker.io_timeout_ms();

        conf.enable_splice = self.worker.enable_splice;
        conf.pipe_buf_size = self.worker.pipe_buf_size;
        conf.pipe_pool_init_cap = self.worker.pipe_pool_init_cap;
        conf.pipe_pool_max_cap = self.worker.pipe_pool_max_cap;
        conf.pipe_pool_idle_time = self.worker.pipe_pool_idle_time;

        conf.enable_send_file = self.worker.enable_send_file;
        conf
    }

    pub fn worker_web_conf(&self) -> ServerConf {
        let mut web_conf = ServerConf::with_hostname(&self.worker.hostname, self.worker.web_port);
        web_conf.name = format!("{}-web", self.cluster_id);
        web_conf.io_threads = self.worker.io_threads;
        web_conf.worker_threads = self.worker.worker_threads;
        web_conf
    }

    pub fn client_rpc_conf(&self) -> RpcConf {
        self.client.client_rpc_conf()
    }

    // Test use
    pub fn worker_sync_client(&self) -> CommonResult<SyncClient> {
        let factory = ClientFactory::new(self.client_rpc_conf());
        Ok(factory.create_sync(&self.worker_addr())?)
    }

    pub fn format() -> Self {
        Self {
            format_master: true,
            ..Default::default()
        }
    }

    // Test and modify the metadata-related path.
    pub fn change_test_meta_dir<T: AsRef<str>>(&mut self, name: T) {
        let pid = std::process::id();
        let rand = Utils::rand_str(6);
        let base = Utils::cur_dir_sub(format!(
            "../target/testing/{}_{}_{}",
            name.as_ref(),
            pid,
            rand
        ));
        self.master.meta_dir = format!("{}/meta", base);
        self.journal.journal_dir = format!("{}/journal", base);
    }

    // Get the rocksdb configuration used to obtain metadata
    pub fn meta_rocks_conf(&self) -> DBConf {
        DBConf::new(&self.master.meta_dir)
            .set_compress_type(&self.master.meta_compression_type)
            .set_disable_wal(self.master.meta_disable_wal)
            .set_db_write_buffer_size(&self.master.meta_db_write_buffer_size)
            .set_write_buffer_size(&self.master.meta_write_buffer_size)
    }

    pub fn io_retry_policy_builder(&self) -> TimeBondedRetryBuilder {
        TimeBondedRetryBuilder::new(
            Duration::from_millis(self.client.rpc_retry_max_duration_ms),
            Duration::from_millis(self.client.rpc_retry_min_sleep_ms),
            Duration::from_millis(self.client.rpc_retry_max_sleep_ms),
        )
    }

    pub fn print(&self) {
        let conf = self.to_pretty_toml().unwrap();
        info!("git version: {}", version::GIT_VERSION);
        info!("cluster conf start: \n{}\n", conf);
    }

    pub fn to_pretty_toml(&self) -> CommonResult<String> {
        Ok(toml::to_string_pretty(self)?)
    }
}

impl Default for ClusterConf {
    fn default() -> Self {
        Self {
            format_master: true,
            format_worker: true,
            testing: false,
            cluster_id: "curvine".to_string(),
            master: Default::default(),
            journal: Default::default(),
            worker: Default::default(),
            log: Default::default(),
            client: Default::default(),
            fuse: FuseConf::default(),
            s3_gateway: Default::default(),
            nfs_gateway: Default::default(),
            job: Default::default(),
            cli: Default::default(),
        }
    }
}

impl Display for ClusterConf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

/// S3 Object Gateway configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct S3GatewayConf {
    pub listen: String,
    pub region: String,
    pub put_temp_dir: String,
    pub put_memory_buffer_threshold: usize,
    pub put_max_memory_buffer: usize,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub enable_distributed_auth: bool,
    pub credentials_path: Option<String>,
    pub cache_refresh_interval_secs: u64,
    pub get_chunk_size_mb: f32,
    pub web_port: u16,
}

impl Default for S3GatewayConf {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:9900".to_string(),
            region: "us-east-1".to_string(),
            put_temp_dir: "/tmp/curvine-temp".to_string(),
            put_memory_buffer_threshold: 1048576, // 1MB
            put_max_memory_buffer: 16777216,      // 16MB
            access_key: None,
            secret_key: None,
            enable_distributed_auth: false,
            credentials_path: None,
            cache_refresh_interval_secs: 30,
            get_chunk_size_mb: 1.0,
            web_port: 9003,
        }
    }
}

/// NFS Gateway configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NfsGatewayConf {
    /// Listen address (default: 0.0.0.0)
    pub listen_addr: String,

    /// Listen port (default: 2049)
    pub listen_port: u16,

    /// Export path (default: /)
    pub export_path: String,

    /// Cluster generation number for file handle consistency across instances
    /// All NFS Gateway instances must use the same value for multi-instance deployment
    /// If 0, will use current timestamp
    pub cluster_generation: u64,

    /// Default UID when owner cannot be resolved (default: 65534 = nobody)
    pub default_uid: u32,

    /// Default GID when group cannot be resolved (default: 65534 = nogroup)
    pub default_gid: u32,

    /// Maximum cached file handles (default: 10000)
    pub max_handles: usize,

    /// Idle timeout for file handles in seconds (default: 60)
    pub handle_idle_timeout_secs: u64,

    /// Maximum cached path entries (fileid -> path) (default: 100000)
    pub path_cache_size: usize,

    /// Path cache TTL in seconds (default: 300)
    pub path_cache_ttl_secs: u64,

    /// Read-only mode (default: false)
    pub read_only: bool,

    /// Maximum read size per request in bytes (default: 1MB)
    pub max_read_size: u32,

    /// Maximum write size per request in bytes (default: 1MB)
    pub max_write_size: u32,

    /// Web metrics port (default: 9300, 0 to disable)
    pub web_port: u16,

    // ========== I/O Cache Configuration ==========
    /// FileBlocks cache capacity (default: 10000)
    pub file_blocks_cache_size: u64,

    /// FileBlocks cache TTL in seconds (default: 60)
    pub file_blocks_cache_ttl_secs: u64,

    /// Reader pool cache capacity - max number of files with cached readers (default: 1000)
    pub reader_cache_size: u64,

    /// Reader cache TTL in seconds (default: 300)
    pub reader_cache_ttl_secs: u64,

    /// Reader pool size per file - number of parallel readers (default: 32)
    /// Increased from 8 to 32 for better multi-thread performance (2025-12-30)
    /// Benchmark: 8 threads with pool_size=8 caused lock contention (2287 MiB/s)
    /// Expected: pool_size=32 should improve 8-thread performance by 20-30%
    pub reader_pool_size: usize,

    /// Writer cache capacity - max number of files with cached writers (default: 1000)
    /// FUSE-aligned: Writers are globally shared per file for data consistency
    pub writer_cache_size: u64,

    /// Writer cache TTL in seconds (default: 300)
    /// How long to keep Writer in cache after last access
    pub writer_cache_ttl_secs: u64,

    /// Writer idle timeout in seconds before auto-close (default: 30)
    pub writer_idle_timeout_secs: u64,

    /// Writer pool size per file - number of parallel writers (default: 4)
    pub writer_pool_size: usize,

    /// Cache cleanup interval in seconds (default: 10)
    pub cache_cleanup_interval_secs: u64,

    /// FileStatus cache capacity - max number of cached file statuses (default: 10000)
    /// Set to -1 to disable FileStatus caching
    pub file_status_cache_size: i64,

    /// FileStatus cache TTL in seconds (default: 30)
    pub file_status_cache_ttl_secs: u64,

    /// Small file data cache capacity - max number of cached file contents (default: 1000)
    /// Only files <= max_cacheable_file_size will be cached
    /// Set to 0 to disable data caching
    pub file_data_cache_size: u64,

    /// File data cache TTL in seconds (default: 10)
    pub file_data_cache_ttl_secs: u64,

    /// Maximum file size for data caching in bytes (default: 65536 = 64KB)
    /// Files larger than this will not be cached to avoid memory pressure
    pub max_cacheable_file_size: u64,

    // ========== NFSv4 Delegation Configuration ==========
    /// Enable NFSv4 delegations (default: false for maximum performance)
    /// Delegations allow clients to cache file data locally, reducing server load
    /// but adding complexity. Only enable if your workload benefits from it.
    pub delegation_enabled: bool,

    /// Delegation recall timeout in seconds (default: 30)
    /// Time to wait for client to return delegation before revoking it
    pub delegation_recall_timeout_secs: u64,

    /// Maximum number of active delegations (default: 1000)
    /// Limits memory usage and prevents delegation storms
    pub delegation_max_count: usize,

    // ========== UNSTABLE Write Optimization ==========
    /// Enable UNSTABLE write optimization (default: true)
    /// UNSTABLE writes return immediately without fsync, data is flushed on COMMIT/CLOSE
    /// This significantly improves small file write performance (2-4x)
    /// Complies with NFS RFC 5661 UNSTABLE write semantics
    pub enable_unstable_write: bool,

    // ========== Small File Async Flush Optimization ==========
    /// Enable async flush optimization for small files (default: true)
    /// Small files skip flush on WRITE and delay flush to CLOSE (async)
    /// This dramatically improves small file write performance (40-80x)
    /// Only applies to files matching small file criteria (max_writes and max_size)
    pub enable_small_file_async_flush: bool,

    /// Maximum write count for small file detection (default: 20)
    /// Files with <= this many writes are considered small files
    pub small_file_max_writes: u32,

    /// Maximum file size for small file detection in bytes (default: 10MB)
    /// Files with <= this size are considered small files
    pub small_file_max_size: u64,
}

impl Default for NfsGatewayConf {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0".to_string(),
            listen_port: 2049,
            export_path: "/".to_string(),
            cluster_generation: 0, // 0 means use timestamp
            default_uid: 65534,    // nobody
            default_gid: 65534,    // nogroup
            max_handles: 10000,
            handle_idle_timeout_secs: 60,
            path_cache_size: 100000,
            path_cache_ttl_secs: 300,
            read_only: false,
            max_read_size: 1024 * 1024,  // 1MB
            max_write_size: 1024 * 1024, // 1MB
            web_port: 9300,
            // I/O Cache defaults
            file_blocks_cache_size: 10000,
            file_blocks_cache_ttl_secs: 60,
            reader_cache_size: 1000,
            reader_cache_ttl_secs: 300,
            reader_pool_size: 32, // Increased from 8 for better multi-thread performance
            writer_cache_size: 1000,
            writer_cache_ttl_secs: 300,
            writer_idle_timeout_secs: 30,
            writer_pool_size: 4,
            cache_cleanup_interval_secs: 10,
            file_status_cache_size: 10000i64,
            file_status_cache_ttl_secs: 30,
            // Small file data cache defaults
            file_data_cache_size: 1000,     // 1000 files max
            file_data_cache_ttl_secs: 10,   // 10 seconds TTL
            max_cacheable_file_size: 65536, // 64KB max
            // NFSv4 Delegation defaults (enabled for better read performance)
            delegation_enabled: true,
            delegation_recall_timeout_secs: 30,
            delegation_max_count: 1000,
            // UNSTABLE Write optimization (enabled for better write performance)
            enable_unstable_write: true,
            // Small file async flush optimization
            enable_small_file_async_flush: true,
            small_file_max_writes: 20,
            small_file_max_size: 10 * 1024 * 1024, // 10MB
        }
    }
}

impl NfsGatewayConf {
    /// Get handle idle timeout as Duration
    #[inline]
    pub fn handle_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.handle_idle_timeout_secs)
    }

    /// Get path cache TTL as Duration
    #[inline]
    pub fn path_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.path_cache_ttl_secs)
    }

    /// Get effective cluster generation
    ///
    /// Returns the configured cluster_generation value. If not configured (0),
    /// derives a stable value from cluster_id hash to ensure file handles
    /// remain valid across NFS Gateway restarts.
    ///
    /// For multi-instance deployments, all instances with the same cluster_id
    /// will automatically have consistent file handle generation.
    #[inline]
    pub fn effective_cluster_generation(&self, cluster_id: &str) -> u64 {
        if self.cluster_generation == 0 {
            // Derive stable generation from cluster_id hash
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            cluster_id.hash(&mut hasher);
            // Ensure non-zero result
            hasher.finish().max(1)
        } else {
            self.cluster_generation
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.listen_port == 0 {
            return Err("Listen port cannot be 0".to_string());
        }
        if self.max_handles == 0 {
            return Err("Max handles cannot be 0".to_string());
        }
        if self.path_cache_size == 0 {
            return Err("Path cache size cannot be 0".to_string());
        }
        if self.max_read_size == 0 {
            return Err("Max read size cannot be 0".to_string());
        }
        if self.max_write_size == 0 {
            return Err("Max write size cannot be 0".to_string());
        }
        Ok(())
    }
}
