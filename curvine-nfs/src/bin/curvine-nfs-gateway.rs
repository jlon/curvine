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

use clap::{Parser, Subcommand};
use curvine_common::conf::{ClusterConf, NfsGatewayConf};
use curvine_nfs::gateway::NfsGatewayServer;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::CommonResult;
use std::sync::Arc;

#[derive(Debug, Parser, Clone)]
#[command(name = "curvine-nfs-gateway")]
#[command(about = "Curvine NFS Gateway - Export Curvine filesystem via NFSv3")]
pub struct NfsGatewayArgs {
    #[arg(
        short,
        long,
        help = "Configuration file path",
        default_value = "etc/curvine-cluster.toml"
    )]
    pub conf: String,

    #[arg(long, help = "Listen address (e.g., 0.0.0.0)")]
    pub listen_addr: Option<String>,

    #[arg(long, help = "Listen port (default: 2049)")]
    pub listen_port: Option<u16>,

    #[arg(long, help = "Export path (default: /)")]
    pub export_path: Option<String>,

    #[arg(long, help = "Enable read-only mode")]
    pub read_only: bool,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Debug, Subcommand, Clone)]
pub enum Commands {
    Serve,
    ShowConfig,
}

fn main() -> CommonResult<()> {
    let args = NfsGatewayArgs::parse();

    let conf = load_cluster_conf(&args.conf)?;

    orpc::common::Logger::init(conf.master.log.clone());

    match &args.command {
        Some(Commands::ShowConfig) => {
            show_config(&conf, &args);
            Ok(())
        }
        Some(Commands::Serve) | None => serve_gateway(args, conf),
    }
}

/// Load cluster configuration from file
fn load_cluster_conf(path: &str) -> CommonResult<ClusterConf> {
    match ClusterConf::from(path) {
        Ok(c) => {
            tracing::info!("Loaded configuration from {}", path);
            Ok(c)
        }
        Err(e) => {
            println!(
                "Warning: Failed to load config file '{path}': {e}. Using default configuration"
            );
            Ok(ClusterConf::default())
        }
    }
}

/// Build NFS Gateway config from cluster conf and CLI args
fn build_nfs_config(conf: &ClusterConf, args: &NfsGatewayArgs) -> NfsGatewayConf {
    let mut nfs_config = conf.nfs_gateway.clone();

    // CLI args override config file
    if let Some(addr) = &args.listen_addr {
        nfs_config.listen_addr = addr.clone();
    }
    if let Some(port) = args.listen_port {
        nfs_config.listen_port = port;
    }
    if let Some(path) = &args.export_path {
        nfs_config.export_path = path.clone();
    }
    if args.read_only {
        nfs_config.read_only = true;
    }

    nfs_config
}

/// Show current configuration
fn show_config(conf: &ClusterConf, args: &NfsGatewayArgs) {
    let nfs_config = build_nfs_config(conf, args);
    println!("NFS Gateway Configuration:");
    println!(
        "  Listen: {}:{}",
        nfs_config.listen_addr, nfs_config.listen_port
    );
    println!("  Export Path: {}", nfs_config.export_path);
    println!("  Read-Only: {}", nfs_config.read_only);
    println!(
        "  Cluster Generation: {}",
        nfs_config.effective_cluster_generation(&conf.cluster_id)
    );
    println!(
        "  Default UID/GID: {}/{}",
        nfs_config.default_uid, nfs_config.default_gid
    );
    println!("  Max Handles: {}", nfs_config.max_handles);
    println!("  Path Cache Size: {}", nfs_config.path_cache_size);
    println!("  Max Read Size: {} bytes", nfs_config.max_read_size);
    println!("  Max Write Size: {} bytes", nfs_config.max_write_size);
    println!("  Web Port: {}", nfs_config.web_port);
}

/// Start the NFS Gateway server
fn serve_gateway(args: NfsGatewayArgs, conf: ClusterConf) -> CommonResult<()> {
    let nfs_config = build_nfs_config(&conf, &args);

    // Validate configuration
    if let Err(e) = nfs_config.validate() {
        return Err(format!("Invalid NFS Gateway configuration: {e}").into());
    }

    tracing::info!(
        "NFS Gateway Configuration: Listen={}:{}, Export={}, ReadOnly={}",
        nfs_config.listen_addr,
        nfs_config.listen_port,
        nfs_config.export_path,
        nfs_config.read_only
    );

    // Create async runtime
    let rt = Arc::new(Runtime::new(
        "curvine-nfs-gateway",
        conf.client.io_threads,
        conf.client.worker_threads,
    ));

    // Run the server
    rt.block_on(run_server(conf, nfs_config, rt.clone()))
}

/// Run the NFS Gateway server (async)
async fn run_server(
    conf: ClusterConf,
    nfs_config: NfsGatewayConf,
    rt: Arc<Runtime>,
) -> CommonResult<()> {
    let server = NfsGatewayServer::new(conf, nfs_config, rt)
        .await
        .map_err(|e| format!("Failed to create NFS Gateway: {e}"))?;

    server
        .start()
        .await
        .map_err(|e| format!("Server error: {e}"))?;

    Ok(())
}
