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

//! NFS Gateway Server
//!
//! Provides the main entry point for starting the NFS Gateway service.
//! This is a thin wrapper around nfsserve's NFSTcpListener that adds
//! Curvine-specific configuration and filesystem integration.
//!
//! Supports both NFSv3 and NFSv4.1 protocols.

use crate::gateway::curvine_nfs_fs::CurvineNfsFileSystem;
use crate::nfs4::state::{
    ClientManager, LockManager, OpenManager, PersistenceConfig, StatePersistenceManager,
    StateSaverTask,
};
use crate::nfs4::{CompoundHandler, Nfs4FileSystem, SessionManager};
use crate::server::tcp::{NFSTcp, NFSTcpListener};
use curvine_common::conf::{ClusterConf, NfsGatewayConf};
use curvine_common::error::FsError;
use curvine_common::executor::ScheduledExecutor;
use orpc::runtime::{RpcRuntime, Runtime};
use std::sync::Arc;
use tracing::{error, info};

/// NFS Gateway Server
///
/// Wraps nfsserve's NFSTcpListener with Curvine-specific functionality:
/// - Creates CurvineNfsFileSystem from ClusterConf
/// - Provides NfsGatewayConf integration
/// - Supports NFSv4.1 with CompoundHandler
/// - Adds background task spawning helper
pub struct NfsGatewayServer {
    listener: NFSTcpListener<CurvineNfsFileSystem>,
    config: NfsGatewayConf,
}

impl NfsGatewayServer {
    /// Create a new NFS Gateway Server with NFSv4.1 support
    ///
    /// # Arguments
    /// * `cluster_conf` - Curvine cluster configuration
    /// * `gateway_config` - NFS Gateway specific configuration
    /// * `runtime` - Tokio runtime for async operations
    pub async fn new(
        cluster_conf: ClusterConf,
        gateway_config: NfsGatewayConf,
        runtime: Arc<Runtime>,
    ) -> Result<Self, FsError> {
        // Create the Curvine NFS filesystem (for NFSv3)
        let fs = CurvineNfsFileSystem::new(
            cluster_conf.clone(),
            gateway_config.clone(),
            runtime.clone(),
        )?;

        // Bind to the configured address
        let bind_addr = format!(
            "{}:{}",
            gateway_config.listen_addr, gateway_config.listen_port
        );
        info!("═══════════════════════════════════════════════════════════");
        info!("  Starting NFS Gateway");
        info!("═══════════════════════════════════════════════════════════");
        info!("  Listen address: {}", bind_addr);

        let mut listener = NFSTcpListener::bind(&bind_addr, fs).await.map_err(|e| {
            FsError::io(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!("Failed to bind NFS Gateway: {e}"),
            ))
        })?;

        // Initialize NFSv4.1 handler
        info!("  Initializing NFSv4.1 handler...");
        let nfs4_handler =
            Self::create_nfs4_handler(cluster_conf, gateway_config.clone(), runtime).await?;
        listener.with_nfs4_handler(nfs4_handler);

        info!("  ✓ NFSv4.1 support enabled");

        Ok(Self {
            listener,
            config: gateway_config,
        })
    }

    /// Create NFSv4.1 CompoundHandler
    async fn create_nfs4_handler(
        cluster_conf: ClusterConf,
        gateway_config: NfsGatewayConf,
        runtime: Arc<Runtime>,
    ) -> Result<Arc<CompoundHandler>, FsError> {
        // Create NFSv4.1 file system
        let nfs4_fs = Arc::new(Nfs4FileSystem::new(
            cluster_conf,
            gateway_config.clone(),
            runtime.clone(),
        )?);

        // Create state persistence manager with default config (30s interval)
        let persistence_config = PersistenceConfig::default();
        let persistence = Arc::new(StatePersistenceManager::new(
            nfs4_fs.clone(),
            persistence_config.clone(),
        ));

        // Initialize state directories
        info!("  Initializing state persistence...");
        if let Err(e) = persistence.initialize().await {
            tracing::warn!("  ⚠ Failed to initialize state persistence: {:?}", e);
        } else {
            info!("  ✓ State persistence initialized");
        }

        // Create state managers
        info!("  Creating state managers...");
        let sessions = Arc::new(SessionManager::new());
        let clients = Arc::new(ClientManager::new());
        let opens = Arc::new(OpenManager::new());
        let locks = Arc::new(LockManager::new());
        info!("  ✓ State managers created");

        // Load persisted state (if any)
        info!("  Loading persisted state...");
        if let Err(e) =
            Self::load_persisted_state(&persistence, &clients, &opens, &locks, &nfs4_fs).await
        {
            tracing::warn!("  ⚠ Failed to load persisted state: {:?}", e);
        } else {
            info!("  ✓ Persisted state loaded");
        }

        // Create compound handler
        let handler = Arc::new(CompoundHandler::new(
            sessions,
            clients.clone(),
            opens.clone(),
            locks.clone(),
            nfs4_fs,
            persistence.clone(),
            &gateway_config, // Pass NFS config for delegation settings
        ));

        // Start periodic state saver (following NFS-Ganesha design)
        // Saves state every 30 seconds (configurable)
        if persistence.is_enabled() {
            info!("  Starting state saver...");
            let saver = StateSaverTask::new(
                persistence.clone(),
                clients.clone(),
                opens.clone(),
                locks.clone(),
                runtime.clone(),
            );
            let save_interval_ms = persistence.save_interval_ms();
            let scheduler = ScheduledExecutor::new("state-saver", save_interval_ms);
            if let Err(e) = scheduler.start(saver) {
                tracing::error!("  ✗ Failed to start state saver: {}", e);
            } else {
                info!(
                    "  ✓ State saver started (interval: {}s, instance: {})",
                    save_interval_ms / 1000,
                    persistence.instance_id()
                );
            }
        }

        // Enter grace period on server startup (90 seconds default)
        // This allows clients to reclaim their state after server restart
        info!("  Entering grace period...");
        if let Err(e) = handler.grace.enter_grace_period() {
            tracing::warn!("  ⚠ Failed to enter grace period immediately: errno={}", e);
            tracing::info!("  Grace period will be entered after outstanding operations complete");
        } else {
            tracing::info!("  ✓ Grace period entered (90 seconds)");
        }

        Ok(handler)
    }

    /// Load persisted state from filesystem and restore to state managers
    ///
    /// This function loads persisted state and restores it to ClientManager and OpenManager.
    /// Restored states are marked as unconfirmed and will be confirmed when clients reclaim
    /// them during grace period using CLAIM_PREVIOUS.
    async fn load_persisted_state(
        persistence: &Arc<StatePersistenceManager>,
        clients: &Arc<ClientManager>,
        opens: &Arc<OpenManager>,
        _locks: &Arc<LockManager>,
        _fs: &Arc<Nfs4FileSystem>,
    ) -> Result<(), FsError> {
        use curvine_common::fs::Path as FsPath;

        // Load recovery metadata
        info!("    [1/4] Loading recovery metadata...");
        if let Ok(Some(meta)) = persistence.load_recovery_metadata().await {
            info!(
                "         ✓ Found recovery metadata (instance: {}, shutdown: {})",
                meta.server_instance_id, meta.last_shutdown_time
            );
        } else {
            info!("         ℹ No recovery metadata found (fresh start)");
        }

        // Load and restore clients
        info!("    [2/4] Loading and restoring client states...");
        let persisted_clients = persistence.load_clients().await.map_err(|e| {
            FsError::io(std::io::Error::other(format!(
                "Failed to load clients: {e:?}"
            )))
        })?;

        let mut restored_clients = 0;
        for persisted_client in &persisted_clients {
            // Restore client state (NFS-Ganesha aligned: restore during grace period)
            if let Err(e) = clients.restore_persisted_client(persisted_client) {
                tracing::warn!(
                    "Failed to restore client {}: {:?}",
                    persisted_client.clientid,
                    e
                );
            } else {
                restored_clients += 1;
            }
        }
        info!(
            "         ✓ Restored {}/{} client state(s)",
            restored_clients,
            persisted_clients.len()
        );

        // Load and restore opens
        info!("    [3/4] Loading and restoring open states...");
        let persisted_opens = persistence.load_opens().await.map_err(|e| {
            FsError::io(std::io::Error::other(format!(
                "Failed to load opens: {e:?}"
            )))
        })?;

        let mut restored_opens = 0;
        for persisted_open in &persisted_opens {
            // Convert path string to Path (curvine_common::fs::Path)
            let path = FsPath::new(&persisted_open.path).map_err(|e| {
                FsError::io(std::io::Error::other(format!(
                    "Invalid path in persisted open: {}: {:?}",
                    persisted_open.path, e
                )))
            })?;

            // Restore open state (NFS-Ganesha aligned: restore during grace period)
            if let Err(e) = opens.restore_persisted_state(
                persisted_open.stateid,
                persisted_open.clientid,
                persisted_open.fileid,
                path,
                persisted_open.share_access,
                persisted_open.share_deny,
                persisted_open.owner_val.clone(),
            ) {
                tracing::warn!(
                    "Failed to restore open state {:02x?}: {:?}",
                    &persisted_open.stateid[..4],
                    e
                );
            } else {
                restored_opens += 1;
            }
        }
        info!(
            "         ✓ Restored {}/{} open state(s)",
            restored_opens,
            persisted_opens.len()
        );

        // Load locks (not restoring yet, will be restored on reclaim)
        info!("    [4/4] Loading lock states...");
        let persisted_locks = persistence.load_locks().await.map_err(|e| {
            FsError::io(std::io::Error::other(format!(
                "Failed to load locks: {e:?}"
            )))
        })?;
        info!("         ✓ Loaded {} lock state(s)", persisted_locks.len());

        // Summary
        let total = persisted_clients.len() + persisted_opens.len() + persisted_locks.len();
        if total > 0 {
            info!("    ────────────────────────────────────────────────────────");
            info!(
                "    State summary: {} client(s), {} open(s), {} lock(s)",
                persisted_clients.len(),
                persisted_opens.len(),
                persisted_locks.len()
            );
            info!(
                "    Restored: {} client(s), {} open(s)",
                restored_clients, restored_opens
            );
            info!("    Note: Clients will reclaim state during grace period");
        } else {
            info!("    ℹ No persisted state found (fresh start)");
        }

        Ok(())
    }

    /// Get the actual listening port
    #[inline]
    pub fn listen_port(&self) -> u16 {
        self.listener.get_listen_port()
    }

    /// Get the actual listening IP
    #[inline]
    pub fn listen_ip(&self) -> std::net::IpAddr {
        self.listener.get_listen_ip()
    }

    /// Start the NFS Gateway server (runs forever)
    ///
    /// This method blocks and handles incoming NFS connections.
    /// Should be spawned in a separate task.
    pub async fn start(self) -> Result<(), FsError> {
        let listen_port = self.listen_port();

        info!("═══════════════════════════════════════════════════════════");
        info!("  NFS Gateway Ready");
        info!("═══════════════════════════════════════════════════════════");
        info!("  Listen address: {}:{}", self.listen_ip(), listen_port);
        info!("  Export path:     {}", self.config.export_path);
        info!("  Read-only mode:  {}", self.config.read_only);
        info!("  Protocols:       NFSv3, NFSv4.1");
        info!("═══════════════════════════════════════════════════════════");

        self.listener
            .handle_forever()
            .await
            .map_err(|e| FsError::io(std::io::Error::other(format!("NFS Gateway error: {e}"))))
    }

    /// Start the NFS Gateway in a background task
    pub fn start_background(self, runtime: Arc<Runtime>) -> tokio::task::JoinHandle<()> {
        runtime.spawn(async move {
            if let Err(e) = self.start().await {
                error!("NFS Gateway stopped with error: {}", e);
            }
        })
    }
}
