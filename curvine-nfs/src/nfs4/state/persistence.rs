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

//! NFSv4 State Persistence
//!
//! Persists NFSv4 state (clients, opens, locks) to Curvine filesystem
//! for recovery after server restart.
//!
//! # Design
//!
//! State is stored in special directory: `/.nfs4_state/`
//! - clients/: Client records
//! - opens/: Open state records
//! - locks/: Lock state records
//! - recovery.meta: Recovery metadata
//!
//! # Recovery Process
//!
//! 1. Server starts, enters grace period
//! 2. Load persisted state from `/.nfs4_state/`
//! 3. Clients can reclaim their state during grace period
//! 4. After grace period, clean up unclaimed state

use crate::nfs4::error::{Nfs4Result, Nfs4Status};
use crate::nfs4::fs::Nfs4FileSystem;
use crate::nfs4::state::{ClientManager, LockManager, OpenManager};
use curvine_common::fs::FileSystem;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

// State directory path in Curvine (base directory)
const STATE_BASE_DIR: &str = "/.nfs4_state";

// Persistence configuration
const DEFAULT_SAVE_INTERVAL_MS: u64 = 30000; // 30 seconds (periodic save)

// ============================================================================
// Persisted State Structures
// ============================================================================

/// Persisted client record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedClient {
    pub clientid: u64,
    pub client_owner: Vec<u8>,
    pub verifier: [u8; 8],
    pub confirmed: bool,
    pub lease_expiry: u64, // Unix timestamp
}

/// Persisted open state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedOpen {
    pub stateid: [u8; 12], // Stateid4.other is 12 bytes
    pub clientid: u64,
    pub fileid: u64,
    pub path: String,
    pub share_access: u32,
    pub share_deny: u32,
}

/// Persisted lock state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedLock {
    pub stateid: [u8; 12], // Stateid4.other is 12 bytes
    pub clientid: u64,
    pub fileid: u64,
    pub lock_type: u32, // READ=1, WRITE=2
    pub offset: u64,
    pub length: u64,
    pub owner: Vec<u8>,
}

/// Recovery metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryMetadata {
    pub server_instance_id: u64,
    pub last_shutdown_time: u64,
    pub grace_period_secs: u64,
    pub recovery_epoch: u64, // Incremented on each save for consistency checking
}

// ============================================================================
// Persistence Configuration
// ============================================================================

/// Persistence configuration
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Enable state persistence (periodic save)
    pub enabled: bool,
    /// Save interval in milliseconds (periodic save)
    pub save_interval_ms: u64,
    /// Instance ID for multi-instance deployment (None = auto-generate from hostname)
    pub instance_id: Option<String>,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            save_interval_ms: DEFAULT_SAVE_INTERVAL_MS,
            instance_id: None, // Auto-generate from hostname
        }
    }
}

// ============================================================================
// State Persistence Manager
// ============================================================================

/// State Persistence Manager
///
/// Manages persistence of NFSv4 state to Curvine filesystem.
///
/// # Design Principles (Performance First!)
///
/// 1. **Periodic Save**: State saved every 30 seconds (configurable) in background
/// 2. **Async Non-Blocking**: Save operations don't block NFS operations
/// 3. **Load on Startup**: State loaded once during server initialization
/// 4. **Grace Period for Recovery**: Clients reconnect during grace period
///
/// # Multi-Instance Deployment (Following NFS-Ganesha Design)
///
/// Each Gateway instance has its own state directory (like NFS-Ganesha's node{id}):
/// - `/.nfs4_state/{instance_id}/clients/`
/// - `/.nfs4_state/{instance_id}/opens/`
/// - `/.nfs4_state/{instance_id}/locks/`
/// - `/.nfs4_state/{instance_id}/recovery.meta`
///
/// Instance ID generation (following NFS-Ganesha logic):
/// - If configured: use config value
/// - Otherwise: use hostname (like NFS-Ganesha non-clustered mode)
///
/// This design ensures:
/// - No state conflicts between instances
/// - Each instance can restart independently
/// - Clients reconnect to the same instance (via load balancer session affinity)
pub struct StatePersistenceManager {
    fs: Arc<Nfs4FileSystem>,
    config: PersistenceConfig,
    recovery_epoch: std::sync::atomic::AtomicU64,
    /// Instance-specific directory paths
    instance_id: String,
    state_dir: String,
    clients_dir: String,
    opens_dir: String,
    locks_dir: String,
    recovery_meta: String,
}

impl StatePersistenceManager {
    /// Create a new persistence manager
    pub fn new(fs: Arc<Nfs4FileSystem>, config: PersistenceConfig) -> Self {
        // Generate instance ID (following NFS-Ganesha design)
        let instance_id = config.instance_id.clone().unwrap_or_else(|| {
            // Priority 1: Use hostname (like NFS-Ganesha non-clustered mode)
            // Priority 2: Use fixed "default" for single-instance deployments
            // Priority 3: Use process ID (only for multi-instance testing)

            // Try to get hostname
            if let Ok(hostname) = hostname::get() {
                if let Some(hostname_str) = hostname.to_str() {
                    if !hostname_str.is_empty() {
                        return hostname_str.to_string();
                    }
                }
            }

            // Check if multi-instance mode is enabled
            if std::env::var("NFS_MULTI_INSTANCE").is_ok() {
                format!("node{}", std::process::id())
            } else {
                // Use fixed "default" for single-instance deployments
                // This ensures state persists across restarts
                "default".to_string()
            }
        });

        // Build instance-specific paths (like NFS-Ganesha: recov_root/recov_dir/node{id})
        let state_dir = format!("{STATE_BASE_DIR}/{instance_id}");
        let clients_dir = format!("{state_dir}/clients");
        let opens_dir = format!("{state_dir}/opens");
        let locks_dir = format!("{state_dir}/locks");
        let recovery_meta = format!("{state_dir}/recovery.meta");

        Self {
            fs,
            config,
            recovery_epoch: std::sync::atomic::AtomicU64::new(1),
            instance_id,
            state_dir,
            clients_dir,
            opens_dir,
            locks_dir,
            recovery_meta,
        }
    }

    /// Create with default configuration
    pub fn with_default_config(fs: Arc<Nfs4FileSystem>) -> Self {
        Self::new(fs, PersistenceConfig::default())
    }

    /// Get instance ID
    #[inline]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Check if persistence is enabled
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get save interval in milliseconds
    #[inline]
    pub fn save_interval_ms(&self) -> u64 {
        self.config.save_interval_ms
    }

    /// Initialize state directories
    ///
    /// Creates the instance-specific state directory structure if it doesn't exist.
    pub async fn initialize(&self) -> Nfs4Result<()> {
        if !self.config.enabled {
            info!("State persistence disabled");
            return Ok(());
        }

        info!(
            "Initializing NFSv4 state persistence for instance '{}'",
            self.instance_id
        );

        // Create state directories (instance-specific)
        self.create_state_dir(&self.state_dir).await?;
        self.create_state_dir(&self.clients_dir).await?;
        self.create_state_dir(&self.opens_dir).await?;
        self.create_state_dir(&self.locks_dir).await?;

        info!(
            "NFSv4 state persistence initialized at {} (save interval: {}s)",
            self.state_dir,
            self.config.save_interval_ms / 1000
        );
        Ok(())
    }

    /// Create state directory if it doesn't exist (recursive)
    async fn create_state_dir(&self, path: &str) -> Nfs4Result<()> {
        use curvine_common::fs::Path as FsPath;

        let fs_path = FsPath::new(path).map_err(|_| {
            error!("Invalid directory path: {}", path);
            Nfs4Status::Inval
        })?;

        // Use mkdir with create_parent=true to recursively create directory structure
        self.fs.ufs().mkdir(&fs_path, true).await.map_err(|e| {
            error!("Failed to create state directory {}: {:?}", path, e);
            Nfs4Status::Serverfault
        })?;

        info!("Created state directory: {}", path);
        Ok(())
    }

    /// Save client state (async, non-blocking)
    ///
    /// NOTE: This method exists for API compatibility but does NOT save immediately.
    /// State is only saved on graceful shutdown to avoid performance impact.
    pub async fn save_client(&self, _client: &PersistedClient) -> Nfs4Result<()> {
        // No-op: State only saved on shutdown for maximum performance
        Ok(())
    }

    /// Load all client states
    pub async fn load_clients(&self) -> Nfs4Result<Vec<PersistedClient>> {
        if !self.config.enabled {
            return Ok(Vec::new());
        }

        info!("Loading persisted client states from {}", self.clients_dir);

        let files = match self.list_dir(&self.clients_dir).await {
            Ok(f) => f,
            Err(_) => {
                debug!("No client state directory found");
                return Ok(Vec::new());
            }
        };

        let mut clients = Vec::new();
        for filename in files {
            if !filename.ends_with(".json") {
                continue;
            }

            let path = format!("{}/{}", self.clients_dir, filename);
            match self.read_state_file(&path).await {
                Ok(data) => match serde_json::from_slice::<PersistedClient>(&data) {
                    Ok(client) => {
                        debug!("Loaded client state: {}", client.clientid);
                        clients.push(client);
                    }
                    Err(e) => {
                        warn!("Failed to deserialize client from {}: {}", path, e);
                    }
                },
                Err(e) => {
                    warn!("Failed to read client state file {}: {:?}", path, e);
                }
            }
        }

        info!("Loaded {} client states", clients.len());
        Ok(clients)
    }

    /// Save open state (async, non-blocking)
    ///
    /// NOTE: This method exists for API compatibility but does NOT save immediately.
    /// State is only saved on graceful shutdown to avoid performance impact.
    pub async fn save_open(&self, _open: &PersistedOpen) -> Nfs4Result<()> {
        // No-op: State only saved on shutdown for maximum performance
        Ok(())
    }

    /// Load all open states
    pub async fn load_opens(&self) -> Nfs4Result<Vec<PersistedOpen>> {
        if !self.config.enabled {
            return Ok(Vec::new());
        }

        info!("Loading persisted open states from {}", self.opens_dir);

        let files = match self.list_dir(&self.opens_dir).await {
            Ok(f) => f,
            Err(_) => {
                debug!("No open state directory found");
                return Ok(Vec::new());
            }
        };

        let mut opens = Vec::new();
        for filename in files {
            if !filename.ends_with(".json") {
                continue;
            }

            let path = format!("{}/{}", self.opens_dir, filename);
            match self.read_state_file(&path).await {
                Ok(data) => match serde_json::from_slice::<PersistedOpen>(&data) {
                    Ok(open) => {
                        debug!("Loaded open state: {:02x?}", &open.stateid[..4]);
                        opens.push(open);
                    }
                    Err(e) => {
                        warn!("Failed to deserialize open from {}: {}", path, e);
                    }
                },
                Err(e) => {
                    warn!("Failed to read open state file {}: {:?}", path, e);
                }
            }
        }

        info!("Loaded {} open states", opens.len());
        Ok(opens)
    }

    /// Save lock state (async, non-blocking)
    ///
    /// NOTE: This method exists for API compatibility but does NOT save immediately.
    /// State is only saved on graceful shutdown to avoid performance impact.
    pub async fn save_lock(&self, _lock: &PersistedLock) -> Nfs4Result<()> {
        // No-op: State only saved on shutdown for maximum performance
        Ok(())
    }

    /// Load all lock states
    pub async fn load_locks(&self) -> Nfs4Result<Vec<PersistedLock>> {
        if !self.config.enabled {
            return Ok(Vec::new());
        }

        info!("Loading persisted lock states from {}", self.locks_dir);

        let files = match self.list_dir(&self.locks_dir).await {
            Ok(f) => f,
            Err(_) => {
                debug!("No lock state directory found");
                return Ok(Vec::new());
            }
        };

        let mut locks = Vec::new();
        for filename in files {
            if !filename.ends_with(".json") {
                continue;
            }

            let path = format!("{}/{}", self.locks_dir, filename);
            match self.read_state_file(&path).await {
                Ok(data) => match serde_json::from_slice::<PersistedLock>(&data) {
                    Ok(lock) => {
                        debug!("Loaded lock state: {:02x?}", &lock.stateid[..4]);
                        locks.push(lock);
                    }
                    Err(e) => {
                        warn!("Failed to deserialize lock from {}: {}", path, e);
                    }
                },
                Err(e) => {
                    warn!("Failed to read lock state file {}: {:?}", path, e);
                }
            }
        }

        info!("Loaded {} lock states", locks.len());
        Ok(locks)
    }

    /// Save recovery metadata (async, non-blocking)
    ///
    /// NOTE: This method exists for API compatibility but does NOT save immediately.
    /// State is only saved on graceful shutdown to avoid performance impact.
    pub async fn save_recovery_metadata(&self, _meta: &RecoveryMetadata) -> Nfs4Result<()> {
        // No-op: State only saved on shutdown for maximum performance
        Ok(())
    }

    /// Load recovery metadata
    pub async fn load_recovery_metadata(&self) -> Nfs4Result<Option<RecoveryMetadata>> {
        if !self.config.enabled {
            return Ok(None);
        }

        info!("Loading recovery metadata from {}", self.recovery_meta);

        match self.read_state_file(&self.recovery_meta).await {
            Ok(data) => {
                match serde_json::from_slice::<RecoveryMetadata>(&data) {
                    Ok(meta) => {
                        info!(
                            "Loaded recovery metadata: server_instance={}, epoch={}",
                            meta.server_instance_id, meta.recovery_epoch
                        );
                        // Update our epoch to be higher than loaded one
                        self.recovery_epoch.store(
                            meta.recovery_epoch + 1,
                            std::sync::atomic::Ordering::Release,
                        );
                        Ok(Some(meta))
                    }
                    Err(e) => {
                        warn!("Failed to deserialize recovery metadata: {}", e);
                        Ok(None)
                    }
                }
            }
            Err(_) => {
                debug!("No recovery metadata found");
                Ok(None)
            }
        }
    }

    /// Save complete state snapshot (periodic save)
    ///
    /// This method is called periodically by the StateSaverTask.
    /// It saves all current state to disk asynchronously.
    pub async fn save_snapshot(
        &self,
        clients: &Arc<ClientManager>,
        opens: &Arc<OpenManager>,
        locks: &Arc<LockManager>,
    ) -> Nfs4Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let start = std::time::Instant::now();

        debug!("Saving NFSv4 state snapshot...");

        // Increment recovery epoch
        let epoch = self
            .recovery_epoch
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        // Save recovery metadata
        let meta = RecoveryMetadata {
            server_instance_id: std::process::id() as u64,
            last_shutdown_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            grace_period_secs: 90,
            recovery_epoch: epoch,
        };

        let data = serde_json::to_vec(&meta).map_err(|e| {
            error!("Failed to serialize recovery metadata: {}", e);
            Nfs4Status::Serverfault
        })?;
        self.write_state_file(&self.recovery_meta, &data).await?;

        // Save clients
        let client_states = clients.export_clients();
        let client_count = client_states.len();
        for (clientid, client) in client_states {
            let persisted = PersistedClient {
                clientid,
                client_owner: client.owner.co_ownerid.clone(),
                verifier: client.owner.co_verifier,
                confirmed: client.is_confirmed(),
                lease_expiry: client.last_renew.read().unwrap().elapsed().as_secs(),
            };

            let data = serde_json::to_vec(&persisted).map_err(|e| {
                error!("Failed to serialize client {}: {}", clientid, e);
                Nfs4Status::Serverfault
            })?;

            let path = format!("{}/{}.json", self.clients_dir, clientid);
            self.write_state_file(&path, &data).await?;
        }

        // Save opens
        let open_states = opens.export_opens();
        let open_count = open_states.len();
        for open in open_states {
            let persisted = PersistedOpen {
                stateid: open.stateid.other,
                clientid: open.clientid,
                fileid: open.fileid,
                path: open.path.path().to_string(),
                share_access: open.get_access(),
                share_deny: open.get_deny(),
            };

            let data = serde_json::to_vec(&persisted).map_err(|e| {
                error!(
                    "Failed to serialize open {:02x?}: {}",
                    &open.stateid.other[..4],
                    e
                );
                Nfs4Status::Serverfault
            })?;

            let stateid_hex = hex::encode(open.stateid.other);
            let path = format!("{}/{stateid_hex}.json", self.opens_dir);
            self.write_state_file(&path, &data).await?;
        }

        // Save locks (only active lock states)
        let lock_states = locks.export_locks();
        let mut lock_count = 0;
        for state in lock_states {
            // Export each lock entry in this state
            let entries = state.lock_entries.read().unwrap();
            for entry in entries.iter() {
                let persisted = PersistedLock {
                    stateid: state.stateid.other,
                    clientid: state.owner.clientid,
                    fileid: entry.fileid,
                    lock_type: entry.lock_type as u32,
                    offset: entry.get_offset(),
                    length: entry.get_length(),
                    owner: state.owner.owner.clone(),
                };

                let data = serde_json::to_vec(&persisted).map_err(|e| {
                    error!(
                        "Failed to serialize lock {:02x?}: {}",
                        &state.stateid.other[..4],
                        e
                    );
                    Nfs4Status::Serverfault
                })?;

                // Use stateid + entry index for unique filename
                let stateid_hex = hex::encode(state.stateid.other);
                let entry_idx = lock_count;
                let path = format!("{}/{stateid_hex}_{entry_idx}.json", self.locks_dir);
                self.write_state_file(&path, &data).await?;
                lock_count += 1;
            }
        }

        let elapsed = start.elapsed();
        debug!(
            "State snapshot saved: {} clients, {} opens, {} locks (epoch: {}, took: {:?})",
            client_count, open_count, lock_count, epoch, elapsed
        );

        Ok(())
    }

    /// Get current recovery epoch
    #[inline]
    pub fn current_epoch(&self) -> u64 {
        self.recovery_epoch
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Clean up all persisted state
    async fn read_state_file(&self, path: &str) -> Nfs4Result<Vec<u8>> {
        use curvine_common::fs::Path as FsPath;

        let fs_path = FsPath::new(path).map_err(|_| {
            error!("Invalid path: {}", path);
            Nfs4Status::Inval
        })?;

        // Get file status to know size
        let status = self.fs.ufs().get_status(&fs_path).await.map_err(|e| {
            debug!("Failed to get status of state file {}: {:?}", path, e);
            Nfs4Status::Noent
        })?;

        if status.len == 0 {
            return Ok(Vec::new());
        }

        // Open file
        let mut reader = self.fs.ufs().open(&fs_path).await.map_err(|e| {
            debug!("Failed to open state file {}: {:?}", path, e);
            Nfs4Status::Noent
        })?;

        // Read entire file using async_read
        use curvine_common::fs::Reader;
        use orpc::sys::DataSlice;
        let mut data = Vec::new();

        while reader.has_remaining() {
            let chunk = reader.async_read(None).await.map_err(|e| {
                error!("Failed to read state file {}: {:?}", path, e);
                Nfs4Status::Serverfault
            })?;

            if chunk.is_empty() {
                break;
            }

            // Extract bytes from DataSlice
            match chunk {
                DataSlice::Buffer(buf) => data.extend_from_slice(&buf),
                DataSlice::Bytes(bytes) => data.extend_from_slice(&bytes),
                DataSlice::MemSlice(mem) => data.extend_from_slice(mem.as_slice()),
                _ => {
                    error!("Unexpected DataSlice variant");
                    return Err(Nfs4Status::Serverfault.into());
                }
            }
        }

        debug!("Read {} bytes from {}", data.len(), path);
        Ok(data)
    }

    /// List files in a directory
    async fn list_dir(&self, dir_path: &str) -> Nfs4Result<Vec<String>> {
        use curvine_common::fs::Path as FsPath;

        let fs_path = FsPath::new(dir_path).map_err(|_| {
            error!("Invalid directory path: {}", dir_path);
            Nfs4Status::Inval
        })?;

        let entries = self.fs.ufs().list_status(&fs_path).await.map_err(|e| {
            debug!("Failed to list directory {}: {:?}", dir_path, e);
            Nfs4Status::Noent
        })?;

        let names: Vec<String> = entries
            .iter()
            .filter(|e| e.file_type == curvine_common::state::FileType::File)
            .map(|e| e.name.clone())
            .collect();

        debug!("Found {} files in {}", names.len(), dir_path);
        Ok(names)
    }

    /// Write state file (simple version for shutdown save)
    async fn write_state_file(&self, path: &str, data: &[u8]) -> Nfs4Result<()> {
        use curvine_common::fs::Path as FsPath;

        let fs_path = FsPath::new(path).map_err(|_| {
            error!("Invalid path: {}", path);
            Nfs4Status::Inval
        })?;

        // Create and write file using FileSystem trait
        let mut writer = self.fs.ufs().create(&fs_path, true).await.map_err(|e| {
            error!("Failed to create state file {}: {:?}", path, e);
            Nfs4Status::Serverfault
        })?;

        use curvine_common::fs::Writer;
        writer.write(data).await.map_err(|e| {
            error!("Failed to write state file {}: {:?}", path, e);
            Nfs4Status::Serverfault
        })?;

        writer.flush().await.map_err(|e| {
            error!("Failed to flush state file {}: {:?}", path, e);
            Nfs4Status::Serverfault
        })?;

        writer.complete().await.map_err(|e| {
            error!("Failed to complete state file {}: {:?}", path, e);
            Nfs4Status::Serverfault
        })?;

        debug!("Wrote {} bytes to {}", data.len(), path);
        Ok(())
    }

    /// Clean up all persisted state
    ///
    /// Called after grace period ends to remove unclaimed state.
    pub async fn cleanup_unclaimed_state(&self) -> Nfs4Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        info!("Cleaning up unclaimed state after grace period");

        // Delete all state files in instance-specific directories
        let dirs = vec![&self.clients_dir, &self.opens_dir, &self.locks_dir];
        for dir in dirs {
            if let Ok(files) = self.list_dir(dir).await {
                for filename in files {
                    let path = format!("{dir}/{filename}");
                    if let Err(e) = self.delete_state_file(&path).await {
                        warn!("Failed to delete unclaimed state file {path}: {e:?}");
                    }
                }
            }
        }

        // Delete recovery metadata
        if let Err(e) = self.delete_state_file(&self.recovery_meta).await {
            debug!("Failed to delete recovery metadata: {e:?}");
        }

        info!("Cleanup completed");
        Ok(())
    }

    // ============================================================================
    // Private Helper Methods
    // ============================================================================

    /// Delete state file
    async fn delete_state_file(&self, path: &str) -> Nfs4Result<()> {
        use curvine_common::fs::Path as FsPath;

        let fs_path = FsPath::new(path).map_err(|_| {
            error!("Invalid path: {}", path);
            Nfs4Status::Inval
        })?;

        self.fs.ufs().delete(&fs_path, false).await.map_err(|e| {
            debug!("Failed to delete state file {}: {:?}", path, e);
            Nfs4Status::Serverfault
        })?;

        debug!("Deleted state file: {}", path);
        Ok(())
    }
}
