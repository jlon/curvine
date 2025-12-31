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

//! NFSv4.1 File System Implementation
//!
//! Key differences from NFSv3:
//! - Stateful: OPEN creates state, CLOSE releases it
//! - Reader/Writer bound to OpenState, not cached globally
//! - No io_cache needed - state management handles resource lifecycle
//!
//! # Architecture
//!
//! ```text
//! NFSv3 (stateless):
//!   READ/WRITE → io_cache → ReaderPool/WriterCache → UnifiedFS
//!
//! NFSv4.1 (stateful):
//!   OPEN → OpenState (holds Reader/Writer) → UnifiedFS
//!   READ/WRITE (stateid) → OpenState → Reader/Writer
//!   CLOSE → OpenState.complete() → release resources
//! ```

use crate::gateway::{NfsWriter, ReaderPool};
use crate::nfs4::error::{Nfs4Error, Nfs4Result, Nfs4Status};
use crate::nfs4::types::*;
use curvine_client::unified::{UnifiedFileSystem, UnifiedWriter};
use curvine_common::conf::{ClusterConf, NfsGatewayConf};
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{FileAllocOpts, FileStatus, FileType, OpenFlags};
use moka::sync::Cache;
use orpc::runtime::Runtime;
use orpc::sys::DataSlice;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio_util::bytes::Bytes;

/// Root directory file ID
pub const ROOT_FILEID: Fileid4 = 1000;

/// Generate a unique fileid from path for UFS files
///
/// UFS (like S3) doesn't have inode concept, so FileStatus.id is 0.
/// We generate a consistent fileid from path hash to ensure:
/// 1. Same path always gets same fileid
/// 2. Different paths get different fileids
/// 3. fileid > 0 (to pass READDIR cookie filter)
///
/// # Algorithm
/// Use FNV-1a hash (fast, good distribution) and ensure result > 0
fn generate_fileid_from_path(path: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    let hash = hasher.finish();
    
    // Ensure fileid > 0 by using high bit as 0 and adding 1
    // This gives us range [1, 2^63]
    (hash & 0x7FFF_FFFF_FFFF_FFFF) + 1
}

/// Get fileid from FileStatus, handling object storage without inode
///
/// # Unified Fileid Generation (CRITICAL for Object Storage)
///
/// This function ensures consistent fileid generation across all operations:
/// - For filesystems with inodes (local FS): use status.id
/// - For object storage without inodes (S3, etc): generate from path hash
///
/// # Why This Matters
/// Object storage like S3 doesn't have inode concept, so FileStatus.id is 0.
/// If we use 0 as fileid, all files would have the same fileid, causing:
/// - OPEN/CLOSE operations to conflict
/// - READ/WRITE operations to access wrong files
/// - Cache corruption
///
/// By using path hash for id==0 cases, we ensure:
/// - Each file has a unique, consistent fileid
/// - Same path always generates same fileid
/// - All operations (OPEN/READ/WRITE/CLOSE) use the same fileid
///
/// # Arguments
/// - status: FileStatus from backend
/// - path: File path (used for hash generation when id==0)
///
/// # Returns
/// Consistent fileid for this file
#[inline]
fn get_fileid_from_status(status: &FileStatus, path: &str) -> u64 {
    if status.id == 0 {
        generate_fileid_from_path(path)
    } else {
        status.id as u64
    }
}

// ============================================================================
// Path Cache (required for fileid -> path mapping)
// ============================================================================

/// Path cache for bidirectional fileid <-> path mapping
///
/// # NFS-Ganesha Alignment
/// NFS-Ganesha uses fsal_obj_handle which directly references the underlying
/// inode. Our implementation uses path_cache as a soft reference, which requires
/// careful cache invalidation during RENAME/REMOVE operations.
///
/// Key operations:
/// - insert(): Add bidirectional mapping
/// - remove(): Remove by fileid (clears both directions)
/// - remove_by_path(): Remove by path (clears both directions)
/// - rename(): Atomic update for RENAME operation
struct PathCache {
    id_to_path: Cache<Fileid4, String>,
    path_to_id: Cache<String, Fileid4>,
    root_id: Fileid4,
}

impl PathCache {
    fn new(max_size: u64, ttl: Duration, root_id: Fileid4) -> Self {
        let id_to_path = Cache::builder()
            .max_capacity(max_size)
            .time_to_live(ttl)
            .build();
        let path_to_id = Cache::builder()
            .max_capacity(max_size)
            .time_to_live(ttl)
            .build();
        Self {
            id_to_path,
            path_to_id,
            root_id,
        }
    }

    #[inline]
    fn get_path(&self, id: Fileid4) -> Option<String> {
        if id == self.root_id {
            return Some("/".to_string());
        }
        self.id_to_path.get(&id)
    }

    #[inline]
    fn get_fileid(&self, path: &str) -> Option<Fileid4> {
        if path == "/" {
            return Some(self.root_id);
        }
        self.path_to_id.get(path)
    }

    #[inline]
    fn insert(&self, id: Fileid4, path: String) {
        if id != self.root_id {
            self.id_to_path.insert(id, path.clone());
            self.path_to_id.insert(path, id);
        }
    }

    #[inline]
    fn remove(&self, id: Fileid4) {
        if let Some(path) = self.id_to_path.get(&id) {
            self.path_to_id.invalidate(&path);
        }
        self.id_to_path.invalidate(&id);
    }

    /// Remove cache entry by path (used when target file is overwritten)
    #[inline]
    fn remove_by_path(&self, path: &str) {
        if let Some(id) = self.path_to_id.get(path) {
            self.id_to_path.invalidate(&id);
        }
        self.path_to_id.invalidate(path);
    }

    /// Atomic rename operation for path cache
    ///
    /// # NFS-Ganesha Alignment
    /// When a file is renamed, we need to:
    /// 1. Remove old path -> fileid mapping
    /// 2. Remove target path -> fileid mapping (if target exists, it's overwritten)
    /// 3. Update fileid -> new_path mapping
    /// 4. Add new_path -> fileid mapping
    ///
    /// # Arguments
    /// - old_fileid: The fileid of the file being renamed
    /// - old_path: The old path of the file
    /// - new_path: The new path of the file
    /// - new_fileid: The fileid after rename (may be different from old_fileid)
    fn rename(&self, old_fileid: Fileid4, old_path: &str, new_path: &str, new_fileid: Fileid4) {
        // Step 1: Remove old path -> fileid mapping
        self.path_to_id.invalidate(old_path);

        // Step 2: Remove target path's old mapping (if target exists, it's overwritten)
        if let Some(target_fileid) = self.path_to_id.get(new_path) {
            self.id_to_path.invalidate(&target_fileid);
            self.path_to_id.invalidate(new_path);
        }

        // Step 3: Remove old fileid -> path mapping (if fileid changed)
        if old_fileid != new_fileid {
            self.id_to_path.invalidate(&old_fileid);
        }

        // Step 4: Add new mappings
        self.id_to_path.insert(new_fileid, new_path.to_string());
        self.path_to_id.insert(new_path.to_string(), new_fileid);
    }
}

// ============================================================================
// Open File State (holds Reader/Writer)
// ============================================================================

/// Open file state - holds Reader and/or Writer (NFS-Ganesha's fsal_fd equivalent)
///
/// NFS-Ganesha equivalent:
/// ```c
/// struct fsal_fd {
///     int fd;  // Our Reader/Writer
///     fsal_openflags_t openflags;
///     int32_t fd_work;  // Reference counting
/// }
/// ```
///
/// Design (NFS-Ganesha aligned):
/// - File-level resource, stored in Nfs4FileSystem.open_files
/// - Shared by multiple OpenStates for the same file
/// - Reference counted: last CLOSE calls complete()
/// - Access mode can be upgraded (READ -> READ+WRITE)
/// - Reader uses ReaderPool with multiple NfsReaders for concurrent reads
/// - Writer uses NfsWriter with internal AsyncMutex for thread-safe writes
pub struct OpenFile {
    /// File ID
    pub fileid: Fileid4,
    /// File path
    pub path: Path,
    /// Reader pool (created when first OPEN with READ access)
    /// Uses ReaderPool with round-robin selection for concurrent reads
    pub reader_pool: RwLock<Option<Arc<ReaderPool>>>,
    /// Writer (created when first OPEN with WRITE access)
    /// NfsWriter is Clone and internally uses Arc<AsyncMutex<...>>
    /// No external lock needed - just clone and use
    pub writer: RwLock<Option<NfsWriter>>,
    /// Current access mode (can be upgraded)
    pub access: RwLock<u32>,
    /// Reference count (number of OpenStates using this OpenFile)
    pub ref_count: AtomicU32,
}

impl std::fmt::Debug for OpenFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenFile")
            .field("fileid", &self.fileid)
            .field("path", &self.path.path())
            .field("access", &self.access.read().unwrap())
            .field("ref_count", &self.ref_count.load(Ordering::Acquire))
            .finish()
    }
}

impl OpenFile {
    /// Create new OpenFile (NFS-Ganesha: fsal_open2)
    ///
    /// This is called when the file is opened for the first time.
    /// Subsequent OPENs for the same file will reuse this OpenFile.
    ///
    /// # Arguments
    /// - writer: If provided, wraps in NfsWriter for auto-extend support
    /// - reader_pool: If provided, uses ReaderPool for concurrent reads
    fn new(
        fileid: Fileid4,
        path: Path,
        reader_pool: Option<Arc<ReaderPool>>,
        writer: Option<UnifiedWriter>,
        access: u32,
    ) -> Self {
        // Wrap UnifiedWriter in NfsWriter for auto-extend support
        let nfs_writer = writer.map(NfsWriter::new);

        Self {
            fileid,
            path,
            reader_pool: RwLock::new(reader_pool),
            writer: RwLock::new(nfs_writer),
            access: RwLock::new(access),
            ref_count: AtomicU32::new(1), // Start with ref_count = 1
        }
    }

    /// Increment reference count (called on each OPEN)
    /// NFS-Ganesha: fsal_start_fd_work()
    pub fn add_ref(&self) -> u32 {
        let old_count = self.ref_count.fetch_add(1, Ordering::AcqRel);
        old_count + 1
    }

    /// Decrement reference count (called on each CLOSE)
    /// Returns true if this was the last reference
    /// NFS-Ganesha: fsal_complete_fd_work()
    pub fn release_ref(&self) -> bool {
        let old_count = self.ref_count.fetch_sub(1, Ordering::AcqRel);
        old_count == 1 // Last reference
    }

    /// Get current write position from Writer (if any)
    ///
    /// This is used by GETATTR to return the correct file size when
    /// there's buffered data in the Writer that hasn't been committed yet.
    ///
    /// Returns None if no Writer exists or Writer has no data.
    /// Get current write position from Writer (if any)
    ///
    /// This is used by GETATTR to return the correct file size when
    /// there's buffered data in the Writer that hasn't been committed yet.
    ///
    /// Returns None if no Writer exists.
    pub async fn get_writer_pos(&self) -> Option<i64> {
        let writer = {
            let writer_guard = self.writer.read().unwrap();
            writer_guard.clone()
        };

        if let Some(writer) = writer {
            writer.get_pos().await
        } else {
            None
        }
    }

    /// Upgrade access mode (NFS-Ganesha: fsal_reopen2)
    ///
    /// Called when an existing OpenFile needs additional access.
    /// For example: file opened with READ, then another OPEN with WRITE.
    ///
    /// # Thread Safety
    /// Uses scoped locking pattern to ensure RwLock guards are released before await points.
    /// This is critical for Rust's Send trait requirements in async contexts.
    pub async fn upgrade_access(
        &self,
        new_access: u32,
        ufs: &UnifiedFileSystem,
        pool_size: usize,
    ) -> Nfs4Result<()> {
        // Step 1: Check if upgrade is needed (acquire and release lock quickly)
        let (current_access, combined_access, need_reader, need_writer) = {
            let access_guard = self.access.read().unwrap();
            let current = *access_guard;
            let combined = current | new_access;
            let need_reader = (combined & 0x01) != 0 && (current & 0x01) == 0;
            let need_writer = (combined & 0x02) != 0 && (current & 0x02) == 0;
            (current, combined, need_reader, need_writer)
        }; // Lock released here - CRITICAL for Send trait

        if combined_access == current_access {
            // No upgrade needed
            return Ok(());
        }

        // Step 2: Create ReaderPool if needed (no locks held during async operation)
        if need_reader {
            let path = self.path.clone();
            let reader_pool = ReaderPool::new(pool_size, || {
                let p = path.clone();
                let fs = ufs.clone();
                async move { fs.open(&p).await }
            })
            .await
            .map_err(Nfs4Error::from)?;

            // Step 3: Update reader_pool (sync lock)
            *self.reader_pool.write().unwrap() = Some(Arc::new(reader_pool));
        }

        // Step 4: Create Writer if needed (no locks held during async operation)
        if need_writer {
            let flags = OpenFlags::new_write_only().set_create(false);
            let opts = ufs.cv().create_opts_builder().build();
            let writer = ufs
                .open_with_opts(&self.path, opts, flags)
                .await
                .map_err(Nfs4Error::from)?;

            // Step 5: Wrap in NfsWriter and update (sync lock)
            let nfs_writer = NfsWriter::new(writer);
            *self.writer.write().unwrap() = Some(nfs_writer);
        }

        // Step 6: Update access mode (final quick lock)
        {
            let mut access_guard = self.access.write().unwrap();
            *access_guard = combined_access;
        } // Lock released immediately

        Ok(())
    }

    /// Read data from file (NFS-Ganesha: fsal_read)
    ///
    /// # Thread Safety
    /// Uses ReaderPool with round-robin selection for concurrent reads.
    /// Each NfsReader is protected by Mutex in ReaderEntry.
    pub async fn read(&self, offset: u64, count: u32) -> Nfs4Result<(Vec<DataSlice>, bool)> {
        // Step 1: Get ReaderPool entry (release RwLock before await)
        let reader_entry = {
            let pool_guard = self.reader_pool.read().unwrap();
            let pool = pool_guard.as_ref().ok_or_else(|| {
                Nfs4Error::with_message(Nfs4Status::Openmode, "File not opened for read")
            })?;
            // Get a reader from pool (round-robin)
            pool.get()
        }; // RwLock released here - CRITICAL for Send trait

        // Step 2: Lock the reader (each reader is independent)
        let mut reader = reader_entry.reader.lock().await;

        // Step 3: Get file length (no additional lock needed)
        let file_len = reader.len();

        // Step 4: Check bounds
        if offset >= file_len as u64 {
            return Ok((vec![], true));
        }

        let remaining = file_len as u64 - offset;
        let read_count = count.min(remaining as u32);

        if read_count == 0 {
            return Ok((vec![], true));
        }

        // Step 5: Perform read operation (zero-copy, direct call)
        let slices = reader
            .fuse_read(offset as i64, read_count as usize)
            .await
            .map_err(Nfs4Error::from)?;

        // Step 6: Calculate EOF
        let total_len: usize = slices.iter().map(|s| s.len()).sum();
        let eof = total_len < count as usize || (offset + total_len as u64) >= file_len as u64;

        Ok((slices, eof))
    }

    /// Write data to file (NFS-Ganesha: fsal_write)
    ///
    /// # NFSv4 Write Semantics
    /// - WRITE operation buffers data in Writer
    /// - Data blocks are committed on CLOSE (via complete())
    /// - If write extends beyond current file size, NfsWriter auto-extends
    ///
    /// # Thread Safety
    /// NfsWriter uses internal AsyncMutex to serialize writes.
    /// Clone is cheap (Arc clone), no external lock needed.
    pub async fn write(&self, offset: u64, data: Vec<u8>) -> Nfs4Result<u32> {
        tracing::info!(
            "OpenFile::write: fileid={} path={} offset={} len={}",
            self.fileid,
            self.path.path(),
            offset,
            data.len()
        );

        // Clone NfsWriter - cheap Arc clone
        let writer = {
            let writer_guard = self.writer.read().unwrap();
            writer_guard
                .as_ref()
                .ok_or_else(|| {
                    tracing::error!(
                        "OpenFile::write: No writer! fileid={} path={}",
                        self.fileid,
                        self.path.path()
                    );
                    Nfs4Error::with_message(Nfs4Status::Openmode, "File not opened for write")
                })?
                .clone()
        }; // RwLock released here before await

        tracing::info!(
            "OpenFile::write: Calling NfsWriter.write fileid={} offset={} len={}",
            self.fileid,
            offset,
            data.len()
        );

        // NfsWriter handles auto-extend internally
        let result = writer
            .write(offset as i64, data)
            .await
            .map_err(Nfs4Error::from)?;

        tracing::info!(
            "OpenFile::write: NfsWriter.write completed fileid={} written={}",
            self.fileid,
            result
        );

        Ok(result)
    }

    /// Flush buffered data to storage (NFS-Ganesha: fsync)
    /// Called on CLOSE when there are still other references
    ///
    /// Unlike complete(), this keeps the Writer open for subsequent writes.
    pub async fn flush(&self) -> Nfs4Result<()> {
        // Clone NfsWriter to avoid holding lock across await
        let writer = {
            let writer_guard = self.writer.read().unwrap();
            writer_guard.clone()
        };

        if let Some(writer) = writer {
            writer.flush().await.map_err(|e| {
                tracing::error!("Failed to flush Writer for file {}: {:?}", self.fileid, e);
                Nfs4Error::from(e)
            })?;
        }
        Ok(())
    }

    /// Complete and release resources (NFS-Ganesha: fsal_close2)
    /// Called when the last CLOSE happens (ref_count reaches 0)
    pub async fn complete(&self) -> Nfs4Result<()> {
        // Step 1: Complete Writer - commit data blocks
        {
            let writer = {
                let writer_guard = self.writer.read().unwrap();
                writer_guard.clone()
            };

            if let Some(writer) = writer {
                writer.complete().await.map_err(|e| {
                    tracing::error!(
                        "Failed to complete Writer for file {}: {:?}",
                        self.fileid,
                        e
                    );
                    Nfs4Error::from(e)
                })?;
            }
        }

        Ok(())
    }
}

/// NFSv4 File System (NFS-Ganesha aligned: file-level fd management)
///
/// Key design (NFS-Ganesha aligned):
/// 1. OpenFile stored at file level (like fsal_obj_handle.fd)
/// 2. Multiple OpenStates share the same OpenFile
/// 3. Reference counting: last CLOSE calls complete()
pub struct Nfs4FileSystem {
    /// Unified file system client
    ufs: UnifiedFileSystem,
    /// Gateway configuration
    config: NfsGatewayConf,
    /// Cluster generation (for file handle validation)
    cluster_generation: u64,
    /// Path cache (fileid -> path)
    path_cache: PathCache,
    /// FileStatus cache (fileid -> FileStatus) - reduces Master queries
    status_cache: Option<Cache<Fileid4, FileStatus>>,
    /// Open files (fileid -> OpenFile) - NFS-Ganesha's fsal_obj_handle.fd equivalent
    /// This is the file-level fd storage, shared by multiple states
    open_files: RwLock<HashMap<Fileid4, Arc<OpenFile>>>,
    /// Runtime for async operations
    #[allow(dead_code)]
    runtime: Arc<Runtime>,
}

impl Nfs4FileSystem {
    pub fn new(
        cluster_conf: ClusterConf,
        gateway_config: NfsGatewayConf,
        runtime: Arc<Runtime>,
    ) -> Result<Self, FsError> {
        let cluster_generation =
            gateway_config.effective_cluster_generation(&cluster_conf.cluster_id);
        let ufs = UnifiedFileSystem::with_rt(cluster_conf, runtime.clone())?;

        let path_cache = PathCache::new(
            gateway_config.path_cache_size as u64,
            gateway_config.path_cache_ttl(),
            ROOT_FILEID,
        );

        // FileStatus cache to reduce Master queries
        // Only create if enabled (capacity >= 0)
        let status_cache = if gateway_config.file_status_cache_size >= 0 {
            Some(
                Cache::builder()
                    .max_capacity(gateway_config.file_status_cache_size as u64)
                    .time_to_live(Duration::from_secs(
                        gateway_config.file_status_cache_ttl_secs,
                    ))
                    .build(),
            )
        } else {
            None
        };

        Ok(Self {
            ufs,
            config: gateway_config,
            cluster_generation,
            path_cache,
            status_cache,
            open_files: RwLock::new(HashMap::new()),
            runtime,
        })
    }

    // ========================================================================
    // File Handle Operations
    // ========================================================================

    /// Get path from file ID
    pub fn get_path(&self, fileid: Fileid4) -> Nfs4Result<Path> {
        if fileid == ROOT_FILEID {
            return Path::new("/").map_err(|_| Nfs4Status::Stale.into());
        }
        self.path_cache
            .get_path(fileid)
            .and_then(|p| Path::new(&p).ok())
            .ok_or_else(|| Nfs4Status::Stale.into())
    }

    /// Cache path for file ID
    #[inline]
    pub fn cache_path(&self, fileid: Fileid4, path: String) {
        self.path_cache.insert(fileid, path);
    }

    /// Convert file ID to file handle
    pub fn fileid_to_fh(&self, fileid: Fileid4) -> Nfs4FileHandle {
        let mut data = Vec::with_capacity(16);
        data.extend_from_slice(&self.cluster_generation.to_le_bytes());
        data.extend_from_slice(&fileid.to_le_bytes());
        Nfs4FileHandle::new(data)
    }

    /// Convert file handle to file ID
    pub fn fh_to_fileid(&self, fh: &Nfs4FileHandle) -> Nfs4Result<Fileid4> {
        if fh.data.len() != 16 {
            return Err(Nfs4Status::Badhandle.into());
        }

        let gen = u64::from_le_bytes(
            fh.data[0..8]
                .try_into()
                .map_err(|_| Nfs4Status::Badhandle)?,
        );
        let fileid = u64::from_le_bytes(
            fh.data[8..16]
                .try_into()
                .map_err(|_| Nfs4Status::Badhandle)?,
        );

        // Validate generation matches server's cluster generation
        if gen != self.cluster_generation {
            return Err(Nfs4Status::Stale.into());
        }

        Ok(fileid)
    }

    // ========================================================================
    // Status Operations
    // ========================================================================

    /// Get file status (with optional caching)
    ///
    /// Note: NFSv4.1 clients cache attributes locally. When Delegation is
    /// enabled, the server uses CB_GETATTR to ensure cache coherency.
    ///
    /// # Writer Position Handling
    /// When a file has an active Writer with buffered data, the file size
    /// from the backend storage may be stale. We check the Writer's current
    /// position and use it as the file size if it's larger than the stored size.
    pub async fn get_status(&self, fileid: Fileid4) -> Nfs4Result<FileStatus> {
        // Try cache first if enabled
        if let Some(ref cache) = self.status_cache {
            if let Some(status) = cache.get(&fileid) {
                return Ok(status);
            }
        }

        // Cache miss - query from server
        let path = self.get_path(fileid)?;
        let mut status = self.ufs.get_status(&path).await.map_err(Nfs4Error::from)?;

        // Check if there's an active Writer with buffered data
        // If so, use the Writer's position as the file size
        // IMPORTANT: Do NOT cache status when Writer is active, as writer_pos changes frequently
        let has_active_writer = if let Some(open_file) = self.get_open_file(fileid) {
            if let Some(writer_pos) = open_file.get_writer_pos().await {
                if writer_pos > status.len {
                    status.len = writer_pos;
                }
                true // Has active Writer
            } else {
                false
            }
        } else {
            false
        };

        // Cache the result ONLY if no active Writer
        // When Writer is active, file size changes frequently, caching would return stale data
        if !has_active_writer {
            if let Some(ref cache) = self.status_cache {
                cache.insert(fileid, status.clone());
            }
        }

        Ok(status)
    }

    /// Invalidate FileStatus cache for a file (called on modifications)
    #[inline]
    fn invalidate_status_cache(&self, fileid: Fileid4) {
        if let Some(ref cache) = self.status_cache {
            cache.invalidate(&fileid);
        }
    }

    /// Public method to invalidate FileStatus cache after write operations
    ///
    /// # NFS-Ganesha Alignment
    /// After a WRITE operation, the file size and mtime change.
    /// We must invalidate the status cache to ensure subsequent GETATTR
    /// returns the updated attributes.
    #[inline]
    pub fn invalidate_status_cache_for_write(&self, fileid: Fileid4) {
        self.invalidate_status_cache(fileid);
    }

    /// Alias for get_status (for compatibility)
    #[inline]
    pub async fn get_status_cached(&self, fileid: Fileid4) -> Nfs4Result<FileStatus> {
        self.get_status(fileid).await
    }

    /// Open file for a NEW state (NFS-Ganesha: fsal_open2)
    ///
    /// This is called when a NEW state is created (new_state=true in open_ex).
    /// It always increments ref_count because a new state is being created.
    ///
    /// # NFS-Ganesha Reference
    /// - fsal_open2() at line 1024 in nfs4_op_open.c
    /// - Called when *file_state == NULL (no existing state)
    ///
    /// # Behavior
    /// - If OpenFile doesn't exist: create it with ref_count=1
    /// - If OpenFile exists: add_ref() and upgrade access
    ///
    /// # Important
    /// This function ALWAYS increments ref_count because it's called
    /// for a NEW state. The ref_count tracks the number of states
    /// referencing this OpenFile.
    pub async fn open_file(&self, fileid: Fileid4, access: u32) -> Nfs4Result<Arc<OpenFile>> {
        // Check if OpenFile already exists (fast path with read lock)
        let existing_file = {
            let open_files = self.open_files.read().unwrap();
            open_files.get(&fileid).cloned()
        }; // Lock released here before await

        let pool_size = self.config.reader_pool_size;

        if let Some(open_file) = existing_file {
            // OpenFile exists - this means another state already opened this file.
            // We need to add_ref() because we're creating a NEW state.
            // NFS-Ganesha: each state holds a reference to the fd
            open_file.add_ref();

            // Upgrade access if needed
            open_file
                .upgrade_access(access, &self.ufs, pool_size)
                .await?;

            return Ok(open_file);
        }

        // OpenFile doesn't exist, create new one (slow path with write lock)
        // Double-check pattern to avoid race condition
        let double_check_result = {
            let open_files = self.open_files.read().unwrap();
            open_files.get(&fileid).cloned()
        }; // Lock released here - CRITICAL for Send trait

        if let Some(open_file) = double_check_result {
            // Race condition: another thread created the OpenFile
            open_file.add_ref();
            open_file
                .upgrade_access(access, &self.ufs, pool_size)
                .await?;
            return Ok(open_file);
        }

        // Create new OpenFile (NFS-Ganesha: fsal_open2)
        // ref_count starts at 1 (for this new state)
        let path = self.get_path(fileid)?;

        // Create ReaderPool if read access requested
        let reader_pool = if access & 0x01 != 0 {
            let p = path.clone();
            let ufs = self.ufs.clone();
            match ReaderPool::new(pool_size, || {
                let path_clone = p.clone();
                let fs = ufs.clone();
                async move { fs.open(&path_clone).await }
            })
            .await
            {
                Ok(pool) => Some(Arc::new(pool)),
                Err(e) => {
                    tracing::error!("Failed to create ReaderPool for fileid={}: {:?}", fileid, e);
                    return Err(Nfs4Error::from(e));
                }
            }
        } else {
            None
        };

        // Create Writer if write access requested
        let writer = if access & 0x02 != 0 {
            let flags = OpenFlags::new_write_only().set_create(false);
            let opts = self.ufs.cv().create_opts_builder().build();
            match self.ufs.open_with_opts(&path, opts, flags).await {
                Ok(w) => Some(w),
                Err(e) => {
                    tracing::error!("Failed to create Writer for fileid={}: {:?}", fileid, e);
                    return Err(Nfs4Error::from(e));
                }
            }
        } else {
            None
        };

        let open_file = Arc::new(OpenFile::new(fileid, path, reader_pool, writer, access));

        // Store in open_files HashMap (acquire write lock again)
        self.open_files
            .write()
            .unwrap()
            .insert(fileid, open_file.clone());

        Ok(open_file)
    }

    /// Reopen file to upgrade access mode (NFS-Ganesha: fsal_reopen2)
    ///
    /// This is called when an EXISTING state is reused (new_state=false in open_ex).
    ///
    /// # NFS-Ganesha Reference
    /// - fsal_reopen2() at line 1097 in nfs4_op_open.c
    /// - Called when *file_state != NULL (existing state found)
    ///
    /// # Behavior
    /// - Increments ref_count (each OPEN represents a file handle reference)
    /// - Upgrades access mode if needed
    ///
    /// # Important
    /// The ref_count tracks the number of OPEN operations (file handles) referencing
    /// this OpenFile. Each OPEN (whether new state or reused state) represents a
    /// separate file handle that will eventually be CLOSEd.
    ///
    /// NFS-Ganesha equivalent: Each OPEN increments fd_work reference count,
    /// and each CLOSE decrements it. Only the last CLOSE calls complete().
    pub async fn reopen_file(&self, fileid: Fileid4, access: u32) -> Nfs4Result<()> {
        self.reopen_file_ex(fileid, access, true).await
    }

    /// Reopen file with optional ref_count increment
    ///
    /// # Arguments
    /// - fileid: File ID
    /// - access: Access mode to upgrade to
    /// - add_ref: Whether to increment ref_count (false for post-CREATE OPEN)
    pub async fn reopen_file_ex(
        &self,
        fileid: Fileid4,
        access: u32,
        add_ref: bool,
    ) -> Nfs4Result<()> {
        let open_file = {
            let open_files = self.open_files.read().unwrap();
            open_files.get(&fileid).cloned()
        };

        let pool_size = self.config.reader_pool_size;

        if let Some(open_file) = open_file {
            // Only increment ref_count if add_ref is true
            // For post-CREATE OPEN, we don't increment because CREATE already did
            if add_ref {
                open_file.add_ref();
            }

            // Upgrade access mode if needed (NFS-Ganesha: fsal_reopen2)
            open_file
                .upgrade_access(access, &self.ufs, pool_size)
                .await?;

            Ok(())
        } else {
            // OpenFile doesn't exist - this shouldn't happen for state reuse
            // But we handle it gracefully by creating a new one
            tracing::warn!(
                "REOPEN2: OpenFile not found for fileid={}, creating new one",
                fileid
            );
            self.open_file(fileid, access).await?;
            Ok(())
        }
    }

    /// Get OpenFile for a file (used by READ/WRITE operations)
    pub fn get_open_file(&self, fileid: Fileid4) -> Option<Arc<OpenFile>> {
        self.open_files.read().unwrap().get(&fileid).cloned()
    }

    /// Close OpenFile (NFS-Ganesha: fsal_close2)
    ///
    /// Decrements reference count. If this is the last reference:
    /// - Calls complete() to commit data
    /// - Invalidates status cache (file size/mtime changed)
    /// - Removes from open_files HashMap
    pub async fn close_file(&self, fileid: Fileid4) -> Nfs4Result<()> {
        let open_file = {
            let open_files = self.open_files.read().unwrap();
            open_files.get(&fileid).cloned()
        };

        let open_file = open_file.ok_or_else(|| {
            tracing::warn!("CLOSE: OpenFile not found for fileid={}", fileid);
            Nfs4Error::with_message(Nfs4Status::BadStateid, "OpenFile not found")
        })?;

        let is_last = open_file.release_ref();

        tracing::info!(
            "CLOSE: fileid={} path={} ref_count={} is_last={}",
            fileid,
            open_file.path.path(),
            open_file.ref_count.load(Ordering::Acquire),
            is_last
        );

        if is_last {
            tracing::info!("CLOSE: Last reference, calling complete() for fileid={}", fileid);
            // Call complete() to commit data
            open_file.complete().await?;

            // Invalidate status cache after complete()
            // This ensures subsequent GETATTR returns the updated file size/mtime
            self.invalidate_status_cache(fileid);

            // Remove from HashMap
            self.open_files.write().unwrap().remove(&fileid);
            tracing::info!("CLOSE: Removed OpenFile from HashMap for fileid={}", fileid);
        }

        Ok(())
    }

    // ========================================================================
    // Directory Operations
    // ========================================================================

    /// Lookup file in directory
    ///
    /// Uses PathCache (path -> fileid) and StatusCache (fileid -> FileStatus)
    /// to avoid querying Master when possible.
    pub async fn lookup(
        &self,
        parent_id: Fileid4,
        name: &str,
    ) -> Nfs4Result<(Fileid4, FileStatus)> {
        let parent_path = self.get_path(parent_id)?;

        // Handle special names - these can use cache via get_status
        if name == "." {
            let status = self.get_status(parent_id).await?;
            return Ok((parent_id, status));
        }
        if name == ".." {
            let parent = parent_path
                .parent()
                .ok()
                .flatten()
                .unwrap_or_else(|| Path::new("/").unwrap());
            let status = self
                .ufs
                .get_status(&parent)
                .await
                .map_err(Nfs4Error::from)?;
            let fileid = get_fileid_from_status(&status, parent.path());
            self.cache_path(fileid, parent.to_string());

            // Populate StatusCache for future get_status calls
            if let Some(ref cache) = self.status_cache {
                cache.insert(fileid, status.clone());
            }

            return Ok((fileid, status));
        }

        // Build child path
        let child_path = self.build_child_path(&parent_path, name)?;
        let child_path_str = child_path.to_string();

        // Try PathCache first (path -> fileid)
        if let Some(fileid) = self.path_cache.get_fileid(&child_path_str) {
            // Try StatusCache (fileid -> FileStatus)
            let status = self.get_status(fileid).await?;
            return Ok((fileid, status));
        }

        // Cache miss - query from Master
        let status = self
            .ufs
            .get_status(&child_path)
            .await
            .map_err(Nfs4Error::from)?;
        let fileid = get_fileid_from_status(&status, &child_path_str);

        // Populate both caches
        self.cache_path(fileid, child_path_str);
        if let Some(ref cache) = self.status_cache {
            cache.insert(fileid, status.clone());
        }

        Ok((fileid, status))
    }

    /// Build child path from parent and name
    fn build_child_path(&self, parent: &Path, name: &str) -> Nfs4Result<Path> {
        let child_str = if parent.path() == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.path())
        };
        Path::new(&child_str).map_err(|_| Nfs4Status::Inval.into())
    }

    /// Read directory entries
    pub async fn readdir(
        &self,
        dir_id: Fileid4,
        cookie: u64,
        max_entries: usize,
    ) -> Nfs4Result<(Vec<(Fileid4, String, FileStatus)>, bool)> {
        let path = self.get_path(dir_id)?;

        let entries = self.ufs.list_status(&path).await.map_err(Nfs4Error::from)?;

        // Sort by file ID for consistent ordering
        let mut sorted: Vec<_> = entries.iter().collect();
        sorted.sort_by_key(|s| s.id);

        // Filter and collect entries
        let result: Vec<_> = sorted
            .into_iter()
            .filter(|s| {
                // Generate consistent fileid using unified function
                let child_path = if path.path() == "/" {
                    format!("/{}", s.name)
                } else {
                    format!("{}/{}", path.path(), s.name)
                };
                let fileid = get_fileid_from_status(s, &child_path);
                
                fileid > cookie
            })
            .take(max_entries)
            .map(|status| {
                // Generate consistent fileid using unified function
                let child_path = if path.path() == "/" {
                    format!("/{}", status.name)
                } else {
                    format!("{}/{}", path.path(), status.name)
                };
                let fileid = get_fileid_from_status(status, &child_path);
                
                self.cache_path(fileid, child_path);
                
                // Create a modified status with the generated fileid
                let mut modified_status = status.clone();
                modified_status.id = fileid as i64;
                
                (fileid, status.name.clone(), modified_status)
            })
            .collect();

        let end = result.len() < max_entries;
        Ok((result, end))
    }

    // ========================================================================
    // File Modification Operations
    // ========================================================================

    /// Create a file
    pub async fn create_file(
        &self,
        parent_id: Fileid4,
        name: &str,
    ) -> Nfs4Result<(Fileid4, FileStatus)> {
        if self.config.read_only {
            tracing::warn!("CREATE: Attempted to create file in read-only mode");
            return Err(Nfs4Status::Rofs.into());
        }

        let parent_path = self.get_path(parent_id)?;
        let child_path = self.build_child_path(&parent_path, name)?;

        // Check if file already exists
        if self.ufs.get_status(&child_path).await.is_ok() {
            tracing::warn!("CREATE: File already exists: {}", child_path.path());
            return Err(Nfs4Status::Exist.into());
        }

        // Create the file
        let mut writer = self.ufs.create(&child_path, true).await.map_err(|e| {
            tracing::error!("Failed to create file {}: {:?}", child_path.path(), e);
            Nfs4Error::from(e)
        })?;

        writer.complete().await.map_err(|e| {
            tracing::error!(
                "Failed to complete file creation {}: {:?}",
                child_path.path(),
                e
            );
            Nfs4Error::from(e)
        })?;

        let status = self.ufs.get_status(&child_path).await.map_err(|e| {
            tracing::error!(
                "Failed to get status of created file {}: {:?}",
                child_path.path(),
                e
            );
            Nfs4Error::from(e)
        })?;
        let fileid = get_fileid_from_status(&status, &child_path.to_string());

        self.cache_path(fileid, child_path.to_string());

        // Invalidate parent directory's status cache so NFS client sees updated change_info
        self.invalidate_status_cache(parent_id);

        Ok((fileid, status))
    }

    /// Create a directory
    pub async fn mkdir(&self, parent_id: Fileid4, name: &str) -> Nfs4Result<(Fileid4, FileStatus)> {
        if self.config.read_only {
            return Err(Nfs4Status::Rofs.into());
        }

        let parent_path = self.get_path(parent_id)?;
        let child_path = self.build_child_path(&parent_path, name)?;

        self.ufs
            .mkdir(&child_path, false)
            .await
            .map_err(Nfs4Error::from)?;

        let status = self
            .ufs
            .get_status(&child_path)
            .await
            .map_err(Nfs4Error::from)?;
        let fileid = get_fileid_from_status(&status, &child_path.to_string());

        self.cache_path(fileid, child_path.to_string());

        // Invalidate parent directory's status cache so NFS client sees updated change_info
        self.invalidate_status_cache(parent_id);

        Ok((fileid, status))
    }

    /// Remove a file or directory
    ///
    /// # NFS-Ganesha Alignment
    /// NFS-Ganesha's fsal_remove() handles:
    /// 1. Pre/post attributes for parent directory
    /// 2. Proper cache invalidation
    /// 3. State cleanup for open files
    ///
    /// Our implementation needs to:
    /// 1. Get fileid before removal
    /// 2. Clean up path_cache and status_cache
    /// 3. Clean up OpenFile if the file is open
    /// 4. Perform the removal
    pub async fn remove(&self, parent_id: Fileid4, name: &str) -> Nfs4Result<()> {
        if self.config.read_only {
            return Err(Nfs4Status::Rofs.into());
        }

        let parent_path = self.get_path(parent_id)?;
        let child_path = self.build_child_path(&parent_path, name)?;

        // Step 1: Get file ID for cache cleanup
        let fileid = if let Ok(status) = self.ufs.get_status(&child_path).await {
            let fileid = get_fileid_from_status(&status, &child_path.to_string());

            // Step 2: Clean up path_cache
            self.path_cache.remove(fileid);

            // Step 3: Clean up status_cache
            self.invalidate_status_cache(fileid);

            Some(fileid)
        } else {
            None
        };

        // Step 4: Clean up OpenFile if the file is open
        // Note: NFS-Ganesha allows removing open files (Unix semantics)
        // The file data remains accessible until all handles are closed
        if let Some(fileid) = fileid {
            let open_file = {
                let open_files = self.open_files.read().unwrap();
                open_files.get(&fileid).cloned()
            };
            if open_file.is_some() {
                // Remove from HashMap - the OpenFile will be dropped when all references are gone
                self.open_files.write().unwrap().remove(&fileid);
            }
        }

        // Step 5: Perform the removal
        self.ufs
            .delete(&child_path, false)
            .await
            .map_err(Nfs4Error::from)?;

        // Step 6: Invalidate parent directory's status cache so NFS client sees updated change_info
        self.invalidate_status_cache(parent_id);

        Ok(())
    }

    /// Rename a file or directory
    ///
    /// # NFS-Ganesha Alignment
    /// NFS-Ganesha's fsal_rename() handles:
    /// 1. Pre/post attributes for both source and target directories
    /// 2. Proper cache invalidation
    /// 3. Atomic rename semantics
    ///
    /// Our implementation needs to:
    /// 1. Get old fileid before rename
    /// 2. Perform the rename
    /// 3. Get new fileid after rename (may be different!)
    /// 4. Update path_cache atomically
    /// 5. Invalidate status_cache for affected files
    /// 6. Handle OpenFile if the renamed file is open
    pub async fn rename(
        &self,
        from_parent: Fileid4,
        from_name: &str,
        to_parent: Fileid4,
        to_name: &str,
    ) -> Nfs4Result<()> {
        if self.config.read_only {
            return Err(Nfs4Status::Rofs.into());
        }

        let from_parent_path = self.get_path(from_parent)?;
        let from_path = self.build_child_path(&from_parent_path, from_name)?;
        let from_path_str = from_path.to_string();

        let to_parent_path = self.get_path(to_parent)?;
        let to_path = self.build_child_path(&to_parent_path, to_name)?;
        let to_path_str = to_path.to_string();

        // Step 1: Get old file status (before rename)
        let old_status = self
            .ufs
            .get_status(&from_path)
            .await
            .map_err(Nfs4Error::from)?;
        let old_fileid = get_fileid_from_status(&old_status, &from_path_str);

        // Step 2: Check if target exists (will be overwritten)
        let target_exists = self.ufs.get_status(&to_path).await.is_ok();
        if target_exists {
            // Invalidate target's cache entries
            self.path_cache.remove_by_path(&to_path_str);
        }

        // Step 3: Perform the rename
        self.ufs
            .rename(&from_path, &to_path)
            .await
            .map_err(Nfs4Error::from)?;

        // Step 4: Get new file status (after rename)
        // The fileid may have changed after rename!
        let new_status = self
            .ufs
            .get_status(&to_path)
            .await
            .map_err(Nfs4Error::from)?;
        let new_fileid = get_fileid_from_status(&new_status, &to_path_str);

        // Step 5: Update path_cache atomically
        self.path_cache
            .rename(old_fileid, &from_path_str, &to_path_str, new_fileid);

        // Step 6: Invalidate status_cache for affected files AND parent directories
        // CRITICAL: Must invalidate parent directories' cache so that NFS client
        // sees the correct change_info (before != after) and refreshes its cache
        self.invalidate_status_cache(old_fileid);
        if old_fileid != new_fileid {
            self.invalidate_status_cache(new_fileid);
        }
        // Invalidate source and target parent directories
        self.invalidate_status_cache(from_parent);
        self.invalidate_status_cache(to_parent);

        // Step 7: Handle OpenFile if the renamed file is open
        // If fileid changed, we need to update the open_files HashMap
        if old_fileid != new_fileid {
            let mut open_files = self.open_files.write().unwrap();
            if let Some(open_file) = open_files.remove(&old_fileid) {
                open_files.insert(new_fileid, open_file);
            }
        }

        Ok(())
    }

    /// Create a symbolic link
    pub async fn symlink(
        &self,
        parent_id: Fileid4,
        name: &str,
        target: &str,
    ) -> Nfs4Result<(Fileid4, FileStatus)> {
        if self.config.read_only {
            return Err(Nfs4Status::Rofs.into());
        }

        let parent_path = self.get_path(parent_id)?;
        let link_path = self.build_child_path(&parent_path, name)?;

        self.ufs
            .symlink(target, &link_path, false)
            .await
            .map_err(Nfs4Error::from)?;

        let status = self
            .ufs
            .get_status(&link_path)
            .await
            .map_err(Nfs4Error::from)?;
        let fileid = get_fileid_from_status(&status, &link_path.to_string());

        self.cache_path(fileid, link_path.to_string());

        // Invalidate parent directory's status cache so NFS client sees updated change_info
        self.invalidate_status_cache(parent_id);

        Ok((fileid, status))
    }

    /// Read symbolic link target
    pub async fn readlink(&self, fileid: Fileid4) -> Nfs4Result<String> {
        let status = self.get_status(fileid).await?;

        if status.file_type != FileType::Link {
            return Err(Nfs4Status::Inval.into());
        }

        status.target.ok_or_else(|| Nfs4Status::Inval.into())
    }

    /// Create a hard link
    pub async fn link(
        &self,
        src_id: Fileid4,
        dst_parent: Fileid4,
        dst_name: &str,
    ) -> Nfs4Result<FileStatus> {
        if self.config.read_only {
            return Err(Nfs4Status::Rofs.into());
        }

        let src_path = self.get_path(src_id)?;
        let dst_parent_path = self.get_path(dst_parent)?;
        let dst_path = self.build_child_path(&dst_parent_path, dst_name)?;

        self.ufs
            .link(&src_path, &dst_path)
            .await
            .map_err(Nfs4Error::from)?;

        let status = self
            .ufs
            .get_status(&src_path)
            .await
            .map_err(Nfs4Error::from)?;
        self.cache_path(status.id as u64, dst_path.to_string());

        // Invalidate target parent directory's status cache so NFS client sees updated change_info
        // Also invalidate source file's cache since nlink changed
        self.invalidate_status_cache(dst_parent);
        self.invalidate_status_cache(src_id);

        Ok(status)
    }

    // ========================================================================
    // Utility Methods
    // ========================================================================

    /// Get root file ID
    #[inline]
    pub fn root_fileid(&self) -> Fileid4 {
        ROOT_FILEID
    }

    /// Check if read-only mode
    #[inline]
    pub fn is_read_only(&self) -> bool {
        self.config.read_only
    }

    /// Get UnifiedFileSystem reference (for advanced operations)
    pub fn ufs(&self) -> &UnifiedFileSystem {
        &self.ufs
    }

    // ========================================================================
    // Stateless Read/Write (for special stateids like ANONYMOUS)
    // ========================================================================

    /// Read file data (stateless, for special stateids like ANONYMOUS)
    ///
    /// NFS-Ganesha: Creates temporary reader for each operation
    /// No caching - simple and correct
    pub async fn read(
        &self,
        fileid: Fileid4,
        offset: u64,
        count: u32,
    ) -> Nfs4Result<(Vec<DataSlice>, bool)> {
        let path = self.get_path(fileid)?;

        // Create temporary reader
        let mut reader = self.ufs.open(&path).await.map_err(Nfs4Error::from)?;
        let file_len = reader.len();

        // Check bounds
        if offset >= file_len as u64 {
            return Ok((vec![], true));
        }

        // Calculate read size
        let remaining = file_len as u64 - offset;
        let read_count = count.min(remaining as u32);

        if read_count == 0 {
            return Ok((vec![], true));
        }

        // Read data
        let slices = reader
            .fuse_read(offset as i64, read_count as usize)
            .await
            .map_err(Nfs4Error::from)?;

        // Calculate EOF
        let total_len: usize = slices.iter().map(|s| s.len()).sum();
        let eof = total_len < count as usize || (offset + total_len as u64) >= file_len as u64;

        // Complete reader
        reader.complete().await.map_err(Nfs4Error::from)?;

        Ok((slices, eof))
    }

    /// Write file data (stateless, for special stateids)
    ///
    /// # Stateless Write Behavior
    /// Creates temporary writer and completes immediately
    /// No CLOSE operation for stateless writes
    ///
    /// # Auto-Extend
    /// Like NfsWriter, this method auto-extends the file if writing beyond
    /// current size. This is required for correct NFSv4 WRITE semantics.
    ///
    /// # Note on resize
    /// We must explicitly call resize() before fuse_write() because:
    /// - fuse_write() calls seek() + async_write()
    /// - async_write() calls write_chunk() which does NOT check pos > len
    /// - Only FsWriterBase::write() checks pos > len and calls resize()
    /// This is different from curvine-fuse which uses FsWriterBase::write() directly
    pub async fn write(&self, fileid: Fileid4, offset: u64, data: Vec<u8>) -> Nfs4Result<u32> {
        if self.config.read_only {
            return Err(Nfs4Status::Rofs.into());
        }

        let path = self.get_path(fileid)?;
        let data_len = data.len() as u32;

        // Create temporary writer
        let flags = OpenFlags::new_write_only().set_create(false);
        let opts = self.ufs.cv().create_opts_builder().build();
        let mut writer = self
            .ufs
            .open_with_opts(&path, opts, flags)
            .await
            .map_err(Nfs4Error::from)?;

        // Auto-extend if writing beyond current size
        // This is necessary because fuse_write() -> async_write() -> write_chunk()
        // does NOT check pos > len like FsWriterBase::write() does
        let current_len = writer.status().len;
        let write_end = offset as i64 + data_len as i64;
        if write_end > current_len {
            let alloc_opts =
                curvine_common::state::FileAllocOpts::with_alloc(write_end, Default::default());
            writer.resize(alloc_opts).await.map_err(Nfs4Error::from)?;
        }

        // Write data
        let chunk = DataSlice::bytes(Bytes::from(data));
        writer
            .fuse_write(offset as i64, chunk)
            .await
            .map_err(Nfs4Error::from)?;
        writer.complete().await.map_err(Nfs4Error::from)?;

        // Invalidate status cache
        self.invalidate_status_cache(fileid);

        Ok(data_len)
    }

    /// Set file attributes
    ///
    /// # NFS-Ganesha Reference
    /// Function: fsal_setattr() in fsal_helper.c
    ///
    /// # Truncate Support
    /// When size is set, we use Writer::resize() to truncate/extend the file.
    /// This requires an open Writer, so we get it from OpenFile if available,
    /// or create a temporary one.
    #[allow(clippy::too_many_arguments)]
    pub async fn setattr(
        &self,
        fileid: Fileid4,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<Nfstime4>,
        _mtime: Option<Nfstime4>,
    ) -> Nfs4Result<FileStatus> {
        if self.config.read_only {
            return Err(Nfs4Status::Rofs.into());
        }

        let path = self.get_path(fileid)?;

        // Handle size change (truncate/extend)
        if let Some(new_size) = size {
            let status = self.get_status(fileid).await?;
            if new_size != status.len as u64 {
                // Try to use existing OpenFile's NfsWriter first
                let open_file = self.get_open_file(fileid);
                if let Some(open_file) = open_file {
                    // Clone NfsWriter to avoid holding lock across await
                    let writer = {
                        let writer_guard = open_file.writer.read().unwrap();
                        writer_guard.clone()
                    };

                    if let Some(writer) = writer {
                        let opts = FileAllocOpts::with_truncate(new_size as i64);
                        writer.resize(opts).await.map_err(|e| {
                            tracing::error!("SETATTR: Failed to resize file {}: {:?}", fileid, e);
                            Nfs4Error::from(e)
                        })?;
                    } else {
                        // No Writer in OpenFile, create temporary one
                        self.resize_with_temp_writer(&path, fileid, new_size)
                            .await?;
                    }
                } else {
                    // No OpenFile, create temporary Writer
                    self.resize_with_temp_writer(&path, fileid, new_size)
                        .await?;
                }

                // Invalidate status cache after resize
                self.invalidate_status_cache(fileid);
            }
        }

        // Build SetAttrOpts for mode/owner/group changes
        let opts = curvine_common::state::SetAttrOpts {
            mode,
            // Use function reference directly instead of closure (clippy::redundant_closure)
            owner: uid.and_then(orpc::sys::get_username_by_uid),
            group: gid.and_then(orpc::sys::get_groupname_by_gid),
            ..Default::default()
        };

        // Set attributes if any are specified
        if opts.mode.is_some() || opts.owner.is_some() || opts.group.is_some() {
            self.ufs
                .set_attr(&path, opts)
                .await
                .map_err(Nfs4Error::from)?;
        }

        // Return updated status
        self.get_status(fileid).await
    }

    /// Resize file using a temporary Writer
    ///
    /// Creates a temporary Writer, resizes the file, and completes it.
    /// Used when no OpenFile exists or OpenFile has no Writer.
    async fn resize_with_temp_writer(
        &self,
        path: &Path,
        fileid: Fileid4,
        new_size: u64,
    ) -> Nfs4Result<()> {
        let flags = OpenFlags::new_write_only().set_create(false);
        let opts = self.ufs.cv().create_opts_builder().build();
        let mut writer = self
            .ufs
            .open_with_opts(path, opts, flags)
            .await
            .map_err(Nfs4Error::from)?;

        let alloc_opts = curvine_common::state::FileAllocOpts::with_truncate(new_size as i64);
        writer.resize(alloc_opts).await.map_err(|e| {
            tracing::error!("SETATTR: Failed to resize file {}: {:?}", fileid, e);
            Nfs4Error::from(e)
        })?;

        writer.complete().await.map_err(Nfs4Error::from)?;

        Ok(())
    }
}
