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

//! Curvine NFS FileSystem implementation
//!
//! Implements the NFSFileSystem trait to expose Curvine's UnifiedFileSystem
//! through the NFSv3 protocol with I/O caching optimization.

use crate::gateway::error::NfsError;
use crate::gateway::io_cache::{IoCache, IoCacheConfig, ReaderPool};
use crate::gateway::path_cache::PathCache;
use crate::gateway::uid_gid::{resolve_gid, resolve_uid};
use crate::nfs::{
    fattr3, fileid3, filename3, ftype3, nfs_fh3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
};
use crate::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use async_trait::async_trait;
use curvine_client::unified::{UnifiedFileSystem, UnifiedWriter};
use curvine_common::conf::{ClusterConf, NfsGatewayConf};
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, Writer};
use curvine_common::state::{FileType, OpenFlags};
use orpc::runtime::Runtime;
use orpc::sys::DataSlice;
use std::cmp::Ordering;
use std::sync::Arc;
use tokio_util::bytes::Bytes;

/// Root directory fileid (same as curvine-fuse FS_ROOT_ID)
const FS_ROOT_ID: fileid3 = 1000;

/// Curvine NFS FileSystem with I/O caching
pub struct CurvineNfsFileSystem {
    /// Unified file system client
    ufs: UnifiedFileSystem,
    /// Gateway configuration
    config: NfsGatewayConf,
    /// Effective cluster generation (computed once at startup)
    cluster_generation: u64,
    /// Path cache for fileid -> path mapping
    path_cache: PathCache,
    /// I/O cache for read/write optimization
    io_cache: Arc<IoCache>,
    /// Runtime for spawning background tasks
    runtime: Arc<Runtime>,
}

impl CurvineNfsFileSystem {
    /// Create a new CurvineNfsFileSystem
    pub fn new(
        cluster_conf: ClusterConf,
        gateway_config: NfsGatewayConf,
        runtime: Arc<Runtime>,
    ) -> Result<Self, FsError> {
        let cluster_generation =
            gateway_config.effective_cluster_generation(&cluster_conf.cluster_id);
        let ufs = UnifiedFileSystem::with_rt(cluster_conf, runtime.clone())?;
        let runtime = runtime.clone();

        let path_cache = PathCache::new(
            gateway_config.path_cache_size,
            gateway_config.path_cache_ttl(),
            FS_ROOT_ID,
        );

        let io_cache = Arc::new(IoCache::new(IoCacheConfig::from(&gateway_config)));

        Ok(Self {
            ufs,
            config: gateway_config,
            cluster_generation,
            path_cache,
            io_cache,
            runtime,
        })
    }

    /// Get path from fileid using cache
    fn get_path(&self, id: fileid3) -> Result<Path, nfsstat3> {
        if id == FS_ROOT_ID {
            return Path::new("/").map_err(|_| nfsstat3::NFS3ERR_STALE);
        }
        self.path_cache
            .get(id)
            .and_then(|p| Path::new(&p).ok())
            .ok_or(nfsstat3::NFS3ERR_STALE)
    }

    /// Cache path for fileid
    #[inline]
    fn cache_path(&self, id: fileid3, path: String) {
        self.path_cache.insert(id, path);
    }

    /// Get file status with caching support
    /// This is the unified entry point for all get_status calls to ensure caching works
    /// First tries to find fileid from path_cache, then checks file_status cache
    async fn get_status_cached(
        &self,
        path: &Path,
    ) -> Result<curvine_common::state::FileStatus, nfsstat3> {
        let path_str = path.to_string();

        // Note: We can't efficiently check cache here because we only have path (not fileid).
        // The cache will be populated after fetch, and subsequent getattr() calls (which have fileid)
        // will benefit from the cache.

        // Fetch from server
        let status = self
            .ufs
            .get_status(path)
            .await
            .map_err(Self::fs_error_to_nfs)?;

        // Cache the result if cache is enabled
        let fileid = status.id as u64;
        self.cache_path(fileid, path_str);
        self.io_cache.insert_file_status(fileid, status.clone());

        Ok(status)
    }

    /// Get or create writer for a file, handling file creation if needed
    /// This is a helper method to avoid code duplication in write operations
    async fn get_or_create_writer_for_path(
        &self,
        _fileid: fileid3,
        path: &Path,
    ) -> Result<UnifiedWriter, FsError> {
        let flags = OpenFlags::new_write_only().set_create(false);
        let opts = self.ufs.cv().create_opts_builder().build();

        // Try to open existing file
        match self.ufs.open_with_opts(path, opts.clone(), flags).await {
            Ok(writer) => Ok(writer),
            Err(_) => {
                // File doesn't exist, create it
                let mut w = self.ufs.create(path, true).await?;
                w.complete().await?;

                // Cache the newly created file's status
                let status = self.ufs.get_status(path).await?;
                let new_fileid = status.id as u64;
                self.cache_path(new_fileid, path.to_string());
                self.io_cache.insert_file_status(new_fileid, status.clone());

                // Open the newly created file for writing
                self.ufs.open_with_opts(path, opts, flags).await
            }
        }
    }

    /// Convert FsError to nfsstat3
    #[inline]
    fn fs_error_to_nfs(e: FsError) -> nfsstat3 {
        NfsError::from(e).stat()
    }

    /// Convert FileStatus to fattr3
    fn status_to_fattr3(&self, status: &curvine_common::state::FileStatus) -> fattr3 {
        fattr3 {
            ftype: Self::file_type_to_nfs(&status.file_type),
            mode: status.mode,
            nlink: status.nlink,
            uid: resolve_uid(&status.owner, self.config.default_uid),
            gid: resolve_gid(&status.group, self.config.default_gid),
            size: status.len as u64,
            used: ((status.len + 511) / 512 * 512) as u64,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: status.id as u64,
            atime: Self::to_nfstime3(status.atime),
            mtime: Self::to_nfstime3(status.mtime),
            ctime: Self::to_nfstime3(status.mtime),
        }
    }

    #[inline]
    fn file_type_to_nfs(ft: &FileType) -> ftype3 {
        match ft {
            FileType::File => ftype3::NF3REG,
            FileType::Dir => ftype3::NF3DIR,
            FileType::Link => ftype3::NF3LNK,
            _ => ftype3::NF3REG,
        }
    }

    #[inline]
    fn to_nfstime3(ms: i64) -> nfstime3 {
        nfstime3 {
            seconds: (ms / 1000) as u32,
            nseconds: ((ms % 1000) * 1_000_000) as u32,
        }
    }

    fn build_child_path(&self, parent: &Path, name: &str) -> Result<Path, nfsstat3> {
        let child_str = if parent.path() == "/" {
            format!("/{name}")
        } else {
            format!("{}/{}", parent.path(), name)
        };
        Path::new(&child_str).map_err(|_| nfsstat3::NFS3ERR_INVAL)
    }

    #[inline]
    fn root_path() -> Path {
        match Path::new("/") {
            Ok(p) => p,
            Err(_) => unreachable!("Root path '/' should always be valid"),
        }
    }
}

#[async_trait]
impl NFSFileSystem for CurvineNfsFileSystem {
    fn capabilities(&self) -> VFSCapabilities {
        if self.config.read_only {
            VFSCapabilities::ReadOnly
        } else {
            VFSCapabilities::ReadWrite
        }
    }

    fn root_dir(&self) -> fileid3 {
        FS_ROOT_ID
    }

    fn id_to_fh(&self, id: fileid3) -> nfs_fh3 {
        let mut data = Vec::with_capacity(16);
        data.extend_from_slice(&self.cluster_generation.to_le_bytes());
        data.extend_from_slice(&id.to_le_bytes());
        nfs_fh3 { data }
    }

    fn fh_to_id(&self, fh: &nfs_fh3) -> Result<fileid3, nfsstat3> {
        if fh.data.len() != 16 {
            return Err(nfsstat3::NFS3ERR_BADHANDLE);
        }
        let gen = u64::from_le_bytes(
            fh.data[0..8]
                .try_into()
                .map_err(|_| nfsstat3::NFS3ERR_BADHANDLE)?,
        );
        let id = u64::from_le_bytes(
            fh.data[8..16]
                .try_into()
                .map_err(|_| nfsstat3::NFS3ERR_BADHANDLE)?,
        );
        match gen.cmp(&self.cluster_generation) {
            Ordering::Less => Err(nfsstat3::NFS3ERR_STALE),
            Ordering::Greater => Err(nfsstat3::NFS3ERR_BADHANDLE),
            Ordering::Equal => Ok(id),
        }
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let parent_path = self.get_path(dirid)?;
        let name = std::str::from_utf8(filename).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;

        if name == "." {
            return Ok(dirid);
        }
        if name == ".." {
            let parent = parent_path
                .parent()
                .ok()
                .flatten()
                .unwrap_or_else(Self::root_path);
            let status = self.get_status_cached(&parent).await?;
            return Ok(status.id as u64);
        }

        let child_path = self.build_child_path(&parent_path, name)?;
        let status = self.get_status_cached(&child_path).await?;
        let fileid = status.id as u64;

        Ok(fileid)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        // Try cache first if enabled (fast path for frequent getattr calls)
        if let Some(ref cache) = self.io_cache.file_status {
            if let Some(status) = cache.get(&id) {
                return Ok(self.status_to_fattr3(&status));
            }
        }

        // Cache miss or cache disabled - fetch from server
        let path = self.get_path(id)?;
        let status = self.get_status_cached(&path).await?;

        Ok(self.status_to_fattr3(&status))
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        if self.config.read_only {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let path = self.get_path(id)?;
        let mut opts = curvine_common::state::SetAttrOpts::default();
        if let crate::nfs::set_mode3::mode(mode) = setattr.mode {
            opts.mode = Some(mode);
        }
        // Invalidate FileStatus cache (attributes changed)
        self.io_cache.invalidate_file_status(&id);

        if let Some(status) = self
            .ufs
            .fuse_set_attr(&path, opts)
            .await
            .map_err(Self::fs_error_to_nfs)?
        {
            // Cache the updated status
            self.io_cache.insert_file_status(id, status.clone());
            Ok(self.status_to_fattr3(&status))
        } else {
            let status = self.get_status_cached(&path).await?;
            Ok(self.status_to_fattr3(&status))
        }
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<DataSlice>, bool), nfsstat3> {
        let path = self.get_path(id)?;

        // Get or create cached FileBlocks using moka
        let file_blocks = match self.io_cache.file_blocks.get(&id) {
            Some(blocks) => blocks,
            None => {
                // Cache miss - fetch from server
                let blocks = self
                    .ufs
                    .cv()
                    .get_block_locations(&path)
                    .await
                    .map_err(Self::fs_error_to_nfs)?;
                self.io_cache.file_blocks.insert(id, blocks.clone());
                blocks
            }
        };

        let mut file_len = file_blocks.status.len;

        // Handle empty files
        if file_len == 0 {
            return Ok((vec![], true));
        }

        let file_blocks = if offset >= file_len as u64 {
            self.io_cache.file_blocks.invalidate(&id);
            let fresh_blocks = self
                .ufs
                .cv()
                .get_block_locations(&path)
                .await
                .map_err(Self::fs_error_to_nfs)?;
            file_len = fresh_blocks.status.len;
            self.io_cache.file_blocks.insert(id, fresh_blocks.clone());
            fresh_blocks
        } else {
            file_blocks
        };

        // If still beyond file size after refresh, return empty data
        // This prevents calling fuse_read with offset >= file_len, which would cause
        // seek(file_len) to fail in get_read_block
        if offset >= file_len as u64 {
            return Ok((vec![], true));
        }

        // Clamp count to remaining bytes to avoid reading beyond file
        let remaining = file_len as u64 - offset;
        let read_count = count.min(remaining as u32);

        // Early return if no data to read
        if read_count == 0 {
            return Ok((vec![], true));
        }

        // Get or create reader pool for this file
        // If FileBlocks were refreshed, invalidate reader pool to force recreation with new FileBlocks
        // This is similar to FUSE's behavior of refreshing the reader when offset >= len
        if offset >= file_blocks.status.len as u64 {
            self.io_cache.reader_pools.invalidate(&id);
        }

        let pool = match self.io_cache.reader_pools.get(&id) {
            Some(p) => p,
            None => {
                let pool_size = self.io_cache.reader_pool_size;
                let p = path.clone();
                let ufs = self.ufs.clone();
                // Use UnifiedReader to create NfsReader (similar to FuseReader)
                let pool = ReaderPool::new(pool_size, || async {
                    // Create UnifiedReader, which will be wrapped by NfsReader
                    ufs.open(&p).await
                })
                .await
                .map_err(Self::fs_error_to_nfs)?;
                let pool = Arc::new(pool);
                self.io_cache.reader_pools.insert(id, Arc::clone(&pool));
                pool
            }
        };

        // Get a reader from pool (round-robin)
        let entry = pool.get();

        // Lock the reader for exclusive access
        let mut reader = entry.reader.lock().await;

        // Read data through reader (NfsReader is thread-safe via Mutex)
        // Critical: fuse_read will call seek(offset) internally, which will call get_read_block(offset)
        // get_read_block fails if offset >= file_len because there's no block at file end
        // We must ensure offset < file_len before calling fuse_read
        // Also, we need to ensure read_count doesn't exceed remaining bytes
        // Important: reader.len() is set when reader is created from file_blocks.status.len
        // If file_blocks were refreshed above, we need to ensure reader pool uses fresh FileBlocks
        let slices = {
            // Final safety check: ensure offset is within valid range
            // reader.len() comes from file_blocks.status.len when reader was created
            // If FileBlocks were refreshed above, reader pool should have been invalidated
            // But to be safe, check reader.len() against current file_len
            let reader_len = reader.len();

            // If reader's len doesn't match current file_len, reader might be using stale FileBlocks
            // This can happen if FileBlocks were refreshed but reader pool wasn't invalidated
            if offset >= reader_len as u64 || reader_len != file_len {
                // Reader's len is stale, refresh FileBlocks and retry
                // Drop the lock before invalidating
                drop(reader);

                self.io_cache.file_blocks.invalidate(&id);
                self.io_cache.reader_pools.invalidate(&id);

                // Re-fetch FileBlocks
                let fresh_blocks = self
                    .ufs
                    .cv()
                    .get_block_locations(&path)
                    .await
                    .map_err(Self::fs_error_to_nfs)?;
                let fresh_len = fresh_blocks.status.len;
                self.io_cache.file_blocks.insert(id, fresh_blocks.clone());

                // If still beyond file size, return empty
                if offset >= fresh_len as u64 {
                    return Ok((vec![], true));
                }

                // Re-create reader pool with fresh FileBlocks
                let pool_size = self.io_cache.reader_pool_size;
                let p = path.clone();
                let ufs = self.ufs.clone();
                // Use UnifiedReader to create NfsReader (similar to FuseReader)
                let pool = ReaderPool::new(pool_size, || async { ufs.open(&p).await })
                    .await
                    .map_err(Self::fs_error_to_nfs)?;
                let pool = Arc::new(pool);
                self.io_cache.reader_pools.insert(id, Arc::clone(&pool));

                // Get reader and retry
                let entry = pool.get();
                let mut reader = entry.reader.lock().await;
                let final_len = reader.len();
                if offset >= final_len as u64 {
                    return Ok((vec![], true));
                }
                let final_remaining = final_len as u64 - offset;
                let final_read_count = count.min(final_remaining as u32);
                if final_read_count == 0 {
                    return Ok((vec![], true));
                }
                reader
                    .fuse_read(offset as i64, final_read_count as usize)
                    .await
                    .map_err(|e| {
                        self.io_cache.file_blocks.invalidate(&id);
                        self.io_cache.reader_pools.invalidate(&id);
                        Self::fs_error_to_nfs(e)
                    })?
            } else {
                // Normal path: offset is within reader's valid range
                // Ensure read_count doesn't exceed remaining bytes
                let remaining = reader_len as u64 - offset;
                let safe_read_count = read_count.min(remaining as u32);
                if safe_read_count == 0 {
                    return Ok((vec![], true));
                }
                reader
                    .fuse_read(offset as i64, safe_read_count as usize)
                    .await
                    .map_err(|e| {
                        self.io_cache.file_blocks.invalidate(&id);
                        self.io_cache.reader_pools.invalidate(&id);
                        Self::fs_error_to_nfs(e)
                    })?
            }
        };

        // Calculate EOF
        let total_len: usize = slices.iter().map(|s| s.len()).sum();
        let eof = total_len < count as usize || (offset as i64 + total_len as i64) >= file_len;

        Ok((slices, eof))
    }

    /// Write with cached writer - single writer per file (Curvine limitation)
    ///
    /// Uses direct Mutex-protected writes for iodepth=1 optimization.
    /// For iodepth>1, writes are serialized which may cause performance issues.
    async fn write(&self, id: fileid3, offset: u64, data: Vec<u8>) -> Result<u32, nfsstat3> {
        if self.config.read_only {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }

        let path = match self.get_path(id) {
            Ok(p) => p,
            Err(nfsstat3::NFS3ERR_STALE) => {
                if id == FS_ROOT_ID {
                    return Err(nfsstat3::NFS3ERR_STALE); // Cannot write to root
                } else {
                    tracing::warn!(
                        "NFS write: path cache miss for fileid={}, returning STALE",
                        id
                    );
                    return Err(nfsstat3::NFS3ERR_STALE);
                }
            }
            Err(e) => return Err(e),
        };

        let data_len = data.len() as u32;

        // Get or create writer entry (with double-check locking)
        let entry = self
            .io_cache
            .get_or_create_writer(id, || async {
                self.get_or_create_writer_for_path(id, &path).await
            })
            .await
            .map_err(Self::fs_error_to_nfs)?;

        {
            let mut writer = entry.writer.lock().await;
            let chunk = DataSlice::bytes(Bytes::from(data));

            let current_pos = writer.pos();
            if offset as i64 == current_pos {
                writer
                    .async_write(chunk)
                    .await
                    .map_err(Self::fs_error_to_nfs)?;
            } else {
                writer
                    .fuse_write(offset as i64, chunk)
                    .await
                    .map_err(Self::fs_error_to_nfs)?;
            }
        }

        // Invalidate caches (file size/status may have changed)
        self.io_cache.file_blocks.invalidate(&id);
        self.io_cache.invalidate_file_status(&id);

        Ok(data_len)
    }

    /// Commit - flush writer to storage
    async fn commit(&self, id: fileid3) -> Result<(), nfsstat3> {
        self.io_cache
            .flush_writer(id)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        Ok(())
    }

    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        if self.config.read_only {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let parent_path = self.get_path(dirid)?;
        let name = std::str::from_utf8(filename).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let child_path = self.build_child_path(&parent_path, name)?;

        let mut writer = self
            .ufs
            .create(&child_path, true)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        writer.complete().await.map_err(Self::fs_error_to_nfs)?;

        let status = self
            .ufs
            .get_status(&child_path)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        let fileid = status.id as u64;
        self.cache_path(fileid, child_path.to_string());
        // Cache the new file's status
        self.io_cache.insert_file_status(fileid, status.clone());
        Ok((fileid, self.status_to_fattr3(&status)))
    }

    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        if self.config.read_only {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let parent_path = self.get_path(dirid)?;
        let name = std::str::from_utf8(filename).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let child_path = self.build_child_path(&parent_path, name)?;

        if self.ufs.exists(&child_path).await.unwrap_or(false) {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }

        let mut writer = self
            .ufs
            .create(&child_path, false)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        writer.complete().await.map_err(Self::fs_error_to_nfs)?;

        let status = self
            .ufs
            .get_status(&child_path)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        self.cache_path(status.id as u64, child_path.to_string());
        Ok(status.id as u64)
    }

    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        if self.config.read_only {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let parent_path = self.get_path(dirid)?;
        let name = std::str::from_utf8(dirname).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let child_path = self.build_child_path(&parent_path, name)?;

        self.ufs
            .mkdir(&child_path, false)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        let status = self
            .ufs
            .get_status(&child_path)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        let fileid = status.id as u64;
        self.cache_path(fileid, child_path.to_string());
        // Cache the new file's status
        self.io_cache.insert_file_status(fileid, status.clone());
        Ok((fileid, self.status_to_fattr3(&status)))
    }

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        if self.config.read_only {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let parent_path = self.get_path(dirid)?;
        let name = std::str::from_utf8(filename).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let child_path = self.build_child_path(&parent_path, name)?;

        // Cleanup caches before deletion
        if let Ok(status) = self.get_status_cached(&child_path).await {
            let fileid = status.id as u64;
            self.path_cache.remove(fileid);
            self.io_cache.invalidate(fileid);
            self.io_cache.invalidate_file_status(&fileid);
        }

        self.ufs
            .delete(&child_path, false)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        Ok(())
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        if self.config.read_only {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let from_parent = self.get_path(from_dirid)?;
        let from_name = std::str::from_utf8(from_filename).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let from_path = self.build_child_path(&from_parent, from_name)?;

        let to_parent = self.get_path(to_dirid)?;
        let to_name = std::str::from_utf8(to_filename).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let to_path = self.build_child_path(&to_parent, to_name)?;

        let old_status = self
            .ufs
            .get_status(&from_path)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        let fileid = old_status.id as u64;

        // Invalidate caches
        self.io_cache.invalidate(fileid);
        self.io_cache.invalidate_file_status(&fileid);

        self.ufs
            .rename(&from_path, &to_path)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        self.cache_path(fileid, to_path.to_string());
        Ok(())
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let path = self.get_path(dirid)?;
        let entries = self
            .ufs
            .list_status(&path)
            .await
            .map_err(Self::fs_error_to_nfs)?;

        let mut sorted: Vec<_> = entries.iter().collect();
        sorted.sort_by_key(|s| s.id);

        let result_entries: Vec<DirEntry> = sorted
            .into_iter()
            .filter(|s| s.id as u64 > start_after)
            .take(max_entries)
            .map(|status| {
                let child_path = if path.path() == "/" {
                    format!("/{}", status.name)
                } else {
                    format!("{}/{}", path.path(), status.name)
                };
                self.cache_path(status.id as u64, child_path);
                DirEntry {
                    fileid: status.id as u64,
                    name: status.name.as_bytes().to_vec().into(),
                    attr: self.status_to_fattr3(status),
                }
            })
            .collect();

        let end = result_entries.len() < max_entries;
        Ok(ReadDirResult {
            entries: result_entries,
            end,
        })
    }

    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        if self.config.read_only {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let parent_path = self.get_path(dirid)?;
        let name = std::str::from_utf8(linkname).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let link_path = self.build_child_path(&parent_path, name)?;
        let target = std::str::from_utf8(symlink).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;

        self.ufs
            .symlink(target, &link_path, false)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        let status = self
            .ufs
            .get_status(&link_path)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        self.cache_path(status.id as u64, link_path.to_string());
        Ok((status.id as u64, self.status_to_fattr3(&status)))
    }

    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        let path = self.get_path(id)?;
        let status = self
            .ufs
            .get_status(&path)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        if status.file_type != FileType::Link {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        match &status.target {
            Some(target) => Ok(target.as_bytes().to_vec().into()),
            None => Err(nfsstat3::NFS3ERR_INVAL),
        }
    }

    async fn link(
        &self,
        id: fileid3,
        dirid: fileid3,
        name: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        if self.config.read_only {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let src_path = self.get_path(id)?;
        let dir_path = self.get_path(dirid)?;
        let name_str = std::str::from_utf8(name).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let dst_path = self.build_child_path(&dir_path, name_str)?;

        self.ufs
            .link(&src_path, &dst_path)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        let status = self
            .ufs
            .get_status(&src_path)
            .await
            .map_err(Self::fs_error_to_nfs)?;
        self.cache_path(status.id as u64, dst_path.to_string());
        Ok((status.id as u64, self.status_to_fattr3(&status)))
    }

    async fn fsinfo(&self, root_fileid: fileid3) -> Result<crate::nfs::fsinfo3, nfsstat3> {
        let dir_attr = match self.getattr(root_fileid).await {
            Ok(v) => crate::nfs::post_op_attr::attributes(v),
            Err(_) => crate::nfs::post_op_attr::Void,
        };
        Ok(crate::nfs::fsinfo3 {
            obj_attributes: dir_attr,
            rtmax: self.config.max_read_size,
            rtpref: self.config.max_read_size,
            rtmult: 4096,
            wtmax: self.config.max_write_size,
            wtpref: self.config.max_write_size,
            wtmult: 4096,
            dtpref: 8192,
            maxfilesize: 128 * 1024 * 1024 * 1024,
            time_delta: nfstime3 {
                seconds: 0,
                nseconds: 1000000,
            },
            properties: crate::nfs::FSF_SYMLINK
                | crate::nfs::FSF_HOMOGENEOUS
                | crate::nfs::FSF_CANSETTIME,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_nfstime3() {
        let time = CurvineNfsFileSystem::to_nfstime3(1735084800000);
        assert_eq!(time.seconds, 1735084800);
        assert_eq!(time.nseconds, 0);
    }

    #[test]
    fn test_file_type_to_nfs() {
        assert!(matches!(
            CurvineNfsFileSystem::file_type_to_nfs(&FileType::File),
            ftype3::NF3REG
        ));
        assert!(matches!(
            CurvineNfsFileSystem::file_type_to_nfs(&FileType::Dir),
            ftype3::NF3DIR
        ));
    }
}
