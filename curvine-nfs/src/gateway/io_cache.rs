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

use crate::gateway::nfs_reader::NfsReader;
use curvine_client::unified::{UnifiedReader, UnifiedWriter};
use curvine_common::conf::NfsGatewayConf;
use curvine_common::error::FsError;
use curvine_common::fs::Writer;
use curvine_common::state::{FileBlocks, FileStatus};
use orpc::sync::FastSyncCache;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// I/O cache configuration
#[derive(Debug, Clone)]
pub struct IoCacheConfig {
    pub file_blocks_capacity: u64,
    pub file_blocks_ttl_secs: u64,
    pub reader_capacity: u64,
    pub reader_ttl_secs: u64,
    pub reader_pool_size: usize,
    pub writer_capacity: u64,
    pub writer_ttl_secs: u64,
    pub file_status_capacity: i64, // -1 means disabled
    pub file_status_ttl_secs: u64,
}

impl Default for IoCacheConfig {
    fn default() -> Self {
        Self {
            file_blocks_capacity: 10000,
            file_blocks_ttl_secs: 60,
            reader_capacity: 1000,
            reader_ttl_secs: 300,
            reader_pool_size: 8,
            writer_capacity: 1000,
            writer_ttl_secs: 30,
            file_status_capacity: 10000i64,
            file_status_ttl_secs: 30,
        }
    }
}

impl From<&NfsGatewayConf> for IoCacheConfig {
    fn from(conf: &NfsGatewayConf) -> Self {
        Self {
            file_blocks_capacity: conf.file_blocks_cache_size,
            file_blocks_ttl_secs: conf.file_blocks_cache_ttl_secs,
            reader_capacity: conf.reader_cache_size,
            reader_ttl_secs: conf.reader_cache_ttl_secs,
            reader_pool_size: conf.reader_pool_size,
            writer_capacity: conf.reader_cache_size,
            writer_ttl_secs: conf.writer_idle_timeout_secs,
            file_status_capacity: conf.file_status_cache_size,
            file_status_ttl_secs: conf.file_status_cache_ttl_secs,
        }
    }
}

pub struct ReaderEntry {
    pub reader: tokio::sync::Mutex<NfsReader>,
}

impl ReaderEntry {
    #[inline]
    pub fn new(reader: NfsReader) -> Self {
        Self {
            reader: tokio::sync::Mutex::new(reader),
        }
    }
}

/// Pool of readers for a single file (round-robin selection)
pub struct ReaderPool {
    readers: Vec<Arc<ReaderEntry>>,
    next_idx: std::sync::atomic::AtomicUsize,
}

impl ReaderPool {
    /// Create a new ReaderPool with specified size
    ///
    /// # Performance Optimization (2025-12-30)
    /// Removed Runtime parameter as NfsReader no longer needs background tasks.
    /// Each NfsReader now directly wraps UnifiedReader for better performance.
    pub async fn new<F, Fut>(pool_size: usize, mut factory: F) -> Result<Self, FsError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<UnifiedReader, FsError>>,
    {
        let mut readers = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let unified_reader = factory().await?;
            let nfs_reader = NfsReader::new(unified_reader);
            readers.push(Arc::new(ReaderEntry::new(nfs_reader)));
        }
        Ok(Self {
            readers,
            next_idx: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    #[inline]
    pub fn get(&self) -> Arc<ReaderEntry> {
        let idx = self
            .next_idx
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.readers.len();
        Arc::clone(&self.readers[idx])
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.readers.len()
    }
}

impl Drop for ReaderPool {
    fn drop(&mut self) {}
}

pub struct WriterEntry {
    pub writer: Mutex<UnifiedWriter>,
}

impl WriterEntry {
    #[inline]
    pub fn new(writer: UnifiedWriter) -> Self {
        Self {
            writer: Mutex::new(writer),
        }
    }
}

pub struct IoCache {
    pub file_blocks: FastSyncCache<u64, FileBlocks>,
    pub reader_pools: FastSyncCache<u64, Arc<ReaderPool>>,
    pub writers: FastSyncCache<u64, Arc<WriterEntry>>,
    pub file_status: Option<FastSyncCache<u64, FileStatus>>,
    pub reader_pool_size: usize,
    writer_creation_lock: Mutex<()>,
}

impl IoCache {
    pub fn new(config: IoCacheConfig) -> Self {
        let reader_pool_size = config.reader_pool_size;

        let reader_pools = FastSyncCache::with_eviction_listener(
            config.reader_capacity,
            Duration::from_secs(config.reader_ttl_secs),
            |_key, _pool, _cause| {},
        );

        let writers = FastSyncCache::with_eviction_listener(
            config.writer_capacity,
            Duration::from_secs(config.writer_ttl_secs),
            |_key, entry: Arc<WriterEntry>, _cause| {
                tokio::spawn(async move {
                    let mut writer = entry.writer.lock().await;
                    let _ = writer.complete().await;
                });
            },
        );

        // Create FileStatus cache only if capacity >= 0 (-1 means disabled)
        let file_status = if config.file_status_capacity >= 0 {
            Some(FastSyncCache::new(
                config.file_status_capacity as u64,
                Duration::from_secs(config.file_status_ttl_secs),
            ))
        } else {
            None
        };

        Self {
            file_blocks: FastSyncCache::new(
                config.file_blocks_capacity,
                Duration::from_secs(config.file_blocks_ttl_secs),
            ),
            reader_pools,
            writers,
            file_status,
            reader_pool_size,
            writer_creation_lock: Mutex::new(()),
        }
    }

    /// Get or create writer with double-check locking
    pub async fn get_or_create_writer<F, Fut>(
        &self,
        fileid: u64,
        factory: F,
    ) -> Result<Arc<WriterEntry>, FsError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<UnifiedWriter, FsError>>,
    {
        // Fast path - cache hit (no lock)
        if let Some(entry) = self.writers.get(&fileid) {
            return Ok(entry);
        }

        // Slow path - acquire lock and double-check
        let _guard = self.writer_creation_lock.lock().await;
        if let Some(entry) = self.writers.get(&fileid) {
            return Ok(entry);
        }

        // Create new writer
        let writer = factory().await?;
        let entry = Arc::new(WriterEntry::new(writer));
        self.writers.insert(fileid, Arc::clone(&entry));
        Ok(entry)
    }

    /// Invalidate all caches for a file
    #[inline]
    pub fn invalidate(&self, fileid: u64) {
        self.file_blocks.invalidate(&fileid);
        self.reader_pools.invalidate(&fileid);
    }

    /// Flush writer for a file
    pub async fn flush_writer(&self, fileid: u64) -> Result<(), FsError> {
        if let Some(entry) = self.writers.get(&fileid) {
            entry.writer.lock().await.flush().await?;
        }
        Ok(())
    }

    /// Complete and remove writer for a file
    pub async fn complete_writer(&self, fileid: u64) {
        if let Some(entry) = self.writers.remove(&fileid) {
            let _ = entry.writer.lock().await.complete().await;
        }
        self.file_blocks.invalidate(&fileid);
        self.reader_pools.invalidate(&fileid);
    }

    /// Invalidate file_status cache (called when file is modified)
    pub fn invalidate_file_status(&self, fileid: &u64) {
        if let Some(ref cache) = self.file_status {
            cache.invalidate(fileid);
        }
    }

    /// Insert file_status into cache (if enabled)
    pub fn insert_file_status(&self, fileid: u64, status: FileStatus) {
        if let Some(ref cache) = self.file_status {
            cache.insert(fileid, status);
        }
    }
}
