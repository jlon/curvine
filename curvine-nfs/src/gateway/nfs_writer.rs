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

//! NFS Writer with message queue pattern for sequential write processing
//!
//! Uses a background task to serialize all write operations, ensuring:
//! - Thread-safe concurrent access from multiple NFS requests
//! - Auto-extend when writing beyond current file size
//! - Proper ordering of writes
//!
//! # Performance Note
//! The channel-based approach has lower latency than AsyncMutex because:
//! - Background task is always ready to receive (no lock contention)
//! - Channel send is very fast, only oneshot receive waits for completion

use curvine_client::unified::UnifiedWriter;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::FileAllocOpts;
use curvine_common::FsResult;
use orpc::sys::DataSlice;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_util::bytes::Bytes;

/// Phase 2 Layer 1: Write pattern tracker for small file detection
///
/// Tracks write operations to determine if a file matches small file criteria.
/// Small files can skip flush on WRITE and delay flush to CLOSE for better performance.
#[derive(Debug, Clone)]
pub struct WritePattern {
    /// Number of write operations
    write_count: u32,

    /// Total bytes written
    total_bytes: u64,

    /// Whether this file has switched to large file mode
    /// Once switched, it remains in large file mode (no switching back)
    switched_to_large: bool,
}

impl WritePattern {
    /// Create new empty write pattern
    fn new() -> Self {
        Self {
            write_count: 0,
            total_bytes: 0,
            switched_to_large: false,
        }
    }

    /// Record a write operation
    #[allow(dead_code)]
    fn record_write(&mut self, bytes: usize) {
        self.write_count += 1;
        self.total_bytes += bytes as u64;
    }

    /// Check if this file matches small file pattern
    #[allow(dead_code)]
    pub fn is_small_file(&self, max_writes: u32, max_size: u64) -> bool {
        if self.switched_to_large {
            return false;
        }

        self.write_count <= max_writes && self.total_bytes <= max_size
    }

    /// Check if should switch to large file mode
    #[allow(dead_code)]
    pub fn should_switch_to_large(&self, max_writes: u32, max_size: u64) -> bool {
        self.write_count > max_writes || self.total_bytes > max_size
    }

    /// Mark as switched to large file mode (irreversible)
    #[allow(dead_code)]
    pub fn mark_switched(&mut self) {
        self.switched_to_large = true;
    }

    /// Get write count (for debugging/logging)
    #[allow(dead_code)]
    pub fn write_count(&self) -> u32 {
        self.write_count
    }

    /// Get total bytes (for debugging/logging)
    #[allow(dead_code)]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Check if switched to large mode (for debugging/logging)
    #[allow(dead_code)]
    pub fn is_switched_to_large(&self) -> bool {
        self.switched_to_large
    }
}

/// Write task sent to background worker
enum WriteTask {
    Write {
        offset: i64,
        data: Bytes,
        reply: tokio::sync::oneshot::Sender<FsResult<u32>>,
    },
    Resize {
        opts: FileAllocOpts,
        reply: tokio::sync::oneshot::Sender<FsResult<()>>,
    },
    GetPos {
        reply: tokio::sync::oneshot::Sender<i64>,
    },
    Flush {
        reply: tokio::sync::oneshot::Sender<FsResult<()>>,
    },
    Complete {
        reply: tokio::sync::oneshot::Sender<FsResult<()>>,
    },
}

/// NFS Writer with sequential write processing via message queue
///
/// # Resource Safety
/// NfsWriter uses a background task for sequential write processing.
/// When all clones are dropped, the background task will automatically
/// exit due to channel closure. However, for proper data commit,
/// you should call `complete()` explicitly before dropping.
///
/// # Clone Behavior
/// NfsWriter is Clone, but all clones share the same background task.
/// The `completed` flag is shared via Arc to track completion status.
#[derive(Clone)]
pub struct NfsWriter {
    path: Path,
    sender: mpsc::Sender<WriteTask>,
    /// Track if complete() has been called (shared across clones)
    completed: Arc<std::sync::atomic::AtomicBool>,

    /// Phase 2 Layer 1: Write pattern tracker (shared across clones)
    /// NOTE: Exists but NOT used yet - Phase 1 behavior maintained
    #[allow(dead_code)]
    write_pattern: Arc<Mutex<WritePattern>>,

    /// Phase 2 Layer 2b: Small file config (max_writes, max_size, enabled)
    /// NOTE: Currently disabled (false) to maintain Phase 1 behavior
    #[allow(dead_code)]
    small_file_config: (u32, u64, bool),
}

impl NfsWriter {
    /// Create new NfsWriter with background processing task
    ///
    /// # Arguments
    /// - writer: UnifiedWriter for actual I/O operations
    /// - small_file_config: (max_writes, max_size, enabled) from NfsGatewayConf
    pub fn new(writer: UnifiedWriter, small_file_config: (u32, u64, bool)) -> Self {
        let path = writer.path().clone();
        // Use bounded channel to provide backpressure
        let (sender, receiver) = mpsc::channel(1024);

        // Spawn background task
        tokio::spawn(Self::writer_task(writer, receiver));

        Self {
            path,
            sender,
            completed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            write_pattern: Arc::new(Mutex::new(WritePattern::new())),
            small_file_config,
        }
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Write data at offset (queued for sequential processing)
    ///
    /// # Arguments
    /// - need_sync: true if stable != UNSTABLE4 (following nfs-ganesha semantics)
    ///
    /// # Returns
    /// - (written_bytes, actual_synced): written count and whether sync was performed
    ///
    /// # Flush Decision (following nfs-ganesha + small file optimization)
    /// 1. If need_sync=true (FILE_SYNC4/DATA_SYNC4), always flush → synced=true
    /// 2. If need_sync=false (UNSTABLE4):
    ///    - Small file: skip flush → synced=false (will flush on COMMIT/CLOSE)
    ///    - Large file: flush → synced=true (avoid memory pressure)
    #[inline]
    pub async fn write(&self, offset: i64, data: Vec<u8>, need_sync: bool) -> FsResult<(u32, bool)> {
        let data_len = data.len();
        tracing::info!(
            "NfsWriter.write() ENTRY: offset={} len={} need_sync={} path={}",
            offset, data_len, need_sync, self.path.path()
        );

        // Phase 2: Track write pattern for small file detection
        let (max_writes, max_size, enabled) = self.small_file_config;
        // Extract pattern info with single lock acquisition to avoid deadlock in tracing! macro
        let (is_small, should_switch, write_count, total_bytes) = {
            let mut pattern = self.write_pattern.lock().unwrap();
            pattern.record_write(data_len);

            if enabled {
                let is_small = pattern.is_small_file(max_writes, max_size);
                let should_switch = pattern.should_switch_to_large(max_writes, max_size);
                (is_small, should_switch, pattern.write_count(), pattern.total_bytes())
            } else {
                // Optimization disabled
                (false, false, pattern.write_count(), pattern.total_bytes())
            }
        }; // Lock released here before tracing

        tracing::debug!(
            "WritePattern: enabled={} is_small={} should_switch={} count={} bytes={} path={}",
            enabled, is_small, should_switch, write_count, total_bytes, self.path.path()
        );

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(WriteTask::Write {
                offset,
                data: Bytes::from(data),
                reply: tx,
            })
            .await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?;

        let result = rx
            .await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?;

        // Flush decision: based on both need_sync (stable parameter) and file size
        // Following nfs-ganesha semantics with small file optimization
        let actual_synced = if need_sync {
            // Case 1: Client requested FILE_SYNC4/DATA_SYNC4 - must flush
            tracing::info!(
                "FlushDecision: need_sync=true (FILE_SYNC4/DATA_SYNC4) - flushing path={}",
                self.path.path()
            );
            self.flush().await?;
            true
        } else if !enabled {
            // Case 2: Optimization disabled - always flush
            tracing::info!(
                "FlushDecision: optimization disabled - flushing path={}",
                self.path.path()
            );
            self.flush().await?;
            true
        } else if should_switch {
            // Case 3: Small file switching to large - flush and mark
            tracing::info!(
                "FlushDecision: UNSTABLE4 but should_switch - flushing path={}",
                self.path.path()
            );
            self.write_pattern.lock().unwrap().mark_switched();
            self.flush().await?;
            true
        } else if !is_small {
            // Case 4: Large file with UNSTABLE4 - flush to avoid memory pressure
            tracing::info!(
                "FlushDecision: UNSTABLE4 but large file - flushing path={}",
                self.path.path()
            );
            self.flush().await?;
            true
        } else {
            // Case 5: Small file with UNSTABLE4 - SKIP flush (the optimization!)
            tracing::info!(
                "FlushDecision: UNSTABLE4 + small file - SKIPPING flush path={}",
                self.path.path()
            );
            // Data will be flushed on COMMIT/CLOSE
            false
        };

        result.map(|written| (written, actual_synced))
    }

    /// Resize file
    #[inline]
    pub async fn resize(&self, opts: FileAllocOpts) -> FsResult<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(WriteTask::Resize { opts, reply: tx })
            .await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?;

        rx.await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?
    }

    /// Get current file position (length tracked by writer)
    #[inline]
    pub async fn get_pos(&self) -> Option<i64> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(WriteTask::GetPos { reply: tx })
            .await
            .ok()?;

        rx.await.ok()
    }

    /// Flush buffered data
    #[inline]
    pub async fn flush(&self) -> FsResult<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(WriteTask::Flush { reply: tx })
            .await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?;

        rx.await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?
    }

    /// Complete and close writer
    ///
    /// # Important
    /// This method should be called explicitly to ensure data is committed.
    /// If not called, the background task will exit when the channel is closed,
    /// but data may not be properly committed.
    #[inline]
    pub async fn complete(&self) -> FsResult<()> {
        // Mark as completed to prevent double-complete
        if self
            .completed
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            // Already completed, return Ok
            return Ok(());
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(WriteTask::Complete { reply: tx })
            .await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?;

        rx.await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?
    }

    /// Check if complete() has been called
    #[inline]
    pub fn is_completed(&self) -> bool {
        self.completed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Background task that processes writes sequentially
    async fn writer_task(mut writer: UnifiedWriter, mut receiver: mpsc::Receiver<WriteTask>) {
        let path = writer.path().clone();
        // Check if this writer needs pre-resize (only CacheSyncWriter for S3/object storage)
        let needs_pre_resize = writer.needs_pre_resize();
        tracing::info!(
            "NfsWriter task started for path={}, needs_pre_resize={}",
            path.path(),
            needs_pre_resize
        );

        // Track current file length to avoid repeated status() calls
        let mut current_len = writer.status().len;
        tracing::info!(
            "NfsWriter task: initial current_len={} for path={}",
            current_len,
            path.path()
        );

        while let Some(task) = receiver.recv().await {
            match task {
                WriteTask::Write {
                    offset,
                    data,
                    reply,
                } => {
                    let data_len = data.len() as u32;
                    let write_end = offset + data_len as i64;

                    tracing::info!(
                        "NfsWriter task: WRITE offset={} len={} write_end={} current_len={} path={}",
                        offset,
                        data_len,
                        write_end,
                        current_len,
                        path.path()
                    );

                    // Pre-resize ONLY for CacheSyncWriter (S3/object storage paths)
                    // This is REQUIRED for CacheSyncWriter because:
                    // 1. fuse_write() calls seek() + async_write()
                    // 2. async_write() calls write_chunk() which does NOT check pos > len
                    // 3. Only FsWriterBase::write() checks pos > len and calls resize()
                    // 4. Without resize, FsWriterBase::len stays at 0, causing complete_file
                    //    to report wrong file size to master
                    //
                    // For FsWriter (direct curvine), pre-resize is NOT needed because:
                    // - FsWriterBase::write() already handles pos > len check and resize
                    // - Pre-resize would interfere with normal write flow
                    if needs_pre_resize && write_end > current_len {
                        let alloc_opts = FileAllocOpts::with_alloc(write_end, Default::default());
                        tracing::info!(
                            "NfsWriter task: Pre-resize to {} for path={}",
                            write_end,
                            path.path()
                        );
                        if let Err(e) = writer.resize(alloc_opts).await {
                            tracing::error!(
                                "NfsWriter task: Pre-resize failed: {:?} for path={}",
                                e,
                                path.path()
                            );
                            let _ = reply.send(Err(e));
                            continue;
                        }
                        current_len = write_end;
                    }

                    tracing::info!(
                        "NfsWriter task: Calling fuse_write offset={} len={} for path={}",
                        offset,
                        data_len,
                        path.path()
                    );
                    let res = writer
                        .fuse_write(offset, DataSlice::Bytes(data))
                        .await
                        .map(|_| data_len);

                    match &res {
                        Ok(written) => {
                            tracing::info!(
                                "NfsWriter task: fuse_write completed, written={} for path={}",
                                written,
                                path.path()
                            );
                            // Update current_len for non-pre-resize writers after successful write
                            if !needs_pre_resize && write_end > current_len {
                                current_len = write_end;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "NfsWriter task: fuse_write failed: {:?} for path={}",
                                e,
                                path.path()
                            );
                        }
                    }

                    let _ = reply.send(res);
                }

                WriteTask::Resize { opts, reply } => {
                    let new_len = opts.len;
                    tracing::info!(
                        "NfsWriter task: RESIZE to {} for path={}",
                        new_len,
                        path.path()
                    );

                    let res = writer.resize(opts).await;
                    match &res {
                        Ok(_) => {
                            current_len = new_len;
                            tracing::info!(
                                "NfsWriter task: Resize succeeded, current_len={} for path={}",
                                current_len,
                                path.path()
                            );
                        }
                        Err(e) => {
                            // Some Writer implementations (e.g., CacheSyncWriter for S3) don't support resize
                            // For these cases, we update current_len and let the actual truncation happen
                            // during complete() when the file is finalized
                            tracing::warn!(
                                "NfsWriter task: Resize not supported ({}), updating current_len={} for path={}",
                                e,
                                new_len,
                                path.path()
                            );
                            current_len = new_len;
                        }
                    }
                    // Always return Ok to avoid breaking the write flow
                    // The actual file size will be set correctly during complete()
                    let _ = reply.send(Ok(()));
                }

                WriteTask::GetPos { reply } => {
                    let _ = reply.send(current_len);
                }

                WriteTask::Flush { reply } => {
                    tracing::info!("NfsWriter task: FLUSH for path={}", path.path());
                    let _ = reply.send(writer.flush().await);
                }

                WriteTask::Complete { reply } => {
                    tracing::info!("NfsWriter task: COMPLETE for path={}", path.path());
                    let _ = reply.send(writer.complete().await);
                    break;
                }
            }
        }

        tracing::info!("NfsWriter task exited for path={}", path.path());
    }
}
