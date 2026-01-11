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
}

impl NfsWriter {
    /// Create new NfsWriter with background processing task
    pub fn new(writer: UnifiedWriter) -> Self {
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
        }
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Write data at offset (queued for sequential processing)
    #[inline]
    pub async fn write(&self, offset: i64, data: Vec<u8>) -> FsResult<u32> {
        let data_len = data.len();

        // Phase 2 Layer 2a: Record write pattern但不改变行为
        {
            let mut pattern = self.write_pattern.lock().unwrap();
            pattern.record_write(data_len);
            tracing::debug!(
                "WritePattern: count={} bytes={} path={}",
                pattern.write_count(),
                pattern.total_bytes(),
                self.path.path()
            );
        }  // Mutex lock released here

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

        // Phase 1 behavior: Always flush after write
        self.flush().await?;

        result
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
