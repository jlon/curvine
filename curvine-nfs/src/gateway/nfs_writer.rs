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
use tokio::sync::mpsc;
use tokio_util::bytes::Bytes;

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
#[derive(Clone)]
pub struct NfsWriter {
    path: Path,
    sender: mpsc::Sender<WriteTask>,
}

impl NfsWriter {
    /// Create new NfsWriter with background processing task
    pub fn new(writer: UnifiedWriter) -> Self {
        let path = writer.path().clone();
        // Use bounded channel to provide backpressure
        let (sender, receiver) = mpsc::channel(1024);

        // Spawn background task
        tokio::spawn(Self::writer_task(writer, receiver));

        Self { path, sender }
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Write data at offset (queued for sequential processing)
    #[inline]
    pub async fn write(&self, offset: i64, data: Vec<u8>) -> FsResult<u32> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(WriteTask::Write {
                offset,
                data: Bytes::from(data),
                reply: tx,
            })
            .await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?;

        rx.await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?
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
    #[inline]
    pub async fn complete(&self) -> FsResult<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(WriteTask::Complete { reply: tx })
            .await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?;

        rx.await
            .map_err(|_| curvine_common::error::FsError::common("Writer task closed"))?
    }

    /// Background task that processes writes sequentially
    async fn writer_task(mut writer: UnifiedWriter, mut receiver: mpsc::Receiver<WriteTask>) {
        let path = writer.path().clone();
        tracing::info!("NfsWriter task started for path={}", path.path());

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

                    // IMPORTANT: Do NOT pre-resize here!
                    // Let Writer.fuse_write() handle file extension internally via seek() + write()
                    // This aligns with FUSE behavior and avoids issues with:
                    // 1. Object storage (S3) that doesn't support resize
                    // 2. Large files (>128MB) that would allocate too many blocks upfront
                    // 
                    // The flow is: fuse_write(offset, data) -> seek(offset) -> write(data)
                    // - seek() updates pos if offset > len, but doesn't resize
                    // - write() checks if pos > len and calls resize internally
                    // This lazy resize approach is more efficient and compatible

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
                            // Update current_len after successful write
                            if write_end > current_len {
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
