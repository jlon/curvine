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
use curvine_common::state::{FileAllocMode, FileAllocOpts};
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
        // Track current file length to avoid repeated status() calls
        let mut current_len = writer.status().len;

        while let Some(task) = receiver.recv().await {
            match task {
                WriteTask::Write { offset, data, reply } => {
                    let data_len = data.len() as u32;
                    let write_end = offset + data_len as i64;

                    // Auto-extend if writing beyond current size
                    if write_end > current_len {
                        let opts = FileAllocOpts::with_alloc(write_end, FileAllocMode::DEFAULT);
                        if let Err(e) = writer.resize(opts).await {
                            let _ = reply.send(Err(e));
                            continue;
                        }
                        current_len = write_end;
                    }

                    let res = writer
                        .fuse_write(offset, DataSlice::Bytes(data))
                        .await
                        .map(|_| data_len);
                    let _ = reply.send(res);
                }

                WriteTask::Resize { opts, reply } => {
                    let new_len = opts.len;
                    let res = writer.resize(opts).await;
                    if res.is_ok() {
                        current_len = new_len;
                    }
                    let _ = reply.send(res);
                }

                WriteTask::GetPos { reply } => {
                    let _ = reply.send(current_len);
                }

                WriteTask::Flush { reply } => {
                    let _ = reply.send(writer.flush().await);
                }

                WriteTask::Complete { reply } => {
                    let _ = reply.send(writer.complete().await);
                    break;
                }
            }
        }
    }
}
