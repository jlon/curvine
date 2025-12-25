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

//! NfsReader: A wrapper around UnifiedReader that uses message queue mechanism
//! Similar to FuseReader, ensuring each read request is independent
//! This prevents background prefetch tasks from seeking beyond file boundaries

use curvine_client::unified::UnifiedReader;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Reader};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender};
use orpc::sync::ErrorMonitor;
use orpc::sys::DataSlice;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::error;

enum ReadTask {
    Read(i64, usize, oneshot::Sender<FsResult<Vec<DataSlice>>>),
    Complete(oneshot::Sender<FsResult<()>>),
}

pub struct NfsReader {
    path: Path,
    len: i64,
    sender: AsyncSender<ReadTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    status: FileStatus,
}

impl NfsReader {
    pub fn new(rt: Arc<Runtime>, reader: UnifiedReader) -> Self {
        let path = reader.path().clone();
        let len = reader.len();
        let err_monitor = Arc::new(ErrorMonitor::new());
        let (sender, receiver) = AsyncChannel::new(1000).split();
        let status = reader.status().clone();

        let monitor = err_monitor.clone();
        rt.spawn(async move {
            let res = Self::read_future(reader, receiver).await;
            match res {
                Ok(_) => (),
                Err(e) => {
                    error!("nfs reader error: {}", e);
                    monitor.set_error(e);
                }
            }
        });

        Self {
            path,
            len,
            sender,
            err_monitor,
            status,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    fn check_error(&self, e: FsError) -> FsError {
        self.err_monitor.take_error().unwrap_or(e)
    }

    /// Read data at specific offset and length
    /// This ensures each read request is independent, similar to FuseReader
    /// The background task only processes explicit read requests, preventing prefetch overflow
    ///
    /// # Thread Safety
    /// This method is thread-safe because AsyncSender.send() takes &self.
    /// Multiple concurrent calls can safely send to the same channel.
    pub async fn fuse_read(&self, offset: i64, len: usize) -> FsResult<Vec<DataSlice>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ReadTask::Read(offset, len, tx))
            .await
            .map_err(|e| self.check_error(e.into()))?;
        rx.await
            .map_err(|_| FsError::from("channel closed".to_string()))?
    }

    pub async fn complete(&self) -> FsResult<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ReadTask::Complete(tx))
            .await
            .map_err(|e| self.check_error(e.into()))?;
        rx.await
            .map_err(|_| FsError::from("channel closed".to_string()))?
    }

    async fn read_future(
        mut reader: UnifiedReader,
        mut req_receiver: AsyncReceiver<ReadTask>,
    ) -> FsResult<()> {
        while let Some(task) = req_receiver.recv().await {
            match task {
                ReadTask::Read(off, len, reply) => {
                    // Each read request is independent
                    // fuse_read(off, len) will read exactly len bytes and return
                    // This prevents background prefetch tasks from seeking beyond file boundaries
                    let data = reader.fuse_read(off, len).await;
                    let _ = reply.send(data);
                }

                ReadTask::Complete(reply) => {
                    let res = reader.complete().await;
                    let _ = reply.send(res);
                }
            }
        }
        Ok(())
    }
}
