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

//! NfsReader: A direct wrapper around UnifiedReader for high-performance reads
//!
//! # Performance Optimization (2025-12-30)
//! Removed channel-based serialization mechanism to improve sequential read performance.
//! Each NfsReader now directly wraps UnifiedReader, allowing true concurrent reads
//! when used with ReaderPool.
//!
//! # Previous Architecture (with channel)
//! - Each NfsReader had an AsyncChannel that serialized all read requests
//! - Even with 8 readers in ReaderPool, each reader processed requests one-by-one
//! - Sequential read performance: ~352 MiB/s
//!
//! # New Architecture (direct)
//! - NfsReader directly wraps UnifiedReader with Arc<Mutex<...>>
//! - Multiple concurrent reads can be processed simultaneously
//! - Target sequential read performance: > 3000 MiB/s

use curvine_client::unified::UnifiedReader;
use curvine_common::fs::{Path, Reader};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use orpc::sys::DataSlice;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Direct NFS Reader - wraps UnifiedReader without channel serialization
///
/// # Design
/// - Uses Arc<Mutex<UnifiedReader>> for thread-safe concurrent access
/// - No channel mechanism - reads are processed directly
/// - Clone is cheap (Arc clone)
/// - When used with ReaderPool, enables true concurrent reads
pub struct NfsReader {
    /// Immutable metadata (no lock needed)
    path: Path,
    len: i64,
    status: FileStatus,
    /// Mutable reader state (protected by Mutex)
    reader: Arc<Mutex<UnifiedReader>>,
}

impl NfsReader {
    /// Create new NfsReader (no background task needed)
    pub fn new(reader: UnifiedReader) -> Self {
        let path = reader.path().clone();
        let len = reader.len();
        let status = reader.status().clone();

        Self {
            path,
            len,
            status,
            reader: Arc::new(Mutex::new(reader)),
        }
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline]
    pub fn len(&self) -> i64 {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    /// Read data at specific offset and length
    ///
    /// # Thread Safety
    /// Uses Mutex to ensure thread-safe access to UnifiedReader.
    /// Multiple concurrent calls will be serialized by the Mutex,
    /// but with ReaderPool (8 readers), we get 8-way concurrency.
    ///
    /// # Performance
    /// - No channel overhead
    /// - Direct call to UnifiedReader.fuse_read()
    /// - Mutex contention is minimal with ReaderPool
    pub async fn fuse_read(&self, offset: i64, len: usize) -> FsResult<Vec<DataSlice>> {
        let mut reader = self.reader.lock().await;
        reader.fuse_read(offset, len).await
    }

    /// Complete the reader and flush any pending data
    pub async fn complete(&self) -> FsResult<()> {
        let mut reader = self.reader.lock().await;
        reader.complete().await
    }
}
