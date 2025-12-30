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
//! Removed Mutex to enable true concurrent reads.
//! Each NfsReader exclusively owns its UnifiedReader instance.
//!
//! # Architecture Evolution
//! 1. Original: AsyncChannel serialization (~352 MiB/s)
//! 2. V2: Arc<Mutex<UnifiedReader>> (~547 MiB/s)
//! 3. V3: Direct ownership, no Mutex (target: > 1000 MiB/s)
//!
//! # Key Insight
//! With ReaderPool having 8 NfsReaders, each NfsReader should independently
//! own its UnifiedReader. No sharing = no locking = maximum concurrency.

use curvine_client::unified::UnifiedReader;
use curvine_common::fs::{Path, Reader};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use orpc::sys::DataSlice;

/// Direct NFS Reader - exclusively owns UnifiedReader for zero-lock reads
///
/// # Design
/// - Each NfsReader owns its UnifiedReader (no Arc, no Mutex)
/// - ReaderPool creates 8 independent NfsReaders
/// - True 8-way concurrency with zero lock contention
/// - Clone is NOT supported (each reader is unique)
pub struct NfsReader {
    /// Immutable metadata (no lock needed)
    path: Path,
    len: i64,
    status: FileStatus,
    /// Exclusively owned reader (no sharing, no locking)
    reader: UnifiedReader,
}

impl NfsReader {
    /// Create new NfsReader with exclusive ownership
    pub fn new(reader: UnifiedReader) -> Self {
        let path = reader.path().clone();
        let len = reader.len();
        let status = reader.status().clone();

        Self {
            path,
            len,
            status,
            reader,
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
    /// # Performance
    /// - Zero lock overhead (no Mutex)
    /// - Direct call to UnifiedReader.fuse_read()
    /// - Each NfsReader in the pool operates independently
    ///
    /// # Thread Safety
    /// Safe because each NfsReader is accessed by only one task at a time
    /// (round-robin selection in ReaderPool ensures this)
    pub async fn fuse_read(&mut self, offset: i64, len: usize) -> FsResult<Vec<DataSlice>> {
        self.reader.fuse_read(offset, len).await
    }

    /// Complete the reader and flush any pending data
    pub async fn complete(&mut self) -> FsResult<()> {
        self.reader.complete().await
    }
}
