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

use std::sync::atomic::{AtomicI64, Ordering};

/// File system statistics tracker for maintaining real-time counts
/// of files and directories without expensive traversal operations.
#[derive(Debug)]
pub struct FileSystemStats {
    /// Total number of files in the file system
    file_count: AtomicI64,
    /// Total number of directories in the file system (excluding root)
    dir_count: AtomicI64,
}

impl FileSystemStats {
    /// Create a new FileSystemStats instance with zero counts
    pub fn new() -> Self {
        Self {
            file_count: AtomicI64::new(0),
            dir_count: AtomicI64::new(0),
        }
    }

    /// Increment file count by 1
    pub fn increment_file_count(&self) {
        self.file_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment directory count by 1
    pub fn increment_dir_count(&self) {
        self.dir_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Add multiple files to the count
    pub fn add_file_count(&self, count: i64) {
        self.file_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Add multiple directories to the count
    pub fn add_dir_count(&self, count: i64) {
        self.dir_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Get both counts as a tuple (file_count, dir_count)
    pub fn counts(&self) -> (i64, i64) {
        (
            self.file_count.load(Ordering::Relaxed),
            self.dir_count.load(Ordering::Relaxed),
        )
    }

    /// Set specific counts (useful for initialization from existing data)
    pub fn set_counts(&self, file_count: i64, dir_count: i64) {
        self.file_count.store(file_count, Ordering::Relaxed);
        self.dir_count.store(dir_count, Ordering::Relaxed);
    }
}

impl Default for FileSystemStats {
    fn default() -> Self {
        Self::new()
    }
}
