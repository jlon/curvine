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

use crate::master::{Master, MasterMetrics};

/// File system statistics tracker for maintaining real-time counts
/// of files and directories without expensive traversal operations.
#[derive(Debug)]
pub struct FileSystemStats {
    metrics: &'static MasterMetrics,
}

impl FileSystemStats {
    pub fn new() -> Self {
        Self {
            metrics: Master::get_metrics(),
        }
    }

    pub fn increment_file_count(&self) {
        self.metrics.inode_file_num.inc()
    }

    pub fn increment_dir_count(&self) {
        self.metrics.inode_dir_num.inc()
    }

    pub fn add_file_count(&self, count: i64) {
        self.metrics.inode_file_num.add(count)
    }

    pub fn add_dir_count(&self, count: i64) {
        self.metrics.inode_dir_num.add(count)
    }

    pub fn counts(&self) -> (i64, i64) {
        (
            self.metrics.inode_dir_num.get(),
            self.metrics.inode_file_num.get(),
        )
    }

    pub fn set_counts(&self, file_count: i64, dir_count: i64) {
        self.metrics.inode_file_num.set(file_count);
        self.metrics.inode_dir_num.set(dir_count);
    }

    /// Add a quota path for tracking (placeholder - not implemented)
    pub fn add_quota_path(&self, _path: &str, _initial_size: i64) {
        // Placeholder - quota tracking will be handled by periodic sync
    }

    /// Remove a quota path from tracking (placeholder - not implemented)
    pub fn remove_quota_path(&self, _path: &str) {
        // Placeholder - quota tracking will be handled by periodic sync
    }

    /// Get current size for a quota path (placeholder - returns None)
    pub fn get_quota_size(&self, _path: &str) -> Option<i64> {
        // Placeholder - will be calculated on demand
        None
    }
}

impl Default for FileSystemStats {
    fn default() -> Self {
        Self::new()
    }
}
