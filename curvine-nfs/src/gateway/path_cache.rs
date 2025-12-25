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

//! Path cache for fileid -> path mapping
//!
//! NFS uses fileid (inode number) to identify files, but Curvine uses paths.
//! This cache maintains the reverse mapping for efficient path lookups.
//!
//! # Implementation
//!
//! Uses `FastSyncCache` (moka-based) for:
//! - Automatic TTL expiration
//! - LRU eviction when capacity is reached
//! - Lock-free concurrent access
//! - Better performance than hand-rolled RwLock<HashMap>

use crate::nfs::fileid3;
use orpc::sync::FastSyncCache;
use std::time::Duration;

/// LRU-based path cache with TTL support
///
/// Wraps `FastSyncCache` to provide fileid -> path mapping with:
/// - Automatic TTL-based expiration
/// - Capacity-based LRU eviction
/// - Special handling for root directory
pub struct PathCache {
    /// Inner cache: fileid -> path
    cache: FastSyncCache<fileid3, String>,
    /// Root fileid (always returns "/" without caching)
    root_id: fileid3,
}

impl PathCache {
    /// Create a new path cache
    ///
    /// # Arguments
    /// * `max_size` - Maximum number of entries
    /// * `ttl` - Time-to-live for cache entries
    /// * `root_id` - Root directory fileid (special case, always returns "/")
    pub fn new(max_size: usize, ttl: Duration, root_id: fileid3) -> Self {
        Self {
            cache: FastSyncCache::new(max_size as u64, ttl),
            root_id,
        }
    }

    /// Get path for fileid
    ///
    /// Returns None if not found or expired.
    /// Root directory always returns "/" without cache lookup.
    #[inline]
    pub fn get(&self, id: fileid3) -> Option<String> {
        // Root directory special case - no cache needed
        if id == self.root_id {
            return Some("/".to_string());
        }

        self.cache.get(&id)
    }

    /// Insert or update path for fileid
    ///
    /// Root directory is never cached (always returns "/").
    #[inline]
    pub fn insert(&self, id: fileid3, path: String) {
        // Don't cache root
        if id == self.root_id {
            return;
        }

        self.cache.insert(id, path);
    }

    /// Remove path for fileid
    #[inline]
    pub fn remove(&self, id: fileid3) {
        self.cache.invalidate(&id);
    }

    /// Get cache statistics (current_size, max_size)
    #[allow(dead_code)]
    pub fn stats(&self) -> (usize, u64) {
        (
            self.cache.entry_count() as usize,
            self.cache.weighted_size(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_cache_basic() {
        let cache = PathCache::new(100, Duration::from_secs(60), 1000);

        // Insert and get
        cache.insert(1001, "/test".to_string());
        assert_eq!(cache.get(1001), Some("/test".to_string()));

        // Root always returns "/"
        assert_eq!(cache.get(1000), Some("/".to_string()));

        // Non-existent returns None
        assert_eq!(cache.get(9999), None);
    }

    #[test]
    fn test_path_cache_expiry() {
        let cache = PathCache::new(100, Duration::from_millis(10), 1000);

        cache.insert(1001, "/test".to_string());
        assert_eq!(cache.get(1001), Some("/test".to_string()));

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(50));

        // moka uses lazy expiration, need to trigger sync
        cache.cache.run_pending_tasks();

        assert_eq!(cache.get(1001), None);
    }

    #[test]
    fn test_path_cache_remove() {
        let cache = PathCache::new(100, Duration::from_secs(60), 1000);

        cache.insert(1001, "/test".to_string());
        assert_eq!(cache.get(1001), Some("/test".to_string()));

        cache.remove(1001);

        // moka uses lazy invalidation
        cache.cache.run_pending_tasks();

        assert_eq!(cache.get(1001), None);
    }

    #[test]
    fn test_path_cache_root_not_cached() {
        let cache = PathCache::new(100, Duration::from_secs(60), 1000);

        // Try to insert root - should be ignored
        cache.insert(1000, "/should_not_cache".to_string());

        // Root always returns "/"
        assert_eq!(cache.get(1000), Some("/".to_string()));
    }
}
