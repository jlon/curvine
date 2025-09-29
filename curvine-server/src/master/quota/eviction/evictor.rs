// English comments only in code blocks

use std::collections::HashMap;
use std::sync::RwLock;
pub trait Evictor: Send + Sync {
    fn on_access(&self, _quota_root: i64, _inode_id: i64) {}
    fn select_victims(&self, quota_root: i64, limit: usize) -> Vec<i64>;
}

pub struct LRUEvictor {
    lru_caches: RwLock<HashMap<i64, lru::LruCache<i64, ()>>>,
}

impl LRUEvictor {
    pub fn new() -> Self {
        Self {
            lru_caches: RwLock::new(HashMap::new()),
        }
    }

    fn with_write_cache<F: FnOnce(&mut lru::LruCache<i64, ()>)>(&self, quota_root: i64, f: F) {
        if let Ok(mut caches) = self.lru_caches.write() {
            let lru = caches
                .entry(quota_root)
                .or_insert_with(|| lru::LruCache::unbounded());
            f(lru);
        }
    }

    fn pop_victims(&self, quota_root: i64, limit: usize) -> Vec<i64> {
        if let Ok(mut caches) = self.lru_caches.write() {
            if let Some(lru) = caches.get_mut(&quota_root) {
                let mut victims = Vec::with_capacity(limit);
                for _ in 0..limit {
                    if let Some((inode_id, _)) = lru.pop_lru() {
                        victims.push(inode_id);
                    } else {
                        break;
                    }
                }
                return victims;
            }
        }
        Vec::new()
    }
}

impl Evictor for LRUEvictor {
    fn on_access(&self, quota_root: i64, inode_id: i64) {
        self.with_write_cache(quota_root, |lru| {
            lru.put(inode_id, ());
        });
    }

    fn select_victims(&self, quota_root: i64, limit: usize) -> Vec<i64> {
        self.pop_victims(quota_root, limit)
    }
}
