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

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::master::fs::MasterFilesystem;
use crate::master::meta::inode::ttl_executor::InodeTtlExecutor;
use crate::master::meta::inode::{InodePath, InodeView};
use crate::master::quota::eviction::detector::{EvictionDetector, WatermarkDetector};
use crate::master::quota::eviction::evictor::{Evictor, LRUEvictor};
use crate::master::quota::eviction::executor::{EvictionExecutor, FileEvictionExecutor};
use crate::master::quota::eviction::types::EvictionPolicy;
use crate::master::quota::eviction::EvictionConf;
use crate::master::quota::QuotaObserver;
use crate::master::quota::QuotaTable;
use crate::master::SyncFsDir;
use curvine_common::conf::ClusterConf;
use curvine_common::state::QuotaInfo;
use curvine_common::state::{FileStatus, QuotaState};
use orpc::common::LocalTime;
use orpc::runtime::RpcRuntime;
use orpc::runtime::Runtime;
use orpc::CommonResult;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct EvictTrigger {
    pub path_hint: String,
}

pub struct QuotaManager {
    quota_table: QuotaTable,
    eviction_conf: Option<EvictionConf>,
    fs: Option<SyncFsDir>,
    detector: Option<Arc<dyn EvictionDetector>>,
    evictor: Option<Arc<dyn Evictor>>,
    executor: Option<Arc<dyn EvictionExecutor>>,
    tx: Option<mpsc::Sender<EvictTrigger>>,
    cleaning: Mutex<HashSet<i64>>,
}

impl QuotaManager {
    pub fn new(conf: &ClusterConf, fs: MasterFilesystem, rt: Arc<Runtime>) -> Arc<Self> {
        let conf = EvictionConf::from_conf(conf);
        let quota_table = crate::master::quota::QuotaTable::new(fs.fs_dir.clone());
        let (tx, mut rx) = mpsc::channel(1024);

        let manager = Arc::new(QuotaManager {
            quota_table,
            eviction_conf: Some(conf.clone()),
            fs: Some(fs.fs_dir.clone()),
            detector: Some(Arc::new(WatermarkDetector::new(conf.clone()))),
            evictor: Some(match conf.policy {
                EvictionPolicy::Lru => Arc::new(LRUEvictor::new()) as Arc<dyn Evictor>,
                EvictionPolicy::Lfu => Arc::new(LRUEvictor::new()) as Arc<dyn Evictor>,
                EvictionPolicy::Arc => Arc::new(LRUEvictor::new()) as Arc<dyn Evictor>,
            }),
            executor: Some(Arc::new(FileEvictionExecutor::new(InodeTtlExecutor::new(
                fs.clone(),
            )))),
            tx: Some(tx),
            cleaning: Mutex::new(HashSet::new()),
        });

        let mgr = manager.clone();
        rt.spawn(async move {
            while let Some(trigger) = rx.recv().await {
                if !mgr.is_eviction_enabled() {
                    continue;
                }
                mgr.handle_trigger(trigger);
            }
        });

        manager
    }

    pub fn restore(&self) {
        self.quota_table.restore();
    }

    pub fn add_quota(&self, inode_id: i64, path: &str, quota_size: i64) -> CommonResult<()> {
        self.quota_table.add_quota(inode_id, path, quota_size)
    }

    pub fn update_quota(&self, inode_id: i64, path: &str, quota_size: i64) -> CommonResult<()> {
        self.quota_table.update_quota(inode_id, path, quota_size)
    }

    pub fn remove_quota(&self, inode_id: i64, path: &str) -> CommonResult<()> {
        self.quota_table.remove_quota(inode_id, path)
    }

    pub fn get_quota_info(&self, inode_id: i64) -> CommonResult<Option<QuotaInfo>> {
        self.quota_table.get_quota_info(inode_id)
    }

    pub fn get_quota_table(&self) -> CommonResult<Vec<QuotaInfo>> {
        self.quota_table.get_quota_table()
    }

    pub fn get_quota_usage(
        &self,
        fs_dir: &crate::master::SyncFsDir,
    ) -> CommonResult<Vec<QuotaInfo>> {
        let mut quotas = self.quota_table.get_quota_table()?;

        for quota_info in &mut quotas {
            log::debug!("Reading subtree_bytes for quota path: {}", quota_info.path);

            let current_size = {
                let fs_dir_guard = fs_dir.read();
                if let Ok(inp) = InodePath::resolve(
                    fs_dir_guard.root_ptr(),
                    &quota_info.path,
                    &fs_dir_guard.store,
                ) {
                    if let Some(last) = inp.get_last_inode() {
                        match last.as_ref() {
                            InodeView::Dir(_, dir) => dir.subtree_bytes,
                            _ => {
                                log::warn!("Quota path is not a directory: {}", quota_info.path);
                                0
                            }
                        }
                    } else {
                        log::warn!("Failed to get last inode for path: {}", quota_info.path);
                        0
                    }
                } else {
                    log::warn!("Failed to resolve path: {}", quota_info.path);
                    0
                }
            };

            quota_info.used_size = current_size;
            log::debug!("Updated quota_info.used_size to: {}", quota_info.used_size);
            quota_info.state = if quota_info.is_exceeded() {
                QuotaState::Exceeded
            } else {
                QuotaState::Available
            };
            quota_info.updated_time = LocalTime::mills() as i64;
        }

        Ok(quotas)
    }

    pub fn unprotected_add_quota(&self, info: QuotaInfo) -> CommonResult<()> {
        self.quota_table.unprotected_add_quota(info)
    }

    fn is_eviction_enabled(&self) -> bool {
        self.eviction_conf
            .as_ref()
            .map(|c| c.enable_prequota_eviction)
            .unwrap_or(false)
    }

    fn handle_trigger(&self, trigger: EvictTrigger) {
        let conf = match &self.eviction_conf {
            Some(c) if c.enable_prequota_eviction => c,
            _ => return,
        };

        let (detector, evictor) = match (&self.detector, &self.evictor) {
            (Some(d), Some(e)) => (d, e),
            _ => return,
        };

        let quotas = if let Some(fs_dir) = &self.fs {
            match self.get_quota_usage(fs_dir) {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("prequota-evict: get_quota_usage failed: {:?}", e);
                    return;
                }
            }
        } else {
            match self.get_quota_table() {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("prequota-evict: get_quota_table failed: {:?}", e);
                    return;
                }
            }
        };

        for q in quotas {
            // Skip if this quota_root is currently being cleaned
            if let Ok(cleaning) = self.cleaning.lock() {
                if cleaning.contains(&q.inode_id) {
                    continue;
                }
            }

            let used = q.used_size;

            if let Some(mut plan) = detector.maybe_create_plan(used, q.quota_size, q.inode_id) {
                if let Ok(mut cleaning) = self.cleaning.lock() {
                    cleaning.insert(q.inode_id);
                }

                loop {
                    let step_free = if conf.max_evict_rate_bytes_per_s > 0 {
                        plan.target_free_bytes.min(conf.max_evict_rate_bytes_per_s)
                    } else {
                        plan.target_free_bytes
                    };

                    if step_free <= 0 {
                        break;
                    }

                    log::debug!(
                        "prequota-evict: triggered path_hint={}, quota_path={}, used={}, quota={}, target_free_step={}",
                        trigger.path_hint, q.path, used, q.quota_size, step_free
                    );

                    let inode_ids =
                        evictor.select_victims(plan.quota_root_inode_id, conf.candidate_scan_page);

                    if inode_ids.is_empty() {
                        log::debug!("prequota-evict: no more victims available, stopping eviction");
                        break;
                    }

                    if conf.dry_run {
                        log::debug!(
                            "prequota-evict: dry_run=true, would process inode_ids_step={}",
                            inode_ids.len()
                        );
                        break;
                    }

                    let freed = match (&self.executor, &self.fs) {
                        (Some(executor), Some(fs)) => {
                            let total_freed = {
                                let fs_guard = fs.read();
                                inode_ids
                                    .iter()
                                    .filter_map(|&inode_id| {
                                        fs_guard.store.get_inode(inode_id, None).ok().flatten()
                                    })
                                    .map(|inode_view| match &inode_view {
                                        InodeView::File(_, f) => f.len.max(0),
                                        InodeView::Dir(_, d) => d.subtree_bytes.max(0),
                                        _ => 0,
                                    })
                                    .sum::<i64>()
                            };

                            // Execute deletions after releasing fs read lock to avoid deadlocks with writers
                            executor.execute(conf.eviction_mode, &inode_ids);
                            log::debug!(
                                "prequota-evict: executed inode_ids_step={}, freed_step_bytes={}",
                                inode_ids.len(),
                                total_freed
                            );

                            total_freed
                        }
                        _ => {
                            log::warn!("prequota-evict: executor or fs missing, skip execution");
                            break;
                        }
                    };

                    plan.target_free_bytes =
                        plan.target_free_bytes.saturating_sub(freed.max(step_free));

                    if plan.target_free_bytes <= 0 {
                        break;
                    }
                }

                if let Ok(mut cleaning) = self.cleaning.lock() {
                    cleaning.remove(&q.inode_id);
                }
            }
        }
    }
}

impl QuotaObserver for QuotaManager {
    fn on_size_change(&self, status: &FileStatus) {
        if !self.is_eviction_enabled() {
            return;
        }

        if let Some(tx) = &self.tx {
            let _ = tx.try_send(EvictTrigger {
                path_hint: status.path.clone(),
            });
        }
    }

    fn on_access(&self, status: &FileStatus) {
        if !self.is_eviction_enabled() {
            return;
        }

        let quotas = match self.get_quota_table() {
            Ok(v) => v,
            Err(e) => {
                log::warn!(
                    "prequota-evict: get_quota_table failed in on_access: {:?}",
                    e
                );
                return;
            }
        };

        // Find the most specific quota (longest matching path)
        let quota_root = quotas
            .iter()
            .filter(|q| {
                let qp = &q.path;
                qp == "/" || status.path == *qp || status.path.starts_with(&format!("{}/", qp))
            })
            .max_by_key(|q| q.path.len())
            .map(|q| q.inode_id);

        if let (Some(quota_root), Some(evictor)) = (quota_root, &self.evictor) {
            evictor.on_access(quota_root, status.id);
        }
    }

    fn on_open(&self, status: &FileStatus) {
        if !self.is_eviction_enabled() {
            return;
        }

        if let Some(tx) = &self.tx {
            let _ = tx.try_send(EvictTrigger {
                path_hint: status.path.clone(),
            });
        }
    }
}
