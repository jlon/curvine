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

use crate::master::quota::QuotaTable;
use crate::master::meta::inode::{InodePath, InodeView};
use curvine_common::state::QuotaInfo;
use orpc::CommonResult;

pub struct QuotaManager {
    quota_table: QuotaTable,
}

impl QuotaManager {
    pub fn new(quota_table: QuotaTable) -> Self {
        QuotaManager { quota_table }
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
                            InodeView::Dir(_, dir) => {
                                dir.subtree_bytes
                            },
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
                curvine_common::state::QuotaState::Exceeded
            } else {
                curvine_common::state::QuotaState::Available
            };
            quota_info.updated_time = orpc::common::LocalTime::mills() as i64;
        }

        Ok(quotas)
    }

    pub fn unprotected_add_quota(&self, info: QuotaInfo) -> CommonResult<()> {
        self.quota_table.unprotected_add_quota(info)
    }

    // Deprecated: recursive calculation replaced by O(1) subtree_bytes read
    // pub fn cal_dir_size(...)
}
