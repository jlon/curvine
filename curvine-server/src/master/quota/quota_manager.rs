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
use crate::master::SyncFsDir;
use curvine_common::state::QuotaInfo;
use orpc::{err_box, CommonResult};

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
            log::debug!("Calculating size for quota path: {}", quota_info.path);

            let current_size = {
                let fs_dir_guard = fs_dir.read();
                if let Ok(inp) = InodePath::resolve(
                    fs_dir_guard.root_ptr(),
                    &quota_info.path,
                    &fs_dir_guard.store,
                ) {
                    log::debug!(
                        "Successfully resolved path {}, calculating size...",
                        quota_info.path
                    );
                    drop(fs_dir_guard); // 释放读锁
                    let size = Self::cal_dir_size(fs_dir, &inp).unwrap_or(0);
                    log::debug!("Calculated size for {}: {} bytes", quota_info.path, size);
                    size
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

    pub fn cal_dir_size(fs_dir: &SyncFsDir, inp: &InodePath) -> CommonResult<i64> {
        let fs_dir_guard = fs_dir.read();
        let mut total_size = 0i64;

        let last_inode = match inp.get_last_inode() {
            Some(inode) => inode,
            None => return err_box!("Directory not found"),
        };

        match last_inode.as_ref() {
            InodeView::Dir(_, dir) => {
                for child in dir.children_iter() {
                    match child {
                        InodeView::File(_, file) => {
                            total_size += file.len;
                        }
                        InodeView::Dir(name, _) => {
                            let child_path_str = inp.child_path(name);
                            if let Ok(child_inp) = InodePath::resolve(fs_dir_guard.root_ptr(), &child_path_str, &fs_dir_guard.store) {
                                let subdir_size = Self::cal_dir_size(fs_dir, &child_inp)?;
                                total_size += subdir_size;
                            }
                        }
                        InodeView::FileEntry(name, inode_id) => {
                            if let Ok(Some(actual_inode)) = fs_dir_guard.store.get_inode(*inode_id, Some(name)) {
                                if let InodeView::File(_, file) = actual_inode {
                                    total_size += file.len;
                                }
                            }
                        }
                    }
                }
            }
            _ => {
                return err_box!("Path is not a directory");
            }
        }

        Ok(total_size)
    }
}
