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

use crate::master::SyncFsDir;
use curvine_common::state::QuotaInfo;
// use log::info; // lowered to debug in this module
use orpc::{err_box, CommonResult};
use std::collections::HashMap;
use std::sync::RwLock;

pub struct QuotaTable {
    quotas: RwLock<HashMap<i64, QuotaInfo>>,
    fs_dir: SyncFsDir,
}

impl QuotaTable {
    pub fn new(fs_dir: SyncFsDir) -> Self {
        QuotaTable {
            quotas: RwLock::new(HashMap::new()),
            fs_dir,
        }
    }

    pub fn restore(&self) {
        if let Ok(quotas) = self.fs_dir.read().get_quota_table() {
            for quota in quotas {
                let _ = self.unprotected_add_quota(quota);
            }
        }
    }

    pub fn has_quota(&self, inode_id: i64) -> bool {
        self.quotas
            .read()
            .map_or(false, |quotas| quotas.contains_key(&inode_id))
    }

    pub fn add_quota(&self, inode_id: i64, path: &str, quota_size: i64) -> CommonResult<()> {
        let quota_info = QuotaInfo::new(inode_id, path, quota_size);

        {
            let mut quotas = self.quotas.write().unwrap();
            if quotas.contains_key(&inode_id) {
                return err_box!("Directory '{}' already has a quota defined. Please remove the existing quota first or use the update command to modify it.", path);
            }
            quotas.insert(inode_id, quota_info.clone());
        }

        log::debug!("add quota: {:?}", quota_info);

        let mut fs_guard = self.fs_dir.write();
        fs_guard.store_quota(quota_info)?;

        Ok(())
    }

    pub fn remove_quota(&self, inode_id: i64, path: &str) -> CommonResult<()> {
        {
            let mut quotas = self.quotas.write().unwrap();
            if !quotas.contains_key(&inode_id) {
                return err_box!(
                    "No quota found for directory '{}'. Use 'cv quota add' to create a quota first.",
                    path
                );
            }
            quotas.remove(&inode_id);
        }

        let mut fs_guard = self.fs_dir.write();
        fs_guard.remove_quota(inode_id)?;

        Ok(())
    }

    pub fn update_quota(&self, inode_id: i64, path: &str, new_quota_size: i64) -> CommonResult<()> {
        let updated_quota = {
            let mut quotas = self.quotas.write().unwrap();
            match quotas.get_mut(&inode_id) {
                Some(quota_info) => {
                    quota_info.quota_size = new_quota_size;
                    quota_info.updated_time = orpc::common::LocalTime::mills() as i64;
                    quota_info.clone()
                }
                None => {
                    return err_box!(
                        "No quota found for directory '{}'. Use 'cv quota add' to create a quota first.",
                        path
                    );
                }
            }
        };

        let mut fs_guard = self.fs_dir.write();
        fs_guard.store_quota(updated_quota)?;

        Ok(())
    }

    pub fn get_quota_info(&self, inode_id: i64) -> CommonResult<Option<QuotaInfo>> {
        let quotas = self.quotas.read().unwrap();
        Ok(quotas.get(&inode_id).cloned())
    }

    pub fn get_quota_table(&self) -> CommonResult<Vec<QuotaInfo>> {
        let quotas = self.quotas.read().unwrap();
        Ok(quotas.values().cloned().collect())
    }

    pub fn unprotected_add_quota(&self, info: QuotaInfo) -> CommonResult<()> {
        let mut quotas = self.quotas.write().unwrap();
        quotas.insert(info.inode_id, info);
        Ok(())
    }
}
