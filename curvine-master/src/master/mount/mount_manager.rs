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

#![allow(unused)]
use crate::master::fs::MasterFilesystem;
use crate::master::mount::MountTable;
use crate::master::{self, SyncFsDir};
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{self, CurvineURI, Path};
use curvine_model::{MkdirOpts, MountInfo, MountOptions};
use curvine_ufs_api::S3Conf;
use log::info;
use parking_lot::Mutex;

pub struct MountManager {
    master_fs: MasterFilesystem,
    mount_table: MountTable,
    commit_lock: Mutex<()>,
}

impl MountManager {
    pub fn new(master_fs: MasterFilesystem) -> Self {
        let fs_dir = master_fs.fs_dir.clone();
        MountManager {
            master_fs,
            mount_table: MountTable::new(fs_dir),
            commit_lock: Mutex::new(()),
        }
    }

    /// recovery mount points from store
    pub fn restore(&self) -> FsResult<()> {
        self.mount_table.restore()
    }

    pub fn restore_best_effort(&self) {
        self.mount_table.restore_best_effort()
    }

    fn create_mount_point(&self, mount_path: &str) -> FsResult<bool> {
        let exist = self.master_fs.exists(mount_path)?;
        if exist {
            return Ok(true);
        }

        let opts = MkdirOpts::with_create(true);
        self.master_fs.mkdir_with_opts(mount_path, opts)?;
        Ok(true)
    }

    fn normalize_mount_config(mount: &mut MountInfo) -> FsResult<()> {
        let path = Path::from_str(&mount.ufs_path)?;
        if !matches!(path.scheme(), Some("s3" | "s3a")) {
            return Ok(());
        }

        let properties = std::mem::take(&mut mount.properties);
        mount.properties = S3Conf::canonicalize_properties(properties).map_err(|err| {
            FsError::common(format!(
                "Invalid mount configuration for {}: {}",
                mount.ufs_path, err
            ))
        })?;
        S3Conf::validate(&mount.properties).map_err(|err| {
            FsError::common(format!(
                "Invalid mount configuration for {}: {}",
                mount.ufs_path, err
            ))
        })
    }

    /// same baseuri of ufs can only mount once
    ///
    /// ufs_uri maybe scheme://authority/xxxx/yyy,
    /// base_uri is scheme://authority/
    fn add_mount(
        &self,
        mnt_id: Option<u32>,
        mount_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        let _commit_guard = self.commit_lock.lock();

        let assign_id = match mnt_id {
            Some(id) => id,
            None => self.mount_table.assign_mount_id()?,
        };
        let mut mount = mnt_opt.clone().to_info(assign_id, mount_path, ufs_path);
        Self::normalize_mount_config(&mut mount)?;
        let mut normalized_options = mnt_opt.clone();
        normalized_options.add_properties = mount.properties;
        let info = self.mount_table.build_mount_info(
            assign_id,
            mount_path,
            ufs_path,
            &normalized_options,
        )?;
        let _ = self.create_mount_point(mount_path)?;
        self.master_fs.commit_mount(info.clone())?;
        self.mount_table.unprotected_add_mount(info)
    }

    fn update_mount(&self, cv_path: &str, mnt_opt: &MountOptions) -> FsResult<()> {
        let _commit_guard = self.commit_lock.lock();
        let path = Path::from_str(cv_path)?;
        let Some(existing) = self.get_mount_info(&path)? else {
            return err_box!("mount point {} not found for update", cv_path);
        };
        let mut merged = existing.merge_with(mnt_opt.clone());
        Self::normalize_mount_config(&mut merged)?;

        let info = self.mount_table.build_updated_mount_info(merged)?;
        self.master_fs.commit_mount(info.clone())?;
        self.mount_table.unprotected_add_mount(info)
    }

    /// same baseuri of ufs can only mount once
    ///
    /// ufs_uri maybe scheme://authority/xxxx/yyy,
    /// base_uri is scheme://authority/
    pub fn mount(
        &self,
        mnt_id: Option<u32>,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        self.master_fs.ensure_metadata_current()?;
        if mnt_opt.update {
            return self.update_mount(cv_path, mnt_opt);
        }

        self.add_mount(mnt_id, cv_path, ufs_path, mnt_opt)
    }

    pub fn unprotected_add_mount(&self, info: MountInfo) -> FsResult<()> {
        self.mount_table.unprotected_add_mount(info)
    }

    pub fn umount(&self, cv_path: &str) -> FsResult<()> {
        self.master_fs.ensure_metadata_current()?;
        let _commit_guard = self.commit_lock.lock();
        let mount_id = self.mount_table.get_mount_id_by_path(cv_path)?;
        self.master_fs.commit_unmount(mount_id)?;
        self.mount_table.unprotected_umount_by_id(mount_id)
    }

    pub fn unmount_by_id(&self, id: u32) -> FsResult<()> {
        self.master_fs.ensure_metadata_current()?;
        let info = self.mount_table.get_mount_info_by_id(id)?;
        self.umount(&info.cv_path)
    }

    pub fn unprotected_umount_by_id(&self, id: u32) -> FsResult<()> {
        self.mount_table.unprotected_umount_by_id(id)
    }

    pub fn unprotected_umount_if_mounted(&self, id: u32) -> FsResult<bool> {
        if !self.mount_table.has_mounted(id)? {
            return Ok(false);
        }
        self.mount_table.unprotected_umount_by_id(id)?;
        Ok(true)
    }

    pub fn has_mounted(&self, id: u32) -> FsResult<bool> {
        self.master_fs.ensure_metadata_current()?;
        self.mount_table.has_mounted(id)
    }

    /**
     * use ufs_uri to find mount entry
     */
    pub fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        self.master_fs.ensure_metadata_current()?;
        self.get_mount_info_unchecked(path)
    }

    pub(crate) fn get_mount_info_unchecked(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        self.mount_table.get_mount_info(path)
    }

    pub fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        self.master_fs.ensure_metadata_current()?;
        let table = self.mount_table.get_mount_table()?;

        let mut entries = Vec::new();
        table.iter().for_each(|entry| {
            entries.push(entry.clone());
        });
        Ok(entries)
    }
}
