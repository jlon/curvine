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

use crate::master::meta::feature::{AclFeature, DirFeature};
use crate::master::meta::inode::inodes_children::{
    DirectoryChildren, DirectoryMutation, DirectoryRenameMutation, DirectoryStatusWrite,
};
use crate::master::meta::inode::{Inode, InodeFile, InodePtr, InodeView, EMPTY_PARENT_ID};
use curvine_core_error::{CommonError, CommonResult};
use curvine_model::{
    DirectoryAttributeDelta, DirectoryAttributes, ListOptions, MkdirOpts, StoragePolicy,
    INTERNAL_CTIME_XATTR,
};
use glob::Pattern;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

pub(crate) const MODE_SETGID: u32 = 0o2000;

#[derive(Debug)]
struct DirectoryInheritanceState {
    group: String,
    mode: u32,
}

#[derive(Debug)]
pub(crate) struct DirectoryAttributeState {
    initialized: AtomicBool,
    persisted: AtomicBool,
    mtime: AtomicI64,
    ctime: AtomicI64,
    nlink: AtomicU32,
    base: OnceLock<DirectoryAttributes>,
    inheritance: OnceLock<Arc<RwLock<DirectoryInheritanceState>>>,
}

impl Default for DirectoryAttributeState {
    fn default() -> Self {
        Self {
            initialized: AtomicBool::new(false),
            persisted: AtomicBool::new(false),
            mtime: AtomicI64::new(0),
            ctime: AtomicI64::new(0),
            nlink: AtomicU32::new(0),
            base: OnceLock::new(),
            inheritance: OnceLock::new(),
        }
    }
}

impl DirectoryAttributeState {
    pub(crate) fn new(attributes: DirectoryAttributes) -> Self {
        Self {
            initialized: AtomicBool::new(true),
            persisted: AtomicBool::new(false),
            mtime: AtomicI64::new(attributes.mtime),
            ctime: AtomicI64::new(attributes.ctime),
            nlink: AtomicU32::new(attributes.nlink),
            base: OnceLock::from(attributes),
            inheritance: OnceLock::new(),
        }
    }

    fn attributes(&self, fallback: DirectoryAttributes) -> DirectoryAttributes {
        self.current().unwrap_or(fallback)
    }

    pub(crate) fn current(&self) -> Option<DirectoryAttributes> {
        self.initialized.load(Ordering::Acquire).then(|| {
            DirectoryAttributes::new(
                self.mtime.load(Ordering::Acquire),
                self.ctime.load(Ordering::Acquire),
                self.nlink.load(Ordering::Acquire),
            )
        })
    }

    fn set_current(&self, attributes: DirectoryAttributes) {
        self.mtime.store(attributes.mtime, Ordering::Release);
        self.ctime.store(attributes.ctime, Ordering::Release);
        self.nlink.store(attributes.nlink, Ordering::Release);
        let _ = self.base.set(attributes);
        self.initialized.store(true, Ordering::Release);
    }

    fn set_persisted(&self, attributes: DirectoryAttributes) {
        self.set_current(attributes);
        self.persisted.store(true, Ordering::Release);
    }

    pub(crate) fn delta(
        &self,
        fallback: DirectoryAttributes,
        delta: DirectoryAttributeDelta,
    ) -> DirectoryAttributeDelta {
        if self.persisted.load(Ordering::Acquire) {
            delta
        } else {
            delta.with_base(*self.base.get_or_init(|| fallback))
        }
    }

    pub(crate) fn mark_persisted(&self) {
        self.persisted.store(true, Ordering::Release);
    }

    fn is_persisted(&self) -> bool {
        self.persisted.load(Ordering::Acquire)
    }

    pub(crate) fn apply(&self, delta: DirectoryAttributeDelta) -> CommonResult<()> {
        if delta.nlink_delta != 0 {
            self.nlink
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |nlink| {
                    nlink.checked_add_signed(delta.nlink_delta)
                })
                .map_err(|_| {
                    CommonError::from(format!(
                        "directory nlink update would overflow or underflow: delta={}",
                        delta.nlink_delta
                    ))
                })?;
        }
        self.update_times(delta.mtime, delta.ctime);
        Ok(())
    }

    fn updated(
        &self,
        fallback: DirectoryAttributes,
        delta: DirectoryAttributeDelta,
    ) -> CommonResult<DirectoryAttributes> {
        let mut attributes = self.attributes(fallback);
        attributes.apply(delta).ok_or_else(|| {
            CommonError::from(format!(
                "directory nlink update would overflow or underflow: delta={}",
                delta.nlink_delta
            ))
        })?;
        Ok(attributes)
    }

    fn update_times(&self, mtime: i64, ctime: i64) {
        Self::advance_time(&self.mtime, mtime);
        Self::advance_time(&self.ctime, ctime);
    }

    fn advance_time(slot: &AtomicI64, time: i64) {
        if time > slot.load(Ordering::Acquire) {
            slot.fetch_max(time, Ordering::AcqRel);
        }
    }

    fn inheritance_state(&self, acl: &AclFeature) -> &Arc<RwLock<DirectoryInheritanceState>> {
        self.inheritance.get_or_init(|| {
            Arc::new(RwLock::new(DirectoryInheritanceState {
                group: acl.group.clone(),
                mode: acl.mode,
            }))
        })
    }

    fn inherited_setgid_group(&self, acl: &AclFeature) -> Option<String> {
        if let Some(state) = self.inheritance.get() {
            let state = state.read();
            return (state.mode & MODE_SETGID != 0).then(|| state.group.clone());
        }

        (acl.mode & MODE_SETGID != 0).then(|| acl.group.clone())
    }

    fn update_inheritance(&self, acl: &AclFeature, group: Option<&str>, mode: Option<u32>) {
        if group.is_none() && mode.is_none() {
            return;
        }

        let mut state = self.inheritance_state(acl).write();
        if let Some(group) = group {
            state.group = group.to_string();
        }
        if let Some(mode) = mode {
            state.mode = mode;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeDir {
    pub(crate) id: i64,
    pub(crate) parent_id: i64,
    pub(crate) mtime: i64,
    pub(crate) atime: i64,
    pub(crate) nlink: u32,
    pub(crate) storage_policy: StoragePolicy,

    pub(crate) features: DirFeature,

    #[serde(skip, default)]
    children: Arc<DirectoryChildren>,
}

impl InodeDir {
    pub fn new(id: i64, time: i64) -> Self {
        Self {
            id,
            parent_id: EMPTY_PARENT_ID,
            mtime: time,
            atime: time,
            nlink: 2,
            storage_policy: Default::default(),
            features: Default::default(),
            children: Arc::new(DirectoryChildren::with_attributes(
                DirectoryAttributes::new(time, time, 2),
            )),
        }
    }

    pub fn with_opts(id: i64, time: i64, opts: MkdirOpts) -> Self {
        Self {
            id,
            parent_id: EMPTY_PARENT_ID,
            mtime: time,
            atime: time,
            nlink: 2,
            storage_policy: opts.storage_policy,
            features: DirFeature {
                acl: AclFeature {
                    mode: opts.mode,
                    owner: opts.owner,
                    group: opts.group,
                },
                x_attr: opts.x_attr,
            },
            children: Arc::new(DirectoryChildren::with_attributes(
                DirectoryAttributes::new(time, time, 2),
            )),
        }
    }

    /// Add a word node and return a reference to that word node.
    pub fn add_child(&mut self, mut inode: InodeView) -> CommonResult<InodePtr> {
        inode.set_parent_id(self.id);
        self.children_ref().add_child(inode)
    }

    /// Get the child node of the specified inode name.
    pub fn get_child(&self, name: &str) -> Option<InodeView> {
        self.children_ref().child_view(name)
    }

    pub fn get_child_ptr(&mut self, name: &str) -> Option<InodePtr> {
        self.children_ref().child_ptr(name)
    }

    pub(crate) fn get_child_ptr_exclusive(&self, name: &str) -> Option<InodePtr> {
        self.children_ref().child_ptr_exclusive(name)
    }

    pub fn get_child_ptr_by_glob_pattern(
        &mut self,
        glob_pattern: &Pattern,
    ) -> Option<Vec<InodePtr>> {
        self.children_ref().child_ptrs_by_glob_pattern(glob_pattern)
    }

    pub fn update_mtime(&mut self, time: i64) {
        if time > self.mtime {
            self.mtime = time;
            self.update_ctime(time);
        }
        self.ensure_directory_attributes();
        self.children_ref()
            .attribute_state()
            .update_times(time, time);
    }

    pub fn update_ctime(&mut self, time: i64) {
        if time > self.ctime() {
            self.features.x_attr.insert(
                INTERNAL_CTIME_XATTR.to_string(),
                time.to_le_bytes().to_vec(),
            );
        }
        self.ensure_directory_attributes();
        self.children_ref()
            .attribute_state()
            .update_times(i64::MIN, time);
    }

    pub(crate) fn directory_attributes(&self) -> DirectoryAttributes {
        let fallback = self.initial_directory_attributes();
        self.children.attribute_state().attributes(fallback)
    }

    pub(crate) fn set_directory_attributes(&self, attributes: DirectoryAttributes) {
        self.children_ref()
            .attribute_state()
            .set_persisted(attributes);
    }

    pub(crate) fn apply_directory_attribute_delta(
        &self,
        delta: DirectoryAttributeDelta,
    ) -> CommonResult<()> {
        self.ensure_directory_attributes();
        self.children_ref().attribute_state().apply(delta)
    }

    pub(crate) fn updated_directory_attributes(
        &self,
        delta: DirectoryAttributeDelta,
    ) -> CommonResult<DirectoryAttributes> {
        self.children_ref()
            .attribute_state()
            .updated(self.initial_directory_attributes(), delta)
    }

    pub(crate) fn directory_attribute_delta(
        &self,
        delta: DirectoryAttributeDelta,
    ) -> DirectoryAttributeDelta {
        self.children_ref()
            .attribute_state()
            .delta(self.initial_directory_attributes(), delta)
    }

    pub(crate) fn mark_directory_attributes_persisted(&self) {
        self.children_ref().attribute_state().mark_persisted();
    }

    pub(crate) fn has_persisted_directory_attributes(&self) -> bool {
        self.children.attribute_state().is_persisted()
    }

    pub(crate) fn inherited_setgid_group(&self) -> Option<String> {
        self.children_ref()
            .attribute_state()
            .inherited_setgid_group(&self.features.acl)
    }

    pub(crate) fn update_inheritance(&self, group: Option<&str>, mode: Option<u32>) {
        self.children_ref()
            .attribute_state()
            .update_inheritance(&self.features.acl, group, mode);
    }

    pub(crate) fn set_mtime(&mut self, time: i64) {
        self.mtime = time;
        let attributes = self.directory_attributes();
        self.children_ref()
            .attribute_state()
            .set_current(DirectoryAttributes::new(
                time,
                attributes.ctime,
                attributes.nlink,
            ));
    }

    pub(crate) fn incr_nlink(&mut self, ctime: i64) -> CommonResult<()> {
        self.nlink = self
            .nlink
            .checked_add(1)
            .ok_or_else(|| CommonError::from("directory nlink overflow"))?;
        self.update_ctime(ctime);
        self.apply_directory_attribute_delta(DirectoryAttributeDelta::new(i64::MIN, ctime, 1))
    }

    pub(crate) fn dec_nlink(&mut self, ctime: i64) -> CommonResult<()> {
        if self.nlink == 0 {
            return Ok(());
        }
        self.nlink -= 1;
        self.update_ctime(ctime);
        self.apply_directory_attribute_delta(DirectoryAttributeDelta::new(i64::MIN, ctime, -1))
    }

    fn ensure_directory_attributes(&self) {
        let attributes = self.children_ref().attribute_state();
        if !attributes.initialized.load(Ordering::Acquire) {
            attributes.set_current(self.initial_directory_attributes());
        }
    }

    fn initial_directory_attributes(&self) -> DirectoryAttributes {
        DirectoryAttributes::new(self.mtime, self.legacy_ctime(), self.nlink)
    }

    fn legacy_ctime(&self) -> i64 {
        self.features
            .x_attr
            .get(INTERNAL_CTIME_XATTR)
            .and_then(|bytes| bytes.as_slice().try_into().ok())
            .map(i64::from_le_bytes)
            .unwrap_or(self.mtime)
    }

    pub fn delete_child(&mut self, child_id: i64, child_name: &str) -> CommonResult<InodeView> {
        self.children_ref().delete_child(child_id, child_name)
    }

    pub fn print_child(&self) {
        for child in self.children_ref().children_vec() {
            let t = if child.is_dir() { "dir" } else { "file" };

            println!("{} {} {}", t, child.id(), child.name());
        }
    }

    pub fn children_iter(&self) -> std::vec::IntoIter<InodeView> {
        self.children_ref().children_vec().into_iter()
    }

    pub fn list_options(&self, options: &ListOptions) -> Vec<InodeView> {
        self.children_ref().list_options(options)
    }

    pub fn children_vec(&self) -> Vec<InodeView> {
        self.children_ref().children_vec()
    }

    pub fn child_ptrs(&self) -> Vec<InodePtr> {
        self.children_ref().child_ptrs()
    }

    pub(crate) fn begin_child_mutation(&self, child_name: &str) -> DirectoryMutation<'_> {
        self.children_ref().begin_mutation(child_name)
    }

    pub(crate) fn begin_rename_child_mutation(
        &self,
        source_name: &str,
        destination_name: &str,
    ) -> DirectoryRenameMutation<'_> {
        self.children_ref()
            .begin_rename_mutation(source_name, destination_name)
    }

    pub(crate) fn begin_status_write(&self) -> DirectoryStatusWrite {
        self.children_ref().begin_status_write()
    }

    pub(crate) fn children_handle(&self) -> Arc<DirectoryChildren> {
        self.children.clone()
    }

    fn children_ref(&self) -> &DirectoryChildren {
        self.children.as_ref()
    }

    pub(crate) fn set_children_handle(&mut self, children: Arc<DirectoryChildren>) {
        self.children = children;
    }

    pub fn children_len(&self) -> usize {
        self.children_ref().len()
    }

    pub fn add_file_child(&mut self, name: &str, file: InodeFile) -> CommonResult<InodePtr> {
        self.add_child(InodeView::new_file(name.to_string(), file))
    }

    pub fn add_dir_child(&mut self, name: &str, dir: InodeDir) -> CommonResult<InodePtr> {
        self.add_child(InodeView::new_dir(name.to_string(), dir))
    }
}

impl Inode for InodeDir {
    fn id(&self) -> i64 {
        self.id
    }

    fn parent_id(&self) -> i64 {
        self.parent_id
    }

    fn is_dir(&self) -> bool {
        true
    }

    fn nlink(&self) -> u32 {
        self.directory_attributes().nlink
    }

    fn mtime(&self) -> i64 {
        self.directory_attributes().mtime
    }

    fn atime(&self) -> i64 {
        self.atime
    }

    fn ctime(&self) -> i64 {
        self.directory_attributes().ctime
    }
}

impl PartialEq for InodeDir {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
