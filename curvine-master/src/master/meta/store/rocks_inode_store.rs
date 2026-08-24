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

use crate::master::meta::inode::InodeView;
use crate::master::meta::LockMeta;
use curvine_core_error::{err_box, try_option, CommonError, CommonResult};
use curvine_error::FsError;
use curvine_model::{
    BlockLocation, DirectoryAttributeDelta, DirectoryAttributes, FileLock, FileStatus, MountInfo,
};
use curvine_rocksdb::{
    CfMergeOperator, DBConf, DBEngine, DBIteratorWithThreadMode, DBPinnableSlice, DBSnapshotReader,
    Error, RocksIterator, RocksUtils, WriteBatchWithTransaction, DB,
};
use curvine_runtime::common::SerdeUtils as Serde;
use std::collections::HashMap;

pub struct RocksInodeStore {
    pub(crate) db: DBEngine,
}

impl RocksInodeStore {
    pub const CF_INODES: &'static str = "inodes";
    pub const CF_EDGES: &'static str = "edges";
    pub const CF_BLOCK: &'static str = "block";
    pub const CF_LOCATION: &'static str = "location";
    pub const CF_COMMON: &'static str = "common";
    pub const CF_DIR_ATTRS: &'static str = "dir_attrs";

    pub const PREFIX_MOUNT: u8 = 0x01;
    pub const PREFIX_LOCK: u8 = 0x02;
    pub const PREFIX_SCHEMA: u8 = 0x03;

    const DIRECTORY_ATTRIBUTE_SCHEMA: u8 = 0x01;
    const DIRECTORY_ATTRIBUTE_SCHEMA_VERSION: u8 = 0x01;

    pub fn new(conf: DBConf, format: bool) -> CommonResult<Self> {
        let conf = conf
            .set_disable_wal(true)
            .add_cf(Self::CF_INODES)
            .add_cf(Self::CF_EDGES)
            .add_cf(Self::CF_BLOCK)
            .add_cf(Self::CF_LOCATION)
            .add_cf(Self::CF_COMMON)
            .add_cf(Self::CF_DIR_ATTRS)
            .set_cf_merge_operator(Self::CF_DIR_ATTRS, CfMergeOperator::DirectoryAttributes);
        let db = DBEngine::new(conf, format)?;
        Ok(Self { db })
    }

    pub fn get_child_ids(
        &self,
        id: i64,
        prefix: Option<&str>,
    ) -> CommonResult<InodeChildrenIter<'_>> {
        let iter = match prefix {
            None => {
                let key = RocksUtils::i64_to_bytes(id);
                self.db.prefix_scan(Self::CF_EDGES, key)?
            }

            Some(v) => {
                let key = RocksUtils::i64_str_to_bytes(id, v);
                self.db.prefix_scan(Self::CF_EDGES, key)?
            }
        };

        Ok(InodeChildrenIter { inner: iter })
    }

    pub fn snapshot(&self) -> RocksInodeStoreSnapshot<'_> {
        RocksInodeStoreSnapshot {
            reader: self.db.snapshot_reader(),
        }
    }

    // Get all location information for all block ids.
    pub fn get_locations(&self, block_id: i64) -> CommonResult<Vec<BlockLocation>> {
        let prefix = RocksUtils::i64_to_bytes(block_id);
        let iter = self.db.prefix_scan(Self::CF_BLOCK, prefix)?;

        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            let location = Serde::deserialize::<BlockLocation>(&bytes.1)?;
            vec.push(location);
        }

        Ok(vec)
    }

    pub fn new_batch(&self) -> InodeWriteBatch<'_> {
        InodeWriteBatch::new(&self.db)
    }

    pub fn inodes_iter(&self) -> CommonResult<RocksIterator<'_>> {
        self.db.scan(Self::CF_INODES)
    }

    pub fn edges_iter(&self, id: i64) -> CommonResult<RocksIterator<'_>> {
        self.db
            .prefix_scan(Self::CF_EDGES, RocksUtils::i64_to_bytes(id))
    }

    pub fn get_inode(&self, id: i64) -> CommonResult<Option<InodeView>> {
        let bytes = self
            .db
            .get_cf(Self::CF_INODES, RocksUtils::i64_to_bytes(id))?;
        match bytes {
            None => Ok(None),

            Some(v) => {
                let inode: InodeView = Serde::deserialize(&v)?;
                if inode.is_dir() {
                    if let Some(attributes) = self.get_directory_attributes(inode.id())? {
                        inode.set_directory_attributes(attributes);
                    }
                }
                Ok(Some(inode))
            }
        }
    }

    pub fn get_directory_attributes(&self, id: i64) -> CommonResult<Option<DirectoryAttributes>> {
        let bytes = self
            .db
            .get_cf(Self::CF_DIR_ATTRS, RocksUtils::i64_to_bytes(id))?;
        bytes
            .map(|bytes| {
                DirectoryAttributes::decode(&bytes).ok_or_else(|| {
                    CommonError::from(format!("invalid directory attributes for inode {id}"))
                })
            })
            .transpose()
    }

    pub fn initialize_root_directory_attributes(&self) -> CommonResult<()> {
        use crate::master::meta::inode::ROOT_INODE_ID;

        if self.get_directory_attributes(ROOT_INODE_ID)?.is_none() {
            let mut batch = self.new_batch();
            batch.write_directory_attributes(ROOT_INODE_ID, DirectoryAttributes::new(0, 0, 2))?;
            batch.commit()?;
        }
        Ok(())
    }

    pub fn directory_attribute_ids(&self) -> CommonResult<Vec<i64>> {
        self.db
            .bulk_scan(Self::CF_DIR_ATTRS)?
            .map(|entry| {
                let (key, _) = entry?;
                if key.len() != std::mem::size_of::<i64>() {
                    return err_box!("invalid directory attribute key length {}", key.len());
                }
                let bytes: [u8; std::mem::size_of::<i64>()] = key[..]
                    .try_into()
                    .map_err(|_| CommonError::from("invalid directory attribute key"))?;
                Ok(i64::from_le_bytes(bytes))
            })
            .collect()
    }

    pub fn directory_attributes(&self) -> CommonResult<HashMap<i64, DirectoryAttributes>> {
        self.db
            .scan(Self::CF_DIR_ATTRS)?
            .map(|entry| {
                let (key, value) = entry?;
                if key.len() != std::mem::size_of::<i64>() {
                    return err_box!("invalid directory attribute key length {}", key.len());
                }
                let bytes: [u8; std::mem::size_of::<i64>()] = key[..]
                    .try_into()
                    .map_err(|_| CommonError::from("invalid directory attribute key"))?;
                let attributes = DirectoryAttributes::decode(&value)
                    .ok_or_else(|| CommonError::from("invalid directory attribute record"))?;
                Ok((i64::from_le_bytes(bytes), attributes))
            })
            .collect()
    }

    pub fn directory_attributes_for_ids(
        &self,
        mut ids: Vec<i64>,
    ) -> CommonResult<HashMap<i64, DirectoryAttributes>> {
        ids.sort_unstable();
        ids.dedup();
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let keys = ids
            .iter()
            .map(|id| RocksUtils::i64_to_bytes(*id))
            .collect::<Vec<_>>();
        let values = self
            .db
            .multi_get_cf_optional(Self::CF_DIR_ATTRS, keys.iter())?;

        ids.into_iter()
            .zip(values)
            .filter_map(|(id, value)| value.map(|value| (id, value)))
            .map(|(id, value)| {
                let attributes = DirectoryAttributes::decode(&value).ok_or_else(|| {
                    CommonError::from(format!("invalid directory attributes for inode {id}"))
                })?;
                Ok((id, attributes))
            })
            .collect()
    }

    pub fn directory_attributes_migrated(&self) -> CommonResult<bool> {
        let key = [Self::PREFIX_SCHEMA, Self::DIRECTORY_ATTRIBUTE_SCHEMA];
        let version = self.db.get_cf(Self::CF_COMMON, key)?;
        Ok(version.is_some_and(|version| {
            version.as_slice() == [Self::DIRECTORY_ATTRIBUTE_SCHEMA_VERSION]
        }))
    }

    pub fn mark_directory_attributes_migrated(&self) -> CommonResult<()> {
        let key = [Self::PREFIX_SCHEMA, Self::DIRECTORY_ATTRIBUTE_SCHEMA];
        self.db.put_cf(
            Self::CF_COMMON,
            key,
            [Self::DIRECTORY_ATTRIBUTE_SCHEMA_VERSION],
        )
    }

    pub fn batched_multi_get_inodes<'a, K, I>(
        &'a self,
        keys: I,
        sorted_input: bool,
    ) -> CommonResult<Vec<Result<Option<DBPinnableSlice<'a>>, Error>>>
    where
        K: AsRef<[u8]> + 'a + ?Sized,
        I: IntoIterator<Item = &'a K>,
    {
        self.db
            .batched_multi_get_cf(Self::CF_INODES, keys, sorted_input)
    }

    pub fn batched_file_statuses(
        &self,
        parent_path: &str,
        children: Vec<&InodeView>,
    ) -> CommonResult<Vec<FileStatus>> {
        self.batched_file_statuses_with_missing(parent_path, children, false)
    }

    pub fn batched_file_statuses_skip_missing(
        &self,
        parent_path: &str,
        children: Vec<&InodeView>,
    ) -> CommonResult<Vec<FileStatus>> {
        self.batched_file_statuses_with_missing(parent_path, children, true)
    }

    fn batched_file_statuses_with_missing(
        &self,
        parent_path: &str,
        children: Vec<&InodeView>,
        skip_missing: bool,
    ) -> CommonResult<Vec<FileStatus>> {
        if children.is_empty() {
            return Ok(vec![]);
        }

        let mut statuses = (0..children.len()).map(|_| None).collect::<Vec<_>>();
        let mut file_entries = Vec::with_capacity(children.len());
        for (index, child) in children.iter().enumerate() {
            if child.is_file_entry() {
                file_entries.push((index, RocksUtils::i64_to_bytes(child.id())));
            } else {
                statuses[index] =
                    Some(child.to_file_status(&Self::child_path(parent_path, child.name()))?);
            }
        }

        if file_entries.is_empty() {
            return Ok(statuses.into_iter().flatten().collect());
        }

        let values =
            self.batched_multi_get_inodes(file_entries.iter().map(|entry| &entry.1), false)?;
        for (index, value) in values.into_iter().enumerate() {
            let child_index = try_option!(
                file_entries.get(index),
                "batched_file_statuses: missing file entry for batch result index {}",
                index
            )
            .0;
            let child = try_option!(
                children.get(child_index),
                "batched_file_statuses: child index {} out of range, list len {}",
                child_index,
                children.len()
            );
            let bytes = match value? {
                Some(bytes) => bytes,
                None => {
                    if skip_missing {
                        continue;
                    }
                    return Err(FsError::file_not_found(Self::child_path(
                        parent_path,
                        child.name(),
                    ))
                    .into());
                }
            };
            let mut inode: InodeView = Serde::deserialize(bytes.as_ref())?;
            if inode.id() != child.id() {
                return err_box!(
                    "batched_file_statuses: inode id mismatch for path {} (expected {}, got {})",
                    Self::child_path(parent_path, child.name()),
                    child.id(),
                    inode.id()
                );
            }
            inode.change_name(child.name().to_owned());
            statuses[child_index] =
                Some(inode.to_file_status(&Self::child_path(parent_path, child.name()))?);
        }

        Ok(statuses.into_iter().flatten().collect())
    }

    fn child_path(parent_path: &str, name: &str) -> String {
        if parent_path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent_path, name)
        }
    }

    pub fn iter_cf<'a: 'b, 'b>(
        &'a self,
        cf: &str,
    ) -> CommonResult<DBIteratorWithThreadMode<'b, DB>> {
        self.db.iter_cf_opt(cf)
    }

    /// Bulk-scan the entire `inodes` CF with tuned ReadOptions (total_order_seek +
    /// fill_cache(false) + 64 MiB readahead).  Use for snapshot restore.
    pub fn bulk_scan_inodes<'a: 'b, 'b>(
        &'a self,
    ) -> CommonResult<DBIteratorWithThreadMode<'b, DB>> {
        self.db.bulk_scan(Self::CF_INODES)
    }

    /// Bulk-scan the entire `edges` CF with tuned ReadOptions.
    pub fn bulk_scan_edges<'a: 'b, 'b>(&'a self) -> CommonResult<DBIteratorWithThreadMode<'b, DB>> {
        self.db.bulk_scan(Self::CF_EDGES)
    }

    pub fn delete_locations(&self, worker_id: u32) -> CommonResult<Vec<i64>> {
        let block_ids = self.get_block_ids(worker_id)?;

        // delete all worker_id -> block_ids
        let prefix = RocksUtils::u32_to_bytes(worker_id);
        self.db.prefix_delete(Self::CF_LOCATION, prefix)?;

        // delete all block_id -> worker_ids
        for block_id in &block_ids {
            let key = RocksUtils::i64_u32_to_bytes(*block_id, worker_id);
            self.db.delete_cf(Self::CF_BLOCK, key)?;
        }

        Ok(block_ids)
    }

    pub fn apply_block_locations(
        &self,
        locations: Vec<(bool, i64, BlockLocation)>,
    ) -> CommonResult<()> {
        let mut batch = self.new_batch();
        for (add, id, loc) in locations {
            if add {
                batch.add_location(id, &loc)?;
            } else {
                batch.delete_location(id, loc.worker_id)?;
            }
        }

        batch.commit()
    }

    pub fn get_block_ids(&self, worker_id: u32) -> CommonResult<Vec<i64>> {
        let prefix = RocksUtils::u32_to_bytes(worker_id);
        let iter = self.db.prefix_scan(Self::CF_LOCATION, prefix)?;

        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            let location = Serde::deserialize::<i64>(&bytes.1)?;
            vec.push(location);
        }

        Ok(vec)
    }

    pub fn add_mountpoint(&self, id: u32, entry: &MountInfo) -> CommonResult<()> {
        let key = RocksUtils::u8_u32_to_bytes(Self::PREFIX_MOUNT, id);
        let value = Serde::serialize(entry)?;
        self.db.put_cf(Self::CF_COMMON, key, value)
    }

    pub fn remove_mountpoint(&self, id: u32) -> CommonResult<()> {
        let key = RocksUtils::u8_u32_to_bytes(Self::PREFIX_MOUNT, id);
        self.db.delete_cf(Self::CF_COMMON, key)
    }

    pub fn get_mount_info(&self, id: u32) -> CommonResult<Option<MountInfo>> {
        let key = RocksUtils::u8_u32_to_bytes(Self::PREFIX_MOUNT, id);

        let bytes = self.db.get_cf(Self::CF_COMMON, key)?;

        match bytes {
            None => Ok(None),

            Some(v) => {
                let info = MountInfo::decode_persisted(&v)?;
                Ok(Some(info))
            }
        }
    }

    pub fn get_mount_table(&self) -> CommonResult<Vec<MountInfo>> {
        let iter = self.db.prefix_scan(Self::CF_COMMON, [Self::PREFIX_MOUNT])?;
        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            let mnt = MountInfo::decode_persisted(&bytes.1)?;
            vec.push(mnt);
        }

        Ok(vec)
    }

    pub fn get_locks(&self, id: i64) -> CommonResult<LockMeta> {
        let key = RocksUtils::u8_i64_to_bytes(Self::PREFIX_LOCK, id);
        let bytes = self.db.get_cf(Self::CF_COMMON, key)?;

        if let Some(bytes) = bytes {
            let locks: Vec<FileLock> = Serde::deserialize(&bytes)?;
            Ok(LockMeta::with_vec(locks))
        } else {
            Ok(LockMeta::default())
        }
    }

    pub fn set_locks(&self, id: i64, lock: &[FileLock]) -> CommonResult<()> {
        let key = RocksUtils::u8_i64_to_bytes(Self::PREFIX_LOCK, id);
        if lock.is_empty() {
            self.db.delete_cf(RocksInodeStore::CF_COMMON, key)
        } else {
            let value = Serde::serialize(&lock)?;
            self.db.put_cf(RocksInodeStore::CF_COMMON, key, value)
        }
    }

    pub fn get_rocksdb_metrics(&self) -> CommonResult<HashMap<String, u64>> {
        self.db.get_rocksdb_metrics()
    }
}

pub struct RocksInodeStoreSnapshot<'a> {
    reader: DBSnapshotReader<'a>,
}

impl RocksInodeStoreSnapshot<'_> {
    pub fn get_inode(&self, id: i64) -> CommonResult<Option<InodeView>> {
        let mut inode = self.get_inode_raw(id)?;
        if let Some(inode) = &mut inode {
            self.hydrate_directory_attributes(inode)?;
        }
        Ok(inode)
    }

    pub fn get_inode_raw(&self, id: i64) -> CommonResult<Option<InodeView>> {
        let bytes = self
            .reader
            .get_cf(RocksInodeStore::CF_INODES, RocksUtils::i64_to_bytes(id))?;
        bytes
            .map(|bytes| Serde::deserialize::<InodeView>(&bytes))
            .transpose()
    }

    pub fn get_inodes<I>(&self, ids: I) -> CommonResult<Vec<Option<InodeView>>>
    where
        I: IntoIterator<Item = i64>,
    {
        let mut inodes = self.get_inodes_raw(ids)?;
        let directory_ids = inodes
            .iter()
            .flatten()
            .filter(|inode| inode.is_dir())
            .map(InodeView::id)
            .collect::<Vec<_>>();
        if directory_ids.is_empty() {
            return Ok(inodes);
        }

        let values = self.reader.multi_get_cf(
            RocksInodeStore::CF_DIR_ATTRS,
            directory_ids.iter().map(|id| RocksUtils::i64_to_bytes(*id)),
        )?;
        for (inode, value) in inodes
            .iter_mut()
            .flatten()
            .filter(|inode| inode.is_dir())
            .zip(values)
        {
            if let Some(bytes) = value? {
                let attributes = DirectoryAttributes::decode(&bytes).ok_or_else(|| {
                    CommonError::from(format!(
                        "invalid directory attributes for inode {}",
                        inode.id()
                    ))
                })?;
                inode.set_directory_attributes(attributes);
            }
        }
        Ok(inodes)
    }

    pub fn get_inodes_raw<I>(&self, ids: I) -> CommonResult<Vec<Option<InodeView>>>
    where
        I: IntoIterator<Item = i64>,
    {
        let keys = ids
            .into_iter()
            .map(RocksUtils::i64_to_bytes)
            .collect::<Vec<_>>();
        let values = self.reader.multi_get_cf(RocksInodeStore::CF_INODES, keys)?;
        let mut inodes = Vec::with_capacity(values.len());

        for value in values {
            let bytes = value?;
            let inode = bytes
                .map(|bytes| Serde::deserialize::<InodeView>(&bytes))
                .transpose()?;
            inodes.push(inode);
        }

        Ok(inodes)
    }

    pub fn get_child_id(&self, parent_id: i64, name: &str) -> CommonResult<Option<i64>> {
        let key = RocksUtils::i64_str_to_bytes(parent_id, name);
        let bytes = self.reader.get_cf(RocksInodeStore::CF_EDGES, key)?;
        bytes
            .map(|bytes| RocksUtils::i64_from_bytes(&bytes))
            .transpose()
    }

    pub fn get_directory_attributes(&self, id: i64) -> CommonResult<Option<DirectoryAttributes>> {
        let bytes = self
            .reader
            .get_cf(RocksInodeStore::CF_DIR_ATTRS, RocksUtils::i64_to_bytes(id))?;
        bytes
            .map(|bytes| {
                DirectoryAttributes::decode(&bytes).ok_or_else(|| {
                    CommonError::from(format!("invalid directory attributes for inode {id}"))
                })
            })
            .transpose()
    }

    pub fn hydrate_directory_attributes(&self, inode: &mut InodeView) -> CommonResult<()> {
        if inode.is_dir() {
            if let Some(attributes) = self.get_directory_attributes(inode.id())? {
                inode.set_directory_attributes(attributes);
            }
        }
        Ok(())
    }

    pub fn get_child_ids(
        &self,
        parent_id: i64,
        start_after: Option<&str>,
        limit: Option<usize>,
    ) -> CommonResult<Vec<(String, i64)>> {
        let parent_prefix = RocksUtils::i64_to_bytes(parent_id);
        let start = match start_after {
            Some(name) => RocksUtils::i64_str_to_bytes(parent_id, name),
            None => parent_prefix.to_vec(),
        };
        let end = RocksUtils::calculate_end_bytes(&parent_prefix);
        let iter = self
            .reader
            .range_scan(RocksInodeStore::CF_EDGES, &start, &end, false)?;
        let limit = limit.unwrap_or(usize::MAX);
        let mut children = Vec::new();

        for item in iter {
            if children.len() >= limit {
                break;
            }

            let (key, value) = item?;
            let (edge_parent_id, child_name) = RocksUtils::i64_str_from_bytes(&key)?;
            if edge_parent_id != parent_id {
                continue;
            }
            if matches!(start_after, Some(start_after) if child_name.as_str() <= start_after) {
                continue;
            }

            children.push((child_name, RocksUtils::i64_from_bytes(&value)?));
        }

        Ok(children)
    }

    pub fn get_locations(&self, block_id: i64) -> CommonResult<Vec<BlockLocation>> {
        let prefix = RocksUtils::i64_to_bytes(block_id);
        let iter = self.reader.prefix_scan(RocksInodeStore::CF_BLOCK, prefix)?;

        let mut locations = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            locations.push(Serde::deserialize::<BlockLocation>(&bytes.1)?);
        }
        Ok(locations)
    }
}

pub struct InodeChildrenIter<'a> {
    inner: RocksIterator<'a>,
}

impl Iterator for InodeChildrenIter<'_> {
    type Item = CommonResult<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(v) = self.inner.next() {
            match v {
                Err(e) => Some(Err(e.into())),

                Ok(bytes) => {
                    let id = match RocksUtils::i64_from_bytes(&bytes.1) {
                        Ok(id) => id,
                        Err(e) => return Some(Err(e)),
                    };
                    Some(Ok(id))
                }
            }
        } else {
            None
        }
    }
}

pub struct InodeWriteBatch<'a> {
    db: &'a DBEngine,
    batch: WriteBatchWithTransaction<false>,
}

impl<'a> InodeWriteBatch<'a> {
    pub fn new(db: &'a DBEngine) -> Self {
        Self {
            db,
            batch: WriteBatchWithTransaction::<false>::default(),
        }
    }

    fn put_cf<K, V>(&mut self, cf: &str, key: K, value: V) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let cf = self.db.cf(cf)?;
        self.batch.put_cf(cf, key, value);
        Ok(())
    }

    fn delete_cf<K>(&mut self, cf: &str, key: K) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
    {
        let cf = self.db.cf(cf)?;
        self.batch.delete_cf(cf, key);
        Ok(())
    }

    fn merge_cf<K, V>(&mut self, cf: &str, key: K, value: V) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let cf = self.db.cf(cf)?;
        self.batch.merge_cf(cf, key, value);
        Ok(())
    }

    pub fn add_location(&mut self, id: i64, loc: &BlockLocation) -> CommonResult<()> {
        // store with the key of  (block_id, worker_id)
        let key = RocksUtils::i64_u32_to_bytes(id, loc.worker_id);
        let value = Serde::serialize(loc)?;
        self.put_cf(RocksInodeStore::CF_BLOCK, key, value)?;

        // store with the key of (worker_id, block_id)
        let key = RocksUtils::u32_i64_to_bytes(loc.worker_id, id);
        let value = Serde::serialize(&id)?;
        self.put_cf(RocksInodeStore::CF_LOCATION, key, value)
    }

    // Add an inode.
    pub fn write_inode(&mut self, inode: &InodeView) -> CommonResult<()> {
        let key = RocksUtils::i64_to_bytes(inode.id());
        let value = Serde::serialize(inode)?;
        self.put_cf(RocksInodeStore::CF_INODES, key, value)
    }

    pub fn write_directory_attributes(
        &mut self,
        id: i64,
        attributes: DirectoryAttributes,
    ) -> CommonResult<()> {
        self.put_cf(
            RocksInodeStore::CF_DIR_ATTRS,
            RocksUtils::i64_to_bytes(id),
            attributes.encode(),
        )
    }

    pub fn merge_directory_attributes(
        &mut self,
        id: i64,
        delta: DirectoryAttributeDelta,
    ) -> CommonResult<()> {
        self.merge_cf(
            RocksInodeStore::CF_DIR_ATTRS,
            RocksUtils::i64_to_bytes(id),
            delta.encode(),
        )
    }

    pub fn delete_directory_attributes(&mut self, id: i64) -> CommonResult<()> {
        self.delete_cf(RocksInodeStore::CF_DIR_ATTRS, RocksUtils::i64_to_bytes(id))
    }

    // Add an edge to identify the subordinate relationship between inodes
    pub fn add_child(
        &mut self,
        parent_id: i64,
        child_name: &str,
        child_id: i64,
    ) -> CommonResult<()> {
        let key = RocksUtils::i64_str_to_bytes(parent_id, child_name);
        let value = RocksUtils::i64_to_bytes(child_id);
        self.put_cf(RocksInodeStore::CF_EDGES, key, value)
    }

    pub fn delete_inode(&mut self, id: i64) -> CommonResult<()> {
        let key = RocksUtils::i64_to_bytes(id);
        self.delete_cf(RocksInodeStore::CF_INODES, key)
    }

    // Delete a subordinate relationship between an inode
    pub fn delete_child(&mut self, parent_id: i64, child_name: &str) -> CommonResult<()> {
        let key = RocksUtils::i64_str_to_bytes(parent_id, child_name);
        self.delete_cf(RocksInodeStore::CF_EDGES, key)
    }

    // Delete 1 block to store information
    pub fn delete_location(&mut self, id: i64, worker_id: u32) -> CommonResult<()> {
        let key = RocksUtils::i64_u32_to_bytes(id, worker_id);
        self.delete_cf(RocksInodeStore::CF_BLOCK, key)?;

        let key = RocksUtils::u32_i64_to_bytes(worker_id, id);
        self.delete_cf(RocksInodeStore::CF_LOCATION, key)
    }

    pub fn commit(self) -> CommonResult<()> {
        self.db.write_batch(self.batch)
    }
}
