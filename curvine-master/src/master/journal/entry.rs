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

use crate::master::meta::inode::{InodeDir, InodeFile};
use crate::master::meta::BlockMeta;
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{
    AccessMode, CommitBlock, FileLock, MountInfo, Provider, SetAttrOpts, StorageType, TtlAction,
    WriteType,
};
use curvine_runtime::common::SerdeUtils;
use log::debug;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Cursor;

#[derive(Debug, Clone)]
pub(crate) struct CvMetadataChange {
    pub(crate) op_id: u64,
    pub(crate) path: String,
    pub(crate) include_subtree: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MkdirEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) dir: InodeDir,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ReopenFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct OverWriteFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
}

// Apply for a new block
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AddBlockEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) blocks: Vec<BlockMeta>,
    pub(crate) commit_block: Vec<CommitBlock>,
}

// File writing is completed.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CompleteFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
    pub(crate) commit_blocks: Vec<CommitBlock>,
}

// Rename
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RenameEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) src: String,
    pub(crate) dst: String,
    pub(crate) mtime: i64,
    pub(crate) flags: u32,
    /// Pre-exchange inode ids for idempotent EXCHANGE replay (0 when absent / legacy).
    pub(crate) src_inode_id: i64,
    pub(crate) dst_inode_id: i64,
}

// delete
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DeleteEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) mtime: i64,
}

// mount
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MountEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) info: MountInfo,
}

// umount
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UnMountEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) id: u32,
}

// set attr
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SetAttrEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) opts: SetAttrOpts,
}

// symlink
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SymlinkEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) link: String,
    pub(crate) new_inode: InodeFile,
    pub(crate) force: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LinkEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    /// Link creation time, reused during replay for parent mtime and inode ctime.
    pub(crate) mtime: i64,
    pub(crate) src_path: String,
    pub(crate) dst_path: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SetLocksEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) ino: i64,
    pub(crate) locks: Vec<FileLock>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FreeEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) mtime: i64,
    #[serde(default)]
    pub(crate) recursive: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UfsAppliedEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) term: u64,
    pub(crate) index: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SnapshotEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) node_id: u64,
    pub(crate) dir: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum JournalEntry {
    Mkdir(MkdirEntry),
    CreateFile(CreateFileEntry),
    ReopenFile(ReopenFileEntry),
    OverWriteFile(OverWriteFileEntry),
    AddBlock(AddBlockEntry),
    CompleteFile(CompleteFileEntry),
    Rename(RenameEntry),
    Delete(DeleteEntry),
    Mount(MountEntry),
    UnMount(UnMountEntry),
    SetAttr(SetAttrEntry),
    Symlink(SymlinkEntry),
    Link(LinkEntry),
    SetLocks(SetLocksEntry),
    Free(FreeEntry),
    UfsApplied(UfsAppliedEntry),
    Snapshot(SnapshotEntry),
}

impl JournalEntry {
    pub fn op_id(&self) -> u64 {
        match self {
            JournalEntry::Mkdir(e) => e.op_id,
            JournalEntry::CreateFile(e) => e.op_id,
            JournalEntry::ReopenFile(e) => e.op_id,
            JournalEntry::OverWriteFile(e) => e.op_id,
            JournalEntry::AddBlock(e) => e.op_id,
            JournalEntry::CompleteFile(e) => e.op_id,
            JournalEntry::Rename(e) => e.op_id,
            JournalEntry::Delete(e) => e.op_id,
            JournalEntry::Mount(e) => e.op_id,
            JournalEntry::UnMount(e) => e.op_id,
            JournalEntry::SetAttr(e) => e.op_id,
            JournalEntry::Symlink(e) => e.op_id,
            JournalEntry::Link(e) => e.op_id,
            JournalEntry::SetLocks(e) => e.op_id,
            JournalEntry::Free(e) => e.op_id,
            JournalEntry::UfsApplied(e) => e.op_id,
            JournalEntry::Snapshot(e) => e.op_id,
        }
    }

    pub fn rpc_id(&self) -> i64 {
        match self {
            JournalEntry::Mkdir(e) => e.rpc_id,
            JournalEntry::CreateFile(e) => e.rpc_id,
            JournalEntry::ReopenFile(e) => e.rpc_id,
            JournalEntry::OverWriteFile(e) => e.rpc_id,
            JournalEntry::AddBlock(e) => e.rpc_id,
            JournalEntry::CompleteFile(e) => e.rpc_id,
            JournalEntry::Rename(e) => e.rpc_id,
            JournalEntry::Delete(e) => e.rpc_id,
            JournalEntry::Mount(e) => e.rpc_id,
            JournalEntry::UnMount(e) => e.rpc_id,
            JournalEntry::SetAttr(e) => e.rpc_id,
            JournalEntry::Symlink(e) => e.rpc_id,
            JournalEntry::Link(e) => e.rpc_id,
            JournalEntry::SetLocks(e) => e.rpc_id,
            JournalEntry::Free(e) => e.rpc_id,
            JournalEntry::UfsApplied(e) => e.rpc_id,
            JournalEntry::Snapshot(e) => e.rpc_id,
        }
    }

    pub fn inode_id(&self) -> Option<i64> {
        match self {
            JournalEntry::Mkdir(e) => Some(e.dir.id),
            JournalEntry::CreateFile(e) => Some(e.file.id),
            JournalEntry::ReopenFile(e) => Some(e.file.id),
            JournalEntry::OverWriteFile(e) => Some(e.file.id),
            JournalEntry::CompleteFile(e) => Some(e.file.id),
            JournalEntry::Symlink(e) => Some(e.new_inode.id),
            JournalEntry::SetLocks(e) => Some(e.ino),
            _ => None,
        }
    }

    pub(crate) fn cv_metadata_changes(&self) -> Vec<CvMetadataChange> {
        match self {
            JournalEntry::Mkdir(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::CreateFile(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::ReopenFile(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::OverWriteFile(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::AddBlock(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::CompleteFile(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::Rename(e) => vec![
                CvMetadataChange::subtree(e.op_id, &e.src),
                CvMetadataChange::subtree(e.op_id, &e.dst),
            ],
            JournalEntry::Delete(e) => vec![CvMetadataChange::subtree(e.op_id, &e.path)],
            JournalEntry::SetAttr(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::Symlink(e) => vec![CvMetadataChange::single(e.op_id, &e.link)],
            JournalEntry::Link(e) => vec![
                CvMetadataChange::single(e.op_id, &e.src_path),
                CvMetadataChange::single(e.op_id, &e.dst_path),
            ],
            JournalEntry::Free(e) => vec![CvMetadataChange::subtree(e.op_id, &e.path)],
            JournalEntry::Mount(_)
            | JournalEntry::UnMount(_)
            | JournalEntry::SetLocks(_)
            | JournalEntry::UfsApplied(_)
            | JournalEntry::Snapshot(_) => Vec::new(),
        }
    }
}

impl CvMetadataChange {
    fn single(op_id: u64, path: &str) -> Self {
        Self {
            op_id,
            path: path.to_string(),
            include_subtree: false,
        }
    }

    fn subtree(op_id: u64, path: &str) -> Self {
        Self {
            op_id,
            path: path.to_string(),
            include_subtree: true,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct JournalBatch {
    pub(crate) seq_id: u64,
    pub(crate) batch: Vec<JournalEntry>,
}

impl JournalBatch {
    pub(crate) fn deserialize_compat(bytes: &[u8]) -> CommonResult<Self> {
        let current_err = match SerdeUtils::deserialize(bytes) {
            Ok(batch) => return Ok(batch),
            Err(error) => error,
        };
        let legacy_mount_err = match deserialize_batch::<LegacyMountJournalBatch>(bytes) {
            Ok(batch) => {
                debug!(
                    "replaying legacy journal batch with legacy mount schema, seq_id={}",
                    batch.seq_id
                );
                return Ok(batch.into());
            }
            Err(error) => error,
        };
        let legacy_rename_err = match deserialize_batch::<LegacyJournalBatch>(bytes) {
            Ok(batch) => {
                debug!(
                    "replaying legacy journal batch with legacy rename schema, seq_id={}",
                    batch.seq_id
                );
                return Ok(batch.into());
            }
            Err(error) => error,
        };
        let legacy_free_err = match deserialize_batch::<LegacyFreeJournalBatch>(bytes) {
            Ok(batch) => {
                debug!(
                    "replaying legacy journal batch with legacy free schema, seq_id={}",
                    batch.seq_id
                );
                return Ok(batch.into());
            }
            Err(error) => error,
        };
        match deserialize_batch::<PreRecursiveJournalBatch>(bytes) {
            Ok(batch) => {
                debug!(
                    "replaying legacy journal batch with legacy rename and free schemas, seq_id={}",
                    batch.seq_id
                );
                Ok(batch.into())
            }
            Err(pre_recursive_err) => err_box!(
                "failed to deserialize journal batch with current or legacy schemas: current={}, legacy_mount={}, legacy_rename={}, legacy_free={}, pre_recursive={}",
                current_err,
                legacy_mount_err,
                legacy_rename_err,
                legacy_free_err,
                pre_recursive_err
            ),
        }
    }

    pub fn new(seq_id: u64) -> Self {
        Self {
            seq_id,
            batch: vec![],
        }
    }

    pub fn push(&mut self, entry: JournalEntry) {
        self.batch.push(entry)
    }

    pub fn len(&self) -> usize {
        self.batch.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn next(&mut self) {
        self.seq_id += 1;
        self.batch.clear();
    }
}

// bincode encodes struct fields positionally, so appending fields to RenameEntry
// cannot be made backward compatible with serde(default). Keep this schema only
// for replaying entries written before exchange inode ids were introduced.
#[derive(Deserialize)]
struct LegacyRenameEntry {
    op_id: u64,
    rpc_id: i64,
    src: String,
    dst: String,
    mtime: i64,
    flags: u32,
}

// FreeEntry::recursive was appended in #721. Old bincode entries do not have
// this field, so decoding them with the current type consumes the next entry's
// enum tag as a bool. This schema is kept separate because batches written
// after #721 can still contain the legacy RenameEntry layout.
#[derive(Deserialize)]
struct LegacyFreeEntry {
    op_id: u64,
    rpc_id: i64,
    path: String,
    mtime: i64,
}

// MountInfo::write_cache was appended in #1497. Journal batches serialize the
// entry stream without a length prefix, so the next entry tag is otherwise
// decoded as this missing bool. Keep the pre-#1497 layout frozen for replay.
#[derive(Deserialize)]
struct LegacyMountInfo {
    cv_path: String,
    ufs_path: String,
    mount_id: u32,
    properties: HashMap<String, String>,
    ttl_ms: i64,
    ttl_action: TtlAction,
    read_verify_ufs: bool,
    storage_type: Option<StorageType>,
    block_size: Option<i64>,
    replicas: Option<i32>,
    write_type: WriteType,
    provider: Option<Provider>,
    auto_cache: bool,
    access_mode: AccessMode,
}

impl From<LegacyMountInfo> for MountInfo {
    fn from(info: LegacyMountInfo) -> Self {
        Self {
            cv_path: info.cv_path,
            ufs_path: info.ufs_path,
            mount_id: info.mount_id,
            properties: info.properties,
            ttl_ms: info.ttl_ms,
            ttl_action: info.ttl_action,
            read_verify_ufs: info.read_verify_ufs,
            storage_type: info.storage_type,
            block_size: info.block_size,
            replicas: info.replicas,
            write_type: info.write_type,
            provider: info.provider,
            auto_cache: info.auto_cache,
            access_mode: info.access_mode,
            write_cache: false,
        }
    }
}

#[derive(Deserialize)]
struct LegacyMountEntry {
    op_id: u64,
    rpc_id: i64,
    info: LegacyMountInfo,
}

#[derive(Deserialize)]
enum LegacyJournalEntry {
    Mkdir(MkdirEntry),
    CreateFile(CreateFileEntry),
    ReopenFile(ReopenFileEntry),
    OverWriteFile(OverWriteFileEntry),
    AddBlock(AddBlockEntry),
    CompleteFile(CompleteFileEntry),
    Rename(LegacyRenameEntry),
    Delete(DeleteEntry),
    Mount(MountEntry),
    UnMount(UnMountEntry),
    SetAttr(SetAttrEntry),
    Symlink(SymlinkEntry),
    Link(LinkEntry),
    SetLocks(SetLocksEntry),
    Free(FreeEntry),
    UfsApplied(UfsAppliedEntry),
    Snapshot(SnapshotEntry),
}

#[derive(Deserialize)]
struct LegacyJournalBatch {
    seq_id: u64,
    batch: Vec<LegacyJournalEntry>,
}

#[derive(Deserialize)]
enum LegacyMountJournalEntry {
    Mkdir(MkdirEntry),
    CreateFile(CreateFileEntry),
    ReopenFile(ReopenFileEntry),
    OverWriteFile(OverWriteFileEntry),
    AddBlock(AddBlockEntry),
    CompleteFile(CompleteFileEntry),
    Rename(RenameEntry),
    Delete(DeleteEntry),
    Mount(LegacyMountEntry),
    UnMount(UnMountEntry),
    SetAttr(SetAttrEntry),
    Symlink(SymlinkEntry),
    Link(LinkEntry),
    SetLocks(SetLocksEntry),
    Free(FreeEntry),
    UfsApplied(UfsAppliedEntry),
    Snapshot(SnapshotEntry),
}

#[derive(Deserialize)]
struct LegacyMountJournalBatch {
    seq_id: u64,
    batch: Vec<LegacyMountJournalEntry>,
}

// FreeEntry::recursive was added after RenameEntry gained exchange inode ids.
// This schema replays batches written during that interval.
#[derive(Deserialize)]
enum LegacyFreeJournalEntry {
    Mkdir(MkdirEntry),
    CreateFile(CreateFileEntry),
    ReopenFile(ReopenFileEntry),
    OverWriteFile(OverWriteFileEntry),
    AddBlock(AddBlockEntry),
    CompleteFile(CompleteFileEntry),
    Rename(RenameEntry),
    Delete(DeleteEntry),
    Mount(MountEntry),
    UnMount(UnMountEntry),
    SetAttr(SetAttrEntry),
    Symlink(SymlinkEntry),
    Link(LinkEntry),
    SetLocks(SetLocksEntry),
    Free(LegacyFreeEntry),
    UfsApplied(UfsAppliedEntry),
    Snapshot(SnapshotEntry),
}

#[derive(Deserialize)]
struct LegacyFreeJournalBatch {
    seq_id: u64,
    batch: Vec<LegacyFreeJournalEntry>,
}

#[derive(Deserialize)]
enum PreRecursiveJournalEntry {
    Mkdir(MkdirEntry),
    CreateFile(CreateFileEntry),
    ReopenFile(ReopenFileEntry),
    OverWriteFile(OverWriteFileEntry),
    AddBlock(AddBlockEntry),
    CompleteFile(CompleteFileEntry),
    Rename(LegacyRenameEntry),
    Delete(DeleteEntry),
    Mount(MountEntry),
    UnMount(UnMountEntry),
    SetAttr(SetAttrEntry),
    Symlink(SymlinkEntry),
    Link(LinkEntry),
    SetLocks(SetLocksEntry),
    Free(LegacyFreeEntry),
    UfsApplied(UfsAppliedEntry),
    Snapshot(SnapshotEntry),
}

#[derive(Deserialize)]
struct PreRecursiveJournalBatch {
    seq_id: u64,
    batch: Vec<PreRecursiveJournalEntry>,
}

fn deserialize_batch<T: DeserializeOwned>(bytes: &[u8]) -> CommonResult<T> {
    let mut reader = Cursor::new(bytes);
    let batch = SerdeUtils::deserialize_from(&mut reader)?;
    if reader.position() != bytes.len() as u64 {
        return err_box!("legacy journal batch has trailing bytes");
    }
    Ok(batch)
}

impl From<LegacyJournalBatch> for JournalBatch {
    fn from(batch: LegacyJournalBatch) -> Self {
        Self {
            seq_id: batch.seq_id,
            batch: batch.batch.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<LegacyMountJournalBatch> for JournalBatch {
    fn from(batch: LegacyMountJournalBatch) -> Self {
        Self {
            seq_id: batch.seq_id,
            batch: batch.batch.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<LegacyMountJournalEntry> for JournalEntry {
    fn from(entry: LegacyMountJournalEntry) -> Self {
        match entry {
            LegacyMountJournalEntry::Mkdir(entry) => Self::Mkdir(entry),
            LegacyMountJournalEntry::CreateFile(entry) => Self::CreateFile(entry),
            LegacyMountJournalEntry::ReopenFile(entry) => Self::ReopenFile(entry),
            LegacyMountJournalEntry::OverWriteFile(entry) => Self::OverWriteFile(entry),
            LegacyMountJournalEntry::AddBlock(entry) => Self::AddBlock(entry),
            LegacyMountJournalEntry::CompleteFile(entry) => Self::CompleteFile(entry),
            LegacyMountJournalEntry::Rename(entry) => Self::Rename(entry),
            LegacyMountJournalEntry::Delete(entry) => Self::Delete(entry),
            LegacyMountJournalEntry::Mount(entry) => Self::Mount(MountEntry {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                info: entry.info.into(),
            }),
            LegacyMountJournalEntry::UnMount(entry) => Self::UnMount(entry),
            LegacyMountJournalEntry::SetAttr(entry) => Self::SetAttr(entry),
            LegacyMountJournalEntry::Symlink(entry) => Self::Symlink(entry),
            LegacyMountJournalEntry::Link(entry) => Self::Link(entry),
            LegacyMountJournalEntry::SetLocks(entry) => Self::SetLocks(entry),
            LegacyMountJournalEntry::Free(entry) => Self::Free(entry),
            LegacyMountJournalEntry::UfsApplied(entry) => Self::UfsApplied(entry),
            LegacyMountJournalEntry::Snapshot(entry) => Self::Snapshot(entry),
        }
    }
}

impl From<LegacyFreeJournalBatch> for JournalBatch {
    fn from(batch: LegacyFreeJournalBatch) -> Self {
        Self {
            seq_id: batch.seq_id,
            batch: batch.batch.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<PreRecursiveJournalBatch> for JournalBatch {
    fn from(batch: PreRecursiveJournalBatch) -> Self {
        Self {
            seq_id: batch.seq_id,
            batch: batch
                .batch
                .into_iter()
                .map(LegacyJournalEntry::from)
                .map(JournalEntry::from)
                .collect(),
        }
    }
}

impl From<PreRecursiveJournalEntry> for LegacyJournalEntry {
    fn from(entry: PreRecursiveJournalEntry) -> Self {
        match entry {
            PreRecursiveJournalEntry::Mkdir(entry) => Self::Mkdir(entry),
            PreRecursiveJournalEntry::CreateFile(entry) => Self::CreateFile(entry),
            PreRecursiveJournalEntry::ReopenFile(entry) => Self::ReopenFile(entry),
            PreRecursiveJournalEntry::OverWriteFile(entry) => Self::OverWriteFile(entry),
            PreRecursiveJournalEntry::AddBlock(entry) => Self::AddBlock(entry),
            PreRecursiveJournalEntry::CompleteFile(entry) => Self::CompleteFile(entry),
            PreRecursiveJournalEntry::Rename(entry) => Self::Rename(entry),
            PreRecursiveJournalEntry::Delete(entry) => Self::Delete(entry),
            PreRecursiveJournalEntry::Mount(entry) => Self::Mount(entry),
            PreRecursiveJournalEntry::UnMount(entry) => Self::UnMount(entry),
            PreRecursiveJournalEntry::SetAttr(entry) => Self::SetAttr(entry),
            PreRecursiveJournalEntry::Symlink(entry) => Self::Symlink(entry),
            PreRecursiveJournalEntry::Link(entry) => Self::Link(entry),
            PreRecursiveJournalEntry::SetLocks(entry) => Self::SetLocks(entry),
            PreRecursiveJournalEntry::Free(entry) => Self::Free(FreeEntry {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                path: entry.path,
                mtime: entry.mtime,
                recursive: false,
            }),
            PreRecursiveJournalEntry::UfsApplied(entry) => Self::UfsApplied(entry),
            PreRecursiveJournalEntry::Snapshot(entry) => Self::Snapshot(entry),
        }
    }
}

impl From<LegacyFreeJournalEntry> for JournalEntry {
    fn from(entry: LegacyFreeJournalEntry) -> Self {
        match entry {
            LegacyFreeJournalEntry::Mkdir(entry) => Self::Mkdir(entry),
            LegacyFreeJournalEntry::CreateFile(entry) => Self::CreateFile(entry),
            LegacyFreeJournalEntry::ReopenFile(entry) => Self::ReopenFile(entry),
            LegacyFreeJournalEntry::OverWriteFile(entry) => Self::OverWriteFile(entry),
            LegacyFreeJournalEntry::AddBlock(entry) => Self::AddBlock(entry),
            LegacyFreeJournalEntry::CompleteFile(entry) => Self::CompleteFile(entry),
            LegacyFreeJournalEntry::Rename(entry) => Self::Rename(entry),
            LegacyFreeJournalEntry::Delete(entry) => Self::Delete(entry),
            LegacyFreeJournalEntry::Mount(entry) => Self::Mount(entry),
            LegacyFreeJournalEntry::UnMount(entry) => Self::UnMount(entry),
            LegacyFreeJournalEntry::SetAttr(entry) => Self::SetAttr(entry),
            LegacyFreeJournalEntry::Symlink(entry) => Self::Symlink(entry),
            LegacyFreeJournalEntry::Link(entry) => Self::Link(entry),
            LegacyFreeJournalEntry::SetLocks(entry) => Self::SetLocks(entry),
            LegacyFreeJournalEntry::Free(entry) => Self::Free(FreeEntry {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                path: entry.path,
                mtime: entry.mtime,
                recursive: false,
            }),
            LegacyFreeJournalEntry::UfsApplied(entry) => Self::UfsApplied(entry),
            LegacyFreeJournalEntry::Snapshot(entry) => Self::Snapshot(entry),
        }
    }
}

impl From<LegacyJournalEntry> for JournalEntry {
    fn from(entry: LegacyJournalEntry) -> Self {
        match entry {
            LegacyJournalEntry::Mkdir(entry) => Self::Mkdir(entry),
            LegacyJournalEntry::CreateFile(entry) => Self::CreateFile(entry),
            LegacyJournalEntry::ReopenFile(entry) => Self::ReopenFile(entry),
            LegacyJournalEntry::OverWriteFile(entry) => Self::OverWriteFile(entry),
            LegacyJournalEntry::AddBlock(entry) => Self::AddBlock(entry),
            LegacyJournalEntry::CompleteFile(entry) => Self::CompleteFile(entry),
            LegacyJournalEntry::Rename(entry) => Self::Rename(RenameEntry {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                src: entry.src,
                dst: entry.dst,
                mtime: entry.mtime,
                flags: entry.flags,
                src_inode_id: 0,
                dst_inode_id: 0,
            }),
            LegacyJournalEntry::Delete(entry) => Self::Delete(entry),
            LegacyJournalEntry::Mount(entry) => Self::Mount(entry),
            LegacyJournalEntry::UnMount(entry) => Self::UnMount(entry),
            LegacyJournalEntry::SetAttr(entry) => Self::SetAttr(entry),
            LegacyJournalEntry::Symlink(entry) => Self::Symlink(entry),
            LegacyJournalEntry::Link(entry) => Self::Link(entry),
            LegacyJournalEntry::SetLocks(entry) => Self::SetLocks(entry),
            LegacyJournalEntry::Free(entry) => Self::Free(entry),
            LegacyJournalEntry::UfsApplied(entry) => Self::UfsApplied(entry),
            LegacyJournalEntry::Snapshot(entry) => Self::Snapshot(entry),
        }
    }
}
