// Copyright 2026 OPPO.
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

use super::*;
use curvine_model::{AccessMode, Provider, StorageType, TtlAction, WriteType};
use curvine_runtime::common::SerdeUtils;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize)]
struct LegacyRenameEntry {
    op_id: u64,
    rpc_id: i64,
    src: String,
    dst: String,
    mtime: i64,
    flags: u32,
}

#[derive(Serialize)]
struct LegacyFreeEntry {
    op_id: u64,
    rpc_id: i64,
    path: String,
    mtime: i64,
}

#[derive(Serialize)]
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

#[derive(Serialize)]
struct LegacyMountEntry {
    op_id: u64,
    rpc_id: i64,
    info: LegacyMountInfo,
}

#[derive(Serialize)]
#[allow(dead_code)]
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

#[derive(Serialize)]
#[allow(dead_code)]
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

#[derive(Serialize)]
struct LegacyJournalBatch {
    seq_id: u64,
    batch: Vec<LegacyJournalEntry>,
}

#[derive(Serialize)]
struct PreRecursiveJournalBatch {
    seq_id: u64,
    batch: Vec<PreRecursiveJournalEntry>,
}

// Rename gained inode ids before Free gained the recursive flag. Bincode's
// positional format requires a dedicated schema for batches from that window.
#[derive(Serialize)]
#[allow(dead_code)]
enum CurrentRenamePreRecursiveFreeJournalEntry {
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

#[derive(Serialize)]
struct CurrentRenamePreRecursiveFreeJournalBatch {
    seq_id: u64,
    batch: Vec<CurrentRenamePreRecursiveFreeJournalEntry>,
}

#[derive(Serialize)]
#[allow(dead_code)]
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

#[derive(Serialize)]
struct LegacyMountJournalBatch {
    seq_id: u64,
    batch: Vec<LegacyMountJournalEntry>,
}

#[test]
fn legacy_rename_batch_uses_zero_exchange_inode_ids() {
    let legacy = LegacyJournalBatch {
        seq_id: 42,
        batch: vec![
            LegacyJournalEntry::Rename(LegacyRenameEntry {
                op_id: 7,
                rpc_id: 9,
                src: "/src".to_string(),
                dst: "/dst".to_string(),
                mtime: 11,
                flags: 0,
            }),
            LegacyJournalEntry::Delete(DeleteEntry {
                op_id: 8,
                rpc_id: 10,
                path: "/deleted".to_string(),
                mtime: 12,
            }),
        ],
    };
    let bytes = SerdeUtils::serialize(&legacy).unwrap();

    assert!(SerdeUtils::deserialize::<JournalBatch>(&bytes).is_err());

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    assert_eq!(batch.seq_id, 42);
    let JournalEntry::Rename(entry) = &batch.batch[0] else {
        panic!("expected rename journal entry");
    };
    assert_eq!(entry.op_id, 7);
    assert_eq!(entry.src_inode_id, 0);
    assert_eq!(entry.dst_inode_id, 0);
    let JournalEntry::Delete(entry) = &batch.batch[1] else {
        panic!("expected delete journal entry");
    };
    assert_eq!(entry.op_id, 8);
    assert_eq!(entry.path, "/deleted");
}

#[test]
fn current_rename_batch_keeps_exchange_inode_ids() {
    let current = JournalBatch {
        seq_id: 43,
        batch: vec![JournalEntry::Rename(RenameEntry {
            op_id: 8,
            rpc_id: 10,
            src: "/src".to_string(),
            dst: "/dst".to_string(),
            mtime: 12,
            flags: 2,
            src_inode_id: 100,
            dst_inode_id: 200,
        })],
    };
    let bytes = SerdeUtils::serialize(&current).unwrap();

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    let JournalEntry::Rename(entry) = &batch.batch[0] else {
        panic!("expected rename journal entry");
    };
    assert_eq!(entry.src_inode_id, 100);
    assert_eq!(entry.dst_inode_id, 200);
}

#[test]
fn legacy_rename_batch_keeps_current_free_schema() {
    let legacy = LegacyJournalBatch {
        seq_id: 44,
        batch: vec![
            LegacyJournalEntry::Rename(LegacyRenameEntry {
                op_id: 9,
                rpc_id: 11,
                src: "/src".to_string(),
                dst: "/dst".to_string(),
                mtime: 13,
                flags: 0,
            }),
            LegacyJournalEntry::Free(FreeEntry {
                op_id: 10,
                rpc_id: 12,
                path: "/free".to_string(),
                mtime: 14,
                recursive: true,
            }),
        ],
    };
    let bytes = SerdeUtils::serialize(&legacy).unwrap();

    assert!(SerdeUtils::deserialize::<JournalBatch>(&bytes).is_err());

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    let JournalEntry::Rename(rename) = &batch.batch[0] else {
        panic!("expected rename journal entry");
    };
    assert_eq!(rename.src_inode_id, 0);
    let JournalEntry::Free(free) = &batch.batch[1] else {
        panic!("expected free journal entry");
    };
    assert!(free.recursive);
}

#[test]
fn current_free_batch_keeps_recursive_flag() {
    let current = JournalBatch {
        seq_id: 44,
        batch: vec![JournalEntry::Free(FreeEntry {
            op_id: 9,
            rpc_id: 11,
            path: "/free".to_string(),
            mtime: 13,
            recursive: true,
        })],
    };
    let bytes = SerdeUtils::serialize(&current).unwrap();

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    let JournalEntry::Free(entry) = &batch.batch[0] else {
        panic!("expected free journal entry");
    };
    assert!(entry.recursive);
}

#[test]
fn legacy_free_batch_defaults_recursive_flag() {
    let legacy = PreRecursiveJournalBatch {
        seq_id: 45,
        batch: vec![
            PreRecursiveJournalEntry::Free(LegacyFreeEntry {
                op_id: 10,
                rpc_id: 12,
                path: "/free".to_string(),
                mtime: 14,
            }),
            PreRecursiveJournalEntry::UfsApplied(UfsAppliedEntry {
                op_id: 11,
                rpc_id: 13,
                term: 2,
                index: 15,
            }),
        ],
    };
    let bytes = SerdeUtils::serialize(&legacy).unwrap();

    assert!(SerdeUtils::deserialize::<JournalBatch>(&bytes).is_err());

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    let JournalEntry::Free(entry) = &batch.batch[0] else {
        panic!("expected free journal entry");
    };
    assert_eq!(entry.op_id, 10);
    assert_eq!(entry.path, "/free");
    assert!(!entry.recursive);
    let JournalEntry::UfsApplied(entry) = &batch.batch[1] else {
        panic!("expected ufs applied journal entry");
    };
    assert_eq!(entry.index, 15);
}

#[test]
fn current_rename_legacy_free_batch_defaults_recursive_flag() {
    let legacy = CurrentRenamePreRecursiveFreeJournalBatch {
        seq_id: 46,
        batch: vec![
            CurrentRenamePreRecursiveFreeJournalEntry::Rename(RenameEntry {
                op_id: 12,
                rpc_id: 14,
                src: "/src".to_string(),
                dst: "/dst".to_string(),
                mtime: 16,
                flags: 2,
                src_inode_id: 100,
                dst_inode_id: 200,
            }),
            CurrentRenamePreRecursiveFreeJournalEntry::Free(LegacyFreeEntry {
                op_id: 13,
                rpc_id: 15,
                path: "/free".to_string(),
                mtime: 17,
            }),
            // UfsApplied is enum tag 15. This is the production failure shape:
            // a current FreeEntry decoder reads this tag as `recursive: bool`.
            CurrentRenamePreRecursiveFreeJournalEntry::UfsApplied(UfsAppliedEntry {
                op_id: 14,
                rpc_id: 16,
                term: 3,
                index: 18,
            }),
        ],
    };
    let bytes = SerdeUtils::serialize(&legacy).unwrap();

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    let JournalEntry::Rename(rename) = &batch.batch[0] else {
        panic!("expected rename journal entry");
    };
    assert_eq!(rename.src_inode_id, 100);
    assert_eq!(rename.dst_inode_id, 200);
    let JournalEntry::Free(free) = &batch.batch[1] else {
        panic!("expected free journal entry");
    };
    assert!(!free.recursive);
    let JournalEntry::UfsApplied(applied) = &batch.batch[2] else {
        panic!("expected ufs applied journal entry");
    };
    assert_eq!(applied.index, 18);
}

#[test]
fn legacy_mount_batch_defaults_write_cache() {
    let legacy = LegacyMountJournalBatch {
        seq_id: 47,
        batch: vec![
            LegacyMountJournalEntry::Mount(LegacyMountEntry {
                op_id: 15,
                rpc_id: 17,
                info: LegacyMountInfo {
                    cv_path: "/flink/user".to_string(),
                    ufs_path: "s3://flink/user".to_string(),
                    mount_id: 19,
                    properties: HashMap::new(),
                    ttl_ms: 86_400_000,
                    ttl_action: TtlAction::Delete,
                    read_verify_ufs: true,
                    storage_type: Some(StorageType::Disk),
                    block_size: Some(134_217_728),
                    replicas: Some(3),
                    write_type: WriteType::CacheMode,
                    provider: Some(Provider::OssHdfs),
                    auto_cache: false,
                    access_mode: AccessMode::ReadWrite,
                },
            }),
            // UfsApplied is enum tag 15. A current MountInfo decoder reads it
            // as the historical record's missing write_cache bool.
            LegacyMountJournalEntry::UfsApplied(UfsAppliedEntry {
                op_id: 16,
                rpc_id: 18,
                term: 4,
                index: 20,
            }),
        ],
    };
    let bytes = SerdeUtils::serialize(&legacy).unwrap();

    assert!(SerdeUtils::deserialize::<JournalBatch>(&bytes).is_err());

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    let JournalEntry::Mount(mount) = &batch.batch[0] else {
        panic!("expected mount journal entry");
    };
    assert_eq!(mount.op_id, 15);
    assert_eq!(mount.info.mount_id, 19);
    assert!(!mount.info.write_cache);
    assert_eq!(mount.info.access_mode, AccessMode::ReadWrite);
    let JournalEntry::UfsApplied(applied) = &batch.batch[1] else {
        panic!("expected ufs applied journal entry");
    };
    assert_eq!(applied.index, 20);
}

#[test]
fn legacy_rename_batch_rejects_trailing_bytes() {
    let legacy = LegacyJournalBatch {
        seq_id: 44,
        batch: vec![LegacyJournalEntry::Rename(LegacyRenameEntry {
            op_id: 9,
            rpc_id: 11,
            src: "/src".to_string(),
            dst: "/dst".to_string(),
            mtime: 13,
            flags: 0,
        })],
    };
    let mut bytes = SerdeUtils::serialize(&legacy).unwrap();
    bytes.extend_from_slice(&[0, 0, 0, 0]);

    assert!(JournalBatch::deserialize_compat(&bytes).is_err());
}
