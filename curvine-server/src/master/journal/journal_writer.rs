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

#![allow(clippy::result_large_err)]

use crate::master::journal::read_barrier::JournalReadBarrier;
use crate::master::journal::*;
use crate::master::meta::inode::{InodeDir, InodeFile, InodePath};
use crate::master::meta::FsDir;
use crate::master::{Master, MasterMetrics};
use curvine_common::conf::JournalConf;
use curvine_common::raft::RaftClient;
use curvine_common::state::{CommitBlock, FileLock, MountInfo, RenameFlags, SetAttrOpts};
use curvine_common::FsResult;
use log::{debug, error};
use orpc::common::LocalTime;
use orpc::sync::channel::{BlockingChannel, BlockingReceiver, BlockingSender};
use orpc::sync::AtomicCounter;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

thread_local! {
    static JOURNAL_PERMIT_SCOPES: RefCell<Vec<Vec<JournalPermit>>> = const { RefCell::new(Vec::new()) };
}

// Write metadata operation logs.
pub struct JournalWriter {
    enable: bool,
    node_id: u64,
    sender: BlockingSender<QueuedJournalEntry>,
    permit_pool: Arc<JournalPermitPool>,
    read_barrier: Arc<JournalReadBarrier>,
    metrics: &'static MasterMetrics,
    receiver: Option<Mutex<BlockingReceiver<QueuedJournalEntry>>>,

    snapshot_entries: u64,
    entries_since_snapshot: AtomicCounter,
    snapshot_requested: AtomicBool,
    snapshot_in_progress: AtomicBool,
}

impl JournalWriter {
    pub fn new(testing: bool, client: RaftClient, conf: &JournalConf) -> FsResult<Self> {
        let node_id = conf.node_id()?;
        let metrics = Master::get_metrics()?;
        let permit_pool = Arc::new(JournalPermitPool::new(conf.writer_channel_size));
        let (sender, receiver) = BlockingChannel::new(conf.writer_channel_size).split();

        let receiver = if !testing {
            // Start the send log thread.
            let task = SenderTask::new(client, conf, 0)?;
            task.spawn(receiver)?;
            None
        } else {
            Some(Mutex::new(receiver))
        };

        Ok(Self {
            enable: conf.enable,
            node_id,
            sender,
            permit_pool,
            read_barrier: Arc::new(JournalReadBarrier::new()),
            metrics,
            receiver,
            snapshot_entries: conf.snapshot_entries,
            entries_since_snapshot: AtomicCounter::new(0),
            snapshot_requested: AtomicBool::new(false),
            snapshot_in_progress: AtomicBool::new(false),
        })
    }

    fn send_inner(&self, entry: JournalEntry) -> FsResult<()> {
        let permit = match Self::take_scoped_permit() {
            Some(permit) => permit,
            None => self.reserve()?,
        };
        self.enqueue_with_permit(permit, entry)
    }

    fn enqueue_after_commit(&self, entry: JournalEntry) -> FsResult<()> {
        if let Err(error) = self.send_inner(entry) {
            error!(
                "journal enqueue failed after metadata commit; aborting master: {}",
                error
            );
            std::process::abort();
        }
        Ok(())
    }

    pub fn reserve(&self) -> FsResult<JournalPermit> {
        Ok(JournalPermit {
            _permit: self.permit_pool.try_acquire()?,
        })
    }

    pub fn begin_metadata_catch_up(&self, applied_index: u64, required_index: u64) {
        self.read_barrier
            .begin_catch_up(applied_index, required_index);
    }

    pub fn require_metadata_catch_up(&self, required_index: u64) {
        self.read_barrier.require_catch_up(required_index);
    }

    pub fn advance_metadata_applied(&self, applied_index: u64) {
        self.read_barrier.advance_applied(applied_index);
    }

    pub fn advance_metadata_catch_up(&self, applied_index: u64) {
        self.read_barrier.advance_catch_up(applied_index);
    }

    pub fn ensure_metadata_current(&self) -> FsResult<()> {
        self.read_barrier.ensure_current()
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn reserve_scope(&self, count: usize) -> FsResult<JournalPermitScope> {
        let mut permits = Vec::with_capacity(count);
        if self.enable {
            for _ in 0..count {
                permits.push(self.reserve()?);
            }
        }

        JOURNAL_PERMIT_SCOPES.with(|scopes| scopes.borrow_mut().push(permits));
        Ok(JournalPermitScope {})
    }

    fn take_scoped_permit() -> Option<JournalPermit> {
        JOURNAL_PERMIT_SCOPES.with(|scopes| {
            let mut scopes = scopes.borrow_mut();
            scopes.last_mut().and_then(|permits| permits.pop())
        })
    }

    pub fn enqueue_with_permit(&self, permit: JournalPermit, entry: JournalEntry) -> FsResult<()> {
        debug!("send_entry {:?}", entry);
        self.sender.send(QueuedJournalEntry {
            entry,
            _permit: permit,
        })?;
        self.metrics.journal_queue_len.inc();
        Ok(())
    }

    fn send(&self, _fs_dir: &FsDir, entry: JournalEntry) -> FsResult<()> {
        if self.enable {
            self.enqueue_after_commit(entry)?;
            self.request_snapshot_if_needed();
        }
        Ok(())
    }

    fn request_snapshot_if_needed(&self) {
        if self.snapshot_entries == 0 {
            return;
        }

        let entries = self.entries_since_snapshot.add_and_get(1);
        if entries < self.snapshot_entries {
            return;
        }

        self.entries_since_snapshot.set(0);
        self.snapshot_requested.store(true, Ordering::SeqCst);
    }

    pub fn try_begin_snapshot(&self) -> bool {
        if !self.snapshot_requested.swap(false, Ordering::SeqCst) {
            return false;
        }

        if self
            .snapshot_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            true
        } else {
            self.snapshot_requested.store(true, Ordering::SeqCst);
            false
        }
    }

    pub fn finish_snapshot(&self, success: bool) {
        if !success {
            self.snapshot_requested.store(true, Ordering::SeqCst);
        }
        self.snapshot_in_progress.store(false, Ordering::SeqCst);
    }

    pub fn enqueue_snapshot_with_permit(
        &self,
        permit: JournalPermit,
        op_id: u64,
        dir: String,
    ) -> FsResult<()> {
        self.enqueue_with_permit(
            permit,
            JournalEntry::Snapshot(SnapshotEntry {
                op_id,
                rpc_id: 0,
                node_id: self.node_id,
                dir,
            }),
        )
    }

    pub fn log_mkdir(&self, fs_dir: &FsDir, path: impl AsRef<str>, dir: &InodeDir) -> FsResult<()> {
        let entry = MkdirEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            dir: dir.clone(),
        };
        self.send(fs_dir, JournalEntry::Mkdir(entry))
    }

    pub fn log_create_file(&self, fs_dir: &FsDir, inp: &InodePath) -> FsResult<()> {
        let entry = CreateFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: inp.path().to_string(),
            file: inp.clone_last_file()?,
        };
        self.send(fs_dir, JournalEntry::CreateFile(entry))
    }

    pub fn log_reopen_file<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        file: &InodeFile,
    ) -> FsResult<()> {
        let entry = ReopenFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            file: file.clone(),
        };
        self.send(fs_dir, JournalEntry::ReopenFile(entry))
    }

    pub fn log_add_block<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        file: &InodeFile,
        commit_block: Vec<CommitBlock>,
    ) -> FsResult<()> {
        let entry = AddBlockEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            blocks: file.blocks.clone(),
            commit_block,
        };
        self.send(fs_dir, JournalEntry::AddBlock(entry))
    }

    pub fn log_complete_file<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        file: &InodeFile,
        commit_blocks: Vec<CommitBlock>,
    ) -> FsResult<()> {
        let entry = CompleteFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            file: file.clone(),
            commit_blocks,
        };
        self.send(fs_dir, JournalEntry::CompleteFile(entry))
    }

    pub fn log_overwrite_file(&self, fs_dir: &FsDir, inp: &InodePath) -> FsResult<()> {
        let entry = OverWriteFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: inp.path().to_string(),
            file: inp.clone_last_file()?,
        };
        self.send(fs_dir, JournalEntry::OverWriteFile(entry))
    }

    pub fn log_rename<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        src: P,
        dst: P,
        mtime: i64,
        flags: RenameFlags,
    ) -> FsResult<()> {
        let entry = RenameEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            src: src.as_ref().to_string(),
            dst: dst.as_ref().to_string(),
            mtime,
            flags: flags.value(),
        };
        self.send(fs_dir, JournalEntry::Rename(entry))
    }

    pub fn log_delete<P: AsRef<str>>(&self, fs_dir: &FsDir, path: P, mtime: i64) -> FsResult<()> {
        let entry = DeleteEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            mtime,
        };
        self.send(fs_dir, JournalEntry::Delete(entry))
    }

    pub fn log_free<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        mtime: i64,
        recursive: bool,
    ) -> FsResult<()> {
        let entry = FreeEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            mtime,
            recursive,
        };
        self.send(fs_dir, JournalEntry::Free(entry))
    }

    pub fn log_mount_by_id(&self, op_id: u64, info: MountInfo) -> FsResult<()> {
        let entry = MountEntry {
            op_id,
            rpc_id: 0,
            info,
        };
        if self.enable {
            self.enqueue_after_commit(JournalEntry::Mount(entry))?;
            self.request_snapshot_if_needed();
        }
        Ok(())
    }

    pub fn log_unmount_by_id(&self, op_id: u64, id: u32) -> FsResult<()> {
        let entry = UnMountEntry {
            op_id,
            rpc_id: 0,
            id,
        };
        if self.enable {
            self.enqueue_after_commit(JournalEntry::UnMount(entry))?;
            self.request_snapshot_if_needed();
        }
        Ok(())
    }

    pub fn log_set_attr(&self, fs_dir: &FsDir, inp: &InodePath, opts: SetAttrOpts) -> FsResult<()> {
        let entry = SetAttrEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: inp.path().to_string(),
            opts,
        };
        self.send(fs_dir, JournalEntry::SetAttr(entry))
    }

    pub fn log_symlink<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        link: P,
        new_inode: InodeFile,
        force: bool,
    ) -> FsResult<()> {
        let entry = SymlinkEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            link: link.as_ref().to_string(),
            new_inode,
            force,
        };
        self.send(fs_dir, JournalEntry::Symlink(entry))
    }

    pub fn log_link<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        src_path: P,
        dst_path: P,
    ) -> FsResult<()> {
        let entry = LinkEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            mtime: LocalTime::mills() as i64,
            src_path: src_path.as_ref().to_string(),
            dst_path: dst_path.as_ref().to_string(),
        };
        self.send(fs_dir, JournalEntry::Link(entry))
    }

    pub fn log_ufs_applied(&self, op_id: u64, term: u64, index: u64) -> FsResult<()> {
        if !self.enable {
            return Ok(());
        }

        let entry = UfsAppliedEntry {
            op_id,
            rpc_id: 0,
            term,
            index,
        };
        self.enqueue_after_commit(JournalEntry::UfsApplied(entry))?;

        Ok(())
    }

    pub fn log_set_locks(&self, fs_dir: &FsDir, ino: i64, locks: Vec<FileLock>) -> FsResult<()> {
        let entry = SetLocksEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            ino,
            locks,
        };
        self.send(fs_dir, JournalEntry::SetLocks(entry))
    }

    pub fn log_set_locks_by_id(&self, op_id: u64, ino: i64, locks: Vec<FileLock>) -> FsResult<()> {
        let entry = SetLocksEntry {
            op_id,
            rpc_id: 0,
            ino,
            locks,
        };
        if self.enable {
            self.enqueue_after_commit(JournalEntry::SetLocks(entry))?;
            self.request_snapshot_if_needed();
        }
        Ok(())
    }

    // for testing
    pub fn take_entries(&self) -> Vec<JournalEntry> {
        let mut entries = vec![];
        let Some(receiver) = self.receiver.as_ref() else {
            return entries;
        };
        let receiver = match receiver.lock() {
            Ok(receiver) => receiver,
            Err(e) => {
                log::error!("failed to take journal entries: {}", e);
                return entries;
            }
        };
        while let Ok(v) = receiver.try_recv() {
            self.metrics.journal_queue_len.dec();
            entries.push(v.entry);
        }
        entries
    }
}

pub struct QueuedJournalEntry {
    pub(crate) entry: JournalEntry,
    _permit: JournalPermit,
}

pub struct JournalPermit {
    _permit: Option<JournalQueuePermit>,
}

pub struct JournalPermitScope {}

impl Drop for JournalPermitScope {
    fn drop(&mut self) {
        JOURNAL_PERMIT_SCOPES.with(|scopes| {
            let _ = scopes.borrow_mut().pop();
        });
    }
}

struct JournalQueuePermit {
    pool: Arc<JournalPermitPool>,
}

impl Drop for JournalQueuePermit {
    fn drop(&mut self) {
        self.pool.release();
    }
}

struct JournalPermitPool {
    state: Mutex<JournalPermitState>,
}

struct JournalPermitState {
    capacity: usize,
    available: usize,
}

impl JournalPermitPool {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(JournalPermitState {
                capacity,
                available: capacity,
            }),
        }
    }

    fn try_acquire(self: &Arc<Self>) -> FsResult<Option<JournalQueuePermit>> {
        let mut state = self.state.lock().expect("journal permit pool poisoned");
        if state.capacity == 0 {
            return Ok(None);
        }

        if state.available == 0 {
            return Err(curvine_common::error::FsError::common(
                "journal writer queue is full",
            ));
        }
        state.available -= 1;
        Ok(Some(JournalQueuePermit { pool: self.clone() }))
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("journal permit pool poisoned");
        if state.capacity == 0 {
            return;
        }
        assert!(
            state.available < state.capacity,
            "journal permit release without matching acquire"
        );
        state.available += 1;
    }
}
