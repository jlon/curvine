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
use curvine_config::JournalConf;
use curvine_error::{FsError, FsResult};
use curvine_model::{CommitBlock, FileLock, MountInfo, RenameFlags, SetAttrOpts};
use curvine_raft::conf::JournalConfExt;
use curvine_raft::raft::RaftClient;
use curvine_runtime::sync::channel::{BlockingChannel, BlockingReceiver, BlockingSender};
use curvine_runtime::sync::AtomicCounter;
use log::{debug, error};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};

thread_local! {
    static JOURNAL_PERMIT_SCOPES: RefCell<Vec<Vec<JournalPermit>>> = const { RefCell::new(Vec::new()) };
    static JOURNAL_COMMIT_SCOPES: RefCell<Vec<Vec<mpsc::Receiver<()>>>> = const { RefCell::new(Vec::new()) };
}

// Write metadata operation logs.
pub struct JournalWriter {
    enable: bool,
    require_raft_commit: bool,
    node_id: u64,
    sender: BlockingSender<QueuedJournalEntry>,
    permit_pool: Arc<JournalPermitPool>,
    read_barrier: Arc<JournalReadBarrier>,
    metrics: &'static MasterMetrics,
    receiver: Option<Mutex<BlockingReceiver<QueuedJournalEntry>>>,
    metadata_delta_log: MetadataDeltaLog,

    snapshot_entries: u64,
    entries_since_snapshot: AtomicCounter,
    snapshot_requested: AtomicBool,
    snapshot_in_progress: AtomicBool,
}

/// Sharded bounded ring of recent CV metadata changes served to delta
/// pagination readers.
///
/// Every namespace mutation used to serialize on one global `Mutex` just to
/// append its change record. Shards are keyed by op id so writers never share
/// a lock line; pagination reads are rare and take each shard lock once in
/// index order. The effective low watermark is the minimum across shards, so
/// rejection of over-range requests stays conservative.
struct MetadataDeltaLog {
    shards: Vec<Mutex<MetadataDeltaShard>>,
}

/// Shard sizing grows the shard count with the configured capacity until each
/// shard owns roughly this many live entries, capped so small capacities stay
/// on a single lock.
const METADATA_DELTA_LOG_SHARD_GRAIN: usize = 1024;
const METADATA_DELTA_LOG_SHARD_LIMIT: usize = 16;

struct MetadataDeltaShard {
    capacity: usize,
    low_watermark: u64,
    changes: VecDeque<CvMetadataChange>,
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
            // Test mode intentionally keeps the receiver for direct journal assertions
            // and does not start SenderTask, so no Raft commit receipt can arrive.
            require_raft_commit: !testing,
            node_id,
            sender,
            permit_pool,
            read_barrier: Arc::new(JournalReadBarrier::new()),
            metrics,
            receiver,
            metadata_delta_log: MetadataDeltaLog::new(conf.metadata_delta_log_capacity),
            snapshot_entries: conf.snapshot_entries,
            entries_since_snapshot: AtomicCounter::new(0),
            snapshot_requested: AtomicBool::new(false),
            snapshot_in_progress: AtomicBool::new(false),
        })
    }

    fn send_inner(
        &self,
        entry: JournalEntry,
        committed: Option<mpsc::SyncSender<()>>,
    ) -> FsResult<()> {
        let permit = match Self::take_scoped_permit() {
            Some(permit) => permit,
            None => self.reserve()?,
        };
        self.enqueue_with_completion(permit, entry, committed)
    }

    fn enqueue_after_commit(&self, entry: JournalEntry) -> FsResult<()> {
        if !self.require_raft_commit {
            return self.send_inner(entry, None);
        }

        let (tx, rx) = mpsc::sync_channel(1);
        if let Err(error) = self.send_inner(entry, Some(tx)) {
            error!(
                "journal enqueue failed after metadata commit; aborting master: {}",
                error
            );
            std::process::abort();
        }
        if let Err(waiter) = Self::record_commit_waiter(rx) {
            JournalCommitScope::wait_for_commit(waiter)?;
        }
        Ok(())
    }

    fn enqueue_background_after_commit(&self, entry: JournalEntry) -> FsResult<()> {
        if let Err(error) = self.send_inner(entry, None) {
            error!(
                "background journal enqueue failed after metadata commit; aborting master: {}",
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

    pub fn is_metadata_current(&self) -> bool {
        self.read_barrier.is_current()
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn reserve_scope(&self, count: usize) -> FsResult<JournalPermitScope> {
        if !self.enable {
            return Ok(JournalPermitScope { active: false });
        }

        let mut permits = Vec::with_capacity(count);
        for _ in 0..count {
            permits.push(self.reserve()?);
        }

        JOURNAL_PERMIT_SCOPES.with(|scopes| scopes.borrow_mut().push(permits));
        Ok(JournalPermitScope { active: true })
    }

    pub fn begin_commit_scope(&self) -> JournalCommitScope {
        if !self.enable {
            return JournalCommitScope {
                active: false,
                completed: true,
            };
        }

        JOURNAL_COMMIT_SCOPES.with(|scopes| scopes.borrow_mut().push(vec![]));
        JournalCommitScope {
            active: true,
            completed: false,
        }
    }

    fn take_scoped_permit() -> Option<JournalPermit> {
        JOURNAL_PERMIT_SCOPES.with(|scopes| {
            let mut scopes = scopes.borrow_mut();
            scopes.last_mut().and_then(|permits| permits.pop())
        })
    }

    fn record_commit_waiter(waiter: mpsc::Receiver<()>) -> Result<(), mpsc::Receiver<()>> {
        JOURNAL_COMMIT_SCOPES.with(|scopes| {
            let mut scopes = scopes.borrow_mut();
            let Some(waiters) = scopes.last_mut() else {
                return Err(waiter);
            };
            waiters.push(waiter);
            Ok(())
        })
    }

    pub fn enqueue_with_permit(&self, permit: JournalPermit, entry: JournalEntry) -> FsResult<()> {
        self.enqueue_with_completion(permit, entry, None)
    }

    fn enqueue_with_completion(
        &self,
        permit: JournalPermit,
        entry: JournalEntry,
        committed: Option<mpsc::SyncSender<()>>,
    ) -> FsResult<()> {
        debug!("send_entry {:?}", entry);
        self.sender.send(QueuedJournalEntry {
            entry,
            _permit: permit,
            committed,
        })?;
        self.metrics.journal_queue_len.inc();
        Ok(())
    }

    fn send(&self, _fs_dir: &FsDir, entry: JournalEntry) -> FsResult<()> {
        if self.enable {
            self.record_metadata_delta(&entry);
            self.enqueue_after_commit(entry)?;
            self.request_snapshot_if_needed();
        }
        Ok(())
    }

    fn record_metadata_delta(&self, entry: &JournalEntry) {
        let changes = entry.cv_metadata_changes();
        if changes.is_empty() {
            return;
        }
        self.metadata_delta_log.push(&changes);
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

    pub fn log_cache_invalidations(
        &self,
        fs_dir: &FsDir,
        inodes: Vec<crate::master::meta::inode::InodeView>,
    ) -> FsResult<()> {
        if inodes.is_empty() {
            return Ok(());
        }

        let entry = CacheInvalidationEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            inodes,
        };
        self.send(fs_dir, JournalEntry::CacheInvalidation(entry))
    }

    pub fn log_rename<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        src: P,
        dst: P,
        mtime: i64,
        flags: RenameFlags,
        exchange_pre_swap_ids: Option<(i64, i64)>,
    ) -> FsResult<()> {
        let (src_inode_id, dst_inode_id) = exchange_pre_swap_ids.unwrap_or((0, 0));
        let entry = RenameEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            src: src.as_ref().to_string(),
            dst: dst.as_ref().to_string(),
            mtime,
            flags: flags.value(),
            src_inode_id,
            dst_inode_id,
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
            let entry = JournalEntry::Mount(entry);
            self.record_metadata_delta(&entry);
            self.enqueue_after_commit(entry)?;
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
            let entry = JournalEntry::UnMount(entry);
            self.record_metadata_delta(&entry);
            self.enqueue_after_commit(entry)?;
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
        mtime: i64,
    ) -> FsResult<()> {
        let entry = LinkEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            mtime,
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
        self.enqueue_background_after_commit(JournalEntry::UfsApplied(entry))?;

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
            let entry = JournalEntry::SetLocks(entry);
            self.record_metadata_delta(&entry);
            self.enqueue_after_commit(entry)?;
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

    pub(crate) fn cv_metadata_changes_since(
        &self,
        from_epoch: u64,
        to_epoch: u64,
    ) -> Option<Vec<CvMetadataChange>> {
        self.metadata_delta_log.changes_since(from_epoch, to_epoch)
    }
}

impl MetadataDeltaLog {
    fn new(capacity: usize) -> Self {
        let shard_count = if capacity == 0 {
            1
        } else {
            (capacity / METADATA_DELTA_LOG_SHARD_GRAIN).clamp(1, METADATA_DELTA_LOG_SHARD_LIMIT)
        };
        let shard_capacity = if capacity == 0 {
            0
        } else {
            capacity.div_ceil(shard_count)
        };
        Self {
            shards: (0..shard_count)
                .map(|_| {
                    Mutex::new(MetadataDeltaShard {
                        capacity: shard_capacity,
                        low_watermark: 0,
                        changes: VecDeque::with_capacity(shard_capacity.min(1024)),
                    })
                })
                .collect(),
        }
    }

    fn push(&self, changes: &[CvMetadataChange]) {
        // Entries carry a single op id; routing on it spreads sequential
        // mutations round-robin across shards without any shared state.
        let route_op_id = changes.iter().map(|change| change.op_id).max();
        let Some(route_op_id) = route_op_id else {
            return;
        };
        let shard_index = (route_op_id as usize) % self.shards.len();
        let mut shard = self.shards[shard_index].lock().unwrap();
        shard.push(changes);
    }

    fn changes_since(&self, from_epoch: u64, to_epoch: u64) -> Option<Vec<CvMetadataChange>> {
        let mut low_watermark = u64::MAX;
        let mut matched = Vec::new();
        for shard in &self.shards {
            let shard = shard.lock().unwrap();
            low_watermark = low_watermark.min(shard.low_watermark);
            matched.extend(
                shard
                    .changes
                    .iter()
                    .filter(|change| change.op_id > from_epoch && change.op_id <= to_epoch)
                    .cloned(),
            );
        }
        if from_epoch < low_watermark {
            return None;
        }
        matched.sort_unstable_by_key(|change| change.op_id);
        Some(matched)
    }
}

impl MetadataDeltaShard {
    fn push(&mut self, changes: &[CvMetadataChange]) {
        if self.capacity == 0 {
            if let Some(max_op_id) = changes.iter().map(|change| change.op_id).max() {
                self.low_watermark = self.low_watermark.max(max_op_id);
            }
            return;
        }
        for change in changes {
            self.changes.push_back(change.clone());
            while self.changes.len() > self.capacity {
                if let Some(evicted) = self.changes.pop_front() {
                    self.low_watermark = self.low_watermark.max(evicted.op_id);
                }
            }
        }
    }
}

pub struct QueuedJournalEntry {
    pub(crate) entry: JournalEntry,
    _permit: JournalPermit,
    pub(crate) committed: Option<mpsc::SyncSender<()>>,
}

pub struct JournalPermit {
    _permit: Option<JournalQueuePermit>,
}

pub struct JournalPermitScope {
    active: bool,
}

impl Drop for JournalPermitScope {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        JOURNAL_PERMIT_SCOPES.with(|scopes| {
            let _ = scopes.borrow_mut().pop();
        });
    }
}

pub struct JournalCommitScope {
    active: bool,
    completed: bool,
}

impl JournalCommitScope {
    pub fn wait(mut self) -> FsResult<()> {
        if !self.active {
            return Ok(());
        }
        let waiters =
            JOURNAL_COMMIT_SCOPES.with(|scopes| scopes.borrow_mut().pop().unwrap_or_default());
        self.completed = true;
        for waiter in waiters {
            Self::wait_for_commit(waiter)?;
        }
        Ok(())
    }

    fn wait_for_commit(waiter: mpsc::Receiver<()>) -> FsResult<()> {
        if let Err(error) = waiter.recv() {
            error!(
                "journal commit failed after metadata commit; aborting master: {}",
                error
            );
            std::process::abort();
        }
        Ok(())
    }
}

impl Drop for JournalCommitScope {
    fn drop(&mut self) {
        if self.active && !self.completed {
            JOURNAL_COMMIT_SCOPES.with(|scopes| {
                let _ = scopes.borrow_mut().pop();
            });
        }
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
    capacity: usize,
    available: AtomicUsize,
}

impl JournalPermitPool {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            available: AtomicUsize::new(capacity),
        }
    }

    fn try_acquire(self: &Arc<Self>) -> FsResult<Option<JournalQueuePermit>> {
        if self.capacity == 0 {
            return Ok(None);
        }

        let mut current = self.available.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                return Err(FsError::common("journal writer queue is full"));
            }
            match self.available.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(Some(JournalQueuePermit { pool: self.clone() })),
                Err(now) => current = now,
            }
        }
    }

    fn release(&self) {
        if self.capacity == 0 {
            return;
        }
        let previous = self.available.fetch_add(1, Ordering::AcqRel);
        assert!(
            previous < self.capacity,
            "journal permit release without matching acquire"
        );
    }
}
