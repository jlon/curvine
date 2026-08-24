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

#![allow(clippy::needless_range_loop)]

use crate::master::fs::MasterFilesystem;
use crate::master::journal::*;
use crate::master::meta::inode::InodeView::File;
use crate::master::meta::inode::{InodePath, InodeView};
use crate::master::meta::store::{RocksStoreHandle, RocksStoreReadGuard};
use crate::master::meta::{CommitGate, CommitGateOwnedBarrier, InodeId};
use crate::master::{JobManager, Master, MasterMetrics, MountManager, SyncFsDir};
use curvine_config::JournalConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsError;
use curvine_model::RenameFlags;
use curvine_raft::conf::JournalConfExt;
use curvine_raft::proto::raft::{AppliedIndex, FsmState, SnapshotData};
use curvine_raft::raft::storage::{AppStorage, ApplyMsg, LogStorage, RocksLogStorage};
use curvine_raft::raft::{RaftClient, RaftError, RaftResult, RaftUtils};
use curvine_runtime::common::{FileUtils, LocalTime, TimeSpent};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel};
use log::{debug, error, info, warn};
use parking_lot::Mutex as ParkingMutex;
use raft::eraftpb::{Entry, EntryType};
use raft::StateRole;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use std::{fs, mem};

// Replay the master metadata operation log.
#[derive(Clone)]
pub struct JournalLoader {
    node_id: u64,
    fs_dir: SyncFsDir,
    store_handle: Arc<RocksStoreHandle>,
    mnt_mgr: Arc<MountManager>,
    journal_writer: Arc<JournalWriter>,
    ufs_loader: UfsLoader,
    log_store: RocksLogStorage,
    sender: AsyncSender<ApplyMsg>,
    fsm_state: Arc<Mutex<FsmState>>,
    retain_checkpoint_num: usize,
    ignore_reply_error: bool,
    max_retry_num: u64,
    skip_failed_ufs_replay_after_retry: bool,
    batch_size: u64,
    retry_interval: Duration,
    metrics: &'static MasterMetrics,
    has_apply_worker: bool,
    metadata_commit_gate: Arc<CommitGate>,
    follower_metadata_write_barrier: Arc<ParkingMutex<Option<CommitGateOwnedBarrier>>>,
    leader_mode: Arc<AtomicBool>,
    metadata_applied_index: Arc<AtomicU64>,
}

impl JournalLoader {
    pub fn new_replay_loader(
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
    ) -> CommonResult<Self> {
        let rt = conf.create_runtime();
        let client = RaftClient::from_conf(rt.clone(), conf);
        let journal_writer = Arc::new(JournalWriter::new(true, client, conf)?);
        let log_store = RocksLogStorage::from_conf(conf, false);
        Self::build(
            rt,
            fs_dir,
            mnt_mgr,
            conf,
            job_manager,
            log_store,
            journal_writer,
            Arc::new(CommitGate::new()),
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        rt: Arc<Runtime>,
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
        metadata_commit_gate: Arc<CommitGate>,
    ) -> CommonResult<Self> {
        Self::build(
            rt,
            fs_dir,
            mnt_mgr,
            conf,
            job_manager,
            log_store,
            journal_writer,
            metadata_commit_gate,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        rt: Arc<Runtime>,
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
        metadata_commit_gate: Arc<CommitGate>,
        testing: bool,
    ) -> CommonResult<Self> {
        let ufs_loader = UfsLoader::new(job_manager, conf);
        let (sender, receiver) = AsyncChannel::new(conf.writer_channel_size).split();
        let store_handle = fs_dir.read().store_handle.clone();
        let loader = Self {
            node_id: conf.node_id()?,
            fs_dir,
            store_handle,
            mnt_mgr,
            journal_writer,
            ufs_loader,
            log_store,
            sender,
            fsm_state: Arc::new(Mutex::new(FsmState::default())),
            retain_checkpoint_num: 3.max(conf.retain_checkpoint_num),
            ignore_reply_error: conf.ignore_reply_error,
            max_retry_num: conf.max_retry_num,
            skip_failed_ufs_replay_after_retry: conf.skip_failed_ufs_replay_after_retry,
            batch_size: conf.scan_batch_size,
            retry_interval: Duration::from_secs(conf.retry_interval_secs),
            metrics: Master::get_metrics()?,
            has_apply_worker: !testing,
            metadata_commit_gate,
            follower_metadata_write_barrier: Default::default(),
            leader_mode: Arc::new(AtomicBool::new(false)),
            metadata_applied_index: Arc::new(AtomicU64::new(0)),
        };

        if !testing {
            let loader1 = loader.clone();
            rt.spawn(async move {
                Self::run_apply(loader1, receiver).await;
            });
        }

        Ok(loader)
    }

    fn fsm_state(&self) -> CommonResult<MutexGuard<'_, FsmState>> {
        match self.fsm_state.lock() {
            Ok(state) => Ok(state),
            Err(e) => err_box!("fsm_state lock poisoned: {}", e),
        }
    }

    fn fsm_state_snapshot(&self) -> CommonResult<FsmState> {
        Ok(self.fsm_state()?.clone())
    }

    fn rocks_store(&self) -> CommonResult<RocksStoreReadGuard<'_>> {
        self.store_handle.read()
    }

    fn get_ufs_applied(&self) -> CommonResult<AppliedIndex> {
        Ok(self.fsm_state()?.ufs_applied.clone())
    }

    fn metadata_applied_index(&self) -> u64 {
        self.metadata_applied_index.load(Ordering::Acquire)
    }

    fn advance_metadata_applied_index(&self, index: u64) {
        let mut current = self.metadata_applied_index.load(Ordering::Acquire);
        while index > current {
            match self.metadata_applied_index.compare_exchange(
                current,
                index,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    fn close_metadata_writes(&self) {
        let mut barrier = self.follower_metadata_write_barrier.lock();
        if barrier.is_none() {
            *barrier = Some(self.metadata_commit_gate.close_and_wait_owned());
        }
    }

    fn reopen_metadata_writes_if_current(&self) {
        if self.leader_mode.load(Ordering::Acquire) && self.journal_writer.is_metadata_current() {
            self.follower_metadata_write_barrier.lock().take();
        }
    }

    fn advance_metadata_catch_up(&self, index: u64) {
        self.journal_writer.advance_metadata_catch_up(index);
        self.reopen_metadata_writes_if_current();
    }

    fn entry_needs_leader_metadata_apply(&self, entry: &Entry) -> bool {
        self.leader_mode.load(Ordering::Acquire)
            && entry.term < self.log_store.hard_state().term
            && entry.index > self.metadata_applied_index()
    }

    fn require_metadata_before_entry_apply(&self, entry: &Entry) {
        if entry.data.is_empty() || entry.get_entry_type() != EntryType::EntryNormal {
            return;
        }
        if self.entry_needs_leader_metadata_apply(entry) {
            self.journal_writer.require_metadata_catch_up(entry.index);
        }
    }

    fn require_metadata_before_apply(&self, msg: &ApplyMsg) {
        let ApplyMsg::Entry(entry) = msg else {
            return;
        };
        self.require_metadata_before_entry_apply(entry);
    }

    fn abort_on_fatal_apply_error(message: impl AsRef<str>) -> ! {
        error!(
            "fatal journal apply error: {}; aborting master to avoid serving inconsistent metadata",
            message.as_ref()
        );
        std::process::abort();
    }

    fn set_applied(
        &self,
        is_leader: bool,
        applied: AppliedIndex,
        has_ufs_affecting: bool,
    ) -> CommonResult<()> {
        if is_leader && has_ufs_affecting {
            self.journal_writer
                .log_ufs_applied(applied.op_id, applied.term, applied.index)?;
        }

        let applied_index = applied.index;
        let mut state = self.fsm_state()?;
        if is_leader {
            if applied.index > state.ufs_applied.index {
                state.ufs_applied = applied.clone();
            }
            if applied.index > state.applied.index {
                state.applied = applied;
            }
        } else if applied.index > state.applied.index {
            state.applied = applied;
        }

        self.metrics.journal_applied.set(state.applied.index as i64);
        self.metrics
            .journal_ufs_applied
            .set(state.ufs_applied.index as i64);
        drop(state);
        self.advance_metadata_applied_index(applied_index);
        if is_leader {
            self.advance_metadata_catch_up(applied_index);
        } else {
            self.journal_writer.advance_metadata_applied(applied_index);
        }

        let state = self.log_store.hard_state();
        self.metrics.journal_committed.set(state.commit as i64);
        self.metrics.journal_term.set(state.term as i64);

        Ok(())
    }

    fn build_applied(entry: &Entry) -> AppliedIndex {
        AppliedIndex {
            term: entry.term,
            index: entry.index,
            ..Default::default()
        }
    }

    async fn apply0(
        &self,
        is_leader: bool,
        entry: &Entry,
        skip_ufs_error: bool,
    ) -> CommonResult<()> {
        if entry.data.is_empty() || entry.get_entry_type() != EntryType::EntryNormal {
            self.advance_metadata_applied_index(entry.index);
            if is_leader {
                self.advance_metadata_catch_up(entry.index);
            } else {
                self.journal_writer.advance_metadata_applied(entry.index);
            }
            return Ok(());
        }

        let cur = self.fsm_state_snapshot()?;
        let apply_metadata = (!is_leader && entry.index > cur.applied.index)
            || self.entry_needs_leader_metadata_apply(entry);
        let apply_ufs = is_leader && entry.index > cur.ufs_applied.index;
        if !apply_metadata && !apply_ufs {
            info!(
                "skip entry index {}, term {}, fsm_state {:?}",
                entry.index, entry.term, cur
            );
            return Ok(());
        }

        let batch = JournalBatch::deserialize_compat(&entry.data)?;
        let batch_len = batch.len();
        let mut snapshot = None;
        let mut applied = Self::build_applied(entry);
        let mut has_ufs_affecting = false;

        for (seq, op_entry) in batch.batch.into_iter().enumerate() {
            applied.op_id = op_entry.op_id();
            applied.rpc_id = op_entry.rpc_id();

            match op_entry {
                JournalEntry::Snapshot(e) if is_leader && e.node_id == self.node_id => {
                    if seq + 1 != batch_len {
                        return err_box!("snapshot should be the last entry");
                    }
                    snapshot.replace(e);
                    continue;
                }

                JournalEntry::UfsApplied(_) => (),

                _ => has_ufs_affecting = true,
            }

            {
                let fs_dir = self.fs_dir.read();
                fs_dir.update_op_id(op_entry.op_id());
                if let Some(inode_id) = op_entry.inode_id() {
                    fs_dir.update_last_inode_id(inode_id)?;
                }
            }

            if apply_metadata {
                self.apply_entry(op_entry.clone())?;
            }

            let res = if apply_ufs {
                self.ufs_loader.apply_entry(&op_entry).await
            } else {
                Ok(())
            };

            if let Err(e) = res {
                if is_leader && skip_ufs_error {
                    error!(
                        "skip failed UFS replay after retries, entry index={}, term={}, journal={:?}, error={}",
                        entry.index, entry.term, op_entry, e
                    );
                    continue;
                }

                return err_box!("failed to apply journal: {:?}: {}", op_entry, e);
            }
        }

        if let Some(e) = snapshot {
            SnapshotManifest::validate_checkpoint_if_present(&e.dir, e.op_id, e.node_id)?;
            let snapshot_state = self.next_fsm_state(is_leader, applied.clone())?;
            let snap_data =
                self.create_snapshot_with_state(Some(e.dir.to_string()), snapshot_state)?;

            self.log_store.create_snapshot(snap_data.clone())?;
            self.log_store.compact(snap_data.fsm_state.compact())?;

            info!(
                "create leader snapshot, dir={}, fsm_state={:?}",
                e.dir, snap_data.fsm_state
            );
        }

        self.set_applied(is_leader, applied, has_ufs_affecting && apply_ufs)?;

        Ok(())
    }

    async fn apply_msg(
        &self,
        is_leader: bool,
        msg: &ApplyMsg,
        skip_ufs_error: bool,
    ) -> CommonResult<()> {
        match msg {
            ApplyMsg::Entry(entry) => {
                self.apply0(is_leader, entry, skip_ufs_error).await?;
                Ok(())
            }

            ApplyMsg::Scan(applied_index) => {
                let mut last_applied = applied_index.index;
                if is_leader && skip_ufs_error {
                    last_applied = last_applied.max(self.fsm_state_snapshot()?.ufs_applied.index);
                }

                let commit_index = self.log_store.hard_state().commit;
                loop {
                    if last_applied >= commit_index {
                        if is_leader {
                            self.advance_metadata_catch_up(commit_index);
                        } else {
                            self.journal_writer.advance_metadata_applied(commit_index);
                        }
                        return Ok(());
                    }

                    let high = (last_applied + self.batch_size).min(commit_index + 1);
                    let list = self.log_store.scan_entries(last_applied + 1, high)?;

                    if list.is_empty() {
                        if is_leader {
                            self.advance_metadata_catch_up(commit_index);
                        } else {
                            self.journal_writer.advance_metadata_applied(commit_index);
                        }
                        return Ok(());
                    };

                    info!(
                        "replay-scan, start_index: {}, entries: {}, commit_index: {}",
                        last_applied + 1,
                        list.len(),
                        commit_index
                    );

                    for entry in list {
                        self.apply0(is_leader, &entry, skip_ufs_error).await?;
                        last_applied = entry.index;
                        if skip_ufs_error {
                            if is_leader {
                                self.advance_metadata_catch_up(last_applied);
                            } else {
                                self.journal_writer.advance_metadata_applied(last_applied);
                            }
                            return Ok(());
                        }
                    }
                }
            }

            _ => err_box!("unsupported apply message in journal loader apply_msg"),
        }
    }

    async fn next_apply_msg(
        &self,
        receiver: &mut AsyncReceiver<ApplyMsg>,
        retry_msg: &mut Option<(ApplyMsg, bool)>,
    ) -> Option<ApplyMsg> {
        match retry_msg.take() {
            Some((msg, delay)) => {
                if delay {
                    tokio::time::sleep(self.retry_interval).await;
                }
                Some(msg)
            }
            None => receiver.recv().await,
        }
    }

    async fn run_apply(self, mut receiver: AsyncReceiver<ApplyMsg>) {
        let mut retry_msg: Option<(ApplyMsg, bool)> = None;
        let mut retry_num: u64 = 0;
        let mut is_leader = false;

        loop {
            let apply_msg = match self.next_apply_msg(&mut receiver, &mut retry_msg).await {
                Some(v) => v,
                None => break,
            };

            match apply_msg {
                ApplyMsg::CreateSnapshot(tx) => {
                    if let Err(e) = tx.send(self.create_snapshot0(None)) {
                        warn!("send create snapshot result failed: {}", e);
                    }
                    retry_num = 0;
                }

                ApplyMsg::ApplySnapshot((tx, snapshot)) => {
                    if let Err(e) = tx.send(self.apply_snapshot0(snapshot)) {
                        warn!("send apply snapshot result failed: {}", e);
                    }
                    retry_num = 0;
                }

                ApplyMsg::RoleChange(role) => {
                    is_leader = role == StateRole::Leader;
                    self.leader_mode.store(is_leader, Ordering::Release);
                    if is_leader {
                        let mut scan_applied = match self.get_ufs_applied() {
                            Ok(ufs_applied) => ufs_applied,
                            Err(e) => {
                                Self::abort_on_fatal_apply_error(format!(
                                    "failed to read fsm_state after leader role change: {}",
                                    e
                                ));
                            }
                        };
                        let metadata_applied = self.metadata_applied_index();
                        if metadata_applied < scan_applied.index {
                            scan_applied.index = metadata_applied;
                        }
                        info!(
                            "role changed to leader, scheduling replay scan from {:?}",
                            scan_applied
                        );
                        retry_msg.replace((ApplyMsg::new_scan(scan_applied), false));
                    }
                }

                ApplyMsg::Shutdown(tx) => {
                    let _ = tx.send(());
                    break;
                }

                msg => match self.apply_msg(is_leader, &msg, false).await {
                    Ok(_) => retry_num = 0,

                    Err(error) => {
                        if self.ignore_reply_error {
                            error!("apply entry failed(skip): {}", error);
                        } else if is_leader {
                            retry_num += 1;

                            if retry_num >= self.max_retry_num {
                                if self.skip_failed_ufs_replay_after_retry {
                                    error!(
                                        "apply entry failed(retry_num={}), skipping failed UFS replay to keep master alive: {}",
                                        retry_num, error
                                    );
                                    let continue_scan = matches!(msg, ApplyMsg::Scan(_));
                                    if let Err(skip_error) =
                                        self.apply_msg(is_leader, &msg, true).await
                                    {
                                        Self::abort_on_fatal_apply_error(format!(
                                            "apply entry failed while skipping failed UFS replay: {}",
                                            skip_error
                                        ));
                                    }
                                    retry_num = 0;
                                    if continue_scan {
                                        retry_msg.replace((msg, true));
                                    }
                                } else {
                                    Self::abort_on_fatal_apply_error(format!(
                                        "apply entry failed(retry_num={}): {}",
                                        retry_num, error
                                    ));
                                }
                            } else {
                                error!("apply entry failed(retry_num={}): {}", retry_num, error);
                                retry_msg.replace((msg, true));
                            }
                        } else {
                            Self::abort_on_fatal_apply_error(format!(
                                "apply entry failed on follower: {}",
                                error
                            ));
                        }
                    }
                },
            }
        }
    }

    fn create_snapshot0(&self, dir_option: Option<String>) -> RaftResult<SnapshotData> {
        if dir_option.is_none() {
            let _barrier = self.metadata_commit_gate.close_and_wait();
            let fsm_state = self.fsm_state_snapshot()?;
            return self.create_snapshot_with_state(None, fsm_state);
        }

        let fsm_state = self.fsm_state_snapshot()?;
        self.create_snapshot_with_state(dir_option, fsm_state)
    }

    fn create_snapshot_with_state(
        &self,
        dir_option: Option<String>,
        fsm_state: FsmState,
    ) -> RaftResult<SnapshotData> {
        let dir = match dir_option {
            Some(dir) => dir,
            None => {
                let fs_dir = self.fs_dir.read();
                let dir = fs_dir.create_checkpoint(fsm_state.applied.index)?;
                SnapshotManifest::write_checkpoint(&dir, fsm_state.op_id(), self.node_id)?;
                dir
            }
        };

        let data = RaftUtils::create_file_snapshot(&dir, self.node_id, fsm_state)?;

        if let Err(e) = self.purge_checkpoint(&dir) {
            warn!("purge checkpoint: {}", e);
        }

        Ok(data)
    }

    fn next_fsm_state(&self, is_leader: bool, applied: AppliedIndex) -> CommonResult<FsmState> {
        let mut state = self.fsm_state_snapshot()?;
        if is_leader {
            state.ufs_applied = applied.clone();
        }
        state.applied = applied;
        Ok(state)
    }

    fn apply_snapshot0(&self, snapshot: SnapshotData) -> RaftResult<()> {
        let mut spend = TimeSpent::new();

        // Resolve restore path first. Always measure dir size for safety checks;
        // the logged checkpoint_size may still be 0 when info logging is disabled.
        let restore_path = match &snapshot.files_data {
            Some(data) => {
                SnapshotManifest::validate_checkpoint_if_present(
                    &data.dir,
                    snapshot.fsm_state.op_id(),
                    snapshot.node_id,
                )?;
                data.dir.clone()
            }
            None => {
                let dir = self.fs_dir.read().get_checkpoint_path(LocalTime::mills());
                FileUtils::create_dir(&dir, true)?;
                dir
            }
        };
        let actual_size = FileUtils::dir_size(&restore_path).unwrap_or_else(|e| {
            warn!(
                "failed to compute checkpoint size for {}: {}",
                restore_path, e
            );
            0
        });
        let checkpoint_size = if log::log_enabled!(log::Level::Info) {
            actual_size
        } else {
            0
        };

        // Never wipe a populated filesystem with an empty checkpoint. Harness
        // daily failures showed FileNotFound after restore from checkpoint_size=0
        // following raft quorum loss (CurvineIO/curvine#1207).
        // get_file_counts() returns (dir_count, file_count).
        {
            let fs_dir = self.fs_dir.read();
            let (dir_count, file_count) = fs_dir.get_file_counts();
            if actual_size == 0 && file_count > 0 {
                return err_box!(
                    "refusing to apply empty snapshot at {} ({} bytes) over filesystem with {} files and {} dirs",
                    restore_path,
                    actual_size,
                    file_count,
                    dir_count
                );
            }
        }

        let mut fs_dir = self.fs_dir.write();
        fs_dir.restore(&restore_path, checkpoint_size)?;
        fs_dir.update_op_id(snapshot.fsm_state.op_id());
        drop(fs_dir);
        let restore_ms = spend.used_ms();
        spend.reset();

        self.mnt_mgr
            .restore()
            .map_err(|error| RaftError::other(error.into()))?;
        let mount_ms = spend.used_ms();

        let applied_index = snapshot.fsm_state.applied.index;
        *self.fsm_state()? = snapshot.fsm_state;
        self.advance_metadata_applied_index(applied_index);
        self.journal_writer.advance_metadata_applied(applied_index);

        info!(
            "apply_snapshot: fs_dir_restore={} ms, mount_restore={} ms, total={} ms",
            restore_ms,
            mount_ms,
            restore_ms + mount_ms
        );

        Ok(())
    }

    pub fn apply_entry(&self, entry: JournalEntry) -> CommonResult<()> {
        debug!("replay entry: {:?}", entry);

        match entry {
            JournalEntry::Mkdir(e) => self.mkdir(e),

            JournalEntry::CreateFile(e) => self.create_file(e),

            JournalEntry::OverWriteFile(e) => self.overwrite_file(e),

            JournalEntry::AddBlock(e) => self.add_block(e),

            JournalEntry::CompleteFile(e) => self.complete_file(e),

            JournalEntry::Rename(e) => self.rename(e),

            JournalEntry::Delete(e) => self.delete(e),

            JournalEntry::Free(e) => self.free(e),

            JournalEntry::ReopenFile(e) => self.reopen_file(e),

            JournalEntry::Mount(e) => self.mount(e),

            JournalEntry::UnMount(e) => self.unmount(e),

            JournalEntry::SetAttr(e) => self.set_attr(e),

            JournalEntry::Symlink(e) => self.symlink(e),

            JournalEntry::Link(e) => self.link(e),

            JournalEntry::SetLocks(e) => self.set_locks(e),

            JournalEntry::UfsApplied(e) => self.ufs_applied(e),

            _ => Ok(()),
        }
    }

    fn mkdir(&self, entry: MkdirEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.read();
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, InodeView::new_dir(name, entry.dir))?;
        Ok(())
    }

    fn create_file(&self, entry: CreateFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.read();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        if inp.is_full() {
            warn!("create_file: file already exists: {:?}", entry);
            return Ok(());
        }
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, InodeView::new_file(name, entry.file))?;
        Ok(())
    }

    fn reopen_file(&self, entry: ReopenFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.read();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("reopen_file: file not found: {:?}", entry);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(file, entry.file);

        fs_dir.store.apply_reopen_file(inode.as_ref())?;
        fs_dir.invalidate_file_status_for_inode(inode.as_ref());

        Ok(())
    }

    fn overwrite_file(&self, entry: OverWriteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.read();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("overwrite_file: file not found: {:?}", entry);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(file, entry.file);

        fs_dir.store.apply_overwrite_file(inode.as_ref())?;
        fs_dir.invalidate_file_status_for_inode(inode.as_ref());

        Ok(())
    }

    fn add_block(&self, entry: AddBlockEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.read();

        let inode_id = entry.blocks.first().map(|v| InodeId::get_id(v.id));

        let mut inode = match MasterFilesystem::resolve_file_inode(&fs_dir, &entry.path, inode_id) {
            Ok(v) => v,
            Err(e) => {
                warn!("add_block: file not found: {:?} {}", entry, e);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(&mut file.blocks, entry.blocks);
        fs_dir
            .store
            .apply_new_block(inode.as_ref(), &entry.commit_block)?;
        fs_dir.invalidate_file_status_for_inode(inode.as_ref());

        Ok(())
    }

    fn complete_file(&self, entry: CompleteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.read();

        let mut inode =
            match MasterFilesystem::resolve_file_inode(&fs_dir, &entry.path, Some(entry.file.id)) {
                Ok(v) => v,
                Err(e) => {
                    warn!("complete_file: file not found: {:?} {}", entry, e);
                    return Ok(());
                }
            };
        let file = inode.as_file_mut()?;

        let _ = mem::replace(file, entry.file);
        // Update block location
        fs_dir
            .store
            .apply_complete_file(inode.as_ref(), &entry.commit_blocks)?;
        fs_dir.invalidate_file_status_for_inode(inode.as_ref());

        Ok(())
    }
    pub fn rename(&self, entry: RenameEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.read();
        let entry_src = entry.src;
        let src_inp = InodePath::resolve(fs_dir.root_ptr(), &entry_src, &fs_dir.store)?;
        let dst_inp = InodePath::resolve(fs_dir.root_ptr(), entry.dst, &fs_dir.store)?;
        if src_inp.get_last_inode().is_none() {
            warn!("Rename: source path not found: {}", entry_src);
            return Ok(());
        }
        let flags = RenameFlags::try_new(entry.flags).ok_or_else(|| {
            FsError::invalid_argument(format!("invalid rename flags: {:#x}", entry.flags))
        })?;
        if !flags.is_supported() {
            return Err(FsError::unsupported(format!(
                "unsupported rename flags: {:#x}",
                entry.flags
            ))
            .into());
        }

        let exchange_pre_swap_ids =
            if flags.exchange_mode() && entry.src_inode_id != 0 && entry.dst_inode_id != 0 {
                Some((entry.src_inode_id, entry.dst_inode_id))
            } else {
                None
            };
        fs_dir.unprotected_rename(
            &src_inp,
            &dst_inp,
            entry.mtime,
            flags,
            exchange_pre_swap_ids,
        )?;

        Ok(())
    }

    pub fn delete(&self, entry: DeleteEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.read();
        let entry_path = entry.path;
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry_path, &fs_dir.store)?;
        if inp.get_last_inode().is_none() {
            warn!("Delete: path not found: {}", entry_path);
            return Ok(());
        }
        fs_dir.unprotected_delete(&inp, entry.mtime)?;
        Ok(())
    }

    pub fn free(&self, entry: FreeEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.read();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;
        let Some(inode) = inp.get_last_inode() else {
            warn!("Free: path not found: {:?}", entry);
            return Ok(());
        };
        fs_dir.unprotected_free(inode, entry.mtime, entry.recursive)?;
        Ok(())
    }

    pub fn mount(&self, entry: MountEntry) -> CommonResult<()> {
        self.rocks_store()?
            .add_mountpoint(entry.info.mount_id, &entry.info)?;
        self.mnt_mgr.unprotected_add_mount(entry.info.clone())?;
        Ok(())
    }

    pub fn unmount(&self, entry: UnMountEntry) -> CommonResult<()> {
        self.rocks_store()?.remove_mountpoint(entry.id)?;
        if !self.mnt_mgr.unprotected_umount_if_mounted(entry.id)? {
            warn!("Unmount: id already unmounted: {:?}", entry);
            return Ok(());
        }
        Ok(())
    }

    pub fn set_attr(&self, entry: SetAttrEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.read();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;
        let last_inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("SetAttr: path not found: {:?}", entry);
                return Ok(());
            }
        };

        let target_is_root = inp.len() <= 1;
        let parent_view = if target_is_root {
            None
        } else {
            inp.get_inode(-2)
        };
        fs_dir.unprotected_set_attr(parent_view.as_deref(), last_inode, entry.opts)?;
        Ok(())
    }

    pub fn symlink(&self, entry: SymlinkEntry) -> CommonResult<()> {
        let link_path = entry.link;
        let fs_dir = self.fs_dir.read();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &link_path, &fs_dir.store)?;
        match fs_dir.unprotected_symlink(inp, entry.new_inode, entry.force) {
            Ok(_) => Ok(()),
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Symlink: file already exists: {:?}", link_path);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn link(&self, entry: LinkEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.read();
        let old_path = InodePath::resolve(fs_dir.root_ptr(), &entry.src_path, &fs_dir.store)?;
        let new_path = InodePath::resolve(fs_dir.root_ptr(), &entry.dst_path, &fs_dir.store)?;

        // Get the original inode ID
        let original_inode_id = match old_path.get_last_inode() {
            Some(inode) => inode.id(),
            None => {
                warn!("Link: source path not found: {:?}", entry);
                return Ok(());
            }
        };

        match fs_dir.unprotected_link(new_path, original_inode_id, entry.mtime as u64) {
            Ok(_) => {
                if let Some(mut inode_ptr) = old_path.get_last_inode() {
                    if let File(_) = inode_ptr.as_mut() {
                        inode_ptr.incr_nlink(entry.mtime)?;
                    }
                }
                Ok(())
            }
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Link: dst_path already exists: {:?}", entry);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_locks(&self, entry: SetLocksEntry) -> CommonResult<()> {
        self.rocks_store()?.set_locks(entry.ino, &entry.locks)
    }

    pub fn ufs_applied(&self, entry: UfsAppliedEntry) -> CommonResult<()> {
        let mut lock = self.fsm_state()?;
        lock.ufs_applied = AppliedIndex {
            op_id: entry.op_id,
            rpc_id: entry.rpc_id,
            term: entry.term,
            index: entry.index,
        };
        Ok(())
    }

    // Clean up expired checkpoints.
    pub fn purge_checkpoint(&self, current_ck: impl AsRef<str>) -> CommonResult<()> {
        let current_ck = current_ck.as_ref();
        let ck_dir = match Path::new(current_ck).parent() {
            None => return Ok(()),
            Some(v) => v,
        };

        let current_mtime = match Path::new(current_ck).metadata() {
            Ok(meta) => FileUtils::mtime(&meta)?,
            Err(_) => return Ok(()),
        };

        let mut vec = vec![];
        for entry in fs::read_dir(ck_dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            let mtime = FileUtils::mtime(&meta)?;
            if mtime < current_mtime {
                vec.push((mtime, entry.path()));
            }
        }

        // Sort oldest-first and keep at most (retain_checkpoint_num - 1) older
        // checkpoints so that together with current_ck the total is retain_checkpoint_num.
        vec.sort_by_key(|x| x.0);
        let keep = self.retain_checkpoint_num.saturating_sub(1);
        let del_num = vec.len().saturating_sub(keep);

        for (_, path) in vec.iter().take(del_num) {
            let path = path.as_path();
            FileUtils::delete_path(path, true)?;
            info!("delete expired checkpoint: {}", path.to_string_lossy());
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> RaftResult<()> {
        let (tx, rx) = CallChannel::channel();
        self.sender.send(ApplyMsg::Shutdown(tx)).await?;
        rx.receive().await?;
        Ok(())
    }
}

impl AppStorage for JournalLoader {
    fn before_apply_entries(&self, entries: &[Entry]) -> RaftResult<()> {
        for entry in entries {
            self.require_metadata_before_entry_apply(entry);
        }
        Ok(())
    }

    async fn apply(&self, wait: bool, msg: ApplyMsg) -> RaftResult<()> {
        if wait || !self.has_apply_worker {
            let is_leader = self.leader_mode.load(Ordering::Acquire);
            self.require_metadata_before_apply(&msg);
            if let Err(e) = self.apply_msg(is_leader, &msg, false).await {
                if self.ignore_reply_error {
                    error!("apply entry failed: {}", e);
                    Ok(())
                } else {
                    Err(e.into())
                }
            } else {
                Ok(())
            }
        } else {
            self.require_metadata_before_apply(&msg);
            self.sender.send(msg).await?;
            Ok(())
        }
    }

    fn get_fsm_state(&self) -> FsmState {
        match self.fsm_state.lock() {
            Ok(state) => state.clone(),
            Err(e) => {
                error!("fatal fsm_state lock poisoned: {}", e);
                std::process::abort();
            }
        }
    }

    async fn role_change(&self, role: StateRole) -> RaftResult<()> {
        if role == StateRole::Leader {
            self.leader_mode.store(true, Ordering::Release);
            let applied_index = self.metadata_applied_index();
            let required = self.log_store.hard_state().commit;
            self.journal_writer
                .begin_metadata_catch_up(applied_index, required);
            self.reopen_metadata_writes_if_current();
        } else {
            // Keep this barrier alive through follower recovery. A request can
            // have passed its pre-entry read barrier immediately before
            // demotion, so merely returning NotLeader from its response is not
            // enough to prevent a stale local metadata mutation.
            self.close_metadata_writes();
            self.leader_mode.store(false, Ordering::Release);
        }
        if !self.has_apply_worker {
            return Ok(());
        }
        self.sender.send(ApplyMsg::RoleChange(role)).await?;
        Ok(())
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        if !self.has_apply_worker {
            return self.create_snapshot0(None);
        }
        let (tx, rx) = CallChannel::channel();
        let msg = ApplyMsg::CreateSnapshot(tx);

        self.sender.send(msg).await?;
        rx.receive().await?
    }

    async fn apply_snapshot(&self, snapshot: SnapshotData) -> RaftResult<()> {
        if !self.has_apply_worker {
            return self.apply_snapshot0(snapshot);
        }
        let (tx, rx) = CallChannel::channel();
        let msg = ApplyMsg::ApplySnapshot((tx, snapshot));

        self.sender.send(msg).await?;
        rx.receive().await?
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        let fs_dir = self.fs_dir.read();
        Ok(fs_dir.get_checkpoint_path(snapshot_id))
    }
}
