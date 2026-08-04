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

use super::{
    JournalCommand, JournalEntry, JournalEnvelope, JournalWriter, MetadataCommand, UfsLoader,
};
use curvine_common::conf::JournalConf;
use curvine_common::error::FsError;
use curvine_common::proto::raft::AppliedIndex;
use curvine_common::raft::storage::{LogStorage, RocksLogStorage};
use curvine_common::FsResult;
use log::{error, info, warn};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::{err_box, CommonResult};
use raft::eraftpb::Entry;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::watch;

#[derive(Clone, Debug, Default)]
struct ReplayState {
    epoch: u64,
    active: bool,
    ufs_applied: AppliedIndex,
    metadata_applied: AppliedIndex,
    shutdown: bool,
}

#[derive(Default)]
struct ReplayProgressState {
    epoch: u64,
    active: bool,
    metadata_applied: AppliedIndex,
    ufs_applied: AppliedIndex,
}

#[derive(Default)]
pub(crate) struct UfsReplayProgress {
    state: Mutex<ReplayProgressState>,
    changed: Condvar,
}

impl UfsReplayProgress {
    fn become_leader(&self, metadata_applied: AppliedIndex) {
        if let Ok(mut state) = self.state.lock() {
            state.epoch = state.epoch.wrapping_add(1);
            state.active = true;
            state.metadata_applied = metadata_applied;
            state.ufs_applied = AppliedIndex::default();
            self.changed.notify_all();
        }
    }

    fn become_follower(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.epoch = state.epoch.wrapping_add(1);
            state.active = false;
            self.changed.notify_all();
        }
    }

    fn set_metadata_applied(&self, applied: AppliedIndex) {
        if let Ok(mut state) = self.state.lock() {
            state.metadata_applied = applied;
            self.changed.notify_all();
        }
    }

    fn set_ufs_applied(&self, applied: AppliedIndex) {
        if let Ok(mut state) = self.state.lock() {
            if applied.op_id > state.ufs_applied.op_id {
                state.ufs_applied = applied;
            }
            self.changed.notify_all();
        }
    }

    pub(crate) fn wait_for_current(&self) -> FsResult<()> {
        let mut state = self.state.lock().map_err(|error| {
            FsError::common(format!("UFS replay progress lock poisoned: {}", error))
        })?;
        if !state.active {
            return Err(FsError::common(
                "master is not the active UFS replay leader",
            ));
        }
        let epoch = state.epoch;
        let target_op_id = state.metadata_applied.op_id;
        while state.ufs_applied.op_id < target_op_id {
            state = self.changed.wait(state).map_err(|error| {
                FsError::common(format!("UFS replay progress lock poisoned: {}", error))
            })?;
            if !state.active || state.epoch != epoch {
                return Err(FsError::common(
                    "master leadership changed while waiting for UFS replay",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct UfsReplayNotifier {
    control: watch::Sender<ReplayState>,
    progress: Arc<UfsReplayProgress>,
}

impl UfsReplayNotifier {
    pub(crate) fn become_leader(&self, ufs_applied: AppliedIndex, metadata_applied: AppliedIndex) {
        self.progress.become_leader(metadata_applied.clone());
        self.update(|state| {
            state.epoch = state.epoch.wrapping_add(1);
            state.active = true;
            state.ufs_applied = ufs_applied;
            state.metadata_applied = metadata_applied;
        });
    }

    pub(crate) fn become_follower(&self) {
        self.progress.become_follower();
        self.update(|state| {
            state.epoch = state.epoch.wrapping_add(1);
            state.active = false;
        });
    }

    pub(crate) fn notify_metadata_applied(&self, metadata_applied: AppliedIndex) {
        self.progress.set_metadata_applied(metadata_applied.clone());
        self.update(|state| {
            if state.active {
                state.metadata_applied = metadata_applied;
            }
        });
    }

    pub(crate) fn shutdown(&self) {
        self.update(|state| {
            state.shutdown = true;
        });
    }

    pub(crate) fn progress(&self) -> Arc<UfsReplayProgress> {
        self.progress.clone()
    }

    fn update(&self, update: impl FnOnce(&mut ReplayState)) {
        let mut state = self.control.borrow().clone();
        update(&mut state);
        self.control.send_replace(state);
    }
}

pub(crate) struct UfsReplayer {
    ufs_loader: UfsLoader,
    journal_writer: Arc<JournalWriter>,
    log_store: RocksLogStorage,
    progress: Arc<UfsReplayProgress>,
    batch_size: u64,
    max_retry_num: u64,
    skip_failed_after_retry: bool,
    retry_interval: Duration,
}

impl UfsReplayer {
    pub(crate) fn spawn(
        rt: Arc<Runtime>,
        ufs_loader: UfsLoader,
        journal_writer: Arc<JournalWriter>,
        log_store: RocksLogStorage,
        conf: &JournalConf,
    ) -> UfsReplayNotifier {
        let (control, receiver) = watch::channel(ReplayState::default());
        let progress = Arc::new(UfsReplayProgress::default());
        let replayer = Self {
            ufs_loader,
            journal_writer,
            log_store,
            progress: progress.clone(),
            batch_size: conf.scan_batch_size,
            max_retry_num: conf.max_retry_num,
            skip_failed_after_retry: conf.skip_failed_ufs_replay_after_retry,
            retry_interval: Duration::from_secs(conf.retry_interval_secs),
        };

        rt.spawn(async move {
            replayer.run(receiver).await;
        });

        UfsReplayNotifier { control, progress }
    }

    async fn run(self, mut receiver: watch::Receiver<ReplayState>) {
        if let Err(error) = self.run0(&mut receiver).await {
            error!(
                "fatal UFS replay error: {}; aborting master to avoid losing ordered UFS replay",
                error
            );
            std::process::abort();
        }
    }

    async fn run0(&self, receiver: &mut watch::Receiver<ReplayState>) -> CommonResult<()> {
        let mut epoch = 0;
        let mut last_replayed = 0;
        let mut retry_num = 0;

        loop {
            let state = receiver.borrow_and_update().clone();
            if state.shutdown {
                return Ok(());
            }
            if !state.active {
                if receiver.changed().await.is_err() {
                    return Ok(());
                }
                continue;
            }

            if state.epoch != epoch {
                epoch = state.epoch;
                last_replayed = state.ufs_applied.index;
                retry_num = 0;
                info!(
                    "starting ordered UFS replay, epoch={}, from_index={}",
                    epoch, last_replayed
                );
                if last_replayed >= state.metadata_applied.index && state.ufs_applied.op_id != 0 {
                    self.journal_writer.log_ufs_applied(
                        state.ufs_applied.op_id,
                        state.ufs_applied.term,
                        state.ufs_applied.index,
                    )?;
                }
            }

            let applied = state.metadata_applied.index;
            if last_replayed >= applied {
                if receiver.changed().await.is_err() {
                    return Ok(());
                }
                continue;
            }

            let high = (last_replayed + self.batch_size).min(applied + 1);
            let entries = self.log_store.scan_entries(last_replayed + 1, high)?;
            if entries.is_empty() {
                return err_box!(
                    "local raft journal is missing metadata-applied UFS entries: expected index {} before metadata_applied={}",
                    last_replayed + 1,
                    applied
                );
            }

            for entry in entries {
                if !Self::is_current_leader(receiver, epoch) {
                    break;
                }

                match self.replay_entry(&entry, false).await {
                    Ok(()) => {
                        last_replayed = entry.index;
                        retry_num = 0;
                    }
                    Err(error) => {
                        retry_num += 1;
                        if retry_num >= self.max_retry_num {
                            if !self.skip_failed_after_retry {
                                return Err(error);
                            }
                            error!(
                                "UFS replay failed after {} retries, skipping failed UFS operations at raft index {}: {}",
                                retry_num, entry.index, error
                            );
                            self.replay_entry(&entry, true).await?;
                            last_replayed = entry.index;
                            retry_num = 0;
                            continue;
                        }

                        error!(
                            "UFS replay failed(retry_num={}) at raft index {}: {}",
                            retry_num, entry.index, error
                        );
                        self.wait_retry(receiver, epoch).await;
                        break;
                    }
                }
            }
        }
    }

    fn is_current_leader(receiver: &watch::Receiver<ReplayState>, epoch: u64) -> bool {
        let state = receiver.borrow();
        state.active && !state.shutdown && state.epoch == epoch
    }

    async fn wait_retry(&self, receiver: &mut watch::Receiver<ReplayState>, epoch: u64) {
        let retry_at = tokio::time::Instant::now() + self.retry_interval;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(retry_at) => return,
                changed = receiver.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let state = receiver.borrow_and_update();
                    if state.shutdown || !state.active || state.epoch != epoch {
                        return;
                    }
                }
            }
        }
    }

    async fn replay_entry(&self, entry: &Entry, skip_failed_ufs: bool) -> CommonResult<()> {
        if entry.data.is_empty() {
            return Ok(());
        }

        let batch = JournalEnvelope::decode(&entry.data)?;
        let mut applied = AppliedIndex {
            term: entry.term,
            index: entry.index,
            ..Default::default()
        };
        let mut should_mark_applied = false;

        for command in batch.into_commands() {
            applied.op_id = command.op_id();
            applied.rpc_id = command.rpc_id();

            let result = match command {
                JournalCommand::Legacy(JournalEntry::UfsApplied(applied)) => {
                    self.progress.set_ufs_applied(AppliedIndex {
                        op_id: applied.op_id,
                        rpc_id: applied.rpc_id,
                        term: applied.term,
                        index: applied.index,
                    });
                    continue;
                }
                JournalCommand::Legacy(JournalEntry::Snapshot(_)) => {
                    should_mark_applied = true;
                    Ok(())
                }
                JournalCommand::Legacy(entry) => {
                    should_mark_applied = true;
                    self.ufs_loader.apply_entry(&entry).await
                }
                JournalCommand::Metadata(command) => {
                    should_mark_applied = true;
                    self.apply_metadata_command(command).await
                }
            };

            if let Err(error) = result {
                if skip_failed_ufs {
                    warn!(
                        "skipping failed UFS operation at raft index {}: {}",
                        entry.index, error
                    );
                    continue;
                }
                return Err(error);
            }
        }

        if should_mark_applied {
            self.journal_writer
                .log_ufs_applied(applied.op_id, applied.term, applied.index)?;
        }
        Ok(())
    }

    async fn apply_metadata_command(&self, command: MetadataCommand) -> CommonResult<()> {
        match command {
            MetadataCommand::Mkdir(entry) => self.ufs_loader.mkdir(&entry).await,
            MetadataCommand::CompleteFile(entry) => {
                self.ufs_loader
                    .apply_entry(&JournalEntry::CompleteFile(entry))
                    .await
            }
            MetadataCommand::Rename(entry) => {
                self.ufs_loader
                    .apply_entry(&JournalEntry::Rename(entry))
                    .await
            }
            MetadataCommand::Delete(entry) => {
                self.ufs_loader
                    .apply_entry(&JournalEntry::Delete(entry))
                    .await
            }
            _ => Ok(()),
        }
    }
}
