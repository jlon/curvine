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

use super::journal_writer::JournalWriteRequest;
use crate::master::journal::{JournalCommandBatch, JournalEntry, MetadataCommand};
use crate::master::{Master, MasterMetrics};
use curvine_config::JournalConf;
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_raft::raft::RaftClient;
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::sync::channel::BlockingReceiver;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;

pub struct SenderTask {
    pub(crate) client: RaftClient,
    pub(crate) batch: JournalCommandBatch,
    pub(crate) flush_batch_ms: u64,
    pub(crate) flush_batch_size: u64,
    pub(crate) last_flush_ms: u64,
    pub(crate) metrics: &'static MasterMetrics,
    metadata_acks: Vec<mpsc::SyncSender<FsResult<()>>>,
}

impl SenderTask {
    pub fn new(client: RaftClient, conf: &JournalConf, batch_seq_id: u64) -> CommonResult<Self> {
        let sender = Self {
            client,
            batch: JournalCommandBatch::new(batch_seq_id),
            flush_batch_ms: conf.writer_flush_batch_ms,
            flush_batch_size: conf.writer_flush_batch_size,
            last_flush_ms: LocalTime::mills(),
            metrics: Master::get_metrics()?,
            metadata_acks: vec![],
        };

        Ok(sender)
    }

    // Start a thread to execute sender task
    pub(crate) fn spawn(self, receiver: BlockingReceiver<JournalWriteRequest>) -> FsResult<()> {
        let poll = Duration::from_millis(self.flush_batch_ms);
        let name = "journal-writer".to_string();
        let task = self;
        thread::Builder::new().name(name.clone()).spawn(move || {
            if let Err(e) = Self::loop0(receiver, poll, task) {
                log::error!("thread {} stop: {:?}; aborting master", name, e);
                std::process::abort();
            }
        })?;
        Ok(())
    }

    fn loop0(
        receiver: BlockingReceiver<JournalWriteRequest>,
        poll: Duration,
        mut task: SenderTask,
    ) -> FsResult<()> {
        // The thread should not end.
        loop {
            let event = match receiver.recv_timeout(poll) {
                Ok(v) => Some(v),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => {
                    task.flush_pending()?;
                    return Ok(());
                }
            };
            task.handle(event)?;
        }
    }

    pub(crate) fn handle(&mut self, req: Option<JournalWriteRequest>) -> FsResult<()> {
        let mut start_metadata_batch = false;
        let force = if let Some(req) = req {
            match req {
                JournalWriteRequest::Legacy(entry) => {
                    let force = matches!(entry, JournalEntry::Snapshot(_));
                    self.batch.push_legacy(entry);
                    self.metrics.journal_queue_len.dec();
                    force
                }
                JournalWriteRequest::Metadata(commands, ack) => {
                    let force = commands.iter().any(|command| {
                        matches!(
                            command,
                            MetadataCommand::Mount(_) | MetadataCommand::UnMount(_)
                        )
                    });
                    if force {
                        self.flush_pending()?;
                    }
                    start_metadata_batch = self.batch.is_empty();
                    for command in commands {
                        self.batch.push_metadata(command);
                    }
                    self.metrics.journal_queue_len.dec();
                    self.metadata_acks.push(ack);
                    force
                }
            }
        } else {
            false
        };

        if start_metadata_batch {
            self.last_flush_ms = LocalTime::mills();
        }

        let len = self.batch.len() as u64;
        if len == 0 {
            return Ok(());
        }

        if force || len >= self.flush_batch_size || self.flush_interval_elapsed() {
            self.flush_pending()?;
        }

        Ok(())
    }

    fn flush_interval_elapsed(&self) -> bool {
        LocalTime::mills() - self.last_flush_ms >= self.flush_batch_ms
    }

    fn flush_pending(&mut self) -> FsResult<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let spend = TimeSpent::new();

        let bytes = match crate::master::journal::JournalEnvelope::encode(self.batch.clone()) {
            Ok(bytes) => bytes,
            Err(error) => {
                let message = error.to_string();
                self.complete_metadata_acks(Some(&message));
                return Err(error.into());
            }
        };
        if let Err(error) = self.client.block_on_send_propose(bytes) {
            let message = error.to_string();
            self.complete_metadata_acks(Some(&message));
            return Err(error.into());
        }

        self.metrics.journal_flush_count.inc();
        self.metrics
            .journal_flush_time
            .inc_by(spend.used_us() as i64);

        self.complete_metadata_acks(None);

        // Scroll to the next batch.
        self.batch.next();
        self.last_flush_ms = LocalTime::mills();

        Ok(())
    }

    fn complete_metadata_acks(&mut self, error: Option<&str>) {
        for ack in std::mem::take(&mut self.metadata_acks) {
            let result = match error {
                Some(error) => Err(FsError::common(error)),
                None => Ok(()),
            };
            if ack.send(result).is_err() {
                log::warn!("metadata journal receiver dropped response");
            }
        }
    }
}
