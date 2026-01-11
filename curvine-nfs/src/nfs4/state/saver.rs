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

//! State Saver Task
//!
//! Periodically saves NFSv4 state to Curvine filesystem.
//! Uses ScheduledExecutor for consistent task scheduling.

use crate::nfs4::state::{ClientManager, LockManager, OpenManager, StatePersistenceManager};
use orpc::runtime::{RpcRuntime, Runtime};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};

/// State saver task (runs periodically)
pub struct StateSaverTask {
    persistence: Arc<StatePersistenceManager>,
    clients: Arc<ClientManager>,
    opens: Arc<OpenManager>,
    locks: Arc<LockManager>,
    runtime: Arc<Runtime>,
    saving: Arc<AtomicU8>,
}

// Ensure StateSaverTask is Send (all Arc types are Send)
unsafe impl Send for StateSaverTask {}

impl StateSaverTask {
    /// Create a new state saver task
    pub fn new(
        persistence: Arc<StatePersistenceManager>,
        clients: Arc<ClientManager>,
        opens: Arc<OpenManager>,
        locks: Arc<LockManager>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            persistence,
            clients,
            opens,
            locks,
            runtime,
            saving: Arc::new(AtomicU8::new(0)),
        }
    }
}

impl orpc::runtime::LoopTask for StateSaverTask {
    type Error = std::io::Error;

    /// Run the state save operation
    ///
    /// This is called periodically by ScheduledExecutor.
    /// We use runtime.block_on() to execute async code in sync context.
    fn run(&self) -> Result<(), Self::Error> {
        if !self.persistence.is_enabled() {
            return Ok(());
        }

        debug!("Running periodic state save...");

        if self
            .saving
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        let persistence = self.persistence.clone();
        let clients = self.clients.clone();
        let opens = self.opens.clone();
        let locks = self.locks.clone();
        let saving = self.saving.clone();

        self.runtime.spawn(async move {
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                persistence.save_snapshot(&clients, &opens, &locks),
            )
            .await;

            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!("Failed to save state snapshot: {:?}", e);
                }
                Err(_) => {
                    error!("Failed to save state snapshot: timeout");
                }
            }

            saving.store(0, Ordering::Release);
        });

        Ok(())
    }

    /// Check if task should terminate (never terminate)
    fn terminate(&self) -> bool {
        false
    }
}
