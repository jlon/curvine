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
use std::sync::Arc;
use tracing::{debug, error};

/// State saver task (runs periodically)
pub struct StateSaverTask {
    persistence: Arc<StatePersistenceManager>,
    clients: Arc<ClientManager>,
    opens: Arc<OpenManager>,
    locks: Arc<LockManager>,
    runtime: Arc<Runtime>,
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

        // Use runtime.block_on() to execute async save operation
        // This is safe because LoopTask::run() is designed to be blocking
        let result = self.runtime.block_on(async {
            self.persistence
                .save_snapshot(&self.clients, &self.opens, &self.locks)
                .await
        });

        if let Err(e) = result {
            error!("Failed to save state snapshot: {:?}", e);
            // Don't propagate error to avoid stopping the task
        }

        Ok(())
    }

    /// Check if task should terminate (never terminate)
    fn terminate(&self) -> bool {
        false
    }
}
