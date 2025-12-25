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

//! Delegation Recall Timeout Reaper
//!
//! This module implements a background task that periodically checks for
//! timed-out delegation recalls and revokes them, following NFS-Ganesha's
//! delegation management approach.

use crate::nfs4::delegation::DelegationManager;
use orpc::runtime::LoopTask;
use std::sync::Arc;
use tracing::{debug, warn};

/// Delegation recall timeout reaper task
///
/// Periodically checks for delegation recalls that have timed out
/// and revokes them. This prevents clients from holding onto
/// delegations indefinitely after a recall has been initiated.
pub struct DelegationReaperTask {
    /// Delegation manager
    delegations: Arc<DelegationManager>,
    /// Termination flag
    terminated: Arc<std::sync::atomic::AtomicBool>,
}

impl DelegationReaperTask {
    pub fn new(delegations: Arc<DelegationManager>) -> Self {
        Self {
            delegations,
            terminated: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn terminate(&self) {
        self.terminated
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

impl LoopTask for DelegationReaperTask {
    type Error = std::io::Error;

    fn run(&self) -> Result<(), Self::Error> {
        // Skip if delegations are disabled
        if !self.delegations.is_enabled() {
            return Ok(());
        }

        // Check for timed-out recalls
        let revoked = self.delegations.cleanup_timed_out_recalls();

        if !revoked.is_empty() {
            warn!("Revoked {} timed-out delegation recalls", revoked.len());
            for (fileid, clientid) in revoked {
                debug!(
                    "Revoked delegation for file {} from client {} due to recall timeout",
                    fileid, clientid
                );
            }
        }

        Ok(())
    }

    fn terminate(&self) -> bool {
        self.terminated.load(std::sync::atomic::Ordering::Relaxed)
    }
}
