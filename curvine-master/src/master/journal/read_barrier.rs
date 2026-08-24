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

use curvine_error::{FsError, FsResult};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub(super) struct JournalReadBarrier {
    catching_up: AtomicBool,
    required_index: AtomicU64,
    applied_index: AtomicU64,
}

impl JournalReadBarrier {
    pub(super) fn new() -> Self {
        Self {
            catching_up: AtomicBool::new(false),
            required_index: AtomicU64::new(0),
            applied_index: AtomicU64::new(0),
        }
    }

    pub(super) fn begin_catch_up(&self, applied_index: u64, required_index: u64) {
        self.applied_index.store(applied_index, Ordering::Release);
        self.required_index.store(required_index, Ordering::Release);
        self.catching_up
            .store(applied_index < required_index, Ordering::Release);
    }

    pub(super) fn require_catch_up(&self, required_index: u64) {
        self.update_max(&self.required_index, required_index);
        self.refresh_catch_up_state();
    }

    pub(super) fn advance_applied(&self, applied_index: u64) {
        if self.catching_up.load(Ordering::Acquire) {
            return;
        }
        self.update_max(&self.applied_index, applied_index);
        self.refresh_catch_up_state();
    }

    pub(super) fn advance_catch_up(&self, applied_index: u64) {
        self.update_max(&self.applied_index, applied_index);
        self.refresh_catch_up_state();
    }

    pub(super) fn ensure_current(&self) -> FsResult<()> {
        if self.is_current() {
            return Ok(());
        }

        let required_index = self.required_index.load(Ordering::Acquire);
        let applied_index = self.applied_index.load(Ordering::Acquire);

        Err(FsError::not_leader(format!(
            "master metadata is catching up with committed raft logs: applied_index={}, required_index={}",
            applied_index, required_index
        )))
    }

    pub(super) fn is_current(&self) -> bool {
        let required_index = self.required_index.load(Ordering::Acquire);
        let applied_index = self.applied_index.load(Ordering::Acquire);
        !self.catching_up.load(Ordering::Acquire) && applied_index >= required_index
    }

    fn refresh_catch_up_state(&self) {
        let required_index = self.required_index.load(Ordering::Acquire);
        let applied_index = self.applied_index.load(Ordering::Acquire);
        self.catching_up
            .store(applied_index < required_index, Ordering::Release);
    }

    fn update_max(&self, target: &AtomicU64, value: u64) {
        let mut current = target.load(Ordering::Acquire);
        while value > current {
            match target.compare_exchange(current, value, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }
}

impl Default for JournalReadBarrier {
    fn default() -> Self {
        Self::new()
    }
}
