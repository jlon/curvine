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

use std::sync::{Condvar, Mutex};

pub struct NamespaceCommitGate {
    state: Mutex<CommitGateState>,
    changed: Condvar,
}

#[derive(Default)]
struct CommitGateState {
    open: bool,
    in_flight: usize,
    closed: usize,
}

impl NamespaceCommitGate {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(CommitGateState {
                open: true,
                in_flight: 0,
                closed: 0,
            }),
            changed: Condvar::new(),
        }
    }

    pub fn enter(&self) -> NamespaceCommitGuard<'_> {
        let mut state = self.state.lock().expect("namespace commit gate poisoned");
        while !state.open {
            state = self
                .changed
                .wait(state)
                .expect("namespace commit gate poisoned");
        }
        state.in_flight += 1;
        NamespaceCommitGuard { gate: self }
    }

    pub fn close_and_wait(&self) -> NamespaceCommitBarrier<'_> {
        let mut state = self.state.lock().expect("namespace commit gate poisoned");
        state.closed += 1;
        state.open = false;
        while state.in_flight != 0 {
            state = self
                .changed
                .wait(state)
                .expect("namespace commit gate poisoned");
        }
        NamespaceCommitBarrier { gate: self }
    }

    fn leave(&self) {
        let mut state = self.state.lock().expect("namespace commit gate poisoned");
        assert!(
            state.in_flight > 0,
            "namespace commit gate in-flight underflow"
        );
        state.in_flight -= 1;
        if state.in_flight == 0 {
            self.changed.notify_all();
        }
    }

    fn open_one(&self) {
        let mut state = self.state.lock().expect("namespace commit gate poisoned");
        assert!(state.closed > 0, "namespace commit gate close underflow");
        state.closed -= 1;
        if state.closed == 0 {
            state.open = true;
            self.changed.notify_all();
        }
    }
}

impl Default for NamespaceCommitGate {
    fn default() -> Self {
        Self::new()
    }
}

pub struct NamespaceCommitGuard<'a> {
    gate: &'a NamespaceCommitGate,
}

impl Drop for NamespaceCommitGuard<'_> {
    fn drop(&mut self) {
        self.gate.leave();
    }
}

pub struct NamespaceCommitBarrier<'a> {
    gate: &'a NamespaceCommitGate,
}

impl Drop for NamespaceCommitBarrier<'_> {
    fn drop(&mut self) {
        self.gate.open_one();
    }
}
