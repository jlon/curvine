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

use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

const CLOSED: usize = 1usize << (usize::BITS - 1);
const IN_FLIGHT_MASK: usize = !CLOSED;

/// Coordinates a bounded set of mutators with an occasional quiescent read.
///
/// The owner chooses the consistency domain by using a distinct gate per
/// domain. The primitive itself intentionally has no namespace-specific
/// behavior.
pub struct CommitGate {
    state: AtomicUsize,
    lifecycle: Mutex<CommitGateLifecycle>,
    changed: Condvar,
}

#[derive(Default)]
struct CommitGateLifecycle {
    closed: usize,
}

impl CommitGate {
    pub fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            lifecycle: Mutex::new(CommitGateLifecycle::default()),
            changed: Condvar::new(),
        }
    }

    pub fn enter(&self) -> CommitGateGuard<'_> {
        loop {
            let state = self.state.load(Ordering::Acquire);
            if state & CLOSED != 0 {
                let mut lifecycle = self.lifecycle.lock();
                while self.state.load(Ordering::Acquire) & CLOSED != 0 {
                    self.changed.wait(&mut lifecycle);
                }
                continue;
            }

            assert!(
                state & IN_FLIGHT_MASK < IN_FLIGHT_MASK,
                "namespace commit gate in-flight overflow"
            );
            if self
                .state
                .compare_exchange_weak(state, state + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return CommitGateGuard { gate: self };
            }
        }
    }

    /// Attempts a non-blocking writer admission.
    ///
    /// This is for an optional fast path. A caller that cannot enter must fall
    /// back to the regular blocking path; it must never mutate outside the
    /// gate. Successful admissions are visible to every close barrier.
    pub fn try_enter(&self) -> Option<CommitGateGuard<'_>> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & CLOSED != 0 {
                return None;
            }

            assert!(
                state & IN_FLIGHT_MASK < IN_FLIGHT_MASK,
                "namespace commit gate in-flight overflow"
            );
            match self.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(CommitGateGuard { gate: self }),
                Err(current) => state = current,
            }
        }
    }

    pub fn close_and_wait(&self) -> CommitGateBarrier<'_> {
        self.close_and_wait0();
        CommitGateBarrier { gate: self }
    }

    /// Starts an exclusive operation only while the gate is open.
    ///
    /// The returned guard both keeps new writers out and contributes one
    /// in-flight writer. A role-change barrier therefore waits for an active
    /// snapshot rather than treating a closed gate as already quiescent.
    pub fn close_and_enter_if_open(&self) -> Option<CommitGateExclusiveGuard<'_>> {
        let mut lifecycle = self.lifecycle.lock();
        if lifecycle.closed != 0 {
            return None;
        }

        lifecycle.closed = 1;
        self.state.fetch_or(CLOSED, Ordering::AcqRel);
        while self.state.load(Ordering::Acquire) & IN_FLIGHT_MASK != 0 {
            self.changed.wait(&mut lifecycle);
        }

        // A lifecycle controller may have closed the gate while this snapshot
        // waited for earlier writers. It owns the fence, so defer this snapshot.
        if lifecycle.closed != 1 {
            lifecycle.closed -= 1;
            return None;
        }

        let previous = self.state.fetch_add(1, Ordering::AcqRel);
        assert_eq!(
            previous, CLOSED,
            "exclusive namespace commit gate entered with active writers"
        );
        Some(CommitGateExclusiveGuard { gate: self })
    }

    /// Closes the gate until the returned RAII guard is dropped.
    ///
    /// This variant owns the gate reference so a lifecycle controller can keep
    /// writes closed across asynchronous state transitions.
    pub fn close_and_wait_owned(self: &std::sync::Arc<Self>) -> CommitGateOwnedBarrier {
        self.close_and_wait0();
        CommitGateOwnedBarrier { gate: self.clone() }
    }

    fn close_and_wait0(&self) {
        let mut lifecycle = self.lifecycle.lock();
        lifecycle.closed += 1;
        if lifecycle.closed == 1 {
            self.state.fetch_or(CLOSED, Ordering::AcqRel);
        }
        while self.state.load(Ordering::Acquire) & IN_FLIGHT_MASK != 0 {
            self.changed.wait(&mut lifecycle);
        }
    }

    fn leave(&self) {
        assert!(
            self.state.load(Ordering::Acquire) & IN_FLIGHT_MASK > 0,
            "namespace commit gate in-flight underflow"
        );
        let previous = self.state.fetch_sub(1, Ordering::Release);
        if previous & CLOSED != 0 && previous & IN_FLIGHT_MASK == 1 {
            let _lifecycle = self.lifecycle.lock();
            self.changed.notify_all();
        }
    }

    fn open_one(&self) {
        let mut lifecycle = self.lifecycle.lock();
        assert!(
            lifecycle.closed > 0,
            "namespace commit gate close underflow"
        );
        lifecycle.closed -= 1;
        if lifecycle.closed == 0 {
            self.state.fetch_and(IN_FLIGHT_MASK, Ordering::Release);
            self.changed.notify_all();
        }
    }

    fn leave_exclusive(&self) {
        let previous = self.state.fetch_sub(1, Ordering::Release);
        assert!(
            previous & CLOSED != 0 && previous & IN_FLIGHT_MASK > 0,
            "exclusive namespace commit gate in-flight underflow"
        );

        let mut lifecycle = self.lifecycle.lock();
        self.changed.notify_all();
        assert!(
            lifecycle.closed > 0,
            "namespace commit gate close underflow"
        );
        lifecycle.closed -= 1;
        if lifecycle.closed == 0 {
            self.state.fetch_and(IN_FLIGHT_MASK, Ordering::Release);
            self.changed.notify_all();
        }
    }
}

impl Default for CommitGate {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CommitGateGuard<'a> {
    gate: &'a CommitGate,
}

impl Drop for CommitGateGuard<'_> {
    fn drop(&mut self) {
        self.gate.leave();
    }
}

pub struct CommitGateBarrier<'a> {
    gate: &'a CommitGate,
}

pub struct CommitGateExclusiveGuard<'a> {
    gate: &'a CommitGate,
}

impl Drop for CommitGateExclusiveGuard<'_> {
    fn drop(&mut self) {
        self.gate.leave_exclusive();
    }
}

impl Drop for CommitGateBarrier<'_> {
    fn drop(&mut self) {
        self.gate.open_one();
    }
}

pub struct CommitGateOwnedBarrier {
    gate: std::sync::Arc<CommitGate>,
}

impl Drop for CommitGateOwnedBarrier {
    fn drop(&mut self) {
        self.gate.open_one();
    }
}
