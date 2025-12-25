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

//! Transaction tracking for retransmission detection

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Transaction key: (xid, client_addr)
/// Uses Arc<str> to avoid repeated String allocations
type TransactionKey = (u32, Arc<str>);

/// Tracks transaction state to detect retransmissions
pub struct TransactionTracker {
    retention_period: Duration,
    transactions: Mutex<HashMap<TransactionKey, TransactionState>>,
}

impl TransactionTracker {
    /// Create a new transaction tracker with the given retention period
    pub fn new(retention_period: Duration) -> Self {
        Self {
            retention_period,
            transactions: Mutex::new(HashMap::new()),
        }
    }

    /// Check if the transaction is a retransmission.
    /// If it's a new transaction, it is marked as `InProgress`.
    ///
    /// Returns `true` if the transaction is a retransmission, `false` otherwise.
    pub fn is_retransmission(&self, xid: u32, client_addr: &Arc<str>) -> bool {
        let key = (xid, Arc::clone(client_addr));
        let Ok(mut transactions) = self.transactions.lock() else {
            // If mutex is poisoned, treat as new transaction to avoid blocking
            return false;
        };
        housekeeping(&mut transactions, self.retention_period);
        if let std::collections::hash_map::Entry::Vacant(e) = transactions.entry(key) {
            e.insert(TransactionState::InProgress);
            false
        } else {
            true
        }
    }

    /// Mark the transaction as processed
    pub fn mark_processed(&self, xid: u32, client_addr: &Arc<str>) {
        let key = (xid, Arc::clone(client_addr));
        let completion_time = SystemTime::now();
        let Ok(mut transactions) = self.transactions.lock() else {
            // If mutex is poisoned, silently ignore
            return;
        };
        if let Some(tx) = transactions.get_mut(&key) {
            *tx = TransactionState::Completed(completion_time);
        }
    }
}

/// Remove old completed transactions
fn housekeeping(transactions: &mut HashMap<TransactionKey, TransactionState>, max_age: Duration) {
    let cutoff = SystemTime::now() - max_age;
    transactions.retain(|_, v| match v {
        TransactionState::InProgress => true,
        TransactionState::Completed(completion_time) => *completion_time >= cutoff,
    });
}

/// State of a transaction
pub enum TransactionState {
    /// Transaction is being processed
    InProgress,
    /// Transaction completed at the given time
    Completed(SystemTime),
}
