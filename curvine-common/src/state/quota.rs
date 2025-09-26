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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaState {
    Available,
    Calculating,
    Exceeded,
}

impl Default for QuotaState {
    fn default() -> Self {
        QuotaState::Available
    }
}

impl std::fmt::Display for QuotaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaState::Available => write!(f, "Available"),
            QuotaState::Calculating => write!(f, "Calculating"),
            QuotaState::Exceeded => write!(f, "Exceeded"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaInfo {
    pub inode_id: i64,
    pub path: String,
    pub quota_size: i64,
    pub used_size: i64,
    pub state: QuotaState,
    pub created_time: i64,
    pub updated_time: i64,
    pub properties: HashMap<String, String>,
}

impl QuotaInfo {
    pub fn new(inode_id: i64, path: &str, quota_size: i64) -> Self {
        let now = orpc::common::LocalTime::mills() as i64;
        QuotaInfo {
            inode_id,
            path: path.to_string(),
            quota_size,
            used_size: 0,
            state: QuotaState::Calculating,
            created_time: now,
            updated_time: now,
            properties: HashMap::new(),
        }
    }

    pub fn is_exceeded(&self) -> bool {
        self.used_size > self.quota_size
    }

    pub fn usage_percentage(&self) -> f64 {
        if self.quota_size == 0 {
            0.0
        } else {
            self.used_size as f64 / self.quota_size as f64
        }
    }

    pub fn update_usage(&mut self, used_size: i64) {
        self.used_size = used_size;
        self.state = if self.is_exceeded() {
            QuotaState::Exceeded
        } else {
            QuotaState::Available
        };
        self.updated_time = orpc::common::LocalTime::mills() as i64;
    }

    pub fn set_calculating(&mut self) {
        self.state = QuotaState::Calculating;
        self.updated_time = orpc::common::LocalTime::mills() as i64;
    }
}
