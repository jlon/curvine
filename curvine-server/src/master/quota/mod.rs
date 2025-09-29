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

mod quota_table;
pub use quota_table::QuotaTable;

mod quota_manager;
pub use quota_manager::QuotaManager;

pub mod eviction;
// Inline observer trait here to avoid separate control_plane module
use curvine_common::state::FileStatus;
pub trait QuotaObserver: Send + Sync {
    fn on_size_change(&self, status: &FileStatus);
    fn on_access(&self, status: &FileStatus);
    fn on_open(&self, status: &FileStatus);
}
