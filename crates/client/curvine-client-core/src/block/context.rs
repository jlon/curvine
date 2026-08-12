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

use curvine_model::StorageType;
use curvine_proto::BlockReadResponse;

pub struct CreateBlockContext {
    pub id: i64,
    pub off: i64,
    pub block_size: i64,
    pub path: Option<String>,
    pub storage_type: StorageType,
}

pub struct BlockReadContext {
    pub id: i64,
    pub len: i64,
    pub path: Option<String>,
    pub storage_type: StorageType,
    pub supports_read_len: bool,
}

impl BlockReadContext {
    pub fn from_req(req: BlockReadResponse) -> Self {
        Self {
            id: req.id,
            len: req.len,
            path: req.path,
            storage_type: StorageType::from(req.storage_type),
            supports_read_len: req.supports_read_len.unwrap_or(false),
        }
    }
}

pub struct CreateBatchBlockContext {
    pub contexts: Vec<CreateBlockContext>,
    pub batch_id: i64,
}

impl CreateBatchBlockContext {
    pub fn new(batch_id: i64) -> Self {
        Self {
            contexts: Vec::new(),
            batch_id,
        }
    }

    pub fn push(&mut self, context: CreateBlockContext) {
        self.contexts.push(context);
    }

    pub fn len(&self) -> usize {
        self.contexts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contexts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::BlockReadContext;
    use curvine_proto::{BlockReadResponse, StorageTypeProto};

    #[test]
    fn old_worker_response_disables_read_len() {
        let context = BlockReadContext::from_req(BlockReadResponse {
            id: 1,
            len: 1,
            path: None,
            storage_type: StorageTypeProto::Disk.into(),
            supports_read_len: None,
        });

        assert!(!context.supports_read_len);
    }

    #[test]
    fn capable_worker_response_enables_read_len() {
        let context = BlockReadContext::from_req(BlockReadResponse {
            id: 1,
            len: 1,
            path: None,
            storage_type: StorageTypeProto::Disk.into(),
            supports_read_len: Some(true),
        });

        assert!(context.supports_read_len);
    }
}
