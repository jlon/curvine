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

use crate::block::BlockClient;
use crate::file::FsContext;
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, WorkerAddress};
use curvine_proto::DataHeaderProto;
use curvine_runtime::common::Utils;

pub struct BlockReaderRemote {
    client: BlockClient,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    pos: i64,
    len: i64,
    req_id: i64,
    seq_id: i32,
    header: Option<DataHeaderProto>,
    supports_read_len: bool,
}

impl BlockReaderRemote {
    pub async fn new(
        fs_context: &FsContext,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        let client = fs_context.acquire_read(&worker_address).await?;
        let context = client
            .open_block(
                &fs_context.conf.client,
                &block,
                off,
                len,
                req_id,
                seq_id,
                false,
            )
            .await?;

        Ok(
            Self::from_opened(client, block, worker_address, off, len, req_id, seq_id)
                .with_read_len_capability(context.supports_read_len),
        )
    }

    pub(crate) fn from_opened(
        client: BlockClient,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
        req_id: i64,
        seq_id: i32,
    ) -> Self {
        Self {
            client,
            block,
            worker_address,
            pos: off,
            len,
            req_id,
            seq_id,
            header: None,
            supports_read_len: false,
        }
    }

    pub(crate) fn with_read_len_capability(mut self, supports_read_len: bool) -> Self {
        self.supports_read_len = supports_read_len;
        self
    }

    pub(crate) fn supports_read_len(&self) -> bool {
        self.supports_read_len
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.pos
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<i64> {
        self.pos = pos;
        self.header = Some(DataHeaderProto {
            offset: pos,
            flush: false,
            is_last: false,
            read_len: None,
        });
        Ok(self.pos)
    }

    pub async fn read(&mut self) -> FsResult<DataSlice> {
        self.read_with_len(None).await
    }

    /// Keep the stateful worker cursor, but cap this response to the FUSE
    /// request when random reads do not need the rest of the session chunk.
    pub(crate) async fn read_with_len(&mut self, max_len: Option<usize>) -> FsResult<DataSlice> {
        if self.remaining() <= 0 {
            return err_box!("No readable data");
        }

        let seq_id = self.next_seq_id();
        let header = match max_len.filter(|_| self.supports_read_len) {
            None => self.header.take(),
            Some(max_len) => {
                let read_len = max_len.min(self.remaining() as usize);
                let read_len = i32::try_from(read_len)
                    .map_err(|_| curvine_error::FsError::common("read length exceeds i32::MAX"))?;
                let mut header = self.header.take().unwrap_or(DataHeaderProto {
                    offset: self.pos,
                    flush: false,
                    is_last: false,
                    read_len: None,
                });
                header.read_len = Some(read_len);
                Some(header)
            }
        };
        let chunk = self.client.read_data(self.req_id, seq_id, header).await?;

        self.pos += chunk.len() as i64;
        Ok(chunk)
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        self.client
            .read_commit(&self.block, self.req_id, next_seq_id)
            .await
    }

    pub fn block_id(&self) -> i64 {
        self.block.id
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }
}
