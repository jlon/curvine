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

#![allow(clippy::too_many_arguments)]

use crate::block::{
    BlockClientPool, BlockReadContext, CreateBatchBlockContext, CreateBlockContext,
};
use crate::file::FsContext;
use curvine_config::ClientConf;
use curvine_core_error::ErrorExt;
use curvine_core_error::{try_option_ref, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{Path, RpcCode};
use curvine_io::DataSlice;
use curvine_model::ProtoUtils;
use curvine_model::{ExtendedBlock, StorageType, WorkerAddress};
use curvine_proto::{
    BlockReadRequest, BlockReadResponse, BlockWriteRequest, BlockWriteResponse,
    BlocksBatchCommitRequest, BlocksBatchWriteRequest, BlocksBatchWriteResponse, DataHeaderProto,
    FileWriteData, FilesBatchWriteRequest,
};
use curvine_rpc::client::RpcClient;
use curvine_rpc::handler::RpcReceiveStats;
use curvine_rpc::message::{Builder, Message, RequestStatus};
use curvine_runtime::common::LocalTime;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A request remains in flight until its complete response has been received.
/// Dropping the request future before that point leaves the flag set so the
/// connection cannot be returned to the block-client pool.
struct RequestGuard {
    in_flight: Arc<AtomicUsize>,
}

impl RequestGuard {
    fn new(in_flight: Arc<AtomicUsize>) -> Self {
        in_flight.fetch_add(1, Ordering::AcqRel);
        Self { in_flight }
    }

    fn complete(self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct BlockClient {
    client: Option<RpcClient>,
    client_name: String,
    timeout: Duration,
    pool: Option<Arc<BlockClientPool>>,
    worker_addr: WorkerAddress,
    uptime: u64,
    in_flight: Arc<AtomicUsize>,
}

impl BlockClient {
    pub fn new(client: RpcClient, worker_addr: WorkerAddress, context: &FsContext) -> Self {
        Self {
            client: Some(client),
            client_name: context.clone_client_name(),
            timeout: Duration::from_millis(context.conf.client.data_timeout_ms),
            pool: None,
            worker_addr,
            uptime: LocalTime::mills(),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn begin_request(&self) -> RequestGuard {
        RequestGuard::new(self.in_flight.clone())
    }

    pub fn set_pool(&mut self, pool: Arc<BlockClientPool>) {
        self.pool.replace(pool);
        self.uptime = LocalTime::mills();
    }

    pub fn clear_pool(&mut self) {
        self.pool.take();
    }

    pub fn worker_addr(&self) -> &WorkerAddress {
        &self.worker_addr
    }

    pub fn pool(&self) -> &Option<Arc<BlockClientPool>> {
        &self.pool
    }

    pub fn uptime(&self) -> u64 {
        self.uptime
    }

    pub fn set_uptime(&mut self) {
        self.uptime = LocalTime::mills();
    }

    pub fn is_active(&self) -> bool {
        self.client
            .as_ref()
            .is_some_and(|client| client.is_active())
    }

    pub async fn rpc(&self, msg: Message) -> FsResult<Message> {
        let request = self.begin_request();
        let client = try_option_ref!(self.client);
        let rep_msg = match client.timeout_rpc(self.timeout, msg).await {
            Ok(rep_msg) => rep_msg,
            Err(err) => {
                client.set_closed();
                return Err(FsError::from(err));
            }
        };
        let result = match rep_msg.check_error_ext::<FsError>() {
            Ok(_) => Ok(rep_msg),
            Err(e) => Err(e.ctx(format!("rpc failed to worker {}", self.worker_addr))),
        };
        request.complete();
        result
    }

    async fn rpc_with_receive_stats(
        &self,
        msg: Message,
    ) -> FsResult<(Message, Option<RpcReceiveStats>)> {
        let request = self.begin_request();
        let client = try_option_ref!(self.client);
        let (rep_msg, receive_stats) = match client
            .timeout_rpc_with_receive_stats(self.timeout, msg)
            .await
        {
            Ok(rep_msg) => rep_msg,
            Err(err) => {
                client.set_closed();
                return Err(FsError::from(err));
            }
        };
        let result = match rep_msg.check_error_ext::<FsError>() {
            Ok(_) => Ok((rep_msg, receive_stats)),
            Err(e) => Err(e.ctx(format!("rpc failed to worker {}", self.worker_addr))),
        };
        request.complete();
        result
    }

    pub async fn write_block(
        &self,
        blk: &ExtendedBlock,
        off: i64,
        block_size: i64,
        req_id: i64,
        seq_id: i32,
        chunk_size: i32,
        short_circuit: bool,
        pipeline_stream: Vec<WorkerAddress>,
    ) -> FsResult<CreateBlockContext> {
        let pipeline_stream = pipeline_stream
            .iter()
            .map(ProtoUtils::worker_address_to_pb)
            .collect();
        let header = BlockWriteRequest {
            block: ProtoUtils::extend_block_to_pb(blk.clone()),
            off,
            block_size,
            short_circuit,
            client_name: self.client_name.to_string(),
            chunk_size,
            pipeline_stream,
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Open)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(header)
            .build();

        let rep = self.rpc(msg).await?;
        let rep_header: BlockWriteResponse = rep.parse_header()?;

        let context = CreateBlockContext {
            id: rep_header.id,
            off: rep_header.off,
            block_size: rep_header.block_size,
            storage_type: StorageType::from(rep_header.storage_type),
            path: rep_header.path,
        };

        Ok(context)
    }

    pub async fn write_data(
        &self,
        buf: DataSlice,
        req_id: i64,
        seq_id: i32,
        header: Option<DataHeaderProto>,
    ) -> CommonResult<()> {
        let mut builder = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq_id)
            .data(buf);

        if let Some(header) = header {
            builder = builder.proto_header(header);
        }

        let msg = builder.build();
        let _ = self.rpc(msg).await?;
        Ok(())
    }

    pub async fn write_flush(&self, pos: i64, req_id: i64, seq_id: i32) -> CommonResult<()> {
        let header = DataHeaderProto {
            offset: pos,
            flush: true,
            is_last: false,
            read_len: None,
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(header)
            .build();
        let _ = self.rpc(msg).await?;
        Ok(())
    }

    // Write complete
    pub async fn write_commit(
        &self,
        block: &ExtendedBlock,
        off: i64,
        block_size: i64,
        req_id: i64,
        seq_id: i32,
        cancel: bool,
    ) -> FsResult<()> {
        let header = BlockWriteRequest {
            block: ProtoUtils::extend_block_to_pb(block.clone()),
            off,
            block_size,
            client_name: self.client_name.to_string(),
            ..Default::default()
        };

        let status = if cancel {
            RequestStatus::Cancel
        } else {
            RequestStatus::Complete
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlock)
            .request(status)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(header)
            .build();

        let _ = FsContext::metrics_track("WriteCommitBlock", self.rpc(msg)).await?;
        Ok(())
    }

    // Open a block.
    pub async fn open_block(
        &self,
        conf: &ClientConf,
        block: &ExtendedBlock,
        off: i64,
        len: i64,
        req_id: i64,
        seq_id: i32,
        short_circuit: bool,
    ) -> FsResult<BlockReadContext> {
        let request = BlockReadRequest {
            id: block.id,
            off,
            len,
            chunk_size: conf.read_chunk_size as i32,
            short_circuit,
            enable_read_ahead: conf.enable_read_ahead,
            read_ahead_len: conf.read_ahead_len,
            drop_cache_len: conf.drop_cache_len,
        };

        let msg = Builder::new()
            .code(RpcCode::ReadBlock)
            .request(RequestStatus::Open)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(request)
            .build();

        let rep = FsContext::metrics_track("OpenBlock", self.rpc(msg)).await?;
        let rep_header: BlockReadResponse = rep.parse_header()?;

        Ok(BlockReadContext::from_req(rep_header))
    }

    pub async fn read_commit(
        &self,
        block: &ExtendedBlock,
        req_id: i64,
        seq_id: i32,
    ) -> FsResult<()> {
        let request = BlockReadRequest {
            id: block.id,
            ..Default::default()
        };

        let msg = Builder::new()
            .code(RpcCode::ReadBlock)
            .request(RequestStatus::Complete)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(request)
            .build();

        let _ = FsContext::metrics_track("ReadCommitBlock", self.rpc(msg)).await?;
        Ok(())
    }

    pub async fn read_data(
        &self,
        req_id: i64,
        seq_id: i32,
        header: Option<DataHeaderProto>,
    ) -> FsResult<DataSlice> {
        let builder = Builder::new()
            .code(RpcCode::ReadBlock)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq_id);

        let msg = if let Some(header) = header {
            builder.proto_header(header).build()
        } else {
            builder.build()
        };

        let (rep, receive_stats) =
            FsContext::metrics_track("ReadBlock", self.rpc_with_receive_stats(msg)).await?;
        if let Some(stats) = receive_stats {
            FsContext::get_metrics().record_read_block_receive(stats);
        }
        Ok(rep.data)
    }

    pub async fn write_blocks_batch(
        &self,
        blocks: &[ExtendedBlock],
        off: i64,
        block_size: i64,
        req_id: i64,
        seq_id: i32,
        chunk_size: i32,
        short_circuit: bool,
    ) -> FsResult<CreateBatchBlockContext> {
        let blocks_pb: Vec<_> = blocks
            .iter()
            .map(|block| ProtoUtils::extend_block_to_pb(block.clone()))
            .collect();

        let req_header = BlocksBatchWriteRequest {
            blocks: blocks_pb,
            off,
            block_size,
            req_id,
            seq_id,
            chunk_size,
            short_circuit,
            client_name: self.client_name.to_string(),
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(RequestStatus::Open)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(req_header)
            .build();

        let rep = self.rpc(msg).await?;
        let rep_header: BlocksBatchWriteResponse = rep.parse_header()?;
        let mut batch_context = CreateBatchBlockContext::new(req_id);

        for response in rep_header.responses {
            let context = CreateBlockContext {
                id: response.id,
                off: response.off,
                block_size: response.block_size,
                storage_type: StorageType::from(response.storage_type),
                path: response.path,
            };
            batch_context.push(context);
        }

        Ok(batch_context)
    }

    pub async fn write_commit_batch(
        &self,
        blocks: &[ExtendedBlock],
        off: i64,
        block_size: i64,
        req_id: i64,
        seq_id: i32,
        cancel: bool,
    ) -> FsResult<()> {
        // Convert blocks to protobuf
        let blocks_pb: Vec<_> = blocks
            .iter()
            .map(|block| ProtoUtils::extend_block_to_pb(block.clone()))
            .collect();

        let header = BlocksBatchCommitRequest {
            blocks: blocks_pb,
            off,
            block_size,
            req_id,
            seq_id,
            cancel,
        };

        let status = if cancel {
            RequestStatus::Cancel
        } else {
            RequestStatus::Complete
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(status)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(header)
            .build();

        let _ = self.rpc(msg).await?;
        Ok(())
    }

    pub async fn write_files_batch(
        &self,
        files: &[(&Path, &str)],
        req_id: i64,
        seq_id: i32,
    ) -> CommonResult<()> {
        let file_data: Vec<_> = files
            .iter()
            .map(|(path, content)| FileWriteData {
                path: path.to_string(),
                content: content.as_bytes().to_vec(),
            })
            .collect();

        let header = FilesBatchWriteRequest {
            files: file_data,
            req_id,
            seq_id,
        };

        let msg = Builder::new()
            .code(RpcCode::WriteBlocksBatch)
            .request(RequestStatus::Running)
            .req_id(req_id)
            .seq_id(seq_id)
            .proto_header(header)
            .build();

        let _ = self.rpc(msg).await?;
        Ok(())
    }
}

impl Drop for BlockClient {
    fn drop(&mut self) {
        if self.in_flight.load(Ordering::Acquire) != 0 {
            // A cancelled receive may leave the old response in this socket.
            // Marking it closed makes `BlockClientPool::release` discard it.
            if let Some(client) = &self.client {
                client.set_closed();
            }
        }
        if let Some(pool) = self.pool.take() {
            if let Some(moved_client) = self.client.take() {
                let client = BlockClient {
                    client: Some(moved_client),
                    client_name: std::mem::take(&mut self.client_name),
                    timeout: self.timeout,
                    pool: Some(pool.clone()),
                    worker_addr: std::mem::take(&mut self.worker_addr),
                    uptime: self.uptime,
                    in_flight: self.in_flight.clone(),
                };

                pool.release(client);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_request_keeps_connection_tainted() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let request = RequestGuard::new(in_flight.clone());
        drop(request);

        assert_eq!(in_flight.load(Ordering::Acquire), 1);
    }

    #[test]
    fn completed_request_clears_connection_taint() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        RequestGuard::new(in_flight.clone()).complete();

        assert_eq!(in_flight.load(Ordering::Acquire), 0);
    }

    #[test]
    fn completed_request_does_not_clear_another_in_flight_request() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let first = RequestGuard::new(in_flight.clone());
        let second = RequestGuard::new(in_flight.clone());

        first.complete();
        assert_eq!(in_flight.load(Ordering::Acquire), 1);

        drop(second);
        assert_eq!(in_flight.load(Ordering::Acquire), 1);
    }
}
