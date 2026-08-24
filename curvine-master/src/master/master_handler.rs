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

use crate::master::fs::{FsRetryCache, MasterFilesystem, OperationStatus};
use crate::master::job::JobHandler;
use crate::master::replication::master_replication_handler::MasterReplicationHandler;
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use crate::master::MountManager;
use crate::master::{Master, MasterMetrics, RpcContext};
use curvine_config::ClusterConf;
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_fs_api::RpcCode;
use curvine_model::ProtoUtils;
use curvine_model::{
    CompatibilityMode,
    CompatibilityPolicy,
    CompatibilityVerdict,
    CreateFileOpts,
    DeleteBlockCmd,
    DeleteResult, FileBlocks,
    FileLock,
    FileStatus,
    FilesystemInfo,
    FreeResult,
    HeartbeatStatus,
    ListOptions,
    OpenFlags,
    RenameFlags,
    WorkerCommand,
    WorkerInfo,
};
use dashmap::DashMap;
use curvine_net::net::ConnState;
use curvine_proto::*;
use curvine_rpc::handler::MessageHandler;
use curvine_rpc::message::Message;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct MasterHandler {
    pub(crate) fs: MasterFilesystem,
    pub(crate) retry_cache: Option<FsRetryCache>,
    pub(crate) metrics: &'static MasterMetrics,
    pub(crate) audit_logging_enabled: bool,
    pub(crate) conn_state: Option<ConnState>,
    pub(crate) job_handler: JobHandler,
    pub(crate) mount_manager: Arc<MountManager>,
    pub(crate) control_rpc_rt: Arc<Runtime>,
    pub(crate) control_rpc_admission: Arc<Semaphore>,
    pub(crate) client_request_admission: Arc<Semaphore>,
    pub(crate) client_blocking_admission: Option<Arc<Semaphore>>,
    pub(crate) control_request_admission: Arc<Semaphore>,
    pub(crate) metadata_read_rt: Arc<Runtime>,
    pub(crate) metadata_read_admission: Arc<Semaphore>,
    pub(crate) replication_handler: Option<MasterReplicationHandler>,
    pub(crate) actor_rt: Arc<Runtime>,
    // Master's own version + compatibility contract, built once at startup.
    master_compatibility: ServerCompatibilityInfoProto,
    // Compatibility policy derived from the master configuration.
    compatibility_policy: CompatibilityPolicy,
    // Last compatibility verdict warned about per peer.
    compat_warned: DashMap<String, CompatibilityVerdict>,
}

impl MasterHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conf: &ClusterConf,
        fs: MasterFilesystem,
        retry_cache: Option<FsRetryCache>,
        conn_state: Option<ConnState>,
        mount_manager: Arc<MountManager>,
        job_handler: JobHandler,
        control_rpc_rt: Arc<Runtime>,
        control_rpc_admission: Arc<Semaphore>,
        client_request_admission: Arc<Semaphore>,
        client_blocking_admission: Option<Arc<Semaphore>>,
        control_request_admission: Arc<Semaphore>,
        metadata_read_rt: Arc<Runtime>,
        metadata_read_admission: Arc<Semaphore>,
        replication_manager: Arc<MasterReplicationManager>,
        actor_rt: Arc<Runtime>,
        metrics: &'static MasterMetrics,
    ) -> Self {
        metrics.active_connections.inc();
        // Build the master's compatibility payload once; GetFilesystemInfo can
        // be hot (statfs) and the underlying version metadata is immutable.
        let master_version = curvine_sys::version::component_version("master");
        let compatibility_policy = conf.master.compatibility.to_policy();
        let master_compatibility =
            ProtoUtils::compatibility_to_pb(&master_version, &compatibility_policy);
        Self {
            fs,
            retry_cache,
            metrics,
            audit_logging_enabled: conf.master.audit_logging_enabled,
            conn_state,
            mount_manager,
            job_handler,
            control_rpc_rt,
            control_rpc_admission,
            client_request_admission,
            client_blocking_admission,
            control_request_admission,
            metadata_read_rt,
            metadata_read_admission,
            replication_handler: Some(MasterReplicationHandler::new(replication_manager)),
            actor_rt,
            master_compatibility,
            compatibility_policy,
            compat_warned: DashMap::new(),
        }
    }

    pub fn get_req_cache(&self, id: i64) -> Option<OperationStatus> {
        if let Some(ref c) = self.retry_cache {
            c.get(&id)
        } else {
            None
        }
    }

    pub fn set_req_cache<T>(&self, id: i64, res: FsResult<T>) -> FsResult<T> {
        if let Some(ref c) = self.retry_cache {
            c.set_status(id, res.is_ok())
        }
        res
    }

    pub fn check_is_retry(&self, id: i64) -> FsResult<bool> {
        if let Some(ref c) = self.retry_cache {
            c.check_is_retry(id)
        } else {
            Ok(false)
        }
    }

    pub fn mkdir(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: MkdirRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let opts = ProtoUtils::mkdir_opts_from_pb(header.opts);
        let status = self.fs.mkdir_with_opts(&header.path, opts)?;
        let rep_header = MkdirResponse {
            status: ProtoUtils::file_status_to_pb(status),
            ..Default::default()
        };
        ctx.response(rep_header)
    }

    fn create_file0(
        &self,
        req_id: i64,
        path: String,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        if self.check_is_retry(req_id)? {
            // HDFS retries return the results of the last calculation
            // Alluxio Retry will re-query the status.
            // The same solution as alluxio is adopted here. In fact, the hdfs solution is better, but rust requires an additional memory copy to achieve it.
            // Re-querying the file status may cause the request to be unidempotent.
            return self.fs.file_status(&path);
        }

        let res = self.fs.create_with_opts(&path, opts, flags);
        self.set_req_cache(req_id, res)
    }

    pub fn retry_check_create_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: CreateFileRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let opts = ProtoUtils::create_opts_from_pb(header.opts);
        let flags = OpenFlags::new(header.flags);
        let status = self.create_file0(ctx.msg.req_id(), header.path, opts, flags)?;

        let rep_header = CreateFileResponse {
            file_status: ProtoUtils::file_status_to_pb(status),
        };
        ctx.response(rep_header)
    }

    pub fn retry_check_open_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: OpenFileRequest = ctx.parse_header()?;

        let opts = ProtoUtils::create_opts_from_pb(header.opts);
        let flags = OpenFlags::new(header.flags);
        let audit_path = format!("{}:{}", flags.access_mark(), header.path);
        ctx.set_audit(Some(audit_path), None);

        let file_blocks = self.open_file0(ctx.msg.req_id(), header.path, opts, flags)?;

        let rep_header = OpenFileResponse {
            file_blocks: ProtoUtils::file_blocks_to_pb(file_blocks),
        };
        ctx.response(rep_header)
    }

    fn open_file0(
        &self,
        req_id: i64,
        path: String,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileBlocks> {
        if flags.read_only() {
            return self.fs.get_block_locations(&path);
        }

        if self.check_is_retry(req_id)? {
            return self.fs.get_block_locations(&path);
        }

        let res = self.fs.open_file(path, opts, flags);
        self.set_req_cache(req_id, res)
    }

    pub fn file_status(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: GetFileStatusRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let status = self.fs.file_status(header.path.as_str())?;
        let rep_header = GetFileStatusResponse {
            status: ProtoUtils::file_status_to_pb(status),
        };

        ctx.response(rep_header)
    }

    async fn async_file_status(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: GetFileStatusRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);
        let fs = self.fs.clone();
        let status = Self::run_master_rpc_task(
            self.metadata_read_rt.clone(),
            self.metadata_read_admission.clone(),
            move || fs.file_status(&header.path),
        )
        .await?;
        ctx.response(GetFileStatusResponse {
            status: ProtoUtils::file_status_to_pb(status),
        })
    }

    pub fn exists(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ExistsRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let exists = self.fs.exists(&header.path)?;
        let rep_header = ExistsResponse { exists };
        ctx.response(rep_header)
    }

    async fn async_exists(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ExistsRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);
        let fs = self.fs.clone();
        let exists = Self::run_master_rpc_task(
            self.metadata_read_rt.clone(),
            self.metadata_read_admission.clone(),
            move || fs.exists(&header.path),
        )
        .await?;
        ctx.response(ExistsResponse { exists })
    }

    pub fn delete0(&self, req_id: i64, header: DeleteRequest) -> FsResult<DeleteResult> {
        if self.check_is_retry(req_id)? {
            return Ok(DeleteResult::default());
        }

        let path = Path::from_str(&header.path)?;
        if let Some(info) = self.mount_manager.get_mount_info(&path)? {
            if path.path() == info.cv_path {
                return err_box!("cannot delete mount point root: {}", info.cv_path);
            }
        }

        let res = self.fs.delete(&header.path, header.recursive);
        self.set_req_cache(req_id, res)
    }

    pub fn retry_check_delete(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: DeleteRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let res = self.delete0(ctx.msg.req_id(), header)?;
        let rep_header = DeleteResponse {
            res: Some(ProtoUtils::delete_res_to_pb(res)),
        };
        ctx.response(rep_header)
    }

    pub fn retry_check_free(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: FreeRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let res = self.free0(ctx.msg.req_id(), header)?;
        ctx.response(FreeResponse {
            res: ProtoUtils::free_res_to_pb(res),
        })
    }

    pub fn free0(&self, req_id: i64, header: FreeRequest) -> FsResult<FreeResult> {
        if self.check_is_retry(req_id)? {
            return Ok(FreeResult::default());
        }

        let res = self.fs.free(&header.path, header.recursive);
        self.set_req_cache(req_id, res)
    }

    pub fn rename0(&self, req_id: i64, header: RenameRequest) -> FsResult<bool> {
        if self.check_is_retry(req_id)? {
            return Ok(true);
        }
        let flags = RenameFlags::try_new(header.flags).ok_or_else(|| {
            FsError::invalid_argument(format!("invalid rename flags: {:#x}", header.flags))
        })?;
        if !flags.is_supported() {
            return Err(FsError::unsupported(format!(
                "unsupported rename flags: {:#x}",
                header.flags
            )));
        }
        let res = self.fs.rename(&header.src, &header.dst, flags);
        self.set_req_cache(req_id, res)
    }

    pub fn retry_check_rename(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: RenameRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.src.to_string()), Some(header.dst.to_string()));

        let result = self.rename0(ctx.msg.req_id(), header)?;
        let rep_header = RenameResponse { result };
        ctx.response(rep_header)
    }

    pub fn list_status(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ListStatusRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let list = Self::process_list_status(self.fs.clone(), header.path)?;
        let res = list
            .into_iter()
            .map(ProtoUtils::file_status_to_pb)
            .collect();

        let rep_header = ListStatusResponse { statuses: res };
        ctx.response(rep_header)
    }

    fn process_list_status(fs: MasterFilesystem, path: String) -> FsResult<Vec<FileStatus>> {
        fs.list_status(&path)
    }

    async fn async_list_status(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ListStatusRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);
        let fs = self.fs.clone();
        let list = Self::run_master_rpc_task(
            self.metadata_read_rt.clone(),
            self.metadata_read_admission.clone(),
            move || Self::process_list_status(fs, header.path),
        )
        .await?;
        ctx.response(ListStatusResponse {
            statuses: list
                .into_iter()
                .map(ProtoUtils::file_status_to_pb)
                .collect(),
        })
    }

    // The add block internally determines whether it is a retry request.
    pub fn add_block(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: AddBlockRequest = ctx.parse_header()?;
        ctx.set_audit(Some(req.path.to_string()), None);

        let path = req.path;
        let client_addr = ProtoUtils::client_address_from_pb(req.client_address);
        let commit_blocks = req
            .commit_blocks
            .into_iter()
            .map(ProtoUtils::commit_block_from_pb)
            .collect();

        let last_block = req.last_block.map(ProtoUtils::extend_block_from_pb);
        let located_block = self.fs.add_block(
            path,
            req.inode_id,
            client_addr,
            commit_blocks,
            req.exclude_workers,
            req.file_len,
            last_block,
        )?;
        let rep_header = ProtoUtils::located_block_to_pb(located_block);
        ctx.response(rep_header)
    }

    // Complete_file internally determines whether it is a retry request.
    pub fn complete_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: CompleteFileRequest = ctx.parse_header()?;

        let audit_path = if req.only_flush {
            format!("flush:{}", req.path)
        } else {
            format!("close:{}", req.path)
        };
        ctx.set_audit(Some(audit_path), None);

        let return_file_blocks = req.return_file_blocks.unwrap_or(true);
        let file_blocks = self.complete_file0(req, return_file_blocks)?;
        let rep_header = CompleteFileResponse {
            result: true,
            file_blocks: file_blocks.map(ProtoUtils::file_blocks_to_pb),
        };
        ctx.response(rep_header)
    }

    fn complete_file0(
        &self,
        req: CompleteFileRequest,
        return_file_blocks: bool,
    ) -> FsResult<Option<FileBlocks>> {
        let commit_blocks = req
            .commit_blocks
            .into_iter()
            .map(ProtoUtils::commit_block_from_pb)
            .collect();
        if req.only_flush && !return_file_blocks {
            self.fs.flush_file(
                req.path,
                req.inode_id,
                req.len,
                commit_blocks,
                req.client_name,
            )?;
            Ok(None)
        } else {
            self.fs.complete_file(
                req.path,
                req.inode_id,
                req.len,
                commit_blocks,
                req.client_name,
                req.only_flush,
                req.set_attr_opts.map(ProtoUtils::set_attr_opts_from_pb),
            )
        }
    }

    pub fn create_files_batch(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: CreateFilesBatchRequest = ctx.parse_header()?;

        let mut results = Vec::with_capacity(header.requests.len());
        for (index, req) in header.requests.into_iter().enumerate() {
            let opts = ProtoUtils::create_opts_from_pb(req.opts);
            let flags = OpenFlags::new(req.flags);

            // Generate unique req_id for each file in batch
            let unique_req_id = ctx.msg.req_id() + index as i64;
            let status = self.create_file0(unique_req_id, req.path, opts, flags)?;
            results.push(status);
        }

        let rep_header = CreateFilesBatchResponse {
            file_statuses: results
                .into_iter()
                .map(ProtoUtils::file_status_to_pb)
                .collect(),
        };
        ctx.response(rep_header)
    }

    pub fn add_blocks_batch(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: AddBlocksBatchRequest = ctx.parse_header()?;
        let mut results = Vec::with_capacity(header.requests.len());
        for req in header.requests {
            let path = req.path;
            let client_addr = ProtoUtils::client_address_from_pb(req.client_address);
            let commit_blocks = req
                .commit_blocks
                .into_iter()
                .map(ProtoUtils::commit_block_from_pb)
                .collect();

            let last_block = req.last_block.map(ProtoUtils::extend_block_from_pb);
            let located_block = self.fs.add_block(
                path,
                req.inode_id,
                client_addr,
                commit_blocks,
                req.exclude_workers,
                req.file_len,
                last_block,
            )?;
            results.push(ProtoUtils::located_block_to_pb(located_block));
        }

        let rep_header = AddBlocksBatchResponse { blocks: results };
        ctx.response(rep_header)
    }

    pub fn complete_files_batch(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: CompleteFilesBatchRequest = ctx.parse_header()?;

        let mut results = Vec::with_capacity(header.requests.len());
        for req in header.requests {
            let result = self.complete_file0(req, false).is_ok();
            results.push(result);
        }

        let rep_header = CompleteFilesBatchResponse { results };
        ctx.response(rep_header)
    }

    pub fn get_block_locations(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: GetBlockLocationsRequest = ctx.parse_header()?;
        ctx.set_audit(Some(req.path.to_string()), None);

        let blocks = Self::process_get_block_locations(self.fs.clone(), req.path)?;
        let rep_header = GetBlockLocationsResponse {
            blocks: ProtoUtils::file_blocks_to_pb(blocks),
        };
        ctx.response(rep_header)
    }

    fn process_get_block_locations(fs: MasterFilesystem, path: String) -> FsResult<FileBlocks> {
        fs.get_block_locations(path)
    }

    async fn async_get_block_locations(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: GetBlockLocationsRequest = ctx.parse_header()?;
        ctx.set_audit(Some(req.path.to_string()), None);
        let fs = self.fs.clone();
        let blocks = Self::run_master_rpc_task(
            self.metadata_read_rt.clone(),
            self.metadata_read_admission.clone(),
            move || Self::process_get_block_locations(fs, req.path),
        )
        .await?;
        let rep_header = GetBlockLocationsResponse {
            blocks: ProtoUtils::file_blocks_to_pb(blocks),
        };
        ctx.response(rep_header)
    }

    async fn async_open_file_read(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: OpenFileRequest = ctx.parse_header()?;
        let flags = OpenFlags::new(header.flags);
        if !flags.read_only() {
            return err_box!("mutable OpenFile request was dispatched as a metadata read");
        }
        let audit_path = format!("{}:{}", flags.access_mark(), header.path);
        ctx.set_audit(Some(audit_path), None);
        let fs = self.fs.clone();
        let blocks = Self::run_master_rpc_task(
            self.metadata_read_rt.clone(),
            self.metadata_read_admission.clone(),
            move || Self::process_get_block_locations(fs, header.path),
        )
        .await?;
        ctx.response(OpenFileResponse {
            file_blocks: ProtoUtils::file_blocks_to_pb(blocks),
        })
    }

    async fn run_master_rpc_task<T, F>(
        rt: Arc<Runtime>,
        admission: Arc<Semaphore>,
        task: F,
    ) -> FsResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> FsResult<T> + Send + 'static,
    {
        let permit = admission
            .acquire_owned()
            .await
            .map_err(|_| FsError::common("master RPC executor has stopped"))?;
        rt.spawn_blocking(move || {
            let _permit = permit;
            panic::catch_unwind(AssertUnwindSafe(task))
                .unwrap_or_else(|_| err_box!("master RPC task panicked"))
        })
        .await
        .map_err(|error| FsError::common(format!("master RPC executor stopped: {error}")))?
    }

    async fn async_get_filesystem_info(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: GetFilesystemInfoRequest = ctx.parse_header()?;
        // Only evaluate the compatibility policy when the result can actually
        // be acted upon; a legacy client would otherwise hit a MissingInfo
        // verdict and log a warning on every statfs call.
        if self
            .compatibility_policy
            .should_evaluate(req.component_info.is_some())
        {
            Self::check_peer_compatibility(
                "client",
                &format!("client:{}", self.client_ip()),
                &self.compat_warned,
                self.compatibility_policy.mode,
                self.compatibility_policy
                    .check_client(req.component_info.as_ref()),
                self.metrics,
            )?;
        }
        let fs = self.fs.clone();
        let info = Self::run_master_rpc_task(
            self.control_rpc_rt.clone(),
            self.control_rpc_admission.clone(),
            move || Self::process_get_filesystem_info(fs),
        )
        .await?;
        let rep_header = Self::build_filesystem_info_response(info, &self.master_compatibility);
        ctx.response(rep_header)
    }

    /// Build the GetFilesystemInfo response, attaching the master's own version
    /// and the default (lenient) compatibility contract on the reserved 1000+
    /// field range. Legacy clients that do not know the field simply skip it.
    fn build_filesystem_info_response(
        info: FilesystemInfo,
        master_compatibility: &ServerCompatibilityInfoProto,
    ) -> GetFilesystemInfoResponse {
        let mut rep_header = ProtoUtils::filesystem_info_to_pb(info);
        rep_header.compatibility = Some(master_compatibility.clone());
        rep_header
    }

    async fn async_get_cv_metadata_snapshot_page(
        &self,
        ctx: &mut RpcContext<'_>,
    ) -> FsResult<Message> {
        let req: GetCvMetadataSnapshotPageRequest = ctx.parse_header()?;
        ctx.set_audit(Some("cv-metadata-snapshot".to_string()), None);
        let fs = self.fs.clone();
        let response = Self::run_master_rpc_task(
            self.metadata_read_rt.clone(),
            self.metadata_read_admission.clone(),
            move || {
                let page = fs.cv_metadata_snapshot_page(
                    req.page_token,
                    req.page_size.unwrap_or(10_000) as usize,
                )?;
                Ok(GetCvMetadataSnapshotPageResponse {
                    entries: page
                        .entries
                        .into_iter()
                        .map(|entry| CvMetadataSnapshotEntryProto {
                            status: ProtoUtils::file_status_to_pb(entry.status),
                            blocks: entry.blocks.map(ProtoUtils::file_blocks_to_pb),
                        })
                        .collect(),
                    next_page_token: page.next_page_token,
                    epoch: page.epoch,
                })
            },
        )
        .await?;
        ctx.response(response)
    }

    async fn async_get_cv_metadata_delta_page(
        &self,
        ctx: &mut RpcContext<'_>,
    ) -> FsResult<Message> {
        let req: GetCvMetadataDeltaPageRequest = ctx.parse_header()?;
        ctx.set_audit(Some("cv-metadata-delta".to_string()), None);
        let fs = self.fs.clone();
        let response = Self::run_master_rpc_task(
            self.metadata_read_rt.clone(),
            self.metadata_read_admission.clone(),
            move || {
                let page = fs.cv_metadata_delta_page(
                    req.from_epoch,
                    req.target_epoch,
                    req.page_token,
                    req.page_size.unwrap_or(10_000) as usize,
                )?;
                Ok(GetCvMetadataDeltaPageResponse {
                    entries: page
                        .entries
                        .into_iter()
                        .map(|entry| CvMetadataDeltaEntryProto {
                            path: entry.path,
                            entry: entry.entry.map(|entry| CvMetadataSnapshotEntryProto {
                                status: ProtoUtils::file_status_to_pb(entry.status),
                                blocks: entry.blocks.map(ProtoUtils::file_blocks_to_pb),
                            }),
                        })
                        .collect(),
                    next_page_token: page.next_page_token,
                    from_epoch: page.from_epoch,
                    to_epoch: page.to_epoch,
                    full_snapshot_required: page.full_snapshot_required,
                })
            },
        )
        .await?;
        ctx.response(response)
    }

    fn process_get_filesystem_info(fs: MasterFilesystem) -> FsResult<FilesystemInfo> {
        fs.filesystem_info()
    }

    /// Evaluate a compatibility verdict against the configured mode.
    ///
    /// - `diagnose` (default): log a warning for non-compatible peers and
    ///   allow the request, so old components are never rejected without
    ///   explicit configuration.
    /// - `enforce`: reject with an explicit error describing the actual peer
    ///   version, the expected bound and the upgrade suggestion.
    ///
    /// Diagnose-mode warnings are deduped per peer: a persistently
    /// incompatible or legacy peer (heartbeats run every few seconds, statfs
    /// every call) warns on the first occurrence and again only when its
    /// verdict changes, so repeated identical warnings do not flood
    /// operational logs.
    fn check_peer_compatibility(
        peer: &str,
        dedup_key: &str,
        warned: &DashMap<String, CompatibilityVerdict>,
        mode: CompatibilityMode,
        verdict: CompatibilityVerdict,
        metrics: &MasterMetrics,
    ) -> FsResult<()> {
        // Record the compatibility verdict as a metric. Only one verdict
        // label per peer is active at a time, so set the current label to 1
        // and clear every other label for the peer; otherwise a peer whose
        // verdict changes over time leaves stale series stuck at 1.
        let verdict_label = Self::verdict_label(&verdict);
        let is_worker = dedup_key.starts_with("worker:");
        for label in Self::VERDICT_LABELS {
            let active = if label == verdict_label { 1 } else { 0 };
            if is_worker {
                let worker_id = &dedup_key["worker:".len()..];
                metrics
                    .compat_worker_verdict
                    .with_label_values(&[worker_id, label])
                    .set(active);
            } else {
                metrics
                    .compat_client_verdict
                    .with_label_values(&[dedup_key, label])
                    .set(active);
            }
        }

        if !verdict.rejects(mode) {
            if !verdict.is_compatible() {
                // Warn on the first occurrence and whenever the verdict
                // changes for this peer; suppress identical repeats.
                let changed = warned
                    .get(dedup_key)
                    .map(|last| *last != verdict)
                    .unwrap_or(true);
                if changed {
                    warned.insert(dedup_key.to_string(), verdict.clone());
                    log::warn!("{} compatibility: {}", peer, verdict.describe());
                }
            } else {
                // The peer is compatible again; forget the previous warning so
                // a future incompatibility is surfaced.
                warned.remove(dedup_key);
            }
            return Ok(());
        }
        // Enforce-mode rejection: record the counter and return the error.
        metrics
            .compat_enforce_rejected_total
            .with_label_values(&[peer, verdict_label])
            .inc();
        err_box!(
            "{} rejected by compatibility policy: {}; upgrade the {} or set master.compatibility.mode = \"diagnose\" to allow it",
            peer,
            verdict.describe(),
            peer
        )
    }

    /// All verdict label values for the compat_*_verdict gauge vectors, kept
    /// in sync with [`Self::verdict_label`]. Only one label per peer is active
    /// at a time: recording a verdict sets the current label to 1 and clears
    /// every other label for that peer.
    const VERDICT_LABELS: [&str; 6] = [
        "compatible",
        "missing_info",
        "blocked",
        "protocol_mismatch",
        "version_too_old",
        "version_unknown",
    ];

    /// Short human-readable label for a compatibility verdict, used as a
    /// Prometheus label value in compat_*_verdict and compat_enforce_rejected_total.
    fn verdict_label(verdict: &CompatibilityVerdict) -> &'static str {
        match verdict {
            CompatibilityVerdict::Compatible => "compatible",
            CompatibilityVerdict::MissingInfo => "missing_info",
            CompatibilityVerdict::Blocked(_) => "blocked",
            CompatibilityVerdict::ProtocolMismatch { .. } => "protocol_mismatch",
            CompatibilityVerdict::VersionTooOld { .. } => "version_too_old",
            CompatibilityVerdict::VersionUnknown { .. } => "version_unknown",
        }
    }


    pub fn worker_heartbeat(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: WorkerHeartbeatRequest = ctx.parse_header()?;
        // Evaluate the compatibility policy only when the result can actually
        // be acted upon; a legacy worker would otherwise hit a MissingInfo
        // verdict and log a warning on every heartbeat.
        if self
            .compatibility_policy
            .should_evaluate(header.component_info.is_some())
        {
            Self::check_peer_compatibility(
                "worker",
                &format!("worker:{}", header.worker_id),
                &self.compat_warned,
                self.compatibility_policy.mode,
                self.compatibility_policy
                    .check_worker(header.component_info.as_ref()),
                self.metrics,
            )?;
        }
        let cmds = Self::process_worker_heartbeat(self.fs.clone(), header)?;
        let rep_header = WorkerHeartbeatResponse {
            cmds: ProtoUtils::worker_cmd_to_pb(cmds),
        };
        ctx.response(rep_header)
    }

    fn process_worker_heartbeat(
        fs: MasterFilesystem,
        header: WorkerHeartbeatRequest,
    ) -> FsResult<Vec<WorkerCommand>> {
        let status = HeartbeatStatus::from(header.status);
        let address = ProtoUtils::worker_address_from_pb(&header.address);
        // Worker weight comes from trusted administrator configuration. Preserve the
        // configured u32 value so the master does not silently alter allocation ratios.
        let weight = header.weight.unwrap_or_else(WorkerInfo::default_weight);
        if matches!(status, HeartbeatStatus::Start) {
            fs.reset_full_block_report(address.worker_id);
        }

        let mut wm = fs.worker_manager.write();
        let cmds = wm.heartbeat(
            &header.cluster_id,
            status,
            address,
            weight,
            header.worker_session_id.unwrap_or_default(),
            curvine_model::TransferWorkerCapabilities {
                task_submit: header.transfer_task_submit.unwrap_or(false),
                report_target: header.transfer_report_target.unwrap_or(false),
                query_task: header.transfer_query_task.unwrap_or(false),
                attempt_safe_output: header.transfer_attempt_safe_output.unwrap_or(false),
                source_read_plan: header.transfer_source_read_plan.unwrap_or(false),
            },
            header.software_version,
            u64::try_from(header.fs_ctime).unwrap_or_default(),
            ProtoUtils::storage_info_list_from_pb(header.storages),
            header.component_info,
        )?;
        Ok(cmds)
    }

    pub fn block_report(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: BlockReportListRequest = ctx.parse_header()?;
        let cmds =
            Self::process_block_report(self.fs.clone(), self.replication_handler.clone(), header)?;
        let rep_header = BlockReportListResponse {
            cmds: ProtoUtils::worker_cmd_to_pb(cmds),
        };
        ctx.response(rep_header)
    }

    fn process_block_report(
        fs: MasterFilesystem,
        replication_handler: Option<MasterReplicationHandler>,
        header: BlockReportListRequest,
    ) -> FsResult<Vec<WorkerCommand>> {
        let list = ProtoUtils::block_report_list_from_pb(header);
        let result = fs.block_report(list, replication_handler)?;

        if result.delete_blocks.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![WorkerCommand::DeleteBlock(DeleteBlockCmd {
                blocks: result.delete_blocks,
            })])
        }
    }

    fn client_ip(&self) -> &str {
        match &self.conn_state {
            None => "",
            Some(v) => &v.remote_addr.hostname,
        }
    }

    fn ensure_active(&self, code: RpcCode) -> FsResult<()> {
        if self.fs.master_monitor.is_active() {
            Ok(())
        } else {
            Err(FsError::not_leader_master(code, self.client_ip()))
        }
    }

    fn is_read_only_open(msg: &Message) -> bool {
        msg.parse_header::<OpenFileRequest>()
            .map(|header| OpenFlags::new(header.flags).read_only())
            .unwrap_or(false)
    }

    fn requires_active_after_response(msg: &Message) -> bool {
        match RpcCode::from(msg.code()) {
            RpcCode::OpenFile => Self::is_read_only_open(msg),
            RpcCode::FileStatus
            | RpcCode::Exists
            | RpcCode::ListStatus
            | RpcCode::ListOptions
            | RpcCode::GetBlockLocations
            | RpcCode::GetLock
            | RpcCode::GetMountTable
            | RpcCode::GetMountInfo
            | RpcCode::GetJobStatus
            | RpcCode::GetFilesystemInfo
            | RpcCode::GetCvMetadataSnapshotPage
            | RpcCode::GetCvMetadataDeltaPage => true,
            _ => false,
        }
    }

    pub fn clone_fs(&self) -> MasterFilesystem {
        self.fs.clone()
    }

    fn mount(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let request: MountRequest = ctx.parse_header()?;
        let mnt_opt = ProtoUtils::mount_options_from_pb(request.mount_options);

        ctx.set_audit(
            Some(request.cv_path.to_string()),
            Some(request.ufs_path.to_string()),
        );

        self.mount_manager
            .mount(None, &request.cv_path, &request.ufs_path, &mnt_opt)?;
        let rep_header = MountResponse::default();
        ctx.response(rep_header)
    }

    fn umount(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let request: UnMountRequest = ctx.parse_header()?;
        ctx.set_audit(Some(request.cv_path.to_string()), None);

        self.mount_manager.umount(&request.cv_path)?;
        let rep_header = UnMountResponse::default();
        ctx.response(rep_header)
    }

    fn get_mount_info(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let request: GetMountInfoRequest = ctx.parse_header()?;
        ctx.set_audit(Some(request.path.to_string()), None);

        let path = Path::from_str(request.path)?;
        let ret = self.mount_manager.get_mount_info(&path)?;
        let rep_header = GetMountInfoResponse {
            mount_info: ret.map(ProtoUtils::mount_info_to_pb),
        };
        ctx.response(rep_header)
    }

    fn get_mount_table(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let _: GetMountTableRequest = ctx.parse_header()?;
        let table = self.mount_manager.get_mount_table()?;

        let mount_table: Vec<MountInfoProto> = table
            .into_iter()
            .map(ProtoUtils::mount_info_to_pb)
            .collect();
        let rep_header = GetMountTableResponse { mount_table };
        ctx.response(rep_header)
    }

    fn set_attr_retry_check(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        if self.check_is_retry(ctx.msg.req_id())? {
            return ctx.response(SetAttrResponse::default());
        }

        let header: SetAttrRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let opts = ProtoUtils::set_attr_opts_from_pb(header.opts);
        let status = self.fs.set_attr(header.path, opts)?;

        ctx.response(SetAttrResponse {
            status: ProtoUtils::file_status_to_pb(status),
        })
    }

    fn symlink_retry_check(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: SymlinkRequest = ctx.parse_header()?;
        ctx.set_audit(
            Some(header.target.to_string()),
            Some(header.link.to_string()),
        );

        if self.check_is_retry(ctx.msg.req_id())? {
            return ctx.response(SymlinkResponse::default());
        }

        self.fs.symlink_with_owner_group(
            &header.target,
            &header.link,
            header.force,
            header.mode,
            header.owner,
            header.group,
        )?;

        ctx.response(SymlinkResponse::default())
    }

    fn metrics_report(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: MetricsReportRequest = ctx.parse_header()?;

        let metrics = ProtoUtils::metrics_report_from_pb(header.metrics);
        Master::get_metrics()?.metrics_report(metrics)?;

        ctx.response(MetricsReportResponse {})
    }

    fn link_retry_check(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: LinkRequest = ctx.parse_header()?;
        ctx.set_audit(
            Some(header.src_path.to_string()),
            Some(header.dst_path.to_string()),
        );

        if self.check_is_retry(ctx.msg.req_id())? {
            return ctx.response(LinkResponse::default());
        }

        self.fs.link(&header.src_path, &header.dst_path)?;

        ctx.response(LinkResponse::default())
    }

    pub fn resize_file(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: FileResizeRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let file_blocks = self.fs.resize(
            &header.path,
            ProtoUtils::file_alloc_opts_from_pb(header.opts),
        )?;
        let rep_header = FileResizeResponse {
            file_blocks: ProtoUtils::file_blocks_to_pb(file_blocks),
        };
        ctx.response(rep_header)
    }

    pub fn assign_worker(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: AssignWorkerRequest = ctx.parse_header()?;
        ctx.set_audit(Some(header.path.to_string()), None);

        let block = self.fs.assign_worker(
            &header.path,
            ProtoUtils::extend_block_from_pb(header.block),
            ProtoUtils::client_address_from_pb(header.client_address),
            header.exclude_workers,
        )?;
        let rep_header = AssignWorkerResponse {
            block: ProtoUtils::located_block_to_pb(block),
        };
        ctx.response(rep_header)
    }

    pub fn get_lock(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: GetLockRequest = ctx.parse_header()?;
        let lock = ProtoUtils::file_lock_from_pb(header.lock);
        ctx.set_audit(Some(header.path.to_string()), None);

        let conflict = Self::process_get_lock(self.fs.clone(), header.path, lock)?;
        let rep_header = GetLockResponse {
            conflict: conflict.map(ProtoUtils::file_lock_to_pb),
        };
        ctx.response(rep_header)
    }

    fn process_get_lock(
        fs: MasterFilesystem,
        path: String,
        lock: FileLock,
    ) -> FsResult<Option<FileLock>> {
        fs.get_lock(path, lock)
    }

    async fn async_get_lock(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: GetLockRequest = ctx.parse_header()?;
        let lock = ProtoUtils::file_lock_from_pb(header.lock);
        ctx.set_audit(Some(header.path.to_string()), None);
        let fs = self.fs.clone();
        let conflict = Self::run_master_rpc_task(
            self.metadata_read_rt.clone(),
            self.metadata_read_admission.clone(),
            move || Self::process_get_lock(fs, header.path, lock),
        )
        .await?;
        ctx.response(GetLockResponse {
            conflict: conflict.map(ProtoUtils::file_lock_to_pb),
        })
    }

    pub fn set_lock(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: SetLockRequest = ctx.parse_header()?;
        let lock = ProtoUtils::file_lock_from_pb(header.lock);

        let audit = format!(
            "[{:?}-{:?}]{}",
            lock.lock_flags, lock.lock_type, header.path
        );
        ctx.set_audit(Some(audit), None);

        let conflict = self.fs.set_lock(header.path, lock)?;
        let rep_header = SetLockResponse {
            conflict: conflict.map(ProtoUtils::file_lock_to_pb),
        };
        ctx.response(rep_header)
    }

    pub fn list_options(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ListOptionsRequest = ctx.parse_header()?;
        if header.options.limit.unwrap_or(0) < 0 {
            return err_box!("list options limit must be greater than 0");
        }
        let opts = ProtoUtils::list_options_from_pb(header.options);
        let audit_path = format!("{}[{}]", header.path, opts);
        ctx.set_audit(Some(audit_path), None);

        let list = Self::process_list_options(self.fs.clone(), header.path, opts)?;
        let res = list
            .into_iter()
            .map(ProtoUtils::file_status_to_pb)
            .collect();
        let rep_header = ListOptionsResponse { statuses: res };
        ctx.response(rep_header)
    }

    fn process_list_options(
        fs: MasterFilesystem,
        path: String,
        opts: ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        fs.list_options(&path, opts)
    }

    async fn async_list_options(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let header: ListOptionsRequest = ctx.parse_header()?;
        if header.options.limit.unwrap_or(0) < 0 {
            return err_box!("list options limit must be greater than 0");
        }
        let opts = ProtoUtils::list_options_from_pb(header.options);
        let audit_path = format!("{}[{}]", header.path, opts);
        ctx.set_audit(Some(audit_path), None);
        let fs = self.fs.clone();
        let list = Self::run_master_rpc_task(
            self.metadata_read_rt.clone(),
            self.metadata_read_admission.clone(),
            move || Self::process_list_options(fs, header.path, opts),
        )
        .await?;
        ctx.response(ListOptionsResponse {
            statuses: list
                .into_iter()
                .map(ProtoUtils::file_status_to_pb)
                .collect(),
        })
    }

    fn record_rpc_observability(&self, ctx: &RpcContext<'_>, response: &FsResult<Message>) {
        let used_us = ctx.spent.used_us();
        if self.audit_logging_enabled {
            ctx.audit_log(response, used_us, self.conn_state.as_ref());
        }

        let code_label = format!("{:?}", ctx.code);
        self.metrics.rpc_request_total_time.inc_by(used_us as i64);
        self.metrics.rpc_request_total_count.inc();

        if ctx.code != RpcCode::WorkerHeartbeat {
            self.metrics
                .operation_duration
                .with_label_values(&[&code_label])
                .observe(used_us as f64);
        };
    }
}

impl Drop for MasterHandler {
    fn drop(&mut self) {
        self.metrics.active_connections.dec();
    }
}

impl MessageHandler for MasterHandler {
    type Error = FsError;

    fn is_sync(&self, msg: &Message) -> bool {
        let code = RpcCode::from(msg.code());
        if code == RpcCode::OpenFile {
            return msg
                .parse_header::<OpenFileRequest>()
                .map(|header| !OpenFlags::new(header.flags).read_only())
                .unwrap_or(false);
        }
        !matches!(
            code,
            RpcCode::SubmitJob
                | RpcCode::GetJobStatus
                | RpcCode::CancelJob
                | RpcCode::ReportTask
                | RpcCode::FileStatus
                | RpcCode::Exists
                | RpcCode::ListStatus
                | RpcCode::ListOptions
                | RpcCode::GetBlockLocations
                | RpcCode::GetLock
                | RpcCode::GetFilesystemInfo
                | RpcCode::GetCvMetadataSnapshotPage
                | RpcCode::GetCvMetadataDeltaPage
        )
    }

    fn handle(&self, msg: &Message) -> FsResult<Message> {
        crate::fault_point! {
            sync,
            name: "master.rpc.before_sync_dispatch",
            description: "Before a synchronous Master RPC is dispatched to its business handler",
            context: {
                "req_id" => msg.req_id(),
                "rpc_code" => msg.code() as i32,
            },
            return_error: |fault| Ok(msg.error_ext(&FsError::common(fault.message))),
        }

        let mut rpc_context = RpcContext::new(msg);
        let ctx = &mut rpc_context;
        let code = RpcCode::from(msg.code());

        // Unified processing of all RPC requests (standby NotLeader uses the same
        // observability + error_ext conversion path as async_handle).
        let response = if !self.fs.master_monitor.is_active() {
            Err(FsError::not_leader_master(ctx.code, self.client_ip()))
        } else {
            match code {
                // File system operation request
                RpcCode::Mkdir => self.mkdir(ctx),
                RpcCode::CreateFile => self.retry_check_create_file(ctx),
                RpcCode::OpenFile => self.retry_check_open_file(ctx),
                RpcCode::FileStatus => self.file_status(ctx),
                RpcCode::AddBlock => self.add_block(ctx),
                RpcCode::CompleteFile => self.complete_file(ctx),
                RpcCode::CreateFilesBatch => self.create_files_batch(ctx),
                RpcCode::AddBlocksBatch => self.add_blocks_batch(ctx),
                RpcCode::CompleteFilesBatch => self.complete_files_batch(ctx),
                RpcCode::Exists => self.exists(ctx),
                RpcCode::Delete => self.retry_check_delete(ctx),
                RpcCode::Free => self.retry_check_free(ctx),
                RpcCode::Rename => self.retry_check_rename(ctx),
                RpcCode::ListStatus => self.list_status(ctx),
                RpcCode::ListOptions => self.list_options(ctx),
                RpcCode::GetBlockLocations => self.get_block_locations(ctx),
                RpcCode::SetAttr => self.set_attr_retry_check(ctx),
                RpcCode::Symlink => self.symlink_retry_check(ctx),
                RpcCode::Link => self.link_retry_check(ctx),
                RpcCode::ResizeFile => self.resize_file(ctx),
                RpcCode::AssignWorker => self.assign_worker(ctx),
                RpcCode::GetLock => self.get_lock(ctx),
                RpcCode::SetLock => self.set_lock(ctx),

                RpcCode::Mount => self.mount(ctx),
                RpcCode::UnMount => self.umount(ctx),
                RpcCode::GetMountTable => self.get_mount_table(ctx),
                RpcCode::GetMountInfo => self.get_mount_info(ctx),

                RpcCode::MetricsReport => self.metrics_report(ctx),

                // Worker related requests
                RpcCode::WorkerHeartbeat => self.worker_heartbeat(ctx),
                RpcCode::WorkerBlockReport => self.block_report(ctx),

                RpcCode::ReportBlockReplicationResult => {
                    if let Some(ref replication_service) = self.replication_handler {
                        replication_service.handle(msg)
                    } else {
                        Err(FsError::common("Replication service not initialized"))
                    }
                }

                // Unsupported request
                _ => err_box!("Unsupported operation"),
            }
        };
        let response = if Self::requires_active_after_response(msg) {
            response.and_then(|message| {
                self.ensure_active(ctx.code)?;
                Ok(message)
            })
        } else {
            response
        };

        self.record_rpc_observability(ctx, &response);

        match response {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }

    async fn async_handle(&self, msg: Message) -> FsResult<Message> {
        crate::fault_point! {
            async,
            name: "master.rpc.before_async_dispatch",
            description: "Before an asynchronous Master RPC is dispatched to its business handler",
            context: {
                "req_id" => msg.req_id(),
                "rpc_code" => msg.code() as i32,
            },
            return_error: |fault| async {
                Ok(msg.error_ext(&FsError::common(fault.message)))
            },
        }

        let mut rpc_context = RpcContext::new(&msg);
        let ctx = &mut rpc_context;
        let code = RpcCode::from(msg.code());

        let res = if let Err(error) = self.ensure_active(ctx.code) {
            Err(error)
        } else {
            match code {
                RpcCode::SubmitJob => self.job_handler.submit_job(ctx).await,
                RpcCode::GetJobStatus => self.job_handler.get_load_status(ctx),
                RpcCode::CancelJob => self.job_handler.cancel_job(ctx).await,
                RpcCode::ReportTask => self.job_handler.task_report(ctx),
                RpcCode::OpenFile => self.async_open_file_read(ctx).await,
                RpcCode::FileStatus => self.async_file_status(ctx).await,
                RpcCode::Exists => self.async_exists(ctx).await,
                RpcCode::ListStatus => self.async_list_status(ctx).await,
                RpcCode::ListOptions => self.async_list_options(ctx).await,
                RpcCode::GetBlockLocations => self.async_get_block_locations(ctx).await,
                RpcCode::GetLock => self.async_get_lock(ctx).await,
                RpcCode::GetFilesystemInfo => self.async_get_filesystem_info(ctx).await,
                RpcCode::GetCvMetadataSnapshotPage => {
                    self.async_get_cv_metadata_snapshot_page(ctx).await
                }
                RpcCode::GetCvMetadataDeltaPage => self.async_get_cv_metadata_delta_page(ctx).await,

                v => err_box!("unsupported operation {:?}", v),
            }
        };
        let res = if Self::requires_active_after_response(&msg) {
            res.and_then(|message| {
                self.ensure_active(ctx.code)?;
                Ok(message)
            })
        } else {
            res
        };

        self.record_rpc_observability(ctx, &res);

        match res {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }

    fn get_rt(&self, msg: &Message) -> Option<&Runtime> {
        let code = RpcCode::from(msg.code());
        if matches!(
            code,
            RpcCode::WorkerHeartbeat
                | RpcCode::WorkerBlockReport
                | RpcCode::ReportBlockReplicationResult
        ) {
            Some(&self.actor_rt)
        } else {
            None
        }
    }

    fn request_admission(&self, msg: &Message) -> Option<Arc<Semaphore>> {
        let code = RpcCode::from(msg.code());
        if matches!(
            code,
            RpcCode::WorkerHeartbeat
                | RpcCode::WorkerBlockReport
                | RpcCode::ReportBlockReplicationResult
        ) {
            Some(self.control_request_admission.clone())
        } else if self.is_sync(msg) {
            self.client_blocking_admission.clone()
        } else {
            Some(self.client_request_admission.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;
    use curvine_model::WorkerAddress;
    use curvine_runtime::common::Utils;

    #[test]
    fn process_worker_heartbeat_stores_worker_report_fields() {
        Master::init_test_metrics();
        let test_name = Utils::rand_str(6);
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("master-handler-test/meta-{}", test_name));
        conf.journal.journal_dir =
            Utils::test_sub_dir(format!("master-handler-test/journal-{}", test_name));

        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        let address = WorkerAddress {
            worker_id: 7,
            hostname: "worker-host".to_string(),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: 1234,
            web_port: 5678,
        };
        let header = WorkerHeartbeatRequest {
            status: HeartbeatStatus::Running.into(),
            cluster_id: conf.cluster_id.clone(),
            address: ProtoUtils::worker_address_to_pb(&address),
            software_version: "0.1.0-test".to_string(),
            fs_ctime: 123_456,
            ..Default::default()
        };

        MasterHandler::process_worker_heartbeat(fs.clone(), header).unwrap();

        let info = fs.filesystem_info().unwrap();
        let worker = info
            .live_workers
            .iter()
            .find(|worker| worker.address.worker_id == address.worker_id)
            .unwrap();
        assert_eq!(worker.software_version, "0.1.0-test");
        assert_eq!(worker.startup_time_ms, 123_456);
    }
}
