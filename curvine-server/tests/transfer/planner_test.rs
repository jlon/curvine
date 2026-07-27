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

use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::{ClientConf, ClusterConf};
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::state::{
    FileBlocks, FileStatus, MountOptions, TransferJobRecord, TransferKind, TransferProgress,
    TransferState,
};
use curvine_common::FsResult;
use curvine_server::common::UfsFactory;
use curvine_server::transfer::{ClusterMetadataCache, CvMetadataReader, TransferPlanner};
use futures::future::BoxFuture;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[test]
fn export_planning_retries_on_metadata_replica_epoch_change() {
    let rt = Arc::new(AsyncRuntime::single());
    let fs = CurvineFileSystem::with_rt(ClusterConf::default(), rt.clone()).unwrap();
    let reader = Arc::new(EpochChangingReader {
        epoch: AtomicU64::new(1),
    });
    let planner = TransferPlanner::new(
        reader,
        Arc::new(UfsFactory::with_rt(&ClientConf::default(), rt.clone())),
        ClusterMetadataCache::new(fs),
        ClientConf::default(),
        10,
        1,
    );
    let planned = rt.block_on(planner.plan(&export_job())).unwrap();
    assert_eq!(planned.cv_metadata_epoch, Some(2));
    assert_eq!(planned.tasks.len(), 1);
}

#[test]
fn export_planning_uses_persisted_metadata_epoch() {
    let rt = Arc::new(AsyncRuntime::single());
    let mut job = export_job();
    job.cv_metadata_epoch = Some(1);
    let planner = export_planner(rt.clone(), Arc::new(RetainedEpochReader));
    let planned = rt.block_on(planner.plan(&job)).unwrap();
    assert_eq!(planned.cv_metadata_epoch, Some(1));
    assert_eq!(planned.tasks.len(), 1);
}

#[test]
fn export_missing_source_requeues_until_replica_covers_job_create_time() {
    let rt = Arc::new(AsyncRuntime::single());
    let planner = export_planner(rt.clone(), Arc::new(MissingSourceReader { covers: false }));
    let err = match rt.block_on(planner.plan(&export_job())) {
        Ok(_) => panic!("missing source before replica watermark should not plan successfully"),
        Err(err) => err,
    };
    assert!(
        matches!(err, FsError::InProgress(_)),
        "missing source before replica watermark should requeue planning, got {err}"
    );
}

#[test]
fn export_missing_source_fails_after_replica_covers_job_create_time() {
    let rt = Arc::new(AsyncRuntime::single());
    let planner = export_planner(rt.clone(), Arc::new(MissingSourceReader { covers: true }));
    let err = match rt.block_on(planner.plan(&export_job())) {
        Ok(_) => panic!("missing source after replica watermark should not plan successfully"),
        Err(err) => err,
    };
    assert!(
        matches!(err, FsError::FileNotFound(_)),
        "missing source after replica watermark should be real FileNotFound, got {err}"
    );
}

fn export_planner(
    rt: Arc<orpc::runtime::Runtime>,
    reader: Arc<dyn CvMetadataReader>,
) -> TransferPlanner {
    let fs = CurvineFileSystem::with_rt(ClusterConf::default(), rt.clone()).unwrap();
    TransferPlanner::new(
        reader,
        Arc::new(UfsFactory::with_rt(&ClientConf::default(), rt.clone())),
        ClusterMetadataCache::new(fs),
        ClientConf::default(),
        10,
        1,
    )
}

fn export_job() -> TransferJobRecord {
    let mount = MountOptions::builder()
        .build()
        .to_info(1, "/dst", "file:///tmp/transfer-target");
    TransferJobRecord {
        job_key: "Export:/src:file:///tmp/transfer-target".to_string(),
        job_id: "job-export-epoch".to_string(),
        run_id: 1,
        kind: TransferKind::Export,
        source_path: "/src".to_string(),
        target_path: "file:///tmp/transfer-target".to_string(),
        command_json: "{}".to_string(),
        mount_snapshot_json: serde_json::to_string(&mount).unwrap(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        cv_metadata_epoch: None,
        state: TransferState::Planning,
        owner: "owner-a".to_string(),
        lease_epoch: 1,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: "request-export-epoch".to_string(),
        submitter: "test".to_string(),
        tenant: "default".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

struct MissingSourceReader {
    covers: bool,
}

impl CvMetadataReader for MissingSourceReader {
    fn current_epoch(&self) -> FsResult<Option<u64>> {
        Ok(Some(1))
    }

    fn covers_time_ms(&self, _time_ms: i64) -> FsResult<bool> {
        Ok(self.covers)
    }

    fn get_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileStatus>> {
        Box::pin(async move { Err(FsError::file_not_found(path.full_path())) })
    }

    fn list_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<Vec<FileStatus>>> {
        Box::pin(async move { Err(FsError::file_not_found(path.full_path())) })
    }

    fn get_block_locations<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileBlocks>> {
        Box::pin(async move { Err(FsError::file_not_found(path.full_path())) })
    }
}

struct EpochChangingReader {
    epoch: AtomicU64,
}

impl EpochChangingReader {
    fn dir(path: &str) -> FileStatus {
        let mut status = FileStatus::with_name(1, path.to_string(), true);
        status.path = path.to_string();
        status
    }

    fn file(path: &str) -> FileStatus {
        let mut status = FileStatus::with_name(2, path.to_string(), false);
        status.path = path.to_string();
        status.is_complete = true;
        status.len = 4;
        status
    }
}

impl CvMetadataReader for EpochChangingReader {
    fn current_epoch(&self) -> FsResult<Option<u64>> {
        Ok(Some(self.epoch.load(Ordering::SeqCst)))
    }

    fn get_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileStatus>> {
        Box::pin(async move { Ok(Self::dir(path.path())) })
    }

    fn list_status<'a>(&'a self, _path: &'a Path) -> BoxFuture<'a, FsResult<Vec<FileStatus>>> {
        Box::pin(async move { Ok(vec![Self::file("/src/file.txt")]) })
    }

    fn get_block_locations<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileBlocks>> {
        Box::pin(async move {
            self.epoch.store(2, Ordering::SeqCst);
            Ok(FileBlocks::new(Self::file(path.path()), Vec::new()))
        })
    }
}

struct RetainedEpochReader;

impl RetainedEpochReader {
    fn dir(path: &str) -> FileStatus {
        EpochChangingReader::dir(path)
    }

    fn file(path: &str) -> FileStatus {
        EpochChangingReader::file(path)
    }

    fn require_epoch(epoch: Option<u64>) -> FsResult<()> {
        if epoch == Some(1) {
            Ok(())
        } else {
            Err(FsError::common(format!(
                "expected retained epoch 1, got {:?}",
                epoch
            )))
        }
    }
}

impl CvMetadataReader for RetainedEpochReader {
    fn current_epoch(&self) -> FsResult<Option<u64>> {
        Ok(Some(2))
    }

    fn get_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileStatus>> {
        self.get_status_at_epoch(path, None)
    }

    fn get_status_at_epoch<'a>(
        &'a self,
        path: &'a Path,
        epoch: Option<u64>,
    ) -> BoxFuture<'a, FsResult<FileStatus>> {
        Box::pin(async move {
            Self::require_epoch(epoch)?;
            Ok(Self::dir(path.path()))
        })
    }

    fn list_status<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<Vec<FileStatus>>> {
        self.list_status_at_epoch(path, None)
    }

    fn list_status_at_epoch<'a>(
        &'a self,
        _path: &'a Path,
        epoch: Option<u64>,
    ) -> BoxFuture<'a, FsResult<Vec<FileStatus>>> {
        Box::pin(async move {
            Self::require_epoch(epoch)?;
            Ok(vec![Self::file("/src/file.txt")])
        })
    }

    fn get_block_locations<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, FsResult<FileBlocks>> {
        self.get_block_locations_at_epoch(path, None)
    }

    fn get_block_locations_at_epoch<'a>(
        &'a self,
        path: &'a Path,
        epoch: Option<u64>,
    ) -> BoxFuture<'a, FsResult<FileBlocks>> {
        Box::pin(async move {
            Self::require_epoch(epoch)?;
            Ok(FileBlocks::new(Self::file(path.path()), Vec::new()))
        })
    }
}
