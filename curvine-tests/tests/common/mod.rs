// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use curvine_server::test::MiniCluster;
use curvine_tests::Testing;
use orpc::CommonResult;
use tokio::runtime::Runtime;

static MINICLUSTER_LOCK: Mutex<()> = Mutex::new(());

pub fn unique_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

pub struct RunningMinicluster {
    pub _guard: MutexGuard<'static, ()>,
    pub _testing: Testing,
    pub _cluster: Arc<MiniCluster>,
    pub conf_path: String,
}

pub fn start_minicluster() -> CommonResult<(RunningMinicluster, Runtime)> {
    let guard = MINICLUSTER_LOCK.lock().expect("poisoned minicluster lock");
    let testing = Testing::builder().default().workers(3).build()?;
    let cluster = testing.start_cluster()?;
    let conf_path = testing.active_conf_path().to_string();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| orpc::CommonError::from(e.to_string()))?;
    Ok((
        RunningMinicluster {
            _guard: guard,
            _testing: testing,
            _cluster: cluster,
            conf_path,
        },
        rt,
    ))
}

pub fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}
