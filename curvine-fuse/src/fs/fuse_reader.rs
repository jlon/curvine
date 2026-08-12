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

use crate::fs::operator::Read;
use crate::fuse_metrics::{mono_now, FuseMetrics, IO_TYPE_READ, STAGE_READER_POOL_WAIT};
use crate::session::FuseResponse;
use curvine_client::unified::UnifiedFileSystem;
use curvine_client::unified::UnifiedReader;
use curvine_config::FuseConf;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path, Reader};
use curvine_model::FileStatus;
use curvine_runtime::runtime::Runtime;
use curvine_runtime::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender};
use curvine_runtime::sync::{AsyncMutex, FastMutex};
use log::warn;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

type ReaderOpenFuture = Pin<Box<dyn Future<Output = FsResult<UnifiedReader>> + Send>>;
type ReaderOpener = Arc<dyn Fn(Path) -> ReaderOpenFuture + Send + Sync>;

#[derive(Clone)]
struct ReadFutureContext {
    path_type: &'static str,
    metrics_enabled: bool,
}

struct ReaderPoolState {
    closing: bool,
    in_flight: usize,
    reader_count: usize,
    opening_count: usize,
    expansion_failed: bool,
}

struct ReaderPool {
    available_sender: AsyncSender<UnifiedReader>,
    available_receiver: AsyncMutex<AsyncReceiver<UnifiedReader>>,
    opener: Option<ReaderOpener>,
    path: Path,
    max_readers: usize,
    read_permits: Option<Arc<Semaphore>>,
    state: FastMutex<ReaderPoolState>,
    idle: Notify,
}

struct ReadLease {
    pool: Arc<ReaderPool>,
    _permit: Option<OwnedSemaphorePermit>,
}

/// Owns a checked-out reader until it is explicitly returned to the pool.
/// If an I/O future is cancelled or unwinds, dropping this guard discards the
/// connection and balances `reader_count`; a partially consumed RPC stream is
/// never returned to another FUSE request.
struct CheckedOutReader {
    pool: Arc<ReaderPool>,
    reader: Option<UnifiedReader>,
}

impl CheckedOutReader {
    fn new(pool: Arc<ReaderPool>, reader: UnifiedReader) -> Self {
        Self {
            pool,
            reader: Some(reader),
        }
    }

    fn reader_mut(&mut self) -> &mut UnifiedReader {
        self.reader.as_mut().expect("checked-out reader is present")
    }

    fn release(mut self) -> FsResult<()> {
        let reader = self.reader.take().expect("checked-out reader is present");
        self.pool.release(reader)
    }
}

impl Drop for CheckedOutReader {
    fn drop(&mut self) {
        if let Some(reader) = self.reader.take() {
            self.pool.discard_failed_reader(reader);
        }
    }
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        self.pool.finish_read();
    }
}

impl ReaderPool {
    fn new(conf: &FuseConf, opener: Option<ReaderOpener>, reader: UnifiedReader) -> Arc<Self> {
        let max_readers = if opener.is_some() {
            conf.per_handle_read_parallel
        } else {
            1
        };
        // The legacy reader had one active worker plus `stream_channel_size`
        // queued requests. A pool has up to `max_readers` active requests, so
        // preserve that queue bound without throttling the reader slots.
        let read_permits = (conf.stream_channel_size != 0).then(|| {
            Arc::new(Semaphore::new(
                conf.stream_channel_size
                    .saturating_add(max_readers)
                    .min(Semaphore::MAX_PERMITS),
            ))
        });
        let path = reader.path().clone();
        let (available_sender, available_receiver) = AsyncChannel::new(0).split();
        available_sender
            .send_sync(reader)
            .expect("new reader pool receiver must be alive");

        Arc::new(Self {
            available_sender,
            available_receiver: AsyncMutex::new(available_receiver),
            opener,
            path,
            max_readers,
            read_permits,
            state: FastMutex::new(ReaderPoolState {
                closing: false,
                in_flight: 0,
                reader_count: 1,
                opening_count: 0,
                expansion_failed: false,
            }),
            idle: Notify::new(),
        })
    }

    async fn begin_read(self: &Arc<Self>) -> FsResult<ReadLease> {
        let permit = match &self.read_permits {
            Some(permits) => Some(
                permits
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| FsError::common("FUSE reader is closing"))?,
            ),
            None => None,
        };
        let mut state = self.state.lock();
        if state.closing {
            return Err(FsError::common("FUSE reader is closing"));
        }
        state.in_flight += 1;
        Ok(ReadLease {
            pool: self.clone(),
            _permit: permit,
        })
    }

    fn finish_read(&self) {
        let notify = {
            let mut state = self.state.lock();
            debug_assert!(state.in_flight > 0, "reader pool in-flight underflow");
            state.in_flight -= 1;
            state.closing && state.in_flight == 0
        };
        if notify {
            self.idle.notify_waiters();
        }
    }

    async fn try_take_available(&self) -> FsResult<Option<UnifiedReader>> {
        Ok(self.available_receiver.lock().await.try_recv()?)
    }

    #[cfg(test)]
    async fn acquire(&self) -> FsResult<UnifiedReader> {
        self.acquire_with_wait(false).await.0
    }

    async fn acquire_with_wait(
        &self,
        measure_wait: bool,
    ) -> (FsResult<UnifiedReader>, Option<u64>) {
        match self.try_take_available().await {
            Ok(Some(reader)) => return (Ok(reader), None),
            Ok(None) => {}
            Err(e) => return (Err(e), None),
        }

        let open_reader = {
            let mut state = self.state.lock();
            if !state.expansion_failed
                && state.reader_count + state.opening_count < self.max_readers
            {
                state.opening_count += 1;
                true
            } else {
                false
            }
        };

        if open_reader {
            let opener = self
                .opener
                .as_ref()
                .expect("reader pool can grow only with an opener")
                .clone();
            let res = opener(self.path.clone()).await;
            let mut state = self.state.lock();
            state.opening_count -= 1;
            match res {
                Ok(reader) => {
                    state.reader_count += 1;
                    if measure_wait {
                        FuseMetrics::with(|m| m.record_reader_pool_expansion("success"));
                    }
                    return (Ok(reader), None);
                }
                Err(e) => {
                    // The original reader remains valid for this open handle.
                    // An opportunistic pool expansion must not turn a request
                    // that could wait for it into a new read failure. Avoid
                    // retrying a failing open for every concurrent request;
                    // the next FUSE handle starts with a fresh pool.
                    state.expansion_failed = true;
                    if measure_wait {
                        FuseMetrics::with(|m| m.record_reader_pool_expansion("error"));
                    }
                    warn!("failed to expand FUSE reader pool for {}: {}", self.path, e);
                }
            }
        }

        let start = measure_wait.then(Instant::now);
        let result = self.available_receiver.lock().await.recv_check().await;
        let elapsed_us = start.map(|start| start.elapsed().as_micros() as u64);
        match result {
            Ok(reader) => (Ok(reader), elapsed_us),
            Err(e) => (Err(e.into()), elapsed_us),
        }
    }

    fn release(&self, reader: UnifiedReader) -> FsResult<()> {
        // The reader pool is intentionally unbounded. Returning a reader is a
        // synchronous ownership transfer, so cancellation cannot strand it
        // between checkout and the available queue.
        Ok(self.available_sender.send_sync(reader)?)
    }

    fn discard_failed_reader(&self, failed: UnifiedReader) {
        // Unlike replacement, a terminal backend timeout has no reader that
        // will occupy this pool slot. Release the accounting before dropping
        // it so later reads can open a fresh connection.
        let mut state = self.state.lock();
        debug_assert!(state.reader_count > 0);
        state.reader_count = state.reader_count.saturating_sub(1);
        drop(state);
        drop(failed);
    }

    async fn close_and_take_all(&self) -> FsResult<Vec<UnifiedReader>> {
        {
            let mut state = self.state.lock();
            if state.closing {
                return Err(FsError::common("FUSE reader is already closing"));
            }
            state.closing = true;
        }
        if let Some(permits) = &self.read_permits {
            permits.close();
        }

        loop {
            let notified = self.idle.notified();
            if self.state.lock().in_flight == 0 {
                break;
            }
            notified.await;
        }

        let reader_count = self.state.lock().reader_count;
        let mut receiver = self.available_receiver.lock().await;
        let mut readers = Vec::with_capacity(reader_count);
        for _ in 0..reader_count {
            readers.push(receiver.recv_check().await?);
        }
        Ok(readers)
    }
}

pub struct FuseReader {
    path: Path,
    len: i64,
    pool: Arc<ReaderPool>,
    status: FileStatus,
    path_type: &'static str,
    metrics_enabled: bool,
    last_read_end: AtomicI64,
}

impl FuseReader {
    pub fn new(conf: &FuseConf, _rt: Arc<Runtime>, reader: UnifiedReader) -> Self {
        Self::new_inner(conf, None, reader)
    }

    pub fn new_with_filesystem(
        conf: &FuseConf,
        _rt: Arc<Runtime>,
        fs: UnifiedFileSystem,
        reader: UnifiedReader,
    ) -> Self {
        let opener: ReaderOpener = Arc::new(move |path| {
            let fs = fs.clone();
            Box::pin(async move { fs.open(&path).await })
        });
        Self::new_inner(conf, Some(opener), reader)
    }

    fn new_inner(conf: &FuseConf, opener: Option<ReaderOpener>, reader: UnifiedReader) -> Self {
        let path = reader.path().clone();
        let len = reader.len();
        let status = reader.status().clone();
        let path_type = reader.path_type();
        let pool = ReaderPool::new(conf, opener, reader);

        Self {
            path,
            len,
            pool,
            status,
            path_type,
            metrics_enabled: conf.metrics_enabled,
            last_read_end: AtomicI64::new(-1),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    pub async fn read(&self, op: Read<'_>, reply: FuseResponse) -> FsResult<()> {
        let lease = self.pool.begin_read().await?;
        let pool = self.pool.clone();
        let context = ReadFutureContext {
            path_type: self.path_type,
            metrics_enabled: self.metrics_enabled,
        };
        let off = op.arg.offset as i64;
        let len = op.arg.size as usize;
        if context.metrics_enabled {
            FuseMetrics::get().record_read_pattern(self.read_pattern(off, len));
        }
        // The receiver owns the READ task lifetime so FUSE_INTERRUPT can
        // cancel the actual backend future. Keep the lease in this future;
        // its Drop still balances the pool on every return path.
        Self::read_future(pool, off, len, reply, context).await;
        drop(lease);
        Ok(())
    }

    fn read_pattern(&self, off: i64, len: usize) -> &'static str {
        let end = off.saturating_add(len as i64);
        match self.last_read_end.swap(end, Ordering::Relaxed) {
            previous if previous < 0 => "initial",
            previous if previous == off => "sequential",
            _ => "positioned",
        }
    }

    pub async fn complete(&self, reply: Option<FuseResponse>) -> FsResult<()> {
        let readers = self.pool.close_and_take_all().await?;
        let mut error = None;
        for mut reader in readers {
            if let Err(e) = reader.complete().await {
                error.get_or_insert(e);
            }
        }
        let result = error.map_or(Ok(()), Err);

        match reply {
            Some(reply) => {
                let result: Result<(), crate::FuseError> = result.map_err(Into::into);
                if let Err(e) = reply.send_rep(result).await {
                    warn!("failed to send FUSE reader complete reply: {}", e);
                }
                Ok(())
            }
            None => result,
        }
    }

    async fn read_future(
        pool: Arc<ReaderPool>,
        off: i64,
        len: usize,
        reply: FuseResponse,
        context: ReadFutureContext,
    ) {
        let (reader_result, pool_wait_us) = pool.acquire_with_wait(context.metrics_enabled).await;
        if context.metrics_enabled {
            if let Some(pool_wait_us) = pool_wait_us {
                FuseMetrics::get().record_stream_stage(
                    STAGE_READER_POOL_WAIT,
                    reader_result.is_ok(),
                    pool_wait_us,
                );
            }
        }

        let data = match reader_result {
            Ok(unified_reader) => {
                let mut checked_reader = CheckedOutReader::new(pool.clone(), unified_reader);
                let io_start = if context.metrics_enabled {
                    Some(mono_now())
                } else {
                    None
                };
                let inflight = FuseMetrics::stream_io_guard(context.metrics_enabled, IO_TYPE_READ);
                let data = checked_reader.reader_mut().fuse_read(off, len).await;
                drop(inflight);

                if let Some(start) = io_start {
                    let ok = data.is_ok();
                    // Actual bytes read (short reads count actual); requested
                    // size is `len` (the request-size distribution).
                    let bytes = data
                        .as_ref()
                        .map(|v| v.iter().map(|s| s.len() as u64).sum())
                        .unwrap_or(0);
                    FuseMetrics::get().record_stream_io(
                        IO_TYPE_READ,
                        context.path_type,
                        ok,
                        bytes,
                        len as u64,
                        start.elapsed().as_micros() as u64,
                    );
                }

                match checked_reader.release() {
                    Ok(()) => data,
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        };

        if let Err(e) = &data {
            warn!(
                "FUSE read failed: path={}, offset={}, len={}: {}",
                pool.path.path(),
                off,
                len,
                e
            );
        }
        if let Err(e) = reply.send_data(data.map_err(Into::into)).await {
            // Losing this reply receiver must not terminate another request.
            warn!("failed to send FUSE read reply: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fuse_metrics::{
        ActiveGuard, FuseMetrics, FuseReqCtx, FuseReqKind, FuseReqLabels, IO_TYPE_READ,
        STAGE_STREAM_IO,
    };
    use crate::raw::fuse_abi::{fuse_in_header, fuse_read_in};
    use crate::session::{FuseResponse, FuseTask};
    use curvine_client::unified::UnifiedReader;
    use curvine_fs_api::local::LocalReader;
    use curvine_metrics::Metrics as m;
    use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
    use curvine_runtime::sync::channel::AsyncChannel;
    use std::io::Write as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{timeout, Duration};

    fn metrics_reply(rt: &AsyncRuntime) -> FuseResponse {
        FuseMetrics::ensure_init().unwrap();
        let (tx, mut rx) = AsyncChannel::<FuseTask>::new(16).split();
        rt.spawn(async move { while rx.recv().await.is_some() {} });
        let gauge = m::new_gauge(
            format!("fr_it_active_{}", std::process::id()),
            "test".to_string(),
        )
        .unwrap();
        let labels = FuseReqLabels::new("Read", FuseReqKind::Stream, 64);
        let ctx = FuseReqCtx {
            labels,
            active: Some(ActiveGuard::new(gauge)),
        };
        FuseResponse::new_reply(1, tx, false, Some(ctx))
    }

    fn closed_reply(unique: u64) -> FuseResponse {
        let (tx, rx) = AsyncChannel::<FuseTask>::new(1).split();
        drop(rx);
        FuseResponse::new_reply(unique, tx, false, None)
    }

    fn read_op_arg(offset: u64, size: u32) -> fuse_read_in {
        fuse_read_in {
            fh: 0,
            offset,
            size,
            read_flags: 0,
            lock_owner: 0,
            flags: 0,
            padding: 0,
        }
    }

    #[tokio::test]
    async fn reader_pool_opens_lazily_and_stays_bounded() {
        let path_buf = std::env::temp_dir().join(format!(
            "fr_pool_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        std::fs::write(&path_buf, b"reader-pool").unwrap();
        let path = curvine_fs_api::Path::from_str(path_buf.to_str().unwrap()).unwrap();
        let open_count = Arc::new(AtomicUsize::new(0));
        let factory_path = path.clone();
        let factory_count = open_count.clone();
        let opener: ReaderOpener = Arc::new(move |_| {
            let path = factory_path.clone();
            let open_count = factory_count.clone();
            Box::pin(async move {
                open_count.fetch_add(1, Ordering::Relaxed);
                Ok(UnifiedReader::Local(LocalReader::new(&path, 4096)?))
            })
        });
        let conf = FuseConf {
            per_handle_read_parallel: 2,
            ..Default::default()
        };
        let initial = UnifiedReader::Local(LocalReader::new(&path, 4096).unwrap());
        let pool = ReaderPool::new(&conf, Some(opener), initial);

        let first = pool.acquire().await.unwrap();
        assert_eq!(
            open_count.load(Ordering::Relaxed),
            0,
            "initial reader is reused"
        );

        let second = pool.acquire().await.unwrap();
        assert_eq!(
            open_count.load(Ordering::Relaxed),
            1,
            "the second concurrent lease opens exactly one reader"
        );

        assert!(
            timeout(Duration::from_millis(20), pool.acquire())
                .await
                .is_err(),
            "pool must wait instead of opening beyond per_handle_read_parallel"
        );

        pool.release(first).unwrap();
        let third = pool.acquire().await.unwrap();
        assert_eq!(
            open_count.load(Ordering::Relaxed),
            1,
            "an available reader is reused without another open"
        );
        pool.release(second).unwrap();
        pool.release(third).unwrap();
        let _ = std::fs::remove_file(&path_buf);
    }

    #[tokio::test]
    async fn reader_pool_discards_failed_reader_and_reopens_for_next_request() {
        let path_buf = std::env::temp_dir().join(format!(
            "fr_pool_discard_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        std::fs::write(&path_buf, b"reader-pool-discard").unwrap();
        let path = curvine_fs_api::Path::from_str(path_buf.to_str().unwrap()).unwrap();
        let open_count = Arc::new(AtomicUsize::new(0));
        let factory_path = path.clone();
        let factory_count = open_count.clone();
        let opener: ReaderOpener = Arc::new(move |_| {
            let path = factory_path.clone();
            let open_count = factory_count.clone();
            Box::pin(async move {
                open_count.fetch_add(1, Ordering::Relaxed);
                Ok(UnifiedReader::Local(LocalReader::new(&path, 4096)?))
            })
        });
        let initial = UnifiedReader::Local(LocalReader::new(&path, 4096).unwrap());
        let pool = ReaderPool::new(&FuseConf::default(), Some(opener), initial);

        let failed = pool.acquire().await.unwrap();
        pool.discard_failed_reader(failed);
        let replacement = pool.acquire().await.unwrap();
        assert_eq!(
            open_count.load(Ordering::Relaxed),
            1,
            "the next request must open one fresh reader after a failed direct read"
        );
        assert_eq!(pool.state.lock().reader_count, 1);
        pool.release(replacement).unwrap();
        let _ = std::fs::remove_file(&path_buf);
    }

    #[tokio::test]
    async fn reader_pool_falls_back_when_expansion_fails() {
        let path_buf = std::env::temp_dir().join(format!(
            "fr_pool_fallback_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        std::fs::write(&path_buf, b"reader-pool-fallback").unwrap();
        let path = curvine_fs_api::Path::from_str(path_buf.to_str().unwrap()).unwrap();
        let open_count = Arc::new(AtomicUsize::new(0));
        let factory_count = open_count.clone();
        let opener: ReaderOpener = Arc::new(move |_| {
            let open_count = factory_count.clone();
            Box::pin(async move {
                open_count.fetch_add(1, Ordering::Relaxed);
                Err(FsError::common("reader pool expansion failed"))
            })
        });
        let conf = FuseConf {
            per_handle_read_parallel: 2,
            ..Default::default()
        };
        let initial = UnifiedReader::Local(LocalReader::new(&path, 4096).unwrap());
        let pool = ReaderPool::new(&conf, Some(opener), initial);

        let first = pool.acquire().await.unwrap();
        let waiting_pool = pool.clone();
        let waiting = tokio::spawn(async move { waiting_pool.acquire().await });
        for _ in 0..20 {
            if open_count.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            open_count.load(Ordering::Relaxed),
            1,
            "the waiting request first attempts one pool expansion"
        );

        pool.release(first).unwrap();
        let second = timeout(Duration::from_secs(1), waiting)
            .await
            .expect("waiting request should fall back to the initial reader")
            .unwrap()
            .unwrap();
        assert!(
            pool.state.lock().expansion_failed,
            "a failed expansion must disable retries for this handle"
        );
        pool.release(second).unwrap();
        let _ = std::fs::remove_file(&path_buf);
    }

    #[tokio::test]
    async fn reader_pool_preserves_stream_channel_backpressure() {
        let path_buf = std::env::temp_dir().join(format!(
            "fr_pool_backpressure_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        std::fs::write(&path_buf, b"reader-pool-backpressure").unwrap();
        let path = curvine_fs_api::Path::from_str(path_buf.to_str().unwrap()).unwrap();
        let conf = FuseConf {
            per_handle_read_parallel: 2,
            stream_channel_size: 1,
            ..Default::default()
        };
        let initial = UnifiedReader::Local(LocalReader::new(&path, 4096).unwrap());
        let opener: ReaderOpener = Arc::new(|_| unreachable!());
        let pool = ReaderPool::new(&conf, Some(opener), initial);

        let first = pool.begin_read().await.unwrap();
        let second = pool.begin_read().await.unwrap();
        let third = pool.begin_read().await.unwrap();

        let waiting_pool = pool.clone();
        let mut waiting = tokio::spawn(async move { waiting_pool.begin_read().await });
        assert!(
            timeout(Duration::from_millis(20), &mut waiting)
                .await
                .is_err(),
            "two active readers plus one queued request must apply backpressure"
        );

        drop(first);
        drop(second);
        drop(third);
        drop(
            timeout(Duration::from_secs(1), waiting)
                .await
                .unwrap()
                .unwrap(),
        );
        let _ = std::fs::remove_file(&path_buf);
    }

    #[tokio::test]
    async fn reader_pool_reports_wait_when_no_reader_is_available() {
        let path_buf = std::env::temp_dir().join(format!(
            "fr_pool_wait_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        std::fs::write(&path_buf, b"reader-pool").unwrap();
        let path = curvine_fs_api::Path::from_str(path_buf.to_str().unwrap()).unwrap();
        let conf = FuseConf {
            per_handle_read_parallel: 1,
            ..Default::default()
        };
        let initial = UnifiedReader::Local(LocalReader::new(&path, 4096).unwrap());
        let pool = ReaderPool::new(&conf, None, initial);

        let (first, first_wait) = pool.acquire_with_wait(true).await;
        let first = first.unwrap();
        assert!(
            first_wait.is_none(),
            "the initial reader is immediately available"
        );
        let waiting_pool = pool.clone();
        let mut waiting = tokio::spawn(async move { waiting_pool.acquire_with_wait(true).await });
        assert!(
            timeout(Duration::from_millis(20), &mut waiting)
                .await
                .is_err(),
            "the second acquire must wait until the first reader is released"
        );

        pool.release(first).unwrap();
        let (second, wait) = timeout(Duration::from_secs(1), waiting)
            .await
            .expect("waiting acquire should resume after release")
            .unwrap();
        let second = second.unwrap();
        pool.release(second).unwrap();

        assert!(
            wait.is_some(),
            "a queued acquire must report its wait duration"
        );
        let _ = std::fs::remove_file(&path_buf);
    }

    #[tokio::test]
    async fn fuse_read_records_reader_pool_wait_after_the_reader_is_released() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let before = mx
            .stage_duration_us
            .with_label_values(&[STAGE_READER_POOL_WAIT, "stream", "success"])
            .get_sample_count();

        let path_buf = std::env::temp_dir().join(format!(
            "fr_pool_wait_request_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        std::fs::write(&path_buf, vec![7u8; 4096]).unwrap();
        let path = curvine_fs_api::Path::from_str(path_buf.to_str().unwrap()).unwrap();
        let conf = FuseConf::default();
        let reader = UnifiedReader::Local(LocalReader::new(&path, 4096).unwrap());
        let pool = ReaderPool::new(&conf, None, reader);
        let held_reader = pool.acquire().await.unwrap();

        let (reply_sender, mut reply_receiver) = AsyncChannel::<FuseTask>::new(1).split();
        tokio::spawn(async move { while reply_receiver.recv().await.is_some() {} });
        let reply = FuseResponse::new_reply(99, reply_sender, false, None);
        let mut waiting_read = tokio::spawn(FuseReader::read_future(
            pool.clone(),
            0,
            4096,
            reply,
            ReadFutureContext {
                path_type: "pool_wait_test",
                metrics_enabled: true,
            },
        ));
        assert!(
            timeout(Duration::from_millis(20), &mut waiting_read)
                .await
                .is_err(),
            "the request must wait while the only reader is held"
        );

        pool.release(held_reader).unwrap();
        timeout(Duration::from_secs(1), waiting_read)
            .await
            .expect("the request should finish after the reader is released")
            .unwrap();

        assert_eq!(
            mx.stage_duration_us
                .with_label_values(&[STAGE_READER_POOL_WAIT, "stream", "success"])
                .get_sample_count(),
            before + 1,
            "a FUSE read that waited for a reader emits exactly one pool-wait sample"
        );
        let _ = std::fs::remove_file(&path_buf);
    }

    #[test]
    fn reply_send_error_does_not_kill_reader_worker() {
        let rt = AsyncRuntime::single();
        rt.block_on(async {
            let path_buf = std::env::temp_dir().join(format!(
                "fr_reply_failure_{}_{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            {
                let mut file = std::fs::File::create(&path_buf).unwrap();
                file.write_all(b"reader-worker-survives").unwrap();
            }
            let path = curvine_fs_api::Path::from_str(path_buf.to_str().unwrap()).unwrap();

            let conf = FuseConf {
                metrics_enabled: false,
                ..Default::default()
            };
            let reader = UnifiedReader::Local(LocalReader::new(&path, 4096).unwrap());
            let rt2 = Arc::new(AsyncRuntime::single());
            let fuse_reader = FuseReader::new(&conf, rt2.clone(), reader);
            std::mem::forget(rt2);

            let header = fuse_in_header::default();
            let first_arg = read_op_arg(0, 6);
            fuse_reader
                .read(
                    Read {
                        header: &header,
                        arg: &first_arg,
                    },
                    closed_reply(1),
                )
                .await
                .unwrap();

            // Submit a normal second read. Receiving its response proves that the
            // worker survived the first request's closed reply channel.
            let (reply_tx, mut reply_rx) = AsyncChannel::<FuseTask>::new(1).split();
            let second_arg = read_op_arg(7, 6);
            fuse_reader
                .read(
                    Read {
                        header: &header,
                        arg: &second_arg,
                    },
                    FuseResponse::new_reply(2, reply_tx, false, None),
                )
                .await
                .unwrap();
            fuse_reader.complete(None).await.unwrap();

            assert!(matches!(reply_rx.recv().await, Some(FuseTask::Reply(_))));
            let _ = std::fs::remove_file(&path_buf);
        });
    }

    #[test]
    fn local_reader_task_body_observes_io_with_local_path_type() {
        let rt = AsyncRuntime::single();
        rt.block_on(async {
            FuseMetrics::ensure_init().unwrap();
            let mx = FuseMetrics::get();
            let dur_before = mx
                .io_duration_us
                .with_label_values(&[IO_TYPE_READ, "local", "success"])
                .get_sample_count();
            let req_before = mx
                .io_requests_total
                .with_label_values(&[IO_TYPE_READ, "local", "success"])
                .get();
            let bytes_before = mx
                .io_bytes_total
                .with_label_values(&[IO_TYPE_READ, "local", "success"])
                .get();
            let size_before = mx
                .io_size_bytes
                .with_label_values(&[IO_TYPE_READ, "local"])
                .get_sample_count();
            let stage_before = mx
                .stage_duration_us
                .with_label_values(&[STAGE_STREAM_IO, "stream", "success"])
                .get_sample_count();

            // A temp file with 4096 bytes of content to read back.
            let path_buf = std::env::temp_dir().join(format!(
                "fr_it_read_{}_{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            {
                let mut f = std::fs::File::create(&path_buf).unwrap();
                f.write_all(&vec![7u8; 4096]).unwrap();
            }
            let path = curvine_fs_api::Path::from_str(path_buf.to_str().unwrap()).unwrap();

            let conf = FuseConf::default(); // metrics_enabled=true, unbounded channel
            let reader = UnifiedReader::Local(LocalReader::new(&path, 4096).unwrap());
            assert_eq!(reader.path_type(), "local");
            let rt2 = Arc::new(AsyncRuntime::single());
            let fuse_reader = FuseReader::new(&conf, rt2.clone(), reader);
            // Leak our Arc so this runtime is never the-last-Arc-dropped inside the
            // outer async block (dropping a tokio runtime from an async context panics).
            std::mem::forget(rt2);

            // Request 8192 bytes; the file only has 4096 (a short read).
            let reply = metrics_reply(&rt);
            let header = fuse_in_header::default();
            let arg = read_op_arg(0, 8192);
            let op = Read {
                header: &header,
                arg: &arg,
            };
            fuse_reader.read(op, reply).await.unwrap();

            let deadline = 50;
            for _ in 0..deadline {
                if mx
                    .io_duration_us
                    .with_label_values(&[IO_TYPE_READ, "local", "success"])
                    .get_sample_count()
                    > dur_before
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }

            assert_eq!(
                mx.io_duration_us
                    .with_label_values(&[IO_TYPE_READ, "local", "success"])
                    .get_sample_count(),
                dur_before + 1,
                "read task body observed io_duration_us{{read,local,success}} exactly once"
            );
            assert_eq!(
                mx.io_requests_total
                    .with_label_values(&[IO_TYPE_READ, "local", "success"])
                    .get(),
                req_before + 1,
                "io_requests_total{{read,local,success}} +1"
            );
            assert_eq!(
                mx.io_bytes_total
                    .with_label_values(&[IO_TYPE_READ, "local", "success"])
                    .get(),
                bytes_before + 4096,
                "io_bytes_total uses ACTUAL bytes read (4096, the short read), not requested 8192"
            );
            assert_eq!(
                mx.io_size_bytes
                    .with_label_values(&[IO_TYPE_READ, "local"])
                    .get_sample_count(),
                size_before + 1,
                "io_size_bytes observed once (the requested-size distribution)"
            );
            assert!(
                mx.stage_duration_us
                    .with_label_values(&[STAGE_STREAM_IO, "stream", "success"])
                    .get_sample_count()
                    > stage_before,
                "read backend call also emits stage=stream_io,kind=stream"
            );

            let _ = std::fs::remove_file(&path_buf);
        });
    }
}
