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

use crate::file::{FsContext, FsReaderBuffer, ReadDetector};
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_fs_api::{Path, Reader};
use curvine_io::DataSlice;
use curvine_model::{FileBlocks, FileStatus};
use curvine_runtime::common::ByteUnit;
use log::debug;
use std::sync::Arc;

type Inner = FsReaderBuffer;

fn should_use_exact_fuse_read_len(
    is_random: bool,
    chunk_is_empty: bool,
    remaining: usize,
    random_chunk_size: usize,
) -> bool {
    is_random && chunk_is_empty && remaining > random_chunk_size
}

pub struct FsReader {
    inner: Inner,
    chunk: DataSlice,
    chunk_size: usize,
    pos: i64,
    len: i64,
    file_blocks: FileBlocks,
    metrics: &'static crate::ClientMetrics,
}

impl FsReader {
    pub fn new(path: Path, fs_context: Arc<FsContext>, file_blocks: FileBlocks) -> FsResult<Self> {
        let chunk_size = fs_context.read_chunk_size();
        let len = file_blocks.status.len;
        let conf = &fs_context.conf.client;

        let read_detector = ReadDetector::with_conf(conf, len);

        debug!(
            "Create reader, path={}, len={}, blocks={}, chunk_size={}, chunk_number={}, read_parallel={}, slice_size={}, read_ahead={}-{}",
            &file_blocks.status.path,
            ByteUnit::byte_to_string(len as u64),
            file_blocks.block_locs.len(),
            chunk_size,
            conf.read_chunk_num,
            read_detector.read_parallel(),
            conf.read_slice_size,
            conf.enable_read_ahead,
            conf.read_ahead_len
        );

        let inner = FsReaderBuffer::new(path, fs_context, file_blocks.clone(), read_detector)?;
        let metrics = FsContext::get_metrics();
        let reader = Self {
            inner,
            chunk: DataSlice::Empty,
            chunk_size,
            pos: 0,
            len,
            file_blocks,
            metrics,
        };
        Ok(reader)
    }

    pub fn file_blocks(&self) -> &FileBlocks {
        &self.file_blocks
    }
}

impl Reader for FsReader {
    fn status(&self) -> &FileStatus {
        &self.file_blocks.status
    }

    fn path(&self) -> &Path {
        self.inner.path()
    }

    fn len(&self) -> i64 {
        self.len
    }

    fn chunk_mut(&mut self) -> &mut DataSlice {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
        self.metrics.track_read(self.inner.read()).await
    }

    async fn fuse_read(&mut self, pos: i64, len: usize) -> FsResult<Vec<DataSlice>> {
        if pos >= self.len() {
            return Ok(Vec::new());
        }

        self.seek(pos).await?;
        self.metrics
            .record_fuse_read_pattern(self.inner.fuse_read_pattern());
        let mut chunks = Vec::with_capacity(len / self.chunk_size() + 1);
        let mut remaining = len;
        while remaining > 0 {
            // A random FUSE request larger than the session chunk would
            // otherwise require several stateful ReadBlock RPCs. The
            // negotiated read_len capability returns exactly the requested
            // bytes without changing sequential prefetch behavior.
            let use_exact_len = should_use_exact_fuse_read_len(
                self.inner.is_random(),
                self.chunk.is_empty(),
                remaining,
                self.inner.random_read_chunk_size(),
            );
            let chunk = if use_exact_len && self.inner.supports_read_len().await? {
                self.metrics
                    .track_read(self.inner.read_with_len(Some(remaining)))
                    .await?
            } else {
                self.read_chunk(Some(remaining)).await?
            };
            let read_len = chunk.len();
            if read_len == 0 {
                break;
            }
            if read_len > remaining {
                return err_box!(
                    "FUSE read response {} exceeds requested remaining {}",
                    read_len,
                    remaining
                );
            }
            chunks.push(chunk);
            remaining -= read_len;
            self.pos += read_len as i64;
        }

        Ok(chunks)
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return err_box!("Cannot seek to negative offset");
        } else if self.pos == pos {
            return Ok(());
        }

        let to_skip = pos - self.pos;
        if to_skip >= 0 && to_skip <= self.chunk.len() as i64 {
            self.chunk.advance(to_skip as usize);
        } else {
            self.chunk.clear();
            self.inner.seek(pos).await?;
        }

        self.pos = pos;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.inner.complete().await
    }
}

impl Drop for FsReader {
    fn drop(&mut self) {
        debug!("Close reader, path={}", self.path())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::ClusterConf;
    use curvine_model::{ExtendedBlock, FileAllocOpts, LocatedBlock};
    use curvine_runtime::runtime::RpcRuntime;

    #[test]
    fn random_fuse_read_keeps_hole_overread_buffered() {
        let mut conf = ClusterConf::default();
        conf.client.enable_smart_prefetch = true;
        conf.client.read_chunk_size_str = "4KB".to_owned();
        conf.client.init().unwrap();

        let context = Arc::new(FsContext::new(conf).unwrap());
        let file_blocks = FileBlocks::new(
            FileStatus {
                id: 1,
                len: 4096,
                is_complete: true,
                ..Default::default()
            },
            vec![LocatedBlock {
                block: ExtendedBlock {
                    id: 1,
                    len: 4096,
                    alloc_opts: Some(FileAllocOpts::with_truncate(4096)),
                    ..Default::default()
                },
                ..Default::default()
            }],
        );
        let mut reader = FsReader::new(
            Path::from_str("/sparse").unwrap(),
            context.clone(),
            file_blocks,
        )
        .unwrap();
        let rt = context.clone_runtime();

        assert!(!should_use_exact_fuse_read_len(true, true, 4096, 4096));
        assert!(should_use_exact_fuse_read_len(true, true, 4097, 4096));
        assert!(!should_use_exact_fuse_read_len(false, true, 8192, 4096));
        assert!(!should_use_exact_fuse_read_len(true, false, 8192, 4096));

        let first = rt.block_on(reader.fuse_read(512, 7)).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].len(), 7);
        assert!(first[0].as_slice().iter().all(|byte| *byte == 0));
        assert_eq!(reader.pos(), 519);

        let second = rt.block_on(reader.fuse_read(519, 5)).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].len(), 5);
        assert_eq!(reader.pos(), 524);
    }
}
