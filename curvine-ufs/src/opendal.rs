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

use crate::OpendalConf;
use bytes::{BufMut, BytesMut};
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{FileStatus, FileType, SetAttrOpts};
use curvine_common::FsResult;
use futures::StreamExt;
use opendal::services::*;
use opendal::{
    layers::{LoggingLayer, RetryLayer, TimeoutLayer},
    Operator,
};
use orpc::sys::DataSlice;
use std::collections::HashMap;
use std::time::Duration;

/// OpenDAL Reader implementation
pub struct OpendalReader {
    operator: Operator,
    path: Path,
    object_path: String,
    length: i64,
    pos: i64,
    chunk: DataSlice,
    chunk_size: usize,
    byte_stream: Option<opendal::FuturesBytesStream>,
}

impl Reader for OpendalReader {
    fn path(&self) -> &Path {
        &self.path
    }

    fn len(&self) -> i64 {
        self.length
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
        // Initialize stream if needed
        if self.byte_stream.is_none() {
            let reader = self
                .operator
                .reader_with(&self.object_path)
                .chunk(self.chunk_size)
                .await
                .map_err(|e| FsError::common(format!("Failed to create reader: {}", e)))?;

            self.byte_stream = Some(
                reader
                    .into_bytes_stream(..self.length as u64)
                    .await
                    .map_err(|e| FsError::common(format!("Failed to create stream: {}", e)))?,
            );
        }

        if let Some(stream) = &mut self.byte_stream {
            if let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => Ok(DataSlice::Bytes(chunk)),
                    Err(e) => Err(FsError::common(format!("Failed to read chunk: {}", e))),
                }
            } else {
                Ok(DataSlice::Empty)
            }
        } else {
            Ok(DataSlice::Empty)
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 || pos > self.length {
            return Err(FsError::common("Invalid seek position"));
        }

        // If seeking backward or forward significantly, reset the stream
        if pos < self.pos || pos > self.pos + (self.chunk_size as i64 * 2) {
            self.byte_stream = None;
            self.chunk = DataSlice::Empty;

            // Create new stream starting from the seek position
            let reader = self
                .operator
                .reader_with(&self.object_path)
                .chunk(self.chunk_size)
                .await
                .map_err(|e| FsError::common(format!("Failed to create reader: {}", e)))?;

            self.byte_stream = Some(
                reader
                    .into_bytes_stream(pos as u64..self.length as u64)
                    .await
                    .map_err(|e| FsError::common(format!("Failed to create stream: {}", e)))?,
            );
        } else {
            // Skip forward in the current stream
            while self.pos < pos {
                let skip_bytes = (pos - self.pos).min(self.chunk_size as i64) as usize;
                if self.chunk.is_empty() {
                    self.chunk = self.read_chunk0().await?;
                }
                if self.chunk.is_empty() {
                    break;
                }
                let actual_skip = skip_bytes.min(self.chunk.len());
                self.chunk.advance(actual_skip);
                self.pos += actual_skip as i64;
            }
        }

        self.pos = pos;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.byte_stream = None;
        self.chunk = DataSlice::Empty;
        Ok(())
    }
}

/// OpenDAL Writer implementation
pub struct OpendalWriter {
    operator: Operator,
    path: Path,
    object_path: String,
    status: FileStatus,
    pos: i64,
    chunk: BytesMut,
    chunk_size: usize,
    writer: Option<opendal::Writer>,
    is_append: bool,
    seek_pos: i64,
    random_write_buffer: Option<BytesMut>,
}

impl OpendalWriter {
    fn convert_data_slice_to_bytes(chunk: DataSlice) -> FsResult<(bytes::Bytes, i64)> {
        match chunk {
            DataSlice::Empty => Ok((bytes::Bytes::new(), 0)),
            DataSlice::Bytes(bytes) => {
                let len = bytes.len() as i64;
                Ok((bytes, len))
            }
            DataSlice::Buffer(buf) => {
                let len = buf.len() as i64;
                Ok((buf.freeze(), len))
            }
            DataSlice::IOSlice(_) | DataSlice::MemSlice(_) => {
                let slice = chunk.as_slice();
                let len = slice.len() as i64;
                Ok((bytes::Bytes::copy_from_slice(slice), len))
            }
        }
    }

    fn write_to_random_buffer(&mut self, data: &[u8]) -> i64 {
        let buffer = self.random_write_buffer.as_mut().unwrap();
        let pos = self.seek_pos as usize;
        let len = data.len() as i64;
        let end_pos = pos + data.len();

        if buffer.len() < end_pos {
            buffer.reserve(end_pos - buffer.len());
            buffer.resize(end_pos, 0);
        }

        buffer[pos..end_pos].copy_from_slice(data);

        self.pos = end_pos as i64;
        self.seek_pos = end_pos as i64;
        len
    }
}

impl Writer for OpendalWriter {
    fn status(&self) -> &FileStatus {
        &self.status
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    fn chunk_mut(&mut self) -> &mut BytesMut {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    async fn write_chunk(&mut self, chunk: DataSlice) -> FsResult<i64> {
        let (data, len) = Self::convert_data_slice_to_bytes(chunk)?;

        if len == 0 {
            return Ok(0);
        }

        if self.is_append {
            if self.writer.is_none() {
                return Err(FsError::common("Append writer not initialized"));
            }

            let writer = self.writer.as_mut().unwrap();
            writer
                .write(data)
                .await
                .map_err(|e| FsError::common(format!("Failed to append write: {}", e)))?;

            self.pos += len;
            return Ok(len);
        }

        if self.random_write_buffer.is_some() {
            return Ok(self.write_to_random_buffer(&data));
        }

        if self.writer.is_none() {
            self.writer = Some(
                self.operator
                    .writer(&self.object_path)
                    .await
                    .map_err(|e| FsError::common(format!("Failed to create writer: {}", e)))?,
            );
        }

        let writer = self.writer.as_mut().unwrap();
        writer
            .write(data)
            .await
            .map_err(|e| FsError::common(format!("Failed to write: {}", e)))?;

        self.pos += len;
        Ok(len)
    }

    async fn flush(&mut self) -> FsResult<()> {
        self.flush_chunk().await?;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        if self.random_write_buffer.is_some() && !self.chunk.is_empty() {
            self.flush_chunk().await?;
        }

        if self.is_append {
            self.flush().await?;
            if let Some(mut writer) = self.writer.take() {
                writer.close().await.map_err(|e| {
                    FsError::common(format!("Failed to close append writer: {}", e))
                })?;
            }
            self.is_append = false;
            return Ok(());
        }

        if let Some(mut buffer) = self.random_write_buffer.take() {
            let final_len = buffer.len().max(self.pos as usize);

            if buffer.len() < final_len {
                buffer.resize(final_len, 0);
            }

            let mut writer = self.operator.writer(&self.object_path).await.map_err(|e| {
                FsError::common(format!("Failed to create writer for random write: {}", e))
            })?;

            let data = buffer.freeze();

            if !data.is_empty() {
                writer.write(data).await.map_err(|e| {
                    FsError::common(format!("Failed to write random write data: {}", e))
                })?;
            }

            writer.close().await.map_err(|e| {
                FsError::common(format!("Failed to close random write writer: {}", e))
            })?;
            return Ok(());
        }

        self.flush().await?;

        if let Some(writer) = self.writer.as_mut() {
            writer
                .close()
                .await
                .map_err(|e| FsError::common(format!("Failed to close writer: {}", e)))?;
        }

        self.writer = None;
        Ok(())
    }

    async fn cancel(&mut self) -> FsResult<()> {
        self.writer = None;
        Ok(())
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return Err(FsError::common(format!(
                "Cannot seek to negative position: {}",
                pos
            )));
        }

        if pos == self.seek_pos {
            return Ok(());
        }

        self.flush_chunk().await?;

        if self.random_write_buffer.is_none() {
            let existing_content = match self.operator.read(&self.object_path).await {
                Ok(data) => data.to_vec(),
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                    vec![]
                }
                Err(e) => {
                    return Err(FsError::common(format!(
                        "Failed to read existing file for seek: {}",
                        e
                    )));
                }
            };

            let capacity = existing_content.len().max(8 * 1024 * 1024);
            let mut buffer = BytesMut::with_capacity(capacity);

            if !existing_content.is_empty() {
                buffer.put_slice(&existing_content);
            }

            self.random_write_buffer = Some(buffer);
        }

        self.seek_pos = pos;
        self.pos = pos;
        Ok(())
    }
}

/// OpenDAL file system implementation
#[derive(Clone)]
pub struct OpendalFileSystem {
    operator: Operator,
    scheme: String,
    bucket_or_container: String,
}

impl OpendalFileSystem {
    fn add_stability_layers(
        base_op: Operator,
        conf: &HashMap<String, String>,
    ) -> FsResult<Operator> {
        let opendal_conf = OpendalConf::from_map(conf)
            .map_err(|e| FsError::common(format!("Failed to parse OpenDAL config: {}", e)))?;

        let total_timeout_ms = opendal_conf.total_timeout_ms();

        let op = base_op
            .layer(LoggingLayer::default())
            .layer(TimeoutLayer::new().with_io_timeout(Duration::from_millis(total_timeout_ms)))
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::from_millis(opendal_conf.retry_interval_ms))
                    .with_max_delay(Duration::from_millis(opendal_conf.retry_max_delay_ms))
                    .with_max_times(opendal_conf.retry_times as usize)
                    .with_factor(2.0)
                    .with_jitter(),
            );

        Ok(op)
    }

    pub fn new(path: &Path, conf: HashMap<String, String>) -> FsResult<Self> {
        let scheme = path
            .scheme()
            .ok_or_else(|| FsError::invalid_path(path.full_path(), "Missing scheme"))?;

        let bucket_or_container = path
            .authority()
            .ok_or_else(|| {
                FsError::invalid_path(path.full_path(), "URI missing bucket/container name")
            })?
            .to_string();

        let operator = match scheme {
            #[cfg(feature = "opendal-hdfs")]
            "hdfs" | "oss" => {
                use crate::jni::{register_jvm, JVM};

                register_jvm();

                let _ = JVM.get_or_init().map_err(|e| {
                    FsError::common(format!("Failed to initialize JVM for HDFS: {}", e))
                })?;

                let mut builder = Hdfs::default();

                let namenode = if let Some(namenode_config) = conf.get("hdfs.namenode") {
                    namenode_config.clone()
                } else {
                    format!("hdfs://{}", bucket_or_container)
                };

                builder = builder.name_node(&namenode);

                let root_path = conf.get("hdfs.root").map(|s| s.as_str()).unwrap_or("/");
                builder = builder.root(root_path);

                let hdfs_user = conf
                    .get("hdfs.user")
                    .cloned()
                    .or_else(|| std::env::var("HADOOP_USER_NAME").ok())
                    .or_else(|| std::env::var("USER").ok());

                if let Some(user) = hdfs_user {
                    builder = builder.user(&user);
                }

                if let Some(ccache) = conf.get("hdfs.kerberos.ccache") {
                    builder = builder.kerberos_ticket_cache_path(ccache);
                } else if let Ok(ccache) = std::env::var("KRB5CCNAME") {
                    builder = builder.kerberos_ticket_cache_path(&ccache);
                }

                if let Some(krb5_conf) = conf.get("hdfs.kerberos.krb5_conf") {
                    std::env::set_var("KRB5_CONFIG", krb5_conf);
                }

                let enable_append = conf
                    .get("hdfs.enable_append")
                    .map(|s| s == "true")
                    .unwrap_or(false);
                builder = builder.enable_append(enable_append);

                if conf
                    .get("hdfs.atomic_write_dir")
                    .map(|s| s == "true")
                    .unwrap_or(false)
                {
                    let atomic_dir = format!("{}/atomic_write_dir", root_path);
                    builder = builder.atomic_write_dir(&atomic_dir);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create HDFS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-webhdfs")]
            "webhdfs" => {
                let mut builder = Webhdfs::default();

                let endpoint = if let Some(endpoint_config) = conf.get("webhdfs.endpoint") {
                    endpoint_config.clone()
                } else {
                    format!("http://{}", bucket_or_container)
                };

                builder = builder.endpoint(&endpoint);

                let root_path = conf.get("webhdfs.root").map(|s| s.as_str()).unwrap_or("/");
                builder = builder.root(root_path);

                let atomic_dir = format!("{}/atomic_write_dir", root_path);
                builder = builder.atomic_write_dir(&atomic_dir);

                let base_op = Operator::new(builder)
                    .map_err(|e| {
                        FsError::common(format!("Failed to create WebHDFS operator: {}", e))
                    })?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-s3")]
            "s3" | "s3a" => {
                let mut builder = S3::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(endpoint) = conf.get("s3.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(region) = conf.get("s3.region_name") {
                    builder = builder.region(region);
                }
                if let Some(access_key) = conf.get("s3.credentials.access") {
                    builder = builder.access_key_id(access_key);
                }
                if let Some(secret_key) = conf.get("s3.credentials.secret") {
                    builder = builder.secret_access_key(secret_key);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create S3 operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-oss")]
            "oss" => {
                let mut builder = Oss::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(endpoint) = conf.get("oss.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(access_key) = conf.get("oss.credentials.access") {
                    builder = builder.access_key_id(access_key);
                }
                if let Some(secret_key) = conf.get("oss.credentials.secret") {
                    builder = builder.secret_access_key(secret_key);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create OSS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-gcs")]
            "gcs" | "gs" => {
                let mut builder = Gcs::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(service_account) = conf.get("gcs.service_account") {
                    builder = builder.credential(service_account);
                }
                if let Some(endpoint) = conf.get("gcs.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create GCS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-azblob")]
            "azblob" => {
                let mut builder = Azblob::default();
                builder = builder.container(&bucket_or_container);

                if let Some(account_name) = conf.get("azure.account_name") {
                    builder = builder.account_name(account_name);
                }
                if let Some(account_key) = conf.get("azure.account_key") {
                    builder = builder.account_key(account_key);
                }
                if let Some(endpoint) = conf.get("azure.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| {
                        FsError::common(format!("Failed to create Azure operator: {}", e))
                    })?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-cos")]
            "cos" => {
                let mut builder = Cos::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(endpoint) = conf.get("cos.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(access_key) = conf.get("cos.credentials.access") {
                    builder = builder.secret_id(access_key);
                }
                if let Some(secret_key) = conf.get("cos.credentials.secret") {
                    builder = builder.secret_key(secret_key);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create COS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            _ => {
                return Err(FsError::unsupported(format!(
                    "Unsupported scheme: {}",
                    scheme
                )));
            }
        };

        Ok(Self {
            operator,
            scheme: scheme.to_string(),
            bucket_or_container,
        })
    }

    fn get_object_path(&self, path: &Path) -> FsResult<String> {
        if path.is_root() {
            Ok("/".to_string())
        } else {
            let full_path = path.path();
            let trimmed = full_path.trim_start_matches('/');
            if trimmed.is_empty() {
                Ok("/".to_string())
            } else {
                Ok(trimmed.to_string())
            }
        }
    }

    async fn process_entry(
        &self,
        entry: opendal::Entry,
        parent_normalized: &str,
    ) -> Option<FileStatus> {
        let raw_path = format!(
            "{}://{}/{}",
            self.scheme,
            self.bucket_or_container,
            entry.path()
        );
        let entry_path = Path::from_str(&raw_path)
            .ok()?
            .normalize_uri()
            .unwrap_or(raw_path.clone());

        let metadata = entry.metadata();

        if metadata.is_dir() && entry_path == parent_normalized {
            log::warn!(
                "Filtered self-reference: '{}' points to parent '{}'",
                entry_path,
                parent_normalized
            );
            return None;
        }

        let (mtime, content_length) =
            if self.scheme == "hdfs" && metadata.is_dir() && metadata.last_modified().is_none() {
                self.get_hdfs_metadata(&entry)
                    .await
                    .unwrap_or((946684800000, metadata.content_length() as i64))
            } else {
                (
                    metadata
                        .last_modified()
                        .map(|t| t.timestamp_millis())
                        .unwrap_or(946684800000),
                    metadata.content_length() as i64,
                )
            };

        let name = entry.name();
        let cleaned_name = if metadata.is_dir() && name.ends_with('/') {
            &name[..name.len() - 1]
        } else {
            name
        };

        Some(FileStatus {
            path: entry_path,
            name: cleaned_name.to_owned(),
            is_dir: metadata.is_dir(),
            file_type: if metadata.is_dir() {
                FileType::Dir
            } else {
                FileType::File
            },
            mtime,
            len: content_length,
            is_complete: true,
            replicas: 1,
            block_size: 4 * 1024 * 1024,
            ..Default::default()
        })
    }

    async fn get_hdfs_metadata(&self, entry: &opendal::Entry) -> Option<(i64, i64)> {
        self.operator.stat(entry.path()).await.ok().map(|stat| {
            (
                stat.last_modified()
                    .map(|t| t.timestamp_millis())
                    .unwrap_or(946684800000),
                stat.content_length() as i64,
            )
        })
    }
}

impl FileSystem<OpendalWriter, OpendalReader> for OpendalFileSystem {
    async fn mkdir(&self, path: &Path, _create_parent: bool) -> FsResult<bool> {
        let mut object_path = self.get_object_path(path)?;
        if !object_path.ends_with('/') && !object_path.is_empty() {
            object_path.push('/');
        }

        self.operator
            .create_dir(&object_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to create directory: {}", e)))?;

        Ok(true)
    }

    async fn create(&self, path: &Path, overwrite: bool) -> FsResult<OpendalWriter> {
        let object_path = self.get_object_path(path)?;

        let file_exists = self.operator.stat(&object_path).await.is_ok();

        if overwrite && file_exists {
            let _ = self.operator.delete(&object_path).await;
        }

        if !file_exists || overwrite {
            let mut writer = self.operator.writer(&object_path).await.map_err(|e| {
                FsError::common(format!("Failed to create writer for new file: {}", e))
            })?;
            writer.close().await.map_err(|e| {
                FsError::common(format!("Failed to close writer for new file: {}", e))
            })?;
        }

        let status = match self.operator.stat(&object_path).await {
            Ok(metadata) => FileStatus {
                path: path.full_path().to_owned(),
                name: path.name().to_owned(),
                is_dir: false,
                mtime: metadata
                    .last_modified()
                    .map(|t| t.timestamp_millis())
                    .unwrap_or(0),
                is_complete: true,
                len: metadata.content_length() as i64,
                replicas: 1,
                block_size: 4 * 1024 * 1024,
                file_type: FileType::File,
                ..Default::default()
            },
            Err(_) => FileStatus {
                path: path.full_path().to_owned(),
                name: path.name().to_owned(),
                is_dir: false,
                mtime: 0,
                is_complete: false,
                len: 0,
                replicas: 1,
                block_size: 4 * 1024 * 1024,
                file_type: FileType::File,
                ..Default::default()
            },
        };

        Ok(OpendalWriter {
            operator: self.operator.clone(),
            path: path.clone(),
            object_path,
            status,
            pos: 0,
            chunk: BytesMut::with_capacity(8 * 1024 * 1024),
            chunk_size: 8 * 1024 * 1024,
            writer: None, // Lazy creation
            is_append: false,
            seek_pos: 0,
            random_write_buffer: None,
        })
    }

    async fn append(&self, path: &Path) -> FsResult<OpendalWriter> {
        let object_path = self.get_object_path(path)?;

        // Get existing file size (if file exists)
        let existing_len = match self.operator.stat(&object_path).await {
            Ok(metadata) => metadata.content_length() as i64,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => 0,
            Err(e) => {
                return Err(FsError::common(format!(
                    "Failed to stat file for append: {}",
                    e
                )));
            }
        };

        let status = FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir: false,
            mtime: 0,
            is_complete: false,
            len: existing_len,
            replicas: 1,
            block_size: 4 * 1024 * 1024,
            file_type: FileType::File,
            ..Default::default()
        };

        // Create append writer using opendal's native append API
        let writer = self
            .operator
            .writer_with(&object_path)
            .append(true) // Use native append mode
            .await
            .map_err(|e| FsError::common(format!("Failed to create append writer: {}", e)))?;

        Ok(OpendalWriter {
            operator: self.operator.clone(),
            path: path.clone(),
            object_path,
            status,
            pos: existing_len,
            chunk: BytesMut::with_capacity(8 * 1024 * 1024),
            chunk_size: 8 * 1024 * 1024,
            writer: Some(writer), // Writer is already created in append mode
            is_append: true,
            seek_pos: existing_len,
            random_write_buffer: None,
        })
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        let object_path = self.get_object_path(path)?;
        match self.operator.stat(&object_path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(FsError::common(format!("Failed to check existence: {}", e))),
        }
    }

    async fn open(&self, path: &Path) -> FsResult<OpendalReader> {
        let object_path = self.get_object_path(path)?;

        let metadata = self
            .operator
            .stat(&object_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to stat file: {}", e)))?;

        Ok(OpendalReader {
            operator: self.operator.clone(),
            path: path.clone(),
            object_path,
            length: metadata.content_length() as i64,
            pos: 0,
            chunk: DataSlice::Empty,
            chunk_size: 8 * 1024 * 1024,
            byte_stream: None,
        })
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        let src_path = self.get_object_path(src)?;
        let dst_path = self.get_object_path(dst)?;

        self.operator
            .rename(&src_path, &dst_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to rename: {}", e)))?;

        Ok(true)
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        let object_path = self.get_object_path(path)?;

        if recursive {
            // Check if it's a directory
            match self.operator.stat(&object_path).await {
                Ok(metadata) if metadata.is_dir() => self.operator.remove_all(&object_path).await,
                _ => self.operator.delete(&object_path).await,
            }
        } else {
            self.operator.delete(&object_path).await
        }
        .map_err(|e| FsError::common(format!("Failed to delete: {}", e)))?;

        Ok(())
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        let object_path = self.get_object_path(path)?;

        let metadata = match self.operator.stat(&object_path).await {
            Ok(m) => m,
            Err(e) => {
                if e.kind() == opendal::ErrorKind::NotFound {
                    return Err(FsError::file_not_found(path.full_path()));
                }
                return Err(FsError::common(format!("Failed to stat: {}", e)));
            }
        };

        Ok(FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir: metadata.is_dir(),
            mtime: metadata
                .last_modified()
                .map(|t| t.timestamp_millis())
                .unwrap_or(946684800000),
            is_complete: true,
            len: metadata.content_length() as i64,
            replicas: 1,
            block_size: 4 * 1024 * 1024,
            file_type: if metadata.is_dir() {
                FileType::Dir
            } else {
                FileType::File
            },
            ..Default::default()
        })
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        // Build object path with HDFS trailing slash (needed for proper directory listing)
        let mut object_path = self.get_object_path(path)?;
        if self.scheme == "hdfs" && !object_path.ends_with('/') && !object_path.is_empty() {
            object_path.push('/');
        }

        let list_result = self
            .operator
            .list(&object_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to list directory: {}", e)))?;

        // Get parent path for self-reference filtering
        let parent_normalized = path
            .normalize_uri()
            .unwrap_or_else(|| path.full_path().to_string());

        let mut statuses = Vec::new();
        for entry in list_result {
            if let Some(status) = self.process_entry(entry, &parent_normalized).await {
                statuses.push(status);
            }
        }

        Ok(statuses)
    }

    async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<()> {
        use tracing::debug;
        // OpenDAL doesn't support setting file attributes directly (like atime, mtime, mode, owner, group)
        // However, for compatibility with commands like `touch`, we should gracefully handle
        // attribute setting requests instead of returning an error.
        //
        // For most use cases:
        // - Time updates (atime, mtime): Ignore silently - file operations will update these naturally
        // - Mode, owner, group: Ignore silently - HDFS handles permissions differently
        // - Other attributes: Ignore silently
        //
        // This allows commands like `touch` to succeed even though we can't actually set the timestamps.
        // The file will be created/updated, and the operation will appear successful to the user.

        // Log at debug level for troubleshooting, but don't fail
        debug!(
            "set_attr called for path {} with opts: atime={:?}, mtime={:?}, mode={:?}, owner={:?}, group={:?}",
            path.full_path(),
            opts.atime,
            opts.mtime,
            opts.mode,
            opts.owner,
            opts.group
        );

        // Return success - we gracefully ignore attribute setting since OpenDAL doesn't support it
        // This allows commands like `touch` to work without errors
        Ok(())
    }
}
