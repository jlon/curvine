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
// WITHOUT WARRANTIES OR ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::{Path as CurvinePath, Reader, Writer};
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use lance_core::error::Result;
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreProvider, StorageOptions,
    DEFAULT_CLOUD_IO_PARALLELISM,
};
use lancedb_upstream::ObjectStoreRegistry;
use lancedb_upstream::Session;
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use url::Url;

pub const CURVINE_SCHEME: &str = "curvine";

pub const CURVINE_CONF_FILE_KEY: &str = "curvine.conf.path";

const COPY_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
struct CurvineContext {
    fs: CurvineFileSystem,
    workspace_root: CurvinePath,
}

impl std::fmt::Debug for CurvineContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurvineContext")
            .field("workspace_root", &self.workspace_root.full_path())
            .finish()
    }
}

#[derive(Clone)]
pub struct CurvineObjectStore {
    context: Arc<CurvineContext>,
}

impl std::fmt::Debug for CurvineObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurvineObjectStore")
            .field("workspace_root", &self.context.workspace_root.full_path())
            .finish()
    }
}

impl std::fmt::Display for CurvineObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CurvineObjectStore({})",
            self.context.workspace_root.full_path()
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct CurvineObjectStoreProvider;

impl CurvineObjectStoreProvider {
    pub fn new() -> Self {
        Self
    }

    fn create_context(
        &self,
        base_path: &Url,
        params: &ObjectStoreParams,
    ) -> Result<Arc<CurvineContext>> {
        let conf_path =
            resolve_curvine_conf_path(params).ok_or_else(missing_curvine_config_error)?;

        let conf = ClusterConf::from(&conf_path).map_err(|e| {
            lance_core::Error::invalid_input(format!(
                "Failed to load Curvine configuration from '{}': {e}",
                conf_path
            ))
        })?;

        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let fs = CurvineFileSystem::with_rt(conf, rt).map_err(|e| {
            lance_core::Error::invalid_input(format!(
                "Failed to initialize Curvine filesystem (config '{}'): {e}",
                conf_path
            ))
        })?;

        let workspace_root = curvine_workspace_root_from_uri(base_path).map_err(|e| {
            lance_core::Error::invalid_input(format!(
                "Invalid curvine:// workspace URI '{}': {e}",
                base_path
            ))
        })?;

        Ok(Arc::new(CurvineContext { fs, workspace_root }))
    }
}

fn resolve_curvine_conf_path(params: &ObjectStoreParams) -> Option<String> {
    params
        .storage_options()
        .and_then(|opts| opts.get(CURVINE_CONF_FILE_KEY))
        .cloned()
        .or_else(|| std::env::var(ClusterConf::ENV_CONF_FILE).ok())
}

fn missing_curvine_config_error() -> lance_core::Error {
    lance_core::Error::invalid_input(format!(
        "Missing Curvine cluster configuration: set storage option `{CURVINE_CONF_FILE_KEY}` \
         (highest priority) or environment variable `{}` to the Curvine client configuration file path.",
        ClusterConf::ENV_CONF_FILE
    ))
}

#[async_trait]
impl ObjectStoreProvider for CurvineObjectStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let context = self.create_context(&base_path, params)?;
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());
        let download_retry_count = storage_options.download_retry_count();

        Ok(ObjectStore::new(
            Arc::new(CurvineObjectStore { context }),
            base_path,
            params.block_size,
            None,
            params.use_constant_size_upload_parts,
            params.list_is_lexically_ordered.unwrap_or(false),
            DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            params.storage_options(),
        ))
    }

    fn extract_path(&self, url: &Url) -> Result<Path> {
        let trimmed = url.path().trim_start_matches('/');
        if trimmed.is_empty() {
            Ok(Path::default())
        } else {
            Path::parse(trimmed).map_err(|e| {
                lance_core::Error::invalid_input(format!(
                    "Invalid curvine object path in URL `{}`: {e}",
                    url.path()
                ))
            })
        }
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<String> {
        let host = url.host_str().unwrap_or("");
        let path = url.path().trim_end_matches('/');
        Ok(format!("curvine${host}{path}"))
    }
}

#[async_trait]
impl object_store::ObjectStore for CurvineObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if !matches!(opts.mode, object_store::PutMode::Overwrite) {
            return Err(object_store::Error::NotImplemented);
        }
        if !opts.attributes.is_empty() {
            return Err(object_store::Error::NotImplemented);
        }

        let path = self.object_path(location)?;
        let mut writer = self
            .context
            .fs
            .create(&path, true)
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;

        for chunk in payload.iter() {
            writer
                .write(chunk)
                .await
                .map_err(|e| fs_error_to_object_store(location, e))?;
        }

        writer
            .complete()
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotImplemented)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.version.is_some() {
            return Err(object_store::Error::NotImplemented);
        }

        if options.head {
            let meta = self.head(location).await?;
            options.check_preconditions(&meta)?;
            let stream =
                stream::once(async move { Ok::<Bytes, object_store::Error>(Bytes::new()) }).boxed();
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range: 0..0,
                attributes: Attributes::default(),
            });
        }

        let cv_path = self.object_path(location)?;
        let meta = self.head(location).await?;
        options.check_preconditions(&meta)?;

        let mut reader = self
            .context
            .fs
            .open(&cv_path)
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;

        let range = match options.range {
            Some(range) => {
                range
                    .as_range(meta.size)
                    .map_err(|source| object_store::Error::Generic {
                        store: CURVINE_SCHEME,
                        source: source.to_string().into(),
                    })?
            }
            None => 0..meta.size,
        };

        if range.start > 0 {
            reader
                .seek(range.start as i64)
                .await
                .map_err(|e| fs_error_to_object_store(location, e))?;
        }

        let len = (range.end - range.start) as usize;
        let mut buf = vec![0u8; len];
        let read_len = reader
            .read_full(&mut buf)
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;
        reader
            .complete()
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;
        buf.truncate(read_len);

        let stream =
            stream::once(async move { Ok::<Bytes, object_store::Error>(Bytes::from(buf)) }).boxed();

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let cv_path = self.object_path(location)?;
        let status = self
            .context
            .fs
            .get_status(&cv_path)
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;

        Ok(file_status_to_object_meta(location.clone(), status))
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let cv_path = self.object_path(location)?;
        self.context
            .fs
            .delete(&cv_path, false)
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        Box::pin(stream! {
            let metas = match store.collect_under_prefix(prefix.as_ref()).await {
                Ok(m) => m,
                Err(err) => {
                    yield Err(err);
                    return;
                }
            };
            for meta in metas {
                yield Ok(meta);
            }
        })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let root_path = match prefix {
            Some(prefix) => self.object_path(prefix)?,
            None => self.context.workspace_root.clone(),
        };

        let statuses = self
            .context
            .fs
            .list_status(&root_path)
            .await
            .map_err(|e| fs_error_to_object_store(prefix.unwrap_or(&Path::default()), e))?;

        let base_prefix = prefix.cloned().unwrap_or_default();
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        for status in statuses {
            let entry_location = relative_object_path(&self.context.workspace_root, &status.path)
                .map_err(|msg| object_store::Error::Generic {
                store: CURVINE_SCHEME,
                source: msg.into(),
            })?;

            let (first, nested) = {
                let mut parts = match entry_location.prefix_match(&base_prefix) {
                    Some(parts) => parts,
                    None => continue,
                };

                let first = match parts.next() {
                    Some(p) => p,
                    None => continue,
                };

                let nested = parts.next().is_some();
                (first, nested)
            };

            if nested {
                continue;
            }

            if status.is_dir {
                common_prefixes.insert(base_prefix.child(first));
            } else {
                objects.push(file_status_to_object_meta(entry_location, status));
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_cv = self.object_path(from)?;
        let to_cv = self.object_path(to)?;
        let meta = self.head(from).await?;
        let size = meta.size;

        let mut reader = self
            .context
            .fs
            .open(&from_cv)
            .await
            .map_err(|e| fs_error_to_object_store(from, e))?;

        let mut writer = self
            .context
            .fs
            .create(&to_cv, true)
            .await
            .map_err(|e| fs_error_to_object_store(to, e))?;

        let mut offset = 0u64;
        while offset < size {
            let take = ((size - offset).min(COPY_CHUNK_BYTES as u64)) as usize;
            if offset > 0 {
                reader
                    .seek(offset as i64)
                    .await
                    .map_err(|e| fs_error_to_object_store(from, e))?;
            }

            let mut buf = vec![0u8; take];
            let n = reader
                .read_full(&mut buf)
                .await
                .map_err(|e| fs_error_to_object_store(from, e))?;
            if n == 0 {
                break;
            }
            writer
                .write(&buf[..n])
                .await
                .map_err(|e| fs_error_to_object_store(to, e))?;
            offset += n as u64;
        }

        reader
            .complete()
            .await
            .map_err(|e| fs_error_to_object_store(from, e))?;
        writer
            .complete()
            .await
            .map_err(|e| fs_error_to_object_store(to, e))?;

        Ok(())
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotImplemented)
    }
}

impl CurvineObjectStore {
    fn object_path(&self, location: &Path) -> object_store::Result<CurvinePath> {
        let rel = location.as_ref().trim_start_matches('/');
        let base = self
            .context
            .workspace_root
            .full_path()
            .trim_end_matches('/');
        let full = if rel.is_empty() {
            base.to_string()
        } else {
            format!("{base}/{rel}")
        };

        CurvinePath::from_str(full).map_err(|e| object_store::Error::Generic {
            store: CURVINE_SCHEME,
            source: e.to_string().into(),
        })
    }

    async fn collect_under_prefix(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<Vec<ObjectMeta>> {
        let root_path = match prefix {
            Some(prefix) => self.object_path(prefix)?,
            None => self.context.workspace_root.clone(),
        };

        let mut out = Vec::new();
        self.collect_files_recursive(&root_path, &mut out).await?;
        Ok(out)
    }

    async fn collect_files_recursive(
        &self,
        dir: &CurvinePath,
        out: &mut Vec<ObjectMeta>,
    ) -> object_store::Result<()> {
        let statuses = self
            .context
            .fs
            .list_status(dir)
            .await
            .map_err(|e| fs_error_to_object_store(&Path::default(), e))?;

        for status in statuses {
            if status.is_dir {
                let child = CurvinePath::from_str(&status.path).map_err(|e| {
                    object_store::Error::Generic {
                        store: CURVINE_SCHEME,
                        source: e.to_string().into(),
                    }
                })?;
                Box::pin(self.collect_files_recursive(&child, out)).await?;
            } else {
                let path = relative_object_path(&self.context.workspace_root, &status.path)
                    .map_err(|msg| object_store::Error::Generic {
                        store: CURVINE_SCHEME,
                        source: msg.into(),
                    })?;
                out.push(file_status_to_object_meta(path, status));
            }
        }

        Ok(())
    }
}

pub fn curvine_registry() -> Arc<ObjectStoreRegistry> {
    let registry = Arc::new(ObjectStoreRegistry::default());
    registry.insert(CURVINE_SCHEME, Arc::new(CurvineObjectStoreProvider::new()));
    registry
}

pub fn curvine_session() -> Arc<Session> {
    Arc::new(Session::new(0, 0, curvine_registry()))
}

fn curvine_workspace_root_from_uri(url: &Url) -> std::result::Result<CurvinePath, String> {
    let authority = url.host_str().unwrap_or_default();
    let raw_path = url.path();
    let full = if authority.is_empty() {
        raw_path.to_string()
    } else if raw_path == "/" {
        format!("/{authority}")
    } else {
        format!("/{authority}{raw_path}")
    };

    CurvinePath::from_str(full).map_err(|e| e.to_string())
}

fn file_status_to_object_meta(
    location: Path,
    status: curvine_common::state::FileStatus,
) -> ObjectMeta {
    let secs = status.mtime.div_euclid(1000);
    let millis = status.mtime.rem_euclid(1000) as u32;
    ObjectMeta {
        location,
        last_modified: DateTime::<Utc>::from_timestamp(secs, millis * 1_000_000)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
        size: status.len as u64,
        e_tag: None,
        version: None,
    }
}

fn relative_object_path(root: &CurvinePath, full_path: &str) -> std::result::Result<Path, String> {
    let root = root.full_path().trim_end_matches('/');
    let relative = full_path
        .strip_prefix(root)
        .unwrap_or(full_path)
        .trim_start_matches('/');
    Path::parse(relative).map_err(|e| e.to_string())
}

fn fs_error_to_object_store(location: &Path, error: FsError) -> object_store::Error {
    match error {
        e @ FsError::FileNotFound(_) => object_store::Error::NotFound {
            path: location.to_string(),
            source: Box::new(e),
        },
        e => object_store::Error::Generic {
            store: CURVINE_SCHEME,
            source: e.to_string().into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectStoreProvider;
    use super::*;

    #[test]
    fn workspace_uri_three_slashes_maps_path_only() {
        let url = Url::parse("curvine:///data/lancedb/demo").unwrap();
        let p = curvine_workspace_root_from_uri(&url).unwrap();
        assert_eq!(p.full_path(), "/data/lancedb/demo");
    }

    #[test]
    fn workspace_uri_host_and_path_merge() {
        let url = Url::parse("curvine://tenant/data/db").unwrap();
        let p = curvine_workspace_root_from_uri(&url).unwrap();
        assert_eq!(p.full_path(), "/tenant/data/db");
    }

    #[test]
    fn extract_path_strips_leading_slash() {
        let provider = CurvineObjectStoreProvider::new();
        let url = Url::parse("curvine:///data/db/key.bin").unwrap();
        let path = ObjectStoreProvider::extract_path(&provider, &url).unwrap();
        assert_eq!(path.as_ref(), "data/db/key.bin");
    }
}
