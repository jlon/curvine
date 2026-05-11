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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{Path as CurvinePath, Reader, Writer};
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use lance_core::error::Result;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreProvider};
use lancedb_upstream::error::Error;
use lancedb_upstream::ObjectStoreRegistry;
use lancedb_upstream::Session;
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use url::Url;

pub const CURVINE_SCHEME: &str = "curvine";
const CURVINE_CONF_FILE_KEY: &str = "curvine.conf.path";

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
        let conf_path = params
            .storage_options()
            .and_then(|opts| opts.get(CURVINE_CONF_FILE_KEY))
            .cloned()
            .or_else(|| std::env::var(ClusterConf::ENV_CONF_FILE).ok())
            .ok_or_else(|| {
                lance_core::Error::invalid_input(format!(
                    "Missing Curvine config path. Set storage option `{CURVINE_CONF_FILE_KEY}` or env `{}`",
                    ClusterConf::ENV_CONF_FILE
                ))
            })?;

        let conf = ClusterConf::from(&conf_path)
            .map_err(|e| lance_core::Error::invalid_input(e.to_string()))?;
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let fs = CurvineFileSystem::with_rt(conf, rt)
            .map_err(|e| lance_core::Error::invalid_input(e.to_string()))?;
        let workspace_root = curvine_path_from_url(base_path)
            .map_err(|e| lance_core::Error::invalid_input(e.to_string()))?;

        Ok(Arc::new(CurvineContext { fs, workspace_root }))
    }
}

#[async_trait]
impl ObjectStoreProvider for CurvineObjectStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let context = self.create_context(&base_path, params)?;
        Ok(ObjectStore::new(
            Arc::new(CurvineObjectStore { context }),
            base_path,
            params.block_size,
            params.object_store_wrapper.clone(),
            params.use_constant_size_upload_parts,
            params.list_is_lexically_ordered.unwrap_or(true),
            lance_io::object_store::DEFAULT_CLOUD_IO_PARALLELISM,
            lance_io::object_store::DEFAULT_DOWNLOAD_RETRY_COUNT,
            params.storage_options(),
        ))
    }

    fn extract_path(&self, url: &Url) -> Result<Path> {
        let path = curvine_path_from_url(url)
            .map_err(|e| lance_core::Error::invalid_input(e.to_string()))?;
        relative_object_path(&path, path.full_path())
            .map_err(|e| lance_core::Error::invalid_input(format!("Invalid curvine path: {e}")))
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        Ok(format!("{}${}", url.scheme(), url.path()))
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
        if !matches!(opts.mode, object_store::PutMode::Overwrite) || !opts.attributes.is_empty() {
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

        let mut buf = vec![0u8; (range.end - range.start) as usize];
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
            let list = match store.list_objects(prefix.as_ref()).await {
                Ok(list) => list,
                Err(err) => {
                    yield Err(err);
                    return;
                }
            };

            for item in list {
                yield item;
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

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for status in statuses {
            let path =
                relative_object_path(&self.context.workspace_root, &status.path).map_err(|e| {
                    object_store::Error::Generic {
                        store: CURVINE_SCHEME,
                        source: e.into(),
                    }
                })?;
            if status.is_dir {
                common_prefixes.insert(path);
            } else {
                objects.push(file_status_to_object_meta(path, status));
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from = self.object_path(from)?;
        let to = self.object_path(to)?;
        self.context
            .fs
            .rename(&from, &to)
            .await
            .map_err(|e| fs_error_to_object_store(&Path::from(from.full_path()), e))?;
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

    async fn list_objects(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<Vec<object_store::Result<ObjectMeta>>> {
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

        Ok(statuses
            .into_iter()
            .filter(|status| !status.is_dir)
            .map(|status| {
                let path = relative_object_path(&self.context.workspace_root, &status.path)
                    .map_err(|e| object_store::Error::Generic {
                        store: CURVINE_SCHEME,
                        source: e.into(),
                    })?;
                Ok(file_status_to_object_meta(path, status))
            })
            .collect())
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

pub fn unsupported_curvine_uri(uri: impl Into<String>) -> Error {
    Error::NotSupported {
        message: format!(
            "Curvine object store, uri={} is not implemented",
            uri.into()
        ),
    }
}

fn curvine_path_from_url(url: &Url) -> std::result::Result<CurvinePath, String> {
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

fn fs_error_to_object_store(location: &Path, error: impl std::fmt::Display) -> object_store::Error {
    let msg = error.to_string();
    if msg.contains("not found") || msg.contains("NotFound") {
        object_store::Error::NotFound {
            path: location.to_string(),
            source: msg.into(),
        }
    } else {
        object_store::Error::Generic {
            store: CURVINE_SCHEME,
            source: msg.into(),
        }
    }
}
