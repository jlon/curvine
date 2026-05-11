// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, StorageOptionsAccessor};
use lance_namespace::models::{
    CreateNamespaceRequest, CreateNamespaceResponse, DescribeNamespaceRequest,
    DescribeNamespaceResponse, DropNamespaceRequest, DropNamespaceResponse, ListNamespacesRequest,
    ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
};
use lance_namespace::LanceNamespace;
use lancedb_upstream::database::{
    CloneTableRequest, CreateTableMode, CreateTableRequest, Database, OpenTableRequest,
    ReadConsistency, TableNamesRequest,
};
use lancedb_upstream::error::{Error, Result};
use lancedb_upstream::table::{BaseTable, NativeTable, ReadParams, WriteOptions};
use lancedb_upstream::utils::validate_namespace_name;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

use crate::object_store::curvine_session;

const ROOT_NAMESPACE_COMPONENT: &str = "default";
const INTERNAL_MARKER_FILE: &str = ".keep";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LatestPointer {
    generation: String,
    updated_at_utc: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionManifest {
    generation: String,
    dataset_uri: String,
    files: Vec<String>,
    created_at_utc: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionChecksum {
    generation: String,
    files: BTreeMap<String, String>,
    generated_at_utc: String,
}

#[derive(Debug)]
pub struct CurvineIntegrityDatabase {
    uri: String,
    object_store: Arc<ObjectStore>,
    base_path: object_store::path::Path,
    read_consistency_interval: Option<std::time::Duration>,
    storage_options: HashMap<String, String>,
    session: Arc<lancedb_upstream::Session>,
}

impl std::fmt::Display for CurvineIntegrityDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CurvineIntegrityDatabase(uri={})", self.uri)
    }
}

impl CurvineIntegrityDatabase {
    pub async fn connect(
        uri: &str,
        storage_options: HashMap<String, String>,
        read_consistency_interval: Option<std::time::Duration>,
        session: Option<Arc<lancedb_upstream::Session>>,
    ) -> Result<Self> {
        let session = session.unwrap_or_else(curvine_session);
        let params = ObjectStoreParams {
            storage_options_accessor: if storage_options.is_empty() {
                None
            } else {
                Some(Arc::new(StorageOptionsAccessor::with_static_options(
                    storage_options.clone(),
                )))
            },
            ..Default::default()
        };
        let (object_store, base_path) =
            ObjectStore::from_uri_and_params(session.store_registry(), uri, &params).await?;
        Ok(Self {
            uri: uri.to_string(),
            object_store,
            base_path,
            read_consistency_interval,
            storage_options,
            session,
        })
    }

    fn store_path(&self, rel: &str) -> Result<object_store::path::Path> {
        let base = self.base_path.as_ref();
        if base.is_empty() {
            object_store::path::Path::parse(rel).map_err(Error::from)
        } else {
            object_store::path::Path::parse(format!("{base}/{rel}")).map_err(Error::from)
        }
    }

    fn namespace_components(namespace_path: &[String]) -> Result<Vec<String>> {
        if namespace_path.is_empty() {
            Ok(vec![ROOT_NAMESPACE_COMPONENT.to_string()])
        } else {
            for component in namespace_path {
                lancedb_upstream::utils::validate_namespace_name(component)?;
            }
            Ok(namespace_path.to_vec())
        }
    }

    fn namespace_root(&self, namespace_path: &[String]) -> Result<String> {
        let ns = Self::namespace_components(namespace_path)?;
        Ok(format!(".lancedb/namespaces/{}", ns.join("/")))
    }

    fn table_root(&self, name: &str, namespace_path: &[String]) -> Result<String> {
        lancedb_upstream::utils::validate_table_name(name)?;
        Ok(format!(
            "{}/tables/{name}",
            self.namespace_root(namespace_path)?
        ))
    }

    fn table_state_dir(&self, name: &str, namespace_path: &[String]) -> Result<String> {
        Ok(format!("{}/state", self.table_root(name, namespace_path)?))
    }

    fn latest_path(
        &self,
        name: &str,
        namespace_path: &[String],
    ) -> Result<object_store::path::Path> {
        self.store_path(&format!(
            "{}/latest.json",
            self.table_state_dir(name, namespace_path)?
        ))
    }

    fn version_root(
        &self,
        name: &str,
        namespace_path: &[String],
        generation: &str,
    ) -> Result<String> {
        Ok(format!(
            "{}/versions/{generation}",
            self.table_root(name, namespace_path)?
        ))
    }

    fn dataset_uri(
        &self,
        name: &str,
        namespace_path: &[String],
        generation: &str,
    ) -> Result<String> {
        Ok(format!(
            "{}/{}/dataset",
            self.uri.trim_end_matches('/'),
            self.version_root(name, namespace_path, generation)?
        ))
    }

    fn manifest_path(
        &self,
        name: &str,
        namespace_path: &[String],
        generation: &str,
    ) -> Result<object_store::path::Path> {
        self.store_path(&format!(
            "{}/manifest.json",
            self.version_root(name, namespace_path, generation)?
        ))
    }

    fn checksum_path(
        &self,
        name: &str,
        namespace_path: &[String],
        generation: &str,
    ) -> Result<object_store::path::Path> {
        self.store_path(&format!(
            "{}/checksum.json",
            self.version_root(name, namespace_path, generation)?
        ))
    }

    fn dataset_prefix(
        &self,
        name: &str,
        namespace_path: &[String],
        generation: &str,
    ) -> Result<object_store::path::Path> {
        self.store_path(&format!(
            "{}/dataset",
            self.version_root(name, namespace_path, generation)?
        ))
    }

    async fn ensure_namespace_path(&self, namespace_path: &[String]) -> Result<()> {
        let components = Self::namespace_components(namespace_path)?;
        let mut prefix = String::from(".lancedb/namespaces");
        for component in components {
            validate_namespace_name(&component)?;
            prefix.push('/');
            prefix.push_str(&component);
            let marker = self.store_path(&format!("{prefix}/{INTERNAL_MARKER_FILE}"))?;
            if !self.object_store.exists(&marker).await? {
                self.object_store.put(&marker, &[]).await?;
            }
        }
        Ok(())
    }

    async fn ensure_table_dirs(&self, name: &str, namespace_path: &[String]) -> Result<()> {
        self.ensure_namespace_path(namespace_path).await?;
        for rel in [
            format!(
                "{}/{}",
                self.table_root(name, namespace_path)?,
                INTERNAL_MARKER_FILE
            ),
            format!(
                "{}/{}",
                self.table_state_dir(name, namespace_path)?,
                INTERNAL_MARKER_FILE
            ),
        ] {
            let path = self.store_path(&rel)?;
            if !self.object_store.exists(&path).await? {
                self.object_store.put(&path, &[]).await?;
            }
        }
        Ok(())
    }

    async fn write_json<T: Serialize>(
        &self,
        path: &object_store::path::Path,
        value: &T,
    ) -> Result<()> {
        let buf = serde_json::to_vec_pretty(value).map_err(|e| Error::Runtime {
            message: format!("failed to serialize metadata json: {e}"),
        })?;
        self.object_store.put(path, &buf).await?;
        Ok(())
    }

    async fn read_json<T: for<'de> Deserialize<'de>>(
        &self,
        path: &object_store::path::Path,
    ) -> Result<T> {
        let buf = self.object_store.read_one_all(path).await?;
        serde_json::from_slice(&buf).map_err(|e| Error::Runtime {
            message: format!("failed to decode metadata json '{}': {e}", path.as_ref()),
        })
    }

    async fn list_dataset_files(
        &self,
        name: &str,
        namespace_path: &[String],
        generation: &str,
    ) -> Result<Vec<String>> {
        let prefix = self.dataset_prefix(name, namespace_path, generation)?;
        let prefix_str = prefix.as_ref().trim_end_matches('/').to_string();
        let mut files: Vec<String> = self
            .object_store
            .list(Some(prefix))
            .try_filter_map(|meta| {
                let prefix_str = prefix_str.clone();
                async move {
                    let rel = meta
                        .location
                        .as_ref()
                        .strip_prefix(&prefix_str)
                        .unwrap_or(meta.location.as_ref());
                    let rel = rel.trim_start_matches('/');
                    if rel.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(rel.to_string()))
                    }
                }
            })
            .try_collect::<Vec<_>>()
            .await?;
        files.sort();
        Ok(files)
    }

    async fn finalize_integrity_metadata(
        &self,
        name: &str,
        namespace_path: &[String],
        generation: &str,
    ) -> Result<()> {
        let files = self
            .list_dataset_files(name, namespace_path, generation)
            .await?;
        if files.is_empty() {
            return Err(Error::Runtime {
                message: format!("dataset write produced no files for table '{name}'"),
            });
        }

        let dataset_uri = self.dataset_uri(name, namespace_path, generation)?;
        let manifest = VersionManifest {
            generation: generation.to_string(),
            dataset_uri,
            files: files.clone(),
            created_at_utc: Utc::now().to_rfc3339(),
        };

        let mut checksum_files = BTreeMap::new();
        for file in &files {
            let path = self.store_path(&format!(
                "{}/dataset/{file}",
                self.version_root(name, namespace_path, generation)?
            ))?;
            let bytes = self.object_store.read_one_all(&path).await?;
            let mut hasher = Md5::new();
            hasher.update(&bytes);
            checksum_files.insert(file.clone(), format!("{:x}", hasher.finalize()));
        }
        let checksum = VersionChecksum {
            generation: generation.to_string(),
            files: checksum_files,
            generated_at_utc: Utc::now().to_rfc3339(),
        };
        let latest = LatestPointer {
            generation: generation.to_string(),
            updated_at_utc: Utc::now().to_rfc3339(),
        };

        self.write_json(
            &self.manifest_path(name, namespace_path, generation)?,
            &manifest,
        )
        .await?;
        self.write_json(
            &self.checksum_path(name, namespace_path, generation)?,
            &checksum,
        )
        .await?;
        self.write_json(&self.latest_path(name, namespace_path)?, &latest)
            .await?;
        Ok(())
    }

    async fn validate_integrity(&self, name: &str, namespace_path: &[String]) -> Result<String> {
        let latest: LatestPointer = self
            .read_json(&self.latest_path(name, namespace_path)?)
            .await?;
        let generation = latest.generation;
        let manifest: VersionManifest = self
            .read_json(&self.manifest_path(name, namespace_path, &generation)?)
            .await?;
        let checksum: VersionChecksum = self
            .read_json(&self.checksum_path(name, namespace_path, &generation)?)
            .await?;

        let manifest_files = manifest.files.iter().cloned().collect::<BTreeSet<_>>();
        let checksum_files = checksum.files.keys().cloned().collect::<BTreeSet<_>>();
        if manifest_files != checksum_files {
            return Err(Error::Runtime {
                message: format!(
                    "checksum file set does not match manifest for table '{name}' generation '{generation}'"
                ),
            });
        }

        for file in &manifest.files {
            let path = self.store_path(&format!(
                "{}/dataset/{file}",
                self.version_root(name, namespace_path, &generation)?
            ))?;
            if !self.object_store.exists(&path).await? {
                return Err(Error::Runtime {
                    message: format!(
                        "manifest references missing file '{file}' for table '{name}' generation '{generation}'"
                    ),
                });
            }
            let bytes = self.object_store.read_one_all(&path).await?;
            let mut hasher = Md5::new();
            hasher.update(&bytes);
            let actual = format!("{:x}", hasher.finalize());
            let expected = checksum.files.get(file).unwrap();
            if &actual != expected {
                return Err(Error::Runtime {
                    message: format!(
                        "checksum mismatch for table '{name}' generation '{generation}' file '{file}'"
                    ),
                });
            }
        }

        self.dataset_uri(name, namespace_path, &generation)
    }

    fn inherited_read_params(&self, request: &OpenTableRequest) -> Option<ReadParams> {
        let mut read_params = request.lance_read_params.clone().unwrap_or_default();

        if !self.storage_options.is_empty() {
            let store_params = read_params
                .store_options
                .get_or_insert_with(Default::default);
            let mut storage_options = store_params.storage_options().cloned().unwrap_or_default();
            for (key, value) in &self.storage_options {
                storage_options
                    .entry(key.clone())
                    .or_insert_with(|| value.clone());
            }
            store_params.storage_options_accessor = Some(Arc::new(
                StorageOptionsAccessor::with_static_options(storage_options),
            ));
        }

        read_params.session(self.session.clone());
        Some(read_params)
    }

    fn inherited_write_options(&self, request: &CreateTableRequest) -> WriteOptions {
        let mut write_options = request.write_options.clone();
        let write_params = write_options
            .lance_write_params
            .get_or_insert_with(Default::default);
        let store_params = write_params
            .store_params
            .get_or_insert_with(Default::default);
        let mut storage_options = store_params.storage_options().cloned().unwrap_or_default();
        for (key, value) in &self.storage_options {
            storage_options
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
        store_params.storage_options_accessor = Some(Arc::new(
            StorageOptionsAccessor::with_static_options(storage_options),
        ));
        write_params.session = Some(self.session.clone());
        write_options
    }
}

#[async_trait]
impl Database for CurvineIntegrityDatabase {
    fn uri(&self) -> &str {
        &self.uri
    }

    async fn read_consistency(&self) -> Result<ReadConsistency> {
        if let Some(interval) = self.read_consistency_interval {
            if interval.is_zero() {
                Ok(ReadConsistency::Strong)
            } else {
                Ok(ReadConsistency::Eventual(interval))
            }
        } else {
            Ok(ReadConsistency::Manual)
        }
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        let id = request.id.unwrap_or_default();
        let root = self.namespace_root(&id)?;
        let prefix = self.store_path(&root)?;
        let listed = self.object_store.read_dir(prefix).await.unwrap_or_default();
        let mut namespaces = listed
            .into_iter()
            .filter(|entry| entry != "tables" && entry != INTERNAL_MARKER_FILE)
            .collect::<Vec<_>>();
        namespaces.sort();
        Ok(ListNamespacesResponse {
            namespaces,
            page_token: None,
        })
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        let id = request.id.unwrap_or_default();
        self.ensure_namespace_path(&id).await?;
        Ok(CreateNamespaceResponse::new())
    }

    async fn drop_namespace(
        &self,
        _request: DropNamespaceRequest,
    ) -> Result<DropNamespaceResponse> {
        Err(Error::NotSupported {
            message: "drop_namespace is not implemented for Curvine integrity database yet"
                .to_string(),
        })
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        let id = request.id.unwrap_or_default();
        self.ensure_namespace_path(&id).await?;
        Ok(DescribeNamespaceResponse {
            properties: Some(HashMap::new()),
        })
    }

    #[allow(deprecated)]
    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        let response = self
            .list_tables(ListTablesRequest {
                id: Some(request.namespace),
                page_token: request.start_after,
                limit: request.limit.map(|v| v as i32),
                ..ListTablesRequest::new()
            })
            .await?;
        Ok(response.tables)
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        let namespace_path = request.id.unwrap_or_default();
        let root = format!("{}/tables", self.namespace_root(&namespace_path)?);
        let prefix = self.store_path(&root)?;
        let listed = self.object_store.read_dir(prefix).await.unwrap_or_default();
        let mut tables = listed
            .into_iter()
            .filter(|entry| entry != INTERNAL_MARKER_FILE)
            .collect::<Vec<_>>();
        tables.sort();
        if let Some(page_token) = request.page_token {
            let index = tables
                .iter()
                .position(|name| name.as_str() > page_token.as_str())
                .unwrap_or(tables.len());
            tables.drain(0..index);
        }
        let next_page_token = if let Some(limit) = request.limit {
            if tables.len() > limit as usize {
                let token = tables[limit as usize].clone();
                tables.truncate(limit as usize);
                Some(token)
            } else {
                None
            }
        } else {
            None
        };
        Ok(ListTablesResponse {
            tables,
            page_token: next_page_token,
        })
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        let name = request.name.clone();
        let namespace_path = request.namespace.clone();

        match request.mode {
            CreateTableMode::Create => {
                if self
                    .object_store
                    .exists(&self.latest_path(&name, &namespace_path)?)
                    .await?
                {
                    return Err(Error::TableAlreadyExists { name });
                }
            }
            CreateTableMode::Overwrite => {
                return Err(Error::NotSupported {
                    message: "overwrite mode is not implemented for Curvine integrity database yet"
                        .to_string(),
                });
            }
            CreateTableMode::ExistOk(_) => {
                return Err(Error::NotSupported {
                    message: "exist_ok mode is not implemented for Curvine integrity database yet"
                        .to_string(),
                });
            }
        }

        self.ensure_table_dirs(&name, &namespace_path).await?;
        let generation = "0000000000000001".to_string();
        let dataset_uri = self.dataset_uri(&name, &namespace_path, &generation)?;
        let write_options = self.inherited_write_options(&request);
        let table = NativeTable::create(
            &dataset_uri,
            &request.name,
            request.namespace.clone(),
            request.data,
            None,
            Some(write_options.lance_write_params.unwrap_or_default()),
            self.read_consistency_interval,
            request.namespace_client,
            false,
        )
        .await?;

        self.finalize_integrity_metadata(&name, &namespace_path, &generation)
            .await?;
        Ok(Arc::new(table))
    }

    async fn clone_table(&self, _request: CloneTableRequest) -> Result<Arc<dyn BaseTable>> {
        Err(Error::NotSupported {
            message: "clone_table is not implemented for Curvine integrity database yet"
                .to_string(),
        })
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        let dataset_uri = self
            .validate_integrity(&request.name, &request.namespace)
            .await?;
        let read_params = self.inherited_read_params(&request);
        let table = NativeTable::open_with_params(
            &dataset_uri,
            &request.name,
            request.namespace,
            None,
            read_params,
            self.read_consistency_interval,
            request.namespace_client,
            false,
            request.managed_versioning,
        )
        .await?;
        Ok(Arc::new(table))
    }

    async fn rename_table(
        &self,
        _cur_name: &str,
        _new_name: &str,
        _cur_namespace_path: &[String],
        _new_namespace_path: &[String],
    ) -> Result<()> {
        Err(Error::NotSupported {
            message: "rename_table is not implemented for Curvine integrity database yet"
                .to_string(),
        })
    }

    async fn drop_table(&self, _name: &str, _namespace_path: &[String]) -> Result<()> {
        Err(Error::NotSupported {
            message: "drop_table is not implemented for Curvine integrity database yet".to_string(),
        })
    }

    async fn drop_all_tables(&self, _namespace_path: &[String]) -> Result<()> {
        Err(Error::NotSupported {
            message: "drop_all_tables is not implemented for Curvine integrity database yet"
                .to_string(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn namespace_client(&self) -> Result<Arc<dyn LanceNamespace>> {
        Err(Error::NotSupported {
            message: "namespace_client is not implemented for Curvine integrity database yet"
                .to_string(),
        })
    }
}
