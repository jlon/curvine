// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lance_io::object_store::ObjectStore;
use lance_namespace::models::{CreateNamespaceRequest, DropNamespaceRequest, ListTablesRequest};
use lancedb::connect_namespace;
use lancedb::curvine_database::CurvineIntegrityDatabase;
use lancedb::object_store::curvine_session;
use lancedb_upstream::database::{CreateTableMode, CreateTableRequest, Database, OpenTableRequest};
use object_store::path::Path;
use serde_json::Value;

fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
}

fn metadata_root(table_name: &str) -> String {
    format!(".lancedb/namespaces/default/tables/{table_name}")
}

fn store_relative(base: &Path, rel: &str) -> Path {
    if base.as_ref().is_empty() {
        Path::parse(rel).unwrap()
    } else {
        Path::parse(format!("{}/{rel}", base.as_ref())).unwrap()
    }
}

#[tokio::test]
async fn curvine_metadata_written_after_create_and_used_on_open() {
    let tmpdir = tempfile::tempdir().unwrap();
    let uri = tmpdir.path().to_str().unwrap();
    let table_name = "integrity_table";

    let db = CurvineIntegrityDatabase::connect(uri, HashMap::new(), None, None)
        .await
        .unwrap();
    db.create_table(CreateTableRequest::new(
        table_name.to_string(),
        Box::new(make_batch()),
    ))
    .await
    .unwrap();

    let (store, base_path) = ObjectStore::from_uri(uri).await.unwrap();
    let root = metadata_root(table_name);
    let latest = store_relative(&base_path, &format!("{root}/state/latest.json"));
    let latest_bytes = store.read_one_all(&latest).await.unwrap();
    let latest_json: Value = serde_json::from_slice(&latest_bytes).unwrap();
    let generation = latest_json
        .get("generation")
        .and_then(Value::as_str)
        .unwrap()
        .to_string();

    let manifest = store_relative(
        &base_path,
        &format!("{root}/versions/{generation}/manifest.json"),
    );
    let checksum = store_relative(
        &base_path,
        &format!("{root}/versions/{generation}/checksum.json"),
    );
    assert!(store.exists(&manifest).await.unwrap());
    assert!(store.exists(&checksum).await.unwrap());

    let manifest_json: Value =
        serde_json::from_slice(&store.read_one_all(&manifest).await.unwrap()).unwrap();
    let dataset_uri = manifest_json
        .get("dataset_uri")
        .and_then(Value::as_str)
        .unwrap()
        .to_string();
    assert!(
        dataset_uri.ends_with(&format!(
            ".lancedb/namespaces/default/tables/{table_name}/versions/{generation}/dataset"
        )),
        "unexpected dataset uri: {dataset_uri}"
    );

    let files = manifest_json
        .get("files")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .map(|v: &Value| v.as_str().unwrap().to_string())
        .collect::<BTreeSet<_>>();
    assert!(!files.is_empty(), "manifest must record dataset files");

    let checksum_json: Value =
        serde_json::from_slice(&store.read_one_all(&checksum).await.unwrap()).unwrap();
    let checksum_files = checksum_json
        .get("files")
        .and_then(Value::as_object)
        .unwrap()
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        files, checksum_files,
        "checksum entries must match manifest"
    );

    let reopened = db
        .open_table(OpenTableRequest {
            name: table_name.to_string(),
            namespace: vec![],
            index_cache_size: None,
            lance_read_params: None,
            location: None,
            namespace_client: None,
            managed_versioning: None,
        })
        .await
        .unwrap();
    assert_eq!(reopened.count_rows(None).await.unwrap(), 3);
}

#[tokio::test]
async fn curvine_open_rejects_checksum_tampering() {
    let tmpdir = tempfile::tempdir().unwrap();
    let uri = tmpdir.path().to_str().unwrap();
    let table_name = "tampered_table";

    let db = CurvineIntegrityDatabase::connect(uri, HashMap::new(), None, None)
        .await
        .unwrap();
    db.create_table(CreateTableRequest::new(
        table_name.to_string(),
        Box::new(make_batch()),
    ))
    .await
    .unwrap();

    let (store, base_path) = ObjectStore::from_uri(uri).await.unwrap();
    let root = metadata_root(table_name);
    let latest = store_relative(&base_path, &format!("{root}/state/latest.json"));
    let latest_json: Value =
        serde_json::from_slice(&store.read_one_all(&latest).await.unwrap()).unwrap();
    let generation = latest_json
        .get("generation")
        .and_then(Value::as_str)
        .unwrap();

    let checksum = store_relative(
        &base_path,
        &format!("{root}/versions/{generation}/checksum.json"),
    );
    let mut checksum_json: Value =
        serde_json::from_slice(&store.read_one_all(&checksum).await.unwrap()).unwrap();
    let files = checksum_json
        .get_mut("files")
        .and_then(Value::as_object_mut)
        .unwrap();
    let first = files.values_mut().next().unwrap();
    *first = Value::String("deadbeef".to_string());
    store
        .put(
            &checksum,
            serde_json::to_vec(&checksum_json).unwrap().as_slice(),
        )
        .await
        .unwrap();

    let err = db
        .open_table(OpenTableRequest {
            name: table_name.to_string(),
            namespace: vec![],
            index_cache_size: None,
            lance_read_params: None,
            location: None,
            namespace_client: None,
            managed_versioning: None,
        })
        .await
        .unwrap_err();
    let rendered = err.to_string();
    assert!(
        rendered.contains("checksum"),
        "unexpected error message: {rendered}"
    );
}

#[tokio::test]
async fn curvine_overwrite_advances_generation_and_latest() {
    let tmpdir = tempfile::tempdir().unwrap();
    let uri = tmpdir.path().to_str().unwrap();
    let table_name = "versioned_table";

    let db = CurvineIntegrityDatabase::connect(uri, HashMap::new(), None, None)
        .await
        .unwrap();
    db.create_table(CreateTableRequest::new(
        table_name.to_string(),
        Box::new(make_batch()),
    ))
    .await
    .unwrap();

    let (store, base_path) = ObjectStore::from_uri(uri).await.unwrap();
    let root = metadata_root(table_name);
    let latest = store_relative(&base_path, &format!("{root}/state/latest.json"));
    let first_latest: Value =
        serde_json::from_slice(&store.read_one_all(&latest).await.unwrap()).unwrap();
    let first_generation = first_latest["generation"].as_str().unwrap().to_string();

    let overwrite_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![10, 20]))],
    )
    .unwrap();
    let mut overwrite = CreateTableRequest::new(table_name.to_string(), Box::new(overwrite_batch));
    overwrite.mode = CreateTableMode::Overwrite;
    db.create_table(overwrite).await.unwrap();

    let second_latest: Value =
        serde_json::from_slice(&store.read_one_all(&latest).await.unwrap()).unwrap();
    let second_generation = second_latest["generation"].as_str().unwrap().to_string();
    assert_ne!(first_generation, second_generation);

    let reopened = db
        .open_table(OpenTableRequest {
            name: table_name.to_string(),
            namespace: vec![],
            index_cache_size: None,
            lance_read_params: None,
            location: None,
            namespace_client: None,
            managed_versioning: None,
        })
        .await
        .unwrap();
    assert_eq!(reopened.count_rows(None).await.unwrap(), 2);
}

#[tokio::test]
async fn curvine_drop_table_removes_latest_and_rejects_open() {
    let tmpdir = tempfile::tempdir().unwrap();
    let uri = tmpdir.path().to_str().unwrap();
    let table_name = "drop_me";

    let db = CurvineIntegrityDatabase::connect(uri, HashMap::new(), None, None)
        .await
        .unwrap();
    db.create_table(CreateTableRequest::new(
        table_name.to_string(),
        Box::new(make_batch()),
    ))
    .await
    .unwrap();

    db.drop_table(table_name, &[]).await.unwrap();

    let (store, base_path) = ObjectStore::from_uri(uri).await.unwrap();
    let latest = store_relative(
        &base_path,
        &format!("{}/state/latest.json", metadata_root(table_name)),
    );
    assert!(
        !store.exists(&latest).await.unwrap(),
        "latest pointer must be removed when table is dropped"
    );

    let err = db
        .open_table(OpenTableRequest {
            name: table_name.to_string(),
            namespace: vec![],
            index_cache_size: None,
            lance_read_params: None,
            location: None,
            namespace_client: None,
            managed_versioning: None,
        })
        .await
        .unwrap_err();
    let rendered = err.to_string();
    assert!(
        rendered.contains("not found")
            || rendered.contains("Not found")
            || rendered.contains("NotFound")
            || rendered.contains("latest"),
        "unexpected error message: {rendered}"
    );
}

#[tokio::test]
async fn curvine_exist_ok_returns_existing_table_without_new_generation() {
    let tmpdir = tempfile::tempdir().unwrap();
    let uri = tmpdir.path().to_str().unwrap();
    let table_name = "exist_ok_table";

    let db = CurvineIntegrityDatabase::connect(uri, HashMap::new(), None, None)
        .await
        .unwrap();
    db.create_table(CreateTableRequest::new(
        table_name.to_string(),
        Box::new(make_batch()),
    ))
    .await
    .unwrap();

    let (store, base_path) = ObjectStore::from_uri(uri).await.unwrap();
    let latest = store_relative(
        &base_path,
        &format!("{}/state/latest.json", metadata_root(table_name)),
    );
    let first_latest: Value =
        serde_json::from_slice(&store.read_one_all(&latest).await.unwrap()).unwrap();

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![9]))],
    )
    .unwrap();
    let mut req = CreateTableRequest::new(table_name.to_string(), Box::new(batch));
    req.mode = CreateTableMode::exist_ok(|open| open);
    let table = db.create_table(req).await.unwrap();
    assert_eq!(table.count_rows(None).await.unwrap(), 3);

    let second_latest: Value =
        serde_json::from_slice(&store.read_one_all(&latest).await.unwrap()).unwrap();
    assert_eq!(first_latest, second_latest);
}

#[tokio::test]
async fn curvine_drop_all_tables_clears_namespace() {
    let tmpdir = tempfile::tempdir().unwrap();
    let uri = tmpdir.path().to_str().unwrap();

    let db = CurvineIntegrityDatabase::connect(uri, HashMap::new(), None, None)
        .await
        .unwrap();
    for name in ["t1", "t2"] {
        db.create_table(CreateTableRequest::new(
            name.to_string(),
            Box::new(make_batch()),
        ))
        .await
        .unwrap();
    }

    db.drop_all_tables(&[]).await.unwrap();
    let listed = db
        .list_tables(ListTablesRequest::new())
        .await
        .unwrap()
        .tables;
    assert!(listed.is_empty());
}

#[tokio::test]
async fn curvine_namespace_tables_are_isolated() {
    let tmpdir = tempfile::tempdir().unwrap();
    let uri = tmpdir.path().to_str().unwrap();

    let db = CurvineIntegrityDatabase::connect(uri, HashMap::new(), None, None)
        .await
        .unwrap();
    db.create_namespace(CreateNamespaceRequest {
        id: Some(vec!["team".to_string()]),
        ..Default::default()
    })
    .await
    .unwrap();

    let mut req = CreateTableRequest::new("ns_table".to_string(), Box::new(make_batch()));
    req.namespace = vec!["team".to_string()];
    db.create_table(req).await.unwrap();

    let root_tables = db
        .list_tables(ListTablesRequest::new())
        .await
        .unwrap()
        .tables;
    assert!(root_tables.is_empty());

    let ns_tables = db
        .list_tables(ListTablesRequest {
            id: Some(vec!["team".to_string()]),
            ..ListTablesRequest::new()
        })
        .await
        .unwrap();
    assert_eq!(ns_tables.tables, vec!["ns_table".to_string()]);

    db.drop_all_tables(&["team".to_string()]).await.unwrap();
    let ns_tables_after = db
        .list_tables(ListTablesRequest {
            id: Some(vec!["team".to_string()]),
            ..ListTablesRequest::new()
        })
        .await
        .unwrap();
    assert!(ns_tables_after.tables.is_empty());
}

#[tokio::test]
async fn curvine_rename_table_moves_metadata_and_open_path() {
    let tmpdir = tempfile::tempdir().unwrap();
    let uri = tmpdir.path().to_str().unwrap();

    let db = CurvineIntegrityDatabase::connect(uri, HashMap::new(), None, None)
        .await
        .unwrap();
    db.create_table(CreateTableRequest::new(
        "old_name".to_string(),
        Box::new(make_batch()),
    ))
    .await
    .unwrap();

    db.rename_table("old_name", "new_name", &[], &[])
        .await
        .unwrap();

    let (store, base_path) = ObjectStore::from_uri(uri).await.unwrap();
    let latest = store_relative(
        &base_path,
        ".lancedb/namespaces/default/tables/new_name/state/latest.json",
    );
    let latest_json: Value =
        serde_json::from_slice(&store.read_one_all(&latest).await.unwrap()).unwrap();
    let generation = latest_json["generation"].as_str().unwrap();
    let manifest = store_relative(
        &base_path,
        &format!(".lancedb/namespaces/default/tables/new_name/versions/{generation}/manifest.json"),
    );
    let manifest_json: Value =
        serde_json::from_slice(&store.read_one_all(&manifest).await.unwrap()).unwrap();
    let dataset_uri = manifest_json["dataset_uri"].as_str().unwrap();
    assert!(
        dataset_uri.ends_with(&format!(
            ".lancedb/namespaces/default/tables/new_name/versions/{generation}/dataset"
        )),
        "rename must rewrite manifest dataset_uri, got {dataset_uri}"
    );

    let renamed = db
        .open_table(OpenTableRequest {
            name: "new_name".to_string(),
            namespace: vec![],
            index_cache_size: None,
            lance_read_params: None,
            location: None,
            namespace_client: None,
            managed_versioning: None,
        })
        .await
        .unwrap();
    assert_eq!(renamed.count_rows(None).await.unwrap(), 3);

    let err = db
        .open_table(OpenTableRequest {
            name: "old_name".to_string(),
            namespace: vec![],
            index_cache_size: None,
            lance_read_params: None,
            location: None,
            namespace_client: None,
            managed_versioning: None,
        })
        .await
        .unwrap_err();
    let rendered = err.to_string();
    assert!(
        rendered.contains("not found")
            || rendered.contains("Not found")
            || rendered.contains("NotFound")
            || rendered.contains("latest"),
        "unexpected error message: {rendered}"
    );
}

#[tokio::test]
async fn curvine_drop_namespace_requires_empty_namespace() {
    let tmpdir = tempfile::tempdir().unwrap();
    let uri = tmpdir.path().to_str().unwrap();

    let db = CurvineIntegrityDatabase::connect(uri, HashMap::new(), None, None)
        .await
        .unwrap();
    db.create_namespace(CreateNamespaceRequest {
        id: Some(vec!["team".to_string()]),
        ..Default::default()
    })
    .await
    .unwrap();

    let mut req = CreateTableRequest::new("keep".to_string(), Box::new(make_batch()));
    req.namespace = vec!["team".to_string()];
    db.create_table(req).await.unwrap();

    let err = db
        .drop_namespace(DropNamespaceRequest {
            id: Some(vec!["team".to_string()]),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not empty") || err.to_string().contains("tables"),
        "unexpected error message: {err}"
    );
}

#[tokio::test]
async fn curvine_drop_namespace_rejects_child_namespaces() {
    let tmpdir = tempfile::tempdir().unwrap();
    let uri = tmpdir.path().to_str().unwrap();

    let db = CurvineIntegrityDatabase::connect(uri, HashMap::new(), None, None)
        .await
        .unwrap();
    db.create_namespace(CreateNamespaceRequest {
        id: Some(vec!["team".to_string(), "sub".to_string()]),
        ..Default::default()
    })
    .await
    .unwrap();

    let err = db
        .drop_namespace(DropNamespaceRequest {
            id: Some(vec!["team".to_string()]),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("child namespaces"),
        "unexpected error message: {err}"
    );
}

#[tokio::test]
#[ignore = "requires Curvine-backed namespace facade path"]
async fn facade_connect_namespace_with_curvine_root_uses_integrity_layout() {
    let tmpdir = tempfile::tempdir().unwrap();
    let local_root = tmpdir.path().to_str().unwrap().trim_start_matches('/');
    let curvine_root = format!("curvine://{local_root}");

    let mut properties = HashMap::new();
    properties.insert("root".to_string(), curvine_root);

    let conn = connect_namespace("dir", properties)
        .session(curvine_session())
        .execute()
        .await
        .unwrap();

    conn.create_table("facade_ns_table", make_batch())
        .namespace(vec!["team".to_string()])
        .execute()
        .await
        .unwrap();

    let (store, base_path) = ObjectStore::from_uri(tmpdir.path().to_str().unwrap())
        .await
        .unwrap();
    let latest = store_relative(
        &base_path,
        ".lancedb/namespaces/team/tables/facade_ns_table/state/latest.json",
    );
    assert!(
        store.exists(&latest).await.unwrap(),
        "connect_namespace(curvine://...) must write Curvine integrity metadata layout"
    );
}
