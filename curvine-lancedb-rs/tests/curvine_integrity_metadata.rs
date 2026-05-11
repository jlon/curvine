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
use lancedb::curvine_database::CurvineIntegrityDatabase;
use lancedb_upstream::database::Database;
use lancedb_upstream::database::{CreateTableRequest, OpenTableRequest};
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
