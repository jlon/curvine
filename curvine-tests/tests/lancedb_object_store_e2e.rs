// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
//! Phase 6 — LanceDB on Curvine E2E (ListingDatabase + `curvine://` object store).
//! 覆盖常用路径与最小向量索引路径；各用例使用独立 `curvine:///tmp/...` workspace，并通过
//! `storage_option(CURVINE_CONF_FILE_KEY, …)` 注入配置，避免依赖进程级环境变量。

mod common;

use std::sync::{Arc, Mutex};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int32Type};
use arrow_array::Array;
use arrow_array::{
    BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use common::{row_count, start_minicluster, unique_ns};
use curvine_common::conf::ClusterConf;
use futures::TryStreamExt;
use lancedb::connect;
use lancedb::error::Error as LanceDbError;
use lancedb::index::{Index, IndexType};
use lancedb::object_store::CURVINE_CONF_FILE_KEY;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use lancedb::DistanceType;
use orpc::{CommonError, CommonResult};
use std::env;

static ENV_MUTATION_LOCK: Mutex<()> = Mutex::new(());

fn int32_values(batch: &RecordBatch, column: &str) -> Vec<Option<i32>> {
    let array = batch[column].as_primitive::<Int32Type>();
    (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                None
            } else {
                Some(array.value(i))
            }
        })
        .collect()
}

fn float32_values(batch: &RecordBatch, column: &str) -> Vec<f32> {
    let array = batch[column].as_primitive::<Float32Type>();
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn float64_values(batch: &RecordBatch, column: &str) -> Vec<f64> {
    let array = batch[column].as_primitive::<Float64Type>();
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn bool_values(batch: &RecordBatch, column: &str) -> Vec<bool> {
    let array = batch[column].as_boolean();
    (0..array.len()).map(|i| array.value(i)).collect()
}

fn string_values(batch: &RecordBatch, column: &str) -> Vec<String> {
    let array = batch[column].as_string::<i32>();
    (0..array.len())
        .map(|i| array.value(i).to_string())
        .collect()
}

#[test]
fn lancedb_on_curvine_minicluster_smoke_extended() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/lancedb_smoke_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![10_i32, 20, 30]))],
        )
        .unwrap();

        conn.create_table("smoke_tbl", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(names.contains(&"smoke_tbl".to_string()));

        let table = conn
            .open_table("smoke_tbl")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );

        let stream = table
            .query()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(row_count(&batches), 3);

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![40_i32, 50, 60]))],
        )
        .unwrap();
        table
            .add(batch)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            6
        );

        conn.drop_table("smoke_tbl", &[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let names_after = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(!names_after.contains(&"smoke_tbl".to_string()));

        let reopen = conn
            .open_table("smoke_tbl")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await;
        assert!(reopen.is_err());

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_table_lifecycle_create_list_open_drop_errors() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/lifecycle_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();

        conn.create_table("life_t", batch.clone())
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let dup = conn
            .create_table("life_t", batch.clone())
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await;
        assert!(
            matches!(dup, Err(LanceDbError::TableAlreadyExists { .. })),
            "expected TableAlreadyExists, got {dup:?}"
        );

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(names.contains(&"life_t".to_string()));

        let open_bad = conn
            .open_table("no_such_table")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await;
        assert!(
            matches!(open_bad, Err(LanceDbError::TableNotFound { .. })),
            "expected TableNotFound, got {open_bad:?}"
        );

        let drop_bad = conn.drop_table("no_such_table", &[]).await;
        assert!(
            drop_bad.is_ok() || matches!(drop_bad, Err(LanceDbError::TableNotFound { .. })),
            "drop missing table: expected Ok (idempotent) or TableNotFound, got {drop_bad:?}"
        );

        conn.drop_table("life_t", &[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        conn.create_table("life_t", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let t = conn
            .open_table("life_t")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            t.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            1
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_write_append_multiple_batches_and_schema_mismatch() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/write_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let b1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
            .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![3, 4, 5]))],
        )
        .unwrap();

        let table = conn
            .create_table("w_t", b1)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        table
            .add(b2)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            table
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            5
        );

        let bad_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        let bad_batch =
            RecordBatch::try_new(bad_schema, vec![Arc::new(Int64Array::from(vec![1_i64]))])
                .unwrap();
        let bad_add = table.add(bad_batch).execute().await;
        assert!(
            bad_add.is_err(),
            "schema mismatch add should fail: {bad_add:?}"
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_write_empty_initial_table_has_zero_rows() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/empty_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let empty =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(Vec::<i32>::new()))])
                .unwrap();
        let r = conn
            .create_table("empty_t", empty)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            r.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            0
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_query_limit_select_filter() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/query_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("score", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("q_t", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let stream = table
            .query()
            .limit(2)
            .select(Select::columns(&["id"]))
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(row_count(&batches), 2);
        assert!(batches.iter().all(|batch| batch.num_columns() == 1));
        assert!(batches
            .iter()
            .all(|batch| batch.schema().field(0).name() == "id"));
        let limited_ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| int32_values(batch, "id"))
            .flatten()
            .collect();
        assert_eq!(limited_ids.len(), 2);
        assert!(limited_ids.iter().all(|id| (1..=5).contains(id)));

        let stream = table
            .query()
            .only_if("score >= 30")
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(row_count(&batches), 3);
        let mut ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| int32_values(batch, "id"))
            .flatten()
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![3, 4, 5]);

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_schema_mixed_types_roundtrip() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/mixed_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("flag", DataType::Boolean, false),
            Field::new("opt_i", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Float32Array::from(vec![1.5_f32, 2.5])),
                Arc::new(Float64Array::from(vec![10.1_f64, 20.2])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Int32Array::from(vec![Some(7), None])),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("mix_t", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let stream = table
            .query()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(row_count(&batches), 2);
        let mut rows = Vec::new();
        for batch in &batches {
            let ids = int32_values(batch, "id");
            let labels = string_values(batch, "label");
            let f32s = float32_values(batch, "f32");
            let f64s = float64_values(batch, "f64");
            let flags = bool_values(batch, "flag");
            let opt_is = int32_values(batch, "opt_i");
            for row in 0..batch.num_rows() {
                rows.push((
                    ids[row].expect("id is non-nullable"),
                    labels[row].clone(),
                    f32s[row],
                    f64s[row],
                    flags[row],
                    opt_is[row],
                ));
            }
        }
        rows.sort_by_key(|row| row.0);
        assert_eq!(
            rows,
            vec![
                (1, "a".to_string(), 1.5_f32, 10.1_f64, true, Some(7)),
                (2, "b".to_string(), 2.5_f32, 20.2_f64, false, None),
            ]
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_drop_recreate_and_listing_clean() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/droprec_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();

        conn.create_table("reuse_name", batch.clone())
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        conn.drop_table("reuse_name", &[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(!names.contains(&"reuse_name".to_string()));

        conn.create_table("reuse_name", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let t = conn
            .open_table("reuse_name")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            t.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            1
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_two_tables_parallel_writes_isolation() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/iso_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let s1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let s2 = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let b1 =
            RecordBatch::try_new(s1.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
        let b2 =
            RecordBatch::try_new(s2.clone(), vec![Arc::new(Int32Array::from(vec![100]))]).unwrap();

        conn.create_table("t_a", b1)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        conn.create_table("t_b", b2)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let ta = conn
            .open_table("t_a")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let tb = conn
            .open_table("t_b")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let add_a = RecordBatch::try_new(s1, vec![Arc::new(Int32Array::from(vec![3]))]).unwrap();
        let add_b = RecordBatch::try_new(s2, vec![Arc::new(Int32Array::from(vec![200]))]).unwrap();
        ta.add(add_a)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        tb.add(add_b)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        assert_eq!(
            ta.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            3
        );
        assert_eq!(
            tb.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            2
        );

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(names.contains(&"t_a".to_string()) && names.contains(&"t_b".to_string()));

        conn.drop_table("t_a", &[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let tb_only = conn
            .open_table("t_b")
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            tb_only
                .count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            2
        );

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_connect_curvine_without_conf_fails() -> CommonResult<()> {
    let _guard = ENV_MUTATION_LOCK.lock().expect("poisoned lock");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| CommonError::from(e.to_string()))?;
    let ns = unique_ns();
    let key = ClusterConf::ENV_CONF_FILE;
    let saved = env::var(key).ok();
    let _restore = RestoreEnvVar::new(key, saved);

    env::remove_var(key);
    let err = rt.block_on(async move {
        connect(&format!("curvine:///tmp/noconf_{ns}"))
            .execute()
            .await
    });
    assert!(err.is_err(), "expected connect failure without conf");

    Ok(())
}

struct RestoreEnvVar {
    key: &'static str,
    prev: Option<String>,
}

impl RestoreEnvVar {
    fn new(key: &'static str, prev: Option<String>) -> Self {
        Self { key, prev }
    }
}

impl Drop for RestoreEnvVar {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => env::set_var(self.key, v),
            None => env::remove_var(self.key),
        }
    }
}

#[test]
fn lancedb_connect_unknown_scheme_fails() -> CommonResult<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| CommonError::from(e.to_string()))?;
    let err = rt.block_on(async {
        connect("unknown-scheme-xyz://localhost/bucket/db")
            .execute()
            .await
    });
    assert!(
        err.is_err(),
        "expected failure for unknown object store scheme"
    );
    Ok(())
}

#[test]
fn lancedb_non_root_workspace_allows_dot_curvine_segment() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        let db_uri = format!("curvine:///tmp/lancedb_dot_curvine_{ns}/.curvine/user_ws");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new("z", DataType::Int32, false)]));
        let name = format!("tbl_under_dot_curvine_{ns}");
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![42]))]).unwrap();

        conn.create_table(&name, batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let names = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(names.contains(&name));

        let t = conn
            .open_table(&name)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(
            t.count_rows(None)
                .await
                .map_err(|e| CommonError::from(e.to_string()))?,
            1
        );

        conn.drop_table(&name, &[])
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

#[test]
fn lancedb_vector_column_search_create_index_and_query_again() -> CommonResult<()> {
    let (cluster, rt) = start_minicluster()?;
    let conf = cluster.conf_path.clone();
    let ns = unique_ns();
    rt.block_on(async move {
        const DIM: i32 = 8;
        const N: usize = 256;
        let item = Arc::new(Field::new("item", DataType::Float32, true));
        let mut flat: Vec<f32> = Vec::with_capacity(N * DIM as usize);
        for r in 0..N {
            for c in 0..DIM as usize {
                flat.push((r as f32) * 0.01 + (c as f32) * 0.001);
            }
        }
        let values = Float32Array::from(flat);
        let list = FixedSizeListArray::try_new(item.clone(), DIM, Arc::new(values), None)
            .map_err(|e| CommonError::from(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("vec", DataType::FixedSizeList(item, DIM), false),
        ]));
        let ids = Int32Array::from_iter_values(0..N as i32);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(list)])
            .map_err(|e| CommonError::from(e.to_string()))?;

        let db_uri = format!("curvine:///tmp/vec_idx_{ns}");
        let conn = connect(&db_uri)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let table = conn
            .create_table("vec_t", batch)
            .storage_option(CURVINE_CONF_FILE_KEY, conf.as_str())
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;

        let qv: Vec<f32> = vec![0.0_f32, 0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007];
        let stream = table
            .vector_search(qv.clone())
            .map_err(|e| CommonError::from(e.to_string()))?
            .limit(10)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let before: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(
            row_count(&before) >= 1,
            "vector search before index should return rows"
        );
        assert_nearest_vector_result(&before)?;

        table
            .create_index(&["vec"], Index::Auto)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let index_configs = table
            .list_indices()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert_eq!(index_configs.len(), 1);
        assert_eq!(index_configs[0].index_type, IndexType::IvfPq);
        assert_eq!(index_configs[0].columns, vec!["vec".to_string()]);

        let stats = table
            .index_stats(&index_configs[0].name)
            .await
            .map_err(|e| CommonError::from(e.to_string()))?
            .expect("created vector index should have stats");
        assert_eq!(stats.num_indexed_rows, N);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.index_type, IndexType::IvfPq);
        assert_eq!(stats.distance_type, Some(DistanceType::L2));

        let stream = table
            .vector_search(qv.clone())
            .map_err(|e| CommonError::from(e.to_string()))?
            .limit(10)
            .execute()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        let after: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| CommonError::from(e.to_string()))?;
        assert!(
            row_count(&after) >= 1,
            "vector search after index should return rows"
        );
        assert_nearest_vector_result(&after)?;

        Ok::<(), CommonError>(())
    })?;
    Ok(())
}

fn assert_nearest_vector_result(batches: &[RecordBatch]) -> CommonResult<()> {
    let first = batches
        .first()
        .ok_or_else(|| CommonError::from("vector search returned no batches"))?;
    assert_eq!(
        int32_values(first, "id").first().copied().flatten(),
        Some(0)
    );
    let distance = float32_values(first, "_distance")
        .first()
        .copied()
        .ok_or_else(|| CommonError::from("vector search returned no distance"))?;
    assert!(
        distance.abs() < 1.0e-6,
        "expected nearest vector distance close to zero, got {distance}"
    );
    Ok(())
}
