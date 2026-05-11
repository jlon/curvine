// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Phase 4B — live Curvine [`object_store`] semantics (one integration test).
//!
//! Run (requires a reachable Curvine cluster and `CURVINE_CONF_FILE`):
//! `CURVINE_CONF_FILE=/path/to/cluster.toml cargo test -p curvine-lancedb-rs --test object_store_semantics -- --ignored`
//!
//! Covered operations: `put`, `head`, `get_opts(head=true)`, ranged `get_opts`, overwrite `put`,
//! `copy` (source retained, destination overwritten when present), `delete`, recursive `list`,
//! `list_with_delimiter` (directory prefix is not listed as a file object).

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use lance_io::object_store::{ObjectStoreParams, StorageOptionsAccessor};
use lancedb::object_store::{CurvineObjectStoreProvider, CURVINE_CONF_FILE_KEY};
use lancedb::ObjectStoreProvider;
use object_store::path::Path;
use object_store::{GetOptions, GetRange};
use url::Url;

#[tokio::test]
#[ignore = "live Curvine cluster + CURVINE_CONF_FILE; cargo test -p curvine-lancedb-rs --test object_store_semantics -- --ignored"]
async fn curvine_object_store_semantics_live_cluster() {
    let conf = match std::env::var(curvine_common::conf::ClusterConf::ENV_CONF_FILE) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("Skipping live object-store semantics test: CURVINE_CONF_FILE is not set");
            return;
        }
    };

    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let mut opts = HashMap::new();
    opts.insert(CURVINE_CONF_FILE_KEY.to_string(), conf);
    let params = ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(opts))),
        ..Default::default()
    };

    let uri = format!("curvine:///tmp/curvine_os_sem_{unique}");
    let url = Url::parse(&uri).unwrap();
    let provider = CurvineObjectStoreProvider::new();
    let store = provider.new_store(url, &params).await.expect("new_store");

    let pfx = format!("pfx_{unique}");
    let rel_root = "root_marker.txt";
    let rel_key = format!("{pfx}/nested/obj.bin");
    let rel_copy = format!("{pfx}/nested/obj_copy.bin");
    let rel_dst = format!("{pfx}/nested/copy_overwrite_dst.bin");
    let rel_src = format!("{pfx}/nested/copy_overwrite_src.bin");

    let root_key = Path::parse(rel_root).unwrap();
    store.put(&root_key, b"root").await.unwrap();

    let key = Path::parse(&rel_key).unwrap();
    let payload: &[u8] = b"hello-range-copy";
    store.put(&key, payload).await.unwrap();

    let meta = store.inner.head(&key).await.unwrap();
    assert_eq!(meta.size, payload.len() as u64);
    assert!(meta.e_tag.is_some());

    let head_get = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(head_get.meta.size, meta.size);

    let slice = store.read_one_range(&key, 1..5).await.unwrap();
    assert_eq!(slice.as_ref(), b"ello");

    let bad_range = store
        .inner
        .get_opts(
            &key,
            GetOptions {
                range: Some(GetRange::Bounded(100..200)),
                ..Default::default()
            },
        )
        .await;
    assert!(bad_range.is_err());

    store.put(&key, b"overwrite").await.unwrap();
    let full = store.read_one_all(&key).await.unwrap();
    assert_eq!(full.as_ref(), b"overwrite");

    let copy_key = Path::parse(&rel_copy).unwrap();
    store.copy(&key, &copy_key).await.unwrap();
    assert_eq!(
        store.read_one_all(&copy_key).await.unwrap().as_ref(),
        b"overwrite"
    );
    assert_eq!(
        store.read_one_all(&key).await.unwrap().as_ref(),
        b"overwrite",
        "copy must retain source object"
    );

    let dst_existing = Path::parse(&rel_dst).unwrap();
    let src_for_overwrite = Path::parse(&rel_src).unwrap();
    store
        .put(&dst_existing, b"stale-destination")
        .await
        .unwrap();
    store
        .put(&src_for_overwrite, b"fresh-source-payload")
        .await
        .unwrap();
    store.copy(&src_for_overwrite, &dst_existing).await.unwrap();
    assert_eq!(
        store.read_one_all(&dst_existing).await.unwrap().as_ref(),
        b"fresh-source-payload",
        "copy replaces an existing destination object"
    );
    assert_eq!(
        store
            .read_one_all(&src_for_overwrite)
            .await
            .unwrap()
            .as_ref(),
        b"fresh-source-payload",
        "copy overwrite must keep the source object"
    );

    let prefix_path = Path::parse(format!("{pfx}/")).unwrap();
    let mut listed: Vec<Path> = store
        .list(Some(prefix_path.clone()))
        .map(|r| r.expect("list entry").location)
        .collect()
        .await;
    listed.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    let ends = |p: &Path, s: &str| p.as_ref().ends_with(s);
    assert!(listed.iter().any(|p| ends(p, &rel_key)));
    assert!(listed.iter().any(|p| ends(p, &rel_copy)));
    assert!(listed.iter().any(|p| ends(p, &rel_dst)));
    assert!(listed.iter().any(|p| ends(p, &rel_src)));

    let lr_root = store.inner.list_with_delimiter(None).await.unwrap();
    assert!(lr_root
        .objects
        .iter()
        .any(|o| o.location.as_ref().ends_with(rel_root)));
    assert!(lr_root
        .common_prefixes
        .iter()
        .any(|p| p.as_ref().starts_with(&pfx)));

    let lr_pfx = store
        .inner
        .list_with_delimiter(Some(&prefix_path))
        .await
        .unwrap();
    assert!(
        lr_pfx
            .common_prefixes
            .iter()
            .any(|p| p.as_ref().contains("nested")),
        "expected nested/ as common prefix"
    );
    let flat_name = Path::parse(&pfx).unwrap();
    assert!(
        !lr_pfx.objects.iter().any(|o| o.location == flat_name),
        "directory prefix must not appear as a file object"
    );

    store.delete(&copy_key).await.unwrap();
    assert!(store.inner.head(&copy_key).await.is_err());

    let listed_after: Vec<Path> = store
        .list(Some(prefix_path))
        .map(|r| r.expect("list entry").location)
        .collect()
        .await;
    assert!(
        !listed_after.iter().any(|p| ends(p, &rel_copy)),
        "delete must remove object from recursive list results"
    );
}
