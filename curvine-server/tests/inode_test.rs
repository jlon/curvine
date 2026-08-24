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

use curvine_core_error::CommonResult;
use curvine_model::{BlockLocation, DirectoryAttributeDelta, DirectoryAttributes, ListOptions};
use curvine_rocksdb::DBConf;
use curvine_runtime::common::Utils;
use curvine_server::master::meta::inode::ttl::TtlBucketList;
use curvine_server::master::meta::inode::{Inode, InodeDir, InodeFile, InodeView, ROOT_INODE_ID};
use curvine_server::master::meta::store::{InodeStore, RocksInodeStore};
use curvine_server::master::meta::FsDir;
use curvine_server::master::Master;
use std::sync::Arc;

#[test]
fn test_inode_dir_add_children_and_sort_alphabetically() {
    let mut root = InodeDir::new(0, 0);
    println!("{:?}", root);
    root.add_file_child("aa1", InodeFile::new(1, 0)).unwrap();
    root.add_file_child("aa3", InodeFile::new(2, 0)).unwrap();
    root.add_file_child("aa2", InodeFile::new(3, 0)).unwrap();
    root.add_file_child("b", InodeFile::new(5, 0)).unwrap();

    root.print_child();

    let children = root.children_vec();

    assert_eq!(children.len(), 4);
    assert_eq!(children[0].name(), "aa1");
    assert_eq!(children[1].name(), "aa2");
    assert_eq!(children[2].name(), "aa3");
    assert_eq!(children[3].name(), "b");
}

#[test]
fn directory_times_never_regress() {
    let mut directory = InodeDir::new(1, 10);

    directory.update_mtime(20);
    directory.update_mtime(15);
    directory.update_ctime(25);
    directory.update_ctime(18);

    assert_eq!(directory.mtime(), 20);
    assert_eq!(directory.ctime(), 25);
}

#[test]
fn large_list_limit_is_bounded_by_sharded_directory_size() {
    let mut root = InodeDir::new(0, 0);
    for id in 0..257 {
        root.add_file_child(&format!("file-{id:04}"), InodeFile::new(id + 1, 0))
            .unwrap();
    }

    let entries = root.list_options(&ListOptions::with_limit(i32::MAX as usize));
    assert_eq!(entries.len(), 257);
}

#[test]
fn test_inode_path_components_parsing() {
    let p1 = "/a/b/c";
    let vec = InodeView::path_components(p1).unwrap();
    assert_eq!(vec.len(), 4);
    assert_eq!(vec[0], "");
    assert_eq!(vec[1], "a");
}

#[test]
fn test_rocks_inode_store_add_delete_child_and_iterator() -> CommonResult<()> {
    let conf = DBConf::default();
    let db = RocksInodeStore::new(conf, true)?;

    let mut batch = db.new_batch();
    batch.add_child(ROOT_INODE_ID, "c", 3)?;
    batch.add_child(ROOT_INODE_ID, "a1", 1)?;
    batch.add_child(ROOT_INODE_ID, "a2", 2)?;
    batch.commit()?;

    // Test iterator
    let iter = db.get_child_ids(ROOT_INODE_ID, None)?;
    let mut vec = vec![];
    for item in iter {
        vec.push(item?)
    }
    println!("vec = {:?}", vec);
    assert_eq!(vec, vec![1, 2, 3]);

    // Prefix iterator.
    let iter1 = db.get_child_ids(ROOT_INODE_ID, Some("a"))?;
    let mut vec1 = vec![];
    for item in iter1 {
        vec1.push(item.unwrap())
    }
    println!("vec = {:?}", vec1);
    assert_eq!(vec1, vec![1, 2]);

    let mut batch = db.new_batch();
    batch.delete_child(ROOT_INODE_ID, "a1")?;
    batch.commit()?;

    let iter = db.get_child_ids(ROOT_INODE_ID, None)?;
    let mut vec = vec![];
    for item in iter {
        vec.push(item?)
    }
    println!("vec = {:?}", vec);
    assert_eq!(vec, vec![2, 3]);

    Ok(())
}

#[test]
fn directory_attribute_merge_survives_store_reopen() -> CommonResult<()> {
    let conf = DBConf::new(Utils::test_sub_dir(format!(
        "inode-test/directory-attributes-{}",
        Utils::rand_str(6)
    )));
    let directory_id = 701;
    {
        let db = RocksInodeStore::new(conf.clone(), true)?;
        let mut batch = db.new_batch();
        batch.write_directory_attributes(directory_id, DirectoryAttributes::new(10, 10, 2))?;
        batch.merge_directory_attributes(directory_id, DirectoryAttributeDelta::for_child(15))?;
        batch.merge_directory_attributes(directory_id, DirectoryAttributeDelta::new(14, 18, -1))?;
        batch.commit()?;

        assert_eq!(
            db.get_directory_attributes(directory_id)?,
            Some(DirectoryAttributes::new(15, 18, 1))
        );
    }

    let db = RocksInodeStore::new(conf, false)?;
    assert_eq!(
        db.get_directory_attributes(directory_id)?,
        Some(DirectoryAttributes::new(15, 18, 1))
    );
    Ok(())
}

#[test]
fn directory_attribute_merge_initializes_missing_base_once() -> CommonResult<()> {
    let conf = DBConf::new(Utils::test_sub_dir(format!(
        "inode-test/directory-attributes-initial-{}",
        Utils::rand_str(6)
    )));
    let directory_id = 702;
    let base = DirectoryAttributes::new(10, 10, 2);
    let db = RocksInodeStore::new(conf, true)?;
    let mut batch = db.new_batch();
    batch.merge_directory_attributes(
        directory_id,
        DirectoryAttributeDelta::new(15, 15, 1).with_base(base),
    )?;
    batch.merge_directory_attributes(
        directory_id,
        DirectoryAttributeDelta::new(20, 20, 1).with_base(base),
    )?;
    batch.commit()?;

    assert_eq!(
        db.get_directory_attributes(directory_id)?,
        Some(DirectoryAttributes::new(20, 20, 4))
    );
    Ok(())
}

#[test]
fn directory_attribute_merge_rejects_conflicting_missing_bases() -> CommonResult<()> {
    let conf = DBConf::new(Utils::test_sub_dir(format!(
        "inode-test/directory-attributes-conflict-{}",
        Utils::rand_str(6)
    )));
    let directory_id = 703;
    let db = RocksInodeStore::new(conf, true)?;
    let mut batch = db.new_batch();
    batch.merge_directory_attributes(
        directory_id,
        DirectoryAttributeDelta::new(15, 15, 1).with_base(DirectoryAttributes::new(10, 10, 2)),
    )?;
    batch.merge_directory_attributes(
        directory_id,
        DirectoryAttributeDelta::new(20, 20, 1).with_base(DirectoryAttributes::new(11, 11, 2)),
    )?;
    if batch.commit().is_ok() {
        assert!(db.get_directory_attributes(directory_id).is_err());
    }
    Ok(())
}

#[test]
fn directory_attribute_base_is_created_when_a_new_directory_becomes_parent() -> CommonResult<()> {
    let conf = DBConf::new(Utils::test_sub_dir(format!(
        "inode-test/directory-attribute-create-{}",
        Utils::rand_str(6)
    )));
    let store = InodeStore::new(
        RocksInodeStore::new(conf, true)?,
        Arc::new(TtlBucketList::new(60_000)?),
    )?;
    store.initialize_root_directory_attributes()?;

    let root = FsDir::create_root();
    let parent = InodeView::new_dir("parent".to_string(), InodeDir::new(801, 10));
    let child = InodeView::new_dir("child".to_string(), InodeDir::new(802, 20));
    let file = InodeView::new_file("file".to_string(), InodeFile::new(803, 30));

    store.apply_add(&root, &parent, None)?;
    assert_eq!(store.store().get_directory_attributes(parent.id())?, None);

    store.apply_add(&parent, &child, None)?;
    assert_eq!(
        store.store().get_directory_attributes(parent.id())?,
        Some(DirectoryAttributes::new(20, 20, 3))
    );

    store.apply_add(&child, &file, None)?;
    assert_eq!(
        store.store().get_directory_attributes(child.id())?,
        Some(DirectoryAttributes::new(30, 30, 2))
    );
    Ok(())
}

#[test]
fn directory_attribute_migration_is_idempotent() -> CommonResult<()> {
    let conf = DBConf::new(Utils::test_sub_dir(format!(
        "inode-test/directory-attribute-migration-{}",
        Utils::rand_str(6)
    )));
    let store = InodeStore::new(
        RocksInodeStore::new(conf, true)?,
        Arc::new(TtlBucketList::new(60_000)?),
    )?;
    let root = FsDir::create_root();
    let directory = InodeView::new_dir("dir".to_string(), InodeDir::new(702, 20));

    {
        let mut batch = store.new_batch();
        batch.write_inode(&root)?;
        batch.write_inode(&directory)?;
        batch.add_child(ROOT_INODE_ID, "dir", directory.id())?;
        batch.commit()?;
    }

    store.migrate_directory_attributes()?;
    assert_eq!(
        store.store().get_directory_attributes(ROOT_INODE_ID)?,
        Some(DirectoryAttributes::new(0, 0, 2))
    );
    assert_eq!(
        store.store().get_directory_attributes(directory.id())?,
        Some(DirectoryAttributes::new(20, 20, 2))
    );

    {
        let mut batch = store.new_batch();
        batch
            .merge_directory_attributes(directory.id(), DirectoryAttributeDelta::new(30, 30, 1))?;
        batch.commit()?;
    }
    store.migrate_directory_attributes()?;
    assert_eq!(
        store.store().get_directory_attributes(directory.id())?,
        Some(DirectoryAttributes::new(30, 30, 3))
    );
    Ok(())
}

#[test]
fn test_inode_store_create_tree_removes_orphan_edges() -> CommonResult<()> {
    Master::init_test_metrics();

    let conf = DBConf::new(Utils::test_sub_dir(format!(
        "inode-test/orphan-edge-{}",
        Utils::rand_str(6)
    )));
    {
        let rocks = RocksInodeStore::new(conf.clone(), true)?;
        let store = InodeStore::new(
            rocks,
            Arc::new(TtlBucketList::new(60_000).expect("valid ttl bucket interval")),
        )?;
        let root = FsDir::create_root();
        let missing_inode_id = 2001;

        {
            let mut batch = store.new_batch();
            batch.write_inode(&root)?;
            batch.add_child(ROOT_INODE_ID, "missing", missing_inode_id)?;
            batch.commit()?;
        }

        let (_, restored) = store.create_tree()?;
        assert!(restored.get_child("missing").is_none());
    }

    let rocks = RocksInodeStore::new(conf, false)?;
    let edge_count = rocks.edges_iter(ROOT_INODE_ID)?.count();
    assert_eq!(edge_count, 0);

    Ok(())
}

#[test]
fn test_rocks_block_store_add_delete_location_operations() -> CommonResult<()> {
    let conf = DBConf::default();
    let db = RocksInodeStore::new(conf, true)?;

    let mut batch = db.new_batch();
    batch.add_location(101, &BlockLocation::with_id(1))?;
    batch.add_location(101, &BlockLocation::with_id(2))?;
    batch.add_location(101, &BlockLocation::with_id(3))?;
    batch.add_location(103, &BlockLocation::with_id(1))?;
    batch.commit()?;

    // Test to get all locations of block id
    let iter = db.get_locations(101)?;
    let mut vec = vec![];
    for item in iter {
        vec.push(item.worker_id)
    }
    println!("vec = {:?}", vec);
    assert_eq!(vec, vec![1, 2, 3]);

    let mut batch = db.new_batch();
    batch.delete_location(101, 2)?;
    batch.commit()?;

    let iter = db.get_locations(101)?;
    let mut vec = vec![];
    for item in iter {
        vec.push(item.worker_id)
    }
    println!("vec = {:?}", vec);
    assert_eq!(vec, vec![1, 3]);

    Ok(())
}

#[test]
fn test_delete_all_block_locations_for_specific_worker() -> CommonResult<()> {
    let conf = DBConf::default();
    let db = RocksInodeStore::new(conf, true)?;

    // Setup: Add locations for multiple blocks and workers
    let mut batch = db.new_batch();
    for block_id in 401..405 {
        for worker_id in 1..4 {
            batch.add_location(block_id, &BlockLocation::with_id(worker_id))?;
        }
    }
    batch.commit()?;

    // Test delete_locations for worker 2
    db.delete_locations(2)?;

    // Verify worker 2's locations are removed from CF_LOCATION
    let block_ids_worker2 = db.get_block_ids(2)?;
    assert_eq!(block_ids_worker2.len(), 0);

    // Verify worker 2's locations are removed from CF_BLOCK
    for block_id in 401..405 {
        let locations = db.get_locations(block_id)?;
        assert_eq!(locations.len(), 2); // Only workers 1 and 3 remain
        let worker_ids: Vec<u32> = locations.iter().map(|loc| loc.worker_id).collect();
        assert_eq!(worker_ids, vec![1, 3]);
    }

    // Verify other workers are unaffected
    for worker_id in [1, 3] {
        let block_ids = db.get_block_ids(worker_id)?;
        assert_eq!(block_ids.len(), 4);
    }

    Ok(())
}
