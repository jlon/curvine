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

use crate::master::meta::store::RocksInodeStore;
use orpc::{err_box, CommonResult};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::Deref;
use std::sync::{Arc, Weak};

pub struct RocksStoreHandle {
    current: RwLock<Weak<RocksInodeStore>>,
}

impl RocksStoreHandle {
    pub fn new(store: &Arc<RocksInodeStore>) -> Self {
        Self {
            current: RwLock::new(Arc::downgrade(store)),
        }
    }

    pub fn read(&self) -> CommonResult<RocksStoreReadGuard<'_>> {
        let guard = self.current.read();
        let store = match guard.upgrade() {
            Some(store) => store,
            None => return err_box!("metadata store is not available"),
        };
        Ok(RocksStoreReadGuard {
            store,
            _guard: guard,
        })
    }

    pub fn write(&self) -> RocksStoreWriteGuard<'_> {
        RocksStoreWriteGuard {
            guard: self.current.write(),
        }
    }
}

pub struct RocksStoreReadGuard<'a> {
    store: Arc<RocksInodeStore>,
    _guard: RwLockReadGuard<'a, Weak<RocksInodeStore>>,
}

impl Deref for RocksStoreReadGuard<'_> {
    type Target = RocksInodeStore;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

pub struct RocksStoreWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, Weak<RocksInodeStore>>,
}

impl RocksStoreWriteGuard<'_> {
    pub fn publish(&mut self, store: &Arc<RocksInodeStore>) {
        *self.guard = Arc::downgrade(store);
    }
}
