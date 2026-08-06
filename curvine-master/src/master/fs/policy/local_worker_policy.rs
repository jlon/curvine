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

use crate::master::fs::policy::{ChooseContext, RobinWorkerPolicy, WorkerPolicy};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{WorkerAddress, WorkerInfo};
use indexmap::IndexMap;
use std::collections::HashSet;

/// Local workers are preferred, and polling policies are used if there are no local workers
pub struct LocalWorkerPolicy {
    inner: RobinWorkerPolicy,
    local_only_workers: HashSet<String>,
}

impl Default for LocalWorkerPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalWorkerPolicy {
    pub fn new() -> Self {
        Self::with_local_only_workers(&[])
    }

    pub fn with_local_only_workers(local_only_workers: &[String]) -> Self {
        Self {
            inner: RobinWorkerPolicy::new(),
            local_only_workers: local_only_workers.iter().cloned().collect(),
        }
    }

    fn exclude_local_only_workers(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        excluded: &mut HashSet<u32>,
    ) {
        if self.local_only_workers.is_empty() {
            return;
        }

        for (id, worker) in workers {
            if self.local_only_workers.contains(&worker.address.hostname) {
                excluded.insert(*id);
            }
        }
    }
}

impl WorkerPolicy for LocalWorkerPolicy {
    fn choose(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        mut ctx: ChooseContext,
    ) -> CommonResult<Vec<WorkerAddress>> {
        if workers.is_empty() {
            return err_box!("No workers available");
        }
        if ctx.replicas < 1 {
            return err_box!("The number of replicas cannot be 0");
        }

        let mut res = vec![];

        // step1: Detect whether the local worker exists
        for (id, worker) in workers {
            if !ctx.exclude_workers.contains(id)
                && worker.address.is_local(&ctx.client_host)
                && worker.available > ctx.block_size
                && worker.is_live()
            {
                res.push(worker.address.clone());
                ctx.exclude_workers.insert(*id);
                break;
            }
        }

        let replicas = ctx.replicas;
        if res.len() < replicas as usize {
            self.exclude_local_only_workers(workers, &mut ctx.exclude_workers);
            let remote_res = self.inner.choose(workers, ctx)?;
            for item in remote_res {
                res.push(item);
                if res.len() == replicas as usize {
                    break;
                }
            }
        }

        Ok(res)
    }

    fn choose_workers(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        count: Option<usize>,
        exclude_workers: Vec<u32>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        // This operation has no client hostname, so local-only workers must not
        // receive replication or other remote allocations.
        let mut excluded = HashSet::from_iter(exclude_workers);
        self.exclude_local_only_workers(workers, &mut excluded);
        self.inner
            .choose_workers(workers, count, excluded.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::fs::policy::ChooseContext;

    fn worker(id: u32, hostname: &str) -> WorkerInfo {
        WorkerInfo {
            address: WorkerAddress {
                worker_id: id,
                hostname: hostname.to_string(),
                ..Default::default()
            },
            capacity: 1024,
            available: 1024,
            ..Default::default()
        }
    }

    fn context(client_host: &str) -> ChooseContext {
        ChooseContext {
            replicas: 1,
            block_size: 1,
            storage_policy: Default::default(),
            client_host: client_host.to_string(),
            exclude_workers: HashSet::new(),
        }
    }

    #[test]
    fn local_only_workers_are_selected_only_for_matching_clients() {
        let workers = IndexMap::from([(1, worker(1, "node-a")), (2, worker(2, "node-b"))]);
        let policy = LocalWorkerPolicy::with_local_only_workers(&["node-a".to_string()]);

        let local = policy.choose(&workers, context("node-a")).unwrap();
        assert_eq!(local[0].worker_id, 1);

        let remote = policy.choose(&workers, context("node-c")).unwrap();
        assert_eq!(remote[0].worker_id, 2);

        let replication = policy.choose_workers(&workers, Some(1), vec![]).unwrap();
        assert_eq!(replication[0].worker_id, 2);
    }

    #[test]
    fn empty_local_only_workers_keep_remote_fallback() {
        let workers = IndexMap::from([(1, worker(1, "node-a"))]);
        let policy = LocalWorkerPolicy::new();

        let remote = policy.choose(&workers, context("node-b")).unwrap();
        assert_eq!(remote[0].worker_id, 1);

        let replication = policy.choose_workers(&workers, Some(1), vec![]).unwrap();
        assert_eq!(replication[0].worker_id, 1);
    }
}
