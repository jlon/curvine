// English comments only in code blocks

// Executor now accepts inode_ids only
use log::warn;

use crate::master::meta::inode::ttl::ttl_executor::InodeTtlExecutor;

use super::EvictionMode;

pub trait EvictionExecutor: Send + Sync {
    fn execute(&self, mode: EvictionMode, inode_ids: &[i64]);
}

pub struct FileEvictionExecutor {
    pub(crate) ttl_executor: InodeTtlExecutor,
}

impl FileEvictionExecutor {
    pub fn new(ttl_executor: InodeTtlExecutor) -> Self {
        Self { ttl_executor }
    }
}

impl EvictionExecutor for FileEvictionExecutor {
    fn execute(&self, mode: EvictionMode, inode_ids: &[i64]) {
        for inode_id_i64 in inode_ids {
            let inode_id = *inode_id_i64 as u64;
            let res = match mode {
                EvictionMode::FreeFile => self.ttl_executor.free_inode(inode_id),
                EvictionMode::DeleteFile => self.ttl_executor.delete_inode(inode_id),
            };
            if let Err(e) = res {
                warn!(
                    "prequota-evict: executor failed for inode_id={}, err={}",
                    inode_id, e
                );
            }
        }
    }
}
