// English comments only in code blocks

use super::types::{EvictPlan, EvictionConf};

pub trait EvictionDetector: Send + Sync {
    fn maybe_create_plan(&self, used: i64, quota: i64, quota_root: i64) -> Option<EvictPlan>;
}

pub struct WatermarkDetector {
    pub(crate) conf: EvictionConf,
}

impl WatermarkDetector {
    pub fn new(conf: EvictionConf) -> Self {
        Self { conf }
    }
}

impl EvictionDetector for WatermarkDetector {
    fn maybe_create_plan(&self, used: i64, quota: i64, quota_root: i64) -> Option<EvictPlan> {
        if quota <= 0 {
            return None;
        }

        let usage_ratio = used as f64 / quota as f64;
        if usage_ratio < self.conf.high_watermark {
            return None;
        }

        let target_ratio = self
            .conf
            .low_watermark
            .min(self.conf.high_watermark - self.conf.target_margin_ratio);
        let target_used = (target_ratio * quota as f64) as i64;
        let target_free_bytes = (used - target_used).max(0);

        Some(EvictPlan {
            quota_root_inode_id: quota_root,
            trigger_used: used,
            quota_size: quota,
            target_free_bytes,
        })
    }
}
