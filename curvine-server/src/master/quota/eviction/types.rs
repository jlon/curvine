// English comments only in code blocks

use curvine_common::conf::ClusterConf;

#[derive(Clone, Copy, Debug)]
pub enum EvictionMode {
    FreeFile,
    DeleteFile,
}

#[derive(Clone, Debug)]
pub struct EvictionConf {
    pub enable_prequota_eviction: bool,
    pub eviction_mode: EvictionMode,
    pub policy: EvictionPolicy,
    pub high_watermark: f64,
    pub low_watermark: f64,
    pub target_margin_ratio: f64,
    pub candidate_scan_page: usize,
    pub max_evict_rate_bytes_per_s: i64,
    pub dry_run: bool,
}

impl EvictionConf {
    pub fn from_conf(conf: &ClusterConf) -> Self {
        let master_conf = &conf.master;

        // Parse eviction mode from string
        let eviction_mode = match master_conf.eviction_mode.as_str() {
            "DeleteFile" => EvictionMode::DeleteFile,
            _ => EvictionMode::FreeFile,
        };

        // Parse eviction policy from string (case-insensitive)
        let policy = match master_conf.eviction_policy.to_lowercase().as_str() {
            "lru" => EvictionPolicy::Lru,
            "lfu" => EvictionPolicy::Lfu,
            "arc" => EvictionPolicy::Arc,
            _ => EvictionPolicy::Lru,
        };

        Self {
            enable_prequota_eviction: master_conf.enable_prequota_eviction,
            eviction_mode,
            policy,
            high_watermark: master_conf.eviction_high_watermark,
            low_watermark: master_conf.eviction_low_watermark,
            target_margin_ratio: master_conf.eviction_target_margin_ratio,
            candidate_scan_page: master_conf.eviction_candidate_scan_page,
            max_evict_rate_bytes_per_s: master_conf.eviction_max_rate_bytes_per_s,
            dry_run: master_conf.eviction_dry_run,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct EvictPlan {
    pub quota_root_inode_id: i64,
    pub trigger_used: i64,
    pub quota_size: i64,
    pub target_free_bytes: i64,
}

#[derive(Clone, Copy, Debug)]
pub enum EvictionPolicy {
    Lru,
    Lfu,
    Arc,
}
