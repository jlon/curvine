// English comments only in code blocks
//! Pre-Quota Eviction module
//! Split into clear submodules similar to TTL architecture:
//! - types.rs: basic types and configuration
//! - detector.rs: watermark detector
//! - evictor.rs: LRU-based evictor
//! - executor.rs: reuse InodeTtlExecutor for execution
//! - manager.rs: orchestration and background worker

pub mod detector;
pub mod evictor;
pub mod executor;
pub mod types;

pub use types::{EvictPlan, EvictionConf, EvictionMode};
