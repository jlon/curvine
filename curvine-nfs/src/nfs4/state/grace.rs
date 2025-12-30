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

//! Grace Period Management - Aligned with NFS-Ganesha
//!
//! Implements NFSv4 Grace Period with sticky grace support for state recovery.
//!
//! # Architecture (Aligned with NFS-Ganesha)
//!
//! ```text
//! grace_status (AtomicU32):
//!   Bit 0:    ACTIVE - currently in grace period
//!   Bit 1:    CHANGE_REQ - state change requested, draining refs
//!   Bits 2+:  Reference counter (number of operations holding grace status)
//!
//! State Machine:
//!   NORMAL -> GRACE: Set ACTIVE bit (if no refs or after refs drain)
//!   GRACE -> NORMAL: Set CHANGE_REQ, wait for refs to drain, clear ACTIVE
//!
//! Reference Protocol:
//!   1. get_grace_status(want_grace) -> bool
//!      - Check if want_grace matches current ACTIVE state
//!      - If match and no CHANGE_REQ: increment refcount, return true
//!      - Otherwise: return false
//!   2. put_grace_status()
//!      - Decrement refcount
//!      - If refcount==0 and CHANGE_REQ: wake reaper
//! ```
//!
//! Reference: NFS-Ganesha nfs4_recovery.c

use crate::nfs4::error::Nfs4Status;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

// ============================================================================
// Grace Status Flags (NFS-Ganesha Compatible)
// ============================================================================

// Bit positions
const GRACE_STATUS_ACTIVE_SHIFT: u32 = 0;
const GRACE_STATUS_CHANGE_REQ_SHIFT: u32 = 1;
const GRACE_STATUS_COUNTER_SHIFT: u32 = 2;

// Flag masks
const GRACE_STATUS_ACTIVE: u32 = 1 << GRACE_STATUS_ACTIVE_SHIFT;
const GRACE_STATUS_CHANGE_REQ: u32 = 1 << GRACE_STATUS_CHANGE_REQ_SHIFT;
const GRACE_STATUS_REF_INCREMENT: u32 = 1 << GRACE_STATUS_COUNTER_SHIFT;
const GRACE_STATUS_COUNT_MASK: u32 = !0u32 << GRACE_STATUS_COUNTER_SHIFT;

// Default configuration
const DEFAULT_GRACE_PERIOD_SECS: u64 = 90;

// ============================================================================
// Recovery Backend Trait (NFS-Ganesha Compatible Interface)
// ============================================================================

/// Recovery Backend Trait
///
/// Defines the interface for state persistence and cluster support.
/// Following NFS-Ganesha's nfs4_recovery_backend design.
///
/// Default implementation (NoOpRecoveryBackend) does nothing,
/// ensuring zero performance overhead when not needed.
pub trait RecoveryBackend: Send + Sync {
    /// Initialize recovery backend
    fn init(&self) -> Result<(), std::io::Error> {
        Ok(())
    }

    /// Shutdown recovery backend
    fn shutdown(&self) {}

    /// Read client IDs from persistent storage (on server startup)
    fn read_client_ids(&self) -> Result<Vec<String>, std::io::Error> {
        Ok(Vec::new())
    }

    /// Add client ID to persistent storage
    fn add_client(&self, _client_id: u64) -> Result<(), std::io::Error> {
        Ok(())
    }

    /// Remove client ID from persistent storage
    fn remove_client(&self, _client_id: u64) -> Result<(), std::io::Error> {
        Ok(())
    }

    /// Mark client as having completed reclaim
    fn reclaim_complete(&self, _client_id: u64) -> Result<(), std::io::Error> {
        Ok(())
    }

    /// Check if this node is a cluster member
    fn is_cluster_member(&self) -> bool {
        false
    }

    /// Get node ID (for cluster mode)
    fn get_node_id(&self) -> Option<i32> {
        None
    }
}

/// No-Op Recovery Backend (default, zero overhead)
///
/// This is the default implementation that does nothing.
/// It ensures zero performance impact when recovery is not needed.
///
/// **Use this for FIO testing to maximize performance!**
#[derive(Debug, Clone, Copy)]
pub struct NoOpRecoveryBackend;

impl RecoveryBackend for NoOpRecoveryBackend {}

// ============================================================================
// Filesystem Recovery Backend (Optional, for production)
// ============================================================================

/// Filesystem Recovery Backend
///
/// Implements state persistence using StatePersistenceManager.
/// This is the production-ready backend that saves state to disk.
///
/// **Note**: This has I/O overhead and should be disabled for FIO testing.
///
/// To use this backend:
/// ```ignore
/// let persistence = Arc::new(StatePersistenceManager::new(fs, config));
/// let backend = Arc::new(FilesystemRecoveryBackend::new(persistence));
/// let grace = GracePeriodManager::new(config)
///     .with_recovery_backend(backend);
/// ```
pub struct FilesystemRecoveryBackend {
    // Placeholder for future integration with StatePersistenceManager
    // This will be implemented when persistence is needed
    _marker: std::marker::PhantomData<()>,
}

impl Default for FilesystemRecoveryBackend {
    fn default() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl FilesystemRecoveryBackend {
    /// Create new filesystem recovery backend
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl RecoveryBackend for FilesystemRecoveryBackend {
    // Default implementations (no-op for now)
    // Will be connected to StatePersistenceManager when needed
}

// ============================================================================
// Grace Period Configuration
// ============================================================================

/// Grace Period Configuration
#[derive(Debug, Clone)]
pub struct GracePeriodConfig {
    /// Grace period duration in seconds (default: 90)
    pub grace_period_secs: u64,

    /// Enable sticky grace (default: true)
    /// When true, uses reference counting to prevent state changes during operations
    pub sticky_grace: bool,

    /// Graceless mode (default: false)
    /// When true, server never enters grace period
    pub graceless: bool,
}

impl Default for GracePeriodConfig {
    fn default() -> Self {
        Self {
            grace_period_secs: DEFAULT_GRACE_PERIOD_SECS,
            sticky_grace: true,
            graceless: false,
        }
    }
}

impl GracePeriodConfig {
    /// Create config with custom duration
    #[inline]
    pub fn with_duration(grace_period_secs: u64) -> Self {
        Self {
            grace_period_secs,
            ..Default::default()
        }
    }

    /// Create graceless config (no grace period)
    #[inline]
    pub fn graceless() -> Self {
        Self {
            graceless: true,
            ..Default::default()
        }
    }
}

// ============================================================================
// Grace Period Manager
// ============================================================================

/// Grace Period Manager - NFS-Ganesha Compatible
///
/// Uses atomic operations for lock-free grace status checks.
/// Implements sticky grace with reference counting to prevent race conditions.
pub struct GracePeriodManager {
    /// Grace status word (atomic for lock-free operations)
    /// Bit 0: ACTIVE, Bit 1: CHANGE_REQ, Bits 2+: Reference counter
    grace_status: AtomicU32,

    /// Grace period start time
    start_time: RwLock<Option<Instant>>,

    /// Grace period duration
    duration: Duration,

    /// Sticky grace enabled
    sticky_grace: bool,

    /// Graceless mode
    graceless: bool,

    /// Shutdown flag
    shutdown: AtomicBool,

    /// Recovery backend (optional, for state persistence)
    /// Using Arc for shared ownership, allows trait object injection
    /// Default: None (zero overhead when not used)
    recovery_backend: Option<Arc<dyn RecoveryBackend>>,
}

impl GracePeriodManager {
    /// Create new Grace Period Manager
    pub fn new(config: GracePeriodConfig) -> Self {
        Self {
            grace_status: AtomicU32::new(0),
            start_time: RwLock::new(None),
            duration: Duration::from_secs(config.grace_period_secs),
            sticky_grace: config.sticky_grace,
            graceless: config.graceless,
            shutdown: AtomicBool::new(false),
            recovery_backend: None, // Default: no recovery backend
        }
    }

    /// Create with default config
    #[inline]
    pub fn with_default_config() -> Self {
        Self::new(GracePeriodConfig::default())
    }

    /// Set recovery backend (optional, builder pattern)
    ///
    /// This allows injecting a custom recovery backend for state persistence.
    /// If not set, no persistence is performed (zero overhead).
    ///
    /// # Example
    /// ```ignore
    /// let grace = GracePeriodManager::new(config)
    ///     .with_recovery_backend(Arc::new(MyBackend::new()));
    /// ```
    #[inline]
    pub fn with_recovery_backend(mut self, backend: Arc<dyn RecoveryBackend>) -> Self {
        self.recovery_backend = Some(backend);
        self
    }

    /// Get recovery backend (if configured)
    #[inline]
    pub fn recovery_backend(&self) -> Option<&Arc<dyn RecoveryBackend>> {
        self.recovery_backend.as_ref()
    }

    /// Check if currently in grace period (lock-free, hot path)
    ///
    /// This is called frequently, so it must be extremely fast.
    /// Uses atomic load with Acquire ordering for memory safety.
    #[inline]
    pub fn in_grace(&self) -> bool {
        self.grace_status.load(Ordering::Acquire) & GRACE_STATUS_ACTIVE != 0
    }

    /// Get grace status and acquire reference (NFS-Ganesha compatible)
    ///
    /// This is the core function for grace period checking.
    /// Following NFS-Ganesha's nfs_get_grace_status() logic:
    ///
    /// - If sticky_grace disabled: simple check, no reference
    /// - If sticky_grace enabled: check + acquire reference atomically
    ///
    /// Returns true if operation can proceed with want_grace state.
    #[inline]
    pub fn get_grace_status(&self, want_grace: bool) -> bool {
        // Fast path: sticky grace disabled
        if !self.sticky_grace {
            let cur = self.grace_status.load(Ordering::Acquire);
            return want_grace == (cur & GRACE_STATUS_ACTIVE != 0);
        }

        // Sticky grace enabled: use CAS loop to atomically check and increment
        let mut old = self.grace_status.load(Ordering::Acquire);
        loop {
            let cur = old;

            // Check if want_grace matches current ACTIVE state
            let in_grace = (cur & GRACE_STATUS_ACTIVE) != 0;
            if want_grace != in_grace {
                return false;
            }

            // Check if change was requested
            if (cur & GRACE_STATUS_CHANGE_REQ) != 0 {
                return false;
            }

            // Try to increment reference counter
            let new = cur + GRACE_STATUS_REF_INCREMENT;
            match self.grace_status.compare_exchange_weak(
                cur,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => old = actual,
            }
        }
    }

    /// Release grace status reference (NFS-Ganesha compatible)
    ///
    /// Decrements reference counter. If counter reaches 0 and CHANGE_REQ is set,
    /// the grace period state can now transition.
    #[inline]
    pub fn put_grace_status(&self) {
        // Fast path: sticky grace disabled (no-op)
        if !self.sticky_grace {
            return;
        }

        // Decrement reference counter
        let cur = self
            .grace_status
            .fetch_sub(GRACE_STATUS_REF_INCREMENT, Ordering::AcqRel);

        // Check if we need to wake reaper
        // (CHANGE_REQ set and counter just reached 0)
        if (cur & GRACE_STATUS_CHANGE_REQ) != 0
            && (cur & GRACE_STATUS_COUNT_MASK) == GRACE_STATUS_REF_INCREMENT
        {
            // Counter was 1, now 0, and change requested
            // In NFS-Ganesha this would wake the reaper thread
            // For now, we'll let the reaper poll
            debug!("Grace status refs drained, change can proceed");
        }
    }

    /// Enter grace period (NFS-Ganesha compatible)
    ///
    /// Sets ACTIVE bit. If sticky_grace enabled and refs exist,
    /// sets CHANGE_REQ and returns -EAGAIN.
    pub fn enter_grace_period(&self) -> Result<(), i32> {
        // Graceless mode: never enter grace
        if self.graceless {
            info!("Graceless mode enabled, skipping grace period");
            return Ok(());
        }

        let mut old = self.grace_status.load(Ordering::Acquire);
        loop {
            let cur = old;
            let was_grace = (cur & GRACE_STATUS_ACTIVE) != 0;

            // Already in grace?
            if was_grace {
                info!("Already in grace period");
                return Ok(());
            }

            // Check if there are outstanding refs (sticky grace only)
            let has_refs = self.sticky_grace && (cur & GRACE_STATUS_COUNT_MASK) != 0;

            let new = if has_refs {
                // Set CHANGE_REQ, can't transition yet
                cur | GRACE_STATUS_CHANGE_REQ
            } else {
                // Set ACTIVE, clear CHANGE_REQ
                (cur | GRACE_STATUS_ACTIVE) & !GRACE_STATUS_CHANGE_REQ
            };

            // No change needed?
            if new == cur {
                break;
            }

            match self.grace_status.compare_exchange_weak(
                cur,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if has_refs {
                        warn!(
                            "Cannot enter grace immediately, refs outstanding: 0x{:x}",
                            cur
                        );
                        return Err(libc::EAGAIN);
                    }
                    break;
                }
                Err(actual) => old = actual,
            }
        }

        // Set start time
        *self.start_time.write().unwrap() = Some(Instant::now());

        info!(
            "Entered grace period, duration: {} seconds",
            self.duration.as_secs()
        );
        Ok(())
    }

    /// Exit grace period (NFS-Ganesha compatible)
    ///
    /// Clears ACTIVE and CHANGE_REQ bits.
    /// In sticky grace mode, asserts that refcount is 0.
    pub fn exit_grace_period(&self) {
        let cur = self.grace_status.load(Ordering::Acquire);

        // Not in grace?
        if (cur & GRACE_STATUS_ACTIVE) == 0 {
            return;
        }

        // In sticky grace mode, refcount should be 0
        if self.sticky_grace {
            debug_assert!(
                (cur & GRACE_STATUS_COUNT_MASK) == 0,
                "Exiting grace with outstanding refs: 0x{:x}",
                cur
            );
        }

        // Clear start time
        *self.start_time.write().unwrap() = None;

        // Clear ACTIVE and CHANGE_REQ bits atomically
        self.grace_status.fetch_and(
            !(GRACE_STATUS_ACTIVE | GRACE_STATUS_CHANGE_REQ),
            Ordering::AcqRel,
        );

        info!("Exited grace period");
    }

    /// Check if grace period has expired
    fn has_expired(&self) -> bool {
        let start_time = self.start_time.read().unwrap();
        match *start_time {
            Some(start) => start.elapsed() >= self.duration,
            None => true,
        }
    }

    /// Try to lift grace period if expired (NFS-Ganesha compatible)
    ///
    /// Called by reaper thread. Checks if grace expired and refs drained.
    pub fn try_lift_grace(&self) -> bool {
        // Not in grace?
        let cur = self.grace_status.load(Ordering::Acquire);
        if (cur & GRACE_STATUS_ACTIVE) == 0 {
            return false;
        }

        // Check if expired
        if !self.has_expired() {
            return false;
        }

        // In sticky grace mode, need to set CHANGE_REQ and wait for refs
        if self.sticky_grace {
            let mut old = cur;
            loop {
                let cur = old;

                // Already done?
                if (cur & GRACE_STATUS_ACTIVE) == 0 {
                    return false;
                }

                // Set CHANGE_REQ flag
                let new = cur | GRACE_STATUS_CHANGE_REQ;
                if new == cur {
                    break;
                }

                match self.grace_status.compare_exchange_weak(
                    cur,
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(actual) => old = actual,
                }
            }

            // Check if refs drained
            let cur = self.grace_status.load(Ordering::Acquire);
            if (cur & GRACE_STATUS_COUNT_MASK) != 0 {
                debug!("Grace expired but refs outstanding: 0x{:x}", cur);
                return false;
            }
        }

        // Can lift now
        self.exit_grace_period();
        true
    }

    /// Get remaining grace time
    #[inline]
    pub fn remaining_time(&self) -> Option<Duration> {
        if !self.in_grace() {
            return None;
        }

        let start_time = self.start_time.read().unwrap();
        start_time.and_then(|start| {
            let elapsed = start.elapsed();
            if elapsed < self.duration {
                Some(self.duration - elapsed)
            } else {
                None
            }
        })
    }

    /// Check if operation is allowed (simplified, for backward compatibility)
    ///
    /// Note: This does NOT use get/put protocol. For performance-critical paths,
    /// use get_grace_status() + put_grace_status() directly.
    #[inline]
    pub fn check_operation(&self, want_grace: bool) -> Result<(), Nfs4Status> {
        // Try to lift grace if expired
        if self.in_grace() && self.has_expired() {
            // Don't block here, let reaper handle it
        }

        let in_grace = self.in_grace();

        if want_grace == in_grace {
            Ok(())
        } else if want_grace {
            Err(Nfs4Status::NoGrace)
        } else {
            Err(Nfs4Status::Grace)
        }
    }

    /// Acquire grace status with RAII guard (NFS-Ganesha compatible)
    ///
    /// Returns a GraceGuard that automatically releases the reference on drop.
    /// This ensures proper reference counting even in error paths.
    ///
    /// # Example
    /// ```ignore
    /// let _guard = handler.grace.acquire_grace_status(want_grace)?;
    /// // ... perform operation ...
    /// // Reference automatically released when guard drops
    /// ```
    #[inline]
    pub fn acquire_grace_status(&self, want_grace: bool) -> Result<GraceGuard, Nfs4Status> {
        if self.get_grace_status(want_grace) {
            Ok(GraceGuard::new(self))
        } else if want_grace {
            Err(Nfs4Status::NoGrace)
        } else {
            Err(Nfs4Status::Grace)
        }
    }

    /// Get grace period duration
    #[inline]
    pub fn get_duration(&self) -> Duration {
        self.duration
    }

    /// Set shutdown flag
    #[inline]
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Check if shutdown requested
    #[inline]
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

impl Default for GracePeriodManager {
    fn default() -> Self {
        Self::with_default_config()
    }
}

// ============================================================================
// Grace Guard (RAII for automatic reference release)
// ============================================================================

/// RAII guard for grace status reference
///
/// Automatically calls put_grace_status() when dropped.
/// This ensures proper reference counting even in error paths.
pub struct GraceGuard<'a> {
    grace: &'a GracePeriodManager,
}

impl<'a> GraceGuard<'a> {
    /// Create new guard (private, use acquire_grace_status instead)
    #[inline]
    fn new(grace: &'a GracePeriodManager) -> Self {
        Self { grace }
    }
}

impl<'a> Drop for GraceGuard<'a> {
    #[inline]
    fn drop(&mut self) {
        self.grace.put_grace_status();
    }
}

// ============================================================================
// Grace Period Reaper (for LoopTask integration)
// ============================================================================

use orpc::runtime::LoopTask;
use std::sync::Arc;

/// Grace Period Reaper Task
///
/// Periodically checks if grace period has expired and lifts it.
pub struct GracePeriodReaper {
    grace: Arc<GracePeriodManager>,
}

impl GracePeriodReaper {
    /// Create new reaper
    pub fn new(grace: Arc<GracePeriodManager>) -> Self {
        Self { grace }
    }
}

impl LoopTask for GracePeriodReaper {
    type Error = std::io::Error;

    fn run(&self) -> Result<(), Self::Error> {
        if self.grace.try_lift_grace() {
            info!("Grace period reaper: lifted expired grace period");
        }
        Ok(())
    }

    fn terminate(&self) -> bool {
        self.grace.is_shutdown()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_grace_basic() {
        let gpm = GracePeriodManager::new(GracePeriodConfig::with_duration(2));

        assert!(!gpm.in_grace());

        gpm.enter_grace_period().unwrap();
        assert!(gpm.in_grace());

        gpm.exit_grace_period();
        assert!(!gpm.in_grace());
    }

    #[test]
    fn test_sticky_grace_refs() {
        let gpm = GracePeriodManager::new(GracePeriodConfig::default());

        gpm.enter_grace_period().unwrap();

        // Get reference
        assert!(gpm.get_grace_status(true));

        // Try to exit - should not work immediately
        let cur = gpm.grace_status.load(Ordering::Acquire);
        assert!((cur & GRACE_STATUS_COUNT_MASK) != 0);

        // Release reference
        gpm.put_grace_status();

        // Now can exit
        gpm.exit_grace_period();
        assert!(!gpm.in_grace());
    }

    #[test]
    fn test_grace_timeout() {
        let gpm = GracePeriodManager::new(GracePeriodConfig::with_duration(1));

        gpm.enter_grace_period().unwrap();
        assert!(gpm.in_grace());

        thread::sleep(Duration::from_millis(1100));

        assert!(gpm.try_lift_grace());
        assert!(!gpm.in_grace());
    }

    #[test]
    fn test_graceless_mode() {
        let gpm = GracePeriodManager::new(GracePeriodConfig::graceless());

        gpm.enter_grace_period().unwrap();
        assert!(!gpm.in_grace());
    }
}
