// Project:   dfe-fetcher
// File:      src/buffer/mod.rs
// Purpose:   Memory pressure detection and tiered sink re-exports
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Buffer management and memory pressure detection.
//!
//! Provides `BufferManager` for tracking in-flight bytes and detecting
//! memory pressure.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::config::BufferConfig;

/// Memory pressure level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    /// Below 50% of limit.
    Low,
    /// 50-80% of limit.
    Medium,
    /// At or above pressure threshold.
    High,
}

/// Tracks in-flight bytes and detects memory pressure.
pub struct BufferManager {
    total_bytes: AtomicU64,
    memory_limit: u64,
    pressure_threshold: f64,
    under_pressure: AtomicBool,
}

impl BufferManager {
    /// Create a new buffer manager.
    pub fn new(config: &BufferConfig) -> Self {
        let memory_limit = if config.memory_limit == 0 {
            // Auto-detect: 67% of available memory
            let sys = sysinfo::System::new_all();
            (sys.total_memory() as f64 * 0.67) as u64
        } else {
            config.memory_limit as u64
        };

        Self {
            total_bytes: AtomicU64::new(0),
            memory_limit,
            pressure_threshold: config.pressure_threshold,
            under_pressure: AtomicBool::new(false),
        }
    }

    /// Add bytes to tracking.
    #[inline]
    pub fn add_bytes(&self, n: u64) {
        let total = self.total_bytes.fetch_add(n, Ordering::Relaxed) + n;
        self.update_pressure(total);
    }

    /// Remove bytes from tracking (saturating — never wraps below zero).
    #[inline]
    pub fn remove_bytes(&self, n: u64) {
        let total = self
            .total_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(n))
            })
            .unwrap_or(0);
        // `total` is the *previous* value; compute the new value for pressure check
        self.update_pressure(total.saturating_sub(n));
    }

    /// Get current total bytes.
    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Get memory limit.
    #[inline]
    pub fn memory_limit(&self) -> u64 {
        self.memory_limit
    }

    /// Get current pressure level.
    pub fn pressure(&self) -> MemoryPressure {
        let ratio = self.total_bytes() as f64 / self.memory_limit as f64;
        if ratio >= self.pressure_threshold {
            MemoryPressure::High
        } else if ratio >= 0.5 {
            MemoryPressure::Medium
        } else {
            MemoryPressure::Low
        }
    }

    /// Check if under memory pressure.
    #[inline]
    pub fn is_under_pressure(&self) -> bool {
        self.under_pressure.load(Ordering::Relaxed)
    }

    /// Update pressure state based on current total.
    fn update_pressure(&self, total: u64) {
        let ratio = total as f64 / self.memory_limit as f64;
        self.under_pressure
            .store(ratio >= self.pressure_threshold, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_manager_pressure() {
        let config = BufferConfig {
            memory_limit: 1000,
            pressure_threshold: 0.8,
        };
        let manager = BufferManager::new(&config);

        assert_eq!(manager.pressure(), MemoryPressure::Low);
        assert!(!manager.is_under_pressure());

        manager.add_bytes(500);
        assert_eq!(manager.pressure(), MemoryPressure::Medium);

        manager.add_bytes(400);
        assert_eq!(manager.pressure(), MemoryPressure::High);
        assert!(manager.is_under_pressure());

        manager.remove_bytes(500);
        assert!(!manager.is_under_pressure());
    }
}
