// Project:   dfe-fetcher
// File:      src/metrics/mod.rs
// Purpose:   Prometheus metrics for fetch operations
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Prometheus metrics for dfe-fetcher.
//!
//! Exposes counters and gauges for monitoring fetch operations,
//! source health, and delivery to Kafka.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// Metrics collector for dfe-fetcher.
#[derive(Debug)]
pub struct Metrics {
    // Fetch counters
    fetches_total: AtomicU64,
    fetches_success: AtomicU64,
    fetches_error: AtomicU64,
    records_fetched: AtomicU64,
    bytes_fetched: AtomicU64,

    // Delivery counters
    messages_sent_kafka: AtomicU64,
    messages_dlq: AtomicU64,

    // Extractor counters
    extractor_runs_total: AtomicU64,
    extractor_runs_success: AtomicU64,
    extractor_runs_error: AtomicU64,
    extractor_records_total: AtomicU64,

    // Gauges
    active_fetches: AtomicU64,
    active_extractors: AtomicU64,
    memory_used_bytes: AtomicU64,
    memory_limit_bytes: AtomicU64,

    // Rate tracking
    rate_window: RwLock<RateWindow>,
}

/// Sliding window for rate calculation.
#[derive(Debug)]
struct RateWindow {
    samples: Vec<(Instant, u64)>,
    window_size: Duration,
}

impl RateWindow {
    fn new(window_size: Duration) -> Self {
        Self {
            samples: Vec::with_capacity(60),
            window_size,
        }
    }

    fn add_sample(&mut self, value: u64) {
        let now = Instant::now();
        self.samples.push((now, value));

        // Remove old samples
        if let Some(cutoff) = now.checked_sub(self.window_size) {
            self.samples.retain(|(t, _)| *t > cutoff);
        }
    }

    fn rate_per_second(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }

        let Some(first) = self.samples.first() else {
            return 0.0;
        };
        let Some(last) = self.samples.last() else {
            return 0.0;
        };

        let duration = last.0.duration_since(first.0);
        if duration.is_zero() {
            return 0.0;
        }

        let delta = last.1.saturating_sub(first.1);
        delta as f64 / duration.as_secs_f64()
    }
}

impl Metrics {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        Self {
            fetches_total: AtomicU64::new(0),
            fetches_success: AtomicU64::new(0),
            fetches_error: AtomicU64::new(0),
            records_fetched: AtomicU64::new(0),
            bytes_fetched: AtomicU64::new(0),
            messages_sent_kafka: AtomicU64::new(0),
            messages_dlq: AtomicU64::new(0),
            extractor_runs_total: AtomicU64::new(0),
            extractor_runs_success: AtomicU64::new(0),
            extractor_runs_error: AtomicU64::new(0),
            extractor_records_total: AtomicU64::new(0),
            active_fetches: AtomicU64::new(0),
            active_extractors: AtomicU64::new(0),
            memory_used_bytes: AtomicU64::new(0),
            memory_limit_bytes: AtomicU64::new(0),
            rate_window: RwLock::new(RateWindow::new(Duration::from_secs(60))),
        }
    }

    // ==========================================================================
    // Fetch counters
    // ==========================================================================

    /// Increment total fetches counter.
    #[inline]
    pub fn inc_fetches_total(&self) {
        let count = self.fetches_total.fetch_add(1, Ordering::Relaxed) + 1;
        self.rate_window.write().add_sample(count);
    }

    /// Increment successful fetches counter.
    #[inline]
    pub fn inc_fetches_success(&self) {
        self.fetches_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment failed fetches counter.
    #[inline]
    pub fn inc_fetches_error(&self) {
        self.fetches_error.fetch_add(1, Ordering::Relaxed);
    }

    /// Add records fetched.
    #[inline]
    pub fn add_records_fetched(&self, count: u64) {
        self.records_fetched.fetch_add(count, Ordering::Relaxed);
    }

    /// Add bytes fetched.
    #[inline]
    pub fn add_bytes_fetched(&self, bytes: u64) {
        self.bytes_fetched.fetch_add(bytes, Ordering::Relaxed);
    }

    // ==========================================================================
    // Delivery counters
    // ==========================================================================

    /// Add messages sent to Kafka.
    #[inline]
    pub fn add_messages_sent_kafka(&self, count: u64) {
        self.messages_sent_kafka.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment DLQ messages counter.
    #[inline]
    pub fn inc_messages_dlq(&self) {
        self.messages_dlq.fetch_add(1, Ordering::Relaxed);
    }

    // ==========================================================================
    // Extractor counters
    // ==========================================================================

    /// Increment total extractor runs.
    #[inline]
    pub fn inc_extractor_runs_total(&self) {
        self.extractor_runs_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment successful extractor runs.
    #[inline]
    pub fn inc_extractor_runs_success(&self) {
        self.extractor_runs_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment failed extractor runs.
    #[inline]
    pub fn inc_extractor_runs_error(&self) {
        self.extractor_runs_error.fetch_add(1, Ordering::Relaxed);
    }

    /// Add records from extractors.
    #[inline]
    pub fn add_extractor_records(&self, count: u64) {
        self.extractor_records_total
            .fetch_add(count, Ordering::Relaxed);
    }

    // ==========================================================================
    // Gauges
    // ==========================================================================

    /// Increment active fetches.
    #[inline]
    pub fn inc_active_fetches(&self) {
        self.active_fetches.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active fetches (saturating — never wraps below zero).
    #[inline]
    pub fn dec_active_fetches(&self) {
        let _ = self
            .active_fetches
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }

    /// Increment active extractors.
    #[inline]
    pub fn inc_active_extractors(&self) {
        self.active_extractors.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active extractors (saturating — never wraps below zero).
    #[inline]
    pub fn dec_active_extractors(&self) {
        let _ = self
            .active_extractors
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }

    /// Set memory usage.
    #[inline]
    pub fn set_memory_usage(&self, used: u64, limit: u64) {
        self.memory_used_bytes.store(used, Ordering::Relaxed);
        self.memory_limit_bytes.store(limit, Ordering::Relaxed);
    }

    /// Get fetch rate per second.
    pub fn fetch_rate(&self) -> f64 {
        self.rate_window.read().rate_per_second()
    }

    /// Render metrics in Prometheus format.
    pub fn render(&self) -> String {
        let mut output = String::with_capacity(4096);

        // Fetch counters
        output.push_str("# HELP fetcher_fetches_total Total number of fetch operations\n");
        output.push_str("# TYPE fetcher_fetches_total counter\n");
        output.push_str(&format!(
            "fetcher_fetches_total {}\n",
            self.fetches_total.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP fetcher_fetches_success Successful fetch operations\n");
        output.push_str("# TYPE fetcher_fetches_success counter\n");
        output.push_str(&format!(
            "fetcher_fetches_success {}\n",
            self.fetches_success.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP fetcher_fetches_error Failed fetch operations\n");
        output.push_str("# TYPE fetcher_fetches_error counter\n");
        output.push_str(&format!(
            "fetcher_fetches_error {}\n",
            self.fetches_error.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP fetcher_records_fetched_total Total records fetched\n");
        output.push_str("# TYPE fetcher_records_fetched_total counter\n");
        output.push_str(&format!(
            "fetcher_records_fetched_total {}\n",
            self.records_fetched.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP fetcher_bytes_fetched_total Total bytes fetched\n");
        output.push_str("# TYPE fetcher_bytes_fetched_total counter\n");
        output.push_str(&format!(
            "fetcher_bytes_fetched_total {}\n",
            self.bytes_fetched.load(Ordering::Relaxed)
        ));

        // Delivery counters
        output.push_str("# HELP fetcher_messages_sent_kafka_total Messages delivered to Kafka\n");
        output.push_str("# TYPE fetcher_messages_sent_kafka_total counter\n");
        output.push_str(&format!(
            "fetcher_messages_sent_kafka_total {}\n",
            self.messages_sent_kafka.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP fetcher_messages_dlq_total Messages sent to DLQ\n");
        output.push_str("# TYPE fetcher_messages_dlq_total counter\n");
        output.push_str(&format!(
            "fetcher_messages_dlq_total {}\n",
            self.messages_dlq.load(Ordering::Relaxed)
        ));

        // Extractor counters
        output.push_str("# HELP fetcher_extractor_runs_total Total extractor runs\n");
        output.push_str("# TYPE fetcher_extractor_runs_total counter\n");
        output.push_str(&format!(
            "fetcher_extractor_runs_total {}\n",
            self.extractor_runs_total.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP fetcher_extractor_runs_success Successful extractor runs\n");
        output.push_str("# TYPE fetcher_extractor_runs_success counter\n");
        output.push_str(&format!(
            "fetcher_extractor_runs_success {}\n",
            self.extractor_runs_success.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP fetcher_extractor_runs_error Failed extractor runs\n");
        output.push_str("# TYPE fetcher_extractor_runs_error counter\n");
        output.push_str(&format!(
            "fetcher_extractor_runs_error {}\n",
            self.extractor_runs_error.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP fetcher_extractor_records_total Total records from extractors\n");
        output.push_str("# TYPE fetcher_extractor_records_total counter\n");
        output.push_str(&format!(
            "fetcher_extractor_records_total {}\n",
            self.extractor_records_total.load(Ordering::Relaxed)
        ));

        // Gauges
        output.push_str("# HELP fetcher_active_fetches Current active fetch operations\n");
        output.push_str("# TYPE fetcher_active_fetches gauge\n");
        output.push_str(&format!(
            "fetcher_active_fetches {}\n",
            self.active_fetches.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP fetcher_active_extractors Current running extractors\n");
        output.push_str("# TYPE fetcher_active_extractors gauge\n");
        output.push_str(&format!(
            "fetcher_active_extractors {}\n",
            self.active_extractors.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP fetcher_memory_used_bytes Current memory usage\n");
        output.push_str("# TYPE fetcher_memory_used_bytes gauge\n");
        output.push_str(&format!(
            "fetcher_memory_used_bytes {}\n",
            self.memory_used_bytes.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP fetcher_memory_limit_bytes Memory limit\n");
        output.push_str("# TYPE fetcher_memory_limit_bytes gauge\n");
        output.push_str(&format!(
            "fetcher_memory_limit_bytes {}\n",
            self.memory_limit_bytes.load(Ordering::Relaxed)
        ));

        // Rate
        output.push_str("# HELP fetcher_fetch_rate_per_second Current fetch rate\n");
        output.push_str("# TYPE fetcher_fetch_rate_per_second gauge\n");
        output.push_str(&format!(
            "fetcher_fetch_rate_per_second {:.2}\n",
            self.fetch_rate()
        ));

        output
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_counters() {
        let metrics = Metrics::new();

        metrics.inc_fetches_total();
        metrics.inc_fetches_total();
        metrics.inc_fetches_success();

        let output = metrics.render();
        assert!(output.contains("fetcher_fetches_total 2"));
        assert!(output.contains("fetcher_fetches_success 1"));
    }

    #[test]
    fn test_metrics_extractor_counters() {
        let metrics = Metrics::new();

        metrics.inc_extractor_runs_total();
        metrics.inc_extractor_runs_success();
        metrics.add_extractor_records(42);

        let output = metrics.render();
        assert!(output.contains("fetcher_extractor_runs_total 1"));
        assert!(output.contains("fetcher_extractor_runs_success 1"));
        assert!(output.contains("fetcher_extractor_records_total 42"));
    }

    #[test]
    fn test_memory_metrics() {
        let metrics = Metrics::new();

        metrics.set_memory_usage(500_000, 1_000_000);

        let output = metrics.render();
        assert!(output.contains("fetcher_memory_used_bytes 500000"));
        assert!(output.contains("fetcher_memory_limit_bytes 1000000"));
    }
}
