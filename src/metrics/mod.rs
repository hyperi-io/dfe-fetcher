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
//!
//! ## Dual-emit architecture
//!
//! Each metric exists as both a local [`AtomicU64`] field (for fast hot-path
//! reads like `pipeline.is_ready()`) **and** as a `metrics` crate emission
//! (so [`MetricsManager`](hyperi_rustlib::metrics::MetricsManager) can render
//! the full Prometheus text format).
//!
//! When [`Metrics::with_dfe()`] is used, fetcher-specific metrics are
//! described and emitted through the `metrics` crate global recorder,
//! alongside the standard DFE metrics from rustlib [`DfeMetrics`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hyperi_rustlib::metrics::DfeMetrics;
use hyperi_rustlib::scaling::RateWindow;

/// Metrics collector for dfe-fetcher.
///
/// Maintains local atomic counters for fast hot-path access and optionally
/// dual-emits to the `metrics` crate global recorder (via rustlib
/// [`DfeMetrics`] for standard DFE metrics, and direct `metrics::counter!` /
/// `metrics::gauge!` calls for fetcher-specific metrics).
pub struct Metrics {
    /// Optional rustlib DfeMetrics for dual-emit to global `metrics` recorder.
    dfe: Option<DfeMetrics>,
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

    // Backpressure / transport health
    transport_backpressured_total: AtomicU64,
    transport_send_errors_total: AtomicU64,
    transport_healthy: AtomicU64, // gauge: 1=healthy, 0=unhealthy

    // Filtering / extractor lifecycle
    records_filtered_total: AtomicU64,
    extractor_restart_exhausted_total: AtomicU64,
    cursor_writes_total: AtomicU64,
    cursor_write_failures_total: AtomicU64,

    // Pipeline / delivery
    pipeline_ready: AtomicU64,          // gauge: 1=ready, 0=backpressured
    records_delivered_total: AtomicU64, // counter

    // Gauges
    active_fetches: AtomicU64,
    active_extractors: AtomicU64,
    memory_used_bytes: AtomicU64,
    memory_limit_bytes: AtomicU64,

    // Rate tracking (rustlib RateWindow has internal RwLock)
    rate_window: RateWindow,
}

impl Metrics {
    /// Create a new metrics collector (without DfeMetrics dual-emit).
    ///
    /// Used in tests and contexts where no global `metrics` recorder is installed.
    pub fn new() -> Self {
        Self {
            dfe: None,
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
            transport_backpressured_total: AtomicU64::new(0),
            transport_send_errors_total: AtomicU64::new(0),
            transport_healthy: AtomicU64::new(1),
            records_filtered_total: AtomicU64::new(0),
            extractor_restart_exhausted_total: AtomicU64::new(0),
            cursor_writes_total: AtomicU64::new(0),
            cursor_write_failures_total: AtomicU64::new(0),
            pipeline_ready: AtomicU64::new(1),
            records_delivered_total: AtomicU64::new(0),
            active_fetches: AtomicU64::new(0),
            active_extractors: AtomicU64::new(0),
            memory_used_bytes: AtomicU64::new(0),
            memory_limit_bytes: AtomicU64::new(0),
            rate_window: RateWindow::new(Duration::from_secs(60)),
        }
    }

    /// Create a new metrics collector with DfeMetrics dual-emit enabled.
    ///
    /// Registers standard DFE metric descriptions **and** fetcher-specific
    /// metric descriptions with the global `metrics` recorder. Use in
    /// production where [`MetricsManager`](hyperi_rustlib::metrics::MetricsManager)
    /// is (or will be) installed.
    pub fn with_dfe() -> Self {
        // Register fetcher-specific metrics with the global recorder.
        // These are NOT part of DfeMetrics (which covers standard DFE metrics
        // shared across receiver/loader/engine) — they are fetcher-only.
        metrics::describe_counter!(
            "dfe_fetcher_fetches_total",
            "Total number of fetch operations"
        );
        metrics::describe_counter!(
            "dfe_fetcher_fetches_success_total",
            "Successful fetch operations"
        );
        metrics::describe_counter!("dfe_fetcher_fetches_error_total", "Failed fetch operations");
        metrics::describe_counter!("dfe_fetcher_bytes_received_total", "Total bytes received");
        metrics::describe_counter!("dfe_fetcher_extractor_runs_total", "Total extractor runs");
        metrics::describe_counter!(
            "dfe_fetcher_extractor_runs_success_total",
            "Successful extractor runs"
        );
        metrics::describe_counter!(
            "dfe_fetcher_extractor_runs_error_total",
            "Failed extractor runs"
        );
        metrics::describe_counter!(
            "dfe_fetcher_extractor_records_total",
            "Total records from extractors"
        );
        metrics::describe_counter!(
            "dfe_fetcher_extractor_restart_exhausted_total",
            "Extractor restart retries exhausted"
        );
        metrics::describe_counter!("dfe_fetcher_cursor_writes_total", "Cursor state writes");
        metrics::describe_counter!(
            "dfe_fetcher_cursor_write_failures_total",
            "Cursor state write failures"
        );
        metrics::describe_gauge!(
            "dfe_fetcher_active_fetches",
            "Current active fetch operations"
        );
        metrics::describe_gauge!(
            "dfe_fetcher_active_extractors",
            "Current running extractors"
        );
        metrics::describe_gauge!("dfe_fetcher_memory_used_bytes", "Current memory usage");
        metrics::describe_gauge!("dfe_fetcher_memory_limit_bytes", "Memory limit");
        metrics::describe_gauge!("dfe_fetcher_fetch_rate", "Current fetch rate");

        Self {
            dfe: Some(DfeMetrics::register()),
            ..Self::new()
        }
    }

    // ==========================================================================
    // Fetch counters
    // ==========================================================================

    /// Increment total fetches counter.
    #[inline]
    pub fn inc_fetches_total(&self) {
        let count = self.fetches_total.fetch_add(1, Ordering::Relaxed) + 1;
        self.rate_window.record(count);
        if self.dfe.is_some() {
            metrics::counter!("dfe_fetcher_fetches_total").increment(1);
        }
    }

    /// Increment successful fetches counter.
    #[inline]
    pub fn inc_fetches_success(&self) {
        self.fetches_success.fetch_add(1, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::counter!("dfe_fetcher_fetches_success_total").increment(1);
        }
    }

    /// Increment failed fetches counter.
    #[inline]
    pub fn inc_fetches_error(&self) {
        self.fetches_error.fetch_add(1, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::counter!("dfe_fetcher_fetches_error_total").increment(1);
        }
    }

    /// Add records fetched.
    #[inline]
    pub fn add_records_fetched(&self, count: u64) {
        self.records_fetched.fetch_add(count, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.records_received(count);
        }
    }

    /// Add bytes fetched.
    #[inline]
    pub fn add_bytes_fetched(&self, bytes: u64) {
        self.bytes_fetched.fetch_add(bytes, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::counter!("dfe_fetcher_bytes_received_total").increment(bytes);
        }
    }

    // ==========================================================================
    // Delivery counters
    // ==========================================================================

    /// Add messages sent to Kafka.
    #[inline]
    pub fn add_messages_sent_kafka(&self, count: u64) {
        self.messages_sent_kafka.fetch_add(count, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.transport_sent("output", count);
        }
    }

    /// Increment DLQ messages counter.
    #[inline]
    pub fn inc_messages_dlq(&self) {
        self.messages_dlq.fetch_add(1, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.records_dlq(1);
        }
    }

    // ==========================================================================
    // Extractor counters
    // ==========================================================================

    /// Increment total extractor runs.
    #[inline]
    pub fn inc_extractor_runs_total(&self) {
        self.extractor_runs_total.fetch_add(1, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::counter!("dfe_fetcher_extractor_runs_total").increment(1);
        }
    }

    /// Increment successful extractor runs.
    #[inline]
    pub fn inc_extractor_runs_success(&self) {
        self.extractor_runs_success.fetch_add(1, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::counter!("dfe_fetcher_extractor_runs_success_total").increment(1);
        }
    }

    /// Increment failed extractor runs.
    #[inline]
    pub fn inc_extractor_runs_error(&self) {
        self.extractor_runs_error.fetch_add(1, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::counter!("dfe_fetcher_extractor_runs_error_total").increment(1);
        }
    }

    /// Get the count of failed extractor runs.
    #[inline]
    pub fn extractor_runs_error(&self) -> u64 {
        self.extractor_runs_error.load(Ordering::Relaxed)
    }

    /// Add records from extractors.
    #[inline]
    pub fn add_extractor_records(&self, count: u64) {
        self.extractor_records_total
            .fetch_add(count, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::counter!("dfe_fetcher_extractor_records_total").increment(count);
        }
    }

    // ==========================================================================
    // Backpressure / transport health
    // ==========================================================================

    /// Increment transport backpressure events.
    #[inline]
    pub fn inc_transport_backpressured(&self) {
        self.transport_backpressured_total
            .fetch_add(1, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.transport_backpressured("output", 1);
        }
    }

    /// Increment transport send errors.
    #[inline]
    pub fn inc_transport_send_errors(&self) {
        self.transport_send_errors_total
            .fetch_add(1, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.transport_send_errors("output", 1);
        }
    }

    /// Set transport health gauge (1=healthy, 0=unhealthy).
    #[inline]
    pub fn set_transport_healthy(&self, healthy: bool) {
        self.transport_healthy
            .store(u64::from(healthy), Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.transport_healthy("output", healthy);
        }
    }

    // ==========================================================================
    // Filtering / extractor lifecycle
    // ==========================================================================

    /// Increment records filtered counter.
    #[inline]
    pub fn inc_records_filtered(&self) {
        self.records_filtered_total.fetch_add(1, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.records_filtered(1);
        }
    }

    /// Increment extractor restart exhausted counter.
    #[inline]
    pub fn inc_extractor_restart_exhausted(&self) {
        self.extractor_restart_exhausted_total
            .fetch_add(1, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::counter!("dfe_fetcher_extractor_restart_exhausted_total").increment(1);
        }
    }

    /// Increment cursor writes counter.
    #[inline]
    pub fn inc_cursor_writes(&self) {
        self.cursor_writes_total.fetch_add(1, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::counter!("dfe_fetcher_cursor_writes_total").increment(1);
        }
    }

    /// Increment cursor write failures counter.
    #[inline]
    pub fn inc_cursor_write_failures(&self) {
        self.cursor_write_failures_total
            .fetch_add(1, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::counter!("dfe_fetcher_cursor_write_failures_total").increment(1);
        }
    }

    // ==========================================================================
    // Pipeline / delivery
    // ==========================================================================

    /// Set pipeline readiness gauge (1=ready, 0=backpressured).
    #[inline]
    pub fn set_pipeline_ready(&self, ready: bool) {
        self.pipeline_ready
            .store(u64::from(ready), Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.pipeline_ready(ready);
        }
    }

    /// Increment records delivered counter.
    #[inline]
    pub fn inc_records_delivered(&self) {
        self.records_delivered_total.fetch_add(1, Ordering::Relaxed);
        if let Some(ref dfe) = self.dfe {
            dfe.records_delivered(1);
        }
    }

    // ==========================================================================
    // Gauges
    // ==========================================================================

    /// Increment active fetches.
    #[inline]
    pub fn inc_active_fetches(&self) {
        let val = self.active_fetches.fetch_add(1, Ordering::Relaxed) + 1;
        if self.dfe.is_some() {
            metrics::gauge!("dfe_fetcher_active_fetches").set(val as f64);
        }
    }

    /// Decrement active fetches (saturating — never wraps below zero).
    #[inline]
    pub fn dec_active_fetches(&self) {
        let _ = self
            .active_fetches
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
        if self.dfe.is_some() {
            let val = self.active_fetches.load(Ordering::Relaxed);
            metrics::gauge!("dfe_fetcher_active_fetches").set(val as f64);
        }
    }

    /// Increment active extractors.
    #[inline]
    pub fn inc_active_extractors(&self) {
        let val = self.active_extractors.fetch_add(1, Ordering::Relaxed) + 1;
        if self.dfe.is_some() {
            metrics::gauge!("dfe_fetcher_active_extractors").set(val as f64);
        }
    }

    /// Decrement active extractors (saturating — never wraps below zero).
    #[inline]
    pub fn dec_active_extractors(&self) {
        let _ = self
            .active_extractors
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
        if self.dfe.is_some() {
            let val = self.active_extractors.load(Ordering::Relaxed);
            metrics::gauge!("dfe_fetcher_active_extractors").set(val as f64);
        }
    }

    /// Set memory usage.
    #[inline]
    pub fn set_memory_usage(&self, used: u64, limit: u64) {
        self.memory_used_bytes.store(used, Ordering::Relaxed);
        self.memory_limit_bytes.store(limit, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::gauge!("dfe_fetcher_memory_used_bytes").set(used as f64);
            metrics::gauge!("dfe_fetcher_memory_limit_bytes").set(limit as f64);
        }
    }

    /// Get fetch rate per second.
    pub fn fetch_rate(&self) -> f64 {
        self.rate_window.rate_per_second()
    }

    /// Update the fetch rate gauge in the metrics recorder.
    ///
    /// Call this periodically (e.g. from the metrics server tick) to keep
    /// the `dfe_fetcher_fetch_rate` gauge current.
    pub fn update_rate_gauge(&self) {
        if self.dfe.is_some() {
            metrics::gauge!("dfe_fetcher_fetch_rate").set(self.fetch_rate());
        }
    }

    /// Render metrics in Prometheus format (hand-rolled, for tests and fallback).
    ///
    /// In production, prefer the [`MetricsManager`](hyperi_rustlib::metrics::MetricsManager)
    /// render path which includes all metrics registered via the `metrics` crate.
    pub fn render(&self) -> String {
        let mut output = String::with_capacity(4096);

        // Fetch counters
        output.push_str("# HELP dfe_fetcher_fetches_total Total number of fetch operations\n");
        output.push_str("# TYPE dfe_fetcher_fetches_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_fetches_total {}\n",
            self.fetches_total.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_fetcher_fetches_success_total Successful fetch operations\n");
        output.push_str("# TYPE dfe_fetcher_fetches_success_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_fetches_success_total {}\n",
            self.fetches_success.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_fetcher_fetches_error_total Failed fetch operations\n");
        output.push_str("# TYPE dfe_fetcher_fetches_error_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_fetches_error_total {}\n",
            self.fetches_error.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_records_received_total Total records received\n");
        output.push_str("# TYPE dfe_records_received_total counter\n");
        output.push_str(&format!(
            "dfe_records_received_total {}\n",
            self.records_fetched.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_fetcher_bytes_received_total Total bytes received\n");
        output.push_str("# TYPE dfe_fetcher_bytes_received_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_bytes_received_total {}\n",
            self.bytes_fetched.load(Ordering::Relaxed)
        ));

        // Delivery counters
        output.push_str(
            "# HELP dfe_transport_sent_total Total messages sent successfully via transport\n",
        );
        output.push_str("# TYPE dfe_transport_sent_total counter\n");
        output.push_str(&format!(
            "dfe_transport_sent_total {}\n",
            self.messages_sent_kafka.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_records_dlq_total Total records routed to dead letter queue\n");
        output.push_str("# TYPE dfe_records_dlq_total counter\n");
        output.push_str(&format!(
            "dfe_records_dlq_total {}\n",
            self.messages_dlq.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_records_delivered_total Total records delivered to output\n");
        output.push_str("# TYPE dfe_records_delivered_total counter\n");
        output.push_str(&format!(
            "dfe_records_delivered_total {}\n",
            self.records_delivered_total.load(Ordering::Relaxed)
        ));

        // Extractor counters
        output.push_str("# HELP dfe_fetcher_extractor_runs_total Total extractor runs\n");
        output.push_str("# TYPE dfe_fetcher_extractor_runs_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_extractor_runs_total {}\n",
            self.extractor_runs_total.load(Ordering::Relaxed)
        ));

        output.push_str(
            "# HELP dfe_fetcher_extractor_runs_success_total Successful extractor runs\n",
        );
        output.push_str("# TYPE dfe_fetcher_extractor_runs_success_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_extractor_runs_success_total {}\n",
            self.extractor_runs_success.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_fetcher_extractor_runs_error_total Failed extractor runs\n");
        output.push_str("# TYPE dfe_fetcher_extractor_runs_error_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_extractor_runs_error_total {}\n",
            self.extractor_runs_error.load(Ordering::Relaxed)
        ));

        output
            .push_str("# HELP dfe_fetcher_extractor_records_total Total records from extractors\n");
        output.push_str("# TYPE dfe_fetcher_extractor_records_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_extractor_records_total {}\n",
            self.extractor_records_total.load(Ordering::Relaxed)
        ));

        // Backpressure / transport health
        output.push_str(
            "# HELP dfe_transport_backpressured_total Total send attempts rejected due to backpressure\n",
        );
        output.push_str("# TYPE dfe_transport_backpressured_total counter\n");
        output.push_str(&format!(
            "dfe_transport_backpressured_total {}\n",
            self.transport_backpressured_total.load(Ordering::Relaxed)
        ));

        output.push_str(
            "# HELP dfe_transport_send_errors_total Total messages that failed to send\n",
        );
        output.push_str("# TYPE dfe_transport_send_errors_total counter\n");
        output.push_str(&format!(
            "dfe_transport_send_errors_total {}\n",
            self.transport_send_errors_total.load(Ordering::Relaxed)
        ));

        output.push_str(
            "# HELP dfe_transport_healthy Transport health status (1=healthy, 0=unhealthy)\n",
        );
        output.push_str("# TYPE dfe_transport_healthy gauge\n");
        output.push_str(&format!(
            "dfe_transport_healthy {}\n",
            self.transport_healthy.load(Ordering::Relaxed)
        ));

        // Filtering / extractor lifecycle
        output.push_str(
            "# HELP dfe_records_filtered_total Total records dropped by filter expressions\n",
        );
        output.push_str("# TYPE dfe_records_filtered_total counter\n");
        output.push_str(&format!(
            "dfe_records_filtered_total {}\n",
            self.records_filtered_total.load(Ordering::Relaxed)
        ));

        output.push_str(
            "# HELP dfe_fetcher_extractor_restart_exhausted_total Extractor restart retries exhausted\n",
        );
        output.push_str("# TYPE dfe_fetcher_extractor_restart_exhausted_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_extractor_restart_exhausted_total {}\n",
            self.extractor_restart_exhausted_total
                .load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_fetcher_cursor_writes_total Cursor state writes\n");
        output.push_str("# TYPE dfe_fetcher_cursor_writes_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_cursor_writes_total {}\n",
            self.cursor_writes_total.load(Ordering::Relaxed)
        ));

        output.push_str(
            "# HELP dfe_fetcher_cursor_write_failures_total Cursor state write failures\n",
        );
        output.push_str("# TYPE dfe_fetcher_cursor_write_failures_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_cursor_write_failures_total {}\n",
            self.cursor_write_failures_total.load(Ordering::Relaxed)
        ));

        // Gauges
        output
            .push_str("# HELP dfe_pipeline_ready Pipeline readiness (1=ready, 0=backpressured)\n");
        output.push_str("# TYPE dfe_pipeline_ready gauge\n");
        output.push_str(&format!(
            "dfe_pipeline_ready {}\n",
            self.pipeline_ready.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_fetcher_active_fetches Current active fetch operations\n");
        output.push_str("# TYPE dfe_fetcher_active_fetches gauge\n");
        output.push_str(&format!(
            "dfe_fetcher_active_fetches {}\n",
            self.active_fetches.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_fetcher_active_extractors Current running extractors\n");
        output.push_str("# TYPE dfe_fetcher_active_extractors gauge\n");
        output.push_str(&format!(
            "dfe_fetcher_active_extractors {}\n",
            self.active_extractors.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_fetcher_memory_used_bytes Current memory usage\n");
        output.push_str("# TYPE dfe_fetcher_memory_used_bytes gauge\n");
        output.push_str(&format!(
            "dfe_fetcher_memory_used_bytes {}\n",
            self.memory_used_bytes.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP dfe_fetcher_memory_limit_bytes Memory limit\n");
        output.push_str("# TYPE dfe_fetcher_memory_limit_bytes gauge\n");
        output.push_str(&format!(
            "dfe_fetcher_memory_limit_bytes {}\n",
            self.memory_limit_bytes.load(Ordering::Relaxed)
        ));

        // Rate
        output.push_str("# HELP dfe_fetcher_fetch_rate Current fetch rate\n");
        output.push_str("# TYPE dfe_fetcher_fetch_rate gauge\n");
        output.push_str(&format!(
            "dfe_fetcher_fetch_rate {:.2}\n",
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
        assert!(output.contains("dfe_fetcher_fetches_total 2"));
        assert!(output.contains("dfe_fetcher_fetches_success_total 1"));
    }

    #[test]
    fn test_metrics_extractor_counters() {
        let metrics = Metrics::new();

        metrics.inc_extractor_runs_total();
        metrics.inc_extractor_runs_success();
        metrics.add_extractor_records(42);

        let output = metrics.render();
        assert!(output.contains("dfe_fetcher_extractor_runs_total 1"));
        assert!(output.contains("dfe_fetcher_extractor_runs_success_total 1"));
        assert!(output.contains("dfe_fetcher_extractor_records_total 42"));
    }

    #[test]
    fn test_memory_metrics() {
        let metrics = Metrics::new();

        metrics.set_memory_usage(500_000, 1_000_000);

        let output = metrics.render();
        assert!(output.contains("dfe_fetcher_memory_used_bytes 500000"));
        assert!(output.contains("dfe_fetcher_memory_limit_bytes 1000000"));
    }
}
