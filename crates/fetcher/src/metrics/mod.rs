// Project:   dfe-fetcher
// File:      crates/fetcher/src/metrics/mod.rs
// Purpose:   Prometheus metrics for fetch operations
// Language:  Rust
//
// License:   BUSL-1.1
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
//! (so [`MetricsManager`] can render
//! the full Prometheus text format).
//!
//! When [`Metrics::with_dfe()`] is used, fetcher-specific metrics are
//! described and emitted through the `metrics` crate global recorder,
//! alongside the standard DFE metrics from scalo [`ServiceMetrics`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dfe_fetcher_core::metric_names as fw;
use scalo::metrics::{MetricsManager, ServiceMetrics, TransportKind};
use scalo::scaling::RateWindow;

/// What became of extractor records that were not delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractorFailure {
    /// The sender was answered to re-send them.
    Retry,
    /// Nothing can re-send them: they are lost.
    Dropped,
}

impl ExtractorFailure {
    /// The `outcome` label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Retry => "retry",
            Self::Dropped => "dropped",
        }
    }
}

/// Why a dead letter was dropped: the `reason` label of
/// `pipeline_dead_letters_dropped_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DroppedDeadLetter {
    /// An output refused the record itself (its size or format, or an
    /// outbound `dlq` filter) and would refuse it again.
    TransportRefused,
    /// The row is over `max_record_bytes`, scalo's `too_large`.
    TooLarge,
}

impl DroppedDeadLetter {
    /// The `reason` label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TransportRefused => "transport_refused",
            Self::TooLarge => "too_large",
        }
    }
}

/// The kind of each framework series, for registration and for the test that
/// keeps this list equal to the core's name list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Counter,
    Gauge,
    Histogram,
}

/// Every framework metric, its kind and its help text.
const FRAMEWORK_METRICS: &[(&str, Kind, &str)] = &[
    (
        fw::API_DURATION_SECONDS,
        Kind::Histogram,
        "Per-API-call latency by source",
    ),
    (
        fw::API_ERRORS_TOTAL,
        Kind::Counter,
        "API errors by source and typed code",
    ),
    (
        fw::PAGES_FETCHED_TOTAL,
        Kind::Counter,
        "Pages requested by source",
    ),
    (
        fw::PAGES_TRUNCATED_TOTAL,
        Kind::Counter,
        "Page sequences cut at max_pages with more to fetch, by source and unit",
    ),
    (
        fw::TAIL_PAGES_FULL_TOTAL,
        Kind::Counter,
        "Database tail pages that came back full, by source and unit",
    ),
    (
        fw::RECORDS_FETCHED_TOTAL,
        Kind::Counter,
        "Rows yielded by the shape before the filter, by source",
    ),
    (
        fw::BYTES_FETCHED_TOTAL,
        Kind::Counter,
        "Response body bytes read by source",
    ),
    (
        fw::RECORDS_FILTERED_TOTAL,
        Kind::Counter,
        "Rows the compiled filter dropped, by source",
    ),
    (
        fw::ACCUMULATE_FLUSHES_TOTAL,
        Kind::Counter,
        "Batch flushes by source and trigger",
    ),
    (
        fw::ACCUMULATE_BATCH_ROWS,
        Kind::Histogram,
        "Rows per flushed batch by source",
    ),
    (
        fw::ACCUMULATE_BATCH_BYTES,
        Kind::Histogram,
        "Bytes per flushed batch by source",
    ),
    (
        fw::ACCUMULATE_PENDING_BYTES,
        Kind::Gauge,
        "Bytes buffered and leased right now, by source",
    ),
    (
        fw::SNAPSHOT_ROWS_TOTAL,
        Kind::Counter,
        "Dump rows emitted by store",
    ),
    (
        fw::SNAPSHOT_ROWS_OVERSIZE_TOTAL,
        Kind::Counter,
        "Dump rows replaced by an oversize stub, by store",
    ),
    (
        fw::SNAPSHOTS_TOTAL,
        Kind::Counter,
        "Dumps by store and status",
    ),
];

/// Describe every framework series with the global recorder.
fn describe_framework_metrics() {
    for (name, kind, help) in FRAMEWORK_METRICS {
        match kind {
            Kind::Counter => metrics::describe_counter!(*name, *help),
            Kind::Gauge => metrics::describe_gauge!(*name, *help),
            Kind::Histogram => metrics::describe_histogram!(*name, *help),
        }
    }
}

/// Metrics collector for dfe-fetcher.
///
/// Maintains local atomic counters for fast hot-path access and optionally
/// dual-emits to the `metrics` crate global recorder (via scalo
/// [`ServiceMetrics`] for standard DFE metrics, and direct `metrics::counter!` /
/// `metrics::gauge!` calls for fetcher-specific metrics).
pub struct Metrics {
    /// Optional scalo ServiceMetrics for dual-emit to global `metrics` recorder.
    dfe: Option<ServiceMetrics>,
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
    extractor_records_failed_total: AtomicU64,
    cursor_writes_total: AtomicU64,
    cursor_write_failures_total: AtomicU64,
    cursor_cold_starts_total: AtomicU64,
    cursor_read_failures_total: AtomicU64,
    dead_letters_dropped_total: AtomicU64,

    // Pipeline / delivery
    pipeline_ready: AtomicU64,          // gauge: 1=ready, 0=backpressured
    records_delivered_total: AtomicU64, // counter

    // Gauges
    active_fetches: AtomicU64,
    active_extractors: AtomicU64,
    memory_used_bytes: AtomicU64,
    memory_limit_bytes: AtomicU64,

    // Rate tracking (scalo RateWindow has internal RwLock)
    rate_window: RateWindow,
    /// Records-per-second rate window (EPS -- events per second).
    records_rate_window: RateWindow,

    // Scaling-signal inputs (cheap, self-normalised -- no per-env target).
    /// Configured concurrent-fetch cap (the scheduler semaphore size). Used as
    /// the denominator of the self-normalised `fetch_pressure` scaling signal
    /// (`active_fetches / concurrency_cap`). 0 = unset (signal contributes 0).
    concurrency_cap: AtomicU64,
    /// Cumulative throttle/rate-limit API errors (HTTP 429 + AWS SlowDown).
    throttle_errors_total: AtomicU64,
    /// Cumulative attempted fetch cycles (success + error), the throttle-ratio
    /// denominator.
    fetch_attempts_total: AtomicU64,
}

impl Metrics {
    /// Create a new metrics collector (without ServiceMetrics dual-emit).
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
            extractor_records_failed_total: AtomicU64::new(0),
            cursor_writes_total: AtomicU64::new(0),
            cursor_write_failures_total: AtomicU64::new(0),
            cursor_cold_starts_total: AtomicU64::new(0),
            cursor_read_failures_total: AtomicU64::new(0),
            dead_letters_dropped_total: AtomicU64::new(0),
            pipeline_ready: AtomicU64::new(1),
            records_delivered_total: AtomicU64::new(0),
            active_fetches: AtomicU64::new(0),
            active_extractors: AtomicU64::new(0),
            memory_used_bytes: AtomicU64::new(0),
            memory_limit_bytes: AtomicU64::new(0),
            rate_window: RateWindow::new(Duration::from_mins(1)),
            records_rate_window: RateWindow::new(Duration::from_mins(1)),
            concurrency_cap: AtomicU64::new(0),
            throttle_errors_total: AtomicU64::new(0),
            fetch_attempts_total: AtomicU64::new(0),
        }
    }

    /// Create a new metrics collector with ServiceMetrics dual-emit enabled.
    ///
    /// Registers standard DFE metric descriptions **and** fetcher-specific
    /// metric descriptions with the global `metrics` recorder. Use in
    /// production where [`MetricsManager`]
    /// is (or will be) installed.
    pub fn with_dfe(manager: &MetricsManager) -> Self {
        // Register fetcher-specific metrics with the global recorder.
        // These are NOT part of ServiceMetrics (which covers standard DFE metrics
        // shared across receiver/loader/engine) -- they are fetcher-only.
        metrics::describe_counter!(
            "dfe_fetcher_fetches_total",
            "Fetch operations by source and status"
        );
        metrics::describe_counter!("dfe_fetcher_bytes_received_total", "Total bytes received");
        metrics::describe_gauge!(
            "dfe_fetcher_fetch_pressure_ratio",
            "Self-normalised fetch backlog: active_fetches / concurrency_cap (0-1)"
        );
        metrics::describe_gauge!(
            "dfe_fetcher_throttle_ratio",
            "Self-normalised upstream throttle rate: throttle_errors / fetch_attempts (0-1)"
        );
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
        metrics::describe_counter!(
            "dfe_fetcher_cursor_cold_start_total",
            "Ticks that found no cursor stored, or no cursor store, by source"
        );
        metrics::describe_counter!(
            "dfe_fetcher_cursor_read_failures_total",
            "Ticks whose cursor read failed, by source"
        );
        metrics::describe_counter!(
            "dfe_fetcher_extractor_records_failed_total",
            "Extractor records not delivered, by extractor and outcome (retry or dropped)"
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
        metrics::describe_histogram!(
            "dfe_fetcher_fetch_duration_seconds",
            metrics::Unit::Seconds,
            "Time per fetch cycle"
        );
        metrics::describe_gauge!(
            "dfe_fetcher_cursor_age_seconds",
            "Seconds since cursor last_fetch_end (data staleness)"
        );
        metrics::describe_counter!(
            "dfe_fetcher_api_errors_total",
            "Cloud API errors by source and category"
        );
        metrics::describe_counter!(
            "dfe_fetcher_ingest_requests_total",
            "Ingest HTTP requests by status"
        );
        metrics::describe_histogram!(
            "dfe_fetcher_ingest_duration_seconds",
            metrics::Unit::Seconds,
            "Ingest request processing latency"
        );
        metrics::describe_histogram!(
            "dfe_fetcher_transport_send_duration_seconds",
            metrics::Unit::Seconds,
            "Per-transport send latency (Kafka, gRPC)"
        );
        metrics::describe_gauge!(
            "dfe_fetcher_events_per_second",
            "Current records-per-second throughput (EPS)"
        );
        describe_framework_metrics();

        Self {
            dfe: Some(ServiceMetrics::register(manager)),
            ..Self::new()
        }
    }

    // ==========================================================================
    // Fetch counters
    // ==========================================================================

    /// Increment successful fetches counter for a named source.
    ///
    /// Increments both the local `fetches_success` and `fetches_total` atomics,
    /// and emits `dfe_fetcher_fetches_total{source="..",status="success"}` via the
    /// `metrics` crate when ServiceMetrics is active.
    #[inline]
    pub fn inc_fetches_success_for(&self, source: &str) {
        self.fetches_success.fetch_add(1, Ordering::Relaxed);
        self.fetch_attempts_total.fetch_add(1, Ordering::Relaxed);
        let count = self.fetches_total.fetch_add(1, Ordering::Relaxed) + 1;
        self.rate_window.record(count);
        if self.dfe.is_some() {
            metrics::counter!(
                "dfe_fetcher_fetches_total",
                "source" => source.to_string(),
                "status" => "success"
            )
            .increment(1);
        }
    }

    /// Increment successful fetches counter (source = "unknown").
    ///
    /// Convenience wrapper for tests and call sites without source context.
    #[inline]
    pub fn inc_fetches_success(&self) {
        self.inc_fetches_success_for("unknown");
    }

    /// Increment failed fetches counter for a named source.
    ///
    /// Increments both the local `fetches_error` and `fetches_total` atomics,
    /// and emits `dfe_fetcher_fetches_total{source="..",status="error"}` via the
    /// `metrics` crate when ServiceMetrics is active.
    #[inline]
    pub fn inc_fetches_error_for(&self, source: &str) {
        self.fetches_error.fetch_add(1, Ordering::Relaxed);
        self.fetch_attempts_total.fetch_add(1, Ordering::Relaxed);
        self.fetches_total.fetch_add(1, Ordering::Relaxed);
        if self.dfe.is_some() {
            metrics::counter!(
                "dfe_fetcher_fetches_total",
                "source" => source.to_string(),
                "status" => "error"
            )
            .increment(1);
        }
    }

    /// Increment failed fetches counter (source = "unknown").
    ///
    /// Convenience wrapper for tests and call sites without source context.
    #[inline]
    pub fn inc_fetches_error(&self) {
        self.inc_fetches_error_for("unknown");
    }

    /// Record a cloud API error.
    ///
    /// `code` should be one of: "throttle", "4xx", "5xx", "timeout", "network".
    /// The "throttle" category (HTTP 429 / AWS SlowDown / rate-limit) is split
    /// out of the generic "4xx" bucket so it also drives the self-normalised
    /// `throttle_ratio` scaling signal (rate-limiting => spread quota over more
    /// pods, NOT a client bug like a 401/404).
    #[inline]
    pub fn inc_api_error(&self, source: &str, code: &str) {
        if code == "throttle" {
            self.throttle_errors_total.fetch_add(1, Ordering::Relaxed);
        }
        if self.dfe.is_some() {
            metrics::counter!(
                "dfe_fetcher_api_errors_total",
                "source" => source.to_string(),
                "code" => code.to_string()
            )
            .increment(1);
        }
    }

    /// Set the configured concurrent-fetch cap (scheduler semaphore size).
    /// Denominator of the self-normalised `fetch_pressure` scaling signal.
    #[inline]
    pub fn set_concurrency_cap(&self, cap: usize) {
        self.concurrency_cap.store(cap as u64, Ordering::Relaxed);
    }

    /// Self-normalised fetch-pressure signal in `[0, 1]`: in-flight fetches over
    /// the concurrency cap. Needs NO per-env target -- 1.0 means the fetch
    /// semaphore is saturated (pod is fetch-bound, scale out helps). Returns 0
    /// when the cap is unset.
    #[must_use]
    pub fn fetch_pressure_ratio(&self) -> f64 {
        let cap = self.concurrency_cap.load(Ordering::Relaxed);
        if cap == 0 {
            return 0.0;
        }
        let active = self.active_fetches.load(Ordering::Relaxed);
        (active as f64 / cap as f64).min(1.0)
    }

    /// Self-normalised throttle signal in `[0, 1]`: cumulative throttle/429
    /// errors over cumulative fetch attempts. Needs NO per-env target -- 1.0
    /// means every fetch is being rate-limited upstream (spread the quota over
    /// more pods). Returns 0 before any fetch attempt.
    #[must_use]
    pub fn throttle_ratio(&self) -> f64 {
        let attempts = self.fetch_attempts_total.load(Ordering::Relaxed);
        if attempts == 0 {
            return 0.0;
        }
        let throttled = self.throttle_errors_total.load(Ordering::Relaxed);
        (throttled as f64 / attempts as f64).min(1.0)
    }

    /// Add records fetched. Also updates the records-per-second rate window (EPS).
    #[inline]
    pub fn add_records_fetched(&self, count: u64) {
        let total = self.records_fetched.fetch_add(count, Ordering::Relaxed) + count;
        self.records_rate_window.record(total);
        if let Some(ref dfe) = self.dfe {
            dfe.records_received(count);
        }
    }

    /// Get the current events per second (records/sec) rate.
    pub fn events_per_second(&self) -> f64 {
        self.records_rate_window.rate_per_second()
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
            dfe.transport_sent(TransportKind::Kafka, count);
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

    /// Increment extractor runs counter with name and status labels.
    ///
    /// Emits `dfe_fetcher_extractor_runs_total{name="..",status=".."}` via the
    /// `metrics` crate when ServiceMetrics is active. Also updates local atomics
    /// for the aggregate counters.
    #[inline]
    pub fn inc_extractor_run_for(&self, name: &str, status: &str) {
        self.extractor_runs_total.fetch_add(1, Ordering::Relaxed);
        match status {
            "success" => {
                self.extractor_runs_success.fetch_add(1, Ordering::Relaxed);
            }
            "error" => {
                self.extractor_runs_error.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        if self.dfe.is_some() {
            metrics::counter!(
                "dfe_fetcher_extractor_runs_total",
                "name" => name.to_string(),
                "status" => status.to_string()
            )
            .increment(1);
        }
    }

    // ==========================================================================
    // Ingest metrics
    // ==========================================================================

    /// Increment ingest request counter with a status label.
    #[inline]
    pub fn inc_ingest_request(&self, status: &str) {
        if self.dfe.is_some() {
            metrics::counter!(
                "dfe_fetcher_ingest_requests_total",
                "status" => status.to_string()
            )
            .increment(1);
        }
    }

    /// Record ingest request processing latency.
    #[inline]
    pub fn record_ingest_duration(&self, duration: std::time::Duration) {
        if self.dfe.is_some() {
            metrics::histogram!("dfe_fetcher_ingest_duration_seconds")
                .record(duration.as_secs_f64());
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

    /// Increment the local transport send error total.
    ///
    /// The platform `transport_send_errors_total` is counted by the scalo
    /// output transport under its own label, so it is not counted here too.
    #[inline]
    pub fn inc_transport_send_errors(&self) {
        self.transport_send_errors_total
            .fetch_add(1, Ordering::Relaxed);
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

    /// Count a tick that found no cursor stored, or no store at all.
    ///
    /// Emits `dfe_fetcher_cursor_cold_start_total{source}` on every miss: an
    /// empty store cannot tell a new source from a lost cursor, so an alert
    /// on a source that has run before is how a lost cursor is noticed.
    #[inline]
    pub fn inc_cursor_cold_start(&self, source: &str) {
        self.cursor_cold_starts_total
            .fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "dfe_fetcher_cursor_cold_start_total",
            "source" => source.to_string()
        )
        .increment(1);
    }

    /// Ticks that found no cursor to resume from, across every source.
    #[inline]
    pub fn cursor_cold_starts(&self) -> u64 {
        self.cursor_cold_starts_total.load(Ordering::Relaxed)
    }

    /// Count a tick whose cursor read failed.
    ///
    /// Emits `dfe_fetcher_cursor_read_failures_total{source}`: the store
    /// failed while the process runs, so unlike a cold start it is a fault.
    #[inline]
    pub fn inc_cursor_read_failure(&self, source: &str) {
        self.cursor_read_failures_total
            .fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "dfe_fetcher_cursor_read_failures_total",
            "source" => source.to_string()
        )
        .increment(1);
    }

    /// Ticks whose cursor read failed, across every source.
    #[inline]
    pub fn cursor_read_failures(&self) -> u64 {
        self.cursor_read_failures_total.load(Ordering::Relaxed)
    }

    /// Count records an extractor took in but could not deliver.
    ///
    /// Emits `dfe_fetcher_extractor_records_failed_total{extractor, outcome}`:
    /// `retry` when the sender was answered to re-send them, `dropped` when
    /// nothing can re-send them (a container's stdout).
    #[inline]
    pub fn add_extractor_records_failed(
        &self,
        extractor: &'static str,
        outcome: ExtractorFailure,
        count: u64,
    ) {
        self.extractor_records_failed_total
            .fetch_add(count, Ordering::Relaxed);
        metrics::counter!(
            "dfe_fetcher_extractor_records_failed_total",
            "extractor" => extractor,
            "outcome" => outcome.as_str()
        )
        .increment(count);
    }

    /// Records extractors took in but could not deliver, across every outcome.
    #[inline]
    pub fn extractor_records_failed(&self) -> u64 {
        self.extractor_records_failed_total.load(Ordering::Relaxed)
    }

    /// Count dead letters dropped with nowhere to go: no DLQ, or a disabled
    /// one.
    ///
    /// Emits `pipeline_dead_letters_dropped_total{reason}`, the series scalo's
    /// run loops count their own dropped dead letters in. `reason` is a
    /// [`DroppedDeadLetter`] label, or scalo's own for a refusal it named.
    #[inline]
    pub fn add_dead_letters_dropped(&self, reason: &'static str, count: u64) {
        self.dead_letters_dropped_total
            .fetch_add(count, Ordering::Relaxed);
        metrics::counter!("pipeline_dead_letters_dropped_total", "reason" => reason)
            .increment(count);
    }

    /// Dead letters dropped with nowhere to go, across every reason.
    #[inline]
    pub fn dead_letters_dropped(&self) -> u64 {
        self.dead_letters_dropped_total.load(Ordering::Relaxed)
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

    /// Decrement active fetches (saturating -- never wraps below zero).
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

    /// Decrement active extractors (saturating -- never wraps below zero).
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
            metrics::gauge!("dfe_fetcher_events_per_second").set(self.events_per_second());
            metrics::gauge!("dfe_fetcher_fetch_pressure_ratio").set(self.fetch_pressure_ratio());
            metrics::gauge!("dfe_fetcher_throttle_ratio").set(self.throttle_ratio());
        }
    }

    /// Record fetch cycle duration.
    #[inline]
    pub fn record_fetch_duration(&self, source: &str, duration: Duration) {
        if self.dfe.is_some() {
            metrics::histogram!(
                "dfe_fetcher_fetch_duration_seconds",
                "source" => source.to_string()
            )
            .record(duration.as_secs_f64());
        }
    }

    /// Set cursor age for a source.
    #[inline]
    pub fn set_cursor_age(&self, source: &str, age_seconds: f64) {
        if self.dfe.is_some() {
            metrics::gauge!(
                "dfe_fetcher_cursor_age_seconds",
                "source" => source.to_string()
            )
            .set(age_seconds);
        }
    }

    /// Render metrics in Prometheus format (hand-rolled, for tests and fallback).
    ///
    /// In production, prefer the [`MetricsManager`]
    /// render path which includes all metrics registered via the `metrics` crate.
    pub fn render(&self) -> String {
        let mut output = String::with_capacity(4096);

        // Fetch counters (labelled by status)
        output.push_str("# HELP dfe_fetcher_fetches_total Fetch operations by source and status\n");
        output.push_str("# TYPE dfe_fetcher_fetches_total counter\n");
        output.push_str(&format!(
            "dfe_fetcher_fetches_total{{status=\"success\"}} {}\n",
            self.fetches_success.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "dfe_fetcher_fetches_total{{status=\"error\"}} {}\n",
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
        output.push_str("# HELP dfe_fetcher_fetch_rate Current fetch rate (fetches/sec)\n");
        output.push_str("# TYPE dfe_fetcher_fetch_rate gauge\n");
        output.push_str(&format!(
            "dfe_fetcher_fetch_rate {:.2}\n",
            self.fetch_rate()
        ));

        // EPS (events per second)
        output.push_str(
            "# HELP dfe_fetcher_events_per_second Current records-per-second throughput (EPS)\n",
        );
        output.push_str("# TYPE dfe_fetcher_events_per_second gauge\n");
        output.push_str(&format!(
            "dfe_fetcher_events_per_second {:.2}\n",
            self.events_per_second()
        ));

        output
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A recorder one test reads the `metrics` series from.
///
/// It is the test thread's own default recorder, never the global one, so it
/// cannot collide with a test that installs a `MetricsManager`, and no test
/// sees another's series. A `#[tokio::test]` runs on one thread, so what it
/// awaits records here too.
#[cfg(test)]
pub(crate) mod recorded {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// Whether `labels` carries every pair in `wanted`.
    pub(crate) fn carries(labels: &[(String, String)], wanted: &[(&str, &str)]) -> bool {
        wanted
            .iter()
            .all(|(k, v)| labels.iter().any(|(lk, lv)| lk == k && lv == v))
    }

    /// The recorder a test sets for its thread with [`Recorder::install`].
    pub(crate) struct Recorder(DebuggingRecorder);

    impl Recorder {
        pub(crate) fn new() -> Self {
            Self(DebuggingRecorder::new())
        }

        /// Record this thread's series here until the guard drops.
        pub(crate) fn install(&self) -> metrics::LocalRecorderGuard<'_> {
            metrics::set_default_local_recorder(&self.0)
        }

        /// Every series named `name` in one snapshot, which drains what it
        /// reads: its labels, then its value.
        fn series(&self, name: &str) -> Vec<(Vec<(String, String)>, DebugValue)> {
            self.0
                .snapshotter()
                .snapshot()
                .into_vec()
                .into_iter()
                .filter(|(key, _, _, _)| key.key().name() == name)
                .map(|(key, _, _, value)| {
                    let labels = key
                        .key()
                        .labels()
                        .map(|l| (l.key().to_owned(), l.value().to_owned()))
                        .collect();
                    (labels, value)
                })
                .collect()
        }

        /// The labels of every gauge named `name` set above zero.
        pub(crate) fn raised_gauges(&self, name: &str) -> Vec<Vec<(String, String)>> {
            self.series(name)
                .into_iter()
                .filter_map(|(labels, value)| match value {
                    DebugValue::Gauge(g) if g.0 > 0.0 => Some(labels),
                    _ => None,
                })
                .collect()
        }

        /// The count of counter `name` carrying every pair in `labels` since
        /// the last read, or `None` when nothing recorded it.
        pub(crate) fn counter(&self, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
            self.series(name)
                .into_iter()
                .find_map(|(found, value)| match value {
                    DebugValue::Counter(n) if carries(&found, labels) => Some(n),
                    _ => None,
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The app describes exactly the series the framework emits: a name added
    /// to the core list without a description here, or described here without
    /// the core emitting it, fails.
    #[test]
    fn framework_metrics_are_described_one_for_one_with_the_core_list() {
        let described: std::collections::BTreeSet<&str> =
            FRAMEWORK_METRICS.iter().map(|(n, _, _)| *n).collect();
        let emitted: std::collections::BTreeSet<&str> = fw::ALL.iter().copied().collect();
        assert_eq!(described, emitted);
        for (name, kind, _) in FRAMEWORK_METRICS {
            let is_counter = name.ends_with("_total");
            assert_eq!(
                *kind == Kind::Counter,
                is_counter,
                "{name}: counters end in _total"
            );
        }
        describe_framework_metrics();
    }

    #[test]
    fn test_metrics_counters() {
        let metrics = Metrics::new();

        metrics.inc_fetches_success();
        metrics.inc_fetches_error();

        let output = metrics.render();
        assert!(output.contains("dfe_fetcher_fetches_total{status=\"success\"} 1"));
        assert!(output.contains("dfe_fetcher_fetches_total{status=\"error\"} 1"));
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

    #[test]
    fn test_metrics_default_creates_valid_instance() {
        let metrics = Metrics::default();
        let output = metrics.render();
        // Default instance should render with all zeroes and healthy defaults
        assert!(output.contains("dfe_fetcher_fetches_total{status=\"success\"} 0"));
        assert!(output.contains("dfe_fetcher_fetches_total{status=\"error\"} 0"));
        assert!(output.contains("dfe_records_received_total 0"));
        // transport_healthy defaults to 1
        assert!(output.contains("dfe_transport_healthy 1"));
        // pipeline_ready defaults to 1
        assert!(output.contains("dfe_pipeline_ready 1"));
    }

    #[test]
    fn test_inc_transport_backpressured() {
        let metrics = Metrics::new();
        metrics.inc_transport_backpressured();
        metrics.inc_transport_backpressured();
        let output = metrics.render();
        assert!(output.contains("dfe_transport_backpressured_total 2"));
    }

    #[test]
    fn test_inc_transport_send_errors() {
        let metrics = Metrics::new();
        metrics.inc_transport_send_errors();
        metrics.inc_transport_send_errors();
        metrics.inc_transport_send_errors();
        let output = metrics.render();
        assert!(output.contains("dfe_transport_send_errors_total 3"));
    }

    /// Counts one named counter across every label set, as a `sum()` over the
    /// name reads it.
    struct CountingRecorder {
        name: &'static str,
        hits: std::sync::Arc<AtomicU64>,
    }

    impl metrics::Recorder for CountingRecorder {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            if key.name() == self.name {
                metrics::Counter::from_arc(std::sync::Arc::clone(&self.hits))
            } else {
                metrics::Counter::noop()
            }
        }

        fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    /// Run `f` with a thread-local recorder counting `name`.
    fn counted(name: &'static str, f: impl FnOnce()) -> u64 {
        let hits = std::sync::Arc::new(AtomicU64::new(0));
        let recorder = CountingRecorder {
            name,
            hits: std::sync::Arc::clone(&hits),
        };
        metrics::with_local_recorder(&recorder, f);
        hits.load(Ordering::Acquire)
    }

    /// The scalo output transport counts its own failed sends, so a failed
    /// send counted here as well would read twice.
    #[test]
    fn a_failed_send_is_left_to_the_transport_in_transport_send_errors_total() {
        let manager = MetricsManager::with_config(scalo::metrics::MetricsConfig::offline(""));
        let mut output = String::new();
        let hits = counted("transport_send_errors_total", || {
            let m = Metrics::with_dfe(&manager);
            m.inc_transport_send_errors();
            m.inc_transport_send_errors();
            output = m.render();
        });
        assert_eq!(hits, 0, "the fetcher adds nothing to the transport's count");
        assert!(
            output.contains("dfe_transport_send_errors_total 2"),
            "the local total still counts both: {output}"
        );
    }

    #[test]
    fn test_inc_records_filtered() {
        let metrics = Metrics::new();
        for _ in 0..7 {
            metrics.inc_records_filtered();
        }
        let output = metrics.render();
        assert!(output.contains("dfe_records_filtered_total 7"));
    }

    #[test]
    fn test_inc_extractor_restart_exhausted() {
        let metrics = Metrics::new();
        metrics.inc_extractor_restart_exhausted();
        let output = metrics.render();
        assert!(output.contains("dfe_fetcher_extractor_restart_exhausted_total 1"));
    }

    #[test]
    fn test_inc_cursor_writes() {
        let metrics = Metrics::new();
        metrics.inc_cursor_writes();
        metrics.inc_cursor_writes();
        let output = metrics.render();
        assert!(output.contains("dfe_fetcher_cursor_writes_total 2"));
    }

    #[test]
    fn test_inc_cursor_write_failures() {
        let metrics = Metrics::new();
        metrics.inc_cursor_write_failures();
        let output = metrics.render();
        assert!(output.contains("dfe_fetcher_cursor_write_failures_total 1"));
    }

    #[test]
    fn test_inc_records_delivered() {
        let metrics = Metrics::new();
        for _ in 0..15 {
            metrics.inc_records_delivered();
        }
        let output = metrics.render();
        assert!(output.contains("dfe_records_delivered_total 15"));
    }

    #[test]
    fn test_set_transport_healthy_true() {
        let metrics = Metrics::new();
        metrics.set_transport_healthy(true);
        let output = metrics.render();
        assert!(
            output.contains("dfe_transport_healthy 1"),
            "Expected healthy=1, got:\n{output}"
        );
    }

    #[test]
    fn test_set_transport_healthy_false() {
        let metrics = Metrics::new();
        metrics.set_transport_healthy(false);
        let output = metrics.render();
        assert!(
            output.contains("dfe_transport_healthy 0"),
            "Expected healthy=0, got:\n{output}"
        );
    }

    #[test]
    fn test_set_pipeline_ready_true() {
        let metrics = Metrics::new();
        metrics.set_pipeline_ready(true);
        let output = metrics.render();
        assert!(output.contains("dfe_pipeline_ready 1"));
    }

    #[test]
    fn test_set_pipeline_ready_false() {
        let metrics = Metrics::new();
        metrics.set_pipeline_ready(false);
        let output = metrics.render();
        assert!(output.contains("dfe_pipeline_ready 0"));
    }

    #[test]
    fn test_active_fetches_inc_dec() {
        let metrics = Metrics::new();
        metrics.inc_active_fetches();
        metrics.inc_active_fetches();
        metrics.inc_active_fetches();
        let output = metrics.render();
        assert!(
            output.contains("dfe_fetcher_active_fetches 3"),
            "Expected 3 active fetches, got:\n{output}"
        );

        metrics.dec_active_fetches();
        let output = metrics.render();
        assert!(
            output.contains("dfe_fetcher_active_fetches 2"),
            "Expected 2 active fetches after dec, got:\n{output}"
        );
    }

    #[test]
    fn test_dec_active_fetches_saturating_at_zero() {
        let metrics = Metrics::new();
        // Start at 0, decrement should not underflow
        metrics.dec_active_fetches();
        metrics.dec_active_fetches();
        let output = metrics.render();
        assert!(
            output.contains("dfe_fetcher_active_fetches 0"),
            "Expected 0 (saturating), got:\n{output}"
        );
    }

    #[test]
    fn test_active_extractors_inc_dec() {
        let metrics = Metrics::new();
        metrics.inc_active_extractors();
        metrics.inc_active_extractors();
        let output = metrics.render();
        assert!(output.contains("dfe_fetcher_active_extractors 2"));

        metrics.dec_active_extractors();
        let output = metrics.render();
        assert!(output.contains("dfe_fetcher_active_extractors 1"));
    }

    #[test]
    fn test_dec_active_extractors_saturating_at_zero() {
        let metrics = Metrics::new();
        metrics.dec_active_extractors();
        let output = metrics.render();
        assert!(
            output.contains("dfe_fetcher_active_extractors 0"),
            "Expected 0 (saturating), got:\n{output}"
        );
    }

    #[test]
    fn test_add_records_fetched() {
        let metrics = Metrics::new();
        metrics.add_records_fetched(100);
        let output = metrics.render();
        assert!(
            output.contains("dfe_records_received_total 100"),
            "Expected 100 records, got:\n{output}"
        );
    }

    #[test]
    fn test_add_bytes_fetched() {
        let metrics = Metrics::new();
        metrics.add_bytes_fetched(5000);
        let output = metrics.render();
        assert!(
            output.contains("dfe_fetcher_bytes_received_total 5000"),
            "Expected 5000 bytes, got:\n{output}"
        );
    }

    #[test]
    fn test_add_messages_sent_kafka() {
        let metrics = Metrics::new();
        metrics.add_messages_sent_kafka(50);
        let output = metrics.render();
        assert!(
            output.contains("dfe_transport_sent_total 50"),
            "Expected 50 sent, got:\n{output}"
        );
    }

    #[test]
    fn test_inc_messages_dlq_multiple() {
        let metrics = Metrics::new();
        metrics.inc_messages_dlq();
        metrics.inc_messages_dlq();
        metrics.inc_messages_dlq();
        metrics.inc_messages_dlq();
        let output = metrics.render();
        assert!(
            output.contains("dfe_records_dlq_total 4"),
            "Expected 4 DLQ messages, got:\n{output}"
        );
    }

    #[test]
    fn test_render_contains_help_and_type_lines() {
        let metrics = Metrics::new();
        let output = metrics.render();

        // Verify HELP lines for key metrics
        assert!(output.contains("# HELP dfe_fetcher_fetches_total"));
        assert!(output.contains("# HELP dfe_records_received_total"));
        assert!(output.contains("# HELP dfe_transport_sent_total"));
        assert!(output.contains("# HELP dfe_records_dlq_total"));
        assert!(output.contains("# HELP dfe_transport_healthy"));
        assert!(output.contains("# HELP dfe_pipeline_ready"));
        assert!(output.contains("# HELP dfe_fetcher_active_fetches"));
        assert!(output.contains("# HELP dfe_fetcher_memory_used_bytes"));

        // Verify TYPE lines
        assert!(output.contains("# TYPE dfe_fetcher_fetches_total counter"));
        assert!(output.contains("# TYPE dfe_records_received_total counter"));
        assert!(output.contains("# TYPE dfe_transport_healthy gauge"));
        assert!(output.contains("# TYPE dfe_pipeline_ready gauge"));
        assert!(output.contains("# TYPE dfe_fetcher_active_fetches gauge"));
    }

    #[test]
    fn test_render_contains_all_metric_families() {
        let metrics = Metrics::new();
        let output = metrics.render();

        // Count distinct metric families (lines starting with a metric name, not # HELP/TYPE)
        let metric_names: std::collections::HashSet<&str> = output
            .lines()
            .filter(|l| !l.starts_with('#') && !l.is_empty())
            .map(|l| {
                // Extract metric name (before first space or brace)
                let name_end = l.find([' ', '{']).unwrap_or(l.len());
                &l[..name_end]
            })
            .collect();

        assert!(
            metric_names.len() >= 20,
            "Expected at least 20 distinct metrics, found {}: {:?}",
            metric_names.len(),
            metric_names
        );
    }

    #[test]
    fn test_fetch_rate_initially_zero() {
        let metrics = Metrics::new();
        assert!(
            (metrics.fetch_rate() - 0.0).abs() < f64::EPSILON,
            "fetch_rate should be 0.0 initially"
        );
    }

    #[test]
    fn test_events_per_second_initially_zero() {
        let metrics = Metrics::new();
        assert!(
            (metrics.events_per_second() - 0.0).abs() < f64::EPSILON,
            "events_per_second should be 0.0 initially"
        );
    }

    #[test]
    fn test_inc_fetches_success_and_error_for_sources() {
        let metrics = Metrics::new();
        metrics.inc_fetches_success_for("aws");
        metrics.inc_fetches_success_for("aws");
        metrics.inc_fetches_error_for("azure");
        let output = metrics.render();

        // success = 2, error = 1
        assert!(
            output.contains("dfe_fetcher_fetches_total{status=\"success\"} 2"),
            "Expected 2 successes, got:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_fetches_total{status=\"error\"} 1"),
            "Expected 1 error, got:\n{output}"
        );
    }

    #[test]
    fn test_extractor_runs_error_getter() {
        let metrics = Metrics::new();
        assert_eq!(metrics.extractor_runs_error(), 0);

        metrics.inc_extractor_runs_error();
        metrics.inc_extractor_runs_error();
        metrics.inc_extractor_runs_error();
        assert_eq!(metrics.extractor_runs_error(), 3);
    }

    #[test]
    fn test_inc_extractor_run_for_success() {
        let metrics = Metrics::new();
        metrics.inc_extractor_run_for("my-tool", "success");
        metrics.inc_extractor_run_for("my-tool", "success");

        // Should increment both total and success
        let output = metrics.render();
        assert!(
            output.contains("dfe_fetcher_extractor_runs_total 2"),
            "Expected 2 total runs, got:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_extractor_runs_success_total 2"),
            "Expected 2 success runs, got:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_extractor_runs_error_total 0"),
            "Expected 0 error runs, got:\n{output}"
        );
    }

    #[test]
    fn test_inc_extractor_run_for_error() {
        let metrics = Metrics::new();
        metrics.inc_extractor_run_for("my-tool", "error");

        let output = metrics.render();
        assert!(
            output.contains("dfe_fetcher_extractor_runs_total 1"),
            "Expected 1 total run, got:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_extractor_runs_error_total 1"),
            "Expected 1 error run, got:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_extractor_runs_success_total 0"),
            "Expected 0 success runs, got:\n{output}"
        );
    }

    // ==========================================================================
    // Comprehensive render() coverage with non-zero counter values
    // ==========================================================================

    #[test]
    fn test_render_all_counters_with_nonzero_values() {
        let metrics = Metrics::new();

        // Exercise every counter and gauge the render() path touches, so the
        // full rendered output contains non-default values for each family.
        metrics.add_messages_sent_kafka(50);
        for _ in 0..3 {
            metrics.inc_messages_dlq();
        }
        for _ in 0..7 {
            metrics.inc_records_delivered();
        }
        for _ in 0..5 {
            metrics.inc_extractor_runs_total();
        }
        for _ in 0..2 {
            metrics.inc_transport_backpressured();
        }
        for _ in 0..4 {
            metrics.inc_transport_send_errors();
        }
        metrics.set_transport_healthy(true);
        for _ in 0..10 {
            metrics.inc_records_filtered();
        }
        metrics.inc_extractor_restart_exhausted();
        for _ in 0..8 {
            metrics.inc_cursor_writes();
        }
        for _ in 0..2 {
            metrics.inc_cursor_write_failures();
        }
        metrics.set_pipeline_ready(false);
        metrics.inc_active_fetches();
        metrics.inc_active_fetches();
        metrics.inc_active_extractors();
        metrics.inc_active_extractors();
        metrics.inc_active_extractors();
        metrics.set_memory_usage(123_456, 789_012);
        metrics.add_records_fetched(42);

        let output = metrics.render();

        // All expected non-zero values
        assert!(
            output.contains("dfe_transport_sent_total 50"),
            "missing dfe_transport_sent_total 50:\n{output}"
        );
        assert!(
            output.contains("dfe_records_dlq_total 3"),
            "missing dfe_records_dlq_total 3:\n{output}"
        );
        assert!(
            output.contains("dfe_records_delivered_total 7"),
            "missing dfe_records_delivered_total 7:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_extractor_runs_total 5"),
            "missing dfe_fetcher_extractor_runs_total 5:\n{output}"
        );
        assert!(
            output.contains("dfe_transport_backpressured_total 2"),
            "missing dfe_transport_backpressured_total 2:\n{output}"
        );
        assert!(
            output.contains("dfe_transport_send_errors_total 4"),
            "missing dfe_transport_send_errors_total 4:\n{output}"
        );
        assert!(
            output.contains("dfe_transport_healthy 1"),
            "missing dfe_transport_healthy 1:\n{output}"
        );
        assert!(
            output.contains("dfe_records_filtered_total 10"),
            "missing dfe_records_filtered_total 10:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_extractor_restart_exhausted_total 1"),
            "missing dfe_fetcher_extractor_restart_exhausted_total 1:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_cursor_writes_total 8"),
            "missing dfe_fetcher_cursor_writes_total 8:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_cursor_write_failures_total 2"),
            "missing dfe_fetcher_cursor_write_failures_total 2:\n{output}"
        );
        assert!(
            output.contains("dfe_pipeline_ready 0"),
            "missing dfe_pipeline_ready 0:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_active_fetches 2"),
            "missing dfe_fetcher_active_fetches 2:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_active_extractors 3"),
            "missing dfe_fetcher_active_extractors 3:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_memory_used_bytes 123456"),
            "missing dfe_fetcher_memory_used_bytes 123456:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_memory_limit_bytes 789012"),
            "missing dfe_fetcher_memory_limit_bytes 789012:\n{output}"
        );

        // Rate gauges are present (values may be 0.0 if the rate window hasn't
        // produced a non-zero derivative, but the metric family must exist).
        assert!(
            output.contains("dfe_fetcher_fetch_rate "),
            "missing dfe_fetcher_fetch_rate gauge:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_events_per_second "),
            "missing dfe_fetcher_events_per_second gauge:\n{output}"
        );
    }

    // ==========================================================================
    // Methods that are no-ops without ServiceMetrics must not panic
    // ==========================================================================

    #[test]
    fn test_record_fetch_duration_without_dfe_is_noop() {
        let metrics = Metrics::new();
        // Must not panic when no global recorder / ServiceMetrics is configured.
        metrics.record_fetch_duration("aws", Duration::from_millis(250));
        metrics.record_fetch_duration("azure", Duration::from_secs(2));
    }

    #[test]
    fn test_set_cursor_age_without_dfe_is_noop() {
        let metrics = Metrics::new();
        metrics.set_cursor_age("aws", 3600.0);
        metrics.set_cursor_age("m365", 0.0);
    }

    #[test]
    fn test_inc_api_error_without_dfe_is_noop() {
        let metrics = Metrics::new();
        metrics.inc_api_error("aws", "5xx");
        metrics.inc_api_error("azure", "timeout");
        metrics.inc_api_error("m365", "4xx");
        metrics.inc_api_error("gcp", "network");
        metrics.inc_api_error("salesforce", "throttle");
    }

    #[test]
    fn test_fetch_pressure_ratio_zero_when_cap_unset() {
        let metrics = Metrics::new();
        metrics.inc_active_fetches();
        // No concurrency cap seeded -> signal contributes 0.
        assert!(metrics.fetch_pressure_ratio().abs() < f64::EPSILON);
    }

    #[test]
    fn test_fetch_pressure_ratio_self_normalised() {
        let metrics = Metrics::new();
        metrics.set_concurrency_cap(4);
        metrics.inc_active_fetches();
        metrics.inc_active_fetches();
        // 2 active / cap 4 = 0.5.
        assert!((metrics.fetch_pressure_ratio() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_fetch_pressure_ratio_saturates_at_one() {
        let metrics = Metrics::new();
        metrics.set_concurrency_cap(2);
        metrics.inc_active_fetches();
        metrics.inc_active_fetches();
        metrics.inc_active_fetches();
        // 3 active / cap 2 clamps to 1.0 (semaphore saturated).
        assert!((metrics.fetch_pressure_ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_throttle_ratio_zero_before_attempts() {
        let metrics = Metrics::new();
        assert!(metrics.throttle_ratio().abs() < f64::EPSILON);
    }

    #[test]
    fn test_throttle_ratio_counts_only_throttle_code() {
        let metrics = Metrics::new();
        // Four attempts (two success, two error), one classified as throttle.
        metrics.inc_fetches_success_for("aws");
        metrics.inc_fetches_success_for("aws");
        metrics.inc_fetches_error_for("aws");
        metrics.inc_fetches_error_for("aws");
        metrics.inc_api_error("aws", "throttle");
        metrics.inc_api_error("aws", "4xx"); // NOT a throttle -> not counted.
        // 1 throttle / 4 attempts = 0.25.
        assert!((metrics.throttle_ratio() - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_inc_ingest_request_without_dfe_is_noop() {
        let metrics = Metrics::new();
        metrics.inc_ingest_request("success");
        metrics.inc_ingest_request("error");
        metrics.inc_ingest_request("unauthorized");
    }

    #[test]
    fn test_record_ingest_duration_without_dfe_is_noop() {
        let metrics = Metrics::new();
        metrics.record_ingest_duration(Duration::from_millis(5));
        metrics.record_ingest_duration(Duration::from_millis(500));
    }

    #[test]
    fn test_update_rate_gauge_without_dfe_is_noop() {
        let metrics = Metrics::new();
        // Should silently return without a ServiceMetrics registered.
        metrics.update_rate_gauge();
        // Feeding the rate window first still shouldn't panic.
        metrics.inc_fetches_success();
        metrics.add_records_fetched(100);
        metrics.update_rate_gauge();
    }

    // ==========================================================================
    // Multi-source labelling keeps aggregate counters consistent
    // ==========================================================================

    #[test]
    fn test_multiple_source_labels_aggregate_correctly() {
        let metrics = Metrics::new();
        metrics.inc_fetches_success_for("aws");
        metrics.inc_fetches_success_for("azure");
        metrics.inc_fetches_error_for("m365");

        let output = metrics.render();
        // Aggregate success = 2, error = 1. Labels aren't emitted by render(),
        // but the atomic aggregates must reflect all calls.
        assert!(
            output.contains("dfe_fetcher_fetches_total{status=\"success\"} 2"),
            "expected aggregate success=2, got:\n{output}"
        );
        assert!(
            output.contains("dfe_fetcher_fetches_total{status=\"error\"} 1"),
            "expected aggregate error=1, got:\n{output}"
        );
    }

    // ==========================================================================
    // add_records_fetched edge cases
    // ==========================================================================

    #[test]
    fn test_add_records_fetched_with_zero_is_noop() {
        let metrics = Metrics::new();
        // add_records_fetched(0) must not panic and must leave the counter at 0.
        metrics.add_records_fetched(0);
        let output = metrics.render();
        assert!(
            output.contains("dfe_records_received_total 0"),
            "expected 0 records after zero-add, got:\n{output}"
        );
    }

    #[test]
    fn test_events_per_second_after_many_records() {
        let metrics = Metrics::new();
        // Push a large batch through the records rate window. We don't assert
        // a specific rate (timing-dependent), but the accessor must not panic
        // and must return a finite non-negative value.
        for _ in 0..100 {
            metrics.add_records_fetched(1_000);
        }
        let eps = metrics.events_per_second();
        assert!(
            eps.is_finite(),
            "events_per_second must be finite, got {eps}"
        );
        assert!(eps >= 0.0, "events_per_second must be >= 0, got {eps}");
    }

    /// Exercise the `Metrics::with_dfe()` constructor end-to-end so the
    /// describe_* + ServiceMetrics::register path is covered, and so every
    /// `if self.dfe.is_some()` branch across the hot-path recording
    /// methods gets executed. A `MetricsManager` is installed as the
    /// global recorder for the duration of this test.
    #[test]
    fn test_with_dfe_exercises_all_dual_emit_paths() {
        use scalo::metrics::MetricsManager;

        // Install a MetricsManager (it registers the global metrics recorder).
        // Use a distinct namespace so this test doesn't collide with any other
        // test trying to install a recorder in a serial run.
        let manager = MetricsManager::new("dfe_fetcher_test_with_dfe");

        let metrics = Metrics::with_dfe(&manager);

        // Fetch counters (source-labelled)
        metrics.inc_fetches_success_for("aws");
        metrics.inc_fetches_error_for("azure");
        metrics.inc_api_error("aws", "5xx");

        // Bytes / records
        metrics.add_records_fetched(17);
        metrics.add_bytes_fetched(4096);

        // Delivery / DLQ
        metrics.add_messages_sent_kafka(3);
        metrics.inc_messages_dlq();

        // Extractor counters
        metrics.inc_extractor_runs_total();
        metrics.inc_extractor_runs_success();
        metrics.inc_extractor_runs_error();
        metrics.add_extractor_records(7);
        metrics.inc_extractor_run_for("my-tool", "success");
        metrics.inc_extractor_run_for("my-tool", "error");
        metrics.inc_extractor_run_for("my-tool", "other"); // unknown status branch
        metrics.inc_extractor_restart_exhausted();

        // Ingest
        metrics.inc_ingest_request("success");
        metrics.record_ingest_duration(Duration::from_millis(5));

        // Backpressure / transport health
        metrics.inc_transport_backpressured();
        metrics.inc_transport_send_errors();
        metrics.set_transport_healthy(true);
        metrics.set_transport_healthy(false);

        // Filtering / cursors / pipeline
        metrics.inc_records_filtered();
        metrics.inc_cursor_writes();
        metrics.inc_cursor_write_failures();
        metrics.set_pipeline_ready(true);
        metrics.set_pipeline_ready(false);
        metrics.inc_records_delivered();

        // Gauges (active fetches/extractors)
        metrics.inc_active_fetches();
        metrics.inc_active_fetches();
        metrics.dec_active_fetches();
        metrics.inc_active_extractors();
        metrics.dec_active_extractors();
        metrics.dec_active_extractors(); // saturates at 0

        // Memory + rate gauges
        metrics.set_memory_usage(10_000, 100_000);
        metrics.update_rate_gauge();

        // Duration histograms
        metrics.record_fetch_duration("aws", Duration::from_millis(42));
        metrics.set_cursor_age("aws.cloudtrail", 120.0);

        // Verify the local atomics are in sync (render path doesn't depend
        // on the global recorder, so this confirms the hot-path arms ran)
        let output = metrics.render();
        assert!(
            output.contains("dfe_fetcher_fetches_total{status=\"success\"} 1"),
            "render must show 1 success fetch"
        );
        assert!(
            output.contains("dfe_fetcher_fetches_total{status=\"error\"} 1"),
            "render must show 1 error fetch"
        );
        assert!(
            output.contains("dfe_records_received_total 17"),
            "render must show 17 records received"
        );
        assert!(
            output.contains("dfe_fetcher_bytes_received_total 4096"),
            "render must show 4096 bytes"
        );
        assert!(
            output.contains("dfe_transport_sent_total 3"),
            "render must show 3 messages sent"
        );
        assert!(
            output.contains("dfe_records_dlq_total 1"),
            "render must show 1 DLQ record"
        );
    }
}
