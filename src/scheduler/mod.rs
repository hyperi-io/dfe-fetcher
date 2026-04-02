// Project:   dfe-fetcher
// File:      src/scheduler/mod.rs
// Purpose:   Fetch scheduling with jitter and concurrency control
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Fetch scheduler module.
//!
//! Manages the timing and concurrency of fetch operations across all
//! sources and extractors. Supports:
//!
//! - Configurable intervals per source (hot-reloaded from shared config)
//! - Jitter to avoid thundering herd (hot-reloaded)
//! - Concurrency limiting across all sources
//! - Graceful shutdown with in-flight fetch completion

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::{SchedulerConfig, SharedConfig};
use crate::cursor::{CursorStore, CursorValue};
use crate::metrics::Metrics;
use crate::source::{FetchResult, FetchWindow, Source};

/// Fetch scheduler that coordinates timing and concurrency.
///
/// The scheduler reads `default_interval_secs`, `jitter_percent`, and
/// `default_window_hours` from `SharedConfig` on every tick so that
/// config hot-reloads take effect without a pod restart.
pub struct Scheduler {
    shared_config: SharedConfig,
    concurrency_semaphore: Arc<Semaphore>,
    cursor_store: Option<Arc<dyn CursorStore>>,
    instance_id: String,
}

impl Scheduler {
    /// Create a new scheduler from configuration.
    pub fn new(
        config: &SchedulerConfig,
        shared_config: SharedConfig,
        cursor_store: Option<Arc<dyn CursorStore>>,
        instance_id: String,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_fetches));

        Self {
            shared_config,
            concurrency_semaphore: semaphore,
            cursor_store,
            instance_id,
        }
    }

    /// Spawn a recurring fetch task for a source.
    ///
    /// The interval and jitter are re-read from `SharedConfig` on each tick
    /// so that config hot-reloads take effect without a restart. The
    /// `source_interval` override (per-source) is baked at spawn time.
    ///
    /// The `is_ready` callback is polled before each fetch. When it returns
    /// `false` (e.g. output transports are backpressured or unhealthy), the
    /// task stalls with a 5-second poll interval instead of fetching and
    /// routing everything to DLQ.
    ///
    /// When a cursor store is configured, the scheduler reads the last fetch
    /// position before each fetch to compute a `FetchWindow`, and writes the
    /// cursor back after successful delivery.
    pub fn spawn_source_task(
        &self,
        source: Arc<dyn Source>,
        source_interval: Option<u64>,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
        callback: Arc<dyn Fn(Vec<FetchResult>) + Send + Sync>,
        is_ready: Arc<dyn Fn() -> bool + Send + Sync>,
    ) {
        let semaphore = Arc::clone(&self.concurrency_semaphore);
        let cursor_store = self.cursor_store.clone();
        let instance_id = self.instance_id.clone();
        let shared_config = self.shared_config.clone();

        tokio::spawn(async move {
            let cursor_key = format!("{}.{}", instance_id, source.cursor_prefix());

            // Initial sleep before first fetch (let startup complete).
            // Read interval from current config so even the first tick is dynamic.
            {
                let config = shared_config.get();
                let base_secs = source_interval.unwrap_or(config.scheduler.default_interval_secs);
                let jitter = calculate_jitter(base_secs, config.scheduler.jitter_percent);
                let sleep_duration = Duration::from_secs(base_secs + jitter);
                tokio::select! {
                    _ = tokio::time::sleep(sleep_duration) => {}
                    _ = shutdown.cancelled() => {
                        info!(source = source.name(), "Scheduler shutting down during initial wait");
                        return;
                    }
                }
            }

            loop {
                // Compute interval from CURRENT config (hot-reloaded)
                let config = shared_config.get();
                let base_secs = source_interval.unwrap_or(config.scheduler.default_interval_secs);
                let jitter = calculate_jitter(base_secs, config.scheduler.jitter_percent);
                let default_window_hours = config.cursor.default_window_hours;

                // Wait for pipeline readiness (backpressure stall)
                while !is_ready() {
                    {
                        use std::sync::atomic::AtomicU64;
                        static BACKPRESSURE_DEBOUNCE: AtomicU64 = AtomicU64::new(0);
                        if hyperi_rustlib::logger::log_debounced(&BACKPRESSURE_DEBOUNCE, 10_000) {
                            warn!(
                                source = source.name(),
                                "Pipeline not ready (backpressure), waiting"
                            );
                        }
                    }
                    debug!(source = source.name(), "Backpressure stall — delaying fetch");
                    metrics.inc_transport_backpressured();
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                        _ = shutdown.cancelled() => {
                            info!(source = source.name(), "Scheduler shutting down during backpressure wait");
                            return;
                        }
                    }
                }

                // Acquire concurrency permit
                let permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!(source = source.name(), "Semaphore closed, stopping");
                        break;
                    }
                };

                metrics.inc_active_fetches();

                // Read cursor to compute fetch window
                let window = build_fetch_window(
                    cursor_store.as_deref(),
                    &cursor_key,
                    default_window_hours,
                    &metrics,
                    &source.cursor_prefix(),
                )
                .await;

                debug!(
                    source = source.name(),
                    cursor_key,
                    window_start = %window.start,
                    window_end = %window.end,
                    "Fetch window computed"
                );

                debug!(
                    source = source.name(),
                    window_start = %window.start,
                    window_end = %window.end,
                    window_hours = (window.end - window.start).num_seconds() as f64 / 3600.0,
                    "Fetch started"
                );

                let fetch_start = std::time::Instant::now();
                match source.fetch(Some(&window)).await {
                    Ok(results) => {
                        let fetch_duration = fetch_start.elapsed();
                        let fetch_duration_ms = fetch_duration.as_millis();
                        metrics.record_fetch_duration(&source.cursor_prefix(), fetch_duration);

                        let total_records: usize = results.iter().map(|r| r.records.len()).sum();
                        let total_bytes: usize = results
                            .iter()
                            .flat_map(|r| r.records.iter())
                            .map(|b| b.len())
                            .sum();
                        metrics.inc_fetches_success_for(&source.cursor_prefix());
                        metrics.add_records_fetched(total_records as u64);

                        debug!(
                            source = source.name(),
                            records = total_records,
                            bytes = total_bytes,
                            duration_ms = fetch_duration_ms,
                            "Fetch complete"
                        );

                        if total_records > 0 {
                            info!(
                                source = source.name(),
                                records = total_records,
                                duration_ms = fetch_duration_ms,
                                "Fetch completed with records"
                            );
                            callback(results);
                        } else {
                            debug!(
                                source = source.name(),
                                duration_ms = fetch_duration_ms,
                                "Fetch completed — no new records in window"
                            );
                        }

                        // Write cursor after successful fetch + delivery
                        write_cursor(
                            cursor_store.as_deref(),
                            &cursor_key,
                            &window,
                            total_records as u64,
                            &metrics,
                        )
                        .await;
                    }
                    Err(e) => {
                        let fetch_duration = fetch_start.elapsed();
                        metrics.record_fetch_duration(&source.cursor_prefix(), fetch_duration);

                        let code = crate::source::classify_api_error(&e);
                        metrics.inc_api_error(&source.cursor_prefix(), code);

                        metrics.inc_fetches_error_for(&source.cursor_prefix());
                        error!(
                            source = source.name(),
                            error = %e,
                            error_code = code,
                            duration_ms = fetch_duration.as_millis(),
                            "Fetch failed"
                        );
                    }
                }

                metrics.dec_active_fetches();
                drop(permit);

                // Sleep until next fetch cycle (interval re-computed per tick)
                let sleep_duration = Duration::from_secs(base_secs + jitter);
                tokio::select! {
                    _ = tokio::time::sleep(sleep_duration) => {}
                    _ = shutdown.cancelled() => {
                        info!(source = source.name(), "Scheduler shutting down");
                        break;
                    }
                }
            }
        });
    }

    /// Calculate the effective interval for a source (for logging at startup).
    ///
    /// This reads the current shared config. In the spawn loop, the interval
    /// is re-computed on each tick so hot-reloaded values take effect.
    pub fn effective_interval(&self, source_interval: Option<u64>) -> Duration {
        let config = self.shared_config.get();
        let base_secs = source_interval.unwrap_or(config.scheduler.default_interval_secs);
        let jitter_secs = calculate_jitter(base_secs, config.scheduler.jitter_percent);
        Duration::from_secs(base_secs + jitter_secs)
    }
}

/// Calculate jitter amount based on configured percentage.
fn calculate_jitter(base_secs: u64, jitter_percent: u8) -> u64 {
    if jitter_percent == 0 {
        return 0;
    }

    let max_jitter = base_secs * u64::from(jitter_percent) / 100;
    if max_jitter == 0 {
        return 0;
    }

    fastrand::u64(0..max_jitter)
}

/// Build a `FetchWindow` from the cursor store. If no cursor exists or the
/// read fails, falls back to `now - default_window_hours`.
///
/// When a cursor is found, records its age (seconds since `last_fetch_end`)
/// as `dfe_fetcher_cursor_age_seconds` for staleness monitoring.
async fn build_fetch_window(
    store: Option<&dyn CursorStore>,
    cursor_key: &str,
    default_window_hours: u64,
    metrics: &Metrics,
    source_prefix: &str,
) -> FetchWindow {
    let now = Utc::now();

    if let Some(store) = store {
        match store.get(cursor_key).await {
            Ok(Some(cursor)) => {
                let age_secs = (now - cursor.last_fetch_end).num_seconds().max(0) as f64;
                debug!(
                    cursor_key,
                    last_end = %cursor.last_fetch_end,
                    age_secs,
                    last_records = cursor.last_fetch_records,
                    "Cursor found, resuming from last position"
                );
                tracing::trace!(
                    cursor_key,
                    last_end = %cursor.last_fetch_end,
                    updated_at = %cursor.updated_at,
                    api_cursor = cursor.api_cursor.as_deref().unwrap_or("none"),
                    version = cursor.version,
                    "Cursor details"
                );
                metrics.set_cursor_age(source_prefix, age_secs);
                return FetchWindow {
                    start: cursor.last_fetch_end,
                    end: now,
                };
            }
            Ok(None) => {
                debug!(
                    cursor_key,
                    default_window_hours,
                    "No cursor found, using default lookback window"
                );
            }
            Err(e) => {
                warn!(cursor_key, error = %e, "Cursor read failed, using default lookback");
            }
        }
    }

    #[allow(clippy::cast_possible_wrap)]
    let start = now - chrono::Duration::hours(default_window_hours as i64);
    FetchWindow { start, end: now }
}

/// Write cursor after a successful fetch. Logs warning on failure but does
/// not propagate the error — cursor failures must not block the pipeline.
async fn write_cursor(
    store: Option<&dyn CursorStore>,
    cursor_key: &str,
    window: &FetchWindow,
    records: u64,
    metrics: &Metrics,
) {
    let Some(store) = store else { return };

    let value = CursorValue {
        cursor_key: cursor_key.to_string(),
        last_fetch_end: window.end,
        last_fetch_records: records,
        updated_at: Utc::now(),
        api_cursor: None,
        version: 1,
    };

    match store.set(cursor_key, &value).await {
        Ok(()) => {
            metrics.inc_cursor_writes();
            debug!(
                cursor_key,
                end = %window.end,
                records,
                "Cursor position written"
            );
            tracing::trace!(
                cursor_key,
                window_start = %window.start,
                window_end = %window.end,
                records,
                "Cursor write details"
            );
        }
        Err(e) => {
            metrics.inc_cursor_write_failures();
            warn!(cursor_key, error = %e, end = %window.end, "Cursor write failed — position not persisted");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a test config with zero jitter for deterministic assertions.
    fn test_config_no_jitter() -> crate::config::Config {
        let mut cfg = crate::config::Config::default();
        cfg.scheduler.jitter_percent = 0;
        cfg
    }

    #[test]
    fn test_effective_interval_default() {
        let config = SchedulerConfig {
            default_interval_secs: 300,
            max_concurrent_fetches: 10,
            jitter_percent: 0,
        };
        let shared = SharedConfig::new(test_config_no_jitter());
        let scheduler = Scheduler::new(&config, shared, None, "test".into());

        let interval = scheduler.effective_interval(None);
        assert_eq!(interval.as_secs(), 300);
    }

    #[test]
    fn test_effective_interval_override() {
        let config = SchedulerConfig {
            default_interval_secs: 300,
            max_concurrent_fetches: 10,
            jitter_percent: 0,
        };
        let shared = SharedConfig::new(test_config_no_jitter());
        let scheduler = Scheduler::new(&config, shared, None, "test".into());

        let interval = scheduler.effective_interval(Some(60));
        assert_eq!(interval.as_secs(), 60);
    }

    #[test]
    fn test_jitter_bounded() {
        let config = SchedulerConfig {
            default_interval_secs: 300,
            max_concurrent_fetches: 10,
            jitter_percent: 10,
        };
        let jitter = calculate_jitter(300, config.jitter_percent);
        assert!(jitter <= 30); // 10% of 300 = 30
    }

    #[test]
    fn test_jitter_zero_percent() {
        assert_eq!(calculate_jitter(300, 0), 0);
    }

    #[test]
    fn test_jitter_zero_base() {
        assert_eq!(calculate_jitter(0, 10), 0);
    }

    #[tokio::test]
    async fn test_build_fetch_window_no_store() {
        let metrics = Metrics::new();
        let window = build_fetch_window(None, "test.key", 2, &metrics, "test").await;
        let expected_start = Utc::now() - chrono::Duration::hours(2);
        // Allow 1 second tolerance
        assert!((window.start - expected_start).num_seconds().abs() < 2);
        assert!((window.end - Utc::now()).num_seconds().abs() < 2);
    }

    #[tokio::test]
    async fn test_build_fetch_window_with_cursor() {
        use crate::cursor::file::FileCursorStore;
        use crate::cursor::{CursorStore, CursorValue};

        let dir = tempfile::TempDir::new().unwrap();
        let store = FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();

        let last_end = Utc::now() - chrono::Duration::minutes(10);
        let cursor = CursorValue {
            cursor_key: "test.key".to_string(),
            last_fetch_end: last_end,
            last_fetch_records: 50,
            updated_at: Utc::now(),
            api_cursor: None,
            version: 1,
        };
        store.set("test.key", &cursor).await.unwrap();

        let metrics = Metrics::new();
        let window = build_fetch_window(Some(&store), "test.key", 2, &metrics, "test").await;

        // Window should start from cursor, not default lookback
        assert!(
            (window.start - last_end).num_seconds().abs() < 2,
            "window.start should match cursor.last_fetch_end"
        );
        assert!(window.end > window.start);
    }

    /// Test that the scheduler stalls when is_ready returns false,
    /// and resumes when it returns true. Uses tokio::time::pause()
    /// to control time without real delays.
    #[tokio::test]
    async fn test_scheduler_stalls_on_backpressure() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicU64};

        use crate::source::{FetchResult, Source};

        // Mock source that counts fetch calls
        struct CountingSource {
            fetch_count: AtomicU64,
        }

        #[async_trait::async_trait]
        impl Source for CountingSource {
            fn name(&self) -> &'static str {
                "counting"
            }
            fn is_enabled(&self) -> bool {
                true
            }
            async fn fetch(
                &self,
                _window: Option<&crate::source::FetchWindow>,
            ) -> crate::error::Result<Vec<FetchResult>> {
                self.fetch_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(vec![])
            }
            async fn health_check(&self) -> crate::error::Result<bool> {
                Ok(true)
            }
        }

        tokio::time::pause();

        // Very short interval so the test runs fast with paused time
        let mut cfg = crate::config::Config::default();
        cfg.scheduler.default_interval_secs = 1;
        cfg.scheduler.jitter_percent = 0;
        let shared = SharedConfig::new(cfg);

        let scheduler_config = SchedulerConfig {
            default_interval_secs: 1,
            max_concurrent_fetches: 10,
            jitter_percent: 0,
        };

        let scheduler = Scheduler::new(&scheduler_config, shared, None, "test".into());

        let source = Arc::new(CountingSource {
            fetch_count: AtomicU64::new(0),
        });
        let metrics = Arc::new(Metrics::new());
        let shutdown = CancellationToken::new();

        // Start with is_ready = false (backpressured)
        let ready = Arc::new(AtomicBool::new(false));
        let ready_clone = ready.clone();
        let is_ready = Arc::new(move || ready_clone.load(std::sync::atomic::Ordering::Relaxed));

        let callback = Arc::new(|_results: Vec<FetchResult>| {});

        scheduler.spawn_source_task(
            source.clone(),
            Some(1),
            metrics.clone(),
            shutdown.clone(),
            callback,
            is_ready,
        );

        // Advance time past the initial sleep + several backpressure poll intervals
        // (step in 1s increments to let spawned task run)
        for _ in 0..20 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        // No fetches should have happened — pipeline was not ready
        let fetches_while_stalled = source
            .fetch_count
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            fetches_while_stalled, 0,
            "scheduler should NOT fetch while backpressured"
        );

        // Mark pipeline as ready
        ready.store(true, std::sync::atomic::Ordering::Relaxed);

        // Advance time for the stall poll to detect readiness + one fetch cycle
        for _ in 0..15 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let fetches_after_ready = source
            .fetch_count
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            fetches_after_ready >= 1,
            "scheduler should fetch after pipeline becomes ready, got {fetches_after_ready}"
        );

        shutdown.cancel();
    }

    /// Test that changing SharedConfig interval is picked up by the scheduler
    /// on the next tick (hot-reload).
    #[tokio::test]
    async fn test_scheduler_hot_reload_interval() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU64;

        use crate::source::{FetchResult, Source};

        struct CountingSource {
            fetch_count: AtomicU64,
        }

        #[async_trait::async_trait]
        impl Source for CountingSource {
            fn name(&self) -> &'static str {
                "counting"
            }
            fn is_enabled(&self) -> bool {
                true
            }
            async fn fetch(
                &self,
                _window: Option<&crate::source::FetchWindow>,
            ) -> crate::error::Result<Vec<FetchResult>> {
                self.fetch_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(vec![])
            }
            async fn health_check(&self) -> crate::error::Result<bool> {
                Ok(true)
            }
        }

        tokio::time::pause();

        let mut cfg = crate::config::Config::default();
        cfg.scheduler.default_interval_secs = 2;
        cfg.scheduler.jitter_percent = 0;
        let shared = SharedConfig::new(cfg);

        let scheduler_config = SchedulerConfig {
            default_interval_secs: 2,
            max_concurrent_fetches: 10,
            jitter_percent: 0,
        };
        let scheduler = Scheduler::new(&scheduler_config, shared.clone(), None, "test".into());

        let source = Arc::new(CountingSource {
            fetch_count: AtomicU64::new(0),
        });
        let metrics = Arc::new(Metrics::new());
        let shutdown = CancellationToken::new();
        let is_ready = Arc::new(|| true);
        let callback = Arc::new(|_results: Vec<FetchResult>| {});

        scheduler.spawn_source_task(
            source.clone(),
            None, // use config default
            metrics,
            shutdown.clone(),
            callback,
            is_ready,
        );

        // Advance past initial sleep (2s) in small steps to let spawned task run
        for _ in 0..10 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let count_before = source
            .fetch_count
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            count_before >= 1,
            "should have at least 1 fetch after 10s with 2s interval, got {count_before}"
        );

        // Hot-reload: change interval to 100s (effectively stop fetching)
        let mut new_cfg = crate::config::Config::default();
        new_cfg.scheduler.default_interval_secs = 100;
        new_cfg.scheduler.jitter_percent = 0;
        shared.update(new_cfg);

        // Advance 10s in steps — should NOT trigger many more fetches (interval is now 100s)
        let count_snapshot = source
            .fetch_count
            .load(std::sync::atomic::Ordering::Relaxed);
        for _ in 0..10 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let count_after = source
            .fetch_count
            .load(std::sync::atomic::Ordering::Relaxed);
        // At most 1 more fetch could sneak in from the previous cycle
        assert!(
            count_after <= count_snapshot + 1,
            "hot-reload should slow fetches: before={count_snapshot}, after={count_after}"
        );

        shutdown.cancel();
    }
}
