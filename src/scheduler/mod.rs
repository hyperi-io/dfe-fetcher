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

                metrics.inc_fetches_total();
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

                let fetch_start = std::time::Instant::now();
                match source.fetch(Some(&window)).await {
                    Ok(results) => {
                        let fetch_duration = fetch_start.elapsed();
                        metrics.record_fetch_duration(&source.cursor_prefix(), fetch_duration);

                        let total_records: usize = results.iter().map(|r| r.records.len()).sum();
                        metrics.inc_fetches_success();
                        metrics.add_records_fetched(total_records as u64);

                        if total_records > 0 {
                            info!(
                                source = source.name(),
                                records = total_records,
                                "Fetch completed"
                            );
                            callback(results);
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

                        metrics.inc_fetches_error();
                        error!(
                            source = source.name(),
                            error = %e,
                            error_code = code,
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
                debug!(cursor_key, last_end = %cursor.last_fetch_end, "Cursor found, resuming");
                let age_secs = (now - cursor.last_fetch_end).num_seconds().max(0) as f64;
                metrics.set_cursor_age(source_prefix, age_secs);
                return FetchWindow {
                    start: cursor.last_fetch_end,
                    end: now,
                };
            }
            Ok(None) => {
                debug!(cursor_key, "No cursor found, using default lookback");
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
            debug!(cursor_key, end = %window.end, "Cursor updated");
        }
        Err(e) => {
            metrics.inc_cursor_write_failures();
            warn!(cursor_key, error = %e, "Cursor write failed");
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
}
