// Project:   dfe-fetcher
// File:      crates/fetcher/src/scheduler/mod.rs
// Purpose:   Fetch scheduling with jitter and concurrency control
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Fetch scheduler module.
//!
//! Manages the timing and concurrency of fetch operations across every
//! connection's [`Driver`]. Supports:
//!
//! - Configurable intervals per source (hot-reloaded from shared config)
//! - Jitter to avoid thundering herd (hot-reloaded)
//! - Concurrency limiting across all sources
//! - Graceful shutdown with in-flight fetch completion

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::{MissingCursor, SchedulerConfig, SharedConfig};
use crate::cursor::{CursorStore, CursorValue};
use crate::driver::Driver;
use crate::error::Error;
use crate::metrics::Metrics;
use dfe_fetcher_core::FetchWindow;

/// Fetch scheduler that coordinates timing and concurrency.
///
/// The scheduler reads `default_interval_secs`, `jitter_percent`, and
/// `default_window_hours` from `SharedConfig` on every tick so that
/// config hot-reloads take effect without a pod restart.
///
/// A tick's window is never wider than `default_window_hours`, and a tick
/// that fails is retried over the width that failed, so a source catching up
/// after an outage drains in bounded steps instead of asking for everything
/// at once.
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

    /// Spawn a recurring fetch task for one connection's driver.
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
    /// cursor back once the tick has awaited every acknowledgement.
    pub fn spawn_source_task(
        &self,
        driver: Arc<Driver>,
        source_interval: Option<u64>,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
        is_ready: Arc<dyn Fn() -> bool + Send + Sync>,
    ) {
        let semaphore = Arc::clone(&self.concurrency_semaphore);
        let cursor_store = self.cursor_store.clone();
        let instance_id = self.instance_id.clone();
        let shared_config = self.shared_config.clone();

        tokio::spawn(async move {
            // The cursor key and the per-source metric and log labels are keyed
            // on the CONNECTION id, not the type name, so each account or
            // tenant of a type checkpoints and reports independently; a
            // single implicit connection's id is the type name, so its cursor
            // key and labels are unchanged.
            let label = driver.name();
            let cursor_key = format!("{instance_id}.{label}");
            let source_log = SourceLog::new(label);

            // Initial sleep before first fetch (let startup complete).
            // Read interval from current config so even the first tick is dynamic.
            {
                let config = shared_config.get();
                let base_secs = source_interval.unwrap_or(config.scheduler.default_interval_secs);
                let jitter = calculate_jitter(base_secs, config.scheduler.jitter_percent);
                let sleep_duration = Duration::from_secs(base_secs + jitter);
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        info!(source = label, "Scheduler shutting down during initial wait");
                        return;
                    }
                    () = tokio::time::sleep(sleep_duration) => {}
                }
            }

            // The width of the last failed attempt. A reload never respawns a
            // live source's task, so a frozen span survives one.
            let mut retry_span: Option<chrono::Duration> = None;

            loop {
                // Compute interval from CURRENT config (hot-reloaded)
                let config = shared_config.get();
                let base_secs = source_interval.unwrap_or(config.scheduler.default_interval_secs);
                let jitter = calculate_jitter(base_secs, config.scheduler.jitter_percent);
                let max_span = max_window_span(config.cursor.default_window_hours, base_secs);

                // Wait for pipeline readiness (backpressure stall)
                while !is_ready() {
                    {
                        use std::sync::atomic::AtomicU64;
                        static BACKPRESSURE_DEBOUNCE: AtomicU64 = AtomicU64::new(0);
                        if scalo::logger::log_debounced(&BACKPRESSURE_DEBOUNCE, REPEAT_LOG_EVERY_MS)
                        {
                            warn!(source = label, "Pipeline not ready (backpressure), waiting");
                        }
                    }
                    debug!(source = label, "Backpressure stall -- delaying fetch");
                    metrics.inc_transport_backpressured();
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => {
                            info!(source = label, "Scheduler shutting down during backpressure wait");
                            return;
                        }
                        () = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                }

                // Acquire concurrency permit
                let permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!(source = label, "Semaphore closed, stopping");
                        break;
                    }
                };

                metrics.inc_active_fetches();
                let next_tick = Duration::from_secs(base_secs + jitter);

                // Read cursor to compute fetch window. Lowering
                // `default_window_hours` mid-freeze narrows the retry --
                // raising it does not widen it.
                let window = match build_fetch_window(
                    cursor_store.as_deref(),
                    &cursor_key,
                    max_span,
                    retry_span.map(|span| span.min(max_span)),
                    config.cursor.on_missing_cursor,
                    &metrics,
                    &source_log,
                )
                .await
                {
                    Ok(window) => window,
                    // Logged, debounced by cause, where it is built.
                    Err(_) => {
                        metrics.inc_fetches_error_for(label);
                        metrics.dec_active_fetches();
                        drop(permit);
                        if wait_for_next_tick(&shutdown, next_tick, label).await {
                            break;
                        }
                        continue;
                    }
                };

                debug!(
                    source = label,
                    cursor_key,
                    window_start = %window.start,
                    window_end = %window.end,
                    window_hours = (window.end - window.start).num_seconds() as f64 / 3600.0,
                    retry = retry_span.is_some(),
                    "Fetch started"
                );

                let fetch_start = std::time::Instant::now();
                match driver.run_tick(Some(&window)).await {
                    Ok(report) => {
                        let fetch_duration = fetch_start.elapsed();
                        let fetch_duration_ms = fetch_duration.as_millis();
                        metrics.record_fetch_duration(label, fetch_duration);

                        let total_records = report.rows;
                        metrics.inc_fetches_success_for(label);
                        metrics.add_records_fetched(total_records);
                        metrics.add_bytes_fetched(report.bytes);

                        if total_records > 0 {
                            info!(
                                source = label,
                                records = total_records,
                                bytes = report.bytes,
                                duration_ms = fetch_duration_ms,
                                "Fetch completed with records"
                            );
                        } else {
                            debug!(
                                source = label,
                                duration_ms = fetch_duration_ms,
                                "Fetch completed -- no new records in window"
                            );
                        }

                        // The driver has awaited every acknowledgement inside
                        // run_tick, so the window is safe to advance past.
                        retry_span = None;
                        write_cursor(
                            cursor_store.as_deref(),
                            &cursor_key,
                            &window,
                            total_records,
                            &metrics,
                        )
                        .await;
                    }
                    // Shutdown is not a fetch failure; counting it as a
                    // network error would alert on every rollout.
                    Err(Error::Shutdown) => {
                        metrics.record_fetch_duration(label, fetch_start.elapsed());
                        info!(source = label, "Fetch stopped for shutdown");
                    }
                    Err(e) => {
                        let fetch_duration = fetch_start.elapsed();
                        metrics.record_fetch_duration(label, fetch_duration);

                        // The cursor did not move, so without this the next
                        // window would be this one plus another interval --
                        // wider than the width that just failed.
                        retry_span = Some(window.end - window.start);

                        let code = classify_api_error(&e);
                        metrics.inc_api_error(label, code);

                        metrics.inc_fetches_error_for(label);
                        error!(
                            source = label,
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
                if wait_for_next_tick(&shutdown, next_tick, label).await {
                    break;
                }
            }
        });
    }

    /// Watch the shared config and cancel the fetch task of any source a
    /// reload has removed.
    ///
    /// `running` maps a connection id to the child token its fetch task and
    /// its driver hold, and the watcher is its only writer -- no lock. The
    /// diff is on IDENTITY alone: a source whose settings changed but whose id
    /// is still configured keeps running, because the driver and the scheduler
    /// re-read the config every tick.
    pub fn spawn_source_watch(
        &self,
        mut running: HashMap<String, CancellationToken>,
        shutdown: CancellationToken,
    ) {
        let shared_config = self.shared_config.clone();

        tokio::spawn(async move {
            let mut versions = shared_config.subscribe();

            // Reconcile once up front: a reload between the startup spawn and
            // this subscription would otherwise go unseen.
            reconcile_running_sources(&shared_config, &mut running);

            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    changed = versions.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
                reconcile_running_sources(&shared_config, &mut running);
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

/// Cancel the fetch task of every running source the live config no longer
/// schedules, and warn about a source it schedules that is not running.
///
/// Cancelling the child token stops the task at its next select and aborts an
/// in-flight tick through the driver's non-alerting shutdown path. Adding a
/// source still needs a restart (dfe-fetcher#134), so its id is named in a
/// warning rather than silently ignored.
fn reconcile_running_sources(
    shared_config: &SharedConfig,
    running: &mut HashMap<String, CancellationToken>,
) {
    let desired: BTreeSet<String> = shared_config.with(|config| {
        config
            .sources
            .scheduled_connection_ids()
            .into_iter()
            .map(str::to_owned)
            .collect()
    });

    running.retain(|id, cancel| {
        if desired.contains(id) {
            return true;
        }
        info!(source = %id, "source removed from the config; stopping its fetch schedule");
        cancel.cancel();
        false
    });

    for id in desired
        .iter()
        .filter(|id| !running.contains_key(id.as_str()))
    {
        warn!(
            source = %id,
            "source added to the config; a restart is needed to start fetching it"
        );
    }
}

/// Sleep until the next tick is due, returning `true` when shutdown came first.
async fn wait_for_next_tick(shutdown: &CancellationToken, wait: Duration, label: &str) -> bool {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => {
            info!(source = label, "Scheduler shutting down");
            true
        }
        () = tokio::time::sleep(wait) => false,
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

/// Classify a tick failure into a bounded category for metrics: a
/// framework error carries its TYPED HTTP status (`throttle`, `4xx`, `5xx`,
/// `timeout`, `oversize_page`), and anything else -- a transport, a
/// checkpoint write -- is not an HTTP answer, so it is a `timeout` or
/// `network` by its text.
///
/// "throttle" is split out of the generic "4xx" bucket: it is the signal a
/// producer wants to scale OUT on (spread the upstream quota over more
/// pods), not a client bug like a 401/404, and feeds the self-normalised
/// `throttle_ratio` scaling signal.
fn classify_api_error(error: &Error) -> &'static str {
    match error {
        Error::Framework(inner) => inner.api_error_code(),
        other => dfe_fetcher_core::error::non_http_error_code(&other.to_string()),
    }
}

/// The widest span one tick may fetch, floored at the fetch interval: a
/// deployment whose interval is longer than its window would otherwise
/// advance its cursor by less than one interval per tick and fall
/// permanently behind.
fn max_window_span(window_hours: u64, interval_secs: u64) -> chrono::Duration {
    let hours = i64::try_from(window_hours).unwrap_or(i64::MAX);
    let secs = i64::try_from(interval_secs).unwrap_or(i64::MAX);
    chrono::Duration::try_hours(hours)
        .unwrap_or(chrono::Duration::MAX)
        .max(chrono::Duration::try_seconds(secs).unwrap_or(chrono::Duration::MAX))
}

/// Least time between two logs of a fault that repeats every tick.
const REPEAT_LOG_EVERY_MS: u64 = 10_000;

/// What one source's fetch task has logged about its cursor, so a condition
/// that holds every tick is not logged every tick. It lives as long as the
/// task, one per source per process.
struct SourceLog<'a> {
    /// The source's label.
    name: &'a str,
    /// Set once the source's first cold start is logged at WARN.
    cold_start_warned: std::sync::atomic::AtomicBool,
    /// When a cursor read failure was last logged, for `log_debounced`.
    read_failure_logged_at: std::sync::atomic::AtomicU64,
    /// When a fetch refused for a cold start was last logged.
    refused_cold_start_logged_at: std::sync::atomic::AtomicU64,
    /// When a fetch refused for a failed cursor read was last logged.
    refused_read_failure_logged_at: std::sync::atomic::AtomicU64,
}

impl<'a> SourceLog<'a> {
    fn new(name: &'a str) -> Self {
        Self {
            name,
            cold_start_warned: std::sync::atomic::AtomicBool::new(false),
            read_failure_logged_at: std::sync::atomic::AtomicU64::new(0),
            refused_cold_start_logged_at: std::sync::atomic::AtomicU64::new(0),
            refused_read_failure_logged_at: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

/// Why a tick has no cursor to resume from.
enum NoCursor {
    /// No store, or nothing stored for the key: a new source, or a cursor lost
    /// before this process started. With no store it is every tick.
    ColdStart(&'static str),
    /// The store could not be read: a fault while the process runs.
    ReadFailed(String),
}

/// Build a `FetchWindow` from the cursor store, falling back to `now -
/// max_span` or refusing the tick, as `on_missing` says, when there is no
/// cursor to resume from.
///
/// A missing cursor is told apart by why. None stored, or no store at all, is
/// a cold start: counted in `dfe_fetcher_cursor_cold_start_total{source}` and
/// logged at WARN the first time per source, since with no store every tick is
/// one. A failed read is a fault: counted in
/// `dfe_fetcher_cursor_read_failures_total{source}` and logged at WARN every
/// time, debounced to one log per `REPEAT_LOG_EVERY_MS`.
///
/// A window running on from a cursor is at most `max_span` wide, or
/// `retry_span` wide when the last attempt failed, so a tick that fails is
/// retried over a window no wider than the one that failed. The cold-start
/// fallback ignores `retry_span`: its start slides forward with `now`, so a
/// narrow span there would pin the window in the past.
///
/// When a cursor is found, records its age (seconds since `last_fetch_end`)
/// as `dfe_fetcher_cursor_age_seconds` for staleness monitoring.
///
/// # Errors
///
/// Returns [`Error::Cursor`], logged at ERROR at most once per
/// `REPEAT_LOG_EVERY_MS` for each cause, when there is no cursor and
/// `on_missing` is [`MissingCursor::Refuse`].
async fn build_fetch_window(
    store: Option<&dyn CursorStore>,
    cursor_key: &str,
    max_span: chrono::Duration,
    retry_span: Option<chrono::Duration>,
    on_missing: MissingCursor,
    metrics: &Metrics,
    source: &SourceLog<'_>,
) -> Result<FetchWindow, Error> {
    let now = Utc::now();
    let source_prefix = source.name;

    let missing = match store {
        None => NoCursor::ColdStart("no cursor store is available"),
        Some(store) => match store.get(cursor_key).await {
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
                // The one-second floor stops a degenerate span pinning the
                // window at the cursor for good.
                let span = retry_span
                    .unwrap_or(max_span)
                    .max(chrono::Duration::seconds(1));
                // A cursor ahead of the clock -- a clock step, a cursor file
                // copied from another host -- gives an empty window rather
                // than an inverted one.
                let end = cursor
                    .last_fetch_end
                    .checked_add_signed(span)
                    .unwrap_or(chrono::DateTime::<Utc>::MAX_UTC)
                    .min(now)
                    .max(cursor.last_fetch_end);
                return Ok(FetchWindow {
                    start: cursor.last_fetch_end,
                    end,
                });
            }
            Ok(None) => NoCursor::ColdStart("no cursor is stored"),
            Err(e) => NoCursor::ReadFailed(e.to_string()),
        },
    };

    let lookback = on_missing == MissingCursor::Lookback;
    let (cause, refused_logged_at) = match missing {
        NoCursor::ColdStart(cause) => {
            metrics.inc_cursor_cold_start(source_prefix);
            // Taken only under lookback, so a reload from refuse still warns.
            let first = lookback
                && !source
                    .cold_start_warned
                    .swap(true, std::sync::atomic::Ordering::Relaxed);
            if first {
                warn!(
                    source = source_prefix,
                    cursor_key,
                    cause,
                    lookback_secs = max_span.num_seconds(),
                    "Cursor cold start: fetching the default lookback window. A new source \
                     starts this way; for an existing one, anything before the window is \
                     skipped. Later cold starts of this source log at DEBUG and count in \
                     dfe_fetcher_cursor_cold_start_total"
                );
            } else if lookback {
                debug!(
                    source = source_prefix,
                    cursor_key, cause, "Cursor cold start: fetching the default lookback window"
                );
            }
            (cause.to_owned(), &source.refused_cold_start_logged_at)
        }
        NoCursor::ReadFailed(error) => {
            metrics.inc_cursor_read_failure(source_prefix);
            if lookback
                && scalo::logger::log_debounced(&source.read_failure_logged_at, REPEAT_LOG_EVERY_MS)
            {
                warn!(
                    source = source_prefix,
                    cursor_key,
                    error = %error,
                    lookback_secs = max_span.num_seconds(),
                    "Cursor read failed: fetching the default lookback window, so anything \
                     before it is skipped. Counted in dfe_fetcher_cursor_read_failures_total"
                );
            }
            (
                format!("the cursor read failed: {error}"),
                &source.refused_read_failure_logged_at,
            )
        }
    };

    match on_missing {
        MissingCursor::Lookback => Ok(FetchWindow {
            start: now - max_span,
            end: now,
        }),
        MissingCursor::Refuse => {
            let refused = Error::Cursor(format!(
                "{cause} for {cursor_key} and cursor.on_missing_cursor is refuse: nothing is \
                 fetched until a cursor exists or the setting is lookback"
            ));
            // Refused every tick until a cursor exists, each one counted in
            // dfe_fetcher_fetches_total, so each cause is logged on its own
            // debounce: a new cause is not held back behind an old one.
            if scalo::logger::log_debounced(refused_logged_at, REPEAT_LOG_EVERY_MS) {
                error!(source = source_prefix, error = %refused, "Fetch refused");
            }
            Err(refused)
        }
    }
}

/// Write cursor after a successful fetch. Logs warning on failure but does
/// not propagate the error -- cursor failures must not block the pipeline.
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
            warn!(cursor_key, error = %e, end = %window.end, "Cursor write failed -- position not persisted");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use futures::StreamExt;
    use futures::future::BoxFuture;

    use crate::driver::{DriverParts, Shape};
    use crate::emit::Emitter;
    use crate::metrics::recorded::Recorder;
    use crate::pipeline::PipelineState;
    use dfe_fetcher_core::batch::AccumulateConfig;
    use dfe_fetcher_core::envelope::OversizePolicy;
    use dfe_fetcher_core::{RowSource, RowStream, SourceMaturity, TickCtx, UnitShape, UnitSpec};

    /// Build a test config with zero jitter for deterministic assertions.
    fn test_config_no_jitter() -> crate::config::Config {
        let mut cfg = crate::config::Config::default();
        cfg.scheduler.jitter_percent = 0;
        cfg
    }

    /// A shape that counts how often its rows are asked for and yields none,
    /// so the scheduler's cadence is observable without a transport.
    struct Counting {
        units: Vec<UnitSpec>,
        ticks: Arc<AtomicU64>,
    }

    impl RowSource for Counting {
        fn name(&self) -> &'static str {
            "counting"
        }
        fn maturity(&self) -> SourceMaturity {
            SourceMaturity::Alpha
        }
        fn units(&self) -> &[UnitSpec] {
            &self.units
        }
        fn rows<'a>(&'a self, _tick: TickCtx<'a>) -> RowStream<'a> {
            self.ticks.fetch_add(1, Ordering::Relaxed);
            futures::stream::empty().boxed()
        }
        fn probe(&self) -> BoxFuture<'_, dfe_fetcher_core::Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// A shape that records the window of every tick and fails the first
    /// `fail_ticks` of them, so the window the scheduler retries over is
    /// observable.
    struct Recording {
        units: Vec<UnitSpec>,
        seen: Arc<Mutex<Vec<FetchWindow>>>,
        ticks: Arc<AtomicU64>,
        fail_ticks: u64,
    }

    impl RowSource for Recording {
        fn name(&self) -> &'static str {
            "recording"
        }
        fn maturity(&self) -> SourceMaturity {
            SourceMaturity::Alpha
        }
        fn units(&self) -> &[UnitSpec] {
            &self.units
        }
        fn rows<'a>(&'a self, tick: TickCtx<'a>) -> RowStream<'a> {
            if let Some(window) = tick.window {
                self.seen.lock().unwrap().push(window.clone());
            }
            if self.ticks.fetch_add(1, Ordering::Relaxed) < self.fail_ticks {
                return futures::stream::once(async {
                    Err(dfe_fetcher_core::Error::Source("recording: refused".into()))
                })
                .boxed();
            }
            futures::stream::empty().boxed()
        }
        fn probe(&self) -> BoxFuture<'_, dfe_fetcher_core::Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// A driver over `source` under `connection_id`, emitting through a
    /// pipeline with no output (nothing is ever emitted).
    fn driver_over(
        shared: &SharedConfig,
        metrics: &Arc<Metrics>,
        connection_id: &str,
        source: Box<dyn RowSource>,
    ) -> Arc<Driver> {
        let state = Arc::new(
            PipelineState::new(
                shared.clone(),
                Arc::clone(metrics),
                None,
                CancellationToken::new(),
            )
            .expect("pipeline state"),
        );
        let oversize = OversizePolicy::default();
        Arc::new(Driver::new(DriverParts {
            shape: Shape::Custom(source),
            connection_id: connection_id.to_owned(),
            instance_id: "inst".into(),
            shared_config: shared.clone(),
            accumulate: AccumulateConfig::default(),
            oversize,
            emitter: Emitter::new(Arc::clone(&state), Arc::clone(metrics), 4),
            pressure: None,
            memory_guard: Arc::clone(state.memory_guard()),
            checkpoints: None,
            metrics: Arc::clone(metrics),
            shutdown: CancellationToken::new(),
        }))
    }

    /// A driver over [`Counting`] under `connection_id`.
    fn counting_driver(
        shared: &SharedConfig,
        metrics: &Arc<Metrics>,
        connection_id: &str,
        ticks: Arc<AtomicU64>,
    ) -> Arc<Driver> {
        driver_over(
            shared,
            metrics,
            connection_id,
            Box::new(Counting {
                units: vec![UnitSpec::new("unit", UnitShape::Incremental, "t")],
                ticks,
            }),
        )
    }

    /// A driver over [`Recording`] under `connection_id`, failing its first
    /// `fail_ticks` ticks.
    fn recording_driver(
        shared: &SharedConfig,
        metrics: &Arc<Metrics>,
        connection_id: &str,
        seen: &Arc<Mutex<Vec<FetchWindow>>>,
        fail_ticks: u64,
    ) -> Arc<Driver> {
        driver_over(
            shared,
            metrics,
            connection_id,
            Box::new(Recording {
                units: vec![UnitSpec::new("unit", UnitShape::Incremental, "t")],
                seen: Arc::clone(seen),
                ticks: Arc::new(AtomicU64::new(0)),
                fail_ticks,
            }),
        )
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

    /// The window a tick builds under the default `lookback` cold start.
    async fn lookback_window(
        store: Option<&dyn CursorStore>,
        cursor_key: &str,
        max_span: chrono::Duration,
        retry_span: Option<chrono::Duration>,
        metrics: &Metrics,
        source_prefix: &str,
    ) -> FetchWindow {
        build_fetch_window(
            store,
            cursor_key,
            max_span,
            retry_span,
            MissingCursor::Lookback,
            metrics,
            &SourceLog::new(source_prefix),
        )
        .await
        .expect("lookback never refuses a tick")
    }

    /// One `lookback` tick of the source `log` belongs to, over a one-hour
    /// window.
    async fn logged_tick(
        store: Option<&dyn CursorStore>,
        metrics: &Metrics,
        log: &SourceLog<'_>,
    ) -> FetchWindow {
        build_fetch_window(
            store,
            &format!("inst.{}", log.name),
            chrono::Duration::hours(1),
            None,
            MissingCursor::Lookback,
            metrics,
            log,
        )
        .await
        .expect("lookback never refuses a tick")
    }

    /// The cold starts `recorder` counted for `source`, or `None` when it has
    /// none.
    fn cold_starts(recorder: &Recorder, source: &str) -> Option<u64> {
        recorder.counter("dfe_fetcher_cursor_cold_start_total", &[("source", source)])
    }

    /// A source with no cursor is a cold start every time: it counts, and the
    /// default still fetches the lookback window so a new source starts.
    #[tokio::test]
    async fn a_missing_cursor_counts_a_cold_start_and_looks_back() {
        let recorder = Recorder::new();
        let _recording = recorder.install();
        let dir = tempfile::TempDir::new().unwrap();
        let store =
            crate::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();
        let metrics = Metrics::new();

        let window = lookback_window(
            Some(&store),
            "inst.fresh",
            chrono::Duration::hours(2),
            None,
            &metrics,
            "fresh",
        )
        .await;
        assert!(
            (window.start - (Utc::now() - chrono::Duration::hours(2)))
                .num_seconds()
                .abs()
                < 2,
            "the default fetches the lookback window"
        );
        assert_eq!(cold_starts(&recorder, "fresh"), Some(1));

        lookback_window(
            Some(&store),
            "inst.fresh",
            chrono::Duration::hours(2),
            None,
            &metrics,
            "fresh",
        )
        .await;
        assert_eq!(metrics.cursor_cold_starts(), 2, "every miss counts");
    }

    /// `refuse` fetches nothing without a cursor, so a lost cursor cannot skip
    /// data, and the miss is still counted.
    #[tokio::test]
    async fn refuse_fails_a_tick_with_no_cursor() {
        let recorder = Recorder::new();
        let _recording = recorder.install();
        let dir = tempfile::TempDir::new().unwrap();
        let store =
            crate::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();
        let metrics = Metrics::new();

        let err = build_fetch_window(
            Some(&store),
            "inst.lost",
            chrono::Duration::hours(1),
            None,
            MissingCursor::Refuse,
            &metrics,
            &SourceLog::new("lost"),
        )
        .await
        .expect_err("refuse fetches nothing without a cursor");

        assert!(matches!(err, Error::Cursor(_)), "{err:?}");
        assert!(err.to_string().contains("on_missing_cursor"), "{err}");
        assert_eq!(cold_starts(&recorder, "lost"), Some(1));
    }

    /// No store at all is a cold start too, refused under `refuse`.
    #[tokio::test]
    async fn refuse_fails_a_tick_with_no_cursor_store() {
        let recorder = Recorder::new();
        let _recording = recorder.install();
        let metrics = Metrics::new();

        let err = build_fetch_window(
            None,
            "inst.storeless",
            chrono::Duration::hours(1),
            None,
            MissingCursor::Refuse,
            &metrics,
            &SourceLog::new("storeless"),
        )
        .await
        .expect_err("refuse fetches nothing without a store");

        assert!(err.to_string().contains("no cursor store"), "{err}");
        assert_eq!(cold_starts(&recorder, "storeless"), Some(1));
    }

    /// Counts the events at `level` the scheduler logs.
    fn scheduler_events(level: tracing::Level) -> crate::logged::Events {
        crate::logged::Events::at(level, "dfe_fetcher::scheduler")
    }

    /// With no cursor store every tick is a cold start: each one counts, and
    /// the WARN is logged once per source, not once a tick.
    #[tokio::test]
    async fn a_storeless_source_warns_of_its_cold_start_once() {
        let warnings = scheduler_events(tracing::Level::WARN);
        let _guard = tracing::subscriber::set_default(warnings.clone());
        let metrics = Metrics::new();
        let nostore = SourceLog::new("nostore");

        for _ in 0..3 {
            logged_tick(None, &metrics, &nostore).await;
        }
        assert_eq!(warnings.count(), 1, "one WARN a source");
        assert_eq!(metrics.cursor_cold_starts(), 3, "every tick still counts");

        logged_tick(None, &metrics, &SourceLog::new("nostore-other")).await;
        assert_eq!(warnings.count(), 2, "another source warns of its own");
    }

    /// A store that holds no cursor, and whose reads fail once `failing` is set.
    #[derive(Default)]
    struct FlakyReads {
        failing: AtomicBool,
    }

    #[async_trait::async_trait]
    impl CursorStore for FlakyReads {
        async fn get(&self, _: &str) -> dfe_fetcher_core::error::Result<Option<CursorValue>> {
            if self.failing.load(Ordering::Relaxed) {
                Err(dfe_fetcher_core::Error::Cursor(
                    "the store is unreachable".into(),
                ))
            } else {
                Ok(None)
            }
        }
        async fn set(&self, _: &str, _: &CursorValue) -> dfe_fetcher_core::error::Result<()> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> dfe_fetcher_core::error::Result<()> {
            Ok(())
        }
    }

    /// A source that cold-started still warns when a later read of its cursor
    /// fails: a store fault is not the startup message repeating. It counts in
    /// its own series, not as a cold start, and a failure that repeats at once
    /// is debounced.
    #[tokio::test]
    async fn a_read_failure_after_a_cold_start_still_warns() {
        let warnings = scheduler_events(tracing::Level::WARN);
        let _guard = tracing::subscriber::set_default(warnings.clone());
        let metrics = Metrics::new();
        let store = FlakyReads::default();
        let flaky = SourceLog::new("flaky");

        logged_tick(Some(&store), &metrics, &flaky).await;
        logged_tick(Some(&store), &metrics, &flaky).await;
        assert_eq!(warnings.count(), 1, "one cold-start WARN");
        assert_eq!(metrics.cursor_cold_starts(), 2);

        store.failing.store(true, Ordering::Relaxed);
        logged_tick(Some(&store), &metrics, &flaky).await;
        assert_eq!(
            warnings.count(),
            2,
            "the read failure warns after the cold start"
        );
        logged_tick(Some(&store), &metrics, &flaky).await;
        assert_eq!(
            warnings.count(),
            2,
            "a repeat within the window is debounced"
        );
        assert_eq!(
            metrics.cursor_read_failures(),
            2,
            "every failed read counts"
        );
        assert_eq!(metrics.cursor_cold_starts(), 2, "and none as a cold start");
    }

    /// Under refuse each cause of a refused tick is logged on its own
    /// debounce: a repeat of one cause is held back, a new cause is not.
    #[tokio::test]
    async fn a_refused_tick_is_logged_once_per_cause_in_its_window() {
        let errors = scheduler_events(tracing::Level::ERROR);
        let _guard = tracing::subscriber::set_default(errors.clone());
        let metrics = Metrics::new();
        let store = FlakyReads::default();
        let refused = SourceLog::new("refused-causes");
        let tick = || {
            build_fetch_window(
                Some(&store),
                "inst.refused-causes",
                chrono::Duration::hours(1),
                None,
                MissingCursor::Refuse,
                &metrics,
                &refused,
            )
        };

        tick().await.expect_err("refused: none stored");
        tick().await.expect_err("refused: none stored");
        assert_eq!(errors.count(), 1, "the repeat is held back");

        store.failing.store(true, Ordering::Relaxed);
        let err = tick().await.expect_err("refused: the read failed");
        assert!(err.to_string().contains("the cursor read failed"), "{err}");
        assert_eq!(errors.count(), 2, "a new cause logs at once");
    }

    /// A stored cursor is resumed under either setting and is no cold start.
    #[tokio::test]
    async fn a_stored_cursor_is_resumed_even_under_refuse() {
        use crate::cursor::file::FileCursorStore;

        let recorder = Recorder::new();
        let _recording = recorder.install();
        let dir = tempfile::TempDir::new().unwrap();
        let store = FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();
        let last_end = Utc::now() - chrono::Duration::minutes(10);
        seed_cursor(&store, "inst.kept", last_end).await;
        let metrics = Metrics::new();

        let window = build_fetch_window(
            Some(&store),
            "inst.kept",
            chrono::Duration::hours(1),
            None,
            MissingCursor::Refuse,
            &metrics,
            &SourceLog::new("kept"),
        )
        .await
        .expect("a stored cursor is resumed");

        assert_eq!(window.start, last_end);
        assert_eq!(cold_starts(&recorder, "kept"), None);
    }

    #[tokio::test]
    async fn test_build_fetch_window_no_store() {
        let metrics = Metrics::new();
        let window = lookback_window(
            None,
            "test.key",
            chrono::Duration::hours(2),
            None,
            &metrics,
            "test",
        )
        .await;
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
        let window = lookback_window(
            Some(&store),
            "test.key",
            chrono::Duration::hours(2),
            None,
            &metrics,
            "test",
        )
        .await;

        // Window should start from cursor, not default lookback
        assert!(
            (window.start - last_end).num_seconds().abs() < 2,
            "window.start should match cursor.last_fetch_end"
        );
        assert!(window.end > window.start);
    }

    /// Seed a cursor store with a position, so a window can be built from a
    /// known `last_fetch_end`.
    async fn seed_cursor(
        store: &crate::cursor::file::FileCursorStore,
        key: &str,
        last_end: chrono::DateTime<Utc>,
    ) {
        let cursor = CursorValue {
            cursor_key: key.to_string(),
            last_fetch_end: last_end,
            last_fetch_records: 0,
            updated_at: Utc::now(),
            api_cursor: None,
            version: 1,
        };
        store.set(key, &cursor).await.unwrap();
    }

    /// The retry of a failed tick is no wider than the window that failed:
    /// the page ceiling makes a wider window a certain failure, so widening
    /// one that already failed is a permanent stall.
    #[tokio::test]
    async fn a_failed_window_is_retried_no_wider_than_it_failed() {
        use crate::cursor::file::FileCursorStore;

        let dir = tempfile::TempDir::new().unwrap();
        let store = FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();
        let last_end = Utc::now() - chrono::Duration::minutes(30);
        seed_cursor(&store, "test.key", last_end).await;

        let metrics = Metrics::new();
        let window = lookback_window(
            Some(&store),
            "test.key",
            chrono::Duration::hours(1),
            Some(chrono::Duration::minutes(5)),
            &metrics,
            "test",
        )
        .await;

        assert_eq!(window.start, last_end, "the retry starts at the cursor");
        assert_eq!(
            window.end,
            last_end + chrono::Duration::minutes(5),
            "the retry ends where the failed attempt ended, not at now"
        );
    }

    /// A window is capped at the configured span, so a source that has been
    /// down for hours asks for one span at a time.
    #[tokio::test]
    async fn a_window_never_exceeds_the_configured_span() {
        use crate::cursor::file::FileCursorStore;

        let dir = tempfile::TempDir::new().unwrap();
        let store = FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();
        let last_end = Utc::now() - chrono::Duration::hours(6);
        seed_cursor(&store, "test.key", last_end).await;

        let metrics = Metrics::new();
        let window = lookback_window(
            Some(&store),
            "test.key",
            chrono::Duration::hours(1),
            None,
            &metrics,
            "test",
        )
        .await;

        assert_eq!(
            window.end,
            last_end + chrono::Duration::hours(1),
            "six hours behind, the window still spans one hour"
        );
        assert!(
            window.end < Utc::now() - chrono::Duration::hours(4),
            "the window ends well short of now"
        );
    }

    /// A degenerate span cannot pin a source at its cursor.
    #[tokio::test]
    async fn a_zero_width_failed_window_does_not_freeze_the_source() {
        use crate::cursor::file::FileCursorStore;

        let dir = tempfile::TempDir::new().unwrap();
        let store = FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();
        let last_end = Utc::now() - chrono::Duration::minutes(30);
        seed_cursor(&store, "test.key", last_end).await;

        let metrics = Metrics::new();
        let window = lookback_window(
            Some(&store),
            "test.key",
            chrono::Duration::hours(1),
            Some(chrono::Duration::zero()),
            &metrics,
            "test",
        )
        .await;

        assert_eq!(
            window.end,
            window.start + chrono::Duration::seconds(1),
            "a zero span is floored at one second, so the source keeps moving"
        );
    }

    /// A cursor ahead of the clock -- a clock step, a cursor file copied from
    /// another host -- gives an empty window, never an inverted one.
    #[tokio::test]
    async fn a_future_cursor_yields_an_empty_window_not_an_inverted_one() {
        use crate::cursor::file::FileCursorStore;

        let dir = tempfile::TempDir::new().unwrap();
        let store = FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();
        let last_end = Utc::now() + chrono::Duration::hours(1);
        seed_cursor(&store, "test.key", last_end).await;

        let metrics = Metrics::new();
        let window = lookback_window(
            Some(&store),
            "test.key",
            chrono::Duration::hours(1),
            None,
            &metrics,
            "test",
        )
        .await;

        assert_eq!(window.start, last_end);
        assert_eq!(
            window.end, last_end,
            "the window stays empty until the clock catches up"
        );
    }

    /// Test that the scheduler stalls when is_ready returns false,
    /// and resumes when it returns true. Uses tokio::time::pause()
    /// to control time without real delays.
    #[tokio::test]
    async fn test_scheduler_stalls_on_backpressure() {
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

        let scheduler = Scheduler::new(&scheduler_config, shared.clone(), None, "test".into());

        let metrics = Arc::new(Metrics::new());
        let ticks = Arc::new(AtomicU64::new(0));
        let driver = counting_driver(&shared, &metrics, "counting", Arc::clone(&ticks));
        let shutdown = CancellationToken::new();

        // Start with is_ready = false (backpressured)
        let ready = Arc::new(AtomicBool::new(false));
        let ready_clone = ready.clone();
        let is_ready = Arc::new(move || ready_clone.load(Ordering::Relaxed));

        scheduler.spawn_source_task(driver, Some(1), metrics.clone(), shutdown.clone(), is_ready);

        // Advance time past the initial sleep + several backpressure poll intervals
        // (step in 1s increments to let spawned task run)
        for _ in 0..20 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        // No fetches should have happened -- pipeline was not ready
        let fetches_while_stalled = ticks.load(Ordering::Relaxed);
        assert_eq!(
            fetches_while_stalled, 0,
            "scheduler should NOT fetch while backpressured"
        );

        // Mark pipeline as ready
        ready.store(true, Ordering::Relaxed);

        // Advance time for the stall poll to detect readiness + one fetch cycle
        for _ in 0..15 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let fetches_after_ready = ticks.load(Ordering::Relaxed);
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

        let metrics = Arc::new(Metrics::new());
        let ticks = Arc::new(AtomicU64::new(0));
        let driver = counting_driver(&shared, &metrics, "counting", Arc::clone(&ticks));
        let shutdown = CancellationToken::new();
        let is_ready = Arc::new(|| true);

        scheduler.spawn_source_task(
            driver,
            None, // use config default
            metrics,
            shutdown.clone(),
            is_ready,
        );

        // Advance past initial sleep (2s) in small steps to let spawned task run
        for _ in 0..10 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let count_before = ticks.load(Ordering::Relaxed);
        assert!(
            count_before >= 1,
            "should have at least 1 fetch after 10s with 2s interval, got {count_before}"
        );

        // Hot-reload: change interval to 100s (effectively stop fetching)
        let mut new_cfg = crate::config::Config::default();
        new_cfg.scheduler.default_interval_secs = 100;
        new_cfg.scheduler.jitter_percent = 0;
        shared.update(new_cfg);

        // Advance 10s in steps -- should NOT trigger many more fetches (interval is now 100s)
        let count_snapshot = ticks.load(Ordering::Relaxed);
        for _ in 0..10 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let count_after = ticks.load(Ordering::Relaxed);
        // At most 1 more fetch could sneak in from the previous cycle
        assert!(
            count_after <= count_snapshot + 1,
            "hot-reload should slow fetches: before={count_snapshot}, after={count_after}"
        );

        shutdown.cancel();
    }

    /// A reload that removes the source must stop its fetch schedule: the
    /// watcher cancels the source's child token, the task returns and the
    /// driver it held is freed.
    #[tokio::test]
    async fn a_source_dropped_by_a_reload_stops_ticking() {
        tokio::time::pause();

        // `scheduled_connection_ids` reports "counting" only while the
        // instance is in the config; the post-reload config keeps the same
        // cadence so the tick count can only change by the cancel.
        let interval_secs = 2;
        let with_source = || {
            let mut cfg = test_config_no_jitter();
            cfg.scheduler.default_interval_secs = interval_secs;
            cfg
        };
        let mut cfg = with_source();
        cfg.sources
            .rest
            .insert("counting".into(), dfe_fetcher_rest::RestInstance::default());
        let shared = SharedConfig::new(cfg);

        let scheduler_config = SchedulerConfig {
            default_interval_secs: interval_secs,
            max_concurrent_fetches: 10,
            jitter_percent: 0,
        };
        let scheduler = Scheduler::new(&scheduler_config, shared.clone(), None, "test".into());

        let metrics = Arc::new(Metrics::new());
        let ticks = Arc::new(AtomicU64::new(0));
        let driver = counting_driver(&shared, &metrics, "counting", Arc::clone(&ticks));
        assert_eq!(driver.connection_id(), "counting");
        let driver_alive = Arc::downgrade(&driver);

        let shutdown = CancellationToken::new();
        let cancel = shutdown.child_token();
        scheduler.spawn_source_task(
            driver,
            None,
            Arc::clone(&metrics),
            cancel.clone(),
            Arc::new(|| true),
        );
        scheduler.spawn_source_watch(
            HashMap::from([("counting".to_string(), cancel.clone())]),
            shutdown.clone(),
        );

        for _ in 0..10 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        let ticks_while_live = ticks.load(Ordering::Relaxed);
        assert!(
            ticks_while_live >= 1,
            "the source should tick while it is configured, got {ticks_while_live}"
        );

        // Drop the source: the unit-level equivalent of a rewritten
        // fetcher.yaml that no longer lists it.
        shared.update(with_source());

        // No time passes here, so nothing but the cancel can end the cadence.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            cancel.is_cancelled(),
            "the watcher should cancel the removed source"
        );

        for _ in 0..20 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            ticks.load(Ordering::Relaxed),
            ticks_while_live,
            "a removed source must not tick again"
        );
        assert!(
            driver_alive.upgrade().is_none(),
            "the cancelled task should have dropped its driver"
        );
        assert!(
            !shutdown.is_cancelled(),
            "cancelling one source must not shut the process down"
        );
    }

    /// Spawn a paced source over a cursor store seeded at `last_end`, whose
    /// shape records each window and fails its first `fail_ticks` ticks.
    /// Returns the windows the shape saw and the store, after `intervals`
    /// one-second ticks.
    async fn run_recorded_ticks(
        last_end: chrono::DateTime<Utc>,
        fail_ticks: u64,
        intervals: usize,
    ) -> (Vec<FetchWindow>, Arc<crate::cursor::file::FileCursorStore>) {
        let mut cfg = test_config_no_jitter();
        cfg.scheduler.default_interval_secs = 1;
        cfg.cursor.default_window_hours = 1;
        let shared = SharedConfig::new(cfg);

        let dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(
            crate::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
        );
        seed_cursor(&store, "test.recording", last_end).await;

        let scheduler_config = SchedulerConfig {
            default_interval_secs: 1,
            max_concurrent_fetches: 10,
            jitter_percent: 0,
        };
        let scheduler = Scheduler::new(
            &scheduler_config,
            shared.clone(),
            Some(Arc::clone(&store) as Arc<dyn CursorStore>),
            "test".into(),
        );

        let metrics = Arc::new(Metrics::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let driver = recording_driver(&shared, &metrics, "recording", &seen, fail_ticks);
        let shutdown = CancellationToken::new();
        scheduler.spawn_source_task(
            driver,
            Some(1),
            Arc::clone(&metrics),
            shutdown.clone(),
            Arc::new(|| true),
        );

        for _ in 0..intervals {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        shutdown.cancel();
        tokio::task::yield_now().await;

        let windows = seen.lock().unwrap().clone();
        (windows, store)
    }

    /// A tick that fails is retried over the window that failed. The cursor
    /// does not move on failure, so the end has to be pinned or the retry is
    /// wider than the attempt that already failed.
    #[tokio::test]
    async fn a_failed_tick_is_retried_over_the_same_window() {
        tokio::time::pause();

        let last_end = Utc::now() - chrono::Duration::hours(6);
        let (seen, _store) = run_recorded_ticks(last_end, u64::MAX, 10).await;

        assert!(
            seen.len() >= 2,
            "the source should have ticked at least twice, got {}",
            seen.len()
        );
        assert_eq!(seen[1], seen[0], "the retry repeats the window that failed");
        assert_eq!(
            seen[0].end - seen[0].start,
            chrono::Duration::hours(1),
            "six hours behind, the first window still spans one hour"
        );
    }

    /// A source catching up after a long outage drains in bounded steps: no
    /// window is wider than the configured span, each one either advances to
    /// the last end or retries the same start, and the cursor walks forward a
    /// span at a time instead of jumping to now.
    #[tokio::test]
    async fn a_long_outage_drains_in_bounded_steps() {
        tokio::time::pause();

        let span = chrono::Duration::hours(1);
        let last_end = Utc::now() - chrono::Duration::hours(30);
        let (seen, store) = run_recorded_ticks(last_end, 1, 20).await;

        assert!(
            seen.len() >= 3,
            "the source should have ticked at least three times, got {}",
            seen.len()
        );
        for pair in seen.windows(2) {
            let (prev, next) = (&pair[0], &pair[1]);
            assert!(
                next.start == prev.end || next.start == prev.start,
                "a window advances to the last end or retries the same start: {prev:?} then {next:?}"
            );
            assert!(
                next.end - next.start <= span,
                "no window is wider than the configured span: {next:?}"
            );
        }

        let cursor = store
            .get("test.recording")
            .await
            .unwrap()
            .expect("the successful ticks wrote a cursor");
        let advanced = cursor.last_fetch_end - last_end;
        assert_eq!(
            advanced.num_seconds() % span.num_seconds(),
            0,
            "the cursor advances a whole span at a time, got {advanced}"
        );
        assert!(
            advanced >= span * 2,
            "several spans should have drained, got {advanced}"
        );
        assert!(
            cursor.last_fetch_end < Utc::now() - chrono::Duration::hours(10),
            "the cursor walked forward rather than jumping to now"
        );
    }

    // -- write_cursor tests --

    #[tokio::test]
    async fn test_write_cursor_no_store_is_noop() {
        let metrics = Metrics::new();
        let window = FetchWindow {
            start: Utc::now() - chrono::Duration::hours(1),
            end: Utc::now(),
        };
        // Must not panic, must not touch metrics
        write_cursor(None, "some.key", &window, 42, &metrics).await;

        // Verify no cursor write counters were incremented
        let rendered = metrics.render();
        assert!(
            rendered.contains("dfe_fetcher_cursor_writes_total 0"),
            "write with no store must not increment success counter: \n{rendered}"
        );
        assert!(
            rendered.contains("dfe_fetcher_cursor_write_failures_total 0"),
            "write with no store must not increment failure counter: \n{rendered}"
        );
    }

    #[tokio::test]
    async fn test_write_cursor_with_store_increments_metric() {
        use crate::cursor::file::FileCursorStore;

        let dir = tempfile::TempDir::new().unwrap();
        let store = FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();
        let metrics = Metrics::new();
        let window = FetchWindow {
            start: Utc::now() - chrono::Duration::hours(1),
            end: Utc::now(),
        };
        write_cursor(Some(&store), "source.test", &window, 7, &metrics).await;

        let rendered = metrics.render();
        assert!(
            rendered.contains("dfe_fetcher_cursor_writes_total 1"),
            "successful write must increment success counter: \n{rendered}"
        );

        // And the cursor is readable back with the written values
        let stored = store
            .get("source.test")
            .await
            .unwrap()
            .expect("cursor persisted");
        assert_eq!(stored.last_fetch_records, 7);
    }

    /// The cursor moves the window on every tick: the next window starts
    /// where the written one ended, so a source that needs several ticks to
    /// drain a busy window never re-reads what it has already landed.
    #[tokio::test]
    async fn test_the_next_window_starts_where_the_written_cursor_ended() {
        use crate::cursor::file::FileCursorStore;

        let dir = tempfile::TempDir::new().unwrap();
        let store = FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();
        let metrics = Metrics::new();

        let span = chrono::Duration::hours(1);
        let first =
            lookback_window(Some(&store), "source.paced", span, None, &metrics, "test").await;
        write_cursor(Some(&store), "source.paced", &first, 10_000, &metrics).await;
        let second =
            lookback_window(Some(&store), "source.paced", span, None, &metrics, "test").await;

        assert_eq!(
            second.start, first.end,
            "the second window starts at the first window's end"
        );
        assert!(second.end > second.start);
        assert!(
            second.start > first.start,
            "the window moved rather than being re-read from the lookback"
        );
    }

    // -- build_fetch_window with corrupt cursor --

    #[tokio::test]
    async fn test_build_fetch_window_with_corrupt_cursor_falls_back() {
        use crate::cursor::file::FileCursorStore;

        let dir = tempfile::TempDir::new().unwrap();

        // Write an invalid cursor file before creating the store. FileCursorStore
        // skips malformed files on load, so the cache will be empty and the
        // scheduler falls back to the default lookback window.
        std::fs::write(
            dir.path().join("corrupt.key.cursor.json"),
            b"THIS IS NOT VALID JSON {{{",
        )
        .unwrap();

        let store = FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();
        let metrics = Metrics::new();

        let window = lookback_window(
            Some(&store),
            "corrupt.key",
            chrono::Duration::hours(3),
            None,
            &metrics,
            "test",
        )
        .await;
        let expected_start = Utc::now() - chrono::Duration::hours(3);
        assert!(
            (window.start - expected_start).num_seconds().abs() < 2,
            "corrupt cursor should fall back to default lookback (3h)"
        );
        assert!(
            window.end > window.start,
            "window end must be after window start"
        );
    }

    // -- effective_interval with source override --

    #[test]
    fn test_effective_interval_source_override_60_seconds() {
        let config = SchedulerConfig {
            default_interval_secs: 300,
            max_concurrent_fetches: 10,
            jitter_percent: 0,
        };
        let shared = SharedConfig::new(test_config_no_jitter());
        let scheduler = Scheduler::new(&config, shared, None, "test".into());

        let interval = scheduler.effective_interval(Some(60));
        assert_eq!(
            interval.as_secs(),
            60,
            "source override should take precedence over default"
        );
    }

    // -- calculate_jitter edge cases --

    #[test]
    fn test_calculate_jitter_100_percent_bounded() {
        // 100% jitter on base=100 => max_jitter=100, fastrand in [0, 100)
        let j = calculate_jitter(100, 100);
        assert!(j < 100, "100% jitter on 100 should be < 100, got {j}");
    }

    #[test]
    fn test_calculate_jitter_rounds_down_to_zero() {
        // base=10, jitter_percent=1 => 10 * 1 / 100 = 0 (integer division)
        // max_jitter==0 path returns 0
        let j = calculate_jitter(10, 1);
        assert_eq!(
            j, 0,
            "tiny jitter that rounds to 0 must return 0, not panic"
        );
    }

    #[test]
    fn test_calculate_jitter_large_values_no_overflow() {
        // Large base with 50% jitter must compute without panicking on overflow.
        // u64::MAX / 200 * 50 / 100 fits easily in u64.
        let base = u64::MAX / 200;
        let j = calculate_jitter(base, 50);
        let expected_max = base * 50 / 100;
        assert!(
            j < expected_max,
            "jitter {j} must be < expected max {expected_max}"
        );
    }

    #[test]
    fn test_calculate_jitter_repeated_within_bound() {
        // Call many times to get coverage of the fastrand path, all must be in range.
        for _ in 0..50 {
            let j = calculate_jitter(1000, 25);
            assert!(j < 250, "25% of 1000 must yield < 250, got {j}");
        }
    }

    // -- Scheduler concurrency configuration --

    #[test]
    fn test_scheduler_with_max_concurrent_one() {
        // Semaphore is constructed internally -- verify the scheduler still
        // reports a sensible effective interval when configured for
        // single-flight concurrency.
        let config = SchedulerConfig {
            default_interval_secs: 120,
            max_concurrent_fetches: 1,
            jitter_percent: 0,
        };
        let shared = SharedConfig::new(test_config_no_jitter());
        let scheduler = Scheduler::new(&config, shared, None, "singleton".into());

        let interval = scheduler.effective_interval(None);
        assert_eq!(
            interval.as_secs(),
            300,
            "effective_interval reads from shared config default (300s), not SchedulerConfig"
        );

        // A source-level override is still honoured
        let overridden = scheduler.effective_interval(Some(45));
        assert_eq!(overridden.as_secs(), 45);
    }

    /// Under `refuse`, a source with no cursor is never fetched, each tick is
    /// counted a failure, and the schedule keeps running for a cursor to
    /// appear.
    #[tokio::test]
    async fn refuse_never_fetches_a_source_with_no_cursor() {
        use crate::cursor::file::FileCursorStore;

        let errors = scheduler_events(tracing::Level::ERROR);
        let _guard = tracing::subscriber::set_default(errors.clone());
        tokio::time::pause();

        let dir = tempfile::TempDir::new().unwrap();
        let store: Arc<dyn CursorStore> =
            Arc::new(FileCursorStore::new(dir.path().to_str().unwrap()).unwrap());
        let mut cfg = test_config_no_jitter();
        cfg.scheduler.default_interval_secs = 1;
        cfg.cursor.on_missing_cursor = MissingCursor::Refuse;
        let shared = SharedConfig::new(cfg);
        let scheduler = Scheduler::new(
            &SchedulerConfig {
                default_interval_secs: 1,
                max_concurrent_fetches: 10,
                jitter_percent: 0,
            },
            shared.clone(),
            Some(store),
            "inst".into(),
        );

        let metrics = Arc::new(Metrics::new());
        let ticks = Arc::new(AtomicU64::new(0));
        let driver = counting_driver(&shared, &metrics, "refused", Arc::clone(&ticks));
        let shutdown = CancellationToken::new();
        scheduler.spawn_source_task(
            driver,
            Some(1),
            Arc::clone(&metrics),
            shutdown.clone(),
            Arc::new(|| true),
        );

        for _ in 0..10 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        shutdown.cancel();

        assert_eq!(ticks.load(Ordering::Relaxed), 0, "nothing is fetched");
        assert!(
            metrics.cursor_cold_starts() >= 2,
            "every refused tick is a counted cold start, got {}",
            metrics.cursor_cold_starts()
        );
        assert_eq!(
            errors.count(),
            1,
            "the refusal is logged once in its window, not once a tick"
        );
        let rendered = metrics.render();
        assert!(
            !rendered.contains("dfe_fetcher_fetches_total{status=\"error\"} 0\n"),
            "a refused tick counts as a failed fetch:\n{rendered}"
        );
    }

    /// Two drivers over the SAME shape but DIFFERENT connection ids must
    /// checkpoint under DIFFERENT cursor keys (cursor key = connection id),
    /// so accounts of one type do not collide.
    #[tokio::test]
    async fn test_per_connection_cursor_keys() {
        use crate::cursor::file::FileCursorStore;

        tokio::time::pause();

        let dir = tempfile::TempDir::new().unwrap();
        let store: Arc<dyn CursorStore> =
            Arc::new(FileCursorStore::new(dir.path().to_str().unwrap()).unwrap());

        let mut cfg = crate::config::Config::default();
        cfg.scheduler.default_interval_secs = 1;
        cfg.scheduler.jitter_percent = 0;
        let shared = SharedConfig::new(cfg);
        let sched_cfg = SchedulerConfig {
            default_interval_secs: 1,
            max_concurrent_fetches: 10,
            jitter_percent: 0,
        };
        let scheduler = Scheduler::new(
            &sched_cfg,
            shared.clone(),
            Some(store.clone()),
            "inst".into(),
        );

        let metrics = Arc::new(Metrics::new());
        let shutdown = CancellationToken::new();
        let is_ready = Arc::new(|| true);

        for conn in ["acct-a", "acct-b"] {
            let driver = counting_driver(&shared, &metrics, conn, Arc::default());
            scheduler.spawn_source_task(
                driver,
                Some(1),
                metrics.clone(),
                shutdown.clone(),
                is_ready.clone(),
            );
        }

        // Advance past the initial sleep, one fetch, and the cursor write.
        for _ in 0..10 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        // Each connection has its OWN cursor key ({instance}.{connection_id}),
        // NOT the shared shape name ("inst.counting").
        assert!(
            store.get("inst.acct-a").await.unwrap().is_some(),
            "connection acct-a must checkpoint under its own cursor key"
        );
        assert!(
            store.get("inst.acct-b").await.unwrap().is_some(),
            "connection acct-b must checkpoint under its own cursor key"
        );
        assert!(
            store.get("inst.counting").await.unwrap().is_none(),
            "cursor must NOT be keyed on the shape name"
        );

        shutdown.cancel();
    }

    // --- classify_api_error tests ---

    #[test]
    fn test_classify_timed_out() {
        let err = Error::Source("request timed out".to_string());
        assert_eq!(classify_api_error(&err), "timeout");
    }

    #[test]
    fn test_classify_timeout_keyword() {
        let err = Error::Source("connection timeout reached".to_string());
        assert_eq!(classify_api_error(&err), "timeout");
    }

    #[test]
    fn test_classify_connection_refused() {
        let err = Error::Source("connection refused".to_string());
        assert_eq!(classify_api_error(&err), "network");
    }

    #[test]
    fn test_classify_dns_failure() {
        let err = Error::Source("DNS resolution failed".to_string());
        assert_eq!(classify_api_error(&err), "network");
    }

    #[test]
    fn test_classify_empty_message() {
        let err = Error::Source(String::new());
        assert_eq!(classify_api_error(&err), "network");
    }

    fn api(status: u16, text: &str) -> Error {
        Error::Framework(dfe_fetcher_core::Error::Api {
            status,
            text: text.to_owned(),
            throttled: false,
        })
    }

    /// An HTTP answer classifies from its typed status alone: a 429 is a
    /// throttle whatever the body says, a 4xx a client error, a 5xx an
    /// upstream one, S3's SlowDown a throttle, and a 408 a timeout.
    #[test]
    fn test_classify_http_answers_by_typed_status() {
        assert_eq!(classify_api_error(&api(429, "")), "throttle");
        assert_eq!(classify_api_error(&api(429, "500 mentioned")), "throttle");
        for status in [400, 401, 403, 404, 422] {
            assert_eq!(classify_api_error(&api(status, "429")), "4xx", "{status}");
        }
        for status in [500, 502, 503, 504] {
            assert_eq!(
                classify_api_error(&api(status, "throttled")),
                "5xx",
                "{status}"
            );
        }
        assert_eq!(
            classify_api_error(&api(503, "<Code>SlowDown</Code>")),
            "throttle"
        );
        assert_eq!(classify_api_error(&api(408, "")), "timeout");
    }

    /// An error that is not an HTTP answer never reads as one: a transport
    /// or checkpoint failure whose text carries a status number is still a
    /// network failure, and a framework page overflow keeps its own code.
    #[test]
    fn test_classify_non_http_errors_never_read_a_status_off_the_text() {
        assert_eq!(
            classify_api_error(&Error::Transport("broker said 429".into())),
            "network"
        );
        assert_eq!(
            classify_api_error(&Error::Cursor("status: 503 writing".into())),
            "network"
        );
        assert_eq!(
            classify_api_error(&Error::Backpressured("kafka timed out".into())),
            "timeout"
        );
        assert_eq!(
            classify_api_error(&Error::Framework(dfe_fetcher_core::Error::Source(
                "AWS SlowDown: reduce request rate".into()
            ))),
            "network"
        );
        assert_eq!(
            classify_api_error(&Error::Framework(dfe_fetcher_core::Error::OversizePage {
                max: 1,
            })),
            "oversize_page"
        );
    }
}
