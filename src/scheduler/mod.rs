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
//! - Configurable intervals per source
//! - Jitter to avoid thundering herd
//! - Concurrency limiting across all sources
//! - Graceful shutdown with in-flight fetch completion

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{Config, SchedulerConfig};
use crate::error::Result;
use crate::metrics::Metrics;
use crate::source::{FetchResult, Source};

/// Fetch scheduler that coordinates timing and concurrency.
pub struct Scheduler {
    config: SchedulerConfig,
    concurrency_semaphore: Arc<Semaphore>,
}

impl Scheduler {
    /// Create a new scheduler from configuration.
    pub fn new(config: &SchedulerConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_fetches));

        Self {
            config: config.clone(),
            concurrency_semaphore: semaphore,
        }
    }

    /// Calculate the effective interval for a source.
    pub fn effective_interval(&self, source_interval: Option<u64>) -> Duration {
        let base_secs = source_interval.unwrap_or(self.config.default_interval_secs);
        let jitter_secs = self.calculate_jitter(base_secs);
        Duration::from_secs(base_secs + jitter_secs)
    }

    /// Calculate jitter amount based on configured percentage.
    fn calculate_jitter(&self, base_secs: u64) -> u64 {
        if self.config.jitter_percent == 0 {
            return 0;
        }

        let max_jitter = base_secs * u64::from(self.config.jitter_percent) / 100;
        if max_jitter == 0 {
            return 0;
        }

        // Simple deterministic jitter based on current time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        now % max_jitter
    }

    /// Spawn a recurring fetch task for a source.
    pub fn spawn_source_task(
        &self,
        source: Arc<dyn Source>,
        interval: Duration,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
        callback: Arc<dyn Fn(Vec<FetchResult>) + Send + Sync>,
    ) {
        let semaphore = Arc::clone(&self.concurrency_semaphore);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            // Skip first immediate tick (let startup complete)
            interval_timer.tick().await;

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
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

                        match source.fetch().await {
                            Ok(results) => {
                                let total_records: usize =
                                    results.iter().map(|r| r.records.len()).sum();
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
                            }
                            Err(e) => {
                                metrics.inc_fetches_error();
                                error!(
                                    source = source.name(),
                                    error = %e,
                                    "Fetch failed"
                                );
                            }
                        }

                        metrics.dec_active_fetches();
                        drop(permit);
                    }
                    _ = shutdown.cancelled() => {
                        info!(source = source.name(), "Scheduler shutting down");
                        break;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_effective_interval_default() {
        let config = SchedulerConfig {
            default_interval_secs: 300,
            max_concurrent_fetches: 10,
            jitter_percent: 0,
        };
        let scheduler = Scheduler::new(&config);

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
        let scheduler = Scheduler::new(&config);

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
        let scheduler = Scheduler::new(&config);

        let jitter = scheduler.calculate_jitter(300);
        assert!(jitter <= 30); // 10% of 300 = 30
    }
}
