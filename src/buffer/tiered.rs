// Project:   dfe-fetcher
// File:      src/buffer/tiered.rs
// Purpose:   TieredSink wrapper with circuit breaker
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! TieredSink wrapper providing in-memory buffering when sinks are unavailable.
//!
//! Uses hyperi-rustlib's CircuitBreaker for health tracking with half-open state support.
//! Messages are buffered in memory during outages and drained when the downstream
//! sink recovers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use hyperi_rustlib::tiered_sink::{CircuitBreaker, CircuitState};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::BufferConfig;
use crate::error::Result;
use crate::sink::Sink;

/// Message queued during sink unavailability.
#[derive(Clone)]
struct SpillMessage {
    topic: String,
    payload: Bytes,
}

/// TieredSink wraps a primary sink with circuit breaker and in-memory buffering.
///
/// Uses hyperi-rustlib's CircuitBreaker for health tracking with half-open state support.
/// When the primary sink fails, messages are buffered in memory and automatically
/// drained when the sink recovers.
pub struct TieredSink<S: Sink> {
    /// Primary sink (hot path).
    primary: Arc<S>,
    /// In-memory spillover queue.
    spill_queue: Mutex<Vec<SpillMessage>>,
    /// Maximum queue size before rejecting.
    max_queue_size: usize,
    /// Circuit breaker from hyperi-rustlib with half-open state support.
    circuit: CircuitBreaker,
    /// Messages queued during outage.
    queued_count: AtomicU64,
    /// Messages drained after recovery.
    drained_count: AtomicU64,
}

impl<S: Sink + Send + Sync + 'static> TieredSink<S> {
    /// Create a new tiered sink.
    #[allow(unused_variables)]
    pub fn new(primary: S, config: &BufferConfig) -> Self {
        Self {
            primary: Arc::new(primary),
            spill_queue: Mutex::new(Vec::with_capacity(1000)),
            max_queue_size: 1000,
            circuit: CircuitBreaker::new(5, Duration::from_secs(30)),
            queued_count: AtomicU64::new(0),
            drained_count: AtomicU64::new(0),
        }
    }

    /// Check if we should attempt the hot path.
    async fn should_use_hot_path(&self) -> bool {
        let state = self.circuit.state().await;
        matches!(state, CircuitState::Closed | CircuitState::HalfOpen)
    }

    /// Queue a message for later delivery.
    fn queue_message(&self, topic: String, payload: Bytes) {
        let mut queue = self.spill_queue.lock();
        queue.push(SpillMessage { topic, payload });
        self.queued_count.fetch_add(1, Ordering::Relaxed);

        if queue.len() >= self.max_queue_size {
            warn!(
                queue_size = queue.len(),
                "Queue at capacity - backpressure recommended"
            );
        }
    }

    /// Try to drain queued messages.
    async fn try_drain(&self) -> usize {
        if !self.should_use_hot_path().await {
            return 0;
        }

        let messages: Vec<SpillMessage> = {
            let mut queue = self.spill_queue.lock();
            if queue.is_empty() {
                return 0;
            }
            let count = queue.len().min(100);
            queue.drain(0..count).collect()
        };

        let count = messages.len();
        let mut drained = 0;

        for msg in messages {
            match self.primary.send(&msg.topic, msg.payload.clone()).await {
                Ok(()) => {
                    drained += 1;
                    self.circuit.record_success().await;
                }
                Err(e) => {
                    self.circuit.record_failure().await;
                    self.queue_message(msg.topic, msg.payload);
                    debug!(error = %e, "Drain failed, re-queuing message");
                    break;
                }
            }
        }

        if drained > 0 {
            self.drained_count
                .fetch_add(drained as u64, Ordering::Relaxed);
            debug!(
                drained = drained,
                remaining = count - drained,
                "Drained queued messages"
            );
        }

        drained
    }

    /// Get statistics.
    pub async fn stats(&self) -> TieredSinkStats {
        TieredSinkStats {
            circuit_state: self.circuit.state().await,
            consecutive_failures: self.circuit.consecutive_failures(),
            queue_size: self.spill_queue.lock().len(),
            queued_total: self.queued_count.load(Ordering::Relaxed),
            drained_total: self.drained_count.load(Ordering::Relaxed),
        }
    }

    /// Start background drain task.
    pub fn start_drain_task(self: Arc<Self>, shutdown: CancellationToken) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("Drain task stopping");
                        let _ = self.try_drain().await;
                        break;
                    }
                    _ = interval.tick() => {
                        let _ = self.try_drain().await;
                    }
                }
            }
        });
    }
}

/// Statistics for the tiered sink.
#[derive(Debug, Clone)]
pub struct TieredSinkStats {
    /// Current circuit breaker state.
    pub circuit_state: CircuitState,
    /// Consecutive failures.
    pub consecutive_failures: u32,
    /// Current queue size.
    pub queue_size: usize,
    /// Total messages queued during outages.
    pub queued_total: u64,
    /// Total messages drained after recovery.
    pub drained_total: u64,
}

impl TieredSinkStats {
    /// Check if circuit is open.
    #[must_use]
    pub fn circuit_open(&self) -> bool {
        self.circuit_state == CircuitState::Open
    }
}

#[async_trait]
impl<S: Sink + Send + Sync + 'static> Sink for TieredSink<S> {
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        if !self.should_use_hot_path().await {
            self.queue_message(topic.to_string(), payload);
            return Ok(());
        }

        match self.primary.send(topic, payload.clone()).await {
            Ok(()) => {
                self.circuit.record_success().await;
                Ok(())
            }
            Err(e) => {
                self.circuit.record_failure().await;
                debug!(error = %e, topic = topic, "Primary send failed, queuing");
                self.queue_message(topic.to_string(), payload);
                Ok(())
            }
        }
    }

    async fn flush(&self) -> Result<()> {
        if let Err(e) = self.primary.flush().await {
            debug!(error = %e, "Primary flush failed");
        }

        self.try_drain().await;

        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.primary.is_healthy() || self.spill_queue.lock().len() < self.max_queue_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use std::sync::atomic::AtomicUsize;

    struct TestSink {
        fail_count: AtomicUsize,
        success_after: usize,
        #[allow(dead_code)]
        sent: Mutex<Vec<(String, Bytes)>>,
    }

    impl TestSink {
        fn new(success_after: usize) -> Self {
            Self {
                fail_count: AtomicUsize::new(0),
                success_after,
                sent: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Sink for TestSink {
        async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
            let count = self.fail_count.fetch_add(1, Ordering::Relaxed);
            if count < self.success_after {
                return Err(Error::Source("test failure".into()));
            }
            self.sent.lock().push((topic.to_string(), payload));
            Ok(())
        }

        async fn flush(&self) -> Result<()> {
            Ok(())
        }

        fn is_healthy(&self) -> bool {
            true
        }
    }

    fn test_config() -> BufferConfig {
        BufferConfig {
            memory_limit: 0,
            pressure_threshold: 0.8,
        }
    }

    #[tokio::test]
    async fn test_tiered_sink_success() {
        let primary = TestSink::new(0);
        let tiered = TieredSink::new(primary, &test_config());

        let result = tiered.send("test", Bytes::from("data")).await;
        assert!(result.is_ok());

        let stats = tiered.stats().await;
        assert_eq!(stats.queued_total, 0);
    }

    #[tokio::test]
    async fn test_tiered_sink_failure_queues() {
        let primary = TestSink::new(100);
        let tiered = TieredSink::new(primary, &test_config());

        let result = tiered.send("test", Bytes::from("data")).await;
        assert!(result.is_ok());

        let stats = tiered.stats().await;
        assert_eq!(stats.queued_total, 1);
        assert_eq!(stats.queue_size, 1);
    }

    #[tokio::test]
    async fn test_tiered_sink_circuit_breaker() {
        let primary = TestSink::new(100);
        let config = test_config();
        let tiered = TieredSink::new(primary, &config);

        for _ in 0..6 {
            let _ = tiered.send("test", Bytes::from("data")).await;
        }

        let stats = tiered.stats().await;
        assert!(stats.circuit_open());
        assert!(stats.consecutive_failures >= 5);
    }

    #[tokio::test]
    async fn test_tiered_sink_drain() {
        let primary = TestSink::new(3);
        let tiered = TieredSink::new(primary, &test_config());

        for _ in 0..3 {
            let _ = tiered.send("test", Bytes::from("data")).await;
        }

        let stats = tiered.stats().await;
        assert_eq!(stats.queue_size, 3);

        let drained = tiered.try_drain().await;
        assert!(drained > 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let primary = TestSink::new(0);
        let tiered = TieredSink::new(primary, &test_config());

        let stats = tiered.stats().await;
        assert!(!stats.circuit_open());
        assert_eq!(stats.consecutive_failures, 0);
        assert_eq!(stats.queue_size, 0);
        assert_eq!(stats.queued_total, 0);
        assert_eq!(stats.drained_total, 0);
    }
}
