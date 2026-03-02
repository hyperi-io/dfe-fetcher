// Project:   dfe-fetcher
// File:      src/pipeline/mod.rs
// Purpose:   Main pipeline orchestration
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Pipeline orchestration module.
//!
//! Coordinates the flow of fetched data from sources/extractors
//! through enrichment and delivery to Kafka sinks.
//!
//! ## Data Flow
//!
//! ```text
//! Native Sources (AWS, Azure, M365, GCP)
//!     │
//! Container Extractors (stdout / HTTP)
//!     │
//! Plugin Extractors (.so modules)
//!     │
//! Vector Extractors (gRPC)
//!     │
//!     └─── Pipeline ──→ Enrich ──→ Kafka Sink ──→ DFE Pipeline
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::buffer::{BufferManager, TieredSink};
use crate::config::{Config, SharedConfig};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::sink::kafka::KafkaSink;
use crate::sink::Sink;
use crate::source::FetchResult;

/// Shared pipeline state accessible from handlers and schedulers.
pub struct PipelineState {
    shared_config: SharedConfig,
    kafka_sink: Option<Arc<TieredSink<KafkaSink>>>,
    buffer_manager: Arc<BufferManager>,
    ready: AtomicBool,
}

impl PipelineState {
    /// Create new pipeline state.
    pub fn new(shared_config: SharedConfig) -> Result<Self> {
        let config = shared_config.get();
        let buffer_manager = Arc::new(BufferManager::new(&config.buffer));

        // Initialise Kafka sink if brokers configured
        let kafka_sink = if !config.kafka.brokers.is_empty() {
            let primary = KafkaSink::new(&config.kafka)?;
            Some(Arc::new(TieredSink::new(primary, &config.buffer)))
        } else {
            None
        };

        Ok(Self {
            shared_config,
            kafka_sink,
            buffer_manager,
            ready: AtomicBool::new(true),
        })
    }

    /// Get the current configuration.
    pub fn config(&self) -> Config {
        self.shared_config.get()
    }

    /// Get the shared config handle.
    pub fn shared_config(&self) -> SharedConfig {
        self.shared_config.clone()
    }

    /// Check if the pipeline is ready.
    pub fn is_ready(&self) -> bool {
        if !self.ready.load(Ordering::Relaxed) {
            return false;
        }

        if self.buffer_manager.is_under_pressure() {
            return false;
        }

        if let Some(ref kafka) = self.kafka_sink {
            if !kafka.is_healthy() {
                return false;
            }
        }

        true
    }

    /// Deliver a batch of fetch results to Kafka.
    pub async fn deliver(&self, results: Vec<FetchResult>) -> Result<()> {
        let config = self.shared_config.get();
        let topic_suffix = &config.kafka.topic_suffix;

        for result in results {
            let topic = format!("{}{}", result.topic, topic_suffix);

            for record in result.records {
                let enriched = self.enrich_record(record, &result.source);
                self.send_to_kafka(&topic, enriched).await?;
            }
        }

        Ok(())
    }

    /// Deliver a single ingest message (from container/HTTP extractors).
    pub async fn deliver_ingest(&self, topic: &str, payload: Bytes) -> Result<()> {
        self.send_to_kafka(topic, payload).await
    }

    /// Enrich a record with fetcher metadata.
    fn enrich_record(&self, payload: Bytes, source: &str) -> Bytes {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        let raw = payload.as_ref();
        let Some(insert_pos) = raw.iter().rposition(|&b| b == b'}') else {
            return payload;
        };

        let mut buf = Vec::with_capacity(raw.len() + 80);
        buf.extend_from_slice(&raw[..insert_pos]);

        // Add comma if not empty object
        if let Some(pos) = raw[..insert_pos]
            .iter()
            .rposition(|b| !b.is_ascii_whitespace())
        {
            if raw[pos] != b'{' {
                buf.push(b',');
            }
        }
        buf.extend_from_slice(
            format!("\"_timestamp_fetcher\":{now_ms},\"_source_fetcher\":\"{source}\"").as_bytes(),
        );
        buf.extend_from_slice(&raw[insert_pos..]);
        Bytes::from(buf)
    }

    /// Send a message to Kafka.
    async fn send_to_kafka(&self, topic: &str, payload: Bytes) -> Result<()> {
        let Some(ref sink) = self.kafka_sink else {
            return Err(Error::Config("Kafka sink not configured".into()));
        };

        let payload_size = payload.len() as u64;
        self.buffer_manager.add_bytes(payload_size);

        let result = sink.send(topic, payload).await;

        self.buffer_manager.remove_bytes(payload_size);
        result
    }

    /// Get buffer manager for external access.
    pub fn buffer_manager(&self) -> &Arc<BufferManager> {
        &self.buffer_manager
    }

    /// Update metrics snapshot.
    pub async fn update_metrics(&self, metrics: &Metrics) {
        metrics.set_memory_usage(
            self.buffer_manager.total_bytes(),
            self.buffer_manager.memory_limit(),
        );
    }

    /// Reload configuration.
    pub fn reload_config(&self, new_config: Config) -> Result<()> {
        self.shared_config.update(new_config);
        let version = self.shared_config.version();
        info!(version, "Configuration reloaded successfully");
        Ok(())
    }
}

/// Main pipeline orchestrator.
pub struct Orchestrator {
    state: Arc<PipelineState>,
    shared_config: SharedConfig,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
}

impl Orchestrator {
    /// Create a new orchestrator.
    pub fn new(config: Config, metrics: Arc<Metrics>, shutdown: CancellationToken) -> Result<Self> {
        let shared_config = SharedConfig::new(config);
        let state = PipelineState::new(shared_config.clone())?;

        Ok(Self {
            state: Arc::new(state),
            shared_config,
            metrics,
            shutdown,
        })
    }

    /// Get shared pipeline state.
    pub fn state(&self) -> Arc<PipelineState> {
        Arc::clone(&self.state)
    }

    /// Get shared config handle.
    pub fn shared_config(&self) -> SharedConfig {
        self.shared_config.clone()
    }

    /// Run the orchestrator (background tasks).
    pub async fn run(&self) -> Result<()> {
        info!("Pipeline orchestrator running");

        // Start drain tasks for tiered sinks
        if let Some(ref kafka) = self.state.kafka_sink {
            kafka.clone().start_drain_task(self.shutdown.clone());
        }

        // Periodic metrics update (1s interval)
        let metrics_state = Arc::clone(&self.state);
        let metrics_ref = Arc::clone(&self.metrics);
        let metrics_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        metrics_state.update_metrics(&metrics_ref).await;
                    }
                    _ = metrics_shutdown.cancelled() => break,
                }
            }
        });

        // Wait for shutdown
        self.shutdown.cancelled().await;

        info!("Pipeline orchestrator shutting down");

        // Flush all sinks
        if let Some(ref kafka) = self.state.kafka_sink {
            if let Err(e) = kafka.flush().await {
                error!(error = %e, "Failed to flush Kafka sink");
            }
        }

        info!("Pipeline orchestrator stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enrich_record_injects_metadata() {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let state = PipelineState::new(shared).unwrap_or_else(|_| {
            // Kafka not configured, create minimal state
            let config = Config::default();
            let shared = SharedConfig::new(config);
            PipelineState {
                shared_config: shared,
                kafka_sink: None,
                buffer_manager: Arc::new(BufferManager::new(
                    &crate::config::BufferConfig::default(),
                )),
                ready: AtomicBool::new(true),
            }
        });

        let payload = Bytes::from(r#"{"key": "value"}"#);
        let enriched = state.enrich_record(payload, "aws.cloudtrail");
        let enriched_str = std::str::from_utf8(&enriched).unwrap();

        assert!(enriched_str.contains("\"_timestamp_fetcher\":"));
        assert!(enriched_str.contains("\"_source_fetcher\":\"aws.cloudtrail\""));

        // Verify it's still valid JSON
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert!(parsed.get("_timestamp_fetcher").is_some());
        assert_eq!(
            parsed.get("_source_fetcher").unwrap().as_str().unwrap(),
            "aws.cloudtrail"
        );
        assert_eq!(parsed.get("key").unwrap(), "value");
    }
}
