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
//! through enrichment and delivery to output transports (Kafka, gRPC).
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
//!     └─── Pipeline ──→ Enrich ──→ Output Transport ──→ DFE Pipeline
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use hyperi_rustlib::dlq::{Dlq, DlqEntry};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::buffer::BufferManager;
use crate::config::{Config, SharedConfig};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::output::OutputManager;
use crate::source::FetchResult;

/// Shared pipeline state accessible from handlers and schedulers.
pub struct PipelineState {
    shared_config: SharedConfig,
    output: Option<Arc<OutputManager>>,
    buffer_manager: Arc<BufferManager>,
    dlq: Option<Dlq>,
    metrics: Arc<Metrics>,
    ready: AtomicBool,
}

impl PipelineState {
    /// Create new pipeline state.
    ///
    /// The `output` parameter is optional: pass `None` for tests or when
    /// output transports are not yet initialised (the [`Orchestrator`]
    /// creates the [`OutputManager`] asynchronously and injects it via
    /// [`set_output`](Self::set_output)).
    pub fn new(
        shared_config: SharedConfig,
        metrics: Arc<Metrics>,
        output: Option<OutputManager>,
    ) -> Result<Self> {
        let config = shared_config.get();
        let buffer_manager = Arc::new(BufferManager::new(&config.buffer));

        // Initialise DLQ if enabled
        let dlq = if config.dlq.enabled {
            match Dlq::file_only(&config.dlq, "dfe-fetcher") {
                Ok(d) => Some(d),
                Err(e) => {
                    warn!(error = %e, "Failed to initialise DLQ, continuing without it");
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            shared_config,
            output: output.map(Arc::new),
            buffer_manager,
            dlq,
            metrics,
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

        if let Some(ref output) = self.output
            && !output.any_healthy()
        {
            return false;
        }

        true
    }

    /// Deliver a batch of fetch results to output transports.
    pub async fn deliver(&self, results: Vec<FetchResult>) -> Result<()> {
        let config = self.shared_config.get();
        let topic_suffix = &config.kafka.topic_suffix;

        for result in results {
            let topic = format!("{}{}", result.topic, topic_suffix);

            for record in result.records {
                let enriched = self.enrich_record(record, &result.source);
                self.send_to_transports(&topic, enriched).await?;
            }
        }

        Ok(())
    }

    /// Deliver a single ingest message (from container/HTTP extractors).
    pub async fn deliver_ingest(&self, topic: &str, payload: Bytes) -> Result<()> {
        self.send_to_transports(topic, payload).await
    }

    /// Enrich a record with fetcher metadata.
    pub fn enrich_record(&self, payload: Bytes, source: &str) -> Bytes {
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
            && raw[pos] != b'{'
        {
            buf.push(b',');
        }
        buf.extend_from_slice(
            format!("\"_timestamp_fetcher\":{now_ms},\"_source_fetcher\":\"{source}\"").as_bytes(),
        );
        buf.extend_from_slice(&raw[insert_pos..]);
        Bytes::from(buf)
    }

    /// Send a message to output transports. On failure, routes to DLQ if available.
    async fn send_to_transports(&self, topic: &str, payload: Bytes) -> Result<()> {
        let Some(ref output) = self.output else {
            return Err(Error::Config("Output transport not configured".into()));
        };

        let payload_size = payload.len() as u64;
        self.buffer_manager.add_bytes(payload_size);

        let result = output.send_all(topic, payload.as_ref()).await;

        self.buffer_manager.remove_bytes(payload_size);

        if let Err(ref transport_err) = result {
            // Track transport health metrics
            let err_str = transport_err.to_string();
            if err_str.contains("backpressured") {
                self.metrics.inc_transport_backpressured();
            } else {
                self.metrics.inc_transport_send_errors();
            }
        }

        if let Err(ref transport_err) = result
            && let Some(ref dlq) = self.dlq
        {
            let entry = DlqEntry::new(
                "dfe-fetcher",
                format!("transport send failed: {transport_err}"),
                payload.to_vec(),
            )
            .with_destination(topic);

            if let Err(dlq_err) = dlq.send(entry).await {
                error!(
                    error = %dlq_err,
                    topic,
                    "Failed to send to DLQ after transport failure"
                );
                return result;
            }

            self.metrics.inc_messages_dlq();
            warn!(topic, error = %transport_err, "Message routed to DLQ after transport failure");
            return Ok(());
        }

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
    /// Create a new orchestrator with output transports initialised.
    ///
    /// Async because output transport creation (Kafka, gRPC) requires
    /// network connections. Pass a config with no brokers to skip
    /// output transport initialisation (tests, config-check).
    pub async fn new(
        config: Config,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let shared_config = SharedConfig::new(config);
        let cfg = shared_config.get();

        // Create output manager if any output transports are configured
        let has_output_kafka = cfg
            .output
            .kafka
            .as_ref()
            .map_or(false, |k| !k.brokers.is_empty());
        let has_legacy_kafka = !cfg.kafka.brokers.is_empty();
        let has_grpc = cfg
            .output
            .grpc
            .as_ref()
            .map_or(false, |g| g.endpoint.is_some());

        let output = if has_output_kafka || has_legacy_kafka || has_grpc {
            Some(OutputManager::new(&cfg.output, &cfg.kafka).await?)
        } else {
            info!("No output transports configured, delivery disabled");
            None
        };

        let state =
            PipelineState::new(shared_config.clone(), Arc::clone(&metrics), output)?;

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
                        // Track transport health gauge
                        if let Some(ref output) = metrics_state.output {
                            metrics_ref.set_transport_healthy(output.any_healthy());
                        }
                    }
                    _ = metrics_shutdown.cancelled() => break,
                }
            }
        });

        // Wait for shutdown
        self.shutdown.cancelled().await;

        info!("Pipeline orchestrator shutting down");

        // Close all output transports
        if let Some(ref output) = self.state.output {
            output.close_all().await;
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
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None).unwrap_or_else(|_| {
            // No output configured, create minimal state
            let config = Config::default();
            let shared = SharedConfig::new(config);
            PipelineState {
                shared_config: shared,
                output: None,
                buffer_manager: Arc::new(BufferManager::new(
                    &crate::config::BufferConfig::default(),
                )),
                dlq: None,
                metrics: Arc::new(Metrics::new()),
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
