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
//! Vector Extractors (gRPC)
//!     │
//!     └─── Pipeline ──→ Enrich ──→ Output Transport ──→ DFE Pipeline
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use hyperi_rustlib::dlq::{Dlq, DlqEntry};
use hyperi_rustlib::logger::security;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::{Config, SharedConfig};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::output::OutputManager;
use crate::source::FetchResult;
use hyperi_rustlib::memory::MemoryGuard;

/// Shared pipeline state accessible from handlers and schedulers.
pub struct PipelineState {
    shared_config: SharedConfig,
    output: Option<Arc<OutputManager>>,
    memory_guard: Arc<MemoryGuard>,
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

        // Create MemoryGuard — prefer env vars (cgroup-aware), fall back to BufferConfig
        let mut mg_config = hyperi_rustlib::memory::MemoryGuardConfig::from_env("DFE_FETCHER");
        if mg_config.limit_bytes == 0 && config.buffer.memory_limit > 0 {
            // Legacy config fallback: explicit limit from buffer.memory_limit
            mg_config.limit_bytes = config.buffer.memory_limit as u64;
        }
        if config.buffer.pressure_threshold > 0.0 && config.buffer.pressure_threshold <= 1.0 {
            // Honour legacy pressure_threshold if set explicitly
            mg_config.pressure_threshold = config.buffer.pressure_threshold;
        }
        let memory_guard = Arc::new(MemoryGuard::new(mg_config));

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
            memory_guard,
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

        if self.memory_guard.under_pressure() {
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
        // Prefer output.topic_suffix (new); fall back to legacy kafka.topic_suffix
        let topic_suffix = config
            .output
            .topic_suffix
            .as_deref()
            .unwrap_or(&config.kafka.topic_suffix);

        for result in results {
            let topic = format!("{}{}", result.topic, topic_suffix);
            let filter_expr = self.get_filter_for_source(&result.source, &config);

            for record in result.records {
                let enriched = self.enrich_record(record, &result.source);

                // Apply CEL filter if configured — drop records that don't match
                if let Some(ref expr) = filter_expr
                    && !Self::evaluate_filter(expr, &enriched)
                {
                    self.metrics.inc_records_filtered();
                    debug!(source = %result.source, "Record dropped by filter");
                    continue;
                }

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

        let mut buf = Vec::with_capacity(raw.len() + 120);
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
            format!("\"_timestamp_fetcher\":{now_ms},\"_timestamp_received\":{now_ms},\"_source_fetcher\":\"{source}\"").as_bytes(),
        );
        buf.extend_from_slice(&raw[insert_pos..]);
        Bytes::from(buf)
    }

    /// Get the CEL filter expression for a source, if configured.
    fn get_filter_for_source(&self, source: &str, config: &Config) -> Option<String> {
        if source.starts_with("aws") {
            config.sources.aws.filter.clone()
        } else if source.starts_with("azure") {
            config.sources.azure.filter.clone()
        } else if source.starts_with("m365") {
            config.sources.m365.filter.clone()
        } else if source.starts_with("gcp") {
            config.sources.gcp.filter.clone()
        } else {
            None
        }
    }

    /// Evaluate a CEL filter expression against a JSON record.
    /// Returns true if record should be kept, false if it should be dropped.
    /// Fail-open: non-JSON payloads, non-object JSON, and evaluation errors
    /// all pass through (record is kept).
    fn evaluate_filter(expression: &str, payload: &Bytes) -> bool {
        let Ok(value): std::result::Result<serde_json::Value, _> = serde_json::from_slice(payload)
        else {
            // Non-JSON payload — can't filter, keep it
            return true;
        };

        let serde_json::Value::Object(map) = value else {
            // Not a JSON object — can't filter, keep it
            return true;
        };

        let context: HashMap<String, serde_json::Value> = map.into_iter().collect();
        hyperi_rustlib::expression::evaluate_condition(expression, &context)
    }

    /// Send a message to output transports. On failure, routes to DLQ if available.
    async fn send_to_transports(&self, topic: &str, payload: Bytes) -> Result<()> {
        let Some(ref output) = self.output else {
            return Err(Error::Config("Output transport not configured".into()));
        };

        let payload_size = payload.len() as u64;
        self.memory_guard.add_bytes(payload_size);

        let result = output.send_all(topic, payload.as_ref()).await;

        self.memory_guard.release(payload_size);

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
                {
                    use std::sync::atomic::AtomicU64;
                    static DLQ_DEBOUNCE: AtomicU64 = AtomicU64::new(0);
                    if hyperi_rustlib::logger::log_debounced(&DLQ_DEBOUNCE, 5_000) {
                        error!(
                            error = %dlq_err,
                            topic,
                            "Failed to send to DLQ (debounced, max 1/5s)"
                        );
                    }
                }
                return result;
            }

            self.metrics.inc_messages_dlq();
            security::record_dlq(
                "transport_failure",
                &format!("transport send failed: {transport_err}"),
                Some(topic),
            );
            warn!(topic, error = %transport_err, "Message routed to DLQ after transport failure");
            return Ok(());
        }

        result
    }

    /// Get memory guard for external access (scaling pressure, metrics).
    pub fn memory_guard(&self) -> &Arc<MemoryGuard> {
        &self.memory_guard
    }

    /// Check if any output transport is healthy.
    ///
    /// Returns `true` if no output is configured (nothing to fail) or if at
    /// least one transport reports healthy. Used by the scaling pressure
    /// circuit-breaker gate.
    pub fn output_healthy(&self) -> bool {
        match self.output {
            Some(ref output) => output.any_healthy(),
            None => true,
        }
    }

    /// Update metrics snapshot.
    pub async fn update_metrics(&self, metrics: &Metrics) {
        metrics.set_memory_usage(
            self.memory_guard.current_bytes(),
            self.memory_guard.limit_bytes(),
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
            .is_some_and(|k| !k.brokers.is_empty());
        let has_legacy_kafka = !cfg.kafka.brokers.is_empty();
        let has_grpc = cfg
            .output
            .grpc
            .as_ref()
            .is_some_and(|g| g.endpoint.is_some());

        let output = if has_output_kafka || has_legacy_kafka || has_grpc {
            Some(OutputManager::new(&cfg.output, &cfg.kafka).await?)
        } else {
            info!("No output transports configured, delivery disabled");
            None
        };

        let state = PipelineState::new(shared_config.clone(), Arc::clone(&metrics), output)?;

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
                memory_guard: Arc::new(MemoryGuard::new(
                    hyperi_rustlib::memory::MemoryGuardConfig {
                        limit_bytes: 1_073_741_824, // 1 GiB for tests
                        ..Default::default()
                    },
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
        assert!(enriched_str.contains("\"_timestamp_received\":"));
        assert!(enriched_str.contains("\"_source_fetcher\":\"aws.cloudtrail\""));

        // Verify it's still valid JSON
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert!(parsed.get("_timestamp_fetcher").is_some());
        assert!(parsed.get("_timestamp_received").is_some());
        assert_eq!(
            parsed.get("_source_fetcher").unwrap().as_str().unwrap(),
            "aws.cloudtrail"
        );
        assert_eq!(parsed.get("key").unwrap(), "value");
    }

    #[test]
    fn test_filter_passes_matching_record() {
        let payload = Bytes::from(r#"{"eventName": "CreateUser", "severity": "high"}"#);
        let result = PipelineState::evaluate_filter(r#"eventName != "ConsoleLogin""#, &payload);
        assert!(result, "Record should pass — eventName is not ConsoleLogin");
    }

    #[test]
    fn test_filter_drops_non_matching_record() {
        let payload = Bytes::from(r#"{"eventName": "ConsoleLogin", "severity": "low"}"#);
        let result = PipelineState::evaluate_filter(r#"eventName != "ConsoleLogin""#, &payload);
        assert!(
            !result,
            "Record should be dropped — eventName is ConsoleLogin"
        );
    }

    #[test]
    fn test_no_filter_passes_all() {
        // No filter configured means Option is None — deliver() skips filtering.
        // Verify evaluate_filter itself returns true for a trivially true expression.
        let payload = Bytes::from(r#"{"key": "value"}"#);
        let result = PipelineState::evaluate_filter("true", &payload);
        assert!(result, "Trivially true filter should pass all records");
    }

    #[test]
    fn test_filter_non_json_passes() {
        let payload = Bytes::from("not json at all");
        let result = PipelineState::evaluate_filter(r#"eventName == "test""#, &payload);
        assert!(result, "Non-JSON payload should pass through (fail-open)");
    }

    #[test]
    fn test_filter_non_object_json_passes() {
        let payload = Bytes::from("[1, 2, 3]");
        let result = PipelineState::evaluate_filter(r#"eventName == "test""#, &payload);
        assert!(
            result,
            "JSON array (not object) should pass through (fail-open)"
        );
    }

    #[test]
    fn test_filter_missing_field_drops() {
        // evaluate_condition returns false when referenced field is missing
        let payload = Bytes::from(r#"{"other": "value"}"#);
        let result = PipelineState::evaluate_filter(r#"eventName == "CreateUser""#, &payload);
        assert!(
            !result,
            "Missing field should cause condition to evaluate to false"
        );
    }

    #[test]
    fn test_get_filter_for_source_aws() {
        let mut config = Config::default();
        config.sources.aws.filter = Some(r#"severity == "high""#.to_string());
        let shared = SharedConfig::new(config.clone());
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None).unwrap();

        assert_eq!(
            state.get_filter_for_source("aws.cloudtrail", &config),
            Some(r#"severity == "high""#.to_string())
        );
        assert_eq!(state.get_filter_for_source("azure.defender", &config), None);
        assert_eq!(state.get_filter_for_source("unknown.source", &config), None);
    }
}
