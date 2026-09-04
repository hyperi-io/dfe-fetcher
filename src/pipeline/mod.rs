// Project:   dfe-fetcher
// File:      src/pipeline/mod.rs
// Purpose:   Main pipeline orchestration
// Language:  Rust
//
// License:   BUSL-1.1
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
use scalo::dlq::{Dlq, DlqEntry};
use scalo::logger::security;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::{Config, SharedConfig};
use crate::error::{Error, Result};
use crate::json_unwrap::unwrap_nested_json;
use crate::metrics::Metrics;
use crate::output::OutputManager;
use crate::source::FetchResult;
use scalo::memory::MemoryGuard;

/// Metadata keys `enrich_record` stamps on every record.
///
/// They are reserved: a payload that already carries one has its value moved
/// aside rather than duplicated. Two top-level keys of the same name make the
/// loader's ClickHouse JSON column reject the whole record ("Duplicate path
/// found during parsing JSON object"), and a rejected record is not
/// dead-lettered -- it is lost.
const RESERVED_KEYS: [&str; 4] = [
    "_timestamp_fetcher",
    "_timestamp_received",
    "_source",
    "_source_fetcher",
];

/// Cheap pre-filter for [`rewrite_reserved_keys`]: true when the raw bytes
/// contain the opening quote of a reserved key name anywhere. A nested field or
/// a string value matches too -- a false positive costs the slower rewrite
/// path, never correctness.
fn may_carry_reserved_key(raw: &[u8]) -> bool {
    raw.windows(8).any(|w| w == b"\"_source".as_slice())
        || raw.windows(12).any(|w| w == b"\"_timestamp_".as_slice())
}

/// Rebuild a record that already carries one or more [`RESERVED_KEYS`].
///
/// Every reserved key the payload brought is renamed to `<key>_original` and
/// the fetcher's value takes the name, so each key appears exactly once.
/// Returns `None` when the payload is not a JSON object, leaving the caller on
/// the append fast path.
fn rewrite_reserved_keys(raw: &[u8], now_ms: u64, dfe_source: &str, source: &str) -> Option<Bytes> {
    let serde_json::Value::Object(mut map) = serde_json::from_slice(raw).ok()? else {
        return None;
    };

    for key in RESERVED_KEYS {
        if let Some(existing) = map.remove(key) {
            map.insert(format!("{key}_original"), existing);
        }
    }

    map.insert("_timestamp_fetcher".to_string(), now_ms.into());
    map.insert("_timestamp_received".to_string(), now_ms.into());
    map.insert("_source".to_string(), dfe_source.into());
    map.insert("_source_fetcher".to_string(), source.into());

    serde_json::to_vec(&map).ok().map(Bytes::from)
}

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
    /// output transports are not yet initialised. The [`Orchestrator`]
    /// creates the [`OutputManager`] asynchronously and injects it through
    /// this `output` parameter.
    pub fn new(
        shared_config: SharedConfig,
        metrics: Arc<Metrics>,
        output: Option<OutputManager>,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let config = shared_config.get();

        // Create MemoryGuard — prefer env vars (cgroup-aware), fall back to BufferConfig
        let mut mg_config = scalo::memory::MemoryGuardConfig::from_env("DFE_FETCHER");
        if mg_config.limit_bytes == 0 && config.buffer.memory_limit > 0 {
            // Legacy config fallback: explicit limit from buffer.memory_limit
            mg_config.limit_bytes = config.buffer.memory_limit as u64;
        }
        if config.buffer.pressure_threshold > 0.0 && config.buffer.pressure_threshold <= 1.0 {
            // Honour legacy pressure_threshold if set explicitly
            mg_config.pressure_threshold = config.buffer.pressure_threshold;
        }
        let memory_guard = Arc::new(MemoryGuard::new(mg_config));

        // Initialise DLQ if enabled. The Kafka backend rides the SAME resolved
        // producer config as the output transport (resolve_kafka_config) --
        // dead-letters must land on the broker the data uses.
        let dlq = if config.dlq.enabled {
            let dlq_kafka = crate::output::resolve_kafka_config(&config.output, &config.kafka);
            match Dlq::spawn(&config.dlq, "dfe-fetcher", Some(&dlq_kafka), shutdown) {
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

        let total_records: usize = results.iter().map(|r| r.records.len()).sum();
        debug!(
            batch_size = results.len(),
            total_records, topic_suffix, "Delivering fetch results batch"
        );

        for result in results {
            let topic = format!("{}{}", result.topic, topic_suffix);
            let filter_expr = self.get_filter_for_source(&result.source, &config);
            let record_count = result.records.len();

            debug!(
                source = %result.source,
                topic,
                records = record_count,
                has_filter = filter_expr.is_some(),
                "Processing fetch result"
            );

            let mut passed = 0usize;
            let mut filtered = 0usize;

            // Enrich and filter records, collecting those that pass
            let mut to_send: Vec<Bytes> = Vec::with_capacity(record_count);
            let unwrap_json = config.unwrap_nested_json;
            for record in result.records {
                let record = if unwrap_json {
                    unwrap_nested_json(&record)
                } else {
                    record
                };
                let enriched = self.enrich_record(record, &result.topic, &result.source);

                // Apply CEL filter if configured — drop records that don't match
                if let Some(ref expr) = filter_expr {
                    let keep = Self::evaluate_filter(expr, &enriched);
                    tracing::trace!(
                        source = %result.source,
                        expr,
                        keep,
                        payload_bytes = enriched.len(),
                        "CEL filter decision"
                    );
                    if !keep {
                        self.metrics.inc_records_filtered();
                        filtered += 1;
                        continue;
                    }
                }

                to_send.push(enriched);
            }

            // Send all passing records concurrently (bounded by transport backpressure)
            for enriched in to_send {
                self.send_to_transports(&topic, enriched).await?;
                passed += 1;
            }

            if filter_expr.is_some() {
                debug!(
                    source = %result.source,
                    topic,
                    passed,
                    filtered,
                    "Filter applied to batch"
                );
            }
        }

        Ok(())
    }

    /// Deliver a single ingest message (from container/HTTP extractors).
    ///
    /// `source` is the DFE source name the loader routes on, `fetcher_source`
    /// the extractor that produced the record.
    pub async fn deliver_ingest(
        &self,
        source: &str,
        fetcher_source: &str,
        topic: &str,
        payload: Bytes,
    ) -> Result<()> {
        let enriched = self.enrich_record(payload, source, fetcher_source);
        self.send_to_transports(topic, enriched).await
    }

    /// Enrich a record with fetcher metadata.
    ///
    /// `_source` is the DFE source name (the topic without its suffix), the field
    /// dfe-loader routes and filters on; `_source_fetcher` names the producer.
    ///
    /// The fetcher's value wins for every key in [`RESERVED_KEYS`] -- the loader
    /// routes on `_source`, so the DFE source name must be the one that survives
    /// -- and whatever the payload carried under that name is kept as
    /// `<key>_original`.
    pub fn enrich_record(&self, payload: Bytes, dfe_source: &str, source: &str) -> Bytes {
        let now_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);

        let raw = payload.as_ref();
        let Some(insert_pos) = raw.iter().rposition(|&b| b == b'}') else {
            tracing::trace!(
                source,
                payload_bytes = raw.len(),
                "Enrich skipped — payload is not a JSON object"
            );
            return payload;
        };

        // Appending blind would emit a second copy of any reserved key the
        // payload already has, so a collision takes the parse-and-rewrite path.
        if may_carry_reserved_key(raw)
            && let Some(rewritten) = rewrite_reserved_keys(raw, now_ms, dfe_source, source)
        {
            tracing::trace!(
                source,
                original_bytes = raw.len(),
                enriched_bytes = rewritten.len(),
                "Record enriched, reserved keys rewritten"
            );
            return rewritten;
        }

        let mut buf = Vec::with_capacity(raw.len() + 160);
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
            format!("\"_timestamp_fetcher\":{now_ms},\"_timestamp_received\":{now_ms},\"_source\":\"{dfe_source}\",\"_source_fetcher\":\"{source}\"").as_bytes(),
        );
        buf.extend_from_slice(&raw[insert_pos..]);

        let enriched = Bytes::from(buf);
        tracing::trace!(
            source,
            original_bytes = raw.len(),
            enriched_bytes = enriched.len(),
            timestamp_fetcher = now_ms,
            "Record enriched with fetcher metadata"
        );
        enriched
    }

    /// Get the CEL filter expression for a source, if configured.
    ///
    /// Routing lives on [`crate::config::SourcesConfig::filter_for_source`], the
    /// same table `Config::validate` syntax-checks.
    fn get_filter_for_source(&self, source: &str, config: &Config) -> Option<String> {
        config.sources.filter_for_source(source).map(str::to_string)
    }

    /// Evaluate a CEL filter expression against a JSON record.
    /// Returns true if record should be kept, false if it should be dropped.
    ///
    /// Fail-open on SHAPE: a non-JSON payload or non-object JSON cannot be
    /// filtered, so it passes through.
    ///
    /// Fail-CLOSED on EVALUATION: `scalo::expression::evaluate_condition`
    /// returns false for a parse error, a type mismatch or a missing field, and
    /// false drops the record. Syntax errors are caught earlier by
    /// `Config::validate`, at startup and on every hot-reload; a type mismatch
    /// on a field whose type varies per record is not, and those records are
    /// dropped. Pinned by `test_evaluate_filter_with_type_mismatch_fails_closed`.
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
        scalo::expression::evaluate_condition(expression, &context)
    }

    /// Send a message to output transports. On failure, routes to DLQ if available.
    async fn send_to_transports(&self, topic: &str, payload: Bytes) -> Result<()> {
        let Some(ref output) = self.output else {
            return Err(Error::Config("Output transport not configured".into()));
        };

        let payload_size = payload.len() as u64;
        tracing::trace!(
            topic,
            payload_bytes = payload_size,
            "Sending record to output transport"
        );
        self.memory_guard.add_bytes(payload_size);

        let send_start = std::time::Instant::now();
        let result = output.send_all(topic, payload.clone()).await;
        let send_duration_ms = send_start.elapsed().as_millis();

        self.memory_guard.release(payload_size);

        if result.is_ok() {
            tracing::trace!(
                topic,
                payload_bytes = payload_size,
                duration_ms = send_duration_ms,
                "Record sent successfully"
            );
        }

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
                    if scalo::logger::log_debounced(&DLQ_DEBOUNCE, 5_000) {
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

        let state = PipelineState::new(
            shared_config.clone(),
            Arc::clone(&metrics),
            output,
            shutdown.clone(),
        )?;

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
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new())
            .unwrap_or_else(|_| {
                // No output configured, create minimal state
                let config = Config::default();
                let shared = SharedConfig::new(config);
                PipelineState {
                    shared_config: shared,
                    output: None,
                    memory_guard: Arc::new(MemoryGuard::new(scalo::memory::MemoryGuardConfig {
                        limit_bytes: 1_073_741_824, // 1 GiB for tests
                        ..Default::default()
                    })),
                    dlq: None,
                    metrics: Arc::new(Metrics::new()),
                    ready: AtomicBool::new(true),
                }
            });

        let payload = Bytes::from(r#"{"key": "value"}"#);
        let enriched = state.enrich_record(payload, "cloudtrail", "aws.cloudtrail");
        let enriched_str = std::str::from_utf8(&enriched).unwrap();

        assert!(enriched_str.contains("\"_timestamp_fetcher\":"));
        assert!(enriched_str.contains("\"_timestamp_received\":"));
        assert!(enriched_str.contains("\"_source\":\"cloudtrail\""));
        assert!(enriched_str.contains("\"_source_fetcher\":\"aws.cloudtrail\""));

        // Verify it's still valid JSON
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert!(parsed.get("_timestamp_fetcher").is_some());
        assert!(parsed.get("_timestamp_received").is_some());
        assert_eq!(
            parsed.get("_source").unwrap().as_str().unwrap(),
            "cloudtrail"
        );
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
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new()).unwrap();

        assert_eq!(
            state.get_filter_for_source("aws.cloudtrail", &config),
            Some(r#"severity == "high""#.to_string())
        );
        assert_eq!(state.get_filter_for_source("azure.defender", &config), None);
        assert_eq!(state.get_filter_for_source("unknown.source", &config), None);
    }

    // -- enrich_record tests --

    /// Helper to create a PipelineState for enrichment tests without output.
    fn make_pipeline_state() -> PipelineState {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        PipelineState::new(shared, metrics, None, CancellationToken::new()).unwrap()
    }

    #[test]
    fn test_enrich_record_empty_object() {
        let state = make_pipeline_state();
        let payload = Bytes::from("{}");
        let enriched = state.enrich_record(payload, "test", "test.source");
        let enriched_str = std::str::from_utf8(&enriched).unwrap();

        // Should be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(enriched_str).unwrap();
        assert!(parsed.get("_timestamp_fetcher").is_some());
        assert!(parsed.get("_timestamp_received").is_some());
        assert_eq!(parsed.get("_source").unwrap().as_str().unwrap(), "test");
        assert_eq!(
            parsed.get("_source_fetcher").unwrap().as_str().unwrap(),
            "test.source"
        );
    }

    #[test]
    fn test_enrich_record_non_json_returns_unchanged() {
        let state = make_pipeline_state();
        let raw = "this is not json at all";
        let payload = Bytes::from(raw);
        let enriched = state.enrich_record(payload, "src", "src.fetcher");
        // Non-JSON payload has no closing brace so should be returned unchanged
        assert_eq!(enriched.as_ref(), raw.as_bytes());
    }

    #[test]
    fn test_enrich_record_nested_json() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"outer":{"inner":42}}"#);
        let enriched = state.enrich_record(payload, "nested", "nested.src");
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();

        // Original nested data preserved
        assert_eq!(parsed["outer"]["inner"], 42);
        // Metadata at top level
        assert!(parsed.get("_timestamp_fetcher").is_some());
        assert_eq!(parsed["_source"], "nested");
        assert_eq!(parsed["_source_fetcher"], "nested.src");
    }

    #[test]
    fn test_enrich_record_large_payload() {
        let state = make_pipeline_state();
        // Build a JSON object with >10KB of content
        let mut big = String::from("{");
        for i in 0..500 {
            if i > 0 {
                big.push(',');
            }
            big.push_str(&format!(
                r#""field_{i}":"value_{val}""#,
                val = "x".repeat(20)
            ));
        }
        big.push('}');
        assert!(big.len() > 10_000, "Test payload should exceed 10KB");

        let payload = Bytes::from(big);
        let enriched = state.enrich_record(payload, "large", "large.source");
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();

        // Metadata added
        assert!(parsed.get("_timestamp_fetcher").is_some());
        assert_eq!(parsed["_source"], "large");
        assert_eq!(parsed["_source_fetcher"], "large.source");
        // Original fields preserved
        assert!(parsed.get("field_0").is_some());
        assert!(parsed.get("field_499").is_some());
    }

    #[test]
    fn test_enrich_record_unicode_content() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"name":"日本語テスト","emoji":"🚀🔥"}"#);
        let enriched = state.enrich_record(payload, "unicode", "unicode.src");
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();

        // Unicode preserved
        assert_eq!(parsed["name"], "日本語テスト");
        assert_eq!(parsed["emoji"], "🚀🔥");
        // Metadata added
        assert!(parsed.get("_timestamp_fetcher").is_some());
    }

    #[test]
    fn test_enrich_record_special_chars_in_source() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"key":"val"}"#);
        let enriched = state.enrich_record(payload, "special", "source/with\"special");
        let enriched_str = std::str::from_utf8(&enriched).unwrap();
        // The source name is inserted as a JSON string value — verify it's present
        assert!(
            enriched_str.contains("source/with\\\"special")
                || enriched_str.contains("source/with\"special")
        );
    }

    // -- PipelineState tests --

    #[test]
    fn test_pipeline_state_is_ready_under_memory_pressure() {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        // Create with a very low memory limit to trigger pressure
        let state = PipelineState {
            shared_config: shared,
            output: None,
            memory_guard: Arc::new(MemoryGuard::new(scalo::memory::MemoryGuardConfig {
                limit_bytes: 1, // 1 byte — will be under pressure
                pressure_threshold: 0.01,
                ..Default::default()
            })),
            dlq: None,
            metrics,
            ready: AtomicBool::new(true),
        };
        // Add bytes to trigger pressure
        state.memory_guard.add_bytes(100);
        assert!(
            !state.is_ready(),
            "Pipeline should not be ready when memory is under pressure"
        );
    }

    #[test]
    fn test_pipeline_state_output_healthy_no_output() {
        let state = make_pipeline_state();
        // No output configured — should report healthy (nothing to fail)
        assert!(
            state.output_healthy(),
            "No output configured should be considered healthy"
        );
    }

    // -- evaluate_filter tests --

    #[test]
    fn test_evaluate_filter_or_expr_matches_high() {
        let payload = Bytes::from(r#"{"severity":"high","eventName":"CreateUser"}"#);
        let result = PipelineState::evaluate_filter(
            r#"severity == "high" || severity == "critical""#,
            &payload,
        );
        assert!(result, "should match severity=high");
    }

    #[test]
    fn test_evaluate_filter_or_expr_matches_critical() {
        let payload = Bytes::from(r#"{"severity":"critical","eventName":"DeleteRole"}"#);
        let result = PipelineState::evaluate_filter(
            r#"severity == "high" || severity == "critical""#,
            &payload,
        );
        assert!(result, "should match severity=critical");
    }

    #[test]
    fn test_evaluate_filter_or_expr_no_match_low() {
        let payload = Bytes::from(r#"{"severity":"low","eventName":"DescribeInstances"}"#);
        let result = PipelineState::evaluate_filter(
            r#"severity == "high" || severity == "critical""#,
            &payload,
        );
        assert!(!result, "should not match severity=low");
    }

    #[test]
    fn test_evaluate_filter_numeric_gt_true() {
        let payload = Bytes::from(r#"{"count":200,"name":"test"}"#);
        let result = PipelineState::evaluate_filter("count > 100", &payload);
        assert!(result, "count=200 should pass count > 100");
    }

    #[test]
    fn test_evaluate_filter_numeric_gt_false() {
        let payload = Bytes::from(r#"{"count":50,"name":"test"}"#);
        let result = PipelineState::evaluate_filter("count > 100", &payload);
        assert!(!result, "count=50 should fail count > 100");
    }

    // -- get_filter_for_source routing --

    #[test]
    fn test_get_filter_routes_all_prefixes() {
        let mut config = Config::default();
        config.sources.aws.filter = Some("aws_filter".to_string());
        config.sources.azure.filter = Some("azure_filter".to_string());
        config.sources.m365.filter = Some("m365_filter".to_string());
        config.sources.gcp.filter = Some("gcp_filter".to_string());

        let shared = SharedConfig::new(config.clone());
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new()).unwrap();

        assert_eq!(
            state.get_filter_for_source("aws.cloudtrail", &config),
            Some("aws_filter".to_string())
        );
        assert_eq!(
            state.get_filter_for_source("azure.sentinel", &config),
            Some("azure_filter".to_string())
        );
        assert_eq!(
            state.get_filter_for_source("m365.audit_log", &config),
            Some("m365_filter".to_string())
        );
        assert_eq!(
            state.get_filter_for_source("gcp.audit_logs", &config),
            Some("gcp_filter".to_string())
        );
        // Unknown prefix returns None
        assert_eq!(state.get_filter_for_source("unknown.source", &config), None);
        assert_eq!(state.get_filter_for_source("something", &config), None);
    }

    /// A filter on any source reaches the records. Only aws / azure / m365 /
    /// gcp used to be routed, so a filter on the other fifteen sources was
    /// accepted at startup, reported nowhere, and never applied -- every record
    /// the operator asked to exclude shipped anyway.
    #[test]
    fn test_filter_routes_for_every_source() {
        let mut config = Config::default();
        let expr = r#"action == "x""#.to_string();
        config.sources.github.filter = Some(expr.clone());
        config.sources.okta.filter = Some(expr.clone());
        config.sources.slack.filter = Some(expr.clone());
        config.sources.object_store.filter = Some(expr.clone());
        config.sources.salesforce.filter = Some(expr.clone());
        config.sources.google_workspace.filter = Some(expr.clone());

        let shared = SharedConfig::new(config.clone());
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new()).unwrap();

        for tag in [
            "github.audit_log",
            "okta.system_log",
            "slack.audit_logs",
            "object_store.aws_cloudtrail",
            "salesforce.event_log_file",
            "google_workspace.login",
        ] {
            assert_eq!(
                state.get_filter_for_source(tag, &config),
                Some(expr.clone()),
                "no filter routed for {tag}, so its records ship unfiltered"
            );
        }
    }

    /// `gcp_pubsub` records must not pick up the `gcp` audit-log filter. The
    /// old routing was `source.starts_with("gcp")`, which matched both.
    #[test]
    fn test_gcp_prefix_does_not_capture_gcp_pubsub() {
        let mut config = Config::default();
        config.sources.gcp.filter = Some("gcp_only".to_string());

        let shared = SharedConfig::new(config.clone());
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new()).unwrap();

        assert_eq!(
            state.get_filter_for_source("gcp.cloud_logging", &config),
            Some("gcp_only".to_string())
        );
        assert_eq!(
            state.get_filter_for_source("gcp_pubsub.my-sub", &config),
            None,
            "the gcp filter must not apply to gcp_pubsub records"
        );
    }

    // -- deliver() tests (no output configured) --

    #[tokio::test]
    async fn test_deliver_without_output_returns_config_error() {
        let state = make_pipeline_state();
        let result = FetchResult {
            records: vec![Bytes::from(r#"{"event":"x"}"#)],
            source: "test".into(),
            topic: "test".into(),
        };
        let err = state
            .deliver(vec![result])
            .await
            .expect_err("deliver without output must fail");
        match err {
            Error::Config(msg) => assert!(
                msg.contains("Output transport not configured"),
                "unexpected error message: {msg}"
            ),
            other => panic!("expected Error::Config, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_deliver_ingest_without_output_returns_config_error() {
        let state = make_pipeline_state();
        let err = state
            .deliver_ingest("any", "test", "any_land", Bytes::from(r#"{"key":"val"}"#))
            .await
            .expect_err("deliver_ingest without output must fail");
        match err {
            Error::Config(msg) => assert!(
                msg.contains("Output transport not configured"),
                "unexpected error message: {msg}"
            ),
            other => panic!("expected Error::Config, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_deliver_empty_batch_succeeds_without_output() {
        // Empty batch never attempts a send, so no error even with no output.
        let state = make_pipeline_state();
        state
            .deliver(vec![])
            .await
            .expect("empty batch should succeed");
    }

    #[tokio::test]
    async fn test_deliver_with_filter_dropping_all_succeeds_without_output() {
        // If CEL filter drops every record, send_to_transports is never called,
        // so deliver() returns Ok(()) even with no output configured.
        let mut config = Config::default();
        config.sources.aws.filter = Some(r#"severity == "never_matches""#.to_string());
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new()).unwrap();

        let result = FetchResult {
            records: vec![Bytes::from(r#"{"severity":"low"}"#)],
            source: "aws.cloudtrail".into(),
            topic: "aws_cloudtrail".into(),
        };
        state
            .deliver(vec![result])
            .await
            .expect("all-dropped batch should succeed");
    }

    // -- reload_config tests --

    #[test]
    fn test_reload_config_updates_and_increments_version() {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let initial_version = shared.version();
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new()).unwrap();

        let mut new_config = Config::default();
        new_config.scheduler.default_interval_secs = 999;
        state
            .reload_config(new_config)
            .expect("reload_config should succeed");

        // Version incremented
        assert_eq!(
            state.shared_config().version(),
            initial_version + 1,
            "version should increment on reload"
        );
        // Config content applied
        assert_eq!(
            state.config().scheduler.default_interval_secs,
            999,
            "reloaded config should be returned by config()"
        );
    }

    // -- Orchestrator tests --

    #[tokio::test]
    async fn test_orchestrator_new_without_output_succeeds() {
        let config = Config::default(); // No brokers, no output.kafka, no output.grpc
        let metrics = Arc::new(Metrics::new());
        let shutdown = CancellationToken::new();
        let orchestrator = Orchestrator::new(config, metrics, shutdown)
            .await
            .expect("Orchestrator should construct without output");

        let state = orchestrator.state();
        // With no output configured, output_healthy reports true
        assert!(
            state.output_healthy(),
            "no output = considered healthy (nothing to fail)"
        );
        // is_ready should still return true since no output was configured
        assert!(
            state.is_ready(),
            "pipeline should be ready when no output is configured"
        );
    }

    #[tokio::test]
    async fn test_orchestrator_run_exits_on_shutdown() {
        let config = Config::default();
        let metrics = Arc::new(Metrics::new());
        let shutdown = CancellationToken::new();
        let orchestrator = Orchestrator::new(config, metrics, shutdown.clone())
            .await
            .expect("Orchestrator::new should succeed");

        // Cancel first so run() exits immediately
        shutdown.cancel();

        let result = tokio::time::timeout(Duration::from_secs(5), orchestrator.run()).await;
        let run_result = result.expect("run() should complete within timeout");
        run_result.expect("run() should exit cleanly on shutdown");
    }

    // -- update_metrics --

    #[tokio::test]
    async fn test_update_metrics_writes_memory_gauges() {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        // Build state with a known memory limit so the gauges have a predictable value
        let state = PipelineState {
            shared_config: shared,
            output: None,
            memory_guard: Arc::new(MemoryGuard::new(scalo::memory::MemoryGuardConfig {
                limit_bytes: 524_288_000, // 500 MB
                pressure_threshold: 0.8,
                ..Default::default()
            })),
            dlq: None,
            metrics: Arc::clone(&metrics),
            ready: AtomicBool::new(true),
        };

        // Push some bytes through the guard so current_bytes > 0
        state.memory_guard.add_bytes(4096);
        state.update_metrics(&metrics).await;

        // Verify the gauges were written by rendering the prom output
        let rendered = metrics.render();
        assert!(
            rendered.contains("dfe_fetcher_memory_limit_bytes 524288000"),
            "limit gauge should be updated: \n{rendered}"
        );
        assert!(
            rendered.contains("dfe_fetcher_memory_used_bytes 4096"),
            "used gauge should be updated: \n{rendered}"
        );
    }

    // -- enrich_record against payloads that already carry a reserved key --

    /// Count the top-level occurrences of a key name in the raw output. A
    /// `serde_json` parse cannot see a duplicate (it keeps the last), so
    /// duplicate-key assertions have to be made on the bytes.
    fn key_occurrences(enriched: &Bytes, key: &str) -> usize {
        let needle = format!("\"{key}\":");
        std::str::from_utf8(enriched)
            .unwrap()
            .matches(&needle)
            .count()
    }

    #[test]
    fn test_enrich_record_with_existing_timestamp_received() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"event":"x","_timestamp_received":12345}"#);
        let enriched = state.enrich_record(payload, "ingest", "ingest.source");

        assert_eq!(key_occurrences(&enriched, "_timestamp_received"), 1);
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["event"], "x");
        assert_eq!(parsed["_source"], "ingest");
        assert_eq!(parsed["_source_fetcher"], "ingest.source");
        assert!(parsed["_timestamp_fetcher"].is_number());
        assert!(parsed["_timestamp_received"].is_number());
        // The caller's stamp is kept beside ours, not dropped.
        assert_eq!(parsed["_timestamp_received_original"], 12345);
    }

    #[test]
    fn test_enrich_record_with_existing_source() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"event":"x","_source":"producer-set"}"#);
        let enriched = state.enrich_record(payload, "ingest", "ingest.source");

        assert_eq!(
            key_occurrences(&enriched, "_source"),
            1,
            "two _source keys make ClickHouse reject the record"
        );
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["event"], "x");
        // The DFE source name wins -- it is what dfe-loader routes on.
        assert_eq!(parsed["_source"], "ingest");
        assert_eq!(parsed["_source_original"], "producer-set");
        assert_eq!(parsed["_source_fetcher"], "ingest.source");
    }

    #[test]
    fn test_enrich_record_with_existing_source_fetcher() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"event":"x","_source_fetcher":"producer-set"}"#);
        let enriched = state.enrich_record(payload, "ingest", "ingest.source");

        assert_eq!(key_occurrences(&enriched, "_source_fetcher"), 1);
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_source_fetcher"], "ingest.source");
        assert_eq!(parsed["_source_fetcher_original"], "producer-set");
        assert_eq!(parsed["_source"], "ingest");
    }

    #[test]
    fn test_enrich_record_with_every_reserved_key_present() {
        let state = make_pipeline_state();
        let payload = Bytes::from(
            r#"{"event":"x","_source":"a","_source_fetcher":"b","_timestamp_fetcher":1,"_timestamp_received":2}"#,
        );
        let enriched = state.enrich_record(payload, "ingest", "ingest.source");

        for key in RESERVED_KEYS {
            assert_eq!(key_occurrences(&enriched, key), 1, "duplicate {key}");
        }
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_source"], "ingest");
        assert_eq!(parsed["_source_fetcher"], "ingest.source");
        assert_eq!(parsed["_source_original"], "a");
        assert_eq!(parsed["_source_fetcher_original"], "b");
        assert_eq!(parsed["_timestamp_fetcher_original"], 1);
        assert_eq!(parsed["_timestamp_received_original"], 2);
    }

    #[test]
    fn test_enrich_record_reserved_key_nested_only_takes_fast_path() {
        // A nested `_source` is not a top-level collision. The pre-filter
        // matches it, the rewrite leaves it where it is, and the record still
        // ends up with exactly one top-level `_source`.
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"inner":{"_source":"nested"}}"#);
        let enriched = state.enrich_record(payload, "ingest", "ingest.source");

        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_source"], "ingest");
        assert_eq!(parsed["inner"]["_source"], "nested");
        assert!(parsed.get("_source_original").is_none());
    }

    #[test]
    fn test_may_carry_reserved_key() {
        assert!(may_carry_reserved_key(br#"{"_source":"x"}"#));
        assert!(may_carry_reserved_key(br#"{"_timestamp_received":1}"#));
        assert!(!may_carry_reserved_key(br#"{"event":"x","id":7}"#));
        assert!(!may_carry_reserved_key(b"{}"));
    }

    /// Exercise PipelineState::new with DLQ enabled to cover the DLQ init branch.
    #[tokio::test]
    async fn test_pipeline_state_new_with_dlq_enabled() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.dlq.enabled = true;
        config.dlq.file.path = tmp.path().join("dlq");

        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new())
            .expect("state creation must succeed");

        // Pipeline reports ready (DLQ init succeeded)
        assert!(state.is_ready(), "state should be ready");
    }

    /// Exercise the `deliver` path: a batch with an applied CEL filter that
    /// drops records. Since no output is configured, the only possible send
    /// is skipped, but the filter counting path is exercised.
    #[tokio::test]
    async fn test_deliver_filter_drops_some_records() {
        let mut config = Config::default();
        config.sources.aws.filter = Some(r#"severity == "high""#.to_string());
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new())
            .expect("state creation must succeed");

        // 2 records: 1 matches filter (kept — send attempts fail with no output)
        //            1 doesn't match (filtered out)
        let results = vec![crate::source::FetchResult {
            records: vec![
                Bytes::from(r#"{"severity":"low"}"#),  // filtered out
                Bytes::from(r#"{"severity":"high"}"#), // kept → send fails
            ],
            source: "aws.cloudtrail".to_string(),
            topic: "aws".to_string(),
        }];

        // With output=None, the filtered-in record's send fails with Config error.
        // The filtered-out record never reaches send.
        let err = state.deliver(results).await.unwrap_err();
        assert!(
            matches!(err, Error::Config(_)),
            "Expected Config error from missing output, got {err:?}"
        );
    }

    /// An unparseable filter expression drops the record: it fails CLOSED.
    /// `scalo::expression::evaluate_condition` returns `false` for a parse
    /// error, and `false` means drop.
    ///
    /// `Config::validate` runs `scalo::expression::validate` over the aws /
    /// azure / m365 / gcp filters at startup and on every hot-reload, and those
    /// are the only four `get_filter_for_source` routes, so an unparseable
    /// expression should not reach here. This pins which way it goes if one does.
    #[test]
    fn test_evaluate_filter_with_invalid_expression_fails_closed() {
        let payload = Bytes::from(r#"{"key":"value"}"#);
        assert!(
            !PipelineState::evaluate_filter("@@@invalid", &payload),
            "an unparseable expression must drop the record; a flip to \
             fail-open means evaluate_filter's doc comment and \
             Config::validate's filter checks both need revisiting"
        );
    }

    /// A type-mismatched comparison also drops the record, and unlike a syntax
    /// error it is reachable in production: startup validates the expression,
    /// not the per-record field types, so a field whose type varies only errors
    /// at evaluation time and those records are dropped.
    #[test]
    fn test_evaluate_filter_with_type_mismatch_fails_closed() {
        // `count` is a string, so `count > 100` cannot be evaluated.
        let payload = Bytes::from(r#"{"count":"not-a-number"}"#);
        assert!(
            !PipelineState::evaluate_filter("count > 100", &payload),
            "a type-mismatched comparison drops the record"
        );
    }

    /// Exercise shared_config() getter.
    #[test]
    fn test_shared_config_getter_returns_handle() {
        let state = make_pipeline_state();
        let shared = state.shared_config();
        let config = shared.get();
        assert_eq!(config.scheduler.default_interval_secs, 300);
    }
}
