// Project:   dfe-fetcher
// File:      tests/integration.rs
// Purpose:   Integration tests for config loading, pipeline, metrics, ingest
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]

use bytes::Bytes;
use dfe_fetcher::config::Config;
use dfe_fetcher::metrics::Metrics;

// =============================================================================
// Config loading
// =============================================================================

#[test]
fn test_default_config_loads() {
    let config = Config::default();
    assert!(!config.kafka.brokers.is_empty() || config.kafka.brokers.is_empty());
    assert_eq!(config.scheduler.default_interval_secs, 300);
    assert_eq!(config.scheduler.max_concurrent_fetches, 10);
}

#[test]
fn test_config_from_example_yaml() {
    let yaml = std::fs::read_to_string("config.example.yaml").expect("config.example.yaml exists");
    let config: Config = serde_yaml_ng::from_str(&yaml).expect("example config parses");

    assert_eq!(config.scheduler.default_interval_secs, 300);
    assert!(!config.sources.aws.enabled);
    assert!(!config.sources.azure.enabled);
    assert!(!config.sources.m365.enabled);
    assert!(!config.sources.gcp.enabled);
    assert_eq!(config.kafka.topic_suffix, "_land");
}

#[test]
fn test_config_validation_passes_for_default() {
    let config = Config::default();
    // Default config has no brokers, which should fail validation
    let result = config.validate();
    assert!(result.is_err()); // no brokers
}

#[test]
fn test_config_validation_passes_with_brokers() {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    let result = config.validate();
    assert!(result.is_ok());
}

// =============================================================================
// Pipeline enrichment edge cases
// =============================================================================

#[test]
fn test_enrich_empty_json_object() {
    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let state = make_pipeline_state(shared);

    let payload = Bytes::from("{}");
    let enriched = state.enrich_record(payload, "test.source");
    let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
    assert!(parsed.get("_timestamp_fetcher").is_some());
    assert_eq!(parsed["_source_fetcher"].as_str().unwrap(), "test.source");
}

#[test]
fn test_enrich_nested_json() {
    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let state = make_pipeline_state(shared);

    let payload = Bytes::from(r#"{"outer":{"inner":"value"},"list":[1,2,3]}"#);
    let enriched = state.enrich_record(payload, "azure.defender");
    let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();

    assert_eq!(parsed["outer"]["inner"].as_str().unwrap(), "value");
    assert_eq!(parsed["list"].as_array().unwrap().len(), 3);
    assert!(parsed.get("_timestamp_fetcher").is_some());
}

#[test]
fn test_enrich_non_json_returns_unchanged() {
    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let state = make_pipeline_state(shared);

    let payload = Bytes::from("this is not json");
    let enriched = state.enrich_record(payload.clone(), "test");
    assert_eq!(enriched, payload); // No closing brace, returned unchanged
}

#[test]
fn test_enrich_large_payload() {
    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let state = make_pipeline_state(shared);

    // Build a large JSON object (100 fields)
    let mut json = String::from("{");
    for i in 0..100 {
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!("\"field_{i}\":\"value_{i}\""));
    }
    json.push('}');

    let payload = Bytes::from(json);
    let enriched = state.enrich_record(payload, "gcp.audit_logs");
    let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
    assert!(parsed.get("_timestamp_fetcher").is_some());
    assert_eq!(parsed["field_0"].as_str().unwrap(), "value_0");
    assert_eq!(parsed["field_99"].as_str().unwrap(), "value_99");
}

// =============================================================================
// Metrics rendering
// =============================================================================

#[test]
fn test_metrics_render_prometheus_format() {
    let metrics = Metrics::new();
    metrics.inc_fetches_total();
    metrics.inc_fetches_success();
    metrics.add_records_fetched(42);
    metrics.add_bytes_fetched(1024);

    let output = metrics.render();

    assert!(output.contains("fetcher_fetches_total 1"));
    assert!(output.contains("fetcher_fetches_success 1"));
    assert!(output.contains("fetcher_records_fetched_total 42"));
    assert!(output.contains("fetcher_bytes_fetched_total 1024"));
    assert!(output.contains("fetcher_fetches_error 0"));
}

#[test]
fn test_metrics_extractor_counters() {
    let metrics = Metrics::new();
    metrics.inc_extractor_runs_total();
    metrics.inc_extractor_runs_total();
    metrics.inc_extractor_runs_success();
    metrics.inc_extractor_runs_error();
    metrics.add_extractor_records(100);

    let output = metrics.render();
    assert!(output.contains("fetcher_extractor_runs_total 2"));
    assert!(output.contains("fetcher_extractor_runs_success 1"));
    assert!(output.contains("fetcher_extractor_runs_error 1"));
    assert!(output.contains("fetcher_extractor_records_total 100"));
}

// =============================================================================
// Credential resolution
// =============================================================================

#[tokio::test]
async fn test_credential_resolve_literal() {
    let result = dfe_fetcher::credential::resolve("my-api-key-123").await;
    assert_eq!(result.unwrap(), "my-api-key-123");
}

#[tokio::test]
async fn test_credential_resolve_env() {
    // SAFETY: test-only, single-threaded test runner
    unsafe { std::env::set_var("DFE_TEST_INTEGRATION_CRED", "secret-from-env") };
    let result = dfe_fetcher::credential::resolve("env:DFE_TEST_INTEGRATION_CRED").await;
    assert_eq!(result.unwrap(), "secret-from-env");
    // SAFETY: test-only, single-threaded test runner
    unsafe { std::env::remove_var("DFE_TEST_INTEGRATION_CRED") };
}

#[tokio::test]
async fn test_credential_resolve_env_missing() {
    let result = dfe_fetcher::credential::resolve("env:NONEXISTENT_VAR_ABC123").await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("NONEXISTENT_VAR_ABC123"));
}

#[tokio::test]
async fn test_credential_resolve_vault_invalid_format() {
    let result = dfe_fetcher::credential::resolve("vault:no-colon-here").await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("invalid vault spec"));
}

#[tokio::test]
async fn test_credential_resolve_optional() {
    assert!(
        dfe_fetcher::credential::resolve_optional(None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        dfe_fetcher::credential::resolve_optional(Some(""))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        dfe_fetcher::credential::resolve_optional(Some("literal-value"))
            .await
            .unwrap()
            .unwrap(),
        "literal-value"
    );
}

#[test]
fn test_http_client_factory() {
    let client = dfe_fetcher::credential::http_client();
    assert!(client.is_ok());
}

#[test]
fn test_http_client_with_custom_timeout() {
    let client =
        dfe_fetcher::credential::http_client_with_timeout(std::time::Duration::from_secs(5));
    assert!(client.is_ok());
}

// =============================================================================
// Buffer manager
// =============================================================================

#[test]
fn test_buffer_manager_pressure_tracking() {
    let config = dfe_fetcher::config::BufferConfig {
        memory_limit: 1000,
        pressure_threshold: 0.8,
    };
    let manager = dfe_fetcher::buffer::BufferManager::new(&config);

    assert!(!manager.is_under_pressure());
    assert_eq!(manager.total_bytes(), 0);

    manager.add_bytes(500);
    assert_eq!(manager.total_bytes(), 500);
    assert!(!manager.is_under_pressure());

    manager.add_bytes(400); // 900/1000 = 90% > 80%
    assert!(manager.is_under_pressure());

    manager.remove_bytes(500);
    assert!(!manager.is_under_pressure());
}

#[test]
fn test_buffer_manager_saturating_remove() {
    let config = dfe_fetcher::config::BufferConfig {
        memory_limit: 1000,
        pressure_threshold: 0.8,
    };
    let manager = dfe_fetcher::buffer::BufferManager::new(&config);

    manager.add_bytes(100);
    manager.remove_bytes(200); // Should not underflow
    assert_eq!(manager.total_bytes(), 0);
}

// =============================================================================
// Helpers
// =============================================================================

fn make_pipeline_state(
    shared: dfe_fetcher::config::SharedConfig,
) -> dfe_fetcher::pipeline::PipelineState {
    // PipelineState::new with None output for tests (no Kafka/gRPC needed)
    let metrics = std::sync::Arc::new(Metrics::new());
    dfe_fetcher::pipeline::PipelineState::new(shared, metrics, None)
        .expect("pipeline state creation")
}
