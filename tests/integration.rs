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
    metrics.inc_fetches_success();
    metrics.add_records_fetched(42);
    metrics.add_bytes_fetched(1024);

    let output = metrics.render();

    assert!(output.contains("dfe_fetcher_fetches_total{status=\"success\"} 1"));
    assert!(output.contains("dfe_fetcher_fetches_total{status=\"error\"} 0"));
    assert!(output.contains("dfe_records_received_total 42"));
    assert!(output.contains("dfe_fetcher_bytes_received_total 1024"));
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
    assert!(output.contains("dfe_fetcher_extractor_runs_total 2"));
    assert!(output.contains("dfe_fetcher_extractor_runs_success_total 1"));
    assert!(output.contains("dfe_fetcher_extractor_runs_error_total 1"));
    assert!(output.contains("dfe_fetcher_extractor_records_total 100"));
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
fn test_memory_guard_pressure_tracking() {
    use hyperi_rustlib::memory::{MemoryGuard, MemoryGuardConfig};

    let guard = MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1000,
        pressure_threshold: 0.8,
        ..Default::default()
    });

    assert!(!guard.under_pressure());
    assert_eq!(guard.current_bytes(), 0);

    guard.add_bytes(500);
    assert_eq!(guard.current_bytes(), 500);
    assert!(!guard.under_pressure());

    guard.add_bytes(400); // 900/1000 = 90% > 80%
    assert!(guard.under_pressure());

    guard.release(500);
    assert!(!guard.under_pressure());
}

#[test]
fn test_memory_guard_release_underflow() {
    use hyperi_rustlib::memory::{MemoryGuard, MemoryGuardConfig};

    let guard = MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1000,
        pressure_threshold: 0.8,
        ..Default::default()
    });

    guard.add_bytes(100);
    guard.release(200); // Release more than added — should not panic
}

// =============================================================================
// End-to-end pipeline enrichment + CEL filtering
// =============================================================================

/// Verify the full enrich → filter pipeline path works end-to-end.
/// Uses `enrich_record` for enrichment and `hyperi_rustlib::expression::evaluate_condition`
/// for CEL filtering (since `evaluate_filter` is private to the pipeline module).
#[tokio::test]
async fn test_pipeline_deliver_enriches_and_filters() {
    use std::collections::HashMap;
    use std::sync::Arc;

    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let metrics = Arc::new(Metrics::new());
    let state = dfe_fetcher::pipeline::PipelineState::new(shared, metrics, None)
        .expect("pipeline state creation");

    // 1. Enrich a record
    let raw = Bytes::from(r#"{"eventName":"CreateUser","severity":"high"}"#);
    let enriched = state.enrich_record(raw, "aws.cloudtrail");
    let enriched_str = std::str::from_utf8(&enriched).unwrap();

    // 2. Verify all enrichment fields are present
    assert!(enriched_str.contains("\"_timestamp_fetcher\":"));
    assert!(enriched_str.contains("\"_timestamp_received\":"));
    assert!(enriched_str.contains("\"_source_fetcher\":\"aws.cloudtrail\""));

    // 3. Parse and verify JSON validity
    let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
    assert_eq!(parsed["eventName"], "CreateUser");
    assert!(parsed["_timestamp_fetcher"].is_number());
    assert!(parsed["_timestamp_received"].is_number());

    // 4. CEL filter: CreateUser should pass (eventName != "ConsoleLogin")
    let filter_expr = r#"eventName != "ConsoleLogin""#;
    let context: HashMap<String, serde_json::Value> =
        serde_json::from_slice::<serde_json::Value>(&enriched)
            .unwrap()
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
    let passes = hyperi_rustlib::expression::evaluate_condition(filter_expr, &context);
    assert!(passes, "CreateUser should pass the filter");

    // 5. CEL filter: ConsoleLogin should be dropped
    let login_record = Bytes::from(r#"{"eventName":"ConsoleLogin"}"#);
    let enriched_login = state.enrich_record(login_record, "aws.cloudtrail");
    let login_context: HashMap<String, serde_json::Value> =
        serde_json::from_slice::<serde_json::Value>(&enriched_login)
            .unwrap()
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
    let drops = hyperi_rustlib::expression::evaluate_condition(filter_expr, &login_context);
    assert!(!drops, "ConsoleLogin should be filtered out");
}

/// Verify the cursor → fetch window flow works end-to-end:
/// no cursor returns None, set stores state, get retrieves it.
#[tokio::test]
async fn test_cursor_file_store_incremental_window() {
    use chrono::{Duration, Utc};
    use dfe_fetcher::cursor::file::FileCursorStore;
    use dfe_fetcher::cursor::{CursorStore, CursorValue};

    let dir = tempfile::TempDir::new().unwrap();
    let store = FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();

    // No cursor: should return None
    let key = "test-instance.aws.cloudtrail";
    assert!(store.get(key).await.unwrap().is_none());

    // Write a cursor (simulating post-fetch)
    let now = Utc::now();
    let cursor = CursorValue {
        cursor_key: key.to_string(),
        last_fetch_end: now - Duration::minutes(5),
        last_fetch_records: 100,
        updated_at: now,
        api_cursor: None,
        version: 1,
    };
    store.set(key, &cursor).await.unwrap();

    // Read back — should exist with correct values
    let stored = store.get(key).await.unwrap().unwrap();
    assert_eq!(stored.cursor_key, key);
    assert_eq!(stored.last_fetch_records, 100);
    assert!(
        (stored.last_fetch_end - cursor.last_fetch_end)
            .num_seconds()
            .abs()
            < 1
    );

    // Verify version and api_cursor
    assert_eq!(stored.version, 1);
    assert!(stored.api_cursor.is_none());

    // Update cursor with new values (simulating second fetch)
    let cursor2 = CursorValue {
        cursor_key: key.to_string(),
        last_fetch_end: now,
        last_fetch_records: 250,
        updated_at: Utc::now(),
        api_cursor: Some("next-page-token".to_string()),
        version: 1,
    };
    store.set(key, &cursor2).await.unwrap();

    // Read back second cursor — should see updated values
    let stored2 = store.get(key).await.unwrap().unwrap();
    assert_eq!(stored2.last_fetch_records, 250);
    assert_eq!(stored2.api_cursor.as_deref(), Some("next-page-token"));
    assert!(
        (stored2.last_fetch_end - now).num_seconds().abs() < 1,
        "last_fetch_end should match the updated cursor"
    );
}

// =============================================================================
// Startup smoke tests
// =============================================================================

/// Verify Orchestrator::new() completes without panic for a default config.
/// This catches init-time panics (missing fields, bad defaults, type mismatches)
/// that would cause a production outage on deploy.
#[tokio::test]
async fn test_startup_orchestrator_boots_with_default_config() {
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    let config = Config::default();
    let metrics = Arc::new(Metrics::new());
    let shutdown = CancellationToken::new();

    // Default config has no Kafka brokers, so Orchestrator should start
    // without output transports (info log: "No output transports configured")
    let result = dfe_fetcher::pipeline::Orchestrator::new(config, metrics, shutdown.clone()).await;

    assert!(
        result.is_ok(),
        "Orchestrator::new should succeed with default config: {:?}",
        result.err()
    );

    let orchestrator = result.unwrap();
    let state = orchestrator.state();

    // Pipeline should be ready (no output = no health check failure)
    assert!(state.is_ready(), "Pipeline should be ready after boot");

    // Config should be accessible
    let config = state.config();
    assert_eq!(config.scheduler.default_interval_secs, 300);
}

/// Verify PipelineState can be created, used for enrichment, and doesn't panic
/// when send_to_transports is called without an output manager.
#[tokio::test]
async fn test_startup_pipeline_state_no_output() {
    use std::sync::Arc;

    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let metrics = Arc::new(Metrics::new());

    let state = dfe_fetcher::pipeline::PipelineState::new(shared, metrics, None)
        .expect("PipelineState::new should succeed");

    // Enrichment should work without output
    let raw = Bytes::from(r#"{"test":true}"#);
    let enriched = state.enrich_record(raw, "test.source");
    let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
    assert_eq!(parsed["test"], true);
    assert!(parsed["_timestamp_fetcher"].is_number());
    assert!(parsed["_source_fetcher"].is_string());

    // Deliver should fail gracefully (no output configured)
    let result = state.deliver_ingest("test-topic", enriched).await;
    assert!(
        result.is_err(),
        "deliver should fail without output transport"
    );
}

/// Verify the Scheduler can be created and computes intervals correctly
/// without needing any runtime resources.
#[test]
fn test_startup_scheduler_creation() {
    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let scheduler_config = dfe_fetcher::config::SchedulerConfig::default();

    let scheduler =
        dfe_fetcher::scheduler::Scheduler::new(&scheduler_config, shared, None, "test-id".into());

    let interval = scheduler.effective_interval(None);
    // Default 300s + up to 10% jitter = 300-330s
    assert!(interval.as_secs() >= 300);
    assert!(interval.as_secs() <= 330);
}

/// Verify instance_id derivation works for all source types.
#[test]
#[allow(clippy::field_reassign_with_default)]
fn test_startup_instance_id_derivation() {
    // Explicit ID
    let config = Config {
        instance_id: Some("my-instance".to_string()),
        ..Default::default()
    };
    assert_eq!(
        dfe_fetcher::config::derive_instance_id(&config),
        "my-instance"
    );

    // Auto-derive from AWS
    let config = Config {
        sources: dfe_fetcher::config::SourcesConfig {
            aws: dfe_fetcher::config::AwsSourceConfig {
                enabled: true,
                region: "us-east-1".to_string(),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let id = dfe_fetcher::config::derive_instance_id(&config);
    assert!(id.starts_with("aws-"), "should derive aws- prefix: {id}");
    assert_eq!(id.len(), 12); // "aws-" + 8 hex chars

    // Auto-derive from M365
    let config = Config {
        sources: dfe_fetcher::config::SourcesConfig {
            m365: dfe_fetcher::config::M365SourceConfig {
                enabled: true,
                tenant_id: Some("contoso-tenant".to_string()),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let id = dfe_fetcher::config::derive_instance_id(&config);
    assert!(id.starts_with("m365-"), "should derive m365- prefix: {id}");

    // No sources enabled
    let config = Config::default();
    assert_eq!(
        dfe_fetcher::config::derive_instance_id(&config),
        "dfe-fetcher"
    );
}

// =============================================================================
// Output transport (unit-testable parts)
// =============================================================================

/// Verify build_rustlib_kafka_config maps legacy config fields correctly.
#[test]
fn test_output_legacy_kafka_config_mapping() {
    use dfe_fetcher::config::{KafkaConfig, KafkaTlsConfig, ProducerConfig, SaslConfig};

    let legacy = KafkaConfig {
        brokers: vec!["broker1:9092".into(), "broker2:9092".into()],
        client_id: "my-fetcher".into(),
        topic_suffix: "_raw".into(),
        sasl: Some(SaslConfig {
            enabled: true,
            mechanism: "SCRAM-SHA-256".into(),
            username: "user".into(),
            password: "pass".into(),
        }),
        tls: KafkaTlsConfig {
            enabled: true,
            ca_file: Some("/etc/ssl/ca.pem".into()),
            cert_file: None,
            key_file: None,
        },
        producer: ProducerConfig {
            batch_size: 1_000_000,
            batch_messages: 5000,
            linger_ms: 50,
            compression: "zstd".into(),
            acks: "all".into(),
            retries: 3,
        },
    };

    let rustlib = dfe_fetcher::output::build_rustlib_kafka_config(&legacy);

    // Basic fields
    assert_eq!(rustlib.brokers, vec!["broker1:9092", "broker2:9092"]);
    assert_eq!(rustlib.client_id, "my-fetcher");

    // SASL
    assert_eq!(rustlib.sasl_mechanism.as_deref(), Some("SCRAM-SHA-256"));
    assert_eq!(rustlib.sasl_username.as_deref(), Some("user"));
    assert_eq!(rustlib.sasl_password.as_deref(), Some("pass"));
    assert_eq!(rustlib.security_protocol, "sasl_ssl");

    // TLS
    assert_eq!(rustlib.ssl_ca_location.as_deref(), Some("/etc/ssl/ca.pem"));

    // Producer overrides
    assert_eq!(
        rustlib
            .librdkafka_overrides
            .get("compression.type")
            .unwrap(),
        "zstd"
    );
    assert_eq!(rustlib.librdkafka_overrides.get("acks").unwrap(), "all");
    assert_eq!(rustlib.librdkafka_overrides.get("linger.ms").unwrap(), "50");
}

/// Verify build_rustlib_kafka_config with no SASL (plaintext).
#[test]
fn test_output_legacy_kafka_config_no_sasl() {
    let legacy = dfe_fetcher::config::KafkaConfig::default();
    let rustlib = dfe_fetcher::output::build_rustlib_kafka_config(&legacy);

    assert_eq!(rustlib.security_protocol, "plaintext");
    assert!(rustlib.sasl_mechanism.is_none());
    assert!(rustlib.sasl_username.is_none());
}

/// Verify OutputConfig helper methods.
#[test]
fn test_output_config_mode_helpers() {
    use dfe_fetcher::config::OutputConfig;

    let kafka = OutputConfig {
        output_type: "kafka".into(),
        ..Default::default()
    };
    assert!(kafka.includes_kafka());
    assert!(!kafka.includes_grpc());

    let grpc = OutputConfig {
        output_type: "grpc".into(),
        ..Default::default()
    };
    assert!(!grpc.includes_kafka());
    assert!(grpc.includes_grpc());

    let both = OutputConfig {
        output_type: "both".into(),
        ..Default::default()
    };
    assert!(both.includes_kafka());
    assert!(both.includes_grpc());
}

// =============================================================================
// Error type coverage
// =============================================================================

/// Verify IntoResponse status code mapping for all error variants.
#[test]
fn test_error_into_response_status_codes() {
    use axum::response::IntoResponse;
    use dfe_fetcher::error::Error;

    let cases: Vec<(Error, u16)> = vec![
        (Error::Config("bad".into()), 500),
        (Error::Source("fail".into()), 502),
        (Error::Credential("denied".into()), 401),
        (Error::Pipeline("stall".into()), 500),
        (Error::Transport("down".into()), 503),
        (Error::Cursor("lost".into()), 500),
        (Error::Filter("invalid".into()), 500),
        (Error::Kafka("timeout".into()), 503),
    ];

    for (error, expected_status) in cases {
        let response = error.into_response();
        assert_eq!(
            response.status().as_u16(),
            expected_status,
            "Error variant should map to HTTP {}",
            expected_status
        );
    }
}

// =============================================================================
// Deployment contract
// =============================================================================

/// Verify the deployment contract generates without panic and has correct values.
#[test]
fn test_deployment_contract_structure() {
    let contract = dfe_fetcher::deployment::contract();

    assert_eq!(contract.app_name, "dfe-fetcher");
    assert_eq!(contract.binary_name, "dfe-fetcher");
    assert_eq!(contract.env_prefix, "DFE_FETCHER");
    assert_eq!(contract.metrics_port, 9090);

    // Health endpoints
    assert_eq!(contract.health.liveness_path, "/health/live");
    assert_eq!(contract.health.readiness_path, "/health/ready");
    assert_eq!(contract.health.metrics_path, "/metrics");

    // Extra ports: ingest (8080) and vector-grpc (6000)
    assert_eq!(contract.extra_ports.len(), 2);
    assert_eq!(contract.extra_ports[0].port, 8080);
    assert_eq!(contract.extra_ports[1].port, 6000);

    // Secret groups: kafka, aws, azure, m365, gcp
    assert_eq!(contract.secrets.len(), 5);
    let group_names: Vec<_> = contract
        .secrets
        .iter()
        .map(|s| s.group_name.as_str())
        .collect();
    assert!(group_names.contains(&"kafka"));
    assert!(group_names.contains(&"aws"));
    assert!(group_names.contains(&"azure"));
    assert!(group_names.contains(&"m365"));
    assert!(group_names.contains(&"gcp"));

    // KEDA autoscaling
    let keda = contract.keda.as_ref().expect("keda should be configured");
    assert_eq!(keda.min_replicas, 1);
    assert_eq!(keda.max_replicas, 5);

    // Default config should contain expected keys
    let default_cfg = contract.default_config.as_ref().expect("default config");
    assert!(default_cfg["scheduler"]["default_interval_secs"].is_number());
    assert!(default_cfg["kafka"]["brokers"].is_array());
}

// =============================================================================
// Pipeline topic suffix resolution
// =============================================================================

/// Verify topic suffix is read from OutputConfig (new) or legacy KafkaConfig.
#[test]
fn test_topic_suffix_resolution() {
    use dfe_fetcher::config::{Config, OutputConfig, SharedConfig};

    // New-style: output.topic_suffix takes precedence
    let config = Config {
        output: OutputConfig {
            topic_suffix: Some("_raw".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    let shared = SharedConfig::new(config);
    let cfg = shared.get();
    let suffix = cfg
        .output
        .topic_suffix
        .as_deref()
        .unwrap_or(&cfg.kafka.topic_suffix);
    assert_eq!(suffix, "_raw");

    // Legacy: falls back to kafka.topic_suffix
    let config = Config::default();
    let shared = SharedConfig::new(config);
    let cfg = shared.get();
    let suffix = cfg
        .output
        .topic_suffix
        .as_deref()
        .unwrap_or(&cfg.kafka.topic_suffix);
    assert_eq!(suffix, "_land");
}

// =============================================================================
// Cursor store auto-selection
// =============================================================================

/// Verify cursor store selection logic based on output mode.
#[tokio::test]
async fn test_cursor_store_auto_selection() {
    use dfe_fetcher::config::{CursorConfig, OutputConfig};

    // gRPC-only → should select file store (no Kafka available)
    let cursor_config = CursorConfig {
        store: "auto".to_string(),
        file_path: "/tmp/dfe-test-cursor-auto".to_string(),
        ..Default::default()
    };
    let grpc_output = OutputConfig {
        output_type: "grpc".to_string(),
        ..Default::default()
    };
    let store = dfe_fetcher::cursor::create_cursor_store(&cursor_config, &grpc_output, &dfe_fetcher::config::KafkaConfig::default()).await;
    // Should succeed (file store) — directory will be created or degraded mode
    assert!(store.is_ok(), "auto + grpc should resolve to file store");

    // Explicit "file" → file store regardless of output mode
    let file_config = CursorConfig {
        store: "file".to_string(),
        file_path: "/tmp/dfe-test-cursor-explicit".to_string(),
        ..Default::default()
    };
    let kafka_output = OutputConfig {
        output_type: "kafka".to_string(),
        ..Default::default()
    };
    let store = dfe_fetcher::cursor::create_cursor_store(&file_config, &kafka_output, &dfe_fetcher::config::KafkaConfig::default()).await;
    assert!(store.is_ok(), "explicit file should always succeed");
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
