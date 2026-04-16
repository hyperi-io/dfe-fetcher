// Project:   dfe-fetcher
// File:      tests/integration/deployment.rs
// Purpose:   Deployment contract, output mapping, error types, cursor selection, metrics
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

use dfe_fetcher::config::Config;
use dfe_fetcher::metrics::Metrics;

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
// Instance ID derivation
// =============================================================================

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
            password: hyperi_rustlib::config::sensitive::SensitiveString::from("pass"),
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
    assert_eq!(
        rustlib
            .sasl_password
            .as_ref()
            .map(hyperi_rustlib::SensitiveString::expose),
        Some("pass")
    );
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
// Cursor store creation
// =============================================================================

/// Verify file cursor store can be created and used.
#[tokio::test]
async fn test_cursor_store_file_creation() {
    use dfe_fetcher::cursor::CursorStore;
    use dfe_fetcher::cursor::file::FileCursorStore;

    let tmp = tempfile::TempDir::new().unwrap();

    let store = FileCursorStore::new(tmp.path().to_str().unwrap());
    assert!(store.is_ok(), "file cursor store should initialise");

    let store = store.unwrap();
    let result = store.get("test.key").await;
    assert!(result.is_ok(), "get on empty store should succeed");
    assert!(
        result.unwrap().is_none(),
        "should return None for missing key"
    );
}

/// Verify file cursor store handles non-writable paths gracefully.
#[tokio::test]
async fn test_cursor_store_readonly_fallback() {
    use dfe_fetcher::cursor::file::FileCursorStore;

    let store = FileCursorStore::new("/proc/nonexistent/cursors");
    assert!(
        store.is_ok(),
        "should fall back to read-only mode, not error"
    );
}

// =============================================================================
// Deployment contract → test config derivation
// =============================================================================

use crate::common;

/// The test contract helper must reflect the live deployment contract.
/// If deployment.rs changes, these assertions catch the drift in tests
/// before it becomes a deploy-time surprise.
#[test]
fn test_app_test_contract_mirrors_deployment_contract() {
    let app = common::AppTestContract::from_app();

    assert_eq!(app.app_name, "dfe-fetcher");
    assert_eq!(app.env_prefix, "DFE_FETCHER");
    assert_eq!(app.metrics_port, 9090);
    assert_eq!(app.liveness_path, "/health/live");
    assert_eq!(app.readiness_path, "/health/ready");
    assert_eq!(app.metrics_path, "/metrics");
    assert_eq!(app.ingest_port, Some(8080));
    assert_eq!(app.vector_grpc_port, Some(6000));
    assert_eq!(app.config_mount_path, "/etc/dfe/fetcher.yaml");
}
