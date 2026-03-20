// Project:   dfe-fetcher
// File:      tests/kafka_integration.rs
// Purpose:   Kafka integration tests for cursor store and output transport
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests requiring a running Kafka broker.
//!
//! Supports two modes via `TEST_MODE` env var:
//!
//! - `remote` (default) — uses DevEx Kafka cluster via `KAFKA_*` env vars
//! - `docker` — uses local Docker Kafka (pending rustlib test infra)
//!
//! Run with: `cargo test --test kafka_integration -- --ignored`

#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::Utc;

use dfe_fetcher::config::{CursorConfig, OutputConfig};
use dfe_fetcher::cursor::kafka::KafkaCursorStore;
use dfe_fetcher::cursor::{CursorStore, CursorValue};

/// Build a KafkaConfig from env vars, respecting TEST_MODE.
///
/// - `remote`: reads `KAFKA_BROKERS`, `KAFKA_SASL_*`, `KAFKA_SECURITY_PROTOCOL`
/// - `docker`: uses `DOCKER_KAFKA_BROKERS` (default `localhost:9092`), plaintext
fn kafka_config_from_env() -> hyperi_rustlib::transport::KafkaConfig {
    // Load .env if present (won't override existing env vars)
    let _ = dotenvy::dotenv();

    let test_mode = std::env::var("TEST_MODE").unwrap_or_else(|_| "remote".to_string());

    if test_mode == "docker" {
        let brokers =
            std::env::var("DOCKER_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
        hyperi_rustlib::transport::KafkaConfig {
            brokers: brokers.split(',').map(|s| s.trim().to_string()).collect(),
            security_protocol: "plaintext".to_string(),
            client_id: "dfe-fetcher-test".to_string(),
            group: "dfe-fetcher-test".to_string(),
            ..Default::default()
        }
    } else {
        // Remote mode — read from KAFKA_* env vars
        let brokers =
            std::env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS required for TEST_MODE=remote");
        let mut config = hyperi_rustlib::transport::KafkaConfig {
            brokers: brokers.split(',').map(|s| s.trim().to_string()).collect(),
            client_id: "dfe-fetcher-test".to_string(),
            group: "dfe-fetcher-test".to_string(),
            ..Default::default()
        };

        if let Ok(protocol) = std::env::var("KAFKA_SECURITY_PROTOCOL") {
            config.security_protocol = protocol.to_lowercase();
        }
        if let Ok(mechanism) = std::env::var("KAFKA_SASL_MECHANISM") {
            config.sasl_mechanism = Some(mechanism);
        }
        if let Ok(user) = std::env::var("KAFKA_SASL_USER") {
            config.sasl_username = Some(user);
        }
        if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD") {
            config.sasl_password = Some(password);
        }

        config
    }
}

/// Test topic name with unique suffix to avoid collisions.
fn test_topic(base: &str) -> String {
    let prefix = std::env::var("TEST_TOPIC_PREFIX").unwrap_or_else(|_| "dfe-fetcher-test".into());
    format!("{prefix}-{base}-{}", Utc::now().timestamp_millis())
}

/// Verify the KafkaCursorStore can write, read, and delete cursors
/// through a real Kafka broker.
#[tokio::test]
#[ignore = "requires Kafka (TEST_MODE=remote or docker)"]
async fn test_cursor_store_kafka_roundtrip() {
    let kafka_config = kafka_config_from_env();
    let cursor_config = CursorConfig {
        kafka_topic: test_topic("cursor"),
        ..Default::default()
    };

    let store = KafkaCursorStore::new(&cursor_config, &kafka_config)
        .await
        .expect("KafkaCursorStore creation should succeed");

    let key = "test.roundtrip.cursor";

    // 1. No cursor exists initially (cache is empty for a fresh topic)
    let initial = store.get(key).await.unwrap();
    assert!(initial.is_none(), "cursor should not exist yet");

    // 2. Write a cursor value
    let cursor = CursorValue {
        cursor_key: key.to_string(),
        last_fetch_end: Utc::now(),
        last_fetch_records: 42,
        updated_at: Utc::now(),
        api_cursor: Some("page-token-abc".to_string()),
        version: 1,
    };
    store.set(key, &cursor).await.unwrap();

    // 3. Read it back (served from cache)
    let loaded = store.get(key).await.unwrap();
    assert!(loaded.is_some(), "cursor should exist after set");
    let loaded = loaded.unwrap();
    assert_eq!(loaded.cursor_key, key);
    assert_eq!(loaded.last_fetch_records, 42);
    assert_eq!(loaded.api_cursor.as_deref(), Some("page-token-abc"));

    // 4. Delete it
    store.delete(key).await.unwrap();

    // 5. Verify it is gone from cache
    let after_delete = store.get(key).await.unwrap();
    assert!(after_delete.is_none(), "cursor should be gone after delete");
}

/// Verify that OutputManager can send a message to Kafka without error.
/// This confirms the Kafka producer initialisation and basic send path work.
#[tokio::test]
#[ignore = "requires Kafka (TEST_MODE=remote or docker)"]
async fn test_output_transport_kafka_send() {
    let kafka_config = kafka_config_from_env();
    let topic = test_topic("output");

    let output_config = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kafka_config),
        grpc: None,
    };

    let legacy_kafka = dfe_fetcher::config::KafkaConfig::default();

    let output = dfe_fetcher::output::OutputManager::new(&output_config, &legacy_kafka)
        .await
        .expect("OutputManager creation should succeed");

    // Send a test message
    let payload = br#"{"test": true, "source": "integration_test"}"#;
    let result = output.send_all(&topic, payload).await;
    assert!(
        result.is_ok(),
        "send_all should succeed: {:?}",
        result.err()
    );

    // Verify transport reports healthy
    assert!(
        output.any_healthy(),
        "transport should be healthy after send"
    );

    // Graceful close
    output.close_all().await;
}
