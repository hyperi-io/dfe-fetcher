// Project:   dfe-fetcher
// File:      tests/e2e/kafka_cursor.rs
// Purpose:   Kafka integration tests for cursor store and output transport
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests requiring a running Kafka broker.
//!
//! Supports dual-mode via `TEST_MODE` env var (see `tests/common/mod.rs`):
//! - `remote` (default) — devex Kafka cluster via `.env`
//! - `docker` — dfe-docker infra profile (localhost:19092, no auth)
//!
//! Run with: `cargo test --test e2e -- --ignored`

use super::common;
use crate::skip_if_no_kafka;

use chrono::Utc;

use dfe_fetcher::config::{CursorConfig, OutputConfig};
use dfe_fetcher::cursor::kafka::KafkaCursorStore;
use dfe_fetcher::cursor::{CursorStore, CursorValue};

/// Verify the KafkaCursorStore can write, read, and delete cursors
/// through a real Kafka broker.
#[tokio::test]
#[ignore = "requires Kafka (TEST_MODE=remote or docker)"]
async fn test_cursor_store_kafka_roundtrip() {
    skip_if_no_kafka!();

    let kf = common::kafka_test_config();
    let kafka_config = kf.to_rustlib_config();
    let cursor_config = CursorConfig {
        kafka_topic: common::test_topic("cursor"),
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
#[tokio::test]
#[ignore = "requires Kafka (TEST_MODE=remote or docker)"]
async fn test_output_transport_kafka_send() {
    skip_if_no_kafka!();

    let kf = common::kafka_test_config();
    let topic = common::test_topic("output");

    let output_config = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_rustlib_config()),
        grpc: None,
        topic_suffix: None,
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
