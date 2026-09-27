// Project:   dfe-fetcher
// File:      crates/fetcher/tests/e2e/kafka_cursor.rs
// Purpose:   Kafka integration tests for output transport
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests requiring a running Kafka broker.
//!
//! Supports dual-mode via `TEST_MODE` env var (see `tests/common/mod.rs`):
//! - `remote` (default) -- a remote Kafka cluster via `.env`
//! - `docker` -- dfe-docker infra profile (localhost:19092, no auth)
//!
//! Run with: `cargo test --test e2e -- --ignored`

use super::common;
use crate::skip_if_no_kafka;

use dfe_fetcher::config::OutputConfig;

/// Verify that OutputManager can send a message to Kafka without error.
#[tokio::test]
#[ignore = "requires Kafka (TEST_MODE=remote or docker)"]
async fn test_output_transport_kafka_send() {
    skip_if_no_kafka!();

    let kf = common::kafka_test_config();
    let topic = common::test_topic("output");

    let output_config = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_scalo_config()),
        grpc: None,
        topic_suffix: None,
        ..Default::default()
    };

    let legacy_kafka = dfe_fetcher::config::KafkaConfig::default();

    let output = dfe_fetcher::output::OutputManager::new(&output_config, &legacy_kafka)
        .await
        .expect("OutputManager creation should succeed");

    // Send a test message
    let payload = bytes::Bytes::from_static(br#"{"test": true, "source": "integration_test"}"#);
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
