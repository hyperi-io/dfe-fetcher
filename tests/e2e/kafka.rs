// Project:   dfe-fetcher
// File:      tests/e2e/kafka.rs
// Purpose:   End-to-end tests exercising real Kafka produce + consume
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! End-to-end Kafka tests — produce via OutputManager, consume back, verify.
//!
//! These tests exercise the full transport path through a real Kafka broker.
//! Run with: `TEST_MODE=docker cargo test --test e2e -- --ignored`

use super::common;
use crate::skip_if_no_kafka;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use serde_json::json;

use hyperi_rustlib::transport::Transport;

use dfe_fetcher::config::{Config, CursorConfig, OutputConfig, SharedConfig};
use dfe_fetcher::cursor::{CursorStore, CursorValue};
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::pipeline::PipelineState;
use dfe_fetcher::source::FetchWindow;

/// Full pipeline round-trip: enrich a record, produce to Kafka, consume it back.
#[tokio::test]
#[ignore = "requires Kafka (TEST_MODE=remote or docker)"]
async fn test_e2e_produce_consume_roundtrip() {
    skip_if_no_kafka!();

    let kf = common::kafka_test_config();
    let topic = common::test_topic("e2e-roundtrip");

    // Create OutputManager pointed at real Kafka
    let output_config = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_rustlib_config()),
        grpc: None,
        topic_suffix: None,
    };
    let legacy_kafka = dfe_fetcher::config::KafkaConfig::default();
    let output = dfe_fetcher::output::OutputManager::new(&output_config, &legacy_kafka)
        .await
        .expect("OutputManager should connect to Kafka");

    // Send 5 messages
    for i in 0..5 {
        let payload = serde_json::to_vec(&json!({
            "event_id": i,
            "source": "e2e_test",
            "timestamp": Utc::now().to_rfc3339(),
        }))
        .unwrap();

        output
            .send_all(&topic, &payload)
            .await
            .unwrap_or_else(|e| panic!("send {i} failed: {e}"));
    }

    // Consume the messages back using a rustlib KafkaTransport in consumer mode
    let mut consumer_config = kf.to_rustlib_config();
    consumer_config.topics = vec![topic.clone()];
    consumer_config.group = format!("e2e-consumer-{}", Utc::now().timestamp_millis());
    consumer_config.auto_offset_reset = "earliest".to_string();
    consumer_config.enable_auto_commit = true;

    let consumer = hyperi_rustlib::transport::KafkaTransport::new(&consumer_config)
        .await
        .expect("consumer should connect");

    // Poll for messages (up to 10 seconds)
    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while received.len() < 5 && tokio::time::Instant::now() < deadline {
        match consumer.recv(100).await {
            Ok(msgs) => {
                for msg in msgs {
                    received.push(msg.payload.clone());
                }
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    assert_eq!(
        received.len(),
        5,
        "should receive all 5 messages, got {}",
        received.len()
    );

    // Verify message content
    for payload in &received {
        let parsed: serde_json::Value = serde_json::from_slice(payload).unwrap();
        assert_eq!(parsed["source"], "e2e_test");
        assert!(parsed["event_id"].is_number());
    }

    output.close_all().await;
    let _ = consumer.close().await;
}

/// Pipeline enrichment -> Kafka delivery: verify _timestamp_fetcher and _source_fetcher
/// are present in the message received from Kafka.
#[tokio::test]
#[ignore = "requires Kafka (TEST_MODE=remote or docker)"]
async fn test_e2e_enriched_record_in_kafka() {
    skip_if_no_kafka!();

    let kf = common::kafka_test_config();
    let topic = common::test_topic("e2e-enriched");

    // Create a PipelineState with real Kafka output
    let config = Config {
        output: OutputConfig {
            output_type: "kafka".to_string(),
            kafka: Some(kf.to_rustlib_config()),
            grpc: None,
            topic_suffix: Some(String::new()), // no suffix — use topic as-is
        },
        ..Default::default()
    };

    let shared = SharedConfig::new(config);
    let metrics = Arc::new(Metrics::new());

    let output = dfe_fetcher::output::OutputManager::new(&shared.get().output, &shared.get().kafka)
        .await
        .expect("output should connect");

    let state = PipelineState::new(shared, metrics, Some(output)).expect("pipeline state");

    // Deliver a raw record through the pipeline (enrichment happens here)
    let raw = Bytes::from(r#"{"eventName":"CreateUser","severity":"high"}"#);
    let enriched = state.enrich_record(raw, "aws.cloudtrail");
    state
        .deliver_ingest(&topic, enriched)
        .await
        .expect("deliver should succeed");

    // Consume and verify enrichment fields
    let mut consumer_config = kf.to_rustlib_config();
    consumer_config.topics = vec![topic.clone()];
    consumer_config.group = format!("e2e-enriched-{}", Utc::now().timestamp_millis());
    consumer_config.auto_offset_reset = "earliest".to_string();
    consumer_config.enable_auto_commit = true;

    let consumer = hyperi_rustlib::transport::KafkaTransport::new(&consumer_config)
        .await
        .expect("consumer should connect");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut found = false;

    while !found && tokio::time::Instant::now() < deadline {
        if let Ok(msgs) = consumer.recv(10).await {
            for msg in msgs {
                let parsed: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
                if parsed.get("_timestamp_fetcher").is_some() {
                    assert!(parsed["_timestamp_fetcher"].is_number());
                    assert!(parsed["_timestamp_received"].is_number());
                    assert_eq!(parsed["_source_fetcher"], "aws.cloudtrail");
                    assert_eq!(parsed["eventName"], "CreateUser");
                    found = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(found, "should find enriched record in Kafka");
    let _ = consumer.close().await;
}

/// Cursor store -> FetchWindow integration: write cursor, verify next window starts
/// from cursor.last_fetch_end.
#[tokio::test]
#[ignore = "requires Kafka (TEST_MODE=remote or docker)"]
async fn test_e2e_cursor_drives_fetch_window() {
    skip_if_no_kafka!();

    let kf = common::kafka_test_config();

    // Use Kafka cursor store
    let cursor_config = CursorConfig {
        kafka_topic: common::test_topic("e2e-cursor"),
        ..Default::default()
    };

    let store =
        dfe_fetcher::cursor::kafka::KafkaCursorStore::new(&cursor_config, &kf.to_rustlib_config())
            .await
            .expect("cursor store should initialise");

    let key = "e2e-instance.aws.cloudtrail";

    // No cursor -> default window (scheduler would use default_window_hours)
    assert!(store.get(key).await.unwrap().is_none());

    // Simulate a successful fetch: write cursor at a known time
    let fetch_end = Utc::now() - chrono::Duration::minutes(10);
    let cursor = CursorValue {
        cursor_key: key.to_string(),
        last_fetch_end: fetch_end,
        last_fetch_records: 50,
        updated_at: Utc::now(),
        api_cursor: None,
        version: 1,
    };
    store.set(key, &cursor).await.unwrap();

    // Read cursor back and build a FetchWindow (simulating what the scheduler does)
    let stored = store.get(key).await.unwrap().expect("cursor should exist");
    let window = FetchWindow {
        start: stored.last_fetch_end,
        end: Utc::now(),
    };

    // Window should start from the cursor's last_fetch_end
    assert!(
        (window.start - fetch_end).num_seconds().abs() < 2,
        "window.start should match cursor.last_fetch_end"
    );
    assert!(
        window.end > window.start,
        "window.end should be after window.start"
    );

    // Simulate second fetch: advance cursor
    let cursor2 = CursorValue {
        cursor_key: key.to_string(),
        last_fetch_end: window.end,
        last_fetch_records: 30,
        updated_at: Utc::now(),
        api_cursor: None,
        version: 1,
    };
    store.set(key, &cursor2).await.unwrap();

    let stored2 = store.get(key).await.unwrap().expect("cursor should exist");
    assert_eq!(stored2.last_fetch_records, 30);
    assert!(
        (stored2.last_fetch_end - window.end).num_seconds().abs() < 2,
        "cursor should advance to window.end"
    );
}
