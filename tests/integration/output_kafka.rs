// Project:   dfe-fetcher
// File:      tests/integration/output_kafka.rs
// Purpose:   Output Kafka round-trip integration test (live → docker fallback)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Output Kafka round-trip in **integration** mode (not e2e).
//!
//! Runs automatically when a Kafka broker is reachable:
//! - Live: `KAFKA_BROKERS` etc. set in `.env` or environment
//! - Docker fallback: `localhost:19092` (dfe-docker infra profile)
//!
//! Skips cleanly with `eprintln!` when NO broker path exists at all -- no live
//! broker and no container runtime -- so the suite still passes on a laptop
//! without infra. `common::require_kafka_path_in_ci` turns that same case into
//! a hard failure under CI, where a container runtime is promised.
//!
//! Once `acquire_kafka()` has returned a broker, every subsequent failure fails
//! the test. A transport that will not initialise, a produce that times out and
//! a consumer that cannot subscribe are all defects in what these tests cover,
//! not environment gaps, so none of them may be downgraded to a skip.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use serde_json::json;

use scalo::transport::{TransportBase, TransportReceiver};

use dfe_fetcher::config::{Config, OutputConfig, SharedConfig};
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::pipeline::PipelineState;

/// Round-trip a single message: produce via OutputManager, consume via raw KafkaTransport.
#[tokio::test]
async fn test_output_kafka_single_message_roundtrip() {
    let Some((kf, _holder)) = common::acquire_kafka("output-kafka-single-message-roundtrip").await
    else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let topic = common::test_topic("integration-single");

    let output_config = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_scalo_config()),
        grpc: None,
        topic_suffix: None,
        ..Default::default()
    };
    let legacy = dfe_fetcher::config::KafkaConfig::default();

    let output = dfe_fetcher::output::OutputManager::new(&output_config, &legacy)
        .await
        .unwrap_or_else(|e| panic!("OutputManager init against {}: {e}", kf.brokers));

    // Send 1 message
    let payload = serde_json::to_vec(&json!({
        "test": "single-roundtrip",
        "timestamp": Utc::now().to_rfc3339(),
    }))
    .unwrap();

    if let Err(e) = output.send_all(&topic, Bytes::from(payload)).await {
        output.close_all().await;
        panic!("produce to {topic} on {}: {e}", kf.brokers);
    }

    // Consume back
    let mut consumer_config = kf.to_scalo_config();
    consumer_config.topics = vec![topic.clone()];
    consumer_config.group = format!("integration-single-{}", Utc::now().timestamp_millis());
    consumer_config.auto_offset_reset = "earliest".to_string();
    consumer_config.enable_auto_commit = true;

    let consumer = match scalo::transport::KafkaTransport::new(&consumer_config).await {
        Ok(c) => c,
        Err(e) => {
            output.close_all().await;
            panic!("consumer init against {}: {e}", kf.brokers);
        }
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut found = false;

    while !found && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = consumer.recv(10).await {
            for record in batch.records {
                if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&record.payload)
                    && parsed["test"] == "single-roundtrip"
                {
                    found = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    output.close_all().await;
    let _ = consumer.close().await;

    assert!(found, "should receive the produced message back from Kafka");
}

/// Pipeline-level round-trip: enrichment fields appear in delivered payload.
#[tokio::test]
async fn test_output_kafka_pipeline_enrichment_roundtrip() {
    let Some((kf, _holder)) =
        common::acquire_kafka("output-kafka-pipeline-enrichment-roundtrip").await
    else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let topic = common::test_topic("integration-enrich");

    let mut config = Config::default();
    config.output = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_scalo_config()),
        grpc: None,
        topic_suffix: Some(String::new()), // no suffix — use topic as-is
        ..Default::default()
    };

    let shared = SharedConfig::new(config);
    let metrics = Arc::new(Metrics::new());

    let output = dfe_fetcher::output::OutputManager::new(&shared.get().output, &shared.get().kafka)
        .await
        .unwrap_or_else(|e| panic!("OutputManager init against {}: {e}", kf.brokers));

    let state = PipelineState::new(
        shared,
        metrics,
        Some(output),
        tokio_util::sync::CancellationToken::new(),
    )
    .expect("pipeline state");

    // Deliver via the pipeline, which enriches on the way through. No suffix,
    // so the DFE source is the topic itself.
    let raw = Bytes::from(r#"{"eventName":"CreateUser","severity":"high"}"#);

    state
        .deliver_ingest(&topic, "aws.cloudtrail", &topic, raw)
        .await
        .unwrap_or_else(|e| panic!("pipeline deliver to {topic} on {}: {e}", kf.brokers));

    // Consume and verify enrichment
    let mut consumer_config = kf.to_scalo_config();
    consumer_config.topics = vec![topic.clone()];
    consumer_config.group = format!("integration-enrich-{}", Utc::now().timestamp_millis());
    consumer_config.auto_offset_reset = "earliest".to_string();
    consumer_config.enable_auto_commit = true;

    let consumer = scalo::transport::KafkaTransport::new(&consumer_config)
        .await
        .unwrap_or_else(|e| panic!("consumer init against {}: {e}", kf.brokers));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut enriched_found = false;

    while !enriched_found && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = consumer.recv(10).await {
            for record in batch.records {
                if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&record.payload)
                    && parsed.get("_timestamp_fetcher").is_some()
                    && parsed["_source"] == topic.as_str()
                    && parsed["_source_fetcher"] == "aws.cloudtrail"
                    && parsed["eventName"] == "CreateUser"
                {
                    assert!(parsed["_timestamp_fetcher"].is_number());
                    assert!(parsed["_timestamp_received"].is_number());
                    enriched_found = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let _ = consumer.close().await;

    assert!(
        enriched_found,
        "enriched record with _timestamp_fetcher and _source_fetcher must arrive at Kafka"
    );
}

/// Multiple messages: produce N, consume N, all distinct.
#[tokio::test]
async fn test_output_kafka_batch_roundtrip() {
    let Some((kf, _holder)) = common::acquire_kafka("output-kafka-batch-roundtrip").await else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let topic = common::test_topic("integration-batch");

    let output_config = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_scalo_config()),
        grpc: None,
        topic_suffix: None,
        ..Default::default()
    };
    let legacy = dfe_fetcher::config::KafkaConfig::default();

    let output = dfe_fetcher::output::OutputManager::new(&output_config, &legacy)
        .await
        .unwrap_or_else(|e| panic!("OutputManager init against {}: {e}", kf.brokers));

    const N: u64 = 10;
    for i in 0..N {
        let payload = serde_json::to_vec(&json!({"seq": i, "tag": "batch-test"})).unwrap();
        if let Err(e) = output.send_all(&topic, Bytes::from(payload)).await {
            output.close_all().await;
            panic!("produce message {i} to {topic} on {}: {e}", kf.brokers);
        }
    }

    let mut consumer_config = kf.to_scalo_config();
    consumer_config.topics = vec![topic.clone()];
    consumer_config.group = format!("integration-batch-{}", Utc::now().timestamp_millis());
    consumer_config.auto_offset_reset = "earliest".to_string();
    consumer_config.enable_auto_commit = true;

    let consumer = match scalo::transport::KafkaTransport::new(&consumer_config).await {
        Ok(c) => c,
        Err(e) => {
            output.close_all().await;
            panic!("consumer init against {}: {e}", kf.brokers);
        }
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut seqs: std::collections::HashSet<u64> = std::collections::HashSet::new();

    while seqs.len() < N as usize && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = consumer.recv(N as usize).await {
            for record in batch.records {
                if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&record.payload)
                    && parsed["tag"] == "batch-test"
                    && let Some(seq) = parsed["seq"].as_u64()
                {
                    seqs.insert(seq);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    output.close_all().await;
    let _ = consumer.close().await;

    assert_eq!(
        seqs.len(),
        N as usize,
        "should receive all {N} distinct messages, got {}",
        seqs.len()
    );
}

/// `close_all()` is idempotent: closing an active transport twice must not
/// panic. The transport is asserted active first, because a "does not panic"
/// test over a transport that never connected proves nothing.
#[tokio::test]
async fn test_output_manager_close_when_already_closed() {
    let Some((kf, _holder)) =
        common::acquire_kafka("output-manager-close-when-already-closed").await
    else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let topic = common::test_topic("integration-close");

    let output_config = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_scalo_config()),
        grpc: None,
        topic_suffix: None,
        ..Default::default()
    };
    let legacy = dfe_fetcher::config::KafkaConfig::default();

    let output = dfe_fetcher::output::OutputManager::new(&output_config, &legacy)
        .await
        .unwrap_or_else(|e| panic!("OutputManager init against {}: {e}", kf.brokers));

    // Send one message so the producer is in active state, then close twice.
    let payload = serde_json::to_vec(&json!({"closing": "test"})).unwrap();
    if let Err(e) = output.send_all(&topic, Bytes::from(payload)).await {
        output.close_all().await;
        panic!("produce to {topic} on {}: {e}", kf.brokers);
    }

    // The double-close below only means something over a live transport.
    assert!(
        output.any_healthy(),
        "transport must be healthy after a successful produce, else the \
         double-close below closes nothing"
    );

    output.close_all().await;
    output.close_all().await; // Second close should be a no-op, not panic
}
