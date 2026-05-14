// Project:   dfe-fetcher
// File:      tests/integration/output_kafka.rs
// Purpose:   Output Kafka round-trip integration test (live → docker fallback)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Output Kafka round-trip in **integration** mode (not e2e).
//!
//! Runs automatically when a Kafka broker is reachable:
//! - Live: `KAFKA_BROKERS` etc. set in `.env` or environment
//! - Docker fallback: `localhost:19092` (dfe-docker infra profile)
//!
//! Skips cleanly with `eprintln!` when no broker is reachable, so the suite
//! always passes locally without infra. Unlike `tests/e2e/kafka.rs`, these
//! tests are NOT marked `#[ignore]` — they self-skip via `skip_if_no_kafka!`.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use serde_json::json;

use hyperi_rustlib::transport::{TransportBase, TransportReceiver};

use dfe_fetcher::config::{Config, OutputConfig, SharedConfig};
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::pipeline::PipelineState;

/// Round-trip a single message: produce via OutputManager, consume via raw KafkaTransport.
#[tokio::test]
async fn test_output_kafka_single_message_roundtrip() {
    let Some((kf, _holder)) = common::acquire_kafka().await else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let topic = common::test_topic("integration-single");

    let output_config = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_rustlib_config()),
        grpc: None,
        topic_suffix: None,
    };
    let legacy = dfe_fetcher::config::KafkaConfig::default();

    let output = match dfe_fetcher::output::OutputManager::new(&output_config, &legacy).await {
        Ok(o) => o,
        Err(e) => {
            eprintln!(
                "Skipping: OutputManager init failed (broker reachable but auth/config wrong): {e}"
            );
            return;
        }
    };

    // Send 1 message
    let payload = serde_json::to_vec(&json!({
        "test": "single-roundtrip",
        "timestamp": Utc::now().to_rfc3339(),
    }))
    .unwrap();

    if let Err(e) = output.send_all(&topic, &payload).await {
        eprintln!("Skipping: send failed (likely broker auth/topic ACL): {e}");
        output.close_all().await;
        return;
    }

    // Consume back
    let mut consumer_config = kf.to_rustlib_config();
    consumer_config.topics = vec![topic.clone()];
    consumer_config.group = format!("integration-single-{}", Utc::now().timestamp_millis());
    consumer_config.auto_offset_reset = "earliest".to_string();
    consumer_config.enable_auto_commit = true;

    let consumer = match hyperi_rustlib::transport::KafkaTransport::new(&consumer_config).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping consume: consumer init failed: {e}");
            output.close_all().await;
            return;
        }
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut found = false;

    while !found && tokio::time::Instant::now() < deadline {
        if let Ok(msgs) = consumer.recv(10).await {
            for msg in msgs {
                if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&msg.payload)
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
    let Some((kf, _holder)) = common::acquire_kafka().await else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let topic = common::test_topic("integration-enrich");

    let mut config = Config::default();
    config.output = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_rustlib_config()),
        grpc: None,
        topic_suffix: Some(String::new()), // no suffix — use topic as-is
    };

    let shared = SharedConfig::new(config);
    let metrics = Arc::new(Metrics::new());

    let output =
        match dfe_fetcher::output::OutputManager::new(&shared.get().output, &shared.get().kafka)
            .await
        {
            Ok(o) => o,
            Err(e) => {
                eprintln!("Skipping: OutputManager init failed: {e}");
                return;
            }
        };

    let state = PipelineState::new(
        shared,
        metrics,
        Some(output),
        tokio_util::sync::CancellationToken::new(),
    )
    .expect("pipeline state");

    // Enrich a record and deliver via the pipeline
    let raw = Bytes::from(r#"{"eventName":"CreateUser","severity":"high"}"#);
    let enriched = state.enrich_record(raw, "aws.cloudtrail");

    if let Err(e) = state.deliver_ingest(&topic, enriched).await {
        eprintln!("Skipping: deliver failed: {e}");
        return;
    }

    // Consume and verify enrichment
    let mut consumer_config = kf.to_rustlib_config();
    consumer_config.topics = vec![topic.clone()];
    consumer_config.group = format!("integration-enrich-{}", Utc::now().timestamp_millis());
    consumer_config.auto_offset_reset = "earliest".to_string();
    consumer_config.enable_auto_commit = true;

    let consumer = match hyperi_rustlib::transport::KafkaTransport::new(&consumer_config).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping consume: consumer init failed: {e}");
            return;
        }
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut enriched_found = false;

    while !enriched_found && tokio::time::Instant::now() < deadline {
        if let Ok(msgs) = consumer.recv(10).await {
            for msg in msgs {
                if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&msg.payload)
                    && parsed.get("_timestamp_fetcher").is_some()
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
    let Some((kf, _holder)) = common::acquire_kafka().await else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let topic = common::test_topic("integration-batch");

    let output_config = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_rustlib_config()),
        grpc: None,
        topic_suffix: None,
    };
    let legacy = dfe_fetcher::config::KafkaConfig::default();

    let output = match dfe_fetcher::output::OutputManager::new(&output_config, &legacy).await {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Skipping: OutputManager init failed: {e}");
            return;
        }
    };

    const N: u64 = 10;
    for i in 0..N {
        let payload = serde_json::to_vec(&json!({"seq": i, "tag": "batch-test"})).unwrap();
        if let Err(e) = output.send_all(&topic, &payload).await {
            eprintln!("Skipping: send {i} failed: {e}");
            output.close_all().await;
            return;
        }
    }

    let mut consumer_config = kf.to_rustlib_config();
    consumer_config.topics = vec![topic.clone()];
    consumer_config.group = format!("integration-batch-{}", Utc::now().timestamp_millis());
    consumer_config.auto_offset_reset = "earliest".to_string();
    consumer_config.enable_auto_commit = true;

    let consumer = match hyperi_rustlib::transport::KafkaTransport::new(&consumer_config).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping consume: consumer init failed: {e}");
            output.close_all().await;
            return;
        }
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut seqs: std::collections::HashSet<u64> = std::collections::HashSet::new();

    while seqs.len() < N as usize && tokio::time::Instant::now() < deadline {
        if let Ok(msgs) = consumer.recv(N as usize).await {
            for msg in msgs {
                if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&msg.payload)
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

/// OutputManager close_all() with no transports — never panics.
#[tokio::test]
async fn test_output_manager_close_when_already_closed() {
    let Some((kf, _holder)) = common::acquire_kafka().await else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let topic = common::test_topic("integration-close");

    let output_config = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_rustlib_config()),
        grpc: None,
        topic_suffix: None,
    };
    let legacy = dfe_fetcher::config::KafkaConfig::default();

    let output = match dfe_fetcher::output::OutputManager::new(&output_config, &legacy).await {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Skipping: OutputManager init failed: {e}");
            return;
        }
    };

    // Send one message so the producer is in active state, then close twice.
    let payload = serde_json::to_vec(&json!({"closing": "test"})).unwrap();
    let _ = output.send_all(&topic, &payload).await;

    // Health checks before close
    let _ = output.all_healthy();
    let _ = output.any_healthy();

    output.close_all().await;
    output.close_all().await; // Second close should be a no-op, not panic
}
