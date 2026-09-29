// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/output_kafka.rs
// Purpose:   Output Kafka round-trip integration test (live -> docker fallback)
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

use crate::common::{self, ConsumeCeilings};

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use serde_json::{Value, json};

use dfe_fetcher::config::{Config, OutputConfig, SharedConfig};
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::pipeline::PipelineState;

/// The frames that parse as JSON records.
fn records(frames: &[Vec<u8>]) -> impl Iterator<Item = Value> + '_ {
    frames.iter().filter_map(|f| serde_json::from_slice(f).ok())
}

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
    output.close_all().await;

    // The read panics, naming the step, unless the record comes back.
    let group = format!("integration-single-{}", Utc::now().timestamp_millis());
    common::consume_until(
        &kf,
        &topic,
        &group,
        ConsumeCeilings::default(),
        "the single-roundtrip record",
        |frames| records(frames).any(|r| r["test"] == "single-roundtrip"),
    )
    .await;
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
        topic_suffix: Some(String::new()), // no suffix -- use topic as-is
        ..Default::default()
    };

    let shared = SharedConfig::new(config);
    let metrics = Arc::new(Metrics::new());

    let output = dfe_fetcher::output::OutputManager::new(&shared.get().output, &shared.get().kafka)
        .await
        .unwrap_or_else(|e| panic!("OutputManager init against {}: {e}", kf.brokers));

    let state = Arc::new(
        PipelineState::new(
            shared,
            Arc::clone(&metrics),
            Some(output),
            tokio_util::sync::CancellationToken::new(),
        )
        .expect("pipeline state"),
    );

    // Deliver via the extractor sink, which enriches on the way through. No
    // suffix, so the DFE source is the topic itself.
    let raw = Bytes::from(r#"{"eventName":"CreateUser","severity":"high"}"#);

    dfe_fetcher::extractor::ExtractorSink::new(state, metrics)
        .deliver(&topic, "aws.cloudtrail", &topic, raw)
        .await
        .unwrap_or_else(|e| panic!("pipeline deliver to {topic} on {}: {e}", kf.brokers));

    let is_enriched = |r: &Value| {
        r.get("_timestamp_fetcher").is_some()
            && r["_source"] == topic.as_str()
            && r["_source_fetcher"] == "aws.cloudtrail"
            && r["eventName"] == "CreateUser"
    };
    let group = format!("integration-enrich-{}", Utc::now().timestamp_millis());
    let frames = common::consume_until(
        &kf,
        &topic,
        &group,
        ConsumeCeilings::default(),
        "the record enriched with _timestamp_fetcher and _source_fetcher",
        |frames| records(frames).any(|r| is_enriched(&r)),
    )
    .await;

    let enriched = records(&frames)
        .find(is_enriched)
        .expect("the read returns only once the enriched record is in");
    assert!(enriched["_timestamp_fetcher"].is_number());
    assert!(enriched["_timestamp_received"].is_number());
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

    output.close_all().await;

    let seqs = |frames: &[Vec<u8>]| -> HashSet<u64> {
        records(frames)
            .filter(|r| r["tag"] == "batch-test")
            .filter_map(|r| r["seq"].as_u64())
            .collect()
    };
    let group = format!("integration-batch-{}", Utc::now().timestamp_millis());
    let frames = common::consume_until(
        &kf,
        &topic,
        &group,
        ConsumeCeilings::default(),
        &format!("{N} distinct batch-test records"),
        |frames| seqs(frames).len() >= N as usize,
    )
    .await;

    assert_eq!(
        seqs(&frames),
        (0..N).collect::<HashSet<u64>>(),
        "every sequence number arrives"
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
