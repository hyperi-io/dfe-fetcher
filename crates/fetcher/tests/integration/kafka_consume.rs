// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/kafka_consume.rs
// Purpose:   The shared Kafka read fails naming the step that ran out of time
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `common::consume_until` on its failure paths.
//!
//! Every Kafka round-trip test reads back through that helper, so a read that
//! returned short without failing would let each of them pass on nothing. These
//! pin that a shortfall panics, and that the panic names the step it was in:
//! the group join, or the frames.

use crate::common::{self, ConsumeCeilings, KafkaTestConfig};

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use futures::FutureExt;

/// The panic text of a read that must fail.
async fn failure_of(read: impl std::future::Future<Output = Vec<Vec<u8>>>) -> String {
    let payload = AssertUnwindSafe(read)
        .catch_unwind()
        .await
        .expect_err("a read that cannot meet its condition must fail, not return");
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .expect("the failure carries a message")
}

#[tokio::test]
async fn a_read_short_of_its_frames_names_the_frames_step() {
    let Some((kf, _holder)) = common::acquire_kafka("kafka-consume-short-read").await else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let topic = common::test_topic("consume-short");
    let output = dfe_fetcher::output::OutputManager::new(
        &dfe_fetcher::config::OutputConfig {
            output_type: "kafka".to_string(),
            kafka: Some(kf.to_scalo_config()),
            grpc: None,
            topic_suffix: None,
            ..Default::default()
        },
        &dfe_fetcher::config::KafkaConfig::default(),
    )
    .await
    .unwrap_or_else(|e| panic!("OutputManager init against {}: {e}", kf.brokers));
    for seq in 0..2 {
        output
            .send_all(&topic, Bytes::from(format!(r#"{{"seq":{seq}}}"#)))
            .await
            .unwrap_or_else(|e| panic!("produce {seq} to {topic}: {e}"));
    }
    output.close_all().await;

    let group = format!("consume-short-{}", Utc::now().timestamp_millis());
    let ceilings = ConsumeCeilings {
        frames: Duration::from_secs(5),
        ..ConsumeCeilings::default()
    };
    let message = failure_of(common::consume_until(
        &kf,
        &topic,
        &group,
        ceilings,
        "3 frames",
        |frames| frames.len() >= 3,
    ))
    .await;

    assert!(
        message.contains(&format!("was assigned `{topic}`")),
        "{message}"
    );
    assert!(
        message.contains("then 2 frame(s) arrived in 5s, still waiting for 3 frames"),
        "{message}"
    );
}

#[tokio::test]
async fn a_read_whose_group_never_joins_names_the_join_step() {
    // Nothing listens on port 1, so the group can never join.
    let kf = KafkaTestConfig {
        brokers: "127.0.0.1:1".to_string(),
        security_protocol: "PLAINTEXT".to_string(),
        sasl_mechanism: None,
        sasl_user: None,
        sasl_password: None,
    };
    let ceilings = ConsumeCeilings {
        assigned: Duration::from_secs(3),
        ..ConsumeCeilings::default()
    };
    let message = failure_of(common::consume_until(
        &kf,
        "consume-never-joins",
        "consume-never-joins",
        ceilings,
        "1 frame",
        |frames| !frames.is_empty(),
    ))
    .await;

    assert!(
        message.contains("was not assigned `consume-never-joins` within 3s"),
        "{message}"
    );
}
