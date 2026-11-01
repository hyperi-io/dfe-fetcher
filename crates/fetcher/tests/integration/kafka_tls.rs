// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/kafka_tls.rs
// Purpose:   The Kafka output over TLS against a real broker
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Kafka output over TLS against a real broker with a private CA: one
//! record through the `kafka:` block with `tls.ca_file` naming the CA, and one
//! with no CA named and `SSL_CERT_FILE` holding it, the way the chart
//! delivers a private CA. Each is read back through its own TLS consumer.

use std::time::{Duration, Instant};

use bytes::Bytes;
use dfe_fetcher::config::{KafkaConfig, KafkaTlsConfig, OutputConfig};
use dfe_fetcher::output::OutputManager;
use scalo::transport::kafka::test_support::{TlsBroker, require_docker, skip_or_fail};
use scalo::transport::kafka::{KafkaAdmin, KafkaTransport};
use scalo::transport::{TransportBase, TransportReceiver};

use crate::common;

/// How long one record may take to come back.
const ROUND_TRIP_WITHIN: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_kafka_output_trusts_a_named_ca_and_one_from_ssl_cert_file() {
    if !require_docker() {
        return;
    }
    let broker = match TlsBroker::start(&common::SUITE, scalo::test_name!()).await {
        Ok(broker) => broker,
        Err(e) => {
            skip_or_fail(&format!("the TLS broker would not start ({e})"));
            return;
        }
    };
    let ca = broker.pki.file("ca.crt");
    let reader = |group: &str, topic: &str| {
        let mut config = scalo::transport::KafkaConfig {
            brokers: vec![broker.tls.clone()],
            group: group.to_owned(),
            topics: vec![topic.to_owned()],
            auto_offset_reset: "earliest".to_owned(),
            ..scalo::transport::KafkaConfig::default()
        }
        .with_tls(Some(ca.as_str()));
        config.enable_auto_commit = true;
        config
    };
    let admin = KafkaAdmin::new(&reader("", "")).expect("admin client over TLS");

    for (case, ca_file) in [("named", Some(ca.clone())), ("ssl-cert-file", None)] {
        if ca_file.is_none() {
            // nextest runs one process per test, so the variable leaks nowhere.
            unsafe {
                std::env::set_var("SSL_CERT_FILE", &ca);
                std::env::remove_var("SSL_CERT_DIR");
            }
        }
        let topic = format!("tls-{case}");
        admin
            .create_topics(&[(topic.as_str(), 1, 1)])
            .await
            .unwrap_or_else(|e| panic!("{case}: create {topic}: {e}"));
        let legacy = KafkaConfig {
            brokers: vec![broker.tls.clone()],
            tls: KafkaTlsConfig {
                enabled: true,
                ca_file,
                ..KafkaTlsConfig::default()
            },
            ..KafkaConfig::default()
        };
        let output = OutputManager::new(&OutputConfig::default(), &legacy)
            .await
            .unwrap_or_else(|e| panic!("{case}: output over TLS: {e}"));
        let payload = format!("{{\"case\":\"{case}\"}}");
        let sent = output.send_all(&topic, Bytes::from(payload.clone())).await;
        output.close_all().await;
        sent.unwrap_or_else(|e| panic!("{case}: produce over TLS: {e}"));

        let consumer = KafkaTransport::new(&reader(&format!("tls-{case}"), &topic))
            .await
            .unwrap_or_else(|e| panic!("{case}: consumer over TLS: {e}"));
        let deadline = Instant::now() + ROUND_TRIP_WITHIN;
        let mut found = false;
        while !found && Instant::now() < deadline {
            let batch = consumer.recv(10).await.expect("recv");
            found = batch
                .records
                .iter()
                .any(|r| r.payload.as_ref() == payload.as_bytes());
        }
        let _ = consumer.close().await;
        assert!(
            found,
            "{case}: the record never came back within {ROUND_TRIP_WITHIN:?}"
        );
    }
}
