// Project:   dfe-fetcher
// File:      src/sink/kafka/mod.rs
// Purpose:   Kafka producer sink
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka producer sink for delivering fetched data to the pipeline.

use async_trait::async_trait;
use bytes::Bytes;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::Duration;
use tracing::{debug, error};

use crate::config::KafkaConfig;
use crate::error::{Error, Result};
use crate::sink::Sink;

/// Kafka producer sink.
pub struct KafkaSink {
    producer: FutureProducer,
}

impl KafkaSink {
    /// Create a new Kafka sink from configuration.
    pub fn new(config: &KafkaConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", config.brokers.join(","))
            .set("client.id", &config.client_id)
            .set("message.timeout.ms", "30000")
            .set("batch.size", config.producer.batch_size.to_string())
            .set(
                "batch.num.messages",
                config.producer.batch_messages.to_string(),
            )
            .set("linger.ms", config.producer.linger_ms.to_string())
            .set("compression.type", &config.producer.compression)
            .set("acks", &config.producer.acks)
            .set(
                "message.send.max.retries",
                config.producer.retries.to_string(),
            );

        // SASL configuration
        if let Some(ref sasl) = config.sasl {
            if sasl.enabled {
                let protocol = if config.tls.enabled {
                    "SASL_SSL"
                } else {
                    "SASL_PLAINTEXT"
                };
                client_config
                    .set("security.protocol", protocol)
                    .set("sasl.mechanism", &sasl.mechanism)
                    .set("sasl.username", &sasl.username)
                    .set("sasl.password", &sasl.password);
            }
        } else if config.tls.enabled {
            client_config.set("security.protocol", "SSL");
        }

        // TLS configuration
        if config.tls.enabled {
            if let Some(ref ca) = config.tls.ca_file {
                client_config.set("ssl.ca.location", ca);
            }
            if let Some(ref cert) = config.tls.cert_file {
                client_config.set("ssl.certificate.location", cert);
            }
            if let Some(ref key) = config.tls.key_file {
                client_config.set("ssl.key.location", key);
            }
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(Error::Kafka)?;

        debug!(
            brokers = config.brokers.join(","),
            "Kafka sink initialised"
        );

        Ok(Self { producer })
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn send(&self, topic: &str, payload: Bytes) -> Result<()> {
        let record: FutureRecord<'_, str, [u8]> =
            FutureRecord::to(topic).payload(payload.as_ref());

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => Ok(()),
            Err((err, _)) => {
                error!(topic = topic, error = %err, "Failed to send to Kafka");
                Err(Error::Kafka(err))
            }
        }
    }

    async fn flush(&self) -> Result<()> {
        self.producer.flush(Duration::from_secs(30)).map_err(|e| {
            error!(error = %e, "Failed to flush Kafka producer");
            Error::Kafka(e)
        })
    }

    fn is_healthy(&self) -> bool {
        true
    }
}
