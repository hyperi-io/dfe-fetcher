// Project:   dfe-fetcher
// File:      src/output.rs
// Purpose:   Output transport layer using rustlib Transport trait
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Output transport layer using rustlib Transport trait.
//!
//! Replaces the custom Sink trait with rustlib's unified Transport.
//! Supports Kafka, gRPC, or both simultaneously via [`OutputManager`].
//!
//! All Kafka access via rustlib's `KafkaTransport` — no direct rdkafka dependency.

use hyperi_rustlib::transport::{
    GrpcTransport, KafkaConfig as RustlibKafkaConfig, KafkaTransport, SendResult, Transport,
};
use tracing::{debug, error, info};

use crate::config::{KafkaConfig as LegacyKafkaConfig, OutputConfig};
use crate::error::{Error, Result};

/// Wrapper enum for transport backends.
///
/// Needed because [`Transport`] has an associated `Token` type, which prevents
/// dynamic dispatch via `dyn Transport`. Each variant delegates to the concrete
/// transport implementation.
pub enum OutputTransport {
    /// Kafka transport (rustlib).
    Kafka(KafkaTransport),
    /// gRPC transport (rustlib).
    Grpc(GrpcTransport),
}

impl OutputTransport {
    /// Send a message through this transport.
    async fn send(&self, key: &str, payload: &[u8]) -> Result<()> {
        let result = match self {
            Self::Kafka(t) => t.send(key, payload).await,
            Self::Grpc(t) => t.send(key, payload).await,
        };

        match result {
            SendResult::Ok => Ok(()),
            SendResult::Backpressured => Err(Error::Transport("transport backpressured".into())),
            SendResult::Fatal(e) => Err(Error::Transport(format!("transport fatal: {e}"))),
        }
    }

    /// Check if this transport is healthy.
    fn is_healthy(&self) -> bool {
        match self {
            Self::Kafka(t) => t.is_healthy(),
            Self::Grpc(t) => t.is_healthy(),
        }
    }

    /// Close this transport gracefully.
    async fn close(&self) -> Result<()> {
        let result = match self {
            Self::Kafka(t) => t.close().await,
            Self::Grpc(t) => t.close().await,
        };
        result.map_err(|e| Error::Transport(format!("close failed: {e}")))
    }

    /// Transport name for logging.
    fn name(&self) -> &'static str {
        match self {
            Self::Kafka(t) => t.name(),
            Self::Grpc(t) => t.name(),
        }
    }
}

/// Manages one or more output transports for delivering pipeline data.
///
/// Created from [`OutputConfig`] (with legacy [`KafkaConfig`] fallback).
/// Sends to all configured transports simultaneously.
pub struct OutputManager {
    transports: Vec<OutputTransport>,
}

impl OutputManager {
    /// Create a new output manager from configuration.
    ///
    /// If `output.kafka` is set, uses that. Otherwise falls back to the
    /// legacy top-level `kafka` section by building a rustlib `KafkaConfig`
    /// from the legacy fields.
    pub async fn new(output: &OutputConfig, legacy_kafka: &LegacyKafkaConfig) -> Result<Self> {
        let mut transports = Vec::new();

        if output.includes_kafka() {
            let kafka_config = if let Some(ref cfg) = output.kafka {
                cfg.clone()
            } else {
                build_rustlib_kafka_config(legacy_kafka)
            };

            let transport = KafkaTransport::new(&kafka_config)
                .await
                .map_err(|e| Error::Transport(format!("kafka init failed: {e}")))?;

            info!(brokers = ?kafka_config.brokers, "Kafka output transport initialised");
            transports.push(OutputTransport::Kafka(transport));
        }

        if output.includes_grpc() {
            let grpc_config = output.grpc.as_ref().ok_or_else(|| {
                Error::Config("grpc config required when output.type includes grpc".into())
            })?;

            let transport = GrpcTransport::new(grpc_config)
                .await
                .map_err(|e| Error::Transport(format!("grpc init failed: {e}")))?;

            info!("gRPC output transport initialised");
            transports.push(OutputTransport::Grpc(transport));
        }

        if transports.is_empty() {
            return Err(Error::Config("no output transports configured".into()));
        }

        debug!(count = transports.len(), "Output manager ready");
        Ok(Self { transports })
    }

    /// Send a message to all configured transports.
    ///
    /// Attempts delivery to every transport even if one fails, so that a
    /// Kafka failure does not prevent gRPC from receiving the message (and
    /// vice versa). Returns the first error encountered for DLQ routing.
    pub async fn send_all(&self, key: &str, payload: &[u8]) -> Result<()> {
        let mut first_error: Option<Error> = None;

        for transport in &self.transports {
            if let Err(e) = transport.send(key, payload).await {
                {
                    use std::sync::atomic::{AtomicU64, Ordering};
                    static SEND_ERROR_SAMPLES: AtomicU64 = AtomicU64::new(0);
                    if hyperi_rustlib::logger::log_sampled(&SEND_ERROR_SAMPLES, 1000) {
                        error!(
                            transport = transport.name(),
                            error = %e,
                            total = SEND_ERROR_SAMPLES.load(Ordering::Relaxed),
                            "Output transport send failed (sampled 1/1000)"
                        );
                    }
                }
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Check if all transports are healthy.
    pub fn all_healthy(&self) -> bool {
        self.transports.iter().all(OutputTransport::is_healthy)
    }

    /// Check if any transport is healthy.
    pub fn any_healthy(&self) -> bool {
        self.transports.iter().any(OutputTransport::is_healthy)
    }

    /// Close all transports gracefully.
    pub async fn close_all(&self) {
        for transport in &self.transports {
            if let Err(e) = transport.close().await {
                error!(
                    transport = transport.name(),
                    error = %e,
                    "Failed to close output transport"
                );
            }
        }
    }
}

/// Build a rustlib [`KafkaConfig`](RustlibKafkaConfig) from the legacy
/// fetcher-specific [`KafkaConfig`](LegacyKafkaConfig) section.
pub(crate) fn build_rustlib_kafka_config(legacy: &LegacyKafkaConfig) -> RustlibKafkaConfig {
    let mut config = RustlibKafkaConfig {
        brokers: legacy.brokers.clone(),
        client_id: legacy.client_id.clone(),
        ..RustlibKafkaConfig::default()
    };

    // Map SASL settings
    if let Some(ref sasl) = legacy.sasl {
        if sasl.enabled {
            config.sasl_mechanism = Some(sasl.mechanism.clone());
            config.sasl_username = Some(sasl.username.clone());
            config.sasl_password = Some(sasl.password.clone());

            config.security_protocol = if legacy.tls.enabled {
                "sasl_ssl".to_string()
            } else {
                "sasl_plaintext".to_string()
            };
        }
    } else if legacy.tls.enabled {
        config.security_protocol = "ssl".to_string();
    }

    // Map TLS settings
    if legacy.tls.enabled {
        config.ssl_ca_location.clone_from(&legacy.tls.ca_file);
        config
            .ssl_certificate_location
            .clone_from(&legacy.tls.cert_file);
        config.ssl_key_location.clone_from(&legacy.tls.key_file);
    }

    // Map producer settings as librdkafka overrides
    config.librdkafka_overrides.insert(
        "batch.size".to_string(),
        legacy.producer.batch_size.to_string(),
    );
    config.librdkafka_overrides.insert(
        "batch.num.messages".to_string(),
        legacy.producer.batch_messages.to_string(),
    );
    config.librdkafka_overrides.insert(
        "linger.ms".to_string(),
        legacy.producer.linger_ms.to_string(),
    );
    config.librdkafka_overrides.insert(
        "compression.type".to_string(),
        legacy.producer.compression.clone(),
    );
    config
        .librdkafka_overrides
        .insert("acks".to_string(), legacy.producer.acks.clone());
    config.librdkafka_overrides.insert(
        "message.send.max.retries".to_string(),
        legacy.producer.retries.to_string(),
    );

    config
}
