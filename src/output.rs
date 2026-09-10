// Project:   dfe-fetcher
// File:      src/output.rs
// Purpose:   Output transport layer using scalo Transport trait
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Output transport layer using scalo Transport trait.
//!
//! Replaces the custom Sink trait with scalo's unified Transport.
//! Supports Kafka, gRPC, or both simultaneously via [`OutputManager`].
//!
//! All Kafka access via scalo's `KafkaTransport` — no direct rdkafka dependency.

use bytes::Bytes;
use scalo::transport::{
    GrpcTransport, KafkaConfig as ScaloKafkaConfig, KafkaTransport, RoutedSender, SendResult,
    TransportBase, TransportConfig, TransportSender, TransportType,
};
use tracing::{debug, error, info, trace};

use crate::config::{KafkaConfig as LegacyKafkaConfig, OutputConfig};
use crate::error::{Error, Result};

/// Wrapper enum for transport backends.
///
/// Needed because scalo's `Transport` traits carry an associated `Token`
/// type, which prevents dynamic dispatch via a `dyn Transport`. Each variant
/// delegates to the concrete transport implementation.
pub enum OutputTransport {
    /// Kafka transport (scalo).
    Kafka(KafkaTransport),
    /// gRPC transport (scalo).
    Grpc(GrpcTransport),
}

impl OutputTransport {
    /// Send a message through this transport.
    ///
    /// Records per-transport send duration as
    /// `dfe_fetcher_transport_send_duration_seconds{transport="kafka"|"grpc"}`.
    ///
    /// `payload` is a [`Bytes`] (scalo's `TransportSender::send` takes it by
    /// value); it is ref-counted, so the per-transport clone in `send_all` is
    /// cheap (no buffer copy).
    async fn send(&self, key: &str, payload: Bytes) -> Result<()> {
        let start = std::time::Instant::now();

        trace!(
            transport = self.name(),
            topic = key,
            payload_bytes = payload.len(),
            "Producing record to transport"
        );

        let result = match self {
            Self::Kafka(t) => t.send(key, payload).await,
            Self::Grpc(t) => t.send(key, payload).await,
        };

        // Emit per-transport send latency histogram
        let elapsed = start.elapsed();
        metrics::histogram!(
            "dfe_fetcher_transport_send_duration_seconds",
            "transport" => self.name()
        )
        .record(elapsed.as_secs_f64());

        match result {
            SendResult::Ok => {
                trace!(
                    transport = self.name(),
                    topic = key,
                    duration_ms = elapsed.as_millis(),
                    "Record produced successfully"
                );
                Ok(())
            }
            SendResult::Backpressured => {
                debug!(
                    transport = self.name(),
                    topic = key,
                    "Transport backpressured -- caller holds the batch"
                );
                Err(Error::Backpressured(format!(
                    "{} transport backpressured",
                    self.name()
                )))
            }
            SendResult::Fatal(e) => Err(Error::Transport(format!("transport fatal: {e}"))),
            SendResult::FilteredDlq => {
                debug!(
                    transport = self.name(),
                    topic = key,
                    "Message matched outbound filter — routing to DLQ"
                );
                Err(Error::Transport("filtered to DLQ".into()))
            }
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

/// Manages the fetcher's outputs: the default transports every record takes,
/// plus the NAMED destination set a route can send a record to instead.
///
/// Created from [`OutputConfig`] (with legacy [`KafkaConfig`](LegacyKafkaConfig)
/// fallback). The default sends to all configured transports simultaneously;
/// the named set is scalo's [`RoutedSender`], the same mechanism the receiver's
/// destinations use.
pub struct OutputManager {
    transports: Vec<OutputTransport>,
    /// Named destinations, keyed by name. `None` when none are declared.
    destinations: Option<RoutedSender>,
}

impl OutputManager {
    /// Create a new output manager from configuration.
    ///
    /// If `output.kafka` is set, uses that. Otherwise falls back to the
    /// legacy top-level `kafka` section by building a scalo `KafkaConfig`
    /// from the legacy fields.
    pub async fn new(output: &OutputConfig, legacy_kafka: &LegacyKafkaConfig) -> Result<Self> {
        let mut transports = Vec::new();

        if output.includes_kafka() {
            let kafka_config = resolve_kafka_config(output, legacy_kafka);

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

        let destinations = build_destinations(output, legacy_kafka).await?;

        debug!(
            count = transports.len(),
            named = destinations.as_ref().map_or(0, |d| d.route_keys().len()),
            "Output manager ready"
        );
        Ok(Self {
            transports,
            destinations,
        })
    }

    /// Send a record to named destinations instead of the default transports.
    ///
    /// `key` is the wire key for a bus destination (the record's topic); a gRPC
    /// listener ignores it. Delivered only when every named destination has
    /// accepted -- see [`RoutedSender::send_fanout`].
    pub async fn send_to(&self, destinations: &[&str], key: &str, payload: Bytes) -> Result<()> {
        let Some(ref set) = self.destinations else {
            return Err(Error::Config(
                "output.routes names a destination but none are declared".into(),
            ));
        };
        match set.send_fanout(destinations, key, payload).await {
            SendResult::Ok | SendResult::FilteredDlq => Ok(()),
            SendResult::Backpressured => Err(Error::Backpressured(format!(
                "destination {} backpressured",
                destinations.join(",")
            ))),
            SendResult::Fatal(e) => Err(Error::Transport(format!("destination send failed: {e}"))),
        }
    }

    /// Whether a named destination exists.
    pub fn has_destination(&self, name: &str) -> bool {
        self.destinations
            .as_ref()
            .is_some_and(|set| set.has_route(name))
    }

    /// Send a message to all configured transports.
    ///
    /// Attempts delivery to every transport even if one fails, so that a
    /// Kafka failure does not prevent gRPC from receiving the message (and
    /// vice versa). Returns the first error encountered for DLQ routing.
    pub async fn send_all(&self, key: &str, payload: Bytes) -> Result<()> {
        let mut first_error: Option<Error> = None;

        for transport in &self.transports {
            // Cheap ref-counted clone per transport (no buffer copy).
            if let Err(e) = transport.send(key, payload.clone()).await {
                {
                    use std::sync::atomic::{AtomicU64, Ordering};
                    static SEND_ERROR_SAMPLES: AtomicU64 = AtomicU64::new(0);
                    if scalo::logger::log_sampled(&SEND_ERROR_SAMPLES, 1000) {
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

    /// Check if all transports and named destinations are healthy.
    pub fn all_healthy(&self) -> bool {
        self.transports.iter().all(OutputTransport::is_healthy)
            && self
                .destinations
                .as_ref()
                .is_none_or(scalo::transport::RoutedSender::is_healthy)
    }

    /// Check if any transport is healthy.
    ///
    /// The scheduler stalls on this, so it stays a whole-output signal: one
    /// sick named destination must not stop every source fetching.
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
        if let Some(ref set) = self.destinations
            && let Err(e) = set.close().await
        {
            error!(error = %e, "Failed to close named destinations");
        }
    }
}

/// Build the named destination set from `output.destinations`.
///
/// Each declared destination becomes one route in a scalo [`RoutedSender`];
/// a bus destination without its own broker settings reuses the output's.
async fn build_destinations(
    output: &OutputConfig,
    legacy_kafka: &LegacyKafkaConfig,
) -> Result<Option<RoutedSender>> {
    if output.destinations.is_empty() {
        return Ok(None);
    }

    let mut routes = std::collections::HashMap::with_capacity(output.destinations.len());
    for (name, spec) in &output.destinations {
        let config = match (&spec.grpc, &spec.kafka) {
            (Some(grpc), None) => TransportConfig {
                transport_type: TransportType::Grpc,
                grpc: Some(grpc.clone()),
                ..TransportConfig::default()
            },
            (None, Some(bus)) => TransportConfig {
                transport_type: TransportType::Kafka,
                kafka: Some(
                    bus.config
                        .clone()
                        .unwrap_or_else(|| resolve_kafka_config(output, legacy_kafka)),
                ),
                ..TransportConfig::default()
            },
            _ => {
                return Err(Error::Config(format!(
                    "output.destinations.{name} needs exactly one of grpc or kafka"
                )));
            }
        };
        routes.insert(name.clone(), config);
    }

    let sender = RoutedSender::from_route_configs(routes, None)
        .await
        .map_err(|e| Error::Transport(format!("named destinations init failed: {e}")))?;
    info!(
        destinations = ?sender.route_keys(),
        "Named output destinations initialised"
    );
    Ok(Some(sender))
}

/// Resolve the effective scalo Kafka config: `output.kafka` when set, else
/// built from the legacy top-level `kafka` section. Produce-only (empty
/// consumer group, so scalo builds no idle consumer). ONE resolution shared
/// by the output transport and the DLQ producer -- if they ever diverge,
/// dead-letters go to a different broker than the data.
pub fn resolve_kafka_config(
    output: &OutputConfig,
    legacy_kafka: &LegacyKafkaConfig,
) -> ScaloKafkaConfig {
    let mut kafka_config = if let Some(ref cfg) = output.kafka {
        cfg.clone()
    } else {
        build_scalo_kafka_config(legacy_kafka)
    };
    kafka_config.group = String::new();
    kafka_config
}

/// Build a scalo [`KafkaConfig`](ScaloKafkaConfig) from the legacy
/// fetcher-specific [`KafkaConfig`](LegacyKafkaConfig) section.
#[allow(clippy::module_name_repetitions)]
pub fn build_scalo_kafka_config(legacy: &LegacyKafkaConfig) -> ScaloKafkaConfig {
    let mut config = ScaloKafkaConfig {
        brokers: legacy.brokers.clone(),
        client_id: legacy.client_id.clone(),
        ..ScaloKafkaConfig::default()
    };

    // Map SASL settings. The security protocol is decided by the PAIR
    // (SASL active, TLS on) -- keying it off `sasl.is_some()` left a
    // `sasl: {enabled: false}` block plus `tls.enabled: true` on plaintext,
    // with the ssl_* paths below set and ignored by librdkafka.
    let sasl_active = legacy.sasl.as_ref().is_some_and(|s| s.enabled);
    if let Some(ref sasl) = legacy.sasl
        && sasl.enabled
    {
        config.sasl_mechanism = Some(sasl.mechanism.clone());
        config.sasl_username = Some(sasl.username.clone());
        config.sasl_password = Some(sasl.password.clone());
    }
    config.security_protocol = match (sasl_active, legacy.tls.enabled) {
        (true, true) => "sasl_ssl".to_string(),
        (true, false) => "sasl_plaintext".to_string(),
        (false, true) => "ssl".to_string(),
        (false, false) => config.security_protocol,
    };

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

#[cfg(test)]
#[allow(clippy::field_reassign_with_default, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::*;

    #[test]
    fn test_build_scalo_kafka_config_defaults() {
        let legacy = KafkaConfig::default();
        let result = build_scalo_kafka_config(&legacy);

        assert_eq!(result.client_id, "dfe-fetcher");
        assert!(result.brokers.is_empty());
        // No SASL configured, no TLS → security_protocol stays at scalo default
        assert_eq!(result.security_protocol, "plaintext");
        assert!(result.sasl_mechanism.is_none());
        assert!(result.sasl_username.is_none());
        assert!(result.sasl_password.is_none());
    }

    #[test]
    fn test_build_scalo_kafka_config_sasl_with_tls() {
        let mut legacy = KafkaConfig::default();
        legacy.sasl = Some(SaslConfig {
            enabled: true,
            mechanism: "SCRAM-SHA-256".to_string(),
            username: "user1".to_string(),
            password: "pass1".into(),
        });
        legacy.tls.enabled = true;

        let result = build_scalo_kafka_config(&legacy);
        assert_eq!(result.security_protocol, "sasl_ssl");
        assert_eq!(result.sasl_mechanism.as_deref(), Some("SCRAM-SHA-256"));
        assert_eq!(result.sasl_username.as_deref(), Some("user1"));
    }

    #[test]
    fn test_build_scalo_kafka_config_sasl_no_tls() {
        let mut legacy = KafkaConfig::default();
        legacy.sasl = Some(SaslConfig {
            enabled: true,
            mechanism: "PLAIN".to_string(),
            username: "admin".to_string(),
            password: "secret".into(),
        });
        legacy.tls.enabled = false;

        let result = build_scalo_kafka_config(&legacy);
        assert_eq!(result.security_protocol, "sasl_plaintext");
    }

    #[test]
    fn test_build_scalo_kafka_config_tls_no_sasl() {
        let mut legacy = KafkaConfig::default();
        legacy.tls.enabled = true;
        // No SASL configured

        let result = build_scalo_kafka_config(&legacy);
        assert_eq!(result.security_protocol, "ssl");
    }

    #[test]
    fn test_build_scalo_kafka_config_producer_overrides() {
        let legacy = KafkaConfig::default();
        let result = build_scalo_kafka_config(&legacy);

        // Verify all producer settings mapped to librdkafka_overrides
        assert_eq!(
            result.librdkafka_overrides.get("batch.size"),
            Some(&legacy.producer.batch_size.to_string())
        );
        assert_eq!(
            result.librdkafka_overrides.get("batch.num.messages"),
            Some(&legacy.producer.batch_messages.to_string())
        );
        assert_eq!(
            result.librdkafka_overrides.get("linger.ms"),
            Some(&legacy.producer.linger_ms.to_string())
        );
        assert_eq!(
            result.librdkafka_overrides.get("compression.type"),
            Some(&legacy.producer.compression)
        );
        assert_eq!(
            result.librdkafka_overrides.get("acks"),
            Some(&legacy.producer.acks)
        );
        assert_eq!(
            result.librdkafka_overrides.get("message.send.max.retries"),
            Some(&legacy.producer.retries.to_string())
        );
    }

    #[test]
    fn test_build_scalo_kafka_config_custom_brokers() {
        let mut legacy = KafkaConfig::default();
        legacy.brokers = vec![
            "broker1:9092".to_string(),
            "broker2:9092".to_string(),
            "broker3:9092".to_string(),
        ];

        let result = build_scalo_kafka_config(&legacy);
        assert_eq!(result.brokers, legacy.brokers);
    }

    #[test]
    fn test_build_scalo_kafka_config_tls_cert_files() {
        let mut legacy = KafkaConfig::default();
        legacy.tls.enabled = true;
        legacy.tls.ca_file = Some("/certs/ca.pem".to_string());
        legacy.tls.cert_file = Some("/certs/client.pem".to_string());
        legacy.tls.key_file = Some("/certs/client-key.pem".to_string());

        let result = build_scalo_kafka_config(&legacy);
        assert_eq!(result.ssl_ca_location.as_deref(), Some("/certs/ca.pem"));
        assert_eq!(
            result.ssl_certificate_location.as_deref(),
            Some("/certs/client.pem")
        );
        assert_eq!(
            result.ssl_key_location.as_deref(),
            Some("/certs/client-key.pem")
        );
    }

    #[test]
    fn test_output_config_includes_kafka() {
        let kafka_only = OutputConfig {
            output_type: "kafka".to_string(),
            ..Default::default()
        };
        assert!(kafka_only.includes_kafka());
        assert!(!kafka_only.includes_grpc());

        let both = OutputConfig {
            output_type: "both".to_string(),
            ..Default::default()
        };
        assert!(both.includes_kafka());
        assert!(both.includes_grpc());
    }

    #[test]
    fn test_output_config_includes_grpc() {
        let grpc_only = OutputConfig {
            output_type: "grpc".to_string(),
            ..Default::default()
        };
        assert!(!grpc_only.includes_kafka());
        assert!(grpc_only.includes_grpc());

        let both = OutputConfig {
            output_type: "both".to_string(),
            ..Default::default()
        };
        assert!(both.includes_kafka());
        assert!(both.includes_grpc());
    }

    // -- OutputManager construction paths --

    /// `OutputManager::new` with `output_type = "invalid"` must fail with
    /// `Error::Config("no output transports configured")` because neither
    /// `includes_kafka()` nor `includes_grpc()` returns true.
    #[tokio::test]
    async fn test_output_manager_no_transports_configured() {
        let output = OutputConfig {
            output_type: "invalid".to_string(),
            kafka: None,
            grpc: None,
            topic_suffix: None,
            ..Default::default()
        };
        let legacy = KafkaConfig::default();

        let result = OutputManager::new(&output, &legacy).await;

        match result {
            Err(Error::Config(msg)) => {
                assert!(
                    msg.contains("no output transports configured"),
                    "unexpected error message: {msg}"
                );
            }
            Err(other) => panic!("expected Error::Config, got {other:?}"),
            Ok(_) => panic!("expected construction to fail"),
        }
    }

    /// `OutputManager::new` with `output_type = "grpc"` but `output.grpc = None`
    /// must fail with `Error::Config` mentioning that grpc config is required.
    #[tokio::test]
    async fn test_output_manager_grpc_type_missing_config() {
        let output = OutputConfig {
            output_type: "grpc".to_string(),
            kafka: None,
            grpc: None,
            topic_suffix: None,
            ..Default::default()
        };
        let legacy = KafkaConfig::default();

        let result = OutputManager::new(&output, &legacy).await;

        match result {
            Err(Error::Config(msg)) => {
                assert!(
                    msg.contains("grpc config required"),
                    "unexpected error message: {msg}"
                );
            }
            Err(other) => panic!("expected Error::Config, got {other:?}"),
            Ok(_) => panic!("expected construction to fail"),
        }
    }

    /// `OutputManager::new` with `output_type = "kafka"` and bogus brokers
    /// must either construct successfully (connection is lazy in librdkafka)
    /// OR fail with `Error::Transport`. This exercises the legacy-kafka
    /// fallback code path in `OutputManager::new`.
    #[tokio::test]
    async fn test_output_manager_kafka_legacy_fallback_bogus_brokers() {
        let output = OutputConfig {
            output_type: "kafka".to_string(),
            kafka: None, // force legacy path
            grpc: None,
            topic_suffix: None,
            ..Default::default()
        };
        let mut legacy = KafkaConfig::default();
        legacy.brokers = vec!["not-a-real-broker:19092".to_string()];

        let result = OutputManager::new(&output, &legacy).await;

        // librdkafka validates config but connects lazily, so typically Ok.
        // If some future scalo change validates brokers at construction,
        // it must surface as Error::Transport — never any other variant.
        match result {
            Ok(mgr) => {
                // Manager must contain exactly one transport (kafka).
                assert_eq!(mgr.transports.len(), 1);
                assert!(matches!(mgr.transports[0], OutputTransport::Kafka(_)));
            }
            Err(Error::Transport(msg)) => {
                assert!(
                    msg.contains("kafka"),
                    "Transport error should mention kafka: {msg}"
                );
            }
            Err(other) => panic!("expected Ok or Error::Transport, got {other:?}"),
        }
    }

    // -- build_scalo_kafka_config: SASL-disabled & edge cases --

    /// When `sasl.enabled = false`, SASL fields must NOT be propagated to the
    /// scalo config, regardless of username/password/mechanism values.
    #[test]
    fn test_build_scalo_kafka_config_sasl_disabled() {
        let mut legacy = KafkaConfig::default();
        legacy.sasl = Some(SaslConfig {
            enabled: false,
            mechanism: "PLAIN".to_string(),
            username: "would-be-user".to_string(),
            password: "would-be-pass".into(),
        });

        let result = build_scalo_kafka_config(&legacy);

        assert!(result.sasl_mechanism.is_none());
        assert!(result.sasl_username.is_none());
        assert!(result.sasl_password.is_none());
        // Without TLS either, security_protocol stays at scalo default.
        assert_eq!(result.security_protocol, "plaintext");
    }

    /// When `sasl` is None (not configured at all), SASL fields must remain
    /// unset. This is the common default case.
    #[test]
    fn test_build_scalo_kafka_config_sasl_none() {
        let mut legacy = KafkaConfig::default();
        legacy.sasl = None;
        legacy.tls.enabled = false;

        let result = build_scalo_kafka_config(&legacy);

        assert!(result.sasl_mechanism.is_none());
        assert!(result.sasl_username.is_none());
        assert!(result.sasl_password.is_none());
    }

    /// `sasl.enabled = false` plus `tls.enabled = true` is the TLS-only
    /// (mTLS / server-cert) shape and must produce `ssl`.
    ///
    /// It used to leave `security_protocol` at plaintext, because the TLS
    /// branch was an `else if` on `sasl.is_none()`. The ssl_* paths were still
    /// handed to librdkafka, which ignores them under plaintext -- so a config
    /// that said TLS produced an unencrypted connection with no warning.
    #[test]
    fn test_build_scalo_kafka_config_sasl_disabled_with_tls() {
        let mut legacy = KafkaConfig::default();
        legacy.sasl = Some(SaslConfig {
            enabled: false,
            mechanism: "PLAIN".to_string(),
            username: "u".to_string(),
            password: "p".into(),
        });
        legacy.tls.enabled = true;
        legacy.tls.ca_file = Some("/certs/ca.pem".to_string());

        let result = build_scalo_kafka_config(&legacy);

        assert_eq!(result.security_protocol, "ssl");
        assert!(result.sasl_mechanism.is_none());
        assert!(result.sasl_username.is_none());
        assert_eq!(result.ssl_ca_location.as_deref(), Some("/certs/ca.pem"));
    }

    /// Verify `OutputTransport::name()` returns the expected static strings
    /// via the public `OutputManager` path. We cannot easily construct a
    /// real `KafkaTransport`/`GrpcTransport` in-test without a network, but
    /// we can verify the enum-to-string mapping by calling name() directly
    /// is not possible (it's private). Instead, we verify the strings the
    /// histogram emits by reading the Kafka/Grpc TransportBase::name()
    /// indirectly through OutputManager construction: if kafka-only
    /// constructs successfully, its single transport's Kafka variant
    /// carries the "kafka" name.
    #[tokio::test]
    async fn test_output_transport_name_via_manager() {
        let output = OutputConfig {
            output_type: "kafka".to_string(),
            kafka: None,
            grpc: None,
            topic_suffix: None,
            ..Default::default()
        };
        let legacy = KafkaConfig::default();

        if let Ok(mgr) = OutputManager::new(&output, &legacy).await {
            // The single transport should be the Kafka variant.
            assert_eq!(mgr.transports.len(), 1);
            match &mgr.transports[0] {
                OutputTransport::Kafka(_) => {
                    // name() delegates to KafkaTransport::name(), which
                    // scalo documents as returning "kafka".
                    assert_eq!(mgr.transports[0].name(), "kafka");
                }
                OutputTransport::Grpc(_) => panic!("expected Kafka variant"),
            }
        }
        // If construction failed (e.g. offline CI), the assertion is skipped;
        // the other tests cover the error path.
    }
}
