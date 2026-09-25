// Project:   dfe-fetcher
// File:      crates/fetcher/src/output.rs
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
//! All Kafka access via scalo's `KafkaTransport` -- no direct rdkafka dependency.
//!
//! A failed send is classified HERE and nowhere else: [`classify_fatal`]
//! decides whether the transport refused one record or is unusable for every
//! record, and the emitter acts on the two error variants without reading any
//! error text of its own.

use std::sync::Arc;

use bytes::Bytes;
use scalo::transport::{
    GrpcTransport, KafkaConfig as ScaloKafkaConfig, KafkaTransport, MemoryTransport, RoutedSender,
    SendResult, SinkConfirmation, TransportBase, TransportConfig, TransportError, TransportSender,
    TransportType,
};
use tracing::{debug, error, info, trace};

use crate::config::{KafkaConfig as LegacyKafkaConfig, OutputConfig};
use crate::error::{Error, Result};

/// The rdkafka error codes, as their Debug names appear in scalo's send error
/// text, that name the RECORD rather than the transport: the broker or the
/// producer refused this message for its size or its format and would refuse
/// it again.
///
/// scalo answers `Fatal` for every produce error except "queue full", so a
/// timed-out delivery, a lost broker or an unknown topic reads the same as an
/// oversize message until scalo-rs #91 types the class; the text match lives
/// here and only here.
const RECORD_FAULT_CODES: [&str; 5] = [
    "MessageSizeTooLarge",
    "InvalidMessageSize",
    "InvalidMessage",
    "InvalidRecord",
    "BadMessage",
];

/// tonic's wording when an outbound gRPC message exceeds the encoder's limit.
const GRPC_RECORD_FAULT: &str = "message length too large";

/// The error a fatal send maps to: a record fault the emitter dead-letters,
/// or a transport fault that aborts the tick with no checkpoint.
fn classify_fatal(what: &str, e: &TransportError) -> Error {
    let record_fault = match e {
        TransportError::Send(text) => {
            RECORD_FAULT_CODES.iter().any(|code| text.contains(code))
                || text.contains(GRPC_RECORD_FAULT)
        }
        _ => false,
    };
    if record_fault {
        Error::TransportRecord(format!("{what}: {e}"))
    } else {
        Error::Transport(format!("{what}: {e}"))
    }
}

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
    /// In-process transport (scalo), shared so the owner can read back what
    /// landed: the framework's own tests and tooling, never a deployment.
    Memory(Arc<MemoryTransport>),
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
            Self::Memory(t) => t.send(key, payload).await,
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
            SendResult::Fatal(e) => Err(classify_fatal("transport fatal", &e)),
            SendResult::FilteredDlq => {
                debug!(
                    transport = self.name(),
                    topic = key,
                    "Message matched outbound filter -- routing to DLQ"
                );
                Err(Error::TransportRecord("filtered to DLQ".into()))
            }
        }
    }

    /// What this transport's `Ok` proves about delivery.
    fn confirms_delivery(&self) -> SinkConfirmation {
        match self {
            Self::Kafka(t) => t.confirms_delivery(),
            Self::Grpc(t) => t.confirms_delivery(),
            Self::Memory(t) => t.confirms_delivery(),
        }
    }

    /// Check if this transport is healthy.
    fn is_healthy(&self) -> bool {
        match self {
            Self::Kafka(t) => t.is_healthy(),
            Self::Grpc(t) => t.is_healthy(),
            Self::Memory(t) => t.is_healthy(),
        }
    }

    /// Close this transport gracefully.
    async fn close(&self) -> Result<()> {
        let result = match self {
            Self::Kafka(t) => t.close().await,
            Self::Grpc(t) => t.close().await,
            Self::Memory(t) => t.close().await,
        };
        result.map_err(|e| Error::Transport(format!("close failed: {e}")))
    }

    /// Transport name for logging.
    fn name(&self) -> &'static str {
        match self {
            Self::Kafka(t) => t.name(),
            Self::Grpc(t) => t.name(),
            Self::Memory(t) => t.name(),
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

    /// What a send's `Ok` proves: the weakest confirmation among the default
    /// transports. A named destination is Kafka or gRPC, which both confirm
    /// remotely, so it never weakens it.
    #[must_use]
    pub fn confirms_delivery(&self) -> SinkConfirmation {
        let strength = |c: SinkConfirmation| match c {
            SinkConfirmation::Remote => 2,
            SinkConfirmation::Local => 1,
            _ => 0,
        };
        self.transports
            .iter()
            .map(OutputTransport::confirms_delivery)
            .min_by_key(|c| strength(*c))
            .unwrap_or_default()
    }

    /// An output over one in-process transport, with no named destinations.
    ///
    /// The transport is shared so the caller can `recv` what the pipeline
    /// sent; every send goes through the same per-transport send path a Kafka
    /// deployment takes.
    #[must_use]
    pub fn memory(transport: Arc<MemoryTransport>) -> Self {
        Self {
            transports: vec![OutputTransport::Memory(transport)],
            destinations: None,
        }
    }

    /// Send a record to named destinations instead of the default transports.
    ///
    /// `key` is the wire key for a bus destination (the record's topic); a gRPC
    /// listener ignores it. Delivered only when every named destination has
    /// accepted. A backpressured or transport-wide failure stops the fan-out
    /// and is returned, so the caller re-sends it whole (at-least-once). A
    /// destination that refuses the record itself -- an outbound `dlq` filter,
    /// an over-size or malformed record -- does not stop the others: the
    /// fan-out finishes and ONE [`Error::TransportRecord`] naming every refusal
    /// comes back, so the record is dead-lettered once.
    pub async fn send_to(&self, destinations: &[&str], key: &str, payload: Bytes) -> Result<()> {
        let Some(ref set) = self.destinations else {
            return Err(Error::Config(
                "output.routes names a destination but none are declared".into(),
            ));
        };
        // Walked here, not through `RoutedSender::send_fanout`, which answers
        // `Ok` for a destination's `FilteredDlq` and so loses the record.
        let mut refused: Vec<String> = Vec::new();
        for destination in destinations {
            // Cheap ref-counted clone per destination (no buffer copy).
            match set.send_to(destination, key, payload.clone()).await {
                SendResult::Ok => {}
                SendResult::FilteredDlq => {
                    refused.push(format!("destination {destination}: filtered to DLQ"));
                }
                SendResult::Backpressured => {
                    return Err(Error::Backpressured(format!(
                        "destination {destination} backpressured"
                    )));
                }
                SendResult::Fatal(e) => {
                    match classify_fatal(&format!("destination {destination} send failed"), &e) {
                        Error::TransportRecord(text) => refused.push(text),
                        transport_wide => return Err(transport_wide),
                    }
                }
            }
        }
        if refused.is_empty() {
            Ok(())
        } else {
            Err(Error::TransportRecord(refused.join("; ")))
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

    /// The record faults rdkafka names (as scalo carries them in its send
    /// error text) and tonic's oversize wording are per-record; everything
    /// else a fatal send can carry is the transport's.
    #[test]
    fn a_fatal_send_is_a_record_fault_only_for_a_message_the_broker_names() {
        let too_large = TransportError::Send(
            "Message production error: MessageSizeTooLarge (Broker: Message size too large)".into(),
        );
        assert!(matches!(
            classify_fatal("transport fatal", &too_large),
            Error::TransportRecord(ref text) if text.contains("MessageSizeTooLarge")
        ));
        let bad_record = TransportError::Send(
            "Message production error: InvalidRecord (Broker: Broker failed to validate record)"
                .into(),
        );
        assert!(matches!(
            classify_fatal("transport fatal", &bad_record),
            Error::TransportRecord(_)
        ));
        let grpc = TransportError::Send(
            "Error, message length too large: found 5000000 bytes, the limit is: 4194304 bytes"
                .into(),
        );
        assert!(matches!(
            classify_fatal("destination send failed", &grpc),
            Error::TransportRecord(_)
        ));

        let timed_out = TransportError::Send(
            "Message production error: MessageTimedOut (Local: Message timed out)".into(),
        );
        assert!(matches!(
            classify_fatal("transport fatal", &timed_out),
            Error::Transport(ref text) if text.contains("MessageTimedOut")
        ));
        for transport_wide in [
            TransportError::Send(
                "Message production error: UnknownTopicOrPartition (Broker: Unknown topic or partition)".into(),
            ),
            TransportError::Send(
                "Message production error: TopicAuthorizationFailed (Broker: Topic authorization failed)".into(),
            ),
            TransportError::Send(
                "Message production error: AllBrokersDown (Local: All broker connections are down)".into(),
            ),
            TransportError::Closed,
            TransportError::Timeout,
            TransportError::Connection("refused".into()),
            TransportError::Config("no route".into()),
        ] {
            assert!(
                matches!(
                    classify_fatal("transport fatal", &transport_wide),
                    Error::Transport(_)
                ),
                "{transport_wide}"
            );
        }
    }

    #[test]
    fn test_build_scalo_kafka_config_defaults() {
        let legacy = KafkaConfig::default();
        let result = build_scalo_kafka_config(&legacy);

        assert_eq!(result.client_id, "dfe-fetcher");
        assert!(result.brokers.is_empty());
        // No SASL configured, no TLS -> security_protocol stays at scalo default
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
        // it must surface as Error::Transport -- never any other variant.
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
                OutputTransport::Grpc(_) | OutputTransport::Memory(_) => {
                    panic!("expected Kafka variant")
                }
            }
        }
        // If construction failed (e.g. offline CI), the assertion is skipped;
        // the other tests cover the error path.
    }

    // -- named-destination fan-out and the dead-letter queue --

    /// A scalo gRPC Push listener on a free loopback port, and the endpoint a
    /// destination dials to reach it.
    async fn grpc_listener() -> (GrpcTransport, String) {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen = format!("127.0.0.1:{port}");
        let server = GrpcTransport::new(&scalo::transport::GrpcConfig::server(&listen))
            .await
            .unwrap();
        (server, format!("http://{listen}"))
    }

    /// The sorted `id` of every record a listener has received.
    async fn received_ids(server: &GrpcTransport) -> Vec<String> {
        use scalo::transport::TransportReceiver;

        let mut ids = Vec::new();
        loop {
            let batch = server.recv(100).await.unwrap();
            if batch.records.is_empty() {
                break;
            }
            for record in batch.records {
                let row: serde_json::Value = serde_json::from_slice(&record.payload).unwrap();
                ids.push(row["id"].as_str().unwrap().to_owned());
            }
        }
        ids.sort();
        ids
    }

    /// An extractor sink over `config`'s output, dead-lettering to files under
    /// `dlq`.
    async fn fanout_pipeline(
        mut config: Config,
        dlq: &std::path::Path,
    ) -> crate::extractor::ExtractorSink {
        config.dlq.enabled = true;
        config.dlq.mode = scalo::dlq::DlqMode::FileOnly;
        config.dlq.file.path = dlq.to_path_buf();
        config.dlq.flush_interval_ms = 10;
        Box::pin(sink_over(config)).await.0
    }

    /// An extractor sink over `config`'s output and DLQ, with its metrics.
    async fn sink_over(
        config: Config,
    ) -> (
        crate::extractor::ExtractorSink,
        Arc<crate::metrics::Metrics>,
    ) {
        let output = OutputManager::new(&config.output, &config.kafka)
            .await
            .unwrap();
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let state = Arc::new(
            crate::pipeline::PipelineState::new(
                crate::config::SharedConfig::new(config),
                Arc::clone(&metrics),
                Some(output),
                tokio_util::sync::CancellationToken::new(),
            )
            .unwrap(),
        );
        (
            crate::extractor::ExtractorSink::new(state, Arc::clone(&metrics)),
            metrics,
        )
    }

    /// Every entry the file DLQ under `dir` holds once its first entry has
    /// flushed and several more flush intervals have passed.
    async fn dead_letters(dir: &std::path::Path) -> Vec<scalo::dlq::DlqEntry> {
        fn read(dir: &std::path::Path, out: &mut Vec<scalo::dlq::DlqEntry>) {
            let Ok(walk) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in walk.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    read(&path, out);
                } else if let Ok(text) = std::fs::read_to_string(&path) {
                    out.extend(text.lines().filter_map(|l| serde_json::from_str(l).ok()));
                }
            }
        }
        let tick = std::time::Duration::from_millis(20);
        let mut out = Vec::new();
        for _ in 0..100 {
            read(dir, &mut out);
            if !out.is_empty() {
                break;
            }
            tokio::time::sleep(tick).await;
        }
        // A second copy, were one written, would land within these intervals.
        tokio::time::sleep(tick * 5).await;
        out.clear();
        read(dir, &mut out);
        out
    }

    /// A gRPC default output over `default_endpoint`, plus named destinations.
    fn grpc_output(default_endpoint: &str, destinations: Vec<(&str, DestinationSpec)>) -> Config {
        let mut config = Config::default();
        config.output.output_type = "grpc".into();
        config.output.grpc = Some(scalo::transport::GrpcConfig::client(default_endpoint));
        config.output.destinations = destinations
            .into_iter()
            .map(|(name, spec)| (name.to_owned(), spec))
            .collect();
        config
    }

    /// A record a fanned-out destination refuses (here scalo's outbound `dlq`
    /// filter, which answers `FilteredDlq` as an over-size send does) still
    /// reaches every destination that accepts it, and lands on the fetcher's
    /// DLQ exactly once however many destinations refused it.
    #[tokio::test]
    async fn a_fanout_record_a_destination_filters_is_delivered_and_dead_lettered_once() {
        use dfe_fetcher_core::batch::Outbound;
        use scalo::transport::GrpcConfig;
        use scalo::transport::filter::{FilterAction, FilterRule};

        let (default_rx, default_ep) = grpc_listener().await;
        let (siem_rx, siem_ep) = grpc_listener().await;
        let (archive_rx, archive_ep) = grpc_listener().await;
        let (loader_rx, loader_ep) = grpc_listener().await;
        let refusing = |endpoint: &str| DestinationSpec {
            grpc: Some(GrpcConfig {
                filters_out: vec![FilterRule {
                    expression: r#"id == "poison""#.into(),
                    action: FilterAction::Dlq,
                }],
                ..GrpcConfig::client(endpoint)
            }),
            kafka: None,
        };
        let config = grpc_output(
            &default_ep,
            vec![
                ("siem", refusing(&siem_ep)),
                ("archive", refusing(&archive_ep)),
                (
                    "loader",
                    DestinationSpec {
                        grpc: Some(GrpcConfig::client(&loader_ep)),
                        kafka: None,
                    },
                ),
            ],
        );
        let dlq = tempfile::tempdir().unwrap();
        let sink = Box::pin(fanout_pipeline(config, dlq.path())).await;

        // The refusing destinations come first: a fan-out that stopped at a
        // refusal would never reach the loader.
        let route: Arc<[Arc<str>]> = ["siem", "archive", "loader"]
            .into_iter()
            .map(Arc::from)
            .collect();
        let report = sink
            .emit(vec![
                Outbound::new("fixture_land", r#"{"id":"poison"}"#).with_route(Arc::clone(&route)),
                Outbound::new("fixture_land", r#"{"id":"clean"}"#).with_route(route),
            ])
            .await
            .expect("a refused record does not fail the batch");

        assert_eq!(
            report,
            crate::emit::EmitReport {
                sent: 1,
                dead_lettered: 1,
                dropped: 0
            }
        );
        assert_eq!(received_ids(&loader_rx).await, ["clean", "poison"]);
        assert_eq!(received_ids(&siem_rx).await, ["clean"]);
        assert_eq!(received_ids(&archive_rx).await, ["clean"]);
        assert!(
            received_ids(&default_rx).await.is_empty(),
            "a routed record skips the default output"
        );

        let entries = dead_letters(dlq.path()).await;
        assert_eq!(entries.len(), 1, "one copy, not one per refusal");
        let parked: serde_json::Value = serde_json::from_slice(&entries[0].payload).unwrap();
        assert_eq!(parked["id"], "poison");
        assert_eq!(entries[0].destination.as_deref(), Some("fixture_land"));
        assert!(
            entries[0].reason.contains("siem") && entries[0].reason.contains("archive"),
            "{}",
            entries[0].reason
        );
    }

    /// A dead letter counts only once the DLQ confirms it holds the record: a
    /// write the DLQ refuses fails the emit, so the tick keeps its cursor and
    /// the record is fetched again rather than counted handled and lost.
    #[tokio::test]
    async fn a_dead_letter_the_dlq_refuses_fails_the_emit() {
        use dfe_fetcher_core::batch::Outbound;
        use scalo::transport::GrpcConfig;
        use scalo::transport::filter::{FilterAction, FilterRule};

        let (_default_rx, default_ep) = grpc_listener().await;
        let mut config = Config::default();
        config.output.output_type = "grpc".into();
        config.output.grpc = Some(GrpcConfig {
            filters_out: vec![FilterRule {
                expression: r#"id == "poison""#.into(),
                action: FilterAction::Dlq,
            }],
            ..GrpcConfig::client(&default_ep)
        });
        let dlq = tempfile::tempdir().unwrap();
        let sink = Box::pin(fanout_pipeline(config, dlq.path())).await;
        // A plain file where the DLQ's directory belongs: every write fails.
        let service_dir = dlq.path().join("dfe-fetcher");
        std::fs::remove_dir_all(&service_dir).unwrap();
        std::fs::write(&service_dir, b"not a directory").unwrap();

        let err = sink
            .emit(vec![Outbound::new("fixture_land", r#"{"id":"poison"}"#)])
            .await
            .expect_err("a dead letter nothing holds fails the emit");

        assert!(matches!(err, Error::Transport(_)), "{err:?}");
        assert!(
            err.to_string().contains("could not be dead-lettered"),
            "{err}"
        );
    }

    /// Each intake reports its guarantee against what the outputs confirm. A
    /// gRPC output confirms remotely, so a scheduled source, whose cursor
    /// waits on delivery, is at-least-once, and a container's stdout, which
    /// holds nothing, is best effort. An in-process output confirms nothing.
    #[tokio::test]
    async fn each_intake_reports_its_delivery_guarantee() {
        use crate::metrics::recorded::{Recorder, carries};
        use crate::pipeline::HeldUntilDelivered;
        use scalo::transport::AckKind;

        let recorder = Recorder::new();
        let _recording = recorder.install();
        let (_rx, endpoint) = grpc_listener().await;
        let mut config = Config::default();
        config.output.output_type = "grpc".into();
        config.output.grpc = Some(scalo::transport::GrpcConfig::client(&endpoint));
        let grpc = OutputManager::new(&config.output, &config.kafka)
            .await
            .unwrap();
        assert_eq!(grpc.confirms_delivery(), SinkConfirmation::Remote);
        let (sink, _) = Box::pin(sink_over(config)).await;

        sink.state()
            .publish_guarantee("scheduled", Some(&HeldUntilDelivered(AckKind::Pull)));
        sink.state().publish_guarantee("container", None);

        let raised = recorder.raised_gauges("pipeline_delivery_guarantee");
        for wanted in [
            [
                ("intake", "scheduled"),
                ("guarantee", "at_least_once"),
                ("reason", "confirmed"),
            ],
            [
                ("intake", "container"),
                ("guarantee", "best_effort"),
                ("reason", "source_cannot_ack"),
            ],
        ] {
            assert!(
                raised.iter().any(|labels| carries(labels, &wanted)),
                "{wanted:?} not in {raised:?}"
            );
        }

        let memory = OutputManager::memory(Arc::new(
            MemoryTransport::new(&scalo::transport::MemoryConfig::default()).unwrap(),
        ));
        assert_eq!(memory.confirms_delivery(), SinkConfirmation::None);
    }

    /// With no DLQ a record the transport refuses is still a permanent
    /// refusal: it is dropped and counted by reason, the rest of the batch is
    /// delivered, and the emit succeeds, so the source moves past a record it
    /// could never send instead of fetching it again for ever.
    #[tokio::test]
    async fn a_refused_record_with_no_dlq_is_dropped_and_counted() {
        use dfe_fetcher_core::batch::Outbound;
        use scalo::transport::GrpcConfig;
        use scalo::transport::filter::{FilterAction, FilterRule};

        let recorder = crate::metrics::recorded::Recorder::new();
        let _recording = recorder.install();
        let (default_rx, default_ep) = grpc_listener().await;
        let mut config = Config::default();
        config.output.output_type = "grpc".into();
        config.output.grpc = Some(GrpcConfig {
            filters_out: vec![FilterRule {
                expression: r#"id == "poison""#.into(),
                action: FilterAction::Dlq,
            }],
            ..GrpcConfig::client(&default_ep)
        });
        config.dlq.enabled = false;
        let (sink, metrics) = Box::pin(sink_over(config)).await;

        let report = sink
            .emit(vec![
                Outbound::new("fixture_land", r#"{"id":"poison"}"#),
                Outbound::new("fixture_land", r#"{"id":"clean"}"#),
            ])
            .await
            .expect("a permanent refusal does not fail the batch");

        assert_eq!(
            report,
            crate::emit::EmitReport {
                sent: 1,
                dead_lettered: 0,
                dropped: 1
            }
        );
        assert_eq!(received_ids(&default_rx).await, ["clean"]);
        assert_eq!(metrics.dead_letters_dropped(), 1);
        assert_eq!(
            recorder.counter(
                "pipeline_dead_letters_dropped_total",
                &[("reason", "transport_refused")]
            ),
            Some(1)
        );
    }

    /// A record over a bus destination's `message.max.bytes` is refused by
    /// librdkafka before any broker is asked, and scalo answers that with
    /// `FilteredDlq`: the record goes to the DLQ and still reaches the gRPC
    /// destination beside it.
    #[tokio::test]
    async fn a_fanout_record_over_a_bus_destinations_size_ceiling_is_dead_lettered() {
        use dfe_fetcher_core::batch::Outbound;
        use scalo::transport::GrpcConfig;

        let (default_rx, default_ep) = grpc_listener().await;
        let (loader_rx, loader_ep) = grpc_listener().await;
        let mut bus = ScaloKafkaConfig {
            // Nothing listens here: the size check is the producer's own.
            brokers: vec!["127.0.0.1:1".into()],
            group: String::new(),
            ..ScaloKafkaConfig::default()
        };
        bus.sizing.producer.message_max_bytes = Some(1000);
        let config = grpc_output(
            &default_ep,
            vec![
                (
                    "bus",
                    DestinationSpec {
                        grpc: None,
                        kafka: Some(KafkaDestination {
                            config: Some(bus),
                            topic: None,
                        }),
                    },
                ),
                (
                    "loader",
                    DestinationSpec {
                        grpc: Some(GrpcConfig::client(&loader_ep)),
                        kafka: None,
                    },
                ),
            ],
        );
        let dlq = tempfile::tempdir().unwrap();
        let sink = Box::pin(fanout_pipeline(config, dlq.path())).await;

        let route: Arc<[Arc<str>]> = ["bus", "loader"].into_iter().map(Arc::from).collect();
        let big = format!(r#"{{"id":"big","blob":"{}"}}"#, "x".repeat(4096));
        let report = sink
            .emit(vec![Outbound::new("fixture_land", big).with_route(route)])
            .await
            .expect("an over-size record does not fail the batch");

        assert_eq!(
            report,
            crate::emit::EmitReport {
                sent: 0,
                dead_lettered: 1,
                dropped: 0
            }
        );
        assert_eq!(received_ids(&loader_rx).await, ["big"]);
        assert!(received_ids(&default_rx).await.is_empty());
        let entries = dead_letters(dlq.path()).await;
        assert_eq!(entries.len(), 1);
        let parked: serde_json::Value = serde_json::from_slice(&entries[0].payload).unwrap();
        assert_eq!(parked["id"], "big");
        assert!(entries[0].reason.contains("bus"), "{}", entries[0].reason);
    }
}
