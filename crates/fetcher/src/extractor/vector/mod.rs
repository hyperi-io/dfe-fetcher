// Project:   dfe-fetcher
// File:      crates/fetcher/src/extractor/vector/mod.rs
// Purpose:   Vector.dev integration for data extraction
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector.dev extractor integration.
//!
//! Runs Vector instances as data extractors, receiving data via the native
//! Vector gRPC sink protocol (supported in scalo).
//!
//! ## Architecture
//!
//! Uses scalo's `GrpcTransport` with `vector_compat` enabled to accept
//! Vector's native `PushEvents` gRPC protocol. This allows any Vector instance
//! configured with a `vector` sink to push data directly to the fetcher.
//!
//! ## Acknowledgements
//!
//! The receiver is built armed, so with `acknowledgements.enabled` (the
//! default) a push is answered only once its events are emitted: `OK` when
//! the outputs took them or the DLQ confirmed them, `UNAVAILABLE` otherwise,
//! which Vector's sink retries. At shutdown the receiver refuses new pushes
//! and delivers what it has queued before the outputs close. While the
//! fetcher's memory-pressure latch holds, pushes are refused `UNAVAILABLE`.
//!
//! ## Topics
//!
//! A Vector push names no instance, so the listener it arrives on says which
//! instance sent it. The shared `grpc_bind_address` carries at most one
//! instance (its pushes land on that instance's topic, or on `vector` when no
//! instance shares it), and every other instance names its own
//! `grpc_bind_address`.
//!
//! ## Modes
//!
//! 1. **Container mode** -- Vector runs as a managed container, configured via
//!    a generated `vector.toml`. Data flows: external source -> Vector -> gRPC -> fetcher.
//!
//! 2. **Sidecar mode** -- Vector runs alongside the fetcher (e.g., in same pod),
//!    connecting to the fetcher's gRPC endpoint.
//!
//! ## Configuration
//!
//! ```yaml
//! extractors:
//!   vector:
//!     enabled: true
//!     grpc_bind_address: "0.0.0.0:6000"
//!     instances:
//!       - name: syslog-collector
//!         mode: container
//!         image: timberio/vector:latest-alpine
//!         vector_config: |
//!           [sources.syslog]
//!           type = "syslog"
//!           address = "0.0.0.0:514"
//!           [sinks.dfe]
//!           type = "vector"
//!           address = "host.docker.internal:6000"
//!         topic: syslog
//!       - name: firewall
//!         mode: sidecar
//!         grpc_bind_address: "127.0.0.1:6001"
//!         topic: firewall
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use scalo::governor::UnifiedPressure;
use scalo::transport::ack::HOLD_RELEASE_MARGIN;
use scalo::transport::{
    DeliveryStatus, GrpcConfig, GrpcToken, GrpcTransport, SourceAck, TransportBase, TransportError,
    TransportReceiver, WorkBatch,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::VectorExtractorConfig;
use crate::emit::EmitReport;
use crate::error::{Error, Result};
use crate::extractor::ExtractorSink;
use crate::metrics::{ExtractorFailure, Metrics};
use crate::pipeline::PipelineState;

/// Most events one receive takes off the queue.
const RECV_MAX: usize = 100;

/// The DFE source name behind an output topic: the topic minus the suffix.
///
/// `_source` carries this value and the receiver routes a fetcher-origin source
/// on it, so the suffix must come off with the same string the topic was built
/// from -- `Config::topic_suffix`, not the legacy `kafka.topic_suffix` alone.
fn source_from_topic<'a>(topic: &'a str, suffix: &str) -> &'a str {
    if suffix.is_empty() {
        return topic;
    }
    topic.strip_suffix(suffix).unwrap_or(topic)
}

/// The receive loop: each block the gRPC server queued is emitted, then its
/// pushes are answered from the outcome.
struct VectorReceiver {
    sink: ExtractorSink,
    metrics: Arc<Metrics>,
    default_topic: String,
    topic_map: HashMap<String, String>,
}

impl VectorReceiver {
    /// Deliver blocks until shutdown, then refuse new pushes and deliver what
    /// is already queued.
    async fn run(self, transport: GrpcTransport, shutdown: CancellationToken) {
        loop {
            let received = tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                received = transport.recv(RECV_MAX) => received,
            };
            match received {
                Ok(batch) => self.deliver(&transport, batch).await,
                Err(TransportError::Closed) => return,
                Err(e) => {
                    warn!(error = %e, "Vector gRPC receive error");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }

        info!("Vector receiver shutting down: refusing new pushes, delivering what is queued");
        if let Err(e) = transport.close().await {
            warn!(error = %e, "Error closing Vector transport");
        }
        let mut drained: u64 = 0;
        loop {
            match transport.recv(RECV_MAX).await {
                // A push that reserved room before the close may still land, so
                // only Closed ends the drain, and each empty receive waits out
                // the server's receive timeout rather than spinning.
                Ok(batch) if batch.records.is_empty() && batch.commit_tokens.is_empty() => {}
                Ok(batch) => {
                    drained += batch.records.len() as u64;
                    self.deliver(&transport, batch).await;
                }
                Err(TransportError::Closed) => break,
                Err(e) => {
                    warn!(error = %e, "Vector gRPC receive error while draining");
                    break;
                }
            }
        }
        info!(records = drained, "Vector receiver drained");
    }

    /// Emit one received block and answer its pushes from the outcome.
    ///
    /// A held push waits no longer than its hold budget: the emit is given up
    /// just before it, so Vector hears `UNAVAILABLE` and retries before its
    /// own deadline.
    async fn deliver(&self, transport: &GrpcTransport, batch: WorkBatch<GrpcToken>) {
        if batch.records.is_empty() && batch.commit_tokens.is_empty() {
            return;
        }
        let count = batch.records.len() as u64;
        let deadline = transport.hold_deadline(&batch.commit_tokens);
        let ack = SourceAck::new(transport, batch.commit_tokens);
        let piece = ack.piece();

        let suffix = self.sink.state().config().topic_suffix().to_owned();
        // A record's key names its instance (mapped to the instance's topic) or
        // the topic itself. Vector-compat events carry none.
        let records: Vec<_> = batch
            .records
            .into_iter()
            .map(|record| {
                let topic = record
                    .key
                    .as_ref()
                    .and_then(|k| self.topic_map.get(k.as_ref()).cloned())
                    .or_else(|| record.key.as_ref().map(|k| k.to_string()))
                    .unwrap_or_else(|| self.default_topic.clone());
                let source = source_from_topic(&topic, &suffix);
                self.sink.record(source, "vector", &topic, record.payload)
            })
            .collect();

        let emitted = match deadline {
            Some(deadline) => {
                let give_up = deadline
                    .checked_sub(HOLD_RELEASE_MARGIN)
                    .unwrap_or(deadline);
                tokio::time::timeout_at(
                    tokio::time::Instant::from_std(give_up),
                    self.sink.emit(records),
                )
                .await
                .unwrap_or_else(|_| {
                    Err(Error::Backpressured(
                        "the push's hold budget ran out before its events were delivered".into(),
                    ))
                })
            }
            None => self.sink.emit(records).await,
        };
        piece.report(status_of(&emitted));
        if let Err(e) = ack.release().await {
            warn!(error = %e, "Answering the Vector pushes failed, so Vector retries them");
        }

        match emitted {
            Ok(_) => {
                self.metrics.add_extractor_records(count);
                debug!(records = count, "Vector block delivered");
            }
            Err(e) => {
                // Held pushes are answered UNAVAILABLE and re-sent. With
                // acknowledgements off they were answered at enqueue and are lost.
                let held = transport.ack_control().is_some_and(|c| c.enabled());
                let outcome = if held {
                    ExtractorFailure::Retry
                } else {
                    ExtractorFailure::Dropped
                };
                self.metrics
                    .add_extractor_records_failed("vector", outcome, count);
                error!(
                    error = %e,
                    records = count,
                    outcome = outcome.as_str(),
                    "Failed to deliver a Vector block"
                );
            }
        }
    }
}

/// The delivery status a block's emit reports: dead-lettered records the DLQ
/// confirmed still release the push.
fn status_of(emitted: &Result<EmitReport>) -> DeliveryStatus {
    match emitted {
        Ok(report) if report.dead_lettered > 0 => DeliveryStatus::Rejected,
        Ok(_) => DeliveryStatus::Delivered,
        Err(_) => DeliveryStatus::Errored,
    }
}

/// Vector extractor manager.
///
/// Manages a gRPC server that accepts Vector protocol events and delivers
/// them to the pipeline. Optionally manages container Vector instances.
pub struct VectorManager {
    config: VectorExtractorConfig,
    sink: ExtractorSink,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    /// The fetcher's memory-pressure latch, shed on by every listener.
    pressure: Option<Arc<UnifiedPressure>>,
}

impl VectorManager {
    /// Create a new Vector manager.
    pub fn new(
        config: VectorExtractorConfig,
        pipeline: Arc<PipelineState>,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            config,
            sink: ExtractorSink::new(pipeline, Arc::clone(&metrics)),
            metrics,
            shutdown,
            pressure: None,
        }
    }

    /// Refuse pushes with `UNAVAILABLE` while `pressure` holds, as the other
    /// push listeners do, so Vector backs off instead of filling memory.
    #[must_use]
    pub fn with_pressure(mut self, pressure: Option<Arc<UnifiedPressure>>) -> Self {
        self.pressure = pressure;
        self
    }

    /// Start the Vector gRPC listeners and managed instances, returning the
    /// address each listener bound, the shared one first (none when
    /// disabled).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when more than one instance shares the
    /// shared listener, and [`Error::Source`] when a listener cannot bind.
    pub async fn start(&self) -> Result<Vec<SocketAddr>> {
        if !self.config.enabled {
            return Ok(Vec::new());
        }
        let listeners = self.config.listeners()?;

        info!(
            grpc_bind = %self.config.grpc_bind_address,
            listeners = listeners.len(),
            instances = self.config.instances.len(),
            acknowledgements = self.config.acknowledgements.enabled,
            "Starting Vector extractor manager"
        );

        // Every listener binds before any receive loop starts, so a bind
        // failure leaves nothing half-started.
        let mut transports = Vec::with_capacity(listeners.len());
        for listener in &listeners {
            transports.push(self.listen(&listener.bind_address).await?);
        }

        // Start managed container instances
        for instance in &self.config.instances {
            if instance.mode == "container" {
                self.start_vector_container(instance);
            }
        }

        let suffix = self.sink.state().config().topic_suffix().to_owned();
        let topic_map = self.build_topic_map();
        let mut addresses = Vec::with_capacity(transports.len());
        for (listener, transport) in listeners.iter().zip(transports) {
            let default_topic = format!("{}{suffix}", listener.topic);
            info!(
                address = %listener.bind_address,
                topic = %default_topic,
                "Vector gRPC listener serving"
            );
            addresses.extend(transport.local_addr());
            let receiver = VectorReceiver {
                sink: self.sink.clone(),
                metrics: Arc::clone(&self.metrics),
                default_topic,
                topic_map: topic_map.clone(),
            };
            // On the intake tracker, so shutdown keeps the outputs open while
            // the receiver delivers what it has queued.
            self.sink
                .state()
                .intake()
                .spawn(receiver.run(transport, self.shutdown.clone()));
        }
        Ok(addresses)
    }

    /// Bind one Vector listener, armed before it listens so no push is
    /// answered before its receive loop has delivered it.
    async fn listen(&self, bind_address: &str) -> Result<GrpcTransport> {
        let grpc_config = GrpcConfig::server(bind_address).with_vector_compat();
        let mut builder = GrpcTransport::builder(&grpc_config)
            .acknowledgements(self.config.acknowledgements)
            .armed(true)
            .memory_guard(Arc::clone(self.sink.state().memory_guard()));
        if let Some(pressure) = &self.pressure {
            builder = builder.pressure(Arc::clone(pressure));
        }
        builder.start().await.map_err(|e| {
            Error::Source(format!(
                "failed to start Vector gRPC server on {bind_address}: {e}"
            ))
        })
    }

    /// Instance name to topic, for a native push whose key names an instance.
    fn build_topic_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        let config = self.sink.state().config();
        let suffix = config.topic_suffix();

        for instance in &self.config.instances {
            let topic = format!("{}{}", instance.topic, suffix);
            map.insert(instance.name.clone(), topic);
        }

        map
    }

    /// Start a managed Vector container instance.
    fn start_vector_container(&self, instance: &crate::config::VectorInstance) {
        let image = instance
            .image
            .as_deref()
            .unwrap_or("timberio/vector:latest-alpine");

        info!(
            name = %instance.name,
            image = %image,
            topic = %instance.topic,
            "Starting managed Vector instance"
        );

        // Build vector config and start container
        let runtime = "docker";
        let container_name = format!("dfe-fetcher-vector-{}", instance.name);

        let mut args = vec![
            "run".to_string(),
            "--rm".to_string(),
            "--name".to_string(),
            container_name,
            "--label".to_string(),
            "managed-by=dfe-fetcher".to_string(),
        ];

        // Pass vector config as environment variable if provided
        if let Some(ref config_str) = instance.vector_config {
            args.push("--env".to_string());
            args.push(format!("VECTOR_CONFIG={config_str}"));
        }

        // Mount vector config file if path provided
        if let Some(ref config_path) = instance.vector_config_path {
            args.push("-v".to_string());
            args.push(format!("{config_path}:/etc/vector/vector.toml:ro"));
        }

        args.push(image.to_string());

        let spawn_result = tokio::process::Command::new(runtime)
            .args(&args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn();

        match spawn_result {
            Ok(_child) => {
                info!(name = %instance.name, "Vector container started");
            }
            Err(e) => {
                error!(
                    name = %instance.name,
                    error = %e,
                    "Failed to start Vector container"
                );
            }
        }
    }

    /// Stop all managed Vector instances.
    pub async fn stop(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        info!("Stopping Vector extractor manager");

        // Stop managed containers
        for instance in &self.config.instances {
            if instance.mode == "container" {
                let container_name = format!("dfe-fetcher-vector-{}", instance.name);
                let _ = tokio::process::Command::new("docker")
                    .args(["stop", "--time", "10", &container_name])
                    .output()
                    .await;
            }
        }

        Ok(())
    }

    /// Check if Vector manager is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use scalo::transport::{MemoryConfig, MemoryTransport, VectorCompatClient};

    use crate::config::{Config, SharedConfig};
    use crate::output::OutputManager;

    /// A pipeline whose only output is `output`, with its metrics.
    fn pipeline_over(output: &Arc<MemoryTransport>) -> (Arc<PipelineState>, Arc<Metrics>) {
        let metrics = Arc::new(Metrics::new());
        let state = Arc::new(PipelineState::for_tests(
            SharedConfig::new(Config::default()),
            Arc::clone(&metrics),
            Some(OutputManager::memory(Arc::clone(output))),
        ));
        (state, metrics)
    }

    fn memory_output() -> Arc<MemoryTransport> {
        Arc::new(MemoryTransport::new(&MemoryConfig::default()).expect("memory transport"))
    }

    /// The Vector receiver config on a free loopback port.
    fn receiver_config(acknowledgements: bool) -> VectorExtractorConfig {
        VectorExtractorConfig {
            enabled: true,
            grpc_bind_address: "127.0.0.1:0".into(),
            instances: vec![],
            acknowledgements: scalo::transport::AcknowledgementsConfig::new(acknowledgements),
        }
    }

    /// A sidecar instance (no container is started for it) on `topic`,
    /// sharing the shared listener or on its own `address`.
    fn instance(name: &str, topic: &str, address: Option<&str>) -> crate::config::VectorInstance {
        crate::config::VectorInstance {
            name: name.into(),
            mode: "sidecar".into(),
            image: None,
            vector_config: None,
            vector_config_path: None,
            topic: topic.into(),
            grpc_bind_address: address.map(str::to_owned),
        }
    }

    /// Start `manager` and return its one listener's address.
    async fn only_listener(manager: &VectorManager) -> SocketAddr {
        let addresses = manager.start().await.expect("start");
        assert_eq!(addresses.len(), 1, "{addresses:?}");
        addresses[0]
    }

    /// Push one event to `addr`.
    async fn push(
        addr: SocketAddr,
        event: serde_json::Value,
    ) -> scalo::transport::TransportResult<()> {
        VectorCompatClient::connect_lazy(&format!("http://{addr}"))
            .expect("client")
            .send_events(&[event])
            .await
    }

    /// The topic every record the memory output has taken landed on.
    async fn topics(output: &MemoryTransport) -> Vec<String> {
        let batch = output.recv(100).await.expect("recv");
        batch
            .records
            .iter()
            .map(|r| r.key.as_deref().unwrap_or_default().to_owned())
            .collect()
    }

    /// Every payload the memory output has taken, parsed.
    async fn delivered(output: &MemoryTransport) -> Vec<serde_json::Value> {
        let mut rows = Vec::new();
        loop {
            let batch = output.recv(100).await.expect("recv");
            if batch.records.is_empty() {
                return rows;
            }
            rows.extend(
                batch
                    .records
                    .iter()
                    .map(|r| serde_json::from_slice(&r.payload).expect("json")),
            );
        }
    }

    /// A push is answered OK once its events reached the output, and only
    /// then do they count as received.
    #[tokio::test]
    async fn a_push_is_answered_once_its_events_are_delivered() {
        let output = memory_output();
        let (state, metrics) = pipeline_over(&output);
        let shutdown = CancellationToken::new();
        let manager = VectorManager::new(
            receiver_config(true),
            Arc::clone(&state),
            Arc::clone(&metrics),
            shutdown.clone(),
        );
        let addr = only_listener(&manager).await;

        let client = VectorCompatClient::connect_lazy(&format!("http://{addr}")).expect("client");
        client
            .send_events(&[serde_json::json!({"id": 1}), serde_json::json!({"id": 2})])
            .await
            .expect("delivered events are answered OK");

        // The answer came after the emit, so both events are already there.
        let rows = delivered(&output).await;
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0]["_source_fetcher"], "vector");
        assert_eq!(metrics.extractor_records_failed(), 0);
        shutdown.cancel();
    }

    /// An emit that fails answers the push UNAVAILABLE so Vector re-sends it,
    /// and the events count as failed for retry, never as received.
    #[tokio::test]
    async fn a_push_whose_events_cannot_be_delivered_is_answered_for_retry() {
        let output = memory_output();
        output.close().await.expect("close");
        let (state, metrics) = pipeline_over(&output);
        let shutdown = CancellationToken::new();
        let manager = VectorManager::new(
            receiver_config(true),
            Arc::clone(&state),
            Arc::clone(&metrics),
            shutdown.clone(),
        );
        let addr = only_listener(&manager).await;

        let client = VectorCompatClient::connect_lazy(&format!("http://{addr}")).expect("client");
        let err = client
            .send_events(&[serde_json::json!({"id": 1})])
            .await
            .expect_err("an undelivered push is not answered OK");
        assert!(
            err.to_string().contains("Unavailable") || err.to_string().contains("unavailable"),
            "{err}"
        );
        assert_eq!(metrics.extractor_records_failed(), 1);
        assert!(
            metrics
                .render()
                .contains("dfe_fetcher_extractor_records_total 0"),
            "a failed event is never counted received"
        );
        shutdown.cancel();
    }

    /// The one instance on the shared listener owns its pushes, so they land
    /// on that instance's topic, not on `vector`.
    #[tokio::test]
    async fn the_instance_on_the_shared_listener_lands_on_its_topic() {
        let output = memory_output();
        let (state, metrics) = pipeline_over(&output);
        let shutdown = CancellationToken::new();
        let mut config = receiver_config(true);
        config.instances = vec![instance("syslog-collector", "syslog", None)];
        let manager = VectorManager::new(config, state, metrics, shutdown.clone());
        let addr = only_listener(&manager).await;

        push(addr, serde_json::json!({"id": 1}))
            .await
            .expect("delivered");

        assert_eq!(topics(&output).await, ["syslog_land"]);
        shutdown.cancel();
    }

    /// Each instance on its own listener lands on its own topic, and the
    /// shared listener with no instance on it lands on `vector`.
    #[tokio::test]
    async fn each_instance_on_its_own_listener_lands_on_its_topic() {
        let output = memory_output();
        let (state, metrics) = pipeline_over(&output);
        let shutdown = CancellationToken::new();
        let mut config = receiver_config(true);
        config.instances = vec![
            instance("syslog-collector", "syslog", Some("127.0.0.1:0")),
            instance("firewall", "firewall", Some("127.0.0.1:0")),
        ];
        let manager = VectorManager::new(config, state, metrics, shutdown.clone());
        let addresses = manager.start().await.expect("start");
        assert_eq!(addresses.len(), 3, "the shared listener and one each");

        for (addr, id) in addresses.iter().zip(1..) {
            push(*addr, serde_json::json!({ "id": id }))
                .await
                .expect("delivered");
        }

        assert_eq!(
            topics(&output).await,
            ["vector_land", "syslog_land", "firewall_land"]
        );
        shutdown.cancel();
    }

    /// Two instances on the shared listener cannot be told apart, so the
    /// manager refuses to start rather than land both on one topic.
    #[tokio::test]
    async fn two_instances_on_the_shared_listener_are_refused() {
        let output = memory_output();
        let (state, metrics) = pipeline_over(&output);
        let mut config = receiver_config(true);
        config.instances = vec![
            instance("syslog-collector", "syslog", None),
            instance("firewall", "firewall", None),
        ];
        let manager = VectorManager::new(config, state, metrics, CancellationToken::new());

        let err = manager.start().await.expect_err("refused");

        assert!(matches!(err, Error::Config(_)), "{err:?}");
        assert!(
            err.to_string().contains("syslog-collector, firewall"),
            "{err}"
        );
    }

    /// A pressure source whose reading the test sets.
    struct Settable(std::sync::atomic::AtomicU64);

    impl scalo::governor::PressureSource for Settable {
        fn name(&self) -> &'static str {
            "test"
        }
        fn sample(&self) -> scalo::governor::Pressure {
            scalo::governor::Pressure::new(
                self.0.load(std::sync::atomic::Ordering::Relaxed) as f64 / 100.0,
            )
        }
        fn is_hard(&self) -> bool {
            true
        }
    }

    /// While the fetcher's pressure latch holds, a push is refused
    /// UNAVAILABLE before any work, and it is taken again once the latch
    /// releases.
    #[tokio::test]
    async fn a_push_under_memory_pressure_is_refused_for_retry() {
        let output = memory_output();
        let (state, metrics) = pipeline_over(&output);
        let reading = Arc::new(Settable(std::sync::atomic::AtomicU64::new(100)));
        let pressure = Arc::new(UnifiedPressure::new(
            vec![Arc::clone(&reading) as Arc<dyn scalo::governor::PressureSource>],
            scalo::governor::Hysteresis::new(0.8, 0.6).expect("band"),
        ));
        let shutdown = CancellationToken::new();
        let manager = VectorManager::new(
            receiver_config(true),
            state,
            Arc::clone(&metrics),
            shutdown.clone(),
        )
        .with_pressure(Some(pressure));
        let addr = only_listener(&manager).await;

        let err = push(addr, serde_json::json!({"id": 1}))
            .await
            .expect_err("refused under pressure");
        assert!(err.to_string().contains("under pressure"), "{err}");
        assert!(topics(&output).await.is_empty(), "nothing was taken");

        reading.0.store(0, std::sync::atomic::Ordering::Relaxed);
        push(addr, serde_json::json!({"id": 2}))
            .await
            .expect("taken once the latch releases");
        assert_eq!(topics(&output).await, ["vector_land"]);
        shutdown.cancel();
    }

    /// Answered at enqueue (acknowledgements off), queued events are still
    /// delivered at shutdown rather than dropped with the queue.
    #[tokio::test]
    async fn shutdown_delivers_what_the_receiver_has_queued() {
        let output = memory_output();
        let (state, metrics) = pipeline_over(&output);
        let config = GrpcConfig::server("127.0.0.1:0").with_vector_compat();
        let transport = GrpcTransport::builder(&config)
            .acknowledgements(scalo::transport::AcknowledgementsConfig::new(false))
            .armed(true)
            .start()
            .await
            .expect("server");
        let addr = transport.local_addr().expect("listening");
        let client = VectorCompatClient::connect_lazy(&format!("http://{addr}")).expect("client");
        for id in 0..3 {
            client
                .send_events(&[serde_json::json!({ "id": id })])
                .await
                .expect("answered at enqueue");
        }

        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let receiver = VectorReceiver {
            sink: ExtractorSink::new(state, Arc::clone(&metrics)),
            metrics: Arc::clone(&metrics),
            default_topic: "vector_land".into(),
            topic_map: HashMap::new(),
        };
        tokio::time::timeout(Duration::from_secs(10), receiver.run(transport, shutdown))
            .await
            .expect("the drain ends once the queue is empty");

        assert_eq!(delivered(&output).await.len(), 3, "every queued event");
    }

    /// Held pushes still waiting at shutdown are delivered by the drain and
    /// answered OK, not left to time out.
    #[tokio::test]
    async fn shutdown_answers_held_pushes_once_the_drain_delivers_them() {
        let output = memory_output();
        let (state, metrics) = pipeline_over(&output);
        let config = GrpcConfig::server("127.0.0.1:0").with_vector_compat();
        let transport = GrpcTransport::builder(&config)
            .armed(true)
            .start()
            .await
            .expect("server");
        let addr = transport.local_addr().expect("listening");

        let sends: Vec<_> = (0..2)
            .map(|id| {
                let endpoint = format!("http://{addr}");
                tokio::spawn(async move {
                    VectorCompatClient::connect_lazy(&endpoint)
                        .expect("client")
                        .send_events(&[serde_json::json!({ "id": id })])
                        .await
                })
            })
            .collect();
        let control = transport.ack_control().expect("a receive server");
        tokio::time::timeout(Duration::from_secs(10), async {
            while control.held().count < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("both pushes held");

        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let receiver = VectorReceiver {
            sink: ExtractorSink::new(state, Arc::clone(&metrics)),
            metrics: Arc::clone(&metrics),
            default_topic: "vector_land".into(),
            topic_map: HashMap::new(),
        };
        tokio::time::timeout(Duration::from_secs(10), receiver.run(transport, shutdown))
            .await
            .expect("the drain ends");

        for send in sends {
            send.await
                .expect("joined")
                .expect("a drained push is answered OK");
        }
        assert_eq!(delivered(&output).await.len(), 2);
    }

    #[test]
    fn test_source_from_topic_strips_the_suffix() {
        assert_eq!(
            source_from_topic("crates_audit_land", "_land"),
            "crates_audit"
        );
    }

    #[test]
    fn test_source_from_topic_keeps_a_topic_without_the_suffix() {
        assert_eq!(source_from_topic("crates_audit", "_land"), "crates_audit");
    }

    #[test]
    fn test_source_from_topic_with_an_empty_suffix() {
        assert_eq!(source_from_topic("crates_audit", ""), "crates_audit");
    }

    #[test]
    fn test_source_from_topic_strips_only_the_trailing_occurrence() {
        assert_eq!(
            source_from_topic("_land_audit_land", "_land"),
            "_land_audit"
        );
    }
}
