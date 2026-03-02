// Project:   dfe-fetcher
// File:      src/extractor/vector/mod.rs
// Purpose:   Vector.dev integration for data extraction
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector.dev extractor integration.
//!
//! Runs Vector instances as data extractors, receiving data via the native
//! Vector gRPC sink protocol (supported in hyperi-rustlib).
//!
//! ## Architecture
//!
//! Uses hyperi-rustlib's `GrpcTransport` with `vector_compat` enabled to accept
//! Vector's native `PushEvents` gRPC protocol. This allows any Vector instance
//! configured with a `vector` sink to push data directly to the fetcher.
//!
//! ## Modes
//!
//! 1. **Container mode** — Vector runs as a managed container, configured via
//!    a generated `vector.toml`. Data flows: external source -> Vector -> gRPC -> fetcher.
//!
//! 2. **Sidecar mode** — Vector runs alongside the fetcher (e.g., in same pod),
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
//!         topic: syslog_land
//! ```

use std::sync::Arc;

use bytes::Bytes;
use hyperi_rustlib::transport::{GrpcConfig, GrpcTransport, Transport};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::VectorExtractorConfig;
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;

/// Vector extractor manager.
///
/// Manages a gRPC server that accepts Vector protocol events and delivers
/// them to the pipeline. Optionally manages container Vector instances.
pub struct VectorManager {
    config: VectorExtractorConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
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
            pipeline,
            metrics,
            shutdown,
        }
    }

    /// Start the Vector gRPC receiver and managed instances.
    pub async fn start(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        info!(
            grpc_bind = %self.config.grpc_bind_address,
            instances = self.config.instances.len(),
            "Starting Vector extractor manager"
        );

        // Start gRPC server using rustlib's Vector protocol support
        let grpc_config = GrpcConfig::server(&self.config.grpc_bind_address)
            .with_vector_compat();

        let transport = GrpcTransport::new(&grpc_config)
            .await
            .map_err(|e| Error::Source(format!("failed to start Vector gRPC server: {e}")))?;

        info!(
            address = %self.config.grpc_bind_address,
            "Vector gRPC server listening"
        );

        // Start managed container instances
        for instance in &self.config.instances {
            if instance.mode == "container" {
                self.start_vector_container(instance).await;
            }
        }

        // Spawn receiver loop
        let pipeline = self.pipeline.clone();
        let metrics = self.metrics.clone();
        let shutdown = self.shutdown.clone();
        let default_topic = self.default_topic();
        let topic_map = self.build_topic_map();

        tokio::spawn(async move {
            Self::receive_loop(transport, pipeline, metrics, shutdown, default_topic, topic_map).await;
        });

        Ok(())
    }

    /// Background loop receiving Vector events and delivering to pipeline.
    async fn receive_loop(
        transport: GrpcTransport,
        pipeline: Arc<PipelineState>,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
        default_topic: String,
        topic_map: std::collections::HashMap<String, String>,
    ) {
        let mut batch_count: u64 = 0;

        loop {
            tokio::select! {
                result = transport.recv(100) => {
                    match result {
                        Ok(messages) if messages.is_empty() => {
                            // No messages, continue polling
                            continue;
                        }
                        Ok(messages) => {
                            batch_count += 1;
                            let msg_count = messages.len() as u64;

                            for msg in messages {
                                // Determine topic from message key or default
                                let topic = msg.key
                                    .as_ref()
                                    .and_then(|k| topic_map.get(k.as_ref()).cloned())
                                    .or_else(|| msg.key.as_ref().map(|k| k.to_string()))
                                    .unwrap_or_else(|| default_topic.clone());

                                let payload = Bytes::from(msg.payload);

                                if let Err(e) = pipeline.deliver_ingest(&topic, payload).await {
                                    error!(
                                        topic = %topic,
                                        error = %e,
                                        "Failed to deliver Vector message"
                                    );
                                }
                            }

                            metrics.add_extractor_records(msg_count);

                            if batch_count % 100 == 0 {
                                debug!(
                                    batches = batch_count,
                                    "Vector receiver processing"
                                );
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Vector gRPC receive error");
                            // Brief backoff on error
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                }
                _ = shutdown.cancelled() => {
                    info!("Vector receiver shutting down");
                    if let Err(e) = transport.close().await {
                        warn!(error = %e, "Error closing Vector transport");
                    }
                    break;
                }
            }
        }
    }

    /// Build topic mapping from Vector instance configs.
    fn build_topic_map(&self) -> std::collections::HashMap<String, String> {
        let mut map = std::collections::HashMap::new();
        let suffix = &self.pipeline.config().kafka.topic_suffix;

        for instance in &self.config.instances {
            let topic = format!("{}{}", instance.topic, suffix);
            map.insert(instance.name.clone(), topic);
        }

        map
    }

    /// Get the default topic for unmapped Vector events.
    fn default_topic(&self) -> String {
        let suffix = &self.pipeline.config().kafka.topic_suffix;
        format!("vector{suffix}")
    }

    /// Start a managed Vector container instance.
    async fn start_vector_container(&self, instance: &crate::config::VectorInstance) {
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
