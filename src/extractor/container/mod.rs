// Project:   dfe-fetcher
// File:      src/extractor/container/mod.rs
// Purpose:   Container-based extractor management (Docker/podman)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Container-based extractor management.
//!
//! Manages isolated containers running third-party extraction tools.
//! Each container is a self-contained process that extracts data from
//! an external service and outputs JSON for the fetcher to consume.
//!
//! ## Container Lifecycle
//!
//! 1. Fetcher starts container with environment variables for config
//! 2. Container runs extraction (one-shot or continuous)
//! 3. Container outputs JSON lines to stdout OR posts to fetcher HTTP endpoint
//! 4. Fetcher reads output, wraps as `FetchResult`, delivers to Kafka
//! 5. For one-shot: fetcher re-runs on schedule. For continuous: monitors health.
//!
//! ## Communication Modes
//!
//! - `stdout` — Fetcher reads container stdout as newline-delimited JSON
//! - `http` — Container posts to fetcher's `/ingest/{source}` endpoint
//!
//! ## Example: CloudWatch Exporter
//!
//! ```yaml
//! extractors:
//!   containers:
//!     - name: cloudwatch-metrics
//!       image: ghcr.io/prometheus-community/yet-another-cloudwatch-exporter:latest
//!       mode: scheduled       # one-shot per schedule tick
//!       communication: stdout # read JSON from stdout
//!       topic: aws_cloudwatch_land
//!       env:
//!         AWS_REGION: us-east-1
//!         AWS_ACCESS_KEY_ID: "vault:secret/aws:access_key"
//!       interval_secs: 300
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::ContainerExtractorConfig;
use crate::error::{Error, Result};
use crate::extractor::Extractor;
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;

/// Container-based extractor.
pub struct ContainerExtractor {
    config: ContainerExtractorConfig,
    running: Arc<AtomicBool>,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
}

impl ContainerExtractor {
    /// Create a new container extractor from configuration.
    pub fn new(
        config: ContainerExtractorConfig,
        pipeline: Arc<PipelineState>,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(false)),
            pipeline,
            metrics,
            shutdown,
        }
    }

    /// Get the container runtime command (docker or podman).
    fn runtime_cmd(&self) -> &str {
        self.config.runtime.as_deref().unwrap_or("docker")
    }

    /// Build the container run command arguments.
    fn build_run_args(&self) -> Vec<String> {
        let mut args = vec![
            "run".to_string(),
            "--rm".to_string(),
            "--name".to_string(),
            self.container_name(),
        ];

        // Add environment variables
        for (key, value) in &self.config.env {
            args.push("--env".to_string());
            args.push(format!("{key}={value}"));
        }

        // Add volume mounts
        for mount in &self.config.volumes {
            args.push("-v".to_string());
            args.push(mount.clone());
        }

        // Add network configuration
        if let Some(ref network) = self.config.network {
            args.push("--network".to_string());
            args.push(network.clone());
        }

        // Add resource limits
        if let Some(ref memory) = self.config.memory_limit {
            args.push("--memory".to_string());
            args.push(memory.clone());
        }

        if let Some(cpus) = self.config.cpu_limit {
            args.push("--cpus".to_string());
            args.push(cpus.to_string());
        }

        // Add labels for management
        args.push("--label".to_string());
        args.push("managed-by=dfe-fetcher".to_string());
        args.push("--label".to_string());
        args.push(format!("dfe-fetcher.source={}", self.config.name));

        // Image and optional command
        args.push(self.config.image.clone());
        if let Some(ref cmd) = self.config.command {
            args.extend(cmd.iter().cloned());
        }

        args
    }

    /// Generate a deterministic container name.
    fn container_name(&self) -> String {
        format!("dfe-fetcher-{}", self.config.name)
    }

    /// Run a single scheduled extraction (one-shot container run).
    async fn run_scheduled(&self) -> Result<()> {
        let runtime = self.runtime_cmd().to_string();
        let args = self.build_run_args();

        debug!(
            name = %self.config.name,
            runtime = %runtime,
            "Running scheduled container extraction"
        );

        let mut child = Command::new(&runtime)
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| Error::Source(format!("failed to spawn container '{}': {e}", self.config.name)))?;

        // Read stdout as JSON lines
        if self.config.communication == "stdout" {
            if let Some(stdout) = child.stdout.take() {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                let mut record_count: u64 = 0;

                while let Ok(Some(line)) = lines.next_line().await {
                    let line = line.trim().to_string();
                    if line.is_empty() {
                        continue;
                    }

                    let payload = Bytes::from(line);
                    let topic = format!("{}{}", self.config.topic, self.pipeline.config().kafka.topic_suffix);

                    if let Err(e) = self.pipeline.deliver_ingest(&topic, payload).await {
                        error!(
                            name = %self.config.name,
                            error = %e,
                            "Failed to deliver container output"
                        );
                    } else {
                        record_count += 1;
                    }
                }

                if record_count > 0 {
                    self.metrics.add_extractor_records(record_count);
                    info!(
                        name = %self.config.name,
                        records = record_count,
                        "Scheduled extraction complete"
                    );
                }
            }
        }

        // Wait for container to exit
        let status = child
            .wait()
            .await
            .map_err(|e| Error::Source(format!("container '{}' wait failed: {e}", self.config.name)))?;

        if !status.success() {
            let code = status.code().unwrap_or(-1);
            warn!(
                name = %self.config.name,
                exit_code = code,
                "Container exited with non-zero status"
            );
        }

        Ok(())
    }

    /// Run a continuous container (long-running with stdout streaming).
    async fn run_continuous(&self) -> Result<()> {
        let runtime = self.runtime_cmd().to_string();
        let args = self.build_run_args();

        info!(
            name = %self.config.name,
            runtime = %runtime,
            "Starting continuous container extractor"
        );

        let mut child = Command::new(&runtime)
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| Error::Source(format!("failed to spawn container '{}': {e}", self.config.name)))?;

        // For stdout mode: stream lines continuously
        if self.config.communication == "stdout" {
            if let Some(stdout) = child.stdout.take() {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                let shutdown = self.shutdown.clone();
                let pipeline = self.pipeline.clone();
                let metrics = self.metrics.clone();
                let config_name = self.config.name.clone();
                let config_topic = self.config.topic.clone();

                loop {
                    tokio::select! {
                        line_result = lines.next_line() => {
                            match line_result {
                                Ok(Some(line)) => {
                                    let line = line.trim().to_string();
                                    if line.is_empty() {
                                        continue;
                                    }
                                    let payload = Bytes::from(line);
                                    let topic = format!("{}{}", config_topic, pipeline.config().kafka.topic_suffix);

                                    if let Err(e) = pipeline.deliver_ingest(&topic, payload).await {
                                        error!(
                                            name = %config_name,
                                            error = %e,
                                            "Failed to deliver container output"
                                        );
                                    } else {
                                        metrics.add_extractor_records(1);
                                    }
                                }
                                Ok(None) => {
                                    info!(name = %config_name, "Container stdout closed");
                                    break;
                                }
                                Err(e) => {
                                    error!(name = %config_name, error = %e, "Error reading container stdout");
                                    break;
                                }
                            }
                        }
                        _ = shutdown.cancelled() => {
                            info!(name = %config_name, "Shutdown signal, stopping container");
                            break;
                        }
                    }
                }
            }
        } else {
            // HTTP mode: container posts to our ingest server, just wait
            tokio::select! {
                status = child.wait() => {
                    match status {
                        Ok(s) if !s.success() => {
                            warn!(name = %self.config.name, exit_code = s.code().unwrap_or(-1), "Container exited with error");
                        }
                        Ok(_) => {
                            info!(name = %self.config.name, "Container exited normally");
                        }
                        Err(e) => {
                            error!(name = %self.config.name, error = %e, "Failed to wait for container");
                        }
                    }
                }
                _ = self.shutdown.cancelled() => {
                    info!(name = %self.config.name, "Shutdown signal, stopping container");
                }
            }
        }

        // Clean up: try to stop the container
        let container_name = self.container_name();
        let _ = Command::new(&runtime)
            .args(["stop", "--time", "10", &container_name])
            .output()
            .await;

        Ok(())
    }

    /// Spawn the extractor as a background task with scheduling.
    pub fn spawn(self: Arc<Self>) {
        let is_scheduled = self.config.mode == "scheduled";
        let interval_secs = self.config.interval_secs.unwrap_or(300);
        let name = self.config.name.clone();

        tokio::spawn(async move {
            if is_scheduled {
                // Scheduled mode: run on interval
                let mut interval = tokio::time::interval(
                    std::time::Duration::from_secs(interval_secs),
                );
                // Skip first tick (let startup complete)
                interval.tick().await;

                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            self.running.store(true, Ordering::Relaxed);
                            self.metrics.inc_extractor_runs_total();

                            match self.run_scheduled().await {
                                Ok(()) => self.metrics.inc_extractor_runs_success(),
                                Err(e) => {
                                    self.metrics.inc_extractor_runs_error();
                                    error!(name = %name, error = %e, "Scheduled extraction failed");
                                }
                            }

                            self.running.store(false, Ordering::Relaxed);
                        }
                        _ = self.shutdown.cancelled() => {
                            info!(name = %name, "Scheduled extractor shutting down");
                            break;
                        }
                    }
                }
            } else {
                // Continuous mode: run once, stream until shutdown/exit
                self.running.store(true, Ordering::Relaxed);
                self.metrics.inc_extractor_runs_total();

                match self.run_continuous().await {
                    Ok(()) => self.metrics.inc_extractor_runs_success(),
                    Err(e) => {
                        self.metrics.inc_extractor_runs_error();
                        error!(name = %name, error = %e, "Continuous extractor failed");
                    }
                }

                self.running.store(false, Ordering::Relaxed);
            }
        });
    }
}

#[async_trait]
impl Extractor for ContainerExtractor {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn instance_id(&self) -> &str {
        &self.config.name
    }

    async fn start(&self) -> Result<()> {
        if self.running.load(Ordering::Relaxed) {
            warn!(name = %self.config.name, "Container extractor already running");
            return Ok(());
        }

        info!(
            name = %self.config.name,
            image = %self.config.image,
            mode = %self.config.mode,
            communication = %self.config.communication,
            runtime = self.runtime_cmd(),
            "Starting container extractor"
        );

        // Note: actual spawning is done via `spawn()` which takes Arc<Self>.
        // The Extractor trait start() is used for the initial start signal.
        self.running.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if !self.running.load(Ordering::Relaxed) {
            return Ok(());
        }

        info!(name = %self.config.name, "Stopping container extractor");

        let runtime = self.runtime_cmd().to_string();
        let container_name = self.container_name();

        let output = Command::new(&runtime)
            .args(["stop", "--time", "10", &container_name])
            .output()
            .await
            .map_err(|e| Error::Source(format!("failed to stop container: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(
                name = %self.config.name,
                stderr = %stderr,
                "Failed to stop container cleanly"
            );
        }

        self.running.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.running.load(Ordering::Relaxed) {
            return Ok(false);
        }

        let runtime = self.runtime_cmd().to_string();
        let container_name = self.container_name();

        let output = Command::new(&runtime)
            .args(["inspect", "--format", "{{.State.Running}}", &container_name])
            .output()
            .await
            .map_err(|e| Error::Source(format!("failed to inspect container: {e}")))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.trim() == "true")
    }
}
