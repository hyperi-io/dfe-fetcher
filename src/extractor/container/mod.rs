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
use tracing::{error, info, warn};

use crate::config::ContainerExtractorConfig;
use crate::error::{Error, Result};
use crate::extractor::Extractor;

/// Container-based extractor.
pub struct ContainerExtractor {
    config: ContainerExtractorConfig,
    running: Arc<AtomicBool>,
}

impl ContainerExtractor {
    /// Create a new container extractor from configuration.
    pub fn new(config: ContainerExtractorConfig) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(false)),
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
            runtime = self.runtime_cmd(),
            "Starting container extractor"
        );

        // TODO: Spawn container process and set up stdout/HTTP communication
        // For stdout mode: read lines from child process stdout
        // For HTTP mode: container posts to our /ingest endpoint

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

        // Send stop signal to container
        let output = tokio::process::Command::new(&runtime)
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

        // Check if container is still running via runtime inspect
        let runtime = self.runtime_cmd().to_string();
        let container_name = self.container_name();

        let output = tokio::process::Command::new(&runtime)
            .args(["inspect", "--format", "{{.State.Running}}", &container_name])
            .output()
            .await
            .map_err(|e| Error::Source(format!("failed to inspect container: {e}")))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.trim() == "true")
    }
}
