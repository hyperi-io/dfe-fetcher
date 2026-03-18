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
//! Supports image pulling, stderr capture, timeouts, and health monitoring.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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

    fn runtime_cmd(&self) -> &str {
        self.config.runtime.as_deref().unwrap_or("docker")
    }

    fn build_run_args(&self) -> Vec<String> {
        let mut args = vec![
            "run".to_string(),
            "--rm".to_string(),
            "--name".to_string(),
            self.container_name(),
        ];

        for (key, value) in &self.config.env {
            args.push("--env".to_string());
            args.push(format!("{key}={value}"));
        }
        for mount in &self.config.volumes {
            args.push("-v".to_string());
            args.push(mount.clone());
        }
        if let Some(ref network) = self.config.network {
            args.push("--network".to_string());
            args.push(network.clone());
        }
        if let Some(ref memory) = self.config.memory_limit {
            args.push("--memory".to_string());
            args.push(memory.clone());
        }
        if let Some(cpus) = self.config.cpu_limit {
            args.push("--cpus".to_string());
            args.push(cpus.to_string());
        }

        args.push("--label".to_string());
        args.push("managed-by=dfe-fetcher".to_string());
        args.push("--label".to_string());
        args.push(format!("dfe-fetcher.source={}", self.config.name));

        args.push(self.config.image.clone());
        if let Some(ref cmd) = self.config.command {
            args.extend(cmd.iter().cloned());
        }
        args
    }

    fn container_name(&self) -> String {
        format!("dfe-fetcher-{}", self.config.name)
    }

    /// Pull the container image per the configured pull policy.
    async fn pull_image(&self) -> Result<()> {
        match self.config.pull_policy.as_str() {
            "never" => return Ok(()),
            "if-not-present" => {
                let status = Command::new(self.runtime_cmd())
                    .args(["image", "inspect", &self.config.image])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .await;
                if let Ok(s) = status
                    && s.success()
                {
                    debug!(image = %self.config.image, "Image exists locally, skipping pull");
                    return Ok(());
                }
            }
            _ => {} // "always" or unknown -> pull
        }

        info!(image = %self.config.image, "Pulling container image");
        let output = Command::new(self.runtime_cmd())
            .args(["pull", &self.config.image])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .map_err(|e| Error::Source(format!("image pull failed: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Source(format!(
                "failed to pull '{}': {stderr}",
                self.config.image
            )));
        }
        Ok(())
    }

    /// Spawn a task that logs container stderr.
    fn spawn_stderr_logger(child: &mut tokio::process::Child, name: String) {
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if !line.trim().is_empty() {
                        warn!(container = %name, stderr = %line, "Container stderr");
                    }
                }
            });
        }
    }

    async fn run_scheduled(&self) -> Result<()> {
        self.pull_image().await?;
        let runtime = self.runtime_cmd().to_string();
        let args = self.build_run_args();

        debug!(name = %self.config.name, "Running scheduled container extraction");

        let mut child = Command::new(&runtime)
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| {
                Error::Source(format!(
                    "failed to spawn container '{}': {e}",
                    self.config.name
                ))
            })?;

        Self::spawn_stderr_logger(&mut child, self.config.name.clone());

        if self.config.communication == "stdout"
            && let Some(stdout) = child.stdout.take()
        {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            let mut record_count: u64 = 0;

            while let Ok(Some(line)) = lines.next_line().await {
                let line = line.trim().to_string();
                if line.is_empty() {
                    continue;
                }
                let payload = Bytes::from(line);
                let topic = format!(
                    "{}{}",
                    self.config.topic,
                    self.pipeline.config().kafka.topic_suffix
                );
                if let Err(e) = self.pipeline.deliver_ingest(&topic, payload).await {
                    error!(name = %self.config.name, error = %e, "Failed to deliver container output");
                } else {
                    record_count += 1;
                }
            }

            if record_count > 0 {
                self.metrics.add_extractor_records(record_count);
                info!(name = %self.config.name, records = record_count, "Scheduled extraction complete");
            }
        }

        // Wait with optional timeout
        let timeout = self
            .config
            .timeout_secs
            .filter(|&t| t > 0)
            .map(std::time::Duration::from_secs);

        let status = if let Some(dur) = timeout {
            match tokio::time::timeout(dur, child.wait()).await {
                Ok(result) => {
                    result.map_err(|e| Error::Source(format!("container wait failed: {e}")))?
                }
                Err(_) => {
                    warn!(name = %self.config.name, timeout_secs = dur.as_secs(), "Container timed out, killing");
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    return Err(Error::Source(format!(
                        "container '{}' timed out after {}s",
                        self.config.name,
                        dur.as_secs()
                    )));
                }
            }
        } else {
            child
                .wait()
                .await
                .map_err(|e| Error::Source(format!("container wait failed: {e}")))?
        };

        if !status.success() {
            warn!(name = %self.config.name, exit_code = status.code().unwrap_or(-1), "Container exited with non-zero status");
        }
        Ok(())
    }

    async fn run_continuous(&self) -> Result<()> {
        self.pull_image().await?;
        let runtime = self.runtime_cmd().to_string();
        let args = self.build_run_args();

        info!(name = %self.config.name, "Starting continuous container extractor");

        let mut child = Command::new(&runtime)
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| {
                Error::Source(format!(
                    "failed to spawn container '{}': {e}",
                    self.config.name
                ))
            })?;

        Self::spawn_stderr_logger(&mut child, self.config.name.clone());

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
                                    if line.is_empty() { continue; }
                                    let payload = Bytes::from(line);
                                    let topic = format!("{}{}", config_topic, pipeline.config().kafka.topic_suffix);
                                    if let Err(e) = pipeline.deliver_ingest(&topic, payload).await {
                                        error!(name = %config_name, error = %e, "Failed to deliver container output");
                                    } else {
                                        metrics.add_extractor_records(1);
                                    }
                                }
                                Ok(None) => { info!(name = %config_name, "Container stdout closed"); break; }
                                Err(e) => { error!(name = %config_name, error = %e, "Error reading container stdout"); break; }
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
            tokio::select! {
                status = child.wait() => {
                    match status {
                        Ok(s) if !s.success() => { warn!(name = %self.config.name, exit_code = s.code().unwrap_or(-1), "Container exited with error"); }
                        Ok(_) => { info!(name = %self.config.name, "Container exited normally"); }
                        Err(e) => { error!(name = %self.config.name, error = %e, "Failed to wait for container"); }
                    }
                }
                _ = self.shutdown.cancelled() => {
                    info!(name = %self.config.name, "Shutdown signal, stopping container");
                }
            }
        }

        let container_name = self.container_name();
        let _ = Command::new(&runtime)
            .args(["stop", "--time", "10", &container_name])
            .output()
            .await;
        Ok(())
    }

    pub fn spawn(self: Arc<Self>) {
        let is_scheduled = self.config.mode == "scheduled";
        let interval_secs = self.config.interval_secs.unwrap_or(300);
        let name = self.config.name.clone();

        tokio::spawn(async move {
            if is_scheduled {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(interval_secs));
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
                let mut attempt = 0u32;
                loop {
                    let start = std::time::Instant::now();
                    self.running.store(true, Ordering::Relaxed);
                    self.metrics.inc_extractor_runs_total();

                    match self.run_continuous().await {
                        Ok(()) => {
                            self.metrics.inc_extractor_runs_success();
                            info!(name = %name, "Continuous extractor exited normally");
                        }
                        Err(e) => {
                            self.metrics.inc_extractor_runs_error();
                            error!(name = %name, error = %e, attempt, "Continuous extractor failed");
                        }
                    }
                    self.running.store(false, Ordering::Relaxed);

                    // Reset backoff if container ran long enough to be considered stable
                    if start.elapsed().as_secs() >= self.config.stable_after_secs {
                        attempt = 0;
                    }

                    attempt += 1;

                    // Check restart limit (0 = unlimited)
                    if self.config.max_restart_attempts > 0
                        && attempt > self.config.max_restart_attempts
                    {
                        error!(
                            name = %name,
                            attempts = attempt,
                            "Restart attempts exhausted, stopping container extractor"
                        );
                        self.metrics.inc_extractor_restart_exhausted();
                        break;
                    }

                    // Exponential backoff: 1s, 2s, 4s, 8s, ... capped at max_restart_backoff_secs
                    let backoff_secs =
                        (1u64 << attempt.min(6)).min(self.config.max_restart_backoff_secs);
                    let backoff = std::time::Duration::from_secs(backoff_secs);

                    info!(
                        name = %name,
                        backoff_secs = backoff.as_secs(),
                        attempt,
                        "Restarting container after backoff"
                    );

                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = self.shutdown.cancelled() => {
                            info!(name = %name, "Shutdown during restart backoff");
                            break;
                        }
                    }
                }
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
        info!(name = %self.config.name, image = %self.config.image, mode = %self.config.mode, "Starting container extractor");
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
            error!(name = %self.config.name, stderr = %stderr, "Failed to stop container cleanly");
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
        let output = Command::new(self.runtime_cmd())
            .args([
                "inspect",
                "--format",
                "{{.State.Running}}",
                &self.container_name(),
            ])
            .output()
            .await
            .map_err(|e| Error::Source(format!("failed to inspect container: {e}")))?;
        Ok(String::from_utf8_lossy(&output.stdout).trim() == "true")
    }
}
