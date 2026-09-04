// Project:   dfe-fetcher
// File:      src/extractor/container/mod.rs
// Purpose:   Container-based extractor management (Docker/podman)
// Language:  Rust
//
// License:   BUSL-1.1
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

    /// Spawn a task that logs container stderr (sampled 1/100 to avoid log spam).
    fn spawn_stderr_logger(child: &mut tokio::process::Child, name: String) {
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                use std::sync::atomic::AtomicU64;
                static STDERR_SAMPLES: AtomicU64 = AtomicU64::new(0);

                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if !line.trim().is_empty() && scalo::logger::log_sampled(&STDERR_SAMPLES, 100) {
                        warn!(
                            container = %name,
                            stderr = %line,
                            total = STDERR_SAMPLES.load(std::sync::atomic::Ordering::Relaxed),
                            "Container stderr (sampled 1/100)"
                        );
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
                let fetcher_source = format!("container.{}", self.config.name);
                if let Err(e) = self
                    .pipeline
                    .deliver_ingest(&self.config.topic, &fetcher_source, &topic, payload)
                    .await
                {
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
                                    let fetcher_source = format!("container.{config_name}");
                                    if let Err(e) = pipeline.deliver_ingest(&config_topic, &fetcher_source, &topic, payload).await {
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
                            match self.run_scheduled().await {
                                Ok(()) => self.metrics.inc_extractor_run_for(&name, "success"),
                                Err(e) => {
                                    self.metrics.inc_extractor_run_for(&name, "error");
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

                    match self.run_continuous().await {
                        Ok(()) => {
                            self.metrics.inc_extractor_run_for(&name, "success");
                            info!(name = %name, "Continuous extractor exited normally");
                        }
                        Err(e) => {
                            self.metrics.inc_extractor_run_for(&name, "error");
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

/// Calculate exponential backoff delay for container restart.
///
/// Formula: `min(2^attempt, max_backoff_secs)` seconds.
/// Attempt 1 = 2s, 2 = 4s, 3 = 8s, ... capped at `max_backoff_secs`.
pub fn restart_backoff_secs(attempt: u32, max_backoff_secs: u64) -> u64 {
    (1u64 << attempt.min(6)).min(max_backoff_secs)
}

/// Determine whether backoff counter should reset based on how long the
/// container ran before failing.
pub fn should_reset_backoff(run_duration_secs: u64, stable_after_secs: u64) -> bool {
    run_duration_secs >= stable_after_secs
}

/// Determine whether restart attempts are exhausted.
pub fn restart_exhausted(attempt: u32, max_attempts: u32) -> bool {
    max_attempts > 0 && attempt > max_attempts
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_restart_backoff_exponential() {
        assert_eq!(restart_backoff_secs(1, 60), 2);
        assert_eq!(restart_backoff_secs(2, 60), 4);
        assert_eq!(restart_backoff_secs(3, 60), 8);
        assert_eq!(restart_backoff_secs(4, 60), 16);
        assert_eq!(restart_backoff_secs(5, 60), 32);
        assert_eq!(restart_backoff_secs(6, 60), 60); // capped
        assert_eq!(restart_backoff_secs(7, 60), 60); // still capped
        assert_eq!(restart_backoff_secs(100, 60), 60); // way past cap
    }

    #[test]
    fn test_restart_backoff_low_cap() {
        assert_eq!(restart_backoff_secs(1, 3), 2);
        assert_eq!(restart_backoff_secs(2, 3), 3); // capped at 3
        assert_eq!(restart_backoff_secs(3, 3), 3);
    }

    #[test]
    fn test_should_reset_backoff() {
        assert!(should_reset_backoff(300, 300)); // exactly at threshold
        assert!(should_reset_backoff(301, 300)); // above threshold
        assert!(!should_reset_backoff(299, 300)); // below threshold
        assert!(!should_reset_backoff(0, 300)); // just started
    }

    #[test]
    fn test_restart_exhausted() {
        // max_attempts = 0 means unlimited
        assert!(!restart_exhausted(1, 0));
        assert!(!restart_exhausted(100, 0));

        // max_attempts = 5
        assert!(!restart_exhausted(1, 5));
        assert!(!restart_exhausted(5, 5));
        assert!(restart_exhausted(6, 5));
    }

    #[test]
    fn test_container_name() {
        let config = ContainerExtractorConfig {
            name: "my-tool".to_string(),
            image: "alpine:latest".to_string(),
            ..default_container_config()
        };
        let metrics = Arc::new(Metrics::new());
        let shutdown = CancellationToken::new();
        let pipeline_config = crate::config::Config::default();
        let shared = crate::config::SharedConfig::new(pipeline_config);
        let state = Arc::new(
            PipelineState::new(shared, metrics.clone(), None, CancellationToken::new())
                .expect("pipeline"),
        );
        let ext = ContainerExtractor::new(config, state, metrics, shutdown);
        assert_eq!(ext.container_name(), "dfe-fetcher-my-tool");
    }

    #[test]
    fn test_build_run_args_basic() {
        let config = ContainerExtractorConfig {
            name: "test".to_string(),
            image: "alpine:latest".to_string(),
            command: Some(vec!["echo".to_string(), "hello".to_string()]),
            ..default_container_config()
        };
        let metrics = Arc::new(Metrics::new());
        let shutdown = CancellationToken::new();
        let pipeline_config = crate::config::Config::default();
        let shared = crate::config::SharedConfig::new(pipeline_config);
        let state = Arc::new(
            PipelineState::new(shared, metrics.clone(), None, CancellationToken::new())
                .expect("pipeline"),
        );
        let ext = ContainerExtractor::new(config, state, metrics, shutdown);
        let args = ext.build_run_args();

        assert!(args.contains(&"run".to_string()));
        assert!(args.contains(&"--rm".to_string()));
        assert!(args.contains(&"alpine:latest".to_string()));
        assert!(args.contains(&"echo".to_string()));
        assert!(args.contains(&"hello".to_string()));
        assert!(args.contains(&"managed-by=dfe-fetcher".to_string()));
    }

    #[test]
    fn test_build_run_args_with_resources() {
        let config = ContainerExtractorConfig {
            name: "test".to_string(),
            image: "alpine:latest".to_string(),
            memory_limit: Some("512m".to_string()),
            cpu_limit: Some(1.5),
            network: Some("dfe-net".to_string()),
            ..default_container_config()
        };
        let metrics = Arc::new(Metrics::new());
        let shutdown = CancellationToken::new();
        let pipeline_config = crate::config::Config::default();
        let shared = crate::config::SharedConfig::new(pipeline_config);
        let state = Arc::new(
            PipelineState::new(shared, metrics.clone(), None, CancellationToken::new())
                .expect("pipeline"),
        );
        let ext = ContainerExtractor::new(config, state, metrics, shutdown);
        let args = ext.build_run_args();

        assert!(args.contains(&"--memory".to_string()));
        assert!(args.contains(&"512m".to_string()));
        assert!(args.contains(&"--cpus".to_string()));
        assert!(args.contains(&"1.5".to_string()));
        assert!(args.contains(&"--network".to_string()));
        assert!(args.contains(&"dfe-net".to_string()));
    }

    fn default_container_config() -> ContainerExtractorConfig {
        ContainerExtractorConfig {
            name: String::new(),
            image: String::new(),
            runtime: None,
            mode: "scheduled".to_string(),
            communication: "stdout".to_string(),
            topic: "test".to_string(),
            interval_secs: None,
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
            command: None,
            timeout_secs: None,
            pull_policy: "if-not-present".to_string(),
            max_restart_attempts: 0,
            max_restart_backoff_secs: 60,
            stable_after_secs: 300,
        }
    }
}
