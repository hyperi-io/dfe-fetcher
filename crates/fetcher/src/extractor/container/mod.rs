// Project:   dfe-fetcher
// File:      crates/fetcher/src/extractor/container/mod.rs
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::ContainerExtractorConfig;
use crate::error::{Error, Result};
use crate::extractor::{Extractor, ExtractorSink};
use crate::metrics::{ExtractorFailure, Metrics};
use crate::pipeline::PipelineState;

/// How long a stopped or killed container's stdout is read before it is given
/// up on: the grace `docker stop --time 10` gives the container.
const STOP_GRACE: Duration = Duration::from_secs(10);

/// First pause before a held stdout line is sent again; it doubles each time.
const HOLD_PAUSE_MIN: Duration = Duration::from_secs(1);
/// Longest pause between two attempts at a held stdout line.
const HOLD_PAUSE_MAX: Duration = Duration::from_secs(30);
/// Least time between two warnings that a line is still held.
const HOLD_WARN_EVERY_MS: u64 = 60_000;

/// What ended one read of the container's stdout.
enum Next {
    Shutdown,
    TimedOut,
    Line(std::io::Result<Option<String>>),
}

/// What one stdout pump delivered, and whether the run's timeout ended it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Pumped {
    delivered: u64,
    timed_out: bool,
}

/// Resolve at `deadline`, or never when there is none.
async fn until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Container-based extractor.
pub struct ContainerExtractor {
    config: ContainerExtractorConfig,
    running: Arc<AtomicBool>,
    sink: ExtractorSink,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
    /// Lines dropped since start, for the sampled drop log.
    drop_samples: AtomicU64,
    /// When a held line was last warned of, for `log_debounced`.
    held_warned_at: AtomicU64,
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
            sink: ExtractorSink::new(pipeline, Arc::clone(&metrics)),
            metrics,
            shutdown,
            drop_samples: AtomicU64::new(0),
            held_warned_at: AtomicU64::new(0),
        }
    }

    /// Deliver the container's stdout, a JSON record per line, until it
    /// closes, shutdown comes, or `deadline` passes.
    ///
    /// On shutdown the container is stopped while its stdout is still read, so
    /// what it writes on the way out is delivered before the outputs close. At
    /// `deadline` the container is killed, and what it wrote before the kill
    /// is still delivered.
    async fn pump_stdout(
        &self,
        stdout: ChildStdout,
        child: &mut Child,
        deadline: Option<tokio::time::Instant>,
    ) -> Pumped {
        let mut lines = BufReader::new(stdout).lines();
        let mut delivered = 0;
        loop {
            let next = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => Next::Shutdown,
                () = until(deadline) => Next::TimedOut,
                line = lines.next_line() => Next::Line(line),
            };
            match next {
                Next::Shutdown => {
                    info!(
                        name = %self.config.name,
                        "Shutdown: stopping the container and delivering what it still writes"
                    );
                    let ((), ()) = tokio::join!(
                        self.stop_container(),
                        self.drain_stdout(&mut lines, &mut delivered)
                    );
                    return Pumped {
                        delivered,
                        timed_out: false,
                    };
                }
                Next::TimedOut => {
                    self.kill_container(child).await;
                    // Bounded: a pipe some other process still holds open never closes.
                    let _ = tokio::time::timeout(
                        STOP_GRACE,
                        self.drain_stdout(&mut lines, &mut delivered),
                    )
                    .await;
                    return Pumped {
                        delivered,
                        timed_out: true,
                    };
                }
                Next::Line(Ok(Some(line))) => {
                    if self.deliver_counted(&line).await {
                        delivered += 1;
                    }
                }
                Next::Line(Ok(None)) => {
                    debug!(name = %self.config.name, "Container stdout closed");
                    return Pumped {
                        delivered,
                        timed_out: false,
                    };
                }
                Next::Line(Err(e)) => {
                    error!(name = %self.config.name, error = %e, "Error reading container stdout");
                    return Pumped {
                        delivered,
                        timed_out: false,
                    };
                }
            }
        }
    }

    /// Deliver every line left on stdout, until it closes, counting each one
    /// delivered into `delivered`.
    async fn drain_stdout(&self, lines: &mut Lines<BufReader<ChildStdout>>, delivered: &mut u64) {
        while let Ok(Some(line)) = lines.next_line().await {
            if self.deliver_counted(&line).await {
                *delivered += 1;
            }
        }
    }

    /// Kill a container that ran past its timeout: the container itself, then
    /// the runtime client, whose exit closes the stdout pipe.
    async fn kill_container(&self, child: &mut Child) {
        warn!(
            name = %self.config.name,
            timeout_secs = self.config.timeout_secs.unwrap_or_default(),
            "Container timed out, killing it"
        );
        let container_name = self.container_name();
        let _ = Command::new(self.runtime_cmd())
            .args(["kill", &container_name])
            .output()
            .await;
        let _ = child.start_kill();
    }

    /// Deliver one stdout line, counting it received only once it is
    /// delivered.
    ///
    /// A pipe cannot be re-read, so a line the outputs cannot take yet is
    /// held and retried with a growing pause rather than dropped. Nothing
    /// reads the container's stdout meanwhile, so the container blocks on its
    /// own writes until the outputs recover. Only a failure no retry can clear
    /// (no output configured) drops the line, and at shutdown a held line gets
    /// one more attempt before it is dropped. Either way it is counted.
    async fn deliver_counted(&self, line: &str) -> bool {
        let line = line.trim();
        if line.is_empty() {
            return false;
        }
        let mut pause = HOLD_PAUSE_MIN;
        let mut held = false;
        loop {
            let e = match self.deliver_line(line.to_owned()).await {
                Ok(()) => {
                    self.metrics.add_extractor_records(1);
                    if held {
                        info!(name = %self.config.name, "Outputs took the held container line");
                    }
                    return true;
                }
                Err(e) => e,
            };
            let permanent = matches!(e, Error::Config(_));
            if permanent || self.shutdown.is_cancelled() {
                self.metrics.add_extractor_records_failed(
                    "container",
                    ExtractorFailure::Dropped,
                    1,
                );
                if scalo::logger::log_sampled(&self.drop_samples, 100) {
                    error!(
                        name = %self.config.name,
                        error = %e,
                        dropped = self.drop_samples.load(Ordering::Relaxed),
                        "Container output not delivered and dropped: its stdout cannot be re-read (sampled 1/100)"
                    );
                }
                return false;
            }
            if !held || scalo::logger::log_debounced(&self.held_warned_at, HOLD_WARN_EVERY_MS) {
                warn!(
                    name = %self.config.name,
                    error = %e,
                    retry_in_ms = pause.as_millis(),
                    "Outputs cannot take a container line: holding it, and the container's stdout, until they recover"
                );
            }
            held = true;
            tokio::select! {
                () = self.shutdown.cancelled() => {}
                () = tokio::time::sleep(pause) => {}
            }
            pause = pause.saturating_mul(2).min(HOLD_PAUSE_MAX);
        }
    }

    /// Stop the container, giving it 10 s to exit.
    async fn stop_container(&self) {
        let container_name = self.container_name();
        let _ = Command::new(self.runtime_cmd())
            .args(["stop", "--time", "10", &container_name])
            .output()
            .await;
    }

    /// Deliver one stdout line as a record on the extractor's topic.
    async fn deliver_line(&self, line: String) -> Result<()> {
        let topic = format!("{}{}", self.config.topic, self.sink.state().topic_suffix());
        let fetcher_source = format!("container.{}", self.config.name);
        self.sink
            .deliver(
                &self.config.topic,
                &fetcher_source,
                &topic,
                Bytes::from(line),
            )
            .await
    }

    fn runtime_cmd(&self) -> &str {
        self.config.runtime.as_deref().unwrap_or("docker")
    }

    /// The runtime's `run` command: its arguments, and each container
    /// variable resolved through the config's spec resolver and set in the
    /// runtime process's own environment. `--env` names the variable only, so
    /// a resolved secret never appears in the host's process table.
    ///
    /// # Errors
    ///
    /// [`Error::Credential`] naming the variable whose spec does not resolve.
    async fn run_command(&self) -> Result<Command> {
        let mut command = Command::new(self.runtime_cmd());
        command.args(self.build_run_args());
        for (key, spec) in &self.config.env {
            let value = crate::credential::resolve(spec).await.map_err(|e| {
                Error::Credential(format!(
                    "container extractor '{}' env {key}: {e}",
                    self.config.name
                ))
            })?;
            command.env(key, value);
        }
        Ok(command)
    }

    fn build_run_args(&self) -> Vec<String> {
        let mut args = vec![
            "run".to_string(),
            "--rm".to_string(),
            "--name".to_string(),
            self.container_name(),
        ];

        for key in self.config.env.keys() {
            args.push("--env".to_string());
            args.push(key.clone());
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
        let mut command = self.run_command().await?;

        debug!(name = %self.config.name, "Running scheduled container extraction");

        let mut child = command
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

        // One deadline for the whole run, stdout included: a container that
        // keeps its stdout open is still killed on time.
        let deadline = self
            .config
            .timeout_secs
            .filter(|&t| t > 0)
            .map(|t| tokio::time::Instant::now() + Duration::from_secs(t));

        let mut timed_out = false;
        if self.config.communication == "stdout"
            && let Some(stdout) = child.stdout.take()
        {
            let pumped = self.pump_stdout(stdout, &mut child, deadline).await;
            timed_out = pumped.timed_out;
            if pumped.delivered > 0 {
                info!(name = %self.config.name, records = pumped.delivered, "Scheduled extraction complete");
            }
        }

        if !timed_out {
            let exited = match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, child.wait()).await.ok(),
                None => Some(child.wait().await),
            };
            if let Some(exited) = exited {
                let status =
                    exited.map_err(|e| Error::Source(format!("container wait failed: {e}")))?;
                if !status.success() {
                    warn!(name = %self.config.name, exit_code = status.code().unwrap_or(-1), "Container exited with non-zero status");
                }
                return Ok(());
            }
            self.kill_container(&mut child).await;
        }

        let _ = child.wait().await;
        Err(Error::Source(format!(
            "container '{}' timed out after {}s and was killed",
            self.config.name,
            self.config.timeout_secs.unwrap_or_default()
        )))
    }

    async fn run_continuous(&self) -> Result<()> {
        self.pull_image().await?;
        let mut command = self.run_command().await?;

        info!(name = %self.config.name, "Starting continuous container extractor");

        let mut child = command
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
                self.pump_stdout(stdout, &mut child, None).await;
            }
        } else {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    info!(name = %self.config.name, "Shutdown signal, stopping container");
                }
                status = child.wait() => {
                    match status {
                        Ok(s) if !s.success() => { warn!(name = %self.config.name, exit_code = s.code().unwrap_or(-1), "Container exited with error"); }
                        Ok(_) => { info!(name = %self.config.name, "Container exited normally"); }
                        Err(e) => { error!(name = %self.config.name, error = %e, "Failed to wait for container"); }
                    }
                }
            }
        }

        self.stop_container().await;
        Ok(())
    }

    /// Run the extractor on the pipeline's intake tracker, so shutdown keeps
    /// the outputs open while it delivers what its container still writes.
    pub fn spawn(self: Arc<Self>) {
        let is_scheduled = self.config.mode == "scheduled";
        let interval_secs = self.config.interval_secs.unwrap_or(300);
        let name = self.config.name.clone();
        let intake = self.sink.state().intake().clone();
        // A pipe holds no acknowledgement. A container posting to the ingest
        // listener gets that intake's guarantee instead.
        if self.config.communication == "stdout" {
            self.sink.state().publish_guarantee("container", None);
        }

        intake.spawn(async move {
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
                    if self.shutdown.is_cancelled() {
                        break;
                    }

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

    /// The run arguments name each container variable and carry no value, so
    /// nothing configured under `env` reaches the host's process table.
    #[test]
    fn run_args_name_each_variable_and_carry_no_value() {
        let config = ContainerExtractorConfig {
            name: "test".to_string(),
            image: "alpine:latest".to_string(),
            env: HashMap::from([
                ("API_TOKEN".to_string(), "literal-secret".to_string()),
                (
                    "DB_PASSWORD".to_string(),
                    "vault:kv/data/db:password".to_string(),
                ),
            ]),
            ..default_container_config()
        };
        let (_, metrics) = extractor_over(None, CancellationToken::new());
        let state = Arc::new(PipelineState::for_tests(
            crate::config::SharedConfig::new(crate::config::Config::default()),
            Arc::clone(&metrics),
            None,
        ));
        let ext = ContainerExtractor::new(config, state, metrics, CancellationToken::new());
        let args = ext.build_run_args();

        for key in ["API_TOKEN", "DB_PASSWORD"] {
            let at = args
                .iter()
                .position(|a| a == key)
                .unwrap_or_else(|| panic!("{key} is named: {args:?}"));
            assert_eq!(args[at - 1], "--env", "{args:?}");
        }
        for arg in &args {
            assert!(
                !arg.contains("literal-secret")
                    && !arg.contains("vault:")
                    && !arg.starts_with("API_TOKEN=")
                    && !arg.starts_with("DB_PASSWORD="),
                "a value reached the argument list: {arg}"
            );
        }
    }

    /// A launched container gets each variable's resolved value through the
    /// runtime's environment: the stand-in runtime echoes the value it was
    /// handed and its own argument list, and only the variable's name is in
    /// the arguments.
    #[tokio::test]
    #[allow(unsafe_code)]
    async fn a_launched_container_gets_resolved_values_through_the_environment() {
        // SAFETY: test-only; a variable name no other test sets.
        unsafe { std::env::set_var("DFE_FETCHER_TEST_CONTAINER_TOKEN", "s3cret") };
        let output = Arc::new(
            scalo::transport::MemoryTransport::new(&scalo::transport::MemoryConfig::default())
                .expect("memory transport"),
        );
        let metrics = Arc::new(Metrics::new());
        let state = Arc::new(PipelineState::for_tests(
            crate::config::SharedConfig::new(crate::config::Config::default()),
            Arc::clone(&metrics),
            Some(crate::output::OutputManager::memory(Arc::clone(&output))),
        ));
        let dir = tempfile::tempdir().expect("tempdir");
        let runtime = crate::extractor::fake_runtime::write(
            dir.path(),
            "case \"$1\" in\n  run) printf '{\"token\":\"%s\",\"argv\":\"%s\"}\\n' \"$API_TOKEN\" \"$*\" ;;\nesac\n",
        );
        let config = ContainerExtractorConfig {
            name: "envy".to_string(),
            image: "unused".to_string(),
            runtime: Some(runtime),
            pull_policy: "never".to_string(),
            timeout_secs: Some(10),
            env: HashMap::from([(
                "API_TOKEN".to_string(),
                "env:DFE_FETCHER_TEST_CONTAINER_TOKEN".to_string(),
            )]),
            ..default_container_config()
        };
        let ext = ContainerExtractor::new(config, state, metrics, CancellationToken::new());

        let run = ext.run_scheduled().await;
        unsafe { std::env::remove_var("DFE_FETCHER_TEST_CONTAINER_TOKEN") };
        run.expect("the run completes");

        let batch = scalo::transport::TransportReceiver::recv(&*output, 10)
            .await
            .expect("recv");
        assert_eq!(batch.records.len(), 1);
        let row: serde_json::Value =
            serde_json::from_slice(&batch.records[0].payload).expect("json");
        assert_eq!(
            row["token"], "s3cret",
            "the value is resolved and in the environment"
        );
        let argv = row["argv"].as_str().expect("argv");
        assert!(argv.contains("--env API_TOKEN "), "{argv}");
        assert!(
            !argv.contains("s3cret") && !argv.contains("env:"),
            "neither the value nor its spec is in the argument list: {argv}"
        );
    }

    /// A variable whose spec does not resolve fails the launch, naming the
    /// variable, before any container is started.
    #[tokio::test]
    async fn an_unresolvable_container_variable_fails_the_launch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let started = dir.path().join("started");
        let runtime = crate::extractor::fake_runtime::write(
            dir.path(),
            &format!(
                "case \"$1\" in\n  run) touch '{}' ;;\nesac\n",
                started.display()
            ),
        );
        let (_, metrics) = extractor_over(None, CancellationToken::new());
        let state = Arc::new(PipelineState::for_tests(
            crate::config::SharedConfig::new(crate::config::Config::default()),
            Arc::clone(&metrics),
            None,
        ));
        let config = ContainerExtractorConfig {
            name: "unresolved".to_string(),
            image: "unused".to_string(),
            runtime: Some(runtime),
            pull_policy: "never".to_string(),
            env: HashMap::from([(
                "API_TOKEN".to_string(),
                "env:DFE_FETCHER_TEST_CONTAINER_TOKEN_NEVER_SET".to_string(),
            )]),
            ..default_container_config()
        };
        let ext = ContainerExtractor::new(config, state, metrics, CancellationToken::new());

        let err = ext.run_scheduled().await.expect_err("the launch fails");

        assert!(matches!(err, Error::Credential(_)), "{err:?}");
        assert!(err.to_string().contains("API_TOKEN"), "{err}");
        assert!(!started.exists(), "no container was started");
    }

    /// An extractor over a pipeline whose only output is `output` (or none).
    fn extractor_over(
        output: Option<&Arc<scalo::transport::MemoryTransport>>,
        shutdown: CancellationToken,
    ) -> (ContainerExtractor, Arc<Metrics>) {
        let metrics = Arc::new(Metrics::new());
        let state = Arc::new(PipelineState::for_tests(
            crate::config::SharedConfig::new(crate::config::Config::default()),
            Arc::clone(&metrics),
            output.map(|o| crate::output::OutputManager::memory(Arc::clone(o))),
        ));
        let config = ContainerExtractorConfig {
            name: "pump".to_string(),
            image: "unused".to_string(),
            // `true stop ...` exits 0, so no container runtime is touched.
            runtime: Some("true".to_string()),
            ..default_container_config()
        };
        (
            ContainerExtractor::new(config, state, Arc::clone(&metrics), shutdown),
            metrics,
        )
    }

    /// An output outage longer than the emitter's own retries (about 6 s)
    /// loses no line: the line is held, the container's stdout is not read
    /// meanwhile, and the line is delivered once the output takes records
    /// again.
    #[tokio::test(start_paused = true)]
    async fn a_line_is_held_through_an_output_outage_not_dropped() {
        use scalo::transport::{MemoryConfig, MemoryTransport, TransportReceiver, TransportSender};

        let output = Arc::new(
            MemoryTransport::new(&MemoryConfig {
                buffer_size: 1,
                ..MemoryConfig::default()
            })
            .expect("memory transport"),
        );
        // The one slot taken: every send is backpressured until it is read.
        assert!(matches!(
            TransportSender::send(&*output, "filler", Bytes::from_static(b"{}")).await,
            scalo::transport::SendResult::Ok
        ));
        let (ext, metrics) = extractor_over(Some(&output), CancellationToken::new());
        let ext = Arc::new(ext);
        let held = tokio::spawn({
            let ext = Arc::clone(&ext);
            async move { ext.deliver_counted(r#"{"n":1}"#).await }
        });

        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(!held.is_finished(), "still held 30 s into the outage");
        assert_eq!(metrics.extractor_records_failed(), 0, "nothing dropped");

        let filler = output.recv(1).await.expect("recv");
        assert_eq!(filler.records.len(), 1, "the outage ends");
        assert!(
            held.await.expect("joined"),
            "delivered once the output takes records again"
        );
        let batch = output.recv(10).await.expect("recv");
        assert_eq!(batch.records.len(), 1);
        let row: serde_json::Value =
            serde_json::from_slice(&batch.records[0].payload).expect("json");
        assert_eq!(row["n"], 1);
        assert_eq!(metrics.extractor_records_failed(), 0);
    }

    /// A line the outputs refuse cannot be read again from the pipe: it is
    /// counted dropped and never counted received.
    #[tokio::test]
    async fn an_undelivered_line_is_counted_dropped_not_received() {
        let (ext, metrics) = extractor_over(None, CancellationToken::new());

        assert!(!ext.deliver_counted(r#"{"event":"x"}"#).await);
        assert!(!ext.deliver_counted("   ").await, "a blank line is skipped");

        assert_eq!(metrics.extractor_records_failed(), 1);
        assert!(
            metrics
                .render()
                .contains("dfe_fetcher_extractor_records_total 0"),
            "never counted received"
        );
    }

    /// At shutdown the pipe is read to its end, so what the tool writes on
    /// the way out is delivered, not left in the pipe.
    #[tokio::test]
    async fn shutdown_delivers_what_the_container_still_writes() {
        let output = Arc::new(
            scalo::transport::MemoryTransport::new(&scalo::transport::MemoryConfig::default())
                .expect("memory transport"),
        );
        let shutdown = CancellationToken::new();
        let (ext, metrics) = extractor_over(Some(&output), shutdown.clone());

        let mut child = Command::new("sh")
            .args([
                "-c",
                r#"echo '{"n":1}'; sleep 1; echo '{"n":2}'; echo '{"n":3}'"#,
            ])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("sh");
        let stdout = child.stdout.take().expect("stdout");
        shutdown.cancel();

        let pumped = tokio::time::timeout(
            Duration::from_secs(10),
            ext.pump_stdout(stdout, &mut child, None),
        )
        .await
        .expect("the pipe closes");
        let _ = child.wait().await;

        assert_eq!(
            pumped,
            Pumped {
                delivered: 3,
                timed_out: false
            },
            "every line written after the shutdown too"
        );
        assert_eq!(metrics.extractor_records_failed(), 0);
        let batch = scalo::transport::TransportReceiver::recv(&*output, 10)
            .await
            .expect("recv");
        assert_eq!(batch.records.len(), 3);
    }

    /// A container runtime stand-in: `run` writes one line and keeps stdout
    /// open, `kill` records the container it was asked to kill in `killed`.
    fn fake_runtime(dir: &std::path::Path, killed: &std::path::Path) -> String {
        crate::extractor::fake_runtime::write(
            dir,
            &format!(
                "case \"$1\" in\n  run) echo '{{\"n\":1}}'; exec sleep 30 ;;\n  kill) echo \"$2\" >> '{}' ;;\nesac\n",
                killed.display()
            ),
        )
    }

    /// A scheduled container that keeps stdout open is still stopped at its
    /// timeout: the container is killed, the run fails as timed out, and the
    /// line it wrote first is delivered.
    #[tokio::test]
    async fn a_scheduled_container_holding_stdout_open_is_killed_at_its_timeout() {
        let output = Arc::new(
            scalo::transport::MemoryTransport::new(&scalo::transport::MemoryConfig::default())
                .expect("memory transport"),
        );
        let (_, metrics) = extractor_over(None, CancellationToken::new());
        let state = Arc::new(PipelineState::for_tests(
            crate::config::SharedConfig::new(crate::config::Config::default()),
            Arc::clone(&metrics),
            Some(crate::output::OutputManager::memory(Arc::clone(&output))),
        ));
        let dir = tempfile::tempdir().expect("tempdir");
        let killed = dir.path().join("killed");
        let config = ContainerExtractorConfig {
            name: "stuck".to_string(),
            image: "unused".to_string(),
            runtime: Some(fake_runtime(dir.path(), &killed)),
            pull_policy: "never".to_string(),
            timeout_secs: Some(1),
            ..default_container_config()
        };
        let ext = ContainerExtractor::new(config, state, metrics, CancellationToken::new());

        let started = std::time::Instant::now();
        let err = tokio::time::timeout(Duration::from_secs(15), ext.run_scheduled())
            .await
            .expect("the run ends at its timeout, not when stdout closes")
            .expect_err("a timed-out run fails");

        assert!(err.to_string().contains("timed out"), "{err}");
        assert!(started.elapsed() >= Duration::from_secs(1));
        assert_eq!(
            std::fs::read_to_string(&killed)
                .expect("the container was killed")
                .trim(),
            "dfe-fetcher-stuck"
        );
        let batch = scalo::transport::TransportReceiver::recv(&*output, 10)
            .await
            .expect("recv");
        assert_eq!(batch.records.len(), 1, "the line written before the kill");
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
