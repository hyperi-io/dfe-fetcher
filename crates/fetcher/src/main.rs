// Project:   dfe-fetcher
// File:      crates/fetcher/src/main.rs
// Purpose:   CLI entry point and runtime initialisation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-fetcher CLI entry point.
//!
//! Uses the scalo CLI module for standard arguments and subcommands.
//! Implements the [`ServiceApp`] trait for the standard DFE service lifecycle.

// Jemalloc -- DFE allocator policy 2026-04-17 (jemalloc only at every channel).
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use scalo::cli::{CliError, CommonArgs, ServiceApp, ServiceRuntime, StandardCommand, VersionInfo};
use scalo::config::reloader::{ConfigReloader, ReloaderConfig};
use scalo::deployment::{generate_compose_fragment, generate_dockerfile};
use scalo::logger::security;
use scalo::scaling::ScalingComponent;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use dfe_fetcher::config::{Config, derive_instance_id, reload_config, unset_instance_id_warning};
use dfe_fetcher::cursor;
use dfe_fetcher::deployment;
use dfe_fetcher::driver::{Driver, DriverParts, Shape};
use dfe_fetcher::emit::Emitter;
use dfe_fetcher::extractor::container::ContainerExtractor;
use dfe_fetcher::extractor::vector::VectorManager;
use dfe_fetcher::ingest;
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::pipeline::{IntakeAcks, Orchestrator, PipelineState};
use dfe_fetcher::scheduler::{Planned, Scheduler, SourceFactory, fingerprint};
use dfe_fetcher_db::{DbInstance, DbShape};
use dfe_fetcher_file::{FileInstance, FileShape};
use dfe_fetcher_rest::{ExchangeClient, HttpClient, RestInstance};
use scalo::governor::{MemoryPressureSource, PressureSource, UnifiedPressure};
use scalo::memory::MemoryGuard;

/// dfe-fetcher: Data fetcher for external services (AWS, Azure, M365, GCP).
#[derive(Parser, Debug)]
#[command(name = "dfe-fetcher")]
#[command(version, about, long_about = None)]
struct App {
    /// Standard CLI arguments (config, log-level, log-format, metrics-addr, verbose, quiet).
    #[command(flatten)]
    common: CommonArgs,

    /// Subcommand (defaults to `run` if omitted).
    #[command(subcommand)]
    command: Option<AppCommand>,
}

/// Application subcommands.
///
/// Standard commands (`run`, `version`, `config-check`, `generate-artefacts`,
/// `metrics-manifest`, `top`) are flattened from scalo's [`StandardCommand`].
/// Local extensions handle only the legacy emit-* shortcuts.
#[derive(Subcommand, Clone, Debug)]
enum AppCommand {
    /// Standard scalo commands (run, version, config-check, generate-artefacts, metrics-manifest).
    #[command(flatten)]
    Standard(StandardCommand),

    /// Generate Dockerfile to stdout (legacy shortcut; prefer `generate-artefacts`).
    #[command(name = "emit-dockerfile")]
    EmitDockerfile,

    /// Generate Docker Compose fragment to stdout (legacy shortcut; prefer `generate-artefacts`).
    #[command(name = "emit-compose")]
    EmitCompose,

    /// Print deployment contract as JSON to stdout (legacy shortcut; prefer `generate-artefacts`).
    #[command(name = "emit-contract")]
    EmitContract,
}

impl ServiceApp for App {
    type Config = Config;

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "dfe-fetcher"
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn env_prefix(&self) -> &str {
        "DFE_FETCHER"
    }

    fn version_info(&self) -> VersionInfo {
        VersionInfo::new("dfe-fetcher", env!("CARGO_PKG_VERSION"))
    }

    fn common_args(&self) -> &CommonArgs {
        &self.common
    }

    fn command(&self) -> Option<&StandardCommand> {
        match &self.command {
            Some(AppCommand::Standard(cmd)) => Some(cmd),
            _ => None,
        }
    }

    /// Not ready until `run_service` installs the pipeline's check.
    fn readiness_check(&self, _config: &Self::Config) -> Option<scalo::metrics::ReadinessFn> {
        Some(Arc::new(|| false))
    }

    fn load_config(&self, path: Option<&str>) -> Result<Config, CliError> {
        let config =
            Config::load(path).map_err(|e| CliError::Config(format!("failed to load: {e}")))?;
        config
            .validate()
            .map_err(|e| CliError::Config(format!("validation failed: {e}")))?;
        // `config-check` is what an operator runs before a deploy, and `run`
        // resolves env:/vault: specs before it starts. Resolve them here too,
        // on a copy, or a config that cannot resolve passes the check and then
        // kills `run`. The lifecycle loads the config once, so this cannot be
        // a second load ahead of it.
        if matches!(self.command(), Some(StandardCommand::ConfigCheck)) {
            let mut resolved = config.clone();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(
                    dfe_fetcher::config::resolve::resolve_config_specs(&mut resolved),
                )
            })
            .map_err(|e| CliError::Config(format!("a spec does not resolve: {e}")))?;
        }
        Ok(config)
    }

    /// The fetcher has work when something can produce records: an enabled
    /// source, a container extractor, the Vector extractor's gRPC receiver, or
    /// the ingest listener. `Config::validate` reads the same predicate, so a
    /// config that idles here is never refused there for want of the transport
    /// it will not use.
    ///
    /// The reason names every listener the gate counts, because an operator
    /// reads it to work out which one they forgot to enable.
    fn work_state(&self, config: &Config) -> scalo::lifecycle::WorkState {
        scalo::lifecycle::WorkState::idle_if(
            !config.has_work(),
            "no enabled sources, container extractors, vector receiver or ingest listener",
        )
    }

    async fn run_service(&self, config: Config, runtime: ServiceRuntime) -> Result<(), CliError> {
        // Box::pin keeps the run_service future small (21KB+ otherwise);
        // run_fetcher_service stack-allocates large state.
        Box::pin(run_fetcher_service(&self.common, config, runtime))
            .await
            .map_err(|e| CliError::Service(e.to_string()))
    }

    fn register_metrics(&self, manager: &scalo::metrics::MetricsManager) {
        // `metrics-manifest` and `generate-artefacts` read the registry without
        // starting the service, so the catalogue stays empty until the
        // fetcher's own metrics are built against their manager.
        let _ = Metrics::with_dfe(manager);
    }

    fn deployment_contract(&self) -> Option<scalo::deployment::DeploymentContract> {
        Some(crate::deployment::contract())
    }

    fn version_check_defaults(&self) -> scalo::version_check::VersionCheckConfig {
        // The runtime overlays the version_check cascade keys on this, so a
        // deployment's explicit enabled: false always wins.
        scalo::version_check::VersionCheckConfig {
            api_url: "https://releases.hyperi.io/api/v1/check".into(),
            ..Default::default()
        }
    }
}

/// Carry `metrics.address` from an explicit `--config` file onto the runtime's
/// metrics bind address.
///
/// scalo resolves that address from its cascade, which takes the file but not
/// the flat `DFE_FETCHER_METRICS_ADDRESS` override `apply_flat_env` maps, so
/// that override reaches the listener only through here.
///
/// `--metrics-addr` / `METRICS_ADDR` still win: this only fills an unset value.
fn apply_config_metrics_addr(app: &mut App) {
    if app.common.metrics_addr.is_some() {
        return;
    }
    let Some(path) = app.common.config.clone() else {
        // No --config: scalo's cascade is populated and resolves it already.
        return;
    };
    // A load failure is not reported here -- the lifecycle loads the same file
    // a moment later and reports it properly.
    if let Ok(config) = Config::load_from_file(&path)
        && !config.metrics.address.is_empty()
    {
        app.common.metrics_addr = Some(config.metrics.address);
    }
}

#[tokio::main]
async fn main() {
    let mut app = App::parse();
    apply_config_metrics_addr(&mut app);

    // Handle non-standard subcommands locally before entering the ServiceApp lifecycle
    // (these don't need config or logging). Standard subcommands fall through to run_app.
    if let Some(ref cmd) = app.command {
        match cmd {
            AppCommand::EmitDockerfile => {
                let contract = deployment::contract();
                // Identity = None: stdout shortcut for local inspection
                // intentionally omits Contract Identity Annotation labels.
                // The full ci/ pipeline (`generate-artefacts`) is where
                // identity gets stamped in once scalo wires it.
                match generate_dockerfile(&contract, None) {
                    Ok(dockerfile) => println!("{dockerfile}"),
                    Err(e) => {
                        eprintln!("fatal: the deployment contract is invalid: {e}");
                        std::process::exit(1);
                    }
                }
                return;
            }
            AppCommand::EmitCompose => {
                let contract = deployment::contract();
                match generate_compose_fragment(&contract) {
                    Ok(compose) => println!("{compose}"),
                    Err(e) => {
                        eprintln!("fatal: the deployment contract is invalid: {e}");
                        std::process::exit(1);
                    }
                }
                return;
            }
            AppCommand::EmitContract => {
                match deployment::contract().to_json() {
                    Ok(json) => println!("{json}"),
                    Err(e) => {
                        eprintln!("fatal: {e}");
                        std::process::exit(1);
                    }
                }
                return;
            }
            AppCommand::Standard(_) => {
                // fall through to run_app (handles run / version / config-check /
                // generate-artefacts / metrics-manifest / top).
            }
        }
    }

    // Delegate to standard ServiceApp lifecycle (logging -> config -> run_service)
    // Box::pin avoids a large-future clippy lint on the run_app async fn.
    if let Err(e) = Box::pin(scalo::cli::run_app(app)).await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

/// What one connection's driver is built from: its own block, and the
/// deployment-wide settings the driver bakes in.
#[derive(serde::Serialize)]
struct SourceSpec {
    kind: SourceKind,
    in_flight: usize,
}

/// The block one connection is configured by.
#[derive(serde::Serialize)]
enum SourceKind {
    Rest(Box<RestInstance>),
    Db(Box<DbInstance>),
    File(Box<FileInstance>),
}

/// Every connection `config` schedules, one per connection id: the typed
/// blocks that are shipped profiles underneath (`sources.github`,
/// `sources.okta`, ...), then `sources.rest`, `sources.db` and `sources.file`.
fn plan_sources(config: &Config) -> anyhow::Result<Vec<Planned<SourceSpec>>> {
    let in_flight = config.accumulate.in_flight;
    let planned = |id: String, interval_secs: Option<u64>, kind: SourceKind| {
        let spec = SourceSpec { kind, in_flight };
        anyhow::Ok(Planned {
            fingerprint: fingerprint(&spec)?,
            id,
            interval_secs,
            spec,
        })
    };
    let rest = config
        .sources
        .builtin_instances()?
        .into_iter()
        .map(|b| (b.connection_id, b.instance))
        .chain(
            config
                .sources
                .rest
                .iter()
                .filter(|(_, instance)| instance.enabled)
                .map(|(id, instance)| (id.clone(), instance.clone())),
        );
    let mut out = Vec::new();
    for (id, instance) in rest {
        let interval = instance.interval_secs;
        out.push(planned(id, interval, SourceKind::Rest(Box::new(instance)))?);
    }
    for (id, instance) in config.sources.db.iter().filter(|(_, i)| i.enabled) {
        let kind = SourceKind::Db(Box::new(instance.clone()));
        out.push(planned(id.clone(), instance.interval_secs, kind)?);
    }
    for (id, instance) in config.sources.file.iter().filter(|(_, i)| i.enabled) {
        let kind = SourceKind::File(Box::new(instance.clone()));
        out.push(planned(id.clone(), instance.interval_secs, kind)?);
    }
    Ok(out)
}

/// Builds every connection's driver over the process's one pipeline, memory
/// guard, checkpoint store and HTTP clients.
struct DriverFactory {
    instance_id: String,
    pipeline_state: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    pressure: Option<Arc<UnifiedPressure>>,
    checkpoints: Option<Arc<dyn cursor::CursorStore>>,
    http_client: HttpClient,
    exchange_client: Arc<ExchangeClient>,
}

impl SourceFactory for DriverFactory {
    type Spec = SourceSpec;

    fn plan(&self, config: &Config) -> anyhow::Result<Vec<Planned<SourceSpec>>> {
        plan_sources(config)
    }

    /// The driver takes its source's OWN cancel token, so dropping the source
    /// from the config aborts its in-flight tick through the driver's
    /// non-alerting shutdown path.
    fn build(
        &self,
        id: &str,
        spec: SourceSpec,
        cancel: CancellationToken,
    ) -> anyhow::Result<Driver> {
        // Every block a cursor fetches and every chunk a reader holds is
        // leased on the memory guard until its rows are out.
        let lease: Arc<dyn dfe_fetcher_core::batch::Lease> = Arc::new(
            dfe_fetcher::driver::GuardLease(Arc::clone(self.pipeline_state.memory_guard())),
        );
        let shape = match spec.kind {
            SourceKind::Rest(instance) => {
                let profile = dfe_fetcher_rest::profile::bound::resolve_profile(
                    &instance,
                    dfe_fetcher::profiles::shipped(),
                )?;
                Shape::for_rest_instance(
                    &profile,
                    &instance,
                    id,
                    self.http_client.clone(),
                    &self.exchange_client,
                )?
            }
            SourceKind::Db(instance) => Shape::Db(Box::new(
                DbShape::from_instance(&instance, id, &lease)
                    .map_err(|e| anyhow::anyhow!("sources.db.{id}: {e}"))?,
            )),
            SourceKind::File(instance) => Shape::File(Box::new(
                FileShape::from_instance(&instance, id, &lease)
                    .map_err(|e| anyhow::anyhow!("sources.file.{id}: {e}"))?,
            )),
        };
        // A scheduled source's cursor advances only once its records are delivered.
        self.pipeline_state.publish_guarantee(
            "scheduled",
            Some(&IntakeAcks {
                kind: scalo::transport::AckKind::Pull,
                enabled: true,
            }),
        );
        Ok(Driver::new(DriverParts {
            shape,
            connection_id: id.to_owned(),
            instance_id: self.instance_id.clone(),
            shared_config: self.pipeline_state.shared_config(),
            emitter: Emitter::new(
                Arc::clone(&self.pipeline_state),
                Arc::clone(&self.metrics),
                spec.in_flight,
            ),
            pressure: self.pressure.clone(),
            memory_guard: Arc::clone(self.pipeline_state.memory_guard()),
            checkpoints: self.checkpoints.clone(),
            metrics: Arc::clone(&self.metrics),
            shutdown: cancel,
        }))
    }
}

/// Main service loop -- called by the ServiceApp lifecycle after logging and config.
async fn run_fetcher_service(
    _common: &CommonArgs,
    config: Config,
    mut runtime: ServiceRuntime,
) -> anyhow::Result<()> {
    let mut config = config;

    // Resolve env:/vault: spec strings on opt-in config fields before
    // anyone reads them. See `config::resolve` for the spec syntax.
    dfe_fetcher::config::resolve::resolve_config_specs(&mut config).await?;

    // Warn if deprecated plugin config is present
    config.extractors.plugins.warn_if_configured();

    // Warn if using legacy kafka: config section instead of output.kafka:
    if !config.kafka.brokers.is_empty() && config.output.kafka.is_none() {
        warn!(
            "Using legacy kafka: config section -- migrate to output.kafka: (scalo KafkaConfig format)"
        );
    }

    // Register config in the global config registry (enables /config debug endpoint)
    config.register_in_registry();

    // Log startup configuration at debug level for observability
    debug!(
        sources_enabled = config.sources.any_enabled(),
        scheduler_interval_secs = config.scheduler.default_interval_secs,
        scheduler_jitter_pct = config.scheduler.jitter_percent,
        scheduler_max_concurrent = config.scheduler.max_concurrent_fetches,
        output_type = %config.output.output_type,
        output_topic_suffix = config.topic_suffix(),
        cursor_dir = %config.cursor.directory,
        cursor_window_hours = config.cursor.default_window_hours,
        dlq_enabled = config.dlq.enabled,
        config_reload_secs = config.config_reload_secs,
        "Startup configuration"
    );
    // Use ServiceRuntime's pre-wired MetricsManager (already started, serving /metrics)
    // and shutdown token (signal handler already installed with K8s pre-stop delay).
    let shutdown_token = runtime.shutdown.clone();

    // The fetcher's own metric descriptions go to the global recorder, and the
    // service metric set to the runtime's manager.
    let metrics = Arc::new(Metrics::with_dfe(&runtime.metrics));

    // Seed the self-normalised fetch-pressure denominator (scheduler semaphore
    // size). Lets the scaling engine read `active_fetches / concurrency_cap`
    // without an unguessable per-env target.
    metrics.set_concurrency_cap(config.scheduler.max_concurrent_fetches);

    // Create and run the pipeline orchestrator
    // Box::pin: the future holds a whole Config, which is past clippy's
    // large-future threshold.
    let orchestrator = Box::pin(Orchestrator::new(
        config.clone(),
        metrics.clone(),
        shutdown_token.clone(),
    ))
    .await?;
    let pipeline_state = orchestrator.state();

    // Build scaling pressure calculator for KEDA autoscaling with fetcher-specific components.
    // The ServiceRuntime's scaling field (if any) uses generic components; we create one
    // with fetcher-specific weights for buffer depth, transport errors, and memory.
    let scaling_pressure = Arc::new(scalo::scaling::ScalingPressure::new(
        config.scaling.clone(),
        scaling_components(),
    ));

    // Start config hot-reload
    // Keep handle alive for entire application lifetime (dropping stops the reloader)
    let _reloader_handle = {
        let config_path_str = config.config_path.clone();
        let shared_config = orchestrator.shared_config();

        let reloader_config = ReloaderConfig {
            config_path: config.config_path.as_ref().map(PathBuf::from),
            poll_interval: Duration::from_secs(config.config_reload_secs.max(5)),
            periodic_interval: if config.config_reload_secs > 0 {
                Duration::from_secs(config.config_reload_secs)
            } else {
                Duration::ZERO
            },
            debounce: Duration::from_millis(500),
            enable_sighup: true,
        };

        let reloader = ConfigReloader::new(
            reloader_config,
            shared_config.clone(),
            move || reload_config_from_path(config_path_str.as_deref()),
            |cfg| {
                cfg.validate()
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            },
        )
        .with_registry_update("dfe_fetcher");

        let handle = reloader.start();

        if config.config_reload_secs > 0 {
            info!(
                interval_secs = config.config_reload_secs,
                "Config hot-reload enabled (SIGHUP + periodic + file polling)"
            );
        } else {
            info!("Config hot-reload enabled (SIGHUP + file polling)");
        }

        handle
    };

    // Spawn periodic scaling pressure update into the `ScalingPressure` engine
    // served at `/scaling/pressure`.
    // We feed that single engine its weighted components (buffer depth + memory)
    // and the outbound-circuit gate. The fetcher's domain signals
    // (fetch_pressure / throttle_ratio) remain emitted as gauges by the metrics
    // module for direct Prometheus/KEDA scraping; they are no longer pushed to a
    // separate engine cell (that cell no longer exists). See update_rate_gauge.
    {
        let scaling = Arc::clone(&scaling_pressure);
        let state = Arc::clone(&pipeline_state);
        let engine_metrics = Arc::clone(&metrics);
        let shutdown = shutdown_token.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mg = state.memory_guard();
                        let used = mg.current_bytes();
                        let limit = mg.limit_bytes();
                        let circuit_open = !state.output_healthy();
                        // Canonical weighted pressure engine (served to KEDA).
                        scaling.set_component("buffer_depth", buffer_depth(used, limit));
                        scaling.set_memory(used, limit);
                        scaling.set_circuit_open(circuit_open);
                        // Keep the per-tick scaling gauges current for KEDA's
                        // Prometheus scaler (fetch_pressure / throttle_ratio).
                        engine_metrics.update_rate_gauge();
                    }
                    () = shutdown.cancelled() => break,
                }
            }
        });
    }

    // Wire readiness and scaling into ServiceRuntime's MetricsManager.
    // The server is already started by run_app() -- we only add the readiness
    // check and scaling pressure callbacks here.
    {
        let ready_state = Arc::clone(&pipeline_state);
        // probe_ready, not is_ready: the probe must not fail this pod for an
        // output outage every replica shares, which would also stall a rollout.
        runtime
            .metrics
            .set_readiness_check(move || ready_state.probe_ready());
        runtime
            .metrics
            .set_scaling_pressure(Arc::clone(&scaling_pressure));
        debug!("Metrics readiness and scaling pressure wired");
    }

    // Derive instance identity for cursor isolation
    let instance_id = derive_instance_id(&config);
    info!(instance_id, "Fetcher instance identity");
    if let Some(message) = unset_instance_id_warning(&config) {
        warn!(instance_id, "{message}");
    }

    // Resolve cursor directory: explicit config > config file's parent > current dir
    let cursor_dir = if config.cursor.directory.is_empty() {
        let fallback = config
            .config_path
            .as_ref()
            .and_then(|p| {
                std::path::Path::new(p)
                    .parent()
                    .map(|d| d.to_string_lossy().to_string())
            })
            .unwrap_or_else(|| ".".to_string());
        warn!(
            fallback_dir = %fallback,
            "cursor.directory not set -- falling back to config file directory. \
             Set cursor.directory to a PVC-backed path for pod restart persistence."
        );
        fallback
    } else {
        config.cursor.directory.clone()
    };

    // Create cursor store for incremental fetching
    let cursor_store: Option<Arc<dyn cursor::CursorStore>> =
        match cursor::file::FileCursorStore::new(&cursor_dir) {
            Ok(store) => {
                info!(directory = %cursor_dir, "Cursor store initialised");
                Some(Arc::new(store))
            }
            Err(e) => {
                warn!(error = %e, "Cursor store unavailable, fetches will use default lookback window");
                None
            }
        };

    // Create scheduler (reads interval/jitter/window_hours from shared_config per tick)
    let scheduler = Scheduler::new(
        &config.scheduler,
        orchestrator.shared_config(),
        cursor_store.clone(),
        instance_id.clone(),
    );

    // One Driver per connection, all sharing the memory guard's pressure
    // latch so a buffer that grows anywhere pauses polling everywhere.
    let pressure = build_pressure(&config, pipeline_state.memory_guard())?;
    let factory = DriverFactory {
        instance_id: instance_id.clone(),
        pipeline_state: Arc::clone(&pipeline_state),
        metrics: Arc::clone(&metrics),
        pressure: pressure.clone(),
        // Framework drivers commit unit checkpoints to the same store the
        // scheduler keeps its window cursors in.
        checkpoints: cursor_store,
        http_client: dfe_fetcher_rest::http_client()?,
        // One exchange client for the whole process: every instance's
        // credential mode posts its token exchange through it.
        exchange_client: dfe_fetcher_rest::exchange_client()?,
    };
    let ready_state = Arc::clone(&pipeline_state);
    let sources = scheduler.start_sources(
        factory,
        &config,
        Arc::clone(&metrics),
        Arc::new(move || ready_state.is_ready()),
        shutdown_token.clone(),
    )?;

    // Start ingest HTTP server (for container extractors using HTTP
    // communication). A listener that cannot start leaves the intake dead
    // while the pod reads Ready, so the process drains, stops and exits
    // non-zero to be restarted.
    let mut intake_failed: Option<anyhow::Error> = None;
    let ingest_server = match ingest::spawn_ingest_server(
        &config.ingest,
        Arc::clone(&pipeline_state),
        Arc::clone(&metrics),
        shutdown_token.clone(),
    )
    .await
    {
        Ok(server) => server,
        Err(e) => {
            error!(error = %e, "The ingest listener failed to start: shutting down to exit non-zero");
            shutdown_token.cancel();
            intake_failed = Some(e);
            None
        }
    };

    // Start container extractors
    for container_config in &config.extractors.containers {
        let extractor = Arc::new(ContainerExtractor::new(
            container_config.clone(),
            Arc::clone(&pipeline_state),
            Arc::clone(&metrics),
            shutdown_token.clone(),
        ));

        info!(
            name = %container_config.name,
            image = %container_config.image,
            mode = %container_config.mode,
            communication = %container_config.communication,
            "Starting container extractor"
        );

        extractor.spawn();
    }

    // Start Vector manager
    let vector_manager = VectorManager::new(
        config.extractors.vector.clone(),
        Arc::clone(&pipeline_state),
        Arc::clone(&metrics),
        shutdown_token.clone(),
    )
    .with_pressure(pressure.clone());
    // A listener that cannot bind leaves the intake dead while the pod reads
    // Ready, so the process drains, stops and exits non-zero to be restarted.
    let vector_failed = if vector_manager.is_enabled() {
        vector_manager.start().await.err()
    } else {
        None
    };
    if let Some(e) = &vector_failed {
        error!(
            error = %e,
            "The Vector extractor failed to start: shutting down to exit non-zero"
        );
        shutdown_token.cancel();
    }

    // Run pipeline orchestrator (blocks until shutdown)
    let run = orchestrator.run().await;

    // The fetch tasks end on the same shutdown. Waiting for them, and for
    // every driver still stopping, writes each file tailer's final checkpoint
    // before the runtime goes.
    sources.shutdown().await;

    // Before any exit: process::exit skips drops, leaving the managed Vector
    // containers running and their inline configs on disk.
    if let Err(e) = vector_manager.stop().await {
        warn!(error = %e, "Error stopping Vector manager");
    }
    if let Err(e) = run {
        error!(error = %e, "Pipeline error");
        std::process::exit(1);
    }
    if let Some(e) = vector_failed {
        return Err(anyhow::anyhow!("the Vector extractor failed to start: {e}"));
    }
    if let Some(server) = ingest_server {
        match server.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => intake_failed = intake_failed.or(Some(e)),
            Err(e) => {
                intake_failed = intake_failed.or(Some(anyhow::anyhow!("ingest server task: {e}")));
            }
        }
    }
    if let Some(e) = intake_failed {
        return Err(e);
    }

    info!("Shutdown complete");
    Ok(())
}

/// The fetcher's scaling components, each read as a 0-1 fraction of its saturation.
fn scaling_components() -> Vec<ScalingComponent> {
    vec![
        ScalingComponent::new("buffer_depth", 0.40, 1.0),
        ScalingComponent::new("transport_errors", 0.30, 100.0),
        ScalingComponent::new("memory", 0.30, 1.0),
    ]
}

/// The share of the memory guard's limit the buffered bytes hold, 0 when it has no limit.
fn buffer_depth(used: u64, limit: u64) -> f64 {
    if limit == 0 {
        return 0.0;
    }
    used as f64 / limit as f64
}

/// The pressure latch framework drivers gate on: the memory guard as the one
/// HARD source under the configured hysteresis, or `None` when the brake is
/// off.
fn build_pressure(
    config: &Config,
    guard: &Arc<MemoryGuard>,
) -> anyhow::Result<Option<Arc<UnifiedPressure>>> {
    let Some(hysteresis) = config.self_regulation.hysteresis()? else {
        info!("self-regulation brake disabled; framework sources never pause for memory pressure");
        return Ok(None);
    };
    let sources: Vec<Arc<dyn PressureSource>> =
        vec![Arc::new(MemoryPressureSource::new(Arc::clone(guard))) as Arc<dyn PressureSource>];
    info!(
        pause_above = hysteresis.pause_above,
        resume_below = hysteresis.resume_below,
        "self-regulation brake armed for framework sources"
    );
    Ok(Some(Arc::new(UnifiedPressure::new(sources, hysteresis))))
}

/// Reload configuration from the original config path.
fn reload_config_from_path(
    config_path: Option<&str>,
) -> std::result::Result<Config, Box<dyn std::error::Error + Send + Sync>> {
    let placeholder = Config {
        config_path: config_path.map(String::from),
        ..Config::default()
    };
    let config = reload_config(&placeholder)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    security::config_changed("config_reload", "system", "configuration reloaded");
    Ok(config)
}

#[cfg(test)]
// Matches the library crate's test posture (src/lib.rs): an assertion reads
// better than a match on a Result the test would fail on anyway.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use scalo::lifecycle::WorkState;
    use scalo::version_check::VersionCheckConfig;

    /// The env opt-out the charts render from the app's own prefix.
    const ENABLED_VAR: &str = "DFE_FETCHER_VERSION_CHECK__ENABLED";

    /// Serialises the tests that set process-wide env vars.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// The version check `ServiceRuntime::build` resolves after `load_config`
    /// reads `yaml` as the `--config` file, with the env opt-out at `enabled`.
    ///
    /// The cascade is a process-global `OnceLock`, so each caller needs its own
    /// process, which nextest gives every test.
    #[allow(unsafe_code)]
    fn resolved_version_check(yaml: &str, enabled: Option<&str>) -> VersionCheckConfig {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fetcher.yaml");
        std::fs::write(&path, yaml).expect("write config");
        let path = path.to_str().expect("utf-8 path");

        // SAFETY: test-only, serialised by ENV_LOCK
        unsafe {
            match enabled {
                Some(v) => std::env::set_var(ENABLED_VAR, v),
                None => std::env::remove_var(ENABLED_VAR),
            }
        }
        let app = App::parse_from(["dfe-fetcher", "--config", path]);
        // run_app sets the cascade up from --config before load_config.
        scalo::config::setup(app.common_args().to_config_options(app.env_prefix()))
            .expect("cascade sets up");
        app.load_config(Some(path)).expect("config loads");
        let resolved =
            VersionCheckConfig::from_cascade_or(app.name(), "0.0.0", app.version_check_defaults());
        // SAFETY: test-only, serialised by ENV_LOCK
        unsafe { std::env::remove_var(ENABLED_VAR) };
        resolved
    }

    /// An idle fetcher reads near 0 and a buffer that fills the guard's limit
    /// reads the component's full 40, whatever `buffer.memory_limit` is.
    #[test]
    fn buffer_depth_pressure_follows_the_guard_limit() {
        let cases: [(&str, u64, u64, f64); 5] = [
            (
                "idle",
                14_225_408,
                684_510_412,
                40.0 * 14_225_408.0 / 684_510_412.0,
            ),
            ("half full", 50, 100, 20.0),
            ("full", 100, 100, 40.0),
            ("over the limit", 500, 100, 40.0),
            ("no limit", 14_225_408, 0, 0.0),
        ];
        for (name, used, limit, expected) in cases {
            let pressure = scalo::scaling::ScalingPressure::new(
                scalo::scaling::ScalingPressureConfig::default(),
                scaling_components(),
            );
            pressure.set_component("buffer_depth", buffer_depth(used, limit));
            let got = pressure.calculate();
            assert!((got - expected).abs() < 1e-9, "{name}: {got} != {expected}");
        }
    }

    /// `metrics-manifest` builds a bare manager and calls `register_metrics`,
    /// so the catalogue holds the fetcher's metrics only if the app overrides it.
    #[test]
    fn register_metrics_fills_the_manifest_catalogue() {
        let manager =
            scalo::metrics::MetricsManager::with_config(scalo::metrics::MetricsConfig::offline(""));
        assert!(
            manager.registry().manifest().metrics.is_empty(),
            "a fresh manager starts with no catalogue"
        );

        App::parse_from(["dfe-fetcher"]).register_metrics(&manager);

        let names: Vec<String> = manager
            .registry()
            .manifest()
            .metrics
            .into_iter()
            .map(|d| d.name)
            .collect();
        for expected in ["records_received_total", "transport_sent_total"] {
            assert!(
                names.iter().any(|n| n == expected),
                "{expected} missing from the catalogue: {names:?}"
            );
        }
    }

    #[test]
    fn version_check_env_opt_out_stops_the_check() {
        let resolved = resolved_version_check("{}\n", Some("false"));
        assert!(
            !resolved.enabled,
            "{ENABLED_VAR}=false must stop the check under --config"
        );
    }

    #[test]
    fn version_check_config_file_opt_out_stops_the_check() {
        let resolved = resolved_version_check("version_check:\n  enabled: false\n", None);
        assert!(
            !resolved.enabled,
            "version_check.enabled: false in the --config file must stop the check"
        );
    }

    #[test]
    fn version_check_stays_on_by_default() {
        let resolved = resolved_version_check("{}\n", None);
        assert!(resolved.enabled, "phone-home is on unless opted out");
        assert_eq!(resolved.api_url, "https://releases.hyperi.io/api/v1/check");
    }

    /// A bus output with a broker, so the active half of the first test has a
    /// transport to name. Neither test is about the transport itself.
    const OUTPUT: &str = "output:\n  type: kafka\n  kafka:\n    brokers: [localhost:9092]\n";

    /// The loop scalo's idle gate runs: re-read the config through the app's
    /// own `load_config`, then ask `work_state` again.
    fn state_of(app: &App, path: &std::path::Path) -> WorkState {
        let config = app
            .load_config(Some(path.to_str().expect("utf-8 path")))
            .expect("config loads");
        app.work_state(&config)
    }

    /// A fetcher with nothing to poll and nothing to receive idles rather than
    /// refusing, and the first enabled source takes it out of idle. The gate
    /// re-reads the config through this same `load_config`, so the predicate
    /// sees a rewritten file exactly as a fresh start would.
    #[test]
    fn the_first_enabled_source_takes_the_fetcher_out_of_idle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fetcher.yaml");
        let no_work =
            format!("{OUTPUT}ingest:\n  enabled: false\nsources:\n  aws:\n    enabled: false\n");
        let with_source = format!(
            "{OUTPUT}ingest:\n  enabled: false\nsources:\n  aws:\n    enabled: true\n    region: us-east-1\n    credential_secret: \"vault:kv/data/aws:credentials\"\n"
        );
        std::fs::write(&path, &no_work).expect("write config");

        let app = App::parse_from(["dfe-fetcher", "--config", path.to_str().expect("utf-8")]);

        let idle = state_of(&app, &path);
        assert!(
            idle.is_idle(),
            "no enabled source idles, it does not refuse"
        );
        assert_eq!(
            idle.reason(),
            Some("no enabled sources, container extractors, vector receiver or ingest listener")
        );

        std::fs::write(&path, &with_source).expect("rewrite config");

        assert_eq!(
            state_of(&app, &path),
            WorkState::Active,
            "the first enabled source gives the fetcher work"
        );
    }

    /// The shape a fetcher is deployed in before anything gives it a source: an
    /// empty config file, no transport named, no listener. `load_config`
    /// validates, so this covers the default bus output with no broker too.
    #[test]
    fn the_default_configuration_loads_and_idles() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fetcher.yaml");
        std::fs::write(&path, "{}\n").expect("write config");

        let app = App::parse_from(["dfe-fetcher", "--config", path.to_str().expect("utf-8")]);

        let state = state_of(&app, &path);
        assert!(
            state.is_idle(),
            "a fetcher deployed with no sources idles, it does not refuse"
        );
        assert_eq!(
            state.reason(),
            Some("no enabled sources, container extractors, vector receiver or ingest listener")
        );
    }

    /// A source's fingerprint moves with any setting its driver is built
    /// from, a secret included, and with nothing else: the reload rebuilds
    /// exactly the sources a restart would build differently.
    #[test]
    fn a_source_fingerprint_moves_with_its_own_settings_only() {
        let prints = |config: &Config| -> std::collections::BTreeMap<String, u64> {
            plan_sources(config)
                .expect("plans")
                .into_iter()
                .map(|p| (p.id, p.fingerprint))
                .collect()
        };
        let mut config = Config::default();
        config
            .sources
            .rest
            .insert("api".into(), RestInstance::default());
        let db = DbInstance {
            enabled: true,
            ..DbInstance::default()
        };
        config.sources.db.insert("pg".into(), db);
        let base = prints(&config);
        assert_eq!(base.keys().collect::<Vec<_>>(), ["api", "pg"]);
        assert_eq!(prints(&config), base, "the same config plans the same");

        let mut unrelated = config.clone();
        unrelated.scheduler.default_interval_secs += 1;
        assert_eq!(prints(&unrelated), base, "a global edit rebuilds nothing");

        let mut secret = config.clone();
        if let Some(pg) = secret.sources.db.get_mut("pg") {
            pg.connection_string = scalo::SensitiveString::from("postgres://elsewhere");
        }
        let moved = prints(&secret);
        assert_eq!(moved["api"], base["api"], "a sibling is untouched");
        assert_ne!(
            moved["pg"], base["pg"],
            "a changed secret rebuilds its source"
        );

        let mut in_flight = config.clone();
        in_flight.accumulate.in_flight += 1;
        let moved = prints(&in_flight);
        assert!(
            base.iter().all(|(id, print)| moved[id] != *print),
            "a setting every driver bakes in rebuilds them all"
        );
    }
}
