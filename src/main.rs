// Project:   dfe-fetcher
// File:      src/main.rs
// Purpose:   CLI entry point and runtime initialisation
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-fetcher CLI entry point.
//!
//! Uses hyperi-rustlib CLI module for standard arguments and subcommands.
//! Implements the [`DfeApp`] trait for the standard DFE service lifecycle.

// Jemalloc — DFE allocator policy 2026-04-17 (jemalloc only at every channel).
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use hyperi_rustlib::cli::{
    CliError, CommonArgs, DfeApp, ServiceRuntime, StandardCommand, TopArgs, VersionInfo,
};
use hyperi_rustlib::config::reloader::{ConfigReloader, ReloaderConfig};
use hyperi_rustlib::deployment::{generate_chart, generate_compose_fragment, generate_dockerfile};
use hyperi_rustlib::logger::security;
use hyperi_rustlib::scaling::ScalingComponent;
use hyperi_rustlib::top::{TopConfig, run_top};
use tracing::{debug, error, info, warn};

use dfe_fetcher::config::{Config, derive_instance_id, reload_config};
use dfe_fetcher::cursor;
use dfe_fetcher::deployment;
use dfe_fetcher::extractor::container::ContainerExtractor;
use dfe_fetcher::extractor::vector::VectorManager;
use dfe_fetcher::ingest;
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::pipeline::Orchestrator;
use dfe_fetcher::scheduler::Scheduler;
use dfe_fetcher::source::Source;
use dfe_fetcher::source::aws::AwsSource;
use dfe_fetcher::source::azure::AzureSource;
use dfe_fetcher::source::gcp::GcpSource;
use dfe_fetcher::source::m365::M365Source;

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
/// `metrics-manifest`) are flattened from rustlib's [`StandardCommand`].
/// Local extensions handle the legacy emit-* shortcuts and the `top` TUI.
#[derive(Subcommand, Clone, Debug)]
enum AppCommand {
    /// Standard rustlib commands (run, version, config-check, generate-artefacts, metrics-manifest).
    #[command(flatten)]
    Standard(StandardCommand),

    /// Generate Dockerfile to stdout (legacy shortcut; prefer `generate-artefacts`).
    #[command(name = "emit-dockerfile")]
    EmitDockerfile,

    /// Generate Helm chart to the given directory (legacy shortcut; prefer `generate-artefacts`).
    #[command(name = "emit-chart")]
    EmitChart {
        /// Output directory for the chart.
        dir: String,
    },

    /// Generate Docker Compose fragment to stdout (legacy shortcut; prefer `generate-artefacts`).
    #[command(name = "emit-compose")]
    EmitCompose,

    /// Print deployment contract as JSON to stdout (legacy shortcut; prefer `generate-artefacts`).
    #[command(name = "emit-contract")]
    EmitContract,

    /// Live TUI metrics dashboard (connects to running instance's /metrics endpoint).
    Top(TopArgs),
}

impl DfeApp for App {
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

    fn load_config(&self, path: Option<&str>) -> Result<Config, CliError> {
        let config =
            Config::load(path).map_err(|e| CliError::Config(format!("failed to load: {e}")))?;
        config
            .validate()
            .map_err(|e| CliError::Config(format!("validation failed: {e}")))?;
        Ok(config)
    }

    async fn run_service(&self, config: Config, runtime: ServiceRuntime) -> Result<(), CliError> {
        run_fetcher_service(&self.common, config, runtime)
            .await
            .map_err(|e| CliError::Service(e.to_string()))
    }

    fn deployment_contract(&self) -> Option<hyperi_rustlib::deployment::DeploymentContract> {
        Some(crate::deployment::contract())
    }
}

#[tokio::main]
async fn main() {
    let app = App::parse();

    // Handle non-standard subcommands locally before entering the DfeApp lifecycle
    // (these don't need config or logging). Standard subcommands fall through to run_app.
    if let Some(ref cmd) = app.command {
        match cmd {
            AppCommand::EmitDockerfile => {
                let contract = deployment::contract();
                println!("{}", generate_dockerfile(&contract));
                return;
            }
            AppCommand::EmitChart { dir } => {
                let contract = deployment::contract();
                if let Err(e) = generate_chart(&contract, dir) {
                    eprintln!("error: failed to generate Helm chart: {e}");
                    std::process::exit(1);
                }
                eprintln!("Helm chart generated in {dir}/");
                return;
            }
            AppCommand::EmitCompose => {
                let contract = deployment::contract();
                println!("{}", generate_compose_fragment(&contract));
                return;
            }
            AppCommand::EmitContract => {
                let contract = deployment::contract();
                println!("{}", contract.to_json());
                return;
            }
            AppCommand::Top(args) => {
                let config = TopConfig::from_args(args);
                if let Err(e) = run_top(&config) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                return;
            }
            AppCommand::Standard(_) => {
                // fall through to run_app
            }
        }
    }

    // Delegate to standard DfeApp lifecycle (logging → config → run_service)
    // Box::pin avoids a large-future clippy lint on the run_app async fn.
    if let Err(e) = Box::pin(hyperi_rustlib::cli::run_app(app)).await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

/// Main service loop — called by the DfeApp lifecycle after logging and config.
async fn run_fetcher_service(
    _common: &CommonArgs,
    config: Config,
    mut runtime: ServiceRuntime,
) -> anyhow::Result<()> {
    // Warn if deprecated plugin config is present
    config.extractors.plugins.warn_if_configured();

    // Warn if using legacy kafka: config section instead of output.kafka:
    if !config.kafka.brokers.is_empty() && config.output.kafka.is_none() {
        warn!(
            "Using legacy kafka: config section — migrate to output.kafka: (rustlib KafkaConfig format)"
        );
    }

    // Register config in the global config registry (enables /config debug endpoint)
    config.register_in_registry();

    // Log startup configuration at debug level for observability
    debug!(
        sources_aws = config.sources.aws.enabled,
        sources_azure = config.sources.azure.enabled,
        sources_m365 = config.sources.m365.enabled,
        sources_gcp = config.sources.gcp.enabled,
        scheduler_interval_secs = config.scheduler.default_interval_secs,
        scheduler_jitter_pct = config.scheduler.jitter_percent,
        scheduler_max_concurrent = config.scheduler.max_concurrent_fetches,
        output_type = %config.output.output_type,
        output_topic_suffix = config.output.topic_suffix.as_deref().unwrap_or(&config.kafka.topic_suffix),
        cursor_dir = %config.cursor.directory,
        cursor_window_hours = config.cursor.default_window_hours,
        dlq_enabled = config.dlq.enabled,
        config_reload_secs = config.config_reload_secs,
        "Startup configuration"
    );
    if config.sources.aws.enabled {
        debug!(
            region = %config.sources.aws.region,
            services = ?config.sources.aws.services.iter().map(|s| &s.name).collect::<Vec<_>>(),
            topic = %config.sources.aws.topic,
            filter = config.sources.aws.filter.as_deref().unwrap_or("none"),
            interval_secs = config.sources.aws.interval_secs,
            "AWS source config"
        );
    }
    if config.sources.azure.enabled {
        debug!(
            tenant_id = config.sources.azure.tenant_id.as_deref().unwrap_or("unset"),
            services = ?config.sources.azure.services.iter().map(|s| &s.name).collect::<Vec<_>>(),
            topic = %config.sources.azure.topic,
            filter = config.sources.azure.filter.as_deref().unwrap_or("none"),
            interval_secs = config.sources.azure.interval_secs,
            "Azure source config"
        );
    }
    if config.sources.m365.enabled {
        debug!(
            tenant_id = config.sources.m365.tenant_id.as_deref().unwrap_or("unset"),
            services = ?config.sources.m365.services.iter().map(|s| &s.name).collect::<Vec<_>>(),
            topic = %config.sources.m365.topic,
            filter = config.sources.m365.filter.as_deref().unwrap_or("none"),
            interval_secs = config.sources.m365.interval_secs,
            "M365 source config"
        );
    }
    if config.sources.gcp.enabled {
        debug!(
            project_id = config.sources.gcp.project_id.as_deref().unwrap_or("unset"),
            services = ?config.sources.gcp.services.iter().map(|s| &s.name).collect::<Vec<_>>(),
            topic = %config.sources.gcp.topic,
            filter = config.sources.gcp.filter.as_deref().unwrap_or("none"),
            interval_secs = config.sources.gcp.interval_secs,
            "GCP source config"
        );
    }

    // Fire-and-forget version check against crates.io
    {
        use hyperi_rustlib::version_check::{VersionCheck, VersionCheckConfig};
        let checker = VersionCheck::new(VersionCheckConfig::from_cascade(
            "dfe-fetcher",
            env!("CARGO_PKG_VERSION"),
        ));
        checker.check_on_startup();
    }

    // Use ServiceRuntime's pre-wired MetricsManager (already started, serving /metrics)
    // and shutdown token (signal handler already installed with K8s pre-stop delay).
    let shutdown_token = runtime.shutdown.clone();

    // Initialise fetcher metrics (with DfeMetrics dual-emit for standard DFE metric names).
    // runtime.dfe is the DfeMetrics already registered by ServiceRuntime; we also register
    // fetcher-specific metric descriptions by constructing Metrics::with_dfe().
    let metrics = Arc::new(Metrics::with_dfe(&runtime.metrics));

    // Create and run the pipeline orchestrator
    let orchestrator =
        Orchestrator::new(config.clone(), metrics.clone(), shutdown_token.clone()).await?;
    let pipeline_state = orchestrator.state();

    // Build scaling pressure calculator for KEDA autoscaling with fetcher-specific components.
    // The ServiceRuntime's scaling field (if any) uses generic components; we create one
    // with fetcher-specific weights for buffer depth, transport errors, and memory.
    let scaling_pressure = Arc::new(hyperi_rustlib::scaling::ScalingPressure::new(
        config.scaling.clone(),
        vec![
            ScalingComponent::new(
                "buffer_depth",
                0.40,
                config.buffer.memory_limit.max(1) as f64,
            ),
            ScalingComponent::new("transport_errors", 0.30, 100.0),
            ScalingComponent::new("memory", 0.30, 1.0),
        ],
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

    // Spawn periodic scaling pressure update (feeds buffer + transport health to ScalingPressure)
    {
        let scaling = Arc::clone(&scaling_pressure);
        let state = Arc::clone(&pipeline_state);
        let shutdown = shutdown_token.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mg = state.memory_guard();
                        let used = mg.current_bytes();
                        let limit = mg.limit_bytes();
                        scaling.set_component("buffer_depth", used as f64);
                        scaling.set_memory(used, limit);
                        scaling.set_circuit_open(!state.output_healthy());
                    }
                    _ = shutdown.cancelled() => break,
                }
            }
        });
    }

    // Wire readiness and scaling into ServiceRuntime's MetricsManager.
    // The server is already started by run_app() — we only add the readiness
    // check and scaling pressure callbacks here.
    {
        let ready_state = Arc::clone(&pipeline_state);
        runtime
            .metrics
            .set_readiness_check(move || ready_state.is_ready());
        runtime
            .metrics
            .set_scaling_pressure(Arc::clone(&scaling_pressure));
        debug!("Metrics readiness and scaling pressure wired");
    }

    // Derive instance identity for cursor isolation
    let instance_id = derive_instance_id(&config);
    info!(instance_id, "Fetcher instance identity");
    if config.instance_id.is_none() {
        warn!(
            instance_id,
            "Instance ID was auto-derived; set 'instance_id' in config for stable cursor keys"
        );
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
            "cursor.directory not set — falling back to config file directory. \
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
        cursor_store,
        instance_id,
    );

    // Register native sources
    let sources: Vec<Arc<dyn Source>> = vec![
        Arc::new(AwsSource::new(config.sources.aws.clone())),
        Arc::new(AzureSource::new(config.sources.azure.clone())),
        Arc::new(M365Source::new(config.sources.m365.clone())),
        Arc::new(GcpSource::new(config.sources.gcp.clone())),
    ];

    // Start fetch tasks for enabled sources
    for source in &sources {
        if !source.is_enabled() {
            continue;
        }

        let initial_interval = scheduler.effective_interval(None);
        let state = Arc::clone(&pipeline_state);
        let ready_state = Arc::clone(&pipeline_state);
        info!(
            source = source.name(),
            interval_secs = initial_interval.as_secs(),
            "Starting fetch schedule (interval is hot-reloaded)"
        );

        scheduler.spawn_source_task(
            Arc::clone(source),
            None,
            Arc::clone(&metrics),
            shutdown_token.clone(),
            Arc::new(move |results| {
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(e) = state.deliver(results).await {
                        tracing::error!(error = %e, "Failed to deliver fetch results");
                    }
                });
            }),
            Arc::new(move || ready_state.is_ready()),
        );
    }

    // Start ingest HTTP server (for container extractors using HTTP communication)
    {
        let ingest_config = config.ingest.clone();
        let ingest_pipeline = Arc::clone(&pipeline_state);
        let ingest_metrics = Arc::clone(&metrics);
        let ingest_shutdown = shutdown_token.clone();
        tokio::spawn(async move {
            if let Err(e) = ingest::run_ingest_server(
                &ingest_config,
                ingest_pipeline,
                ingest_metrics,
                ingest_shutdown,
            )
            .await
            {
                error!(error = %e, "Ingest server error");
            }
        });
    }

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
    );
    if vector_manager.is_enabled()
        && let Err(e) = vector_manager.start().await
    {
        error!(error = %e, "Failed to start Vector manager");
    }

    // Run pipeline orchestrator (blocks until shutdown)
    if let Err(e) = orchestrator.run().await {
        error!(error = %e, "Pipeline error");
        std::process::exit(1);
    }

    // Cleanup
    if let Err(e) = vector_manager.stop().await {
        warn!(error = %e, "Error stopping Vector manager");
    }

    info!("Shutdown complete");
    Ok(())
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
