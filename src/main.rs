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

// Jemalloc takes priority when enabled
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Mimalloc only when jemalloc is not enabled
#[cfg(all(feature = "mimalloc", not(feature = "jemalloc")))]
#[global_allocator]
static GLOBAL_MIMALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use hyperi_rustlib::cli::{CliError, CommonArgs, DfeApp, StandardCommand, VersionInfo};
use hyperi_rustlib::config::reloader::{ConfigReloader, ReloaderConfig};
use hyperi_rustlib::deployment::{generate_chart, generate_compose_fragment, generate_dockerfile};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use dfe_fetcher::config::{Config, reload_config};
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
/// Standard commands (`run`, `version`, `config-check`) delegate to the
/// rustlib CLI lifecycle. Deployment commands generate artifacts from the
/// [`DeploymentContract`](dfe_fetcher::deployment::contract).
#[derive(Subcommand, Clone, Debug)]
enum AppCommand {
    /// Start the service (default if no subcommand given).
    Run,

    /// Print version information and exit.
    Version,

    /// Validate configuration and exit.
    #[command(name = "config-check")]
    ConfigCheck,

    /// Generate Dockerfile to stdout.
    #[command(name = "emit-dockerfile")]
    EmitDockerfile,

    /// Generate Helm chart to the given directory.
    #[command(name = "emit-chart")]
    EmitChart {
        /// Output directory for the chart.
        dir: String,
    },

    /// Generate Docker Compose fragment to stdout.
    #[command(name = "emit-compose")]
    EmitCompose,

    /// Print deployment contract as JSON to stdout.
    #[command(name = "emit-contract")]
    EmitContract,
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
        // Map app commands to standard commands.
        // Deployment commands are handled before run_app is called.
        match &self.command {
            Some(AppCommand::Version) => {
                // Store a local static to return a reference
                static VERSION: StandardCommand = StandardCommand::Version;
                Some(&VERSION)
            }
            Some(AppCommand::ConfigCheck) => {
                static CONFIG_CHECK: StandardCommand = StandardCommand::ConfigCheck;
                Some(&CONFIG_CHECK)
            }
            // Run (explicit or default) and deployment commands
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

    async fn run_service(&self, config: Config) -> Result<(), CliError> {
        run_fetcher_service(&self.common, config)
            .await
            .map_err(|e| CliError::Service(e.to_string()))
    }
}

#[tokio::main]
async fn main() {
    let app = App::parse();

    // Handle deployment artifact commands before entering the DfeApp lifecycle
    // (these don't need config or logging)
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
            _ => {}
        }
    }

    // Delegate to standard DfeApp lifecycle (logging → config → run_service)
    if let Err(e) = hyperi_rustlib::cli::run_app(app).await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

/// Main service loop — called by the DfeApp lifecycle after logging and config.
async fn run_fetcher_service(common: &CommonArgs, config: Config) -> anyhow::Result<()> {
    // Initialise metrics
    let metrics = Arc::new(Metrics::new());

    // Create cancellation token for coordinated shutdown
    let shutdown_token = CancellationToken::new();

    // Spawn signal handler for graceful shutdown (SIGINT + SIGTERM)
    let signal_token = shutdown_token.clone();
    tokio::spawn(async move {
        let ctrl_c = signal::ctrl_c();

        #[cfg(unix)]
        {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .unwrap_or_else(|e| {
                    warn!(error = %e, "Failed to register SIGTERM handler");
                    panic!("SIGTERM handler registration failed: {e}");
                });

            tokio::select! {
                result = ctrl_c => {
                    if let Err(e) = result {
                        warn!(error = %e, "Failed to listen for SIGINT");
                        return;
                    }
                    info!("Received SIGINT, initiating shutdown");
                }
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, initiating shutdown");
                }
            }
        }

        #[cfg(not(unix))]
        {
            if let Err(e) = ctrl_c.await {
                warn!(error = %e, "Failed to listen for SIGINT");
                return;
            }
            info!("Received SIGINT, initiating shutdown");
        }

        signal_token.cancel();
    });

    // Parse metrics server address
    let default_metrics_addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], 9090));
    let metrics_addr: SocketAddr = common.metrics_addr.parse().unwrap_or_else(|_| {
        warn!(addr = %common.metrics_addr, "Invalid metrics address, using default");
        default_metrics_addr
    });

    // Create and run the pipeline orchestrator
    let orchestrator =
        Orchestrator::new(config.clone(), metrics.clone(), shutdown_token.clone()).await?;
    let pipeline_state = orchestrator.state();

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
        );

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

    // Spawn metrics server
    let metrics_token = shutdown_token.clone();
    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        if let Err(e) = run_metrics_server(metrics_addr, metrics_clone, metrics_token).await {
            error!(error = %e, "Metrics server error");
        }
    });

    // Create scheduler
    let scheduler = Scheduler::new(&config.scheduler);

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

        let interval = scheduler.effective_interval(None);
        let state = Arc::clone(&pipeline_state);
        info!(
            source = source.name(),
            interval_secs = interval.as_secs(),
            "Starting fetch schedule"
        );

        scheduler.spawn_source_task(
            Arc::clone(source),
            interval,
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
    reload_config(&placeholder).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
}

/// Run the Prometheus metrics HTTP server.
async fn run_metrics_server(
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    use axum::Router;
    use axum::routing::get;

    let app = Router::new()
        .route("/metrics", get(move || async move { metrics.render() }))
        .route("/health/live", get(|| async { "OK" }))
        .route("/health/ready", get(|| async { "OK" }));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr = %addr, "Metrics server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await?;

    Ok(())
}
