// Project:   dfe-fetcher
// File:      src/main.rs
// Purpose:   CLI entry point and runtime initialisation
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-fetcher CLI entry point.
//!
//! Handles argument parsing, configuration loading, logging initialisation,
//! and orchestrates the main fetch pipeline with graceful shutdown.
//!
//! Uses hyperi-rustlib for:
//! - Configuration (7-layer cascade)
//! - Logging (structured JSON/text with masking)
//! - Metrics (Prometheus with process/container metrics)

#![forbid(unsafe_code)]

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

use anyhow::Context;
use clap::Parser;
use hyperi_rustlib::config::reloader::{ConfigReloader, ReloaderConfig};
use hyperi_rustlib::env::Environment;
use hyperi_rustlib::logger::{self, LogFormat, LoggerOptions};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn, Level};

use dfe_fetcher::config::{reload_config, Config};
use dfe_fetcher::extractor::container::ContainerExtractor;
use dfe_fetcher::extractor::vector::VectorManager;
use dfe_fetcher::extractor::Extractor;
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::pipeline::Orchestrator;
use dfe_fetcher::scheduler::Scheduler;
use dfe_fetcher::source::aws::AwsSource;
use dfe_fetcher::source::azure::AzureSource;
use dfe_fetcher::source::gcp::GcpSource;
use dfe_fetcher::source::m365::M365Source;
use dfe_fetcher::source::Source;

/// dfe-fetcher: Data fetcher for external services (AWS, Azure, M365, GCP).
#[derive(Parser, Debug)]
#[command(name = "dfe-fetcher")]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to configuration file.
    #[arg(short, long, env = "DFE_FETCHER_CONFIG")]
    config: Option<String>,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, env = "DFE_FETCHER_LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Log format (json, text, auto).
    #[arg(long, env = "DFE_FETCHER_LOG_FORMAT", default_value = "auto")]
    log_format: String,

    /// Metrics server address.
    #[arg(
        long,
        env = "DFE_FETCHER_METRICS_ADDR",
        default_value = "0.0.0.0:9090"
    )]
    metrics_addr: String,

    /// Validate configuration and exit.
    #[arg(long)]
    validate: bool,

    /// Print loaded configuration and exit.
    #[arg(long)]
    print_config: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Detect environment early
    let env = Environment::detect();

    // Parse CLI arguments
    let args = Args::parse();

    // Initialise logging
    init_logging(&args.log_format, &args.log_level).context("failed to initialise logging")?;

    info!(
        environment = ?env,
        "Runtime environment detected"
    );

    // Load and validate configuration
    let config = Config::load(args.config.as_deref()).context("failed to load configuration")?;

    if let Err(e) = config.validate() {
        error!(error = %e, "configuration validation failed");
        std::process::exit(1);
    }

    // Early exit for special modes
    if args.print_config {
        println!("{config:#?}");
        return Ok(());
    }

    if args.validate {
        info!("Configuration is valid");
        return Ok(());
    }

    // Log startup info
    info!(
        version = env!("CARGO_PKG_VERSION"),
        config_path = ?args.config,
        "Starting dfe-fetcher"
    );

    // Initialise metrics
    let metrics = Arc::new(Metrics::new());

    // Create cancellation token for coordinated shutdown
    let shutdown_token = CancellationToken::new();

    // Spawn signal handler for graceful shutdown
    let signal_token = shutdown_token.clone();
    tokio::spawn(async move {
        if let Err(e) = signal::ctrl_c().await {
            warn!(error = %e, "Failed to listen for SIGINT");
            return;
        }
        info!("Received SIGINT, initiating shutdown");
        signal_token.cancel();
    });

    // Parse metrics server address
    let default_metrics_addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], 9090));
    let metrics_addr: SocketAddr = args.metrics_addr.parse().unwrap_or_else(|_| {
        warn!(addr = %args.metrics_addr, "Invalid metrics address, using default");
        default_metrics_addr
    });

    // Create and run the pipeline orchestrator
    let orchestrator = Orchestrator::new(config.clone(), metrics.clone(), shutdown_token.clone())?;
    let pipeline_state = orchestrator.state();

    // Start config hot-reload
    {
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

        let _handle = reloader.start();

        if config.config_reload_secs > 0 {
            info!(
                interval_secs = config.config_reload_secs,
                "Config hot-reload enabled (SIGHUP + periodic + file polling)"
            );
        } else {
            info!("Config hot-reload enabled (SIGHUP + file polling)");
        }
    }

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

    // Start container extractors
    for container_config in &config.extractors.containers {
        let extractor = ContainerExtractor::new(container_config.clone());
        info!(
            name = %container_config.name,
            image = %container_config.image,
            "Starting container extractor"
        );

        if let Err(e) = extractor.start().await {
            error!(
                name = %container_config.name,
                error = %e,
                "Failed to start container extractor"
            );
        }
    }

    // Start Vector manager
    let vector_manager = VectorManager::new(config.extractors.vector.clone());
    if vector_manager.is_enabled() {
        if let Err(e) = vector_manager.start().await {
            error!(error = %e, "Failed to start Vector manager");
        }
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

/// Initialise logging using hyperi-rustlib's logger module.
fn init_logging(format: &str, level: &str) -> anyhow::Result<()> {
    let log_format = match format {
        "json" => LogFormat::Json,
        "text" => LogFormat::Text,
        _ => LogFormat::Auto,
    };

    let log_level = match level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" | "warning" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    logger::setup(LoggerOptions {
        level: log_level,
        format: log_format,
        add_source: true,
        enable_masking: true,
        sensitive_fields: vec![
            "password".to_string(),
            "secret".to_string(),
            "token".to_string(),
            "api_key".to_string(),
            "access_key".to_string(),
            "secret_key".to_string(),
            "client_secret".to_string(),
        ],
        span_events: false,
    })
    .map_err(|e| anyhow::anyhow!("logger setup failed: {e}"))?;

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
    use axum::routing::get;
    use axum::Router;

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
