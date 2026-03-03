// Project:   dfe-fetcher
// File:      src/ingest/mod.rs
// Purpose:   HTTP ingest server for container extractors
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! HTTP ingest server for receiving data from container extractors.
//!
//! Container extractors in HTTP communication mode POST JSON data to this
//! server, which then delivers it through the pipeline to Kafka.
//!
//! ## Endpoints
//!
//! - `POST /ingest/{source}` — Receive JSON payload for a source.
//!   The topic is derived from the source name + configured suffix.
//! - `GET /health` — Health check for the ingest server.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::config::IngestConfig;
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;

/// Shared state for the ingest server.
struct IngestState {
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
}

/// Start the ingest HTTP server.
///
/// Binds to the configured address and serves the ingest endpoints.
/// Returns when the shutdown token is cancelled.
pub async fn run_ingest_server(
    config: &IngestConfig,
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    if !config.enabled {
        info!("Ingest server disabled");
        return Ok(());
    }

    let state = Arc::new(IngestState { pipeline, metrics });

    let app = Router::new()
        .route("/ingest/:source", post(handle_ingest))
        .route("/ingest/:source/:topic", post(handle_ingest_with_topic))
        .route("/health", get(|| async { "OK" }))
        .with_state(state)
        .layer(axum::extract::DefaultBodyLimit::max(config.max_body_size));

    let addr: SocketAddr = config
        .bind_address
        .parse()
        .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 8080)));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr = %addr, "Ingest server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await?;

    info!("Ingest server stopped");
    Ok(())
}

/// Handle POST /ingest/{source}
///
/// Topic is derived from source name using the configured topic suffix.
async fn handle_ingest(
    State(state): State<Arc<IngestState>>,
    Path(source): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    if body.is_empty() {
        return StatusCode::BAD_REQUEST;
    }

    let config = state.pipeline.config();
    let topic = format!("{}{}", source, config.kafka.topic_suffix);

    debug!(
        source = %source,
        topic = %topic,
        bytes = body.len(),
        "Ingest received"
    );

    state.metrics.add_records_fetched(1);

    match state.pipeline.deliver_ingest(&topic, body).await {
        Ok(()) => StatusCode::OK,
        Err(e) => {
            error!(source = %source, error = %e, "Failed to deliver ingest message");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

/// Handle POST /ingest/{source}/{topic}
///
/// Topic is provided explicitly in the URL path.
async fn handle_ingest_with_topic(
    State(state): State<Arc<IngestState>>,
    Path((source, topic)): Path<(String, String)>,
    body: Bytes,
) -> impl IntoResponse {
    if body.is_empty() {
        return StatusCode::BAD_REQUEST;
    }

    debug!(
        source = %source,
        topic = %topic,
        bytes = body.len(),
        "Ingest received (explicit topic)"
    );

    state.metrics.add_records_fetched(1);

    match state.pipeline.deliver_ingest(&topic, body).await {
        Ok(()) => StatusCode::OK,
        Err(e) => {
            error!(source = %source, topic = %topic, error = %e, "Failed to deliver ingest message");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use crate::config::Config;
    use crate::config::SharedConfig;

    fn test_app() -> (Router, Arc<PipelineState>) {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        let pipeline = Arc::new(
            PipelineState::new(shared, Arc::clone(&metrics)).unwrap_or_else(|_| {
                let config = Config::default();
                let shared = SharedConfig::new(config);
                // Kafka not configured — pipeline won't deliver but won't panic
                PipelineState::new(shared, Arc::new(Metrics::new()))
                    .expect("default config should work")
            }),
        );
        let state = Arc::new(IngestState {
            pipeline: pipeline.clone(),
            metrics,
        });

        let app = Router::new()
            .route("/ingest/:source", post(handle_ingest))
            .route("/ingest/:source/:topic", post(handle_ingest_with_topic))
            .route("/health", get(|| async { "OK" }))
            .with_state(state);

        (app, pipeline)
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let (app, _) = test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_ingest_empty_body_rejected() {
        let (app, _) = test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/test_source")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
