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
//! - `GET /health/live` — Liveness check (via rustlib `HttpServer`).
//! - `GET /health/ready` — Readiness check (via rustlib `HttpServer`).
//!
//! ## Authentication
//!
//! When `ingest.auth_token` is configured, all `/ingest` endpoints require
//! a `Authorization: Bearer <token>` header. The `/health` endpoint is
//! always exempt (K8s probes need unauthenticated access).

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::routing::post;
use hyperi_rustlib::http_server::{HttpServer, HttpServerConfig};
use hyperi_rustlib::logger::security;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::IngestConfig;
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;

/// Shared state for the ingest server.
struct IngestState {
    pipeline: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    /// Resolved bearer token. `None` means no authentication required.
    auth_token: Option<String>,
}

/// Constant-time byte comparison. Note: returns false immediately on
/// different lengths, which leaks length information. Acceptable for
/// this internal bearer token use case. For internet-facing auth,
/// use the `subtle` crate or hash both values before comparison.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}

/// Bearer token authentication middleware.
///
/// Skips auth for `/health` endpoints (K8s probes).
/// When no token is configured, all requests pass through.
async fn auth_middleware(
    State(state): State<Arc<IngestState>>,
    request: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    // Health endpoints are always exempt from auth
    if request.uri().path().starts_with("/health") {
        return next.run(request).await;
    }

    let Some(ref expected_token) = state.auth_token else {
        // No auth configured — pass through
        return next.run(request).await;
    };

    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let Some(header_value) = auth_header else {
        security::auth_failure("ingest_bearer", "missing_header", None);
        return StatusCode::UNAUTHORIZED.into_response();
    };

    let Some(token) = header_value.strip_prefix("Bearer ") else {
        security::auth_failure("ingest_bearer", "invalid_scheme", None);
        return StatusCode::UNAUTHORIZED.into_response();
    };

    if constant_time_eq(token.as_bytes(), expected_token.as_bytes()) {
        security::auth_success("ingest_bearer", "container_extractor", None);
        next.run(request).await
    } else {
        security::auth_failure("ingest_bearer", "invalid_token", None);
        StatusCode::UNAUTHORIZED.into_response()
    }
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

    let resolved_token = match config.auth_token.as_deref() {
        Some(token) if !token.is_empty() => {
            info!("Ingest server authentication enabled");
            Some(token.to_string())
        }
        Some(_) => {
            warn!("Ingest auth_token is empty — running without authentication");
            None
        }
        None => {
            warn!("No ingest auth_token configured — running without authentication");
            None
        }
    };

    let state = Arc::new(IngestState {
        pipeline,
        metrics,
        auth_token: resolved_token,
    });

    let app = Router::new()
        .route("/ingest/{source}", post(handle_ingest))
        .route("/ingest/{source}/{topic}", post(handle_ingest_with_topic))
        .with_state(state.clone())
        .layer(axum::middleware::from_fn_with_state(state, auth_middleware))
        .layer(axum::extract::DefaultBodyLimit::max(config.max_body_size));

    let http_config = HttpServerConfig::new(&config.bind_address);

    info!(addr = %config.bind_address, "Ingest server listening");

    let server = HttpServer::new(http_config);
    server
        .serve_with_shutdown(app, shutdown.cancelled_owned())
        .await
        .map_err(|e| anyhow::anyhow!("Ingest server error: {e}"))?;

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
    let start = std::time::Instant::now();

    if body.is_empty() {
        state.metrics.record_ingest_duration(start.elapsed());
        state.metrics.inc_ingest_request("error");
        return StatusCode::BAD_REQUEST;
    }

    let config = state.pipeline.config();
    let topic_suffix = config
        .output
        .topic_suffix
        .as_deref()
        .unwrap_or(&config.kafka.topic_suffix);
    let topic = format!("{}{}", source, topic_suffix);

    debug!(
        source = %source,
        topic = %topic,
        bytes = body.len(),
        "Ingest received"
    );

    state.metrics.add_records_fetched(1);

    match state.pipeline.deliver_ingest(&topic, body).await {
        Ok(()) => {
            state.metrics.record_ingest_duration(start.elapsed());
            state.metrics.inc_ingest_request("success");
            StatusCode::OK
        }
        Err(e) => {
            state.metrics.record_ingest_duration(start.elapsed());
            state.metrics.inc_ingest_request("error");
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
    let start = std::time::Instant::now();

    if body.is_empty() {
        state.metrics.record_ingest_duration(start.elapsed());
        state.metrics.inc_ingest_request("error");
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
        Ok(()) => {
            state.metrics.record_ingest_duration(start.elapsed());
            state.metrics.inc_ingest_request("success");
            StatusCode::OK
        }
        Err(e) => {
            state.metrics.record_ingest_duration(start.elapsed());
            state.metrics.inc_ingest_request("error");
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
    use axum::routing::get;
    use tower::ServiceExt;

    use crate::config::Config;
    use crate::config::SharedConfig;

    /// Build a test app with optional auth token.
    ///
    /// Adds `/health/live` and `/health/ready` manually to match
    /// what rustlib `HttpServer::build_router` adds in production
    /// (that method is private, so we replicate the routes here).
    fn test_app_with_auth(auth_token: Option<String>) -> (Router, Arc<PipelineState>) {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        let pipeline = Arc::new(
            PipelineState::new(shared, Arc::clone(&metrics), None).unwrap_or_else(|_| {
                let config = Config::default();
                let shared = SharedConfig::new(config);
                PipelineState::new(shared, Arc::new(Metrics::new()), None)
                    .expect("default config should work")
            }),
        );
        let state = Arc::new(IngestState {
            pipeline: pipeline.clone(),
            metrics,
            auth_token,
        });

        let app = Router::new()
            .route("/ingest/{source}", post(handle_ingest))
            .route("/ingest/{source}/{topic}", post(handle_ingest_with_topic))
            .route("/health/live", get(|| async { "OK" }))
            .route("/health/ready", get(|| async { "OK" }))
            .with_state(state.clone())
            .layer(axum::middleware::from_fn_with_state(state, auth_middleware));

        (app, pipeline)
    }

    /// Backward-compatible helper: no auth configured.
    fn test_app() -> (Router, Arc<PipelineState>) {
        test_app_with_auth(None)
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let (app, _) = test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health/live")
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

    // -- Auth tests --

    #[tokio::test]
    async fn test_ingest_no_auth_configured_allows_requests() {
        let (app, _) = test_app_with_auth(None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/test_source")
                    .body(Body::from(r#"{"event":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should pass through (may be OK or SERVICE_UNAVAILABLE depending on
        // pipeline state, but NOT 401)
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_ingest_rejects_missing_token() {
        let (app, _) = test_app_with_auth(Some("secret-token-123".to_string()));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/test_source")
                    .body(Body::from(r#"{"event":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_ingest_rejects_wrong_token() {
        let (app, _) = test_app_with_auth(Some("secret-token-123".to_string()));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/test_source")
                    .header("Authorization", "Bearer wrong-token")
                    .body(Body::from(r#"{"event":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_ingest_rejects_non_bearer_scheme() {
        let (app, _) = test_app_with_auth(Some("secret-token-123".to_string()));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/test_source")
                    .header("Authorization", "Basic dXNlcjpwYXNz")
                    .body(Body::from(r#"{"event":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_ingest_accepts_valid_token() {
        let (app, _) = test_app_with_auth(Some("secret-token-123".to_string()));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/test_source")
                    .header("Authorization", "Bearer secret-token-123")
                    .body(Body::from(r#"{"event":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should NOT be 401 — passed auth. May be OK or SERVICE_UNAVAILABLE
        // depending on pipeline/kafka state.
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_health_exempt_from_auth() {
        let (app, _) = test_app_with_auth(Some("secret-token-123".to_string()));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_ingest_with_topic_requires_auth() {
        let (app, _) = test_app_with_auth(Some("secret-token-123".to_string()));

        // Without token — rejected
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/test_source/my_topic")
                    .body(Body::from(r#"{"event":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_constant_time_eq_equal() {
        assert!(constant_time_eq(b"secret", b"secret"));
    }

    #[test]
    fn test_constant_time_eq_different() {
        assert!(!constant_time_eq(b"secret", b"wrong!"));
    }

    #[test]
    fn test_constant_time_eq_different_length() {
        assert!(!constant_time_eq(b"short", b"longer-string"));
    }

    #[test]
    fn test_constant_time_eq_empty() {
        assert!(constant_time_eq(b"", b""));
    }
}
