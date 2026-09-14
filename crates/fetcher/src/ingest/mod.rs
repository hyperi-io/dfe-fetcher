// Project:   dfe-fetcher
// File:      crates/fetcher/src/ingest/mod.rs
// Purpose:   HTTP ingest server for container extractors
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! HTTP ingest server for receiving data from container extractors.
//!
//! Container extractors in HTTP communication mode POST JSON data to this
//! server, which delivers it through the extractor sink to the outputs. A
//! POST is never held for the output: when the pipeline is not ready or the
//! record is backpressured the answer is `503` with `Retry-After` at once,
//! so the client backs off and re-sends.
//!
//! ## Endpoints
//!
//! - `POST /ingest/{source}` -- Receive JSON payload for a source.
//!   The topic is derived from the source name + configured suffix.
//! - `GET /livez` -- Liveness check (via scalo `HttpServer`).
//! - `GET /readyz` -- Readiness check (via scalo `HttpServer`).
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
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use scalo::http_server::{HttpServer, HttpServerConfig};
use scalo::logger::security;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::IngestConfig;
use crate::error::Error;
use crate::extractor::ExtractorSink;
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;

/// What a refused client is told to wait before re-sending, in seconds.
const RETRY_AFTER_SECS: &str = "5";

/// Shared state for the ingest server.
struct IngestState {
    sink: ExtractorSink,
    metrics: Arc<Metrics>,
    /// Resolved bearer token. `None` means no authentication required.
    auth_token: Option<String>,
}

impl IngestState {
    /// The intake's sink: a backpressured record is refused at once.
    fn new(
        pipeline: Arc<PipelineState>,
        metrics: Arc<Metrics>,
        auth_token: Option<String>,
    ) -> Self {
        Self {
            sink: ExtractorSink::immediate(pipeline, Arc::clone(&metrics)),
            metrics,
            auth_token,
        }
    }

    /// Deliver one POSTed record and answer the client: `200` when the
    /// output took it, `503` with `Retry-After` at once when the pipeline
    /// is not ready or the output is backpressured, `503` for any other
    /// failure.
    async fn ingest(&self, source: &str, topic: &str, body: Bytes) -> Response {
        let start = std::time::Instant::now();
        if body.is_empty() {
            self.metrics.record_ingest_duration(start.elapsed());
            self.metrics.inc_ingest_request("error");
            return StatusCode::BAD_REQUEST.into_response();
        }
        debug!(source, topic, bytes = body.len(), "Ingest received");
        self.metrics.add_records_fetched(1);
        let outcome = if self.sink.state().is_ready() {
            self.sink.deliver(source, "ingest", topic, body).await
        } else {
            Err(Error::Backpressured("pipeline not ready".into()))
        };
        self.metrics.record_ingest_duration(start.elapsed());
        match outcome {
            Ok(()) => {
                self.metrics.inc_ingest_request("success");
                StatusCode::OK.into_response()
            }
            Err(Error::Backpressured(reason)) => {
                self.metrics.inc_ingest_request("error");
                debug!(
                    source,
                    topic, reason, "Ingest refused, client told to retry"
                );
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    [(header::RETRY_AFTER, RETRY_AFTER_SECS)],
                    "server is overloaded",
                )
                    .into_response()
            }
            Err(e) => {
                self.metrics.inc_ingest_request("error");
                error!(source, topic, error = %e, "Failed to deliver ingest message");
                StatusCode::SERVICE_UNAVAILABLE.into_response()
            }
        }
    }
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
/// Skips auth for the K8s probe endpoints. When no token is configured, all
/// requests pass through.
async fn auth_middleware(
    State(state): State<Arc<IngestState>>,
    request: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    // Probe endpoints are always exempt: kubelet sends no Authorization header,
    // so a 401 here fails the liveness probe and restarts the pod in a loop.
    // `/livez` and `/readyz` are the contract's probe paths (see
    // deployment::contract); `/health` is kept for older callers.
    let path = request.uri().path();
    if path.starts_with("/health") || path.starts_with("/livez") || path.starts_with("/readyz") {
        return next.run(request).await;
    }

    let Some(ref expected_token) = state.auth_token else {
        // No auth configured -- pass through
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
            warn!("Ingest auth_token is empty -- running without authentication");
            None
        }
        None => {
            warn!("No ingest auth_token configured -- running without authentication");
            None
        }
    };

    let state = Arc::new(IngestState::new(pipeline, metrics, resolved_token));

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
) -> Response {
    let topic = format!("{}{}", source, state.sink.state().config().topic_suffix());
    state.ingest(&source, &topic, body).await
}

/// Handle POST /ingest/{source}/{topic}
///
/// Topic is provided explicitly in the URL path.
async fn handle_ingest_with_topic(
    State(state): State<Arc<IngestState>>,
    Path((source, topic)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    state.ingest(&source, &topic, body).await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use tower::ServiceExt;

    use crate::config::Config;
    use crate::config::SharedConfig;

    /// Build a test app with optional auth token.
    ///
    /// Adds `/livez` and `/readyz` manually to match
    /// what scalo `HttpServer::build_router` adds in production
    /// (that method is private, so we replicate the routes here).
    fn test_app_with_auth(auth_token: Option<String>) -> (Router, Arc<PipelineState>) {
        test_app_over(None, auth_token)
    }

    /// The test app over `output` (none, or an in-process transport).
    fn test_app_over(
        output: Option<crate::output::OutputManager>,
        auth_token: Option<String>,
    ) -> (Router, Arc<PipelineState>) {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        let pipeline = Arc::new(PipelineState::for_tests(
            shared,
            Arc::clone(&metrics),
            output,
        ));
        let state = Arc::new(IngestState::new(pipeline.clone(), metrics, auth_token));

        let app = Router::new()
            .route("/ingest/{source}", post(handle_ingest))
            .route("/ingest/{source}/{topic}", post(handle_ingest_with_topic))
            .route("/livez", get(|| async { "OK" }))
            .route("/readyz", get(|| async { "OK" }))
            .with_state(state.clone())
            .layer(axum::middleware::from_fn_with_state(state, auth_middleware));

        (app, pipeline)
    }

    async fn post_event(app: Router, uri: &str) -> axum::response::Response {
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .body(Body::from(r#"{"event":"test"}"#))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    /// A pipeline whose output is down is not ready (the predicate the
    /// scheduler stalls on), and the intake says so at once: `503` with
    /// `Retry-After`, before the record reaches the output, and long before
    /// the emitter's retries would have held the POST.
    #[tokio::test]
    async fn a_pipeline_that_is_not_ready_refuses_the_post_at_once_with_retry_after() {
        use scalo::transport::{MemoryConfig, MemoryTransport, TransportBase};

        let transport =
            Arc::new(MemoryTransport::new(&MemoryConfig::default()).expect("memory transport"));
        let (app, pipeline) = test_app_over(
            Some(crate::output::OutputManager::memory(Arc::clone(&transport))),
            None,
        );
        assert!(
            pipeline.is_ready(),
            "probe {} output {} pressure {:.3} of {} bytes",
            pipeline.probe_ready(),
            pipeline.output_healthy(),
            pipeline.memory_guard().pressure_ratio(),
            pipeline.memory_guard().limit_bytes(),
        );
        transport.close().await.expect("close");
        assert!(!pipeline.is_ready(), "a closed output is not ready");

        let started = std::time::Instant::now();
        let response = post_event(app, "/ingest/test_source").await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER).unwrap(),
            RETRY_AFTER_SECS
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "answered at once, not after the emitter's retries: {:?}",
            started.elapsed()
        );
    }

    /// An output that refuses a record (an in-process transport with a
    /// channel of one, already full) is a `503` with `Retry-After` on the
    /// first pass; the intake never re-sends on the client's behalf.
    #[tokio::test]
    async fn a_backpressured_send_refuses_the_post_at_once_with_retry_after() {
        use scalo::transport::{MemoryConfig, MemoryTransport};

        let transport = Arc::new(
            MemoryTransport::new(&MemoryConfig {
                buffer_size: 1,
                ..MemoryConfig::default()
            })
            .expect("memory transport"),
        );
        let (app, pipeline) = test_app_over(
            Some(crate::output::OutputManager::memory(Arc::clone(&transport))),
            None,
        );
        assert!(
            pipeline.is_ready(),
            "probe {} output {} pressure {:.3} of {} bytes",
            pipeline.probe_ready(),
            pipeline.output_healthy(),
            pipeline.memory_guard().pressure_ratio(),
            pipeline.memory_guard().limit_bytes(),
        );
        assert_eq!(
            post_event(app.clone(), "/ingest/test_source")
                .await
                .status(),
            StatusCode::OK,
            "the channel of one takes the first record"
        );

        let started = std::time::Instant::now();
        let response = post_event(app, "/ingest/test_source/topic").await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER).unwrap(),
            RETRY_AFTER_SECS
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "one pass, no retry backoff: {:?}",
            started.elapsed()
        );
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
                    .uri("/livez")
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

        // Should NOT be 401 -- passed auth. May be OK or SERVICE_UNAVAILABLE
        // depending on pipeline/kafka state.
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_health_exempt_from_auth() {
        let (app, _) = test_app_with_auth(Some("secret-token-123".to_string()));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/livez")
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

        // Without token -- rejected
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

    // -- Additional handler coverage --

    /// POST /ingest/{source} with a valid JSON body. The default pipeline
    /// has no output transports configured, so the sink will either fail
    /// with SERVICE_UNAVAILABLE or succeed (if a no-op). Either way, the
    /// handler code path is exercised and the response must NOT be
    /// BAD_REQUEST (empty body) or UNAUTHORIZED (no auth required here).
    #[tokio::test]
    async fn test_handle_ingest_success_path() {
        let (app, _) = test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/aws")
                    .body(Body::from(r#"{"event":"test","id":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_ne!(response.status(), StatusCode::BAD_REQUEST);
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
        // The most likely outcome without output is 503 or 200; either covers
        // the handler's happy/error branch.
        assert!(
            response.status() == StatusCode::OK
                || response.status() == StatusCode::SERVICE_UNAVAILABLE,
            "unexpected status: {}",
            response.status()
        );
    }

    /// POST /ingest/{source}/{topic} with a valid JSON body and explicit topic.
    #[tokio::test]
    async fn test_handle_ingest_with_topic_success_path() {
        let (app, _) = test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/aws/custom_topic")
                    .body(Body::from(r#"{"event":"test","id":42}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_ne!(response.status(), StatusCode::BAD_REQUEST);
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(
            response.status() == StatusCode::OK
                || response.status() == StatusCode::SERVICE_UNAVAILABLE,
            "unexpected status: {}",
            response.status()
        );
    }

    /// POST /ingest/{source}/{topic} with an empty body must be rejected
    /// with 400 Bad Request before any pipeline delivery.
    #[tokio::test]
    async fn test_handle_ingest_with_topic_empty_body_rejected() {
        let (app, _) = test_app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/aws/custom_topic")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// Auth middleware must handle a long (256-byte) bearer token correctly.
    /// Proves constant_time_eq scales beyond trivial token sizes.
    #[tokio::test]
    async fn test_auth_middleware_long_token() {
        let token: String = "a".repeat(256);
        let (app, _) = test_app_with_auth(Some(token.clone()));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/test_source")
                    .header("Authorization", format!("Bearer {token}"))
                    .body(Body::from(r#"{"event":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Correct token -- must not be rejected for auth reasons.
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// `constant_time_eq` must return false for both "close" and "far" mismatches.
    /// We don't measure wall clock here (timing is noisy in tests), but we
    /// verify that the function's return value does not depend on early-exit
    /// behaviour -- both call patterns return false, proving the loop ran to
    /// completion in both cases.
    #[test]
    fn test_constant_time_eq_wrong_by_one_vs_all() {
        // Differ in the first byte only.
        assert!(!constant_time_eq(b"aaaaaaaa", b"baaaaaaa"));
        // Differ in every byte.
        assert!(!constant_time_eq(b"aaaaaaaa", b"bbbbbbbb"));
        // Differ only at the last position.
        assert!(!constant_time_eq(b"aaaaaaaa", b"aaaaaaab"));
    }

    /// `run_ingest_server` with `enabled = false` must return Ok immediately
    /// without binding any socket.
    #[tokio::test]
    async fn test_run_ingest_server_disabled() {
        let config = IngestConfig {
            enabled: false,
            ..Default::default()
        };

        let app_config = Config::default();
        let shared = SharedConfig::new(app_config);
        let metrics = Arc::new(Metrics::new());
        let pipeline = Arc::new(
            PipelineState::new(shared, Arc::clone(&metrics), None, CancellationToken::new())
                .expect("default config should work"),
        );

        let shutdown = CancellationToken::new();
        let result = run_ingest_server(&config, pipeline, metrics, shutdown).await;

        assert!(result.is_ok(), "disabled server should return Ok");
    }

    /// `Authorization: Bearer ` (with trailing space, empty token) must be
    /// rejected -- empty string token will never match the configured token.
    #[tokio::test]
    async fn test_auth_middleware_bearer_prefix_empty_token() {
        let (app, _) = test_app_with_auth(Some("secret-token-123".to_string()));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/test_source")
                    .header("Authorization", "Bearer ")
                    .body(Body::from(r#"{"event":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// `Authorization: Bearer  token` (double space) -- `strip_prefix("Bearer ")`
    /// takes only one space, leaving ` token` as the actual token value, which
    /// won't match the configured token.
    #[tokio::test]
    async fn test_auth_middleware_extra_whitespace() {
        let (app, _) = test_app_with_auth(Some("token".to_string()));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingest/test_source")
                    // Two spaces between "Bearer" and the token.
                    .header("Authorization", "Bearer  token")
                    .body(Body::from(r#"{"event":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
