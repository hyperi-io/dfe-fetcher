// Project:   dfe-fetcher
// File:      crates/fetcher/src/error.rs
// Purpose:   Centralised error types
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Centralised error types for dfe-fetcher.
//!
//! Uses `thiserror` for ergonomic error derivation with automatic
//! `From` implementations for common conversions.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

/// Main error type for dfe-fetcher.
#[derive(Error, Debug)]
pub enum Error {
    /// Configuration loading or validation error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Source fetch error (API call to external service failed).
    #[error("source error: {0}")]
    Source(String),

    /// Authentication/credential error for external services.
    #[error("credential error: {0}")]
    Credential(String),

    /// Kafka producer error.
    #[error("Kafka error: {0}")]
    Kafka(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Scheduling error.
    #[error("scheduler error: {0}")]
    Scheduler(String),

    /// Pipeline processing error.
    #[error("pipeline error: {0}")]
    Pipeline(String),

    /// The transport is unusable for every record: closed, timed out, the
    /// broker or topic gone, not authorised. The tick aborts with no
    /// checkpoint and the scheduler re-fetches.
    #[error("transport error: {0}")]
    Transport(String),

    /// The transport refused ONE record for what the record is (its size,
    /// its format) and would refuse it again: the record is dead-lettered
    /// whole and the rest of the batch goes on.
    #[error("transport refused the record: {0}")]
    TransportRecord(String),

    /// The destination is full and the batch must be held.
    ///
    /// Distinct from [`Error::Transport`] because it is not a delivery failure:
    /// the records are still good and the cursor must NOT advance past them.
    /// Never dead-lettered -- a DLQ is for records that cannot be delivered,
    /// and on the direct transport there is no broker holding one.
    #[error("destination backpressured: {0}")]
    Backpressured(String),

    /// Cursor store error.
    #[error("cursor error: {0}")]
    Cursor(String),

    /// Filter/expression evaluation error.
    #[error("filter error: {0}")]
    Filter(String),

    /// Shutdown requested.
    #[error("shutdown requested")]
    Shutdown,

    /// Secrets management error.
    #[error("secrets error: {0}")]
    Secrets(#[from] scalo::SecretsError),

    /// Dead letter queue error.
    #[error("DLQ error: {0}")]
    Dlq(#[from] scalo::dlq::DlqError),

    /// An error raised inside the source framework (a shape, the batcher, the
    /// rules or a checkpoint), carried whole so its typed HTTP status survives
    /// to the metrics label.
    #[error(transparent)]
    Framework(#[from] dfe_fetcher_core::Error),
}

/// Result type alias for dfe-fetcher operations.
pub type Result<T> = std::result::Result<T, Error>;

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Config(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Config(s.to_string())
    }
}

impl From<serde_yaml_ng::Error> for Error {
    fn from(err: serde_yaml_ng::Error) -> Self {
        Error::Config(format!("YAML parse error: {err}"))
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Source(format!("JSON error: {err}"))
    }
}

impl From<scalo::secrets::CredentialError> for Error {
    fn from(e: scalo::secrets::CredentialError) -> Self {
        Error::Credential(e.to_string())
    }
}

/// Convert errors to HTTP responses for axum health/metrics handlers.
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Error::Config(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            Error::Source(msg) => (StatusCode::BAD_GATEWAY, msg.clone()),
            Error::Credential(msg) => (StatusCode::UNAUTHORIZED, msg.clone()),
            Error::Kafka(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "kafka unavailable".to_string(),
            ),
            Error::Transport(msg) | Error::TransportRecord(msg) | Error::Backpressured(msg) => {
                (StatusCode::SERVICE_UNAVAILABLE, msg.clone())
            }
            Error::Cursor(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            Error::Filter(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            Error::Shutdown => (StatusCode::SERVICE_UNAVAILABLE, "shutting down".to_string()),
            Error::Framework(inner) => match inner {
                dfe_fetcher_core::Error::Backpressured(_) => {
                    (StatusCode::SERVICE_UNAVAILABLE, inner.to_string())
                }
                dfe_fetcher_core::Error::Credential(_) => {
                    (StatusCode::UNAUTHORIZED, inner.to_string())
                }
                dfe_fetcher_core::Error::Api { .. }
                | dfe_fetcher_core::Error::Source(_)
                | dfe_fetcher_core::Error::OriginRefused(_) => {
                    (StatusCode::BAD_GATEWAY, inner.to_string())
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, inner.to_string()),
            },
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            ),
        };

        let body = serde_json::json!({
            "error": message,
        });

        (status, axum::Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::Source("API rate limited".to_string());
        assert_eq!(err.to_string(), "source error: API rate limited");
    }

    #[test]
    fn test_error_from_string() {
        let err: Error = "test error".into();
        assert!(matches!(err, Error::Config(_)));
    }

    // --- Display tests for all variants ---

    #[test]
    fn test_display_config() {
        let err = Error::Config("bad yaml".to_string());
        assert_eq!(err.to_string(), "configuration error: bad yaml");
    }

    #[test]
    fn test_display_source() {
        let err = Error::Source("API down".to_string());
        assert_eq!(err.to_string(), "source error: API down");
    }

    #[test]
    fn test_display_credential() {
        let err = Error::Credential("expired token".to_string());
        assert_eq!(err.to_string(), "credential error: expired token");
    }

    #[test]
    fn test_display_kafka() {
        let err = Error::Kafka("broker unreachable".to_string());
        assert_eq!(err.to_string(), "Kafka error: broker unreachable");
    }

    #[test]
    fn test_display_transport() {
        let err = Error::Transport("gRPC channel closed".to_string());
        assert_eq!(err.to_string(), "transport error: gRPC channel closed");
    }

    #[test]
    fn test_display_cursor() {
        let err = Error::Cursor("file locked".to_string());
        assert_eq!(err.to_string(), "cursor error: file locked");
    }

    #[test]
    fn test_display_filter() {
        let err = Error::Filter("invalid CEL".to_string());
        assert_eq!(err.to_string(), "filter error: invalid CEL");
    }

    #[test]
    fn test_display_shutdown() {
        let err = Error::Shutdown;
        assert_eq!(err.to_string(), "shutdown requested");
    }

    #[test]
    fn test_display_pipeline() {
        let err = Error::Pipeline("enrichment failed".to_string());
        assert_eq!(err.to_string(), "pipeline error: enrichment failed");
    }

    #[test]
    fn test_display_scheduler() {
        let err = Error::Scheduler("tick overflow".to_string());
        assert_eq!(err.to_string(), "scheduler error: tick overflow");
    }

    #[test]
    fn test_display_io() {
        let err = Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        assert_eq!(err.to_string(), "I/O error: gone");
    }

    // --- From conversions ---

    #[test]
    fn test_from_serde_yaml_error() {
        let yaml_err = serde_yaml_ng::from_str::<serde_json::Value>("{{{{").unwrap_err();
        let err: Error = yaml_err.into();
        match &err {
            Error::Config(msg) => assert!(
                msg.starts_with("YAML parse error"),
                "expected YAML parse error prefix, got: {msg}"
            ),
            other => panic!("expected Error::Config, got: {other:?}"),
        }
    }

    #[test]
    fn test_from_serde_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("{bad}").unwrap_err();
        let err: Error = json_err.into();
        match &err {
            Error::Source(msg) => assert!(
                msg.starts_with("JSON error"),
                "expected JSON error prefix, got: {msg}"
            ),
            other => panic!("expected Error::Source, got: {other:?}"),
        }
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn test_from_str_ref() {
        let err: Error = "some config issue".into();
        assert!(matches!(err, Error::Config(ref s) if s == "some config issue"));
    }

    #[test]
    fn test_from_owned_string() {
        let err: Error = String::from("owned error").into();
        assert!(matches!(err, Error::Config(ref s) if s == "owned error"));
    }

    // --- IntoResponse status code tests ---

    fn extract_status_and_body(err: Error) -> (StatusCode, serde_json::Value) {
        let response = err.into_response();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX);
        // Use a blocking approach since these are sync tests -- build a mini runtime.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("build runtime");
        let bytes = rt.block_on(body).expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("parse response JSON");
        (status, json)
    }

    #[test]
    fn test_into_response_config() {
        let (status, body) = extract_status_and_body(Error::Config("bad".into()));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"], "bad");
    }

    #[test]
    fn test_into_response_source() {
        let (status, body) = extract_status_and_body(Error::Source("api fail".into()));
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body["error"], "api fail");
    }

    #[test]
    fn test_into_response_credential() {
        let (status, body) = extract_status_and_body(Error::Credential("bad token".into()));
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"], "bad token");
    }

    #[test]
    fn test_into_response_kafka() {
        let (status, body) = extract_status_and_body(Error::Kafka("down".into()));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "kafka unavailable");
    }

    #[test]
    fn test_into_response_transport() {
        let (status, body) = extract_status_and_body(Error::Transport("broken".into()));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "broken");
    }

    #[test]
    fn test_into_response_cursor() {
        let (status, body) = extract_status_and_body(Error::Cursor("corrupt".into()));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"], "corrupt");
    }

    #[test]
    fn test_into_response_filter() {
        let (status, body) = extract_status_and_body(Error::Filter("bad CEL".into()));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"], "bad CEL");
    }

    #[test]
    fn test_into_response_shutdown() {
        let (status, body) = extract_status_and_body(Error::Shutdown);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "shutting down");
    }

    #[test]
    fn test_into_response_io_wildcard() {
        let io_err = std::io::Error::other("disk full");
        let (status, body) = extract_status_and_body(Error::Io(io_err));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"], "internal error");
    }

    #[test]
    fn test_into_response_pipeline() {
        let (status, body) = extract_status_and_body(Error::Pipeline("broken pipe".into()));
        // Pipeline falls through to the wildcard arm
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"], "internal error");
    }

    #[test]
    fn test_into_response_scheduler() {
        let (status, body) = extract_status_and_body(Error::Scheduler("overrun".into()));
        // Scheduler falls through to the wildcard arm
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"], "internal error");
    }

    #[test]
    fn test_into_response_body_has_error_key() {
        let (_, body) = extract_status_and_body(Error::Config("test".into()));
        assert!(
            body.get("error").is_some(),
            "response must contain 'error' key"
        );
    }
}
