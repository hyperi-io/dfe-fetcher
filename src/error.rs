// Project:   dfe-fetcher
// File:      src/error.rs
// Purpose:   Centralised error types
// Language:  Rust
//
// License:   FSL-1.1-ALv2
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
    Kafka(#[from] rdkafka::error::KafkaError),

    /// HTTP client error.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Scheduling error.
    #[error("scheduler error: {0}")]
    Scheduler(String),

    /// Pipeline processing error.
    #[error("pipeline error: {0}")]
    Pipeline(String),

    /// Shutdown requested.
    #[error("shutdown requested")]
    Shutdown,

    /// Secrets management error.
    #[error("secrets error: {0}")]
    Secrets(#[from] hyperi_rustlib::SecretsError),
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
            Error::Shutdown => (StatusCode::SERVICE_UNAVAILABLE, "shutting down".to_string()),
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
}
