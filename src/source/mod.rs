// Project:   dfe-fetcher
// File:      src/source/mod.rs
// Purpose:   Source trait and native provider implementations
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Source module for fetching data from external services.
//!
//! Provides the `Source` trait that all native data providers implement.
//! Each provider (AWS, Azure, M365, GCP) lives in its own submodule.
//!
//! For external extractors (non-Rust, containerised), see the `extractor` module.

pub mod aws;
pub mod azure;
pub mod gcp;
pub mod m365;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::error::Result;

/// Time window for incremental fetching.
///
/// When provided to `Source::fetch`, the source should restrict its query
/// to events within `[start, end)`. When `None`, sources fall back to
/// their own default window (typically "last N hours").
#[derive(Debug, Clone)]
pub struct FetchWindow {
    /// Inclusive start of the window.
    pub start: DateTime<Utc>,
    /// Exclusive end of the window.
    pub end: DateTime<Utc>,
}

/// A batch of fetched records ready for delivery to the pipeline.
#[derive(Debug, Clone)]
pub struct FetchResult {
    /// Records fetched (each is a JSON payload).
    pub records: Vec<Bytes>,

    /// Source identifier (e.g., "aws.cloudtrail", "azure.defender").
    pub source: String,

    /// Target Kafka topic for these records.
    pub topic: String,
}

/// Trait for native data sources (AWS, Azure, M365, GCP).
///
/// Each source implementation is responsible for:
/// - Authenticating with the external service
/// - Fetching data (with pagination, cursors, etc.)
/// - Converting responses to JSON `Bytes` payloads
/// - Tracking state (last fetch timestamp, cursors) for incremental fetching
#[async_trait]
pub trait Source: Send + Sync {
    /// Human-readable source name (e.g., "aws", "azure").
    fn name(&self) -> &'static str;

    /// Check if the source is enabled in configuration.
    fn is_enabled(&self) -> bool;

    /// Fetch data from the external service.
    ///
    /// When `window` is `Some`, the source should restrict its query to the
    /// given time range. When `None`, sources use their own default window.
    ///
    /// Returns a list of fetch results (one per service/sub-source).
    /// Each result contains records ready for Kafka delivery.
    async fn fetch(&self, window: Option<&FetchWindow>) -> Result<Vec<FetchResult>>;

    /// Check if the source is healthy (credentials valid, API reachable).
    async fn health_check(&self) -> Result<bool>;

    /// Prefix used for cursor keys in the cursor store.
    ///
    /// Defaults to the source name. Override if a more specific prefix is needed.
    fn cursor_prefix(&self) -> String {
        self.name().to_string()
    }

    /// List of configured service names within this source.
    ///
    /// Used by the cursor store to track per-service fetch positions.
    fn service_names(&self) -> Vec<&str> {
        vec![]
    }
}

/// Classify an HTTP/API error into a bounded category for metrics.
///
/// Returns one of: "4xx", "5xx", "timeout", "network"
pub fn classify_api_error(error: &crate::error::Error) -> &'static str {
    let msg = error.to_string();
    if msg.contains("timed out") || msg.contains("timeout") {
        "timeout"
    } else if msg.contains("status: 4")
        || msg.contains("401")
        || msg.contains("403")
        || msg.contains("404")
        || msg.contains("429")
    {
        "4xx"
    } else if msg.contains("status: 5")
        || msg.contains("500")
        || msg.contains("502")
        || msg.contains("503")
    {
        "5xx"
    } else {
        "network"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetch_result() {
        let result = FetchResult {
            records: vec![Bytes::from(r#"{"test": "data"}"#)],
            source: "aws.cloudtrail".to_string(),
            topic: "aws_land".to_string(),
        };
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.source, "aws.cloudtrail");
    }
}
