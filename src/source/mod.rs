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

use crate::error::Result;

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
    /// Returns a list of fetch results (one per service/sub-source).
    /// Each result contains records ready for Kafka delivery.
    async fn fetch(&self) -> Result<Vec<FetchResult>>;

    /// Check if the source is healthy (credentials valid, API reachable).
    async fn health_check(&self) -> Result<bool>;
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
