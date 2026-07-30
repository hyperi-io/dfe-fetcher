// Project:   dfe-fetcher
// File:      src/extractor/mod.rs
// Purpose:   External data extractor management (containers, vector)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! External extractor management.
//!
//! Manages non-native data extractors that run outside the core fetcher:
//!
//! ## Extraction Modes
//!
//! 1. **Container extractors** (`container/`) — Isolated containers (Docker/podman)
//!    running third-party tools. Managed as child processes. Each container
//!    outputs JSON lines to stdout or posts to the fetcher's ingest endpoint.
//!    Example: `yet-another-cloudwatch-exporter` for CloudWatch metrics.
//!
//! 2. **Vector extractors** (`vector/`) — Vector.dev instances configured as
//!    sources that send data to the fetcher via native gRPC (Vector sink protocol).
//!    Tightly coupled via scalo's gRPC support.
//!
//! ## Design Philosophy
//!
//! - If a good Rust crate exists → build natively in `src/source/`
//! - If a great OSS tool exists in another language → wrap in container
//! - One container per source + config (no horizontal scaling needed)
//! - Multiple instances of same type with different configs (e.g., 10 M365 orgs)
//!
//! ## Container Communication
//!
//! Containers communicate with the fetcher via:
//! - **stdout** — JSON lines protocol (container writes, fetcher reads)
//! - **HTTP POST** — Container posts to fetcher's `/ingest` endpoint
//! - **gRPC** — Vector protocol for Vector-based extractors

pub mod container;
pub mod vector;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;
use crate::source::FetchResult;

/// Trait for external extractors (containers, vector instances).
///
/// Unlike `Source` which actively pulls data, extractors are managed processes
/// that push data to the fetcher. The fetcher starts/stops/monitors them.
#[async_trait]
pub trait Extractor: Send + Sync {
    /// Human-readable extractor name.
    fn name(&self) -> &str;

    /// Unique instance ID (for multiple instances of same extractor type).
    fn instance_id(&self) -> &str;

    /// Start the extractor process.
    async fn start(&self) -> Result<()>;

    /// Stop the extractor process gracefully.
    async fn stop(&self) -> Result<()>;

    /// Check if the extractor is running.
    fn is_running(&self) -> bool;

    /// Check if the extractor is healthy.
    async fn health_check(&self) -> Result<bool>;
}

/// Ingest endpoint message received from an extractor.
#[derive(Debug, Clone)]
pub struct IngestMessage {
    /// Raw JSON payload.
    pub payload: Bytes,

    /// Source identifier (set by extractor or derived from config).
    pub source: String,

    /// Target Kafka topic.
    pub topic: String,
}

impl IngestMessage {
    /// Convert to a FetchResult with a single record.
    pub fn into_fetch_result(self) -> FetchResult {
        FetchResult {
            records: vec![self.payload],
            source: self.source,
            topic: self.topic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingest_message_into_fetch_result() {
        let msg = IngestMessage {
            payload: Bytes::from(r#"{"event":"test","severity":"high"}"#),
            source: "container.my-tool".to_string(),
            topic: "security_events".to_string(),
        };

        let result = msg.into_fetch_result();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.source, "container.my-tool");
        assert_eq!(result.topic, "security_events");
        assert_eq!(
            result.records[0].as_ref(),
            br#"{"event":"test","severity":"high"}"#
        );
    }

    #[test]
    fn test_ingest_message_preserves_binary_payload() {
        let binary = vec![0u8, 1, 2, 255, 254, 253];
        let msg = IngestMessage {
            payload: Bytes::from(binary.clone()),
            source: "raw".to_string(),
            topic: "raw_land".to_string(),
        };

        let result = msg.into_fetch_result();
        assert_eq!(result.records[0].as_ref(), &binary[..]);
    }

    #[test]
    fn test_ingest_message_empty_payload() {
        let msg = IngestMessage {
            payload: Bytes::new(),
            source: "empty".to_string(),
            topic: "empty_land".to_string(),
        };

        let result = msg.into_fetch_result();
        assert_eq!(result.records.len(), 1);
        assert!(result.records[0].is_empty());
    }

    #[test]
    fn test_ingest_message_large_payload() {
        let large = Bytes::from(vec![b'x'; 10 * 1024 * 1024]); // 10MB
        let msg = IngestMessage {
            payload: large.clone(),
            source: "bulk".to_string(),
            topic: "bulk_land".to_string(),
        };

        let result = msg.into_fetch_result();
        assert_eq!(result.records[0].len(), 10 * 1024 * 1024);
    }
}
