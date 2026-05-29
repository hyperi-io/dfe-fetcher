// Project:   dfe-fetcher
// File:      src/source/mod.rs
// Purpose:   Source trait and native provider implementations
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Source module for fetching data from external services.
//!
//! Provides the `Source` trait that all native data providers implement.
//! Each provider (AWS, Azure, M365, GCP) lives in its own submodule.
//!
//! For external extractors (non-Rust, containerised), see the `extractor` module.

pub mod aws;
pub mod azure;
pub mod bitwarden;
pub mod cloudflare;
pub mod crates_io;
pub mod crowdstrike;
pub mod duo;
pub mod gcp;
pub mod gcp_pubsub;
pub mod github;
pub mod go_modules;
pub mod google_workspace;
pub mod m365;
pub mod object_store;
pub mod okta;
pub mod onepassword;
pub mod pypi;
pub mod salesforce;
pub mod slack;

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

/// Release-maturity stage of a source, used for runtime warnings and docs.
///
/// The progression is `Alpha -> Beta -> Stable`:
/// - `Alpha`   - code-complete but not production-validated. Default for all
///   sources. Use at your own risk; behaviour and config may change.
/// - `Beta`    - validated against a live service, hardening in progress.
/// - `Stable`  - production-ready. The four core sources (aws, azure, m365,
///   gcp) are stable; everything else is currently alpha.
///
/// New sources start at `Alpha` (the trait default) until explicitly promoted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceMaturity {
    /// Code-complete, not production-validated.
    Alpha,
    /// Live-validated, hardening in progress.
    Beta,
    /// Production-ready.
    Stable,
}

impl std::fmt::Display for SourceMaturity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            SourceMaturity::Alpha => "alpha",
            SourceMaturity::Beta => "beta",
            SourceMaturity::Stable => "stable",
        };
        f.write_str(s)
    }
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

    /// Release-maturity stage of this source.
    ///
    /// Defaults to [`SourceMaturity::Alpha`] - new sources are alpha until
    /// explicitly promoted. The four core sources (aws, azure, m365, gcp)
    /// override this to [`SourceMaturity::Stable`]. The orchestrator logs a
    /// warning at startup for any enabled non-stable source.
    fn maturity(&self) -> SourceMaturity {
        SourceMaturity::Alpha
    }

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

/// Return the first `Link`-header URL whose relation is `next` (RFC 5988).
///
/// Used by sources whose APIs paginate via `Link` headers: GitHub audit log,
/// Okta system log, etc. Handles:
///
/// - Comma-separated link entries within a single header
/// - Multiple `Link` headers (some servers split entries across headers)
/// - Both quoted (`rel="next"`) and unquoted (`rel=next`) relation parameters
pub fn parse_link_next_url(headers: &reqwest::header::HeaderMap) -> Option<String> {
    for header_value in &headers.get_all(reqwest::header::LINK) {
        let Ok(h) = header_value.to_str() else {
            continue;
        };
        if let Some(url) = parse_link_next_url_from_str(h) {
            return Some(url);
        }
    }
    None
}

/// Parse a single `Link` header value for the `rel="next"` URL.
///
/// Exposed separately so tests can supply arbitrary header strings without
/// building a fake `HeaderMap`.
pub fn parse_link_next_url_from_str(link_header: &str) -> Option<String> {
    for part in link_header.split(',') {
        let part = part.trim();
        let Some((url_part, rels)) = part.split_once(';') else {
            continue;
        };
        if !rels.split(';').any(|p| {
            let p = p.trim();
            p == "rel=\"next\"" || p == "rel=next"
        }) {
            continue;
        }
        let url = url_part.trim();
        if let Some(stripped) = url.strip_prefix('<').and_then(|s| s.strip_suffix('>')) {
            return Some(stripped.to_string());
        }
    }
    None
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

    // --- classify_api_error tests ---

    #[test]
    fn test_classify_timed_out() {
        let err = crate::error::Error::Source("request timed out".to_string());
        assert_eq!(classify_api_error(&err), "timeout");
    }

    #[test]
    fn test_classify_timeout_keyword() {
        let err = crate::error::Error::Source("connection timeout reached".to_string());
        assert_eq!(classify_api_error(&err), "timeout");
    }

    #[test]
    fn test_classify_status_4xx_prefix() {
        let err = crate::error::Error::Source("HTTP status: 422 Unprocessable".to_string());
        assert_eq!(classify_api_error(&err), "4xx");
    }

    #[test]
    fn test_classify_401() {
        let err = crate::error::Error::Source("received 401 Unauthorized".to_string());
        assert_eq!(classify_api_error(&err), "4xx");
    }

    #[test]
    fn test_classify_403() {
        let err = crate::error::Error::Source("responded with 403 Forbidden".to_string());
        assert_eq!(classify_api_error(&err), "4xx");
    }

    #[test]
    fn test_classify_404() {
        let err = crate::error::Error::Source("endpoint returned 404".to_string());
        assert_eq!(classify_api_error(&err), "4xx");
    }

    #[test]
    fn test_classify_429() {
        let err = crate::error::Error::Source("rate limited with 429".to_string());
        assert_eq!(classify_api_error(&err), "4xx");
    }

    #[test]
    fn test_classify_status_5xx_prefix() {
        let err = crate::error::Error::Source("server status: 504 Gateway Timeout".to_string());
        assert_eq!(classify_api_error(&err), "5xx");
    }

    #[test]
    fn test_classify_500() {
        let err = crate::error::Error::Source("internal 500 error".to_string());
        assert_eq!(classify_api_error(&err), "5xx");
    }

    #[test]
    fn test_classify_502() {
        let err = crate::error::Error::Source("bad gateway 502".to_string());
        assert_eq!(classify_api_error(&err), "5xx");
    }

    #[test]
    fn test_classify_503() {
        let err = crate::error::Error::Source("service unavailable 503".to_string());
        assert_eq!(classify_api_error(&err), "5xx");
    }

    #[test]
    fn test_classify_connection_refused() {
        let err = crate::error::Error::Source("connection refused".to_string());
        assert_eq!(classify_api_error(&err), "network");
    }

    #[test]
    fn test_classify_dns_failure() {
        let err = crate::error::Error::Source("DNS resolution failed".to_string());
        assert_eq!(classify_api_error(&err), "network");
    }

    #[test]
    fn test_classify_empty_message() {
        let err = crate::error::Error::Source(String::new());
        assert_eq!(classify_api_error(&err), "network");
    }

    // --- parse_link_next_url tests ---

    #[test]
    fn link_next_picks_rel_next_quoted() {
        let h = r#"<https://api.example.com/x?page=2>; rel="next", <https://api.example.com/x?page=10>; rel="last""#;
        assert_eq!(
            parse_link_next_url_from_str(h).unwrap(),
            "https://api.example.com/x?page=2"
        );
    }

    #[test]
    fn link_next_picks_rel_next_unquoted() {
        let h = r#"<https://api.example.com/x?page=2>; rel=next"#;
        assert_eq!(
            parse_link_next_url_from_str(h).unwrap(),
            "https://api.example.com/x?page=2"
        );
    }

    #[test]
    fn link_next_returns_none_when_only_last() {
        let h = r#"<https://api.example.com/x?page=10>; rel="last""#;
        assert!(parse_link_next_url_from_str(h).is_none());
    }

    #[test]
    fn link_next_returns_none_for_empty_header() {
        assert!(parse_link_next_url_from_str("").is_none());
    }

    #[test]
    fn link_next_returns_none_when_url_lacks_brackets() {
        // Defence-in-depth against malformed Link headers.
        let h = r#"https://api.example.com/x?page=2; rel="next""#;
        assert!(parse_link_next_url_from_str(h).is_none());
    }

    #[test]
    fn link_next_picks_first_when_split_across_entries() {
        // Some servers send multiple `<...>; rel=...` entries in one header.
        let h =
            r#"<https://api.example.com/a>; rel="prev", <https://api.example.com/b>; rel="next""#;
        assert_eq!(
            parse_link_next_url_from_str(h).unwrap(),
            "https://api.example.com/b"
        );
    }
}
