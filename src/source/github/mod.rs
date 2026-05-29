// Project:   dfe-fetcher
// File:      src/source/github/mod.rs
// Purpose:   GitHub audit-log source
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! GitHub audit-log source.
//!
//! Fetches audit-log events from either a GitHub organisation
//! (`/orgs/{org}/audit-log`) or a GitHub Enterprise Cloud account
//! (`/enterprises/{enterprise}/audit-log`).
//!
//! Authentication: bearer token (Personal Access Token with `read:audit_log`
//! for org scope or `read:enterprise` for enterprise scope, fine-grained PAT,
//! or GitHub App installation token).
//!
//! Pagination follows the `Link` HTTP header (`rel="next"`). The API returns
//! results as a JSON array, not wrapped in a `{value: [...]}` envelope.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use crate::config::{GithubService, GithubSourceConfig};
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source, parse_link_next_url};

/// Default lookback when no `FetchWindow` is supplied.
const DEFAULT_LOOKBACK_HOURS: i64 = 1;

/// Per-request budget for the audit-log endpoint. GitHub caps `per_page` at 100.
const PAGE_SIZE: usize = 100;

/// Maximum pages we will follow per fetch call. Prevents unbounded paging
/// under rare runaway conditions; normal traffic completes in a handful of
/// pages per tick.
const MAX_PAGES: usize = 50;

/// Per-request HTTP timeout. The audit-log endpoint is slower than other
/// GitHub APIs - 30s is the documented worst-case for high-cardinality orgs.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// GitHub audit-log data source.
pub struct GithubSource {
    config: GithubSourceConfig,
    client: reqwest::Client,
}

impl GithubSource {
    /// Create a new GitHub source from configuration.
    pub fn new(config: GithubSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    fn api_base(&self) -> &str {
        self.config
            .api_url_override
            .as_deref()
            .unwrap_or("https://api.github.com")
    }

    /// Resolve the bearer token from `credential_secret` or the literal `token`.
    async fn resolve_token(&self) -> Result<String> {
        if let Some(ref spec) = self.config.credential_secret {
            return credential::resolve(spec).await;
        }
        self.config
            .token
            .as_ref()
            .map(|s| s.expose().to_string())
            .ok_or_else(|| Error::Credential("github.token is required".into()))
    }

    /// Return the audit-log URL path component for either the org or
    /// enterprise scope. Caller appends query params.
    fn audit_log_path(&self) -> Result<String> {
        match (
            self.config.org.as_deref(),
            self.config.enterprise.as_deref(),
        ) {
            (Some(org), None) if !org.is_empty() => Ok(format!("orgs/{org}/audit-log")),
            (None, Some(ent)) if !ent.is_empty() => Ok(format!("enterprises/{ent}/audit-log")),
            (Some(_), Some(_)) => Err(Error::Config(
                "github source must set exactly one of `org` or `enterprise`, not both".into(),
            )),
            _ => Err(Error::Config(
                "github source requires `org` or `enterprise` to be set".into(),
            )),
        }
    }

    /// Compute the effective `[start, end)` time window for this fetch.
    fn window(&self, window: Option<&FetchWindow>) -> (DateTime<Utc>, DateTime<Utc>) {
        match window {
            Some(w) => (w.start, w.end),
            None => {
                let now = Utc::now();
                (now - chrono::Duration::hours(DEFAULT_LOOKBACK_HOURS), now)
            }
        }
    }

    /// Format an ISO-8601 timestamp the GitHub audit-log `phrase` filter
    /// accepts (`YYYY-MM-DDTHH:MM:SS+00:00`).
    fn format_phrase_time(ts: DateTime<Utc>) -> String {
        ts.format("%Y-%m-%dT%H:%M:%S+00:00").to_string()
    }

    /// Fetch the audit-log service. Single endpoint, optional `include`
    /// service-config to filter `web` / `git` events.
    async fn fetch_audit_log(
        &self,
        service: &GithubService,
        window: Option<&FetchWindow>,
    ) -> Result<Option<FetchResult>> {
        let token = self.resolve_token().await?;
        let path = self.audit_log_path()?;
        let (start, end) = self.window(window);

        let include = service
            .config
            .get("include")
            .and_then(|v| v.as_str())
            .filter(|s| matches!(*s, "all" | "web" | "git"))
            .unwrap_or("all");

        let phrase = format!(
            "created:{}..{}",
            Self::format_phrase_time(start),
            Self::format_phrase_time(end),
        );

        let initial_url = format!(
            "{}/{}?per_page={}&include={}&phrase={}",
            self.api_base(),
            path,
            PAGE_SIZE,
            include,
            urlencoding_encode(&phrase),
        );

        let mut next_url: Option<String> = Some(initial_url);
        let mut records: Vec<Bytes> = Vec::new();

        for page in 0..MAX_PAGES {
            let Some(url) = next_url.take() else { break };
            let resp = self
                .client
                .get(&url)
                .bearer_auth(&token)
                .header("Accept", "application/vnd.github+json")
                .header("X-GitHub-Api-Version", "2022-11-28")
                .send()
                .await
                .map_err(|e| Error::Source(format!("GitHub audit-log request failed: {e}")))?;

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "GitHub audit-log returned {status}: {body}"
                )));
            }

            let header_next = parse_link_next_url(resp.headers());

            let items: Vec<serde_json::Value> = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("GitHub audit-log parse failed: {e}")))?;

            for item in &items {
                if let Ok(buf) = serde_json::to_vec(item) {
                    records.push(Bytes::from(buf));
                }
            }

            debug!(
                page = page + 1,
                items = items.len(),
                total = records.len(),
                "GitHub audit-log page fetched"
            );

            next_url = header_next;
        }

        if records.is_empty() {
            return Ok(None);
        }

        info!(records = records.len(), include, "GitHub audit-log fetched");
        Ok(Some(FetchResult {
            records,
            source: "github.audit_log".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}

#[async_trait]
impl Source for GithubSource {
    fn name(&self) -> &'static str {
        "github"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn fetch(&self, window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        info!(
            services = self.config.services.len(),
            "Fetching GitHub data"
        );

        let mut results: Vec<FetchResult> = Vec::new();
        for service in &self.config.services {
            let outcome = match service.name.as_str() {
                "audit_log" => self.fetch_audit_log(service, window).await,
                other => {
                    warn!(service = other, "Unknown GitHub service, skipping");
                    continue;
                }
            };
            match outcome {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => warn!(
                    error = %e,
                    service = %service.name,
                    "GitHub service fetch failed, continuing"
                ),
            }
        }
        Ok(results)
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        // Hitting /user is cheap and verifies token + reachability.
        let token = match self.resolve_token().await {
            Ok(t) => t,
            Err(_) => return Ok(false),
        };
        let url = format!("{}/user", self.api_base());
        let resp = match self
            .client
            .get(&url)
            .bearer_auth(&token)
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => return Ok(false),
        };
        Ok(resp.status().is_success())
    }

    fn cursor_prefix(&self) -> String {
        "github".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        self.config
            .services
            .iter()
            .map(|s| s.name.as_str())
            .collect()
    }
}

/// Minimal RFC 3986 percent-encoder for query-string values. We only ever feed
/// it ASCII timestamp / scope strings, so a hand-rolled table-free encoder is
/// simpler than dragging in another dependency. Encodes everything outside the
/// unreserved set.
fn urlencoding_encode(input: &str) -> String {
    const UNRESERVED: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                                abcdefghijklmnopqrstuvwxyz\
                                0123456789-_.~";
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        if UNRESERVED.contains(&b) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push_str(&format!("{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(org: Option<&str>, enterprise: Option<&str>) -> GithubSourceConfig {
        GithubSourceConfig {
            enabled: true,
            org: org.map(String::from),
            enterprise: enterprise.map(String::from),
            token: Some("test-token".to_string().into()),
            ..GithubSourceConfig::default()
        }
    }

    #[test]
    fn audit_log_path_org() {
        let src = GithubSource::new(cfg(Some("hyperi"), None));
        assert_eq!(src.audit_log_path().unwrap(), "orgs/hyperi/audit-log");
    }

    #[test]
    fn audit_log_path_enterprise() {
        let src = GithubSource::new(cfg(None, Some("hyperi-ent")));
        assert_eq!(
            src.audit_log_path().unwrap(),
            "enterprises/hyperi-ent/audit-log"
        );
    }

    #[test]
    fn audit_log_path_rejects_both_set() {
        let src = GithubSource::new(cfg(Some("hyperi"), Some("hyperi-ent")));
        assert!(src.audit_log_path().is_err());
    }

    #[test]
    fn audit_log_path_rejects_neither_set() {
        let src = GithubSource::new(cfg(None, None));
        assert!(src.audit_log_path().is_err());
    }

    #[test]
    fn audit_log_path_rejects_empty_strings() {
        let src = GithubSource::new(cfg(Some(""), None));
        assert!(src.audit_log_path().is_err());
    }

    #[test]
    fn format_phrase_time_iso8601_seconds_no_fractional() {
        let t = DateTime::parse_from_rfc3339("2026-05-21T13:30:00.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            GithubSource::format_phrase_time(t),
            "2026-05-21T13:30:00+00:00"
        );
    }

    #[test]
    fn urlencoding_encode_passes_unreserved_through() {
        assert_eq!(urlencoding_encode("Hello.world-1_2~3"), "Hello.world-1_2~3");
    }

    #[test]
    fn urlencoding_encode_encodes_colons_and_dots_safely() {
        // colon and double-dot in created:...
        assert_eq!(
            urlencoding_encode("created:2026-05-21T00:00:00+00:00..2026-05-21T01:00:00+00:00"),
            "created%3A2026-05-21T00%3A00%3A00%2B00%3A00..2026-05-21T01%3A00%3A00%2B00%3A00"
        );
    }
}
