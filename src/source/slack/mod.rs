// Project:   dfe-fetcher
// File:      src/source/slack/mod.rs
// Purpose:   Slack audit-log source
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Slack audit-log source.
//!
//! Pulls audit-log entries from `https://api.slack.com/audit/v1/logs`.
//! Enterprise Grid only - the audit API isn't available to Pro/Business+.
//!
//! Authentication: org-admin user token (`xoxa-...` or `xoxb-...`) with
//! the `auditlogs:read` scope.
//!
//! Pagination: cursor-based via response `response_metadata.next_cursor`,
//! request `?cursor=`.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use crate::config::{SlackService, SlackSourceConfig};
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

/// Default lookback when no `FetchWindow` is supplied.
const DEFAULT_LOOKBACK_HOURS: i64 = 1;

/// Default per-page size. Slack caps `limit` at 1000.
const DEFAULT_LIMIT: u32 = 200;

/// Maximum pages we will follow per fetch call.
const MAX_PAGES: usize = 50;

/// Per-request HTTP timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Slack audit-log data source.
pub struct SlackSource {
    config: SlackSourceConfig,
    client: reqwest::Client,
}

impl SlackSource {
    /// Create a new Slack source from configuration.
    pub fn new(config: SlackSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    fn api_base(&self) -> &str {
        self.config
            .api_url_override
            .as_deref()
            .unwrap_or("https://api.slack.com")
    }

    async fn resolve_token(&self) -> Result<String> {
        if let Some(ref spec) = self.config.credential_secret {
            return Ok(credential::resolve(spec).await?);
        }
        self.config
            .token
            .as_ref()
            .map(|s| s.expose().to_string())
            .ok_or_else(|| Error::Credential("slack.token is required".into()))
    }

    fn window(&self, window: Option<&FetchWindow>) -> (DateTime<Utc>, DateTime<Utc>) {
        match window {
            Some(w) => (w.start, w.end),
            None => {
                let now = Utc::now();
                (now - chrono::Duration::hours(DEFAULT_LOOKBACK_HOURS), now)
            }
        }
    }

    async fn fetch_audit_logs(
        &self,
        service: &SlackService,
        window: Option<&FetchWindow>,
    ) -> Result<Option<FetchResult>> {
        let token = self.resolve_token().await?;
        let (start, end) = self.window(window);

        let limit = service
            .config
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| n.min(1000) as u32)
            .unwrap_or(DEFAULT_LIMIT);

        // Build URL with oldest / latest unix timestamps. `oldest` is
        // inclusive, `latest` is exclusive (per Slack docs).
        let mut base = format!(
            "{}/audit/v1/logs?oldest={}&latest={}&limit={}",
            self.api_base(),
            start.timestamp(),
            end.timestamp(),
            limit,
        );
        if let Some(action) = service.config.get("action").and_then(|v| v.as_str()) {
            base.push_str("&action=");
            base.push_str(&percent_encode(action));
        }
        if let Some(entity) = service.config.get("entity").and_then(|v| v.as_str()) {
            base.push_str("&entity=");
            base.push_str(&percent_encode(entity));
        }

        let mut records: Vec<Bytes> = Vec::new();
        let mut cursor: Option<String> = None;

        for page in 0..MAX_PAGES {
            let url = match cursor.as_ref() {
                Some(c) => format!("{base}&cursor={}", percent_encode(c)),
                None => base.clone(),
            };

            let resp = self
                .client
                .get(&url)
                .bearer_auth(&token)
                .header("Accept", "application/json")
                .send()
                .await
                .map_err(|e| Error::Source(format!("Slack audit-log request failed: {e}")))?;

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "Slack audit-log returned {status}: {body}"
                )));
            }

            let envelope: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("Slack audit-log parse failed: {e}")))?;

            // Slack returns `{"ok": false, "error": "..."}` even on a 200.
            if envelope["ok"].as_bool() == Some(false) {
                let err = envelope["error"].as_str().unwrap_or("unknown");
                return Err(Error::Source(format!(
                    "Slack audit-log API ok=false: {err}"
                )));
            }

            let entries = envelope["entries"].as_array().cloned().unwrap_or_default();
            for entry in &entries {
                if let Ok(buf) = serde_json::to_vec(entry) {
                    records.push(Bytes::from(buf));
                }
            }

            let next = envelope["response_metadata"]["next_cursor"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(String::from);

            debug!(
                page = page + 1,
                items = entries.len(),
                total = records.len(),
                has_more = next.is_some(),
                "Slack audit-log page fetched"
            );

            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        if records.is_empty() {
            return Ok(None);
        }

        info!(records = records.len(), "Slack audit-log fetched");
        Ok(Some(FetchResult {
            records,
            source: "slack.audit_logs".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}

#[async_trait]
impl Source for SlackSource {
    fn name(&self) -> &'static str {
        "slack"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn fetch(&self, window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        info!(services = self.config.services.len(), "Fetching Slack data");

        let mut results: Vec<FetchResult> = Vec::new();
        for service in &self.config.services {
            let outcome = match service.name.as_str() {
                "audit_logs" => self.fetch_audit_logs(service, window).await,
                other => {
                    warn!(service = other, "Unknown Slack service, skipping");
                    continue;
                }
            };
            match outcome {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => warn!(
                    error = %e,
                    service = %service.name,
                    "Slack service fetch failed, continuing"
                ),
            }
        }
        Ok(results)
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        let token = match self.resolve_token().await {
            Ok(t) => t,
            Err(_) => return Ok(false),
        };
        // `/api/auth.test` is the standard Slack token probe; returns
        // `{ok: true, ...}` for valid tokens.
        let url = format!("{}/api/auth.test", self.api_base());
        let resp = match self
            .client
            .get(&url)
            .bearer_auth(&token)
            .header("Accept", "application/json")
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => return Ok(false),
        };
        if !resp.status().is_success() {
            return Ok(false);
        }
        let body: serde_json::Value = match resp.json().await {
            Ok(b) => b,
            Err(_) => return Ok(false),
        };
        Ok(body["ok"].as_bool() == Some(true))
    }

    fn cursor_prefix(&self) -> String {
        "slack".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        self.config
            .services
            .iter()
            .map(|s| s.name.as_str())
            .collect()
    }
}

/// Minimal RFC 3986 percent-encoder for query-string values.
fn percent_encode(input: &str) -> String {
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

    #[test]
    fn percent_encode_passes_unreserved() {
        assert_eq!(percent_encode("user_login-1.0~x"), "user_login-1.0~x");
    }

    #[test]
    fn percent_encode_encodes_colon_and_space() {
        assert_eq!(percent_encode("a:b c"), "a%3Ab%20c");
    }

    fn cfg() -> SlackSourceConfig {
        SlackSourceConfig {
            enabled: true,
            token: Some("test-token".to_string().into()),
            ..SlackSourceConfig::default()
        }
    }

    #[test]
    fn api_base_default_when_no_override() {
        let src = SlackSource::new(cfg());
        assert_eq!(src.api_base(), "https://api.slack.com");
    }

    #[test]
    fn api_base_uses_override_when_present() {
        let mut c = cfg();
        c.api_url_override = Some("http://localhost:9999".into());
        let src = SlackSource::new(c);
        assert_eq!(src.api_base(), "http://localhost:9999");
    }
}
