// Project:   dfe-fetcher
// File:      src/source/okta/mod.rs
// Purpose:   Okta System Log source
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Okta System Log source.
//!
//! Fetches events from the Okta System Log API
//! (`{tenant_url}/api/v1/logs`). Pull-mode, per-tenant.
//!
//! Authentication: legacy SSWS API token (`Authorization: SSWS <token>`) or
//! OAuth bearer token. Selectable via `use_ssws_header` in config.
//!
//! Pagination: shared RFC 5988 `Link` header parser in [`crate::source::parse_link_next_url`].
//! Time-window via `since=<ISO8601>&until=<ISO8601>` query params.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use crate::config::{OktaService, OktaSourceConfig};
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source, parse_link_next_url};

/// Default lookback when no `FetchWindow` is supplied.
const DEFAULT_LOOKBACK_HOURS: i64 = 1;

/// Default per-page size. Okta caps at 1000; we default to 100 to match the
/// rest of the fetcher's per-source defaults.
const DEFAULT_PAGE_SIZE: u32 = 100;

/// Maximum pages we will follow per fetch call.
const MAX_PAGES: usize = 50;

/// Per-request HTTP timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Okta System Log data source.
pub struct OktaSource {
    config: OktaSourceConfig,
    client: reqwest::Client,
}

impl OktaSource {
    /// Create a new Okta source from configuration.
    pub fn new(config: OktaSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    /// Effective API base URL. Test override takes precedence; otherwise the
    /// configured tenant URL is required.
    fn api_base(&self) -> Result<&str> {
        if let Some(ref override_url) = self.config.api_url_override {
            return Ok(override_url.as_str());
        }
        self.config
            .tenant_url
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Config("okta.tenant_url is required".into()))
    }

    /// Resolve the bearer token from `credential_secret` or the literal `token`.
    async fn resolve_token(&self) -> Result<String> {
        if let Some(ref spec) = self.config.credential_secret {
            return Ok(credential::resolve(spec).await?);
        }
        self.config
            .token
            .as_ref()
            .map(|s| s.expose().to_string())
            .ok_or_else(|| Error::Credential("okta.token is required".into()))
    }

    /// Format an `Authorization` header value for an Okta request.
    fn auth_header_value(&self, token: &str) -> String {
        if self.config.use_ssws_header {
            format!("SSWS {token}")
        } else {
            format!("Bearer {token}")
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

    /// Okta accepts ISO-8601 with millisecond precision and a `Z` zone suffix.
    fn format_okta_time(ts: DateTime<Utc>) -> String {
        ts.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    }

    /// Fetch the system_log service.
    async fn fetch_system_log(
        &self,
        service: &OktaService,
        window: Option<&FetchWindow>,
    ) -> Result<Option<FetchResult>> {
        let token = self.resolve_token().await?;
        let auth_header = self.auth_header_value(&token);
        let base = self.api_base()?.trim_end_matches('/').to_string();
        let (start, end) = self.window(window);

        let limit = service
            .config
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| n.min(1000) as u32)
            .unwrap_or(DEFAULT_PAGE_SIZE);

        let mut url = format!(
            "{base}/api/v1/logs?since={}&until={}&limit={}&sortOrder=ASCENDING",
            Self::format_okta_time(start),
            Self::format_okta_time(end),
            limit,
        );

        // Optional server-side filter (Okta OData syntax).
        if let Some(filter) = service.config.get("filter").and_then(|v| v.as_str()) {
            url.push_str("&filter=");
            for byte in filter.bytes() {
                if matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~')
                {
                    url.push(byte as char);
                } else {
                    url.push_str(&format!("%{byte:02X}"));
                }
            }
        }

        let mut next_url: Option<String> = Some(url);
        let mut records: Vec<Bytes> = Vec::new();

        for page in 0..MAX_PAGES {
            let Some(u) = next_url.take() else { break };
            let resp = self
                .client
                .get(&u)
                .header("Authorization", &auth_header)
                .header("Accept", "application/json")
                .send()
                .await
                .map_err(|e| Error::Source(format!("Okta system_log request failed: {e}")))?;

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "Okta system_log returned {status}: {body}"
                )));
            }

            let header_next = parse_link_next_url(resp.headers());

            let items: Vec<serde_json::Value> = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("Okta system_log parse failed: {e}")))?;

            for item in &items {
                if let Ok(buf) = serde_json::to_vec(item) {
                    records.push(Bytes::from(buf));
                }
            }

            debug!(
                page = page + 1,
                items = items.len(),
                total = records.len(),
                "Okta system_log page fetched"
            );

            next_url = header_next;
        }

        if records.is_empty() {
            return Ok(None);
        }

        info!(records = records.len(), "Okta system_log fetched");
        Ok(Some(FetchResult {
            records,
            source: "okta.system_log".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}

#[async_trait]
impl Source for OktaSource {
    fn name(&self) -> &'static str {
        "okta"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn fetch(&self, window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        info!(services = self.config.services.len(), "Fetching Okta data");

        let mut results: Vec<FetchResult> = Vec::new();
        for service in &self.config.services {
            let outcome = match service.name.as_str() {
                "system_log" => self.fetch_system_log(service, window).await,
                other => {
                    warn!(service = other, "Unknown Okta service, skipping");
                    continue;
                }
            };
            match outcome {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => warn!(
                    error = %e,
                    service = %service.name,
                    "Okta service fetch failed, continuing"
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
        let base = match self.api_base() {
            Ok(b) => b.trim_end_matches('/').to_string(),
            Err(_) => return Ok(false),
        };
        // `/api/v1/users/me` is the cheapest authenticated probe.
        let url = format!("{base}/api/v1/users/me");
        let resp = match self
            .client
            .get(&url)
            .header("Authorization", self.auth_header_value(&token))
            .header("Accept", "application/json")
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => return Ok(false),
        };
        Ok(resp.status().is_success())
    }

    fn cursor_prefix(&self) -> String {
        "okta".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        self.config
            .services
            .iter()
            .map(|s| s.name.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> OktaSourceConfig {
        OktaSourceConfig {
            enabled: true,
            tenant_url: Some("https://hyperi.okta.com".to_string()),
            token: Some("test-token".to_string().into()),
            use_ssws_header: true,
            ..OktaSourceConfig::default()
        }
    }

    #[test]
    fn api_base_uses_override_when_present() {
        let mut c = cfg();
        c.api_url_override = Some("http://localhost:9999".into());
        let src = OktaSource::new(c);
        assert_eq!(src.api_base().unwrap(), "http://localhost:9999");
    }

    #[test]
    fn api_base_uses_tenant_url_when_no_override() {
        let src = OktaSource::new(cfg());
        assert_eq!(src.api_base().unwrap(), "https://hyperi.okta.com");
    }

    #[test]
    fn api_base_errors_when_no_url() {
        let mut c = cfg();
        c.tenant_url = None;
        c.api_url_override = None;
        let src = OktaSource::new(c);
        assert!(src.api_base().is_err());
    }

    #[test]
    fn api_base_errors_when_tenant_url_is_empty() {
        let mut c = cfg();
        c.tenant_url = Some(String::new());
        let src = OktaSource::new(c);
        assert!(src.api_base().is_err());
    }

    #[test]
    fn auth_header_ssws_by_default() {
        let src = OktaSource::new(cfg());
        assert_eq!(src.auth_header_value("abc123"), "SSWS abc123");
    }

    #[test]
    fn auth_header_bearer_when_ssws_disabled() {
        let mut c = cfg();
        c.use_ssws_header = false;
        let src = OktaSource::new(c);
        assert_eq!(src.auth_header_value("abc123"), "Bearer abc123");
    }

    #[test]
    fn format_okta_time_has_millis_and_z_suffix() {
        let t = DateTime::parse_from_rfc3339("2026-05-21T13:30:00.987654321Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(OktaSource::format_okta_time(t), "2026-05-21T13:30:00.987Z");
    }

    #[test]
    fn format_okta_time_zero_millis_is_explicit() {
        let t = DateTime::parse_from_rfc3339("2026-05-21T13:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(OktaSource::format_okta_time(t), "2026-05-21T13:30:00.000Z");
    }
}
