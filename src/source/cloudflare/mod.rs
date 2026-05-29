// Project:   dfe-fetcher
// File:      src/source/cloudflare/mod.rs
// Purpose:   Cloudflare audit-log source
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Cloudflare audit-log source.
//!
//! Fetches account-level audit log entries from
//! `https://api.cloudflare.com/client/v4/accounts/{account_id}/audit_logs`.
//! One Cloudflare account per fetcher instance.
//!
//! Authentication: scoped API token in `Authorization: Bearer <token>`.
//! Cloudflare wraps responses in `{"result": [...], "result_info": {...}, ...}`.
//! Pagination is page-number based via `result_info.total_pages`.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use crate::config::{CloudflareService, CloudflareSourceConfig};
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

/// Default lookback when no `FetchWindow` is supplied.
const DEFAULT_LOOKBACK_HOURS: i64 = 1;

/// Default per-page size. Cloudflare caps at 1000.
const DEFAULT_PER_PAGE: u32 = 100;

/// Maximum pages we will follow per fetch call.
const MAX_PAGES: usize = 50;

/// Per-request HTTP timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Cloudflare audit-log data source.
pub struct CloudflareSource {
    config: CloudflareSourceConfig,
    client: reqwest::Client,
}

impl CloudflareSource {
    /// Create a new Cloudflare source from configuration.
    pub fn new(config: CloudflareSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    fn api_base(&self) -> &str {
        self.config
            .api_url_override
            .as_deref()
            .unwrap_or("https://api.cloudflare.com/client/v4")
    }

    fn account_id(&self) -> Result<&str> {
        self.config
            .account_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Config("cloudflare.account_id is required".into()))
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
            .ok_or_else(|| Error::Credential("cloudflare.token is required".into()))
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

    /// Cloudflare accepts RFC 3339 timestamps for the `since`/`before` params.
    fn format_cf_time(ts: DateTime<Utc>) -> String {
        ts.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }

    /// Percent-encode an arbitrary query-string value (used for actor_email
    /// since it contains `@`).
    fn encode_query_value(input: &str) -> String {
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

    async fn fetch_audit_logs(
        &self,
        service: &CloudflareService,
        window: Option<&FetchWindow>,
    ) -> Result<Option<FetchResult>> {
        let token = self.resolve_token().await?;
        let account_id = self.account_id()?;
        let (start, end) = self.window(window);

        let per_page = service
            .config
            .get("per_page")
            .and_then(|v| v.as_u64())
            .map(|n| n.min(1000) as u32)
            .unwrap_or(DEFAULT_PER_PAGE);

        // Build base URL. Cloudflare audit-logs endpoint takes `since`,
        // `before`, `per_page`, optional `actor.email`, `action.type`.
        let mut base_url = format!(
            "{}/accounts/{account_id}/audit_logs?since={}&before={}&per_page={}",
            self.api_base(),
            Self::format_cf_time(start),
            Self::format_cf_time(end),
            per_page,
        );

        if let Some(email) = service.config.get("actor_email").and_then(|v| v.as_str()) {
            base_url.push_str("&actor.email=");
            base_url.push_str(&Self::encode_query_value(email));
        }
        if let Some(action_type) = service.config.get("action_type").and_then(|v| v.as_str()) {
            base_url.push_str("&action.type=");
            base_url.push_str(&Self::encode_query_value(action_type));
        }

        let mut records: Vec<Bytes> = Vec::new();
        let mut page: u32 = 1;
        let mut total_pages: u32 = 1; // updated from result_info on first response

        loop {
            if page as usize > MAX_PAGES {
                warn!(
                    page,
                    MAX_PAGES, "Cloudflare audit-log paging cap reached, truncating result"
                );
                break;
            }
            let url = format!("{base_url}&page={page}");

            let resp = self
                .client
                .get(&url)
                .bearer_auth(&token)
                .header("Accept", "application/json")
                .send()
                .await
                .map_err(|e| Error::Source(format!("Cloudflare audit-log request failed: {e}")))?;

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "Cloudflare audit-log returned {status}: {body}"
                )));
            }

            let envelope: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("Cloudflare audit-log parse failed: {e}")))?;

            // Cloudflare always returns `{"success": ..., "result": [...], ...}`.
            // Treat `success=false` as an error even on a 2xx.
            if envelope["success"].as_bool() == Some(false) {
                let errors = envelope["errors"].to_string();
                return Err(Error::Source(format!(
                    "Cloudflare audit-log API success=false: {errors}"
                )));
            }

            let items = envelope["result"].as_array().cloned().unwrap_or_default();

            for item in &items {
                if let Ok(buf) = serde_json::to_vec(item) {
                    records.push(Bytes::from(buf));
                }
            }

            // Update total_pages from result_info.
            if let Some(tp) = envelope["result_info"]["total_pages"].as_u64() {
                total_pages = tp as u32;
            }

            debug!(
                page,
                total_pages,
                items = items.len(),
                total_records = records.len(),
                "Cloudflare audit-log page fetched"
            );

            if page >= total_pages {
                break;
            }
            page += 1;
        }

        if records.is_empty() {
            return Ok(None);
        }

        info!(records = records.len(), "Cloudflare audit-log fetched");
        Ok(Some(FetchResult {
            records,
            source: "cloudflare.audit_logs".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}

#[async_trait]
impl Source for CloudflareSource {
    fn name(&self) -> &'static str {
        "cloudflare"
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
            "Fetching Cloudflare data"
        );

        let mut results: Vec<FetchResult> = Vec::new();
        for service in &self.config.services {
            let outcome = match service.name.as_str() {
                "audit_logs" => self.fetch_audit_logs(service, window).await,
                other => {
                    warn!(service = other, "Unknown Cloudflare service, skipping");
                    continue;
                }
            };
            match outcome {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => warn!(
                    error = %e,
                    service = %service.name,
                    "Cloudflare service fetch failed, continuing"
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
        // `/user/tokens/verify` is a free-tier-friendly token introspection
        // endpoint that returns success when the token is valid.
        let url = format!("{}/user/tokens/verify", self.api_base());
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
        Ok(resp.status().is_success())
    }

    fn cursor_prefix(&self) -> String {
        "cloudflare".to_string()
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

    fn cfg() -> CloudflareSourceConfig {
        CloudflareSourceConfig {
            enabled: true,
            account_id: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
            token: Some("test-token".to_string().into()),
            ..CloudflareSourceConfig::default()
        }
    }

    #[test]
    fn account_id_required() {
        let mut c = cfg();
        c.account_id = None;
        let src = CloudflareSource::new(c);
        assert!(src.account_id().is_err());
    }

    #[test]
    fn account_id_rejects_empty() {
        let mut c = cfg();
        c.account_id = Some(String::new());
        let src = CloudflareSource::new(c);
        assert!(src.account_id().is_err());
    }

    #[test]
    fn api_base_uses_override_when_present() {
        let mut c = cfg();
        c.api_url_override = Some("http://localhost:9999/v4".into());
        let src = CloudflareSource::new(c);
        assert_eq!(src.api_base(), "http://localhost:9999/v4");
    }

    #[test]
    fn api_base_default_when_no_override() {
        let src = CloudflareSource::new(cfg());
        assert_eq!(src.api_base(), "https://api.cloudflare.com/client/v4");
    }

    #[test]
    fn format_cf_time_rfc3339_seconds_z_suffix() {
        let t = DateTime::parse_from_rfc3339("2026-05-21T13:30:00.987Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(CloudflareSource::format_cf_time(t), "2026-05-21T13:30:00Z");
    }

    #[test]
    fn encode_query_value_passes_unreserved() {
        assert_eq!(
            CloudflareSource::encode_query_value("simple.value-1_2~3"),
            "simple.value-1_2~3"
        );
    }

    #[test]
    fn encode_query_value_encodes_at_and_plus() {
        assert_eq!(
            CloudflareSource::encode_query_value("user+tag@example.com"),
            "user%2Btag%40example.com"
        );
    }
}
