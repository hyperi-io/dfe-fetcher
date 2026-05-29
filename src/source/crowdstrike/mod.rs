// Project:   dfe-fetcher
// File:      src/source/crowdstrike/mod.rs
// Purpose:   CrowdStrike Falcon source
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! CrowdStrike Falcon source.
//!
//! Pulls alerts from the Falcon public API. Auth: OAuth2
//! `client_credentials` against `/oauth2/token`, then bearer-token to the
//! data endpoints.
//!
//! The `alerts` service is a two-stage API call against the Alerts API
//! (the legacy Detects API - `/detects/queries/detects/v1` +
//! `/detects/entities/summaries/GET/v1` - was decommissioned 2025-09-30):
//!
//! 1. `GET /alerts/queries/alerts/v2?filter=...&offset=...&limit=...`
//!    returns alert composite IDs only.
//! 2. `POST /alerts/entities/alerts/v2` with `{"composite_ids": [...]}`
//!    returns full alert entities.
//!
//! Region awareness: Falcon tenants live on different cloud regions, each
//! with its own API host. The fetcher takes the API base URL from config
//! (`api_url_override`); default is US-1 (`api.crowdstrike.com`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::{CrowdstrikeService, CrowdstrikeSourceConfig};
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

/// Default lookback when no `FetchWindow` is supplied.
const DEFAULT_LOOKBACK_HOURS: i64 = 1;

/// Default alerts page size. The Alerts query API caps `limit` at 1000.
const DEFAULT_LIMIT: u32 = 100;

/// Hard cap on the configurable query page size for the Alerts API.
const MAX_LIMIT: u32 = 1000;

/// Maximum query-pages we will follow per fetch call (each holds <=`limit` IDs).
const MAX_PAGES: usize = 50;

/// Maximum composite IDs per entities POST batch. Falcon docs say 1000.
const ENTITY_BATCH_LIMIT: usize = 1000;

/// Per-request HTTP timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Cached OAuth2 access token + expiry.
#[derive(Clone)]
struct CachedToken {
    token: String,
    /// Absolute Instant after which the token must be re-fetched.
    valid_until: std::time::Instant,
}

/// CrowdStrike Falcon data source.
pub struct CrowdstrikeSource {
    config: CrowdstrikeSourceConfig,
    client: reqwest::Client,
    cached_token: Arc<Mutex<Option<CachedToken>>>,
}

impl CrowdstrikeSource {
    /// Create a new CrowdStrike source from configuration.
    pub fn new(config: CrowdstrikeSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            config,
            client,
            cached_token: Arc::new(Mutex::new(None)),
        }
    }

    fn api_base(&self) -> &str {
        self.config
            .api_url_override
            .as_deref()
            .unwrap_or("https://api.crowdstrike.com")
    }

    fn client_id(&self) -> Result<&str> {
        self.config
            .client_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Credential("crowdstrike.client_id is required".into()))
    }

    /// Resolve client_secret from `credential_secret` or the literal value.
    async fn resolve_client_secret(&self) -> Result<String> {
        if let Some(ref spec) = self.config.credential_secret {
            return credential::resolve(spec).await;
        }
        self.config
            .client_secret
            .as_ref()
            .map(|s| s.expose().to_string())
            .ok_or_else(|| Error::Credential("crowdstrike.client_secret is required".into()))
    }

    /// Fetch (or reuse cached) access token via OAuth2 client_credentials.
    async fn get_access_token(&self) -> Result<String> {
        {
            let cached = self.cached_token.lock().await;
            if let Some(c) = cached.as_ref()
                && c.valid_until > std::time::Instant::now()
            {
                return Ok(c.token.clone());
            }
        }

        let client_id = self.client_id()?.to_string();
        let client_secret = self.resolve_client_secret().await?;
        let url = format!("{}/oauth2/token", self.api_base());

        let resp = self
            .client
            .post(&url)
            .form(&[
                ("client_id", client_id.as_str()),
                ("client_secret", client_secret.as_str()),
            ])
            .send()
            .await
            .map_err(|e| Error::Credential(format!("CrowdStrike token exchange failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Credential(format!(
                "CrowdStrike token exchange returned {status}: {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| {
            Error::Credential(format!("CrowdStrike token response parse failed: {e}"))
        })?;

        let token = body["access_token"]
            .as_str()
            .ok_or_else(|| Error::Credential("CrowdStrike response missing access_token".into()))?
            .to_string();
        let expires_in = body["expires_in"].as_u64().unwrap_or(1800);
        // Refresh 60s before stated expiry to avoid edge-of-window misses.
        let valid_for = expires_in.saturating_sub(60).max(60);
        let valid_until = std::time::Instant::now() + Duration::from_secs(valid_for);

        let mut cached = self.cached_token.lock().await;
        *cached = Some(CachedToken {
            token: token.clone(),
            valid_until,
        });

        Ok(token)
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

    /// Falcon Query Language requires single-quoted timestamps in RFC 3339
    /// form. The FQL syntax is `<field>:>'<value>'` for greater-than.
    fn format_fql_time(ts: DateTime<Utc>) -> String {
        ts.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }

    /// Build the Falcon Query Language filter clause for the time window
    /// plus any user-provided service-config `filter`.
    fn build_fql_filter(
        service: &CrowdstrikeService,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> String {
        let base = format!(
            "created_timestamp:>'{}'+created_timestamp:<'{}'",
            Self::format_fql_time(start),
            Self::format_fql_time(end),
        );
        match service.config.get("filter").and_then(|v| v.as_str()) {
            Some(extra) if !extra.is_empty() => format!("{base}+{extra}"),
            _ => base,
        }
    }

    async fn fetch_alerts(
        &self,
        service: &CrowdstrikeService,
        window: Option<&FetchWindow>,
    ) -> Result<Option<FetchResult>> {
        let token = self.get_access_token().await?;
        let (start, end) = self.window(window);

        let limit = service
            .config
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| (n.min(u64::from(MAX_LIMIT))) as u32)
            .unwrap_or(DEFAULT_LIMIT);

        let filter = Self::build_fql_filter(service, start, end);
        let api_base = self.api_base().to_string();

        // Stage 1: collect alert composite IDs via /alerts/queries/alerts/v2.
        let mut ids: Vec<String> = Vec::new();
        let mut offset: u32 = 0;
        for page in 0..MAX_PAGES {
            let url = format!(
                "{api_base}/alerts/queries/alerts/v2?filter={}&offset={offset}&limit={limit}&sort=created_timestamp.asc",
                urlencoded(&filter),
            );

            let resp = self
                .client
                .get(&url)
                .bearer_auth(&token)
                .header("Accept", "application/json")
                .send()
                .await
                .map_err(|e| Error::Source(format!("CrowdStrike alert query failed: {e}")))?;

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "CrowdStrike alert query returned {status}: {body}"
                )));
            }

            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("CrowdStrike alert query parse failed: {e}")))?;

            let resources = body["resources"].as_array().cloned().unwrap_or_default();
            let batch_len = resources.len();
            for r in resources {
                if let Some(s) = r.as_str() {
                    ids.push(s.to_string());
                }
            }
            let total = body["meta"]["pagination"]["total"].as_u64().unwrap_or(0) as u32;

            debug!(
                page = page + 1,
                batch_len,
                total_ids = ids.len(),
                total_advertised = total,
                "CrowdStrike alert-query page fetched"
            );

            if (ids.len() as u32) >= total || batch_len == 0 {
                break;
            }
            offset = ids.len() as u32;
        }

        if ids.is_empty() {
            return Ok(None);
        }

        // Stage 2: POST batches of composite IDs to /alerts/entities/alerts/v2.
        let entities_url = format!("{api_base}/alerts/entities/alerts/v2");
        let mut records: Vec<Bytes> = Vec::new();
        for chunk in ids.chunks(ENTITY_BATCH_LIMIT) {
            let body = serde_json::json!({ "composite_ids": chunk });
            let resp = self
                .client
                .post(&entities_url)
                .bearer_auth(&token)
                .header("Accept", "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|e| {
                    Error::Source(format!("CrowdStrike alert entities POST failed: {e}"))
                })?;

            let status = resp.status();
            if !status.is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "CrowdStrike alert entities returned {status}: {body_text}"
                )));
            }

            let envelope: serde_json::Value = resp.json().await.map_err(|e| {
                Error::Source(format!("CrowdStrike alert entities parse failed: {e}"))
            })?;

            let resources = envelope["resources"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            for item in resources {
                if let Ok(buf) = serde_json::to_vec(&item) {
                    records.push(Bytes::from(buf));
                }
            }

            debug!(
                chunk_size = chunk.len(),
                total_records = records.len(),
                "CrowdStrike alert-entities batch fetched"
            );
        }

        if records.is_empty() {
            return Ok(None);
        }

        info!(records = records.len(), "CrowdStrike alerts fetched");
        Ok(Some(FetchResult {
            records,
            source: "crowdstrike.alerts".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}

#[async_trait]
impl Source for CrowdstrikeSource {
    fn name(&self) -> &'static str {
        "crowdstrike"
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
            "Fetching CrowdStrike data"
        );

        let mut results: Vec<FetchResult> = Vec::new();
        for service in &self.config.services {
            let outcome = match service.name.as_str() {
                "alerts" => self.fetch_alerts(service, window).await,
                other => {
                    warn!(service = other, "Unknown CrowdStrike service, skipping");
                    continue;
                }
            };
            match outcome {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => warn!(
                    error = %e,
                    service = %service.name,
                    "CrowdStrike service fetch failed, continuing"
                ),
            }
        }
        Ok(results)
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        // Token-exchange success is the cheapest valid auth probe.
        Ok(self.get_access_token().await.is_ok())
    }

    fn cursor_prefix(&self) -> String {
        "crowdstrike".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        self.config
            .services
            .iter()
            .map(|s| s.name.as_str())
            .collect()
    }
}

/// Minimal RFC 3986 percent-encoder for FQL filter values. The FQL strings we
/// produce only ever contain `:`, `'`, `+`, `.`, and ASCII timestamp chars.
fn urlencoded(input: &str) -> String {
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
    use std::collections::HashMap;

    use super::*;

    fn cfg() -> CrowdstrikeSourceConfig {
        CrowdstrikeSourceConfig {
            enabled: true,
            client_id: Some("test-client".to_string()),
            client_secret: Some("test-secret".to_string().into()),
            ..CrowdstrikeSourceConfig::default()
        }
    }

    #[test]
    fn api_base_defaults_to_us1() {
        let src = CrowdstrikeSource::new(cfg());
        assert_eq!(src.api_base(), "https://api.crowdstrike.com");
    }

    #[test]
    fn api_base_uses_override_when_present() {
        let mut c = cfg();
        c.api_url_override = Some("https://api.eu-1.crowdstrike.com".into());
        let src = CrowdstrikeSource::new(c);
        assert_eq!(src.api_base(), "https://api.eu-1.crowdstrike.com");
    }

    #[test]
    fn client_id_required() {
        let mut c = cfg();
        c.client_id = None;
        let src = CrowdstrikeSource::new(c);
        assert!(src.client_id().is_err());
    }

    #[test]
    fn build_fql_filter_uses_inclusive_start_exclusive_end() {
        let start = DateTime::parse_from_rfc3339("2026-05-21T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2026-05-21T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let svc = CrowdstrikeService {
            name: "alerts".into(),
            config: HashMap::default(),
        };
        let f = CrowdstrikeSource::build_fql_filter(&svc, start, end);
        assert_eq!(
            f,
            "created_timestamp:>'2026-05-21T00:00:00Z'+created_timestamp:<'2026-05-21T01:00:00Z'"
        );
    }

    #[test]
    fn build_fql_filter_appends_user_filter() {
        let start = DateTime::parse_from_rfc3339("2026-05-21T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2026-05-21T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut svc = CrowdstrikeService {
            name: "alerts".into(),
            config: HashMap::default(),
        };
        svc.config.insert(
            "filter".into(),
            serde_json::Value::String("severity:>=70".into()),
        );
        let f = CrowdstrikeSource::build_fql_filter(&svc, start, end);
        assert!(f.ends_with("+severity:>=70"), "got: {f}");
    }

    #[test]
    fn urlencoded_encodes_single_quote_colon_plus() {
        assert_eq!(
            urlencoded("created_timestamp:>'2026-05-21T00:00:00Z'"),
            "created_timestamp%3A%3E%272026-05-21T00%3A00%3A00Z%27"
        );
    }

    #[test]
    fn format_fql_time_no_fractional_seconds() {
        let t = DateTime::parse_from_rfc3339("2026-05-21T13:30:00.987Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            CrowdstrikeSource::format_fql_time(t),
            "2026-05-21T13:30:00Z"
        );
    }
}
