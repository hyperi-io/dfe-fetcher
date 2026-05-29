// Project:   dfe-fetcher
// File:      src/source/m365/mod.rs
// Purpose:   Microsoft 365 data source (Audit Log, DLP, Exchange Audit, Alerts)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Microsoft 365 data source.
//!
//! Fetches security and compliance data from M365 services via two parallel surfaces:
//!
//! - **Office 365 Management Activity API** (`manage.office.com`) -- raw per-event
//!   audit records. Per-content-type subscriptions, per-feed app permissions
//!   (`ActivityFeed.Read`, `ActivityFeed.ReadDlp`). The primary surface for
//!   `audit_log`, `dlp`, and `exchange_audit`.
//! - **Microsoft Graph Security** (`graph.microsoft.com/security/*`) -- curated,
//!   post-classification alerts. Single broad app permission (`SecurityAlert.Read.All`).
//!   Used only for `alerts`.
//!
//! Authentication: OAuth2 client_credentials against `login.microsoftonline.com`.

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tracing::{debug, info, warn};

use crate::config::{M365Service, M365SourceConfig};
use crate::credential::{self, TokenManager};
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source, SourceMaturity};

/// Office 365 Management Activity API content types.
///
/// Default set fetched by the `audit_log` service when no `content_types`
/// override is given in service config.
const DEFAULT_OMAP_CONTENT_TYPES: &[&str] = &[
    "Audit.General",
    "Audit.AzureActiveDirectory",
    "Audit.Exchange",
    "Audit.SharePoint",
    "DLP.All",
];

/// OMAP per-query window cap (the API rejects ranges larger than this).
const OMAP_MAX_WINDOW_HOURS: i64 = 24;

/// Default lookback when no `FetchWindow` is supplied -- matches typical
/// scheduler tick interval, kept short to avoid duplicate fetches.
const OMAP_DEFAULT_LOOKBACK_HOURS: i64 = 1;

/// Maximum retry attempts per content-blob fetch before giving up on that blob.
const OMAP_MAX_BLOB_RETRIES: usize = 3;

/// Backoff applied when the OMAP API signals throttling (HTTP 429 or
/// "too many requests" body).
const OMAP_RATE_LIMIT_BACKOFF: Duration = Duration::from_secs(30);

/// Fallback `PublisherIdentifier` used when `M365_PUBLISHER_IDENTIFIER` env
/// is unset. Operators should override per deployment so that per-publisher
/// rate-limit budgets aren't shared across HyperI customer tenants.
const OMAP_PUBLISHER_DEFAULT: &str = "12345678-1234-1234-1234-123456789123";

/// Microsoft 365 data source implementation.
pub struct M365Source {
    config: M365SourceConfig,
    client: reqwest::Client,
}

impl M365Source {
    /// Create a new M365 source from configuration.
    pub fn new(config: M365SourceConfig) -> Self {
        let client = credential::http_client().unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    fn mgmt_base(&self) -> &str {
        self.config
            .management_url_override
            .as_deref()
            .unwrap_or("https://manage.office.com")
    }

    fn graph_base(&self) -> &str {
        self.config
            .graph_url_override
            .as_deref()
            .unwrap_or("https://graph.microsoft.com")
    }

    fn tenant_id(&self) -> Result<&str> {
        self.config
            .tenant_id
            .as_deref()
            .ok_or_else(|| Error::Credential("m365.tenant_id is required".into()))
    }

    fn publisher_id(&self) -> String {
        std::env::var("M365_PUBLISHER_IDENTIFIER")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| OMAP_PUBLISHER_DEFAULT.to_string())
    }

    /// Build a token manager for the Office 365 Management Activity API.
    async fn management_token_manager(&self) -> Result<TokenManager> {
        let tenant_id = self.tenant_id()?;
        let client_id = self.resolve_client_id().await?;
        let client_secret = self.resolve_client_secret().await?;

        let token_url = self.config.token_url_override.clone().unwrap_or_else(|| {
            format!("https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token")
        });
        Ok(TokenManager::new(
            self.client.clone(),
            token_url,
            client_id,
            client_secret,
            "https://manage.office.com/.default".to_string(),
        ))
    }

    /// Build a token manager for the Microsoft Graph API.
    async fn graph_token_manager(&self) -> Result<TokenManager> {
        let tenant_id = self.tenant_id()?;
        let client_id = self.resolve_client_id().await?;
        let client_secret = self.resolve_client_secret().await?;

        let token_url = self.config.token_url_override.clone().unwrap_or_else(|| {
            format!("https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token")
        });
        Ok(TokenManager::new(
            self.client.clone(),
            token_url,
            client_id,
            client_secret,
            "https://graph.microsoft.com/.default".to_string(),
        ))
    }

    async fn resolve_client_id(&self) -> Result<String> {
        if let Some(ref secret_spec) = self.config.credential_secret {
            return credential::resolve(secret_spec).await;
        }
        self.config
            .client_id
            .clone()
            .ok_or_else(|| Error::Credential("m365.client_id is required".into()))
    }

    async fn resolve_client_secret(&self) -> Result<String> {
        if let Some(ref secret_spec) = self.config.credential_secret {
            return credential::resolve(secret_spec).await;
        }
        self.config
            .client_secret
            .as_ref()
            .map(|s| s.expose().to_string())
            .ok_or_else(|| Error::Credential("m365.client_secret is required".into()))
    }

    /// Split a `FetchWindow` into chunks no larger than the OMAP per-query cap.
    ///
    /// When `window` is `None`, falls back to the last [`OMAP_DEFAULT_LOOKBACK_HOURS`].
    fn split_window(window: Option<&FetchWindow>) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
        let (start, end) = match window {
            Some(w) => (w.start, w.end),
            None => {
                let now = Utc::now();
                (
                    now - ChronoDuration::hours(OMAP_DEFAULT_LOOKBACK_HOURS),
                    now,
                )
            }
        };
        if start >= end {
            return Vec::new();
        }
        let max_chunk = ChronoDuration::hours(OMAP_MAX_WINDOW_HOURS);
        let mut chunks = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let next = std::cmp::min(cursor + max_chunk, end);
            chunks.push((cursor, next));
            cursor = next;
        }
        chunks
    }

    /// List currently-enabled OMAP subscriptions for the tenant.
    async fn list_subscriptions(&self, token: &str) -> Result<Vec<String>> {
        let tenant_id = self.tenant_id()?;
        let url = format!(
            "{}/api/v1.0/{tenant_id}/activity/feed/subscriptions/list",
            self.mgmt_base()
        );
        let resp = self
            .client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| Error::Source(format!("M365 subscriptions/list failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "M365 subscriptions/list returned {status}: {body}"
            )));
        }
        let items: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
        Ok(items
            .iter()
            .filter(|item| {
                item["status"]
                    .as_str()
                    .is_some_and(|s| s.eq_ignore_ascii_case("enabled"))
            })
            .filter_map(|item| item["contentType"].as_str().map(String::from))
            .collect())
    }

    /// Enable OMAP subscriptions for the given content types.
    ///
    /// Lists current subscriptions once, then issues `/start` only for the
    /// content types that are not already enabled. Tolerates list failures by
    /// falling through to per-type `/start` attempts.
    async fn ensure_subscriptions(&self, token: &str, content_types: &[&str]) -> Result<()> {
        let tenant_id = self.tenant_id()?;
        let existing: HashSet<String> = match self.list_subscriptions(token).await {
            Ok(types) => types.into_iter().map(|s| s.to_lowercase()).collect(),
            Err(e) => {
                warn!(error = %e, "M365 subscriptions/list failed; will attempt to start each type blind");
                HashSet::new()
            }
        };
        for ct in content_types {
            if existing.contains(&ct.to_lowercase()) {
                debug!(content_type = ct, "M365 subscription already enabled");
                continue;
            }
            self.start_subscription(token, tenant_id, ct).await?;
        }
        Ok(())
    }

    /// Start an OMAP subscription for the given content type.
    ///
    /// Idempotent on the server side -- an already-started feed returns 200.
    async fn start_subscription(
        &self,
        token: &str,
        tenant_id: &str,
        content_type: &str,
    ) -> Result<()> {
        let url = format!(
            "{}/api/v1.0/{tenant_id}/activity/feed/subscriptions/start?contentType={content_type}",
            self.mgmt_base()
        );
        match self.client.post(&url).bearer_auth(token).send().await {
            Ok(r) if r.status().is_success() => {
                info!(content_type, "M365 subscription started");
            }
            Ok(r) => {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                warn!(content_type, status = %status, body = %body,
                    "M365 subscription/start non-success");
            }
            Err(e) => {
                warn!(error = %e, content_type, "M365 subscription/start request failed");
            }
        }
        Ok(())
    }

    /// Detect rate-limit response from OMAP/Graph.
    fn is_rate_limited(status: reqwest::StatusCode, body: &str) -> bool {
        status.as_u16() == 429 || body.to_ascii_lowercase().contains("too many request")
    }

    /// Fetch a single content blob with bounded retries.
    ///
    /// Returns the JSON event records found in the blob (one `Bytes` per event).
    /// Empty vec on terminal failure -- callers continue to the next blob rather
    /// than failing the whole content-type fetch.
    async fn fetch_blob_with_retry(client: &reqwest::Client, token: &str, url: &str) -> Vec<Bytes> {
        let mut attempts: usize = 0;
        while attempts < OMAP_MAX_BLOB_RETRIES {
            attempts += 1;
            let resp = match client.get(url).bearer_auth(token).send().await {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, url, attempt = attempts, "M365 blob fetch network error");
                    continue;
                }
            };
            let status = resp.status();
            if status.is_success() {
                let events: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
                return events
                    .into_iter()
                    .filter_map(|e| serde_json::to_vec(&e).ok().map(Bytes::from))
                    .collect();
            }
            let body = resp.text().await.unwrap_or_default();
            if Self::is_rate_limited(status, &body) {
                warn!(
                    url,
                    attempt = attempts,
                    "M365 blob fetch rate-limited; backing off"
                );
                tokio::time::sleep(OMAP_RATE_LIMIT_BACKOFF).await;
                continue;
            }
            warn!(status = %status, url, attempt = attempts, body = %body,
                "M365 blob fetch non-success");
        }
        warn!(url, attempts, "M365 blob fetch gave up after retries");
        Vec::new()
    }

    /// Office 365 Management Activity API timestamp format.
    ///
    /// API expects `YYYY-MM-DDTHH:MM:SS` (no fractional seconds, no zone suffix --
    /// the API assumes UTC).
    fn format_omap_time(ts: DateTime<Utc>) -> String {
        ts.format("%Y-%m-%dT%H:%M:%S").to_string()
    }

    /// Generic OMAP content fetcher.
    ///
    /// Lists content blobs for `content_type` over `window` (chunked into
    /// <=24h ranges), follows `NextPageUri` pagination, then fetches each blob
    /// with bounded retry. Subscription is assumed to already be enabled --
    /// callers should run [`ensure_subscriptions`] first.
    ///
    /// Returns `Ok(None)` when no records are found, `Ok(Some(_))` with the
    /// records tagged by `source_suffix`.
    ///
    /// [`ensure_subscriptions`]: Self::ensure_subscriptions
    async fn fetch_omap_content(
        &self,
        token: &str,
        content_type: &str,
        source_suffix: &str,
        window: Option<&FetchWindow>,
    ) -> Result<Option<FetchResult>> {
        let tenant_id = self.tenant_id()?;
        let publisher = self.publisher_id();
        let chunks = Self::split_window(window);
        if chunks.is_empty() {
            return Ok(None);
        }

        let mut blob_urls: Vec<String> = Vec::new();
        for (start, end) in chunks {
            let mut next_url = Some(format!(
                "{}/api/v1.0/{tenant_id}/activity/feed/subscriptions/content\
                 ?contentType={content_type}\
                 &startTime={}\
                 &endTime={}\
                 &PublisherIdentifier={publisher}",
                self.mgmt_base(),
                Self::format_omap_time(start),
                Self::format_omap_time(end),
            ));

            while let Some(url) = next_url.take() {
                let resp = self
                    .client
                    .get(&url)
                    .bearer_auth(token)
                    .send()
                    .await
                    .map_err(|e| Error::Source(format!("M365 content list failed: {e}")))?;
                let status = resp.status();

                if status.as_u16() == 404 {
                    // Subscription was disabled between ensure_subscriptions and this call.
                    // Re-arm and continue to the next chunk; don't fail the whole fetch.
                    debug!(
                        content_type,
                        "M365 content list returned 404; re-starting subscription"
                    );
                    self.start_subscription(token, tenant_id, content_type)
                        .await?;
                    break;
                }

                // NextPageUri is returned via response header before the body is consumed.
                let header_next = resp
                    .headers()
                    .get("NextPageUri")
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);

                if !status.is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    if Self::is_rate_limited(status, &body) {
                        warn!(content_type, "M365 content list rate-limited; backing off");
                        tokio::time::sleep(OMAP_RATE_LIMIT_BACKOFF).await;
                        next_url = Some(url);
                        continue;
                    }
                    return Err(Error::Source(format!(
                        "M365 content list returned {status}: {body}"
                    )));
                }

                let items: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
                for item in items {
                    if let Some(uri) = item["contentUri"].as_str() {
                        blob_urls.push(uri.to_string());
                    }
                }
                next_url = header_next;
            }
        }

        if blob_urls.is_empty() {
            return Ok(None);
        }

        let token_owned = token.to_string();
        let blob_futures = blob_urls.into_iter().map(|url| {
            let client = self.client.clone();
            let token = token_owned.clone();
            async move { Self::fetch_blob_with_retry(&client, &token, &url).await }
        });
        let records: Vec<Bytes> = futures::future::join_all(blob_futures)
            .await
            .into_iter()
            .flatten()
            .collect();

        if records.is_empty() {
            return Ok(None);
        }

        info!(
            records = records.len(),
            content_type, source_suffix, "M365 OMAP content fetched"
        );
        Ok(Some(FetchResult {
            records,
            source: format!("m365.{source_suffix}"),
            topic: self.config.topic.clone(),
        }))
    }

    /// Fetch every configured OMAP content type for the `audit_log` service.
    ///
    /// Service config may override the default set of content types with:
    ///
    /// ```yaml
    /// - name: audit_log
    ///   config:
    ///     content_types: ["Audit.General", "Audit.SharePoint"]
    /// ```
    async fn fetch_audit_log_all(
        &self,
        service: &M365Service,
        window: Option<&FetchWindow>,
    ) -> Result<Vec<FetchResult>> {
        let configured: Option<Vec<String>> = service
            .config
            .get("content_types")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            });
        let content_types: Vec<&str> = match configured.as_ref() {
            Some(v) => v.iter().map(String::as_str).collect(),
            None => DEFAULT_OMAP_CONTENT_TYPES.to_vec(),
        };

        let tm = self.management_token_manager().await?;
        let token = tm.get_token().await?;
        self.ensure_subscriptions(&token, &content_types).await?;

        let mut results = Vec::with_capacity(content_types.len());
        for ct in &content_types {
            let suffix = format!("audit_log.{}", ct.to_lowercase().replace('.', "_"));
            match self.fetch_omap_content(&token, ct, &suffix, window).await {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => {
                    warn!(error = %e, content_type = ct, "M365 audit_log content fetch failed");
                }
            }
        }
        Ok(results)
    }

    /// DLP via Office 365 Management Activity API (raw DLP rule-match events).
    async fn fetch_dlp(
        &self,
        _service: &M365Service,
        window: Option<&FetchWindow>,
    ) -> Result<Option<FetchResult>> {
        let tm = self.management_token_manager().await?;
        let token = tm.get_token().await?;
        self.ensure_subscriptions(&token, &["DLP.All"]).await?;
        self.fetch_omap_content(&token, "DLP.All", "dlp", window)
            .await
    }

    /// Exchange audit events via OMAP `Audit.Exchange`. Per-message audit
    /// records (sender / recipients / action) - this is the real mail-flow
    /// audit feed, not the Graph `reports/getEmailActivityCounts` aggregate
    /// counts the earlier implementation accidentally pointed at.
    async fn fetch_exchange_audit(
        &self,
        _service: &M365Service,
        window: Option<&FetchWindow>,
    ) -> Result<Option<FetchResult>> {
        let tm = self.management_token_manager().await?;
        let token = tm.get_token().await?;
        self.ensure_subscriptions(&token, &["Audit.Exchange"])
            .await?;
        self.fetch_omap_content(&token, "Audit.Exchange", "exchange_audit", window)
            .await
    }

    /// Security alerts via Microsoft Graph `alerts_v2`.
    ///
    /// Requires `SecurityAlert.Read.All` Graph application permission.
    async fn fetch_alerts(&self, _service: &M365Service) -> Result<Option<FetchResult>> {
        let tm = self.graph_token_manager().await?;
        let token = tm.get_token().await?;

        let url = format!(
            "{}/v1.0/security/alerts_v2?$top=100&$orderby=createdDateTime desc",
            self.graph_base()
        );
        let items = self.fetch_graph_paginated(&token, &url, 10).await?;
        if items.is_empty() {
            return Ok(None);
        }

        let records: Vec<Bytes> = items
            .into_iter()
            .filter_map(|item| serde_json::to_vec(&item).ok().map(Bytes::from))
            .collect();

        info!(
            records = records.len(),
            "M365 Graph security alerts fetched"
        );
        Ok(Some(FetchResult {
            records,
            source: "m365.alerts".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    /// Follow `@odata.nextLink` pagination on a Graph collection endpoint.
    async fn fetch_graph_paginated(
        &self,
        token: &str,
        initial_url: &str,
        max_pages: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let mut all_items = Vec::new();
        let mut url = initial_url.to_string();

        for page in 0..max_pages {
            let resp = self
                .client
                .get(&url)
                .bearer_auth(token)
                .send()
                .await
                .map_err(|e| Error::Source(format!("M365 Graph request failed: {e}")))?;
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "M365 Graph returned {status}: {body}"
                )));
            }
            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("M365 Graph parse failed: {e}")))?;
            if let Some(items) = body["value"].as_array().cloned() {
                all_items.extend(items);
            }
            let next_link = body["@odata.nextLink"].as_str();
            match next_link {
                Some(next) => {
                    url = next.to_string();
                    debug!(
                        page = page + 1,
                        items = all_items.len(),
                        "M365 Graph next page"
                    );
                }
                None => break,
            }
        }
        Ok(all_items)
    }
}

#[async_trait]
impl Source for M365Source {
    fn name(&self) -> &'static str {
        "m365"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    fn maturity(&self) -> SourceMaturity {
        SourceMaturity::Stable
    }

    async fn fetch(&self, window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        info!(services = self.config.services.len(), "Fetching M365 data");

        let mut results: Vec<FetchResult> = Vec::new();
        for service in &self.config.services {
            let svc_outcome: Result<Vec<FetchResult>> = match service.name.as_str() {
                "audit_log" => self.fetch_audit_log_all(service, window).await,
                "dlp" => self
                    .fetch_dlp(service, window)
                    .await
                    .map(|opt| opt.into_iter().collect()),
                "exchange_audit" => self
                    .fetch_exchange_audit(service, window)
                    .await
                    .map(|opt| opt.into_iter().collect()),
                "alerts" => self
                    .fetch_alerts(service)
                    .await
                    .map(|opt| opt.into_iter().collect()),
                other => {
                    warn!(service = other, "Unknown M365 service, skipping");
                    continue;
                }
            };
            match svc_outcome {
                Ok(rs) => results.extend(rs),
                Err(e) => {
                    warn!(error = %e, service = %service.name,
                        "M365 service fetch failed, continuing");
                }
            }
        }
        Ok(results)
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        // Management token is the broader credential -- if it works, OMAP-based
        // services will work; Graph alerts require an extra permission grant.
        match self.management_token_manager().await {
            Ok(tm) => Ok(tm.get_token().await.is_ok()),
            Err(_) => Ok(false),
        }
    }

    fn cursor_prefix(&self) -> String {
        "m365".to_string()
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

    #[test]
    fn split_window_none_yields_default_lookback() {
        let chunks = M365Source::split_window(None);
        assert_eq!(chunks.len(), 1, "default window should produce one chunk");
        let (start, end) = chunks[0];
        let duration = end - start;
        assert!(duration <= ChronoDuration::hours(OMAP_DEFAULT_LOOKBACK_HOURS));
    }

    #[test]
    fn split_window_short_window_one_chunk() {
        let end = Utc::now();
        let start = end - ChronoDuration::hours(2);
        let chunks = M365Source::split_window(Some(&FetchWindow { start, end }));
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], (start, end));
    }

    #[test]
    fn split_window_chunks_at_24h() {
        let end = Utc::now();
        let start = end - ChronoDuration::hours(50);
        let chunks = M365Source::split_window(Some(&FetchWindow { start, end }));
        assert_eq!(chunks.len(), 3, "50h window splits into 24h+24h+2h");
        for (s, e) in &chunks {
            assert!(*e - *s <= ChronoDuration::hours(24));
        }
        assert_eq!(chunks[0].0, start);
        assert_eq!(chunks.last().unwrap().1, end);
    }

    #[test]
    fn split_window_empty_for_inverted_range() {
        let now = Utc::now();
        let chunks = M365Source::split_window(Some(&FetchWindow {
            start: now,
            end: now - ChronoDuration::hours(1),
        }));
        assert!(chunks.is_empty());
    }

    #[test]
    fn rate_limit_detected_from_429_status() {
        assert!(M365Source::is_rate_limited(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            ""
        ));
    }

    #[test]
    fn rate_limit_detected_from_body_hint() {
        assert!(M365Source::is_rate_limited(
            reqwest::StatusCode::BAD_REQUEST,
            "You have made TOO MANY REQUESTS"
        ));
    }

    #[test]
    fn rate_limit_not_detected_on_success() {
        assert!(!M365Source::is_rate_limited(reqwest::StatusCode::OK, ""));
    }

    #[test]
    fn format_omap_time_no_fractional_or_zone() {
        let t = DateTime::parse_from_rfc3339("2026-05-21T13:30:00.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(M365Source::format_omap_time(t), "2026-05-21T13:30:00");
    }

    #[test]
    fn default_content_types_covers_all_five_omap_feeds() {
        assert!(DEFAULT_OMAP_CONTENT_TYPES.contains(&"Audit.General"));
        assert!(DEFAULT_OMAP_CONTENT_TYPES.contains(&"Audit.AzureActiveDirectory"));
        assert!(DEFAULT_OMAP_CONTENT_TYPES.contains(&"Audit.Exchange"));
        assert!(DEFAULT_OMAP_CONTENT_TYPES.contains(&"Audit.SharePoint"));
        assert!(DEFAULT_OMAP_CONTENT_TYPES.contains(&"DLP.All"));
    }
}
