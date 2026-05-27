// Project:   dfe-fetcher
// File:      src/source/bitwarden/mod.rs
// Purpose:   Bitwarden Events API source
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Bitwarden Events API source.
//!
//! Pulls events from a Bitwarden organisation via `/public/events`.
//! Auth: OAuth2 `client_credentials` against `/identity/connect/token`
//! using organisation API credentials
//! (Settings > Organization info > View API Key).
//!
//! Supports both Bitwarden Cloud and self-hosted instances via
//! `api_url_override` / `identity_url_override`.
//!
//! Pagination: `continuationToken` on response, sent as `continuationToken`
//! query param on the next call.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::{BitwardenService, BitwardenSourceConfig};
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

/// Default lookback when no `FetchWindow` is supplied.
const DEFAULT_LOOKBACK_HOURS: i64 = 1;

/// Maximum pages we will follow per fetch call.
const MAX_PAGES: usize = 50;

/// Per-request HTTP timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct CachedToken {
    token: String,
    valid_until: std::time::Instant,
}

/// Bitwarden Events API data source.
pub struct BitwardenSource {
    config: BitwardenSourceConfig,
    client: reqwest::Client,
    cached_token: Arc<Mutex<Option<CachedToken>>>,
}

impl BitwardenSource {
    /// Create a new Bitwarden source from configuration.
    pub fn new(config: BitwardenSourceConfig) -> Self {
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
            .unwrap_or("https://api.bitwarden.com")
    }

    fn identity_url(&self) -> &str {
        self.config
            .identity_url_override
            .as_deref()
            .unwrap_or("https://identity.bitwarden.com/connect/token")
    }

    fn client_id(&self) -> Result<&str> {
        self.config
            .client_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Credential("bitwarden.client_id is required".into()))
    }

    async fn resolve_client_secret(&self) -> Result<String> {
        if let Some(ref spec) = self.config.credential_secret {
            return credential::resolve(spec).await;
        }
        self.config
            .client_secret
            .as_ref()
            .map(|s| s.expose().to_string())
            .ok_or_else(|| Error::Credential("bitwarden.client_secret is required".into()))
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

        let resp = self
            .client
            .post(self.identity_url())
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", client_id.as_str()),
                ("client_secret", client_secret.as_str()),
                ("scope", "api.organization"),
            ])
            .send()
            .await
            .map_err(|e| Error::Credential(format!("Bitwarden token exchange failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Credential(format!(
                "Bitwarden token exchange returned {status}: {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| {
            Error::Credential(format!("Bitwarden token response parse failed: {e}"))
        })?;

        let token = body["access_token"]
            .as_str()
            .ok_or_else(|| Error::Credential("Bitwarden response missing access_token".into()))?
            .to_string();
        let expires_in = body["expires_in"].as_u64().unwrap_or(3600);
        let valid_for = expires_in.saturating_sub(60).max(60);
        let valid_until = std::time::Instant::now() + Duration::from_secs(valid_for);

        let mut cached = self.cached_token.lock().await;
        *cached = Some(CachedToken {
            token: token.clone(),
            valid_until,
        });

        Ok(token)
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

    /// Bitwarden Events API accepts RFC 3339 timestamps.
    fn format_bw_time(ts: DateTime<Utc>) -> String {
        ts.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }

    async fn fetch_events(
        &self,
        _service: &BitwardenService,
        window: Option<&FetchWindow>,
    ) -> Result<Option<FetchResult>> {
        let token = self.get_access_token().await?;
        let (start, end) = self.window(window);

        let base = format!(
            "{}/public/events?start={}&end={}",
            self.api_base(),
            percent_encode(&Self::format_bw_time(start)),
            percent_encode(&Self::format_bw_time(end)),
        );

        let mut records: Vec<Bytes> = Vec::new();
        let mut continuation: Option<String> = None;

        for page in 0..MAX_PAGES {
            let url = match continuation.as_ref() {
                Some(c) => format!("{base}&continuationToken={}", percent_encode(c)),
                None => base.clone(),
            };

            let resp = self
                .client
                .get(&url)
                .bearer_auth(&token)
                .header("Accept", "application/json")
                .send()
                .await
                .map_err(|e| Error::Source(format!("Bitwarden events request failed: {e}")))?;

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "Bitwarden events returned {status}: {body}"
                )));
            }

            let envelope: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("Bitwarden events parse failed: {e}")))?;

            let items = envelope["data"].as_array().cloned().unwrap_or_default();
            for item in &items {
                if let Ok(buf) = serde_json::to_vec(item) {
                    records.push(Bytes::from(buf));
                }
            }

            let next = envelope["continuationToken"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(String::from);

            debug!(
                page = page + 1,
                items = items.len(),
                total = records.len(),
                has_more = next.is_some(),
                "Bitwarden events page fetched"
            );

            match next {
                Some(c) => continuation = Some(c),
                None => break,
            }
        }

        if records.is_empty() {
            return Ok(None);
        }

        info!(records = records.len(), "Bitwarden events fetched");
        Ok(Some(FetchResult {
            records,
            source: "bitwarden.events".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}

#[async_trait]
impl Source for BitwardenSource {
    fn name(&self) -> &'static str {
        "bitwarden"
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
            "Fetching Bitwarden data"
        );

        let mut results: Vec<FetchResult> = Vec::new();
        for service in &self.config.services {
            let outcome = match service.name.as_str() {
                "events" => self.fetch_events(service, window).await,
                other => {
                    warn!(service = other, "Unknown Bitwarden service, skipping");
                    continue;
                }
            };
            match outcome {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => warn!(
                    error = %e,
                    service = %service.name,
                    "Bitwarden service fetch failed, continuing"
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
        "bitwarden".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        self.config
            .services
            .iter()
            .map(|s| s.name.as_str())
            .collect()
    }
}

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

    fn cfg() -> BitwardenSourceConfig {
        BitwardenSourceConfig {
            enabled: true,
            client_id: Some("organization.00000000-0000-0000-0000-000000000000".into()),
            client_secret: Some("test-secret".to_string().into()),
            ..BitwardenSourceConfig::default()
        }
    }

    #[test]
    fn defaults_point_at_cloud() {
        let src = BitwardenSource::new(cfg());
        assert_eq!(src.api_base(), "https://api.bitwarden.com");
        assert_eq!(
            src.identity_url(),
            "https://identity.bitwarden.com/connect/token"
        );
    }

    #[test]
    fn self_hosted_overrides_take_effect() {
        let mut c = cfg();
        c.api_url_override = Some("https://vault.example.com/api".into());
        c.identity_url_override = Some("https://vault.example.com/identity/connect/token".into());
        let src = BitwardenSource::new(c);
        assert_eq!(src.api_base(), "https://vault.example.com/api");
        assert_eq!(
            src.identity_url(),
            "https://vault.example.com/identity/connect/token"
        );
    }

    #[test]
    fn client_id_required() {
        let mut c = cfg();
        c.client_id = None;
        let src = BitwardenSource::new(c);
        assert!(src.client_id().is_err());
    }

    #[test]
    fn format_bw_time_rfc3339_seconds_z_suffix() {
        let t = DateTime::parse_from_rfc3339("2026-05-21T13:30:00.987Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(BitwardenSource::format_bw_time(t), "2026-05-21T13:30:00Z");
    }
}
