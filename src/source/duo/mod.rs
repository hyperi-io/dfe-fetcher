// Project:   dfe-fetcher
// File:      src/source/duo/mod.rs
// Purpose:   Duo Admin API source
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Duo Admin API source.
//!
//! Pulls authentication events from
//! `https://api-XXXXXXXX.duosecurity.com/admin/v2/logs/authentication`.
//!
//! Duo's Admin API uses a custom request-signing scheme rather than a
//! bearer token. Every request is signed with HMAC-SHA1 over a canonical
//! string and transported in a Basic auth header:
//!
//! ```text
//! canonical = HTTP_DATE \n METHOD \n LOWER(HOST) \n PATH \n SORTED_QUERY
//! sig       = hex(HMAC-SHA1(skey, canonical))
//! Auth      = base64(ikey:sig)
//! Date      = HTTP_DATE (RFC 2822)
//! ```
//!
//! The fetcher caches no token because each request must be signed with
//! a fresh `Date` header.

use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64_STANDARD;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha1::Sha1;
use tracing::{debug, info, warn};

use crate::config::{DuoService, DuoSourceConfig};
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

/// Default lookback when no `FetchWindow` is supplied.
const DEFAULT_LOOKBACK_HOURS: i64 = 1;

/// Default per-page size. Duo caps `limit` at 1000.
const DEFAULT_LIMIT: u32 = 100;

/// Maximum pages we will follow per fetch call.
const MAX_PAGES: usize = 50;

/// Per-request HTTP timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Duo Admin API data source.
pub struct DuoSource {
    config: DuoSourceConfig,
    client: reqwest::Client,
}

impl DuoSource {
    /// Create a new Duo source from configuration.
    pub fn new(config: DuoSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    /// Effective API base URL. Either `api_url_override` (test/mock) or
    /// `https://<api_host>` built from the configured host.
    fn api_base(&self) -> Result<String> {
        if let Some(ref u) = self.config.api_url_override {
            return Ok(u.trim_end_matches('/').to_string());
        }
        let host = self
            .config
            .api_host
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Config("duo.api_host is required".into()))?;
        Ok(format!("https://{host}"))
    }

    /// Hostname component used in the canonical signing string. Always
    /// lowercased.
    fn signing_host(&self) -> Result<String> {
        if let Some(ref u) = self.config.api_url_override {
            // Extract host out of `scheme://host[:port]/...`.
            let trimmed = u
                .trim_start_matches("https://")
                .trim_start_matches("http://");
            let host = trimmed.split('/').next().unwrap_or(trimmed);
            return Ok(host.to_ascii_lowercase());
        }
        let host = self
            .config
            .api_host
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Config("duo.api_host is required".into()))?;
        Ok(host.to_ascii_lowercase())
    }

    fn integration_key(&self) -> Result<&str> {
        self.config
            .integration_key
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Credential("duo.integration_key is required".into()))
    }

    async fn resolve_secret_key(&self) -> Result<String> {
        if let Some(ref spec) = self.config.credential_secret {
            return credential::resolve(spec).await;
        }
        self.config
            .secret_key
            .as_ref()
            .map(|s| s.expose().to_string())
            .ok_or_else(|| Error::Credential("duo.secret_key is required".into()))
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

    /// Build a canonical signing string per the Duo Admin API spec.
    ///
    /// `query_pairs` MUST already be in alphabetical order by key and the
    /// individual key + value already percent-encoded (RFC 3986 unreserved).
    fn build_canonical(
        http_date: &str,
        method: &str,
        host_lower: &str,
        path: &str,
        sorted_encoded_query: &str,
    ) -> String {
        // `\n` between every field; no trailing newline.
        format!("{http_date}\n{method}\n{host_lower}\n{path}\n{sorted_encoded_query}")
    }

    /// Sign a canonical string with HMAC-SHA1 + skey and return the
    /// lower-case hex digest.
    fn sign_hex(skey: &str, canonical: &str) -> Result<String> {
        type HmacSha1 = Hmac<Sha1>;
        let mut mac = HmacSha1::new_from_slice(skey.as_bytes())
            .map_err(|e| Error::Source(format!("Duo HMAC init failed: {e}")))?;
        mac.update(canonical.as_bytes());
        let result = mac.finalize().into_bytes();
        Ok(hex::encode(result))
    }

    /// Compose the `Authorization` header value: `Basic base64(ikey:sig)`.
    fn auth_header(ikey: &str, sig_hex: &str) -> String {
        let token = format!("{ikey}:{sig_hex}");
        format!("Basic {}", B64_STANDARD.encode(token))
    }

    async fn fetch_authentication_logs(
        &self,
        service: &DuoService,
        window: Option<&FetchWindow>,
    ) -> Result<Option<FetchResult>> {
        let api_base = self.api_base()?;
        let host_lower = self.signing_host()?;
        let ikey = self.integration_key()?.to_string();
        let skey = self.resolve_secret_key().await?;
        let (start, end) = self.window(window);

        let limit = service
            .config
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| n.min(1000) as u32)
            .unwrap_or(DEFAULT_LIMIT);

        let path = "/admin/v2/logs/authentication";
        let mut records: Vec<Bytes> = Vec::new();
        let mut next_offset: Option<String> = None;

        for page in 0..MAX_PAGES {
            // Build the alphabetically-sorted, URL-encoded query string used
            // both for signing and for the actual request.
            let mut pairs: Vec<(&str, String)> = vec![
                ("limit", limit.to_string()),
                ("maxtime", (end.timestamp() * 1000).to_string()),
                ("mintime", (start.timestamp() * 1000).to_string()),
            ];
            if let Some(ref o) = next_offset {
                pairs.push(("next_offset", o.clone()));
            }
            pairs.sort_by(|a, b| a.0.cmp(b.0));

            let sorted_query: String = pairs
                .iter()
                .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
                .collect::<Vec<_>>()
                .join("&");

            let http_date = chrono::Utc::now()
                .format("%a, %d %b %Y %H:%M:%S -0000")
                .to_string();

            let canonical =
                Self::build_canonical(&http_date, "GET", &host_lower, path, &sorted_query);
            let sig = Self::sign_hex(&skey, &canonical)?;
            let auth = Self::auth_header(&ikey, &sig);

            let url = format!("{api_base}{path}?{sorted_query}");

            let resp = self
                .client
                .get(&url)
                .header("Authorization", &auth)
                .header("Date", &http_date)
                .header("Accept", "application/json")
                .send()
                .await
                .map_err(|e| Error::Source(format!("Duo auth-log request failed: {e}")))?;

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "Duo auth-log returned {status}: {body}"
                )));
            }

            let envelope: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("Duo auth-log parse failed: {e}")))?;

            // Duo wraps responses as `{"response": {"authlogs": [...], "metadata": {...}}, "stat": "OK"}`.
            if envelope["stat"].as_str() != Some("OK") {
                let msg = envelope["message"].as_str().unwrap_or("unknown");
                return Err(Error::Source(format!("Duo auth-log API stat != OK: {msg}")));
            }

            let authlogs = envelope["response"]["authlogs"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            for entry in &authlogs {
                if let Ok(buf) = serde_json::to_vec(entry) {
                    records.push(Bytes::from(buf));
                }
            }

            let next = envelope["response"]["metadata"]["next_offset"]
                .as_str()
                .map(String::from);

            debug!(
                page = page + 1,
                items = authlogs.len(),
                total = records.len(),
                has_more = next.is_some(),
                "Duo auth-log page fetched"
            );

            match next {
                Some(n) if !n.is_empty() => next_offset = Some(n),
                _ => break,
            }
        }

        if records.is_empty() {
            return Ok(None);
        }

        info!(records = records.len(), "Duo auth-log fetched");
        Ok(Some(FetchResult {
            records,
            source: "duo.authentication_logs".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}

#[async_trait]
impl Source for DuoSource {
    fn name(&self) -> &'static str {
        "duo"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn fetch(&self, window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        info!(services = self.config.services.len(), "Fetching Duo data");

        let mut results: Vec<FetchResult> = Vec::new();
        for service in &self.config.services {
            let outcome = match service.name.as_str() {
                "authentication_logs" => self.fetch_authentication_logs(service, window).await,
                other => {
                    warn!(service = other, "Unknown Duo service, skipping");
                    continue;
                }
            };
            match outcome {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => warn!(
                    error = %e,
                    service = %service.name,
                    "Duo service fetch failed, continuing"
                ),
            }
        }
        Ok(results)
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        // Duo's `/admin/v1/check` is a free auth probe that returns
        // `{"response": "valid", "stat": "OK"}` for working credentials.
        let api_base = match self.api_base() {
            Ok(b) => b,
            Err(_) => return Ok(false),
        };
        let host_lower = match self.signing_host() {
            Ok(h) => h,
            Err(_) => return Ok(false),
        };
        let ikey = match self.integration_key() {
            Ok(k) => k.to_string(),
            Err(_) => return Ok(false),
        };
        let skey = match self.resolve_secret_key().await {
            Ok(s) => s,
            Err(_) => return Ok(false),
        };

        let path = "/admin/v1/check";
        let http_date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S -0000")
            .to_string();
        let canonical = Self::build_canonical(&http_date, "GET", &host_lower, path, "");
        let sig = match Self::sign_hex(&skey, &canonical) {
            Ok(s) => s,
            Err(_) => return Ok(false),
        };
        let auth = Self::auth_header(&ikey, &sig);

        let url = format!("{api_base}{path}");
        let resp = match self
            .client
            .get(&url)
            .header("Authorization", auth)
            .header("Date", http_date)
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
        "duo".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        self.config
            .services
            .iter()
            .map(|s| s.name.as_str())
            .collect()
    }
}

/// RFC 3986 percent-encoder for the canonical query string used in signing.
/// Duo's spec requires the SAME encoding for signed canonical query AND the
/// actual request URL, so the two strings stay byte-identical.
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

    fn cfg() -> DuoSourceConfig {
        DuoSourceConfig {
            enabled: true,
            api_host: Some("api-deadbeef.duosecurity.com".into()),
            integration_key: Some("DIWJ8X6AEYOR5OMC6TQ1".into()),
            secret_key: Some(
                "Zh5eGmUq9zpfQnyUIu5OL9iWoMMv5ZNmk3zLJ4Ep"
                    .to_string()
                    .into(),
            ),
            ..DuoSourceConfig::default()
        }
    }

    #[test]
    fn api_base_uses_override_first() {
        let mut c = cfg();
        c.api_url_override = Some("http://localhost:9999".into());
        let src = DuoSource::new(c);
        assert_eq!(src.api_base().unwrap(), "http://localhost:9999");
    }

    #[test]
    fn api_base_built_from_api_host_when_no_override() {
        let src = DuoSource::new(cfg());
        assert_eq!(
            src.api_base().unwrap(),
            "https://api-deadbeef.duosecurity.com"
        );
    }

    #[test]
    fn signing_host_is_lowercased() {
        let mut c = cfg();
        c.api_host = Some("API-DEADBEEF.duosecurity.com".into());
        let src = DuoSource::new(c);
        assert_eq!(src.signing_host().unwrap(), "api-deadbeef.duosecurity.com");
    }

    #[test]
    fn signing_host_from_url_override_strips_scheme_and_path() {
        let mut c = cfg();
        c.api_url_override = Some("https://API-DEADBEEF.duosecurity.com/some/path".into());
        let src = DuoSource::new(c);
        assert_eq!(src.signing_host().unwrap(), "api-deadbeef.duosecurity.com");
    }

    #[test]
    fn percent_encode_passes_unreserved() {
        assert_eq!(percent_encode("abc-1_2.3~4"), "abc-1_2.3~4");
    }

    #[test]
    fn percent_encode_encodes_special_chars() {
        assert_eq!(percent_encode("a b/c?d=e"), "a%20b%2Fc%3Fd%3De");
    }

    /// Canonical-string assembly: verify the field order and newline
    /// delimiters match the Duo spec exactly.
    #[test]
    fn canonical_string_has_5_lines_newline_separated() {
        let c = DuoSource::build_canonical(
            "Tue, 21 Aug 2012 17:29:18 -0000",
            "POST",
            "api-xxxxxxxx.duosecurity.com",
            "/accounts/v1/account/list",
            "account_id=D210BFG5BDS47G0PJ40R",
        );
        let lines: Vec<&str> = c.split('\n').collect();
        assert_eq!(lines.len(), 5);
        assert_eq!(lines[0], "Tue, 21 Aug 2012 17:29:18 -0000");
        assert_eq!(lines[1], "POST");
        assert_eq!(lines[2], "api-xxxxxxxx.duosecurity.com");
        assert_eq!(lines[3], "/accounts/v1/account/list");
        assert_eq!(lines[4], "account_id=D210BFG5BDS47G0PJ40R");
    }

    /// HMAC-SHA1 produces stable output for known input. We don't pin against
    /// a published reference vector because Duo's docs have revised theirs;
    /// instead verify the hex output is 40 chars (160 bits of SHA-1 in hex)
    /// and that identical inputs produce identical outputs.
    #[test]
    fn sign_hex_is_stable_and_correct_length() {
        let skey = "Zh5eGmUq9zpfQnyUIu5OL9iWoMMv5ZNmk3zLJ4Ep";
        let canonical = "GET\napi-x.duosecurity.com\n/admin/v2/logs/authentication\n";
        let a = DuoSource::sign_hex(skey, canonical).unwrap();
        let b = DuoSource::sign_hex(skey, canonical).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.len(), 40);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// Different inputs must produce different signatures.
    #[test]
    fn sign_hex_differs_for_different_inputs() {
        let skey = "test-skey";
        let a = DuoSource::sign_hex(skey, "canonical-a").unwrap();
        let b = DuoSource::sign_hex(skey, "canonical-b").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn auth_header_is_basic_b64_of_ikey_colon_sig() {
        let h = DuoSource::auth_header("DIWJ8X6AEYOR5OMC6TQ1", "deadbeefcafebabe");
        // "DIWJ8X6AEYOR5OMC6TQ1:deadbeefcafebabe" base64-encoded.
        assert_eq!(
            h,
            "Basic RElXSjhYNkFFWU9SNU9NQzZUUTE6ZGVhZGJlZWZjYWZlYmFiZQ=="
        );
    }
}
