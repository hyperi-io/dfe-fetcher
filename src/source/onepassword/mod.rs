// Project:   dfe-fetcher
// File:      src/source/onepassword/mod.rs
// Purpose:   1Password Events Reporting source
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! 1Password Events Reporting source.
//!
//! Pulls events from the 1Password Events Reporting API at
//! `events.1password.com/api/v2/{signinattempts,itemusages,auditevents}`.
//! Requires a 1Password Business or Enterprise account with Events Reporting
//! enabled on the tenant.
//!
//! Authentication: bearer token from the 1Password Business dashboard
//! (Integrations > Directory > Events Reporting > Add Integration).
//!
//! Pagination: POST a `cursor` body until the response indicates `has_more`
//! is false. Each event-class endpoint takes the same request body shape
//! (time-window on first call, cursor on subsequent calls).

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use crate::config::{OnePasswordService, OnePasswordSourceConfig};
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

/// Default lookback when no `FetchWindow` is supplied.
const DEFAULT_LOOKBACK_HOURS: i64 = 1;

/// Default per-page size. 1Password caps `limit` at 1000.
const DEFAULT_LIMIT: u32 = 100;

/// Maximum pages we will follow per fetch call.
const MAX_PAGES: usize = 50;

/// Per-request HTTP timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Mapping of fetcher service name to 1Password Events Reporting endpoint.
struct OpEventEndpoint {
    /// Service config name; also used as the source suffix.
    service_name: &'static str,
    /// Path segment under `/api/v2/`.
    path: &'static str,
}

const OP_ENDPOINTS: &[OpEventEndpoint] = &[
    OpEventEndpoint {
        service_name: "signin_attempts",
        path: "signinattempts",
    },
    OpEventEndpoint {
        service_name: "item_usages",
        path: "itemusages",
    },
    OpEventEndpoint {
        service_name: "audit_events",
        path: "auditevents",
    },
];

const OP_SERVICE_NAMES: &[&str] = &["signin_attempts", "item_usages", "audit_events"];

/// 1Password Events Reporting data source.
pub struct OnePasswordSource {
    config: OnePasswordSourceConfig,
    client: reqwest::Client,
}

impl OnePasswordSource {
    /// Create a new 1Password source from configuration.
    pub fn new(config: OnePasswordSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    fn api_base(&self) -> &str {
        self.config
            .api_url_override
            .as_deref()
            .unwrap_or("https://events.1password.com")
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
            .ok_or_else(|| Error::Credential("onepassword.token is required".into()))
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

    /// 1Password Events Reporting accepts RFC 3339 timestamps with `Z` suffix.
    fn format_op_time(ts: DateTime<Utc>) -> String {
        ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }

    async fn fetch_endpoint(
        &self,
        endpoint: &OpEventEndpoint,
        service: &OnePasswordService,
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

        let url = format!("{}/api/v2/{}", self.api_base(), endpoint.path);

        // First call: time-window body. Subsequent calls: cursor body.
        let initial_body = serde_json::json!({
            "limit": limit,
            "start_time": Self::format_op_time(start),
            "end_time": Self::format_op_time(end),
        });

        let mut records: Vec<Bytes> = Vec::new();
        let mut body = initial_body;

        for page in 0..MAX_PAGES {
            let resp = self
                .client
                .post(&url)
                .bearer_auth(&token)
                .header("Accept", "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|e| {
                    Error::Source(format!(
                        "1Password Events request failed ({}): {e}",
                        endpoint.service_name
                    ))
                })?;

            let status = resp.status();
            if !status.is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "1Password Events ({}) returned {status}: {body_text}",
                    endpoint.service_name
                )));
            }

            let envelope: serde_json::Value = resp.json().await.map_err(|e| {
                Error::Source(format!(
                    "1Password Events parse failed ({}): {e}",
                    endpoint.service_name
                ))
            })?;

            let items = envelope["items"].as_array().cloned().unwrap_or_default();
            for item in &items {
                if let Ok(buf) = serde_json::to_vec(item) {
                    records.push(Bytes::from(buf));
                }
            }

            let has_more = envelope["has_more"].as_bool().unwrap_or(false);
            let cursor = envelope["cursor"].as_str().map(String::from);

            debug!(
                page = page + 1,
                items = items.len(),
                total = records.len(),
                has_more,
                "1Password Events page fetched"
            );

            match (has_more, cursor) {
                (true, Some(c)) => {
                    body = serde_json::json!({ "cursor": c });
                }
                _ => break,
            }
        }

        if records.is_empty() {
            return Ok(None);
        }

        info!(
            records = records.len(),
            endpoint = endpoint.service_name,
            "1Password Events fetched"
        );
        Ok(Some(FetchResult {
            records,
            source: format!("onepassword.{}", endpoint.service_name),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_one(
        &self,
        service: &OnePasswordService,
        window: Option<&FetchWindow>,
    ) -> Result<Option<FetchResult>> {
        let endpoint = OP_ENDPOINTS
            .iter()
            .find(|e| e.service_name == service.name)
            .ok_or_else(|| Error::Source(format!("unknown 1Password service: {}", service.name)))?;
        self.fetch_endpoint(endpoint, service, window).await
    }
}

#[async_trait]
impl Source for OnePasswordSource {
    fn name(&self) -> &'static str {
        "onepassword"
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
            "Fetching 1Password data"
        );

        let mut results: Vec<FetchResult> = Vec::new();
        for service in &self.config.services {
            if !OP_SERVICE_NAMES.contains(&service.name.as_str()) {
                warn!(service = %service.name, "Unknown 1Password service, skipping");
                continue;
            }
            match self.fetch_one(service, window).await {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => warn!(
                    error = %e,
                    service = %service.name,
                    "1Password service fetch failed, continuing"
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
        // `/api/auth/introspect` returns 200 + token metadata when the token
        // is valid. It does NOT consume any of the per-endpoint event budget.
        let url = format!("{}/api/auth/introspect", self.api_base());
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
        "onepassword".to_string()
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

    fn cfg() -> OnePasswordSourceConfig {
        OnePasswordSourceConfig {
            enabled: true,
            token: Some("test-token".to_string().into()),
            ..OnePasswordSourceConfig::default()
        }
    }

    #[test]
    fn api_base_default_when_no_override() {
        let src = OnePasswordSource::new(cfg());
        assert_eq!(src.api_base(), "https://events.1password.com");
    }

    #[test]
    fn api_base_uses_override_when_present() {
        let mut c = cfg();
        c.api_url_override = Some("https://events.ent.1password.eu".into());
        let src = OnePasswordSource::new(c);
        assert_eq!(src.api_base(), "https://events.ent.1password.eu");
    }

    #[test]
    fn endpoint_paths_match_service_names() {
        for ep in OP_ENDPOINTS {
            let expected = match ep.service_name {
                "signin_attempts" => "signinattempts",
                "item_usages" => "itemusages",
                "audit_events" => "auditevents",
                other => panic!("unexpected 1Password service {other}"),
            };
            assert_eq!(
                ep.path, expected,
                "wrong endpoint path for {}",
                ep.service_name
            );
        }
    }

    #[test]
    fn endpoint_table_and_service_name_table_agree() {
        let endpoint_names: std::collections::HashSet<&str> =
            OP_ENDPOINTS.iter().map(|e| e.service_name).collect();
        let flat: std::collections::HashSet<&str> = OP_SERVICE_NAMES.iter().copied().collect();
        assert_eq!(endpoint_names, flat);
    }

    #[test]
    fn format_op_time_has_millis_and_z_suffix() {
        let t = DateTime::parse_from_rfc3339("2026-05-21T13:30:00.987654321Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            OnePasswordSource::format_op_time(t),
            "2026-05-21T13:30:00.987Z"
        );
    }
}
