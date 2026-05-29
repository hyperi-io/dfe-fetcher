// Project:   dfe-fetcher
// File:      src/source/gcp/mod.rs
// Purpose:   GCP data source (Audit Logs, SCC, Cloud Logging)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Google Cloud Platform data source.
//!
//! Fetches security and operational data from GCP services:
//! - Cloud Audit Logs (admin/data access activity)
//! - Security Command Center (security findings)
//! - Cloud Logging (application and infrastructure logs)
//!
//! Authentication: Service account key file or workload identity.
//! Uses Google Cloud REST APIs with OAuth2 bearer tokens.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use crate::config::GcpSourceConfig;
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source, SourceMaturity};

/// A Cloud Logging-backed GCP service.
///
/// Maps a fetcher service name to a Cloud Logging filter clause. The fetcher
/// appends a timestamp range and posts to `logging.googleapis.com/v2/entries:list`.
struct GcpLoggingService {
    /// Service config name; also used as the source suffix
    /// (becomes `gcp.<service_name>`).
    service_name: &'static str,
    /// Cloud Logging filter clause excluding timestamp range. The dispatcher
    /// appends `AND timestamp >= "..." AND timestamp < "..."`.
    ///
    /// Reference: <https://cloud.google.com/logging/docs/view/logging-query-language>
    filter_clause: &'static str,
}

/// Cloud Logging-backed GCP services.
///
/// Each entry becomes a distinct fetcher service with its own source tag and
/// cursor. Some require tenant-side enablement (see the per-entry comments).
const GCP_LOGGING_SERVICES: &[GcpLoggingService] = &[
    // -- Cloud Audit Log subtypes (always on for `admin_activity` /
    // `system_event` / `policy_denied`; tenant must enable Data Access audit
    // logs in IAM policy for `data_access` to return records).
    GcpLoggingService {
        service_name: "admin_activity",
        filter_clause: "log_id(\"cloudaudit.googleapis.com/activity\")",
    },
    GcpLoggingService {
        service_name: "data_access",
        filter_clause: "log_id(\"cloudaudit.googleapis.com/data_access\")",
    },
    GcpLoggingService {
        service_name: "system_event",
        filter_clause: "log_id(\"cloudaudit.googleapis.com/system_event\")",
    },
    GcpLoggingService {
        service_name: "policy_denied",
        filter_clause: "log_id(\"cloudaudit.googleapis.com/policy\")",
    },
    // -- Other Cloud Logging streams (tenant-side enablement listed).
    // VPC Flow Logs: tenant enables per-subnet via
    // `gcloud compute networks subnets update --enable-flow-logs`.
    GcpLoggingService {
        service_name: "vpc_flow_logs",
        filter_clause: "log_id(\"compute.googleapis.com/vpc_flows\")",
    },
    // Cloud DNS query logs: tenant enables via a DNS server policy with
    // `enable_logging = true` on the policy attached to the relevant network.
    GcpLoggingService {
        service_name: "dns_queries",
        filter_clause: "log_id(\"dns.googleapis.com/dns_queries\")",
    },
    // Cloud Storage data-access events: subset of the `data_access` subtype
    // narrowed to `gcs_bucket` resources. Useful when consumers want
    // Storage-only audit without the wider data_access volume. Requires the
    // same Data Access audit-log enablement as the `data_access` service.
    GcpLoggingService {
        service_name: "storage_access",
        filter_clause: "resource.type=\"gcs_bucket\" AND log_id(\"cloudaudit.googleapis.com/data_access\")",
    },
];

/// Service names dispatchable to the Cloud Logging path (for the match arm).
const GCP_LOGGING_SERVICE_NAMES: &[&str] = &[
    "admin_activity",
    "data_access",
    "system_event",
    "policy_denied",
    "vpc_flow_logs",
    "dns_queries",
    "storage_access",
];

/// GCP data source implementation.
pub struct GcpSource {
    config: GcpSourceConfig,
    client: reqwest::Client,
}

impl GcpSource {
    /// Create a new GCP source from configuration.
    pub fn new(config: GcpSourceConfig) -> Self {
        let client = credential::http_client().unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    /// Obtain a GCP access token.
    ///
    /// Uses service account key file for JWT -> access token exchange,
    /// or falls back to metadata server for workload identity.
    async fn get_access_token(&self) -> Result<String> {
        // If a credential secret is specified, resolve it
        if let Some(ref spec) = self.config.credential_secret {
            let token = credential::resolve(spec).await?;
            return Ok(token);
        }

        // If service account key file is provided, use it for token exchange
        if let Some(ref key_path) = self.config.service_account_key {
            let key_path = credential::resolve(key_path).await?;
            return self.token_from_service_account(&key_path).await;
        }

        // Fall back to GCP metadata server (workload identity / GCE)
        self.token_from_metadata().await
    }

    /// Exchange a service account key file for an access token.
    async fn token_from_service_account(&self, key_path: &str) -> Result<String> {
        let key_json = std::fs::read_to_string(key_path)
            .map_err(|e| Error::Credential(format!("failed to read GCP key file: {e}")))?;

        let key: serde_json::Value = serde_json::from_str(&key_json)
            .map_err(|e| Error::Credential(format!("invalid GCP key JSON: {e}")))?;

        let client_email = key["client_email"]
            .as_str()
            .ok_or_else(|| Error::Credential("missing client_email in GCP key".into()))?;
        let token_uri = self
            .config
            .token_url_override
            .as_deref()
            .or_else(|| key["token_uri"].as_str())
            .unwrap_or("https://oauth2.googleapis.com/token");
        let private_key = key["private_key"]
            .as_str()
            .ok_or_else(|| Error::Credential("missing private_key in GCP key".into()))?;

        // Build JWT claims for service account token exchange
        let now = chrono::Utc::now().timestamp();
        let claims = serde_json::json!({
            "iss": client_email,
            "scope": "https://www.googleapis.com/auth/cloud-platform",
            "aud": token_uri,
            "iat": now,
            "exp": now + 3600,
        });

        // Sign JWT with RS256 using the service account's private key
        let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key.as_bytes())
            .map_err(|e| Error::Credential(format!("invalid GCP private key: {e}")))?;
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        let jwt = jsonwebtoken::encode(&header, &claims, &encoding_key)
            .map_err(|e| Error::Credential(format!("JWT signing failed: {e}")))?;

        // Exchange signed JWT for an access token
        let resp = self
            .client
            .post(token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &jwt),
            ])
            .send()
            .await
            .map_err(|e| Error::Credential(format!("GCP token exchange failed: {e}")))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Credential(format!(
                "GCP token exchange error: {body}"
            )));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| Error::Credential(format!("failed to parse GCP token response: {e}")))?;

        body["access_token"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| Error::Credential("missing access_token in GCP response".into()))
    }

    /// Get token from GCP metadata server (for workload identity / GCE).
    async fn token_from_metadata(&self) -> Result<String> {
        let url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

        let resp = self
            .client
            .get(url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map_err(|e| Error::Credential(format!("GCP metadata request failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(Error::Credential(
                "GCP metadata server unavailable — not running on GCE/GKE?".into(),
            ));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| Error::Credential(format!("failed to parse metadata response: {e}")))?;

        body["access_token"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| Error::Credential("missing access_token in metadata response".into()))
    }

    /// Fetch paginated results from a GCP API using `nextPageToken`.
    async fn fetch_paginated_gcp(
        &self,
        token: &str,
        initial_url: &str,
        max_pages: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let mut all_items = Vec::new();
        let mut page_token: Option<String> = None;

        for page in 0..max_pages {
            let url = match &page_token {
                Some(pt) => {
                    if initial_url.contains('?') {
                        format!("{initial_url}&pageToken={pt}")
                    } else {
                        format!("{initial_url}?pageToken={pt}")
                    }
                }
                None => initial_url.to_string(),
            };

            let resp = self
                .client
                .get(&url)
                .bearer_auth(token)
                .send()
                .await
                .map_err(|e| Error::Source(format!("GCP API request failed: {e}")))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!("GCP API returned {status}: {body}")));
            }

            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("failed to parse GCP response: {e}")))?;

            // GCP APIs vary: some use "entries", some "findings", some "listFindingsResults"
            let items = body["entries"]
                .as_array()
                .or_else(|| body["findings"].as_array())
                .or_else(|| body["listFindingsResults"].as_array())
                .or_else(|| body["results"].as_array());

            if let Some(arr) = items.cloned() {
                all_items.extend(arr);
            }

            page_token = body["nextPageToken"].as_str().map(String::from);
            match &page_token {
                Some(_) => {
                    debug!(
                        page = page + 1,
                        items = all_items.len(),
                        "Fetching next page"
                    );
                }
                None => break,
            }
        }

        Ok(all_items)
    }

    /// POST-based paginated fetch (for Cloud Logging entries.list).
    async fn post_paginated_gcp(
        &self,
        token: &str,
        url: &str,
        mut body: serde_json::Value,
        max_pages: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let mut all_items = Vec::new();

        for page in 0..max_pages {
            let resp = self
                .client
                .post(url)
                .bearer_auth(token)
                .json(&body)
                .send()
                .await
                .map_err(|e| Error::Source(format!("GCP API request failed: {e}")))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let resp_body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "GCP API returned {status}: {resp_body}"
                )));
            }

            let resp_json: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("failed to parse GCP response: {e}")))?;

            if let Some(entries) = resp_json["entries"].as_array().cloned() {
                all_items.extend(entries);
            }

            match resp_json["nextPageToken"].as_str() {
                Some(pt) => {
                    body["pageToken"] = serde_json::Value::String(pt.to_string());
                    debug!(
                        page = page + 1,
                        items = all_items.len(),
                        "Fetching next page"
                    );
                }
                None => break,
            }
        }

        Ok(all_items)
    }
}

#[async_trait]
impl Source for GcpSource {
    fn name(&self) -> &'static str {
        "gcp"
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

        info!(services = self.config.services.len(), "Fetching GCP data");

        let now = Utc::now();
        let (start, end) = match window {
            Some(w) => (w.start, w.end),
            None => (now - chrono::Duration::hours(1), now),
        };

        // Concurrent service fetching via join_all (no spawn, borrows &self)
        let service_futures: Vec<_> = self
            .config
            .services
            .iter()
            .filter_map(|service| {
                let fut: std::pin::Pin<
                    Box<
                        dyn std::future::Future<Output = (&str, Result<Option<FetchResult>>)>
                            + Send
                            + '_,
                    >,
                > = match service.name.as_str() {
                    // Per-subtype audit services. Each emits its own FetchResult
                    // tagged `gcp.admin_activity` / `gcp.data_access` /
                    // `gcp.system_event` / `gcp.policy_denied`. There is
                    // intentionally no combined `audit_logs` service - consumers
                    // configure each subtype they want with its own cursor and
                    // source tag.
                    name if GCP_LOGGING_SERVICE_NAMES.contains(&name) => Box::pin(async move {
                        (
                            &*service.name,
                            self.fetch_logging_service(name, start, end).await,
                        )
                    }),
                    "scc" => {
                        Box::pin(async move { (&*service.name, self.fetch_scc(service).await) })
                    }
                    "cloud_logging" => Box::pin(async move {
                        (
                            &*service.name,
                            self.fetch_cloud_logging(service, start, end).await,
                        )
                    }),
                    other => {
                        warn!(service = other, "Unknown GCP service, skipping");
                        return None;
                    }
                };
                Some(fut)
            })
            .collect();

        let mut results = Vec::new();
        for (name, fetch_result) in futures::future::join_all(service_futures).await {
            match fetch_result {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => warn!(error = %e, service = name, "GCP service fetch failed, continuing"),
            }
        }
        Ok(results)
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        match self.get_access_token().await {
            Ok(_) => Ok(true),
            Err(e) => {
                warn!(error = %e, "GCP health check failed");
                Ok(false)
            }
        }
    }

    fn cursor_prefix(&self) -> String {
        "gcp".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        self.config
            .services
            .iter()
            .map(|s| s.name.as_str())
            .collect()
    }
}

impl GcpSource {
    /// Fetch a Cloud Logging-backed service entry with a fixed filter clause.
    ///
    /// Returns `Ok(None)` when no entries fall in the window. Tags the result
    /// `gcp.<service_name>` (e.g. `gcp.admin_activity`, `gcp.vpc_flow_logs`).
    async fn fetch_logging_entries(
        &self,
        token: &str,
        svc: &GcpLoggingService,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        let project_id = self.config.project_id.as_deref().ok_or_else(|| {
            Error::Config(format!(
                "gcp.project_id is required for {}",
                svc.service_name
            ))
        })?;

        let body = serde_json::json!({
            "resourceNames": [format!("projects/{project_id}")],
            "filter": format!(
                "{} AND timestamp >= \"{}\" AND timestamp < \"{}\"",
                svc.filter_clause,
                start.to_rfc3339(),
                end.to_rfc3339()
            ),
            "pageSize": 100,
            "orderBy": "timestamp desc"
        });

        let api_base = self
            .config
            .api_url_override
            .as_deref()
            .unwrap_or("https://logging.googleapis.com");
        let url = format!("{api_base}/v2/entries:list");
        let items = self.post_paginated_gcp(token, &url, body, 10).await?;

        if items.is_empty() {
            return Ok(None);
        }

        let records: Vec<Bytes> = items
            .into_iter()
            .filter_map(|item| serde_json::to_vec(&item).ok().map(Bytes::from))
            .collect();

        if records.is_empty() {
            return Ok(None);
        }

        info!(
            records = records.len(),
            service = svc.service_name,
            "GCP Cloud Logging service fetched"
        );
        Ok(Some(FetchResult {
            records,
            source: format!("gcp.{}", svc.service_name),
            topic: self.config.topic.clone(),
        }))
    }

    /// Dispatch a Cloud Logging-backed service to its filter entry.
    async fn fetch_logging_service(
        &self,
        service_name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        let svc = GCP_LOGGING_SERVICES
            .iter()
            .find(|s| s.service_name == service_name)
            .ok_or_else(|| {
                Error::Source(format!("unknown GCP Cloud Logging service: {service_name}"))
            })?;
        let token = self.get_access_token().await?;
        self.fetch_logging_entries(&token, svc, start, end).await
    }

    async fn fetch_scc(&self, _service: &crate::config::GcpService) -> Result<Option<FetchResult>> {
        let token = self.get_access_token().await?;

        // SCC requires an organization ID — look in service config
        let org_id = self
            .config
            .services
            .iter()
            .find(|s| s.name == "scc")
            .and_then(|s| s.config.get("organization_id"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        if org_id.is_empty() {
            warn!("GCP SCC requires organization_id in service config");
            return Ok(None);
        }

        let api_base = self
            .config
            .api_url_override
            .as_deref()
            .unwrap_or("https://securitycenter.googleapis.com");
        let url = format!("{api_base}/v1/organizations/{org_id}/sources/-/findings?pageSize=100");

        let items = self.fetch_paginated_gcp(&token, &url, 10).await?;
        if items.is_empty() {
            return Ok(None);
        }

        let records: Vec<Bytes> = items
            .into_iter()
            .filter_map(|item| serde_json::to_vec(&item).ok().map(Bytes::from))
            .collect();

        info!(records = records.len(), "GCP SCC findings fetched");
        Ok(Some(FetchResult {
            records,
            source: "gcp.scc".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_cloud_logging(
        &self,
        _service: &crate::config::GcpService,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        let token = self.get_access_token().await?;
        let project_id =
            self.config.project_id.as_deref().ok_or_else(|| {
                Error::Config("gcp.project_id is required for cloud_logging".into())
            })?;

        // Get custom filter from service config, or use default
        let filter = self
            .config
            .services
            .iter()
            .find(|s| s.name == "cloud_logging")
            .and_then(|s| s.config.get("filter"))
            .and_then(|v| v.as_str())
            .unwrap_or("severity >= WARNING");

        let body = serde_json::json!({
            "resourceNames": [format!("projects/{project_id}")],
            "filter": format!("{filter} AND timestamp >= \"{}\" AND timestamp < \"{}\"", start.to_rfc3339(), end.to_rfc3339()),
            "pageSize": 100,
            "orderBy": "timestamp desc"
        });

        let api_base = self
            .config
            .api_url_override
            .as_deref()
            .unwrap_or("https://logging.googleapis.com");
        let url = format!("{api_base}/v2/entries:list");
        let items = self.post_paginated_gcp(&token, &url, body, 10).await?;

        if items.is_empty() {
            return Ok(None);
        }

        let records: Vec<Bytes> = items
            .into_iter()
            .filter_map(|item| serde_json::to_vec(&item).ok().map(Bytes::from))
            .collect();

        info!(records = records.len(), "GCP cloud logging entries fetched");
        Ok(Some(FetchResult {
            records,
            source: "gcp.cloud_logging".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn svc(name: &str) -> &'static GcpLoggingService {
        GCP_LOGGING_SERVICES
            .iter()
            .find(|s| s.service_name == name)
            .expect("service must exist in GCP_LOGGING_SERVICES")
    }

    #[test]
    fn logging_services_includes_four_audit_subtypes() {
        let names: std::collections::HashSet<&str> = GCP_LOGGING_SERVICES
            .iter()
            .map(|s| s.service_name)
            .collect();
        for expected in [
            "admin_activity",
            "data_access",
            "system_event",
            "policy_denied",
        ] {
            assert!(
                names.contains(expected),
                "audit subtype {expected} missing from GCP_LOGGING_SERVICES"
            );
        }
    }

    #[test]
    fn logging_services_includes_level_2_streams() {
        let names: std::collections::HashSet<&str> = GCP_LOGGING_SERVICES
            .iter()
            .map(|s| s.service_name)
            .collect();
        for expected in ["vpc_flow_logs", "dns_queries", "storage_access"] {
            assert!(
                names.contains(expected),
                "Level 2 service {expected} missing from GCP_LOGGING_SERVICES"
            );
        }
    }

    #[test]
    fn logging_service_names_table_matches_struct_table() {
        let struct_names: std::collections::HashSet<&str> = GCP_LOGGING_SERVICES
            .iter()
            .map(|s| s.service_name)
            .collect();
        let flat_names: std::collections::HashSet<&str> =
            GCP_LOGGING_SERVICE_NAMES.iter().copied().collect();
        assert_eq!(
            struct_names, flat_names,
            "GCP_LOGGING_SERVICES and GCP_LOGGING_SERVICE_NAMES must agree on service names"
        );
    }

    #[test]
    fn policy_denied_uses_policy_log_id() {
        // Service is named `policy_denied` for clarity; underlying log id is `policy`.
        assert_eq!(
            svc("policy_denied").filter_clause,
            "log_id(\"cloudaudit.googleapis.com/policy\")"
        );
    }

    #[test]
    fn vpc_flow_logs_filter_matches_official_log_id() {
        assert_eq!(
            svc("vpc_flow_logs").filter_clause,
            "log_id(\"compute.googleapis.com/vpc_flows\")"
        );
    }

    #[test]
    fn dns_queries_filter_matches_official_log_id() {
        assert_eq!(
            svc("dns_queries").filter_clause,
            "log_id(\"dns.googleapis.com/dns_queries\")"
        );
    }

    #[test]
    fn storage_access_filter_scopes_to_gcs_bucket_resource() {
        assert_eq!(
            svc("storage_access").filter_clause,
            "resource.type=\"gcs_bucket\" AND log_id(\"cloudaudit.googleapis.com/data_access\")"
        );
    }

    #[test]
    fn no_duplicate_service_names() {
        let mut names: Vec<&str> = GCP_LOGGING_SERVICES
            .iter()
            .map(|s| s.service_name)
            .collect();
        names.sort_unstable();
        let len_before = names.len();
        names.dedup();
        assert_eq!(
            len_before,
            names.len(),
            "GCP_LOGGING_SERVICES has duplicate service_name entries"
        );
    }
}
