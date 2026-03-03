// Project:   dfe-fetcher
// File:      src/source/gcp/mod.rs
// Purpose:   GCP data source (Audit Logs, SCC, Cloud Logging)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
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
use tracing::{debug, info, warn};

use crate::config::GcpSourceConfig;
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, Source};

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

            if let Some(arr) = items {
                all_items.extend(arr.iter().cloned());
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

            if let Some(entries) = resp_json["entries"].as_array() {
                all_items.extend(entries.iter().cloned());
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

    async fn fetch(&self) -> Result<Vec<FetchResult>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        info!(services = self.config.services.len(), "Fetching GCP data");

        let mut results = Vec::new();
        for service in &self.config.services {
            let fetch_result = match service.name.as_str() {
                "audit_logs" => self.fetch_audit_logs(service).await?,
                "scc" => self.fetch_scc(service).await?,
                "cloud_logging" => self.fetch_cloud_logging(service).await?,
                other => {
                    warn!(service = other, "Unknown GCP service, skipping");
                    continue;
                }
            };
            if let Some(result) = fetch_result {
                results.push(result);
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
}

impl GcpSource {
    async fn fetch_audit_logs(
        &self,
        _service: &crate::config::GcpService,
    ) -> Result<Option<FetchResult>> {
        let token = self.get_access_token().await?;
        let project_id = self
            .config
            .project_id
            .as_deref()
            .ok_or_else(|| Error::Config("gcp.project_id is required for audit_logs".into()))?;

        let now = chrono::Utc::now();
        let start = now - chrono::Duration::hours(1);

        let body = serde_json::json!({
            "resourceNames": [format!("projects/{project_id}")],
            "filter": format!(
                "logName:\"cloudaudit.googleapis.com\" AND timestamp >= \"{}\"",
                start.to_rfc3339()
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
        let items = self.post_paginated_gcp(&token, &url, body, 10).await?;

        if items.is_empty() {
            return Ok(None);
        }

        let records: Vec<Bytes> = items
            .into_iter()
            .filter_map(|item| serde_json::to_vec(&item).ok().map(Bytes::from))
            .collect();

        info!(records = records.len(), "GCP audit logs fetched");
        Ok(Some(FetchResult {
            records,
            source: "gcp.audit_logs".to_string(),
            topic: self.config.topic.clone(),
        }))
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

        let now = chrono::Utc::now();
        let start = now - chrono::Duration::hours(1);

        let body = serde_json::json!({
            "resourceNames": [format!("projects/{project_id}")],
            "filter": format!("{filter} AND timestamp >= \"{}\"", start.to_rfc3339()),
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
