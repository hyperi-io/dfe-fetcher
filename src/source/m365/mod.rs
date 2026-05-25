// Project:   dfe-fetcher
// File:      src/source/m365/mod.rs
// Purpose:   Microsoft 365 data source (Audit Log, Message Trace, DLP, Alerts)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Microsoft 365 data source.
//!
//! Fetches security and compliance data from M365 services:
//! - Unified Audit Log (user/admin activity)
//! - Message Trace (email flow)
//! - DLP (Data Loss Prevention alerts)
//! - Security & Compliance Alerts
//!
//! Authentication: OAuth2 client_credentials via Microsoft Graph / Office 365 Management API.

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::config::M365SourceConfig;
use crate::credential::{self, TokenManager};
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

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

    /// Build a token manager for the Office 365 Management Activity API.
    async fn management_token_manager(&self) -> Result<TokenManager> {
        let tenant_id = self
            .config
            .tenant_id
            .as_deref()
            .ok_or_else(|| Error::Credential("m365.tenant_id is required".into()))?;
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
        let tenant_id = self
            .config
            .tenant_id
            .as_deref()
            .ok_or_else(|| Error::Credential("m365.tenant_id is required".into()))?;
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
            return credential::resolve(secret_spec).await.map_err(Into::into);
        }
        self.config
            .client_id
            .clone()
            .ok_or_else(|| Error::Credential("m365.client_id is required".into()))
    }

    async fn resolve_client_secret(&self) -> Result<String> {
        if let Some(ref secret_spec) = self.config.credential_secret {
            return credential::resolve(secret_spec).await.map_err(Into::into);
        }
        self.config
            .client_secret
            .as_ref()
            .map(|s| s.expose().to_string())
            .ok_or_else(|| Error::Credential("m365.client_secret is required".into()))
    }

    /// Fetch paginated results following `@odata.nextLink`.
    async fn fetch_paginated(
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
                .map_err(|e| Error::Source(format!("M365 API request failed: {e}")))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!("M365 API returned {status}: {body}")));
            }

            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("failed to parse M365 response: {e}")))?;

            if let Some(items) = body["value"].as_array().cloned() {
                all_items.extend(items);
            }

            let next_link = body["@odata.nextLink"]
                .as_str()
                .or_else(|| body["NextPageUri"].as_str());

            match next_link {
                Some(next) => {
                    url = next.to_string();
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

    /// Start an Office 365 Management Activity API subscription.
    async fn start_subscription(
        &self,
        token: &str,
        tenant_id: &str,
        content_type: &str,
    ) -> Result<()> {
        let mgmt_base = self
            .config
            .management_url_override
            .as_deref()
            .unwrap_or("https://manage.office.com");
        let url = format!(
            "{mgmt_base}/api/v1.0/{tenant_id}/activity/feed/subscriptions/start?contentType={content_type}"
        );
        let resp = self.client.post(&url).bearer_auth(token).send().await;
        match resp {
            Ok(r) if r.status().is_success() => {
                info!(content_type = content_type, "M365 subscription started");
            }
            Ok(r) => {
                let status = r.status();
                warn!(content_type = content_type, status = %status, "Failed to start M365 subscription");
            }
            Err(e) => {
                warn!(error = %e, "M365 subscription start request failed");
            }
        }
        Ok(())
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

    async fn fetch(&self, _window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        info!(services = self.config.services.len(), "Fetching M365 data");

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
                    "audit_log" => {
                        Box::pin(
                            async move { (&*service.name, self.fetch_audit_log(service).await) },
                        )
                    }
                    "message_trace" => Box::pin(async move {
                        (&*service.name, self.fetch_message_trace(service).await)
                    }),
                    "dlp" => {
                        Box::pin(async move { (&*service.name, self.fetch_dlp(service).await) })
                    }
                    "alerts" => {
                        Box::pin(async move { (&*service.name, self.fetch_alerts(service).await) })
                    }
                    other => {
                        warn!(service = other, "Unknown M365 service, skipping");
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
                Err(e) => {
                    warn!(error = %e, service = name, "M365 service fetch failed, continuing");
                }
            }
        }
        Ok(results)
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        match self.graph_token_manager().await {
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

impl M365Source {
    async fn fetch_audit_log(
        &self,
        _service: &crate::config::M365Service,
    ) -> Result<Option<FetchResult>> {
        let tm = self.management_token_manager().await?;
        let token = tm.get_token().await?;
        let tenant_id = self.config.tenant_id.as_deref().unwrap_or_default();

        let mgmt_base = self
            .config
            .management_url_override
            .as_deref()
            .unwrap_or("https://manage.office.com");
        let list_url = format!(
            "{mgmt_base}/api/v1.0/{tenant_id}/activity/feed/subscriptions/content?contentType=Audit.General"
        );

        let resp = self
            .client
            .get(&list_url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| Error::Source(format!("M365 audit log list failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            if status.as_u16() == 404 {
                debug!("M365 audit log subscription not active, attempting to start");
                self.start_subscription(&token, tenant_id, "Audit.General")
                    .await?;
                return Ok(None);
            }
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "M365 audit log API returned {status}: {body}"
            )));
        }

        let content_items: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
        if content_items.is_empty() {
            return Ok(None);
        }

        // Fetch content blobs concurrently (bounded by content_items count)
        let content_futures: Vec<_> = content_items
            .iter()
            .filter_map(|item| {
                item["contentUri"].as_str().map(|uri| {
                    let client = self.client.clone();
                    let token = token.clone();
                    let uri = uri.to_string();
                    async move {
                        let resp = client.get(&uri).bearer_auth(&token).send().await;
                        match resp {
                            Ok(r) if r.status().is_success() => {
                                let events: Vec<serde_json::Value> =
                                    r.json().await.unwrap_or_default();
                                events
                                    .into_iter()
                                    .filter_map(|event| serde_json::to_vec(&event).ok())
                                    .map(Bytes::from)
                                    .collect::<Vec<_>>()
                            }
                            _ => Vec::new(),
                        }
                    }
                })
            })
            .collect();

        let results = futures::future::join_all(content_futures).await;
        let records: Vec<Bytes> = results.into_iter().flatten().collect();

        if records.is_empty() {
            return Ok(None);
        }

        info!(records = records.len(), "M365 audit log fetched");
        Ok(Some(FetchResult {
            records,
            source: "m365.audit_log".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_message_trace(
        &self,
        _service: &crate::config::M365Service,
    ) -> Result<Option<FetchResult>> {
        let tm = self.graph_token_manager().await?;
        let token = tm.get_token().await?;

        let graph_base = self
            .config
            .graph_url_override
            .as_deref()
            .unwrap_or("https://graph.microsoft.com");
        let url = format!("{graph_base}/v1.0/reports/getEmailActivityCounts(period='D1')");
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| Error::Source(format!("M365 message trace failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "M365 message trace returned {status}: {body}"
            )));
        }

        let body = resp.bytes().await.unwrap_or_default();
        if body.is_empty() {
            return Ok(None);
        }

        info!(bytes = body.len(), "M365 message trace fetched");
        Ok(Some(FetchResult {
            records: vec![Bytes::from(body.to_vec())],
            source: "m365.message_trace".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_dlp(
        &self,
        _service: &crate::config::M365Service,
    ) -> Result<Option<FetchResult>> {
        let tm = self.graph_token_manager().await?;
        let token = tm.get_token().await?;

        let graph_base = self
            .config
            .graph_url_override
            .as_deref()
            .unwrap_or("https://graph.microsoft.com");
        let url = format!(
            "{graph_base}/v1.0/security/alerts_v2?$filter=category eq 'DataLossPrevention'&$top=100"
        );
        let items = self.fetch_paginated(&token, &url, 10).await?;
        if items.is_empty() {
            return Ok(None);
        }

        let records: Vec<Bytes> = items
            .into_iter()
            .filter_map(|item| serde_json::to_vec(&item).ok().map(Bytes::from))
            .collect();

        info!(records = records.len(), "M365 DLP alerts fetched");
        Ok(Some(FetchResult {
            records,
            source: "m365.dlp".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_alerts(
        &self,
        _service: &crate::config::M365Service,
    ) -> Result<Option<FetchResult>> {
        let tm = self.graph_token_manager().await?;
        let token = tm.get_token().await?;

        let graph_base = self
            .config
            .graph_url_override
            .as_deref()
            .unwrap_or("https://graph.microsoft.com");
        let url =
            format!("{graph_base}/v1.0/security/alerts_v2?$top=100&$orderby=createdDateTime desc");
        let items = self.fetch_paginated(&token, &url, 10).await?;
        if items.is_empty() {
            return Ok(None);
        }

        let records: Vec<Bytes> = items
            .into_iter()
            .filter_map(|item| serde_json::to_vec(&item).ok().map(Bytes::from))
            .collect();

        info!(records = records.len(), "M365 security alerts fetched");
        Ok(Some(FetchResult {
            records,
            source: "m365.alerts".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}
