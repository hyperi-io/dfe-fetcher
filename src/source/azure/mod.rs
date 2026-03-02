// Project:   dfe-fetcher
// File:      src/source/azure/mod.rs
// Purpose:   Azure data source (Activity Log, Defender, Sentinel, Entra ID)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Azure data source.
//!
//! Fetches security and operational data from Azure services:
//! - Activity Log (audit trail)
//! - Defender for Cloud (security findings)
//! - Sentinel (SIEM alerts)
//! - Entra ID / Azure AD (identity events)
//!
//! Authentication: Client credentials (OAuth2 client_credentials grant).

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::config::AzureSourceConfig;
use crate::credential::{self, TokenManager};
use crate::error::{Error, Result};
use crate::source::{FetchResult, Source};

/// Azure data source implementation.
pub struct AzureSource {
    config: AzureSourceConfig,
    client: reqwest::Client,
}

impl AzureSource {
    /// Create a new Azure source from configuration.
    pub fn new(config: AzureSourceConfig) -> Self {
        let client = credential::http_client().unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    /// Build token manager for Azure Management API.
    async fn management_token_manager(&self) -> Result<TokenManager> {
        let tenant_id = self
            .config
            .tenant_id
            .as_deref()
            .ok_or_else(|| Error::Credential("azure.tenant_id is required".into()))?;
        let client_id = self.resolve_client_id().await?;
        let client_secret = self.resolve_client_secret().await?;

        Ok(TokenManager::microsoft(
            self.client.clone(),
            tenant_id,
            client_id,
            client_secret,
            "https://management.azure.com/.default".to_string(),
        ))
    }

    /// Build token manager for Microsoft Graph API.
    async fn graph_token_manager(&self) -> Result<TokenManager> {
        let tenant_id = self
            .config
            .tenant_id
            .as_deref()
            .ok_or_else(|| Error::Credential("azure.tenant_id is required".into()))?;
        let client_id = self.resolve_client_id().await?;
        let client_secret = self.resolve_client_secret().await?;

        Ok(TokenManager::microsoft(
            self.client.clone(),
            tenant_id,
            client_id,
            client_secret,
            "https://graph.microsoft.com/.default".to_string(),
        ))
    }

    async fn resolve_client_id(&self) -> Result<String> {
        if let Some(ref spec) = self.config.credential_secret {
            return credential::resolve(spec).await;
        }
        self.config
            .client_id
            .clone()
            .ok_or_else(|| Error::Credential("azure.client_id is required".into()))
    }

    async fn resolve_client_secret(&self) -> Result<String> {
        if let Some(ref spec) = self.config.credential_secret {
            return credential::resolve(spec).await;
        }
        self.config
            .client_secret
            .clone()
            .ok_or_else(|| Error::Credential("azure.client_secret is required".into()))
    }

    /// Fetch paginated results following `@odata.nextLink` or `nextLink`.
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
                .map_err(|e| Error::Source(format!("Azure API request failed: {e}")))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "Azure API returned {status}: {body}"
                )));
            }

            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("failed to parse Azure response: {e}")))?;

            if let Some(items) = body["value"].as_array() {
                all_items.extend(items.iter().cloned());
            }

            let next_link = body["@odata.nextLink"]
                .as_str()
                .or_else(|| body["nextLink"].as_str());

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
}

#[async_trait]
impl Source for AzureSource {
    fn name(&self) -> &'static str {
        "azure"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn fetch(&self) -> Result<Vec<FetchResult>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        info!(services = self.config.services.len(), "Fetching Azure data");

        let mut results = Vec::new();
        for service in &self.config.services {
            let fetch_result = match service.name.as_str() {
                "activity_log" => self.fetch_activity_log(service).await?,
                "defender" => self.fetch_defender(service).await?,
                "sentinel" => self.fetch_sentinel(service).await?,
                "entra_id" => self.fetch_entra_id(service).await?,
                other => {
                    warn!(service = other, "Unknown Azure service, skipping");
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
        match self.management_token_manager().await {
            Ok(tm) => Ok(tm.get_token().await.is_ok()),
            Err(_) => Ok(false),
        }
    }
}

impl AzureSource {
    async fn fetch_activity_log(
        &self,
        _service: &crate::config::AzureService,
    ) -> Result<Option<FetchResult>> {
        let tm = self.management_token_manager().await?;
        let token = tm.get_token().await?;

        let subscription_id = self.config.subscription_id.as_deref().ok_or_else(|| {
            Error::Config("azure.subscription_id is required for activity_log".into())
        })?;

        // Fetch last 24 hours of activity logs
        let now = chrono::Utc::now();
        let start = now - chrono::Duration::hours(24);
        let filter = format!(
            "eventTimestamp ge '{}' and eventTimestamp le '{}'",
            start.to_rfc3339(),
            now.to_rfc3339()
        );

        let url = format!(
            "https://management.azure.com/subscriptions/{subscription_id}/providers/microsoft.insights/eventtypes/management/values?api-version=2015-04-01&$filter={filter}"
        );

        let items = self.fetch_paginated(&token, &url, 10).await?;
        if items.is_empty() {
            return Ok(None);
        }

        let records: Vec<Bytes> = items
            .into_iter()
            .filter_map(|item| serde_json::to_vec(&item).ok().map(Bytes::from))
            .collect();

        info!(records = records.len(), "Azure Activity Log fetched");
        Ok(Some(FetchResult {
            records,
            source: "azure.activity_log".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_defender(
        &self,
        _service: &crate::config::AzureService,
    ) -> Result<Option<FetchResult>> {
        let tm = self.management_token_manager().await?;
        let token = tm.get_token().await?;

        let subscription_id = self.config.subscription_id.as_deref().ok_or_else(|| {
            Error::Config("azure.subscription_id is required for defender".into())
        })?;

        let url = format!(
            "https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Security/alerts?api-version=2022-01-01"
        );

        let items = self.fetch_paginated(&token, &url, 10).await?;
        if items.is_empty() {
            return Ok(None);
        }

        let records: Vec<Bytes> = items
            .into_iter()
            .filter_map(|item| serde_json::to_vec(&item).ok().map(Bytes::from))
            .collect();

        info!(records = records.len(), "Azure Defender alerts fetched");
        Ok(Some(FetchResult {
            records,
            source: "azure.defender".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_sentinel(
        &self,
        _service: &crate::config::AzureService,
    ) -> Result<Option<FetchResult>> {
        let tm = self.management_token_manager().await?;
        let token = tm.get_token().await?;

        let subscription_id = self.config.subscription_id.as_deref().ok_or_else(|| {
            Error::Config("azure.subscription_id is required for sentinel".into())
        })?;

        // Sentinel requires a resource group and workspace — use config if provided
        let resource_group = self
            .config
            .services
            .iter()
            .find(|s| s.name == "sentinel")
            .and_then(|s| s.config.get("resource_group"))
            .and_then(|v| v.as_str())
            .unwrap_or("default");

        let workspace = self
            .config
            .services
            .iter()
            .find(|s| s.name == "sentinel")
            .and_then(|s| s.config.get("workspace_name"))
            .and_then(|v| v.as_str())
            .unwrap_or("default");

        let url = format!(
            "https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.OperationalInsights/workspaces/{workspace}/providers/Microsoft.SecurityInsights/incidents?api-version=2023-11-01"
        );

        let items = self.fetch_paginated(&token, &url, 10).await?;
        if items.is_empty() {
            return Ok(None);
        }

        let records: Vec<Bytes> = items
            .into_iter()
            .filter_map(|item| serde_json::to_vec(&item).ok().map(Bytes::from))
            .collect();

        info!(records = records.len(), "Azure Sentinel incidents fetched");
        Ok(Some(FetchResult {
            records,
            source: "azure.sentinel".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_entra_id(
        &self,
        _service: &crate::config::AzureService,
    ) -> Result<Option<FetchResult>> {
        let tm = self.graph_token_manager().await?;
        let token = tm.get_token().await?;

        let mut all_records = Vec::new();

        // Fetch sign-in logs
        let signin_url = "https://graph.microsoft.com/v1.0/auditLogs/signIns?$top=100&$orderby=createdDateTime desc";
        let signins = self.fetch_paginated(&token, signin_url, 5).await?;
        for item in signins {
            if let Ok(json) = serde_json::to_vec(&item) {
                all_records.push(Bytes::from(json));
            }
        }

        // Fetch directory audit logs
        let audit_url = "https://graph.microsoft.com/v1.0/auditLogs/directoryAudits?$top=100&$orderby=activityDateTime desc";
        let audits = self.fetch_paginated(&token, audit_url, 5).await?;
        for item in audits {
            if let Ok(json) = serde_json::to_vec(&item) {
                all_records.push(Bytes::from(json));
            }
        }

        if all_records.is_empty() {
            return Ok(None);
        }

        info!(records = all_records.len(), "Azure Entra ID logs fetched");
        Ok(Some(FetchResult {
            records: all_records,
            source: "azure.entra_id".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}
