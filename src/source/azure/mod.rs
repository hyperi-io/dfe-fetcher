// Project:   dfe-fetcher
// File:      src/source/azure/mod.rs
// Purpose:   Azure data source (Activity Log, Defender, Sentinel, Entra ID)
// Language:  Rust
//
// License:   BUSL-1.1
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
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use crate::config::AzureSourceConfig;
use crate::credential::{self, TokenManager};
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source, SourceMaturity};

/// Microsoft Graph audit-endpoint metadata.
///
/// Drives the Entra ID split: each endpoint has a distinct path and a distinct
/// OData time-filter field. Declarative table lets one helper iterate every
/// endpoint without per-endpoint code duplication.
struct GraphAuditEndpoint {
    /// Path under `/v1.0/`.
    path: &'static str,
    /// OData filter field for time-windowing.
    time_field: &'static str,
    /// Service config name; also used as the source suffix
    /// (becomes `azure.<service_name>`).
    service_name: &'static str,
}

/// The three Entra ID Graph audit endpoints exposed as distinct services.
const ENTRA_ENDPOINTS: &[GraphAuditEndpoint] = &[
    GraphAuditEndpoint {
        path: "auditLogs/signIns",
        time_field: "createdDateTime",
        service_name: "entra_signins",
    },
    GraphAuditEndpoint {
        path: "auditLogs/directoryAudits",
        time_field: "activityDateTime",
        service_name: "entra_directory_audits",
    },
    GraphAuditEndpoint {
        path: "auditLogs/provisioning",
        time_field: "activityDateTime",
        service_name: "entra_provisioning",
    },
];

/// Names of the per-endpoint Entra services (for dispatch matching).
const ENTRA_SPLIT_SERVICES: &[&str] = &[
    "entra_signins",
    "entra_directory_audits",
    "entra_provisioning",
];

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

        let token_url = self.config.token_url_override.clone().unwrap_or_else(|| {
            format!("https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token")
        });
        Ok(TokenManager::new(
            self.client.clone(),
            token_url,
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
            .as_ref()
            .map(|s| s.expose().to_string())
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

            if let Some(items) = body["value"].as_array().cloned() {
                all_items.extend(items);
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

    fn maturity(&self) -> SourceMaturity {
        SourceMaturity::Stable
    }

    async fn fetch(&self, window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        info!(services = self.config.services.len(), "Fetching Azure data");

        let now = Utc::now();
        let (start, end) = match window {
            Some(w) => (w.start, w.end),
            None => (now - chrono::Duration::hours(24), now),
        };

        // Concurrent service fetching via join_all (no spawn, borrows &self)
        let service_futures: Vec<_> =
            self.config
                .services
                .iter()
                .filter_map(|service| {
                    let fut: std::pin::Pin<
                        Box<
                            dyn std::future::Future<Output = (&str, Result<Option<FetchResult>>)>
                                + Send
                                + '_,
                        >,
                    > =
                        match service.name.as_str() {
                            "activity_log" => Box::pin(async move {
                                (
                                    &*service.name,
                                    self.fetch_activity_log(service, start, end).await,
                                )
                            }),
                            "defender" => Box::pin(async move {
                                (&*service.name, self.fetch_defender(service).await)
                            }),
                            "sentinel" => Box::pin(async move {
                                (&*service.name, self.fetch_sentinel(service).await)
                            }),
                            // Per-endpoint Entra services. Each emits its own FetchResult
                            // tagged `azure.entra_signins` / `azure.entra_directory_audits`
                            // / `azure.entra_provisioning`. There is intentionally no
                            // combined `entra_id` service - consumers configure each
                            // feed they want, with its own cursor and source tag.
                            name if ENTRA_SPLIT_SERVICES.contains(&name) => Box::pin(async move {
                                (&*service.name, self.fetch_entra_one(name, start, end).await)
                            }),
                            "log_analytics" => Box::pin(async move {
                                (
                                    &*service.name,
                                    self.fetch_log_analytics(service, start, end).await,
                                )
                            }),
                            other => {
                                warn!(service = other, "Unknown Azure service, skipping");
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
                    warn!(error = %e, service = name, "Azure service fetch failed, continuing");
                }
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

    fn cursor_prefix(&self) -> String {
        "azure".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        self.config
            .services
            .iter()
            .map(|s| s.name.as_str())
            .collect()
    }
}

impl AzureSource {
    async fn fetch_activity_log(
        &self,
        _service: &crate::config::AzureService,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        let tm = self.management_token_manager().await?;
        let token = tm.get_token().await?;

        let subscription_id = self.config.subscription_id.as_deref().ok_or_else(|| {
            Error::Config("azure.subscription_id is required for activity_log".into())
        })?;

        // Azure Activity Log requires ISO 8601 without sub-second precision
        let fmt = "%Y-%m-%dT%H:%M:%SZ";
        let filter = format!(
            "eventTimestamp ge '{}' and eventTimestamp le '{}'",
            start.format(fmt),
            end.format(fmt)
        );

        let mgmt_base = self
            .config
            .management_url_override
            .as_deref()
            .unwrap_or("https://management.azure.com");
        let url = format!(
            "{mgmt_base}/subscriptions/{subscription_id}/providers/microsoft.insights/eventtypes/management/values?api-version=2015-04-01&$filter={filter}"
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

        let mgmt_base = self
            .config
            .management_url_override
            .as_deref()
            .unwrap_or("https://management.azure.com");
        let url = format!(
            "{mgmt_base}/subscriptions/{subscription_id}/providers/Microsoft.Security/alerts?api-version=2022-01-01"
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

        let mgmt_base = self
            .config
            .management_url_override
            .as_deref()
            .unwrap_or("https://management.azure.com");
        let url = format!(
            "{mgmt_base}/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.OperationalInsights/workspaces/{workspace}/providers/Microsoft.SecurityInsights/incidents?api-version=2023-11-01"
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

    /// Fetch a single Entra Graph audit endpoint with OData time-window filter.
    ///
    /// Returns `Ok(None)` when the endpoint has no records in the window.
    /// Tags the result `azure.<service_name>` (e.g. `azure.entra_signins`).
    async fn fetch_entra_endpoint(
        &self,
        token: &str,
        endpoint: &GraphAuditEndpoint,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        let graph_base = self
            .config
            .graph_url_override
            .as_deref()
            .unwrap_or("https://graph.microsoft.com");
        let start_iso = start.format("%Y-%m-%dT%H:%M:%SZ");
        let end_iso = end.format("%Y-%m-%dT%H:%M:%SZ");
        // OData filter; Graph accepts unencoded spaces in $filter clauses.
        let url = format!(
            "{graph_base}/v1.0/{}?$top=100&$filter={} ge {} and {} lt {}",
            endpoint.path, endpoint.time_field, start_iso, endpoint.time_field, end_iso,
        );

        let items = self.fetch_paginated(token, &url, 10).await?;
        if items.is_empty() {
            return Ok(None);
        }

        let records: Vec<Bytes> = items
            .into_iter()
            .filter_map(|i| serde_json::to_vec(&i).ok().map(Bytes::from))
            .collect();

        if records.is_empty() {
            return Ok(None);
        }

        info!(
            records = records.len(),
            endpoint = endpoint.path,
            "Azure Entra endpoint fetched"
        );
        Ok(Some(FetchResult {
            records,
            source: format!("azure.{}", endpoint.service_name),
            topic: self.config.topic.clone(),
        }))
    }

    /// Dispatch a single Entra split service (`entra_signins`,
    /// `entra_directory_audits`, `entra_provisioning`) to its endpoint.
    async fn fetch_entra_one(
        &self,
        service_name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        let endpoint = ENTRA_ENDPOINTS
            .iter()
            .find(|e| e.service_name == service_name)
            .ok_or_else(|| Error::Source(format!("unknown Entra split service: {service_name}")))?;
        let tm = self.graph_token_manager().await?;
        let token = tm.get_token().await?;
        self.fetch_entra_endpoint(&token, endpoint, start, end)
            .await
    }

    /// Build an Azure Management API token manager (different audience to
    /// Graph - used for Log Analytics queries against
    /// `api.loganalytics.azure.com`).
    async fn log_analytics_token_manager(&self) -> Result<TokenManager> {
        let tenant_id = self
            .config
            .tenant_id
            .as_deref()
            .ok_or_else(|| Error::Credential("azure.tenant_id is required".into()))?;
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
            "https://api.loganalytics.io/.default".to_string(),
        ))
    }

    /// Run a KQL query against a Log Analytics workspace.
    ///
    /// Service config requires:
    /// - `workspace_id`: the workspace's GUID (NOT the customer-facing name).
    /// - `kql`: the KQL query string. The fetcher injects a server-side
    ///   timespan via the `timespan` query param, so the KQL itself should
    ///   NOT include `where TimeGenerated ...`. Use the timespan instead.
    ///
    /// Returns one record per row, each tagged with the column names from
    /// the result table.
    async fn fetch_log_analytics(
        &self,
        service: &crate::config::AzureService,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        let workspace_id = service
            .config
            .get("workspace_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::Config(
                    "azure.log_analytics requires service config `workspace_id` (GUID)".into(),
                )
            })?;
        let kql = service
            .config
            .get("kql")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::Config("azure.log_analytics requires service config `kql`".into())
            })?;

        let tm = self.log_analytics_token_manager().await?;
        let token = tm.get_token().await?;

        // ISO 8601 timespan: `start/end`. Log Analytics requires exact form.
        let timespan = format!(
            "{}/{}",
            start.format("%Y-%m-%dT%H:%M:%SZ"),
            end.format("%Y-%m-%dT%H:%M:%SZ"),
        );

        let api_base = "https://api.loganalytics.io";
        let url = format!("{api_base}/v1/workspaces/{workspace_id}/query");
        let body = serde_json::json!({
            "query": kql,
            "timespan": timespan,
        });

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::Source(format!("Log Analytics query failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "Log Analytics returned {status}: {body_text}"
            )));
        }

        let envelope: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| Error::Source(format!("Log Analytics parse failed: {e}")))?;

        // Response shape: { "tables": [{ "name": ..., "columns": [{ "name": ..., "type": ... }, ...], "rows": [[...], ...] }] }
        // Convert each row into a JSON object keyed by column name.
        let mut records: Vec<Bytes> = Vec::new();
        if let Some(tables) = envelope["tables"].as_array() {
            for table in tables {
                let columns: Vec<String> = table["columns"]
                    .as_array()
                    .map(|cols| {
                        cols.iter()
                            .filter_map(|c| c["name"].as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                if let Some(rows) = table["rows"].as_array() {
                    for row in rows {
                        if let Some(row_arr) = row.as_array() {
                            let obj: serde_json::Map<String, serde_json::Value> = columns
                                .iter()
                                .zip(row_arr.iter())
                                .map(|(c, v)| (c.clone(), v.clone()))
                                .collect();
                            let json = serde_json::Value::Object(obj);
                            if let Ok(buf) = serde_json::to_vec(&json) {
                                records.push(Bytes::from(buf));
                            }
                        }
                    }
                }
            }
        }

        if records.is_empty() {
            return Ok(None);
        }

        info!(
            records = records.len(),
            "Azure Log Analytics query result fetched"
        );
        Ok(Some(FetchResult {
            records,
            source: "azure.log_analytics".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entra_endpoints_cover_three_distinct_paths() {
        let paths: Vec<&str> = ENTRA_ENDPOINTS.iter().map(|e| e.path).collect();
        assert_eq!(paths.len(), 3);
        assert!(paths.contains(&"auditLogs/signIns"));
        assert!(paths.contains(&"auditLogs/directoryAudits"));
        assert!(paths.contains(&"auditLogs/provisioning"));
    }

    #[test]
    fn entra_endpoints_use_correct_time_field_per_path() {
        for ep in ENTRA_ENDPOINTS {
            let expected = match ep.path {
                "auditLogs/signIns" => "createdDateTime",
                "auditLogs/directoryAudits" => "activityDateTime",
                "auditLogs/provisioning" => "activityDateTime",
                other => panic!("unexpected Entra path {other}"),
            };
            assert_eq!(ep.time_field, expected, "wrong time_field for {}", ep.path);
        }
    }

    #[test]
    fn entra_split_services_match_endpoints() {
        let endpoint_names: std::collections::HashSet<&str> =
            ENTRA_ENDPOINTS.iter().map(|e| e.service_name).collect();
        let split_names: std::collections::HashSet<&str> =
            ENTRA_SPLIT_SERVICES.iter().copied().collect();
        assert_eq!(
            endpoint_names, split_names,
            "ENTRA_ENDPOINTS and ENTRA_SPLIT_SERVICES must agree on service names"
        );
    }
}
