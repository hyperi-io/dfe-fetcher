// Project:   dfe-fetcher
// File:      src/source/go_modules/mod.rs
// Purpose:   Go module-proxy supply-chain audit source
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Go module-proxy supply-chain audit source.
//!
//! For each configured Go module path, lists known versions via
//! `<proxy>/<module>/@v/list` and fetches per-version `.info` documents.
//! Emits one record per module per tick containing the version list and
//! per-version metadata. Downstream tooling computes deltas across ticks
//! to spot unexpected publications or commit-hash drift.
//!
//! The default proxy is Google's at `https://proxy.golang.org` -
//! GOPROXY-compatible, no authentication. Internal mirrors can be
//! substituted via `api_url_override`.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::config::GoModulesSourceConfig;
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Cap on per-version info fetches per module per tick. Most HyperI modules
/// will have far fewer published versions than this; the cap guards against
/// unbounded calls if a module ever has a runaway version count.
const MAX_VERSIONS_PER_MODULE: usize = 100;

/// Go module-proxy supply-chain audit data source.
pub struct GoModulesSource {
    config: GoModulesSourceConfig,
    client: reqwest::Client,
}

impl GoModulesSource {
    pub fn new(config: GoModulesSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    fn api_base(&self) -> &str {
        self.config
            .api_url_override
            .as_deref()
            .unwrap_or("https://proxy.golang.org")
    }

    /// Fetch the version list for one module (plain-text response, one
    /// version per line).
    async fn fetch_version_list(&self, module: &str) -> Result<Vec<String>> {
        let url = format!("{}/{module}/@v/list", self.api_base());
        let resp = self.client.get(&url).send().await.map_err(|e| {
            Error::Source(format!("Go proxy list request for {module} failed: {e}"))
        })?;
        let status = resp.status();
        if status.as_u16() == 404 {
            warn!(module, "Go proxy returned 404 - module not yet published");
            return Ok(Vec::new());
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "Go proxy returned {status} for {module} list: {body}"
            )));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Source(format!("Go proxy {module} list read failed: {e}")))?;
        Ok(body
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(String::from)
            .collect())
    }

    /// Fetch the per-version info document. Returns `null` on 404 (version
    /// retracted or transient proxy gap).
    async fn fetch_version_info(&self, module: &str, version: &str) -> Result<serde_json::Value> {
        let url = format!("{}/{module}/@v/{version}.info", self.api_base());
        let resp = self
            .client
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| {
                Error::Source(format!(
                    "Go proxy info request for {module}@{version} failed: {e}"
                ))
            })?;
        let status = resp.status();
        if status.as_u16() == 404 {
            return Ok(serde_json::Value::Null);
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "Go proxy returned {status} for {module}@{version} info: {body}"
            )));
        }
        resp.json().await.map_err(|e| {
            Error::Source(format!(
                "Go proxy {module}@{version} info parse failed: {e}"
            ))
        })
    }

    /// Build the single per-module record: version list + per-version info,
    /// plus a `_dfe_fetcher_module` field.
    async fn build_module_record(&self, module: &str) -> Result<Option<Bytes>> {
        let versions = self.fetch_version_list(module).await?;
        if versions.is_empty() {
            return Ok(None);
        }

        let mut infos = serde_json::Map::with_capacity(versions.len());
        for version in versions.iter().take(MAX_VERSIONS_PER_MODULE) {
            match self.fetch_version_info(module, version).await {
                Ok(v) => {
                    infos.insert(version.clone(), v);
                }
                Err(e) => {
                    warn!(error = %e, module, version,
                        "Go proxy version info failed, continuing");
                }
            }
        }

        let doc = serde_json::json!({
            "_dfe_fetcher_module": module,
            "versions": versions,
            "version_info": serde_json::Value::Object(infos),
        });
        let buf = serde_json::to_vec(&doc)
            .map_err(|e| Error::Source(format!("Go proxy {module} record build failed: {e}")))?;
        Ok(Some(Bytes::from(buf)))
    }
}

#[async_trait]
impl Source for GoModulesSource {
    fn name(&self) -> &'static str {
        "go_modules"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn fetch(&self, _window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled || self.config.modules.is_empty() {
            return Ok(vec![]);
        }

        info!(
            modules = self.config.modules.len(),
            "Fetching Go module metadata"
        );

        let mut records: Vec<Bytes> = Vec::with_capacity(self.config.modules.len());
        for module in &self.config.modules {
            match self.build_module_record(module).await {
                Ok(Some(b)) => {
                    records.push(b);
                    debug!(module, "Go module metadata fetched");
                }
                Ok(None) => {}
                Err(e) => warn!(error = %e, module, "Go module fetch failed, continuing"),
            }
        }

        if records.is_empty() {
            return Ok(vec![]);
        }

        info!(records = records.len(), "Go module metadata fetched");
        Ok(vec![FetchResult {
            records,
            source: "go_modules.metadata".to_string(),
            topic: self.config.topic.clone(),
        }])
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        // The proxy returns 200 + a Go HTTP-204-like response on root. The
        // safest probe is a request for a well-known no-op module - but we
        // don't want to depend on the existence of any specific module. The
        // root URL responds 200 to a GET in practice.
        let url = self.api_base().to_string();
        let resp = match self.client.get(&url).send().await {
            Ok(r) => r,
            Err(_) => return Ok(false),
        };
        // Treat any non-5xx as reachability success - 404 / 200 both mean
        // the proxy answered.
        Ok(!resp.status().is_server_error())
    }

    fn cursor_prefix(&self) -> String {
        "go_modules".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_base_default_when_no_override() {
        let src = GoModulesSource::new(GoModulesSourceConfig::default());
        assert_eq!(src.api_base(), "https://proxy.golang.org");
    }

    #[test]
    fn api_base_uses_override_when_present() {
        let cfg = GoModulesSourceConfig {
            api_url_override: Some("https://goproxy.internal".into()),
            ..GoModulesSourceConfig::default()
        };
        let src = GoModulesSource::new(cfg);
        assert_eq!(src.api_base(), "https://goproxy.internal");
    }
}
