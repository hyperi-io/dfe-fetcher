// Project:   dfe-fetcher
// File:      src/source/pypi/mod.rs
// Purpose:   PyPI supply-chain audit source
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! PyPI supply-chain audit source.
//!
//! For each configured package, fetches the full metadata document from
//! `https://pypi.org/pypi/<package>/json` and emits one record. Downstream
//! tooling computes deltas across ticks to spot unexpected version
//! publications, file changes, maintainer additions, etc.
//!
//! No authentication required.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::config::PypiSourceConfig;
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// PyPI supply-chain audit data source.
pub struct PypiSource {
    config: PypiSourceConfig,
    client: reqwest::Client,
}

impl PypiSource {
    pub fn new(config: PypiSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    fn api_base(&self) -> &str {
        self.config
            .api_url_override
            .as_deref()
            .unwrap_or("https://pypi.org")
    }

    /// Fetch metadata for one package. Returns the raw JSON document with
    /// a `_dfe_fetcher_package` field injected for downstream filtering.
    async fn fetch_package(&self, package: &str) -> Result<Option<Bytes>> {
        let url = format!("{}/pypi/{package}/json", self.api_base());
        let resp = self
            .client
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| Error::Source(format!("PyPI request for {package} failed: {e}")))?;

        let status = resp.status();
        if status.as_u16() == 404 {
            // Package doesn't exist (or was deleted). Emit nothing rather
            // than fail the whole tick - other configured packages should
            // still be fetched.
            warn!(package, "PyPI returned 404 - package missing or renamed");
            return Ok(None);
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "PyPI returned {status} for {package}: {body}"
            )));
        }

        let mut doc: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| Error::Source(format!("PyPI {package} parse failed: {e}")))?;

        // Inject the package name we queried so consumers can filter/route
        // without needing to walk into `info.name`.
        if let Some(obj) = doc.as_object_mut() {
            obj.insert(
                "_dfe_fetcher_package".to_string(),
                serde_json::Value::String(package.to_string()),
            );
        }

        let buf = serde_json::to_vec(&doc)
            .map_err(|e| Error::Source(format!("PyPI {package} re-serialise failed: {e}")))?;
        Ok(Some(Bytes::from(buf)))
    }
}

#[async_trait]
impl Source for PypiSource {
    fn name(&self) -> &'static str {
        "pypi"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn fetch(&self, _window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled || self.config.packages.is_empty() {
            return Ok(vec![]);
        }

        info!(
            packages = self.config.packages.len(),
            "Fetching PyPI package metadata"
        );

        let mut records: Vec<Bytes> = Vec::with_capacity(self.config.packages.len());
        for package in &self.config.packages {
            match self.fetch_package(package).await {
                Ok(Some(b)) => {
                    records.push(b);
                    debug!(package, "PyPI metadata fetched");
                }
                Ok(None) => {}
                Err(e) => warn!(error = %e, package, "PyPI fetch failed, continuing"),
            }
        }

        if records.is_empty() {
            return Ok(vec![]);
        }

        info!(records = records.len(), "PyPI metadata fetched");
        Ok(vec![FetchResult {
            records,
            source: "pypi.metadata".to_string(),
            topic: self.config.topic.clone(),
        }])
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        // pypi.org is a public service; success on GET / verifies reachability.
        let url = format!("{}/", self.api_base());
        let resp = match self.client.get(&url).send().await {
            Ok(r) => r,
            Err(_) => return Ok(false),
        };
        Ok(resp.status().is_success())
    }

    fn cursor_prefix(&self) -> String {
        "pypi".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_base_default_when_no_override() {
        let src = PypiSource::new(PypiSourceConfig::default());
        assert_eq!(src.api_base(), "https://pypi.org");
    }

    #[test]
    fn api_base_uses_override_when_present() {
        let cfg = PypiSourceConfig {
            api_url_override: Some("http://localhost:9999".into()),
            ..PypiSourceConfig::default()
        };
        let src = PypiSource::new(cfg);
        assert_eq!(src.api_base(), "http://localhost:9999");
    }
}
