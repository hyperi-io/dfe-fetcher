// Project:   dfe-fetcher
// File:      src/source/crates_io/mod.rs
// Purpose:   crates.io supply-chain audit source
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! crates.io supply-chain audit source.
//!
//! For each configured crate, fetches the metadata document from
//! `https://crates.io/api/v1/crates/<name>` and emits one record per crate.
//! The metadata includes versions, owners, yanks, downloads, dependency
//! summary - enough for downstream tooling to spot unexpected publishing
//! or ownership changes.
//!
//! No authentication required, but crates.io asks consumers to set a
//! contact User-Agent header.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::config::CratesIoSourceConfig;
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// User-Agent header used for crates.io requests. crates.io's API policy
/// asks consumers to identify themselves with a contact URL or email.
const USER_AGENT: &str = "dfe-fetcher (https://github.com/hyperi-io/dfe-fetcher)";

/// crates.io supply-chain audit data source.
pub struct CratesIoSource {
    config: CratesIoSourceConfig,
    client: reqwest::Client,
}

impl CratesIoSource {
    pub fn new(config: CratesIoSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    fn api_base(&self) -> &str {
        self.config
            .api_url_override
            .as_deref()
            .unwrap_or("https://crates.io")
    }

    async fn fetch_crate(&self, crate_name: &str) -> Result<Option<Bytes>> {
        let url = format!("{}/api/v1/crates/{crate_name}", self.api_base());
        let resp = self
            .client
            .get(&url)
            .header("Accept", "application/json")
            .header("User-Agent", USER_AGENT)
            .send()
            .await
            .map_err(|e| {
                Error::Source(format!("crates.io request for {crate_name} failed: {e}"))
            })?;

        let status = resp.status();
        if status.as_u16() == 404 {
            warn!(crate_name, "crates.io returned 404 - crate missing");
            return Ok(None);
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "crates.io returned {status} for {crate_name}: {body}"
            )));
        }

        let mut doc: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| Error::Source(format!("crates.io {crate_name} parse failed: {e}")))?;

        if let Some(obj) = doc.as_object_mut() {
            obj.insert(
                "_dfe_fetcher_crate".to_string(),
                serde_json::Value::String(crate_name.to_string()),
            );
        }

        let buf = serde_json::to_vec(&doc).map_err(|e| {
            Error::Source(format!("crates.io {crate_name} re-serialise failed: {e}"))
        })?;
        Ok(Some(Bytes::from(buf)))
    }
}

#[async_trait]
impl Source for CratesIoSource {
    fn name(&self) -> &'static str {
        "crates_io"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn fetch(&self, _window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled || self.config.crates.is_empty() {
            return Ok(vec![]);
        }

        info!(
            crates = self.config.crates.len(),
            "Fetching crates.io metadata"
        );

        let mut records: Vec<Bytes> = Vec::with_capacity(self.config.crates.len());
        for crate_name in &self.config.crates {
            match self.fetch_crate(crate_name).await {
                Ok(Some(b)) => {
                    records.push(b);
                    debug!(crate_name, "crates.io metadata fetched");
                }
                Ok(None) => {}
                Err(e) => warn!(error = %e, crate_name, "crates.io fetch failed, continuing"),
            }
        }

        if records.is_empty() {
            return Ok(vec![]);
        }

        info!(records = records.len(), "crates.io metadata fetched");
        Ok(vec![FetchResult {
            records,
            source: "crates_io.metadata".to_string(),
            topic: self.config.topic.clone(),
        }])
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        let url = format!("{}/api/v1/summary", self.api_base());
        let resp = match self
            .client
            .get(&url)
            .header("User-Agent", USER_AGENT)
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => return Ok(false),
        };
        Ok(resp.status().is_success())
    }

    fn cursor_prefix(&self) -> String {
        "crates_io".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_base_default_when_no_override() {
        let src = CratesIoSource::new(CratesIoSourceConfig::default());
        assert_eq!(src.api_base(), "https://crates.io");
    }

    #[test]
    fn api_base_uses_override_when_present() {
        let cfg = CratesIoSourceConfig {
            api_url_override: Some("http://localhost:9999".into()),
            ..CratesIoSourceConfig::default()
        };
        let src = CratesIoSource::new(cfg);
        assert_eq!(src.api_base(), "http://localhost:9999");
    }

    #[test]
    fn user_agent_is_polite() {
        // crates.io asks consumers to identify themselves; verify we do.
        assert!(USER_AGENT.contains("dfe-fetcher"));
        assert!(USER_AGENT.contains("https://github.com/"));
    }
}
