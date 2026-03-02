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
use tracing::info;

use crate::config::M365SourceConfig;
use crate::error::Result;
use crate::source::{FetchResult, Source};

/// Microsoft 365 data source implementation.
pub struct M365Source {
    config: M365SourceConfig,
}

impl M365Source {
    /// Create a new M365 source from configuration.
    pub fn new(config: M365SourceConfig) -> Self {
        Self { config }
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

    async fn fetch(&self) -> Result<Vec<FetchResult>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        info!(
            services = self.config.services.len(),
            "Fetching M365 data"
        );

        let mut results = Vec::new();

        for service in &self.config.services {
            let fetch_result = match service.name.as_str() {
                "audit_log" => self.fetch_audit_log(service).await?,
                "message_trace" => self.fetch_message_trace(service).await?,
                "dlp" => self.fetch_dlp(service).await?,
                "alerts" => self.fetch_alerts(service).await?,
                other => {
                    tracing::warn!(service = other, "Unknown M365 service, skipping");
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
        // TODO: Validate credentials with token acquisition
        Ok(self.config.enabled)
    }
}

impl M365Source {
    async fn fetch_audit_log(
        &self,
        _service: &crate::config::M365Service,
    ) -> Result<Option<FetchResult>> {
        // TODO: Implement Office 365 Management Activity API
        info!("M365 Audit Log fetch - not yet implemented");
        Ok(None)
    }

    async fn fetch_message_trace(
        &self,
        _service: &crate::config::M365Service,
    ) -> Result<Option<FetchResult>> {
        // TODO: Implement Exchange Message Trace API
        info!("M365 Message Trace fetch - not yet implemented");
        Ok(None)
    }

    async fn fetch_dlp(
        &self,
        _service: &crate::config::M365Service,
    ) -> Result<Option<FetchResult>> {
        // TODO: Implement M365 DLP policy matches API
        info!("M365 DLP fetch - not yet implemented");
        Ok(None)
    }

    async fn fetch_alerts(
        &self,
        _service: &crate::config::M365Service,
    ) -> Result<Option<FetchResult>> {
        // TODO: Implement Microsoft Graph Security Alerts API
        info!("M365 Alerts fetch - not yet implemented");
        Ok(None)
    }
}
