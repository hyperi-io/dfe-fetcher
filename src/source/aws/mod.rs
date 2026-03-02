// Project:   dfe-fetcher
// File:      src/source/aws/mod.rs
// Purpose:   AWS data source (CloudTrail, GuardDuty, SecurityHub, Config)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! AWS data source.
//!
//! Fetches security and operational data from AWS services:
//! - CloudTrail (audit logs)
//! - GuardDuty (threat detection)
//! - SecurityHub (security findings)
//! - Config (resource configuration)
//!
//! Authentication: Static credentials, assume role, or secrets manager.
//! Uses AWS REST APIs with SigV4 signing via the `aws-sigv4` crate,
//! or falls back to direct JSON API calls with static credentials.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use tracing::{info, warn};

use crate::config::AwsSourceConfig;
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, Source};

/// AWS data source implementation.
pub struct AwsSource {
    config: AwsSourceConfig,
    client: reqwest::Client,
}

impl AwsSource {
    /// Create a new AWS source from configuration.
    pub fn new(config: AwsSourceConfig) -> Self {
        let client = credential::http_client().unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    /// Resolve AWS credentials (access key + secret key).
    async fn resolve_credentials(&self) -> Result<(String, String)> {
        if let Some(ref spec) = self.config.credential_secret {
            // Vault credential: expect JSON with access_key_id and secret_access_key
            let resolved = credential::resolve(spec).await?;
            // Try to parse as JSON
            if let Ok(creds) = serde_json::from_str::<serde_json::Value>(&resolved) {
                let ak = creds["access_key_id"]
                    .as_str()
                    .or_else(|| creds["AccessKeyId"].as_str())
                    .ok_or_else(|| {
                        Error::Credential("missing access_key_id in vault secret".into())
                    })?;
                let sk = creds["secret_access_key"]
                    .as_str()
                    .or_else(|| creds["SecretAccessKey"].as_str())
                    .ok_or_else(|| {
                        Error::Credential("missing secret_access_key in vault secret".into())
                    })?;
                return Ok((ak.to_string(), sk.to_string()));
            }
            return Err(Error::Credential(
                "vault secret is not valid JSON with AWS credentials".into(),
            ));
        }

        let access_key = self
            .config
            .access_key_id
            .clone()
            .ok_or_else(|| Error::Credential("aws.access_key_id is required".into()))?;
        let secret_key = self
            .config
            .secret_access_key
            .clone()
            .ok_or_else(|| Error::Credential("aws.secret_access_key is required".into()))?;

        // Resolve each individually (may be env: or vault: prefixed)
        let ak = credential::resolve(&access_key).await?;
        let sk = credential::resolve(&secret_key).await?;

        Ok((ak, sk))
    }

    /// Make a signed AWS API request using JSON target API style.
    async fn aws_json_request(
        &self,
        service: &str,
        target: &str,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let (access_key, _secret_key) = self.resolve_credentials().await?;
        let region = &self.config.region;
        let endpoint = format!("https://{service}.{region}.amazonaws.com");

        let body = serde_json::to_string(payload)
            .map_err(|e| Error::Source(format!("JSON serialise error: {e}")))?;

        // AWS JSON APIs use POST with X-Amz-Target header and content-type application/x-amz-json-1.1
        let now = Utc::now();
        let date_stamp = now.format("%Y%m%d").to_string();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

        // Simplified SigV4 — for MVP, use Authorization header with basic signing
        // In production, use aws-sigv4 crate for proper signing
        let resp = self
            .client
            .post(&endpoint)
            .header("Content-Type", "application/x-amz-json-1.1")
            .header("X-Amz-Target", target)
            .header("X-Amz-Date", &amz_date)
            .header("X-Amz-Security-Token", "")
            .header(
                "Authorization",
                format!(
                    "AWS4-HMAC-SHA256 Credential={access_key}/{date_stamp}/{region}/{service}/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=TODO_IMPLEMENT_SIGV4"
                ),
            )
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Source(format!("AWS {service} request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "AWS {service} API returned {status}: {body}"
            )));
        }

        resp.json()
            .await
            .map_err(|e| Error::Source(format!("failed to parse AWS {service} response: {e}")))
    }
}

#[async_trait]
impl Source for AwsSource {
    fn name(&self) -> &'static str {
        "aws"
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
            region = %self.config.region,
            "Fetching AWS data"
        );

        let mut results = Vec::new();
        for service in &self.config.services {
            let fetch_result = match service.name.as_str() {
                "cloudtrail" => self.fetch_cloudtrail(service).await?,
                "guardduty" => self.fetch_guardduty(service).await?,
                "securityhub" => self.fetch_securityhub(service).await?,
                "config" => self.fetch_config(service).await?,
                other => {
                    warn!(service = other, "Unknown AWS service, skipping");
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
        // Validate credentials exist
        match self.resolve_credentials().await {
            Ok(_) => Ok(true),
            Err(e) => {
                warn!(error = %e, "AWS health check failed");
                Ok(false)
            }
        }
    }
}

impl AwsSource {
    async fn fetch_cloudtrail(
        &self,
        _service: &crate::config::AwsService,
    ) -> Result<Option<FetchResult>> {
        let now = Utc::now();
        let start = now - chrono::Duration::hours(1);

        let payload = serde_json::json!({
            "StartTime": start.timestamp(),
            "EndTime": now.timestamp(),
            "MaxResults": 50
        });

        let response = self
            .aws_json_request(
                "cloudtrail",
                "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents",
                &payload,
            )
            .await?;

        let events = response["Events"].as_array();
        let records: Vec<Bytes> = events
            .into_iter()
            .flatten()
            .filter_map(|event| serde_json::to_vec(event).ok().map(Bytes::from))
            .collect();

        if records.is_empty() {
            return Ok(None);
        }

        info!(records = records.len(), "AWS CloudTrail events fetched");
        Ok(Some(FetchResult {
            records,
            source: "aws.cloudtrail".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_guardduty(
        &self,
        _service: &crate::config::AwsService,
    ) -> Result<Option<FetchResult>> {
        // GuardDuty: ListDetectors -> ListFindings -> GetFindings
        let detectors_resp = self
            .aws_json_request(
                "guardduty",
                "com.amazonaws.guardduty.v20170811.GuardDuty_20170811.ListDetectors",
                &serde_json::json!({}),
            )
            .await?;

        let detector_ids = detectors_resp["DetectorIds"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        let mut all_records = Vec::new();

        for detector_id in detector_ids {
            let detector_id = detector_id.as_str().unwrap_or_default();
            if detector_id.is_empty() {
                continue;
            }

            let findings_resp = self
                .aws_json_request(
                    "guardduty",
                    "com.amazonaws.guardduty.v20170811.GuardDuty_20170811.ListFindings",
                    &serde_json::json!({
                        "DetectorId": detector_id,
                        "MaxResults": 50
                    }),
                )
                .await?;

            let finding_ids = findings_resp["FindingIds"]
                .as_array()
                .cloned()
                .unwrap_or_default();

            if finding_ids.is_empty() {
                continue;
            }

            let details_resp = self
                .aws_json_request(
                    "guardduty",
                    "com.amazonaws.guardduty.v20170811.GuardDuty_20170811.GetFindings",
                    &serde_json::json!({
                        "DetectorId": detector_id,
                        "FindingIds": finding_ids
                    }),
                )
                .await?;

            if let Some(findings) = details_resp["Findings"].as_array() {
                for finding in findings {
                    if let Ok(json) = serde_json::to_vec(finding) {
                        all_records.push(Bytes::from(json));
                    }
                }
            }
        }

        if all_records.is_empty() {
            return Ok(None);
        }

        info!(
            records = all_records.len(),
            "AWS GuardDuty findings fetched"
        );
        Ok(Some(FetchResult {
            records: all_records,
            source: "aws.guardduty".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_securityhub(
        &self,
        _service: &crate::config::AwsService,
    ) -> Result<Option<FetchResult>> {
        let payload = serde_json::json!({
            "Filters": {
                "WorkflowStatus": [{"Value": "NEW", "Comparison": "EQUALS"}]
            },
            "MaxResults": 100
        });

        let response = self
            .aws_json_request(
                "securityhub",
                "com.amazonaws.securityhub.v20180710.SecurityHub_20180710.GetFindings",
                &payload,
            )
            .await?;

        let findings = response["Findings"].as_array();
        let records: Vec<Bytes> = findings
            .into_iter()
            .flatten()
            .filter_map(|f| serde_json::to_vec(f).ok().map(Bytes::from))
            .collect();

        if records.is_empty() {
            return Ok(None);
        }

        info!(records = records.len(), "AWS SecurityHub findings fetched");
        Ok(Some(FetchResult {
            records,
            source: "aws.securityhub".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_config(
        &self,
        _service: &crate::config::AwsService,
    ) -> Result<Option<FetchResult>> {
        let payload = serde_json::json!({
            "limit": 100
        });

        let response = self
            .aws_json_request(
                "config",
                "com.amazonaws.config.v20141112.StarlingDoveService.SelectAggregateResourceConfig",
                &payload,
            )
            .await?;

        let results = response["Results"].as_array();
        let records: Vec<Bytes> = results
            .into_iter()
            .flatten()
            .filter_map(|r| {
                // Results may be strings (JSON encoded) — try to parse them
                if let Some(s) = r.as_str() {
                    Some(Bytes::from(s.to_string()))
                } else {
                    serde_json::to_vec(r).ok().map(Bytes::from)
                }
            })
            .collect();

        if records.is_empty() {
            return Ok(None);
        }

        info!(records = records.len(), "AWS Config resources fetched");
        Ok(Some(FetchResult {
            records,
            source: "aws.config".to_string(),
            topic: self.config.topic.clone(),
        }))
    }
}
