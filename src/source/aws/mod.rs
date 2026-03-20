// Project:   dfe-fetcher
// File:      src/source/aws/mod.rs
// Purpose:   AWS data source (CloudTrail, GuardDuty, SecurityHub, Config, CloudWatch)
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
//! - CloudWatch Logs (log groups)
//! - CloudWatch Metrics (monitoring data)
//!
//! Authentication: Static credentials, assume role, or secrets manager.
//! Uses AWS REST APIs with SigV4 signing via the `reqsign` crate.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric},
    resource::v1::Resource,
};
use prost::Message;
use reqsign::{AwsCredential, AwsV4Signer};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::config::AwsSourceConfig;
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

/// Map CloudWatch unit strings to UCUM (Unified Code for Units of Measure) codes
/// for OpenTelemetry compatibility.
fn cloudwatch_unit_to_ucum(unit: &str) -> &str {
    match unit {
        "Seconds" => "s",
        "Microseconds" => "us",
        "Milliseconds" => "ms",
        "Bytes" => "By",
        "Kilobytes" => "kBy",
        "Megabytes" => "MBy",
        "Gigabytes" => "GBy",
        "Terabytes" => "TBy",
        "Bits" => "bit",
        "Kilobits" => "kbit",
        "Megabits" => "Mbit",
        "Gigabits" => "Gbit",
        "Terabits" => "Tbit",
        "Percent" => "%",
        "Count" => "{Count}",
        "Bytes/Second" => "By/s",
        "Kilobytes/Second" => "kBy/s",
        "Megabytes/Second" => "MBy/s",
        "Gigabytes/Second" => "GBy/s",
        "Terabytes/Second" => "TBy/s",
        "Bits/Second" => "bit/s",
        "Kilobits/Second" => "kbit/s",
        "Megabits/Second" => "Mbit/s",
        "Gigabits/Second" => "Gbit/s",
        "Terabits/Second" => "Tbit/s",
        "Count/Second" => "{Count}/s",
        _ => "1",
    }
}

/// Build an OTel `KeyValue` attribute with a string value.
fn otel_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

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
    ///
    /// `json_version` is the AWS JSON protocol version: "1.1" for most services,
    /// "1.0" for CloudWatch Monitoring.
    async fn aws_json_request(
        &self,
        service: &str,
        target: &str,
        payload: &serde_json::Value,
        json_version: &str,
    ) -> Result<serde_json::Value> {
        let (access_key, secret_key) = self.resolve_credentials().await?;
        let region = &self.config.region;
        let endpoint = match &self.config.endpoint_override {
            Some(url) => url.clone(),
            None => format!("https://{service}.{region}.amazonaws.com"),
        };

        let body = serde_json::to_string(payload)
            .map_err(|e| Error::Source(format!("JSON serialise error: {e}")))?;

        // Compute body SHA256 for SigV4 (non-S3 services don't accept UNSIGNED-PAYLOAD)
        let body_hash = hex::encode(Sha256::digest(body.as_bytes()));

        // Build the request, then sign it with SigV4 before sending
        let mut req = self
            .client
            .post(&endpoint)
            .header(
                "Content-Type",
                format!("application/x-amz-json-{json_version}"),
            )
            .header("X-Amz-Target", target)
            .header("x-amz-content-sha256", &body_hash)
            .body(body)
            .build()
            .map_err(|e| Error::Source(format!("failed to build AWS request: {e}")))?;

        // Sign with SigV4 using reqsign — handles date, signature, and all canonical headers
        let cred = AwsCredential {
            access_key_id: access_key,
            secret_access_key: secret_key,
            session_token: None,
            expires_in: None,
        };
        let signer = AwsV4Signer::new(service, region);
        signer
            .sign(&mut req, &cred)
            .map_err(|e| Error::Source(format!("SigV4 signing failed: {e}")))?;

        let resp = self
            .client
            .execute(req)
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

    async fn fetch(&self, window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        info!(
            services = self.config.services.len(),
            region = %self.config.region,
            "Fetching AWS data"
        );

        let now = Utc::now();
        let (start, end) = match window {
            Some(w) => (w.start, w.end),
            None => (now - chrono::Duration::hours(1), now),
        };

        let mut results = Vec::new();
        for service in &self.config.services {
            let fetch_result = match service.name.as_str() {
                "cloudtrail" => self.fetch_cloudtrail(service, start, end).await?,
                "guardduty" => self.fetch_guardduty(service).await?,
                "securityhub" => self.fetch_securityhub(service).await?,
                "config" => self.fetch_config(service).await?,
                "cloudwatch_logs" => self.fetch_cloudwatch_logs(service, start, end).await?,
                "cloudwatch_metrics" => self.fetch_cloudwatch_metrics(service, start, end).await?,
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

    fn cursor_prefix(&self) -> String {
        "aws".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        self.config
            .services
            .iter()
            .map(|s| s.name.as_str())
            .collect()
    }
}

impl AwsSource {
    async fn fetch_cloudtrail(
        &self,
        _service: &crate::config::AwsService,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        let payload = serde_json::json!({
            "StartTime": start.timestamp(),
            "EndTime": end.timestamp(),
            "MaxResults": 50
        });

        let response = self
            .aws_json_request(
                "cloudtrail",
                "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents",
                &payload,
                "1.1",
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
                "1.1",
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
                    "1.1",
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
                    "1.1",
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
                "1.1",
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
                "1.1",
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

    async fn fetch_cloudwatch_logs(
        &self,
        service: &crate::config::AwsService,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        let log_group = service
            .config
            .get("log_group_name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::Config("cloudwatch_logs requires log_group_name in service config".into())
            })?;

        let filter_pattern = service
            .config
            .get("filter_pattern")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let mut all_records = Vec::new();
        let mut next_token: Option<String> = None;

        for _page in 0..20 {
            let mut payload = serde_json::json!({
                "logGroupName": log_group,
                "startTime": start.timestamp_millis(),
                "endTime": end.timestamp_millis(),
                "limit": 10000
            });

            if !filter_pattern.is_empty() {
                payload["filterPattern"] = serde_json::Value::String(filter_pattern.to_string());
            }

            if let Some(ref token) = next_token {
                payload["nextToken"] = serde_json::Value::String(token.clone());
            }

            let response = self
                .aws_json_request("logs", "Logs_20140328.FilterLogEvents", &payload, "1.1")
                .await?;

            if let Some(events) = response["events"].as_array() {
                for event in events {
                    if let Ok(json) = serde_json::to_vec(event) {
                        all_records.push(Bytes::from(json));
                    }
                }
            }

            match response["nextToken"].as_str() {
                Some(token) => next_token = Some(token.to_string()),
                None => break,
            }
        }

        if all_records.is_empty() {
            return Ok(None);
        }

        info!(
            records = all_records.len(),
            log_group = log_group,
            "AWS CloudWatch Logs fetched"
        );
        Ok(Some(FetchResult {
            records: all_records,
            source: "aws.cloudwatch_logs".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn fetch_cloudwatch_metrics(
        &self,
        service: &crate::config::AwsService,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        // Namespaces to query (required — prevents firehose)
        let namespaces: Vec<String> = service
            .config
            .get("namespaces")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        if namespaces.is_empty() {
            return Err(Error::Config(
                "cloudwatch_metrics requires namespaces in service config".into(),
            ));
        }

        // Optional metric name whitelist
        let metric_names: Vec<String> = service
            .config
            .get("metric_names")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let period_secs: i64 = service
            .config
            .get("period_secs")
            .and_then(|v| v.as_i64())
            .unwrap_or(300);

        let stat = service
            .config
            .get("stat")
            .and_then(|v| v.as_str())
            .unwrap_or("Average");

        // Output format: "json" (default) or "otlp" (protobuf)
        let output_format = service
            .config
            .get("output_format")
            .and_then(|v| v.as_str())
            .unwrap_or("json");

        // Discover metrics per namespace via ListMetrics
        // Tuple: (namespace, metric_name, dimensions, unit)
        let mut queries = Vec::new();
        let mut query_meta: Vec<(String, String, serde_json::Value, String)> = Vec::new();

        for namespace in &namespaces {
            let mut next_token: Option<String> = None;

            for _page in 0..10 {
                let mut payload = serde_json::json!({
                    "Namespace": namespace
                });

                if let Some(ref token) = next_token {
                    payload["NextToken"] = serde_json::Value::String(token.clone());
                }

                let response = self
                    .aws_json_request(
                        "monitoring",
                        "GraniteServiceVersion20100801.ListMetrics",
                        &payload,
                        "1.0",
                    )
                    .await?;

                if let Some(metrics) = response["Metrics"].as_array() {
                    for metric in metrics {
                        let metric_name = metric["MetricName"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string();

                        // Apply metric_names whitelist if set
                        if !metric_names.is_empty() && !metric_names.contains(&metric_name) {
                            continue;
                        }

                        let dimensions = metric["Dimensions"].clone();
                        let unit = metric["Unit"].as_str().unwrap_or("None").to_string();
                        let query_id = format!("q{}", queries.len());

                        queries.push(serde_json::json!({
                            "Id": query_id,
                            "MetricStat": {
                                "Metric": {
                                    "Namespace": namespace,
                                    "MetricName": metric_name,
                                    "Dimensions": dimensions
                                },
                                "Period": period_secs,
                                "Stat": stat
                            }
                        }));
                        query_meta.push((namespace.clone(), metric_name, dimensions, unit));
                    }
                }

                match response["NextToken"].as_str() {
                    Some(token) => next_token = Some(token.to_string()),
                    None => break,
                }
            }
        }

        if queries.is_empty() {
            return Ok(None);
        }

        // Intermediate data: (namespace, metric_name, dimensions_json, unit, timestamp_f64, value_f64)
        let mut data_points: Vec<(String, String, serde_json::Value, String, f64, f64)> =
            Vec::new();

        // GetMetricData in batches of 500 (API limit)
        for (batch_idx, chunk) in queries.chunks(500).enumerate() {
            let mut payload = serde_json::json!({
                "StartTime": start.timestamp(),
                "EndTime": end.timestamp(),
                "MetricDataQueries": chunk
            });

            let mut next_token: Option<String> = None;

            for _page in 0..10 {
                if let Some(ref token) = next_token {
                    payload["NextToken"] = serde_json::Value::String(token.clone());
                }

                let response = self
                    .aws_json_request(
                        "monitoring",
                        "GraniteServiceVersion20100801.GetMetricData",
                        &payload,
                        "1.0",
                    )
                    .await?;

                if let Some(results) = response["MetricDataResults"].as_array() {
                    for result in results {
                        let id = result["Id"].as_str().unwrap_or_default();

                        // Map query ID back to metadata
                        let id_num: usize = id
                            .strip_prefix('q')
                            .and_then(|n| n.parse().ok())
                            .unwrap_or(0);
                        let global_idx = batch_idx * 500 + id_num;

                        let (ns, mn, dims, unit) =
                            query_meta.get(global_idx).cloned().unwrap_or_default();

                        let timestamps =
                            result["Timestamps"].as_array().cloned().unwrap_or_default();
                        let values = result["Values"].as_array().cloned().unwrap_or_default();

                        for (ts, val) in timestamps.iter().zip(values.iter()) {
                            let ts_f64 = ts.as_f64().unwrap_or_default();
                            let val_f64 = val.as_f64().unwrap_or_default();
                            data_points.push((
                                ns.clone(),
                                mn.clone(),
                                dims.clone(),
                                unit.clone(),
                                ts_f64,
                                val_f64,
                            ));
                        }
                    }
                }

                match response["NextToken"].as_str() {
                    Some(token) => next_token = Some(token.to_string()),
                    None => break,
                }
            }
        }

        if data_points.is_empty() {
            return Ok(None);
        }

        // Build output records based on format
        let all_records = if output_format == "otlp" {
            self.build_otlp_metrics(&data_points, stat)
        } else {
            self.build_json_metrics(&data_points, stat)
        };

        info!(
            records = all_records.len(),
            namespaces = ?namespaces,
            output_format,
            "AWS CloudWatch Metrics fetched"
        );
        Ok(Some(FetchResult {
            records: all_records,
            source: "aws.cloudwatch_metrics".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    /// Build JSON records from CloudWatch metric data points.
    fn build_json_metrics(
        &self,
        data_points: &[(String, String, serde_json::Value, String, f64, f64)],
        stat: &str,
    ) -> Vec<Bytes> {
        data_points
            .iter()
            .filter_map(|(ns, mn, dims, unit, ts, val)| {
                let record = serde_json::json!({
                    "namespace": ns,
                    "metric_name": mn,
                    "dimensions": dims,
                    "unit": unit,
                    "timestamp": ts,
                    "value": val,
                    "stat": stat
                });
                serde_json::to_vec(&record).ok().map(Bytes::from)
            })
            .collect()
    }

    /// Build OTLP protobuf from CloudWatch metric data points.
    ///
    /// Groups data points by (namespace, metric_name, unit, dimensions) into
    /// OTel Gauge metrics within a single `ExportMetricsServiceRequest`.
    fn build_otlp_metrics(
        &self,
        data_points: &[(String, String, serde_json::Value, String, f64, f64)],
        stat: &str,
    ) -> Vec<Bytes> {
        use std::collections::BTreeMap;

        // Group by (namespace, metric_name, unit, dimensions_json) for proper OTel structure
        // Each unique combination becomes one Metric with multiple data points
        let mut grouped: BTreeMap<(String, String, String, String), Vec<NumberDataPoint>> =
            BTreeMap::new();

        for (ns, mn, dims, unit, ts, val) in data_points {
            let dims_key = dims.to_string();

            // Build data point attributes: Namespace + individual dimensions
            let mut attributes = vec![otel_kv("Namespace", ns)];
            if let Some(dim_array) = dims.as_array() {
                for dim in dim_array {
                    let name = dim["Name"].as_str().unwrap_or_default();
                    let value = dim["Value"].as_str().unwrap_or_default();
                    if !name.is_empty() {
                        attributes.push(otel_kv(name, value));
                    }
                }
            }

            // Timestamp: CloudWatch returns seconds, OTel wants nanoseconds
            #[allow(clippy::cast_sign_loss)]
            let time_unix_nano = (*ts * 1_000_000_000.0) as u64;

            let data_point = NumberDataPoint {
                attributes,
                start_time_unix_nano: 0,
                time_unix_nano,
                exemplars: Vec::new(),
                flags: 0,
                value: Some(
                    opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                        *val,
                    ),
                ),
            };

            let key = (ns.clone(), mn.clone(), unit.clone(), dims_key);
            grouped.entry(key).or_default().push(data_point);
        }

        // Build OTel Metric objects
        let metrics: Vec<Metric> = grouped
            .into_iter()
            .map(|((_, mn, unit, _), points)| {
                let ucum_unit = cloudwatch_unit_to_ucum(&unit);
                Metric {
                    name: mn,
                    description: String::new(),
                    unit: ucum_unit.to_string(),
                    metadata: vec![otel_kv("stat", stat)],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: points,
                    })),
                }
            })
            .collect();

        // Build resource with cloud metadata
        let resource = Resource {
            attributes: vec![
                otel_kv("cloud.provider", "aws"),
                otel_kv("cloud.region", &self.config.region),
                otel_kv("service.name", "dfe-fetcher"),
            ],
            dropped_attributes_count: 0,
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(resource),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "dfe-fetcher".to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                    }),
                    metrics,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        // Serialise to protobuf bytes
        let mut buf = Vec::with_capacity(request.encoded_len());
        if request.encode(&mut buf).is_ok() {
            vec![Bytes::from(buf)]
        } else {
            warn!("Failed to encode OTLP metrics protobuf");
            Vec::new()
        }
    }
}
