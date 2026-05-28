// Project:   dfe-fetcher
// File:      src/source/salesforce/mod.rs
// Purpose:   Salesforce audit source (SetupAuditTrail, LoginHistory, EventLogFile)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Salesforce audit source.
//!
//! **Alpha** (code-complete, not production-validated) and additionally
//! pending a Salesforce connected app before it can be exercised live.
//! Same status as the `google_workspace` and `gcp_pubsub` sources. The
//! code is written against the documented Salesforce REST API but cannot
//! be exercised against a live org until a connected app is provisioned
//! (consumer key plus X.509 cert for JWT bearer, or a run-as user for
//! client credentials) and the integration user is granted API Enabled,
//! View Setup Audit Trail, and View Event Log Files. Until then the e2e
//! tests in this area stay `#[ignore]`'d.
//!
//! Pulls security/audit data from a Salesforce org over the REST API:
//!
//! - `setup_audit_trail` - admin configuration changes, via SOQL against the
//!   `SetupAuditTrail` sObject. Available on every org (no add-on).
//! - `login_history` - login events, via SOQL against `LoginHistory`.
//!   Available on every org.
//! - `event_log_file` - runtime events delivered as downloadable CSV log
//!   files. Two-stage: SOQL lists `EventLogFile` rows in the window, then each
//!   row's body is downloaded from
//!   `/sobjects/EventLogFile/<id>/LogFile` and parsed from CSV into one record
//!   per line. Seven event types are available free with 1-day retention;
//!   70+ require the Event Monitoring / Shield add-on.
//!
//! Real-Time Event Monitoring (platform events via Pub/Sub or big-object
//! SOQL) is intentionally out of scope here - it is Shield-only and its
//! transport differs enough to warrant a separate source family if needed.
//!
//! ## Authentication
//!
//! OAuth2 against `<login_url>/services/oauth2/token` via one of two
//! server-to-server flows, selected by which config fields are present:
//!
//! - **JWT bearer** (Salesforce-recommended): an RS256 JWT with claims
//!   `{iss: client_id, sub: username, aud: login_url, exp: now+5m}` signed
//!   with the connected app's RSA private key.
//! - **client credentials**: `client_id` + `client_secret` form post.
//!
//! Either way the token response carries an `instance_url` that all
//! subsequent API calls target (not `login_url`).

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use tracing::{debug, info, warn};

use crate::config::{SalesforceService, SalesforceSourceConfig};
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

const REQUEST_TIMEOUT: Duration = Duration::from_mins(2);

/// Default OAuth2 login base URL (production). Sandboxes use
/// `https://test.salesforce.com`.
const LOGIN_URL_DEFAULT: &str = "https://login.salesforce.com";

/// Default REST API version path segment.
const API_VERSION_DEFAULT: &str = "v60.0";

/// JWT lifetime. Salesforce rejects assertions with `exp` more than a few
/// minutes out; 5 minutes is the documented norm.
const JWT_TTL_SECS: i64 = 300;

/// Cap on SOQL result pages followed per service per tick.
const MAX_QUERY_PAGES: usize = 50;

/// Cap on EventLogFile bodies downloaded per tick. Each file can be large;
/// this bounds a single tick's work. Remaining files roll into later ticks
/// as the window advances.
const MAX_LOG_FILES_PER_TICK: usize = 200;

/// One SOQL-backed audit surface emitted as JSON rows directly.
struct SoqlAuditQuery {
    service_name: &'static str,
    sobject: &'static str,
    select_fields: &'static str,
    time_field: &'static str,
}

const SOQL_AUDIT_QUERIES: &[SoqlAuditQuery] = &[
    SoqlAuditQuery {
        service_name: "setup_audit_trail",
        sobject: "SetupAuditTrail",
        select_fields: "Id, Action, Section, CreatedDate, Display, DelegateUser, CreatedBy.Username",
        time_field: "CreatedDate",
    },
    SoqlAuditQuery {
        service_name: "login_history",
        sobject: "LoginHistory",
        select_fields: "Id, UserId, LoginTime, LoginType, SourceIp, Status, Application, Browser, Platform, CountryIso, ApiType, TlsProtocol",
        time_field: "LoginTime",
    },
];

/// Resolved OAuth token plus the instance URL to address subsequent calls.
#[derive(Debug)]
struct SalesforceToken {
    access_token: String,
    instance_url: String,
}

/// Salesforce audit data source.
pub struct SalesforceSource {
    config: SalesforceSourceConfig,
    client: reqwest::Client,
}

impl SalesforceSource {
    pub fn new(config: SalesforceSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    fn login_url(&self) -> &str {
        self.config
            .login_url
            .as_deref()
            .unwrap_or(LOGIN_URL_DEFAULT)
    }

    fn api_version(&self) -> &str {
        self.config
            .api_version
            .as_deref()
            .unwrap_or(API_VERSION_DEFAULT)
    }

    /// Acquire an access token + instance URL. Picks the JWT-bearer flow when
    /// a private key is configured, else the client-credentials flow.
    async fn get_token(&self) -> Result<SalesforceToken> {
        let token_url = format!("{}/services/oauth2/token", self.login_url());

        let has_private_key =
            self.config.private_key.is_some() || self.config.private_key_secret.is_some();
        let has_secret =
            self.config.client_secret.is_some() || self.config.credential_secret.is_some();

        let resp = if has_private_key {
            self.token_via_jwt_bearer(&token_url).await?
        } else if has_secret {
            self.token_via_client_credentials(&token_url).await?
        } else {
            return Err(Error::Credential(
                "salesforce requires JWT-bearer (client_id + username + private_key) or \
                 client-credentials (client_id + client_secret) auth fields"
                    .into(),
            ));
        };

        let access_token = resp["access_token"]
            .as_str()
            .ok_or_else(|| Error::Credential("missing access_token in Salesforce response".into()))?
            .to_string();
        let instance_url = self
            .config
            .instance_url_override
            .clone()
            .or_else(|| resp["instance_url"].as_str().map(String::from))
            .ok_or_else(|| {
                Error::Credential("missing instance_url in Salesforce token response".into())
            })?;

        Ok(SalesforceToken {
            access_token,
            instance_url: instance_url.trim_end_matches('/').to_string(),
        })
    }

    async fn token_via_jwt_bearer(&self, token_url: &str) -> Result<serde_json::Value> {
        let client_id = self.config.client_id.as_deref().ok_or_else(|| {
            Error::Credential("salesforce.client_id is required for JWT auth".into())
        })?;
        let username = self.config.username.as_deref().ok_or_else(|| {
            Error::Credential("salesforce.username is required for JWT auth".into())
        })?;

        let private_key = if let Some(spec) = self.config.private_key_secret.as_deref() {
            credential::resolve(spec).await?
        } else {
            self.config
                .private_key
                .clone()
                .ok_or_else(|| Error::Credential("salesforce private key missing".into()))?
        };

        // aud is the login host (production/sandbox/My-Domain), per the
        // JWT-bearer spec - NOT the instance URL.
        let now = Utc::now().timestamp();
        let claims = serde_json::json!({
            "iss": client_id,
            "sub": username,
            "aud": self.login_url(),
            "exp": now + JWT_TTL_SECS,
        });

        let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key.as_bytes())
            .map_err(|e| Error::Credential(format!("invalid Salesforce private key: {e}")))?;
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        let assertion = jsonwebtoken::encode(&header, &claims, &encoding_key)
            .map_err(|e| Error::Credential(format!("Salesforce JWT signing failed: {e}")))?;

        let resp = self
            .client
            .post(token_url)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &assertion),
            ])
            .send()
            .await
            .map_err(|e| Error::Credential(format!("Salesforce JWT token request failed: {e}")))?;

        Self::parse_token_response(resp).await
    }

    async fn token_via_client_credentials(&self, token_url: &str) -> Result<serde_json::Value> {
        let client_id = self.config.client_id.as_deref().ok_or_else(|| {
            Error::Credential("salesforce.client_id is required for client-credentials auth".into())
        })?;
        let client_secret = if let Some(spec) = self.config.credential_secret.as_deref() {
            credential::resolve(spec).await?
        } else {
            self.config
                .client_secret
                .as_ref()
                .map(|s| s.expose().to_string())
                .ok_or_else(|| Error::Credential("salesforce.client_secret is required".into()))?
        };

        let resp = self
            .client
            .post(token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", client_id),
                ("client_secret", &client_secret),
            ])
            .send()
            .await
            .map_err(|e| {
                Error::Credential(format!("Salesforce client-credentials request failed: {e}"))
            })?;

        Self::parse_token_response(resp).await
    }

    async fn parse_token_response(resp: reqwest::Response) -> Result<serde_json::Value> {
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Credential(format!(
                "Salesforce token endpoint returned {status}: {body}"
            )));
        }
        resp.json()
            .await
            .map_err(|e| Error::Credential(format!("failed to parse Salesforce token: {e}")))
    }

    /// Run a SOQL query, following `nextRecordsUrl` pagination, returning all
    /// `records` objects across pages.
    async fn soql_query(
        &self,
        token: &SalesforceToken,
        soql: &str,
    ) -> Result<Vec<serde_json::Value>> {
        let mut records: Vec<serde_json::Value> = Vec::new();
        let mut next_url = Some(format!(
            "{}/services/data/{}/query?q={}",
            token.instance_url,
            self.api_version(),
            percent_encode(soql),
        ));

        for _ in 0..MAX_QUERY_PAGES {
            let Some(url) = next_url.take() else { break };

            let resp = self
                .client
                .get(&url)
                .bearer_auth(&token.access_token)
                .header("Accept", "application/json")
                .send()
                .await
                .map_err(|e| Error::Source(format!("Salesforce SOQL request failed: {e}")))?;

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(Error::Source(format!(
                    "Salesforce SOQL returned {status}: {body}"
                )));
            }

            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::Source(format!("Salesforce SOQL parse failed: {e}")))?;

            if let Some(arr) = body["records"].as_array() {
                records.extend(arr.iter().cloned());
            }

            // `done: false` means more pages; `nextRecordsUrl` is a path on
            // the same instance.
            if body["done"].as_bool().unwrap_or(true) {
                break;
            }
            next_url = body["nextRecordsUrl"]
                .as_str()
                .map(|p| format!("{}{}", token.instance_url, p));
        }

        Ok(records)
    }

    /// Fetch a SOQL-backed audit surface (setup_audit_trail / login_history).
    async fn fetch_soql_audit(
        &self,
        token: &SalesforceToken,
        q: &SoqlAuditQuery,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        // SOQL datetime literals are NOT quoted (unlike string literals).
        let soql = format!(
            "SELECT {} FROM {} WHERE {} >= {} AND {} < {} ORDER BY {} ASC",
            q.select_fields,
            q.sobject,
            q.time_field,
            soql_datetime(start),
            q.time_field,
            soql_datetime(end),
            q.time_field,
        );

        let rows = self.soql_query(token, &soql).await?;
        if rows.is_empty() {
            return Ok(None);
        }

        let mut records: Vec<Bytes> = Vec::with_capacity(rows.len());
        for row in &rows {
            let buf = serde_json::to_vec(row).map_err(|e| {
                Error::Source(format!(
                    "Salesforce {} serialise failed: {e}",
                    q.service_name
                ))
            })?;
            records.push(Bytes::from(buf));
        }

        info!(
            service = q.service_name,
            records = records.len(),
            "Salesforce audit rows fetched"
        );
        Ok(Some(FetchResult {
            records,
            source: format!("salesforce.{}", q.service_name),
            topic: self.config.topic.clone(),
        }))
    }

    /// Fetch EventLogFile: list rows in the window, download each LogFile
    /// CSV, emit one record per line tagged with EventType + LogDate.
    async fn fetch_event_log_file(
        &self,
        token: &SalesforceToken,
        svc: &SalesforceService,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<FetchResult>> {
        let interval = svc
            .config
            .get("interval")
            .and_then(|v| v.as_str())
            .unwrap_or("Daily");

        let mut where_clause = format!(
            "LogDate >= {} AND LogDate < {} AND Interval = '{}'",
            soql_datetime(start),
            soql_datetime(end),
            soql_escape(interval),
        );

        // Optional EventType allow-list.
        if let Some(types) = svc.config.get("event_types").and_then(|v| v.as_array()) {
            let quoted: Vec<String> = types
                .iter()
                .filter_map(|v| v.as_str())
                .map(|t| format!("'{}'", soql_escape(t)))
                .collect();
            if !quoted.is_empty() {
                where_clause.push_str(&format!(" AND EventType IN ({})", quoted.join(",")));
            }
        }

        let soql = format!(
            "SELECT Id, EventType, LogDate, LogFileLength, Interval FROM EventLogFile \
             WHERE {where_clause} ORDER BY LogDate ASC"
        );

        let files = self.soql_query(token, &soql).await?;
        if files.is_empty() {
            return Ok(None);
        }

        let mut records: Vec<Bytes> = Vec::new();
        let mut downloaded = 0usize;
        for file in files.iter().take(MAX_LOG_FILES_PER_TICK) {
            let Some(id) = file["Id"].as_str() else {
                continue;
            };
            let event_type = file["EventType"].as_str().unwrap_or("Unknown");
            let log_date = file["LogDate"].as_str().unwrap_or_default();

            let body = match self.download_log_file(token, id).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, event_log_file = id, "Salesforce LogFile download failed, skipping");
                    continue;
                }
            };

            match parse_event_log_csv(&body, event_type, log_date) {
                Ok(mut emitted) => {
                    downloaded += 1;
                    records.append(&mut emitted);
                }
                Err(e) => {
                    warn!(error = %e, event_log_file = id, "Salesforce LogFile CSV parse failed, skipping");
                }
            }
        }

        if records.is_empty() {
            return Ok(None);
        }

        info!(
            files = downloaded,
            records = records.len(),
            "Salesforce EventLogFile rows fetched"
        );
        Ok(Some(FetchResult {
            records,
            source: "salesforce.event_log_file".to_string(),
            topic: self.config.topic.clone(),
        }))
    }

    async fn download_log_file(&self, token: &SalesforceToken, id: &str) -> Result<Bytes> {
        let url = format!(
            "{}/services/data/{}/sobjects/EventLogFile/{}/LogFile",
            token.instance_url,
            self.api_version(),
            id,
        );
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&token.access_token)
            .send()
            .await
            .map_err(|e| Error::Source(format!("LogFile request failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "LogFile download returned {status}: {body}"
            )));
        }
        resp.bytes()
            .await
            .map_err(|e| Error::Source(format!("LogFile body read failed: {e}")))
    }
}

#[async_trait]
impl Source for SalesforceSource {
    fn name(&self) -> &'static str {
        "salesforce"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn fetch(&self, window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled || self.config.services.is_empty() {
            return Ok(vec![]);
        }

        let now = Utc::now();
        let (start, end) = match window {
            Some(w) => (w.start, w.end),
            None => (now - chrono::Duration::hours(1), now),
        };

        info!(
            services = self.config.services.len(),
            "Fetching Salesforce audit data"
        );

        let token = self.get_token().await?;

        let mut results = Vec::new();
        for svc in &self.config.services {
            let outcome = match svc.name.as_str() {
                "event_log_file" => self.fetch_event_log_file(&token, svc, start, end).await,
                name => match SOQL_AUDIT_QUERIES.iter().find(|q| q.service_name == name) {
                    Some(q) => self.fetch_soql_audit(&token, q, start, end).await,
                    None => {
                        warn!(service = name, "Unknown Salesforce service, skipping");
                        continue;
                    }
                },
            };
            match outcome {
                Ok(Some(r)) => results.push(r),
                Ok(None) => {}
                Err(e) => warn!(
                    error = %e,
                    service = svc.name,
                    "Salesforce service fetch failed, continuing"
                ),
            }
        }
        Ok(results)
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        match self.get_token().await {
            Ok(_) => Ok(true),
            Err(e) => {
                warn!(error = %e, "Salesforce health check failed");
                Ok(false)
            }
        }
    }

    fn cursor_prefix(&self) -> String {
        "salesforce".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        self.config
            .services
            .iter()
            .map(|s| s.name.as_str())
            .collect()
    }
}

/// Format a timestamp as a SOQL datetime literal (RFC 3339, `Z` suffix, no
/// surrounding quotes - SOQL datetime literals are bare).
fn soql_datetime(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Escape a value for use inside a single-quoted SOQL string literal.
fn soql_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Percent-encode a query value using RFC 3986 unreserved rules. The SOQL
/// `q=` parameter contains spaces, commas, comparison operators and quotes,
/// all of which must be encoded.
fn percent_encode(s: &str) -> String {
    const UNRESERVED: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if UNRESERVED.contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Parse an EventLogFile CSV body into one JSON record per data row. Each
/// record carries its column values as strings plus the `_dfe_fetcher_*`
/// provenance fields so consumers can correlate back to the source file.
fn parse_event_log_csv(body: &Bytes, event_type: &str, log_date: &str) -> Result<Vec<Bytes>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(&body[..]);

    let headers = reader
        .headers()
        .map_err(|e| Error::Source(format!("EventLogFile CSV header read failed: {e}")))?
        .clone();

    let mut out: Vec<Bytes> = Vec::new();
    for result in reader.records() {
        let record =
            result.map_err(|e| Error::Source(format!("EventLogFile CSV row read failed: {e}")))?;

        let mut obj = serde_json::Map::with_capacity(headers.len() + 2);
        for (h, v) in headers.iter().zip(record.iter()) {
            obj.insert(h.to_string(), serde_json::Value::String(v.to_string()));
        }
        obj.insert(
            "_dfe_fetcher_event_type".to_string(),
            serde_json::Value::String(event_type.to_string()),
        );
        obj.insert(
            "_dfe_fetcher_log_date".to_string(),
            serde_json::Value::String(log_date.to_string()),
        );

        let buf = serde_json::to_vec(&serde_json::Value::Object(obj))
            .map_err(|e| Error::Source(format!("EventLogFile record serialise failed: {e}")))?;
        out.push(Bytes::from(buf));
    }

    debug!(
        event_type,
        log_date,
        records = out.len(),
        "Parsed EventLogFile CSV"
    );
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> SalesforceSourceConfig {
        SalesforceSourceConfig {
            enabled: true,
            ..SalesforceSourceConfig::default()
        }
    }

    #[test]
    fn login_url_defaults_to_production() {
        let src = SalesforceSource::new(cfg());
        assert_eq!(src.login_url(), "https://login.salesforce.com");
    }

    #[test]
    fn login_url_uses_override() {
        let c = SalesforceSourceConfig {
            login_url: Some("https://test.salesforce.com".into()),
            ..cfg()
        };
        let src = SalesforceSource::new(c);
        assert_eq!(src.login_url(), "https://test.salesforce.com");
    }

    #[test]
    fn api_version_defaults() {
        let src = SalesforceSource::new(cfg());
        assert_eq!(src.api_version(), "v60.0");
    }

    #[test]
    fn soql_audit_table_has_known_surfaces() {
        let names: Vec<&str> = SOQL_AUDIT_QUERIES.iter().map(|q| q.service_name).collect();
        assert!(names.contains(&"setup_audit_trail"));
        assert!(names.contains(&"login_history"));
    }

    #[test]
    fn soql_datetime_is_bare_rfc3339_z() {
        let ts = DateTime::parse_from_rfc3339("2026-05-28T03:04:05Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(soql_datetime(ts), "2026-05-28T03:04:05Z");
    }

    #[test]
    fn soql_escape_handles_quotes_and_backslash() {
        assert_eq!(soql_escape("O'Brien"), "O\\'Brien");
        assert_eq!(soql_escape("a\\b"), "a\\\\b");
    }

    #[test]
    fn percent_encode_escapes_soql_specials() {
        assert_eq!(percent_encode("SELECT Id"), "SELECT%20Id");
        assert_eq!(percent_encode("a,b"), "a%2Cb");
        assert_eq!(percent_encode("x>y"), "x%3Ey");
        assert_eq!(percent_encode("'v'"), "%27v%27");
    }

    #[test]
    fn parse_event_log_csv_emits_one_record_per_row() {
        let body = Bytes::from_static(
            b"EVENT_TYPE,TIMESTAMP,USER_ID\nLogin,20260528030405.123,005xx\nLogout,20260528030505.456,005xx\n",
        );
        let out =
            parse_event_log_csv(&body, "Login", "2026-05-28T00:00:00.000+0000").expect("parse");
        assert_eq!(out.len(), 2);
        let first: serde_json::Value = serde_json::from_slice(&out[0]).unwrap();
        assert_eq!(first["EVENT_TYPE"], "Login");
        assert_eq!(first["USER_ID"], "005xx");
        assert_eq!(first["_dfe_fetcher_event_type"], "Login");
        assert_eq!(
            first["_dfe_fetcher_log_date"],
            "2026-05-28T00:00:00.000+0000"
        );
    }

    #[test]
    fn parse_event_log_csv_handles_quoted_fields() {
        let body =
            Bytes::from_static(b"EVENT_TYPE,URI\nReportExport,\"/reports/00O,with,commas\"\n");
        let out = parse_event_log_csv(&body, "ReportExport", "d").expect("parse");
        assert_eq!(out.len(), 1);
        let rec: serde_json::Value = serde_json::from_slice(&out[0]).unwrap();
        assert_eq!(rec["URI"], "/reports/00O,with,commas");
    }

    #[tokio::test]
    async fn fetch_returns_empty_when_disabled() {
        let src = SalesforceSource::new(SalesforceSourceConfig::default());
        let r = src.fetch(None).await.expect("fetch should not error");
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn get_token_errors_without_any_auth_fields() {
        let src = SalesforceSource::new(cfg());
        let err = src
            .get_token()
            .await
            .expect_err("should require auth fields");
        assert!(matches!(err, Error::Credential(_)));
    }
}
