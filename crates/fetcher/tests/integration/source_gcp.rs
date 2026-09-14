// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_gcp.rs
// Purpose:   Characterisation of the GCP source: service-account JWT exchange, Cloud Logging and SCC shapes
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The GCP source against wiremock.
//!
//! Each test configures the typed `sources.gcp` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the requests wiremock recorded (the `entries:list` POST body per Cloud
//! Logging unit with its documented filter and the window, `nextPageToken`
//! fed back as `pageToken`, the SCC findings GET, the bearer), on the
//! service-account exchange (an RS256 assertion the mock verifies against
//! the test's public key), and on the records that landed (the provider's
//! entry, semantically, plus what enrichment added). The typed config block
//! is the operator's contract; the shipped `gcp` profile serves it through
//! the framework driver.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use dfe_fetcher::config::{Config, GcpConnection, GcpService, GcpSourceConfig};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::common::{rsa_key_pair, service_account_key};

const ENTRIES: &str = "/v2/entries:list";
const FINDINGS: &str = "/v1/organizations/123456789/sources/-/findings";
const TOKEN_PATH: &str = "/token";
const CLIENT_EMAIL: &str = "fetcher@test-project.iam.gserviceaccount.com";

/// A deployment config carrying `gcp` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(gcp: GcpSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.gcp = gcp;
    config
}

/// The typed block an operator writes with a resolved bearer token: the
/// project, the API hosts pointed at wiremock, the services.
fn project_config(server: &MockServer, services: &[&str]) -> GcpSourceConfig {
    GcpSourceConfig {
        enabled: true,
        project_id: Some("test-project".into()),
        credential_secret: Some("mock-gcp-token".into()),
        api_url_override: Some(server.uri()),
        services: services.iter().map(|s| service(s, &[])).collect(),
        topic: "test-gcp".into(),
        ..GcpSourceConfig::default()
    }
}

fn service(name: &str, config: &[(&str, Value)]) -> GcpService {
    GcpService {
        name: name.into(),
        config: config
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    }
}

/// A service-account key on disk and the mock token endpoint that verifies
/// the assertions signed with it, recording their claims.
struct ServiceAccount {
    _dir: tempfile::TempDir,
    key_path: String,
    claims: Arc<Mutex<Vec<Value>>>,
}

struct JwtExchange {
    public_pem: String,
    claims: Arc<Mutex<Vec<Value>>>,
}

impl Respond for JwtExchange {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let form = form_of(request);
        let refused = ResponseTemplate::new(401).set_body_json(json!({"error": "invalid_grant"}));
        if form.get("grant_type").map(String::as_str)
            != Some("urn:ietf:params:oauth:grant-type:jwt-bearer")
        {
            return refused;
        }
        let Some(assertion) = form.get("assertion") else {
            return refused;
        };
        let key = jsonwebtoken::DecodingKey::from_rsa_pem(self.public_pem.as_bytes())
            .expect("public key");
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
        validation.validate_aud = false;
        validation.set_required_spec_claims(&["exp", "iat"]);
        match jsonwebtoken::decode::<Value>(assertion, &key, &validation) {
            Ok(data) => {
                self.claims.lock().unwrap().push(data.claims);
                ResponseTemplate::new(200).set_body_json(json!({
                    "access_token": "sa-token",
                    "expires_in": 3599,
                    "token_type": "Bearer"
                }))
            }
            Err(_) => refused,
        }
    }
}

/// Mount the exchange and write the key whose `token_uri` names it.
async fn service_account(server: &MockServer) -> ServiceAccount {
    let (private_pem, public_pem) = rsa_key_pair();
    let claims = Arc::new(Mutex::new(Vec::new()));
    Mock::given(method("POST"))
        .and(path(TOKEN_PATH))
        .respond_with(JwtExchange {
            public_pem,
            claims: Arc::clone(&claims),
        })
        .mount(server)
        .await;
    let dir = tempfile::tempdir().expect("tempdir");
    let key_path = dir.path().join("sa-key.json");
    std::fs::write(
        &key_path,
        service_account_key(
            &private_pem,
            CLIENT_EMAIL,
            &format!("{}{TOKEN_PATH}", server.uri()),
        ),
    )
    .expect("write key");
    ServiceAccount {
        key_path: key_path.to_string_lossy().into_owned(),
        _dir: dir,
        claims,
    }
}

/// A Cloud Logging page, with a next token when given.
fn entries(rows: Vec<Value>, next: Option<&str>) -> ResponseTemplate {
    let mut body = serde_json::Map::new();
    body.insert("entries".into(), Value::Array(rows));
    if let Some(next) = next {
        body.insert("nextPageToken".into(), Value::String(next.into()));
    }
    ResponseTemplate::new(200).set_body_json(Value::Object(body))
}

async fn mount_entries(server: &MockServer, rows: Vec<Value>) {
    Mock::given(method("POST"))
        .and(path(ENTRIES))
        .respond_with(entries(rows, None))
        .mount(server)
        .await;
}

/// One tick of the `gcp` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "gcp", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "gcp")).await
}

fn at(rfc3339: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(rfc3339)
        .expect("rfc3339")
        .with_timezone(&Utc)
}

fn fetch_window(start: &str, end: &str) -> FetchWindow {
    FetchWindow {
        start: at(start),
        end: at(end),
    }
}

/// The requests wiremock saw whose path is `at`, in order.
async fn requests_to(server: &MockServer, at: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.url.path() == at)
        .collect()
}

fn body_of(request: &Request) -> Value {
    serde_json::from_slice(&request.body).expect("a JSON body")
}

fn header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request.headers.get(name).and_then(|v| v.to_str().ok())
}

fn query_of(request: &Request) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = request
        .url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    pairs.sort();
    pairs
}

/// The form fields of a token exchange.
fn form_of(request: &Request) -> HashMap<String, String> {
    url::form_urlencoded::parse(&request.body)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

/// The `entries:list` body for one Cloud Logging unit's filter clause.
fn list_body(clause: &str, start: &str, end: &str) -> Value {
    json!({
        "resourceNames": ["projects/test-project"],
        "filter": format!("{clause} AND timestamp >= \"{start}\" AND timestamp < \"{end}\""),
        "pageSize": 100,
        "orderBy": "timestamp desc"
    })
}

fn entry(name: &str) -> Value {
    json!({"logName": format!("projects/test-project/logs/{name}"), "severity": "NOTICE", "insertId": name})
}

fn insert_ids(rows: &[Landed]) -> Vec<String> {
    rows.iter()
        .map(|r| r.record["insertId"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn test_gcp_disabled_returns_empty() {
    let server = MockServer::start().await;
    let mut cfg = project_config(&server, &["admin_activity"]);
    cfg.enabled = false;
    let (_, rows) = run(config(cfg), None).await;
    assert!(rows.is_empty());
    assert!(
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty()
    );
}

#[tokio::test]
async fn test_gcp_health_check_disabled() {
    let server = MockServer::start().await;
    let mut cfg = project_config(&server, &["admin_activity"]);
    cfg.enabled = false;
    assert!(!matches!(health(config(cfg)).await, Ok(true)));
}

/// No token, no key: the metadata server is the fallback, and off GCE it
/// is unreachable, so the health check is not healthy.
#[tokio::test]
async fn test_gcp_health_check_no_credentials() {
    let server = MockServer::start().await;
    let mut cfg = project_config(&server, &["admin_activity"]);
    cfg.credential_secret = None;
    cfg.service_account_key = None;
    assert!(!matches!(health(config(cfg)).await, Ok(true)));
}

/// An audit subtype: one `entries:list` POST with the project, its
/// documented `log_id` filter ANDed with the window in RFC 3339 (to the
/// millisecond when the window has one, `+00:00`), a page of 100 newest
/// first, the bearer, and every entry landed enriched on the type's topic.
#[tokio::test]
async fn test_gcp_fetch_admin_activity_success() {
    let server = MockServer::start().await;
    mount_entries(&server, vec![entry("activity-1"), entry("activity-2")]).await;
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(
        config(project_config(&server, &["admin_activity"])),
        Some(&w),
    )
    .await;
    outcome.expect("fetch");

    let seen = requests_to(&server, ENTRIES).await;
    assert_eq!(seen.len(), 1);
    assert_eq!(
        body_of(&seen[0]),
        list_body(
            "log_id(\"cloudaudit.googleapis.com/activity\")",
            "2026-05-21T13:30:00.987+00:00",
            "2026-05-21T14:30:00+00:00"
        )
    );
    assert_eq!(
        header(&seen[0], "authorization"),
        Some("Bearer mock-gcp-token")
    );
    assert_eq!(header(&seen[0], "content-type"), Some("application/json"));

    assert_eq!(rows.len(), 2);
    for (row, expected) in rows.iter().zip([entry("activity-1"), entry("activity-2")]) {
        assert_eq!(row.topic, "test-gcp_land");
        let e = enriched(row);
        assert_eq!(e.row, expected, "the provider's entry, semantically");
        assert_eq!(e.source, "test-gcp");
        assert_eq!(e.source_fetcher, "gcp.admin_activity");
    }
}

/// `nextPageToken` goes back in the next POST's body as `pageToken` beside
/// the unchanged filter.
#[tokio::test]
async fn test_gcp_fetch_admin_activity_pagination() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(ENTRIES))
        .respond_with(entries(vec![entry("audit-1")], Some("page2-token")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_entries(&server, vec![entry("audit-2"), entry("audit-3")]).await;

    let (outcome, rows) = run(config(project_config(&server, &["admin_activity"])), None).await;
    outcome.expect("fetch");
    assert_eq!(insert_ids(&rows), ["audit-1", "audit-2", "audit-3"]);
    let seen = requests_to(&server, ENTRIES).await;
    assert_eq!(seen.len(), 2);
    let first = body_of(&seen[0]);
    let second = body_of(&seen[1]);
    assert_eq!(second["pageToken"], "page2-token");
    assert_eq!(second["filter"], first["filter"]);
    assert_eq!(second["resourceNames"], first["resourceNames"]);
    assert!(first.get("pageToken").is_none());
}

/// Every Cloud Logging unit sends its documented filter clause and lands
/// under its own tag.
#[tokio::test]
async fn every_logging_unit_sends_its_documented_filter() {
    let server = MockServer::start().await;
    mount_entries(&server, vec![entry("e")]).await;
    let units = [
        (
            "admin_activity",
            "log_id(\"cloudaudit.googleapis.com/activity\")",
        ),
        (
            "data_access",
            "log_id(\"cloudaudit.googleapis.com/data_access\")",
        ),
        (
            "system_event",
            "log_id(\"cloudaudit.googleapis.com/system_event\")",
        ),
        (
            "policy_denied",
            "log_id(\"cloudaudit.googleapis.com/policy\")",
        ),
        (
            "vpc_flow_logs",
            "log_id(\"compute.googleapis.com/vpc_flows\")",
        ),
        ("dns_queries", "log_id(\"dns.googleapis.com/dns_queries\")"),
        (
            "storage_access",
            "resource.type=\"gcs_bucket\" AND log_id(\"cloudaudit.googleapis.com/data_access\")",
        ),
    ];
    let names: Vec<&str> = units.iter().map(|(name, _)| *name).collect();
    let (outcome, rows) = run(config(project_config(&server, &names)), None).await;
    outcome.expect("fetch");
    let mut tags: Vec<String> = rows.iter().map(|r| enriched(r).source_fetcher).collect();
    tags.sort();
    let mut expected: Vec<String> = names.iter().map(|n| format!("gcp.{n}")).collect();
    expected.sort();
    assert_eq!(tags, expected);
    let mut clauses: Vec<String> = requests_to(&server, ENTRIES)
        .await
        .iter()
        .map(|r| {
            body_of(r)["filter"]
                .as_str()
                .unwrap()
                .split(" AND timestamp >= ")
                .next()
                .unwrap()
                .to_owned()
        })
        .collect();
    clauses.sort();
    let mut documented: Vec<String> = units.iter().map(|(_, c)| (*c).to_owned()).collect();
    documented.sort();
    assert_eq!(clauses, documented);
}

/// Cloud Logging answers an empty window with an empty object, no
/// `entries` key at all: nothing lands and the tick is fine.
#[tokio::test]
async fn test_gcp_fetch_audit_logs_empty() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(ENTRIES))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(project_config(&server, &["admin_activity"])), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty());
    assert_eq!(requests_to(&server, ENTRIES).await.len(), 1);
}

/// SCC findings: a GET under the organisation the knob names, `pageSize`
/// 100, rows from `listFindingsResults`, `nextPageToken` fed back as the
/// `pageToken` query parameter.
#[tokio::test]
async fn test_gcp_fetch_scc_success() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(FINDINGS))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "listFindingsResults": [
                {"finding": {"name": "finding-1", "severity": "HIGH"}},
                {"finding": {"name": "finding-2", "severity": "MEDIUM"}}
            ],
            "nextPageToken": "scc-page-2",
            "totalSize": 3
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(FINDINGS))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "listFindingsResults": [{"finding": {"name": "finding-3", "severity": "LOW"}}],
            "totalSize": 3
        })))
        .mount(&server)
        .await;
    let mut cfg = project_config(&server, &[]);
    cfg.services = vec![service("scc", &[("organization_id", json!("123456789"))])];

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let names: Vec<&str> = rows
        .iter()
        .map(|r| r.record["finding"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["finding-1", "finding-2", "finding-3"]);
    assert_eq!(enriched(&rows[0]).source_fetcher, "gcp.scc");
    let seen = requests_to(&server, FINDINGS).await;
    assert_eq!(seen.len(), 2);
    assert_eq!(
        query_of(&seen[0]),
        [("pageSize".to_string(), "100".to_string())]
    );
    assert_eq!(
        query_of(&seen[1]),
        [
            ("pageSize".to_string(), "100".to_string()),
            ("pageToken".to_string(), "scc-page-2".to_string()),
        ]
    );
    assert_eq!(
        header(&seen[0], "authorization"),
        Some("Bearer mock-gcp-token")
    );
}

/// `cloud_logging` uses the `filter` knob, `severity >= WARNING` by default.
#[tokio::test]
async fn test_gcp_fetch_cloud_logging_success() {
    let server = MockServer::start().await;
    mount_entries(
        &server,
        vec![
            json!({"severity": "WARNING", "textPayload": "disk nearly full", "insertId": "w"}),
            json!({"severity": "ERROR", "textPayload": "connection timeout", "insertId": "e"}),
        ],
    )
    .await;
    let (outcome, rows) = run(config(project_config(&server, &["cloud_logging"])), None).await;
    outcome.expect("fetch");
    assert_eq!(insert_ids(&rows), ["w", "e"]);
    assert_eq!(enriched(&rows[0]).source_fetcher, "gcp.cloud_logging");
    let body = body_of(&requests_to(&server, ENTRIES).await[0]);
    assert!(
        body["filter"]
            .as_str()
            .unwrap()
            .starts_with("severity >= WARNING AND timestamp >= "),
        "{}",
        body["filter"]
    );

    let mut cfg = project_config(&server, &[]);
    cfg.services = vec![service(
        "cloud_logging",
        &[("filter", json!("resource.type=\"k8s_container\""))],
    )];
    run(config(cfg), None).await.0.expect("fetch");
    let body = body_of(&requests_to(&server, ENTRIES).await[1]);
    assert!(
        body["filter"]
            .as_str()
            .unwrap()
            .starts_with("resource.type=\"k8s_container\" AND timestamp >= "),
        "{}",
        body["filter"]
    );
}

#[tokio::test]
async fn without_a_window_the_last_hour_is_fetched() {
    let server = MockServer::start().await;
    mount_entries(&server, vec![]).await;
    let before = Utc::now();
    run(config(project_config(&server, &["system_event"])), None)
        .await
        .0
        .expect("fetch");
    let body = body_of(&requests_to(&server, ENTRIES).await[0]);
    let filter = body["filter"].as_str().unwrap();
    let (start, end) = filter
        .split_once(" AND timestamp >= \"")
        .and_then(|(_, rest)| rest.split_once("\" AND timestamp < \""))
        .map(|(s, e)| (at(s), at(e.trim_end_matches('"'))))
        .expect("the window clause");
    assert_eq!((end - start).num_seconds(), 3600, "one hour of lookback");
    assert!(
        end >= before - chrono::Duration::seconds(1) && end <= Utc::now(),
        "the window ends now: {end} vs {before}"
    );
}

/// A `credential_secret` spec resolves to the bearer token itself.
#[tokio::test]
async fn test_gcp_health_check_credential_secret() {
    let server = MockServer::start().await;
    mount_entries(&server, vec![entry("e")]).await;
    let healthy = health(config(project_config(&server, &["admin_activity"])))
        .await
        .expect("health");
    assert!(healthy);
    // nextest runs one process per test, so the variable leaks nowhere.
    unsafe { std::env::set_var("DFE_FETCHER_TEST_GCP_TOKEN", "token-from-env") };
    let mut cfg = project_config(&server, &["admin_activity"]);
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_GCP_TOKEN".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        header(&requests_to(&server, ENTRIES).await[0], "authorization"),
        Some("Bearer token-from-env")
    );
}

/// A service-account key file: an RS256 assertion signed with its key, with
/// `iss` the key's client email, the cloud-platform scope, `aud` the key's
/// `token_uri` (or the override), a one-hour lifetime; the exchange's token
/// is the bearer, minted once for the tick, and the exchange is the health
/// check.
#[tokio::test]
async fn a_service_account_key_is_exchanged_for_the_bearer() {
    let server = MockServer::start().await;
    let sa = service_account(&server).await;
    mount_entries(&server, vec![entry("e")]).await;
    let mut cfg = project_config(&server, &["admin_activity", "system_event"]);
    cfg.credential_secret = None;
    cfg.service_account_key = Some(sa.key_path.clone());

    let (outcome, rows) = run(config(cfg.clone()), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 2);
    let claims = sa.claims.lock().unwrap().clone();
    assert_eq!(claims.len(), 1, "one exchange for the tick");
    assert_eq!(claims[0]["iss"], CLIENT_EMAIL);
    assert_eq!(
        claims[0]["scope"],
        "https://www.googleapis.com/auth/cloud-platform"
    );
    assert_eq!(claims[0]["aud"], format!("{}{TOKEN_PATH}", server.uri()));
    assert!(claims[0].get("sub").is_none());
    assert_eq!(
        claims[0]["exp"].as_i64().unwrap() - claims[0]["iat"].as_i64().unwrap(),
        3600
    );
    for request in requests_to(&server, ENTRIES).await {
        assert_eq!(header(&request, "authorization"), Some("Bearer sa-token"));
    }
    assert!(health(config(cfg.clone())).await.expect("health"));

    // The token URL override wins over the key's token_uri.
    Mock::given(method("POST"))
        .and(path("/other/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "other-token",
            "expires_in": 3599
        })))
        .mount(&server)
        .await;
    cfg.token_url_override = Some(format!("{}/other/token", server.uri()));
    run(config(cfg), None).await.0.expect("fetch");
    assert_eq!(requests_to(&server, "/other/token").await.len(), 1);
    assert_eq!(
        header(
            requests_to(&server, ENTRIES).await.last().unwrap(),
            "authorization"
        ),
        Some("Bearer other-token")
    );
}

/// No token and no key: the workload's token comes from the GCE metadata
/// server with the `Metadata-Flavor` header it requires, once for the tick.
/// The typed block has no override for that host, so the instance's var is
/// pointed at wiremock here.
#[tokio::test]
async fn without_a_credential_the_metadata_server_supplies_the_token() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/computeMetadata/v1/instance/service-accounts/default/token",
        ))
        .and(wiremock::matchers::header("Metadata-Flavor", "Google"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "metadata-token",
            "expires_in": 3599,
            "token_type": "Bearer"
        })))
        .mount(&server)
        .await;
    mount_entries(&server, vec![entry("e")]).await;
    let mut cfg = project_config(&server, &["admin_activity", "system_event"]);
    cfg.credential_secret = None;
    let config = config(cfg);
    let mut built = crate::builtin_run::built_instance(&config, "gcp").unwrap();
    built.instance.vars.insert(
        "metadata_url".into(),
        Value::String(format!(
            "{}/computeMetadata/v1/instance/service-accounts/default/token",
            server.uri()
        )),
    );
    let (outcome, rows) = Box::pin(crate::builtin_run::run_instance(config, &built, None)).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        requests_to(
            &server,
            "/computeMetadata/v1/instance/service-accounts/default/token"
        )
        .await
        .len(),
        1,
        "one metadata token for the tick"
    );
    for request in requests_to(&server, ENTRIES).await {
        assert_eq!(
            header(&request, "authorization"),
            Some("Bearer metadata-token")
        );
    }
}

/// An assertion the token endpoint refuses: the tick fails with the
/// exchange's status and no data is requested.
#[tokio::test]
async fn a_refused_exchange_fails_the_tick_and_requests_no_data() {
    let server = MockServer::start().await;
    let sa = service_account(&server).await;
    mount_entries(&server, vec![entry("e")]).await;
    let (other_private, _) = rsa_key_pair();
    std::fs::write(
        &sa.key_path,
        service_account_key(
            &other_private,
            CLIENT_EMAIL,
            &format!("{}{TOKEN_PATH}", server.uri()),
        ),
    )
    .expect("rewrite key");
    let mut cfg = project_config(&server, &["admin_activity"]);
    cfg.credential_secret = None;
    cfg.service_account_key = Some(sa.key_path.clone());
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(rows.is_empty());
    assert!(requests_to(&server, ENTRIES).await.is_empty());
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(err.contains("401"), "{err}");
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let server = MockServer::start().await;
    mount_entries(
        &server,
        vec![
            json!({"insertId": "a", "severity": "INFO"}),
            json!({"insertId": "b", "severity": "ERROR"}),
            json!({"insertId": "c", "severity": "INFO"}),
        ],
    )
    .await;
    let mut cfg = project_config(&server, &["admin_activity"]);
    cfg.filter = Some("severity != \"INFO\"".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(insert_ids(&rows), ["b"]);
}

/// A 5xx the API keeps answering: the tick fails after the bounded
/// retries (the POST is a read) and nothing lands.
#[tokio::test]
async fn test_gcp_fetch_error_500() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(ENTRIES))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(project_config(&server, &["admin_activity"])), None).await;
    assert!(rows.is_empty(), "a failed service produces no records");
    assert_eq!(
        requests_to(&server, ENTRIES).await.len(),
        4,
        "the first attempt and three retries"
    );
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
}

/// A 429 with `Retry-After` is retried after the wait, and the entries
/// land; a 403 is never retried and its `error.message` is the text.
#[tokio::test]
async fn a_429_is_retried_and_a_403_is_not() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(ENTRIES))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_entries(&server, vec![entry("e")]).await;
    let (outcome, rows) = run(config(project_config(&server, &["admin_activity"])), None).await;
    outcome.expect("tick");
    assert_eq!(requests_to(&server, ENTRIES).await.len(), 2, "429 then 200");
    assert_eq!(rows.len(), 1);

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(ENTRIES))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {"code": 403, "message": "Permission 'logging.logEntries.list' denied", "status": "PERMISSION_DENIED"}
        })))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(project_config(&server, &["admin_activity"])), None).await;
    assert!(rows.is_empty());
    assert_eq!(requests_to(&server, ENTRIES).await.len(), 1);
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(
        err.contains("403") && err.contains("logging.logEntries.list"),
        "{err}"
    );
}

/// Two connections of the type carry their own project and token, and each
/// record carries its connection's `_source_fetcher` tag.
#[tokio::test]
async fn two_connections_poll_independently() {
    let server = MockServer::start().await;
    mount_entries(&server, vec![entry("e")]).await;
    let mut cfg = project_config(&server, &["admin_activity"]);
    cfg.project_id = None;
    cfg.credential_secret = None;
    cfg.connections = vec![
        GcpConnection {
            id: "gcp-prod".into(),
            project_id: Some("prod-project".into()),
            credential_secret: Some("token-prod".into()),
            ..GcpConnection::default()
        },
        GcpConnection {
            id: "gcp-dev".into(),
            project_id: Some("dev-project".into()),
            credential_secret: Some("token-dev".into()),
            ..GcpConnection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["gcp-prod", "gcp-dev"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        for row in rows {
            tags.push((id.to_string(), enriched(&row).source_fetcher));
        }
    }
    let seen = requests_to(&server, ENTRIES).await;
    assert_eq!(
        body_of(&seen[0])["resourceNames"],
        json!(["projects/prod-project"])
    );
    assert_eq!(header(&seen[0], "authorization"), Some("Bearer token-prod"));
    assert_eq!(
        body_of(&seen[1])["resourceNames"],
        json!(["projects/dev-project"])
    );
    assert_eq!(header(&seen[1], "authorization"), Some("Bearer token-dev"));
    assert_eq!(
        tags,
        [
            (
                "gcp-prod".to_string(),
                "gcp-prod.admin_activity".to_string()
            ),
            ("gcp-dev".to_string(), "gcp-dev.admin_activity".to_string()),
        ]
    );
}

/// A service the profile does not know, a Cloud Logging unit without a
/// project, or `scc` without its organisation is refused at validation
/// naming the field, instead of a warning every tick.
#[tokio::test]
async fn misconfiguration_is_refused_at_validation() {
    let server = MockServer::start().await;
    let cfg = project_config(&server, &["audit_logs"]);
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.gcp") && err.contains("audit_logs"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "{outcome:?}");
    assert!(rows.is_empty());
    assert!(
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty()
    );

    let mut cfg = project_config(&server, &["admin_activity"]);
    cfg.project_id = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.gcp") && err.contains("project_id"),
        "{err}"
    );

    let cfg = project_config(&server, &["scc"]);
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.gcp") && err.contains("organization_id"),
        "{err}"
    );
}
