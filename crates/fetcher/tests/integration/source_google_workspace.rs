// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_google_workspace.rs
// Purpose:   Characterisation of the Google Workspace Reports source: delegated JWT exchange, per-application activity
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Google Workspace Reports source against wiremock.
//!
//! Each test configures the typed `sources.google_workspace` block, runs
//! one tick through the real pipeline into scalo's memory transport, and
//! asserts on the service-account exchange wiremock recorded (an RS256
//! assertion with the admin as `sub` and the Reports scope, verified
//! against the test's public key), the activity requests per application
//! (`customerId`, the RFC 3339 window, `maxResults`, an optional
//! `eventName`, `nextPageToken` fed back as `pageToken`) and the records
//! that landed (the provider's activity, semantically, plus what
//! enrichment added). The typed config block is the operator's contract;
//! the shipped `google_workspace` profile serves it through the framework
//! driver.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use dfe_fetcher::config::{
    Config, GoogleWorkspaceConnection, GoogleWorkspaceService, GoogleWorkspaceSourceConfig,
};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::common::{rsa_key_pair, service_account_key};

const TOKEN_PATH: &str = "/token";
const CLIENT_EMAIL: &str = "reports@test-project.iam.gserviceaccount.com";
const REPORTS_SCOPE: &str = "https://www.googleapis.com/auth/admin.reports.audit.readonly";

fn activity_path(application: &str) -> String {
    format!("/admin/reports/v1/activity/users/all/applications/{application}")
}

/// A deployment config carrying `google_workspace` as its one source,
/// landing on `<topic>_land`, no dead-letter queue.
fn config(workspace: GoogleWorkspaceSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.google_workspace = workspace;
    config
}

/// The key the tests sign with and the exchange that verifies it.
struct Tenant {
    key_json: String,
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
                    "access_token": "ws-token",
                    "expires_in": 3599,
                    "token_type": "Bearer"
                }))
            }
            Err(_) => refused,
        }
    }
}

async fn tenant(server: &MockServer) -> Tenant {
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
    Tenant {
        key_json: service_account_key(
            &private_pem,
            CLIENT_EMAIL,
            "https://oauth2.googleapis.com/token",
        ),
        claims,
    }
}

/// The typed block an operator writes: the key JSON as the credential
/// spec, the admin to impersonate, the hosts pointed at wiremock, the
/// applications.
fn workspace_config(
    server: &MockServer,
    tenant: &Tenant,
    applications: &[&str],
) -> GoogleWorkspaceSourceConfig {
    GoogleWorkspaceSourceConfig {
        enabled: true,
        credential_secret: Some(tenant.key_json.clone()),
        admin_email: Some("audit-admin@example.com".into()),
        api_url_override: Some(server.uri()),
        token_url_override: Some(format!("{}{TOKEN_PATH}", server.uri())),
        services: applications.iter().map(|a| service(a, &[])).collect(),
        ..GoogleWorkspaceSourceConfig::default()
    }
}

fn service(name: &str, config: &[(&str, Value)]) -> GoogleWorkspaceService {
    GoogleWorkspaceService {
        name: name.into(),
        config: config
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    }
}

fn activity(id: &str, application: &str) -> Value {
    json!({
        "kind": "admin#reports#activity",
        "id": {"time": "2026-05-21T13:31:00.000Z", "uniqueQualifier": id, "applicationName": application, "customerId": "C0123"},
        "actor": {"email": "user@example.com", "profileId": "1"},
        "events": [{"type": "login", "name": "login_success"}]
    })
}

/// A page of activities, with a next token when given.
fn page(rows: Vec<Value>, next: Option<&str>) -> ResponseTemplate {
    let mut body = serde_json::Map::new();
    body.insert("kind".into(), json!("admin#reports#activities"));
    body.insert("items".into(), Value::Array(rows));
    if let Some(next) = next {
        body.insert("nextPageToken".into(), Value::String(next.into()));
    }
    ResponseTemplate::new(200).set_body_json(Value::Object(body))
}

async fn mount_page(server: &MockServer, application: &str, rows: Vec<Value>) {
    Mock::given(method("GET"))
        .and(path(activity_path(application)))
        .respond_with(page(rows, None))
        .mount(server)
        .await;
}

/// One tick of the `google_workspace` source as configured, through the
/// pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "google_workspace", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "google_workspace")).await
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

fn query_of(request: &Request) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = request
        .url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    pairs.sort();
    pairs
}

fn query_value(request: &Request, name: &str) -> Option<String> {
    request
        .url
        .query_pairs()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
}

fn header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request.headers.get(name).and_then(|v| v.to_str().ok())
}

fn form_of(request: &Request) -> HashMap<String, String> {
    url::form_urlencoded::parse(&request.body)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

fn qualifiers(rows: &[Landed]) -> Vec<String> {
    rows.iter()
        .map(|r| {
            r.record["id"]["uniqueQualifier"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

/// One application: the delegated exchange (the key's client email as
/// `iss`, the admin as `sub`, the Reports audit scope, the token URL as
/// `aud`, an hour), then the activity GET with the customer, the window in
/// RFC 3339 `+00:00`, a page of 1000, the bearer, and every activity
/// landed enriched under `google_workspace.<application>`.
#[tokio::test]
async fn login_activities_are_fetched_with_a_delegated_token() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    mount_page(
        &server,
        "login",
        vec![activity("q-1", "login"), activity("q-2", "login")],
    )
    .await;
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(
        config(workspace_config(&server, &tenant, &["login"])),
        Some(&w),
    )
    .await;
    outcome.expect("fetch");

    let claims = tenant.claims.lock().unwrap().clone();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0]["iss"], CLIENT_EMAIL);
    assert_eq!(claims[0]["sub"], "audit-admin@example.com");
    assert_eq!(claims[0]["scope"], REPORTS_SCOPE);
    assert_eq!(claims[0]["aud"], format!("{}{TOKEN_PATH}", server.uri()));
    assert_eq!(
        claims[0]["exp"].as_i64().unwrap() - claims[0]["iat"].as_i64().unwrap(),
        3600
    );

    let seen = requests_to(&server, &activity_path("login")).await;
    assert_eq!(seen.len(), 1);
    assert_eq!(
        query_of(&seen[0]),
        [
            ("customerId".to_string(), "my_customer".to_string()),
            (
                "endTime".to_string(),
                "2026-05-21T14:30:00+00:00".to_string()
            ),
            ("maxResults".to_string(), "1000".to_string()),
            (
                "startTime".to_string(),
                "2026-05-21T13:30:00.987+00:00".to_string()
            ),
        ]
    );
    assert_eq!(header(&seen[0], "authorization"), Some("Bearer ws-token"));

    assert_eq!(rows.len(), 2);
    for (row, expected) in rows
        .iter()
        .zip([activity("q-1", "login"), activity("q-2", "login")])
    {
        assert_eq!(row.topic, "google_workspace_land");
        let e = enriched(row);
        assert_eq!(e.row, expected, "the provider's activity, semantically");
        assert_eq!(e.source, "google_workspace");
        assert_eq!(e.source_fetcher, "google_workspace.login");
    }
}

/// `nextPageToken` goes back as the `pageToken` query parameter beside the
/// unchanged window.
#[tokio::test]
async fn paging_follows_the_next_page_token() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    Mock::given(method("GET"))
        .and(path(activity_path("admin")))
        .respond_with(page(vec![activity("q-1", "admin")], Some("tok-2")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_page(
        &server,
        "admin",
        vec![activity("q-2", "admin"), activity("q-3", "admin")],
    )
    .await;

    let (outcome, rows) = run(config(workspace_config(&server, &tenant, &["admin"])), None).await;
    outcome.expect("fetch");
    assert_eq!(qualifiers(&rows), ["q-1", "q-2", "q-3"]);
    let seen = requests_to(&server, &activity_path("admin")).await;
    assert_eq!(seen.len(), 2);
    assert_eq!(query_value(&seen[1], "pageToken").as_deref(), Some("tok-2"));
    assert_eq!(
        query_value(&seen[1], "startTime"),
        query_value(&seen[0], "startTime")
    );
    assert!(query_value(&seen[0], "pageToken").is_none());
}

/// Each configured application is its own unit under its own tag, an
/// `event_name` knob narrows one of them, and the customer id is the
/// block's when set.
#[tokio::test]
async fn each_application_is_fetched_under_its_own_tag_with_its_knobs() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    mount_page(&server, "login", vec![activity("l-1", "login")]).await;
    mount_page(
        &server,
        "drive",
        vec![activity("d-1", "drive"), activity("d-2", "drive")],
    )
    .await;
    let mut cfg = workspace_config(&server, &tenant, &[]);
    cfg.customer_id = Some("C0123abcd".into());
    cfg.services = vec![
        service("login", &[]),
        service("drive", &[("event_name", json!("download"))]),
    ];

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let mut tags: Vec<(String, String)> = rows
        .iter()
        .map(|r| {
            (
                enriched(r).source_fetcher,
                r.record["id"]["uniqueQualifier"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            )
        })
        .collect();
    tags.sort();
    assert_eq!(
        tags,
        [
            ("google_workspace.drive".to_string(), "d-1".to_string()),
            ("google_workspace.drive".to_string(), "d-2".to_string()),
            ("google_workspace.login".to_string(), "l-1".to_string()),
        ]
    );
    let login = requests_to(&server, &activity_path("login")).await;
    assert_eq!(
        query_value(&login[0], "customerId").as_deref(),
        Some("C0123abcd")
    );
    assert!(query_value(&login[0], "eventName").is_none());
    let drive = requests_to(&server, &activity_path("drive")).await;
    assert_eq!(
        query_value(&drive[0], "eventName").as_deref(),
        Some("download")
    );
    assert_eq!(
        tenant.claims.lock().unwrap().len(),
        1,
        "one delegated token for the tick"
    );
}

#[tokio::test]
async fn without_a_window_the_last_hour_is_fetched() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    mount_page(&server, "login", vec![]).await;
    let before = Utc::now();
    let (outcome, rows) = run(config(workspace_config(&server, &tenant, &["login"])), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty());
    let seen = requests_to(&server, &activity_path("login")).await;
    let start = at(&query_value(&seen[0], "startTime").unwrap());
    let end = at(&query_value(&seen[0], "endTime").unwrap());
    assert_eq!((end - start).num_seconds(), 3600, "one hour of lookback");
    assert!(
        end >= before - chrono::Duration::seconds(1) && end <= Utc::now(),
        "the window ends now: {end} vs {before}"
    );
}

/// The key may come from a file the block names by path instead of the
/// credential spec.
#[tokio::test]
async fn a_service_account_key_file_is_read_for_the_exchange() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    mount_page(&server, "login", vec![activity("q-1", "login")]).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let key_path = dir.path().join("ws-key.json");
    std::fs::write(&key_path, &tenant.key_json).expect("write key");
    let mut cfg = workspace_config(&server, &tenant, &["login"]);
    cfg.credential_secret = None;
    cfg.service_account_key = Some(key_path.to_string_lossy().into_owned());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        tenant.claims.lock().unwrap()[0]["sub"],
        "audit-admin@example.com"
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    mount_page(
        &server,
        "login",
        vec![
            json!({"id": {"uniqueQualifier": "a"}, "actor": {"email": "alice@example.com"}}),
            json!({"id": {"uniqueQualifier": "b"}, "actor": {"email": "bob@example.com"}}),
        ],
    )
    .await;
    let mut cfg = workspace_config(&server, &tenant, &["login"]);
    cfg.filter = Some("actor.email == \"bob@example.com\"".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(qualifiers(&rows), ["b"]);
}

/// A 5xx the API keeps answering: the tick fails after the bounded
/// retries and nothing lands.
#[tokio::test]
async fn a_server_error_fails_the_tick_after_retries_and_lands_nothing() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    Mock::given(method("GET"))
        .and(path(activity_path("login")))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(workspace_config(&server, &tenant, &["login"])), None).await;
    assert!(rows.is_empty());
    assert_eq!(
        requests_to(&server, &activity_path("login")).await.len(),
        4,
        "the first attempt and three retries"
    );
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
}

/// A 403 (the admin lacks the Reports privilege, or delegation is not
/// granted) is never retried and its `error.message` is the text.
#[tokio::test]
async fn a_refusal_is_never_retried() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    Mock::given(method("GET"))
        .and(path(activity_path("login")))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {"code": 403, "message": "Not Authorized to access this resource/api", "status": "PERMISSION_DENIED"}
        })))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(workspace_config(&server, &tenant, &["login"])), None).await;
    assert!(rows.is_empty());
    assert_eq!(requests_to(&server, &activity_path("login")).await.len(), 1);
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(
        err.contains("403") && err.contains("Not Authorized"),
        "{err}"
    );
}

/// An assertion the token endpoint refuses: the tick fails with the
/// exchange's status and no activity is requested.
#[tokio::test]
async fn a_refused_exchange_fails_the_tick_and_requests_no_data() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    mount_page(&server, "login", vec![activity("q-1", "login")]).await;
    let (other_private, _) = rsa_key_pair();
    let mut cfg = workspace_config(&server, &tenant, &["login"]);
    cfg.credential_secret = Some(service_account_key(
        &other_private,
        CLIENT_EMAIL,
        "https://oauth2.googleapis.com/token",
    ));
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(rows.is_empty());
    assert!(
        requests_to(&server, &activity_path("login"))
            .await
            .is_empty()
    );
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(err.contains("401"), "{err}");
}

/// Two connections carry their own admin and key, and each record carries
/// its connection's `_source_fetcher` tag.
#[tokio::test]
async fn two_connections_poll_independently() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    mount_page(&server, "login", vec![activity("q-1", "login")]).await;
    let mut cfg = workspace_config(&server, &tenant, &["login"]);
    cfg.credential_secret = None;
    cfg.admin_email = None;
    cfg.connections = vec![
        GoogleWorkspaceConnection {
            id: "ws-corp".into(),
            credential_secret: Some(tenant.key_json.clone()),
            admin_email: Some("admin@corp.example".into()),
            ..GoogleWorkspaceConnection::default()
        },
        GoogleWorkspaceConnection {
            id: "ws-lab".into(),
            credential_secret: Some(tenant.key_json.clone()),
            admin_email: Some("admin@lab.example".into()),
            customer_id: Some("C0lab".into()),
            ..GoogleWorkspaceConnection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["ws-corp", "ws-lab"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        for row in rows {
            tags.push((id.to_string(), enriched(&row).source_fetcher));
        }
    }
    let claims = tenant.claims.lock().unwrap().clone();
    assert_eq!(claims[0]["sub"], "admin@corp.example");
    assert_eq!(claims[1]["sub"], "admin@lab.example");
    let seen = requests_to(&server, &activity_path("login")).await;
    assert_eq!(
        query_value(&seen[0], "customerId").as_deref(),
        Some("my_customer")
    );
    assert_eq!(
        query_value(&seen[1], "customerId").as_deref(),
        Some("C0lab")
    );
    assert_eq!(
        tags,
        [
            ("ws-corp".to_string(), "ws-corp.login".to_string()),
            ("ws-lab".to_string(), "ws-lab.login".to_string()),
        ]
    );
}

/// A missing admin email or key, or an application name the Reports API
/// does not document, is refused at validation naming the field.
#[tokio::test]
async fn misconfiguration_is_refused_at_validation() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    let mut cfg = workspace_config(&server, &tenant, &["login"]);
    cfg.admin_email = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.google_workspace") && err.contains("admin_email"),
        "{err}"
    );
    let mut cfg = workspace_config(&server, &tenant, &["login"]);
    cfg.credential_secret = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("service_account_key") && err.contains("credential_secret"),
        "{err}"
    );
    let cfg = workspace_config(&server, &tenant, &["sheets"]);
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.google_workspace") && err.contains("sheets"),
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
}

/// The health check is the delegated token exchange: a minted token is
/// healthy, a refused one is the error, and no activity is requested.
#[tokio::test]
async fn the_health_check_is_the_delegated_exchange() {
    let server = MockServer::start().await;
    let tenant = tenant(&server).await;
    let healthy = health(config(workspace_config(&server, &tenant, &["login"])))
        .await
        .expect("health");
    assert!(healthy);
    assert_eq!(tenant.claims.lock().unwrap().len(), 1);
    assert!(
        requests_to(&server, &activity_path("login"))
            .await
            .is_empty()
    );

    let (other_private, _) = rsa_key_pair();
    let mut cfg = workspace_config(&server, &tenant, &["login"]);
    cfg.credential_secret = Some(service_account_key(
        &other_private,
        CLIENT_EMAIL,
        "https://oauth2.googleapis.com/token",
    ));
    assert!(!matches!(health(config(cfg)).await, Ok(true)));
}
