// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_azure.rs
// Purpose:   Characterisation of the Azure source: token audiences, ARM and Graph paging, the record shape
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Azure source against wiremock.
//!
//! Each test configures the typed `sources.azure` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the token exchanges wiremock recorded (one scope per API audience:
//! Management, Graph, Log Analytics), the requests each service sent (path,
//! `api-version`, the OData window filter, the bearer of the right
//! audience, `nextLink` and `@odata.nextLink` paging) and the records that
//! landed (the provider's row, semantically, plus what enrichment added).
//! The typed config block is the operator's contract; the shipped `azure`
//! profile serves it through the framework driver.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use dfe_fetcher::config::{AzureConnection, AzureService, AzureSourceConfig, Config};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};

const TOKEN_PATH: &str = "/oauth2/v2.0/token";
const ACTIVITY_LOG: &str =
    "/subscriptions/test-sub-id/providers/microsoft.insights/eventtypes/management/values";
const DEFENDER: &str = "/subscriptions/test-sub-id/providers/Microsoft.Security/alerts";
const SIGNINS: &str = "/v1.0/auditLogs/signIns";
const DIRECTORY_AUDITS: &str = "/v1.0/auditLogs/directoryAudits";
const PROVISIONING: &str = "/v1.0/auditLogs/provisioning";

/// A deployment config carrying `azure` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(azure: AzureSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.azure = azure;
    config
}

/// The typed block an operator writes: the tenant, the service principal,
/// the subscription, every API host pointed at wiremock, and the services.
fn tenant_config(server: &MockServer, services: &[&str]) -> AzureSourceConfig {
    AzureSourceConfig {
        enabled: true,
        tenant_id: Some("test-tenant".into()),
        client_id: Some("test-client-id".into()),
        client_secret: Some("test-client-secret".to_string().into()),
        subscription_id: Some("test-sub-id".into()),
        management_url_override: Some(server.uri()),
        graph_url_override: Some(server.uri()),
        token_url_override: Some(format!("{}{TOKEN_PATH}", server.uri())),
        services: services.iter().map(|s| service(s, &[])).collect(),
        ..AzureSourceConfig::default()
    }
}

fn service(name: &str, config: &[(&str, Value)]) -> AzureService {
    AzureService {
        name: name.into(),
        config: config
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    }
}

/// The token endpoint: a bearer that names the audience it was minted for,
/// so a data request proves which token it carried.
struct ScopedToken;

impl Respond for ScopedToken {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let form = form_of(request);
        let scope = form.get("scope").map(String::as_str).unwrap_or("none");
        let audience = scope
            .trim_start_matches("https://")
            .split('/')
            .next()
            .unwrap_or("none");
        ResponseTemplate::new(200).set_body_json(json!({
            "access_token": format!("tok-{audience}"),
            "token_type": "Bearer",
            "expires_in": 3599
        }))
    }
}

async fn mount_token(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(TOKEN_PATH))
        .respond_with(ScopedToken)
        .mount(server)
        .await;
}

/// A page of `value` rows, with a next link when given.
fn page(rows: Vec<Value>, next: Option<(&str, String)>) -> ResponseTemplate {
    let mut body = serde_json::Map::new();
    body.insert("value".into(), Value::Array(rows));
    if let Some((key, url)) = next {
        body.insert(key.into(), Value::String(url));
    }
    ResponseTemplate::new(200).set_body_json(Value::Object(body))
}

async fn mount_page(server: &MockServer, at: &str, rows: Vec<Value>) {
    Mock::given(method("GET"))
        .and(path(at))
        .respond_with(page(rows, None))
        .mount(server)
        .await;
}

/// One tick of the `azure` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "azure", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "azure")).await
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

/// The decoded query of a request, sorted by name.
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

/// The form fields of a token exchange.
fn form_of(request: &Request) -> HashMap<String, String> {
    url::form_urlencoded::parse(&request.body)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

/// The scopes exchanged so far, in order.
async fn scopes(server: &MockServer) -> Vec<String> {
    requests_to(server, TOKEN_PATH)
        .await
        .iter()
        .map(|r| form_of(r).remove("scope").unwrap_or_default())
        .collect()
}

fn event(id: &str) -> Value {
    json!({"id": id, "operationName": {"value": "Microsoft.Compute/virtualMachines/write"}, "level": "Informational"})
}

fn ids(rows: &[Landed]) -> Vec<String> {
    rows.iter()
        .map(|r| r.record["id"].as_str().unwrap().to_owned())
        .collect()
}

// Wiremock-backed tests: the token endpoint and every API host are the same
// mock server, told apart by path.

#[tokio::test]
async fn test_azure_disabled_returns_empty() {
    let server = MockServer::start().await;
    let mut cfg = tenant_config(&server, &["activity_log"]);
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
async fn test_azure_health_check_disabled() {
    let server = MockServer::start().await;
    let mut cfg = tenant_config(&server, &["activity_log"]);
    cfg.enabled = false;
    assert!(!matches!(health(config(cfg)).await, Ok(true)));
}

#[tokio::test]
async fn test_azure_missing_tenant_id() {
    let server = MockServer::start().await;
    let mut cfg = tenant_config(&server, &["activity_log"]);
    cfg.tenant_id = None;
    assert!(!matches!(health(config(cfg)).await, Ok(true)));
}

/// The Activity Log: one Management-audience token exchange, the ARM path
/// under the subscription with `api-version` and the OData window filter to
/// the second, the bearer of that audience, and every event landed
/// enriched on the type's topic.
#[tokio::test]
async fn test_azure_fetch_activity_log_success() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_page(&server, ACTIVITY_LOG, vec![event("evt-1"), event("evt-2")]).await;
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-22T13:30:00Z");

    let (outcome, rows) = run(config(tenant_config(&server, &["activity_log"])), Some(&w)).await;
    outcome.expect("fetch");

    let exchanges = requests_to(&server, TOKEN_PATH).await;
    assert_eq!(exchanges.len(), 1, "one Management token for the tick");
    let form = form_of(&exchanges[0]);
    assert_eq!(
        form.get("grant_type").map(String::as_str),
        Some("client_credentials")
    );
    assert_eq!(
        form.get("client_id").map(String::as_str),
        Some("test-client-id")
    );
    assert_eq!(
        form.get("client_secret").map(String::as_str),
        Some("test-client-secret")
    );
    assert_eq!(
        form.get("scope").map(String::as_str),
        Some("https://management.azure.com/.default")
    );

    let seen = requests_to(&server, ACTIVITY_LOG).await;
    assert_eq!(seen.len(), 1);
    assert_eq!(
        query_of(&seen[0]),
        [
            (
                "$filter".to_string(),
                "eventTimestamp ge '2026-05-21T13:30:00Z' and eventTimestamp le '2026-05-22T13:30:00Z'"
                    .to_string()
            ),
            ("api-version".to_string(), "2015-04-01".to_string()),
        ],
        "the window to the second, quoted, as ARM's $filter"
    );
    assert_eq!(
        header(&seen[0], "authorization"),
        Some("Bearer tok-management.azure.com")
    );

    assert_eq!(rows.len(), 2);
    for (row, expected) in rows.iter().zip([event("evt-1"), event("evt-2")]) {
        assert_eq!(row.topic, "azure_land");
        let e = enriched(row);
        assert_eq!(e.row, expected, "the provider's event, semantically");
        assert_eq!(e.source, "azure");
        assert_eq!(e.source_fetcher, "azure.activity_log");
    }
}

/// ARM pages with an absolute `nextLink`, which is followed as given.
#[tokio::test]
async fn test_azure_fetch_activity_log_pagination() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("GET"))
        .and(path(ACTIVITY_LOG))
        .respond_with(page(
            vec![event("evt-1")],
            Some(("nextLink", format!("{}/page2?skiptoken=abc", server.uri()))),
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/page2"))
        .respond_with(page(vec![event("evt-2"), event("evt-3")], None))
        .mount(&server)
        .await;

    let (outcome, rows) = run(config(tenant_config(&server, &["activity_log"])), None).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["evt-1", "evt-2", "evt-3"]);
    let second = requests_to(&server, "/page2").await;
    assert_eq!(second.len(), 1);
    assert_eq!(query_value(&second[0], "skiptoken").as_deref(), Some("abc"));
    assert_eq!(
        header(&second[0], "authorization"),
        Some("Bearer tok-management.azure.com")
    );
}

#[tokio::test]
async fn test_azure_fetch_activity_log_empty() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_page(&server, ACTIVITY_LOG, vec![]).await;
    let (outcome, rows) = run(config(tenant_config(&server, &["activity_log"])), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty());
}

/// Defender alerts and Sentinel incidents list under the subscription with
/// their own `api-version`; Sentinel's resource group and workspace come
/// from the service knobs, `default` when unset.
#[tokio::test]
async fn test_azure_fetch_defender_success() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_page(
        &server,
        DEFENDER,
        vec![
            json!({"id": "alert-1", "properties": {"severity": "High"}}),
            json!({"id": "alert-2", "properties": {"severity": "Medium"}}),
        ],
    )
    .await;
    let (outcome, rows) = run(config(tenant_config(&server, &["defender"])), None).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["alert-1", "alert-2"]);
    assert_eq!(enriched(&rows[0]).source_fetcher, "azure.defender");
    let seen = requests_to(&server, DEFENDER).await;
    assert_eq!(
        query_of(&seen[0]),
        [("api-version".to_string(), "2022-01-01".to_string())]
    );
    assert_eq!(
        header(&seen[0], "authorization"),
        Some("Bearer tok-management.azure.com")
    );
}

#[tokio::test]
async fn sentinel_incidents_list_under_the_workspace_the_knobs_name() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    let named = "/subscriptions/test-sub-id/resourceGroups/rg-sec/providers/Microsoft.OperationalInsights/workspaces/ws-sentinel/providers/Microsoft.SecurityInsights/incidents";
    mount_page(&server, named, vec![json!({"id": "inc-1"})]).await;
    let mut cfg = tenant_config(&server, &[]);
    cfg.services = vec![service(
        "sentinel",
        &[
            ("resource_group", json!("rg-sec")),
            ("workspace_name", json!("ws-sentinel")),
        ],
    )];
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["inc-1"]);
    assert_eq!(enriched(&rows[0]).source_fetcher, "azure.sentinel");
    let seen = requests_to(&server, named).await;
    assert_eq!(
        query_of(&seen[0]),
        [("api-version".to_string(), "2023-11-01".to_string())]
    );

    let defaulted = "/subscriptions/test-sub-id/resourceGroups/default/providers/Microsoft.OperationalInsights/workspaces/default/providers/Microsoft.SecurityInsights/incidents";
    mount_page(&server, defaulted, vec![]).await;
    run(config(tenant_config(&server, &["sentinel"])), None)
        .await
        .0
        .expect("fetch");
    assert_eq!(
        requests_to(&server, defaulted).await.len(),
        1,
        "`default` stands in for an unset group and workspace"
    );
}

/// The three Entra units are separate Graph audit endpoints under one
/// Graph-audience token: `$top=100`, the window as an OData clause on each
/// endpoint's own time field, each tagged with its own service name.
#[tokio::test]
async fn test_azure_fetch_entra_split_services_success() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_page(&server, SIGNINS, vec![json!({"id": "signin-1"})]).await;
    mount_page(
        &server,
        DIRECTORY_AUDITS,
        vec![json!({"id": "audit-1"}), json!({"id": "audit-2"})],
    )
    .await;
    mount_page(&server, PROVISIONING, vec![json!({"id": "prov-1"})]).await;
    let w = fetch_window("2026-05-21T13:30:00Z", "2026-05-22T13:30:00Z");

    let (outcome, rows) = run(
        config(tenant_config(
            &server,
            &[
                "entra_signins",
                "entra_directory_audits",
                "entra_provisioning",
            ],
        )),
        Some(&w),
    )
    .await;
    outcome.expect("fetch");

    let mut by_source: HashMap<String, Vec<String>> = HashMap::new();
    for row in &rows {
        by_source
            .entry(enriched(row).source_fetcher)
            .or_default()
            .push(row.record["id"].as_str().unwrap().to_owned());
    }
    assert_eq!(by_source["azure.entra_signins"], ["signin-1"]);
    assert_eq!(
        by_source["azure.entra_directory_audits"],
        ["audit-1", "audit-2"]
    );
    assert_eq!(by_source["azure.entra_provisioning"], ["prov-1"]);

    for (at, field) in [
        (SIGNINS, "createdDateTime"),
        (DIRECTORY_AUDITS, "activityDateTime"),
        (PROVISIONING, "activityDateTime"),
    ] {
        let seen = requests_to(&server, at).await;
        assert_eq!(seen.len(), 1, "{at}");
        assert_eq!(
            query_of(&seen[0]),
            [
                (
                    "$filter".to_string(),
                    format!("{field} ge 2026-05-21T13:30:00Z and {field} lt 2026-05-22T13:30:00Z")
                ),
                ("$top".to_string(), "100".to_string()),
            ],
            "{at}"
        );
        assert_eq!(
            header(&seen[0], "authorization"),
            Some("Bearer tok-graph.microsoft.com"),
            "{at}: the Graph audience"
        );
    }
    assert!(
        scopes(&server)
            .await
            .contains(&"https://graph.microsoft.com/.default".to_string())
    );
}

/// Graph pages with `@odata.nextLink`, followed as given.
#[tokio::test]
async fn entra_signins_follow_the_odata_next_link() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("GET"))
        .and(path(SIGNINS))
        .respond_with(page(
            vec![json!({"id": "signin-1"})],
            Some((
                "@odata.nextLink",
                format!("{SIGNINS}?$skiptoken=xyz")
                    .replace(SIGNINS, &format!("{}{SIGNINS}", server.uri())),
            )),
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(SIGNINS))
        .respond_with(page(vec![json!({"id": "signin-2"})], None))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(tenant_config(&server, &["entra_signins"])), None).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["signin-1", "signin-2"]);
    let seen = requests_to(&server, SIGNINS).await;
    assert_eq!(seen.len(), 2);
    assert_eq!(query_value(&seen[1], "$skiptoken").as_deref(), Some("xyz"));
}

#[tokio::test]
async fn without_a_window_the_last_day_is_fetched() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_page(&server, ACTIVITY_LOG, vec![]).await;
    let before = Utc::now();
    run(config(tenant_config(&server, &["activity_log"])), None)
        .await
        .0
        .expect("fetch");
    let filter = query_value(&requests_to(&server, ACTIVITY_LOG).await[0], "$filter").unwrap();
    let (start, end) = filter
        .strip_prefix("eventTimestamp ge '")
        .and_then(|f| f.split_once("' and eventTimestamp le '"))
        .map(|(s, e)| (at(s), at(e.trim_end_matches('\''))))
        .expect("the window clause");
    assert_eq!((end - start).num_hours(), 24, "one day of lookback");
    assert!(
        end >= before - chrono::Duration::seconds(1) && end <= Utc::now(),
        "the window ends now: {end} vs {before}"
    );
}

/// A `credential_secret` spec is resolved for the exchange; `client_id`
/// stays the literal id the block names.
#[tokio::test]
async fn a_credential_secret_spec_is_resolved_as_the_client_secret() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_page(&server, ACTIVITY_LOG, vec![event("evt-1")]).await;
    // nextest runs one process per test, so the variable leaks nowhere.
    unsafe { std::env::set_var("DFE_FETCHER_TEST_AZURE_SECRET", "secret-from-env") };
    let mut cfg = tenant_config(&server, &["activity_log"]);
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_AZURE_SECRET".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    let form = form_of(&requests_to(&server, TOKEN_PATH).await[0]);
    assert_eq!(
        form.get("client_secret").map(String::as_str),
        Some("secret-from-env")
    );
    assert_eq!(
        form.get("client_id").map(String::as_str),
        Some("test-client-id")
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_page(
        &server,
        ACTIVITY_LOG,
        vec![
            json!({"id": "a", "level": "Critical"}),
            json!({"id": "b", "level": "Informational"}),
            json!({"id": "c", "level": "Error"}),
        ],
    )
    .await;
    let mut cfg = tenant_config(&server, &["activity_log"]);
    cfg.filter = Some("level == \"Critical\" || level == \"Error\"".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["a", "c"]);
}

/// A 5xx the API keeps answering: the tick fails after the bounded
/// retries and nothing lands.
#[tokio::test]
async fn test_azure_fetch_error_500() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("GET"))
        .and(path(ACTIVITY_LOG))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(tenant_config(&server, &["activity_log"])), None).await;
    assert!(rows.is_empty(), "a failed service produces no records");
    assert_eq!(
        requests_to(&server, ACTIVITY_LOG).await.len(),
        4,
        "the first attempt and three retries"
    );
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
}

/// A 429 with `Retry-After` is retried after the wait, and the events land.
#[tokio::test]
async fn a_429_with_retry_after_is_retried() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("GET"))
        .and(path(ACTIVITY_LOG))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_page(&server, ACTIVITY_LOG, vec![event("evt-1")]).await;
    let (outcome, rows) = run(config(tenant_config(&server, &["activity_log"])), None).await;
    outcome.expect("tick");
    assert_eq!(
        requests_to(&server, ACTIVITY_LOG).await.len(),
        2,
        "429 then 200"
    );
    assert_eq!(rows.len(), 1);
}

/// A 403 is never retried: the tick fails on the one request.
#[tokio::test]
async fn a_refusal_is_never_retried() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("GET"))
        .and(path(SIGNINS))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {"code": "Authorization_RequestDenied", "message": "Insufficient privileges to complete the operation."}
        })))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(tenant_config(&server, &["entra_signins"])), None).await;
    assert!(rows.is_empty());
    assert_eq!(requests_to(&server, SIGNINS).await.len(), 1);
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(
        err.contains("403") && err.contains("Insufficient privileges"),
        "the error text comes from error.message: {err}"
    );
}

/// A refused token exchange: the tick fails with the exchange's status and
/// no data is requested.
#[tokio::test]
async fn a_refused_token_exchange_fails_the_tick_and_requests_no_data() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(TOKEN_PATH))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "error": "invalid_client",
            "error_description": "Invalid client credentials"
        })))
        .mount(&server)
        .await;
    mount_page(&server, ACTIVITY_LOG, vec![event("evt-1")]).await;
    let (outcome, rows) = run(config(tenant_config(&server, &["activity_log"])), None).await;
    assert!(rows.is_empty());
    assert_eq!(requests_to(&server, TOKEN_PATH).await.len(), 1);
    assert!(requests_to(&server, ACTIVITY_LOG).await.is_empty());
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(err.contains("401"), "{err}");
}

/// Two connections of the type exchange their own tenant's credentials, and
/// each record carries its connection's `_source_fetcher` tag
/// (`<connection>.<unit>`), so the connection is distinguishable on the
/// record.
#[tokio::test]
async fn two_connections_poll_independently() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_page(&server, ACTIVITY_LOG, vec![event("evt-1")]).await;
    let mut cfg = tenant_config(&server, &["activity_log"]);
    cfg.tenant_id = None;
    cfg.client_id = None;
    cfg.client_secret = None;
    cfg.connections = vec![
        AzureConnection {
            id: "az-prod".into(),
            tenant_id: Some("tenant-prod".into()),
            client_id: Some("client-prod".into()),
            client_secret: Some("secret-prod".to_string().into()),
            ..AzureConnection::default()
        },
        AzureConnection {
            id: "az-dev".into(),
            tenant_id: Some("tenant-dev".into()),
            client_id: Some("client-dev".into()),
            client_secret: Some("secret-dev".to_string().into()),
            ..AzureConnection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["az-prod", "az-dev"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        for row in rows {
            tags.push((id.to_string(), enriched(&row).source_fetcher));
        }
    }
    let exchanges = requests_to(&server, TOKEN_PATH).await;
    assert_eq!(
        form_of(&exchanges[0]).get("client_id").map(String::as_str),
        Some("client-prod")
    );
    assert_eq!(
        form_of(&exchanges[1]).get("client_id").map(String::as_str),
        Some("client-dev")
    );
    assert_eq!(
        tags,
        [
            ("az-prod".to_string(), "az-prod.activity_log".to_string()),
            ("az-dev".to_string(), "az-dev.activity_log".to_string()),
        ]
    );
}

/// A service the profile does not know is refused at validation by name,
/// instead of being skipped with a warning on every tick.
#[tokio::test]
async fn an_unknown_service_name_is_refused_at_validation() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    let cfg = tenant_config(&server, &["entra_id"]);
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.azure") && err.contains("entra_id"),
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

/// A missing tenant, a missing client secret, or a subscription-scoped
/// service without a subscription is refused at validation naming the
/// field, instead of failing silently every tick; the Graph units need no
/// subscription.
#[tokio::test]
async fn missing_identity_is_refused_at_validation() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    let mut cfg = tenant_config(&server, &["activity_log"]);
    cfg.tenant_id = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.azure") && err.contains("tenant_id"),
        "{err}"
    );
    let mut cfg = tenant_config(&server, &["activity_log"]);
    cfg.client_secret = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.azure") && err.contains("credential_secret"),
        "{err}"
    );
    let mut cfg = tenant_config(&server, &["defender"]);
    cfg.subscription_id = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.azure") && err.contains("subscription_id"),
        "{err}"
    );
    mount_page(&server, SIGNINS, vec![json!({"id": "signin-1"})]).await;
    let mut cfg = tenant_config(&server, &["entra_signins"]);
    cfg.subscription_id = None;
    config(cfg.clone())
        .validate()
        .expect("Graph needs no subscription");
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
}

/// Log Analytics: one POST per configured `log_analytics` service to that
/// service's workspace with the KQL and the window as the `timespan`, under
/// a Log Analytics-audience token, each result table's rows built into one
/// object per row keyed by column, all tagged `azure.log_analytics`. The
/// typed block has no override for this host, so the instance's var is
/// pointed at wiremock here.
#[tokio::test]
async fn log_analytics_posts_each_query_and_builds_rows_from_the_tables() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("POST"))
        .and(path("/v1/workspaces/ws-1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"tables": [{
            "name": "PrimaryResult",
            "columns": [{"name": "TimeGenerated", "type": "datetime"}, {"name": "Computer", "type": "string"}],
            "rows": [["2026-05-21T13:31:00Z", "web-1"], ["2026-05-21T13:32:00Z", "web-2"]]
        }]})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/workspaces/ws-2/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"tables": [{
            "name": "PrimaryResult",
            "columns": [{"name": "Account", "type": "string"}],
            "rows": [["alice"]]
        }]})))
        .mount(&server)
        .await;
    let w = fetch_window("2026-05-21T13:30:00Z", "2026-05-22T13:30:00Z");
    let mut cfg = tenant_config(&server, &[]);
    cfg.services = vec![
        service(
            "log_analytics",
            &[
                ("workspace_id", json!("ws-1")),
                ("kql", json!("Heartbeat | take 10")),
            ],
        ),
        service(
            "log_analytics",
            &[
                ("workspace_id", json!("ws-2")),
                ("kql", json!("SecurityEvent | where EventID == 4625")),
            ],
        ),
    ];
    let config = config(cfg);
    let mut built = crate::builtin_run::built_instance(&config, "azure").unwrap();
    built
        .instance
        .vars
        .insert("log_analytics_url".into(), Value::String(server.uri()));

    let (outcome, rows) =
        Box::pin(crate::builtin_run::run_instance(config, &built, Some(&w))).await;
    outcome.expect("fetch");
    let landed: Vec<Value> = rows.iter().map(|r| enriched(r).row).collect();
    assert_eq!(
        landed,
        [
            json!({"TimeGenerated": "2026-05-21T13:31:00Z", "Computer": "web-1"}),
            json!({"TimeGenerated": "2026-05-21T13:32:00Z", "Computer": "web-2"}),
            json!({"Account": "alice"}),
        ]
    );
    for row in &rows {
        assert_eq!(enriched(row).source_fetcher, "azure.log_analytics");
    }
    let first = requests_to(&server, "/v1/workspaces/ws-1/query").await;
    assert_eq!(first.len(), 1);
    assert_eq!(
        serde_json::from_slice::<Value>(&first[0].body).unwrap(),
        json!({"query": "Heartbeat | take 10", "timespan": "2026-05-21T13:30:00Z/2026-05-22T13:30:00Z"})
    );
    assert_eq!(header(&first[0], "content-type"), Some("application/json"));
    assert_eq!(
        header(&first[0], "authorization"),
        Some("Bearer tok-api.loganalytics.io")
    );
    let second = requests_to(&server, "/v1/workspaces/ws-2/query").await;
    assert_eq!(
        serde_json::from_slice::<Value>(&second[0].body).unwrap()["query"],
        "SecurityEvent | where EventID == 4625"
    );
    assert_eq!(
        scopes(&server).await,
        ["https://api.loganalytics.io/.default"],
        "one token for both queries"
    );
}

/// The health check is the Management token exchange: a minted token is
/// healthy, a refused exchange is the error, and no data endpoint is
/// touched.
#[tokio::test]
async fn test_azure_health_check_success() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    let healthy = health(config(tenant_config(&server, &["activity_log"])))
        .await
        .expect("health");
    assert!(healthy);
    assert_eq!(
        scopes(&server).await,
        ["https://management.azure.com/.default"]
    );
    assert!(requests_to(&server, ACTIVITY_LOG).await.is_empty());
}

#[tokio::test]
async fn test_azure_health_check_token_failure() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(".*/oauth2/v2.0/token"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "error": "invalid_client",
            "error_description": "Invalid client credentials"
        })))
        .mount(&server)
        .await;
    assert!(!matches!(
        health(config(tenant_config(&server, &["activity_log"]))).await,
        Ok(true)
    ));
}
