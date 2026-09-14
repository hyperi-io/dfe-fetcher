// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_m365.rs
// Purpose:   Characterisation of the M365 source: the OMAP subscription, content list and blob flow, Graph alerts, the record shape
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The M365 source against wiremock.
//!
//! Each test configures the typed `sources.m365` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the token exchanges wiremock recorded (one scope per API audience:
//! Management Activity, Graph), the requests each service sent (the
//! `subscriptions/start` per content type, the content list with the
//! window as `startTime`/`endTime` and the `PublisherIdentifier`, the
//! `NextPageUri` header followed, every blob fetched, the Graph
//! `alerts_v2` page with `@odata.nextLink`) and the records that landed
//! (the provider's row, semantically, plus what enrichment added). The
//! typed config block is the operator's contract; the shipped `m365`
//! profile serves it through the framework driver.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, path_regex, query_param};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use dfe_fetcher::config::{Config, M365Connection, M365Service, M365SourceConfig};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};

const TOKEN_PATH: &str = "/oauth2/v2.0/token";
const SUBSCRIPTIONS_LIST: &str = "/api/v1.0/test-tenant/activity/feed/subscriptions/list";
const SUBSCRIPTIONS_START: &str = "/api/v1.0/test-tenant/activity/feed/subscriptions/start";
const CONTENT: &str = "/api/v1.0/test-tenant/activity/feed/subscriptions/content";
const BLOBS: &str = "/api/v1.0/test-tenant/activity/feed/audit/";
const ALERTS: &str = "/v1.0/security/alerts_v2";
const DEFAULT_PUBLISHER: &str = "12345678-1234-1234-1234-123456789123";

/// The five OMAP content types the `audit_log` service fetches unless its
/// `content_types` knob narrows them, each with the suffix its records are
/// tagged with.
const AUDIT_LOG_FEEDS: &[(&str, &str)] = &[
    ("Audit.AzureActiveDirectory", "audit_azureactivedirectory"),
    ("Audit.Exchange", "audit_exchange"),
    ("Audit.SharePoint", "audit_sharepoint"),
    ("Audit.General", "audit_general"),
    ("DLP.All", "dlp_all"),
];

/// A deployment config carrying `m365` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(m365: M365SourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.m365 = m365;
    config
}

/// The typed block an operator writes: the tenant, the application, both
/// API hosts and the token endpoint pointed at wiremock, and the services.
fn tenant_config(server: &MockServer, services: &[&str]) -> M365SourceConfig {
    M365SourceConfig {
        enabled: true,
        tenant_id: Some("test-tenant".into()),
        client_id: Some("test-client-id".into()),
        client_secret: Some("test-client-secret".to_string().into()),
        management_url_override: Some(server.uri()),
        graph_url_override: Some(server.uri()),
        token_url_override: Some(format!("{}{TOKEN_PATH}", server.uri())),
        services: services.iter().map(|s| service(s, &[])).collect(),
        ..M365SourceConfig::default()
    }
}

fn service(name: &str, config: &[(&str, Value)]) -> M365Service {
    M365Service {
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

/// The subscription endpoints: nothing listed as enabled, and every
/// `start` answered 200 as the API does for a fresh subscription.
async fn mount_subscriptions(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path(SUBSCRIPTIONS_LIST))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(SUBSCRIPTIONS_START))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "enabled"})))
        .mount(server)
        .await;
}

/// One entry of a content list: a pointer to a blob on the same server.
fn content_item(server: &MockServer, id: &str, content_type: &str, created: &str) -> Value {
    json!({
        "contentUri": format!("{}{BLOBS}{id}", server.uri()),
        "contentId": id,
        "contentType": content_type,
        "contentCreated": created,
        "contentExpiration": "2026-06-01T00:00:00.000Z"
    })
}

/// The content list of one content type, optionally with a next page.
fn content_page(items: Vec<Value>, next: Option<String>) -> ResponseTemplate {
    let template = ResponseTemplate::new(200).set_body_json(Value::Array(items));
    match next {
        Some(url) => template.insert_header("NextPageUri", url.as_str()),
        None => template,
    }
}

async fn mount_content(server: &MockServer, content_type: &str, items: Vec<Value>) {
    Mock::given(method("GET"))
        .and(path(CONTENT))
        .and(query_param("contentType", content_type))
        .respond_with(content_page(items, None))
        .mount(server)
        .await;
}

/// Every content list empty, whatever the content type.
async fn mount_empty_content(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path(CONTENT))
        .respond_with(content_page(Vec::new(), None))
        .mount(server)
        .await;
}

/// A blob: the JSON array of records a content URI answers.
async fn mount_blob(server: &MockServer, id: &str, records: Vec<Value>) {
    Mock::given(method("GET"))
        .and(path(format!("{BLOBS}{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(Value::Array(records)))
        .mount(server)
        .await;
}

/// A Graph page of `value` rows, with a next link when given.
fn graph_page(rows: Vec<Value>, next: Option<String>) -> ResponseTemplate {
    let mut body = serde_json::Map::new();
    body.insert("value".into(), Value::Array(rows));
    if let Some(url) = next {
        body.insert("@odata.nextLink".into(), Value::String(url));
    }
    ResponseTemplate::new(200).set_body_json(Value::Object(body))
}

async fn mount_alerts(server: &MockServer, rows: Vec<Value>) {
    Mock::given(method("GET"))
        .and(path(ALERTS))
        .respond_with(graph_page(rows, None))
        .mount(server)
        .await;
}

/// One tick of the `m365` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "m365", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "m365")).await
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

/// The requests wiremock saw whose path starts with `prefix`, in order.
async fn requests_under(server: &MockServer, prefix: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.url.path().starts_with(prefix))
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

/// The content lists sent for one content type, in order.
async fn content_lists(server: &MockServer, content_type: &str) -> Vec<Request> {
    requests_to(server, CONTENT)
        .await
        .into_iter()
        .filter(|r| query_value(r, "contentType").as_deref() == Some(content_type))
        .collect()
}

/// The content types `subscriptions/start` was sent for, in order.
async fn started(server: &MockServer) -> Vec<String> {
    requests_to(server, SUBSCRIPTIONS_START)
        .await
        .iter()
        .map(|r| query_value(r, "contentType").unwrap_or_default())
        .collect()
}

fn record(id: &str) -> Value {
    json!({"Id": id, "Operation": "FileAccessed", "Workload": "SharePoint", "UserId": "alice@contoso.example"})
}

fn ids(rows: &[Landed]) -> Vec<String> {
    rows.iter()
        .map(|r| r.record["Id"].as_str().unwrap().to_owned())
        .collect()
}

/// The landed record ids grouped by `_source_fetcher`, in landing order.
fn by_source(rows: &[Landed]) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        out.entry(enriched(row).source_fetcher)
            .or_default()
            .push(row.record["Id"].as_str().unwrap().to_owned());
    }
    out
}

// Wiremock-backed tests: the token endpoint and both API hosts are the same
// mock server, told apart by path.

#[tokio::test]
async fn test_m365_disabled_returns_empty() {
    let server = MockServer::start().await;
    let mut cfg = tenant_config(&server, &["alerts"]);
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
async fn test_m365_health_check_disabled() {
    let server = MockServer::start().await;
    let mut cfg = tenant_config(&server, &["alerts"]);
    cfg.enabled = false;
    assert!(!matches!(health(config(cfg)).await, Ok(true)));
}

#[tokio::test]
async fn test_m365_missing_tenant_id() {
    let server = MockServer::start().await;
    let mut cfg = tenant_config(&server, &["alerts"]);
    cfg.tenant_id = None;
    assert!(!matches!(health(config(cfg)).await, Ok(true)));
}

/// `audit_log` with no `content_types` knob: every one of the five OMAP
/// feeds gets its subscription started and its content listed under one
/// Management-audience token, with the window as `startTime`/`endTime`
/// to the second with no zone suffix and the default
/// `PublisherIdentifier`; empty lists land nothing.
#[tokio::test]
async fn test_m365_fetch_audit_log_empty() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_subscriptions(&server).await;
    mount_empty_content(&server).await;
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(config(tenant_config(&server, &["audit_log"])), Some(&w)).await;
    outcome.expect("fetch");
    assert!(rows.is_empty());

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
        Some("https://manage.office.com/.default")
    );

    let mut expected: Vec<&str> = AUDIT_LOG_FEEDS.iter().map(|(ct, _)| *ct).collect();
    expected.sort_unstable();
    let mut seen = started(&server).await;
    seen.sort_unstable();
    assert_eq!(seen, expected, "every feed's subscription is started");

    for (content_type, _) in AUDIT_LOG_FEEDS {
        let lists = content_lists(&server, content_type).await;
        assert_eq!(lists.len(), 1, "{content_type}");
        assert_eq!(
            query_of(&lists[0]),
            [
                (
                    "PublisherIdentifier".to_string(),
                    DEFAULT_PUBLISHER.to_string()
                ),
                ("contentType".to_string(), (*content_type).to_string()),
                ("endTime".to_string(), "2026-05-21T14:30:00".to_string()),
                ("startTime".to_string(), "2026-05-21T13:30:00".to_string()),
            ],
            "{content_type}: the window to the second with no zone, the default publisher"
        );
        assert_eq!(
            header(&lists[0], "authorization"),
            Some("Bearer tok-manage.office.com")
        );
    }
}

/// The subscription is started BLIND every tick: one idempotent `start`
/// per content type, no `subscriptions/list` first. The API answers an
/// already-enabled feed with 400 `AF20024`, which is not a failure.
#[tokio::test]
async fn subscriptions_are_started_blind_every_tick() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("GET"))
        .and(path(SUBSCRIPTIONS_LIST))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"contentType": "DLP.All", "status": "enabled", "webhook": null}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(SUBSCRIPTIONS_START))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {"code": "AF20024", "message": "The subscription is already enabled. No property change."}
        })))
        .mount(&server)
        .await;
    mount_content(
        &server,
        "DLP.All",
        vec![content_item(
            &server,
            "dlp-1",
            "DLP.All",
            "2026-05-21T13:40:00.000Z",
        )],
    )
    .await;
    mount_blob(&server, "dlp-1", vec![record("d1")]).await;

    let (outcome, rows) = run(config(tenant_config(&server, &["dlp"])), None).await;
    outcome.expect("an already-enabled subscription is not a failure");
    assert_eq!(ids(&rows), ["d1"]);
    assert_eq!(started(&server).await, ["DLP.All"], "one blind start");
    assert_eq!(
        query_value(
            &requests_to(&server, SUBSCRIPTIONS_START).await[0],
            "PublisherIdentifier"
        )
        .as_deref(),
        Some(DEFAULT_PUBLISHER),
        "the start carries the publisher like every OMAP call"
    );
    assert!(
        requests_to(&server, SUBSCRIPTIONS_LIST).await.is_empty(),
        "the list is not consulted"
    );
}

/// A 404 on the content list (the subscription is not enabled after all)
/// fails the unit's tick, so the window is not advanced past content that
/// was never listed; the next tick starts the subscription again.
#[tokio::test]
async fn test_m365_fetch_audit_log_404_starts_subscription() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_subscriptions(&server).await;
    Mock::given(method("GET"))
        .and(path(CONTENT))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {"code": "AF20022", "message": "No subscription found for the specified content type"}
        })))
        .mount(&server)
        .await;

    let (outcome, rows) = run(config(tenant_config(&server, &["dlp"])), None).await;
    assert!(rows.is_empty());
    assert_eq!(
        started(&server).await,
        ["DLP.All"],
        "the start before the list"
    );
    assert_eq!(
        requests_to(&server, CONTENT).await.len(),
        1,
        "a 404 is never retried"
    );
    let err = outcome.expect_err("the tick reports the missing subscription");
    assert!(
        err.contains("404") && err.contains("No subscription found"),
        "the error text comes from error.message: {err}"
    );
}

/// The OMAP flow end to end for the `content_types` knob's feeds: each
/// content list is asked for under the window, every item's `contentUri`
/// is fetched with the Management bearer, and each blob's records land in
/// list order tagged `m365.audit_log.<feed>`.
#[tokio::test]
async fn audit_log_lists_each_content_type_and_fetches_every_blob() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_subscriptions(&server).await;
    mount_content(
        &server,
        "Audit.SharePoint",
        vec![
            content_item(
                &server,
                "sp-1",
                "Audit.SharePoint",
                "2026-05-21T13:40:00.000Z",
            ),
            content_item(
                &server,
                "sp-2",
                "Audit.SharePoint",
                "2026-05-21T13:50:00.000Z",
            ),
        ],
    )
    .await;
    mount_content(
        &server,
        "Audit.General",
        vec![content_item(
            &server,
            "gen-1",
            "Audit.General",
            "2026-05-21T13:45:00.000Z",
        )],
    )
    .await;
    mount_blob(&server, "sp-1", vec![record("sp-1a"), record("sp-1b")]).await;
    mount_blob(&server, "sp-2", vec![record("sp-2a")]).await;
    mount_blob(&server, "gen-1", vec![record("gen-1a"), record("gen-1b")]).await;
    let w = fetch_window("2026-05-21T13:30:00Z", "2026-05-21T14:30:00Z");
    let mut cfg = tenant_config(&server, &[]);
    cfg.services = vec![service(
        "audit_log",
        &[(
            "content_types",
            json!(["Audit.SharePoint", "Audit.General"]),
        )],
    )];

    let (outcome, rows) = run(config(cfg), Some(&w)).await;
    outcome.expect("fetch");

    let landed = by_source(&rows);
    assert_eq!(
        landed["m365.audit_log.audit_sharepoint"],
        ["sp-1a", "sp-1b", "sp-2a"],
        "every blob's records, in list order"
    );
    assert_eq!(landed["m365.audit_log.audit_general"], ["gen-1a", "gen-1b"]);
    assert_eq!(landed.len(), 2, "only the two feeds the knob names");
    for row in &rows {
        assert_eq!(row.topic, "m365_land");
        let e = enriched(row);
        assert_eq!(e.source, "m365");
        assert_eq!(
            e.row,
            record(e.row["Id"].as_str().unwrap()),
            "the provider's record, semantically"
        );
    }

    let mut seen = started(&server).await;
    seen.sort_unstable();
    assert_eq!(seen, ["Audit.General", "Audit.SharePoint"]);
    assert!(content_lists(&server, "Audit.Exchange").await.is_empty());
    let blobs = requests_under(&server, BLOBS).await;
    assert_eq!(blobs.len(), 3, "each content URI fetched once");
    for blob in &blobs {
        assert_eq!(
            header(blob, "authorization"),
            Some("Bearer tok-manage.office.com"),
            "{}",
            blob.url.path()
        );
    }
    assert_eq!(
        scopes(&server).await,
        ["https://manage.office.com/.default"],
        "one token for the whole OMAP tick"
    );
}

/// A content list pages by the `NextPageUri` HEADER, followed as given.
#[tokio::test]
async fn content_lists_follow_the_next_page_uri_header() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_subscriptions(&server).await;
    let next = format!("{}{CONTENT}?contentType=DLP.All&nextPage=abc", server.uri());
    Mock::given(method("GET"))
        .and(path(CONTENT))
        .and(query_param("nextPage", "abc"))
        .respond_with(content_page(
            vec![content_item(
                &server,
                "dlp-2",
                "DLP.All",
                "2026-05-21T13:50:00.000Z",
            )],
            None,
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(CONTENT))
        .respond_with(content_page(
            vec![content_item(
                &server,
                "dlp-1",
                "DLP.All",
                "2026-05-21T13:40:00.000Z",
            )],
            Some(next),
        ))
        .mount(&server)
        .await;
    mount_blob(&server, "dlp-1", vec![record("d1")]).await;
    mount_blob(&server, "dlp-2", vec![record("d2")]).await;

    let (outcome, rows) = run(config(tenant_config(&server, &["dlp"])), None).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["d1", "d2"]);
    let lists = requests_to(&server, CONTENT).await;
    assert_eq!(lists.len(), 2);
    assert_eq!(query_value(&lists[1], "nextPage").as_deref(), Some("abc"));
    assert_eq!(
        header(&lists[1], "authorization"),
        Some("Bearer tok-manage.office.com")
    );
}

/// The API caps one content list at 24 h, so a longer window is asked for
/// in day-sized steps, the last one short.
#[tokio::test]
async fn a_window_longer_than_a_day_is_listed_in_24h_steps() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_subscriptions(&server).await;
    mount_empty_content(&server).await;
    let w = fetch_window("2026-05-20T00:00:00Z", "2026-05-22T02:00:00Z");

    let (outcome, _) = run(
        config(tenant_config(&server, &["exchange_audit"])),
        Some(&w),
    )
    .await;
    outcome.expect("fetch");
    let bounds: Vec<(String, String)> = content_lists(&server, "Audit.Exchange")
        .await
        .iter()
        .map(|r| {
            (
                query_value(r, "startTime").unwrap(),
                query_value(r, "endTime").unwrap(),
            )
        })
        .collect();
    assert_eq!(
        bounds,
        [
            (
                "2026-05-20T00:00:00".to_string(),
                "2026-05-21T00:00:00".to_string()
            ),
            (
                "2026-05-21T00:00:00".to_string(),
                "2026-05-22T00:00:00".to_string()
            ),
            (
                "2026-05-22T00:00:00".to_string(),
                "2026-05-22T02:00:00".to_string()
            ),
        ]
    );
}

/// `dlp` and `exchange_audit` are single-feed services on the same OMAP
/// flow, each tagged with its own service name.
#[tokio::test]
async fn dlp_and_exchange_audit_fetch_their_own_feed() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_subscriptions(&server).await;
    mount_content(
        &server,
        "DLP.All",
        vec![content_item(
            &server,
            "dlp-1",
            "DLP.All",
            "2026-05-21T13:40:00.000Z",
        )],
    )
    .await;
    mount_content(
        &server,
        "Audit.Exchange",
        vec![content_item(
            &server,
            "ex-1",
            "Audit.Exchange",
            "2026-05-21T13:41:00.000Z",
        )],
    )
    .await;
    mount_blob(&server, "dlp-1", vec![record("d1")]).await;
    mount_blob(&server, "ex-1", vec![record("e1"), record("e2")]).await;

    let (outcome, rows) = run(
        config(tenant_config(&server, &["dlp", "exchange_audit"])),
        None,
    )
    .await;
    outcome.expect("fetch");
    let landed = by_source(&rows);
    assert_eq!(landed["m365.dlp"], ["d1"]);
    assert_eq!(landed["m365.exchange_audit"], ["e1", "e2"]);
    assert_eq!(landed.len(), 2);
    let mut seen = started(&server).await;
    seen.sort_unstable();
    assert_eq!(seen, ["Audit.Exchange", "DLP.All"]);
}

/// Graph security alerts: `$top=100` newest first under a Graph-audience
/// token, no window on the request, every alert landed as `m365.alerts`.
#[tokio::test]
async fn test_m365_fetch_alerts_success() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_alerts(
        &server,
        vec![
            json!({"id": "alert-1", "severity": "high"}),
            json!({"id": "alert-2", "severity": "medium"}),
        ],
    )
    .await;
    let w = fetch_window("2026-05-21T13:30:00Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(config(tenant_config(&server, &["alerts"])), Some(&w)).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 2);
    for (row, expected) in rows.iter().zip([
        json!({"id": "alert-1", "severity": "high"}),
        json!({"id": "alert-2", "severity": "medium"}),
    ]) {
        assert_eq!(row.topic, "m365_land");
        let e = enriched(row);
        assert_eq!(e.row, expected);
        assert_eq!(e.source, "m365");
        assert_eq!(e.source_fetcher, "m365.alerts");
    }
    let seen = requests_to(&server, ALERTS).await;
    assert_eq!(seen.len(), 1);
    assert_eq!(
        query_of(&seen[0]),
        [
            ("$orderby".to_string(), "createdDateTime desc".to_string()),
            ("$top".to_string(), "100".to_string()),
        ],
        "the newest 100, nothing about the window"
    );
    assert_eq!(
        header(&seen[0], "authorization"),
        Some("Bearer tok-graph.microsoft.com")
    );
    assert_eq!(
        scopes(&server).await,
        ["https://graph.microsoft.com/.default"]
    );
    assert!(requests_to(&server, SUBSCRIPTIONS_START).await.is_empty());
}

/// Graph pages with `@odata.nextLink`, followed as given.
#[tokio::test]
async fn alerts_follow_the_odata_next_link() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("GET"))
        .and(path(ALERTS))
        .and(query_param("$skiptoken", "xyz"))
        .respond_with(graph_page(vec![json!({"id": "alert-2"})], None))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(ALERTS))
        .respond_with(graph_page(
            vec![json!({"id": "alert-1"})],
            Some(format!("{}{ALERTS}?$skiptoken=xyz", server.uri())),
        ))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(tenant_config(&server, &["alerts"])), None).await;
    outcome.expect("fetch");
    let landed: Vec<&str> = rows
        .iter()
        .map(|r| r.record["id"].as_str().unwrap())
        .collect();
    assert_eq!(landed, ["alert-1", "alert-2"]);
    assert_eq!(requests_to(&server, ALERTS).await.len(), 2);
}

/// Alerts are the tenant's current alerts, not events in the window: a
/// multi-day window is one request, not one per day.
#[tokio::test]
async fn alerts_are_fetched_once_per_tick_whatever_the_window() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_alerts(&server, vec![json!({"id": "alert-1"})]).await;
    let w = fetch_window("2026-05-20T00:00:00Z", "2026-05-22T02:00:00Z");
    let (outcome, rows) = run(config(tenant_config(&server, &["alerts"])), Some(&w)).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(requests_to(&server, ALERTS).await.len(), 1);
}

#[tokio::test]
async fn without_a_window_the_last_hour_is_fetched() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_subscriptions(&server).await;
    mount_empty_content(&server).await;
    let before = Utc::now();
    run(config(tenant_config(&server, &["dlp"])), None)
        .await
        .0
        .expect("fetch");
    let list = &content_lists(&server, "DLP.All").await[0];
    let parse = |name: &str| at(&format!("{}Z", query_value(list, name).unwrap()));
    let (start, end) = (parse("startTime"), parse("endTime"));
    assert_eq!((end - start).num_hours(), 1, "one hour of lookback");
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
    mount_alerts(&server, vec![json!({"id": "alert-1"})]).await;
    // nextest runs one process per test, so the variable leaks nowhere.
    unsafe { std::env::set_var("DFE_FETCHER_TEST_M365_SECRET", "secret-from-env") };
    let mut cfg = tenant_config(&server, &["alerts"]);
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_M365_SECRET".into());
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

/// The `PublisherIdentifier` every OMAP call carries comes from the block's
/// `publisher_identifier` knob, the shared default when unset; the process
/// environment is not consulted.
#[tokio::test]
async fn the_publisher_identifier_comes_from_config_not_the_environment() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_subscriptions(&server).await;
    mount_empty_content(&server).await;
    // nextest runs one process per test, so the variable leaks nowhere.
    unsafe { std::env::set_var("M365_PUBLISHER_IDENTIFIER", "publisher-from-env") };

    run(config(tenant_config(&server, &["dlp"])), None)
        .await
        .0
        .expect("fetch");
    assert_eq!(
        query_value(
            &content_lists(&server, "DLP.All").await[0],
            "PublisherIdentifier"
        )
        .as_deref(),
        Some(DEFAULT_PUBLISHER),
        "the environment is ignored"
    );

    let mut cfg = tenant_config(&server, &["dlp"]);
    cfg.publisher_identifier = Some("11111111-2222-3333-4444-555555555555".into());
    run(config(cfg), None).await.0.expect("fetch");
    let lists = content_lists(&server, "DLP.All").await;
    assert_eq!(
        query_value(&lists[1], "PublisherIdentifier").as_deref(),
        Some("11111111-2222-3333-4444-555555555555"),
        "the knob is honoured"
    );
    assert_eq!(
        query_value(
            &requests_to(&server, SUBSCRIPTIONS_START).await[1],
            "PublisherIdentifier"
        )
        .as_deref(),
        Some("11111111-2222-3333-4444-555555555555"),
        "on the start as well"
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_alerts(
        &server,
        vec![
            json!({"id": "a", "severity": "high"}),
            json!({"id": "b", "severity": "low"}),
            json!({"id": "c", "severity": "high"}),
        ],
    )
    .await;
    let mut cfg = tenant_config(&server, &["alerts"]);
    cfg.filter = Some("severity == \"high\"".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let landed: Vec<&str> = rows
        .iter()
        .map(|r| r.record["id"].as_str().unwrap())
        .collect();
    assert_eq!(landed, ["a", "c"]);
}

/// A 5xx the API keeps answering: the tick fails after the bounded
/// retries and nothing lands.
#[tokio::test]
async fn test_m365_fetch_error_500() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("GET"))
        .and(path(ALERTS))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(tenant_config(&server, &["alerts"])), None).await;
    assert!(rows.is_empty(), "a failed service produces no records");
    assert_eq!(
        requests_to(&server, ALERTS).await.len(),
        4,
        "the first attempt and three retries"
    );
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
}

/// A blob the API keeps failing fails the unit's tick after the bounded
/// retries, so the window is not advanced past records that never landed.
#[tokio::test]
async fn a_blob_that_keeps_failing_fails_the_tick() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_subscriptions(&server).await;
    mount_content(
        &server,
        "DLP.All",
        vec![
            content_item(&server, "dlp-1", "DLP.All", "2026-05-21T13:40:00.000Z"),
            content_item(&server, "dlp-2", "DLP.All", "2026-05-21T13:50:00.000Z"),
        ],
    )
    .await;
    Mock::given(method("GET"))
        .and(path(format!("{BLOBS}dlp-1")))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&server)
        .await;
    mount_blob(&server, "dlp-2", vec![record("d2")]).await;

    let (outcome, rows) = run(config(tenant_config(&server, &["dlp"])), None).await;
    assert!(rows.is_empty(), "nothing lands from a failed unit");
    assert_eq!(
        requests_to(&server, &format!("{BLOBS}dlp-1")).await.len(),
        4,
        "the first attempt and three retries"
    );
    let err = outcome.expect_err("the tick reports the failed blob");
    assert!(err.contains("500"), "{err}");
}

/// A 429 with `Retry-After` is retried after the wait, and the records land.
#[tokio::test]
async fn a_429_with_retry_after_is_retried() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("GET"))
        .and(path(ALERTS))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_alerts(&server, vec![json!({"id": "alert-1"})]).await;
    let (outcome, rows) = run(config(tenant_config(&server, &["alerts"])), None).await;
    outcome.expect("tick");
    assert_eq!(requests_to(&server, ALERTS).await.len(), 2, "429 then 200");
    assert_eq!(rows.len(), 1);
}

/// A 403 is never retried: the tick fails on the one request with the
/// API's message.
#[tokio::test]
async fn a_refusal_is_never_retried() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("GET"))
        .and(path(ALERTS))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {"code": "Authorization_RequestDenied", "message": "Insufficient privileges to complete the operation."}
        })))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(tenant_config(&server, &["alerts"])), None).await;
    assert!(rows.is_empty());
    assert_eq!(requests_to(&server, ALERTS).await.len(), 1);
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
    mount_alerts(&server, vec![json!({"id": "alert-1"})]).await;
    let (outcome, rows) = run(config(tenant_config(&server, &["alerts"])), None).await;
    assert!(rows.is_empty());
    assert_eq!(requests_to(&server, TOKEN_PATH).await.len(), 1);
    assert!(requests_to(&server, ALERTS).await.is_empty());
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
    mount_alerts(&server, vec![json!({"id": "alert-1"})]).await;
    let mut cfg = tenant_config(&server, &["alerts"]);
    cfg.tenant_id = None;
    cfg.client_id = None;
    cfg.client_secret = None;
    cfg.connections = vec![
        M365Connection {
            id: "m365-prod".into(),
            tenant_id: Some("tenant-prod".into()),
            client_id: Some("client-prod".into()),
            client_secret: Some("secret-prod".to_string().into()),
            ..M365Connection::default()
        },
        M365Connection {
            id: "m365-dev".into(),
            tenant_id: Some("tenant-dev".into()),
            client_id: Some("client-dev".into()),
            client_secret: Some("secret-dev".to_string().into()),
            ..M365Connection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["m365-prod", "m365-dev"] {
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
            ("m365-prod".to_string(), "m365-prod.alerts".to_string()),
            ("m365-dev".to_string(), "m365-dev.alerts".to_string()),
        ]
    );
}

/// A service the profile does not know is refused at validation by name,
/// instead of being skipped with a warning on every tick.
#[tokio::test]
async fn an_unknown_service_name_is_refused_at_validation() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    let cfg = tenant_config(&server, &["message_trace"]);
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.m365") && err.contains("message_trace"),
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

/// A content type outside the five OMAP feeds is refused at validation by
/// name, instead of a 400 from the API every tick.
#[tokio::test]
async fn an_unknown_content_type_is_refused_at_validation() {
    let server = MockServer::start().await;
    let mut cfg = tenant_config(&server, &[]);
    cfg.services = vec![service(
        "audit_log",
        &[("content_types", json!(["Audit.SharePoint", "Audit.Teams"]))],
    )];
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.m365") && err.contains("Audit.Teams"),
        "{err}"
    );
}

/// A missing tenant or a missing client secret is refused at validation
/// naming the field, instead of failing silently every tick.
#[tokio::test]
async fn missing_identity_is_refused_at_validation() {
    let server = MockServer::start().await;
    let mut cfg = tenant_config(&server, &["alerts"]);
    cfg.tenant_id = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.m365") && err.contains("tenant_id"),
        "{err}"
    );
    let mut cfg = tenant_config(&server, &["alerts"]);
    cfg.client_secret = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.m365") && err.contains("credential_secret"),
        "{err}"
    );
}

/// The health check is the Management token exchange: a minted token is
/// healthy, a refused exchange is the error, and no data endpoint is
/// touched.
#[tokio::test]
async fn test_m365_health_check_success() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    let healthy = health(config(tenant_config(&server, &["alerts"])))
        .await
        .expect("health");
    assert!(healthy);
    assert_eq!(
        scopes(&server).await,
        ["https://manage.office.com/.default"]
    );
    assert!(requests_to(&server, ALERTS).await.is_empty());
}

#[tokio::test]
async fn test_m365_health_check_token_failure() {
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
        health(config(tenant_config(&server, &["alerts"]))).await,
        Ok(true)
    ));
}
