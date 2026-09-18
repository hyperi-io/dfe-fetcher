// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_salesforce.rs
// Purpose:   Characterisation of the Salesforce source: the two OAuth2 flows, the SOQL windows and their paging, the EventLogFile CSV manifest
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Salesforce source against wiremock.
//!
//! Each test configures the typed `sources.salesforce` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the requests wiremock recorded (the JWT-bearer or client-credentials
//! exchange at the login host, every data call on the `instance_url` the
//! exchange answered, the SOQL of each unit with the window as bare RFC 3339
//! literals, `nextRecordsUrl` followed, each EventLogFile's `LogFile`
//! downloaded) and the records that landed (the SOQL rows, the CSV rows as
//! objects stamped with their file's event type and log date, plus what
//! enrichment added). The typed config block is the operator's contract;
//! the shipped `salesforce` profile serves it through the framework
//! driver.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use dfe_fetcher::config::{
    Config, SalesforceConnection, SalesforceService, SalesforceSourceConfig,
};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::common::rsa_key_pair;

const TOKEN_PATH: &str = "/services/oauth2/token";
const QUERY: &str = "/services/data/v60.0/query";
const CLIENT_ID: &str = "3MVG9consumerkey";
const USERNAME: &str = "audit@acme.example";

/// A deployment config carrying `salesforce` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(salesforce: SalesforceSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.salesforce = salesforce;
    config
}

fn service(name: &str, config: &[(&str, Value)]) -> SalesforceService {
    SalesforceService {
        name: name.into(),
        config: config
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    }
}

/// The typed block an operator writes for the JWT-bearer flow: the
/// connected app's consumer key, the integration user, the private key,
/// the login host pointed at wiremock.
fn jwt_config(
    server: &MockServer,
    key: &ConnectedApp,
    services: &[&str],
) -> SalesforceSourceConfig {
    SalesforceSourceConfig {
        enabled: true,
        login_url: Some(server.uri()),
        client_id: Some(CLIENT_ID.into()),
        username: Some(USERNAME.into()),
        private_key: Some(key.private_pem.clone().into()),
        services: services.iter().map(|s| service(s, &[])).collect(),
        ..SalesforceSourceConfig::default()
    }
}

/// The same block on the client-credentials flow.
fn secret_config(server: &MockServer, services: &[&str]) -> SalesforceSourceConfig {
    SalesforceSourceConfig {
        enabled: true,
        login_url: Some(server.uri()),
        client_id: Some(CLIENT_ID.into()),
        client_secret: Some("secret-consumer".to_string().into()),
        services: services.iter().map(|s| service(s, &[])).collect(),
        ..SalesforceSourceConfig::default()
    }
}

/// One recorded exchange: the form sent, and the verified claims of a
/// JWT-bearer assertion.
type Exchange = (HashMap<String, String>, Option<Value>);

/// The connected app's key pair and the token endpoint that verifies
/// assertions signed with it (or a client secret), recording every
/// exchange's form and claims and answering the org's `instance_url`.
struct ConnectedApp {
    private_pem: String,
    exchanges: Arc<Mutex<Vec<Exchange>>>,
}

struct TokenEndpoint {
    public_pem: String,
    instance_url: String,
    exchanges: Arc<Mutex<Vec<Exchange>>>,
}

impl Respond for TokenEndpoint {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let form = form_of(request);
        let refused = ResponseTemplate::new(400)
            .set_body_json(json!({"error": "invalid_grant", "error_description": "user hasn't approved this consumer"}));
        let claims = match form.get("grant_type").map(String::as_str) {
            Some("urn:ietf:params:oauth:grant-type:jwt-bearer") => {
                let Some(assertion) = form.get("assertion") else {
                    return refused;
                };
                let key = jsonwebtoken::DecodingKey::from_rsa_pem(self.public_pem.as_bytes())
                    .expect("public key");
                let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
                validation.validate_aud = false;
                validation.set_required_spec_claims(&["exp"]);
                match jsonwebtoken::decode::<Value>(assertion, &key, &validation) {
                    Ok(data) => Some(data.claims),
                    Err(_) => return refused,
                }
            }
            Some("client_credentials") => {
                if form.get("client_secret").map(String::as_str) != Some("secret-consumer") {
                    return refused;
                }
                None
            }
            _ => return refused,
        };
        self.exchanges.lock().unwrap().push((form, claims));
        ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "00Dxx!token",
            "instance_url": self.instance_url,
            "id": "https://login.salesforce.com/id/00Dxx/005xx",
            "token_type": "Bearer",
            "issued_at": "1716290000000",
            "signature": "sig"
        }))
    }
}

/// Mount the token endpoint answering `instance_url` as the org's host.
async fn connected_app(server: &MockServer, instance_url: &str) -> ConnectedApp {
    let (private_pem, public_pem) = rsa_key_pair();
    let exchanges = Arc::new(Mutex::new(Vec::new()));
    Mock::given(method("POST"))
        .and(path(TOKEN_PATH))
        .respond_with(TokenEndpoint {
            public_pem,
            instance_url: instance_url.to_owned(),
            exchanges: Arc::clone(&exchanges),
        })
        .mount(server)
        .await;
    ConnectedApp {
        private_pem,
        exchanges,
    }
}

/// A SOQL result page, with `nextRecordsUrl` when more follow.
fn soql_page(records: &[Value], next: Option<&str>) -> ResponseTemplate {
    let mut body = json!({"totalSize": records.len(), "done": next.is_none(), "records": records});
    if let Some(next) = next {
        body["nextRecordsUrl"] = Value::String(next.into());
    }
    ResponseTemplate::new(200).set_body_json(body)
}

/// Mount the query answering `records` for a SOQL whose text starts with
/// `soql_prefix`, under the instance host `at` (`""` for the root).
async fn mount_query(server: &MockServer, at: &str, sobject: &str, response: ResponseTemplate) {
    Mock::given(method("GET"))
        .and(path(format!("{at}{QUERY}")))
        .and(SoqlFor(sobject.to_owned()))
        .respond_with(response)
        .mount(server)
        .await;
}

/// Matches a query whose SOQL selects from `sobject`.
struct SoqlFor(String);

impl wiremock::Match for SoqlFor {
    fn matches(&self, request: &Request) -> bool {
        query_value(request, "q").is_some_and(|q| q.contains(&format!("FROM {} ", self.0)))
    }
}

async fn mount_log_file(server: &MockServer, id: &str, csv: &str) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/services/data/v60.0/sobjects/EventLogFile/{id}/LogFile"
        )))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(csv)
                .insert_header("Content-Type", "text/csv"),
        )
        .mount(server)
        .await;
}

/// One tick of the `salesforce` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "salesforce", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "salesforce")).await
}

fn at(rfc3339: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(rfc3339)
        .expect("rfc3339")
        .with_timezone(&Utc)
}

fn window() -> FetchWindow {
    FetchWindow {
        start: at("2026-05-21T13:30:00Z"),
        end: at("2026-05-21T14:30:00Z"),
    }
}

async fn requests_to(server: &MockServer, at: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.url.path() == at)
        .collect()
}

/// The paths wiremock saw, in order.
async fn paths(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .map(|r| r.url.path().to_owned())
        .collect()
}

fn header_of<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request.headers.get(name).and_then(|v| v.to_str().ok())
}

fn query_value(request: &Request, name: &str) -> Option<String> {
    request
        .url
        .query_pairs()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
}

/// The form fields of a token exchange.
fn form_of(request: &Request) -> HashMap<String, String> {
    url::form_urlencoded::parse(&request.body)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

/// The SOQL of every query sent, in order.
async fn soqls(server: &MockServer) -> Vec<String> {
    requests_to(server, QUERY)
        .await
        .iter()
        .filter_map(|r| query_value(r, "q"))
        .collect()
}

fn setup_row(id: &str) -> Value {
    json!({"attributes": {"type": "SetupAuditTrail", "url": format!("/services/data/v60.0/sobjects/SetupAuditTrail/{id}")}, "Id": id, "Action": "changedPassword", "Section": "Manage Users", "CreatedDate": "2026-05-21T13:45:00.000+0000", "Display": "Changed password", "DelegateUser": null, "CreatedBy": {"Username": USERNAME}})
}

fn login_row(id: &str) -> Value {
    json!({"attributes": {"type": "LoginHistory"}, "Id": id, "UserId": "005xx", "LoginTime": "2026-05-21T13:40:00.000+0000", "LoginType": "Application", "SourceIp": "203.0.113.5", "Status": "Success"})
}

fn ids(rows: &[Landed]) -> Vec<String> {
    rows.iter()
        .map(|r| r.record["Id"].as_str().unwrap_or_default().to_owned())
        .collect()
}

#[tokio::test]
async fn the_soql_units_query_their_windows_on_the_instance_and_page_by_next_records_url() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    mount_query(
        &server,
        "",
        "SetupAuditTrail",
        soql_page(
            &[setup_row("0Ym1"), setup_row("0Ym2")],
            Some("/services/data/v60.0/query/01gxx-2000"),
        ),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/services/data/v60.0/query/01gxx-2000"))
        .respond_with(soql_page(&[setup_row("0Ym3")], None))
        .mount(&server)
        .await;
    mount_query(
        &server,
        "",
        "LoginHistory",
        soql_page(&[login_row("0Ya1")], None),
    )
    .await;

    let (outcome, rows) = run(
        config(jwt_config(
            &server,
            &app,
            &["setup_audit_trail", "login_history"],
        )),
        Some(&window()),
    )
    .await;
    outcome.expect("fetch");

    let exchanges = app.exchanges.lock().unwrap().clone();
    assert_eq!(exchanges.len(), 1, "one exchange for the tick");
    let (form, claims) = &exchanges[0];
    assert_eq!(
        form.get("grant_type").map(String::as_str),
        Some("urn:ietf:params:oauth:grant-type:jwt-bearer")
    );
    let claims = claims.as_ref().expect("a verified assertion");
    assert_eq!(claims["iss"], CLIENT_ID);
    assert_eq!(claims["sub"], USERNAME);
    assert_eq!(
        claims["aud"],
        server.uri(),
        "the login host, not the instance"
    );
    let exp = claims["exp"].as_i64().unwrap();
    let now = Utc::now().timestamp();
    assert!(
        (295..=305).contains(&(exp - now)),
        "five minutes: {}",
        exp - now
    );
    assert_eq!(exp - claims["iat"].as_i64().unwrap(), 300);

    assert_eq!(
        soqls(&server).await,
        [
            "SELECT Id, Action, Section, CreatedDate, Display, DelegateUser, CreatedBy.Username FROM SetupAuditTrail WHERE CreatedDate >= 2026-05-21T13:30:00Z AND CreatedDate < 2026-05-21T14:30:00Z ORDER BY CreatedDate ASC",
            "SELECT Id, UserId, LoginTime, LoginType, SourceIp, Status, Application, Browser, Platform, CountryIso, ApiType, TlsProtocol FROM LoginHistory WHERE LoginTime >= 2026-05-21T13:30:00Z AND LoginTime < 2026-05-21T14:30:00Z ORDER BY LoginTime ASC",
        ]
    );
    for request in requests_to(&server, QUERY).await {
        assert_eq!(
            header_of(&request, "authorization"),
            Some("Bearer 00Dxx!token")
        );
        assert_eq!(header_of(&request, "accept"), Some("application/json"));
    }
    assert_eq!(
        requests_to(&server, "/services/data/v60.0/query/01gxx-2000")
            .await
            .len(),
        1,
        "the relative nextRecordsUrl resolved against the instance host"
    );

    assert_eq!(ids(&rows), ["0Ym1", "0Ym2", "0Ym3", "0Ya1"]);
    for row in &rows[..3] {
        assert_eq!(row.topic, "salesforce_land");
        let e = enriched(row);
        assert_eq!(e.source, "salesforce");
        assert_eq!(e.source_fetcher, "salesforce.setup_audit_trail");
    }
    assert_eq!(
        enriched(&rows[0]).row,
        setup_row("0Ym1"),
        "the record as the API sent it"
    );
    assert_eq!(
        enriched(&rows[3]).source_fetcher,
        "salesforce.login_history"
    );
}

/// Without a private key the client-credentials flow runs: consumer key
/// and secret as the form, `credential_secret` supplying the secret when
/// set.
#[tokio::test]
async fn client_credentials_run_when_no_private_key_is_set() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    mount_query(
        &server,
        "",
        "LoginHistory",
        soql_page(&[login_row("0Ya1")], None),
    )
    .await;
    let (outcome, rows) = run(
        config(secret_config(&server, &["login_history"])),
        Some(&window()),
    )
    .await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["0Ya1"]);
    let exchanges = app.exchanges.lock().unwrap().clone();
    let (form, claims) = &exchanges[0];
    assert!(claims.is_none());
    assert_eq!(
        form.get("grant_type").map(String::as_str),
        Some("client_credentials")
    );
    assert_eq!(form.get("client_id").map(String::as_str), Some(CLIENT_ID));
    assert_eq!(
        form.get("client_secret").map(String::as_str),
        Some("secret-consumer")
    );

    let mut cfg = secret_config(&server, &["login_history"]);
    cfg.client_secret = None;
    cfg.credential_secret = Some("secret-consumer".into());
    let (outcome, _) = run(config(cfg), Some(&window())).await;
    outcome.expect("credential_secret supplies the consumer secret");
}

/// A pinned `instance_url_override` wins over the instance URL the
/// exchange answered.
#[tokio::test]
async fn the_instance_url_override_wins_over_the_exchanges() {
    let server = MockServer::start().await;
    let app = connected_app(&server, "https://never.example").await;
    mount_query(
        &server,
        "/pinned",
        "LoginHistory",
        soql_page(&[login_row("0Ya1")], None),
    )
    .await;
    let mut cfg = jwt_config(&server, &app, &["login_history"]);
    cfg.instance_url_override = Some(format!("{}/pinned", server.uri()));
    let (outcome, rows) = run(config(cfg), Some(&window())).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["0Ya1"]);
    assert_eq!(
        paths(&server).await,
        [TOKEN_PATH, &format!("/pinned{QUERY}")]
    );
}

/// EventLogFile: the files of the window at the configured interval are
/// listed, each `LogFile` downloaded as CSV, and every CSV row lands as an
/// object stamped with its file's event type and log date.
#[tokio::test]
async fn event_log_file_lists_the_windows_files_and_lands_each_csv_row() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    let file = |id: &str, event_type: &str| json!({"attributes": {"type": "EventLogFile"}, "Id": id, "EventType": event_type, "LogDate": "2026-05-21T00:00:00.000+0000", "LogFileLength": 512.0, "Interval": "Daily"});
    mount_query(
        &server,
        "",
        "EventLogFile",
        soql_page(&[file("0AT1", "Login"), file("0AT2", "Logout")], None),
    )
    .await;
    mount_log_file(
        &server,
        "0AT1",
        "EVENT_TYPE,TIMESTAMP,USER_ID,URI\nLogin,20260521130405.123,005xx,\"/home,index\"\nLogin,20260521130505.456,005yy,/x\n",
    )
    .await;
    mount_log_file(
        &server,
        "0AT2",
        "EVENT_TYPE,TIMESTAMP\nLogout,20260521140000.000\n",
    )
    .await;

    let (outcome, rows) = run(
        config(jwt_config(&server, &app, &["event_log_file"])),
        Some(&window()),
    )
    .await;
    outcome.expect("fetch");
    assert_eq!(
        soqls(&server).await,
        [
            "SELECT Id, EventType, LogDate, LogFileLength, Interval FROM EventLogFile WHERE LogDate >= 2026-05-21T13:30:00Z AND LogDate < 2026-05-21T14:30:00Z AND Interval = 'Daily' ORDER BY LogDate ASC"
        ]
    );
    assert_eq!(
        paths(&server).await,
        [
            TOKEN_PATH,
            QUERY,
            "/services/data/v60.0/sobjects/EventLogFile/0AT1/LogFile",
            "/services/data/v60.0/sobjects/EventLogFile/0AT2/LogFile",
        ],
        "each file in list order"
    );
    let download = &requests_to(
        &server,
        "/services/data/v60.0/sobjects/EventLogFile/0AT1/LogFile",
    )
    .await[0];
    assert_eq!(
        header_of(download, "authorization"),
        Some("Bearer 00Dxx!token")
    );
    assert_eq!(
        header_of(download, "accept"),
        Some("*/*"),
        "a CSV, not JSON"
    );

    assert_eq!(rows.len(), 3);
    let first = enriched(&rows[0]);
    assert_eq!(
        first.row,
        json!({
            "EVENT_TYPE": "Login",
            "TIMESTAMP": "20260521130405.123",
            "USER_ID": "005xx",
            "URI": "/home,index",
            "_dfe_fetcher_event_type": "Login",
            "_dfe_fetcher_log_date": "2026-05-21T00:00:00.000+0000"
        }),
        "every column a string, the quoted comma kept, the file's stamps"
    );
    assert_eq!(first.source_fetcher, "salesforce.event_log_file");
    assert_eq!(rows[2].record["_dfe_fetcher_event_type"], "Logout");
}

/// The `interval` and `event_types` knobs narrow the listing.
#[tokio::test]
async fn event_log_file_knobs_narrow_the_listing() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    mount_query(&server, "", "EventLogFile", soql_page(&[], None)).await;
    let mut cfg = jwt_config(&server, &app, &[]);
    cfg.services = vec![service(
        "event_log_file",
        &[
            ("interval", json!("Hourly")),
            ("event_types", json!(["Login", "O'Reilly"])),
        ],
    )];
    let (outcome, rows) = run(config(cfg), Some(&window())).await;
    outcome.expect("fetch");
    assert!(rows.is_empty());
    assert_eq!(
        soqls(&server).await,
        [
            "SELECT Id, EventType, LogDate, LogFileLength, Interval FROM EventLogFile WHERE LogDate >= 2026-05-21T13:30:00Z AND LogDate < 2026-05-21T14:30:00Z AND Interval = 'Hourly' AND EventType IN ('Login','O\\'Reilly') ORDER BY LogDate ASC"
        ]
    );
}

/// At most two hundred log files are downloaded a tick, in list order.
#[tokio::test]
async fn at_most_200_log_files_are_downloaded_per_tick() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    let files: Vec<Value> = (1..=205)
        .map(|n| json!({"Id": format!("0AT{n:04}"), "EventType": "Login", "LogDate": "2026-05-21T00:00:00.000+0000", "LogFileLength": 1.0, "Interval": "Daily"}))
        .collect();
    mount_query(&server, "", "EventLogFile", soql_page(&files, None)).await;
    Mock::given(method("GET"))
        .and(path_regex(
            r"^/services/data/v60\.0/sobjects/EventLogFile/[^/]+/LogFile$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_string("EVENT_TYPE\nLogin\n"))
        .mount(&server)
        .await;
    let (outcome, rows) = run(
        config(jwt_config(&server, &app, &["event_log_file"])),
        Some(&window()),
    )
    .await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 200);
    let downloads: Vec<String> = paths(&server)
        .await
        .into_iter()
        .filter(|p| p.ends_with("/LogFile"))
        .collect();
    assert_eq!(downloads.len(), 200);
    assert!(downloads[0].contains("0AT0001"));
    assert!(downloads[199].contains("0AT0200"));
}

/// A `LogFile` the org keeps failing to serve is retried per the policy
/// and then fails the unit's tick, so the window is not advanced past it.
#[tokio::test]
async fn a_failing_log_file_is_retried_and_then_fails_the_tick() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    let file = |id: &str| json!({"Id": id, "EventType": "Login", "LogDate": "2026-05-21T00:00:00.000+0000", "LogFileLength": 1.0, "Interval": "Daily"});
    mount_query(
        &server,
        "",
        "EventLogFile",
        soql_page(&[file("0ATbad"), file("0ATgood")], None),
    )
    .await;
    Mock::given(method("GET"))
        .and(path(
            "/services/data/v60.0/sobjects/EventLogFile/0ATbad/LogFile",
        ))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_json(
                    json!([{"message": "storage unavailable", "errorCode": "SERVER_ERROR"}]),
                )
                .insert_header("Retry-After", "0"),
        )
        .mount(&server)
        .await;
    mount_log_file(&server, "0ATgood", "EVENT_TYPE\nLogin\n").await;
    let (outcome, rows) = run(
        config(jwt_config(&server, &app, &["event_log_file"])),
        Some(&window()),
    )
    .await;
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("storage unavailable"), "the API's text: {err}");
    assert!(rows.is_empty(), "{rows:?}");
    assert_eq!(
        requests_to(
            &server,
            "/services/data/v60.0/sobjects/EventLogFile/0ATbad/LogFile"
        )
        .await
        .len(),
        4,
        "the first attempt and three retries"
    );
    assert!(
        requests_to(
            &server,
            "/services/data/v60.0/sobjects/EventLogFile/0ATgood/LogFile"
        )
        .await
        .is_empty(),
        "the files after the failure wait for the next tick"
    );
}

/// A service the profile has no unit for is refused when the block is
/// mapped, naming it, before anything is exchanged.
#[tokio::test]
async fn an_unknown_service_is_refused_at_validation() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    let (outcome, rows) = run(
        config(jwt_config(
            &server,
            &app,
            &["real_time_events", "login_history"],
        )),
        Some(&window()),
    )
    .await;
    let err = outcome.expect_err("refused");
    assert!(err.contains("real_time_events"), "{err}");
    assert!(rows.is_empty());
    assert!(paths(&server).await.is_empty());
}

/// A refused exchange fails the tick with no query sent.
#[tokio::test]
async fn a_refused_token_exchange_fails_the_tick_and_queries_nothing() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    mount_query(
        &server,
        "",
        "LoginHistory",
        soql_page(&[login_row("0Ya1")], None),
    )
    .await;
    let (other_private, _) = rsa_key_pair();
    let mut cfg = jwt_config(&server, &app, &["login_history"]);
    cfg.private_key = Some(other_private.into());
    let (outcome, rows) = run(config(cfg), Some(&window())).await;
    let err = outcome.expect_err("the refusal is reported");
    assert!(err.contains("invalid_grant"), "{err}");
    assert!(rows.is_empty());
    assert!(requests_to(&server, QUERY).await.is_empty());
}

/// Two connections poll independently: each exchanges its own token and
/// tags its rows.
#[tokio::test]
async fn two_connections_poll_independently() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    mount_query(
        &server,
        "",
        "LoginHistory",
        soql_page(&[login_row("0Ya1")], None),
    )
    .await;
    let mut cfg = jwt_config(&server, &app, &["login_history"]);
    cfg.connections = vec![
        SalesforceConnection {
            id: "org_a".into(),
            username: Some("a@acme.example".into()),
            ..SalesforceConnection::default()
        },
        SalesforceConnection {
            id: "org_b".into(),
            username: Some("b@acme.example".into()),
            ..SalesforceConnection::default()
        },
    ];
    let cfg = config(cfg);
    let mut tags = Vec::new();
    for connection in ["org_a", "org_b"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(
            cfg.clone(),
            connection,
            Some(&window()),
        ))
        .await;
        outcome.expect("fetch");
        tags.push(enriched(&rows[0]).source_fetcher);
    }
    assert_eq!(
        tags,
        ["org_a.login_history", "org_b.login_history"],
        "each connection's rows carry its own id"
    );
    let subs: Vec<String> = app
        .exchanges
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_, claims)| {
            claims
                .as_ref()
                .map(|c| c["sub"].as_str().unwrap().to_owned())
        })
        .collect();
    assert_eq!(subs, ["a@acme.example", "b@acme.example"]);
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    let mut failed = login_row("0Ya2");
    failed["Status"] = Value::String("Invalid Password".into());
    mount_query(
        &server,
        "",
        "LoginHistory",
        soql_page(&[login_row("0Ya1"), failed], None),
    )
    .await;
    let mut cfg = jwt_config(&server, &app, &["login_history"]);
    cfg.filter = Some("Status != \"Success\"".into());
    let (outcome, rows) = run(config(cfg), Some(&window())).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["0Ya2"]);
}

/// No services: nothing is exchanged or queried and the tick is Ok.
#[tokio::test]
async fn no_services_requests_nothing() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    let (outcome, rows) = run(config(jwt_config(&server, &app, &[])), Some(&window())).await;
    outcome.expect("nothing to do is not a failure");
    assert!(rows.is_empty());
    assert!(paths(&server).await.is_empty());
}

/// The health check is the token exchange.
#[tokio::test]
async fn the_health_check_is_the_token_exchange() {
    let server = MockServer::start().await;
    let app = connected_app(&server, &server.uri()).await;
    let healthy = health(config(jwt_config(&server, &app, &["login_history"])))
        .await
        .expect("health");
    assert!(healthy);
    assert_eq!(paths(&server).await, [TOKEN_PATH]);
    let refused = health(config(
        secret_config(&server, &["login_history"]).with_secret("wrong"),
    ))
    .await;
    assert!(
        matches!(refused, Ok(false) | Err(_)),
        "a refused exchange is not healthy: {refused:?}"
    );
}

trait WithSecret {
    fn with_secret(self, secret: &str) -> Self;
}

impl WithSecret for SalesforceSourceConfig {
    fn with_secret(mut self, secret: &str) -> Self {
        self.client_secret = Some(secret.to_string().into());
        self
    }
}
