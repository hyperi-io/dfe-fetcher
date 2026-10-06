// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_aws.rs
// Purpose:   Characterisation of the AWS source: SigV4 scope per service, the JSON-1.x and REST-JSON shapes, paging, the record shape
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The AWS source against wiremock.
//!
//! Each test configures the typed `sources.aws` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the requests wiremock recorded (the `X-Amz-Target` and `Content-Type` of
//! each JSON-1.x call, the REST-JSON paths, the body each service sends,
//! the SigV4 credential scope naming the service and region the call was
//! signed for, `NextToken` paging) and the records that landed (the
//! provider's row, semantically, plus what enrichment added). The typed
//! config block is the operator's contract; the shipped `aws` profile
//! serves it through the framework driver.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use dfe_fetcher::config::{AwsConnection, AwsService, AwsSourceConfig, Config};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const JSON_1_1: &str = "application/x-amz-json-1.1";
const JSON_1_0: &str = "application/x-amz-json-1.0";
const LOOKUP_EVENTS: &str = "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents";
const FILTER_LOG_EVENTS: &str = "Logs_20140328.FilterLogEvents";
const LIST_METRICS: &str = "GraniteServiceVersion20100801.ListMetrics";
const GET_METRIC_DATA: &str = "GraniteServiceVersion20100801.GetMetricData";
const DESCRIBE_EVENTS: &str = "AWSHealth_20160804.DescribeEvents";
const SELECT_RESOURCE_CONFIG: &str = "StarlingDoveService.SelectResourceConfig";

/// A deployment config carrying `aws` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(aws: AwsSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.aws = aws;
    config
}

/// The typed block an operator writes: a region, the example key pair,
/// every service host pointed at wiremock, and the services.
fn account_config(server: &MockServer, services: Vec<AwsService>) -> AwsSourceConfig {
    AwsSourceConfig {
        enabled: true,
        region: "us-east-1".into(),
        access_key_id: Some(ACCESS_KEY.into()),
        secret_access_key: Some(SECRET_KEY.into()),
        endpoint_override: Some(server.uri()),
        services,
        topic: "test-aws".into(),
        ..AwsSourceConfig::default()
    }
}

fn service(name: &str, config: &[(&str, Value)]) -> AwsService {
    AwsService {
        name: name.into(),
        config: config
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    }
}

fn one(server: &MockServer, name: &str) -> Config {
    config(account_config(server, vec![service(name, &[])]))
}

/// One tick of the `aws` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "aws", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "aws")).await
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

/// A JSON-1.x mock: a POST to the service root carrying the target.
fn json_target(target: &str) -> wiremock::MockBuilder {
    Mock::given(method("POST"))
        .and(path("/"))
        .and(header("X-Amz-Target", target))
}

fn ok(body: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(body)
}

/// The requests wiremock saw carrying `X-Amz-Target: target`, in order.
async fn requests_targeting(server: &MockServer, target: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| header_of(r, "x-amz-target") == Some(target))
        .collect()
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

fn header_of<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request.headers.get(name).and_then(|v| v.to_str().ok())
}

fn body_of(request: &Request) -> Value {
    serde_json::from_slice(&request.body).unwrap_or(Value::Null)
}

/// The SigV4 facts a signed request carries: the credential scope names the
/// key, the region and the service the call was signed for, the payload
/// hash is the body's, and the signing date is present.
fn assert_signed(request: &Request, key: &str, region: &str, service: &str) {
    let authorization = header_of(request, "authorization").expect("Authorization");
    assert!(
        authorization.starts_with(&format!("AWS4-HMAC-SHA256 Credential={key}/")),
        "signed with the configured key: {authorization}"
    );
    assert!(
        authorization.contains(&format!("/{region}/{service}/aws4_request, SignedHeaders=")),
        "the scope names the service and region the call was signed for: {authorization}"
    );
    assert_eq!(
        header_of(request, "x-amz-content-sha256"),
        Some(hex::encode(Sha256::digest(&request.body)).as_str()),
        "the payload hash is the body's (UNSIGNED-PAYLOAD is refused off S3)"
    );
    assert!(header_of(request, "x-amz-date").is_some());
}

/// The landed record split from what enrichment added, checked against the
/// type's topic, `_source` and `_source_fetcher`.
fn assert_landed(row: &Landed, expected: &Value, unit: &str) {
    assert_eq!(row.topic, "test-aws_land");
    let e = enriched(row);
    assert_eq!(&e.row, expected, "the provider's row, semantically");
    assert_eq!(e.source, "test-aws");
    assert_eq!(e.source_fetcher, format!("aws.{unit}"));
}

fn ids(rows: &[Landed], key: &str) -> Vec<String> {
    rows.iter()
        .map(|r| r.record[key].as_str().unwrap_or_default().to_owned())
        .collect()
}

fn event(id: &str) -> Value {
    json!({"EventId": id, "EventName": "ConsoleLogin", "CloudTrailEvent": "{\"eventVersion\":\"1.08\"}"})
}

// =============================================================================
// Enablement and the health check
// =============================================================================

#[tokio::test]
async fn test_aws_disabled_returns_empty() {
    let server = MockServer::start().await;
    let mut cfg = account_config(&server, vec![service("cloudtrail", &[])]);
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
async fn test_aws_health_check_disabled() {
    let server = MockServer::start().await;
    let mut cfg = account_config(&server, vec![service("cloudtrail", &[])]);
    cfg.enabled = false;
    assert!(!matches!(health(config(cfg)).await, Ok(true)));
}

#[tokio::test]
async fn test_aws_health_check_no_credentials() {
    let server = MockServer::start().await;
    let mut cfg = account_config(&server, vec![service("cloudtrail", &[])]);
    cfg.access_key_id = None;
    cfg.secret_access_key = None;
    assert!(!matches!(health(config(cfg)).await, Ok(true)));
}

/// The health check proves the key against STS: a signed
/// `GetCallerIdentity`, and a refusal is an error rather than "healthy".
#[tokio::test]
async fn test_aws_health_check_with_credentials() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "<GetCallerIdentityResponse><GetCallerIdentityResult><Arn>arn:aws:iam::123456789012:user/test</Arn></GetCallerIdentityResult></GetCallerIdentityResponse>",
        ))
        .mount(&server)
        .await;
    assert!(matches!(health(one(&server, "cloudtrail")).await, Ok(true)));
    let seen = requests_to(&server, "/").await;
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].method, "GET");
    let mut query: Vec<(String, String)> = seen[0]
        .url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    query.sort();
    assert_eq!(
        query,
        [
            ("Action".to_string(), "GetCallerIdentity".to_string()),
            ("Version".to_string(), "2011-06-15".to_string())
        ]
    );
    assert_signed(&seen[0], ACCESS_KEY, "us-east-1", "sts");

    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(403).set_body_string(
            "<ErrorResponse><Error><Code>InvalidClientTokenId</Code></Error></ErrorResponse>",
        ))
        .mount(&server)
        .await;
    let refused = health(one(&server, "cloudtrail")).await;
    assert!(
        refused.as_ref().is_err_and(|e| e.contains("403")),
        "{refused:?}"
    );
}

// =============================================================================
// CloudTrail: JSON-1.1 LookupEvents on the service root
// =============================================================================

/// One JSON-1.1 POST to the CloudTrail root signed for `cloudtrail` in the
/// configured region, the window as epoch seconds with `MaxResults` 50,
/// and every event landed enriched on the type's topic.
#[tokio::test]
async fn test_aws_fetch_cloudtrail_success() {
    let server = MockServer::start().await;
    json_target(LOOKUP_EVENTS)
        .respond_with(ok(json!({"Events": [event("evt-1"), event("evt-2")]})))
        .mount(&server)
        .await;
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-22T13:30:00Z");

    let (outcome, rows) = run(one(&server, "cloudtrail"), Some(&w)).await;
    outcome.expect("fetch");

    let seen = requests_targeting(&server, LOOKUP_EVENTS).await;
    assert_eq!(seen.len(), 1);
    assert_eq!(header_of(&seen[0], "content-type"), Some(JSON_1_1));
    assert_eq!(
        body_of(&seen[0]),
        json!({"StartTime": w.start.timestamp(), "EndTime": w.end.timestamp(), "MaxResults": 50}),
        "the window as epoch-second NUMBERS, 50 a page"
    );
    assert_signed(&seen[0], ACCESS_KEY, "us-east-1", "cloudtrail");

    assert_eq!(rows.len(), 2);
    for (row, id) in rows.iter().zip(["evt-1", "evt-2"]) {
        let mut expected = event(id);
        // The deployment's `unwrap_nested_json` (on by default) parses the
        // JSON-encoded `CloudTrailEvent` string into the event it holds.
        expected["CloudTrailEvent"] = json!({"eventVersion": "1.08"});
        assert_landed(row, &expected, "cloudtrail");
    }
}

#[tokio::test]
async fn test_aws_fetch_cloudtrail_empty() {
    let server = MockServer::start().await;
    json_target(LOOKUP_EVENTS)
        .respond_with(ok(json!({"Events": []})))
        .mount(&server)
        .await;
    let (outcome, rows) = run(one(&server, "cloudtrail"), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty());
}

/// Without a window the lookback is the last hour.
#[tokio::test]
async fn test_aws_default_lookback_is_one_hour() {
    let server = MockServer::start().await;
    json_target(LOOKUP_EVENTS)
        .respond_with(ok(json!({"Events": []})))
        .mount(&server)
        .await;
    let before = Utc::now();
    let (outcome, _) = run(one(&server, "cloudtrail"), None).await;
    outcome.expect("fetch");
    let body = body_of(&requests_targeting(&server, LOOKUP_EVENTS).await[0]);
    let start = body["StartTime"].as_i64().unwrap();
    let end = body["EndTime"].as_i64().unwrap();
    assert!((end - start - 3600).abs() <= 2, "start={start} end={end}");
    assert!((end - before.timestamp()).abs() <= 5);
}

/// LookupEvents answers `NextToken` when more than a page of events match.
/// The legacy source never sent it back (one request, the first 50 landed,
/// the rest of the window was lost); the profile feeds the token back in
/// the body beside the unchanged window until the API stops answering one.
#[tokio::test]
async fn test_aws_fetch_cloudtrail_pagination() {
    let server = MockServer::start().await;
    json_target(LOOKUP_EVENTS)
        .respond_with(ok(
            json!({"Events": [event("evt-1"), event("evt-2")], "NextToken": "page-2"}),
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    json_target(LOOKUP_EVENTS)
        .respond_with(ok(json!({"Events": [event("evt-3"), event("evt-4")]})))
        .mount(&server)
        .await;

    let (outcome, rows) = run(one(&server, "cloudtrail"), None).await;
    outcome.expect("fetch");
    let seen = requests_targeting(&server, LOOKUP_EVENTS).await;
    assert_eq!(seen.len(), 2, "the NextToken is followed");
    assert!(body_of(&seen[0]).get("NextToken").is_none());
    let second = body_of(&seen[1]);
    assert_eq!(second["NextToken"], "page-2");
    assert_eq!(second["MaxResults"], 50);
    assert_eq!(second["StartTime"], body_of(&seen[0])["StartTime"]);
    assert_eq!(
        ids(&rows, "EventId"),
        ["evt-1", "evt-2", "evt-3", "evt-4"],
        "the whole window lands"
    );
}

/// Requests a second LookupEvents allows per account and region.
const PACED_TPS: f64 = 2.0;

/// What the paced mock has seen: when each request arrived, and the tokens
/// left after the last one.
struct Paced {
    seen: Vec<Instant>,
    tokens: f64,
}

impl Paced {
    /// Take a token for a request arriving at `now`, having refilled for the
    /// time since the previous one; `false` is the refusal. The bucket holds
    /// one request of burst over the rate, as the API's does, so a sequence
    /// paced at 2 a second is never refused for the latency an arrival
    /// carries while a burst is refused on its third request.
    fn take(&mut self, now: Instant) -> bool {
        if let Some(last) = self.seen.last() {
            let refill = now.duration_since(*last).as_secs_f64() * PACED_TPS;
            self.tokens = (self.tokens + refill).min(PACED_TPS);
        }
        self.seen.push(now);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

/// LookupEvents as AWS paces it: a page of `per_page` events with a
/// `NextToken` until page `pages`, and anything faster than the documented
/// rate refused the way the API refuses it.
struct PacedLookupEvents {
    pages: usize,
    per_page: usize,
    state: Arc<Mutex<Paced>>,
}

impl Respond for PacedLookupEvents {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        if !self.state.lock().unwrap().take(Instant::now()) {
            return ResponseTemplate::new(400).set_body_json(
                json!({"__type": "ThrottlingException", "message": "Rate exceeded"}),
            );
        }
        // The page comes off the token the pager fed back, so a retried
        // request is answered with the page it asked for.
        let page: usize = body_of(request)["NextToken"]
            .as_str()
            .and_then(|token| token.strip_prefix("page-"))
            .and_then(|n| n.parse().ok())
            .unwrap_or(1);
        let events: Vec<Value> = (0..self.per_page)
            .map(|i| event(&format!("evt-{page}-{i}")))
            .collect();
        let mut body = json!({"Events": events});
        if page < self.pages {
            body["NextToken"] = json!(format!("page-{}", page + 1));
        }
        ok(body)
    }
}

/// Mount `pages` pages of paced LookupEvents, handing back what the mock
/// records as each request arrives.
async fn mount_paced(server: &MockServer, pages: usize, per_page: usize) -> Arc<Mutex<Paced>> {
    let state = Arc::new(Mutex::new(Paced {
        seen: Vec::new(),
        tokens: PACED_TPS,
    }));
    json_target(LOOKUP_EVENTS)
        .respond_with(PacedLookupEvents {
            pages,
            per_page,
            state: Arc::clone(&state),
        })
        .mount(server)
        .await;
    state
}

/// LookupEvents allows 2 requests a second, and the unit's declared rate
/// holds its page sequence to that, so a window several pages wide drains
/// with the API never refusing a call. (Without the rate the third request
/// of the window came back 400 `ThrottlingException`, which the retry
/// policy did not recognise, and the tick failed with the window unfetched.)
#[tokio::test]
async fn test_aws_cloudtrail_paces_pages_to_the_documented_limit() {
    let server = MockServer::start().await;
    let paced = mount_paced(&server, 6, 50).await;

    let (outcome, rows) = run(one(&server, "cloudtrail"), None).await;
    outcome.expect("fetch");

    assert_eq!(
        requests_targeting(&server, LOOKUP_EVENTS).await.len(),
        6,
        "six pages, none of them refused"
    );
    assert_eq!(rows.len(), 300, "fifty events a page, every page landed");
    let landed = ids(&rows, "EventId");
    assert_eq!(landed.first().map(String::as_str), Some("evt-1-0"));
    assert_eq!(
        landed.last().map(String::as_str),
        Some("evt-6-49"),
        "the pages land in order"
    );

    let seen = paced.lock().unwrap().seen.clone();
    assert_eq!(seen.len(), 6, "the mock was asked six times, refusing none");
    let span = seen[5].duration_since(seen[0]);
    // Five waits of 500ms, less the latency the first arrival carried; an
    // unpaced sequence spans a few milliseconds.
    assert!(
        span >= Duration::from_millis(2_000),
        "six pages at 2 a second cannot arrive inside {span:?}"
    );
}

/// A declared throttle is retried rather than failing the tick: the profile
/// names AWS's 400 `ThrottlingException` as one, so the next attempt lands
/// the page. (It read as a plain 400 before, so it was never retried and
/// never counted a throttle.)
#[tokio::test]
async fn test_aws_cloudtrail_throttle_is_retried_then_lands() {
    let server = MockServer::start().await;
    json_target(LOOKUP_EVENTS)
        .respond_with(
            ResponseTemplate::new(400).set_body_json(
                json!({"__type": "ThrottlingException", "message": "Rate exceeded"}),
            ),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    json_target(LOOKUP_EVENTS)
        .respond_with(ok(json!({"Events": [event("evt-1")]})))
        .mount(&server)
        .await;

    let (outcome, rows) = run(one(&server, "cloudtrail"), None).await;
    outcome.expect("fetch");
    assert_eq!(
        requests_targeting(&server, LOOKUP_EVENTS).await.len(),
        2,
        "the throttle is retried"
    );
    assert_eq!(ids(&rows, "EventId"), ["evt-1"]);
}

/// A window wider than the default page ceiling drains in one tick: the unit
/// raises its own ceiling, so the window is not abandoned with rows
/// unfetched. (At the profile default of 50 pages a busier account failed
/// every tick with `PageCeiling` and never advanced its cursor.)
#[tokio::test]
async fn test_aws_cloudtrail_window_wider_than_the_default_ceiling_drains() {
    let server = MockServer::start().await;
    mount_paced(&server, 60, 1).await;

    let (outcome, rows) = run(one(&server, "cloudtrail"), None).await;
    let failure = outcome.err().unwrap_or_default();
    assert!(
        !failure.contains("max_pages"),
        "the ceiling must not cut a window short: {failure}"
    );
    assert!(failure.is_empty(), "{failure}");
    assert_eq!(rows.len(), 60, "sixty pages of one event each");
}

/// A 5xx from the provider is retried, then fails the tick, so the
/// scheduler does not advance past the window (the legacy swallowed it
/// after one request and reported a clean tick).
#[tokio::test]
async fn test_aws_fetch_error_500() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&server)
        .await;
    let (outcome, rows) = run(one(&server, "cloudtrail"), None).await;
    assert!(rows.is_empty());
    let seen = requests_targeting(&server, LOOKUP_EVENTS).await;
    assert_eq!(seen.len(), 4, "three retries of a read POST");
    let err = outcome.expect_err("the tick fails");
    assert!(err.contains("500"), "{err}");
}

/// A 403 is never retried.
#[tokio::test]
async fn test_aws_fetch_error_403_not_retried() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(403).set_body_json(
                json!({"__type": "AccessDeniedException", "message": "not authorized"}),
            ),
        )
        .mount(&server)
        .await;
    let (_, rows) = run(one(&server, "cloudtrail"), None).await;
    assert!(rows.is_empty());
    assert_eq!(requests_targeting(&server, LOOKUP_EVENTS).await.len(), 1);
}

/// The per-source CEL filter runs on the provider's row.
#[tokio::test]
async fn test_aws_fetch_filter_drops_records() {
    let server = MockServer::start().await;
    json_target(LOOKUP_EVENTS)
        .respond_with(ok(json!({"Events": [
            {"EventId": "evt-1", "EventName": "ConsoleLogin"},
            {"EventId": "evt-2", "EventName": "AssumeRole"}
        ]})))
        .mount(&server)
        .await;
    let mut cfg = account_config(&server, vec![service("cloudtrail", &[])]);
    cfg.filter = Some(r#"EventName != "ConsoleLogin""#.into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows, "EventId"), ["evt-2"]);
}

/// `credential_secret` resolves to a JSON document carrying both halves of
/// the key pair; the request is signed with the key id it names.
#[tokio::test]
async fn test_aws_credential_secret_json() {
    unsafe {
        std::env::set_var(
            "DFE_FETCHER_TEST_AWS_CREDENTIALS_JSON",
            r#"{"access_key_id": "AKIAFROMVAULTEXAMPLE", "secret_access_key": "vault-secret"}"#,
        );
    }
    let server = MockServer::start().await;
    json_target(LOOKUP_EVENTS)
        .respond_with(ok(json!({"Events": [event("evt-1")]})))
        .mount(&server)
        .await;
    let mut cfg = account_config(&server, vec![service("cloudtrail", &[])]);
    cfg.access_key_id = None;
    cfg.secret_access_key = None;
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_AWS_CREDENTIALS_JSON".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    let seen = requests_targeting(&server, LOOKUP_EVENTS).await;
    assert_signed(&seen[0], "AKIAFROMVAULTEXAMPLE", "us-east-1", "cloudtrail");
}

/// Two connections of the block: each signs with its own key and region.
#[tokio::test]
async fn test_aws_two_connections_sign_with_their_own_keys() {
    let server = MockServer::start().await;
    json_target(LOOKUP_EVENTS)
        .respond_with(ok(json!({"Events": [event("evt-1")]})))
        .mount(&server)
        .await;
    let mut cfg = account_config(&server, vec![service("cloudtrail", &[])]);
    cfg.access_key_id = None;
    cfg.secret_access_key = None;
    cfg.connections = vec![
        AwsConnection {
            id: "acct-123".into(),
            region: Some("eu-west-1".into()),
            access_key_id: Some("AKIAACCT123EXAMPLE00".into()),
            secret_access_key: Some("secret-123".to_string().into()),
            ..AwsConnection::default()
        },
        AwsConnection {
            id: "acct-456".into(),
            access_key_id: Some("AKIAACCT456EXAMPLE00".into()),
            secret_access_key: Some("secret-456".to_string().into()),
            ..AwsConnection::default()
        },
    ];
    let config = config(cfg);
    for (id, key, region) in [
        ("acct-123", "AKIAACCT123EXAMPLE00", "eu-west-1"),
        ("acct-456", "AKIAACCT456EXAMPLE00", "us-east-1"),
    ] {
        server.reset().await;
        json_target(LOOKUP_EVENTS)
            .respond_with(ok(json!({"Events": [event("evt-1")]})))
            .mount(&server)
            .await;
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("fetch");
        assert_eq!(rows.len(), 1, "{id}");
        assert_eq!(
            enriched(&rows[0]).source_fetcher,
            format!("{id}.cloudtrail"),
            "each connection is told apart by its id"
        );
        let seen = requests_targeting(&server, LOOKUP_EVENTS).await;
        assert_signed(&seen[0], key, region, "cloudtrail");
    }
}

/// A service the block lists but the profile has no unit for is refused
/// when the block is mapped (the legacy warned every tick and fetched
/// nothing).
#[tokio::test]
async fn test_aws_unknown_service_is_refused() {
    let server = MockServer::start().await;
    let (outcome, rows) = run(one(&server, "lambda_logs"), None).await;
    assert!(rows.is_empty());
    let err = outcome.expect_err("refused");
    assert!(err.contains("lambda_logs"), "{err}");
    assert!(
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty()
    );
}

// =============================================================================
// GuardDuty: ListDetectors -> ListFindings (paged) -> GetFindings in batches
// =============================================================================

/// The documented REST-JSON chain: `GET /detector` for the detector ids,
/// `POST /detector/{id}/findings` for each detector's finding ids, and
/// `POST /detector/{id}/findings/get` for the findings, every call signed
/// for `guardduty`. (The legacy sent three JSON-1.1 targets to the service
/// root, which GuardDuty answers with a 403 "Unable to determine
/// service/operation name".)
#[tokio::test]
async fn test_aws_fetch_guardduty_chain() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/detector"))
        .respond_with(ok(json!({"detectorIds": ["detector-1"]})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/detector/detector-1/findings"))
        .respond_with(ok(json!({"findingIds": ["finding-1", "finding-2"]})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/detector/detector-1/findings/get"))
        .respond_with(ok(json!({"findings": [
            {"id": "finding-1", "severity": 8.0, "type": "Recon:EC2/PortProbeUnprotectedPort"},
            {"id": "finding-2", "severity": 5.0, "type": "UnauthorizedAccess:EC2/SSHBruteForce"}
        ]})))
        .mount(&server)
        .await;

    let (outcome, rows) = run(one(&server, "guardduty"), None).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows, "id"), ["finding-1", "finding-2"]);
    for row in &rows {
        assert_eq!(enriched(row).source_fetcher, "aws.guardduty");
    }
    let detectors = requests_to(&server, "/detector").await;
    assert_eq!(detectors.len(), 1);
    assert_signed(&detectors[0], ACCESS_KEY, "us-east-1", "guardduty");
    let list = requests_to(&server, "/detector/detector-1/findings").await;
    assert_eq!(list.len(), 1);
    assert_eq!(
        header_of(&list[0], "content-type"),
        Some("application/json")
    );
    assert!(header_of(&list[0], "x-amz-target").is_none());
    assert_eq!(body_of(&list[0]), json!({"maxResults": 50}));
    assert_signed(&list[0], ACCESS_KEY, "us-east-1", "guardduty");
    let get = requests_to(&server, "/detector/detector-1/findings/get").await;
    assert_eq!(get.len(), 1);
    assert_eq!(
        body_of(&get[0]),
        json!({"findingIds": ["finding-1", "finding-2"]})
    );
}

/// ListFindings pages by `nextToken` (the legacy never followed it, so a
/// detector with more than 50 findings lost the rest); every page's ids
/// are looked up under the detector that listed them, 50 a call.
#[tokio::test]
async fn test_aws_fetch_guardduty_pagination() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/detector"))
        .respond_with(ok(json!({"detectorIds": ["detector-1"]})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/detector/detector-1/findings"))
        .respond_with(ok(
            json!({"findingIds": ["finding-1", "finding-2"], "nextToken": "page-2"}),
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/detector/detector-1/findings"))
        .respond_with(ok(json!({"findingIds": ["finding-3"]})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/detector/detector-1/findings/get"))
        .respond_with(FindingsForIds)
        .mount(&server)
        .await;

    let (outcome, rows) = run(one(&server, "guardduty"), None).await;
    outcome.expect("fetch");
    let list = requests_to(&server, "/detector/detector-1/findings").await;
    assert_eq!(list.len(), 2, "the nextToken is followed");
    assert_eq!(
        body_of(&list[1]),
        json!({"maxResults": 50, "nextToken": "page-2"})
    );
    let get = requests_to(&server, "/detector/detector-1/findings/get").await;
    assert_eq!(get.len(), 1, "three ids fit one call of 50");
    assert_eq!(
        body_of(&get[0])["findingIds"],
        json!(["finding-1", "finding-2", "finding-3"])
    );
    assert_eq!(ids(&rows, "id"), ["finding-1", "finding-2", "finding-3"]);
}

/// GetFindings answers one finding per id asked for.
struct FindingsForIds;

impl Respond for FindingsForIds {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body = body_of(request);
        let ids = body
            .get("FindingIds")
            .or_else(|| body.get("findingIds"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let findings: Vec<Value> = ids
            .iter()
            .filter_map(Value::as_str)
            .map(|id| json!({"id": id, "severity": 5.0}))
            .collect();
        ok(json!({"Findings": findings, "findings": findings}))
    }
}

// =============================================================================
// Security Hub: GetFindings over the window on UpdatedAt, paged
// =============================================================================

/// One finding in each workflow status, as ASFF records it under
/// `Workflow.Status`.
fn findings_in_every_status() -> Vec<Value> {
    ["NEW", "NOTIFIED", "RESOLVED", "SUPPRESSED"]
        .into_iter()
        .map(|status| {
            json!({
                "Id": format!("finding-{}", status.to_ascii_lowercase()),
                "Title": "S3 bucket public",
                "Workflow": {"Status": status}
            })
        })
        .collect()
}

/// `GetFindings` as far as its workflow filter goes: the findings whose
/// `Workflow.Status` the request's `WorkflowStatus` filter lists, or every one
/// when the request sends no such filter.
struct FindingsByStatus(Vec<Value>);

impl Respond for FindingsByStatus {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body = body_of(request);
        let wanted: Option<Vec<&str>> = body["Filters"]["WorkflowStatus"]
            .as_array()
            .map(|filters| filters.iter().filter_map(|f| f["Value"].as_str()).collect());
        let findings: Vec<&Value> = self
            .0
            .iter()
            .filter(|f| {
                wanted.as_ref().is_none_or(|statuses| {
                    statuses.contains(&f["Workflow"]["Status"].as_str().unwrap_or_default())
                })
            })
            .collect();
        ok(json!({ "Findings": findings }))
    }
}

/// The documented REST-JSON `POST /findings`, 100 a page, signed for
/// `securityhub`, asking for every finding whose record changed in the window
/// and naming no workflow status. A resolved, suppressed or notified finding
/// lands beside a new one, where a NEW-only filter lost all three.
#[tokio::test]
async fn test_aws_fetch_securityhub_success() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/findings"))
        .respond_with(FindingsByStatus(findings_in_every_status()))
        .mount(&server)
        .await;
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(one(&server, "securityhub"), Some(&w)).await;
    outcome.expect("fetch");
    assert_eq!(
        ids(&rows, "Id"),
        [
            "finding-new",
            "finding-notified",
            "finding-resolved",
            "finding-suppressed"
        ],
        "a finding in every workflow status arrives"
    );
    for row in &rows {
        assert_eq!(enriched(row).source_fetcher, "aws.securityhub");
    }
    let seen = requests_to(&server, "/findings").await;
    assert_eq!(seen.len(), 1);
    assert_eq!(
        header_of(&seen[0], "content-type"),
        Some("application/json")
    );
    assert!(header_of(&seen[0], "x-amz-target").is_none());
    assert_eq!(
        body_of(&seen[0]),
        json!({
            "Filters": {"UpdatedAt": [{"Start": "2026-05-21T13:30:00.987Z", "End": "2026-05-21T14:30:00.000Z"}]},
            "MaxResults": 100
        }),
        "the window on UpdatedAt, and no workflow filter"
    );
    assert_signed(&seen[0], ACCESS_KEY, "us-east-1", "securityhub");
}

/// `workflow_status` narrows the unit to the statuses listed, sent as
/// `WorkflowStatus` beside the window.
#[tokio::test]
async fn test_aws_securityhub_workflow_status_narrows_the_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/findings"))
        .respond_with(FindingsByStatus(findings_in_every_status()))
        .mount(&server)
        .await;
    let w = fetch_window("2026-05-21T13:30:00Z", "2026-05-21T14:30:00Z");
    let cfg = account_config(
        &server,
        vec![service(
            "securityhub",
            &[("workflow_status", json!(["RESOLVED", "SUPPRESSED"]))],
        )],
    );

    let (outcome, rows) = run(config(cfg), Some(&w)).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows, "Id"), ["finding-resolved", "finding-suppressed"]);
    let seen = requests_to(&server, "/findings").await;
    assert_eq!(
        body_of(&seen[0])["Filters"],
        json!({
            "UpdatedAt": [{"Start": "2026-05-21T13:30:00.000Z", "End": "2026-05-21T14:30:00.000Z"}],
            "WorkflowStatus": [
                {"Value": "RESOLVED", "Comparison": "EQUALS"},
                {"Value": "SUPPRESSED", "Comparison": "EQUALS"}
            ]
        })
    );
}

/// GetFindings pages by `NextToken` (the legacy never followed it): the
/// token is fed back beside the unchanged filter.
#[tokio::test]
async fn test_aws_fetch_securityhub_pagination() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/findings"))
        .respond_with(ok(
            json!({"Findings": [{"Id": "finding-1"}], "NextToken": "page-2"}),
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/findings"))
        .respond_with(ok(json!({"Findings": [{"Id": "finding-2"}]})))
        .mount(&server)
        .await;
    let w = fetch_window("2026-05-21T13:30:00Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(one(&server, "securityhub"), Some(&w)).await;
    outcome.expect("fetch");
    let seen = requests_to(&server, "/findings").await;
    assert_eq!(seen.len(), 2, "the NextToken is followed");
    let second = body_of(&seen[1]);
    assert_eq!(second["NextToken"], "page-2");
    assert_eq!(second["MaxResults"], 100);
    assert_eq!(
        second["Filters"],
        body_of(&seen[0])["Filters"],
        "the filter rides along"
    );
    assert_eq!(
        second["Filters"]["UpdatedAt"][0]["Start"],
        "2026-05-21T13:30:00.000Z"
    );
    assert_eq!(ids(&rows, "Id"), ["finding-1", "finding-2"]);
}

// =============================================================================
// AWS Config: SelectResourceConfig, Results are JSON-encoded strings
// =============================================================================

/// `SelectResourceConfig` with an expression selecting every resource's
/// configuration, 100 a page, `NextToken` followed; each `Results` element
/// is a JSON-encoded string and lands as the document it holds. (The
/// legacy sent `SelectAggregateResourceConfig` with `{"limit": 100}`, which
/// AWS refuses outright, so the unit never landed a row.)
#[tokio::test]
async fn test_aws_fetch_config_resources() {
    let server = MockServer::start().await;
    json_target(SELECT_RESOURCE_CONFIG)
        .respond_with(ok(json!({
            "QueryInfo": {"SelectFields": [{"Name": "resourceId"}]},
            "Results": [
                "{\"resourceId\":\"i-1\",\"resourceType\":\"AWS::EC2::Instance\",\"configuration\":{\"instanceType\":\"t2.micro\"}}"
            ],
            "NextToken": "page-2"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    json_target(SELECT_RESOURCE_CONFIG)
        .respond_with(ok(json!({
            "QueryInfo": {"SelectFields": [{"Name": "resourceId"}]},
            "Results": [
                "{\"resourceId\":\"vol-1\",\"resourceType\":\"AWS::EC2::Volume\",\"configuration\":{\"size\":100}}"
            ]
        })))
        .mount(&server)
        .await;

    let (outcome, rows) = run(one(&server, "config"), None).await;
    outcome.expect("fetch");
    let seen = requests_targeting(&server, SELECT_RESOURCE_CONFIG).await;
    assert_eq!(seen.len(), 2, "the NextToken is followed");
    assert_eq!(header_of(&seen[0], "content-type"), Some(JSON_1_1));
    let first = body_of(&seen[0]);
    let expression = first["Expression"].as_str().expect("an Expression");
    assert!(
        expression.starts_with("SELECT ") && expression.contains("configuration"),
        "every resource's configuration: {expression}"
    );
    assert_eq!(first["Limit"], 100);
    assert!(first.get("NextToken").is_none());
    assert_eq!(body_of(&seen[1])["NextToken"], "page-2");
    assert_signed(&seen[0], ACCESS_KEY, "us-east-1", "config");
    assert_eq!(rows.len(), 2);
    assert_landed(
        &rows[0],
        &json!({"resourceId": "i-1", "resourceType": "AWS::EC2::Instance", "configuration": {"instanceType": "t2.micro"}}),
        "config",
    );
    assert_eq!(rows[1].record["resourceId"], "vol-1");

    let cfg = account_config(
        &server,
        vec![service(
            "config",
            &[(
                "expression",
                json!("SELECT resourceId WHERE resourceType = 'AWS::S3::Bucket'"),
            )],
        )],
    );
    server.reset().await;
    json_target(SELECT_RESOURCE_CONFIG)
        .respond_with(ok(json!({"Results": []})))
        .mount(&server)
        .await;
    let (outcome, _) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(
        body_of(&requests_targeting(&server, SELECT_RESOURCE_CONFIG).await[0])["Expression"],
        "SELECT resourceId WHERE resourceType = 'AWS::S3::Bucket'",
        "the expression knob replaces the default"
    );
}

// =============================================================================
// CloudWatch Logs: FilterLogEvents, paged by nextToken
// =============================================================================

/// The log group and the window as epoch MILLISECONDS with `limit` 10000,
/// no `filterPattern` unless configured, signed for `logs`.
#[tokio::test]
async fn test_aws_fetch_cloudwatch_logs_success() {
    let server = MockServer::start().await;
    json_target(FILTER_LOG_EVENTS)
        .respond_with(ok(json!({"events": [
            {"eventId": "ev-1", "logStreamName": "stream-1", "message": "INFO hello"},
            {"eventId": "ev-2", "logStreamName": "stream-1", "message": "ERROR fail"},
            {"eventId": "ev-3", "logStreamName": "stream-2", "message": "WARN something"}
        ]})))
        .mount(&server)
        .await;
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-22T13:30:00Z");
    let cfg = account_config(
        &server,
        vec![service(
            "cloudwatch_logs",
            &[("log_group_name", json!("/aws/lambda/test"))],
        )],
    );
    let (outcome, rows) = run(config(cfg), Some(&w)).await;
    outcome.expect("fetch");

    let seen = requests_targeting(&server, FILTER_LOG_EVENTS).await;
    assert_eq!(seen.len(), 1);
    assert_eq!(header_of(&seen[0], "content-type"), Some(JSON_1_1));
    assert_eq!(
        body_of(&seen[0]),
        json!({"logGroupName": "/aws/lambda/test", "startTime": w.start.timestamp_millis(), "endTime": w.end.timestamp_millis(), "limit": 10000}),
        "the window in milliseconds, the millisecond kept"
    );
    assert_signed(&seen[0], ACCESS_KEY, "us-east-1", "logs");
    assert_eq!(ids(&rows, "eventId"), ["ev-1", "ev-2", "ev-3"]);
    for row in &rows {
        assert_eq!(enriched(row).source_fetcher, "aws.cloudwatch_logs");
    }
}

#[tokio::test]
async fn test_aws_fetch_cloudwatch_logs_empty() {
    let server = MockServer::start().await;
    json_target(FILTER_LOG_EVENTS)
        .respond_with(ok(json!({"events": []})))
        .mount(&server)
        .await;
    let cfg = account_config(
        &server,
        vec![service(
            "cloudwatch_logs",
            &[("log_group_name", json!("/aws/lambda/test"))],
        )],
    );
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty());
}

/// `nextToken` is fed back in the body beside the unchanged window.
#[tokio::test]
async fn test_aws_fetch_cloudwatch_logs_pagination() {
    let server = MockServer::start().await;
    json_target(FILTER_LOG_EVENTS)
        .respond_with(ok(
            json!({"events": [{"eventId": "ev-1", "message": "first page"}], "nextToken": "page2token"}),
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    json_target(FILTER_LOG_EVENTS)
        .respond_with(ok(
            json!({"events": [{"eventId": "ev-2", "message": "second page"}]}),
        ))
        .mount(&server)
        .await;
    let cfg = account_config(
        &server,
        vec![service(
            "cloudwatch_logs",
            &[("log_group_name", json!("/aws/lambda/paginated"))],
        )],
    );
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let seen = requests_targeting(&server, FILTER_LOG_EVENTS).await;
    assert_eq!(seen.len(), 2);
    let second = body_of(&seen[1]);
    assert_eq!(second["nextToken"], "page2token");
    assert_eq!(second["logGroupName"], "/aws/lambda/paginated");
    assert_eq!(second["limit"], 10000);
    assert_eq!(ids(&rows, "eventId"), ["ev-1", "ev-2"]);
}

/// A `filter_pattern` knob is sent as `filterPattern`.
#[tokio::test]
async fn test_aws_fetch_cloudwatch_logs_filter_pattern() {
    let server = MockServer::start().await;
    json_target(FILTER_LOG_EVENTS)
        .respond_with(ok(
            json!({"events": [{"eventId": "ev-1", "message": "ERROR"}]}),
        ))
        .mount(&server)
        .await;
    let cfg = account_config(
        &server,
        vec![service(
            "cloudwatch_logs",
            &[
                ("log_group_name", json!("/aws/lambda/test")),
                ("filter_pattern", json!("ERROR")),
            ],
        )],
    );
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    let seen = requests_targeting(&server, FILTER_LOG_EVENTS).await;
    assert_eq!(body_of(&seen[0])["filterPattern"], "ERROR");
}

// =============================================================================
// CloudWatch Metrics: ListMetrics per namespace -> GetMetricData in batches
// =============================================================================

fn metric(name: &str, unit: Option<&str>) -> Value {
    let mut m = json!({
        "Namespace": "AWS/EC2",
        "MetricName": name,
        "Dimensions": [{"Name": "InstanceId", "Value": "i-1234"}]
    });
    if let Some(unit) = unit {
        m["Unit"] = json!(unit);
    }
    m
}

/// GetMetricData answers every query asked for with two datapoints; the
/// value carries the query's position so a test can see the join.
struct DataForQueries;

impl Respond for DataForQueries {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body = body_of(request);
        let results: Vec<Value> = body["MetricDataQueries"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .enumerate()
            .map(|(i, q)| {
                json!({
                    "Id": q["Id"],
                    "Label": q["MetricStat"]["Metric"]["MetricName"],
                    "Timestamps": [1_709_424_000.0, 1_709_424_300.0],
                    "Values": [45.2 + i as f64, 62.1 + i as f64],
                    "StatusCode": "Complete"
                })
            })
            .collect();
        ok(json!({"MetricDataResults": results}))
    }
}

/// ListMetrics (JSON-1.0, signed for `monitoring`) per namespace, then one
/// GetMetricData with every metric as a query over the window with the
/// default period and statistic; one JSON row per datapoint carrying the
/// metric's namespace, name, dimensions, unit and stat.
#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_success() {
    let server = MockServer::start().await;
    json_target(LIST_METRICS)
        .respond_with(ok(
            json!({"Metrics": [metric("CPUUtilization", Some("Percent"))]}),
        ))
        .mount(&server)
        .await;
    json_target(GET_METRIC_DATA)
        .respond_with(DataForQueries)
        .mount(&server)
        .await;
    let w = fetch_window("2026-05-21T13:30:00Z", "2026-05-21T14:30:00Z");
    let cfg = account_config(
        &server,
        vec![service(
            "cloudwatch_metrics",
            &[("namespaces", json!(["AWS/EC2"]))],
        )],
    );
    let (outcome, rows) = run(config(cfg), Some(&w)).await;
    outcome.expect("fetch");

    let list = requests_targeting(&server, LIST_METRICS).await;
    assert_eq!(list.len(), 1);
    assert_eq!(header_of(&list[0], "content-type"), Some(JSON_1_0));
    assert_eq!(body_of(&list[0]), json!({"Namespace": "AWS/EC2"}));
    assert_signed(&list[0], ACCESS_KEY, "us-east-1", "monitoring");
    let get = requests_targeting(&server, GET_METRIC_DATA).await;
    assert_eq!(get.len(), 1);
    assert_eq!(
        body_of(&get[0]),
        json!({
            "StartTime": w.start.timestamp(),
            "EndTime": w.end.timestamp(),
            "MetricDataQueries": [{
                "Id": "q0",
                "MetricStat": {
                    "Metric": {"Namespace": "AWS/EC2", "MetricName": "CPUUtilization", "Dimensions": [{"Name": "InstanceId", "Value": "i-1234"}]},
                    "Period": 300,
                    "Stat": "Average"
                }
            }]
        })
    );

    assert_eq!(rows.len(), 2, "one row per datapoint");
    assert_landed(
        &rows[0],
        &json!({
            "namespace": "AWS/EC2",
            "metric_name": "CPUUtilization",
            "dimensions": [{"Name": "InstanceId", "Value": "i-1234"}],
            "unit": "Percent",
            "timestamp": 1_709_424_000_000_i64,
            "value": 45.2,
            "stat": "Average"
        }),
        "cloudwatch_metrics",
    );
    assert_eq!(rows[1].record["timestamp"], 1_709_424_300_000_i64);
    assert_eq!(rows[1].record["value"], 62.1);
}

/// A metric ListMetrics reports without a `Unit` (the live shape) lands
/// with unit `None`.
#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_without_unit() {
    let server = MockServer::start().await;
    json_target(LIST_METRICS)
        .respond_with(ok(json!({"Metrics": [metric("EBSReadOps", None)]})))
        .mount(&server)
        .await;
    json_target(GET_METRIC_DATA)
        .respond_with(DataForQueries)
        .mount(&server)
        .await;
    let cfg = account_config(
        &server,
        vec![service(
            "cloudwatch_metrics",
            &[("namespaces", json!(["AWS/EC2"]))],
        )],
    );
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].record["unit"], "None");
}

#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_empty() {
    let server = MockServer::start().await;
    json_target(LIST_METRICS)
        .respond_with(ok(json!({"Metrics": []})))
        .mount(&server)
        .await;
    let cfg = account_config(
        &server,
        vec![service(
            "cloudwatch_metrics",
            &[("namespaces", json!(["AWS/EC2"]))],
        )],
    );
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty());
    assert!(
        requests_targeting(&server, GET_METRIC_DATA)
            .await
            .is_empty(),
        "no queries, no GetMetricData"
    );
}

/// ListMetrics answers the namespace's three metrics, or the one the
/// request names when it carries a `MetricName` filter.
struct MetricsForFilter;

impl Respond for MetricsForFilter {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body = body_of(request);
        let all = [
            metric("CPUUtilization", Some("Percent")),
            metric("NetworkIn", Some("Bytes")),
            metric("DiskReadOps", Some("Count")),
        ];
        let metrics: Vec<Value> = all
            .into_iter()
            .filter(|m| {
                body.get("MetricName")
                    .and_then(Value::as_str)
                    .is_none_or(|name| m["MetricName"] == name)
            })
            .collect();
        ok(json!({"Metrics": metrics}))
    }
}

/// The `period_secs`, `stat` and `metric_names` knobs: the names narrow
/// ListMetrics to one request per (namespace, name) so the API does the
/// filtering, and the query carries the period and statistic.
#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_knobs() {
    let server = MockServer::start().await;
    json_target(LIST_METRICS)
        .respond_with(MetricsForFilter)
        .mount(&server)
        .await;
    json_target(GET_METRIC_DATA)
        .respond_with(DataForQueries)
        .mount(&server)
        .await;
    let cfg = account_config(
        &server,
        vec![service(
            "cloudwatch_metrics",
            &[
                ("namespaces", json!(["AWS/EC2"])),
                ("metric_names", json!(["NetworkIn"])),
                ("period_secs", json!(60)),
                ("stat", json!("Maximum")),
            ],
        )],
    );
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let list = requests_targeting(&server, LIST_METRICS).await;
    assert_eq!(list.len(), 1);
    assert_eq!(
        body_of(&list[0]),
        json!({"Namespace": "AWS/EC2", "MetricName": "NetworkIn"}),
        "the name goes to the API as its filter"
    );
    let get = requests_targeting(&server, GET_METRIC_DATA).await;
    assert_eq!(get.len(), 1);
    let queries = body_of(&get[0])["MetricDataQueries"].clone();
    assert_eq!(queries.as_array().map(Vec::len), Some(1), "only NetworkIn");
    assert_eq!(
        queries[0]["MetricStat"]["Metric"]["MetricName"],
        "NetworkIn"
    );
    assert_eq!(queries[0]["MetricStat"]["Period"], 60);
    assert_eq!(queries[0]["MetricStat"]["Stat"], "Maximum");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].record["metric_name"], "NetworkIn");
    assert_eq!(rows[0].record["unit"], "Bytes");
    assert_eq!(rows[0].record["stat"], "Maximum");
}

/// ListMetrics pages by `NextToken`; every page's metrics are queried.
#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_list_pagination() {
    let server = MockServer::start().await;
    json_target(LIST_METRICS)
        .respond_with(ok(
            json!({"Metrics": [metric("CPUUtilization", Some("Percent"))], "NextToken": "more"}),
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    json_target(LIST_METRICS)
        .respond_with(ok(json!({"Metrics": [metric("NetworkIn", Some("Bytes"))]})))
        .mount(&server)
        .await;
    json_target(GET_METRIC_DATA)
        .respond_with(DataForQueries)
        .mount(&server)
        .await;
    let cfg = account_config(
        &server,
        vec![service(
            "cloudwatch_metrics",
            &[("namespaces", json!(["AWS/EC2"]))],
        )],
    );
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let list = requests_targeting(&server, LIST_METRICS).await;
    assert_eq!(list.len(), 2);
    assert_eq!(body_of(&list[1])["NextToken"], "more");
    assert_eq!(
        ids(&rows, "metric_name"),
        ["CPUUtilization", "CPUUtilization", "NetworkIn", "NetworkIn"]
    );
}

/// More than 500 metrics go to GetMetricData in batches of 500, each batch
/// numbering its queries from `q0`, and every datapoint keeps the name of
/// the metric it belongs to. (The legacy mapped the second batch's ids back
/// onto the wrong slot and landed rows with an empty namespace, name and
/// unit.)
#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_batches_of_500_keep_their_names() {
    let server = MockServer::start().await;
    let metrics: Vec<Value> = (0..501)
        .map(|i| metric(&format!("m{i}"), Some("Count")))
        .collect();
    json_target(LIST_METRICS)
        .respond_with(ok(json!({"Metrics": metrics})))
        .mount(&server)
        .await;
    json_target(GET_METRIC_DATA)
        .respond_with(DataForQueries)
        .mount(&server)
        .await;
    let cfg = account_config(
        &server,
        vec![service(
            "cloudwatch_metrics",
            &[("namespaces", json!(["AWS/EC2"]))],
        )],
    );
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let get = requests_targeting(&server, GET_METRIC_DATA).await;
    assert_eq!(get.len(), 2, "500 queries a request");
    assert_eq!(
        body_of(&get[0])["MetricDataQueries"]
            .as_array()
            .map(Vec::len),
        Some(500)
    );
    assert_eq!(
        body_of(&get[1])["MetricDataQueries"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    assert_eq!(
        body_of(&get[1])["MetricDataQueries"][0]["Id"],
        "q0",
        "ids are positions within the batch"
    );
    assert_eq!(rows.len(), 1002, "two datapoints per metric");
    let last = &rows[1001].record;
    assert_eq!(
        last["metric_name"], "m500",
        "the 501st metric keeps its name"
    );
    assert_eq!(last["namespace"], "AWS/EC2");
    assert_eq!(last["unit"], "Count");
}

/// `output_format: otlp` lands one OTLP protobuf `ExportMetricsServiceRequest`
/// per GetMetricData response: a Gauge per metric with UCUM units, the
/// namespace and dimensions as datapoint attributes, the region on the
/// resource.
#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_otlp() {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value;
    use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point};
    use prost::Message;

    let server = MockServer::start().await;
    json_target(LIST_METRICS)
        .respond_with(ok(
            json!({"Metrics": [metric("CPUUtilization", Some("Percent"))]}),
        ))
        .mount(&server)
        .await;
    json_target(GET_METRIC_DATA)
        .respond_with(DataForQueries)
        .mount(&server)
        .await;
    let cfg = account_config(
        &server,
        vec![service(
            "cloudwatch_metrics",
            &[
                ("namespaces", json!(["AWS/EC2"])),
                ("output_format", json!("otlp")),
            ],
        )],
    );
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1, "one protobuf record for the response");
    assert_eq!(rows[0].topic, "test-aws_land");

    let request = ExportMetricsServiceRequest::decode(rows[0].raw.as_slice())
        .expect("the landed record is an OTLP export request");
    let rm = &request.resource_metrics[0];
    let attrs =
        |kvs: &[opentelemetry_proto::tonic::common::v1::KeyValue]| -> HashMap<String, String> {
            kvs.iter()
                .map(|kv| {
                    let value = match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                        Some(any_value::Value::StringValue(s)) => s.clone(),
                        _ => String::new(),
                    };
                    (kv.key.clone(), value)
                })
                .collect()
        };
    let resource = attrs(&rm.resource.as_ref().unwrap().attributes);
    assert_eq!(resource["cloud.provider"], "aws");
    assert_eq!(resource["cloud.region"], "us-east-1");
    assert_eq!(resource["service.name"], "dfe-fetcher");
    let sm = &rm.scope_metrics[0];
    assert_eq!(sm.scope.as_ref().unwrap().name, "dfe-fetcher");
    assert_eq!(sm.metrics.len(), 1);
    let m = &sm.metrics[0];
    assert_eq!(m.name, "CPUUtilization");
    assert_eq!(m.unit, "%", "UCUM for Percent");
    assert_eq!(attrs(&m.metadata)["stat"], "Average");
    let Some(metric::Data::Gauge(gauge)) = &m.data else {
        panic!("a gauge");
    };
    assert_eq!(gauge.data_points.len(), 2);
    assert_eq!(
        gauge.data_points[0].time_unix_nano,
        1_709_424_000_000_000_000
    );
    assert!(matches!(
        gauge.data_points[0].value,
        Some(number_data_point::Value::AsDouble(v)) if (v - 45.2).abs() < f64::EPSILON
    ));
    let point = attrs(&gauge.data_points[0].attributes);
    assert_eq!(point["Namespace"], "AWS/EC2");
    assert_eq!(point["InstanceId"], "i-1234");
}

/// An OTLP record whose metric unit is `{Count}` carries the `0x7D` byte the
/// enricher used to splice `_source*` / `_timestamp*` in front of; declared
/// binary by the profile, it lands exactly as the builder encoded it (a
/// prost re-encode of the decoded record is the landed bytes) with no
/// identity keys inside, and the JSON output of the same knob set is still
/// enriched.
#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_otlp_count_unit_lands_byte_identical() {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use prost::Message;

    let server = MockServer::start().await;
    json_target(LIST_METRICS)
        .respond_with(ok(
            json!({"Metrics": [metric("DiskReadOps", Some("Count")), metric("CPUUtilization", Some("Percent"))]}),
        ))
        .mount(&server)
        .await;
    json_target(GET_METRIC_DATA)
        .respond_with(DataForQueries)
        .mount(&server)
        .await;
    let knobs = |format: &str| {
        vec![service(
            "cloudwatch_metrics",
            &[
                ("namespaces", json!(["AWS/EC2"])),
                ("output_format", json!(format)),
            ],
        )]
    };
    let (outcome, rows) = run(config(account_config(&server, knobs("otlp"))), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1, "one protobuf record for the response");
    assert_eq!(rows[0].topic, "test-aws_land");
    let raw = rows[0].raw.as_slice();
    assert!(
        raw.contains(&b'}'),
        "the record carries the byte the enricher splices on"
    );
    let request = ExportMetricsServiceRequest::decode(raw)
        .expect("the landed record is an OTLP export request");
    assert_eq!(
        request.encode_to_vec(),
        raw,
        "the bytes out are the builder's bytes, nothing spliced in"
    );
    assert!(
        !raw.windows(7).any(|w| w == b"_source"),
        "no identity key inside a binary record"
    );
    let units: Vec<&str> = request.resource_metrics[0].scope_metrics[0]
        .metrics
        .iter()
        .map(|m| m.unit.as_str())
        .collect();
    assert!(
        units.contains(&"{Count}") && units.contains(&"%"),
        "{units:?}"
    );

    let (outcome, rows) = run(config(account_config(&server, knobs("json"))), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 4, "two metrics, two datapoints each");
    for row in &rows {
        let e = enriched(row);
        assert_eq!(e.source, "test-aws");
        assert_eq!(e.source_fetcher, "aws.cloudwatch_metrics");
    }
}

/// The knob is required: no namespaces is refused when the block is mapped
/// (the legacy warned every tick and fetched nothing).
#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_requires_namespaces() {
    let server = MockServer::start().await;
    let (outcome, rows) = run(one(&server, "cloudwatch_metrics"), None).await;
    assert!(rows.is_empty());
    let err = outcome.expect_err("refused");
    assert!(err.contains("namespaces"), "{err}");
    assert!(requests_targeting(&server, LIST_METRICS).await.is_empty());
}

// =============================================================================
// Inspector v2: REST-JSON POST /findings/list, paged by nextToken
// =============================================================================

/// The REST-JSON shape: `POST /findings/list` with `application/json`, no
/// target header, the window as epoch seconds on `lastObservedAt`,
/// `maxResults` 100 (capped), `nextToken` followed, signed for
/// `inspector2`.
#[tokio::test]
async fn test_aws_fetch_inspector() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/findings/list"))
        .respond_with(ok(
            json!({"findings": [{"findingArn": "arn:1", "severity": "HIGH"}], "nextToken": "page-2"}),
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/findings/list"))
        .respond_with(ok(
            json!({"findings": [{"findingArn": "arn:2", "severity": "LOW"}]}),
        ))
        .mount(&server)
        .await;
    let w = fetch_window("2026-05-21T13:30:00Z", "2026-05-22T13:30:00Z");
    let cfg = account_config(
        &server,
        vec![service("inspector", &[("max_results", json!(500))])],
    );
    let (outcome, rows) = run(config(cfg), Some(&w)).await;
    outcome.expect("fetch");

    let seen = requests_to(&server, "/findings/list").await;
    assert_eq!(seen.len(), 2);
    assert_eq!(
        header_of(&seen[0], "content-type"),
        Some("application/json")
    );
    assert_eq!(header_of(&seen[0], "accept"), Some("application/json"));
    assert!(header_of(&seen[0], "x-amz-target").is_none());
    assert_eq!(
        body_of(&seen[0]),
        json!({
            "filterCriteria": {"lastObservedAt": [{"startInclusive": w.start.timestamp(), "endInclusive": w.end.timestamp()}]},
            "maxResults": 100
        }),
        "max_results capped at 100"
    );
    assert_eq!(body_of(&seen[1])["nextToken"], "page-2");
    assert_signed(&seen[0], ACCESS_KEY, "us-east-1", "inspector2");
    assert_eq!(ids(&rows, "findingArn"), ["arn:1", "arn:2"]);
    for row in &rows {
        assert_eq!(enriched(row).source_fetcher, "aws.inspector");
    }
}

// =============================================================================
// AWS Health: JSON-1.1 DescribeEvents, region-locked to us-east-1
// =============================================================================

/// Signed for `health` in `us-east-1` whatever region the block names,
/// `lastUpdatedTimes` over the window as the epoch-second numbers the API
/// documents (the legacy sent RFC 3339 strings), `maxResults` 100,
/// `nextToken` followed.
#[tokio::test]
async fn test_aws_fetch_health() {
    let server = MockServer::start().await;
    json_target(DESCRIBE_EVENTS)
        .respond_with(ok(
            json!({"events": [{"arn": "arn:health:1", "service": "EC2"}], "nextToken": "page-2"}),
        ))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    json_target(DESCRIBE_EVENTS)
        .respond_with(ok(
            json!({"events": [{"arn": "arn:health:2", "service": "RDS"}]}),
        ))
        .mount(&server)
        .await;
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-22T13:30:00Z");
    let mut cfg = account_config(&server, vec![service("health", &[])]);
    cfg.region = "ap-southeast-2".into();
    let (outcome, rows) = run(config(cfg), Some(&w)).await;
    outcome.expect("fetch");

    let seen = requests_targeting(&server, DESCRIBE_EVENTS).await;
    assert_eq!(seen.len(), 2);
    assert_eq!(header_of(&seen[0], "content-type"), Some(JSON_1_1));
    assert_eq!(
        body_of(&seen[0]),
        json!({
            "filter": {"lastUpdatedTimes": [{"from": w.start.timestamp(), "to": w.end.timestamp()}]},
            "maxResults": 100
        }),
        "the window as the epoch-second numbers the API documents"
    );
    assert_eq!(body_of(&seen[1])["nextToken"], "page-2");
    assert_signed(&seen[0], ACCESS_KEY, "us-east-1", "health");
    assert_eq!(ids(&rows, "arn"), ["arn:health:1", "arn:health:2"]);
    for row in &rows {
        assert_eq!(enriched(row).source_fetcher, "aws.health");
    }
}

// =============================================================================
// LocalStack integration tests (live -> docker fallback)
//
// LocalStack emulates AWS APIs. The community image serves STS and
// CloudWatch (CloudTrail is a licensed service there and answers 501).
// These tests drive the real request path against a real HTTP server: URL
// construction, headers, the SigV4 signing code executing, and response
// parsing.
//
// They do NOT prove the signature is correct. LocalStack does not verify SigV4
// -- a request signed with a wrong secret is accepted, with or without
// ENFORCE_IAM=1 -- so a bad signature is only caught against a real AWS
// endpoint, in tests/e2e/smoke_remote.rs.
//
// To run:  docker run --rm -p 4566:4566 localstack/localstack:4.14
//          OR set LOCALSTACK_ENDPOINT to a remote LocalStack instance.
// The pinned tag must stay on the 4.x SEMVER line; see LOCALSTACK_TAG in
// tests/common/mod.rs.
// =============================================================================

use crate::common;

fn localstack_config(ls: &common::LocalStackConfig) -> Config {
    config(AwsSourceConfig {
        enabled: true,
        region: ls.region.clone(),
        access_key_id: Some(ls.access_key_id.clone()),
        secret_access_key: Some(ls.secret_access_key.clone().into()),
        endpoint_override: Some(ls.endpoint.clone()),
        services: vec![service(
            "cloudwatch_metrics",
            &[("namespaces", json!(["AWS/EC2"]))],
        )],
        topic: "test-aws-localstack".into(),
        ..AwsSourceConfig::default()
    })
}

#[tokio::test]
async fn test_aws_localstack_cloudwatch_list_metrics() {
    let Some(ls) = common::LocalStackConfig::acquire("aws-cloudwatch-list-metrics").await else {
        eprintln!("Skipping: no live LocalStack and Docker unavailable for testcontainer");
        return;
    };
    // A fetch error is the failure this test exists to catch: LocalStack
    // serves ListMetrics, and an empty namespace on a fresh instance is
    // still a clean tick.
    let (outcome, rows) = run(localstack_config(&ls), None).await;
    outcome.unwrap_or_else(|e| {
        panic!(
            "CloudWatch ListMetrics against LocalStack at {}: {e}",
            ls.endpoint
        )
    });
    for row in &rows {
        assert!(row.record.is_object(), "record must be a JSON object");
    }
}

#[tokio::test]
async fn test_aws_localstack_health_check() {
    let Some(ls) = common::LocalStackConfig::acquire("aws-health-check").await else {
        eprintln!("Skipping: no live LocalStack and Docker unavailable for testcontainer");
        return;
    };
    // The health check exercises credential resolution and the signed probe;
    // it must not return Err with valid credentials.
    let result = health(localstack_config(&ls)).await;
    assert!(
        result.is_ok(),
        "health_check must not return Err with valid credentials, got {result:?}"
    );
}

#[tokio::test]
async fn test_aws_localstack_with_time_window() {
    let Some(ls) = common::LocalStackConfig::acquire("aws-with-time-window").await else {
        eprintln!("Skipping: no live LocalStack and Docker unavailable for testcontainer");
        return;
    };
    let window = FetchWindow {
        start: Utc::now() - chrono::Duration::minutes(5),
        end: Utc::now(),
    };
    // A window-scoped request is signed exactly like an unscoped one.
    let (outcome, _) = run(localstack_config(&ls), Some(&window)).await;
    outcome.unwrap_or_else(|e| {
        panic!(
            "window-scoped CloudWatch fetch against LocalStack at {}: {e}",
            ls.endpoint
        )
    });
}
