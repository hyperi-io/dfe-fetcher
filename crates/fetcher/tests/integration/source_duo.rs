// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_duo.rs
// Purpose:   Characterisation of the Duo Admin API source: signed requests, paging, records landed
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Duo Admin API authentication-log source against the in-test provider.
//!
//! Each test configures the typed `sources.duo` block, runs one tick through
//! the real pipeline into scalo's memory transport, and asserts on the
//! requests the provider recorded and VERIFIED (the provider recomputes the
//! signature with the known secret key and refuses a mismatch as Duo does),
//! how the v2 `next_offset` was followed, and the records that landed (the
//! provider's auth log, semantically, plus what enrichment added). The typed
//! config block is the operator's contract; the shipped `duo` profile serves
//! it through the framework driver on the `signature` auth mode, whose
//! `duo_v5` preset is Duo's documented scheme and whose `duo_v2` is the legacy
//! one an older tenant selects.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use base64::Engine as _;

use dfe_fetcher::config::{
    Config, DuoConnection, DuoService, DuoSignatureVersion, DuoSourceConfig,
};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::saas_provider::{DUO_IKEY, DUO_SKEY, Provider, Seen};

const LOGS: &str = "/admin/v2/logs/authentication";

/// A deployment config carrying `duo` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(duo: DuoSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.duo = duo;
    config
}

/// The typed block an operator writes: the Admin API integration and
/// secret keys, the mock URL pointed at the provider, the authentication
/// logs service.
fn tenant_config(provider: &Provider) -> DuoSourceConfig {
    DuoSourceConfig {
        enabled: true,
        api_host: Some("api-deadbeef.duosecurity.com".into()),
        integration_key: Some(DUO_IKEY.into()),
        secret_key: Some(DUO_SKEY.to_string().into()),
        api_url_override: Some(provider.base_url()),
        services: vec![DuoService {
            name: "authentication_logs".into(),
            config: HashMap::new(),
        }],
        ..DuoSourceConfig::default()
    }
}

fn service_with(config: &[(&str, Value)]) -> DuoService {
    DuoService {
        name: "authentication_logs".into(),
        config: config
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    }
}

/// One tick of the `duo` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "duo", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "duo")).await
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

fn authlog(txid: &str, result: &str) -> Value {
    json!({
        "txid": txid,
        "timestamp": 1_779_370_260,
        "isotimestamp": "2026-05-21T13:31:00.000000+00:00",
        "result": result,
        "reason": if result == "success" { "user_approved" } else { "user_marked_fraud" },
        "user": { "name": "kaz", "key": "DU1" },
        "access_device": { "ip": "203.0.113.9", "browser": "Chrome" },
        "factor": "duo_push"
    })
}

fn query_of(seen: &Seen) -> Vec<(&str, &str)> {
    seen.query
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect()
}

/// The signature of a request's Basic credential, whose user name is the
/// integration key. The provider verified it before recording the request, so
/// what this reads back is a signature Duo would have accepted.
fn signature_of(seen: &Seen) -> String {
    let auth = seen.header("authorization").expect("authorization");
    let encoded = auth
        .strip_prefix("Basic ")
        .unwrap_or_else(|| panic!("{auth}"));
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .expect("base64");
    let credential = String::from_utf8(decoded).expect("utf8");
    let (ikey, signature) = credential.split_once(':').expect("ikey:signature");
    assert_eq!(ikey, DUO_IKEY);
    signature.to_owned()
}

/// Every request carried a Basic credential of `ikey:signature` that the
/// provider accepted, a `Date` in RFC 2822 with a `-0000` zone, and the JSON
/// accept header. The signature's length is the digest's: 128 hex characters
/// for the SHA-512 Duo documents, 40 for the SHA-1 of its legacy scheme.
fn assert_signed(seen: &Seen, digest_hex_len: usize) {
    assert_eq!(signature_of(seen).len(), digest_hex_len);
    let date = seen.header("date").expect("date");
    assert!(date.ends_with(" -0000"), "RFC 2822 with -0000: {date}");
    assert!(
        DateTime::parse_from_rfc2822(date).is_ok(),
        "parses as RFC 2822: {date}"
    );
    assert_eq!(seen.header("accept"), Some("application/json"));
}

/// The signature length of Duo's documented scheme, HMAC-SHA512 as hex.
const SHA512_HEX: usize = 128;

/// The signature length of Duo's legacy scheme, HMAC-SHA1 as hex.
const SHA1_HEX: usize = 40;

#[tokio::test]
async fn authentication_logs_are_signed_and_land_enriched() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![
        authlog("t1", "success"),
        authlog("t2", "denied"),
    ]]);
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(config(tenant_config(&provider)), Some(&w)).await;
    outcome.expect("fetch");

    let seen = provider.requests_to(LOGS);
    assert_eq!(seen.len(), 1);
    assert_eq!(
        query_of(&seen[0]),
        [
            ("limit", "100"),
            ("maxtime", "1779373800000"),
            ("mintime", "1779370200987"),
        ],
        "mintime/maxtime in epoch milliseconds to the millisecond, limit defaults to 100"
    );
    assert_signed(&seen[0], SHA512_HEX);

    assert_eq!(rows.len(), 2);
    for (row, expected) in rows
        .iter()
        .zip([authlog("t1", "success"), authlog("t2", "denied")])
    {
        assert_eq!(row.topic, "duo_land");
        let e = enriched(row);
        assert_eq!(e.row, expected, "the provider's auth log, semantically");
        assert_eq!(e.source, "duo");
        assert_eq!(e.source_fetcher, "duo.authentication_logs");
    }
}

/// A tenant whose endpoints verify Duo's legacy scheme selects it on its own
/// connection, and every request of that connection signs SHA-1 while the
/// default connection signs SHA-512. The provider verifies whichever arrived,
/// so a request that reached it was one Duo would have accepted.
#[tokio::test]
async fn the_signature_version_a_connection_names_is_the_one_it_signs() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![authlog("t1", "success")]]);
    let mut cfg = tenant_config(&provider);
    cfg.connections = vec![
        DuoConnection {
            id: "duo-current".into(),
            ..DuoConnection::default()
        },
        DuoConnection {
            id: "duo-legacy".into(),
            signature_version: Some(DuoSignatureVersion::V2),
            ..DuoConnection::default()
        },
    ];
    let config = config(cfg);
    for (id, digest_hex_len) in [("duo-current", SHA512_HEX), ("duo-legacy", SHA1_HEX)] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        assert_eq!(rows.len(), 1);
        let seen = provider.requests_to(LOGS);
        assert_signed(seen.last().expect("a request"), digest_hex_len);
    }
}

/// The v2 API answers `metadata.next_offset` as a two-element array (the
/// timestamp and the offset id) and takes it back comma-joined as the
/// `next_offset` parameter, signed like everything else. The legacy source
/// read it as a string, found none, and stopped after the first page: an
/// account with more than `limit` events per window lost the rest.
#[tokio::test]
async fn paging_follows_the_array_next_offset_comma_joined() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![authlog("t1", "success"), authlog("t2", "success")],
        vec![authlog("t3", "success")],
        vec![authlog("t4", "success")],
    ]);
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    outcome.expect("fetch");
    let seen = provider.requests_to(LOGS);
    assert_eq!(seen.len(), 3, "every page is requested");
    assert_eq!(seen[0].query_value("next_offset"), None);
    assert_eq!(
        seen[1].query_value("next_offset"),
        Some("1532951895000,cursor-1"),
        "the list, comma-joined"
    );
    assert_eq!(
        seen[2].query_value("next_offset"),
        Some("1532951895000,cursor-2")
    );
    for request in &seen {
        assert_signed(request, SHA512_HEX);
    }
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["txid"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["t1", "t2", "t3", "t4"], "nothing is lost");
}

#[tokio::test]
async fn the_limit_knob_is_capped_at_1000_and_a_non_integer_limit_is_ignored() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![authlog("t1", "success")]]);
    let mut cfg = tenant_config(&provider);
    cfg.services = vec![service_with(&[("limit", json!(5000))])];
    run(config(cfg), None).await.0.expect("fetch");
    assert_eq!(
        provider.requests_to(LOGS)[0].query_value("limit"),
        Some("1000")
    );
    provider.serve(vec![vec![]]);
    let mut cfg = tenant_config(&provider);
    cfg.services = vec![service_with(&[("limit", json!("250"))])];
    run(config(cfg), None).await.0.expect("fetch");
    assert_eq!(
        provider
            .requests_to(LOGS)
            .last()
            .unwrap()
            .query_value("limit"),
        Some("100")
    );
}

#[tokio::test]
async fn without_a_window_the_last_hour_is_fetched() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![]]);
    let before = Utc::now();
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty(), "an empty page lands nothing");

    let seen = provider.requests_to(LOGS);
    let mintime: i64 = seen[0].query_value("mintime").unwrap().parse().unwrap();
    let maxtime: i64 = seen[0].query_value("maxtime").unwrap().parse().unwrap();
    assert_eq!(maxtime - mintime, 3_600_000, "one hour of lookback, in ms");
    assert!(
        maxtime >= before.timestamp_millis() - 1000 && maxtime <= Utc::now().timestamp_millis(),
        "the window ends now: {maxtime} vs {before}"
    );
}

#[tokio::test]
async fn a_credential_secret_spec_is_resolved_as_the_secret_key() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![authlog("t1", "success")]]);
    // nextest runs one process per test, so the variable leaks nowhere.
    unsafe { std::env::set_var("DFE_FETCHER_TEST_DUO_SKEY", DUO_SKEY) };
    let mut cfg = tenant_config(&provider);
    cfg.secret_key = Some("not-the-key".to_string().into());
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_DUO_SKEY".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(
        rows.len(),
        1,
        "the provider accepted the signature made with the resolved key"
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![
        authlog("t1", "success"),
        authlog("t2", "fraud"),
        authlog("t3", "success"),
    ]]);
    let mut cfg = tenant_config(&provider);
    cfg.filter = Some("result == \"fraud\"".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["txid"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["t2"]);
}

/// Duo reports a failure inside a 200 (`stat: FAIL`): the tick fails with
/// Duo's message, nothing lands, and the scheduler does not advance the
/// window past it.
#[tokio::test]
async fn a_stat_fail_body_fails_the_tick_with_duos_message() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![authlog("t1", "success")]]);
    provider.fail_first(json!({"stat": "FAIL", "code": 40002, "message": "Invalid request parameters", "message_detail": "mintime"}));
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to(LOGS).len(), 1);
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("Invalid request parameters"), "{err}");
}

/// A wrong secret key: the provider refuses the signature with a 401, which
/// is never retried, and the tick fails on the one request.
#[tokio::test]
async fn a_bad_signature_is_refused_and_never_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![authlog("t1", "success")]]);
    let mut cfg = tenant_config(&provider);
    cfg.secret_key = Some("wrong".to_string().into());
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to(LOGS).len(), 1);
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(
        err.contains("401") && err.contains("Invalid signature"),
        "{err}"
    );
}

/// A 5xx the provider keeps answering: the tick fails after the bounded
/// retries, each attempt freshly signed, and nothing lands.
#[tokio::test]
async fn a_server_error_fails_the_tick_after_retries_and_lands_nothing() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![authlog("t1", "success")]]);
    for _ in 0..4 {
        provider.answer_first(500, Some(0));
    }
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(
        provider.requests_to(LOGS).len(),
        4,
        "the first attempt and three retries"
    );
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
}

/// A 429 with `Retry-After` is retried after the wait with a fresh
/// signature, and the page then lands.
#[tokio::test]
async fn a_429_with_retry_after_is_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![authlog("t1", "success")]]);
    provider.answer_first(429, Some(0));
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    outcome.expect("tick");
    assert_eq!(provider.requests_to(LOGS).len(), 2, "429 then 200");
    assert_eq!(rows.len(), 1);
}

/// Two connections of the type sign with their own keys, and each record
/// carries its connection's `_source_fetcher` tag (`<connection>.<unit>`),
/// so the connection is distinguishable on the record.
#[tokio::test]
async fn two_connections_poll_independently() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![authlog("t1", "success")]]);
    let mut cfg = tenant_config(&provider);
    cfg.integration_key = None;
    cfg.secret_key = None;
    cfg.connections = vec![
        DuoConnection {
            id: "duo-acme".into(),
            integration_key: Some(DUO_IKEY.into()),
            secret_key: Some(DUO_SKEY.to_string().into()),
            ..DuoConnection::default()
        },
        DuoConnection {
            id: "duo-globex".into(),
            integration_key: Some(DUO_IKEY.into()),
            secret_key: Some(DUO_SKEY.to_string().into()),
            ..DuoConnection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["duo-acme", "duo-globex"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        for row in rows {
            tags.push((id.to_string(), enriched(&row).source_fetcher));
        }
    }
    assert_eq!(provider.requests_to(LOGS).len(), 2);
    assert_eq!(
        tags,
        [
            (
                "duo-acme".to_string(),
                "duo-acme.authentication_logs".to_string()
            ),
            (
                "duo-globex".to_string(),
                "duo-globex.authentication_logs".to_string()
            ),
        ]
    );
}

/// A service the profile does not know is refused at validation by name,
/// instead of being skipped with a warning on every tick.
#[tokio::test]
async fn an_unknown_service_name_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![authlog("t1", "success")]]);
    let mut cfg = tenant_config(&provider);
    cfg.services = vec![DuoService {
        name: "telephony_logs".into(),
        config: HashMap::new(),
    }];
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.duo") && err.contains("telephony_logs"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "{outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());
}

/// A missing integration key or secret key is refused at validation naming
/// the field, instead of failing silently every tick.
#[tokio::test]
async fn a_missing_integration_key_or_secret_key_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![authlog("t1", "success")]]);
    let mut cfg = tenant_config(&provider);
    cfg.integration_key = Some(String::new());
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.duo") && err.contains("integration_key"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "the tick never starts: {outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());

    let mut cfg = tenant_config(&provider);
    cfg.secret_key = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.duo") && err.contains("credential_secret"),
        "{err}"
    );
}

#[tokio::test]
async fn the_health_check_signs_the_check_endpoint() {
    let provider = crate::saas_provider::start().await;
    let healthy = health(config(tenant_config(&provider)))
        .await
        .expect("health");
    assert!(healthy);
    let seen = provider.requests_to("/admin/v1/check");
    assert_eq!(seen.len(), 1);
    assert_signed(&seen[0], SHA512_HEX);
    assert_eq!(
        seen[0].query,
        [] as [(std::string::String, std::string::String); 0]
    );

    let mut cfg = tenant_config(&provider);
    cfg.secret_key = Some("wrong".to_string().into());
    let err = health(config(cfg))
        .await
        .expect_err("a refused signature is unhealthy");
    assert!(err.contains("401"), "{err}");
}
