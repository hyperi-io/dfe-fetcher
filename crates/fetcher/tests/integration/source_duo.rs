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
//! HMAC-SHA1 signature with the known secret key and refuses a mismatch as
//! Duo does), how the v2 `next_offset` was followed, and the records that
//! landed (the provider's auth log, semantically, plus what enrichment
//! added). The typed config block is the operator's contract; the shipped
//! `duo` profile serves it through the framework driver with the
//! `duo_hmac` auth mode.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use dfe_fetcher::config::{Config, DuoConnection, DuoService, DuoSourceConfig};
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

/// Every request carried a Basic credential of `ikey:signature` that the
/// provider accepted, a `Date` in RFC 2822 with a `-0000` zone, and the
/// JSON accept header.
fn assert_signed(seen: &Seen) {
    let auth = seen.header("authorization").expect("authorization");
    assert!(auth.starts_with("Basic "), "{auth}");
    let date = seen.header("date").expect("date");
    assert!(date.ends_with(" -0000"), "RFC 2822 with -0000: {date}");
    assert!(
        DateTime::parse_from_rfc2822(date).is_ok(),
        "parses as RFC 2822: {date}"
    );
    assert_eq!(seen.header("accept"), Some("application/json"));
}

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
    assert_signed(&seen[0]);

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
        assert_signed(request);
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
    assert_signed(&seen[0]);
    assert!(seen[0].query.is_empty());

    let mut cfg = tenant_config(&provider);
    cfg.secret_key = Some("wrong".to_string().into());
    let err = health(config(cfg))
        .await
        .expect_err("a refused signature is unhealthy");
    assert!(err.contains("401"), "{err}");
}
