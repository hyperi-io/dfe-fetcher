// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_onepassword.rs
// Purpose:   Characterisation of the 1Password Events Reporting source: requests sent, records landed
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The 1Password Events Reporting source against the in-test provider.
//!
//! Each test configures the typed `sources.onepassword` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the requests the provider recorded (path, the POST body: the window on
//! the first call, the bare cursor on the next) and the records that landed
//! (the provider's item, semantically, plus what enrichment added). The
//! typed config block is the operator's contract; the shipped `onepassword`
//! profile serves it through the framework driver.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use dfe_fetcher::config::{
    Config, OnePasswordConnection, OnePasswordService, OnePasswordSourceConfig,
};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::saas_provider::Provider;

/// A deployment config carrying `onepassword` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(onepassword: OnePasswordSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.onepassword = onepassword;
    config
}

fn service(name: &str) -> OnePasswordService {
    OnePasswordService {
        name: name.into(),
        config: HashMap::new(),
    }
}

/// The typed block an operator writes: a literal Events Reporting token,
/// the sign-in attempts service, pointed at the provider.
fn account_config(provider: &Provider) -> OnePasswordSourceConfig {
    OnePasswordSourceConfig {
        enabled: true,
        token: Some("tok-op".to_string().into()),
        api_url_override: Some(provider.base_url()),
        services: vec![service("signin_attempts")],
        ..OnePasswordSourceConfig::default()
    }
}

/// One tick of the `onepassword` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "onepassword", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "onepassword")).await
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

fn item(uuid: &str, category: &str) -> Value {
    json!({
        "uuid": uuid,
        "session_uuid": "s1",
        "timestamp": "2026-05-21T13:31:00.123Z",
        "category": category,
        "type": "credentials_ok",
        "country": "AU",
        "target_user": { "uuid": "u1", "name": "Kaz", "email": "kaz@example.com" },
        "client": { "app_name": "1Password Browser Extension", "platform_name": "Chrome" }
    })
}

#[tokio::test]
async fn signin_attempts_post_the_window_then_the_bare_cursor_until_has_more_is_false() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![item("s1", "success"), item("s2", "success")],
        vec![item("s3", "credentials_failed")],
        vec![item("s4", "success")],
    ]);
    let w = fetch_window("2026-05-21T13:30:00.987654Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(config(account_config(&provider)), Some(&w)).await;
    outcome.expect("fetch");

    let seen = provider.requests_to("/api/v2/signinattempts");
    assert_eq!(seen.len(), 3, "one POST per page");
    assert_eq!(
        seen[0].body,
        Some(json!({
            "limit": 100,
            "start_time": "2026-05-21T13:30:00.987Z",
            "end_time": "2026-05-21T14:30:00.000Z"
        })),
        "the first call carries the window in RFC 3339 milliseconds and a numeric limit"
    );
    assert_eq!(
        seen[1].body,
        Some(json!({"cursor": "op-cursor-1"})),
        "the next call carries the cursor and nothing else"
    );
    assert_eq!(seen[2].body, Some(json!({"cursor": "op-cursor-2"})));
    for request in &seen {
        assert_eq!(request.header("authorization"), Some("Bearer tok-op"));
        assert_eq!(request.header("accept"), Some("application/json"));
        assert!(
            request
                .header("content-type")
                .is_some_and(|c| c.starts_with("application/json")),
            "{:?}",
            request.header("content-type")
        );
        assert!(request.query.is_empty(), "nothing goes in the query");
    }

    assert_eq!(rows.len(), 4, "every page's items land, in order");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["uuid"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["s1", "s2", "s3", "s4"]);
    for (row, expected) in rows.iter().zip([
        item("s1", "success"),
        item("s2", "success"),
        item("s3", "credentials_failed"),
        item("s4", "success"),
    ]) {
        assert_eq!(row.topic, "onepassword_land");
        let e = enriched(row);
        assert_eq!(e.row, expected, "the provider's item, semantically");
        assert_eq!(e.source, "onepassword");
        assert_eq!(e.source_fetcher, "onepassword.signin_attempts");
    }
}

/// The three services map onto the three event-class endpoints, each tagged
/// with its own service name, and each honours its own `limit` knob (capped
/// at 1000, a non-integer ignored).
#[tokio::test]
async fn every_service_hits_its_own_endpoint_with_its_own_limit() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![item("x1", "success")]]);
    let mut cfg = account_config(&provider);
    cfg.services = vec![
        service("signin_attempts"),
        OnePasswordService {
            name: "item_usages".into(),
            config: [("limit".to_string(), json!(5000))].into_iter().collect(),
        },
        OnePasswordService {
            name: "audit_events".into(),
            config: [("limit".to_string(), json!("250"))].into_iter().collect(),
        },
    ];
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let signin = provider.requests_to("/api/v2/signinattempts");
    let usages = provider.requests_to("/api/v2/itemusages");
    let audit = provider.requests_to("/api/v2/auditevents");
    assert_eq!((signin.len(), usages.len(), audit.len()), (1, 1, 1));
    assert_eq!(signin[0].body.as_ref().unwrap()["limit"], 100);
    assert_eq!(
        usages[0].body.as_ref().unwrap()["limit"],
        1000,
        "capped at 1Password's page size"
    );
    assert_eq!(
        audit[0].body.as_ref().unwrap()["limit"],
        100,
        "a non-integer limit is ignored"
    );
    let mut tags: Vec<String> = rows.iter().map(|r| enriched(r).source_fetcher).collect();
    tags.sort();
    assert_eq!(
        tags,
        [
            "onepassword.audit_events",
            "onepassword.item_usages",
            "onepassword.signin_attempts",
        ]
    );
}

#[tokio::test]
async fn without_a_window_the_last_hour_is_fetched() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![]]);
    let before = Utc::now();
    let (outcome, rows) = run(config(account_config(&provider)), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty(), "an empty page lands nothing");

    let seen = provider.requests_to("/api/v2/signinattempts");
    let body = seen[0].body.as_ref().unwrap();
    let start = at(body["start_time"].as_str().unwrap());
    let end = at(body["end_time"].as_str().unwrap());
    assert_eq!((end - start).num_seconds(), 3600, "one hour of lookback");
    assert!(
        end >= before - chrono::Duration::seconds(1) && end <= Utc::now(),
        "the window ends now: {end} vs {before}"
    );
}

#[tokio::test]
async fn a_credential_secret_spec_is_resolved_and_wins_over_the_literal_token() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![item("s1", "success")]]);
    // nextest runs one process per test, so the variable leaks nowhere.
    unsafe { std::env::set_var("DFE_FETCHER_TEST_OP_TOKEN", "tok-from-env") };
    let mut cfg = account_config(&provider);
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_OP_TOKEN".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        provider.requests_to("/api/v2/signinattempts")[0].header("authorization"),
        Some("Bearer tok-from-env")
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![
        item("s1", "success"),
        item("s2", "credentials_failed"),
        item("s3", "success"),
    ]]);
    let mut cfg = account_config(&provider);
    cfg.filter = Some("category != \"success\"".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["uuid"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["s2"]);
}

#[tokio::test]
async fn an_empty_page_with_has_more_is_followed() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![item("s1", "success")],
        vec![],
        vec![item("s2", "success")],
    ]);
    let (outcome, rows) = run(config(account_config(&provider)), None).await;
    outcome.expect("fetch");
    assert_eq!(provider.requests_to("/api/v2/signinattempts").len(), 3);
    assert_eq!(rows.len(), 2);
}

/// A 5xx the provider keeps answering: the POST is a read, so it is retried
/// like a GET; the tick fails after the bounded retries and nothing lands.
#[tokio::test]
async fn a_server_error_fails_the_tick_after_retries_and_lands_nothing() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![item("s1", "success")]]);
    for _ in 0..4 {
        provider.answer_first(500, Some(0));
    }
    let (outcome, rows) = run(config(account_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(
        provider.requests_to("/api/v2/signinattempts").len(),
        4,
        "the first attempt and three retries"
    );
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
}

/// A 429 with `Retry-After` is retried after the wait, and the page then
/// lands.
#[tokio::test]
async fn a_429_with_retry_after_is_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![item("s1", "success")]]);
    provider.answer_first(429, Some(0));
    let (outcome, rows) = run(config(account_config(&provider)), None).await;
    outcome.expect("tick");
    assert_eq!(
        provider.requests_to("/api/v2/signinattempts").len(),
        2,
        "429 then 200"
    );
    assert_eq!(rows.len(), 1);
}

/// A 401 or 403 is never retried: the tick fails on the one request.
#[tokio::test]
async fn a_refusal_is_never_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![item("s1", "success")]]);
    provider.answer_first(403, Some(0));
    let (outcome, rows) = run(config(account_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to("/api/v2/signinattempts").len(), 1);
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(err.contains("403"), "{err}");
}

/// Two connections of the type poll with their own tokens, and each record
/// carries its connection's `_source_fetcher` tag (`<connection>.<unit>`),
/// so the connection is distinguishable on the record.
#[tokio::test]
async fn two_connections_poll_independently() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![item("s1", "success")]]);
    let mut cfg = account_config(&provider);
    cfg.token = None;
    cfg.connections = vec![
        OnePasswordConnection {
            id: "op-acme".into(),
            token: Some("tok-a".to_string().into()),
            ..OnePasswordConnection::default()
        },
        OnePasswordConnection {
            id: "op-globex".into(),
            token: Some("tok-b".to_string().into()),
            ..OnePasswordConnection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["op-acme", "op-globex"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        for row in rows {
            tags.push((id.to_string(), enriched(&row).source_fetcher));
        }
    }
    let seen = provider.requests_to("/api/v2/signinattempts");
    assert_eq!(seen[0].header("authorization"), Some("Bearer tok-a"));
    assert_eq!(seen[1].header("authorization"), Some("Bearer tok-b"));
    assert_eq!(
        tags,
        [
            ("op-acme".to_string(), "op-acme.signin_attempts".to_string()),
            (
                "op-globex".to_string(),
                "op-globex.signin_attempts".to_string()
            ),
        ]
    );
}

/// A service the profile does not know is refused at validation by name,
/// instead of being skipped with a warning on every tick.
#[tokio::test]
async fn an_unknown_service_name_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![item("s1", "success")]]);
    let mut cfg = account_config(&provider);
    cfg.services = vec![service("signinattempts")];
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.onepassword") && err.contains("signinattempts"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "{outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());
}

/// A token missing from both `token` and `credential_secret` is refused at
/// validation, naming both fields, instead of failing silently every tick.
#[tokio::test]
async fn a_missing_token_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    let mut cfg = account_config(&provider);
    cfg.token = None;
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.onepassword") && err.contains("credential_secret"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "the tick never starts: {outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn the_health_check_introspects_the_token() {
    let provider = crate::saas_provider::start().await;
    let healthy = health(config(account_config(&provider)))
        .await
        .expect("health");
    assert!(healthy);
    let seen = provider.requests_to("/api/auth/introspect");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].header("authorization"), Some("Bearer tok-op"));
    assert_eq!(seen[0].header("accept"), Some("application/json"));
}
