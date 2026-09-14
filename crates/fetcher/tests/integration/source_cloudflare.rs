// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_cloudflare.rs
// Purpose:   Characterisation of the Cloudflare audit-log source: requests sent, records landed
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Cloudflare account audit-log source against the in-test provider.
//!
//! Each test configures the typed `sources.cloudflare` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the requests the provider recorded (path, query, headers, how the page
//! number was advanced against `result_info.total_pages`) and the records
//! that landed (the provider's entry, semantically, plus what enrichment
//! added). The typed config block is the operator's contract; the shipped
//! `cloudflare` profile serves it through the framework driver.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use dfe_fetcher::config::{
    CloudflareConnection, CloudflareService, CloudflareSourceConfig, Config,
};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::saas_provider::{Provider, Seen};

const ACCOUNT: &str = "0123456789abcdef0123456789abcdef";

/// A deployment config carrying `cloudflare` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(cloudflare: CloudflareSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.cloudflare = cloudflare;
    config
}

/// The typed block an operator writes: one account, a literal token, the
/// audit_logs service, pointed at the provider.
fn account_config(provider: &Provider) -> CloudflareSourceConfig {
    CloudflareSourceConfig {
        enabled: true,
        account_id: Some(ACCOUNT.into()),
        token: Some("tok-cf".to_string().into()),
        api_url_override: Some(provider.base_url()),
        services: vec![CloudflareService {
            name: "audit_logs".into(),
            config: HashMap::new(),
        }],
        ..CloudflareSourceConfig::default()
    }
}

fn service_with(config: &[(&str, Value)]) -> CloudflareService {
    CloudflareService {
        name: "audit_logs".into(),
        config: config
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    }
}

/// One tick of the `cloudflare` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "cloudflare", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "cloudflare")).await
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

fn entry(id: &str, action_type: &str) -> Value {
    json!({
        "id": id,
        "when": "2026-05-21T13:31:00Z",
        "action": { "type": action_type, "result": true },
        "actor": { "email": "kaz@example.com", "id": "u1", "type": "user" },
        "resource": { "type": "zone", "id": "z1" },
        "metadata": { "zone_name": "example.com" }
    })
}

fn logs_path() -> String {
    format!("/accounts/{ACCOUNT}/audit_logs")
}

fn query_of(seen: &Seen) -> Vec<(&str, &str)> {
    seen.query
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect()
}

#[tokio::test]
async fn audit_logs_walk_the_page_numbers_to_total_pages_and_land_enriched() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![entry("c1", "login"), entry("c2", "add")],
        vec![entry("c3", "delete")],
        vec![entry("c4", "change")],
    ]);
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(config(account_config(&provider)), Some(&w)).await;
    outcome.expect("fetch");

    let seen = provider.requests_to(&logs_path());
    assert_eq!(seen.len(), 3, "one request per page, up to total_pages");
    assert_eq!(
        query_of(&seen[0]),
        [
            ("before", "2026-05-21T14:30:00Z"),
            ("page", "1"),
            ("per_page", "100"),
            ("since", "2026-05-21T13:30:00Z"),
        ],
        "since/before to the second with Z, per_page defaults to 100, pages from 1"
    );
    assert_eq!(seen[1].query_value("page"), Some("2"));
    assert_eq!(seen[2].query_value("page"), Some("3"));
    for request in &seen {
        assert_eq!(request.header("authorization"), Some("Bearer tok-cf"));
        assert_eq!(request.header("accept"), Some("application/json"));
    }

    assert_eq!(rows.len(), 4, "every page's entries land, in order");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["c1", "c2", "c3", "c4"]);
    for (row, expected) in rows.iter().zip([
        entry("c1", "login"),
        entry("c2", "add"),
        entry("c3", "delete"),
        entry("c4", "change"),
    ]) {
        assert_eq!(row.topic, "cloudflare_land");
        let e = enriched(row);
        assert_eq!(e.row, expected, "the provider's entry, semantically");
        assert_eq!(e.source, "cloudflare");
        assert_eq!(e.source_fetcher, "cloudflare.audit_logs");
    }
}

#[tokio::test]
async fn the_actor_email_action_type_and_per_page_knobs_shape_the_request() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("c1", "login")]]);
    let mut cfg = account_config(&provider);
    cfg.services = vec![service_with(&[
        ("actor_email", json!("user+tag@example.com")),
        ("action_type", json!("login")),
        ("per_page", json!(50)),
    ])];
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    let seen = provider.requests_to(&logs_path());
    assert_eq!(
        seen[0].query_value("actor.email"),
        Some("user+tag@example.com"),
        "decoded by the provider; the `+` and `@` survive the encoding"
    );
    assert_eq!(seen[0].query_value("action.type"), Some("login"));
    assert_eq!(seen[0].query_value("per_page"), Some("50"));

    // Cloudflare caps a page at 1000; a non-integer per_page is ignored.
    provider.serve(vec![vec![]]);
    let mut cfg = account_config(&provider);
    cfg.services = vec![service_with(&[("per_page", json!(5000))])];
    run(config(cfg), None).await.0.expect("fetch");
    assert_eq!(
        provider
            .requests_to(&logs_path())
            .last()
            .unwrap()
            .query_value("per_page"),
        Some("1000")
    );
    provider.serve(vec![vec![]]);
    let mut cfg = account_config(&provider);
    cfg.services = vec![service_with(&[("per_page", json!("250"))])];
    run(config(cfg), None).await.0.expect("fetch");
    assert_eq!(
        provider
            .requests_to(&logs_path())
            .last()
            .unwrap()
            .query_value("per_page"),
        Some("100")
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

    let seen = provider.requests_to(&logs_path());
    let since = at(seen[0].query_value("since").unwrap());
    let until = at(seen[0].query_value("before").unwrap());
    assert_eq!((until - since).num_seconds(), 3600, "one hour of lookback");
    assert!(
        until >= before - chrono::Duration::seconds(1) && until <= Utc::now(),
        "the window ends now: {until} vs {before}"
    );
}

#[tokio::test]
async fn a_credential_secret_spec_is_resolved_and_wins_over_the_literal_token() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("c1", "login")]]);
    // nextest runs one process per test, so the variable leaks nowhere.
    unsafe { std::env::set_var("DFE_FETCHER_TEST_CF_TOKEN", "tok-from-env") };
    let mut cfg = account_config(&provider);
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_CF_TOKEN".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        provider.requests_to(&logs_path())[0].header("authorization"),
        Some("Bearer tok-from-env")
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![
        entry("c1", "login"),
        entry("c2", "view"),
        entry("c3", "view"),
    ]]);
    let mut cfg = account_config(&provider);
    cfg.filter = Some("action.type != \"view\"".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["c1"]);
}

/// An empty page inside the total is still followed: `total_pages`, not
/// the page's size, ends the sequence.
#[tokio::test]
async fn an_empty_page_before_total_pages_is_followed() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![entry("c1", "login")],
        vec![],
        vec![entry("c2", "login")],
    ]);
    let (outcome, rows) = run(config(account_config(&provider)), None).await;
    outcome.expect("fetch");
    assert_eq!(provider.requests_to(&logs_path()).len(), 3);
    assert_eq!(rows.len(), 2);
}

/// Cloudflare reports a provider-level failure inside a 200
/// (`success: false`): the tick fails with Cloudflare's reasons, nothing
/// lands, and the scheduler does not advance the window past it.
#[tokio::test]
async fn a_success_false_body_fails_the_tick_with_cloudflares_reasons() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("c1", "login")]]);
    provider.fail_first(json!({
        "success": false,
        "errors": [{"code": 10000, "message": "Authentication error"}],
        "messages": [],
        "result": null
    }));
    let (outcome, rows) = run(config(account_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to(&logs_path()).len(), 1);
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("Authentication error"), "{err}");
}

/// A 5xx the provider keeps answering: the tick fails after the bounded
/// retries and nothing lands.
#[tokio::test]
async fn a_server_error_fails_the_tick_after_retries_and_lands_nothing() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("c1", "login")]]);
    for _ in 0..4 {
        provider.answer_first(500, Some(0));
    }
    let (outcome, rows) = run(config(account_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(
        provider.requests_to(&logs_path()).len(),
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
    provider.serve(vec![vec![entry("c1", "login")]]);
    provider.answer_first(429, Some(0));
    let (outcome, rows) = run(config(account_config(&provider)), None).await;
    outcome.expect("tick");
    assert_eq!(provider.requests_to(&logs_path()).len(), 2, "429 then 200");
    assert_eq!(rows.len(), 1);
}

/// A 401 or 403 is never retried: the tick fails on the one request.
#[tokio::test]
async fn a_refusal_is_never_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("c1", "login")]]);
    provider.answer_first(403, Some(0));
    let (outcome, rows) = run(config(account_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to(&logs_path()).len(), 1);
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(err.contains("403"), "{err}");
}

/// Two connections of the type poll their own accounts with their own
/// tokens, and each record carries its connection's `_source_fetcher` tag
/// (`<connection>.<unit>`), so the connection is distinguishable on the
/// record.
#[tokio::test]
async fn two_connections_poll_independently() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("c1", "login")]]);
    let mut cfg = account_config(&provider);
    cfg.account_id = None;
    cfg.token = None;
    cfg.connections = vec![
        CloudflareConnection {
            id: "cf-acme".into(),
            account_id: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
            token: Some("tok-a".to_string().into()),
            ..CloudflareConnection::default()
        },
        CloudflareConnection {
            id: "cf-globex".into(),
            account_id: Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into()),
            token: Some("tok-b".to_string().into()),
            ..CloudflareConnection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["cf-acme", "cf-globex"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        for row in rows {
            tags.push((id.to_string(), enriched(&row).source_fetcher));
        }
    }
    assert_eq!(
        provider.requests_to("/accounts/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/audit_logs")[0]
            .header("authorization"),
        Some("Bearer tok-a")
    );
    assert_eq!(
        provider.requests_to("/accounts/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/audit_logs")[0]
            .header("authorization"),
        Some("Bearer tok-b")
    );
    assert_eq!(
        tags,
        [
            ("cf-acme".to_string(), "cf-acme.audit_logs".to_string()),
            ("cf-globex".to_string(), "cf-globex.audit_logs".to_string()),
        ]
    );
}

/// A service the profile does not know is refused at validation by name,
/// instead of being skipped with a warning on every tick.
#[tokio::test]
async fn an_unknown_service_name_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("c1", "login")]]);
    let mut cfg = account_config(&provider);
    cfg.services = vec![CloudflareService {
        name: "audit_log".into(),
        config: HashMap::new(),
    }];
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.cloudflare") && err.contains("audit_log"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "{outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());
}

/// A missing account id or token is refused at validation naming the
/// field, instead of failing silently every tick.
#[tokio::test]
async fn a_missing_account_id_or_token_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("c1", "login")]]);
    let mut cfg = account_config(&provider);
    cfg.account_id = Some(String::new());
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.cloudflare") && err.contains("account_id"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "the tick never starts: {outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());

    let mut cfg = account_config(&provider);
    cfg.token = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.cloudflare") && err.contains("credential_secret"),
        "{err}"
    );
}

#[tokio::test]
async fn the_health_check_verifies_the_token() {
    let provider = crate::saas_provider::start().await;
    let healthy = health(config(account_config(&provider)))
        .await
        .expect("health");
    assert!(healthy);
    let seen = provider.requests_to("/user/tokens/verify");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].header("authorization"), Some("Bearer tok-cf"));
    assert_eq!(seen[0].header("accept"), Some("application/json"));
}
