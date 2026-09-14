// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_slack.rs
// Purpose:   Characterisation of the Slack audit-log source: requests sent, records landed
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Slack audit-log source against the in-test provider.
//!
//! Each test configures the typed `sources.slack` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the requests the provider recorded (path, query, headers, how the body
//! cursor was followed) and the records that landed (the provider's entry,
//! semantically, plus what enrichment added). The typed config block is the
//! operator's contract; the shipped `slack` profile serves it through the
//! framework driver.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use dfe_fetcher::config::{Config, SlackConnection, SlackService, SlackSourceConfig};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::saas_provider::{Provider, Seen};

/// A deployment config carrying `slack` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(slack: SlackSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.slack = slack;
    config
}

/// The typed block an operator writes: a literal org-admin token, the
/// audit_logs service, pointed at the provider.
fn org_config(provider: &Provider) -> SlackSourceConfig {
    SlackSourceConfig {
        enabled: true,
        token: Some("tok-slack".to_string().into()),
        api_url_override: Some(provider.base_url()),
        services: vec![SlackService {
            name: "audit_logs".into(),
            config: HashMap::new(),
        }],
        ..SlackSourceConfig::default()
    }
}

fn service_with(config: &[(&str, Value)]) -> SlackService {
    SlackService {
        name: "audit_logs".into(),
        config: config
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    }
}

/// One tick of the `slack` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "slack", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "slack")).await
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

fn entry(id: &str, action: &str) -> Value {
    json!({
        "id": id,
        "date_create": 1_700_000_000,
        "action": action,
        "actor": { "type": "user", "user": { "id": "W1", "email": "kaz@example.com" } },
        "entity": { "type": "workspace", "workspace": { "id": "T1", "name": "acme" } },
        "context": { "ua": "test", "ip_address": "203.0.113.9" }
    })
}

fn query_of(seen: &Seen) -> Vec<(&str, &str)> {
    seen.query
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect()
}

#[tokio::test]
async fn audit_logs_follow_the_body_cursor_with_an_epoch_window_and_land_enriched() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![entry("a1", "user_login"), entry("a2", "file_downloaded")],
        vec![entry("a3", "user_logout")],
        vec![entry("a4", "channel_created")],
    ]);
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(config(org_config(&provider)), Some(&w)).await;
    outcome.expect("fetch");

    let seen = provider.requests_to("/audit/v1/logs");
    assert_eq!(seen.len(), 3, "one request per page");
    assert_eq!(
        query_of(&seen[0]),
        [
            ("latest", "1779373800"),
            ("limit", "200"),
            ("oldest", "1779370200"),
        ],
        "oldest/latest are epoch seconds, limit defaults to 200"
    );
    assert_eq!(
        query_of(&seen[1]),
        [
            ("cursor", "cursor-1"),
            ("latest", "1779373800"),
            ("limit", "200"),
            ("oldest", "1779370200"),
        ],
        "the cursor is added to the same query"
    );
    assert_eq!(seen[2].query_value("cursor"), Some("cursor-2"));
    for request in &seen {
        assert_eq!(request.header("authorization"), Some("Bearer tok-slack"));
        assert_eq!(request.header("accept"), Some("application/json"));
    }

    assert_eq!(rows.len(), 4, "every page's entries land, in order");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["a1", "a2", "a3", "a4"]);
    for (row, expected) in rows.iter().zip([
        entry("a1", "user_login"),
        entry("a2", "file_downloaded"),
        entry("a3", "user_logout"),
        entry("a4", "channel_created"),
    ]) {
        assert_eq!(row.topic, "slack_land");
        let e = enriched(row);
        assert_eq!(e.row, expected, "the provider's entry, semantically");
        assert_eq!(e.source, "slack");
        assert_eq!(e.source_fetcher, "slack.audit_logs");
    }
}

#[tokio::test]
async fn the_action_entity_and_limit_knobs_shape_the_request() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("a1", "user_login")]]);
    let mut cfg = org_config(&provider);
    cfg.services = vec![service_with(&[
        ("action", json!("user_login")),
        ("entity", json!("user")),
        ("limit", json!(50)),
    ])];
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    let seen = provider.requests_to("/audit/v1/logs");
    assert_eq!(seen[0].query_value("action"), Some("user_login"));
    assert_eq!(seen[0].query_value("entity"), Some("user"));
    assert_eq!(seen[0].query_value("limit"), Some("50"));

    // Slack caps a page at 1000; a non-integer limit is ignored.
    provider.serve(vec![vec![]]);
    let mut cfg = org_config(&provider);
    cfg.services = vec![service_with(&[("limit", json!(5000))])];
    run(config(cfg), None).await.0.expect("fetch");
    assert_eq!(
        provider
            .requests_to("/audit/v1/logs")
            .last()
            .unwrap()
            .query_value("limit"),
        Some("1000")
    );
    provider.serve(vec![vec![]]);
    let mut cfg = org_config(&provider);
    cfg.services = vec![service_with(&[("limit", json!("250"))])];
    run(config(cfg), None).await.0.expect("fetch");
    assert_eq!(
        provider
            .requests_to("/audit/v1/logs")
            .last()
            .unwrap()
            .query_value("limit"),
        Some("200")
    );
}

#[tokio::test]
async fn without_a_window_the_last_hour_is_fetched() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![]]);
    let before = Utc::now();
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty(), "an empty page lands nothing");

    let seen = provider.requests_to("/audit/v1/logs");
    let oldest: i64 = seen[0].query_value("oldest").unwrap().parse().unwrap();
    let latest: i64 = seen[0].query_value("latest").unwrap().parse().unwrap();
    assert_eq!(latest - oldest, 3600, "one hour of lookback");
    assert!(
        latest >= before.timestamp() - 1 && latest <= Utc::now().timestamp(),
        "the window ends now: {latest} vs {before}"
    );
}

#[tokio::test]
async fn a_credential_secret_spec_is_resolved_and_wins_over_the_literal_token() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("a1", "user_login")]]);
    // nextest runs one process per test, so the variable leaks nowhere.
    unsafe { std::env::set_var("DFE_FETCHER_TEST_SLACK_TOKEN", "tok-from-env") };
    let mut cfg = org_config(&provider);
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_SLACK_TOKEN".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        provider.requests_to("/audit/v1/logs")[0].header("authorization"),
        Some("Bearer tok-from-env")
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![
        entry("a1", "user_login"),
        entry("a2", "file_downloaded"),
        entry("a3", "user_login"),
    ]]);
    let mut cfg = org_config(&provider);
    cfg.filter = Some("action != \"user_login\"".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["a2"]);
}

#[tokio::test]
async fn an_empty_page_with_a_next_cursor_is_followed() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![entry("a1", "user_login")],
        vec![],
        vec![entry("a2", "user_login")],
    ]);
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    outcome.expect("fetch");
    assert_eq!(provider.requests_to("/audit/v1/logs").len(), 3);
    assert_eq!(rows.len(), 2);
}

/// Slack reports a provider-level failure inside a 200 (`ok: false`): the
/// tick fails with Slack's reason, nothing lands, and the scheduler does not
/// advance the window past it.
#[tokio::test]
async fn an_ok_false_body_fails_the_tick_with_slacks_reason() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("a1", "user_login")]]);
    provider.fail_first(json!({"ok": false, "error": "invalid_auth"}));
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to("/audit/v1/logs").len(), 1);
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("invalid_auth"), "{err}");
}

/// A 5xx the provider keeps answering: the tick fails after the bounded
/// retries and nothing lands.
#[tokio::test]
async fn a_server_error_fails_the_tick_after_retries_and_lands_nothing() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("a1", "user_login")]]);
    for _ in 0..4 {
        provider.answer_first(500, Some(0));
    }
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(
        provider.requests_to("/audit/v1/logs").len(),
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
    provider.serve(vec![vec![entry("a1", "user_login")]]);
    provider.answer_first(429, Some(0));
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    outcome.expect("tick");
    assert_eq!(
        provider.requests_to("/audit/v1/logs").len(),
        2,
        "429 then 200"
    );
    assert_eq!(rows.len(), 1);
}

/// A 401 or 403 is never retried: the tick fails on the one request.
#[tokio::test]
async fn a_refusal_is_never_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("a1", "user_login")]]);
    provider.answer_first(403, Some(0));
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to("/audit/v1/logs").len(), 1);
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(err.contains("403"), "{err}");
}

/// Two connections of the type poll independently, and each record carries
/// its connection's `_source_fetcher` tag (`<connection>.<unit>`), so the
/// connection is distinguishable on the record.
#[tokio::test]
async fn two_connections_poll_independently() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("a1", "user_login")]]);
    let mut cfg = org_config(&provider);
    cfg.token = None;
    cfg.connections = vec![
        SlackConnection {
            id: "slack-acme".into(),
            token: Some("tok-a".to_string().into()),
            ..SlackConnection::default()
        },
        SlackConnection {
            id: "slack-globex".into(),
            token: Some("tok-b".to_string().into()),
            ..SlackConnection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["slack-acme", "slack-globex"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        for row in rows {
            tags.push((id.to_string(), enriched(&row).source_fetcher));
        }
    }
    let seen = provider.requests_to("/audit/v1/logs");
    assert_eq!(seen[0].header("authorization"), Some("Bearer tok-a"));
    assert_eq!(seen[1].header("authorization"), Some("Bearer tok-b"));
    assert_eq!(
        tags,
        [
            (
                "slack-acme".to_string(),
                "slack-acme.audit_logs".to_string()
            ),
            (
                "slack-globex".to_string(),
                "slack-globex.audit_logs".to_string()
            ),
        ]
    );
}

/// A service the profile does not know is refused at validation by name,
/// instead of being skipped with a warning on every tick.
#[tokio::test]
async fn an_unknown_service_name_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![entry("a1", "user_login")]]);
    let mut cfg = org_config(&provider);
    cfg.services = vec![SlackService {
        name: "audit_log".into(),
        config: HashMap::new(),
    }];
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.slack") && err.contains("audit_log"),
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
    let mut cfg = org_config(&provider);
    cfg.token = None;
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.slack") && err.contains("credential_secret"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "the tick never starts: {outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn the_health_check_probes_auth_test_and_reads_ok() {
    let provider = crate::saas_provider::start().await;
    let healthy = health(config(org_config(&provider))).await.expect("health");
    assert!(healthy);
    let seen = provider.requests_to("/api/auth.test");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].header("authorization"), Some("Bearer tok-slack"));
    assert_eq!(seen[0].header("accept"), Some("application/json"));

    // Slack answers a bad token with a 200 carrying `ok: false`: unhealthy,
    // reported with Slack's reason.
    provider.probe_answers(json!({"ok": false, "error": "invalid_auth"}));
    let err = health(config(org_config(&provider)))
        .await
        .expect_err("ok: false is unhealthy even on a 200");
    assert!(err.contains("invalid_auth"), "{err}");
}
