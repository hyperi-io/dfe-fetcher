// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_okta.rs
// Purpose:   Characterisation of the Okta System Log source: requests sent, records landed
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Okta System Log source against the in-test provider.
//!
//! Same shape as the GitHub module: the typed `sources.okta` block is the
//! operator's contract, one tick runs through the real pipeline into scalo's
//! memory transport, and the assertions cover the requests the provider
//! recorded and the records that landed. The shipped `okta` profile serves
//! the block through the framework driver.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use dfe_fetcher::config::{Config, OktaConnection, OktaService, OktaSourceConfig};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::saas_provider::{Provider, Seen};

/// A deployment config carrying `okta` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(okta: OktaSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.okta = okta;
    config
}

/// The typed block an operator writes: the tenant URL, an SSWS token, the
/// system_log service.
fn tenant_config(provider: &Provider) -> OktaSourceConfig {
    OktaSourceConfig {
        enabled: true,
        tenant_url: Some(provider.base_url()),
        token: Some("tok-okta".to_string().into()),
        services: vec![OktaService {
            name: "system_log".into(),
            config: HashMap::new(),
        }],
        ..OktaSourceConfig::default()
    }
}

fn service_with(config: &[(&str, Value)]) -> OktaService {
    OktaService {
        name: "system_log".into(),
        config: config
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    }
}

async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "okta", window)).await
}

async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "okta")).await
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

fn event(uuid: &str, event_type: &str) -> Value {
    json!({
        "uuid": uuid,
        "eventType": event_type,
        "published": "2026-05-21T13:31:00.000Z",
        "actor": { "id": "00u1", "type": "User", "alternateId": "kaz@example.com" },
        "outcome": { "result": "SUCCESS" },
        "debugContext": { "debugData": { "requestUri": "/api/v1/authn" } }
    })
}

fn query_of(seen: &Seen) -> Vec<(&str, &str)> {
    seen.query
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect()
}

#[tokio::test]
async fn system_log_follows_the_link_header_with_ssws_and_a_millisecond_window() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![
            event("u1", "user.session.start"),
            event("u2", "user.authentication.sso"),
        ],
        vec![event("u3", "user.session.end")],
    ]);
    let w = fetch_window("2026-05-21T13:30:00.987654321Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(config(tenant_config(&provider)), Some(&w)).await;
    outcome.expect("fetch");

    let seen = provider.requests_to("/api/v1/logs");
    assert_eq!(seen.len(), 2, "one request per page");
    assert_eq!(
        query_of(&seen[0]),
        [
            ("limit", "100"),
            ("since", "2026-05-21T13:30:00.987Z"),
            ("sortOrder", "ASCENDING"),
            ("until", "2026-05-21T14:30:00.000Z"),
        ],
        "ISO millis with a Z suffix, the default page size, ascending"
    );
    assert_eq!(
        query_of(&seen[1]),
        [("after", "cursor-2"), ("page", "2"), ("per_page", "100")],
        "the Link URL is requested as given"
    );
    for request in &seen {
        assert_eq!(request.header("authorization"), Some("SSWS tok-okta"));
        assert_eq!(request.header("accept"), Some("application/json"));
    }

    assert_eq!(rows.len(), 3);
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["uuid"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["u1", "u2", "u3"]);
    for (row, expected) in rows.iter().zip([
        event("u1", "user.session.start"),
        event("u2", "user.authentication.sso"),
        event("u3", "user.session.end"),
    ]) {
        assert_eq!(row.topic, "okta_land");
        let e = enriched(row);
        assert_eq!(e.row, expected, "the provider's record, semantically");
        assert_eq!(e.source, "okta");
        assert_eq!(e.source_fetcher, "okta.system_log");
    }
}

#[tokio::test]
async fn a_bearer_header_when_ssws_is_off() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("u1", "user.session.start")]]);
    let mut cfg = tenant_config(&provider);
    cfg.use_ssws_header = false;
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        provider.requests_to("/api/v1/logs")[0].header("authorization"),
        Some("Bearer tok-okta")
    );
}

#[tokio::test]
async fn the_limit_is_capped_at_1000_and_the_filter_is_sent_verbatim() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![]]);
    let mut cfg = tenant_config(&provider);
    cfg.services = vec![service_with(&[
        ("limit", json!(5000)),
        ("filter", json!("eventType eq \"user.session.start\"")),
    ])];
    let (outcome, _) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let seen = provider.requests_to("/api/v1/logs");
    assert_eq!(seen[0].query_value("limit"), Some("1000"));
    assert_eq!(
        seen[0].query_value("filter"),
        Some("eventType eq \"user.session.start\"")
    );

    // A limit that is not a JSON integer is ignored: the default applies.
    provider.serve(vec![vec![]]);
    let mut cfg = tenant_config(&provider);
    cfg.services = vec![service_with(&[("limit", json!("250"))])];
    let (outcome, _) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let seen = provider.requests_to("/api/v1/logs");
    assert_eq!(seen[1].query_value("limit"), Some("100"));
    assert_eq!(seen[1].query_value("filter"), None);
}

#[tokio::test]
async fn the_api_url_override_beats_the_tenant_url_and_a_trailing_slash_is_trimmed() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("u1", "user.session.start")]]);
    let mut cfg = tenant_config(&provider);
    cfg.tenant_url = Some("https://nowhere.invalid".into());
    cfg.api_url_override = Some(format!("{}/", provider.base_url()));
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(provider.requests_to("/api/v1/logs").len(), 1);
}

#[tokio::test]
async fn without_a_window_the_last_hour_is_fetched() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![]]);
    let before = Utc::now();
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty());
    let seen = provider.requests_to("/api/v1/logs");
    let start = at(seen[0].query_value("since").expect("since"));
    let end = at(seen[0].query_value("until").expect("until"));
    assert_eq!((end - start).num_seconds(), 3600);
    assert!(end >= before - chrono::Duration::seconds(1) && end <= Utc::now());
}

#[tokio::test]
async fn a_credential_secret_spec_is_resolved_and_wins_over_the_literal_token() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("u1", "user.session.start")]]);
    unsafe { std::env::set_var("DFE_FETCHER_TEST_OKTA_TOKEN", "tok-from-env") };
    let mut cfg = tenant_config(&provider);
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_OKTA_TOKEN".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        provider.requests_to("/api/v1/logs")[0].header("authorization"),
        Some("SSWS tok-from-env")
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![
        event("u1", "user.session.start"),
        event("u2", "user.session.access_token"),
    ]]);
    let mut cfg = tenant_config(&provider);
    cfg.filter = Some("eventType != \"user.session.access_token\"".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["uuid"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["u1"]);
}

#[tokio::test]
async fn an_empty_page_with_a_next_link_is_followed() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![event("u1", "user.session.start")],
        vec![],
        vec![event("u2", "user.session.start")],
    ]);
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    outcome.expect("fetch");
    assert_eq!(provider.requests_to("/api/v1/logs").len(), 3);
    assert_eq!(rows.len(), 2);
}

/// A 5xx the provider keeps answering: the tick fails after the bounded
/// retries and nothing lands.
#[tokio::test]
async fn a_server_error_fails_the_tick_after_retries_and_lands_nothing() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("u1", "user.session.start")]]);
    for _ in 0..4 {
        provider.answer_first(500, Some(0));
    }
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to("/api/v1/logs").len(), 4);
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
}

/// A 429 with `Retry-After` is retried after the wait, and the page then
/// lands.
#[tokio::test]
async fn a_429_with_retry_after_is_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("u1", "user.session.start")]]);
    provider.answer_first(429, Some(0));
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    outcome.expect("tick");
    assert_eq!(
        provider.requests_to("/api/v1/logs").len(),
        2,
        "429 then 200"
    );
    assert_eq!(rows.len(), 1);
}

/// A 401 or 403 is never retried: the tick fails on the one request.
#[tokio::test]
async fn a_refusal_is_never_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("u1", "user.session.start")]]);
    provider.answer_first(401, Some(0));
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to("/api/v1/logs").len(), 1);
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(err.contains("401"), "{err}");
}

/// Two tenants of the type poll independently, each with its own token and
/// header style, and each record carries its connection's `_source_fetcher`
/// tag.
#[tokio::test]
async fn two_connections_poll_independently() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("u1", "user.session.start")]]);
    let mut cfg = tenant_config(&provider);
    cfg.tenant_url = None;
    cfg.token = None;
    cfg.connections = vec![
        OktaConnection {
            id: "okta-a".into(),
            tenant_url: Some(provider.base_url()),
            token: Some("tok-a".to_string().into()),
            ..OktaConnection::default()
        },
        OktaConnection {
            id: "okta-b".into(),
            tenant_url: Some(provider.base_url()),
            token: Some("tok-b".to_string().into()),
            use_ssws_header: Some(false),
            ..OktaConnection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["okta-a", "okta-b"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        for row in rows {
            tags.push((id.to_string(), enriched(&row).source_fetcher));
        }
    }
    let seen = provider.requests_to("/api/v1/logs");
    assert_eq!(seen[0].header("authorization"), Some("SSWS tok-a"));
    assert_eq!(seen[1].header("authorization"), Some("Bearer tok-b"));
    assert_eq!(
        tags,
        [
            ("okta-a".to_string(), "okta-a.system_log".to_string()),
            ("okta-b".to_string(), "okta-b.system_log".to_string()),
        ]
    );
}

/// No tenant URL and no override: the config is refused at validation,
/// naming the block and the field, and nothing is requested.
#[tokio::test]
async fn a_missing_tenant_url_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("u1", "user.session.start")]]);
    let mut cfg = tenant_config(&provider);
    cfg.tenant_url = Some(String::new());
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.okta") && err.contains("tenant_url"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "the tick never starts: {outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());
}

/// A service the profile does not know is refused at validation by name.
#[tokio::test]
async fn an_unknown_service_name_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("u1", "user.session.start")]]);
    let mut cfg = tenant_config(&provider);
    cfg.services = vec![OktaService {
        name: "system_logs".into(),
        config: HashMap::new(),
    }];
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.okta") && err.contains("system_logs"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "{outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn the_health_check_probes_the_current_user() {
    let provider = crate::saas_provider::start().await;
    let healthy = health(config(tenant_config(&provider)))
        .await
        .expect("health");
    assert!(healthy);
    let seen = provider.requests_to("/api/v1/users/me");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].header("authorization"), Some("SSWS tok-okta"));
    assert_eq!(seen[0].header("accept"), Some("application/json"));
}
