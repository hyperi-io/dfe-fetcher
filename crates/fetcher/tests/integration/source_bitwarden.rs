// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_bitwarden.rs
// Purpose:   Characterisation of the Bitwarden Events source: token exchange, requests sent, records landed
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Bitwarden organisation Events source against the in-test provider.
//!
//! Each test configures the typed `sources.bitwarden` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the OAuth2 exchange the provider recorded (the form fields), the data
//! requests (path, query, headers, how `continuationToken` was followed) and
//! the records that landed (the provider's event, semantically, plus what
//! enrichment added). The typed config block is the operator's contract;
//! the shipped `bitwarden` profile serves it through the framework driver.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use dfe_fetcher::config::{BitwardenConnection, BitwardenService, BitwardenSourceConfig, Config};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::saas_provider::{Provider, Seen};

/// A deployment config carrying `bitwarden` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(bitwarden: BitwardenSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.bitwarden = bitwarden;
    config
}

/// The typed block an operator writes for a self-hosted vault: the
/// organisation API client, both URL overrides pointed at the provider,
/// the events service.
fn org_config(provider: &Provider) -> BitwardenSourceConfig {
    BitwardenSourceConfig {
        enabled: true,
        client_id: Some("organization.11111111-2222-3333-4444-555555555555".into()),
        client_secret: Some("secret-bw".to_string().into()),
        api_url_override: Some(provider.base_url()),
        identity_url_override: Some(format!("{}/connect/token", provider.base_url())),
        services: vec![BitwardenService {
            name: "events".into(),
            config: HashMap::new(),
        }],
        ..BitwardenSourceConfig::default()
    }
}

/// One tick of the `bitwarden` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "bitwarden", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "bitwarden")).await
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

fn event(id: &str, event_type: u64) -> Value {
    json!({
        "object": "event",
        "id": id,
        "type": event_type,
        "itemId": null,
        "collectionId": null,
        "groupId": null,
        "policyId": null,
        "memberId": "m1",
        "actingUserId": "u1",
        "date": "2026-05-21T13:31:00.000Z",
        "device": 9,
        "ipAddress": "203.0.113.9"
    })
}

fn query_of(seen: &Seen) -> Vec<(&str, &str)> {
    seen.query
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect()
}

#[tokio::test]
async fn events_follow_the_continuation_token_after_a_client_credentials_exchange() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![event("e1", 1000), event("e2", 1100)],
        vec![event("e3", 1500)],
        vec![event("e4", 1600)],
    ]);
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(config(org_config(&provider)), Some(&w)).await;
    outcome.expect("fetch");

    let exchanges = provider.token_exchanges();
    assert_eq!(exchanges.len(), 1, "one token exchange for the tick");
    assert_eq!(
        exchanges[0].get("grant_type").map(String::as_str),
        Some("client_credentials")
    );
    assert_eq!(
        exchanges[0].get("client_id").map(String::as_str),
        Some("organization.11111111-2222-3333-4444-555555555555")
    );
    assert_eq!(
        exchanges[0].get("client_secret").map(String::as_str),
        Some("secret-bw")
    );
    assert_eq!(
        exchanges[0].get("scope").map(String::as_str),
        Some("api.organization"),
        "the organisation scope is always requested"
    );

    let seen = provider.requests_to("/public/events");
    assert_eq!(seen.len(), 3, "one request per page");
    assert_eq!(
        query_of(&seen[0]),
        [
            ("end", "2026-05-21T14:30:00Z"),
            ("start", "2026-05-21T13:30:00Z"),
        ],
        "start/end to the second with Z"
    );
    assert_eq!(
        query_of(&seen[1]),
        [
            ("continuationToken", "ct-1"),
            ("end", "2026-05-21T14:30:00Z"),
            ("start", "2026-05-21T13:30:00Z"),
        ],
        "the token is added to the same query"
    );
    assert_eq!(seen[2].query_value("continuationToken"), Some("ct-2"));
    for request in &seen {
        assert_eq!(
            request.header("authorization"),
            Some("Bearer bw-token-1"),
            "the minted token is the bearer"
        );
        assert_eq!(request.header("accept"), Some("application/json"));
    }

    assert_eq!(rows.len(), 4, "every page's events land, in order");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["e1", "e2", "e3", "e4"]);
    for (row, expected) in rows.iter().zip([
        event("e1", 1000),
        event("e2", 1100),
        event("e3", 1500),
        event("e4", 1600),
    ]) {
        assert_eq!(row.topic, "bitwarden_land");
        let e = enriched(row);
        assert_eq!(e.row, expected, "the provider's event, semantically");
        assert_eq!(e.source, "bitwarden");
        assert_eq!(e.source_fetcher, "bitwarden.events");
    }
}

#[tokio::test]
async fn without_a_window_the_last_hour_is_fetched() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![]]);
    let before = Utc::now();
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty(), "an empty page lands nothing");

    let seen = provider.requests_to("/public/events");
    let start = at(seen[0].query_value("start").unwrap());
    let end = at(seen[0].query_value("end").unwrap());
    assert_eq!((end - start).num_seconds(), 3600, "one hour of lookback");
    assert!(
        end >= before - chrono::Duration::seconds(1) && end <= Utc::now(),
        "the window ends now: {end} vs {before}"
    );
}

#[tokio::test]
async fn a_credential_secret_spec_is_resolved_as_the_client_secret() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", 1000)]]);
    // nextest runs one process per test, so the variable leaks nowhere.
    unsafe { std::env::set_var("DFE_FETCHER_TEST_BW_SECRET", "secret-from-env") };
    let mut cfg = org_config(&provider);
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_BW_SECRET".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        provider.token_exchanges()[0]
            .get("client_secret")
            .map(String::as_str),
        Some("secret-from-env"),
        "the spec wins over the literal client_secret"
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![
        event("e1", 1000),
        event("e2", 1500),
        event("e3", 1600),
    ]]);
    let mut cfg = org_config(&provider);
    cfg.filter = Some("type < 1500".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["e1"]);
}

#[tokio::test]
async fn an_empty_page_with_a_continuation_token_is_followed() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![event("e1", 1000)],
        vec![],
        vec![event("e2", 1000)],
    ]);
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    outcome.expect("fetch");
    assert_eq!(provider.requests_to("/public/events").len(), 3);
    assert_eq!(rows.len(), 2);
}

/// A token exchange the identity server refuses: the tick fails with the
/// exchange's status and no data is requested.
#[tokio::test]
async fn a_refused_token_exchange_fails_the_tick_and_requests_no_data() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", 1000)]]);
    let mut cfg = org_config(&provider);
    cfg.client_secret = Some("wrong".to_string().into());
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.token_exchanges().len(), 1);
    assert!(provider.requests_to("/public/events").is_empty());
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(err.contains("400"), "{err}");
}

/// A 5xx the provider keeps answering: the tick fails after the bounded
/// retries and nothing lands.
#[tokio::test]
async fn a_server_error_fails_the_tick_after_retries_and_lands_nothing() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", 1000)]]);
    for _ in 0..4 {
        provider.answer_first(500, Some(0));
    }
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(
        provider.requests_to("/public/events").len(),
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
    provider.serve(vec![vec![event("e1", 1000)]]);
    provider.answer_first(429, Some(0));
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    outcome.expect("tick");
    assert_eq!(
        provider.requests_to("/public/events").len(),
        2,
        "429 then 200"
    );
    assert_eq!(rows.len(), 1);
}

/// A 401 or 403 is never retried: the tick fails on the one request.
#[tokio::test]
async fn a_refusal_is_never_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", 1000)]]);
    provider.answer_first(403, Some(0));
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to("/public/events").len(), 1);
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(err.contains("403"), "{err}");
}

/// Two connections of the type exchange their own client credentials, and
/// each record carries its connection's `_source_fetcher` tag
/// (`<connection>.<unit>`), so the connection is distinguishable on the
/// record.
#[tokio::test]
async fn two_connections_poll_independently() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", 1000)]]);
    let mut cfg = org_config(&provider);
    cfg.client_id = None;
    cfg.client_secret = None;
    cfg.connections = vec![
        BitwardenConnection {
            id: "bw-acme".into(),
            client_id: Some("organization.acme".into()),
            client_secret: Some("secret-a".to_string().into()),
            ..BitwardenConnection::default()
        },
        BitwardenConnection {
            id: "bw-globex".into(),
            client_id: Some("organization.globex".into()),
            client_secret: Some("secret-b".to_string().into()),
            ..BitwardenConnection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["bw-acme", "bw-globex"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        for row in rows {
            tags.push((id.to_string(), enriched(&row).source_fetcher));
        }
    }
    let exchanges = provider.token_exchanges();
    assert_eq!(
        exchanges[0].get("client_id").map(String::as_str),
        Some("organization.acme")
    );
    assert_eq!(
        exchanges[1].get("client_id").map(String::as_str),
        Some("organization.globex")
    );
    let seen = provider.requests_to("/public/events");
    assert_eq!(seen[0].header("authorization"), Some("Bearer bw-token-1"));
    assert_eq!(seen[1].header("authorization"), Some("Bearer bw-token-2"));
    assert_eq!(
        tags,
        [
            ("bw-acme".to_string(), "bw-acme.events".to_string()),
            ("bw-globex".to_string(), "bw-globex.events".to_string()),
        ]
    );
}

/// A service the profile does not know is refused at validation by name,
/// instead of being skipped with a warning on every tick.
#[tokio::test]
async fn an_unknown_service_name_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", 1000)]]);
    let mut cfg = org_config(&provider);
    cfg.services = vec![BitwardenService {
        name: "event".into(),
        config: HashMap::new(),
    }];
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.bitwarden") && err.contains("event"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "{outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());
}

/// A missing client id or client secret is refused at validation naming
/// the field, instead of failing silently every tick.
#[tokio::test]
async fn a_missing_client_id_or_secret_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", 1000)]]);
    let mut cfg = org_config(&provider);
    cfg.client_id = Some(String::new());
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.bitwarden") && err.contains("client_id"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "the tick never starts: {outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());

    let mut cfg = org_config(&provider);
    cfg.client_secret = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.bitwarden") && err.contains("credential_secret"),
        "{err}"
    );
}

/// The health check is the token exchange: a minted token is healthy, a
/// refused exchange is the error, and no data endpoint is touched.
#[tokio::test]
async fn the_health_check_is_the_token_exchange() {
    let provider = crate::saas_provider::start().await;
    let healthy = health(config(org_config(&provider))).await.expect("health");
    assert!(healthy);
    assert_eq!(provider.token_exchanges().len(), 1);
    assert!(provider.requests_to("/public/events").is_empty());

    let mut cfg = org_config(&provider);
    cfg.client_secret = Some("wrong".to_string().into());
    let err = health(config(cfg))
        .await
        .expect_err("a refused exchange is unhealthy");
    assert!(err.contains("400"), "{err}");
}
