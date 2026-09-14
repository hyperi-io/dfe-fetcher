// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_crowdstrike.rs
// Purpose:   Characterisation of the CrowdStrike Falcon alerts source: token exchange, id query, entity lookup
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The CrowdStrike Falcon alerts source against the in-test provider.
//!
//! Each test configures the typed `sources.crowdstrike` block, runs one
//! tick through the real pipeline into scalo's memory transport, and asserts
//! on the OAuth2 exchange the provider recorded, the two-stage fetch (the
//! FQL id query walked by offset against `meta.pagination.total`, then the
//! entities POST for the ids collected) and the records that landed (the
//! provider's alert, semantically, plus what enrichment added). The typed
//! config block is the operator's contract; the shipped `crowdstrike`
//! profile serves it through the framework driver.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use dfe_fetcher::config::{
    Config, CrowdstrikeConnection, CrowdstrikeService, CrowdstrikeSourceConfig,
};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::saas_provider::{Provider, Seen};

const QUERY: &str = "/alerts/queries/alerts/v2";
const ENTITIES: &str = "/alerts/entities/alerts/v2";

/// A deployment config carrying `crowdstrike` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(crowdstrike: CrowdstrikeSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.crowdstrike = crowdstrike;
    config
}

/// The typed block an operator writes: the Falcon API client, the region
/// base pointed at the provider, the alerts service.
fn tenant_config(provider: &Provider) -> CrowdstrikeSourceConfig {
    CrowdstrikeSourceConfig {
        enabled: true,
        api_url_override: Some(provider.base_url()),
        client_id: Some("falcon-client".into()),
        client_secret: Some("secret-cs".to_string().into()),
        services: vec![CrowdstrikeService {
            name: "alerts".into(),
            config: HashMap::new(),
        }],
        ..CrowdstrikeSourceConfig::default()
    }
}

fn service_with(config: &[(&str, Value)]) -> CrowdstrikeService {
    CrowdstrikeService {
        name: "alerts".into(),
        config: config
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    }
}

/// One tick of the `crowdstrike` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "crowdstrike", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "crowdstrike")).await
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

fn alert(id: &str, severity: u64) -> Value {
    json!({
        "composite_id": id,
        "created_timestamp": "2026-05-21T13:31:00.000Z",
        "severity": severity,
        "severity_name": if severity >= 70 { "High" } else { "Low" },
        "status": "new",
        "tactic": "Execution",
        "device": { "hostname": "host-1", "platform_name": "Linux" }
    })
}

fn query_of(seen: &Seen) -> Vec<(&str, &str)> {
    seen.query
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect()
}

fn composite_ids(seen: &Seen) -> Vec<&str> {
    seen.body.as_ref().unwrap()["composite_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn alerts_are_queried_by_offset_then_looked_up_by_composite_id() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![alert("a:1", 90), alert("a:2", 20)],
        vec![alert("a:3", 70), alert("a:4", 50)],
        vec![alert("a:5", 10)],
    ]);
    let w = fetch_window("2026-05-21T13:30:00.987Z", "2026-05-21T14:30:00Z");
    let mut cfg = tenant_config(&provider);
    cfg.services = vec![service_with(&[("limit", json!(2))])];

    let (outcome, rows) = run(config(cfg), Some(&w)).await;
    outcome.expect("fetch");

    let exchanges = provider.token_exchanges();
    assert_eq!(exchanges.len(), 1, "one token exchange for the tick");
    assert_eq!(
        exchanges[0].get("client_id").map(String::as_str),
        Some("falcon-client")
    );
    assert_eq!(
        exchanges[0].get("client_secret").map(String::as_str),
        Some("secret-cs")
    );
    assert_eq!(
        exchanges[0].get("grant_type").map(String::as_str),
        Some("client_credentials"),
        "the standard OAuth2 form field, which Falcon accepts"
    );

    let queries = provider.requests_to(QUERY);
    assert_eq!(
        queries.len(),
        3,
        "5 ids at 2 per page: offsets 0, 2 and 4, stopping at the total"
    );
    assert_eq!(
        query_of(&queries[0]),
        [
            (
                "filter",
                "created_timestamp:>'2026-05-21T13:30:00Z'+created_timestamp:<'2026-05-21T14:30:00Z'"
            ),
            ("limit", "2"),
            ("offset", "0"),
            ("sort", "created_timestamp.asc"),
        ],
        "the FQL window to the second, oldest first"
    );
    assert_eq!(queries[1].query_value("offset"), Some("2"));
    assert_eq!(queries[2].query_value("offset"), Some("4"));
    for request in &queries {
        assert_eq!(request.header("authorization"), Some("Bearer cs-token-1"));
        assert_eq!(request.header("accept"), Some("application/json"));
    }

    let lookups = provider.requests_to(ENTITIES);
    assert_eq!(lookups.len(), 1, "one entities POST for the five ids");
    assert_eq!(
        composite_ids(&lookups[0]),
        ["a:1", "a:2", "a:3", "a:4", "a:5"],
        "the ids in query order"
    );
    assert_eq!(
        lookups[0].header("authorization"),
        Some("Bearer cs-token-1")
    );
    assert_eq!(lookups[0].header("accept"), Some("application/json"));

    assert_eq!(rows.len(), 5, "every entity lands, in order");
    for (row, expected) in rows.iter().zip([
        alert("a:1", 90),
        alert("a:2", 20),
        alert("a:3", 70),
        alert("a:4", 50),
        alert("a:5", 10),
    ]) {
        assert_eq!(row.topic, "crowdstrike_land");
        let e = enriched(row);
        assert_eq!(e.row, expected, "the provider's alert, semantically");
        assert_eq!(e.source, "crowdstrike");
        assert_eq!(e.source_fetcher, "crowdstrike.alerts");
    }
}

/// The `filter` knob is ANDed onto the window clause; `limit` is capped at
/// 1000 and a non-integer one ignored (default 100).
#[tokio::test]
async fn the_filter_and_limit_knobs_shape_the_query() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![alert("a:1", 90)]]);
    let mut cfg = tenant_config(&provider);
    cfg.services = vec![service_with(&[
        ("filter", json!("severity:>=70")),
        ("limit", json!(5000)),
    ])];
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    let query = &provider.requests_to(QUERY)[0];
    assert!(
        query
            .query_value("filter")
            .unwrap()
            .ends_with("'+severity:>=70"),
        "{:?}",
        query.query_value("filter")
    );
    assert_eq!(query.query_value("limit"), Some("1000"));

    provider.serve(vec![vec![alert("a:1", 90)]]);
    let mut cfg = tenant_config(&provider);
    cfg.services = vec![service_with(&[("limit", json!("250"))])];
    run(config(cfg), None).await.0.expect("fetch");
    assert_eq!(
        provider
            .requests_to(QUERY)
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
    assert!(
        rows.is_empty(),
        "no ids: nothing is looked up, nothing lands"
    );
    assert!(
        provider.requests_to(ENTITIES).is_empty(),
        "no entities POST without ids"
    );

    let filter = provider.requests_to(QUERY)[0]
        .query_value("filter")
        .unwrap()
        .to_owned();
    let (start, end) = filter
        .strip_prefix("created_timestamp:>'")
        .and_then(|f| f.split_once("'+created_timestamp:<'"))
        .map(|(s, e)| (at(s), at(e.trim_end_matches('\''))))
        .expect("the window clause");
    assert_eq!((end - start).num_seconds(), 3600, "one hour of lookback");
    assert!(
        end >= before - chrono::Duration::seconds(1) && end <= Utc::now(),
        "the window ends now: {end} vs {before}"
    );
}

#[tokio::test]
async fn a_credential_secret_spec_is_resolved_as_the_client_secret() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![alert("a:1", 90)]]);
    // nextest runs one process per test, so the variable leaks nowhere.
    unsafe { std::env::set_var("DFE_FETCHER_TEST_CS_SECRET", "secret-from-env") };
    let mut cfg = tenant_config(&provider);
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_CS_SECRET".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        provider.token_exchanges()[0]
            .get("client_secret")
            .map(String::as_str),
        Some("secret-from-env")
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![
        alert("a:1", 90),
        alert("a:2", 20),
        alert("a:3", 70),
    ]]);
    let mut cfg = tenant_config(&provider);
    cfg.filter = Some("severity >= 70".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["composite_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["a:1", "a:3"]);
}

/// A token exchange Falcon refuses: the tick fails with the exchange's
/// status and no data is requested.
#[tokio::test]
async fn a_refused_token_exchange_fails_the_tick_and_requests_no_data() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![alert("a:1", 90)]]);
    let mut cfg = tenant_config(&provider);
    cfg.client_secret = Some("wrong".to_string().into());
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.token_exchanges().len(), 1);
    assert!(provider.requests_to(QUERY).is_empty());
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(err.contains("400"), "{err}");
}

/// A 5xx the id query keeps answering: the tick fails after the bounded
/// retries, nothing is looked up, nothing lands.
#[tokio::test]
async fn a_server_error_fails_the_tick_after_retries_and_lands_nothing() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![alert("a:1", 90)]]);
    for _ in 0..4 {
        provider.answer_first(500, Some(0));
    }
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(
        provider.requests_to(QUERY).len(),
        4,
        "the first attempt and three retries"
    );
    assert!(provider.requests_to(ENTITIES).is_empty());
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
}

/// A 429 with `Retry-After` is retried after the wait, and the alerts then
/// land.
#[tokio::test]
async fn a_429_with_retry_after_is_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![alert("a:1", 90)]]);
    provider.answer_first(429, Some(0));
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    outcome.expect("tick");
    assert_eq!(provider.requests_to(QUERY).len(), 2, "429 then 200");
    assert_eq!(rows.len(), 1);
}

/// A 401 or 403 is never retried: the tick fails on the one request.
#[tokio::test]
async fn a_refusal_is_never_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![alert("a:1", 90)]]);
    provider.answer_first(403, Some(0));
    let (outcome, rows) = run(config(tenant_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to(QUERY).len(), 1);
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
    provider.serve(vec![vec![alert("a:1", 90)]]);
    let mut cfg = tenant_config(&provider);
    cfg.client_id = None;
    cfg.client_secret = None;
    cfg.connections = vec![
        CrowdstrikeConnection {
            id: "cs-us".into(),
            client_id: Some("client-us".into()),
            client_secret: Some("secret-a".to_string().into()),
            ..CrowdstrikeConnection::default()
        },
        CrowdstrikeConnection {
            id: "cs-eu".into(),
            client_id: Some("client-eu".into()),
            client_secret: Some("secret-b".to_string().into()),
            ..CrowdstrikeConnection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["cs-us", "cs-eu"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        for row in rows {
            tags.push((id.to_string(), enriched(&row).source_fetcher));
        }
    }
    let exchanges = provider.token_exchanges();
    assert_eq!(
        exchanges[0].get("client_id").map(String::as_str),
        Some("client-us")
    );
    assert_eq!(
        exchanges[1].get("client_id").map(String::as_str),
        Some("client-eu")
    );
    let queries = provider.requests_to(QUERY);
    assert_eq!(
        queries[0].header("authorization"),
        Some("Bearer cs-token-1")
    );
    assert_eq!(
        queries[1].header("authorization"),
        Some("Bearer cs-token-2")
    );
    assert_eq!(
        tags,
        [
            ("cs-us".to_string(), "cs-us.alerts".to_string()),
            ("cs-eu".to_string(), "cs-eu.alerts".to_string()),
        ]
    );
}

/// A service the profile does not know is refused at validation by name,
/// instead of being skipped with a warning on every tick.
#[tokio::test]
async fn an_unknown_service_name_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![alert("a:1", 90)]]);
    let mut cfg = tenant_config(&provider);
    cfg.services = vec![CrowdstrikeService {
        name: "detections".into(),
        config: HashMap::new(),
    }];
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.crowdstrike") && err.contains("detections"),
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
    provider.serve(vec![vec![alert("a:1", 90)]]);
    let mut cfg = tenant_config(&provider);
    cfg.client_id = Some(String::new());
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.crowdstrike") && err.contains("client_id"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "the tick never starts: {outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());

    let mut cfg = tenant_config(&provider);
    cfg.client_secret = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.crowdstrike") && err.contains("credential_secret"),
        "{err}"
    );
}

/// The health check is the token exchange: a minted token is healthy, a
/// refused exchange is the error, and no data endpoint is touched.
#[tokio::test]
async fn the_health_check_is_the_token_exchange() {
    let provider = crate::saas_provider::start().await;
    let healthy = health(config(tenant_config(&provider)))
        .await
        .expect("health");
    assert!(healthy);
    assert_eq!(provider.token_exchanges().len(), 1);
    assert!(provider.requests_to(QUERY).is_empty());

    let mut cfg = tenant_config(&provider);
    cfg.client_secret = Some("wrong".to_string().into());
    let err = health(config(cfg))
        .await
        .expect_err("a refused exchange is unhealthy");
    assert!(err.contains("400"), "{err}");
}
