// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_github.rs
// Purpose:   Characterisation of the GitHub audit-log source: requests sent, records landed
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The GitHub audit-log source against the in-test provider.
//!
//! Each test configures the typed `sources.github` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! two things: the requests the provider recorded (path, query, headers,
//! how the `Link` header was followed) and the records that landed (the
//! provider's record, semantically, plus what enrichment added). The typed
//! config block is the operator's contract; the shipped `github` profile
//! serves it through the framework driver.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use dfe_fetcher::config::{Config, GithubConnection, GithubService, GithubSourceConfig};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::saas_provider::{Provider, Seen};

/// A deployment config carrying `github` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(github: GithubSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.github = github;
    config
}

/// The typed block an operator writes: one org, a literal token, the
/// audit_log service, pointed at the provider.
fn org_config(provider: &Provider) -> GithubSourceConfig {
    GithubSourceConfig {
        enabled: true,
        org: Some("acme".into()),
        token: Some("tok-github".to_string().into()),
        api_url_override: Some(provider.base_url()),
        services: vec![GithubService {
            name: "audit_log".into(),
            config: HashMap::new(),
        }],
        ..GithubSourceConfig::default()
    }
}

fn service_with(include: &str) -> GithubService {
    let mut config = HashMap::new();
    config.insert("include".to_string(), Value::String(include.to_owned()));
    GithubService {
        name: "audit_log".into(),
        config,
    }
}

/// One tick of the `github` source as configured, through the pipeline.
/// Returns the tick's outcome and whatever landed.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "github", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "github")).await
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

fn event(id: &str, action: &str) -> Value {
    json!({
        "@timestamp": 1_700_000_000_000_u64,
        "_document_id": id,
        "action": action,
        "actor": "octocat",
        "org": "acme",
        "data": { "nested": { "flag": true }, "list": [1, 2, 3] }
    })
}

fn query_of(seen: &Seen) -> Vec<(&str, &str)> {
    seen.query
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect()
}

#[tokio::test]
async fn org_audit_log_follows_the_link_header_and_lands_enriched_records() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![event("e1", "repo.create"), event("e2", "org.add_member")],
        vec![event("e3", "git.clone"), event("e4", "repo.destroy")],
        vec![event("e5", "team.create")],
    ]);
    let w = fetch_window("2026-05-21T13:30:00.123Z", "2026-05-21T14:30:00Z");

    let (outcome, rows) = run(config(org_config(&provider)), Some(&w)).await;
    outcome.expect("fetch");

    let seen = provider.requests_to("/orgs/acme/audit-log");
    assert_eq!(seen.len(), 3, "one request per page");
    assert_eq!(
        query_of(&seen[0]),
        [
            ("include", "all"),
            ("per_page", "100"),
            (
                "phrase",
                "created:2026-05-21T13:30:00+00:00..2026-05-21T14:30:00+00:00"
            ),
        ],
        "the window is the phrase filter, to the second, +00:00 zone"
    );
    assert_eq!(
        query_of(&seen[1]),
        [("after", "cursor-2"), ("page", "2"), ("per_page", "100")],
        "the Link URL is requested as given, nothing re-appended"
    );
    assert_eq!(seen[2].query_value("page"), Some("3"));
    for request in &seen {
        assert_eq!(request.header("authorization"), Some("Bearer tok-github"));
        assert_eq!(
            request.header("accept"),
            Some("application/vnd.github+json")
        );
        assert_eq!(request.header("x-github-api-version"), Some("2022-11-28"));
    }

    assert_eq!(rows.len(), 5, "every page's records land, in order");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["_document_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["e1", "e2", "e3", "e4", "e5"]);
    for (row, expected) in rows.iter().zip([
        event("e1", "repo.create"),
        event("e2", "org.add_member"),
        event("e3", "git.clone"),
        event("e4", "repo.destroy"),
        event("e5", "team.create"),
    ]) {
        assert_eq!(row.topic, "github_land");
        let e = enriched(row);
        assert_eq!(e.row, expected, "the provider's record, semantically");
        assert_eq!(e.source, "github");
        assert_eq!(e.source_fetcher, "github.audit_log");
    }
}

#[tokio::test]
async fn enterprise_scope_and_the_include_knob_shape_the_request() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", "git.push")]]);
    let mut cfg = org_config(&provider);
    cfg.org = None;
    cfg.enterprise = Some("hyperi-ent".into());
    cfg.services = vec![service_with("git")];

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let seen = provider.requests_to("/enterprises/hyperi-ent/audit-log");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].query_value("include"), Some("git"));
    assert_eq!(rows.len(), 1);

    // A value outside all|web|git is not sent as given: the source falls back
    // to `all`.
    provider.serve(vec![vec![]]);
    let mut cfg = org_config(&provider);
    cfg.services = vec![service_with("everything")];
    let (outcome, _) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let seen = provider.requests_to("/orgs/acme/audit-log");
    assert_eq!(seen.last().unwrap().query_value("include"), Some("all"));
}

#[tokio::test]
async fn without_a_window_the_last_hour_is_fetched() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![]]);
    let before = Utc::now();
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty(), "an empty page lands nothing");

    let seen = provider.requests_to("/orgs/acme/audit-log");
    let phrase = seen[0].query_value("phrase").expect("phrase");
    let (start, end) = phrase
        .strip_prefix("created:")
        .and_then(|p| p.split_once(".."))
        .expect("created:<start>..<end>");
    let start = at(start);
    let end = at(end);
    assert_eq!((end - start).num_seconds(), 3600, "one hour of lookback");
    assert!(
        end >= before - chrono::Duration::seconds(1) && end <= Utc::now(),
        "the window ends now: {end} vs {before}"
    );
}

#[tokio::test]
async fn a_credential_secret_spec_is_resolved_and_wins_over_the_literal_token() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", "repo.create")]]);
    // nextest runs one process per test, so the variable leaks nowhere.
    unsafe { std::env::set_var("DFE_FETCHER_TEST_GITHUB_PAT", "tok-from-env") };
    let mut cfg = org_config(&provider);
    cfg.credential_secret = Some("env:DFE_FETCHER_TEST_GITHUB_PAT".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        provider.requests_to("/orgs/acme/audit-log")[0].header("authorization"),
        Some("Bearer tok-from-env")
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![
        event("e1", "repo.create"),
        event("e2", "git.clone"),
        event("e3", "git.clone"),
    ]]);
    let mut cfg = org_config(&provider);
    cfg.filter = Some("action != \"git.clone\"".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r.record["_document_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["e1"]);
}

#[tokio::test]
async fn stringified_json_fields_are_unwrapped_unless_the_deployment_says_not_to() {
    let provider = crate::saas_provider::start().await;
    let stringified = json!({
        "_document_id": "u1",
        "action": "repo.create",
        "payload": "{\"inner\":true,\"n\":[1,2]}",
        "note": "{not json",
    });
    provider.serve(vec![vec![stringified.clone()]]);

    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    outcome.expect("fetch");
    let row = enriched(&rows[0]).row;
    assert_eq!(
        row["payload"],
        json!({"inner": true, "n": [1, 2]}),
        "unwrap_nested_json is on by default"
    );
    assert_eq!(row["note"], "{not json", "a string that is not JSON stays");

    provider.serve(vec![vec![stringified.clone()]]);
    let mut config = config(org_config(&provider));
    config.unwrap_nested_json = false;
    let (outcome, rows) = run(config, None).await;
    outcome.expect("fetch");
    assert_eq!(
        enriched(&rows[0]).row,
        stringified,
        "with unwrap off the record lands as the provider sent it"
    );
}

#[tokio::test]
async fn an_empty_page_with_a_next_link_is_followed() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![
        vec![event("e1", "repo.create")],
        vec![],
        vec![event("e2", "repo.create")],
    ]);
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    outcome.expect("fetch");
    assert_eq!(provider.requests_to("/orgs/acme/audit-log").len(), 3);
    assert_eq!(rows.len(), 2);
}

/// A 5xx the provider keeps answering: the tick fails after the bounded
/// retries, nothing lands, and the scheduler does not advance the window
/// past it.
#[tokio::test]
async fn a_server_error_fails_the_tick_after_retries_and_lands_nothing() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", "repo.create")]]);
    for _ in 0..4 {
        provider.answer_first(500, Some(0));
    }
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(
        provider.requests_to("/orgs/acme/audit-log").len(),
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
    provider.serve(vec![vec![event("e1", "repo.create")]]);
    provider.answer_first(429, Some(0));
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    outcome.expect("tick");
    assert_eq!(
        provider.requests_to("/orgs/acme/audit-log").len(),
        2,
        "429 then 200"
    );
    assert_eq!(rows.len(), 1);
}

/// A 401 or 403 is never retried: the tick fails on the one request.
#[tokio::test]
async fn a_refusal_is_never_retried() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", "repo.create")]]);
    provider.answer_first(403, Some(0));
    let (outcome, rows) = run(config(org_config(&provider)), None).await;
    assert!(rows.is_empty());
    assert_eq!(provider.requests_to("/orgs/acme/audit-log").len(), 1);
    let err = outcome.expect_err("the tick reports the refusal");
    assert!(err.contains("403"), "{err}");
}

/// Two connections of the type poll independently, and each record carries
/// its connection's `_source_fetcher` tag (`<connection>.<unit>`), so the
/// connection is distinguishable on the record.
#[tokio::test]
async fn two_connections_poll_independently() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", "repo.create")]]);
    let mut cfg = org_config(&provider);
    cfg.org = None;
    cfg.token = None;
    cfg.connections = vec![
        GithubConnection {
            id: "gh-acme".into(),
            org: Some("acme".into()),
            token: Some("tok-a".to_string().into()),
            ..GithubConnection::default()
        },
        GithubConnection {
            id: "gh-globex".into(),
            enterprise: Some("globex".into()),
            token: Some("tok-b".to_string().into()),
            ..GithubConnection::default()
        },
    ];
    let config = config(cfg);
    let mut tags = Vec::new();
    for id in ["gh-acme", "gh-globex"] {
        let (outcome, rows) = Box::pin(crate::builtin_run::run(config.clone(), id, None)).await;
        outcome.expect("tick");
        for row in rows {
            tags.push((id.to_string(), enriched(&row).source_fetcher));
        }
    }
    assert_eq!(
        provider.requests_to("/orgs/acme/audit-log")[0].header("authorization"),
        Some("Bearer tok-a")
    );
    assert_eq!(
        provider.requests_to("/enterprises/globex/audit-log")[0].header("authorization"),
        Some("Bearer tok-b")
    );
    assert_eq!(
        tags,
        [
            ("gh-acme".to_string(), "gh-acme.audit_log".to_string()),
            ("gh-globex".to_string(), "gh-globex.audit_log".to_string()),
        ]
    );
}

/// Both scopes set, or neither: the config is refused at validation, naming
/// the block, and nothing is requested.
#[tokio::test]
async fn both_or_neither_scope_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", "repo.create")]]);
    let mut both = org_config(&provider);
    both.enterprise = Some("ent".into());
    let err = config(both).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.github") && err.contains("exactly one"),
        "{err}"
    );

    let mut neither = org_config(&provider);
    neither.org = None;
    let err = config(neither.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.github") && err.contains("enterprise"),
        "{err}"
    );
    let (outcome, rows) = run(config(neither), None).await;
    assert!(outcome.is_err(), "the tick never starts: {outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty(), "nothing was requested");
}

/// A service the profile does not know is refused at validation by name,
/// instead of being skipped with a warning on every tick.
#[tokio::test]
async fn an_unknown_service_name_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    provider.serve(vec![vec![event("e1", "repo.create")]]);
    let mut cfg = org_config(&provider);
    cfg.services = vec![GithubService {
        name: "audit_logs".into(),
        config: HashMap::new(),
    }];
    let err = config(cfg.clone()).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.github") && err.contains("audit_logs"),
        "{err}"
    );
    let (outcome, rows) = run(config(cfg), None).await;
    assert!(outcome.is_err(), "{outcome:?}");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());
}

/// A token missing from both `token` and `credential_secret` is refused at
/// validation, naming both fields.
#[tokio::test]
async fn a_missing_token_is_refused_at_validation() {
    let provider = crate::saas_provider::start().await;
    let mut cfg = org_config(&provider);
    cfg.token = None;
    let err = config(cfg).validate().unwrap_err().to_string();
    assert!(
        err.contains("sources.github") && err.contains("credential_secret"),
        "{err}"
    );
}

#[tokio::test]
async fn the_health_check_probes_the_authenticated_user() {
    let provider = crate::saas_provider::start().await;
    let healthy = health(config(org_config(&provider))).await.expect("health");
    assert!(healthy);
    let seen = provider.requests_to("/user");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].header("authorization"), Some("Bearer tok-github"));
    assert_eq!(
        seen[0].header("accept"),
        Some("application/vnd.github+json")
    );
}
