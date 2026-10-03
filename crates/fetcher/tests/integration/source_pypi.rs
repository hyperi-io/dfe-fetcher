// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_pypi.rs
// Purpose:   Characterisation of the PyPI supply-chain source: one document per package, tagged
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The PyPI package-metadata source against the in-test provider.
//!
//! Each test configures the typed `sources.pypi` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the requests the provider recorded (one per configured package) and the
//! records that landed (the provider's document with the queried package
//! name stamped on it, plus what enrichment added). The typed config block
//! is the operator's contract; the shipped `pypi` profile serves it through
//! the framework driver.

use serde_json::{Value, json};

use dfe_fetcher::config::{Config, PypiSourceConfig};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::saas_provider::Provider;

/// A deployment config carrying `pypi` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(pypi: PypiSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.pypi = pypi;
    config
}

/// The typed block an operator writes: the packages to watch, pointed at
/// the provider.
fn packages_config(provider: &Provider, packages: &[&str]) -> PypiSourceConfig {
    PypiSourceConfig {
        enabled: true,
        packages: packages.iter().map(|p| (*p).to_owned()).collect(),
        api_url_override: Some(provider.base_url()),
        ..PypiSourceConfig::default()
    }
}

/// One tick of the `pypi` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "pypi", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "pypi")).await
}

/// A PyPI JSON document: `info`, `releases`, `urls` as the registry shapes
/// them, trimmed to what the assertions read.
fn document(name: &str, version: &str) -> Value {
    json!({
        "info": {
            "name": name,
            "version": version,
            "summary": format!("{name} does things"),
            "yanked": false,
            "project_urls": { "Homepage": format!("https://example.com/{name}") }
        },
        "releases": { version: [ { "filename": format!("{name}-{version}.tar.gz"), "digests": { "sha256": "00" } } ] },
        "urls": [],
        "last_serial": 42
    })
}

/// The document the source lands: the provider's, plus the package it asked
/// for stamped on the top level.
fn stamped(name: &str, version: &str) -> Value {
    let mut doc = document(name, version);
    doc["_dfe_fetcher_package"] = Value::String(name.to_owned());
    doc
}

#[tokio::test]
async fn each_package_is_one_request_and_one_stamped_document() {
    let provider = crate::saas_provider::start().await;
    provider.document("requests", document("requests", "2.32.0"));
    provider.document("scalo", document("scalo", "0.9.1"));

    let (outcome, rows) = run(
        config(packages_config(&provider, &["requests", "scalo"])),
        None,
    )
    .await;
    outcome.expect("fetch");

    let seen = provider.requests();
    let paths: Vec<&str> = seen.iter().map(|s| s.path.as_str()).collect();
    assert_eq!(
        paths,
        ["/pypi/requests/json", "/pypi/scalo/json"],
        "one request per package, in the configured order"
    );
    for request in &seen {
        assert_eq!(request.header("accept"), Some("application/json"));
        assert!(request.header("authorization").is_none(), "no credential");
        assert_eq!(
            request.query,
            [] as [(std::string::String, std::string::String); 0]
        );
    }

    assert_eq!(rows.len(), 2);
    for (row, expected) in rows
        .iter()
        .zip([stamped("requests", "2.32.0"), stamped("scalo", "0.9.1")])
    {
        assert_eq!(row.topic, "pypi_land");
        let e = enriched(row);
        assert_eq!(
            e.row, expected,
            "the document with `_dfe_fetcher_package` stamped on it"
        );
        assert_eq!(e.source, "pypi");
        assert_eq!(e.source_fetcher, "pypi.metadata");
    }
}

/// The scheduler's window is not part of the request: a package document
/// is the current state, whatever the tick's window.
#[tokio::test]
async fn the_window_does_not_reach_the_request() {
    let provider = crate::saas_provider::start().await;
    provider.document("requests", document("requests", "2.32.0"));
    let window = FetchWindow {
        start: chrono::Utc::now() - chrono::Duration::hours(2),
        end: chrono::Utc::now(),
    };
    let (outcome, rows) = run(
        config(packages_config(&provider, &["requests"])),
        Some(&window),
    )
    .await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        provider.requests()[0].query,
        [] as [(std::string::String, std::string::String); 0]
    );
}

/// A package the registry does not know (404): skipped, the others still
/// land, the tick is Ok.
#[tokio::test]
async fn a_missing_package_is_skipped_and_the_others_land() {
    let provider = crate::saas_provider::start().await;
    provider.document("requests", document("requests", "2.32.0"));
    let (outcome, rows) = run(
        config(packages_config(&provider, &["gone", "requests"])),
        None,
    )
    .await;
    outcome.expect("a 404 is not a failure");
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].record["_dfe_fetcher_package"], "requests");
}

/// A transient 5xx on one package is retried and the document then lands
/// with the others; a 5xx the registry keeps answering fails the tick after
/// the bounded retries, and the packages after it wait for the next tick.
#[tokio::test]
async fn a_server_error_is_retried_and_a_persistent_one_fails_the_tick() {
    let provider = crate::saas_provider::start().await;
    provider.document("first", document("first", "1.0.0"));
    provider.document("second", document("second", "2.0.0"));
    provider.answer_first(500, Some(0));
    let (outcome, rows) = run(
        config(packages_config(&provider, &["first", "second"])),
        None,
    )
    .await;
    outcome.expect("one 500 is retried");
    assert_eq!(
        provider.requests_to("/pypi/first/json").len(),
        2,
        "500 then 200"
    );
    let landed: Vec<&str> = rows
        .iter()
        .map(|r| r.record["_dfe_fetcher_package"].as_str().unwrap())
        .collect();
    assert_eq!(landed, ["first", "second"]);

    let provider = crate::saas_provider::start().await;
    provider.document("first", document("first", "1.0.0"));
    provider.document("second", document("second", "2.0.0"));
    for _ in 0..4 {
        provider.answer_first(500, Some(0));
    }
    let (outcome, rows) = run(
        config(packages_config(&provider, &["first", "second"])),
        None,
    )
    .await;
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
    assert_eq!(
        provider.requests_to("/pypi/first/json").len(),
        4,
        "the first attempt and three retries"
    );
    assert!(
        provider.requests_to("/pypi/second/json").is_empty(),
        "the tick stops at the failure; nothing is lost, the next tick re-asks"
    );
    assert!(rows.is_empty());
}

/// A 429 with `Retry-After` is retried after the wait, and the document
/// then lands.
#[tokio::test]
async fn a_429_with_retry_after_is_retried() {
    let provider = crate::saas_provider::start().await;
    provider.document("requests", document("requests", "2.32.0"));
    provider.answer_first(429, Some(0));
    let (outcome, rows) = run(config(packages_config(&provider, &["requests"])), None).await;
    outcome.expect("tick");
    assert_eq!(provider.requests().len(), 2, "429 then 200");
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let provider = crate::saas_provider::start().await;
    provider.document("requests", document("requests", "2.32.0"));
    provider.document("scalo", document("scalo", "0.9.1"));
    let mut cfg = packages_config(&provider, &["requests", "scalo"]);
    cfg.filter = Some("info.name == \"scalo\"".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].record["_dfe_fetcher_package"], "scalo");
}

/// The stamped package name is visible to the filter, so a deployment can
/// route on what it asked for rather than on `info.name`.
#[tokio::test]
async fn the_filter_sees_the_stamped_package_name() {
    let provider = crate::saas_provider::start().await;
    provider.document("requests", document("requests", "2.32.0"));
    provider.document("scalo", document("scalo", "0.9.1"));
    let mut cfg = packages_config(&provider, &["requests", "scalo"]);
    cfg.filter = Some("_dfe_fetcher_package == \"requests\"".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].record["info"]["name"], "requests");
}

/// No packages: nothing is requested and the tick is Ok.
#[tokio::test]
async fn no_packages_requests_nothing() {
    let provider = crate::saas_provider::start().await;
    let (outcome, rows) = run(config(packages_config(&provider, &[])), None).await;
    outcome.expect("nothing to do is not a failure");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn the_health_check_reaches_the_registry_root() {
    let provider = crate::saas_provider::start().await;
    let healthy = health(config(packages_config(&provider, &["requests"])))
        .await
        .expect("health");
    assert!(healthy);
    let seen = provider.requests_to("/");
    assert_eq!(seen.len(), 1);
    assert!(seen[0].header("authorization").is_none());
}
