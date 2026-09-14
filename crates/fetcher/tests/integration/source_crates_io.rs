// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_crates_io.rs
// Purpose:   Characterisation of the crates.io supply-chain source: one document per crate, tagged
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The crates.io crate-metadata source against the in-test provider.
//!
//! Same shape as the PyPI module with what differs pinned: the path, the
//! `User-Agent` crates.io's policy asks for, the `_dfe_fetcher_crate` stamp
//! and the summary endpoint as the probe. The typed config block is the
//! operator's contract; the shipped `crates_io` profile serves it through
//! the framework driver.

use serde_json::{Value, json};

use dfe_fetcher::config::{Config, CratesIoSourceConfig};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::saas_provider::Provider;

fn config(crates_io: CratesIoSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.crates_io = crates_io;
    config
}

fn crates_config(provider: &Provider, crates: &[&str]) -> CratesIoSourceConfig {
    CratesIoSourceConfig {
        enabled: true,
        crates: crates.iter().map(|c| (*c).to_owned()).collect(),
        api_url_override: Some(provider.base_url()),
        ..CratesIoSourceConfig::default()
    }
}

/// One tick of the `crates_io` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "crates_io", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "crates_io")).await
}

/// A crates.io crate document, trimmed to what the assertions read.
fn document(name: &str, version: &str) -> Value {
    json!({
        "crate": { "id": name, "name": name, "max_version": version, "downloads": 12 },
        "versions": [ { "num": version, "yanked": false, "crate": name } ],
        "keywords": [],
        "categories": []
    })
}

fn stamped(name: &str, version: &str) -> Value {
    let mut doc = document(name, version);
    doc["_dfe_fetcher_crate"] = Value::String(name.to_owned());
    doc
}

#[tokio::test]
async fn each_crate_is_one_request_with_a_contact_user_agent_and_one_stamped_document() {
    let provider = crate::saas_provider::start().await;
    provider.document("serde", document("serde", "1.0.219"));
    provider.document("dfe-fetcher", document("dfe-fetcher", "1.4.21"));

    let (outcome, rows) = run(
        config(crates_config(&provider, &["serde", "dfe-fetcher"])),
        None,
    )
    .await;
    outcome.expect("fetch");

    let seen = provider.requests();
    let paths: Vec<&str> = seen.iter().map(|s| s.path.as_str()).collect();
    assert_eq!(
        paths,
        ["/api/v1/crates/serde", "/api/v1/crates/dfe-fetcher"],
        "one request per crate, in the configured order"
    );
    for request in &seen {
        assert_eq!(request.header("accept"), Some("application/json"));
        assert_eq!(
            request.header("user-agent"),
            Some("dfe-fetcher (https://github.com/hyperi-io/dfe-fetcher)"),
            "crates.io asks consumers to identify themselves with a contact"
        );
        assert!(request.header("authorization").is_none(), "no credential");
    }

    assert_eq!(rows.len(), 2);
    for (row, expected) in rows.iter().zip([
        stamped("serde", "1.0.219"),
        stamped("dfe-fetcher", "1.4.21"),
    ]) {
        assert_eq!(row.topic, "crates_io_land");
        let e = enriched(row);
        assert_eq!(
            e.row, expected,
            "the document with `_dfe_fetcher_crate` stamped on it"
        );
        assert_eq!(e.source, "crates_io");
        assert_eq!(e.source_fetcher, "crates_io.metadata");
    }
}

/// A crate the registry does not know (404): skipped, the others still
/// land, the tick is Ok.
#[tokio::test]
async fn a_missing_crate_is_skipped_and_the_others_land() {
    let provider = crate::saas_provider::start().await;
    provider.document("serde", document("serde", "1.0.219"));
    let (outcome, rows) = run(config(crates_config(&provider, &["gone", "serde"])), None).await;
    outcome.expect("a 404 is not a failure");
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].record["_dfe_fetcher_crate"], "serde");
}

#[tokio::test]
async fn the_filter_sees_the_stamped_crate_name() {
    let provider = crate::saas_provider::start().await;
    provider.document("serde", document("serde", "1.0.219"));
    provider.document("tokio", document("tokio", "1.53.1"));
    let mut cfg = crates_config(&provider, &["serde", "tokio"]);
    cfg.filter = Some("_dfe_fetcher_crate == \"tokio\"".into());

    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].record["crate"]["name"], "tokio");
}

#[tokio::test]
async fn no_crates_requests_nothing() {
    let provider = crate::saas_provider::start().await;
    let (outcome, rows) = run(config(crates_config(&provider, &[])), None).await;
    outcome.expect("nothing to do is not a failure");
    assert!(rows.is_empty());
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn the_health_check_reads_the_registry_summary_with_the_user_agent() {
    let provider = crate::saas_provider::start().await;
    let healthy = health(config(crates_config(&provider, &["serde"])))
        .await
        .expect("health");
    assert!(healthy);
    let seen = provider.requests_to("/api/v1/summary");
    assert_eq!(seen.len(), 1);
    assert_eq!(
        seen[0].header("user-agent"),
        Some("dfe-fetcher (https://github.com/hyperi-io/dfe-fetcher)")
    );
}
