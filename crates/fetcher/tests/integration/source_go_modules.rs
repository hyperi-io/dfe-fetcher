// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_go_modules.rs
// Purpose:   Characterisation of the Go module-proxy source: one row per module folding its versions' info documents
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Go module-proxy source against wiremock.
//!
//! Each test configures the typed `sources.go_modules` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the requests wiremock recorded (the version list of each module as text
//! lines, then one `.info` request per version in list order) and the
//! records that landed (one per module: the version list, each version's
//! document keyed by version, the module stamped on the row, plus what
//! enrichment added). The typed config block is the operator's contract;
//! the shipped `go_modules` profile serves it through the framework driver.

use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use dfe_fetcher::config::{Config, GoModulesSourceConfig};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};

/// A deployment config carrying `go_modules` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(go_modules: GoModulesSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.go_modules = go_modules;
    config
}

/// The typed block an operator writes: the modules to watch, pointed at
/// wiremock.
fn modules_config(server: &MockServer, modules: &[&str]) -> GoModulesSourceConfig {
    GoModulesSourceConfig {
        enabled: true,
        modules: modules.iter().map(|m| (*m).to_owned()).collect(),
        api_url_override: Some(server.uri()),
        ..GoModulesSourceConfig::default()
    }
}

/// One tick of the `go_modules` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "go_modules", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "go_modules")).await
}

/// The proxy's version list for a module: one version per line.
async fn mount_list(server: &MockServer, module: &str, versions: &[&str]) {
    let mut body = versions.join("\n");
    body.push('\n');
    Mock::given(method("GET"))
        .and(path(format!("/{module}/@v/list")))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

/// The proxy's `.info` document for one version.
fn info(version: &str) -> Value {
    json!({
        "Version": version,
        "Time": "2026-05-21T10:00:00Z",
        "Origin": {"VCS": "git", "URL": "https://go.googlesource.com/text", "Ref": format!("refs/tags/{version}"), "Hash": "abc123"}
    })
}

async fn mount_info(server: &MockServer, module: &str, version: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/{module}/@v/{version}.info")))
        .respond_with(ResponseTemplate::new(200).set_body_json(info(version)))
        .mount(server)
        .await;
}

/// A status answered for a path; a retried one says `Retry-After: 0` so
/// the retries do not wait.
async fn mount_status(server: &MockServer, at: &str, status: u16) {
    Mock::given(method("GET"))
        .and(path(at))
        .respond_with(
            ResponseTemplate::new(status)
                .set_body_string("scripted refusal")
                .insert_header("Retry-After", "0"),
        )
        .mount(server)
        .await;
}

/// The requests wiremock saw, as paths in order.
async fn paths(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .map(|r| r.url.path().to_owned())
        .collect()
}

async fn requests_to(server: &MockServer, at: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.url.path() == at)
        .collect()
}

fn header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request.headers.get(name).and_then(|v| v.to_str().ok())
}

/// The row the source lands for a module whose versions all answered.
fn module_row(module: &str, versions: &[&str]) -> Value {
    let mut infos = serde_json::Map::new();
    for version in versions {
        infos.insert((*version).to_owned(), info(version));
    }
    json!({
        "_dfe_fetcher_module": module,
        "versions": versions,
        "version_info": infos,
    })
}

#[tokio::test]
async fn each_module_is_one_row_folding_its_versions_info_documents() {
    let server = MockServer::start().await;
    mount_list(&server, "golang.org/x/text", &["v0.3.0", "v0.4.0"]).await;
    mount_info(&server, "golang.org/x/text", "v0.3.0").await;
    mount_info(&server, "golang.org/x/text", "v0.4.0").await;
    mount_list(&server, "github.com/hyperi-io/dfe", &["v1.0.0"]).await;
    mount_info(&server, "github.com/hyperi-io/dfe", "v1.0.0").await;

    let (outcome, rows) = run(
        config(modules_config(
            &server,
            &["golang.org/x/text", "github.com/hyperi-io/dfe"],
        )),
        None,
    )
    .await;
    outcome.expect("fetch");

    assert_eq!(
        paths(&server).await,
        [
            "/golang.org/x/text/@v/list",
            "/golang.org/x/text/@v/v0.3.0.info",
            "/golang.org/x/text/@v/v0.4.0.info",
            "/github.com/hyperi-io/dfe/@v/list",
            "/github.com/hyperi-io/dfe/@v/v1.0.0.info",
        ],
        "each module's list, then its versions in list order, then the next module"
    );
    for request in server.received_requests().await.unwrap_or_default() {
        assert_eq!(header(&request, "accept"), Some("application/json"));
        assert!(
            header(&request, "authorization").is_none(),
            "a public proxy"
        );
    }

    assert_eq!(rows.len(), 2, "one row per module: {rows:?}");
    for (row, expected) in rows.iter().zip([
        module_row("golang.org/x/text", &["v0.3.0", "v0.4.0"]),
        module_row("github.com/hyperi-io/dfe", &["v1.0.0"]),
    ]) {
        assert_eq!(row.topic, "go_modules_land");
        let e = enriched(row);
        assert_eq!(
            e.row, expected,
            "the version list and every version's document"
        );
        assert_eq!(e.source, "go_modules");
        assert_eq!(e.source_fetcher, "go_modules.metadata");
    }
}

/// The scheduler's window plays no part: a module's versions are its
/// current state.
#[tokio::test]
async fn the_window_does_not_reach_the_request() {
    let server = MockServer::start().await;
    mount_list(&server, "golang.org/x/text", &["v0.3.0"]).await;
    mount_info(&server, "golang.org/x/text", "v0.3.0").await;
    let window = FetchWindow {
        start: chrono::Utc::now() - chrono::Duration::hours(2),
        end: chrono::Utc::now(),
    };
    let (outcome, rows) = run(
        config(modules_config(&server, &["golang.org/x/text"])),
        Some(&window),
    )
    .await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    for request in server.received_requests().await.unwrap_or_default() {
        assert!(request.url.query().is_none(), "{}", request.url);
    }
}

/// A module the proxy does not know (404 on its list) yields no row and
/// the others still land.
#[tokio::test]
async fn an_unpublished_module_yields_no_row_and_the_others_land() {
    let server = MockServer::start().await;
    mount_status(&server, "/example.com/unpublished/@v/list", 404).await;
    mount_list(&server, "golang.org/x/text", &["v0.3.0"]).await;
    mount_info(&server, "golang.org/x/text", "v0.3.0").await;
    let (outcome, rows) = run(
        config(modules_config(
            &server,
            &["example.com/unpublished", "golang.org/x/text"],
        )),
        None,
    )
    .await;
    outcome.expect("a 404 on a list is not a failure");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].record["_dfe_fetcher_module"], "golang.org/x/text");
    assert_eq!(
        paths(&server).await,
        [
            "/example.com/unpublished/@v/list",
            "/golang.org/x/text/@v/list",
            "/golang.org/x/text/@v/v0.3.0.info",
        ]
    );
}

/// A version whose `.info` the proxy answers 404 for (retracted, or a
/// proxy gap) is left out of the row: the fold holds the documents that
/// answered, so the version is in neither the list nor the map.
#[tokio::test]
async fn a_retracted_version_is_left_out_of_the_row() {
    let server = MockServer::start().await;
    mount_list(
        &server,
        "golang.org/x/text",
        &["v0.3.0", "v0.4.0", "v0.5.0"],
    )
    .await;
    mount_info(&server, "golang.org/x/text", "v0.3.0").await;
    mount_status(&server, "/golang.org/x/text/@v/v0.4.0.info", 404).await;
    mount_info(&server, "golang.org/x/text", "v0.5.0").await;
    let (outcome, rows) = run(
        config(modules_config(&server, &["golang.org/x/text"])),
        None,
    )
    .await;
    outcome.expect("a 404 on a version is not a failure");
    assert_eq!(rows.len(), 1);
    let row = enriched(&rows[0]).row;
    assert_eq!(row["versions"], json!(["v0.3.0", "v0.5.0"]));
    assert!(row["version_info"].get("v0.4.0").is_none());
    assert_eq!(row["version_info"]["v0.5.0"]["Version"], "v0.5.0");
    assert_eq!(
        requests_to(&server, "/golang.org/x/text/@v/v0.4.0.info")
            .await
            .len(),
        1,
        "a 404 is never retried"
    );
}

/// The per-module version cap: at most 100 documents are fetched, in list
/// order, and the row holds those hundred.
#[tokio::test]
async fn at_most_100_versions_are_fetched_per_module() {
    let server = MockServer::start().await;
    let versions: Vec<String> = (0..120).map(|i| format!("v1.{i}.0")).collect();
    let refs: Vec<&str> = versions.iter().map(String::as_str).collect();
    mount_list(&server, "golang.org/x/text", &refs).await;
    for version in &refs {
        mount_info(&server, "golang.org/x/text", version).await;
    }
    let (outcome, rows) = run(
        config(modules_config(&server, &["golang.org/x/text"])),
        None,
    )
    .await;
    outcome.expect("fetch");
    let infos = paths(&server)
        .await
        .into_iter()
        .filter(|p| p.contains("/@v/") && !p.ends_with("/list"))
        .count();
    assert_eq!(infos, 100, "the cap on documents fetched");
    assert_eq!(
        paths(&server).await[1],
        "/golang.org/x/text/@v/v1.0.0.info",
        "the first hundred, in list order"
    );
    let row = enriched(&rows[0]).row;
    assert_eq!(row["versions"].as_array().unwrap().len(), 100);
    assert_eq!(row["version_info"].as_object().unwrap().len(), 100);
}

/// Blank lines in the version list are skipped, so a list with a trailing
/// blank or spacing yields only the versions.
#[tokio::test]
async fn blank_lines_in_the_version_list_are_skipped() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/golang.org/x/text/@v/list"))
        .respond_with(ResponseTemplate::new(200).set_body_string("v0.3.0\n\n  v0.4.0  \n\n"))
        .mount(&server)
        .await;
    mount_info(&server, "golang.org/x/text", "v0.3.0").await;
    mount_info(&server, "golang.org/x/text", "v0.4.0").await;
    let (outcome, rows) = run(
        config(modules_config(&server, &["golang.org/x/text"])),
        None,
    )
    .await;
    outcome.expect("fetch");
    assert_eq!(
        enriched(&rows[0]).row["versions"],
        json!(["v0.3.0", "v0.4.0"])
    );
}

/// A `.info` the proxy keeps failing is retried per the policy and then
/// fails the tick: no half-folded module lands, and the next tick asks
/// again.
#[tokio::test]
async fn a_failing_info_is_retried_and_then_fails_the_tick() {
    let server = MockServer::start().await;
    mount_list(&server, "golang.org/x/text", &["v0.3.0", "v0.4.0"]).await;
    mount_info(&server, "golang.org/x/text", "v0.3.0").await;
    mount_status(&server, "/golang.org/x/text/@v/v0.4.0.info", 500).await;
    let (outcome, rows) = run(
        config(modules_config(&server, &["golang.org/x/text"])),
        None,
    )
    .await;
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
    assert!(rows.is_empty(), "no partial module row: {rows:?}");
    assert_eq!(
        requests_to(&server, "/golang.org/x/text/@v/v0.4.0.info")
            .await
            .len(),
        4,
        "the first attempt and three retries"
    );
}

/// A list the proxy keeps failing is retried and then fails the tick: what
/// the tick still held is not emitted, the modules after it are not asked,
/// and the next tick re-asks for everything, so nothing is lost.
#[tokio::test]
async fn a_failing_list_is_retried_and_then_fails_the_tick() {
    let server = MockServer::start().await;
    mount_list(&server, "github.com/hyperi-io/dfe", &["v1.0.0"]).await;
    mount_info(&server, "github.com/hyperi-io/dfe", "v1.0.0").await;
    mount_status(&server, "/example.com/broken/@v/list", 500).await;
    mount_list(&server, "golang.org/x/text", &["v0.3.0"]).await;
    mount_info(&server, "golang.org/x/text", "v0.3.0").await;
    let (outcome, rows) = run(
        config(modules_config(
            &server,
            &[
                "github.com/hyperi-io/dfe",
                "example.com/broken",
                "golang.org/x/text",
            ],
        )),
        None,
    )
    .await;
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
    assert!(
        rows.is_empty(),
        "a failed tick emits nothing it still held; the next tick re-asks: {rows:?}"
    );
    assert_eq!(
        requests_to(&server, "/github.com/hyperi-io/dfe/@v/v1.0.0.info")
            .await
            .len(),
        1,
        "the module before the failure was fetched"
    );
    assert_eq!(
        requests_to(&server, "/example.com/broken/@v/list")
            .await
            .len(),
        4,
        "the first attempt and three retries"
    );
    assert!(
        requests_to(&server, "/golang.org/x/text/@v/list")
            .await
            .is_empty(),
        "the tick stops at the failure; the next tick re-asks"
    );
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let server = MockServer::start().await;
    mount_list(&server, "golang.org/x/text", &["v0.3.0"]).await;
    mount_info(&server, "golang.org/x/text", "v0.3.0").await;
    mount_list(&server, "github.com/hyperi-io/dfe", &["v1.0.0"]).await;
    mount_info(&server, "github.com/hyperi-io/dfe", "v1.0.0").await;
    let mut cfg = modules_config(&server, &["golang.org/x/text", "github.com/hyperi-io/dfe"]);
    cfg.filter = Some("_dfe_fetcher_module == \"github.com/hyperi-io/dfe\"".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].record["versions"], json!(["v1.0.0"]));
}

/// No modules: nothing is requested and the tick is Ok.
#[tokio::test]
async fn no_modules_requests_nothing() {
    let server = MockServer::start().await;
    let (outcome, rows) = run(config(modules_config(&server, &[])), None).await;
    outcome.expect("nothing to do is not a failure");
    assert!(rows.is_empty());
    assert!(paths(&server).await.is_empty());
}

/// The health check asks the proxy's root and any answer but a 5xx is
/// healthy.
#[tokio::test]
async fn the_health_check_reaches_the_proxy_root() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>proxy</html>"))
        .mount(&server)
        .await;
    let healthy = health(config(modules_config(&server, &["golang.org/x/text"])))
        .await
        .expect("health");
    assert!(healthy);
    assert_eq!(paths(&server).await, ["/"]);
}
