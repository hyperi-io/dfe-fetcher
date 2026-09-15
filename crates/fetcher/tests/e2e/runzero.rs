// Project:   dfe-fetcher
// File:      crates/fetcher/tests/e2e/runzero.rs
// Purpose:   The shipped runZero profile against live consoles: a dump to Kafka, and the cloud auth path
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The shipped `runzero` profile against real consoles.
//!
//! No runZero-specific Rust exists anywhere in the fetcher; these tests bind
//! the shipped profile to two instances built from `.env-cloud` and prove the
//! framework carries the live contract:
//!
//! - self-hosted, export token as a static bearer: the `assets` store streams
//!   through the real `Driver` into a real Kafka broker and a consumer rebuilds
//!   the snapshot to exactly the row count the export returned;
//! - self-hosted, OAuth2 client with `_oid`: the same store, the same count,
//!   a second identity on the same shape;
//! - cloud, OAuth2 client: the token exchange and the account API succeed and
//!   an inventory export is refused with a 403 that is never retried.
//!
//! The cloud tenant is read-only for these tests: nothing is created, nothing
//! is scanned, and its API client carries no inventory grant on purpose.
//!
//! ```bash
//! cargo nextest run --test e2e -- --ignored runzero
//! ```
//!
//! Variables (written to `.env-cloud` by the infra repo's `gen-test-env`):
//! `RUNZERO_SELFHOSTED_CONSOLE_URL`,
//! `RUNZERO_SELFHOSTED_EXPORT_TOKEN`, `RUNZERO_SELFHOSTED_CLIENT_ID`,
//! `RUNZERO_SELFHOSTED_CLIENT_SECRET`, `RUNZERO_SELFHOSTED_ORG_ID`,
//! `RUNZERO_CLOUD_CONSOLE_URL`, `RUNZERO_CLOUD_CLIENT_ID`,
//! `RUNZERO_CLOUD_CLIENT_SECRET`, `RUNZERO_CLOUD_TOKEN_ENDPOINT` (optional).
//! Credentials reach the profile as `env:` specs, so no test code ever holds
//! a secret in a string it could print.

use crate::common;
use crate::common::{load_env, optional, require, shape_rows};

use std::sync::Arc;

use chrono::Utc;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use dfe_fetcher::config::{Config, OutputConfig, SharedConfig};
use dfe_fetcher::driver::{Driver, DriverParts, Shape};
use dfe_fetcher::emit::Emitter;
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::output::OutputManager;
use dfe_fetcher::pipeline::PipelineState;
use dfe_fetcher_core::RowSource;
use dfe_fetcher_core::envelope::Envelope;
use dfe_fetcher_core::error::Error;
use dfe_fetcher_core::metric_names;
use dfe_fetcher_rest::RestShape;
use dfe_fetcher_rest::profile::{RestInstance, RestProfile, UnitOverride};

/// The metrics recorder these tests read, installed once per process.
///
/// The recorder takes a global slot, so a second install in the same process
/// fails; both tests share this one and each snapshots what it needs.
fn snapshotter() -> &'static Snapshotter {
    static SNAPSHOTTER: std::sync::OnceLock<Snapshotter> = std::sync::OnceLock::new();
    SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder
            .install()
            .expect("the global recorder slot is free");
        snapshotter
    })
}

/// The shipped profile, exactly as an operator's `profile: runzero` binds it.
fn shipped_profile() -> RestProfile {
    dfe_fetcher::profiles::shipped()["runzero"].clone()
}

/// `<console>/api/v1.0`, the base every runZero console serves the API under.
fn api_base(console_var: &str) -> String {
    format!("{}/api/v1.0", require(console_var).trim_end_matches('/'))
}

/// An instance of the shipped profile narrowed to `units`; every other store
/// is switched off so a test exercises one export.
fn instance(yaml: &str, units: &[&str]) -> RestInstance {
    let mut inst: RestInstance =
        serde_yaml_ng::from_str(yaml).unwrap_or_else(|e| panic!("instance yaml: {e}"));
    for endpoint in &shipped_profile().endpoints {
        if !units.contains(&endpoint.unit.as_str()) {
            inst.units.insert(
                endpoint.unit.clone(),
                UnitOverride {
                    enabled: false,
                    ..UnitOverride::default()
                },
            );
        }
    }
    inst
}

fn self_hosted_export_token(units: &[&str]) -> RestInstance {
    instance(
        &format!(
            "profile: runzero\ntopic: runzero\nauth: {{ mode: bearer, token: \"env:RUNZERO_SELFHOSTED_EXPORT_TOKEN\" }}\nvars: {{ base_url: \"{}\" }}\n",
            api_base("RUNZERO_SELFHOSTED_CONSOLE_URL")
        ),
        units,
    )
}

fn self_hosted_oauth2(units: &[&str]) -> RestInstance {
    instance(
        &format!(
            "profile: runzero\ntopic: runzero\nauth: {{ mode: oauth2_client_credentials, client_id: \"{}\", client_secret: \"env:RUNZERO_SELFHOSTED_CLIENT_SECRET\" }}\nvars: {{ base_url: \"{}\", org_id: \"{}\" }}\n",
            require("RUNZERO_SELFHOSTED_CLIENT_ID"),
            api_base("RUNZERO_SELFHOSTED_CONSOLE_URL"),
            require("RUNZERO_SELFHOSTED_ORG_ID"),
        ),
        units,
    )
}

fn cloud_oauth2(org_id: &str, units: &[&str]) -> RestInstance {
    instance(
        &format!(
            "profile: runzero\ntopic: runzero-cloud\nauth: {{ mode: oauth2_client_credentials, client_id: \"{}\", client_secret: \"env:RUNZERO_CLOUD_CLIENT_SECRET\" }}\nvars: {{ base_url: \"{}\", org_id: \"{org_id}\" }}\n",
            require("RUNZERO_CLOUD_CLIENT_ID"),
            api_base("RUNZERO_CLOUD_CONSOLE_URL"),
        ),
        units,
    )
}

fn shape(profile: &RestProfile, inst: &RestInstance, connection_id: &str) -> RestShape {
    RestShape::from_instance(
        profile,
        inst,
        connection_id,
        dfe_fetcher_rest::http_client().expect("http client"),
    )
    .unwrap_or_else(|e| panic!("bind {connection_id}: {e}"))
}

/// The export's own row count, read outside the framework with the export
/// token, so the framework is checked against the API and not against itself.
async fn export_line_count(store: &str) -> usize {
    let token = require("RUNZERO_SELFHOSTED_EXPORT_TOKEN");
    let mut bearer = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
        .expect("token is a header value");
    bearer.set_sensitive(true);
    let body = dfe_fetcher_rest::http_client()
        .expect("http client")
        .get(format!(
            "{}/export/org/{store}.jsonl",
            api_base("RUNZERO_SELFHOSTED_CONSOLE_URL")
        ))
        .header(reqwest::header::AUTHORIZATION, bearer)
        .send()
        .await
        .expect("export request")
        .error_for_status()
        .expect("export answers 2xx")
        .text()
        .await
        .expect("export body");
    body.lines().filter(|l| !l.trim().is_empty()).count()
}

/// One reading of every series the recorder saw. `Snapshotter::snapshot` is
/// destructive (counters and gauges swap to zero, histograms clear), so a test
/// takes one snapshot and queries it.
type Reading = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn reading(snapshotter: &Snapshotter) -> Reading {
    snapshotter.snapshot().into_vec()
}

/// The recorded series with `name` and a `source` label of `source`.
fn series<'a>(reading: &'a Reading, name: &str, source: &str) -> Vec<&'a DebugValue> {
    reading
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "source" && l.value() == source)
        })
        .map(|(_, _, _, value)| value)
        .collect()
}

fn histogram_samples(reading: &Reading, name: &str, source: &str) -> usize {
    series(reading, name, source)
        .into_iter()
        .map(|v| match v {
            DebugValue::Histogram(samples) => samples.len(),
            other => panic!("{name} is a histogram, got {other:?}"),
        })
        .sum()
}

#[tokio::test]
#[ignore = "requires live runZero self-hosted credentials and Kafka"]
async fn self_hosted_assets_dump_reaches_kafka_and_rebuilds_to_the_export_count() {
    load_env();
    let snapshotter = snapshotter();

    let expected = export_line_count("assets").await;
    assert!(expected > 0, "the console has scanned something");

    let (kf, _holder) = common::acquire_kafka("runzero-selfhosted-assets")
        .await
        .expect("a live test needs Kafka: no live broker and no testcontainer");
    let suffix = format!("-{}", Utc::now().timestamp_millis());
    let topic = format!("runzero-assets{suffix}");

    let mut config = Config {
        output: OutputConfig {
            output_type: "kafka".to_string(),
            kafka: Some(kf.to_scalo_config()),
            grpc: None,
            topic_suffix: Some(suffix.clone()),
            ..Default::default()
        },
        ..Default::default()
    };
    config.dlq.enabled = false;
    let shared = SharedConfig::new(config.clone());
    let metrics = Arc::new(Metrics::new());
    let output = OutputManager::new(&config.output, &config.kafka)
        .await
        .unwrap_or_else(|e| panic!("OutputManager init against {}: {e}", kf.brokers));
    let state = Arc::new(
        PipelineState::new(
            shared.clone(),
            Arc::clone(&metrics),
            Some(output),
            CancellationToken::new(),
        )
        .expect("pipeline state"),
    );
    let connection_id = "runzero_self_hosted";
    let driver = Driver::new(DriverParts {
        shape: Shape::Rest(Box::new(shape(
            &shipped_profile(),
            &self_hosted_export_token(&["assets"]),
            connection_id,
        ))),
        connection_id: connection_id.into(),
        instance_id: "e2e".into(),
        shared_config: shared.clone(),
        accumulate: config.accumulate,
        oversize: config.oversize,
        emitter: Emitter::new(
            Arc::clone(&state),
            Arc::clone(&metrics),
            config.accumulate.in_flight,
        ),
        pressure: None,
        memory_guard: Arc::clone(state.memory_guard()),
        checkpoints: None,
        metrics: Arc::clone(&metrics),
        shutdown: CancellationToken::new(),
    });
    assert_eq!(
        driver.units().len(),
        1,
        "every other store is switched off on this instance"
    );

    let report = driver.run_tick(None).await.expect("tick");
    assert_eq!(
        report.rows as usize, expected,
        "the driver emitted every row"
    );
    assert_eq!(report.oversize, 0, "no asset row is over max_record_bytes");
    assert!(report.flushes >= 1);
    eprintln!(
        "runzero self-hosted assets: export lines {expected}, driver rows {}, flushes {}",
        report.rows, report.flushes
    );

    let frames =
        common::consume_frames(&kf, &topic, &format!("runzero-e2e{suffix}"), expected + 2).await;
    assert_eq!(frames.len(), expected + 2, "begin + rows + end");
    let (asm, id) = common::reassemble(&frames);
    let rebuilt = asm
        .complete(id)
        .expect("the consumer rebuilds the whole dump by snapshot_id");
    assert_eq!(
        rebuilt.len(),
        expected,
        "rebuilt rows equal the export count"
    );
    eprintln!("runzero self-hosted assets: rebuilt rows {}", rebuilt.len());
    assert!(
        rebuilt.iter().all(|r| r["id"].is_string()),
        "every asset row carries its row key"
    );
    let landed: Value = serde_json::from_slice(&frames[1]).expect("frame is JSON");
    assert_eq!(landed["kind"], "row");
    assert_eq!(landed["_source"], "runzero-assets");
    assert_eq!(landed["_source_fetcher"], "runzero_self_hosted.assets");
    assert_eq!(landed["store"], "runzero_self_hosted.assets");
    assert_eq!(landed["timestamp"], landed["snapshot_at"]);
    let end: Envelope =
        serde_json::from_slice(frames.last().expect("end frame")).expect("envelope");
    assert_eq!(end.kind(), dfe_fetcher_core::envelope::Kind::End);

    // One page, no retry, and the console's usage counters surfaced.
    let seen = reading(snapshotter);
    assert_eq!(
        histogram_samples(&seen, metric_names::API_DURATION_SECONDS, connection_id),
        1,
        "one export call, never retried"
    );
    assert!(
        series(&seen, metric_names::API_ERRORS_TOTAL, connection_id).is_empty(),
        "no API error on the data path"
    );
    // The console's `x-api-usage-*` headers reach the instance's gauges, and
    // the console counts this very call, so today's reading is at least one.
    for name in ["usage_today", "usage_total"] {
        let usage = series(
            &seen,
            &format!("{}{name}", metric_names::API_QUOTA_PREFIX),
            connection_id,
        );
        match usage.as_slice() {
            [DebugValue::Gauge(v)] => {
                eprintln!(
                    "runzero self-hosted assets: x-api-{name} = {}",
                    v.into_inner()
                );
                assert!(v.into_inner() >= 1.0, "x-api-{name} counts this call");
            }
            other => panic!("x-api-{name} is a gauge on the instance: {other:?}"),
        }
    }
}

#[tokio::test]
#[ignore = "requires live runZero self-hosted credentials"]
async fn self_hosted_oauth2_instance_with_oid_reads_the_same_store_count() {
    load_env();
    let expected = export_line_count("assets").await;
    let oauth = shape(
        &shipped_profile(),
        &self_hosted_oauth2(&["assets"]),
        "runzero_self_hosted_oauth",
    );
    oauth
        .probe()
        .await
        .expect("the self-hosted token exchange succeeds");
    let rows = shape_rows(&oauth, "assets")
        .await
        .expect("an OAuth2 client with inventory:read reads assets with _oid");
    assert_eq!(
        rows.len(),
        expected,
        "the OAuth2 identity sees the same store as the export token"
    );
    eprintln!(
        "runzero self-hosted oauth2 assets: rows {} (export lines {expected})",
        rows.len()
    );
}

#[tokio::test]
#[ignore = "requires live runZero cloud credentials"]
async fn cloud_instance_exchanges_a_token_lists_orgs_and_is_refused_inventory() {
    load_env();
    let snapshotter = snapshotter();

    let base = api_base("RUNZERO_CLOUD_CONSOLE_URL");
    if let Some(recorded) = optional("RUNZERO_CLOUD_TOKEN_ENDPOINT") {
        assert_eq!(
            format!("{base}/account/api/token"),
            recorded,
            "the profile's token_url template derives the recorded endpoint"
        );
    }

    // The account API is not a store the shipped profile carries (an org
    // object holds the org's export and download tokens, which must never
    // reach a topic), so the probe is a test-only inline profile on the same
    // grammar and the same OAuth2 identity.
    let account: RestProfile = serde_yaml_ng::from_str(
        r#"
profile: runzero_account_probe
base_url: "{{ vars.base_url }}"
shape: dump
auth:
  accepts: [oauth2_client_credentials]
  oauth2_client_credentials: { token_url: "{{ base_url }}/account/api/token", expires_in_fallback_secs: 1800 }
retry: { never_retry: [401, 403] }
error: { at: "/error" }
endpoints:
  - { unit: orgs, path: /account/orgs, rows: { decoder: json_array }, row_key: "/id" }
"#,
    )
    .expect("probe profile parses");
    let account_instance: RestInstance = serde_yaml_ng::from_str(&format!(
        "profile: x\ntopic: runzero-cloud\nauth: {{ mode: oauth2_client_credentials, client_id: \"{}\", client_secret: \"env:RUNZERO_CLOUD_CLIENT_SECRET\" }}\nvars: {{ base_url: \"{base}\" }}\n",
        require("RUNZERO_CLOUD_CLIENT_ID"),
    ))
    .expect("probe instance parses");
    let account_shape = shape(&account, &account_instance, "runzero_cloud_account");
    account_shape
        .probe()
        .await
        .expect("the cloud token exchange succeeds");
    let orgs = shape_rows(&account_shape, "orgs")
        .await
        .expect("GET /account/orgs succeeds for the account-level client");
    assert!(
        !orgs.is_empty(),
        "the account has at least one organisation"
    );
    // Only the count and the id leave this scope: the org object carries
    // credentials.
    let org_id = orgs[0]["id"]
        .as_str()
        .expect("an org has a string id")
        .to_owned();
    eprintln!("runzero cloud: token exchange ok, orgs {}", orgs.len());

    let cloud = shape(
        &shipped_profile(),
        &cloud_oauth2(&org_id, &["assets"]),
        "runzero_cloud",
    );
    let refused = shape_rows(&cloud, "assets")
        .await
        .expect_err("the cloud client has no inventory grant");
    match &refused {
        Error::Api { status, text, .. } => {
            assert_eq!(*status, 403, "refused, not a 400 for a missing _oid");
            assert!(!text.is_empty(), "error.at read the refusal text");
            eprintln!("runzero cloud: export refused with status {status}");
        }
        other => panic!("expected Api 403, got {other}"),
    }
    let seen = reading(snapshotter);
    assert_eq!(
        histogram_samples(&seen, metric_names::API_DURATION_SECONDS, "runzero_cloud"),
        1,
        "a refusal is sent once and never retried"
    );
    let errors = series(&seen, metric_names::API_ERRORS_TOTAL, "runzero_cloud");
    assert!(
        matches!(errors.as_slice(), [DebugValue::Counter(1)]),
        "one 4xx counted: {errors:?}"
    );
}
