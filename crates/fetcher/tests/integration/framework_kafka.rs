// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/framework_kafka.rs
// Purpose:   A declarative REST dump through the batcher to real Kafka, rebuilt by snapshot_id
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The framework end to end on real Kafka.
//!
//! An in-test HTTP provider (axum on 127.0.0.1:0) serves an NDJSON store; an
//! inline profile in the grammar describes it as a dump; a `Driver` runs it
//! through the batcher to a Kafka broker (live if reachable, else a
//! testcontainer named per test); a consumer reads the topic back and rebuilds
//! the snapshot by `snapshot_id`, refuses the same frames with one row
//! missing, and sees a second dump, taken after the provider lost a row,
//! rebuild without it.

use crate::common;
use crate::common::{consume_frames as consume, reassemble as snapshot_of};

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::routing::get;
use chrono::Utc;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use dfe_fetcher::config::{Config, OutputConfig, SharedConfig};
use dfe_fetcher::driver::{Driver, DriverParts, Shape};
use dfe_fetcher::emit::Emitter;
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::output::OutputManager;
use dfe_fetcher::pipeline::PipelineState;
use dfe_fetcher_core::envelope::{Envelope, Kind, Status};
use dfe_fetcher_rest::RestShape;
use dfe_fetcher_rest::profile::{RestInstance, RestProfile};

type Store = Arc<Mutex<Vec<Value>>>;

async fn assets(State(store): State<Store>) -> String {
    store
        .lock()
        .unwrap()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

/// Serve `store` as `/export/org/assets.jsonl` on an ephemeral port.
async fn provider(store: Store) -> String {
    let app = Router::new()
        .route("/export/org/assets.jsonl", get(assets))
        .with_state(store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn profile() -> RestProfile {
    serde_yaml_ng::from_str(
        r#"
profile: fixture_dump
base_url: "{{ vars.base_url }}"
shape: dump
auth: { accepts: [bearer] }
retry: { never_retry: [401, 403] }
endpoints:
  - { unit: assets, path: /export/org/assets.jsonl, rows: { decoder: ndjson }, row_key: "/id" }
"#,
    )
    .unwrap()
}

fn instance(base_url: &str) -> RestInstance {
    serde_yaml_ng::from_str(&format!(
        "profile: fixture_dump\ntopic: fixture\nauth: {{ mode: bearer, token: test-token }}\nvars: {{ base_url: \"{base_url}\" }}\n"
    ))
    .unwrap()
}

#[tokio::test]
async fn a_rest_dump_reaches_kafka_and_a_consumer_rebuilds_it_by_snapshot_id() {
    let Some((kf, _holder)) = common::acquire_kafka("framework-kafka-snapshot-roundtrip").await
    else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let store: Store = Arc::new(Mutex::new(
        (0..15)
            .map(|i| serde_json::json!({"id": format!("asset-{i}"), "alive": i % 2 == 0}))
            .collect(),
    ));
    let base_url = provider(Arc::clone(&store)).await;
    let suffix = format!("-{}", Utc::now().timestamp_millis());
    let topic = format!("fixture-assets{suffix}");

    let mut config = Config::default();
    config.output = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_scalo_config()),
        grpc: None,
        topic_suffix: Some(suffix.clone()),
        ..Default::default()
    };
    config.dlq.enabled = false;
    config.accumulate.max_rows = 4;
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
    let shape = RestShape::from_instance(
        &profile(),
        &instance(&base_url),
        "fixture_dump",
        reqwest::Client::new(),
        &dfe_fetcher_rest::exchange_client().expect("exchange client"),
    )
    .expect("bind");
    let build_driver = |shape: RestShape| {
        Driver::new(DriverParts {
            shape: Shape::Rest(Box::new(shape)),
            connection_id: "fixture_dump".into(),
            instance_id: "inst".into(),
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
        })
    };
    let driver = build_driver(shape);

    let report = driver.run_tick(None).await.expect("first tick");
    assert_eq!(report.rows, 15);
    assert!(
        report.flushes >= 4,
        "max_rows 4 over 17 frames flushes at least four times, got {}",
        report.flushes
    );

    let frames = consume(&kf, &topic, &format!("framework-first{suffix}"), 17).await;
    assert_eq!(frames.len(), 17, "begin + 15 rows + end");
    let (asm, first_id) = snapshot_of(&frames);
    let rows = asm
        .complete(first_id)
        .expect("the consumer rebuilds the whole dump by snapshot_id");
    assert_eq!(rows.len(), 15);
    assert!(rows.iter().any(|r| r["id"] == "asset-7"));
    let landed: Value = serde_json::from_slice(&frames[1]).unwrap();
    assert_eq!(landed["_source"], "fixture-assets");
    assert_eq!(landed["_source_fetcher"], "fixture_dump.assets");
    assert_eq!(landed["store"], "fixture_dump.assets");

    let mut truncated: Vec<Vec<u8>> = frames.clone();
    let dropped = truncated
        .iter()
        .position(|f| serde_json::from_slice::<Envelope>(f).unwrap().kind() == Kind::Row)
        .unwrap();
    truncated.remove(dropped);
    let (asm, id) = snapshot_of(&truncated);
    assert!(
        asm.complete(id).is_none(),
        "one row short of end.row_count is not a snapshot"
    );
    assert_eq!(
        asm.status(id),
        Status::Truncated {
            expected: 15,
            seen: 14
        }
    );

    store.lock().unwrap().retain(|row| row["id"] != "asset-7");
    let second = build_driver(
        RestShape::from_instance(
            &profile(),
            &instance(&base_url),
            "fixture_dump",
            reqwest::Client::new(),
            &dfe_fetcher_rest::exchange_client().expect("exchange client"),
        )
        .unwrap(),
    );
    let report = second.run_tick(None).await.expect("second tick");
    assert_eq!(report.rows, 14);

    let all = consume(&kf, &topic, &format!("framework-second{suffix}"), 17 + 16).await;
    assert_eq!(all.len(), 33, "both dumps are on the topic");
    let second_frames: Vec<Vec<u8>> = all
        .iter()
        .filter(|f| {
            serde_json::from_slice::<Envelope>(f)
                .unwrap()
                .head()
                .snapshot_id
                != first_id
        })
        .cloned()
        .collect();
    assert_eq!(second_frames.len(), 16);
    let (asm, second_id) = snapshot_of(&second_frames);
    assert_ne!(second_id, first_id);
    let rows = asm
        .complete(second_id)
        .expect("the second dump rebuilds on its own id");
    assert_eq!(rows.len(), 14);
    assert!(
        rows.iter().all(|r| r["id"] != "asset-7"),
        "the removed row is gone from the new snapshot"
    );
    let (asm, _) = snapshot_of(&all);
    assert_eq!(
        asm.complete(first_id).unwrap().len(),
        15,
        "the first dump is still whole on the same topic"
    );
}
