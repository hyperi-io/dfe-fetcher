// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/db_odbc_kafka.rs
// Purpose:   A PostgreSQL dump over ODBC through the batcher to real Kafka, rebuilt by snapshot_id
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The database shape end to end on real Kafka.
//!
//! A PostgreSQL container seeds a store; the `DbShape` over its ODBC driver
//! runs through a `Driver` and the batcher to a Kafka broker (live if
//! reachable, else a testcontainer named per test); a consumer reads the
//! topic back and rebuilds the snapshot by `snapshot_id`, and refuses the
//! same frames with one row missing.

use crate::common;

use std::sync::Arc;

use chrono::Utc;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use dfe_fetcher::config::{Config, OutputConfig, SharedConfig};
use dfe_fetcher::driver::{Driver, DriverParts, Shape};
use dfe_fetcher::emit::Emitter;
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::output::OutputManager;
use dfe_fetcher::pipeline::PipelineState;
use dfe_fetcher_core::batch::{Lease, NoLease};
use dfe_fetcher_core::envelope::{Envelope, Kind, Status};
use dfe_fetcher_db::{DbInstance, DbShape};

const INIT: &str = r#"
CREATE TABLE assets (
    id integer PRIMARY KEY,
    name text NOT NULL,
    alive boolean NOT NULL,
    seen_at timestamptz NOT NULL
);
INSERT INTO assets
SELECT n, 'asset-' || n, n % 2 = 0, '2026-01-01 00:00:00+00'::timestamptz + (n || ' minutes')::interval
FROM generate_series(0, 14) AS n;
"#;

#[tokio::test]
async fn a_postgres_dump_reaches_kafka_and_a_consumer_rebuilds_it_by_snapshot_id() {
    let Some(driver_name) = common::odbc_driver("PostgreSQL", "odbc-postgresql") else {
        return;
    };
    let Some(db) = common::acquire_postgres("db-odbc-kafka-roundtrip", INIT).await else {
        return;
    };
    let Some((kf, _holder)) = common::acquire_kafka("db-odbc-kafka-roundtrip").await else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let suffix = format!("-{}", Utc::now().timestamp_millis());
    let topic = format!("pgdump-assets{suffix}");
    let connection_string = format!(
        "Driver={{{driver_name}}};Server={};Port={};Database=postgres;Uid=postgres;Pwd=postgres;UseDeclareFetch=1;BoolsAsChar=0",
        db.host, db.port
    );
    let instance: DbInstance = serde_yaml_ng::from_str(&format!(
        "engine: odbc\ndialect: postgres\nconnection_string: '{connection_string}'\ntopic: pgdump\nbatch: {{ max_rows: 4 }}\nstores:\n  - {{ unit: assets, shape: dump, query: 'SELECT * FROM assets ORDER BY id', row_key: '/id' }}\n"
    ))
    .unwrap();

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
    config.sources.db.insert("pg".into(), instance.clone());
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
    let lease: Arc<dyn Lease> = Arc::new(NoLease);
    let shape = DbShape::from_instance(&instance, "pg", &lease).expect("shape");
    let driver = Driver::new(DriverParts {
        shape: Shape::Db(Box::new(shape)),
        connection_id: "pg".into(),
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
    });

    let report = driver.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 15);
    assert!(
        report.flushes >= 4,
        "max_rows 4 over 17 frames flushes at least four times, got {}",
        report.flushes
    );

    let frames = common::consume_frames(&kf, &topic, &format!("pgdump-first{suffix}"), 17).await;
    assert_eq!(frames.len(), 17, "begin + 15 rows + end");
    let (asm, id) = common::reassemble(&frames);
    let rows = asm
        .complete(id)
        .expect("the consumer rebuilds the whole dump by snapshot_id");
    assert_eq!(rows.len(), 15);
    assert!(rows.iter().any(|r| r["id"] == 7 && r["name"] == "asset-7"));
    assert!(rows.iter().all(|r| r["alive"].is_boolean()));
    assert!(
        rows.iter()
            .all(|r| r["seen_at"].as_str().is_some_and(|s| s.ends_with('Z'))),
        "timestamps land as RFC 3339 UTC"
    );
    let landed: Value = serde_json::from_slice(&frames[1]).unwrap();
    assert_eq!(landed["_source"], "pgdump-assets");
    assert_eq!(landed["_source_fetcher"], "pg.assets");
    assert_eq!(landed["store"], "pg.assets");

    let mut truncated: Vec<Vec<u8>> = frames.clone();
    let dropped = truncated
        .iter()
        .position(|f| serde_json::from_slice::<Envelope>(f).unwrap().kind() == Kind::Row)
        .unwrap();
    truncated.remove(dropped);
    let (asm, id) = common::reassemble(&truncated);
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
}
