// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/db_clickhouse.rs
// Purpose:   The ClickHouse dump and keyset-tail shapes against a real server through the driver
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The ClickHouse engine end to end.
//!
//! A ClickHouse container (pinned tag, per-test name) holds a table that
//! covers the server-side type mapping; a `Driver` over the `DbShape` streams
//! its `JSONEachRow` body into scalo's in-process transport and the test reads
//! the envelope back. The tail proof binds the committed key tuple through
//! typed `{k<i>:Type}` placeholders whose types the store read from
//! `DESCRIBE`, across a restart.

use crate::common;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use scalo::transport::{MemoryConfig, MemoryTransport, TransportReceiver};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use dfe_fetcher::config::{Config, SharedConfig};
use dfe_fetcher::driver::{Driver, DriverParts, Shape};
use dfe_fetcher::emit::Emitter;
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::output::OutputManager;
use dfe_fetcher::pipeline::PipelineState;
use dfe_fetcher_core::CheckpointValue;
use dfe_fetcher_core::batch::Lease;
use dfe_fetcher_core::checkpoint::CursorStore;
use dfe_fetcher_db::{DbInstance, DbShape};

struct Counting(AtomicI64);

impl Lease for Counting {
    fn add(&self, bytes: u64) {
        self.0.fetch_add(bytes.cast_signed(), Ordering::SeqCst);
    }
    fn release(&self, bytes: u64) {
        self.0.fetch_sub(bytes.cast_signed(), Ordering::SeqCst);
    }
}

const DDL: &str = "CREATE TABLE audit (
    id UInt64,
    signed Int64,
    ratio Float64,
    amount Decimal(10, 2),
    flag Bool,
    at DateTime('Asia/Tokyo'),
    at_ms DateTime64(3, 'UTC'),
    day Date,
    name String,
    maybe Nullable(String),
    tags Array(String),
    uid UUID
) ENGINE = MergeTree ORDER BY id";

const ROWS: &str = "INSERT INTO audit VALUES
    (18446744073709551615, -5, 1.5, 1234.56, true, '2026-01-01 09:00:00', '2026-01-01 00:00:00.250', '2026-01-01', 'héllo \"q\"', NULL, ['a', 'b'], 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
    (2, 0, 0, 0, false, '2026-01-01 09:00:01', '2026-01-01 00:00:01', '1999-12-31', '', 'set', [], '00000000-0000-0000-0000-000000000000')";

async fn seed(url: &str, sql: &str) {
    let response = reqwest::Client::new()
        .post(url)
        .body(sql.to_owned())
        .send()
        .await
        .expect("clickhouse answers");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert!(status.is_success(), "seed `{sql}` failed: {status} {body}");
}

#[tokio::test]
async fn a_clickhouse_dump_streams_server_typed_rows_through_the_driver_into_the_envelope() {
    let Some(db) = common::acquire_clickhouse("db-clickhouse-dump").await else {
        return;
    };
    let url = format!("http://{}:{}", db.host, db.port);
    seed(&url, DDL).await;
    seed(&url, ROWS).await;

    let yaml = format!(
        "engine: clickhouse\nconnection_string: '{url}/default'\ntopic: ch\nstores:\n  - {{ unit: audit, shape: dump, query: 'SELECT * FROM audit ORDER BY id', row_key: '/id' }}\n"
    );
    let instance: DbInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let mut config = Config::default();
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.db.insert("ch".into(), instance.clone());
    let transport = Arc::new(
        MemoryTransport::new(&MemoryConfig {
            buffer_size: 1000,
            ..MemoryConfig::default()
        })
        .expect("memory transport"),
    );
    let shared = SharedConfig::new(config.clone());
    let metrics = Arc::new(Metrics::new());
    let state = Arc::new(
        PipelineState::new(
            shared.clone(),
            Arc::clone(&metrics),
            Some(OutputManager::memory(Arc::clone(&transport))),
            CancellationToken::new(),
        )
        .expect("pipeline state"),
    );
    let lease = Arc::new(Counting(AtomicI64::new(0)));
    let as_lease: Arc<dyn Lease> = lease.clone();
    let shape = DbShape::from_instance(&instance, "ch", &as_lease).expect("shape");
    let d = Driver::new(DriverParts {
        shape: Shape::Db(Box::new(shape)),
        connection_id: "ch".into(),
        instance_id: "inst".into(),
        shared_config: shared,
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
    assert!(d.health_check().await.expect("probe"), "ping answers");

    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 2);
    assert_eq!(lease.0.load(Ordering::SeqCst), 0, "every chunk released");

    let batch = transport.recv(1000).await.expect("recv");
    let frames: Vec<(String, Value)> = batch
        .records
        .into_iter()
        .map(|r| {
            (
                r.key.as_deref().unwrap_or("").to_owned(),
                serde_json::from_slice(&r.payload).expect("landed row is JSON"),
            )
        })
        .collect();
    assert_eq!(frames.len(), 4, "begin + 2 rows + end");
    assert!(frames.iter().all(|(topic, _)| topic == "ch-audit_land"));
    assert_eq!(frames[0].1["store"], "ch.audit");
    assert_eq!(frames[3].1["row_count"], 2);

    let rows: Vec<Value> = frames
        .iter()
        .filter(|(_, v)| v["kind"] == "row")
        .map(|(_, v)| v["record"].clone())
        .collect();
    let full = &rows[1];
    assert_eq!(
        full["id"], 18_446_744_073_709_551_615_u64,
        "UInt64 is a number, not a quoted string"
    );
    assert_eq!(full["signed"], -5);
    assert_eq!(full["ratio"], 1.5);
    assert_eq!(full["amount"], 1234.56);
    assert_eq!(full["flag"], true);
    assert_eq!(
        full["at"], "2026-01-01T00:00:00Z",
        "a DateTime in Asia/Tokyo lands as UTC under the iso output format"
    );
    assert_eq!(full["at_ms"], "2026-01-01T00:00:00.250Z");
    assert_eq!(full["day"], "2026-01-01");
    assert_eq!(full["name"], "h\u{e9}llo \"q\"");
    assert!(full["maybe"].is_null(), "Nullable lands as null: {full}");
    assert_eq!(full["tags"], serde_json::json!(["a", "b"]));
    assert_eq!(full["uid"], "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");

    let second = &rows[0];
    assert_eq!(second["id"], 2);
    assert_eq!(second["maybe"], "set");
    assert_eq!(second["tags"], serde_json::json!([]));
}

/// The key column renders in Tokyo, so the proof covers the zone: the row
/// reports UTC, the checkpoint holds UTC, and the bound parameter must be
/// the same instant.
const EVENTS_DDL: &str = "CREATE TABLE events (
    ts DateTime64(3, 'Asia/Tokyo'),
    id UInt64,
    body String
) ENGINE = MergeTree ORDER BY (ts, id)";

const EVENTS: &str = "INSERT INTO events VALUES
    ('2026-03-01 09:00:00.000', 1, 'a'),
    ('2026-03-01 09:00:00.000', 2, 'b'),
    ('2026-03-01 09:00:01.500', 1, 'c'),
    ('2026-03-01 09:00:02.000', 1, 'd'),
    ('2026-03-01 09:00:02.000', 2, 'e')";

#[tokio::test]
async fn a_clickhouse_tail_resumes_from_the_committed_keyset_through_typed_placeholders() {
    let Some(db) = common::acquire_clickhouse("db-clickhouse-tail").await else {
        return;
    };
    let url = format!("http://{}:{}", db.host, db.port);
    seed(&url, EVENTS_DDL).await;
    seed(&url, EVENTS).await;

    let yaml = format!(
        "engine: clickhouse\nconnection_string: '{url}/default'\ntopic: ch\nstores:\n  - {{ unit: events, shape: tail, query: 'SELECT ts, id, body FROM events', key: [ts, id], limit: 2, max_pages_per_tick: 1 }}\n"
    );
    let instance: DbInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let mut config = Config::default();
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.db.insert("ch".into(), instance.clone());
    let transport = Arc::new(
        MemoryTransport::new(&MemoryConfig {
            buffer_size: 1000,
            ..MemoryConfig::default()
        })
        .expect("memory transport"),
    );
    let shared = SharedConfig::new(config.clone());
    let metrics = Arc::new(Metrics::new());
    let state = Arc::new(
        PipelineState::new(
            shared.clone(),
            Arc::clone(&metrics),
            Some(OutputManager::memory(Arc::clone(&transport))),
            CancellationToken::new(),
        )
        .expect("pipeline state"),
    );
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    let build = || {
        let lease: Arc<dyn Lease> = Arc::new(Counting(AtomicI64::new(0)));
        let shape = DbShape::from_instance(&instance, "ch", &lease).expect("shape");
        Driver::new(DriverParts {
            shape: Shape::Db(Box::new(shape)),
            connection_id: "ch".into(),
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
            checkpoints: Some(Arc::clone(&store)),
            metrics: Arc::clone(&metrics),
            shutdown: CancellationToken::new(),
        })
    };

    let first = build();
    assert_eq!(first.run_tick(None).await.expect("tick 1").rows, 2);
    let cursor = store
        .get("inst.ch.events")
        .await
        .unwrap()
        .expect("committed after the acks");
    assert_eq!(
        cursor.checkpoint(),
        Some(CheckpointValue::Keyset(vec![
            Value::from("2026-03-01T00:00:00.000Z"),
            Value::from(2)
        ])),
        "the DateTime64 lands as RFC 3339 and is what the next tick binds back"
    );
    assert_eq!(first.run_tick(None).await.expect("tick 2").rows, 2);

    // A new store reads the key types from DESCRIBE again and binds the
    // committed tuple through `{k0:DateTime64(3)}` and `{k1:UInt64}`.
    let second = build();
    assert_eq!(second.run_tick(None).await.expect("tick 3").rows, 1);
    assert_eq!(second.run_tick(None).await.expect("tick 4").rows, 0);

    let batch = transport.recv(1000).await.expect("recv");
    let bodies: Vec<String> = batch
        .records
        .iter()
        .map(|r| {
            let v: Value = serde_json::from_slice(&r.payload).unwrap();
            assert_eq!(r.key.as_deref(), Some("ch_land"));
            assert!(v.get("kind").is_none(), "a tail carries no envelope");
            v["body"].as_str().unwrap().to_owned()
        })
        .collect();
    assert_eq!(
        bodies,
        ["a", "b", "c", "d", "e"],
        "every row once, in key order, across the restart"
    );
}
