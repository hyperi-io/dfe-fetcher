// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/db_odbc.rs
// Purpose:   The ODBC dump and tail shapes against real PostgreSQL and MariaDB, and the tail on SQL Server and Oracle where their drivers are installed
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The ODBC engine end to end.
//!
//! Each test starts its own database container (pinned tag, per-test name),
//! seeds a table that covers the type table, and runs a `Driver` over the
//! `DbShape` into scalo's in-process transport. The ODBC driver itself must be
//! registered with the host's driver manager: without it the test skips and
//! names the package, in CI it fails, because a gate that disappears with its
//! environment is not a gate.

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
use dfe_fetcher_core::envelope::{Envelope, Kind, Reassembler};
use dfe_fetcher_db::{DbInstance, DbShape, Dialect};

/// Counts leased bytes so a test can see blocks come and go.
struct Counting {
    current: AtomicI64,
    peak: AtomicI64,
}

impl Counting {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            current: AtomicI64::new(0),
            peak: AtomicI64::new(0),
        })
    }

    /// The same counter as the shape's `Lease`.
    fn as_lease(self: &Arc<Self>) -> Arc<dyn Lease> {
        Arc::clone(self) as Arc<dyn Lease>
    }
}

impl Lease for Counting {
    fn add(&self, bytes: u64) {
        let now = self
            .current
            .fetch_add(bytes.cast_signed(), Ordering::SeqCst)
            + bytes.cast_signed();
        self.peak.fetch_max(now, Ordering::SeqCst);
    }
    fn release(&self, bytes: u64) {
        self.current
            .fetch_sub(bytes.cast_signed(), Ordering::SeqCst);
    }
}

struct Harness {
    state: Arc<PipelineState>,
    transport: Arc<MemoryTransport>,
    metrics: Arc<Metrics>,
    shared: SharedConfig,
    config: Config,
}

fn harness(instance_yaml: &str) -> Harness {
    let mut config = Config::default();
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.accumulate.max_rows = 4;
    config.sources.db.insert(
        "inv".into(),
        serde_yaml_ng::from_str(instance_yaml).unwrap(),
    );
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
    Harness {
        state,
        transport,
        metrics,
        shared,
        config,
    }
}

fn driver(h: &Harness, shape: DbShape, checkpoints: Option<Arc<dyn CursorStore>>) -> Driver {
    Driver::new(DriverParts {
        shape: Shape::Db(Box::new(shape)),
        connection_id: "inv".into(),
        instance_id: "inst".into(),
        shared_config: h.shared.clone(),
        accumulate: h.config.accumulate,
        oversize: h.config.oversize,
        emitter: Emitter::new(
            Arc::clone(&h.state),
            Arc::clone(&h.metrics),
            h.config.accumulate.in_flight,
        ),
        pressure: None,
        memory_guard: Arc::clone(h.state.memory_guard()),
        checkpoints,
        metrics: Arc::clone(&h.metrics),
        shutdown: CancellationToken::new(),
    })
}

/// Everything on the transport so far, as (topic, payload).
async fn landed(transport: &MemoryTransport) -> Vec<(String, Value)> {
    let batch = transport.recv(1000).await.expect("recv");
    batch
        .records
        .into_iter()
        .map(|r| {
            (
                r.key.as_deref().unwrap_or("").to_owned(),
                serde_json::from_slice(&r.payload).expect("landed row is JSON"),
            )
        })
        .collect()
}

fn instance_yaml(dialect: &str, connection_string: &str, stores: &str) -> String {
    format!(
        "engine: odbc\ndialect: {dialect}\nconnection_string: '{connection_string}'\ntopic: inventory\nbatch: {{ max_rows: 2 }}\nstores:\n{stores}"
    )
}

const PG_INIT: &str = r#"
CREATE TABLE hosts (
    id integer PRIMARY KEY,
    big bigint NOT NULL,
    price numeric(10, 2),
    ratio double precision,
    alive boolean,
    seen_at timestamptz,
    local_at timestamp,
    born date,
    name text,
    secret bytea,
    uid uuid,
    doc jsonb,
    tags text[]
);
INSERT INTO hosts VALUES
    (1, 9007199254740993, 1234.56, 1.5, true, '2026-01-01 10:00:00+10', '2026-01-01 00:00:00', '2026-01-01', 'h' || chr(233) || 'llo "q"', '\xdeadbeef', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"k": [1, 2]}', '{a,b}'),
    (2, 0, NULL, NULL, false, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    (3, -1, 0.10, 2.25, NULL, '2026-06-30 23:59:59.123456+00', '2026-06-30 23:59:59.5', '1999-12-31', '', '\x00ff', NULL, '[]', '{}');
CREATE TABLE events (
    ts timestamptz NOT NULL,
    id integer NOT NULL,
    body text,
    PRIMARY KEY (ts, id)
);
INSERT INTO events VALUES
    ('2026-03-01 00:00:00+00', 1, 'a'),
    ('2026-03-01 00:00:00+00', 2, 'b'),
    ('2026-03-01 00:00:01+00', 1, 'c'),
    ('2026-03-01 00:00:02+00', 1, 'd'),
    ('2026-03-01 00:00:02+00', 2, 'e');
"#;

fn pg_connection_string(driver: &str, db: &common::DatabaseTestConfig) -> String {
    format!(
        "Driver={{{driver}}};Server={};Port={};Database=postgres;Uid=postgres;Pwd=postgres;UseDeclareFetch=1;BoolsAsChar=0",
        db.host, db.port
    )
}

fn rows_of(frames: &[(String, Value)]) -> Vec<Value> {
    frames
        .iter()
        .filter(|(_, v)| v["kind"] == "row")
        .map(|(_, v)| v["record"].clone())
        .collect()
}

#[tokio::test]
async fn a_postgres_dump_streams_typed_rows_through_the_driver_into_the_envelope() {
    let Some(driver_name) = common::odbc_driver("PostgreSQL", "odbc-postgresql") else {
        return;
    };
    let Some(db) = common::acquire_postgres("db-odbc-postgres-dump", PG_INIT).await else {
        return;
    };
    let yaml = instance_yaml(
        "postgres",
        &pg_connection_string(&driver_name, &db),
        "  - { unit: hosts, shape: dump, query: 'SELECT * FROM hosts ORDER BY id', row_key: '/id' }\n",
    );
    let h = harness(&yaml);
    let instance: DbInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let lease = Counting::new();
    let shape = DbShape::from_instance(&instance, "inv", &lease.as_lease()).expect("shape");
    let d = driver(&h, shape, None);

    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 3);
    assert_eq!(
        lease.current.load(Ordering::SeqCst),
        0,
        "every block released"
    );
    assert!(
        lease.peak.load(Ordering::SeqCst) > 0,
        "blocks were leased while held"
    );

    let frames = landed(&h.transport).await;
    assert_eq!(frames.len(), 5, "begin + 3 rows + end");
    assert!(
        frames
            .iter()
            .all(|(topic, _)| topic == "inventory-hosts_land")
    );
    assert_eq!(frames[0].1["kind"], "begin");
    assert_eq!(frames[0].1["store"], "inv.hosts");
    assert_eq!(frames[4].1["kind"], "end");
    assert_eq!(frames[4].1["row_count"], 3);
    assert_eq!(frames[1].1["_source_fetcher"], "inv.hosts");
    assert_eq!(frames[1].1["_source"], "inventory-hosts");

    let rows = rows_of(&frames);
    let full = &rows[0];
    assert_eq!(full["id"], 1);
    assert_eq!(full["big"], 9_007_199_254_740_993_i64);
    assert_eq!(full["price"], 1234.56);
    assert_eq!(full["ratio"], 1.5);
    assert_eq!(
        full["alive"], true,
        "boolean is a JSON bool with BoolsAsChar=0"
    );
    assert_eq!(
        full["seen_at"], "2026-01-01T00:00:00Z",
        "timestamptz converted to UTC by the session prelude"
    );
    assert_eq!(full["local_at"], "2026-01-01T00:00:00Z");
    assert_eq!(full["born"], "2026-01-01");
    assert_eq!(full["name"], "h\u{e9}llo \"q\"");
    assert_eq!(full["secret"], "3q2+7w==", "bytea is base64");
    assert!(
        full["uid"]
            .as_str()
            .is_some_and(|u| u.eq_ignore_ascii_case("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")),
        "uuid is a string as the driver renders it (psqlodbc upper-cases it): {}",
        full["uid"]
    );
    assert_eq!(
        full["doc"],
        serde_json::json!({"k": [1, 2]}),
        "jsonb reaches the driver as text and the deployment's unwrap_nested_json (on by default) parses it"
    );
    assert_eq!(
        full["tags"], "{a,b}",
        "an array literal is not JSON and stays text"
    );

    let nulls = &rows[1];
    assert_eq!(nulls["id"], 2);
    assert_eq!(nulls["big"], 0);
    assert_eq!(nulls["alive"], false);
    for key in [
        "price", "ratio", "seen_at", "local_at", "born", "name", "secret", "uid", "doc", "tags",
    ] {
        assert!(
            nulls.get(key).is_some_and(Value::is_null),
            "`{key}` is an explicit null: {nulls}"
        );
    }

    let third = &rows[2];
    assert_eq!(third["big"], -1);
    assert_eq!(third["price"], 0.1);
    assert_eq!(third["seen_at"], "2026-06-30T23:59:59.123456Z");
    assert_eq!(
        third["local_at"], "2026-06-30T23:59:59.500Z",
        "the fraction is printed at 3, 6 or 9 digits as the value needs"
    );
    assert_eq!(third["born"], "1999-12-31");
    assert_eq!(third["name"], "");
    assert_eq!(third["secret"], "AP8=");
    assert!(third["alive"].is_null());

    let mut asm = Reassembler::default();
    for (_, frame) in &frames {
        asm.offer(serde_json::to_vec(frame).unwrap().as_slice())
            .unwrap();
    }
    let id: Envelope = serde_json::from_value(frames[0].1.clone()).unwrap();
    assert_eq!(
        asm.complete(id.head().snapshot_id)
            .expect("the dump reassembles")
            .len(),
        3
    );
    assert!(frames.iter().all(|(_, f)| {
        let e: Envelope = serde_json::from_value(f.clone()).unwrap();
        e.kind() != Kind::Oversize
    }));
}

/// The restart proof every engine's tail runs: five rows keyed `(ts, id)`
/// with two rows sharing a `ts`, `limit: 2`, two ticks on one driver, a
/// second driver over the same cursor store (the restart) for the rest.
/// Every row lands once, in key order; the checkpoint after the first tick
/// is the second row's tuple, written only after the acks.
///
/// Each store pins `max_pages_per_tick: 1`, so a tick is one page and the
/// proof is about the keyset binding rather than how far a tick drains; the
/// paging itself is proven in `crates/db/src/shape.rs`.
fn tail_resumes_across_a_restart(
    yaml: &str,
    first_checkpoint: CheckpointValue,
) -> futures::future::BoxFuture<'_, ()> {
    Box::pin(tail_restart_proof(yaml, first_checkpoint))
}

async fn tail_restart_proof(yaml: &str, first_checkpoint: CheckpointValue) {
    let h = harness(yaml);
    let instance: DbInstance = serde_yaml_ng::from_str(yaml).unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    let build = || {
        let shape = DbShape::from_instance(&instance, "inv", &Counting::new().as_lease()).unwrap();
        driver(&h, shape, Some(Arc::clone(&store)))
    };

    let first = build();
    assert_eq!(first.run_tick(None).await.expect("tick 1").rows, 2);
    let cursor = store
        .get("inst.inv.events")
        .await
        .unwrap()
        .expect("committed after the acks");
    assert_eq!(cursor.checkpoint(), Some(first_checkpoint));
    assert_eq!(first.run_tick(None).await.expect("tick 2").rows, 2);

    // A new driver over the same store is a restart: it binds the committed
    // tuple as parameters and carries on.
    let second = build();
    assert_eq!(second.run_tick(None).await.expect("tick 3").rows, 1);
    assert_eq!(
        second.run_tick(None).await.expect("tick 4").rows,
        0,
        "nothing past the last row"
    );

    let frames = landed(&h.transport).await;
    assert!(frames.iter().all(|(topic, _)| topic == "inventory_land"));
    // Oracle and Snowflake report a plain column in upper case.
    let bodies: Vec<&str> = frames
        .iter()
        .map(|(_, v)| {
            v.as_object()
                .unwrap()
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("body"))
                .and_then(|(_, b)| b.as_str())
                .unwrap()
        })
        .collect();
    assert_eq!(
        bodies,
        ["a", "b", "c", "d", "e"],
        "every row once, in key order, across the restart"
    );
    assert!(
        frames.iter().all(|(_, v)| v.get("kind").is_none()),
        "a tail carries no snapshot envelope"
    );
}

#[tokio::test]
async fn a_postgres_tail_resumes_from_the_committed_keyset_across_a_restart() {
    let Some(driver_name) = common::odbc_driver("PostgreSQL", "odbc-postgresql") else {
        return;
    };
    let Some(db) = common::acquire_postgres("db-odbc-postgres-tail", PG_INIT).await else {
        return;
    };
    let yaml = instance_yaml(
        "postgres",
        &pg_connection_string(&driver_name, &db),
        "  - { unit: events, shape: tail, query: 'SELECT ts, id, body FROM events', key: [ts, id], limit: 2, max_pages_per_tick: 1 }\n",
    );
    tail_resumes_across_a_restart(
        &yaml,
        CheckpointValue::Keyset(vec![Value::from("2026-03-01T00:00:00Z"), Value::from(2)]),
    )
    .await;
}

const MARIADB_EVENTS: &str = r#"
CREATE TABLE events (
    ts DATETIME(6) NOT NULL,
    id INT NOT NULL,
    body VARCHAR(16),
    PRIMARY KEY (ts, id)
);
INSERT INTO events VALUES
    ('2026-03-01 00:00:00', 1, 'a'),
    ('2026-03-01 00:00:00', 2, 'b'),
    ('2026-03-01 00:00:01', 1, 'c'),
    ('2026-03-01 00:00:02', 1, 'd'),
    ('2026-03-01 00:00:02', 2, 'e');
"#;

#[tokio::test]
async fn a_mariadb_tail_resumes_from_the_committed_keyset_across_a_restart() {
    let Some(driver_name) = common::odbc_driver("MariaDB", "odbc-mariadb") else {
        return;
    };
    let Some(db) = common::acquire_mariadb("db-odbc-mariadb-tail", MARIADB_EVENTS).await else {
        return;
    };
    let connection_string = format!(
        "Driver={{{driver_name}}};Server={};Port={};Database=test;User=root;NO_CACHE=1;FORWARDONLY=1",
        db.host, db.port
    );
    let yaml = instance_yaml(
        "mysql",
        &connection_string,
        "  - { unit: events, shape: tail, query: 'SELECT ts, id, body FROM events', key: [ts, id], limit: 2, max_pages_per_tick: 1 }\n",
    );
    tail_resumes_across_a_restart(
        &yaml,
        CheckpointValue::Keyset(vec![Value::from("2026-03-01T00:00:00Z"), Value::from(2)]),
    )
    .await;
}

/// The five rows every proprietary-engine proof seeds, keyed `(seq, id)`
/// with two rows sharing a `seq`: the composite key is what the expanded
/// `a > ? OR (a = ? AND b > ?)` predicate exists for.
const SEQ_ROWS: [(i32, i32, &str); 5] = [
    (1, 1, "a"),
    (1, 2, "b"),
    (2, 1, "c"),
    (3, 1, "d"),
    (3, 2, "e"),
];

fn seq_inserts(table: &str) -> Vec<String> {
    SEQ_ROWS
        .iter()
        .map(|(seq, id, body)| {
            format!("INSERT INTO {table} (seq, id, body) VALUES ({seq}, {id}, '{body}')")
        })
        .collect()
}

/// SQL Server through the operator-supplied `msodbcsql18` driver: skips,
/// saying so, wherever the driver is not registered (every shared runner).
/// Proves the expanded disjunction, `[ ]`-free plain keys, the `AS` alias,
/// and `OFFSET 0 ROWS FETCH NEXT n ROWS ONLY` after the `ORDER BY`.
#[tokio::test]
async fn a_mssql_tail_resumes_from_the_committed_keyset_across_a_restart() {
    let Some(driver_name) = common::odbc_driver_optional(
        "SQL Server",
        "Microsoft's ODBC Driver 18 (proprietary, redistributable under its EULA)",
    ) else {
        return;
    };
    let password = "Dfe-fetcher-1";
    let Some(db) = common::acquire_mssql("db-odbc-mssql-tail", password).await else {
        return;
    };
    let connection_string = format!(
        "Driver={{{driver_name}}};Server={},{};Database=master;Uid=sa;Pwd={password};Encrypt=no",
        db.host, db.port
    );
    let mut statements =
        vec!["CREATE TABLE events (seq INT NOT NULL, id INT NOT NULL, body VARCHAR(16), PRIMARY KEY (seq, id))".to_owned()];
    statements.extend(seq_inserts("events"));
    let refs: Vec<&str> = statements.iter().map(String::as_str).collect();
    dfe_fetcher_db::odbc::run_statements(&connection_string, Dialect::Mssql, &refs)
        .await
        .expect("seed");
    let yaml = instance_yaml(
        "mssql",
        &connection_string,
        "  - { unit: events, shape: tail, query: 'SELECT seq, id, body FROM events', key: [seq, id], limit: 2, max_pages_per_tick: 1 }\n",
    );
    tail_resumes_across_a_restart(
        &yaml,
        CheckpointValue::Keyset(vec![Value::from(1), Value::from(2)]),
    )
    .await;
}

/// Oracle through the operator-supplied Instant Client ODBC driver: skips,
/// saying so, wherever the driver is not registered. Proves the expanded
/// disjunction, the alias with no `AS`, and `FETCH FIRST n ROWS ONLY`.
#[tokio::test]
async fn an_oracle_tail_resumes_from_the_committed_keyset_across_a_restart() {
    let Some(driver_name) = common::odbc_driver_optional(
        "Oracle",
        "Oracle Instant Client ODBC (proprietary, OTN licence)",
    ) else {
        return;
    };
    let password = "DfeFetcher1";
    let Some(db) = common::acquire_oracle("db-odbc-oracle-tail", password).await else {
        return;
    };
    let connection_string = format!(
        "Driver={{{driver_name}}};DBQ={}:{}/FREEPDB1;UID=dfe;PWD={password}",
        db.host, db.port
    );
    let mut statements = vec![
        "CREATE TABLE events (seq NUMBER(10) NOT NULL, id NUMBER(10) NOT NULL, body VARCHAR2(16), PRIMARY KEY (seq, id))".to_owned(),
    ];
    statements.extend(seq_inserts("events"));
    let refs: Vec<&str> = statements.iter().map(String::as_str).collect();
    dfe_fetcher_db::odbc::run_statements(&connection_string, Dialect::Oracle, &refs)
        .await
        .expect("seed");
    let yaml = instance_yaml(
        "oracle",
        &connection_string,
        "  - { unit: events, shape: tail, query: 'SELECT seq, id, body FROM events', key: [seq, id], limit: 2, max_pages_per_tick: 1 }\n",
    );
    tail_resumes_across_a_restart(
        &yaml,
        CheckpointValue::Keyset(vec![Value::from(1), Value::from(2)]),
    )
    .await;
}

const MARIADB_INIT: &str = r#"
CREATE TABLE hosts (
    id INT PRIMARY KEY,
    big BIGINT NOT NULL,
    price DECIMAL(10, 2),
    ratio DOUBLE,
    alive TINYINT(1),
    seen_at TIMESTAMP(6) NULL,
    local_at DATETIME(3),
    born DATE,
    name VARCHAR(64),
    notes TEXT,
    secret BLOB,
    fixed BINARY(2),
    doc JSON
);
SET time_zone = '+10:00';
INSERT INTO hosts VALUES
    (1, 9007199254740993, 1234.56, 1.5, 1, '2026-01-01 10:00:00', '2026-01-01 00:00:00.250', '2026-01-01', 'hello "q"', 'long form', X'DEADBEEF', X'00FF', '{"k": [1, 2]}'),
    (2, 0, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
"#;

#[tokio::test]
async fn a_mariadb_dump_streams_typed_rows_through_the_driver_into_the_envelope() {
    let Some(driver_name) = common::odbc_driver("MariaDB", "odbc-mariadb") else {
        return;
    };
    let Some(db) = common::acquire_mariadb("db-odbc-mariadb-dump", MARIADB_INIT).await else {
        return;
    };
    let connection_string = format!(
        "Driver={{{driver_name}}};Server={};Port={};Database=test;User=root;NO_CACHE=1;FORWARDONLY=1",
        db.host, db.port
    );
    let yaml = instance_yaml(
        "mysql",
        &connection_string,
        "  - { unit: hosts, shape: dump, query: 'SELECT * FROM hosts ORDER BY id', row_key: '/id' }\n",
    );
    let h = harness(&yaml);
    let instance: DbInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let lease = Counting::new();
    let shape = DbShape::from_instance(&instance, "inv", &lease.as_lease()).expect("shape");
    let d = driver(&h, shape, None);

    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 2);
    assert_eq!(lease.current.load(Ordering::SeqCst), 0);

    let frames = landed(&h.transport).await;
    assert_eq!(frames.len(), 4, "begin + 2 rows + end");
    assert_eq!(frames[0].1["kind"], "begin");
    assert_eq!(frames[3].1["row_count"], 2);
    let rows = rows_of(&frames);
    let full = &rows[0];
    assert_eq!(full["id"], 1);
    assert_eq!(full["big"], 9_007_199_254_740_993_i64);
    assert_eq!(full["price"], 1234.56);
    assert_eq!(full["ratio"], 1.5);
    assert_eq!(
        full["alive"], 1,
        "TINYINT(1) is a number; MariaDB has no boolean type"
    );
    assert_eq!(
        full["seen_at"], "2026-01-01T00:00:00Z",
        "TIMESTAMP stored at +10:00 comes back in UTC under the session prelude"
    );
    assert_eq!(full["local_at"], "2026-01-01T00:00:00.250Z");
    assert_eq!(full["born"], "2026-01-01");
    assert_eq!(full["name"], "hello \"q\"");
    assert_eq!(full["notes"], "long form");
    assert_eq!(full["secret"], "3q2+7w==", "BLOB is base64");
    assert_eq!(full["fixed"], "AP8=", "BINARY(n) is base64");
    assert_eq!(
        full["doc"],
        serde_json::json!({"k": [1, 2]}),
        "JSON reaches the driver as text and the deployment's unwrap_nested_json (on by default) parses it"
    );

    let nulls = &rows[1];
    assert_eq!(nulls["alive"], 0);
    for key in [
        "price", "ratio", "seen_at", "local_at", "born", "name", "notes", "secret", "fixed", "doc",
    ] {
        assert!(
            nulls.get(key).is_some_and(Value::is_null),
            "`{key}` is an explicit null: {nulls}"
        );
    }
}

#[tokio::test]
async fn a_missing_table_aborts_the_tick_with_the_engine_error_and_no_end_marker() {
    let Some(driver_name) = common::odbc_driver("PostgreSQL", "odbc-postgresql") else {
        return;
    };
    let Some(db) = common::acquire_postgres("db-odbc-postgres-missing", PG_INIT).await else {
        return;
    };
    let yaml = instance_yaml(
        "postgres",
        &pg_connection_string(&driver_name, &db),
        "  - { unit: ghosts, shape: dump, query: 'SELECT * FROM ghosts' }\n",
    );
    let h = harness(&yaml);
    let instance: DbInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let shape = DbShape::from_instance(&instance, "inv", &Counting::new().as_lease()).unwrap();
    let d = driver(&h, shape, None);
    let err = d.run_tick(None).await.expect_err("no such table");
    assert!(
        err.to_string().contains("42P01") || err.to_string().contains("ghosts"),
        "the engine's own error reaches the tick: {err}"
    );
    let frames = landed(&h.transport).await;
    assert!(
        frames.iter().all(|(_, v)| v["kind"] != "end"),
        "an aborted dump emits no end marker: {frames:?}"
    );
    assert!(
        d.health_check().await.expect("probe"),
        "the connection itself is fine"
    );
}
