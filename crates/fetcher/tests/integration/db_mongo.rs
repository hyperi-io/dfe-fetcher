// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/db_mongo.rs
// Purpose:   The MongoDB dump, change-stream tail and keyset tail against a real server through the driver
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The MongoDB engine end to end.
//!
//! Each test starts its own `mongo` container (pinned tag, per-test name): a
//! one-member replica set for the change stream, a standalone server for the
//! dump and the keyset fallback. Documents are seeded through the driver the
//! engine itself uses; a `Driver` over the `DbShape` lands the rows on
//! scalo's in-process transport and the test reads them back.

use crate::common;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use dfe_fetcher_db::mongo::mongodb;
use mongodb::bson::{Bson, DateTime, Document, doc, oid::ObjectId};
use mongodb::{Client, Collection};
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
use dfe_fetcher_core::envelope::{Envelope, Reassembler};
use dfe_fetcher_db::{DbInstance, DbShape};

/// Counts leased bytes so a test can see documents come and go.
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

fn instance_yaml(uri: &str, stores: &str) -> String {
    format!(
        "engine: mongodb\nconnection_string: '{uri}'\ntopic: inventory\nbatch: {{ max_rows: 2 }}\nstores:\n{stores}"
    )
}

fn file_store() -> (tempfile::TempDir, Arc<dyn CursorStore>) {
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    (dir, store)
}

/// The collection the tests seed, through the same driver the engine uses.
async fn collection(uri: &str) -> Collection<Document> {
    Client::with_uri_str(uri)
        .await
        .expect("client")
        .database("inventory")
        .collection("hosts")
}

/// `n` hosts with an `ObjectId` identity and the types the row shape maps.
fn hosts(n: u32) -> Vec<Document> {
    (1..=n)
        .map(|i| {
            doc! {
                "_id": ObjectId::new(),
                "n": i,
                "name": format!("host-{i}"),
                "alive": i % 2 == 1,
                "seen_at": DateTime::from_millis(1_767_225_600_000 + i64::from(i) * 1000),
                "big": 9_007_199_254_740_993_i64,
                "tags": ["a", "b"],
                "nested": { "k": Bson::Null },
            }
        })
        .collect()
}

#[tokio::test]
async fn a_mongodb_dump_streams_relaxed_extended_json_through_the_driver_into_the_envelope() {
    let Some(db) = common::acquire_mongo("db-mongo-dump", false).await else {
        return;
    };
    let uri = common::mongo_uri(&db, false);
    let coll = collection(&uri).await;
    coll.insert_many(hosts(7)).await.expect("seed");

    let yaml = instance_yaml(
        &uri,
        "  - { unit: hosts, shape: dump, row_key: '/_id/$oid', mongodb: { database: inventory, collection: hosts, filter: { alive: true } } }\n",
    );
    let h = harness(&yaml);
    let instance: DbInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let lease = Counting::new();
    let shape = DbShape::from_instance(&instance, "inv", &lease.as_lease()).expect("shape");
    let d = driver(&h, shape, None);
    assert!(d.health_check().await.expect("probe"), "ping answers");

    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 4, "the filter keeps the odd-numbered hosts");
    assert_eq!(
        lease.current.load(Ordering::SeqCst),
        0,
        "every document released"
    );
    assert!(lease.peak.load(Ordering::SeqCst) > 0);

    let frames = landed(&h.transport).await;
    assert_eq!(frames.len(), 6, "begin + 4 rows + end");
    assert!(
        frames
            .iter()
            .all(|(topic, _)| topic == "inventory-hosts_land")
    );
    assert_eq!(frames[0].1["kind"], "begin");
    assert_eq!(frames[0].1["store"], "inv.hosts");
    assert_eq!(frames[5].1["kind"], "end");
    assert_eq!(frames[5].1["row_count"], 4);
    assert_eq!(frames[1].1["_source_fetcher"], "inv.hosts");

    let rows: Vec<Value> = frames
        .iter()
        .filter(|(_, v)| v["kind"] == "row")
        .map(|(_, v)| v["record"].clone())
        .collect();
    let first = &rows[0];
    assert!(
        first["_id"]["$oid"]
            .as_str()
            .is_some_and(|oid| oid.len() == 24),
        "ObjectId lands as relaxed extended JSON: {first}"
    );
    assert_eq!(first["n"], 1);
    assert_eq!(first["name"], "host-1");
    assert_eq!(first["alive"], true);
    assert_eq!(
        first["seen_at"],
        serde_json::json!({ "$date": "2026-01-01T00:00:01Z" })
    );
    assert_eq!(first["big"], 9_007_199_254_740_993_i64, "Int64 stays exact");
    assert_eq!(first["tags"], serde_json::json!(["a", "b"]));
    assert!(first["nested"]["k"].is_null());
    let names: Vec<&str> = rows.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["host-1", "host-3", "host-5", "host-7"]);

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
        4
    );
}

#[tokio::test]
async fn a_mongodb_change_stream_tail_resumes_from_the_committed_token_across_a_restart() {
    let Some(db) = common::acquire_mongo("db-mongo-change-stream", true).await else {
        return;
    };
    let uri = common::mongo_uri(&db, true);
    let coll = collection(&uri).await;

    let yaml = instance_yaml(
        &uri,
        "  - { unit: changes, shape: tail, limit: 3, max_pages_per_tick: 1, mongodb: { database: inventory, collection: hosts } }\n",
    );
    let h = harness(&yaml);
    let instance: DbInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let (_dir, store) = file_store();
    let build = || {
        let shape = DbShape::from_instance(&instance, "inv", &Counting::new().as_lease()).unwrap();
        driver(&h, shape, Some(Arc::clone(&store)))
    };

    // A stream with no token starts at "now" and sees nothing; the idle tick
    // still commits where the stream reached.
    let first = build();
    assert_eq!(first.run_tick(None).await.expect("tick 0").rows, 0);
    let idle = store
        .get("inst.inv.changes")
        .await
        .unwrap()
        .expect("an idle tick commits the stream's post-batch token");
    assert!(
        matches!(idle.checkpoint(), Some(CheckpointValue::Keyset(ref t)) if t.len() == 1),
        "{idle:?}"
    );

    // A writer inserts probes until a tick has emitted some of them and
    // committed the last one's token, then the rest drain tick by tick.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let probes = {
        let writer = collection(&uri).await;
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut inserted = 0_u32;
            while !stop.load(Ordering::SeqCst) {
                inserted += 1;
                writer
                    .insert_one(doc! { "probe": inserted })
                    .await
                    .expect("probe insert");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            inserted
        })
    };
    // Tick until one carries probes: a tick can finish before the writer's
    // first insert reaches the stream, and stopping the writer first would
    // leave nothing to emit.
    let mut bootstrap = first.run_tick(None).await.expect("bootstrap tick");
    for _ in 0..100 {
        if bootstrap.rows > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        bootstrap = first.run_tick(None).await.expect("bootstrap tick");
    }
    stop.store(true, Ordering::SeqCst);
    let probes = probes.await.unwrap();
    assert!(
        (1..=3).contains(&bootstrap.rows),
        "a tick emits what the open stream saw, capped by limit 3: {}",
        bootstrap.rows
    );
    let cursor = store
        .get("inst.inv.changes")
        .await
        .unwrap()
        .expect("committed after the acks");
    let Some(CheckpointValue::Keyset(token)) = cursor.checkpoint() else {
        panic!("a keyset checkpoint holding the resume token");
    };
    assert_eq!(token.len(), 1);
    assert!(
        token[0]["_data"].is_string(),
        "the resume token is the event's `_id` document: {token:?}"
    );
    let mut drained = bootstrap.rows;
    loop {
        let rows = first.run_tick(None).await.expect("drain tick").rows;
        if rows == 0 {
            break;
        }
        drained += rows;
    }
    assert_eq!(
        drained,
        u64::from(probes),
        "every probe inserted while a stream was open or resumable lands exactly once"
    );

    // Seven changes with no stream open, then a new driver resumes from the
    // committed token and picks them up three a tick.
    coll.insert_many(hosts(5).into_iter().map(|mut d| {
        d.insert("wave", 3);
        d
    }))
    .await
    .expect("third wave");
    coll.update_one(
        doc! { "wave": 3, "n": 1 },
        doc! { "$set": { "alive": false } },
    )
    .await
    .expect("update");
    coll.delete_one(doc! { "wave": 3, "n": 2 })
        .await
        .expect("delete");
    drop(first);
    let second = build();
    assert_eq!(second.run_tick(None).await.expect("tick a").rows, 3);
    assert_eq!(second.run_tick(None).await.expect("tick b").rows, 3);
    assert_eq!(
        second.run_tick(None).await.expect("tick c").rows,
        1,
        "the seventh event"
    );
    assert_eq!(second.run_tick(None).await.expect("tick d").rows, 0);

    let frames = landed(&h.transport).await;
    assert!(frames.iter().all(|(topic, _)| topic == "inventory_land"));
    let probe_frames = frames
        .iter()
        .filter(|(_, v)| v["fullDocument"].get("probe").is_some())
        .count();
    assert_eq!(probe_frames, probes as usize, "one frame per probe");
    let wave: Vec<&Value> = frames
        .iter()
        .map(|(_, v)| v)
        .filter(|v| v["fullDocument"].get("probe").is_none())
        .collect();
    assert_eq!(wave.len(), 7);
    let ops: Vec<&str> = wave
        .iter()
        .map(|v| v["operationType"].as_str().unwrap())
        .collect();
    assert_eq!(
        ops,
        [
            "insert", "insert", "insert", "insert", "insert", "update", "delete"
        ],
        "every event once, in oplog order, across the restart"
    );
    for v in &frames {
        let v = &v.1;
        assert!(v["_id"]["_data"].is_string(), "{v}");
        assert_eq!(v["ns"]["db"], "inventory");
        assert_eq!(v["ns"]["coll"], "hosts");
        assert!(v["documentKey"]["_id"]["$oid"].is_string(), "{v}");
        assert!(v.get("kind").is_none(), "a tail carries no envelope");
    }
    let update = wave[5];
    assert_eq!(
        update["fullDocument"]["alive"], false,
        "update_lookup carries the current document: {update}"
    );
    assert_eq!(update["updateDescription"]["updatedFields"]["alive"], false);
    assert!(
        wave[6].get("fullDocument").is_none(),
        "a delete has no document"
    );
    assert_eq!(wave[0]["fullDocument"]["name"], "host-1");
    assert!(
        wave[0]["clusterTime"]["$timestamp"]["t"].is_number(),
        "the cluster time is extended JSON: {}",
        wave[0]
    );
}

/// An idle tick commits the stream's post-batch resume token, so a write made
/// while no stream was open is seen by the next tick.
///
/// Without it the committed token stays where the last EVENT was: a reopened
/// stream starts at "now", everything written in between is lost, and once the
/// oplog rolls past that token the server refuses the resume outright.
#[tokio::test]
async fn an_idle_change_stream_tick_commits_a_token_that_still_sees_later_writes() {
    let Some(db) = common::acquire_mongo("db-mongo-idle-advance", true).await else {
        return;
    };
    let uri = common::mongo_uri(&db, true);
    let coll = collection(&uri).await;
    let yaml = instance_yaml(
        &uri,
        "  - { unit: changes, shape: tail, limit: 3, max_pages_per_tick: 1, mongodb: { database: inventory, collection: hosts } }\n",
    );
    let h = harness(&yaml);
    let instance: DbInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let (_dir, store) = file_store();
    let shape = DbShape::from_instance(&instance, "inv", &Counting::new().as_lease()).unwrap();
    let tail = driver(&h, shape, Some(Arc::clone(&store)));

    assert_eq!(tail.run_tick(None).await.expect("idle tick").rows, 0);
    let idle = store
        .get("inst.inv.changes")
        .await
        .unwrap()
        .expect("an idle tick commits the post-batch token");
    let Some(CheckpointValue::Keyset(token)) = idle.checkpoint() else {
        panic!("a keyset checkpoint holding the resume token");
    };
    assert_eq!(token.len(), 1);
    assert!(token[0]["_data"].is_string(), "{token:?}");

    // Written with no stream open, and seen because the next tick resumes
    // from the committed token instead of opening at "now".
    coll.insert_many(hosts(2)).await.expect("between ticks");
    assert_eq!(
        tail.run_tick(None).await.expect("next tick").rows,
        2,
        "the writes made between the ticks are not lost"
    );
    let names: Vec<String> = landed(&h.transport)
        .await
        .iter()
        .map(|(_, v)| {
            v["fullDocument"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    assert_eq!(names, ["host-1", "host-2"]);
}

#[tokio::test]
async fn a_mongodb_keyset_tail_follows_id_on_a_standalone_server_across_a_restart() {
    let Some(db) = common::acquire_mongo("db-mongo-keyset", false).await else {
        return;
    };
    let uri = common::mongo_uri(&db, false);
    let coll = collection(&uri).await;
    coll.insert_many(hosts(5)).await.expect("seed");

    // The default tail on a standalone server is refused naming the fallback.
    let yaml = instance_yaml(
        &uri,
        "  - { unit: changes, shape: tail, limit: 2, mongodb: { database: inventory, collection: hosts } }\n",
    );
    let h = harness(&yaml);
    let instance: DbInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let shape = DbShape::from_instance(&instance, "inv", &Counting::new().as_lease()).unwrap();
    let err = driver(&h, shape, None)
        .run_tick(None)
        .await
        .expect_err("no oplog on a standalone server");
    assert!(
        err.to_string().contains("mongodb.tail: keyset"),
        "the refusal names the fallback as the grammar spells it: {err}"
    );

    let yaml = instance_yaml(
        &uri,
        "  - { unit: rows, shape: tail, limit: 2, max_pages_per_tick: 1, mongodb: { database: inventory, collection: hosts, tail: keyset, filter: { n: { $ne: 3 } } } }\n",
    );
    let h = harness(&yaml);
    let instance: DbInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let (_dir, store) = file_store();
    let build = || {
        let shape = DbShape::from_instance(&instance, "inv", &Counting::new().as_lease()).unwrap();
        driver(&h, shape, Some(Arc::clone(&store)))
    };

    let first = build();
    assert_eq!(first.run_tick(None).await.expect("tick 1").rows, 2);
    let cursor = store
        .get("inst.inv.rows")
        .await
        .unwrap()
        .expect("committed after the acks");
    let Some(CheckpointValue::Keyset(id)) = cursor.checkpoint() else {
        panic!("a keyset checkpoint holding the `_id`");
    };
    assert!(
        id[0]["$oid"].as_str().is_some_and(|oid| oid.len() == 24),
        "the checkpoint is the ObjectId in extended JSON: {id:?}"
    );
    assert_eq!(
        first.run_tick(None).await.expect("tick 2").rows,
        2,
        "the other two of the four documents the filter keeps"
    );

    // Rows appended after the checkpoint, then a restart.
    coll.insert_many(hosts(2).into_iter().map(|mut d| {
        d.insert("wave", 2);
        d
    }))
    .await
    .expect("append");
    let second = build();
    assert_eq!(second.run_tick(None).await.expect("tick 3").rows, 2);
    assert_eq!(
        second.run_tick(None).await.expect("tick 4").rows,
        0,
        "nothing past the last _id"
    );

    let frames = landed(&h.transport).await;
    assert!(frames.iter().all(|(topic, _)| topic == "inventory_land"));
    let seen: Vec<(u64, u64)> = frames
        .iter()
        .map(|(_, v)| (v["wave"].as_u64().unwrap_or(1), v["n"].as_u64().unwrap()))
        .collect();
    assert_eq!(
        seen,
        [(1, 1), (1, 2), (1, 4), (1, 5), (2, 1), (2, 2)],
        "every document once, in _id order, the filter applied, across the restart"
    );
    assert!(frames.iter().all(|(_, v)| v["_id"]["$oid"].is_string()));
}
