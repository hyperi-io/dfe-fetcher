// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/framework.rs
// Purpose:   The driver end to end over scalo's in-process transport: envelope, filter, oversize, gate, checkpoint
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The driver loop over a scripted shape and scalo's memory transport.
//!
//! Every test builds a real `PipelineState` whose output is the in-process
//! transport, runs one tick of a `Driver` over a scripted `RowSource`, and
//! reads back what landed. The memory guard and pressure latch are the
//! driver's own, pinned to reservation accounting so a test decides the
//! pressure.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use scalo::governor::{Hysteresis, MemoryPressureSource, PressureSource, UnifiedPressure};
use scalo::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};
use scalo::transport::{MemoryConfig, MemoryTransport, TransportBase, TransportReceiver};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use dfe_fetcher::config::{Config, SharedConfig};
use dfe_fetcher::driver::{Driver, DriverParts, Shape};
use dfe_fetcher::emit::Emitter;
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::output::OutputManager;
use dfe_fetcher::pipeline::PipelineState;
use dfe_fetcher_core::FetchWindow;
use dfe_fetcher_core::batch::AccumulateConfig;
use dfe_fetcher_core::checkpoint::CursorStore;
use dfe_fetcher_core::envelope::{Envelope, Kind, OversizePolicy, Reassembler};
use dfe_fetcher_core::{
    Mark, Row, RowSource, RowStream, SnapshotScope, SourceMaturity, TickCtx, UnitShape, UnitSpec,
};

/// A shape whose rows are decided by the test.
struct Scripted {
    units: Vec<UnitSpec>,
    rows: Vec<Row>,
    polled: Arc<AtomicUsize>,
}

impl RowSource for Scripted {
    fn name(&self) -> &'static str {
        "scripted"
    }
    fn maturity(&self) -> SourceMaturity {
        SourceMaturity::Alpha
    }
    fn units(&self) -> &[UnitSpec] {
        &self.units
    }
    fn rows<'a>(&'a self, _tick: TickCtx<'a>) -> RowStream<'a> {
        let polled = Arc::clone(&self.polled);
        futures::stream::iter(self.rows.clone().into_iter().map(Ok))
            .inspect(move |_| {
                polled.fetch_add(1, Ordering::SeqCst);
            })
            .boxed()
    }
    fn probe(&self) -> BoxFuture<'_, dfe_fetcher_core::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct Harness {
    state: Arc<PipelineState>,
    transport: Arc<MemoryTransport>,
    metrics: Arc<Metrics>,
    guard: Arc<MemoryGuard>,
    shared: SharedConfig,
}

fn harness(config: Config, buffer_size: usize) -> Harness {
    harness_over(
        config,
        &MemoryConfig {
            buffer_size,
            ..MemoryConfig::default()
        },
    )
}

/// A harness whose in-process transport is configured by the test (its
/// buffer, its outbound filters).
fn harness_over(config: Config, memory: &MemoryConfig) -> Harness {
    let transport = Arc::new(MemoryTransport::new(memory).expect("memory transport"));
    let shared = SharedConfig::new(config);
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
    let guard = Arc::new(MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: 1000,
            ..MemoryGuardConfig::default()
        },
        UsageSource::Reservations,
    ));
    Harness {
        state,
        transport,
        metrics,
        guard,
        shared,
    }
}

fn base_config() -> Config {
    let mut config = Config::default();
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config
}

fn pressure_over(guard: &Arc<MemoryGuard>) -> Arc<UnifiedPressure> {
    let sources: Vec<Arc<dyn PressureSource>> =
        vec![Arc::new(MemoryPressureSource::new(Arc::clone(guard))) as Arc<dyn PressureSource>];
    Arc::new(UnifiedPressure::new(
        sources,
        Hysteresis::new(0.8, 0.5).expect("band"),
    ))
}

#[allow(clippy::too_many_arguments)]
fn driver(
    h: &Harness,
    shape: Scripted,
    accumulate: AccumulateConfig,
    oversize: OversizePolicy,
    pressure: Option<Arc<UnifiedPressure>>,
    checkpoints: Option<Arc<dyn CursorStore>>,
    shutdown: CancellationToken,
) -> Driver {
    Driver::new(DriverParts {
        shape: Shape::Custom(Box::new(shape)),
        connection_id: "conn".into(),
        instance_id: "inst".into(),
        shared_config: h.shared.clone(),
        accumulate,
        oversize,
        emitter: Emitter::new(
            Arc::clone(&h.state),
            Arc::clone(&h.metrics),
            accumulate.in_flight,
        ),
        pressure,
        memory_guard: Arc::clone(&h.guard),
        checkpoints,
        metrics: Arc::clone(&h.metrics),
        shutdown,
    })
}

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

fn rows(payloads: &[&str]) -> Vec<Row> {
    payloads.iter().map(|p| Row::new(p.to_string())).collect()
}

fn small() -> AccumulateConfig {
    AccumulateConfig {
        max_rows: 1000,
        max_bytes: 8 * 1024 * 1024,
        window_ms: 1000,
        in_flight: 4,
    }
}

#[tokio::test]
async fn a_dump_lands_in_the_envelope_with_the_filter_applied_before_the_buffer() {
    let mut config = base_config();
    config.sources.rest.insert(
        "conn".into(),
        serde_yaml_ng::from_str("profile: x\ntopic: t\nfilter: 'record.alive == true'\n").unwrap(),
    );
    let h = harness(config, 100);
    let shape = Scripted {
        units: vec![UnitSpec::new("assets", UnitShape::Dump, "fixture-assets")],
        rows: rows(&[
            r#"{"id":"a","alive":true}"#,
            r#"{"id":"b","alive":false}"#,
            r#"{"id":"c","alive":true}"#,
        ]),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        None,
        CancellationToken::new(),
    );
    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 2, "seq and row_count count emitted rows");
    assert_eq!(report.filtered, 1);

    let frames = landed(&h.transport).await;
    let topics: Vec<&str> = frames.iter().map(|(t, _)| t.as_str()).collect();
    assert!(
        topics.iter().all(|t| *t == "fixture-assets_land"),
        "{topics:?}"
    );
    let kinds: Vec<&str> = frames
        .iter()
        .map(|(_, v)| v["kind"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        ["begin", "row", "row", "end"],
        "markers are never filtered"
    );
    assert_eq!(frames[1].1["seq"], 0);
    assert_eq!(frames[2].1["seq"], 1);
    assert_eq!(frames[2].1["record"]["id"], "c");
    assert_eq!(frames[3].1["row_count"], 2);
    assert_eq!(frames[3].1["seq"], 2);
    for (_, frame) in &frames {
        assert_eq!(frame["_source"], "fixture-assets");
        assert_eq!(frame["_source_fetcher"], "conn.assets");
        assert!(frame["_timestamp_fetcher"].is_number());
        assert_eq!(frame["store"], "conn.assets");
        assert_eq!(frame["timestamp"], frame["snapshot_at"]);
    }
    let mut asm = Reassembler::default();
    let mut id = None;
    for (_, frame) in &frames {
        asm.offer(serde_json::to_vec(frame).unwrap().as_slice())
            .unwrap();
        id = Some(
            serde_json::from_value::<Envelope>(frame.clone())
                .unwrap()
                .head()
                .snapshot_id,
        );
    }
    let rebuilt = asm.complete(id.unwrap()).expect("complete snapshot");
    assert_eq!(rebuilt.len(), 2);
    assert!(h.metrics.render().contains("dfe_records_filtered_total 1"));
    assert!(h.metrics.render().contains("dfe_records_delivered_total 4"));
}

#[tokio::test]
async fn an_oversize_row_becomes_a_stub_with_a_truncated_dlq_copy_and_the_metric() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // The recorder takes a global slot, so a second install in one process fails.
    let _ = recorder.install();

    let dlq_dir = tempfile::TempDir::new().unwrap();
    let mut config = base_config();
    config.dlq.enabled = true;
    config.dlq.mode = scalo::dlq::DlqMode::FileOnly;
    config.dlq.file.path = dlq_dir.path().to_path_buf();
    config.dlq.flush_interval_ms = 10;
    let h = harness(config, 100);
    let big = format!(r#"{{"id":"big","blob":"{}"}}"#, "x".repeat(200));
    let shape = Scripted {
        units: vec![UnitSpec {
            row_key: Some("/id".into()),
            // The unit name alone decides the `store` label, and another test
            // here emits this counter for a unit named `assets`.
            ..UnitSpec::new("dlqassets", UnitShape::Dump, "fixture-assets")
        }],
        rows: rows(&[r#"{"id":"a"}"#, big.as_str(), r#"{"id":"c"}"#]),
        polled: Arc::default(),
    };
    let policy = OversizePolicy {
        max_record_bytes: 64,
        max_dlq_bytes: 16,
    };
    let d = driver(
        &h,
        shape,
        small(),
        policy,
        None,
        None,
        CancellationToken::new(),
    );
    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 3, "the stub counts as an emitted row");
    assert_eq!(report.oversize, 1);

    let frames = landed(&h.transport).await;
    let seqs: Vec<u64> = frames[1..4]
        .iter()
        .map(|(_, v)| v["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, [0, 1, 2], "seq stays monotonic across the stub");
    assert_eq!(frames[2].1["kind"], "oversize");
    assert_eq!(frames[2].1["row_key"], "big");
    assert_eq!(frames[2].1["bytes"].as_u64().unwrap() as usize, big.len());
    assert_eq!(frames[4].1["row_count"], 3);

    let mut copy = None;
    for _ in 0..50 {
        copy = dlq_entries(dlq_dir.path()).into_iter().next();
        if copy.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let entry = copy.expect("a truncated copy reached the dead-letter queue");
    assert_eq!(
        entry.payload.len(),
        16,
        "the DLQ copy is cut at max_dlq_bytes"
    );
    assert_eq!(&entry.payload, &big.as_bytes()[..16]);
    assert_eq!(entry.destination.as_deref(), Some("fixture-assets_land"));
    assert!(entry.reason.contains("max_record_bytes"));

    // Other tests here emit this counter into the same process-wide recorder.
    let snapshot = snapshotter.snapshot().into_vec();
    let oversize = snapshot
        .iter()
        .find(|(key, _, _, _)| {
            key.key().name() == "dfe_fetcher_snapshot_rows_oversize_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "store" && l.value() == "conn.dlqassets")
        })
        .expect("the oversize counter was emitted for this store");
    assert!(
        matches!(oversize.3, metrics_util::debugging::DebugValue::Counter(1)),
        "{:?}",
        oversize.3
    );
}

fn item_rows(keyed: &[(&str, &str)]) -> Vec<Row> {
    keyed
        .iter()
        .enumerate()
        .map(|(i, (key, payload))| Row {
            payload: Bytes::from(payload.to_string()),
            mark: Some(Mark::Item {
                key: (*key).into(),
                position: chrono::Utc::now() + chrono::Duration::seconds(i64::try_from(i).unwrap()),
            }),
        })
        .collect()
}

/// A dump unit scoped per item (a file dump) opens a snapshot when an item's
/// first row arrives and closes it, commits it and moves on when the key
/// changes; `seq` and `row_count` are the item's own.
#[tokio::test]
async fn a_per_item_dump_emits_one_snapshot_per_item_key_and_commits_each_as_it_closes() {
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    let h = harness(base_config(), 100);
    let shape = Scripted {
        units: vec![UnitSpec {
            snapshot_scope: SnapshotScope::Item,
            ..UnitSpec::new("files", UnitShape::Dump, "fixture-files")
        }],
        rows: item_rows(&[
            ("one.jsonl", r#"{"id":"a"}"#),
            ("one.jsonl", r#"{"id":"b"}"#),
            ("two.jsonl", r#"{"id":"c"}"#),
        ]),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        Some(Arc::clone(&store)),
        CancellationToken::new(),
    );
    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 3);
    assert_eq!(report.flushes, 2, "one flush closes each item");

    let frames = landed(&h.transport).await;
    let kinds: Vec<&str> = frames
        .iter()
        .map(|(_, v)| v["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["begin", "row", "row", "end", "begin", "row", "end"]);
    assert_eq!(frames[1].1["seq"], 0);
    assert_eq!(frames[2].1["seq"], 1);
    assert_eq!(frames[3].1["row_count"], 2);
    assert_eq!(frames[5].1["seq"], 0, "seq restarts with the item");
    assert_eq!(frames[6].1["row_count"], 1);
    let ids: Vec<uuid::Uuid> = frames
        .iter()
        .filter(|(_, v)| v["kind"] == "begin")
        .map(|(_, v)| {
            serde_json::from_value::<Envelope>(v.clone())
                .unwrap()
                .head()
                .snapshot_id
        })
        .collect();
    assert_ne!(ids[0], ids[1], "two items, two snapshot ids");
    let mut asm = Reassembler::default();
    for (_, frame) in &frames {
        asm.offer(serde_json::to_vec(frame).unwrap().as_slice())
            .unwrap();
    }
    assert_eq!(asm.complete(ids[0]).expect("item one").len(), 2);
    assert_eq!(asm.complete(ids[1]).expect("item two").len(), 1);
    let cursor = store
        .get("inst.conn.files")
        .await
        .unwrap()
        .expect("committed");
    assert!(
        matches!(cursor.checkpoint(), Some(dfe_fetcher_core::CheckpointValue::Item { ref key, .. }) if key == "two.jsonl")
    );
}

/// The per-item scope is a contract with the shape: every row names its
/// item. A row that does not is a shape defect, so the tick aborts rather
/// than guess which snapshot the row belongs to.
#[tokio::test]
async fn a_per_item_dump_refuses_a_row_without_an_item_mark() {
    let h = harness(base_config(), 100);
    let shape = Scripted {
        units: vec![UnitSpec {
            snapshot_scope: SnapshotScope::Item,
            ..UnitSpec::new("files", UnitShape::Dump, "fixture-files")
        }],
        rows: rows(&[r#"{"id":"a"}"#]),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        None,
        CancellationToken::new(),
    );
    let err = d.run_tick(None).await.expect_err("an unmarked row");
    assert!(err.to_string().contains("item mark"), "{err}");
    assert!(
        landed(&h.transport).await.is_empty(),
        "nothing was opened for the row"
    );
}

/// The deployment's `unwrap_nested_json` applies to framework rows as it does
/// to the legacy delivery path: a string field holding serialised JSON lands
/// as the parsed value unless the deployment turns the unwrap off. For a
/// dump the provider's row is unwrapped inside the envelope's `record`.
#[tokio::test]
async fn stringified_json_fields_are_unwrapped_per_the_deployment_setting() {
    let stringified = r#"{"id":"u1","payload":"{\"inner\":true,\"n\":[1,2]}","note":"{not json"}"#;
    let expected_payload = serde_json::json!({"inner": true, "n": [1, 2]});

    let h = harness(base_config(), 100);
    let shape = Scripted {
        units: vec![UnitSpec::new("events", UnitShape::Incremental, "fixture")],
        rows: rows(&[stringified]),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        None,
        CancellationToken::new(),
    );
    d.run_tick(None).await.expect("tick");
    let frames = landed(&h.transport).await;
    assert_eq!(frames[0].1["payload"], expected_payload, "on by default");
    assert_eq!(frames[0].1["note"], "{not json");

    let mut off = base_config();
    off.unwrap_nested_json = false;
    let h = harness(off, 100);
    let shape = Scripted {
        units: vec![UnitSpec::new("events", UnitShape::Incremental, "fixture")],
        rows: rows(&[stringified]),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        None,
        CancellationToken::new(),
    );
    d.run_tick(None).await.expect("tick");
    let frames = landed(&h.transport).await;
    assert_eq!(
        frames[0].1["payload"], "{\"inner\":true,\"n\":[1,2]}",
        "off: the string stays a string"
    );

    let h = harness(base_config(), 100);
    let shape = Scripted {
        units: vec![UnitSpec::new("assets", UnitShape::Dump, "fixture-assets")],
        rows: rows(&[stringified]),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        None,
        CancellationToken::new(),
    );
    d.run_tick(None).await.expect("tick");
    let frames = landed(&h.transport).await;
    assert_eq!(frames[1].1["kind"], "row");
    assert_eq!(
        frames[1].1["record"]["payload"], expected_payload,
        "a dump row is unwrapped inside the envelope"
    );
}

/// A binary unit's rows land byte for byte: no enrichment spliced before
/// the last `}` byte, no unwrap, no filter, no route, and the JSON path of
/// the same tick is unchanged. The bytes are an OTLP export request whose
/// `{Count}` unit carries the `0x7D` the enricher used to splice on.
#[tokio::test]
async fn a_binary_unit_lands_byte_for_byte_and_a_json_unit_is_still_enriched() {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, ResourceMetrics, ScopeMetrics, metric,
    };
    use prost::Message;

    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "DiskReadOps".to_owned(),
                    description: String::new(),
                    unit: "{Count}".to_owned(),
                    metadata: Vec::new(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: Vec::new(),
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let otlp = Bytes::from(request.encode_to_vec());
    assert!(
        otlp.contains(&b'}'),
        "the fixture carries the byte the enricher splices on"
    );

    /// Each unit's own rows, so the protobuf reaches only the binary unit.
    struct PerUnit {
        units: Vec<UnitSpec>,
        rows: Vec<Vec<Row>>,
    }

    impl RowSource for PerUnit {
        fn name(&self) -> &'static str {
            "per-unit"
        }
        fn maturity(&self) -> SourceMaturity {
            SourceMaturity::Alpha
        }
        fn units(&self) -> &[UnitSpec] {
            &self.units
        }
        fn rows<'a>(&'a self, tick: TickCtx<'a>) -> RowStream<'a> {
            let at = self
                .units
                .iter()
                .position(|u| u.name == tick.unit.name)
                .expect("a known unit");
            futures::stream::iter(self.rows[at].clone().into_iter().map(Ok)).boxed()
        }
        fn probe(&self) -> BoxFuture<'_, dfe_fetcher_core::Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    let mut config = base_config();
    config.sources.rest.insert(
        "conn".into(),
        serde_yaml_ng::from_str("profile: x\ntopic: t\nfilter: 'alive == true'\n").unwrap(),
    );
    let h = harness(config, 100);
    let shape = PerUnit {
        units: vec![
            UnitSpec {
                content: dfe_fetcher_core::RowContent::Binary,
                ..UnitSpec::new("metrics", UnitShape::Incremental, "fixture-metrics")
            },
            UnitSpec::new("events", UnitShape::Incremental, "fixture-events"),
        ],
        rows: vec![
            vec![Row::new(otlp.clone())],
            vec![
                Row::new(r#"{"id":"a","alive":true}"#),
                Row::new(r#"{"id":"b","alive":false}"#),
            ],
        ],
    };
    let d = Driver::new(DriverParts {
        shape: Shape::Custom(Box::new(shape)),
        connection_id: "conn".into(),
        instance_id: "inst".into(),
        shared_config: h.shared.clone(),
        accumulate: small(),
        oversize: OversizePolicy::default(),
        emitter: Emitter::new(Arc::clone(&h.state), Arc::clone(&h.metrics), 4),
        pressure: None,
        memory_guard: Arc::clone(&h.guard),
        checkpoints: None,
        metrics: Arc::clone(&h.metrics),
        shutdown: CancellationToken::new(),
    });
    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 2, "the protobuf and the kept JSON row");
    assert_eq!(report.filtered, 1, "the JSON unit is still filtered");

    let batch = h.transport.recv(1000).await.expect("recv");
    let raw: Vec<(String, Bytes)> = batch
        .records
        .into_iter()
        .map(|r| (r.key.as_deref().unwrap_or("").to_owned(), r.payload))
        .collect();
    let metrics: Vec<&Bytes> = raw
        .iter()
        .filter(|(t, _)| t == "fixture-metrics_land")
        .map(|(_, p)| p)
        .collect();
    assert_eq!(metrics.len(), 1);
    assert_eq!(
        metrics[0], &otlp,
        "the bytes out are the bytes in, `}}` byte included"
    );
    let decoded = ExportMetricsServiceRequest::decode(metrics[0].as_ref()).expect("still OTLP");
    assert_eq!(
        decoded.resource_metrics[0].scope_metrics[0].metrics[0].unit,
        "{Count}"
    );
    assert!(
        !metrics[0].windows(7).any(|w| w == b"_source"),
        "no identity is spliced into a binary row"
    );
    let events: Vec<Value> = raw
        .iter()
        .filter(|(t, _)| t == "fixture-events_land")
        .map(|(_, p)| serde_json::from_slice(p).expect("the JSON unit still lands as JSON"))
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["id"], "a");
    assert_eq!(events[0]["_source"], "fixture-events");
    assert_eq!(events[0]["_source_fetcher"], "conn.events");
    assert!(events[0]["_timestamp_fetcher"].is_number());
}

fn dlq_entries(dir: &Path) -> Vec<scalo::dlq::DlqEntry> {
    let mut out = Vec::new();
    let Ok(walk) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in walk.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(dlq_entries(&path));
        } else if let Ok(text) = std::fs::read_to_string(&path) {
            out.extend(text.lines().filter_map(|l| serde_json::from_str(l).ok()));
        }
    }
    out
}

#[tokio::test]
async fn the_gate_pauses_polling_under_pressure_and_resumes_on_drain() {
    let h = harness(base_config(), 100);
    let polled = Arc::new(AtomicUsize::new(0));
    let shape = Scripted {
        units: vec![UnitSpec::new("events", UnitShape::Incremental, "fixture")],
        rows: rows(&[r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#]),
        polled: Arc::clone(&polled),
    };
    let pressure = pressure_over(&h.guard);
    let d = Arc::new(driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        Some(pressure),
        None,
        CancellationToken::new(),
    ));
    // 900 of 1000 bytes reserved elsewhere: 0.9 >= pause_above, the gate holds.
    h.guard.add_bytes(900);
    let tick = {
        let d = Arc::clone(&d);
        tokio::spawn(async move { d.run_tick(None).await })
    };
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        polled.load(Ordering::SeqCst),
        0,
        "nothing is pulled while held"
    );
    assert!(!tick.is_finished());
    // Drain: 0 bytes held, 0.0 <= resume_below, the gate opens.
    h.guard.release(900);
    let report = tokio::time::timeout(Duration::from_secs(5), tick)
        .await
        .expect("the tick resumes once pressure drops")
        .expect("join")
        .expect("tick");
    assert_eq!(report.rows, 3);
    assert_eq!(polled.load(Ordering::SeqCst), 3);
    assert_eq!(landed(&h.transport).await.len(), 3);
    assert_eq!(
        h.guard.reserved_bytes(),
        0,
        "every lease released after emit"
    );
}

/// A shape fed row by row from the test over a channel, so the test decides
/// when the next row is available.
struct Fed {
    units: Vec<UnitSpec>,
    rx: tokio::sync::Mutex<Option<tokio::sync::mpsc::Receiver<Row>>>,
}

impl RowSource for Fed {
    fn name(&self) -> &'static str {
        "fed"
    }
    fn maturity(&self) -> SourceMaturity {
        SourceMaturity::Alpha
    }
    fn units(&self) -> &[UnitSpec] {
        &self.units
    }
    fn rows<'a>(&'a self, _tick: TickCtx<'a>) -> RowStream<'a> {
        let rx = self
            .rx
            .try_lock()
            .expect("one tick at a time")
            .take()
            .expect("one tick only");
        tokio_stream::wrappers::ReceiverStream::new(rx)
            .map(Ok)
            .boxed()
    }
    fn probe(&self) -> BoxFuture<'_, dfe_fetcher_core::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn a_held_batch_is_flushed_so_memory_drains_while_the_source_waits() {
    let h = harness(base_config(), 100);
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let shape = Fed {
        units: vec![UnitSpec::new("events", UnitShape::Incremental, "fixture")],
        rx: tokio::sync::Mutex::new(Some(rx)),
    };
    let pressure = pressure_over(&h.guard);
    let mut acc = small();
    acc.window_ms = 60_000;
    let d = Arc::new(Driver::new(DriverParts {
        shape: Shape::Custom(Box::new(shape)),
        connection_id: "conn".into(),
        instance_id: "inst".into(),
        shared_config: h.shared.clone(),
        accumulate: acc,
        oversize: OversizePolicy::default(),
        emitter: Emitter::new(Arc::clone(&h.state), Arc::clone(&h.metrics), 4),
        pressure: Some(pressure),
        memory_guard: Arc::clone(&h.guard),
        checkpoints: None,
        metrics: Arc::clone(&h.metrics),
        shutdown: CancellationToken::new(),
    }));
    let tick = {
        let d = Arc::clone(&d);
        tokio::spawn(async move { d.run_tick(None).await })
    };
    // The first row is admitted and sits in the batch under the long window.
    tx.send(Row::new(r#"{"n":1}"#)).await.unwrap();
    let mut waited = 0;
    while h.guard.reserved_bytes() == 0 && waited < 50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        waited += 1;
    }
    assert!(
        h.guard.reserved_bytes() > 0,
        "the buffered row leased its bytes"
    );
    // The driver is parked on the next row. Pressure rises; the second row
    // completes that poll, and the admission check before the third holds:
    // the buffer is flushed so its lease drains while the source is not polled.
    h.guard.add_bytes(900);
    tx.send(Row::new(r#"{"n":2}"#)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(!tick.is_finished(), "held before the third row");
    let flushed = h.transport.recv(10).await.expect("recv").records.len();
    assert_eq!(flushed, 2, "the buffered rows were flushed while held");
    assert_eq!(
        h.guard.reserved_bytes(),
        900,
        "only the test's own reservation remains"
    );
    tx.send(Row::new(r#"{"n":3}"#)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        h.transport.recv(10).await.expect("recv").records.is_empty(),
        "nothing is pulled while held"
    );
    h.guard.release(900);
    drop(tx);
    let report = tokio::time::timeout(Duration::from_secs(5), tick)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.rows, 3);
    assert_eq!(
        h.transport.recv(10).await.expect("recv").records.len(),
        1,
        "the third row followed after resume"
    );
}

#[tokio::test]
async fn the_checkpoint_is_written_after_the_emit_and_not_when_the_emit_fails() {
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    let h = harness(base_config(), 100);
    let with_marks = |ids: &[&str]| -> Vec<Row> {
        ids.iter()
            .map(|id| Row {
                payload: Bytes::from(format!(r#"{{"id":"{id}"}}"#)),
                mark: Some(Mark::Item {
                    key: (*id).into(),
                    position: chrono::Utc::now(),
                }),
            })
            .collect()
    };
    let shape = Scripted {
        units: vec![UnitSpec::new("blobs", UnitShape::Incremental, "fixture")],
        rows: with_marks(&["b1", "b2"]),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        Some(Arc::clone(&store)),
        CancellationToken::new(),
    );
    assert!(
        store.get("inst.conn.blobs").await.unwrap().is_none(),
        "nothing before the tick"
    );
    d.run_tick(None).await.expect("tick");
    let cursor = store
        .get("inst.conn.blobs")
        .await
        .unwrap()
        .expect("committed after ack");
    assert_eq!(cursor.version, 2);
    assert_eq!(cursor.last_fetch_records, 2);
    let checkpoint = cursor.checkpoint().expect("decodes");
    assert!(
        matches!(checkpoint, dfe_fetcher_core::CheckpointValue::Item { ref key, .. } if key == "b2")
    );
    assert_eq!(landed(&h.transport).await.len(), 2);

    // A closed transport fails every send; with no DLQ the emit errors and the
    // unit's checkpoint must stay where it was.
    h.transport.close().await.unwrap();
    let shape = Scripted {
        units: vec![UnitSpec::new("blobs", UnitShape::Incremental, "fixture")],
        rows: with_marks(&["b3"]),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        Some(Arc::clone(&store)),
        CancellationToken::new(),
    );
    let err = d.run_tick(None).await.expect_err("emit failed");
    assert!(matches!(err, dfe_fetcher::Error::Transport(_)), "{err:?}");
    let cursor = store.get("inst.conn.blobs").await.unwrap().unwrap();
    assert!(
        matches!(cursor.checkpoint(), Some(dfe_fetcher_core::CheckpointValue::Item { ref key, .. }) if key == "b2"),
        "b3 was not committed"
    );
}

/// A config whose dead-letter queue is a file under `dir`, flushed fast.
fn file_dlq_config(dir: &Path) -> Config {
    let mut config = base_config();
    config.dlq.enabled = true;
    config.dlq.mode = scalo::dlq::DlqMode::FileOnly;
    config.dlq.file.path = dir.to_path_buf();
    config.dlq.flush_interval_ms = 10;
    config
}

/// Rows carrying an item mark per id.
fn item_marked(ids: &[&str], blob: usize) -> Vec<Row> {
    ids.iter()
        .map(|id| Row {
            payload: Bytes::from(format!(r#"{{"id":"{id}","blob":"{}"}}"#, "x".repeat(blob))),
            mark: Some(Mark::Item {
                key: (*id).into(),
                position: chrono::Utc::now(),
            }),
        })
        .collect()
}

/// A transport that is down for every record (closed, timed out, the topic
/// or broker gone) is not a per-record failure: with the dead-letter queue
/// ENABLED nothing is dead-lettered, the tick aborts and the checkpoint stays
/// where it was, so the rows are re-fetched rather than parked.
#[tokio::test]
async fn a_transport_wide_failure_aborts_the_tick_with_no_checkpoint_and_no_dead_letter() {
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    let dlq_dir = tempfile::TempDir::new().unwrap();
    let h = harness(file_dlq_config(dlq_dir.path()), 100);
    h.transport.close().await.unwrap();
    let shape = Scripted {
        units: vec![UnitSpec::new("blobs", UnitShape::Incremental, "fixture")],
        rows: item_marked(&["b1", "b2"], 0),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        Some(Arc::clone(&store)),
        CancellationToken::new(),
    );
    let err = d.run_tick(None).await.expect_err("the transport is down");
    assert!(matches!(err, dfe_fetcher::Error::Transport(_)), "{err:?}");
    assert!(
        store.get("inst.conn.blobs").await.unwrap().is_none(),
        "no checkpoint advanced past rows the transport never took"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        dlq_entries(dlq_dir.path()).is_empty(),
        "a transport-wide failure dead-letters nothing"
    );
    assert!(
        !h.metrics.render().contains("dfe_messages_dlq_total 1"),
        "{}",
        h.metrics.render()
    );
}

/// A record the transport refuses for what it is (here scalo's outbound
/// filter says DLQ, as a broker's MessageSizeTooLarge does) is dead-lettered
/// WHOLE -- truncation is the oversize policy, not the transport path -- and
/// the rows around it land and commit.
#[tokio::test]
async fn a_record_the_transport_refuses_is_dead_lettered_whole_and_the_rest_commit() {
    use scalo::transport::filter::{FilterAction, FilterRule};

    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    let dlq_dir = tempfile::TempDir::new().unwrap();
    let h = harness_over(
        file_dlq_config(dlq_dir.path()),
        &MemoryConfig {
            buffer_size: 100,
            filters_out: vec![FilterRule {
                expression: r#"id == "poison""#.into(),
                action: FilterAction::Dlq,
            }],
            ..MemoryConfig::default()
        },
    );
    // A 100 KiB record: under max_record_bytes, over max_dlq_bytes.
    let blob = 100 * 1024;
    let shape = Scripted {
        units: vec![UnitSpec::new("blobs", UnitShape::Incremental, "fixture")],
        rows: item_marked(&["b1", "poison", "b3"], blob),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        Some(Arc::clone(&store)),
        CancellationToken::new(),
    );
    let report = d.run_tick(None).await.expect("the batch goes on");
    assert_eq!(report.rows, 3);
    let ids: Vec<String> = landed(&h.transport)
        .await
        .iter()
        .map(|(_, v)| v["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        ids,
        ["b1", "b3"],
        "the refused record is the only one missing"
    );
    let cursor = store
        .get("inst.conn.blobs")
        .await
        .unwrap()
        .expect("committed");
    assert!(
        matches!(cursor.checkpoint(), Some(dfe_fetcher_core::CheckpointValue::Item { ref key, .. }) if key == "b3"),
        "{cursor:?}"
    );

    let mut copy = None;
    for _ in 0..50 {
        copy = dlq_entries(dlq_dir.path()).into_iter().next();
        if copy.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let entry = copy.expect("the refused record reached the dead-letter queue");
    let parked: Value =
        serde_json::from_slice(&entry.payload).expect("the copy is the whole record");
    assert_eq!(parked["id"], "poison");
    assert_eq!(
        parked["blob"].as_str().map(str::len),
        Some(blob),
        "the copy is not cut at max_dlq_bytes"
    );
    assert_eq!(parked["_source_fetcher"], "conn.blobs");
    assert_eq!(entry.destination.as_deref(), Some("fixture_land"));
    assert!(entry.reason.contains("refused"), "{}", entry.reason);
    assert_eq!(dlq_entries(dlq_dir.path()).len(), 1);
}

/// A row the filter drops is consumed, not undelivered: a keyset unit whose
/// every row of the tick is filtered still commits the last key, or the next
/// tick would bind the same key and drop the same rows for ever.
#[tokio::test]
async fn a_keyset_unit_whose_rows_are_all_filtered_still_commits_the_last_key() {
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    let mut config = base_config();
    config.sources.rest.insert(
        "conn".into(),
        serde_yaml_ng::from_str("profile: x\ntopic: t\nfilter: 'level == \"ERROR\"'\n").unwrap(),
    );
    let h = harness(config, 100);
    let shape = Scripted {
        units: vec![UnitSpec::new("events", UnitShape::Incremental, "fixture")],
        rows: (1..=3)
            .map(|i| Row {
                payload: Bytes::from(format!(r#"{{"id":{i},"level":"INFO"}}"#)),
                mark: Some(Mark::Keyset(smallvec::smallvec![Value::from(i)])),
            })
            .collect(),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        Some(Arc::clone(&store)),
        CancellationToken::new(),
    );
    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 0);
    assert_eq!(report.filtered, 3);
    assert_eq!(report.flushes, 0, "nothing was emitted");
    assert!(landed(&h.transport).await.is_empty());
    let cursor = store
        .get("inst.conn.events")
        .await
        .unwrap()
        .expect("the filtered rows were consumed and their key committed");
    assert_eq!(
        cursor.checkpoint(),
        Some(dfe_fetcher_core::CheckpointValue::Keyset(vec![
            Value::from(3)
        ]))
    );
}

/// A shape that records the ids it is asked to acknowledge.
struct Acking {
    inner: Scripted,
    acked: Arc<parking_lot::Mutex<Vec<Vec<Box<str>>>>>,
}

impl RowSource for Acking {
    fn name(&self) -> &'static str {
        "acking"
    }
    fn maturity(&self) -> SourceMaturity {
        SourceMaturity::Alpha
    }
    fn units(&self) -> &[UnitSpec] {
        self.inner.units()
    }
    fn rows<'a>(&'a self, tick: TickCtx<'a>) -> RowStream<'a> {
        self.inner.rows(tick)
    }
    fn ack<'a>(
        &'a self,
        _unit: &'a UnitSpec,
        ids: Vec<Box<str>>,
    ) -> BoxFuture<'a, dfe_fetcher_core::Result<()>> {
        self.acked.lock().push(ids);
        Box::pin(async { Ok(()) })
    }
    fn probe(&self) -> BoxFuture<'_, dfe_fetcher_core::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// A queue message the filter drops is still acknowledged after the tick,
/// even when it is the last row and the final batch is empty; otherwise the
/// broker redelivers it and the fetcher filters it again, for ever.
#[tokio::test]
async fn a_filtered_queue_message_is_still_acknowledged_after_the_tick() {
    let mut config = base_config();
    config.sources.rest.insert(
        "conn".into(),
        serde_yaml_ng::from_str("profile: x\ntopic: t\nfilter: 'severity == \"ERROR\"'\n").unwrap(),
    );
    let h = harness(config, 100);
    let acked = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let shape = Acking {
        inner: Scripted {
            units: vec![UnitSpec::new("sub", UnitShape::Incremental, "fixture")],
            rows: vec![
                Row {
                    payload: Bytes::from(r#"{"severity":"ERROR","n":1}"#),
                    mark: Some(Mark::Ack("ack-1".into())),
                },
                Row {
                    payload: Bytes::from(r#"{"severity":"NOTICE","n":2}"#),
                    mark: Some(Mark::Ack("ack-2".into())),
                },
            ],
            polled: Arc::default(),
        },
        acked: Arc::clone(&acked),
    };
    let d = Driver::new(DriverParts {
        shape: Shape::Custom(Box::new(shape)),
        connection_id: "conn".into(),
        instance_id: "inst".into(),
        shared_config: h.shared.clone(),
        accumulate: small(),
        oversize: OversizePolicy::default(),
        emitter: Emitter::new(Arc::clone(&h.state), Arc::clone(&h.metrics), 4),
        pressure: None,
        memory_guard: Arc::clone(&h.guard),
        checkpoints: None,
        metrics: Arc::clone(&h.metrics),
        shutdown: CancellationToken::new(),
    });
    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 1);
    assert_eq!(report.filtered, 1);
    assert_eq!(landed(&h.transport).await.len(), 1);
    let acked: Vec<Box<str>> = acked.lock().iter().flatten().cloned().collect();
    assert_eq!(
        acked,
        vec![Box::from("ack-1"), Box::from("ack-2")],
        "the delivered message and the filtered one are both acknowledged"
    );
}

/// An oversize row on a unit with no envelope is dead-lettered and dropped,
/// and its mark is still committed: the tail must not bind the same key
/// again and hit the same row every tick.
#[tokio::test]
async fn an_oversize_row_on_a_non_dump_unit_still_commits_its_mark() {
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    let h = harness(base_config(), 100);
    let big = format!(r#"{{"id":2,"blob":"{}"}}"#, "x".repeat(200));
    let shape = Scripted {
        units: vec![UnitSpec::new("events", UnitShape::Incremental, "fixture")],
        rows: vec![
            Row {
                payload: Bytes::from(r#"{"id":1}"#),
                mark: Some(Mark::Keyset(smallvec::smallvec![Value::from(1)])),
            },
            Row {
                payload: Bytes::from(big),
                mark: Some(Mark::Keyset(smallvec::smallvec![Value::from(2)])),
            },
        ],
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy {
            max_record_bytes: 64,
            max_dlq_bytes: 16,
        },
        None,
        Some(Arc::clone(&store)),
        CancellationToken::new(),
    );
    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 1);
    assert_eq!(report.oversize, 1);
    assert_eq!(landed(&h.transport).await.len(), 1);
    let cursor = store
        .get("inst.conn.events")
        .await
        .unwrap()
        .expect("committed");
    assert_eq!(
        cursor.checkpoint(),
        Some(dfe_fetcher_core::CheckpointValue::Keyset(vec![
            Value::from(2)
        ])),
        "the oversize row's key is passed, not the row before it"
    );
}

/// A per-item dump whose item is filtered whole still commits the item's
/// marker: the file was read, so the next tick must not re-list it and land
/// another empty snapshot for it.
#[tokio::test]
async fn a_per_item_dump_whose_item_is_fully_filtered_commits_the_item_marker() {
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    let mut config = base_config();
    config.sources.rest.insert(
        "conn".into(),
        serde_yaml_ng::from_str("profile: x\ntopic: t\nfilter: 'record.alive == true'\n").unwrap(),
    );
    let h = harness(config, 100);
    let shape = Scripted {
        units: vec![UnitSpec {
            snapshot_scope: SnapshotScope::Item,
            ..UnitSpec::new("files", UnitShape::Dump, "fixture-files")
        }],
        rows: item_rows(&[
            ("one.jsonl", r#"{"id":"a","alive":false}"#),
            ("one.jsonl", r#"{"id":"b","alive":false}"#),
        ]),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        Some(Arc::clone(&store)),
        CancellationToken::new(),
    );
    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 0);
    assert_eq!(report.filtered, 2);
    let frames = landed(&h.transport).await;
    let kinds: Vec<&str> = frames
        .iter()
        .map(|(_, v)| v["kind"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        ["begin", "end"],
        "the file's snapshot is honest: empty after the filter"
    );
    assert_eq!(frames[1].1["row_count"], 0);
    let cursor = store
        .get("inst.conn.files")
        .await
        .unwrap()
        .expect("the item was consumed and its marker committed");
    assert!(
        matches!(cursor.checkpoint(), Some(dfe_fetcher_core::CheckpointValue::Item { ref key, .. }) if key == "one.jsonl"),
        "{cursor:?}"
    );
}

/// The oversize stub is the envelope's own frame, like `begin` and `end`: a
/// filter written against `record.*` does not see it, so it lands, keeps
/// `seq` contiguous and is counted in `row_count`.
#[tokio::test]
async fn the_oversize_stub_bypasses_the_filter_and_lands() {
    let mut config = base_config();
    config.sources.rest.insert(
        "conn".into(),
        serde_yaml_ng::from_str("profile: x\ntopic: t\nfilter: 'record.alive == true'\n").unwrap(),
    );
    let h = harness(config, 100);
    let big = format!(
        r#"{{"id":"big","alive":true,"blob":"{}"}}"#,
        "x".repeat(200)
    );
    let shape = Scripted {
        units: vec![UnitSpec {
            row_key: Some("/id".into()),
            ..UnitSpec::new("assets", UnitShape::Dump, "fixture-assets")
        }],
        rows: rows(&[
            r#"{"id":"a","alive":true}"#,
            big.as_str(),
            r#"{"id":"c","alive":false}"#,
        ]),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy {
            max_record_bytes: 64,
            max_dlq_bytes: 16,
        },
        None,
        None,
        CancellationToken::new(),
    );
    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 2, "the row and the stub");
    assert_eq!(report.oversize, 1);
    assert_eq!(report.filtered, 1, "only the dead row is filtered");
    let frames = landed(&h.transport).await;
    let kinds: Vec<&str> = frames
        .iter()
        .map(|(_, v)| v["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["begin", "row", "oversize", "end"]);
    assert_eq!(frames[1].1["seq"], 0);
    assert_eq!(frames[2].1["seq"], 1);
    assert_eq!(frames[2].1["row_key"], "big");
    assert_eq!(frames[3].1["row_count"], 2);
}

/// A pause the drain cannot end (the held memory is not the buffer's) is
/// bounded by `self_regulation.max_hold_secs`: after it the driver polls on
/// and the tick completes, rather than holding for ever.
#[tokio::test]
async fn the_gate_hold_is_bounded_by_max_hold_secs() {
    let mut config = base_config();
    config.self_regulation.max_hold_secs = 1;
    let h = harness(config, 100);
    let polled = Arc::new(AtomicUsize::new(0));
    let shape = Scripted {
        units: vec![UnitSpec::new("events", UnitShape::Incremental, "fixture")],
        rows: rows(&[r#"{"n":1}"#, r#"{"n":2}"#]),
        polled: Arc::clone(&polled),
    };
    let pressure = pressure_over(&h.guard);
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        Some(pressure),
        None,
        CancellationToken::new(),
    );
    // Reserved by something the pause cannot drain; never released.
    h.guard.add_bytes(900);
    let started = std::time::Instant::now();
    let report = tokio::time::timeout(Duration::from_secs(10), d.run_tick(None))
        .await
        .expect("the hold is bounded, the tick completes")
        .expect("tick");
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "the hold lasted at least max_hold_secs"
    );
    assert_eq!(report.rows, 2);
    assert_eq!(polled.load(Ordering::SeqCst), 2);
    assert_eq!(landed(&h.transport).await.len(), 2);
    h.guard.release(900);
}

/// The window deadline bounds how long a partial batch waits: a slow source
/// (a queue long-poll, a sparse tail, a paginated API between pages) lands
/// what it has after `window_ms` rather than holding it to the end of the
/// tick. This is the path every slow unit takes and it bounds both latency
/// and the memory a partial batch holds.
#[tokio::test]
async fn the_window_deadline_flushes_a_partial_batch_before_the_next_row_arrives() {
    let h = harness(base_config(), 100);
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let shape = Fed {
        units: vec![UnitSpec::new("events", UnitShape::Incremental, "fixture")],
        rx: tokio::sync::Mutex::new(Some(rx)),
    };
    let mut acc = small();
    acc.window_ms = 200;
    let d = Arc::new(Driver::new(DriverParts {
        shape: Shape::Custom(Box::new(shape)),
        connection_id: "conn".into(),
        instance_id: "inst".into(),
        shared_config: h.shared.clone(),
        accumulate: acc,
        oversize: OversizePolicy::default(),
        emitter: Emitter::new(Arc::clone(&h.state), Arc::clone(&h.metrics), 4),
        pressure: None,
        memory_guard: Arc::clone(&h.guard),
        checkpoints: None,
        metrics: Arc::clone(&h.metrics),
        shutdown: CancellationToken::new(),
    }));
    let tick = {
        let d = Arc::clone(&d);
        tokio::spawn(async move { d.run_tick(None).await })
    };
    tx.send(Row::new(r#"{"n":1}"#)).await.unwrap();
    // Nothing else arrives for several windows.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(!tick.is_finished(), "the unit is still open");
    assert_eq!(
        h.transport.recv(10).await.expect("recv").records.len(),
        1,
        "the window deadline flushed the row without waiting for the stream to end"
    );

    tx.send(Row::new(r#"{"n":2}"#)).await.unwrap();
    drop(tx);
    let report = tokio::time::timeout(Duration::from_secs(5), tick)
        .await
        .expect("the tick ends with the stream")
        .expect("join")
        .expect("tick");
    assert_eq!(report.rows, 2);
    assert_eq!(
        report.flushes, 2,
        "one flush on the window, one closing the unit"
    );
    assert_eq!(h.transport.recv(10).await.expect("recv").records.len(), 1);
}

/// A shape yields ONE kind of mark per unit. A unit that mixes them has a
/// defect the checkpoint cannot express: the second kind is not folded, so
/// committing the first would record a position the unit never reached. The
/// tick fails and nothing is committed.
#[tokio::test]
async fn a_unit_that_folds_two_kinds_of_mark_fails_the_tick_and_commits_nothing() {
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    let h = harness(base_config(), 100);
    let shape = Scripted {
        units: vec![UnitSpec::new("events", UnitShape::Incremental, "fixture")],
        rows: vec![
            Row {
                payload: Bytes::from(r#"{"n":1}"#),
                mark: Some(Mark::Keyset(smallvec::smallvec![Value::from(1)])),
            },
            Row {
                payload: Bytes::from(r#"{"n":2}"#),
                mark: Some(Mark::Item {
                    key: "an-item".into(),
                    position: chrono::Utc::now(),
                }),
            },
        ],
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        Some(Arc::clone(&store)),
        CancellationToken::new(),
    );
    let err = d.run_tick(None).await.expect_err("two kinds of mark");
    assert!(
        err.to_string().contains("more than one kind"),
        "the error names the defect: {err}"
    );
    assert!(
        store.get("inst.conn.events").await.unwrap().is_none(),
        "a position the unit never reached is not committed"
    );
}

#[tokio::test]
async fn a_full_transport_is_backpressure_and_the_tick_aborts_without_a_checkpoint() {
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    // A channel of one: the second send of a batch is refused.
    let h = harness(base_config(), 1);
    let shape = Scripted {
        units: vec![UnitSpec::new("events", UnitShape::Incremental, "fixture")],
        rows: (0..3)
            .map(|i| Row {
                payload: Bytes::from(format!(r#"{{"n":{i}}}"#)),
                mark: Some(Mark::Keyset(smallvec::smallvec![Value::from(i)])),
            })
            .collect(),
        polled: Arc::default(),
    };
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        Some(Arc::clone(&store)),
        CancellationToken::new(),
    );
    let err = d
        .run_tick(None)
        .await
        .expect_err("held records abort the tick");
    assert!(
        matches!(err, dfe_fetcher::Error::Backpressured(_)),
        "{err:?}"
    );
    assert!(
        store.get("inst.conn.events").await.unwrap().is_none(),
        "no checkpoint after backpressure"
    );
    assert!(
        h.metrics
            .render()
            .contains("dfe_transport_backpressured_total 1")
    );
}

#[tokio::test]
async fn shutdown_stops_a_tick_at_the_next_row_without_a_checkpoint() {
    let dir = tempfile::TempDir::new().unwrap();
    let store: Arc<dyn CursorStore> = Arc::new(
        dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap()).unwrap(),
    );
    let h = harness(base_config(), 100);
    let shape = Scripted {
        units: vec![UnitSpec::new("events", UnitShape::Incremental, "fixture")],
        rows: rows(&[r#"{"n":1}"#, r#"{"n":2}"#]),
        polled: Arc::default(),
    };
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let d = driver(
        &h,
        shape,
        small(),
        OversizePolicy::default(),
        None,
        Some(store.clone()),
        shutdown,
    );
    let err = d.run_tick(None).await.expect_err("cancelled");
    assert!(matches!(err, dfe_fetcher::Error::Shutdown));
    assert!(store.get("inst.conn.events").await.unwrap().is_none());
}

/// What each unit's tick asked for: the unit name and the window it was given.
type SeenWindows = Arc<parking_lot::Mutex<Vec<(String, Option<FetchWindow>)>>>;

#[tokio::test]
async fn the_window_is_passed_to_incremental_units_and_withheld_from_dumps() {
    struct WindowSpy {
        units: Vec<UnitSpec>,
        seen: SeenWindows,
    }
    impl RowSource for WindowSpy {
        fn name(&self) -> &'static str {
            "spy"
        }
        fn maturity(&self) -> SourceMaturity {
            SourceMaturity::Alpha
        }
        fn units(&self) -> &[UnitSpec] {
            &self.units
        }
        fn rows<'a>(&'a self, tick: TickCtx<'a>) -> RowStream<'a> {
            self.seen
                .lock()
                .push((tick.unit.name.to_string(), tick.window.cloned()));
            futures::stream::empty().boxed()
        }
        fn probe(&self) -> BoxFuture<'_, dfe_fetcher_core::Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }
    let h = harness(base_config(), 100);
    let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let spy = WindowSpy {
        units: vec![
            UnitSpec::new("events", UnitShape::Incremental, "t"),
            UnitSpec::new("assets", UnitShape::Dump, "t"),
        ],
        seen: Arc::clone(&seen),
    };
    let d = Driver::new(DriverParts {
        shape: Shape::Custom(Box::new(spy)),
        connection_id: "conn".into(),
        instance_id: "inst".into(),
        shared_config: h.shared.clone(),
        accumulate: small(),
        oversize: OversizePolicy::default(),
        emitter: Emitter::new(Arc::clone(&h.state), Arc::clone(&h.metrics), 4),
        pressure: None,
        memory_guard: Arc::clone(&h.guard),
        checkpoints: None,
        metrics: Arc::clone(&h.metrics),
        shutdown: CancellationToken::new(),
    });
    let window = FetchWindow {
        start: chrono::Utc::now() - chrono::Duration::hours(1),
        end: chrono::Utc::now(),
    };
    d.run_tick(Some(&window)).await.unwrap();
    let seen = seen.lock();
    assert_eq!(seen[0].0, "events");
    assert_eq!(seen[0].1.as_ref(), Some(&window));
    assert_eq!(seen[1].0, "assets");
    assert!(seen[1].1.is_none());
    // An empty dump still brackets itself with begin and end.
    let frames = landed(&h.transport).await;
    let kinds: Vec<&str> = frames
        .iter()
        .map(|(_, v)| v["kind"].as_str().unwrap_or(""))
        .collect();
    assert_eq!(kinds, ["begin", "end"]);
    assert_eq!(frames[1].1["row_count"], 0);
    assert_eq!(Kind::ALL.len(), 4);
}
