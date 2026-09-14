// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/file_dump.rs
// Purpose:   The file dump shape on a temp directory through the driver: envelope, done marker, restart, bad files
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The file dump end to end.
//!
//! Each test writes files under a temp directory and runs a `Driver` over
//! the `FileShape` into scalo's in-process transport, with a real
//! `FileCursorStore` where the done marker matters.

use std::io::Write as _;
use std::path::Path;
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
use dfe_fetcher_core::envelope::{Envelope, Reassembler};
use dfe_fetcher_file::{FileInstance, FileShape};

/// Counts leased bytes so a test can see chunks come and go.
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
    config.sources.file.insert(
        "exp".into(),
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

fn driver(h: &Harness, shape: FileShape, checkpoints: Option<Arc<dyn CursorStore>>) -> Driver {
    Driver::new(DriverParts {
        shape: Shape::File(Box::new(shape)),
        connection_id: "exp".into(),
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

fn rows_of(frames: &[(String, Value)]) -> Vec<Value> {
    frames
        .iter()
        .filter(|(_, v)| v["kind"] == "row")
        .map(|(_, v)| v["record"].clone())
        .collect()
}

fn instance_yaml(dir: &Path, glob: &str) -> String {
    format!(
        "topic: exports\nunits:\n  - unit: assets\n    dump: {{ paths: [\"{}/{glob}\"], chunk_bytes: 16 }}\n    row_key: \"/id\"\n",
        dir.display()
    )
}

fn gz(data: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(data).unwrap();
    enc.finish().unwrap()
}

fn cursor_store(dir: &Path) -> Arc<dyn CursorStore> {
    Arc::new(dfe_fetcher::cursor::file::FileCursorStore::new(dir.to_str().unwrap()).unwrap())
}

/// The snapshot ids in the order their `begin` frames landed.
fn snapshot_ids(frames: &[(String, Value)]) -> Vec<uuid::Uuid> {
    frames
        .iter()
        .filter(|(_, v)| v["kind"] == "begin")
        .map(|(_, v)| {
            serde_json::from_value::<Envelope>(v.clone())
                .unwrap()
                .head()
                .snapshot_id
        })
        .collect()
}

fn reassemble(frames: &[(String, Value)]) -> Reassembler {
    let mut asm = Reassembler::default();
    for (_, frame) in frames {
        asm.offer(serde_json::to_vec(frame).unwrap().as_slice())
            .unwrap();
    }
    asm
}

#[tokio::test]
async fn a_directory_dump_lands_one_snapshot_per_file_with_every_format_and_gzip_variant() {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("a.jsonl"),
        "{\"id\":1,\"alive\":true}\n{\"id\":2,\"alive\":false}\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("b.json.gz"),
        gz(b"[{\"id\":3,\"alive\":true}]"),
    )
    .unwrap();
    // Two gzip members are one CSV stream: the header is read once.
    let mut two = gz(b"id,alive\n4,true\n");
    two.extend(gz(b"5,false\n"));
    std::fs::write(dir.path().join("c.csv.gz"), two).unwrap();

    let yaml = instance_yaml(dir.path(), "*");
    let h = harness(&yaml);
    let instance: FileInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let lease = Counting::new();
    let shape = FileShape::from_instance(&instance, "exp", &lease.as_lease()).expect("shape");
    let d = driver(&h, shape, None);

    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 5);
    assert_eq!(
        lease.current.load(Ordering::SeqCst),
        0,
        "every chunk released"
    );
    assert!(
        lease.peak.load(Ordering::SeqCst) > 0,
        "chunks were leased while held"
    );

    let frames = landed(&h.transport).await;
    assert!(
        frames
            .iter()
            .all(|(topic, _)| topic == "exports-assets_land")
    );
    let kinds: Vec<&str> = frames
        .iter()
        .map(|(_, v)| v["kind"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        [
            "begin", "row", "row", "end", "begin", "row", "end", "begin", "row", "row", "end"
        ],
        "one snapshot per file, oldest change first: a.jsonl, b.json.gz, c.csv.gz"
    );
    assert!(frames.iter().all(|(_, v)| v["store"] == "exp.assets"));
    assert_eq!(frames[1].1["_source_fetcher"], "exp.assets");
    assert_eq!(frames[1].1["_source"], "exports-assets");
    let seqs: Vec<u64> = frames
        .iter()
        .filter(|(_, v)| v["kind"] == "row")
        .map(|(_, v)| v["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, [0, 1, 0, 0, 1], "seq restarts with every file");
    let ends: Vec<u64> = frames
        .iter()
        .filter(|(_, v)| v["kind"] == "end")
        .map(|(_, v)| v["row_count"].as_u64().unwrap())
        .collect();
    assert_eq!(ends, [2, 1, 2], "row_count is the file's own");

    let rows = rows_of(&frames);
    let mut ids: Vec<String> = rows.iter().map(|r| r["id"].to_string()).collect();
    ids.sort();
    assert_eq!(
        ids,
        ["\"4\"", "\"5\"", "1", "2", "3"],
        "JSON ids are numbers, CSV ids are strings"
    );
    let csv_row = rows.iter().find(|r| r["id"] == "4").unwrap();
    assert_eq!(csv_row["alive"], "true", "CSV fields are strings");

    let ids = snapshot_ids(&frames);
    assert_eq!(ids.len(), 3);
    assert!(
        ids.iter().collect::<std::collections::HashSet<_>>().len() == 3,
        "every file has its own snapshot id"
    );
    let asm = reassemble(&frames);
    let sizes: Vec<usize> = ids
        .iter()
        .map(|id| asm.complete(*id).expect("each file reassembles").len())
        .collect();
    assert_eq!(sizes, [2, 1, 2]);
}

#[tokio::test]
async fn an_idle_tick_emits_nothing_and_commits_nothing() {
    let dir = tempfile::TempDir::new().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let yaml = instance_yaml(&data, "*.jsonl");
    let h = harness(&yaml);
    let instance: FileInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let store = cursor_store(&dir.path().join("cursors"));
    let shape = FileShape::from_instance(&instance, "exp", &Counting::new().as_lease()).unwrap();
    let d = driver(&h, shape, Some(Arc::clone(&store)));

    let report = d.run_tick(None).await.expect("tick");
    assert_eq!(report.rows, 0);
    assert_eq!(report.flushes, 0, "no batch was opened");
    assert!(
        landed(&h.transport).await.is_empty(),
        "no file, no snapshot: an empty snapshot would read as an empty store"
    );
    assert!(store.get("inst.exp.assets").await.unwrap().is_none());
}

#[tokio::test]
async fn a_read_file_is_never_re_read_and_a_new_one_is_read_next_tick_across_a_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(data.join("first.jsonl"), "{\"id\":1}\n").unwrap();
    let yaml = instance_yaml(&data, "*.jsonl");
    let h = harness(&yaml);
    let instance: FileInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let store = cursor_store(&dir.path().join("cursors"));
    let build = || {
        let shape =
            FileShape::from_instance(&instance, "exp", &Counting::new().as_lease()).unwrap();
        driver(&h, shape, Some(Arc::clone(&store)))
    };

    let first = build();
    assert_eq!(first.run_tick(None).await.expect("tick 1").rows, 1);
    let cursor = store
        .get("inst.exp.assets")
        .await
        .unwrap()
        .expect("committed after the acks");
    let Some(CheckpointValue::Item { key, .. }) = cursor.checkpoint() else {
        panic!("a file marker");
    };
    assert!(key.ends_with("first.jsonl"), "{key}");

    // A file that appears mid-run: written while tick 1 has already listed.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    std::fs::write(data.join("second.jsonl"), "{\"id\":2}\n").unwrap();
    assert_eq!(
        first.run_tick(None).await.expect("tick 2").rows,
        1,
        "only the new file"
    );

    let frames = landed(&h.transport).await;
    let ids: Vec<i64> = rows_of(&frames)
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, [1, 2], "each file once, in order of appearance");
    let ends: Vec<u64> = frames
        .iter()
        .filter(|(_, v)| v["kind"] == "end")
        .map(|(_, v)| v["row_count"].as_u64().unwrap())
        .collect();
    assert_eq!(ends, [1, 1], "one snapshot per file");
    let snapshots = snapshot_ids(&frames);
    assert_eq!(snapshots.len(), 2);
    assert_ne!(snapshots[0], snapshots[1], "each file has its own id");
    let asm = reassemble(&frames);
    for id in &snapshots {
        assert_eq!(
            asm.complete(*id).expect("reassembles on its own id").len(),
            1
        );
    }

    // A restart: a new driver over the same store reads nothing again, and
    // an idle tick publishes nothing.
    let second = build();
    assert_eq!(second.run_tick(None).await.expect("tick 3").rows, 0);
    assert!(
        landed(&h.transport).await.is_empty(),
        "an idle tick emits no frames"
    );
}

/// A file that fails before its first row (an extension the decoder cannot
/// place) errors the tick, but the file before it -- whose rows were all
/// read -- is closed with its `end` marker and committed first; once the bad
/// file is removed the next tick reads only what follows.
#[tokio::test]
async fn a_file_failing_before_its_first_row_closes_and_commits_the_file_before_it() {
    let dir = tempfile::TempDir::new().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(data.join("a.jsonl"), "{\"id\":1}\n").unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    std::fs::write(data.join("b.unknown"), "{\"id\":2}\n").unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    std::fs::write(data.join("c.jsonl"), "{\"id\":3}\n").unwrap();
    let yaml = instance_yaml(&data, "*");
    let h = harness(&yaml);
    let instance: FileInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let store = cursor_store(&dir.path().join("cursors"));
    let shape = FileShape::from_instance(&instance, "exp", &Counting::new().as_lease()).unwrap();
    let d = driver(&h, shape, Some(Arc::clone(&store)));

    let err = d
        .run_tick(None)
        .await
        .expect_err("b.unknown has no decoder");
    assert!(err.to_string().contains("b.unknown"), "{err}");
    let cursor = store
        .get("inst.exp.assets")
        .await
        .unwrap()
        .expect("a.jsonl was read whole and committed before b failed");
    let Some(CheckpointValue::Item { key, .. }) = cursor.checkpoint() else {
        panic!("a file marker");
    };
    assert!(key.ends_with("a.jsonl"), "{key}");
    let frames = landed(&h.transport).await;
    let kinds: Vec<&str> = frames
        .iter()
        .map(|(_, v)| v["kind"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        ["begin", "row", "end"],
        "a.jsonl's snapshot is complete; nothing of b or c landed"
    );
    assert_eq!(frames[2].1["row_count"], 1);

    std::fs::remove_file(data.join("b.unknown")).unwrap();
    assert_eq!(
        d.run_tick(None).await.expect("tick 2").rows,
        1,
        "only c.jsonl: a.jsonl is not read again"
    );
    let rows: Vec<i64> = landed(&h.transport)
        .await
        .iter()
        .filter(|(_, v)| v["kind"] == "row")
        .map(|(_, v)| v["record"]["id"].as_i64().unwrap())
        .collect();
    assert_eq!(rows, [3]);
}

#[tokio::test]
async fn a_truncated_gzip_aborts_only_its_snapshot_and_leaves_earlier_files_complete_and_committed()
{
    let dir = tempfile::TempDir::new().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(data.join("good.jsonl.gz"), gz(b"{\"id\":1}\n")).unwrap();
    // Changed later than good.jsonl.gz, so it streams second.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let whole = gz(b"{\"id\":2}\n{\"id\":3}\n{\"id\":4}\n");
    std::fs::write(data.join("cut.jsonl.gz"), &whole[..whole.len() - 8]).unwrap();
    let yaml = instance_yaml(&data, "*.gz");
    let h = harness(&yaml);
    let instance: FileInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let store = cursor_store(&dir.path().join("cursors"));
    let shape = FileShape::from_instance(&instance, "exp", &Counting::new().as_lease()).unwrap();
    let d = driver(&h, shape, Some(Arc::clone(&store)));

    let err = d.run_tick(None).await.expect_err("a truncated member");
    assert!(
        err.to_string().contains("cut.jsonl.gz"),
        "the file is named: {err}"
    );
    let cursor = store
        .get("inst.exp.assets")
        .await
        .unwrap()
        .expect("the complete file was committed before the bad one was read");
    let Some(CheckpointValue::Item { key, .. }) = cursor.checkpoint() else {
        panic!("a file marker");
    };
    assert!(
        key.ends_with("good.jsonl.gz"),
        "the aborted file is not committed: {key}"
    );

    let frames = landed(&h.transport).await;
    let snapshots = snapshot_ids(&frames);
    let asm = reassemble(&frames);
    let good = snapshots[0];
    assert_eq!(
        asm.complete(good)
            .expect("the first file's snapshot is complete")
            .len(),
        1
    );
    let ends: Vec<&Value> = frames
        .iter()
        .filter(|(_, v)| v["kind"] == "end")
        .map(|(_, v)| v)
        .collect();
    assert_eq!(ends.len(), 1, "only the complete file has an end marker");
    for id in &snapshots[1..] {
        assert!(
            asm.complete(*id).is_none(),
            "the truncated file's snapshot has no end: {frames:?}"
        );
    }

    // The operator replaces the file; its new change time makes it new and
    // the committed file is not read again.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    std::fs::write(data.join("cut.jsonl.gz"), &whole).unwrap();
    assert_eq!(
        d.run_tick(None).await.expect("tick 2").rows,
        3,
        "only the replaced file"
    );
    let cursor = store.get("inst.exp.assets").await.unwrap().unwrap();
    let Some(CheckpointValue::Item { key, .. }) = cursor.checkpoint() else {
        panic!("a file marker");
    };
    assert!(key.ends_with("cut.jsonl.gz"), "{key}");
    let frames = landed(&h.transport).await;
    let kinds: Vec<&str> = frames
        .iter()
        .map(|(_, v)| v["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["begin", "row", "row", "row", "end"]);
    assert!(
        d.health_check().await.expect("probe"),
        "the directory itself is fine"
    );
}
