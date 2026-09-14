// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/file_tail.rs
// Purpose:   The file tail shape on a temp directory through the driver: lines, line checkpoints after ack, rotation, restart
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The vendored tailer end to end.
//!
//! Each test appends to files under a temp directory and runs a `Driver`
//! over the `FileShape` into scalo's in-process transport, with a real
//! `FileCursorStore` carrying the line checkpoints the driver commits after
//! the acks.

use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;

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
use dfe_fetcher_core::batch::{Lease, NoLease};
use dfe_fetcher_core::checkpoint::CursorStore;
use dfe_fetcher_file::{FileInstance, FileShape};

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
        "app".into(),
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

fn driver(h: &Harness, shape: FileShape, checkpoints: Arc<dyn CursorStore>) -> Driver {
    Driver::new(DriverParts {
        shape: Shape::File(Box::new(shape)),
        connection_id: "app".into(),
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
        checkpoints: Some(checkpoints),
        metrics: Arc::clone(&h.metrics),
        shutdown: CancellationToken::new(),
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

/// The include glob covers rotated names too (`*.log*`), so a rotation that
/// happens while the tailer is down is still picked up by fingerprint.
fn instance_yaml(dir: &Path) -> String {
    format!(
        "topic: app-logs\nunits:\n  - unit: logs\n    tail:\n      include: [\"{0}/logs/*.log*\"]\n      data_dir: \"{0}/state\"\n      glob_minimum_cooldown_ms: 50\n      rotate_wait_secs: 1\n      max_tick_secs: 5\n",
        dir.display()
    )
}

fn append(path: &Path, lines: &[&str]) {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    for line in lines {
        writeln!(f, "{line}").unwrap();
    }
    f.flush().unwrap();
}

fn cursor_store(dir: &Path) -> Arc<dyn CursorStore> {
    Arc::new(dfe_fetcher::cursor::file::FileCursorStore::new(dir.to_str().unwrap()).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tailed_lines_land_incrementally_and_offsets_are_committed_after_the_acks() {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("logs")).unwrap();
    let log = dir.path().join("logs").join("app.log");
    append(&log, &["{\"n\":1}", "{\"n\":2}", "{\"n\":3}"]);
    let yaml = instance_yaml(dir.path());
    let h = harness(&yaml);
    let instance: FileInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let store = cursor_store(&dir.path().join("cursors"));
    let lease: Arc<dyn Lease> = Arc::new(NoLease);
    let shape = FileShape::from_instance(&instance, "app", &lease).unwrap();
    let d = driver(&h, shape, Arc::clone(&store));

    assert_eq!(d.run_tick(None).await.expect("tick 1").rows, 3);
    let cursor = store
        .get("inst.app.logs")
        .await
        .unwrap()
        .expect("committed after the acks");
    let Some(CheckpointValue::Lines(files)) = cursor.checkpoint() else {
        panic!("line offsets");
    };
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].1, 24, "offset just past the third line");

    append(&log, &["{\"n\":4}"]);
    assert_eq!(d.run_tick(None).await.expect("tick 2").rows, 1);
    assert_eq!(d.run_tick(None).await.expect("tick 3").rows, 0);

    let frames = landed(&h.transport).await;
    assert!(frames.iter().all(|(topic, _)| topic == "app-logs_land"));
    assert!(
        frames.iter().all(|(_, v)| v.get("kind").is_none()),
        "a tail carries no snapshot envelope"
    );
    let ns: Vec<i64> = frames
        .iter()
        .map(|(_, v)| v["n"].as_i64().unwrap())
        .collect();
    assert_eq!(ns, [1, 2, 3, 4]);
    assert_eq!(frames[0].1["_source_fetcher"], "app.logs");
    if let Shape::File(shape) = &d.shape() {
        shape.stop().await;
    }
}

/// A cursor store whose next write fails once, so a tick fails AFTER its
/// lines were handed out and emitted.
struct FailOnce {
    inner: Arc<dyn CursorStore>,
    fail_next_set: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl CursorStore for FailOnce {
    async fn get(
        &self,
        key: &str,
    ) -> dfe_fetcher_core::Result<Option<dfe_fetcher_core::CursorValue>> {
        self.inner.get(key).await
    }
    async fn set(
        &self,
        key: &str,
        value: &dfe_fetcher_core::CursorValue,
    ) -> dfe_fetcher_core::Result<()> {
        if self
            .fail_next_set
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(dfe_fetcher_core::Error::Cursor(
                "scripted write failure".into(),
            ));
        }
        self.inner.set(key, value).await
    }
    async fn delete(&self, key: &str) -> dfe_fetcher_core::Result<()> {
        self.inner.delete(key).await
    }
}

/// A tick that fails after its lines were handed out commits nothing; the
/// next tick on the same driver, handed the older checkpoint, lands those
/// lines again (at-least-once) and the ones appended since, and its commit
/// covers them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_tick_loses_no_lines_on_the_next_tick_of_the_same_driver() {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("logs")).unwrap();
    let log = dir.path().join("logs").join("app.log");
    append(&log, &["{\"n\":1}", "{\"n\":2}"]);
    let yaml = instance_yaml(dir.path());
    let h = harness(&yaml);
    let instance: FileInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let flaky = Arc::new(FailOnce {
        inner: cursor_store(&dir.path().join("cursors")),
        fail_next_set: std::sync::atomic::AtomicBool::new(false),
    });
    let store: Arc<dyn CursorStore> = flaky.clone();
    let lease: Arc<dyn Lease> = Arc::new(NoLease);
    let shape = FileShape::from_instance(&instance, "app", &lease).unwrap();
    let d = driver(&h, shape, Arc::clone(&store));
    assert_eq!(d.run_tick(None).await.expect("tick 1").rows, 2);

    // Tick 2 hands out n=3, emits it, and fails at the checkpoint write.
    append(&log, &["{\"n\":3}"]);
    flaky
        .fail_next_set
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let err = d.run_tick(None).await.expect_err("the commit fails");
    assert!(err.to_string().contains("scripted write failure"), "{err}");

    // Tick 3 must read n=3 again, plus n=4, from the tick-1 checkpoint.
    append(&log, &["{\"n\":4}"]);
    assert_eq!(d.run_tick(None).await.expect("tick 3").rows, 2);
    assert_eq!(d.run_tick(None).await.expect("tick 4").rows, 0);

    let frames = landed(&h.transport).await;
    let ns: Vec<i64> = frames
        .iter()
        .map(|(_, v)| v["n"].as_i64().unwrap())
        .collect();
    assert_eq!(
        ns,
        [1, 2, 3, 3, 4],
        "n=3 lands twice (once uncommitted), n=4 once, nothing skipped"
    );
    let cursor = store.get("inst.app.logs").await.unwrap().unwrap();
    let Some(CheckpointValue::Lines(files)) = cursor.checkpoint() else {
        panic!("line offsets");
    };
    assert_eq!(files[0].1, 32, "offset just past the fourth line");
    if let Shape::File(shape) = &d.shape() {
        shape.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_resumes_from_the_committed_offsets_and_a_rotation_loses_nothing() {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("logs")).unwrap();
    let log = dir.path().join("logs").join("app.log");
    append(&log, &["{\"n\":1}", "{\"n\":2}"]);
    let yaml = instance_yaml(dir.path());
    let h = harness(&yaml);
    let instance: FileInstance = serde_yaml_ng::from_str(&yaml).unwrap();
    let store = cursor_store(&dir.path().join("cursors"));
    let lease: Arc<dyn Lease> = Arc::new(NoLease);

    let first_shape = FileShape::from_instance(&instance, "app", &lease).unwrap();
    let first = driver(&h, first_shape, Arc::clone(&store));
    assert_eq!(first.run_tick(None).await.expect("tick 1").rows, 2);
    if let Shape::File(shape) = &first.shape() {
        shape.stop().await;
    }
    drop(first);

    // Rotate while nothing is running, then restart over the same store and
    // the same tailer data_dir.
    append(&log, &["{\"n\":3}"]);
    std::fs::rename(&log, dir.path().join("logs").join("app.log.1")).unwrap();
    append(&log, &["{\"m\":1}"]);

    let second_shape = FileShape::from_instance(&instance, "app", &lease).unwrap();
    let second = driver(&h, second_shape, Arc::clone(&store));
    let report = second.run_tick(None).await.expect("tick 2");
    assert_eq!(
        report.rows, 2,
        "the rotated file's tail and the new file's start"
    );
    assert_eq!(second.run_tick(None).await.expect("tick 3").rows, 0);
    if let Shape::File(shape) = &second.shape() {
        shape.stop().await;
    }

    let frames = landed(&h.transport).await;
    let mut texts: Vec<String> = frames.iter().map(|(_, v)| v.to_string()).collect();
    texts.sort();
    let mut expect: Vec<String> = frames
        .iter()
        .map(|(_, v)| v.to_string())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    expect.sort();
    assert_eq!(texts, expect, "no duplicates across the restart");
    let ns: std::collections::BTreeSet<i64> = frames
        .iter()
        .filter_map(|(_, v)| v.get("n").and_then(Value::as_i64))
        .collect();
    assert_eq!(ns.into_iter().collect::<Vec<_>>(), [1, 2, 3]);
    assert!(
        frames.iter().any(|(_, v)| v.get("m").is_some()),
        "{frames:?}"
    );
}
