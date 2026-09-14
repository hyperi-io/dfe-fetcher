// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/builtin_run.rs
// Purpose:   One tick of a typed source block through the framework driver into the memory transport
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Running a typed block (`sources.github`, `sources.okta`) the way the
//! service does: the block maps onto instances of its shipped profile, each
//! gets a `Driver`, and one tick lands its rows in scalo's memory transport,
//! enriched by the same pipeline state production uses.

#![allow(dead_code)]

use std::sync::Arc;

use scalo::transport::{MemoryConfig, MemoryTransport, TransportReceiver};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use dfe_fetcher::config::{BuiltinInstance, Config, SharedConfig};
use dfe_fetcher::driver::{Driver, DriverParts, Shape};
use dfe_fetcher::emit::Emitter;
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::output::OutputManager;
use dfe_fetcher::pipeline::PipelineState;
use dfe_fetcher_core::FetchWindow;

/// One record as it reached the transport: the bytes, and the JSON they
/// parse as (`Null` for a record that is not JSON, such as an OTLP protobuf).
#[derive(Debug, Clone)]
pub struct Landed {
    pub topic: String,
    pub record: Value,
    pub raw: Vec<u8>,
}

/// What enrichment stamped on a landed record, split from the provider's row.
pub struct Enriched {
    pub row: Value,
    pub source: String,
    pub source_fetcher: String,
}

/// Split the four enrichment keys off a landed record, asserting both
/// timestamps are numbers.
pub fn enriched(landed: &Landed) -> Enriched {
    let mut map = landed
        .record
        .as_object()
        .expect("landed record is an object")
        .clone();
    assert!(
        map.remove("_timestamp_fetcher")
            .is_some_and(|v| v.is_number()),
        "_timestamp_fetcher is a number"
    );
    assert!(
        map.remove("_timestamp_received")
            .is_some_and(|v| v.is_number()),
        "_timestamp_received is a number"
    );
    let source = map
        .remove("_source")
        .and_then(|v| v.as_str().map(str::to_owned))
        .expect("_source is a string");
    let source_fetcher = map
        .remove("_source_fetcher")
        .and_then(|v| v.as_str().map(str::to_owned))
        .expect("_source_fetcher is a string");
    Enriched {
        row: Value::Object(map),
        source,
        source_fetcher,
    }
}

struct Harness {
    state: Arc<PipelineState>,
    transport: Arc<MemoryTransport>,
    shared: SharedConfig,
    metrics: Arc<Metrics>,
}

fn harness(config: Config) -> Harness {
    harness_with_output(config, true)
}

/// The harness with or without an output: without one every send fails
/// at once, the delivery failure a test needs to prove what is NOT done
/// (a checkpoint, an acknowledgement) when nothing lands.
fn harness_with_output(config: Config, with_output: bool) -> Harness {
    let transport = Arc::new(
        MemoryTransport::new(&MemoryConfig {
            buffer_size: 10_000,
            ..MemoryConfig::default()
        })
        .expect("memory transport"),
    );
    let shared = SharedConfig::new(config);
    let metrics = Arc::new(Metrics::new());
    let state = Arc::new(
        PipelineState::new(
            shared.clone(),
            Arc::clone(&metrics),
            with_output.then(|| OutputManager::memory(Arc::clone(&transport))),
            CancellationToken::new(),
        )
        .expect("pipeline state"),
    );
    Harness {
        state,
        transport,
        shared,
        metrics,
    }
}

/// One tick of `built` under `config` with NO output transport: the first
/// flush fails, so the tick fails after the shape has fetched.
pub async fn run_without_output(
    config: Config,
    built: &BuiltinInstance,
    window: Option<&FetchWindow>,
) -> Result<(), String> {
    let h = harness_with_output(config, false);
    let driver = driver_with(&h, built, None)?;
    driver
        .run_tick(window)
        .await
        .map(drop)
        .map_err(|e| e.to_string())
}

async fn landed(transport: &MemoryTransport) -> Vec<Landed> {
    let batch = transport.recv(10_000).await.expect("recv");
    batch
        .records
        .into_iter()
        .map(|r| Landed {
            topic: r.key.as_deref().unwrap_or("").to_owned(),
            record: serde_json::from_slice(&r.payload).unwrap_or(Value::Null),
            raw: r.payload.to_vec(),
        })
        .collect()
}

/// The driver the service builds for one built-in connection, committing
/// unit checkpoints to `checkpoints` when given one.
fn driver_with(
    h: &Harness,
    built: &BuiltinInstance,
    checkpoints: Option<Arc<dyn dfe_fetcher_core::CursorStore>>,
) -> Result<Driver, String> {
    let profile = dfe_fetcher_rest::profile::bound::resolve_profile(
        &built.instance,
        dfe_fetcher::profiles::shipped(),
    )
    .map_err(|e| e.to_string())?;
    let shape = Shape::for_rest_instance(
        &profile,
        &built.instance,
        &built.connection_id,
        reqwest::Client::new(),
    )
    .map_err(|e| e.to_string())?;
    let config = h.shared.get();
    Ok(Driver::new(DriverParts {
        shape,
        connection_id: built.connection_id.clone(),
        instance_id: "test".into(),
        shared_config: h.shared.clone(),
        accumulate: config.accumulate,
        oversize: config.oversize,
        emitter: Emitter::new(
            Arc::clone(&h.state),
            Arc::clone(&h.metrics),
            config.accumulate.in_flight,
        ),
        pressure: None,
        memory_guard: Arc::clone(h.state.memory_guard()),
        checkpoints,
        metrics: Arc::clone(&h.metrics),
        shutdown: CancellationToken::new(),
    }))
}

/// One tick of `built` under `config`, committing unit checkpoints to
/// `checkpoints`, so a second tick from the same store starts where the
/// first left off.
pub async fn run_checkpointed(
    config: Config,
    built: &BuiltinInstance,
    window: Option<&FetchWindow>,
    checkpoints: Arc<dyn dfe_fetcher_core::CursorStore>,
) -> (Result<(), String>, Vec<Landed>) {
    let h = harness(config);
    let outcome = async {
        let driver = driver_with(&h, built, Some(checkpoints))?;
        driver
            .run_tick(window)
            .await
            .map(drop)
            .map_err(|e| e.to_string())
    }
    .await;
    (outcome, landed(&h.transport).await)
}

/// The driver the service builds for one built-in connection, with no
/// checkpoint store.
fn driver(h: &Harness, built: &BuiltinInstance) -> Result<Driver, String> {
    driver_with(h, built, None)
}

/// The instance a config maps its built-in connection `connection_id` onto.
pub fn built_instance(config: &Config, connection_id: &str) -> Result<BuiltinInstance, String> {
    config
        .sources
        .builtin_instances()
        .map_err(|e| e.to_string())?
        .into_iter()
        .find(|b| b.connection_id == connection_id)
        .ok_or_else(|| format!("no built-in connection `{connection_id}`"))
}

/// One tick of the built-in connection `connection_id` as `config` maps it,
/// through the pipeline. Returns the tick's outcome and whatever landed.
pub async fn run(
    config: Config,
    connection_id: &str,
    window: Option<&FetchWindow>,
) -> (Result<(), String>, Vec<Landed>) {
    match built_instance(&config, connection_id) {
        Ok(built) => Box::pin(run_instance(config, &built, window)).await,
        Err(e) => (Err(e), Vec::new()),
    }
}

/// One tick of `built` (an instance a test may have adjusted, such as a
/// host var the typed block has no override for) under `config`.
pub async fn run_instance(
    config: Config,
    built: &BuiltinInstance,
    window: Option<&FetchWindow>,
) -> (Result<(), String>, Vec<Landed>) {
    let h = harness(config);
    let outcome = async {
        let driver = driver(&h, built)?;
        driver
            .run_tick(window)
            .await
            .map(drop)
            .map_err(|e| e.to_string())
    }
    .await;
    (outcome, landed(&h.transport).await)
}

/// The health check of the built-in connection `connection_id`.
pub async fn health(config: Config, connection_id: &str) -> Result<bool, String> {
    let h = harness(config.clone());
    let built = config
        .sources
        .builtin_instances()
        .map_err(|e| e.to_string())?
        .into_iter()
        .find(|b| b.connection_id == connection_id)
        .ok_or_else(|| format!("no built-in connection `{connection_id}`"))?;
    driver(&h, &built)?
        .health_check()
        .await
        .map_err(|e| e.to_string())
}
