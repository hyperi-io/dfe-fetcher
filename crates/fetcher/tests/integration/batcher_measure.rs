// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/batcher_measure.rs
// Purpose:   The batcher and concurrent emit measured against real Kafka: rows/s, bytes/s, flushes
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The accumulate-and-emit path measured.
//!
//! A fake `RowSource` yields 50k small rows, then another 50k rows of 4 KiB;
//! a `Driver` with the deployment's default accumulate bounds runs each
//! through the batcher and the emitter to a Kafka broker (live if reachable,
//! else a testcontainer named per test). The tick awaits every delivery
//! report, so the elapsed time covers the rows being on the broker, not just
//! queued. Ignored by default: it moves 200 MiB through a broker and its
//! numbers are the point, printed per unit.

use crate::common;

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use dfe_fetcher::config::{Config, OutputConfig, SharedConfig};
use dfe_fetcher::driver::{Driver, DriverParts, Shape};
use dfe_fetcher::emit::Emitter;
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::output::OutputManager;
use dfe_fetcher::pipeline::PipelineState;
use dfe_fetcher_core::error::Result;
use dfe_fetcher_core::{Row, RowSource, RowStream, SourceMaturity, TickCtx, UnitShape, UnitSpec};

const ROWS_PER_UNIT: u64 = 50_000;

/// The wide row's size, filler included.
const WIDE_BYTES: usize = 4096;

/// A small JSON row.
fn small(i: u64) -> Bytes {
    Bytes::from(format!("{{\"id\":{i},\"v\":\"x\"}}"))
}

/// A 4 KiB JSON row.
fn wide(i: u64) -> Bytes {
    let head = format!("{{\"id\":{i},\"blob\":\"");
    let tail = "\"}";
    let filler = WIDE_BYTES - head.len() - tail.len();
    let mut row = String::with_capacity(WIDE_BYTES);
    row.push_str(&head);
    row.extend(std::iter::repeat_n('x', filler));
    row.push_str(tail);
    Bytes::from(row)
}

/// One unit of `count` synthetic rows, generated as they are pulled.
struct Fake {
    units: Vec<UnitSpec>,
    make: fn(u64) -> Bytes,
    count: u64,
}

impl Fake {
    fn new(unit: &str, make: fn(u64) -> Bytes, count: u64) -> Self {
        Self {
            units: vec![UnitSpec::new(unit, UnitShape::Incremental, "measure")],
            make,
            count,
        }
    }
}

impl RowSource for Fake {
    fn name(&self) -> &'static str {
        "measure"
    }

    fn maturity(&self) -> SourceMaturity {
        SourceMaturity::Alpha
    }

    fn units(&self) -> &[UnitSpec] {
        &self.units
    }

    fn rows<'a>(&'a self, _tick: TickCtx<'a>) -> RowStream<'a> {
        let make = self.make;
        futures::stream::iter((0..self.count).map(move |i| Ok(Row::new(make(i))))).boxed()
    }

    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: moves 100k rows (200 MiB) through real Kafka; run with --ignored"]
async fn the_batcher_and_concurrent_emit_measured_against_real_kafka() {
    let Some((kf, _holder)) = common::acquire_kafka("batcher-measure").await else {
        eprintln!("Skipping: no live Kafka and Docker unavailable for testcontainer");
        return;
    };
    let suffix = format!("-{}", Utc::now().timestamp_millis());

    let mut config = Config::default();
    config.output = OutputConfig {
        output_type: "kafka".to_string(),
        kafka: Some(kf.to_scalo_config()),
        grpc: None,
        topic_suffix: Some(suffix.clone()),
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
    let driver = |fake: Fake, in_flight: usize| {
        let accumulate = dfe_fetcher_core::batch::AccumulateConfig {
            in_flight,
            ..config.accumulate
        };
        Driver::new(DriverParts {
            shape: Shape::Custom(Box::new(fake)),
            connection_id: "measure".into(),
            instance_id: "inst".into(),
            shared_config: shared.clone(),
            accumulate,
            oversize: config.oversize,
            emitter: Emitter::new(Arc::clone(&state), Arc::clone(&metrics), in_flight),
            pressure: None,
            memory_guard: Arc::clone(state.memory_guard()),
            checkpoints: None,
            metrics: Arc::clone(&metrics),
            shutdown: CancellationToken::new(),
        })
    };

    // One row first, so topic creation and the producer's first connection
    // are not on the clock.
    driver(Fake::new("warm", small, 1), config.accumulate.in_flight)
        .run_tick(None)
        .await
        .expect("warm-up tick");

    eprintln!(
        "batcher measurement: accumulate max_rows={} max_bytes={} window_ms={} broker={}",
        config.accumulate.max_rows,
        config.accumulate.max_bytes,
        config.accumulate.window_ms,
        kf.brokers
    );
    // The default `in_flight`, then the whole batch in flight: the sends of
    // one flush are what librdkafka gets to batch inside one linger window.
    for in_flight in [config.accumulate.in_flight, config.accumulate.max_rows] {
        for (unit, make) in [("small", small as fn(u64) -> Bytes), ("wide", wide)] {
            let d = driver(Fake::new(unit, make, ROWS_PER_UNIT), in_flight);
            let started = Instant::now();
            let report = d
                .run_tick(None)
                .await
                .unwrap_or_else(|e| panic!("unit {unit}: {e}"));
            let secs = started.elapsed().as_secs_f64();
            assert_eq!(report.rows, ROWS_PER_UNIT);
            let rows_per_s = report.rows as f64 / secs;
            let bytes_per_s = report.bytes as f64 / secs;
            eprintln!(
                "in_flight={in_flight} unit={unit} rows={} bytes={} flushes={} elapsed={secs:.2}s rows/s={rows_per_s:.0} bytes/s={bytes_per_s:.0} ({:.1} MiB/s) rows/flush={:.0}",
                report.rows,
                report.bytes,
                report.flushes,
                bytes_per_s / 1_048_576.0,
                report.rows as f64 / report.flushes as f64
            );
        }
    }
}
