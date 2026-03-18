// Project:   dfe-fetcher
// File:      benches/pipeline.rs
// Purpose:   Pipeline enrichment, CEL filter, and cursor benchmarks
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};

use dfe_fetcher::config::{Config, SharedConfig};
use dfe_fetcher::cursor::file::FileCursorStore;
use dfe_fetcher::cursor::{CursorStore, CursorValue};
use dfe_fetcher::metrics::Metrics;
use dfe_fetcher::pipeline::PipelineState;

/// Create a PipelineState with no output transport (enrichment-only).
fn make_pipeline_state() -> PipelineState {
    let config = Config::default();
    let shared = SharedConfig::new(config);
    let metrics = Arc::new(Metrics::new());
    PipelineState::new(shared, metrics, None).expect("PipelineState creation should succeed")
}

/// Generate a JSON payload of approximately the given byte size.
fn make_json_payload(approx_bytes: usize) -> Bytes {
    // Base object overhead is about 20 bytes for `{"k":""}`, so pad the value
    let padding_len = approx_bytes.saturating_sub(20);
    let value: String = "x".repeat(padding_len);
    Bytes::from(format!(r#"{{"key":"{value}"}}"#))
}

// =============================================================================
// Benchmark: enrich_record
// =============================================================================

fn bench_enrich_record(c: &mut Criterion) {
    let state = make_pipeline_state();

    let small = make_json_payload(100);
    let medium = make_json_payload(1_024);
    let large = make_json_payload(10_240);

    let mut group = c.benchmark_group("enrich_record");
    group.throughput(Throughput::Elements(1));

    group.bench_function("small_100b", |b| {
        b.iter(|| state.enrich_record(black_box(small.clone()), "aws.cloudtrail"));
    });

    group.bench_function("medium_1kb", |b| {
        b.iter(|| state.enrich_record(black_box(medium.clone()), "azure.sentinel"));
    });

    group.bench_function("large_10kb", |b| {
        b.iter(|| state.enrich_record(black_box(large.clone()), "gcp.audit"));
    });

    // Batch of 1000 small records
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("batch_1000_small", |b| {
        b.iter(|| {
            for _ in 0..1_000 {
                let _ = state.enrich_record(black_box(small.clone()), "m365.audit");
            }
        });
    });

    group.finish();
}

// =============================================================================
// Benchmark: CEL filter evaluation
// =============================================================================

fn bench_cel_filter_evaluation(c: &mut Criterion) {
    let mut group = c.benchmark_group("cel_filter");
    group.throughput(Throughput::Elements(1));

    // Build a context HashMap similar to what evaluate_filter builds
    let mut context: HashMap<String, serde_json::Value> = HashMap::new();
    context.insert(
        "eventName".to_string(),
        serde_json::Value::String("CreateUser".to_string()),
    );
    context.insert(
        "severity".to_string(),
        serde_json::Value::String("high".to_string()),
    );
    context.insert(
        "sourceIPAddress".to_string(),
        serde_json::Value::String("10.0.0.1".to_string()),
    );

    group.bench_function("not_equal_string", |b| {
        b.iter(|| {
            hyperi_rustlib::expression::evaluate_condition(
                black_box(r#"eventName != "ConsoleLogin""#),
                black_box(&context),
            )
        });
    });

    group.bench_function("equal_string", |b| {
        b.iter(|| {
            hyperi_rustlib::expression::evaluate_condition(
                black_box(r#"severity == "high""#),
                black_box(&context),
            )
        });
    });

    group.finish();
}

// =============================================================================
// Benchmark: FileCursorStore set + get cycle
// =============================================================================

fn bench_file_cursor_set_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let tmp_dir = tempfile::TempDir::new().expect("temp dir");
    let store = FileCursorStore::new(tmp_dir.path().to_str().expect("path")).expect("cursor store");

    let cursor = CursorValue {
        cursor_key: "bench.cursor".to_string(),
        last_fetch_end: chrono::Utc::now(),
        last_fetch_records: 42,
        updated_at: chrono::Utc::now(),
        api_cursor: None,
        version: 1,
    };

    let mut group = c.benchmark_group("file_cursor");
    group.throughput(Throughput::Elements(1));

    group.bench_function("set_get_cycle", |b| {
        b.iter(|| {
            rt.block_on(async {
                store
                    .set(black_box("bench.cursor"), black_box(&cursor))
                    .await
                    .expect("set");
                let _ = store.get(black_box("bench.cursor")).await.expect("get");
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_enrich_record,
    bench_cel_filter_evaluation,
    bench_file_cursor_set_get
);
criterion_main!(benches);
