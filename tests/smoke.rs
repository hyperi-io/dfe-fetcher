// Project:   dfe-fetcher
// File:      tests/smoke.rs
// Purpose:   Mandatory startup smoke tests (always run, never #[ignore])
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::unwrap_used, clippy::expect_used)]

use bytes::Bytes;
use dfe_fetcher::config::Config;
use dfe_fetcher::metrics::Metrics;

/// Verify Orchestrator::new() completes without panic for a default config.
/// This catches init-time panics (missing fields, bad defaults, type mismatches)
/// that would cause a production outage on deploy.
#[tokio::test]
async fn test_startup_orchestrator_boots_with_default_config() {
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    let config = Config::default();
    let metrics = Arc::new(Metrics::new());
    let shutdown = CancellationToken::new();

    // Default config has no Kafka brokers, so Orchestrator should start
    // without output transports (info log: "No output transports configured")
    let result = dfe_fetcher::pipeline::Orchestrator::new(config, metrics, shutdown.clone()).await;

    assert!(
        result.is_ok(),
        "Orchestrator::new should succeed with default config: {:?}",
        result.err()
    );

    let orchestrator = result.unwrap();
    let state = orchestrator.state();

    // Pipeline should be ready (no output = no health check failure)
    assert!(state.is_ready(), "Pipeline should be ready after boot");

    // Config should be accessible
    let config = state.config();
    assert_eq!(config.scheduler.default_interval_secs, 300);
}

/// Verify PipelineState can be created, used for enrichment, and doesn't panic
/// when send_to_transports is called without an output manager.
#[tokio::test]
async fn test_startup_pipeline_state_no_output() {
    use std::sync::Arc;

    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let metrics = Arc::new(Metrics::new());

    let state = dfe_fetcher::pipeline::PipelineState::new(
        shared,
        metrics,
        None,
        tokio_util::sync::CancellationToken::new(),
    )
    .expect("PipelineState::new should succeed");

    // Enrichment should work without output
    let raw = Bytes::from(r#"{"test":true}"#);
    let enriched = state.enrich_record(raw.clone(), "test", "test.source");
    let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
    assert_eq!(parsed["test"], true);
    assert!(parsed["_timestamp_fetcher"].is_number());
    assert_eq!(parsed["_source"], "test");
    assert!(parsed["_source_fetcher"].is_string());

    // Deliver should fail gracefully (no output configured)
    let result = state
        .deliver_ingest("test", "test.source", "test-topic", raw)
        .await;
    assert!(
        result.is_err(),
        "deliver should fail without output transport"
    );
}

/// Verify the Scheduler can be created and computes intervals correctly
/// without needing any runtime resources.
#[test]
fn test_startup_scheduler_creation() {
    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let scheduler_config = dfe_fetcher::config::SchedulerConfig::default();

    let scheduler =
        dfe_fetcher::scheduler::Scheduler::new(&scheduler_config, shared, None, "test-id".into());

    let interval = scheduler.effective_interval(None);
    // Default 300s + up to 10% jitter = 300-330s
    assert!(interval.as_secs() >= 300);
    assert!(interval.as_secs() <= 330);
}
