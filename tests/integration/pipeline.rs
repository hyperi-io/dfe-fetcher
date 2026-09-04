// Project:   dfe-fetcher
// File:      tests/integration/pipeline.rs
// Purpose:   Pipeline enrichment, CEL filtering, and cursor store tests
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

use bytes::Bytes;
use dfe_fetcher::config::Config;
use dfe_fetcher::metrics::Metrics;

#[test]
fn test_enrich_empty_json_object() {
    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let state = make_pipeline_state(shared);

    let payload = Bytes::from("{}");
    let enriched = state.enrich_record(payload, "test", "test.source");
    let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
    assert!(parsed.get("_timestamp_fetcher").is_some());
    assert_eq!(parsed["_source"].as_str().unwrap(), "test");
    assert_eq!(parsed["_source_fetcher"].as_str().unwrap(), "test.source");
}

#[test]
fn test_enrich_nested_json() {
    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let state = make_pipeline_state(shared);

    let payload = Bytes::from(r#"{"outer":{"inner":"value"},"list":[1,2,3]}"#);
    let enriched = state.enrich_record(payload, "defender", "azure.defender");
    let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();

    assert_eq!(parsed["outer"]["inner"].as_str().unwrap(), "value");
    assert_eq!(parsed["list"].as_array().unwrap().len(), 3);
    assert!(parsed.get("_timestamp_fetcher").is_some());
}

#[test]
fn test_enrich_non_json_returns_unchanged() {
    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let state = make_pipeline_state(shared);

    let payload = Bytes::from("this is not json");
    let enriched = state.enrich_record(payload.clone(), "test", "test.source");
    assert_eq!(enriched, payload); // No closing brace, returned unchanged
}

#[test]
fn test_enrich_large_payload() {
    let config = Config::default();
    let shared = dfe_fetcher::config::SharedConfig::new(config);
    let state = make_pipeline_state(shared);

    // Build a large JSON object (100 fields)
    let mut json = String::from("{");
    for i in 0..100 {
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!("\"field_{i}\":\"value_{i}\""));
    }
    json.push('}');

    let payload = Bytes::from(json);
    let enriched = state.enrich_record(payload, "audit_logs", "gcp.audit_logs");
    let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
    assert!(parsed.get("_timestamp_fetcher").is_some());
    assert_eq!(parsed["field_0"].as_str().unwrap(), "value_0");
    assert_eq!(parsed["field_99"].as_str().unwrap(), "value_99");
}

/// Verify the full enrich -> filter pipeline path works end-to-end.
/// Uses `enrich_record` for enrichment and `scalo::expression::evaluate_condition`
/// for CEL filtering (since `evaluate_filter` is private to the pipeline module).
#[tokio::test]
async fn test_pipeline_deliver_enriches_and_filters() {
    use std::collections::HashMap;
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
    .expect("pipeline state creation");

    // 1. Enrich a record
    let raw = Bytes::from(r#"{"eventName":"CreateUser","severity":"high"}"#);
    let enriched = state.enrich_record(raw, "cloudtrail", "aws.cloudtrail");
    let enriched_str = std::str::from_utf8(&enriched).unwrap();

    // 2. Verify all enrichment fields are present
    assert!(enriched_str.contains("\"_timestamp_fetcher\":"));
    assert!(enriched_str.contains("\"_timestamp_received\":"));
    assert!(enriched_str.contains("\"_source\":\"cloudtrail\""));
    assert!(enriched_str.contains("\"_source_fetcher\":\"aws.cloudtrail\""));

    // 3. Parse and verify JSON validity
    let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
    assert_eq!(parsed["eventName"], "CreateUser");
    assert!(parsed["_timestamp_fetcher"].is_number());
    assert!(parsed["_timestamp_received"].is_number());

    // 4. CEL filter: CreateUser should pass (eventName != "ConsoleLogin")
    let filter_expr = r#"eventName != "ConsoleLogin""#;
    let context: HashMap<String, serde_json::Value> =
        serde_json::from_slice::<serde_json::Value>(&enriched)
            .unwrap()
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
    let passes = scalo::expression::evaluate_condition(filter_expr, &context);
    assert!(passes, "CreateUser should pass the filter");

    // 5. CEL filter: ConsoleLogin should be dropped
    let login_record = Bytes::from(r#"{"eventName":"ConsoleLogin"}"#);
    let enriched_login = state.enrich_record(login_record, "cloudtrail", "aws.cloudtrail");
    let login_context: HashMap<String, serde_json::Value> =
        serde_json::from_slice::<serde_json::Value>(&enriched_login)
            .unwrap()
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
    let drops = scalo::expression::evaluate_condition(filter_expr, &login_context);
    assert!(!drops, "ConsoleLogin should be filtered out");
}

/// Verify the cursor -> fetch window flow works end-to-end:
/// no cursor returns None, set stores state, get retrieves it.
#[tokio::test]
async fn test_cursor_file_store_incremental_window() {
    use chrono::{Duration, Utc};
    use dfe_fetcher::cursor::file::FileCursorStore;
    use dfe_fetcher::cursor::{CursorStore, CursorValue};

    let dir = tempfile::TempDir::new().unwrap();
    let store = FileCursorStore::new(dir.path().to_str().unwrap()).unwrap();

    // No cursor: should return None
    let key = "test-instance.aws.cloudtrail";
    assert!(store.get(key).await.unwrap().is_none());

    // Write a cursor (simulating post-fetch)
    let now = Utc::now();
    let cursor = CursorValue {
        cursor_key: key.to_string(),
        last_fetch_end: now - Duration::minutes(5),
        last_fetch_records: 100,
        updated_at: now,
        api_cursor: None,
        version: 1,
    };
    store.set(key, &cursor).await.unwrap();

    // Read back — should exist with correct values
    let stored = store.get(key).await.unwrap().unwrap();
    assert_eq!(stored.cursor_key, key);
    assert_eq!(stored.last_fetch_records, 100);
    assert!(
        (stored.last_fetch_end - cursor.last_fetch_end)
            .num_seconds()
            .abs()
            < 1
    );

    // Verify version and api_cursor
    assert_eq!(stored.version, 1);
    assert!(stored.api_cursor.is_none());

    // Update cursor with new values (simulating second fetch)
    let cursor2 = CursorValue {
        cursor_key: key.to_string(),
        last_fetch_end: now,
        last_fetch_records: 250,
        updated_at: Utc::now(),
        api_cursor: Some("next-page-token".to_string()),
        version: 1,
    };
    store.set(key, &cursor2).await.unwrap();

    // Read back second cursor — should see updated values
    let stored2 = store.get(key).await.unwrap().unwrap();
    assert_eq!(stored2.last_fetch_records, 250);
    assert_eq!(stored2.api_cursor.as_deref(), Some("next-page-token"));
    assert!(
        (stored2.last_fetch_end - now).num_seconds().abs() < 1,
        "last_fetch_end should match the updated cursor"
    );
}

fn make_pipeline_state(
    shared: dfe_fetcher::config::SharedConfig,
) -> dfe_fetcher::pipeline::PipelineState {
    // PipelineState::new with None output for tests (no Kafka/gRPC needed)
    let metrics = std::sync::Arc::new(Metrics::new());
    dfe_fetcher::pipeline::PipelineState::new(
        shared,
        metrics,
        None,
        tokio_util::sync::CancellationToken::new(),
    )
    .expect("pipeline state creation")
}

#[test]
fn test_unwrap_nested_json_default_enabled() {
    // Config::default() should have unwrap_nested_json enabled
    let config = Config::default();
    assert!(
        config.unwrap_nested_json,
        "unwrap_nested_json should be enabled by default"
    );
}

#[test]
fn test_unwrap_nested_json_cloudtrail_shape() {
    // Verify the pipeline unwrap module handles the realistic CloudTrail shape
    let raw = Bytes::from(
        r#"{"EventId":"abc","CloudTrailEvent":"{\"eventVersion\":\"1.11\",\"sourceIPAddress\":\"10.0.0.1\"}"}"#,
    );
    let unwrapped = dfe_fetcher::json_unwrap::unwrap_nested_json(&raw);
    let parsed: serde_json::Value = serde_json::from_slice(&unwrapped).unwrap();
    assert_eq!(parsed["EventId"], "abc");
    assert_eq!(parsed["CloudTrailEvent"]["eventVersion"], "1.11");
    assert_eq!(parsed["CloudTrailEvent"]["sourceIPAddress"], "10.0.0.1");
}
