// Project:   dfe-fetcher
// File:      tests/source_gcp.rs
// Purpose:   GCP source tests with wiremock HTTP mocking
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use dfe_fetcher::config::{GcpService, GcpSourceConfig};
use dfe_fetcher::source::Source;
use dfe_fetcher::source::gcp::GcpSource;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn make_disabled_config() -> GcpSourceConfig {
    GcpSourceConfig {
        enabled: false,
        ..Default::default()
    }
}

#[tokio::test]
async fn test_gcp_disabled_returns_empty() {
    let config = make_disabled_config();
    let source = GcpSource::new(config);

    assert!(!source.is_enabled());
    assert_eq!(source.name(), "gcp");

    let results = source.fetch(None).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_gcp_health_check_disabled() {
    let config = make_disabled_config();
    let source = GcpSource::new(config);
    let healthy = source.health_check().await.unwrap();
    assert!(!healthy);
}

#[tokio::test]
async fn test_gcp_health_check_no_credentials() {
    // No credentials and no metadata server — should fail gracefully
    let config = GcpSourceConfig {
        enabled: true,
        project_id: Some("test-project".into()),
        credential_secret: None,
        service_account_key: None,
        ..Default::default()
    };
    let source = GcpSource::new(config);
    let healthy = source.health_check().await.unwrap();
    assert!(!healthy);
}

// =============================================================================
// Wiremock tests — mock external HTTP boundaries
//
// GCP auth is bypassed by using credential_secret with a literal token value.
// This avoids needing RSA keys or JWT signing in tests.
// =============================================================================

fn make_wiremock_config(server_uri: &str, services: Vec<GcpService>) -> GcpSourceConfig {
    GcpSourceConfig {
        enabled: true,
        project_id: Some("test-project".to_string()),
        credential_secret: Some("mock-gcp-token".to_string()),
        api_url_override: Some(server_uri.to_string()),
        services,
        topic: "test-gcp".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_gcp_fetch_audit_logs_success() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path_regex(".*/v2/entries:list"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "entries": [
                {"logName": "cloudaudit.googleapis.com/activity", "severity": "NOTICE"},
                {"logName": "cloudaudit.googleapis.com/data_access", "severity": "INFO"}
            ]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![GcpService {
            name: "audit_logs".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = GcpSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "gcp.audit_logs");
    assert_eq!(results[0].topic, "test-gcp");
    assert_eq!(results[0].records.len(), 2);
}

#[tokio::test]
async fn test_gcp_fetch_audit_logs_pagination() {
    let server = MockServer::start().await;

    // Page 1 — has nextPageToken
    Mock::given(method("POST"))
        .and(path_regex(".*/v2/entries:list"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "entries": [{"logName": "audit-1"}],
            "nextPageToken": "page2-token"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Page 2 — no nextPageToken (last page)
    Mock::given(method("POST"))
        .and(path_regex(".*/v2/entries:list"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "entries": [{"logName": "audit-2"}, {"logName": "audit-3"}]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![GcpService {
            name: "audit_logs".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = GcpSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].records.len(), 3);
}

#[tokio::test]
async fn test_gcp_fetch_audit_logs_empty() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path_regex(".*/v2/entries:list"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "entries": []
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![GcpService {
            name: "audit_logs".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = GcpSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_gcp_fetch_scc_success() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(".*/v1/organizations/.*/sources/-/findings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "listFindingsResults": [
                {"finding": {"name": "finding-1", "severity": "HIGH"}},
                {"finding": {"name": "finding-2", "severity": "MEDIUM"}}
            ]
        })))
        .mount(&server)
        .await;

    let mut scc_config = HashMap::new();
    scc_config.insert(
        "organization_id".to_string(),
        serde_json::json!("123456789"),
    );

    let config = make_wiremock_config(
        &server.uri(),
        vec![GcpService {
            name: "scc".to_string(),
            config: scc_config,
        }],
    );
    let source = GcpSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "gcp.scc");
    assert_eq!(results[0].records.len(), 2);
}

#[tokio::test]
async fn test_gcp_fetch_cloud_logging_success() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path_regex(".*/v2/entries:list"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "entries": [
                {"severity": "WARNING", "textPayload": "disk nearly full"},
                {"severity": "ERROR", "textPayload": "connection timeout"}
            ]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![GcpService {
            name: "cloud_logging".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = GcpSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "gcp.cloud_logging");
    assert_eq!(results[0].records.len(), 2);
}

#[tokio::test]
async fn test_gcp_fetch_error_500() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path_regex(".*/v2/entries:list"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![GcpService {
            name: "audit_logs".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = GcpSource::new(config);
    let err = source.fetch(None).await.unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("500"),
        "Error should contain status code: {msg}"
    );
}

#[tokio::test]
async fn test_gcp_health_check_credential_secret() {
    // credential_secret resolves as a literal token — health check should pass
    let config = GcpSourceConfig {
        enabled: true,
        project_id: Some("test-project".to_string()),
        credential_secret: Some("mock-token".to_string()),
        ..Default::default()
    };
    let source = GcpSource::new(config);
    let healthy = source.health_check().await.unwrap();

    assert!(healthy);
}
