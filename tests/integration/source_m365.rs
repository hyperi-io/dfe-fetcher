// Project:   dfe-fetcher
// File:      tests/integration/source_m365.rs
// Purpose:   M365 source tests with wiremock HTTP mocking
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::HashMap;

use dfe_fetcher::config::{M365Service, M365SourceConfig};
use dfe_fetcher::source::Source;
use dfe_fetcher::source::m365::M365Source;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn make_disabled_config() -> M365SourceConfig {
    M365SourceConfig {
        enabled: false,
        ..Default::default()
    }
}

#[tokio::test]
async fn test_m365_disabled_returns_empty() {
    let config = make_disabled_config();
    let source = M365Source::new(config);

    assert!(!source.is_enabled());
    assert_eq!(source.name(), "m365");

    let results = source.fetch(None).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_m365_health_check_disabled() {
    let config = make_disabled_config();
    let source = M365Source::new(config);
    let healthy = source.health_check().await.unwrap();
    assert!(!healthy);
}

#[tokio::test]
async fn test_m365_missing_tenant_id() {
    let config = M365SourceConfig {
        enabled: true,
        tenant_id: None,
        client_id: Some("test-client".into()),
        client_secret: Some("test-secret".into()),
        ..Default::default()
    };
    let source = M365Source::new(config);
    let healthy = source.health_check().await.unwrap();
    assert!(!healthy); // Should fail gracefully
}

// =============================================================================
// Wiremock tests — mock external HTTP boundaries
// =============================================================================

async fn mount_token_mock(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path_regex(".*/oauth2/v2.0/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "mock-token-m365",
            "token_type": "Bearer",
            "expires_in": 3600
        })))
        .mount(server)
        .await;
}

fn make_wiremock_config(server_uri: &str, services: Vec<M365Service>) -> M365SourceConfig {
    M365SourceConfig {
        enabled: true,
        tenant_id: Some("test-tenant".to_string()),
        client_id: Some("test-client-id".to_string()),
        client_secret: Some("test-client-secret".to_string()),
        management_url_override: Some(server_uri.to_string()),
        graph_url_override: Some(server_uri.to_string()),
        token_url_override: Some(format!("{server_uri}/oauth2/v2.0/token")),
        services,
        topic: "test-m365".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_m365_fetch_audit_log_success() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    // Content list response with content URIs pointing back at the mock server
    Mock::given(method("GET"))
        .and(path_regex(".*/activity/feed/subscriptions/content"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"contentUri": format!("{}/content/1", server.uri())},
            {"contentUri": format!("{}/content/2", server.uri())}
        ])))
        .mount(&server)
        .await;

    // Content URI 1 returns events
    Mock::given(method("GET"))
        .and(path_regex(".*/content/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": "event-1", "operation": "UserLoggedIn"}
        ])))
        .mount(&server)
        .await;

    // Content URI 2 returns events
    Mock::given(method("GET"))
        .and(path_regex(".*/content/2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": "event-2", "operation": "FileAccessed"},
            {"id": "event-3", "operation": "FileModified"}
        ])))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![M365Service {
            name: "audit_log".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = M365Source::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "m365.audit_log");
    assert_eq!(results[0].records.len(), 3);
}

#[tokio::test]
async fn test_m365_fetch_audit_log_empty() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    Mock::given(method("GET"))
        .and(path_regex(".*/activity/feed/subscriptions/content"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![M365Service {
            name: "audit_log".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = M365Source::new(config);
    let results = source.fetch(None).await.unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_m365_fetch_audit_log_404_starts_subscription() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    // Content list returns 404 (subscription not started)
    Mock::given(method("GET"))
        .and(path_regex(".*/activity/feed/subscriptions/content"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    // Subscription start endpoint
    Mock::given(method("POST"))
        .and(path_regex(".*/activity/feed/subscriptions/start"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![M365Service {
            name: "audit_log".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = M365Source::new(config);
    let results = source.fetch(None).await.unwrap();

    // Returns empty (subscription just started, no data yet)
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_m365_fetch_message_trace_success() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    Mock::given(method("GET"))
        .and(path_regex(".*/reports/getEmailActivityCounts"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("Report Refresh Date,Send Count\n2026-03-03,42"),
        )
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![M365Service {
            name: "message_trace".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = M365Source::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "m365.message_trace");
    assert_eq!(results[0].records.len(), 1); // CSV as single record
}

#[tokio::test]
async fn test_m365_fetch_dlp_success() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    Mock::given(method("GET"))
        .and(path_regex(".*/security/alerts_v2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": [
                {"id": "dlp-1", "category": "DataLossPrevention"},
                {"id": "dlp-2", "category": "DataLossPrevention"}
            ]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![M365Service {
            name: "dlp".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = M365Source::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "m365.dlp");
    assert_eq!(results[0].records.len(), 2);
}

#[tokio::test]
async fn test_m365_fetch_dlp_pagination() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    // Page 1 — has nextLink
    Mock::given(method("GET"))
        .and(path_regex(".*/security/alerts_v2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": [{"id": "dlp-1"}],
            "@odata.nextLink": format!("{}/page2", server.uri())
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Page 2
    Mock::given(method("GET"))
        .and(path_regex(".*/page2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": [{"id": "dlp-2"}, {"id": "dlp-3"}]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![M365Service {
            name: "dlp".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = M365Source::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].records.len(), 3);
}

#[tokio::test]
async fn test_m365_fetch_alerts_success() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    Mock::given(method("GET"))
        .and(path_regex(".*/security/alerts_v2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": [
                {"id": "alert-1", "severity": "high"},
                {"id": "alert-2", "severity": "medium"}
            ]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![M365Service {
            name: "alerts".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = M365Source::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "m365.alerts");
    assert_eq!(results[0].records.len(), 2);
}

#[tokio::test]
async fn test_m365_fetch_error_500() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![M365Service {
            name: "alerts".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = M365Source::new(config);
    let err = source.fetch(None).await.unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("500"),
        "Error should contain status code: {msg}"
    );
}

#[tokio::test]
async fn test_m365_health_check_success() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    let config = make_wiremock_config(&server.uri(), vec![]);
    let source = M365Source::new(config);
    let healthy = source.health_check().await.unwrap();

    assert!(healthy);
}

#[tokio::test]
async fn test_m365_health_check_token_failure() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path_regex(".*/oauth2/v2.0/token"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": "invalid_client"
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(&server.uri(), vec![]);
    let source = M365Source::new(config);
    let healthy = source.health_check().await.unwrap();

    assert!(!healthy);
}
