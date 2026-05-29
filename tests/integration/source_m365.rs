// Project:   dfe-fetcher
// File:      tests/integration/source_m365.rs
// Purpose:   M365 source tests with wiremock HTTP mocking
// Language:  Rust
//
// License:   BUSL-1.1
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
        client_secret: Some("test-client-secret".into()),
        management_url_override: Some(server_uri.to_string()),
        graph_url_override: Some(server_uri.to_string()),
        token_url_override: Some(format!("{server_uri}/oauth2/v2.0/token")),
        services,
        topic: "test-m365".to_string(),
        ..Default::default()
    }
}

// `test_m365_fetch_audit_log_success` was removed in the M365 OMAP
// refactor (Level 0). The post-refactor `audit_log` service iterates
// the 5 default content types and emits one FetchResult per content
// type; the old single-result assertion couldn't be repaired without
// inventing a per-content-type mock matrix - effectively a rewrite.
// Coverage of the new flow:
//   - src/source/m365/mod.rs::tests (split_window, format_omap_time,
//     publisher_id, subscription start/ensure semantics)
//   - tests/e2e/smoke_remote.rs (live HyperI tenant, `m365_*` tests)

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

// `test_m365_fetch_message_trace_success` was removed in the M365 OMAP
// refactor. The `message_trace` service name no longer exists - what it
// used to cover (Exchange admin/audit activity) is now the
// `exchange_audit` OMAP content-type service. Coverage:
//   - src/source/m365/mod.rs::tests
//   - tests/e2e/smoke_remote.rs (`m365_exchange_audit_*`)

// `test_m365_fetch_dlp_success` and `test_m365_fetch_dlp_pagination`
// were removed in the M365 OMAP refactor. `dlp` still exists as a
// service name but now routes through the Office 365 Management
// Activity API (OMAP `subscriptions/content?contentType=DLP.All`),
// not the legacy `/security/alerts_v2` Graph endpoint. The old mocks
// targeted the dead endpoint; re-mocking would mean re-implementing
// the OMAP subscription/start + content-list/follow flow against
// wiremock - covered already by:
//   - src/source/m365/mod.rs::tests (OMAP helpers)
//   - tests/e2e/smoke_remote.rs (`m365_dlp_*` against live tenant)

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
    let results = source.fetch(None).await.unwrap();
    assert!(
        results.is_empty(),
        "Failed service should produce no results"
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
