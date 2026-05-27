// Project:   dfe-fetcher
// File:      tests/integration/source_azure.rs
// Purpose:   Azure source tests with wiremock HTTP mocking
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::HashMap;

use dfe_fetcher::config::{AzureService, AzureSourceConfig};
use dfe_fetcher::source::Source;
use dfe_fetcher::source::azure::AzureSource;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn make_disabled_config() -> AzureSourceConfig {
    AzureSourceConfig {
        enabled: false,
        ..Default::default()
    }
}

#[tokio::test]
async fn test_azure_disabled_returns_empty() {
    let config = make_disabled_config();
    let source = AzureSource::new(config);

    assert!(!source.is_enabled());
    assert_eq!(source.name(), "azure");

    let results = source.fetch(None).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_azure_health_check_disabled() {
    let config = make_disabled_config();
    let source = AzureSource::new(config);
    let healthy = source.health_check().await.unwrap();
    assert!(!healthy);
}

#[tokio::test]
async fn test_azure_missing_tenant_id() {
    let config = AzureSourceConfig {
        enabled: true,
        tenant_id: None,
        client_id: Some("test-client-id".into()),
        client_secret: Some("test-secret".into()),
        ..Default::default()
    };
    let source = AzureSource::new(config);
    let healthy = source.health_check().await.unwrap();
    assert!(!healthy); // Should fail gracefully
}

// =============================================================================
// Wiremock tests — mock external HTTP boundaries
// =============================================================================

/// Mount a mock token endpoint that returns a valid access token.
async fn mount_token_mock(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path_regex(".*/oauth2/v2.0/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "mock-token-azure",
            "token_type": "Bearer",
            "expires_in": 3600
        })))
        .mount(server)
        .await;
}

/// Build an Azure config pointing at a wiremock server.
fn make_wiremock_config(server_uri: &str, services: Vec<AzureService>) -> AzureSourceConfig {
    AzureSourceConfig {
        enabled: true,
        tenant_id: Some("test-tenant".to_string()),
        client_id: Some("test-client-id".to_string()),
        client_secret: Some("test-client-secret".into()),
        subscription_id: Some("test-sub-id".to_string()),
        management_url_override: Some(server_uri.to_string()),
        graph_url_override: Some(server_uri.to_string()),
        token_url_override: Some(format!("{server_uri}/oauth2/v2.0/token")),
        services,
        topic: "test-azure".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_azure_fetch_activity_log_success() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    Mock::given(method("GET"))
        .and(path_regex(
            ".*/microsoft.insights/eventtypes/management/values",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": [
                {"id": "evt-1", "operationName": {"value": "test.write"}},
                {"id": "evt-2", "operationName": {"value": "test.read"}}
            ]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![AzureService {
            name: "activity_log".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = AzureSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "azure.activity_log");
    assert_eq!(results[0].topic, "test-azure");
    assert_eq!(results[0].records.len(), 2);
}

#[tokio::test]
async fn test_azure_fetch_activity_log_pagination() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    // Page 1 — has nextLink
    Mock::given(method("GET"))
        .and(path_regex(
            ".*/microsoft.insights/eventtypes/management/values",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": [{"id": "evt-1"}],
            "@odata.nextLink": format!("{}/page2", server.uri())
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Page 2 — no nextLink
    Mock::given(method("GET"))
        .and(path_regex(".*/page2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": [{"id": "evt-2"}, {"id": "evt-3"}]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![AzureService {
            name: "activity_log".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = AzureSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].records.len(), 3);
}

#[tokio::test]
async fn test_azure_fetch_activity_log_empty() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    Mock::given(method("GET"))
        .and(path_regex(
            ".*/microsoft.insights/eventtypes/management/values",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": []
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![AzureService {
            name: "activity_log".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = AzureSource::new(config);
    let results = source.fetch(None).await.unwrap();

    // Empty value array → no FetchResult returned
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_azure_fetch_defender_success() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    Mock::given(method("GET"))
        .and(path_regex(".*/Microsoft.Security/alerts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": [
                {"id": "alert-1", "severity": "High"},
                {"id": "alert-2", "severity": "Medium"}
            ]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![AzureService {
            name: "defender".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = AzureSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "azure.defender");
    assert_eq!(results[0].records.len(), 2);
}

#[tokio::test]
async fn test_azure_fetch_entra_split_services_success() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    // Sign-in logs -- the entra_signins service hits this.
    Mock::given(method("GET"))
        .and(path_regex(".*/auditLogs/signIns"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": [{"id": "signin-1"}]
        })))
        .mount(&server)
        .await;

    // Directory audits -- the entra_directory_audits service hits this.
    Mock::given(method("GET"))
        .and(path_regex(".*/auditLogs/directoryAudits"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": [{"id": "audit-1"}, {"id": "audit-2"}]
        })))
        .mount(&server)
        .await;

    // Configure both split-out Entra services. The combined `entra_id`
    // alias was removed in the Level 1.1 split; each subtype carries its
    // own cursor and source tag.
    let config = make_wiremock_config(
        &server.uri(),
        vec![
            AzureService {
                name: "entra_signins".to_string(),
                config: HashMap::new(),
            },
            AzureService {
                name: "entra_directory_audits".to_string(),
                config: HashMap::new(),
            },
        ],
    );
    let source = AzureSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 2, "one FetchResult per split-out service");
    let by_source: std::collections::HashMap<&str, usize> = results
        .iter()
        .map(|r| (r.source.as_str(), r.records.len()))
        .collect();
    assert_eq!(by_source.get("azure.entra_signins"), Some(&1));
    assert_eq!(by_source.get("azure.entra_directory_audits"), Some(&2));
}

#[tokio::test]
async fn test_azure_fetch_error_500() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    Mock::given(method("GET"))
        .and(path_regex(
            ".*/microsoft.insights/eventtypes/management/values",
        ))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![AzureService {
            name: "activity_log".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = AzureSource::new(config);
    // With concurrent fetching, individual service failures are logged and
    // skipped — fetch() returns Ok with empty results instead of Err.
    let results = source.fetch(None).await.unwrap();
    assert!(
        results.is_empty(),
        "Failed service should produce no results"
    );
}

#[tokio::test]
async fn test_azure_health_check_success() {
    let server = MockServer::start().await;
    mount_token_mock(&server).await;

    let config = make_wiremock_config(&server.uri(), vec![]);
    let source = AzureSource::new(config);
    let healthy = source.health_check().await.unwrap();

    assert!(healthy);
}

#[tokio::test]
async fn test_azure_health_check_token_failure() {
    let server = MockServer::start().await;

    // Token endpoint returns 401
    Mock::given(method("POST"))
        .and(path_regex(".*/oauth2/v2.0/token"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": "invalid_client",
            "error_description": "Invalid client credentials"
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(&server.uri(), vec![]);
    let source = AzureSource::new(config);
    let healthy = source.health_check().await.unwrap();

    assert!(!healthy);
}
