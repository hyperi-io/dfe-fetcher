// Project:   dfe-fetcher
// File:      tests/source_azure.rs
// Purpose:   Azure source tests with wiremock HTTP mocking
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

use dfe_fetcher::config::AzureSourceConfig;
use dfe_fetcher::source::azure::AzureSource;
use dfe_fetcher::source::Source;

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

    let results = source.fetch().await.unwrap();
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
