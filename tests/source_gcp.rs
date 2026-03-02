// Project:   dfe-fetcher
// File:      tests/source_gcp.rs
// Purpose:   GCP source tests
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

use dfe_fetcher::config::GcpSourceConfig;
use dfe_fetcher::source::gcp::GcpSource;
use dfe_fetcher::source::Source;

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

    let results = source.fetch().await.unwrap();
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
