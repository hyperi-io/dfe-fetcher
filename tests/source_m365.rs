// Project:   dfe-fetcher
// File:      tests/source_m365.rs
// Purpose:   M365 source tests
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

use dfe_fetcher::config::M365SourceConfig;
use dfe_fetcher::source::m365::M365Source;
use dfe_fetcher::source::Source;

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

    let results = source.fetch().await.unwrap();
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
