// Project:   dfe-fetcher
// File:      tests/source_aws.rs
// Purpose:   AWS source tests
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

use dfe_fetcher::config::AwsSourceConfig;
use dfe_fetcher::source::aws::AwsSource;
use dfe_fetcher::source::Source;

fn make_disabled_config() -> AwsSourceConfig {
    AwsSourceConfig {
        enabled: false,
        ..Default::default()
    }
}

#[tokio::test]
async fn test_aws_disabled_returns_empty() {
    let config = make_disabled_config();
    let source = AwsSource::new(config);

    assert!(!source.is_enabled());
    assert_eq!(source.name(), "aws");

    let results = source.fetch().await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_aws_health_check_disabled() {
    let config = make_disabled_config();
    let source = AwsSource::new(config);
    let healthy = source.health_check().await.unwrap();
    assert!(!healthy);
}

#[tokio::test]
async fn test_aws_health_check_no_credentials() {
    let config = AwsSourceConfig {
        enabled: true,
        access_key_id: None,
        secret_access_key: None,
        credential_secret: None,
        ..Default::default()
    };
    let source = AwsSource::new(config);
    let healthy = source.health_check().await.unwrap();
    assert!(!healthy);
}
