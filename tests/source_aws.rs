// Project:   dfe-fetcher
// File:      tests/source_aws.rs
// Purpose:   AWS source tests with wiremock HTTP mocking
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::HashMap;

use dfe_fetcher::config::{AwsService, AwsSourceConfig};
use dfe_fetcher::source::aws::AwsSource;
use dfe_fetcher::source::Source;
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

// =============================================================================
// Wiremock tests — mock external HTTP boundaries
//
// AWS SigV4 signing runs against the mock server (reqsign signs the request,
// wiremock doesn't validate signatures). We use standard AWS example credentials.
// =============================================================================

fn make_wiremock_config(server_uri: &str, services: Vec<AwsService>) -> AwsSourceConfig {
    AwsSourceConfig {
        enabled: true,
        region: "us-east-1".to_string(),
        access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
        secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
        endpoint_override: Some(server_uri.to_string()),
        services,
        topic: "test-aws".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_aws_fetch_cloudtrail_success() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(header(
            "X-Amz-Target",
            "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Events": [
                {"EventId": "evt-1", "EventName": "ConsoleLogin"},
                {"EventId": "evt-2", "EventName": "AssumeRole"}
            ]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![AwsService {
            name: "cloudtrail".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = AwsSource::new(config);
    let results = source.fetch().await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "aws.cloudtrail");
    assert_eq!(results[0].topic, "test-aws");
    assert_eq!(results[0].records.len(), 2);
}

#[tokio::test]
async fn test_aws_fetch_cloudtrail_empty() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(header(
            "X-Amz-Target",
            "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Events": []
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![AwsService {
            name: "cloudtrail".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = AwsSource::new(config);
    let results = source.fetch().await.unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_aws_fetch_error_500() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![AwsService {
            name: "cloudtrail".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = AwsSource::new(config);
    let err = source.fetch().await.unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("500"),
        "Error should contain status code: {msg}"
    );
}

#[tokio::test]
async fn test_aws_fetch_guardduty_chain() {
    let server = MockServer::start().await;

    // ListDetectors
    Mock::given(method("POST"))
        .and(header(
            "X-Amz-Target",
            "com.amazonaws.guardduty.v20170811.GuardDuty_20170811.ListDetectors",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "DetectorIds": ["detector-1"]
        })))
        .mount(&server)
        .await;

    // ListFindings
    Mock::given(method("POST"))
        .and(header(
            "X-Amz-Target",
            "com.amazonaws.guardduty.v20170811.GuardDuty_20170811.ListFindings",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "FindingIds": ["finding-1", "finding-2"]
        })))
        .mount(&server)
        .await;

    // GetFindings
    Mock::given(method("POST"))
        .and(header(
            "X-Amz-Target",
            "com.amazonaws.guardduty.v20170811.GuardDuty_20170811.GetFindings",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Findings": [
                {"id": "finding-1", "severity": 8.0, "type": "Recon:EC2/PortProbeUnprotectedPort"},
                {"id": "finding-2", "severity": 5.0, "type": "UnauthorizedAccess:EC2/SSHBruteForce"}
            ]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![AwsService {
            name: "guardduty".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = AwsSource::new(config);
    let results = source.fetch().await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "aws.guardduty");
    assert_eq!(results[0].records.len(), 2);
}

#[tokio::test]
async fn test_aws_fetch_securityhub_success() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(header(
            "X-Amz-Target",
            "com.amazonaws.securityhub.v20180710.SecurityHub_20180710.GetFindings",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Findings": [
                {"Id": "finding-1", "Title": "S3 bucket public", "Severity": {"Label": "HIGH"}},
                {"Id": "finding-2", "Title": "IAM key not rotated", "Severity": {"Label": "MEDIUM"}}
            ]
        })))
        .mount(&server)
        .await;

    let config = make_wiremock_config(
        &server.uri(),
        vec![AwsService {
            name: "securityhub".to_string(),
            config: HashMap::new(),
        }],
    );
    let source = AwsSource::new(config);
    let results = source.fetch().await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "aws.securityhub");
    assert_eq!(results[0].records.len(), 2);
}

#[tokio::test]
async fn test_aws_health_check_with_credentials() {
    // Health check just validates credentials exist — no API call needed
    let config = AwsSourceConfig {
        enabled: true,
        access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
        secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
        ..Default::default()
    };
    let source = AwsSource::new(config);
    let healthy = source.health_check().await.unwrap();

    assert!(healthy);
}
