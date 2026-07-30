// Project:   dfe-fetcher
// File:      tests/integration/source_aws.rs
// Purpose:   AWS source tests with wiremock HTTP mocking
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::HashMap;

use dfe_fetcher::config::{AwsService, AwsSourceConfig};
use dfe_fetcher::source::Source;
use dfe_fetcher::source::aws::AwsSource;
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

    let results = source.fetch(None).await.unwrap();
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
        secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
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
    let results = source.fetch(None).await.unwrap();

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
    let results = source.fetch(None).await.unwrap();

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
    // AWS fetch continues on service errors (commit 723af3f) — returns Ok([]) instead of Err
    let results = source.fetch(None).await.unwrap();
    assert!(
        results.is_empty(),
        "500 error should return empty results (continue on failure), got: {results:?}"
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
    let results = source.fetch(None).await.unwrap();

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
    let results = source.fetch(None).await.unwrap();

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
        secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
        ..Default::default()
    };
    let source = AwsSource::new(config);
    let healthy = source.health_check().await.unwrap();

    assert!(healthy);
}

// =============================================================================
// CloudWatch Logs wiremock tests
// =============================================================================

#[tokio::test]
async fn test_aws_fetch_cloudwatch_logs_success() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(header("X-Amz-Target", "Logs_20140328.FilterLogEvents"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [
                {"eventId": "ev-1", "logStreamName": "stream-1", "message": "INFO hello"},
                {"eventId": "ev-2", "logStreamName": "stream-1", "message": "ERROR fail"},
                {"eventId": "ev-3", "logStreamName": "stream-2", "message": "WARN something"}
            ]
        })))
        .mount(&server)
        .await;

    let mut svc_config = HashMap::new();
    svc_config.insert(
        "log_group_name".to_string(),
        serde_json::Value::String("/aws/lambda/test".to_string()),
    );

    let config = make_wiremock_config(
        &server.uri(),
        vec![AwsService {
            name: "cloudwatch_logs".to_string(),
            config: svc_config,
        }],
    );
    let source = AwsSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "aws.cloudwatch_logs");
    assert_eq!(results[0].records.len(), 3);
}

#[tokio::test]
async fn test_aws_fetch_cloudwatch_logs_empty() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(header("X-Amz-Target", "Logs_20140328.FilterLogEvents"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "events": [] })))
        .mount(&server)
        .await;

    let mut svc_config = HashMap::new();
    svc_config.insert(
        "log_group_name".to_string(),
        serde_json::Value::String("/aws/lambda/test".to_string()),
    );

    let config = make_wiremock_config(
        &server.uri(),
        vec![AwsService {
            name: "cloudwatch_logs".to_string(),
            config: svc_config,
        }],
    );
    let source = AwsSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_aws_fetch_cloudwatch_logs_pagination() {
    let server = MockServer::start().await;

    // Page 1 — returns events + nextToken
    Mock::given(method("POST"))
        .and(header("X-Amz-Target", "Logs_20140328.FilterLogEvents"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [
                {"eventId": "ev-1", "message": "first page"}
            ],
            "nextToken": "page2token"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Page 2 — returns events without nextToken (last page)
    Mock::given(method("POST"))
        .and(header("X-Amz-Target", "Logs_20140328.FilterLogEvents"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [
                {"eventId": "ev-2", "message": "second page"}
            ]
        })))
        .mount(&server)
        .await;

    let mut svc_config = HashMap::new();
    svc_config.insert(
        "log_group_name".to_string(),
        serde_json::Value::String("/aws/lambda/paginated".to_string()),
    );

    let config = make_wiremock_config(
        &server.uri(),
        vec![AwsService {
            name: "cloudwatch_logs".to_string(),
            config: svc_config,
        }],
    );
    let source = AwsSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "aws.cloudwatch_logs");
    assert_eq!(results[0].records.len(), 2);
}

// =============================================================================
// CloudWatch Metrics wiremock tests
// =============================================================================

#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_success() {
    let server = MockServer::start().await;

    // ListMetrics — returns discovered metrics
    Mock::given(method("POST"))
        .and(header(
            "X-Amz-Target",
            "GraniteServiceVersion20100801.ListMetrics",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Metrics": [
                {
                    "Namespace": "AWS/EC2",
                    "MetricName": "CPUUtilization",
                    "Dimensions": [{"Name": "InstanceId", "Value": "i-1234"}],
                    "Unit": "Percent"
                }
            ]
        })))
        .mount(&server)
        .await;

    // GetMetricData — returns data points
    Mock::given(method("POST"))
        .and(header(
            "X-Amz-Target",
            "GraniteServiceVersion20100801.GetMetricData",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MetricDataResults": [
                {
                    "Id": "q0",
                    "Timestamps": [1_709_424_000.0, 1_709_424_300.0],
                    "Values": [45.2, 62.1]
                }
            ]
        })))
        .mount(&server)
        .await;

    let mut svc_config = HashMap::new();
    svc_config.insert("namespaces".to_string(), serde_json::json!(["AWS/EC2"]));

    let config = make_wiremock_config(
        &server.uri(),
        vec![AwsService {
            name: "cloudwatch_metrics".to_string(),
            config: svc_config,
        }],
    );
    let source = AwsSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "aws.cloudwatch_metrics");
    assert_eq!(results[0].records.len(), 2);

    // Verify record structure
    let record: serde_json::Value = serde_json::from_slice(&results[0].records[0]).unwrap();
    assert_eq!(record["namespace"], "AWS/EC2");
    assert_eq!(record["metric_name"], "CPUUtilization");
    assert_eq!(record["unit"], "Percent");
    assert_eq!(record["stat"], "Average");
}

#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_empty() {
    let server = MockServer::start().await;

    // ListMetrics — no metrics found
    Mock::given(method("POST"))
        .and(header(
            "X-Amz-Target",
            "GraniteServiceVersion20100801.ListMetrics",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "Metrics": [] })),
        )
        .mount(&server)
        .await;

    let mut svc_config = HashMap::new();
    svc_config.insert("namespaces".to_string(), serde_json::json!(["AWS/EC2"]));

    let config = make_wiremock_config(
        &server.uri(),
        vec![AwsService {
            name: "cloudwatch_metrics".to_string(),
            config: svc_config,
        }],
    );
    let source = AwsSource::new(config);
    let results = source.fetch(None).await.unwrap();

    assert!(results.is_empty());
}

// =============================================================================
// CloudWatch Metrics OTLP output tests
// =============================================================================

#[tokio::test]
async fn test_aws_fetch_cloudwatch_metrics_otlp() {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use prost::Message;

    let server = MockServer::start().await;

    // ListMetrics
    Mock::given(method("POST"))
        .and(header(
            "X-Amz-Target",
            "GraniteServiceVersion20100801.ListMetrics",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Metrics": [
                {
                    "Namespace": "AWS/EC2",
                    "MetricName": "CPUUtilization",
                    "Dimensions": [{"Name": "InstanceId", "Value": "i-1234"}],
                    "Unit": "Percent"
                }
            ]
        })))
        .mount(&server)
        .await;

    // GetMetricData
    Mock::given(method("POST"))
        .and(header(
            "X-Amz-Target",
            "GraniteServiceVersion20100801.GetMetricData",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "MetricDataResults": [
                {
                    "Id": "q0",
                    "Timestamps": [1_709_424_000.0, 1_709_424_300.0],
                    "Values": [45.2, 62.1]
                }
            ]
        })))
        .mount(&server)
        .await;

    let mut svc_config = HashMap::new();
    svc_config.insert("namespaces".to_string(), serde_json::json!(["AWS/EC2"]));
    svc_config.insert("output_format".to_string(), serde_json::json!("otlp"));

    let config = make_wiremock_config(
        &server.uri(),
        vec![AwsService {
            name: "cloudwatch_metrics".to_string(),
            config: svc_config,
        }],
    );
    let source = AwsSource::new(config);
    let results = source.fetch(None).await.unwrap();

    // OTLP format emits a single protobuf record
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "aws.cloudwatch_metrics");
    assert_eq!(results[0].records.len(), 1);

    // Deserialise and verify the protobuf structure
    let request = ExportMetricsServiceRequest::decode(results[0].records[0].as_ref()).unwrap();

    assert_eq!(request.resource_metrics.len(), 1);
    let rm = &request.resource_metrics[0];

    // Verify resource attributes
    let resource = rm.resource.as_ref().unwrap();
    let res_attrs: HashMap<&str, &str> = resource
        .attributes
        .iter()
        .map(|kv| {
            let val = match &kv.value {
                Some(v) => match &v.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => s.as_str(),
                    _ => "",
                },
                None => "",
            };
            (kv.key.as_str(), val)
        })
        .collect();
    assert_eq!(res_attrs["cloud.provider"], "aws");
    assert_eq!(res_attrs["cloud.region"], "us-east-1");
    assert_eq!(res_attrs["service.name"], "dfe-fetcher");

    // Verify scope
    let sm = &rm.scope_metrics[0];
    let scope = sm.scope.as_ref().unwrap();
    assert_eq!(scope.name, "dfe-fetcher");

    // Verify metric
    assert_eq!(sm.metrics.len(), 1);
    let metric = &sm.metrics[0];
    assert_eq!(metric.name, "CPUUtilization");
    assert_eq!(metric.unit, "%"); // UCUM for Percent

    // Verify gauge data points
    let gauge = match &metric.data {
        Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(g)) => g,
        _ => panic!("Expected Gauge metric data"),
    };
    assert_eq!(gauge.data_points.len(), 2);
    assert_eq!(
        gauge.data_points[0].time_unix_nano,
        1_709_424_000_000_000_000
    );

    // Verify data point value
    match gauge.data_points[0].value {
        Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v)) => {
            assert!((v - 45.2).abs() < f64::EPSILON);
        }
        _ => panic!("Expected AsDouble value"),
    }

    // Verify data point attributes include Namespace and InstanceId
    let dp_attrs: HashMap<&str, &str> = gauge.data_points[0]
        .attributes
        .iter()
        .map(|kv| {
            let val = match &kv.value {
                Some(v) => match &v.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => s.as_str(),
                    _ => "",
                },
                None => "",
            };
            (kv.key.as_str(), val)
        })
        .collect();
    assert_eq!(dp_attrs["Namespace"], "AWS/EC2");
    assert_eq!(dp_attrs["InstanceId"], "i-1234");
}

// =============================================================================
// LocalStack integration tests (live → docker fallback)
//
// LocalStack emulates AWS APIs. CloudTrail's LookupEvents endpoint is supported
// in the community image. These tests drive the real request path against a
// real HTTP server: URL construction, headers, query encoding, the SigV4
// signing code executing, and response parsing.
//
// They do NOT prove the signature is correct. LocalStack does not verify SigV4
// -- a request signed with a wrong secret is accepted, with or without
// ENFORCE_IAM=1 -- so a bad signature is only caught against a real AWS
// endpoint, in tests/e2e/smoke_remote.rs.
//
// To run:  docker run --rm -p 4566:4566 localstack/localstack:4.14
//          OR set LOCALSTACK_ENDPOINT to a remote LocalStack instance.
// The pinned tag must stay on the 4.x SEMVER line; see LOCALSTACK_TAG in
// tests/common/mod.rs.
// =============================================================================

use crate::common;

fn make_localstack_config(ls: &common::LocalStackConfig) -> AwsSourceConfig {
    AwsSourceConfig {
        enabled: true,
        region: ls.region.clone(),
        access_key_id: Some(ls.access_key_id.clone()),
        secret_access_key: Some(ls.secret_access_key.clone().into()),
        endpoint_override: Some(ls.endpoint.clone()),
        services: vec![AwsService {
            name: "cloudtrail".to_string(),
            config: HashMap::new(),
        }],
        topic: "test-aws-localstack".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_aws_localstack_cloudtrail_lookup_events() {
    let Some(ls) = common::LocalStackConfig::acquire("aws-cloudtrail-lookup-events").await else {
        eprintln!("Skipping: no live LocalStack and Docker unavailable for testcontainer");
        return;
    };
    let config = make_localstack_config(&ls);
    let source = AwsSource::new(config);

    // Real CloudTrail LookupEvents call against LocalStack.
    // The fetch should succeed (empty events list is fine for a fresh LocalStack).
    let result = source.fetch(None).await;

    // A fetch error is the failure this test exists to catch, so it must not be
    // downgraded to an infrastructure excuse: LocalStack serves LookupEvents,
    // and an empty event list from a fresh instance is still `Ok`.
    let results = result.unwrap_or_else(|e| {
        panic!(
            "CloudTrail LookupEvents against LocalStack at {}: {e}",
            ls.endpoint
        )
    });

    // CloudTrail service returns at most one FetchResult
    assert!(
        results.len() <= 1,
        "expected at most 1 FetchResult, got {}",
        results.len()
    );
    // If records present, each should be valid JSON
    for fr in &results {
        for record in &fr.records {
            let parsed: serde_json::Value =
                serde_json::from_slice(record).expect("record must be valid JSON");
            assert!(
                parsed.is_object(),
                "record must be a JSON object, got: {parsed:?}"
            );
        }
    }
}

#[tokio::test]
async fn test_aws_localstack_health_check() {
    let Some(ls) = common::LocalStackConfig::acquire("aws-health-check").await else {
        eprintln!("Skipping: no live LocalStack and Docker unavailable for testcontainer");
        return;
    };
    let config = make_localstack_config(&ls);
    let source = AwsSource::new(config);

    // Health check exercises credential resolution and HTTP client setup.
    // Should return Ok (true or false) — must NOT panic or return Err.
    let result = source.health_check().await;
    assert!(
        result.is_ok(),
        "health_check must not return Err with valid credentials, got {result:?}"
    );
}

#[tokio::test]
async fn test_aws_localstack_with_time_window() {
    let Some(ls) = common::LocalStackConfig::acquire("aws-with-time-window").await else {
        eprintln!("Skipping: no live LocalStack and Docker unavailable for testcontainer");
        return;
    };
    let config = make_localstack_config(&ls);
    let source = AwsSource::new(config);

    // Use a narrow time window — should still complete the SigV4 request.
    let window = dfe_fetcher::source::FetchWindow {
        start: chrono::Utc::now() - chrono::Duration::minutes(5),
        end: chrono::Utc::now(),
    };

    // A window-scoped request is signed exactly like an unscoped one, so it
    // must succeed too. Discarding the result would leave a test that can only
    // fail on a panic, and an error return is not a panic.
    source.fetch(Some(&window)).await.unwrap_or_else(|e| {
        panic!(
            "window-scoped CloudTrail fetch against LocalStack at {}: {e}",
            ls.endpoint
        )
    });
}
