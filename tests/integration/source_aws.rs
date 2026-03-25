// Project:   dfe-fetcher
// File:      tests/integration/source_aws.rs
// Purpose:   AWS source tests with wiremock HTTP mocking
// Language:  Rust
//
// License:   FSL-1.1-ALv2
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
    let err = source.fetch(None).await.unwrap_err();

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
