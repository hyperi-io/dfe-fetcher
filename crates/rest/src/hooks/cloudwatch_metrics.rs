// Project:   dfe-fetcher
// File:      crates/rest/src/hooks/cloudwatch_metrics.rs
// Purpose:   The CloudWatch metrics row builder: ListMetrics descriptors into GetMetricData queries, the response joined back into rows
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! CloudWatch metrics are a two-request synthesis: `ListMetrics` yields
//! metric descriptors, `GetMetricData` takes them back as queries and
//! answers per query id with timestamps and values that name no
//! namespace, dimensions or unit. This builder shapes a batch of
//! descriptors into the queries (`q<position>` ids, the period and
//! statistic from the unit's `vars`) and joins each response back onto
//! the descriptors by position: one JSON row per datapoint, or one OTLP
//! `ExportMetricsServiceRequest` per response when `vars.output_format`
//! is `otlp`.

use std::collections::BTreeMap;

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};

use dfe_fetcher_core::error::{Error, Result};

use crate::profile::template::TemplateCtx;

/// Seconds between datapoints unless `vars.period_secs` says otherwise.
const DEFAULT_PERIOD_SECS: i64 = 300;
/// The statistic unless `vars.stat` says otherwise.
const DEFAULT_STAT: &str = "Average";

#[derive(Deserialize)]
struct MetricDataResult {
    #[serde(rename = "Id", default)]
    id: String,
    #[serde(rename = "Timestamps", default)]
    timestamps: Vec<f64>,
    #[serde(rename = "Values", default)]
    values: Vec<f64>,
}

#[derive(Deserialize)]
struct MetricDataResponse {
    #[serde(rename = "MetricDataResults", default)]
    results: Vec<MetricDataResult>,
}

/// One datapoint joined onto the descriptor it belongs to.
struct Point<'a> {
    descriptor: &'a Value,
    timestamp: f64,
    value: f64,
}

/// A string var of the unit, or `fallback`.
fn var<'a>(ctx: &'a TemplateCtx, name: &str, fallback: &'a str) -> &'a str {
    ctx.get("vars")
        .and_then(|v| v.get(name))
        .and_then(Value::as_str)
        .unwrap_or(fallback)
}

/// The `MetricDataQueries` a batch of `ListMetrics` descriptors becomes:
/// one query per descriptor, its id the descriptor's position.
pub(super) fn queries(descriptors: &[Value], ctx: &TemplateCtx) -> Vec<Value> {
    let period = ctx
        .get("vars")
        .and_then(|v| v.get("period_secs"))
        .and_then(Value::as_i64)
        .unwrap_or(DEFAULT_PERIOD_SECS);
    let stat = var(ctx, "stat", DEFAULT_STAT);
    descriptors
        .iter()
        .enumerate()
        .map(|(i, m)| {
            json!({
                "Id": format!("q{i}"),
                "MetricStat": {
                    "Metric": {
                        "Namespace": m["Namespace"],
                        "MetricName": m["MetricName"],
                        "Dimensions": m.get("Dimensions").cloned().unwrap_or_else(|| json!([]))
                    },
                    "Period": period,
                    "Stat": stat
                }
            })
        })
        .collect()
}

/// The rows one `GetMetricData` response holds, joined onto the batch's
/// descriptors (`ids` in the context) by query id.
pub(super) fn expand(response: &[u8], ctx: &TemplateCtx) -> Result<Vec<Bytes>> {
    let response: MetricDataResponse = serde_json::from_slice(response).map_err(|e| {
        Error::Decode(format!(
            "cloudwatch_metrics: the row is not a GetMetricData response: {e}"
        ))
    })?;
    let descriptors = ctx.get("ids").and_then(Value::as_array);
    let mut points = Vec::new();
    for result in &response.results {
        let descriptor = result
            .id
            .strip_prefix('q')
            .and_then(|n| n.parse::<usize>().ok())
            .and_then(|i| descriptors?.get(i))
            .ok_or_else(|| {
                Error::Decode(format!(
                    "cloudwatch_metrics: result `{}` matches no query of the batch",
                    result.id
                ))
            })?;
        for (timestamp, value) in result.timestamps.iter().zip(&result.values) {
            points.push(Point {
                descriptor,
                timestamp: *timestamp,
                value: *value,
            });
        }
    }
    let stat = var(ctx, "stat", DEFAULT_STAT);
    if var(ctx, "output_format", "json") == "otlp" {
        if points.is_empty() {
            return Ok(Vec::new());
        }
        return otlp_record(&points, stat, var(ctx, "region", "")).map(|record| vec![record]);
    }
    points
        .iter()
        .map(|p| {
            serde_json::to_vec(&json!({
                "namespace": p.descriptor["Namespace"],
                "metric_name": p.descriptor["MetricName"],
                "dimensions": p.descriptor.get("Dimensions").cloned().unwrap_or_else(|| json!([])),
                "unit": p.descriptor.get("Unit").and_then(Value::as_str).unwrap_or("None"),
                "timestamp": p.timestamp,
                "value": p.value,
                "stat": stat
            }))
            .map(Bytes::from)
            .map_err(|e| Error::Decode(format!("cloudwatch_metrics: {e}")))
        })
        .collect()
}

/// CloudWatch unit names as UCUM codes, the OTel convention.
fn ucum(unit: &str) -> &str {
    match unit {
        "Seconds" => "s",
        "Microseconds" => "us",
        "Milliseconds" => "ms",
        "Bytes" => "By",
        "Kilobytes" => "kBy",
        "Megabytes" => "MBy",
        "Gigabytes" => "GBy",
        "Terabytes" => "TBy",
        "Bits" => "bit",
        "Kilobits" => "kbit",
        "Megabits" => "Mbit",
        "Gigabits" => "Gbit",
        "Terabits" => "Tbit",
        "Percent" => "%",
        "Count" => "{Count}",
        "Bytes/Second" => "By/s",
        "Kilobytes/Second" => "kBy/s",
        "Megabytes/Second" => "MBy/s",
        "Gigabytes/Second" => "GBy/s",
        "Terabytes/Second" => "TBy/s",
        "Bits/Second" => "bit/s",
        "Kilobits/Second" => "kbit/s",
        "Megabits/Second" => "Mbit/s",
        "Gigabits/Second" => "Gbit/s",
        "Terabits/Second" => "Tbit/s",
        "Count/Second" => "{Count}/s",
        _ => "1",
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_owned())),
        }),
    }
}

/// One `ExportMetricsServiceRequest` over the points: a Gauge per
/// descriptor, the namespace and dimensions as datapoint attributes, the
/// statistic as metric metadata, the region on the resource.
fn otlp_record(points: &[Point<'_>], stat: &str, region: &str) -> Result<Bytes> {
    let mut grouped: BTreeMap<String, (&Value, Vec<NumberDataPoint>)> = BTreeMap::new();
    for point in points {
        let d = point.descriptor;
        let mut attributes = vec![kv("Namespace", d["Namespace"].as_str().unwrap_or_default())];
        if let Value::Array(dimensions) = &d["Dimensions"] {
            for dimension in dimensions {
                let name = dimension["Name"].as_str().unwrap_or_default();
                if !name.is_empty() {
                    attributes.push(kv(name, dimension["Value"].as_str().unwrap_or_default()));
                }
            }
        }
        let data_point = NumberDataPoint {
            attributes,
            start_time_unix_nano: 0,
            time_unix_nano: (point.timestamp * 1_000_000_000.0) as u64,
            exemplars: Vec::new(),
            flags: 0,
            value: Some(number_data_point::Value::AsDouble(point.value)),
        };
        grouped
            .entry(d.to_string())
            .or_insert_with(|| (d, Vec::new()))
            .1
            .push(data_point);
    }
    let metrics = grouped
        .into_values()
        .map(|(d, data_points)| Metric {
            name: d["MetricName"].as_str().unwrap_or_default().to_owned(),
            description: String::new(),
            unit: ucum(d.get("Unit").and_then(Value::as_str).unwrap_or("None")).to_owned(),
            metadata: vec![kv("stat", stat)],
            data: Some(metric::Data::Gauge(Gauge { data_points })),
        })
        .collect();
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![
                    kv("cloud.provider", "aws"),
                    kv("cloud.region", region),
                    kv("service.name", "dfe-fetcher"),
                ],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "dfe-fetcher".to_owned(),
                    version: env!("CARGO_PKG_VERSION").to_owned(),
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                }),
                metrics,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let mut buf = Vec::with_capacity(request.encoded_len());
    request
        .encode(&mut buf)
        .map_err(|e| Error::Decode(format!("cloudwatch_metrics: OTLP encoding: {e}")))?;
    Ok(Bytes::from(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(vars: Value, ids: Vec<Value>) -> TemplateCtx {
        let mut ctx = TemplateCtx::new();
        ctx.set("vars", vars);
        ctx.set("ids", Value::Array(ids));
        ctx
    }

    fn descriptor(name: &str, unit: Option<&str>) -> Value {
        let mut d = json!({"Namespace": "AWS/EC2", "MetricName": name, "Dimensions": [{"Name": "InstanceId", "Value": "i-1"}]});
        if let Some(unit) = unit {
            d["Unit"] = json!(unit);
        }
        d
    }

    #[test]
    fn descriptors_become_positional_queries_with_the_period_and_stat_of_the_vars() {
        let ids = vec![
            descriptor("CPUUtilization", Some("Percent")),
            descriptor("NetworkIn", None),
        ];
        let queries = queries(
            &ids,
            &ctx(json!({"period_secs": 60, "stat": "Maximum"}), vec![]),
        );
        assert_eq!(
            queries,
            [
                json!({"Id": "q0", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "CPUUtilization", "Dimensions": [{"Name": "InstanceId", "Value": "i-1"}]}, "Period": 60, "Stat": "Maximum"}}),
                json!({"Id": "q1", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "NetworkIn", "Dimensions": [{"Name": "InstanceId", "Value": "i-1"}]}, "Period": 60, "Stat": "Maximum"}}),
            ],
            "the unit is left out of the query so CloudWatch answers every datapoint"
        );
        let defaults = super::queries(&ids[..1], &ctx(json!({}), vec![]));
        assert_eq!(defaults[0]["MetricStat"]["Period"], 300);
        assert_eq!(defaults[0]["MetricStat"]["Stat"], "Average");
    }

    #[test]
    fn a_response_joins_each_result_onto_its_descriptor_as_one_row_per_datapoint() {
        let ids = vec![
            descriptor("CPUUtilization", Some("Percent")),
            descriptor("NetworkIn", None),
        ];
        let response = json!({"MetricDataResults": [
            {"Id": "q1", "Label": "NetworkIn", "Timestamps": [1_709_424_300.0], "Values": [7.0], "StatusCode": "Complete"},
            {"Id": "q0", "Label": "CPUUtilization", "Timestamps": [1_709_424_000.0, 1_709_424_300.0], "Values": [45.2, 62.1], "StatusCode": "Complete"}
        ]});
        let rows = expand(
            &serde_json::to_vec(&response).unwrap(),
            &ctx(json!({}), ids.clone()),
        )
        .unwrap();
        let rows: Vec<Value> = rows
            .iter()
            .map(|r| serde_json::from_slice(r).unwrap())
            .collect();
        assert_eq!(
            rows,
            [
                json!({"namespace": "AWS/EC2", "metric_name": "NetworkIn", "dimensions": [{"Name": "InstanceId", "Value": "i-1"}], "unit": "None", "timestamp": 1_709_424_300.0, "value": 7.0, "stat": "Average"}),
                json!({"namespace": "AWS/EC2", "metric_name": "CPUUtilization", "dimensions": [{"Name": "InstanceId", "Value": "i-1"}], "unit": "Percent", "timestamp": 1_709_424_000.0, "value": 45.2, "stat": "Average"}),
                json!({"namespace": "AWS/EC2", "metric_name": "CPUUtilization", "dimensions": [{"Name": "InstanceId", "Value": "i-1"}], "unit": "Percent", "timestamp": 1_709_424_300.0, "value": 62.1, "stat": "Average"}),
            ],
            "joined by query position, whatever order the results come back in"
        );
        let unknown =
            json!({"MetricDataResults": [{"Id": "q9", "Timestamps": [1.0], "Values": [1.0]}]});
        let err = expand(&serde_json::to_vec(&unknown).unwrap(), &ctx(json!({}), ids)).unwrap_err();
        assert!(err.to_string().contains("q9"), "{err}");
        assert!(
            expand(b"not json", &ctx(json!({}), vec![])).is_err(),
            "not a response"
        );
        assert_eq!(
            expand(br#"{"MetricDataResults": []}"#, &ctx(json!({}), vec![])).unwrap(),
            [] as [bytes::Bytes; 0]
        );
    }

    #[test]
    fn otlp_output_is_one_export_request_with_a_gauge_per_metric() {
        let ids = vec![
            descriptor("CPUUtilization", Some("Percent")),
            descriptor("DiskReadOps", Some("Count")),
        ];
        let response = json!({"MetricDataResults": [
            {"Id": "q0", "Timestamps": [1_709_424_000.0, 1_709_424_300.0], "Values": [45.2, 62.1]},
            {"Id": "q1", "Timestamps": [1_709_424_000.0], "Values": [3.0]}
        ]});
        let vars = json!({"output_format": "otlp", "region": "ap-southeast-2", "stat": "Sum"});
        let rows = expand(
            &serde_json::to_vec(&response).unwrap(),
            &ctx(vars.clone(), ids.clone()),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let request = ExportMetricsServiceRequest::decode(rows[0].as_ref()).unwrap();
        let rm = &request.resource_metrics[0];
        let resource: Vec<(&str, &str)> = rm
            .resource
            .as_ref()
            .unwrap()
            .attributes
            .iter()
            .map(|kv| {
                let Some(any_value::Value::StringValue(v)) =
                    kv.value.as_ref().and_then(|v| v.value.as_ref())
                else {
                    panic!("string")
                };
                (kv.key.as_str(), v.as_str())
            })
            .collect();
        assert_eq!(
            resource,
            [
                ("cloud.provider", "aws"),
                ("cloud.region", "ap-southeast-2"),
                ("service.name", "dfe-fetcher")
            ]
        );
        let metrics = &rm.scope_metrics[0].metrics;
        assert_eq!(metrics.len(), 2);
        let cpu = metrics.iter().find(|m| m.name == "CPUUtilization").unwrap();
        assert_eq!(cpu.unit, "%");
        let Some(metric::Data::Gauge(gauge)) = &cpu.data else {
            panic!("gauge")
        };
        assert_eq!(gauge.data_points.len(), 2);
        assert_eq!(
            gauge.data_points[0].time_unix_nano,
            1_709_424_000_000_000_000
        );
        assert_eq!(
            gauge.data_points[0].attributes.len(),
            2,
            "Namespace plus one dimension"
        );
        let disk = metrics.iter().find(|m| m.name == "DiskReadOps").unwrap();
        assert_eq!(disk.unit, "{Count}");
        assert!(
            expand(br#"{"MetricDataResults": []}"#, &ctx(vars, ids))
                .unwrap()
                .is_empty(),
            "no datapoints, no record"
        );
        assert_eq!(ucum("Furlongs"), "1");
    }
}
