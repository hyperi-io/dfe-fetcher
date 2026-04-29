// Project:   dfe-fetcher
// File:      src/bin/pgo-driver.rs
// Purpose:   PGO workload driver — long-running mock cloud-API server
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! PGO workload driver for dfe-fetcher.
//!
//! Long-running HTTP server that mimics the cloud-provider APIs the fetcher
//! polls (Azure Activity Log + Microsoft Graph shapes), so a running,
//! PGO-instrumented `dfe-fetcher` accumulates representative profile data
//! across its hot path:
//!
//!   HTTP fetch → JSON parse → per-record enrichment → CEL filter →
//!   output produce (Kafka) → cursor advance.
//!
//! Invoked by `scripts/pgo-workload.sh` which owns the Kafka testcontainer
//! lifecycle and the fetcher process.
//!
//! Built only with `--features pgo-driver`. Main fetcher binary unaffected.
//!
//! Configuration via environment variables:
//! - `PGO_DRIVER_BIND` (default `127.0.0.1:19090`)
//! - `PGO_DRIVER_PAGE_SIZE` (default 500) — records per response
//! - `PGO_DRIVER_PAGES_BEFORE_END` (default 3) — pagination depth before
//!   responses stop including `@odata.nextLink`
//!
//! Endpoints:
//! - `POST /oauth/token` — OAuth2 client_credentials response (Azure/M365/GCP)
//! - `GET  /azure/activity/...` — Azure Activity Log (`value` + `@odata.nextLink`)
//! - `GET  /azure/graph/...` — Microsoft Graph response shape
//! - `POST /` — AWS JSON dispatch (uses X-Amz-Target header to vary shape)
//! - `POST /gcp/v2/entries:list` — GCP Cloud Logging entries
//!
//! Exits cleanly on SIGINT / SIGTERM (process-group cleanup in workload.sh).

#![allow(clippy::expect_used)] // workload driver, not library code

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Json;
use axum::routing::{any, get, post};
use serde_json::{Value, json};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let cfg = Config::from_env();
    eprintln!("pgo-driver starting: {cfg:#?}");

    let state = Arc::new(DriverState::new(&cfg));

    let app = Router::new()
        .route("/oauth/token", post(oauth_token))
        .route("/oauth2/v2.0/token", post(oauth_token))
        .route("/token", post(oauth_token))
        .route("/azure/activity/{*rest}", get(azure_activity))
        .route("/azure/graph/{*rest}", get(azure_graph))
        .route("/m365/management/{*rest}", get(m365_management))
        .route("/m365/graph/{*rest}", get(m365_graph))
        .route("/gcp/v2/entries:list", post(gcp_entries_list))
        // AWS uses X-Amz-Target header for routing — single POST endpoint.
        .route("/aws", post(aws_dispatch))
        .route("/aws/", post(aws_dispatch))
        // Catch-all: respond with a generic Graph-shaped response. Keeps the
        // hot path warm even if the fetcher hits a route we didn't model.
        .fallback(any(fallback_paginated))
        .with_state(state.clone());

    let addr: SocketAddr = cfg.bind.parse().expect("invalid PGO_DRIVER_BIND");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind PGO_DRIVER_BIND");
    eprintln!("pgo-driver listening on http://{addr}");

    let reporter_state = state.clone();
    let reporter = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(15));
        tick.tick().await;
        loop {
            tick.tick().await;
            reporter_state.report();
        }
    });

    let shutdown = async {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("pgo-driver: SIGINT received");
            }
            _ = sigterm() => {
                eprintln!("pgo-driver: SIGTERM received");
            }
        }
    };

    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown);
    if let Err(e) = server.await {
        eprintln!("pgo-driver: server error: {e}");
    }

    reporter.abort();
    state.report();
    eprintln!("pgo-driver: complete");
}

#[cfg(unix)]
async fn sigterm() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sig = signal(SignalKind::terminate()).expect("install SIGTERM");
    sig.recv().await;
}

#[cfg(not(unix))]
async fn sigterm() {
    std::future::pending::<()>().await;
}

// ===========================================================================
// Config
// ===========================================================================

#[derive(Clone, Debug)]
struct Config {
    bind: String,
    page_size: usize,
    pages_before_end: u32,
}

impl Config {
    fn from_env() -> Self {
        Self {
            bind: env_str("PGO_DRIVER_BIND", "127.0.0.1:19090"),
            page_size: env_usize("PGO_DRIVER_PAGE_SIZE", 500),
            pages_before_end: env_u32("PGO_DRIVER_PAGES_BEFORE_END", 3),
        }
    }
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}
fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ===========================================================================
// Driver state
// ===========================================================================

struct DriverState {
    config: Config,
    /// Monotonic page counter — used to drive `@odata.nextLink` rotation so
    /// the fetcher exercises pagination both with and without follow-on calls.
    page_counter: AtomicU64,
    /// Total responses served — for end-of-run reporting only.
    served: AtomicU64,
    started: Instant,
}

impl DriverState {
    fn new(cfg: &Config) -> Self {
        Self {
            config: cfg.clone(),
            page_counter: AtomicU64::new(0),
            served: AtomicU64::new(0),
            started: Instant::now(),
        }
    }

    fn next_page(&self) -> u64 {
        self.page_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn record(&self) {
        self.served.fetch_add(1, Ordering::Relaxed);
    }

    fn report(&self) {
        let elapsed = self.started.elapsed().as_secs_f64();
        let served = self.served.load(Ordering::Relaxed);
        eprintln!(
            "pgo-driver [{elapsed:>6.1}s] served={served:>8} rate={:.0}/s",
            (served as f64) / elapsed.max(1.0)
        );
    }
}

// ===========================================================================
// Handlers
// ===========================================================================

async fn oauth_token(_state: State<Arc<DriverState>>) -> Json<Value> {
    // Common OAuth2 client_credentials response shape (Azure/M365/Graph).
    Json(json!({
        "access_token": "pgo-mock-token-aaaaaaaaaa",
        "token_type": "Bearer",
        "expires_in": 3600,
        "scope": "https://graph.microsoft.com/.default"
    }))
}

async fn azure_activity(State(state): State<Arc<DriverState>>) -> Json<Value> {
    state.record();
    let page = state.next_page();
    let next = pagination_link(&state, page, "/azure/activity/page");
    Json(json!({
        "value": activity_log_records(state.config.page_size, page),
        "@odata.nextLink": next,
    }))
}

async fn azure_graph(State(state): State<Arc<DriverState>>) -> Json<Value> {
    state.record();
    let page = state.next_page();
    let next = pagination_link(&state, page, "/azure/graph/page");
    Json(json!({
        "value": graph_signin_records(state.config.page_size, page),
        "@odata.nextLink": next,
    }))
}

async fn m365_management(State(state): State<Arc<DriverState>>) -> Json<Value> {
    state.record();
    let page = state.next_page();
    let next = pagination_link(&state, page, "/m365/management/page");
    Json(json!({
        "value": m365_audit_records(state.config.page_size, page),
        "@odata.nextLink": next,
    }))
}

async fn m365_graph(State(state): State<Arc<DriverState>>) -> Json<Value> {
    state.record();
    let page = state.next_page();
    let next = pagination_link(&state, page, "/m365/graph/page");
    Json(json!({
        "value": m365_alert_records(state.config.page_size, page),
        "@odata.nextLink": next,
    }))
}

async fn gcp_entries_list(State(state): State<Arc<DriverState>>) -> Json<Value> {
    state.record();
    let page = state.next_page();
    let token = if (page % u64::from(state.config.pages_before_end.max(1))) != 0 {
        format!("pgtok-{page}")
    } else {
        String::new()
    };
    Json(json!({
        "entries": gcp_audit_log_entries(state.config.page_size, page),
        "nextPageToken": token,
    }))
}

async fn aws_dispatch(State(state): State<Arc<DriverState>>, headers: HeaderMap) -> Json<Value> {
    state.record();
    let page = state.next_page();
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if target.contains("LookupEvents") {
        Json(json!({
            "Events": cloudtrail_events(state.config.page_size, page),
        }))
    } else if target.contains("ListDetectors") {
        Json(json!({
            "DetectorIds": ["pgo-detector-1", "pgo-detector-2"],
        }))
    } else if target.contains("ListFindings") {
        let ids: Vec<String> = (0..state.config.page_size.min(50))
            .map(|i| format!("finding-{page}-{i}"))
            .collect();
        Json(json!({ "FindingIds": ids }))
    } else if target.contains("GetFindings") {
        Json(json!({
            "Findings": guardduty_findings(state.config.page_size.min(50), page),
        }))
    } else if target.contains("SelectAggregateResourceConfig") {
        Json(json!({
            "Results": config_results(state.config.page_size, page),
        }))
    } else {
        // Unknown target — return empty AWS shape so the source code's
        // happy path still runs and exits cleanly without panicking.
        Json(json!({}))
    }
}

async fn fallback_paginated(State(state): State<Arc<DriverState>>) -> Json<Value> {
    state.record();
    let page = state.next_page();
    Json(json!({
        "value": activity_log_records(state.config.page_size, page),
    }))
}

// ===========================================================================
// Helpers
// ===========================================================================

fn pagination_link(state: &DriverState, page: u64, base_path: &str) -> Option<String> {
    let depth = u64::from(state.config.pages_before_end.max(1));
    if (page % depth) == depth - 1 {
        // Final page in the cycle — no nextLink.
        None
    } else {
        Some(format!(
            "http://{}{}/{}",
            state.config.bind, base_path, page
        ))
    }
}

// ===========================================================================
// Payload generators — realistic shapes per source
// ===========================================================================

fn activity_log_records(n: usize, page: u64) -> Vec<Value> {
    (0..n)
        .map(|i| {
            json!({
                "id": format!("act-{page}-{i}"),
                "eventTimestamp": "2026-04-29T12:00:00Z",
                "operationName": { "value": "Microsoft.Compute/virtualMachines/start/action" },
                "category": { "value": "Administrative" },
                "level": "Informational",
                "resourceGroupName": format!("rg-{}", i % 16),
                "subscriptionId": "00000000-0000-0000-0000-000000000001",
                "caller": format!("user-{}@example.com", i % 32),
                "claims": {
                    "aud": "https://management.core.windows.net/",
                    "iss": "https://sts.windows.net/00000000-0000-0000-0000-000000000001/",
                    "appid": "11111111-1111-1111-1111-111111111111",
                },
                "properties": {
                    "statusCode": "OK",
                    "serviceRequestId": format!("svc-req-{page}-{i}"),
                    "eventCategory": "Administrative",
                },
            })
        })
        .collect()
}

fn graph_signin_records(n: usize, page: u64) -> Vec<Value> {
    (0..n)
        .map(|i| {
            json!({
                "id": format!("signin-{page}-{i}"),
                "createdDateTime": "2026-04-29T12:00:00Z",
                "userPrincipalName": format!("user{}@example.com", i % 64),
                "appDisplayName": "Office 365 Exchange Online",
                "ipAddress": format!("203.0.113.{}", i % 255),
                "clientAppUsed": "Browser",
                "status": { "errorCode": 0, "additionalDetails": "" },
                "deviceDetail": {
                    "operatingSystem": "Windows 11",
                    "browser": "Edge 130.0.0",
                    "trustType": "Hybrid Azure AD joined",
                },
                "location": { "city": "Sydney", "countryOrRegion": "AU" },
                "conditionalAccessStatus": "success",
                "isInteractive": true,
            })
        })
        .collect()
}

fn m365_audit_records(n: usize, page: u64) -> Vec<Value> {
    (0..n)
        .map(|i| {
            json!({
                "Id": format!("audit-{page}-{i}"),
                "CreationTime": "2026-04-29T12:00:00",
                "Operation": "FileAccessed",
                "OrganizationId": "00000000-0000-0000-0000-000000000001",
                "RecordType": 6,
                "UserKey": format!("user{}@contoso.com", i % 32),
                "UserType": 0,
                "Workload": "SharePoint",
                "ClientIP": format!("198.51.100.{}", i % 255),
                "ObjectId": format!("https://contoso.sharepoint.com/sites/team/Document{i}.docx"),
                "UserId": format!("user{}@contoso.com", i % 32),
                "EventSource": "SharePoint",
                "ItemType": "File",
                "ListId": "1234abcd-5678-90ef-1234-567890abcdef",
                "Site": "/sites/team",
            })
        })
        .collect()
}

fn m365_alert_records(n: usize, page: u64) -> Vec<Value> {
    (0..n)
        .map(|i| {
            json!({
                "id": format!("alert-{page}-{i}"),
                "title": "Suspicious sign-in activity detected",
                "category": "InitialAccess",
                "severity": if i % 4 == 0 { "high" } else { "medium" },
                "status": "new",
                "createdDateTime": "2026-04-29T12:00:00Z",
                "lastUpdateDateTime": "2026-04-29T12:00:00Z",
                "tenantId": "00000000-0000-0000-0000-000000000001",
                "evidence": [{
                    "@odata.type": "#microsoft.graph.security.userEvidence",
                    "userAccount": { "userPrincipalName": format!("user{}@contoso.com", i % 32) },
                }],
            })
        })
        .collect()
}

fn gcp_audit_log_entries(n: usize, page: u64) -> Vec<Value> {
    (0..n)
        .map(|i| {
            json!({
                "logName": "projects/pgo-project/logs/cloudaudit.googleapis.com%2Factivity",
                "resource": {
                    "type": "gce_instance",
                    "labels": {
                        "project_id": "pgo-project",
                        "zone": "australia-southeast1-a",
                        "instance_id": format!("inst-{}", i % 64),
                    }
                },
                "timestamp": "2026-04-29T12:00:00Z",
                "severity": "NOTICE",
                "insertId": format!("audit-{page}-{i}"),
                "protoPayload": {
                    "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
                    "serviceName": "compute.googleapis.com",
                    "methodName": "v1.compute.instances.start",
                    "authenticationInfo": {
                        "principalEmail": format!("user{}@example.com", i % 32),
                    },
                    "requestMetadata": {
                        "callerIp": format!("203.0.113.{}", i % 255),
                    },
                },
            })
        })
        .collect()
}

fn cloudtrail_events(n: usize, page: u64) -> Vec<Value> {
    (0..n)
        .map(|i| {
            // CloudTrail responses wrap the actual event JSON inside the
            // `CloudTrailEvent` field as a string, mirroring the real API.
            let event = json!({
                "eventVersion": "1.09",
                "userIdentity": {
                    "type": "IAMUser",
                    "principalId": format!("AIDA{:020}", i),
                    "arn": format!("arn:aws:iam::123456789012:user/user-{}", i % 32),
                    "accountId": "123456789012",
                    "userName": format!("user-{}", i % 32),
                },
                "eventTime": "2026-04-29T12:00:00Z",
                "eventSource": "ec2.amazonaws.com",
                "eventName": "DescribeInstances",
                "awsRegion": "ap-southeast-2",
                "sourceIPAddress": format!("203.0.113.{}", i % 255),
                "userAgent": "aws-cli/2.15.30",
                "requestParameters": null,
                "responseElements": null,
                "requestID": format!("req-{page}-{i}"),
                "eventID": format!("evt-{page}-{i}"),
                "readOnly": true,
                "eventType": "AwsApiCall",
                "managementEvent": true,
                "recipientAccountId": "123456789012",
                "eventCategory": "Management",
            });
            json!({
                "EventId": format!("evt-{page}-{i}"),
                "EventName": "DescribeInstances",
                "EventTime": 1_761_739_200_u64 + i as u64,
                "Username": format!("user-{}", i % 32),
                "Resources": [],
                "CloudTrailEvent": serde_json::to_string(&event).unwrap_or_default(),
            })
        })
        .collect()
}

fn guardduty_findings(n: usize, page: u64) -> Vec<Value> {
    (0..n)
        .map(|i| {
            json!({
                "Id": format!("gd-{page}-{i}"),
                "AccountId": "123456789012",
                "Region": "ap-southeast-2",
                "Type": "UnauthorizedAccess:EC2/SSHBruteForce",
                "Severity": 5.0,
                "Title": "EC2 instance under SSH brute-force attack",
                "CreatedAt": "2026-04-29T12:00:00.000Z",
                "UpdatedAt": "2026-04-29T12:00:00.000Z",
                "Resource": {
                    "ResourceType": "Instance",
                    "InstanceDetails": {
                        "InstanceId": format!("i-{:017x}", i),
                        "Tags": [{ "Key": "Environment", "Value": "production" }],
                    },
                },
            })
        })
        .collect()
}

fn config_results(n: usize, page: u64) -> Vec<String> {
    (0..n)
        .map(|i| {
            // Config returns Results as JSON-encoded strings.
            let item = json!({
                "resourceId": format!("i-{:017x}", i),
                "resourceType": "AWS::EC2::Instance",
                "configurationItemCaptureTime": "2026-04-29T12:00:00.000Z",
                "configurationItemStatus": "OK",
                "awsRegion": "ap-southeast-2",
                "accountId": "123456789012",
                "tags": { "Environment": "production", "Owner": format!("team-{}", i % 8) },
                "_meta": { "page": page },
            });
            item.to_string()
        })
        .collect()
}
