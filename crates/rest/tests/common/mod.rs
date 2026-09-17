// Project:   dfe-fetcher
// File:      crates/rest/tests/common/mod.rs
// Purpose:   An in-test HTTP provider (axum on :0) exercising every pager, decoder and auth mode
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The fixture provider.
//!
//! A real HTTP server the tests own, bound to port 0, serving the response
//! shapes the live surveys recorded: Link-header paging, a `next_key` cursor
//! that terminates with an empty string on a full page, page-number and offset
//! paging with totals, a windowed endpoint that records the bounds it was
//! asked for, NDJSON with and without a trailing newline, a 0-byte body, gzip
//! bodies, three auth modes with an OAuth2 token endpoint, the retry and
//! error-text cases, a manifest listing blobs by URL, and a subscription
//! start that answers 400 once enabled. Every request is recorded so a test
//! can assert what the shape actually sent.

#![allow(dead_code)]

use std::collections::HashMap;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine as _;
use futures::StreamExt as _;
use serde_json::{Value, json};

/// One request the fixture saw.
#[derive(Debug, Clone)]
pub struct Seen {
    pub path: String,
    pub query: Vec<(String, String)>,
    pub authorization: Option<String>,
    pub body: Option<Value>,
    /// Every header of the request, lower-cased, for a provider whose
    /// credential does not travel in `Authorization`.
    pub headers: Vec<(String, String)>,
}

impl Seen {
    /// The value of a header the request carried.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(seen, _)| seen == name)
            .map(|(_, value)| value.as_str())
    }
}

#[derive(Debug, Default)]
pub struct Recorded {
    pub requests: Vec<Seen>,
    pub token_exchanges: u32,
    pub flaky_hits: u32,
    pub forbidden_hits: u32,
    /// The `Host` header of each token exchange, in order: which host the
    /// exchange was actually posted to.
    pub token_hosts: Vec<String>,
    /// The lifetime minted tokens advertise; the endpoint's own default when
    /// unset. Zero makes every request its own exchange, which is how a test
    /// drives a renewal without waiting for one.
    pub token_ttl_secs: Option<u64>,
    /// The first token `/auth/revoked` saw, which it refuses from then on.
    pub revoked_token: Option<String>,
    /// The scope each minted token was exchanged for, by token.
    pub token_scopes: HashMap<String, String>,
    /// The claims of every JWT-bearer assertion the exchange verified.
    pub assertions: Vec<Value>,
    /// The RSA public key (SPKI PEM) assertions are verified against.
    pub jwt_public_key: Option<String>,
    /// Every `jti` an assertion has already been accepted with; a repeat is
    /// refused, as Okta refuses a replayed assertion.
    pub assertion_ids: std::collections::HashSet<String>,
    pub metadata_hits: u32,
    /// The content types `/prelude/start` has enabled, in order.
    pub started: Vec<String>,
    /// The ack ids `/queue/ack` has received, one entry per request.
    pub acked: Vec<Vec<String>>,
    /// Pulls served by `/queue/pull` so far.
    pub pulls: u32,
}

#[derive(Clone)]
pub struct Fixture {
    pub addr: SocketAddr,
    pub recorded: Arc<Mutex<Recorded>>,
}

impl Fixture {
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn requests_to(&self, path: &str) -> Vec<Seen> {
        self.recorded
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|s| s.path == path)
            .cloned()
            .collect()
    }

    pub fn token_exchanges(&self) -> u32 {
        self.recorded.lock().unwrap().token_exchanges
    }

    /// The hosts the token exchanges were posted to, in order.
    pub fn token_hosts(&self) -> Vec<String> {
        self.recorded.lock().unwrap().token_hosts.clone()
    }

    /// How long a minted token advertises it lives. Zero leaves every token
    /// past its renewal point the moment it is read.
    pub fn set_token_ttl(&self, secs: u64) {
        self.recorded.lock().unwrap().token_ttl_secs = Some(secs);
    }

    /// The scope a minted token carries.
    pub fn scope_of(&self, token: &str) -> Option<String> {
        self.recorded
            .lock()
            .unwrap()
            .token_scopes
            .get(token)
            .cloned()
    }

    /// The claims of the JWT-bearer assertions verified so far.
    pub fn assertions(&self) -> Vec<Value> {
        self.recorded.lock().unwrap().assertions.clone()
    }

    /// Accept JWT-bearer assertions signed by the key this public PEM pairs
    /// with.
    pub fn accept_assertions_from(&self, public_key_pem: &str) {
        self.recorded.lock().unwrap().jwt_public_key = Some(public_key_pem.to_owned());
    }

    pub fn metadata_hits(&self) -> u32 {
        self.recorded.lock().unwrap().metadata_hits
    }

    /// The content types the prelude route has enabled so far.
    pub fn started(&self) -> Vec<String> {
        self.recorded.lock().unwrap().started.clone()
    }

    /// The ack batches the queue route has received so far.
    pub fn acked(&self) -> Vec<Vec<String>> {
        self.recorded.lock().unwrap().acked.clone()
    }

    /// The paths of every request seen, in order.
    pub fn paths(&self) -> Vec<String> {
        self.recorded
            .lock()
            .unwrap()
            .requests
            .iter()
            .map(|s| s.path.clone())
            .collect()
    }
}

/// A throwaway RSA key pair: the private key as a PKCS#8 PEM and the public
/// key as an SPKI PEM, generated per test so no key is ever committed.
pub fn rsa_key_pair() -> (String, String) {
    use aws_lc_rs::encoding::AsDer;
    use aws_lc_rs::rsa::{KeyPair, KeySize};
    use aws_lc_rs::signature::KeyPair as _;

    let pair = KeyPair::generate(KeySize::Rsa2048).unwrap();
    let pem = |label: &str, der: &[u8]| {
        let body = base64::engine::general_purpose::STANDARD.encode(der);
        let lines: Vec<&str> = body
            .as_bytes()
            .chunks(64)
            .map(|c| std::str::from_utf8(c).unwrap())
            .collect();
        format!(
            "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
            lines.join("\n")
        )
    };
    let private = pem("PRIVATE KEY", pair.as_der().unwrap().as_ref());
    let public = pem("PUBLIC KEY", pair.public_key().as_der().unwrap().as_ref());
    (private, public)
}

/// A Google-style service-account key JSON around a private key PEM.
pub fn service_account_key(private_key_pem: &str, token_uri: &str) -> String {
    json!({
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "kid-1",
        "private_key": private_key_pem,
        "client_email": "fetcher@test-project.iam.gserviceaccount.com",
        "client_id": "1234567890",
        "token_uri": token_uri,
    })
    .to_string()
}

type Shared = Arc<Mutex<Recorded>>;

fn record(
    state: &Shared,
    path: &str,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
    body: Option<Value>,
) {
    let mut query: Vec<(String, String)> =
        query.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    query.sort();
    state.lock().unwrap().requests.push(Seen {
        path: path.to_owned(),
        query,
        authorization: headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        body,
        headers: headers
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect(),
    });
}

fn rows(from: u64, count: u64) -> Vec<Value> {
    (from..from + count).map(|i| json!({"id": i})).collect()
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(bytes).unwrap();
    enc.finish().unwrap()
}

/// What a client authenticating with a signed JWT says it is presenting
/// (RFC 7523 s2.2).
const CLIENT_ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// The token endpoint: client credentials for `client-a`, a client assertion
/// in place of that secret (Okta's `private_key_jwt`), or a JWT-bearer
/// assertion -- the last two verified against the public key a test
/// registered. A minted token remembers the scope it was exchanged for.
async fn token(
    State(state): State<Shared>,
    headers: HeaderMap,
    axum::Form(form): axum::Form<HashMap<String, String>>,
) -> Response {
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1")
        .to_owned();
    let mut recorded = state.lock().unwrap();
    recorded.token_exchanges += 1;
    recorded.token_hosts.push(host.clone());
    let n = recorded.token_exchanges;
    let refused = || {
        (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": "bad client"})),
        )
            .into_response()
    };
    // A token lives an hour unless a test sets its own lifetime, so a test that
    // is not about expiry is never racing one.
    let ttl = recorded.token_ttl_secs.unwrap_or(3600);
    let (scope, expires_in) = match form.get("grant_type").map(String::as_str) {
        // The client proves itself with a signed assertion rather than a
        // secret. Okta refuses a reused `jti`, so this does too: a mint that
        // replayed the last assertion is a failed exchange, not a silent pass.
        Some("client_credentials") if form.contains_key("client_assertion") => {
            if form.get("client_assertion_type").map(String::as_str) != Some(CLIENT_ASSERTION_TYPE)
            {
                return refused();
            }
            let Some(claims) = verified_claims(&mut recorded, form.get("client_assertion")) else {
                return refused();
            };
            let jti = claims["jti"].as_str().unwrap_or_default().to_owned();
            if jti.is_empty() || !recorded.assertion_ids.insert(jti) {
                return refused();
            }
            recorded.assertions.push(claims);
            (form.get("scope").cloned().unwrap_or_default(), ttl)
        }
        Some("client_credentials") => {
            if form.get("client_id").map(String::as_str) != Some("client-a")
                || form.get("client_secret").map(String::as_str) != Some("secret-a")
            {
                return refused();
            }
            (form.get("scope").cloned().unwrap_or_default(), ttl)
        }
        Some("urn:ietf:params:oauth:grant-type:jwt-bearer") => {
            let Some(claims) = verified_claims(&mut recorded, form.get("assertion")) else {
                return refused();
            };
            let scope = claims["scope"].as_str().unwrap_or_default().to_owned();
            recorded.assertions.push(claims);
            (scope, ttl)
        }
        _ => return refused(),
    };
    let token = format!("tok-{n}");
    recorded.token_scopes.insert(token.clone(), scope);
    // The Salesforce shape: the org's own host rides beside the token, as
    // does a refresh token a profile must never be able to expose.
    axum::Json(json!({
        "access_token": token,
        "expires_in": expires_in,
        "token_type": "Bearer",
        "instance_url": format!("http://{host}/instance"),
        "refresh_token": "never-exposed"
    }))
    .into_response()
}

/// The claims of an RS256 assertion verified against the public key a test
/// registered, or `None` when there is no key, no assertion, or the signature
/// does not check out.
fn verified_claims(recorded: &mut Recorded, assertion: Option<&String>) -> Option<Value> {
    let public = recorded.jwt_public_key.clone()?;
    let key = jsonwebtoken::DecodingKey::from_rsa_pem(public.as_bytes()).unwrap();
    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
    validation.validate_aud = false;
    validation.set_required_spec_claims(&["exp", "iat"]);
    jsonwebtoken::decode::<Value>(assertion?, &key, &validation)
        .ok()
        .map(|data| data.claims)
}

/// The Salesforce SOQL shape behind an instance URL the token named: a
/// page of records with a RELATIVE `nextRecordsUrl` until `done`.
async fn instance_query(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/instance/query", &query, &headers, None);
    axum::Json(json!({
        "totalSize": 3,
        "done": false,
        "nextRecordsUrl": "/instance/query/next-2000",
        "records": rows(0, 2)
    }))
    .into_response()
}

async fn instance_query_next(
    State(state): State<Shared>,
    Path(cursor): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(
        &state,
        &format!("/instance/query/{cursor}"),
        &query,
        &headers,
        None,
    );
    axum::Json(json!({"totalSize": 3, "done": true, "records": rows(2, 1)})).into_response()
}

/// The Go module proxy shape: the version list as text lines, then one
/// `.info` document per version, 404 for a version the proxy has retracted.
async fn goproxy_list(
    State(state): State<Shared>,
    Path(module): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(
        &state,
        &format!("/goproxy/{module}/@v/list"),
        &query,
        &headers,
        None,
    );
    match module.as_str() {
        "textmod" => "v0.3.0\nv0.4.0\nv0.5.0\n".into_response(),
        "unpublished" => (StatusCode::NOT_FOUND, "not found: module unknown").into_response(),
        _ => String::new().into_response(),
    }
}

async fn goproxy_info(
    State(state): State<Shared>,
    Path((module, version)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(
        &state,
        &format!("/goproxy/{module}/@v/{version}"),
        &query,
        &headers,
        None,
    );
    let Some(version) = version.strip_suffix(".info") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if version == "v0.4.0" {
        return (StatusCode::NOT_FOUND, "not found: v0.4.0 retracted").into_response();
    }
    axum::Json(
        json!({"Version": version, "Time": format!("2026-0{}-01T00:00:00Z", &version[3..4])}),
    )
    .into_response()
}

/// The S3 shape: a SigV4-signed `ListObjectsV2` answering two XML pages on
/// a continuation token (keys in key order, times out of key order), and
/// `GetObject` answering gzipped NDJSON for one key and plain text for
/// another.
async fn s3_list(
    State(state): State<Shared>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, &format!("/s3/{bucket}"), &query, &headers, None);
    if !headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.starts_with("AWS4-HMAC-SHA256 Credential="))
    {
        return (
            StatusCode::FORBIDDEN,
            "<Error><Code>AccessDenied</Code></Error>",
        )
            .into_response();
    }
    let page = if query.contains_key("continuation-token") {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>logs</Name><IsTruncated>false</IsTruncated>
    <Contents><Key>logs/c.txt</Key><LastModified>2026-05-21T12:00:00.000Z</LastModified><Size>12</Size></Contents>
</ListBucketResult>"#
    } else {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>logs</Name><IsTruncated>true</IsTruncated>
    <NextContinuationToken>page-2</NextContinuationToken>
    <Contents><Key>logs/a b.jsonl.gz</Key><LastModified>2026-05-21T11:00:00.000Z</LastModified><Size>40</Size></Contents>
    <Contents><Key>logs/b.jsonl.gz</Key><LastModified>2026-05-21T10:00:00.000Z</LastModified><Size>40</Size></Contents>
</ListBucketResult>"#
    };
    ([(header::CONTENT_TYPE, "application/xml")], page).into_response()
}

async fn s3_object(
    State(state): State<Shared>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(
        &state,
        &format!("/s3/{bucket}/{key}"),
        &query,
        &headers,
        None,
    );
    match key.as_str() {
        "logs/a b.jsonl.gz" => {
            // Two gzip members, as a log shipper that appends writes them.
            let mut body = gzip(b"{\"id\":\"a-1\"}\n");
            body.extend(gzip(b"{\"id\":\"a-2\"}\n"));
            body.into_response()
        }
        "logs/b.jsonl.gz" => gzip(b"{\"id\":\"b-1\"}\n").into_response(),
        "logs/c.txt" => gzip(b"hello\nworld\n").into_response(),
        _ => (
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchKey</Code></Error>",
        )
            .into_response(),
    }
}

/// The Pub/Sub shape: the first pull answers two messages, every later
/// pull none; the ack records the ids it was given.
async fn queue_pull(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    record(&state, "/queue/pull", &query, &headers, Some(body));
    let mut recorded = state.lock().unwrap();
    recorded.pulls += 1;
    if recorded.pulls > 1 {
        return axum::Json(json!({})).into_response();
    }
    let message = |n: u32, data: &str| {
        json!({
            "ackId": format!("ack-{n}"),
            "message": {
                "data": base64::engine::general_purpose::STANDARD.encode(data),
                "messageId": format!("m-{n}"),
                "publishTime": "2026-05-21T10:00:00.000Z"
            }
        })
    };
    axum::Json(json!({"receivedMessages": [message(1, "{\"n\":1}"), message(2, "{\"n\":2}")]}))
        .into_response()
}

async fn queue_ack(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    record(&state, "/queue/ack", &query, &headers, Some(body.clone()));
    let ids: Vec<String> = body["ackIds"]
        .as_array()
        .map(|ids| {
            ids.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    state.lock().unwrap().acked.push(ids);
    axum::Json(json!({})).into_response()
}

/// The GCE metadata server's token endpoint: answers only a request that
/// carries `Metadata-Flavor: Google`.
async fn metadata_token(State(state): State<Shared>, headers: HeaderMap) -> Response {
    let mut recorded = state.lock().unwrap();
    recorded.metadata_hits += 1;
    if headers.get("metadata-flavor").and_then(|v| v.to_str().ok()) != Some("Google") {
        return (
            StatusCode::FORBIDDEN,
            "Missing Metadata-Flavor:Google header",
        )
            .into_response();
    }
    let n = recorded.metadata_hits;
    axum::Json(
        json!({"access_token": format!("meta-{n}"), "expires_in": 3599, "token_type": "Bearer"}),
    )
    .into_response()
}

/// A route that admits only a token minted for the audience in its path.
async fn scoped(
    State(state): State<Shared>,
    Path(audience): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(
        &state,
        &format!("/scoped/{audience}"),
        &query,
        &headers,
        None,
    );
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default();
    let scope = state.lock().unwrap().token_scopes.get(token).cloned();
    if scope.as_deref() == Some(audience.as_str()) {
        axum::Json(json!([{"audience": audience, "token": token}])).into_response()
    } else {
        (
            StatusCode::UNAUTHORIZED,
            axum::Json(
                json!({"error": {"message": format!("token is for {scope:?}, not {audience}")}}),
            ),
        )
            .into_response()
    }
}

/// The Log Analytics query shape: a POST answered with columnar tables.
async fn columnar_query(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    record(&state, "/columnar/query", &query, &headers, Some(body));
    axum::Json(json!({"tables": [
        {"name": "PrimaryResult",
         "columns": [{"name": "TimeGenerated", "type": "datetime"}, {"name": "Computer", "type": "string"}],
         "rows": [["2026-01-01T00:00:00Z", "web-1"], ["2026-01-01T00:01:00Z", "web-2"]]},
        {"name": "Second",
         "columns": [{"name": "Count", "type": "long"}],
         "rows": [[3]]}
    ]}))
    .into_response()
}

async fn link_page(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/link/page", &query, &headers, None);
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1")
        .to_owned();
    let page: u64 = query.get("page").and_then(|p| p.parse().ok()).unwrap_or(1);
    let body = axum::Json(rows(page * 10, 2));
    if page < 3 {
        let next = format!("<http://{host}/link/page?page={}>; rel=\"next\"", page + 1);
        ([(header::LINK, next)], body).into_response()
    } else {
        body.into_response()
    }
}

async fn cursor_assets(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/cursor/assets.json", &query, &headers, None);
    let start = query
        .get("start_key")
        .and_then(|k| k.strip_prefix("key-"))
        .and_then(|k| k.parse::<u64>().ok())
        .unwrap_or(0);
    let page_size: u64 = query
        .get("page_size")
        .and_then(|p| p.parse().ok())
        .unwrap_or(5);
    // 15 rows in total: three full pages, the last carrying next_key "".
    let next_key = if start + page_size >= 15 {
        String::new()
    } else {
        format!("key-{}", start + page_size)
    };
    axum::Json(json!({"assets": rows(start, page_size), "next_key": next_key})).into_response()
}

async fn number_items(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/number/items", &query, &headers, None);
    let page: u64 = query.get("page").and_then(|p| p.parse().ok()).unwrap_or(1);
    axum::Json(json!({"result": rows(page * 100, 3), "result_info": {"total_pages": 3}}))
        .into_response()
}

async fn offset_items(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/offset/items", &query, &headers, None);
    let offset: u64 = query
        .get("offset")
        .and_then(|p| p.parse().ok())
        .unwrap_or(0);
    let remaining = 250u64.saturating_sub(offset).min(100);
    axum::Json(
        json!({"resources": rows(offset, remaining), "meta": {"pagination": {"total": 250}}}),
    )
    .into_response()
}

async fn window_events(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/window/events", &query, &headers, None);
    axum::Json(json!([{"window": query.get("start").cloned().unwrap_or_default()}])).into_response()
}

async fn ndjson(
    State(state): State<Shared>,
    Path(name): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, &format!("/ndjson/{name}"), &query, &headers, None);
    let two = "{\"id\":1}\n{\"id\":2}";
    match name.as_str() {
        "trailing.jsonl" => format!("{two}\n").into_response(),
        "notrailing.jsonl" => two.to_owned().into_response(),
        "empty.jsonl" => String::new().into_response(),
        "single.jsonl" => "{\"only\":true}".to_owned().into_response(),
        "raw.jsonl.gz" => (
            [(header::CONTENT_TYPE, "application/gzip")],
            gzip(format!("{two}\n").as_bytes()),
        )
            .into_response(),
        "encoded.jsonl" => (
            [
                (header::CONTENT_ENCODING, "gzip"),
                (header::CONTENT_TYPE, "application/jsonl"),
            ],
            gzip(format!("{two}\n").as_bytes()),
        )
            .into_response(),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn json_array(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/array/items.json", &query, &headers, None);
    axum::Json(rows(0, 4)).into_response()
}

/// `count` NDJSON rows streamed one per `every_ms`, so the body outlives any
/// total timeout shorter than `count x every_ms` while no single read waits
/// longer than `every_ms`.
async fn trickle(
    State(state): State<Shared>,
    Path(count): Path<u64>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, &format!("/trickle/{count}"), &query, &headers, None);
    let every = std::time::Duration::from_millis(
        query
            .get("every_ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(100),
    );
    let lines = futures::stream::iter(0..count).then(move |i| async move {
        if i > 0 {
            tokio::time::sleep(every).await;
        }
        Ok::<_, std::io::Error>(Bytes::from(format!("{{\"id\":{i}}}\n")))
    });
    (
        [(header::CONTENT_TYPE, "application/x-ndjson")],
        axum::body::Body::from_stream(lines),
    )
        .into_response()
}

/// One row, then nothing for `hold_ms`, then a second row: a body that
/// stalls mid-stream.
async fn stall(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/stall", &query, &headers, None);
    let hold = std::time::Duration::from_millis(
        query
            .get("hold_ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(2000),
    );
    let lines = futures::stream::iter(0..2u64).then(move |i| async move {
        if i > 0 {
            tokio::time::sleep(hold).await;
        }
        Ok::<_, std::io::Error>(Bytes::from(format!("{{\"id\":{i}}}\n")))
    });
    axum::body::Body::from_stream(lines).into_response()
}

/// A 302 to `to` (any URL) for `cross`, or to this fixture's own array
/// route for `same`.
async fn redirect(
    State(state): State<Shared>,
    Path(kind): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, &format!("/redirect/{kind}"), &query, &headers, None);
    let to = match kind.as_str() {
        "same" => "/array/items.json".to_owned(),
        _ => query.get("to").cloned().unwrap_or_default(),
    };
    (StatusCode::FOUND, [(header::LOCATION, to)]).into_response()
}

async fn auth_gate(
    State(state): State<Shared>,
    Path(mode): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, &format!("/auth/{mode}"), &query, &headers, None);
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let ok = match mode.as_str() {
        "bearer" => auth == "Bearer secret-token",
        "oauth" => auth.starts_with("Bearer tok-") || auth.starts_with("Bearer meta-"),
        "apikey" => auth == "SSWS the-key",
        // Two credentials on the one request, as Datadog wants them: the gate
        // refuses either key alone, so a 2xx is proof both arrived.
        "twokeys" => {
            let key = |name: &str| {
                headers
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
            };
            key("dd-api-key") == "api-key-value" && key("dd-application-key") == "app-key-value"
        }
        // Both halves of a key pair inside one header value, as Tenable wants
        // them: the gate checks the composed string, so a 2xx is proof it was
        // composed and placed whole.
        "composed" => auth == "accessKey=access-key-value;secretKey=secret-key-value",
        _ => false,
    };
    if ok {
        axum::Json(json!([{"mode": mode, "authorization": auth}])).into_response()
    } else {
        (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": "refused"})),
        )
            .into_response()
    }
}

/// A provider that revokes the first token it is shown and accepts the next:
/// what a rotated client, a revoked grant or a signing-key roll looks like from
/// the fetcher's side, well inside the token's advertised lifetime.
async fn revoked(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/revoked/data", &query, &headers, None);
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .trim_start_matches("Bearer ")
        .to_owned();
    let mut recorded = state.lock().unwrap();
    let revoked = recorded.revoked_token.get_or_insert(token.clone());
    if *revoked == token {
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": "the token has been revoked"})),
        )
            .into_response();
    }
    axum::Json(json!([{"token": token}])).into_response()
}

async fn flaky(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/retry/flaky", &query, &headers, None);
    let mut recorded = state.lock().unwrap();
    recorded.flaky_hits += 1;
    if recorded.flaky_hits == 1 {
        (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, "0")],
            "slow down",
        )
            .into_response()
    } else {
        axum::Json(rows(0, 1)).into_response()
    }
}

async fn forbidden(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/retry/forbidden", &query, &headers, None);
    state.lock().unwrap().forbidden_hits += 1;
    (
        StatusCode::FORBIDDEN,
        [("x-throttle", "Throttling for block (1/25)")],
        axum::Json(json!({"error": "the API client grant does not permit this"})),
    )
        .into_response()
}

async fn bad_request(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/error/bad", &query, &headers, None);
    (
        StatusCode::BAD_REQUEST,
        axum::Json(json!({"error": "missing or invalid _oid Parameter"})),
    )
        .into_response()
}

async fn post_search(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    record(&state, "/post/search", &query, &headers, Some(body.clone()));
    let cursor = body.get("cursor").and_then(Value::as_str).unwrap_or("");
    let (items, next) = match cursor {
        "" => (rows(0, 2), "c1"),
        "c1" => (rows(2, 2), ""),
        _ => (Vec::new(), ""),
    };
    axum::Json(json!({"items": items, "cursor": next, "has_more": !next.is_empty()}))
        .into_response()
}

/// The 1Password shape: a cursor on EVERY page, `has_more` saying whether to
/// continue, and the request body echoed back so a test can see whether the
/// next call carried the cursor alone.
async fn post_replace(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    record(
        &state,
        "/post/replace",
        &query,
        &headers,
        Some(body.clone()),
    );
    let cursor = body.get("cursor").and_then(Value::as_str).unwrap_or("");
    let (items, next, has_more) = match cursor {
        "" => (rows(0, 2), "r1", true),
        "r1" => (rows(2, 2), "r2", false),
        _ => (Vec::new(), "r3", false),
    };
    axum::Json(json!({"items": items, "cursor": next, "has_more": has_more})).into_response()
}

/// A per-key registry (the PyPI / crates.io shape): one document per key,
/// 404 for a key it does not know.
async fn keyed_document(
    State(state): State<Shared>,
    Path(key): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, &format!("/keyed/{key}"), &query, &headers, None);
    if key == "missing" {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"message": "Not Found"})),
        )
            .into_response();
    }
    axum::Json(json!({"info": {"name": key}, "version": format!("{key}-1.0")})).into_response()
}

/// The CrowdStrike shape, stage one: an offset-paged list of ids with a
/// total; five ids in all.
async fn lookup_ids(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/lookup/ids", &query, &headers, None);
    let offset: usize = query
        .get("offset")
        .and_then(|o| o.parse().ok())
        .unwrap_or(0);
    let limit: usize = query
        .get("limit")
        .and_then(|l| l.parse().ok())
        .unwrap_or(100);
    let ids: Vec<String> = (1..=5).map(|i| format!("id-{i}")).collect();
    let page: Vec<&String> = ids.iter().skip(offset).take(limit).collect();
    axum::Json(json!({"resources": page, "meta": {"pagination": {"total": ids.len()}}}))
        .into_response()
}

/// The CrowdStrike shape, stage two: the entities for a batch of ids.
async fn lookup_entities(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    record(
        &state,
        "/lookup/entities",
        &query,
        &headers,
        Some(body.clone()),
    );
    let entities: Vec<Value> = body["ids"]
        .as_array()
        .map(|ids| {
            ids.iter()
                .filter_map(Value::as_str)
                .map(|id| json!({"id": id, "severity": id.len()}))
                .collect()
        })
        .unwrap_or_default();
    axum::Json(json!({"resources": entities})).into_response()
}

/// A probe that answers 200 either way, `ok: false` when asked to fail (the
/// Slack `auth.test` shape).
async fn probe_status(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/probe/status", &query, &headers, None);
    if query.get("fail").is_some_and(|f| f == "1") {
        axum::Json(json!({"ok": false, "error": "invalid_auth"})).into_response()
    } else {
        axum::Json(json!({"ok": true, "team": "acme"})).into_response()
    }
}

/// The GuardDuty shape, stage zero: the detector ids a keyset request reads.
async fn detectors(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/detector", &query, &headers, None);
    axum::Json(json!({"detectorIds": ["d1", "d2"]})).into_response()
}

/// The GuardDuty shape, stage one: each detector's finding ids, paged by a
/// body `nextToken`; d1 has three, d2 one.
async fn detector_findings(
    State(state): State<Shared>,
    Path(detector): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    record(
        &state,
        &format!("/detector/{detector}/findings"),
        &query,
        &headers,
        Some(body.clone()),
    );
    let all: Vec<String> = match detector.as_str() {
        "d1" => vec!["f1".into(), "f2".into(), "f3".into()],
        "d2" => vec!["f4".into()],
        _ => Vec::new(),
    };
    let max = body["maxResults"].as_u64().unwrap_or(50) as usize;
    let from: usize = body["nextToken"]
        .as_str()
        .and_then(|t| t.strip_prefix("from-"))
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    let page: Vec<&String> = all.iter().skip(from).take(max).collect();
    let mut answer = json!({"findingIds": page});
    if from + max < all.len() {
        answer["nextToken"] = json!(format!("from-{}", from + max));
    }
    axum::Json(answer).into_response()
}

/// The GuardDuty shape, stage two: the findings for a batch of ids, each
/// naming the detector it was asked under.
async fn detector_findings_get(
    State(state): State<Shared>,
    Path(detector): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    record(
        &state,
        &format!("/detector/{detector}/findings/get"),
        &query,
        &headers,
        Some(body.clone()),
    );
    let findings: Vec<Value> = body["findingIds"]
        .as_array()
        .map(|ids| {
            ids.iter()
                .filter_map(Value::as_str)
                .map(|id| json!({"id": id, "detector": detector}))
                .collect()
        })
        .unwrap_or_default();
    axum::Json(json!({"findings": findings})).into_response()
}

/// The CloudWatch shape, stage one: two metric descriptors per namespace
/// asked for.
async fn metrics_list(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    record(
        &state,
        "/metrics/list",
        &query,
        &headers,
        Some(body.clone()),
    );
    let namespace = body["Namespace"].as_str().unwrap_or("none");
    axum::Json(json!({"Metrics": [
        {"Namespace": namespace, "MetricName": "CPUUtilization", "Dimensions": [{"Name": "InstanceId", "Value": "i-1"}], "Unit": "Percent"},
        {"Namespace": namespace, "MetricName": "NetworkIn", "Dimensions": [{"Name": "InstanceId", "Value": "i-1"}]}
    ]}))
    .into_response()
}

/// The CloudWatch shape, stage two: one datapoint per query, with a second
/// page carrying one more when no token was sent.
async fn metrics_data(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    record(
        &state,
        "/metrics/data",
        &query,
        &headers,
        Some(body.clone()),
    );
    let second = body.get("NextToken").is_some();
    let timestamp = if second {
        1_709_424_300.0
    } else {
        1_709_424_000.0
    };
    let results: Vec<Value> = body["MetricDataQueries"]
        .as_array()
        .map(|queries| {
            queries
                .iter()
                .enumerate()
                .map(|(i, q)| {
                    json!({"Id": q["Id"], "Timestamps": [timestamp], "Values": [i as f64 + 0.5], "StatusCode": "Complete"})
                })
                .collect()
        })
        .unwrap_or_default();
    let mut answer = json!({"MetricDataResults": results});
    if !second {
        answer["NextToken"] = json!("more");
    }
    axum::Json(answer).into_response()
}

/// A SigV4-gated route: answers the credential scope the request was
/// signed for, so a test can see which service and region the mode
/// rendered from the unit's context.
async fn sigv4_scope(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/sigv4/scope", &query, &headers, None);
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let scope = authorization
        .split_once("Credential=")
        .and_then(|(_, rest)| rest.split_once(", "))
        .map(|(credential, _)| credential.to_owned())
        .unwrap_or_default();
    if scope.is_empty() || headers.get("x-amz-content-sha256").is_none() {
        return (StatusCode::FORBIDDEN, "unsigned").into_response();
    }
    axum::Json(json!([{"scope": scope}])).into_response()
}

/// The OMAP shape, the manifest: a content list whose items point at
/// blobs on this server, paged by a `NextPageUri` header; the second page
/// names one blob that does not exist.
async fn manifest_list(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/manifest/list", &query, &headers, None);
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1")
        .to_owned();
    let item = |id: &str, created: &str| json!({"uri": format!("http://{host}/manifest/blob/{id}"), "id": id, "created": created});
    if query.get("page").is_some_and(|p| p == "2") {
        axum::Json(vec![
            item("missing", "2026-01-01T00:20:00Z"),
            item("c", "2026-01-01T00:30:00Z"),
        ])
        .into_response()
    } else {
        let next = format!("http://{host}/manifest/list?page=2");
        (
            [("NextPageUri", next)],
            axum::Json(vec![
                item("a", "2026-01-01T00:00:00Z"),
                item("b", "2026-01-01T00:10:00Z"),
            ]),
        )
            .into_response()
    }
}

/// A manifest item's blob: two records named after the item, a 404 for
/// the one the list points at but the store has already expired.
async fn manifest_blob(
    State(state): State<Shared>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(
        &state,
        &format!("/manifest/blob/{id}"),
        &query,
        &headers,
        None,
    );
    if id == "missing" {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"error": {"code": "AF20051", "message": "Content requested has already expired."}})),
        )
            .into_response();
    }
    axum::Json(json!([{"id": format!("{id}-1")}, {"id": format!("{id}-2")}])).into_response()
}

/// The OMAP `subscriptions/start` shape: 200 the first time a content
/// type is started, 400 `AF20024` every time after, as the API answers.
async fn prelude_start(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/prelude/start", &query, &headers, None);
    let content_type = query.get("contentType").cloned().unwrap_or_default();
    let mut recorded = state.lock().unwrap();
    if recorded.started.contains(&content_type) {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": {"code": "AF20024", "message": "The subscription is already enabled. No property change."}})),
        )
            .into_response();
    }
    recorded.started.push(content_type.clone());
    axum::Json(json!({"contentType": content_type, "status": "enabled"})).into_response()
}

/// The content behind a prelude: answers only a content type that has
/// been started, 404 otherwise.
async fn prelude_content(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/prelude/content", &query, &headers, None);
    let content_type = query.get("contentType").cloned().unwrap_or_default();
    if state.lock().unwrap().started.contains(&content_type) {
        axum::Json(json!([{"contentType": content_type}])).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"error": {"code": "AF20022", "message": "No subscription found for the specified content type"}})),
        )
            .into_response()
    }
}

/// A route that always refuses, for a prelude step that must fail the tick.
async fn prelude_refused(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/prelude/refused", &query, &headers, None);
    (
        StatusCode::FORBIDDEN,
        axum::Json(json!({"error": {"message": "the application lacks ActivityFeed.Read"}})),
    )
        .into_response()
}

/// Start the fixture on an ephemeral port; the server stops when the test's
/// runtime drops.
pub async fn start() -> Fixture {
    let recorded: Shared = Arc::new(Mutex::new(Recorded::default()));
    let app = Router::new()
        .route("/token", post(token))
        .route("/link/page", get(link_page))
        .route("/cursor/assets.json", get(cursor_assets))
        .route("/number/items", get(number_items))
        .route("/offset/items", get(offset_items))
        .route("/window/events", get(window_events))
        .route("/ndjson/{name}", get(ndjson))
        .route("/array/items.json", get(json_array))
        .route("/trickle/{count}", get(trickle))
        .route("/stall", get(stall))
        .route("/redirect/{kind}", get(redirect))
        .route("/auth/{mode}", get(auth_gate))
        .route("/revoked/data", get(revoked))
        .route("/retry/flaky", get(flaky))
        .route("/retry/forbidden", get(forbidden))
        .route("/error/bad", get(bad_request))
        .route("/post/search", post(post_search))
        .route("/post/replace", post(post_replace))
        .route("/keyed/{key}", get(keyed_document))
        .route("/lookup/ids", get(lookup_ids))
        .route("/lookup/entities", post(lookup_entities))
        .route("/probe/status", get(probe_status))
        .route("/metadata/token", get(metadata_token))
        .route("/scoped/{audience}", get(scoped))
        .route("/columnar/query", post(columnar_query))
        .route("/detector", get(detectors))
        .route("/detector/{id}/findings", post(detector_findings))
        .route("/detector/{id}/findings/get", post(detector_findings_get))
        .route("/metrics/list", post(metrics_list))
        .route("/metrics/data", post(metrics_data))
        .route("/sigv4/scope", get(sigv4_scope))
        .route("/manifest/list", get(manifest_list))
        .route("/manifest/blob/{id}", get(manifest_blob))
        .route("/prelude/start", post(prelude_start))
        .route("/prelude/content", get(prelude_content))
        .route("/prelude/refused", post(prelude_refused))
        .route("/instance/query", get(instance_query))
        .route("/instance/query/{cursor}", get(instance_query_next))
        .route("/goproxy/{module}/@v/list", get(goproxy_list))
        .route("/goproxy/{module}/@v/{version}", get(goproxy_info))
        .route("/s3/{bucket}", get(s3_list))
        .route("/s3/{bucket}/{*key}", get(s3_object))
        .route("/queue/pull", post(queue_pull))
        .route("/queue/ack", post(queue_ack))
        .with_state(Arc::clone(&recorded));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Fixture { addr, recorded }
}
