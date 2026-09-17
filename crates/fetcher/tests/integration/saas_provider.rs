// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/saas_provider.rs
// Purpose:   An in-test HTTP provider shaped like the SaaS audit APIs the typed source blocks fetch
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The provider the SaaS characterisation tests fetch from.
//!
//! A real HTTP server the test owns, bound to port 0. Every API is served
//! from ONE scripted page sequence: each route wraps the page the request
//! asks for in that API's envelope and paging token -- `Link: rel="next"`
//! (GitHub, Okta), a body cursor (Slack, Bitwarden, Duo), a page number with
//! a total (Cloudflare), a POST body cursor (1Password), an offset query with
//! a total and a second-stage lookup (CrowdStrike) -- plus each API's cheapest
//! authenticated probe and, for the two OAuth2 APIs, a token endpoint. The
//! per-key registries (PyPI, crates.io) serve scripted documents by name. A
//! test may queue statuses to answer before the pages (a 429 with
//! `Retry-After`, a 500) or a 2xx failure body, and reads back every request
//! the server saw: path, decoded query, lowercase headers, JSON body.

// A handler's early return IS the response: the `Err` branch carries it.
#![allow(dead_code, clippy::result_large_err)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha1::Sha1;
use sha2::{Digest as _, Sha512};

/// One request the provider saw.
#[derive(Debug, Clone)]
pub struct Seen {
    pub path: String,
    /// Decoded query pairs, sorted by name.
    pub query: Vec<(String, String)>,
    /// Header values by lowercase name.
    pub headers: BTreeMap<String, String>,
    /// The JSON body of a POST, when there was one.
    pub body: Option<Value>,
}

impl Seen {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    pub fn query_value(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// What the provider answers next.
#[derive(Debug, Default)]
pub struct Script {
    /// The page sequence; page N (1-based) carries a `next` link while page
    /// N+1 exists.
    pub pages: Vec<Vec<Value>>,
    /// Statuses answered, in order, before the first page is served; each
    /// with an optional `Retry-After` in seconds.
    pub before: VecDeque<(u16, Option<u64>)>,
    /// A 200 answered with exactly this body before the first page: the
    /// provider-level failure some APIs report inside a 2xx.
    pub failure_body: Option<Value>,
    /// The probe answers 200 with this body instead of its usual one.
    pub probe_body: Option<Value>,
    /// Documents by key, for the per-key registries; a missing key is a 404.
    pub documents: HashMap<String, Value>,
}

#[derive(Debug, Default)]
pub struct Recorded {
    pub requests: Vec<Seen>,
    pub script: Script,
    /// The form fields of every token exchange, in order.
    pub token_exchanges: Vec<HashMap<String, String>>,
}

#[derive(Clone)]
pub struct Provider {
    pub addr: SocketAddr,
    pub recorded: Arc<Mutex<Recorded>>,
}

/// The Duo secret key the provider verifies signatures against.
pub const DUO_SKEY: &str = "Zh5eGmUq9zpfQnyUIu5OL9iWoMMv5ZNmk3zLJ4Ep";
/// The Duo integration key the provider expects in the Basic user name.
pub const DUO_IKEY: &str = "DIWJ8X6AEYOR5OMC6TQ1";

impl Provider {
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Replace the page script; a fresh sequence per test case.
    pub fn serve(&self, pages: Vec<Vec<Value>>) {
        let mut recorded = self.recorded.lock().unwrap();
        recorded.script.pages = pages;
        recorded.script.before.clear();
        recorded.script.failure_body = None;
    }

    /// Answer `status` (with an optional `Retry-After`) before the pages.
    pub fn answer_first(&self, status: u16, retry_after: Option<u64>) {
        self.recorded
            .lock()
            .unwrap()
            .script
            .before
            .push_back((status, retry_after));
    }

    /// Answer a 200 with `body` before the pages.
    pub fn fail_first(&self, body: Value) {
        self.recorded.lock().unwrap().script.failure_body = Some(body);
    }

    /// The probe answers 200 with `body`.
    pub fn probe_answers(&self, body: Value) {
        self.recorded.lock().unwrap().script.probe_body = Some(body);
    }

    /// Serve `document` for `key` on the per-key registries.
    pub fn document(&self, key: &str, document: Value) {
        self.recorded
            .lock()
            .unwrap()
            .script
            .documents
            .insert(key.to_owned(), document);
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

    pub fn requests(&self) -> Vec<Seen> {
        self.recorded.lock().unwrap().requests.clone()
    }

    pub fn token_exchanges(&self) -> Vec<HashMap<String, String>> {
        self.recorded.lock().unwrap().token_exchanges.clone()
    }
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
    let headers = headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.as_str().to_ascii_lowercase(), v.to_owned()))
        })
        .collect();
    state.lock().unwrap().requests.push(Seen {
        path: path.to_owned(),
        query,
        headers,
        body,
    });
}

/// The page a request selected.
struct PageOut {
    rows: Vec<Value>,
    /// Whether a page follows this one.
    has_next: bool,
    total_pages: usize,
}

/// Record the request, then answer the next queued status or failure body
/// (as `Err`), else the scripted page at `index` (0-based).
fn page(
    state: &Shared,
    path: &str,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
    body: Option<Value>,
    index: usize,
) -> Result<PageOut, Response> {
    record(state, path, query, headers, body);
    let mut recorded = state.lock().unwrap();
    if let Some((status, retry_after)) = recorded.script.before.pop_front() {
        let status = StatusCode::from_u16(status).expect("scripted status");
        let body = axum::Json(json!({"message": "scripted refusal"}));
        return Err(match retry_after {
            Some(secs) => (status, [(header::RETRY_AFTER, secs.to_string())], body).into_response(),
            None => (status, body).into_response(),
        });
    }
    if let Some(body) = recorded.script.failure_body.take() {
        return Err(axum::Json(body).into_response());
    }
    let total_pages = recorded.script.pages.len();
    let rows = recorded
        .script
        .pages
        .get(index)
        .cloned()
        .unwrap_or_default();
    Ok(PageOut {
        rows,
        has_next: index + 1 < total_pages,
        total_pages,
    })
}

/// The 0-based page index a `<prefix>-N` token names; none is the first page.
fn indexed(token: Option<&str>, prefix: &str) -> usize {
    token
        .and_then(|t| t.strip_prefix(prefix))
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

// -----------------------------------------------------------------------------
// GitHub and Okta: a top-level array linked by `Link: rel="next"`.
// -----------------------------------------------------------------------------

/// Serve the scripted page the request asks for, or the next queued status.
fn paged(
    state: &Shared,
    path: &str,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Response {
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1")
        .to_owned();
    let index = query
        .get("page")
        .and_then(|p| p.parse::<usize>().ok())
        .unwrap_or(1)
        .saturating_sub(1);
    match page(state, path, query, headers, None, index) {
        Err(response) => response,
        Ok(out) => {
            let body = axum::Json(out.rows);
            if out.has_next {
                let next = format!(
                    "<http://{host}{path}?page={}&after=cursor-{}&per_page=100>; rel=\"next\"",
                    index + 2,
                    index + 2
                );
                ([(header::LINK, next)], body).into_response()
            } else {
                body.into_response()
            }
        }
    }
}

async fn github_org(
    State(state): State<Shared>,
    Path(org): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    paged(&state, &format!("/orgs/{org}/audit-log"), &query, &headers)
}

async fn github_enterprise(
    State(state): State<Shared>,
    Path(ent): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    paged(
        &state,
        &format!("/enterprises/{ent}/audit-log"),
        &query,
        &headers,
    )
}

async fn okta_logs(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    paged(&state, "/api/v1/logs", &query, &headers)
}

/// The bearer probes: 200 with an object body when an `Authorization` header
/// is present, 401 otherwise.
fn probe(
    state: &Shared,
    path: &'static str,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Response {
    record(state, path, query, headers, None);
    if let Some(body) = state.lock().unwrap().script.probe_body.take() {
        return axum::Json(body).into_response();
    }
    if headers.contains_key(header::AUTHORIZATION) {
        axum::Json(json!({"login": "probe", "ok": true})).into_response()
    } else {
        (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"message": "Requires authentication"})),
        )
            .into_response()
    }
}

async fn github_user(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    probe(&state, "/user", &query, &headers)
}

async fn okta_me(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    probe(&state, "/api/v1/users/me", &query, &headers)
}

// -----------------------------------------------------------------------------
// Slack: `{ok, entries, response_metadata.next_cursor}`, cursor in the query.
// -----------------------------------------------------------------------------

async fn slack_logs(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let index = indexed(query.get("cursor").map(String::as_str), "cursor-");
    match page(&state, "/audit/v1/logs", &query, &headers, None, index) {
        Err(response) => response,
        Ok(out) => axum::Json(json!({
            "ok": true,
            "entries": out.rows,
            "response_metadata": {
                "next_cursor": if out.has_next { format!("cursor-{}", index + 1) } else { String::new() }
            }
        }))
        .into_response(),
    }
}

/// Slack's token probe answers 200 either way, `ok: false` without a token.
async fn slack_auth_test(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/api/auth.test", &query, &headers, None);
    if let Some(body) = state.lock().unwrap().script.probe_body.take() {
        return axum::Json(body).into_response();
    }
    if headers.contains_key(header::AUTHORIZATION) {
        axum::Json(json!({"ok": true, "team": "acme", "user": "audit-bot"})).into_response()
    } else {
        axum::Json(json!({"ok": false, "error": "not_authed"})).into_response()
    }
}

// -----------------------------------------------------------------------------
// Cloudflare: `{success, result, result_info.total_pages}`, 1-based page query.
// -----------------------------------------------------------------------------

async fn cloudflare_audit_logs(
    State(state): State<Shared>,
    Path(account): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let path = format!("/accounts/{account}/audit_logs");
    let index = query
        .get("page")
        .and_then(|p| p.parse::<usize>().ok())
        .unwrap_or(1)
        .saturating_sub(1);
    match page(&state, &path, &query, &headers, None, index) {
        Err(response) => response,
        Ok(out) => axum::Json(json!({
            "success": true,
            "errors": [],
            "messages": [],
            "result": out.rows,
            "result_info": {
                "page": index + 1,
                "per_page": query.get("per_page").and_then(|p| p.parse::<u64>().ok()).unwrap_or(100),
                "total_pages": out.total_pages,
                "count": out.rows.len()
            }
        }))
        .into_response(),
    }
}

async fn cloudflare_verify(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    probe(&state, "/user/tokens/verify", &query, &headers)
}

// -----------------------------------------------------------------------------
// Bitwarden: OAuth2 token endpoint, then `{data, continuationToken}`.
// -----------------------------------------------------------------------------

/// A client-credentials token endpoint: records the form, refuses a wrong
/// secret with a 400, else mints `<prefix>-token-N`.
fn token_endpoint(
    state: &Shared,
    path: &'static str,
    headers: &HeaderMap,
    form: HashMap<String, String>,
    prefix: &str,
    expires_in: u64,
) -> Response {
    record(state, path, &HashMap::new(), headers, None);
    let mut recorded = state.lock().unwrap();
    let secret_ok = form
        .get("client_secret")
        .is_some_and(|s| s.starts_with("secret-"));
    recorded.token_exchanges.push(form);
    let n = recorded.token_exchanges.len();
    if !secret_ok {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "invalid_client"})),
        )
            .into_response();
    }
    axum::Json(json!({
        "access_token": format!("{prefix}-token-{n}"),
        "expires_in": expires_in,
        "token_type": "Bearer"
    }))
    .into_response()
}

async fn bitwarden_token(
    State(state): State<Shared>,
    headers: HeaderMap,
    axum::Form(form): axum::Form<HashMap<String, String>>,
) -> Response {
    token_endpoint(&state, "/connect/token", &headers, form, "bw", 3600)
}

async fn bitwarden_events(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let index = indexed(query.get("continuationToken").map(String::as_str), "ct-");
    match page(&state, "/public/events", &query, &headers, None, index) {
        Err(response) => response,
        Ok(out) => axum::Json(json!({
            "object": "list",
            "data": out.rows,
            "continuationToken": if out.has_next { Value::String(format!("ct-{}", index + 1)) } else { Value::Null }
        }))
        .into_response(),
    }
}

// -----------------------------------------------------------------------------
// 1Password: POST with a window body first, then `{cursor}` only; the
// response always carries a cursor and says `has_more`.
// -----------------------------------------------------------------------------

async fn onepassword_events(
    State(state): State<Shared>,
    Path(kind): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = format!("/api/v2/{kind}");
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let index = indexed(body.get("cursor").and_then(Value::as_str), "op-cursor-");
    match page(&state, &path, &query, &headers, Some(body), index) {
        Err(response) => response,
        Ok(out) => axum::Json(json!({
            "items": out.rows,
            "cursor": format!("op-cursor-{}", index + 1),
            "has_more": out.has_next
        }))
        .into_response(),
    }
}

async fn onepassword_introspect(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    probe(&state, "/api/auth/introspect", &query, &headers)
}

// -----------------------------------------------------------------------------
// PyPI and crates.io: one document per key, 404 for an unknown key.
// -----------------------------------------------------------------------------

fn registry_document(
    state: &Shared,
    path: &str,
    key: &str,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Response {
    record(state, path, query, headers, None);
    let mut recorded = state.lock().unwrap();
    if let Some((status, retry_after)) = recorded.script.before.pop_front() {
        let status = StatusCode::from_u16(status).expect("scripted status");
        let body = axum::Json(json!({"message": "scripted refusal"}));
        return match retry_after {
            Some(secs) => (status, [(header::RETRY_AFTER, secs.to_string())], body).into_response(),
            None => (status, body).into_response(),
        };
    }
    match recorded.script.documents.get(key) {
        Some(document) => axum::Json(document.clone()).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"message": "Not Found"})),
        )
            .into_response(),
    }
}

async fn pypi_package(
    State(state): State<Shared>,
    Path(package): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    registry_document(
        &state,
        &format!("/pypi/{package}/json"),
        &package,
        &query,
        &headers,
    )
}

async fn pypi_root(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/", &query, &headers, None);
    "<html>pypi</html>".into_response()
}

async fn crates_io_crate(
    State(state): State<Shared>,
    Path(name): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    registry_document(
        &state,
        &format!("/api/v1/crates/{name}"),
        &name,
        &query,
        &headers,
    )
}

async fn crates_io_summary(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    record(&state, "/api/v1/summary", &query, &headers, None);
    axum::Json(json!({"num_crates": 1})).into_response()
}

// -----------------------------------------------------------------------------
// CrowdStrike: OAuth2 token endpoint, an offset-paged id query with a total,
// and a POST lookup that returns the entities for a batch of ids. Each
// scripted row is an entity carrying `composite_id`.
// -----------------------------------------------------------------------------

async fn crowdstrike_token(
    State(state): State<Shared>,
    headers: HeaderMap,
    axum::Form(form): axum::Form<HashMap<String, String>>,
) -> Response {
    token_endpoint(&state, "/oauth2/token", &headers, form, "cs", 1799)
}

fn composite_id(row: &Value) -> String {
    row["composite_id"]
        .as_str()
        .expect("a scripted alert carries composite_id")
        .to_owned()
}

async fn crowdstrike_query(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let offset: usize = query
        .get("offset")
        .and_then(|o| o.parse().ok())
        .unwrap_or(0);
    let limit: usize = query
        .get("limit")
        .and_then(|l| l.parse().ok())
        .unwrap_or(100);
    record(&state, "/alerts/queries/alerts/v2", &query, &headers, None);
    let mut recorded = state.lock().unwrap();
    if let Some((status, retry_after)) = recorded.script.before.pop_front() {
        let status = StatusCode::from_u16(status).expect("scripted status");
        let body = axum::Json(json!({"errors": [{"message": "scripted refusal"}]}));
        return match retry_after {
            Some(secs) => (status, [(header::RETRY_AFTER, secs.to_string())], body).into_response(),
            None => (status, body).into_response(),
        };
    }
    let ids: Vec<String> = recorded
        .script
        .pages
        .iter()
        .flatten()
        .map(composite_id)
        .collect();
    let slice: Vec<&String> = ids.iter().skip(offset).take(limit).collect();
    axum::Json(json!({
        "meta": {"pagination": {"offset": offset, "limit": limit, "total": ids.len()}},
        "resources": slice,
        "errors": []
    }))
    .into_response()
}

async fn crowdstrike_entities(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    record(
        &state,
        "/alerts/entities/alerts/v2",
        &query,
        &headers,
        Some(body.clone()),
    );
    let wanted: Vec<&str> = body["composite_ids"]
        .as_array()
        .map(|ids| ids.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let recorded = state.lock().unwrap();
    let entities: Vec<Value> = recorded
        .script
        .pages
        .iter()
        .flatten()
        .filter(|row| wanted.contains(&composite_id(row).as_str()))
        .cloned()
        .collect();
    axum::Json(json!({"meta": {}, "resources": entities, "errors": []})).into_response()
}

// -----------------------------------------------------------------------------
// Duo: every request is signed over the canonical string; the provider
// recomputes the signature with the known secret key and refuses a mismatch the
// way Duo does (401, `stat: FAIL`). Version 5 and the legacy version 2 are both
// verified, told apart by the length of the signature -- the digest's -- which
// is what a server accepting either has to do. The v2 log answers its
// `next_offset` as the two-element array the API documents.
// -----------------------------------------------------------------------------

/// Which version of Duo's signing a request carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuoSigning {
    /// HMAC-SHA512 over seven lines.
    V5,
    /// HMAC-SHA1 over five.
    V2,
}

impl DuoSigning {
    /// The version whose digest is this many hex characters long.
    fn of_signature(signature: &str) -> Option<Self> {
        match signature.len() {
            128 => Some(DuoSigning::V5),
            40 => Some(DuoSigning::V2),
            _ => None,
        }
    }
}

/// RFC 3986 percent-encoding with only the unreserved set left bare, the
/// encoding Duo's signing spec names.
fn duo_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The canonical string Duo signs, from what the request carried: the Date
/// header, the method, the lowercase Host header, the path, and the query
/// pairs sorted by name and re-encoded. Version 5 adds the hash of the body --
/// empty on every route here, all of them GET -- and the hash of the signed
/// `x-duo-` headers, of which the fetcher sends none.
fn duo_canonical(
    version: DuoSigning,
    method: &str,
    headers: &HeaderMap,
    path: &str,
    query: &HashMap<String, String>,
) -> String {
    let date = headers
        .get("date")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    let mut pairs: Vec<(&String, &String)> = query.iter().collect();
    pairs.sort();
    let query: Vec<String> = pairs
        .iter()
        .map(|(k, v)| format!("{}={}", duo_encode(k), duo_encode(v)))
        .collect();
    let five = format!("{date}\n{method}\n{host}\n{path}\n{}", query.join("&"));
    match version {
        DuoSigning::V2 => five,
        DuoSigning::V5 => {
            let empty = hex::encode(Sha512::digest(b""));
            format!("{five}\n{empty}\n{empty}")
        }
    }
}

/// The `hex(hmac(skey, canonical))` a correctly signed request carries in its
/// Basic credential.
pub fn duo_signature(version: DuoSigning, canonical: &str) -> String {
    match version {
        DuoSigning::V5 => {
            let mut mac = Hmac::<Sha512>::new_from_slice(DUO_SKEY.as_bytes())
                .expect("hmac accepts any key length");
            mac.update(canonical.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        }
        DuoSigning::V2 => {
            let mut mac = Hmac::<Sha1>::new_from_slice(DUO_SKEY.as_bytes())
                .expect("hmac accepts any key length");
            mac.update(canonical.as_bytes());
            hex::encode(mac.finalize().into_bytes())
        }
    }
}

/// Check a Duo request's signature; `Err` is the 401 Duo answers.
fn duo_verify(
    headers: &HeaderMap,
    method: &str,
    path: &str,
    query: &HashMap<String, String>,
) -> Result<(), Response> {
    let refused = |message: &str| {
        Err((
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"stat": "FAIL", "code": 40101, "message": message})),
        )
            .into_response())
    };
    let Some(basic) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
    else {
        return refused("Missing request credentials");
    };
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(basic)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .unwrap_or_default();
    let Some((ikey, sig)) = decoded.split_once(':') else {
        return refused("Invalid request credentials");
    };
    if ikey != DUO_IKEY {
        return refused("Invalid integration key in request credentials");
    }
    let Some(version) = DuoSigning::of_signature(sig) else {
        return refused("Invalid signature in request credentials");
    };
    let expected = duo_signature(
        version,
        &duo_canonical(version, method, headers, path, query),
    );
    if sig != expected {
        return refused("Invalid signature in request credentials");
    }
    Ok(())
}

async fn duo_authentication(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let path = "/admin/v2/logs/authentication";
    // The request's `next_offset` is `<ts>,<cursor>`; the cursor names the page.
    let index = indexed(
        query.get("next_offset").and_then(|o| o.split(',').nth(1)),
        "cursor-",
    );
    match page(&state, path, &query, &headers, None, index) {
        Err(response) => response,
        Ok(out) => {
            if let Err(refusal) = duo_verify(&headers, "GET", path, &query) {
                return refusal;
            }
            let next_offset = if out.has_next {
                json!(["1532951895000", format!("cursor-{}", index + 1)])
            } else {
                Value::Null
            };
            axum::Json(json!({
                "stat": "OK",
                "response": {
                    "authlogs": out.rows,
                    "metadata": {"next_offset": next_offset, "total_objects": out.rows.len()}
                }
            }))
            .into_response()
        }
    }
}

async fn duo_check(
    State(state): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let path = "/admin/v1/check";
    record(&state, path, &query, &headers, None);
    if let Err(refusal) = duo_verify(&headers, "GET", path, &query) {
        return refusal;
    }
    axum::Json(json!({"stat": "OK", "response": "valid"})).into_response()
}

/// Start the provider on an ephemeral port; it stops when the test's runtime
/// drops.
pub async fn start() -> Provider {
    let recorded: Shared = Arc::new(Mutex::new(Recorded::default()));
    let app = Router::new()
        .route("/orgs/{org}/audit-log", get(github_org))
        .route("/enterprises/{ent}/audit-log", get(github_enterprise))
        .route("/user", get(github_user))
        .route("/api/v1/logs", get(okta_logs))
        .route("/api/v1/users/me", get(okta_me))
        .route("/audit/v1/logs", get(slack_logs))
        .route("/api/auth.test", get(slack_auth_test))
        .route("/accounts/{account}/audit_logs", get(cloudflare_audit_logs))
        .route("/user/tokens/verify", get(cloudflare_verify))
        .route("/connect/token", post(bitwarden_token))
        .route("/public/events", get(bitwarden_events))
        .route("/api/v2/{kind}", post(onepassword_events))
        .route("/api/auth/introspect", get(onepassword_introspect))
        .route("/pypi/{package}/json", get(pypi_package))
        .route("/", get(pypi_root))
        .route("/api/v1/crates/{name}", get(crates_io_crate))
        .route("/api/v1/summary", get(crates_io_summary))
        .route("/oauth2/token", post(crowdstrike_token))
        .route("/alerts/queries/alerts/v2", get(crowdstrike_query))
        .route("/alerts/entities/alerts/v2", post(crowdstrike_entities))
        .route("/admin/v2/logs/authentication", get(duo_authentication))
        .route("/admin/v1/check", get(duo_check))
        .with_state(Arc::clone(&recorded));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Provider { addr, recorded }
}
