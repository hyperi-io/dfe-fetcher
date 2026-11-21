// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_s4hana.rs
// Purpose:   The shipped s4hana profile against a recorded OData V2 response: window filter, paging, auth, refusals
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The shipped `s4hana` profile bound to an instance and driven against an
//! in-test server that replays the synthetic OData V2 pages under
//! `tests/fixtures/`. The pages are shaped as the OData V2 JSON format defines
//! them (`d.results` rows, `d.__next` a URI to the next partial set) and carry
//! no customer data. The tests assert on the requests the server saw as well
//! as on the rows, so a filter that drifts or a next link that is rebuilt
//! instead of followed fails here.

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine as _;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde_json::Value;

use dfe_fetcher_core::error::Error;
use dfe_fetcher_core::{FetchWindow, RowSource, TickCtx};
use dfe_fetcher_rest::RestShape;
use dfe_fetcher_rest::profile::RestInstance;

const PAGE_ONE: &str = include_str!("../fixtures/s4hana_odata_v2_page1.json");
const PAGE_TWO: &str = include_str!("../fixtures/s4hana_odata_v2_page2.json");
const ENTITY_PATH: &str = "/sap/opu/odata/sap/API_SYNTHETIC_SRV/ChangeLogEntries";
const TOKEN_PATH: &str = "/sap/bc/sec/oauth2/token";
const PASSWORD: &str = "synth-pass";

/// One request the server saw.
#[derive(Debug, Clone)]
struct Seen {
    path: String,
    /// Decoded query pairs in request order.
    query: Vec<(String, String)>,
    authorization: Option<String>,
    sap_client: Option<String>,
    body: String,
}

impl Seen {
    fn query_value(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Clone)]
struct Server {
    /// What `{{host}}` in a fixture page becomes: the `__next` host.
    next_host: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

fn record(server: &Server, path: &str, raw_query: Option<String>, headers: &HeaderMap, body: &str) {
    let query: Vec<(String, String)> = raw_query
        .map(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect()
        })
        .unwrap_or_default();
    server.seen.lock().unwrap().push(Seen {
        path: path.to_owned(),
        query,
        authorization: headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        sap_client: headers
            .get("sap-client")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        body: body.to_owned(),
    });
}

/// The entity set: page one, or page two once the request carries the
/// `$skiptoken` page one's `__next` named. A wrong Basic password is the
/// OData error body SAP's own V2 services answer with.
async fn entity_set(
    State(server): State<Server>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    record(&server, ENTITY_PATH, query.clone(), &headers, "");
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let basic = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("SYNTH_USER:{PASSWORD}"))
    );
    if authorization.starts_with("Basic ") && authorization != basic {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::CONTENT_TYPE, "application/json")],
            r#"{"error":{"code":"S/001","message":{"lang":"en","value":"Logon failed"}}}"#,
        )
            .into_response();
    }
    let has_skiptoken = query
        .as_deref()
        .is_some_and(|q| url::form_urlencoded::parse(q.as_bytes()).any(|(k, _)| k == "$skiptoken"));
    let page = if has_skiptoken { PAGE_TWO } else { PAGE_ONE };
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        page.replace("{{host}}", &server.next_host),
    )
        .into_response()
}

async fn token(State(server): State<Server>, headers: HeaderMap, body: Bytes) -> Response {
    record(
        &server,
        TOKEN_PATH,
        None,
        &headers,
        &String::from_utf8_lossy(&body),
    );
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        r#"{"access_token":"tok-1","token_type":"Bearer","expires_in":3600}"#,
    )
        .into_response()
}

/// Start the server. `next_host` replaces `{{host}}` in the fixture pages:
/// `None` means this server's own origin.
async fn start(next_host: Option<&str>) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::new(Mutex::new(Vec::new()));
    let server = Server {
        next_host: next_host.unwrap_or(&base_url).to_owned(),
        seen: Arc::clone(&seen),
    };
    let app = Router::new()
        .route(ENTITY_PATH, get(entity_set))
        .route(TOKEN_PATH, post(token))
        .with_state(server);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (base_url, seen)
}

fn at(rfc3339: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(rfc3339)
        .expect("rfc3339")
        .with_timezone(&Utc)
}

/// One tick of the `entity_set` unit of an instance of the shipped profile.
/// `instance_yaml` is everything under the instance's keys after `profile`;
/// the window starts mid-second to show the bounds are cut to the second.
async fn tick(base_url: &str, instance_yaml: &str) -> Result<Vec<Value>, Error> {
    tick_with_vars(base_url, "key_field: EntryId, ", instance_yaml).await
}

/// `tick` with the `key_field` entry of the instance's vars given by the caller.
async fn tick_with_vars(
    base_url: &str,
    key_field: &str,
    instance_yaml: &str,
) -> Result<Vec<Value>, Error> {
    let yaml = format!(
        "profile: s4hana\ntopic: s4\nvars: {{ base_url: \"{base_url}\", \
         service: API_SYNTHETIC_SRV, entity_set: ChangeLogEntries, time_field: ChangedAt, \
         {key_field}sap_client: \"100\" }}\n{instance_yaml}"
    );
    let shape = bind_shape(&yaml)?;
    run_unit(&shape).await
}

/// The shipped profile bound to the instance `yaml` describes.
fn bind_shape(yaml: &str) -> Result<RestShape, Error> {
    let instance: RestInstance =
        scalo::yaml::from_str(yaml, "s4hana.yaml", scalo::yaml::Profile::Config)
            .unwrap_or_else(|e| panic!("instance does not load: {e}\n{yaml}"));
    let profile = dfe_fetcher_rest::profile::bound::resolve_profile(
        &instance,
        dfe_fetcher::profiles::shipped(),
    )
    .expect("s4hana is shipped");
    let shape = RestShape::from_instance(
        &profile,
        &instance,
        "s4",
        dfe_fetcher_rest::http_client().expect("http client"),
        &dfe_fetcher_rest::exchange_client().expect("exchange client"),
    )?;
    Ok(shape)
}

/// One tick of the `entity_set` unit over a fixed window that starts mid-second.
async fn run_unit(shape: &RestShape) -> Result<Vec<Value>, Error> {
    let spec = shape
        .units()
        .iter()
        .find(|u| &*u.name == "entity_set")
        .expect("the entity_set unit is bound")
        .clone();
    let window = FetchWindow {
        start: at("2026-11-20T01:00:00.987Z"),
        end: at("2026-11-20T02:00:00Z"),
    };
    let ctx = TickCtx {
        window: Some(&window),
        connection_id: "s4",
        unit: &spec,
        checkpoint: None,
    };
    let mut rows = shape.rows(ctx);
    let mut out: Vec<Value> = Vec::new();
    while let Some(row) = rows.next().await {
        out.push(serde_json::from_slice(&row?.payload).expect("row is JSON"));
    }
    Ok(out)
}

fn entity_requests(seen: &Arc<Mutex<Vec<Seen>>>) -> Vec<Seen> {
    seen.lock()
        .unwrap()
        .iter()
        .filter(|s| s.path == ENTITY_PATH)
        .cloned()
        .collect()
}

const BASIC: &str = "auth: { mode: basic, username: SYNTH_USER, password: synth-pass }";

#[tokio::test]
async fn both_pages_are_read_once_each_in_order_and_the_next_link_is_followed_verbatim() {
    let (base_url, seen) = start(None).await;

    let rows = tick(&base_url, BASIC).await.expect("tick");

    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r["EntryId"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        ["0000000001", "0000000002", "0000000003", "0000000004"]
    );
    assert_eq!(
        rows[0]["ChangedAt"], "/Date(1795136711000)/",
        "the V2 date literal is passed through, not reinterpreted"
    );

    let requests = entity_requests(&seen);
    assert_eq!(
        requests.len(),
        2,
        "one request per page, none after the last"
    );
    let filter = "ChangedAt ge datetimeoffset'2026-11-20T01:00:00Z' and ChangedAt lt datetimeoffset'2026-11-20T02:00:00Z'";
    let first = &requests[0];
    assert_eq!(
        first.query_value("$filter"),
        Some(filter),
        "bounds cut to the second"
    );
    assert_eq!(
        first.query_value("$orderby"),
        Some("ChangedAt asc,EntryId asc")
    );
    assert_eq!(first.query_value("$format"), Some("json"));
    assert_eq!(first.sap_client.as_deref(), Some("100"));
    assert_eq!(first.query_value("$skiptoken"), None);
    assert_eq!(
        first.authorization.as_deref(),
        Some("Basic U1lOVEhfVVNFUjpzeW50aC1wYXNz"),
        "SYNTH_USER:synth-pass"
    );

    let second = &requests[1];
    assert_eq!(second.query_value("$skiptoken"), Some("2"));
    assert_eq!(
        second.query_value("$filter"),
        Some(
            "ChangedAt ge datetimeoffset'2026-11-20T01:00:00Z' and ChangedAt lt datetimeoffset'2026-11-20T02:00:00Z'"
        ),
        "the link's own filter, kept as the fixture wrote it"
    );
    assert_eq!(
        second.query_value("sap-client"),
        None,
        "the link is followed as given, not rebuilt from the unit's query"
    );
    assert_eq!(
        second.sap_client.as_deref(),
        Some("100"),
        "a link without `sap-client` still reads the selected client: it is a header"
    );
    assert_eq!(second.authorization, first.authorization);
}

#[tokio::test]
async fn a_datetime_property_is_filtered_with_a_zoneless_literal() {
    let (base_url, seen) = start(None).await;

    tick(
        &base_url,
        &format!("{BASIC}\nunits:\n  entity_set: {{ vars: {{ time_type: datetime }} }}"),
    )
    .await
    .expect("tick");

    assert_eq!(
        entity_requests(&seen)[0].query_value("$filter"),
        Some(
            "ChangedAt ge datetime'2026-11-20T01:00:00' and ChangedAt lt datetime'2026-11-20T02:00:00'"
        )
    );
}

#[tokio::test]
async fn an_extra_filter_is_joined_with_and_and_an_empty_sap_client_is_left_out() {
    let (base_url, seen) = start(None).await;

    tick(
        &base_url,
        &format!(
            "{BASIC}\nunits:\n  entity_set: {{ vars: {{ extra_filter: \"ObjectClass eq 'SYNTH_ORDER'\", sap_client: \"\" }} }}"
        ),
    )
    .await
    .expect("tick");

    let first = &entity_requests(&seen)[0];
    assert!(
        first
            .query_value("$filter")
            .unwrap()
            .ends_with("2026-11-20T02:00:00Z' and (ObjectClass eq 'SYNTH_ORDER')"),
        "{:?}",
        first.query_value("$filter")
    );
    assert_eq!(first.sap_client, None);
}

#[tokio::test]
async fn an_instance_without_a_key_field_does_not_bind() {
    let (base_url, seen) = start(None).await;

    let err = tick_with_vars(&base_url, "", BASIC).await.unwrap_err();

    assert!(
        err.to_string().contains("key_field"),
        "names the var: {err}"
    );
    assert!(
        entity_requests(&seen).is_empty(),
        "no request without a tie-break key"
    );
}

#[tokio::test]
async fn client_credentials_mint_one_token_at_the_hosts_token_endpoint_and_both_pages_carry_it() {
    let (base_url, seen) = start(None).await;

    let rows = tick(
        &base_url,
        "auth: { mode: oauth2_client_credentials, client_id: synth-client, client_secret: synth-secret }",
    )
    .await
    .expect("tick");

    assert_eq!(rows.len(), 4);
    let all = seen.lock().unwrap().clone();
    let exchanges: Vec<&Seen> = all.iter().filter(|s| s.path == TOKEN_PATH).collect();
    assert_eq!(exchanges.len(), 1, "one exchange for the tick");
    for field in [
        "grant_type=client_credentials",
        "client_id=synth-client",
        "client_secret=synth-secret",
    ] {
        assert!(
            exchanges[0].body.contains(field),
            "{field} in {}",
            exchanges[0].body
        );
    }
    for request in all.iter().filter(|s| s.path == ENTITY_PATH) {
        assert_eq!(request.authorization.as_deref(), Some("Bearer tok-1"));
    }
}

#[tokio::test]
async fn a_next_link_naming_another_host_fails_the_tick_before_any_credential_is_sent_there() {
    let (base_url, seen) = start(Some("https://internal.s4.invalid:44300")).await;

    let err = tick(&base_url, BASIC).await.unwrap_err();

    assert!(
        err.to_string().contains("internal.s4.invalid"),
        "names the host: {err}"
    );
    assert_eq!(
        entity_requests(&seen).len(),
        1,
        "page one was read, page two never requested: the tick fails whole"
    );
}

#[tokio::test]
async fn a_logon_failure_surfaces_the_odata_error_text_and_is_not_retried() {
    let (base_url, seen) = start(None).await;

    let err = tick(
        &base_url,
        "auth: { mode: basic, username: SYNTH_USER, password: wrong }",
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("Logon failed"), "{err}");
    assert_eq!(entity_requests(&seen).len(), 1, "a 401 is never retried");
}

#[tokio::test]
async fn the_shape_reports_the_profile_overlap_and_an_instances_own() {
    let vars = "vars: { base_url: \"http://127.0.0.1:1\", service: S, entity_set: E, \
                time_field: T, key_field: K }\n";
    let base = format!("profile: s4hana\ntopic: s4\n{BASIC}\n{vars}");

    let shipped = bind_shape(&base).unwrap();
    assert_eq!(
        shipped.window_overlap(),
        std::time::Duration::from_secs(300)
    );
    assert_eq!(shipped.window_delay(), std::time::Duration::from_secs(300));

    let own = bind_shape(&format!("{base}window: {{ delay: 20m, overlap: 1h }}\n")).unwrap();
    assert_eq!(own.window_overlap(), std::time::Duration::from_secs(3600));
    assert_eq!(own.window_delay(), std::time::Duration::from_mins(20));
}
