// Project:   dfe-fetcher
// File:      crates/rest/tests/fixture.rs
// Purpose:   The REST shape end to end over a real HTTP provider: every pager, decoder and auth mode
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The REST shape against the in-test provider in `common`.
//!
//! Each test writes a profile in the grammar, binds it to an instance whose
//! `base_url` var points at the fixture, and drives `RowSource::rows` for one
//! unit, then asserts on the rows AND on the requests the fixture recorded.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use chrono::{TimeZone, Utc};
use futures::StreamExt;
use serde_json::Value;

use dfe_fetcher_core::error::Error;
use dfe_fetcher_core::{FetchWindow, Mark, Row, RowSource, TickCtx};
use dfe_fetcher_rest::profile::{RestInstance, RestProfile};
use dfe_fetcher_rest::shape::RestShape;

fn profile(yaml: &str) -> RestProfile {
    serde_yaml_ng::from_str(yaml).unwrap_or_else(|e| panic!("profile does not load: {e}\n{yaml}"))
}

fn instance(fixture: &common::Fixture, yaml: &str) -> RestInstance {
    let mut inst: RestInstance = serde_yaml_ng::from_str(yaml).unwrap();
    inst.vars
        .insert("base_url".into(), Value::String(fixture.base_url()));
    inst
}

/// The client a credential exchange posts through, as the app builds it.
fn exchange() -> std::sync::Arc<dfe_fetcher_rest::ExchangeClient> {
    dfe_fetcher_rest::exchange_client().unwrap()
}

fn shape(fixture: &common::Fixture, profile_yaml: &str, instance_yaml: &str) -> RestShape {
    RestShape::from_instance(
        &profile(profile_yaml),
        &instance(fixture, instance_yaml),
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .unwrap_or_else(|e| panic!("bind failed: {e}"))
}

/// Every row of one unit's tick, marks included.
async fn fetch_rows(
    shape: &RestShape,
    unit: &str,
    window: Option<&FetchWindow>,
) -> Result<Vec<Row>, Error> {
    let spec = shape
        .units()
        .iter()
        .find(|u| &*u.name == unit)
        .unwrap_or_else(|| panic!("no unit {unit}"))
        .clone();
    let tick = TickCtx {
        window,
        connection_id: "conn",
        unit: &spec,
        checkpoint: None,
    };
    let mut rows = shape.rows(tick);
    let mut out = Vec::new();
    while let Some(row) = rows.next().await {
        out.push(row?);
    }
    Ok(out)
}

async fn fetch(
    shape: &RestShape,
    unit: &str,
    window: Option<&FetchWindow>,
) -> Result<Vec<Value>, Error> {
    Ok(fetch_rows(shape, unit, window)
        .await?
        .iter()
        .map(|row| serde_json::from_slice(&row.payload).expect("row is JSON"))
        .collect())
}

const PLAIN: &str =
    "profile: plain\nbase_url: \"{{ vars.base_url }}\"\nauth: { accepts: [none] }\n";
const BEARER_INSTANCE: &str = "profile: x\ntopic: t\nauth: { mode: none }\n";

/// A shape over a client with the given connect and read timeouts.
fn shape_with_client(
    fixture: &common::Fixture,
    profile_yaml: &str,
    instance_yaml: &str,
    client: reqwest::Client,
) -> RestShape {
    RestShape::from_instance(
        &profile(profile_yaml),
        &instance(fixture, instance_yaml),
        "conn",
        client,
        &exchange(),
    )
    .unwrap_or_else(|e| panic!("bind failed: {e}"))
}

/// A streamed body takes as long as it takes: the client bounds each read,
/// not the whole request, so a dump that trickles past what a total timeout
/// would allow still completes, while a body that stalls for longer than one
/// read may wait fails as a timeout.
#[tokio::test]
async fn a_streamed_body_is_bounded_per_read_not_in_total() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}endpoints:\n  - unit: slow\n    path: /trickle/6\n    query: {{ every_ms: 400 }}\n    rows: {{ decoder: ndjson }}\n  - unit: stalled\n    path: /stall\n    query: {{ hold_ms: 2500 }}\n    rows: {{ decoder: ndjson }}\n"
    );
    let client = dfe_fetcher_rest::request::http_client_with(
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
    )
    .unwrap();
    let s = shape_with_client(&fx, &p, BEARER_INSTANCE, client);
    let started = std::time::Instant::now();
    let rows = fetch(&s, "slow", None)
        .await
        .expect("six rows over ~2.4 s, each read under the 1 s read timeout");
    assert_eq!(rows.len(), 6);
    assert!(
        started.elapsed() >= std::time::Duration::from_secs(2),
        "the body really did outlive a 1 s total bound"
    );
    let err = fetch(&s, "stalled", None)
        .await
        .expect_err("a 2.5 s stall exceeds the 1 s read timeout");
    assert_eq!(err.api_error_code(), "timeout", "{err}");
}

/// `timeout_secs` puts a total bound on a request whose rows are read whole,
/// and is refused on a unit whose decoder streams.
#[tokio::test]
async fn timeout_secs_bounds_a_page_bounded_request_and_is_refused_for_a_streaming_one() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}endpoints:\n  - unit: whole\n    path: /trickle/6\n    query: {{ every_ms: 400 }}\n    rows: {{ decoder: json }}\n    timeout_secs: 1\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    let err = fetch(&s, "whole", None)
        .await
        .expect_err("2.4 s of body under a 1 s total bound");
    assert_eq!(err.api_error_code(), "timeout", "{err}");

    let streaming = format!(
        "{PLAIN}endpoints:\n  - unit: lines\n    path: /trickle/6\n    rows: {{ decoder: ndjson }}\n    timeout_secs: 1\n"
    );
    let issues = profile(&streaming).validate();
    assert!(
        issues
            .iter()
            .any(|i| i.field.ends_with("timeout_secs") && i.message.contains("streaming")),
        "{issues:?}"
    );
    let zero = format!(
        "{PLAIN}endpoints:\n  - unit: whole\n    path: /trickle/6\n    rows: {{ decoder: json }}\n    timeout_secs: 0\n"
    );
    assert!(
        profile(&zero)
            .validate()
            .iter()
            .any(|i| i.field.ends_with("timeout_secs")),
        "zero is refused"
    );
    let item = format!(
        "{PLAIN}endpoints:\n  - unit: m\n    path: /manifest/list\n    rows: {{ decoder: json_array }}\n    construct:\n      manifest:\n        item_request: {{ path: \"/manifest/blob/{{{{ item.id }}}}\", timeout_secs: 5 }}\n        rows: {{ decoder: ndjson }}\n"
    );
    assert!(
        profile(&item)
            .validate()
            .iter()
            .any(|i| i.field.ends_with("item_request.timeout_secs")),
        "a manifest item read as a stream refuses the bound too"
    );
}

/// A redirect is followed only within the request's own origin: a hop to
/// another host is answered as the 3xx it is and the credential header never
/// reaches that host, while a same-origin hop is followed.
#[tokio::test]
async fn a_redirect_off_the_origin_is_not_followed_and_the_key_stays_home() {
    let fx = common::start().await;
    let elsewhere = common::start().await;
    let p = format!(
        "profile: keyed\nbase_url: \"{{{{ vars.base_url }}}}\"\nauth:\n  accepts: [api_key]\n  api_key: {{ header: X-Api-Key }}\nendpoints:\n  - unit: away\n    path: /redirect/cross\n    query: {{ to: \"{}/array/items.json\" }}\n    rows: {{ decoder: json_array }}\n  - unit: home\n    path: /redirect/same\n    rows: {{ decoder: json_array }}\n",
        elsewhere.base_url()
    );
    let s = shape_with_client(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: api_key, key: the-key }\n",
        dfe_fetcher_rest::request::http_client().unwrap(),
    );
    let err = fetch(&s, "away", None)
        .await
        .expect_err("a cross-host redirect is not followed");
    assert!(
        matches!(err, Error::Api { status: 302, .. }),
        "the 3xx surfaces as the API answer it is: {err}"
    );
    assert!(
        elsewhere.paths().is_empty(),
        "the other host saw no request at all: {:?}",
        elsewhere.paths()
    );
    let rows = fetch(&s, "home", None)
        .await
        .expect("a same-origin hop is followed");
    assert_eq!(rows.len(), 4);
    assert_eq!(fx.requests_to("/array/items.json").len(), 1);
}

/// A transport failure must not carry the credential. reqwest's `Display`
/// appends the request URL, which holds the key when `auth.api_key.query`
/// puts it there, so every error site strips it with `without_url()`. Nothing
/// but this test stops a later change putting the URL back.
#[tokio::test]
async fn a_transport_failure_never_carries_the_query_key() {
    // A port that was free and is now closed, so the connection is refused
    // rather than answered: the only path that reaches the transport-error
    // arm, since a status code takes the other one.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = listener.local_addr().unwrap();
    drop(listener);

    let p = "profile: keyed\nbase_url: \"{{ vars.base_url }}\"\nauth:\n  accepts: [api_key]\n  api_key: { query: api_key }\nretry: { min_backoff_ms: 1, max_backoff_ms: 5 }\nendpoints:\n  - { unit: items, path: /array/items.json, rows: { decoder: json_array } }\n";
    let mut inst: RestInstance = serde_yaml_ng::from_str(
        "profile: x\ntopic: t\nauth: { mode: api_key, key: super-secret-key }\n",
    )
    .unwrap();
    inst.vars
        .insert("base_url".into(), Value::String(format!("http://{dead}")));
    let s = RestShape::from_instance(
        &profile(p),
        &inst,
        "conn",
        dfe_fetcher_rest::request::http_client().unwrap(),
        &exchange(),
    )
    .expect("bind");

    let text = fetch(&s, "items", None)
        .await
        .expect_err("nothing is listening on a closed port")
        .to_string();
    assert!(
        !text.contains("super-secret-key"),
        "the credential must never reach the error text: {text}"
    );
    assert!(
        !text.contains("api_key="),
        "nor the query parameter carrying it: {text}"
    );
    assert!(
        !text.contains(&dead.to_string()),
        "the URL is stripped, so the address does not appear either: {text}"
    );
}

/// The page ceiling on a unit that reads the window is a failed tick, so the
/// scheduler does not advance the window past the pages never fetched; on a
/// dump, which has no window to lose, the sequence is cut short.
#[tokio::test]
async fn the_page_ceiling_fails_a_windowed_unit_and_cuts_a_dump_short() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}window: {{ format: epoch_secs }}\nendpoints:\n  - unit: events\n    path: /link/page\n    query: {{ page: 1, since: \"{{{{ window.start }}}}\" }}\n    rows: {{ decoder: json_array }}\n    paginate: {{ strategy: link_header }}\n    max_pages: 2\n  - unit: store\n    shape: dump\n    path: /link/page\n    query: {{ page: 1 }}\n    rows: {{ decoder: json_array }}\n    paginate: {{ strategy: link_header }}\n    max_pages: 2\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    let window = FetchWindow {
        start: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        end: Utc.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap(),
    };
    let err = fetch(&s, "events", Some(&window))
        .await
        .expect_err("three pages under a ceiling of two");
    assert!(
        matches!(err, Error::PageCeiling { ref unit, max_pages: 2 } if unit == "events"),
        "{err}"
    );
    assert_eq!(err.api_error_code(), "page_ceiling");
    assert_eq!(
        fx.requests_to("/link/page").len(),
        2,
        "the third page is never asked for"
    );

    let rows = fetch(&s, "store", None)
        .await
        .expect("a dump is cut at the ceiling, not failed");
    assert_eq!(rows.len(), 4, "two pages of two");
}

/// The page sequence is LAZY: page N+1 is requested only once the driver has
/// polled past the last row of page N. That is what "stop asking" means for a
/// paginated provider under memory pressure -- the gate stops polling, and the
/// provider stops being asked -- and it is the half of the backpressure design
/// that lives in the shape rather than the driver.
#[tokio::test]
async fn the_next_page_is_not_requested_until_the_rows_of_this_one_are_taken() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}endpoints:\n  - unit: pages\n    path: /link/page\n    query: {{ page: 1 }}\n    rows: {{ decoder: json_array }}\n    paginate: {{ strategy: link_header }}\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    let spec = s
        .units()
        .iter()
        .find(|u| &*u.name == "pages")
        .expect("the unit")
        .clone();
    let tick = TickCtx {
        window: None,
        connection_id: "conn",
        unit: &spec,
        checkpoint: None,
    };
    let mut rows = s.rows(tick);

    let first: Value =
        serde_json::from_slice(&rows.next().await.expect("a row").unwrap().payload).unwrap();
    assert_eq!(first["id"], 10, "the first row of page one");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        fx.requests_to("/link/page").len(),
        1,
        "page two is not fetched while a row of page one is still unread"
    );

    // Draining page one is what asks for page two.
    let second: Value =
        serde_json::from_slice(&rows.next().await.expect("a row").unwrap().payload).unwrap();
    assert_eq!(second["id"], 11, "the last row of page one");
    assert_eq!(
        fx.requests_to("/link/page").len(),
        1,
        "still one page: the fetch happens on the poll PAST the last row"
    );
    let third: Value =
        serde_json::from_slice(&rows.next().await.expect("a row").unwrap().payload).unwrap();
    assert_eq!(third["id"], 20, "the first row of page two");
    assert_eq!(fx.requests_to("/link/page").len(), 2);
}

#[tokio::test]
async fn link_header_paging_follows_the_providers_absolute_urls() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}endpoints:\n  - unit: pages\n    path: /link/page\n    query: {{ page: 1 }}\n    rows: {{ decoder: json_array }}\n    paginate: {{ strategy: link_header }}\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    let rows = fetch(&s, "pages", None).await.unwrap();
    let ids: Vec<u64> = rows.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    assert_eq!(ids, [10, 11, 20, 21, 30, 31]);
    let seen = fx.requests_to("/link/page");
    assert_eq!(seen.len(), 3);
    assert_eq!(
        seen[1].query,
        [("page".to_string(), "2".to_string())],
        "the Link URL was used as-is"
    );
}

#[tokio::test]
async fn cursor_paging_stops_on_the_empty_next_key_of_a_full_last_page() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}endpoints:\n  - unit: assets\n    path: /cursor/assets.json\n    query: {{ page_size: 5 }}\n    rows: {{ decoder: json_at, at: /assets }}\n    paginate: {{ strategy: cursor, from: \"body:/next_key\", into: \"query:start_key\", stop_when: \"body.next_key == ''\" }}\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    let rows = fetch(&s, "assets", None).await.unwrap();
    assert_eq!(
        rows.len(),
        15,
        "three full pages, the last terminated by next_key \"\""
    );
    let seen = fx.requests_to("/cursor/assets.json");
    assert_eq!(
        seen.len(),
        3,
        "a full last page with an empty key is not fetched again"
    );
    assert_eq!(
        seen[1].query,
        [
            ("page_size".to_string(), "5".to_string()),
            ("start_key".to_string(), "key-5".to_string())
        ]
    );
    assert_eq!(
        seen[2].query[1],
        ("start_key".to_string(), "key-10".to_string())
    );
}

#[tokio::test]
async fn page_number_and_offset_paging_honour_the_totals_in_the_body() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}endpoints:\n  - unit: numbered\n    path: /number/items\n    rows: {{ decoder: json_at, at: /result }}\n    paginate: {{ strategy: page_number, param: page, start: 1, total_pages_at: /result_info/total_pages }}\n  - unit: offsets\n    path: /offset/items\n    rows: {{ decoder: json_at, at: /resources }}\n    paginate: {{ strategy: offset, param: offset, page_size: 100, total_at: /meta/pagination/total }}\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    let rows = fetch(&s, "numbered", None).await.unwrap();
    assert_eq!(rows.len(), 9);
    let pages: Vec<String> = fx
        .requests_to("/number/items")
        .iter()
        .map(|r| r.query[0].1.clone())
        .collect();
    assert_eq!(pages, ["1", "2", "3"]);
    let rows = fetch(&s, "offsets", None).await.unwrap();
    assert_eq!(rows.len(), 250);
    let offsets: Vec<String> = fx
        .requests_to("/offset/items")
        .iter()
        .map(|r| r.query[0].1.clone())
        .collect();
    assert_eq!(offsets, ["0", "100", "200"]);
}

#[tokio::test]
async fn a_window_step_splits_the_tick_into_chunked_requests() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}window: {{ format: epoch_secs, step: 1h }}\nendpoints:\n  - unit: events\n    path: /window/events\n    query: {{ start: \"{{{{ window.start }}}}\", end: \"{{{{ window.end }}}}\" }}\n    rows: {{ decoder: json_array }}\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    let window = FetchWindow {
        start: Utc.timestamp_opt(0, 0).single().unwrap(),
        end: Utc.timestamp_opt(9000, 0).single().unwrap(),
    };
    let rows = fetch(&s, "events", Some(&window)).await.unwrap();
    assert_eq!(rows.len(), 3, "one row per step");
    let bounds: Vec<(String, String)> = fx
        .requests_to("/window/events")
        .iter()
        .map(|r| (r.query[1].1.clone(), r.query[0].1.clone()))
        .collect();
    assert_eq!(
        bounds,
        [
            ("0".to_string(), "3600".to_string()),
            ("3600".to_string(), "7200".to_string()),
            ("7200".to_string(), "9000".to_string()),
        ]
    );
}

#[tokio::test]
async fn ndjson_edge_cases_over_http() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}defaults: {{ rows: {{ decoder: ndjson }} }}\nendpoints:\n  - {{ unit: trailing, path: /ndjson/trailing.jsonl }}\n  - {{ unit: notrailing, path: /ndjson/notrailing.jsonl }}\n  - {{ unit: empty, path: /ndjson/empty.jsonl }}\n  - {{ unit: single, path: /ndjson/single.jsonl }}\n  - {{ unit: raw_gzip, path: /ndjson/raw.jsonl.gz, rows: {{ decoder: ndjson, gzip: true }} }}\n  - {{ unit: encoded, path: /ndjson/encoded.jsonl }}\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    assert_eq!(fetch(&s, "trailing", None).await.unwrap().len(), 2);
    assert_eq!(fetch(&s, "notrailing", None).await.unwrap().len(), 2);
    assert!(
        fetch(&s, "empty", None).await.unwrap().is_empty(),
        "0 bytes is an empty store"
    );
    let single = fetch(&s, "single", None).await.unwrap();
    assert_eq!(single, [serde_json::json!({"only": true})]);
    assert_eq!(
        fetch(&s, "raw_gzip", None).await.unwrap().len(),
        2,
        "explicit gzip body"
    );
    assert_eq!(
        fetch(&s, "encoded", None).await.unwrap().len(),
        2,
        "Content-Encoding is transparent"
    );
}

#[tokio::test]
async fn a_json_array_root_streams_and_a_dump_unit_lands_on_its_own_topic() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}shape: dump\nendpoints:\n  - {{ unit: items, path: /array/items.json, rows: {{ decoder: json_array }} }}\n"
    );
    let s = shape(
        &fx,
        &p,
        "profile: x\ntopic: fixture\nauth: { mode: none }\n",
    );
    assert_eq!(fetch(&s, "items", None).await.unwrap().len(), 4);
    let unit = &s.units()[0];
    assert!(unit.is_dump());
    assert_eq!(&*unit.topic, "fixture-items");
}

/// The three placed-or-minted modes end to end, and the renewal point: a token
/// still inside its hold is reused, one past it is exchanged again, and a
/// refusal is not held so the next tick reads the endpoint's current answer.
///
/// The lifetime the endpoint advertises is the knob, so no leg of this waits on
/// a clock.
#[tokio::test]
async fn bearer_api_key_and_oauth2_modes_authenticate_and_a_token_is_reused_until_its_renewal_point()
 {
    let fx = common::start().await;
    let p = "profile: auth\nbase_url: \"{{ vars.base_url }}\"\nauth:\n  accepts: [bearer, api_key, oauth2_client_credentials]\n  api_key: { header: Authorization, prefix: \"SSWS \" }\n  oauth2_client_credentials: { token_url: \"{{ base_url }}/token\", early_refresh_secs: 1 }\nendpoints:\n  - { unit: bearer, path: /auth/bearer, rows: { decoder: json_array } }\n  - { unit: apikey, path: /auth/apikey, rows: { decoder: json_array } }\n  - { unit: oauth, path: /auth/oauth, rows: { decoder: json_array } }\n".to_string();
    let bearer = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: bearer, token: secret-token }\n",
    );
    assert_eq!(fetch(&bearer, "bearer", None).await.unwrap().len(), 1);
    let err = fetch(&bearer, "oauth", None).await.unwrap_err();
    assert!(matches!(err, Error::Api { status: 401, .. }), "{err:?}");

    let apikey = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: api_key, key: the-key }\n",
    );
    assert_eq!(fetch(&apikey, "apikey", None).await.unwrap().len(), 1);

    let oauth = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: oauth2_client_credentials, client_id: client-a, client_secret: secret-a }\n",
    );
    assert_eq!(fetch(&oauth, "oauth", None).await.unwrap().len(), 1);
    assert_eq!(fx.token_exchanges(), 1);
    assert_eq!(fetch(&oauth, "oauth", None).await.unwrap().len(), 1);
    assert_eq!(
        fx.token_exchanges(),
        1,
        "a token still inside its hold is reused"
    );

    // A token that advertises no lifetime at all is past its renewal point the
    // moment it is read, so every request of a mode holding one exchanges again.
    fx.set_token_ttl(0);
    let expiring = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: oauth2_client_credentials, client_id: client-a, client_secret: secret-a }\n",
    );
    assert_eq!(fetch(&expiring, "oauth", None).await.unwrap().len(), 1);
    assert_eq!(fetch(&expiring, "oauth", None).await.unwrap().len(), 1);
    assert_eq!(
        fx.token_exchanges(),
        3,
        "each request found the held token past its renewal point"
    );
    let seen = fx.requests_to("/auth/oauth");
    assert_eq!(
        seen.last().unwrap().authorization.as_deref(),
        Some("Bearer tok-3"),
        "the request after the renewal carried the fresh token, not the stale one"
    );
    fx.set_token_ttl(3600);

    let wrong = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: oauth2_client_credentials, client_id: client-a, client_secret: wrong }\n",
    );
    let before = fx.token_exchanges();
    for _ in 0..2 {
        let err = fetch(&wrong, "oauth", None).await.unwrap_err();
        assert!(
            matches!(err, Error::Api { status: 401, .. }),
            "token refusal is terminal: {err:?}"
        );
    }
    assert_eq!(
        fx.token_exchanges() - before,
        2,
        "a refusal is not held, so the next tick reads the endpoint's current answer"
    );
}

/// A provider that revokes a token before its advertised expiry answers 401,
/// and the mode has to drop what it holds: the cache would otherwise present the
/// revoked token until its renewal point -- at least half its lifetime -- so
/// every tick until then would fail.
#[tokio::test]
async fn a_refused_request_drops_the_token_so_the_next_tick_mints_again() {
    let fx = common::start().await;
    let p = "profile: revoked\nbase_url: \"{{ vars.base_url }}\"\nauth:\n  accepts: [oauth2_client_credentials]\n  oauth2_client_credentials: { token_url: \"{{ base_url }}/token\" }\nendpoints:\n  - { unit: data, path: /revoked/data, rows: { decoder: json_array } }\n";
    let s = shape(
        &fx,
        p,
        "profile: x\ntopic: t\nauth: { mode: oauth2_client_credentials, client_id: client-a, client_secret: secret-a }\n",
    );

    // The token lives an hour, so nothing here turns on expiry.
    let err = fetch(&s, "data", None).await.unwrap_err();
    assert!(
        matches!(err, Error::Api { status: 401, .. }),
        "the provider refused the token it had just been given: {err:?}"
    );
    assert_eq!(fx.token_exchanges(), 1);

    let rows = fetch(&s, "data", None)
        .await
        .expect("the second tick mints");
    assert_eq!(rows[0]["token"], "tok-2");
    assert_eq!(
        fx.token_exchanges(),
        2,
        "the refusal dropped the held token rather than holding it to its renewal point"
    );
    let seen = fx.requests_to("/revoked/data");
    assert_eq!(seen[0].authorization.as_deref(), Some("Bearer tok-1"));
    assert_eq!(seen[1].authorization.as_deref(), Some("Bearer tok-2"));
}

/// A credential is minted once per instance and per scope, so a template that
/// decides what it mints may not depend on which unit asks. Binding refuses the
/// profile rather than freezing whichever unit's answer reached the mode first,
/// which for a domain-wide-delegation `sub` would run every unit as another
/// unit's principal.
#[tokio::test]
async fn a_credential_template_that_would_differ_by_unit_is_refused_when_the_instance_binds() {
    let fx = common::start().await;

    // The token endpoint reads `base_url`, and one unit names its own.
    let p = "profile: hosts\nbase_url: \"{{ vars.base_url }}\"\nauth:\n  accepts: [oauth2_client_credentials]\n  oauth2_client_credentials: { token_url: \"{{ base_url }}/token\" }\nendpoints:\n  - { unit: here, path: /auth/oauth, rows: { decoder: json_array } }\n  - { unit: elsewhere, base_url: \"{{ vars.other_url }}\", path: /auth/oauth, rows: { decoder: json_array } }\n";
    let mut inst = instance(
        &fx,
        "profile: x\ntopic: t\nauth: { mode: oauth2_client_credentials, client_id: client-a, client_secret: secret-a }\n",
    );
    inst.vars.insert(
        "other_url".into(),
        Value::String("http://other.example".into()),
    );
    let err = RestShape::from_instance(
        &profile(p),
        &inst,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .expect_err("one mode, one token endpoint");
    assert!(
        err.to_string()
            .contains("auth.oauth2_client_credentials.token_url"),
        "{err}"
    );
    assert!(err.to_string().contains("elsewhere"), "{err}");
    assert_eq!(fx.token_exchanges(), 0, "nothing was minted at all");

    // The impersonation subject of a JWT-bearer assertion off a per-unit var:
    // the claim that decides WHO the token acts as.
    let (private_pem, public_pem) = common::rsa_key_pair();
    fx.accept_assertions_from(&public_pem);
    let key_json = common::service_account_key(&private_pem, &format!("{}/token", fx.base_url()));
    let dwd = "profile: dwd\nbase_url: \"{{ vars.base_url }}\"\nauth:\n  accepts: [jwt_bearer]\n  jwt_bearer:\n    token_url: \"{{ auth.token_uri }}\"\n    claims: { iss: \"{{ auth.client_email }}\", scope: \"{{ vars.scope }}\", sub: \"{{ vars.admin_email }}\" }\nvars: { scope: cloud-platform, admin_email: \"admin@example.com\" }\nendpoints:\n  - { unit: one, path: /auth/oauth, rows: { decoder: json_array } }\n  - { unit: two, path: /auth/oauth, vars: { admin_email: \"other@example.com\" }, rows: { decoder: json_array } }\n";
    let mut inst = instance(&fx, "profile: x\ntopic: t\nauth: { mode: jwt_bearer }\n");
    inst.auth.service_account_key = Some(key_json.clone().into());
    let err = RestShape::from_instance(
        &profile(dwd),
        &inst,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .expect_err("the subject decides who the token acts as");
    assert!(
        err.to_string().contains("auth.jwt_bearer.claims.sub"),
        "{err}"
    );
    assert!(err.to_string().contains("two"), "{err}");
    assert_eq!(fx.token_exchanges(), 0, "nothing was minted at all");

    // A unit may still name its own host, and its own vars, as long as no
    // credential template reads them.
    let fine = dwd.replace(
        "token_url: \"{{ auth.token_uri }}\"",
        "token_url: \"{{ vars.token_url }}\"",
    );
    let fine = fine.replace(
        "vars: { scope: cloud-platform, admin_email: \"admin@example.com\" }",
        "vars: { scope: cloud-platform, admin_email: \"admin@example.com\", token_url: \"\" }",
    );
    let fine = fine.replace(
        "vars: { admin_email: \"other@example.com\" }",
        "vars: { page_size: 10 }",
    );
    let mut inst = instance(&fx, "profile: x\ntopic: t\nauth: { mode: jwt_bearer }\n");
    inst.auth.service_account_key = Some(key_json.into());
    inst.vars.insert(
        "token_url".into(),
        Value::String(format!("{}/token", fx.base_url())),
    );
    let s = RestShape::from_instance(
        &profile(&fine),
        &inst,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .expect("per-unit vars a credential template does not read are fine");
    assert_eq!(fetch(&s, "one", None).await.unwrap().len(), 1);
    assert_eq!(fetch(&s, "two", None).await.unwrap().len(), 1);
    assert_eq!(
        fx.token_exchanges(),
        1,
        "one mode, one token, whichever unit asks"
    );
    assert_eq!(fx.assertions()[0]["sub"], "admin@example.com");
}

#[tokio::test]
async fn two_instances_of_one_profile_carry_different_credential_kinds() {
    let fx = common::start().await;
    let p = "profile: two\nbase_url: \"{{ vars.base_url }}\"\nauth:\n  accepts: [bearer, oauth2_client_credentials]\n  oauth2_client_credentials: { token_url: \"{{ base_url }}/token\" }\nendpoints:\n  - { unit: bearer, path: /auth/bearer, rows: { decoder: json_array } }\n  - { unit: oauth, path: /auth/oauth, rows: { decoder: json_array } }\n".to_string();
    let self_hosted = shape(
        &fx,
        &p,
        "profile: x\ntopic: a\nauth: { mode: bearer, token: secret-token }\n",
    );
    let cloud = shape(
        &fx,
        &p,
        "profile: x\ntopic: b\nauth: { mode: oauth2_client_credentials, client_id: client-a, client_secret: secret-a }\n",
    );
    assert_eq!(fetch(&self_hosted, "bearer", None).await.unwrap().len(), 1);
    assert_eq!(fetch(&cloud, "oauth", None).await.unwrap().len(), 1);
    assert_eq!(
        fx.requests_to("/auth/bearer")[0].authorization.as_deref(),
        Some("Bearer secret-token")
    );
    assert_eq!(
        fx.requests_to("/auth/oauth")[0].authorization.as_deref(),
        Some("Bearer tok-1")
    );
    assert_eq!(
        fx.token_exchanges(),
        1,
        "the bearer instance never touched the token endpoint"
    );
}

#[tokio::test]
async fn retries_a_429_with_retry_after_and_never_a_403() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}retry: {{ min_backoff_ms: 1, max_backoff_ms: 5 }}\nerror: {{ at: /error }}\nendpoints:\n  - {{ unit: flaky, path: /retry/flaky, rows: {{ decoder: json_array }} }}\n  - {{ unit: forbidden, path: /retry/forbidden, rows: {{ decoder: json_array }} }}\n  - {{ unit: bad, path: /error/bad, rows: {{ decoder: json_array }} }}\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    assert_eq!(fetch(&s, "flaky", None).await.unwrap().len(), 1);
    assert_eq!(fx.recorded.lock().unwrap().flaky_hits, 2, "429 then 200");

    let err = fetch(&s, "forbidden", None).await.unwrap_err();
    match err {
        Error::Api { status, text, .. } => {
            assert_eq!(status, 403);
            assert_eq!(
                text, "the API client grant does not permit this",
                "error.at read the text"
            );
        }
        other => panic!("expected Api 403, got {other:?}"),
    }
    assert_eq!(
        fx.recorded.lock().unwrap().forbidden_hits,
        1,
        "a refusal is never retried"
    );

    let err = fetch(&s, "bad", None).await.unwrap_err();
    assert!(
        matches!(&err, Error::Api { status: 400, text, .. } if text == "missing or invalid _oid Parameter"),
        "{err:?}"
    );
    assert_eq!(err.api_error_code(), "4xx");
}

#[tokio::test]
async fn a_post_body_cursor_is_injected_into_the_rendered_body() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}endpoints:\n  - unit: search\n    method: POST\n    path: /post/search\n    body: {{ limit: 2, org: \"{{{{ vars.org }}}}\" }}\n    rows: {{ decoder: json_at, at: /items }}\n    paginate: {{ strategy: cursor, from: \"body:/cursor\", into: \"body:/cursor\" }}\n"
    );
    let s = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: none }\nvars: { org: acme }\n",
    );
    let rows = fetch(&s, "search", None).await.unwrap();
    assert_eq!(rows.len(), 4);
    let seen = fx.requests_to("/post/search");
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].body.as_ref().unwrap()["org"], "acme");
    assert!(
        seen[0].body.as_ref().unwrap().get("cursor").is_none(),
        "first request carries no cursor"
    );
    assert_eq!(seen[1].body.as_ref().unwrap()["cursor"], "c1");
}

/// The 1Password shape: the first POST carries the window and a NUMERIC
/// page size, every later one carries the cursor alone, and `has_more:
/// false` ends the sequence although a cursor is present. A unit's own
/// `vars` override the instance's for that unit only.
#[tokio::test]
async fn a_body_replace_cursor_sends_the_bare_cursor_and_body_leaves_stay_typed() {
    let fx = common::start().await;
    let endpoint = |unit: &str| {
        format!(
            "  - unit: {unit}\n    method: POST\n    path: /post/replace\n    body: {{ limit: \"{{{{ vars.limit }}}}\", org: \"{{{{ vars.org }}}}\", ids: \"{{{{ vars.ids }}}}\" }}\n    rows: {{ decoder: json_at, at: /items }}\n    paginate: {{ strategy: cursor, from: \"body:/cursor\", into: \"body_replace:/cursor\", stop_when: \"body.has_more == false\" }}\n"
        )
    };
    let p = format!(
        "{PLAIN}vars: {{ limit: 2, ids: [\"a\", \"b\"] }}\nendpoints:\n{}{}",
        endpoint("search"),
        endpoint("other")
    );
    let s = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: none }\nvars: { org: acme }\nunits: { other: { vars: { limit: 7 } } }\n",
    );
    let rows = fetch(&s, "search", None).await.unwrap();
    assert_eq!(
        rows.len(),
        4,
        "two pages; the cursor on the last page is not followed"
    );
    let seen = fx.requests_to("/post/replace");
    assert_eq!(seen.len(), 2);
    assert_eq!(
        seen[0].body,
        Some(serde_json::json!({"limit": 2, "org": "acme", "ids": ["a", "b"]})),
        "single-expression leaves keep their type"
    );
    assert_eq!(
        seen[1].body,
        Some(serde_json::json!({"cursor": "r1"})),
        "the next request is the cursor and nothing else"
    );

    fetch(&s, "other", None).await.unwrap();
    let other = &fx.requests_to("/post/replace")[2];
    assert_eq!(
        other.body.as_ref().unwrap()["limit"],
        7,
        "the unit's vars override the instance's for that unit"
    );
    assert_eq!(other.body.as_ref().unwrap()["org"], "acme");
}

/// A keyset unit (the PyPI shape): one request per key of the instance's
/// list, each document stamped with the key it was asked for, a key the
/// provider does not know answered by an ignored 404 and skipped, and an
/// empty list requesting nothing.
#[tokio::test]
async fn a_keyset_unit_requests_each_key_and_stamps_it_on_the_document() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}vars: {{ packages: [] }}\nendpoints:\n  - unit: metadata\n    path: \"/keyed/{{{{ key }}}}\"\n    rows: {{ decoder: document }}\n    construct: {{ keyset: {{ from: \"{{{{ vars.packages }}}}\" }} }}\n    ignore_status: [404]\n    add_fields: {{ _dfe_fetcher_package: \"{{{{ key }}}}\", fixed: constant }}\n"
    );
    let s = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: none }\nvars: { packages: [requests, missing, hyperi-pylib] }\n",
    );
    let rows = fetch(&s, "metadata", None).await.unwrap();
    assert_eq!(
        rows,
        [
            serde_json::json!({"info": {"name": "requests"}, "version": "requests-1.0", "_dfe_fetcher_package": "requests"}),
            serde_json::json!({"info": {"name": "hyperi-pylib"}, "version": "hyperi-pylib-1.0", "_dfe_fetcher_package": "hyperi-pylib"}),
        ],
        "one document per known key, stamped with its key by the shape"
    );
    let paths: Vec<String> = fx
        .recorded
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|s| s.path.starts_with("/keyed/"))
        .map(|s| s.path.clone())
        .collect();
    assert_eq!(
        paths,
        ["/keyed/requests", "/keyed/missing", "/keyed/hyperi-pylib"],
        "every key is requested once, in order"
    );
    assert_eq!(
        s.units()[0].add_fields,
        [("fixed".to_string(), serde_json::json!("constant"))],
        "the static field rides on the unit; the key field is stamped by the shape"
    );

    let none = shape(&fx, &p, BEARER_INSTANCE);
    assert!(fetch(&none, "metadata", None).await.unwrap().is_empty());

    let not_a_list = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: none }\nvars: { packages: requests }\n",
    );
    let err = fetch(&not_a_list, "metadata", None).await.unwrap_err();
    assert!(err.to_string().contains("not a list"), "{err}");
}

/// A lookup unit (the CrowdStrike shape): the pages carry ids walked by
/// offset against a total, and the rows are the entities a POST per batch
/// of ids returns, in id order, with a partial last batch.
#[tokio::test]
async fn a_lookup_unit_posts_each_batch_of_ids_and_yields_the_entities() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}retry: {{ retry_non_idempotent: true }}\nendpoints:\n  - unit: alerts\n    path: /lookup/ids\n    query: {{ limit: 2 }}\n    rows: {{ decoder: json_at, at: /resources }}\n    paginate: {{ strategy: offset, param: offset, total_at: /meta/pagination/total }}\n    construct:\n      lookup:\n        batch: 3\n        request: {{ path: /lookup/entities, body: {{ ids: \"{{{{ ids }}}}\" }} }}\n        rows: {{ decoder: json_at, at: /resources }}\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    let rows = fetch(&s, "alerts", None).await.unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(
        ids,
        ["id-1", "id-2", "id-3", "id-4", "id-5"],
        "entities in id order"
    );
    let offsets: Vec<String> = fx
        .requests_to("/lookup/ids")
        .iter()
        .map(|r| {
            r.query
                .iter()
                .find(|(k, _)| k == "offset")
                .unwrap()
                .1
                .clone()
        })
        .collect();
    assert_eq!(
        offsets,
        ["0", "2", "4"],
        "the offset advances by the ids each page carried, up to the total"
    );
    let batches: Vec<Value> = fx
        .requests_to("/lookup/entities")
        .iter()
        .map(|r| r.body.as_ref().unwrap()["ids"].clone())
        .collect();
    assert_eq!(
        batches,
        [
            serde_json::json!(["id-1", "id-2", "id-3"]),
            serde_json::json!(["id-4", "id-5"]),
        ],
        "a full batch as soon as it fills, the rest at the end, as a typed list"
    );
}

#[tokio::test]
async fn an_empty_var_omits_the_query_parameter_and_a_unit_override_adds_one() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}defaults: {{ query: {{ _oid: \"{{{{ vars.org_id }}}}\" }} }}\nendpoints:\n  - {{ unit: items, path: /array/items.json, rows: {{ decoder: json_array }} }}\n"
    );
    let s = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: none }\nvars: { org_id: \"\" }\nunits: { items: { query: { fields: \"id,alive\" } } }\n",
    );
    fetch(&s, "items", None).await.unwrap();
    let seen = fx.requests_to("/array/items.json");
    assert_eq!(
        seen[0].query,
        [("fields".to_string(), "id,alive".to_string())],
        "_oid omitted, fields added"
    );
}

/// A profile declares the variables it reads with their defaults, so an
/// instance may leave one unset: the default renders, an empty default omits
/// the parameter, and an instance value wins over the default. Without the
/// default an unset variable is a render error, which is the typo guard the
/// strict rule exists for.
#[tokio::test]
async fn a_profile_var_default_lets_an_instance_leave_the_var_unset() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}vars: {{ org_id: \"\" }}\ndefaults: {{ query: {{ _oid: \"{{{{ vars.org_id }}}}\" }} }}\nendpoints:\n  - {{ unit: items, path: /array/items.json, rows: {{ decoder: json_array }} }}\n"
    );
    let unset = shape(&fx, &p, "profile: x\ntopic: t\nauth: { mode: none }\n");
    fetch(&unset, "items", None).await.unwrap();
    assert!(
        fx.requests_to("/array/items.json")[0].query.is_empty(),
        "the empty default omits _oid"
    );

    let set = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: none }\nvars: { org_id: org-1 }\n",
    );
    fetch(&set, "items", None).await.unwrap();
    assert_eq!(
        fx.requests_to("/array/items.json")[1].query,
        [("_oid".to_string(), "org-1".to_string())],
        "the instance value overrides the profile default"
    );

    let no_default = p.replace("vars: { org_id: \"\" }\n", "");
    let strict = shape(
        &fx,
        &no_default,
        "profile: x\ntopic: t\nauth: { mode: none }\n",
    );
    let err = fetch(&strict, "items", None).await.unwrap_err();
    assert!(
        err.to_string().contains("vars.org_id"),
        "an undeclared, unset variable names itself: {err}"
    );
}

/// A profile with a `probe` sends that request as its health check, with the
/// instance's credential and the profile's headers; a refusal is the probe's
/// error. Without a `probe` the health check only resolves the credential.
#[tokio::test]
async fn the_probe_request_is_sent_with_the_credential_and_reports_a_refusal() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}headers: {{ Accept: application/vnd.fixture+json }}\nprobe: {{ path: /auth/bearer }}\nendpoints:\n  - {{ unit: items, path: /array/items.json, rows: {{ decoder: json_array }} }}\n"
    )
    .replace("auth: { accepts: [none] }", "auth: { accepts: [bearer] }");
    let ok = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: bearer, token: secret-token }\n",
    );
    ok.probe().await.expect("the probe answers 2xx");
    let seen = fx.requests_to("/auth/bearer");
    assert_eq!(seen.len(), 1, "one probe request, no data request");
    assert_eq!(
        seen[0].authorization.as_deref(),
        Some("Bearer secret-token")
    );
    assert!(
        fx.requests_to("/array/items.json").is_empty(),
        "the probe never touches a data endpoint"
    );

    let refused = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: bearer, token: wrong }\n",
    );
    let err = refused.probe().await.unwrap_err();
    assert!(matches!(err, Error::Api { status: 401, .. }), "{err:?}");

    let unprobed = shape(
        &fx,
        &p.replace("probe: { path: /auth/bearer }\n", ""),
        "profile: x\ntopic: t\nauth: { mode: bearer, token: wrong }\n",
    );
    unprobed
        .probe()
        .await
        .expect("without a probe only the credential is resolved");
    assert_eq!(fx.requests_to("/auth/bearer").len(), 2, "no third request");
}

/// A probe with `fail_when` reads its 2xx body: an API that answers a bad
/// credential with `ok: false` inside a 200 is unhealthy, and the error text
/// comes from `error.at`.
#[tokio::test]
async fn a_probe_fails_on_the_body_predicate_it_declares() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}error: {{ at: /error }}\nprobe: {{ path: /probe/status, query: {{ fail: \"{{{{ vars.fail }}}}\" }}, fail_when: \"body.ok == false\" }}\nvars: {{ fail: \"\" }}\nendpoints:\n  - {{ unit: items, path: /array/items.json, rows: {{ decoder: json_array }} }}\n"
    );
    let healthy = shape(&fx, &p, BEARER_INSTANCE);
    healthy.probe().await.expect("ok: true passes");

    let failing = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: none }\nvars: { fail: \"1\" }\n",
    );
    let err = failing.probe().await.unwrap_err();
    assert!(
        err.to_string().contains("invalid_auth"),
        "the text at error.at names the failure: {err}"
    );
    assert_eq!(fx.requests_to("/probe/status").len(), 2);
}

#[tokio::test]
async fn a_disabled_unit_is_not_bound_and_an_unknown_one_is_refused_at_bind() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}endpoints:\n  - {{ unit: a, path: /array/items.json, rows: {{ decoder: json_array }} }}\n  - {{ unit: b, path: /array/items.json, rows: {{ decoder: json_array }} }}\n"
    );
    let s = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: none }\nunits: { b: { enabled: false } }\n",
    );
    let names: Vec<&str> = s.units().iter().map(|u| &*u.name).collect();
    assert_eq!(names, ["a"]);
    let err = RestShape::from_instance(
        &profile(&p),
        &instance(
            &fx,
            "profile: x\ntopic: t\nauth: { mode: none }\nunits: { zzz: {} }\n",
        ),
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("units.zzz"), "{err}");
}

/// Units of one API on different audiences (Azure's Management and Graph):
/// a unit that names `auth.scope` sends a token minted for that scope, units
/// sharing a scope share one token, and a unit without one carries the
/// instance's.
#[tokio::test]
async fn a_unit_scope_mints_its_own_token_and_units_sharing_it_share_one() {
    let fx = common::start().await;
    let p = "profile: scoped\nbase_url: \"{{ vars.base_url }}\"\nauth:\n  accepts: [oauth2_client_credentials]\n  oauth2_client_credentials: { token_url: \"{{ base_url }}/token\", scope: mgmt, early_refresh_secs: 0 }\nendpoints:\n  - { unit: mgmt, path: /scoped/mgmt, rows: { decoder: json_array } }\n  - { unit: graph_a, path: /scoped/graph, auth: { scope: graph }, rows: { decoder: json_array } }\n  - { unit: graph_b, path: /scoped/graph, auth: { scope: graph }, rows: { decoder: json_array } }\n";
    let s = shape(
        &fx,
        p,
        "profile: x\ntopic: t\nauth: { mode: oauth2_client_credentials, client_id: client-a, client_secret: secret-a }\n",
    );
    assert_eq!(fetch(&s, "mgmt", None).await.unwrap().len(), 1);
    assert_eq!(fetch(&s, "graph_a", None).await.unwrap().len(), 1);
    assert_eq!(fetch(&s, "graph_b", None).await.unwrap().len(), 1);
    assert_eq!(
        fx.token_exchanges(),
        2,
        "one token per scope: the two graph units share theirs"
    );
    let mgmt = fx.requests_to("/scoped/mgmt");
    let graph = fx.requests_to("/scoped/graph");
    let bearer = |seen: &common::Seen| {
        seen.authorization
            .as_deref()
            .unwrap()
            .trim_start_matches("Bearer ")
            .to_owned()
    };
    assert_eq!(fx.scope_of(&bearer(&mgmt[0])), Some("mgmt".into()));
    assert_eq!(fx.scope_of(&bearer(&graph[0])), Some("graph".into()));
    assert_eq!(
        graph[0].authorization, graph[1].authorization,
        "shared token"
    );
}

/// A cold mode reached by many callers at once mints ONE token: the callers
/// that arrive while the exchange is in flight wait on it instead of each
/// posting to the token endpoint.
#[tokio::test]
async fn a_cold_mode_reached_by_many_callers_at_once_mints_one_token() {
    let fx = common::start().await;
    let p = "profile: rush\nbase_url: \"{{ vars.base_url }}\"\nauth:\n  accepts: [oauth2_client_credentials]\n  oauth2_client_credentials: { token_url: \"{{ base_url }}/token\" }\nendpoints:\n  - { unit: oauth, path: /auth/oauth, rows: { decoder: json_array } }\n";
    let s = shape(
        &fx,
        p,
        "profile: x\ntopic: t\nauth: { mode: oauth2_client_credentials, client_id: client-a, client_secret: secret-a }\n",
    );
    let ticks = (0..8).map(|_| fetch(&s, "oauth", None));
    for rows in futures::future::join_all(ticks).await {
        assert_eq!(rows.unwrap().len(), 1);
    }
    assert_eq!(fx.requests_to("/auth/oauth").len(), 8);
    assert_eq!(
        fx.token_exchanges(),
        1,
        "the callers that arrived during the exchange waited on it"
    );
}

/// A unit whose API lives on another host names its own `base_url`; the
/// profile's applies to the rest.
#[tokio::test]
async fn a_unit_names_its_own_base_url() {
    let fx = common::start().await;
    let p = "profile: hosts\nbase_url: \"{{ vars.base_url }}/nowhere\"\nauth: { accepts: [none] }\nendpoints:\n  - { unit: here, base_url: \"{{ vars.other_url }}\", path: /array/items.json, rows: { decoder: json_array } }\n  - { unit: there, path: /array/items.json, rows: { decoder: json_array } }\n";
    let mut inst = instance(&fx, BEARER_INSTANCE);
    inst.vars.insert(
        "other_url".into(),
        Value::String(format!("{}/", fx.base_url())),
    );
    let s = RestShape::from_instance(
        &profile(p),
        &inst,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .unwrap();
    assert_eq!(fetch(&s, "here", None).await.unwrap().len(), 4);
    let err = fetch(&s, "there", None).await.unwrap_err();
    assert!(
        matches!(err, Error::Api { status: 404, .. }),
        "the profile's base is elsewhere: {err:?}"
    );
    assert_eq!(fx.requests_to("/array/items.json").len(), 1);
}

/// `rows.content` is a template rendered once at bind: a var decides
/// whether the unit's rows are JSON or opaque bytes, a literal that is
/// neither is refused by validation, and a dump unit cannot be binary.
#[tokio::test]
async fn rows_content_is_bound_from_the_vars_and_a_binary_dump_is_refused() {
    use dfe_fetcher_core::RowContent;

    let fx = common::start().await;
    let p = format!(
        "{PLAIN}vars: {{ output_format: json }}\nendpoints:\n  - unit: metrics\n    path: /array/items.json\n    rows: {{ decoder: json_array, content: \"{{{{ vars.output_format == 'otlp' ? 'binary' : 'json' }}}}\" }}\n  - unit: raw\n    path: /array/items.json\n    rows: {{ decoder: document, content: binary }}\n"
    );
    let inst = instance(&fx, BEARER_INSTANCE);
    let s = RestShape::from_instance(
        &profile(&p),
        &inst,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .unwrap();
    let content = |name: &str| {
        s.units()
            .iter()
            .find(|u| &*u.name == name)
            .map(|u| u.content)
            .unwrap()
    };
    assert_eq!(content("metrics"), RowContent::Json, "the var says json");
    assert_eq!(content("raw"), RowContent::Binary, "a literal");
    let mut otlp = inst.clone();
    otlp.vars
        .insert("output_format".into(), Value::String("otlp".into()));
    let s = RestShape::from_instance(
        &profile(&p),
        &otlp,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .unwrap();
    assert!(
        s.units()
            .iter()
            .find(|u| &*u.name == "metrics")
            .unwrap()
            .is_binary(),
        "the var says otlp, so the unit is binary"
    );

    let bad = profile(&p.replace("content: binary", "content: protobuf"));
    let issues = bad.validate();
    assert!(
        issues
            .iter()
            .any(|i| i.field == "endpoints[1].rows.content" && i.message.contains("`protobuf`")),
        "{issues:?}"
    );
    let dump = profile(&p.replace(
        "    path: /array/items.json\n    rows: { decoder: document, content: binary }",
        "    shape: dump\n    path: /array/items.json\n    rows: { decoder: document, content: binary }",
    ));
    let issues = dump.validate();
    assert!(
        issues
            .iter()
            .any(|i| i.field == "endpoints[1].rows.content" && i.message.contains("dump")),
        "{issues:?}"
    );
    let err = RestShape::from_instance(
        &profile(
            &p.replace("output_format: json", "output_format: otlp")
                .replace(
                    "  - unit: metrics\n",
                    "  - unit: metrics\n    shape: dump\n",
                ),
        ),
        &inst,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("endpoints[metrics].rows.content")
            && err.to_string().contains("dump"),
        "a template that renders binary on a dump is refused at bind: {err}"
    );
}

/// The Log Analytics result shape: `rows.builder: columnar_table` turns each
/// table framed at `/tables` into one object per row keyed by column, across
/// every table in the page.
#[tokio::test]
async fn a_columnar_table_page_becomes_one_row_per_table_row() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}endpoints:\n  - unit: query\n    method: POST\n    path: /columnar/query\n    body: {{ query: \"{{{{ vars.kql }}}}\", timespan: \"{{{{ window.start }}}}/{{{{ window.end }}}}\" }}\n    rows: {{ decoder: json_at, at: /tables, builder: columnar_table }}\n"
    );
    let mut inst = instance(&fx, BEARER_INSTANCE);
    inst.vars
        .insert("kql".into(), Value::String("Heartbeat | take 10".into()));
    let s = RestShape::from_instance(
        &profile(&p),
        &inst,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .unwrap();
    let w = FetchWindow {
        start: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        end: Utc.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap(),
    };
    let rows = fetch(&s, "query", Some(&w)).await.unwrap();
    assert_eq!(
        rows,
        [
            serde_json::json!({"TimeGenerated": "2026-01-01T00:00:00Z", "Computer": "web-1"}),
            serde_json::json!({"TimeGenerated": "2026-01-01T00:01:00Z", "Computer": "web-2"}),
            serde_json::json!({"Count": 3}),
        ]
    );
    let seen = fx.requests_to("/columnar/query");
    assert_eq!(
        seen[0].body,
        Some(
            serde_json::json!({"query": "Heartbeat | take 10", "timespan": "2026-01-01T00:00:00Z/2026-01-01T01:00:00Z"})
        )
    );
}

/// The JWT-bearer grant end to end: the assertion is signed with the
/// instance's key (a key JSON, or a file holding one), its claims come from
/// the profile's templates with `iss` and `aud` read off the key, an empty
/// `sub` is left out, the provider verifies the signature, and the minted
/// token is cached across requests.
#[tokio::test]
async fn a_jwt_bearer_instance_signs_an_assertion_the_provider_verifies() {
    let fx = common::start().await;
    let (private_pem, public_pem) = common::rsa_key_pair();
    fx.accept_assertions_from(&public_pem);
    let key_json = common::service_account_key(&private_pem, &format!("{}/token", fx.base_url()));
    let p = "profile: jwt\nbase_url: \"{{ vars.base_url }}\"\nauth:\n  accepts: [jwt_bearer]\n  jwt_bearer:\n    token_url: \"{{ auth.token_uri }}\"\n    claims: { iss: \"{{ auth.client_email }}\", scope: \"{{ vars.scope }}\", aud: \"{{ auth.token_url }}\", sub: \"{{ vars.admin_email }}\" }\n    ttl_secs: 600\nvars: { scope: cloud-platform, admin_email: \"\" }\nendpoints:\n  - { unit: oauth, path: /auth/oauth, rows: { decoder: json_array } }\n  - { unit: scoped, path: /scoped/reports, auth: { scope: reports }, rows: { decoder: json_array } }\n";
    let mut inst = instance(&fx, "profile: x\ntopic: t\nauth: { mode: jwt_bearer }\n");
    inst.auth.service_account_key = Some(key_json.clone().into());
    let s = RestShape::from_instance(
        &profile(p),
        &inst,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .unwrap();
    assert_eq!(fetch(&s, "oauth", None).await.unwrap().len(), 1);
    assert_eq!(fetch(&s, "oauth", None).await.unwrap().len(), 1);
    assert_eq!(
        fx.token_exchanges(),
        1,
        "the second request reused the cached token"
    );
    let claims = fx.assertions();
    assert_eq!(claims.len(), 1);
    assert_eq!(
        claims[0]["iss"], "fetcher@test-project.iam.gserviceaccount.com",
        "read off the key"
    );
    assert_eq!(claims[0]["scope"], "cloud-platform");
    assert_eq!(claims[0]["aud"], format!("{}/token", fx.base_url()));
    assert!(claims[0].get("sub").is_none(), "an empty claim is left out");
    assert_eq!(
        claims[0]["exp"].as_i64().unwrap() - claims[0]["iat"].as_i64().unwrap(),
        600
    );

    // A unit scope replaces the `scope` claim for that unit's token.
    assert_eq!(fetch(&s, "scoped", None).await.unwrap().len(), 1);
    assert_eq!(fx.assertions()[1]["scope"], "reports");

    // The same key from a file the instance names by path, with a `sub`.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sa-key.json");
    std::fs::write(&path, &key_json).unwrap();
    let mut by_file = instance(&fx, "profile: x\ntopic: t\nauth: { mode: jwt_bearer }\n");
    by_file.auth.service_account_key_file = Some(path.to_string_lossy().into_owned().into());
    by_file.vars.insert(
        "admin_email".into(),
        Value::String("admin@example.com".into()),
    );
    let s = RestShape::from_instance(
        &profile(p),
        &by_file,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .unwrap();
    assert_eq!(fetch(&s, "oauth", None).await.unwrap().len(), 1);
    assert_eq!(fx.assertions()[2]["sub"], "admin@example.com");

    // A signature the provider cannot verify is a terminal refusal.
    let (other_private, _) = common::rsa_key_pair();
    let mut wrong = instance(&fx, "profile: x\ntopic: t\nauth: { mode: jwt_bearer }\n");
    wrong.auth.private_key = Some(other_private.into());
    let bare = p.replace(
        "token_url: \"{{ auth.token_uri }}\"",
        "token_url: \"{{ base_url }}/token\"",
    );
    let s = RestShape::from_instance(
        &profile(&bare),
        &wrong,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .unwrap();
    let err = fetch(&s, "oauth", None).await.unwrap_err();
    assert!(matches!(err, Error::Api { status: 401, .. }), "{err:?}");
}

/// The GCE metadata mode asks the metadata server for the workload's token
/// with the `Metadata-Flavor` header it requires and caches it.
#[tokio::test]
async fn gce_metadata_fetches_the_workload_token_from_the_metadata_server() {
    let fx = common::start().await;
    let p = "profile: gce\nbase_url: \"{{ vars.base_url }}\"\nauth:\n  accepts: [gce_metadata]\n  gce_metadata: { url: \"{{ vars.metadata_url }}\" }\nendpoints:\n  - { unit: oauth, path: /auth/oauth, rows: { decoder: json_array } }\n";
    let mut inst = instance(&fx, "profile: x\ntopic: t\nauth: { mode: gce_metadata }\n");
    inst.vars.insert(
        "metadata_url".into(),
        Value::String(format!("{}/metadata/token", fx.base_url())),
    );
    let s = RestShape::from_instance(
        &profile(p),
        &inst,
        "conn",
        reqwest::Client::new(),
        &exchange(),
    )
    .unwrap();
    assert_eq!(fetch(&s, "oauth", None).await.unwrap().len(), 1);
    assert_eq!(fetch(&s, "oauth", None).await.unwrap().len(), 1);
    assert_eq!(fx.metadata_hits(), 1, "cached for its 3599 s");
    assert_eq!(
        fx.requests_to("/auth/oauth")[0].authorization.as_deref(),
        Some("Bearer meta-1")
    );
    s.probe().await.unwrap();
}

/// One `defaults.path` with `{{ unit.name }}` serves every unit that sets no
/// path of its own.
#[tokio::test]
async fn a_default_path_reads_the_unit_name() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}defaults: {{ path: \"/keyed/{{{{ unit.name }}}}\", rows: {{ decoder: document }} }}\nendpoints:\n  - {{ unit: login }}\n  - {{ unit: admin }}\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    assert_eq!(
        fetch(&s, "login", None).await.unwrap()[0]["version"],
        "login-1.0"
    );
    assert_eq!(
        fetch(&s, "admin", None).await.unwrap()[0]["version"],
        "admin-1.0"
    );
}

/// A keyset read from a request (the GuardDuty shape): the unit first asks
/// for its detector ids, walks each detector's paged finding ids, and
/// looks the ids of a detector up under THAT detector before moving to the
/// next, so a batch never mixes keys.
#[tokio::test]
async fn a_keyset_request_yields_the_keys_and_each_key_looks_up_its_own_ids() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}retry: {{ retry_non_idempotent: true }}\nendpoints:\n  - unit: findings\n    method: POST\n    path: \"/detector/{{{{ key }}}}/findings\"\n    body: {{ maxResults: 2 }}\n    rows: {{ decoder: json_at, at: /findingIds }}\n    paginate: {{ strategy: cursor, from: \"body:/nextToken\", into: \"body:/nextToken\" }}\n    construct:\n      keyset:\n        request: {{ method: GET, path: /detector }}\n        keys_at: /detectorIds\n      lookup:\n        batch: 50\n        request: {{ path: \"/detector/{{{{ key }}}}/findings/get\", body: {{ findingIds: \"{{{{ ids }}}}\" }} }}\n        rows: {{ decoder: json_at, at: /findings }}\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    let rows = fetch(&s, "findings", None).await.unwrap();
    let landed: Vec<(String, String)> = rows
        .iter()
        .map(|r| {
            (
                r["id"].as_str().unwrap().to_owned(),
                r["detector"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        landed,
        [
            ("f1".to_string(), "d1".to_string()),
            ("f2".to_string(), "d1".to_string()),
            ("f3".to_string(), "d1".to_string()),
            ("f4".to_string(), "d2".to_string()),
        ],
        "every finding looked up under the detector that listed it"
    );
    assert_eq!(fx.requests_to("/detector").len(), 1, "the keys once");
    let d1_pages = fx.requests_to("/detector/d1/findings");
    assert_eq!(d1_pages.len(), 2, "three ids at two a page");
    assert_eq!(
        d1_pages[1].body.as_ref().unwrap()["nextToken"],
        "from-2",
        "the cursor fed back in the body"
    );
    let d1_lookup = fx.requests_to("/detector/d1/findings/get");
    assert_eq!(
        d1_lookup.len(),
        1,
        "one batch for the key, sent before the next key"
    );
    assert_eq!(
        d1_lookup[0].body.as_ref().unwrap()["findingIds"],
        serde_json::json!(["f1", "f2", "f3"])
    );
    assert_eq!(
        fx.requests_to("/detector/d2/findings/get")[0]
            .body
            .as_ref()
            .unwrap()["findingIds"],
        serde_json::json!(["f4"])
    );
    let order: Vec<String> = fx
        .recorded
        .lock()
        .unwrap()
        .requests
        .iter()
        .map(|s| s.path.clone())
        .filter(|p| p.starts_with("/detector/"))
        .collect();
    assert_eq!(
        order,
        [
            "/detector/d1/findings",
            "/detector/d1/findings",
            "/detector/d1/findings/get",
            "/detector/d2/findings",
            "/detector/d2/findings/get",
        ],
        "d1's lookup goes out before d2's first page"
    );
}

/// A lookup whose batch response pages (the CloudWatch shape): the ids are
/// metric descriptors a keyset of namespaces listed with the key as the
/// whole body, the builder shapes them into positional queries and joins
/// each page of the answer back, and the second page is asked for with the
/// token the first returned.
#[tokio::test]
async fn a_lookup_batch_follows_its_own_pages_and_the_builder_joins_them() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}retry: {{ retry_non_idempotent: true }}\nwindow: {{ format: epoch_secs }}\nvars: {{ namespaces: [], stat: Maximum }}\nendpoints:\n  - unit: metrics\n    method: POST\n    path: /metrics/list\n    body: \"{{{{ key }}}}\"\n    rows: {{ decoder: json_at, at: /Metrics }}\n    construct:\n      keyset: {{ from: \"{{{{ vars.namespaces }}}}\" }}\n      lookup:\n        batch: 500\n        request:\n          path: /metrics/data\n          body: {{ StartTime: \"{{{{ int(window.start) }}}}\", EndTime: \"{{{{ int(window.end) }}}}\", MetricDataQueries: \"{{{{ ids }}}}\" }}\n        rows: {{ decoder: document, builder: cloudwatch_metrics }}\n        paginate: {{ strategy: cursor, from: \"body:/NextToken\", into: \"body:/NextToken\" }}\n"
    );
    let s = shape(
        &fx,
        &p,
        "profile: x\ntopic: t\nauth: { mode: none }\nvars: { namespaces: [{Namespace: AWS/EC2}, {Namespace: AWS/RDS}] }\n",
    );
    let window = FetchWindow {
        start: Utc.timestamp_opt(1_709_424_000, 0).single().unwrap(),
        end: Utc.timestamp_opt(1_709_427_600, 0).single().unwrap(),
    };
    let rows = fetch(&s, "metrics", Some(&window)).await.unwrap();
    let lists = fx.requests_to("/metrics/list");
    assert_eq!(
        lists
            .iter()
            .map(|r| r.body.clone().unwrap())
            .collect::<Vec<_>>(),
        [
            serde_json::json!({"Namespace": "AWS/EC2"}),
            serde_json::json!({"Namespace": "AWS/RDS"})
        ],
        "the key is the whole body"
    );
    let data = fx.requests_to("/metrics/data");
    assert_eq!(data.len(), 2, "one batch of four descriptors, two pages");
    let first = data[0].body.as_ref().unwrap();
    assert_eq!(
        first["StartTime"], 1_709_424_000,
        "a typed number from int()"
    );
    assert_eq!(
        first["MetricDataQueries"][3]["Id"], "q3",
        "positional ids across both namespaces"
    );
    assert_eq!(
        first["MetricDataQueries"][3]["MetricStat"]["Metric"]["Namespace"],
        "AWS/RDS"
    );
    assert_eq!(
        first["MetricDataQueries"][0]["MetricStat"]["Stat"],
        "Maximum"
    );
    assert!(first.get("NextToken").is_none());
    assert_eq!(data[1].body.as_ref().unwrap()["NextToken"], "more");
    assert_eq!(rows.len(), 8, "four metrics, one datapoint per page");
    assert_eq!(rows[0]["metric_name"], "CPUUtilization");
    assert_eq!(rows[0]["namespace"], "AWS/EC2");
    assert_eq!(rows[0]["unit"], "Percent");
    assert_eq!(rows[0]["timestamp"], 1_709_424_000.0);
    assert_eq!(rows[3]["namespace"], "AWS/RDS");
    assert_eq!(rows[3]["unit"], "None");
    assert_eq!(
        rows[4]["timestamp"], 1_709_424_300.0,
        "the second page's datapoints"
    );
    assert_eq!(rows[7]["stat"], "Maximum");
}

/// The profile's base URL is rendered per unit, so a unit var may pick
/// the host path, and a SigV4 unit's scope follows its own vars.
#[tokio::test]
async fn the_base_url_and_the_sigv4_scope_render_from_the_units_vars() {
    let fx = common::start().await;
    let p = "profile: aws\nbase_url: \"{{ vars.base_url }}/{{ vars.prefix }}\"\nauth:\n  accepts: [sigv4]\n  sigv4: { service: \"{{ vars.service }}\", region: \"{{ vars.region }}\" }\nvars: { prefix: sigv4, region: us-east-1, service: sts }\nendpoints:\n  - { unit: trail, path: /scope, vars: { service: cloudtrail }, rows: { decoder: json_array } }\n  - { unit: health, path: /scope, vars: { service: health, region: us-east-1 }, rows: { decoder: json_array } }\n  - { unit: items, path: /items.json, vars: { prefix: array }, rows: { decoder: json_array } }\n";
    let s = shape(
        &fx,
        p,
        "profile: x\ntopic: t\nauth: { mode: sigv4, access_key_id: AKIAIOSFODNN7EXAMPLE, secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY }\nvars: { region: ap-southeast-2 }\n",
    );
    let trail = fetch(&s, "trail", None).await.unwrap();
    assert!(
        trail[0]["scope"]
            .as_str()
            .unwrap()
            .ends_with("/ap-southeast-2/cloudtrail/aws4_request"),
        "{trail:?}"
    );
    let health = fetch(&s, "health", None).await.unwrap();
    assert!(
        health[0]["scope"]
            .as_str()
            .unwrap()
            .ends_with("/us-east-1/health/aws4_request"),
        "the region-locked unit signs for its own region: {health:?}"
    );
    assert_eq!(
        fetch(&s, "items", None).await.unwrap().len(),
        4,
        "the unit's prefix var reached the profile's base_url template"
    );
    assert_eq!(fx.requests_to("/array/items.json").len(), 1);
}

/// A manifest unit (the OMAP content list): the pages' rows are items
/// listed across a `NextPageUri` header, each fetched by the whole URL it
/// names in list order, its records the unit's rows, every row marked with
/// the item's key and position; an item the store no longer has answers a
/// status the item request ignores and yields nothing, and without that
/// ignore the tick fails on it.
#[tokio::test]
async fn a_manifest_unit_fetches_each_item_and_marks_its_rows_with_the_item() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}error: {{ at: /error/message }}\nendpoints:\n  - unit: content\n    path: /manifest/list\n    rows: {{ decoder: json_array }}\n    paginate: {{ strategy: request_path, from: \"header:NextPageUri\" }}\n    construct:\n      manifest:\n        item_request: {{ path: \"{{{{ item.uri }}}}\", ignore_status: [404] }}\n        rows: {{ decoder: json_array }}\n        key: \"{{{{ item.id }}}}\"\n        position: \"{{{{ item.created }}}}\"\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    let rows = fetch_rows(&s, "content", None).await.unwrap();
    let landed: Vec<(String, Option<Mark>)> = rows
        .iter()
        .map(|r| {
            let value: Value = serde_json::from_slice(&r.payload).unwrap();
            (value["id"].as_str().unwrap().to_owned(), r.mark.clone())
        })
        .collect();
    let item = |id: &str, minute: u32| {
        Some(Mark::Item {
            key: id.into(),
            position: Utc.with_ymd_and_hms(2026, 1, 1, 0, minute, 0).unwrap(),
        })
    };
    assert_eq!(
        landed,
        [
            ("a-1".to_string(), item("a", 0)),
            ("a-2".to_string(), item("a", 0)),
            ("b-1".to_string(), item("b", 10)),
            ("b-2".to_string(), item("b", 10)),
            ("c-1".to_string(), item("c", 30)),
            ("c-2".to_string(), item("c", 30)),
        ],
        "every item's records in list order, each marked with its item"
    );
    let paths: Vec<String> = fx
        .paths()
        .into_iter()
        .filter(|p| p.starts_with("/manifest/"))
        .collect();
    assert_eq!(
        paths,
        [
            "/manifest/list",
            "/manifest/blob/a",
            "/manifest/blob/b",
            "/manifest/list",
            "/manifest/blob/missing",
            "/manifest/blob/c",
        ],
        "a page's items are fetched before the next page is asked for"
    );
    assert_eq!(
        fx.requests_to("/manifest/list")[1].query,
        [("page".to_string(), "2".to_string())],
        "the header URL is used as-is"
    );

    let strict = shape(
        &fx,
        &p.replace(", ignore_status: [404]", ""),
        BEARER_INSTANCE,
    );
    let err = fetch_rows(&strict, "content", None).await.unwrap_err();
    assert!(
        matches!(&err, Error::Api { status: 404, text, .. } if text == "Content requested has already expired."),
        "an item the request does not ignore fails the tick with the API's text: {err:?}"
    );

    let unmarked = shape(
        &fx,
        &p.replace(
            "        key: \"{{ item.id }}\"\n        position: \"{{ item.created }}\"\n",
            "",
        ),
        BEARER_INSTANCE,
    );
    let rows = fetch_rows(&unmarked, "content", None).await.unwrap();
    assert_eq!(rows.len(), 6);
    assert!(rows.iter().all(|r| r.mark.is_none()), "no key, no mark");
}

/// A prelude (the OMAP `subscriptions/start`): each step goes out once
/// per tick before the first page, in order, with the unit's vars and the
/// instance's credential; the 400 an already-enabled subscription answers
/// is ignored and the pages follow, while a step that is refused fails
/// the tick before any page is asked for.
#[tokio::test]
async fn a_prelude_runs_once_per_tick_before_the_first_page_and_ignores_its_status() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}error: {{ at: /error/message }}\nvars: {{ content_type: \"\" }}\ndefaults:\n  prelude:\n    - path: /prelude/start\n      query: {{ contentType: \"{{{{ vars.content_type }}}}\" }}\n      ignore_status: [400]\nendpoints:\n  - unit: general\n    vars: {{ content_type: Audit.General }}\n    path: /prelude/content\n    query: {{ contentType: \"{{{{ vars.content_type }}}}\" }}\n    rows: {{ decoder: json_array }}\n  - unit: refused\n    path: /prelude/content\n    rows: {{ decoder: json_array }}\n    prelude: [{{ path: /prelude/refused }}]\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    assert_eq!(fetch(&s, "general", None).await.unwrap().len(), 1);
    assert_eq!(fetch(&s, "general", None).await.unwrap().len(), 1);
    assert_eq!(
        fx.started(),
        ["Audit.General"],
        "started once, enabled thereafter"
    );
    let starts = fx.requests_to("/prelude/start");
    assert_eq!(starts.len(), 2, "one blind start per tick");
    assert_eq!(
        starts[1].query,
        [("contentType".to_string(), "Audit.General".to_string())]
    );
    assert_eq!(
        fx.paths()
            .into_iter()
            .filter(|p| p.starts_with("/prelude/"))
            .collect::<Vec<_>>(),
        [
            "/prelude/start",
            "/prelude/content",
            "/prelude/start",
            "/prelude/content",
        ],
        "the step goes out before the page, every tick"
    );

    let err = fetch(&s, "refused", None).await.unwrap_err();
    assert!(
        matches!(&err, Error::Api { status: 403, text, .. } if text == "the application lacks ActivityFeed.Read"),
        "a refused step fails the tick with the API's text: {err:?}"
    );
    assert_eq!(fx.requests_to("/prelude/refused").len(), 1);
    assert_eq!(
        fx.requests_to("/prelude/content").len(),
        2,
        "no page was asked for after the refused step"
    );
}

/// A unit whose requests never read the window is the provider's current
/// state, so the profile's `window.step` does not multiply it: the
/// windowed unit asks per step, the unwindowed one once.
#[tokio::test]
async fn a_unit_that_reads_no_window_runs_once_however_the_profile_chunks_it() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}window: {{ format: epoch_secs, step: 1h }}\nendpoints:\n  - unit: events\n    path: /window/events\n    query: {{ start: \"{{{{ window.start }}}}\", end: \"{{{{ window.end }}}}\" }}\n    rows: {{ decoder: json_array }}\n  - unit: state\n    path: /array/items.json\n    rows: {{ decoder: json_array }}\n"
    );
    let s = shape(&fx, &p, BEARER_INSTANCE);
    let window = FetchWindow {
        start: Utc.timestamp_opt(0, 0).single().unwrap(),
        end: Utc.timestamp_opt(9000, 0).single().unwrap(),
    };
    assert_eq!(fetch(&s, "events", Some(&window)).await.unwrap().len(), 3);
    assert_eq!(fx.requests_to("/window/events").len(), 3, "one per step");
    assert_eq!(fetch(&s, "state", Some(&window)).await.unwrap().len(), 4);
    assert_eq!(
        fx.requests_to("/array/items.json").len(),
        1,
        "the state unit is asked once for the tick"
    );
}

/// A fold unit (the Go module proxy): a keyset over modules, each key's
/// version list as text lines, each line a manifest item whose `.info`
/// document is one row, and the key's rows folded into ONE row stamped
/// with the key; a retracted version's 404 yields nothing, a module the
/// proxy does not know yields no row, and `max_items` bounds the items
/// fetched per key.
#[tokio::test]
async fn a_fold_unit_yields_one_row_per_key_from_its_items() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}vars: {{ modules: [] }}\nendpoints:\n  - unit: metadata\n    path: \"/goproxy/{{{{ key }}}}/@v/list\"\n    rows: {{ decoder: lines }}\n    ignore_status: [404]\n    fold: go_module_aggregate\n    add_fields: {{ _dfe_fetcher_module: \"{{{{ key }}}}\" }}\n    construct:\n      keyset: {{ from: \"{{{{ vars.modules }}}}\" }}\n      manifest:\n        item_request: {{ path: \"/goproxy/{{{{ key }}}}/@v/{{{{ item.line }}}}.info\", ignore_status: [404] }}\n        rows: {{ decoder: document }}\n"
    );
    let i =
        "profile: x\ntopic: t\nauth: { mode: none }\nvars: { modules: [textmod, unpublished] }\n";
    let s = shape(&fx, &p, i);
    let rows = fetch(&s, "metadata", None).await.unwrap();
    assert_eq!(rows.len(), 1, "one row per module that answered: {rows:?}");
    assert_eq!(rows[0]["_dfe_fetcher_module"], "textmod");
    assert_eq!(rows[0]["versions"], serde_json::json!(["v0.3.0", "v0.5.0"]));
    assert_eq!(rows[0]["version_info"]["v0.5.0"]["Version"], "v0.5.0");
    assert!(
        rows[0]["version_info"].get("v0.4.0").is_none(),
        "a retracted version's 404 yields no document"
    );
    assert_eq!(
        fx.paths()
            .into_iter()
            .filter(|p| p.starts_with("/goproxy/"))
            .collect::<Vec<_>>(),
        [
            "/goproxy/textmod/@v/list",
            "/goproxy/textmod/@v/v0.3.0.info",
            "/goproxy/textmod/@v/v0.4.0.info",
            "/goproxy/textmod/@v/v0.5.0.info",
            "/goproxy/unpublished/@v/list",
        ],
        "the list, then each version in list order, then the next module"
    );

    let capped = shape(
        &fx,
        &p.replace(
            "        rows: { decoder: document }\n",
            "        rows: { decoder: document }\n        max_items: 2\n",
        ),
        i,
    );
    let rows = fetch(&capped, "metadata", None).await.unwrap();
    assert_eq!(rows[0]["versions"], serde_json::json!(["v0.3.0"]));
    assert!(
        fx.requests_to("/goproxy/textmod/@v/v0.5.0.info").len() == 1,
        "the third version was not fetched under the cap"
    );
}

/// A token-minting mode exposes the token-response fields the profile
/// names as `auth.*`, so a Salesforce-shaped unit addresses the instance
/// the exchange named; a field the profile does not name never renders;
/// and a relative `nextRecordsUrl` resolves against the page's own URL.
#[tokio::test]
async fn an_exposed_token_field_reaches_the_templates_and_a_relative_next_url_resolves() {
    let fx = common::start().await;
    let p = "profile: sf\nbase_url: \"{{ vars.base_url }}\"\nauth:\n  accepts: [oauth2_client_credentials]\n  oauth2_client_credentials: { token_url: \"{{ base_url }}/token\", expose: [instance_url] }\nendpoints:\n  - unit: query\n    path: \"{{ auth.instance_url }}/query\"\n    query: { q: \"SELECT Id FROM LoginHistory\" }\n    rows: { decoder: json_at, at: /records }\n    paginate: { strategy: request_path, from: \"body:/nextRecordsUrl\" }\n  - unit: leak\n    path: \"{{ auth.refresh_token }}/query\"\n    rows: { decoder: json_at, at: /records }\n";
    let i = "profile: x\ntopic: t\nauth: { mode: oauth2_client_credentials, client_id: client-a, client_secret: secret-a }\n";
    let s = shape(&fx, p, i);
    let rows = fetch(&s, "query", None).await.unwrap();
    let ids: Vec<u64> = rows.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    assert_eq!(ids, [0, 1, 2], "both pages, the second by its relative URL");
    assert_eq!(fx.requests_to("/instance/query").len(), 1);
    assert_eq!(
        fx.requests_to("/instance/query/next-2000").len(),
        1,
        "resolved against the page's request URL, no `base` needed"
    );
    assert!(
        fx.token_exchanges() >= 1,
        "the render minted the token the exposed field came from"
    );

    let err = fetch(&s, "leak", None).await.unwrap_err();
    assert!(
        err.to_string().contains("refresh_token"),
        "a field outside `expose` is not in the context: {err}"
    );
}

/// An instance runs one profile endpoint under names of its own, each
/// with its vars and topic: two keyed documents from one `doc` endpoint,
/// tagged and routed apart.
#[tokio::test]
async fn an_instance_unit_runs_an_endpoint_under_its_own_name_and_topic() {
    let fx = common::start().await;
    let p = format!(
        "{PLAIN}endpoints:\n  - unit: doc\n    path: \"/keyed/{{{{ vars.key }}}}\"\n    rows: {{ decoder: document }}\n"
    );
    let i = "profile: x\ntopic: t\nauth: { mode: none }\nunits:\n  doc: { enabled: false }\n  beta: { endpoint: doc, vars: { key: beta } }\n  alpha: { endpoint: doc, vars: { key: alpha }, topic: alpha-topic }\n";
    let s = shape(&fx, &p, i);
    let names: Vec<&str> = s.units().iter().map(|u| &*u.name).collect();
    assert_eq!(
        names,
        ["alpha", "beta"],
        "the template is off; the instances in name order"
    );
    let topics: Vec<&str> = s.units().iter().map(|u| &*u.topic).collect();
    assert_eq!(topics, ["alpha-topic", "t"]);
    let rows = fetch(&s, "alpha", None).await.unwrap();
    assert_eq!(rows[0]["info"]["name"], "alpha");
    let rows = fetch(&s, "beta", None).await.unwrap();
    assert_eq!(rows[0]["info"]["name"], "beta");
    assert_eq!(fx.paths(), ["/keyed/alpha", "/keyed/beta"]);
}

/// A lister unit (the S3 backend): the signed listing walked on its
/// continuation token, the objects newer than the cutoff (the window's
/// start on a first tick, the checkpoint after) read oldest first, each
/// streamed through the manifest's decoder (a two-member gzip inflates
/// whole, a line that is not JSON is wrapped), every row stamped with the
/// object envelope and marked with the object's key and time; a second
/// tick from the checkpoint reads only what is newer.
#[tokio::test]
async fn a_lister_unit_lists_after_its_checkpoint_and_reads_each_object_in_time_order() {
    let fx = common::start().await;
    let p = "profile: s3\nbase_url: \"{{ vars.base_url }}/s3\"\nauth:\n  accepts: [sigv4]\n  sigv4: { service: s3, region: \"{{ vars.region }}\" }\nwindow: { lookback: 3650d }\nvars: { region: ap-southeast-2, prefix: \"\" }\nendpoints:\n  - unit: objects\n    lister: s3\n    path: \"{{ vars.bucket }}\"\n    query: { list-type: 2, prefix: \"{{ vars.prefix }}\", max-keys: 1000 }\n    max_pages: 10\n    construct:\n      manifest:\n        item_request: { path: \"{{ vars.bucket }}/{{ item.path }}\" }\n        rows: { decoder: ndjson, gzip: true, builder: wrap_non_object }\n        key: \"{{ item.key }}\"\n        position: \"{{ item.last_modified }}\"\n        max_items: 1000\n        add_fields:\n          _dfe_fetcher_object: { provider: s3, bucket: \"{{ vars.bucket }}\", key: \"{{ item.key }}\", last_modified: \"{{ item.last_modified }}\", size: \"{{ item.size }}\" }\n";
    let i = "profile: x\ntopic: t\nauth: { mode: sigv4, access_key_id: AKIATEST, secret_access_key: sekrit }\nvars: { bucket: logs, prefix: \"logs/\" }\n";
    let s = shape(&fx, p, i);
    let rows = fetch_rows(&s, "objects", None).await.unwrap();
    let landed: Vec<(Value, Option<Mark>)> = rows
        .iter()
        .map(|r| (serde_json::from_slice(&r.payload).unwrap(), r.mark.clone()))
        .collect();
    let at = |h: u32| Utc.with_ymd_and_hms(2026, 5, 21, h, 0, 0).unwrap();
    let ids: Vec<String> = landed
        .iter()
        .map(|(v, _)| {
            v.get("id")
                .or_else(|| v.get("_dfe_fetcher_raw_line"))
                .and_then(Value::as_str)
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        ids,
        ["b-1", "a-1", "a-2", "hello", "world"],
        "oldest object first, both gzip members, the text lines wrapped"
    );
    assert_eq!(
        landed[1].1,
        Some(Mark::Item {
            key: "logs/a b.jsonl.gz".into(),
            position: at(11)
        })
    );
    assert_eq!(
        landed[3]
            .1
            .as_ref()
            .map(|m| matches!(m, Mark::Item { position, .. } if *position == at(12))),
        Some(true)
    );
    let envelope = &landed[1].0["_dfe_fetcher_object"];
    assert_eq!(envelope["provider"], "s3");
    assert_eq!(envelope["bucket"], "logs");
    assert_eq!(envelope["key"], "logs/a b.jsonl.gz");
    assert_eq!(envelope["last_modified"], "2026-05-21T11:00:00+00:00");
    assert_eq!(envelope["size"], 40, "a template leaf keeps its type");
    assert!(
        landed[3].0["_dfe_fetcher_parse_error"].is_string(),
        "the text line carries its parse error beside the raw line"
    );
    let listings = fx.requests_to("/s3/logs");
    assert_eq!(listings.len(), 2, "two listing pages");
    assert_eq!(
        listings[0].query,
        [
            ("list-type".to_string(), "2".to_string()),
            ("max-keys".to_string(), "1000".to_string()),
            ("prefix".to_string(), "logs/".to_string()),
        ]
    );
    assert_eq!(
        listings[1]
            .query
            .iter()
            .find(|(k, _)| k == "continuation-token"),
        Some(&("continuation-token".to_string(), "page-2".to_string()))
    );
    assert!(
        listings[0]
            .authorization
            .as_deref()
            .is_some_and(|a| a.contains("/ap-southeast-2/s3/aws4_request")),
        "signed for s3 in the instance's region"
    );
    assert_eq!(
        fx.paths()
            .into_iter()
            .filter(|p| p.starts_with("/s3/logs/"))
            .collect::<Vec<_>>(),
        [
            "/s3/logs/logs/b.jsonl.gz",
            "/s3/logs/logs/a b.jsonl.gz",
            "/s3/logs/logs/c.txt"
        ],
        "each object fetched by its encoded key, in time order"
    );

    let spec = s.units()[0].clone();
    let checkpoint = dfe_fetcher_core::CheckpointValue::Item {
        key: "logs/a b.jsonl.gz".into(),
        position: at(11),
    };
    let tick = TickCtx {
        window: None,
        connection_id: "conn",
        unit: &spec,
        checkpoint: Some(&checkpoint),
    };
    let mut stream = s.rows(tick);
    let mut later = Vec::new();
    while let Some(row) = stream.next().await {
        later.push(row.unwrap());
    }
    assert_eq!(
        later.len(),
        2,
        "only the object newer than the checkpoint: {later:?}"
    );
    assert_eq!(
        fx.requests_to("/s3/logs/logs/b.jsonl.gz").len(),
        1,
        "an object at or before the checkpoint is not read again"
    );
}

/// A queue unit (the Pub/Sub pull): each pulled message's last row carries
/// its ack id as a mark, the tick pulls again until a pull is empty, and
/// the acknowledgement goes out only when the driver hands the ids back,
/// `ack_batch` at a time.
#[tokio::test]
async fn a_queue_unit_marks_each_message_with_its_ack_id_and_acks_when_told() {
    use dfe_fetcher_rest::shape::queue::QueueShape;

    let fx = common::start().await;
    let p = format!(
        "{PLAIN}retry: {{ retry_non_idempotent: true }}\nendpoints:\n  - unit: pull\n    method: POST\n    path: /queue/pull\n    body: {{ maxMessages: 10, returnImmediately: true }}\n    rows: {{ decoder: json_at, at: /receivedMessages, builder: pubsub_message }}\n    max_pages: 3\n    construct:\n      queue:\n        ack_at: /ackId\n        ack_request: {{ path: /queue/ack, body: {{ ackIds: \"{{{{ ids }}}}\" }} }}\n        ack_batch: 1\n"
    );
    let i = "profile: x\ntopic: t\nauth: { mode: none }\nvars: { project_id: proj, subscription_id: sub }\n";
    let q = QueueShape::new(shape(&fx, &p, i)).unwrap();
    let spec = q.units()[0].clone();
    let tick = TickCtx {
        window: None,
        connection_id: "conn",
        unit: &spec,
        checkpoint: None,
    };
    let mut stream = q.rows(tick);
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.unwrap());
    }
    drop(stream);
    let marks: Vec<Option<Mark>> = rows.iter().map(|r| r.mark.clone()).collect();
    assert_eq!(
        marks,
        [
            Some(Mark::Ack("ack-1".into())),
            Some(Mark::Ack("ack-2".into()))
        ]
    );
    let first: Value = serde_json::from_slice(&rows[0].payload).unwrap();
    assert_eq!(first["n"], 1);
    assert_eq!(
        first["_dfe_fetcher_pubsub"]["subscription"],
        "projects/proj/subscriptions/sub"
    );
    let pulls = fx.requests_to("/queue/pull");
    assert_eq!(pulls.len(), 2, "pulled until a pull was empty");
    assert_eq!(pulls[0].body.as_ref().unwrap()["maxMessages"], 10);
    assert!(
        fx.acked().is_empty(),
        "nothing acknowledged until the driver says so"
    );

    q.ack(&spec, vec!["ack-1".into(), "ack-2".into()])
        .await
        .unwrap();
    assert_eq!(
        fx.acked(),
        [vec!["ack-1".to_string()], vec!["ack-2".to_string()]],
        "one id per acknowledgement at ack_batch 1"
    );

    let not_a_queue = shape(
        &fx,
        &format!("{PLAIN}endpoints:\n  - unit: a\n    path: /array/items.json\n"),
        BEARER_INSTANCE,
    );
    assert!(QueueShape::new(not_a_queue).is_err());
}
