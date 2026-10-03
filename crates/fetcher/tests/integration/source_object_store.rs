// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_object_store.rs
// Purpose:   Characterisation of the object-store source: the signed S3 listing, each new object's records with the object envelope, the read-once checkpoint
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The object-store source (S3 backend) against wiremock.
//!
//! Each test configures the typed `sources.object_store` block, runs one
//! tick through the real pipeline into scalo's memory transport, and asserts
//! on the requests wiremock recorded (the SigV4-signed `ListObjectsV2`
//! under the bucket with `list-type`, `prefix` and `max-keys`, the
//! continuation token followed, each object fetched by its key in
//! `last_modified` order) and the records that landed (each object's
//! records framed per the prefix's format, every one carrying the
//! `_dfe_fetcher_object` envelope, tagged `object_store.<source_tag>` on
//! the prefix's topic, plus what enrichment added). The typed config block
//! is the operator's contract; the shipped `object_store` profile serves it
//! through the framework driver.

use std::io::Write as _;
use std::sync::Arc;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use dfe_fetcher::config::{
    Config, GcsBackendConfig, ObjectStoreBackendConfig, ObjectStoreBucket, ObjectStoreFormat,
    ObjectStorePrefix, ObjectStoreSourceConfig, S3BackendConfig,
};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};

const ACCESS_KEY: &str = "AKIATESTFETCHER";
const REGION: &str = "ap-southeast-2";
const BUCKET: &str = "audit-logs";

/// A deployment config carrying `object_store` as its one source, landing
/// on `<topic>_land`, no dead-letter queue. The broker is named so
/// `validate` reaches the source checks; nothing here connects to it.
fn config(object_store: ObjectStoreSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.object_store = object_store;
    config
}

fn prefix(prefix: &str, format: ObjectStoreFormat, tag: &str) -> ObjectStorePrefix {
    ObjectStorePrefix {
        prefix: prefix.into(),
        format,
        source_tag: tag.into(),
        topic: None,
    }
}

/// The typed block an operator writes: one S3 backend pointed at wiremock
/// by `endpoint_override`, the static key pair, one bucket with the given
/// prefixes.
fn s3_config(server: &MockServer, prefixes: Vec<ObjectStorePrefix>) -> ObjectStoreSourceConfig {
    ObjectStoreSourceConfig {
        enabled: true,
        backends: vec![ObjectStoreBackendConfig::S3(S3BackendConfig {
            region: REGION.into(),
            endpoint_override: Some(server.uri()),
            access_key_id: Some(ACCESS_KEY.into()),
            secret_access_key: Some("test-secret-key".to_string().into()),
            credential_secret: None,
            buckets: vec![ObjectStoreBucket {
                bucket: BUCKET.into(),
                prefixes,
            }],
        })],
        ..ObjectStoreSourceConfig::default()
    }
}

/// The one-prefix block most tests use: `logs/` as gzipped JSON lines
/// tagged `aws_cloudtrail`.
fn logs_config(server: &MockServer) -> ObjectStoreSourceConfig {
    s3_config(
        server,
        vec![prefix("logs/", ObjectStoreFormat::JsonGz, "aws_cloudtrail")],
    )
}

/// One tick of the `object_store` source as configured, through the
/// pipeline, with no checkpoint store.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "object_store", window)).await
}

/// One tick of the source committing its checkpoints to `store`.
async fn run_checkpointed(
    config: Config,
    store: Arc<dyn dfe_fetcher_core::CursorStore>,
) -> (Result<(), String>, Vec<Landed>) {
    let built = crate::builtin_run::built_instance(&config, "object_store").expect("maps");
    Box::pin(crate::builtin_run::run_checkpointed(
        config, &built, None, store,
    ))
    .await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "object_store")).await
}

/// A checkpoint store in a fresh temporary directory.
fn cursor_store() -> (tempfile::TempDir, Arc<dyn dfe_fetcher_core::CursorStore>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dfe_fetcher::cursor::file::FileCursorStore::new(dir.path().to_str().unwrap())
        .expect("cursor store");
    (dir, Arc::new(store))
}

/// `hours` ago, to the second.
fn ago(hours: i64) -> DateTime<Utc> {
    let at = Utc::now() - Duration::hours(hours);
    at.with_nanosecond_zero()
}

trait Truncate {
    fn with_nanosecond_zero(self) -> Self;
}

impl Truncate for DateTime<Utc> {
    fn with_nanosecond_zero(self) -> Self {
        DateTime::from_timestamp(self.timestamp(), 0).expect("in range")
    }
}

/// One `<Contents>` of a listing page.
fn contents(key: &str, modified: DateTime<Utc>, size: u64) -> String {
    format!(
        "<Contents><Key>{key}</Key><LastModified>{}</LastModified><ETag>\"x\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>",
        modified.to_rfc3339_opts(SecondsFormat::Millis, true)
    )
}

/// A `ListObjectsV2` page, with a continuation token when one follows.
fn listing_page(objects: &[String], next: Option<&str>) -> String {
    let token = next.map_or(String::new(), |t| {
        format!("<NextContinuationToken>{t}</NextContinuationToken>")
    });
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{BUCKET}</Name><IsTruncated>{}</IsTruncated>{token}{}</ListBucketResult>",
        next.is_some(),
        objects.join("")
    )
}

/// Mount the listing under the bucket, answering `page` for a request
/// carrying `token` (none for the first page).
async fn mount_listing(server: &MockServer, token: Option<&str>, page: String) {
    let mock = Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/")))
        .and(query_param("list-type", "2"));
    let mock = match token {
        Some(token) => mock.and(query_param("continuation-token", token)),
        None => mock,
    };
    mock.respond_with(
        ResponseTemplate::new(200)
            .set_body_string(page)
            .insert_header("Content-Type", "application/xml"),
    )
    .mount(server)
    .await;
}

/// Mount one object under the bucket.
async fn mount_object(server: &MockServer, key: &str, body: Vec<u8>) {
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/{key}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .mount(server)
        .await;
}

async fn mount_status(server: &MockServer, at: &str, status: u16) {
    Mock::given(method("GET"))
        .and(path(at))
        .respond_with(
            ResponseTemplate::new(status)
                .set_body_string("<Error><Code>Scripted</Code></Error>")
                .insert_header("Retry-After", "0"),
        )
        .mount(server)
        .await;
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(bytes).unwrap();
    enc.finish().unwrap()
}

async fn requests_to(server: &MockServer, at: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.url.path() == at)
        .collect()
}

/// The paths wiremock saw, in order.
async fn paths(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .map(|r| r.url.path().to_owned())
        .collect()
}

fn header_of<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request.headers.get(name).and_then(|v| v.to_str().ok())
}

fn query_value(request: &Request, name: &str) -> Option<String> {
    request
        .url
        .query_pairs()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
}

/// The request carries a SigV4 signature by the configured key for `s3`
/// in the configured region, with the empty payload's hash.
fn assert_signed(request: &Request) {
    let authorization = header_of(request, "authorization").expect("Authorization");
    assert!(
        authorization.starts_with(&format!("AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/")),
        "signed with the configured key: {authorization}"
    );
    assert!(
        authorization.contains(&format!("/{REGION}/s3/aws4_request, SignedHeaders=")),
        "the scope is s3 in the instance's region: {authorization}"
    );
    assert_eq!(
        header_of(request, "x-amz-content-sha256"),
        Some(hex::encode(Sha256::digest(&request.body)).as_str())
    );
    assert!(header_of(request, "x-amz-date").is_some());
}

/// The envelope every record of an object carries.
fn envelope(key: &str, modified: DateTime<Utc>, size: u64) -> Value {
    json!({
        "provider": "s3",
        "bucket": BUCKET,
        "key": key,
        "last_modified": modified.to_rfc3339(),
        "size": size,
    })
}

/// The landed records' `id` values, in landing order.
fn ids(rows: &[Landed]) -> Vec<String> {
    rows.iter()
        .map(|r| r.record["id"].as_str().unwrap_or_default().to_owned())
        .collect()
}

#[tokio::test]
async fn each_new_object_lands_as_its_records_with_the_object_envelope_oldest_first() {
    let server = MockServer::start().await;
    let (newer, older) = (ago(1), ago(2));
    mount_listing(
        &server,
        None,
        listing_page(
            &[
                contents("logs/2026/a.jsonl.gz", newer, 44),
                contents("logs/2026/b.jsonl.gz", older, 22),
            ],
            None,
        ),
    )
    .await;
    mount_object(
        &server,
        "logs/2026/a.jsonl.gz",
        gzip(b"{\"id\":\"a-1\"}\n{\"id\":\"a-2\"}\n"),
    )
    .await;
    mount_object(&server, "logs/2026/b.jsonl.gz", gzip(b"{\"id\":\"b-1\"}\n")).await;

    let (outcome, rows) = run(config(logs_config(&server)), None).await;
    outcome.expect("fetch");

    let listings = requests_to(&server, &format!("/{BUCKET}/")).await;
    assert_eq!(listings.len(), 1, "one listing page");
    assert_signed(&listings[0]);
    assert_eq!(query_value(&listings[0], "list-type").as_deref(), Some("2"));
    assert_eq!(
        query_value(&listings[0], "prefix").as_deref(),
        Some("logs/")
    );
    assert_eq!(
        query_value(&listings[0], "max-keys").as_deref(),
        Some("1000")
    );
    assert_eq!(
        paths(&server).await,
        [
            format!("/{BUCKET}/"),
            format!("/{BUCKET}/logs/2026/b.jsonl.gz"),
            format!("/{BUCKET}/logs/2026/a.jsonl.gz"),
        ],
        "the listing, then each object oldest first"
    );
    for object in requests_to(&server, &format!("/{BUCKET}/logs/2026/a.jsonl.gz")).await {
        assert_signed(&object);
    }

    assert_eq!(ids(&rows), ["b-1", "a-1", "a-2"]);
    for row in &rows {
        assert_eq!(row.topic, "object_store_land");
        let e = enriched(row);
        assert_eq!(e.source, "object_store");
        assert_eq!(e.source_fetcher, "object_store.aws_cloudtrail");
    }
    assert_eq!(
        enriched(&rows[0]).row,
        json!({"id": "b-1", "_dfe_fetcher_object": envelope("logs/2026/b.jsonl.gz", older, 22)})
    );
    assert_eq!(
        enriched(&rows[2]).row["_dfe_fetcher_object"],
        envelope("logs/2026/a.jsonl.gz", newer, 44)
    );
}

/// The listing's continuation token is followed until the last page, and
/// the objects of every page are ordered together by time.
#[tokio::test]
async fn the_listing_follows_the_continuation_token() {
    let server = MockServer::start().await;
    // The tokened page is mounted first: wiremock answers with the first
    // mock that matches, and the first-page mock matches every listing.
    mount_listing(
        &server,
        Some("page-2"),
        listing_page(&[contents("logs/b.jsonl", ago(2), 1)], None),
    )
    .await;
    mount_listing(
        &server,
        None,
        listing_page(&[contents("logs/a.jsonl", ago(1), 1)], Some("page-2")),
    )
    .await;
    mount_object(&server, "logs/a.jsonl", b"{\"id\":\"a\"}\n".to_vec()).await;
    mount_object(&server, "logs/b.jsonl", b"{\"id\":\"b\"}\n".to_vec()).await;
    let (outcome, rows) = run(
        config(s3_config(
            &server,
            vec![prefix("logs/", ObjectStoreFormat::Jsonl, "plain")],
        )),
        None,
    )
    .await;
    outcome.expect("fetch");
    let listings = requests_to(&server, &format!("/{BUCKET}/")).await;
    assert_eq!(listings.len(), 2);
    assert_eq!(
        query_value(&listings[1], "continuation-token").as_deref(),
        Some("page-2")
    );
    assert_eq!(
        query_value(&listings[1], "prefix").as_deref(),
        Some("logs/"),
        "the prefix rides along with the token"
    );
    assert_eq!(ids(&rows), ["b", "a"], "across pages, oldest first");
}

/// The first tick reads what was written in the last day: an object older
/// than that is listed but not read.
#[tokio::test]
async fn the_first_tick_reads_the_last_day_only() {
    let server = MockServer::start().await;
    mount_listing(
        &server,
        None,
        listing_page(
            &[
                contents("logs/old.jsonl", ago(25), 1),
                contents("logs/recent.jsonl", ago(23), 1),
            ],
            None,
        ),
    )
    .await;
    mount_object(&server, "logs/old.jsonl", b"{\"id\":\"old\"}\n".to_vec()).await;
    mount_object(
        &server,
        "logs/recent.jsonl",
        b"{\"id\":\"recent\"}\n".to_vec(),
    )
    .await;
    let (outcome, rows) = run(
        config(s3_config(
            &server,
            vec![prefix("logs/", ObjectStoreFormat::Jsonl, "plain")],
        )),
        None,
    )
    .await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["recent"]);
    assert!(
        requests_to(&server, &format!("/{BUCKET}/logs/old.jsonl"))
            .await
            .is_empty(),
        "the day-old object is not read"
    );
}

/// An object is read once: the newest `last_modified` a tick delivered is
/// the prefix's checkpoint, and the next tick lists from there, so a
/// listing that has not changed reads nothing and an object written since
/// is read without the ones before it.
#[tokio::test]
async fn objects_are_read_once_from_the_checkpoint() {
    let server = MockServer::start().await;
    let a_modified = ago(2);
    mount_listing(
        &server,
        None,
        listing_page(&[contents("logs/a.jsonl", a_modified, 1)], None),
    )
    .await;
    mount_object(&server, "logs/a.jsonl", b"{\"id\":\"a\"}\n".to_vec()).await;
    mount_object(&server, "logs/b.jsonl", b"{\"id\":\"b\"}\n".to_vec()).await;
    let cfg = config(s3_config(
        &server,
        vec![prefix("logs/", ObjectStoreFormat::Jsonl, "plain")],
    ));
    let (_dir, store) = cursor_store();
    let (first, rows) = run_checkpointed(cfg.clone(), Arc::clone(&store)).await;
    first.expect("first tick");
    assert_eq!(ids(&rows), ["a"]);
    let checkpoint = store
        .get("test.object_store.plain")
        .await
        .expect("read")
        .expect("a checkpoint per prefix once it lands data");
    assert!(
        matches!(
            checkpoint.checkpoint(),
            Some(dfe_fetcher_core::CheckpointValue::Item { ref key, position })
                if key == "logs/a.jsonl" && position == a_modified
        ),
        "{checkpoint:?}"
    );

    let (second, rows) = run_checkpointed(cfg.clone(), Arc::clone(&store)).await;
    second.expect("second tick");
    assert!(
        rows.is_empty(),
        "nothing newer than the checkpoint: {rows:?}"
    );
    assert_eq!(
        requests_to(&server, &format!("/{BUCKET}/logs/a.jsonl"))
            .await
            .len(),
        1,
        "the object read on the first tick is not read again"
    );

    server.reset().await;
    mount_listing(
        &server,
        None,
        listing_page(
            &[
                contents("logs/a.jsonl", a_modified, 1),
                contents("logs/b.jsonl", ago(1), 1),
            ],
            None,
        ),
    )
    .await;
    mount_object(&server, "logs/b.jsonl", b"{\"id\":\"b\"}\n".to_vec()).await;
    let (third, rows) = run_checkpointed(cfg, store).await;
    third.expect("third tick");
    assert_eq!(ids(&rows), ["b"], "only the object written since");
    assert!(
        requests_to(&server, &format!("/{BUCKET}/logs/a.jsonl"))
            .await
            .is_empty()
    );
}

/// `LastModified` is second-resolution, so the checkpoint is a position AND a
/// key: an object written in the same second as the committed one is read on
/// the next tick rather than skipped for ever, and the committed one is not
/// read twice.
#[tokio::test]
async fn an_object_written_in_the_committed_second_is_read_and_the_committed_one_is_not() {
    let server = MockServer::start().await;
    let same = ago(2);
    mount_listing(
        &server,
        None,
        listing_page(&[contents("logs/a.jsonl", same, 1)], None),
    )
    .await;
    mount_object(&server, "logs/a.jsonl", b"{\"id\":\"a\"}\n".to_vec()).await;
    let cfg = config(s3_config(
        &server,
        vec![prefix("logs/", ObjectStoreFormat::Jsonl, "plain")],
    ));
    let (_dir, store) = cursor_store();

    let (first, rows) = run_checkpointed(cfg.clone(), Arc::clone(&store)).await;
    first.expect("first tick");
    assert_eq!(ids(&rows), ["a"]);

    // `b` is written in the same second as `a` and listed after it.
    server.reset().await;
    mount_listing(
        &server,
        None,
        listing_page(
            &[
                contents("logs/a.jsonl", same, 1),
                contents("logs/b.jsonl", same, 1),
            ],
            None,
        ),
    )
    .await;
    mount_object(&server, "logs/b.jsonl", b"{\"id\":\"b\"}\n".to_vec()).await;
    let (second, rows) = run_checkpointed(cfg.clone(), Arc::clone(&store)).await;
    second.expect("second tick");
    assert_eq!(
        ids(&rows),
        ["b"],
        "the object sharing the committed second is read"
    );
    assert!(
        requests_to(&server, &format!("/{BUCKET}/logs/a.jsonl"))
            .await
            .is_empty(),
        "the committed object is not read again"
    );

    let (third, rows) = run_checkpointed(cfg, store).await;
    third.expect("third tick");
    assert!(rows.is_empty(), "both objects are done: {rows:?}");
}

/// A gzipped object written as several members (a shipper that appends)
/// inflates whole: every member's records land.
#[tokio::test]
async fn a_multi_member_gzip_object_is_read_whole() {
    let server = MockServer::start().await;
    mount_listing(
        &server,
        None,
        listing_page(&[contents("logs/two.jsonl.gz", ago(1), 1)], None),
    )
    .await;
    let mut body = gzip(b"{\"id\":\"m1\"}\n");
    body.extend(gzip(b"{\"id\":\"m2\"}\n"));
    mount_object(&server, "logs/two.jsonl.gz", body).await;
    let (outcome, rows) = run(config(logs_config(&server)), None).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["m1", "m2"]);
}

/// Every format the block accepts: gzipped JSON lines, plain JSON lines,
/// a JSON document (an array is one record per element, an object one
/// record), plain text and gzipped text (each line under `line`).
#[tokio::test]
async fn each_format_frames_its_objects_records() {
    let server = MockServer::start().await;
    let cases: Vec<(&str, ObjectStoreFormat, Vec<u8>)> = vec![
        ("gz/", ObjectStoreFormat::JsonGz, gzip(b"{\"id\":\"gz\"}\n")),
        (
            "jsonl/",
            ObjectStoreFormat::Jsonl,
            b"{\"id\":\"jl\"}\n".to_vec(),
        ),
        (
            "array/",
            ObjectStoreFormat::Json,
            b"[{\"id\":\"j1\"},{\"id\":\"j2\"}]".to_vec(),
        ),
        (
            "object/",
            ObjectStoreFormat::Json,
            b"{\"id\":\"jo\"}".to_vec(),
        ),
        (
            "text/",
            ObjectStoreFormat::Text,
            b"first line\n\nsecond line\n".to_vec(),
        ),
        ("textgz/", ObjectStoreFormat::TextGz, gzip(b"zipped line\n")),
    ];
    let mut prefixes = Vec::new();
    for (pre, format, body) in &cases {
        let key = format!("{pre}object");
        let tag = format!("{}_objects", pre.trim_end_matches('/'));
        Mock::given(method("GET"))
            .and(path(format!("/{BUCKET}/")))
            .and(query_param("prefix", *pre))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(listing_page(&[contents(&key, ago(1), 1)], None)),
            )
            .mount(&server)
            .await;
        mount_object(&server, &key, body.clone()).await;
        prefixes.push(prefix(pre, *format, &tag));
    }
    let (outcome, rows) = run(config(s3_config(&server, prefixes)), None).await;
    outcome.expect("fetch");
    // The prefixes are units and run in name order; each prefix's records
    // keep their order, so the comparison sorts by tag.
    let mut by_tag: Vec<(String, Value)> = rows
        .iter()
        .map(|r| {
            let e = enriched(r);
            let mut row = e.row.clone();
            row.as_object_mut().unwrap().remove("_dfe_fetcher_object");
            (e.source_fetcher, row)
        })
        .collect();
    by_tag.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        by_tag,
        [
            (
                "object_store.array_objects".to_string(),
                json!({"id": "j1"})
            ),
            (
                "object_store.array_objects".to_string(),
                json!({"id": "j2"})
            ),
            ("object_store.gz_objects".to_string(), json!({"id": "gz"})),
            (
                "object_store.jsonl_objects".to_string(),
                json!({"id": "jl"})
            ),
            (
                "object_store.object_objects".to_string(),
                json!({"id": "jo"})
            ),
            (
                "object_store.text_objects".to_string(),
                json!({"line": "first line"})
            ),
            (
                "object_store.text_objects".to_string(),
                json!({"line": "second line"})
            ),
            (
                "object_store.textgz_objects".to_string(),
                json!({"line": "zipped line"})
            ),
        ]
    );
    for row in &rows {
        assert!(
            row.record["_dfe_fetcher_object"]["key"].is_string(),
            "every record carries the envelope: {row:?}"
        );
    }
}

/// A JSON-lines line that is not JSON lands as a raw-line record with the
/// parse error, and a JSON record that is not an object lands under
/// `payload`; both carry the envelope, so nothing is lost silently.
#[tokio::test]
async fn a_bad_line_and_a_non_object_record_are_wrapped_not_dropped() {
    let server = MockServer::start().await;
    mount_listing(
        &server,
        None,
        listing_page(&[contents("logs/mixed.jsonl", ago(1), 1)], None),
    )
    .await;
    mount_object(
        &server,
        "logs/mixed.jsonl",
        b"{\"id\":\"ok\"}\nNOT JSON\n[1, 2]\n".to_vec(),
    )
    .await;
    let (outcome, rows) = run(
        config(s3_config(
            &server,
            vec![prefix("logs/", ObjectStoreFormat::Jsonl, "plain")],
        )),
        None,
    )
    .await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].record["id"], "ok");
    assert_eq!(rows[1].record["_dfe_fetcher_raw_line"], "NOT JSON");
    assert!(rows[1].record["_dfe_fetcher_parse_error"].is_string());
    assert_eq!(rows[2].record["payload"], json!([1, 2]));
    for row in &rows {
        assert_eq!(row.record["_dfe_fetcher_object"]["key"], "logs/mixed.jsonl");
    }
}

/// A prefix's own topic routes its records; two prefixes are two tags.
#[tokio::test]
async fn a_prefix_topic_override_routes_its_records_under_its_own_tag() {
    let server = MockServer::start().await;
    for pre in ["one/", "two/"] {
        Mock::given(method("GET"))
            .and(path(format!("/{BUCKET}/")))
            .and(query_param("prefix", pre))
            .respond_with(ResponseTemplate::new(200).set_body_string(listing_page(
                &[contents(&format!("{pre}o.jsonl"), ago(1), 1)],
                None,
            )))
            .mount(&server)
            .await;
        mount_object(
            &server,
            &format!("{pre}o.jsonl"),
            format!("{{\"id\":\"{pre}\"}}\n").into_bytes(),
        )
        .await;
    }
    let mut routed = prefix("two/", ObjectStoreFormat::Jsonl, "second");
    routed.topic = Some("elsewhere".into());
    let (outcome, rows) = run(
        config(s3_config(
            &server,
            vec![prefix("one/", ObjectStoreFormat::Jsonl, "first"), routed],
        )),
        None,
    )
    .await;
    outcome.expect("fetch");
    let landed: Vec<(String, String, String)> = rows
        .iter()
        .map(|r| {
            let e = enriched(r);
            (r.topic.clone(), e.source, e.source_fetcher)
        })
        .collect();
    assert_eq!(
        landed,
        [
            (
                "object_store_land".to_string(),
                "object_store".to_string(),
                "object_store.first".to_string()
            ),
            (
                "elsewhere_land".to_string(),
                "elsewhere".to_string(),
                "object_store.second".to_string()
            ),
        ]
    );
}

/// A GCS backend is not implemented: it is skipped with a warning and the
/// S3 backend beside it still runs.
#[tokio::test]
async fn an_unimplemented_backend_is_skipped_and_the_s3_backend_still_runs() {
    let server = MockServer::start().await;
    mount_listing(
        &server,
        None,
        listing_page(&[contents("logs/a.jsonl", ago(1), 1)], None),
    )
    .await;
    mount_object(&server, "logs/a.jsonl", b"{\"id\":\"a\"}\n".to_vec()).await;
    let mut cfg = s3_config(
        &server,
        vec![prefix("logs/", ObjectStoreFormat::Jsonl, "plain")],
    );
    cfg.backends.insert(
        0,
        ObjectStoreBackendConfig::Gcs(GcsBackendConfig {
            service_account_key: None,
            credential_secret: None,
            buckets: vec![ObjectStoreBucket {
                bucket: "gcs-bucket".into(),
                prefixes: vec![prefix("x/", ObjectStoreFormat::Jsonl, "gcs")],
            }],
        }),
    );
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("an unimplemented backend is not a failure");
    assert_eq!(ids(&rows), ["a"]);
}

/// An object the store keeps failing to serve is retried per the policy
/// and then fails the prefix's tick: the checkpoint does not move past it,
/// so the next tick asks for it again and nothing is lost.
#[tokio::test]
async fn a_failing_object_is_retried_and_then_fails_the_tick() {
    let server = MockServer::start().await;
    mount_listing(
        &server,
        None,
        listing_page(
            &[
                contents("logs/broken.jsonl", ago(2), 1),
                contents("logs/fine.jsonl", ago(1), 1),
            ],
            None,
        ),
    )
    .await;
    mount_status(&server, &format!("/{BUCKET}/logs/broken.jsonl"), 500).await;
    mount_object(&server, "logs/fine.jsonl", b"{\"id\":\"fine\"}\n".to_vec()).await;
    let (_dir, store) = cursor_store();
    let (outcome, rows) = run_checkpointed(
        config(s3_config(
            &server,
            vec![prefix("logs/", ObjectStoreFormat::Jsonl, "plain")],
        )),
        Arc::clone(&store),
    )
    .await;
    let err = outcome.expect_err("the tick reports the failure");
    assert!(err.contains("500"), "{err}");
    assert!(rows.is_empty(), "{rows:?}");
    assert_eq!(
        requests_to(&server, &format!("/{BUCKET}/logs/broken.jsonl"))
            .await
            .len(),
        4,
        "the first attempt and three retries"
    );
    assert!(
        requests_to(&server, &format!("/{BUCKET}/logs/fine.jsonl"))
            .await
            .is_empty(),
        "the objects after the failure wait for the next tick"
    );
    assert!(
        store
            .get("test.object_store.plain")
            .await
            .expect("read")
            .is_none(),
        "no checkpoint moved"
    );
}

/// A listing the store refuses (403) fails the prefix's tick with the
/// store's text and is never retried.
#[tokio::test]
async fn a_refused_listing_fails_the_prefix() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/")))
        .respond_with(ResponseTemplate::new(403).set_body_string(
            "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>",
        ))
        .mount(&server)
        .await;
    let (outcome, rows) = run(config(logs_config(&server)), None).await;
    let err = outcome.expect_err("a refusal is reported");
    assert!(err.contains("AccessDenied"), "{err}");
    assert!(rows.is_empty());
    assert_eq!(
        requests_to(&server, &format!("/{BUCKET}/")).await.len(),
        1,
        "never retried"
    );
}

/// `credential_secret` resolves to a JSON document carrying both halves of
/// the key, and signs the listing with its key id.
#[tokio::test]
async fn a_credential_secret_document_supplies_both_keys() {
    let server = MockServer::start().await;
    mount_listing(&server, None, listing_page(&[], None)).await;
    let mut cfg = logs_config(&server);
    let ObjectStoreBackendConfig::S3(s3) = &mut cfg.backends[0] else {
        panic!("s3")
    };
    s3.access_key_id = None;
    s3.secret_access_key = None;
    s3.credential_secret = Some(
        json!({"access_key_id": ACCESS_KEY, "secret_access_key": "from-the-document"}).to_string(),
    );
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert!(rows.is_empty(), "an empty prefix lands nothing");
    let listings = requests_to(&server, &format!("/{BUCKET}/")).await;
    assert_eq!(listings.len(), 1);
    assert_signed(&listings[0]);
}

#[tokio::test]
async fn the_filter_drops_records_before_they_land() {
    let server = MockServer::start().await;
    mount_listing(
        &server,
        None,
        listing_page(&[contents("logs/a.jsonl", ago(1), 1)], None),
    )
    .await;
    mount_object(
        &server,
        "logs/a.jsonl",
        b"{\"id\":\"keep\",\"level\":\"ERROR\"}\n{\"id\":\"drop\",\"level\":\"INFO\"}\n".to_vec(),
    )
    .await;
    let mut cfg = s3_config(
        &server,
        vec![prefix("logs/", ObjectStoreFormat::Jsonl, "plain")],
    );
    cfg.filter = Some("level == \"ERROR\"".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(ids(&rows), ["keep"]);
}

/// A block with no S3 backend maps onto nothing to schedule; the health
/// check resolves the S3 credentials without a request.
#[tokio::test]
async fn no_backends_requests_nothing_and_health_resolves_the_credentials() {
    let server = MockServer::start().await;
    let mut cfg = logs_config(&server);
    cfg.backends.clear();
    assert!(
        config(cfg)
            .sources
            .builtin_instances()
            .expect("maps")
            .is_empty(),
        "no S3 backend, no instance"
    );
    assert_eq!(paths(&server).await, [] as [std::string::String; 0]);

    let healthy = health(config(logs_config(&server))).await.expect("health");
    assert!(healthy);
    assert!(
        paths(&server).await.is_empty(),
        "the health check resolves the key pair and sends nothing"
    );
}
