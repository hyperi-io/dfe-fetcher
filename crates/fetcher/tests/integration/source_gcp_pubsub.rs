// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/source_gcp_pubsub.rs
// Purpose:   Characterisation of the GCP Pub/Sub pull source: the pull, the decoded message with its envelope, when the ack goes out
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The GCP Pub/Sub pull source against wiremock.
//!
//! Each test configures the typed `sources.gcp_pubsub` block, runs one tick
//! through the real pipeline into scalo's memory transport, and asserts on
//! the requests wiremock recorded (the JWT-bearer exchange for the Pub/Sub
//! scope, the `:pull` per subscription with `maxMessages` and
//! `returnImmediately`, the `:acknowledge` with the pulled ack ids) and the
//! records that landed (each message's base64 `data` decoded, the message
//! envelope under `_dfe_fetcher_pubsub`, plus what enrichment added). The
//! typed config block is the operator's contract; the shipped `gcp_pubsub`
//! profile serves it through the framework driver.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use dfe_fetcher::config::{Config, GcpPubsubSourceConfig, GcpPubsubSubscription};
use dfe_fetcher_core::FetchWindow;

use crate::builtin_run::{Landed, enriched};
use crate::common::{rsa_key_pair, service_account_key};

const TOKEN_PATH: &str = "/token";
const CLIENT_EMAIL: &str = "fetcher@test-project.iam.gserviceaccount.com";
const PUBSUB_SCOPE: &str = "https://www.googleapis.com/auth/pubsub";
const PROJECT: &str = "test-project";

/// A deployment config carrying `gcp_pubsub` as its one source, landing on
/// `<topic>_land`, no dead-letter queue. The broker is named so `validate`
/// reaches the source checks; nothing here connects to it.
fn config(gcp_pubsub: GcpPubsubSourceConfig) -> Config {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".into()];
    config.output.topic_suffix = Some("_land".into());
    config.dlq.enabled = false;
    config.sources.gcp_pubsub = gcp_pubsub;
    config
}

fn subscription(id: &str, max_messages: u32) -> GcpPubsubSubscription {
    GcpPubsubSubscription {
        project_id: PROJECT.into(),
        subscription_id: id.into(),
        max_messages,
        return_immediately: true,
    }
}

/// The typed block an operator writes: the service-account key file, the
/// API and token endpoints pointed at wiremock, the subscriptions.
fn pubsub_config(
    server: &MockServer,
    account: &ServiceAccount,
    subscriptions: Vec<GcpPubsubSubscription>,
) -> GcpPubsubSourceConfig {
    GcpPubsubSourceConfig {
        enabled: true,
        service_account_key: Some(account.key_path.clone()),
        api_url_override: Some(server.uri()),
        token_url_override: Some(format!("{}{TOKEN_PATH}", server.uri())),
        subscriptions,
        ..GcpPubsubSourceConfig::default()
    }
}

/// A service-account key on disk and the mock token endpoint that verifies
/// the assertions signed with it, recording their claims.
struct ServiceAccount {
    _dir: tempfile::TempDir,
    key_path: String,
    claims: Arc<Mutex<Vec<Value>>>,
}

struct JwtExchange {
    public_pem: String,
    claims: Arc<Mutex<Vec<Value>>>,
}

impl Respond for JwtExchange {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let form = form_of(request);
        let refused = ResponseTemplate::new(401).set_body_json(json!({"error": "invalid_grant"}));
        if form.get("grant_type").map(String::as_str)
            != Some("urn:ietf:params:oauth:grant-type:jwt-bearer")
        {
            return refused;
        }
        let Some(assertion) = form.get("assertion") else {
            return refused;
        };
        let key = jsonwebtoken::DecodingKey::from_rsa_pem(self.public_pem.as_bytes())
            .expect("public key");
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
        validation.validate_aud = false;
        validation.set_required_spec_claims(&["exp"]);
        match jsonwebtoken::decode::<Value>(assertion, &key, &validation) {
            Ok(data) => {
                self.claims.lock().unwrap().push(data.claims);
                ResponseTemplate::new(200).set_body_json(json!({
                    "access_token": "pubsub-token",
                    "expires_in": 3599,
                    "token_type": "Bearer"
                }))
            }
            Err(_) => refused,
        }
    }
}

/// Mount the exchange and write the key whose `token_uri` names it.
async fn service_account(server: &MockServer) -> ServiceAccount {
    let (private_pem, public_pem) = rsa_key_pair();
    let claims = Arc::new(Mutex::new(Vec::new()));
    Mock::given(method("POST"))
        .and(path(TOKEN_PATH))
        .respond_with(JwtExchange {
            public_pem,
            claims: Arc::clone(&claims),
        })
        .mount(server)
        .await;
    let dir = tempfile::tempdir().expect("tempdir");
    let key_path = dir.path().join("sa-key.json");
    std::fs::write(
        &key_path,
        service_account_key(
            &private_pem,
            CLIENT_EMAIL,
            &format!("{}{TOKEN_PATH}", server.uri()),
        ),
    )
    .expect("write key");
    ServiceAccount {
        key_path: key_path.to_string_lossy().into_owned(),
        _dir: dir,
        claims,
    }
}

fn pull_path(sub: &str) -> String {
    format!("/v1/projects/{PROJECT}/subscriptions/{sub}:pull")
}

fn ack_path(sub: &str) -> String {
    format!("/v1/projects/{PROJECT}/subscriptions/{sub}:acknowledge")
}

/// One received message: the ack id, the base64 data, the message id.
fn received(n: u32, data: &str) -> Value {
    json!({
        "ackId": format!("ack-{n}"),
        "message": {
            "data": base64::engine::general_purpose::STANDARD.encode(data),
            "messageId": format!("m-{n}"),
            "publishTime": "2026-05-21T10:00:00.000Z",
            "attributes": {"logging.googleapis.com/timestamp": "2026-05-21T09:59:59Z"}
        }
    })
}

/// The pull answers `messages` on every request; the ack answers `{}`.
async fn mount_subscription(server: &MockServer, sub: &str, messages: Vec<Value>) {
    Mock::given(method("POST"))
        .and(path(pull_path(sub)))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"receivedMessages": messages})),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(ack_path(sub)))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(server)
        .await;
}

/// One tick of the `gcp_pubsub` source as configured, through the pipeline.
async fn run(config: Config, window: Option<&FetchWindow>) -> (Result<(), String>, Vec<Landed>) {
    Box::pin(crate::builtin_run::run(config, "gcp_pubsub", window)).await
}

/// The source's health check as configured.
async fn health(config: Config) -> Result<bool, String> {
    Box::pin(crate::builtin_run::health(config, "gcp_pubsub")).await
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

fn body_of(request: &Request) -> Value {
    serde_json::from_slice(&request.body).unwrap_or(Value::Null)
}

/// The form fields of a token exchange.
fn form_of(request: &Request) -> HashMap<String, String> {
    url::form_urlencoded::parse(&request.body)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

/// The ack ids of every acknowledgement sent for `sub`, in request order.
async fn acked(server: &MockServer, sub: &str) -> Vec<Vec<String>> {
    requests_to(server, &ack_path(sub))
        .await
        .iter()
        .map(|r| {
            body_of(r)["ackIds"]
                .as_array()
                .map(|ids| {
                    ids.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default()
        })
        .collect()
}

#[tokio::test]
async fn each_subscription_is_pulled_and_its_messages_land_with_the_envelope() {
    let server = MockServer::start().await;
    let account = service_account(&server).await;
    mount_subscription(
        &server,
        "audit-sub",
        vec![
            received(1, r#"{"severity":"NOTICE","n":1}"#),
            received(2, r#"{"severity":"ERROR","n":2}"#),
        ],
    )
    .await;
    mount_subscription(&server, "flow-sub", vec![received(3, "plain text")]).await;

    let (outcome, rows) = run(
        config(pubsub_config(
            &server,
            &account,
            vec![subscription("audit-sub", 500), subscription("flow-sub", 10)],
        )),
        None,
    )
    .await;
    outcome.expect("fetch");

    let claims = account.claims.lock().unwrap().clone();
    assert_eq!(claims.len(), 1, "one exchange for the tick");
    assert_eq!(claims[0]["iss"], CLIENT_EMAIL);
    assert_eq!(claims[0]["scope"], PUBSUB_SCOPE);
    assert_eq!(claims[0]["aud"], format!("{}{TOKEN_PATH}", server.uri()));
    assert!(claims[0].get("sub").is_none(), "no impersonation");
    assert_eq!(
        claims[0]["exp"].as_i64().unwrap() - claims[0]["iat"].as_i64().unwrap(),
        3600
    );

    let pulls = requests_to(&server, &pull_path("audit-sub")).await;
    assert_eq!(pulls.len(), 1);
    assert_eq!(
        header_of(&pulls[0], "authorization"),
        Some("Bearer pubsub-token")
    );
    assert_eq!(
        body_of(&pulls[0]),
        json!({"maxMessages": 500, "returnImmediately": true})
    );
    assert_eq!(
        body_of(&requests_to(&server, &pull_path("flow-sub")).await[0])["maxMessages"],
        10
    );

    assert_eq!(rows.len(), 3);
    let first = enriched(&rows[0]);
    assert_eq!(
        first.row,
        json!({
            "severity": "NOTICE",
            "n": 1,
            "_dfe_fetcher_pubsub": {
                "subscription": format!("projects/{PROJECT}/subscriptions/audit-sub"),
                "message_id": "m-1",
                "publish_time": "2026-05-21T10:00:00.000Z",
                "attributes": {"logging.googleapis.com/timestamp": "2026-05-21T09:59:59Z"},
                "ordering_key": null
            }
        })
    );
    assert_eq!(rows[0].topic, "gcp_pubsub_land");
    assert_eq!(first.source, "gcp_pubsub");
    assert_eq!(first.source_fetcher, "gcp_pubsub.audit-sub");
    let text = enriched(&rows[2]);
    assert_eq!(text.row["data"], "plain text", "non-JSON data is wrapped");
    assert_eq!(text.source_fetcher, "gcp_pubsub.flow-sub");
    assert_eq!(
        acked(&server, "audit-sub").await,
        [vec!["ack-1".to_string(), "ack-2".to_string()]],
        "the pulled ids acknowledged in one request"
    );
    assert_eq!(
        acked(&server, "flow-sub").await,
        [vec!["ack-3".to_string()]]
    );
}

/// The scheduler's window plays no part: a pull takes what the
/// subscription holds.
#[tokio::test]
async fn the_window_does_not_reach_the_request() {
    let server = MockServer::start().await;
    let account = service_account(&server).await;
    mount_subscription(&server, "audit-sub", vec![received(1, "{}")]).await;
    let window = FetchWindow {
        start: chrono::Utc::now() - chrono::Duration::hours(2),
        end: chrono::Utc::now(),
    };
    let (outcome, rows) = run(
        config(pubsub_config(
            &server,
            &account,
            vec![subscription("audit-sub", 1000)],
        )),
        Some(&window),
    )
    .await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    let pulls = requests_to(&server, &pull_path("audit-sub")).await;
    assert_eq!(
        body_of(&pulls[0]),
        json!({"maxMessages": 1000, "returnImmediately": true})
    );
}

/// The messages are acknowledged AFTER they are delivered: a pull whose
/// records the transport never took is not acknowledged, so the broker
/// redelivers them instead of losing them.
#[tokio::test]
async fn messages_are_acknowledged_only_after_delivery() {
    let server = MockServer::start().await;
    let account = service_account(&server).await;
    mount_subscription(
        &server,
        "audit-sub",
        vec![received(1, "{}"), received(2, "{}")],
    )
    .await;
    let cfg = config(pubsub_config(
        &server,
        &account,
        vec![subscription("audit-sub", 1000)],
    ));
    let built = crate::builtin_run::built_instance(&cfg, "gcp_pubsub").expect("maps");
    let err = Box::pin(crate::builtin_run::run_without_output(cfg, &built, None))
        .await
        .expect_err("with no transport the delivery fails");
    assert!(err.contains("Output transport not configured"), "{err}");
    assert_eq!(
        requests_to(&server, &pull_path("audit-sub")).await.len(),
        1,
        "the messages were pulled"
    );
    assert!(
        requests_to(&server, &ack_path("audit-sub"))
            .await
            .is_empty(),
        "and not acknowledged, since nothing was delivered"
    );
}

/// An empty subscription: the pull answers no messages, nothing lands,
/// nothing is acknowledged, the tick is Ok.
#[tokio::test]
async fn an_empty_subscription_lands_nothing_and_acks_nothing() {
    let server = MockServer::start().await;
    let account = service_account(&server).await;
    Mock::given(method("POST"))
        .and(path(pull_path("quiet")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    let (outcome, rows) = run(
        config(pubsub_config(
            &server,
            &account,
            vec![subscription("quiet", 1000)],
        )),
        None,
    )
    .await;
    outcome.expect("fetch");
    assert!(rows.is_empty());
    assert!(requests_to(&server, &ack_path("quiet")).await.is_empty());
}

/// A pull the API keeps refusing is retried per the policy and then fails
/// that subscription's tick; the other subscriptions still run and land.
#[tokio::test]
async fn a_failing_pull_fails_its_unit_and_the_others_still_land() {
    let server = MockServer::start().await;
    let account = service_account(&server).await;
    Mock::given(method("POST"))
        .and(path(pull_path("broken")))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_json(json!({"error": {"message": "backend error"}}))
                .insert_header("Retry-After", "0"),
        )
        .mount(&server)
        .await;
    mount_subscription(&server, "fine", vec![received(1, "{}")]).await;
    let (outcome, rows) = run(
        config(pubsub_config(
            &server,
            &account,
            vec![subscription("broken", 1000), subscription("fine", 1000)],
        )),
        None,
    )
    .await;
    let err = outcome.expect_err("the failed unit is reported");
    assert!(err.contains("backend error"), "{err}");
    assert_eq!(rows.len(), 1, "the other unit's records landed");
    assert_eq!(enriched(&rows[0]).source_fetcher, "gcp_pubsub.fine");
    assert_eq!(
        requests_to(&server, &pull_path("broken")).await.len(),
        4,
        "the first attempt and three retries"
    );
    assert_eq!(acked(&server, "fine").await, [vec!["ack-1".to_string()]]);
}

/// A refused token exchange fails the tick with no pull sent.
#[tokio::test]
async fn a_refused_token_exchange_fails_the_tick_and_pulls_nothing() {
    let server = MockServer::start().await;
    let account = service_account(&server).await;
    mount_subscription(&server, "audit-sub", vec![received(1, "{}")]).await;
    let (other_private, _) = rsa_key_pair();
    let dir = tempfile::tempdir().expect("tempdir");
    let wrong = dir.path().join("wrong.json");
    std::fs::write(
        &wrong,
        service_account_key(
            &other_private,
            CLIENT_EMAIL,
            &format!("{}{TOKEN_PATH}", server.uri()),
        ),
    )
    .expect("write key");
    let mut cfg = pubsub_config(&server, &account, vec![subscription("audit-sub", 1000)]);
    cfg.service_account_key = Some(wrong.to_string_lossy().into_owned());
    let (outcome, rows) = run(config(cfg), None).await;
    let err = outcome.expect_err("the refusal is reported");
    assert!(
        err.contains("401") || err.contains("invalid_grant"),
        "{err}"
    );
    assert!(rows.is_empty());
    assert!(
        requests_to(&server, &pull_path("audit-sub"))
            .await
            .is_empty()
    );
}

/// Acks go out in chunks of at most 500 ids.
#[tokio::test]
async fn acknowledgements_are_sent_500_ids_at_a_time() {
    let server = MockServer::start().await;
    let account = service_account(&server).await;
    let messages: Vec<Value> = (1..=1001).map(|n| received(n, "{}")).collect();
    mount_subscription(&server, "busy", messages).await;
    let (outcome, rows) = run(
        config(pubsub_config(
            &server,
            &account,
            vec![subscription("busy", 1001)],
        )),
        None,
    )
    .await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1001);
    let batches = acked(&server, "busy").await;
    let sizes: Vec<usize> = batches.iter().map(Vec::len).collect();
    assert_eq!(sizes, [500, 500, 1]);
    assert_eq!(batches[0][0], "ack-1");
    assert_eq!(batches[2][0], "ack-1001");
}

/// A filtered record never lands, and the message that carried it is still
/// acknowledged: a dropped record is CONSUMED, not undelivered. Without the
/// ack the broker redelivers it after the deadline, the fetcher drops it
/// again, and once the stuck set fills a pull the subscription starves.
#[tokio::test]
async fn a_filtered_message_does_not_land_and_is_still_acknowledged() {
    let server = MockServer::start().await;
    let account = service_account(&server).await;
    mount_subscription(
        &server,
        "audit-sub",
        vec![
            received(1, r#"{"severity":"NOTICE"}"#),
            received(2, r#"{"severity":"ERROR"}"#),
        ],
    )
    .await;
    let mut cfg = pubsub_config(&server, &account, vec![subscription("audit-sub", 1000)]);
    cfg.filter = Some("severity == \"ERROR\"".into());
    let (outcome, rows) = run(config(cfg), None).await;
    outcome.expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].record["severity"], "ERROR");
    assert_eq!(
        acked(&server, "audit-sub").await,
        [vec!["ack-1".to_string(), "ack-2".to_string()]],
        "the filtered message is acknowledged beside the delivered one"
    );
}

/// No subscriptions: nothing is exchanged or pulled and the tick is Ok.
#[tokio::test]
async fn no_subscriptions_requests_nothing() {
    let server = MockServer::start().await;
    let account = service_account(&server).await;
    let (outcome, rows) = run(config(pubsub_config(&server, &account, vec![])), None).await;
    outcome.expect("nothing to do is not a failure");
    assert!(rows.is_empty());
    assert!(paths(&server).await.is_empty());
}

/// The health check is the token exchange for the Pub/Sub scope.
#[tokio::test]
async fn the_health_check_is_the_token_exchange() {
    let server = MockServer::start().await;
    let account = service_account(&server).await;
    let healthy = health(config(pubsub_config(
        &server,
        &account,
        vec![subscription("audit-sub", 1000)],
    )))
    .await
    .expect("health");
    assert!(healthy);
    assert_eq!(paths(&server).await, [TOKEN_PATH]);
    assert_eq!(account.claims.lock().unwrap()[0]["scope"], PUBSUB_SCOPE);
}
