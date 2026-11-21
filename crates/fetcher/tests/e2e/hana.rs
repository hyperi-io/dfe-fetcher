// Project:   dfe-fetcher
// File:      crates/fetcher/tests/e2e/hana.rs
// Purpose:   The SAP HANA engine against a live system: password login, TLS, a resumable keyset tail, SYS.AUDIT_LOG
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The `hana` engine against a real HANA system.
//!
//! Two targets, the same tests. SAP HANA Cloud is reached over TLS with a
//! password: `SAP_HANA_HOST`, `SAP_HANA_PORT`, `SAP_HANA_DB_ADMIN_USERNAME`
//! and `SAP_HANA_DB_ADMIN_PASSWORD` build an `hdbsqls://` URL that trusts the
//! public roots. A HANA Express container is reached by `HANA_TEST_URL`, a full
//! `hdbsql://` or `hdbsqls://` URL. The tests are `#[ignore]`d because no runner
//! holds either, and a missing variable fails the test rather than skipping it.
//!
//! ```bash
//! hyperi-ai secrets run -- hyperi-ai remote run \
//!   --env SAP_HANA_HOST --env SAP_HANA_PORT \
//!   --env SAP_HANA_DB_ADMIN_USERNAME --env SAP_HANA_DB_ADMIN_PASSWORD -- \
//!   cargo nextest run -p dfe-fetcher --features db-hana --test e2e --run-ignored only hana::
//! ```
//!
//! The tests create one table of their own in the login's schema and drop it
//! when they finish. They read `SYS.AUDIT_LOG` and write nothing to it.

use crate::common::{optional, require};

use std::sync::Arc;

use futures::StreamExt;
use scalo::SensitiveString;
use serde_json::{Value, json};

use dfe_fetcher_core::batch::{Lease, NoLease};
use dfe_fetcher_core::error::Error;
use dfe_fetcher_core::{Mark, Row};
use dfe_fetcher_db::hana::HanaStore;
use dfe_fetcher_db::secret::Secret;
use dfe_fetcher_db::{BatchSpec, Store};

/// The connection URL of the target, with `password` in place of the login's
/// own when one is given.
fn connection_string(password: Option<&str>) -> String {
    if let Some(given) = optional("HANA_TEST_URL") {
        let Some(password) = password else {
            return given;
        };
        let mut url = url::Url::parse(&given).expect("HANA_TEST_URL is a URL");
        url.set_password(Some(password)).expect("a password");
        return url.to_string();
    }
    let mut url = url::Url::parse(&format!(
        "hdbsqls://{}:{}",
        require("SAP_HANA_HOST"),
        require("SAP_HANA_PORT")
    ))
    .expect("the host and port form a URL");
    url.set_username(&require("SAP_HANA_DB_ADMIN_USERNAME"))
        .expect("a username");
    url.set_password(Some(
        password
            .map_or_else(|| require("SAP_HANA_DB_ADMIN_PASSWORD"), str::to_owned)
            .as_str(),
    ))
    .expect("a password");
    url.set_query(Some("use_mozillas_root_certificates"));
    url.to_string()
}

fn store_with(url: &str, query: &str, keys: &[&str], batch: BatchSpec) -> HanaStore {
    let lease: Arc<dyn Lease> = Arc::new(NoLease);
    HanaStore::new(
        "e2e",
        Arc::new(Secret::new(SensitiveString::from(url.to_owned()))),
        query.to_owned(),
        keys.iter().map(|k| (*k).to_owned()).collect(),
        batch,
        lease,
    )
}

fn store(query: &str, keys: &[&str]) -> HanaStore {
    store_with(&connection_string(None), query, keys, BatchSpec::default())
}

async fn rows(stream: dfe_fetcher_core::RowStream<'_>) -> Result<Vec<Row>, Error> {
    let mut out = Vec::new();
    let mut stream = stream;
    while let Some(row) = stream.next().await {
        out.push(row?);
    }
    Ok(out)
}

fn json_of(row: &Row) -> Value {
    serde_json::from_slice(&row.payload).expect("a row is JSON")
}

fn keys_of(row: &Row) -> Vec<Value> {
    match &row.mark {
        Some(Mark::Keyset(values)) => values.to_vec(),
        other => panic!("a tail row carries a keyset mark, not {other:?}"),
    }
}

/// One table of the test's own, dropped when it goes out of scope's owner calls `drop_table`.
struct Table {
    name: String,
}

impl Table {
    async fn create() -> Self {
        let name = format!(
            "DFE_FETCHER_E2E_{}",
            uuid::Uuid::new_v4().simple().to_string()[..8].to_uppercase()
        );
        let seed = store("SELECT 1 FROM DUMMY", &[]);
        let connection = seed.connect().await.expect("log in");
        connection
            .exec(format!(
                "CREATE COLUMN TABLE {name} (TS TIMESTAMP NOT NULL, ID INTEGER NOT NULL, \
                 NOTE NVARCHAR(100), AMOUNT DECIMAL(12,2), BODY NCLOB, FLAG BOOLEAN, \
                 DAY DATE, PRIMARY KEY (TS, ID))"
            ))
            .await
            .expect("create the table");
        Self { name }
    }

    async fn insert(&self, rows: &[(&str, i32)]) {
        let seed = store("SELECT 1 FROM DUMMY", &[]);
        let connection = seed.connect().await.expect("log in");
        for (ts, id) in rows {
            connection
                .dml(format!(
                    "INSERT INTO {} VALUES ('{ts}', {id}, 'row {id}', {id}.25, 'body {id}', \
                     TRUE, '2026-11-20')",
                    self.name
                ))
                .await
                .expect("insert a row");
        }
        connection.commit().await.expect("commit");
    }

    async fn drop_table(&self) {
        let seed = store("SELECT 1 FROM DUMMY", &[]);
        let connection = seed.connect().await.expect("log in");
        connection
            .exec(format!("DROP TABLE {}", self.name))
            .await
            .expect("drop the table");
    }

    fn query(&self) -> String {
        format!(
            "SELECT TS, ID, NOTE, AMOUNT, BODY, FLAG, DAY FROM {}",
            self.name
        )
    }
}

#[tokio::test]
#[ignore = "needs a live HANA system: see the module doc"]
async fn a_password_login_over_tls_answers_a_probe_and_a_wrong_password_is_refused() {
    store("SELECT 1 FROM DUMMY", &[])
        .probe()
        .await
        .expect("the login probes");

    let err = store_with(
        &connection_string(Some("not-the-password")),
        "SELECT 1 FROM DUMMY",
        &[],
        BatchSpec::default(),
    )
    .probe()
    .await
    .unwrap_err();
    assert!(
        matches!(err, Error::Credential(_)),
        "a bad password is a credential error, not {err:?}"
    );
}

#[tokio::test]
#[ignore = "needs a live HANA system: see the module doc"]
async fn a_tail_resumes_past_its_last_row_and_reads_each_row_once_across_ties() {
    let table = Table::create().await;
    // Rows 4 to 6 share a timestamp: only the second key keeps their order.
    table
        .insert(&[
            ("2026-11-20 01:00:00.1000000", 1),
            ("2026-11-20 01:00:00.2000000", 2),
            ("2026-11-20 01:00:00.3000000", 3),
            ("2026-11-20 01:00:01.0000000", 4),
            ("2026-11-20 01:00:01.0000000", 5),
            ("2026-11-20 01:00:01.0000000", 6),
            ("2026-11-20 01:00:02.0000000", 7),
        ])
        .await;
    let tail = store(&table.query(), &["TS", "ID"]);

    let first = rows(tail.tail(None, 4)).await.expect("first page");
    assert_eq!(first.len(), 4, "the page is capped at its limit");
    let ids: Vec<i64> = first
        .iter()
        .map(|r| json_of(r)["ID"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, [1, 2, 3, 4]);
    let body = json_of(&first[0]);
    assert_eq!(
        body["TS"],
        json!("2026-11-20T01:00:00.1000000"),
        "a timestamp is zoneless ISO text"
    );
    assert_eq!(body["NOTE"], json!("row 1"));
    assert_eq!(
        body["AMOUNT"],
        json!(1.25),
        "a DECIMAL that fits is a number"
    );
    assert_eq!(body["BODY"], json!("body 1"), "an NCLOB is read as text");
    assert_eq!(body["FLAG"], json!(true));
    assert_eq!(body["DAY"], json!("2026-11-20"));

    let resume = keys_of(first.last().unwrap());
    assert_eq!(resume, [json!("2026-11-20T01:00:01.0000000"), json!(4)]);
    let second = rows(tail.tail(Some(resume), 10))
        .await
        .expect("resumed page");
    let ids: Vec<i64> = second
        .iter()
        .map(|r| json_of(r)["ID"].as_i64().unwrap())
        .collect();
    assert_eq!(
        ids,
        [5, 6, 7],
        "rows tied with the checkpoint on the first key are not skipped or repeated"
    );

    let end = keys_of(second.last().unwrap());
    let caught_up = rows(tail.tail(Some(end), 10)).await.expect("caught up");
    assert_eq!(caught_up.len(), 0, "nothing is past the last row");

    table.insert(&[("2026-11-20 01:00:03.0000000", 8)]).await;
    let end = keys_of(second.last().unwrap());
    let late = rows(tail.tail(Some(end), 10)).await.expect("a late row");
    assert_eq!(late.len(), 1);
    assert_eq!(json_of(&late[0])["ID"], json!(8));

    let dump = rows(store(&table.query(), &[]).dump()).await.expect("dump");
    assert_eq!(dump.len(), 8);
    assert!(dump.iter().all(|r| r.mark.is_none()));

    table.drop_table().await;
}

#[tokio::test]
#[ignore = "needs a live HANA system: see the module doc"]
async fn a_lob_over_the_batch_cap_fails_the_read_naming_the_cap() {
    let table = Table::create().await;
    table.insert(&[("2026-11-20 01:00:00.1000000", 1)]).await;
    let capped = store_with(
        &connection_string(None),
        &table.query(),
        &[],
        BatchSpec {
            max_text_bytes: 4,
            ..BatchSpec::default()
        },
    );

    let err = rows(capped.dump()).await.unwrap_err().to_string();

    table.drop_table().await;
    assert!(err.contains("batch.max_text_bytes"), "{err}");
}

/// Every column of `SYS.AUDIT_LOG` is nullable, the timestamp included, and a
/// null key never compares greater than anything: a row with one would be
/// skipped. So the keys are coalesced in the query, which is how an operator
/// reads this view, and the view has no unique id.
const AUDIT_QUERY: &str = "SELECT COALESCE(TIMESTAMP, '1970-01-01 00:00:00') AS TIMESTAMP, \
    COALESCE(CONNECTION_ID, 0) AS CONNECTION_ID, COALESCE(EVENT_ACTION, '') AS EVENT_ACTION, \
    COALESCE(USER_NAME, '') AS USER_NAME, EVENT_STATUS, STATEMENT_STRING FROM SYS.AUDIT_LOG";

#[tokio::test]
#[ignore = "needs a live HANA system: see the module doc"]
async fn sys_audit_log_is_read_in_key_order_and_a_one_row_page_resume_reads_what_one_read_does() {
    let keys = ["TIMESTAMP", "CONNECTION_ID", "EVENT_ACTION"];
    let audit = store(AUDIT_QUERY, &keys);

    let all = rows(audit.tail(None, 10_000))
        .await
        .expect("read the audit log");
    let marks: Vec<Vec<Value>> = all.iter().map(keys_of).collect();
    assert!(
        marks
            .windows(2)
            .all(|w| w[0][0].as_str() <= w[1][0].as_str()),
        "rows come back in timestamp order"
    );

    // Page through it a row at a time, resuming from each row's own mark.
    let mut paged: Vec<Vec<Value>> = Vec::new();
    let mut after: Option<Vec<Value>> = None;
    loop {
        let page = rows(audit.tail(after.clone(), 1))
            .await
            .expect("a one-row page");
        let Some(row) = page.last() else { break };
        assert_eq!(page.len(), 1, "the page is capped at one row");
        let mark = keys_of(row);
        paged.push(mark.clone());
        after = Some(mark);
        assert!(paged.len() <= marks.len() + 1, "the resume never loops");
    }
    let mut unique = marks.clone();
    unique.dedup();
    assert_eq!(
        paged, unique,
        "resuming one row at a time reads each distinct key once, in order"
    );
    for row in &all {
        let json = json_of(row);
        assert!(
            json["TIMESTAMP"].is_string(),
            "a timestamp key is ISO text: {json}"
        );
        assert!(json["CONNECTION_ID"].is_number());
    }
}
