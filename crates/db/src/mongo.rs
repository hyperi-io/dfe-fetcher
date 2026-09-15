// Project:   dfe-fetcher
// File:      crates/db/src/mongo.rs
// Purpose:   The MongoDB engine: a collection dumped by find, tailed by change stream or by _id
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The MongoDB engine.
//!
//! One [`MongoStore`] per configured store over the official driver. A dump
//! runs `find` with the store's filter and streams the cursor: the driver
//! issues a `getMore` only when the cursor is polled, so an unpolled stream
//! holds at most one batch of `batch.max_rows` documents. A tail follows the
//! collection one of two ways:
//!
//! - `change_stream` (the default): `watch` with `full_document:
//!   update_lookup`, so every insert, update, replace and delete arrives as
//!   an event carrying the current document. Each event's `_id` is the resume
//!   token; the driver commits the last emitted one after the acks and the
//!   next tick reopens the stream with `resume_after` that token, so a
//!   restart resumes from exactly the last acknowledged event. A tick drains
//!   what the oplog holds, `limit` events at most, and ends once a `getMore`
//!   has waited its short await time and found nothing. Needs a replica set:
//!   a standalone server has no oplog and answers code 40573, which the
//!   store reports naming the keyset fallback.
//! - `keyset`: documents whose `_id` sorts after the last committed one, in
//!   `_id` order, `limit` per tick. Works on a standalone server and sees
//!   inserts only (an updated document keeps its `_id`), which is the same
//!   contract as a SQL keyset over an insert-ordered key.
//!
//! Both tails carry `_id` as the row's mark, so one checkpoint shape serves
//! both: a resume token document or the document's own identity.
//!
//! # Row shape
//!
//! Rows are relaxed extended JSON, the driver's own conversion: `ObjectId`
//! lands as `{"$oid": "..."}`, a date as `{"$date": "2026-01-01T00:00:00Z"}`,
//! `Int64` and `Double` as numbers, binary as `{"$binary": {...}}`. A change
//! event lands whole (`_id`, `operationType`, `ns`, `documentKey`,
//! `fullDocument`, `clusterTime`, ...), camel-cased as the server sends it.
//! The same conversion runs backwards on a checkpoint, so an `_id` of any
//! BSON type round-trips through the cursor store.

use std::sync::Arc;
use std::time::Duration;

use bson::{Bson, Document, doc};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::ResumeToken;
use mongodb::error::ErrorKind;
use mongodb::options::FullDocumentType;
use mongodb::{Client, Collection};
use serde_json::Value;
use tokio::sync::OnceCell;

use dfe_fetcher_core::batch::Lease;
use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::{Mark, Row, RowStream};

use crate::config::{BatchSpec, StoreSpec, TailMode};
use crate::lines::{LeasedBlock, rows_of_blocks};
use crate::secret::Secret;
use crate::store::Store;

/// The driver crate, re-exported so a caller can seed or inspect a
/// deployment with the client the engine itself speaks through.
pub use mongodb;

/// How long one change-stream `getMore` waits for an event.
const CHANGE_STREAM_AWAIT: Duration = Duration::from_millis(250);

/// Empty batches in a row that end a change-stream tick: the first may be
/// the `aggregate`'s own initial batch, which the driver reports without a
/// server wait, so only the second is a `getMore` that waited and found
/// nothing.
const CAUGHT_UP_AFTER_EMPTY_BATCHES: u8 = 2;

/// The server's answer to `$changeStream` on a deployment with no oplog.
const CHANGE_STREAM_NEEDS_REPLICA_SET: i32 = 40573;

/// The server's answer when the oplog no longer holds the resume point.
const CHANGE_STREAM_HISTORY_LOST: i32 = 286;

/// The mark column of both tails.
const ID: &str = "_id";

/// The framework error for a driver failure.
fn mongo_error(e: &mongodb::error::Error) -> Error {
    match e.kind.as_ref() {
        ErrorKind::Authentication { message, .. } => {
            Error::Credential(format!("mongodb authentication: {message}"))
        }
        // Unauthorized, AuthenticationFailed.
        ErrorKind::Command(cmd) if matches!(cmd.code, 13 | 18) => Error::Credential(format!(
            "mongodb ({} {}): {}",
            cmd.code, cmd.code_name, cmd.message
        )),
        ErrorKind::Command(cmd) => Error::Source(format!(
            "mongodb ({} {}): {}",
            cmd.code, cmd.code_name, cmd.message
        )),
        ErrorKind::InvalidArgument { message, .. } => Error::Config(format!("mongodb: {message}")),
        ErrorKind::InvalidTlsConfig { message, .. } => {
            Error::Config(format!("mongodb tls: {message}"))
        }
        ErrorKind::Io(_)
        | ErrorKind::DnsResolve { .. }
        | ErrorKind::ServerSelection { .. }
        | ErrorKind::ConnectionPoolCleared { .. } => Error::Source(format!("mongodb network: {e}")),
        _ => Error::Source(format!("mongodb: {e}")),
    }
}

/// A `watch` failure, with the two cases an operator has to act on named:
/// no oplog at all, and an oplog that has rolled past the committed token.
fn change_stream_error(e: &mongodb::error::Error, unit: &str) -> Error {
    if let ErrorKind::Command(cmd) = e.kind.as_ref() {
        if cmd.code == CHANGE_STREAM_NEEDS_REPLICA_SET {
            return Error::Config(format!(
                "mongodb ({} {}): {}; a change stream needs a replica set -- set \
                 `mongodb.tail: keyset` for a standalone server",
                cmd.code, cmd.code_name, cmd.message
            ));
        }
        if cmd.code == CHANGE_STREAM_HISTORY_LOST {
            return Error::Cursor(format!(
                "mongodb ({} {}): {}; the oplog no longer reaches unit `{unit}`'s committed \
                 resume token -- delete that unit's checkpoint to restart the tail from now, \
                 accepting the gap",
                cmd.code, cmd.code_name, cmd.message
            ));
        }
    }
    mongo_error(e)
}

/// One document as relaxed extended JSON.
fn json_of(doc: Document) -> Result<Vec<u8>> {
    let value = Bson::Document(doc).into_relaxed_extjson();
    serde_json::to_vec(&value).map_err(|e| Error::Decode(format!("document to JSON: {e}")))
}

/// One document as an NDJSON line in relaxed extended JSON.
fn line_of(doc: Document) -> Result<Vec<u8>> {
    let mut line = json_of(doc)?;
    line.push(b'\n');
    Ok(line)
}

/// A mark-only row carrying `token`, when the stream sits somewhere the tail
/// has not committed; `None` when the server reported no token or the tail is
/// already there.
///
/// An idle tick emits no event, so without this the committed token stays
/// where the last event was and the oplog eventually rolls past it, which the
/// server answers with code 286 on every later tick.
fn idle_advance(token: Option<ResumeToken>, committed: Option<&Value>) -> Option<Result<Row>> {
    let token = token?;
    let value = match bson::serialize_to_bson(&token) {
        Ok(bson) => bson.into_relaxed_extjson(),
        Err(e) => {
            return Some(Err(Error::Cursor(format!(
                "change stream resume token is not BSON: {e}"
            ))));
        }
    };
    if committed == Some(&value) {
        return None;
    }
    Some(Ok(Row::mark_only(Mark::Keyset(smallvec::smallvec![value]))))
}

/// A checkpointed `_id` back as BSON, extended JSON understood.
fn bson_of(value: &Value) -> Result<Bson> {
    Bson::try_from(value.clone())
        .map_err(|e| Error::Cursor(format!("checkpoint `_id` is not extended JSON: {e}")))
}

/// A checkpointed resume token back as the driver's own type.
fn resume_token_of(value: &Value) -> Result<ResumeToken> {
    let token = bson_of(value)?;
    if !matches!(token, Bson::Document(_)) {
        return Err(Error::Cursor(
            "checkpoint is not a resume token document".into(),
        ));
    }
    bson::deserialize_from_bson(token)
        .map_err(|e| Error::Cursor(format!("checkpoint is not a resume token: {e}")))
}

/// One store on a MongoDB deployment.
pub struct MongoStore {
    uri: Arc<Secret>,
    client: OnceCell<Client>,
    database: String,
    collection: String,
    filter: Document,
    tail: TailMode,
    batch_size: u32,
    lease: Arc<dyn Lease>,
    unit: Arc<str>,
}

impl MongoStore {
    /// A store over `spec`'s collection on the deployment `uri` resolves to.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the spec carries no `mongodb` block or
    /// its filter is not a BSON document.
    pub fn new(
        spec: &StoreSpec,
        uri: Arc<Secret>,
        batch: BatchSpec,
        lease: Arc<dyn Lease>,
    ) -> Result<Self> {
        let mongo = spec
            .mongodb
            .as_ref()
            .ok_or_else(|| Error::Config("mongodb: is required for the mongodb engine".into()))?;
        let filter = match &mongo.filter {
            Some(map) => Document::try_from(map.clone())
                .map_err(|e| Error::Config(format!("mongodb.filter: not a BSON document: {e}")))?,
            None => Document::new(),
        };
        Ok(Self {
            uri,
            client: OnceCell::new(),
            database: mongo.database.clone(),
            collection: mongo.collection.clone(),
            filter,
            tail: mongo.tail_mode(),
            batch_size: u32::try_from(batch.max_rows).unwrap_or(u32::MAX),
            lease,
            unit: Arc::from(spec.unit.as_str()),
        })
    }

    /// The tail mode in force.
    #[must_use]
    pub fn tail_mode(&self) -> TailMode {
        self.tail
    }

    async fn client(&self) -> Result<&Client> {
        self.client
            .get_or_try_init(|| async {
                let uri = self.uri.value().await?;
                Client::with_uri_str(uri).await.map_err(|e| mongo_error(&e))
            })
            .await
    }

    async fn collection(&self) -> Result<Collection<Document>> {
        Ok(self
            .client()
            .await?
            .database(&self.database)
            .collection(&self.collection))
    }

    fn block(&self, doc: Document) -> Result<LeasedBlock> {
        Ok(LeasedBlock::new(
            Bytes::from(line_of(doc)?),
            Arc::clone(&self.lease),
        ))
    }

    /// The store's filter with the keyset predicate folded in.
    fn keyset_filter(&self, after: Option<&Value>) -> Result<Document> {
        let Some(after) = after else {
            return Ok(self.filter.clone());
        };
        let past = doc! { ID: { "$gt": bson_of(after)? } };
        if self.filter.is_empty() {
            return Ok(past);
        }
        Ok(doc! { "$and": [self.filter.clone(), past] })
    }

    /// The documents past `after` in `_id` order, at most `limit`.
    fn keyset_blocks(
        &self,
        after: Option<Value>,
        limit: u32,
    ) -> BoxStream<'_, Result<LeasedBlock>> {
        let start = async move {
            let filter = self.keyset_filter(after.as_ref())?;
            let cursor = self
                .collection()
                .await?
                .find(filter)
                .sort(doc! { ID: 1 })
                .limit(i64::from(limit))
                .batch_size(self.batch_size)
                .await
                .map_err(|e| mongo_error(&e))?;
            Ok::<_, Error>(cursor.map(move |item| self.block(item.map_err(|e| mongo_error(&e))?)))
        };
        futures::stream::once(start).try_flatten().boxed()
    }

    /// One change event as a row, marked with its own resume token (`_id`).
    fn event_row(&self, event: Document) -> Result<Row> {
        let token = event
            .get(ID)
            .cloned()
            .ok_or_else(|| Error::Decode("change event carries no `_id` resume token".into()))?;
        Ok(Row {
            payload: Bytes::from(json_of(event)?),
            mark: Some(Mark::Keyset(smallvec::smallvec![
                token.into_relaxed_extjson()
            ])),
        })
    }

    /// The change events past `after`, at most `limit`, ending once the
    /// server has waited and found nothing.
    ///
    /// A tick that emits no event ends with a mark-only row carrying the
    /// stream's post-batch resume token, so a quiet collection still moves the
    /// committed position forward.
    fn change_stream_rows(&self, after: Option<Value>, limit: u32) -> RowStream<'_> {
        /// Where one tick of the stream is up to.
        struct Tick {
            stream: ChangeStream<Document>,
            remaining: u32,
            empties: u8,
            emitted: bool,
            committed: Option<Value>,
            done: bool,
        }

        let start = async move {
            let collection = self.collection().await?;
            let mut watch = collection
                .watch()
                .full_document(FullDocumentType::UpdateLookup)
                .batch_size(self.batch_size)
                .max_await_time(CHANGE_STREAM_AWAIT);
            if !self.filter.is_empty() {
                watch = watch.pipeline([doc! { "$match": self.filter.clone() }]);
            }
            if let Some(token) = &after {
                // `start_after`, not `resume_after`: they behave alike on an
                // ordinary token, and only this one resumes past the
                // `invalidate` that ends a stream when a collection is dropped.
                watch = watch.start_after(resume_token_of(token)?);
            }
            let stream: ChangeStream<Document> = watch
                .await
                .map_err(|e| change_stream_error(&e, &self.unit))?
                .with_type();
            tracing::debug!(unit = %self.unit, resumed = after.is_some(), "change stream open");
            let tick = Tick {
                stream,
                remaining: limit,
                empties: 0,
                emitted: false,
                committed: after,
                done: false,
            };
            Ok::<_, Error>(futures::stream::unfold(tick, move |mut t| async move {
                if t.done {
                    return None;
                }
                while t.remaining > 0 && t.empties < CAUGHT_UP_AFTER_EMPTY_BATCHES {
                    match t.stream.next_if_any().await {
                        Ok(Some(event)) => {
                            t.remaining -= 1;
                            t.empties = 0;
                            t.emitted = true;
                            return Some((self.event_row(event), t));
                        }
                        Ok(None) => t.empties += 1,
                        Err(e) => {
                            t.done = true;
                            return Some((Err(mongo_error(&e)), t));
                        }
                    }
                }
                t.done = true;
                if t.emitted {
                    return None;
                }
                let advance = idle_advance(t.stream.resume_token(), t.committed.as_ref());
                advance.map(|row| (row, t))
            }))
        };
        futures::stream::once(start).try_flatten().boxed()
    }
}

impl Store for MongoStore {
    fn dump(&self) -> RowStream<'_> {
        let start = async move {
            let cursor = self
                .collection()
                .await?
                .find(self.filter.clone())
                .batch_size(self.batch_size)
                .await
                .map_err(|e| mongo_error(&e))?;
            Ok::<_, Error>(cursor.map(move |item| self.block(item.map_err(|e| mongo_error(&e))?)))
        };
        rows_of_blocks(futures::stream::once(start).try_flatten().boxed(), None)
    }

    fn tail(&self, after: Option<Vec<Value>>, limit: u32) -> RowStream<'_> {
        let after = match after {
            Some(mut values) if values.len() == 1 => Some(values.remove(0)),
            Some(values) => {
                let problem = format!(
                    "checkpoint has {} values where a mongodb tail keeps one `_id`",
                    values.len()
                );
                return futures::stream::once(async move { Err(Error::Cursor(problem)) }).boxed();
            }
            None => None,
        };
        match self.tail {
            TailMode::ChangeStream => self.change_stream_rows(after, limit),
            TailMode::Keyset => rows_of_blocks(
                self.keyset_blocks(after, limit),
                Some(Arc::from(vec![ID.to_owned()])),
            ),
        }
    }

    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        async move {
            self.client()
                .await?
                .database("admin")
                .run_command(doc! { "ping": 1 })
                .await
                .map(|_| ())
                .map_err(|e| mongo_error(&e))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::oid::ObjectId;
    use dfe_fetcher_core::batch::NoLease;
    use scalo::SensitiveString;

    fn store(spec_yaml: &str) -> MongoStore {
        let spec: StoreSpec = serde_yaml_ng::from_str(spec_yaml).unwrap();
        MongoStore::new(
            &spec,
            Arc::new(Secret::new(SensitiveString::from(
                "mongodb://localhost:27017".to_owned(),
            ))),
            BatchSpec::default(),
            Arc::new(NoLease),
        )
        .unwrap()
    }

    #[test]
    fn a_document_lands_as_relaxed_extended_json_and_its_id_round_trips() {
        let oid = ObjectId::new();
        let at = bson::DateTime::from_millis(1_767_225_600_000);
        let line = line_of(doc! {
            "_id": oid,
            "n": 9_007_199_254_740_993_i64,
            "f": 1.5,
            "at": at,
            "s": "h\u{e9}llo",
            "b": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![0xde, 0xad] },
            "arr": [1, 2],
            "nested": { "k": Bson::Null },
        })
        .unwrap();
        assert_eq!(line.last(), Some(&b'\n'));
        let value: Value = serde_json::from_slice(&line[..line.len() - 1]).unwrap();
        assert_eq!(value["_id"], serde_json::json!({ "$oid": oid.to_hex() }));
        assert_eq!(value["n"], 9_007_199_254_740_993_i64, "Int64 is a number");
        assert_eq!(value["f"], 1.5);
        assert_eq!(
            value["at"],
            serde_json::json!({ "$date": "2026-01-01T00:00:00Z" })
        );
        assert_eq!(value["s"], "h\u{e9}llo");
        assert_eq!(value["b"]["$binary"]["base64"], "3q0=");
        assert_eq!(value["arr"], serde_json::json!([1, 2]));
        assert!(value["nested"]["k"].is_null());
        assert_eq!(bson_of(&value["_id"]).unwrap(), Bson::ObjectId(oid));
        assert_eq!(
            bson_of(&value["n"]).unwrap(),
            Bson::Int64(9_007_199_254_740_993)
        );
        assert_eq!(bson_of(&value["at"]).unwrap(), Bson::DateTime(at));
    }

    #[test]
    fn a_resume_token_round_trips_through_the_checkpoint_json() {
        let token_doc = doc! { "_data": "8266F1A2B3000000012B022C0100296E5A1004" };
        let token: ResumeToken =
            bson::deserialize_from_bson(Bson::Document(token_doc.clone())).unwrap();
        let stored = Bson::Document(token_doc.clone()).into_relaxed_extjson();
        assert_eq!(stored["_data"], "8266F1A2B3000000012B022C0100296E5A1004");
        let back = resume_token_of(&stored).unwrap();
        assert_eq!(back, token);
        assert_eq!(
            bson::serialize_to_bson(&back).unwrap(),
            Bson::Document(token_doc)
        );
        assert!(matches!(
            resume_token_of(&serde_json::json!("not a token")),
            Err(Error::Cursor(_))
        ));
    }

    #[test]
    fn the_keyset_filter_folds_the_operator_filter_in_with_the_id_predicate() {
        let s = store(
            "unit: a\nshape: tail\nmongodb: { database: d, collection: c, tail: keyset, filter: { alive: true } }\n",
        );
        assert_eq!(s.tail_mode(), TailMode::Keyset);
        assert_eq!(s.keyset_filter(None).unwrap(), doc! { "alive": true });
        let oid = ObjectId::new();
        let after = serde_json::json!({ "$oid": oid.to_hex() });
        assert_eq!(
            s.keyset_filter(Some(&after)).unwrap(),
            doc! { "$and": [{ "alive": true }, { "_id": { "$gt": oid } }] }
        );
        let bare = store("unit: a\nshape: tail\nmongodb: { database: d, collection: c, tail: keyset }\n");
        assert_eq!(
            bare.keyset_filter(Some(&serde_json::json!(7))).unwrap(),
            doc! { "_id": { "$gt": 7 } }
        );
    }

    #[test]
    fn the_default_tail_is_the_change_stream_and_a_bad_filter_is_a_config_error() {
        let s = store("unit: a\nshape: tail\nmongodb: { database: d, collection: c }\n");
        assert_eq!(s.tail_mode(), TailMode::ChangeStream);
        assert_eq!(s.batch_size, 5000);
        let spec: StoreSpec = serde_yaml_ng::from_str(
            "unit: a\nmongodb: { database: d, collection: c, filter: { at: { $date: 'not a date' } } }\n",
        )
        .unwrap();
        let Err(err) = MongoStore::new(
            &spec,
            Arc::new(Secret::new(SensitiveString::from("mongodb://x".to_owned()))),
            BatchSpec::default(),
            Arc::new(NoLease),
        ) else {
            panic!("a `$date` that is not a date is not a document");
        };
        assert!(matches!(err, Error::Config(_)), "{err}");
        assert!(
            err.to_string().contains("filter: not a BSON document"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_multi_value_checkpoint_is_refused_before_any_connection() {
        let s = store("unit: a\nshape: tail\nmongodb: { database: d, collection: c }\n");
        let values = [serde_json::json!(1), serde_json::json!(2)];
        let err = s
            .tail(Some(values.to_vec()), 10)
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, Error::Cursor(_)), "{err}");
    }

    /// An idle tick commits where the stream now sits, but only when that is
    /// somewhere new: otherwise a quiet collection would rewrite the same
    /// cursor every tick, and a busy one would never need to.
    #[test]
    fn an_idle_tick_advances_only_when_the_stream_has_moved() {
        let token_doc = doc! { "_data": "8266F1A2B3000000012B022C0100296E5A1004" };
        let token = || -> ResumeToken {
            bson::deserialize_from_bson(Bson::Document(token_doc.clone())).unwrap()
        };
        let value = Bson::Document(token_doc.clone()).into_relaxed_extjson();

        let row = idle_advance(Some(token()), None)
            .expect("a stream with a token advances")
            .expect("the token converts");
        assert!(row.is_mark_only(), "an advance carries no record");
        assert_eq!(
            row.mark,
            Some(Mark::Keyset(smallvec::smallvec![value.clone()]))
        );

        assert!(
            idle_advance(Some(token()), Some(&value)).is_none(),
            "already committed there: nothing to write"
        );
        assert!(
            idle_advance(None, None).is_none(),
            "a stream that reports no token cannot advance"
        );
    }

    /// A server error as the wire carries it.
    fn command_error(code: i32, name: &str, message: &str) -> mongodb::error::Error {
        let cmd: mongodb::error::CommandError = bson::deserialize_from_document(doc! {
            "code": code, "codeName": name, "errmsg": message,
        })
        .unwrap();
        mongodb::error::Error::from(ErrorKind::Command(cmd))
    }

    #[test]
    fn driver_errors_map_to_the_framework_vocabulary() {
        let denied = command_error(18, "AuthenticationFailed", "bad password");
        assert!(matches!(mongo_error(&denied), Error::Credential(_)));
        let standalone = command_error(
            40573,
            "Location40573",
            "The $changeStream stage is only supported on replica sets",
        );
        let mapped = change_stream_error(&standalone, "changes");
        assert!(matches!(mapped, Error::Config(_)), "{mapped}");
        assert!(
            mapped.to_string().contains("mongodb.tail: keyset"),
            "the error must name the key as the grammar spells it: {mapped}"
        );

        // The oplog has rolled past the committed token: the operator has to
        // clear that unit's checkpoint, so the error names it.
        let lost = command_error(
            286,
            "ChangeStreamHistoryLost",
            "Resume of change stream was not possible, as the resume point may no longer be in the oplog",
        );
        let mapped = change_stream_error(&lost, "changes");
        assert!(matches!(mapped, Error::Cursor(_)), "{mapped}");
        assert!(mapped.to_string().contains("`changes`"), "{mapped}");
        assert!(mapped.to_string().contains("checkpoint"), "{mapped}");
        let refused = mongodb::error::Error::from(std::io::Error::from(
            std::io::ErrorKind::ConnectionRefused,
        ));
        assert_eq!(mongo_error(&refused).api_error_code(), "network");
    }
}
