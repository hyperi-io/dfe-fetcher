// Project:   dfe-fetcher
// File:      crates/core/src/envelope.rs
// Purpose:   Snapshot envelope for dump units: begin/row/oversize/end markers
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The snapshot envelope a dump unit's rows travel in.
//!
//! A dump is one `begin` marker, the store's rows, and one `end` marker
//! carrying the row count, all on the same topic and all stamped with the same
//! `snapshot_id` (UUIDv7) and `snapshot_at`. `timestamp` repeats `snapshot_at`
//! so the loader's `_timestamp` groups the whole dump; the provider's record
//! sits under `record` so its own keys cannot collide with the envelope's.
//! `seq` is 0-based and monotonic over rows and oversize stubs, so
//! `end.row_count` equals the number of `row` and `oversize` frames a consumer
//! must see. A dump that aborts emits no `end`, which is the consumer-side
//! incompleteness signal; [`Reassembler`] is that check.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};

/// The four frame kinds of a snapshot, the value of the top-level `kind` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// First frame of a dump; carries the store's own count when known.
    Begin,
    /// One provider record under `record`.
    Row,
    /// A record too large for the transport, replaced by its size and key.
    Oversize,
    /// Last frame of a dump; carries the emitted row count.
    End,
}

impl Kind {
    /// Every kind, for exhaustiveness checks.
    pub const ALL: [Kind; 4] = [Kind::Begin, Kind::Row, Kind::Oversize, Kind::End];

    /// The wire spelling of this kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Kind::Begin => "begin",
            Kind::Row => "row",
            Kind::Oversize => "oversize",
            Kind::End => "end",
        }
    }
}

/// The fields every frame of a snapshot carries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Head {
    /// The dump this frame belongs to, a UUIDv7 minted at `snapshot_at`.
    pub snapshot_id: Uuid,
    /// When the dump started.
    pub snapshot_at: DateTime<Utc>,
    /// Equal to `snapshot_at`, so the loader's `_timestamp` groups the dump.
    pub timestamp: DateTime<Utc>,
    /// `<connection>.<unit>`.
    pub store: String,
    /// 0-based position: the row's own for `row` and `oversize`, the next row's
    /// for `begin` (always 0) and `end` (the row count).
    pub seq: u64,
}

/// One frame of a snapshot as a consumer or a test reads it back.
///
/// The writer is [`Snapshot`], which builds the bytes without a `Value` tree;
/// this type is the read side and is exhaustive over [`Kind`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Envelope {
    /// First frame.
    Begin {
        /// Common fields.
        #[serde(flatten)]
        head: Head,
        /// The store's own count, when the connector knows it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        total: Option<u64>,
    },
    /// A provider record.
    Row {
        /// Common fields.
        #[serde(flatten)]
        head: Head,
        /// The record as the provider sent it.
        record: serde_json::Value,
    },
    /// A record replaced by a stub because it exceeded the transport limit.
    Oversize {
        /// Common fields.
        #[serde(flatten)]
        head: Head,
        /// Size of the record that was not sent.
        bytes: u64,
        /// The record's identity per the unit's `row_key`, when it had one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        row_key: Option<String>,
    },
    /// Last frame.
    End {
        /// Common fields.
        #[serde(flatten)]
        head: Head,
        /// Rows and oversize stubs emitted, which is what a consumer must count.
        row_count: u64,
        /// When the dump finished.
        completed_at: DateTime<Utc>,
    },
}

impl Envelope {
    /// Which frame this is.
    #[must_use]
    pub fn kind(&self) -> Kind {
        match self {
            Envelope::Begin { .. } => Kind::Begin,
            Envelope::Row { .. } => Kind::Row,
            Envelope::Oversize { .. } => Kind::Oversize,
            Envelope::End { .. } => Kind::End,
        }
    }

    /// The common fields.
    #[must_use]
    pub fn head(&self) -> &Head {
        match self {
            Envelope::Begin { head, .. }
            | Envelope::Row { head, .. }
            | Envelope::Oversize { head, .. }
            | Envelope::End { head, .. } => head,
        }
    }
}

/// The writer side of one dump: mints the id, stamps every frame, counts `seq`.
///
/// `row` costs one allocation (the fixed head, built once, plus the provider's
/// bytes) and never parses the record.
#[derive(Debug, Clone)]
pub struct Snapshot {
    id: Uuid,
    at: DateTime<Utc>,
    store: Arc<str>,
    seq: u64,
    /// `"snapshot_id":"..","snapshot_at":"..","timestamp":"..","store":"..","seq":`
    /// -- everything after the kind, built once per dump.
    head: Vec<u8>,
}

impl Snapshot {
    /// Start a dump of `store` now.
    #[must_use]
    pub fn start(store: &str) -> Self {
        Self::start_at(store, Utc::now())
    }

    /// Start a dump of `store` at a fixed instant, for deterministic tests and
    /// for a connector that reports the store's own export time.
    #[must_use]
    pub fn start_at(store: &str, at: DateTime<Utc>) -> Self {
        let id = Uuid::new_v7(uuid::Timestamp::from_unix(
            uuid::NoContext,
            u64::try_from(at.timestamp()).unwrap_or(0),
            at.timestamp_subsec_nanos(),
        ));
        let stamp = at.to_rfc3339_opts(SecondsFormat::Millis, true);
        let store_literal = serde_json::Value::String(store.to_owned()).to_string();
        let mut head = Vec::with_capacity(160 + store_literal.len());
        head.extend_from_slice(b"\"snapshot_id\":\"");
        head.extend_from_slice(id.hyphenated().to_string().as_bytes());
        head.extend_from_slice(b"\",\"snapshot_at\":\"");
        head.extend_from_slice(stamp.as_bytes());
        head.extend_from_slice(b"\",\"timestamp\":\"");
        head.extend_from_slice(stamp.as_bytes());
        head.extend_from_slice(b"\",\"store\":");
        head.extend_from_slice(store_literal.as_bytes());
        head.extend_from_slice(b",\"seq\":");
        Self {
            id,
            at,
            store: Arc::from(store),
            seq: 0,
            head,
        }
    }

    /// The dump's id.
    #[must_use]
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// When the dump started.
    #[must_use]
    pub fn started_at(&self) -> DateTime<Utc> {
        self.at
    }

    /// The `store` every frame carries.
    #[must_use]
    pub fn store(&self) -> &str {
        &self.store
    }

    /// Rows and stubs emitted so far; the `seq` the next row will take.
    #[must_use]
    pub fn seq(&self) -> u64 {
        self.seq
    }

    fn frame(&self, kind: Kind, seq: u64, tail_capacity: usize) -> Vec<u8> {
        let mut b = Vec::with_capacity(self.head.len() + tail_capacity + 40);
        b.extend_from_slice(b"{\"kind\":\"");
        b.extend_from_slice(kind.as_str().as_bytes());
        b.extend_from_slice(b"\",");
        b.extend_from_slice(&self.head);
        b.extend_from_slice(itoa::Buffer::new().format(seq).as_bytes());
        b
    }

    /// The `begin` marker, with the store's own count when known.
    #[must_use]
    pub fn begin(&self, total: Option<u64>) -> Bytes {
        let mut b = self.frame(Kind::Begin, self.seq, 32);
        if let Some(total) = total {
            b.extend_from_slice(b",\"total\":");
            b.extend_from_slice(itoa::Buffer::new().format(total).as_bytes());
        }
        b.push(b'}');
        Bytes::from(b)
    }

    /// Wrap one provider record; takes the next `seq`.
    pub fn row(&mut self, record: &[u8]) -> Bytes {
        let mut b = self.frame(Kind::Row, self.seq, record.len() + 12);
        b.extend_from_slice(b",\"record\":");
        b.extend_from_slice(record);
        b.push(b'}');
        self.seq += 1;
        Bytes::from(b)
    }

    /// Give back the `seq` of the last row when the filter dropped it, so the
    /// next emitted row takes it and `seq` stays contiguous over what lands.
    pub fn retract(&mut self) {
        self.seq = self.seq.saturating_sub(1);
    }

    /// The stub for a record that exceeded the transport limit; takes the next
    /// `seq` so the count still reconciles.
    pub fn oversize(&mut self, row_key: Option<&str>, bytes: usize) -> Bytes {
        let key_literal = row_key.map(|k| serde_json::Value::String(k.to_owned()).to_string());
        let mut b = self.frame(
            Kind::Oversize,
            self.seq,
            32 + key_literal.as_ref().map_or(0, String::len),
        );
        b.extend_from_slice(b",\"bytes\":");
        b.extend_from_slice(itoa::Buffer::new().format(bytes).as_bytes());
        if let Some(key) = key_literal {
            b.extend_from_slice(b",\"row_key\":");
            b.extend_from_slice(key.as_bytes());
        }
        b.push(b'}');
        self.seq += 1;
        Bytes::from(b)
    }

    /// The `end` marker: `row_count` is what the driver emitted after the
    /// filter, which is [`Self::seq`] when every row went through this writer.
    #[must_use]
    pub fn end(&self, row_count: u64) -> Bytes {
        let completed = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let mut b = self.frame(Kind::End, row_count, 64);
        b.extend_from_slice(b",\"row_count\":");
        b.extend_from_slice(itoa::Buffer::new().format(row_count).as_bytes());
        b.extend_from_slice(b",\"completed_at\":\"");
        b.extend_from_slice(completed.as_bytes());
        b.extend_from_slice(b"\"}");
        Bytes::from(b)
    }
}

/// When a row is too large to send, and how much of it the dead-letter copy keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct OversizePolicy {
    /// A row longer than this becomes an oversize stub. Default sits under
    /// librdkafka's 1,000,000-byte `message.max.bytes`.
    pub max_record_bytes: usize,
    /// How many leading bytes of an oversize row the dead-letter copy keeps.
    pub max_dlq_bytes: usize,
}

impl Default for OversizePolicy {
    fn default() -> Self {
        Self {
            max_record_bytes: 900 * 1024,
            max_dlq_bytes: 64 * 1024,
        }
    }
}

impl OversizePolicy {
    /// Whether a payload of this length must become a stub.
    #[must_use]
    pub fn is_oversize(&self, payload: &Bytes) -> bool {
        payload.len() > self.max_record_bytes
    }

    /// The dead-letter copy of an oversize payload: its first `max_dlq_bytes`.
    #[must_use]
    pub fn truncate(&self, payload: &Bytes) -> Bytes {
        payload.slice(..payload.len().min(self.max_dlq_bytes))
    }
}

/// Reassembly state of one snapshot as a consumer sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// No frame of this snapshot has been offered.
    Unknown,
    /// Frames seen, no `end` yet.
    Open {
        /// Rows and stubs seen so far.
        seen: u64,
    },
    /// `end` seen and every `seq` in `0..row_count` accounted for.
    Complete {
        /// `end.row_count`.
        rows: u64,
    },
    /// `end` seen but frames are missing.
    Truncated {
        /// `end.row_count`.
        expected: u64,
        /// Distinct rows and stubs seen.
        seen: u64,
    },
}

#[derive(Debug, Default)]
struct Partial {
    rows: BTreeMap<u64, serde_json::Value>,
    stubs: BTreeMap<u64, Option<String>>,
    begun: bool,
    row_count: Option<u64>,
}

impl Partial {
    fn seen(&self) -> u64 {
        // Both maps are keyed by seq and a seq is either a row or a stub.
        (self.rows.len() + self.stubs.len()) as u64
    }

    fn contiguous(&self, count: u64) -> bool {
        self.seen() == count
            && (0..count).all(|seq| self.rows.contains_key(&seq) || self.stubs.contains_key(&seq))
    }
}

/// Rebuilds snapshots from envelope frames, as a consumer would.
///
/// Frames may arrive in any order and more than once (at-least-once
/// delivery); a snapshot is complete only when its `end` has arrived and every
/// `seq` in `0..row_count` is present as a row or a stub.
#[derive(Debug, Default)]
pub struct Reassembler {
    snapshots: HashMap<Uuid, Partial>,
}

impl Reassembler {
    /// Feed one frame.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Decode`] when the bytes are not an envelope frame.
    pub fn offer(&mut self, frame: &[u8]) -> Result<Kind> {
        let envelope: Envelope = serde_json::from_slice(frame)
            .map_err(|e| Error::Decode(format!("not a snapshot envelope frame: {e}")))?;
        let kind = envelope.kind();
        let partial = self
            .snapshots
            .entry(envelope.head().snapshot_id)
            .or_default();
        match envelope {
            Envelope::Begin { .. } => partial.begun = true,
            Envelope::Row { head, record } => {
                partial.rows.insert(head.seq, record);
            }
            Envelope::Oversize { head, row_key, .. } => {
                partial.stubs.insert(head.seq, row_key);
            }
            Envelope::End { row_count, .. } => partial.row_count = Some(row_count),
        }
        Ok(kind)
    }

    /// Whether `begin` has been seen for this snapshot.
    #[must_use]
    pub fn has_begun(&self, id: Uuid) -> bool {
        self.snapshots.get(&id).is_some_and(|p| p.begun)
    }

    /// Reassembly state of one snapshot.
    #[must_use]
    pub fn status(&self, id: Uuid) -> Status {
        let Some(partial) = self.snapshots.get(&id) else {
            return Status::Unknown;
        };
        match partial.row_count {
            None => Status::Open {
                seen: partial.seen(),
            },
            Some(count) if partial.contiguous(count) => Status::Complete { rows: count },
            Some(count) => Status::Truncated {
                expected: count,
                seen: partial.seen(),
            },
        }
    }

    /// The rows of a complete snapshot in `seq` order; `None` while it is open
    /// or truncated. Stubs are counted for completeness and excluded here.
    #[must_use]
    pub fn complete(&self, id: Uuid) -> Option<Vec<serde_json::Value>> {
        match self.status(id) {
            Status::Complete { .. } => self
                .snapshots
                .get(&id)
                .map(|p| p.rows.values().cloned().collect()),
            _ => None,
        }
    }

    /// The `row_key`s of the oversize stubs seen for a snapshot, in `seq` order.
    #[must_use]
    pub fn oversize_keys(&self, id: Uuid) -> Vec<String> {
        self.snapshots
            .get(&id)
            .map_or_else(Vec::new, |p| p.stubs.values().flatten().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn snapshot() -> Snapshot {
        Snapshot::start("runzero_lab.assets")
    }

    #[test]
    fn begin_row_end_round_trip_through_serde() {
        let mut snap = snapshot();
        let begin: Envelope = serde_json::from_slice(&snap.begin(Some(15))).unwrap();
        let row: Envelope =
            serde_json::from_slice(&snap.row(br#"{"id":"a1","alive":true}"#)).unwrap();
        let stub: Envelope = serde_json::from_slice(&snap.oversize(Some("a2"), 1_048_576)).unwrap();
        let end: Envelope = serde_json::from_slice(&snap.end(2)).unwrap();

        assert_eq!(begin.kind(), Kind::Begin);
        match begin {
            Envelope::Begin { head, total } => {
                assert_eq!(head.store, "runzero_lab.assets");
                assert_eq!(head.seq, 0);
                assert_eq!(head.timestamp, head.snapshot_at);
                assert_eq!(head.snapshot_id, snap.id());
                assert_eq!(
                    head.snapshot_id.get_version(),
                    Some(uuid::Version::SortRand)
                );
                assert_eq!(total, Some(15));
            }
            other => panic!("expected begin, got {other:?}"),
        }
        match row {
            Envelope::Row { head, record } => {
                assert_eq!(
                    head.seq, 0,
                    "the first data row is seq 0; markers carry no seq"
                );
                assert_eq!(record, json!({"id": "a1", "alive": true}));
            }
            other => panic!("expected row, got {other:?}"),
        }
        match stub {
            Envelope::Oversize {
                head,
                bytes,
                row_key,
            } => {
                assert_eq!(
                    head.seq, 1,
                    "an oversize stub takes the seq the row would have"
                );
                assert_eq!(bytes, 1_048_576);
                assert_eq!(row_key.as_deref(), Some("a2"));
            }
            other => panic!("expected oversize, got {other:?}"),
        }
        match end {
            Envelope::End {
                head,
                row_count,
                completed_at,
            } => {
                assert_eq!(
                    head.seq, 2,
                    "end.seq is the row count, one past the last row"
                );
                assert_eq!(row_count, 2);
                assert!(completed_at >= head.snapshot_at);
            }
            other => panic!("expected end, got {other:?}"),
        }
    }

    #[test]
    fn every_kind_is_a_variant_and_the_tag_is_kind() {
        let mut snap = snapshot();
        let frames = [
            snap.begin(None),
            snap.row(b"{}"),
            snap.oversize(None, 1),
            snap.end(2),
        ];
        let kinds: Vec<Kind> = frames
            .iter()
            .map(|f| serde_json::from_slice::<Envelope>(f).unwrap().kind())
            .collect();
        assert_eq!(kinds, [Kind::Begin, Kind::Row, Kind::Oversize, Kind::End]);
        for frame in &frames {
            let value: serde_json::Value = serde_json::from_slice(frame).unwrap();
            assert!(value["kind"].is_string(), "kind is a top-level string tag");
            assert_eq!(value["timestamp"], value["snapshot_at"]);
        }
        assert_eq!(
            Kind::ALL.len(),
            4,
            "add the new kind to the reassembler before extending this"
        );
    }

    #[test]
    fn row_bytes_are_the_providers_bytes_verbatim() {
        let mut snap = snapshot();
        let raw = br#"{"z":1,  "a":  [1,2] }"#;
        let framed = snap.row(raw);
        let text = std::str::from_utf8(&framed).unwrap();
        assert!(text.ends_with(r#","record":{"z":1,  "a":  [1,2] }}"#));
        assert!(text.starts_with(r#"{"kind":"row","snapshot_id":""#));
    }

    #[test]
    fn seq_is_monotonic_across_rows_and_stubs_and_end_carries_the_count() {
        let mut snap = snapshot();
        let seqs: Vec<u64> = [
            snap.row(b"{}"),
            snap.oversize(None, 9),
            snap.row(b"{}"),
            snap.oversize(Some("k"), 9),
        ]
        .iter()
        .map(|f| serde_json::from_slice::<Envelope>(f).unwrap().head().seq)
        .collect();
        assert_eq!(seqs, [0, 1, 2, 3]);
        assert_eq!(snap.seq(), 4);
        let end: Envelope = serde_json::from_slice(&snap.end(snap.seq())).unwrap();
        assert_eq!(end.head().seq, 4);
    }

    #[test]
    fn a_retracted_row_gives_its_seq_to_the_next_emitted_row() {
        let mut snap = snapshot();
        let _dropped_by_filter = snap.row(br#"{"alive":false}"#);
        snap.retract();
        let kept: Envelope = serde_json::from_slice(&snap.row(br#"{"alive":true}"#)).unwrap();
        assert_eq!(kept.head().seq, 0);
        assert_eq!(snap.seq(), 1);
        let mut fresh = snapshot();
        fresh.retract();
        assert_eq!(fresh.seq(), 0, "retracting with nothing emitted is a no-op");
    }

    #[test]
    fn reassembler_completes_only_with_end_and_matching_count() {
        let mut snap = snapshot();
        let mut asm = Reassembler::default();
        asm.offer(&snap.begin(None)).unwrap();
        asm.offer(&snap.row(br#"{"id":1}"#)).unwrap();
        asm.offer(&snap.row(br#"{"id":2}"#)).unwrap();
        assert!(asm.complete(snap.id()).is_none(), "no end marker yet");
        asm.offer(&snap.end(2)).unwrap();
        let rows = asm.complete(snap.id()).expect("complete snapshot");
        assert_eq!(rows, vec![json!({"id": 1}), json!({"id": 2})]);
    }

    #[test]
    fn reassembler_rejects_a_truncated_snapshot() {
        let mut snap = snapshot();
        let mut asm = Reassembler::default();
        asm.offer(&snap.begin(None)).unwrap();
        asm.offer(&snap.row(br#"{"id":1}"#)).unwrap();
        let _lost_in_transit = snap.row(br#"{"id":2}"#);
        asm.offer(&snap.end(2)).unwrap();
        assert!(
            asm.complete(snap.id()).is_none(),
            "end.row_count says 2 but only one row arrived"
        );
        assert_eq!(
            asm.status(snap.id()),
            Status::Truncated {
                expected: 2,
                seen: 1
            }
        );
    }

    #[test]
    fn reassembler_tolerates_out_of_order_arrival_and_duplicates() {
        let mut snap = snapshot();
        let begin = snap.begin(None);
        let r0 = snap.row(br#"{"id":0}"#);
        let r1 = snap.row(br#"{"id":1}"#);
        let end = snap.end(2);
        let mut asm = Reassembler::default();
        asm.offer(&end).unwrap();
        asm.offer(&r1).unwrap();
        asm.offer(&r1).unwrap();
        asm.offer(&r0).unwrap();
        asm.offer(&begin).unwrap();
        let rows = asm.complete(snap.id()).expect("complete after reordering");
        assert_eq!(
            rows,
            vec![json!({"id": 0}), json!({"id": 1})],
            "rows come back in seq order"
        );
    }

    #[test]
    fn oversize_stubs_count_toward_completeness_and_are_reported() {
        let mut snap = snapshot();
        let mut asm = Reassembler::default();
        asm.offer(&snap.begin(None)).unwrap();
        asm.offer(&snap.row(br#"{"id":0}"#)).unwrap();
        asm.offer(&snap.oversize(Some("big"), 2_000_000)).unwrap();
        asm.offer(&snap.end(2)).unwrap();
        let rows = asm.complete(snap.id()).expect("stubs complete the count");
        assert_eq!(rows.len(), 1, "the stub is not a row");
        assert_eq!(asm.oversize_keys(snap.id()), vec!["big".to_string()]);
    }

    #[test]
    fn reassembler_refuses_non_envelope_bytes() {
        let mut asm = Reassembler::default();
        assert!(asm.offer(b"not json").is_err());
        assert!(
            asm.offer(br#"{"kind":"row"}"#).is_err(),
            "missing head fields"
        );
    }

    #[test]
    fn oversize_policy_decides_by_payload_length() {
        let policy = OversizePolicy {
            max_record_bytes: 8,
            max_dlq_bytes: 4,
        };
        assert!(!policy.is_oversize(&Bytes::from_static(b"12345678")));
        assert!(policy.is_oversize(&Bytes::from_static(b"123456789")));
        assert_eq!(
            policy.truncate(&Bytes::from_static(b"123456789")),
            &b"1234"[..]
        );
        assert_eq!(policy.truncate(&Bytes::from_static(b"12")), &b"12"[..]);
    }
}
