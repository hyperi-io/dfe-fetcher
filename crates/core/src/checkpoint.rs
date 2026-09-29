// Project:   dfe-fetcher
// File:      crates/core/src/checkpoint.rs
// Purpose:   Checkpoint contract: mark folding, cursor values, the store trait
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The checkpoint contract.
//!
//! A shape attaches a [`Mark`] to each row; the driver folds them into one
//! [`Checkpoint`] per unit and commits it only after the batch carrying the last
//! row has been acknowledged by the transport. An aborted tick therefore
//! re-fetches from the previous checkpoint (at-least-once, never loss).
//!
//! The persisted form is the pre-framework [`CursorValue`]: the scheduler's
//! window cursor stays `version: 1` with `api_cursor: None`, and a unit
//! checkpoint is `version: 2` with the [`CheckpointValue`] encoded as JSON in
//! `api_cursor`, so one store and one file format carry both.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::Mark;
use crate::error::Result;

/// Value stored for each cursor key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CursorValue {
    /// The key this cursor is stored under.
    pub cursor_key: String,

    /// End timestamp of the last successful fetch window.
    pub last_fetch_end: DateTime<Utc>,

    /// Number of records emitted by the last fetch.
    pub last_fetch_records: u64,

    /// When this cursor was last written.
    pub updated_at: DateTime<Utc>,

    /// The unit checkpoint, JSON-encoded, when `version >= 2`; the opaque API
    /// cursor slot of version 1, which nothing wrote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_cursor: Option<String>,

    /// Schema version: 1 is the window cursor, 2 adds the unit checkpoint.
    pub version: u32,
}

/// Version of [`CursorValue`] whose `api_cursor` holds a [`CheckpointValue`].
pub const CHECKPOINT_CURSOR_VERSION: u32 = 2;

impl CursorValue {
    /// The unit checkpoint this cursor carries, if it is a version-2 cursor with
    /// a readable `api_cursor`. An unreadable value reads as no checkpoint: the
    /// unit then starts over, which is the at-least-once side of the contract.
    #[must_use]
    pub fn checkpoint(&self) -> Option<CheckpointValue> {
        if self.version < CHECKPOINT_CURSOR_VERSION {
            return None;
        }
        self.api_cursor
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
    }
}

/// Trait for cursor persistence backends.
#[async_trait]
pub trait CursorStore: Send + Sync {
    /// Get the cursor value for a key, or `None` if no cursor exists.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Cursor`] when the backend cannot be read.
    async fn get(&self, key: &str) -> Result<Option<CursorValue>>;

    /// Persist a cursor value for a key.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Cursor`] when the value cannot be written.
    async fn set(&self, key: &str, value: &CursorValue) -> Result<()>;

    /// Delete a cursor for a key.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Cursor`] when the backend refuses the delete.
    async fn delete(&self, key: &str) -> Result<()>;
}

/// Normalise a cursor key to lowercase for consistent lookups.
#[must_use]
pub fn normalize_cursor_key(key: &str) -> String {
    key.to_lowercase()
}

/// The committed position of one unit, one kind per shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CheckpointValue {
    /// Manifest shape: the last item fully emitted, in `(position, key)` order.
    Item {
        /// The item's identity, which orders items sharing one position.
        key: String,
        /// Its position in the listing order.
        position: DateTime<Utc>,
    },
    /// Tail shape: the ordering key tuple of the last emitted row.
    Keyset(Vec<serde_json::Value>),
    /// File tail: `(file_id, end_offset)` per file, in first-seen order.
    Lines(Vec<(u64, u64)>),
}

/// Folds the [`Mark`]s of one unit's tick into the value to commit after ack.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    unit_key: String,
    value: Option<CheckpointValue>,
    acks: Vec<Box<str>>,
    inconsistent: bool,
}

impl Checkpoint {
    /// An empty checkpoint for the unit stored under `unit_key`.
    #[must_use]
    pub fn new(unit_key: impl Into<String>) -> Self {
        Self {
            unit_key: unit_key.into(),
            value: None,
            acks: Vec::new(),
            inconsistent: false,
        }
    }

    /// The key the folded value is committed under.
    #[must_use]
    pub fn unit_key(&self) -> &str {
        &self.unit_key
    }

    /// Fold one row's mark.
    pub fn fold(&mut self, mark: Mark) {
        match (mark, &mut self.value) {
            (Mark::Ack(id), _) => self.acks.push(id),
            (Mark::Keyset(values), slot @ (None | Some(CheckpointValue::Keyset(_)))) => {
                *slot = Some(CheckpointValue::Keyset(values.into_vec()));
            }
            (Mark::Item { key, position }, None) => {
                self.value = Some(CheckpointValue::Item {
                    key: key.into_string(),
                    position,
                });
            }
            (
                Mark::Item { key, position },
                Some(CheckpointValue::Item {
                    key: seen_key,
                    position: seen,
                }),
            ) => {
                // The greatest `(position, key)` pair, the order a listing sorts
                // by, so items sharing one position are ordered by key.
                if (position, &*key) > (*seen, seen_key.as_str()) {
                    *seen_key = key.into_string();
                    *seen = position;
                }
            }
            (
                Mark::Line {
                    file_id,
                    end_offset,
                },
                None,
            ) => {
                self.value = Some(CheckpointValue::Lines(vec![(file_id, end_offset)]));
            }
            (
                Mark::Line {
                    file_id,
                    end_offset,
                },
                Some(CheckpointValue::Lines(files)),
            ) => match files.iter_mut().find(|(id, _)| *id == file_id) {
                Some((_, offset)) => *offset = (*offset).max(end_offset),
                None => files.push((file_id, end_offset)),
            },
            (Mark::Keyset(_) | Mark::Item { .. } | Mark::Line { .. }, Some(_)) => {
                self.inconsistent = true;
            }
        }
    }

    /// The folded value so far.
    #[must_use]
    pub fn value(&self) -> Option<&CheckpointValue> {
        self.value.as_ref()
    }

    /// Whether marks of two different kinds were folded, which no shape does.
    ///
    /// The second kind is not folded, so the value here is only the first
    /// kind's: the driver refuses to commit it rather than record a position
    /// the unit never reached.
    #[must_use]
    pub fn is_inconsistent(&self) -> bool {
        self.inconsistent
    }

    /// Take the acknowledgement ids collected since the last take.
    pub fn take_acks(&mut self) -> Vec<Box<str>> {
        std::mem::take(&mut self.acks)
    }

    /// The version-2 cursor to write for what is folded so far, or `None`
    /// when nothing was. Folding continues afterwards, so a driver that
    /// commits per item can keep one checkpoint for the tick.
    #[must_use]
    pub fn cursor(&self, now: DateTime<Utc>, records: u64) -> Option<CursorValue> {
        let value = self.value.as_ref()?;
        let api_cursor = serde_json::to_string(value).ok()?;
        Some(CursorValue {
            cursor_key: self.unit_key.clone(),
            last_fetch_end: now,
            last_fetch_records: records,
            updated_at: now,
            api_cursor: Some(api_cursor),
            version: CHECKPOINT_CURSOR_VERSION,
        })
    }

    /// The version-2 cursor to write, or `None` when nothing was folded.
    #[must_use]
    pub fn into_cursor(self, now: DateTime<Utc>, records: u64) -> Option<CursorValue> {
        self.cursor(now, records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Mark;
    use chrono::TimeZone;
    use serde_json::json;
    use smallvec::smallvec;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).single().unwrap()
    }

    #[test]
    fn cursor_value_v1_round_trips_and_skips_absent_api_cursor() {
        let value = CursorValue {
            cursor_key: "inst.aws".into(),
            last_fetch_end: at(1_700_000_000),
            last_fetch_records: 42,
            updated_at: at(1_700_000_001),
            api_cursor: None,
            version: 1,
        };
        let json = serde_json::to_string(&value).unwrap();
        assert!(!json.contains("api_cursor"));
        let back: CursorValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back, value);
        assert!(
            back.checkpoint().is_none(),
            "a v1 cursor carries no unit checkpoint"
        );
    }

    #[test]
    fn keyset_fold_keeps_the_last_row_in_key_order() {
        let mut cp = Checkpoint::new("inst.db.events");
        cp.fold(Mark::Keyset(smallvec![json!(1), json!("a")]));
        cp.fold(Mark::Keyset(smallvec![json!(2), json!("b")]));
        cp.fold(Mark::Keyset(smallvec![json!(3), json!("c")]));
        assert_eq!(
            cp.value(),
            Some(&CheckpointValue::Keyset(vec![json!(3), json!("c")]))
        );
    }

    #[test]
    fn item_fold_keeps_the_latest_position_not_the_last_seen() {
        let mut cp = Checkpoint::new("inst.m365.audit");
        cp.fold(Mark::Item {
            key: "blob-b".into(),
            position: at(200),
        });
        cp.fold(Mark::Item {
            key: "blob-a".into(),
            position: at(100),
        });
        assert_eq!(
            cp.value(),
            Some(&CheckpointValue::Item {
                key: "blob-b".to_string(),
                position: at(200),
            })
        );
    }

    #[test]
    fn item_fold_breaks_a_tie_at_one_position_by_key() {
        let mut cp = Checkpoint::new("inst.m365.audit");
        for key in ["blob-a", "blob-c", "blob-b"] {
            cp.fold(Mark::Item {
                key: key.into(),
                position: at(100),
            });
        }
        assert_eq!(
            cp.value(),
            Some(&CheckpointValue::Item {
                key: "blob-c".to_string(),
                position: at(100),
            }),
            "the greatest key at the position, the order a listing sorts by"
        );
    }

    #[test]
    fn line_fold_keeps_the_furthest_offset_per_file() {
        let mut cp = Checkpoint::new("inst.file.logs");
        cp.fold(Mark::Line {
            file_id: 7,
            end_offset: 10,
        });
        cp.fold(Mark::Line {
            file_id: 9,
            end_offset: 5,
        });
        cp.fold(Mark::Line {
            file_id: 7,
            end_offset: 30,
        });
        assert_eq!(
            cp.value(),
            Some(&CheckpointValue::Lines(vec![(7, 30), (9, 5)]))
        );
    }

    #[test]
    fn acks_are_collected_and_taken_per_flush_not_folded_into_the_cursor() {
        let mut cp = Checkpoint::new("inst.pubsub.sub");
        cp.fold(Mark::Ack("a".into()));
        cp.fold(Mark::Ack("b".into()));
        assert!(cp.value().is_none());
        assert_eq!(cp.take_acks(), vec![Box::from("a"), Box::from("b")]);
        assert!(cp.take_acks().is_empty(), "taking drains");
    }

    #[test]
    fn mixed_marks_on_one_unit_are_a_defect() {
        let mut cp = Checkpoint::new("inst.x");
        cp.fold(Mark::Keyset(smallvec![json!(1)]));
        cp.fold(Mark::Item {
            key: "k".into(),
            position: at(1),
        });
        assert!(cp.is_inconsistent(), "a shape yields one kind of mark");
    }

    #[test]
    fn committing_encodes_a_v2_cursor_the_next_tick_decodes() {
        let mut cp = Checkpoint::new("inst.db.events");
        cp.fold(Mark::Keyset(smallvec![json!(7), json!("z")]));
        let cursor = cp
            .into_cursor(at(500), 12)
            .expect("a folded mark yields a cursor");
        assert_eq!(cursor.version, 2);
        assert_eq!(cursor.cursor_key, "inst.db.events");
        assert_eq!(cursor.last_fetch_records, 12);
        assert_eq!(cursor.last_fetch_end, at(500));
        let api_cursor = cursor.api_cursor.as_deref().expect("encoded checkpoint");
        let raw: serde_json::Value = serde_json::from_str(api_cursor).unwrap();
        assert_eq!(raw["kind"], "keyset");
        assert_eq!(
            cursor.checkpoint(),
            Some(CheckpointValue::Keyset(vec![json!(7), json!("z")]))
        );
    }

    #[test]
    fn a_checkpoint_with_nothing_folded_writes_no_cursor() {
        let cp = Checkpoint::new("inst.rest.dump");
        assert!(cp.cursor(at(1), 0).is_none());
        assert!(cp.into_cursor(at(1), 0).is_none());
    }

    #[test]
    fn a_cursor_taken_mid_tick_carries_what_is_folded_and_folding_goes_on() {
        let mut cp = Checkpoint::new("inst.file.assets");
        cp.fold(Mark::Item {
            key: "first.jsonl".into(),
            position: at(100),
        });
        let first = cp.cursor(at(101), 1).expect("the first item");
        assert!(
            matches!(first.checkpoint(), Some(CheckpointValue::Item { ref key, .. }) if key == "first.jsonl")
        );
        cp.fold(Mark::Item {
            key: "second.jsonl".into(),
            position: at(200),
        });
        let second = cp.cursor(at(201), 2).expect("the second item");
        assert!(
            matches!(second.checkpoint(), Some(CheckpointValue::Item { ref key, position }) if key == "second.jsonl" && position == at(200))
        );
        assert_eq!(second.last_fetch_records, 2);
    }

    #[test]
    fn a_v2_cursor_with_unreadable_api_cursor_decodes_to_none_not_a_panic() {
        let cursor = CursorValue {
            cursor_key: "k".into(),
            last_fetch_end: at(1),
            last_fetch_records: 0,
            updated_at: at(1),
            api_cursor: Some("not json".into()),
            version: 2,
        };
        assert!(cursor.checkpoint().is_none());
    }

    #[test]
    fn cursor_keys_normalise_to_lowercase() {
        assert_eq!(normalize_cursor_key("Inst.AWS"), "inst.aws");
        assert_eq!(normalize_cursor_key(""), "");
    }
}
