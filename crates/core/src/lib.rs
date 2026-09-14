// Project:   dfe-fetcher
// File:      crates/core/src/lib.rs
// Purpose:   I/O-free core of the source framework
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Core of the dfe-fetcher source framework.
//!
//! Every source is a shape that turns a tick into a lazily pulled stream of
//! [`Row`]s. This crate holds what every shape and the driver share and nothing
//! that does I/O: the row and checkpoint types, the [`RowSource`] trait, the
//! [`batch::Batcher`], the compiled per-row [`rules::RowRules`], the snapshot
//! [`envelope`] and the [`checkpoint`] contract. HTTP, databases, files and
//! transports live in the sibling crates; the app crate wires them together.
//!
//! The crate depends on tokio for its clock types (the batcher window is
//! testable under `tokio::time::pause`) and its once-cell, and never spawns
//! a task or owns a runtime.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::missing_errors_doc)]
#![warn(rustdoc::broken_intra_doc_links)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]

pub mod batch;
pub mod checkpoint;
pub mod envelope;
pub mod error;
pub mod frame;
pub mod metric_names;
pub mod rules;
pub mod secret;

use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

pub use checkpoint::{CheckpointValue, CursorStore, CursorValue};
pub use error::{Error, Result};

/// Time window for incremental fetching, half-open `[start, end)`.
///
/// `None` in a [`TickCtx`] means the unit is a dump or a queue and has no
/// window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchWindow {
    /// Inclusive start of the window.
    pub start: DateTime<Utc>,
    /// Exclusive end of the window.
    pub end: DateTime<Utc>,
}

/// Release-maturity stage of a source, used for runtime warnings and docs.
///
/// New sources start at `Alpha` until explicitly promoted; the orchestrator
/// warns at startup for every enabled non-stable source.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum SourceMaturity {
    /// Code-complete, not production-validated.
    #[default]
    Alpha,
    /// Live-validated, hardening in progress.
    Beta,
    /// Production-ready.
    Stable,
}

impl std::fmt::Display for SourceMaturity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SourceMaturity::Alpha => "alpha",
            SourceMaturity::Beta => "beta",
            SourceMaturity::Stable => "stable",
        })
    }
}

/// The checkpoint contribution of one row, folded by the driver and committed
/// only after the batch carrying the row is acknowledged by the transport.
#[derive(Debug, Clone, PartialEq)]
pub enum Mark {
    /// Queue shape: acknowledge this id after emit.
    Ack(Box<str>),
    /// Manifest shape: the item this row came from is done once its last row is
    /// acknowledged.
    Item {
        /// The item's identity (object key, content id, file path).
        key: Box<str>,
        /// Where the item sits in the listing order.
        position: DateTime<Utc>,
    },
    /// Tail shape: the ordering key tuple of this row.
    Keyset(SmallVec<[serde_json::Value; 2]>),
    /// File tail: the line's file fingerprint and end offset.
    Line {
        /// The tailer's fingerprint of the file.
        file_id: u64,
        /// Byte offset just past this line.
        end_offset: u64,
    },
}

/// One JSON object as the provider sent it (or as a hook built it), plus the
/// checkpoint contribution of this row, if the shape has one.
///
/// A row with an EMPTY payload and a mark is a checkpoint advance with no
/// record behind it (a change stream's post-batch resume token): the driver
/// folds the mark and emits nothing.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// The row's bytes, verbatim from the provider.
    pub payload: Bytes,
    /// What committing this row means for the unit's checkpoint.
    pub mark: Option<Mark>,
}

impl Row {
    /// A row with no checkpoint contribution.
    #[must_use]
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
            mark: None,
        }
    }

    /// A checkpoint advance carrying no record.
    #[must_use]
    pub fn mark_only(mark: Mark) -> Self {
        Self {
            payload: Bytes::new(),
            mark: Some(mark),
        }
    }

    /// Whether this row advances the checkpoint without a record.
    #[must_use]
    pub fn is_mark_only(&self) -> bool {
        self.payload.is_empty() && self.mark.is_some()
    }
}

/// A lazily pulled stream of rows; polled only when the driver admits the next
/// row, which is what pushes back on the provider.
pub type RowStream<'a> = BoxStream<'a, Result<Row>>;

/// What a unit's rows are: events in a window, or the current state of a store.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum UnitShape {
    /// Rows are events inside the scheduler's window.
    #[default]
    Incremental,
    /// Rows are the whole store; the driver wraps them in the snapshot envelope.
    Dump,
}

/// What a unit's rows are made of, which decides what the driver may do to
/// them on the way to the transport.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RowContent {
    /// One JSON object per row: enriched, filtered, routed and unwrapped.
    #[default]
    Json,
    /// Opaque bytes (an OTLP protobuf): emitted verbatim, so the identity a
    /// JSON row carries in `_source*` and `_timestamp*` is the topic's alone.
    Binary,
}

impl std::str::FromStr for RowContent {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "json" => Ok(RowContent::Json),
            "binary" => Ok(RowContent::Binary),
            other => Err(format!(
                "`{other}` is not a row content kind (json, binary)"
            )),
        }
    }
}

/// What one snapshot of a dump unit covers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SnapshotScope {
    /// The tick: one `begin` / `end` pair per tick, opened before the first
    /// row, so an empty store lands as an empty snapshot.
    #[default]
    Tick,
    /// One [`Mark::Item`] key: a snapshot opens at an item's first row and
    /// closes when the key changes or the tick ends, with `seq` and
    /// `row_count` the item's own and its marks committed as it closes. A
    /// tick that yields no row emits nothing, so an idle directory never
    /// publishes a snapshot that reads as "the store is empty".
    Item,
}

/// One endpoint, store, subscription or prefix of a source, as the driver sees
/// it: the shape-agnostic part of a unit. The shape keeps its own request or
/// query description alongside.
#[derive(Debug, Clone, PartialEq)]
pub struct UnitSpec {
    /// Unit name, unique within the source; the second half of `_source_fetcher`
    /// and of the dump `store` field.
    pub name: Arc<str>,
    /// Whether rows are events or a dump.
    pub shape: UnitShape,
    /// Topic base the unit's rows land on, without the deployment's suffix.
    pub topic: Arc<str>,
    /// JSON pointer to the row's identity, for the oversize stub and logs.
    pub row_key: Option<String>,
    /// Fields added to every row before the rules run (Airbyte `AddFields`).
    pub add_fields: Vec<(String, serde_json::Value)>,
    /// What one snapshot covers; read only when the shape is a dump.
    pub snapshot_scope: SnapshotScope,
    /// What the rows are made of.
    pub content: RowContent,
}

impl UnitSpec {
    /// A unit with just a name, shape and topic.
    #[must_use]
    pub fn new(name: &str, shape: UnitShape, topic: &str) -> Self {
        Self {
            name: Arc::from(name),
            shape,
            topic: Arc::from(topic),
            row_key: None,
            add_fields: Vec::new(),
            snapshot_scope: SnapshotScope::default(),
            content: RowContent::default(),
        }
    }

    /// Whether the driver wraps this unit's rows in the snapshot envelope.
    #[must_use]
    pub fn is_dump(&self) -> bool {
        self.shape == UnitShape::Dump
    }

    /// Whether the rows are opaque bytes the driver must emit verbatim.
    #[must_use]
    pub fn is_binary(&self) -> bool {
        self.content == RowContent::Binary
    }

    /// Whether this dump opens one snapshot per [`Mark::Item`] key rather
    /// than one per tick.
    #[must_use]
    pub fn snapshots_per_item(&self) -> bool {
        self.is_dump() && self.snapshot_scope == SnapshotScope::Item
    }

    /// The `store` value stamped on a dump's envelope: `<connection>.<unit>`.
    #[must_use]
    pub fn store(&self, connection_id: &str) -> String {
        format!("{connection_id}.{}", self.name)
    }
}

/// What a shape gets per tick. Shape-agnostic: a shape holds its own I/O
/// handles, so this never names an HTTP client or a database connection.
#[derive(Debug, Clone, Copy)]
pub struct TickCtx<'a> {
    /// The scheduler's window, `None` for dump and queue units.
    pub window: Option<&'a FetchWindow>,
    /// The instance's connection id: cursor key, metric label, `_source_fetcher` prefix.
    pub connection_id: &'a str,
    /// The unit this tick fetches.
    pub unit: &'a UnitSpec,
    /// The last committed checkpoint for this unit, if the shape keeps one.
    pub checkpoint: Option<&'a CheckpointValue>,
}

/// What one tick of one unit produced, for telemetry and the cursor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TickReport {
    /// Rows emitted (after the filter), oversize stubs included.
    pub rows: u64,
    /// Payload bytes emitted.
    pub bytes: u64,
    /// Rows the filter dropped.
    pub filtered: u64,
    /// Rows replaced by an oversize stub.
    pub oversize: u64,
    /// Batches flushed to the transport.
    pub flushes: u64,
}

impl TickReport {
    /// Fold another unit's report into this one.
    pub fn absorb(&mut self, other: TickReport) {
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.filtered += other.filtered;
        self.oversize += other.oversize;
        self.flushes += other.flushes;
    }
}

/// A shape: turns a tick into a lazily pulled row stream.
///
/// Both async methods return boxed futures and streams so the trait stays
/// dyn-compatible; one virtual `poll_next` per row is the accepted cost on the
/// fetch side.
///
/// # Errors
///
/// `rows` yields `Err` for a page, decode or provider failure; the driver then
/// aborts the tick without a checkpoint. `probe` returns `Err` when credentials
/// cannot be resolved or the provider is unreachable.
pub trait RowSource: Send + Sync {
    /// Source name for logs and the maturity warning.
    fn name(&self) -> &str;

    /// Release-maturity stage of the source.
    fn maturity(&self) -> SourceMaturity;

    /// The units this source fetches, in tick order.
    fn units(&self) -> &[UnitSpec];

    /// The rows of one unit for one tick.
    fn rows<'a>(&'a self, tick: TickCtx<'a>) -> RowStream<'a>;

    /// Acknowledge to the provider the rows of `unit` that carried
    /// [`Mark::Ack`] ids, called once per flush after the transport has
    /// taken the batch; a shape that yields no such marks is never asked.
    fn ack<'a>(&'a self, unit: &'a UnitSpec, ids: Vec<Box<str>>) -> BoxFuture<'a, Result<()>> {
        let _ = (unit, ids);
        Box::pin(std::future::ready(Ok(())))
    }

    /// Health check: credentials resolve and the provider answers.
    fn probe(&self) -> BoxFuture<'_, Result<()>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn row_types_are_send_sync() {
        assert_send_sync::<Row>();
        assert_send_sync::<Mark>();
        assert_send_sync::<UnitSpec>();
        assert_send_sync::<TickReport>();
    }

    #[test]
    fn a_mark_only_row_carries_a_mark_and_no_bytes() {
        let advance = Row::mark_only(Mark::Ack("m".into()));
        assert!(advance.is_mark_only());
        assert!(advance.payload.is_empty());
        assert!(
            !Row::new("{}").is_mark_only(),
            "a record is never mark-only"
        );
        let empty = Row::new("");
        assert!(
            !empty.is_mark_only(),
            "an empty payload without a mark is a (blank) record, not an advance"
        );
    }

    #[test]
    fn store_is_connection_dot_unit() {
        let unit = UnitSpec::new("assets", UnitShape::Dump, "runzero-assets");
        assert_eq!(unit.store("runzero_lab"), "runzero_lab.assets");
        assert!(unit.is_dump());
        assert!(!UnitSpec::new("audit_log", UnitShape::Incremental, "github").is_dump());
    }

    #[test]
    fn a_dump_snapshots_per_tick_unless_scoped_per_item() {
        let per_tick = UnitSpec::new("assets", UnitShape::Dump, "runzero-assets");
        assert_eq!(per_tick.snapshot_scope, SnapshotScope::Tick);
        assert!(!per_tick.snapshots_per_item());
        let per_file = UnitSpec {
            snapshot_scope: SnapshotScope::Item,
            ..UnitSpec::new("assets", UnitShape::Dump, "exports-assets")
        };
        assert!(per_file.snapshots_per_item());
        let manifest = UnitSpec {
            snapshot_scope: SnapshotScope::Item,
            ..UnitSpec::new("blobs", UnitShape::Incremental, "m365")
        };
        assert!(
            !manifest.snapshots_per_item(),
            "an incremental unit has no envelope, whatever its marks"
        );
    }

    #[test]
    fn a_unit_is_json_unless_declared_binary() {
        let unit = UnitSpec::new("metrics", UnitShape::Incremental, "aws");
        assert_eq!(unit.content, RowContent::Json);
        assert!(!unit.is_binary());
        let binary = UnitSpec {
            content: RowContent::Binary,
            ..unit
        };
        assert!(binary.is_binary());
        assert_eq!("json".parse::<RowContent>(), Ok(RowContent::Json));
        assert_eq!("binary".parse::<RowContent>(), Ok(RowContent::Binary));
        let err = "protobuf".parse::<RowContent>().unwrap_err();
        assert!(
            err.contains("`protobuf`") && err.contains("binary"),
            "{err}"
        );
    }

    #[test]
    fn maturity_displays_lowercase_and_round_trips_serde() {
        assert_eq!(SourceMaturity::Alpha.to_string(), "alpha");
        assert_eq!(SourceMaturity::Stable.to_string(), "stable");
        let json = serde_json::to_string(&SourceMaturity::Beta).unwrap();
        assert_eq!(json, "\"beta\"");
        let back: SourceMaturity = serde_json::from_str(&json).unwrap();
        assert_eq!(back, SourceMaturity::Beta);
    }

    #[test]
    fn tick_report_absorbs_every_counter() {
        let mut total = TickReport::default();
        total.absorb(TickReport {
            rows: 1,
            bytes: 2,
            filtered: 3,
            oversize: 4,
            flushes: 5,
        });
        total.absorb(TickReport {
            rows: 10,
            bytes: 20,
            filtered: 30,
            oversize: 40,
            flushes: 50,
        });
        assert_eq!(
            total,
            TickReport {
                rows: 11,
                bytes: 22,
                filtered: 33,
                oversize: 44,
                flushes: 55,
            }
        );
    }
}
