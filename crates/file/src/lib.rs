// Project:   dfe-fetcher
// File:      crates/file/src/lib.rs
// Purpose:   File shapes of the source framework
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! File shapes for the dfe-fetcher source framework.
//!
//! A DUMP reads each file a glob matches once, framing NDJSON, a JSON array or
//! CSV (gzip by magic) into rows, lands each file as its own snapshot, and
//! marks the file done on the cursor store with
//! `Mark::Item { key: path, position: ctime }` after its last row is
//! acknowledged. A TAIL follows growing files through rotation and truncation
//! and commits `(file fingerprint, end offset)` per line only after the batch
//! carrying it is acknowledged; it rides a vendored file tailer behind the
//! `tail` feature and keeps that tailer's own checkpoint file.
//!
//! What both share lives here: the [`FileSource`] contract, the
//! [`config::FileInstance`] grammar and the [`shape::FileShape`] the driver
//! ticks. Every byte a reader holds ahead of the driver is leased on the
//! memory guard, so an unpolled stream is a stalled read, never a growing
//! buffer.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::missing_errors_doc)]
#![warn(rustdoc::broken_intra_doc_links)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]

pub mod config;
pub mod dump;
pub mod shape;
pub mod tail;

use futures::future::BoxFuture;

use dfe_fetcher_core::RowStream;
use dfe_fetcher_core::checkpoint::CheckpointValue;
use dfe_fetcher_core::error::Result;

pub use config::{FileInstance, FileUnit, TAIL_FEATURE, tail_is_built};
pub use dump::{DumpDecoder, DumpSpec, FileDump};
pub use shape::FileShape;
pub use tail::{TailDecoder, TailSpec};

/// A file-backed unit the driver can pull rows from.
///
/// Both shapes read under a bound: a dump holds one chunk per open file, a
/// tail one read pass, and each is leased on the memory guard until its rows
/// are out. Rows are the file's bytes for NDJSON and JSON arrays, and objects
/// built from the header for CSV.
///
/// # Errors
///
/// `rows` yields `Err` for an unreadable path, a file that does not frame, or
/// a checkpoint of another shape's kind; the driver aborts the tick without
/// marking anything done. `probe` returns `Err` when a configured path is not
/// there to read or the tailer cannot keep its checkpoint file.
pub trait FileSource: Send + Sync {
    /// The rows of one tick, given the unit's last committed checkpoint.
    fn rows<'a>(&'a self, checkpoint: Option<&'a CheckpointValue>) -> RowStream<'a>;

    /// Check the configured paths are usable.
    fn probe(&self) -> BoxFuture<'_, Result<()>>;
}
