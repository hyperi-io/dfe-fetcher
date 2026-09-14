// Project:   dfe-fetcher
// File:      crates/db/src/lib.rs
// Purpose:   Database shapes of the source framework
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Database shapes for the dfe-fetcher source framework.
//!
//! Two shapes over one contract: a DUMP selects a whole store and the driver
//! wraps its rows in the snapshot envelope; a TAIL selects rows past the last
//! committed key tuple (`WHERE key > $last ORDER BY key LIMIT n`, or for
//! MongoDB a change stream resumed from its token) and each row carries its
//! key as `Mark::Keyset`, committed only after the batch is acknowledged.
//! Engines are opt-in features (`odbc`, `clickhouse`, `mongodb`) so a
//! deployment links only the drivers it ships; the config grammar and the
//! shape compile regardless, and an instance naming an engine that is not
//! built fails validation with that reason.
//!
//! What every engine shares lives here: the [`store::Store`] contract, the
//! [`config::DbInstance`] grammar, the [`shape::DbShape`] the driver ticks,
//! the per-dialect keyset predicate ([`keyset`]), the blocking-cursor pump
//! ([`pump`]) that turns a synchronous driver cursor into a bounded stream,
//! and the block-to-row framing ([`lines`]) that leases every buffered block
//! on the memory guard. Streaming is the memory-safety rule, not an
//! optimisation: an engine feeds rows in bounded blocks and stops fetching
//! when the driver stops polling.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::missing_errors_doc)]
#![warn(rustdoc::broken_intra_doc_links)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
pub mod config;
pub mod keyset;
pub mod lines;
#[cfg(feature = "mongodb")]
pub mod mongo;
#[cfg(feature = "odbc")]
pub mod odbc;
pub mod pump;
pub mod secret;
pub mod shape;
pub mod store;

pub use config::{BatchSpec, DbInstance, Engine, StoreShape, StoreSpec, TailMode};
pub use shape::DbShape;
pub use store::{Dialect, Store};
