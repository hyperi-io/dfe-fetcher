// Project:   dfe-fetcher
// File:      crates/fetcher/tests/e2e/main.rs
// Purpose:   End-to-end test suite (real infrastructure)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::items_after_statements
)]

#[path = "../common/mod.rs"]
mod common;

mod container;
mod kafka;
mod kafka_cursor;
mod runzero;
mod smoke_remote;
