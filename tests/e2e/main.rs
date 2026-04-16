// Project:   dfe-fetcher
// File:      tests/e2e/mod.rs
// Purpose:   End-to-end test suite (real infrastructure)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
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
mod smoke_cloud;
