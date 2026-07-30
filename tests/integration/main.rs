// Project:   dfe-fetcher
// File:      tests/integration/mod.rs
// Purpose:   Integration test suite (single binary)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::items_after_statements,
    clippy::field_reassign_with_default,
    clippy::await_holding_lock,
    unsafe_code
)]

#[path = "../common/mod.rs"]
mod common;

mod config;
mod container_hygiene;
mod credentials;
mod deployment;
mod output_kafka;
mod pipeline;
mod source_aws;
mod source_azure;
mod source_gcp;
mod source_m365;
