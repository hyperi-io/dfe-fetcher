// Project:   dfe-fetcher
// File:      tests/integration/mod.rs
// Purpose:   Integration test suite (single binary)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]

#[path = "../common/mod.rs"]
mod common;

mod config;
mod credentials;
mod deployment;
mod pipeline;
mod source_aws;
mod source_azure;
mod source_gcp;
mod source_m365;
