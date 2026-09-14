// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/main.rs
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

mod batcher_measure;
mod builtin_run;
mod config;
mod config_db;
mod config_reachability;
mod config_rest;
mod container_hygiene;
mod credentials;
#[cfg(feature = "db-clickhouse")]
mod db_clickhouse;
#[cfg(feature = "db-mongodb")]
mod db_mongo;
#[cfg(feature = "db-odbc")]
mod db_odbc;
#[cfg(feature = "db-odbc")]
mod db_odbc_kafka;
mod deployment;
mod file_config;
mod file_dump;
#[cfg(feature = "file-tail")]
mod file_tail;
mod framework;
mod framework_kafka;
mod output_kafka;
mod pipeline;
mod saas_provider;
mod source_aws;
mod source_azure;
mod source_bitwarden;
mod source_cloudflare;
mod source_crates_io;
mod source_crowdstrike;
mod source_duo;
mod source_gcp;
mod source_gcp_pubsub;
mod source_github;
mod source_go_modules;
mod source_google_workspace;
mod source_m365;
mod source_object_store;
mod source_okta;
mod source_onepassword;
mod source_pypi;
mod source_salesforce;
mod source_slack;
