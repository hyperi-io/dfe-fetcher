// Project:   dfe-fetcher
// File:      src/lib.rs
// Purpose:   Library root with public exports
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-fetcher: Data fetcher for external services.
//!
//! Fetches security and operational data from cloud services (AWS, Azure,
//! M365, GCP) and external extractors, delivering to the DFE pipeline
//! via Kafka.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────┐
//! │         Native Sources          │
//! │  (AWS, Azure, M365, GCP)        │
//! └──────────┬──────────────────────┘
//!            │
//! ┌──────────┴──────────────────────┐
//! │       Plugin Sources (.so)      │
//! └──────────┬──────────────────────┘
//!            │
//! ┌──────────┴──────────────────────┐
//! │   Container Extractors          │
//! │  (Docker/podman, any language)  │
//! └──────────┬──────────────────────┘
//!            │
//! ┌──────────┴──────────────────────┐
//! │   Vector.dev Extractors         │
//! │  (gRPC native protocol)        │
//! └──────────┬──────────────────────┘
//!            │
//!            ▼
//! ┌─────────────────────────────────┐
//! │     Pipeline (enrich + route)   │
//! └──────────┬──────────────────────┘
//!            │
//!            ▼
//! ┌─────────────────────────────────┐
//! │   Kafka Sink (TieredSink)       │
//! └─────────────────────────────────┘
//! ```

#![forbid(unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::ignored_unit_patterns)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::format_push_string)]
#![allow(clippy::if_not_else)]
#![allow(clippy::unused_self)]
#![allow(clippy::unused_async)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::single_match_else)]
#![allow(clippy::redundant_closure_for_method_calls)]
// Allow unwrap/expect in test code
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]

pub mod buffer;
pub mod config;
pub mod credential;
pub mod error;
pub mod extractor;
pub mod ingest;
pub mod metrics;
pub mod pipeline;
pub mod scheduler;
pub mod sink;
pub mod source;

pub use error::{Error, Result};
