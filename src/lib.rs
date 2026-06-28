// Project:   dfe-fetcher
// File:      src/lib.rs
// Purpose:   Library root with public exports
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! dfe-fetcher: Data fetcher for external services.
//!
//! Fetches security and operational data from cloud services (AWS, Azure,
//! M365, GCP) and external extractors, delivering to the DFE pipeline
//! via Kafka and/or gRPC.
//!
//! ## Architecture
//!
//! Three input families -- native sources, container extractors, and Vector.dev
//! extractors -- feed a common pipeline that enriches, filters, and routes each
//! record to the output transport (Kafka and/or gRPC). See `docs/DESIGN.md` for
//! diagrams and the full data flow.

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

// buffer module removed — replaced by scalo::memory::MemoryGuard
pub mod config;
pub mod credential;
pub mod cursor;
pub mod deployment;
pub mod error;
pub mod extractor;
pub mod ingest;
pub mod json_unwrap;
pub mod metrics;
pub mod output;
pub mod pipeline;
pub mod scheduler;
pub mod source;

pub use error::{Error, Result};
