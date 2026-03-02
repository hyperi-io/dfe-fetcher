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
// Allow unwrap/expect in test code
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]

pub mod buffer;
pub mod config;
pub mod error;
pub mod extractor;
pub mod metrics;
pub mod pipeline;
pub mod scheduler;
pub mod sink;
pub mod source;

pub use error::{Error, Result};
