// Project:   dfe-fetcher
// File:      crates/fetcher/src/lib.rs
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
//! Three input families -- framework sources (a driver per connection over a
//! REST profile, a database or a file shape), container extractors, and
//! Vector.dev extractors -- feed one emit path that enriches, filters, and
//! routes each record to the output transport (Kafka and/or gRPC). See
//! `docs/DESIGN.md` for diagrams and the full data flow.

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

pub mod config;
pub mod credential;
pub mod cursor;
pub mod deployment;
pub mod deployment_catalog;
pub mod driver;
pub mod emit;
pub mod error;
pub mod extractor;
pub mod ingest;
pub mod json_unwrap;
pub mod metrics;
pub mod output;
pub mod pipeline;
pub mod profiles;
pub mod scheduler;

pub use error::{Error, Result};

/// A tracing subscriber the lib tests count log events with.
#[cfg(test)]
pub(crate) mod logged {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Counts the events at `level` from `target` logged on the thread it is
    /// set on, which includes the tasks a current-thread runtime spawns.
    #[derive(Clone)]
    pub(crate) struct Events {
        level: tracing::Level,
        target: &'static str,
        count: Arc<AtomicU64>,
    }

    impl Events {
        pub(crate) fn at(level: tracing::Level, target: &'static str) -> Self {
            Self {
                level,
                target,
                count: Arc::default(),
            }
        }

        pub(crate) fn count(&self) -> u64 {
            self.count.load(Ordering::Relaxed)
        }
    }

    impl tracing::Subscriber for Events {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            let meta = event.metadata();
            if *meta.level() == self.level && meta.target() == self.target {
                self.count.fetch_add(1, Ordering::Relaxed);
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }
}
