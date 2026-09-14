// Project:   dfe-fetcher
// File:      crates/fetcher/src/extractor/mod.rs
// Purpose:   External data extractor management (containers, vector) and their delivery path
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! External extractor management.
//!
//! Manages non-native data extractors that run outside the core fetcher:
//!
//! ## Extraction Modes
//!
//! 1. **Container extractors** (`container/`) -- Isolated containers (Docker/podman)
//!    running third-party tools. Managed as child processes. Each container
//!    outputs JSON lines to stdout or posts to the fetcher's ingest endpoint.
//!    Example: `yet-another-cloudwatch-exporter` for CloudWatch metrics.
//!
//! 2. **Vector extractors** (`vector/`) -- Vector.dev instances configured as
//!    sources that send data to the fetcher via native gRPC (Vector sink protocol).
//!    Tightly coupled via scalo's gRPC support.
//!
//! ## Design Philosophy
//!
//! - If the API is REST-shaped -> a profile under `profiles/` on the framework
//! - If a great OSS tool exists in another language -> wrap in container
//! - One container per source + config (no horizontal scaling needed)
//! - Multiple instances of same type with different configs (e.g., 10 M365 orgs)
//!
//! ## Container Communication
//!
//! Containers communicate with the fetcher via:
//! - **stdout** -- JSON lines protocol (container writes, fetcher reads)
//! - **HTTP POST** -- Container posts to fetcher's `/ingest` endpoint
//! - **gRPC** -- Vector protocol for Vector-based extractors
//!
//! Whichever way a record arrives, it leaves through [`ExtractorSink`]: the
//! pipeline's enrichment, then the same batcher and [`Emitter`] a framework
//! driver flushes through, so an extractor record is leased, dead-lettered
//! and held under backpressure exactly as a fetched row is.

pub mod container;
pub mod vector;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use dfe_fetcher_core::batch::{AccumulateConfig, Batcher, Lease, Outbound};

use crate::driver::GuardLease;
use crate::emit::{EmitReport, Emitter};
use crate::error::Result;
use crate::metrics::Metrics;
use crate::pipeline::PipelineState;

/// Trait for external extractors (containers, vector instances).
///
/// Unlike a framework driver, which pulls, extractors are managed processes
/// that push data to the fetcher. The fetcher starts/stops/monitors them.
#[async_trait]
pub trait Extractor: Send + Sync {
    /// Human-readable extractor name.
    fn name(&self) -> &str;

    /// Unique instance ID (for multiple instances of same extractor type).
    fn instance_id(&self) -> &str;

    /// Start the extractor process.
    async fn start(&self) -> Result<()>;

    /// Stop the extractor process gracefully.
    async fn stop(&self) -> Result<()>;

    /// Check if the extractor is running.
    fn is_running(&self) -> bool;

    /// Check if the extractor is healthy.
    async fn health_check(&self) -> Result<bool>;
}

/// Where an extractor's records go: enriched by the pipeline, buffered on
/// the memory guard, and emitted through the driver's own emit path.
#[derive(Clone)]
pub struct ExtractorSink {
    emitter: Emitter,
    lease: Arc<dyn Lease>,
    accumulate: AccumulateConfig,
    /// Whether a backpressured record is refused at once rather than held
    /// through the emitter's bounded retries.
    immediate: bool,
}

impl ExtractorSink {
    /// A sink over the pipeline's outputs, with the deployment's batch
    /// bounds and oversize policy; a backpressured record is held and
    /// re-sent as a driver's row is.
    #[must_use]
    pub fn new(state: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        let config = state.config();
        let lease: Arc<dyn Lease> = Arc::new(GuardLease(Arc::clone(state.memory_guard())));
        Self {
            emitter: Emitter::new(state, metrics, config.accumulate.in_flight),
            lease,
            accumulate: config.accumulate,
            immediate: false,
        }
    }

    /// The same sink for an intake that answers a waiting client: a
    /// backpressured record is refused at once with [`crate::Error::Backpressured`]
    /// so the client can back off, never held for the emitter's retries.
    #[must_use]
    pub fn immediate(state: Arc<PipelineState>, metrics: Arc<Metrics>) -> Self {
        Self {
            immediate: true,
            ..Self::new(state, metrics)
        }
    }

    /// The pipeline state the sink sends through.
    #[must_use]
    pub fn state(&self) -> &Arc<PipelineState> {
        self.emitter.state()
    }

    /// One record for `topic`, enriched as `source` (the DFE source name the
    /// loader routes on) from `fetcher_source` (the extractor that produced
    /// it).
    #[must_use]
    pub fn record(
        &self,
        source: &str,
        fetcher_source: &str,
        topic: &str,
        payload: Bytes,
    ) -> Outbound {
        let enriched = self.state().enrich_record(payload, source, fetcher_source);
        Outbound::new(topic, enriched)
    }

    /// Emit records as one batch; every record gets a terminal outcome
    /// before this returns.
    ///
    /// # Errors
    ///
    /// As [`Emitter::emit`]: [`crate::Error::Backpressured`] when the output
    /// refuses (after the bounded retries, or at once for an
    /// [`ExtractorSink::immediate`] sink), [`crate::Error::Config`] when no
    /// output is configured, [`crate::Error::Transport`] when the transport
    /// fails or a refused record cannot be dead-lettered.
    pub async fn emit(&self, records: Vec<Outbound>) -> Result<EmitReport> {
        if records.is_empty() {
            return Ok(EmitReport::default());
        }
        let mut batch = Batcher::new(self.accumulate, Arc::clone(&self.lease));
        for record in records {
            batch.push(record, None);
        }
        if self.immediate {
            self.emitter.emit_once(batch.take()).await
        } else {
            self.emitter.emit(batch.take()).await
        }
    }

    /// Enrich and emit one record; see [`ExtractorSink::record`].
    ///
    /// # Errors
    ///
    /// As [`ExtractorSink::emit`].
    pub async fn deliver(
        &self,
        source: &str,
        fetcher_source: &str,
        topic: &str,
        payload: Bytes,
    ) -> Result<()> {
        let record = self.record(source, fetcher_source, topic, payload);
        self.emit(vec![record]).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::{Context, Waker};

    use tokio_util::sync::CancellationToken;

    use crate::config::{Config, SharedConfig};
    use crate::error::Error;

    fn sink() -> ExtractorSink {
        let shared = SharedConfig::new(Config::default());
        let metrics = Arc::new(Metrics::new());
        let state = Arc::new(
            PipelineState::new(shared, Arc::clone(&metrics), None, CancellationToken::new())
                .expect("pipeline state"),
        );
        ExtractorSink::new(state, metrics)
    }

    #[test]
    fn a_record_is_enriched_for_its_topic() {
        let sink = sink();
        let out = sink.record(
            "container-tool",
            "container.my-tool",
            "container-tool_land",
            Bytes::from(r#"{"event":"test"}"#),
        );
        assert_eq!(&*out.topic, "container-tool_land");
        assert!(out.route.is_none());
        let parsed: serde_json::Value = serde_json::from_slice(&out.payload).unwrap();
        assert_eq!(parsed["event"], "test");
        assert_eq!(parsed["_source"], "container-tool");
        assert_eq!(parsed["_source_fetcher"], "container.my-tool");
        assert!(parsed["_timestamp_fetcher"].is_number());
    }

    /// With no output the record has nowhere to go, and the caller hears the
    /// configuration gap rather than the record vanishing into a DLQ.
    #[tokio::test]
    async fn delivery_without_an_output_fails_and_holds_no_memory() {
        let sink = sink();
        let before = sink.state().memory_guard().current_bytes();
        let err = sink
            .deliver("any", "test", "any_land", Bytes::from(r#"{"key":"val"}"#))
            .await
            .expect_err("no output configured");
        assert!(
            matches!(err, Error::Config(_)),
            "the failure names the missing output, got {err:?}"
        );
        assert!(
            err.to_string().contains("Output transport not configured"),
            "{err}"
        );
        assert_eq!(
            sink.state().memory_guard().current_bytes(),
            before,
            "the batch's lease is released once every outcome is known"
        );
        sink.emit(Vec::new())
            .await
            .expect("an empty batch sends nothing");
    }

    /// Bytes stay leased across a pending emit and come back when the future
    /// is dropped there, so an ingest client that disconnects mid-send cannot
    /// leave the guard high. `Box::pin` owns the future, so the scope exit
    /// drops it.
    #[test]
    fn a_dropped_in_flight_batch_releases_its_lease() {
        let sink = sink();
        let guard = Arc::clone(sink.state().memory_guard());
        let before = guard.current_bytes();
        let mut cx = Context::from_waker(Waker::noop());

        {
            let mut in_flight = Box::pin(async {
                let mut batch = Batcher::new(sink.accumulate, Arc::clone(&sink.lease));
                batch.push(Outbound::new("t", vec![b'x'; 4096]), None);
                let _taken = batch.take();
                std::future::pending::<()>().await;
            });
            assert!(
                in_flight.as_mut().poll(&mut cx).is_pending(),
                "the send must still be in flight for this to test anything"
            );
            assert_eq!(
                guard.current_bytes(),
                before + 4096,
                "bytes must stay tracked while the send is in flight"
            );
        }

        assert_eq!(
            guard.current_bytes(),
            before,
            "cancelling the send must return the tracked bytes"
        );
    }
}
