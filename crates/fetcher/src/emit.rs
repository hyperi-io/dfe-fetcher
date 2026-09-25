// Project:   dfe-fetcher
// File:      crates/fetcher/src/emit.rs
// Purpose:   One flush to the transport: concurrent sends, per-record dead-lettering, bounded retry
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The emitter.
//!
//! One call per batch flush. scalo's Kafka transport awaits each produce's
//! delivery report and has no batch call, so the emitter issues a flush's
//! sends as concurrent futures (`in_flight` at a time) and librdkafka forms its
//! own wire batches behind them. Every record gets a terminal outcome: sent,
//! dead-lettered whole (the transport refused THAT record and would again, and
//! the DLQ confirmed it holds the record -- a refused or unconfirmed dead-letter
//! write aborts the tick like a transport failure), or backpressured -- the
//! backpressured subset is retried with a bounded backoff and, if still
//! refused, the whole tick aborts WITHOUT a checkpoint so the scheduler's
//! stall loop takes over. A failure the transport reports
//! for every record alike (closed, timed out, the topic or broker gone) is
//! not a dead-letter case: the tick aborts with no checkpoint and the rows
//! are re-fetched, which is the at-least-once side of the contract. The
//! batch's memory lease is released when the batch drops, after every
//! outcome is known.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tracing::debug;

use dfe_fetcher_core::batch::{Batch, Outbound};

use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::pipeline::{DeadLetter, DeadLettered, PipelineState};

/// How many times a backpressured subset is re-sent before the tick aborts.
const BACKPRESSURE_RETRIES: u32 = 5;
/// First wait between backpressure retries; doubles each time.
const BACKPRESSURE_BACKOFF: Duration = Duration::from_millis(200);

/// What one flush did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EmitReport {
    /// Records the transport accepted.
    pub sent: u64,
    /// Records that went to the dead-letter queue instead.
    pub dead_lettered: u64,
}

/// Sends batches through the pipeline's output.
#[derive(Clone)]
pub struct Emitter {
    state: Arc<PipelineState>,
    metrics: Arc<Metrics>,
    in_flight: usize,
}

impl Emitter {
    /// An emitter over the pipeline's output transports.
    #[must_use]
    pub fn new(state: Arc<PipelineState>, metrics: Arc<Metrics>, in_flight: usize) -> Self {
        Self {
            state,
            metrics,
            in_flight: in_flight.max(1),
        }
    }

    /// The pipeline state the emitter sends through.
    #[must_use]
    pub fn state(&self) -> &Arc<PipelineState> {
        &self.state
    }

    /// Send every row of `batch`, returning once each has a terminal outcome;
    /// a backpressured subset is re-sent with a bounded backoff first.
    ///
    /// SHORTCUT: per-record `send` futures bounded by `in_flight`, not one
    /// produce call; switch to scalo's native Kafka `send_batch` when it ships.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Backpressured`] when a subset is still refused after
    /// the bounded retries, [`Error::Config`] when no output is configured,
    /// and [`Error::Transport`] when the transport fails for every record or
    /// a refused record cannot be dead-lettered.
    pub async fn emit(&self, batch: Batch) -> Result<EmitReport> {
        self.run(batch, BACKPRESSURE_RETRIES).await
    }

    /// Send every row of `batch` once: a backpressured subset is refused at
    /// once, for an intake that answers a waiting client rather than holding
    /// its request.
    ///
    /// # Errors
    ///
    /// As [`Emitter::emit`], with [`Error::Backpressured`] after the one pass.
    pub async fn emit_once(&self, batch: Batch) -> Result<EmitReport> {
        self.run(batch, 0).await
    }

    async fn run(&self, mut batch: Batch, retries: u32) -> Result<EmitReport> {
        let mut report = EmitReport::default();
        // The rows move out; the batch stays alive to the end so its memory
        // lease is released only once every outcome is known.
        let mut pending: Vec<Outbound> = std::mem::take(&mut batch.rows);
        let total = pending.len();
        let mut backoff = BACKPRESSURE_BACKOFF;
        for attempt in 0..=retries {
            if pending.is_empty() {
                break;
            }
            if attempt > 0 {
                debug!(
                    attempt,
                    held = pending.len(),
                    "re-sending the backpressured subset of a batch"
                );
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
            pending = self.send_round(pending, &mut report).await?;
        }
        if !pending.is_empty() {
            self.metrics.inc_transport_backpressured();
            return Err(Error::Backpressured(format!(
                "{} of {total} records refused after {} attempt(s)",
                pending.len(),
                retries + 1,
            )));
        }
        drop(batch);
        Ok(report)
    }

    /// One concurrent pass over `rows`; returns the backpressured ones.
    async fn send_round(
        &self,
        rows: Vec<Outbound>,
        report: &mut EmitReport,
    ) -> Result<Vec<Outbound>> {
        let mut held = Vec::new();
        let mut refused = Vec::new();
        let mut in_flight = FuturesUnordered::new();
        let mut rows = rows.into_iter();
        loop {
            while in_flight.len() < self.in_flight
                && let Some(row) = rows.next()
            {
                in_flight.push(self.send_one(row));
            }
            let Some((row, outcome)) = in_flight.next().await else {
                break;
            };
            match outcome {
                Ok(()) => {
                    report.sent += 1;
                    self.metrics.inc_records_delivered();
                }
                Err(Error::Backpressured(_)) => held.push(row),
                // The record itself is refused: it goes to the DLQ whole
                // (truncation is the oversize policy, not this path) and the
                // batch goes on.
                Err(Error::TransportRecord(e)) => {
                    self.metrics.inc_transport_send_errors();
                    refused.push(DeadLetter {
                        topic: row.topic,
                        payload: row.payload,
                        reason: format!("transport refused the record: {e}"),
                    });
                }
                // Anything else is the transport's own failure: no record is
                // dead-lettered and the tick aborts with no checkpoint.
                Err(e) => {
                    self.metrics.inc_transport_send_errors();
                    return Err(e);
                }
            }
        }
        self.dead_letter(refused, report).await?;
        Ok(held)
    }

    /// Dead-letter the round's refused records in one confirmed write, so a
    /// record counts as handled only once the DLQ holds it.
    async fn dead_letter(&self, refused: Vec<DeadLetter>, report: &mut EmitReport) -> Result<()> {
        if refused.is_empty() {
            return Ok(());
        }
        let count = refused.len() as u64;
        let first_topic = Arc::clone(&refused[0].topic);
        match self.state.dead_letter(refused).await {
            Ok(DeadLettered::Held) => {
                report.dead_lettered += count;
                Ok(())
            }
            Ok(DeadLettered::NoQueue) => Err(Error::Transport(format!(
                "{count} record(s) to {first_topic} refused and no dead-letter queue is configured"
            ))),
            Err(e) => Err(Error::Transport(format!(
                "{count} record(s) to {first_topic} refused and could not be dead-lettered: {e}"
            ))),
        }
    }

    async fn send_one(&self, row: Outbound) -> (Outbound, Result<()>) {
        let outcome = match &row.route {
            Some(names) => {
                let names: Vec<&str> = names.iter().map(|n| &**n).collect();
                self.state
                    .send_to(&names, &row.topic, row.payload.clone())
                    .await
            }
            None => self.state.send_all(&row.topic, row.payload.clone()).await,
        };
        (row, outcome)
    }
}
