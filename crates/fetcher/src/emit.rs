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
//! write aborts the tick like a transport failure), dropped (refused the same
//! way with no DLQ configured, counted in
//! `pipeline_dead_letters_dropped_total{reason="transport_refused"}`, so a
//! record the transport always refuses cannot hold its source back for good),
//! or backpressured -- the backpressured subset is retried with a bounded
//! backoff and, if still refused, the whole tick aborts WITHOUT a checkpoint
//! so the scheduler's stall loop takes over. A failure the transport reports
//! for every record alike (closed, timed out, the topic or broker gone) is
//! not a dead-letter case: the tick aborts with no checkpoint and the rows
//! are re-fetched, which is the at-least-once side of the contract. The
//! batch's memory lease is released when the batch drops, after every
//! outcome is known.
//!
//! A flush that settles adds the bytes of the records it sent to
//! `usage_ingested_bytes_total{org_id}`. A dead-lettered or dropped record is
//! not counted, and nor is any record of a flush that aborts, because the
//! re-fetch sends it again.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tracing::{debug, warn};

use dfe_fetcher_core::batch::{Batch, Outbound};

use crate::error::{Error, Result};
use crate::metrics::{DroppedDeadLetter, Metrics};
use crate::pipeline::{DeadLetter, DeadLettered, PipelineState};

/// How many times a backpressured subset is re-sent before the tick aborts.
const BACKPRESSURE_RETRIES: u32 = 5;
/// First wait between backpressure retries; doubles each time.
const BACKPRESSURE_BACKOFF: Duration = Duration::from_millis(200);
/// Least time between two warnings of refused records dropped.
const DROPPED_WARN_EVERY_MS: u64 = 10_000;
/// When refused records dropped were last warned of.
static DROPPED_WARNED_AT: AtomicU64 = AtomicU64::new(0);

/// Payload bytes the outputs accepted, by the org each record was stamped with.
pub const USAGE_INGESTED_BYTES_TOTAL: &str = "usage_ingested_bytes_total";

/// What one flush did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EmitReport {
    /// Records the transport accepted.
    pub sent: u64,
    /// Records that went to the dead-letter queue instead.
    pub dead_lettered: u64,
    /// Records the transport refused that no dead-letter queue could take.
    pub dropped: u64,
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
    /// a backpressured subset is re-sent with a bounded backoff first. What
    /// it sends is counted under the pipeline's org, read now.
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
        let org = self.state.org_id();
        self.run(batch, BACKPRESSURE_RETRIES, &org).await
    }

    /// [`Emitter::emit`] for a batch whose records were stamped with `org`,
    /// so the usage count names the org the records carry.
    ///
    /// # Errors
    ///
    /// As [`Emitter::emit`].
    pub async fn emit_for_org(&self, batch: Batch, org: &str) -> Result<EmitReport> {
        self.run(batch, BACKPRESSURE_RETRIES, org).await
    }

    /// Send every row of `batch` once: a backpressured subset is refused at
    /// once, for an intake that answers a waiting client rather than holding
    /// its request.
    ///
    /// # Errors
    ///
    /// As [`Emitter::emit`], with [`Error::Backpressured`] after the one pass.
    pub async fn emit_once(&self, batch: Batch) -> Result<EmitReport> {
        let org = self.state.org_id();
        self.run(batch, 0, &org).await
    }

    async fn run(&self, mut batch: Batch, retries: u32, org: &str) -> Result<EmitReport> {
        let mut report = EmitReport::default();
        let mut sent_bytes: u64 = 0;
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
            pending = self
                .send_round(pending, &mut report, &mut sent_bytes)
                .await?;
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
        if sent_bytes > 0 {
            metrics::counter!(USAGE_INGESTED_BYTES_TOTAL, "org_id" => org.to_owned())
                .increment(sent_bytes);
        }
        Ok(report)
    }

    /// One concurrent pass over `rows`: returns the backpressured ones and
    /// adds the bytes of the rows sent to `sent_bytes`.
    async fn send_round(
        &self,
        rows: Vec<Outbound>,
        report: &mut EmitReport,
        sent_bytes: &mut u64,
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
                    *sent_bytes += row.payload.len() as u64;
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
    /// record counts as handled only once the DLQ holds it. A refusal is
    /// permanent, so a record with nowhere to go -- no DLQ configured, or one
    /// no DLQ backend can hold -- is dropped and counted rather than refetched
    /// for ever.
    async fn dead_letter(&self, refused: Vec<DeadLetter>, report: &mut EmitReport) -> Result<()> {
        if refused.is_empty() {
            return Ok(());
        }
        let count = refused.len() as u64;
        let first_topic = Arc::clone(&refused[0].topic);
        let first_reason = refused[0].reason.clone();
        match self.state.dead_letter(refused).await {
            Ok(DeadLettered::Held { dropped }) => {
                report.dead_lettered += count - dropped;
                report.dropped += dropped;
                Ok(())
            }
            Ok(DeadLettered::NoQueue) => {
                report.dropped += count;
                self.metrics
                    .add_dead_letters_dropped(DroppedDeadLetter::TransportRefused.as_str(), count);
                if scalo::logger::log_debounced(&DROPPED_WARNED_AT, DROPPED_WARN_EVERY_MS) {
                    warn!(
                        records = count,
                        topic = %first_topic,
                        reason = %first_reason,
                        "Records the transport refused were dropped: no dead-letter queue is \
                         configured. Counted in pipeline_dead_letters_dropped_total"
                    );
                }
                Ok(())
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    use dfe_fetcher_core::batch::{AccumulateConfig, Batcher, NoLease};
    use scalo::transport::filter::{FilterAction, FilterRule};
    use scalo::transport::{MemoryConfig, MemoryTransport};

    use crate::config::{Config, SharedConfig};
    use crate::metrics::recorded::Recorder;
    use crate::output::OutputManager;

    /// An emitter over an in-process transport built from `memory`, with no
    /// DLQ, under the config `yaml`.
    fn emitter_over(yaml: &str, memory: &MemoryConfig) -> Emitter {
        let (config, _) = Config::parse_yaml(yaml, "fetcher.yaml").expect("config");
        let transport = Arc::new(MemoryTransport::new(memory).expect("memory transport"));
        let metrics = Arc::new(Metrics::new());
        let state = Arc::new(PipelineState::for_tests(
            SharedConfig::new(config),
            Arc::clone(&metrics),
            Some(OutputManager::memory(transport)),
        ));
        Emitter::new(state, metrics, 4)
    }

    fn batch(rows: &[&str]) -> Batch {
        let mut batcher = Batcher::new(AccumulateConfig::default(), Arc::new(NoLease));
        for row in rows {
            batcher.push(Outbound::new("fixture_land", (*row).to_owned()), None);
        }
        batcher.take()
    }

    /// The bytes of every record a settled flush sent count once, under the
    /// org the config binds.
    #[tokio::test]
    async fn a_settled_flush_counts_the_bytes_it_sent_under_the_bound_org() {
        let recorder = Recorder::new();
        let _recording = recorder.install();
        let emitter = emitter_over("org_id: acme\n", &MemoryConfig::default());

        let report = emitter
            .emit(batch(&[r#"{"a":1}"#, r#"{"bb":22}"#]))
            .await
            .expect("sent");

        assert_eq!(report.sent, 2);
        assert_eq!(
            recorder.counter("usage_ingested_bytes_total", &[("org_id", "acme")]),
            Some(16),
            "7 + 9 payload bytes"
        );
    }

    /// The driver names the org it stamped, so the count follows the records
    /// rather than a config read later.
    #[tokio::test]
    async fn emit_for_org_counts_under_the_org_it_is_given() {
        let recorder = Recorder::new();
        let _recording = recorder.install();
        let emitter = emitter_over("org_id: acme\n", &MemoryConfig::default());

        emitter
            .emit_for_org(batch(&[r#"{"a":1}"#]), "beta")
            .await
            .expect("sent");

        assert_eq!(
            recorder.counter("usage_ingested_bytes_total", &[("org_id", "beta")]),
            Some(7)
        );
        assert_eq!(
            recorder.counter("usage_ingested_bytes_total", &[("org_id", "acme")]),
            None
        );
    }

    /// A record the transport refuses is not ingested, so only the record it
    /// took is counted, under the built-in default org when none is bound.
    #[tokio::test]
    async fn a_refused_record_is_not_counted() {
        let recorder = Recorder::new();
        let _recording = recorder.install();
        let memory = MemoryConfig {
            filters_out: vec![FilterRule {
                expression: r#"id == "poison""#.into(),
                action: FilterAction::Dlq,
            }],
            ..MemoryConfig::default()
        };
        let emitter = emitter_over("{}\n", &memory);

        let report = emitter
            .emit(batch(&[r#"{"id":"poison"}"#, r#"{"id":"clean"}"#]))
            .await
            .expect("a refusal does not fail the batch");

        assert_eq!((report.sent, report.dropped), (1, 1));
        assert_eq!(
            recorder.counter("usage_ingested_bytes_total", &[("org_id", "default")]),
            Some(14)
        );
    }

    /// A flush that aborts counts nothing, even for the records it sent before
    /// the abort: the tick re-fetches and sends them again.
    #[tokio::test]
    async fn an_aborted_flush_counts_nothing() {
        let recorder = Recorder::new();
        let _recording = recorder.install();
        let emitter = emitter_over(
            "{}\n",
            &MemoryConfig {
                buffer_size: 1,
                ..MemoryConfig::default()
            },
        );

        let err = emitter
            .emit_once(batch(&[r#"{"a":1}"#, r#"{"a":2}"#, r#"{"a":3}"#]))
            .await
            .expect_err("the second record finds the transport full");

        assert!(matches!(err, Error::Backpressured(_)), "{err:?}");
        assert_eq!(
            recorder.counter("usage_ingested_bytes_total", &[("org_id", "default")]),
            None
        );
    }
}
