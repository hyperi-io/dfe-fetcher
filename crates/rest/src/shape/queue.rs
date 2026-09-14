// Project:   dfe-fetcher
// File:      crates/rest/src/shape/queue.rs
// Purpose:   The queue shape: pull, deliver, then acknowledge
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The queue shape.
//!
//! A queue unit's page is one pull of messages; each framed row carries its
//! acknowledgement id at the profile's `ack_at`, which becomes the row's
//! [`Mark::Ack`] before the builder turns the message into the record it
//! holds. The driver folds the acks of every batch it flushes and hands
//! them back through [`RowSource::ack`] only after the transport has taken
//! the batch, so a message the transport never took is never acknowledged
//! and the broker redelivers it. A tick pulls again while a pull yields
//! messages, up to the unit's `max_pages`. The requests, the framing and the
//! credential are the REST shape's; this shape owns the mark and the ack.

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde_json::{Value, json};

use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::metric_names;
use dfe_fetcher_core::{Mark, Row, RowSource, RowStream, SourceMaturity, TickCtx, UnitSpec};

use super::{RestShape, Stage};
use crate::page::Pager;
use crate::profile::bound::{BoundEndpoint, BoundQueue};
use crate::profile::template::TemplateCtx;

/// The queue shape of one instance.
#[derive(Debug)]
pub struct QueueShape {
    rest: RestShape,
}

impl QueueShape {
    /// The queue shape over a bound REST shape whose units are all queues.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] naming a unit that declares no queue.
    pub fn new(rest: RestShape) -> Result<Self> {
        if let Some(unit) = rest.bound().endpoints.iter().find(|e| e.queue.is_none()) {
            return Err(Error::Config(format!(
                "unit `{}` declares no `construct.queue`; the queue shape runs queues only",
                unit.unit.name
            )));
        }
        Ok(Self { rest })
    }

    /// The REST shape underneath, for the instance's connection id and
    /// bound profile.
    #[must_use]
    pub fn rest(&self) -> &RestShape {
        &self.rest
    }

    fn endpoint(&self, unit: &str) -> Result<(&BoundEndpoint, &BoundQueue)> {
        let endpoint = self.rest.bound.endpoint(unit);
        endpoint
            .and_then(|e| e.queue.as_ref().map(|q| (e, q)))
            .ok_or_else(|| Error::Config(format!("unit `{unit}` is not a queue of this profile")))
    }

    /// One pull: the messages the unit's page request answers, each row
    /// expanded by the builder and its last row marked with the message's
    /// ack id.
    async fn pull(
        &self,
        endpoint: &BoundEndpoint,
        queue: &BoundQueue,
        ctx: &TemplateCtx,
        bytes: metrics::Counter,
    ) -> Result<Vec<Row>> {
        let stage = Stage {
            builder: None,
            ..Stage::pages(endpoint)
        };
        let mut page = self
            .rest
            .fetch(endpoint, stage, ctx, &Pager::None.first(), bytes)
            .await?;
        let mut rows = Vec::new();
        while let Some(message) = page.rows.next().await {
            let message = message?;
            let value: Value = serde_json::from_slice(&message)
                .map_err(|e| Error::Decode(format!("queue message is not JSON: {e}")))?;
            let ack: Box<str> = value
                .pointer(&queue.ack_at)
                .and_then(Value::as_str)
                .map(Box::from)
                .ok_or_else(|| {
                    Error::Decode(format!(
                        "queue message carries no ack id at `{}`",
                        queue.ack_at
                    ))
                })?;
            let expanded = match endpoint.builder {
                Some(builder) => builder.expand(&message, ctx)?,
                None => vec![message],
            };
            let last = expanded.len().saturating_sub(1);
            rows.extend(expanded.into_iter().enumerate().map(|(i, payload)| Row {
                payload,
                mark: (i == last).then(|| Mark::Ack(ack.clone())),
            }));
        }
        Ok(rows)
    }
}

impl RowSource for QueueShape {
    fn name(&self) -> &str {
        self.rest.name()
    }

    fn maturity(&self) -> SourceMaturity {
        self.rest.maturity()
    }

    fn units(&self) -> &[UnitSpec] {
        self.rest.units()
    }

    /// Pull until a pull answers nothing or `max_pages` pulls have been
    /// sent, streaming each pull's rows as it lands.
    fn rows<'a>(&'a self, tick: TickCtx<'a>) -> RowStream<'a> {
        let source = self.rest.connection_id().to_owned();
        let records =
            metrics::counter!(metric_names::RECORDS_FETCHED_TOTAL, "source" => source.clone());
        let bytes = metrics::counter!(metric_names::BYTES_FETCHED_TOTAL, "source" => source);
        let unit = tick.unit;
        let step = move |(ctx, sent): (Option<TemplateCtx>, u32)| {
            let (records, bytes) = (records.clone(), bytes.clone());
            async move {
                let (endpoint, queue) = self.endpoint(&unit.name)?;
                if sent >= endpoint.max_pages {
                    return Ok::<_, Error>(None);
                }
                let ctx = match ctx {
                    Some(ctx) => ctx,
                    None => self.rest.tick_ctx(endpoint).await?,
                };
                let rows = self.pull(endpoint, queue, &ctx, bytes).await?;
                records.increment(rows.len() as u64);
                if rows.is_empty() {
                    return Ok(None);
                }
                let pulled = stream::iter(rows.into_iter().map(Ok));
                Ok(Some((pulled, (Some(ctx), sent + 1))))
            }
        };
        stream::try_unfold((None, 0), step).try_flatten().boxed()
    }

    /// Send the unit's acknowledgement request for `ids`, `ack_batch` at a
    /// time, with `ids` in the request's context.
    fn ack<'a>(&'a self, unit: &'a UnitSpec, ids: Vec<Box<str>>) -> BoxFuture<'a, Result<()>> {
        async move {
            let (endpoint, queue) = self.endpoint(&unit.name)?;
            let ctx = self.rest.tick_ctx(endpoint).await?;
            for chunk in ids.chunks(queue.ack_batch) {
                let mut ctx = ctx.clone();
                ctx.set("ids", json!(chunk));
                let (url, headers, body) = self.rest.build_parts(
                    &endpoint.base_url,
                    &queue.ack_request,
                    &Pager::None,
                    &Pager::None.first(),
                    &ctx,
                )?;
                self.rest
                    .send_request(
                        self.rest.auth_for(endpoint),
                        &ctx,
                        &queue.ack_request,
                        url,
                        headers,
                        body.as_ref(),
                    )
                    .await?;
            }
            Ok(())
        }
        .boxed()
    }

    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        self.rest.probe()
    }
}
