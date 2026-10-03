// Project:   dfe-fetcher
// File:      crates/rest/src/hooks/mod.rs
// Purpose:   The row-builder axis: shape transforms no decoder expresses, selected by `rows.builder` or `fold`
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The row-builder axis.
//!
//! A decoder frames a body into rows; a [`RowBuilder`] takes one framed row
//! and yields the rows it holds, reading the request's context (the unit's
//! `vars`, and on a lookup response the `ids` the batch asked for). A
//! folding builder instead takes every row of one key of a unit and yields
//! the one row they fold into. It is the last escape of the profile
//! grammar, a closed vocabulary like the other axes: `rows.builder` (or
//! `fold`) names a variant, each variant is one module with its own test,
//! and adding one is a `match` arm the compiler checks. The listers live
//! beside them: a listing protocol standing where a unit's page fetch would.

mod cloudwatch_metrics;
mod columnar_table;
mod go_module_aggregate;
mod pubsub_message;
pub mod s3_list;
mod wrap_non_object;

use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;

use dfe_fetcher_core::error::Result;

use crate::decode::RowBytes;
use crate::profile::RowBuilderKind;
use crate::profile::template::TemplateCtx;

pub use s3_list::Lister;

/// The transform a unit's framed rows go through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowBuilder {
    /// A columnar table becomes one object per row keyed by column name.
    ColumnarTable,
    /// A CloudWatch `GetMetricData` response joined with the descriptors
    /// the batch asked for: one JSON row per datapoint, or one OTLP
    /// protobuf per response.
    CloudwatchMetrics,
    /// The `.info` documents of one Go module's versions folded into the
    /// one row the proxy source emits per module.
    GoModuleAggregate,
    /// A row that is not a JSON object wrapped as one.
    WrapNonObject,
    /// A Pub/Sub received message decoded into the record it carries.
    PubsubMessage,
}

impl RowBuilder {
    /// The builder a row spec names, if any.
    #[must_use]
    pub fn build(kind: Option<RowBuilderKind>) -> Option<Self> {
        kind.map(|kind| match kind {
            RowBuilderKind::ColumnarTable => RowBuilder::ColumnarTable,
            RowBuilderKind::CloudwatchMetrics => RowBuilder::CloudwatchMetrics,
            RowBuilderKind::GoModuleAggregate => RowBuilder::GoModuleAggregate,
            RowBuilderKind::WrapNonObject => RowBuilder::WrapNonObject,
            RowBuilderKind::PubsubMessage => RowBuilder::PubsubMessage,
        })
    }

    /// The rows one framed row holds, given the request's context.
    ///
    /// # Errors
    ///
    /// Returns [`dfe_fetcher_core::error::Error::Decode`] when the row does
    /// not have the shape the builder expects, or the builder is a fold.
    pub fn expand(self, row: &[u8], ctx: &TemplateCtx) -> Result<Vec<Bytes>> {
        match self {
            RowBuilder::ColumnarTable => columnar_table::expand(row),
            RowBuilder::CloudwatchMetrics => cloudwatch_metrics::expand(row, ctx),
            RowBuilder::WrapNonObject => wrap_non_object::expand(row),
            RowBuilder::PubsubMessage => pubsub_message::expand(row, ctx),
            RowBuilder::GoModuleAggregate => Err(dfe_fetcher_core::error::Error::Decode(
                "go_module_aggregate folds a key's rows; name it under `fold`".into(),
            )),
        }
    }

    /// The one row the rows of a key fold into, given the key's context;
    /// `None` when there is nothing to fold.
    ///
    /// # Errors
    ///
    /// Returns [`dfe_fetcher_core::error::Error::Decode`] when a row does
    /// not have the shape the fold expects, or the builder is not a fold.
    pub fn fold(self, rows: &[Bytes], ctx: &TemplateCtx) -> Result<Option<Bytes>> {
        match self {
            RowBuilder::GoModuleAggregate => go_module_aggregate::fold(rows, ctx),
            RowBuilder::ColumnarTable
            | RowBuilder::CloudwatchMetrics
            | RowBuilder::WrapNonObject
            | RowBuilder::PubsubMessage => Err(dfe_fetcher_core::error::Error::Decode(
                "the builder expands rows one at a time and folds nothing".into(),
            )),
        }
    }

    /// The ids a lookup request carries for a batch: the ids themselves,
    /// unless the builder synthesises the request as well as the rows.
    ///
    /// # Errors
    ///
    /// Returns [`dfe_fetcher_core::error::Error::Decode`] when an id does
    /// not have the shape the builder expects.
    pub fn request_ids(self, ids: &[Value], ctx: &TemplateCtx) -> Result<Vec<Value>> {
        match self {
            RowBuilder::CloudwatchMetrics => Ok(cloudwatch_metrics::queries(ids, ctx)),
            RowBuilder::ColumnarTable
            | RowBuilder::GoModuleAggregate
            | RowBuilder::WrapNonObject
            | RowBuilder::PubsubMessage => Ok(ids.to_vec()),
        }
    }

    /// Every row of a framed stream, expanded in order against `ctx`.
    pub fn expand_stream(self, rows: RowBytes<'_>, ctx: TemplateCtx) -> RowBytes<'_> {
        rows.map(move |row| row.and_then(|row| self.expand(&row, &ctx)))
            .flat_map(|expanded| match expanded {
                Ok(rows) => futures::stream::iter(rows.into_iter().map(Ok)).boxed(),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            })
            .boxed()
    }
}
