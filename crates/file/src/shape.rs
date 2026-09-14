// Project:   dfe-fetcher
// File:      crates/file/src/shape.rs
// Purpose:   The file shape: one instance's units as the driver ticks them
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The file shape.
//!
//! [`FileShape::from_instance`] turns a validated [`FileInstance`] into one
//! [`FileSource`] per unit -- a [`FileDump`] or, when the tailer is built, a
//! `FileTail` -- and the [`UnitSpec`]s the driver iterates. A dump unit is
//! enveloped by the driver as one snapshot per file (its rows carry the
//! file's item mark); a tail unit is incremental and its rows carry line
//! marks the driver commits after the acks.

use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};

use dfe_fetcher_core::batch::Lease;
use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::{RowSource, RowStream, SnapshotScope, SourceMaturity, TickCtx, UnitSpec};

use crate::FileSource;
use crate::config::{FileInstance, FileUnit, TAIL_FEATURE};
use crate::dump::FileDump;

/// One unit on whichever reader its spec names.
pub enum UnitKind {
    /// Files read once each.
    Dump(Box<FileDump>),
    /// Files followed as they grow.
    #[cfg(feature = "tail")]
    Tail(Box<crate::tail::FileTail>),
}

impl FileSource for UnitKind {
    fn rows<'a>(
        &'a self,
        checkpoint: Option<&'a dfe_fetcher_core::CheckpointValue>,
    ) -> RowStream<'a> {
        match self {
            UnitKind::Dump(d) => d.rows(checkpoint),
            #[cfg(feature = "tail")]
            UnitKind::Tail(t) => t.rows(checkpoint),
        }
    }

    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        match self {
            UnitKind::Dump(d) => d.probe(),
            #[cfg(feature = "tail")]
            UnitKind::Tail(t) => t.probe(),
        }
    }
}

struct Bound {
    unit: UnitSpec,
    kind: UnitKind,
}

/// The file shape of one instance.
pub struct FileShape {
    name: String,
    units: Vec<UnitSpec>,
    bound: Vec<Bound>,
}

impl std::fmt::Debug for FileShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileShape")
            .field("name", &self.name)
            .field("units", &self.units)
            .finish_non_exhaustive()
    }
}

impl FileShape {
    /// Build the shape for `instance` as connection `connection_id`, leasing
    /// buffered chunks on `lease`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] carrying every validation issue, or naming
    /// the feature when a tail unit is configured on a binary without the
    /// tailer.
    pub fn from_instance(
        instance: &FileInstance,
        connection_id: &str,
        lease: &Arc<dyn Lease>,
    ) -> Result<Self> {
        let issues = instance.validate();
        if !issues.is_empty() {
            return Err(Error::Config(issues.join("; ")));
        }
        let mut bound = Vec::with_capacity(instance.units.len());
        for spec in &instance.units {
            let shape = spec.unit_shape();
            let topic = if spec.tail.is_some() {
                instance.topic.clone()
            } else {
                format!("{}-{}", instance.topic, spec.unit)
            };
            let mut unit = UnitSpec::new(&spec.unit, shape, &topic);
            unit.row_key.clone_from(&spec.row_key);
            if spec.dump.is_some() {
                unit.snapshot_scope = SnapshotScope::Item;
            }
            let kind = build_unit(spec, lease)?;
            bound.push(Bound { unit, kind });
        }
        let units = bound.iter().map(|b| b.unit.clone()).collect();
        Ok(Self {
            name: connection_id.to_owned(),
            units,
            bound,
        })
    }

    /// The reader behind a unit name.
    #[must_use]
    pub fn unit(&self, unit: &str) -> Option<&UnitKind> {
        self.bound
            .iter()
            .find(|b| &*b.unit.name == unit)
            .map(|b| &b.kind)
    }

    /// Stop every tailer, writing its final checkpoints; a no-op for dumps.
    ///
    /// Boxed like [`RowSource::probe`] so the signature is the same whether
    /// or not the tailer is built: without it there is nothing to await.
    pub fn stop(&self) -> BoxFuture<'_, ()> {
        #[cfg(feature = "tail")]
        {
            async move {
                for b in &self.bound {
                    if let UnitKind::Tail(t) = &b.kind {
                        t.stop().await;
                    }
                }
            }
            .boxed()
        }
        #[cfg(not(feature = "tail"))]
        {
            futures::future::ready(()).boxed()
        }
    }
}

fn build_unit(spec: &FileUnit, lease: &Arc<dyn Lease>) -> Result<UnitKind> {
    if let Some(dump) = &spec.dump {
        return Ok(UnitKind::Dump(Box::new(FileDump::new(
            &spec.unit,
            dump.clone(),
            Arc::clone(lease),
        ))));
    }
    #[cfg(feature = "tail")]
    if let Some(tail) = &spec.tail {
        return Ok(UnitKind::Tail(Box::new(crate::tail::FileTail::new(
            &spec.unit,
            tail.clone(),
            Arc::clone(lease),
        )?)));
    }
    Err(Error::Config(format!(
        "unit `{}`: the file tailer is not built into this binary; build with `--features {TAIL_FEATURE}`",
        spec.unit
    )))
}

impl RowSource for FileShape {
    fn name(&self) -> &str {
        &self.name
    }

    fn maturity(&self) -> SourceMaturity {
        SourceMaturity::Alpha
    }

    fn units(&self) -> &[UnitSpec] {
        &self.units
    }

    fn rows<'a>(&'a self, tick: TickCtx<'a>) -> RowStream<'a> {
        let Some(bound) = self.bound.iter().find(|b| b.unit.name == tick.unit.name) else {
            let name = tick.unit.name.clone();
            return futures::stream::once(async move {
                Err(Error::Config(format!(
                    "unit `{name}` is not a unit of this instance"
                )))
            })
            .boxed();
        };
        bound.kind.rows(tick.checkpoint)
    }

    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        async move {
            for b in &self.bound {
                b.kind.probe().await?;
            }
            Ok(())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dfe_fetcher_core::UnitShape;
    use dfe_fetcher_core::batch::NoLease;

    fn no_lease() -> Arc<dyn Lease> {
        Arc::new(NoLease)
    }

    fn instance(yaml: &str) -> FileInstance {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    #[test]
    fn units_bind_with_the_db_topic_convention() {
        let inst = instance(
            r#"
topic: exports
units:
  - unit: assets
    dump: { paths: ["/data/*.jsonl"] }
    row_key: "/id"
  - unit: logs
    tail: { include: ["/var/log/*.log"], data_dir: "/tmp/never-created" }
"#,
        );
        let built = FileShape::from_instance(&inst, "exp", &no_lease());
        if crate::tail_is_built() {
            let shape = built.expect("tail is built");
            assert_eq!(shape.name(), "exp");
            assert_eq!(shape.maturity(), SourceMaturity::Alpha);
            let units = shape.units();
            assert_eq!(units.len(), 2);
            assert_eq!(&*units[0].name, "assets");
            assert_eq!(units[0].shape, UnitShape::Dump);
            assert_eq!(&*units[0].topic, "exports-assets");
            assert_eq!(units[0].row_key.as_deref(), Some("/id"));
            assert!(
                units[0].snapshots_per_item(),
                "a directory dump is one snapshot per file"
            );
            assert_eq!(units[1].shape, UnitShape::Incremental);
            assert_eq!(&*units[1].topic, "exports");
            assert!(!units[1].snapshots_per_item());
            assert!(shape.unit("assets").is_some());
            assert!(shape.unit("nope").is_none());
        } else {
            let err = built.expect_err("tail is not built");
            assert!(err.to_string().contains("file-tail"), "{err}");
        }
    }

    #[test]
    fn an_invalid_instance_is_refused_with_its_issues() {
        let inst = instance("units: [{ unit: a, dump: { paths: [x] } }]\n");
        let err = FileShape::from_instance(&inst, "exp", &no_lease()).unwrap_err();
        assert!(err.to_string().contains("topic: is required"), "{err}");
    }

    #[tokio::test]
    async fn a_unit_the_shape_does_not_own_is_a_config_error_stream() {
        let inst =
            instance("topic: t\nunits: [{ unit: a, dump: { paths: [\"/nowhere/*.jsonl\"] } }]\n");
        let shape = FileShape::from_instance(&inst, "exp", &no_lease()).unwrap();
        let stray = UnitSpec::new("stray", UnitShape::Dump, "t");
        let tick = TickCtx {
            window: None,
            connection_id: "exp",
            unit: &stray,
            checkpoint: None,
        };
        let err = shape.rows(tick).next().await.unwrap().unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
    }
}
