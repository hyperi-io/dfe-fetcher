// Project:   dfe-fetcher
// File:      crates/db/src/shape.rs
// Purpose:   The database shape: one instance's stores as units the driver ticks
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The database shape.
//!
//! [`DbShape::from_instance`] turns a validated [`DbInstance`] into one
//! [`Store`] per configured store, all on the engine the instance names, and
//! the [`UnitSpec`]s the driver iterates. A dump unit streams the store's
//! rows and the driver wraps them in the snapshot envelope; a tail unit
//! streams the rows past the checkpoint the driver hands in, each carrying its
//! keyset mark, and the driver commits the last one after the acks.

use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use serde_json::Value;

use dfe_fetcher_core::batch::Lease;
use dfe_fetcher_core::checkpoint::CheckpointValue;
use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::metric_names;
use dfe_fetcher_core::{Mark, RowSource, RowStream, SourceMaturity, TickCtx, UnitSpec};

use crate::config::{DbInstance, Engine, StoreShape};
use crate::store::Store;

/// One store on whichever engine the instance built.
pub enum StoreKind {
    /// unixODBC plus the engine's driver.
    #[cfg(feature = "odbc")]
    Odbc(Box<crate::odbc::OdbcStore>),
    /// The ClickHouse HTTP interface.
    #[cfg(feature = "clickhouse")]
    Clickhouse(Box<crate::clickhouse::ChStore>),
    /// The official MongoDB driver.
    #[cfg(feature = "mongodb")]
    Mongodb(Box<crate::mongo::MongoStore>),
}

impl Store for StoreKind {
    fn dump(&self) -> RowStream<'_> {
        match self {
            #[cfg(feature = "odbc")]
            StoreKind::Odbc(s) => s.dump(),
            #[cfg(feature = "clickhouse")]
            StoreKind::Clickhouse(s) => s.dump(),
            #[cfg(feature = "mongodb")]
            StoreKind::Mongodb(s) => s.dump(),
            #[cfg(not(any(feature = "odbc", feature = "clickhouse", feature = "mongodb")))]
            _ => match *self {},
        }
    }

    fn tail(&self, after: Option<Vec<Value>>, limit: u32) -> RowStream<'_> {
        match self {
            #[cfg(feature = "odbc")]
            StoreKind::Odbc(s) => s.tail(after, limit),
            #[cfg(feature = "clickhouse")]
            StoreKind::Clickhouse(s) => s.tail(after, limit),
            #[cfg(feature = "mongodb")]
            StoreKind::Mongodb(s) => s.tail(after, limit),
            #[cfg(not(any(feature = "odbc", feature = "clickhouse", feature = "mongodb")))]
            _ => {
                let _ = (after, limit);
                match *self {}
            }
        }
    }

    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        match self {
            #[cfg(feature = "odbc")]
            StoreKind::Odbc(s) => s.probe(),
            #[cfg(feature = "clickhouse")]
            StoreKind::Clickhouse(s) => s.probe(),
            #[cfg(feature = "mongodb")]
            StoreKind::Mongodb(s) => s.probe(),
            #[cfg(not(any(feature = "odbc", feature = "clickhouse", feature = "mongodb")))]
            _ => match *self {},
        }
    }
}

/// One tick-able store: its unit and how it is fetched.
struct Bound {
    unit: UnitSpec,
    shape: StoreShape,
    limit: u32,
    max_pages_per_tick: u32,
    store: StoreKind,
}

/// The database shape of one instance.
pub struct DbShape {
    name: String,
    units: Vec<UnitSpec>,
    bound: Vec<Bound>,
}

impl std::fmt::Debug for DbShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbShape")
            .field("name", &self.name)
            .field("units", &self.units)
            .finish_non_exhaustive()
    }
}

impl DbShape {
    /// Build the shape for `instance` as connection `connection_id`, leasing
    /// buffered blocks on `lease`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] carrying every validation issue, or naming
    /// an engine this binary was built without.
    pub fn from_instance(
        instance: &DbInstance,
        connection_id: &str,
        lease: &Arc<dyn Lease>,
    ) -> Result<Self> {
        let issues = instance.validate();
        if !issues.is_empty() {
            return Err(Error::Config(issues.join("; ")));
        }
        let secret = Arc::new(crate::secret::Secret::new(
            instance.connection_string.clone(),
        ));
        let mut bound = Vec::with_capacity(instance.stores.len());
        for spec in &instance.stores {
            let topic = match spec.shape {
                StoreShape::Dump => format!("{}-{}", instance.topic, spec.unit),
                StoreShape::Tail => instance.topic.clone(),
            };
            let mut unit = UnitSpec::new(&spec.unit, spec.shape.unit_shape(), &topic);
            unit.row_key.clone_from(&spec.row_key);
            let store = build_store(instance, spec, &secret, lease)?;
            bound.push(Bound {
                unit,
                shape: spec.shape,
                limit: spec.limit,
                max_pages_per_tick: spec.max_pages_per_tick,
                store,
            });
        }
        let units = bound.iter().map(|b| b.unit.clone()).collect();
        Ok(Self {
            name: connection_id.to_owned(),
            units,
            bound,
        })
    }

    /// The store behind a unit name.
    #[must_use]
    pub fn store(&self, unit: &str) -> Option<&StoreKind> {
        self.bound
            .iter()
            .find(|b| &*b.unit.name == unit)
            .map(|b| &b.store)
    }
}

/// The store for `spec` on the instance's engine; an engine this binary was
/// built without answers with the feature that would build it.
fn build_store(
    instance: &DbInstance,
    spec: &crate::config::StoreSpec,
    secret: &Arc<crate::secret::Secret>,
    lease: &Arc<dyn Lease>,
) -> Result<StoreKind> {
    #[cfg(not(any(feature = "odbc", feature = "clickhouse", feature = "mongodb")))]
    let _ = (spec, secret, lease);
    match instance.engine {
        #[cfg(feature = "odbc")]
        Engine::Odbc => {
            let dialect = instance
                .dialect()
                .ok_or_else(|| Error::Config("dialect: is required for the odbc engine".into()))?;
            Ok(StoreKind::Odbc(Box::new(crate::odbc::OdbcStore::new(
                &spec.unit,
                Arc::clone(secret),
                dialect,
                spec.query.clone(),
                spec.key.clone(),
                instance.batch,
                Arc::clone(lease),
            ))))
        }
        #[cfg(not(feature = "odbc"))]
        Engine::Odbc => Err(not_built(Engine::Odbc)),
        #[cfg(feature = "clickhouse")]
        Engine::Clickhouse => Ok(StoreKind::Clickhouse(Box::new(
            crate::clickhouse::ChStore::new(
                &spec.unit,
                Arc::clone(secret),
                spec.query.clone(),
                spec.key.clone(),
                Arc::clone(lease),
            ),
        ))),
        #[cfg(not(feature = "clickhouse"))]
        Engine::Clickhouse => Err(not_built(Engine::Clickhouse)),
        #[cfg(feature = "mongodb")]
        Engine::Mongodb => Ok(StoreKind::Mongodb(Box::new(crate::mongo::MongoStore::new(
            spec,
            Arc::clone(secret),
            instance.batch,
            Arc::clone(lease),
        )?))),
        #[cfg(not(feature = "mongodb"))]
        Engine::Mongodb => Err(not_built(Engine::Mongodb)),
    }
}

#[cfg(not(all(feature = "odbc", feature = "clickhouse", feature = "mongodb")))]
fn not_built(engine: Engine) -> Error {
    Error::Config(format!(
        "engine `{}` is not built into this binary; build with `--features {}`",
        engine.as_str(),
        engine.feature()
    ))
}

/// One tail unit's rows for a tick: pages of `limit` rows, each starting past
/// the last row of the page before, until a short page says the store is
/// caught up or `max_pages` have been read.
///
/// One query per tick would cap a unit at `limit / interval` rows a second
/// whatever the table does, and a table appending faster would fall behind
/// for ever; the page cap bounds the tick instead.
fn tail_pages<S: Store + ?Sized>(
    store: &S,
    after: Option<Vec<Value>>,
    limit: u32,
    max_pages: u32,
    source: String,
    unit: String,
) -> RowStream<'_> {
    /// Where the tick is up to: the page in flight, the key it resumes from,
    /// and what the page before it returned.
    struct Pages<'a, S: ?Sized> {
        store: &'a S,
        after: Option<Vec<Value>>,
        limit: u32,
        cap: u32,
        read: u32,
        rows: u32,
        current: Option<RowStream<'a>>,
        failed: bool,
        source: String,
        unit: String,
    }

    let state = Pages {
        store,
        after,
        limit,
        cap: max_pages,
        read: 0,
        rows: 0,
        current: None,
        failed: false,
        source,
        unit,
    };
    futures::stream::unfold(state, |mut s| async move {
        loop {
            if s.current.is_none() {
                if s.failed {
                    return None;
                }
                // A short page is the store caught up; a full one at the cap
                // leaves the rest for the next tick.
                if s.read > 0 && s.rows < s.limit {
                    return None;
                }
                if s.read >= s.cap {
                    tracing::debug!(
                        source = %s.source,
                        unit = %s.unit,
                        max_pages = s.cap,
                        "tail page cap reached; the rest of the backlog waits for the next tick"
                    );
                    return None;
                }
                s.rows = 0;
                s.read += 1;
                s.current = Some(s.store.tail(s.after.clone(), s.limit));
            }
            let stream = s.current.as_mut()?;
            match stream.next().await {
                Some(Ok(row)) => {
                    s.rows += 1;
                    if let Some(Mark::Keyset(values)) = &row.mark {
                        s.after = Some(values.to_vec());
                    }
                    return Some((Ok(row), s));
                }
                Some(Err(e)) => {
                    s.failed = true;
                    s.current = None;
                    return Some((Err(e), s));
                }
                None => {
                    s.current = None;
                    if s.rows >= s.limit {
                        metrics::counter!(metric_names::TAIL_PAGES_FULL_TOTAL, "source" => s.source.clone(), "unit" => s.unit.clone()).increment(1);
                    }
                }
            }
        }
    })
    .boxed()
}

impl RowSource for DbShape {
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
                    "unit `{name}` is not a store of this instance"
                )))
            })
            .boxed();
        };
        match bound.shape {
            StoreShape::Dump => bound.store.dump(),
            StoreShape::Tail => {
                let after = match tick.checkpoint {
                    Some(CheckpointValue::Keyset(values)) => Some(values.clone()),
                    Some(other) => {
                        let problem = format!(
                            "unit `{}` has a {} checkpoint where a keyset was expected",
                            bound.unit.name,
                            match other {
                                CheckpointValue::Item { .. } => "manifest",
                                CheckpointValue::Lines(_) => "file",
                                CheckpointValue::Keyset(_) => "keyset",
                            }
                        );
                        return futures::stream::once(async move { Err(Error::Cursor(problem)) })
                            .boxed();
                    }
                    None => None,
                };
                tail_pages(
                    &bound.store,
                    after,
                    bound.limit,
                    bound.max_pages_per_tick,
                    self.name.clone(),
                    bound.unit.name.to_string(),
                )
            }
        }
    }

    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        async move {
            match self.bound.first() {
                Some(first) => first.store.probe().await,
                None => Ok(()),
            }
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

    fn instance(engine: &str) -> DbInstance {
        serde_yaml_ng::from_str(&format!(
            r#"
engine: {engine}
dialect: {}
connection_string: "env:DFE_TEST_DSN"
topic: inventory
stores:
  - {{ unit: hosts, shape: dump, query: "SELECT * FROM hosts", row_key: "/id" }}
  - {{ unit: events, shape: tail, query: "SELECT * FROM events", key: [ts, id], limit: 7 }}
"#,
            if engine == "clickhouse" {
                "clickhouse"
            } else {
                "postgres"
            }
        ))
        .unwrap()
    }

    #[test]
    fn a_built_engine_binds_every_store_as_a_unit() {
        let inst = instance("odbc");
        let built = DbShape::from_instance(&inst, "inv", &no_lease());
        if Engine::Odbc.is_built() {
            let shape = built.expect("odbc is built");
            assert_eq!(shape.name(), "inv");
            assert_eq!(shape.maturity(), SourceMaturity::Alpha);
            let units = shape.units();
            assert_eq!(units.len(), 2);
            assert_eq!(&*units[0].name, "hosts");
            assert_eq!(units[0].shape, UnitShape::Dump);
            assert_eq!(&*units[0].topic, "inventory-hosts");
            assert_eq!(units[0].row_key.as_deref(), Some("/id"));
            assert_eq!(units[1].shape, UnitShape::Incremental);
            assert_eq!(&*units[1].topic, "inventory");
            assert!(shape.store("hosts").is_some());
            assert!(shape.store("nope").is_none());
        } else {
            let err = built.expect_err("odbc is not built");
            assert!(err.to_string().contains("db-odbc"), "{err}");
        }
    }

    #[test]
    fn an_invalid_instance_is_refused_with_its_issues() {
        let mut inst = instance("odbc");
        inst.topic.clear();
        let err = DbShape::from_instance(&inst, "inv", &no_lease()).unwrap_err();
        assert!(err.to_string().contains("topic: is required"), "{err}");
    }

    /// A store whose pages the test scripts, recording what each page was
    /// asked to resume from.
    struct Paged {
        pages: std::sync::Mutex<Vec<Vec<i64>>>,
        asked: std::sync::Mutex<Vec<Option<Vec<Value>>>>,
    }

    impl Paged {
        fn new(pages: Vec<Vec<i64>>) -> Self {
            Self {
                pages: std::sync::Mutex::new(pages),
                asked: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl Store for Paged {
        fn dump(&self) -> RowStream<'_> {
            futures::stream::empty().boxed()
        }

        fn tail(&self, after: Option<Vec<Value>>, _limit: u32) -> RowStream<'_> {
            self.asked.lock().unwrap().push(after);
            let mut pages = self.pages.lock().unwrap();
            let page = if pages.is_empty() {
                Vec::new()
            } else {
                pages.remove(0)
            };
            futures::stream::iter(page.into_iter().map(|id| {
                Ok(dfe_fetcher_core::Row {
                    payload: bytes::Bytes::from(format!("{{\"id\":{id}}}")),
                    mark: Some(Mark::Keyset(smallvec::smallvec![Value::from(id)])),
                })
            }))
            .boxed()
        }

        fn probe(&self) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    async fn tailed(store: &Paged, limit: u32, max_pages: u32) -> Vec<i64> {
        tail_pages(store, None, limit, max_pages, "inv".into(), "events".into())
            .map(|row| {
                let row = row.expect("row");
                let value: Value = serde_json::from_slice(&row.payload).unwrap();
                value["id"].as_i64().unwrap()
            })
            .collect()
            .await
    }

    /// A tick keeps reading while the pages come back full, each page
    /// resuming past the last row of the one before, and stops on the first
    /// short page.
    #[tokio::test]
    async fn a_tail_reads_pages_until_a_short_one_and_resumes_from_the_last_row() {
        let store = Paged::new(vec![vec![1, 2], vec![3, 4], vec![5]]);
        assert_eq!(tailed(&store, 2, 10).await, [1, 2, 3, 4, 5]);
        let asked = store.asked.lock().unwrap().clone();
        assert_eq!(asked.len(), 3, "three pages, the last one short");
        assert_eq!(asked[0], None, "the first page starts at the checkpoint");
        assert_eq!(
            asked[1],
            Some(vec![Value::from(2)]),
            "the second page resumes past the last row of the first"
        );
        assert_eq!(asked[2], Some(vec![Value::from(4)]));
    }

    /// The per-tick page cap bounds a store that is behind: the rest of the
    /// backlog waits for the next tick rather than holding this one open.
    #[tokio::test]
    async fn the_page_cap_ends_a_tick_that_is_still_catching_up() {
        let store = Paged::new(vec![vec![1, 2], vec![3, 4], vec![5, 6]]);
        assert_eq!(tailed(&store, 2, 2).await, [1, 2, 3, 4]);
        assert_eq!(
            store.asked.lock().unwrap().len(),
            2,
            "the third page is not requested"
        );
    }

    /// An empty first page is a caught-up store, not a page to follow.
    #[tokio::test]
    async fn a_tail_with_nothing_new_asks_once() {
        let store = Paged::new(vec![vec![]]);
        assert_eq!(tailed(&store, 2, 10).await, [] as [i64; 0]);
        assert_eq!(store.asked.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_unit_the_shape_does_not_own_is_a_config_error_stream() {
        if !Engine::Odbc.is_built() {
            return;
        }
        let inst = instance("odbc");
        let shape = DbShape::from_instance(&inst, "inv", &no_lease()).unwrap();
        let stray = UnitSpec::new("stray", UnitShape::Dump, "t");
        let tick = TickCtx {
            window: None,
            connection_id: "inv",
            unit: &stray,
            checkpoint: None,
        };
        let err = shape.rows(tick).next().await.unwrap().unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
    }
}
