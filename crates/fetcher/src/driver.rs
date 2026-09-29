// Project:   dfe-fetcher
// File:      crates/fetcher/src/driver.rs
// Purpose:   The one generic loop every framework shape runs through: the tick the scheduler runs
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The driver.
//!
//! One [`Driver`] per connection wraps a [`Shape`] and is what the scheduler
//! ticks. A tick runs every unit of the shape through one loop: admission
//! gate, lazily pulled rows, the deployment's nested-JSON unwrap, the
//! snapshot envelope for a dump, oversize stubs, the compiled rules (filter,
//! routes, added fields), enrichment, the batcher, and one emit per flush.
//! The checkpoint is committed only after the last emit is acknowledged; a
//! tick that fails commits nothing more and re-fetches next time. A row the
//! filter drops or the oversize policy replaces with nothing is still
//! CONSUMED: its mark rides the batch that follows so the checkpoint (and a
//! queue's acknowledgement) passes it, otherwise a run of filtered rows would
//! be re-fetched every tick.
//!
//! A dump scoped per tick opens its snapshot before the first row and closes
//! it when the stream ends. A dump scoped per item (a directory of files)
//! opens a snapshot at each item's first row and closes it -- `end` marker,
//! flush, checkpoint -- when the item key changes or a LATER item fails, so a
//! tick with no rows publishes nothing and an item that fails leaves the ones
//! before it complete and committed.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use scalo::governor::{
    Admit, GateActuator, InboundGate, NoopActuator, ObservingActuator, UnifiedPressure,
};
use scalo::memory::MemoryGuard;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use chrono::Utc;
use dfe_fetcher_core::batch::{AccumulateConfig, Batcher, Lease, Outbound, Trigger};
use dfe_fetcher_core::checkpoint::{Checkpoint, CheckpointValue, CursorStore};
use dfe_fetcher_core::envelope::{OversizePolicy, Snapshot};
use dfe_fetcher_core::metric_names;
use dfe_fetcher_core::rules::{Route, RowRules, Verdict};
use dfe_fetcher_core::{
    FetchWindow, Mark, Row, RowSource, RowStream, SourceMaturity, TickCtx, TickReport, UnitSpec,
};
use dfe_fetcher_db::DbShape;
use dfe_fetcher_file::FileShape;
use dfe_fetcher_rest::{ExchangeClient, HttpClient, RestShape};

use crate::config::{Config, SharedConfig};
use crate::emit::{EmitReport, Emitter};
use crate::error::{Error, Result};
use crate::json_unwrap::unwrap_nested_json;
use crate::metrics::{DroppedDeadLetter, Metrics};
use crate::pipeline::{DeadLetter, DeadLettered, SourceNames};

/// How long the driver waits between admission checks while held.
const HOLD_POLL: Duration = Duration::from_millis(250);

/// The runtime-selected shape of a connection.
pub enum Shape {
    /// A declarative REST profile bound to an instance.
    Rest(Box<RestShape>),
    /// A database instance: its stores dumped or tailed on one engine.
    Db(Box<DbShape>),
    /// A file instance: its units dumped by glob or tailed.
    File(Box<FileShape>),
    /// A Rust shape on the framework's primitives.
    Custom(Box<dyn RowSource>),
}

impl Shape {
    /// The shape a REST instance runs on: the queue shape when its profile
    /// declares a queue unit, the REST shape otherwise.
    ///
    /// # Errors
    ///
    /// Returns [`dfe_fetcher_core::Error::Config`] with the field for
    /// anything the profile or the instance gets wrong.
    pub fn for_rest_instance(
        profile: &dfe_fetcher_rest::profile::RestProfile,
        instance: &dfe_fetcher_rest::profile::RestInstance,
        connection_id: &str,
        client: HttpClient,
        exchange: &Arc<ExchangeClient>,
    ) -> dfe_fetcher_core::Result<Self> {
        let rest = RestShape::from_instance(profile, instance, connection_id, client, exchange)?;
        if profile.is_queue() {
            let queue = dfe_fetcher_rest::shape::queue::QueueShape::new(rest)?;
            return Ok(Shape::Custom(Box::new(queue)));
        }
        Ok(Shape::Rest(Box::new(rest)))
    }
}

impl RowSource for Shape {
    fn name(&self) -> &str {
        match self {
            Shape::Rest(s) => s.name(),
            Shape::Db(s) => s.name(),
            Shape::File(s) => s.name(),
            Shape::Custom(s) => s.name(),
        }
    }

    fn maturity(&self) -> SourceMaturity {
        match self {
            Shape::Rest(s) => s.maturity(),
            Shape::Db(s) => s.maturity(),
            Shape::File(s) => s.maturity(),
            Shape::Custom(s) => s.maturity(),
        }
    }

    fn units(&self) -> &[UnitSpec] {
        match self {
            Shape::Rest(s) => s.units(),
            Shape::Db(s) => s.units(),
            Shape::File(s) => s.units(),
            Shape::Custom(s) => s.units(),
        }
    }

    fn rows<'a>(&'a self, tick: TickCtx<'a>) -> RowStream<'a> {
        match self {
            Shape::Rest(s) => s.rows(tick),
            Shape::Db(s) => s.rows(tick),
            Shape::File(s) => s.rows(tick),
            Shape::Custom(s) => s.rows(tick),
        }
    }

    fn ack<'a>(
        &'a self,
        unit: &'a UnitSpec,
        ids: Vec<Box<str>>,
    ) -> futures::future::BoxFuture<'a, dfe_fetcher_core::Result<()>> {
        match self {
            Shape::Rest(s) => s.ack(unit, ids),
            Shape::Db(s) => s.ack(unit, ids),
            Shape::File(s) => s.ack(unit, ids),
            Shape::Custom(s) => s.ack(unit, ids),
        }
    }

    fn probe(&self) -> futures::future::BoxFuture<'_, dfe_fetcher_core::Result<()>> {
        match self {
            Shape::Rest(s) => s.probe(),
            Shape::Db(s) => s.probe(),
            Shape::File(s) => s.probe(),
            Shape::Custom(s) => s.probe(),
        }
    }
}

/// The memory guard as the batcher's lease.
pub struct GuardLease(pub Arc<MemoryGuard>);

impl Lease for GuardLease {
    fn add(&self, bytes: u64) {
        self.0.add_bytes(bytes);
    }

    fn release(&self, bytes: u64) {
        self.0.release(bytes);
    }
}

/// Everything a driver is built from.
pub struct DriverParts {
    /// The shape.
    pub shape: Shape,
    /// Connection id: cursor key, metric label, `_source_fetcher` prefix.
    pub connection_id: String,
    /// The fetcher instance, the first half of every cursor key.
    pub instance_id: String,
    /// Live config, for the per-tick filter, routes and topic suffix.
    pub shared_config: SharedConfig,
    /// Batch bounds.
    pub accumulate: AccumulateConfig,
    /// Oversize policy.
    pub oversize: OversizePolicy,
    /// The emit path.
    pub emitter: Emitter,
    /// The shared pressure latch, when self-regulation is on.
    pub pressure: Option<Arc<UnifiedPressure>>,
    /// The memory guard buffered bytes are leased on.
    pub memory_guard: Arc<MemoryGuard>,
    /// Where unit checkpoints are committed.
    pub checkpoints: Option<Arc<dyn CursorStore>>,
    /// The app's metrics.
    pub metrics: Arc<Metrics>,
    /// Cancelled on shutdown; a tick stops at the next row.
    pub shutdown: CancellationToken,
}

struct RulesCache {
    version: Option<u64>,
    rules: Arc<RowRules>,
}

/// What one tick takes from the live config: read once per tick so a reload
/// mid-tick cannot change the rules between two rows.
struct TickSettings {
    rules: Arc<RowRules>,
    topic_suffix: String,
    /// The deployment's `unwrap_nested_json`: string fields holding
    /// serialised JSON are replaced by the parsed value before the rules run.
    unwrap_nested_json: bool,
    /// The longest the admission gate may hold the unit before it polls on.
    max_hold: Duration,
}

/// The state of one unit's tick that every step of the loop touches: the
/// buffer, the marks folded from what was flushed, and the counters.
struct UnitRun {
    batch: Batcher,
    pending: Checkpoint,
    report: TickReport,
}

/// One connection of one source, as the scheduler ticks it.
pub struct Driver {
    shape: Shape,
    connection_id: String,
    name: &'static str,
    instance_id: String,
    shared_config: SharedConfig,
    rules: parking_lot::Mutex<RulesCache>,
    accumulate: AccumulateConfig,
    oversize: OversizePolicy,
    emitter: Emitter,
    gate: Option<InboundGate>,
    lease: Arc<dyn Lease>,
    checkpoints: Option<Arc<dyn CursorStore>>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
}

impl Driver {
    /// Build a driver. The connection id is interned once so the gate's
    /// metric label and [`Driver::name`] can be `'static`.
    #[must_use]
    pub fn new(parts: DriverParts) -> Self {
        let name: &'static str = Box::leak(parts.connection_id.clone().into_boxed_str());
        let gate = parts.pressure.map(|pressure| {
            let actuator: Box<dyn GateActuator> =
                Box::new(ObservingActuator::new(name, Box::new(NoopActuator)));
            InboundGate::new(pressure, actuator)
        });
        Self {
            shape: parts.shape,
            connection_id: parts.connection_id,
            name,
            instance_id: parts.instance_id,
            shared_config: parts.shared_config,
            rules: parking_lot::Mutex::new(RulesCache {
                version: None,
                rules: Arc::new(RowRules::empty()),
            }),
            accumulate: parts.accumulate,
            oversize: parts.oversize,
            emitter: parts.emitter,
            gate,
            lease: Arc::new(GuardLease(parts.memory_guard)),
            checkpoints: parts.checkpoints,
            metrics: parts.metrics,
            shutdown: parts.shutdown,
        }
    }

    /// The connection id as the scheduler's `'static` log and metric label.
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The maturity the shape declares; the orchestrator warns at startup
    /// for every enabled non-stable source.
    #[must_use]
    pub fn maturity(&self) -> SourceMaturity {
        self.shape.maturity()
    }

    /// The shape's units.
    #[must_use]
    pub fn units(&self) -> &[UnitSpec] {
        self.shape.units()
    }

    /// The shape itself, for a caller that must stop what it runs (a file
    /// tailer's background task) before the driver is dropped.
    #[must_use]
    pub fn shape(&self) -> &Shape {
        &self.shape
    }

    /// The connection id.
    #[must_use]
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    /// The instance's filter and the deployment's routes, compiled once per
    /// config version and reused until a reload changes it.
    fn rules_for(&self, config: &Config) -> Result<Arc<RowRules>> {
        let version = self.shared_config.version();
        let mut cache = self.rules.lock();
        if cache.version == Some(version) {
            return Ok(Arc::clone(&cache.rules));
        }
        let filter = config.sources.filter_for_source(&self.connection_id);
        let routes = config
            .output
            .routes
            .iter()
            .map(|r| Route::new(&r.match_field, &r.match_value, r.destination.names()))
            .collect();
        let rules = Arc::new(RowRules::compile(filter, routes)?);
        cache.version = Some(version);
        cache.rules = Arc::clone(&rules);
        Ok(rules)
    }

    /// The unit's checkpoint key: `{instance}.{connection}.{unit}`.
    fn unit_key(&self, unit: &UnitSpec) -> String {
        format!("{}.{}.{}", self.instance_id, self.connection_id, unit.name)
    }

    async fn load_checkpoint(&self, unit: &UnitSpec) -> Option<CheckpointValue> {
        let store = self.checkpoints.as_ref()?;
        match store.get(&self.unit_key(unit)).await {
            Ok(cursor) => cursor.and_then(|c| c.checkpoint()),
            Err(e) => {
                warn!(source = self.name, unit = %unit.name, error = %e, "checkpoint read failed; starting the unit over");
                None
            }
        }
    }

    /// Wait until the admission gate opens, flushing what is buffered first so
    /// held memory drains while the source is not polled. A hold longer than
    /// `max_hold` ends anyway: memory the pause cannot release (a pumped
    /// cursor, a file chunk, an open row) would otherwise hold the unit for
    /// ever with only the paused gauge to say so.
    async fn admit(&self, unit: &UnitSpec, run: &mut UnitRun, max_hold: Duration) -> Result<()> {
        let Some(gate) = &self.gate else {
            return Ok(());
        };
        if gate.evaluate() == Admit::Yes {
            return Ok(());
        }
        if !run.batch.is_empty() {
            self.flush(unit, run, Trigger::Hold).await?;
        }
        let held_since = tokio::time::Instant::now();
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Err(Error::Shutdown),
                () = tokio::time::sleep(HOLD_POLL) => {}
            }
            if gate.evaluate() == Admit::Yes {
                return Ok(());
            }
            if held_since.elapsed() >= max_hold {
                warn!(
                    source = self.name,
                    unit = %unit.name,
                    held_secs = held_since.elapsed().as_secs(),
                    "memory pressure did not clear within self_regulation.max_hold_secs; polling on"
                );
                return Ok(());
            }
        }
    }

    /// Emit the batch, count it, and hand the shape the ack ids its rows
    /// carried once the transport has taken them, so a queue message is
    /// acknowledged only after it is delivered. A batch holding only the
    /// marks of consumed rows emits nothing and still folds and acks them.
    async fn flush(&self, unit: &UnitSpec, run: &mut UnitRun, trigger: Trigger) -> Result<()> {
        if run.batch.is_settled() {
            return Ok(());
        }
        let taken = run.batch.take();
        let rows = taken.rows.len() as u64;
        let bytes = taken.bytes as u64;
        for mark in taken.marks.iter().cloned() {
            run.pending.fold(mark);
        }
        let emitted = if rows == 0 {
            EmitReport::default()
        } else {
            self.emitter.emit(taken).await?
        };
        let acks = run.pending.take_acks();
        if !acks.is_empty() {
            self.shape.ack(unit, acks).await?;
        }
        if rows == 0 {
            return Ok(());
        }
        run.report.flushes += 1;
        run.report.bytes += bytes;
        let source = self.name;
        metrics::counter!(metric_names::ACCUMULATE_FLUSHES_TOTAL, "source" => source, "trigger" => trigger.as_str())
            .increment(1);
        metrics::histogram!(metric_names::ACCUMULATE_BATCH_ROWS, "source" => source)
            .record(rows as f64);
        metrics::histogram!(metric_names::ACCUMULATE_BATCH_BYTES, "source" => source)
            .record(bytes as f64);
        metrics::gauge!(metric_names::ACCUMULATE_PENDING_BYTES, "source" => source).set(0.0);
        debug!(
            source,
            rows,
            bytes,
            sent = emitted.sent,
            dead_lettered = emitted.dead_lettered,
            trigger = trigger.as_str(),
            "batch flushed"
        );
        Ok(())
    }

    /// Stamp one framed payload for the transport: enrich, route, topic.
    fn stamp(
        &self,
        payload: Bytes,
        route: Option<Arc<[Arc<str>]>>,
        names: &SourceNames<'_>,
        topic: &str,
    ) -> Outbound {
        let enriched = self.emitter.state().enrich_record_with(payload, names);
        let mut out = Outbound::new(topic, enriched);
        if let Some(route) = route {
            out = out.with_route(route);
        }
        out
    }

    /// Start a snapshot of `store` and buffer its `begin` marker.
    fn open_snapshot(
        &self,
        store: &str,
        run: &mut UnitRun,
        names: &SourceNames<'_>,
        topic: &str,
    ) -> Snapshot {
        let snap = Snapshot::start(store);
        run.batch
            .push(self.stamp(snap.begin(None), None, names, topic), None);
        snap
    }

    /// Close a snapshot: buffer its `end` marker carrying the rows it
    /// emitted, flush, count it complete, and commit the marks folded so far
    /// now that every row of it is acknowledged.
    async fn close_snapshot(
        &self,
        snap: &Snapshot,
        unit: &UnitSpec,
        run: &mut UnitRun,
        names: &SourceNames<'_>,
        topic: &str,
    ) -> Result<()> {
        run.batch
            .push(self.stamp(snap.end(snap.seq()), None, names, topic), None);
        self.flush(unit, run, Trigger::End).await?;
        metrics::counter!(metric_names::SNAPSHOTS_TOTAL, "store" => snap.store().to_owned(), "status" => "complete").increment(1);
        self.commit(run).await
    }

    /// Commit the folded checkpoint, when the unit has a store and a mark was
    /// folded; called only after the flush carrying those marks is
    /// acknowledged.
    async fn commit(&self, run: &UnitRun) -> Result<()> {
        // Two kinds of mark means the second was dropped on the way in, so the
        // folded value is a position the unit never reached.
        if run.pending.is_inconsistent() {
            return Err(Error::Framework(dfe_fetcher_core::Error::Cursor(format!(
                "unit `{}` folded marks of more than one kind; nothing is committed",
                run.pending.unit_key()
            ))));
        }
        if let (Some(store), Some(cursor)) = (
            &self.checkpoints,
            run.pending.cursor(Utc::now(), run.report.rows),
        ) {
            store.set(&cursor.cursor_key, &cursor).await?;
            self.metrics.inc_cursor_writes();
        }
        Ok(())
    }

    /// Count an oversize row and put its truncated copy on the dead-letter
    /// queue; the caller decides what, if anything, stands in for the row.
    ///
    /// # Errors
    ///
    /// A DLQ that refuses or cannot confirm the copy aborts the unit, so its
    /// checkpoint never passes a row nothing holds.
    async fn dead_letter_oversize(
        &self,
        unit: &UnitSpec,
        topic: &str,
        raw: &Bytes,
        run: &mut UnitRun,
    ) -> Result<()> {
        run.report.oversize += 1;
        let letter = DeadLetter {
            topic: Arc::from(topic),
            payload: self.oversize.truncate(raw),
            reason: format!(
                "row exceeds max_record_bytes ({} > {})",
                raw.len(),
                self.oversize.max_record_bytes
            ),
        };
        match self.emitter.state().dead_letter(vec![letter]).await? {
            DeadLettered::Held { .. } => {}
            DeadLettered::NoQueue => {
                self.metrics
                    .add_dead_letters_dropped(DroppedDeadLetter::TooLarge.as_str(), 1);
                debug!(source = self.name, unit = %unit.name, bytes = raw.len(), "oversize row dropped; no dead-letter queue");
            }
        }
        Ok(())
    }

    /// Run one unit's rows through the loop. Cancel-safe at every await: the
    /// row stream keeps its own state between polls and the flush sits outside
    /// the `select!`.
    ///
    /// SHORTCUT: fetch and emit do not overlap; while a flush awaits its
    /// delivery reports the row stream is not polled. Lift (one batch in
    /// flight) when a unit's dump time exceeds half its interval with both page
    /// and emit latency above ~100 ms.
    ///
    /// SHORTCUT: one virtual `poll_next` per row through the boxed stream and
    /// one allocation per enveloped row; monomorphise the driver over the
    /// shape only if a profile ever measures it.
    ///
    /// # Errors
    ///
    /// The first shape, rules, emit or checkpoint error aborts the unit; the
    /// caller commits nothing further for it.
    async fn run_rows<S>(
        &self,
        mut rows: S,
        unit: &UnitSpec,
        settings: &TickSettings,
    ) -> Result<TickReport>
    where
        S: Stream<Item = dfe_fetcher_core::Result<Row>> + Unpin,
    {
        let rules = &*settings.rules;
        let source_fetcher = format!("{}.{}", self.connection_id, unit.name);
        let names = SourceNames::new(&unit.topic, &source_fetcher);
        let topic = format!("{}{}", unit.topic, settings.topic_suffix);
        let store = unit.store(&self.connection_id);
        let per_item = unit.snapshots_per_item();
        let binary = unit.is_binary();
        let mut run = UnitRun {
            batch: Batcher::new(self.accumulate, Arc::clone(&self.lease)),
            pending: Checkpoint::new(self.unit_key(unit)),
            report: TickReport::default(),
        };
        let source = self.name;
        let mut snap = (unit.is_dump() && !per_item)
            .then(|| self.open_snapshot(&store, &mut run, &names, &topic));
        let mut item: Option<Box<str>> = None;

        loop {
            self.admit(unit, &mut run, settings.max_hold).await?;
            let next = match run.batch.window_deadline() {
                Some(deadline) => tokio::select! {
                    biased;
                    () = self.shutdown.cancelled() => return Err(Error::Shutdown),
                    r = rows.next() => r,
                    () = tokio::time::sleep_until(deadline) => {
                        self.flush(unit, &mut run, Trigger::Window).await?;
                        continue;
                    }
                },
                None => tokio::select! {
                    biased;
                    () = self.shutdown.cancelled() => return Err(Error::Shutdown),
                    r = rows.next() => r,
                },
            };
            let Some(row) = next else { break };
            let row = match row {
                Ok(row) => row,
                Err(e) => {
                    // A later item failing means the open one completed: it
                    // is closed and committed before the failure propagates.
                    if let (Some(open), Some(current), dfe_fetcher_core::Error::Item { key, .. }) =
                        (&snap, &item, &e)
                        && per_item
                        && **key != **current
                    {
                        self.close_snapshot(open, unit, &mut run, &names, &topic)
                            .await?;
                    }
                    return Err(e.into());
                }
            };
            if row.is_mark_only() {
                run.batch.consume(row.mark);
                continue;
            }
            let Row { payload: raw, mark } = row;
            if per_item {
                let Some(Mark::Item { key, .. }) = &mark else {
                    return Err(Error::Framework(dfe_fetcher_core::Error::Source(format!(
                        "unit `{}`: a per-item dump row carries no item mark; the shape must name every row's item",
                        unit.name
                    ))));
                };
                if item.as_deref() != Some(&**key) {
                    if let Some(open) = &snap {
                        self.close_snapshot(open, unit, &mut run, &names, &topic)
                            .await?;
                    }
                    snap = Some(self.open_snapshot(&store, &mut run, &names, &topic));
                    item = Some(key.clone());
                }
            }
            let raw = if settings.unwrap_nested_json && !binary {
                unwrap_nested_json(&raw)
            } else {
                raw
            };
            let oversize = self.oversize.is_oversize(&raw);
            let (payload, stub) = match &mut snap {
                Some(s) if oversize => {
                    let key = unit.row_key.as_deref().and_then(|p| row_key_at(&raw, p));
                    let stub = s.oversize(key.as_deref(), raw.len());
                    metrics::counter!(metric_names::SNAPSHOT_ROWS_OVERSIZE_TOTAL, "store" => store.clone()).increment(1);
                    self.dead_letter_oversize(unit, &topic, &raw, &mut run)
                        .await?;
                    (stub, true)
                }
                Some(s) => (s.row(&raw), false),
                None if oversize => {
                    self.dead_letter_oversize(unit, &topic, &raw, &mut run)
                        .await?;
                    run.batch.consume(mark);
                    continue;
                }
                None => (raw, false),
            };
            // A binary row is opaque bytes: the rules cannot read it and the
            // enricher would splice JSON keys into it, so it goes out verbatim.
            // A stub is the envelope's own frame, like the markers: it lands
            // whatever the filter says of `record.*`.
            let out = if binary {
                Outbound::new(&topic, payload)
            } else if stub {
                self.stamp(payload, None, &names, &topic)
            } else {
                let (payload, route) = match rules.apply(payload, &unit.add_fields) {
                    Verdict::Drop => {
                        run.report.filtered += 1;
                        self.metrics.inc_records_filtered();
                        metrics::counter!(metric_names::RECORDS_FILTERED_TOTAL, "source" => source)
                            .increment(1);
                        if let Some(s) = &mut snap {
                            s.retract();
                        }
                        run.batch.consume(mark);
                        continue;
                    }
                    Verdict::Keep { payload, route } => (payload, route),
                };
                self.stamp(payload, route, &names, &topic)
            };
            if !run.batch.fits(out.payload.len()) {
                self.flush(unit, &mut run, Trigger::Bytes).await?;
            }
            metrics::gauge!(metric_names::ACCUMULATE_PENDING_BYTES, "source" => source)
                .set((run.batch.bytes() + out.payload.len()) as f64);
            run.batch.push(out, mark);
            run.report.rows += 1;
            if snap.is_some() {
                metrics::counter!(metric_names::SNAPSHOT_ROWS_TOTAL, "store" => store.clone())
                    .increment(1);
            }
            if run.batch.full() {
                self.flush(unit, &mut run, Trigger::Rows).await?;
            }
        }
        match &snap {
            Some(s) => {
                self.close_snapshot(s, unit, &mut run, &names, &topic)
                    .await?;
            }
            None => {
                self.flush(unit, &mut run, Trigger::End).await?;
                self.commit(&run).await?;
            }
        }
        Ok(run.report)
    }
}

/// The string or number at a JSON pointer in a raw row, for the oversize stub.
fn row_key_at(payload: &[u8], pointer: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    match value.pointer(pointer)? {
        serde_json::Value::String(s) => Some(s.clone()),
        other @ (serde_json::Value::Number(_) | serde_json::Value::Bool(_)) => {
            Some(other.to_string())
        }
        _ => None,
    }
}

impl Driver {
    /// Run one scheduled tick: every unit of the shape in turn, each
    /// streamed, emitted and checkpointed inside the tick, and the report
    /// of what was emitted. `window` is the scheduler's; a dump unit ignores
    /// it.
    ///
    /// # Errors
    ///
    /// Every unit is attempted, and each failed unit is counted once in
    /// `dfe_fetcher_api_errors_total`. The first unit's error is returned once
    /// the rest have run, and [`Error::Shutdown`] returns at once.
    pub async fn run_tick(&self, window: Option<&FetchWindow>) -> Result<TickReport> {
        let config = self.shared_config.get();
        let settings = TickSettings {
            rules: self.rules_for(&config)?,
            topic_suffix: config.topic_suffix().to_owned(),
            unwrap_nested_json: config.unwrap_nested_json,
            max_hold: config.self_regulation.max_hold(),
        };
        let mut total = TickReport::default();
        let mut first_error: Option<Error> = None;
        for unit in self.shape.units() {
            let checkpoint = self.load_checkpoint(unit).await;
            let tick = TickCtx {
                window: if unit.is_dump() { None } else { window },
                connection_id: &self.connection_id,
                unit,
                checkpoint: checkpoint.as_ref(),
            };
            let rows = self.shape.rows(tick);
            match self.run_rows(rows, unit, &settings).await {
                Ok(report) => {
                    info!(
                        source = self.name,
                        unit = %unit.name,
                        rows = report.rows,
                        filtered = report.filtered,
                        oversize = report.oversize,
                        flushes = report.flushes,
                        "unit tick complete"
                    );
                    total.absorb(report);
                }
                Err(Error::Shutdown) => return Err(Error::Shutdown),
                Err(e) => {
                    // The one count of a failure, whatever shape raised it.
                    metrics::counter!(
                        metric_names::API_ERRORS_TOTAL,
                        "source" => self.connection_id.clone(),
                        "code" => e.api_error_code()
                    )
                    .increment(1);
                    if unit.is_dump() {
                        metrics::counter!(metric_names::SNAPSHOTS_TOTAL, "store" => unit.store(&self.connection_id), "status" => "aborted").increment(1);
                    }
                    warn!(source = self.name, unit = %unit.name, error = %e, "unit tick failed; the connection's cursor will not advance");
                    first_error.get_or_insert(e);
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(total),
        }
    }

    /// Health check: the credential resolves and the provider answers.
    ///
    /// # Errors
    ///
    /// Returns the shape's probe error.
    pub async fn health_check(&self) -> Result<bool> {
        self.shape.probe().await?;
        Ok(true)
    }
}
