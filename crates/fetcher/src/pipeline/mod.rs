// Project:   dfe-fetcher
// File:      crates/fetcher/src/pipeline/mod.rs
// Purpose:   The shared pipeline state: enrichment, the outputs, the DLQ, readiness
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Pipeline state and orchestration.
//!
//! [`PipelineState`] is what every producer of records shares: the one
//! enrichment implementation (`_source`, `_source_fetcher`, `_org_id`, the two
//! timestamps), the output transports, the dead-letter queue, the memory
//! guard and the readiness answer. Nothing here batches or retries: a
//! framework driver and an extractor both send through
//! [`crate::emit::Emitter`], which is the one delivery path.
//!
//! ## Data Flow
//!
//! ```text
//! Framework drivers (REST profiles, databases, files)
//!     |
//! Container extractors (stdout / HTTP ingest)
//!     |
//! Vector extractors (gRPC)
//!     |
//!     +--- enrich --> batch --> Emitter --> Output transport --> DFE pipeline
//! ```

use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dfe_fetcher_core::batch::{Outbound, Tenancy};
use dfe_fetcher_core::frame::scan::trim_range;
use dfe_fetcher_core::frame::split_array;
use scalo::dlq::{Dlq, DlqConfig, DlqEntry};
use scalo::logger::security;
use scalo::transport::KafkaConfig as ScaloKafkaConfig;
use scalo::transport::ack::EffectiveGuarantee;
use scalo::transport::{AckControl, AckKind, HeldAcks, SinkConfirmation};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::config::{Config, SharedConfig};
use crate::error::{Error, Result};
use crate::metrics::Metrics;
use crate::output::OutputManager;
use scalo::memory::MemoryGuard;

/// Metadata keys `enrich_record` stamps on every record.
///
/// They are reserved: a payload that already carries one has its value moved
/// aside rather than duplicated. Two top-level keys of the same name make the
/// loader's ClickHouse JSON column reject the whole record ("Duplicate path
/// found during parsing JSON object"), and a rejected record is not
/// dead-lettered -- it is lost. A fetched `_org_id` is moved aside the same
/// way, so a record cannot name its own tenant. A payload that may carry one
/// and does not parse cannot be rebuilt, so it is refused and never sent.
const RESERVED_KEYS: [&str; 5] = [
    "_timestamp_fetcher",
    "_timestamp_received",
    "_source",
    "_source_fetcher",
    "_org_id",
];

/// Prefixes the pre-filter searches for, one per reserved-key family.
static RESERVED_KEY_FINDERS: LazyLock<[memchr::memmem::Finder<'static>; 3]> = LazyLock::new(|| {
    [
        memchr::memmem::Finder::new(b"\"_source"),
        memchr::memmem::Finder::new(b"\"_timestamp_"),
        memchr::memmem::Finder::new(b"\"_org_id"),
    ]
});

/// The backslash that opens a JSON escape.
const JSON_ESCAPE: u8 = 0x5c;

/// The UTF-8 byte-order mark some writers put before a JSON document.
const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

/// Cheap pre-filter for [`rewrite_reserved_keys`]: true when the raw bytes
/// contain the opening quote of a reserved key name anywhere, or any backslash.
/// A key spelled with an escape decodes to the reserved name without matching
/// its literal bytes, so any escape takes the rewrite path. A nested field or a
/// string value matches too -- a false positive costs the slower rewrite path,
/// never correctness.
fn may_carry_reserved_key(raw: &[u8]) -> bool {
    memchr::memchr(JSON_ESCAPE, raw).is_some()
        || RESERVED_KEY_FINDERS.iter().any(|f| f.find(raw).is_some())
}

/// Park a reserved key's incoming value beside the fetcher's own.
///
/// `<key>_original` is the first choice; when the payload already carries that
/// name too the parked value goes to the next free `<key>_original_<n>`, so a
/// replayed record cannot overwrite the original it was enriched with first.
fn park_original(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: serde_json::Value,
) {
    let preferred = format!("{key}_original");
    if !map.contains_key(&preferred) {
        map.insert(preferred, value);
        return;
    }

    // The probe is bounded by map.len(), so one buffer is reused rather than a
    // String allocated per candidate.
    let mut parked = String::with_capacity(preferred.len() + 5);
    let mut digits = itoa::Buffer::new();
    let mut n = 2u32;
    loop {
        parked.clear();
        parked.push_str(&preferred);
        parked.push('_');
        parked.push_str(digits.format(n));
        if !map.contains_key(&parked) {
            break;
        }
        n += 1;
    }
    {
        use std::sync::atomic::AtomicU64;
        static PARK_DEBOUNCE: AtomicU64 = AtomicU64::new(0);
        if scalo::logger::log_debounced(&PARK_DEBOUNCE, 5_000) {
            warn!(
                reserved_key = key,
                parked_as = parked.as_str(),
                "Reserved key collided with an existing _original, parked under a numbered name \
                 (debounced, max 1/5s)"
            );
        }
    }
    map.insert(parked, value);
}

/// Render a string as a quoted JSON string literal, escaping included.
///
/// The append path interpolates the source names into JSON text, so serde has
/// to own the escaping or a quote or backslash in a name breaks the record.
fn json_string_literal(s: &str) -> String {
    // Value's Display has no failure case, so nothing here can fall back to an
    // empty literal and blank `_source`, the field dfe-loader routes on.
    serde_json::Value::String(s.to_owned()).to_string()
}

/// The names and the org one enrich pass stamps, escaped once as JSON literals.
///
/// They are constant for a whole batch while [`PipelineState::enrich_record_with`]
/// runs per record, so the escaping is done here and the append path borrows
/// the result.
pub struct Stamp<'a> {
    /// The DFE source name, the value of `_source`.
    dfe_source: &'a str,
    /// The producing fetcher source, the value of `_source_fetcher`.
    source: &'a str,
    /// The org the record belongs to, the value of `_org_id`.
    org: &'a str,
    /// `dfe_source` as a quoted, escaped JSON string literal.
    dfe_source_literal: String,
    /// `source` as a quoted, escaped JSON string literal.
    source_literal: String,
    /// `org` as a quoted, escaped JSON string literal.
    org_literal: String,
}

impl<'a> Stamp<'a> {
    /// Escape the names and the org as JSON string literals, once for the batch.
    #[must_use]
    pub fn new(dfe_source: &'a str, source: &'a str, org: &'a str) -> Self {
        Self {
            dfe_source,
            source,
            org,
            dfe_source_literal: json_string_literal(dfe_source),
            source_literal: json_string_literal(source),
            org_literal: json_string_literal(org),
        }
    }
}

/// Rebuild a record that already carries one or more [`RESERVED_KEYS`].
///
/// Every reserved key the payload brought is renamed to `<key>_original` and
/// the fetcher's value takes the name, so each key appears exactly once.
/// Returns `None` when the payload does not parse as a JSON object.
fn rewrite_reserved_keys(raw: &[u8], now_ms: u64, stamp: &Stamp<'_>) -> Option<Bytes> {
    let serde_json::Value::Object(mut map) = serde_json::from_slice(raw).ok()? else {
        return None;
    };

    for key in RESERVED_KEYS {
        if let Some(existing) = map.remove(key) {
            park_original(&mut map, key, existing);
        }
    }

    map.insert("_timestamp_fetcher".to_string(), now_ms.into());
    map.insert("_timestamp_received".to_string(), now_ms.into());
    map.insert("_source".to_string(), stamp.dfe_source.into());
    map.insert("_source_fetcher".to_string(), stamp.source.into());
    map.insert("_org_id".to_string(), stamp.org.into());

    serde_json::to_vec(&map).ok().map(Bytes::from)
}

/// Stamp one JSON object whose closing brace is `raw[insert_pos]`.
///
/// `None` when the object may carry a reserved key and does not parse: the
/// key cannot be replaced, and appending would leave the record two of it.
fn stamp_object(raw: &[u8], insert_pos: usize, now_ms: u64, stamp: &Stamp<'_>) -> Option<Bytes> {
    if may_carry_reserved_key(raw) {
        return rewrite_reserved_keys(raw, now_ms, stamp);
    }

    // 86 bytes of key text and two timestamps of at most 20 digits fit in
    // 160, so with the escaped literals this is the fast path's only allocation.
    let mut buf = Vec::with_capacity(
        raw.len()
            + 160
            + stamp.dfe_source_literal.len()
            + stamp.source_literal.len()
            + stamp.org_literal.len(),
    );
    buf.extend_from_slice(&raw[..insert_pos]);

    // Add comma if not empty object
    if let Some(pos) = raw[..insert_pos]
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        && raw[pos] != b'{'
    {
        buf.push(b',');
    }
    // Written straight into the record buffer, so the fast path allocates
    // once: the literals are escaped per batch and the timestamp formats
    // into a stack buffer.
    let mut digits = itoa::Buffer::new();
    let now_str = digits.format(now_ms);
    buf.extend_from_slice(b"\"_timestamp_fetcher\":");
    buf.extend_from_slice(now_str.as_bytes());
    buf.extend_from_slice(b",\"_timestamp_received\":");
    buf.extend_from_slice(now_str.as_bytes());
    buf.extend_from_slice(b",\"_source\":");
    buf.extend_from_slice(stamp.dfe_source_literal.as_bytes());
    buf.extend_from_slice(b",\"_source_fetcher\":");
    buf.extend_from_slice(stamp.source_literal.as_bytes());
    buf.extend_from_slice(b",\"_org_id\":");
    buf.extend_from_slice(stamp.org_literal.as_bytes());
    buf.extend_from_slice(&raw[insert_pos..]);
    Some(Bytes::from(buf))
}

/// The elements of the JSON array at `payload[start..]` when every element is
/// an object, as slices of `payload`, or `None` when it is anything else.
fn split_objects(payload: &Bytes, start: usize) -> Option<Vec<Bytes>> {
    let elements = split_array(payload, start, payload.len()).ok()?;
    elements
        .iter()
        .all(|element| element.first() == Some(&b'{') && element.last() == Some(&b'}'))
        .then_some(elements)
}

/// Whether `payload` carries a record boundary: a closing brace meeting the
/// next record's opening brace with only whitespace between, in a body with a
/// newline. The same scan as the loader's `has_ndjson_boundary`.
fn has_ndjson_boundary(payload: &[u8]) -> bool {
    // A raw newline is illegal inside a JSON string, so one compact record has none.
    if memchr::memchr(b'\n', payload).is_none() {
        return false;
    }
    let mut closed = false;
    for &b in payload {
        match b {
            b'}' => closed = true,
            b'{' if closed => return true,
            b if b.is_ascii_whitespace() => {}
            _ => closed = false,
        }
    }
    false
}

/// The records of `payload[start..]` when it is two or more whitespace-separated
/// JSON objects and nothing else, as slices of `payload` trimmed of the
/// whitespace between them: what the loader's `split_ndjson` fans out. `None`
/// for anything else, which the loader keeps whole too.
fn split_ndjson(payload: &Bytes, start: usize) -> Option<Vec<Bytes>> {
    let body = &payload[start..];
    if !has_ndjson_boundary(body) {
        return None;
    }
    let mut records = Vec::new();
    let mut consumed = 0;
    let mut stream =
        serde_json::Deserializer::from_slice(body).into_iter::<serde::de::IgnoredAny>();
    while let Some(item) = stream.next() {
        item.ok()?;
        let end = stream.byte_offset();
        let (from, to) = trim_range(&body[consumed..end]);
        let record = payload.slice(start + consumed + from..start + consumed + to);
        if record.first() != Some(&b'{') {
            return None;
        }
        records.push(record);
        consumed = end;
    }
    if body[consumed..].iter().any(|b| !b.is_ascii_whitespace()) {
        return None;
    }
    (records.len() > 1).then_some(records)
}

/// One stamped object per record of a split body, each record a JSON object
/// ending in its closing brace. A record that cannot be stamped refuses the
/// body whole, so the dead letter holds it as fetched.
fn stamp_each(payload: Bytes, records: &[Bytes], now_ms: u64, stamp: &Stamp<'_>) -> Enriched {
    let parts: Option<Vec<(Bytes, u64)>> = records
        .iter()
        .map(|record| {
            stamp_object(record, record.len() - 1, now_ms, stamp)
                .map(|stamped| (stamped, record.len() as u64))
        })
        .collect();
    let Some(parts) = parts else {
        return refused(payload, stamp.source);
    };
    tracing::trace!(
        source = stamp.source,
        original_bytes = payload.len(),
        records = parts.len(),
        "Body split into stamped objects"
    );
    Enriched::Split(parts)
}

/// What enriching one record made of it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum Enriched {
    /// One row, and what became of its org stamp.
    One(Bytes, Tenancy),
    /// A JSON array whose every element is an object, or a body of two or
    /// more newline-delimited objects, as one stamped object per record in
    /// order, each with its length before the stamp.
    Split(Vec<(Bytes, u64)>),
}

impl Enriched {
    /// The rows the record goes out as on `topic`, to `route` when one
    /// matched. One row counts `ingested`, the record's length as fetched,
    /// and each element of a split counts its own length before the stamp.
    pub fn into_rows(
        self,
        topic: &str,
        ingested: u64,
        route: Option<Arc<[Arc<str>]>>,
    ) -> impl Iterator<Item = Outbound> + use<> {
        let topic: Arc<str> = Arc::from(topic);
        let row = move |payload: Bytes, ingested_bytes: u64, tenancy: Tenancy| Outbound {
            topic: Arc::clone(&topic),
            payload,
            route: route.clone(),
            ingested_bytes,
            tenancy,
        };
        let (one, split) = match self {
            Enriched::One(payload, tenancy) => (Some(row(payload, ingested, tenancy)), Vec::new()),
            Enriched::Split(parts) => (None, parts),
        };
        one.into_iter().chain(
            split
                .into_iter()
                .map(move |(payload, len)| row(payload, len, Tenancy::Stamped)),
        )
    }
}

/// A record that opens as a JSON object the stamp cannot reach, which is
/// never sent.
#[cold]
fn refused(payload: Bytes, source: &str) -> Enriched {
    if scalo::logger::log_debounced(&REFUSED_WARNED_AT, DLQ_LOG_EVERY_MS) {
        warn!(
            source,
            payload_bytes = payload.len(),
            "A record that opens as a JSON object could not be stamped with its org, so it \
             is dead-lettered, never sent (debounced, max 1/5s)"
        );
    }
    Enriched::One(payload, Tenancy::Refused)
}

/// How long shutdown waits for the extractors to deliver what they hold before
/// the outputs close. After the runtime's 5 s pre-stop delay it fits the 45 s
/// termination grace the deployment contract sets (`termination_grace_seconds`)
/// with room for the final flush, and Kubernetes' 30 s default too.
pub const INTAKE_DRAIN_DEADLINE: Duration = Duration::from_secs(20);

/// Least time between two logs of a DLQ fault that repeats every write.
const DLQ_LOG_EVERY_MS: u64 = 5_000;
/// When a dead letter no DLQ backend can hold was last logged.
static DLQ_REFUSED_WARNED_AT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// When a failed DLQ write was last logged.
static DLQ_WRITE_FAILED_AT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// When a record the stamp could not reach was last logged.
static REFUSED_WARNED_AT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// One record for the dead-letter queue.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    /// The topic the record was bound for.
    pub topic: Arc<str>,
    /// The record as it would have been sent.
    pub payload: Bytes,
    /// Why it was not sent.
    pub reason: String,
}

/// An intake's acknowledgement in the shape scalo's guarantee reads. Enabled,
/// it answers only once its records are delivered or held by the DLQ: a
/// scheduled source advances its cursor then (`Pull`), the ingest listener
/// answers then (`Push`).
#[derive(Debug, Clone, Copy)]
pub struct IntakeAcks {
    /// How the intake acknowledges.
    pub kind: AckKind,
    /// Whether it holds its acknowledgement until delivery.
    pub enabled: bool,
}

impl AckControl for IntakeAcks {
    fn enabled(&self) -> bool {
        self.enabled
    }

    fn arm(&self) {}

    fn is_armed(&self) -> bool {
        true
    }

    fn kind(&self) -> AckKind {
        self.kind
    }

    fn held(&self) -> HeldAcks {
        HeldAcks::default()
    }
}

/// Where a dead-letter write ended up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum DeadLettered {
    /// A DLQ backend confirmed it holds every record it could take. The
    /// `dropped` ones no backend can ever hold (over every ceiling) were left
    /// out and counted in `pipeline_dead_letters_dropped_total`.
    Held {
        /// Records no DLQ backend can hold.
        dropped: u64,
    },
    /// No DLQ is configured, so nothing holds them.
    NoQueue,
}

/// The scalo DLQ config, and the Kafka client config its Kafka backend is
/// built on.
///
/// When the output reaches the bus, the Kafka backend rides the SAME resolved
/// producer config as the output transport (`resolve_kafka_config`), so
/// dead-letters land on the broker the data uses. A gRPC-only fetcher has no
/// broker: its DLQ gets no Kafka backend and no client config, and dead
/// letters go to the file backend. A Kafka backend there queues every entry
/// for a broker nobody configured, and scalo's cascade never hands a queued
/// entry on to the file backend.
fn dlq_backends(config: &Config) -> (DlqConfig, Option<ScaloKafkaConfig>) {
    let mut dlq = config.dlq.clone();
    if !config.output.uses_bus() {
        dlq.kafka.enabled = false;
        return (dlq, None);
    }
    (
        dlq,
        Some(crate::output::resolve_kafka_config(
            &config.output,
            &config.kafka,
        )),
    )
}

/// The memory guard's settings: the `DFE_FETCHER_MEMORY_*` env vars, then
/// `buffer.memory_limit` and `buffer.pressure_threshold` for what the env
/// leaves unset. `buffer.pressure_threshold` always holds a value, so applied
/// unconditionally it would override the env's threshold every time.
fn memory_guard_config(config: &Config) -> scalo::memory::MemoryGuardConfig {
    let mut mg_config = scalo::memory::MemoryGuardConfig::from_env("DFE_FETCHER");
    if mg_config.limit_bytes == 0 && config.buffer.memory_limit > 0 {
        mg_config.limit_bytes = config.buffer.memory_limit as u64;
    }
    let env_threshold =
        scalo::config::flat_env::flat_env_parsed::<f64>("DFE_FETCHER", "MEMORY_PRESSURE_THRESHOLD");
    if env_threshold.is_none()
        && config.buffer.pressure_threshold > 0.0
        && config.buffer.pressure_threshold <= 1.0
    {
        mg_config.pressure_threshold = config.buffer.pressure_threshold;
    }
    mg_config
}

/// Shared pipeline state accessible from handlers and schedulers.
pub struct PipelineState {
    shared_config: SharedConfig,
    output: Option<Arc<OutputManager>>,
    memory_guard: Arc<MemoryGuard>,
    dlq: Option<Dlq>,
    metrics: Arc<Metrics>,
    ready: AtomicBool,
    /// The extractor tasks shutdown waits for before the outputs close.
    intake: TaskTracker,
}

impl PipelineState {
    /// Create new pipeline state.
    ///
    /// The `output` parameter is optional: pass `None` for tests or when
    /// output transports are not yet initialised. The [`Orchestrator`]
    /// creates the [`OutputManager`] asynchronously and injects it through
    /// this `output` parameter.
    ///
    /// # Errors
    ///
    /// Returns an error when the pipeline components cannot be built from the config.
    pub fn new(
        shared_config: SharedConfig,
        metrics: Arc<Metrics>,
        output: Option<OutputManager>,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let config = shared_config.get();
        let memory_guard = Arc::new(MemoryGuard::new(memory_guard_config(&config)));

        // Initialise DLQ if enabled.
        let dlq = if config.dlq.enabled {
            let (dlq_config, dlq_kafka) = dlq_backends(&config);
            match Dlq::spawn(&dlq_config, "dfe-fetcher", dlq_kafka.as_ref(), shutdown) {
                Ok(d) => Some(d),
                Err(e) => {
                    warn!(error = %e, "Failed to initialise DLQ, continuing without it");
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            shared_config,
            output: output.map(Arc::new),
            memory_guard,
            dlq,
            metrics,
            ready: AtomicBool::new(true),
            intake: TaskTracker::new(),
        })
    }

    /// State whose memory guard counts only reserved bytes.
    ///
    /// `new` auto-detects usage from the cgroup, so a readiness assertion there
    /// measures the whole container rather than this pipeline.
    #[cfg(test)]
    pub(crate) fn for_tests(
        shared_config: SharedConfig,
        metrics: Arc<Metrics>,
        output: Option<OutputManager>,
    ) -> Self {
        Self {
            shared_config,
            output: output.map(Arc::new),
            memory_guard: Arc::new(MemoryGuard::with_usage_source(
                scalo::memory::MemoryGuardConfig {
                    limit_bytes: 1_073_741_824,
                    ..Default::default()
                },
                scalo::memory::UsageSource::Reservations,
            )),
            dlq: None,
            metrics,
            ready: AtomicBool::new(true),
            intake: TaskTracker::new(),
        }
    }

    /// State as [`Self::for_tests`], with `dlq` as its dead-letter queue.
    #[cfg(test)]
    pub(crate) fn for_tests_with_dlq(
        shared_config: SharedConfig,
        metrics: Arc<Metrics>,
        output: Option<OutputManager>,
        dlq: Dlq,
    ) -> Self {
        Self {
            dlq: Some(dlq),
            ..Self::for_tests(shared_config, metrics, output)
        }
    }

    /// The extractor tasks shutdown drains: a task spawned here keeps the
    /// outputs open until it finishes or [`INTAKE_DRAIN_DEADLINE`] passes.
    pub fn intake(&self) -> &TaskTracker {
        &self.intake
    }

    /// Get the current configuration.
    pub fn config(&self) -> Config {
        self.shared_config.get()
    }

    /// The configured topic suffix.
    ///
    /// Reads the one field under the lock: `config()` clones the whole `Config`,
    /// including every source block and destination, and the extractors call
    /// this per batch and per line.
    pub fn topic_suffix(&self) -> String {
        self.shared_config.with(|c| c.topic_suffix().to_owned())
    }

    /// Get the shared config handle.
    pub fn shared_config(&self) -> SharedConfig {
        self.shared_config.clone()
    }

    /// Check if the pipeline is ready.
    pub fn is_ready(&self) -> bool {
        if !self.probe_ready() {
            return false;
        }

        if let Some(ref output) = self.output
            && !output.any_healthy()
        {
            return false;
        }

        true
    }

    /// What `/readyz` answers: startup state and pressure, NOT output health.
    ///
    /// Every replica shares the output, so failing the probe on it fails them
    /// all at once -- and an unready pod blocks a Deployment rollout, so an
    /// output outage during a rollout stalls it indefinitely. Stalling the
    /// fetch loop is [`Self::is_ready`]'s job and stays where it is.
    pub fn probe_ready(&self) -> bool {
        if !self.ready.load(Ordering::Relaxed) {
            return false;
        }

        if self.memory_guard.under_pressure() {
            return false;
        }

        true
    }

    /// The org every record this instance emits belongs to: the configured
    /// `org_id`, else `default_org_id`.
    pub fn org_id(&self) -> String {
        self.shared_config.with(|c| c.stamped_org().to_owned())
    }

    /// The org a record that carries no `_org_id` counts for usage under:
    /// `default_org_id`. Usage measures ingest into the platform, so it counts
    /// such a record though the loader refuses it.
    pub fn default_org_id(&self) -> String {
        self.shared_config.with(|c| c.default_org_id.clone())
    }

    /// Enrich a record with fetcher metadata.
    ///
    /// `_source` is the DFE source name (the topic without its suffix), the field
    /// dfe-loader routes and filters on. `_source_fetcher` names the producer,
    /// and `_org_id` is [`Self::org_id`].
    ///
    /// The fetcher's value wins for every reserved metadata key -- the loader
    /// routes on `_source`, so the DFE source name must be the one that survives
    /// -- and whatever the payload carried under that name is kept as
    /// `<key>_original`, or the next free `<key>_original_<n>` when that name is
    /// taken too.
    ///
    /// This reads the org and escapes the names on every call. A batch that
    /// shares them builds a [`Stamp`] once and calls
    /// [`Self::enrich_record_with`].
    pub fn enrich_record(&self, payload: Bytes, dfe_source: &str, source: &str) -> Enriched {
        let org = self.org_id();
        self.enrich_record_with(payload, &Stamp::new(dfe_source, source, &org))
    }

    /// Enrich a record against a stamp already escaped for the whole batch.
    ///
    /// Same contract as [`Self::enrich_record`], minus the per-record escaping.
    ///
    /// A JSON array whose every element is an object, and a body of two or
    /// more whitespace-separated objects and nothing else, come back
    /// [`Enriched::Split`], one stamped object per record: the loader splits
    /// both itself, and a record with no `_org_id` lands in its default org.
    /// Anything else that is not an object (not JSON, a scalar, an array
    /// holding a non-object) comes back [`Tenancy::Unstamped`] and unchanged.
    /// The loader refuses such a row, though the archiver may keep it, and its
    /// usage counts under `default_org_id`, since usage measures ingest into
    /// the platform rather than what the loader stores.
    /// A record that opens as an object, after whitespace or a UTF-8
    /// byte-order mark, and cannot be stamped (no closing brace, or a reserved
    /// key that does not parse) comes back [`Tenancy::Refused`] and unchanged:
    /// sent, it would keep any `_org_id` its sender gave it. A stamped row
    /// leaves without the byte-order mark.
    pub fn enrich_record_with(&self, payload: Bytes, stamp: &Stamp<'_>) -> Enriched {
        let source = stamp.source;
        let now_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);

        let raw = payload.as_ref();
        let start = if raw.starts_with(UTF8_BOM) {
            UTF8_BOM.len()
        } else {
            0
        };
        let body = &raw[start..];
        // The stamp lands at an object's top level: in an array or a scalar it
        // would land inside one element, or nowhere.
        match body.iter().find(|b| !b.is_ascii_whitespace()) {
            Some(b'{') => {
                // The stamp lands before the last closing brace only, so a body of
                // several records is stamped one record at a time.
                if let Some(records) = split_ndjson(&payload, start) {
                    return stamp_each(payload, &records, now_ms, stamp);
                }
                let Some(enriched) = body
                    .iter()
                    .rposition(|&b| b == b'}')
                    .and_then(|insert_pos| stamp_object(body, insert_pos, now_ms, stamp))
                else {
                    return refused(payload, source);
                };
                tracing::trace!(
                    source,
                    original_bytes = raw.len(),
                    enriched_bytes = enriched.len(),
                    timestamp_fetcher = now_ms,
                    "Record enriched with fetcher metadata"
                );
                return Enriched::One(enriched, Tenancy::Stamped);
            }
            Some(b'[') => {
                if let Some(elements) = split_objects(&payload, start) {
                    return stamp_each(payload, &elements, now_ms, stamp);
                }
            }
            _ => {}
        }
        tracing::trace!(
            source,
            payload_bytes = raw.len(),
            "Enrich skipped -- payload is not a JSON object or an array of objects"
        );
        Enriched::One(payload, Tenancy::Unstamped)
    }

    /// Send one record to every default transport, with no dead-lettering and
    /// no memory lease: the emitter accounts for both itself.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when no output is configured, otherwise the
    /// transport's own error.
    pub async fn send_all(&self, topic: &str, payload: Bytes) -> Result<()> {
        let Some(ref output) = self.output else {
            return Err(Error::Config("Output transport not configured".into()));
        };
        output.send_all(topic, payload).await
    }

    /// Send one record to named destinations; see [`Self::send_all`]. A bus
    /// destination naming its own topic gets the suffix in force appended, as
    /// a source's topic does.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when no output is configured, otherwise the
    /// destination's own error.
    pub async fn send_to(&self, destinations: &[&str], topic: &str, payload: Bytes) -> Result<()> {
        let Some(ref output) = self.output else {
            return Err(Error::Config("Output transport not configured".into()));
        };
        let suffix = self.topic_suffix();
        output.send_to(destinations, topic, &suffix, payload).await
    }

    /// Write `letters` to the dead-letter queue and wait until a backend holds
    /// every one, so a caller counts a record handled only once it is durable.
    ///
    /// A letter no backend can ever hold (scalo's `Dlq::refusal`: over the
    /// Kafka DLQ's ceiling once encoded) is left out of the write and counted
    /// dropped, since no retry could land it and it would hold its source for
    /// good.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Dlq`] when the DLQ refused the write or could not
    /// confirm it. That can clear, so nothing in `letters` may then be counted
    /// as handled, and the caller holds and retries.
    pub async fn dead_letter(&self, letters: Vec<DeadLetter>) -> Result<DeadLettered> {
        let Some(dlq) = self.dlq.as_ref().filter(|d| d.is_enabled()) else {
            return Ok(DeadLettered::NoQueue);
        };
        let mut dropped = 0;
        let mut held = Vec::with_capacity(letters.len());
        let mut entries = Vec::with_capacity(letters.len());
        for letter in letters {
            let entry = DlqEntry::new("dfe-fetcher", &letter.reason, letter.payload.to_vec())
                .with_destination(&*letter.topic);
            let entry = dlq.cap(entry);
            if let Some(refusal) = dlq.refusal(&entry) {
                dropped += 1;
                self.metrics.add_dead_letters_dropped(refusal.as_str(), 1);
                if scalo::logger::log_debounced(&DLQ_REFUSED_WARNED_AT, DLQ_LOG_EVERY_MS) {
                    warn!(
                        topic = %letter.topic,
                        reason = %letter.reason,
                        refusal = %refusal,
                        "A dead letter no DLQ backend can hold was dropped. Counted in \
                         pipeline_dead_letters_dropped_total"
                    );
                }
            } else {
                entries.push(entry);
                held.push(letter);
            }
        }
        if entries.is_empty() {
            return Ok(DeadLettered::Held { dropped });
        }
        let letters = held;
        if let Err(dlq_err) = dlq.write_confirmed(entries).await {
            if scalo::logger::log_debounced(&DLQ_WRITE_FAILED_AT, DLQ_LOG_EVERY_MS) {
                error!(
                    error = %dlq_err,
                    records = letters.len(),
                    "The DLQ refused a dead-letter write, so its records stay undelivered (debounced, max 1/5s)"
                );
            }
            return Err(dlq_err.into());
        }
        {
            use std::sync::atomic::{AtomicU64, Ordering};
            static DLQ_SAMPLES: AtomicU64 = AtomicU64::new(0);
            for letter in &letters {
                self.metrics.inc_messages_dlq();
                security::record_dlq("transport_failure", &letter.reason, Some(&letter.topic));
                if scalo::logger::log_sampled(&DLQ_SAMPLES, 100) {
                    warn!(
                        topic = %letter.topic,
                        reason = %letter.reason,
                        total = DLQ_SAMPLES.load(Ordering::Relaxed),
                        "Message routed to DLQ after transport failure (sampled 1/100)"
                    );
                }
            }
        }
        Ok(DeadLettered::Held { dropped })
    }

    /// Publish `pipeline_delivery_guarantee{listener, guarantee, reason}`
    /// through scalo's `EffectiveGuarantee::publish_for` for one intake, whose
    /// source acknowledgement is `source` (`None` for one that holds nothing,
    /// such as a container's stdout), against what the outputs confirm.
    pub fn publish_guarantee(
        &self,
        listener: &str,
        source: Option<&dyn AckControl>,
    ) -> EffectiveGuarantee {
        let sink = self
            .output
            .as_ref()
            .map_or(SinkConfirmation::None, |o| o.confirms_delivery());
        let effective = EffectiveGuarantee::of(source, sink);
        effective.publish_for(listener);
        effective
    }

    /// Get memory guard for external access (scaling pressure, metrics).
    pub fn memory_guard(&self) -> &Arc<MemoryGuard> {
        &self.memory_guard
    }

    /// Check if any output transport is healthy.
    ///
    /// Returns `true` if no output is configured (nothing to fail) or if at
    /// least one transport reports healthy. Used by the scaling pressure
    /// circuit-breaker gate.
    pub fn output_healthy(&self) -> bool {
        match self.output {
            Some(ref output) => output.any_healthy(),
            None => true,
        }
    }

    /// Update metrics snapshot.
    pub async fn update_metrics(&self, metrics: &Metrics) {
        metrics.set_memory_usage(
            self.memory_guard.current_bytes(),
            self.memory_guard.limit_bytes(),
        );
    }

    /// Reload configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the reload cannot be applied.
    pub fn reload_config(&self, new_config: Config) -> Result<()> {
        self.shared_config.update(new_config);
        let version = self.shared_config.version();
        info!(version, "Configuration reloaded successfully");
        Ok(())
    }
}

/// Main pipeline orchestrator.
pub struct Orchestrator {
    state: Arc<PipelineState>,
    shared_config: SharedConfig,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
}

impl Orchestrator {
    /// Create a new orchestrator with output transports initialised.
    ///
    /// Async because output transport creation (Kafka, gRPC) requires
    /// network connections. Pass a config with no brokers to skip
    /// output transport initialisation (tests, config-check).
    ///
    /// # Errors
    ///
    /// Returns an error when an output transport or the pipeline cannot be created.
    pub async fn new(
        config: Config,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let shared_config = SharedConfig::new(config);
        let cfg = shared_config.get();

        // Create output manager if any output transports are configured
        let has_output_kafka = cfg
            .output
            .kafka
            .as_ref()
            .is_some_and(|k| !k.brokers.is_empty());
        let has_legacy_kafka = !cfg.kafka.brokers.is_empty();
        let has_grpc = cfg
            .output
            .grpc
            .as_ref()
            .is_some_and(|g| g.endpoint.is_some());

        let output = if has_output_kafka || has_legacy_kafka || has_grpc {
            Some(OutputManager::new(&cfg.output, &cfg.kafka).await?)
        } else {
            info!("No output transports configured, delivery disabled");
            None
        };

        let state = PipelineState::new(
            shared_config.clone(),
            Arc::clone(&metrics),
            output,
            shutdown.clone(),
        )?;

        Ok(Self {
            state: Arc::new(state),
            shared_config,
            metrics,
            shutdown,
        })
    }

    /// Get shared pipeline state.
    pub fn state(&self) -> Arc<PipelineState> {
        Arc::clone(&self.state)
    }

    /// Get shared config handle.
    pub fn shared_config(&self) -> SharedConfig {
        self.shared_config.clone()
    }

    /// Run the orchestrator (background tasks).
    ///
    /// # Errors
    ///
    /// Returns an error when a background task fails.
    pub async fn run(&self) -> Result<()> {
        info!("Pipeline orchestrator running");

        // Periodic metrics update (1s interval)
        let metrics_state = Arc::clone(&self.state);
        let metrics_ref = Arc::clone(&self.metrics);
        let metrics_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        metrics_state.update_metrics(&metrics_ref).await;
                        // Track transport health gauge
                        if let Some(ref output) = metrics_state.output {
                            metrics_ref.set_transport_healthy(output.any_healthy());
                        }
                    }
                    _ = metrics_shutdown.cancelled() => break,
                }
            }
        });

        // Wait for shutdown
        self.shutdown.cancelled().await;

        info!("Pipeline orchestrator shutting down");
        self.drain_intake().await;

        // Close all output transports
        if let Some(ref output) = self.state.output {
            output.close_all().await;
        }

        info!("Pipeline orchestrator stopped");
        Ok(())
    }

    /// Wait for the extractors to deliver what they hold, while the outputs
    /// are still open, for at most [`INTAKE_DRAIN_DEADLINE`].
    async fn drain_intake(&self) {
        let intake = self.state.intake();
        intake.close();
        if intake.is_empty() {
            return;
        }
        info!(
            tasks = intake.len(),
            deadline_secs = INTAKE_DRAIN_DEADLINE.as_secs(),
            "Draining extractor intake before the outputs close"
        );
        if tokio::time::timeout(INTAKE_DRAIN_DEADLINE, intake.wait())
            .await
            .is_err()
        {
            warn!(
                tasks = intake.len(),
                "Extractor intake still draining at the deadline. Closing the outputs: \
                 what it holds is answered for retry or counted dropped"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enrich_record_injects_metadata() {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new())
            .unwrap_or_else(|_| {
                // No output configured, create minimal state
                let config = Config::default();
                let shared = SharedConfig::new(config);
                PipelineState {
                    shared_config: shared,
                    output: None,
                    memory_guard: Arc::new(MemoryGuard::new(scalo::memory::MemoryGuardConfig {
                        limit_bytes: 1_073_741_824, // 1 GiB for tests
                        ..Default::default()
                    })),
                    dlq: None,
                    metrics: Arc::new(Metrics::new()),
                    ready: AtomicBool::new(true),
                    intake: TaskTracker::new(),
                }
            });

        let payload = Bytes::from(r#"{"key": "value"}"#);
        let enriched = stamped(state.enrich_record(payload, "cloudtrail", "aws.cloudtrail"));
        let enriched_str = std::str::from_utf8(&enriched).unwrap();

        assert!(enriched_str.contains("\"_timestamp_fetcher\":"));
        assert!(enriched_str.contains("\"_timestamp_received\":"));
        assert!(enriched_str.contains("\"_source\":\"cloudtrail\""));
        assert!(enriched_str.contains("\"_source_fetcher\":\"aws.cloudtrail\""));

        // Verify it's still valid JSON
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert!(parsed.get("_timestamp_fetcher").is_some());
        assert!(parsed.get("_timestamp_received").is_some());
        assert_eq!(
            parsed.get("_source").unwrap().as_str().unwrap(),
            "cloudtrail"
        );
        assert_eq!(
            parsed.get("_source_fetcher").unwrap().as_str().unwrap(),
            "aws.cloudtrail"
        );
        assert_eq!(parsed.get("key").unwrap(), "value");
    }

    // -- enrich_record tests --

    /// Helper to create a PipelineState for enrichment tests without output.
    ///
    /// Reservation-counted, so a readiness or pressure assertion reads what the
    /// test reserved rather than the container's memory.
    fn make_pipeline_state() -> PipelineState {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        PipelineState::for_tests(shared, metrics, None)
    }

    /// The one stamped row a record became.
    fn stamped(enriched: Enriched) -> Bytes {
        match enriched {
            Enriched::One(payload, Tenancy::Stamped) => payload,
            Enriched::One(_, tenancy) => panic!("expected one stamped row, got {tenancy:?}"),
            Enriched::Split(parts) => panic!("expected one stamped row, got {parts:?}"),
        }
    }

    /// A record that may name its own org and does not parse cannot have its
    /// `_org_id` replaced, so it comes back refused and byte-identical, never
    /// with a second `_org_id` appended after the sender's. One such element
    /// refuses an array whole.
    #[test]
    fn a_record_that_may_name_its_org_and_does_not_parse_is_refused() {
        let state = pipeline_over("org_id: acme\n");
        // A lone surrogate escape, built from the backslash's code point: serde refuses it.
        let lone_surrogate = format!(r#"{{"_org_id":"victim","s":"{}ud800"}}"#, char::from(0x5c));
        let in_an_array = format!(r#"[{{"id":"a"}},{lone_surrogate}]"#);
        for raw in [
            lone_surrogate.as_str(),
            r#"{"_org_id":"victim","s":}"#,
            in_an_array.as_str(),
        ] {
            let payload = Bytes::from(raw.to_owned());
            assert_eq!(
                state.enrich_record(payload.clone(), "ingest", "ingest.source"),
                Enriched::One(payload, Tenancy::Refused),
                "{raw}"
            );
        }
    }

    #[test]
    fn test_enrich_record_empty_object() {
        let state = make_pipeline_state();
        let payload = Bytes::from("{}");
        let enriched = stamped(state.enrich_record(payload, "test", "test.source"));
        let enriched_str = std::str::from_utf8(&enriched).unwrap();

        // Should be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(enriched_str).unwrap();
        assert!(parsed.get("_timestamp_fetcher").is_some());
        assert!(parsed.get("_timestamp_received").is_some());
        assert_eq!(parsed.get("_source").unwrap().as_str().unwrap(), "test");
        assert_eq!(
            parsed.get("_source_fetcher").unwrap().as_str().unwrap(),
            "test.source"
        );
    }

    #[test]
    fn test_enrich_record_non_json_returns_unchanged() {
        let state = make_pipeline_state();
        let raw = "this is not json at all";
        let payload = Bytes::from(raw);
        assert_eq!(
            state.enrich_record(payload.clone(), "src", "src.fetcher"),
            Enriched::One(payload, Tenancy::Unstamped),
            "no object to stamp, so it goes out as it came"
        );
    }

    /// Only an object takes the stamp: in a scalar it has nowhere to go, and
    /// in an array holding a non-object it would land inside one element and
    /// leave the rest untagged. Those leave unstamped and byte-identical.
    #[test]
    fn a_payload_that_cannot_carry_the_org_leaves_unstamped_and_byte_identical() {
        let state = pipeline_over("org_id: acme\n");
        for raw in [
            "[1,2]",
            r#"[{"a":1},2]"#,
            r#"[[{"a":1}]]"#,
            r#"[{"a":1},"#,
            r#""text}""#,
            "42",
        ] {
            let payload = Bytes::from(raw);
            assert_eq!(
                state.enrich_record(payload.clone(), "ingest", "ingest.source"),
                Enriched::One(payload, Tenancy::Unstamped),
                "{raw}"
            );
        }

        let enriched = stamped(state.enrich_record(Bytes::from(r#"  {"a":1}"#), "ingest", "i.s"));
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(
            parsed["_org_id"], "acme",
            "an object after whitespace is stamped"
        );
    }

    /// A JSON array of objects splits into one stamped object per element, in
    /// order, each with its length before the stamp. A reserved key an element
    /// carries is parked as it is in a single object, and `[]` holds no row.
    #[test]
    fn an_array_of_objects_splits_into_one_stamped_object_per_element() {
        let state = pipeline_over("org_id: acme\n");
        let (first, second) = (r#"{"id":"a"}"#, r#"{"id":"b","_org_id":"someone-else"}"#);
        let Enriched::Split(parts) = state.enrich_record(
            Bytes::from(format!(" [{first},\n {second}] ")),
            "ingest",
            "ingest.source",
        ) else {
            panic!("an array of objects splits");
        };
        let got: Vec<(serde_json::Value, u64)> = parts
            .iter()
            .map(|(payload, len)| (serde_json::from_slice(payload).unwrap(), *len))
            .collect();
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(
            (&got[0].0["id"], &got[0].0["_org_id"], got[0].1),
            (
                &serde_json::json!("a"),
                &serde_json::json!("acme"),
                first.len() as u64
            )
        );
        assert_eq!(
            (
                &got[1].0["id"],
                &got[1].0["_org_id"],
                &got[1].0["_org_id_original"],
                got[1].1
            ),
            (
                &serde_json::json!("b"),
                &serde_json::json!("acme"),
                &serde_json::json!("someone-else"),
                second.len() as u64
            )
        );
        assert_eq!(key_occurrences(&parts[1].0, "_org_id"), 1);
        assert_eq!(
            state.enrich_record(Bytes::from("[]"), "ingest", "ingest.source"),
            Enriched::Split(Vec::new())
        );
    }

    /// A payload that opens as an object, after whitespace or a byte-order
    /// mark, and cannot be stamped is refused: sent as it came it would keep
    /// the `_org_id` its sender gave it.
    #[test]
    fn an_object_that_cannot_be_stamped_is_refused_not_sent_unstamped() {
        let state = pipeline_over("org_id: acme\n");
        for raw in [
            &br#"{"_org_id":"victim""#[..],
            br#"  {"_org_id":"victim","a":1"#,
            b"\xEF\xBB\xBF{\"_org_id\":\"victim\"",
            br#"{"a":1"#,
        ] {
            let payload = Bytes::copy_from_slice(raw);
            assert_eq!(
                state.enrich_record(payload.clone(), "ingest", "ingest.source"),
                Enriched::One(payload, Tenancy::Refused),
                "{}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    /// A byte-order mark is not part of the JSON: an object behind one is
    /// stamped like any other, and the stamped row leaves without the mark.
    #[test]
    fn an_object_behind_a_byte_order_mark_is_stamped_without_it() {
        let state = pipeline_over("org_id: acme\n");
        let enriched = stamped(state.enrich_record(
            Bytes::from_static(b"\xEF\xBB\xBF{\"_org_id\":\"victim\",\"a\":1}"),
            "ingest",
            "ingest.source",
        ));
        assert_eq!(enriched.first(), Some(&b'{'), "the mark is dropped");
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(
            (
                &parsed["_org_id"],
                &parsed["_org_id_original"],
                &parsed["a"]
            ),
            (
                &serde_json::json!("acme"),
                &serde_json::json!("victim"),
                &serde_json::json!(1)
            )
        );
    }

    /// A body of two or more whitespace-separated objects and nothing else is
    /// split into one stamped object per record, exactly where the loader's
    /// `split_ndjson` would fan it out, so no record leaves without the org.
    #[test]
    fn newline_delimited_objects_split_where_the_loader_splits_them() {
        let state = pipeline_over("org_id: acme\n");
        let (first, second) = (r#"{"a": 1}"#, r#"{"b": 2, "_org_id": "x"}"#);
        let Enriched::Split(parts) = state.enrich_record(
            Bytes::from(format!("  {first}\r\n\r\n  {second}  \n")),
            "ingest",
            "ingest.source",
        ) else {
            panic!("two records split");
        };
        let got: Vec<(serde_json::Value, serde_json::Value, u64)> = parts
            .iter()
            .map(|(payload, len)| {
                let row: serde_json::Value = serde_json::from_slice(payload).unwrap();
                (
                    row["_org_id"].clone(),
                    row["_org_id_original"].clone(),
                    *len,
                )
            })
            .collect();
        assert_eq!(
            got,
            [
                (
                    serde_json::json!("acme"),
                    serde_json::Value::Null,
                    first.len() as u64
                ),
                (
                    serde_json::json!("acme"),
                    serde_json::json!("x"),
                    second.len() as u64
                ),
            ],
            "each record counts its own bytes, the separators none"
        );

        // What the loader leaves whole stays one row here too.
        for raw in [
            "{\n  \"a\": 1\n}\n",
            "{\"msg\": \"}\n{\"}",
            "{\"a\": 1}\n[2]\n{\"b\": 3}",
            "{\"a\": 1}\n{\"b\": 2}\njunk",
        ] {
            assert!(
                matches!(
                    state.enrich_record(Bytes::from(raw), "ingest", "ingest.source"),
                    Enriched::One(..)
                ),
                "{raw}"
            );
        }
    }

    /// The rows a record goes out as: one counted at its fetched length, or
    /// one per element of a split counted at the element's own, each on the
    /// topic and route given.
    #[test]
    fn enriched_records_go_out_as_rows_on_their_topic_and_route() {
        let route: Arc<[Arc<str>]> = Arc::from([Arc::from("siem")]);
        let one: Vec<Outbound> = Enriched::One(Bytes::from("text"), Tenancy::Unstamped)
            .into_rows("t_land", 4, Some(Arc::clone(&route)))
            .collect();
        assert_eq!(
            one,
            [Outbound::new("t_land", "text")
                .with_tenancy(Tenancy::Unstamped)
                .with_route(Arc::clone(&route))]
        );
        let split: Vec<(u64, Tenancy)> = Enriched::Split(vec![
            (Bytes::from("{\"a\":1}"), 3),
            (Bytes::from("{\"b\":2}"), 5),
        ])
        .into_rows("t_land", 99, None)
        .map(|row| (row.ingested_bytes, row.tenancy))
        .collect();
        assert_eq!(split, [(3, Tenancy::Stamped), (5, Tenancy::Stamped)]);
    }

    #[test]
    fn test_enrich_record_nested_json() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"outer":{"inner":42}}"#);
        let enriched = stamped(state.enrich_record(payload, "nested", "nested.src"));
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();

        // Original nested data preserved
        assert_eq!(parsed["outer"]["inner"], 42);
        // Metadata at top level
        assert!(parsed.get("_timestamp_fetcher").is_some());
        assert_eq!(parsed["_source"], "nested");
        assert_eq!(parsed["_source_fetcher"], "nested.src");
    }

    #[test]
    fn test_enrich_record_large_payload() {
        let state = make_pipeline_state();
        // Build a JSON object with >10KB of content
        let mut big = String::from("{");
        for i in 0..500 {
            if i > 0 {
                big.push(',');
            }
            big.push_str(&format!(
                r#""field_{i}":"value_{val}""#,
                val = "x".repeat(20)
            ));
        }
        big.push('}');
        assert!(big.len() > 10_000, "Test payload should exceed 10KB");

        let payload = Bytes::from(big);
        let enriched = stamped(state.enrich_record(payload, "large", "large.source"));
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();

        // Metadata added
        assert!(parsed.get("_timestamp_fetcher").is_some());
        assert_eq!(parsed["_source"], "large");
        assert_eq!(parsed["_source_fetcher"], "large.source");
        // Original fields preserved
        assert!(parsed.get("field_0").is_some());
        assert!(parsed.get("field_499").is_some());
    }

    #[test]
    fn test_enrich_record_unicode_content() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"name":"日本語テスト","emoji":"🚀🔥"}"#);
        let enriched = stamped(state.enrich_record(payload, "unicode", "unicode.src"));
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();

        // Unicode preserved
        assert_eq!(parsed["name"], "日本語テスト");
        assert_eq!(parsed["emoji"], "🚀🔥");
        // Metadata added
        assert!(parsed.get("_timestamp_fetcher").is_some());
    }

    #[test]
    fn test_enrich_record_special_chars_in_source() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"key":"val"}"#);
        let enriched = stamped(state.enrich_record(payload, "special", "source/with\"special"));
        let enriched_str = std::str::from_utf8(&enriched).unwrap();
        // The quote is escaped in the bytes, so the record still parses.
        assert!(
            enriched_str.contains(r#"source/with\"special"#),
            "{enriched_str}"
        );
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_source_fetcher"], "source/with\"special");
    }

    // -- PipelineState tests --

    #[test]
    fn test_pipeline_state_is_ready_under_memory_pressure() {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        // Create with a very low memory limit to trigger pressure
        let state = PipelineState {
            shared_config: shared,
            output: None,
            memory_guard: Arc::new(MemoryGuard::new(scalo::memory::MemoryGuardConfig {
                limit_bytes: 1, // 1 byte -- will be under pressure
                pressure_threshold: 0.01,
                ..Default::default()
            })),
            dlq: None,
            metrics,
            ready: AtomicBool::new(true),
            intake: TaskTracker::new(),
        };
        // Add bytes to trigger pressure
        state.memory_guard.add_bytes(100);
        assert!(
            !state.is_ready(),
            "Pipeline should not be ready when memory is under pressure"
        );
    }

    #[test]
    fn test_pipeline_state_output_healthy_no_output() {
        let state = make_pipeline_state();
        // No output configured -- should report healthy (nothing to fail)
        assert!(
            state.output_healthy(),
            "No output configured should be considered healthy"
        );
    }

    #[test]
    fn pressure_fails_the_probe_as_well_as_the_fetch_stall() {
        // Output health is the only divergence and is not covered here: this
        // state configures no output, so `output_healthy()` is true always.
        let state = make_pipeline_state();
        assert!(state.probe_ready(), "a fresh pipeline must pass the probe");
        assert!(state.is_ready(), "and must not stall the fetch loop");

        state.memory_guard.add_bytes(u64::MAX / 2);

        assert!(!state.probe_ready(), "pressure must fail the probe");
        assert!(!state.is_ready(), "and must stall the fetch loop");
    }

    // -- reload_config tests --

    #[test]
    fn test_reload_config_updates_and_increments_version() {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let initial_version = shared.version();
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new()).unwrap();

        let mut new_config = Config::default();
        new_config.scheduler.default_interval_secs = 999;
        state
            .reload_config(new_config)
            .expect("reload_config should succeed");

        // Version incremented
        assert_eq!(
            state.shared_config().version(),
            initial_version + 1,
            "version should increment on reload"
        );
        // Config content applied
        assert_eq!(
            state.config().scheduler.default_interval_secs,
            999,
            "reloaded config should be returned by config()"
        );
    }

    // -- Orchestrator tests --

    #[tokio::test]
    async fn test_orchestrator_new_without_output_succeeds() {
        let config = Config::default(); // No brokers, no output.kafka, no output.grpc
        let metrics = Arc::new(Metrics::new());
        let shutdown = CancellationToken::new();
        let orchestrator = Box::pin(Orchestrator::new(config, metrics, shutdown))
            .await
            .expect("Orchestrator should construct without output");

        let state = orchestrator.state();
        // With no output configured, output_healthy reports true
        assert!(
            state.output_healthy(),
            "no output = considered healthy (nothing to fail)"
        );
    }

    #[tokio::test]
    async fn test_orchestrator_run_exits_on_shutdown() {
        let config = Config::default();
        let metrics = Arc::new(Metrics::new());
        let shutdown = CancellationToken::new();
        let orchestrator = Box::pin(Orchestrator::new(config, metrics, shutdown.clone()))
            .await
            .expect("Orchestrator::new should succeed");

        // Cancel first so run() exits immediately
        shutdown.cancel();

        let result = tokio::time::timeout(Duration::from_secs(5), orchestrator.run()).await;
        let run_result = result.expect("run() should complete within timeout");
        run_result.expect("run() should exit cleanly on shutdown");
    }

    /// Shutdown waits for a tracked extractor to deliver what it holds before
    /// the outputs close.
    #[tokio::test]
    async fn shutdown_waits_for_the_intake_to_drain() {
        let shutdown = CancellationToken::new();
        let orchestrator = Box::pin(Orchestrator::new(
            Config::default(),
            Arc::new(Metrics::new()),
            shutdown.clone(),
        ))
        .await
        .expect("orchestrator");
        let drained = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&drained);
        let token = shutdown.clone();
        orchestrator.state().intake().spawn(async move {
            token.cancelled().await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            flag.store(true, Ordering::Release);
        });

        shutdown.cancel();
        orchestrator.run().await.expect("run");

        assert!(
            drained.load(Ordering::Acquire),
            "run closed the outputs before the intake drained"
        );
    }

    /// An intake task that never finishes holds the outputs open for the drain
    /// deadline and no longer.
    #[tokio::test(start_paused = true)]
    async fn shutdown_closes_the_outputs_at_the_drain_deadline() {
        let shutdown = CancellationToken::new();
        let orchestrator = Box::pin(Orchestrator::new(
            Config::default(),
            Arc::new(Metrics::new()),
            shutdown.clone(),
        ))
        .await
        .expect("orchestrator");
        orchestrator
            .state()
            .intake()
            .spawn(std::future::pending::<()>());

        shutdown.cancel();
        let started = tokio::time::Instant::now();
        orchestrator.run().await.expect("run");

        assert!(
            started.elapsed() >= INTAKE_DRAIN_DEADLINE,
            "gave up after {:?}",
            started.elapsed()
        );
    }

    /// A gRPC-only fetcher has no broker, so its DLQ builds no Kafka backend
    /// and no Kafka client, and a dead letter lands in the file backend.
    #[tokio::test]
    async fn a_grpc_only_dead_letter_lands_in_the_file_dlq_and_no_kafka_client_is_built() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.output.output_type = "grpc".to_string();
        config.dlq.enabled = true;
        config.dlq.file.path = dir.path().to_path_buf();
        assert_eq!(config.dlq.mode, scalo::dlq::DlqMode::Cascade, "the default");
        assert!(config.dlq.kafka.enabled, "the shipped default");

        let (dlq_config, kafka) = dlq_backends(&config);
        assert!(
            kafka.is_none(),
            "a Kafka client config reached Dlq::spawn on a gRPC-only output"
        );
        assert!(
            !dlq_config.kafka.enabled,
            "the gRPC-only DLQ asked for a Kafka backend"
        );

        let state = PipelineState::new(
            SharedConfig::new(config),
            Arc::new(Metrics::new()),
            None,
            CancellationToken::new(),
        )
        .expect("pipeline state");
        let letter = DeadLetter {
            topic: Arc::from("t_land"),
            payload: Bytes::from_static(br#"{"id":1}"#),
            reason: "refused".into(),
        };
        assert_eq!(
            state.dead_letter(vec![letter]).await.expect("held"),
            DeadLettered::Held { dropped: 0 }
        );
        let written = std::fs::read_to_string(dir.path().join("dfe-fetcher/dlq.ndjson"))
            .expect("the DLQ file");
        assert_eq!(written.lines().count(), 1);
    }

    /// The env's pressure threshold wins over `buffer.pressure_threshold`,
    /// which holds its default whether or not the file sets it. With the env
    /// unset, the buffer value applies.
    #[test]
    #[allow(unsafe_code)]
    fn the_env_pressure_threshold_wins_over_the_buffer_default() {
        const VAR: &str = "DFE_FETCHER_MEMORY_PRESSURE_THRESHOLD";
        let mut config = Config::default();
        config.buffer.pressure_threshold = 0.7;
        // SAFETY: test-only, and nextest runs each test in its own process.
        unsafe { std::env::set_var(VAR, "0.5") };
        let from_env = memory_guard_config(&config).pressure_threshold;
        unsafe { std::env::remove_var(VAR) };
        let from_buffer = memory_guard_config(&config).pressure_threshold;
        assert!((from_env - 0.5).abs() < f64::EPSILON, "env: {from_env}");
        assert!(
            (from_buffer - 0.7).abs() < f64::EPSILON,
            "buffer: {from_buffer}"
        );
    }

    /// An output that reaches the bus keeps the Kafka backend, on the same
    /// producer config the output uses.
    #[test]
    fn a_bus_output_keeps_its_kafka_dlq_backend() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["broker:9092".into()];
        for output_type in ["kafka", "both"] {
            config.output.output_type = output_type.to_string();
            let (dlq_config, kafka) = dlq_backends(&config);
            assert!(dlq_config.kafka.enabled, "{output_type}");
            assert_eq!(
                kafka.expect("the Kafka client config").brokers,
                ["broker:9092"],
                "{output_type}"
            );
        }
    }

    /// A DLQ write is counted only once a backend confirms it. With no DLQ the
    /// caller hears that nothing holds the records.
    #[tokio::test]
    async fn a_dead_letter_is_confirmed_before_it_counts() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq_config = Config::default().dlq;
        dlq_config.enabled = true;
        dlq_config.mode = scalo::dlq::DlqMode::FileOnly;
        dlq_config.file.path = dir.path().to_path_buf();
        let dlq =
            Dlq::spawn(&dlq_config, "dfe-fetcher", None, CancellationToken::new()).expect("dlq");
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::for_tests_with_dlq(
            SharedConfig::new(Config::default()),
            Arc::clone(&metrics),
            None,
            dlq,
        );
        let letter = |id: u8| DeadLetter {
            topic: Arc::from("t_land"),
            payload: Bytes::from(format!(r#"{{"id":{id}}}"#)),
            reason: "refused".into(),
        };

        assert_eq!(
            state
                .dead_letter(vec![letter(1), letter(2)])
                .await
                .expect("held"),
            DeadLettered::Held { dropped: 0 }
        );
        let lines = std::fs::read_to_string(dir.path().join("dfe-fetcher/dlq.ndjson"))
            .expect("the DLQ file");
        assert_eq!(lines.lines().count(), 2, "on disk before the call returned");

        // The service directory replaced by a file: the next write is refused.
        std::fs::remove_dir_all(dir.path().join("dfe-fetcher")).unwrap();
        std::fs::write(dir.path().join("dfe-fetcher"), b"not a directory").unwrap();
        let refused = state.dead_letter(vec![letter(3)]).await;
        assert!(matches!(refused, Err(Error::Dlq(_))), "{refused:?}");

        let without = make_pipeline_state();
        assert_eq!(
            without.dead_letter(vec![letter(4)]).await.expect("no DLQ"),
            DeadLettered::NoQueue
        );
    }

    /// A dead letter over the only DLQ backend's ceiling can never be held,
    /// so it is dropped and counted instead of failing the write on every
    /// attempt and holding its source for good.
    #[tokio::test]
    async fn a_dead_letter_no_dlq_backend_can_hold_is_dropped_and_counted() {
        let recorder = crate::metrics::recorded::Recorder::new();
        let _recording = recorder.install();
        // Nothing listens here: the ceiling is the producer's own.
        let mut kafka = scalo::transport::KafkaConfig {
            brokers: vec!["127.0.0.1:1".into()],
            group: String::new(),
            ..scalo::transport::KafkaConfig::default()
        };
        kafka.sizing.producer.message_max_bytes = Some(2_000);
        let mut dlq_config = Config::default().dlq;
        dlq_config.enabled = true;
        dlq_config.mode = scalo::dlq::DlqMode::KafkaOnly;
        dlq_config.file.enabled = false;
        dlq_config.kafka.enabled = true;
        dlq_config.kafka.send_timeout_ms = 300;
        let dlq = Dlq::spawn(
            &dlq_config,
            "dfe-fetcher",
            Some(&kafka),
            CancellationToken::new(),
        )
        .expect("dlq");
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::for_tests_with_dlq(
            SharedConfig::new(Config::default()),
            Arc::clone(&metrics),
            None,
            dlq,
        );
        let big = DeadLetter {
            topic: Arc::from("t_land"),
            payload: Bytes::from(vec![b'x'; 4_000]),
            reason: "refused".into(),
        };

        let outcome = tokio::time::timeout(Duration::from_secs(5), state.dead_letter(vec![big]))
            .await
            .expect("answered at once, not held")
            .expect("a refusal is not a DLQ failure");

        assert_eq!(outcome, DeadLettered::Held { dropped: 1 });
        assert_eq!(metrics.dead_letters_dropped(), 1);
        assert_eq!(
            recorder.counter(
                "pipeline_dead_letters_dropped_total",
                &[("reason", "too_large")]
            ),
            Some(1)
        );
    }

    // -- update_metrics --

    #[tokio::test]
    async fn test_update_metrics_writes_memory_gauges() {
        let config = Config::default();
        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        // Usage pinned to the reservation counter so the used gauge reads the
        // 4096 reserved below, not the test process's resident size.
        let state = PipelineState {
            shared_config: shared,
            output: None,
            memory_guard: Arc::new(MemoryGuard::with_usage_source(
                scalo::memory::MemoryGuardConfig {
                    limit_bytes: 524_288_000, // 500 MB
                    pressure_threshold: 0.8,
                    ..Default::default()
                },
                scalo::memory::UsageSource::Reservations,
            )),
            dlq: None,
            metrics: Arc::clone(&metrics),
            ready: AtomicBool::new(true),
            intake: TaskTracker::new(),
        };

        // Push some bytes through the guard so current_bytes > 0
        state.memory_guard.add_bytes(4096);
        state.update_metrics(&metrics).await;

        // Verify the gauges were written by rendering the prom output
        let rendered = metrics.render();
        assert!(
            rendered.contains("dfe_fetcher_memory_limit_bytes 524288000"),
            "limit gauge should be updated: \n{rendered}"
        );
        assert!(
            rendered.contains("dfe_fetcher_memory_used_bytes 4096"),
            "used gauge should be updated: \n{rendered}"
        );
    }

    // -- enrich_record against payloads that already carry a reserved key --

    /// Count the top-level occurrences of a key name in the raw output. A
    /// `serde_json` parse cannot see a duplicate (it keeps the last), so
    /// duplicate-key assertions have to be made on the bytes.
    fn key_occurrences(enriched: &Bytes, key: &str) -> usize {
        let needle = format!("\"{key}\":");
        std::str::from_utf8(enriched)
            .unwrap()
            .matches(&needle)
            .count()
    }

    #[test]
    fn test_enrich_record_with_existing_timestamp_received() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"event":"x","_timestamp_received":12345}"#);
        let enriched = stamped(state.enrich_record(payload, "ingest", "ingest.source"));

        assert_eq!(key_occurrences(&enriched, "_timestamp_received"), 1);
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["event"], "x");
        assert_eq!(parsed["_source"], "ingest");
        assert_eq!(parsed["_source_fetcher"], "ingest.source");
        assert!(parsed["_timestamp_fetcher"].is_number());
        assert!(parsed["_timestamp_received"].is_number());
        // The caller's stamp is kept beside ours, not dropped.
        assert_eq!(parsed["_timestamp_received_original"], 12345);
    }

    #[test]
    fn test_enrich_record_with_existing_source() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"event":"x","_source":"producer-set"}"#);
        let enriched = stamped(state.enrich_record(payload, "ingest", "ingest.source"));

        assert_eq!(
            key_occurrences(&enriched, "_source"),
            1,
            "two _source keys make ClickHouse reject the record"
        );
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["event"], "x");
        // The DFE source name wins -- it is what dfe-loader routes on.
        assert_eq!(parsed["_source"], "ingest");
        assert_eq!(parsed["_source_original"], "producer-set");
        assert_eq!(parsed["_source_fetcher"], "ingest.source");
    }

    #[test]
    fn test_enrich_record_with_existing_source_fetcher() {
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"event":"x","_source_fetcher":"producer-set"}"#);
        let enriched = stamped(state.enrich_record(payload, "ingest", "ingest.source"));

        assert_eq!(key_occurrences(&enriched, "_source_fetcher"), 1);
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_source_fetcher"], "ingest.source");
        assert_eq!(parsed["_source_fetcher_original"], "producer-set");
        assert_eq!(parsed["_source"], "ingest");
    }

    #[test]
    fn test_enrich_record_with_every_reserved_key_present() {
        let state = make_pipeline_state();
        let payload = Bytes::from(
            r#"{"event":"x","_source":"a","_source_fetcher":"b","_timestamp_fetcher":1,"_timestamp_received":2}"#,
        );
        let enriched = stamped(state.enrich_record(payload, "ingest", "ingest.source"));

        for key in RESERVED_KEYS {
            assert_eq!(key_occurrences(&enriched, key), 1, "duplicate {key}");
        }
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_source"], "ingest");
        assert_eq!(parsed["_source_fetcher"], "ingest.source");
        assert_eq!(parsed["_source_original"], "a");
        assert_eq!(parsed["_source_fetcher_original"], "b");
        assert_eq!(parsed["_timestamp_fetcher_original"], 1);
        assert_eq!(parsed["_timestamp_received_original"], 2);
    }

    #[test]
    fn test_enrich_record_nested_reserved_key_takes_the_rewrite_path() {
        // A nested `_source` is not a top-level collision, but the pre-filter
        // matches it, so the record is parsed and rebuilt. The rewrite leaves
        // the nested key where it is and adds exactly one top-level `_source`.
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"inner":{"_source":"nested"}}"#);
        let enriched = stamped(state.enrich_record(payload, "ingest", "ingest.source"));

        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_source"], "ingest");
        assert_eq!(parsed["inner"]["_source"], "nested");
        assert!(parsed.get("_source_original").is_none());
    }

    #[test]
    fn test_enrich_record_without_a_reserved_key_takes_the_append_path() {
        // No reserved key and no escape means no parse: the payload is copied
        // byte for byte and the envelope is appended before the closing brace.
        let state = make_pipeline_state();
        let raw = r#"{"event":"x","id":7,"nested":{"a":[1,2]},"note":"plain text"}"#;
        let enriched = stamped(state.enrich_record(Bytes::from(raw), "ingest", "ingest.source"));
        let enriched_str = std::str::from_utf8(&enriched).unwrap();

        let head = &raw[..raw.len() - 1];
        assert!(
            enriched_str.starts_with(head),
            "append path must not re-serialise the payload: {enriched_str}"
        );
        let envelope = &enriched_str[head.len()..];
        assert!(
            envelope.starts_with(",\"_timestamp_fetcher\":"),
            "{envelope}"
        );
        assert!(envelope.ends_with('}'), "{envelope}");
        assert!(!envelope.contains("_original"), "{envelope}");
    }

    #[test]
    fn test_enrich_record_append_path_escapes_the_source_names() {
        // A quote or backslash in a source name has to be escaped by serde or
        // the append path emits a record no parser accepts.
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"event":"x"}"#);
        let enriched = stamped(state.enrich_record(payload, r#"in"gest"#, r#"c:\logs\"a""#));

        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_source"], r#"in"gest"#);
        assert_eq!(parsed["_source_fetcher"], r#"c:\logs\"a""#);
    }

    #[test]
    fn test_enrich_record_keeps_an_existing_original() {
        // A payload carrying both `_source` and `_source_original` must not
        // lose the one it already had.
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"_source":"producer-set","_source_original":"first-hop"}"#);
        let enriched = stamped(state.enrich_record(payload, "ingest", "ingest.source"));

        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_source"], "ingest");
        assert_eq!(parsed["_source_original"], "first-hop");
        assert_eq!(parsed["_source_original_2"], "producer-set");
    }

    #[test]
    fn test_enrich_record_replay_keeps_the_first_original() {
        // Enriching an already-enriched record is the replay case: the second
        // pass parks its value under _original_2 rather than overwriting.
        let state = make_pipeline_state();
        let payload = Bytes::from(r#"{"event":"x","_source":"producer-set"}"#);
        let once = stamped(state.enrich_record(payload, "ingest", "ingest.source"));
        let twice = stamped(state.enrich_record(once, "replay", "replay.source"));

        let parsed: serde_json::Value = serde_json::from_slice(&twice).unwrap();
        assert_eq!(parsed["_source"], "replay");
        assert_eq!(parsed["_source_original"], "producer-set");
        assert_eq!(parsed["_source_original_2"], "ingest");
        assert_eq!(parsed["_source_fetcher_original"], "ingest.source");
    }

    #[test]
    fn test_enrich_record_every_reserved_key_keeps_its_existing_original() {
        let state = make_pipeline_state();
        let payload = Bytes::from(
            r#"{"_source":"a","_source_original":"a0","_source_fetcher":"b","_source_fetcher_original":"b0","_timestamp_fetcher":1,"_timestamp_fetcher_original":10,"_timestamp_received":2,"_timestamp_received_original":20}"#,
        );
        let enriched = stamped(state.enrich_record(payload, "ingest", "ingest.source"));

        for key in RESERVED_KEYS {
            assert_eq!(key_occurrences(&enriched, key), 1, "duplicate {key}");
        }
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_source_original"], "a0");
        assert_eq!(parsed["_source_original_2"], "a");
        assert_eq!(parsed["_source_fetcher_original"], "b0");
        assert_eq!(parsed["_source_fetcher_original_2"], "b");
        assert_eq!(parsed["_timestamp_fetcher_original"], 10);
        assert_eq!(parsed["_timestamp_fetcher_original_2"], 1);
        assert_eq!(parsed["_timestamp_received_original"], 20);
        assert_eq!(parsed["_timestamp_received_original_2"], 2);
    }

    #[test]
    fn test_park_original_walks_past_every_taken_name() {
        let mut map = serde_json::Map::new();
        map.insert("_source_original".to_string(), "a".into());
        map.insert("_source_original_2".to_string(), "b".into());
        map.insert("_source_original_3".to_string(), "c".into());

        park_original(&mut map, "_source", "d".into());

        assert_eq!(map["_source_original"], "a");
        assert_eq!(map["_source_original_4"], "d");
    }

    #[test]
    fn test_park_original_with_many_taken_names_parks_at_the_first_free_slot() {
        // The walk reuses one buffer, so a long run of taken names must still
        // land on the first free slot rather than skipping or reusing one.
        let mut map = serde_json::Map::new();
        map.insert("_source_original".to_string(), "a".into());
        for n in 2..=2_000u32 {
            map.insert(format!("_source_original_{n}"), n.into());
        }

        park_original(&mut map, "_source", "parked".into());

        assert_eq!(map["_source_original"], "a");
        assert_eq!(map["_source_original_2000"], 2_000);
        assert_eq!(map["_source_original_2001"], "parked");
    }

    #[test]
    fn test_stamp_escapes_the_literals_once() {
        // The literals the append path writes carry serde's escaping, quotes
        // included, so a quote or backslash in a name or an org cannot break
        // the record.
        let stamp = Stamp::new(r#"in"gest"#, r"c:\logs", r#"ac"me"#);

        assert_eq!(stamp.dfe_source_literal, r#""in\"gest""#);
        assert_eq!(stamp.source_literal, r#""c:\\logs""#);
        assert_eq!(stamp.org_literal, r#""ac\"me""#);
        assert_eq!(stamp.dfe_source, r#"in"gest"#);
        assert_eq!(stamp.source, r"c:\logs");
        assert_eq!(stamp.org, r#"ac"me"#);
    }

    /// A pipeline over `yaml` as the config file, reservation-counted like
    /// [`make_pipeline_state`].
    fn pipeline_over(yaml: &str) -> PipelineState {
        let (config, _) = Config::parse_yaml(yaml, "fetcher.yaml").expect("config");
        PipelineState::for_tests(SharedConfig::new(config), Arc::new(Metrics::new()), None)
    }

    /// With no org bound, every record carries the built-in default org, on
    /// the append path and the rewrite path alike.
    #[test]
    fn every_record_carries_the_default_org_when_none_is_bound() {
        let state = make_pipeline_state();
        for payload in [r#"{"event":"x"}"#, r#"{"event":"x","_source":"theirs"}"#] {
            let enriched =
                stamped(state.enrich_record(Bytes::from(payload), "ingest", "ingest.source"));
            assert_eq!(key_occurrences(&enriched, "_org_id"), 1, "{payload}");
            let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
            assert_eq!(parsed["_org_id"], "default", "{payload}");
        }
    }

    /// The org the config binds is the one every record carries, and an empty
    /// binding takes the deployment default.
    #[test]
    fn every_record_carries_the_bound_org_and_an_empty_binding_takes_the_default() {
        let bound = pipeline_over("org_id: acme\n");
        let enriched =
            stamped(bound.enrich_record(Bytes::from(r#"{"event":"x"}"#), "ingest", "i.s"));
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_org_id"], "acme");

        let empty = pipeline_over("org_id: ''\ndefault_org_id: house\n");
        let enriched =
            stamped(empty.enrich_record(Bytes::from(r#"{"event":"x"}"#), "ingest", "i.s"));
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_org_id"], "house");
    }

    /// An `_org_id` the fetched data carried is parked as `_org_id_original`,
    /// as `_source` is, and the bound org takes the name: a record cannot pick
    /// its own tenant.
    #[test]
    fn a_fetched_org_id_is_parked_and_the_bound_org_takes_its_place() {
        let state = pipeline_over("org_id: acme\n");
        let enriched = stamped(state.enrich_record(
            Bytes::from(r#"{"event":"x","_org_id":"someone-else"}"#),
            "ingest",
            "ingest.source",
        ));
        assert_eq!(
            key_occurrences(&enriched, "_org_id"),
            1,
            "two _org_id keys make ClickHouse reject the record"
        );
        let parsed: serde_json::Value = serde_json::from_slice(&enriched).unwrap();
        assert_eq!(parsed["_org_id"], "acme");
        assert_eq!(parsed["_org_id_original"], "someone-else");
        assert_eq!(parsed["event"], "x");

        let replayed = stamped(state.enrich_record(enriched, "ingest", "ingest.source"));
        let parsed: serde_json::Value = serde_json::from_slice(&replayed).unwrap();
        assert_eq!(parsed["_org_id"], "acme");
        assert_eq!(parsed["_org_id_original"], "someone-else");
        assert_eq!(parsed["_org_id_original_2"], "acme");
    }

    /// Every top-level key as JSON decodes it, duplicates kept, so a key spelled
    /// with an escape counts as the name it decodes to.
    fn decoded_keys(enriched: &[u8]) -> Vec<(String, serde_json::Value)> {
        struct Pairs(Vec<(String, serde_json::Value)>);
        impl<'de> serde::Deserialize<'de> for Pairs {
            fn deserialize<D: serde::Deserializer<'de>>(
                d: D,
            ) -> std::result::Result<Self, D::Error> {
                struct Visit;
                impl<'de> serde::de::Visitor<'de> for Visit {
                    type Value = Pairs;
                    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.write_str("a JSON object")
                    }
                    fn visit_map<A: serde::de::MapAccess<'de>>(
                        self,
                        mut map: A,
                    ) -> std::result::Result<Pairs, A::Error> {
                        let mut pairs = Vec::new();
                        while let Some(pair) = map.next_entry()? {
                            pairs.push(pair);
                        }
                        Ok(Pairs(pairs))
                    }
                }
                d.deserialize_map(Visit)
            }
        }
        serde_json::from_slice::<Pairs>(enriched)
            .expect("a JSON object")
            .0
    }

    /// A reserved key spelled with a JSON escape decodes to the reserved name,
    /// so it is parked like the plain spelling: the record carries one
    /// `_org_id` and one `_source`, both the fetcher's.
    #[test]
    fn an_escaped_reserved_key_is_parked_like_the_plain_one() {
        let state = pipeline_over("org_id: acme\n");
        // The JSON escape for an underscore, built from the backslash's code point.
        let underscore = format!("{}u005f", char::from(0x5c));
        let cases = [
            (
                format!(r#"{{"{underscore}org_id":"someone-else","a":1}}"#),
                "_org_id",
                "someone-else",
            ),
            (
                format!(r#"{{"_org{underscore}id":"someone-else","a":1}}"#),
                "_org_id",
                "someone-else",
            ),
            (
                format!(r#"{{"{underscore}source":"other_table","a":1}}"#),
                "_source",
                "other_table",
            ),
        ];
        let mut got = Vec::new();
        let mut want = Vec::new();
        for (payload, key, fetched) in &cases {
            assert!(payload.contains(char::from(0x5c)), "{payload}");
            let enriched = stamped(state.enrich_record(
                Bytes::from(payload.clone()),
                "ingest",
                "ingest.source",
            ));
            let keys = decoded_keys(&enriched);
            let values = |name: &str| -> Vec<serde_json::Value> {
                keys.iter()
                    .filter(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
                    .collect()
            };
            let parked = format!("{key}_original");
            got.push((
                payload.as_str(),
                values("_org_id"),
                values("_source"),
                values(&parked),
            ));
            want.push((
                payload.as_str(),
                vec![serde_json::json!("acme")],
                vec![serde_json::json!("ingest")],
                vec![serde_json::json!(fetched)],
            ));
        }
        assert_eq!(got, want);
    }

    #[test]
    fn test_may_carry_reserved_key() {
        assert!(may_carry_reserved_key(br#"{"_source":"x"}"#));
        assert!(may_carry_reserved_key(br#"{"_timestamp_received":1}"#));
        assert!(may_carry_reserved_key(br#"{"_org_id":"x"}"#));
        assert!(!may_carry_reserved_key(br#"{"event":"x","id":7}"#));
        assert!(!may_carry_reserved_key(b"{}"));
        let escaped = format!(r#"{{"path":"c:{0}{0}logs"}}"#, char::from(0x5c));
        assert!(may_carry_reserved_key(escaped.as_bytes()));
    }

    /// Exercise PipelineState::new with DLQ enabled to cover the DLQ init branch.
    #[tokio::test]
    async fn test_pipeline_state_new_with_dlq_enabled() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.dlq.enabled = true;
        config.dlq.file.path = tmp.path().join("dlq");

        let shared = SharedConfig::new(config);
        let metrics = Arc::new(Metrics::new());
        let state = PipelineState::new(shared, metrics, None, CancellationToken::new())
            .expect("state creation must succeed");

        assert!(state.dlq.is_some(), "the DLQ init branch ran");
    }

    /// Exercise shared_config() getter.
    #[test]
    fn test_shared_config_getter_returns_handle() {
        let state = make_pipeline_state();
        let shared = state.shared_config();
        let config = shared.get();
        assert_eq!(config.scheduler.default_interval_secs, 300);
    }
}
