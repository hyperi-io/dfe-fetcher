// Project:   dfe-fetcher
// File:      crates/fetcher/src/config/mod.rs
// Purpose:   Configuration loading and validation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration management using scalo's 7-layer cascade.
//!
//! Priority (highest to lowest):
//! 1. CLI arguments
//! 2. Flat env overrides (`DFE_FETCHER_KAFKA_BROKERS`, see [`ApplyFlatEnv`])
//! 3. Nested env vars, `__` between levels, either separator after the prefix:
//!    `DFE_FETCHER__SOURCES__AWS__ACCESS_KEY_ID` (the chart / deployment-contract
//!    form) and `DFE_FETCHER_SOURCES__AWS__ACCESS_KEY_ID` (the docs form) reach
//!    the same key
//! 4. .env file
//! 5. The file named by `--config`, else settings.{env}.yaml / settings.yaml /
//!    defaults.yaml from the scalo cascade
//! 6. Hard-coded defaults
//!
//! Layer 3 applies on BOTH load paths. `--config` bypasses the scalo cascade
//! (it discovers files by name, and the container mounts `fetcher.yaml`), so
//! without the env merge in `apply_nested_env` every env var above would be
//! silently dropped in exactly the deployment that sets them.

pub mod builtin;
pub mod resolve;
mod shared;

pub use builtin::{Block, BuiltinInstance, REGISTRY};
pub use shared::SharedConfig;

use std::collections::{BTreeMap, BTreeSet, HashMap};

use dfe_fetcher_core::batch::AccumulateConfig;
use dfe_fetcher_core::envelope::OversizePolicy;
use dfe_fetcher_core::secret::spec_issue;
use dfe_fetcher_db::DbInstance;
use dfe_fetcher_file::FileInstance;
use dfe_fetcher_rest::RestInstance;
use scalo::config::flat_env::{self, ApplyFlatEnv};
use scalo::config::sensitive::SensitiveString;
use scalo::config::{self, ConfigOptions};
use scalo::dlq::DlqConfig;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Environment variable prefix for configuration.
pub const ENV_PREFIX: &str = "DFE_FETCHER";

/// Main configuration struct.
///
/// ## Hot-reload behaviour
///
/// The config file is watched for changes. When reloaded, values are
/// available via `SharedConfig::get()`. However, not all settings take
/// effect without a restart.
///
/// **Hot-reloaded (takes effect on next fetch cycle):**
/// - `scheduler.default_interval_secs` -- fetch interval re-computed each cycle
/// - `scheduler.jitter_percent` -- jitter re-computed each cycle
/// - `kafka.topic_suffix` -- topic name suffix re-read on each delivery
/// - `sources.*.filter` -- CEL filter re-evaluated on each record
/// - `cursor.default_window_hours` -- the cap on one tick's window, re-read each cycle
/// - dropping a source, or `sources.*.enabled: false` -- its fetch task is cancelled on reload
///
/// **Requires pod restart:**
/// - `output.*` -- transport connections established at startup
/// - `kafka.*` (except `topic_suffix`) -- transport config bound at startup
/// - `ingest.*` -- HTTP server binds at startup, auth token resolved once
/// - `extractors.*` -- containers and Vector instances spawned at startup
/// - `metrics.*` -- metrics server binds at startup
/// - `instance_id` -- cursor key prefix set at startup
/// - `cursor.directory` -- cursor store created at startup
/// - adding a source, or `sources.*.enabled: true` -- source registration at startup
/// - `sources.*.credential_secret`, `tenant_id`, `subscription_id`, `project_id` -- resolved once at startup
/// - `scheduler.max_concurrent_fetches` -- semaphore created at startup
/// - `dlq.*` -- DLQ created at startup
/// - `buffer.*` -- buffer manager created at startup
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct Config {
    /// Scheduler configuration.
    pub scheduler: SchedulerConfig,

    /// Native source modules configuration.
    pub sources: SourcesConfig,

    /// External extractors configuration (containers, vector).
    pub extractors: ExtractorsConfig,

    /// HTTP ingest server configuration (for container extractors to post data).
    pub ingest: IngestConfig,

    /// Kafka producer configuration (output).
    pub kafka: KafkaConfig,

    /// Buffer and memory configuration.
    pub buffer: BufferConfig,

    /// Metrics configuration.
    pub metrics: MetricsConfig,

    /// Dead letter queue configuration.
    #[serde(default)]
    pub dlq: DlqConfig,

    /// Periodic config reload interval in seconds (0 = disabled, SIGHUP only).
    #[serde(default)]
    pub config_reload_secs: u64,

    /// Instance identity for cursor isolation across multiple fetcher pods.
    /// Auto-derived from source config if omitted.
    #[serde(default)]
    pub instance_id: Option<String>,

    /// Output transport configuration.
    #[serde(default)]
    pub output: OutputConfig,

    /// Cursor store configuration.
    #[serde(default)]
    pub cursor: CursorConfig,

    /// Scaling pressure configuration for KEDA autoscaling.
    #[serde(default)]
    pub scaling: scalo::scaling::ScalingPressureConfig,

    /// Recursively unwrap double-serialised JSON string fields before delivery.
    ///
    /// When enabled (default), any string field whose value parses as a JSON
    /// object or array is replaced with the parsed value. This makes fields
    /// like AWS CloudTrail's `CloudTrailEvent` queryable as nested JSON in
    /// downstream systems like ClickHouse.
    ///
    /// Set to `false` for sources that intentionally store JSON as strings.
    #[serde(default = "default_unwrap_nested_json")]
    pub unwrap_nested_json: bool,

    /// Batch bounds for framework sources: rows and bytes per flush, and how
    /// long a partial batch waits. Overridable per source under
    /// `sources.rest.<id>.accumulate`.
    #[serde(default)]
    pub accumulate: AccumulateConfig,

    /// When a framework row is too large to send, and how much of it the
    /// dead-letter copy keeps.
    #[serde(default)]
    pub oversize: OversizePolicy,

    /// The memory-pressure brake on framework sources: when the memory guard
    /// crosses `pause_above` a source stops polling its provider until the
    /// buffer drains below `resume_below`.
    #[serde(default)]
    pub self_regulation: SelfRegulationConfig,

    /// Path to the config file (set by loader, not deserialized).
    #[serde(skip)]
    pub config_path: Option<String>,
}

/// The memory-pressure brake, `self_regulation` in the cascade.
///
/// A local section rather than scalo's own because that one carries no JSON
/// schema and the deployment contract derives one from every section here.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SelfRegulationConfig {
    /// Master switch; off means framework sources never pause for pressure.
    pub enabled: bool,
    /// Memory pressure ratio at which polling pauses.
    pub pause_above: f64,
    /// Memory pressure ratio at which polling resumes; below `pause_above`.
    pub resume_below: f64,
    /// Longest one pause lasts: memory the pause itself cannot release (a
    /// pumped cursor, a file chunk, an open row) may hold the ratio above
    /// `resume_below` for ever, so after this many seconds the source flushes
    /// what it holds and polls on; `self_regulation_inbound_paused` is the
    /// signal an alert watches for a pause that keeps recurring.
    pub max_hold_secs: u64,
}

impl Default for SelfRegulationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            pause_above: 0.80,
            resume_below: 0.65,
            max_hold_secs: 30,
        }
    }
}

impl SelfRegulationConfig {
    /// The hysteresis band, when the brake is on.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the band is not `0 <= resume_below <
    /// pause_above <= 1`.
    pub fn hysteresis(&self) -> Result<Option<scalo::governor::Hysteresis>> {
        if !self.enabled {
            return Ok(None);
        }
        if self.max_hold_secs == 0 {
            return Err(Error::Config(
                "self_regulation.max_hold_secs must be > 0".into(),
            ));
        }
        scalo::governor::Hysteresis::new(self.pause_above, self.resume_below)
            .map(Some)
            .map_err(|e| Error::Config(format!("self_regulation: {e}")))
    }

    /// The longest one pause lasts.
    #[must_use]
    pub fn max_hold(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.max_hold_secs)
    }
}

const fn default_unwrap_nested_json() -> bool {
    true
}

impl Default for Config {
    fn default() -> Self {
        Self {
            scheduler: SchedulerConfig::default(),
            sources: SourcesConfig::default(),
            extractors: ExtractorsConfig::default(),
            ingest: IngestConfig::default(),
            kafka: KafkaConfig::default(),
            buffer: BufferConfig::default(),
            metrics: MetricsConfig::default(),
            // Fleet DLQ standard defaults: fixed per-app topic, routing=common.
            // Fetcher entries carry topic destinations, so scalo's per-table
            // default would target `{topic}.dlq` names nothing creates.
            // Applies when the config file has no `dlq:` key; a partial `dlq:`
            // block reverts nested fields to scalo's own defaults.
            dlq: {
                let mut dlq = DlqConfig::default();
                dlq.kafka.routing = scalo::dlq::DlqRouting::Common;
                dlq.kafka.common_topic = "dfe_fetcher_dlq".to_string();
                dlq
            },
            config_reload_secs: 0,
            instance_id: None,
            output: OutputConfig::default(),
            cursor: CursorConfig::default(),
            scaling: scalo::scaling::ScalingPressureConfig::default(),
            unwrap_nested_json: default_unwrap_nested_json(),
            accumulate: AccumulateConfig::default(),
            oversize: OversizePolicy::default(),
            self_regulation: SelfRegulationConfig::default(),
            config_path: None,
        }
    }
}

impl Config {
    /// The output topic suffix in force, `output.topic_suffix` over the legacy
    /// `kafka.topic_suffix`.
    ///
    /// Every path that builds a topic or derives `_source` from one must use
    /// this: a caller that reads `kafka.topic_suffix` directly publishes to a
    /// different topic than the native sources do once `output.topic_suffix`
    /// is set, and leaves the suffix on `_source`, which the receiver routes on.
    #[must_use]
    pub fn topic_suffix(&self) -> &str {
        self.output
            .topic_suffix
            .as_deref()
            .unwrap_or(&self.kafka.topic_suffix)
    }

    /// Whether anything in this config can produce a record: an enabled source,
    /// a container extractor, or the ingest listener.
    ///
    /// The idle gate's `work_state` and the transport checks in
    /// [`validate`](Self::validate) read the same predicate, so an empty config
    /// cannot idle by one definition and be refused by another.
    #[must_use]
    pub fn has_work(&self) -> bool {
        self.sources.any_enabled() || !self.extractors.containers.is_empty() || self.ingest.enabled
    }

    /// Load configuration with cascade: CLI -> ENV -> .env -> file -> defaults
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        // If an explicit config file is provided, load it directly
        if let Some(path) = config_path {
            return Self::load_from_file(path);
        }

        // Otherwise, use scalo's 7-layer cascade
        config::setup(ConfigOptions {
            env_prefix: ENV_PREFIX.to_string(),
            config_paths: Vec::new(),
            load_dotenv: true,
            ..Default::default()
        })
        .map_err(|e| Error::Config(format!("failed to setup config: {e}")))?;

        // Get the global config and unmarshal to our struct
        let cfg = config::get();

        // A deserialise failure is fatal. Defaulting instead discards the whole
        // file over one mistyped value, leaving a fetcher with no sources
        // enabled that reports healthy and fetches nothing. Every field is
        // `#[serde(default)]`, so an absent config file deserialises cleanly.
        let mut config: Config = cfg
            .unmarshal()
            .map_err(|e| Error::Config(format!("failed to parse configuration: {e}")))?;

        // Store config path for reload support
        config.config_path = config_path.map(String::from);

        // scalo's own merge strips exactly `DFE_FETCHER_`, so the chart's
        // `DFE_FETCHER__...` form lands on `_sources.aws....` and is dropped.
        // Re-run the merge with the separator trimmed so both forms reach the key.
        apply_nested_env(&mut config)?;

        // Apply flat env var overrides (DFE_FETCHER_*)
        config.apply_flat_env("DFE_FETCHER");

        Ok(config)
    }

    /// Load configuration from a YAML file directly.
    ///
    /// This is the path every container takes: the entrypoint passes
    /// `--config /etc/dfe/fetcher.yaml`, and the scalo cascade can only
    /// discover files it names itself. Env layering therefore has to happen
    /// here too, or the deployment's env vars reach nothing.
    pub fn load_from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("failed to read config file: {e}")))?;

        let mut config: Config = serde_yaml_ng::from_str(&content)?;
        config.config_path = Some(path.to_string());
        apply_nested_env(&mut config)?;
        config.apply_flat_env("DFE_FETCHER");
        Ok(config)
    }

    /// Register all config sections in the global config registry.
    /// Enables the /config debug endpoint and change notifications.
    pub fn register_in_registry(&self) {
        use scalo::config::registry;
        registry::register("scheduler", &self.scheduler);
        registry::register("sources", &self.sources);
        registry::register("extractors", &self.extractors);
        registry::register("ingest", &self.ingest);
        registry::register("kafka", &self.kafka);
        registry::register("buffer", &self.buffer);
        registry::register("metrics", &self.metrics);
        registry::register("dlq", &self.dlq);
        registry::register("output", &self.output);
        registry::register("cursor", &self.cursor);
        registry::register("scaling", &self.scaling);
        registry::register("accumulate", &self.accumulate);
        registry::register("oversize", &self.oversize);
        registry::register("self_regulation", &self.self_regulation);
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        // A missing broker or endpoint is a contradiction only once something
        // would send to it; with nothing to send, the config is valid but empty
        // of work and the idle gate holds it (main.rs `work_state`).
        if self.has_work() {
            if self.output.includes_kafka() {
                // Check output.kafka first, then legacy kafka section
                let has_output_brokers = self
                    .output
                    .kafka
                    .as_ref()
                    .is_some_and(|k| !k.brokers.is_empty());
                if !has_output_brokers && self.kafka.brokers.is_empty() {
                    return Err(Error::Config(
                        "kafka brokers required when output.type includes kafka".into(),
                    ));
                }
            }
            if self.output.includes_grpc()
                && self
                    .output
                    .grpc
                    .as_ref()
                    .is_none_or(|g| g.endpoint.is_none())
            {
                return Err(Error::Config(
                    "grpc.endpoint required when output.type includes grpc".into(),
                ));
            }
        }

        self.output.validate_routes()?;

        // A DLQ that only writes to Kafka has nowhere to go on a deployment
        // with no broker, and a silently dropped dead-letter is worse than a
        // refused config.
        if self.dlq.enabled
            && self.dlq.mode == scalo::dlq::DlqMode::KafkaOnly
            && !self.output.uses_bus()
        {
            return Err(Error::Config(
                "dlq.mode: kafka_only has no broker to write to when the output is gRPC-only; \
                 set dlq.enabled: false (backpressure holds the batch instead) or give the \
                 DLQ its own brokers"
                    .into(),
            ));
        }

        // `metrics.enabled` reaches nothing: the runtime always starts the
        // metrics server, and the same listener serves /livez and /readyz, so
        // there is no build in which switching it off is honoured. Say so
        // rather than accepting the key and serving metrics anyway.
        if !self.metrics.enabled {
            return Err(Error::Config(
                "metrics.enabled: false is not honoured -- the metrics listener also serves \
                 /livez and /readyz, so it always starts. Remove the key, or keep the port \
                 off the network (the chart's Service and the Prometheus scrape annotations \
                 are what expose it)."
                    .into(),
            ));
        }

        // Validate buffer config
        if self.buffer.pressure_threshold < 0.0 || self.buffer.pressure_threshold > 1.0 {
            return Err(Error::Config(
                "buffer.pressure_threshold must be between 0.0 and 1.0".into(),
            ));
        }

        // Validate scheduler config
        if self.scheduler.default_interval_secs == 0 {
            return Err(Error::Config(
                "scheduler.default_interval_secs must be > 0".into(),
            ));
        }

        // Validate container extractor names are unique
        {
            let mut seen = std::collections::HashSet::new();
            for container in &self.extractors.containers {
                if !seen.insert(&container.name) {
                    return Err(Error::Config(format!(
                        "duplicate container extractor name: '{}'",
                        container.name
                    )));
                }
                if container.topic.is_empty() {
                    return Err(Error::Config(format!(
                        "container extractor '{}' has empty topic",
                        container.name
                    )));
                }
                // An env value is handed to the container verbatim, so a spec
                // the resolver cannot read reaches the tool as literal text
                // and comes back as that tool's auth error, not ours.
                for (key, value) in &container.env {
                    if let Some(issue) = spec_issue(value) {
                        return Err(Error::Config(format!(
                            "container extractor '{}' env '{key}': {issue}",
                            container.name
                        )));
                    }
                }
            }
        }

        // Validate ingest bind address
        if self.ingest.enabled
            && self
                .ingest
                .bind_address
                .parse::<std::net::SocketAddr>()
                .is_err()
        {
            return Err(Error::Config(format!(
                "invalid ingest bind address: '{}'",
                self.ingest.bind_address
            )));
        }

        // Validate source filter expressions (CEL). Driven by the same table
        // the pipeline routes on, so a filter cannot be validated here and then
        // ignored at runtime, or applied at runtime without being checked.
        for (source, filter) in self.sources.filters() {
            let Some(expr) = filter else { continue };
            let errors = scalo::expression::validate(expr);
            if !errors.is_empty() {
                return Err(Error::Config(format!(
                    "sources.{source}.filter invalid: {}",
                    errors.join(", ")
                )));
            }
        }

        // Validate multi-endpoint connection ids: each connection needs a
        // non-empty id, unique within its type. The id is the cursor key (C4)
        // and the metric/log label; a collision would cross-checkpoint two
        // accounts. Only enabled types are checked (a disabled type never
        // spawns). Empty `connections` resolves to the block's own name.
        for block in REGISTRY.iter().filter(|b| b.enabled(&self.sources)) {
            validate_connection_ids(block.name, &block.connection_ids(&self.sources))?;
        }

        // Validate Vector gRPC address
        if self.extractors.vector.enabled
            && self
                .extractors
                .vector
                .grpc_bind_address
                .parse::<std::net::SocketAddr>()
                .is_err()
        {
            return Err(Error::Config(format!(
                "invalid vector gRPC bind address: '{}'",
                self.extractors.vector.grpc_bind_address
            )));
        }

        self.accumulate.validate()?;
        self.self_regulation.hysteresis()?;
        self.validate_builtin_instances()?;
        self.validate_rest_instances()?;
        self.validate_db_instances()?;
        self.validate_file_instances()?;
        // Last: a property of the whole deployment, meaningful once the
        // sources it counts are known to be sound.
        self.validate_instance_id()?;

        Ok(())
    }

    /// Refuse a config that schedules more than one source and sets no
    /// `instance_id`.
    ///
    /// The instance id prefixes every cursor key (`{instance_id}.{connection_id}`)
    /// and labels every metric series. [`derive_instance_id`] reads only the
    /// aws, azure, m365 and gcp blocks, so a deployment built from
    /// `sources.rest`, `sources.db`, `sources.file` or any other typed block
    /// falls through to [`FALLBACK_INSTANCE_ID`] and shares it with every
    /// fetcher in the same position -- two of them then read and write each
    /// other's fetch windows. Deriving an id from the generic families instead
    /// would have to pick one source out of several, which is no more distinct
    /// than the fallback, so the config is refused and the operator names the
    /// deployment.
    ///
    /// More than one source means more than one entry in
    /// [`scheduled_connection_ids`](SourcesConfig::scheduled_connection_ids),
    /// which is exactly the set the instance id is joined with to form a cursor
    /// key. A single-source fetcher keeps loading: that is the standalone
    /// shape, and there is nothing to pick between.
    fn validate_instance_id(&self) -> Result<()> {
        if self.instance_id.is_some() || derive_instance_id(self) != FALLBACK_INSTANCE_ID {
            return Ok(());
        }
        let scheduled = self.sources.scheduled_connection_ids();
        if scheduled.len() < 2 {
            return Ok(());
        }
        Err(Error::Config(format!(
            "instance_id is unset and cannot be derived from these sources, so this fetcher \
             would run as '{FALLBACK_INSTANCE_ID}' -- the id every other fetcher without one \
             also gets. The instance id prefixes every cursor key and labels every metric \
             series, so two fetchers sharing it read and write each other's fetch windows. \
             Set instance_id to a value unique to this deployment. Sources scheduled here: {}",
            scheduled.into_iter().collect::<Vec<_>>().join(", ")
        )))
    }

    /// Map every enabled built-in block (`sources.github`, `sources.okta`)
    /// onto its shipped profile and bind each connection, so a missing
    /// credential, a scope set both ways or a service the profile lacks fails
    /// here with the block's name, not at the first tick.
    fn validate_builtin_instances(&self) -> Result<()> {
        let shipped = crate::profiles::shipped();
        for BuiltinInstance {
            connection_id,
            instance,
        } in self.sources.builtin_instances()?
        {
            let profile = dfe_fetcher_rest::profile::bound::resolve_profile(&instance, shipped)?;
            let block = match &instance.profile {
                dfe_fetcher_rest::profile::ProfileRef::Named(name) => name.clone(),
                dfe_fetcher_rest::profile::ProfileRef::Inline(_) => connection_id.clone(),
            };
            let issues = instance.validate(&profile);
            if let Some(first) = issues.first() {
                return Err(Error::Config(format!(
                    "sources.{block} ({connection_id}): {first}{}",
                    if issues.len() > 1 {
                        format!(" (and {} more)", issues.len() - 1)
                    } else {
                        String::new()
                    }
                )));
            }
        }
        Ok(())
    }

    /// Check every enabled `sources.db` instance so a missing dialect, an
    /// engine this binary was built without, or a bad store fails here with
    /// its field path, not at the first tick.
    fn validate_db_instances(&self) -> Result<()> {
        let typed: Vec<&str> = self
            .sources
            .enabled_flags()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        for (id, instance) in &self.sources.db {
            if id.trim().is_empty() {
                return Err(Error::Config(
                    "sources.db: every instance needs a non-empty id".into(),
                ));
            }
            if typed.contains(&id.as_str()) || self.sources.rest.contains_key(id) {
                return Err(Error::Config(format!(
                    "sources.db.{id}: `{id}` is already a source name; instance ids are cursor \
                     keys and metric labels and must not collide with one"
                )));
            }
            if !instance.enabled {
                continue;
            }
            if let Some(filter) = &instance.filter {
                let errors = scalo::expression::validate(filter);
                if !errors.is_empty() {
                    return Err(Error::Config(format!(
                        "sources.db.{id}.filter invalid: {}",
                        errors.join(", ")
                    )));
                }
            }
            let issues = instance.validate();
            if let Some(first) = issues.first() {
                return Err(Error::Config(format!(
                    "sources.db.{id}.{first}{}",
                    if issues.len() > 1 {
                        format!(" (and {} more)", issues.len() - 1)
                    } else {
                        String::new()
                    }
                )));
            }
        }
        Ok(())
    }

    /// Check every enabled `sources.file` instance so a glob that does not
    /// parse, a tail on a binary built without the tailer, or a unit that is
    /// neither dump nor tail fails here with its field path, not at the
    /// first tick.
    fn validate_file_instances(&self) -> Result<()> {
        let typed: Vec<&str> = self
            .sources
            .enabled_flags()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        for (id, instance) in &self.sources.file {
            if id.trim().is_empty() {
                return Err(Error::Config(
                    "sources.file: every instance needs a non-empty id".into(),
                ));
            }
            if typed.contains(&id.as_str())
                || self.sources.rest.contains_key(id)
                || self.sources.db.contains_key(id)
            {
                return Err(Error::Config(format!(
                    "sources.file.{id}: `{id}` is already a source name; instance ids are cursor \
                     keys and metric labels and must not collide with one"
                )));
            }
            if !instance.enabled {
                continue;
            }
            if let Some(filter) = &instance.filter {
                let errors = scalo::expression::validate(filter);
                if !errors.is_empty() {
                    return Err(Error::Config(format!(
                        "sources.file.{id}.filter invalid: {}",
                        errors.join(", ")
                    )));
                }
            }
            let issues = instance.validate();
            if let Some(first) = issues.first() {
                return Err(Error::Config(format!(
                    "sources.file.{id}.{first}{}",
                    if issues.len() > 1 {
                        format!(" (and {} more)", issues.len() - 1)
                    } else {
                        String::new()
                    }
                )));
            }
        }
        Ok(())
    }

    /// Bind every enabled `sources.rest` instance to its profile so a bad
    /// profile, an unknown shipped name, an unaccepted auth mode or a missing
    /// credential field fails here with its field path, not at the first tick.
    fn validate_rest_instances(&self) -> Result<()> {
        let typed: Vec<&str> = self
            .sources
            .enabled_flags()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        let shipped = crate::profiles::shipped();
        for (id, instance) in &self.sources.rest {
            if id.trim().is_empty() {
                return Err(Error::Config(
                    "sources.rest: every instance needs a non-empty id".into(),
                ));
            }
            if typed.contains(&id.as_str()) {
                return Err(Error::Config(format!(
                    "sources.rest.{id}: `{id}` is a built-in source type name; instance ids \
                     are cursor keys and metric labels and must not collide with one"
                )));
            }
            if !instance.enabled {
                continue;
            }
            let profile = dfe_fetcher_rest::profile::bound::resolve_profile(instance, shipped)
                .map_err(|e| Error::Config(format!("sources.rest.{id}.profile: {e}")))?;
            let mut issues = profile.validate();
            issues.extend(instance.validate(&profile));
            if let Some(first) = issues.first() {
                return Err(Error::Config(format!(
                    "sources.rest.{id}.{first}{}",
                    if issues.len() > 1 {
                        format!(" (and {} more)", issues.len() - 1)
                    } else {
                        String::new()
                    }
                )));
            }
            if let Some(acc) = &instance.accumulate {
                acc.validate()
                    .map_err(|e| Error::Config(format!("sources.rest.{id}.{e}")))?;
            }
        }
        Ok(())
    }
}

/// Merge `DFE_FETCHER...` env vars onto an already-loaded config, `__` nesting.
///
/// figment's `prefixed` strips exactly `DFE_FETCHER_`, so the chart and the
/// deployment contract's `DFE_FETCHER__SOURCES__AWS__ACCESS_KEY_ID` arrives as
/// the key `_sources.aws.access_key_id`, which matches no field and serde drops
/// without a word. Trimming the separator figment leaves behind is what makes
/// that form reach the same key as `DFE_FETCHER_SOURCES__AWS__ACCESS_KEY_ID`;
/// no section starts with `_`.
///
/// An empty value counts as unset, matching `scalo::config::flat_env`. The
/// chart declares every secret env var whether or not the operator supplied a
/// value, so without this an unset credential would arrive as `Some("")` and
/// sign requests with an empty key instead of failing on the missing one.
///
/// `config_path` is `#[serde(skip)]`, so the round trip through serde loses it --
/// it is carried across by hand.
fn apply_nested_env(config: &mut Config) -> Result<()> {
    use figment::Figment;
    use figment::providers::{Env, Serialized};
    use scalo::config::sensitive::expose_during;

    let config_path = config.config_path.clone();
    let prefix = format!("{ENV_PREFIX}_");

    // Secrets already in the config serialise as `***REDACTED***` outside an
    // expose window, which would overwrite them with the mask.
    let merged = expose_during(|| {
        // `Env::raw` hands over the untouched variable name, which is what
        // makes the value reachable for the empty check.
        let env = Env::raw()
            .filter_map(move |key| {
                if !key.starts_with(&prefix)
                    || std::env::var(key.as_str()).is_ok_and(|v| v.is_empty())
                {
                    return None;
                }
                Some(key.as_str()[prefix.len()..].trim_start_matches('_').into())
            })
            .split("__");
        // `figment::Error` is 200+ bytes; carry the message, not the value.
        Figment::from(Serialized::defaults(&*config))
            .merge(env)
            .extract::<Config>()
            .map_err(|e| e.to_string())
    })
    .map_err(|e| {
        Error::Config(format!(
            "failed to apply {ENV_PREFIX} environment overrides: {e}"
        ))
    })?;

    *config = merged;
    config.config_path = config_path;
    Ok(())
}

/// Overlay a per-connection optional field onto the resolved config: when the
/// connection sets it, its value wins; when unset, the shared type-level value
/// stays. Used by every block's [`ConnectionOverlay::overlay`].
///
/// Takes `&Option<T>` (not `Option<&T>`) so call sites can pass `&connection.field`
/// directly; `clone_from` reuses the destination allocation.
#[allow(clippy::ref_option)]
fn overlay_opt<T: Clone>(dst: &mut Option<T>, src: &Option<T>) {
    if src.is_some() {
        dst.clone_from(src);
    }
}

/// Validate the connection ids of one source type: each must be non-empty and
/// unique within the type (they become cursor keys and metric labels).
fn validate_connection_ids(type_name: &str, ids: &[&str]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for id in ids {
        if id.trim().is_empty() {
            return Err(Error::Config(format!(
                "sources.{type_name}: every connection needs a non-empty 'id'"
            )));
        }
        if !seen.insert(*id) {
            return Err(Error::Config(format!(
                "sources.{type_name}: duplicate connection id '{id}' (ids must be unique)"
            )));
        }
    }
    Ok(())
}

/// The instance id [`derive_instance_id`] falls back to when nothing in the
/// config names or distinguishes the deployment. Every fetcher that reaches it
/// gets the same one, which is why [`Config::validate`] refuses it for a config
/// carrying more than one source.
const FALLBACK_INSTANCE_ID: &str = "dfe-fetcher";

/// Derive instance ID from config. Uses explicit value if set,
/// otherwise auto-derives from first enabled source's distinguishing config.
#[must_use]
pub fn derive_instance_id(config: &Config) -> String {
    use sha2::{Digest, Sha256};

    if let Some(ref id) = config.instance_id {
        return id.to_lowercase();
    }

    if config.sources.aws.enabled {
        let input = format!(
            "{}{}",
            config.sources.aws.region,
            config.sources.aws.access_key_id.as_deref().unwrap_or("")
        );
        let hash = hex::encode(&Sha256::digest(input.as_bytes())[..4]);
        return format!("aws-{hash}");
    }
    if config.sources.azure.enabled {
        let input = format!(
            "{}{}",
            config.sources.azure.tenant_id.as_deref().unwrap_or(""),
            config
                .sources
                .azure
                .subscription_id
                .as_deref()
                .unwrap_or("")
        );
        let hash = hex::encode(&Sha256::digest(input.as_bytes())[..4]);
        return format!("azure-{hash}");
    }
    if config.sources.m365.enabled {
        let input = config.sources.m365.tenant_id.as_deref().unwrap_or("");
        let hash = hex::encode(&Sha256::digest(input.as_bytes())[..4]);
        return format!("m365-{hash}");
    }
    if config.sources.gcp.enabled {
        let input = config.sources.gcp.project_id.as_deref().unwrap_or("");
        let hash = hex::encode(&Sha256::digest(input.as_bytes())[..4]);
        return format!("gcp-{hash}");
    }

    FALLBACK_INSTANCE_ID.to_string()
}

/// Reload configuration from the same source.
pub fn reload_config(current: &Config) -> Result<Config> {
    Config::load(current.config_path.as_deref())
}

impl ApplyFlatEnv for Config {
    fn apply_flat_env(&mut self, prefix: &str) {
        // Kafka
        if let Some(v) = flat_env::flat_env_list(prefix, "KAFKA_BROKERS") {
            self.kafka.brokers = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_CLIENT_ID") {
            self.kafka.client_id = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_SASL_MECHANISM") {
            let sasl = self.kafka.sasl.get_or_insert_with(SaslConfig::default);
            // No `enabled = true` here: an existing block keeps whatever it
            // set, and a block created by this line already defaults to on.
            sasl.mechanism = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_SECURITY_PROTOCOL") {
            self.kafka.tls.enabled = v.to_uppercase().contains("SSL");
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_SASL_USER") {
            let sasl = self.kafka.sasl.get_or_insert_with(SaslConfig::default);
            sasl.username = v;
        }
        if let Some(v) = flat_env::flat_env_string_sensitive(prefix, "KAFKA_SASL_PASSWORD") {
            let sasl = self.kafka.sasl.get_or_insert_with(SaslConfig::default);
            sasl.password = SensitiveString::from(v);
        }

        // Scheduler
        if let Some(v) = flat_env::flat_env_parsed::<u64>(prefix, "DEFAULT_INTERVAL_SECS") {
            self.scheduler.default_interval_secs = v;
        }

        // Topic suffix
        if let Some(v) = flat_env::flat_env_string(prefix, "TOPIC_SUFFIX") {
            self.kafka.topic_suffix = v;
        }

        // Buffer / memory
        if let Some(v) = flat_env::flat_env_parsed::<usize>(prefix, "MEMORY_LIMIT") {
            self.buffer.memory_limit = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<f64>(prefix, "PRESSURE_THRESHOLD") {
            self.buffer.pressure_threshold = v;
        }

        // Metrics
        if let Some(v) = flat_env::flat_env_string(prefix, "METRICS_ADDRESS") {
            self.metrics.address = v;
        }

        // Config reload
        if let Some(v) = flat_env::flat_env_parsed::<u64>(prefix, "CONFIG_RELOAD_SECS") {
            self.config_reload_secs = v;
        }

        // DLQ (fleet-uniform names: DLQ_ENABLED / DLQ_TOPIC / DLQ_MODE).
        // TOPIC routes every entry to one fixed topic (the per-app standard,
        // e.g. dfe_fetcher_dlq) rather than per-destination suffix topics.
        if let Some(v) = flat_env::flat_env_bool(prefix, "DLQ_ENABLED") {
            self.dlq.enabled = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "DLQ_PATH") {
            self.dlq.file.path = v.into();
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "DLQ_TOPIC") {
            self.dlq.kafka.routing = scalo::dlq::DlqRouting::Common;
            self.dlq.kafka.common_topic = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "DLQ_MODE") {
            use scalo::dlq::DlqMode;
            self.dlq.mode = match v.as_str() {
                "fan_out" => DlqMode::FanOut,
                "file_only" => DlqMode::FileOnly,
                "kafka_only" => DlqMode::KafkaOnly,
                "cascade" => DlqMode::Cascade,
                other => {
                    // A typo'd mode must not silently pick a backend -- cascade
                    // includes the file backend, an EROFS no-op deployed.
                    tracing::warn!(mode = %other, "unknown DLQ_MODE, using cascade");
                    DlqMode::Cascade
                }
            };
        }
    }
}

// =============================================================================
// Scheduler configuration
// =============================================================================

/// Scheduler configuration for fetch timing.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SchedulerConfig {
    /// Default fetch interval in seconds (used when source doesn't specify its own).
    pub default_interval_secs: u64,

    /// Maximum concurrent fetch tasks across all sources.
    pub max_concurrent_fetches: usize,

    /// Jitter percentage (0-100) added to intervals to avoid thundering herd.
    pub jitter_percent: u8,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            default_interval_secs: 300, // 5 minutes
            max_concurrent_fetches: 10,
            jitter_percent: 10,
        }
    }
}

// =============================================================================
// Sources configuration
// =============================================================================

/// Top-level sources configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SourcesConfig {
    /// AWS source configuration.
    pub aws: AwsSourceConfig,

    /// Azure source configuration.
    pub azure: AzureSourceConfig,

    /// Microsoft 365 source configuration.
    pub m365: M365SourceConfig,

    /// Google Cloud Platform source configuration.
    pub gcp: GcpSourceConfig,

    /// GitHub audit-log source configuration.
    pub github: GithubSourceConfig,

    /// Okta System Log source configuration.
    pub okta: OktaSourceConfig,

    /// Cloudflare audit-log source configuration.
    pub cloudflare: CloudflareSourceConfig,

    /// 1Password Events Reporting source configuration.
    pub onepassword: OnePasswordSourceConfig,

    /// CrowdStrike Falcon source configuration.
    pub crowdstrike: CrowdstrikeSourceConfig,

    /// Slack audit-log source configuration.
    pub slack: SlackSourceConfig,

    /// Bitwarden Events API source configuration.
    pub bitwarden: BitwardenSourceConfig,

    /// Duo Admin API source configuration.
    pub duo: DuoSourceConfig,

    /// PyPI supply-chain audit source configuration.
    pub pypi: PypiSourceConfig,

    /// crates.io supply-chain audit source configuration.
    pub crates_io: CratesIoSourceConfig,

    /// Go module-proxy supply-chain audit source configuration.
    pub go_modules: GoModulesSourceConfig,

    /// Google Workspace Reports API source configuration.
    ///
    /// **Alpha** - additionally pending hyperi-infra#5 (domain-wide-delegation
    /// service account + manual Admin-console scope grants) before live use.
    pub google_workspace: GoogleWorkspaceSourceConfig,

    /// GCP Pub/Sub pull source configuration.
    ///
    /// **Alpha** - additionally pending a hyperi-infra issue (TBD) for
    /// tenant-side Log Sink + Pub/Sub topic + subscription provisioning.
    pub gcp_pubsub: GcpPubsubSourceConfig,

    /// Object-store source family configuration (S3 / GCS / Azure Blob).
    ///
    /// The S3 backend is live; GCS and Azure Blob are stubs that log-and-skip,
    /// so naming them here does not fail the tick.
    pub object_store: ObjectStoreSourceConfig,

    /// Salesforce audit source configuration (SetupAuditTrail,
    /// LoginHistory, EventLogFile).
    ///
    /// **Alpha** - additionally pending a Salesforce connected app for live
    /// testing.
    pub salesforce: SalesforceSourceConfig,

    /// Declarative REST sources on the source framework, keyed by instance id.
    ///
    /// Each entry binds a profile (a shipped one by name, or written inline)
    /// to one deployment's identity: base URL, credential kind and refs,
    /// topic, interval, per-unit narrowing. The id is the connection id: the
    /// cursor key, the metric and log label, and the `_source_fetcher` prefix.
    pub rest: BTreeMap<String, RestInstance>,

    /// Database sources on the source framework, keyed by instance id.
    ///
    /// Each entry names an engine (`odbc` with a dialect, or `clickhouse`),
    /// its connection string as a credential spec, the block-cursor bounds,
    /// and the stores: each a query dumped whole into the snapshot envelope
    /// or tailed by keyset. The id is the connection id, as for `rest`. An
    /// engine this binary was built without is refused at load.
    pub db: BTreeMap<String, DbInstance>,

    /// File sources on the source framework, keyed by instance id.
    ///
    /// Each entry names units: a `dump` (globs of NDJSON, JSON-array or CSV
    /// files, gzip by magic, each read once into the snapshot envelope) or
    /// a `tail` (files followed through rotation, lines committed after
    /// acknowledgement). The id is the connection id, as for `rest`. A tail
    /// on a binary built without the tailer is refused at load.
    pub file: BTreeMap<String, FileInstance>,
}

impl Default for SourcesConfig {
    fn default() -> Self {
        Self {
            rest: BTreeMap::new(),
            db: BTreeMap::new(),
            file: BTreeMap::new(),
            aws: AwsSourceConfig::default(),
            azure: AzureSourceConfig::default(),
            m365: M365SourceConfig::default(),
            gcp: GcpSourceConfig::default(),
            github: GithubSourceConfig::default(),
            okta: OktaSourceConfig::default(),
            cloudflare: CloudflareSourceConfig::default(),
            onepassword: OnePasswordSourceConfig::default(),
            crowdstrike: CrowdstrikeSourceConfig::default(),
            slack: SlackSourceConfig::default(),
            bitwarden: BitwardenSourceConfig::default(),
            duo: DuoSourceConfig::default(),
            pypi: PypiSourceConfig::default(),
            crates_io: CratesIoSourceConfig::default(),
            go_modules: GoModulesSourceConfig::default(),
            google_workspace: GoogleWorkspaceSourceConfig::default(),
            gcp_pubsub: GcpPubsubSourceConfig::default(),
            object_store: ObjectStoreSourceConfig::default(),
            salesforce: SalesforceSourceConfig::default(),
        }
    }
}

impl SourcesConfig {
    /// Every typed block's CEL `filter`, keyed by its `sources.` name, read
    /// off the [registry](builtin::REGISTRY).
    ///
    /// One table drives both `Config::validate` and the per-row rules a
    /// driver compiles, so a filter cannot be accepted at startup and then
    /// never applied. `test_filter_table_covers_every_source` fails if a
    /// block with a `filter` field is missing from the registry.
    #[must_use]
    pub fn filters(&self) -> Vec<(&'static str, Option<&str>)> {
        builtin::REGISTRY
            .iter()
            .map(|block| (block.name, block.filter(self)))
            .collect()
    }

    /// Every typed block's `enabled` flag, keyed by its `sources.` name,
    /// read off the [registry](builtin::REGISTRY).
    #[must_use]
    pub fn enabled_flags(&self) -> Vec<(&'static str, bool)> {
        builtin::REGISTRY
            .iter()
            .map(|block| (block.name, block.enabled(self)))
            .collect()
    }

    /// Whether any source is enabled: a typed source, a REST instance, a
    /// database instance or a file instance.
    #[must_use]
    pub fn any_enabled(&self) -> bool {
        self.enabled_flags().iter().any(|(_, enabled)| *enabled)
            || self.rest.values().any(|r| r.enabled)
            || self.db.values().any(|d| d.enabled)
            || self.file.values().any(|f| f.enabled)
    }

    /// Every connection id the config schedules a fetch task for: each
    /// enabled built-in block's resolved connections, plus each enabled
    /// `sources.rest`, `sources.db` and `sources.file` entry. Exactly the set
    /// `main` expands at start, so the set of running tasks can be diffed
    /// against the live config.
    ///
    /// Infallible, so a reload can never skip the diff for a config error.
    #[must_use]
    pub fn scheduled_connection_ids(&self) -> BTreeSet<&str> {
        builtin::REGISTRY
            .iter()
            .filter(|block| block.enabled(self))
            .flat_map(|block| block.connection_ids(self))
            .chain(
                self.rest
                    .iter()
                    .filter(|(_, instance)| instance.enabled)
                    .map(|(id, _)| id.as_str()),
            )
            .chain(
                self.db
                    .iter()
                    .filter(|(_, instance)| instance.enabled)
                    .map(|(id, _)| id.as_str()),
            )
            .chain(
                self.file
                    .iter()
                    .filter(|(_, instance)| instance.enabled)
                    .map(|(id, _)| id.as_str()),
            )
            .collect()
    }

    /// The CEL keep-filter of the source a driver runs, by its connection
    /// id: a `sources.rest`, `sources.db` or `sources.file` entry's own
    /// filter, or the type-wide filter of the typed block whose resolved
    /// connection ids include it; `None` when nothing owns the id or it has
    /// no filter.
    ///
    /// The id is resolved through the registry, never by splitting the
    /// `_source_fetcher` tag on `.`, so a connection under `connections[]`
    /// (`gh-acme`) finds its block's filter and `gcp_pubsub` cannot match a
    /// `gcp` prefix.
    #[must_use]
    pub fn filter_for_source(&self, connection_id: &str) -> Option<&str> {
        if let Some(instance) = self.rest.get(connection_id) {
            return instance.filter.as_deref();
        }
        if let Some(instance) = self.db.get(connection_id) {
            return instance.filter.as_deref();
        }
        if let Some(instance) = self.file.get(connection_id) {
            return instance.filter.as_deref();
        }
        builtin::REGISTRY
            .iter()
            .find(|block| block.connection_ids(self).contains(&connection_id))
            .and_then(|block| block.filter(self))
    }
}

// =============================================================================
// Multi-endpoint (GA 2.2) connection model
// =============================================================================
//
// A source TYPE (e.g. AWS) maps 1:1 to a fetcher pod group and carries a
// `connections` list -- many accounts/tenants of that ONE type, each polled by
// its own scheduler task. Type-wide fields (`services`, `topic`, `filter`, the
// default `interval_secs`) live at the top of the type config and are SHARED by
// every connection. Connection-specific fields (credentials, region, endpoints)
// live per entry in `connections`. When `connections` is empty the top-level
// fields define a single implicit connection (id = the type name), preserving
// single-account configs and their cursor keys.

/// One resolved connection, ready to instantiate a source.
///
/// Produced by each type config's `resolved()`: the shared type-level fields
/// merged with one connection's identity/credentials, tagged with a stable
/// `id`. The `id` is the connection's cursor key (C4) and its metric/log/DLQ
/// label -- keep it identical to the engine's source-def connection `id`.
#[derive(Debug, Clone)]
pub struct Resolved<C> {
    /// Stable connection id: cursor key + metric/log/DLQ label.
    pub id: String,

    /// Per-connection source config (shared type fields + this connection).
    pub config: C,

    /// Effective fetch-interval override for this connection, in seconds.
    /// Per-connection override falls back to the type-level `interval_secs`.
    pub interval_secs: Option<u64>,
}

impl<C> Resolved<C> {
    /// Wrap a single config as a one-element connection list (the implicit
    /// single-connection form: id defaults to the type name).
    pub fn single(id: impl Into<String>, config: C, interval_secs: Option<u64>) -> Vec<Self> {
        vec![Self {
            id: id.into(),
            config,
            interval_secs,
        }]
    }
}

/// A typed source block that carries a `connections` list.
///
/// The type-wide fields are shared by every connection and each entry
/// overlays its own identity on a copy of them; a block supplies the overlay
/// and inherits the one `resolved` expansion.
pub trait ConnectionOverlay: Clone {
    /// One entry of the block's `connections`.
    type Connection;

    /// The block's `connections`.
    fn connections(&self) -> &[Self::Connection];

    /// The block's `connections`, cleared on the per-connection copy because
    /// the runtime config never re-reads it.
    fn connections_mut(&mut self) -> &mut Vec<Self::Connection>;

    /// The type-level interval a connection without its own inherits.
    fn interval_secs(&self) -> Option<u64>;

    /// A connection's stable id.
    fn connection_id(connection: &Self::Connection) -> &str;

    /// A connection's own interval override.
    fn connection_interval_secs(connection: &Self::Connection) -> Option<u64>;

    /// Write the connection's set fields over the type-wide ones.
    fn overlay(&mut self, connection: &Self::Connection);

    /// The ids the block resolves to: each connection's own, or `default_id`
    /// alone for the implicit single connection.
    fn connection_ids<'a>(&'a self, default_id: &'a str) -> Vec<&'a str> {
        if self.connections().is_empty() {
            return vec![default_id];
        }
        self.connections().iter().map(Self::connection_id).collect()
    }

    /// Expand into one resolved config per connection (C1/C4). With no
    /// `connections`, returns the single implicit connection keyed on
    /// `default_id`.
    #[must_use]
    fn resolved(&self, default_id: &str) -> Vec<Resolved<Self>> {
        let without_connections = || {
            let mut copy = self.clone();
            copy.connections_mut().clear();
            copy
        };
        if self.connections().is_empty() {
            return Resolved::single(default_id, without_connections(), self.interval_secs());
        }
        self.connections()
            .iter()
            .map(|connection| {
                let mut config = without_connections();
                config.overlay(connection);
                Resolved {
                    id: Self::connection_id(connection).to_owned(),
                    config,
                    interval_secs: Self::connection_interval_secs(connection)
                        .or(self.interval_secs()),
                }
            })
            .collect()
    }
}

/// One AWS connection: an account/region + its credentials. Type-wide fields
/// (`services`, `topic`, `filter`) are shared and stay on [`AwsSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AwsConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// AWS region. Inherits the type-level `region` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    /// Access key ID (prefer `credential_secret` in production).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,

    /// Secret access key (always redacted in serialisation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<SensitiveString>,

    /// Assume role ARN for cross-account access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assume_role_arn: Option<String>,

    /// Secret source for credentials ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Endpoint URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

/// AWS source configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct AwsSourceConfig {
    /// Enable AWS source.
    pub enabled: bool,

    /// AWS region.
    pub region: String,

    /// Access key ID (prefer secrets manager in production).
    pub access_key_id: Option<String>,

    /// Secret access key (prefer secrets manager in production; always redacted in serialisation).
    pub secret_access_key: Option<SensitiveString>,

    /// Assume role ARN for cross-account access.
    pub assume_role_arn: Option<String>,

    /// Secret source for credentials.
    /// Format: `vault:<mount>/data/<path>:<key>` (e.g. `vault:kv/data/aws:credentials`);
    /// the literal `data` segment names the KV v2 mount.
    pub credential_secret: Option<String>,

    /// Fetch interval override in seconds.
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<AwsService>,

    /// Output Kafka topic.
    pub topic: String,

    /// Endpoint URL override for testing (e.g., wiremock server URI).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_override: Option<String>,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple accounts/regions of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<AwsConnection>,
}

impl Default for AwsSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            region: "us-east-1".to_string(),
            access_key_id: None,
            secret_access_key: None,
            assume_role_arn: None,
            credential_secret: None,
            interval_secs: None,
            services: vec![],
            topic: "aws".to_string(),
            endpoint_override: None,
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for AwsSourceConfig {
    type Connection = AwsConnection;

    fn connections(&self) -> &[AwsConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<AwsConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &AwsConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &AwsConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &AwsConnection) {
        if let Some(region) = &connection.region {
            self.region.clone_from(region);
        }
        overlay_opt(&mut self.access_key_id, &connection.access_key_id);
        overlay_opt(&mut self.secret_access_key, &connection.secret_access_key);
        overlay_opt(&mut self.assume_role_arn, &connection.assume_role_arn);
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(&mut self.endpoint_override, &connection.endpoint_override);
    }
}

/// AWS service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AwsService {
    /// Service name (e.g., "cloudtrail", "guardduty", "securityhub", "config").
    pub name: String,

    /// Service-specific configuration.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// One Azure connection: a tenant/subscription + its service-principal
/// credentials. Type-wide fields (`services`, `topic`, `filter`) are shared and
/// stay on [`AzureSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AzureConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// Azure tenant ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,

    /// Client (application) ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// Client secret (always redacted in serialisation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SensitiveString>,

    /// Secret source for credentials ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Subscription ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<String>,

    /// Management API base URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub management_url_override: Option<String>,

    /// Graph API base URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_url_override: Option<String>,

    /// Token endpoint URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

/// Azure source configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct AzureSourceConfig {
    /// Enable Azure source.
    pub enabled: bool,

    /// Azure tenant ID.
    pub tenant_id: Option<String>,

    /// Client (application) ID.
    pub client_id: Option<String>,

    /// Client secret (prefer secrets manager in production; always redacted in serialisation).
    pub client_secret: Option<SensitiveString>,

    /// Secret source for credentials.
    pub credential_secret: Option<String>,

    /// Subscription ID.
    pub subscription_id: Option<String>,

    /// Fetch interval override in seconds.
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<AzureService>,

    /// Output Kafka topic.
    pub topic: String,

    /// Management API base URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub management_url_override: Option<String>,

    /// Graph API base URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_url_override: Option<String>,

    /// Token endpoint URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url_override: Option<String>,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple tenants/subscriptions of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<AzureConnection>,
}

impl Default for AzureSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tenant_id: None,
            client_id: None,
            client_secret: None,
            credential_secret: None,
            subscription_id: None,
            interval_secs: None,
            services: vec![],
            topic: "azure".to_string(),
            management_url_override: None,
            graph_url_override: None,
            token_url_override: None,
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for AzureSourceConfig {
    type Connection = AzureConnection;

    fn connections(&self) -> &[AzureConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<AzureConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &AzureConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &AzureConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &AzureConnection) {
        overlay_opt(&mut self.tenant_id, &connection.tenant_id);
        overlay_opt(&mut self.client_id, &connection.client_id);
        overlay_opt(&mut self.client_secret, &connection.client_secret);
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(&mut self.subscription_id, &connection.subscription_id);
        overlay_opt(
            &mut self.management_url_override,
            &connection.management_url_override,
        );
        overlay_opt(&mut self.graph_url_override, &connection.graph_url_override);
        overlay_opt(&mut self.token_url_override, &connection.token_url_override);
    }
}

/// Azure service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AzureService {
    /// Service name (e.g., "activity_log", "defender", "sentinel",
    /// "entra_signins", "entra_directory_audits", "entra_provisioning",
    /// "log_analytics").
    pub name: String,

    /// Service-specific configuration.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// One M365 connection: a tenant + its application credentials. Type-wide
/// fields (`services`, `topic`, `filter`) are shared and stay on
/// [`M365SourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct M365Connection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// Azure AD tenant ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,

    /// Client (application) ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// Client secret (always redacted in serialisation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SensitiveString>,

    /// Secret source for credentials ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Management API (manage.office.com) base URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub management_url_override: Option<String>,

    /// Graph API base URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_url_override: Option<String>,

    /// Token endpoint URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

/// Microsoft 365 source configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct M365SourceConfig {
    /// Enable M365 source.
    pub enabled: bool,

    /// Azure AD tenant ID.
    pub tenant_id: Option<String>,

    /// Client (application) ID.
    pub client_id: Option<String>,

    /// Client secret (prefer secrets manager in production; always redacted in serialisation).
    pub client_secret: Option<SensitiveString>,

    /// Secret source for credentials.
    pub credential_secret: Option<String>,

    /// Fetch interval override in seconds.
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<M365Service>,

    /// Output Kafka topic.
    pub topic: String,

    /// Management API (manage.office.com) base URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub management_url_override: Option<String>,

    /// Graph API base URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_url_override: Option<String>,

    /// Token endpoint URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url_override: Option<String>,

    /// The `PublisherIdentifier` every Management Activity call carries,
    /// keying this deployment's OMAP rate budget; the shared default unless
    /// set, and an operator who set `M365_PUBLISHER_IDENTIFIER` in the
    /// environment must move it here, as the environment is no longer read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publisher_identifier: Option<String>,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple tenants of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<M365Connection>,
}

impl Default for M365SourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tenant_id: None,
            client_id: None,
            client_secret: None,
            credential_secret: None,
            interval_secs: None,
            services: vec![],
            topic: "m365".to_string(),
            management_url_override: None,
            graph_url_override: None,
            token_url_override: None,
            publisher_identifier: None,
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for M365SourceConfig {
    type Connection = M365Connection;

    fn connections(&self) -> &[M365Connection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<M365Connection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &M365Connection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &M365Connection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &M365Connection) {
        overlay_opt(&mut self.tenant_id, &connection.tenant_id);
        overlay_opt(&mut self.client_id, &connection.client_id);
        overlay_opt(&mut self.client_secret, &connection.client_secret);
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(
            &mut self.management_url_override,
            &connection.management_url_override,
        );
        overlay_opt(&mut self.graph_url_override, &connection.graph_url_override);
        overlay_opt(&mut self.token_url_override, &connection.token_url_override);
    }
}

/// M365 service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct M365Service {
    /// Service name (e.g., "audit_log", "exchange_audit", "dlp", "alerts").
    pub name: String,

    /// Service-specific configuration.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// One GCP connection: a project + its service-account credentials. Type-wide
/// fields (`services`, `topic`, `filter`) are shared and stay on
/// [`GcpSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GcpConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// GCP project ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,

    /// Path to service account key file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_key: Option<String>,

    /// Secret source for credentials ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Token endpoint URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

/// Google Cloud Platform source configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct GcpSourceConfig {
    /// Enable GCP source.
    pub enabled: bool,

    /// GCP project ID.
    pub project_id: Option<String>,

    /// Path to service account key file.
    pub service_account_key: Option<String>,

    /// Secret source for credentials.
    pub credential_secret: Option<String>,

    /// Fetch interval override in seconds.
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<GcpService>,

    /// Output Kafka topic.
    pub topic: String,

    /// API base URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Token endpoint URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url_override: Option<String>,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple projects of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<GcpConnection>,
}

impl Default for GcpSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            project_id: None,
            service_account_key: None,
            credential_secret: None,
            interval_secs: None,
            services: vec![],
            topic: "gcp".to_string(),
            api_url_override: None,
            token_url_override: None,
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for GcpSourceConfig {
    type Connection = GcpConnection;

    fn connections(&self) -> &[GcpConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<GcpConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &GcpConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &GcpConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &GcpConnection) {
        overlay_opt(&mut self.project_id, &connection.project_id);
        overlay_opt(
            &mut self.service_account_key,
            &connection.service_account_key,
        );
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(&mut self.api_url_override, &connection.api_url_override);
        overlay_opt(&mut self.token_url_override, &connection.token_url_override);
    }
}

/// GCP service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GcpService {
    /// Service name (e.g., "admin_activity", "data_access", "system_event",
    /// "policy_denied", "vpc_flow_logs", "dns_queries", "storage_access",
    /// "scc", "cloud_logging").
    pub name: String,

    /// Service-specific configuration.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// GitHub source configuration.
///
/// Pulls audit-log events from either a GitHub organisation
/// (`/orgs/{org}/audit-log`) or a GitHub Enterprise Cloud account
/// (`/enterprises/{enterprise}/audit-log`). Set exactly one of `org` or
/// `enterprise` per fetcher instance - scale to multiple by deploying multiple
/// fetcher instances with different configs (per the no-horizontal-scaling rule
/// in CLAUDE.md).
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct GithubSourceConfig {
    /// Enable GitHub source.
    pub enabled: bool,

    /// Organisation slug for `/orgs/{org}/audit-log`. Exactly one of `org` /
    /// `enterprise` must be set when enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,

    /// Enterprise slug for `/enterprises/{enterprise}/audit-log`. Requires a
    /// GitHub Enterprise Cloud plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enterprise: Option<String>,

    /// Personal Access Token, fine-grained PAT, or GitHub App installation
    /// token. Required scope: `read:audit_log` (org) or `read:enterprise`
    /// (enterprise). Always redacted on serialisation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SensitiveString>,

    /// Secret source spec for the token (e.g. `vault:kv/data/github:token`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base URL override for testing (e.g. wiremock).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<GithubService>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple orgs/enterprises of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<GithubConnection>,
}

/// One GitHub connection: an org or enterprise + its token. Type-wide fields
/// (`services`, `topic`, `filter`) are shared and stay on
/// [`GithubSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GithubConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// Organisation slug for `/orgs/{org}/audit-log`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,

    /// Enterprise slug for `/enterprises/{enterprise}/audit-log`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enterprise: Option<String>,

    /// Audit-log token (always redacted on serialisation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SensitiveString>,

    /// Secret source spec for the token ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base URL override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

impl Default for GithubSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            org: None,
            enterprise: None,
            token: None,
            credential_secret: None,
            api_url_override: None,
            interval_secs: None,
            services: vec![],
            topic: "github".to_string(),
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for GithubSourceConfig {
    type Connection = GithubConnection;

    fn connections(&self) -> &[GithubConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<GithubConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &GithubConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &GithubConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &GithubConnection) {
        overlay_opt(&mut self.org, &connection.org);
        overlay_opt(&mut self.enterprise, &connection.enterprise);
        overlay_opt(&mut self.token, &connection.token);
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(&mut self.api_url_override, &connection.api_url_override);
    }
}

/// GitHub service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GithubService {
    /// Service name (currently only "audit_log").
    pub name: String,

    /// Service-specific configuration. Recognised keys for `audit_log`:
    /// - `include`: one of `"all"` (default), `"web"`, or `"git"`.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Okta source configuration.
///
/// Pulls events from the Okta System Log API
/// (`{tenant_url}/api/v1/logs`). One Okta tenant per fetcher instance.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct OktaSourceConfig {
    /// Enable Okta source.
    pub enabled: bool,

    /// Tenant URL, e.g. `https://hyperi.okta.com` (no trailing slash).
    /// The OAuth-style preview API uses `oktapreview.com`; either is accepted
    /// here verbatim. Override per environment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_url: Option<String>,

    /// SSWS API token (legacy auth) or bearer token from OAuth.
    /// Always redacted on serialisation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SensitiveString>,

    /// If `true`, send the token as `Authorization: SSWS <token>` (Okta's
    /// legacy API-token header). If `false`, send as `Authorization: Bearer
    /// <token>` (OAuth access token). Default: `true`.
    #[serde(default = "default_okta_use_ssws")]
    pub use_ssws_header: bool,

    /// Secret source spec for the token (e.g. `vault:kv/data/okta:token`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<OktaService>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple tenants of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<OktaConnection>,
}

/// One Okta connection: a tenant URL + its token. Type-wide fields (`services`,
/// `topic`, `filter`) are shared and stay on [`OktaSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct OktaConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// Tenant URL, e.g. `https://hyperi.okta.com` (no trailing slash).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_url: Option<String>,

    /// SSWS API token or OAuth bearer token (always redacted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SensitiveString>,

    /// Override the SSWS vs Bearer header choice for this connection.
    /// Inherits the type-level `use_ssws_header` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_ssws_header: Option<bool>,

    /// Secret source spec for the token ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

fn default_okta_use_ssws() -> bool {
    true
}

impl Default for OktaSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tenant_url: None,
            token: None,
            use_ssws_header: true,
            credential_secret: None,
            api_url_override: None,
            interval_secs: None,
            services: vec![],
            topic: "okta".to_string(),
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for OktaSourceConfig {
    type Connection = OktaConnection;

    fn connections(&self) -> &[OktaConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<OktaConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &OktaConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &OktaConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &OktaConnection) {
        overlay_opt(&mut self.tenant_url, &connection.tenant_url);
        overlay_opt(&mut self.token, &connection.token);
        if let Some(v) = connection.use_ssws_header {
            self.use_ssws_header = v;
        }
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(&mut self.api_url_override, &connection.api_url_override);
    }
}

/// Okta service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct OktaService {
    /// Service name (currently only "system_log").
    pub name: String,

    /// Service-specific configuration. Recognised keys for `system_log`:
    /// - `filter`: OData-style filter applied server-side
    ///   (e.g. `eventType eq "user.session.start"`).
    /// - `limit`: per-page size (Okta caps at 1000).
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Cloudflare source configuration.
///
/// Pulls events from Cloudflare's REST API. One account per fetcher instance.
/// Auth: scoped API token (read-only). Account ID required for audit logs.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct CloudflareSourceConfig {
    /// Enable Cloudflare source.
    pub enabled: bool,

    /// Account ID (32-char hex) for account-level audit logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,

    /// API token. Always redacted on serialisation.
    /// Required token permission for the v1 account audit-logs endpoint this
    /// source calls (`/accounts/{id}/audit_logs`): **Account Settings: Read**.
    /// (The separate "Account Audit Logs Read" permission applies to the
    /// newer v2 `/accounts/{id}/logs/audit` API, which this source does not
    /// use yet.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SensitiveString>,

    /// Secret source spec for the token (e.g. `vault:kv/data/cloudflare:token`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<CloudflareService>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple accounts of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<CloudflareConnection>,
}

/// One Cloudflare connection: an account + its token. Type-wide fields
/// (`services`, `topic`, `filter`) are shared and stay on
/// [`CloudflareSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CloudflareConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// Account ID (32-char hex) for account-level audit logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,

    /// API token (always redacted on serialisation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SensitiveString>,

    /// Secret source spec for the token ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

impl Default for CloudflareSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            account_id: None,
            token: None,
            credential_secret: None,
            api_url_override: None,
            interval_secs: None,
            services: vec![],
            topic: "cloudflare".to_string(),
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for CloudflareSourceConfig {
    type Connection = CloudflareConnection;

    fn connections(&self) -> &[CloudflareConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<CloudflareConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &CloudflareConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &CloudflareConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &CloudflareConnection) {
        overlay_opt(&mut self.account_id, &connection.account_id);
        overlay_opt(&mut self.token, &connection.token);
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(&mut self.api_url_override, &connection.api_url_override);
    }
}

/// Cloudflare service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CloudflareService {
    /// Service name (currently only "audit_logs").
    pub name: String,

    /// Service-specific configuration. Recognised keys for `audit_logs`:
    /// - `actor_email`: filter by acting user (optional).
    /// - `action_type`: filter by action category (optional).
    /// - `per_page`: page size (default 100, max 1000).
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// 1Password Events Reporting source configuration.
///
/// Pulls events from the 1Password Events Reporting API
/// (`events.1password.com/api/v2/*`). Requires a 1Password Business or
/// Enterprise account with Events Reporting enabled.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct OnePasswordSourceConfig {
    /// Enable 1Password source.
    pub enabled: bool,

    /// Events Reporting API token (Bearer token, generated from the
    /// 1Password Business dashboard). Always redacted on serialisation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SensitiveString>,

    /// Secret source spec for the token
    /// (e.g. `vault:kv/data/onepassword:events_token`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override for testing.
    /// Production default: `https://events.1password.com`.
    /// EU-region tenants: `https://events.ent.1password.eu`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<OnePasswordService>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple 1Password accounts of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<OnePasswordConnection>,
}

/// One 1Password connection: an account + its Events Reporting token. Type-wide
/// fields (`services`, `topic`, `filter`) are shared and stay on
/// [`OnePasswordSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct OnePasswordConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// Events Reporting API token (always redacted on serialisation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SensitiveString>,

    /// Secret source spec for the token ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

impl Default for OnePasswordSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            token: None,
            credential_secret: None,
            api_url_override: None,
            interval_secs: None,
            services: vec![],
            topic: "onepassword".to_string(),
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for OnePasswordSourceConfig {
    type Connection = OnePasswordConnection;

    fn connections(&self) -> &[OnePasswordConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<OnePasswordConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &OnePasswordConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &OnePasswordConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &OnePasswordConnection) {
        overlay_opt(&mut self.token, &connection.token);
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(&mut self.api_url_override, &connection.api_url_override);
    }
}

/// 1Password service to fetch data from. Each maps to one Events Reporting
/// endpoint: `signinattempts`, `itemusages`, or `auditevents`.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct OnePasswordService {
    /// Service name: one of "signin_attempts", "item_usages", "audit_events".
    pub name: String,

    /// Service-specific configuration. Recognised keys:
    /// - `limit`: page size (default 100, max 1000).
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// CrowdStrike Falcon source configuration.
///
/// Pulls detections, incidents, and host data from CrowdStrike Falcon's
/// public API via OAuth2 client_credentials. Region-aware: each Falcon
/// instance lives on a different cloud (US-1, US-2, EU-1, US-GOV-1) and
/// the API host changes accordingly.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct CrowdstrikeSourceConfig {
    /// Enable CrowdStrike source.
    pub enabled: bool,

    /// API base URL for the region the tenant lives on.
    /// Examples:
    /// - US-1: `https://api.crowdstrike.com` (default)
    /// - US-2: `https://api.us-2.crowdstrike.com`
    /// - EU-1: `https://api.eu-1.crowdstrike.com`
    /// - US-GOV-1: `https://api.laggar.gcw.crowdstrike.com`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// OAuth2 client ID (Falcon API client, created in Falcon admin console).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// OAuth2 client secret. Always redacted on serialisation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SensitiveString>,

    /// Secret source spec for the client_secret
    /// (e.g. `vault:kv/data/crowdstrike:client_secret`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<CrowdstrikeService>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple Falcon tenants of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<CrowdstrikeConnection>,
}

/// One CrowdStrike connection: a Falcon tenant (region + OAuth2 client). Type-
/// wide fields (`services`, `topic`, `filter`) are shared and stay on
/// [`CrowdstrikeSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CrowdstrikeConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// API base URL for the region the tenant lives on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// OAuth2 client ID (Falcon API client).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// OAuth2 client secret (always redacted on serialisation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SensitiveString>,

    /// Secret source spec for the client_secret ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

impl Default for CrowdstrikeSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_url_override: None,
            client_id: None,
            client_secret: None,
            credential_secret: None,
            interval_secs: None,
            services: vec![],
            topic: "crowdstrike".to_string(),
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for CrowdstrikeSourceConfig {
    type Connection = CrowdstrikeConnection;

    fn connections(&self) -> &[CrowdstrikeConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<CrowdstrikeConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &CrowdstrikeConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &CrowdstrikeConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &CrowdstrikeConnection) {
        overlay_opt(&mut self.api_url_override, &connection.api_url_override);
        overlay_opt(&mut self.client_id, &connection.client_id);
        overlay_opt(&mut self.client_secret, &connection.client_secret);
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
    }
}

/// CrowdStrike service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CrowdstrikeService {
    /// Service name (currently only "alerts" - returns enriched EPP alert
    /// entities from the Alerts API v2).
    pub name: String,

    /// Service-specific configuration. Recognised keys for `alerts`:
    /// - `limit`: query page size (default 100, max 1000).
    /// - `filter`: Falcon Query Language clause appended to the auto-built
    ///   `created_timestamp:>'<start>'+created_timestamp:<'<end>'`.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Slack audit-log source configuration.
///
/// Pulls audit-log events from `https://api.slack.com/audit/v1/logs`.
/// Enterprise Grid only - requires an Org Admin token with the
/// `auditlogs:read` scope.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SlackSourceConfig {
    /// Enable Slack source.
    pub enabled: bool,

    /// Org-admin user token (xoxa-... or xoxb-...) with `auditlogs:read`.
    /// Always redacted on serialisation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SensitiveString>,

    /// Secret source spec for the token
    /// (e.g. `vault:kv/data/slack:audit_token`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<SlackService>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple Slack orgs of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<SlackConnection>,
}

/// One Slack connection: an Enterprise Grid org + its admin token. Type-wide
/// fields (`services`, `topic`, `filter`) are shared and stay on
/// [`SlackSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SlackConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// Org-admin user token (always redacted on serialisation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SensitiveString>,

    /// Secret source spec for the token ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

impl Default for SlackSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            token: None,
            credential_secret: None,
            api_url_override: None,
            interval_secs: None,
            services: vec![],
            topic: "slack".to_string(),
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for SlackSourceConfig {
    type Connection = SlackConnection;

    fn connections(&self) -> &[SlackConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<SlackConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &SlackConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &SlackConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &SlackConnection) {
        overlay_opt(&mut self.token, &connection.token);
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(&mut self.api_url_override, &connection.api_url_override);
    }
}

/// Slack service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SlackService {
    /// Service name (currently only "audit_logs").
    pub name: String,

    /// Service-specific configuration. Recognised keys for `audit_logs`:
    /// - `action`: filter by action name (e.g. `user_login`).
    /// - `entity`: filter by entity type (`user` / `workspace` / etc).
    /// - `limit`: per-page size (default 200, max 1000).
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Bitwarden Events API source configuration.
///
/// Pulls events from a Bitwarden Teams/Enterprise organisation via
/// `/public/events`. Auth: OAuth2 client_credentials against
/// `/identity/connect/token` using organisation API credentials.
///
/// Supports both Bitwarden Cloud and self-hosted instances - set
/// `api_url_override` and `identity_url_override` for self-hosted.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct BitwardenSourceConfig {
    /// Enable Bitwarden source.
    pub enabled: bool,

    /// Organisation API client ID. Generated in Bitwarden admin:
    /// Settings > Organization info > "View API Key".
    /// Format is `organization.<uuid>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// Organisation API client secret. Always redacted on serialisation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SensitiveString>,

    /// Secret source spec for the client_secret
    /// (e.g. `vault:kv/data/bitwarden:client_secret`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override (default `https://api.bitwarden.com`).
    /// Self-hosted: `https://your-host/api`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Identity/token endpoint override
    /// (default `https://identity.bitwarden.com/connect/token`).
    /// Self-hosted: `https://your-host/identity/connect/token`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<BitwardenService>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple Bitwarden organisations of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<BitwardenConnection>,
}

/// One Bitwarden connection: an organisation + its API credentials. Type-wide
/// fields (`services`, `topic`, `filter`) are shared and stay on
/// [`BitwardenSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct BitwardenConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// Organisation API client ID (`organization.<uuid>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// Organisation API client secret (always redacted on serialisation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SensitiveString>,

    /// Secret source spec for the client_secret ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Identity/token endpoint override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

impl Default for BitwardenSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            client_id: None,
            client_secret: None,
            credential_secret: None,
            api_url_override: None,
            identity_url_override: None,
            interval_secs: None,
            services: vec![],
            topic: "bitwarden".to_string(),
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for BitwardenSourceConfig {
    type Connection = BitwardenConnection;

    fn connections(&self) -> &[BitwardenConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<BitwardenConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &BitwardenConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &BitwardenConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &BitwardenConnection) {
        overlay_opt(&mut self.client_id, &connection.client_id);
        overlay_opt(&mut self.client_secret, &connection.client_secret);
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(&mut self.api_url_override, &connection.api_url_override);
        overlay_opt(
            &mut self.identity_url_override,
            &connection.identity_url_override,
        );
    }
}

/// Bitwarden service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct BitwardenService {
    /// Service name (currently only "events").
    pub name: String,

    /// Service-specific configuration. No recognised keys yet.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Duo Admin API source configuration.
///
/// Pulls authentication events from a Duo tenant's
/// `api-XXXXXXXX.duosecurity.com/admin/v2/logs/authentication` endpoint.
/// Auth uses Duo's proprietary scheme: HMAC-SHA1 over a canonical request
/// signature, transported in a Basic auth header.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct DuoSourceConfig {
    /// Enable Duo source.
    pub enabled: bool,

    /// API hostname (without scheme). Format: `api-XXXXXXXX.duosecurity.com`.
    /// Available in the Duo Admin Panel under Applications > Admin API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_host: Option<String>,

    /// Integration key (`ikey`). Identifies the Admin API integration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integration_key: Option<String>,

    /// Secret key (`skey`). Used to compute the HMAC-SHA1 request signature.
    /// Always redacted on serialisation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<SensitiveString>,

    /// Secret source spec for the secret_key
    /// (e.g. `vault:kv/data/duo:skey`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override for testing (full URL incl. scheme). Production
    /// should use `api_host` only; this is for mock servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<DuoService>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple Duo tenants of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<DuoConnection>,
}

/// One Duo connection: a tenant (api_host) + its integration/secret keys.
/// Type-wide fields (`services`, `topic`, `filter`) are shared and stay on
/// [`DuoSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DuoConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// API hostname (without scheme).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_host: Option<String>,

    /// Integration key (`ikey`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integration_key: Option<String>,

    /// Secret key (`skey`) (always redacted on serialisation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<SensitiveString>,

    /// Secret source spec for the secret_key ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

impl Default for DuoSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_host: None,
            integration_key: None,
            secret_key: None,
            credential_secret: None,
            api_url_override: None,
            interval_secs: None,
            services: vec![],
            topic: "duo".to_string(),
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for DuoSourceConfig {
    type Connection = DuoConnection;

    fn connections(&self) -> &[DuoConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<DuoConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &DuoConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &DuoConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &DuoConnection) {
        overlay_opt(&mut self.api_host, &connection.api_host);
        overlay_opt(&mut self.integration_key, &connection.integration_key);
        overlay_opt(&mut self.secret_key, &connection.secret_key);
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(&mut self.api_url_override, &connection.api_url_override);
    }
}

/// Duo service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DuoService {
    /// Service name (currently only "authentication_logs").
    pub name: String,

    /// Service-specific configuration. Recognised keys for
    /// `authentication_logs`:
    /// - `limit`: per-page size (default 100, max 1000).
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

// -----------------------------------------------------------------------------
// Public-registry supply-chain audit sources (no auth required)
// -----------------------------------------------------------------------------

/// PyPI source configuration.
///
/// Fetches metadata for a configured list of PyPI packages on each tick.
/// Use case: supply-chain monitoring of HyperI-published Python packages -
/// downstream tooling computes deltas against a known-good baseline and
/// alerts on unexpected version, file, or maintainer changes.
///
/// No authentication required - all responses come from
/// `https://pypi.org/pypi/<package>/json`.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct PypiSourceConfig {
    /// Enable PyPI source.
    pub enabled: bool,

    /// Package names to monitor. Each becomes one record per fetch tick.
    pub packages: Vec<String>,

    /// API base override for testing (default `https://pypi.org`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,
}

impl Default for PypiSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            packages: vec![],
            api_url_override: None,
            interval_secs: None,
            topic: "pypi".to_string(),
            filter: None,
        }
    }
}

/// crates.io source configuration.
///
/// Fetches metadata for a configured list of crates on each tick.
/// No authentication required - all responses come from
/// `https://crates.io/api/v1/crates/<name>`.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct CratesIoSourceConfig {
    /// Enable crates.io source.
    pub enabled: bool,

    /// Crate names to monitor.
    pub crates: Vec<String>,

    /// API base override for testing (default `https://crates.io`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,
}

impl Default for CratesIoSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            crates: vec![],
            api_url_override: None,
            interval_secs: None,
            topic: "crates_io".to_string(),
            filter: None,
        }
    }
}

/// Google Workspace Reports API source configuration.
///
/// **Alpha** (code-complete, not production-validated) and additionally
/// pending hyperi-infra#5. The fetcher code is written
/// against the documented Workspace Reports API but cannot be live-tested
/// until the GCP service account is provisioned with domain-wide delegation
/// and the manual Admin-console scope grants are completed.
///
/// Pulls per-application audit/activity reports from
/// `admin.googleapis.com/admin/reports/v1/activity/users/all/applications/<app>`.
/// Auth: OAuth2 service account using JWT-with-subject (RS256) - the SA
/// impersonates a designated Workspace admin email so the Reports API
/// returns data scoped to the tenant.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct GoogleWorkspaceSourceConfig {
    /// Enable Google Workspace source.
    pub enabled: bool,

    /// Path to the service account JSON key file (same shape as the GCP
    /// source). The SA must have domain-wide delegation enabled and the
    /// required Reports API scopes granted in the Workspace Admin console.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_key: Option<String>,

    /// Secret source spec for the SA key
    /// (e.g. `vault:kv/data/google_workspace:sa_key`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Admin email the SA impersonates (the `sub` claim of the signed JWT).
    /// Must be a Workspace admin in the target tenant; without this the
    /// Reports API returns 403.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_email: Option<String>,

    /// Customer ID. Defaults to `my_customer` (the tenant the SA's
    /// impersonated admin belongs to). Explicit C-prefixed IDs are only
    /// needed for multi-tenant reseller scenarios.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub customer_id: Option<String>,

    /// API base override for testing
    /// (default `https://admin.googleapis.com`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Token endpoint override for testing
    /// (default `https://oauth2.googleapis.com/token`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<GoogleWorkspaceService>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple Workspace tenants of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<GoogleWorkspaceConnection>,
}

/// One Google Workspace connection: a tenant (impersonated admin) + its SA key.
/// Type-wide fields (`services`, `topic`, `filter`) are shared and stay on
/// [`GoogleWorkspaceSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GoogleWorkspaceConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// Path to the service account JSON key file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_key: Option<String>,

    /// Secret source spec for the SA key ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Admin email the SA impersonates (the `sub` claim of the signed JWT).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_email: Option<String>,

    /// Customer ID (defaults to `my_customer` when unset).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub customer_id: Option<String>,

    /// API base override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Token endpoint override for testing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

impl Default for GoogleWorkspaceSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_account_key: None,
            credential_secret: None,
            admin_email: None,
            customer_id: None,
            api_url_override: None,
            token_url_override: None,
            interval_secs: None,
            services: vec![],
            topic: "google_workspace".to_string(),
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for GoogleWorkspaceSourceConfig {
    type Connection = GoogleWorkspaceConnection;

    fn connections(&self) -> &[GoogleWorkspaceConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<GoogleWorkspaceConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &GoogleWorkspaceConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &GoogleWorkspaceConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &GoogleWorkspaceConnection) {
        overlay_opt(
            &mut self.service_account_key,
            &connection.service_account_key,
        );
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(&mut self.admin_email, &connection.admin_email);
        overlay_opt(&mut self.customer_id, &connection.customer_id);
        overlay_opt(&mut self.api_url_override, &connection.api_url_override);
        overlay_opt(&mut self.token_url_override, &connection.token_url_override);
    }
}

/// Google Workspace service. Each maps to one `applicationName` under the
/// Reports API: `login`, `admin`, `drive`, `mobile`, `groups`, `calendar`,
/// `chat`, `meet`, `token`, etc.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GoogleWorkspaceService {
    /// Service name; passed directly as the `applicationName` URL segment.
    pub name: String,

    /// Service-specific configuration. Recognised keys:
    /// - `event_name`: filter to a single event name (optional).
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Salesforce audit source configuration.
///
/// **Alpha** (code-complete, not production-validated) and additionally
/// pending a Salesforce connected app + integration user for live testing
/// (same status as [`GoogleWorkspaceSourceConfig`] and
/// [`GcpPubsubSourceConfig`]). The fetcher code is complete but cannot be
/// live-tested until a connected app is provisioned in a HyperI Salesforce org.
///
/// Pulls security/audit data from a Salesforce org via the REST API:
/// - `setup_audit_trail` - admin config changes (SOQL, every org)
/// - `login_history` - login events (SOQL, every org)
/// - `event_log_file` - runtime events as downloadable CSV log files
///   (7 event types free with 1-day retention; 70+ with the Event
///   Monitoring / Shield add-on)
///
/// Auth is OAuth2 against `<login_url>/services/oauth2/token`, via one of
/// two server-to-server flows selected by which fields are set:
/// - **JWT bearer** (Salesforce-recommended): set `client_id` (connected
///   app consumer key), `username` (integration user), and one of
///   `private_key` / `private_key_secret` (RSA private key PEM). An RS256
///   JWT is signed and exchanged for a token.
/// - **client credentials**: set `client_id` + one of `client_secret` /
///   `credential_secret`. Requires the connected app to have the client-
///   credentials flow enabled with a run-as user.
///
/// The token response carries an `instance_url` which is used for all
/// subsequent API calls (not `login_url`). `instance_url_override` pins it
/// for testing or custom-domain deployments.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SalesforceSourceConfig {
    /// Enable Salesforce source.
    pub enabled: bool,

    /// OAuth2 login base URL. Default `https://login.salesforce.com`.
    /// Sandboxes use `https://test.salesforce.com`; My Domain orgs may use
    /// `https://<mydomain>.my.salesforce.com`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub login_url: Option<String>,

    /// REST API version path segment. Default `v60.0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,

    /// Connected app consumer key. Used as the JWT `iss` claim (JWT bearer)
    /// or the `client_id` form field (client credentials).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// Integration username (JWT `sub` claim). JWT-bearer flow only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// RSA private key PEM for the JWT-bearer flow (full
    /// `-----BEGIN PRIVATE KEY-----` ... block). JWT-bearer flow only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key: Option<String>,

    /// Secret source spec resolving to the RSA private key PEM
    /// (e.g. `vault:kv/data/salesforce:private_key`). Takes precedence over
    /// `private_key` when set. JWT-bearer flow only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key_secret: Option<String>,

    /// Connected app consumer secret. client-credentials flow only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SensitiveString>,

    /// Secret source spec resolving to the consumer secret
    /// (e.g. `vault:kv/data/salesforce:client_secret`). Takes precedence
    /// over `client_secret` when set. client-credentials flow only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Pin the API instance URL instead of using the token response's
    /// `instance_url`. For testing or custom-domain deployments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Services to fetch from.
    pub services: Vec<SalesforceService>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,

    /// Multiple Salesforce orgs of this type, each polled independently.
    /// Empty = a single implicit connection from the fields above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<SalesforceConnection>,
}

/// One Salesforce connection: an org + its OAuth2 credentials (JWT-bearer or
/// client-credentials). Type-wide fields (`services`, `topic`, `filter`) are
/// shared and stay on [`SalesforceSourceConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SalesforceConnection {
    /// Stable, unique connection id (cursor key + metric/log label).
    pub id: String,

    /// OAuth2 login base URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub login_url: Option<String>,

    /// REST API version path segment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,

    /// Connected app consumer key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// Integration username (JWT `sub` claim). JWT-bearer flow only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// RSA private key PEM for the JWT-bearer flow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key: Option<String>,

    /// Secret source spec resolving to the RSA private key PEM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key_secret: Option<String>,

    /// Connected app consumer secret (client-credentials flow only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SensitiveString>,

    /// Secret source spec resolving to the consumer secret ("provider:path:key").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Pin the API instance URL for this connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_url_override: Option<String>,

    /// Per-connection fetch-interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

impl Default for SalesforceSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            login_url: None,
            api_version: None,
            client_id: None,
            username: None,
            private_key: None,
            private_key_secret: None,
            client_secret: None,
            credential_secret: None,
            instance_url_override: None,
            interval_secs: None,
            services: vec![],
            topic: "salesforce".to_string(),
            filter: None,
            connections: vec![],
        }
    }
}

impl ConnectionOverlay for SalesforceSourceConfig {
    type Connection = SalesforceConnection;

    fn connections(&self) -> &[SalesforceConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<SalesforceConnection> {
        &mut self.connections
    }

    fn interval_secs(&self) -> Option<u64> {
        self.interval_secs
    }

    fn connection_id(connection: &SalesforceConnection) -> &str {
        &connection.id
    }

    fn connection_interval_secs(connection: &SalesforceConnection) -> Option<u64> {
        connection.interval_secs
    }

    fn overlay(&mut self, connection: &SalesforceConnection) {
        overlay_opt(&mut self.login_url, &connection.login_url);
        overlay_opt(&mut self.api_version, &connection.api_version);
        overlay_opt(&mut self.client_id, &connection.client_id);
        overlay_opt(&mut self.username, &connection.username);
        overlay_opt(&mut self.private_key, &connection.private_key);
        overlay_opt(&mut self.private_key_secret, &connection.private_key_secret);
        overlay_opt(&mut self.client_secret, &connection.client_secret);
        overlay_opt(&mut self.credential_secret, &connection.credential_secret);
        overlay_opt(
            &mut self.instance_url_override,
            &connection.instance_url_override,
        );
    }
}

/// Salesforce service. `name` selects the audit surface:
/// `setup_audit_trail`, `login_history`, or `event_log_file`.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SalesforceService {
    /// Service name (audit surface to pull).
    pub name: String,

    /// Service-specific configuration. Recognised keys (event_log_file):
    /// - `event_types`: array of EventType values to include (default all)
    /// - `interval`: `"Hourly"` or `"Daily"` (default `"Daily"`)
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// GCP Pub/Sub pull source configuration.
///
/// **Alpha** (code-complete, not production-validated) and additionally
/// pending a hyperi-infra issue (TBD). The Pub/Sub pull
/// source is written but cannot be exercised against the live HyperI GCP
/// tenant until:
///
/// 1. A Cloud Logging Log Sink is provisioned to route the desired
///    log entries (audit, VPC flow, DNS query, custom workloads) into
///    a Pub/Sub topic.
/// 2. A Pub/Sub subscription is created on that topic for the fetcher
///    SA to pull from.
/// 3. The fetcher's GCP SA is granted `roles/pubsub.subscriber` on the
///    subscription.
///
/// Subscriptions are read via the REST `:pull` endpoint (synchronous
/// pull); the fetcher acknowledges drained messages with `:acknowledge`
/// after they have been emitted to Kafka. The gRPC StreamingPull variant
/// is intentionally not used - fetcher volumes do not justify the extra
/// dependency surface (tonic + protobuf). If a tenant's subscription
/// ever sustains volumes that REST pull cannot keep up with, revisit
/// with a v2 source.
///
/// Each subscription is one entry under `subscriptions`. The fetcher
/// emits one record per Pub/Sub message; `message.data` is base64
/// decoded and parsed as JSON if possible (typical for Log Sink
/// payloads), otherwise emitted as a string.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct GcpPubsubSourceConfig {
    /// Enable Pub/Sub pull source.
    pub enabled: bool,

    /// Path to a GCP service account JSON key file. The SA must have
    /// `roles/pubsub.subscriber` on every configured subscription.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_key: Option<String>,

    /// Secret source spec for the SA key
    /// (e.g. `vault:kv/data/gcp-pubsub:sa_key`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// API base override for testing
    /// (default `https://pubsub.googleapis.com`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// OAuth2 token endpoint override for testing
    /// (default `https://oauth2.googleapis.com/token`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Subscriptions to pull from. Each becomes one `FetchResult`
    /// tagged `gcp_pubsub.<subscription-id>`.
    pub subscriptions: Vec<GcpPubsubSubscription>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,
}

impl Default for GcpPubsubSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_account_key: None,
            credential_secret: None,
            api_url_override: None,
            token_url_override: None,
            interval_secs: None,
            subscriptions: vec![],
            topic: "gcp_pubsub".to_string(),
            filter: None,
        }
    }
}

/// A single Pub/Sub subscription configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GcpPubsubSubscription {
    /// GCP project ID owning the subscription.
    pub project_id: String,

    /// Subscription ID (the short name, not the fully-qualified path).
    pub subscription_id: String,

    /// Maximum messages to pull per tick. Default 1000 (the REST API cap).
    #[serde(default = "default_pubsub_max_messages")]
    pub max_messages: u32,

    /// Whether to return immediately when the subscription is empty,
    /// or block for `returnImmediately=false` server-side wait.
    /// Default true (single-shot, fits the polling fetcher model).
    #[serde(default = "default_pubsub_return_immediately")]
    pub return_immediately: bool,
}

fn default_pubsub_max_messages() -> u32 {
    1000
}

fn default_pubsub_return_immediately() -> bool {
    true
}

/// Object-store source family configuration.
///
/// Polls one or more bucket prefixes across S3 / GCS / Azure Blob,
/// emitting one record per line of every new object since the cursor.
///
/// Only the S3 backend is implemented. GCS and Azure Blob compile but their
/// `list_new_objects` / `get_object` calls return a not-implemented error, so
/// configuring one is safe (the source skips it with a warning) and fetches
/// nothing.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct ObjectStoreSourceConfig {
    /// Enable object-store source.
    pub enabled: bool,

    /// One or more cloud-store backends to poll.
    pub backends: Vec<ObjectStoreBackendConfig>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Default output topic. Each prefix may override it with its own
    /// `topic:` field.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,
}

impl Default for ObjectStoreSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backends: vec![],
            interval_secs: None,
            topic: "object_store".to_string(),
            filter: None,
        }
    }
}

/// One backend: provider + auth + buckets/containers to tail.
///
/// Provider is selected by the `provider` discriminator (serde-tagged
/// enum). Each provider variant carries its own auth shape; only S3 is
/// implemented.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum ObjectStoreBackendConfig {
    /// Amazon S3 (or S3-compatible: MinIO, R2, B2 via endpoint_override).
    S3(S3BackendConfig),

    /// Google Cloud Storage. **Not implemented** - listing and fetching both
    /// return an error.
    Gcs(GcsBackendConfig),

    /// Azure Blob Storage. **Not implemented** - listing and fetching both
    /// return an error.
    AzureBlob(AzureBlobBackendConfig),
}

/// S3 backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct S3BackendConfig {
    /// AWS region for SigV4 signing + endpoint construction.
    pub region: String,

    /// Optional S3 endpoint override for S3-compatible stores
    /// (MinIO, R2, B2) or for VPC endpoints. When unset, uses
    /// `https://<bucket>.s3.<region>.amazonaws.com`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_override: Option<String>,

    /// AWS access key ID. May be a `vault:` or `env:` spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,

    /// AWS secret access key. May be a `vault:` or `env:` spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<SensitiveString>,

    /// Vault secret spec containing both access_key_id +
    /// secret_access_key as a JSON object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Buckets + prefixes to tail.
    pub buckets: Vec<ObjectStoreBucket>,
}

/// GCS backend configuration. **Not implemented.**
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GcsBackendConfig {
    /// Service account JSON key path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_key: Option<String>,

    /// Vault secret spec for the SA key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Buckets + prefixes to tail.
    pub buckets: Vec<ObjectStoreBucket>,
}

/// Azure Blob backend configuration. **Not implemented.**
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AzureBlobBackendConfig {
    /// Storage account name (the `<name>` in
    /// `https://<name>.blob.core.windows.net`).
    pub account: String,

    /// Shared Key (account key). May be a `vault:` or `env:` spec.
    /// Mutually exclusive with `sas_token` and `tenant_id`/`client_*`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_key: Option<SensitiveString>,

    /// SAS token (without leading `?`). Mutually exclusive with
    /// `account_key` and SP credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sas_token: Option<SensitiveString>,

    /// Entra Service-Principal tenant ID (for OAuth2 bearer auth).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,

    /// Entra Service-Principal client ID (for OAuth2 bearer auth).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// Entra Service-Principal client secret (for OAuth2 bearer auth).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SensitiveString>,

    /// Vault secret spec for whichever auth method is in use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_secret: Option<String>,

    /// Blob containers + prefixes to tail. The `bucket` field on each
    /// entry is the container name.
    pub buckets: Vec<ObjectStoreBucket>,
}

/// One bucket / container to tail, plus its prefixes.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ObjectStoreBucket {
    /// Bucket (S3, GCS) or container (Azure Blob) name.
    pub bucket: String,

    /// Prefixes within the bucket to poll. At least one is required.
    pub prefixes: Vec<ObjectStorePrefix>,
}

/// One prefix to tail, plus its format and routing.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ObjectStorePrefix {
    /// Object key prefix (may be empty to scan the whole bucket).
    #[serde(default)]
    pub prefix: String,

    /// Object format. Drives parser dispatch and gzip handling.
    pub format: ObjectStoreFormat,

    /// Source tag attached to emitted records (e.g. `aws_cloudtrail`,
    /// `aws_vpc_flow`, `gcp_audit_sink`). Becomes the `source` field on
    /// the `FetchResult` as `object_store.<source_tag>`.
    pub source_tag: String,

    /// Per-prefix Kafka topic override. Falls back to the source-level
    /// `topic` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

/// Object body format. Drives gzip handling + parser dispatch.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreFormat {
    /// Gzipped JSON-lines (one JSON value per newline).
    JsonGz,
    /// Plain JSON-lines (one JSON value per newline).
    Jsonl,
    /// Single JSON document (array -> one record per element,
    /// object -> one record).
    Json,
    /// Gzipped plain text. Emits each non-empty line as `{"line": "..."}` -
    /// there is no ALB / CloudFront / S3 access-log parsing.
    TextGz,
    /// Plain text. Same as `text_gz` minus the gunzip step.
    Text,
}

/// Go module-proxy source configuration.
///
/// Fetches version list + per-version `.info` metadata for a configured list
/// of Go modules. The default proxy is Google's at
/// `https://proxy.golang.org` - free for everyone, GOPROXY-compatible.
///
/// No authentication required.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct GoModulesSourceConfig {
    /// Enable Go modules source.
    pub enabled: bool,

    /// Module paths to monitor (e.g. `github.com/hyperi-io/dfe-loader`).
    pub modules: Vec<String>,

    /// Module proxy base override (default `https://proxy.golang.org`).
    /// Use an internal mirror if desired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url_override: Option<String>,

    /// Fetch interval override in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,

    /// Output Kafka topic.
    pub topic: String,

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,
}

impl Default for GoModulesSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            modules: vec![],
            api_url_override: None,
            interval_secs: None,
            topic: "go_modules".to_string(),
            filter: None,
        }
    }
}

// =============================================================================
// Extractors configuration (containers, vector)
// =============================================================================

/// External extractors configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct ExtractorsConfig {
    /// Container-based extractors.
    pub containers: Vec<ContainerExtractorConfig>,

    /// Deprecated plugin config (kept for backwards-compatible deserialisation).
    pub plugins: PluginsConfig,

    /// Vector.dev extractor integration.
    pub vector: VectorExtractorConfig,
}

impl Default for ExtractorsConfig {
    fn default() -> Self {
        Self {
            containers: vec![],
            plugins: PluginsConfig::default(),
            vector: VectorExtractorConfig::default(),
        }
    }
}

/// Container-based extractor configuration.
///
/// Each container runs an isolated extraction tool (any language/runtime).
/// One container per source + config -- no horizontal scaling needed.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ContainerExtractorConfig {
    /// Unique name for this extractor instance.
    pub name: String,

    /// Container image (e.g., "ghcr.io/org/tool:latest").
    pub image: String,

    /// Container runtime (docker, podman). Default: "docker".
    pub runtime: Option<String>,

    /// Run mode: "scheduled" (one-shot per tick) or "continuous" (long-running).
    #[serde(default = "default_scheduled")]
    pub mode: String,

    /// Communication mode: "stdout" (JSON lines) or "http" (POST to /ingest).
    #[serde(default = "default_stdout")]
    pub communication: String,

    /// Output Kafka topic for this extractor's data.
    pub topic: String,

    /// Fetch interval for scheduled mode (seconds).
    pub interval_secs: Option<u64>,

    /// Environment variables passed to the container.
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Volume mounts (host:container format).
    #[serde(default)]
    pub volumes: Vec<String>,

    /// Docker network to attach to.
    pub network: Option<String>,

    /// Container memory limit (e.g., "512m", "1g").
    pub memory_limit: Option<String>,

    /// Container CPU limit (e.g., 0.5, 1.0, 2.0).
    pub cpu_limit: Option<f64>,

    /// Override container command.
    pub command: Option<Vec<String>>,

    /// Timeout in seconds for scheduled (one-shot) containers (0 = no timeout).
    #[serde(default)]
    pub timeout_secs: Option<u64>,

    /// Image pull policy: "always", "if-not-present", "never". Default: "if-not-present".
    #[serde(default = "default_pull_policy")]
    pub pull_policy: String,

    /// Maximum restart attempts for continuous mode (0 = unlimited).
    #[serde(default)]
    pub max_restart_attempts: u32,

    /// Maximum backoff delay between restarts in seconds.
    #[serde(default = "default_restart_backoff_max")]
    pub max_restart_backoff_secs: u64,

    /// Seconds of stable running before resetting backoff counter.
    #[serde(default = "default_stable_after")]
    pub stable_after_secs: u64,
}

fn default_restart_backoff_max() -> u64 {
    60
}

fn default_stable_after() -> u64 {
    300
}

fn default_pull_policy() -> String {
    "if-not-present".to_string()
}

fn default_scheduled() -> String {
    "scheduled".to_string()
}

fn default_stdout() -> String {
    "stdout".to_string()
}

/// Deprecated plugin configuration -- logs warning if non-empty values present.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PluginsConfig {
    #[serde(default)]
    pub directory: Option<String>,
    #[serde(flatten, default)]
    #[allow(clippy::pub_underscore_fields)]
    pub _rest: serde_json::Map<String, serde_json::Value>,
}

impl PluginsConfig {
    pub fn warn_if_configured(&self) {
        if self.directory.is_some() || !self._rest.is_empty() {
            tracing::warn!("extractors.plugins is deprecated -- use container extractors instead");
        }
    }
}

/// Vector.dev extractor configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct VectorExtractorConfig {
    /// Enable Vector extractor integration.
    pub enabled: bool,

    /// gRPC bind address for receiving Vector sink data.
    pub grpc_bind_address: String,

    /// Managed Vector instances.
    pub instances: Vec<VectorInstance>,
}

impl Default for VectorExtractorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            grpc_bind_address: "0.0.0.0:6000".to_string(),
            instances: vec![],
        }
    }
}

/// A managed Vector instance configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct VectorInstance {
    /// Instance name.
    pub name: String,

    /// Run mode: "container" or "sidecar".
    #[serde(default = "default_container_mode")]
    pub mode: String,

    /// Container image for container mode.
    pub image: Option<String>,

    /// Vector configuration (inline TOML/YAML).
    pub vector_config: Option<String>,

    /// Path to Vector configuration file.
    pub vector_config_path: Option<String>,

    /// Output Kafka topic.
    pub topic: String,
}

fn default_container_mode() -> String {
    "container".to_string()
}

// =============================================================================
// Ingest server configuration (for container extractors)
// =============================================================================

/// HTTP ingest server for container extractors to post data.
///
/// Off unless a deployment enables it. `/ingest/{source}/{topic}` takes the
/// destination topic from the URL, so an exposed listener with no token lets
/// anything that can reach the port write to any topic.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct IngestConfig {
    /// Enable ingest HTTP endpoint. Off by default.
    pub enabled: bool,

    /// Bind address.
    pub bind_address: String,

    /// Maximum request body size in bytes.
    pub max_body_size: usize,

    /// Bearer token for authentication (credential resolver format).
    /// Empty or absent = no auth (backward compatible, logs warning).
    #[serde(default)]
    pub auth_token: Option<String>,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            // Off unless a deployment asks for it: the listener is work in its
            // own right, so defaulting it on holds every fetcher out of idle
            // and binds a port no deployment publishes.
            enabled: false,
            bind_address: "0.0.0.0:8080".to_string(),
            max_body_size: 10 * 1024 * 1024, // 10MB
            auth_token: None,
        }
    }
}

// =============================================================================
// Kafka configuration (output)
// =============================================================================

/// Legacy Kafka producer configuration.
///
/// **Deprecated:** Use `output.kafka` (scalo `KafkaConfig`) instead.
/// This struct is kept for backward compatibility with existing config files
/// that use the top-level `kafka:` section. Will be removed in next major version.
///
/// Migration: move your `kafka:` settings under `output.kafka:` using scalo
/// `KafkaConfig` format (profiles, `librdkafka_overrides`, standard field names).
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct KafkaConfig {
    /// Broker addresses.
    pub brokers: Vec<String>,

    /// Client ID.
    pub client_id: String,

    /// Suffix appended to source topic names.
    pub topic_suffix: String,

    /// SASL configuration.
    pub sasl: Option<SaslConfig>,

    /// TLS configuration.
    pub tls: KafkaTlsConfig,

    /// Producer-specific settings.
    pub producer: ProducerConfig,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: vec![],
            client_id: "dfe-fetcher".to_string(),
            topic_suffix: "_land".to_string(),
            sasl: None,
            tls: KafkaTlsConfig::default(),
            producer: ProducerConfig::default(),
        }
    }
}

/// SASL authentication configuration.
///
/// Every field defaults, so the two secret env vars the chart injects are a
/// complete SASL block on their own -- which is what the chart claims they are.
/// Without that, a partial block was a startup parse error.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct SaslConfig {
    /// Enable SASL. Defaults to TRUE, because reaching this struct at all means
    /// a `kafka.sasl` block exists -- written by hand or built out of the two
    /// secret env vars the chart injects -- and that is a request for SASL.
    /// `enabled: false` still switches it off and is never inferred away.
    /// `kafka.sasl` itself defaults to absent, so a config that says nothing
    /// gets no SASL.
    pub enabled: bool,

    /// SASL mechanism, in librdkafka's spelling: `PLAIN`, `SCRAM-SHA-256`,
    /// `SCRAM-SHA-512`, `GSSAPI`, `OAUTHBEARER`. Passed through verbatim, so
    /// anything else is refused by the client at connect.
    pub mechanism: String,

    /// Username.
    pub username: String,

    /// Password (always redacted in serialisation/debug output).
    ///
    /// The schema default is pinned to the empty string: serialising the real
    /// default outside an expose window yields the mask, and a UI that
    /// pre-filled a password box with `***REDACTED***` would send that as the
    /// password.
    #[schemars(extend("default" = ""))]
    pub password: SensitiveString,
}

impl Default for SaslConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mechanism: "SCRAM-SHA-512".to_string(),
            username: String::new(),
            password: SensitiveString::default(),
        }
    }
}

/// Kafka TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct KafkaTlsConfig {
    /// Enable TLS for Kafka.
    pub enabled: bool,

    /// CA certificate file.
    pub ca_file: Option<String>,

    /// Client certificate file.
    pub cert_file: Option<String>,

    /// Client key file.
    pub key_file: Option<String>,
}

impl Default for KafkaTlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_file: None,
            cert_file: None,
            key_file: None,
        }
    }
}

/// Kafka producer settings.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct ProducerConfig {
    /// Maximum batch size in bytes.
    pub batch_size: usize,

    /// Maximum messages per batch.
    pub batch_messages: usize,

    /// Linger time in milliseconds.
    pub linger_ms: u32,

    /// Compression type (none, gzip, snappy, lz4, zstd).
    pub compression: String,

    /// Acknowledgment level (0, 1, all).
    pub acks: String,

    /// Number of retries.
    pub retries: u32,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            batch_size: 8 * 1024 * 1024, // 8MiB
            batch_messages: 10_000,
            linger_ms: 20,
            compression: "lz4".to_string(),
            acks: "all".to_string(),
            retries: 5,
        }
    }
}

// =============================================================================
// Buffer configuration
// =============================================================================

/// Buffer and memory configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct BufferConfig {
    /// Maximum memory for buffers in bytes (0 = auto-detect 67% of available).
    pub memory_limit: usize,

    /// Memory pressure threshold (0.0-1.0).
    pub pressure_threshold: f64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            memory_limit: 0,
            pressure_threshold: 0.8,
        }
    }
}

// =============================================================================
// Metrics configuration
// =============================================================================

/// Metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct MetricsConfig {
    /// Enable metrics.
    pub enabled: bool,

    /// Metrics server address.
    pub address: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: "0.0.0.0:9090".to_string(),
        }
    }
}

// =============================================================================
// Output transport configuration
// =============================================================================

/// Output transport mode.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct OutputConfig {
    /// Transport type: "kafka", "grpc", or "both".
    #[serde(rename = "type", default = "default_output_type")]
    pub output_type: String,

    /// Kafka transport configuration (scalo KafkaConfig).
    #[serde(default)]
    pub kafka: Option<scalo::transport::KafkaConfig>,

    /// gRPC transport configuration (scalo GrpcConfig, client mode).
    #[serde(default)]
    pub grpc: Option<scalo::transport::GrpcConfig>,

    /// Suffix appended to source topic names (e.g., "_land").
    /// If set, takes precedence over legacy `kafka.topic_suffix`.
    #[serde(default)]
    pub topic_suffix: Option<String>,

    /// Named destinations beyond the default transports, keyed by name.
    ///
    /// The default destination is the source's own -- its topic on the bus,
    /// the configured endpoint on the direct transport. A destination declared
    /// here is reached by a [`route`](Self::routes) naming it, so a record can
    /// go to a transform instead of, or as well as, the default.
    #[serde(default)]
    pub destinations: std::collections::HashMap<String, DestinationSpec>,

    /// Per-record routes over the named destinations (first match wins).
    #[serde(default)]
    pub routes: Vec<OutputRoute>,
}

/// A declared destination: exactly one transport block.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct DestinationSpec {
    /// Deliver over gRPC to a scalo Push listener (a transform, the loader,
    /// the archiver).
    pub grpc: Option<scalo::transport::GrpcConfig>,

    /// Deliver over the bus.
    pub kafka: Option<KafkaDestination>,
}

/// A bus destination.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct KafkaDestination {
    /// Broker settings for this destination. Unset reuses the output's own.
    pub config: Option<scalo::transport::KafkaConfig>,

    /// Fixed topic. Unset means the record's own source topic.
    pub topic: Option<String>,
}

/// A per-record route: matched records go to the named destination(s) instead
/// of the default.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct OutputRoute {
    /// Top-level field to match.
    pub match_field: String,

    /// Value to match.
    pub match_value: String,

    /// Destination for matched records: one name, or a list to fan out.
    pub destination: DestinationRef,
}

/// One destination name, or a list of them to fan a matched record out to.
///
/// A fan-out is delivered when every destination has accepted it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum DestinationRef {
    /// A single destination name.
    One(String),
    /// Several destination names -- the record goes to all of them.
    Many(Vec<String>),
}

impl DestinationRef {
    /// The names, one or many.
    pub fn names(&self) -> &[String] {
        match self {
            Self::One(name) => std::slice::from_ref(name),
            Self::Many(names) => names,
        }
    }
}

impl From<&str> for DestinationRef {
    fn from(name: &str) -> Self {
        Self::One(name.to_string())
    }
}

fn default_output_type() -> String {
    "kafka".to_string()
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            output_type: default_output_type(),
            kafka: None,
            grpc: None,
            topic_suffix: None,
            destinations: std::collections::HashMap::new(),
            routes: Vec::new(),
        }
    }
}

impl OutputConfig {
    /// Check if output includes Kafka transport.
    pub fn includes_kafka(&self) -> bool {
        self.output_type == "kafka" || self.output_type == "both"
    }

    /// Check if output includes gRPC transport.
    pub fn includes_grpc(&self) -> bool {
        self.output_type == "grpc" || self.output_type == "both"
    }

    /// Whether anything the output can reach is on the bus -- the default
    /// transports or a declared destination. A deployment where this is false
    /// has no broker, so nothing may fall back to a Kafka topic.
    pub fn uses_bus(&self) -> bool {
        self.includes_kafka() || self.destinations.values().any(|d| d.kafka.is_some())
    }

    /// Validate the named destinations and the routes over them.
    ///
    /// # Errors
    /// A destination without exactly one transport block, or a route naming a
    /// destination that was never declared.
    pub fn validate_routes(&self) -> Result<()> {
        for (name, spec) in &self.destinations {
            match (&spec.grpc, &spec.kafka) {
                (Some(_), None) | (None, Some(_)) => {}
                _ => {
                    return Err(Error::Config(format!(
                        "output.destinations.{name} needs exactly one of grpc or kafka"
                    )));
                }
            }
        }
        for route in &self.routes {
            for name in route.destination.names() {
                if !self.destinations.contains_key(name) {
                    return Err(Error::Config(format!(
                        "output.routes names destination '{name}', which is not declared under output.destinations"
                    )));
                }
            }
        }
        Ok(())
    }
}

// =============================================================================
// Cursor store configuration
// =============================================================================

/// Cursor store configuration for incremental fetching.
///
/// Cursors are stored as individual JSON files in `directory`, one per
/// connection (e.g. `prod-aws.aws-acct-123.cursor.json`); every service of a
/// connection shares its cursor. The directory should be PVC-backed for pod
/// restart persistence.
///
/// If `directory` is empty, the cursor store falls back to the config
/// file's parent directory with a warning.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct CursorConfig {
    /// Directory for cursor files. Each connection gets its own file named
    /// `{instance_id}.{connection_id}.cursor.json`; a single implicit
    /// connection uses the source type name as its id.
    /// Empty = fall back to config file directory (with warning).
    pub directory: String,

    /// The widest span one tick fetches, in hours, and the lookback when no
    /// cursor exists -- a source further behind than this drains a span per
    /// tick, floored at the fetch interval.
    pub default_window_hours: u64,
}

impl Default for CursorConfig {
    fn default() -> Self {
        Self {
            directory: String::new(), // empty = auto-resolve from config path
            default_window_hours: 1,
        }
    }
}

#[cfg(test)]
#[allow(
    unsafe_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.scheduler.default_interval_secs, 300);
        assert_eq!(config.kafka.producer.batch_messages, 10_000);
        assert_eq!(config.kafka.topic_suffix, "_land");
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_pressure_threshold() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.buffer.pressure_threshold = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_scheduler_interval() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.scheduler.default_interval_secs = 0;
        assert!(config.validate().is_err());
    }

    // -- env override tests --

    // Serialise all tests that mutate process-wide env vars
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_env<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        for (k, v) in vars {
            // SAFETY: test-only, serialised by ENV_LOCK
            unsafe { std::env::set_var(k, v) };
        }
        f();
        for (k, _) in vars {
            // SAFETY: test-only, serialised by ENV_LOCK
            unsafe { std::env::remove_var(k) };
        }
    }

    #[test]
    fn test_env_override_kafka_brokers() {
        with_env(
            &[("DFE_FETCHER_KAFKA_BROKERS", "broker1:9092, broker2:9092")],
            || {
                let mut config = Config::default();
                config.apply_flat_env("DFE_FETCHER");
                assert_eq!(
                    config.kafka.brokers,
                    vec!["broker1:9092".to_string(), "broker2:9092".to_string()]
                );
            },
        );
    }

    #[test]
    fn test_env_override_kafka_client_id() {
        with_env(&[("DFE_FETCHER_KAFKA_CLIENT_ID", "my-fetcher")], || {
            let mut config = Config::default();
            config.apply_flat_env("DFE_FETCHER");
            assert_eq!(config.kafka.client_id, "my-fetcher");
        });
    }

    #[test]
    fn test_env_override_default_interval() {
        with_env(&[("DFE_FETCHER_DEFAULT_INTERVAL_SECS", "60")], || {
            let mut config = Config::default();
            config.apply_flat_env("DFE_FETCHER");
            assert_eq!(config.scheduler.default_interval_secs, 60);
        });
    }

    #[test]
    fn test_env_override_topic_suffix() {
        with_env(&[("DFE_FETCHER_TOPIC_SUFFIX", "_raw")], || {
            let mut config = Config::default();
            config.apply_flat_env("DFE_FETCHER");
            assert_eq!(config.kafka.topic_suffix, "_raw");
        });
    }

    #[test]
    fn test_topic_suffix_defaults_to_the_kafka_value() {
        let config = Config::default();
        assert_eq!(config.topic_suffix(), "_land");
    }

    #[test]
    fn test_topic_suffix_prefers_the_output_value() {
        let mut config = Config::default();
        config.output.topic_suffix = Some("_raw".to_string());
        assert_eq!(config.topic_suffix(), "_raw");
    }

    #[test]
    fn test_topic_suffix_honours_an_empty_output_value() {
        let mut config = Config::default();
        config.output.topic_suffix = Some(String::new());
        assert_eq!(config.topic_suffix(), "");
    }

    #[test]
    fn test_env_override_memory_limit() {
        with_env(&[("DFE_FETCHER_MEMORY_LIMIT", "1073741824")], || {
            let mut config = Config::default();
            config.apply_flat_env("DFE_FETCHER");
            assert_eq!(config.buffer.memory_limit, 1_073_741_824);
        });
    }

    #[test]
    fn test_env_override_metrics_address() {
        with_env(&[("DFE_FETCHER_METRICS_ADDRESS", "0.0.0.0:8888")], || {
            let mut config = Config::default();
            config.apply_flat_env("DFE_FETCHER");
            assert_eq!(config.metrics.address, "0.0.0.0:8888");
        });
    }

    #[test]
    fn test_valid_filter_expression() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.sources.aws.filter = Some(r#"eventName != "ConsoleLogin""#.to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_filter_expression() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.sources.aws.filter = Some("invalid @@@ expression".to_string());
        let result = config.validate();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("sources.aws.filter invalid"),
            "Error should mention aws filter: {err_msg}"
        );
    }

    /// The registry must list every typed block that has a `filter` field,
    /// since `filters()` is read off it: a block missing from the registry
    /// has its filter accepted at startup, unvalidated, and never applied --
    /// the operator's exclusions ship downstream silently.
    ///
    /// The expected set comes from the derived JSON Schema rather than a
    /// hand-written list, so adding a block with a `filter` field fails this
    /// test until the registry gains its entry.
    #[test]
    fn test_filter_table_covers_every_source() {
        let schema = serde_json::to_value(schemars::schema_for!(SourcesConfig))
            .expect("SourcesConfig schema serialises");
        let defs = schema
            .get("$defs")
            .and_then(serde_json::Value::as_object)
            .expect("schema has $defs");
        let properties = schema
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .expect("SourcesConfig schema has properties");

        let mut with_filter_field: Vec<&str> = properties
            .iter()
            .filter(|(_, prop)| {
                // Each source property is a `$ref` into `$defs`; the filter
                // field lives on the referenced type, not the reference.
                prop.get("$ref")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|r| r.rsplit('/').next())
                    .and_then(|name| defs.get(name))
                    .and_then(|def| def.get("properties"))
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|fields| fields.contains_key("filter"))
            })
            .map(|(name, _)| name.as_str())
            .collect();
        with_filter_field.sort_unstable();
        assert!(
            !with_filter_field.is_empty(),
            "found no source with a filter field -- the schema walk is broken, \
             not the table"
        );

        let sources = SourcesConfig::default();
        let mut tabled: Vec<&str> = sources.filters().into_iter().map(|(n, _)| n).collect();
        tabled.sort_unstable();
        let mut registered: Vec<&str> = REGISTRY.iter().map(|b| b.name).collect();
        registered.sort_unstable();

        assert_eq!(
            registered, with_filter_field,
            "the registry is out of step with the blocks that have a filter \
             field; add the missing block so its filter is both validated and \
             applied"
        );
        assert_eq!(tabled, registered, "filters() is read off the registry");
    }

    /// The idle gate reads `enabled_flags()`, `validate` reads the connection
    /// ids and the catalog reads the capabilities, all off the registry, so
    /// every registry entry must name a shipped profile and describe itself
    /// under that same name.
    #[test]
    fn test_enabled_table_matches_the_filter_table() {
        let sources = SourcesConfig::default();
        let enabled: Vec<&str> = sources
            .enabled_flags()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        let filtered: Vec<&str> = sources.filters().into_iter().map(|(n, _)| n).collect();
        let registered: Vec<&str> = REGISTRY.iter().map(|b| b.name).collect();
        assert_eq!(
            enabled, registered,
            "enabled_flags() is read off the registry"
        );
        assert_eq!(filtered, registered, "filters() is read off the registry");
        for block in &REGISTRY {
            assert!(
                crate::profiles::shipped().contains_key(block.name),
                "registry block `{}` names no shipped profile",
                block.name
            );
            assert_eq!(
                block.capability().name,
                block.name,
                "the catalog entry must carry the block's own name"
            );
            assert_eq!(
                block.connection_ids(&sources),
                vec![block.name],
                "with no connections the block resolves to its own name"
            );
            assert!(
                !block.enabled(&sources) && block.filter(&sources).is_none(),
                "a default block is off and unfiltered"
            );
        }
    }

    #[test]
    fn test_no_source_enabled_by_default() {
        assert!(
            !SourcesConfig::default().any_enabled(),
            "a fetcher with a default config has no work, and must idle rather than run"
        );
    }

    /// A connection id resolves to its block's filter through the registry:
    /// the block's own name for the implicit connection, each
    /// `connections[].id` otherwise, and never a name that merely shares a
    /// prefix.
    #[test]
    fn test_filter_for_source_resolves_the_connection_id_through_the_registry() {
        let mut sources = SourcesConfig::default();
        sources.crates_io.filter = Some("crates_filter".to_string());
        sources.gcp.filter = Some("gcp_only".to_string());
        assert_eq!(
            sources.filter_for_source("crates_io"),
            Some("crates_filter")
        );
        assert_eq!(sources.filter_for_source("crates"), None);
        assert_eq!(sources.filter_for_source("crates_io.metadata"), None);
        assert_eq!(sources.filter_for_source("gcp"), Some("gcp_only"));
        assert_eq!(
            sources.filter_for_source("gcp_pubsub"),
            None,
            "the gcp filter must not apply to gcp_pubsub records"
        );
        assert_eq!(sources.filter_for_source("unknown"), None);

        sources.github.filter = Some("action != \"git.clone\"".to_string());
        sources.github.connections = vec![GithubConnection {
            id: "gh-acme".into(),
            org: Some("acme".into()),
            ..GithubConnection::default()
        }];
        assert_eq!(
            sources.filter_for_source("gh-acme"),
            Some("action != \"git.clone\"")
        );
        assert_eq!(
            sources.filter_for_source("github"),
            None,
            "no implicit connection once connections are listed"
        );
    }

    /// A filter on any typed block reaches its driver: every registry entry
    /// resolves its own name to the filter set on it.
    #[test]
    fn test_filter_for_source_reaches_every_registry_block() {
        let mut sources = SourcesConfig::default();
        let expr = r#"action == "x""#.to_string();
        sources.github.filter = Some(expr.clone());
        sources.okta.filter = Some(expr.clone());
        sources.slack.filter = Some(expr.clone());
        sources.object_store.filter = Some(expr.clone());
        sources.salesforce.filter = Some(expr.clone());
        sources.google_workspace.filter = Some(expr.clone());
        for name in [
            "github",
            "okta",
            "slack",
            "object_store",
            "salesforce",
            "google_workspace",
        ] {
            assert_eq!(
                sources.filter_for_source(name),
                Some(expr.as_str()),
                "no filter routed for {name}, so its records ship unfiltered"
            );
        }
        let filtered: Vec<&str> = sources
            .filters()
            .into_iter()
            .filter_map(|(n, f)| f.map(|_| n))
            .collect();
        assert_eq!(
            filtered,
            [
                "github",
                "okta",
                "slack",
                "google_workspace",
                "object_store",
                "salesforce",
            ],
            "filters() lists the set filters in registry order"
        );
    }

    #[test]
    fn test_env_override_no_vars_set() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut config = Config::default();
        let original = config.clone();
        config.apply_flat_env("DFE_FETCHER");
        assert_eq!(config.kafka.brokers, original.kafka.brokers);
        assert_eq!(
            config.scheduler.default_interval_secs,
            original.scheduler.default_interval_secs
        );
        assert_eq!(config.config_reload_secs, original.config_reload_secs);
    }

    // =========================================================================
    // Helper: build a valid baseline config (kafka brokers populated)
    // =========================================================================

    fn valid_config() -> Config {
        let mut cfg = Config::default();
        cfg.kafka.brokers = vec!["localhost:9092".to_string()];
        cfg
    }

    // =========================================================================
    // 1. Config validation edge cases (expected failures)
    // =========================================================================

    /// The credential-spec refusal reaches every TYPED block, not just
    /// `sources.rest`: `validate_builtin_instances` maps each enabled block
    /// onto an instance of its shipped profile and validates that.
    #[test]
    fn a_typed_block_credential_spec_the_resolver_cannot_read_is_refused_at_load() {
        let mut cfg = valid_config();
        cfg.sources.okta.enabled = true;
        cfg.sources.okta.tenant_url = Some("https://example.okta.com".to_string());
        cfg.sources.okta.services = vec![OktaService {
            name: "system_log".to_string(),
            config: HashMap::new(),
        }];
        cfg.sources.okta.token = Some("vault:kv/data/okta:token".into());
        cfg.validate().expect("a well-formed vault spec loads");

        cfg.sources.okta.token = Some("file:/run/secrets/token".into());
        cfg.validate().expect("a file spec loads");

        cfg.sources.okta.token = Some("aws:prod/okta:token".into());
        let err = cfg.validate().unwrap_err().to_string();
        // Okta defaults to the SSWS header, so its token maps onto the
        // `api_key` mode and the refusal names `auth.key` rather than
        // `auth.token`.
        assert!(
            err.contains("sources.okta")
                && err.contains("auth.key")
                && err.contains("is not a credential spec")
                && err.contains("`secrets-aws`"),
            "{err}"
        );
        assert!(
            !err.contains("prod/okta"),
            "the refusal names the prefix and never echoes the spec: {err}"
        );
    }

    /// A misspelt `services[].name` is refused with the block's own name and
    /// the units its shipped profile declares, so the operator can correct
    /// the typo without opening the profile file.
    #[test]
    fn a_typed_block_service_the_profile_has_no_unit_for_is_refused_naming_the_accepted_names() {
        let mut cfg = valid_config();
        cfg.sources.aws.enabled = true;
        cfg.sources.aws.credential_secret = Some("vault:kv/data/aws:credentials".to_string());
        cfg.sources.aws.services = vec![AwsService {
            name: "cloudtrial".to_string(),
            config: HashMap::new(),
        }];

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("sources.aws"), "names the block: {err}");
        assert!(err.contains("cloudtrial"), "names the typo: {err}");
        assert!(
            err.contains("cloudtrail") && err.contains("guardduty"),
            "lists the units the profile declares: {err}"
        );

        cfg.sources.aws.services[0].name = "cloudtrail".to_string();
        cfg.validate().expect("a declared unit name loads");
    }

    #[test]
    fn test_validate_grpc_output_without_endpoint() {
        let mut cfg = valid_config();
        // The transport checks only fire on a config with work to send.
        cfg.sources.aws.enabled = true;
        cfg.output.output_type = "grpc".to_string();
        cfg.output.grpc = None;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("grpc.endpoint required"),
            "Expected 'grpc.endpoint required', got: {err}"
        );
    }

    #[test]
    fn test_validate_both_output_kafka_ok_grpc_missing() {
        let mut cfg = valid_config();
        cfg.sources.aws.enabled = true;
        cfg.output.output_type = "both".to_string();
        // kafka brokers are set via valid_config(), but no grpc endpoint
        cfg.output.grpc = None;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("grpc.endpoint required"),
            "Expected grpc endpoint error, got: {err}"
        );
    }

    /// The shape a fetcher is deployed in before an operator writes a source:
    /// the default output is the bus and no broker is named anywhere. It is
    /// valid but empty of work, which the idle gate holds -- so validate must
    /// not refuse it.
    #[test]
    fn test_validate_accepts_the_default_config_with_no_broker() {
        let cfg = Config::default();
        assert!(cfg.kafka.brokers.is_empty(), "the default names no broker");
        assert!(!cfg.has_work(), "the default config has nothing to send");
        cfg.validate()
            .expect("a valid-but-empty config idles, it is not refused");
    }

    /// The first enabled source turns the broker requirement back on: something
    /// would now produce records, and there is nowhere to put them.
    #[test]
    fn test_validate_refuses_an_enabled_source_with_no_broker() {
        let mut cfg = Config::default();
        cfg.sources.aws.enabled = true;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("kafka brokers required"),
            "Expected kafka brokers error, got: {err}"
        );
    }

    /// The ingest listener is a producer too, so turning it on demands the same
    /// broker an enabled source would.
    #[test]
    fn test_validate_refuses_an_enabled_ingest_listener_with_no_broker() {
        let mut cfg = Config::default();
        cfg.ingest.enabled = true;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("kafka brokers required"),
            "Expected kafka brokers error, got: {err}"
        );
    }

    #[test]
    fn test_validate_negative_pressure_threshold() {
        let mut cfg = valid_config();
        cfg.buffer.pressure_threshold = -0.1;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("pressure_threshold"),
            "Expected pressure_threshold error, got: {err}"
        );
    }

    #[test]
    fn test_validate_duplicate_container_extractor_names() {
        let mut cfg = valid_config();
        let container = ContainerExtractorConfig {
            name: "dup-name".to_string(),
            image: "img:latest".to_string(),
            runtime: None,
            mode: "scheduled".to_string(),
            communication: "stdout".to_string(),
            topic: "topic-a".to_string(),
            interval_secs: None,
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
            command: None,
            timeout_secs: None,
            pull_policy: "if-not-present".to_string(),
            max_restart_attempts: 0,
            max_restart_backoff_secs: 60,
            stable_after_secs: 300,
        };
        cfg.extractors.containers = vec![container.clone(), container];
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("duplicate container extractor name"),
            "Expected duplicate name error, got: {err}"
        );
    }

    /// An extractor env value is handed to the container verbatim, so a spec
    /// the resolver cannot read would reach the tool as literal text and fail
    /// as that tool's auth error rather than as a credential-configuration
    /// error of ours.
    #[test]
    fn a_container_env_spec_the_resolver_cannot_read_is_refused_at_load() {
        let container = |value: &str| ContainerExtractorConfig {
            name: "env-spec-test".to_string(),
            image: "img:latest".to_string(),
            runtime: None,
            mode: "scheduled".to_string(),
            communication: "stdout".to_string(),
            topic: "t".to_string(),
            interval_secs: None,
            env: HashMap::from([("AWS_SECRET_ACCESS_KEY".to_string(), value.to_string())]),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
            command: None,
            timeout_secs: None,
            pull_policy: "if-not-present".to_string(),
            max_restart_attempts: 0,
            max_restart_backoff_secs: 60,
            stable_after_secs: 300,
        };

        // What the resolver handles, and a plain value, both pass through.
        for value in [
            "vault:kv/data/aws:secret_key",
            "bao:kv/data/aws:secret_key",
            "openbao:kv/data/aws:secret_key",
            "file:/run/secrets/aws_secret_key",
            "env:AWS_SECRET",
            "AKIAEXAMPLE",
        ] {
            let mut cfg = valid_config();
            cfg.extractors.containers = vec![container(value)];
            cfg.validate()
                .unwrap_or_else(|e| panic!("{value} should load: {e}"));
        }

        let mut cfg = valid_config();
        cfg.extractors.containers = vec![container("aws:prod/aws:secret_key")];
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("env-spec-test")
                && err.contains("AWS_SECRET_ACCESS_KEY")
                && err.contains("is not a credential spec")
                && err.contains("`secrets-aws`"),
            "{err}"
        );
        assert!(
            !err.contains("prod/aws"),
            "the refusal names the prefix and never echoes the path: {err}"
        );
    }

    #[test]
    fn test_validate_container_extractor_empty_topic() {
        let mut cfg = valid_config();
        cfg.extractors.containers = vec![ContainerExtractorConfig {
            name: "empty-topic-test".to_string(),
            image: "img:latest".to_string(),
            runtime: None,
            mode: "scheduled".to_string(),
            communication: "stdout".to_string(),
            topic: String::new(), // empty
            interval_secs: None,
            env: HashMap::new(),
            volumes: vec![],
            network: None,
            memory_limit: None,
            cpu_limit: None,
            command: None,
            timeout_secs: None,
            pull_policy: "if-not-present".to_string(),
            max_restart_attempts: 0,
            max_restart_backoff_secs: 60,
            stable_after_secs: 300,
        }];
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("empty topic"),
            "Expected empty topic error, got: {err}"
        );
    }

    #[test]
    fn test_validate_invalid_ingest_bind_address() {
        let mut cfg = valid_config();
        cfg.ingest.enabled = true;
        cfg.ingest.bind_address = "not-an-address".to_string();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("invalid ingest bind address"),
            "Expected invalid ingest bind address error, got: {err}"
        );
    }

    #[test]
    fn test_validate_invalid_azure_filter() {
        let mut cfg = valid_config();
        cfg.sources.azure.filter = Some("invalid @@@ expression".to_string());
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("filter invalid"),
            "Expected azure filter invalid error, got: {err}"
        );
    }

    #[test]
    fn test_validate_invalid_m365_filter() {
        let mut cfg = valid_config();
        cfg.sources.m365.filter = Some("((( broken".to_string());
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("filter invalid"),
            "Expected m365 filter invalid error, got: {err}"
        );
    }

    #[test]
    fn test_validate_invalid_gcp_filter() {
        let mut cfg = valid_config();
        cfg.sources.gcp.filter = Some("not_a_valid @@".to_string());
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("filter invalid"),
            "Expected gcp filter invalid error, got: {err}"
        );
    }

    #[test]
    fn test_validate_invalid_vector_grpc_bind_address() {
        let mut cfg = valid_config();
        cfg.extractors.vector.enabled = true;
        cfg.extractors.vector.grpc_bind_address = "not-valid".to_string();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("invalid vector gRPC bind address"),
            "Expected invalid vector gRPC bind address error, got: {err}"
        );
    }

    // =========================================================================
    // 2. Config YAML loading
    // =========================================================================

    #[test]
    fn test_load_from_valid_yaml_file() {
        // load_from_file applies DFE_FETCHER_* env overrides, so serialise
        // against the env-override tests via ENV_LOCK -- otherwise a parallel
        // test's DFE_FETCHER_KAFKA_CLIENT_ID (etc.) bleeds into this file load.
        let _env_guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let yaml = r#"
scheduler:
  default_interval_secs: 120
  max_concurrent_fetches: 5
  jitter_percent: 20
kafka:
  brokers:
    - "broker1:9092"
  client_id: "test-client"
  topic_suffix: "_raw"
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-config.yaml");
        std::fs::write(&path, yaml).unwrap();

        let cfg = Config::load_from_file(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.scheduler.default_interval_secs, 120);
        assert_eq!(cfg.scheduler.max_concurrent_fetches, 5);
        assert_eq!(cfg.scheduler.jitter_percent, 20);
        assert_eq!(cfg.kafka.brokers, vec!["broker1:9092"]);
        assert_eq!(cfg.kafka.client_id, "test-client");
        assert_eq!(cfg.kafka.topic_suffix, "_raw");
        assert!(cfg.config_path.is_some());
    }

    #[test]
    fn test_load_from_nonexistent_path() {
        let result = Config::load_from_file("/nonexistent/path/config.yaml");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("failed to read config file"),
            "Expected file read error, got: {err}"
        );
    }

    #[test]
    fn test_load_from_invalid_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.yaml");
        std::fs::write(&path, "{{{{not valid yaml!!!!").unwrap();

        let result = Config::load_from_file(path.to_str().unwrap());
        assert!(result.is_err());
    }

    /// The cascade path (`Config::load(None)` -- no `--config`) must not swallow
    /// a deserialise failure.
    ///
    /// One mistyped value in `settings.yaml` / `defaults.yaml` /
    /// `/config/*.yaml` / a `DFE_FETCHER_*` env var is enough to fail the
    /// unmarshal. Substituting defaults there is undetectable downstream:
    /// `apply_flat_env` runs after the unmarshal, so env-supplied brokers land
    /// on the defaulted config, `validate()` passes, and `config-check` reports
    /// "configuration is valid" for a file it never read.
    ///
    /// Needs one process per test (`cargo nextest`), because `config::setup()`
    /// writes a `OnceCell`.
    #[test]
    fn test_cascade_load_surfaces_deserialise_failure() {
        with_env(
            &[
                // Reaches the config via apply_flat_env even when the unmarshal
                // fails, which is what lets validate() pass on defaults.
                ("DFE_FETCHER_KAFKA_BROKERS", "broker1:9092"),
                // u64 field, so the figment extract cannot coerce it.
                ("DFE_FETCHER_CONFIG_RELOAD_SECS", "not-a-number"),
            ],
            || {
                // An already-initialised global makes Config::load fail at
                // `config::setup` instead, which would pass the assertion below
                // for the wrong reason.
                assert!(
                    config::try_get().is_none(),
                    "the scalo config global is already initialised in this \
                     process, so Config::load(None) cannot reach the unmarshal. \
                     Run under `cargo nextest` (one process per test), not \
                     `cargo test`."
                );

                let err = Config::load(None)
                    .err()
                    .map(|e| e.to_string())
                    .unwrap_or_default();
                assert!(
                    err.contains("failed to parse configuration"),
                    "a mistyped config value must fail the load, not silently \
                     become Config::default(); got: {err:?}"
                );
            },
        );
    }

    #[test]
    fn test_load_yaml_with_all_sources_and_services() {
        let yaml = r#"
scheduler:
  default_interval_secs: 60
kafka:
  brokers: ["localhost:9092"]
sources:
  aws:
    enabled: true
    region: ap-southeast-2
    services:
      - name: cloudtrail
      - name: guardduty
  azure:
    enabled: true
    tenant_id: "tenant-1"
    services:
      - name: activity_log
      - name: defender
  m365:
    enabled: true
    tenant_id: "m365-tenant"
    services:
      - name: audit_log
  gcp:
    enabled: true
    project_id: "my-project"
    services:
      - name: audit_logs
      - name: scc
"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full.yaml");
        std::fs::write(&path, yaml).unwrap();

        let cfg = Config::load_from_file(path.to_str().unwrap()).unwrap();
        assert!(cfg.sources.aws.enabled);
        assert_eq!(cfg.sources.aws.region, "ap-southeast-2");
        assert_eq!(cfg.sources.aws.services.len(), 2);
        assert!(cfg.sources.azure.enabled);
        assert_eq!(cfg.sources.azure.services.len(), 2);
        assert!(cfg.sources.m365.enabled);
        assert_eq!(cfg.sources.m365.services.len(), 1);
        assert!(cfg.sources.gcp.enabled);
        assert_eq!(cfg.sources.gcp.services.len(), 2);
    }

    // =========================================================================
    // 3. Instance ID derivation
    // =========================================================================

    #[test]
    fn test_instance_id_explicit_lowercased() {
        let mut cfg = Config::default();
        cfg.instance_id = Some("My-Custom-ID".to_string());
        assert_eq!(derive_instance_id(&cfg), "my-custom-id");
    }

    #[test]
    fn test_instance_id_aws_enabled() {
        let mut cfg = Config::default();
        cfg.sources.aws.enabled = true;
        cfg.sources.aws.region = "us-west-2".to_string();
        cfg.sources.aws.access_key_id = Some("AKIATEST".to_string());
        let id = derive_instance_id(&cfg);
        assert!(id.starts_with("aws-"), "Expected aws- prefix, got: {id}");
        assert_eq!(id.len(), "aws-".len() + 8); // 4 bytes = 8 hex chars
    }

    #[test]
    fn test_instance_id_azure_enabled() {
        let mut cfg = Config::default();
        cfg.sources.azure.enabled = true;
        cfg.sources.azure.tenant_id = Some("tenant-abc".to_string());
        cfg.sources.azure.subscription_id = Some("sub-123".to_string());
        let id = derive_instance_id(&cfg);
        assert!(
            id.starts_with("azure-"),
            "Expected azure- prefix, got: {id}"
        );
        assert_eq!(id.len(), "azure-".len() + 8);
    }

    #[test]
    fn test_instance_id_m365_enabled() {
        let mut cfg = Config::default();
        cfg.sources.m365.enabled = true;
        cfg.sources.m365.tenant_id = Some("m365-tenant".to_string());
        let id = derive_instance_id(&cfg);
        assert!(id.starts_with("m365-"), "Expected m365- prefix, got: {id}");
        assert_eq!(id.len(), "m365-".len() + 8);
    }

    #[test]
    fn test_instance_id_gcp_enabled() {
        let mut cfg = Config::default();
        cfg.sources.gcp.enabled = true;
        cfg.sources.gcp.project_id = Some("gcp-project-42".to_string());
        let id = derive_instance_id(&cfg);
        assert!(id.starts_with("gcp-"), "Expected gcp- prefix, got: {id}");
        assert_eq!(id.len(), "gcp-".len() + 8);
    }

    #[test]
    fn test_instance_id_no_sources_enabled() {
        let cfg = Config::default();
        assert_eq!(derive_instance_id(&cfg), "dfe-fetcher");
    }

    #[test]
    fn test_instance_id_priority_aws_wins_over_azure() {
        let mut cfg = Config::default();
        cfg.sources.aws.enabled = true;
        cfg.sources.azure.enabled = true;
        cfg.sources.m365.enabled = true;
        cfg.sources.gcp.enabled = true;
        let id = derive_instance_id(&cfg);
        assert!(id.starts_with("aws-"), "AWS should win priority, got: {id}");
    }

    #[test]
    fn test_instance_id_priority_azure_when_aws_disabled() {
        let mut cfg = Config::default();
        cfg.sources.azure.enabled = true;
        cfg.sources.m365.enabled = true;
        cfg.sources.gcp.enabled = true;
        let id = derive_instance_id(&cfg);
        assert!(
            id.starts_with("azure-"),
            "Azure should win when AWS disabled, got: {id}"
        );
    }

    #[test]
    fn test_instance_id_deterministic() {
        let mut cfg = Config::default();
        cfg.sources.aws.enabled = true;
        cfg.sources.aws.region = "eu-west-1".to_string();
        cfg.sources.aws.access_key_id = Some("AKIAEXAMPLE".to_string());
        let id1 = derive_instance_id(&cfg);
        let id2 = derive_instance_id(&cfg);
        assert_eq!(id1, id2, "Same config must produce same instance ID");
    }

    /// One generic source, the standalone shape: no `instance_id` and none
    /// derivable, and it still loads.
    const ONE_GENERIC_SOURCE: &str = r#"
kafka:
  brokers: ["localhost:9092"]
sources:
  file:
    exports:
      topic: exports
      units:
        - unit: assets
          dump: { paths: ["/data/exports/assets-*.jsonl"] }
"#;

    /// The same config with a second generic source, which is the composed
    /// deployment the derivation never covered.
    const TWO_GENERIC_SOURCES: &str = r#"
kafka:
  brokers: ["localhost:9092"]
sources:
  file:
    exports:
      topic: exports
      units:
        - unit: assets
          dump: { paths: ["/data/exports/assets-*.jsonl"] }
    inventory:
      topic: inventory
      units:
        - unit: hosts
          dump: { paths: ["/data/inventory/hosts-*.jsonl"] }
"#;

    fn from_yaml(yaml: &str) -> Config {
        serde_yaml_ng::from_str(yaml).expect("the config parses")
    }

    #[test]
    fn one_generic_source_without_an_instance_id_still_loads() {
        let cfg = from_yaml(ONE_GENERIC_SOURCE);
        assert!(cfg.instance_id.is_none());
        assert_eq!(derive_instance_id(&cfg), FALLBACK_INSTANCE_ID);
        cfg.validate().expect("the standalone shape is not refused");
    }

    #[test]
    fn two_generic_sources_without_an_instance_id_are_refused_naming_the_field() {
        let cfg = from_yaml(TWO_GENERIC_SOURCES);
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("instance_id"), "names the fix: {err}");
        assert!(
            err.contains("exports") && err.contains("inventory"),
            "names the sources it counted: {err}"
        );
    }

    #[test]
    fn two_generic_sources_with_an_instance_id_load() {
        let mut cfg = from_yaml(TWO_GENERIC_SOURCES);
        cfg.instance_id = Some("exports-pod".to_string());
        cfg.validate().expect("an explicit id is the fix");
        assert_eq!(derive_instance_id(&cfg), "exports-pod");
    }

    /// A typed block the derivation reads produces a distinct id, so the
    /// refusal never fires however many generic sources run beside it.
    #[test]
    fn a_derived_typed_block_id_is_not_refused_alongside_generic_sources() {
        let mut cfg = from_yaml(TWO_GENERIC_SOURCES);
        cfg.sources.aws.enabled = true;
        cfg.sources.aws.credential_secret = Some("vault:kv/data/aws:credentials".to_string());
        cfg.sources.aws.services = vec![AwsService {
            name: "cloudtrail".to_string(),
            config: HashMap::new(),
        }];
        assert!(cfg.instance_id.is_none());
        assert!(derive_instance_id(&cfg).starts_with("aws-"));
        cfg.validate().expect("a derived id distinguishes the pod");
    }

    #[test]
    fn test_instance_id_different_config_different_hash() {
        let mut cfg1 = Config::default();
        cfg1.sources.aws.enabled = true;
        cfg1.sources.aws.region = "us-east-1".to_string();
        cfg1.sources.aws.access_key_id = Some("AKIAONE".to_string());

        let mut cfg2 = Config::default();
        cfg2.sources.aws.enabled = true;
        cfg2.sources.aws.region = "eu-west-1".to_string();
        cfg2.sources.aws.access_key_id = Some("AKIATWO".to_string());

        let id1 = derive_instance_id(&cfg1);
        let id2 = derive_instance_id(&cfg2);
        assert_ne!(id1, id2, "Different configs must produce different IDs");
    }

    // =========================================================================
    // 4. Env var overrides (SASL, TLS, config_reload, DLQ, pressure)
    // =========================================================================

    #[test]
    fn test_env_override_sasl_mechanism_username_password() {
        with_env(
            &[
                ("DFE_FETCHER_KAFKA_SASL_MECHANISM", "scram_sha_256"),
                ("DFE_FETCHER_KAFKA_SASL_USER", "admin"),
                ("DFE_FETCHER_KAFKA_SASL_PASSWORD", "s3cret"),
            ],
            || {
                let mut cfg = Config::default();
                cfg.apply_flat_env("DFE_FETCHER");
                let sasl = cfg.kafka.sasl.as_ref().expect("SASL should be set");
                assert!(sasl.enabled);
                assert_eq!(sasl.mechanism, "scram_sha_256");
                assert_eq!(sasl.username, "admin");
                assert_eq!(sasl.password.expose(), "s3cret");
            },
        );
    }

    #[test]
    fn test_env_override_security_protocol_ssl_enables_tls() {
        with_env(
            &[("DFE_FETCHER_KAFKA_SECURITY_PROTOCOL", "SASL_SSL")],
            || {
                let mut cfg = Config::default();
                assert!(!cfg.kafka.tls.enabled);
                cfg.apply_flat_env("DFE_FETCHER");
                assert!(
                    cfg.kafka.tls.enabled,
                    "TLS should be enabled when protocol contains SSL"
                );
            },
        );
    }

    #[test]
    fn test_env_override_security_protocol_plaintext_no_tls() {
        with_env(
            &[("DFE_FETCHER_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")],
            || {
                let mut cfg = Config::default();
                cfg.apply_flat_env("DFE_FETCHER");
                assert!(
                    !cfg.kafka.tls.enabled,
                    "TLS should remain disabled for PLAINTEXT"
                );
            },
        );
    }

    #[test]
    fn test_env_override_config_reload_secs() {
        with_env(&[("DFE_FETCHER_CONFIG_RELOAD_SECS", "30")], || {
            let mut cfg = Config::default();
            assert_eq!(cfg.config_reload_secs, 0);
            cfg.apply_flat_env("DFE_FETCHER");
            assert_eq!(cfg.config_reload_secs, 30);
        });
    }

    #[test]
    fn test_env_override_dlq_enabled_and_path() {
        with_env(
            &[
                ("DFE_FETCHER_DLQ_ENABLED", "true"),
                ("DFE_FETCHER_DLQ_PATH", "/data/dlq"),
            ],
            || {
                let mut cfg = Config::default();
                cfg.apply_flat_env("DFE_FETCHER");
                assert!(cfg.dlq.enabled);
                assert_eq!(cfg.dlq.file.path.to_str().unwrap(), "/data/dlq");
            },
        );
    }

    #[test]
    fn test_default_dlq_routes_common_to_standard_topic() {
        let cfg = Config::default();
        assert_eq!(cfg.dlq.kafka.routing, scalo::dlq::DlqRouting::Common);
        assert_eq!(cfg.dlq.kafka.common_topic, "dfe_fetcher_dlq");
    }

    #[test]
    fn test_env_override_dlq_unknown_mode_falls_back_to_cascade() {
        with_env(&[("DFE_FETCHER_DLQ_MODE", "kafka-only")], || {
            let mut cfg = Config::default();
            cfg.apply_flat_env("DFE_FETCHER");
            assert_eq!(cfg.dlq.mode, scalo::dlq::DlqMode::Cascade);
        });
    }

    #[test]
    fn test_env_override_dlq_topic_and_mode() {
        with_env(
            &[
                ("DFE_FETCHER_DLQ_TOPIC", "dfe_fetcher_dlq"),
                ("DFE_FETCHER_DLQ_MODE", "kafka_only"),
            ],
            || {
                let mut cfg = Config::default();
                cfg.apply_flat_env("DFE_FETCHER");
                assert_eq!(cfg.dlq.kafka.common_topic, "dfe_fetcher_dlq");
                assert_eq!(cfg.dlq.kafka.routing, scalo::dlq::DlqRouting::Common);
                assert_eq!(cfg.dlq.mode, scalo::dlq::DlqMode::KafkaOnly);
            },
        );
    }

    #[test]
    fn test_env_override_pressure_threshold() {
        with_env(&[("DFE_FETCHER_PRESSURE_THRESHOLD", "0.65")], || {
            let mut cfg = Config::default();
            cfg.apply_flat_env("DFE_FETCHER");
            assert!(
                (cfg.buffer.pressure_threshold - 0.65).abs() < f64::EPSILON,
                "Expected 0.65, got: {}",
                cfg.buffer.pressure_threshold
            );
        });
    }

    #[test]
    fn test_env_override_multiple_vars_simultaneously() {
        with_env(
            &[
                ("DFE_FETCHER_KAFKA_BROKERS", "b1:9092,b2:9092"),
                ("DFE_FETCHER_DEFAULT_INTERVAL_SECS", "45"),
                ("DFE_FETCHER_METRICS_ADDRESS", "127.0.0.1:9999"),
                ("DFE_FETCHER_CONFIG_RELOAD_SECS", "15"),
                ("DFE_FETCHER_TOPIC_SUFFIX", "_ingest"),
            ],
            || {
                let mut cfg = Config::default();
                cfg.apply_flat_env("DFE_FETCHER");
                assert_eq!(cfg.kafka.brokers, vec!["b1:9092", "b2:9092"]);
                assert_eq!(cfg.scheduler.default_interval_secs, 45);
                assert_eq!(cfg.metrics.address, "127.0.0.1:9999");
                assert_eq!(cfg.config_reload_secs, 15);
                assert_eq!(cfg.kafka.topic_suffix, "_ingest");
            },
        );
    }

    // =========================================================================
    // 5. OutputConfig methods
    // =========================================================================

    #[test]
    fn test_output_config_includes_kafka() {
        let mut oc = OutputConfig::default();
        oc.output_type = "kafka".to_string();
        assert!(oc.includes_kafka());
        assert!(!oc.includes_grpc());

        oc.output_type = "grpc".to_string();
        assert!(!oc.includes_kafka());
        assert!(oc.includes_grpc());

        oc.output_type = "both".to_string();
        assert!(oc.includes_kafka());
        assert!(oc.includes_grpc());
    }

    #[test]
    fn test_output_config_unknown_type_includes_neither() {
        let mut oc = OutputConfig::default();
        oc.output_type = "file".to_string();
        assert!(!oc.includes_kafka());
        assert!(!oc.includes_grpc());
    }

    // =========================================================================
    // 6. PluginsConfig.warn_if_configured()
    // =========================================================================

    #[test]
    fn test_plugins_warn_empty_no_panic() {
        let plugins = PluginsConfig::default();
        // Should not panic when no directory or extra fields set
        plugins.warn_if_configured();
    }

    #[test]
    fn test_plugins_warn_with_directory_no_panic() {
        let plugins = PluginsConfig {
            directory: Some("/old/plugins".to_string()),
            _rest: serde_json::Map::new(),
        };
        // Should log a warning but not panic
        plugins.warn_if_configured();
    }

    #[test]
    fn test_plugins_warn_with_extra_fields_no_panic() {
        let mut rest = serde_json::Map::new();
        rest.insert("extra_field".to_string(), serde_json::Value::Bool(true));
        let plugins = PluginsConfig {
            directory: None,
            _rest: rest,
        };
        plugins.warn_if_configured();
    }

    // =========================================================================
    // 7. Config serialization roundtrip
    // =========================================================================

    #[test]
    fn test_config_yaml_roundtrip() {
        let mut cfg = valid_config();
        cfg.scheduler.default_interval_secs = 180;
        cfg.scheduler.jitter_percent = 15;
        cfg.scheduler.max_concurrent_fetches = 8;
        cfg.kafka.client_id = "roundtrip-test".to_string();
        cfg.kafka.topic_suffix = "_test".to_string();
        cfg.buffer.pressure_threshold = 0.75;
        cfg.sources.aws.enabled = true;
        cfg.sources.aws.region = "ap-southeast-2".to_string();
        cfg.cursor.default_window_hours = 4;
        cfg.config_reload_secs = 60;

        let yaml = serde_yaml_ng::to_string(&cfg).expect("serialize to YAML");
        let restored: Config = serde_yaml_ng::from_str(&yaml).expect("deserialize from YAML");

        assert_eq!(
            restored.scheduler.default_interval_secs,
            cfg.scheduler.default_interval_secs
        );
        assert_eq!(
            restored.scheduler.jitter_percent,
            cfg.scheduler.jitter_percent
        );
        assert_eq!(
            restored.scheduler.max_concurrent_fetches,
            cfg.scheduler.max_concurrent_fetches
        );
        assert_eq!(restored.kafka.client_id, cfg.kafka.client_id);
        assert_eq!(restored.kafka.topic_suffix, cfg.kafka.topic_suffix);
        assert_eq!(restored.kafka.brokers, cfg.kafka.brokers);
        assert!(
            (restored.buffer.pressure_threshold - cfg.buffer.pressure_threshold).abs()
                < f64::EPSILON
        );
        assert_eq!(restored.sources.aws.enabled, cfg.sources.aws.enabled);
        assert_eq!(restored.sources.aws.region, cfg.sources.aws.region);
        assert_eq!(
            restored.cursor.default_window_hours,
            cfg.cursor.default_window_hours
        );
        assert_eq!(restored.config_reload_secs, cfg.config_reload_secs);
    }

    #[test]
    fn test_config_roundtrip_preserves_output_type() {
        let mut cfg = valid_config();
        cfg.output.output_type = "grpc".to_string();
        cfg.output.grpc = Some(scalo::transport::GrpcConfig {
            endpoint: Some("http://receiver:6000".to_string()),
            ..Default::default()
        });

        let yaml = serde_yaml_ng::to_string(&cfg).expect("serialize");
        let restored: Config = serde_yaml_ng::from_str(&yaml).expect("deserialize");
        assert_eq!(restored.output.output_type, "grpc");
        assert!(restored.output.includes_grpc());
        assert!(!restored.output.includes_kafka());
    }

    // =========================================================================
    // Multi-endpoint (GA 2.2) connection model
    // =========================================================================

    #[test]
    fn test_resolved_no_connections_is_single_implicit() {
        let mut cfg = AwsSourceConfig::default();
        cfg.region = "sa-east-1".to_string();
        cfg.interval_secs = Some(120);

        let resolved = cfg.resolved("aws");
        assert_eq!(
            resolved.len(),
            1,
            "no connections => one implicit connection"
        );
        assert_eq!(
            resolved[0].id, "aws",
            "implicit id defaults to the type name"
        );
        assert_eq!(resolved[0].config.region, "sa-east-1");
        assert_eq!(resolved[0].interval_secs, Some(120));
        assert!(resolved[0].config.connections.is_empty());
    }

    #[test]
    fn test_resolved_multi_overlays_connection_fields() {
        let mut cfg = AwsSourceConfig::default();
        cfg.enabled = true;
        cfg.region = "us-east-1".to_string();
        cfg.topic = "aws".to_string();
        cfg.services = vec![AwsService {
            name: "cloudtrail".to_string(),
            config: HashMap::new(),
        }];
        cfg.connections = vec![
            AwsConnection {
                id: "acct-a".to_string(),
                region: Some("eu-west-1".to_string()),
                credential_secret: Some("vault:kv/data/a:creds".to_string()),
                ..Default::default()
            },
            AwsConnection {
                id: "acct-b".to_string(),
                region: Some("ap-southeast-2".to_string()),
                interval_secs: Some(60),
                ..Default::default()
            },
        ];

        let resolved = cfg.resolved("aws");
        assert_eq!(resolved.len(), 2);

        // Connection A: id + per-connection region/credential overlaid.
        assert_eq!(resolved[0].id, "acct-a");
        assert_eq!(resolved[0].config.region, "eu-west-1");
        assert_eq!(
            resolved[0].config.credential_secret.as_deref(),
            Some("vault:kv/data/a:creds")
        );
        // Shared type-level fields are preserved on every connection.
        assert_eq!(resolved[0].config.topic, "aws");
        assert_eq!(resolved[0].config.services.len(), 1);
        // The runtime config never re-reads the connections list.
        assert!(resolved[0].config.connections.is_empty());

        // Connection B: region overlaid, credential inherits (None), interval set.
        assert_eq!(resolved[1].id, "acct-b");
        assert_eq!(resolved[1].config.region, "ap-southeast-2");
        assert_eq!(resolved[1].config.credential_secret, None);
        assert_eq!(resolved[1].interval_secs, Some(60));
    }

    #[test]
    fn test_resolved_interval_precedence() {
        let mut cfg = AwsSourceConfig::default();
        cfg.interval_secs = Some(300);
        cfg.connections = vec![
            AwsConnection {
                id: "a".to_string(),
                interval_secs: Some(60),
                ..Default::default()
            },
            AwsConnection {
                id: "b".to_string(),
                interval_secs: None,
                ..Default::default()
            },
        ];

        let resolved = cfg.resolved("aws");
        assert_eq!(resolved[0].interval_secs, Some(60), "per-connection wins");
        assert_eq!(
            resolved[1].interval_secs,
            Some(300),
            "unset connection inherits the type-level interval"
        );
    }

    #[test]
    fn test_resolved_okta_use_ssws_bool_override() {
        let mut cfg = OktaSourceConfig::default(); // use_ssws_header default true
        cfg.connections = vec![
            OktaConnection {
                id: "t1".to_string(),
                tenant_url: Some("https://a.okta.com".to_string()),
                use_ssws_header: Some(false),
                ..Default::default()
            },
            OktaConnection {
                id: "t2".to_string(),
                ..Default::default()
            },
        ];

        let resolved = cfg.resolved("okta");
        assert!(
            !resolved[0].config.use_ssws_header,
            "explicit false overlaid"
        );
        assert_eq!(
            resolved[0].config.tenant_url.as_deref(),
            Some("https://a.okta.com")
        );
        assert!(
            resolved[1].config.use_ssws_header,
            "unset connection inherits the type-level default (true)"
        );
    }

    #[test]
    fn test_validate_rejects_empty_connection_id() {
        let mut cfg = valid_config();
        cfg.sources.aws.enabled = true;
        cfg.sources.aws.connections = vec![AwsConnection {
            id: String::new(),
            ..Default::default()
        }];
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("non-empty 'id'"), "{err}");
    }

    #[test]
    fn test_validate_rejects_duplicate_connection_id() {
        let mut cfg = valid_config();
        cfg.sources.aws.enabled = true;
        cfg.sources.aws.connections = vec![
            AwsConnection {
                id: "dup".to_string(),
                ..Default::default()
            },
            AwsConnection {
                id: "dup".to_string(),
                ..Default::default()
            },
        ];
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("duplicate connection id"), "{err}");
    }

    #[test]
    fn test_validate_accepts_unique_connection_ids() {
        let mut cfg = valid_config();
        cfg.sources.aws.enabled = true;
        cfg.sources.aws.connections = vec![
            AwsConnection {
                id: "acct-a".to_string(),
                credential_secret: Some("vault:kv/data/aws-a:credentials".to_string()),
                ..Default::default()
            },
            AwsConnection {
                id: "acct-b".to_string(),
                credential_secret: Some("vault:kv/data/aws-b:credentials".to_string()),
                ..Default::default()
            },
        ];
        assert!(cfg.validate().is_ok(), "{:?}", cfg.validate());
    }

    #[test]
    fn test_validate_ignores_disabled_type_connection_ids() {
        let mut cfg = valid_config();
        cfg.sources.okta.enabled = false;
        cfg.sources.okta.connections = vec![
            OktaConnection {
                id: "dup".to_string(),
                ..Default::default()
            },
            OktaConnection {
                id: "dup".to_string(),
                ..Default::default()
            },
        ];
        assert!(
            cfg.validate().is_ok(),
            "a disabled type never spawns, so its connection ids are not validated"
        );
    }

    #[test]
    fn test_connections_yaml_roundtrip() {
        let yaml = r#"
sources:
  aws:
    enabled: true
    topic: aws
    services:
      - name: cloudtrail
    connections:
      - id: acct-123
        region: us-east-1
        credential_secret: "vault:kv/data/aws-123:creds"
      - id: acct-456
        region: eu-west-1
        credential_secret: "vault:kv/data/aws-456:creds"
"#;
        let cfg: Config = serde_yaml_ng::from_str(yaml).expect("parse connections");
        assert_eq!(cfg.sources.aws.connections.len(), 2);
        assert_eq!(cfg.sources.aws.connections[0].id, "acct-123");
        assert_eq!(
            cfg.sources.aws.connections[0].region.as_deref(),
            Some("us-east-1")
        );

        let resolved = cfg.sources.aws.resolved("aws");
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[1].config.region, "eu-west-1");
        // All connections of a group share the one type-level topic (C2).
        assert_eq!(resolved[0].config.topic, "aws");
        assert_eq!(resolved[1].config.topic, "aws");
    }
}
