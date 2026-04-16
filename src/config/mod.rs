// Project:   dfe-fetcher
// File:      src/config/mod.rs
// Purpose:   Configuration loading and validation
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration management using hyperi-rustlib's 7-layer cascade.
//!
//! Priority (highest to lowest):
//! 1. CLI arguments
//! 2. Environment variables (DFE_FETCHER_*)
//! 3. .env file
//! 4. settings.{env}.yaml
//! 5. settings.yaml
//! 6. defaults.yaml
//! 7. Hard-coded defaults

mod shared;

pub use shared::SharedConfig;

use std::collections::HashMap;

use hyperi_rustlib::config::flat_env::{self, ApplyFlatEnv};
use hyperi_rustlib::config::sensitive::SensitiveString;
use hyperi_rustlib::config::{self, ConfigOptions};
use hyperi_rustlib::dlq::DlqConfig;
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
/// - `scheduler.default_interval_secs` — fetch interval re-computed each cycle
/// - `scheduler.jitter_percent` — jitter re-computed each cycle
/// - `kafka.topic_suffix` — topic name suffix re-read on each delivery
/// - `sources.*.filter` — CEL filter re-evaluated on each record
/// - `cursor.default_window_hours` — lookback window re-read when no cursor exists
///
/// **Requires pod restart:**
/// - `output.*` — transport connections established at startup
/// - `kafka.*` (except `topic_suffix`) — transport config bound at startup
/// - `ingest.*` — HTTP server binds at startup, auth token resolved once
/// - `extractors.*` — containers and Vector instances spawned at startup
/// - `metrics.*` — metrics server binds at startup
/// - `instance_id` — cursor key prefix set at startup
/// - `cursor.directory` — cursor store created at startup
/// - `sources.*.enabled` — source registration at startup
/// - `sources.*.credential_secret` / `tenant_id` / `client_id` etc. — credentials resolved once
/// - `scheduler.max_concurrent_fetches` — semaphore created at startup
/// - `dlq.*` — DLQ created at startup
/// - `buffer.*` — buffer manager created at startup
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub scaling: hyperi_rustlib::scaling::ScalingPressureConfig,

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

    /// Path to the config file (set by loader, not deserialized).
    #[serde(skip)]
    pub config_path: Option<String>,
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
            dlq: DlqConfig::default(),
            config_reload_secs: 0,
            instance_id: None,
            output: OutputConfig::default(),
            cursor: CursorConfig::default(),
            scaling: hyperi_rustlib::scaling::ScalingPressureConfig::default(),
            unwrap_nested_json: default_unwrap_nested_json(),
            config_path: None,
        }
    }
}

impl Config {
    /// Load configuration with cascade: CLI -> ENV -> .env -> file -> defaults
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        // If an explicit config file is provided, load it directly
        if let Some(path) = config_path {
            return Self::load_from_file(path);
        }

        // Otherwise, use hyperi-rustlib's 7-layer cascade
        config::setup(ConfigOptions {
            env_prefix: ENV_PREFIX.to_string(),
            config_paths: Vec::new(),
            load_dotenv: true,
            ..Default::default()
        })
        .map_err(|e| Error::Config(format!("failed to setup config: {e}")))?;

        // Get the global config and unmarshal to our struct
        let cfg = config::get();

        // Try to unmarshal the full config, falling back to defaults
        let mut config: Config = cfg.unmarshal().unwrap_or_default();

        // Store config path for reload support
        config.config_path = config_path.map(String::from);

        // Apply flat env var overrides (DFE_FETCHER_*)
        config.apply_flat_env("DFE_FETCHER");

        Ok(config)
    }

    /// Load configuration from a YAML file directly.
    pub fn load_from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("failed to read config file: {e}")))?;

        let mut config: Config = serde_yaml_ng::from_str(&content)?;
        config.config_path = Some(path.to_string());
        config.apply_flat_env("DFE_FETCHER");
        Ok(config)
    }

    /// Register all config sections in the global config registry.
    /// Enables the /config debug endpoint and change notifications.
    pub fn register_in_registry(&self) {
        use hyperi_rustlib::config::registry;
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
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        // At least one source must be enabled
        if !self.sources.aws.enabled
            && !self.sources.azure.enabled
            && !self.sources.m365.enabled
            && !self.sources.gcp.enabled
        {
            // Not an error — just a warning scenario (no sources to fetch)
            // Allow startup with no sources for config validation
        }

        // Validate output transport config
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

        // Validate source filter expressions (CEL)
        if let Some(ref filter) = self.sources.aws.filter {
            let errors = hyperi_rustlib::expression::validate(filter);
            if !errors.is_empty() {
                return Err(Error::Config(format!(
                    "sources.aws.filter invalid: {}",
                    errors.join(", ")
                )));
            }
        }
        if let Some(ref filter) = self.sources.azure.filter {
            let errors = hyperi_rustlib::expression::validate(filter);
            if !errors.is_empty() {
                return Err(Error::Config(format!(
                    "sources.azure.filter invalid: {}",
                    errors.join(", ")
                )));
            }
        }
        if let Some(ref filter) = self.sources.m365.filter {
            let errors = hyperi_rustlib::expression::validate(filter);
            if !errors.is_empty() {
                return Err(Error::Config(format!(
                    "sources.m365.filter invalid: {}",
                    errors.join(", ")
                )));
            }
        }
        if let Some(ref filter) = self.sources.gcp.filter {
            let errors = hyperi_rustlib::expression::validate(filter);
            if !errors.is_empty() {
                return Err(Error::Config(format!(
                    "sources.gcp.filter invalid: {}",
                    errors.join(", ")
                )));
            }
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

        Ok(())
    }
}

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

    "dfe-fetcher".to_string()
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
            let sasl = self.kafka.sasl.get_or_insert_with(|| SaslConfig {
                enabled: true,
                mechanism: String::new(),
                username: String::new(),
                password: SensitiveString::default(),
            });
            sasl.mechanism = v;
            sasl.enabled = true;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_SECURITY_PROTOCOL") {
            self.kafka.tls.enabled = v.to_uppercase().contains("SSL");
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "KAFKA_SASL_USER") {
            let sasl = self.kafka.sasl.get_or_insert_with(|| SaslConfig {
                enabled: true,
                mechanism: String::new(),
                username: String::new(),
                password: SensitiveString::default(),
            });
            sasl.username = v;
        }
        if let Some(v) = flat_env::flat_env_string_sensitive(prefix, "KAFKA_SASL_PASSWORD") {
            let sasl = self.kafka.sasl.get_or_insert_with(|| SaslConfig {
                enabled: true,
                mechanism: String::new(),
                username: String::new(),
                password: SensitiveString::default(),
            });
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

        // DLQ
        if let Some(v) = flat_env::flat_env_bool(prefix, "DLQ_ENABLED") {
            self.dlq.enabled = v;
        }
        if let Some(v) = flat_env::flat_env_string(prefix, "DLQ_PATH") {
            self.dlq.file.path = v.into();
        }
    }
}

// =============================================================================
// Scheduler configuration
// =============================================================================

/// Scheduler configuration for fetch timing.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

impl Default for SourcesConfig {
    fn default() -> Self {
        Self {
            aws: AwsSourceConfig::default(),
            azure: AzureSourceConfig::default(),
            m365: M365SourceConfig::default(),
            gcp: GcpSourceConfig::default(),
        }
    }
}

/// AWS source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Format: "provider:path:key" (e.g., "vault:secret/aws:credentials")
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
        }
    }
}

/// AWS service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsService {
    /// Service name (e.g., "cloudtrail", "guardduty", "securityhub", "config").
    pub name: String,

    /// Service-specific configuration.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Azure source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        }
    }
}

/// Azure service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureService {
    /// Service name (e.g., "activity_log", "defender", "sentinel", "entra_id").
    pub name: String,

    /// Service-specific configuration.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Microsoft 365 source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// CEL filter expression applied to fetched records.
    #[serde(default)]
    pub filter: Option<String>,
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
            filter: None,
        }
    }
}

/// M365 service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct M365Service {
    /// Service name (e.g., "audit_log", "message_trace", "dlp", "alerts").
    pub name: String,

    /// Service-specific configuration.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Google Cloud Platform source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        }
    }
}

/// GCP service to fetch data from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcpService {
    /// Service name (e.g., "audit_logs", "scc", "cloud_logging").
    pub name: String,

    /// Service-specific configuration.
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

// =============================================================================
// Extractors configuration (containers, vector)
// =============================================================================

/// External extractors configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
/// One container per source + config — no horizontal scaling needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Deprecated plugin configuration — logs warning if non-empty values present.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
            tracing::warn!("extractors.plugins is deprecated — use container extractors instead");
        }
    }
}

/// Vector.dev extractor configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct IngestConfig {
    /// Enable ingest HTTP endpoint.
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
            enabled: true,
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
/// **Deprecated:** Use `output.kafka` (rustlib `KafkaConfig`) instead.
/// This struct is kept for backward compatibility with existing config files
/// that use the top-level `kafka:` section. Will be removed in next major version.
///
/// Migration: move your `kafka:` settings under `output.kafka:` using rustlib
/// `KafkaConfig` format (profiles, `librdkafka_overrides`, standard field names).
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaslConfig {
    /// Enable SASL.
    pub enabled: bool,

    /// SASL mechanism (plain, scram_sha_256, scram_sha_512).
    pub mechanism: String,

    /// Username.
    pub username: String,

    /// Password (always redacted in serialisation/debug output).
    pub password: SensitiveString,
}

/// Kafka TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OutputConfig {
    /// Transport type: "kafka", "grpc", or "both".
    #[serde(rename = "type", default = "default_output_type")]
    pub output_type: String,

    /// Kafka transport configuration (rustlib KafkaConfig).
    #[serde(default)]
    pub kafka: Option<hyperi_rustlib::transport::KafkaConfig>,

    /// gRPC transport configuration (rustlib GrpcConfig, client mode).
    #[serde(default)]
    pub grpc: Option<hyperi_rustlib::transport::GrpcConfig>,

    /// Suffix appended to source topic names (e.g., "_land").
    /// If set, takes precedence over legacy `kafka.topic_suffix`.
    #[serde(default)]
    pub topic_suffix: Option<String>,
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
}

// =============================================================================
// Cursor store configuration
// =============================================================================

/// Cursor store configuration for incremental fetching.
///
/// Cursors are stored as individual JSON files in `directory`, one per
/// source service (e.g., `aws.cloudtrail.cursor.json`). The directory
/// should be PVC-backed for pod restart persistence.
///
/// If `directory` is empty, the cursor store falls back to the config
/// file's parent directory with a warning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CursorConfig {
    /// Directory for cursor files. Each source gets its own file
    /// named `{instance_id}.{source}.{service}.cursor.json`.
    /// Empty = fall back to config file directory (with warning).
    pub directory: String,

    /// Default lookback window in hours when no cursor exists.
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
    fn test_config_validation_no_brokers() {
        let config = Config::default();
        assert!(config.validate().is_err());
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

    #[test]
    fn test_validate_grpc_output_without_endpoint() {
        let mut cfg = valid_config();
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
        cfg.output.output_type = "both".to_string();
        // kafka brokers are set via valid_config(), but no grpc endpoint
        cfg.output.grpc = None;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("grpc.endpoint required"),
            "Expected grpc endpoint error, got: {err}"
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
        cfg.output.grpc = Some(hyperi_rustlib::transport::GrpcConfig {
            endpoint: Some("http://receiver:6000".to_string()),
            ..Default::default()
        });

        let yaml = serde_yaml_ng::to_string(&cfg).expect("serialize");
        let restored: Config = serde_yaml_ng::from_str(&yaml).expect("deserialize");
        assert_eq!(restored.output.output_type, "grpc");
        assert!(restored.output.includes_grpc());
        assert!(!restored.output.includes_kafka());
    }
}
