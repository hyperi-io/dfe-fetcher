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
/// - `cursor.store` / `cursor.file_path` / `cursor.kafka_topic` — cursor store created at startup
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

    /// Path to the config file (set by loader, not deserialized).
    #[serde(skip)]
    pub config_path: Option<String>,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CursorConfig {
    /// Store backend: "auto", "kafka", or "file".
    pub store: String,

    /// File-based cursor directory.
    pub file_path: String,

    /// Kafka topic for cursor state (compacted).
    pub kafka_topic: String,

    /// Default lookback window in hours when no cursor exists.
    pub default_window_hours: u64,
}

impl Default for CursorConfig {
    fn default() -> Self {
        Self {
            store: "auto".to_string(),
            file_path: "/var/lib/dfe-fetcher/cursors".to_string(),
            kafka_topic: "dfe-fetcher-cursors".to_string(),
            default_window_hours: 1,
        }
    }
}

#[cfg(test)]
#[allow(unsafe_code, clippy::unwrap_used, clippy::expect_used)]
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
}
