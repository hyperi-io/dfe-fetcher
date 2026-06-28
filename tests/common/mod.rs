// Project:   dfe-fetcher
// File:      tests/common/mod.rs
// Purpose:   Shared test infrastructure for dual-mode (remote/docker) testing
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared test helpers for dual-mode test infrastructure.
//!
//! Tests can run against either:
//! - **Remote** — devex cluster via `.env` (KAFKA_BROKERS, KAFKA_SASL_*, etc.)
//! - **Docker** — dfe-docker infra profile (localhost:19092, PLAINTEXT, no SASL)
//!
//! Set `TEST_MODE=docker` or `TEST_MODE=remote` in `.env` or environment.

#![allow(dead_code)] // Shared utilities — not all used by every test file

use std::env;

/// Test backend mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMode {
    Remote,
    Docker,
}

impl TestMode {
    pub fn detect() -> Self {
        load_dotenv();
        match env::var("TEST_MODE").unwrap_or_default().as_str() {
            "docker" => Self::Docker,
            _ => Self::Remote,
        }
    }
}

impl std::fmt::Display for TestMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Remote => write!(f, "remote"),
            Self::Docker => write!(f, "docker"),
        }
    }
}

/// Load .env file (won't override existing env vars).
pub fn load_dotenv() {
    let _ = dotenvy::dotenv();
}

// =============================================================================
// Kafka config
// =============================================================================

/// Kafka connection config for tests.
pub struct KafkaTestConfig {
    pub brokers: String,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_user: Option<String>,
    pub sasl_password: Option<String>,
}

impl KafkaTestConfig {
    pub fn has_sasl(&self) -> bool {
        self.sasl_mechanism.is_some() && self.sasl_user.is_some()
    }

    /// Check if broker is reachable via TCP (3s timeout).
    pub fn is_reachable(&self) -> bool {
        use std::net::ToSocketAddrs;
        let first = self.brokers.split(',').next().unwrap_or(&self.brokers);
        first
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next())
            .map(|a| {
                std::net::TcpStream::connect_timeout(&a, std::time::Duration::from_secs(3)).is_ok()
            })
            .unwrap_or(false)
    }

    /// Convert to rustlib KafkaConfig for use with KafkaTransport.
    pub fn to_rustlib_config(&self) -> scalo::transport::KafkaConfig {
        let mut config = scalo::transport::KafkaConfig {
            brokers: self
                .brokers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            security_protocol: self.security_protocol.to_lowercase(),
            client_id: "dfe-fetcher-test".to_string(),
            group: "dfe-fetcher-test".to_string(),
            ..Default::default()
        };
        if let Some(ref mechanism) = self.sasl_mechanism {
            config.sasl_mechanism = Some(mechanism.clone());
        }
        if let Some(ref user) = self.sasl_user {
            config.sasl_username = Some(user.clone());
        }
        if let Some(ref password) = self.sasl_password {
            config.sasl_password = Some(password.clone().into());
        }
        config
    }
}

/// Returns Kafka connection config for the active test mode.
///
/// - Docker mode: `localhost:19092`, PLAINTEXT, no SASL
/// - Remote mode: from env vars (`KAFKA_BROKERS`, `KAFKA_SASL_*`, etc.)
pub fn kafka_test_config() -> KafkaTestConfig {
    load_dotenv();
    match TestMode::detect() {
        TestMode::Docker => KafkaTestConfig {
            brokers: env::var("DOCKER_KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:19092".to_string()),
            security_protocol: "PLAINTEXT".to_string(),
            sasl_mechanism: None,
            sasl_user: None,
            sasl_password: None,
        },
        TestMode::Remote => KafkaTestConfig {
            brokers: env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string()),
            security_protocol: env::var("KAFKA_SECURITY_PROTOCOL")
                .unwrap_or_else(|_| "SASL_PLAINTEXT".to_string()),
            sasl_mechanism: env::var("KAFKA_SASL_MECHANISM").ok(),
            sasl_user: env::var("KAFKA_SASL_USER").ok(),
            sasl_password: env::var("KAFKA_SASL_PASSWORD").ok(),
        },
    }
}

/// Test topic name with unique suffix to avoid collisions.
pub fn test_topic(base: &str) -> String {
    let prefix = env::var("TEST_TOPIC_PREFIX").unwrap_or_else(|_| "dfe-fetcher-test".into());
    format!("{prefix}-{base}-{}", chrono::Utc::now().timestamp_millis())
}

// =============================================================================
// Deployment-contract-derived test config
// =============================================================================

/// Authoritative test settings derived from the dfe-fetcher deployment contract.
///
/// Tests that need to know the app's metrics port, ingest port, health paths,
/// or env prefix should pull values from here rather than hard-coding them so
/// that contract changes propagate automatically.
pub struct AppTestContract {
    pub app_name: String,
    pub env_prefix: String,
    pub metrics_port: u16,
    pub liveness_path: String,
    pub readiness_path: String,
    pub metrics_path: String,
    pub ingest_port: Option<u16>,
    pub vector_grpc_port: Option<u16>,
    pub config_mount_path: String,
}

impl AppTestContract {
    /// Build from the live `deployment::contract()` so tests stay in sync.
    pub fn from_app() -> Self {
        let c = dfe_fetcher::deployment::contract();
        let ingest_port = c
            .extra_ports
            .iter()
            .find(|p| p.name == "ingest")
            .map(|p| p.port);
        let vector_grpc_port = c
            .extra_ports
            .iter()
            .find(|p| p.name == "vector-grpc")
            .map(|p| p.port);
        Self {
            app_name: c.app_name,
            env_prefix: c.env_prefix,
            metrics_port: c.metrics_port,
            liveness_path: c.health.liveness_path,
            readiness_path: c.health.readiness_path,
            metrics_path: c.health.metrics_path,
            ingest_port,
            vector_grpc_port,
            config_mount_path: c.config_mount_path,
        }
    }
}

// =============================================================================
// OpenBao / Vault config (live → testcontainers fallback)
// =============================================================================

/// OpenBao/Vault connection config for tests.
///
/// Wraps an optional testcontainer that's auto-stopped when this struct is
/// dropped (Drop impl for [`testcontainers::ContainerAsync`]).
pub struct VaultTestConfig {
    pub address: String,
    pub token: String,
    pub mount_path: String,
    /// Holds the testcontainer alive for the test's lifetime.
    /// `None` when a live external Vault is being used.
    _container: Option<TestcontainerHolder>,
}

/// Holder for any auto-managed testcontainer. Drop stops the container.
pub enum TestcontainerHolder {
    GenericVault(testcontainers::ContainerAsync<testcontainers::GenericImage>),
    LocalStack(testcontainers::ContainerAsync<testcontainers_modules::localstack::LocalStack>),
    Kafka(testcontainers::ContainerAsync<testcontainers_modules::kafka::apache::Kafka>),
}

impl VaultTestConfig {
    /// Acquire a Vault test config: live (env) if available, else start a
    /// throwaway OpenBao container that is auto-stopped on `Drop`.
    ///
    /// Returns `None` if no live Vault is configured AND Docker is unavailable.
    pub async fn acquire() -> Option<Self> {
        load_dotenv();

        // Live mode: env vars present → use them, no container managed
        if let Ok(address) = env::var("VAULT_ADDR").or_else(|_| env::var("BAO_ADDR"))
            && let Ok(token) = env::var("VAULT_TOKEN").or_else(|_| env::var("BAO_TOKEN"))
        {
            let mount_path = env::var("VAULT_KV_MOUNT").unwrap_or_else(|_| "secret".to_string());
            return Some(Self {
                address,
                token,
                mount_path,
                _container: None,
            });
        }

        // Fallback: start an OpenBao testcontainer
        use testcontainers::core::{IntoContainerPort, WaitFor};
        use testcontainers::runners::AsyncRunner;
        use testcontainers::{GenericImage, ImageExt};

        let image = GenericImage::new("openbao/openbao", "latest")
            .with_exposed_port(8200u16.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Vault server started"))
            .with_env_var("VAULT_DEV_ROOT_TOKEN_ID", "root")
            .with_env_var("VAULT_DEV_LISTEN_ADDRESS", "0.0.0.0:8200")
            .with_cmd(["server", "-dev"]);

        let container = image.start().await.ok()?;
        let host = container.get_host().await.ok()?;
        let port = container.get_host_port_ipv4(8200u16).await.ok()?;
        let address = format!("http://{host}:{port}");

        Some(Self {
            address,
            token: "root".to_string(),
            mount_path: "secret".to_string(),
            _container: Some(TestcontainerHolder::GenericVault(container)),
        })
    }
}

// =============================================================================
// LocalStack (AWS emulator) config
// =============================================================================

/// LocalStack endpoint config for AWS source tests.
///
/// Wraps an optional testcontainer that's auto-stopped on `Drop`.
pub struct LocalStackConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    _container: Option<TestcontainerHolder>,
}

impl LocalStackConfig {
    /// Acquire a LocalStack endpoint: live first, else start a testcontainer.
    /// Returns `None` if neither is available.
    pub async fn acquire() -> Option<Self> {
        load_dotenv();

        // Live mode
        if let Ok(endpoint) = env::var("LOCALSTACK_ENDPOINT") {
            return Some(Self {
                endpoint,
                region: env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
                access_key_id: env::var("LOCALSTACK_AWS_ACCESS_KEY_ID")
                    .unwrap_or_else(|_| "test".to_string()),
                secret_access_key: env::var("LOCALSTACK_AWS_SECRET_ACCESS_KEY")
                    .unwrap_or_else(|_| "test".to_string()),
                _container: None,
            });
        }

        // Fallback: testcontainer
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::localstack::LocalStack;

        let container = LocalStack::default().start().await.ok()?;
        let host = container.get_host().await.ok()?;
        let port = container.get_host_port_ipv4(4566).await.ok()?;
        let endpoint = format!("http://{host}:{port}");

        Some(Self {
            endpoint,
            region: "us-east-1".to_string(),
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            _container: Some(TestcontainerHolder::LocalStack(container)),
        })
    }
}

// =============================================================================
// Kafka testcontainer (live → docker fallback)
// =============================================================================

/// Acquire a Kafka config: live (env) if reachable, else testcontainer.
///
/// Returns `(KafkaTestConfig, holder)` — the holder must be kept alive for
/// the test's duration; dropping it stops the container.
pub async fn acquire_kafka() -> Option<(KafkaTestConfig, Option<TestcontainerHolder>)> {
    // Live: existing TestMode pattern
    let live = kafka_test_config();
    if live.is_reachable() {
        return Some((live, None));
    }

    // Fallback: start an Apache Kafka testcontainer
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache::Kafka;

    let container = Kafka::default().start().await.ok()?;
    let host = container.get_host().await.ok()?;
    let port = container
        .get_host_port_ipv4(testcontainers_modules::kafka::apache::KAFKA_PORT)
        .await
        .ok()?;
    let brokers = format!("{host}:{port}");

    let cfg = KafkaTestConfig {
        brokers,
        security_protocol: "PLAINTEXT".to_string(),
        sasl_mechanism: None,
        sasl_user: None,
        sasl_password: None,
    };
    Some((cfg, Some(TestcontainerHolder::Kafka(container))))
}

/// Skip test if Kafka is not reachable in the current test mode.
///
/// Usage: `skip_if_no_kafka!();` at the top of a test function.
#[macro_export]
macro_rules! skip_if_no_kafka {
    () => {
        let kf = common::kafka_test_config();
        if !kf.is_reachable() {
            eprintln!(
                "Skipping: Kafka not reachable at {} (TEST_MODE={})",
                kf.brokers,
                common::TestMode::detect()
            );
            return;
        }
    };
}
