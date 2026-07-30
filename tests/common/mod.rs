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

    /// Is this config usable as-is: reachable AND coherent?
    ///
    /// `is_reachable` only proves a TCP port answered. A SASL protocol with no
    /// credentials is not usable: the TCP probe answers on a PLAINTEXT broker
    /// bound to the same port, and every produce then fails authentication.
    /// Fall through to the container instead.
    pub fn is_usable(&self) -> bool {
        if self.security_protocol.to_uppercase().contains("SASL") && !self.has_sasl() {
            return false;
        }
        self.is_reachable()
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

    /// Convert to scalo KafkaConfig for use with KafkaTransport.
    pub fn to_scalo_config(&self) -> scalo::transport::KafkaConfig {
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

impl VaultTestConfig {
    /// True when this config owns a container, false when it points at a live
    /// external OpenBao. A naming assertion only applies to the former.
    #[must_use]
    pub fn manages_container(&self) -> bool {
        self._container.is_some()
    }
}

// =============================================================================
// Test image pins
// =============================================================================
//
// Pinned HERE rather than left to testcontainers-modules' defaults, which lag
// badly: Kafka 3.8.0 and LocalStack 4.5, which predates its move to CalVer. A
// tag baked into a dependency's source is invisible to dependency review --
// Renovate reads Cargo.toml, correctly reports the crate current, and never
// sees the image. Hoisting the tags out is what puts them back under review,
// hence the annotations.

/// renovate: datasource=docker depName=apache/kafka-native
const KAFKA_TAG: &str = "4.3.1";

/// SEMVER line only -- do NOT move this to the CalVer tags (`2026.07.0` etc).
/// The CalVer images on `localstack/localstack` require a licence: they exit 55
/// with "License activation failed! ... set the LOCALSTACK_AUTH_TOKEN
/// variable", so every LocalStack test skips. 4.x is the newest line that boots
/// with no token; reject a CalVer bump.
///
/// renovate: datasource=docker depName=localstack/localstack versioning=semver
const LOCALSTACK_TAG: &str = "4.14";

/// A floating `latest` was worse than a stale pin: the harness silently
/// retargeted on every image refresh, so a break landed with nothing in the
/// diff to explain it.
///
/// renovate: datasource=docker depName=openbao/openbao
const OPENBAO_TAG: &str = "2.6.1";

/// Holder for any auto-managed testcontainer. Drop stops the container.
pub enum TestcontainerHolder {
    GenericVault(testcontainers::ContainerAsync<testcontainers::GenericImage>),
    LocalStack(testcontainers::ContainerAsync<testcontainers_modules::localstack::LocalStack>),
    Kafka(testcontainers::ContainerAsync<testcontainers_modules::kafka::apache::Kafka>),
}

// ============================================================================
// Container naming and cleanup
// ============================================================================
//
// Every container this suite starts carries a name that says which repo, which
// suite and which backing service it is, so an operator looking at `docker ps`
// can tell what left it behind. testcontainers' default is a random hex name,
// which is untraceable the moment one survives.
//
// Naming: `dfe-fetcher-test-integration-<service>` for an instance shared by a
// group of tests, or `dfe-fetcher-test-integration-<test>-<service>` when a
// single test owns one. `container_name` builds both.
//
// Cleanup is belt AND braces, because `Drop` alone is not enough:
//
//   - Normal completion and a panic both unwind, so `Drop` stops the container.
//   - A SIGKILL, an abort, or Ctrl-C on the test run does NOT. `Drop` never
//     runs and the container survives.
//
// testcontainers-rs 0.27 has no resource reaper (no Ryuk), so the second case
// is the one that leaves crap behind. A deterministic name would then make it
// WORSE than a random one -- the leaked container holds the name and every
// later run fails with "name already in use". `reap_stale` closes that: remove
// any container already holding the name before starting, so a leak costs the
// next run nothing and self-heals.
//
// The label goes on as well, so a sweep can find these regardless of name:
//   docker rm -f $(docker ps -aq --filter label=io.hyperi.test.suite=dfe-fetcher-integration)

/// Label marking every container this suite starts, for bulk cleanup.
pub const TEST_SUITE_LABEL: (&str, &str) = ("io.hyperi.test.suite", "dfe-fetcher-integration");

/// Labels for a container this suite starts: what it is, and whose run owns it.
///
/// The name says what and why; these say WHO, which is what you need when
/// several runs share a machine and one has left something behind. The pid is
/// the owning test process -- `ps -p <pid>` answers "is that run still alive, or
/// is this rubbish I can remove?".
fn test_labels(service: &str) -> Vec<(String, String)> {
    vec![
        (
            TEST_SUITE_LABEL.0.to_string(),
            TEST_SUITE_LABEL.1.to_string(),
        ),
        ("io.hyperi.test.repo".to_string(), "dfe-fetcher".to_string()),
        ("io.hyperi.test.service".to_string(), service.to_string()),
        (
            "io.hyperi.test.owner-pid".to_string(),
            std::process::id().to_string(),
        ),
    ]
}

/// Container name for a backing service in this suite.
///
/// Pass `Some(test)` when one test owns the container, `None` when a group
/// shares it. Names are lowercased and non-alphanumerics collapse to `-`,
/// because Docker only accepts `[a-zA-Z0-9][a-zA-Z0-9_.-]*`, and a Rust test
/// path (`credentials::test_vault_resolve`) has colons in it.
#[must_use]
pub fn container_name(test: Option<&str>, service: &str) -> String {
    let slug = |s: &str| {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>()
    };
    match test {
        Some(t) => format!("dfe-fetcher-test-integration-{}-{}", slug(t), slug(service)),
        None => format!("dfe-fetcher-test-integration-{}", slug(service)),
    }
}

/// Remove a DEAD container holding `name`, so a leak from a killed run cannot
/// block this one.
///
/// Never touches a RUNNING container. Two concurrent runs of this suite on one
/// machine share these names, and force-removing a live one would sabotage the
/// other run -- a confusing mid-test failure in a process that did nothing
/// wrong. Leaving it means the start below fails with "name is already in use",
/// which says what actually happened.
///
/// Best-effort otherwise: no Docker, nothing to remove, or an already-gone
/// container are all fine. A failure here must not fail the test -- the start
/// that follows reports the real problem.
pub fn reap_stale(name: &str) {
    let running = std::process::Command::new("docker")
        .args(["ps", "--quiet", "--filter", &format!("name=^{name}$")])
        .output();
    // Non-empty stdout means a container by this name is up. Leave it alone.
    if let Ok(out) = &running
        && !out.stdout.is_empty()
    {
        return;
    }
    let _ = std::process::Command::new("docker")
        .args(["rm", "--force", "--volumes", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
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

        // BAO_-prefixed, and the readiness line reads "OpenBao server started!".
        // The VAULT_ spellings this used to carry are silently ignored: the
        // server issues a RANDOM root token instead of the one asked for, and
        // the wait strategy never matches, so the container start just times
        // out. Neither failure names OpenBao as the cause.
        let name = container_name(None, "openbao");
        reap_stale(&name);
        let image = GenericImage::new("openbao/openbao", OPENBAO_TAG)
            .with_exposed_port(8200u16.tcp())
            .with_wait_for(WaitFor::message_on_stdout("OpenBao server started"))
            .with_env_var("BAO_DEV_ROOT_TOKEN_ID", "root")
            .with_env_var("BAO_DEV_LISTEN_ADDRESS", "0.0.0.0:8200")
            .with_cmd(["server", "-dev"])
            .with_container_name(&name)
            .with_labels(test_labels("openbao"));

        let container = match image.start().await {
            Ok(c) => c,
            Err(e) => {
                require_container_path_in_ci("OpenBao", &e.to_string());
                return None;
            }
        };
        let host = match container.get_host().await {
            Ok(h) => h,
            Err(e) => {
                require_container_path_in_ci("OpenBao", &format!("get_host: {e}"));
                return None;
            }
        };
        let port = match container.get_host_port_ipv4(8200u16).await {
            Ok(p) => p,
            Err(e) => {
                require_container_path_in_ci("OpenBao", &format!("get_host_port: {e}"));
                return None;
            }
        };
        let address = format!("http://{host}:{port}");

        #[allow(clippy::used_underscore_binding)]
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
        use testcontainers::ImageExt;
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::localstack::LocalStack;

        // Carry the start error through rather than `.ok()?`: a container that
        // refuses to boot is indistinguishable from an absent Docker daemon
        // once the reason is dropped, and both turn every test here into a
        // silent no-op.
        let name = container_name(None, "localstack");
        reap_stale(&name);
        let container = match LocalStack::default()
            .with_tag(LOCALSTACK_TAG)
            .with_container_name(&name)
            .with_labels(test_labels("localstack"))
            .start()
            .await
        {
            Ok(c) => c,
            Err(e) => {
                require_container_path_in_ci("LocalStack", &e.to_string());
                return None;
            }
        };
        let host = match container.get_host().await {
            Ok(h) => h,
            Err(e) => {
                require_container_path_in_ci("LocalStack", &format!("get_host: {e}"));
                return None;
            }
        };
        let port = match container.get_host_port_ipv4(4566).await {
            Ok(p) => p,
            Err(e) => {
                require_container_path_in_ci("LocalStack", &format!("get_host_port: {e}"));
                return None;
            }
        };
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
    if live.is_usable() {
        return Some((live, None));
    }

    // Fallback: start an Apache Kafka testcontainer
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache::Kafka;

    let name = container_name(None, "kafka");
    reap_stale(&name);
    let Ok(container) = Kafka::default()
        .with_tag(KAFKA_TAG)
        .with_container_name(&name)
        .with_labels(test_labels("kafka"))
        .start()
        .await
    else {
        require_kafka_path_in_ci();
        return None;
    };
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

/// Panic if NEITHER a live service nor a container is available while in CI.
///
/// Scoped to "no path at all", not to "the live service is absent". CI is not
/// promised an external Kafka / OpenBao / LocalStack, but it IS promised a
/// container runtime, so an `acquire_*` helper should always find one of the
/// two. If it finds neither, the test would pass VACUOUSLY -- green while
/// exercising nothing.
///
/// The live-only probe in `skip_if_no_kafka!` stays a plain skip for the same
/// reason: failing on it would assert an environment nobody agreed to provide.
pub fn require_container_path_in_ci(service: &str, reason: &str) {
    assert!(
        std::env::var_os("CI").is_none(),
        "no {service} available in CI -- no live endpoint and the container \
         would not start ({reason}). Integration tests must RUN here, not \
         skip; skipping would report green while testing nothing."
    );
    // Outside CI the skip is legitimate, but the reason still has to be
    // visible -- it is the only signal that the test did not run.
    eprintln!("{service} container unavailable, test will skip: {reason}");
}

/// Kafka spelling of [`require_container_path_in_ci`].
pub fn require_kafka_path_in_ci() {
    require_container_path_in_ci("Kafka", "testcontainer start failed");
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
