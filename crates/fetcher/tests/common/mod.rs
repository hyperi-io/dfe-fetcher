// Project:   dfe-fetcher
// File:      crates/fetcher/tests/common/mod.rs
// Purpose:   Shared test infrastructure for dual-mode (remote/docker) testing
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared test helpers for dual-mode test infrastructure.
//!
//! Tests can run against either:
//! - **Remote** -- devex cluster via `.env` (KAFKA_BROKERS, KAFKA_SASL_*, etc.)
//! - **Docker** -- dfe-docker infra profile (localhost:19092, PLAINTEXT, no SASL)
//!
//! Set `TEST_MODE=docker` or `TEST_MODE=remote` in `.env` or environment.

#![allow(dead_code)] // Shared utilities -- not all used by every test file

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
// Live-credential env: .env-cloud is canonical, .env is fallback
// =============================================================================

/// Load cloud credentials from `.env-cloud` (preferred) or `.env` (fallback).
/// `dotenvy::from_filename_override` replaces any existing env vars so stale
/// values from a previous session can't leak through. Both lookups walk up
/// from the test's working directory, so the repo-root file is found from a
/// workspace member.
pub fn load_env() {
    let _ = dotenvy::from_filename_override(".env-cloud");
    let _ = dotenvy::dotenv_override();
}

/// A required live-test variable; a live test panics rather than skips when
/// it is missing, so it never reports green having exercised nothing.
pub fn require(key: &str) -> String {
    env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| panic!("missing required env var {key} (check .env-cloud)"))
}

/// An optional live-test variable; empty counts as unset.
pub fn optional(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.is_empty())
}

// =============================================================================
// Service-account keys for the JWT-bearer tests
// =============================================================================

/// A throwaway RSA key pair: the private key as a PKCS#8 PEM and the public
/// key as an SPKI PEM, generated per test so no key is ever committed.
pub fn rsa_key_pair() -> (String, String) {
    use aws_lc_rs::encoding::AsDer;
    use aws_lc_rs::rsa::{KeyPair, KeySize};
    use aws_lc_rs::signature::KeyPair as _;
    use base64::Engine as _;

    let pair = KeyPair::generate(KeySize::Rsa2048).expect("rsa key");
    let pem = |label: &str, der: &[u8]| {
        let body = base64::engine::general_purpose::STANDARD.encode(der);
        let lines: Vec<&str> = body
            .as_bytes()
            .chunks(64)
            .map(|c| std::str::from_utf8(c).expect("base64 is ascii"))
            .collect();
        format!(
            "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
            lines.join("\n")
        )
    };
    let private = pem("PRIVATE KEY", pair.as_der().expect("pkcs8").as_ref());
    let public = pem(
        "PUBLIC KEY",
        pair.public_key().as_der().expect("spki").as_ref(),
    );
    (private, public)
}

/// A Google-style service-account key JSON around a private key PEM.
pub fn service_account_key(private_key_pem: &str, client_email: &str, token_uri: &str) -> String {
    serde_json::json!({
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "kid-1",
        "private_key": private_key_pem,
        "client_email": client_email,
        "client_id": "1234567890",
        "token_uri": token_uri,
    })
    .to_string()
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
// OpenBao / Vault config (live -> testcontainers fallback)
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

/// The version the fleet deploys, not the newest published. Strimzi 0.51 is
/// held back deliberately (1.0 drops the CRD versions the charts use) and its
/// ceiling is Kafka 4.2.0, so a test proving broker behaviour above that proves
/// it against something nobody runs. Raise this only with the operator.
///
/// renovate: datasource=docker depName=apache/kafka-native allowedVersions=<=4.2.0
const KAFKA_TAG: &str = "4.2.0";

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

/// renovate: datasource=docker depName=postgres
const POSTGRES_TAG: &str = "18.6-alpine";

/// The current LTS line.
///
/// renovate: datasource=docker depName=mariadb
const MARIADB_TAG: &str = "11.8.9";

/// The line the DFE stack runs; from 25.x the entrypoint refuses a
/// passwordless `default` user unless `CLICKHOUSE_SKIP_USER_SETUP` says so.
///
/// renovate: datasource=docker depName=clickhouse/clickhouse-server
const CLICKHOUSE_TAG: &str = "26.3.32.14";

/// The current 8.x line; the image carries `mongosh`, which the replica-set
/// start runs `rs.initiate()` through.
///
/// renovate: datasource=docker depName=mongo
const MONGO_TAG: &str = "8.3.9";

/// MongoDB 8.x refuses to start on Linux 6.19 through 7.0.13 (SERVER-121912:
/// its vendored TCMalloc and the kernel disagree over `rseq`); with glibc
/// owning `rseq` the allocator takes its fallback path and the server runs,
/// so the test container carries the tunable.
const MONGO_KERNEL_WORKAROUND: (&str, &str) = ("GLIBC_TUNABLES", "glibc.pthread.rseq=1");

/// SQL Server 2025, the current line; the image is Microsoft's and starting
/// it accepts the EULA (`ACCEPT_EULA=Y`), which is why the test that uses it
/// runs only where an operator has installed the matching ODBC driver.
///
/// renovate: datasource=docker depName=mcr.microsoft.com/mssql/server
const MSSQL_TAG: &str = "2025-CU8-ubuntu-24.04";

/// Oracle Database Free 23ai, the community image with a fast start; same
/// operator-supplied-driver gate as SQL Server.
///
/// renovate: datasource=docker depName=gvenzl/oracle-free
const ORACLE_TAG: &str = "23.26.3-slim-faststart";

/// Holder for any auto-managed testcontainer. Drop stops the container.
pub enum TestcontainerHolder {
    GenericVault(testcontainers::ContainerAsync<testcontainers::GenericImage>),
    LocalStack(testcontainers::ContainerAsync<testcontainers_modules::localstack::LocalStack>),
    Kafka(testcontainers::ContainerAsync<testcontainers_modules::kafka::apache::Kafka>),
    Postgres(testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>),
    Mariadb(testcontainers::ContainerAsync<testcontainers_modules::mariadb::Mariadb>),
    ClickHouse(testcontainers::ContainerAsync<testcontainers_modules::clickhouse::ClickHouse>),
    Generic(testcontainers::ContainerAsync<testcontainers::GenericImage>),
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
// Naming: `dfe-fetcher-test-integration-<test>-<service>`, because every
// container here is owned by exactly ONE test. nextest runs each test in its own
// process, so nothing is shared even when it looks like it should be -- four
// tests calling `acquire_kafka()` start four brokers. That was already true with
// testcontainers' random names; the only thing a single shared name would add is
// a collision, where the first test wins and the rest fail with "name is already
// in use" and skip. `container_name` still takes `None` for a container started
// once for a whole binary, but no suite does that today.
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
/// Pass `Some(test)` -- the owning test -- for anything a test starts for itself,
/// which is everything here. `None` is for a container started once for a whole
/// test binary; nothing does that today, and using it from several tests would
/// make them collide on the name rather than share the container.
///
/// Names are lowercased and non-alphanumerics collapse to `-`, because Docker
/// only accepts `[a-zA-Z0-9][a-zA-Z0-9_.-]*`, and a Rust test path
/// (`credentials::test_vault_resolve`) has colons in it.
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
    /// `test` names the calling test and goes into the container name, so
    /// concurrent tests do not collide on it.
    ///
    /// Returns `None` if no live Vault is configured AND Docker is unavailable.
    pub async fn acquire(test: &str) -> Option<Self> {
        load_dotenv();

        // Live mode: env vars present -> use them, no container managed
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
        let name = container_name(Some(test), "openbao");
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
    ///
    /// `test` names the calling test and goes into the container name, so
    /// concurrent tests do not collide on it.
    ///
    /// Returns `None` if neither is available.
    pub async fn acquire(test: &str) -> Option<Self> {
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
        let name = container_name(Some(test), "localstack");
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
// Kafka testcontainer (live -> docker fallback)
// =============================================================================

/// Acquire a Kafka config: live (env) if reachable, else testcontainer.
///
/// `test` names the calling test and goes into the container name, so concurrent
/// tests do not collide on it.
///
/// Returns `(KafkaTestConfig, holder)` -- the holder must be kept alive for
/// the test's duration; dropping it stops the container.
pub async fn acquire_kafka(test: &str) -> Option<(KafkaTestConfig, Option<TestcontainerHolder>)> {
    // Live: existing TestMode pattern
    let live = kafka_test_config();
    if live.is_usable() {
        return Some((live, None));
    }

    // Fallback: start an Apache Kafka testcontainer
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache::Kafka;

    let name = container_name(Some(test), "kafka");
    // A start on a busy runner fails transiently while sibling tests start theirs;
    // retry before deciding the runtime has no Kafka to offer.
    let mut container = None;
    let mut last_error = String::new();
    for attempt in 1..=3 {
        reap_stale(&name);
        match Kafka::default()
            .with_tag(KAFKA_TAG)
            .with_container_name(&name)
            .with_labels(test_labels("kafka"))
            .start()
            .await
        {
            Ok(started) => {
                container = Some(started);
                break;
            }
            Err(error) => {
                last_error = format!("attempt {attempt}: {error}");
                eprintln!("kafka testcontainer {name}: {last_error}");
                tokio::time::sleep(std::time::Duration::from_secs(2 * attempt)).await;
            }
        }
    }
    let Some(container) = container else {
        require_container_path_in_ci(
            "Kafka",
            &format!("testcontainer start failed, {last_error}"),
        );
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

// =============================================================================
// Framework snapshot round-trip helpers
// =============================================================================

/// Read raw frames from `topic` until `want` have arrived or the deadline
/// passes. The consumer group must be unique per call: two callers sharing one
/// would split the partitions and read each other's frames.
pub async fn consume_frames(
    kf: &KafkaTestConfig,
    topic: &str,
    group: &str,
    want: usize,
) -> Vec<Vec<u8>> {
    use scalo::transport::{TransportBase, TransportReceiver};

    let mut consumer_config = kf.to_scalo_config();
    consumer_config.topics = vec![topic.to_owned()];
    consumer_config.group = group.to_owned();
    consumer_config.auto_offset_reset = "earliest".to_string();
    consumer_config.enable_auto_commit = true;
    let consumer = scalo::transport::KafkaTransport::new(&consumer_config)
        .await
        .unwrap_or_else(|e| panic!("consumer init against {}: {e}", kf.brokers));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut frames = Vec::new();
    while frames.len() < want && tokio::time::Instant::now() < deadline {
        if let Ok(batch) = consumer.recv(100).await {
            frames.extend(batch.records.into_iter().map(|r| r.payload.to_vec()));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let _ = consumer.close().await;
    frames
}

/// Pull one unit's rows straight off a REST shape, as the driver would, with
/// no window (the profile's lookback applies) and no checkpoint.
pub async fn shape_rows(
    shape: &dfe_fetcher_rest::RestShape,
    unit: &str,
) -> Result<Vec<serde_json::Value>, dfe_fetcher_core::error::Error> {
    use dfe_fetcher_core::{RowSource, TickCtx};
    use futures::StreamExt;

    let spec = shape
        .units()
        .iter()
        .find(|u| &*u.name == unit)
        .unwrap_or_else(|| panic!("unit {unit} is bound"))
        .clone();
    let tick = TickCtx {
        window: None,
        connection_id: shape.connection_id(),
        unit: &spec,
        checkpoint: None,
    };
    let mut stream = shape.rows(tick);
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(serde_json::from_slice(&row?.payload).expect("row is JSON"));
    }
    Ok(out)
}

/// The one REST shape a typed block maps onto: its shipped profile bound to
/// the instance the block's single connection became.
pub fn builtin_shape(
    instances: Vec<dfe_fetcher::config::BuiltinInstance>,
) -> dfe_fetcher_rest::RestShape {
    let [built] = <[dfe_fetcher::config::BuiltinInstance; 1]>::try_from(instances)
        .unwrap_or_else(|v| panic!("one connection, got {}", v.len()));
    let profile = dfe_fetcher_rest::profile::bound::resolve_profile(
        &built.instance,
        dfe_fetcher::profiles::shipped(),
    )
    .expect("a shipped profile");
    dfe_fetcher_rest::RestShape::from_instance(
        &profile,
        &built.instance,
        &built.connection_id,
        dfe_fetcher_rest::http_client().expect("http client"),
        &dfe_fetcher_rest::exchange_client().expect("exchange client"),
    )
    .unwrap_or_else(|e| panic!("bind {}: {e}", built.connection_id))
}

/// Offer every frame to a reassembler and return it with the last frame's
/// snapshot id.
pub fn reassemble(frames: &[Vec<u8>]) -> (dfe_fetcher_core::envelope::Reassembler, uuid::Uuid) {
    let mut asm = dfe_fetcher_core::envelope::Reassembler::default();
    let mut id = None;
    for frame in frames {
        asm.offer(frame).expect("frame is an envelope");
        let envelope: dfe_fetcher_core::envelope::Envelope =
            serde_json::from_slice(frame).expect("frame is an envelope");
        id = Some(envelope.head().snapshot_id);
    }
    (asm, id.expect("at least one frame"))
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

// =============================================================================
// Database containers for the database-shape tests
// =============================================================================

/// A database a test can dump: where it listens, and the container that
/// backs it (kept alive for the test's duration).
pub struct DatabaseTestConfig {
    pub host: String,
    pub port: u16,
    pub _container: TestcontainerHolder,
}

/// Start a PostgreSQL container seeded with `init_sql`; user, password and
/// database are all `postgres`.
///
/// Returns `None` (a skip outside CI, a panic in CI) when no container can
/// start.
pub async fn acquire_postgres(test: &str, init_sql: &str) -> Option<DatabaseTestConfig> {
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;

    let name = container_name(Some(test), "postgres");
    reap_stale(&name);
    let container = match Postgres::default()
        .with_init_sql(init_sql.to_owned().into_bytes())
        .with_tag(POSTGRES_TAG)
        .with_container_name(&name)
        .with_labels(test_labels("postgres"))
        .start()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            require_container_path_in_ci("PostgreSQL", &e.to_string());
            return None;
        }
    };
    let host = container.get_host().await.ok()?.to_string();
    let port = container.get_host_port_ipv4(5432).await.ok()?;
    Some(DatabaseTestConfig {
        host,
        port,
        _container: TestcontainerHolder::Postgres(container),
    })
}

/// Start a MariaDB container seeded with `init_sql`; user `root` with no
/// password, database `test`.
pub async fn acquire_mariadb(test: &str, init_sql: &str) -> Option<DatabaseTestConfig> {
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::mariadb::Mariadb;

    let name = container_name(Some(test), "mariadb");
    reap_stale(&name);
    let container = match Mariadb::default()
        .with_init_sql(init_sql.to_owned().into_bytes())
        .with_tag(MARIADB_TAG)
        .with_container_name(&name)
        .with_labels(test_labels("mariadb"))
        .start()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            require_container_path_in_ci("MariaDB", &e.to_string());
            return None;
        }
    };
    let host = container.get_host().await.ok()?.to_string();
    let port = container.get_host_port_ipv4(3306).await.ok()?;
    Some(DatabaseTestConfig {
        host,
        port,
        _container: TestcontainerHolder::Mariadb(container),
    })
}

/// Start a ClickHouse container; user `default` with no password, HTTP on
/// the returned port.
pub async fn acquire_clickhouse(test: &str) -> Option<DatabaseTestConfig> {
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::clickhouse::ClickHouse;

    let name = container_name(Some(test), "clickhouse");
    reap_stale(&name);
    let container = match ClickHouse::default()
        .with_tag(CLICKHOUSE_TAG)
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .with_container_name(&name)
        .with_labels(test_labels("clickhouse"))
        .with_startup_timeout(std::time::Duration::from_secs(180))
        .start()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            require_container_path_in_ci("ClickHouse", &e.to_string());
            return None;
        }
    };
    let host = container.get_host().await.ok()?.to_string();
    let port = container.get_host_port_ipv4(8123).await.ok()?;
    Some(DatabaseTestConfig {
        host,
        port,
        _container: TestcontainerHolder::ClickHouse(container),
    })
}

/// Start a MongoDB container: a standalone server, or a one-member replica
/// set (`--replSet rs`, then `rs.initiate()` through `mongosh`, ready when
/// the log says writes are permitted) when `replica_set` is set, which is
/// what a change stream needs. No auth; the returned config's `host:port`
/// is the driver's address, and a replica set needs `?directConnection=true`
/// on the URI because the member advertises its container hostname, which
/// the host cannot resolve.
pub async fn acquire_mongo(test: &str, replica_set: bool) -> Option<DatabaseTestConfig> {
    use testcontainers::core::{CmdWaitFor, ExecCommand, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    let name = container_name(Some(test), "mongo");
    reap_stale(&name);
    let mut image = GenericImage::new("mongo", MONGO_TAG)
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
        .with_env_var(MONGO_KERNEL_WORKAROUND.0, MONGO_KERNEL_WORKAROUND.1)
        .with_container_name(&name)
        .with_labels(test_labels("mongo"))
        .with_startup_timeout(std::time::Duration::from_secs(120));
    if replica_set {
        image = image.with_cmd(["--replSet", "rs"]);
    }
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            require_container_path_in_ci("MongoDB", &e.to_string());
            return None;
        }
    };
    if replica_set {
        let initiate = ExecCommand::new(["mongosh", "--quiet", "--eval", "rs.initiate()"])
            .with_cmd_ready_condition(CmdWaitFor::message_on_stdout(
                "Using a default configuration for the set",
            ))
            .with_container_ready_conditions(vec![WaitFor::message_on_stdout(
                "Transition to primary complete; database writes are now permitted",
            )]);
        if let Err(e) = container.exec(initiate).await {
            require_container_path_in_ci("MongoDB replica set", &e.to_string());
            return None;
        }
    }
    let host = container.get_host().await.ok()?.to_string();
    let port = container.get_host_port_ipv4(27017).await.ok()?;
    Some(DatabaseTestConfig {
        host,
        port,
        _container: TestcontainerHolder::Generic(container),
    })
}

/// The MongoDB URI for a container `acquire_mongo` started.
pub fn mongo_uri(db: &DatabaseTestConfig, replica_set: bool) -> String {
    let direct = if replica_set {
        "/?directConnection=true"
    } else {
        "/"
    };
    format!("mongodb://{}:{}{direct}", db.host, db.port)
}

/// Start a SQL Server container; login `sa` with the returned password on
/// port 1433. Starting it accepts Microsoft's EULA on the operator's behalf,
/// so only a test already gated on the operator-supplied driver calls this.
pub async fn acquire_mssql(test: &str, sa_password: &str) -> Option<DatabaseTestConfig> {
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    let name = container_name(Some(test), "mssql");
    reap_stale(&name);
    let image = GenericImage::new("mcr.microsoft.com/mssql/server", MSSQL_TAG)
        .with_wait_for(WaitFor::message_on_stdout(
            "SQL Server is now ready for client connections",
        ))
        .with_env_var("ACCEPT_EULA", "Y")
        .with_env_var("MSSQL_SA_PASSWORD", sa_password)
        .with_container_name(&name)
        .with_labels(test_labels("mssql"))
        .with_startup_timeout(std::time::Duration::from_secs(180));
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            require_container_path_in_ci("SQL Server", &e.to_string());
            return None;
        }
    };
    let host = container.get_host().await.ok()?.to_string();
    let port = container.get_host_port_ipv4(1433).await.ok()?;
    Some(DatabaseTestConfig {
        host,
        port,
        _container: TestcontainerHolder::Generic(container),
    })
}

/// Start an Oracle Database Free container; user `dfe` with the returned
/// password on service `FREEPDB1`, port 1521.
pub async fn acquire_oracle(test: &str, password: &str) -> Option<DatabaseTestConfig> {
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    let name = container_name(Some(test), "oracle");
    reap_stale(&name);
    let image = GenericImage::new("gvenzl/oracle-free", ORACLE_TAG)
        .with_wait_for(WaitFor::message_on_stdout("DATABASE IS READY TO USE!"))
        .with_env_var("ORACLE_PASSWORD", password)
        .with_env_var("APP_USER", "dfe")
        .with_env_var("APP_USER_PASSWORD", password)
        .with_container_name(&name)
        .with_labels(test_labels("oracle"))
        .with_startup_timeout(std::time::Duration::from_secs(300));
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            require_container_path_in_ci("Oracle", &e.to_string());
            return None;
        }
    };
    let host = container.get_host().await.ok()?.to_string();
    let port = container.get_host_port_ipv4(1521).await.ok()?;
    Some(DatabaseTestConfig {
        host,
        port,
        _container: TestcontainerHolder::Generic(container),
    })
}

/// The installed ODBC driver whose name contains `fragment`, preferring a
/// Unicode build.
///
/// `None` skips outside CI with the apt package to install; in CI a missing
/// driver is a failure, because a test that skips for its environment is not
/// a gate.
#[cfg(feature = "db-odbc")]
pub fn odbc_driver(fragment: &str, apt_package: &str) -> Option<String> {
    let drivers = match dfe_fetcher_db::odbc::installed_drivers() {
        Ok(d) => d,
        Err(e) => {
            assert!(
                std::env::var_os("CI").is_none(),
                "the ODBC driver manager is unusable in CI ({e}); install unixodbc"
            );
            eprintln!("ODBC driver manager unusable, test will skip: {e}");
            return None;
        }
    };
    let found = registered_odbc_driver(&drivers, fragment);
    if found.is_none() {
        assert!(
            std::env::var_os("CI").is_none(),
            "no `{fragment}` ODBC driver registered in CI (have {drivers:?}); install {apt_package}"
        );
        eprintln!(
            "no `{fragment}` ODBC driver registered (have {drivers:?}); install {apt_package} -- test will skip"
        );
    }
    found
}

/// The installed ODBC driver whose name contains `fragment` for an engine
/// whose driver is proprietary and operator-supplied (a click-through
/// licence no runner installs), so its absence is a named skip everywhere,
/// CI included, never a failure and never a silent green.
#[cfg(feature = "db-odbc")]
pub fn odbc_driver_optional(fragment: &str, licence_note: &str) -> Option<String> {
    let drivers = match dfe_fetcher_db::odbc::installed_drivers() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("ODBC driver manager unusable, test will skip: {e}");
            return None;
        }
    };
    let found = registered_odbc_driver(&drivers, fragment);
    if found.is_none() {
        eprintln!(
            "SKIPPED: no `{fragment}` ODBC driver registered (have {drivers:?}); it is {licence_note} \
             and this proof runs only where an operator has installed it"
        );
    }
    found
}

/// The driver named like `fragment`, a Unicode build first.
#[cfg(feature = "db-odbc")]
fn registered_odbc_driver(drivers: &[String], fragment: &str) -> Option<String> {
    drivers
        .iter()
        .find(|d| d.contains(fragment) && d.contains("Unicode"))
        .or_else(|| drivers.iter().find(|d| d.contains(fragment)))
        .cloned()
}
