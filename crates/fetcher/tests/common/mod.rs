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
//! - **Remote** -- a remote Kafka cluster via `.env` (KAFKA_BROKERS, KAFKA_SASL_*, etc.)
//! - **Docker** -- dfe-docker infra profile (localhost:19092, PLAINTEXT, no SASL)
//!
//! Set `TEST_MODE=docker` or `TEST_MODE=remote` in `.env` or environment.

#![allow(dead_code)] // Shared utilities -- not all used by every test file

use std::env;
use std::sync::OnceLock;

use scalo::deployment::test_support::shared_service::{
    self, Ready, ServiceSpec, SharedContainer, Suite,
};
use scalo::transport::kafka::test_support::{require_docker, shared_kafka};

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

/// Load this repo's own `.env` (won't override existing env vars):
/// `dotenvy::dotenv()` would load the first `.env` in any parent.
pub fn load_dotenv() {
    let _ = dotenvy::from_path(dfe_fetcher::deployment::repo_root().join(".env"));
}

// =============================================================================
// Live-credential env: .env-cloud is canonical, .env is fallback
// =============================================================================

/// Load cloud credentials from this repo's `.env-cloud` (preferred) or `.env`
/// (fallback), replacing any existing env vars so stale values from a previous
/// session can't leak through. Both are read from the repo root by path, never
/// found by a search up the tree.
pub fn load_env() {
    let root = dfe_fetcher::deployment::repo_root();
    let _ = dotenvy::from_path_override(root.join(".env-cloud"));
    let _ = dotenvy::from_path_override(root.join(".env"));
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
// OpenBao / Vault config (live -> shared container fallback)
// =============================================================================

/// OpenBao/Vault connection config for tests.
pub struct VaultTestConfig {
    pub address: String,
    pub token: String,
    pub mount_path: String,
    /// The run's shared OpenBao, `None` when a live one is configured.
    container: Option<SharedContainer>,
}

impl VaultTestConfig {
    /// The run's shared OpenBao container, `None` when a live one is used.
    #[must_use]
    pub fn container(&self) -> Option<&SharedContainer> {
        self.container.as_ref()
    }
}

// =============================================================================
// Test image pins
// =============================================================================
//
// Each tag carries a Renovate annotation so the image stays under dependency
// review. The Kafka image is scalo's `KAFKA_IMAGE`, the one the fleet tests on.

/// SEMVER line only -- do NOT move this to the CalVer tags (`2026.07.0` etc).
/// The CalVer images on `localstack/localstack` require a licence: they exit 55
/// with "License activation failed! ... set the LOCALSTACK_AUTH_TOKEN
/// variable", so every LocalStack test skips. 4.x is the newest line that boots
/// with no token; reject a CalVer bump.
///
/// renovate: datasource=docker depName=localstack/localstack versioning=semver
const LOCALSTACK_TAG: &str = "4.14";

/// Digest of `LOCALSTACK_TAG`, apart from it because the Renovate regex stops at a colon.
const LOCALSTACK_DIGEST: &str =
    "sha256:3ebc37595918b8accb852f8048fef2aff047d465167edd655528065b07bc364a";

/// A floating `latest` was worse than a stale pin: the harness silently
/// retargeted on every image refresh, so a break landed with nothing in the
/// diff to explain it.
///
/// renovate: datasource=docker depName=openbao/openbao
const OPENBAO_TAG: &str = "2.6.1";

/// Digest of `OPENBAO_TAG`, apart from it because the Renovate regex stops at a colon.
const OPENBAO_DIGEST: &str =
    "sha256:5b2486ab0fb90bbc788cc345b0a08616dfb375873ee8be5df3a2fd4d378a67e0";

/// renovate: datasource=docker depName=postgres
const POSTGRES_TAG: &str = "18.6-alpine";

/// Digest of `POSTGRES_TAG`, apart from it because the Renovate regex stops at a colon.
const POSTGRES_DIGEST: &str =
    "sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873";

/// The current LTS line.
///
/// renovate: datasource=docker depName=mariadb
const MARIADB_TAG: &str = "11.8.9";

/// Digest of `MARIADB_TAG`, apart from it because the Renovate regex stops at a colon.
const MARIADB_DIGEST: &str =
    "sha256:6422478cb8e159f080fb1d8ccf65101e26fe51385787fde7d16c3b165a331f15";

/// The line the DFE stack runs; from 25.x the entrypoint refuses a
/// passwordless `default` user unless `CLICKHOUSE_SKIP_USER_SETUP` says so.
///
/// renovate: datasource=docker depName=clickhouse/clickhouse-server
const CLICKHOUSE_TAG: &str = "26.3.42.3";

/// Digest of `CLICKHOUSE_TAG`, apart from it because the Renovate regex stops at a colon.
const CLICKHOUSE_DIGEST: &str =
    "sha256:21d572843e59539c7d100286b6f5a6053c341fe4b33c2d7b74b1ff7cb24c5399";

/// The current 8.x line; the image carries `mongosh`, which the replica-set
/// start runs `rs.initiate()` through.
///
/// renovate: datasource=docker depName=mongo
const MONGO_TAG: &str = "8.3.9";

/// Digest of `MONGO_TAG`, apart from it because the Renovate regex stops at a colon.
const MONGO_DIGEST: &str =
    "sha256:81a1c8842a09589fc8d5f285266f3340bf4abdf66700ba22988f14cc9b2b3118";

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

/// Digest of `MSSQL_TAG`, apart from it because the Renovate regex stops at a colon.
/// This tag is a single amd64 manifest, not a multi-arch index.
const MSSQL_DIGEST: &str =
    "sha256:4bab24f36c1ecd48e85f7d37df26e6bf301641d84c3fe652f9a0dcc947d512e1";

/// Oracle Database Free 23ai, the community image with a fast start; same
/// operator-supplied-driver gate as SQL Server.
///
/// renovate: datasource=docker depName=gvenzl/oracle-free
const ORACLE_TAG: &str = "23.26.3-slim-faststart";

/// Digest of `ORACLE_TAG`, apart from it because the Renovate regex stops at a colon.
const ORACLE_DIGEST: &str =
    "sha256:f5ff19033860d662c821cb04eb10483fa94f14f78eae252d054291ea07028093";

/// Holder for a container one test starts for itself. Drop stops it.
///
/// Only the ODBC engines' suites start one: each seeds its own database at
/// start, and none of them runs where the ODBC drivers are not installed.
pub enum TestcontainerHolder {
    Postgres(testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>),
    Mariadb(testcontainers::ContainerAsync<testcontainers_modules::mariadb::Mariadb>),
    Generic(testcontainers::ContainerAsync<testcontainers::GenericImage>),
}

// ============================================================================
// Container naming and cleanup
// ============================================================================

/// The suite every container here is named and labelled for: names start
/// `dfe-fetcher-test-integration-`, and every container carries the
/// `io.hyperi.test.*` labels with `io.hyperi.test.suite=dfe-fetcher-integration`.
pub const SUITE: Suite = Suite {
    repo: "dfe-fetcher",
    manifest_dir: env!("CARGO_MANIFEST_DIR"),
    tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
};

/// The run's one container for `spec`: the first test process to ask creates
/// it, the rest join it by name, and a watcher removes it when the run ends.
/// `None` (a skip outside CI, a failure in CI) when no Docker daemon answers.
async fn shared(
    cell: &'static OnceLock<SharedContainer>,
    spec: fn() -> ServiceSpec,
) -> Option<SharedContainer> {
    if let Some(container) = cell.get() {
        return Some(container.clone());
    }
    if !require_docker() {
        return None;
    }
    let container = tokio::task::spawn_blocking(move || {
        cell.get_or_init(|| shared_service::acquire(&SUITE, &spec()))
            .clone()
    })
    .await
    .unwrap_or_else(|e| panic!("acquire a shared container: {e}"));
    Some(container)
}

/// Container name for a database one ODBC test starts for itself.
#[must_use]
pub fn container_name(test: &str, service: &str) -> String {
    shared_service::container_name(&SUITE, Some(test), service)
}

/// Labels for a container one ODBC test starts for itself.
fn test_labels(service: &str) -> Vec<(String, String)> {
    shared_service::labels(&SUITE, service, std::process::id())
}

/// Every image the shared services start, as `name:tag@digest`.
#[must_use]
pub fn shared_images() -> Vec<String> {
    vec![
        scalo::transport::kafka::test_support::KAFKA_IMAGE.to_owned(),
        openbao_spec().image,
        localstack_spec().image,
        clickhouse_spec().image,
        mongo_spec().image,
    ]
}

/// The OpenBao dev server, root token `root`, KV v2 at `secret`.
fn openbao_spec() -> ServiceSpec {
    // BAO_-prefixed: the VAULT_ spellings are ignored, so the server would
    // issue a random root token and never log the readiness line.
    ServiceSpec {
        ports: vec![8200],
        env: vec![
            ("BAO_DEV_ROOT_TOKEN_ID".into(), "root".into()),
            ("BAO_DEV_LISTEN_ADDRESS".into(), "0.0.0.0:8200".into()),
        ],
        args: vec!["server".into(), "-dev".into()],
        ready: Ready::Log("OpenBao server started"),
        ..ServiceSpec::new(
            "openbao",
            format!("openbao/openbao:{OPENBAO_TAG}@{OPENBAO_DIGEST}"),
            std::time::Duration::from_secs(120),
        )
    }
}

/// LocalStack on its edge port.
fn localstack_spec() -> ServiceSpec {
    ServiceSpec {
        ports: vec![4566],
        ready: Ready::Log("Ready."),
        ..ServiceSpec::new(
            "localstack",
            format!("localstack/localstack:{LOCALSTACK_TAG}@{LOCALSTACK_DIGEST}"),
            std::time::Duration::from_secs(180),
        )
    }
}

/// ClickHouse over HTTP, user `default` with no password.
fn clickhouse_spec() -> ServiceSpec {
    ServiceSpec {
        ports: vec![8123],
        env: vec![("CLICKHOUSE_SKIP_USER_SETUP".into(), "1".into())],
        ready: Ready::Http {
            port: 8123,
            path: "/ping",
            body: "Ok.",
        },
        ..ServiceSpec::new(
            "clickhouse",
            format!("clickhouse/clickhouse-server:{CLICKHOUSE_TAG}@{CLICKHOUSE_DIGEST}"),
            std::time::Duration::from_secs(180),
        )
    }
}

/// A standalone MongoDB server, no auth.
fn mongo_spec() -> ServiceSpec {
    ServiceSpec {
        ports: vec![27017],
        env: vec![(
            MONGO_KERNEL_WORKAROUND.0.into(),
            MONGO_KERNEL_WORKAROUND.1.into(),
        )],
        ready: Ready::Log("Waiting for connections"),
        ..ServiceSpec::new(
            "mongo",
            format!("mongo:{MONGO_TAG}@{MONGO_DIGEST}"),
            std::time::Duration::from_secs(120),
        )
    }
}

/// A one-member MongoDB replica set: `--replSet rs`, `rs.initiate()` once the
/// server listens, ready once the member is primary.
fn mongo_replica_set_spec() -> ServiceSpec {
    ServiceSpec {
        args: vec!["--replSet".into(), "rs".into()],
        after_start: initiate_replica_set,
        ready: Ready::Log("Transition to primary complete; database writes are now permitted"),
        ..ServiceSpec {
            service: "mongo-rs",
            ..mongo_spec()
        }
    }
}

/// `rs.initiate()` through `mongosh`, retried until the server accepts it:
/// the hook runs before the readiness wait, so the server may not listen yet.
fn initiate_replica_set(container: &SharedContainer) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        let out = std::process::Command::new("docker")
            .args([
                "exec",
                &container.name,
                "mongosh",
                "--quiet",
                "--eval",
                "rs.initiate()",
            ])
            .output()
            .expect("docker exec");
        if out.status.success() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rs.initiate() in {}: {}",
            container.name,
            String::from_utf8_lossy(&out.stderr)
        );
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}

impl VaultTestConfig {
    /// Acquire a Vault test config: live (env) if available, else the run's
    /// shared OpenBao container. Tests keep apart by writing their own paths.
    ///
    /// Returns `None` if no live Vault is configured AND Docker is unavailable.
    pub async fn acquire(_test: &str) -> Option<Self> {
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
                container: None,
            });
        }

        static OPENBAO: OnceLock<SharedContainer> = OnceLock::new();
        let container = shared(&OPENBAO, openbao_spec).await?;
        Some(Self {
            address: format!("http://{}", container.addr(8200)),
            token: "root".to_string(),
            mount_path: "secret".to_string(),
            container: Some(container),
        })
    }
}

// =============================================================================
// LocalStack (AWS emulator) config
// =============================================================================

/// LocalStack endpoint config for AWS source tests.
pub struct LocalStackConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl LocalStackConfig {
    /// Acquire a LocalStack endpoint: live first, else the run's shared
    /// container.
    ///
    /// Returns `None` if neither is available.
    pub async fn acquire(_test: &str) -> Option<Self> {
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
            });
        }

        static LOCALSTACK: OnceLock<SharedContainer> = OnceLock::new();
        let container = shared(&LOCALSTACK, localstack_spec).await?;
        Some(Self {
            endpoint: format!("http://{}", container.addr(4566)),
            region: "us-east-1".to_string(),
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
        })
    }
}

// =============================================================================
// Kafka (live -> shared container fallback)
// =============================================================================

/// Acquire a Kafka config: live (env) if reachable, else the run's shared
/// broker. Tests keep apart by their own topics and consumer groups.
///
/// The second half is the container, `None` for a live broker.
pub async fn acquire_kafka(_test: &str) -> Option<(KafkaTestConfig, Option<SharedContainer>)> {
    let live = kafka_test_config();
    if live.is_usable() {
        return Some((live, None));
    }
    if !require_docker() {
        return None;
    }
    let container = shared_kafka(SUITE).await;
    let cfg = KafkaTestConfig {
        brokers: container.addr(9092),
        security_protocol: "PLAINTEXT".to_string(),
        sasl_mechanism: None,
        sasl_user: None,
        sasl_password: None,
    };
    Some((cfg, Some(container)))
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

/// How long each step of a [`consume_until`] read may take.
#[derive(Debug, Clone, Copy)]
pub struct ConsumeCeilings {
    /// From creating the consumer to its group holding an assignment.
    pub assigned: std::time::Duration,
    /// From the assignment to the frames meeting the read's condition.
    pub frames: std::time::Duration,
}

impl Default for ConsumeCeilings {
    // A loaded runner stretches the group join several-fold, and a read that
    // meets its condition returns at once, so only a failure pays these.
    fn default() -> Self {
        Self {
            assigned: std::time::Duration::from_secs(120),
            frames: std::time::Duration::from_secs(60),
        }
    }
}

/// Read raw frames from `topic` until `done` holds for everything read so far.
///
/// The read has two steps, each with its own ceiling: the consumer group joins
/// and is assigned the topic, then the frames arrive. Running out of time
/// panics naming the step, so a slow join never reads as missing frames.
/// `wanted` names the condition in that message. The consumer group must be
/// unique per call: two callers sharing one would split the partitions and
/// read each other's frames.
pub async fn consume_until(
    kf: &KafkaTestConfig,
    topic: &str,
    group: &str,
    ceilings: ConsumeCeilings,
    wanted: &str,
    done: impl Fn(&[Vec<u8>]) -> bool,
) -> Vec<Vec<u8>> {
    use scalo::transport::{TransportBase, TransportReceiver};
    use std::time::{Duration, Instant};

    // The group state comes from librdkafka's statistics, so a short interval
    // sees the join soon after it happens.
    let mut consumer_config = kf
        .to_scalo_config()
        .with_override("statistics.interval.ms", "250");
    consumer_config.topics = vec![topic.to_owned()];
    consumer_config.group = group.to_owned();
    consumer_config.auto_offset_reset = "earliest".to_string();
    consumer_config.enable_auto_commit = true;
    let started = Instant::now();
    let consumer = scalo::transport::KafkaTransport::new(&consumer_config)
        .await
        .unwrap_or_else(|e| panic!("consumer init against {}: {e}", kf.brokers));
    let mut frames: Vec<Vec<u8>> = Vec::new();
    let mut assigned: Option<Duration> = None;
    let mut last_error: Option<String> = None;
    while !done(&frames) {
        match consumer.recv(100).await {
            Ok(batch) => frames.extend(batch.records.into_iter().map(|r| r.payload.to_vec())),
            Err(e) => last_error = Some(e.to_string()),
        }
        let stats = consumer.stats();
        // `up` is only the coordinator link and the first rebalance is the
        // assignment. A frame read proves one the statistics may not show yet.
        let joined =
            stats.consumer_group_state.as_deref() == Some("up") && stats.rebalance_count > 0;
        if assigned.is_none() && (joined || !frames.is_empty()) {
            assigned = Some(started.elapsed());
        }
        let failure = match assigned {
            None if started.elapsed() > ceilings.assigned => Some(format!(
                "consumer group `{group}` was not assigned `{topic}` within {:?}: \
                 group state {:?}, {} rebalance(s), last receive error {last_error:?}",
                ceilings.assigned, stats.consumer_group_state, stats.rebalance_count,
            )),
            Some(at)
                if started.elapsed().saturating_sub(at) > ceilings.frames && !done(&frames) =>
            {
                let ends: std::collections::BTreeMap<i32, i64> = stats
                    .partition_high_watermark
                    .iter()
                    .filter(|((t, _), _)| t == topic)
                    .map(|((_, partition), end)| (*partition, *end))
                    .collect();
                Some(format!(
                    "consumer group `{group}` was assigned `{topic}` after {at:.1?}, then {} \
                     frame(s) arrived in {:?}, still waiting for {wanted}: partition ends \
                     {ends:?}, last receive error {last_error:?}",
                    frames.len(),
                    ceilings.frames,
                ))
            }
            _ => None,
        };
        if let Some(message) = failure {
            let _ = consumer.close().await;
            panic!("{message}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = consumer.close().await;
    frames
}

/// Read `want` raw frames from `topic`; see [`consume_until`].
pub async fn consume_frames(
    kf: &KafkaTestConfig,
    topic: &str,
    group: &str,
    want: usize,
) -> Vec<Vec<u8>> {
    consume_until(
        kf,
        topic,
        group,
        ConsumeCeilings::default(),
        &format!("{want} frames"),
        |frames| frames.len() >= want,
    )
    .await
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

/// A database a test can dump: where it listens, and the container a test
/// started for itself (kept alive for the test's duration), `None` for the
/// run's shared one.
pub struct DatabaseTestConfig {
    pub host: String,
    pub port: u16,
    pub _container: Option<TestcontainerHolder>,
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

    let name = container_name(test, "postgres");
    let container = match Postgres::default()
        .with_init_sql(init_sql.to_owned().into_bytes())
        .with_tag(format!("{POSTGRES_TAG}@{POSTGRES_DIGEST}"))
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
        _container: Some(TestcontainerHolder::Postgres(container)),
    })
}

/// Start a MariaDB container seeded with `init_sql`; user `root` with no
/// password, database `test`.
pub async fn acquire_mariadb(test: &str, init_sql: &str) -> Option<DatabaseTestConfig> {
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::mariadb::Mariadb;

    let name = container_name(test, "mariadb");
    let container = match Mariadb::default()
        .with_init_sql(init_sql.to_owned().into_bytes())
        .with_tag(format!("{MARIADB_TAG}@{MARIADB_DIGEST}"))
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
        _container: Some(TestcontainerHolder::Mariadb(container)),
    })
}

/// The run's shared ClickHouse; user `default` with no password, HTTP on
/// the returned port. Tests keep apart by their own tables.
pub async fn acquire_clickhouse(_test: &str) -> Option<DatabaseTestConfig> {
    static CLICKHOUSE: OnceLock<SharedContainer> = OnceLock::new();
    let container = shared(&CLICKHOUSE, clickhouse_spec).await?;
    Some(DatabaseTestConfig {
        host: container.host.clone(),
        port: container.port(8123),
        _container: None,
    })
}

/// The run's shared MongoDB: a standalone server, or a one-member replica set
/// when `replica_set` is set, which is what a change stream needs. No auth.
/// Tests keep apart by their own database ([`mongo_database`]). A replica set
/// needs `?directConnection=true` on the URI because the member advertises
/// its container hostname, which the host cannot resolve.
pub async fn acquire_mongo(_test: &str, replica_set: bool) -> Option<DatabaseTestConfig> {
    static MONGO: OnceLock<SharedContainer> = OnceLock::new();
    static MONGO_RS: OnceLock<SharedContainer> = OnceLock::new();
    let container = if replica_set {
        shared(&MONGO_RS, mongo_replica_set_spec).await?
    } else {
        shared(&MONGO, mongo_spec).await?
    };
    Some(DatabaseTestConfig {
        host: container.host.clone(),
        port: container.port(27017),
        _container: None,
    })
}

/// The database a MongoDB test owns on the shared server.
#[must_use]
pub fn mongo_database(test: &str) -> String {
    format!(
        "inventory_{}",
        test.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
    )
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

    let name = container_name(test, "mssql");
    let image = GenericImage::new(
        "mcr.microsoft.com/mssql/server".to_string(),
        format!("{MSSQL_TAG}@{MSSQL_DIGEST}"),
    )
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
        _container: Some(TestcontainerHolder::Generic(container)),
    })
}

/// Start an Oracle Database Free container; user `dfe` with the returned
/// password on service `FREEPDB1`, port 1521.
pub async fn acquire_oracle(test: &str, password: &str) -> Option<DatabaseTestConfig> {
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    let name = container_name(test, "oracle");
    let image = GenericImage::new(
        "gvenzl/oracle-free".to_string(),
        format!("{ORACLE_TAG}@{ORACLE_DIGEST}"),
    )
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
        _container: Some(TestcontainerHolder::Generic(container)),
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
