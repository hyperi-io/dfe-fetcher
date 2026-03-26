// Project:   dfe-fetcher
// File:      tests/common/mod.rs
// Purpose:   Shared test infrastructure for dual-mode (remote/docker) testing
// Language:  Rust
//
// License:   FSL-1.1-ALv2
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
    pub fn to_rustlib_config(&self) -> hyperi_rustlib::transport::KafkaConfig {
        let mut config = hyperi_rustlib::transport::KafkaConfig {
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
            config.sasl_password = Some(password.clone());
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
