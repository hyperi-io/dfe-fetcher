// Project:   dfe-fetcher
// File:      src/deployment.rs
// Purpose:   Deployment contract for artifact generation
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract for dfe-fetcher.
//!
//! Builds a [`DeploymentContract`] that drives generation of Dockerfile,
//! Helm chart, and Docker Compose fragments via `hyperi-rustlib`.

use hyperi_rustlib::deployment::{
    DeploymentContract, HealthContract, ImageProfile, KedaContract, NativeDepsContract,
    PortContract, SecretEnvContract, SecretGroupContract,
};

/// Build the deployment contract for dfe-fetcher.
///
/// This captures all deployment-facing configuration: ports, health paths,
/// secrets, KEDA scaling, and default config. Artifact generators
/// (`generate_dockerfile`, `generate_chart`, `generate_compose_fragment`)
/// use this contract as their single source of truth.
pub fn contract() -> DeploymentContract {
    DeploymentContract {
        app_name: "dfe-fetcher".into(),
        binary_name: "dfe-fetcher".into(),
        base_image: "ubuntu:24.04".into(),
        native_deps: NativeDepsContract::for_rustlib_features(
            &[
                "config",
                "config-reload",
                "logger",
                "metrics",
                "http-server",
                "transport-kafka",
                "transport-grpc",
                "transport-grpc-vector-compat",
                "spool",
                "tiered-sink",
                "runtime",
                "secrets",
                "dlq",
                "deployment",
                "cli",
            ],
            "ubuntu:24.04",
        ),
        image_profile: ImageProfile::Production,
        description: "Data fetcher for external services (AWS, Azure, M365, GCP)".into(),
        metrics_port: 9090,
        health: HealthContract {
            liveness_path: "/health/live".into(),
            readiness_path: "/health/ready".into(),
            metrics_path: "/metrics".into(),
        },
        env_prefix: "DFE_FETCHER".into(),
        metric_prefix: "fetcher".into(),
        config_mount_path: "/etc/dfe/fetcher.yaml".into(),
        image_registry: "ghcr.io/hyperi-io".into(),
        extra_ports: vec![
            PortContract {
                name: "ingest".into(),
                port: 8080,
                protocol: "TCP".into(),
            },
            PortContract {
                name: "vector-grpc".into(),
                port: 6000,
                protocol: "TCP".into(),
            },
        ],
        entrypoint_args: vec!["--config".into(), "/etc/dfe/fetcher.yaml".into()],
        secrets: vec![
            SecretGroupContract {
                group_name: "kafka".into(),
                env_vars: vec![
                    SecretEnvContract {
                        env_var: "DFE_FETCHER__KAFKA__SASL__USERNAME".into(),
                        key_name: "username".into(),
                        secret_key: "kafka-username".into(),
                    },
                    SecretEnvContract {
                        env_var: "DFE_FETCHER__KAFKA__SASL__PASSWORD".into(),
                        key_name: "password".into(),
                        secret_key: "kafka-password".into(),
                    },
                ],
            },
            SecretGroupContract {
                group_name: "aws".into(),
                env_vars: vec![
                    SecretEnvContract {
                        env_var: "DFE_FETCHER__SOURCES__AWS__ACCESS_KEY_ID".into(),
                        key_name: "access-key-id".into(),
                        secret_key: "aws-access-key-id".into(),
                    },
                    SecretEnvContract {
                        env_var: "DFE_FETCHER__SOURCES__AWS__SECRET_ACCESS_KEY".into(),
                        key_name: "secret-access-key".into(),
                        secret_key: "aws-secret-access-key".into(),
                    },
                ],
            },
            SecretGroupContract {
                group_name: "azure".into(),
                env_vars: vec![
                    SecretEnvContract {
                        env_var: "DFE_FETCHER__SOURCES__AZURE__CLIENT_ID".into(),
                        key_name: "client-id".into(),
                        secret_key: "azure-client-id".into(),
                    },
                    SecretEnvContract {
                        env_var: "DFE_FETCHER__SOURCES__AZURE__CLIENT_SECRET".into(),
                        key_name: "client-secret".into(),
                        secret_key: "azure-client-secret".into(),
                    },
                ],
            },
            SecretGroupContract {
                group_name: "m365".into(),
                env_vars: vec![
                    SecretEnvContract {
                        env_var: "DFE_FETCHER__SOURCES__M365__CLIENT_ID".into(),
                        key_name: "client-id".into(),
                        secret_key: "m365-client-id".into(),
                    },
                    SecretEnvContract {
                        env_var: "DFE_FETCHER__SOURCES__M365__CLIENT_SECRET".into(),
                        key_name: "client-secret".into(),
                        secret_key: "m365-client-secret".into(),
                    },
                ],
            },
            SecretGroupContract {
                group_name: "gcp".into(),
                env_vars: vec![SecretEnvContract {
                    env_var: "DFE_FETCHER__SOURCES__GCP__SERVICE_ACCOUNT_KEY".into(),
                    key_name: "service-account-key".into(),
                    secret_key: "gcp-service-account-key".into(),
                }],
            },
        ],
        default_config: Some(serde_json::json!({
            "scheduler": {
                "default_interval_secs": 300,
                "max_concurrent_fetches": 10,
                "jitter_percent": 10
            },
            "sources": {
                "aws": { "enabled": false, "region": "us-east-1", "topic": "aws" },
                "azure": { "enabled": false, "topic": "azure" },
                "m365": { "enabled": false, "topic": "m365" },
                "gcp": { "enabled": false, "topic": "gcp" }
            },
            "kafka": {
                "brokers": ["kafka:9092"],
                "client_id": "dfe-fetcher",
                "topic_suffix": "_land",
                "producer": {
                    "compression": "zstd",
                    "acks": "all"
                }
            },
            "ingest": {
                "enabled": true,
                "bind_address": "0.0.0.0:8080"
            },
            "metrics": {
                "enabled": true,
                "address": "0.0.0.0:9090"
            }
        })),
        depends_on: vec!["kafka".into()],
        keda: Some(KedaContract {
            min_replicas: 1,
            max_replicas: 5,
            polling_interval: 30,
            cooldown_period: 300,
            kafka_lag_threshold: 5000,
            activation_lag_threshold: 0,
            cpu_enabled: false,
            cpu_threshold: 80,
        }),
        schema_version: 2,
        oci_labels: hyperi_rustlib::deployment::OciLabels::default(),
    }
}
