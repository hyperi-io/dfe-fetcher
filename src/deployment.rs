// Project:   dfe-fetcher
// File:      src/deployment.rs
// Purpose:   Deployment contract for artifact generation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract for dfe-fetcher.
//!
//! Builds a [`DeploymentContract`] that drives generation of Dockerfile,
//! Helm chart, and Docker Compose fragments via `scalo`.

use scalo::deployment::{
    DeploymentContract, HealthContract, ImageProfile, KedaConfig, KedaContract, NativeDepsContract,
    PortContract, SecretEnvContract, SecretGroupContract, base_image_from_cascade,
    image_registry_from_cascade,
};

/// Build the deployment contract for dfe-fetcher.
///
/// This captures all deployment-facing configuration: ports, health paths,
/// secrets, KEDA scaling, and default config. Artifact generators
/// (`generate_dockerfile`, `generate_chart`, `generate_compose_fragment`)
/// use this contract as their single source of truth.
pub fn contract() -> DeploymentContract {
    // Resolve base image + registry via the scalo cascade helpers so
    // org-wide overrides in deployment.* config keys (or env) win
    // before we fall back to scalo's DEFAULT_BASE_IMAGE / DEFAULT_IMAGE_REGISTRY.
    let base_image = base_image_from_cascade();
    let image_registry = image_registry_from_cascade();
    DeploymentContract {
        app_name: "dfe-fetcher".into(),
        binary_name: "dfe-fetcher".into(),
        native_deps: NativeDepsContract::for_scalo_features(
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
            &base_image,
        ),
        base_image,
        image_profile: ImageProfile::Production,
        description: "Data fetcher for external services (AWS, Azure, M365, GCP)".into(),
        metrics_port: 9090,
        health: HealthContract {
            liveness_path: "/livez".into(),
            readiness_path: "/readyz".into(),
            metrics_path: "/metrics".into(),
        },
        env_prefix: "DFE_FETCHER".into(),
        metric_prefix: "fetcher".into(),
        config_mount_path: "/etc/dfe/fetcher.yaml".into(),
        image_registry,
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
                "enabled": false,
                "bind_address": "0.0.0.0:8080"
            },
            "metrics": {
                "enabled": true,
                "address": "0.0.0.0:9090"
            }
        })),
        depends_on: vec!["kafka".into()],
        // `KedaContract` is `#[non_exhaustive]` (scalo 2.8.13) so it can no
        // longer be built via a struct literal. Construct a `KedaConfig` with
        // the fetcher's real KEDA values and convert via `from_config`;
        // `..Default::default()` fills the rest (the 2.8.12 scaling-pressure
        // trigger stays OFF -- it needs a cluster-specific Prometheus
        // serverAddress before enabling).
        keda: Some(KedaContract::from_config(&KedaConfig {
            min_replicas: 1,
            max_replicas: 5,
            polling_interval: 30,
            cooldown_period: 300,
            kafka_lag_threshold: 5000,
            activation_lag_threshold: 0,
            cpu_enabled: false,
            cpu_threshold: 80,
            ..Default::default()
        })),
        schema_version: 3,
        // dfe-fetcher is BUSL-1.1 (scalo itself is Apache-2.0). Drive the OCI
        // licenses label + the generated Dockerfile's `# License` header from the
        // contract so a regen never stamps Apache into this BUSL repo.
        oci_labels: scalo::deployment::OciLabels {
            licenses: "BUSL-1.1".into(),
            ..Default::default()
        },
        // Reflectable config (scalo-rs#6): the derived JSON Schema of the full
        // multi-endpoint `Config` (secret fields carry `x-dfe-secret`) plus the
        // hand-authored capability catalog schemars cannot derive (service names
        // + their knobs). Emitted to docs/config-schema.* + docs/capability-catalog.*.
        config_schema: Some(scalo::deployment::config_schema_json::<crate::config::Config>()),
        capabilities: crate::deployment_catalog::capabilities(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contract_app_name() {
        let c = contract();
        assert_eq!(c.app_name, "dfe-fetcher");
    }

    #[test]
    fn test_contract_binary_name() {
        let c = contract();
        assert_eq!(c.binary_name, "dfe-fetcher");
    }

    #[test]
    fn test_contract_base_image() {
        // The cascade helper resolves to the org-wide `deployment.base_image`
        // override (config or env) when set, else falls back to scalo's
        // DEFAULT_BASE_IMAGE. In CI / local-dev with no overrides the
        // default applies. Just assert the value is non-empty and well-formed.
        let c = contract();
        assert!(!c.base_image.is_empty());
        assert!(
            c.base_image.contains(':'),
            "base_image must include an explicit tag: {}",
            c.base_image
        );
    }

    #[test]
    fn test_contract_metrics_port() {
        let c = contract();
        assert_eq!(c.metrics_port, 9090);
    }

    #[test]
    fn test_contract_health_liveness_path() {
        let c = contract();
        assert_eq!(c.health.liveness_path, "/livez");
    }

    #[test]
    fn test_contract_health_readiness_path() {
        let c = contract();
        assert_eq!(c.health.readiness_path, "/readyz");
    }

    #[test]
    fn test_contract_health_metrics_path() {
        let c = contract();
        assert_eq!(c.health.metrics_path, "/metrics");
    }

    #[test]
    fn test_contract_env_prefix() {
        let c = contract();
        assert_eq!(c.env_prefix, "DFE_FETCHER");
    }

    #[test]
    fn test_contract_config_mount_path() {
        let c = contract();
        assert_eq!(c.config_mount_path, "/etc/dfe/fetcher.yaml");
    }

    #[test]
    fn test_contract_extra_ports_count() {
        let c = contract();
        assert_eq!(c.extra_ports.len(), 2);
    }

    #[test]
    fn the_contract_default_agrees_with_the_code_default_on_ingest() {
        // Changing IngestConfig::default() alone changes nothing a deployment
        // sees: the chart's config block and this contract default both
        // override it. Flipping one and not the others is how a security
        // default gets fixed on paper and left open in the cluster.
        let c = contract();
        let default_config = c
            .default_config
            .as_ref()
            .expect("contract must carry a default config");
        let contract_enabled = default_config["ingest"]["enabled"]
            .as_bool()
            .expect("ingest.enabled must be a bool in the contract default");

        assert_eq!(
            contract_enabled,
            crate::config::IngestConfig::default().enabled,
            "the contract's ingest.enabled must track IngestConfig::default()"
        );
    }

    #[test]
    fn test_contract_ingest_port() {
        let c = contract();
        let ingest = c
            .extra_ports
            .iter()
            .find(|p| p.name == "ingest")
            .expect("ingest port must exist");
        assert_eq!(ingest.port, 8080);
        assert_eq!(ingest.protocol, "TCP");
    }

    #[test]
    fn test_contract_vector_grpc_port() {
        let c = contract();
        let vector = c
            .extra_ports
            .iter()
            .find(|p| p.name == "vector-grpc")
            .expect("vector-grpc port must exist");
        assert_eq!(vector.port, 6000);
        assert_eq!(vector.protocol, "TCP");
    }

    #[test]
    fn test_contract_secret_groups_count() {
        let c = contract();
        assert_eq!(c.secrets.len(), 5);
    }

    #[test]
    fn test_contract_secret_group_kafka() {
        let c = contract();
        let group = c
            .secrets
            .iter()
            .find(|g| g.group_name == "kafka")
            .expect("kafka secret group must exist");
        let env_vars: Vec<&str> = group.env_vars.iter().map(|e| e.env_var.as_str()).collect();
        assert!(env_vars.contains(&"DFE_FETCHER__KAFKA__SASL__USERNAME"));
        assert!(env_vars.contains(&"DFE_FETCHER__KAFKA__SASL__PASSWORD"));
    }

    #[test]
    fn test_contract_secret_group_aws() {
        let c = contract();
        let group = c
            .secrets
            .iter()
            .find(|g| g.group_name == "aws")
            .expect("aws secret group must exist");
        let env_vars: Vec<&str> = group.env_vars.iter().map(|e| e.env_var.as_str()).collect();
        assert!(env_vars.contains(&"DFE_FETCHER__SOURCES__AWS__ACCESS_KEY_ID"));
        assert!(env_vars.contains(&"DFE_FETCHER__SOURCES__AWS__SECRET_ACCESS_KEY"));
    }

    #[test]
    fn test_contract_secret_group_azure() {
        let c = contract();
        let group = c
            .secrets
            .iter()
            .find(|g| g.group_name == "azure")
            .expect("azure secret group must exist");
        let env_vars: Vec<&str> = group.env_vars.iter().map(|e| e.env_var.as_str()).collect();
        assert!(env_vars.contains(&"DFE_FETCHER__SOURCES__AZURE__CLIENT_ID"));
        assert!(env_vars.contains(&"DFE_FETCHER__SOURCES__AZURE__CLIENT_SECRET"));
    }

    #[test]
    fn test_contract_secret_group_m365() {
        let c = contract();
        let group = c
            .secrets
            .iter()
            .find(|g| g.group_name == "m365")
            .expect("m365 secret group must exist");
        let env_vars: Vec<&str> = group.env_vars.iter().map(|e| e.env_var.as_str()).collect();
        assert!(env_vars.contains(&"DFE_FETCHER__SOURCES__M365__CLIENT_ID"));
        assert!(env_vars.contains(&"DFE_FETCHER__SOURCES__M365__CLIENT_SECRET"));
    }

    #[test]
    fn test_contract_secret_group_gcp() {
        let c = contract();
        let group = c
            .secrets
            .iter()
            .find(|g| g.group_name == "gcp")
            .expect("gcp secret group must exist");
        assert_eq!(group.env_vars.len(), 1);
        assert_eq!(
            group.env_vars[0].env_var,
            "DFE_FETCHER__SOURCES__GCP__SERVICE_ACCOUNT_KEY"
        );
    }

    #[test]
    fn test_contract_default_config_is_some() {
        let c = contract();
        assert!(c.default_config.is_some());
    }

    #[test]
    fn test_contract_default_config_scheduler_defaults() {
        let c = contract();
        let cfg = c
            .default_config
            .as_ref()
            .expect("default_config must exist");
        assert_eq!(cfg["scheduler"]["default_interval_secs"], 300);
        assert_eq!(cfg["scheduler"]["max_concurrent_fetches"], 10);
        assert_eq!(cfg["scheduler"]["jitter_percent"], 10);
    }

    #[test]
    fn test_contract_keda_present() {
        let c = contract();
        assert!(c.keda.is_some());
    }

    #[test]
    fn test_contract_keda_defaults() {
        let c = contract();
        let keda = c.keda.as_ref().expect("keda must exist");
        assert_eq!(keda.min_replicas, 1);
        assert_eq!(keda.max_replicas, 5);
        assert_eq!(keda.polling_interval, 30);
        assert_eq!(keda.cooldown_period, 300);
        assert_eq!(keda.kafka_lag_threshold, 5000);
    }

    #[test]
    fn test_contract_schema_version() {
        let c = contract();
        assert_eq!(c.schema_version, 3);
    }

    #[test]
    fn test_contract_carries_config_schema() {
        let c = contract();
        assert!(c.config_schema.is_some(), "config_schema must be populated");
    }

    #[test]
    fn test_contract_carries_capabilities() {
        let c = contract();
        assert_eq!(c.capabilities.len(), 19, "one capability per source type");
    }

    /// The derived schema must mark the fetcher's secret fields with the
    /// `x-dfe-secret` marker (via scalo's SensitiveString JsonSchema impl).
    #[test]
    fn test_config_schema_marks_secrets() {
        let c = contract();
        let schema = c.config_schema.expect("config_schema");
        let json = serde_json::to_string(&schema).expect("serialise schema");
        assert!(
            json.contains("x-dfe-secret"),
            "schema must carry the x-dfe-secret marker on secret fields"
        );
    }

    /// The committed reflectable artefacts under docs/ must not drift from a
    /// fresh regeneration of the current Config + catalog. Regenerate with
    /// `dfe-fetcher config-schema --dir docs` (or generate-artefacts) and commit.
    #[test]
    fn test_config_artifacts_do_not_drift() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs");
        scalo::deployment::assert_no_config_artifact_drift(&contract(), dir);
    }

    #[test]
    fn test_contract_to_json_valid() {
        let c = contract();
        let json = serde_json::to_string(&c).expect("contract must serialise to JSON");
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("serialised JSON must parse");
        assert_eq!(parsed["app_name"], "dfe-fetcher");
    }

    #[test]
    fn test_contract_entrypoint_args_contain_config() {
        let c = contract();
        assert!(
            c.entrypoint_args.contains(&"--config".into()),
            "entrypoint_args must contain --config"
        );
    }

    #[test]
    fn test_contract_entrypoint_args_contain_config_path() {
        let c = contract();
        assert!(
            c.entrypoint_args.contains(&"/etc/dfe/fetcher.yaml".into()),
            "entrypoint_args must contain the config mount path"
        );
    }

    use scalo::deployment::generate_dockerfile;

    #[test]
    fn checked_in_dockerfile_matches_generated() {
        // The committed Dockerfile is generated from this contract, and the
        // image is built from the committed copy -- so if the two drift, CI
        // ships whatever the stale file says. A contract change that never got
        // regenerated (base image, licence label, health path) would otherwise
        // reach production silently.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Dockerfile");
        let on_disk = std::fs::read_to_string(&path).expect("read Dockerfile");
        let generated = generate_dockerfile(&contract(), None);
        assert_eq!(
            on_disk.trim(),
            generated.trim(),
            "Dockerfile on disk does not match the contract -- regenerate with: \
             `cargo run --bin dfe-fetcher -- emit-dockerfile > Dockerfile`",
        );
    }
}
