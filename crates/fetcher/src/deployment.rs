// Project:   dfe-fetcher
// File:      crates/fetcher/src/deployment.rs
// Purpose:   Deployment contract for artifact generation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract for dfe-fetcher.
//!
//! Builds a [`DeploymentContract`] that drives generation of Dockerfile,
//! Helm chart, and Docker Compose fragments via `scalo`.

use std::path::{Path, PathBuf};

use scalo::deployment::{
    DeploymentContract, HealthContract, ImageProfile, NativeDepsContract, PortContract,
    SecretEnvContract, SecretGroupContract, base_image_from_cascade, image_registry_from_cascade,
};

/// The repository root, where the operator-facing files live: the committed
/// config schema and catalog under `docs/`, `config.example.yaml`, the chart
/// and the Dockerfile. The app crate sits two directories below it.
#[must_use]
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map_or_else(
            || PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            Path::to_path_buf,
        )
}

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
        // Each listener binds only while its switch is on, so its port is gated
        // on the same switch and names the address it serves.
        extra_ports: vec![
            PortContract::tcp("ingest", 8080)
                .when_enabled("config.ingest.enabled")
                .bound_from("ingest.bind_address"),
            PortContract::tcp("vector-grpc", 6000)
                .when_enabled("config.extractors.vector.enabled")
                .bound_from("extractors.vector.grpc_bind_address"),
        ],
        unbound_listen_paths: vec![],
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
            // Matches `IngestConfig::default`: the listener counts as work, so
            // shipping it on would hold every generated deployment out of idle.
            "ingest": {
                "enabled": false,
                "bind_address": "0.0.0.0:8080"
            },
            // Matches `VectorExtractorConfig::default`, and carries the address
            // and switch the vector-grpc port is bound from and gated on.
            "extractors": {
                "vector": {
                    "enabled": false,
                    "grpc_bind_address": "0.0.0.0:6000"
                }
            },
            "metrics": {
                "enabled": true,
                "address": "0.0.0.0:9090"
            }
        })),
        depends_on: vec!["kafka".into()],
        // The fetcher polls its upstreams rather than draining a queue, so it
        // never scales out and the chart carries no ScaledObject.
        keda: None,
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
    fn the_contract_default_agrees_with_the_code_default_on_vector() {
        let c = contract();
        let default_config = c
            .default_config
            .as_ref()
            .expect("contract must carry a default config");
        let vector = &default_config["extractors"]["vector"];
        let code = crate::config::VectorExtractorConfig::default();

        assert_eq!(vector["enabled"].as_bool(), Some(code.enabled));
        assert_eq!(
            vector["grpc_bind_address"].as_str(),
            Some(code.grpc_bind_address.as_str())
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
        assert_eq!(
            ingest.when.as_ref().map(|w| w.path()),
            Some("config.ingest.enabled")
        );
        assert_eq!(ingest.bound_from.as_deref(), Some("ingest.bind_address"));
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
        assert_eq!(
            vector.when.as_ref().map(|w| w.path()),
            Some("config.extractors.vector.enabled")
        );
        assert_eq!(
            vector.bound_from.as_deref(),
            Some("extractors.vector.grpc_bind_address")
        );
    }

    /// generate-artefacts refuses a contract whose default config binds a
    /// listener no port declares.
    #[test]
    fn every_listener_has_a_port() {
        scalo::deployment::assert_listeners_declared(&contract());
    }

    /// A values path the chart reads but the default config never sets renders
    /// empty, so every one must resolve.
    #[test]
    fn every_values_path_the_chart_reads_resolves() {
        assert_eq!(contract().unresolved_values_paths(), Vec::<String>::new());
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

    /// No lag trigger and no CPU trigger means no ScaledObject at all, which
    /// scalo spells `keda: None`.
    #[test]
    fn the_chart_does_not_autoscale() {
        assert!(contract().keda.is_none());
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
        assert_eq!(
            c.capabilities.len(),
            crate::config::REGISTRY.len(),
            "one capability per registry block"
        );
    }

    /// Property names that hold a secret value wherever the config declares them.
    /// `service_account_key` is not one: the typed blocks hold a key file path
    /// under that name.
    const SECRET_FIELDS: &[&str] = &[
        "auth_token",
        "password",
        "private_key",
        "client_secret",
        "secret_access_key",
        "secret_key",
        "token",
        "account_key",
        "sas_token",
    ];

    /// Every property declared in `schema` at any depth, as its name and whether
    /// its own object carries `x-dfe-secret`.
    fn declared_properties(schema: &serde_json::Value) -> Vec<(String, bool)> {
        let mut found = Vec::new();
        let mut stack = vec![schema];
        while let Some(node) = stack.pop() {
            match node {
                serde_json::Value::Object(map) => {
                    if let Some(serde_json::Value::Object(props)) = map.get("properties") {
                        for (name, prop) in props {
                            let marked =
                                prop.get("x-dfe-secret") == Some(&serde_json::Value::Bool(true));
                            found.push((name.clone(), marked));
                        }
                    }
                    stack.extend(map.values());
                }
                serde_json::Value::Array(items) => stack.extend(items),
                _ => {}
            }
        }
        found
    }

    /// The secret fields `schema` declares at least once without the marker.
    fn unmarked_secret_fields(schema: &serde_json::Value) -> std::collections::BTreeSet<String> {
        declared_properties(schema)
            .into_iter()
            .filter(|(name, marked)| !marked && SECRET_FIELDS.contains(&name.as_str()))
            .map(|(name, _)| name)
            .collect()
    }

    /// Every declaration of every secret field carries the marker, because the
    /// engine's config composer reads this schema to decide what becomes a
    /// Secret rather than a plaintext ConfigMap entry. Each listed field must be
    /// declared somewhere, so the list cannot rot into names nothing uses.
    #[test]
    fn every_secret_field_carries_the_marker_wherever_it_is_declared() {
        let schema = contract().config_schema.expect("config_schema");
        assert_eq!(
            unmarked_secret_fields(&schema),
            std::collections::BTreeSet::new(),
            "type these as `SensitiveString`, not `String`"
        );
        let declared: std::collections::BTreeSet<String> = declared_properties(&schema)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        for field in SECRET_FIELDS {
            assert!(
                declared.contains(*field),
                "`{field}` is declared nowhere in the config -- drop it from SECRET_FIELDS"
            );
        }
    }

    /// The check fails for a secret held as a plain string and passes for the
    /// same field held as a `SensitiveString`.
    #[test]
    fn a_secret_field_typed_as_a_plain_string_fails_the_check() {
        #[derive(schemars::JsonSchema)]
        #[allow(dead_code)]
        struct Leaky {
            password: Option<String>,
        }
        #[derive(schemars::JsonSchema)]
        #[allow(dead_code)]
        struct Guarded {
            password: Option<scalo::config::sensitive::SensitiveString>,
        }
        let leaky = serde_json::to_value(schemars::schema_for!(Leaky)).expect("schema");
        let guarded = serde_json::to_value(schemars::schema_for!(Guarded)).expect("schema");
        assert_eq!(
            unmarked_secret_fields(&leaky),
            std::collections::BTreeSet::from(["password".to_owned()])
        );
        assert_eq!(
            unmarked_secret_fields(&guarded),
            std::collections::BTreeSet::new()
        );
    }

    /// `repo_root` is the workspace root, not the app crate's directory.
    #[test]
    fn repo_root_is_the_workspace_root() {
        let root = repo_root();
        let manifest = std::fs::read_to_string(root.join("Cargo.toml")).expect("root manifest");
        assert!(
            manifest.contains("[workspace]"),
            "{} is not the workspace root",
            root.display()
        );
        assert!(root.join("config.example.yaml").is_file());
        assert_ne!(root, std::path::Path::new(env!("CARGO_MANIFEST_DIR")));
    }

    /// The committed reflectable artefacts under docs/ must not drift from a
    /// fresh regeneration of the current Config + catalog. Regenerate with
    /// `dfe-fetcher config-schema --dir docs` (or generate-artefacts) and commit.
    #[test]
    fn test_config_artifacts_do_not_drift() {
        let dir = repo_root().join("docs");
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

    /// Map a chart directory to relative path -> file body.
    fn chart_files(root: &std::path::Path) -> std::collections::BTreeMap<String, String> {
        let mut files = std::collections::BTreeMap::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read_dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let rel = path.strip_prefix(root).expect("relative path");
                    files.insert(
                        rel.display().to_string(),
                        std::fs::read_to_string(&path).expect("read chart file"),
                    );
                }
            }
        }
        files
    }

    #[test]
    fn checked_in_chart_matches_generated() {
        // A deployment installs the committed chart, not a freshly generated
        // one, so drift means the cluster gets whatever the stale file says.
        const REGEN: &str = "regenerate with: `dfe-fetcher emit-chart chart`";

        let tmp = tempfile::tempdir().expect("tempdir");
        scalo::deployment::generate_chart(&contract(), tmp.path(), None).expect("generate_chart");
        let generated = chart_files(tmp.path());
        let committed = chart_files(&repo_root().join("chart"));

        let generated_names: Vec<&String> = generated.keys().collect();
        let committed_names: Vec<&String> = committed.keys().collect();
        assert_eq!(
            committed_names, generated_names,
            "chart/ file list differs from the contract -- {REGEN}"
        );
        for (name, want) in &generated {
            assert_eq!(
                committed.get(name),
                Some(want),
                "chart/{name} differs from the contract -- {REGEN}"
            );
        }
    }

    use scalo::deployment::generate_dockerfile;

    #[test]
    fn checked_in_dockerfile_matches_generated() {
        // The committed Dockerfile is generated from this contract, and the
        // image is built from the committed copy -- so if the two drift, CI
        // ships whatever the stale file says. A contract change that never got
        // regenerated (base image, licence label, health path) would otherwise
        // reach production silently.
        let path = repo_root().join("Dockerfile");
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
