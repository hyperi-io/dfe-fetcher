// Project:   dfe-fetcher
// File:      crates/fetcher/src/deployment.rs
// Purpose:   Deployment contract for artifact generation
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract for dfe-fetcher.
//!
//! Builds a [`DeploymentContract`] that drives generation of the Dockerfile
//! and the Docker Compose fragment via `scalo`, and that the release emits for
//! the thin chart it assembles on the scalo-service library chart.

use std::path::{Path, PathBuf};

use scalo::deployment::{
    CONTRACT_SCHEMA_VERSION, DeploymentContract, HealthContract, ImageProfile, NativeDepsContract,
    PortContract, ResourceList, ResourcesContract, SecretEnvContract, SecretGroupContract,
    SecurityContract, WritablePath, base_image_from_cascade, image_registry_from_cascade,
};

/// Where the cursor store keeps its files, on the contract's persistent
/// writable path `cursor`.
const CURSOR_DIR: &str = "/var/lib/dfe-fetcher";

/// The repository root, where the operator-facing files live: the committed
/// config schema and catalog under `docs/`, `config.example.yaml` and the
/// Dockerfile. The app crate sits two directories below it.
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
/// secrets, writable paths, resources, KEDA scaling, and default config.
/// Artifact generators (`generate_dockerfile`, `generate_chart`,
/// `generate_compose_fragment`) and the released thin chart use this contract
/// as their single source of truth.
pub fn contract() -> DeploymentContract {
    // A deployment.* cascade key (or env) wins over the base image and registry
    // defaults; scalo names no default registry, so the published one is ours.
    let base_image = base_image_from_cascade();
    let image_registry =
        image_registry_from_cascade().unwrap_or_else(|| "ghcr.io/hyperi-io".into());
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
            startup_budget_seconds: 120,
            ..HealthContract::default()
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
            // Cleartext gRPC, so a proxy in front of it must speak h2c.
            PortContract::tcp("vector-grpc", 6000)
                .when_enabled("config.extractors.vector.enabled")
                .bound_from("extractors.vector.grpc_bind_address")
                .app_protocol("kubernetes.io/h2c"),
        ],
        unbound_listen_paths: vec![],
        entrypoint_args: vec!["--config".into(), "/etc/dfe/fetcher.yaml".into()],
        secrets: secrets(),
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
            },
            // Left empty, the cursor store falls back to the config file's
            // read-only mount and loses every cursor on a restart.
            "cursor": {
                "directory": CURSOR_DIR
            }
        })),
        depends_on: vec!["kafka".into()],
        // The fetcher polls its upstreams rather than draining a queue, so it
        // never scales out and the chart carries no ScaledObject.
        keda: None,
        schema_version: CONTRACT_SCHEMA_VERSION,
        // scalo writes no vendor, licence or copyright of its own, so the
        // labels and the generated Dockerfile header carry exactly these.
        oci_labels: scalo::deployment::OciLabels {
            vendor: "HYPERI PTY LIMITED".into(),
            label_namespace: "io.hyperi".into(),
            licenses: "BUSL-1.1".into(),
            copyright: "(c) 2026 HYPERI PTY LIMITED".into(),
            ..Default::default()
        },
        // Reflectable config (scalo-rs#6): the derived JSON Schema of the full
        // multi-endpoint `Config` (secret fields carry `x-scalo-secret`) plus the
        // hand-authored capability catalog schemars cannot derive (service names
        // + their knobs). Emitted to docs/config-schema.* + docs/capability-catalog.*.
        config_schema: Some(scalo::deployment::config_schema_json::<crate::config::Config>()),
        capabilities: crate::deployment_catalog::capabilities(),
        // The cursor store's files outlive the pod, so a restart resumes each
        // connection's window instead of re-fetching the default one.
        writable_paths: vec![WritablePath::new("cursor", CURSOR_DIR).persistent("1Gi")],
        termination_grace_seconds: 45,
        resources: ResourcesContract {
            requests: ResourceList {
                cpu: "100m".into(),
                memory: "128Mi".into(),
            },
            limits: ResourceList {
                cpu: "500m".into(),
                memory: "256Mi".into(),
            },
        },
        security: SecurityContract::default(),
        singleton: false,
    }
}

/// The Kafka Secret the chart mounts as env vars.
///
/// `Config::apply_flat_env` reads these `DFE_FETCHER_KAFKA_SASL_*` names into
/// `kafka.sasl`. Cloud credentials are no group here: a deployment supplies
/// them as `DFE_FETCHER_SOURCES__<BLOCK>__<FIELD>` env vars or `env:` specs.
fn secrets() -> Vec<SecretGroupContract> {
    let env = |env_var: &str, key_name: &str, secret_key: &str| SecretEnvContract {
        env_var: env_var.into(),
        key_name: key_name.into(),
        secret_key: secret_key.into(),
    };
    vec![SecretGroupContract::new(
        "kafka",
        vec![
            env("DFE_FETCHER_KAFKA_SASL_USER", "username", "username"),
            env("DFE_FETCHER_KAFKA_SASL_PASSWORD", "password", "password"),
            env(
                "DFE_FETCHER_KAFKA_SASL_MECHANISM",
                "mechanism",
                "sasl.mechanism",
            ),
        ],
    )]
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
        assert_ne!(c.base_image, "");
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

    /// The chart's startup probe allows this long before it restarts the pod.
    #[test]
    fn test_contract_startup_budget() {
        assert_eq!(contract().health.startup_budget_seconds, 120);
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
        // Anything that builds a deployment's config from this contract default
        // overrides IngestConfig::default(). Flipping one and not the other is
        // how a security default gets fixed on paper and left open in the
        // cluster.
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
        // The receiver is cleartext gRPC, so a proxy in front of it must speak h2c.
        assert_eq!(vector.app_protocol, "kubernetes.io/h2c");
    }

    /// generate-artefacts refuses a contract whose default config binds a
    /// listener no port declares.
    #[test]
    fn every_listener_has_a_port() {
        scalo::deployment::assert_listeners_declared(&contract());
    }

    /// A values path the `emit-chart` chart reads but the default config never
    /// sets renders empty, so every one must resolve.
    #[test]
    fn every_values_path_the_chart_reads_resolves() {
        assert_eq!(contract().unresolved_values_paths(), Vec::<String>::new());
    }

    /// Kafka is the one Secret the chart mounts. Cloud credentials reach a
    /// deployment through its own env, so a group for them would mount a
    /// Secret every deployment must create whether or not it runs that source.
    #[test]
    fn test_contract_secret_groups_count() {
        let c = contract();
        assert_eq!(c.secrets.len(), 1);
        assert_eq!(c.secrets[0].group_name, "kafka");
    }

    /// The pod cannot produce without its broker credentials, so the group is
    /// required and carries the three names `apply_flat_env` reads.
    #[test]
    fn test_contract_secret_group_kafka() {
        let c = contract();
        let group = c
            .secrets
            .iter()
            .find(|g| g.group_name == "kafka")
            .expect("kafka secret group must exist");
        assert!(!group.optional);
        let env_vars: Vec<&str> = group.env_vars.iter().map(|e| e.env_var.as_str()).collect();
        assert_eq!(
            env_vars,
            [
                "DFE_FETCHER_KAFKA_SASL_USER",
                "DFE_FETCHER_KAFKA_SASL_PASSWORD",
                "DFE_FETCHER_KAFKA_SASL_MECHANISM",
            ]
        );
    }

    /// The root filesystem is read-only, so the default cursor directory must
    /// sit under an ungated persistent writable path, or every restart loses
    /// each connection's window and re-fetches the default one.
    #[test]
    fn the_default_cursor_directory_is_a_persistent_writable_path() {
        let c = contract();
        let directory = c
            .default_config
            .as_ref()
            .and_then(|config| config.pointer("/cursor/directory"))
            .and_then(serde_json::Value::as_str)
            .expect("the contract names a cursor directory");

        assert!(c.security.read_only_root_filesystem);
        assert!(
            c.writable_paths.iter().any(|writable| writable.persistent
                && writable.when.is_none()
                && std::path::Path::new(directory).starts_with(&writable.path)),
            "no persistent writable path covers {directory}: {:?}",
            c.writable_paths
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
        assert_eq!(c.schema_version, CONTRACT_SCHEMA_VERSION);
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
        "service_account_key",
    ];

    /// Every property declared in `schema` at any depth, as its name and whether
    /// its own object carries `x-scalo-secret`.
    fn declared_properties(schema: &serde_json::Value) -> Vec<(String, bool)> {
        let mut found = Vec::new();
        let mut stack = vec![schema];
        while let Some(node) = stack.pop() {
            match node {
                serde_json::Value::Object(map) => {
                    if let Some(serde_json::Value::Object(props)) = map.get("properties") {
                        for (name, prop) in props {
                            let marked =
                                prop.get("x-scalo-secret") == Some(&serde_json::Value::Bool(true));
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

    /// The catalog builds the console's forms, so a secret field it declares as
    /// a plain string is shown and edited as text.
    #[test]
    fn every_secret_field_the_catalog_declares_is_flagged_secret() {
        let mut plain = std::collections::BTreeSet::new();
        let mut stack: Vec<&scalo::deployment::Capability> = Vec::new();
        let catalog = contract().capabilities;
        stack.extend(&catalog);
        while let Some(capability) = stack.pop() {
            for field in &capability.fields {
                if SECRET_FIELDS.contains(&field.name.as_str()) && !field.secret {
                    plain.insert(format!("{}.{}", capability.name, field.name));
                }
            }
            stack.extend(&capability.children);
        }
        assert_eq!(
            plain,
            std::collections::BTreeSet::new(),
            "declare these with `FieldSpec::secret`"
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
