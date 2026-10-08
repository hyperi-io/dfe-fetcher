// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/config_reachability.rs
// Purpose:   A setting a deployment can write must reach something
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Config reachability: a setting a deployment can write must reach something.
//!
//! Every test here guards a knob that once parsed cleanly and then did nothing --
//! no error, no warning. The class is worth its own file because the failures
//! look identical from outside: the process starts, logs a healthy line, and
//! runs on a value the operator did not set.
//!
//! [`every_declared_secret_env_var_reaches_the_config`] walks the deployment
//! contract, so a secret added to the contract later fails until its field is
//! named in [`SECRET_FIELDS`].

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use dfe_fetcher::config::{Config, KafkaConfig, SaslConfig};
use dfe_fetcher::output::build_scalo_kafka_config;
use scalo::config::sensitive::expose_during;
use serde_json::Value;

/// Env vars are process-global; the integration suite runs tests in parallel.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Walk a dotted path through a JSON object.
fn at<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(root, |node, key| node.get(key))
}

/// The config field each Secret env var the contract declares must fill.
///
/// The flat names do not spell their fields (`..._SASL_USER` fills
/// `kafka.sasl.username`), so the field is named here rather than derived.
const SECRET_FIELDS: &[(&str, &str)] = &[
    ("DFE_FETCHER_KAFKA_SASL_USER", "kafka.sasl.username"),
    ("DFE_FETCHER_KAFKA_SASL_PASSWORD", "kafka.sasl.password"),
    ("DFE_FETCHER_KAFKA_SASL_MECHANISM", "kafka.sasl.mechanism"),
];

fn write_config(dir: &tempfile::TempDir, body: &str) -> String {
    let path = dir.path().join("fetcher.yaml");
    std::fs::write(&path, body).expect("write config");
    path.to_str().expect("utf-8 path").to_string()
}

/// A config file with just enough in it to load and validate.
fn minimal_config(dir: &tempfile::TempDir) -> String {
    write_config(dir, "kafka:\n  brokers: [broker:9092]\n")
}

// ============================================================================
// Deployment contract <-> config reader
// ============================================================================

/// Every Secret env var the deployment contract declares lands on the one
/// config field it fills, on the `--config` path the container takes.
///
/// The chart mounts the Secret under every declared name, so a name the config
/// never reads leaves the credential silently unused, and a name read into
/// another field connects with the wrong value.
///
/// The whole sweep is one test with set/remove around each case.
#[test]
fn every_declared_secret_env_var_reaches_the_config() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let contract = dfe_fetcher::deployment::contract();
    let declared: BTreeSet<&str> = contract
        .secrets
        .iter()
        .flat_map(|group| group.env_vars.iter().map(|env| env.env_var.as_str()))
        .collect();
    let named: BTreeSet<&str> = SECRET_FIELDS.iter().map(|(env_var, _)| *env_var).collect();
    assert_eq!(
        declared, named,
        "every env var the contract declares needs its field in SECRET_FIELDS"
    );

    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = minimal_config(&dir);
    for group in &contract.secrets {
        for env in &group.env_vars {
            let field = SECRET_FIELDS
                .iter()
                .find(|(env_var, _)| *env_var == env.env_var)
                .map(|(_, field)| *field)
                .expect("checked against SECRET_FIELDS above");
            let sentinel = format!("sentinel-{}", env.key_name);

            // SAFETY: test-only; set and removed inside this one locked test.
            unsafe { std::env::set_var(&env.env_var, &sentinel) };
            let loaded = Config::load_from_file(&path).expect("config loads");
            unsafe { std::env::remove_var(&env.env_var) };

            // A credential field redacts on every other serialise path.
            let applied =
                expose_during(|| serde_json::to_value(&loaded)).expect("config serialises");
            let reached = at(&applied, field).and_then(Value::as_str) == Some(sentinel.as_str());
            // The message carries the env var and group names only, never a value.
            assert!(
                reached,
                "{} ({}) was set and the config field it fills did not read it",
                env.env_var, group.group_name
            );
        }
    }
}

/// A service-account key set by env is the key JSON itself, so the block must
/// read that value as the key. Read as a key file path, every exchange fails on
/// a file named after the key.
#[test]
fn a_gcp_key_set_by_env_is_read_as_the_key() {
    const GCP_VAR: &str = "DFE_FETCHER__SOURCES__GCP__SERVICE_ACCOUNT_KEY";
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = write_config(
        &dir,
        "kafka:\n  brokers: [broker:9092]\nsources:\n  gcp:\n    enabled: true\n    \
         project_id: proj\n    services: [{name: admin_activity}]\n",
    );
    let key = r#"{"type":"service_account","client_email":"sa@proj.iam.gserviceaccount.com"}"#;

    // SAFETY: test-only; set and removed inside this one locked test.
    unsafe { std::env::set_var(GCP_VAR, key) };
    let loaded = Config::load_from_file(&path);
    unsafe { std::env::remove_var(GCP_VAR) };

    let built = loaded
        .expect("config loads")
        .sources
        .gcp
        .instances()
        .expect("the gcp block builds");
    let auth = &built.first().expect("one connection").instance.auth;
    let read_as_key = auth
        .service_account_key
        .as_ref()
        .map(scalo::config::sensitive::SensitiveString::expose)
        == Some(key);
    // The message names no value: assert_eq! would print the key on failure.
    assert!(
        read_as_key,
        "the key JSON set by env was not used as the key"
    );
    assert!(
        auth.service_account_key_file.is_none(),
        "the key JSON was read as a key file path"
    );
}

/// The documented single-separator form must reach the same key on the
/// `--config` path. Every `docs/cloud-setup/*.md` "Environment variables"
/// section spells settings this way, and the container always passes
/// `--config`, so this is the form an operator copies out of the docs.
#[test]
fn documented_env_form_reaches_the_config_over_an_explicit_file() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = minimal_config(&dir);

    // SAFETY: test-only; removed before the assertion runs.
    unsafe {
        std::env::set_var(
            "DFE_FETCHER_SOURCES__OKTA__TENANT_URL",
            "https://acme.okta.com",
        );
    }
    let loaded = Config::load_from_file(&path);
    unsafe { std::env::remove_var("DFE_FETCHER_SOURCES__OKTA__TENANT_URL") };

    assert_eq!(
        loaded
            .expect("config loads")
            .sources
            .okta
            .tenant_url
            .as_deref(),
        Some("https://acme.okta.com")
    );
}

/// The cascade runs before the resolver: a spec in the file resolves when
/// nothing overrides it, and a flat env var replaces the spec outright, so
/// the resolver sees the operator's value and never the spec it displaced.
#[test]
fn a_flat_env_var_beats_a_spec_and_a_spec_beats_the_file() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = write_config(
        &dir,
        "sources:\n  azure:\n    enabled: true\n    tenant_id: env:DFE_FETCHER_TEST_PRECEDENCE_TENANT\n",
    );
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let resolve = |mut config: Config| {
        rt.block_on(dfe_fetcher::config::resolve::resolve_config_specs(
            &mut config,
        ))
        .expect("resolves");
        config
    };

    // SAFETY: test-only; both removed before the assertions run.
    unsafe { std::env::set_var("DFE_FETCHER_TEST_PRECEDENCE_TENANT", "from-spec") };
    let spec_only = resolve(Config::load_from_file(&path).expect("config loads"));
    assert_eq!(
        spec_only.sources.azure.tenant_id.as_deref(),
        Some("from-spec"),
        "with nothing above it the spec resolves"
    );

    unsafe { std::env::set_var("DFE_FETCHER_SOURCES__AZURE__TENANT_ID", "from-flat-env") };
    let loaded = Config::load_from_file(&path);
    unsafe {
        std::env::remove_var("DFE_FETCHER_SOURCES__AZURE__TENANT_ID");
        std::env::remove_var("DFE_FETCHER_TEST_PRECEDENCE_TENANT");
    }
    let overridden = resolve(loaded.expect("config loads"));
    assert_eq!(
        overridden.sources.azure.tenant_id.as_deref(),
        Some("from-flat-env"),
        "the flat env var replaces the spec before it resolves and passes through as the literal it is"
    );
}

/// The file still wins over nothing, and an env var still wins over the file.
#[test]
fn env_overrides_the_file_and_the_file_overrides_the_default() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = write_config(
        &dir,
        "kafka:\n  brokers: [broker:9092]\n  client_id: from-file\n",
    );

    assert_eq!(
        Config::load_from_file(&path)
            .expect("config loads")
            .kafka
            .client_id,
        "from-file"
    );

    // SAFETY: test-only; removed immediately after the load.
    unsafe { std::env::set_var("DFE_FETCHER__KAFKA__CLIENT_ID", "from-env") };
    let loaded = Config::load_from_file(&path);
    unsafe { std::env::remove_var("DFE_FETCHER__KAFKA__CLIENT_ID") };
    assert_eq!(loaded.expect("config loads").kafka.client_id, "from-env");
}

/// An env var set to the empty string counts as unset.
///
/// A deployment can declare an env var with no value behind it, so without
/// this an unconfigured AWS credential would arrive as `Some("")` and sign
/// requests with an empty key instead of failing on the missing one.
#[test]
fn an_empty_env_value_is_not_a_setting() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = minimal_config(&dir);

    // SAFETY: test-only; removed immediately after the load.
    unsafe { std::env::set_var("DFE_FETCHER__SOURCES__AWS__ACCESS_KEY_ID", "") };
    let loaded = Config::load_from_file(&path);
    unsafe { std::env::remove_var("DFE_FETCHER__SOURCES__AWS__ACCESS_KEY_ID") };

    assert_eq!(
        loaded.expect("config loads").sources.aws.access_key_id,
        None,
        "an empty env var must leave the field unset, not set it to \"\""
    );
}

// ============================================================================
// Kafka SASL
// ============================================================================

/// The Kafka Secret's env vars are a complete SASL block on their own.
///
/// `SaslConfig` had no field defaults, so env vars alone were a partial block
/// and a partial block was a startup parse error.
#[test]
fn the_kafka_secret_alone_is_a_complete_sasl_block() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = minimal_config(&dir);
    let contract = dfe_fetcher::deployment::contract();
    let kafka_secret = contract
        .secrets
        .iter()
        .find(|group| group.group_name == "kafka")
        .expect("the contract declares the Kafka Secret");
    // A mechanism off the default, so its arrival is visible.
    let value_of = |key_name: &str| match key_name {
        "username" => "kuser",
        "password" => "kpass",
        "mechanism" => "SCRAM-SHA-256",
        other => panic!("no test value for the Kafka Secret key {other}"),
    };

    for env in &kafka_secret.env_vars {
        // SAFETY: test-only; removed immediately after the load.
        unsafe { std::env::set_var(&env.env_var, value_of(&env.key_name)) };
    }
    let loaded = Config::load_from_file(&path);
    for env in &kafka_secret.env_vars {
        // SAFETY: as above.
        unsafe { std::env::remove_var(&env.env_var) };
    }

    let sasl = loaded
        .expect("config loads")
        .kafka
        .sasl
        .expect("credentials must produce a sasl block");
    assert!(sasl.enabled, "a sasl block that exists must be on");
    assert_eq!(sasl.mechanism, "SCRAM-SHA-256");
    assert_eq!(sasl.username, "kuser");

    // And the block must survive the trip into the transport config.
    let mut kafka = KafkaConfig::default();
    kafka.sasl = Some(sasl);
    let scalo = build_scalo_kafka_config(&kafka);
    assert_eq!(scalo.sasl_username.as_deref(), Some("kuser"));
    assert_eq!(scalo.sasl_mechanism.as_deref(), Some("SCRAM-SHA-256"));
    assert_eq!(scalo.security_protocol, "sasl_plaintext");
}

/// An explicit `enabled: false` is never inferred away.
///
/// `enabled` defaults to true when the block exists, so the off switch has to be
/// the one thing that survives every layer -- otherwise the fix for the chart
/// would have created the same defect pointing the other way.
#[test]
fn sasl_enabled_false_stays_false() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = write_config(
        &dir,
        "kafka:\n  brokers: [broker:9092]\n  sasl:\n    enabled: false\n    username: u\n\
         \n    password: pw\n",
    );

    // Credentials in the file, and an env var on top of them: neither may
    // re-enable what the operator switched off.
    // SAFETY: test-only; removed immediately after the load.
    unsafe { std::env::set_var("DFE_FETCHER__KAFKA__SASL__PASSWORD", "from-env") };
    let config = Config::load_from_file(&path);
    unsafe { std::env::remove_var("DFE_FETCHER__KAFKA__SASL__PASSWORD") };

    assert_eq!(
        config.expect("config loads").kafka.sasl.map(|s| s.enabled),
        Some(false),
        "an explicit kafka.sasl.enabled: false must not be flipped on"
    );
}

/// A hand-written block with no `enabled` key means SASL on.
///
/// Writing a `kafka.sasl` block IS the request; before this, that config was a
/// parse error, so no deployment depends on the other reading.
#[test]
fn a_sasl_block_without_an_enabled_key_is_on() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = write_config(
        &dir,
        "kafka:\n  brokers: [broker:9092]\n  sasl:\n    username: u\n    password: pw\n",
    );
    let config = Config::load_from_file(&path).expect("config loads");
    assert_eq!(config.kafka.sasl.map(|s| s.enabled), Some(true));
}

/// Saying nothing about SASL still means no SASL.
#[test]
fn no_sasl_block_means_no_sasl() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = minimal_config(&dir);
    let config = Config::load_from_file(&path).expect("config loads");
    assert!(config.kafka.sasl.is_none());
    assert_eq!(
        build_scalo_kafka_config(&config.kafka).security_protocol,
        "plaintext"
    );
}

// ============================================================================
// Kafka TLS
// ============================================================================

/// TLS with SASL off is the mTLS / server-cert shape and must select `ssl`.
///
/// The protocol used to be chosen on `sasl.is_some()`, so a `sasl:` block with
/// `enabled: false` next to `tls.enabled: true` left the connection on
/// plaintext while the ssl_* paths were handed to a client that ignores them.
#[test]
fn tls_without_sasl_selects_the_ssl_protocol() {
    let mut kafka = KafkaConfig::default();
    kafka.tls.enabled = true;
    kafka.tls.ca_file = Some("/certs/ca.pem".to_string());
    kafka.sasl = Some(SaslConfig {
        enabled: false,
        ..SaslConfig::default()
    });

    let scalo = build_scalo_kafka_config(&kafka);
    assert_eq!(
        scalo.security_protocol, "ssl",
        "tls.enabled: true must not leave the connection unencrypted"
    );
    assert_eq!(scalo.ssl_ca_location.as_deref(), Some("/certs/ca.pem"));
}

// ============================================================================
// Metrics
// ============================================================================

/// `metrics.address` must reach the listener on the `--config` path.
///
/// The runtime resolves it from scalo's cascade, so a `--config` file that does
/// not reach the cascade leaves the value in the contract's default config and
/// in config.example.yaml read by nothing, and the listener binds the
/// hard-coded default. `config-check` reports the address the runtime would
/// use, so it is the honest place to assert.
#[test]
fn metrics_address_from_the_config_file_reaches_the_listener() {
    // The child process inherits this process's env at spawn.
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = write_config(
        &dir,
        "kafka:\n  brokers: [broker:9092]\nmetrics:\n  address: 127.0.0.1:9391\n",
    );

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-fetcher"))
        .args(["--config", &path, "config-check"])
        .output()
        .expect("run config-check");
    let report = String::from_utf8_lossy(&out.stderr);

    // The `metrics_addr` LINE, not the whole report: the report also dumps the
    // parsed config, which contains the address whether or not it is honoured.
    let line = report
        .lines()
        .find(|l| l.trim_start().starts_with("metrics_addr"))
        .unwrap_or_else(|| panic!("config-check did not report metrics_addr:\n{report}"));

    assert!(
        line.contains("127.0.0.1:9391"),
        "the runtime would bind a metrics address the config did not set: {line:?}"
    );
}

/// `config-check` run by the binary from `dir` with no `--config`, returning
/// what it printed.
fn config_check_in(dir: &Path) -> String {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-fetcher"))
        .arg("config-check")
        .current_dir(dir)
        .env_remove("DFE_FETCHER_KAFKA_CLIENT_ID")
        .output()
        .expect("the binary runs");
    let printed = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(out.status.success(), "config-check failed:\n{printed}");
    printed
}

/// The binary reads the `.env` in its working directory and no other.
///
/// A `.env` in a parent directory belongs to whatever project sits above, so a
/// search up the tree loads another project's settings and credentials.
#[test]
fn a_dotenv_in_a_parent_directory_is_not_loaded() {
    // The child process inherits this process's env at spawn.
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = tempfile::TempDir::new().expect("tempdir");
    let project = root.path().join("project");
    std::fs::create_dir(&project).expect("project dir");
    std::fs::write(
        root.path().join(".env"),
        "DFE_FETCHER_KAFKA_CLIENT_ID=from_parent_dotenv\n",
    )
    .expect("parent .env");

    let printed = config_check_in(&project);
    assert!(
        !printed.contains("from_parent_dotenv"),
        "a .env in the parent directory reached the config"
    );

    // The project's own .env still loads, through the same field.
    std::fs::write(
        project.join(".env"),
        "DFE_FETCHER_KAFKA_CLIENT_ID=from_project_dotenv\n",
    )
    .expect("project .env");
    let printed = config_check_in(&project);
    assert!(
        printed.contains("from_project_dotenv"),
        "the project's own .env did not reach the config"
    );
}

/// `metrics.enabled: false` is refused rather than accepted and ignored.
///
/// Nothing reads it: the runtime always starts the metrics server and the same
/// listener answers /livez and /readyz, so there is no build in which turning it
/// off does anything.
#[test]
fn metrics_enabled_false_is_rejected_not_ignored() {
    let mut config = Config::default();
    config.kafka.brokers = vec!["broker:9092".to_string()];
    config.metrics.enabled = false;

    let err = config.validate().expect_err("must fail").to_string();
    assert!(
        err.contains("metrics.enabled"),
        "expected metrics.enabled to be named in the error, got: {err}"
    );
}

// ============================================================================
// Committed config files
// ============================================================================

/// Every committed `config*.yaml` must load AND validate.
///
/// Parsing was checked; validating was not, so a committed example could name a
/// value the running binary refuses and nothing would say so until a deploy.
#[test]
fn every_committed_config_file_loads_and_validates() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = dfe_fetcher::deployment::repo_root();
    let mut checked = 0;

    for entry in std::fs::read_dir(&root).expect("read repo root") {
        let path = entry.expect("dir entry").path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let is_yaml = path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("yaml"));
        if !name.starts_with("config") || !is_yaml {
            continue;
        }

        let text = path.to_str().expect("utf-8 path");
        let config =
            Config::load_from_file(text).unwrap_or_else(|e| panic!("{name} does not load: {e}"));
        config
            .validate()
            .unwrap_or_else(|e| panic!("{name} loads but does not validate: {e}"));
        checked += 1;
    }

    assert!(checked >= 1, "expected at least config.example.yaml");
}

/// Every `vault:` spec in the shipped examples names its mount and a path:
/// `vault:<mount>/<path>:<key>`, with an optional `data` segment after the mount.
///
/// scalo's OpenBao path parser reads the first segment as the mount and drops a
/// `data` segment after it, but files a spec with one segment under the default
/// `secret` mount. An operator copying such an example reads a mount the spec
/// never names, and gets a not-found at fetch time, not at config load.
#[test]
fn every_documented_vault_spec_names_a_mount_and_a_path() {
    let root = dfe_fetcher::deployment::repo_root();
    let mut files = vec![root.join("config.example.yaml"), root.join("README.md")];
    collect_docs(&root.join("docs"), &mut files);

    let mut seen = 0;
    let mut bad = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        let shown = file.strip_prefix(&root).unwrap_or(file).display();
        for (i, line) in text.lines().enumerate() {
            for spec in vault_specs(line) {
                seen += 1;
                if !names_mount_and_path(spec) {
                    bad.push(format!("{shown}:{}: {spec}", i + 1));
                }
            }
        }
    }

    assert!(
        seen > 0,
        "expected at least one vault: example in the shipped docs"
    );
    assert!(
        bad.is_empty(),
        "vault: specs that name no mount, so they resolve under the default `secret` \
         mount -- write them as vault:<mount>/<path>:<key>:\n{}",
        bad.join("\n")
    );
}

/// Markdown, YAML and JSON under `dir`, recursively.
fn collect_docs(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display())) {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            // Local-only working scratch, never committed.
            if path.file_name().is_some_and(|n| n == "superpowers") {
                continue;
            }
            collect_docs(&path, out);
        } else if path
            .extension()
            .is_some_and(|e| e == "md" || e == "yaml" || e == "json")
        {
            out.push(path);
        }
    }
}

/// Every spec-shaped `vault:<path>:<key>` token on a line, cut at the first
/// character that cannot be part of a spec. A bare `vault:` naming the scheme
/// in prose is not a spec.
fn vault_specs(line: &str) -> Vec<&str> {
    line.match_indices("vault:")
        .map(|(start, _)| {
            let rest = &line[start..];
            let end = rest
                .find(|c: char| c.is_whitespace() || "\"'`()[]{},;".contains(c))
                .unwrap_or(rest.len());
            &rest[..end]
        })
        .filter(|token| {
            token["vault:".len()..]
                .split_once(':')
                .is_some_and(|(path, key)| !path.is_empty() && !key.is_empty())
        })
        .collect()
}

/// scalo's rule: the first path segment is the mount, and a `data` segment
/// after it is the KV v2 prefix, not part of the path.
fn names_mount_and_path(spec: &str) -> bool {
    let Some(path_key) = spec.strip_prefix("vault:") else {
        return false;
    };
    let Some((path, _key)) = path_key.split_once(':') else {
        return false;
    };
    let Some((mount, rest)) = path.split_once('/') else {
        return false;
    };
    let rest = rest.strip_prefix("data/").unwrap_or(rest);
    !mount.is_empty() && !rest.is_empty() && !rest.ends_with('/')
}

/// The rule above agrees with scalo's parser on the shapes it documents.
#[test]
fn vault_spec_rule_matches_the_scalo_parser() {
    for good in [
        "vault:kv/aws/prod:credentials",
        "vault:kv/data/aws/prod:credentials",
        "vault:secret/dfe/okta:token",
        "vault:data/foo/bar:key",
    ] {
        assert!(
            names_mount_and_path(good),
            "{good} names a mount and a path"
        );
    }
    for bad in [
        "vault:aws:credentials",
        "vault:/aws/prod:credentials",
        "vault:kv/:credentials",
        "vault:kv/data/:credentials",
    ] {
        assert!(
            !names_mount_and_path(bad),
            "{bad} names no mount or no path"
        );
    }
}
