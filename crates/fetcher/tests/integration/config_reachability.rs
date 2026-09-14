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
//! The generic ones ([`contract_secret_env_vars_reach_the_config_they_name`] and
//! [`committed_chart_injects_every_contract_secret_env_var`]) walk the
//! deployment contract rather than a hand-written list, so a secret added to the
//! contract later is checked without touching this file.

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

/// The config key an env var names, by the cascade's own rules: drop the
/// `DFE_FETCHER` prefix and either separator that follows it, then `__` nests.
fn config_key_for(env_var: &str, env_prefix: &str) -> String {
    env_var
        .strip_prefix(env_prefix)
        .unwrap_or(env_var)
        .trim_start_matches('_')
        .to_lowercase()
        .replace("__", ".")
}

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

/// Every secret the deployment contract declares must land on the config key
/// its name spells out.
///
/// The chart injects all nine as `DFE_FETCHER__SECTION__FIELD`. figment strips
/// exactly `DFE_FETCHER_`, so that form used to arrive as the key
/// `_sources.aws.access_key_id`, which matches no field and serde drops without
/// a word -- and on the `--config` path the container takes, no env layer ran at
/// all. Every Helm deploy ran with no Kafka SASL and no cloud credentials while
/// the chart said otherwise.
///
/// The whole sweep is one test with set/remove around each case.
#[test]
fn contract_secret_env_vars_reach_the_config_they_name() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let contract = dfe_fetcher::deployment::contract();
    let prefix = contract.env_prefix.clone();
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = minimal_config(&dir);

    let mut checked = 0;
    for group in &contract.secrets {
        for secret in &group.env_vars {
            let key = config_key_for(&secret.env_var, &prefix);
            let sentinel = format!("reachability-{}", key.replace('.', "-"));

            // SAFETY: test-only; set and removed inside this one locked test.
            unsafe { std::env::set_var(&secret.env_var, &sentinel) };
            let loaded = Config::load_from_file(&path).expect("config loads");
            unsafe { std::env::remove_var(&secret.env_var) };

            let json = expose_during(|| serde_json::to_value(&loaded)).expect("config serialises");
            let found = at(&json, &key);

            assert_eq!(
                found.and_then(Value::as_str),
                Some(sentinel.as_str()),
                "deployment contract declares {} for secret '{}', but setting it left \
                 config key '{}' at {:?}. The contract, the chart and the config reader \
                 have to agree on the env var name or the secret never reaches the process.",
                secret.env_var,
                secret.secret_key,
                key,
                found,
            );
            checked += 1;
        }
    }

    assert!(
        checked >= 9,
        "expected the contract to declare kafka + the four cloud sources, checked {checked}"
    );
}

/// The committed chart must inject every secret env var the contract declares.
///
/// Nothing else compares the two: the chart is generated from the contract but
/// committed by hand, and only the Dockerfile has a drift test.
#[test]
fn committed_chart_injects_every_contract_secret_env_var() {
    let contract = dfe_fetcher::deployment::contract();
    let template = std::fs::read_to_string(
        dfe_fetcher::deployment::repo_root().join("chart/templates/deployment.yaml"),
    )
    .expect("read committed deployment template");

    for group in &contract.secrets {
        for secret in &group.env_vars {
            assert!(
                template.contains(&secret.env_var),
                "chart/templates/deployment.yaml does not inject '{}' ({} / {}), so that \
                 secret never reaches the pod",
                secret.env_var,
                group.group_name,
                secret.secret_key,
            );
        }
    }
}

/// The chart's `config:` block must be the contract's `default_config`.
///
/// The chart is generated from the contract and then committed by hand, so a
/// default changed in one and not the other ships a deployment running on a
/// value the code no longer chooses -- and only the Dockerfile had a drift test.
#[test]
fn committed_chart_config_block_matches_the_contract_default() {
    let contract = dfe_fetcher::deployment::contract();
    let expected = contract
        .default_config
        .as_ref()
        .expect("contract carries a default config");

    let values: Value = serde_yaml_ng::from_str(
        &std::fs::read_to_string(dfe_fetcher::deployment::repo_root().join("chart/values.yaml"))
            .expect("read committed chart values"),
    )
    .expect("chart values parse");
    let actual = values.get("config").expect("chart values carry config:");

    assert_eq!(
        actual, expected,
        "chart/values.yaml config: has drifted from the deployment contract's \
         default_config. Regenerate the chart (`dfe-fetcher emit-chart chart`) or fix \
         the contract -- whichever one is now wrong."
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
/// The chart declares each secret env var whether or not the operator supplied
/// a value, so without this an unconfigured AWS credential would arrive as
/// `Some("")` and sign requests with an empty key instead of failing on the
/// missing one.
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

/// The two secret env vars the chart injects must be a complete SASL block on
/// their own -- which is what the chart's own comment promises.
///
/// `SaslConfig` had no field defaults, so the two env vars were a partial block
/// and a partial block was a startup parse error.
#[test]
fn the_charts_two_sasl_secrets_are_enough_on_their_own() {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = minimal_config(&dir);

    // SAFETY: test-only; removed immediately after the load.
    unsafe {
        std::env::set_var("DFE_FETCHER__KAFKA__SASL__USERNAME", "kuser");
        std::env::set_var("DFE_FETCHER__KAFKA__SASL__PASSWORD", "kpass");
    }
    let loaded = Config::load_from_file(&path);
    unsafe {
        std::env::remove_var("DFE_FETCHER__KAFKA__SASL__USERNAME");
        std::env::remove_var("DFE_FETCHER__KAFKA__SASL__PASSWORD");
    }

    let sasl = loaded
        .expect("config loads")
        .kafka
        .sasl
        .expect("credentials must produce a sasl block");
    assert!(sasl.enabled, "a sasl block that exists must be on");
    assert_eq!(sasl.mechanism, "SCRAM-SHA-512");
    assert_eq!(sasl.username, "kuser");

    // And the block must survive the trip into the transport config.
    let mut kafka = KafkaConfig::default();
    kafka.sasl = Some(sasl);
    let scalo = build_scalo_kafka_config(&kafka);
    assert_eq!(scalo.sasl_username.as_deref(), Some("kuser"));
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
/// The runtime resolves it from scalo's cascade, which `--config` never
/// populates, so the value shipped in chart/values.yaml, in the contract's
/// default config and in config.example.yaml was read by nothing and the
/// listener bound the hard-coded default. `config-check` reports the address
/// the runtime would use, so it is the honest place to assert.
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

/// Every `vault:` spec in the shipped examples names the KV v2 `data` segment:
/// `vault:<mount>/data/<path>:<key>`.
///
/// scalo's OpenBao path parser only recognises a mount when a literal `data`
/// segment follows it, and otherwise files the whole path under the default
/// `secret` mount -- so `vault:secret/aws:credentials` reads
/// `secret/data/secret/aws`, a path nobody wrote to. An operator copying that
/// example gets a not-found at fetch time, not at config load.
#[test]
fn every_documented_vault_spec_names_the_kv_data_segment() {
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
                if !names_data_segment(spec) {
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
        "vault: specs that resolve under `secret/data/<whole path>` instead of the mount \
         they name -- write them as vault:<mount>/data/<path>:<key>:\n{}",
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

/// scalo's rule: the first path segment is the mount only when the second is
/// `data`.
fn names_data_segment(spec: &str) -> bool {
    let Some(path_key) = spec.strip_prefix("vault:") else {
        return false;
    };
    let Some((path, _key)) = path_key.split_once(':') else {
        return false;
    };
    let mut segments = path.split('/');
    segments.next().is_some_and(|mount| !mount.is_empty())
        && segments.next() == Some("data")
        && segments.next().is_some_and(|rest| !rest.is_empty())
}
