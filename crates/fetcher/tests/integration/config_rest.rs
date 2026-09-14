// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/config_rest.rs
// Purpose:   The REST profile grammar through the config cascade: loads, binds, and refuses with line and field
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `sources.rest` through `Config::load_from_file` and `Config::validate`.

use dfe_fetcher::config::Config;

fn write(dir: &tempfile::TempDir, body: &str) -> String {
    let path = dir.path().join("fetcher.yaml");
    std::fs::write(&path, body).expect("write config");
    path.to_str().expect("utf-8 path").to_string()
}

const INLINE: &str = r#"
kafka:
  brokers: [broker:9092]
accumulate:
  max_rows: 500
  window_ms: 250
self_regulation:
  pause_above: 0.75
  resume_below: 0.5
sources:
  rest:
    inventory:
      enabled: true
      topic: inventory
      interval_secs: 3600
      filter: "record.alive == true"
      auth: { mode: bearer, token: "env:INVENTORY_EXPORT_TOKEN" }
      vars: { base_url: "https://inventory.example.internal/api/v1.0" }
      accumulate: { max_bytes: 1048576 }
      profile:
        base_url: "{{ vars.base_url }}"
        shape: dump
        auth:
          accepts: [bearer, oauth2_client_credentials]
          oauth2_client_credentials: { token_url: "{{ base_url }}/account/api/token" }
        defaults: { rows: { decoder: ndjson } }
        endpoints:
          - { unit: assets, path: /export/org/assets.jsonl, row_key: "/id" }
          - { unit: services, path: /export/org/services.jsonl, row_key: "/service_id" }
"#;

#[test]
fn an_inline_profile_loads_through_the_cascade_and_validates() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = Config::load_from_file(&write(&dir, INLINE)).expect("loads");
    config.validate().expect("validates");
    assert_eq!(config.accumulate.max_rows, 500);
    assert_eq!(config.accumulate.window_ms, 250);
    assert_eq!(
        config.accumulate.max_bytes,
        8 * 1024 * 1024,
        "unset keys keep their defaults"
    );
    assert!((config.self_regulation.pause_above - 0.75).abs() < f64::EPSILON);
    let inventory = &config.sources.rest["inventory"];
    assert_eq!(inventory.interval_secs, Some(3600));
    assert_eq!(inventory.accumulate.unwrap().max_bytes, 1_048_576);
    assert_eq!(
        inventory.accumulate.unwrap().max_rows,
        1000,
        "a per-source block starts from the defaults"
    );
    assert!(
        config.sources.any_enabled(),
        "a REST instance counts as work"
    );
    assert!(config.has_work());
}

#[test]
fn an_unknown_decoder_fails_the_load_with_the_line_and_the_field() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = INLINE.replace("decoder: ndjson", "decoder: parquet");
    let err = Config::load_from_file(&write(&dir, &body))
        .expect_err("refused")
        .to_string();
    assert!(err.contains("parquet"), "{err}");
    assert!(err.contains("decoder"), "names the field: {err}");
    assert!(err.contains("line"), "carries the line: {err}");
}

#[test]
fn an_unknown_pager_and_shape_fail_the_load() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = INLINE.replace("shape: dump", "shape: snapshot");
    let err = Config::load_from_file(&write(&dir, &body))
        .expect_err("refused")
        .to_string();
    assert!(err.contains("snapshot") && err.contains("line"), "{err}");
    let body = INLINE.replace(
        "defaults: { rows: { decoder: ndjson } }",
        "defaults: { rows: { decoder: ndjson }, paginate: { strategy: teleport } }",
    );
    let err = Config::load_from_file(&write(&dir, &body))
        .expect_err("refused")
        .to_string();
    assert!(err.contains("teleport") && err.contains("line"), "{err}");
}

#[test]
fn validation_names_the_instance_and_field_for_a_binding_problem() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = INLINE.replace(
        "auth: { mode: bearer, token: \"env:INVENTORY_EXPORT_TOKEN\" }",
        "auth: { mode: basic, username: u, password: p }",
    );
    let config = Config::load_from_file(&write(&dir, &body)).expect("loads");
    let err = config
        .validate()
        .expect_err("basic is not accepted")
        .to_string();
    assert!(
        err.starts_with("configuration error: sources.rest.inventory.auth.mode"),
        "{err}"
    );

    let body = INLINE.replace(
        "profile:\n        base_url",
        "profile: runzero_cloud\n      unused:\n        base_url",
    );
    let err = Config::load_from_file(&write(&dir, &body))
        .expect_err("unknown key")
        .to_string();
    assert!(err.contains("unused"), "{err}");
}

#[test]
fn a_named_profile_that_is_not_shipped_is_refused_at_validate() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = r#"
kafka:
  brokers: [broker:9092]
sources:
  rest:
    thing:
      topic: thing
      profile: no_such_profile
      auth: { mode: none }
"#;
    let config = Config::load_from_file(&write(&dir, body)).expect("loads");
    let err = config
        .validate()
        .expect_err("unknown shipped profile")
        .to_string();
    assert!(
        err.contains("sources.rest.thing.profile") && err.contains("no_such_profile"),
        "{err}"
    );
}

#[test]
fn an_instance_id_may_not_shadow_a_built_in_source_type() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = INLINE.replace("    inventory:", "    aws:");
    let config = Config::load_from_file(&write(&dir, &body)).expect("loads");
    let err = config.validate().expect_err("collides").to_string();
    assert!(err.contains("sources.rest.aws"), "{err}");
}

#[test]
fn a_disabled_instance_is_not_bound_and_zero_accumulate_bounds_are_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = INLINE.replace("enabled: true", "enabled: false").replace(
        "auth: { mode: bearer, token: \"env:INVENTORY_EXPORT_TOKEN\" }",
        "auth: { mode: basic }",
    );
    let config = Config::load_from_file(&write(&dir, &body)).expect("loads");
    config
        .validate()
        .expect("a disabled instance is never bound");
    let body = INLINE.replace("max_rows: 500", "max_rows: 0");
    let config = Config::load_from_file(&write(&dir, &body)).expect("loads");
    let err = config.validate().expect_err("zero rows").to_string();
    assert!(err.contains("accumulate.max_rows"), "{err}");
    let body = INLINE.replace("resume_below: 0.5", "resume_below: 0.9");
    let config = Config::load_from_file(&write(&dir, &body)).expect("loads");
    let err = config.validate().expect_err("inverted band").to_string();
    assert!(err.contains("self_regulation"), "{err}");
}

/// The shipped example carries the two runZero instances of one shipped
/// profile -- self-hosted on the export token, cloud on OAuth2 with `_oid` --
/// as placeholders, and they bind once enabled, so an operator copying the
/// block gets a load error only for what they left unfilled.
#[test]
fn the_example_runzero_instances_bind_to_the_shipped_profile_once_enabled() {
    use dfe_fetcher_rest::profile::{AuthKind, ProfileRef};

    let path = dfe_fetcher::deployment::repo_root().join("config.example.yaml");
    let mut config = Config::load_from_file(path.to_str().unwrap()).expect("example loads");
    config.validate().expect("the example validates as shipped");

    let self_hosted = &config.sources.rest["runzero_self_hosted"];
    let cloud = &config.sources.rest["runzero_cloud"];
    for (id, instance) in [
        ("runzero_self_hosted", self_hosted),
        ("runzero_cloud", cloud),
    ] {
        assert!(!instance.enabled, "{id} ships disabled");
        assert!(
            matches!(&instance.profile, ProfileRef::Named(name) if name == "runzero"),
            "{id} names the shipped profile"
        );
        assert!(
            instance.vars["base_url"]
                .as_str()
                .is_some_and(|u| u.contains("example")),
            "{id}: the base URL is a placeholder"
        );
    }
    assert_eq!(self_hosted.auth.mode, AuthKind::Bearer);
    assert!(
        self_hosted
            .auth
            .token
            .as_ref()
            .is_some_and(|t| t.expose().starts_with("vault:kv/data/")),
        "the export token is a secret ref, never a literal"
    );
    assert!(
        !self_hosted.vars.contains_key("org_id"),
        "the export token path sends no _oid"
    );
    assert_eq!(cloud.auth.mode, AuthKind::Oauth2ClientCredentials);
    assert!(
        cloud
            .auth
            .client_secret
            .as_ref()
            .is_some_and(|s| s.expose().starts_with("vault:kv/data/")),
        "the client secret is a secret ref, never a literal"
    );
    assert!(
        cloud.vars["org_id"].as_str().is_some_and(|o| !o.is_empty()),
        "OAuth needs _oid on every export"
    );
    assert!(
        cloud.units.values().any(|u| !u.enabled),
        "the cloud instance narrows the stores an OAuth client is refused on"
    );

    for id in ["runzero_self_hosted", "runzero_cloud"] {
        config.sources.rest.get_mut(id).unwrap().enabled = true;
    }
    config
        .validate()
        .expect("both example instances bind to the shipped profile");
    let json = serde_json::to_string(&config).unwrap();
    assert!(
        !json.contains("vault:kv/data/"),
        "secret refs are redacted on serialise: {json}"
    );
}

#[test]
fn two_instances_of_one_inline_profile_carry_their_own_identity() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = format!(
        "{INLINE}    inventory_cloud:\n      topic: inventory-cloud\n      auth: {{ mode: oauth2_client_credentials, client_id: id, client_secret: \"env:CLOUD_SECRET\" }}\n      vars: {{ base_url: \"https://console.example/api/v1.0\", org_id: org-1 }}\n      profile:\n        base_url: \"{{{{ vars.base_url }}}}\"\n        shape: dump\n        auth:\n          accepts: [bearer, oauth2_client_credentials]\n          oauth2_client_credentials: {{ token_url: \"{{{{ base_url }}}}/account/api/token\" }}\n        endpoints:\n          - {{ unit: assets, path: /export/org/assets.jsonl, rows: {{ decoder: ndjson }} }}\n"
    );
    let config = Config::load_from_file(&write(&dir, &body)).expect("loads");
    config.validate().expect("both bind");
    assert_eq!(config.sources.rest.len(), 2);
    assert_eq!(
        config.sources.rest["inventory"].auth.mode,
        dfe_fetcher_rest::profile::AuthKind::Bearer
    );
    assert_eq!(
        config.sources.rest["inventory_cloud"].auth.mode,
        dfe_fetcher_rest::profile::AuthKind::Oauth2ClientCredentials
    );
    let json = serde_json::to_string(&config).unwrap();
    assert!(
        !json.contains("CLOUD_SECRET"),
        "secret specs are redacted on serialise: {json}"
    );
}
