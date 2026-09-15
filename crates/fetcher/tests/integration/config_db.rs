// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/config_db.rs
// Purpose:   The database grammar through the config cascade: loads, validates, refuses with the field
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `sources.db` through `Config::load_from_file` and `Config::validate`.

use dfe_fetcher::config::Config;
use dfe_fetcher_db::{Engine, TailMode};

fn write(dir: &tempfile::TempDir, body: &str) -> String {
    let path = dir.path().join("fetcher.yaml");
    std::fs::write(&path, body).expect("write config");
    path.to_str().expect("utf-8 path").to_string()
}

const INSTANCE: &str = r#"
kafka:
  brokers: [broker:9092]
sources:
  db:
    inventory:
      engine: odbc
      dialect: postgres
      connection_string: "env:INVENTORY_DSN"
      topic: inventory
      interval_secs: 3600
      filter: "record.alive == true"
      batch: { max_rows: 500 }
      stores:
        - { unit: hosts, shape: dump, query: "SELECT * FROM hosts", row_key: "/id" }
        - { unit: events, shape: tail, query: "SELECT * FROM events", key: [ts, id], limit: 100 }
"#;

/// A binary without the engine refuses the instance for that reason; one with
/// it accepts it.
#[test]
fn a_database_instance_loads_through_the_cascade_and_validates_per_the_built_engines() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = Config::load_from_file(&write(&dir, INSTANCE)).expect("loads");
    let inst = &config.sources.db["inventory"];
    assert_eq!(inst.batch.max_rows, 500);
    assert_eq!(
        inst.batch.max_bytes,
        4 * 1024 * 1024,
        "unset keys keep their defaults"
    );
    assert_eq!(inst.stores.len(), 2);
    assert!(config.sources.any_enabled());
    assert_eq!(
        config.sources.filter_for_source("inventory"),
        Some("record.alive == true")
    );
    match config.validate() {
        Ok(()) => assert!(
            Engine::Odbc.is_built(),
            "accepted only when the engine is built"
        ),
        Err(e) => {
            assert!(
                !Engine::Odbc.is_built(),
                "refused only when the engine is absent: {e}"
            );
            let text = e.to_string();
            assert!(
                text.starts_with("configuration error: sources.db.inventory."),
                "{text}"
            );
            assert!(text.contains("db-odbc"), "{text}");
        }
    }
}

#[test]
fn a_store_problem_is_reported_under_its_instance_and_field() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = INSTANCE.replace("key: [ts, id], ", "");
    let err = Config::load_from_file(&write(&dir, &body))
        .expect("loads")
        .validate()
        .expect_err("a tail without keys");
    let text = err.to_string();
    if Engine::Odbc.is_built() {
        assert!(
            text.contains("sources.db.inventory.stores[1].key"),
            "{text}"
        );
    } else {
        assert!(
            text.contains("sources.db.inventory.engine") && text.contains("(and 1 more)"),
            "the build gate is reported first and the store problem counted: {text}"
        );
    }
}

#[test]
fn a_bad_filter_and_a_colliding_id_are_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = INSTANCE.replace("record.alive == true", "record.alive ==");
    let err = Config::load_from_file(&write(&dir, &body))
        .expect("loads")
        .validate()
        .expect_err("a filter that does not compile");
    assert!(
        err.to_string()
            .contains("sources.db.inventory.filter invalid"),
        "{err}"
    );

    let body = INSTANCE.replace("    inventory:", "    aws:");
    let err = Config::load_from_file(&write(&dir, &body))
        .expect("loads")
        .validate()
        .expect_err("an id that is a built-in source name");
    assert!(
        err.to_string()
            .contains("sources.db.aws: `aws` is already a source name"),
        "{err}"
    );
}

#[test]
fn an_unknown_key_fails_the_load_and_a_disabled_instance_is_not_validated() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = INSTANCE.replace("batch: { max_rows: 500 }", "dsn: x");
    assert!(Config::load_from_file(&write(&dir, &body)).is_err());

    let body = INSTANCE.replace(
        "      engine: odbc",
        "      enabled: false\n      engine: odbc",
    );
    let body = body.replace("      dialect: postgres\n", "");
    let config = Config::load_from_file(&write(&dir, &body)).expect("loads");
    config
        .validate()
        .expect("a disabled instance is not bound, so its missing dialect is not an error");
    assert!(!config.sources.any_enabled());
}

#[test]
fn the_example_config_carries_a_pasteable_db_stanza() {
    let yaml =
        std::fs::read_to_string(dfe_fetcher::deployment::repo_root().join("config.example.yaml"))
            .expect("config.example.yaml exists");
    let start = yaml
        .find("# sources:\n#   db:")
        .expect("the commented sources.db stanza is present");
    let stanza: String = yaml[start..]
        .lines()
        .take_while(|l| l.starts_with('#'))
        .map(|l| l.trim_start_matches('#').strip_prefix(' ').unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    let config: Config = serde_yaml_ng::from_str(&stanza).expect("the stanza parses as config");
    assert_eq!(config.sources.db.len(), 3);
    for (id, engine) in [
        ("inventory", Engine::Odbc),
        ("audit", Engine::Clickhouse),
        ("assets", Engine::Mongodb),
    ] {
        let instance = &config.sources.db[id];
        assert_eq!(instance.engine, engine);
        let issues = instance.validate();
        assert!(
            issues
                .iter()
                .all(|i| i.contains("not built into this binary")),
            "{id}: {issues:?}"
        );
    }
    let assets = &config.sources.db["assets"];
    assert_eq!(assets.stores.len(), 3);
    let mongo = |i: usize| assets.stores[i].mongodb.as_ref().expect("mongodb block");
    assert_eq!(mongo(1).tail_mode(), TailMode::ChangeStream);
    assert_eq!(mongo(2).tail_mode(), TailMode::Keyset);
}
