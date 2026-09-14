// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/file_config.rs
// Purpose:   The file grammar through the config cascade: loads, validates, refuses with the field
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `sources.file` through `Config::load_from_file` and `Config::validate`.

use dfe_fetcher::config::Config;
use dfe_fetcher_file::tail_is_built;

fn write(dir: &tempfile::TempDir, body: &str) -> String {
    let path = dir.path().join("fetcher.yaml");
    std::fs::write(&path, body).expect("write config");
    path.to_str().expect("utf-8 path").to_string()
}

const INSTANCE: &str = r#"
kafka:
  brokers: [broker:9092]
sources:
  file:
    exports:
      topic: exports
      interval_secs: 300
      filter: "record.alive == true"
      units:
        - unit: assets
          dump: { paths: ["/data/exports/assets-*.jsonl.gz"], chunk_bytes: 4096 }
          row_key: "/id"
        - unit: logs
          tail: { include: ["/var/log/app/*.log"], data_dir: "/var/lib/dfe-fetcher/tail/logs", decoder: line }
"#;

/// A binary without the tailer refuses the tail unit for that reason; one
/// with it accepts the instance.
#[test]
fn a_file_instance_loads_through_the_cascade_and_validates_per_the_built_tailer() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = Config::load_from_file(&write(&dir, INSTANCE)).expect("loads");
    let inst = &config.sources.file["exports"];
    assert_eq!(inst.units.len(), 2);
    let dump = inst.units[0].dump.as_ref().expect("a dump unit");
    assert_eq!(dump.chunk_bytes, 4096);
    assert_eq!(
        dump.decoder,
        dfe_fetcher_file::DumpDecoder::Auto,
        "unset keys keep their defaults"
    );
    let tail = inst.units[1].tail.as_ref().expect("a tail unit");
    assert_eq!(tail.max_line_bytes, 102_400);
    assert_eq!(tail.decoder, dfe_fetcher_file::TailDecoder::Line);
    assert!(config.sources.any_enabled());
    assert_eq!(
        config.sources.filter_for_source("exports"),
        Some("record.alive == true")
    );
    match config.validate() {
        Ok(()) => assert!(tail_is_built(), "accepted only when the tailer is built"),
        Err(e) => {
            assert!(
                !tail_is_built(),
                "refused only when the tailer is absent: {e}"
            );
            let text = e.to_string();
            assert!(
                text.starts_with("configuration error: sources.file.exports.units[1].tail"),
                "{text}"
            );
            assert!(text.contains("file-tail"), "{text}");
        }
    }
}

#[test]
fn a_unit_problem_is_reported_under_its_instance_and_field() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = INSTANCE.replace("row_key: \"/id\"", "row_key: id");
    let err = Config::load_from_file(&write(&dir, &body))
        .expect("loads")
        .validate()
        .expect_err("a row key that is not a pointer");
    let text = err.to_string();
    assert!(
        text.contains("sources.file.exports.units[0].row_key"),
        "{text}"
    );
    if !tail_is_built() {
        assert!(text.contains("(and 1 more)"), "{text}");
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
            .contains("sources.file.exports.filter invalid"),
        "{err}"
    );

    let body = INSTANCE.replace("    exports:", "    aws:");
    let err = Config::load_from_file(&write(&dir, &body))
        .expect("loads")
        .validate()
        .expect_err("an id that is a built-in source name");
    assert!(
        err.to_string()
            .contains("sources.file.aws: `aws` is already a source name"),
        "{err}"
    );
}

#[test]
fn the_example_config_carries_a_pasteable_file_stanza() {
    let yaml =
        std::fs::read_to_string(dfe_fetcher::deployment::repo_root().join("config.example.yaml"))
            .expect("config.example.yaml exists");
    let start = yaml
        .find("# sources:\n#   file:")
        .expect("the commented sources.file stanza is present");
    let stanza: String = yaml[start..]
        .lines()
        .take_while(|l| l.starts_with('#'))
        .map(|l| l.trim_start_matches('#').strip_prefix(' ').unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(stanza.is_ascii(), "the example is ASCII only");
    let config: Config = serde_yaml_ng::from_str(&stanza).expect("the stanza parses as config");
    assert_eq!(config.sources.file.len(), 1);
    let exports = &config.sources.file["exports"];
    assert!(
        exports
            .validate()
            .iter()
            .all(|i| i.contains("not built into this binary")),
        "{:?}",
        exports.validate()
    );
    assert_eq!(exports.units.len(), 2);
    let dump = exports.units[0].dump.as_ref().expect("a dump unit");
    assert_eq!(
        *dump,
        dfe_fetcher_file::DumpSpec {
            paths: dump.paths.clone(),
            ..Default::default()
        },
        "every spelled-out dump knob is its default"
    );
    let tail = exports.units[1].tail.as_ref().expect("a tail unit");
    assert_eq!(
        *tail,
        dfe_fetcher_file::TailSpec {
            include: tail.include.clone(),
            data_dir: tail.data_dir.clone(),
            ..Default::default()
        },
        "every spelled-out tail knob is its default"
    );
}

#[test]
fn an_unknown_key_fails_the_load_and_a_disabled_instance_is_not_validated() {
    let dir = tempfile::TempDir::new().unwrap();
    let body = INSTANCE.replace("chunk_bytes: 4096", "read_ahead: 4096");
    assert!(Config::load_from_file(&write(&dir, &body)).is_err());

    let body = INSTANCE.replace(
        "      topic: exports",
        "      enabled: false\n      topic: exports",
    );
    let body = body.replace("row_key: \"/id\"", "row_key: id");
    let config = Config::load_from_file(&write(&dir, &body)).expect("loads");
    config
        .validate()
        .expect("a disabled instance is not bound, so its bad row key is not an error");
    assert!(!config.sources.any_enabled());
}
