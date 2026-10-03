// Project:   dfe-fetcher
// File:      crates/file/src/config.rs
// Purpose:   The `sources.file.<id>` grammar: one instance, its units, each a dump or a tail
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The file instance grammar.
//!
//! One [`FileInstance`] per `sources.file.<id>` entry: the topic base, the
//! interval, an optional filter, and the units -- each a [`DumpSpec`] (a set
//! of globs read once per file into the snapshot envelope) or a
//! [`TailSpec`] (files followed through rotation). Everything that can be
//! checked without touching the filesystem is checked by
//! [`FileInstance::validate`], with the field path, so a bad instance fails at
//! load rather than at the first tick.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use dfe_fetcher_core::UnitShape;
use dfe_fetcher_core::batch::AccumulateConfig;

use crate::dump::DumpSpec;
use crate::tail::TailSpec;

/// The app feature that compiles the tailer in.
pub const TAIL_FEATURE: &str = "file-tail";

/// Whether this binary was built with the tailer.
#[must_use]
pub const fn tail_is_built() -> bool {
    cfg!(feature = "tail")
}

/// One unit: a dump or a tail, never both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FileUnit {
    /// Unit name: the second half of `_source_fetcher` and of a dump's `store`.
    pub unit: String,
    /// Read every matched file once, into the snapshot envelope.
    pub dump: Option<DumpSpec>,
    /// Follow the matched files as they grow.
    pub tail: Option<TailSpec>,
    /// Dump only: JSON pointer to the row's identity, for the oversize stub
    /// and logs.
    pub row_key: Option<String>,
}

impl Default for FileUnit {
    fn default() -> Self {
        Self {
            unit: String::new(),
            dump: None,
            tail: None,
            row_key: None,
        }
    }
}

impl FileUnit {
    /// The driver-facing shape: a dump is enveloped, a tail is incremental
    /// with a line checkpoint.
    #[must_use]
    pub fn unit_shape(&self) -> UnitShape {
        if self.tail.is_some() {
            UnitShape::Incremental
        } else {
            UnitShape::Dump
        }
    }
}

/// One `sources.file.<id>` instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FileInstance {
    /// Whether the instance runs.
    pub enabled: bool,
    /// Fetch interval; the scheduler default when unset. A tail polls at this
    /// cadence between ticks, so set it low for a log directory.
    pub interval_secs: Option<u64>,
    /// Topic base; dump units land on `<topic>-<unit>`, tails on `<topic>`.
    pub topic: String,
    /// CEL keep-filter over each row, hot-reloaded.
    pub filter: Option<String>,
    /// The units, in tick order.
    pub units: Vec<FileUnit>,
    /// Batch bounds for this instance; the deployment's when unset.
    pub accumulate: Option<AccumulateConfig>,
}

impl Default for FileInstance {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: None,
            topic: String::new(),
            filter: None,
            units: Vec::new(),
            accumulate: None,
        }
    }
}

impl FileInstance {
    /// Every problem with this instance, each as `field: problem`.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut issues = Vec::new();
        if self.topic.trim().is_empty() {
            issues.push("topic: is required".to_owned());
        }
        if self.units.is_empty() {
            issues.push("units: at least one unit is required".to_owned());
        }
        let mut seen = BTreeSet::new();
        for (i, unit) in self.units.iter().enumerate() {
            let at = |f: &str| format!("units[{i}].{f}");
            if unit.unit.trim().is_empty() {
                issues.push(format!("{}: is required", at("unit")));
            } else if !seen.insert(unit.unit.as_str()) {
                issues.push(format!("{}: `{}` is declared twice", at("unit"), unit.unit));
            } else if unit.unit.contains('.') {
                issues.push(format!(
                    "{}: `{}` cannot contain `.`, which separates the connection from the unit",
                    at("unit"),
                    unit.unit
                ));
            }
            match (&unit.dump, &unit.tail) {
                (None, None) => issues.push(format!(
                    "{}: a unit is a `dump` or a `tail`; neither is set",
                    at("dump")
                )),
                (Some(_), Some(_)) => issues.push(format!(
                    "{}: a unit is a `dump` or a `tail`, not both",
                    at("tail")
                )),
                (Some(dump), None) => {
                    if let Err(e) = dump.validate() {
                        issues.push(format!("{}: {e}", at("dump")));
                    }
                }
                (None, Some(tail)) => {
                    if !tail_is_built() {
                        issues.push(format!(
                            "{}: the file tailer is not built into this binary; build with `--features {TAIL_FEATURE}`",
                            at("tail")
                        ));
                    }
                    if let Err(e) = tail.validate() {
                        issues.push(format!("{}: {e}", at("tail")));
                    }
                    if unit.row_key.is_some() {
                        issues.push(format!(
                            "{}: only a dump has a row key; a tail's rows carry their file offset",
                            at("row_key")
                        ));
                    }
                }
            }
            if let Some(pointer) = &unit.row_key
                && !pointer.starts_with('/')
            {
                issues.push(format!(
                    "{}: `{pointer}` is not a JSON pointer (must start with `/`)",
                    at("row_key")
                ));
            }
        }
        if let Some(acc) = &self.accumulate
            && let Err(e) = acc.validate()
        {
            issues.push(e.to_string());
        }
        issues
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance(yaml: &str) -> FileInstance {
        serde_yaml_ng::from_str(yaml).expect("instance parses")
    }

    const GOOD: &str = r#"
topic: exports
interval_secs: 300
units:
  - unit: assets
    dump: { paths: ["/data/exports/assets-*.jsonl.gz"] }
    row_key: "/id"
  - unit: logs
    tail: { include: ["/var/log/app/*.log"], data_dir: "/var/lib/dfe-fetcher/tail/logs" }
"#;

    fn without_build_gate(issues: Vec<String>) -> Vec<String> {
        issues
            .into_iter()
            .filter(|i| !i.contains("not built into this binary"))
            .collect()
    }

    #[test]
    fn a_complete_instance_has_no_issues_beyond_the_build_gate() {
        let inst = instance(GOOD);
        assert_eq!(
            without_build_gate(inst.validate()),
            [] as [std::string::String; 0]
        );
        assert_eq!(inst.units[0].unit_shape(), UnitShape::Dump);
        assert_eq!(inst.units[1].unit_shape(), UnitShape::Incremental);
        assert_eq!(inst.interval_secs, Some(300));
        assert!(inst.enabled, "enabled by default");
    }

    /// The stanza the example config documents, every knob spelled out.
    const DOCUMENTED: &str = r#"
enabled: true
topic: exports
interval_secs: 300
filter: 'record.alive == true'
units:
  - unit: assets
    dump:
      paths: ["/data/exports/**/*.jsonl.gz", "/data/exports/**/*.csv"]
      decoder: auto
      chunk_bytes: 65536
    row_key: "/id"
  - unit: logs
    tail:
      include: ["/var/log/app/*.log*"]
      exclude: []
      read_from: beginning
      decoder: ndjson
      max_line_bytes: 102400
      glob_minimum_cooldown_ms: 1000
      rotate_wait_secs: 30
      max_tick_secs: 30
      fingerprint: { strategy: checksum, bytes: 256, lines: 1, ignored_header_bytes: 0 }
      data_dir: /var/lib/dfe-fetcher/tail/logs
accumulate: { max_rows: 1000, max_bytes: 8388608, window_ms: 1000, in_flight: 1000 }
"#;

    #[test]
    fn the_documented_stanza_parses_and_every_spelled_out_value_is_the_default() {
        let inst = instance(DOCUMENTED);
        assert_eq!(
            without_build_gate(inst.validate()),
            [] as [std::string::String; 0]
        );
        let dump = inst.units[0].dump.as_ref().unwrap();
        assert_eq!(dump.decoder, DumpSpec::default().decoder);
        assert_eq!(dump.chunk_bytes, DumpSpec::default().chunk_bytes);
        let tail = inst.units[1].tail.as_ref().unwrap();
        let defaults = TailSpec::default();
        assert_eq!(tail.read_from, defaults.read_from);
        assert_eq!(tail.decoder, defaults.decoder);
        assert_eq!(tail.max_line_bytes, defaults.max_line_bytes);
        assert_eq!(
            tail.glob_minimum_cooldown_ms,
            defaults.glob_minimum_cooldown_ms
        );
        assert_eq!(tail.rotate_wait_secs, defaults.rotate_wait_secs);
        assert_eq!(tail.max_tick_secs, defaults.max_tick_secs);
        assert_eq!(tail.fingerprint, defaults.fingerprint);
    }

    #[test]
    fn the_build_gate_names_the_feature() {
        let issues = instance(GOOD).validate();
        if tail_is_built() {
            assert!(
                issues.iter().all(|i| !i.contains("not built")),
                "{issues:?}"
            );
        } else {
            assert!(
                issues
                    .iter()
                    .any(|i| i.starts_with("units[1].tail:") && i.contains("file-tail")),
                "{issues:?}"
            );
        }
    }

    #[test]
    fn unit_problems_carry_their_index_and_field() {
        let inst = instance(
            r#"
topic: t
units:
  - unit: a
  - unit: a
    dump: { paths: ["x"] }
    tail: { include: ["y"], data_dir: "d" }
  - unit: "x.y"
    dump: { paths: [] }
    row_key: id
  - unit: z
    tail: { include: ["y"], data_dir: "" }
    row_key: "/id"
"#,
        );
        let issues = without_build_gate(inst.validate());
        for expected in [
            "units[0].dump: a unit is a `dump` or a `tail`; neither",
            "units[1].unit: `a` is declared twice",
            "units[1].tail: a unit is a `dump` or a `tail`, not both",
            "units[2].unit: `x.y` cannot contain `.`",
            "units[2].dump: configuration error: file dump `paths`",
            "units[2].row_key: `id` is not a JSON pointer",
            "units[3].tail: configuration error: file tail `data_dir`",
            "units[3].row_key: only a dump has a row key",
        ] {
            assert!(
                issues.iter().any(|i| i.starts_with(expected)),
                "missing `{expected}` in {issues:?}"
            );
        }
    }

    #[test]
    fn a_missing_topic_and_an_empty_unit_list_are_named() {
        let issues = instance("enabled: true\n").validate();
        assert!(
            issues.iter().any(|i| i == "topic: is required"),
            "{issues:?}"
        );
        assert!(
            issues
                .iter()
                .any(|i| i == "units: at least one unit is required"),
            "{issues:?}"
        );
    }

    #[test]
    fn unknown_keys_fail_the_load_at_every_level() {
        assert!(serde_yaml_ng::from_str::<FileInstance>("topic: t\npath: x\n").is_err());
        assert!(
            serde_yaml_ng::from_str::<FileInstance>(
                "topic: t\nunits: [{ unit: a, dump: { globs: [x] } }]\n"
            )
            .is_err()
        );
        assert!(
            serde_yaml_ng::from_str::<FileInstance>(
                "topic: t\nunits: [{ unit: a, tail: { paths: [x], data_dir: d } }]\n"
            )
            .is_err()
        );
    }

    #[test]
    fn a_bad_accumulate_override_is_reported() {
        let inst = instance(
            "topic: t\nunits: [{ unit: a, dump: { paths: [x] } }]\naccumulate: { max_rows: 0 }\n",
        );
        assert!(
            inst.validate().iter().any(|i| i.contains("max_rows")),
            "{:?}",
            inst.validate()
        );
    }
}
