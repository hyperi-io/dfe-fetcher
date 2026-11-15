// Project:   dfe-fetcher
// File:      crates/fetcher/src/cursor/mod.rs
// Purpose:   Cursor store for incremental fetching state
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Cursor store for tracking incremental fetch state across restarts.
//!
//! The contract -- [`CursorValue`], the [`CursorStore`] trait and the key
//! normalisation -- lives in the framework core so every shape crate can
//! name it; this module keeps the file-backed store and re-exports the
//! contract under its old path.
//!
//! Each connection keeps a cursor recording its last successful fetch window
//! (version 1); a framework unit keeps its checkpoint in the same store as a
//! version-2 cursor. One JSON file per key; in-memory cache serves reads and
//! an atomic write-then-rename persists each write.

pub mod file;

use std::path::{Path, PathBuf};

use dfe_fetcher_file::FileInstance;

pub use dfe_fetcher_core::checkpoint::{CursorStore, CursorValue, normalize_cursor_key};

/// The cursor key of a framework unit: `{instance}.{connection}.{unit}`.
#[must_use]
pub fn unit_cursor_key(instance_id: &str, connection_id: &str, unit: &str) -> String {
    format!("{instance_id}.{connection_id}.{unit}")
}

/// Where a file tail unit keeps its checkpoint file when its config sets no
/// `data_dir`: `<cursor_dir>/<unit cursor key>.tail`, beside the unit's
/// `<unit cursor key>.cursor.json` and named for the same normalised key, so
/// the unit has one cursor and one tail directory and no two units share
/// either.
#[must_use]
pub fn default_tail_data_dir(
    cursor_dir: &Path,
    instance_id: &str,
    connection_id: &str,
    unit: &str,
) -> PathBuf {
    let key = normalize_cursor_key(&unit_cursor_key(instance_id, connection_id, unit));
    cursor_dir.join(format!("{key}.tail"))
}

/// A copy of `instance` with every tail unit that sets no `data_dir` given
/// [`default_tail_data_dir`]. A `data_dir` the config sets is kept as written.
#[must_use]
pub fn with_default_tail_data_dirs(
    instance: &FileInstance,
    cursor_dir: &Path,
    instance_id: &str,
    connection_id: &str,
) -> FileInstance {
    let mut resolved = instance.clone();
    for unit in &mut resolved.units {
        if let Some(tail) = unit.tail.as_mut()
            && tail.data_dir.trim().is_empty()
        {
            tail.data_dir =
                default_tail_data_dir(cursor_dir, instance_id, connection_id, &unit.unit)
                    .display()
                    .to_string();
        }
    }
    resolved
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn file_instance(yaml: &str) -> FileInstance {
        scalo::yaml::from_str(yaml, "source.yaml", scalo::yaml::Profile::Config).unwrap()
    }

    #[test]
    fn a_tail_with_no_data_dir_gets_one_beside_its_cursor_file() {
        let instance = file_instance(
            r#"
topic: t
units:
  - unit: Logs
    tail: { include: ["/var/log/*.log"] }
  - unit: audit
    tail: { include: ["/var/log/audit/*.log"], data_dir: /srv/audit }
  - unit: dumped
    dump: { paths: ["/data/*.jsonl"] }
"#,
        );
        let resolved = with_default_tail_data_dirs(
            &instance,
            Path::new("/var/lib/dfe-fetcher/cursors"),
            "Prod",
            "app",
        );
        let tail_dir = |i: usize| resolved.units[i].tail.as_ref().unwrap().data_dir.as_str();
        assert_eq!(
            tail_dir(0),
            "/var/lib/dfe-fetcher/cursors/prod.app.logs.tail"
        );
        assert_eq!(tail_dir(1), "/srv/audit", "a data_dir the config sets wins");
        assert!(resolved.units[2].tail.is_none());
        assert_eq!(
            resolved
                .validate()
                .iter()
                .filter(|i| i.contains("data_dir"))
                .count(),
            0
        );
    }

    #[test]
    fn no_two_units_connections_or_instances_default_to_one_data_dir() {
        let cursors = Path::new("/c");
        let dirs: std::collections::HashSet<PathBuf> = [
            ("i1", "app", "logs"),
            ("i1", "app", "audit"),
            ("i1", "web", "logs"),
            ("i2", "app", "logs"),
        ]
        .into_iter()
        .map(|(i, c, u)| default_tail_data_dir(cursors, i, c, u))
        .collect();
        assert_eq!(dirs.len(), 4);
        assert!(
            dirs.iter()
                .all(|d| !d.to_string_lossy().ends_with("cursor.json")),
            "the cursor store loads every entry whose name ends so: {dirs:?}"
        );
    }

    #[test]
    fn test_cursor_value_serialization_roundtrip() {
        let value = CursorValue {
            cursor_key: "aws.cloudtrail".to_string(),
            last_fetch_end: Utc::now(),
            last_fetch_records: 42,
            updated_at: Utc::now(),
            api_cursor: Some("next-page-token".to_string()),
            version: 1,
        };

        let json = serde_json::to_string(&value).unwrap();
        let parsed: CursorValue = serde_json::from_str(&json).unwrap();

        assert_eq!(value.cursor_key, parsed.cursor_key);
        assert_eq!(value.last_fetch_records, parsed.last_fetch_records);
        assert_eq!(value.api_cursor, parsed.api_cursor);
        assert_eq!(value.version, parsed.version);
    }

    #[test]
    fn test_cursor_value_serialization_without_api_cursor() {
        let value = CursorValue {
            cursor_key: "azure.activity_log".to_string(),
            last_fetch_end: Utc::now(),
            last_fetch_records: 0,
            updated_at: Utc::now(),
            api_cursor: None,
            version: 1,
        };

        let json = serde_json::to_string(&value).unwrap();
        assert!(
            !json.contains("api_cursor"),
            "None fields should be skipped"
        );

        let parsed: CursorValue = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.api_cursor, None);
    }

    #[test]
    fn test_cursor_key_normalised_to_lowercase() {
        assert_eq!(normalize_cursor_key("AWS.CloudTrail"), "aws.cloudtrail");
        assert_eq!(normalize_cursor_key("already_lower"), "already_lower");
        assert_eq!(normalize_cursor_key("UPPER"), "upper");
        assert_eq!(normalize_cursor_key(""), "");
    }
}
