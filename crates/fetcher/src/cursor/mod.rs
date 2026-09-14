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

pub use dfe_fetcher_core::checkpoint::{CursorStore, CursorValue, normalize_cursor_key};

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::Utc;

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
