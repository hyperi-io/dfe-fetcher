// Project:   dfe-fetcher
// File:      src/cursor/file.rs
// Purpose:   File-based cursor store with one file per cursor key
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! File-based cursor store — one JSON file per cursor key.
//!
//! Each source service gets its own file in the configured directory:
//! ```text
//! /var/lib/dfe-fetcher/cursors/
//! ├── contoso-m365.m365.audit_log.cursor.json
//! ├── contoso-m365.m365.message_trace.cursor.json
//! └── prod-aws.aws.cloudtrail.cursor.json
//! ```
//!
//! In-memory cache serves reads without file I/O. Writes update the
//! cache then atomically persist the individual file (write `.tmp`,
//! rename). If the directory is not writable, the store operates in
//! degraded mode — cache works for the current process but state is
//! lost on restart.

use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use super::{CursorStore, CursorValue, normalize_cursor_key};
use crate::error::{Error, Result};

/// File extension for cursor files.
const CURSOR_EXTENSION: &str = "cursor.json";

/// File-based cursor store — one `.cursor.json` file per key in a directory.
pub struct FileCursorStore {
    directory: PathBuf,
    cache: RwLock<HashMap<String, CursorValue>>,
    read_only: bool,
}

impl FileCursorStore {
    /// Create a new file cursor store in the given directory.
    ///
    /// Creates the directory if it does not exist. Loads any existing
    /// `.cursor.json` files into the in-memory cache. If the directory
    /// is not writable, falls back to read-only mode.
    pub fn new(directory: &str) -> Result<Self> {
        let dir_path = PathBuf::from(directory);

        // Create directory if needed
        if let Err(e) = std::fs::create_dir_all(&dir_path) {
            warn!(
                directory,
                error = %e,
                "Cursor directory not writable, operating in read-only mode"
            );
            return Ok(Self {
                directory: dir_path,
                cache: RwLock::new(HashMap::new()),
                read_only: true,
            });
        }

        // Load existing cursor files
        let mut cache = HashMap::new();
        if let Ok(entries) = std::fs::read_dir(&dir_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !name.ends_with(CURSOR_EXTENSION) {
                    continue;
                }

                match std::fs::read_to_string(&path) {
                    Ok(content) => match serde_json::from_str::<CursorValue>(&content) {
                        Ok(cursor) => {
                            let key = normalize_cursor_key(&cursor.cursor_key);
                            debug!(key, path = %path.display(), "Loaded cursor");
                            cache.insert(key, cursor);
                        }
                        Err(e) => {
                            warn!(
                                path = %path.display(),
                                error = %e,
                                "Skipping malformed cursor file"
                            );
                        }
                    },
                    Err(e) => {
                        warn!(
                            path = %path.display(),
                            error = %e,
                            "Failed to read cursor file"
                        );
                    }
                }
            }
        }

        // Probe writability
        let read_only = {
            let probe = dir_path.join(".cursor_probe");
            if std::fs::write(&probe, b"ok").is_ok() {
                let _ = std::fs::remove_file(&probe);
                false
            } else {
                warn!(
                    directory,
                    "Cursor directory is not writable, operating in read-only mode"
                );
                true
            }
        };

        info!(
            directory,
            cursors = cache.len(),
            read_only,
            "File cursor store initialised"
        );

        Ok(Self {
            directory: dir_path,
            cache: RwLock::new(cache),
            read_only,
        })
    }

    /// Build the file path for a cursor key.
    ///
    /// Key `contoso-m365.m365.audit_log` → `contoso-m365.m365.audit_log.cursor.json`
    fn cursor_path(&self, normalised_key: &str) -> PathBuf {
        self.directory
            .join(format!("{normalised_key}.{CURSOR_EXTENSION}"))
    }

    /// Persist a single cursor to its file atomically (write `.tmp`, rename).
    fn persist_one(&self, normalised_key: &str, value: &CursorValue) -> Result<()> {
        let path = self.cursor_path(normalised_key);
        let tmp_path = path.with_extension("cursor.json.tmp");

        let json = serde_json::to_string_pretty(value)
            .map_err(|e| Error::Cursor(format!("failed to serialise cursor: {e}")))?;

        std::fs::write(&tmp_path, json.as_bytes()).map_err(|e| {
            Error::Cursor(format!(
                "failed to write cursor temp file '{}': {e}",
                tmp_path.display()
            ))
        })?;

        std::fs::rename(&tmp_path, &path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            Error::Cursor(format!(
                "failed to rename '{}' -> '{}': {e}",
                tmp_path.display(),
                path.display()
            ))
        })?;

        Ok(())
    }

    /// Remove a cursor file from disk.
    fn remove_file(&self, normalised_key: &str) {
        let path = self.cursor_path(normalised_key);
        if let Err(e) = std::fs::remove_file(&path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                path = %path.display(),
                error = %e,
                "Failed to remove cursor file"
            );
        }
    }
}

#[async_trait]
impl CursorStore for FileCursorStore {
    async fn get(&self, key: &str) -> Result<Option<CursorValue>> {
        let normalised = normalize_cursor_key(key);
        let cache = self.cache.read();
        Ok(cache.get(&normalised).cloned())
    }

    async fn set(&self, key: &str, value: &CursorValue) -> Result<()> {
        let normalised = normalize_cursor_key(key);

        {
            let mut cache = self.cache.write();
            cache.insert(normalised.clone(), value.clone());
        }

        if self.read_only {
            warn!(
                key,
                "Cursor store is read-only, cache updated but not persisted to disk"
            );
            return Ok(());
        }

        self.persist_one(&normalised, value)?;
        debug!(
            key = normalised,
            path = %self.cursor_path(&normalised).display(),
            "Cursor persisted"
        );
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let normalised = normalize_cursor_key(key);

        {
            let mut cache = self.cache.write();
            cache.remove(&normalised);
        }

        if !self.read_only {
            self.remove_file(&normalised);
        }

        debug!(key = normalised, "Cursor deleted");
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::TempDir;

    fn make_cursor(key: &str) -> CursorValue {
        CursorValue {
            cursor_key: key.to_string(),
            last_fetch_end: Utc::now(),
            last_fetch_records: 10,
            updated_at: Utc::now(),
            api_cursor: None,
            version: 1,
        }
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        let cursor = make_cursor("aws.cloudtrail");
        store.set("aws.cloudtrail", &cursor).await.unwrap();

        let loaded = store.get("aws.cloudtrail").await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().cursor_key, "aws.cloudtrail");
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();
        assert!(store.get("nonexistent").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        store
            .set("azure.sentinel", &make_cursor("azure.sentinel"))
            .await
            .unwrap();
        assert!(store.get("azure.sentinel").await.unwrap().is_some());

        store.delete("azure.sentinel").await.unwrap();
        assert!(store.get("azure.sentinel").await.unwrap().is_none());

        // File should be gone too
        let path = tmp.path().join("azure.sentinel.cursor.json");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn test_meaningful_filenames() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        store
            .set(
                "contoso-m365.m365.audit_log",
                &make_cursor("contoso-m365.m365.audit_log"),
            )
            .await
            .unwrap();
        store
            .set(
                "prod-aws.aws.cloudtrail",
                &make_cursor("prod-aws.aws.cloudtrail"),
            )
            .await
            .unwrap();

        // Verify filenames are human-readable
        let f1 = tmp.path().join("contoso-m365.m365.audit_log.cursor.json");
        let f2 = tmp.path().join("prod-aws.aws.cloudtrail.cursor.json");
        assert!(f1.exists(), "Expected {}", f1.display());
        assert!(f2.exists(), "Expected {}", f2.display());

        // Verify contents are valid JSON with the right key
        let content: CursorValue =
            serde_json::from_str(&std::fs::read_to_string(&f1).unwrap()).unwrap();
        assert_eq!(content.cursor_key, "contoso-m365.m365.audit_log");
    }

    #[tokio::test]
    async fn test_persistence_across_instances() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // First instance writes
        {
            let store = FileCursorStore::new(dir).unwrap();
            store
                .set("aws.cloudtrail", &make_cursor("aws.cloudtrail"))
                .await
                .unwrap();
            store
                .set("azure.sentinel", &make_cursor("azure.sentinel"))
                .await
                .unwrap();
        }

        // Second instance loads from same directory
        {
            let store = FileCursorStore::new(dir).unwrap();
            assert!(store.get("aws.cloudtrail").await.unwrap().is_some());
            assert!(store.get("azure.sentinel").await.unwrap().is_some());
            assert!(store.get("nonexistent").await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_key_normalisation() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        store
            .set("AWS.CloudTrail", &make_cursor("AWS.CloudTrail"))
            .await
            .unwrap();

        // Retrievable with original and lowercase
        assert!(store.get("AWS.CloudTrail").await.unwrap().is_some());
        assert!(store.get("aws.cloudtrail").await.unwrap().is_some());

        // File uses normalised name
        assert!(tmp.path().join("aws.cloudtrail.cursor.json").exists());
    }

    #[tokio::test]
    async fn test_readonly_fallback() {
        let store = FileCursorStore::new("/proc/nonexistent/cursors").unwrap();

        // set() doesn't panic — updates cache only
        let cursor = make_cursor("test.key");
        store.set("test.key", &cursor).await.unwrap();

        // get() returns from cache even in read-only mode
        assert!(store.get("test.key").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_atomic_write_no_tmp_left() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        store
            .set("gcp.audit", &make_cursor("gcp.audit"))
            .await
            .unwrap();

        // .tmp file should not persist
        let tmp_file = tmp.path().join("gcp.audit.cursor.json.tmp");
        assert!(!tmp_file.exists());
        // Real file should exist
        assert!(tmp.path().join("gcp.audit.cursor.json").exists());
    }

    #[tokio::test]
    async fn test_overwrite() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        let mut c = make_cursor("m365.audit");
        c.last_fetch_records = 5;
        store.set("m365.audit", &c).await.unwrap();

        c.last_fetch_records = 99;
        c.api_cursor = Some("page2".to_string());
        store.set("m365.audit", &c).await.unwrap();

        let loaded = store.get("m365.audit").await.unwrap().unwrap();
        assert_eq!(loaded.last_fetch_records, 99);
        assert_eq!(loaded.api_cursor.as_deref(), Some("page2"));
    }

    #[tokio::test]
    async fn test_corrupted_file_skipped_on_load() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // Write a valid cursor
        {
            let store = FileCursorStore::new(dir).unwrap();
            store
                .set("good.key", &make_cursor("good.key"))
                .await
                .unwrap();
        }

        // Corrupt one file, add another corrupt one
        std::fs::write(
            tmp.path().join("bad.key.cursor.json"),
            b"NOT VALID JSON {{{",
        )
        .unwrap();

        // Load again — should not panic, should skip corrupt file
        let store = FileCursorStore::new(dir).unwrap();
        assert!(
            store.get("good.key").await.unwrap().is_some(),
            "valid cursor should survive"
        );
        assert!(
            store.get("bad.key").await.unwrap().is_none(),
            "corrupt cursor should be skipped"
        );
    }

    #[tokio::test]
    async fn test_cursor_file_deleted_mid_operation() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        store
            .set("aws.cloudtrail", &make_cursor("aws.cloudtrail"))
            .await
            .unwrap();

        // Externally delete the file (simulates file loss)
        let path = tmp.path().join("aws.cloudtrail.cursor.json");
        std::fs::remove_file(&path).unwrap();

        // get() still returns from cache (no panic)
        assert!(store.get("aws.cloudtrail").await.unwrap().is_some());

        // set() should recreate the file (no panic)
        store
            .set("aws.cloudtrail", &make_cursor("aws.cloudtrail"))
            .await
            .unwrap();
        assert!(path.exists(), "file should be recreated after set()");
    }

    #[tokio::test]
    async fn test_empty_directory_deleted_mid_operation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("cursors");
        std::fs::create_dir_all(&dir).unwrap();
        let store = FileCursorStore::new(dir.to_str().unwrap()).unwrap();

        // Delete the directory (simulates PVC unmount or accidental rm)
        std::fs::remove_dir_all(&dir).unwrap();

        // get() returns from cache (no panic)
        assert!(store.get("any.key").await.unwrap().is_none());

        // set() fails gracefully (returns error, doesn't panic)
        let result = store.set("any.key", &make_cursor("any.key")).await;
        // May error (directory gone) but must not panic
        if result.is_err() {
            // Expected — directory is gone
        }
    }

    // ==========================================================================
    // Round-trip equality: a value written must come back exactly the same
    // ==========================================================================

    #[tokio::test]
    async fn test_write_then_read_cycle_returns_equal_value() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        let mut cursor = make_cursor("aws.guardduty");
        cursor.last_fetch_records = 4_242;
        cursor.api_cursor = Some("opaque-token-xyz".to_string());
        cursor.version = 3;

        store.set("aws.guardduty", &cursor).await.unwrap();
        let loaded = store.get("aws.guardduty").await.unwrap().unwrap();

        // CursorValue derives PartialEq — full-value comparison
        assert_eq!(loaded, cursor);
    }

    // ==========================================================================
    // delete() clears the key so subsequent get() returns None
    // ==========================================================================

    #[tokio::test]
    async fn test_delete_removes_key_from_store() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        store
            .set("gcp.logging", &make_cursor("gcp.logging"))
            .await
            .unwrap();
        assert!(store.get("gcp.logging").await.unwrap().is_some());

        store.delete("gcp.logging").await.unwrap();
        assert!(
            store.get("gcp.logging").await.unwrap().is_none(),
            "expected None after delete"
        );
    }

    // ==========================================================================
    // get() on a fresh store (no prior writes) returns Ok(None)
    // ==========================================================================

    #[tokio::test]
    async fn test_get_on_fresh_store_returns_none() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();
        let result = store.get("never.written").await;
        assert!(result.is_ok(), "get() must return Ok on fresh store");
        assert!(result.unwrap().is_none(), "fresh store has no cursors");
    }

    // ==========================================================================
    // Many keys written and read back
    // ==========================================================================

    #[tokio::test]
    async fn test_write_and_read_many_keys() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        let mut keys = Vec::new();
        for i in 0..20 {
            let key = format!("source-{i}.stream-{i}");
            store.set(&key, &make_cursor(&key)).await.unwrap();
            keys.push(key);
        }

        // Read each one back and verify the cursor_key matches (normalised)
        for key in &keys {
            let loaded = store.get(key).await.unwrap();
            assert!(loaded.is_some(), "missing cursor for key {key}");
            assert_eq!(loaded.unwrap().cursor_key, *key);
        }
    }

    // ==========================================================================
    // Persistence across instances — values survive a fresh constructor
    // ==========================================================================

    #[tokio::test]
    async fn test_persistence_new_instance_sees_prior_writes() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        // Write with first instance, then drop it
        {
            let store = FileCursorStore::new(&dir).unwrap();
            let mut cursor = make_cursor("aws.config");
            cursor.last_fetch_records = 999;
            cursor.api_cursor = Some("persist-me".to_string());
            store.set("aws.config", &cursor).await.unwrap();
        }

        // Second instance reads from the same directory
        {
            let store = FileCursorStore::new(&dir).unwrap();
            let loaded = store.get("aws.config").await.unwrap().unwrap();
            assert_eq!(loaded.cursor_key, "aws.config");
            assert_eq!(loaded.last_fetch_records, 999);
            assert_eq!(loaded.api_cursor.as_deref(), Some("persist-me"));
        }
    }

    // ==========================================================================
    // Nested directory creation — FileCursorStore::new() calls create_dir_all
    // ==========================================================================

    #[tokio::test]
    async fn test_new_creates_nested_parent_directories() {
        let tmp = TempDir::new().unwrap();
        let nested = tmp
            .path()
            .join("deeply")
            .join("nested")
            .join("cursor-store");
        assert!(
            !nested.exists(),
            "pre-condition: nested path does not exist"
        );

        let store = FileCursorStore::new(nested.to_str().unwrap()).unwrap();
        assert!(nested.exists(), "new() must create the nested directory");
        assert!(nested.is_dir(), "created path must be a directory");

        // Store must be usable for writes
        store
            .set("nested.source", &make_cursor("nested.source"))
            .await
            .unwrap();
        assert!(
            nested.join("nested.source.cursor.json").exists(),
            "cursor file must be written under the created directory"
        );
    }

    // ==========================================================================
    // Special characters in keys: dots, hyphens, underscores
    // ==========================================================================

    #[tokio::test]
    async fn test_keys_with_dots_hyphens_underscores() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        let keys = [
            "simple",
            "with.dots.in.key",
            "with-hyphens-in-key",
            "with_underscores_in_key",
            "mix.of-all_three.chars",
            "contoso-m365.m365.audit_log",
        ];

        for key in &keys {
            store.set(key, &make_cursor(key)).await.unwrap();
        }

        for key in &keys {
            let loaded = store.get(key).await.unwrap();
            assert!(
                loaded.is_some(),
                "cursor with key '{key}' should be retrievable"
            );
            // Filename must have been created for each
            let expected_file = tmp.path().join(format!("{key}.cursor.json"));
            assert!(
                expected_file.exists(),
                "expected cursor file {} to exist",
                expected_file.display()
            );
        }
    }
}
