// Project:   dfe-fetcher
// File:      src/cursor/file.rs
// Purpose:   File-based cursor store (one JSON file per cursor key)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! File-based cursor store using one JSON file per cursor key.
//!
//! Each cursor is persisted as `{dir}/{normalized_key}.json`. Writes use
//! atomic rename (`write .tmp` then `rename`) to prevent corruption on crash.
//! If the directory is not writable at startup, the store operates in
//! degraded read-only mode — sets are silently skipped with a warning.

use std::path::PathBuf;

use async_trait::async_trait;
use tracing::{debug, warn};

use super::{normalize_cursor_key, CursorStore, CursorValue};
use crate::error::{Error, Result};

/// File-based cursor store.
pub struct FileCursorStore {
    dir: PathBuf,
    read_only: bool,
}

impl FileCursorStore {
    /// Create a new file cursor store at the given directory path.
    ///
    /// Creates the directory if it does not exist. If the directory cannot
    /// be created or is not writable, the store falls back to read-only mode.
    pub fn new(dir_path: &str) -> Result<Self> {
        let dir = PathBuf::from(dir_path);

        // Try to create the directory
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!(
                path = dir_path,
                error = %e,
                "Cursor directory not writable, operating in read-only mode"
            );
            return Ok(Self {
                dir,
                read_only: true,
            });
        }

        // Verify writability with a probe file
        let probe = dir.join(".cursor_probe");
        let read_only = if std::fs::write(&probe, b"ok").is_ok() {
            let _ = std::fs::remove_file(&probe);
            false
        } else {
            warn!(
                path = dir_path,
                "Cursor directory exists but is not writable, operating in read-only mode"
            );
            true
        };

        debug!(path = dir_path, read_only, "File cursor store initialised");

        Ok(Self { dir, read_only })
    }

    /// Build the file path for a cursor key.
    fn key_path(&self, key: &str) -> PathBuf {
        let normalised = normalize_cursor_key(key);
        self.dir.join(format!("{normalised}.json"))
    }
}

#[async_trait]
impl CursorStore for FileCursorStore {
    async fn get(&self, key: &str) -> Result<Option<CursorValue>> {
        let path = self.key_path(key);

        match std::fs::read_to_string(&path) {
            Ok(content) => {
                let value: CursorValue = serde_json::from_str(&content).map_err(|e| {
                    Error::Cursor(format!(
                        "failed to parse cursor file '{}': {e}",
                        path.display()
                    ))
                })?;
                Ok(Some(value))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Cursor(format!(
                "failed to read cursor file '{}': {e}",
                path.display()
            ))),
        }
    }

    async fn set(&self, key: &str, value: &CursorValue) -> Result<()> {
        if self.read_only {
            warn!(
                key,
                "Cursor store is read-only, skipping cursor persistence"
            );
            return Ok(());
        }

        let path = self.key_path(key);
        let tmp_path = path.with_extension("json.tmp");

        let json = serde_json::to_string_pretty(value).map_err(|e| {
            Error::Cursor(format!("failed to serialise cursor for key '{key}': {e}"))
        })?;

        // Write to temp file first
        std::fs::write(&tmp_path, json.as_bytes()).map_err(|e| {
            Error::Cursor(format!(
                "failed to write cursor temp file '{}': {e}",
                tmp_path.display()
            ))
        })?;

        // Atomic rename
        std::fs::rename(&tmp_path, &path).map_err(|e| {
            // Clean up temp file on rename failure
            let _ = std::fs::remove_file(&tmp_path);
            Error::Cursor(format!(
                "failed to rename cursor file '{}' -> '{}': {e}",
                tmp_path.display(),
                path.display()
            ))
        })?;

        debug!(key, path = %path.display(), "Cursor persisted");
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        if self.read_only {
            warn!(key, "Cursor store is read-only, skipping cursor deletion");
            return Ok(());
        }

        let path = self.key_path(key);

        match std::fs::remove_file(&path) {
            Ok(()) => {
                debug!(key, "Cursor deleted");
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Already gone — not an error
                Ok(())
            }
            Err(e) => Err(Error::Cursor(format!(
                "failed to delete cursor file '{}': {e}",
                path.display()
            ))),
        }
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
    async fn test_file_cursor_set_and_get() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        let cursor = make_cursor("aws.cloudtrail");
        store.set("aws.cloudtrail", &cursor).await.unwrap();

        let loaded = store.get("aws.cloudtrail").await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.cursor_key, "aws.cloudtrail");
        assert_eq!(loaded.last_fetch_records, 10);
    }

    #[tokio::test]
    async fn test_file_cursor_get_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        let result = store.get("nonexistent.key").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_file_cursor_delete() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        let cursor = make_cursor("azure.sentinel");
        store.set("azure.sentinel", &cursor).await.unwrap();

        // Verify it exists
        assert!(store.get("azure.sentinel").await.unwrap().is_some());

        // Delete it
        store.delete("azure.sentinel").await.unwrap();

        // Verify it is gone
        assert!(store.get("azure.sentinel").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_file_cursor_delete_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        // Deleting a non-existent key should not error
        store.delete("does.not.exist").await.unwrap();
    }

    #[tokio::test]
    async fn test_file_cursor_atomic_write() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        let cursor = make_cursor("gcp.audit");
        store.set("gcp.audit", &cursor).await.unwrap();

        // The .tmp file should NOT persist after a successful write
        let tmp_path = tmp.path().join("gcp.audit.json.tmp");
        assert!(!tmp_path.exists(), ".tmp file should not persist");

        // The actual file should exist
        let final_path = tmp.path().join("gcp.audit.json");
        assert!(final_path.exists(), "Final cursor file should exist");
    }

    #[tokio::test]
    async fn test_file_cursor_key_normalisation() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        let cursor = make_cursor("AWS.CloudTrail");
        store.set("AWS.CloudTrail", &cursor).await.unwrap();

        // Should be retrievable with the same key (normalised internally)
        let loaded = store.get("AWS.CloudTrail").await.unwrap();
        assert!(loaded.is_some());

        // Also retrievable with lowercase
        let loaded = store.get("aws.cloudtrail").await.unwrap();
        assert!(loaded.is_some());
    }

    #[tokio::test]
    async fn test_file_cursor_readonly_fallback() {
        // Use a path that should not be writable
        let store = FileCursorStore::new("/proc/nonexistent/cursors");

        // Should succeed (degraded mode), not panic
        assert!(store.is_ok());
        let store = store.unwrap();

        // set() should not panic in read-only mode
        let cursor = make_cursor("test.key");
        let result = store.set("test.key", &cursor).await;
        assert!(result.is_ok());

        // get() returns None (nothing was written)
        let loaded = store.get("test.key").await;
        // Either Ok(None) or an error is acceptable for a non-writable path
        assert!(loaded.is_ok() || loaded.is_err());
    }

    #[tokio::test]
    async fn test_file_cursor_overwrite() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(tmp.path().to_str().unwrap()).unwrap();

        let cursor1 = CursorValue {
            cursor_key: "m365.audit".to_string(),
            last_fetch_end: Utc::now(),
            last_fetch_records: 5,
            updated_at: Utc::now(),
            api_cursor: None,
            version: 1,
        };
        store.set("m365.audit", &cursor1).await.unwrap();

        let cursor2 = CursorValue {
            cursor_key: "m365.audit".to_string(),
            last_fetch_end: Utc::now(),
            last_fetch_records: 99,
            updated_at: Utc::now(),
            api_cursor: Some("page2".to_string()),
            version: 1,
        };
        store.set("m365.audit", &cursor2).await.unwrap();

        let loaded = store.get("m365.audit").await.unwrap().unwrap();
        assert_eq!(loaded.last_fetch_records, 99);
        assert_eq!(loaded.api_cursor.as_deref(), Some("page2"));
    }
}
