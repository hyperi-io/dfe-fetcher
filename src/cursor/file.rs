// Project:   dfe-fetcher
// File:      src/cursor/file.rs
// Purpose:   Single-file JSON cursor store with in-memory cache
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Single-file JSON cursor store.
//!
//! All cursors are persisted as a single JSON map in `file_path`. An
//! in-memory `RwLock<HashMap>` cache serves reads without file I/O.
//! Writes update the cache then atomically persist the entire map
//! (write to `.tmp`, rename). If the path is not writable at startup,
//! the store operates in degraded read-only mode — sets are silently
//! skipped with a warning (cache still works for the current process).

use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::{debug, warn};

use super::{CursorStore, CursorValue, normalize_cursor_key};
use crate::error::{Error, Result};

/// File-based cursor store backed by a single JSON file.
pub struct FileCursorStore {
    file_path: PathBuf,
    cache: RwLock<HashMap<String, CursorValue>>,
    read_only: bool,
}

impl FileCursorStore {
    /// Create a new file cursor store at the given file path.
    ///
    /// Creates the parent directory if it does not exist. If the file already
    /// exists, loads its contents into the in-memory cache. If the path is
    /// not writable, falls back to read-only mode.
    pub fn new(path: &str) -> Result<Self> {
        let file_path = PathBuf::from(path);

        // Create parent directory if needed
        if let Some(parent) = file_path.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            warn!(
                path,
                error = %e,
                "Cursor file parent directory not writable, operating in read-only mode"
            );
            return Ok(Self {
                file_path,
                cache: RwLock::new(HashMap::new()),
                read_only: true,
            });
        }

        // Load existing data if the file exists
        let cache = match std::fs::read_to_string(&file_path) {
            Ok(content) => {
                let map: HashMap<String, CursorValue> =
                    serde_json::from_str(&content).map_err(|e| {
                        Error::Cursor(format!(
                            "failed to parse cursor file '{}': {e}",
                            file_path.display()
                        ))
                    })?;
                debug!(path, count = map.len(), "Loaded cursors from file");
                map
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!(path, "Cursor file does not exist yet, starting empty");
                HashMap::new()
            }
            Err(e) => {
                warn!(
                    path,
                    error = %e,
                    "Failed to read cursor file, starting with empty cache"
                );
                HashMap::new()
            }
        };

        // Probe writability with a temp file in the same directory
        let read_only = if let Some(parent) = file_path.parent() {
            let probe = parent.join(".cursor_probe");
            if std::fs::write(&probe, b"ok").is_ok() {
                let _ = std::fs::remove_file(&probe);
                false
            } else {
                warn!(
                    path,
                    "Cursor file path is not writable, operating in read-only mode"
                );
                true
            }
        } else {
            true
        };

        debug!(path, read_only, "File cursor store initialised");

        Ok(Self {
            file_path,
            cache: RwLock::new(cache),
            read_only,
        })
    }

    /// Persist the entire cache to the file atomically (write .tmp then rename).
    fn persist(&self) -> Result<()> {
        let cache = self.cache.read();
        let json = serde_json::to_string_pretty(&*cache)
            .map_err(|e| Error::Cursor(format!("failed to serialise cursor cache: {e}")))?;
        drop(cache);

        let tmp_path = self.file_path.with_extension("json.tmp");

        std::fs::write(&tmp_path, json.as_bytes()).map_err(|e| {
            Error::Cursor(format!(
                "failed to write cursor temp file '{}': {e}",
                tmp_path.display()
            ))
        })?;

        std::fs::rename(&tmp_path, &self.file_path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            Error::Cursor(format!(
                "failed to rename cursor file '{}' -> '{}': {e}",
                tmp_path.display(),
                self.file_path.display()
            ))
        })?;

        Ok(())
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

        self.persist()?;
        debug!(key = normalised, path = %self.file_path.display(), "Cursor persisted");
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let normalised = normalize_cursor_key(key);

        {
            let mut cache = self.cache.write();
            cache.remove(&normalised);
        }

        if self.read_only {
            warn!(
                key,
                "Cursor store is read-only, cache updated but not persisted to disk"
            );
            return Ok(());
        }

        self.persist()?;
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

    fn store_path(dir: &TempDir) -> String {
        dir.path()
            .join("cursors.json")
            .to_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn test_file_cursor_set_and_get() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(&store_path(&tmp)).unwrap();

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
        let store = FileCursorStore::new(&store_path(&tmp)).unwrap();

        let result = store.get("nonexistent.key").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_file_cursor_delete() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(&store_path(&tmp)).unwrap();

        let cursor = make_cursor("azure.sentinel");
        store.set("azure.sentinel", &cursor).await.unwrap();

        assert!(store.get("azure.sentinel").await.unwrap().is_some());

        store.delete("azure.sentinel").await.unwrap();

        assert!(store.get("azure.sentinel").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_file_cursor_delete_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(&store_path(&tmp)).unwrap();

        // Deleting a non-existent key should not error
        store.delete("does.not.exist").await.unwrap();
    }

    #[tokio::test]
    async fn test_file_cursor_atomic_write() {
        let tmp = TempDir::new().unwrap();
        let path = store_path(&tmp);
        let store = FileCursorStore::new(&path).unwrap();

        let cursor = make_cursor("gcp.audit");
        store.set("gcp.audit", &cursor).await.unwrap();

        // The .tmp file should NOT persist after a successful write
        let tmp_path = PathBuf::from(&path).with_extension("json.tmp");
        assert!(!tmp_path.exists(), ".tmp file should not persist");

        // The actual file should exist
        assert!(PathBuf::from(&path).exists(), "Cursor file should exist");
    }

    #[tokio::test]
    async fn test_file_cursor_key_normalisation() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(&store_path(&tmp)).unwrap();

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
        let store = FileCursorStore::new("/proc/nonexistent/cursors.json");

        // Should succeed (degraded mode), not panic
        assert!(store.is_ok());
        let store = store.unwrap();

        // set() should not panic in read-only mode (updates cache only)
        let cursor = make_cursor("test.key");
        let result = store.set("test.key", &cursor).await;
        assert!(result.is_ok());

        // get() returns from cache (was set above even in read-only mode)
        let loaded = store.get("test.key").await.unwrap();
        assert!(loaded.is_some(), "cache should work even in read-only mode");
    }

    #[tokio::test]
    async fn test_file_cursor_overwrite() {
        let tmp = TempDir::new().unwrap();
        let store = FileCursorStore::new(&store_path(&tmp)).unwrap();

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

    #[tokio::test]
    async fn test_file_cursor_persistence_across_instances() {
        let tmp = TempDir::new().unwrap();
        let path = store_path(&tmp);

        // First instance writes cursors
        {
            let store = FileCursorStore::new(&path).unwrap();
            let cursor = make_cursor("aws.cloudtrail");
            store.set("aws.cloudtrail", &cursor).await.unwrap();

            let cursor2 = make_cursor("azure.sentinel");
            store.set("azure.sentinel", &cursor2).await.unwrap();
        }

        // Second instance loads from the same file
        {
            let store = FileCursorStore::new(&path).unwrap();
            let loaded = store.get("aws.cloudtrail").await.unwrap();
            assert!(loaded.is_some(), "cursor should survive across instances");
            assert_eq!(loaded.unwrap().cursor_key, "aws.cloudtrail");

            let loaded2 = store.get("azure.sentinel").await.unwrap();
            assert!(loaded2.is_some());
        }
    }

    #[tokio::test]
    async fn test_file_cursor_multiple_keys_in_single_file() {
        let tmp = TempDir::new().unwrap();
        let path = store_path(&tmp);
        let store = FileCursorStore::new(&path).unwrap();

        store
            .set("source.a", &make_cursor("source.a"))
            .await
            .unwrap();
        store
            .set("source.b", &make_cursor("source.b"))
            .await
            .unwrap();
        store
            .set("source.c", &make_cursor("source.c"))
            .await
            .unwrap();

        // Verify the file contains all three as a JSON map
        let content = std::fs::read_to_string(&path).unwrap();
        let map: HashMap<String, CursorValue> = serde_json::from_str(&content).unwrap();
        assert_eq!(map.len(), 3);
        assert!(map.contains_key("source.a"));
        assert!(map.contains_key("source.b"));
        assert!(map.contains_key("source.c"));

        // Delete one and verify file is updated
        store.delete("source.b").await.unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        let map: HashMap<String, CursorValue> = serde_json::from_str(&content).unwrap();
        assert_eq!(map.len(), 2);
        assert!(!map.contains_key("source.b"));
    }
}
