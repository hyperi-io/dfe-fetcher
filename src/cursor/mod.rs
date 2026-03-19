// Project:   dfe-fetcher
// File:      src/cursor/mod.rs
// Purpose:   Cursor store for incremental fetching state
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Cursor store for tracking incremental fetch state across restarts.
//!
//! Each source maintains a cursor that records the last successful fetch
//! window, enabling sources to resume from where they left off rather than
//! re-fetching the same time range.
//!
//! Two backends are available:
//! - **File**: One JSON file per cursor key (default for non-Kafka outputs)
//! - **Kafka**: Compacted topic shared across all fetcher instances

pub mod file;
pub mod kafka;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::config::{CursorConfig, OutputConfig};
use crate::error::{Error, Result};

/// Value stored for each cursor, representing the last fetch state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CursorValue {
    /// Unique key identifying this cursor (e.g., "aws.cloudtrail").
    pub cursor_key: String,

    /// End timestamp of the last successful fetch window.
    pub last_fetch_end: DateTime<Utc>,

    /// Number of records returned in the last fetch.
    pub last_fetch_records: u64,

    /// When this cursor was last updated.
    pub updated_at: DateTime<Utc>,

    /// Opaque API pagination cursor from the upstream service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_cursor: Option<String>,

    /// Schema version for forward compatibility.
    pub version: u32,
}

/// Trait for cursor persistence backends.
#[async_trait]
pub trait CursorStore: Send + Sync {
    /// Get the cursor value for a key, or None if no cursor exists.
    async fn get(&self, key: &str) -> Result<Option<CursorValue>>;

    /// Persist a cursor value for a key.
    async fn set(&self, key: &str, value: &CursorValue) -> Result<()>;

    /// Delete a cursor for a key.
    async fn delete(&self, key: &str) -> Result<()>;
}

/// Normalise a cursor key to lowercase for consistent lookups.
#[must_use]
pub fn normalize_cursor_key(key: &str) -> String {
    key.to_lowercase()
}

/// Create the appropriate cursor store based on configuration.
///
/// Resolution logic for `store = "auto"`:
/// - If output includes Kafka, use `KafkaCursorStore`
/// - Otherwise, use `FileCursorStore`
pub async fn create_cursor_store(
    cursor_config: &CursorConfig,
    output_config: &OutputConfig,
) -> Result<Box<dyn CursorStore>> {
    let backend = match cursor_config.store.as_str() {
        "auto" => {
            if output_config.includes_kafka() {
                "kafka"
            } else {
                "file"
            }
        }
        other => other,
    };

    debug!(backend, "Creating cursor store");

    match backend {
        "kafka" => {
            let kafka_config = output_config.kafka.as_ref().ok_or_else(|| {
                Error::Cursor(
                    "cursor store is 'kafka' but no output.kafka config is present".into(),
                )
            })?;
            let store = kafka::KafkaCursorStore::new(cursor_config, kafka_config).await?;
            Ok(Box::new(store))
        }
        "file" => {
            let store = file::FileCursorStore::new(&cursor_config.file_path)?;
            Ok(Box::new(store))
        }
        other => Err(Error::Cursor(format!(
            "unknown cursor store backend: '{other}' (valid: auto, kafka, file)"
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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
