// Project:   dfe-fetcher
// File:      src/cursor/kafka.rs
// Purpose:   Kafka-backed cursor store using a compacted topic
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka-backed cursor store using a single compacted topic.
//!
//! All fetcher instances share the same topic. Each cursor key is stored as
//! a Kafka message where the payload is the serialised `CursorValue` (which
//! includes the `cursor_key` field for identification).
//!
//! On startup, the store consumes from the beginning of the topic to build
//! an in-memory cache. Subsequent reads are served from cache; writes go to
//! both cache and Kafka.
//!
//! ## Compaction note
//!
//! The rustlib `KafkaTransport::send` uses the first argument as the Kafka
//! topic (not a message key), so true key-based compaction is not available
//! through this abstraction. The cursor key is embedded in the JSON payload
//! and the cache is the primary read path. A future iteration may use
//! rdkafka directly to set proper message keys for log compaction.

use std::collections::HashMap;

use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::{debug, warn};

use super::{CursorStore, CursorValue, normalize_cursor_key};
use crate::config::CursorConfig;
use crate::error::{Error, Result};

/// Kafka-backed cursor store with in-memory cache.
pub struct KafkaCursorStore {
    /// In-memory cache of all cursors (populated from Kafka on startup).
    cache: RwLock<HashMap<String, CursorValue>>,

    /// Kafka topic for cursor persistence.
    topic: String,

    /// Rustlib Kafka transport for producing cursor updates.
    transport: hyperi_rustlib::transport::KafkaTransport,
}

impl KafkaCursorStore {
    /// Create a new Kafka cursor store.
    ///
    /// On startup, consumes all existing messages from the cursor topic
    /// to rebuild the in-memory cache. If the topic does not exist or
    /// consumption fails, starts with an empty cache (log warning).
    pub async fn new(
        config: &CursorConfig,
        kafka_config: &hyperi_rustlib::transport::KafkaConfig,
    ) -> Result<Self> {
        let topic = config.kafka_topic.clone();

        // Build a producer-only config for writing cursor updates
        let mut producer_config = kafka_config.clone();
        producer_config.client_id = format!("{}-cursor", producer_config.client_id);
        producer_config.topics = vec![topic.clone()];
        // Consumer group for initial load — unique per startup to always read from beginning
        producer_config.group =
            format!("{}-cursor-{}", producer_config.group, uuid::Uuid::new_v4());
        producer_config.auto_offset_reset = "earliest".to_string();
        producer_config.enable_partition_eof = true;

        let transport = hyperi_rustlib::transport::KafkaTransport::new(&producer_config)
            .await
            .map_err(|e| Error::Cursor(format!("failed to create Kafka cursor transport: {e}")))?;

        let mut cache = HashMap::new();

        // Load existing cursors from the topic
        match Self::load_from_topic(&transport, &mut cache).await {
            Ok(count) => {
                debug!(count, topic, "Loaded cursors from Kafka topic");
            }
            Err(e) => {
                warn!(
                    error = %e,
                    topic,
                    "Failed to load cursors from Kafka topic, starting with empty cache"
                );
            }
        }

        Ok(Self {
            cache: RwLock::new(cache),
            topic,
            transport,
        })
    }

    /// Consume all messages from the cursor topic to rebuild the cache.
    ///
    /// Reads until partition EOF or a timeout is reached. Messages with
    /// invalid JSON are skipped with a warning.
    async fn load_from_topic(
        transport: &hyperi_rustlib::transport::KafkaTransport,
        cache: &mut HashMap<String, CursorValue>,
    ) -> Result<usize> {
        use hyperi_rustlib::transport::Transport;

        let mut loaded = 0;
        let mut consecutive_empty = 0;
        let max_empty_polls = 3;

        loop {
            let messages = transport
                .recv(1000)
                .await
                .map_err(|e| Error::Cursor(format!("failed to consume cursor topic: {e}")))?;

            if messages.is_empty() {
                consecutive_empty += 1;
                if consecutive_empty >= max_empty_polls {
                    break;
                }
                continue;
            }

            consecutive_empty = 0;

            for msg in &messages {
                if msg.payload.is_empty() {
                    // Tombstone — remove from cache
                    // The key information would be in the Kafka message key,
                    // but since we embed cursor_key in the payload, tombstones
                    // without payload cannot identify which cursor to remove.
                    // This is a known limitation of the Transport abstraction.
                    continue;
                }

                match serde_json::from_slice::<CursorValue>(&msg.payload) {
                    Ok(value) => {
                        let key = normalize_cursor_key(&value.cursor_key);
                        cache.insert(key, value);
                        loaded += 1;
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "Skipping invalid cursor message from Kafka"
                        );
                    }
                }
            }

            // Commit offsets so the consumer advances
            if let Some(last) = messages.last() {
                let _ = transport.commit(std::slice::from_ref(&last.token)).await;
            }
        }

        Ok(loaded)
    }
}

#[async_trait]
impl CursorStore for KafkaCursorStore {
    async fn get(&self, key: &str) -> Result<Option<CursorValue>> {
        let normalised = normalize_cursor_key(key);
        let cache = self.cache.read();
        Ok(cache.get(&normalised).cloned())
    }

    async fn set(&self, key: &str, value: &CursorValue) -> Result<()> {
        use hyperi_rustlib::transport::{SendResult, Transport};

        let normalised = normalize_cursor_key(key);

        // Update cache immediately (ensures reads see the latest value even
        // if the Kafka produce is slow or fails)
        {
            let mut cache = self.cache.write();
            cache.insert(normalised.clone(), value.clone());
        }

        // Produce to Kafka for durability
        let payload = serde_json::to_vec(value).map_err(|e| {
            Error::Cursor(format!("failed to serialise cursor for key '{key}': {e}"))
        })?;

        match self.transport.send(&self.topic, &payload).await {
            SendResult::Ok => {
                debug!(
                    key = normalised,
                    topic = self.topic,
                    "Cursor persisted to Kafka"
                );
            }
            SendResult::Backpressured => {
                warn!(
                    key = normalised,
                    "Kafka backpressured on cursor write, cache updated but not persisted"
                );
            }
            SendResult::Fatal(e) => {
                warn!(
                    key = normalised,
                    error = %e,
                    "Failed to persist cursor to Kafka, cache updated but not persisted"
                );
            }
        }

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        use hyperi_rustlib::transport::{SendResult, Transport};

        let normalised = normalize_cursor_key(key);

        // Remove from cache
        {
            let mut cache = self.cache.write();
            cache.remove(&normalised);
        }

        // Produce a tombstone (empty payload) to Kafka
        match self.transport.send(&self.topic, &[]).await {
            SendResult::Ok => {
                debug!(key = normalised, "Cursor tombstone sent to Kafka");
            }
            SendResult::Backpressured | SendResult::Fatal(_) => {
                warn!(
                    key = normalised,
                    "Failed to send cursor tombstone to Kafka, removed from cache only"
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::Utc;

    /// Unit test for the in-memory cache logic — no Kafka needed.
    #[tokio::test]
    async fn test_kafka_cursor_cache_set_get() {
        // Directly test the cache HashMap logic without a real transport
        let cache = RwLock::new(HashMap::new());

        let value = CursorValue {
            cursor_key: "aws.cloudtrail".to_string(),
            last_fetch_end: Utc::now(),
            last_fetch_records: 42,
            updated_at: Utc::now(),
            api_cursor: None,
            version: 1,
        };

        // Insert into cache
        {
            let mut c = cache.write();
            c.insert(normalize_cursor_key("aws.cloudtrail"), value.clone());
        }

        // Read from cache
        {
            let c = cache.read();
            let loaded = c.get(&normalize_cursor_key("aws.cloudtrail"));
            assert!(loaded.is_some());
            assert_eq!(loaded.unwrap().last_fetch_records, 42);
        }

        // Delete from cache
        {
            let mut c = cache.write();
            c.remove(&normalize_cursor_key("aws.cloudtrail"));
        }

        // Verify deleted
        {
            let c = cache.read();
            assert!(c.get(&normalize_cursor_key("aws.cloudtrail")).is_none());
        }
    }

    /// Unit test for cache key normalisation.
    #[tokio::test]
    async fn test_kafka_cursor_cache_normalises_keys() {
        let cache = RwLock::new(HashMap::new());

        let value = CursorValue {
            cursor_key: "Azure.Sentinel".to_string(),
            last_fetch_end: Utc::now(),
            last_fetch_records: 7,
            updated_at: Utc::now(),
            api_cursor: None,
            version: 1,
        };

        {
            let mut c = cache.write();
            c.insert(normalize_cursor_key("Azure.Sentinel"), value);
        }

        // Lookup with different casing
        let c = cache.read();
        assert!(c.get("azure.sentinel").is_some());
    }

    /// Integration test requiring a real Kafka broker.
    #[tokio::test]
    #[ignore = "Requires running Kafka broker — run with: cargo test --test '*' -- --ignored"]
    async fn test_kafka_cursor_roundtrip() {
        use crate::config::CursorConfig;

        let kafka_config = hyperi_rustlib::transport::KafkaConfig {
            brokers: vec!["localhost:9092".to_string()],
            group: "cursor-test".to_string(),
            client_id: "cursor-test".to_string(),
            ..Default::default()
        };

        let cursor_config = CursorConfig {
            kafka_topic: "dfe-fetcher-cursors-test".to_string(),
            ..Default::default()
        };

        let store = KafkaCursorStore::new(&cursor_config, &kafka_config)
            .await
            .unwrap();

        let value = CursorValue {
            cursor_key: "test.roundtrip".to_string(),
            last_fetch_end: Utc::now(),
            last_fetch_records: 100,
            updated_at: Utc::now(),
            api_cursor: Some("page-2".to_string()),
            version: 1,
        };

        store.set("test.roundtrip", &value).await.unwrap();

        // Read from cache (immediate)
        let loaded = store.get("test.roundtrip").await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().last_fetch_records, 100);
    }
}
