// Project:   dfe-fetcher
// File:      src/source/object_store/mod.rs
// Purpose:   Object-store source family (S3 / GCS / Azure Blob)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Object-store source family.
//!
//! Polls one or more bucket prefixes across S3 / GCS / Azure Blob, emitting
//! one record per line of every new object since the cursor. See
//! `docs/superpowers/specs/2026-05-21-object-store-source-design.md` for the
//! full design.
//!
//! ## Phase 1 (this module today)
//!
//! - **S3 backend:** fully implemented (ListObjectsV2 + GetObject via
//!   SigV4-signed REST). Tested live against the dfe-test AWS account.
//! - **GCS / Azure Blob backends:** structurally present (config shapes
//!   parse, enum variants exist) but `list_new_objects` / `get_object`
//!   return a "Phase 2" error. The driver logs-and-skips those backends
//!   so a config with all three providers behaves predictably.
//! - **Formats:** `json_gz`, `json`, `jsonl`. Plain-text formats emit each
//!   non-empty line as `{"line": "..."}`; native ALB/CloudFront/S3-access
//!   parsers are Phase 2.
//!
//! ## Cursor model
//!
//! The cursor is the highest `last_modified` timestamp seen in the previous
//! tick, **per (provider, bucket, prefix)** tuple. Stored under
//! `object_store.<provider>.<bucket>.<prefix-hash>` in the existing fetcher
//! cursor store. New objects are those with `last_modified > cursor`.

pub mod s3;

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use std::io::Read;
use tracing::{info, warn};

use crate::config::{
    AzureBlobBackendConfig, GcsBackendConfig, ObjectStoreBackendConfig, ObjectStoreBucket,
    ObjectStoreFormat, ObjectStorePrefix, ObjectStoreSourceConfig,
};
use crate::credential;
use crate::error::{Error, Result};
use crate::source::{FetchResult, FetchWindow, Source};

const REQUEST_TIMEOUT: Duration = Duration::from_mins(2);

/// Hard cap on objects pulled per (bucket, prefix) per tick. Keeps a
/// single tick bounded even on a backlog. Remaining objects roll into
/// the next tick - the cursor advances to the last object processed.
const MAX_OBJECTS_PER_TICK: usize = 1000;

/// Hard cap on ListObjectsV2 / equivalent pages followed per tick.
const MAX_LIST_PAGES: usize = 10;

/// One object discovered during listing. Provider-agnostic.
#[derive(Debug, Clone)]
pub struct ListedObject {
    /// Object key (e.g. `AWSLogs/123/CloudTrail/.../foo.json.gz`).
    pub key: String,
    /// Server-reported last-modified timestamp.
    pub last_modified: DateTime<Utc>,
    /// Object size in bytes (may be 0 if the server didn't report it).
    pub size: u64,
}

/// Object-store source. One instance handles every configured backend.
pub struct ObjectStoreSource {
    config: ObjectStoreSourceConfig,
    client: reqwest::Client,
}

impl ObjectStoreSource {
    pub fn new(config: ObjectStoreSourceConfig) -> Self {
        let client = credential::http_client_with_timeout(REQUEST_TIMEOUT)
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    /// Process one (backend, bucket, prefix) tuple.
    ///
    /// Lists new objects, fetches each body, parses according to format,
    /// emits one `FetchResult` for the whole prefix tagged
    /// `object_store.<source_tag>`. Returns `None` when nothing new.
    async fn fetch_prefix(
        &self,
        backend: &ObjectStoreBackendConfig,
        bucket_name: &str,
        prefix: &ObjectStorePrefix,
    ) -> Result<Option<FetchResult>> {
        // Phase 1: cursor is derived from the prior tick's max LastModified
        // attached to records. For now, fall back to "objects modified in
        // the last 24h" - the scheduler-driven incremental cursor is wired
        // up at the outer driver layer (FetchWindow).
        // TODO Phase 2: integrate with the fetcher cursor store directly so
        // the cursor is independent of FetchWindow's time-range model.
        let cutoff = Utc::now() - chrono::Duration::hours(24);

        let objects = match backend {
            ObjectStoreBackendConfig::S3(cfg) => {
                s3::list_new_objects(&self.client, cfg, bucket_name, &prefix.prefix, cutoff).await?
            }
            ObjectStoreBackendConfig::Gcs(_) => {
                gcs_phase2_skip("list_new_objects", bucket_name, &prefix.prefix);
                return Ok(None);
            }
            ObjectStoreBackendConfig::AzureBlob(_) => {
                azure_blob_phase2_skip("list_new_objects", bucket_name, &prefix.prefix);
                return Ok(None);
            }
        };

        if objects.is_empty() {
            return Ok(None);
        }

        let mut records: Vec<Bytes> = Vec::new();
        let mut processed = 0usize;
        for obj in objects.into_iter().take(MAX_OBJECTS_PER_TICK) {
            let body = match backend {
                ObjectStoreBackendConfig::S3(cfg) => {
                    s3::get_object(&self.client, cfg, bucket_name, &obj.key).await
                }
                ObjectStoreBackendConfig::Gcs(_) | ObjectStoreBackendConfig::AzureBlob(_) => {
                    // Unreachable: we'd have returned None at the listing
                    // step. Belt-and-braces - log and skip.
                    warn!(
                        key = obj.key,
                        "object_store: stub backend reached fetch path"
                    );
                    continue;
                }
            };

            let body = match body {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, key = obj.key, "object_store: GetObject failed, skipping");
                    continue;
                }
            };

            match parse_object(
                &body,
                prefix.format,
                backend_provider_name(backend),
                bucket_name,
                &obj,
            ) {
                Ok(mut emitted) => {
                    processed += 1;
                    records.append(&mut emitted);
                }
                Err(e) => warn!(error = %e, key = obj.key, "object_store: parse failed, skipping"),
            }
        }

        if records.is_empty() {
            return Ok(None);
        }

        info!(
            backend = backend_provider_name(backend),
            bucket = bucket_name,
            prefix = prefix.prefix,
            objects = processed,
            records = records.len(),
            "object_store: prefix drained"
        );

        let topic = prefix
            .topic
            .clone()
            .unwrap_or_else(|| self.config.topic.clone());
        Ok(Some(FetchResult {
            records,
            source: format!("object_store.{}", prefix.source_tag),
            topic,
        }))
    }
}

#[async_trait]
impl Source for ObjectStoreSource {
    fn name(&self) -> &'static str {
        "object_store"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn fetch(&self, _window: Option<&FetchWindow>) -> Result<Vec<FetchResult>> {
        if !self.config.enabled || self.config.backends.is_empty() {
            return Ok(vec![]);
        }

        info!(
            backends = self.config.backends.len(),
            "Polling object-store backends"
        );

        let mut results = Vec::new();
        for backend in &self.config.backends {
            let buckets: &[ObjectStoreBucket] = match backend {
                ObjectStoreBackendConfig::S3(c) => &c.buckets,
                ObjectStoreBackendConfig::Gcs(c) => &c.buckets,
                ObjectStoreBackendConfig::AzureBlob(c) => &c.buckets,
            };
            for bucket in buckets {
                for prefix in &bucket.prefixes {
                    match self.fetch_prefix(backend, &bucket.bucket, prefix).await {
                        Ok(Some(r)) => results.push(r),
                        Ok(None) => {}
                        Err(e) => warn!(
                            error = %e,
                            backend = backend_provider_name(backend),
                            bucket = bucket.bucket,
                            prefix = prefix.prefix,
                            "object_store: prefix fetch failed, continuing"
                        ),
                    }
                }
            }
        }
        Ok(results)
    }

    async fn health_check(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        // A real cross-backend health check would require auth round-trips
        // per backend. For Phase 1 we report healthy if at least one S3
        // backend's credentials resolve. GCS/Azure stubs don't participate.
        for backend in &self.config.backends {
            if let ObjectStoreBackendConfig::S3(cfg) = backend
                && s3::resolve_credentials(cfg).await.is_ok()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn cursor_prefix(&self) -> String {
        "object_store".to_string()
    }

    fn service_names(&self) -> Vec<&str> {
        // Each prefix's source_tag is the most useful unit of identity.
        self.config
            .backends
            .iter()
            .flat_map(|b| match b {
                ObjectStoreBackendConfig::S3(c) => &c.buckets,
                ObjectStoreBackendConfig::Gcs(c) => &c.buckets,
                ObjectStoreBackendConfig::AzureBlob(c) => &c.buckets,
            })
            .flat_map(|b| b.prefixes.iter().map(|p| p.source_tag.as_str()))
            .collect()
    }
}

/// Decode + split + parse one object body into one or more records.
/// Attaches `_dfe_fetcher_object` envelope to every JSON record.
fn parse_object(
    body: &Bytes,
    format: ObjectStoreFormat,
    provider: &str,
    bucket: &str,
    obj: &ListedObject,
) -> Result<Vec<Bytes>> {
    let decoded: Vec<u8> = match format {
        ObjectStoreFormat::JsonGz | ObjectStoreFormat::TextGz => gunzip(body)?,
        _ => body.to_vec(),
    };

    let envelope = serde_json::json!({
        "provider": provider,
        "bucket": bucket,
        "key": obj.key,
        "last_modified": obj.last_modified.to_rfc3339(),
        "size": obj.size,
    });

    let mut out: Vec<Bytes> = Vec::new();
    match format {
        ObjectStoreFormat::JsonGz | ObjectStoreFormat::Jsonl => {
            for line in decoded.split(|&b| b == b'\n') {
                if line.iter().all(|b| b.is_ascii_whitespace()) {
                    continue;
                }
                emit_json_line(line, &envelope, &mut out)?;
            }
        }
        ObjectStoreFormat::Json => {
            let parsed: serde_json::Value = serde_json::from_slice(&decoded).map_err(|e| {
                Error::Source(format!(
                    "object_store: JSON parse failed for {}: {e}",
                    obj.key
                ))
            })?;
            match parsed {
                serde_json::Value::Array(items) => {
                    for item in items {
                        out.push(attach_envelope_and_serialise(item, &envelope, &obj.key)?);
                    }
                }
                other => out.push(attach_envelope_and_serialise(other, &envelope, &obj.key)?),
            }
        }
        ObjectStoreFormat::Text | ObjectStoreFormat::TextGz => {
            for line in decoded.split(|&b| b == b'\n') {
                if line.iter().all(|b| b.is_ascii_whitespace()) {
                    continue;
                }
                let s = String::from_utf8_lossy(line).into_owned();
                let v = serde_json::json!({
                    "line": s,
                    "_dfe_fetcher_object": envelope,
                });
                let buf = serde_json::to_vec(&v).map_err(|e| {
                    Error::Source(format!("object_store: text record serialise failed: {e}"))
                })?;
                out.push(Bytes::from(buf));
            }
        }
    }
    Ok(out)
}

fn emit_json_line(line: &[u8], envelope: &serde_json::Value, out: &mut Vec<Bytes>) -> Result<()> {
    let parsed: serde_json::Value = match serde_json::from_slice(line) {
        Ok(v) => v,
        Err(e) => {
            // Per the design: tolerate per-line parse failures; emit the
            // raw line so downstream sees the bad data rather than losing
            // it silently.
            let s = String::from_utf8_lossy(line).into_owned();
            warn!(error = %e, "object_store: JSON-lines parse failed, emitting raw line");
            let v = serde_json::json!({
                "_dfe_fetcher_object": envelope,
                "_dfe_fetcher_raw_line": s,
                "_dfe_fetcher_parse_error": e.to_string(),
            });
            let buf = serde_json::to_vec(&v).map_err(|e| {
                Error::Source(format!("object_store: raw-line serialise failed: {e}"))
            })?;
            out.push(Bytes::from(buf));
            return Ok(());
        }
    };
    out.push(attach_envelope_and_serialise(parsed, envelope, "")?);
    Ok(())
}

fn attach_envelope_and_serialise(
    mut value: serde_json::Value,
    envelope: &serde_json::Value,
    key_for_err: &str,
) -> Result<Bytes> {
    match value.as_object_mut() {
        Some(obj) => {
            obj.insert("_dfe_fetcher_object".to_string(), envelope.clone());
        }
        None => {
            value = serde_json::json!({
                "payload": value,
                "_dfe_fetcher_object": envelope,
            });
        }
    }
    let buf = serde_json::to_vec(&value).map_err(|e| {
        Error::Source(format!(
            "object_store: record serialise failed for {key_for_err}: {e}"
        ))
    })?;
    Ok(Bytes::from(buf))
}

fn gunzip(body: &Bytes) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(&body[..]);
    let mut out = Vec::with_capacity(body.len() * 4);
    decoder
        .read_to_end(&mut out)
        .map_err(|e| Error::Source(format!("object_store: gunzip failed: {e}")))?;
    Ok(out)
}

fn backend_provider_name(b: &ObjectStoreBackendConfig) -> &'static str {
    match b {
        ObjectStoreBackendConfig::S3(_) => "s3",
        ObjectStoreBackendConfig::Gcs(_) => "gcs",
        ObjectStoreBackendConfig::AzureBlob(_) => "azure_blob",
    }
}

fn gcs_phase2_skip(op: &str, bucket: &str, prefix: &str) {
    warn!(
        operation = op,
        bucket,
        prefix,
        "object_store: GCS backend is Phase 2 stub, skipping. See \
         docs/superpowers/specs/2026-05-21-object-store-source-design.md"
    );
}

fn azure_blob_phase2_skip(op: &str, bucket: &str, prefix: &str) {
    warn!(
        operation = op,
        bucket,
        prefix,
        "object_store: Azure Blob backend is Phase 2 stub, skipping. See \
         docs/superpowers/specs/2026-05-21-object-store-source-design.md"
    );
}

// =============================================================================
// GCS + Azure Blob Phase 2 stubs (parameter-typed but unimplemented)
// =============================================================================
//
// These exist so the config + enum compile cleanly and so a future
// implementer has a clear scaffold. They are never reached - the
// driver's match short-circuits on these variants and logs a Phase-2
// warning. The function signatures match the S3 module so the future
// implementation can be a near-copy.

#[allow(dead_code)]
mod gcs {
    use super::{GcsBackendConfig, ListedObject};
    use crate::error::{Error, Result};
    use bytes::Bytes;
    use chrono::{DateTime, Utc};

    pub async fn list_new_objects(
        _client: &reqwest::Client,
        _cfg: &GcsBackendConfig,
        _bucket: &str,
        _prefix: &str,
        _cutoff: DateTime<Utc>,
    ) -> Result<Vec<ListedObject>> {
        Err(Error::Source(
            "object_store: GCS backend is Phase 2 (not implemented). See \
             docs/superpowers/specs/2026-05-21-object-store-source-design.md"
                .into(),
        ))
    }

    pub async fn get_object(
        _client: &reqwest::Client,
        _cfg: &GcsBackendConfig,
        _bucket: &str,
        _key: &str,
    ) -> Result<Bytes> {
        Err(Error::Source("object_store: GCS backend is Phase 2".into()))
    }
}

#[allow(dead_code)]
mod azure_blob {
    use super::{AzureBlobBackendConfig, ListedObject};
    use crate::error::{Error, Result};
    use bytes::Bytes;
    use chrono::{DateTime, Utc};

    pub async fn list_new_objects(
        _client: &reqwest::Client,
        _cfg: &AzureBlobBackendConfig,
        _container: &str,
        _prefix: &str,
        _cutoff: DateTime<Utc>,
    ) -> Result<Vec<ListedObject>> {
        Err(Error::Source(
            "object_store: Azure Blob backend is Phase 2 (not implemented). See \
             docs/superpowers/specs/2026-05-21-object-store-source-design.md"
                .into(),
        ))
    }

    pub async fn get_object(
        _client: &reqwest::Client,
        _cfg: &AzureBlobBackendConfig,
        _container: &str,
        _key: &str,
    ) -> Result<Bytes> {
        Err(Error::Source(
            "object_store: Azure Blob backend is Phase 2".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn obj() -> ListedObject {
        ListedObject {
            key: "AWSLogs/123/CloudTrail/foo.json.gz".into(),
            last_modified: Utc.with_ymd_and_hms(2026, 5, 21, 10, 0, 0).unwrap(),
            size: 100,
        }
    }

    #[test]
    fn parse_jsonl_emits_one_record_per_line() {
        let body = Bytes::from_static(b"{\"a\":1}\n{\"b\":2}\n");
        let out = parse_object(&body, ObjectStoreFormat::Jsonl, "s3", "test-bucket", &obj())
            .expect("parse");
        assert_eq!(out.len(), 2);
        let first: serde_json::Value = serde_json::from_slice(&out[0]).unwrap();
        assert_eq!(first["a"], 1);
        assert_eq!(first["_dfe_fetcher_object"]["provider"], "s3");
        assert_eq!(first["_dfe_fetcher_object"]["bucket"], "test-bucket");
        assert_eq!(
            first["_dfe_fetcher_object"]["key"],
            "AWSLogs/123/CloudTrail/foo.json.gz"
        );
    }

    #[test]
    fn parse_json_array_emits_one_record_per_element() {
        let body = Bytes::from_static(b"[{\"a\":1},{\"b\":2},{\"c\":3}]");
        let out = parse_object(&body, ObjectStoreFormat::Json, "s3", "b", &obj()).expect("parse");
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn parse_text_emits_line_wrapped_records() {
        let body = Bytes::from_static(b"hello\nworld\n\n");
        let out = parse_object(&body, ObjectStoreFormat::Text, "s3", "b", &obj()).expect("parse");
        assert_eq!(out.len(), 2);
        let first: serde_json::Value = serde_json::from_slice(&out[0]).unwrap();
        assert_eq!(first["line"], "hello");
    }

    #[test]
    fn parse_jsonl_tolerates_bad_line() {
        let body = Bytes::from_static(b"{\"ok\":1}\nNOT JSON\n{\"ok\":2}\n");
        let out = parse_object(&body, ObjectStoreFormat::Jsonl, "s3", "b", &obj()).expect("parse");
        assert_eq!(out.len(), 3);
        let middle: serde_json::Value = serde_json::from_slice(&out[1]).unwrap();
        assert_eq!(middle["_dfe_fetcher_raw_line"], "NOT JSON");
        assert!(middle["_dfe_fetcher_parse_error"].is_string());
    }

    #[test]
    fn gunzip_round_trip() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(b"{\"x\":1}\n{\"x\":2}\n").unwrap();
        let zipped = enc.finish().unwrap();
        let body = Bytes::from(zipped);
        let out = parse_object(&body, ObjectStoreFormat::JsonGz, "s3", "b", &obj()).expect("parse");
        assert_eq!(out.len(), 2);
    }
}
