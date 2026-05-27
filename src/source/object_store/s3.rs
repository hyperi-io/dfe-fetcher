// Project:   dfe-fetcher
// File:      src/source/object_store/s3.rs
// Purpose:   S3 backend for the object_store source family
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! S3 backend: SigV4-signed ListObjectsV2 + GetObject.
//!
//! Uses `reqsign` directly (same crate the AWS audit-log source uses)
//! rather than the aws-sdk-s3 crate. The fetcher only needs two S3
//! operations (List + Get); the SDK's dependency surface isn't justified.
//!
//! URL style is path-style by default
//! (`https://s3.<region>.amazonaws.com/<bucket>/<key>`). This is the
//! style that works seamlessly with S3-compatible stores (MinIO, R2,
//! Backblaze B2) and with `endpoint_override`. Virtual-host style
//! (`<bucket>.s3.<region>.amazonaws.com`) is supported by AWS for
//! historical reasons but not required.

use std::str::FromStr;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use quick_xml::Reader;
use quick_xml::events::Event;
use reqsign::{AwsCredential, AwsV4Signer};
use sha2::{Digest, Sha256};
use tracing::debug;

use crate::config::S3BackendConfig;
use crate::credential;
use crate::error::{Error, Result};

use super::{ListedObject, MAX_LIST_PAGES, MAX_OBJECTS_PER_TICK};

/// Resolve the S3 backend's static credentials. Vault JSON wins over
/// individual `access_key_id` / `secret_access_key` fields when
/// `credential_secret` is set.
pub async fn resolve_credentials(cfg: &S3BackendConfig) -> Result<(String, String)> {
    if let Some(spec) = cfg.credential_secret.as_deref() {
        let resolved = credential::resolve(spec).await?;
        let creds: serde_json::Value = serde_json::from_str(&resolved).map_err(|_| {
            Error::Credential("object_store.s3 credential_secret is not JSON".into())
        })?;
        let ak = creds["access_key_id"]
            .as_str()
            .or_else(|| creds["AccessKeyId"].as_str())
            .ok_or_else(|| Error::Credential("missing access_key_id in vault secret".into()))?;
        let sk = creds["secret_access_key"]
            .as_str()
            .or_else(|| creds["SecretAccessKey"].as_str())
            .ok_or_else(|| Error::Credential("missing secret_access_key in vault secret".into()))?;
        return Ok((ak.to_string(), sk.to_string()));
    }
    let ak_spec = cfg
        .access_key_id
        .as_deref()
        .ok_or_else(|| Error::Credential("object_store.s3.access_key_id is required".into()))?;
    let sk_spec = cfg
        .secret_access_key
        .as_ref()
        .map(|s| s.expose().to_string())
        .ok_or_else(|| Error::Credential("object_store.s3.secret_access_key is required".into()))?;
    let ak = credential::resolve(ak_spec).await?;
    let sk = credential::resolve(&sk_spec).await?;
    Ok((ak, sk))
}

/// Build the bucket-rooted endpoint, honouring `endpoint_override` for
/// S3-compatible stores.
fn bucket_endpoint(cfg: &S3BackendConfig, bucket: &str) -> String {
    match &cfg.endpoint_override {
        Some(base) => format!("{}/{bucket}", base.trim_end_matches('/')),
        None => format!("https://s3.{}.amazonaws.com/{bucket}", cfg.region),
    }
}

/// SigV4-sign and execute a GET. Body is empty -> the SigV4 payload hash
/// is the well-known SHA256 of the empty string.
async fn signed_get(
    client: &reqwest::Client,
    cfg: &S3BackendConfig,
    url: &str,
) -> Result<reqwest::Response> {
    let (access_key, secret_key) = resolve_credentials(cfg).await?;

    let empty_body_hash = hex::encode(Sha256::digest(b""));

    let mut req = client
        .get(url)
        .header("x-amz-content-sha256", &empty_body_hash)
        .build()
        .map_err(|e| Error::Source(format!("object_store.s3: build request failed: {e}")))?;

    let cred = AwsCredential {
        access_key_id: access_key,
        secret_access_key: secret_key,
        session_token: None,
        expires_in: None,
    };
    let signer = AwsV4Signer::new("s3", &cfg.region);
    signer
        .sign(&mut req, &cred)
        .map_err(|e| Error::Source(format!("object_store.s3: SigV4 signing failed: {e}")))?;

    client
        .execute(req)
        .await
        .map_err(|e| Error::Source(format!("object_store.s3: request failed: {e}")))
}

/// List objects under `prefix` in `bucket` modified strictly after
/// `cutoff`. Sorted ascending by `last_modified`, capped at
/// `MAX_OBJECTS_PER_TICK`. Follows `NextContinuationToken` up to
/// `MAX_LIST_PAGES`.
pub async fn list_new_objects(
    client: &reqwest::Client,
    cfg: &S3BackendConfig,
    bucket: &str,
    prefix: &str,
    cutoff: DateTime<Utc>,
) -> Result<Vec<ListedObject>> {
    let mut out: Vec<ListedObject> = Vec::new();
    let mut continuation: Option<String> = None;

    for page in 0..MAX_LIST_PAGES {
        let mut url = format!(
            "{}/?list-type=2&prefix={}&max-keys=1000",
            bucket_endpoint(cfg, bucket),
            url_encode(prefix),
        );
        if let Some(token) = &continuation {
            url.push_str("&continuation-token=");
            url.push_str(&url_encode(token));
        }

        let resp = signed_get(client, cfg, &url).await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Source(format!(
                "object_store.s3: ListObjectsV2 returned {status} for {bucket}/{prefix}: {body}"
            )));
        }
        let body = resp.text().await.map_err(|e| {
            Error::Source(format!("object_store.s3: ListObjectsV2 read failed: {e}"))
        })?;

        let (objects, next_token) = parse_list_objects_v2(&body)?;
        for obj in objects {
            if obj.last_modified > cutoff {
                out.push(obj);
            }
        }

        match next_token {
            Some(t) => {
                continuation = Some(t);
                debug!(
                    page = page + 1,
                    bucket,
                    prefix,
                    listed = out.len(),
                    "object_store.s3: continuing list"
                );
            }
            None => break,
        }
    }

    out.sort_by_key(|o| o.last_modified);
    out.truncate(MAX_OBJECTS_PER_TICK);
    Ok(out)
}

/// GetObject. Returns the raw body bytes - decompression and parsing
/// happens in the driver.
pub async fn get_object(
    client: &reqwest::Client,
    cfg: &S3BackendConfig,
    bucket: &str,
    key: &str,
) -> Result<Bytes> {
    let url = format!("{}/{}", bucket_endpoint(cfg, bucket), url_encode_path(key));
    let resp = signed_get(client, cfg, &url).await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(Error::Source(format!(
            "object_store.s3: GetObject returned {status} for {bucket}/{key}: {body}"
        )));
    }
    resp.bytes()
        .await
        .map_err(|e| Error::Source(format!("object_store.s3: GetObject body read failed: {e}")))
}

/// Parse a ListObjectsV2 XML response. Returns the list of objects
/// found plus the optional `NextContinuationToken`.
///
/// Reads only the four elements we need: `Key`, `LastModified`, `Size`,
/// `NextContinuationToken`. Tolerates unknown elements (forwards-compat
/// with future S3 schema additions).
fn parse_list_objects_v2(xml: &str) -> Result<(Vec<ListedObject>, Option<String>)> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut objects: Vec<ListedObject> = Vec::new();
    let mut next_token: Option<String> = None;

    let mut current_key: Option<String> = None;
    let mut current_modified: Option<DateTime<Utc>> = None;
    let mut current_size: u64 = 0;

    let mut in_contents = false;
    let mut current_tag: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                if name == "Contents" {
                    in_contents = true;
                    current_key = None;
                    current_modified = None;
                    current_size = 0;
                }
                current_tag = Some(name);
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                if name == "Contents" {
                    if let (Some(k), Some(m)) = (current_key.take(), current_modified.take()) {
                        objects.push(ListedObject {
                            key: k,
                            last_modified: m,
                            size: current_size,
                        });
                    }
                    in_contents = false;
                }
                current_tag = None;
            }
            Ok(Event::Text(t)) => {
                let value = t
                    .decode()
                    .map_err(|e| Error::Source(format!("object_store.s3: XML decode: {e}")))?
                    .into_owned();
                match current_tag.as_deref() {
                    Some("Key") if in_contents => current_key = Some(value),
                    Some("LastModified") if in_contents => {
                        current_modified = DateTime::<Utc>::from_str(&value).ok();
                    }
                    Some("Size") if in_contents => {
                        current_size = value.parse().unwrap_or(0);
                    }
                    Some("NextContinuationToken") => next_token = Some(value),
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => {
                return Err(Error::Source(format!(
                    "object_store.s3: ListObjectsV2 XML parse failed: {e}"
                )));
            }
        }
    }

    Ok((objects, next_token))
}

/// Percent-encode a single query-value or path-segment. RFC 3986
/// unreserved set only. Matches the encoding reqsign expects when
/// computing the canonical request.
fn url_encode(s: &str) -> String {
    const UNRESERVED: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if UNRESERVED.contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Percent-encode a key path. Differs from `url_encode` in that `/`
/// passes through unchanged (S3 keys are slash-delimited).
fn url_encode_path(s: &str) -> String {
    s.split('/').map(url_encode).collect::<Vec<_>>().join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_default_uses_path_style_https() {
        let cfg = S3BackendConfig {
            region: "ap-southeast-2".into(),
            endpoint_override: None,
            access_key_id: None,
            secret_access_key: None,
            credential_secret: None,
            buckets: vec![],
        };
        assert_eq!(
            bucket_endpoint(&cfg, "my-bucket"),
            "https://s3.ap-southeast-2.amazonaws.com/my-bucket"
        );
    }

    #[test]
    fn endpoint_uses_override_with_trailing_slash_trim() {
        let cfg = S3BackendConfig {
            region: "us-east-1".into(),
            endpoint_override: Some("http://localhost:9000/".into()),
            access_key_id: None,
            secret_access_key: None,
            credential_secret: None,
            buckets: vec![],
        };
        assert_eq!(bucket_endpoint(&cfg, "test"), "http://localhost:9000/test");
    }

    #[test]
    fn url_encode_preserves_unreserved_and_escapes_space() {
        assert_eq!(url_encode("AWSLogs"), "AWSLogs");
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("a/b"), "a%2Fb");
    }

    #[test]
    fn url_encode_path_keeps_slashes() {
        assert_eq!(
            url_encode_path("AWSLogs/123/CloudTrail/file.json.gz"),
            "AWSLogs/123/CloudTrail/file.json.gz"
        );
        assert_eq!(
            url_encode_path("dir with space/file"),
            "dir%20with%20space/file"
        );
    }

    #[test]
    fn parse_list_objects_v2_extracts_keys_and_token() {
        // Real S3 response shape, abbreviated.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>my-bucket</Name>
    <Prefix>logs/</Prefix>
    <KeyCount>2</KeyCount>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>true</IsTruncated>
    <NextContinuationToken>abc123</NextContinuationToken>
    <Contents>
        <Key>logs/2026/05/21/file-001.json.gz</Key>
        <LastModified>2026-05-21T10:00:00.000Z</LastModified>
        <ETag>"deadbeef"</ETag>
        <Size>4321</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
    <Contents>
        <Key>logs/2026/05/21/file-002.json.gz</Key>
        <LastModified>2026-05-21T11:30:45.500Z</LastModified>
        <ETag>"cafebabe"</ETag>
        <Size>9999</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
</ListBucketResult>"#;
        let (objs, token) = parse_list_objects_v2(xml).expect("parse");
        assert_eq!(objs.len(), 2);
        assert_eq!(objs[0].key, "logs/2026/05/21/file-001.json.gz");
        assert_eq!(objs[0].size, 4321);
        assert_eq!(objs[1].key, "logs/2026/05/21/file-002.json.gz");
        assert_eq!(token.as_deref(), Some("abc123"));
    }

    #[test]
    fn parse_list_objects_v2_empty_response_returns_no_token() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>my-bucket</Name>
    <Prefix>logs/</Prefix>
    <KeyCount>0</KeyCount>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>false</IsTruncated>
</ListBucketResult>"#;
        let (objs, token) = parse_list_objects_v2(xml).expect("parse");
        assert!(objs.is_empty());
        assert!(token.is_none());
    }
}
