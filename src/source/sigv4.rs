//  Project:      dfe-fetcher
//  File:         src/source/sigv4.rs
//  Purpose:      One AWS SigV4 signing seam shared by every AWS-flavoured source.
//  Language:     Rust
//
//  License:      BUSL-1.1
//  Copyright:    (c) 2026 HYPERI PTY LIMITED
//! AWS SigV4 request signing over static credentials.
//!
//! reqsign signs an [`http::request::Parts`], so the caller's `reqwest::Request`
//! is projected into one, signed, and the committed URI + headers copied back.
//! Keeping that in one place means a reqsign API move is a single edit rather
//! than one per AWS source.

use reqsign::aws::{RequestSigner, StaticCredentialProvider};

use crate::error::{Error, Result};

/// Sign `req` in place with SigV4 for `service` in `region`.
///
/// `what` names the caller in error messages ("object_store.s3", "aws.cloudtrail").
pub async fn sign_static(
    req: &mut reqwest::Request,
    what: &str,
    service: &str,
    region: &str,
    access_key_id: &str,
    secret_access_key: &str,
) -> Result<()> {
    let mut builder = http::Request::builder()
        .method(req.method().clone())
        .uri(req.url().as_str());
    if let Some(headers) = builder.headers_mut() {
        *headers = req.headers().clone();
    }
    let (mut parts, ()) = builder
        .body(())
        .map_err(|e| Error::Source(format!("{what}: SigV4 request projection failed: {e}")))?
        .into_parts();

    let signer = reqsign::Signer::new(
        reqsign::default_context(),
        StaticCredentialProvider::new(access_key_id, secret_access_key),
        RequestSigner::new(service, region),
    );
    signer
        .sign(&mut parts, None)
        .await
        .map_err(|e| Error::Source(format!("{what}: SigV4 signing failed: {e}")))?;

    // reqsign commits only the URI and headers, so those are all that copy back.
    *req.headers_mut() = parts.headers;
    *req.url_mut() = url::Url::parse(&parts.uri.to_string())
        .map_err(|e| Error::Source(format!("{what}: signed URI is not a URL: {e}")))?;
    Ok(())
}
