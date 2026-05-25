// Project:   dfe-fetcher
// File:      src/credential.rs
// Purpose:   Credential re-exports + OAuth2 token management + HTTP client factory
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Credential resolution and OAuth2 token management.
//!
//! `resolve` / `resolve_optional` / `CredentialError` are re-exported from
//! [`hyperi_rustlib::credential`]. This module additionally owns the
//! fetcher-specific OAuth2 [`TokenManager`] and shared HTTP client
//! factories.

pub use hyperi_rustlib::credential::{resolve, resolve_optional, CredentialError};

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::debug;

use crate::error::{Error, Result};

// =============================================================================
// OAuth2 Client Credentials Token Manager
// =============================================================================

/// Cached OAuth2 token with expiry tracking.
#[derive(Clone)]
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

/// OAuth2 client_credentials token manager with automatic caching.
///
/// Fetches tokens from the specified token endpoint and caches them
/// until 60 seconds before expiry.
pub struct TokenManager {
    client: reqwest::Client,
    token_url: String,
    client_id: String,
    client_secret: String,
    scope: String,
    cached: Arc<RwLock<Option<CachedToken>>>,
}

impl TokenManager {
    /// Create a new token manager.
    pub fn new(
        client: reqwest::Client,
        token_url: String,
        client_id: String,
        client_secret: String,
        scope: String,
    ) -> Self {
        Self {
            client,
            token_url,
            client_id,
            client_secret,
            scope,
            cached: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a token manager for Microsoft Entra ID (Azure AD).
    pub fn microsoft(
        client: reqwest::Client,
        tenant_id: &str,
        client_id: String,
        client_secret: String,
        scope: String,
    ) -> Self {
        let token_url = format!("https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token");
        Self::new(client, token_url, client_id, client_secret, scope)
    }

    /// Get a valid access token, refreshing if necessary.
    pub async fn get_token(&self) -> Result<String> {
        // Check cache
        {
            let cached = self.cached.read();
            if let Some(ref token) = *cached
                && Instant::now() < token.expires_at
            {
                return Ok(token.access_token.clone());
            }
        }

        // Fetch new token
        self.refresh_token().await
    }

    /// Force refresh the token.
    async fn refresh_token(&self) -> Result<String> {
        debug!(token_url = %self.token_url, "Refreshing OAuth2 token");

        let resp = self
            .client
            .post(&self.token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("scope", &self.scope),
            ])
            .send()
            .await
            .map_err(|e| Error::Credential(format!("token request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Credential(format!(
                "token endpoint returned {status}: {body}"
            )));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| Error::Credential(format!("failed to parse token response: {e}")))?;

        let access_token = body["access_token"]
            .as_str()
            .ok_or_else(|| Error::Credential("missing access_token in response".into()))?
            .to_string();

        let expires_in = body["expires_in"].as_u64().unwrap_or(3600);

        // Cache with 60s safety margin
        let expires_at = Instant::now() + Duration::from_secs(expires_in.saturating_sub(60));

        {
            let mut cached = self.cached.write();
            *cached = Some(CachedToken {
                access_token: access_token.clone(),
                expires_at,
            });
        }

        debug!(expires_in_secs = expires_in, "OAuth2 token refreshed");
        Ok(access_token)
    }
}

// =============================================================================
// Shared HTTP Client Factory
// =============================================================================

/// Build a shared `reqwest::Client` with standard defaults.
///
/// All sources should use this client for consistency:
/// - 30s connect timeout, 60s total timeout
/// - User-Agent header
/// - TLS via rustls
/// - Gzip/brotli decompression
pub fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(format!("dfe-fetcher/{}", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_mins(1))
        .pool_max_idle_per_host(10)
        .build()
        .map_err(|e| Error::Source(format!("failed to build HTTP client: {e}")))
}

/// Build an HTTP client that accepts a custom timeout.
pub fn http_client_with_timeout(timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(format!("dfe-fetcher/{}", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(30))
        .timeout(timeout)
        .pool_max_idle_per_host(10)
        .build()
        .map_err(|e| Error::Source(format!("failed to build HTTP client: {e}")))
}

#[cfg(test)]
#[allow(unsafe_code, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_http_client_builds() {
        let client = http_client();
        assert!(client.is_ok());
    }
}
