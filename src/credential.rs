// Project:   dfe-fetcher
// File:      src/credential.rs
// Purpose:   Credential resolution (vault, env, literal) and OAuth2 token management
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Credential resolution and OAuth2 token management.
//!
//! Resolves credential specifications in the format:
//! - `vault:path:key` — Fetch from secrets manager (OpenBao/Vault)
//! - `env:VAR_NAME` — Read from environment variable
//! - Literal string — Use as-is
//!
//! Also provides an OAuth2 client_credentials token manager with caching.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::debug;

use crate::error::{Error, Result};

/// Resolve a credential specification to its plaintext value.
///
/// Supports three formats:
/// - `vault:secret/path:key` — Resolve via secrets manager
/// - `env:VARIABLE_NAME` — Read from environment
/// - Any other string — Returned as-is (literal)
pub async fn resolve(spec: &str) -> Result<String> {
    if let Some(rest) = spec.strip_prefix("vault:") {
        resolve_vault(rest).await
    } else if let Some(var_name) = spec.strip_prefix("env:") {
        resolve_env(var_name)
    } else {
        Ok(spec.to_string())
    }
}

/// Resolve a vault secret in the format `path:key`.
async fn resolve_vault(path_key: &str) -> Result<String> {
    use hyperi_rustlib::secrets::{SecretSource, SecretsConfig};
    use std::collections::HashMap;

    let parts: Vec<&str> = path_key.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err(Error::Credential(format!(
            "invalid vault spec '{path_key}', expected 'path:key'"
        )));
    }

    let path = parts[0];
    let key = parts[1];

    // Build a SecretsConfig with the requested vault source
    let mut sources = HashMap::new();
    sources.insert(
        "_vault_lookup".to_string(),
        SecretSource::OpenBao {
            path: path.to_string(),
            key: key.to_string(),
        },
    );

    let config = SecretsConfig {
        sources,
        ..Default::default()
    };

    let secrets = hyperi_rustlib::secrets::SecretsManager::new(config)
        .map_err(|e| Error::Credential(format!("failed to initialise secrets manager: {e}")))?;

    let secret_value = secrets
        .get("_vault_lookup")
        .await
        .map_err(|e| Error::Credential(format!("vault lookup failed for {path}:{key}: {e}")))?;

    let text = secret_value
        .as_str()
        .map_err(|e| Error::Credential(format!("vault secret not valid UTF-8: {e}")))?;

    debug!(path = path, key = key, "Resolved vault credential");
    Ok(text.to_string())
}

/// Resolve an environment variable.
fn resolve_env(var_name: &str) -> Result<String> {
    std::env::var(var_name)
        .map_err(|_| Error::Credential(format!("environment variable '{var_name}' not set")))
}

/// Resolve an optional credential spec — returns `None` if the spec is `None`.
pub async fn resolve_optional(spec: Option<&str>) -> Result<Option<String>> {
    match spec {
        Some("") | None => Ok(None),
        Some(s) => Ok(Some(resolve(s).await?)),
    }
}

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
        .timeout(Duration::from_secs(60))
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
    fn test_resolve_literal() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(resolve("my-secret-value"));
        assert_eq!(result.unwrap(), "my-secret-value");
    }

    #[test]
    fn test_resolve_env() {
        // SAFETY: test-only, single-threaded test runner
        unsafe { std::env::set_var("DFE_TEST_CRED_VAR", "test-value-123") };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(resolve("env:DFE_TEST_CRED_VAR"));
        assert_eq!(result.unwrap(), "test-value-123");
        // SAFETY: test-only, single-threaded test runner
        unsafe { std::env::remove_var("DFE_TEST_CRED_VAR") };
    }

    #[test]
    fn test_resolve_env_missing() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(resolve("env:DFE_NONEXISTENT_VAR_XYZ"));
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_optional_none() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(resolve_optional(None));
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_resolve_optional_empty() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(resolve_optional(Some("")));
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_http_client_builds() {
        let client = http_client();
        assert!(client.is_ok());
    }
}
