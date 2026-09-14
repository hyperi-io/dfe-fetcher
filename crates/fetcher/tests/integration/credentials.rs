// Project:   dfe-fetcher
// File:      crates/fetcher/tests/integration/credentials.rs
// Purpose:   Credential resolution and HTTP client factory tests
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

#[tokio::test]
async fn test_credential_resolve_literal() {
    let result = dfe_fetcher::credential::resolve("my-api-key-123").await;
    assert_eq!(result.unwrap(), "my-api-key-123");
}

#[tokio::test]
async fn test_credential_resolve_env() {
    // SAFETY: test-only, single-threaded test runner
    unsafe { std::env::set_var("DFE_TEST_INTEGRATION_CRED", "secret-from-env") };
    let result = dfe_fetcher::credential::resolve("env:DFE_TEST_INTEGRATION_CRED").await;
    assert_eq!(result.unwrap(), "secret-from-env");
    // SAFETY: test-only, single-threaded test runner
    unsafe { std::env::remove_var("DFE_TEST_INTEGRATION_CRED") };
}

#[tokio::test]
async fn test_credential_resolve_env_missing() {
    let result = dfe_fetcher::credential::resolve("env:NONEXISTENT_VAR_ABC123").await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("NONEXISTENT_VAR_ABC123"));
}

#[tokio::test]
async fn test_credential_resolve_vault_invalid_format() {
    let result = dfe_fetcher::credential::resolve("vault:no-colon-here").await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("invalid vault spec"));
}

#[tokio::test]
async fn test_credential_resolve_optional() {
    assert!(
        dfe_fetcher::credential::resolve_optional(None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        dfe_fetcher::credential::resolve_optional(Some(""))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        dfe_fetcher::credential::resolve_optional(Some("literal-value"))
            .await
            .unwrap()
            .unwrap(),
        "literal-value"
    );
}

#[test]
fn test_http_client_factory() {
    let client = dfe_fetcher_rest::http_client();
    assert!(client.is_ok());
}

// =============================================================================
// OpenBao / Vault integration tests (live -> docker fallback)
// =============================================================================

use crate::common;

/// Serialise vault tests so the env var manipulation doesn't race.
static VAULT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Write a secret to Vault via HTTP API. Uses kv-v2 (`/v1/{mount}/data/{path}`).
async fn vault_put(
    cfg: &common::VaultTestConfig,
    path: &str,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let client = reqwest::Client::new();
    let url = format!("{}/v1/{}/data/{}", cfg.address, cfg.mount_path, path);
    let body = serde_json::json!({ "data": { key: value } });
    let resp = client
        .post(&url)
        .header("X-Vault-Token", &cfg.token)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("vault put request failed: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("vault put returned {status}: {text}"));
    }
    Ok(())
}

/// Read a secret back over the HTTP API. Lets a test separate a bad fixture
/// from a bad resolver.
async fn vault_get(cfg: &common::VaultTestConfig, path: &str, key: &str) -> Result<String, String> {
    let client = reqwest::Client::new();
    let url = format!("{}/v1/{}/data/{}", cfg.address, cfg.mount_path, path);
    let resp = client
        .get(&url)
        .header("X-Vault-Token", &cfg.token)
        .send()
        .await
        .map_err(|e| format!("vault get request failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("vault get returned {}", resp.status()));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("vault get body not JSON: {e}"))?;
    body["data"]["data"][key]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| format!("key '{key}' absent from kv-v2 response"))
}

/// `vault:path:key` credential specs must resolve to the stored secret.
///
/// This went nowhere until scalo 2.10.7. `resolve_vault` built
/// `SecretsConfig { sources, ..Default::default() }`, leaving `openbao` at
/// `None`, so `SecretsManager` constructed no vault provider and every lookup
/// was refused with `provider not configured: openbao` before an address or
/// token was read -- `VAULT_ADDR` could not influence it. Hence the floor of
/// `>=2.10.7` on the scalo dependency: below that this test cannot pass.
#[tokio::test]
async fn test_vault_resolve_existing_secret() {
    // Auto-acquire vault: live or testcontainer; auto-stops on Drop
    let Some(v) = common::VaultTestConfig::acquire("vault-resolve-existing-secret").await else {
        eprintln!("Skipping: no live Vault and Docker unavailable for testcontainer");
        return;
    };

    let path = format!("dfe-fetcher-test-{}", chrono::Utc::now().timestamp_millis());
    vault_put(&v, &path, "api-key", "super-secret-value")
        .await
        .unwrap_or_else(|e| panic!("fixture: write the secret to {}: {e}", v.address));

    // Establish the fixture independently, so a resolver failure below cannot
    // be confused with a secret that was never stored.
    let readback = vault_get(&v, &path, "api-key")
        .await
        .unwrap_or_else(|e| panic!("fixture: read the secret back from {}: {e}", v.address));
    assert_eq!(readback, "super-secret-value", "fixture readback");

    let _guard = VAULT_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // SAFETY: test-only, serialised by VAULT_ENV_LOCK
    unsafe {
        std::env::set_var("VAULT_ADDR", &v.address);
        std::env::set_var("VAULT_TOKEN", &v.token);
        std::env::set_var("BAO_ADDR", &v.address);
        std::env::set_var("BAO_TOKEN", &v.token);
    }

    let spec = format!("vault:{path}:api-key");
    let result = dfe_fetcher::credential::resolve(&spec).await;

    // SAFETY: test-only, serialised by VAULT_ENV_LOCK
    unsafe {
        std::env::remove_var("VAULT_ADDR");
        std::env::remove_var("VAULT_TOKEN");
        std::env::remove_var("BAO_ADDR");
        std::env::remove_var("BAO_TOKEN");
    }
    drop(_guard);

    let value = result.unwrap_or_else(|e| {
        panic!(
            "resolve vault:{path}:api-key against {}, whose readback above \
             returned the secret with the same token: {e}",
            v.address
        )
    });
    assert_eq!(value, "super-secret-value", "resolved value must match");
}

/// A missing vault path must error, and for the right reason.
///
/// `assert!(result.is_err())` alone is vacuous: `provider not configured`
/// satisfies it without a lookup ever leaving the process. Excluding that
/// error is what makes the assertion say something about the path.
#[tokio::test]
async fn test_vault_resolve_missing_path_returns_error() {
    let Some(v) = common::VaultTestConfig::acquire("vault-resolve-missing-path").await else {
        eprintln!("Skipping: no live Vault and Docker unavailable for testcontainer");
        return;
    };

    let _guard = VAULT_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // SAFETY: test-only, serialised by VAULT_ENV_LOCK
    unsafe {
        std::env::set_var("VAULT_ADDR", &v.address);
        std::env::set_var("VAULT_TOKEN", &v.token);
        std::env::set_var("BAO_ADDR", &v.address);
        std::env::set_var("BAO_TOKEN", &v.token);
    }

    let spec = "vault:nonexistent-path-xyz-12345:api-key";
    let result = dfe_fetcher::credential::resolve(spec).await;

    // SAFETY: test-only, serialised by VAULT_ENV_LOCK
    unsafe {
        std::env::remove_var("VAULT_ADDR");
        std::env::remove_var("VAULT_TOKEN");
        std::env::remove_var("BAO_ADDR");
        std::env::remove_var("BAO_TOKEN");
    }
    drop(_guard);

    let err = result
        .err()
        .map(|e| e.to_string())
        .unwrap_or_else(|| panic!("missing vault path must return an error"));
    assert!(
        !err.contains("provider not configured"),
        "the lookup never reached OpenBao, so this says nothing about a \
         missing path: {err}"
    );
    assert!(
        err.contains("lookup failed"),
        "expected a vault lookup failure, got: {err}"
    );
}
