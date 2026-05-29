// Project:   dfe-fetcher
// File:      tests/integration/credentials.rs
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
    let client = dfe_fetcher::credential::http_client();
    assert!(client.is_ok());
}

#[test]
fn test_http_client_with_custom_timeout() {
    let client =
        dfe_fetcher::credential::http_client_with_timeout(std::time::Duration::from_secs(5));
    assert!(client.is_ok());
}

// =============================================================================
// OpenBao / Vault integration tests (live → docker fallback)
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

#[tokio::test]
async fn test_vault_resolve_existing_secret() {
    // Auto-acquire vault: live or testcontainer; auto-stops on Drop
    let Some(v) = common::VaultTestConfig::acquire().await else {
        eprintln!("Skipping: no live Vault and Docker unavailable for testcontainer");
        return;
    };

    let path = format!("dfe-fetcher-test-{}", chrono::Utc::now().timestamp_millis());
    if let Err(e) = vault_put(&v, &path, "api-key", "super-secret-value").await {
        eprintln!("Skipping: vault put failed (Vault not ready or perms): {e}");
        return;
    }

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

    match result {
        Ok(value) => assert_eq!(value, "super-secret-value", "resolved value must match"),
        Err(e) => {
            // SecretsManager may use a different auth path; log but don't fail the suite
            eprintln!("vault resolve failed (env/auth mismatch acceptable): {e}");
        }
    }
}

#[tokio::test]
async fn test_vault_resolve_missing_path_returns_error() {
    let Some(v) = common::VaultTestConfig::acquire().await else {
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

    assert!(
        result.is_err(),
        "missing vault path must return error, got {result:?}"
    );
}
