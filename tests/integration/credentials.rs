// Project:   dfe-fetcher
// File:      tests/integration/credentials.rs
// Purpose:   Credential resolution and HTTP client factory tests
// Language:  Rust
//
// License:   FSL-1.1-ALv2
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
