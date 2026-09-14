// Project:   dfe-fetcher
// File:      crates/fetcher/src/config/resolve.rs
// Purpose:   Resolve env:/vault:/literal spec strings on selected config fields
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Post-load spec resolution for dfe-fetcher config.
//!
//! Runs once at startup, after `Config::load()` and before any source is
//! constructed. Resolves `env:VAR_NAME` / `vault:path:key` / literal specs
//! on fields that opt in to the syntax.
//!
//! Currently resolves:
//! - `sources.aws.region` (type-level and each `connections[].region`)

use crate::config::Config;
use crate::credential::{CredentialError, resolve};

/// Resolve all `env:`/`vault:` spec strings on opted-in config fields.
pub async fn resolve_config_specs(config: &mut Config) -> Result<(), CredentialError> {
    config.sources.aws.region = resolve(&config.sources.aws.region).await?;
    // Multi-endpoint: each AWS connection carries its own region, which may
    // also be an `env:`/`vault:` spec.
    for conn in &mut config.sources.aws.connections {
        if let Some(region) = &conn.region {
            conn.region = Some(resolve(region).await?);
        }
    }
    // Unresolved, a `vault:`/`env:` spec here becomes the literal bearer token
    // the server accepts -- an auth setting that reads as configured and is not.
    if let Some(token) = &config.ingest.auth_token {
        config.ingest.auth_token = Some(resolve(token).await?);
    }
    Ok(())
}

#[cfg(test)]
#[allow(unsafe_code, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn base_config() -> Config {
        Config::default()
    }

    #[tokio::test]
    async fn literal_region_passes_through() {
        let mut cfg = base_config();
        cfg.sources.aws.region = "ap-southeast-2".to_string();
        resolve_config_specs(&mut cfg).await.unwrap();
        assert_eq!(cfg.sources.aws.region, "ap-southeast-2");
    }

    #[tokio::test]
    async fn env_region_resolves() {
        // SAFETY: test-only; uses a unique var name to avoid interference with parallel tests
        unsafe { std::env::set_var("DFE_FETCHER_TEST_AWS_REGION", "eu-west-1") };

        let mut cfg = base_config();
        cfg.sources.aws.region = "env:DFE_FETCHER_TEST_AWS_REGION".to_string();
        resolve_config_specs(&mut cfg).await.unwrap();
        assert_eq!(cfg.sources.aws.region, "eu-west-1");

        unsafe { std::env::remove_var("DFE_FETCHER_TEST_AWS_REGION") };
    }

    #[tokio::test]
    async fn ingest_auth_token_resolves() {
        // Unresolved, the spec string itself became the accepted bearer token,
        // so a deployment that configured auth had none.
        // SAFETY: test-only; unique var name so parallel tests do not collide.
        unsafe { std::env::set_var("DFE_FETCHER_TEST_INGEST_TOKEN", "s3cret") };

        let mut cfg = base_config();
        cfg.ingest.auth_token = Some("env:DFE_FETCHER_TEST_INGEST_TOKEN".to_string());
        resolve_config_specs(&mut cfg).await.unwrap();
        assert_eq!(cfg.ingest.auth_token.as_deref(), Some("s3cret"));

        unsafe { std::env::remove_var("DFE_FETCHER_TEST_INGEST_TOKEN") };
    }

    #[tokio::test]
    async fn ingest_off_by_default() {
        assert!(
            !base_config().ingest.enabled,
            "a fetcher offers no send-to surface unless asked"
        );
    }

    #[tokio::test]
    async fn missing_env_returns_clear_error() {
        let mut cfg = base_config();
        cfg.sources.aws.region = "env:DFE_FETCHER_NONEXISTENT_REGION_XYZ".to_string();
        let err = resolve_config_specs(&mut cfg).await.unwrap_err();
        match err {
            CredentialError::MissingEnvVar { name } => {
                assert_eq!(name, "DFE_FETCHER_NONEXISTENT_REGION_XYZ");
            }
            other => panic!("expected MissingEnvVar, got {other:?}"),
        }
    }
}
