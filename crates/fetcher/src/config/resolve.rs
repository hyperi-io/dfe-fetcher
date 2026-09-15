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
//! Currently resolves, each at block level and on every connection:
//! - `sources.aws.region`
//! - `sources.azure.tenant_id` and `sources.azure.subscription_id`
//! - `sources.m365.tenant_id`
//! - `sources.gcp.project_id`
//! - `ingest.auth_token`

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
    resolve_opt(&mut config.ingest.auth_token).await?;
    // The identity fields are not secrets, but they are specs: unresolved they
    // reach the provider as literal text and come back as a bad tenant or
    // project rather than as a configuration error.
    let azure = &mut config.sources.azure;
    resolve_opt(&mut azure.tenant_id).await?;
    resolve_opt(&mut azure.subscription_id).await?;
    for conn in &mut azure.connections {
        resolve_opt(&mut conn.tenant_id).await?;
        resolve_opt(&mut conn.subscription_id).await?;
    }
    let m365 = &mut config.sources.m365;
    resolve_opt(&mut m365.tenant_id).await?;
    for conn in &mut m365.connections {
        resolve_opt(&mut conn.tenant_id).await?;
    }
    let gcp = &mut config.sources.gcp;
    resolve_opt(&mut gcp.project_id).await?;
    for conn in &mut gcp.connections {
        resolve_opt(&mut conn.project_id).await?;
    }
    Ok(())
}

/// Resolve an optional spec field in place; `None` stays `None`.
async fn resolve_opt(field: &mut Option<String>) -> Result<(), CredentialError> {
    if let Some(spec) = field.as_deref() {
        *field = Some(resolve(spec).await?);
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

    /// Unresolved, these reach the provider as literal text: Entra answers
    /// AADSTS900023 about a tenant named after the spec string itself.
    #[tokio::test]
    async fn identity_fields_resolve_at_block_and_connection_level() {
        // SAFETY: test-only; unique var names so parallel tests do not collide.
        unsafe {
            std::env::set_var("DFE_FETCHER_TEST_TENANT", "tenant-uuid");
            std::env::set_var("DFE_FETCHER_TEST_SUBSCRIPTION", "sub-uuid");
            std::env::set_var("DFE_FETCHER_TEST_PROJECT", "proj-id");
        }

        let mut cfg = base_config();
        cfg.sources.azure.tenant_id = Some("env:DFE_FETCHER_TEST_TENANT".to_string());
        cfg.sources.azure.subscription_id = Some("env:DFE_FETCHER_TEST_SUBSCRIPTION".to_string());
        cfg.sources
            .azure
            .connections
            .push(crate::config::AzureConnection {
                tenant_id: Some("env:DFE_FETCHER_TEST_TENANT".to_string()),
                subscription_id: Some("env:DFE_FETCHER_TEST_SUBSCRIPTION".to_string()),
                ..Default::default()
            });
        cfg.sources.m365.tenant_id = Some("env:DFE_FETCHER_TEST_TENANT".to_string());
        cfg.sources.gcp.project_id = Some("env:DFE_FETCHER_TEST_PROJECT".to_string());
        // A literal is left alone.
        cfg.sources
            .m365
            .connections
            .push(crate::config::M365Connection {
                tenant_id: Some("literal-tenant".to_string()),
                ..Default::default()
            });

        resolve_config_specs(&mut cfg).await.unwrap();

        assert_eq!(cfg.sources.azure.tenant_id.as_deref(), Some("tenant-uuid"));
        assert_eq!(
            cfg.sources.azure.subscription_id.as_deref(),
            Some("sub-uuid")
        );
        assert_eq!(
            cfg.sources.azure.connections[0].tenant_id.as_deref(),
            Some("tenant-uuid"),
            "a connection carries its own identity and must resolve too"
        );
        assert_eq!(
            cfg.sources.azure.connections[0].subscription_id.as_deref(),
            Some("sub-uuid")
        );
        assert_eq!(cfg.sources.m365.tenant_id.as_deref(), Some("tenant-uuid"));
        assert_eq!(cfg.sources.gcp.project_id.as_deref(), Some("proj-id"));
        assert_eq!(
            cfg.sources.m365.connections[0].tenant_id.as_deref(),
            Some("literal-tenant"),
            "a literal passes through unchanged"
        );

        unsafe {
            std::env::remove_var("DFE_FETCHER_TEST_TENANT");
            std::env::remove_var("DFE_FETCHER_TEST_SUBSCRIPTION");
            std::env::remove_var("DFE_FETCHER_TEST_PROJECT");
        }
    }

    #[tokio::test]
    async fn an_unresolvable_identity_spec_fails_at_startup() {
        let mut cfg = base_config();
        cfg.sources.gcp.project_id = Some("env:DFE_FETCHER_NONEXISTENT_PROJECT_XYZ".to_string());
        let err = resolve_config_specs(&mut cfg).await.unwrap_err();
        match err {
            CredentialError::MissingEnvVar { name } => {
                assert_eq!(name, "DFE_FETCHER_NONEXISTENT_PROJECT_XYZ");
            }
            other => panic!("expected MissingEnvVar, got {other:?}"),
        }
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
