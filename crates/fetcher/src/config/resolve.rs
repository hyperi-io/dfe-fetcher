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
//! Resolves `ingest.auth_token` and, at block level and on every connection,
//! each source's plain identity fields: the identifiers a provider is
//! addressed by (tenant, subscription, project, account, organisation, user,
//! client id, tenant URL, API host) plus AWS `region` and `assume_role_arn`
//! and Duo's `integration_key`. The pass below is the list.
//!
//! Provider secrets are not here. The REST crate resolves those on first use
//! through `Secret`, and a value resolved at load would sit in plaintext in
//! the config for the life of the process.
//!
//! A block that is not enabled is skipped: it is never instantiated, so a spec
//! left behind in one must not stop the process starting.

use crate::config::Config;
use crate::credential::{CredentialError, resolve};

/// Resolve all `env:`/`vault:` spec strings on opted-in config fields.
pub async fn resolve_config_specs(config: &mut Config) -> Result<(), CredentialError> {
    let aws = &mut config.sources.aws;
    if aws.enabled {
        aws.region = resolve(&aws.region).await?;
        resolve_opt(&mut aws.assume_role_arn).await?;
        // Multi-endpoint: each AWS connection carries its own region, which
        // may also be an `env:`/`vault:` spec.
        for conn in &mut aws.connections {
            if let Some(region) = &conn.region {
                conn.region = Some(resolve(region).await?);
            }
            resolve_opt(&mut conn.assume_role_arn).await?;
        }
    }
    // Unresolved, a `vault:`/`env:` spec here becomes the literal bearer token
    // the server accepts -- an auth setting that reads as configured and is not.
    if config.ingest.enabled {
        resolve_opt(&mut config.ingest.auth_token).await?;
    }
    // The identity fields are not secrets, but they are specs: unresolved they
    // reach the provider as literal text and come back as a bad tenant or
    // project rather than as a configuration error.
    let azure = &mut config.sources.azure;
    if azure.enabled {
        resolve_opt(&mut azure.tenant_id).await?;
        resolve_opt(&mut azure.subscription_id).await?;
        resolve_opt(&mut azure.client_id).await?;
        for conn in &mut azure.connections {
            resolve_opt(&mut conn.tenant_id).await?;
            resolve_opt(&mut conn.subscription_id).await?;
            resolve_opt(&mut conn.client_id).await?;
        }
    }
    let m365 = &mut config.sources.m365;
    if m365.enabled {
        resolve_opt(&mut m365.tenant_id).await?;
        resolve_opt(&mut m365.client_id).await?;
        for conn in &mut m365.connections {
            resolve_opt(&mut conn.tenant_id).await?;
            resolve_opt(&mut conn.client_id).await?;
        }
    }
    let gcp = &mut config.sources.gcp;
    if gcp.enabled {
        resolve_opt(&mut gcp.project_id).await?;
        for conn in &mut gcp.connections {
            resolve_opt(&mut conn.project_id).await?;
        }
    }
    // `client_id`, `username` and `integration_key` are identifiers the REST
    // crate deliberately keeps out of its secret specs, so nothing downstream
    // resolves them either.
    let github = &mut config.sources.github;
    if github.enabled {
        resolve_opt(&mut github.org).await?;
        resolve_opt(&mut github.enterprise).await?;
        for conn in &mut github.connections {
            resolve_opt(&mut conn.org).await?;
            resolve_opt(&mut conn.enterprise).await?;
        }
    }
    let okta = &mut config.sources.okta;
    if okta.enabled {
        resolve_opt(&mut okta.tenant_url).await?;
        for conn in &mut okta.connections {
            resolve_opt(&mut conn.tenant_url).await?;
        }
    }
    let cloudflare = &mut config.sources.cloudflare;
    if cloudflare.enabled {
        resolve_opt(&mut cloudflare.account_id).await?;
        for conn in &mut cloudflare.connections {
            resolve_opt(&mut conn.account_id).await?;
        }
    }
    let crowdstrike = &mut config.sources.crowdstrike;
    if crowdstrike.enabled {
        resolve_opt(&mut crowdstrike.client_id).await?;
        for conn in &mut crowdstrike.connections {
            resolve_opt(&mut conn.client_id).await?;
        }
    }
    let bitwarden = &mut config.sources.bitwarden;
    if bitwarden.enabled {
        resolve_opt(&mut bitwarden.client_id).await?;
        for conn in &mut bitwarden.connections {
            resolve_opt(&mut conn.client_id).await?;
        }
    }
    let duo = &mut config.sources.duo;
    if duo.enabled {
        resolve_opt(&mut duo.api_host).await?;
        resolve_opt(&mut duo.integration_key).await?;
        for conn in &mut duo.connections {
            resolve_opt(&mut conn.api_host).await?;
            resolve_opt(&mut conn.integration_key).await?;
        }
    }
    let workspace = &mut config.sources.google_workspace;
    if workspace.enabled {
        resolve_opt(&mut workspace.admin_email).await?;
        resolve_opt(&mut workspace.customer_id).await?;
        for conn in &mut workspace.connections {
            resolve_opt(&mut conn.admin_email).await?;
            resolve_opt(&mut conn.customer_id).await?;
        }
    }
    let salesforce = &mut config.sources.salesforce;
    if salesforce.enabled {
        resolve_opt(&mut salesforce.login_url).await?;
        resolve_opt(&mut salesforce.client_id).await?;
        resolve_opt(&mut salesforce.username).await?;
        for conn in &mut salesforce.connections {
            resolve_opt(&mut conn.login_url).await?;
            resolve_opt(&mut conn.client_id).await?;
            resolve_opt(&mut conn.username).await?;
        }
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
        cfg.sources.aws.enabled = true;
        cfg.sources.aws.region = "ap-southeast-2".to_string();
        resolve_config_specs(&mut cfg).await.unwrap();
        assert_eq!(cfg.sources.aws.region, "ap-southeast-2");
    }

    #[tokio::test]
    async fn env_region_resolves() {
        // SAFETY: test-only; uses a unique var name to avoid interference with parallel tests
        unsafe { std::env::set_var("DFE_FETCHER_TEST_AWS_REGION", "eu-west-1") };

        let mut cfg = base_config();
        cfg.sources.aws.enabled = true;
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
        cfg.ingest.enabled = true;
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
        cfg.sources.azure.enabled = true;
        cfg.sources.m365.enabled = true;
        cfg.sources.gcp.enabled = true;
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
        cfg.sources
            .m365
            .connections
            .push(crate::config::M365Connection {
                tenant_id: Some("env:DFE_FETCHER_TEST_TENANT".to_string()),
                ..Default::default()
            });
        // A literal is left alone.
        cfg.sources
            .m365
            .connections
            .push(crate::config::M365Connection {
                tenant_id: Some("literal-tenant".to_string()),
                ..Default::default()
            });
        cfg.sources
            .gcp
            .connections
            .push(crate::config::GcpConnection {
                project_id: Some("env:DFE_FETCHER_TEST_PROJECT".to_string()),
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
            Some("tenant-uuid"),
            "deleting the m365 connection loop must fail this"
        );
        assert_eq!(
            cfg.sources.m365.connections[1].tenant_id.as_deref(),
            Some("literal-tenant"),
            "a literal passes through unchanged"
        );
        assert_eq!(
            cfg.sources.gcp.connections[0].project_id.as_deref(),
            Some("proj-id"),
            "deleting the gcp connection loop must fail this"
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
        cfg.sources.gcp.enabled = true;
        cfg.sources.gcp.project_id = Some("env:DFE_FETCHER_NONEXISTENT_PROJECT_XYZ".to_string());
        let err = resolve_config_specs(&mut cfg).await.unwrap_err();
        match err {
            CredentialError::MissingEnvVar { name } => {
                assert_eq!(name, "DFE_FETCHER_NONEXISTENT_PROJECT_XYZ");
            }
            other => panic!("expected MissingEnvVar, got {other:?}"),
        }
    }

    /// A block nobody enabled is never instantiated, so a spec left behind in
    /// one cannot stop the process starting.
    #[tokio::test]
    async fn a_disabled_block_with_an_unresolvable_spec_does_not_stop_startup() {
        let mut cfg = base_config();
        assert!(
            !cfg.sources.gcp.enabled && !cfg.sources.azure.enabled && !cfg.sources.aws.enabled,
            "the default disables every source"
        );
        cfg.sources.gcp.project_id = Some("env:DFE_FETCHER_NONEXISTENT_PROJECT_ABC".to_string());
        cfg.sources.azure.tenant_id = Some("env:DFE_FETCHER_NONEXISTENT_TENANT_ABC".to_string());
        cfg.sources.aws.region = "env:DFE_FETCHER_NONEXISTENT_REGION_ABC".to_string();
        resolve_config_specs(&mut cfg)
            .await
            .expect("a disabled block cannot stop startup");
        assert_eq!(
            cfg.sources.gcp.project_id.as_deref(),
            Some("env:DFE_FETCHER_NONEXISTENT_PROJECT_ABC"),
            "and its spec is left as written rather than half-resolved"
        );
    }

    /// Every plain identity field resolves at block level and on every
    /// connection. One variable backs every field, so a call site deleted
    /// from the pass leaves its spec unresolved and fails this test by name.
    #[tokio::test]
    async fn every_identity_field_resolves_at_block_and_connection_level() {
        use crate::config::{
            AwsConnection, AzureConnection, BitwardenConnection, CloudflareConnection,
            CrowdstrikeConnection, DuoConnection, GithubConnection, GoogleWorkspaceConnection,
            M365Connection, OktaConnection, SalesforceConnection,
        };
        // SAFETY: test-only; unique var name so parallel tests do not collide.
        unsafe { std::env::set_var("DFE_FETCHER_TEST_IDENTITY", "resolved") };
        let spec = || Some("env:DFE_FETCHER_TEST_IDENTITY".to_string());

        let mut cfg = base_config();
        let s = &mut cfg.sources;
        s.aws.enabled = true;
        s.aws.assume_role_arn = spec();
        s.aws.connections.push(AwsConnection {
            assume_role_arn: spec(),
            ..Default::default()
        });
        s.azure.enabled = true;
        s.azure.client_id = spec();
        s.azure.connections.push(AzureConnection {
            client_id: spec(),
            ..Default::default()
        });
        s.m365.enabled = true;
        s.m365.client_id = spec();
        s.m365.connections.push(M365Connection {
            client_id: spec(),
            ..Default::default()
        });
        s.github.enabled = true;
        s.github.org = spec();
        s.github.enterprise = spec();
        s.github.connections.push(GithubConnection {
            org: spec(),
            enterprise: spec(),
            ..Default::default()
        });
        s.okta.enabled = true;
        s.okta.tenant_url = spec();
        s.okta.connections.push(OktaConnection {
            tenant_url: spec(),
            ..Default::default()
        });
        s.cloudflare.enabled = true;
        s.cloudflare.account_id = spec();
        s.cloudflare.connections.push(CloudflareConnection {
            account_id: spec(),
            ..Default::default()
        });
        s.crowdstrike.enabled = true;
        s.crowdstrike.client_id = spec();
        s.crowdstrike.connections.push(CrowdstrikeConnection {
            client_id: spec(),
            ..Default::default()
        });
        s.bitwarden.enabled = true;
        s.bitwarden.client_id = spec();
        s.bitwarden.connections.push(BitwardenConnection {
            client_id: spec(),
            ..Default::default()
        });
        s.duo.enabled = true;
        s.duo.api_host = spec();
        s.duo.integration_key = spec();
        s.duo.connections.push(DuoConnection {
            api_host: spec(),
            integration_key: spec(),
            ..Default::default()
        });
        s.google_workspace.enabled = true;
        s.google_workspace.admin_email = spec();
        s.google_workspace.customer_id = spec();
        s.google_workspace
            .connections
            .push(GoogleWorkspaceConnection {
                admin_email: spec(),
                customer_id: spec(),
                ..Default::default()
            });
        s.salesforce.enabled = true;
        s.salesforce.login_url = spec();
        s.salesforce.client_id = spec();
        s.salesforce.username = spec();
        s.salesforce.connections.push(SalesforceConnection {
            login_url: spec(),
            client_id: spec(),
            username: spec(),
            ..Default::default()
        });

        resolve_config_specs(&mut cfg).await.unwrap();
        unsafe { std::env::remove_var("DFE_FETCHER_TEST_IDENTITY") };

        let s = &cfg.sources;
        let fields = [
            ("aws.assume_role_arn", &s.aws.assume_role_arn),
            (
                "aws.connections.assume_role_arn",
                &s.aws.connections[0].assume_role_arn,
            ),
            ("azure.client_id", &s.azure.client_id),
            (
                "azure.connections.client_id",
                &s.azure.connections[0].client_id,
            ),
            ("m365.client_id", &s.m365.client_id),
            (
                "m365.connections.client_id",
                &s.m365.connections[0].client_id,
            ),
            ("github.org", &s.github.org),
            ("github.enterprise", &s.github.enterprise),
            ("github.connections.org", &s.github.connections[0].org),
            (
                "github.connections.enterprise",
                &s.github.connections[0].enterprise,
            ),
            ("okta.tenant_url", &s.okta.tenant_url),
            (
                "okta.connections.tenant_url",
                &s.okta.connections[0].tenant_url,
            ),
            ("cloudflare.account_id", &s.cloudflare.account_id),
            (
                "cloudflare.connections.account_id",
                &s.cloudflare.connections[0].account_id,
            ),
            ("crowdstrike.client_id", &s.crowdstrike.client_id),
            (
                "crowdstrike.connections.client_id",
                &s.crowdstrike.connections[0].client_id,
            ),
            ("bitwarden.client_id", &s.bitwarden.client_id),
            (
                "bitwarden.connections.client_id",
                &s.bitwarden.connections[0].client_id,
            ),
            ("duo.api_host", &s.duo.api_host),
            ("duo.integration_key", &s.duo.integration_key),
            ("duo.connections.api_host", &s.duo.connections[0].api_host),
            (
                "duo.connections.integration_key",
                &s.duo.connections[0].integration_key,
            ),
            (
                "google_workspace.admin_email",
                &s.google_workspace.admin_email,
            ),
            (
                "google_workspace.customer_id",
                &s.google_workspace.customer_id,
            ),
            (
                "google_workspace.connections.admin_email",
                &s.google_workspace.connections[0].admin_email,
            ),
            (
                "google_workspace.connections.customer_id",
                &s.google_workspace.connections[0].customer_id,
            ),
            ("salesforce.login_url", &s.salesforce.login_url),
            ("salesforce.client_id", &s.salesforce.client_id),
            ("salesforce.username", &s.salesforce.username),
            (
                "salesforce.connections.login_url",
                &s.salesforce.connections[0].login_url,
            ),
            (
                "salesforce.connections.client_id",
                &s.salesforce.connections[0].client_id,
            ),
            (
                "salesforce.connections.username",
                &s.salesforce.connections[0].username,
            ),
        ];
        for (field, value) in fields {
            assert_eq!(
                value.as_deref(),
                Some("resolved"),
                "{field} did not resolve"
            );
        }
    }

    #[tokio::test]
    async fn missing_env_returns_clear_error() {
        let mut cfg = base_config();
        cfg.sources.aws.enabled = true;
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
