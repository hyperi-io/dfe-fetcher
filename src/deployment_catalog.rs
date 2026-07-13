// Project:   dfe-fetcher
// File:      src/deployment_catalog.rs
// Purpose:   Reflectable capability catalog for the fetcher (scalo-rs#6)
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Capability catalog for dfe-fetcher.
//!
//! The derived JSON Schema (via schemars) describes the typed `Config` shape --
//! including the GA 2.2 multi-endpoint `connections` arrays. But it CANNOT
//! describe the runtime data: which SERVICE names each source type accepts, and
//! what knobs each service reads from its ad-hoc `config` map. This catalog
//! fills that gap so dfe-engine / dfe-ui can build the correct per-source-type
//! and per-service CRUD forms.
//!
//! One [`Capability`] per source TYPE (`kind = "source"`); its per-connection
//! form fields are the source's `fields`, and each SERVICE it supports is a
//! child `Capability` (`kind = "service"`) carrying its own knob `fields`. The
//! service names + knobs here are grounded in each source module's fetch code
//! (documented on the `<Type>Service.config` fields in `config/mod.rs`).
//!
//! Multi-endpoint types describe per-CONNECTION fields (many accounts/tenants of
//! one type). The five single-connection types (`pypi`, `crates_io`,
//! `go_modules`, `gcp_pubsub`, `object_store`) have no `connections` array;
//! their fields sit at the type level.

use scalo::deployment::{Capability, FieldSpec};

/// Per-connection id: the cursor key + metric/log/DLQ label (required, unique).
fn conn_id() -> FieldSpec {
    FieldSpec::string("id")
        .required()
        .description("Stable, unique connection id -- the cursor key and metric/log/DLQ label.")
}

/// The secret-ref field carried by every multi-endpoint connection. NOT itself
/// a secret value: a `provider:path:key` reference (ENV strategy C) that ESO
/// materialises and the app resolves at fetch time via `scalo::secrets::resolve`.
fn credential_secret() -> FieldSpec {
    FieldSpec::string("credential_secret").description(
        "Secret reference ('provider:path:key', e.g. vault:secret/aws/prod:credentials) \
         for this connection's credentials. ESO materialises it; the app resolves it at \
         fetch time. Prefer this over inline credential fields in production.",
    )
}

/// A `filter`/`limit`-style integer knob helper.
fn int_knob(name: &str, desc: &str) -> FieldSpec {
    FieldSpec::int(name).description(desc.to_string())
}

/// A service capability with a name + description at `stable` maturity.
fn service(name: &str, desc: &str) -> Capability {
    Capability::service(name)
        .description(desc.to_string())
        .maturity("stable")
}

/// The full fetcher capability catalog: one entry per source type.
#[must_use]
pub fn capabilities() -> Vec<Capability> {
    vec![
        aws(),
        azure(),
        m365(),
        gcp(),
        github(),
        okta(),
        cloudflare(),
        onepassword(),
        crowdstrike(),
        slack(),
        bitwarden(),
        duo(),
        google_workspace(),
        salesforce(),
        pypi(),
        crates_io(),
        go_modules(),
        gcp_pubsub(),
        object_store(),
    ]
}

fn aws() -> Capability {
    Capability::source("aws")
        .description("AWS audit + security sources via SigV4-signed API calls.")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::string("region")
                .default_value("us-east-1")
                .description("AWS region."),
            FieldSpec::string("access_key_id")
                .description("Access key ID (prefer credential_secret in production)."),
            FieldSpec::secret("secret_access_key").description("AWS secret access key."),
            FieldSpec::string("assume_role_arn")
                .description("Assume-role ARN for cross-account access."),
            credential_secret(),
            FieldSpec::string("endpoint_override").description("Endpoint URL override (testing)."),
        ])
        .children(vec![
            service(
                "cloudtrail",
                "CloudTrail management + data events via LookupEvents.",
            ),
            service("guardduty", "GuardDuty findings."),
            service("securityhub", "Security Hub findings."),
            service("config", "AWS Config configuration + compliance items."),
            service(
                "cloudwatch_logs",
                "CloudWatch Logs events for a named log group.",
            )
            .field(
                FieldSpec::string("log_group_name")
                    .required()
                    .description("CloudWatch log group to pull events from."),
            ),
            service(
                "cloudwatch_metrics",
                "CloudWatch metric statistics for namespaces.",
            )
            .field(
                FieldSpec::list("namespaces")
                    .description("CloudWatch namespaces to pull metrics for."),
            ),
        ])
}

fn azure() -> Capability {
    Capability::source("azure")
        .description("Azure tenant audit + security sources (Management + Graph APIs).")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::string("tenant_id").description("Azure AD tenant ID."),
            FieldSpec::string("client_id").description("Client (application) ID."),
            FieldSpec::secret("client_secret").description("Client secret."),
            FieldSpec::string("subscription_id").description("Subscription ID."),
            credential_secret(),
        ])
        .children(vec![
            service(
                "activity_log",
                "Azure Monitor activity log (control-plane events).",
            ),
            service("defender", "Microsoft Defender for Cloud alerts."),
            service("sentinel", "Microsoft Sentinel incidents."),
            service("entra_id", "Entra ID (Azure AD) sign-in + audit logs."),
        ])
}

fn m365() -> Capability {
    Capability::source("m365")
        .description("Microsoft 365 tenant audit sources (Management Activity + Graph).")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::string("tenant_id").description("Azure AD tenant ID."),
            FieldSpec::string("client_id").description("Client (application) ID."),
            FieldSpec::secret("client_secret").description("Client secret."),
            credential_secret(),
        ])
        .children(vec![
            service(
                "audit_log",
                "Unified audit log via the Management Activity API.",
            ),
            service("message_trace", "Exchange Online message trace."),
            service("dlp", "Data-loss-prevention events."),
            service("alerts", "Security + compliance alerts."),
        ])
}

fn gcp() -> Capability {
    Capability::source("gcp")
        .description("Google Cloud audit + security sources (service-account auth).")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::string("project_id").description("GCP project ID."),
            FieldSpec::string("service_account_key")
                .description("Path to the service-account JSON key file."),
            credential_secret(),
        ])
        .children(vec![
            service(
                "audit_logs",
                "Cloud Audit Logs (admin activity + data access).",
            ),
            service("scc", "Security Command Center findings."),
            service(
                "cloud_logging",
                "Cloud Logging entries via a logging filter.",
            ),
        ])
}

fn github() -> Capability {
    Capability::source("github")
        .description("GitHub org / Enterprise Cloud audit-log events.")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::string("org")
                .description("Organisation slug (set exactly one of org/enterprise)."),
            FieldSpec::string("enterprise")
                .description("Enterprise slug (GitHub Enterprise Cloud)."),
            FieldSpec::secret("token")
                .description("PAT / fine-grained PAT / App token with read:audit_log."),
            credential_secret(),
        ])
        .child(
            service("audit_log", "Org or enterprise audit-log stream.").field(
                FieldSpec::enumeration("include", ["all", "web", "git"])
                    .default_value("all")
                    .description("Which event families to include."),
            ),
        )
}

fn okta() -> Capability {
    Capability::source("okta")
        .description("Okta System Log events for one tenant.")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::string("tenant_url")
                .description("Tenant URL, e.g. https://hyperi.okta.com."),
            FieldSpec::secret("token").description("SSWS API token or OAuth bearer token."),
            FieldSpec::bool("use_ssws_header")
                .default_value(true)
                .description("Send `Authorization: SSWS <token>` (true) vs Bearer (false)."),
            credential_secret(),
        ])
        .child(
            service("system_log", "Okta System Log API (/api/v1/logs).")
                .field(FieldSpec::string("filter").description("OData-style server-side filter."))
                .field(int_knob("limit", "Per-page size (Okta caps at 1000).")),
        )
}

fn cloudflare() -> Capability {
    Capability::source("cloudflare")
        .description("Cloudflare account audit-log events.")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::string("account_id")
                .description("Account ID (32-char hex) for account-level audit logs."),
            FieldSpec::secret("token").description("Scoped API token (Account Settings: Read)."),
            credential_secret(),
        ])
        .child(
            service(
                "audit_logs",
                "Account audit logs (/accounts/{id}/audit_logs).",
            )
            .field(FieldSpec::string("actor_email").description("Filter by acting user."))
            .field(FieldSpec::string("action_type").description("Filter by action category."))
            .field(int_knob("per_page", "Page size (default 100, max 1000).")),
        )
}

fn onepassword() -> Capability {
    Capability::source("onepassword")
        .description("1Password Events Reporting API (Business/Enterprise).")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::secret("token").description("Events Reporting Bearer token."),
            credential_secret(),
        ])
        .children(vec![
            service("signin_attempts", "Sign-in attempt events.")
                .field(int_knob("limit", "Page size (default 100, max 1000).")),
            service("item_usages", "Item-usage events.")
                .field(int_knob("limit", "Page size (default 100, max 1000).")),
            service("audit_events", "Account audit events.")
                .field(int_knob("limit", "Page size (default 100, max 1000).")),
        ])
}

fn crowdstrike() -> Capability {
    Capability::source("crowdstrike")
        .description("CrowdStrike Falcon detections via OAuth2 client-credentials.")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::string("api_url_override")
                .description("Region API base (US-1/US-2/EU-1/US-GOV-1)."),
            FieldSpec::string("client_id").description("Falcon API OAuth2 client ID."),
            FieldSpec::secret("client_secret").description("Falcon API OAuth2 client secret."),
            credential_secret(),
        ])
        .child(
            service("detections", "Enriched EPP detection summaries.")
                .field(int_knob(
                    "limit",
                    "Query page size (default 100, max 9999).",
                ))
                .field(
                    FieldSpec::string("filter")
                        .description("Falcon Query Language clause (ANDed with the time window)."),
                ),
        )
}

fn slack() -> Capability {
    Capability::source("slack")
        .description("Slack Enterprise Grid audit-log events.")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::secret("token").description("Org-admin token with auditlogs:read."),
            credential_secret(),
        ])
        .child(
            service("audit_logs", "Enterprise Grid audit logs (audit/v1/logs).")
                .field(
                    FieldSpec::string("action")
                        .description("Filter by action name (e.g. user_login)."),
                )
                .field(
                    FieldSpec::string("entity")
                        .description("Filter by entity type (user/workspace/...)."),
                )
                .field(int_knob("limit", "Page size (default 200, max 1000).")),
        )
}

fn bitwarden() -> Capability {
    Capability::source("bitwarden")
        .description("Bitwarden organisation Events API (OAuth2 client-credentials).")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::string("client_id")
                .description("Organisation API client ID (organization.<uuid>)."),
            FieldSpec::secret("client_secret").description("Organisation API client secret."),
            credential_secret(),
        ])
        .child(service(
            "events",
            "Organisation event stream (/public/events).",
        ))
}

fn duo() -> Capability {
    Capability::source("duo")
        .description("Duo Admin API authentication events (HMAC-SHA1 signed).")
        .maturity("stable")
        .fields(vec![
            conn_id(),
            FieldSpec::string("api_host").description("api-XXXXXXXX.duosecurity.com."),
            FieldSpec::string("integration_key").description("Admin API integration key (ikey)."),
            FieldSpec::secret("secret_key").description("Admin API secret key (skey)."),
            credential_secret(),
        ])
        .child(
            service("authentication_logs", "Admin API v2 authentication logs.")
                .field(int_knob("limit", "Page size (default 100, max 1000).")),
        )
}

fn google_workspace() -> Capability {
    Capability::source("google_workspace")
        .description("Google Workspace Reports API per-application activity (domain-wide-delegation SA).")
        .maturity("alpha")
        .fields(vec![
            conn_id(),
            FieldSpec::string("service_account_key").description("Path to the SA JSON key (domain-wide delegation)."),
            FieldSpec::string("admin_email").description("Workspace admin the SA impersonates (JWT sub)."),
            FieldSpec::string("customer_id").default_value("my_customer").description("Customer ID."),
            credential_secret(),
        ])
        .child(
            service("applications", "Any Reports API applicationName (login/admin/drive/mobile/groups/calendar/chat/meet/token/...); set `name` to the application.")
                .field(FieldSpec::string("event_name").description("Filter to a single event name.")),
        )
}

fn salesforce() -> Capability {
    Capability::source("salesforce")
        .description(
            "Salesforce audit surfaces via REST (JWT-bearer or client-credentials OAuth2).",
        )
        .maturity("alpha")
        .fields(vec![
            conn_id(),
            FieldSpec::string("login_url")
                .default_value("https://login.salesforce.com")
                .description("OAuth2 login base URL."),
            FieldSpec::string("client_id").description("Connected-app consumer key."),
            FieldSpec::string("username")
                .description("Integration username (JWT sub; JWT-bearer flow)."),
            FieldSpec::string("private_key").description("RSA private key PEM (JWT-bearer flow)."),
            FieldSpec::string("private_key_secret")
                .description("Secret ref for the RSA private key PEM."),
            FieldSpec::secret("client_secret")
                .description("Connected-app consumer secret (client-credentials flow)."),
            credential_secret(),
        ])
        .children(vec![
            service("setup_audit_trail", "Admin config changes (SOQL)."),
            service("login_history", "Login events (SOQL)."),
            service("event_log_file", "Runtime events as downloadable CSV logs.")
                .field(
                    FieldSpec::list("event_types")
                        .description("EventType values to include (default all)."),
                )
                .field(
                    FieldSpec::enumeration("interval", ["Hourly", "Daily"])
                        .default_value("Daily")
                        .description("Log-file cadence."),
                ),
        ])
}

// --- Single-connection types (no `connections` array) ---

fn pypi() -> Capability {
    Capability::source("pypi")
        .description("PyPI package metadata (supply-chain monitoring; no auth). Single-connection.")
        .maturity("stable")
        .field(
            FieldSpec::list("packages")
                .required()
                .description("Package names to monitor (one record each per tick)."),
        )
        .field(
            FieldSpec::string("topic")
                .default_value("pypi")
                .description("Output Kafka topic."),
        )
}

fn crates_io() -> Capability {
    Capability::source("crates_io")
        .description(
            "crates.io crate metadata (supply-chain monitoring; no auth). Single-connection.",
        )
        .maturity("stable")
        .field(
            FieldSpec::list("crates")
                .required()
                .description("Crate names to monitor."),
        )
        .field(
            FieldSpec::string("topic")
                .default_value("crates_io")
                .description("Output Kafka topic."),
        )
}

fn go_modules() -> Capability {
    Capability::source("go_modules")
        .description(
            "Go module-proxy metadata (supply-chain monitoring; no auth). Single-connection.",
        )
        .maturity("stable")
        .field(
            FieldSpec::list("modules")
                .required()
                .description("Module paths to monitor."),
        )
        .field(
            FieldSpec::string("topic")
                .default_value("go_modules")
                .description("Output Kafka topic."),
        )
}

fn gcp_pubsub() -> Capability {
    Capability::source("gcp_pubsub")
        .description("GCP Pub/Sub pull source (Log Sink delivery). Single-connection.")
        .maturity("alpha")
        .fields(vec![
            FieldSpec::string("service_account_key")
                .description("Path to the SA JSON key (roles/pubsub.subscriber)."),
            credential_secret(),
        ])
        .child(
            Capability::service("subscription")
                .description("One entry per `subscriptions[]`: a subscription to pull from.")
                .maturity("alpha")
                .field(
                    FieldSpec::string("project_id")
                        .required()
                        .description("GCP project owning the subscription."),
                )
                .field(
                    FieldSpec::string("subscription_id")
                        .required()
                        .description("Subscription short name."),
                )
                .field(
                    int_knob(
                        "max_messages",
                        "Max messages per tick (default 1000, REST cap).",
                    )
                    .default_value(1000),
                )
                .field(
                    FieldSpec::bool("return_immediately")
                        .default_value(true)
                        .description("Single-shot pull vs server-side wait."),
                ),
        )
}

fn object_store() -> Capability {
    Capability::source("object_store")
        .description("Object-store tailing (S3 live; GCS / Azure Blob are Phase-2 stubs). Single-connection.")
        .maturity("alpha")
        .child(
            Capability::service("s3")
                .description("Amazon S3 (or S3-compatible: MinIO / R2 / B2 via endpoint_override).")
                .maturity("beta")
                .field(FieldSpec::string("region").required().description("AWS region for SigV4 + endpoint construction."))
                .field(FieldSpec::string("endpoint_override").description("S3 endpoint override (S3-compatible / VPC endpoints)."))
                .field(FieldSpec::string("access_key_id").description("Access key ID (or a vault:/env: spec)."))
                .field(FieldSpec::secret("secret_access_key").description("AWS secret access key."))
                .field(credential_secret()),
        )
        .children(vec![
            Capability::service("gcs").description("Google Cloud Storage (Phase-2 stub).").maturity("alpha"),
            Capability::service("azure_blob").description("Azure Blob Storage (Phase-2 stub).").maturity("alpha"),
        ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_covers_all_source_types() {
        let caps = capabilities();
        assert_eq!(caps.len(), 19, "expected 19 source types");
        for c in &caps {
            assert_eq!(c.kind, "source", "top-level entries are sources");
            assert!(!c.name.is_empty());
            assert!(!c.description.is_empty(), "{} needs a description", c.name);
        }
    }

    #[test]
    fn aws_has_cloudwatch_logs_knob() {
        let caps = capabilities();
        let aws = caps.iter().find(|c| c.name == "aws").unwrap();
        let cwl = aws
            .children
            .iter()
            .find(|c| c.name == "cloudwatch_logs")
            .expect("aws must describe cloudwatch_logs");
        assert!(
            cwl.fields
                .iter()
                .any(|f| f.name == "log_group_name" && f.required),
            "cloudwatch_logs must require log_group_name"
        );
    }

    #[test]
    fn secret_fields_are_flagged() {
        let caps = capabilities();
        let aws = caps.iter().find(|c| c.name == "aws").unwrap();
        let sak = aws
            .fields
            .iter()
            .find(|f| f.name == "secret_access_key")
            .unwrap();
        assert!(sak.secret, "secret_access_key must be flagged secret");
    }

    #[test]
    fn catalog_serialises_to_json() {
        let caps = capabilities();
        let json = serde_json::to_string(&caps).unwrap();
        assert!(json.contains("cloudtrail"));
        assert!(json.contains("\"kind\":\"service\""));
    }
}
