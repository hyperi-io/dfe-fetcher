// Project:   dfe-fetcher
// File:      crates/fetcher/src/deployment_catalog.rs
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
//! One [`Capability`] per typed block of the [registry](crate::config::REGISTRY)
//! (`kind = "source"`), which names the entry function for each; its
//! per-connection form fields are the source's `fields`, and each SERVICE it
//! supports is a child `Capability` (`kind = "service"`) carrying its own knob
//! `fields`. The service names + knobs here are grounded in the units of the
//! block's shipped profile (documented on the `<Type>Service.config` fields
//! in `config/mod.rs`); the maturity is the profile's, stamped once in
//! [`capabilities`].
//!
//! Multi-endpoint types describe per-CONNECTION fields (many accounts/tenants of
//! one type). The five single-connection types (`pypi`, `crates_io`,
//! `go_modules`, `gcp_pubsub`, `object_store`) have no `connections` array;
//! their fields sit at the type level.

use scalo::deployment::{Capability, FieldSpec};

use crate::config::REGISTRY;

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
        "Secret reference ('vault:<mount>/data/<path>:<key>', e.g. \
         vault:kv/data/aws/prod:credentials -- the literal data segment names the KV v2 \
         mount) for this connection's credentials. ESO materialises it; the app resolves \
         it at fetch time. Prefer this over inline credential fields in production.",
    )
}

/// A `filter`/`limit`-style integer knob helper.
fn int_knob(name: &str, desc: &str) -> FieldSpec {
    FieldSpec::int(name).description(desc.to_string())
}

/// A service capability with a name + description. Maturity is declared per
/// source by its shipped profile, not per service, so none is stamped here.
fn service(name: &str, desc: &str) -> Capability {
    Capability::service(name).description(desc.to_string())
}

/// The maturity a shipped profile declares, which the driver answers for
/// the block that maps onto it.
fn profile_maturity(name: &str) -> String {
    crate::profiles::shipped()
        .get(name)
        .unwrap_or_else(|| panic!("`{name}` is a shipped profile"))
        .maturity
        .to_string()
}

/// The auth modes a shipped profile accepts, in its declared order.
///
/// Read from the profile rather than listed here, so a profile that gains or
/// loses a mode cannot leave the catalogue claiming the old set.
fn accepted_auth_modes(name: &str) -> String {
    crate::profiles::shipped()
        .get(name)
        .unwrap_or_else(|| panic!("`{name}` is a shipped profile"))
        .auth
        .accepts
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The full fetcher capability catalog: one entry per registry block, each
/// stamped with its shipped profile's maturity and the auth modes that profile
/// accepts.
///
/// The auth modes are appended to the description rather than declared as a
/// field, because a catalogue field names a config knob and an operator selects
/// a mode by supplying its credential, not by naming it.
#[must_use]
pub fn capabilities() -> Vec<Capability> {
    REGISTRY
        .iter()
        .map(|block| {
            let mut capability = block.capability().maturity(profile_maturity(block.name));
            capability.description = format!(
                "{} Accepted auth modes: {}.",
                capability.description,
                accepted_auth_modes(block.name)
            );
            capability
        })
        .collect()
}

pub(crate) fn aws() -> Capability {
    Capability::source("aws")
        .description("AWS audit + security sources via SigV4-signed API calls.")
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
            service(
                "config",
                "AWS Config resource configurations via an advanced query.",
            )
            .field(
                FieldSpec::string("expression").description(
                    "The SELECT expression; every resource's configuration unless set.",
                ),
            ),
            service(
                "cloudwatch_logs",
                "CloudWatch Logs events for a named log group.",
            )
            .field(
                FieldSpec::string("log_group_name")
                    .required()
                    .description("CloudWatch log group to pull events from."),
            )
            .field(
                FieldSpec::string("filter_pattern")
                    .description("CloudWatch Logs filter pattern; every event unless set."),
            ),
            service(
                "cloudwatch_metrics",
                "CloudWatch metric statistics for namespaces.",
            )
            .field(
                FieldSpec::list("namespaces")
                    .required()
                    .description("CloudWatch namespaces to pull metrics for."),
            )
            .field(
                FieldSpec::list("metric_names").description(
                    "Metric names to keep; every metric of the namespaces unless set.",
                ),
            )
            .field(int_knob(
                "period_secs",
                "Seconds between datapoints (default 300).",
            ))
            .field(FieldSpec::string("stat").description("The statistic (default Average)."))
            .field(FieldSpec::string("output_format").description(
                "json (one row per datapoint, default) or otlp (one protobuf per response).",
            )),
            service(
                "inspector",
                "Inspector v2 findings (must be enabled tenant-side; empty otherwise).",
            )
            .field(int_knob(
                "max_results",
                "Per-page size (default 100, max 100).",
            )),
            service(
                "health",
                "AWS Health events (Business / Enterprise support plans only).",
            )
            .field(int_knob(
                "max_results",
                "Per-page size (default 100, max 100).",
            )),
        ])
}

pub(crate) fn azure() -> Capability {
    Capability::source("azure")
        .description("Azure tenant audit + security sources (Management + Graph APIs).")
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
            service("sentinel", "Microsoft Sentinel incidents.")
                .field(
                    FieldSpec::string("resource_group")
                        .description("Resource group of the Sentinel workspace."),
                )
                .field(
                    FieldSpec::string("workspace_name")
                        .description("Log Analytics workspace Sentinel is enabled on."),
                ),
            service(
                "entra_signins",
                "Entra ID sign-in logs (auditLogs/signIns).",
            ),
            service(
                "entra_directory_audits",
                "Entra ID directory audit logs (auditLogs/directoryAudits).",
            ),
            service(
                "entra_provisioning",
                "Entra ID provisioning logs (auditLogs/provisioning).",
            ),
            service(
                "log_analytics",
                "Arbitrary KQL query against a Log Analytics workspace.",
            )
            .field(
                FieldSpec::string("workspace_id")
                    .required()
                    .description("Log Analytics workspace ID (GUID)."),
            )
            .field(
                FieldSpec::string("kql")
                    .required()
                    .description("KQL query; the fetch window is applied as the timespan."),
            ),
        ])
}

pub(crate) fn m365() -> Capability {
    Capability::source("m365")
        .description("Microsoft 365 tenant audit sources (Management Activity + Graph).")
        .fields(vec![
            conn_id(),
            FieldSpec::string("tenant_id").description("Azure AD tenant ID."),
            FieldSpec::string("client_id").description("Client (application) ID."),
            FieldSpec::secret("client_secret").description("Client secret."),
            credential_secret(),
            FieldSpec::string("publisher_identifier").description(
                "PublisherIdentifier on every Management Activity call, keying this \
                 deployment's OMAP rate budget; the shared default unless set.",
            ),
        ])
        .children(vec![
            service(
                "audit_log",
                "Unified audit log via the Management Activity API, one feed per content type.",
            )
            .field(FieldSpec::list("content_types").description(
                "Content types to fetch (Audit.AzureActiveDirectory, Audit.Exchange, \
                 Audit.SharePoint, Audit.General, DLP.All); every feed unless set.",
            )),
            service(
                "exchange_audit",
                "Exchange per-message audit records (Management Activity Audit.Exchange).",
            ),
            service("dlp", "Data-loss-prevention events."),
            service("alerts", "Security + compliance alerts."),
        ])
}

pub(crate) fn gcp() -> Capability {
    Capability::source("gcp")
        .description("Google Cloud audit + security sources (service-account auth).")
        .fields(vec![
            conn_id(),
            FieldSpec::string("project_id").description("GCP project ID."),
            FieldSpec::string("service_account_key")
                .description("Path to the service-account JSON key file."),
            credential_secret(),
        ])
        .children(vec![
            service("admin_activity", "Cloud Audit Logs: admin activity."),
            service(
                "data_access",
                "Cloud Audit Logs: data access (tenant must enable Data Access audit logs).",
            ),
            service("system_event", "Cloud Audit Logs: system events."),
            service("policy_denied", "Cloud Audit Logs: policy-denied events."),
            service(
                "vpc_flow_logs",
                "VPC Flow Logs (tenant enables per subnet).",
            ),
            service(
                "dns_queries",
                "Cloud DNS query logs (tenant enables via a DNS server policy).",
            ),
            service(
                "storage_access",
                "Cloud Storage data-access events (the gcs_bucket slice of data_access).",
            ),
            service("scc", "Security Command Center findings.").field(
                FieldSpec::string("organization_id")
                    .required()
                    .description("GCP organisation ID the findings are listed under."),
            ),
            service(
                "cloud_logging",
                "Cloud Logging entries via a logging filter.",
            )
            .field(
                FieldSpec::string("filter")
                    .default_value("severity >= WARNING")
                    .description("Logging query-language filter; the fetch window is ANDed on."),
            ),
        ])
}

pub(crate) fn github() -> Capability {
    Capability::source("github")
        .description("GitHub org / Enterprise Cloud audit-log events.")
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

pub(crate) fn okta() -> Capability {
    Capability::source("okta")
        .description("Okta System Log events for one tenant.")
        .fields(vec![
            conn_id(),
            FieldSpec::string("tenant_url")
                .description("Tenant URL, e.g. https://your-tenant.okta.com."),
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

pub(crate) fn cloudflare() -> Capability {
    Capability::source("cloudflare")
        .description("Cloudflare account audit-log events.")
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

pub(crate) fn onepassword() -> Capability {
    Capability::source("onepassword")
        .description("1Password Events Reporting API (Business/Enterprise).")
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

pub(crate) fn crowdstrike() -> Capability {
    Capability::source("crowdstrike")
        .description("CrowdStrike Falcon alerts via OAuth2 client-credentials.")
        .fields(vec![
            conn_id(),
            FieldSpec::string("api_url_override")
                .description("Region API base (US-1/US-2/EU-1/US-GOV-1)."),
            FieldSpec::string("client_id").description("Falcon API OAuth2 client ID."),
            FieldSpec::secret("client_secret").description("Falcon API OAuth2 client secret."),
            credential_secret(),
        ])
        .child(
            service("alerts", "Enriched EPP alert entities (Alerts API v2).")
                .field(int_knob(
                    "limit",
                    "Query page size (default 100, max 1000).",
                ))
                .field(
                    FieldSpec::string("filter")
                        .description("Falcon Query Language clause (ANDed with the time window)."),
                ),
        )
}

pub(crate) fn slack() -> Capability {
    Capability::source("slack")
        .description("Slack Enterprise Grid audit-log events.")
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

pub(crate) fn bitwarden() -> Capability {
    Capability::source("bitwarden")
        .description("Bitwarden organisation Events API (OAuth2 client-credentials).")
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

pub(crate) fn duo() -> Capability {
    Capability::source("duo")
        .description("Duo Admin API authentication events (request-signed).")
        .fields(vec![
            conn_id(),
            FieldSpec::string("api_host").description("api-XXXXXXXX.duosecurity.com."),
            FieldSpec::string("integration_key").description("Admin API integration key (ikey)."),
            FieldSpec::secret("secret_key").description("Admin API secret key (skey)."),
            FieldSpec::enumeration("signature_version", ["v5", "v2"])
                .default_value("v5")
                .description("Signing version the tenant verifies; v2 is the legacy scheme."),
            credential_secret(),
        ])
        .child(
            service("authentication_logs", "Admin API v2 authentication logs.")
                .field(int_knob("limit", "Page size (default 100, max 1000).")),
        )
}

pub(crate) fn google_workspace() -> Capability {
    Capability::source("google_workspace")
        .description("Google Workspace Reports API per-application activity (domain-wide-delegation SA).")
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

pub(crate) fn salesforce() -> Capability {
    Capability::source("salesforce")
        .description(
            "Salesforce audit surfaces via REST (JWT-bearer or client-credentials OAuth2).",
        )
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

pub(crate) fn pypi() -> Capability {
    Capability::source("pypi")
        .description("PyPI package metadata (supply-chain monitoring; no auth). Single-connection.")
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

pub(crate) fn crates_io() -> Capability {
    Capability::source("crates_io")
        .description(
            "crates.io crate metadata (supply-chain monitoring; no auth). Single-connection.",
        )
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

pub(crate) fn go_modules() -> Capability {
    Capability::source("go_modules")
        .description(
            "Go module-proxy metadata (supply-chain monitoring; no auth). Single-connection.",
        )
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

pub(crate) fn gcp_pubsub() -> Capability {
    Capability::source("gcp_pubsub")
        .description("GCP Pub/Sub pull source (Log Sink delivery). Single-connection.")
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

pub(crate) fn object_store() -> Capability {
    Capability::source("object_store")
        .description("Object-store tailing (S3 live; GCS / Azure Blob are Phase-2 stubs). Single-connection.")
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
    use crate::config::Config;

    /// The shipped example config -- the operator's copy-paste reference, and
    /// the config the catalog is checked against.
    fn example_config() -> Config {
        let path = crate::deployment::repo_root().join("config.example.yaml");
        let yaml = std::fs::read_to_string(path).expect("read config.example.yaml");
        serde_yaml_ng::from_str(&yaml).expect("example config parses")
    }

    /// What the catalog is checked against for one source type: the name,
    /// the maturity its shipped profile declares, and the services the
    /// example config lists for it.
    struct Witness {
        name: &'static str,
        maturity: String,
        services: Vec<String>,
    }

    impl Witness {
        /// A typed block served by a shipped profile: the profile declares
        /// the maturity, the block lists the services.
        fn profile(name: &'static str, services: impl Iterator<Item = String>) -> Self {
            Self {
                name,
                maturity: profile_maturity(name),
                services: services.collect(),
            }
        }
    }

    /// A typed block a shipped profile serves: the services the example
    /// config lists for it, and the profile units those services map to
    /// (the same names, except where one service is several units).
    struct ProfileBacked {
        name: &'static str,
        services: Vec<String>,
        units: Vec<String>,
    }

    impl ProfileBacked {
        /// A block whose services are its units, one to one.
        fn same(name: &'static str, services: Vec<&String>) -> Self {
            let services: Vec<String> = services.into_iter().cloned().collect();
            Self {
                name,
                units: services.clone(),
                services,
            }
        }
    }

    /// The typed blocks a shipped profile serves, from the example config.
    fn profile_backed(config: &Config) -> Vec<ProfileBacked> {
        let s = &config.sources;
        vec![
            ProfileBacked::same("aws", s.aws.services.iter().map(|x| &x.name).collect()),
            ProfileBacked::same("azure", s.azure.services.iter().map(|x| &x.name).collect()),
            // The `audit_log` service is one unit per content type, so the
            // block's own mapping names the units the profile is held to.
            ProfileBacked {
                name: "m365",
                services: s.m365.services.iter().map(|x| x.name.clone()).collect(),
                units: s
                    .m365
                    .service_units()
                    .expect("the example config names known content types"),
            },
            ProfileBacked::same("gcp", s.gcp.services.iter().map(|x| &x.name).collect()),
            ProfileBacked::same(
                "google_workspace",
                s.google_workspace
                    .services
                    .iter()
                    .map(|x| &x.name)
                    .collect(),
            ),
            ProfileBacked::same(
                "github",
                s.github.services.iter().map(|x| &x.name).collect(),
            ),
            ProfileBacked::same("okta", s.okta.services.iter().map(|x| &x.name).collect()),
            ProfileBacked::same("slack", s.slack.services.iter().map(|x| &x.name).collect()),
            ProfileBacked::same(
                "cloudflare",
                s.cloudflare.services.iter().map(|x| &x.name).collect(),
            ),
            ProfileBacked::same(
                "bitwarden",
                s.bitwarden.services.iter().map(|x| &x.name).collect(),
            ),
            ProfileBacked::same(
                "onepassword",
                s.onepassword.services.iter().map(|x| &x.name).collect(),
            ),
            ProfileBacked::same(
                "crowdstrike",
                s.crowdstrike.services.iter().map(|x| &x.name).collect(),
            ),
            ProfileBacked::same("duo", s.duo.services.iter().map(|x| &x.name).collect()),
            ProfileBacked::same(
                "salesforce",
                s.salesforce.services.iter().map(|x| &x.name).collect(),
            ),
            // The registry blocks list keys, not services; the catalog
            // describes their fields and the profile's one unit is implied.
            ProfileBacked::same("pypi", Vec::new()),
            ProfileBacked::same("crates_io", Vec::new()),
            ProfileBacked::same("go_modules", Vec::new()),
            // The prefixes are units named by the operator's `source_tag`,
            // instantiated from the format endpoints; nothing fixed to check.
            ProfileBacked::same("object_store", Vec::new()),
            // The subscriptions are units named by their ids, instantiated
            // from `pull`; nothing fixed to check.
            ProfileBacked::same("gcp_pubsub", Vec::new()),
        ]
    }

    /// One witness per source type, built from `config`.
    fn every_source(config: &Config) -> Vec<Witness> {
        profile_backed(config)
            .into_iter()
            .map(|backed| Witness::profile(backed.name, backed.services.into_iter()))
            .collect()
    }

    fn catalog_entry<'a>(caps: &'a [Capability], name: &str) -> &'a Capability {
        caps.iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("catalog has no entry for source `{name}`"))
    }

    #[test]
    fn catalog_covers_all_source_types() {
        let caps = capabilities();
        let sources = every_source(&example_config());
        assert_eq!(
            caps.len(),
            sources.len(),
            "one catalog entry per source type"
        );
        assert_eq!(
            caps.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            REGISTRY.iter().map(|b| b.name).collect::<Vec<_>>(),
            "the catalog is the registry, in registry order"
        );
        for source in &sources {
            catalog_entry(&caps, source.name);
        }
        for c in &caps {
            assert_eq!(c.kind, "source", "top-level entries are sources");
            assert!(!c.description.is_empty(), "{} needs a description", c.name);
        }
        // The auth-mode sentence is appended after a single space, so a
        // hand-written description missing its full stop would run on. Checked
        // before the append, which supplies a stop of its own.
        for block in &REGISTRY {
            let base = block.capability().description;
            assert!(
                base.ends_with('.'),
                "{} description must end with a full stop: {base}",
                block.name
            );
        }
    }

    /// The shipped profile's `maturity`, which the driver answers, is the
    /// single source of truth; the catalog repeats it for dfe-ui and must say
    /// the same thing.
    #[test]
    fn catalog_maturity_matches_source_maturity() {
        let caps = capabilities();
        for source in every_source(&example_config()) {
            assert_eq!(
                catalog_entry(&caps, source.name).maturity.as_deref(),
                Some(source.maturity.as_str()),
                "catalog maturity for `{}` must match the code",
                source.name
            );
        }
        // Both sides above read the same helper, so the loop cannot catch a
        // wrong maturity -- only a missing catalog entry. The stable set is
        // pinned because promotion is what the startup warning stops covering.
        let mut stable: Vec<&str> = every_source(&example_config())
            .into_iter()
            .filter(|s| s.maturity.as_str() == "stable")
            .map(|s| s.name)
            .collect();
        stable.sort_unstable();
        assert_eq!(
            stable,
            ["aws", "azure", "gcp", "m365"],
            "promoting a source past alpha silences its startup warning -- say so here first"
        );
    }

    /// The shipped profile's `accepts` list is the single source of truth for
    /// which auth modes a source takes; the catalog repeats it so coverage is
    /// readable without opening the Rust, and must say the same thing.
    #[test]
    fn catalog_names_the_auth_modes_the_profile_accepts() {
        let caps = capabilities();
        for source in every_source(&example_config()) {
            let modes = accepted_auth_modes(source.name);
            assert!(
                !modes.is_empty(),
                "`{}` names no auth mode, but a profile must accept at least one",
                source.name
            );
            let description = &catalog_entry(&caps, source.name).description;
            assert!(
                description.ends_with(&format!("Accepted auth modes: {modes}.")),
                "catalog description for `{}` must name its profile's modes, got: {description}",
                source.name
            );
        }
        // Both sides above derive from the same helper, so one case is spelled
        // out to pin the config spelling and the full list.
        assert_eq!(
            accepted_auth_modes("gcp"),
            "jwt_bearer, gce_metadata, bearer",
            "the catalog names every accepted mode, in the profile's order, in its config spelling"
        );
    }

    /// A field whose config type is a closed set is typed as one in the
    /// catalog, so a form built from the contract offers the values the
    /// deserialiser accepts instead of a free string the operator has to guess.
    /// Duo's signing version is the one such field today; the check is against
    /// the config enum's own serialisation, so the two cannot drift.
    #[test]
    fn catalog_types_a_closed_config_set_as_an_enumeration() {
        use crate::config::DuoSignatureVersion;

        let caps = capabilities();
        let field = catalog_entry(&caps, "duo")
            .fields
            .iter()
            .find(|f| f.name == "signature_version")
            .expect("duo describes its signing version");
        let spelling = |version: DuoSignatureVersion| {
            serde_json::to_value(version)
                .expect("the version serialises")
                .as_str()
                .expect("as a string")
                .to_owned()
        };
        assert_eq!(
            field.enum_values,
            [DuoSignatureVersion::V5, DuoSignatureVersion::V2]
                .map(spelling)
                .to_vec()
        );
        assert_eq!(
            field.default.as_ref().and_then(serde_json::Value::as_str),
            Some(spelling(DuoSignatureVersion::default()).as_str())
        );
    }

    /// Every service the example config lists for a source is a service the
    /// catalog describes. The names a source accepts are its profile's
    /// units, so the shipped example -- loaded through the same config
    /// types -- is the witness. Sources whose units are subscription ids,
    /// bucket tags or Workspace application names have no fixed list to
    /// check.
    #[test]
    fn catalog_services_cover_the_example_config() {
        let caps = capabilities();
        let open_ended = ["google_workspace", "gcp_pubsub", "object_store"];
        let mut checked = 0;
        for source in every_source(&example_config()) {
            if open_ended.contains(&source.name) {
                continue;
            }
            let cap = catalog_entry(&caps, source.name);
            for service in &source.services {
                checked += 1;
                assert!(
                    cap.children
                        .iter()
                        .any(|c| c.kind == "service" && c.name == *service),
                    "config.example.yaml lists service `{service}` for `{}`, which the \
                     catalog does not describe",
                    source.name
                );
            }
        }
        assert!(checked > 0, "the example config lists no services at all");
    }

    /// The services the example config lists for a profile-backed block map
    /// onto units its shipped profile declares, so the catalog, the example
    /// and the profile agree on the names.
    #[test]
    fn profile_backed_services_are_units_of_their_profile() {
        let shipped = crate::profiles::shipped();
        let mut checked = 0;
        for backed in profile_backed(&example_config()) {
            let name = backed.name;
            let profile = &shipped[name];
            for unit in backed.units {
                checked += 1;
                assert!(
                    profile.endpoints.iter().any(|e| e.unit == unit),
                    "{name} unit `{unit}` is not a unit of the {name} profile"
                );
            }
        }
        assert!(checked > 0, "the example lists no profile-backed service");
    }

    /// The other direction: every service the catalog describes is a unit its
    /// block's shipped profile declares, so a child no profile serves cannot
    /// advertise a source that never runs. A dotted unit is named by the head
    /// segment, which is the service an operator lists. Blocks whose units
    /// are subscription ids, bucket tags or Workspace application names have
    /// no fixed list to check.
    #[test]
    fn every_catalog_service_is_a_unit_of_its_block_profile() {
        let shipped = crate::profiles::shipped();
        let open_ended = ["google_workspace", "gcp_pubsub", "object_store"];
        let mut checked = 0;
        for cap in capabilities() {
            if open_ended.contains(&cap.name.as_str()) {
                continue;
            }
            let profile = &shipped[&cap.name];
            for child in cap.children.iter().filter(|c| c.kind == "service") {
                checked += 1;
                assert!(
                    profile.unit_names().iter().any(|unit| *unit == child.name
                        || unit
                            .split_once('.')
                            .is_some_and(|(head, _)| head == child.name)),
                    "the catalog describes service `{}` for `{}`, which that profile has no \
                     unit for",
                    child.name,
                    cap.name
                );
            }
        }
        assert!(checked > 0, "the catalog describes no services at all");
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
