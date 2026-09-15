// Project:   dfe-fetcher
// File:      crates/fetcher/src/profiles/mod.rs
// Purpose:   The shipped REST profiles, embedded at build time and parsed once
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shipped REST profiles.
//!
//! A shipped profile is a YAML file under `profiles/` embedded into the binary
//! and referenced by name from `sources.rest.<id>.profile`. The list below is
//! the registry: adding a profile is adding its file and one line here, and
//! the test at the bottom parses and validates every entry so a profile that
//! does not bind never ships.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use dfe_fetcher_rest::RestProfile;

/// `(name, yaml)` for every shipped profile.
const SHIPPED: &[(&str, &str)] = &[
    ("aws", include_str!("../../profiles/aws.yaml")),
    ("azure", include_str!("../../profiles/azure.yaml")),
    ("bitwarden", include_str!("../../profiles/bitwarden.yaml")),
    ("cloudflare", include_str!("../../profiles/cloudflare.yaml")),
    ("crates_io", include_str!("../../profiles/crates_io.yaml")),
    (
        "crowdstrike",
        include_str!("../../profiles/crowdstrike.yaml"),
    ),
    ("duo", include_str!("../../profiles/duo.yaml")),
    ("gcp", include_str!("../../profiles/gcp.yaml")),
    ("gcp_pubsub", include_str!("../../profiles/gcp_pubsub.yaml")),
    ("github", include_str!("../../profiles/github.yaml")),
    ("go_modules", include_str!("../../profiles/go_modules.yaml")),
    (
        "google_workspace",
        include_str!("../../profiles/google_workspace.yaml"),
    ),
    ("m365", include_str!("../../profiles/m365.yaml")),
    (
        "object_store",
        include_str!("../../profiles/object_store.yaml"),
    ),
    ("okta", include_str!("../../profiles/okta.yaml")),
    (
        "onepassword",
        include_str!("../../profiles/onepassword.yaml"),
    ),
    ("pypi", include_str!("../../profiles/pypi.yaml")),
    ("runzero", include_str!("../../profiles/runzero.yaml")),
    ("salesforce", include_str!("../../profiles/salesforce.yaml")),
    ("slack", include_str!("../../profiles/slack.yaml")),
];

static REGISTRY: LazyLock<BTreeMap<String, RestProfile>> = LazyLock::new(|| {
    SHIPPED
        .iter()
        .map(|(name, yaml)| {
            let profile: RestProfile = serde_yaml_ng::from_str(yaml)
                .unwrap_or_else(|e| panic!("shipped profile `{name}` does not parse: {e}"));
            ((*name).to_owned(), profile)
        })
        .collect()
});

/// Every shipped profile by name.
#[must_use]
pub fn shipped() -> &'static BTreeMap<String, RestProfile> {
    &REGISTRY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_shipped_profile_parses_validates_and_is_named_as_registered() {
        for (name, profile) in shipped() {
            assert_eq!(
                &profile.profile, name,
                "registry key and `profile:` must agree"
            );
            let issues = profile.validate();
            assert!(issues.is_empty(), "shipped profile `{name}`: {issues:?}");
        }
        assert_eq!(shipped().len(), SHIPPED.len());
    }

    /// The runZero profile is the contract RECON s18 measured live: NDJSON
    /// exports with no paging, a refusal terminal for the tick, the error text
    /// under `error`, the daily usage counter as a gauge, and a row key that
    /// is store-specific because `id` is the asset id on the joined stores.
    #[test]
    fn runzero_ships_as_a_dump_of_the_verified_export_stores() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, PagerStrategy};

        let profile = shipped().get("runzero").expect("runzero is shipped");
        assert_eq!(profile.shape, UnitShape::Dump);
        assert_eq!(
            profile.auth.accepts,
            [AuthKind::Bearer, AuthKind::Oauth2ClientCredentials],
            "export token on the data path, OAuth2 for the cloud instance"
        );
        assert_eq!(
            profile.auth.oauth2_client_credentials.token_url,
            "{{ base_url }}/account/api/token"
        );
        assert!(profile.auth.oauth2_client_credentials.scope.is_empty());
        assert!(
            !profile.retry.retries(401) && !profile.retry.retries(403),
            "a refusal ticks the console's throttle; never retried"
        );
        assert_eq!(profile.error.at.as_deref(), Some("/error"));
        assert_eq!(
            profile.quota.headers.get("usage_today").map(String::as_str),
            Some("x-api-usage-today")
        );
        assert_eq!(
            profile.defaults.query.get("_oid").map(String::as_str),
            Some("{{ vars.org_id }}"),
            "OAuth needs _oid; an unset var omits it for the export token"
        );
        assert_eq!(
            profile.vars.get("org_id"),
            Some(&serde_json::Value::String(String::new())),
            "the profile defaults org_id to empty so an export-token instance may leave it unset"
        );
        assert!(
            !profile.vars.contains_key("base_url"),
            "base_url has no default: every instance names its console"
        );

        let verified = [
            ("assets", "/id"),
            ("services", "/service_id"),
            ("software", "/software_id"),
            ("vulnerabilities", "/vulnerability_id"),
            ("sites", "/id"),
            ("certificates", "/id"),
            ("findings", "/finding_code"),
            ("tasks", "/id"),
        ];
        for (unit, row_key) in verified {
            let endpoint = profile
                .endpoints
                .iter()
                .find(|e| e.unit == unit)
                .unwrap_or_else(|| panic!("store `{unit}` is declared"));
            assert_eq!(endpoint.path, format!("/export/org/{unit}.jsonl"));
            assert_eq!(endpoint.row_key.as_deref(), Some(row_key), "{unit}");
            assert_eq!(
                profile.method_of(endpoint),
                dfe_fetcher_rest::profile::Method::Get
            );
            let rows = profile.rows_of(endpoint);
            assert_eq!(rows.decoder, DecoderKind::Ndjson, "{unit}");
            assert_eq!(
                profile.paginate_of(endpoint).strategy,
                PagerStrategy::None,
                "{unit}: .jsonl ignores page_size"
            );
        }
        for endpoint in &profile.endpoints {
            assert!(
                std::path::Path::new(&endpoint.path)
                    .extension()
                    .is_some_and(|ext| ext == "jsonl"),
                "{}: every store streams NDJSON",
                endpoint.unit
            );
        }
    }

    /// The GitHub profile is the audit-log contract the characterisation
    /// tests pinned: one incremental unit, a static bearer, the two API
    /// headers, the phrase window to the second with a `+00:00` zone, Link
    /// paging capped at 50 pages, `/user` as the probe, and the public API as
    /// the default base an instance may override.
    #[test]
    fn github_ships_as_the_audit_log_event_window() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, PagerStrategy, WindowFormat};

        let profile = shipped().get("github").expect("github is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::Bearer]);
        assert_eq!(
            profile.headers.get("Accept").map(String::as_str),
            Some("application/vnd.github+json")
        );
        assert_eq!(
            profile
                .headers
                .get("X-GitHub-Api-Version")
                .map(String::as_str),
            Some("2022-11-28")
        );
        assert_eq!(
            profile.window.format,
            WindowFormat::Strftime("%Y-%m-%dT%H:%M:%S+00:00".into())
        );
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert_eq!(
            profile.probe.as_ref().map(|p| p.path.as_str()),
            Some("/user")
        );
        assert_eq!(
            profile.vars.get("api_url"),
            Some(&serde_json::Value::String("https://api.github.com".into()))
        );
        assert_eq!(
            profile.vars.get("include"),
            Some(&serde_json::Value::String("all".into()))
        );
        assert!(
            !profile.vars.contains_key("scope_path"),
            "the scope is the instance's: orgs/<org>/audit-log or enterprises/<ent>/audit-log"
        );
        let [endpoint] = profile.endpoints.as_slice() else {
            panic!("one unit")
        };
        assert_eq!(endpoint.unit, "audit_log");
        assert_eq!(endpoint.path, "/{{ vars.scope_path }}");
        assert_eq!(
            endpoint.query.get("phrase").map(String::as_str),
            Some("created:{{ window.start }}..{{ window.end }}")
        );
        assert_eq!(
            endpoint.query.get("per_page").map(String::as_str),
            Some("100")
        );
        assert_eq!(
            endpoint.query.get("include").map(String::as_str),
            Some("{{ vars.include }}")
        );
        assert_eq!(profile.rows_of(endpoint).decoder, DecoderKind::JsonArray);
        assert_eq!(
            profile.paginate_of(endpoint).strategy,
            PagerStrategy::LinkHeader
        );
        assert_eq!(profile.max_pages_of(endpoint), 50);
    }

    /// The Okta profile is the System Log contract the characterisation
    /// tests pinned: `SSWS` as an API key or a plain bearer, ISO millisecond
    /// window bounds, the ascending page of `limit` rows, an optional
    /// server-side filter, Link paging, `/api/v1/users/me` as the probe, and
    /// no default base URL because every tenant has its own.
    #[test]
    fn okta_ships_as_the_system_log_event_window() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, PagerStrategy, WindowFormat};

        let profile = shipped().get("okta").expect("okta is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::ApiKey, AuthKind::Bearer]);
        assert_eq!(
            profile.auth.api_key.header.as_deref(),
            Some("Authorization")
        );
        assert_eq!(profile.auth.api_key.prefix, "SSWS ");
        assert_eq!(
            profile.headers.get("Accept").map(String::as_str),
            Some("application/json")
        );
        assert_eq!(profile.window.format, WindowFormat::Rfc3339Millis);
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert_eq!(
            profile.probe.as_ref().map(|p| p.path.as_str()),
            Some("/api/v1/users/me")
        );
        assert!(!profile.vars.contains_key("base_url"));
        assert_eq!(profile.vars.get("limit"), Some(&serde_json::json!(100)));
        assert_eq!(
            profile.vars.get("filter"),
            Some(&serde_json::Value::String(String::new())),
            "an empty filter is omitted from the request"
        );
        let [endpoint] = profile.endpoints.as_slice() else {
            panic!("one unit")
        };
        assert_eq!(endpoint.unit, "system_log");
        assert_eq!(endpoint.path, "/api/v1/logs");
        for (name, template) in [
            ("since", "{{ window.start }}"),
            ("until", "{{ window.end }}"),
            ("limit", "{{ vars.limit }}"),
            ("sortOrder", "ASCENDING"),
            ("filter", "{{ vars.filter }}"),
        ] {
            assert_eq!(
                endpoint.query.get(name).map(String::as_str),
                Some(template),
                "{name}"
            );
        }
        assert_eq!(profile.rows_of(endpoint).decoder, DecoderKind::JsonArray);
        assert_eq!(
            profile.paginate_of(endpoint).strategy,
            PagerStrategy::LinkHeader
        );
        assert_eq!(profile.max_pages_of(endpoint), 50);
    }

    /// The one endpoint of a single-unit profile.
    fn only_endpoint(profile: &RestProfile) -> &dfe_fetcher_rest::profile::EndpointSpec {
        let [endpoint] = profile.endpoints.as_slice() else {
            panic!("`{}` declares one unit", profile.profile)
        };
        endpoint
    }

    /// The query templates of an endpoint, for a one-line comparison.
    fn query_of(endpoint: &dfe_fetcher_rest::profile::EndpointSpec) -> Vec<(&str, &str)> {
        endpoint
            .query
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }

    /// The Slack profile is the audit-log contract the characterisation
    /// tests pinned: a bearer, an epoch-second window as `oldest`/`latest`,
    /// `limit` defaulting to 200, optional `action`/`entity`, entries under
    /// `/entries` with the `next_cursor` fed back as `cursor`, `ok: false`
    /// as the failure inside a 200 on the page and on the `auth.test` probe.
    #[test]
    fn slack_ships_as_the_audit_logs_event_window() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, PagerStrategy, WindowFormat};

        let profile = shipped().get("slack").expect("slack is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::Bearer]);
        assert_eq!(profile.window.format, WindowFormat::EpochSecs);
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert_eq!(profile.error.at.as_deref(), Some("/error"));
        let probe = profile.probe.as_ref().expect("a probe");
        assert_eq!(probe.path, "/api/auth.test");
        assert_eq!(probe.fail_when.as_deref(), Some("body.ok == false"));
        assert_eq!(
            profile.vars.get("api_url"),
            Some(&serde_json::Value::String("https://api.slack.com".into()))
        );
        assert_eq!(profile.vars.get("limit"), Some(&serde_json::json!(200)));
        for optional in ["action", "entity"] {
            assert_eq!(
                profile.vars.get(optional),
                Some(&serde_json::Value::String(String::new())),
                "{optional} is omitted unless set"
            );
        }
        let endpoint = only_endpoint(profile);
        assert_eq!(endpoint.unit, "audit_logs");
        assert_eq!(endpoint.path, "/audit/v1/logs");
        assert_eq!(
            query_of(endpoint),
            [
                ("action", "{{ vars.action }}"),
                ("entity", "{{ vars.entity }}"),
                ("latest", "{{ window.end }}"),
                ("limit", "{{ vars.limit }}"),
                ("oldest", "{{ window.start }}"),
            ]
        );
        let rows = profile.rows_of(endpoint);
        assert_eq!(rows.decoder, DecoderKind::JsonAt);
        assert_eq!(rows.at.as_deref(), Some("/entries"));
        let paginate = profile.paginate_of(endpoint);
        assert_eq!(paginate.strategy, PagerStrategy::Cursor);
        assert_eq!(
            paginate.from.as_deref(),
            Some("body:/response_metadata/next_cursor")
        );
        assert_eq!(paginate.into.as_deref(), Some("query:cursor"));
        assert_eq!(endpoint.fail_when.as_deref(), Some("body.ok == false"));
        assert_eq!(profile.max_pages_of(endpoint), 50);
    }

    /// The Cloudflare profile is the account audit-log contract the
    /// characterisation tests pinned: a bearer, RFC 3339 seconds as
    /// `since`/`before`, `per_page` defaulting to 100, optional
    /// `actor.email`/`action.type`, entries under `/result`, page numbers
    /// from 1 bounded by `result_info.total_pages`, `success: false` as the
    /// failure inside a 200 with the reasons under `errors`, and the token
    /// verify endpoint as the probe.
    #[test]
    fn cloudflare_ships_as_the_audit_logs_event_window() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, PagerStrategy, WindowFormat};

        let profile = shipped().get("cloudflare").expect("cloudflare is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::Bearer]);
        assert_eq!(profile.window.format, WindowFormat::Rfc3339Secs);
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert_eq!(profile.error.at.as_deref(), Some("/errors"));
        assert_eq!(
            profile.probe.as_ref().map(|p| p.path.as_str()),
            Some("/user/tokens/verify")
        );
        assert_eq!(
            profile.vars.get("api_url"),
            Some(&serde_json::Value::String(
                "https://api.cloudflare.com/client/v4".into()
            ))
        );
        assert_eq!(profile.vars.get("per_page"), Some(&serde_json::json!(100)));
        assert!(
            !profile.vars.contains_key("account_id"),
            "account_id has no default: every instance names its account"
        );
        let endpoint = only_endpoint(profile);
        assert_eq!(endpoint.unit, "audit_logs");
        assert_eq!(endpoint.path, "/accounts/{{ vars.account_id }}/audit_logs");
        assert_eq!(
            query_of(endpoint),
            [
                ("action.type", "{{ vars.action_type }}"),
                ("actor.email", "{{ vars.actor_email }}"),
                ("before", "{{ window.end }}"),
                ("per_page", "{{ vars.per_page }}"),
                ("since", "{{ window.start }}"),
            ]
        );
        let rows = profile.rows_of(endpoint);
        assert_eq!(rows.decoder, DecoderKind::JsonAt);
        assert_eq!(rows.at.as_deref(), Some("/result"));
        let paginate = profile.paginate_of(endpoint);
        assert_eq!(paginate.strategy, PagerStrategy::PageNumber);
        assert_eq!(paginate.param.as_deref(), Some("page"));
        assert_eq!(paginate.start, Some(1));
        assert_eq!(
            paginate.total_pages_at.as_deref(),
            Some("/result_info/total_pages")
        );
        assert_eq!(endpoint.fail_when.as_deref(), Some("body.success == false"));
        assert_eq!(profile.max_pages_of(endpoint), 50);
    }

    /// The Bitwarden profile is the events contract the characterisation
    /// tests pinned: an OAuth2 client-credentials exchange at the identity
    /// URL with the `api.organization` scope, RFC 3339 seconds as
    /// `start`/`end`, events under `/data`, the `continuationToken` fed back
    /// under the same name, cloud defaults a self-hosted vault overrides, and
    /// no probe because the exchange is the health check.
    #[test]
    fn bitwarden_ships_as_the_events_event_window() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, PagerStrategy, WindowFormat};

        let profile = shipped().get("bitwarden").expect("bitwarden is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::Oauth2ClientCredentials]);
        let oauth = &profile.auth.oauth2_client_credentials;
        assert_eq!(oauth.token_url, "{{ vars.identity_url }}");
        assert_eq!(oauth.scope, "api.organization");
        assert_eq!(oauth.expires_in_fallback_secs, 3600);
        assert_eq!(profile.window.format, WindowFormat::Rfc3339Secs);
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert!(profile.probe.is_none(), "the token exchange is the probe");
        assert_eq!(
            profile.vars.get("api_url"),
            Some(&serde_json::Value::String(
                "https://api.bitwarden.com".into()
            ))
        );
        assert_eq!(
            profile.vars.get("identity_url"),
            Some(&serde_json::Value::String(
                "https://identity.bitwarden.com/connect/token".into()
            ))
        );
        let endpoint = only_endpoint(profile);
        assert_eq!(endpoint.unit, "events");
        assert_eq!(endpoint.path, "/public/events");
        assert_eq!(
            query_of(endpoint),
            [("end", "{{ window.end }}"), ("start", "{{ window.start }}")]
        );
        let rows = profile.rows_of(endpoint);
        assert_eq!(rows.decoder, DecoderKind::JsonAt);
        assert_eq!(rows.at.as_deref(), Some("/data"));
        let paginate = profile.paginate_of(endpoint);
        assert_eq!(paginate.strategy, PagerStrategy::Cursor);
        assert_eq!(paginate.from.as_deref(), Some("body:/continuationToken"));
        assert_eq!(paginate.into.as_deref(), Some("query:continuationToken"));
        assert_eq!(profile.max_pages_of(endpoint), 50);
    }

    /// The 1Password profile is the Events Reporting contract the
    /// characterisation tests pinned: a bearer, three POST units on one
    /// shared body (the RFC 3339 millisecond window and a numeric `limit`
    /// defaulting to 100), items under `/items`, the cursor fed back as the
    /// whole next body, `has_more: false` ending the sequence, the POST
    /// retried as the read it is, and token introspection as the probe.
    #[test]
    fn onepassword_ships_as_three_event_classes_on_one_post_shape() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{
            AuthKind, DecoderKind, Method, PagerStrategy, WindowFormat,
        };

        let profile = shipped()
            .get("onepassword")
            .expect("onepassword is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::Bearer]);
        assert_eq!(profile.window.format, WindowFormat::Rfc3339Millis);
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert!(profile.retry.retry_non_idempotent, "the POST is a read");
        assert_eq!(
            profile.probe.as_ref().map(|p| p.path.as_str()),
            Some("/api/auth/introspect")
        );
        assert_eq!(
            profile.vars.get("api_url"),
            Some(&serde_json::Value::String(
                "https://events.1password.com".into()
            ))
        );
        assert_eq!(profile.vars.get("limit"), Some(&serde_json::json!(100)));
        let units: Vec<(&str, &str)> = profile
            .endpoints
            .iter()
            .map(|e| (e.unit.as_str(), e.path.as_str()))
            .collect();
        assert_eq!(
            units,
            [
                ("signin_attempts", "/api/v2/signinattempts"),
                ("item_usages", "/api/v2/itemusages"),
                ("audit_events", "/api/v2/auditevents"),
            ]
        );
        for endpoint in &profile.endpoints {
            assert_eq!(
                profile.method_of(endpoint),
                Method::Post,
                "{}",
                endpoint.unit
            );
            assert_eq!(
                profile.body_of(endpoint),
                Some(&serde_json::json!({
                    "limit": "{{ vars.limit }}",
                    "start_time": "{{ window.start }}",
                    "end_time": "{{ window.end }}"
                })),
                "{}",
                endpoint.unit
            );
            let rows = profile.rows_of(endpoint);
            assert_eq!(rows.decoder, DecoderKind::JsonAt);
            assert_eq!(rows.at.as_deref(), Some("/items"));
            let paginate = profile.paginate_of(endpoint);
            assert_eq!(paginate.strategy, PagerStrategy::Cursor);
            assert_eq!(paginate.from.as_deref(), Some("body:/cursor"));
            assert_eq!(paginate.into.as_deref(), Some("body_replace:/cursor"));
            assert_eq!(
                paginate.stop_when.as_deref(),
                Some("body.has_more == false")
            );
            assert_eq!(profile.max_pages_of(endpoint), 50);
        }
    }

    /// A per-key registry profile (PyPI, crates.io) is the contract the
    /// characterisation tests pinned: no credential, one document per key
    /// of the instance's list with a 404 skipped, the key stamped on the
    /// document under the given field, and a reachability probe.
    fn assert_registry_profile(name: &str, path: &str, stamp: &str, probe: &str, api_url: &str) {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, PagerStrategy};

        let profile = shipped()
            .get(name)
            .unwrap_or_else(|| panic!("{name} is shipped"));
        assert_eq!(profile.shape, UnitShape::Incremental, "{name}: bare rows");
        assert_eq!(profile.auth.accepts, [AuthKind::None]);
        assert_eq!(profile.probe.as_ref().map(|p| p.path.as_str()), Some(probe));
        assert_eq!(
            profile.vars.get("api_url"),
            Some(&serde_json::Value::String(api_url.into()))
        );
        let endpoint = only_endpoint(profile);
        assert_eq!(endpoint.unit, "metadata");
        assert_eq!(endpoint.path, path);
        assert_eq!(profile.rows_of(endpoint).decoder, DecoderKind::Document);
        assert_eq!(profile.paginate_of(endpoint).strategy, PagerStrategy::None);
        assert_eq!(endpoint.ignore_status, [404], "an unknown key is skipped");
        assert_eq!(
            endpoint
                .add_fields
                .get(stamp)
                .and_then(serde_json::Value::as_str),
            Some("{{ key }}")
        );
        assert!(endpoint.keyset().is_some(), "one request per key");
    }

    #[test]
    fn pypi_ships_as_one_document_per_package() {
        assert_registry_profile(
            "pypi",
            "/pypi/{{ key }}/json",
            "_dfe_fetcher_package",
            "/",
            "https://pypi.org",
        );
        assert_eq!(
            shipped()["pypi"].endpoints[0]
                .keyset()
                .and_then(|k| k.from.as_deref()),
            Some("{{ vars.packages }}")
        );
    }

    #[test]
    fn crates_io_ships_as_one_document_per_crate_with_a_contact_user_agent() {
        assert_registry_profile(
            "crates_io",
            "/api/v1/crates/{{ key }}",
            "_dfe_fetcher_crate",
            "/api/v1/summary",
            "https://crates.io",
        );
        let profile = &shipped()["crates_io"];
        assert_eq!(
            profile.endpoints[0]
                .keyset()
                .and_then(|k| k.from.as_deref()),
            Some("{{ vars.crates }}")
        );
        assert_eq!(
            profile.headers.get("User-Agent").map(String::as_str),
            Some("dfe-fetcher (https://github.com/hyperi-io/dfe-fetcher)"),
            "crates.io's usage policy asks for a contact"
        );
    }

    /// The Go module-proxy profile is the fold contract the characterisation
    /// tests pinned: no credential, the version list of each module as text
    /// lines (a 404 skipped), each version's `.info` document as a manifest
    /// item in list order (a 404 yielding nothing, a hundred a module), the
    /// module's documents folded into one row stamped with the module, and
    /// the proxy's root as the probe.
    #[test]
    fn go_modules_ships_as_one_folded_row_per_module() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, PagerStrategy, RowBuilderKind};

        let profile = shipped().get("go_modules").expect("go_modules is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::None]);
        assert_eq!(profile.probe.as_ref().map(|p| p.path.as_str()), Some("/"));
        assert_eq!(
            profile.vars.get("api_url"),
            Some(&serde_json::Value::String(
                "https://proxy.golang.org".into()
            ))
        );
        let endpoint = only_endpoint(profile);
        assert_eq!(endpoint.unit, "metadata");
        assert_eq!(endpoint.path, "/{{ key }}/@v/list");
        assert_eq!(profile.rows_of(endpoint).decoder, DecoderKind::Lines);
        assert_eq!(profile.paginate_of(endpoint).strategy, PagerStrategy::None);
        assert_eq!(
            endpoint.ignore_status,
            [404],
            "an unpublished module is skipped"
        );
        assert_eq!(endpoint.fold, Some(RowBuilderKind::GoModuleAggregate));
        assert_eq!(
            endpoint
                .add_fields
                .get("_dfe_fetcher_module")
                .and_then(serde_json::Value::as_str),
            Some("{{ key }}")
        );
        assert_eq!(
            endpoint.keyset().and_then(|k| k.from.as_deref()),
            Some("{{ vars.modules }}")
        );
        let manifest = profile
            .manifest_of(endpoint)
            .expect("a manifest per version");
        assert_eq!(
            manifest.item_request.path,
            "/{{ key }}/@v/{{ item.line }}.info"
        );
        assert_eq!(manifest.item_request.ignore_status, [404]);
        assert_eq!(manifest.rows.decoder, DecoderKind::Document);
        assert_eq!(manifest.max_items, Some(100));
        assert!(manifest.key.is_none(), "a module's state has no checkpoint");
    }

    /// The Pub/Sub profile is the queue contract the characterisation tests
    /// pinned: a service-account JWT on the Pub/Sub scope (or the metadata
    /// server), one `:pull` per subscription unit with `maxMessages` and
    /// `returnImmediately` from the unit's vars, each received message
    /// built into its record, the `ackId` as the row's mark, and the
    /// `:acknowledge` with the delivered ids five hundred at a time; the
    /// token exchange is the health check.
    #[test]
    fn gcp_pubsub_ships_as_a_pull_per_subscription_acknowledged_after_delivery() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, Method, RowBuilderKind};

        let profile = shipped().get("gcp_pubsub").expect("gcp_pubsub is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert!(profile.is_queue(), "runs on the queue shape");
        assert_eq!(
            profile.auth.accepts,
            [AuthKind::JwtBearer, AuthKind::GceMetadata]
        );
        let jwt = &profile.auth.jwt_bearer;
        assert_eq!(
            jwt.claims.get("scope").map(String::as_str),
            Some("https://www.googleapis.com/auth/pubsub")
        );
        assert_eq!(
            jwt.claims.get("iss").map(String::as_str),
            Some("{{ auth.client_email }}")
        );
        assert_eq!(
            jwt.claims.get("aud").map(String::as_str),
            Some("{{ auth.token_url }}")
        );
        assert!(!jwt.claims.contains_key("sub"), "no impersonation");
        assert_eq!(jwt.ttl_secs, 3600);
        assert!(profile.probe.is_none(), "the exchange is the health check");
        assert!(
            profile.retry.retry_non_idempotent,
            "a pull and an ack are safe to repeat"
        );
        assert_eq!(profile.error.at.as_deref(), Some("/error/message"));
        assert_eq!(
            profile.vars.get("api_url"),
            Some(&serde_json::Value::String(
                "https://pubsub.googleapis.com".into()
            ))
        );
        let endpoint = only_endpoint(profile);
        assert_eq!(endpoint.unit, "pull");
        assert_eq!(profile.method_of(endpoint), Method::Post);
        assert_eq!(
            endpoint.path,
            "/v1/projects/{{ vars.project_id }}/subscriptions/{{ vars.subscription_id }}:pull"
        );
        let body = endpoint.body.as_ref().expect("a pull body");
        assert_eq!(body["maxMessages"], "{{ vars.max_messages }}");
        assert_eq!(body["returnImmediately"], "{{ vars.return_immediately }}");
        let rows = profile.rows_of(endpoint);
        assert_eq!(rows.decoder, DecoderKind::JsonAt);
        assert_eq!(rows.at.as_deref(), Some("/receivedMessages"));
        assert_eq!(rows.builder, Some(RowBuilderKind::PubsubMessage));
        assert_eq!(profile.max_pages_of(endpoint), 1, "one pull per tick");
        let queue = profile.queue_of(endpoint).expect("a queue");
        assert_eq!(queue.ack_at, "/ackId");
        assert_eq!(
            queue.ack_request.path,
            "/v1/projects/{{ vars.project_id }}/subscriptions/{{ vars.subscription_id }}:acknowledge"
        );
        assert_eq!(
            queue.ack_request.body.as_ref().unwrap()["ackIds"],
            "{{ ids }}"
        );
        assert_eq!(queue.ack_batch, 500);
    }

    /// The Salesforce profile is the audit contract the characterisation
    /// tests pinned: the JWT-bearer grant (consumer key, username, login
    /// host, five minutes) or client credentials at the login host's token
    /// endpoint, the org's `instance_url` exposed by the exchange and read
    /// by every unit, the two SOQL units with their documented fields and
    /// the window as bare RFC 3339 literals, `nextRecordsUrl` followed for
    /// fifty pages, and EventLogFile as a manifest of CSV downloads capped
    /// at two hundred a tick.
    #[test]
    fn salesforce_ships_as_soql_audit_windows_and_an_event_log_file_manifest() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, PagerStrategy, WindowFormat};

        let profile = shipped().get("salesforce").expect("salesforce is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(
            profile.auth.accepts,
            [AuthKind::JwtBearer, AuthKind::Oauth2ClientCredentials]
        );
        let jwt = &profile.auth.jwt_bearer;
        assert_eq!(jwt.token_url, "{{ base_url }}/services/oauth2/token");
        assert_eq!(
            jwt.claims.get("iss").map(String::as_str),
            Some("{{ vars.client_id }}")
        );
        assert_eq!(
            jwt.claims.get("sub").map(String::as_str),
            Some("{{ vars.username }}")
        );
        assert_eq!(
            jwt.claims.get("aud").map(String::as_str),
            Some("{{ base_url }}")
        );
        assert_eq!(jwt.ttl_secs, 300);
        assert_eq!(jwt.expose, ["instance_url"]);
        let oauth = &profile.auth.oauth2_client_credentials;
        assert_eq!(oauth.token_url, "{{ base_url }}/services/oauth2/token");
        assert_eq!(oauth.expose, ["instance_url"]);
        assert!(profile.probe.is_none(), "the exchange is the health check");
        assert_eq!(profile.window.format, WindowFormat::Rfc3339Secs);
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert_eq!(profile.error.at.as_deref(), Some("/0/message"));
        assert_eq!(
            profile.vars.get("login_url"),
            Some(&serde_json::Value::String(
                "https://login.salesforce.com".into()
            ))
        );
        assert_eq!(
            profile.vars.get("api_version"),
            Some(&serde_json::Value::String("v60.0".into()))
        );
        assert_eq!(
            profile.defaults.path.as_deref(),
            Some(
                "{{ vars.instance_url != '' ? vars.instance_url : auth.instance_url }}/services/data/{{ vars.api_version }}/query"
            ),
            "the pinned instance URL wins over the exchange's"
        );
        assert_eq!(
            profile.defaults.paginate.as_ref().map(|p| p.strategy),
            Some(PagerStrategy::RequestPath)
        );
        assert_eq!(
            profile
                .defaults
                .paginate
                .as_ref()
                .and_then(|p| p.from.as_deref()),
            Some("body:/nextRecordsUrl")
        );
        assert_eq!(profile.defaults.max_pages, Some(50));
        let units: Vec<&str> = profile.endpoints.iter().map(|e| e.unit.as_str()).collect();
        assert_eq!(
            units,
            ["setup_audit_trail", "login_history", "event_log_file"]
        );
        let setup = &profile.endpoints[0];
        assert_eq!(
            setup.query.get("q").map(String::as_str),
            Some(
                "SELECT Id, Action, Section, CreatedDate, Display, DelegateUser, CreatedBy.Username FROM SetupAuditTrail WHERE CreatedDate >= {{ window.start }} AND CreatedDate < {{ window.end }} ORDER BY CreatedDate ASC"
            )
        );
        assert_eq!(profile.rows_of(setup).decoder, DecoderKind::JsonAt);
        assert_eq!(profile.rows_of(setup).at.as_deref(), Some("/records"));
        let login = &profile.endpoints[1];
        assert!(
            login.query["q"].starts_with("SELECT Id, UserId, LoginTime, LoginType, SourceIp, Status, Application, Browser, Platform, CountryIso, ApiType, TlsProtocol FROM LoginHistory WHERE LoginTime >= {{ window.start }}")
        );
        let elf = &profile.endpoints[2];
        assert!(elf.query["q"].contains("FROM EventLogFile WHERE LogDate >= {{ window.start }} AND LogDate < {{ window.end }} AND Interval = '{{ vars.interval }}'"));
        assert!(elf.query["q"].ends_with("ORDER BY LogDate ASC"));
        let manifest = profile.manifest_of(elf).expect("a manifest per log file");
        assert!(
            manifest
                .item_request
                .path
                .ends_with("/sobjects/EventLogFile/{{ item.Id }}/LogFile")
        );
        assert_eq!(manifest.rows.decoder, DecoderKind::Csv);
        assert!(manifest.rows.header);
        assert_eq!(manifest.max_items, Some(200));
        assert_eq!(
            manifest.add_fields["_dfe_fetcher_event_type"],
            "{{ item.EventType }}"
        );
        assert_eq!(
            manifest.add_fields["_dfe_fetcher_log_date"],
            "{{ item.LogDate }}"
        );
        assert!(
            manifest.key.is_none(),
            "the window narrows the files, not a checkpoint"
        );
    }

    /// The object-store profile is the S3 contract the characterisation
    /// tests pinned: SigV4 for `s3` in the instance's region on a
    /// path-style host, the `ListObjectsV2` listing under the bucket with
    /// the prefix and a thousand keys a page for ten pages, one endpoint
    /// per object format each a lister feeding a manifest that reads a
    /// thousand objects a tick by key with the object envelope stamped on
    /// every row and the object's key and time as its checkpoint, and a
    /// day's lookback for a first tick.
    #[test]
    fn object_store_ships_as_a_listed_prefix_per_format_on_a_read_once_checkpoint() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, ListerKind, RowBuilderKind};

        let profile = shipped()
            .get("object_store")
            .expect("object_store is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::SigV4]);
        assert_eq!(profile.auth.sigv4.service, "s3");
        assert_eq!(profile.auth.sigv4.region, "{{ vars.region }}");
        assert_eq!(profile.window.lookback.0.as_secs(), 86_400);
        assert!(
            profile.probe.is_none(),
            "the key pair resolving is the health check"
        );
        assert_eq!(profile.defaults.path.as_deref(), Some("{{ vars.bucket }}/"));
        assert_eq!(
            profile.defaults.query.get("list-type").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            profile.defaults.query.get("prefix").map(String::as_str),
            Some("{{ vars.prefix }}")
        );
        assert_eq!(
            profile.defaults.query.get("max-keys").map(String::as_str),
            Some("1000")
        );
        assert_eq!(profile.defaults.max_pages, Some(10));
        let formats = [
            (
                "json_gz",
                DecoderKind::Ndjson,
                true,
                Some(RowBuilderKind::WrapNonObject),
            ),
            (
                "jsonl",
                DecoderKind::Ndjson,
                false,
                Some(RowBuilderKind::WrapNonObject),
            ),
            (
                "json",
                DecoderKind::Json,
                false,
                Some(RowBuilderKind::WrapNonObject),
            ),
            ("text", DecoderKind::Lines, false, None),
            ("text_gz", DecoderKind::Lines, true, None),
        ];
        assert_eq!(profile.endpoints.len(), formats.len());
        for (unit, decoder, gzip, builder) in formats {
            let endpoint = profile
                .endpoints
                .iter()
                .find(|e| e.unit == unit)
                .unwrap_or_else(|| panic!("format `{unit}` is an endpoint"));
            assert_eq!(endpoint.lister, Some(ListerKind::S3), "{unit}");
            let manifest = profile
                .manifest_of(endpoint)
                .expect("a manifest per object");
            assert_eq!(
                manifest.item_request.path, "{{ vars.bucket }}/{{ item.path }}",
                "{unit}"
            );
            assert_eq!(manifest.rows.decoder, decoder, "{unit}");
            assert_eq!(manifest.rows.gzip, gzip, "{unit}");
            assert_eq!(manifest.rows.builder, builder, "{unit}");
            assert_eq!(manifest.key.as_deref(), Some("{{ item.key }}"));
            assert_eq!(
                manifest.position.as_deref(),
                Some("{{ item.last_modified }}")
            );
            assert_eq!(manifest.max_items, Some(1000), "{unit}");
            let envelope = &manifest.add_fields["_dfe_fetcher_object"];
            assert_eq!(envelope["provider"], "s3");
            assert_eq!(envelope["bucket"], "{{ vars.bucket }}");
            assert_eq!(envelope["key"], "{{ item.key }}");
            assert_eq!(envelope["last_modified"], "{{ item.last_modified }}");
            assert_eq!(envelope["size"], "{{ item.size }}");
        }
    }

    /// The CrowdStrike profile is the two-stage alerts contract the
    /// characterisation tests pinned: an OAuth2 exchange at `/oauth2/token`
    /// with no scope, the FQL window clause with the instance filter ANDed
    /// on, `limit` 100 default, ascending sort, ids under `/resources`
    /// walked by `offset` against the total, then the entities POST per
    /// batch of 1000 composite ids, retried as the read it is.
    #[test]
    fn crowdstrike_ships_as_an_offset_id_query_with_an_entity_lookup() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{
            AuthKind, DecoderKind, Method, PagerStrategy, WindowFormat,
        };

        let profile = shipped()
            .get("crowdstrike")
            .expect("crowdstrike is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::Oauth2ClientCredentials]);
        let oauth = &profile.auth.oauth2_client_credentials;
        assert_eq!(oauth.token_url, "{{ base_url }}/oauth2/token");
        assert!(oauth.scope.is_empty(), "Falcon takes no scope");
        assert_eq!(oauth.expires_in_fallback_secs, 1800);
        assert_eq!(profile.window.format, WindowFormat::Rfc3339Secs);
        assert!(
            profile.retry.retry_non_idempotent,
            "the lookup POST is a read"
        );
        assert!(profile.probe.is_none(), "the token exchange is the probe");
        assert_eq!(
            profile.vars.get("api_url"),
            Some(&serde_json::Value::String(
                "https://api.crowdstrike.com".into()
            ))
        );
        assert_eq!(profile.vars.get("limit"), Some(&serde_json::json!(100)));
        assert_eq!(
            profile.vars.get("filter"),
            Some(&serde_json::Value::String(String::new()))
        );
        let endpoint = only_endpoint(profile);
        assert_eq!(endpoint.unit, "alerts");
        assert_eq!(endpoint.path, "/alerts/queries/alerts/v2");
        assert_eq!(
            query_of(endpoint),
            [
                (
                    "filter",
                    "created_timestamp:>'{{ window.start }}'+created_timestamp:<'{{ window.end }}'{{ vars.filter != '' ? '+' + vars.filter : '' }}"
                ),
                ("limit", "{{ vars.limit }}"),
                ("sort", "created_timestamp.asc"),
            ]
        );
        let rows = profile.rows_of(endpoint);
        assert_eq!(rows.decoder, DecoderKind::JsonAt);
        assert_eq!(rows.at.as_deref(), Some("/resources"));
        let paginate = profile.paginate_of(endpoint);
        assert_eq!(paginate.strategy, PagerStrategy::Offset);
        assert_eq!(paginate.param.as_deref(), Some("offset"));
        assert_eq!(paginate.total_at.as_deref(), Some("/meta/pagination/total"));
        let lookup = endpoint.lookup().expect("the entity lookup");
        assert_eq!(lookup.batch, 1000, "Falcon's ids-per-POST cap");
        assert_eq!(lookup.request.method_or(Method::Post), Method::Post);
        assert_eq!(lookup.request.path, "/alerts/entities/alerts/v2");
        assert_eq!(
            lookup.request.body,
            Some(serde_json::json!({"composite_ids": "{{ ids }}"}))
        );
        assert_eq!(lookup.rows.decoder, DecoderKind::JsonAt);
        assert_eq!(lookup.rows.at.as_deref(), Some("/resources"));
        assert_eq!(profile.max_pages_of(endpoint), 50);
    }

    /// The Azure profile is the tenant audit contract the characterisation
    /// tests pinned: one client-credentials identity minting a token per
    /// API audience (Management by default, Graph and Log Analytics per
    /// unit), the three Resource Manager units under the subscription with
    /// their `api-version` and `nextLink` paging, the three Entra units on
    /// the Graph host with `$top` 100, the window as an OData clause on
    /// each one's time field and `@odata.nextLink` paging, and Log
    /// Analytics as one KQL POST per configured query with the result
    /// tables built into rows; RFC 3339 seconds, a day of lookback, ten
    /// pages a tick, the public clouds as the default hosts.
    #[test]
    fn azure_ships_as_three_audiences_of_one_service_principal() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{
            AuthKind, DecoderKind, Method, PagerStrategy, RowBuilderKind, WindowFormat,
        };

        let profile = shipped().get("azure").expect("azure is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::Oauth2ClientCredentials]);
        let oauth = &profile.auth.oauth2_client_credentials;
        assert_eq!(oauth.scope, "https://management.azure.com/.default");
        assert!(
            oauth.token_url.contains("vars.token_url")
                && oauth.token_url.contains("vars.tenant_id"),
            "the override wins, else the tenant's v2.0 endpoint: {}",
            oauth.token_url
        );
        assert_eq!(profile.window.format, WindowFormat::Rfc3339Secs);
        assert_eq!(profile.window.lookback.0.as_secs(), 24 * 3600);
        assert_eq!(profile.error.at.as_deref(), Some("/error/message"));
        assert!(profile.probe.is_none(), "the token exchange is the probe");
        for (var, host) in [
            ("management_url", "https://management.azure.com"),
            ("graph_url", "https://graph.microsoft.com"),
            ("log_analytics_url", "https://api.loganalytics.io"),
            ("login_url", "https://login.microsoftonline.com"),
        ] {
            assert_eq!(
                profile.vars.get(var),
                Some(&serde_json::Value::String(host.into())),
                "{var}"
            );
        }
        assert!(
            !profile.vars.contains_key("subscription_id")
                && !profile.vars.contains_key("tenant_id"),
            "identity has no default"
        );
        let units: Vec<&str> = profile.endpoints.iter().map(|e| e.unit.as_str()).collect();
        assert_eq!(
            units,
            [
                "activity_log",
                "defender",
                "sentinel",
                "entra_signins",
                "entra_directory_audits",
                "entra_provisioning",
                "log_analytics",
            ]
        );
        for endpoint in &profile.endpoints[..3] {
            assert!(
                endpoint
                    .path
                    .starts_with("/subscriptions/{{ vars.subscription_id }}/"),
                "{}",
                endpoint.unit
            );
            assert!(endpoint.base_url.is_none() && endpoint.auth_scope().is_none());
            let paginate = profile.paginate_of(endpoint);
            assert_eq!(paginate.strategy, PagerStrategy::RequestPath);
            assert_eq!(paginate.from.as_deref(), Some("body:/nextLink"));
            assert_eq!(profile.rows_of(endpoint).at.as_deref(), Some("/value"));
            assert_eq!(profile.max_pages_of(endpoint), 10);
        }
        assert_eq!(
            profile.endpoints[0]
                .query
                .get("$filter")
                .map(String::as_str),
            Some("eventTimestamp ge '{{ window.start }}' and eventTimestamp le '{{ window.end }}'")
        );
        for (endpoint, time_field) in profile.endpoints[3..6].iter().zip([
            "createdDateTime",
            "activityDateTime",
            "activityDateTime",
        ]) {
            assert_eq!(endpoint.base_url.as_deref(), Some("{{ vars.graph_url }}"));
            assert_eq!(
                endpoint.auth_scope(),
                Some("https://graph.microsoft.com/.default")
            );
            assert_eq!(endpoint.vars["time_field"], time_field, "{}", endpoint.unit);
            assert_eq!(endpoint.query.get("$top").map(String::as_str), Some("100"));
            assert_eq!(
                profile.paginate_of(endpoint).from.as_deref(),
                Some("body:/@odata.nextLink")
            );
        }
        let la = &profile.endpoints[6];
        assert_eq!(la.base_url.as_deref(), Some("{{ vars.log_analytics_url }}"));
        assert_eq!(
            la.auth_scope(),
            Some("https://api.loganalytics.io/.default")
        );
        assert_eq!(profile.method_of(la), Method::Post);
        assert_eq!(la.path, "/v1/workspaces/{{ key.workspace_id }}/query");
        assert_eq!(
            profile.body_of(la),
            Some(
                &serde_json::json!({"query": "{{ key.kql }}", "timespan": "{{ window.start }}/{{ window.end }}"})
            )
        );
        let rows = profile.rows_of(la);
        assert_eq!(rows.decoder, DecoderKind::JsonAt);
        assert_eq!(rows.at.as_deref(), Some("/tables"));
        assert_eq!(rows.builder, Some(RowBuilderKind::ColumnarTable));
        assert_eq!(profile.paginate_of(la).strategy, PagerStrategy::None);
        assert_eq!(
            la.keyset().and_then(|k| k.from.as_deref()),
            Some("{{ vars.log_analytics_queries }}")
        );
    }

    /// The M365 profile is the tenant audit contract the characterisation
    /// tests pinned: one Management Activity token for the OMAP units and
    /// a Graph token for `alerts`; every OMAP unit starts its subscription
    /// blind each tick ignoring the 400 an enabled feed answers, lists its
    /// content type with the window to the second and no zone suffix in
    /// day-sized steps under the publisher identifier, follows the
    /// `NextPageUri` header, and fetches each item's `contentUri` as a JSON
    /// array of records marked with the item; the five `audit_log.*` feeds,
    /// `dlp` and `exchange_audit` share one query; `alerts` reads no window
    /// and pages Graph by `@odata.nextLink`, ten pages of 100.
    #[test]
    fn m365_ships_as_omap_content_manifests_and_graph_alerts() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{
            AuthKind, DecoderKind, Method, PagerStrategy, WindowFormat,
        };

        let profile = shipped().get("m365").expect("m365 is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::Oauth2ClientCredentials]);
        let oauth = &profile.auth.oauth2_client_credentials;
        assert_eq!(oauth.scope, "https://manage.office.com/.default");
        assert!(
            oauth.token_url.contains("vars.token_url")
                && oauth.token_url.contains("vars.tenant_id"),
            "the override wins, else the tenant's v2.0 endpoint: {}",
            oauth.token_url
        );
        assert_eq!(
            profile.window.format,
            WindowFormat::Strftime("%Y-%m-%dT%H:%M:%S".into())
        );
        assert_eq!(profile.window.step.map(|s| s.0.as_secs()), Some(24 * 3600));
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert_eq!(profile.error.at.as_deref(), Some("/error/message"));
        assert!(profile.probe.is_none(), "the token exchange is the probe");
        for (var, value) in [
            ("management_url", "https://manage.office.com"),
            ("graph_url", "https://graph.microsoft.com"),
            ("login_url", "https://login.microsoftonline.com"),
            (
                "publisher_identifier",
                "12345678-1234-1234-1234-123456789123",
            ),
        ] {
            assert_eq!(
                profile.vars.get(var),
                Some(&serde_json::Value::String(value.into())),
                "{var}"
            );
        }
        assert!(
            !profile.vars.contains_key("tenant_id"),
            "identity has no default"
        );
        let units: Vec<&str> = profile.endpoints.iter().map(|e| e.unit.as_str()).collect();
        assert_eq!(
            units,
            [
                "audit_log.audit_azureactivedirectory",
                "audit_log.audit_exchange",
                "audit_log.audit_sharepoint",
                "audit_log.audit_general",
                "audit_log.dlp_all",
                "dlp",
                "exchange_audit",
                "alerts",
            ]
        );
        for (endpoint, content_type) in profile.endpoints[..7].iter().zip([
            "Audit.AzureActiveDirectory",
            "Audit.Exchange",
            "Audit.SharePoint",
            "Audit.General",
            "DLP.All",
            "DLP.All",
            "Audit.Exchange",
        ]) {
            let unit = &endpoint.unit;
            assert_eq!(endpoint.vars["content_type"], content_type, "{unit}");
            assert!(endpoint.base_url.is_none() && endpoint.auth_scope().is_none());
            assert_eq!(
                profile.path_of(endpoint),
                "/api/v1.0/{{ vars.tenant_id }}/activity/feed/subscriptions/content"
            );
            assert_eq!(profile.method_of(endpoint), Method::Get);
            let query: Vec<(&str, &str)> = endpoint
                .query
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            assert_eq!(
                query,
                [
                    ("PublisherIdentifier", "{{ vars.publisher_identifier }}"),
                    ("contentType", "{{ vars.content_type }}"),
                    ("endTime", "{{ window.end }}"),
                    ("startTime", "{{ window.start }}"),
                ],
                "{unit}"
            );
            assert_eq!(profile.rows_of(endpoint).decoder, DecoderKind::JsonArray);
            let paginate = profile.paginate_of(endpoint);
            assert_eq!(paginate.strategy, PagerStrategy::RequestPath);
            assert_eq!(paginate.from.as_deref(), Some("header:NextPageUri"));
            assert_eq!(profile.max_pages_of(endpoint), 50);
            let prelude = profile.prelude_of(endpoint);
            assert_eq!(prelude.len(), 1, "{unit}");
            assert_eq!(
                prelude[0].path,
                "/api/v1.0/{{ vars.tenant_id }}/activity/feed/subscriptions/start"
            );
            assert_eq!(prelude[0].method_or(Method::Post), Method::Post);
            assert_eq!(
                prelude[0].query.get("contentType").map(String::as_str),
                Some("{{ vars.content_type }}")
            );
            assert_eq!(prelude[0].ignore_status, [400]);
            let manifest = profile.manifest_of(endpoint).expect("a manifest");
            assert_eq!(manifest.item_request.path, "{{ item.contentUri }}");
            assert_eq!(manifest.item_request.method_or(Method::Get), Method::Get);
            assert_eq!(manifest.rows.decoder, DecoderKind::JsonArray);
            assert_eq!(manifest.key.as_deref(), Some("{{ item.contentId }}"));
            assert_eq!(
                manifest.position.as_deref(),
                Some("{{ item.contentCreated }}")
            );
            assert!(profile.lookup_of(endpoint).is_none() && profile.keyset_of(endpoint).is_none());
        }
        let alerts = &profile.endpoints[7];
        assert_eq!(alerts.base_url.as_deref(), Some("{{ vars.graph_url }}"));
        assert_eq!(
            alerts.auth_scope(),
            Some("https://graph.microsoft.com/.default")
        );
        assert_eq!(alerts.path, "/v1.0/security/alerts_v2");
        assert_eq!(alerts.query.get("$top").map(String::as_str), Some("100"));
        assert_eq!(
            alerts.query.get("$orderby").map(String::as_str),
            Some("createdDateTime desc")
        );
        assert!(
            !alerts.query.values().any(|v| v.contains("window")),
            "the tenant's current alerts, not a window"
        );
        assert_eq!(profile.rows_of(alerts).at.as_deref(), Some("/value"));
        assert_eq!(
            profile.paginate_of(alerts).from.as_deref(),
            Some("body:/@odata.nextLink")
        );
        assert_eq!(profile.max_pages_of(alerts), 10);
        assert!(profile.prelude_of(alerts).is_empty(), "no subscription");
        assert!(
            profile
                .construct_of(alerts)
                .is_some_and(|c| c.manifest.is_none()),
            "no manifest"
        );
    }

    /// The GCP profile is the project audit contract the characterisation
    /// tests pinned: a service-account assertion (or the metadata server, or
    /// a resolved bearer) for the cloud-platform scope, eight Cloud Logging
    /// units on one `entries:list` POST with their documented filter clauses
    /// and the window in RFC 3339 `+00:00`, `nextPageToken` fed back in the
    /// body, SCC findings as a GET under the organisation paged by query,
    /// an hour of lookback, ten pages a tick, the public endpoints as the
    /// default hosts.
    #[test]
    fn gcp_ships_as_cloud_logging_units_and_scc_on_a_service_account() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{
            AuthKind, DecoderKind, Method, PagerStrategy, WindowFormat,
        };

        let profile = shipped().get("gcp").expect("gcp is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(
            profile.auth.accepts,
            [AuthKind::JwtBearer, AuthKind::GceMetadata, AuthKind::Bearer]
        );
        let jwt = &profile.auth.jwt_bearer;
        assert!(
            jwt.token_url.contains("vars.token_url") && jwt.token_url.contains("auth.token_uri"),
            "the override wins, else the key's token_uri: {}",
            jwt.token_url
        );
        assert_eq!(
            jwt.claims.get("iss").map(String::as_str),
            Some("{{ auth.client_email }}")
        );
        assert_eq!(
            jwt.claims.get("scope").map(String::as_str),
            Some("https://www.googleapis.com/auth/cloud-platform")
        );
        assert_eq!(
            jwt.claims.get("aud").map(String::as_str),
            Some("{{ auth.token_url }}")
        );
        assert!(!jwt.claims.contains_key("sub"), "no delegation on GCP");
        assert_eq!(jwt.ttl_secs, 3600);
        assert_eq!(profile.auth.gce_metadata.url, "{{ vars.metadata_url }}");
        assert_eq!(
            profile.window.format,
            WindowFormat::Strftime("%Y-%m-%dT%H:%M:%S%.f+00:00".into())
        );
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert!(
            profile.retry.retry_non_idempotent,
            "the list POST is a read"
        );
        assert_eq!(profile.error.at.as_deref(), Some("/error/message"));
        assert!(profile.probe.is_none(), "the token exchange is the probe");
        assert_eq!(
            profile.vars.get("logging_url"),
            Some(&serde_json::Value::String(
                "https://logging.googleapis.com".into()
            ))
        );
        assert_eq!(
            profile.vars.get("scc_url"),
            Some(&serde_json::Value::String(
                "https://securitycenter.googleapis.com".into()
            ))
        );
        assert!(
            !profile.vars.contains_key("project_id")
                && !profile.vars.contains_key("organization_id"),
            "identity has no default"
        );
        let logging: Vec<(&str, &str)> = profile.endpoints[..8]
            .iter()
            .map(|e| (e.unit.as_str(), e.vars["log_filter"].as_str().unwrap()))
            .collect();
        assert_eq!(
            logging,
            [
                (
                    "admin_activity",
                    "log_id(\"cloudaudit.googleapis.com/activity\")"
                ),
                (
                    "data_access",
                    "log_id(\"cloudaudit.googleapis.com/data_access\")"
                ),
                (
                    "system_event",
                    "log_id(\"cloudaudit.googleapis.com/system_event\")"
                ),
                (
                    "policy_denied",
                    "log_id(\"cloudaudit.googleapis.com/policy\")"
                ),
                (
                    "vpc_flow_logs",
                    "log_id(\"compute.googleapis.com/vpc_flows\")"
                ),
                ("dns_queries", "log_id(\"dns.googleapis.com/dns_queries\")"),
                (
                    "storage_access",
                    "resource.type=\"gcs_bucket\" AND log_id(\"cloudaudit.googleapis.com/data_access\")"
                ),
                ("cloud_logging", "severity >= WARNING"),
            ]
        );
        for endpoint in &profile.endpoints[..8] {
            assert_eq!(
                profile.method_of(endpoint),
                Method::Post,
                "{}",
                endpoint.unit
            );
            assert_eq!(profile.path_of(endpoint), "/v2/entries:list");
            assert_eq!(
                profile.body_of(endpoint),
                Some(&serde_json::json!({
                    "resourceNames": ["projects/{{ vars.project_id }}"],
                    "filter": "{{ vars.log_filter }} AND timestamp >= \"{{ window.start }}\" AND timestamp < \"{{ window.end }}\"",
                    "pageSize": 100,
                    "orderBy": "timestamp desc"
                })),
                "{}",
                endpoint.unit
            );
            let rows = profile.rows_of(endpoint);
            assert_eq!(rows.decoder, DecoderKind::JsonAt);
            assert_eq!(rows.at.as_deref(), Some("/entries"));
            let paginate = profile.paginate_of(endpoint);
            assert_eq!(paginate.strategy, PagerStrategy::Cursor);
            assert_eq!(paginate.from.as_deref(), Some("body:/nextPageToken"));
            assert_eq!(paginate.into.as_deref(), Some("body:/pageToken"));
            assert_eq!(profile.max_pages_of(endpoint), 10);
        }
        let scc = &profile.endpoints[8];
        assert_eq!(scc.unit, "scc");
        assert_eq!(scc.base_url.as_deref(), Some("{{ vars.scc_url }}"));
        assert_eq!(profile.method_of(scc), Method::Get);
        assert_eq!(profile.body_of(scc), None, "a GET inherits no body");
        assert_eq!(
            scc.path,
            "/v1/organizations/{{ vars.organization_id }}/sources/-/findings"
        );
        assert_eq!(scc.query.get("pageSize").map(String::as_str), Some("100"));
        assert_eq!(
            profile.rows_of(scc).at.as_deref(),
            Some("/listFindingsResults")
        );
        assert_eq!(
            profile.paginate_of(scc).into.as_deref(),
            Some("query:pageToken")
        );
    }

    /// The AWS profile is the contract the characterisation tests pinned:
    /// one SigV4 key pair signing each unit for its own service (Health for
    /// `us-east-1`), the JSON-1.x units as targeted POSTs to the service
    /// root and the REST-JSON ones on their documented paths, every list
    /// paged to the end on its body token, the window as typed epoch
    /// seconds (milliseconds for CloudWatch Logs), Config's quoted results,
    /// the CloudWatch list-then-get lookup with its builder, and STS as the
    /// probe.
    #[test]
    fn aws_ships_as_one_key_signing_each_unit_for_its_own_service() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{
            AuthKind, DecoderKind, Method, PagerStrategy, RowBuilderKind, WindowFormat,
        };

        let profile = shipped().get("aws").expect("aws is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::SigV4]);
        assert_eq!(profile.auth.sigv4.service, "{{ vars.service }}");
        assert_eq!(profile.auth.sigv4.region, "{{ vars.region }}");
        assert!(
            profile.base_url.contains("vars.endpoint_url")
                && profile.base_url.contains("vars.service")
                && profile.base_url.contains("vars.region"),
            "the override, else the service's own host: {}",
            profile.base_url
        );
        assert_eq!(profile.window.format, WindowFormat::EpochSecs);
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert!(profile.retry.retry_non_idempotent, "every POST is a read");
        assert!(profile.error.at.is_none(), "AWS spells `message` both ways");
        let probe = profile.probe.as_ref().expect("STS is the probe");
        assert_eq!(probe.path, "/");
        assert_eq!(
            probe.query.get("Action").map(String::as_str),
            Some("GetCallerIdentity")
        );
        assert_eq!(
            profile.vars.get("service"),
            Some(&serde_json::json!("sts")),
            "the instance-level scope is the probe's"
        );
        assert_eq!(
            profile.vars.get("region"),
            Some(&serde_json::json!("us-east-1"))
        );
        assert!(
            !profile.vars.contains_key("log_group_name")
                && !profile.vars.contains_key("access_key_id"),
            "identity and the required knob have no default"
        );

        let unit = |name: &str| {
            profile
                .endpoints
                .iter()
                .find(|e| e.unit == name)
                .unwrap_or_else(|| panic!("unit `{name}` is declared"))
        };
        let services: Vec<(&str, &str)> = profile
            .endpoints
            .iter()
            .map(|e| (e.unit.as_str(), e.vars["service"].as_str().unwrap()))
            .collect();
        assert_eq!(
            services,
            [
                ("cloudtrail", "cloudtrail"),
                ("guardduty", "guardduty"),
                ("securityhub", "securityhub"),
                ("config", "config"),
                ("cloudwatch_logs", "logs"),
                ("cloudwatch_metrics", "monitoring"),
                ("inspector", "inspector2"),
                ("health", "health"),
            ]
        );
        for endpoint in &profile.endpoints {
            assert_eq!(
                profile.method_of(endpoint),
                Method::Post,
                "{}",
                endpoint.unit
            );
            let paginate = profile.paginate_of(endpoint);
            assert_eq!(
                paginate.strategy,
                PagerStrategy::Cursor,
                "{}",
                endpoint.unit
            );
            assert!(
                paginate
                    .into
                    .as_deref()
                    .is_some_and(|i| i.starts_with("body:/")),
                "{}: the token goes back in the body",
                endpoint.unit
            );
        }
        for json_unit in [
            "cloudtrail",
            "config",
            "cloudwatch_logs",
            "cloudwatch_metrics",
            "health",
        ] {
            let e = unit(json_unit);
            assert_eq!(profile.path_of(e), "/", "{json_unit}: the service root");
            assert!(
                e.headers.contains_key("X-Amz-Target"),
                "{json_unit}: a JSON-1.x target"
            );
        }
        for rest_unit in ["guardduty", "securityhub", "inspector"] {
            let e = unit(rest_unit);
            assert!(
                !e.headers.contains_key("X-Amz-Target"),
                "{rest_unit}: REST-JSON has no target"
            );
            assert_eq!(
                e.headers.get("Content-Type").map(String::as_str),
                Some("application/json")
            );
        }

        let cloudtrail = unit("cloudtrail");
        assert_eq!(
            cloudtrail.headers.get("X-Amz-Target").map(String::as_str),
            Some("com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents")
        );
        assert_eq!(
            profile.body_of(cloudtrail),
            Some(&serde_json::json!({
                "StartTime": "{{ int(window.start) }}",
                "EndTime": "{{ int(window.end) }}",
                "MaxResults": 50
            }))
        );
        assert_eq!(
            profile.max_pages_of(cloudtrail),
            200,
            "CloudTrail raises its own ceiling over the profile's 50"
        );
        assert_eq!(
            profile.rate_of(cloudtrail).map(|r| r.requests_per_sec),
            Some(2.0),
            "LookupEvents allows 2 requests a second"
        );
        assert_eq!(
            profile
                .retry
                .throttle_when
                .as_ref()
                .map(|t| (t.status, t.body_contains.as_str())),
            Some((400, "ThrottlingException")),
            "an AWS throttle is a 400 the retry policy must recognise"
        );

        let guardduty = unit("guardduty");
        assert_eq!(profile.path_of(guardduty), "/detector/{{ key }}/findings");
        let keyset = guardduty
            .keyset()
            .expect("the detectors are a keyset request");
        assert_eq!(
            keyset.request.as_ref().map(|r| (r.method, r.path.as_str())),
            Some((Some(Method::Get), "/detector"))
        );
        assert_eq!(keyset.keys_at.as_deref(), Some("/detectorIds"));
        let lookup = guardduty.lookup().expect("the findings are a lookup");
        assert_eq!(lookup.batch, 50, "GetFindings takes 50 ids");
        assert_eq!(lookup.request.path, "/detector/{{ key }}/findings/get");
        assert_eq!(lookup.rows.at.as_deref(), Some("/findings"));

        let securityhub = unit("securityhub");
        assert_eq!(profile.path_of(securityhub), "/findings");
        assert_eq!(
            profile.body_of(securityhub),
            Some(&serde_json::json!({
                "Filters": {"WorkflowStatus": [{"Value": "NEW", "Comparison": "EQUALS"}]},
                "MaxResults": 100
            }))
        );

        let config = unit("config");
        assert_eq!(
            config.headers.get("X-Amz-Target").map(String::as_str),
            Some("StarlingDoveService.SelectResourceConfig")
        );
        let rows = profile.rows_of(config);
        assert_eq!(rows.decoder, DecoderKind::JsonAt);
        assert_eq!(rows.at.as_deref(), Some("/Results"));
        assert!(rows.quoted, "Results are JSON-encoded strings");
        assert!(
            profile.vars["expression"]
                .as_str()
                .is_some_and(|e| e.starts_with("SELECT ") && e.contains("configuration")),
            "the default expression returns every resource's configuration"
        );

        let logs = unit("cloudwatch_logs");
        assert_eq!(
            profile.window_of(logs).format,
            WindowFormat::EpochMillis,
            "FilterLogEvents takes milliseconds"
        );
        assert_eq!(
            profile.body_of(logs).and_then(|b| b.get("filterPattern")),
            Some(&serde_json::json!(
                "{{ vars.filter_pattern != '' ? vars.filter_pattern : null }}"
            )),
            "omitted unless set"
        );

        let metrics = unit("cloudwatch_metrics");
        assert_eq!(
            metrics.headers.get("Content-Type").map(String::as_str),
            Some("application/x-amz-json-1.0")
        );
        assert_eq!(
            profile.body_of(metrics),
            Some(&serde_json::json!("{{ key }}")),
            "the ListMetrics filter is the key"
        );
        assert_eq!(profile.max_pages_of(metrics), 10);
        let lookup = metrics.lookup().expect("GetMetricData is the lookup");
        assert_eq!(lookup.batch, 500);
        assert_eq!(lookup.rows.builder, Some(RowBuilderKind::CloudwatchMetrics));
        assert_eq!(lookup.rows.decoder, DecoderKind::Document);
        assert_eq!(
            lookup.paginate.as_ref().map(|p| p.strategy),
            Some(PagerStrategy::Cursor)
        );
        assert_eq!(lookup.max_pages, Some(10));

        let inspector = unit("inspector");
        assert_eq!(profile.path_of(inspector), "/findings/list");
        assert_eq!(
            profile.body_of(inspector).and_then(|b| b.get("maxResults")),
            Some(&serde_json::json!("{{ vars.max_results }}"))
        );

        let health = unit("health");
        assert_eq!(
            health.vars.get("region"),
            Some(&serde_json::json!("us-east-1")),
            "region-locked"
        );
        assert_eq!(
            health.headers.get("X-Amz-Target").map(String::as_str),
            Some("AWSHealth_20160804.DescribeEvents")
        );
    }

    /// The Google Workspace profile is the Reports activity contract the
    /// characterisation tests pinned: a delegated JWT-bearer exchange with
    /// the admin as `sub` and the audit-readonly scope, one unit per
    /// documented `applicationName` on one GET shape (customer, RFC 3339
    /// `+00:00` window, 1000 a page, an optional `eventName`), `nextPageToken`
    /// fed back in the query, items as rows, an hour of lookback, fifty
    /// pages a tick, the public endpoint as the default host.
    #[test]
    fn google_workspace_ships_as_one_unit_per_documented_application() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{
            AuthKind, DecoderKind, Method, PagerStrategy, WindowFormat,
        };

        let profile = shipped()
            .get("google_workspace")
            .expect("google_workspace is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::JwtBearer]);
        let jwt = &profile.auth.jwt_bearer;
        assert_eq!(
            jwt.claims.get("sub").map(String::as_str),
            Some("{{ vars.admin_email }}"),
            "domain-wide delegation impersonates the admin"
        );
        assert_eq!(
            jwt.claims.get("scope").map(String::as_str),
            Some("https://www.googleapis.com/auth/admin.reports.audit.readonly")
        );
        assert_eq!(jwt.ttl_secs, 3600);
        assert_eq!(
            profile.window.format,
            WindowFormat::Strftime("%Y-%m-%dT%H:%M:%S%.f+00:00".into())
        );
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert!(profile.probe.is_none(), "the token exchange is the probe");
        assert_eq!(
            profile.vars.get("api_url"),
            Some(&serde_json::Value::String(
                "https://admin.googleapis.com".into()
            ))
        );
        assert_eq!(
            profile.vars.get("customer_id"),
            Some(&serde_json::Value::String("my_customer".into()))
        );
        assert_eq!(
            profile.vars.get("event_name"),
            Some(&serde_json::Value::String(String::new())),
            "omitted unless a unit sets one"
        );
        assert!(!profile.vars.contains_key("admin_email"));
        for application in [
            "login", "admin", "drive", "mobile", "groups", "calendar", "chat", "meet", "token",
            "saml",
        ] {
            assert!(
                profile.endpoints.iter().any(|e| e.unit == application),
                "{application} is a documented applicationName"
            );
        }
        let mut units: Vec<&str> = profile.endpoints.iter().map(|e| e.unit.as_str()).collect();
        let count = units.len();
        units.dedup();
        assert_eq!(units.len(), count, "no unit twice");
        for endpoint in &profile.endpoints {
            assert!(
                endpoint.path.is_empty(),
                "{}: the default path",
                endpoint.unit
            );
            assert_eq!(
                profile.path_of(endpoint),
                "/admin/reports/v1/activity/users/all/applications/{{ unit.name }}"
            );
            assert_eq!(profile.method_of(endpoint), Method::Get);
            let rows = profile.rows_of(endpoint);
            assert_eq!(rows.decoder, DecoderKind::JsonAt);
            assert_eq!(rows.at.as_deref(), Some("/items"));
            let paginate = profile.paginate_of(endpoint);
            assert_eq!(paginate.strategy, PagerStrategy::Cursor);
            assert_eq!(paginate.from.as_deref(), Some("body:/nextPageToken"));
            assert_eq!(paginate.into.as_deref(), Some("query:pageToken"));
            assert_eq!(profile.max_pages_of(endpoint), 50);
        }
        let query: Vec<(&str, &str)> = profile
            .defaults
            .query
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            query,
            [
                ("customerId", "{{ vars.customer_id }}"),
                ("endTime", "{{ window.end }}"),
                ("eventName", "{{ vars.event_name }}"),
                ("maxResults", "1000"),
                ("startTime", "{{ window.start }}"),
            ]
        );
    }

    /// The Duo profile is the v2 authentication-log contract the
    /// characterisation tests pinned: Duo's request signing as the one
    /// accepted mode, an epoch-millisecond window as `mintime`/`maxtime`,
    /// `limit` 100 default, rows under `/response/authlogs`, the
    /// `next_offset` list fed back under the same name, `stat != OK` as the
    /// failure inside a 200 with `message` as the text, the free credential
    /// check as the probe, and no default base URL because every tenant has
    /// its own hostname.
    #[test]
    fn duo_ships_as_the_signed_authentication_log_event_window() {
        use dfe_fetcher_core::UnitShape;
        use dfe_fetcher_rest::profile::{AuthKind, DecoderKind, PagerStrategy, WindowFormat};

        let profile = shipped().get("duo").expect("duo is shipped");
        assert_eq!(profile.shape, UnitShape::Incremental);
        assert_eq!(profile.auth.accepts, [AuthKind::DuoHmac]);
        assert_eq!(profile.window.format, WindowFormat::EpochMillis);
        assert_eq!(profile.window.lookback.0.as_secs(), 3600);
        assert_eq!(profile.error.at.as_deref(), Some("/message"));
        let probe = profile.probe.as_ref().expect("a probe");
        assert_eq!(probe.path, "/admin/v1/check");
        assert_eq!(probe.fail_when.as_deref(), Some("body.stat != 'OK'"));
        assert!(!profile.vars.contains_key("base_url"));
        assert_eq!(profile.vars.get("limit"), Some(&serde_json::json!(100)));
        let endpoint = only_endpoint(profile);
        assert_eq!(endpoint.unit, "authentication_logs");
        assert_eq!(endpoint.path, "/admin/v2/logs/authentication");
        assert_eq!(
            query_of(endpoint),
            [
                ("limit", "{{ vars.limit }}"),
                ("maxtime", "{{ window.end }}"),
                ("mintime", "{{ window.start }}"),
            ]
        );
        let rows = profile.rows_of(endpoint);
        assert_eq!(rows.decoder, DecoderKind::JsonAt);
        assert_eq!(rows.at.as_deref(), Some("/response/authlogs"));
        let paginate = profile.paginate_of(endpoint);
        assert_eq!(paginate.strategy, PagerStrategy::Cursor);
        assert_eq!(
            paginate.from.as_deref(),
            Some("body:/response/metadata/next_offset")
        );
        assert_eq!(paginate.into.as_deref(), Some("query:next_offset"));
        assert_eq!(endpoint.fail_when.as_deref(), Some("body.stat != 'OK'"));
        assert_eq!(profile.max_pages_of(endpoint), 50);
    }
}
