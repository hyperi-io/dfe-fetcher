// Project:   dfe-fetcher
// File:      crates/fetcher/src/config/builtin.rs
// Purpose:   Typed source blocks that are shipped REST profiles underneath, mapped onto instances, and their registry
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Built-in profile sources.
//!
//! The typed SaaS blocks (`sources.github`, `sources.okta`, `sources.slack`,
//! ...) keep their structs and their `resolved()` connections -- the
//! operator's config surface is unchanged -- and each resolved connection
//! becomes one instance of the shipped profile of the same name: the
//! connection id, topic, filter and interval carry over, the credential
//! becomes the instance's auth mode, and the block's remaining fields become
//! the `vars` the profile's templates read. Nothing here fetches; the driver
//! runs the bound profile like any `sources.rest` instance.
//!
//! [`REGISTRY`] is the one table of the typed blocks: the filter and enabled
//! tables, the instance expansion, the connection ids `validate` checks and
//! the capability catalog all derive from it.

use std::collections::{BTreeMap, HashMap};

use dfe_fetcher_rest::profile::{AuthKind, InstanceAuth, ProfileRef, RestInstance, UnitOverride};
use scalo::config::sensitive::SensitiveString;
use scalo::deployment::Capability;
use serde_json::Value;

use super::{
    AwsSourceConfig, AzureSourceConfig, BitwardenSourceConfig, CloudflareSourceConfig,
    ConnectionOverlay, CratesIoSourceConfig, CrowdstrikeSourceConfig, DuoSourceConfig,
    GcpPubsubSourceConfig, GcpSourceConfig, GithubSourceConfig, GoModulesSourceConfig,
    GoogleWorkspaceSourceConfig, M365SourceConfig, ObjectStoreBackendConfig, ObjectStoreFormat,
    ObjectStoreSourceConfig, OktaSourceConfig, OnePasswordSourceConfig, PypiSourceConfig, Resolved,
    SalesforceSourceConfig, SlackSourceConfig, SourcesConfig,
};
use crate::deployment_catalog as catalog;
use crate::error::{Error, Result};

/// The ad-hoc `config` map of one listed service, read by knob name.
struct Knobs<'a>(Option<&'a HashMap<String, Value>>);

impl<'a> Knobs<'a> {
    /// The knobs of the service called `name` among `services`, each given
    /// as `(name, config)`.
    fn of(
        services: impl IntoIterator<Item = (&'a str, &'a HashMap<String, Value>)>,
        name: &str,
    ) -> Self {
        Self(
            services
                .into_iter()
                .find(|(n, _)| *n == name)
                .map(|(_, config)| config),
        )
    }

    /// An integer knob, capped at `cap`; a non-integer value is ignored.
    fn int(&self, key: &str, cap: u64) -> Option<u64> {
        self.0?.get(key)?.as_u64().map(|n| n.min(cap))
    }

    /// A string knob.
    fn text(&self, key: &str) -> Option<&'a str> {
        self.0?.get(key)?.as_str()
    }

    /// A list-of-strings knob; non-string elements are ignored.
    fn texts(&self, key: &str) -> Vec<&'a str> {
        self.0
            .and_then(|c| c.get(key))
            .and_then(Value::as_array)
            .map(|items| items.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default()
    }
}

/// One connection of a built-in block as the profile instance the driver runs.
#[derive(Debug, Clone)]
pub struct BuiltinInstance {
    /// The connection id: cursor key, metric label, `_source_fetcher` prefix.
    pub connection_id: String,
    /// The instance the shipped profile of the same name binds to.
    pub instance: RestInstance,
}

/// The parts of an instance every built-in block fills the same way.
struct Common<'a> {
    profile: &'static str,
    topic: &'a str,
    filter: Option<&'a str>,
    interval_secs: Option<u64>,
    /// The service names the block lists; a profile unit not among them is
    /// switched off, a name the profile lacks is left for validation to
    /// refuse.
    services: Vec<&'a str>,
}

fn instance(common: Common<'_>, auth: InstanceAuth, vars: BTreeMap<String, Value>) -> RestInstance {
    let mut units = BTreeMap::new();
    let shipped = crate::profiles::shipped()
        .get(common.profile)
        .unwrap_or_else(|| panic!("`{}` is a shipped profile", common.profile));
    for endpoint in &shipped.endpoints {
        units.insert(
            endpoint.unit.clone(),
            UnitOverride {
                enabled: common.services.contains(&endpoint.unit.as_str()),
                ..UnitOverride::default()
            },
        );
    }
    for service in common.services {
        units
            .entry(service.to_owned())
            .or_insert_with(UnitOverride::default);
    }
    RestInstance {
        enabled: true,
        profile: ProfileRef::Named(common.profile.to_owned()),
        interval_secs: common.interval_secs,
        topic: common.topic.to_owned(),
        filter: common.filter.map(str::to_owned),
        auth,
        vars,
        units,
        accumulate: None,
    }
}

/// The credential spec a block's connection uses: the secret reference when
/// set, else the literal `field` (`token`, `client_secret`, `secret_key`).
fn credential(
    block: &str,
    field: &str,
    credential_secret: Option<&str>,
    literal: Option<&SensitiveString>,
) -> Result<SensitiveString> {
    credential_secret
        .map(SensitiveString::from)
        .or_else(|| literal.cloned())
        .ok_or_else(|| {
            Error::Config(format!(
                "sources.{block}: `{field}` or `credential_secret` is required"
            ))
        })
}

/// A required, non-empty identity field of a block (an account id, a client
/// id, an API host).
fn required<'a>(block: &str, field: &str, value: Option<&'a str>) -> Result<&'a str> {
    value
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Config(format!("sources.{block}: `{field}` is required")))
}

/// The OAuth2 client-credentials identity of a block's connection.
fn oauth2_identity(
    block: &str,
    client_id: Option<&str>,
    credential_secret: Option<&str>,
    client_secret: Option<&SensitiveString>,
) -> Result<InstanceAuth> {
    Ok(InstanceAuth {
        mode: AuthKind::Oauth2ClientCredentials,
        client_id: Some(required(block, "client_id", client_id)?.to_owned()),
        client_secret: Some(credential(
            block,
            "client_secret",
            credential_secret,
            client_secret,
        )?),
        ..InstanceAuth::default()
    })
}

/// The JWT-bearer identity a block's `service_account_key` names: the key JSON
/// itself when the value opens with `{`, else the path of the key file.
fn service_account_identity(key: &SensitiveString) -> InstanceAuth {
    let mut auth = InstanceAuth {
        mode: AuthKind::JwtBearer,
        ..InstanceAuth::default()
    };
    // A key JSON is an object and no file path opens with a brace.
    if key.expose().trim_start().starts_with('{') {
        auth.service_account_key = Some(key.clone());
    } else {
        auth.service_account_key_file = Some(key.clone());
    }
    auth
}

impl GithubSourceConfig {
    /// One instance of the shipped `github` profile per resolved connection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection sets both or neither of
    /// `org` and `enterprise`, or carries no token.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("github")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let scope_path = match (
                        config.org.as_deref().filter(|s| !s.is_empty()),
                        config.enterprise.as_deref().filter(|s| !s.is_empty()),
                    ) {
                        (Some(org), None) => format!("orgs/{org}/audit-log"),
                        (None, Some(enterprise)) => format!("enterprises/{enterprise}/audit-log"),
                        (Some(_), Some(_)) => {
                            return Err(Error::Config(
                            "sources.github: set exactly one of `org` or `enterprise`, not both"
                                .into(),
                        ));
                        }
                        (None, None) => {
                            return Err(Error::Config(
                                "sources.github: `org` or `enterprise` is required".into(),
                            ));
                        }
                    };
                    let token = credential(
                        "github",
                        "token",
                        config.credential_secret.as_deref(),
                        config.token.as_ref(),
                    )?;
                    let include = Knobs::of(
                        config.services.iter().map(|s| (s.name.as_str(), &s.config)),
                        "audit_log",
                    )
                    .text("include")
                    .filter(|v| matches!(*v, "all" | "web" | "git"))
                    .unwrap_or("all");
                    let mut vars = BTreeMap::new();
                    vars.insert("scope_path".into(), Value::String(scope_path));
                    vars.insert("include".into(), Value::String(include.into()));
                    if let Some(api_url) = &config.api_url_override {
                        vars.insert("api_url".into(), Value::String(api_url.clone()));
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance: instance(
                            Common {
                                profile: "github",
                                topic: &config.topic,
                                filter: config.filter.as_deref(),
                                interval_secs,
                                services: config.services.iter().map(|s| s.name.as_str()).collect(),
                            },
                            InstanceAuth {
                                mode: AuthKind::Bearer,
                                token: Some(token),
                                ..InstanceAuth::default()
                            },
                            vars,
                        ),
                    })
                },
            )
            .collect()
    }
}

/// Okta caps a page at this many events; a larger `limit` is sent as the cap.
const OKTA_MAX_LIMIT: u64 = 1000;

impl OktaSourceConfig {
    /// One instance of the shipped `okta` profile per resolved connection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection has neither a tenant URL
    /// nor an override, or carries no token.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("okta")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let base_url = config
                        .api_url_override
                        .as_deref()
                        .or(config.tenant_url.as_deref())
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| {
                            Error::Config("sources.okta: `tenant_url` is required".into())
                        })?
                        .to_owned();
                    let token = credential(
                        "okta",
                        "token",
                        config.credential_secret.as_deref(),
                        config.token.as_ref(),
                    )?;
                    let auth = if config.use_ssws_header {
                        InstanceAuth {
                            mode: AuthKind::ApiKey,
                            key: Some(token),
                            ..InstanceAuth::default()
                        }
                    } else {
                        InstanceAuth {
                            mode: AuthKind::Bearer,
                            token: Some(token),
                            ..InstanceAuth::default()
                        }
                    };
                    let knobs = Knobs::of(
                        config.services.iter().map(|s| (s.name.as_str(), &s.config)),
                        "system_log",
                    );
                    let mut vars = BTreeMap::new();
                    vars.insert("base_url".into(), Value::String(base_url));
                    if let Some(limit) = knobs.int("limit", OKTA_MAX_LIMIT) {
                        vars.insert("limit".into(), Value::from(limit));
                    }
                    if let Some(filter) = knobs.text("filter") {
                        vars.insert("filter".into(), Value::String(filter.to_owned()));
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance: instance(
                            Common {
                                profile: "okta",
                                topic: &config.topic,
                                filter: config.filter.as_deref(),
                                interval_secs,
                                services: config.services.iter().map(|s| s.name.as_str()).collect(),
                            },
                            auth,
                            vars,
                        ),
                    })
                },
            )
            .collect()
    }
}

/// Slack caps a page at this many entries; a larger `limit` is sent as the cap.
const SLACK_MAX_LIMIT: u64 = 1000;

impl SlackSourceConfig {
    /// One instance of the shipped `slack` profile per resolved connection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection carries no token.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("slack")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let token = credential(
                        "slack",
                        "token",
                        config.credential_secret.as_deref(),
                        config.token.as_ref(),
                    )?;
                    let knobs = Knobs::of(
                        config.services.iter().map(|s| (s.name.as_str(), &s.config)),
                        "audit_logs",
                    );
                    let mut vars = BTreeMap::new();
                    if let Some(api_url) = &config.api_url_override {
                        vars.insert("api_url".into(), Value::String(api_url.clone()));
                    }
                    if let Some(limit) = knobs.int("limit", SLACK_MAX_LIMIT) {
                        vars.insert("limit".into(), Value::from(limit));
                    }
                    for knob in ["action", "entity"] {
                        if let Some(value) = knobs.text(knob) {
                            vars.insert(knob.into(), Value::String(value.to_owned()));
                        }
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance: instance(
                            Common {
                                profile: "slack",
                                topic: &config.topic,
                                filter: config.filter.as_deref(),
                                interval_secs,
                                services: config.services.iter().map(|s| s.name.as_str()).collect(),
                            },
                            InstanceAuth {
                                mode: AuthKind::Bearer,
                                token: Some(token),
                                ..InstanceAuth::default()
                            },
                            vars,
                        ),
                    })
                },
            )
            .collect()
    }
}

/// Cloudflare caps a page at this many entries; a larger `per_page` is sent as
/// the cap.
const CLOUDFLARE_MAX_PER_PAGE: u64 = 1000;

impl CloudflareSourceConfig {
    /// One instance of the shipped `cloudflare` profile per resolved
    /// connection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection has no account id or no
    /// token.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("cloudflare")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let account_id =
                        required("cloudflare", "account_id", config.account_id.as_deref())?;
                    let token = credential(
                        "cloudflare",
                        "token",
                        config.credential_secret.as_deref(),
                        config.token.as_ref(),
                    )?;
                    let knobs = Knobs::of(
                        config.services.iter().map(|s| (s.name.as_str(), &s.config)),
                        "audit_logs",
                    );
                    let mut vars = BTreeMap::new();
                    vars.insert("account_id".into(), Value::String(account_id.to_owned()));
                    if let Some(api_url) = &config.api_url_override {
                        vars.insert("api_url".into(), Value::String(api_url.clone()));
                    }
                    if let Some(per_page) = knobs.int("per_page", CLOUDFLARE_MAX_PER_PAGE) {
                        vars.insert("per_page".into(), Value::from(per_page));
                    }
                    for knob in ["actor_email", "action_type"] {
                        if let Some(value) = knobs.text(knob) {
                            vars.insert(knob.into(), Value::String(value.to_owned()));
                        }
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance: instance(
                            Common {
                                profile: "cloudflare",
                                topic: &config.topic,
                                filter: config.filter.as_deref(),
                                interval_secs,
                                services: config.services.iter().map(|s| s.name.as_str()).collect(),
                            },
                            InstanceAuth {
                                mode: AuthKind::Bearer,
                                token: Some(token),
                                ..InstanceAuth::default()
                            },
                            vars,
                        ),
                    })
                },
            )
            .collect()
    }
}

impl BitwardenSourceConfig {
    /// One instance of the shipped `bitwarden` profile per resolved
    /// connection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection has no client id or no
    /// client secret.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("bitwarden")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let auth = oauth2_identity(
                        "bitwarden",
                        config.client_id.as_deref(),
                        config.credential_secret.as_deref(),
                        config.client_secret.as_ref(),
                    )?;
                    let mut vars = BTreeMap::new();
                    if let Some(api_url) = &config.api_url_override {
                        vars.insert("api_url".into(), Value::String(api_url.clone()));
                    }
                    if let Some(identity_url) = &config.identity_url_override {
                        vars.insert("identity_url".into(), Value::String(identity_url.clone()));
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance: instance(
                            Common {
                                profile: "bitwarden",
                                topic: &config.topic,
                                filter: config.filter.as_deref(),
                                interval_secs,
                                services: config.services.iter().map(|s| s.name.as_str()).collect(),
                            },
                            auth,
                            vars,
                        ),
                    })
                },
            )
            .collect()
    }
}

/// 1Password caps a page at this many items; a larger `limit` is sent as the
/// cap.
const ONEPASSWORD_MAX_LIMIT: u64 = 1000;

impl OnePasswordSourceConfig {
    /// One instance of the shipped `onepassword` profile per resolved
    /// connection; each listed service's `limit` becomes that unit's var.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection carries no token.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("onepassword")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let token = credential(
                        "onepassword",
                        "token",
                        config.credential_secret.as_deref(),
                        config.token.as_ref(),
                    )?;
                    let mut vars = BTreeMap::new();
                    if let Some(api_url) = &config.api_url_override {
                        vars.insert("api_url".into(), Value::String(api_url.clone()));
                    }
                    let mut instance = instance(
                        Common {
                            profile: "onepassword",
                            topic: &config.topic,
                            filter: config.filter.as_deref(),
                            interval_secs,
                            services: config.services.iter().map(|s| s.name.as_str()).collect(),
                        },
                        InstanceAuth {
                            mode: AuthKind::Bearer,
                            token: Some(token),
                            ..InstanceAuth::default()
                        },
                        vars,
                    );
                    for service in &config.services {
                        let limit = Knobs::of(
                            std::iter::once((service.name.as_str(), &service.config)),
                            &service.name,
                        )
                        .int("limit", ONEPASSWORD_MAX_LIMIT);
                        if let (Some(limit), Some(unit)) =
                            (limit, instance.units.get_mut(&service.name))
                        {
                            unit.vars.insert("limit".into(), Value::from(limit));
                        }
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance,
                    })
                },
            )
            .collect()
    }
}

/// A public per-key registry block (PyPI, crates.io): no credential, the
/// keys to watch as a var, the one `metadata` unit. Single-connection, so the
/// connection id is the type name.
fn registry_instance(
    profile: &'static str,
    keys_var: &str,
    keys: &[String],
    api_url_override: Option<&str>,
    topic: &str,
    filter: Option<&str>,
    interval_secs: Option<u64>,
) -> BuiltinInstance {
    let mut vars = BTreeMap::new();
    vars.insert(
        keys_var.to_owned(),
        Value::Array(keys.iter().cloned().map(Value::String).collect()),
    );
    if let Some(api_url) = api_url_override {
        vars.insert("api_url".into(), Value::String(api_url.to_owned()));
    }
    BuiltinInstance {
        connection_id: profile.to_owned(),
        instance: instance(
            Common {
                profile,
                topic,
                filter,
                interval_secs,
                services: vec!["metadata"],
            },
            InstanceAuth::default(),
            vars,
        ),
    }
}

impl PypiSourceConfig {
    /// The one instance of the shipped `pypi` profile the block maps onto.
    #[must_use]
    pub fn instances(&self) -> Vec<BuiltinInstance> {
        vec![registry_instance(
            "pypi",
            "packages",
            &self.packages,
            self.api_url_override.as_deref(),
            &self.topic,
            self.filter.as_deref(),
            self.interval_secs,
        )]
    }
}

impl CratesIoSourceConfig {
    /// The one instance of the shipped `crates_io` profile the block maps
    /// onto.
    #[must_use]
    pub fn instances(&self) -> Vec<BuiltinInstance> {
        vec![registry_instance(
            "crates_io",
            "crates",
            &self.crates,
            self.api_url_override.as_deref(),
            &self.topic,
            self.filter.as_deref(),
            self.interval_secs,
        )]
    }
}

/// The `object_store` profile's endpoint for each object format.
fn object_store_unit(format: ObjectStoreFormat) -> &'static str {
    match format {
        ObjectStoreFormat::JsonGz => "json_gz",
        ObjectStoreFormat::Jsonl => "jsonl",
        ObjectStoreFormat::Json => "json",
        ObjectStoreFormat::Text => "text",
        ObjectStoreFormat::TextGz => "text_gz",
    }
}

impl ObjectStoreSourceConfig {
    /// The one instance of the shipped `object_store` profile the block's
    /// S3 backend maps onto: `credential_secret` is the credentials
    /// document, else the key pair; the region and the endpoint override
    /// are vars; every prefix of every bucket is a unit of its own,
    /// instantiated from its format's endpoint under its `source_tag`, with
    /// the bucket and prefix as its vars and its own topic when it sets
    /// one. A GCS or Azure Blob backend is not implemented and is skipped
    /// with a warning, as before; a block with no S3 backend maps onto no
    /// instance.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the block lists more than one S3
    /// backend (one key pair and region per instance), a backend has no
    /// credential, or two prefixes share a `source_tag`.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        let mut s3 = Vec::new();
        for backend in &self.backends {
            match backend {
                ObjectStoreBackendConfig::S3(config) => s3.push(config),
                ObjectStoreBackendConfig::Gcs(_) => tracing::warn!(
                    "sources.object_store: the GCS backend is not implemented and is skipped; use the s3 backend, or an S3-compatible endpoint"
                ),
                ObjectStoreBackendConfig::AzureBlob(_) => tracing::warn!(
                    "sources.object_store: the Azure Blob backend is not implemented and is skipped; use the s3 backend, or an S3-compatible endpoint"
                ),
            }
        }
        let [backend] = s3.as_slice() else {
            if s3.is_empty() {
                return Ok(Vec::new());
            }
            return Err(Error::Config(
                "sources.object_store: one S3 backend per block (one key pair and region); \
                 run a second bucket account as a `sources.rest` instance of the `object_store` profile"
                    .into(),
            ));
        };
        let auth = match &backend.credential_secret {
            Some(spec) => InstanceAuth {
                mode: AuthKind::SigV4,
                credentials_json: Some(SensitiveString::from(spec.as_str())),
                ..InstanceAuth::default()
            },
            None => InstanceAuth {
                mode: AuthKind::SigV4,
                access_key_id: Some(SensitiveString::from(required(
                    "object_store",
                    "access_key_id",
                    backend.access_key_id.as_deref(),
                )?)),
                secret_access_key: Some(backend.secret_access_key.clone().ok_or_else(|| {
                    Error::Config(
                        "sources.object_store: `secret_access_key` or `credential_secret` is required"
                            .into(),
                    )
                })?),
                ..InstanceAuth::default()
            },
        };
        let mut vars = BTreeMap::new();
        vars.insert("region".into(), Value::String(backend.region.clone()));
        if let Some(url) = backend
            .endpoint_override
            .as_deref()
            .filter(|u| !u.is_empty())
        {
            vars.insert(
                "endpoint_url".into(),
                Value::String(url.trim_end_matches('/').to_owned()),
            );
        }
        let formats: Vec<&str> = crate::profiles::shipped()["object_store"]
            .endpoints
            .iter()
            .map(|e| e.unit.as_str())
            .collect();
        let mut units: BTreeMap<String, UnitOverride> = formats
            .iter()
            .map(|format| {
                (
                    (*format).to_owned(),
                    UnitOverride {
                        enabled: false,
                        ..UnitOverride::default()
                    },
                )
            })
            .collect();
        for bucket in &backend.buckets {
            for prefix in &bucket.prefixes {
                if formats.contains(&prefix.source_tag.as_str()) {
                    return Err(Error::Config(format!(
                        "sources.object_store: `source_tag` `{}` is the name of a format; pick another tag",
                        prefix.source_tag
                    )));
                }
                let mut unit_vars = BTreeMap::new();
                unit_vars.insert("bucket".into(), Value::String(bucket.bucket.clone()));
                unit_vars.insert("prefix".into(), Value::String(prefix.prefix.clone()));
                let over = UnitOverride {
                    endpoint: Some(object_store_unit(prefix.format).to_owned()),
                    topic: prefix.topic.clone(),
                    vars: unit_vars,
                    ..UnitOverride::default()
                };
                if units.insert(prefix.source_tag.clone(), over).is_some() {
                    return Err(Error::Config(format!(
                        "sources.object_store: `source_tag` `{}` is used by two prefixes; each prefix is a unit and needs its own",
                        prefix.source_tag
                    )));
                }
            }
        }
        Ok(vec![BuiltinInstance {
            connection_id: "object_store".to_owned(),
            instance: RestInstance {
                enabled: true,
                profile: ProfileRef::Named("object_store".to_owned()),
                interval_secs: self.interval_secs,
                topic: self.topic.clone(),
                filter: self.filter.clone(),
                auth,
                vars,
                units,
                accumulate: None,
            },
        }])
    }
}

/// A value inside a single-quoted SOQL string literal.
fn soql_quoted(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}

impl SalesforceSourceConfig {
    /// One instance of the shipped `salesforce` profile per resolved
    /// connection: a private key (`private_key_secret` first) makes it a
    /// JWT-bearer instance with the consumer key and username as the
    /// claims' vars, else a consumer secret (`credential_secret` first)
    /// makes it a client-credentials instance; the login URL, API version
    /// and pinned instance URL become vars, and the `event_log_file`
    /// service's `interval` and `event_types` its unit's vars, the types
    /// already quoted for SOQL.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection has no `client_id`, no
    /// private key or secret, or a private key without a `username`.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("salesforce")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let client_id =
                        required("salesforce", "client_id", config.client_id.as_deref())?
                            .to_owned();
                    let private_key = config
                        .private_key_secret
                        .as_deref()
                        .or_else(|| config.private_key.as_ref().map(SensitiveString::expose))
                        .filter(|k| !k.is_empty());
                    let mut vars = BTreeMap::new();
                    let auth = match private_key {
                        Some(key) => {
                            vars.insert("client_id".into(), Value::String(client_id.clone()));
                            vars.insert(
                                "username".into(),
                                Value::String(
                                    required("salesforce", "username", config.username.as_deref())?
                                        .to_owned(),
                                ),
                            );
                            InstanceAuth {
                                mode: AuthKind::JwtBearer,
                                private_key: Some(SensitiveString::from(key)),
                                ..InstanceAuth::default()
                            }
                        }
                        None => InstanceAuth {
                            mode: AuthKind::Oauth2ClientCredentials,
                            client_id: Some(client_id),
                            client_secret: Some(credential(
                                "salesforce",
                                "client_secret (or private_key)",
                                config.credential_secret.as_deref(),
                                config.client_secret.as_ref(),
                            )?),
                            ..InstanceAuth::default()
                        },
                    };
                    for (var, value) in [
                        ("login_url", &config.login_url),
                        ("api_version", &config.api_version),
                        ("instance_url", &config.instance_url_override),
                    ] {
                        if let Some(value) = value.as_deref().filter(|v| !v.is_empty()) {
                            vars.insert(
                                var.into(),
                                Value::String(value.trim_end_matches('/').to_owned()),
                            );
                        }
                    }
                    let mut instance = instance(
                        Common {
                            profile: "salesforce",
                            topic: &config.topic,
                            filter: config.filter.as_deref(),
                            interval_secs,
                            services: config.services.iter().map(|s| s.name.as_str()).collect(),
                        },
                        auth,
                        vars,
                    );
                    let knobs = Knobs::of(
                        config.services.iter().map(|s| (s.name.as_str(), &s.config)),
                        "event_log_file",
                    );
                    if let Some(unit) = instance.units.get_mut("event_log_file") {
                        if let Some(interval) = knobs.text("interval") {
                            unit.vars.insert(
                                "interval".into(),
                                Value::String(interval.replace('\\', "\\\\").replace('\'', "\\'")),
                            );
                        }
                        let types = knobs.texts("event_types");
                        if !types.is_empty() {
                            unit.vars.insert(
                                "event_types_soql".into(),
                                Value::String(
                                    types
                                        .iter()
                                        .map(|t| soql_quoted(t))
                                        .collect::<Vec<_>>()
                                        .join(","),
                                ),
                            );
                        }
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance,
                    })
                },
            )
            .collect()
    }
}

impl GcpPubsubSourceConfig {
    /// The one instance of the shipped `gcp_pubsub` profile the block maps
    /// onto: `credential_secret` is the service-account key JSON, else
    /// `service_account_key` is the key or its file, else the workload's
    /// token comes from the GCE metadata server; the API and token overrides are vars;
    /// every subscription is a unit of its own, instantiated from `pull`
    /// under its subscription id with the project, id and pull knobs as
    /// its vars.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when two subscriptions share an id.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        let auth = match (&self.credential_secret, &self.service_account_key) {
            (Some(key_json), _) => InstanceAuth {
                mode: AuthKind::JwtBearer,
                service_account_key: Some(SensitiveString::from(key_json.as_str())),
                ..InstanceAuth::default()
            },
            (None, Some(key)) => service_account_identity(key),
            (None, None) => InstanceAuth {
                mode: AuthKind::GceMetadata,
                ..InstanceAuth::default()
            },
        };
        let mut vars = BTreeMap::new();
        for (var, value) in [
            ("api_url", &self.api_url_override),
            ("token_url", &self.token_url_override),
        ] {
            if let Some(value) = value.as_deref().filter(|v| !v.is_empty()) {
                vars.insert(var.into(), Value::String(value.to_owned()));
            }
        }
        let mut units = BTreeMap::new();
        units.insert(
            "pull".to_owned(),
            UnitOverride {
                enabled: false,
                ..UnitOverride::default()
            },
        );
        for sub in &self.subscriptions {
            if sub.subscription_id == "pull" {
                return Err(Error::Config(
                    "sources.gcp_pubsub: a subscription may not be called `pull`, the profile's own unit"
                        .into(),
                ));
            }
            let mut unit_vars = BTreeMap::new();
            unit_vars.insert("project_id".into(), Value::String(sub.project_id.clone()));
            unit_vars.insert(
                "subscription_id".into(),
                Value::String(sub.subscription_id.clone()),
            );
            unit_vars.insert("max_messages".into(), Value::from(sub.max_messages));
            unit_vars.insert(
                "return_immediately".into(),
                Value::Bool(sub.return_immediately),
            );
            let over = UnitOverride {
                endpoint: Some("pull".to_owned()),
                vars: unit_vars,
                ..UnitOverride::default()
            };
            if units.insert(sub.subscription_id.clone(), over).is_some() {
                return Err(Error::Config(format!(
                    "sources.gcp_pubsub: subscription `{}` is listed twice; each subscription is a unit and needs its own id",
                    sub.subscription_id
                )));
            }
        }
        Ok(vec![BuiltinInstance {
            connection_id: "gcp_pubsub".to_owned(),
            instance: RestInstance {
                enabled: true,
                profile: ProfileRef::Named("gcp_pubsub".to_owned()),
                interval_secs: self.interval_secs,
                topic: self.topic.clone(),
                filter: self.filter.clone(),
                auth,
                vars,
                units,
                accumulate: None,
            },
        }])
    }
}

impl GoModulesSourceConfig {
    /// The one instance of the shipped `go_modules` profile the block maps
    /// onto: the module paths as the keyset, the proxy override as the
    /// base URL.
    #[must_use]
    pub fn instances(&self) -> Vec<BuiltinInstance> {
        vec![registry_instance(
            "go_modules",
            "modules",
            &self.modules,
            self.api_url_override.as_deref(),
            &self.topic,
            self.filter.as_deref(),
            self.interval_secs,
        )]
    }
}

/// Falcon caps an id query page at this many ids; a larger `limit` is sent as
/// the cap.
const CROWDSTRIKE_MAX_LIMIT: u64 = 1000;

impl CrowdstrikeSourceConfig {
    /// One instance of the shipped `crowdstrike` profile per resolved
    /// connection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection has no client id or no
    /// client secret.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("crowdstrike")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let auth = oauth2_identity(
                        "crowdstrike",
                        config.client_id.as_deref(),
                        config.credential_secret.as_deref(),
                        config.client_secret.as_ref(),
                    )?;
                    let knobs = Knobs::of(
                        config.services.iter().map(|s| (s.name.as_str(), &s.config)),
                        "alerts",
                    );
                    let mut vars = BTreeMap::new();
                    if let Some(api_url) = &config.api_url_override {
                        vars.insert("api_url".into(), Value::String(api_url.clone()));
                    }
                    if let Some(limit) = knobs.int("limit", CROWDSTRIKE_MAX_LIMIT) {
                        vars.insert("limit".into(), Value::from(limit));
                    }
                    if let Some(filter) = knobs.text("filter") {
                        vars.insert("filter".into(), Value::String(filter.to_owned()));
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance: instance(
                            Common {
                                profile: "crowdstrike",
                                topic: &config.topic,
                                filter: config.filter.as_deref(),
                                interval_secs,
                                services: config.services.iter().map(|s| s.name.as_str()).collect(),
                            },
                            auth,
                            vars,
                        ),
                    })
                },
            )
            .collect()
    }
}

/// Duo caps a page at this many events; a larger `limit` is sent as the cap.
const DUO_MAX_LIMIT: u64 = 1000;

impl DuoSourceConfig {
    /// One instance of the shipped `duo` profile per resolved connection.
    ///
    /// The connection carries its own signing version, so one deployment can
    /// poll a tenant on Duo's current scheme beside one still verifying the
    /// legacy scheme.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection has no API host (and no
    /// URL override), no integration key or no secret key.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("duo")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let base_url = match config.api_url_override.as_deref() {
                        Some(url) => url.trim_end_matches('/').to_owned(),
                        None => format!(
                            "https://{}",
                            required("duo", "api_host", config.api_host.as_deref())?
                        ),
                    };
                    let auth = InstanceAuth {
                        mode: AuthKind::Signature,
                        key_id: Some(
                            required("duo", "integration_key", config.integration_key.as_deref())?
                                .to_owned(),
                        ),
                        secret_key: Some(credential(
                            "duo",
                            "secret_key",
                            config.credential_secret.as_deref(),
                            config.secret_key.as_ref(),
                        )?),
                        signature_preset: Some(config.signature_version.preset()),
                        ..InstanceAuth::default()
                    };
                    let knobs = Knobs::of(
                        config.services.iter().map(|s| (s.name.as_str(), &s.config)),
                        "authentication_logs",
                    );
                    let mut vars = BTreeMap::new();
                    vars.insert("base_url".into(), Value::String(base_url));
                    if let Some(limit) = knobs.int("limit", DUO_MAX_LIMIT) {
                        vars.insert("limit".into(), Value::from(limit));
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance: instance(
                            Common {
                                profile: "duo",
                                topic: &config.topic,
                                filter: config.filter.as_deref(),
                                interval_secs,
                                services: config.services.iter().map(|s| s.name.as_str()).collect(),
                            },
                            auth,
                            vars,
                        ),
                    })
                },
            )
            .collect()
    }
}

/// The Azure units that list under a subscription.
const AZURE_ARM_UNITS: &[&str] = &["activity_log", "defender", "sentinel"];

impl AzureSourceConfig {
    /// One instance of the shipped `azure` profile per resolved connection:
    /// the service principal as the OAuth2 identity, the tenant and hosts as
    /// vars, the Sentinel knobs as vars, and every `log_analytics` service's
    /// `workspace_id` and `kql` as one entry of the query list.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection has no tenant id, no
    /// client id or secret, lists a subscription-scoped service without a
    /// subscription, or a `log_analytics` service without its workspace and
    /// query.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("azure")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let auth = oauth2_identity(
                        "azure",
                        config.client_id.as_deref(),
                        config.credential_secret.as_deref(),
                        config.client_secret.as_ref(),
                    )?;
                    let services: Vec<(&str, &HashMap<String, Value>)> = config
                        .services
                        .iter()
                        .map(|s| (s.name.as_str(), &s.config))
                        .collect();
                    let mut vars = BTreeMap::new();
                    vars.insert(
                        "tenant_id".into(),
                        Value::String(
                            required("azure", "tenant_id", config.tenant_id.as_deref())?.to_owned(),
                        ),
                    );
                    if let Some(subscription) =
                        config.subscription_id.as_deref().filter(|s| !s.is_empty())
                    {
                        vars.insert(
                            "subscription_id".into(),
                            Value::String(subscription.to_owned()),
                        );
                    } else if let Some((unit, _)) = services
                        .iter()
                        .find(|(name, _)| AZURE_ARM_UNITS.contains(name))
                    {
                        return Err(Error::Config(format!(
                            "sources.azure: `subscription_id` is required for `{unit}`"
                        )));
                    }
                    for (var, value) in [
                        ("management_url", &config.management_url_override),
                        ("graph_url", &config.graph_url_override),
                        ("token_url", &config.token_url_override),
                    ] {
                        if let Some(value) = value {
                            vars.insert(var.into(), Value::String(value.clone()));
                        }
                    }
                    let sentinel = Knobs::of(services.iter().copied(), "sentinel");
                    for (var, knob) in [
                        ("sentinel_resource_group", "resource_group"),
                        ("sentinel_workspace_name", "workspace_name"),
                    ] {
                        if let Some(value) = sentinel.text(knob) {
                            vars.insert(var.into(), Value::String(value.to_owned()));
                        }
                    }
                    let mut queries = Vec::new();
                    for (_, knobs) in services.iter().filter(|(name, _)| *name == "log_analytics") {
                        let knobs = Knobs(Some(knobs));
                        let workspace_id = required(
                            "azure",
                            "log_analytics workspace_id",
                            knobs.text("workspace_id"),
                        )?;
                        let kql = required("azure", "log_analytics kql", knobs.text("kql"))?;
                        queries
                            .push(serde_json::json!({ "workspace_id": workspace_id, "kql": kql }));
                    }
                    if !queries.is_empty() {
                        vars.insert("log_analytics_queries".into(), Value::Array(queries));
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance: instance(
                            Common {
                                profile: "azure",
                                topic: &config.topic,
                                filter: config.filter.as_deref(),
                                interval_secs,
                                services: services.iter().map(|(name, _)| *name).collect(),
                            },
                            auth,
                            vars,
                        ),
                    })
                },
            )
            .collect()
    }
}

/// The Management Activity content types the `audit_log` service fetches
/// unless its `content_types` knob narrows them.
const M365_AUDIT_LOG_FEEDS: &[&str] = &[
    "Audit.AzureActiveDirectory",
    "Audit.Exchange",
    "Audit.SharePoint",
    "Audit.General",
    "DLP.All",
];

/// The unit an `audit_log` content type lands under: `audit_log.<feed>`,
/// the feed being the content type lowercased with its dots as underscores.
fn m365_audit_log_unit(content_type: &str) -> String {
    format!(
        "audit_log.{}",
        content_type.to_ascii_lowercase().replace('.', "_")
    )
}

impl M365SourceConfig {
    /// The content types one `audit_log` service fetches: the knob's list
    /// when it is set, else every feed.
    fn audit_log_feeds(service: &HashMap<String, Value>) -> Result<Vec<&str>> {
        let Some(listed) = service.get("content_types") else {
            return Ok(M365_AUDIT_LOG_FEEDS.to_vec());
        };
        let listed: Vec<&str> = listed
            .as_array()
            .map(|items| items.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        if let Some(unknown) = listed.iter().find(|ct| !M365_AUDIT_LOG_FEEDS.contains(ct)) {
            return Err(Error::Config(format!(
                "sources.m365: `content_types` names `{unknown}`, not one of {}",
                M365_AUDIT_LOG_FEEDS.join(", ")
            )));
        }
        Ok(listed)
    }

    /// The profile units the block's services enable: `audit_log` is one
    /// unit per content type it fetches, every other service the unit of
    /// its own name, a name the profile lacks kept for validation to refuse.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when `content_types` names a content type
    /// outside the five feeds.
    pub fn service_units(&self) -> Result<Vec<String>> {
        let mut units = Vec::new();
        for service in &self.services {
            if service.name == "audit_log" {
                units.extend(
                    Self::audit_log_feeds(&service.config)?
                        .into_iter()
                        .map(m365_audit_log_unit),
                );
            } else {
                units.push(service.name.clone());
            }
        }
        Ok(units)
    }

    /// One instance of the shipped `m365` profile per resolved connection:
    /// the application as the OAuth2 identity, the tenant, the hosts and
    /// the publisher identifier as vars, and the services as the units
    /// they name.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection has no tenant id, no
    /// client id or secret, or an `audit_log` service names a content type
    /// outside the five feeds.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("m365")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let auth = oauth2_identity(
                        "m365",
                        config.client_id.as_deref(),
                        config.credential_secret.as_deref(),
                        config.client_secret.as_ref(),
                    )?;
                    let mut vars = BTreeMap::new();
                    vars.insert(
                        "tenant_id".into(),
                        Value::String(
                            required("m365", "tenant_id", config.tenant_id.as_deref())?.to_owned(),
                        ),
                    );
                    for (var, value) in [
                        ("management_url", &config.management_url_override),
                        ("graph_url", &config.graph_url_override),
                        ("token_url", &config.token_url_override),
                        ("publisher_identifier", &config.publisher_identifier),
                    ] {
                        if let Some(value) = value.as_deref().filter(|v| !v.is_empty()) {
                            vars.insert(var.into(), Value::String(value.to_owned()));
                        }
                    }
                    let units = config.service_units()?;
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance: instance(
                            Common {
                                profile: "m365",
                                topic: &config.topic,
                                filter: config.filter.as_deref(),
                                interval_secs,
                                services: units.iter().map(String::as_str).collect(),
                            },
                            auth,
                            vars,
                        ),
                    })
                },
            )
            .collect()
    }
}

/// The GCP units that list a project's Cloud Logging entries.
const GCP_LOGGING_UNITS: &[&str] = &[
    "admin_activity",
    "data_access",
    "system_event",
    "policy_denied",
    "vpc_flow_logs",
    "dns_queries",
    "storage_access",
    "cloud_logging",
];

impl GcpSourceConfig {
    /// One instance of the shipped `gcp` profile per resolved connection:
    /// `credential_secret` is a resolved bearer, else `service_account_key`
    /// is the key, or its file, of a JWT-bearer exchange, else the workload's token
    /// comes from the GCE metadata server; the project, the hosts, the SCC
    /// organisation and the `cloud_logging` clause become vars.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a Cloud Logging unit is listed without
    /// a project id or `scc` without its `organization_id`.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("gcp")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let auth = match (&config.credential_secret, &config.service_account_key) {
                        (Some(token), _) => InstanceAuth {
                            mode: AuthKind::Bearer,
                            token: Some(SensitiveString::from(token.as_str())),
                            ..InstanceAuth::default()
                        },
                        (None, Some(key)) => service_account_identity(key),
                        (None, None) => InstanceAuth {
                            mode: AuthKind::GceMetadata,
                            ..InstanceAuth::default()
                        },
                    };
                    let services: Vec<(&str, &HashMap<String, Value>)> = config
                        .services
                        .iter()
                        .map(|s| (s.name.as_str(), &s.config))
                        .collect();
                    let mut vars = BTreeMap::new();
                    if let Some(project) = config.project_id.as_deref().filter(|s| !s.is_empty()) {
                        vars.insert("project_id".into(), Value::String(project.to_owned()));
                    } else if let Some((unit, _)) = services
                        .iter()
                        .find(|(name, _)| GCP_LOGGING_UNITS.contains(name))
                    {
                        return Err(Error::Config(format!(
                            "sources.gcp: `project_id` is required for `{unit}`"
                        )));
                    }
                    if let Some(api_url) = &config.api_url_override {
                        vars.insert("logging_url".into(), Value::String(api_url.clone()));
                        vars.insert("scc_url".into(), Value::String(api_url.clone()));
                    }
                    if let Some(token_url) = &config.token_url_override {
                        vars.insert("token_url".into(), Value::String(token_url.clone()));
                    }
                    if services.iter().any(|(name, _)| *name == "scc") {
                        let organization = required(
                            "gcp",
                            "scc organization_id",
                            Knobs::of(services.iter().copied(), "scc").text("organization_id"),
                        )?;
                        vars.insert(
                            "organization_id".into(),
                            Value::String(organization.to_owned()),
                        );
                    }
                    let mut instance = instance(
                        Common {
                            profile: "gcp",
                            topic: &config.topic,
                            filter: config.filter.as_deref(),
                            interval_secs,
                            services: services.iter().map(|(name, _)| *name).collect(),
                        },
                        auth,
                        vars,
                    );
                    if let Some(filter) =
                        Knobs::of(services.iter().copied(), "cloud_logging").text("filter")
                        && let Some(unit) = instance.units.get_mut("cloud_logging")
                    {
                        unit.vars
                            .insert("log_filter".into(), Value::String(filter.to_owned()));
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance,
                    })
                },
            )
            .collect()
    }
}

impl GoogleWorkspaceSourceConfig {
    /// One instance of the shipped `google_workspace` profile per resolved
    /// connection: `credential_secret` is the key JSON, else
    /// `service_account_key` the key or its file; the admin, customer and hosts become
    /// vars and each application's `event_name` its unit's var.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection has no admin email or no
    /// key.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("google_workspace")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let auth = match (&config.credential_secret, &config.service_account_key) {
                        (Some(key_json), _) => InstanceAuth {
                            mode: AuthKind::JwtBearer,
                            service_account_key: Some(SensitiveString::from(key_json.as_str())),
                            ..InstanceAuth::default()
                        },
                        (None, Some(key)) => service_account_identity(key),
                        (None, None) => {
                            return Err(Error::Config(
                                "sources.google_workspace: `service_account_key` or `credential_secret` is required"
                                    .into(),
                            ));
                        }
                    };
                    let mut vars = BTreeMap::new();
                    vars.insert(
                        "admin_email".into(),
                        Value::String(
                            required("google_workspace", "admin_email", config.admin_email.as_deref())?
                                .to_owned(),
                        ),
                    );
                    for (var, value) in [
                        ("customer_id", &config.customer_id),
                        ("api_url", &config.api_url_override),
                        ("token_url", &config.token_url_override),
                    ] {
                        if let Some(value) = value.as_deref().filter(|s| !s.is_empty()) {
                            vars.insert(var.into(), Value::String(value.to_owned()));
                        }
                    }
                    let mut instance = instance(
                        Common {
                            profile: "google_workspace",
                            topic: &config.topic,
                            filter: config.filter.as_deref(),
                            interval_secs,
                            services: config.services.iter().map(|s| s.name.as_str()).collect(),
                        },
                        auth,
                        vars,
                    );
                    for service in &config.services {
                        let event_name = Knobs::of(
                            std::iter::once((service.name.as_str(), &service.config)),
                            &service.name,
                        )
                        .text("event_name");
                        if let (Some(event_name), Some(unit)) =
                            (event_name, instance.units.get_mut(&service.name))
                        {
                            unit.vars
                                .insert("event_name".into(), Value::String(event_name.to_owned()));
                        }
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance,
                    })
                },
            )
            .collect()
    }
}

/// Inspector v2 and Health cap a page at this many items; a larger
/// `max_results` is sent as the cap.
const AWS_MAX_RESULTS: u64 = 100;

/// The workflow statuses Security Hub's `GetFindings` filter accepts.
const SECURITYHUB_WORKFLOW_STATUSES: [&str; 4] = ["NEW", "NOTIFIED", "RESOLVED", "SUPPRESSED"];

impl AwsSourceConfig {
    /// One instance of the shipped `aws` profile per resolved connection:
    /// `credential_secret` is the credentials document, else the key pair,
    /// and `assume_role_arn` the role those keys assume; the region and the
    /// endpoint override are vars; each listed service's knobs become its
    /// unit's vars, and the CloudWatch namespaces (crossed with the metric
    /// names, when given) become the `ListMetrics` filters the unit's keyset
    /// walks.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a connection has no credential,
    /// `cloudwatch_logs` is listed without `log_group_name`,
    /// `cloudwatch_metrics` without `namespaces`, or `securityhub` with a
    /// `workflow_status` Security Hub does not define.
    pub fn instances(&self) -> Result<Vec<BuiltinInstance>> {
        self.resolved("aws")
            .into_iter()
            .map(
                |Resolved {
                     id,
                     config,
                     interval_secs,
                 }| {
                    let assume_role_arn = config
                        .assume_role_arn
                        .clone()
                        .filter(|arn| !arn.trim().is_empty());
                    let auth = match &config.credential_secret {
                        Some(spec) => InstanceAuth {
                            mode: AuthKind::SigV4,
                            credentials_json: Some(SensitiveString::from(spec.as_str())),
                            assume_role_arn,
                            ..InstanceAuth::default()
                        },
                        None => InstanceAuth {
                            mode: AuthKind::SigV4,
                            access_key_id: Some(SensitiveString::from(required(
                                "aws",
                                "access_key_id",
                                config.access_key_id.as_deref(),
                            )?)),
                            secret_access_key: Some(
                                config.secret_access_key.clone().ok_or_else(|| {
                                    Error::Config(
                                        "sources.aws: `secret_access_key` or `credential_secret` is required"
                                            .into(),
                                    )
                                })?,
                            ),
                            assume_role_arn,
                            ..InstanceAuth::default()
                        },
                    };
                    let services: Vec<(&str, &HashMap<String, Value>)> = config
                        .services
                        .iter()
                        .map(|s| (s.name.as_str(), &s.config))
                        .collect();
                    let mut vars = BTreeMap::new();
                    vars.insert("region".into(), Value::String(config.region.clone()));
                    if let Some(url) = config.endpoint_override.as_deref().filter(|u| !u.is_empty()) {
                        vars.insert("endpoint_url".into(), Value::String(url.to_owned()));
                    }
                    let mut instance = instance(
                        Common {
                            profile: "aws",
                            topic: &config.topic,
                            filter: config.filter.as_deref(),
                            interval_secs,
                            services: services.iter().map(|(name, _)| *name).collect(),
                        },
                        auth,
                        vars,
                    );
                    for (name, knobs) in &services {
                        let knobs = Knobs(Some(knobs));
                        let Some(unit) = instance.units.get_mut(*name) else {
                            continue;
                        };
                        match *name {
                            "cloudwatch_logs" => {
                                let group = required("aws", "cloudwatch_logs log_group_name", knobs.text("log_group_name"))?;
                                unit.vars.insert("log_group_name".into(), Value::String(group.to_owned()));
                                if let Some(pattern) = knobs.text("filter_pattern") {
                                    unit.vars.insert("filter_pattern".into(), Value::String(pattern.to_owned()));
                                }
                            }
                            "cloudwatch_metrics" => {
                                let namespaces = knobs.texts("namespaces");
                                if namespaces.is_empty() {
                                    return Err(Error::Config(
                                        "sources.aws: `cloudwatch_metrics` needs `namespaces` in its config".into(),
                                    ));
                                }
                                let names = knobs.texts("metric_names");
                                let filters: Vec<Value> = namespaces
                                    .iter()
                                    .flat_map(|namespace| {
                                        if names.is_empty() {
                                            vec![serde_json::json!({ "Namespace": namespace })]
                                        } else {
                                            names
                                                .iter()
                                                .map(|metric| serde_json::json!({ "Namespace": namespace, "MetricName": metric }))
                                                .collect()
                                        }
                                    })
                                    .collect();
                                unit.vars.insert("metric_filters".into(), Value::Array(filters));
                                if let Some(period) = knobs.int("period_secs", u64::MAX) {
                                    unit.vars.insert("period_secs".into(), Value::from(period));
                                }
                                for knob in ["stat", "output_format"] {
                                    if let Some(value) = knobs.text(knob) {
                                        unit.vars.insert(knob.into(), Value::String(value.to_owned()));
                                    }
                                }
                            }
                            "config" => {
                                if let Some(expression) = knobs.text("expression") {
                                    unit.vars.insert("expression".into(), Value::String(expression.to_owned()));
                                }
                            }
                            "inspector" | "health" => {
                                if let Some(max) = knobs.int("max_results", AWS_MAX_RESULTS) {
                                    unit.vars.insert("max_results".into(), Value::from(max));
                                }
                            }
                            "securityhub" => {
                                let statuses = knobs.texts("workflow_status");
                                if let Some(unknown) = statuses
                                    .iter()
                                    .find(|s| !SECURITYHUB_WORKFLOW_STATUSES.contains(s))
                                {
                                    return Err(Error::Config(format!(
                                        "sources.aws: securityhub `workflow_status` `{unknown}` is not one of {}",
                                        SECURITYHUB_WORKFLOW_STATUSES.join(", ")
                                    )));
                                }
                                if !statuses.is_empty() {
                                    let filters = statuses
                                        .into_iter()
                                        .map(|s| serde_json::json!({ "Value": s, "Comparison": "EQUALS" }))
                                        .collect();
                                    unit.vars.insert("workflow_status_filter".into(), Value::Array(filters));
                                }
                            }
                            _ => {}
                        }
                    }
                    Ok(BuiltinInstance {
                        connection_id: id,
                        instance,
                    })
                },
            )
            .collect()
    }
}

/// One typed block in the registry: how the tables that used to be written
/// by hand -- the filter table, the enabled table, the instance expansion,
/// the connection ids `validate` checks and the capability catalog -- read
/// that block.
pub struct Block {
    /// The block's `sources.` key, which is also its shipped profile's name.
    pub name: &'static str,
    enabled: fn(&SourcesConfig) -> bool,
    filter: for<'a> fn(&'a SourcesConfig) -> Option<&'a str>,
    connection_ids: for<'a> fn(&'a SourcesConfig) -> Vec<&'a str>,
    instances: fn(&SourcesConfig) -> Result<Vec<BuiltinInstance>>,
    capability: fn() -> Capability,
}

impl Block {
    /// Whether the block is switched on.
    #[must_use]
    pub fn enabled(&self, sources: &SourcesConfig) -> bool {
        (self.enabled)(sources)
    }

    /// The block's type-wide CEL keep-filter.
    #[must_use]
    pub fn filter<'a>(&self, sources: &'a SourcesConfig) -> Option<&'a str> {
        (self.filter)(sources)
    }

    /// The connection ids the block resolves to: each `connections[].id`,
    /// or the block's name for the implicit single connection.
    #[must_use]
    pub fn connection_ids<'a>(&self, sources: &'a SourcesConfig) -> Vec<&'a str> {
        (self.connection_ids)(sources)
    }

    /// The block's connections as profile instances.
    ///
    /// # Errors
    ///
    /// Returns the block's [`Error::Config`], named by its `sources.` key.
    pub fn instances(&self, sources: &SourcesConfig) -> Result<Vec<BuiltinInstance>> {
        (self.instances)(sources)
    }

    /// The block's entry in the capability catalog, without its maturity.
    #[must_use]
    pub fn capability(&self) -> Capability {
        (self.capability)()
    }
}

/// The registry: every typed source block, in the order of the
/// [`SourcesConfig`] fields. Adding a block is one entry here; the filter
/// and enabled tables, the instance expansion, the connection-id check and
/// the capability catalog follow.
pub const REGISTRY: [Block; 19] = [
    Block {
        name: "aws",
        enabled: |s| s.aws.enabled,
        filter: |s| s.aws.filter.as_deref(),
        connection_ids: |s| s.aws.connection_ids("aws"),
        instances: |s| s.aws.instances(),
        capability: catalog::aws,
    },
    Block {
        name: "azure",
        enabled: |s| s.azure.enabled,
        filter: |s| s.azure.filter.as_deref(),
        connection_ids: |s| s.azure.connection_ids("azure"),
        instances: |s| s.azure.instances(),
        capability: catalog::azure,
    },
    Block {
        name: "m365",
        enabled: |s| s.m365.enabled,
        filter: |s| s.m365.filter.as_deref(),
        connection_ids: |s| s.m365.connection_ids("m365"),
        instances: |s| s.m365.instances(),
        capability: catalog::m365,
    },
    Block {
        name: "gcp",
        enabled: |s| s.gcp.enabled,
        filter: |s| s.gcp.filter.as_deref(),
        connection_ids: |s| s.gcp.connection_ids("gcp"),
        instances: |s| s.gcp.instances(),
        capability: catalog::gcp,
    },
    Block {
        name: "github",
        enabled: |s| s.github.enabled,
        filter: |s| s.github.filter.as_deref(),
        connection_ids: |s| s.github.connection_ids("github"),
        instances: |s| s.github.instances(),
        capability: catalog::github,
    },
    Block {
        name: "okta",
        enabled: |s| s.okta.enabled,
        filter: |s| s.okta.filter.as_deref(),
        connection_ids: |s| s.okta.connection_ids("okta"),
        instances: |s| s.okta.instances(),
        capability: catalog::okta,
    },
    Block {
        name: "cloudflare",
        enabled: |s| s.cloudflare.enabled,
        filter: |s| s.cloudflare.filter.as_deref(),
        connection_ids: |s| s.cloudflare.connection_ids("cloudflare"),
        instances: |s| s.cloudflare.instances(),
        capability: catalog::cloudflare,
    },
    Block {
        name: "onepassword",
        enabled: |s| s.onepassword.enabled,
        filter: |s| s.onepassword.filter.as_deref(),
        connection_ids: |s| s.onepassword.connection_ids("onepassword"),
        instances: |s| s.onepassword.instances(),
        capability: catalog::onepassword,
    },
    Block {
        name: "crowdstrike",
        enabled: |s| s.crowdstrike.enabled,
        filter: |s| s.crowdstrike.filter.as_deref(),
        connection_ids: |s| s.crowdstrike.connection_ids("crowdstrike"),
        instances: |s| s.crowdstrike.instances(),
        capability: catalog::crowdstrike,
    },
    Block {
        name: "slack",
        enabled: |s| s.slack.enabled,
        filter: |s| s.slack.filter.as_deref(),
        connection_ids: |s| s.slack.connection_ids("slack"),
        instances: |s| s.slack.instances(),
        capability: catalog::slack,
    },
    Block {
        name: "bitwarden",
        enabled: |s| s.bitwarden.enabled,
        filter: |s| s.bitwarden.filter.as_deref(),
        connection_ids: |s| s.bitwarden.connection_ids("bitwarden"),
        instances: |s| s.bitwarden.instances(),
        capability: catalog::bitwarden,
    },
    Block {
        name: "duo",
        enabled: |s| s.duo.enabled,
        filter: |s| s.duo.filter.as_deref(),
        connection_ids: |s| s.duo.connection_ids("duo"),
        instances: |s| s.duo.instances(),
        capability: catalog::duo,
    },
    Block {
        name: "pypi",
        enabled: |s| s.pypi.enabled,
        filter: |s| s.pypi.filter.as_deref(),
        connection_ids: |_| vec!["pypi"],
        instances: |s| Ok(s.pypi.instances()),
        capability: catalog::pypi,
    },
    Block {
        name: "crates_io",
        enabled: |s| s.crates_io.enabled,
        filter: |s| s.crates_io.filter.as_deref(),
        connection_ids: |_| vec!["crates_io"],
        instances: |s| Ok(s.crates_io.instances()),
        capability: catalog::crates_io,
    },
    Block {
        name: "go_modules",
        enabled: |s| s.go_modules.enabled,
        filter: |s| s.go_modules.filter.as_deref(),
        connection_ids: |_| vec!["go_modules"],
        instances: |s| Ok(s.go_modules.instances()),
        capability: catalog::go_modules,
    },
    Block {
        name: "google_workspace",
        enabled: |s| s.google_workspace.enabled,
        filter: |s| s.google_workspace.filter.as_deref(),
        connection_ids: |s| s.google_workspace.connection_ids("google_workspace"),
        instances: |s| s.google_workspace.instances(),
        capability: catalog::google_workspace,
    },
    Block {
        name: "gcp_pubsub",
        enabled: |s| s.gcp_pubsub.enabled,
        filter: |s| s.gcp_pubsub.filter.as_deref(),
        connection_ids: |_| vec!["gcp_pubsub"],
        instances: |s| s.gcp_pubsub.instances(),
        capability: catalog::gcp_pubsub,
    },
    Block {
        name: "object_store",
        enabled: |s| s.object_store.enabled,
        filter: |s| s.object_store.filter.as_deref(),
        connection_ids: |_| vec!["object_store"],
        instances: |s| s.object_store.instances(),
        capability: catalog::object_store,
    },
    Block {
        name: "salesforce",
        enabled: |s| s.salesforce.enabled,
        filter: |s| s.salesforce.filter.as_deref(),
        connection_ids: |s| s.salesforce.connection_ids("salesforce"),
        instances: |s| s.salesforce.instances(),
        capability: catalog::salesforce,
    },
];

impl SourcesConfig {
    /// Every enabled built-in block's connections as profile instances, in
    /// registry order.
    ///
    /// # Errors
    ///
    /// Returns the first block's [`Error::Config`], named by its `sources.`
    /// key.
    pub fn builtin_instances(&self) -> Result<Vec<BuiltinInstance>> {
        let mut out = Vec::new();
        for block in REGISTRY.iter().filter(|b| b.enabled(self)) {
            out.extend(block.instances(self)?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AwsConnection, AwsService, AwsSourceConfig, AzureService, AzureSourceConfig,
        BitwardenService, BitwardenSourceConfig, CloudflareService, CloudflareSourceConfig,
        CratesIoSourceConfig, CrowdstrikeService, CrowdstrikeSourceConfig, DuoConnection,
        DuoService, DuoSignatureVersion, DuoSourceConfig, GcpPubsubSourceConfig,
        GcpPubsubSubscription, GcpService, GcpSourceConfig, GithubConnection, GithubService,
        GithubSourceConfig, GoModulesSourceConfig, GoogleWorkspaceService,
        GoogleWorkspaceSourceConfig, M365Service, M365SourceConfig, ObjectStoreBucket,
        ObjectStorePrefix, OktaConnection, OktaService, OktaSourceConfig, OnePasswordService,
        OnePasswordSourceConfig, PypiSourceConfig, S3BackendConfig, SalesforceConnection,
        SalesforceService, SalesforceSourceConfig, SlackService, SlackSourceConfig, SourcesConfig,
    };
    use dfe_fetcher_rest::profile::{AuthKind, ProfileRef, SignaturePreset};
    use serde_json::{Value, json};
    use std::collections::HashMap;

    fn github() -> GithubSourceConfig {
        GithubSourceConfig {
            enabled: true,
            org: Some("acme".into()),
            token: Some("tok".to_string().into()),
            services: vec![GithubService {
                name: "audit_log".into(),
                config: HashMap::new(),
            }],
            filter: Some("action != \"git.clone\"".into()),
            interval_secs: Some(120),
            ..GithubSourceConfig::default()
        }
    }

    fn okta() -> OktaSourceConfig {
        OktaSourceConfig {
            enabled: true,
            tenant_url: Some("https://acme.okta.com".into()),
            token: Some("tok".to_string().into()),
            services: vec![OktaService {
                name: "system_log".into(),
                config: HashMap::new(),
            }],
            ..OktaSourceConfig::default()
        }
    }

    fn only(instances: Vec<BuiltinInstance>) -> BuiltinInstance {
        let [one] = <[BuiltinInstance; 1]>::try_from(instances)
            .unwrap_or_else(|v| panic!("one instance, got {}", v.len()));
        one
    }

    #[test]
    fn github_org_becomes_a_bearer_instance_of_the_shipped_profile() {
        let b = only(github().instances().unwrap());
        assert_eq!(
            b.connection_id, "github",
            "the implicit connection keeps the type name"
        );
        let i = &b.instance;
        assert!(i.enabled);
        assert_eq!(i.profile, ProfileRef::Named("github".into()));
        assert_eq!(i.topic, "github");
        assert_eq!(i.filter.as_deref(), Some("action != \"git.clone\""));
        assert_eq!(i.interval_secs, Some(120));
        assert_eq!(i.auth.mode, AuthKind::Bearer);
        assert_eq!(i.auth.token.as_ref().unwrap().expose(), "tok");
        assert_eq!(i.vars["scope_path"], "orgs/acme/audit-log");
        assert_eq!(i.vars["include"], "all");
        assert!(
            !i.vars.contains_key("api_url"),
            "no override: the profile's public API default applies"
        );
        assert!(i.units["audit_log"].enabled);
    }

    #[test]
    fn github_enterprise_override_and_include_map_to_vars() {
        let mut cfg = github();
        cfg.org = None;
        cfg.enterprise = Some("globex".into());
        cfg.api_url_override = Some("http://127.0.0.1:1/".into());
        cfg.services[0]
            .config
            .insert("include".into(), Value::String("git".into()));
        let b = only(cfg.instances().unwrap());
        assert_eq!(
            b.instance.vars["scope_path"],
            "enterprises/globex/audit-log"
        );
        assert_eq!(b.instance.vars["include"], "git");
        assert_eq!(b.instance.vars["api_url"], "http://127.0.0.1:1/");
    }

    #[test]
    fn github_include_outside_the_three_values_falls_back_to_all() {
        let mut cfg = github();
        cfg.services[0]
            .config
            .insert("include".into(), Value::String("everything".into()));
        assert_eq!(
            only(cfg.instances().unwrap()).instance.vars["include"],
            "all"
        );
        cfg.services[0].config.insert("include".into(), json!(7));
        assert_eq!(
            only(cfg.instances().unwrap()).instance.vars["include"],
            "all"
        );
    }

    #[test]
    fn github_credential_secret_wins_over_the_literal_token() {
        let mut cfg = github();
        cfg.credential_secret = Some("env:GH_PAT".into());
        let b = only(cfg.instances().unwrap());
        assert_eq!(
            b.instance.auth.token.as_ref().unwrap().expose(),
            "env:GH_PAT"
        );
    }

    #[test]
    fn github_scope_must_be_exactly_one_of_org_and_enterprise() {
        let mut both = github();
        both.enterprise = Some("globex".into());
        let err = both.instances().unwrap_err().to_string();
        assert!(err.contains("exactly one"), "{err}");
        let mut neither = github();
        neither.org = None;
        let err = neither.instances().unwrap_err().to_string();
        assert!(err.contains("org") && err.contains("enterprise"), "{err}");
        let mut empty = github();
        empty.org = Some(String::new());
        assert!(empty.instances().is_err(), "an empty slug is unset");
    }

    #[test]
    fn github_without_any_token_is_refused_with_the_field_names() {
        let mut cfg = github();
        cfg.token = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("token") && err.contains("credential_secret"),
            "{err}"
        );
    }

    #[test]
    fn github_connections_become_one_instance_each_with_their_own_identity() {
        let mut cfg = github();
        cfg.org = None;
        cfg.token = None;
        cfg.connections = vec![
            GithubConnection {
                id: "gh-acme".into(),
                org: Some("acme".into()),
                token: Some("tok-a".to_string().into()),
                interval_secs: Some(30),
                ..GithubConnection::default()
            },
            GithubConnection {
                id: "gh-globex".into(),
                enterprise: Some("globex".into()),
                credential_secret: Some("env:GLOBEX".into()),
                ..GithubConnection::default()
            },
        ];
        let instances = cfg.instances().unwrap();
        let ids: Vec<&str> = instances.iter().map(|b| b.connection_id.as_str()).collect();
        assert_eq!(ids, ["gh-acme", "gh-globex"]);
        assert_eq!(
            instances[0].instance.vars["scope_path"],
            "orgs/acme/audit-log"
        );
        assert_eq!(instances[0].instance.interval_secs, Some(30));
        assert_eq!(
            instances[0].instance.auth.token.as_ref().unwrap().expose(),
            "tok-a"
        );
        assert_eq!(
            instances[1].instance.vars["scope_path"],
            "enterprises/globex/audit-log"
        );
        assert_eq!(
            instances[1].instance.interval_secs,
            Some(120),
            "the type-level interval"
        );
        assert_eq!(
            instances[1].instance.auth.token.as_ref().unwrap().expose(),
            "env:GLOBEX"
        );
        for b in &instances {
            assert_eq!(b.instance.topic, "github", "type-wide");
            assert_eq!(
                b.instance.filter.as_deref(),
                Some("action != \"git.clone\""),
                "type-wide"
            );
        }
    }

    #[test]
    fn a_service_not_listed_is_a_disabled_unit_and_an_unknown_one_is_kept_for_validation() {
        let mut cfg = github();
        cfg.services.clear();
        let b = only(cfg.instances().unwrap());
        assert!(!b.instance.units["audit_log"].enabled);
        let mut cfg = github();
        cfg.services[0].name = "audit_logs".into();
        let b = only(cfg.instances().unwrap());
        assert!(!b.instance.units["audit_log"].enabled);
        assert!(
            b.instance.units["audit_logs"].enabled,
            "left for the profile's unit check to refuse by name"
        );
    }

    #[test]
    fn okta_tenant_with_ssws_becomes_an_api_key_instance() {
        let b = only(okta().instances().unwrap());
        assert_eq!(b.connection_id, "okta");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("okta".into()));
        assert_eq!(i.topic, "okta");
        assert_eq!(i.auth.mode, AuthKind::ApiKey);
        assert_eq!(i.auth.key.as_ref().unwrap().expose(), "tok");
        assert!(i.auth.token.is_none());
        assert_eq!(i.vars["base_url"], "https://acme.okta.com");
        assert!(!i.vars.contains_key("limit"), "the profile default applies");
        assert!(!i.vars.contains_key("filter"));
        assert!(i.units["system_log"].enabled);
    }

    #[test]
    fn okta_without_ssws_is_a_bearer_instance() {
        let mut cfg = okta();
        cfg.use_ssws_header = false;
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(i.auth.mode, AuthKind::Bearer);
        assert_eq!(i.auth.token.as_ref().unwrap().expose(), "tok");
        assert!(i.auth.key.is_none());
    }

    #[test]
    fn okta_limit_is_capped_at_1000_and_a_non_integer_limit_is_ignored() {
        let mut cfg = okta();
        cfg.services[0].config.insert("limit".into(), json!(5000));
        cfg.services[0].config.insert(
            "filter".into(),
            Value::String("eventType eq \"user.session.start\"".into()),
        );
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(i.vars["limit"], json!(1000));
        assert_eq!(i.vars["filter"], "eventType eq \"user.session.start\"");
        let mut cfg = okta();
        cfg.services[0]
            .config
            .insert("limit".into(), Value::String("250".into()));
        assert!(
            !only(cfg.instances().unwrap())
                .instance
                .vars
                .contains_key("limit")
        );
    }

    #[test]
    fn okta_api_url_override_beats_the_tenant_url() {
        let mut cfg = okta();
        cfg.api_url_override = Some("http://127.0.0.1:1/".into());
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(i.vars["base_url"], "http://127.0.0.1:1/");
    }

    #[test]
    fn okta_needs_a_tenant_url_or_an_override() {
        let mut cfg = okta();
        cfg.tenant_url = Some(String::new());
        let err = cfg.instances().unwrap_err().to_string();
        assert!(err.contains("tenant_url"), "{err}");
        cfg.tenant_url = None;
        assert!(cfg.instances().is_err());
    }

    #[test]
    fn okta_connections_carry_their_own_header_style() {
        let mut cfg = okta();
        cfg.connections = vec![
            OktaConnection {
                id: "okta-a".into(),
                token: Some("tok-a".to_string().into()),
                ..OktaConnection::default()
            },
            OktaConnection {
                id: "okta-b".into(),
                tenant_url: Some("https://b.okta.com".into()),
                token: Some("tok-b".to_string().into()),
                use_ssws_header: Some(false),
                ..OktaConnection::default()
            },
        ];
        let instances = cfg.instances().unwrap();
        assert_eq!(instances[0].connection_id, "okta-a");
        assert_eq!(instances[0].instance.auth.mode, AuthKind::ApiKey);
        assert_eq!(
            instances[0].instance.vars["base_url"], "https://acme.okta.com",
            "inherits the type-level tenant"
        );
        assert_eq!(instances[1].connection_id, "okta-b");
        assert_eq!(instances[1].instance.auth.mode, AuthKind::Bearer);
        assert_eq!(instances[1].instance.vars["base_url"], "https://b.okta.com");
    }

    fn slack() -> SlackSourceConfig {
        SlackSourceConfig {
            enabled: true,
            token: Some("tok".to_string().into()),
            services: vec![SlackService {
                name: "audit_logs".into(),
                config: HashMap::new(),
            }],
            ..SlackSourceConfig::default()
        }
    }

    #[test]
    fn slack_becomes_a_bearer_instance_with_the_knobs_as_vars() {
        let b = only(slack().instances().unwrap());
        assert_eq!(b.connection_id, "slack");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("slack".into()));
        assert_eq!(i.topic, "slack");
        assert_eq!(i.auth.mode, AuthKind::Bearer);
        assert_eq!(i.auth.token.as_ref().unwrap().expose(), "tok");
        assert!(
            i.vars.is_empty(),
            "no override, no knobs: the profile defaults apply"
        );
        assert!(i.units["audit_logs"].enabled);

        let mut cfg = slack();
        cfg.api_url_override = Some("http://127.0.0.1:1".into());
        cfg.credential_secret = Some("env:SLACK".into());
        cfg.services[0].config.insert("limit".into(), json!(5000));
        cfg.services[0]
            .config
            .insert("action".into(), Value::String("user_login".into()));
        cfg.services[0]
            .config
            .insert("entity".into(), Value::String("user".into()));
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(i.auth.token.as_ref().unwrap().expose(), "env:SLACK");
        assert_eq!(i.vars["api_url"], "http://127.0.0.1:1");
        assert_eq!(i.vars["limit"], json!(1000), "capped at Slack's page size");
        assert_eq!(i.vars["action"], "user_login");
        assert_eq!(i.vars["entity"], "user");

        let mut cfg = slack();
        cfg.token = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.slack") && err.contains("token"),
            "{err}"
        );
    }

    fn cloudflare() -> CloudflareSourceConfig {
        CloudflareSourceConfig {
            enabled: true,
            account_id: Some("0123456789abcdef0123456789abcdef".into()),
            token: Some("tok".to_string().into()),
            services: vec![CloudflareService {
                name: "audit_logs".into(),
                config: HashMap::new(),
            }],
            ..CloudflareSourceConfig::default()
        }
    }

    #[test]
    fn cloudflare_becomes_a_bearer_instance_with_the_account_and_knobs_as_vars() {
        let b = only(cloudflare().instances().unwrap());
        assert_eq!(b.connection_id, "cloudflare");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("cloudflare".into()));
        assert_eq!(i.auth.mode, AuthKind::Bearer);
        assert_eq!(i.auth.token.as_ref().unwrap().expose(), "tok");
        assert_eq!(i.vars["account_id"], "0123456789abcdef0123456789abcdef");
        assert_eq!(
            i.vars.len(),
            1,
            "no override, no knobs: the profile defaults apply"
        );

        let mut cfg = cloudflare();
        cfg.api_url_override = Some("http://127.0.0.1:1/v4".into());
        cfg.services[0]
            .config
            .insert("per_page".into(), json!(5000));
        cfg.services[0].config.insert(
            "actor_email".into(),
            Value::String("ops@example.com".into()),
        );
        cfg.services[0]
            .config
            .insert("action_type".into(), Value::String("login".into()));
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(i.vars["api_url"], "http://127.0.0.1:1/v4");
        assert_eq!(
            i.vars["per_page"],
            json!(1000),
            "capped at Cloudflare's page size"
        );
        assert_eq!(i.vars["actor_email"], "ops@example.com");
        assert_eq!(i.vars["action_type"], "login");

        let mut cfg = cloudflare();
        cfg.account_id = Some(String::new());
        let err = cfg.instances().unwrap_err().to_string();
        assert!(err.contains("account_id"), "an empty id is unset: {err}");
        let mut cfg = cloudflare();
        cfg.token = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.cloudflare") && err.contains("credential_secret"),
            "{err}"
        );
    }

    fn bitwarden() -> BitwardenSourceConfig {
        BitwardenSourceConfig {
            enabled: true,
            client_id: Some("organization.acme".into()),
            client_secret: Some("sec".to_string().into()),
            services: vec![BitwardenService {
                name: "events".into(),
                config: HashMap::new(),
            }],
            ..BitwardenSourceConfig::default()
        }
    }

    #[test]
    fn bitwarden_becomes_an_oauth2_instance_with_the_url_overrides_as_vars() {
        let b = only(bitwarden().instances().unwrap());
        assert_eq!(b.connection_id, "bitwarden");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("bitwarden".into()));
        assert_eq!(i.auth.mode, AuthKind::Oauth2ClientCredentials);
        assert_eq!(i.auth.client_id.as_deref(), Some("organization.acme"));
        assert_eq!(i.auth.client_secret.as_ref().unwrap().expose(), "sec");
        assert!(i.vars.is_empty(), "cloud defaults apply");
        assert!(i.units["events"].enabled);

        let mut cfg = bitwarden();
        cfg.credential_secret = Some("env:BW".into());
        cfg.api_url_override = Some("https://vault.example.com/api".into());
        cfg.identity_url_override = Some("https://vault.example.com/identity/connect/token".into());
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(
            i.auth.client_secret.as_ref().unwrap().expose(),
            "env:BW",
            "the spec wins over the literal secret"
        );
        assert_eq!(i.vars["api_url"], "https://vault.example.com/api");
        assert_eq!(
            i.vars["identity_url"],
            "https://vault.example.com/identity/connect/token"
        );

        let mut cfg = bitwarden();
        cfg.client_id = Some(String::new());
        let err = cfg.instances().unwrap_err().to_string();
        assert!(err.contains("client_id"), "an empty id is unset: {err}");
        let mut cfg = bitwarden();
        cfg.client_secret = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.bitwarden")
                && err.contains("client_secret")
                && err.contains("credential_secret"),
            "{err}"
        );
    }

    fn onepassword() -> OnePasswordSourceConfig {
        OnePasswordSourceConfig {
            enabled: true,
            token: Some("tok".to_string().into()),
            services: vec![
                OnePasswordService {
                    name: "signin_attempts".into(),
                    config: HashMap::new(),
                },
                OnePasswordService {
                    name: "audit_events".into(),
                    config: [("limit".to_string(), json!(5000))].into_iter().collect(),
                },
            ],
            ..OnePasswordSourceConfig::default()
        }
    }

    #[test]
    fn onepassword_becomes_a_bearer_instance_with_each_services_limit_on_its_unit() {
        let b = only(onepassword().instances().unwrap());
        assert_eq!(b.connection_id, "onepassword");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("onepassword".into()));
        assert_eq!(i.auth.mode, AuthKind::Bearer);
        assert_eq!(i.auth.token.as_ref().unwrap().expose(), "tok");
        assert!(i.vars.is_empty(), "the public host applies");
        assert!(i.units["signin_attempts"].enabled);
        assert!(
            i.units["signin_attempts"].vars.is_empty(),
            "no knob: the profile's limit applies"
        );
        assert!(i.units["audit_events"].enabled);
        assert_eq!(
            i.units["audit_events"].vars["limit"],
            json!(1000),
            "the service's limit, capped at 1Password's page size"
        );
        assert!(!i.units["item_usages"].enabled, "not listed");

        let mut cfg = onepassword();
        cfg.api_url_override = Some("https://events.ent.1password.eu".into());
        cfg.credential_secret = Some("env:OP".into());
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(i.vars["api_url"], "https://events.ent.1password.eu");
        assert_eq!(i.auth.token.as_ref().unwrap().expose(), "env:OP");

        let mut cfg = onepassword();
        cfg.token = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.onepassword") && err.contains("credential_secret"),
            "{err}"
        );
    }

    #[test]
    fn pypi_becomes_an_unauthenticated_instance_with_the_packages_as_a_var() {
        let cfg = PypiSourceConfig {
            enabled: true,
            packages: vec!["requests".into(), "scalo".into()],
            filter: Some("info.yanked == false".into()),
            interval_secs: Some(3600),
            ..PypiSourceConfig::default()
        };
        let b = only(cfg.instances());
        assert_eq!(b.connection_id, "pypi", "single-connection: the type name");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("pypi".into()));
        assert_eq!(i.topic, "pypi");
        assert_eq!(i.filter.as_deref(), Some("info.yanked == false"));
        assert_eq!(i.interval_secs, Some(3600));
        assert_eq!(i.auth.mode, AuthKind::None);
        assert_eq!(i.vars["packages"], json!(["requests", "scalo"]));
        assert!(
            !i.vars.contains_key("api_url"),
            "the public registry applies"
        );
        assert!(i.units["metadata"].enabled);

        let cfg = PypiSourceConfig {
            enabled: true,
            api_url_override: Some("http://127.0.0.1:1".into()),
            ..PypiSourceConfig::default()
        };
        let i = only(cfg.instances()).instance;
        assert_eq!(i.vars["api_url"], "http://127.0.0.1:1");
        assert_eq!(
            i.vars["packages"],
            json!([]),
            "no packages is an empty keyset"
        );
    }

    #[test]
    fn crates_io_becomes_an_unauthenticated_instance_with_the_crates_as_a_var() {
        let cfg = CratesIoSourceConfig {
            enabled: true,
            crates: vec!["serde".into(), "tokio".into()],
            ..CratesIoSourceConfig::default()
        };
        let b = only(cfg.instances());
        assert_eq!(b.connection_id, "crates_io");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("crates_io".into()));
        assert_eq!(i.topic, "crates_io");
        assert_eq!(i.auth.mode, AuthKind::None);
        assert_eq!(i.vars["crates"], json!(["serde", "tokio"]));
        assert!(i.units["metadata"].enabled);
    }

    #[test]
    fn go_modules_becomes_an_unauthenticated_instance_with_the_modules_as_a_var() {
        let cfg = GoModulesSourceConfig {
            enabled: true,
            modules: vec![
                "golang.org/x/text".into(),
                "github.com/hyperi-io/dfe".into(),
            ],
            api_url_override: Some("https://goproxy.internal".into()),
            filter: Some("_dfe_fetcher_module == \"golang.org/x/text\"".into()),
            ..GoModulesSourceConfig::default()
        };
        let b = only(cfg.instances());
        assert_eq!(b.connection_id, "go_modules");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("go_modules".into()));
        assert_eq!(i.topic, "go_modules");
        assert_eq!(i.auth.mode, AuthKind::None);
        assert_eq!(
            i.vars["modules"],
            json!(["golang.org/x/text", "github.com/hyperi-io/dfe"])
        );
        assert_eq!(i.vars["api_url"], "https://goproxy.internal");
        assert_eq!(i.filter.as_deref(), cfg.filter.as_deref());
        assert!(i.units["metadata"].enabled);
        let sources = SourcesConfig {
            go_modules: cfg,
            ..SourcesConfig::default()
        };
        assert_eq!(
            sources.builtin_instances().unwrap().len(),
            1,
            "an enabled block is one built-in instance"
        );
    }

    fn s3_block(prefixes: Vec<ObjectStorePrefix>) -> ObjectStoreSourceConfig {
        ObjectStoreSourceConfig {
            enabled: true,
            backends: vec![ObjectStoreBackendConfig::S3(S3BackendConfig {
                region: "ap-southeast-2".into(),
                endpoint_override: Some("http://localhost:9000/".into()),
                access_key_id: Some("AKIA".into()),
                secret_access_key: Some("sekrit".to_string().into()),
                credential_secret: None,
                buckets: vec![ObjectStoreBucket {
                    bucket: "audit".into(),
                    prefixes,
                }],
            })],
            ..ObjectStoreSourceConfig::default()
        }
    }

    fn s3_prefix(
        prefix: &str,
        format: ObjectStoreFormat,
        tag: &str,
        topic: Option<&str>,
    ) -> ObjectStorePrefix {
        ObjectStorePrefix {
            prefix: prefix.into(),
            format,
            source_tag: tag.into(),
            topic: topic.map(str::to_owned),
        }
    }

    #[test]
    fn object_store_becomes_a_sigv4_instance_with_a_unit_per_prefix() {
        let cfg = s3_block(vec![
            s3_prefix(
                "AWSLogs/1/CloudTrail/",
                ObjectStoreFormat::JsonGz,
                "aws_cloudtrail",
                None,
            ),
            s3_prefix("alb/", ObjectStoreFormat::Text, "alb", Some("alb-logs")),
        ]);
        let b = only(cfg.instances().unwrap());
        assert_eq!(b.connection_id, "object_store");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("object_store".into()));
        assert_eq!(i.topic, "object_store");
        assert_eq!(i.auth.mode, AuthKind::SigV4);
        assert_eq!(i.auth.access_key_id.as_ref().unwrap().expose(), "AKIA");
        assert_eq!(
            i.auth.secret_access_key.as_ref().unwrap().expose(),
            "sekrit"
        );
        assert_eq!(i.vars["region"], "ap-southeast-2");
        assert_eq!(
            i.vars["endpoint_url"], "http://localhost:9000",
            "trailing slash trimmed"
        );
        for template in ["json_gz", "jsonl", "json", "text", "text_gz"] {
            assert!(
                !i.units[template].enabled,
                "{template} is a template, not a unit"
            );
        }
        let cloudtrail = &i.units["aws_cloudtrail"];
        assert_eq!(cloudtrail.endpoint.as_deref(), Some("json_gz"));
        assert_eq!(cloudtrail.vars["bucket"], "audit");
        assert_eq!(cloudtrail.vars["prefix"], "AWSLogs/1/CloudTrail/");
        assert!(cloudtrail.topic.is_none());
        let alb = &i.units["alb"];
        assert_eq!(alb.endpoint.as_deref(), Some("text"));
        assert_eq!(alb.topic.as_deref(), Some("alb-logs"));

        let document = {
            let mut cfg = s3_block(vec![]);
            let ObjectStoreBackendConfig::S3(s3) = &mut cfg.backends[0] else {
                panic!("s3")
            };
            s3.access_key_id = None;
            s3.secret_access_key = None;
            s3.credential_secret = Some("vault:kv/data/s3:credentials".into());
            cfg
        };
        let b = only(document.instances().unwrap());
        assert_eq!(
            b.instance.auth.credentials_json.as_ref().unwrap().expose(),
            "vault:kv/data/s3:credentials"
        );
        assert!(b.instance.auth.access_key_id.is_none());

        let mut keyless = s3_block(vec![]);
        let ObjectStoreBackendConfig::S3(s3) = &mut keyless.backends[0] else {
            panic!("s3")
        };
        s3.secret_access_key = None;
        let err = keyless.instances().unwrap_err().to_string();
        assert!(err.contains("secret_access_key"), "{err}");

        let twice = s3_block(vec![
            s3_prefix("a/", ObjectStoreFormat::Jsonl, "same", None),
            s3_prefix("b/", ObjectStoreFormat::Jsonl, "same", None),
        ]);
        let err = twice.instances().unwrap_err().to_string();
        assert!(err.contains("`same`"), "{err}");

        let format_name = s3_block(vec![s3_prefix(
            "a/",
            ObjectStoreFormat::Text,
            "jsonl",
            None,
        )]);
        let err = format_name.instances().unwrap_err().to_string();
        assert!(err.contains("name of a format"), "{err}");

        let mut two_backends = s3_block(vec![]);
        two_backends.backends.push(two_backends.backends[0].clone());
        let err = two_backends.instances().unwrap_err().to_string();
        assert!(err.contains("one S3 backend"), "{err}");

        let mut none = s3_block(vec![]);
        none.backends.clear();
        assert!(
            none.instances().unwrap().is_empty(),
            "no S3 backend, no instance"
        );
    }

    #[test]
    fn gcp_pubsub_becomes_a_jwt_or_metadata_instance_with_a_unit_per_subscription() {
        let sub = |id: &str, max: u32| GcpPubsubSubscription {
            project_id: "proj".into(),
            subscription_id: id.into(),
            max_messages: max,
            return_immediately: false,
        };
        let cfg = GcpPubsubSourceConfig {
            enabled: true,
            service_account_key: Some("/etc/gcp/key.json".into()),
            api_url_override: Some("http://localhost:8085".into()),
            token_url_override: Some("http://localhost:8085/token".into()),
            subscriptions: vec![sub("audit", 500), sub("flow", 10)],
            ..GcpPubsubSourceConfig::default()
        };
        let b = only(cfg.instances().unwrap());
        assert_eq!(b.connection_id, "gcp_pubsub");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("gcp_pubsub".into()));
        assert_eq!(i.topic, "gcp_pubsub");
        assert_eq!(i.auth.mode, AuthKind::JwtBearer);
        assert_eq!(
            i.auth.service_account_key_file.as_ref().unwrap().expose(),
            "/etc/gcp/key.json"
        );
        assert_eq!(i.vars["api_url"], "http://localhost:8085");
        assert_eq!(i.vars["token_url"], "http://localhost:8085/token");
        assert!(!i.units["pull"].enabled, "the endpoint is a template");
        let audit = &i.units["audit"];
        assert_eq!(audit.endpoint.as_deref(), Some("pull"));
        assert_eq!(audit.vars["project_id"], "proj");
        assert_eq!(audit.vars["subscription_id"], "audit");
        assert_eq!(audit.vars["max_messages"], 500);
        assert_eq!(audit.vars["return_immediately"], false);
        assert_eq!(i.units["flow"].vars["max_messages"], 10);

        let json = GcpPubsubSourceConfig {
            enabled: true,
            credential_secret: Some("vault:kv/data/gcp:sa_key".into()),
            ..GcpPubsubSourceConfig::default()
        };
        let b = only(json.instances().unwrap());
        assert_eq!(b.instance.auth.mode, AuthKind::JwtBearer);
        assert_eq!(
            b.instance
                .auth
                .service_account_key
                .as_ref()
                .unwrap()
                .expose(),
            "vault:kv/data/gcp:sa_key"
        );
        assert!(
            b.instance.vars.is_empty(),
            "the public API and the key's token_uri"
        );

        let workload = GcpPubsubSourceConfig {
            enabled: true,
            ..GcpPubsubSourceConfig::default()
        };
        assert_eq!(
            only(workload.instances().unwrap()).instance.auth.mode,
            AuthKind::GceMetadata
        );

        let twice = GcpPubsubSourceConfig {
            enabled: true,
            subscriptions: vec![sub("same", 1), sub("same", 1)],
            ..GcpPubsubSourceConfig::default()
        };
        let err = twice.instances().unwrap_err().to_string();
        assert!(err.contains("`same`"), "{err}");
    }

    #[test]
    fn salesforce_becomes_a_jwt_or_client_credentials_instance_with_its_units() {
        let service = |name: &str, config: Vec<(&str, Value)>| SalesforceService {
            name: name.into(),
            config: config.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
        };
        let jwt = SalesforceSourceConfig {
            enabled: true,
            login_url: Some("https://test.salesforce.com/".into()),
            api_version: Some("v61.0".into()),
            client_id: Some("3MVG9consumer".into()),
            username: Some("audit@example.com".into()),
            private_key_secret: Some("vault:kv/data/salesforce:private_key".into()),
            private_key: Some("-----BEGIN PRIVATE KEY-----ignored".into()),
            instance_url_override: Some("https://acme.my.salesforce.com".into()),
            services: vec![
                service("setup_audit_trail", vec![]),
                service(
                    "event_log_file",
                    vec![
                        ("interval", json!("Hourly")),
                        ("event_types", json!(["Login", "O'Reilly"])),
                    ],
                ),
            ],
            ..SalesforceSourceConfig::default()
        };
        let b = only(jwt.instances().unwrap());
        assert_eq!(b.connection_id, "salesforce");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("salesforce".into()));
        assert_eq!(i.topic, "salesforce");
        assert_eq!(i.auth.mode, AuthKind::JwtBearer);
        assert_eq!(
            i.auth.private_key.as_ref().unwrap().expose(),
            "vault:kv/data/salesforce:private_key",
            "the secret spec wins over the literal key"
        );
        assert!(
            i.auth.client_id.is_none(),
            "the JWT claims read the consumer key as a var"
        );
        assert_eq!(i.vars["client_id"], "3MVG9consumer");
        assert_eq!(i.vars["username"], "audit@example.com");
        assert_eq!(i.vars["login_url"], "https://test.salesforce.com");
        assert_eq!(i.vars["api_version"], "v61.0");
        assert_eq!(i.vars["instance_url"], "https://acme.my.salesforce.com");
        assert!(i.units["setup_audit_trail"].enabled);
        assert!(!i.units["login_history"].enabled);
        let elf = &i.units["event_log_file"];
        assert!(elf.enabled);
        assert_eq!(elf.vars["interval"], "Hourly");
        assert_eq!(
            elf.vars["event_types_soql"], "'Login','O\\'Reilly'",
            "quoted and escaped for SOQL"
        );

        let secret = SalesforceSourceConfig {
            enabled: true,
            client_id: Some("3MVG9consumer".into()),
            client_secret: Some("shh".to_string().into()),
            services: vec![service("login_history", vec![])],
            connections: vec![
                SalesforceConnection {
                    id: "org_a".into(),
                    credential_secret: Some("vault:kv/data/sf/a:secret".into()),
                    ..SalesforceConnection::default()
                },
                SalesforceConnection {
                    id: "org_b".into(),
                    ..SalesforceConnection::default()
                },
            ],
            ..SalesforceSourceConfig::default()
        };
        let built = secret.instances().unwrap();
        assert_eq!(built.len(), 2);
        assert_eq!(built[0].connection_id, "org_a");
        assert_eq!(
            built[0].instance.auth.mode,
            AuthKind::Oauth2ClientCredentials
        );
        assert_eq!(
            built[0].instance.auth.client_id.as_deref(),
            Some("3MVG9consumer")
        );
        assert_eq!(
            built[0]
                .instance
                .auth
                .client_secret
                .as_ref()
                .unwrap()
                .expose(),
            "vault:kv/data/sf/a:secret",
            "the connection's secret spec"
        );
        assert_eq!(
            built[1]
                .instance
                .auth
                .client_secret
                .as_ref()
                .unwrap()
                .expose(),
            "shh",
            "the block's literal secret"
        );
        assert!(
            built[1].instance.vars.is_empty(),
            "the production login and v60.0 apply"
        );

        let bare = SalesforceSourceConfig {
            enabled: true,
            client_id: Some("x".into()),
            ..SalesforceSourceConfig::default()
        };
        let err = bare.instances().unwrap_err().to_string();
        assert!(err.contains("private_key"), "{err}");
        let no_user = SalesforceSourceConfig {
            enabled: true,
            client_id: Some("x".into()),
            private_key: Some("pem".into()),
            ..SalesforceSourceConfig::default()
        };
        let err = no_user.instances().unwrap_err().to_string();
        assert!(err.contains("username"), "{err}");
    }

    #[test]
    fn crowdstrike_becomes_an_oauth2_instance_with_the_region_and_knobs_as_vars() {
        let base = CrowdstrikeSourceConfig {
            enabled: true,
            client_id: Some("falcon".into()),
            client_secret: Some("sec".to_string().into()),
            services: vec![CrowdstrikeService {
                name: "alerts".into(),
                config: HashMap::new(),
            }],
            ..CrowdstrikeSourceConfig::default()
        };
        let b = only(base.instances().unwrap());
        assert_eq!(b.connection_id, "crowdstrike");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("crowdstrike".into()));
        assert_eq!(i.auth.mode, AuthKind::Oauth2ClientCredentials);
        assert_eq!(i.auth.client_id.as_deref(), Some("falcon"));
        assert_eq!(i.auth.client_secret.as_ref().unwrap().expose(), "sec");
        assert!(i.vars.is_empty(), "US-1 and the profile knobs apply");
        assert!(i.units["alerts"].enabled);

        let mut cfg = base.clone();
        cfg.api_url_override = Some("https://api.eu-1.crowdstrike.com".into());
        cfg.services[0].config.insert("limit".into(), json!(5000));
        cfg.services[0]
            .config
            .insert("filter".into(), Value::String("severity:>=70".into()));
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(i.vars["api_url"], "https://api.eu-1.crowdstrike.com");
        assert_eq!(i.vars["limit"], json!(1000), "capped at Falcon's page size");
        assert_eq!(i.vars["filter"], "severity:>=70");

        let mut cfg = base;
        cfg.client_id = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.crowdstrike") && err.contains("client_id"),
            "{err}"
        );
    }

    #[test]
    fn duo_becomes_a_signed_instance_with_the_host_as_its_base_url() {
        let base = DuoSourceConfig {
            enabled: true,
            api_host: Some("API-DEADBEEF.duosecurity.com".into()),
            integration_key: Some("DIWJ8X6AEYOR5OMC6TQ1".into()),
            secret_key: Some("skey".to_string().into()),
            services: vec![DuoService {
                name: "authentication_logs".into(),
                config: HashMap::new(),
            }],
            ..DuoSourceConfig::default()
        };
        let b = only(base.instances().unwrap());
        assert_eq!(b.connection_id, "duo");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("duo".into()));
        assert_eq!(i.auth.mode, AuthKind::Signature);
        assert_eq!(i.auth.key_id.as_deref(), Some("DIWJ8X6AEYOR5OMC6TQ1"));
        assert_eq!(i.auth.secret_key.as_ref().unwrap().expose(), "skey");
        assert_eq!(
            i.auth.signature_preset,
            Some(SignaturePreset::DuoV5),
            "the current scheme unless a tenant says otherwise"
        );
        assert_eq!(
            i.vars["base_url"], "https://API-DEADBEEF.duosecurity.com",
            "the host as given; the signer lowercases it"
        );
        assert!(!i.vars.contains_key("limit"));
        assert!(i.units["authentication_logs"].enabled);

        let mut cfg = base.clone();
        cfg.api_url_override = Some("http://127.0.0.1:1/".into());
        cfg.credential_secret = Some("env:DUO".into());
        cfg.services[0].config.insert("limit".into(), json!(5000));
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(
            i.vars["base_url"], "http://127.0.0.1:1",
            "the override wins, trailing slash trimmed"
        );
        assert_eq!(i.auth.secret_key.as_ref().unwrap().expose(), "env:DUO");
        assert_eq!(i.vars["limit"], json!(1000), "capped at Duo's page size");

        // An older tenant selects Duo's legacy scheme per connection, which is
        // the only way back to it: the shipped profile carries the current one.
        let mut cfg = base.clone();
        cfg.connections = vec![DuoConnection {
            id: "legacy".into(),
            signature_version: Some(DuoSignatureVersion::V2),
            ..DuoConnection::default()
        }];
        let legacy = only(cfg.instances().unwrap()).instance;
        assert_eq!(
            legacy.auth.signature_preset,
            Some(SignaturePreset::DuoV2),
            "the connection's version overlays the type's"
        );

        let mut cfg = base.clone();
        cfg.api_host = Some(String::new());
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.duo") && err.contains("api_host"),
            "{err}"
        );
        let mut cfg = base.clone();
        cfg.integration_key = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(err.contains("integration_key"), "{err}");
        let mut cfg = base;
        cfg.secret_key = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("secret_key") && err.contains("credential_secret"),
            "{err}"
        );
    }

    fn azure() -> AzureSourceConfig {
        AzureSourceConfig {
            enabled: true,
            tenant_id: Some("tenant".into()),
            client_id: Some("app".into()),
            client_secret: Some("sec".to_string().into()),
            subscription_id: Some("sub".into()),
            services: vec![
                AzureService {
                    name: "activity_log".into(),
                    config: HashMap::new(),
                },
                AzureService {
                    name: "entra_signins".into(),
                    config: HashMap::new(),
                },
            ],
            ..AzureSourceConfig::default()
        }
    }

    #[test]
    fn azure_becomes_an_oauth2_instance_with_the_tenant_hosts_and_queries_as_vars() {
        let b = only(azure().instances().unwrap());
        assert_eq!(b.connection_id, "azure");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("azure".into()));
        assert_eq!(i.topic, "azure");
        assert_eq!(i.auth.mode, AuthKind::Oauth2ClientCredentials);
        assert_eq!(i.auth.client_id.as_deref(), Some("app"));
        assert_eq!(i.auth.client_secret.as_ref().unwrap().expose(), "sec");
        assert_eq!(i.vars["tenant_id"], "tenant");
        assert_eq!(i.vars["subscription_id"], "sub");
        assert_eq!(
            i.vars.len(),
            2,
            "no override, no knobs: the public clouds and `default` apply"
        );
        assert!(i.units["activity_log"].enabled && i.units["entra_signins"].enabled);
        assert!(!i.units["defender"].enabled && !i.units["log_analytics"].enabled);

        let mut cfg = azure();
        cfg.credential_secret = Some("env:AZ".into());
        cfg.management_url_override = Some("http://127.0.0.1:1".into());
        cfg.graph_url_override = Some("http://127.0.0.1:2".into());
        cfg.token_url_override = Some("http://127.0.0.1:3/token".into());
        cfg.services.push(AzureService {
            name: "sentinel".into(),
            config: [
                ("resource_group".to_string(), json!("rg")),
                ("workspace_name".to_string(), json!("ws")),
            ]
            .into_iter()
            .collect(),
        });
        for (workspace, kql) in [("w1", "Heartbeat | take 1"), ("w2", "SecurityEvent")] {
            cfg.services.push(AzureService {
                name: "log_analytics".into(),
                config: [
                    ("workspace_id".to_string(), json!(workspace)),
                    ("kql".to_string(), json!(kql)),
                ]
                .into_iter()
                .collect(),
            });
        }
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(
            i.auth.client_secret.as_ref().unwrap().expose(),
            "env:AZ",
            "the spec supplies the secret alone"
        );
        assert_eq!(i.auth.client_id.as_deref(), Some("app"));
        assert_eq!(i.vars["management_url"], "http://127.0.0.1:1");
        assert_eq!(i.vars["graph_url"], "http://127.0.0.1:2");
        assert_eq!(i.vars["token_url"], "http://127.0.0.1:3/token");
        assert_eq!(i.vars["sentinel_resource_group"], "rg");
        assert_eq!(i.vars["sentinel_workspace_name"], "ws");
        assert_eq!(
            i.vars["log_analytics_queries"],
            json!([
                {"workspace_id": "w1", "kql": "Heartbeat | take 1"},
                {"workspace_id": "w2", "kql": "SecurityEvent"}
            ]),
            "every log_analytics service is one query of the one unit"
        );
        assert!(i.units["log_analytics"].enabled);

        let mut cfg = azure();
        cfg.tenant_id = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.azure") && err.contains("tenant_id"),
            "{err}"
        );
        let mut cfg = azure();
        cfg.subscription_id = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("subscription_id") && err.contains("activity_log"),
            "{err}"
        );
        cfg.services.remove(0);
        assert!(
            cfg.instances().is_ok(),
            "the Graph units need no subscription"
        );
        let mut cfg = azure();
        cfg.services.push(AzureService {
            name: "log_analytics".into(),
            config: [("kql".to_string(), json!("Heartbeat"))]
                .into_iter()
                .collect(),
        });
        let err = cfg.instances().unwrap_err().to_string();
        assert!(err.contains("workspace_id"), "{err}");
    }

    fn m365() -> M365SourceConfig {
        M365SourceConfig {
            enabled: true,
            tenant_id: Some("tenant".into()),
            client_id: Some("app".into()),
            client_secret: Some("sec".to_string().into()),
            services: vec![
                M365Service {
                    name: "audit_log".into(),
                    config: HashMap::new(),
                },
                M365Service {
                    name: "alerts".into(),
                    config: HashMap::new(),
                },
            ],
            ..M365SourceConfig::default()
        }
    }

    #[test]
    fn m365_becomes_an_oauth2_instance_with_one_unit_per_audit_log_feed() {
        let b = only(m365().instances().unwrap());
        assert_eq!(b.connection_id, "m365");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("m365".into()));
        assert_eq!(i.topic, "m365");
        assert_eq!(i.auth.mode, AuthKind::Oauth2ClientCredentials);
        assert_eq!(i.auth.client_id.as_deref(), Some("app"));
        assert_eq!(i.auth.client_secret.as_ref().unwrap().expose(), "sec");
        assert_eq!(i.vars["tenant_id"], "tenant");
        assert_eq!(
            i.vars.len(),
            1,
            "no override, no publisher: the public clouds and the shared publisher apply"
        );
        for unit in [
            "audit_log.audit_azureactivedirectory",
            "audit_log.audit_exchange",
            "audit_log.audit_sharepoint",
            "audit_log.audit_general",
            "audit_log.dlp_all",
            "alerts",
        ] {
            assert!(i.units[unit].enabled, "{unit}");
        }
        assert!(!i.units["dlp"].enabled && !i.units["exchange_audit"].enabled);

        let mut cfg = m365();
        cfg.credential_secret = Some("env:M365".into());
        cfg.management_url_override = Some("http://127.0.0.1:1".into());
        cfg.graph_url_override = Some("http://127.0.0.1:2".into());
        cfg.token_url_override = Some("http://127.0.0.1:3/token".into());
        cfg.publisher_identifier = Some("11111111-2222-3333-4444-555555555555".into());
        cfg.services = vec![
            M365Service {
                name: "audit_log".into(),
                config: [(
                    "content_types".to_string(),
                    json!(["Audit.SharePoint", "DLP.All"]),
                )]
                .into_iter()
                .collect(),
            },
            M365Service {
                name: "dlp".into(),
                config: HashMap::new(),
            },
        ];
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(
            i.auth.client_secret.as_ref().unwrap().expose(),
            "env:M365",
            "the spec supplies the secret alone"
        );
        assert_eq!(i.auth.client_id.as_deref(), Some("app"));
        assert_eq!(i.vars["management_url"], "http://127.0.0.1:1");
        assert_eq!(i.vars["graph_url"], "http://127.0.0.1:2");
        assert_eq!(i.vars["token_url"], "http://127.0.0.1:3/token");
        assert_eq!(
            i.vars["publisher_identifier"],
            "11111111-2222-3333-4444-555555555555"
        );
        assert!(
            i.units["audit_log.audit_sharepoint"].enabled && i.units["audit_log.dlp_all"].enabled
        );
        assert!(i.units["dlp"].enabled);
        assert!(
            !i.units["audit_log.audit_general"].enabled && !i.units["alerts"].enabled,
            "the knob narrows the feeds"
        );

        let mut cfg = m365();
        cfg.tenant_id = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.m365") && err.contains("tenant_id"),
            "{err}"
        );
        let mut cfg = m365();
        cfg.client_secret = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(err.contains("credential_secret"), "{err}");
        let mut cfg = m365();
        cfg.services[0].config = [("content_types".to_string(), json!(["Audit.Teams"]))]
            .into_iter()
            .collect();
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.m365") && err.contains("Audit.Teams"),
            "{err}"
        );
        let mut cfg = m365();
        cfg.services[0].config = [("content_types".to_string(), json!([]))]
            .into_iter()
            .collect();
        let i = only(cfg.instances().unwrap()).instance;
        assert!(
            !i.units
                .iter()
                .any(|(name, unit)| name.starts_with("audit_log.") && unit.enabled),
            "an empty list fetches no feed, as before"
        );
        assert!(i.units["alerts"].enabled);
    }

    fn gcp() -> GcpSourceConfig {
        GcpSourceConfig {
            enabled: true,
            project_id: Some("proj".into()),
            service_account_key: Some("/etc/gcp/sa-key.json".into()),
            services: vec![
                GcpService {
                    name: "admin_activity".into(),
                    config: HashMap::new(),
                },
                GcpService {
                    name: "cloud_logging".into(),
                    config: HashMap::new(),
                },
            ],
            ..GcpSourceConfig::default()
        }
    }

    #[test]
    fn gcp_maps_its_three_credential_paths_onto_three_modes() {
        let b = only(gcp().instances().unwrap());
        assert_eq!(b.connection_id, "gcp");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("gcp".into()));
        assert_eq!(i.topic, "gcp");
        assert_eq!(
            i.auth.mode,
            AuthKind::JwtBearer,
            "a key file is the JWT grant"
        );
        assert_eq!(
            i.auth.service_account_key_file.as_ref().unwrap().expose(),
            "/etc/gcp/sa-key.json"
        );
        assert!(i.auth.service_account_key.is_none() && i.auth.token.is_none());
        assert_eq!(i.vars["project_id"], "proj");
        assert_eq!(i.vars.len(), 1, "the public endpoints apply");
        assert!(i.units["admin_activity"].enabled && i.units["cloud_logging"].enabled);
        assert!(!i.units["scc"].enabled && !i.units["data_access"].enabled);
        assert!(
            i.units["cloud_logging"].vars.is_empty(),
            "no knob: the profile's clause applies"
        );

        let mut cfg = gcp();
        cfg.credential_secret = Some("env:GCP_TOKEN".into());
        cfg.api_url_override = Some("http://127.0.0.1:1".into());
        cfg.token_url_override = Some("http://127.0.0.1:2/token".into());
        cfg.services[1]
            .config
            .insert("filter".into(), Value::String("severity >= ERROR".into()));
        cfg.services.push(GcpService {
            name: "scc".into(),
            config: [("organization_id".to_string(), json!("123"))]
                .into_iter()
                .collect(),
        });
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(
            i.auth.mode,
            AuthKind::Bearer,
            "credential_secret is a resolved token and wins"
        );
        assert_eq!(i.auth.token.as_ref().unwrap().expose(), "env:GCP_TOKEN");
        assert_eq!(i.vars["logging_url"], "http://127.0.0.1:1");
        assert_eq!(
            i.vars["scc_url"], "http://127.0.0.1:1",
            "one override for both hosts"
        );
        assert_eq!(i.vars["token_url"], "http://127.0.0.1:2/token");
        assert_eq!(i.vars["organization_id"], "123");
        assert_eq!(
            i.units["cloud_logging"].vars["log_filter"],
            "severity >= ERROR"
        );
        assert!(i.units["scc"].enabled);

        let mut cfg = gcp();
        cfg.service_account_key = None;
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(
            i.auth.mode,
            AuthKind::GceMetadata,
            "no token and no key: the metadata server"
        );

        let mut cfg = gcp();
        cfg.project_id = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.gcp")
                && err.contains("project_id")
                && err.contains("admin_activity"),
            "{err}"
        );
        let mut cfg = gcp();
        cfg.services = vec![GcpService {
            name: "scc".into(),
            config: HashMap::new(),
        }];
        let err = cfg.instances().unwrap_err().to_string();
        assert!(err.contains("organization_id"), "{err}");
        cfg.project_id = None;
        cfg.services[0]
            .config
            .insert("organization_id".into(), json!("123"));
        assert!(cfg.instances().is_ok(), "scc needs no project");
    }

    /// A Secret set as an env var carries the key JSON itself, and every block
    /// reads a `service_account_key` that opens with `{` as the key, not as a path.
    #[test]
    fn a_service_account_key_holding_the_key_json_is_the_key_not_its_path() {
        let key = "\n  {\"type\":\"service_account\",\"client_email\":\"sa@proj.iam.gserviceaccount.com\"}";

        let mut gcp = gcp();
        gcp.service_account_key = Some(key.into());
        let pubsub = GcpPubsubSourceConfig {
            enabled: true,
            service_account_key: Some(key.into()),
            ..GcpPubsubSourceConfig::default()
        };
        let mut workspace = workspace();
        workspace.credential_secret = None;
        workspace.service_account_key = Some(key.into());

        for (block, auth) in [
            ("gcp", only(gcp.instances().unwrap()).instance.auth),
            (
                "gcp_pubsub",
                only(pubsub.instances().unwrap()).instance.auth,
            ),
            (
                "google_workspace",
                only(workspace.instances().unwrap()).instance.auth,
            ),
        ] {
            assert_eq!(auth.mode, AuthKind::JwtBearer, "{block}");
            assert_eq!(
                auth.service_account_key
                    .as_ref()
                    .map(SensitiveString::expose),
                Some(key),
                "{block}: the value is the key"
            );
            assert!(
                auth.service_account_key_file.is_none(),
                "{block}: the key JSON is not a path"
            );
        }
    }

    fn aws() -> AwsSourceConfig {
        AwsSourceConfig {
            enabled: true,
            region: "ap-southeast-2".into(),
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".into()),
            secret_access_key: Some("env:AWS_SECRET".to_string().into()),
            services: vec![
                AwsService {
                    name: "cloudtrail".into(),
                    config: HashMap::new(),
                },
                AwsService {
                    name: "cloudwatch_logs".into(),
                    config: [
                        ("log_group_name".to_string(), json!("/aws/lambda/x")),
                        ("filter_pattern".to_string(), json!("ERROR")),
                    ]
                    .into_iter()
                    .collect(),
                },
                AwsService {
                    name: "cloudwatch_metrics".into(),
                    config: [
                        ("namespaces".to_string(), json!(["AWS/EC2", "AWS/RDS"])),
                        ("metric_names".to_string(), json!(["CPUUtilization"])),
                        ("period_secs".to_string(), json!(60)),
                        ("stat".to_string(), json!("Maximum")),
                        ("output_format".to_string(), json!("otlp")),
                    ]
                    .into_iter()
                    .collect(),
                },
                AwsService {
                    name: "inspector".into(),
                    config: [("max_results".to_string(), json!(500))]
                        .into_iter()
                        .collect(),
                },
            ],
            ..AwsSourceConfig::default()
        }
    }

    #[test]
    fn aws_becomes_a_sigv4_instance_with_each_services_knobs_on_its_unit() {
        let b = only(aws().instances().unwrap());
        assert_eq!(b.connection_id, "aws");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("aws".into()));
        assert_eq!(i.topic, "aws");
        assert_eq!(i.auth.mode, AuthKind::SigV4);
        assert_eq!(
            i.auth.access_key_id.as_ref().unwrap().expose(),
            "AKIAIOSFODNN7EXAMPLE"
        );
        assert_eq!(
            i.auth.secret_access_key.as_ref().unwrap().expose(),
            "env:AWS_SECRET",
            "a spec, resolved on first use"
        );
        assert!(i.auth.credentials_json.is_none());
        assert!(
            i.auth.assume_role_arn.is_none(),
            "no role unless the block names one"
        );
        assert_eq!(i.vars["region"], "ap-southeast-2");
        assert_eq!(i.vars.len(), 1, "no endpoint override: the public hosts");

        let mut cfg = aws();
        cfg.assume_role_arn = Some("arn:aws:iam::123456789012:role/dfe-reader".into());
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(
            i.auth.assume_role_arn.as_deref(),
            Some("arn:aws:iam::123456789012:role/dfe-reader"),
            "the role rides on the instance's identity and the signer assumes it"
        );
        cfg.assume_role_arn = Some("  ".into());
        assert!(
            only(cfg.instances().unwrap())
                .instance
                .auth
                .assume_role_arn
                .is_none(),
            "a blank ARN is no role"
        );
        assert!(i.units["cloudtrail"].enabled && i.units["inspector"].enabled);
        assert!(!i.units["guardduty"].enabled && !i.units["health"].enabled);
        assert!(i.units["cloudtrail"].vars.is_empty());
        let logs = &i.units["cloudwatch_logs"].vars;
        assert_eq!(logs["log_group_name"], "/aws/lambda/x");
        assert_eq!(logs["filter_pattern"], "ERROR");
        let metrics = &i.units["cloudwatch_metrics"].vars;
        assert_eq!(
            metrics["metric_filters"],
            json!([
                {"Namespace": "AWS/EC2", "MetricName": "CPUUtilization"},
                {"Namespace": "AWS/RDS", "MetricName": "CPUUtilization"}
            ]),
            "namespaces crossed with the metric names"
        );
        assert_eq!(metrics["period_secs"], 60);
        assert_eq!(metrics["stat"], "Maximum");
        assert_eq!(metrics["output_format"], "otlp");
        assert_eq!(
            i.units["inspector"].vars["max_results"], 100,
            "capped at the API's maximum"
        );

        let mut cfg = aws();
        cfg.credential_secret = Some("vault:kv/data/aws:credentials".into());
        cfg.endpoint_override = Some("http://127.0.0.1:4566".into());
        cfg.services[2].config.remove("metric_names");
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(
            i.auth.credentials_json.as_ref().unwrap().expose(),
            "vault:kv/data/aws:credentials",
            "the credentials document wins over the pair"
        );
        assert!(i.auth.access_key_id.is_none() && i.auth.secret_access_key.is_none());
        assert_eq!(i.vars["endpoint_url"], "http://127.0.0.1:4566");
        assert_eq!(
            i.units["cloudwatch_metrics"].vars["metric_filters"],
            json!([{"Namespace": "AWS/EC2"}, {"Namespace": "AWS/RDS"}]),
            "no names: one filter per namespace"
        );

        let mut cfg = aws();
        cfg.access_key_id = None;
        cfg.secret_access_key = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.aws") && err.contains("access_key_id"),
            "{err}"
        );
        let mut cfg = aws();
        cfg.services[1].config.remove("log_group_name");
        let err = cfg.instances().unwrap_err().to_string();
        assert!(err.contains("log_group_name"), "{err}");
        let mut cfg = aws();
        cfg.services[2].config.clear();
        let err = cfg.instances().unwrap_err().to_string();
        assert!(err.contains("namespaces"), "{err}");

        let mut cfg = aws();
        cfg.access_key_id = None;
        cfg.secret_access_key = None;
        cfg.connections = vec![
            AwsConnection {
                id: "acct-123".into(),
                region: Some("eu-west-1".into()),
                credential_secret: Some("vault:kv/data/aws-123:creds".into()),
                ..AwsConnection::default()
            },
            AwsConnection {
                id: "acct-456".into(),
                access_key_id: Some("AKIA456".into()),
                secret_access_key: Some("s456".to_string().into()),
                ..AwsConnection::default()
            },
        ];
        let built = cfg.instances().unwrap();
        assert_eq!(built.len(), 2);
        assert_eq!(built[0].connection_id, "acct-123");
        assert_eq!(built[0].instance.vars["region"], "eu-west-1");
        assert!(built[0].instance.auth.credentials_json.is_some());
        assert_eq!(built[1].instance.vars["region"], "ap-southeast-2");
        assert_eq!(
            built[1]
                .instance
                .auth
                .access_key_id
                .as_ref()
                .unwrap()
                .expose(),
            "AKIA456"
        );
    }

    /// Security Hub's unit carries the listed workflow statuses, carries none
    /// when the knob is absent so every status is fetched, and refuses at load a
    /// status the API would refuse on every tick.
    #[test]
    fn securityhub_workflow_status_reaches_its_unit_and_an_unknown_one_is_refused() {
        let with = |statuses: Value| {
            let mut cfg = aws();
            cfg.services.push(AwsService {
                name: "securityhub".into(),
                config: [("workflow_status".to_string(), statuses)]
                    .into_iter()
                    .collect(),
            });
            cfg
        };

        let i = only(with(json!(["RESOLVED", "NOTIFIED"])).instances().unwrap()).instance;
        assert!(i.units["securityhub"].enabled);
        assert_eq!(
            i.units["securityhub"].vars["workflow_status_filter"],
            json!([
                {"Value": "RESOLVED", "Comparison": "EQUALS"},
                {"Value": "NOTIFIED", "Comparison": "EQUALS"}
            ])
        );

        let mut cfg = aws();
        cfg.services.push(AwsService {
            name: "securityhub".into(),
            config: HashMap::new(),
        });
        let i = only(cfg.instances().unwrap()).instance;
        assert!(
            !i.units["securityhub"]
                .vars
                .contains_key("workflow_status_filter"),
            "no knob leaves the profile's empty list: every status"
        );

        let err = with(json!(["NEW", "CLOSED"]))
            .instances()
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("workflow_status") && err.contains("CLOSED"),
            "{err}"
        );
    }

    fn workspace() -> GoogleWorkspaceSourceConfig {
        GoogleWorkspaceSourceConfig {
            enabled: true,
            credential_secret: Some("vault:kv/data/google_workspace:sa_key".into()),
            admin_email: Some("audit-admin@example.com".into()),
            services: vec![
                GoogleWorkspaceService {
                    name: "login".into(),
                    config: HashMap::new(),
                },
                GoogleWorkspaceService {
                    name: "drive".into(),
                    config: [("event_name".to_string(), json!("download"))]
                        .into_iter()
                        .collect(),
                },
            ],
            ..GoogleWorkspaceSourceConfig::default()
        }
    }

    #[test]
    fn google_workspace_becomes_a_delegated_jwt_instance_with_one_unit_per_application() {
        let b = only(workspace().instances().unwrap());
        assert_eq!(b.connection_id, "google_workspace");
        let i = &b.instance;
        assert_eq!(i.profile, ProfileRef::Named("google_workspace".into()));
        assert_eq!(i.topic, "google_workspace");
        assert_eq!(i.auth.mode, AuthKind::JwtBearer);
        assert_eq!(
            i.auth.service_account_key.as_ref().unwrap().expose(),
            "vault:kv/data/google_workspace:sa_key",
            "credential_secret is the key JSON"
        );
        assert!(i.auth.service_account_key_file.is_none());
        assert_eq!(i.vars["admin_email"], "audit-admin@example.com");
        assert_eq!(i.vars.len(), 1, "my_customer and the public endpoint apply");
        assert!(i.units["login"].enabled && i.units["drive"].enabled);
        assert!(!i.units["admin"].enabled && !i.units["calendar"].enabled);
        assert_eq!(i.units["drive"].vars["event_name"], "download");
        assert!(i.units["login"].vars.is_empty());

        let mut cfg = workspace();
        cfg.credential_secret = None;
        cfg.service_account_key = Some("/etc/workspace/sa-key.json".into());
        cfg.customer_id = Some("C0123abcd".into());
        cfg.api_url_override = Some("http://127.0.0.1:1".into());
        cfg.token_url_override = Some("http://127.0.0.1:2/token".into());
        let i = only(cfg.instances().unwrap()).instance;
        assert_eq!(
            i.auth.service_account_key_file.as_ref().unwrap().expose(),
            "/etc/workspace/sa-key.json"
        );
        assert_eq!(i.vars["customer_id"], "C0123abcd");
        assert_eq!(i.vars["api_url"], "http://127.0.0.1:1");
        assert_eq!(i.vars["token_url"], "http://127.0.0.1:2/token");

        let mut cfg = workspace();
        cfg.admin_email = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("sources.google_workspace") && err.contains("admin_email"),
            "{err}"
        );
        let mut cfg = workspace();
        cfg.credential_secret = None;
        let err = cfg.instances().unwrap_err().to_string();
        assert!(
            err.contains("service_account_key") && err.contains("credential_secret"),
            "{err}"
        );
    }

    #[test]
    fn sources_lists_every_enabled_builtin_and_skips_disabled_blocks() {
        assert!(
            SourcesConfig::default()
                .builtin_instances()
                .unwrap()
                .is_empty()
        );
        let mut sources = SourcesConfig {
            github: github(),
            okta: OktaSourceConfig {
                enabled: false,
                ..okta()
            },
            ..SourcesConfig::default()
        };
        let ids: Vec<String> = sources
            .builtin_instances()
            .unwrap()
            .into_iter()
            .map(|b| b.connection_id)
            .collect();
        assert_eq!(ids, ["github"]);
        sources.okta.enabled = true;
        let ids: Vec<String> = sources
            .builtin_instances()
            .unwrap()
            .into_iter()
            .map(|b| b.connection_id)
            .collect();
        assert_eq!(ids, ["github", "okta"]);
        sources.github.org = None;
        let err = sources.builtin_instances().unwrap_err().to_string();
        assert!(err.contains("sources.github:"), "the block is named: {err}");
    }

    #[test]
    fn the_framework_filter_resolves_builtin_connection_ids_to_the_type_wide_filter() {
        let mut sources = SourcesConfig {
            github: github(),
            okta: OktaSourceConfig {
                filter: Some("eventType != \"x\"".into()),
                ..okta()
            },
            ..SourcesConfig::default()
        };
        assert_eq!(
            sources.filter_for_source("github"),
            Some("action != \"git.clone\"")
        );
        assert_eq!(
            sources.filter_for_source("okta"),
            Some("eventType != \"x\"")
        );
        assert_eq!(sources.filter_for_source("nope"), None);
        sources.github.org = None;
        sources.github.connections = vec![GithubConnection {
            id: "gh-acme".into(),
            org: Some("acme".into()),
            ..GithubConnection::default()
        }];
        assert_eq!(
            sources.filter_for_source("gh-acme"),
            Some("action != \"git.clone\"")
        );
        assert_eq!(
            sources.filter_for_source("github"),
            None,
            "no implicit connection now"
        );
    }

    #[test]
    fn the_scheduled_set_is_every_enabled_builtin_connection_plus_the_enabled_entries() {
        let mut sources = SourcesConfig {
            github: GithubSourceConfig {
                org: None,
                connections: vec![
                    GithubConnection {
                        id: "gh-acme".into(),
                        org: Some("acme".into()),
                        ..GithubConnection::default()
                    },
                    GithubConnection {
                        id: "gh-beta".into(),
                        org: Some("beta".into()),
                        ..GithubConnection::default()
                    },
                ],
                ..github()
            },
            okta: OktaSourceConfig {
                enabled: false,
                ..okta()
            },
            ..SourcesConfig::default()
        };
        for (id, enabled) in [("rest-on", true), ("rest-off", false)] {
            sources.rest.insert(
                id.into(),
                dfe_fetcher_rest::RestInstance {
                    enabled,
                    ..dfe_fetcher_rest::RestInstance::default()
                },
            );
        }
        for (id, enabled) in [("db-on", true), ("db-off", false)] {
            sources.db.insert(
                id.into(),
                dfe_fetcher_db::DbInstance {
                    enabled,
                    ..dfe_fetcher_db::DbInstance::default()
                },
            );
        }
        for (id, enabled) in [("file-on", true), ("file-off", false)] {
            sources.file.insert(
                id.into(),
                dfe_fetcher_file::FileInstance {
                    enabled,
                    ..dfe_fetcher_file::FileInstance::default()
                },
            );
        }

        // The two halves must stay in step: a set that under-reports would
        // cancel a source the config still schedules.
        let mut expected: std::collections::BTreeSet<String> = sources
            .builtin_instances()
            .unwrap()
            .into_iter()
            .map(|b| b.connection_id)
            .collect();
        assert_eq!(
            expected,
            ["gh-acme".to_string(), "gh-beta".to_string()]
                .into_iter()
                .collect(),
            "the disabled block contributes nothing"
        );
        expected.extend(["rest-on".to_string(), "db-on".into(), "file-on".into()]);

        let scheduled: std::collections::BTreeSet<String> = sources
            .scheduled_connection_ids()
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert_eq!(scheduled, expected);
    }
}
