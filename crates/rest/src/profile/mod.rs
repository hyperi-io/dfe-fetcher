// Project:   dfe-fetcher
// File:      crates/rest/src/profile/mod.rs
// Purpose:   The declarative REST profile grammar and the instance that binds it
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The declarative REST profile grammar.
//!
//! A PROFILE is a shape and carries no identity: base URL template, the auth
//! modes the API accepts, headers, retry policy, where errors live, the window
//! format, and the endpoints with their decoder and pager. An INSTANCE is one
//! deployment of that shape with its own credential kind and refs, `vars`,
//! topic and interval; N instances of one profile is the normal case. Every
//! struct rejects unknown keys and every enum is a closed vocabulary, so a typo
//! or an unsupported mode is a load error carrying the YAML line, never a
//! setting that parses and does nothing.

pub mod bound;
pub mod template;

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use dfe_fetcher_core::UnitShape;
use dfe_fetcher_core::batch::AccumulateConfig;
use scalo::SensitiveString;
use serde::{Deserialize, Serialize};

pub use bound::{BoundEndpoint, BoundProfile, bind};
pub use template::{Predicate, Template, TemplateCtx, TemplateMap};

/// One validation finding: which field and what is wrong with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    /// Dotted path from the profile or instance root, e.g. `endpoints[2].rows.at`.
    pub field: String,
    /// What is wrong.
    pub message: String,
}

impl Issue {
    fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Issue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

/// The auth modes this build implements; a profile's `accepts` and an
/// instance's `mode` draw from the same vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthKind {
    /// No credential on the request.
    None,
    /// `Authorization: Bearer <token>` with a static token.
    Bearer,
    /// A key in a named header (with optional prefix) or query parameter.
    ApiKey,
    /// HTTP Basic.
    Basic,
    /// OAuth2 client-credentials exchange, cached until shortly before expiry.
    Oauth2ClientCredentials,
    /// Duo Admin API signing: HMAC-SHA1 over the request's date, method,
    /// host, path and sorted query, sent as a Basic credential of
    /// `integration_key:signature` with the same `Date` header.
    DuoHmac,
    /// OAuth2 JWT-bearer grant (RFC 7523): an RS256 assertion signed with the
    /// instance's private key, exchanged for a cached access token.
    JwtBearer,
    /// The GCE metadata server's token for the workload's service account,
    /// cached until shortly before expiry.
    GceMetadata,
    /// AWS Signature Version 4 over static keys: every request is signed
    /// for the service and region the profile's templates name, with the
    /// body's SHA-256 as the payload hash.
    #[serde(rename = "sigv4")]
    SigV4,
}

impl AuthKind {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            AuthKind::None => "none",
            AuthKind::Bearer => "bearer",
            AuthKind::ApiKey => "api_key",
            AuthKind::Basic => "basic",
            AuthKind::Oauth2ClientCredentials => "oauth2_client_credentials",
            AuthKind::DuoHmac => "duo_hmac",
            AuthKind::JwtBearer => "jwt_bearer",
            AuthKind::GceMetadata => "gce_metadata",
            AuthKind::SigV4 => "sigv4",
        }
    }

    /// Whether the mode mints a token for a scope, so a unit may ask for its
    /// own.
    #[must_use]
    pub const fn is_scoped(self) -> bool {
        matches!(
            self,
            AuthKind::Oauth2ClientCredentials | AuthKind::JwtBearer
        )
    }
}

/// Where an API key goes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ApiKeySpec {
    /// Header name carrying the key.
    pub header: Option<String>,
    /// Query parameter carrying the key.
    pub query: Option<String>,
    /// Text put in front of the key in a header, e.g. `SSWS `.
    pub prefix: String,
}

/// The OAuth2 client-credentials exchange as the API shapes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct OAuth2Spec {
    /// Token endpoint, a template (may use `base_url`).
    pub token_url: String,
    /// Scope sent with the exchange; omitted from the form when empty.
    pub scope: String,
    /// Lifetime assumed when the token response carries no `expires_in`.
    pub expires_in_fallback_secs: u64,
    /// How long before expiry the token is refreshed.
    pub early_refresh_secs: u64,
    /// Top-level fields of the token response the templates read as
    /// `auth.<name>` (Salesforce's `instance_url`); an allow-list, so a
    /// refresh or id token the response also carries never reaches a URL.
    pub expose: Vec<String>,
}

impl Default for OAuth2Spec {
    fn default() -> Self {
        Self {
            token_url: String::new(),
            scope: String::new(),
            expires_in_fallback_secs: 3600,
            early_refresh_secs: 60,
            expose: Vec::new(),
        }
    }
}

/// The JWT-bearer grant as the API shapes it: where the assertion is
/// exchanged, the claims it carries and how long it is valid.
///
/// Every claim value is a template over the instance's `vars` and `auth.*`,
/// where the authenticator exposes `client_email` and `token_uri` from a
/// service-account key and `token_url` once rendered; a claim that renders
/// empty is left out (an optional `sub`). `iat` and `exp` are set from
/// `ttl_secs` on every assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct JwtBearerSpec {
    /// Token endpoint, a template (may read `auth.token_uri` from the key).
    pub token_url: String,
    /// Claim templates: `iss`, `scope`, `aud`, an optional `sub`.
    pub claims: BTreeMap<String, String>,
    /// Assertion lifetime, `exp - iat`.
    pub ttl_secs: u64,
    /// Lifetime assumed when the token response carries no `expires_in`.
    pub expires_in_fallback_secs: u64,
    /// How long before expiry the token is refreshed.
    pub early_refresh_secs: u64,
    /// Top-level fields of the token response the templates read as
    /// `auth.<name>`, an allow-list as on `oauth2_client_credentials`.
    pub expose: Vec<String>,
}

impl Default for JwtBearerSpec {
    fn default() -> Self {
        Self {
            token_url: String::new(),
            claims: BTreeMap::new(),
            ttl_secs: 3600,
            expires_in_fallback_secs: 3600,
            early_refresh_secs: 60,
            expose: Vec::new(),
        }
    }
}

/// The GCE metadata token endpoint as the profile names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct GceMetadataSpec {
    /// The service account's token URL on the metadata server, a template.
    pub url: String,
    /// Lifetime assumed when the response carries no `expires_in`.
    pub expires_in_fallback_secs: u64,
    /// How long before expiry the token is refreshed.
    pub early_refresh_secs: u64,
}

impl Default for GceMetadataSpec {
    fn default() -> Self {
        Self {
            url: String::new(),
            expires_in_fallback_secs: 3600,
            early_refresh_secs: 60,
        }
    }
}

/// The SigV4 signing scope as the profile names it: both are templates
/// rendered per request from the unit's context, so a profile whose units
/// are different AWS services (and one region-locked unit) varies them with
/// `endpoints[].vars`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SigV4Spec {
    /// The service code of the credential scope (`cloudtrail`, `logs`,
    /// `monitoring`), a template.
    pub service: String,
    /// The region of the credential scope, a template.
    pub region: String,
}

/// What the API accepts and the shape of each mode; identity lives on the instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct AuthSpec {
    /// Modes an instance may pick.
    pub accepts: Vec<AuthKind>,
    /// Shape of the `api_key` mode.
    pub api_key: ApiKeySpec,
    /// Shape of the `oauth2_client_credentials` mode.
    pub oauth2_client_credentials: OAuth2Spec,
    /// Shape of the `jwt_bearer` mode.
    pub jwt_bearer: JwtBearerSpec,
    /// Shape of the `gce_metadata` mode.
    pub gce_metadata: GceMetadataSpec,
    /// Shape of the `sigv4` mode.
    pub sigv4: SigV4Spec,
}

impl Default for AuthSpec {
    fn default() -> Self {
        Self {
            accepts: vec![AuthKind::None],
            api_key: ApiKeySpec::default(),
            oauth2_client_credentials: OAuth2Spec::default(),
            jwt_bearer: JwtBearerSpec::default(),
            gce_metadata: GceMetadataSpec::default(),
            sigv4: SigV4Spec::default(),
        }
    }
}

/// One entry of `retry_on`: a status code or a whole class (`4xx`, `5xx`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusMatch {
    /// One status code.
    Code(u16),
    /// Every status in the hundreds class, e.g. `Class(5)` for 500-599.
    Class(u8),
}

impl StatusMatch {
    fn matches(self, status: u16) -> bool {
        match self {
            StatusMatch::Code(code) => code == status,
            StatusMatch::Class(class) => status / 100 == u16::from(class),
        }
    }
}

impl Serialize for StatusMatch {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            StatusMatch::Code(code) => s.serialize_u16(*code),
            StatusMatch::Class(class) => s.serialize_str(&format!("{class}xx")),
        }
    }
}

impl<'de> Deserialize<'de> for StatusMatch {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Code(u16),
            Text(String),
        }
        match Raw::deserialize(d)? {
            Raw::Code(code) if (100..600).contains(&code) => Ok(StatusMatch::Code(code)),
            Raw::Code(code) => Err(serde::de::Error::custom(format!(
                "{code} is not an HTTP status code"
            ))),
            Raw::Text(text) => match text.as_str() {
                "1xx" => Ok(StatusMatch::Class(1)),
                "2xx" => Ok(StatusMatch::Class(2)),
                "3xx" => Ok(StatusMatch::Class(3)),
                "4xx" => Ok(StatusMatch::Class(4)),
                "5xx" => Ok(StatusMatch::Class(5)),
                other => Err(serde::de::Error::custom(format!(
                    "retry status must be a code or one of 1xx..5xx, got `{other}`"
                ))),
            },
        }
    }
}

impl schemars::JsonSchema for StatusMatch {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "StatusMatch".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "An HTTP status code (100-599) or a class string such as \"5xx\"",
            "anyOf": [
                { "type": "integer", "minimum": 100, "maximum": 599 },
                { "type": "string", "pattern": "^[1-5]xx$" }
            ]
        })
    }
}

/// Retry policy for one profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RetrySpec {
    /// Attempts after the first.
    pub max_retries: u32,
    /// First backoff.
    pub min_backoff_ms: u64,
    /// Backoff ceiling.
    pub max_backoff_ms: u64,
    /// Statuses that are retried.
    pub retry_on: Vec<StatusMatch>,
    /// Statuses that are never retried, whatever `retry_on` says.
    pub never_retry: Vec<u16>,
    /// Honour `Retry-After` on a retried response.
    pub retry_after_header: bool,
    /// Retry POST as well as GET; off unless the endpoint dedupes.
    pub retry_non_idempotent: bool,
}

impl Default for RetrySpec {
    fn default() -> Self {
        Self {
            max_retries: 3,
            min_backoff_ms: 500,
            max_backoff_ms: 30_000,
            retry_on: vec![
                StatusMatch::Code(408),
                StatusMatch::Code(429),
                StatusMatch::Class(5),
            ],
            never_retry: vec![401, 403],
            retry_after_header: true,
            retry_non_idempotent: false,
        }
    }
}

impl RetrySpec {
    /// Whether a response status is retried.
    #[must_use]
    pub fn retries(&self, status: u16) -> bool {
        !self.never_retry.contains(&status) && self.retry_on.iter().any(|m| m.matches(status))
    }
}

/// Where the provider's error text lives in a non-2xx body.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ErrorSpec {
    /// JSON pointer to the error text; the whole body is used when unset.
    pub at: Option<String>,
}

/// Provider quota headers surfaced as gauges.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct QuotaSpec {
    /// Gauge name suffix -> response header name.
    pub headers: BTreeMap<String, String>,
}

/// How the window bounds are written into a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFormat {
    /// `2026-01-01T00:00:00Z`.
    Rfc3339Secs,
    /// `2026-01-01T00:00:00.000Z`.
    Rfc3339Millis,
    /// Seconds since the epoch.
    EpochSecs,
    /// Milliseconds since the epoch.
    EpochMillis,
    /// A chrono `strftime` pattern.
    Strftime(String),
}

impl Default for WindowFormat {
    fn default() -> Self {
        Self::Rfc3339Secs
    }
}

impl WindowFormat {
    /// Render one instant.
    #[must_use]
    pub fn format(&self, at: chrono::DateTime<chrono::Utc>) -> String {
        match self {
            WindowFormat::Rfc3339Secs => at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            WindowFormat::Rfc3339Millis => at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            WindowFormat::EpochSecs => at.timestamp().to_string(),
            WindowFormat::EpochMillis => at.timestamp_millis().to_string(),
            WindowFormat::Strftime(pattern) => at.format(pattern).to_string(),
        }
    }

    fn as_text(&self) -> String {
        match self {
            WindowFormat::Rfc3339Secs => "rfc3339_secs".into(),
            WindowFormat::Rfc3339Millis => "rfc3339_millis".into(),
            WindowFormat::EpochSecs => "epoch_secs".into(),
            WindowFormat::EpochMillis => "epoch_millis".into(),
            WindowFormat::Strftime(pattern) => format!("strftime:{pattern}"),
        }
    }
}

impl Serialize for WindowFormat {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.as_text())
    }
}

impl<'de> Deserialize<'de> for WindowFormat {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        match text.as_str() {
            "rfc3339_secs" => Ok(WindowFormat::Rfc3339Secs),
            "rfc3339_millis" => Ok(WindowFormat::Rfc3339Millis),
            "epoch_secs" => Ok(WindowFormat::EpochSecs),
            "epoch_millis" => Ok(WindowFormat::EpochMillis),
            other => match other.strip_prefix("strftime:") {
                Some(pattern) if !pattern.is_empty() => {
                    Ok(WindowFormat::Strftime(pattern.to_owned()))
                }
                _ => Err(serde::de::Error::custom(format!(
                    "window.format must be rfc3339_secs, rfc3339_millis, epoch_secs, \
                     epoch_millis or strftime:<pattern>, got `{other}`"
                ))),
            },
        }
    }
}

impl schemars::JsonSchema for WindowFormat {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "WindowFormat".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "rfc3339_secs | rfc3339_millis | epoch_secs | epoch_millis | strftime:<pattern>",
            "type": "string"
        })
    }
}

/// Parse `<n>s|m|h|d` into a duration.
///
/// # Errors
///
/// Returns a message for a missing unit, a non-numeric count or zero.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let (digits, unit) = text.split_at(text.trim_end_matches(char::is_alphabetic).len());
    let count: u64 = digits
        .parse()
        .map_err(|_| format!("duration `{text}` must be <number><s|m|h|d>"))?;
    let secs = match unit {
        "s" => count,
        "m" => count * 60,
        "h" => count * 3600,
        "d" => count * 86_400,
        _ => return Err(format!("duration `{text}` must end in s, m, h or d")),
    };
    if secs == 0 {
        return Err(format!("duration `{text}` must be greater than zero"));
    }
    Ok(Duration::from_secs(secs))
}

/// A duration written as `<n>s|m|h|d`, validated on load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationText(pub Duration);

impl Serialize for DurationText {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{}s", self.0.as_secs()))
    }
}

impl<'de> Deserialize<'de> for DurationText {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        parse_duration(&text)
            .map(DurationText)
            .map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for DurationText {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "DurationText".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "A duration such as 90s, 15m, 24h or 2d",
            "type": "string",
            "pattern": "^[0-9]+[smhd]$"
        })
    }
}

/// How the scheduler's window is written into requests and chunked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WindowSpec {
    /// How `window.start` and `window.end` are rendered.
    pub format: WindowFormat,
    /// Split the window into steps of at most this length, one page sequence each.
    pub step: Option<DurationText>,
    /// Window used when the scheduler passes none.
    pub lookback: DurationText,
}

impl Default for WindowSpec {
    fn default() -> Self {
        Self {
            format: WindowFormat::default(),
            step: None,
            lookback: DurationText(Duration::from_secs(3600)),
        }
    }
}

/// Framing of a response body into rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DecoderKind {
    /// A top-level JSON array, streamed element by element.
    JsonArray,
    /// One JSON object per line, streamed; a missing trailing newline and an
    /// empty body are both fine.
    Ndjson,
    /// A wrapped page read whole, rows at the `at` pointer.
    JsonAt,
    /// The whole body is one row.
    Document,
    /// A whole JSON document read whole: a top-level array is one row per
    /// element, anything else is one row (an object export that may be
    /// either).
    Json,
    /// Plain text, one row per line wrapped as `{"line": ...}`.
    Lines,
    /// CSV with an optional header row, one object per record.
    Csv,
}

impl DecoderKind {
    /// Whether this decoder needs the whole page in memory.
    #[must_use]
    pub const fn is_page_bounded(self) -> bool {
        matches!(
            self,
            DecoderKind::JsonAt | DecoderKind::Document | DecoderKind::Json | DecoderKind::Csv
        )
    }
}

/// A row builder: the shape transform no decoder expresses, applied to each
/// framed row and yielding the rows it holds, or (a folding builder) to the
/// rows of one key of a unit, yielding the one row they fold into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RowBuilderKind {
    /// A columnar table (`{columns: [{name}], rows: [[...]]}`, the Log
    /// Analytics result shape) becomes one object per row keyed by column.
    ColumnarTable,
    /// A CloudWatch `GetMetricData` response joined with the metric
    /// descriptors the lookup asked for: one JSON row per datapoint, or one
    /// OTLP protobuf per response when `vars.output_format` is `otlp`.
    CloudwatchMetrics,
    /// A fold: the `.info` documents of one Go module's versions become the
    /// one row per module the proxy source emits (`versions` in list order,
    /// `version_info` keyed by version).
    GoModuleAggregate,
    /// A row that is not a JSON object is wrapped as one: a JSON scalar or
    /// array under `payload`, text that is not JSON under
    /// `_dfe_fetcher_raw_line` with the parse error beside it.
    WrapNonObject,
    /// A Pub/Sub `receivedMessages[]` entry becomes the record its base64
    /// `message.data` holds (JSON as itself, anything else as `{"data"}`),
    /// with the message envelope under `_dfe_fetcher_pubsub`.
    PubsubMessage,
}

impl RowBuilderKind {
    /// Whether the builder folds a key's rows into one rather than
    /// expanding each framed row; named under `fold`, never `rows.builder`.
    #[must_use]
    pub const fn folds(self) -> bool {
        matches!(self, RowBuilderKind::GoModuleAggregate)
    }
}

/// A lister: a listing protocol no decoder expresses, standing where the
/// unit's page fetch would be and yielding the listing as one page of items
/// for the unit's manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ListerKind {
    /// S3 `ListObjectsV2`: the XML pages under `path` (the bucket, with the
    /// `list-type`, `prefix` and `max-keys` query the unit names) walked on
    /// `NextContinuationToken` up to `max_pages`, the keys modified after
    /// the unit's checkpoint sorted by `last_modified`, each an item
    /// `{key, path, last_modified, size}` (`path` is the key encoded for a
    /// URL path).
    S3,
}

/// Where the rows are in a response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RowsSpec {
    /// The framing.
    pub decoder: DecoderKind,
    /// JSON pointer to the row array (`json_at` only).
    pub at: Option<String>,
    /// The body is a gzip member sequence to inflate before framing (a
    /// `Content-Encoding: gzip` response is inflated by the client regardless).
    pub gzip: bool,
    /// The first CSV record is a header naming the columns.
    pub header: bool,
    /// Each framed row is a JSON string whose content is the row (AWS
    /// Config's `Results`); it is unquoted before it is a row.
    pub quoted: bool,
    /// A transform each framed row goes through before it is a row.
    pub builder: Option<RowBuilderKind>,
    /// What the rows are made of, `json` or `binary`, as a template rendered
    /// once when the unit is bound (so a builder whose output a var decides
    /// can say `{{ vars.output_format == 'otlp' ? 'binary' : 'json' }}`); a
    /// binary row is emitted verbatim, never enriched, filtered, routed or
    /// unwrapped, and a dump unit cannot be binary.
    pub content: String,
}

impl Default for RowsSpec {
    fn default() -> Self {
        Self {
            decoder: DecoderKind::JsonArray,
            at: None,
            gzip: false,
            header: true,
            quoted: false,
            builder: None,
            content: "json".to_owned(),
        }
    }
}

/// How the next page is found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PagerStrategy {
    /// One page.
    None,
    /// RFC 5988 `Link: <url>; rel="next"`.
    LinkHeader,
    /// A token read from the body or a header, injected into the next request.
    Cursor,
    /// A page number parameter, optionally bounded by a total in the body.
    PageNumber,
    /// An offset parameter advanced by the page size, optionally bounded by a total.
    Offset,
    /// The next request's full URL read from the body or a header.
    RequestPath,
}

/// Pagination as data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PaginateSpec {
    /// The strategy.
    pub strategy: PagerStrategy,
    /// Where the token or URL comes from: `body:/pointer` or `header:Name`.
    pub from: Option<String>,
    /// Where the token goes on the next request: `query:name`, `body:/pointer`
    /// (written into the rendered body), `body_replace:/pointer` (the next
    /// body is the token and nothing else) or `path`.
    pub into: Option<String>,
    /// CEL over `body` and `headers`; true stops after this page.
    pub stop_when: Option<String>,
    /// JSON pointer to the total page count (`page_number`).
    pub total_pages_at: Option<String>,
    /// JSON pointer to the total row count (`offset`).
    pub total_at: Option<String>,
    /// Rows per page (`offset`): a shorter page is the last. Optional when
    /// `total_at` is set; the offset itself advances by the rows received.
    pub page_size: Option<u64>,
    /// Query parameter carrying the page number or offset.
    pub param: Option<String>,
    /// First page number (`page_number`) or first offset (`offset`).
    pub start: Option<u64>,
    /// Base URL a relative `request_path` value is resolved against.
    pub base: Option<String>,
}

impl Default for PaginateSpec {
    fn default() -> Self {
        Self {
            strategy: PagerStrategy::None,
            from: None,
            into: None,
            stop_when: None,
            total_pages_at: None,
            total_at: None,
            page_size: None,
            param: None,
            start: None,
            base: None,
        }
    }
}

impl PaginateSpec {
    /// Whether advancing needs the page body as a tree.
    #[must_use]
    pub fn reads_body(&self) -> bool {
        let from_body = self.from.as_deref().is_some_and(|f| f.starts_with("body:"));
        let stops_on_body = self
            .stop_when
            .as_deref()
            .is_some_and(|s| s.contains("body"));
        from_body || stops_on_body || self.total_pages_at.is_some() || self.total_at.is_some()
    }
}

/// HTTP method of an endpoint.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "UPPERCASE")]
pub enum Method {
    /// GET, idempotent, retried by default.
    #[default]
    Get,
    /// POST, retried only with `retry.retry_non_idempotent`.
    Post,
}

/// One request sequence per key of a list: the unit's templates and its
/// `add_fields` read the current key as `{{ key }}`. The keys come from a
/// template over the instance's vars (`from`) or from one request the unit
/// sends first (`request` with the keys at `keys_at`, GuardDuty's detector
/// ids).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct KeysetSpec {
    /// A template rendering the list of keys, e.g. `{{ vars.packages }}`.
    pub from: Option<String>,
    /// The request whose response carries the keys.
    pub request: Option<LookupRequest>,
    /// JSON pointer to the key array in that response.
    pub keys_at: Option<String>,
}

/// A secondary request of a unit: what a lookup sends for one batch of ids
/// (`{{ ids }}` is the batch), what a keyset sends for its keys, what a
/// manifest sends per item (`{{ item }}`), or one prelude step.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct LookupRequest {
    /// GET or POST; unset takes the construct's default (POST for a lookup
    /// batch, a keyset request and a prelude step, GET for a manifest item).
    pub method: Option<Method>,
    /// Path template appended to `base_url`, or a whole URL when it renders
    /// one (a manifest item's `contentUri`).
    pub path: String,
    /// Query parameter templates.
    pub query: BTreeMap<String, String>,
    /// Header templates.
    pub headers: BTreeMap<String, String>,
    /// JSON body; a leaf that is one expression keeps its type, so
    /// `"{{ ids }}"` is the id list.
    pub body: Option<serde_json::Value>,
    /// Non-2xx statuses that are not failures: a lookup or keyset answers
    /// an empty page, a manifest item yields no rows, a prelude step
    /// carries on (the 400 an already-enabled OMAP subscription answers).
    pub ignore_status: Vec<u16>,
    /// Total seconds one of these requests may take, body included; only
    /// for a request whose rows are read whole (a streaming decoder is
    /// bounded per read by the client, never in total).
    pub timeout_secs: Option<u64>,
}

impl LookupRequest {
    /// The method this request sends: its own, else `default`.
    #[must_use]
    pub fn method_or(&self, default: Method) -> Method {
        self.method.unwrap_or(default)
    }
}

/// A manifest: the endpoint's rows are POINTERS to content, and the rows
/// of the unit are what one request per pointer returns (the OMAP content
/// list, whose items name blobs of records). Each framed row of the pages
/// is an item the templates read as `{{ item }}`. With `key` and
/// `position` set, every row of an item carries an item checkpoint mark
/// that the driver commits once the row is acknowledged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ManifestSpec {
    /// The request sent per item; GET by default.
    pub item_request: LookupRequest,
    /// Row framing of an item's response.
    pub rows: RowsSpec,
    /// Template of the item's identity for its checkpoint mark.
    pub key: Option<String>,
    /// Template of the item's RFC 3339 position in the listing order for
    /// its checkpoint mark; set together with `key`.
    pub position: Option<String>,
    /// Items fetched per key per tick; the rest wait for the next tick
    /// (their checkpoint has not moved past them).
    pub max_items: Option<u32>,
    /// Fields added to every row of an item, rendered per item: a value is
    /// a template, or a JSON object or array whose string leaves are
    /// templates, and may read `item`.
    pub add_fields: BTreeMap<String, serde_json::Value>,
}

/// A queue: the unit's pages are pulled messages, each row carrying the
/// ack id at `ack_at`, and `ack_request` is sent for each batch of ids the
/// driver hands back once the batch is delivered (`{{ ids }}` is the
/// batch), so a message is acknowledged only after it is on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct QueueSpec {
    /// JSON pointer into each framed row to its ack id.
    pub ack_at: String,
    /// The acknowledgement request; POST by default, `ids` is the batch.
    pub ack_request: LookupRequest,
    /// Ids per acknowledgement request.
    pub ack_batch: usize,
}

/// Default ack ids per acknowledgement request.
pub const DEFAULT_ACK_BATCH: usize = 500;

impl Default for QueueSpec {
    fn default() -> Self {
        Self {
            ack_at: String::new(),
            ack_request: LookupRequest::default(),
            ack_batch: DEFAULT_ACK_BATCH,
        }
    }
}

/// Default ids per lookup request.
pub const DEFAULT_LOOKUP_BATCH: usize = 1000;

/// A two-stage unit: the endpoint's pages carry ids, and the rows are the
/// entities a second request returns for each batch of them. A batch's
/// response may page like a first-stage one (`paginate`, `max_pages`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct LookupSpec {
    /// JSON pointer into each first-stage row to its id; unset when the row
    /// is the id itself (a bare string in the page's array).
    pub id_at: Option<String>,
    /// Ids per lookup request.
    pub batch: usize,
    /// The lookup request.
    pub request: LookupRequest,
    /// Row framing of the lookup response.
    pub rows: RowsSpec,
    /// Pagination of one batch's response; one page when unset.
    pub paginate: Option<PaginateSpec>,
    /// Page ceiling per batch.
    pub max_pages: Option<u32>,
}

impl Default for LookupSpec {
    fn default() -> Self {
        Self {
            id_at: None,
            batch: DEFAULT_LOOKUP_BATCH,
            request: LookupRequest::default(),
            rows: RowsSpec::default(),
            paginate: None,
            max_pages: None,
        }
    }
}

/// A shape within the shape: how one unit's requests are multiplied. A
/// unit takes at most one of `lookup`, `manifest` and `queue`, the first
/// two being the second request its pages point at and the third the
/// acknowledgement its rows earn.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ConstructSpec {
    /// One page sequence per key of a list.
    pub keyset: Option<KeysetSpec>,
    /// A second request per batch of ids the pages yield.
    pub lookup: Option<LookupSpec>,
    /// A second request per item the pages yield, whose rows are the
    /// unit's.
    pub manifest: Option<ManifestSpec>,
    /// The pages are pulled messages acknowledged after delivery.
    pub queue: Option<QueueSpec>,
}

/// Values every endpoint inherits unless it sets its own.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EndpointDefaults {
    /// HTTP method of every endpoint that sets none.
    pub method: Option<Method>,
    /// Path template of every endpoint that sets none; `{{ unit.name }}` is
    /// how one template serves many units.
    pub path: Option<String>,
    /// Query parameters merged under each endpoint's own.
    pub query: BTreeMap<String, String>,
    /// Headers merged under each endpoint's own.
    pub headers: BTreeMap<String, String>,
    /// Body of every POST endpoint that sets none; a GET never inherits it.
    pub body: Option<serde_json::Value>,
    /// Row framing.
    pub rows: Option<RowsSpec>,
    /// Pagination.
    pub paginate: Option<PaginateSpec>,
    /// Page ceiling per tick.
    pub max_pages: Option<u32>,
    /// Bytes a page-bounded decoder may buffer.
    pub max_page_bytes: Option<usize>,
    /// Prelude of every endpoint that sets none; an endpoint's own
    /// `prelude: []` opts out.
    pub prelude: Option<Vec<LookupRequest>>,
    /// Construct of every endpoint that sets none, replaced whole by an
    /// endpoint's own; an endpoint's own `construct: {}` opts out.
    pub construct: Option<ConstructSpec>,
}

/// What a unit needs of the credential beyond the instance's mode.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EndpointAuthSpec {
    /// The scope (OAuth2) or `scope` claim (JWT bearer) this unit's token is
    /// minted for, when its API audience differs from the profile's; units
    /// with the same scope share one token.
    pub scope: Option<String>,
}

/// How one unit writes the window when its API differs from the profile's
/// (CloudWatch Logs takes milliseconds where CloudTrail takes seconds).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EndpointWindowSpec {
    /// How this unit's `window.start` and `window.end` are rendered.
    pub format: Option<WindowFormat>,
}

/// One endpoint of a profile: one unit of the source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EndpointSpec {
    /// Unit name: `_source_fetcher` suffix, dump `store` suffix, topic suffix.
    pub unit: String,
    /// Overrides the profile's shape for this unit.
    pub shape: Option<UnitShape>,
    /// Base URL template of this unit when its API host differs from the
    /// profile's; validated like the profile's.
    pub base_url: Option<String>,
    /// The credential narrowing this unit needs.
    pub auth: Option<EndpointAuthSpec>,
    /// The window rendering this unit needs when it differs from the
    /// profile's.
    pub window: Option<EndpointWindowSpec>,
    /// Values this unit's templates read as `vars.*`, over the instance's
    /// and under the instance's `units.<name>.vars`.
    pub vars: BTreeMap<String, serde_json::Value>,
    /// GET or POST; the defaults' when unset, else GET.
    pub method: Option<Method>,
    /// Path template appended to `base_url`; the defaults' when unset.
    pub path: String,
    /// Query parameter templates; an empty rendering is omitted.
    pub query: BTreeMap<String, String>,
    /// Header templates.
    pub headers: BTreeMap<String, String>,
    /// JSON body for POST; string leaves are templates, a leaf that is one
    /// expression keeps its type. The defaults' when unset.
    pub body: Option<serde_json::Value>,
    /// Row framing.
    pub rows: Option<RowsSpec>,
    /// JSON pointer to the row's identity.
    pub row_key: Option<String>,
    /// Pagination.
    pub paginate: Option<PaginateSpec>,
    /// CEL over a 2xx body; true fails the tick with the text at `error.at`.
    pub fail_when: Option<String>,
    /// Page ceiling per tick.
    pub max_pages: Option<u32>,
    /// Fields added to every row: a value is a template, or a JSON object
    /// or array whose string leaves are templates. One that reads `key` is
    /// rendered per key of the unit's keyset.
    pub add_fields: BTreeMap<String, serde_json::Value>,
    /// A folding builder: the rows of one key (of the tick, without a
    /// keyset) become the one row it builds, stamped with `add_fields`.
    pub fold: Option<RowBuilderKind>,
    /// A listing protocol standing in for the page fetch: the unit's
    /// `path` and `query` name the listing, the lister frames and pages
    /// it, and `rows` and `paginate` are its own.
    pub lister: Option<ListerKind>,
    /// Bytes a page-bounded decoder may buffer.
    pub max_page_bytes: Option<usize>,
    /// How the unit's requests are multiplied (a keyset, a lookup, a
    /// manifest, a queue); the defaults' when unset.
    pub construct: Option<ConstructSpec>,
    /// Idempotent requests sent once per tick before the unit's first page
    /// (the OMAP `subscriptions/start`); the defaults' when unset. A step
    /// renders from the unit's vars alone, never the window, a key or an
    /// item.
    pub prelude: Option<Vec<LookupRequest>>,
    /// Non-2xx statuses answered as an empty page instead of a failure (a
    /// 404 for a key the provider does not know).
    pub ignore_status: Vec<u16>,
    /// Total seconds one page request may take, body included; only for a
    /// unit whose decoder reads the page whole (json_at, document, json,
    /// csv). A streaming decoder is bounded per read by the client and
    /// takes as long as the body takes.
    pub timeout_secs: Option<u64>,
}

impl Default for EndpointSpec {
    fn default() -> Self {
        Self {
            unit: String::new(),
            shape: None,
            base_url: None,
            auth: None,
            window: None,
            vars: BTreeMap::new(),
            method: None,
            path: String::new(),
            query: BTreeMap::new(),
            headers: BTreeMap::new(),
            body: None,
            rows: None,
            row_key: None,
            paginate: None,
            fail_when: None,
            max_pages: None,
            add_fields: BTreeMap::new(),
            fold: None,
            lister: None,
            max_page_bytes: None,
            construct: None,
            prelude: None,
            ignore_status: Vec::new(),
            timeout_secs: None,
        }
    }
}

impl EndpointSpec {
    /// The unit's own keyset, when it declares one.
    #[must_use]
    pub fn keyset(&self) -> Option<&KeysetSpec> {
        self.construct.as_ref()?.keyset.as_ref()
    }

    /// The unit's own queue, when it declares one.
    #[must_use]
    pub fn queue(&self) -> Option<&QueueSpec> {
        self.construct.as_ref()?.queue.as_ref()
    }

    /// The unit's own lookup stage, when it declares one.
    #[must_use]
    pub fn lookup(&self) -> Option<&LookupSpec> {
        self.construct.as_ref()?.lookup.as_ref()
    }

    /// The unit's own manifest, when it declares one.
    #[must_use]
    pub fn manifest(&self) -> Option<&ManifestSpec> {
        self.construct.as_ref()?.manifest.as_ref()
    }

    /// The scope this unit's token is minted for, when it names one.
    #[must_use]
    pub fn auth_scope(&self) -> Option<&str> {
        self.auth.as_ref()?.scope.as_deref()
    }

    /// The window format this unit names, when it differs from the profile's.
    #[must_use]
    pub fn window_format(&self) -> Option<&WindowFormat> {
        self.window.as_ref()?.format.as_ref()
    }
}

/// The request a health check sends: the API's cheapest authenticated call.
///
/// Without one the health check only resolves the credential (and mints a
/// token for an OAuth2 instance); with one it also proves the provider
/// answers that identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ProbeSpec {
    /// GET or POST.
    pub method: Method,
    /// Path template appended to `base_url`.
    pub path: String,
    /// Query parameter templates; an empty rendering is omitted.
    pub query: BTreeMap<String, String>,
    /// CEL over the 2xx body; true fails the probe with the text at
    /// `error.at` (an API that answers a bad credential inside a 200).
    pub fail_when: Option<String>,
}

/// Default page ceiling.
pub const DEFAULT_MAX_PAGES: u32 = 50;
/// Default bound on a page-bounded decoder's buffer.
pub const DEFAULT_MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;

/// A declarative REST profile: the shape of an API, with no identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RestProfile {
    /// Registry key for a shipped profile; an inline profile may leave it empty.
    pub profile: String,
    /// Release maturity of sources built from this profile.
    pub maturity: dfe_fetcher_core::SourceMaturity,
    /// Base URL template; must reference a variable, never a literal URL.
    pub base_url: String,
    /// Default shape of every endpoint.
    pub shape: UnitShape,
    /// Accepted auth modes and their shapes.
    pub auth: AuthSpec,
    /// Header templates on every request.
    pub headers: BTreeMap<String, String>,
    /// Retry policy.
    pub retry: RetrySpec,
    /// Where a non-2xx body keeps its error text.
    pub error: ErrorSpec,
    /// Quota headers surfaced as gauges.
    pub quota: QuotaSpec,
    /// Window rendering and chunking.
    pub window: WindowSpec,
    /// The health-check request, when the API has a cheap one.
    pub probe: Option<ProbeSpec>,
    /// Endpoint defaults.
    pub defaults: EndpointDefaults,
    /// The units.
    pub endpoints: Vec<EndpointSpec>,
    /// Default `vars` an instance may override.
    pub vars: BTreeMap<String, serde_json::Value>,
}

fn pointer_issue(field: &str, pointer: &str) -> Option<Issue> {
    (!pointer.starts_with('/')).then(|| {
        Issue::new(
            field,
            format!("`{pointer}` must be a JSON pointer starting with `/`"),
        )
    })
}

fn template_issue(field: &str, text: &str) -> Option<Issue> {
    Template::compile(text)
        .err()
        .map(|e| Issue::new(field, e.to_string()))
}

/// A base URL is a required template that reads a variable, never a literal.
fn base_url_issue(field: &str, text: &str) -> Option<Issue> {
    if text.trim().is_empty() {
        return Some(Issue::new(field, "is required"));
    }
    match Template::compile(text) {
        Err(e) => Some(Issue::new(field, e.to_string())),
        Ok(t) if t.is_literal() => Some(Issue::new(
            field,
            "a profile carries no identity: a literal URL belongs in the instance's \
             vars and the profile references it as `{{ vars.base_url }}`",
        )),
        Ok(_) => None,
    }
}

fn predicate_issue(field: &str, text: &str) -> Option<Issue> {
    Predicate::compile(text)
        .err()
        .map(|e| Issue::new(field, e.to_string()))
}

/// The token-response fields a mode may not expose: the credentials
/// themselves, which a template would put in a URL or a log line.
const NEVER_EXPOSED: &[&str] = &["access_token", "refresh_token", "id_token"];

/// Problems with a mode's `expose` list.
fn expose_issues(field: &str, expose: &[String]) -> Vec<Issue> {
    expose
        .iter()
        .filter_map(|name| {
            if name.trim().is_empty() {
                Some(Issue::new(field, "names an empty field"))
            } else if NEVER_EXPOSED.contains(&name.as_str()) {
                Some(Issue::new(
                    field,
                    format!("`{name}` is a credential and is never exposed to templates"),
                ))
            } else {
                None
            }
        })
        .collect()
}

impl RestProfile {
    /// The effective method of an endpoint (its own, else the defaults',
    /// else GET).
    #[must_use]
    pub fn method_of(&self, endpoint: &EndpointSpec) -> Method {
        endpoint.method.or(self.defaults.method).unwrap_or_default()
    }

    /// The effective body of an endpoint: its own, else the defaults' when
    /// the endpoint is a POST (a GET carries no body and inherits none).
    #[must_use]
    pub fn body_of<'a>(&'a self, endpoint: &'a EndpointSpec) -> Option<&'a serde_json::Value> {
        endpoint.body.as_ref().or_else(|| {
            (self.method_of(endpoint) == Method::Post)
                .then_some(self.defaults.body.as_ref())
                .flatten()
        })
    }

    /// The effective path template of an endpoint (its own, else the
    /// defaults'); empty when neither is set.
    #[must_use]
    pub fn path_of<'a>(&'a self, endpoint: &'a EndpointSpec) -> &'a str {
        if endpoint.path.trim().is_empty() {
            self.defaults.path.as_deref().unwrap_or_default()
        } else {
            &endpoint.path
        }
    }

    /// The effective base URL template of an endpoint (its own, else the
    /// profile's). Either is rendered per unit, so a profile whose hosts
    /// differ by unit may read a per-unit var in the one template.
    #[must_use]
    pub fn base_url_of<'a>(&'a self, endpoint: &'a EndpointSpec) -> &'a str {
        endpoint.base_url.as_deref().unwrap_or(&self.base_url)
    }

    /// The effective window spec of an endpoint: the profile's, with the
    /// format the endpoint names in place of the profile's.
    #[must_use]
    pub fn window_of(&self, endpoint: &EndpointSpec) -> WindowSpec {
        let mut window = self.window.clone();
        if let Some(format) = endpoint.window_format() {
            window.format = format.clone();
        }
        window
    }

    /// The effective row spec of an endpoint (its own, else the defaults').
    #[must_use]
    pub fn rows_of(&self, endpoint: &EndpointSpec) -> RowsSpec {
        endpoint
            .rows
            .clone()
            .or_else(|| self.defaults.rows.clone())
            .unwrap_or_default()
    }

    /// The `content` template of the rows a unit emits: the lookup's or the
    /// manifest's rows when the unit has one, its own rows otherwise.
    #[must_use]
    pub fn content_of(&self, endpoint: &EndpointSpec) -> String {
        if let Some(lookup) = self.lookup_of(endpoint) {
            return lookup.rows.content.clone();
        }
        if let Some(manifest) = self.manifest_of(endpoint) {
            return manifest.rows.content.clone();
        }
        self.rows_of(endpoint).content
    }

    /// The effective pagination of an endpoint.
    #[must_use]
    pub fn paginate_of(&self, endpoint: &EndpointSpec) -> PaginateSpec {
        endpoint
            .paginate
            .clone()
            .or_else(|| self.defaults.paginate.clone())
            .unwrap_or_default()
    }

    /// The effective page ceiling of an endpoint.
    #[must_use]
    pub fn max_pages_of(&self, endpoint: &EndpointSpec) -> u32 {
        endpoint
            .max_pages
            .or(self.defaults.max_pages)
            .unwrap_or(DEFAULT_MAX_PAGES)
    }

    /// The effective page buffer bound of an endpoint.
    #[must_use]
    pub fn max_page_bytes_of(&self, endpoint: &EndpointSpec) -> usize {
        endpoint
            .max_page_bytes
            .or(self.defaults.max_page_bytes)
            .unwrap_or(DEFAULT_MAX_PAGE_BYTES)
    }

    /// The effective prelude of an endpoint: its own (an empty list opts
    /// out), else the defaults', else none.
    #[must_use]
    pub fn prelude_of<'a>(&'a self, endpoint: &'a EndpointSpec) -> &'a [LookupRequest] {
        endpoint
            .prelude
            .as_deref()
            .or(self.defaults.prelude.as_deref())
            .unwrap_or_default()
    }

    /// The effective construct of an endpoint: its own (an empty one opts
    /// out), else the defaults'.
    #[must_use]
    pub fn construct_of<'a>(&'a self, endpoint: &'a EndpointSpec) -> Option<&'a ConstructSpec> {
        endpoint
            .construct
            .as_ref()
            .or(self.defaults.construct.as_ref())
    }

    /// The effective keyset of an endpoint.
    #[must_use]
    pub fn keyset_of<'a>(&'a self, endpoint: &'a EndpointSpec) -> Option<&'a KeysetSpec> {
        self.construct_of(endpoint)?.keyset.as_ref()
    }

    /// The effective lookup stage of an endpoint.
    #[must_use]
    pub fn lookup_of<'a>(&'a self, endpoint: &'a EndpointSpec) -> Option<&'a LookupSpec> {
        self.construct_of(endpoint)?.lookup.as_ref()
    }

    /// The effective manifest of an endpoint.
    #[must_use]
    pub fn manifest_of<'a>(&'a self, endpoint: &'a EndpointSpec) -> Option<&'a ManifestSpec> {
        self.construct_of(endpoint)?.manifest.as_ref()
    }

    /// The effective queue of an endpoint.
    #[must_use]
    pub fn queue_of<'a>(&'a self, endpoint: &'a EndpointSpec) -> Option<&'a QueueSpec> {
        self.construct_of(endpoint)?.queue.as_ref()
    }

    /// Whether any unit of the profile is a queue, which the app runs on
    /// the queue shape rather than the REST shape.
    #[must_use]
    pub fn is_queue(&self) -> bool {
        self.endpoints.iter().any(|e| self.queue_of(e).is_some())
    }

    /// Every structural problem, with the field it sits on. Empty means the
    /// profile binds.
    #[must_use]
    pub fn validate(&self) -> Vec<Issue> {
        let mut issues = Vec::new();
        issues.extend(base_url_issue("base_url", &self.base_url));
        if self.auth.accepts.is_empty() {
            issues.push(Issue::new("auth.accepts", "must list at least one mode"));
        }
        let scoped_mode = self.auth.accepts.iter().any(|k| k.is_scoped());
        if self.auth.accepts.contains(&AuthKind::JwtBearer) {
            let jwt = &self.auth.jwt_bearer;
            if jwt.token_url.trim().is_empty() {
                issues.push(Issue::new("auth.jwt_bearer.token_url", "is required"));
            } else {
                issues.extend(template_issue("auth.jwt_bearer.token_url", &jwt.token_url));
            }
            for (name, value) in &jwt.claims {
                issues.extend(template_issue(
                    &format!("auth.jwt_bearer.claims.{name}"),
                    value,
                ));
            }
            for reserved in ["iat", "exp"] {
                if jwt.claims.contains_key(reserved) {
                    issues.push(Issue::new(
                        format!("auth.jwt_bearer.claims.{reserved}"),
                        "is set from ttl_secs on every assertion",
                    ));
                }
            }
            if jwt.ttl_secs == 0 {
                issues.push(Issue::new("auth.jwt_bearer.ttl_secs", "must be at least 1"));
            }
            issues.extend(expose_issues("auth.jwt_bearer.expose", &jwt.expose));
        }
        if self.auth.accepts.contains(&AuthKind::GceMetadata) {
            let gce = &self.auth.gce_metadata;
            if gce.url.trim().is_empty() {
                issues.push(Issue::new("auth.gce_metadata.url", "is required"));
            } else {
                issues.extend(template_issue("auth.gce_metadata.url", &gce.url));
            }
        }
        if self.auth.accepts.contains(&AuthKind::ApiKey) {
            let key = &self.auth.api_key;
            if key.header.is_some() == key.query.is_some() {
                issues.push(Issue::new(
                    "auth.api_key",
                    "needs exactly one of `header` or `query`",
                ));
            }
        }
        if self.auth.accepts.contains(&AuthKind::SigV4) {
            for (field, value) in [
                ("service", &self.auth.sigv4.service),
                ("region", &self.auth.sigv4.region),
            ] {
                let field = format!("auth.sigv4.{field}");
                if value.trim().is_empty() {
                    issues.push(Issue::new(field, "is required"));
                } else {
                    issues.extend(template_issue(&field, value));
                }
            }
        }
        if self
            .auth
            .accepts
            .contains(&AuthKind::Oauth2ClientCredentials)
        {
            let oauth = &self.auth.oauth2_client_credentials;
            if oauth.token_url.trim().is_empty() {
                issues.push(Issue::new(
                    "auth.oauth2_client_credentials.token_url",
                    "is required",
                ));
            } else {
                issues.extend(template_issue(
                    "auth.oauth2_client_credentials.token_url",
                    &oauth.token_url,
                ));
            }
            issues.extend(expose_issues(
                "auth.oauth2_client_credentials.expose",
                &oauth.expose,
            ));
        }
        if self.retry.max_backoff_ms < self.retry.min_backoff_ms {
            issues.push(Issue::new(
                "retry.max_backoff_ms",
                "must be at least min_backoff_ms",
            ));
        }
        if let Some(at) = &self.error.at {
            issues.extend(pointer_issue("error.at", at));
        }
        for (name, value) in &self.headers {
            issues.extend(template_issue(&format!("headers.{name}"), value));
        }
        for (name, value) in &self.defaults.query {
            issues.extend(template_issue(&format!("defaults.query.{name}"), value));
        }
        if let Some(body) = &self.defaults.body {
            issues.extend(body_template_issues("defaults.body", body));
        }
        for (name, value) in &self.defaults.headers {
            issues.extend(template_issue(&format!("defaults.headers.{name}"), value));
        }
        if let Some(path) = &self.defaults.path {
            issues.extend(template_issue("defaults.path", path));
        }
        if let Some(prelude) = &self.defaults.prelude {
            issues.extend(prelude_issues("defaults.prelude", prelude));
        }
        if let Some(construct) = &self.defaults.construct {
            issues.extend(construct_issues("defaults.construct", construct));
        }
        if let Some(probe) = &self.probe {
            if probe.path.trim().is_empty() {
                issues.push(Issue::new("probe.path", "is required"));
            } else {
                issues.extend(template_issue("probe.path", &probe.path));
            }
            for (name, value) in &probe.query {
                issues.extend(template_issue(&format!("probe.query.{name}"), value));
            }
            if let Some(expr) = &probe.fail_when {
                issues.extend(predicate_issue("probe.fail_when", expr));
            }
        }
        if self.endpoints.is_empty() {
            issues.push(Issue::new("endpoints", "must list at least one unit"));
        }
        let mut seen = std::collections::HashSet::new();
        for (i, endpoint) in self.endpoints.iter().enumerate() {
            let at = |f: &str| format!("endpoints[{i}].{f}");
            if endpoint.unit.trim().is_empty() {
                issues.push(Issue::new(at("unit"), "is required"));
            } else if !seen.insert(endpoint.unit.as_str()) {
                issues.push(Issue::new(
                    at("unit"),
                    format!("`{}` is declared twice", endpoint.unit),
                ));
            }
            let path = self.path_of(endpoint);
            if path.trim().is_empty() {
                issues.push(Issue::new(
                    at("path"),
                    "is required (here or under defaults.path)",
                ));
            } else if !endpoint.path.trim().is_empty() {
                issues.extend(template_issue(&at("path"), path));
            }
            if let Some(base_url) = &endpoint.base_url {
                issues.extend(base_url_issue(&at("base_url"), base_url));
            }
            if endpoint.auth_scope().is_some() && !scoped_mode {
                issues.push(Issue::new(
                    at("auth.scope"),
                    "a unit scope needs a mode that mints a token for one \
                     (oauth2_client_credentials or jwt_bearer) in auth.accepts",
                ));
            }
            for (name, value) in &endpoint.query {
                issues.extend(template_issue(&at(&format!("query.{name}")), value));
            }
            for (name, value) in &endpoint.headers {
                issues.extend(template_issue(&at(&format!("headers.{name}")), value));
            }
            for (name, value) in &endpoint.add_fields {
                let field = at(&format!("add_fields.{name}"));
                let leaf_issues = body_template_issues(&field, value);
                if leaf_issues.is_empty() {
                    for var in ["window", "page", "item"] {
                        if value_references(value, var) {
                            issues.push(Issue::new(
                                &field,
                                format!("add_fields renders once per request set and cannot read `{var}`"),
                            ));
                        }
                    }
                    if value_references(value, "key") && self.keyset_of(endpoint).is_none() {
                        issues.push(Issue::new(
                            &field,
                            "reads `key` but the unit declares no `construct.keyset`",
                        ));
                    }
                }
                issues.extend(leaf_issues);
            }
            if let Some(fold) = endpoint.fold
                && !fold.folds()
            {
                issues.push(Issue::new(
                    at("fold"),
                    "names a per-row builder; a fold takes a folding builder (go_module_aggregate)",
                ));
            }
            if let Some(construct) = &endpoint.construct {
                issues.extend(construct_issues(&at("construct"), construct));
            }
            if endpoint.lister.is_some() {
                if endpoint.rows.is_some() {
                    issues.push(Issue::new(
                        at("rows"),
                        "a lister frames its own listing; `rows` is the manifest's",
                    ));
                }
                if endpoint.paginate.is_some() {
                    issues.push(Issue::new(
                        at("paginate"),
                        "a lister walks its own pages; `paginate` is not read",
                    ));
                }
                if self.manifest_of(endpoint).is_none() {
                    issues.push(Issue::new(
                        at("lister"),
                        "a listing yields items for a manifest; declare `construct.manifest`",
                    ));
                }
            }
            if let Some(prelude) = &endpoint.prelude {
                issues.extend(prelude_issues(&at("prelude"), prelude));
            }
            for status in &endpoint.ignore_status {
                if !(100..600).contains(status) {
                    issues.push(Issue::new(
                        at("ignore_status"),
                        format!("{status} is not an HTTP status code"),
                    ));
                }
            }
            if let Some(body) = &endpoint.body {
                issues.extend(body_template_issues(&at("body"), body));
                if self.method_of(endpoint) == Method::Get {
                    issues.push(Issue::new(at("body"), "a GET carries no body"));
                }
            }
            if let Some(key) = &endpoint.row_key {
                issues.extend(pointer_issue(&at("row_key"), key));
            }
            if endpoint.max_pages == Some(0) {
                issues.push(Issue::new(at("max_pages"), "must be at least 1"));
            }
            if endpoint.max_page_bytes == Some(0) {
                issues.push(Issue::new(at("max_page_bytes"), "must be at least 1"));
            }
            let rows = self.rows_of(endpoint);
            issues.extend(timeout_issue(
                &at("timeout_secs"),
                endpoint.timeout_secs,
                &rows,
            ));
            issues.extend(rows_issues(&at("rows"), &rows));
            if endpoint.shape.unwrap_or(self.shape) == UnitShape::Dump
                && self.content_of(endpoint).trim() == "binary"
            {
                issues.push(Issue::new(
                    at("rows.content"),
                    "a dump unit cannot carry binary rows; the snapshot envelope is JSON",
                ));
            }
            let paginate = self.paginate_of(endpoint);
            issues.extend(
                paginate_issues(&paginate, rows.decoder)
                    .into_iter()
                    .map(|(f, m)| Issue::new(at(&format!("paginate{f}")), m)),
            );
            if let Some(expr) = &endpoint.fail_when {
                issues.extend(predicate_issue(&at("fail_when"), expr));
                if !rows.decoder.is_page_bounded() {
                    issues.push(Issue::new(
                        at("fail_when"),
                        "reads the body; use a page-bounded decoder (json_at, document, csv)",
                    ));
                }
            }
        }
        issues
    }
}

/// Problems with a `timeout_secs` at `field`: zero, or a total bound on a
/// request whose rows stream.
fn timeout_issue(field: &str, timeout_secs: Option<u64>, rows: &RowsSpec) -> Vec<Issue> {
    match timeout_secs {
        None => Vec::new(),
        Some(0) => vec![Issue::new(field, "must be at least 1")],
        Some(_) if !crate::decode::Decoder::build(rows).is_page_bounded() => vec![Issue::new(
            field,
            "bounds the whole request, which a streaming decoder (ndjson, json_array, lines) must not be; the client bounds each read instead",
        )],
        Some(_) => Vec::new(),
    }
}

/// Problems with a secondary request (a lookup's, a keyset's, a manifest
/// item's, a prelude step's); `default` is the method it sends unless it
/// names one, `rows` the framing of its response when it carries rows.
fn request_issues(
    field: &str,
    request: &LookupRequest,
    default: Method,
    rows: Option<&RowsSpec>,
) -> Vec<Issue> {
    let mut issues = Vec::new();
    match rows {
        Some(rows) => issues.extend(timeout_issue(
            &format!("{field}.timeout_secs"),
            request.timeout_secs,
            rows,
        )),
        None if request.timeout_secs == Some(0) => {
            issues.push(Issue::new(
                format!("{field}.timeout_secs"),
                "must be at least 1",
            ));
        }
        None => {}
    }
    if request.path.trim().is_empty() {
        issues.push(Issue::new(format!("{field}.path"), "is required"));
    } else {
        issues.extend(template_issue(&format!("{field}.path"), &request.path));
    }
    for (name, value) in &request.query {
        issues.extend(template_issue(&format!("{field}.query.{name}"), value));
    }
    for (name, value) in &request.headers {
        issues.extend(template_issue(&format!("{field}.headers.{name}"), value));
    }
    if let Some(body) = &request.body {
        issues.extend(body_template_issues(&format!("{field}.body"), body));
        if request.method_or(default) == Method::Get {
            issues.push(Issue::new(format!("{field}.body"), "a GET carries no body"));
        }
    }
    for status in &request.ignore_status {
        if !(100..600).contains(status) {
            issues.push(Issue::new(
                format!("{field}.ignore_status"),
                format!("{status} is not an HTTP status code"),
            ));
        }
    }
    issues
}

/// Problems with a construct, its keyset, lookup and manifest, at `field`.
fn construct_issues(field: &str, construct: &ConstructSpec) -> Vec<Issue> {
    let mut issues = Vec::new();
    if let Some(keyset) = &construct.keyset {
        let kat = |f: &str| format!("{field}.keyset.{f}");
        match (&keyset.from, &keyset.request) {
            (Some(from), None) => issues.extend(template_issue(&kat("from"), from)),
            (None, Some(request)) => {
                issues.extend(request_issues(&kat("request"), request, Method::Post, None));
                match &keyset.keys_at {
                    Some(pointer) => issues.extend(pointer_issue(&kat("keys_at"), pointer)),
                    None => issues.push(Issue::new(
                        kat("keys_at"),
                        "a keyset request needs the pointer to its key array",
                    )),
                }
            }
            _ => issues.push(Issue::new(
                kat("from"),
                "a keyset needs exactly one of `from` or `request`",
            )),
        }
        if keyset.keys_at.is_some() && keyset.request.is_none() {
            issues.push(Issue::new(
                kat("keys_at"),
                "reads a response but the keyset sends no `request`",
            ));
        }
    }
    let stages = [
        construct.lookup.is_some(),
        construct.manifest.is_some(),
        construct.queue.is_some(),
    ]
    .into_iter()
    .filter(|set| *set)
    .count();
    if stages > 1 {
        issues.push(Issue::new(
            format!("{field}.manifest"),
            "a unit takes one of `lookup`, `manifest` and `queue`, not two",
        ));
    }
    if let Some(queue) = &construct.queue {
        let qat = |f: &str| format!("{field}.queue.{f}");
        if queue.ack_at.trim().is_empty() {
            issues.push(Issue::new(qat("ack_at"), "is required"));
        } else {
            issues.extend(pointer_issue(&qat("ack_at"), &queue.ack_at));
        }
        issues.extend(request_issues(
            &qat("ack_request"),
            &queue.ack_request,
            Method::Post,
            None,
        ));
        if queue.ack_batch == 0 {
            issues.push(Issue::new(qat("ack_batch"), "must be at least 1"));
        }
        if construct.keyset.is_some() {
            issues.push(Issue::new(
                qat("ack_request"),
                "a queue is one subscription; instantiate a unit per subscription instead of a keyset",
            ));
        }
    }
    if let Some(lookup) = &construct.lookup {
        let lat = |f: &str| format!("{field}.lookup.{f}");
        if let Some(pointer) = &lookup.id_at {
            issues.extend(pointer_issue(&lat("id_at"), pointer));
        }
        if lookup.batch == 0 {
            issues.push(Issue::new(lat("batch"), "must be at least 1"));
        }
        issues.extend(request_issues(
            &lat("request"),
            &lookup.request,
            Method::Post,
            Some(&lookup.rows),
        ));
        issues.extend(rows_issues(&lat("rows"), &lookup.rows));
        if let Some(paginate) = &lookup.paginate {
            issues.extend(
                paginate_issues(paginate, lookup.rows.decoder)
                    .into_iter()
                    .map(|(f, m)| Issue::new(lat(&format!("paginate{f}")), m)),
            );
        }
        if lookup.max_pages == Some(0) {
            issues.push(Issue::new(lat("max_pages"), "must be at least 1"));
        }
    }
    if let Some(manifest) = &construct.manifest {
        let mat = |f: &str| format!("{field}.manifest.{f}");
        issues.extend(request_issues(
            &mat("item_request"),
            &manifest.item_request,
            Method::Get,
            Some(&manifest.rows),
        ));
        issues.extend(rows_issues(&mat("rows"), &manifest.rows));
        match (&manifest.key, &manifest.position) {
            (Some(key), Some(position)) => {
                issues.extend(template_issue(&mat("key"), key));
                issues.extend(template_issue(&mat("position"), position));
            }
            (None, None) => {}
            _ => issues.push(Issue::new(
                mat("position"),
                "an item checkpoint needs both `key` and `position`",
            )),
        }
        if manifest.max_items == Some(0) {
            issues.push(Issue::new(mat("max_items"), "must be at least 1"));
        }
        for (name, value) in &manifest.add_fields {
            issues.extend(body_template_issues(
                &mat(&format!("add_fields.{name}")),
                value,
            ));
        }
    }
    issues
}

/// Problems with a prelude at `field`: each step is a request that may
/// read only the unit's own context.
fn prelude_issues(field: &str, prelude: &[LookupRequest]) -> Vec<Issue> {
    let mut issues = Vec::new();
    for (i, step) in prelude.iter().enumerate() {
        let at = format!("{field}[{i}]");
        issues.extend(request_issues(&at, step, Method::Post, None));
        let mut templates: Vec<&str> = vec![step.path.as_str()];
        templates.extend(step.query.values().map(String::as_str));
        templates.extend(step.headers.values().map(String::as_str));
        if let Some(body) = &step.body {
            body_strings(body, &mut templates);
        }
        for text in templates {
            let Ok(template) = Template::compile(text) else {
                continue;
            };
            for var in ["window", "page", "key", "item", "ids"] {
                if template.references(var) {
                    issues.push(Issue::new(
                        &at,
                        format!(
                            "a prelude step runs before the first page and cannot read `{var}`"
                        ),
                    ));
                }
            }
        }
    }
    issues
}

/// Problems with a row spec: `json_at` needs a pointer and nothing else
/// reads one, and a folding builder is named under `fold`, not here.
fn rows_issues(field: &str, rows: &RowsSpec) -> Vec<Issue> {
    let mut issues = match (rows.decoder, rows.at.as_deref()) {
        (DecoderKind::JsonAt, None) => vec![Issue::new(
            format!("{field}.at"),
            "json_at needs a pointer to the row array",
        )],
        (DecoderKind::JsonAt, Some(pointer)) => pointer_issue(&format!("{field}.at"), pointer)
            .into_iter()
            .collect(),
        (_, Some(_)) => vec![Issue::new(
            format!("{field}.at"),
            "only json_at reads a pointer",
        )],
        (_, None) => Vec::new(),
    };
    if rows.builder.is_some_and(RowBuilderKind::folds) {
        issues.push(Issue::new(
            format!("{field}.builder"),
            "names a folding builder; a fold over a key's rows goes under the unit's `fold`",
        ));
    }
    match Template::compile(&rows.content) {
        Err(e) => issues.push(Issue::new(format!("{field}.content"), e.to_string())),
        Ok(template) if template.is_literal() => {
            if let Err(e) = rows.content.parse::<dfe_fetcher_core::RowContent>() {
                issues.push(Issue::new(format!("{field}.content"), e));
            }
        }
        Ok(_) => {}
    }
    issues
}

/// Whether any string leaf of a JSON value with template leaves reads
/// `var`; a leaf that does not compile reads nothing (validation reports it).
fn value_references(value: &serde_json::Value, var: &str) -> bool {
    match value {
        serde_json::Value::String(s) => Template::compile(s).is_ok_and(|t| t.references(var)),
        serde_json::Value::Object(map) => map.values().any(|v| value_references(v, var)),
        serde_json::Value::Array(items) => items.iter().any(|v| value_references(v, var)),
        _ => false,
    }
}

/// Every string leaf of a JSON body, in document order.
fn body_strings<'a>(body: &'a serde_json::Value, out: &mut Vec<&'a str>) {
    match body {
        serde_json::Value::String(s) => out.push(s),
        serde_json::Value::Object(map) => map.values().for_each(|v| body_strings(v, out)),
        serde_json::Value::Array(items) => items.iter().for_each(|v| body_strings(v, out)),
        _ => {}
    }
}

fn body_template_issues(field: &str, body: &serde_json::Value) -> Vec<Issue> {
    let mut issues = Vec::new();
    match body {
        serde_json::Value::String(s) => issues.extend(template_issue(field, s)),
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                issues.extend(body_template_issues(&format!("{field}.{k}"), v));
            }
        }
        serde_json::Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                issues.extend(body_template_issues(&format!("{field}[{i}]"), v));
            }
        }
        _ => {}
    }
    issues
}

/// Problems with one pagination spec, as `(field suffix, message)`.
fn paginate_issues(p: &PaginateSpec, decoder: DecoderKind) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let needs = |field: &str, present: bool, out: &mut Vec<(String, String)>| {
        if !present {
            out.push((
                format!(".{field}"),
                format!("{} needs `{field}`", strategy_name(p.strategy)),
            ));
        }
    };
    match p.strategy {
        PagerStrategy::None | PagerStrategy::LinkHeader => {}
        PagerStrategy::Cursor => {
            needs("from", p.from.is_some(), &mut out);
            needs("into", p.into.is_some(), &mut out);
        }
        PagerStrategy::PageNumber => needs("param", p.param.is_some(), &mut out),
        PagerStrategy::Offset => {
            needs("param", p.param.is_some(), &mut out);
            if p.page_size.is_none() && p.total_at.is_none() {
                out.push((
                    ".page_size".into(),
                    "offset needs `page_size` or `total_at` to know the last page".into(),
                ));
            }
        }
        PagerStrategy::RequestPath => needs("from", p.from.is_some(), &mut out),
    }
    if let Some(from) = &p.from
        && !(from.starts_with("body:/") || from.starts_with("header:"))
    {
        out.push((
            ".from".into(),
            "must be `body:/pointer` or `header:Name`".into(),
        ));
    }
    if let Some(into) = &p.into
        && !(into.starts_with("query:")
            || into.starts_with("body:/")
            || into.starts_with("body_replace:/")
            || into == "path")
    {
        out.push((
            ".into".into(),
            "must be `query:name`, `body:/pointer`, `body_replace:/pointer` or `path`".into(),
        ));
    }
    for (name, pointer) in [
        ("total_pages_at", &p.total_pages_at),
        ("total_at", &p.total_at),
    ] {
        if let Some(pointer) = pointer
            && !pointer.starts_with('/')
        {
            out.push((
                format!(".{name}"),
                format!("`{pointer}` must be a JSON pointer"),
            ));
        }
    }
    if let Some(expr) = &p.stop_when
        && let Err(e) = Predicate::compile(expr)
    {
        out.push((".stop_when".into(), e.to_string()));
    }
    if p.page_size == Some(0) {
        out.push((".page_size".into(), "must be at least 1".into()));
    }
    if p.reads_body() && !decoder.is_page_bounded() {
        out.push((
            String::new(),
            format!(
                "reads the page body but `{}` streams it; use a page-bounded decoder \
                 (json_at, document, csv)",
                decoder_name(decoder)
            ),
        ));
    }
    out
}

fn strategy_name(s: PagerStrategy) -> &'static str {
    match s {
        PagerStrategy::None => "none",
        PagerStrategy::LinkHeader => "link_header",
        PagerStrategy::Cursor => "cursor",
        PagerStrategy::PageNumber => "page_number",
        PagerStrategy::Offset => "offset",
        PagerStrategy::RequestPath => "request_path",
    }
}

fn decoder_name(d: DecoderKind) -> &'static str {
    match d {
        DecoderKind::JsonArray => "json_array",
        DecoderKind::Ndjson => "ndjson",
        DecoderKind::JsonAt => "json_at",
        DecoderKind::Document => "document",
        DecoderKind::Json => "json",
        DecoderKind::Lines => "lines",
        DecoderKind::Csv => "csv",
    }
}

/// A shipped profile by name, or a one-off profile written inline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum ProfileRef {
    /// Registry key of a shipped profile.
    Named(String),
    /// The profile itself.
    Inline(Box<RestProfile>),
}

impl Default for ProfileRef {
    fn default() -> Self {
        Self::Named(String::new())
    }
}

/// Hand-written rather than `#[serde(untagged)]`: an untagged enum reports
/// "did not match any variant" and drops the inline profile's own error, which
/// is the one carrying the bad field and its line.
impl<'de> Deserialize<'de> for ProfileRef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = ProfileRef;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a shipped profile name or an inline profile mapping")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(ProfileRef::Named(v.to_owned()))
            }

            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(ProfileRef::Named(v))
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                RestProfile::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(|p| ProfileRef::Inline(Box::new(p)))
            }
        }

        d.deserialize_any(Visitor)
    }
}

/// The identity half of the auth axis: the mode picked and its credentials.
///
/// Secret fields are credential specs (`vault:<mount>/data/<path>:<key>`,
/// `env:VAR`, or a literal) resolved when first used.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct InstanceAuth {
    /// One of the profile's `auth.accepts`.
    pub mode: AuthKind,
    /// `bearer`: the token.
    pub token: Option<SensitiveString>,
    /// `api_key`: the key.
    pub key: Option<SensitiveString>,
    /// `basic`: the user name.
    pub username: Option<String>,
    /// `basic`: the password.
    pub password: Option<SensitiveString>,
    /// `oauth2_client_credentials`: the client id.
    pub client_id: Option<String>,
    /// `oauth2_client_credentials`: the client secret.
    pub client_secret: Option<SensitiveString>,
    /// `oauth2_client_credentials`: overrides the profile's scope.
    pub scope: Option<String>,
    /// `duo_hmac`: the integration key (`ikey`), the Basic user name.
    pub integration_key: Option<String>,
    /// `duo_hmac`: the secret key (`skey`) the signature is made with.
    pub secret_key: Option<SensitiveString>,
    /// `jwt_bearer`: a Google-style service-account key JSON (`client_email`,
    /// `private_key`, `token_uri`) as a credential spec.
    pub service_account_key: Option<SensitiveString>,
    /// `jwt_bearer`: a credential spec resolving to the PATH of such a key
    /// file.
    pub service_account_key_file: Option<SensitiveString>,
    /// `jwt_bearer`: a bare RSA private key PEM as a credential spec, for an
    /// API whose issuer and audience come from `vars` alone.
    pub private_key: Option<SensitiveString>,
    /// `sigv4`: the access key id, a credential spec.
    pub access_key_id: Option<SensitiveString>,
    /// `sigv4`: the secret access key, a credential spec.
    pub secret_access_key: Option<SensitiveString>,
    /// `sigv4`: a credential spec resolving to a JSON document carrying
    /// `access_key_id` and `secret_access_key` (the AWS `AccessKeyId` /
    /// `SecretAccessKey` spelling is accepted too), instead of the pair.
    pub credentials_json: Option<SensitiveString>,
    /// `sigv4`: an IAM role the keys assume through STS `AssumeRole` once per
    /// instance, its session credentials cached to their expiry and every
    /// request signed with them.
    pub assume_role_arn: Option<String>,
}

impl InstanceAuth {
    /// How many of the three `jwt_bearer` key sources are set.
    #[must_use]
    pub fn jwt_key_sources(&self) -> usize {
        [
            self.service_account_key.is_some(),
            self.service_account_key_file.is_some(),
            self.private_key.is_some(),
        ]
        .into_iter()
        .filter(|set| *set)
        .count()
    }

    /// Every credential-spec field that is set, as `(field, spec)`.
    ///
    /// Destructured exhaustively so a field added to the struct cannot
    /// quietly escape the load-time spec check: the plain identifiers are
    /// named and discarded, the specs are listed.
    #[must_use]
    pub fn credential_specs(&self) -> Vec<(&'static str, &SensitiveString)> {
        let Self {
            mode: _,
            username: _,
            client_id: _,
            scope: _,
            integration_key: _,
            assume_role_arn: _,
            token,
            key,
            password,
            client_secret,
            secret_key,
            service_account_key,
            service_account_key_file,
            private_key,
            access_key_id,
            secret_access_key,
            credentials_json,
        } = self;
        [
            ("token", token),
            ("key", key),
            ("password", password),
            ("client_secret", client_secret),
            ("secret_key", secret_key),
            ("service_account_key", service_account_key),
            ("service_account_key_file", service_account_key_file),
            ("private_key", private_key),
            ("access_key_id", access_key_id),
            ("secret_access_key", secret_access_key),
            ("credentials_json", credentials_json),
        ]
        .into_iter()
        .filter_map(|(field, spec)| spec.as_ref().map(|spec| (field, spec)))
        .collect()
    }

    /// Whether the `sigv4` identity is complete: the JSON document alone, or
    /// both halves of the key pair.
    #[must_use]
    pub fn sigv4_keys_complete(&self) -> bool {
        match &self.credentials_json {
            Some(_) => self.access_key_id.is_none() && self.secret_access_key.is_none(),
            None => self.access_key_id.is_some() && self.secret_access_key.is_some(),
        }
    }
}

impl Default for InstanceAuth {
    fn default() -> Self {
        Self {
            mode: AuthKind::None,
            token: None,
            key: None,
            username: None,
            password: None,
            client_id: None,
            client_secret: None,
            scope: None,
            integration_key: None,
            secret_key: None,
            service_account_key: None,
            service_account_key_file: None,
            private_key: None,
            access_key_id: None,
            secret_access_key: None,
            credentials_json: None,
            assume_role_arn: None,
        }
    }
}

/// Per-unit narrowing an instance applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct UnitOverride {
    /// Whether this instance fetches the unit.
    pub enabled: bool,
    /// The profile endpoint this unit is an instance of, when the unit's
    /// name is the instance's own: the endpoint runs once more under this
    /// name with these overrides (one unit per bucket prefix, per
    /// subscription), tagged `<connection>.<name>`.
    pub endpoint: Option<String>,
    /// Topic base for this unit's rows in place of the instance's.
    pub topic: Option<String>,
    /// Query parameters merged over the endpoint's.
    pub query: BTreeMap<String, String>,
    /// Headers merged over the endpoint's.
    pub headers: BTreeMap<String, String>,
    /// Values overriding the instance's `vars` for this unit's templates (a
    /// per-service page size).
    pub vars: BTreeMap<String, serde_json::Value>,
}

impl Default for UnitOverride {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: None,
            topic: None,
            query: BTreeMap::new(),
            headers: BTreeMap::new(),
            vars: BTreeMap::new(),
        }
    }
}

/// One deployment of a profile: identity, topic, interval, narrowing.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RestInstance {
    /// Whether the instance runs.
    pub enabled: bool,
    /// The shape.
    pub profile: ProfileRef,
    /// Fetch interval; the scheduler default when unset.
    pub interval_secs: Option<u64>,
    /// Topic base; dump units land on `<topic>-<unit>`.
    pub topic: String,
    /// CEL keep-filter over each row, hot-reloaded.
    pub filter: Option<String>,
    /// Credential kind and refs.
    pub auth: InstanceAuth,
    /// Values the profile's templates read as `vars.*`.
    pub vars: BTreeMap<String, serde_json::Value>,
    /// Per-unit narrowing, keyed by unit name.
    pub units: BTreeMap<String, UnitOverride>,
    /// Batch bounds for this instance; the deployment's when unset.
    pub accumulate: Option<AccumulateConfig>,
}

impl Default for RestInstance {
    fn default() -> Self {
        Self {
            enabled: true,
            profile: ProfileRef::default(),
            interval_secs: None,
            topic: String::new(),
            filter: None,
            auth: InstanceAuth::default(),
            vars: BTreeMap::new(),
            units: BTreeMap::new(),
            accumulate: None,
        }
    }
}

impl RestInstance {
    /// Every problem binding this instance to `profile`.
    #[must_use]
    pub fn validate(&self, profile: &RestProfile) -> Vec<Issue> {
        let mut issues = Vec::new();
        if self.topic.trim().is_empty() {
            issues.push(Issue::new("topic", "is required"));
        }
        if !profile.auth.accepts.contains(&self.auth.mode) {
            issues.push(Issue::new(
                "auth.mode",
                format!(
                    "`{}` is not one of the profile's accepted modes ({})",
                    self.auth.mode.as_str(),
                    profile
                        .auth
                        .accepts
                        .iter()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
        let needs = |field: &str, present: bool, issues: &mut Vec<Issue>| {
            if !present {
                issues.push(Issue::new(
                    format!("auth.{field}"),
                    format!("`{}` needs `{field}`", self.auth.mode.as_str()),
                ));
            }
        };
        match self.auth.mode {
            AuthKind::None => {}
            AuthKind::Bearer => needs("token", self.auth.token.is_some(), &mut issues),
            AuthKind::ApiKey => needs("key", self.auth.key.is_some(), &mut issues),
            AuthKind::Basic => {
                needs("username", self.auth.username.is_some(), &mut issues);
                needs("password", self.auth.password.is_some(), &mut issues);
            }
            AuthKind::Oauth2ClientCredentials => {
                needs("client_id", self.auth.client_id.is_some(), &mut issues);
                needs(
                    "client_secret",
                    self.auth.client_secret.is_some(),
                    &mut issues,
                );
            }
            AuthKind::DuoHmac => {
                needs(
                    "integration_key",
                    self.auth.integration_key.is_some(),
                    &mut issues,
                );
                needs("secret_key", self.auth.secret_key.is_some(), &mut issues);
            }
            AuthKind::JwtBearer => {
                if self.auth.jwt_key_sources() != 1 {
                    issues.push(Issue::new(
                        "auth.service_account_key",
                        "`jwt_bearer` needs exactly one of `service_account_key`, \
                         `service_account_key_file` or `private_key`",
                    ));
                }
            }
            AuthKind::GceMetadata => {}
            AuthKind::SigV4 => {
                if !self.auth.sigv4_keys_complete() {
                    issues.push(Issue::new(
                        "auth.access_key_id",
                        "`sigv4` needs `access_key_id` and `secret_access_key`, or \
                         `credentials_json` alone",
                    ));
                }
                if let Some(issue) = self
                    .auth
                    .assume_role_arn
                    .as_deref()
                    .and_then(crate::auth::role_arn_issue)
                {
                    issues.push(Issue::new("auth.assume_role_arn", issue));
                }
            }
        }
        if self.auth.assume_role_arn.is_some() && self.auth.mode != AuthKind::SigV4 {
            issues.push(Issue::new(
                "auth.assume_role_arn",
                "only the `sigv4` mode assumes a role",
            ));
        }
        // A spec the resolver cannot read otherwise reaches the provider as
        // literal text and comes back as a 401 about the credential's value.
        for (field, spec) in self.auth.credential_specs() {
            if let Some(issue) = dfe_fetcher_core::secret::spec_issue(spec.expose()) {
                issues.push(Issue::new(format!("auth.{field}"), issue));
            }
        }
        if let Some(expr) = &self.filter
            && let Err(e) = dfe_fetcher_core::rules::RowRules::compile(Some(expr), Vec::new())
        {
            issues.push(Issue::new("filter", e.to_string()));
        }
        if let Some(acc) = &self.accumulate
            && let Err(e) = acc.validate()
        {
            issues.push(Issue::new("accumulate", e.to_string()));
        }
        for (name, unit) in &self.units {
            let declared = profile.endpoints.iter().any(|e| e.unit == *name);
            match &unit.endpoint {
                None if !declared => issues.push(Issue::new(
                    format!("units.{name}"),
                    "the profile has no such unit",
                )),
                Some(_) if declared => issues.push(Issue::new(
                    format!("units.{name}.endpoint"),
                    "instantiates an endpoint under a name the profile already declares",
                )),
                Some(endpoint) if !profile.endpoints.iter().any(|e| e.unit == *endpoint) => {
                    issues.push(Issue::new(
                        format!("units.{name}.endpoint"),
                        format!("the profile has no endpoint `{endpoint}`"),
                    ));
                }
                _ => {}
            }
            if unit.topic.as_deref().is_some_and(|t| t.trim().is_empty()) {
                issues.push(Issue::new(
                    format!("units.{name}.topic"),
                    "is empty; leave it unset for the instance's topic",
                ));
            }
            for (k, v) in &unit.query {
                issues.extend(template_issue(&format!("units.{name}.query.{k}"), v));
            }
            for (k, v) in &unit.headers {
                issues.extend(template_issue(&format!("units.{name}.headers.{k}"), v));
            }
        }
        issues
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GITHUB: &str = r#"
profile: github
base_url: "{{ vars.api_url }}"
shape: incremental
auth: { accepts: [bearer] }
headers: { Accept: application/vnd.github+json }
window: { format: "strftime:%Y-%m-%dT%H:%M:%S+00:00", lookback: 1h }
retry: { never_retry: [401, 403] }
endpoints:
  - unit: audit_log
    path: "/orgs/{{ vars.org }}/audit-log"
    query: { per_page: 100, phrase: "created:{{ window.start }}..{{ window.end }}" }
    rows: { decoder: json_array }
    paginate: { strategy: link_header }
    max_pages: 50
"#;

    const RUNZERO: &str = r#"
profile: runzero
base_url: "{{ vars.base_url }}"
shape: dump
auth:
  accepts: [bearer, oauth2_client_credentials]
  oauth2_client_credentials: { token_url: "{{ base_url }}/account/api/token", scope: "", expires_in_fallback_secs: 1800 }
retry: { never_retry: [401, 403] }
error: { at: "/error" }
quota: { headers: { api_usage_today: x-api-usage-today } }
defaults: { query: { _oid: "{{ vars.org_id }}" }, rows: { decoder: ndjson }, paginate: { strategy: none } }
endpoints:
  - { unit: assets,   path: /export/org/assets.jsonl,   row_key: "/id" }
  - { unit: services, path: /export/org/services.jsonl, row_key: "/service_id" }
  - unit: assets_paged
    path: /export/org/assets.json
    query: { page_size: 500 }
    rows: { decoder: json_at, at: "/assets" }
    paginate: { strategy: cursor, from: "body:/next_key", into: "query:start_key", stop_when: "body.next_key == ''" }
"#;

    fn parse(yaml: &str) -> RestProfile {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    /// Every credential field is checked, not the mode's own alone, and the
    /// refusal names the field and the prefix without echoing the spec.
    #[test]
    fn a_credential_spec_the_resolver_cannot_read_is_refused_at_load() {
        let profile = parse(RUNZERO);
        let bearer = |spec: &str| {
            let instance: RestInstance = serde_yaml_ng::from_str(&format!(
                "profile: x\ntopic: t\nauth: {{ mode: bearer, token: \"{spec}\" }}\n"
            ))
            .unwrap();
            instance.validate(&profile)
        };

        for spec in ["env:RUNZERO_TOKEN", "vault:kv/data/team/x:token"] {
            assert!(bearer(spec).is_empty(), "{spec}: {:?}", bearer(spec));
        }

        let issues = bearer("file:/run/secrets/token");
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.token");
        assert!(
            issues[0].message.contains("is not a credential spec"),
            "{:?}",
            issues[0]
        );
        assert!(
            !issues[0].message.contains("/run/secrets"),
            "the refusal never echoes the spec: {:?}",
            issues[0]
        );

        for (spec, wanted) in [
            ("Vault:kv/data/x:k", "matched exactly"),
            ("vault:kv/data/x", "names no key"),
        ] {
            let issues = bearer(spec);
            assert_eq!(issues.len(), 1, "{spec}: {issues:?}");
            assert!(
                issues[0].message.contains(wanted),
                "{spec}: {:?}",
                issues[0]
            );
        }

        // A field of another mode is checked the same way.
        let oauth: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: oauth2_client_credentials, client_id: id, client_secret: \"bao:kv/data/x:k\" }\n",
        )
        .unwrap();
        let issues = oauth.validate(&profile);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.client_secret");

        // Every spec-carrying field is enumerated, so none escapes the check.
        let mut auth = InstanceAuth {
            token: Some("env:A".into()),
            key: Some("env:A".into()),
            password: Some("env:A".into()),
            client_secret: Some("env:A".into()),
            secret_key: Some("env:A".into()),
            service_account_key: Some("env:A".into()),
            service_account_key_file: Some("env:A".into()),
            private_key: Some("env:A".into()),
            access_key_id: Some("env:A".into()),
            secret_access_key: Some("env:A".into()),
            credentials_json: Some("env:A".into()),
            ..InstanceAuth::default()
        };
        let fields: Vec<&str> = auth
            .credential_specs()
            .into_iter()
            .map(|(field, _)| field)
            .collect();
        assert_eq!(
            fields,
            [
                "token",
                "key",
                "password",
                "client_secret",
                "secret_key",
                "service_account_key",
                "service_account_key_file",
                "private_key",
                "access_key_id",
                "secret_access_key",
                "credentials_json"
            ]
        );
        auth.token = None;
        assert_eq!(
            auth.credential_specs().len(),
            10,
            "only the fields that are set"
        );
    }

    #[test]
    fn the_worked_profiles_parse_and_validate_clean() {
        for yaml in [GITHUB, RUNZERO] {
            let profile = parse(yaml);
            let issues = profile.validate();
            assert!(issues.is_empty(), "{}: {issues:?}", profile.profile);
        }
        let runzero = parse(RUNZERO);
        assert_eq!(runzero.endpoints.len(), 3);
        assert_eq!(runzero.shape, dfe_fetcher_core::UnitShape::Dump);
        assert_eq!(
            runzero.auth.accepts,
            [AuthKind::Bearer, AuthKind::Oauth2ClientCredentials]
        );
        assert_eq!(
            runzero.defaults.query.get("_oid").map(String::as_str),
            Some("{{ vars.org_id }}")
        );
        assert_eq!(
            runzero.endpoints[2].paginate.as_ref().unwrap().strategy,
            PagerStrategy::Cursor
        );
    }

    #[test]
    fn an_unknown_decoder_is_a_load_error_with_line_and_field() {
        let yaml = GITHUB.replace("decoder: json_array", "decoder: xml_soap");
        let err = serde_yaml_ng::from_str::<RestProfile>(&yaml)
            .unwrap_err()
            .to_string();
        assert!(err.contains("xml_soap"), "names the bad value: {err}");
        assert!(err.contains("line"), "carries the line: {err}");
        assert!(
            err.contains("json_array"),
            "lists the accepted values: {err}"
        );
    }

    #[test]
    fn an_unknown_pager_and_shape_are_load_errors() {
        let yaml = GITHUB.replace("strategy: link_header", "strategy: magic");
        let err = serde_yaml_ng::from_str::<RestProfile>(&yaml)
            .unwrap_err()
            .to_string();
        assert!(err.contains("magic") && err.contains("line"), "{err}");
        let yaml = GITHUB.replace("shape: incremental", "shape: snapshot");
        let err = serde_yaml_ng::from_str::<RestProfile>(&yaml)
            .unwrap_err()
            .to_string();
        assert!(err.contains("snapshot") && err.contains("dump"), "{err}");
    }

    #[test]
    fn a_misspelt_key_is_a_load_error_not_a_silently_ignored_setting() {
        let yaml = GITHUB.replace("max_pages: 50", "max_page: 50");
        let err = serde_yaml_ng::from_str::<RestProfile>(&yaml)
            .unwrap_err()
            .to_string();
        assert!(err.contains("max_page") && err.contains("line"), "{err}");
    }

    #[test]
    fn validation_names_the_field_for_each_structural_problem() {
        let mut profile = parse(GITHUB);
        profile.base_url = String::new();
        profile.endpoints[0].unit = String::new();
        profile.endpoints[0].max_pages = Some(0);
        profile.endpoints.push(parse(GITHUB).endpoints[0].clone());
        profile.endpoints.push(parse(GITHUB).endpoints[0].clone());
        let issues = profile.validate();
        let fields: Vec<&str> = issues.iter().map(|i| i.field.as_str()).collect();
        assert!(fields.contains(&"base_url"), "{fields:?}");
        assert!(fields.contains(&"endpoints[0].unit"), "{fields:?}");
        assert!(fields.contains(&"endpoints[0].max_pages"), "{fields:?}");
        assert!(
            fields.contains(&"endpoints[2].unit"),
            "the second audit_log is the duplicate: {fields:?}"
        );
    }

    /// Units that share a method and a body (1Password's three event
    /// classes) declare them once under `defaults`; an endpoint's own still
    /// wins, and a GET unit among POST siblings (GCP's SCC listing next to
    /// the Cloud Logging POSTs) inherits no body at all.
    #[test]
    fn defaults_carry_the_method_and_body_an_endpoint_leaves_unset() {
        let yaml = format!(
            "{}defaults: {{ method: POST, body: {{ limit: \"{{{{ vars.limit }}}}\" }}, rows: {{ decoder: json_at, at: /items }} }}\nendpoints:\n  - {{ unit: a, path: /a }}\n  - {{ unit: b, path: /b, method: GET, body: null }}\n",
            GITHUB.split("endpoints:").next().unwrap()
        );
        let profile = parse(&yaml);
        let [a, b] = profile.endpoints.as_slice() else {
            panic!("two units")
        };
        assert_eq!(profile.method_of(a), Method::Post);
        assert_eq!(
            profile.body_of(a),
            Some(&serde_json::json!({"limit": "{{ vars.limit }}"}))
        );
        assert_eq!(profile.method_of(b), Method::Get, "the endpoint's own wins");
        assert_eq!(
            profile.body_of(b),
            None,
            "a GET never inherits the defaults' body"
        );
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        let own_body_on_get =
            parse(&yaml.replace("method: GET, body: null", "method: GET, body: { q: 1 }"));
        assert!(
            own_body_on_get
                .validate()
                .iter()
                .any(|i| i.field == "endpoints[1].body" && i.message.contains("GET")),
            "an endpoint's own body on a GET is refused: {:?}",
            own_body_on_get.validate()
        );
        assert!(
            parse(GITHUB).endpoints[0].method.is_none()
                && parse(GITHUB).method_of(&parse(GITHUB).endpoints[0]) == Method::Get,
            "unset everywhere is GET"
        );
    }

    /// Units of one API that live on different hosts and audiences (Azure's
    /// Management, Graph and Log Analytics) name their own `base_url` and
    /// `auth.scope`, and a unit's fixed values ride on `vars` under the
    /// instance's per-unit overrides.
    #[test]
    fn a_unit_may_name_its_own_base_url_scope_and_vars() {
        let head = GITHUB.split("endpoints:").next().unwrap().replace(
            "auth: { accepts: [bearer] }",
            "auth:\n  accepts: [oauth2_client_credentials]\n  oauth2_client_credentials: { token_url: \"{{ vars.token_url }}\", scope: \"https://management.azure.com/.default\" }",
        );
        let yaml = format!(
            "{head}endpoints:\n  - unit: signins\n    base_url: \"{{{{ vars.graph_url }}}}\"\n    auth: {{ scope: \"https://graph.microsoft.com/.default\" }}\n    vars: {{ time_field: createdDateTime }}\n    path: /v1.0/auditLogs/signIns\n    query: {{ $filter: \"{{{{ vars.time_field }}}} ge {{{{ window.start }}}}\" }}\n    rows: {{ decoder: json_at, at: /value }}\n  - unit: activity\n    path: /values\n    rows: {{ decoder: json_at, at: /value }}\n"
        );
        let profile = parse(&yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        let [signins, activity] = profile.endpoints.as_slice() else {
            panic!("two units")
        };
        assert_eq!(profile.base_url_of(signins), "{{ vars.graph_url }}");
        assert_eq!(
            profile.base_url_of(activity),
            "{{ vars.api_url }}",
            "the profile's base URL unless the unit names one"
        );
        assert_eq!(
            signins.auth_scope(),
            Some("https://graph.microsoft.com/.default")
        );
        assert_eq!(activity.auth_scope(), None);
        assert_eq!(
            signins.vars.get("time_field"),
            Some(&serde_json::json!("createdDateTime"))
        );

        let literal = parse(&yaml.replace(
            "base_url: \"{{ vars.graph_url }}\"",
            "base_url: \"https://graph.microsoft.com\"",
        ));
        assert!(
            literal
                .validate()
                .iter()
                .any(|i| i.field == "endpoints[0].base_url" && i.message.contains("literal")),
            "{:?}",
            literal.validate()
        );
        let unscoped =
            parse(&yaml.replace("accepts: [oauth2_client_credentials]", "accepts: [bearer]"));
        assert!(
            unscoped
                .validate()
                .iter()
                .any(|i| i.field == "endpoints[0].auth.scope"),
            "a scope needs a token-minting mode: {:?}",
            unscoped.validate()
        );
    }

    /// Many units on one path shape (the Workspace Reports API's one
    /// activity endpoint per application) declare the path once under
    /// `defaults` with `{{ unit.name }}`; a unit without a path anywhere is
    /// refused.
    #[test]
    fn defaults_path_serves_units_that_set_none() {
        let yaml = format!(
            "{}defaults: {{ path: \"/activity/{{{{ unit.name }}}}\", rows: {{ decoder: json_at, at: /items }} }}\nendpoints:\n  - {{ unit: login }}\n  - {{ unit: admin, path: /admin-activity }}\n",
            GITHUB.split("endpoints:").next().unwrap()
        );
        let profile = parse(&yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        assert_eq!(
            profile.path_of(&profile.endpoints[0]),
            "/activity/{{ unit.name }}"
        );
        assert_eq!(
            profile.path_of(&profile.endpoints[1]),
            "/admin-activity",
            "the unit's own path wins"
        );
        let mut none = parse(&yaml);
        none.defaults.path = None;
        assert!(
            none.validate()
                .iter()
                .any(|i| i.field == "endpoints[0].path" && i.message.contains("defaults.path")),
            "{:?}",
            none.validate()
        );
        let mut bad = parse(&yaml);
        bad.defaults.path = Some("/{{ unit.name".into());
        assert!(bad.validate().iter().any(|i| i.field == "defaults.path"));
    }

    /// The JWT-bearer and GCE-metadata modes: the profile shapes the
    /// assertion (token URL, claim templates, lifetime) and names the
    /// metadata URL, the instance supplies exactly one key source.
    #[test]
    fn jwt_bearer_and_gce_metadata_declare_their_shapes_and_the_instance_its_key() {
        let head = GITHUB.split("endpoints:").next().unwrap().replace(
            "auth: { accepts: [bearer] }",
            "auth:\n  accepts: [jwt_bearer, gce_metadata]\n  jwt_bearer:\n    token_url: \"{{ auth.token_uri }}\"\n    claims: { iss: \"{{ auth.client_email }}\", scope: cloud-platform, aud: \"{{ auth.token_url }}\", sub: \"{{ vars.admin_email }}\" }\n    ttl_secs: 3600\n  gce_metadata: { url: \"{{ vars.metadata_url }}\" }",
        );
        let yaml = format!(
            "{head}endpoints:\n  - {{ unit: a, path: /a, rows: {{ decoder: json_array }} }}\n"
        );
        let profile = parse(&yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        assert_eq!(
            profile.auth.accepts,
            [AuthKind::JwtBearer, AuthKind::GceMetadata]
        );
        assert_eq!(profile.auth.jwt_bearer.ttl_secs, 3600);
        assert_eq!(
            profile
                .auth
                .jwt_bearer
                .claims
                .get("sub")
                .map(String::as_str),
            Some("{{ vars.admin_email }}")
        );
        assert_eq!(profile.auth.gce_metadata.url, "{{ vars.metadata_url }}");
        assert!(AuthKind::JwtBearer.is_scoped() && !AuthKind::GceMetadata.is_scoped());

        let mut reserved = parse(&yaml);
        reserved
            .auth
            .jwt_bearer
            .claims
            .insert("exp".into(), "1".into());
        assert!(
            reserved
                .validate()
                .iter()
                .any(|i| i.field == "auth.jwt_bearer.claims.exp"),
            "{:?}",
            reserved.validate()
        );
        let mut no_url = parse(&yaml);
        no_url.auth.jwt_bearer.token_url = String::new();
        no_url.auth.gce_metadata.url = String::new();
        let issues = no_url.validate();
        let fields: Vec<&str> = issues.iter().map(|i| i.field.as_str()).collect();
        assert!(
            fields.contains(&"auth.jwt_bearer.token_url")
                && fields.contains(&"auth.gce_metadata.url"),
            "{fields:?}"
        );

        let two_keys: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: jwt_bearer, service_account_key: \"env:K\", private_key: \"env:P\" }\n",
        )
        .unwrap();
        assert!(
            two_keys
                .validate(&profile)
                .iter()
                .any(|i| i.field == "auth.service_account_key" && i.message.contains("exactly one")),
            "{:?}",
            two_keys.validate(&profile)
        );
        let no_key: RestInstance =
            serde_yaml_ng::from_str("profile: x\ntopic: t\nauth: { mode: jwt_bearer }\n").unwrap();
        assert!(!no_key.validate(&profile).is_empty());
        let file_key: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: jwt_bearer, service_account_key_file: \"env:K\" }\n",
        )
        .unwrap();
        assert!(file_key.validate(&profile).is_empty());
        let metadata: RestInstance =
            serde_yaml_ng::from_str("profile: x\ntopic: t\nauth: { mode: gce_metadata }\n")
                .unwrap();
        assert!(
            metadata.validate(&profile).is_empty(),
            "the metadata server needs no identity field"
        );
    }

    /// `rows.builder` is a closed vocabulary like every other axis.
    #[test]
    fn a_rows_builder_is_a_closed_vocabulary() {
        let yaml = GITHUB.replace(
            "rows: { decoder: json_array }",
            "rows: { decoder: json_at, at: /tables, builder: columnar_table }",
        );
        let profile = parse(&yaml);
        assert_eq!(
            profile.rows_of(&profile.endpoints[0]).builder,
            Some(RowBuilderKind::ColumnarTable)
        );
        assert!(profile.validate().is_empty());
        let err = serde_yaml_ng::from_str::<RestProfile>(&yaml.replace("columnar_table", "magic"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("magic") && err.contains("columnar_table"),
            "{err}"
        );
    }

    /// A keyset unit (PyPI: one document per configured package) names the
    /// list its keys come from, reads `{{ key }}` in its templates and its
    /// `add_fields`, and may ignore a status per key (a 404 for a package
    /// the registry does not know).
    #[test]
    fn a_keyset_unit_reads_key_in_its_templates_and_add_fields() {
        let yaml = format!(
            "{}vars: {{ packages: [] }}\nendpoints:\n  - unit: metadata\n    path: \"/pypi/{{{{ key }}}}/json\"\n    rows: {{ decoder: document }}\n    construct: {{ keyset: {{ from: \"{{{{ vars.packages }}}}\" }} }}\n    ignore_status: [404]\n    add_fields: {{ _dfe_fetcher_package: \"{{{{ key }}}}\" }}\n",
            GITHUB.split("endpoints:").next().unwrap()
        );
        let profile = parse(&yaml);
        let endpoint = &profile.endpoints[0];
        assert_eq!(
            endpoint
                .construct
                .as_ref()
                .and_then(|c| c.keyset.as_ref())
                .and_then(|k| k.from.as_deref()),
            Some("{{ vars.packages }}")
        );
        assert_eq!(endpoint.ignore_status, [404]);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());

        let no_keyset = parse(&yaml.replace(
            "    construct: { keyset: { from: \"{{ vars.packages }}\" } }\n",
            "",
        ));
        let issues = no_keyset.validate();
        assert!(
            issues
                .iter()
                .any(|i| i.field.ends_with("add_fields._dfe_fetcher_package")
                    && i.message.contains("key")),
            "`key` without a keyset is refused: {issues:?}"
        );
        let bad_status = parse(&yaml.replace("ignore_status: [404]", "ignore_status: [42]"));
        assert!(
            bad_status
                .validate()
                .iter()
                .any(|i| i.field.ends_with("ignore_status")),
            "{:?}",
            bad_status.validate()
        );
        let bad_from = parse(&yaml.replace("{{ vars.packages }}", "{{ vars.packages"));
        assert!(
            bad_from
                .validate()
                .iter()
                .any(|i| i.field.ends_with("construct.keyset.from")),
            "{:?}",
            bad_from.validate()
        );
    }

    /// A lookup unit (CrowdStrike alerts): the endpoint's pages carry ids,
    /// walked by offset against a total, and the rows come from a second
    /// POST per batch of ids whose body reads `{{ ids }}`.
    #[test]
    fn a_lookup_unit_declares_the_second_request_and_its_rows() {
        let yaml = format!(
            "{}vars: {{ limit: 100 }}\nendpoints:\n  - unit: alerts\n    path: /alerts/queries/alerts/v2\n    query: {{ limit: \"{{{{ vars.limit }}}}\" }}\n    rows: {{ decoder: json_at, at: /resources }}\n    paginate: {{ strategy: offset, param: offset, total_at: /meta/pagination/total }}\n    construct:\n      lookup:\n        batch: 1000\n        request: {{ path: /alerts/entities/alerts/v2, body: {{ composite_ids: \"{{{{ ids }}}}\" }} }}\n        rows: {{ decoder: json_at, at: /resources }}\n",
            GITHUB.split("endpoints:").next().unwrap()
        );
        let profile = parse(&yaml);
        let lookup = profile.endpoints[0].lookup().expect("a lookup");
        assert_eq!(lookup.batch, 1000);
        assert!(lookup.request.method.is_none());
        assert_eq!(
            lookup.request.method_or(Method::Post),
            Method::Post,
            "POST by default"
        );
        assert_eq!(lookup.request.path, "/alerts/entities/alerts/v2");
        assert_eq!(lookup.rows.at.as_deref(), Some("/resources"));
        assert!(lookup.id_at.is_none(), "the page rows are the ids");
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        assert_eq!(
            profile.paginate_of(&profile.endpoints[0]).page_size,
            None,
            "offset with a total needs no page_size"
        );

        let bad = parse(&yaml.replace("batch: 1000", "batch: 0"));
        assert!(
            bad.validate()
                .iter()
                .any(|i| i.field.ends_with("construct.lookup.batch")),
            "{:?}",
            bad.validate()
        );
        let no_path = parse(&yaml.replace("path: /alerts/entities/alerts/v2, ", ""));
        assert!(
            no_path
                .validate()
                .iter()
                .any(|i| i.field.ends_with("construct.lookup.request.path")),
            "{:?}",
            no_path.validate()
        );
        let no_total = parse(&yaml.replace(", total_at: /meta/pagination/total", ""));
        assert!(
            no_total
                .validate()
                .iter()
                .any(|i| i.field.ends_with("paginate.page_size")),
            "offset without a total needs a page_size: {:?}",
            no_total.validate()
        );
    }

    #[test]
    fn a_body_reading_pager_needs_a_page_bounded_decoder() {
        let mut profile = parse(RUNZERO);
        profile.endpoints[2].rows = Some(RowsSpec {
            decoder: DecoderKind::Ndjson,
            ..RowsSpec::default()
        });
        let issues = profile.validate();
        assert!(
            issues
                .iter()
                .any(|i| i.field == "endpoints[2].paginate" && i.message.contains("body")),
            "{issues:?}"
        );
    }

    #[test]
    fn json_at_needs_a_pointer_and_pointers_start_with_a_slash() {
        let mut profile = parse(RUNZERO);
        profile.endpoints[2].rows.as_mut().unwrap().at = None;
        assert!(
            profile
                .validate()
                .iter()
                .any(|i| i.field == "endpoints[2].rows.at")
        );
        profile.endpoints[2].rows.as_mut().unwrap().at = Some("assets".into());
        assert!(
            profile
                .validate()
                .iter()
                .any(|i| i.field == "endpoints[2].rows.at")
        );
        profile.endpoints[0].row_key = Some("id".into());
        assert!(
            profile
                .validate()
                .iter()
                .any(|i| i.field == "endpoints[0].row_key")
        );
    }

    #[test]
    fn a_profile_may_not_carry_identity() {
        let yaml = RUNZERO.replace(
            "accepts: [bearer, oauth2_client_credentials]",
            "accepts: [bearer]\n  token: \"vault:kv/data/team/x:token\"",
        );
        let err = serde_yaml_ng::from_str::<RestProfile>(&yaml)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("token"),
            "identity fields are not in the profile grammar: {err}"
        );
        let yaml = RUNZERO.replace("{{ vars.base_url }}", "https://literal.example/api");
        let issues = parse(&yaml).validate();
        assert!(
            issues
                .iter()
                .any(|i| i.field == "base_url" && i.message.contains("literal")),
            "{issues:?}"
        );
    }

    #[test]
    fn window_formats_and_durations_parse_or_fail_at_load() {
        assert_eq!(
            parse(GITHUB).window.format,
            WindowFormat::Strftime("%Y-%m-%dT%H:%M:%S+00:00".into())
        );
        for (text, expected) in [
            ("rfc3339_secs", WindowFormat::Rfc3339Secs),
            ("rfc3339_millis", WindowFormat::Rfc3339Millis),
            ("epoch_secs", WindowFormat::EpochSecs),
            ("epoch_millis", WindowFormat::EpochMillis),
        ] {
            let yaml = GITHUB.replace("strftime:%Y-%m-%dT%H:%M:%S+00:00", text);
            assert_eq!(parse(&yaml).window.format, expected, "{text}");
        }
        let yaml = GITHUB.replace("strftime:%Y-%m-%dT%H:%M:%S+00:00", "iso");
        assert!(serde_yaml_ng::from_str::<RestProfile>(&yaml).is_err());
        assert_eq!(
            parse_duration("24h").unwrap(),
            std::time::Duration::from_hours(24)
        );
        assert_eq!(
            parse_duration("90s").unwrap(),
            std::time::Duration::from_secs(90)
        );
        assert_eq!(
            parse_duration("15m").unwrap(),
            std::time::Duration::from_mins(15)
        );
        assert_eq!(
            parse_duration("2d").unwrap(),
            std::time::Duration::from_hours(48)
        );
        assert!(parse_duration("1 hour").is_err());
        assert!(parse_duration("0h").is_err());
        let yaml = GITHUB.replace("lookback: 1h", "lookback: soon");
        assert!(serde_yaml_ng::from_str::<RestProfile>(&yaml).is_err());
    }

    #[test]
    fn retry_status_matches_accept_codes_and_classes() {
        let yaml = GITHUB.replace(
            "never_retry: [401, 403]",
            "never_retry: [401, 403], retry_on: [408, 429, 5xx]",
        );
        let profile = parse(&yaml);
        assert_eq!(
            profile.retry.retry_on,
            [
                StatusMatch::Code(408),
                StatusMatch::Code(429),
                StatusMatch::Class(5)
            ]
        );
        assert!(profile.retry.retries(503));
        assert!(profile.retry.retries(429));
        assert!(!profile.retry.retries(404));
        assert!(
            !profile.retry.retries(401),
            "never_retry wins over a class match"
        );
        let default = RetrySpec::default();
        assert!(default.retries(500) && default.retries(429) && default.retries(408));
        assert!(!default.retries(400));
        let bad = GITHUB.replace("never_retry: [401, 403]", "retry_on: [\"teapot\"]");
        assert!(serde_yaml_ng::from_str::<RestProfile>(&bad).is_err());
    }

    #[test]
    fn instance_validation_checks_mode_identity_and_unit_names() {
        let profile = parse(RUNZERO);
        let instance: RestInstance = serde_yaml_ng::from_str(
            r#"
enabled: true
profile: runzero
topic: runzero
auth: { mode: basic, username: u, password: p }
vars: { base_url: "https://runzero.example.internal/api/v1.0" }
units: { assets: { query: { fields: "id,alive" } }, nope: {} }
"#,
        )
        .unwrap();
        let issues = instance.validate(&profile);
        let fields: Vec<&str> = issues.iter().map(|i| i.field.as_str()).collect();
        assert!(
            fields.contains(&"auth.mode"),
            "basic is not in accepts: {fields:?}"
        );
        assert!(fields.contains(&"units.nope"), "{fields:?}");

        let bearer_without_token: RestInstance = serde_yaml_ng::from_str(
            "profile: runzero\ntopic: runzero\nauth: { mode: bearer }\nvars: { base_url: x }\n",
        )
        .unwrap();
        assert!(
            bearer_without_token
                .validate(&profile)
                .iter()
                .any(|i| i.field == "auth.token")
        );
        let oauth_without_secret: RestInstance = serde_yaml_ng::from_str(
            "profile: runzero\ntopic: runzero\nauth: { mode: oauth2_client_credentials, client_id: id }\nvars: { base_url: x }\n",
        )
        .unwrap();
        assert!(
            oauth_without_secret
                .validate(&profile)
                .iter()
                .any(|i| i.field == "auth.client_secret")
        );
    }

    #[test]
    fn instance_secrets_are_redacted_on_serialise() {
        let instance: RestInstance = serde_yaml_ng::from_str(
            "profile: runzero\ntopic: runzero\nauth: { mode: bearer, token: super-secret }\n",
        )
        .unwrap();
        let json = serde_json::to_string(&instance).unwrap();
        assert!(!json.contains("super-secret"), "{json}");
        assert_eq!(
            instance.auth.token.as_ref().unwrap().expose(),
            "super-secret"
        );
    }

    /// A profile may name the request its health check sends; the path is a
    /// template like any other and a GET carries no body.
    #[test]
    fn a_probe_is_a_templated_get_validated_like_an_endpoint() {
        let yaml = format!("{GITHUB}probe: {{ path: \"/user\" }}\n");
        let profile = parse(&yaml);
        let probe = profile.probe.as_ref().expect("probe parsed");
        assert_eq!(probe.path, "/user");
        assert_eq!(probe.method, Method::Get);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());

        let templated = format!("{GITHUB}probe: {{ path: \"/orgs/{{{{ vars.org }}}}\" }}\n");
        assert!(parse(&templated).validate().is_empty());

        let mut bad = parse(GITHUB);
        bad.probe = Some(ProbeSpec {
            path: "/{{ vars.org".into(),
            ..ProbeSpec::default()
        });
        let issues = bad.validate();
        assert!(
            issues.iter().any(|i| i.field == "probe.path"),
            "an unterminated template names the field: {issues:?}"
        );
        let mut empty = parse(GITHUB);
        empty.probe = Some(ProbeSpec::default());
        assert!(
            empty
                .validate()
                .iter()
                .any(|i| i.field == "probe.path" && i.message.contains("required"))
        );
        assert!(parse(GITHUB).probe.is_none(), "no probe unless declared");
    }

    /// An API that answers a bad credential with a 2xx (Slack's `ok: false`)
    /// needs the probe to read the body: `fail_when` is a predicate like an
    /// endpoint's, validated at load.
    #[test]
    fn a_probe_may_fail_on_a_body_predicate() {
        let yaml =
            format!("{GITHUB}probe: {{ path: /api/auth.test, fail_when: \"body.ok == false\" }}\n");
        let profile = parse(&yaml);
        assert_eq!(
            profile.probe.as_ref().unwrap().fail_when.as_deref(),
            Some("body.ok == false")
        );
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());

        let mut bad = parse(GITHUB);
        bad.probe = Some(ProbeSpec {
            path: "/api/auth.test".into(),
            fail_when: Some("body.ok ==".into()),
            ..ProbeSpec::default()
        });
        assert!(
            bad.validate().iter().any(|i| i.field == "probe.fail_when"),
            "{:?}",
            bad.validate()
        );
    }

    /// The SigV4 mode: the profile names the signing scope as two templates
    /// (so units vary the service and a region-locked unit its region
    /// through `vars`), the instance supplies the key pair or one JSON
    /// document carrying both halves.
    #[test]
    fn sigv4_names_its_scope_as_templates_and_the_instance_its_keys() {
        let head = GITHUB.split("endpoints:").next().unwrap().replace(
            "auth: { accepts: [bearer] }",
            "auth:\n  accepts: [sigv4]\n  sigv4: { service: \"{{ vars.service }}\", region: \"{{ vars.region }}\" }",
        );
        let yaml = format!(
            "{head}endpoints:\n  - {{ unit: cloudtrail, path: /, vars: {{ service: cloudtrail }}, rows: {{ decoder: json_at, at: /Events }} }}\n  - {{ unit: health, path: /, vars: {{ service: health, region: us-east-1 }}, rows: {{ decoder: json_at, at: /events }} }}\n"
        );
        let profile = parse(&yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        assert_eq!(profile.auth.accepts, [AuthKind::SigV4]);
        assert_eq!(profile.auth.sigv4.service, "{{ vars.service }}");
        assert_eq!(profile.auth.sigv4.region, "{{ vars.region }}");
        assert!(!AuthKind::SigV4.is_scoped(), "signing mints no token");
        assert_eq!(
            profile.endpoints[1].vars.get("region"),
            Some(&serde_json::json!("us-east-1")),
            "a region-locked unit overrides the region through its vars"
        );

        let mut blank = parse(&yaml);
        blank.auth.sigv4.service = String::new();
        blank.auth.sigv4.region = "{{ vars.region".into();
        let fields: Vec<String> = blank.validate().into_iter().map(|i| i.field).collect();
        assert!(
            fields.contains(&"auth.sigv4.service".to_string()),
            "{fields:?}"
        );
        assert!(
            fields.contains(&"auth.sigv4.region".to_string()),
            "{fields:?}"
        );

        let pair: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: sigv4, access_key_id: AKIA, secret_access_key: \"env:K\" }\n",
        )
        .unwrap();
        assert!(pair.validate(&profile).is_empty());
        let document: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: sigv4, credentials_json: \"vault:kv/data/aws:credentials\" }\n",
        )
        .unwrap();
        assert!(document.validate(&profile).is_empty());
        let half: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: sigv4, access_key_id: AKIA }\n",
        )
        .unwrap();
        assert!(
            half.validate(&profile)
                .iter()
                .any(|i| i.field == "auth.access_key_id"),
            "{:?}",
            half.validate(&profile)
        );
        let both: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: sigv4, access_key_id: AKIA, secret_access_key: s, credentials_json: j }\n",
        )
        .unwrap();
        assert!(
            !both.validate(&profile).is_empty(),
            "one identity form, not two"
        );

        let role: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: sigv4, access_key_id: AKIA, secret_access_key: s, assume_role_arn: \"arn:aws:iam::123456789012:role/dfe-reader\" }\n",
        )
        .unwrap();
        assert!(
            role.validate(&profile).is_empty(),
            "{:?}",
            role.validate(&profile)
        );
        let malformed: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: sigv4, access_key_id: AKIA, secret_access_key: s, assume_role_arn: \"arn:aws:iam::123:user/nope\" }\n",
        )
        .unwrap();
        assert!(
            malformed
                .validate(&profile)
                .iter()
                .any(|i| i.field == "auth.assume_role_arn" && i.message.contains("role ARN")),
            "{:?}",
            malformed.validate(&profile)
        );
        let govcloud: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: sigv4, access_key_id: AKIA, secret_access_key: s, assume_role_arn: \"arn:aws-us-gov:iam::123456789012:role/dfe-reader\" }\n",
        )
        .unwrap();
        assert!(
            govcloud.validate(&profile).is_empty(),
            "another partition is checked against the region at the first signing"
        );
        let bearer_profile = parse(GITHUB);
        let not_sigv4: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: bearer, token: t, assume_role_arn: \"arn:aws:iam::123456789012:role/dfe-reader\" }\n",
        )
        .unwrap();
        assert!(
            not_sigv4
                .validate(&bearer_profile)
                .iter()
                .any(|i| i.field == "auth.assume_role_arn" && i.message.contains("sigv4")),
            "{:?}",
            not_sigv4.validate(&bearer_profile)
        );
    }

    /// A keyset whose keys come from a request the unit sends first
    /// (GuardDuty's detector ids), and a lookup whose batch responses page.
    #[test]
    fn a_keyset_may_come_from_a_request_and_a_lookup_may_page() {
        let yaml = format!(
            "{}endpoints:\n  - unit: findings\n    method: POST\n    path: \"/detector/{{{{ key }}}}/findings\"\n    body: {{ maxResults: 50 }}\n    rows: {{ decoder: json_at, at: /findingIds }}\n    paginate: {{ strategy: cursor, from: \"body:/nextToken\", into: \"body:/nextToken\" }}\n    construct:\n      keyset:\n        request: {{ method: GET, path: /detector }}\n        keys_at: /detectorIds\n      lookup:\n        batch: 50\n        request: {{ path: \"/detector/{{{{ key }}}}/findings/get\", body: {{ findingIds: \"{{{{ ids }}}}\" }} }}\n        rows: {{ decoder: json_at, at: /findings }}\n        paginate: {{ strategy: cursor, from: \"body:/NextToken\", into: \"body:/NextToken\" }}\n        max_pages: 10\n",
            GITHUB.split("endpoints:").next().unwrap()
        );
        let profile = parse(&yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        let keyset = profile.endpoints[0].keyset().expect("a keyset");
        assert!(keyset.from.is_none());
        assert_eq!(
            keyset.request.as_ref().map(|r| (r.method, r.path.as_str())),
            Some((Some(Method::Get), "/detector"))
        );
        assert_eq!(keyset.keys_at.as_deref(), Some("/detectorIds"));
        let lookup = profile.endpoints[0].lookup().expect("a lookup");
        assert_eq!(
            lookup.paginate.as_ref().map(|p| p.strategy),
            Some(PagerStrategy::Cursor)
        );
        assert_eq!(lookup.max_pages, Some(10));

        let no_pointer = parse(&yaml.replace("        keys_at: /detectorIds\n", ""));
        assert!(
            no_pointer
                .validate()
                .iter()
                .any(|i| i.field.ends_with("construct.keyset.keys_at")),
            "{:?}",
            no_pointer.validate()
        );
        let both = parse(&yaml.replace(
            "        request: { method: GET, path: /detector }\n",
            "        request: { method: GET, path: /detector }\n        from: \"{{ vars.detectors }}\"\n",
        ));
        assert!(
            both.validate()
                .iter()
                .any(|i| i.field.ends_with("construct.keyset.from")
                    && i.message.contains("exactly one")),
            "{:?}",
            both.validate()
        );
        let streamed = parse(&yaml.replace(
            "        rows: { decoder: json_at, at: /findings }\n",
            "        rows: { decoder: json_array }\n",
        ));
        assert!(
            streamed
                .validate()
                .iter()
                .any(|i| i.field.ends_with("construct.lookup.paginate")),
            "a body-reading lookup pager needs a page-bounded decoder: {:?}",
            streamed.validate()
        );
    }

    /// A unit may render the window in its own format, and a row spec may
    /// say its framed rows are JSON strings holding the rows.
    #[test]
    fn a_unit_window_format_and_quoted_rows_are_grammar() {
        let yaml = format!(
            "{}window: {{ format: epoch_secs }}\nendpoints:\n  - {{ unit: secs, path: /a, rows: {{ decoder: json_at, at: /a }} }}\n  - {{ unit: millis, path: /b, window: {{ format: epoch_millis }}, rows: {{ decoder: json_at, at: /Results, quoted: true }} }}\n",
            GITHUB.split("endpoints:").next().unwrap().replace(
                "window: { format: \"strftime:%Y-%m-%dT%H:%M:%S+00:00\", lookback: 1h }\n",
                ""
            )
        );
        let profile = parse(&yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        assert_eq!(
            profile.window_of(&profile.endpoints[0]).format,
            WindowFormat::EpochSecs,
            "the profile's format unless the unit names one"
        );
        assert_eq!(
            profile.window_of(&profile.endpoints[1]).format,
            WindowFormat::EpochMillis
        );
        assert!(!profile.rows_of(&profile.endpoints[0]).quoted);
        assert!(profile.rows_of(&profile.endpoints[1]).quoted);
        let built =
            parse(&yaml.replace("quoted: true", "quoted: true, builder: cloudwatch_metrics"));
        assert_eq!(
            built.rows_of(&built.endpoints[1]).builder,
            Some(RowBuilderKind::CloudwatchMetrics)
        );
    }

    /// A body member whose leaf renders `null` is left out, so a profile
    /// writes an optional member as a ternary to null; a typed leaf and a
    /// literal null in an array stay as they are.
    #[test]
    fn a_body_member_that_renders_null_is_left_out() {
        let mut ctx = TemplateCtx::new();
        ctx.set("vars", serde_json::json!({"pattern": "", "limit": 10}));
        let body = serde_json::json!({
            "logGroupName": "g",
            "filterPattern": "{{ vars.pattern != '' ? vars.pattern : null }}",
            "limit": "{{ vars.limit }}",
            "nested": {"gone": "{{ null }}", "kept": [null, "{{ vars.limit }}"]}
        });
        assert_eq!(
            bound::render_body(&body, &ctx).unwrap(),
            serde_json::json!({"logGroupName": "g", "limit": 10, "nested": {"kept": [null, 10]}})
        );
        ctx.set("vars", serde_json::json!({"pattern": "ERROR", "limit": 10}));
        assert_eq!(
            bound::render_body(&body, &ctx).unwrap()["filterPattern"],
            "ERROR"
        );
    }

    /// A manifest unit (the OMAP content list): the pages' rows are items,
    /// each fetched by its own GET at the URL the item names, the rows
    /// framed by the manifest's own spec, with an item checkpoint from two
    /// templates that come together or not at all; a unit takes a lookup or
    /// a manifest, never both.
    #[test]
    fn a_manifest_unit_declares_its_item_request_rows_and_checkpoint() {
        let yaml = format!(
            "{}endpoints:\n  - unit: content\n    path: /content\n    rows: {{ decoder: json_array }}\n    paginate: {{ strategy: request_path, from: \"header:NextPageUri\" }}\n    construct:\n      manifest:\n        item_request: {{ path: \"{{{{ item.contentUri }}}}\", ignore_status: [404] }}\n        rows: {{ decoder: json_array }}\n        key: \"{{{{ item.contentId }}}}\"\n        position: \"{{{{ item.contentCreated }}}}\"\n",
            GITHUB.split("endpoints:").next().unwrap()
        );
        let profile = parse(&yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        let manifest = profile
            .manifest_of(&profile.endpoints[0])
            .expect("a manifest");
        assert!(manifest.item_request.method.is_none());
        assert_eq!(
            manifest.item_request.method_or(Method::Get),
            Method::Get,
            "an item is fetched, so GET by default"
        );
        assert_eq!(manifest.item_request.path, "{{ item.contentUri }}");
        assert_eq!(manifest.item_request.ignore_status, [404]);
        assert_eq!(manifest.rows.decoder, DecoderKind::JsonArray);
        assert_eq!(manifest.key.as_deref(), Some("{{ item.contentId }}"));
        assert_eq!(
            manifest.position.as_deref(),
            Some("{{ item.contentCreated }}")
        );
        assert!(profile.lookup_of(&profile.endpoints[0]).is_none());

        let half = parse(&yaml.replace("        key: \"{{ item.contentId }}\"\n", ""));
        assert!(
            half.validate()
                .iter()
                .any(|i| i.field.ends_with("construct.manifest.position")
                    && i.message.contains("both")),
            "{:?}",
            half.validate()
        );
        let no_path = parse(&yaml.replace("path: \"{{ item.contentUri }}\", ", ""));
        assert!(
            no_path
                .validate()
                .iter()
                .any(|i| i.field.ends_with("construct.manifest.item_request.path")),
            "{:?}",
            no_path.validate()
        );
        let both = parse(&yaml.replace(
            "    construct:\n      manifest:\n",
            "    construct:\n      lookup: { request: { path: /x }, rows: { decoder: json_array } }\n      manifest:\n",
        ));
        assert!(
            both.validate()
                .iter()
                .any(|i| i.field.ends_with("construct.manifest") && i.message.contains("not two")),
            "{:?}",
            both.validate()
        );
        let bad_status = parse(&yaml.replace("ignore_status: [404]", "ignore_status: [7]"));
        assert!(
            bad_status.validate().iter().any(|i| i
                .field
                .ends_with("construct.manifest.item_request.ignore_status")),
            "{:?}",
            bad_status.validate()
        );
    }

    /// A prelude (the OMAP `subscriptions/start`): idempotent requests
    /// sent once per tick before the unit's first page, POST by default,
    /// each able to ignore a status, rendering from the unit's vars alone.
    #[test]
    fn a_prelude_is_a_list_of_idempotent_requests_before_the_first_page() {
        let yaml = format!(
            "{}endpoints:\n  - unit: content\n    path: /content\n    query: {{ contentType: \"{{{{ vars.content_type }}}}\" }}\n    rows: {{ decoder: json_array }}\n    prelude:\n      - path: /subscriptions/start\n        query: {{ contentType: \"{{{{ vars.content_type }}}}\" }}\n        ignore_status: [400]\n",
            GITHUB.split("endpoints:").next().unwrap()
        );
        let profile = parse(&yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        let prelude = profile.prelude_of(&profile.endpoints[0]);
        assert_eq!(prelude.len(), 1);
        assert_eq!(
            prelude[0].method_or(Method::Post),
            Method::Post,
            "a prelude step is an action, so POST by default"
        );
        assert_eq!(prelude[0].path, "/subscriptions/start");
        assert_eq!(prelude[0].ignore_status, [400]);

        let windowed = parse(&yaml.replace(
            "query: { contentType: \"{{ vars.content_type }}\" }\n        ignore_status",
            "query: { since: \"{{ window.start }}\" }\n        ignore_status",
        ));
        assert!(
            windowed
                .validate()
                .iter()
                .any(|i| i.field == "endpoints[0].prelude[0]" && i.message.contains("window")),
            "a step cannot read the window: {:?}",
            windowed.validate()
        );
        let keyed = parse(&yaml.replace(
            "      - path: /subscriptions/start\n",
            "      - path: /subscriptions/start\n        body: { id: \"{{ key }}\" }\n",
        ));
        assert!(
            keyed
                .validate()
                .iter()
                .any(|i| i.field == "endpoints[0].prelude[0]" && i.message.contains("key")),
            "nor a key, in the body either: {:?}",
            keyed.validate()
        );
        let bad = parse(&yaml.replace(
            "      - path: /subscriptions/start\n",
            "      - path: \"\"\n",
        ));
        assert!(
            bad.validate()
                .iter()
                .any(|i| i.field == "endpoints[0].prelude[0].path"),
            "{:?}",
            bad.validate()
        );
    }

    /// Units that share a prelude and a construct (the seven OMAP feeds)
    /// declare them once under `defaults`; a unit's own replaces the
    /// default whole, and an empty own list or construct opts out.
    #[test]
    fn defaults_carry_the_prelude_and_construct_an_endpoint_leaves_unset() {
        let yaml = format!(
            "{}defaults:\n  prelude: [{{ path: /start }}]\n  construct: {{ manifest: {{ item_request: {{ path: \"{{{{ item.uri }}}}\" }} }} }}\nendpoints:\n  - {{ unit: a, path: /a, rows: {{ decoder: json_array }} }}\n  - {{ unit: b, path: /b, rows: {{ decoder: json_array }}, prelude: [], construct: {{}} }}\n  - {{ unit: c, path: /c, rows: {{ decoder: json_array }}, construct: {{ keyset: {{ from: \"{{{{ vars.keys }}}}\" }} }} }}\n",
            GITHUB.split("endpoints:").next().unwrap()
        );
        let profile = parse(&yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        let [a, b, c] = profile.endpoints.as_slice() else {
            panic!("three units")
        };
        assert_eq!(profile.prelude_of(a).len(), 1, "inherited");
        assert!(profile.manifest_of(a).is_some(), "inherited");
        assert!(profile.prelude_of(b).is_empty(), "opted out");
        assert!(profile.manifest_of(b).is_none(), "opted out");
        assert!(profile.keyset_of(c).is_some(), "its own");
        assert!(
            profile.manifest_of(c).is_none(),
            "an own construct replaces the default whole"
        );
        assert_eq!(profile.prelude_of(c).len(), 1, "the prelude still inherits");

        let bad = parse(&yaml.replace("prelude: [{ path: /start }]", "prelude: [{ path: \"\" }]"));
        assert!(
            bad.validate()
                .iter()
                .any(|i| i.field == "defaults.prelude[0].path"),
            "{:?}",
            bad.validate()
        );
    }

    #[test]
    fn an_inline_profile_and_a_named_profile_both_deserialise() {
        let named: RestInstance = serde_yaml_ng::from_str("profile: runzero\ntopic: t\n").unwrap();
        assert!(matches!(named.profile, ProfileRef::Named(ref n) if n == "runzero"));
        let inline: RestInstance = serde_yaml_ng::from_str(
            "topic: t\nprofile:\n  base_url: \"{{ vars.u }}\"\n  endpoints: [{ unit: a, path: /a }]\n",
        )
        .unwrap();
        assert!(matches!(inline.profile, ProfileRef::Inline(_)));
    }

    /// A manifest bounds the items it fetches per key per tick and stamps
    /// fields rendered per item (an object envelope with template leaves)
    /// on every row of the item; a folding builder is named under the
    /// unit's `fold`, never under `rows.builder`, and a per-row builder
    /// never under `fold`.
    #[test]
    fn a_manifest_bounds_its_items_and_stamps_per_item_fields_and_a_fold_is_the_units() {
        let yaml = format!(
            "{}endpoints:\n  - unit: metadata\n    path: \"/{{{{ key }}}}/@v/list\"\n    rows: {{ decoder: lines }}\n    fold: go_module_aggregate\n    add_fields:\n      _dfe_fetcher_module: \"{{{{ key }}}}\"\n    construct:\n      keyset: {{ from: \"{{{{ vars.modules }}}}\" }}\n      manifest:\n        item_request: {{ path: \"/{{{{ key }}}}/@v/{{{{ item.line }}}}.info\", ignore_status: [404] }}\n        rows: {{ decoder: document }}\n        max_items: 100\n        add_fields:\n          _dfe_fetcher_object: {{ provider: s3, key: \"{{{{ item.line }}}}\", size: \"{{{{ item.size }}}}\" }}\n",
            GITHUB.split("endpoints:").next().unwrap()
        );
        let profile = parse(&yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        let endpoint = &profile.endpoints[0];
        assert_eq!(endpoint.fold, Some(RowBuilderKind::GoModuleAggregate));
        let manifest = profile.manifest_of(endpoint).expect("a manifest");
        assert_eq!(manifest.max_items, Some(100));
        assert_eq!(
            manifest.add_fields["_dfe_fetcher_object"]["key"], "{{ item.line }}",
            "a JSON object whose leaves are templates"
        );

        let zero = parse(&yaml.replace("max_items: 100", "max_items: 0"));
        assert!(
            zero.validate()
                .iter()
                .any(|i| i.field.ends_with("construct.manifest.max_items")),
            "{:?}",
            zero.validate()
        );
        let bad_leaf = parse(&yaml.replace("size: \"{{ item.size }}\"", "size: \"{{ item. }}\""));
        assert!(
            bad_leaf.validate().iter().any(|i| i
                .field
                .ends_with("construct.manifest.add_fields._dfe_fetcher_object.size")),
            "{:?}",
            bad_leaf.validate()
        );
        let per_row = parse(&yaml.replace("fold: go_module_aggregate", "fold: columnar_table"));
        assert!(
            per_row
                .validate()
                .iter()
                .any(|i| i.field.ends_with("].fold") && i.message.contains("per-row")),
            "{:?}",
            per_row.validate()
        );
        let as_builder = parse(&yaml.replace(
            "rows: { decoder: lines }\n    fold: go_module_aggregate",
            "rows: { decoder: lines, builder: go_module_aggregate }",
        ));
        assert!(
            as_builder
                .validate()
                .iter()
                .any(|i| i.field.ends_with("rows.builder") && i.message.contains("fold")),
            "{:?}",
            as_builder.validate()
        );
    }

    /// A lister unit names the listing protocol and leaves framing and
    /// paging to it: `rows` and `paginate` are refused, a manifest is
    /// required to consume the items, and the token-minting modes may
    /// expose named top-level fields of the token response, never the
    /// credentials themselves.
    #[test]
    fn a_lister_owns_its_framing_and_paging_and_a_mode_exposes_named_fields_only() {
        let yaml = "profile: s3\nbase_url: \"{{ vars.endpoint }}\"\nauth:\n  accepts: [sigv4, oauth2_client_credentials]\n  sigv4: { service: s3, region: \"{{ vars.region }}\" }\n  oauth2_client_credentials: { token_url: \"{{ base_url }}/token\", expose: [instance_url] }\nendpoints:\n  - unit: objects\n    lister: s3\n    path: \"{{ vars.bucket }}/\"\n    query: { list-type: 2, prefix: \"{{ vars.prefix }}\", max-keys: 1000 }\n    max_pages: 10\n    construct:\n      manifest:\n        item_request: { path: \"{{ vars.bucket }}/{{ item.path }}\" }\n        rows: { decoder: ndjson, gzip: true }\n        key: \"{{ item.key }}\"\n        position: \"{{ item.last_modified }}\"\n        max_items: 1000\n";
        let profile = parse(yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        assert_eq!(profile.endpoints[0].lister, Some(ListerKind::S3));
        assert_eq!(
            profile.auth.oauth2_client_credentials.expose,
            ["instance_url"]
        );
        assert!(!profile.is_queue());

        let framed = parse(&yaml.replace(
            "    lister: s3\n",
            "    lister: s3\n    rows: { decoder: json_array }\n    paginate: { strategy: link_header }\n",
        ));
        let issues = framed.validate();
        assert!(
            issues.iter().any(|i| i.field.ends_with("].rows"))
                && issues.iter().any(|i| i.field.ends_with("].paginate")),
            "{issues:?}"
        );
        let no_manifest = parse(yaml.split("    construct:").next().unwrap());
        assert!(
            no_manifest
                .validate()
                .iter()
                .any(|i| i.field.ends_with("].lister") && i.message.contains("manifest")),
            "{:?}",
            no_manifest.validate()
        );
        let leak = parse(&yaml.replace("expose: [instance_url]", "expose: [refresh_token]"));
        assert!(
            leak.validate()
                .iter()
                .any(|i| i.field == "auth.oauth2_client_credentials.expose"
                    && i.message.contains("credential")),
            "{:?}",
            leak.validate()
        );
    }

    /// A queue unit: its rows carry an ack id at a pointer and the ack
    /// request is sent per batch of ids after delivery; it excludes a
    /// lookup, a manifest and a keyset, and marks the profile as a queue.
    #[test]
    fn a_queue_unit_declares_its_ack_pointer_request_and_batch() {
        let yaml = "profile: pubsub\nbase_url: \"{{ vars.api_url }}\"\nauth: { accepts: [bearer] }\nendpoints:\n  - unit: pull\n    method: POST\n    path: \"/v1/projects/{{ vars.project_id }}/subscriptions/{{ vars.subscription_id }}:pull\"\n    body: { maxMessages: \"{{ vars.max_messages }}\" }\n    rows: { decoder: json_at, at: /receivedMessages, builder: pubsub_message }\n    max_pages: 1\n    construct:\n      queue:\n        ack_at: /ackId\n        ack_request:\n          path: \"/v1/projects/{{ vars.project_id }}/subscriptions/{{ vars.subscription_id }}:acknowledge\"\n          body: { ackIds: \"{{ ids }}\" }\n        ack_batch: 500\n";
        let profile = parse(yaml);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        assert!(profile.is_queue());
        let queue = profile.queue_of(&profile.endpoints[0]).expect("a queue");
        assert_eq!(queue.ack_at, "/ackId");
        assert_eq!(queue.ack_batch, 500);
        assert_eq!(queue.ack_request.method_or(Method::Post), Method::Post);
        assert_eq!(
            queue.ack_request.body.as_ref().unwrap()["ackIds"],
            "{{ ids }}"
        );

        let no_pointer = parse(&yaml.replace("ack_at: /ackId", "ack_at: ackId"));
        assert!(
            no_pointer
                .validate()
                .iter()
                .any(|i| i.field.ends_with("construct.queue.ack_at")),
            "{:?}",
            no_pointer.validate()
        );
        let zero = parse(&yaml.replace("ack_batch: 500", "ack_batch: 0"));
        assert!(
            zero.validate()
                .iter()
                .any(|i| i.field.ends_with("construct.queue.ack_batch")),
            "{:?}",
            zero.validate()
        );
        let with_keyset = parse(&yaml.replace(
            "    construct:\n      queue:",
            "    construct:\n      keyset: { from: \"{{ vars.subs }}\" }\n      queue:",
        ));
        assert!(
            with_keyset
                .validate()
                .iter()
                .any(|i| i.field.ends_with("construct.queue.ack_request")
                    && i.message.contains("subscription")),
            "{:?}",
            with_keyset.validate()
        );
        let two = parse(&yaml.replace(
            "    construct:\n      queue:",
            "    construct:\n      lookup: { request: { path: /x }, rows: { decoder: json_array } }\n      queue:",
        ));
        assert!(
            two.validate().iter().any(|i| i.message.contains("not two")),
            "{:?}",
            two.validate()
        );
    }

    /// An instance runs a profile endpoint once more under its own name:
    /// the unit is `<connection>.<name>` with its own vars and, when set,
    /// its own topic; the name must be new and the endpoint must exist.
    #[test]
    fn an_instance_unit_instantiates_a_profile_endpoint_under_its_own_name() {
        let profile = parse(GITHUB);
        let yaml = "profile: github\ntopic: t\nauth: { mode: bearer, token: x }\nunits:\n  acme_audit:\n    endpoint: audit_log\n    topic: acme\n    vars: { org: acme }\n";
        let instance: RestInstance = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(
            instance.validate(&profile).is_empty(),
            "{:?}",
            instance.validate(&profile)
        );
        let unit = &instance.units["acme_audit"];
        assert_eq!(unit.endpoint.as_deref(), Some("audit_log"));
        assert_eq!(unit.topic.as_deref(), Some("acme"));

        let unknown: RestInstance =
            serde_yaml_ng::from_str(&yaml.replace("endpoint: audit_log", "endpoint: nope"))
                .unwrap();
        assert!(
            unknown
                .validate(&profile)
                .iter()
                .any(|i| i.field == "units.acme_audit.endpoint"),
            "{:?}",
            unknown.validate(&profile)
        );
        let taken: RestInstance =
            serde_yaml_ng::from_str(&yaml.replace("  acme_audit:", "  audit_log:")).unwrap();
        assert!(
            taken
                .validate(&profile)
                .iter()
                .any(|i| i.field == "units.audit_log.endpoint" && i.message.contains("already")),
            "{:?}",
            taken.validate(&profile)
        );
        let blank: RestInstance =
            serde_yaml_ng::from_str(&yaml.replace("topic: acme", "topic: \"\"")).unwrap();
        assert!(
            blank
                .validate(&profile)
                .iter()
                .any(|i| i.field == "units.acme_audit.topic"),
            "{:?}",
            blank.validate(&profile)
        );
    }
}
