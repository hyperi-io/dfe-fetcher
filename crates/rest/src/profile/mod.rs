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

use std::borrow::Cow;
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
    /// A keyed digest over a canonical string of the built request, in the
    /// digest, the canonical shape and the placement the profile's
    /// [`SignatureSpec`] names. Duo's Admin API signing is the `duo_v5` and
    /// `duo_v2` presets of it; `duo_hmac` is the old spelling of the mode, and
    /// a profile on that spelling with no `signature` block signs the `duo_v2`
    /// the name used to mean.
    #[serde(alias = "duo_hmac")]
    Signature,
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
    /// More than one credential on the one request, each in the place the
    /// profile names: two headers of their own (Datadog's `DD-API-KEY` and
    /// `DD-APPLICATION-KEY`), or one header whose value is composed from
    /// several (Tenable's `accessKey=...;secretKey=...`).
    Credentials,
    /// A login call that mints a session token, which is what an appliance
    /// offers instead of an OAuth2 endpoint: the login posts the instance's
    /// user name and password, the token comes back in a field of the response
    /// or in a response header, and every later request carries it in the
    /// header the profile names.
    SessionLogin,
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
            AuthKind::Signature => "signature",
            AuthKind::JwtBearer => "jwt_bearer",
            AuthKind::GceMetadata => "gce_metadata",
            AuthKind::SigV4 => "sigv4",
            AuthKind::Credentials => "credentials",
            AuthKind::SessionLogin => "session_login",
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

/// One place a credential goes under the `credentials` mode, and which of the
/// instance's named credentials it carries.
///
/// `from` puts one credential where the API wants it, as it resolved. `value`
/// composes several into one value, for an API that wants both halves of a key
/// pair inside one header. A value is NOT a template: its only substitution is
/// `{{ credentials.<name> }}`, so nothing but a named credential can reach a
/// credential value and the value cannot vary by unit, request or var.
///
/// One credential is written as `from` with a `prefix` when the API wants text
/// in front of it; a `value` naming one credential says the same thing the long
/// way round.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CredentialPlacementSpec {
    /// Header name carrying the credential.
    pub header: Option<String>,
    /// Query parameter carrying it.
    pub query: Option<String>,
    /// Text put in front of it in a header, e.g. `Bearer `.
    pub prefix: String,
    /// The one named credential this placement carries, as it resolved.
    pub from: Option<String>,
    /// The value this placement carries, composed from named credentials.
    pub value: Option<String>,
}

impl CredentialPlacementSpec {
    /// The named credentials this placement reads, in the order it reads them.
    #[must_use]
    pub fn reads(&self) -> Vec<String> {
        match (&self.from, &self.value) {
            (Some(name), _) => vec![name.clone()],
            (None, Some(value)) => credential_value_parts(value)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|part| match part {
                    ValuePart::Credential(name) => Some(name),
                    ValuePart::Text(_) => None,
                })
                .collect(),
            (None, None) => Vec::new(),
        }
    }

    /// Where this placement puts its credential, as the grammar spells it.
    fn target(&self) -> Option<String> {
        match (&self.header, &self.query) {
            (Some(name), None) => Some(format!("header {}", name.to_ascii_lowercase())),
            (None, Some(name)) => Some(format!("query {name}")),
            _ => None,
        }
    }
}

/// One piece of a composed credential value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValuePart {
    /// Literal text the profile wrote.
    Text(String),
    /// The named credential whose resolved value is substituted here.
    Credential(String),
}

/// Split a composed credential value into its parts.
///
/// The only substitution is `{{ credentials.<name> }}`. Anything else between
/// the braces is refused rather than evaluated: a credential value is composed,
/// not computed, so no expression ever runs over a secret and no context --
/// `vars`, `unit`, `window` -- can reach the value. That is also why a composed
/// value needs no per-unit check the way a token endpoint does, because there is
/// nothing in it that a unit could change.
///
/// # Errors
///
/// Returns the reason as text, for the caller to name its field with.
pub fn credential_value_parts(value: &str) -> std::result::Result<Vec<ValuePart>, String> {
    const OPEN: &str = "{{";
    const CLOSE: &str = "}}";

    let mut parts = Vec::new();
    let mut named = 0usize;
    let mut rest = value;
    while let Some(at) = rest.find(OPEN) {
        if at > 0 {
            parts.push(literal_text(&rest[..at])?);
        }
        let after = &rest[at + OPEN.len()..];
        let Some(end) = after.find(CLOSE) else {
            return Err("has a `{{` that is never closed".to_owned());
        };
        parts.push(ValuePart::Credential(placeholder_name(
            after[..end].trim(),
        )?));
        named += 1;
        rest = &after[end + CLOSE.len()..];
    }
    if !rest.is_empty() {
        parts.push(literal_text(rest)?);
    }
    if named == 0 {
        return Err(
            "names no credential; a value substitutes `{{ credentials.<name> }}`".to_owned(),
        );
    }
    Ok(parts)
}

/// One run of literal text between placeholders, or why it is not one.
///
/// `{{` and `}}` are the whole of the value's syntax, so a brace outside a pair
/// is a miscount rather than text: `{{ credentials.k }}}` would otherwise compose
/// the credential with a `}` after it and go out looking authentic.
fn literal_text(run: &str) -> std::result::Result<ValuePart, String> {
    if run.contains(['{', '}']) {
        return Err("has a `{` or `}` outside a `{{ credentials.<name> }}` placeholder".to_owned());
    }
    Ok(ValuePart::Text(run.to_owned()))
}

/// The credential a placeholder names, or why it is not one.
fn placeholder_name(inside: &str) -> std::result::Result<String, String> {
    let Some(name) = inside.strip_prefix("credentials.") else {
        return Err(format!(
            "`{{{{ {inside} }}}}` is not a credential; a composed value substitutes \
             `{{{{ credentials.<name> }}}}` and nothing else, because it is not a template"
        ));
    };
    match credential_name_issue(name) {
        Some(reason) => Err(reason),
        None => Ok(name.to_owned()),
    }
}

/// Why `name` is not a credential name, or `None` when it is one.
///
/// One grammar for the three places a credential is named -- a placement's
/// `from`, a `{{ credentials.<name> }}` placeholder, and the instance's
/// `auth.credentials` key -- because they are matched against each other: a name
/// one of them accepts and another refuses is a credential that can be placed
/// and never supplied.
fn credential_name_issue(name: &str) -> Option<String> {
    let usable = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    (!usable).then(|| {
        format!("`{name}` is not a credential name; a name is letters, digits, `_` and `-`")
    })
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
    /// The signed assertion the exchange authenticates with in place of a
    /// client secret, when the API accepts one; unset means the client secret
    /// is the only way in.
    pub client_assertion: Option<ClientAssertionSpec>,
}

impl Default for OAuth2Spec {
    fn default() -> Self {
        Self {
            token_url: String::new(),
            scope: String::new(),
            expires_in_fallback_secs: 3600,
            early_refresh_secs: 60,
            expose: Vec::new(),
            client_assertion: None,
        }
    }
}

/// Authenticating the client-credentials exchange with a private-key JWT
/// (RFC 7523 s2.2), which Okta and Microsoft Entra both call
/// `private_key_jwt`.
///
/// The exchange posts `grant_type=client_credentials` with
/// `client_assertion_type` and a `client_assertion` the fetcher signs, instead
/// of a `client_secret`. The instance carries the key, so declaring this block
/// says the API accepts the path; an instance picks it by supplying a
/// `private_key` rather than a `client_secret`.
///
/// The assertion always carries `iss` and `sub` as the client id, `aud` as the
/// rendered token endpoint, `iat` and `exp` from `ttl_secs`, and a fresh `jti`:
/// Okta refuses a replayed assertion, so no two carry the same id. `claims`
/// adds to those or overrides the first three, each a template over the
/// instance's `vars`, `base_url` and the `auth.token_url` the exchange renders.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ClientAssertionSpec {
    /// Claim templates added to the assertion, or replacing `iss`, `sub` or
    /// `aud`.
    pub claims: BTreeMap<String, String>,
    /// Assertion lifetime, `exp - iat`. Okta caps it at an hour.
    pub ttl_secs: u64,
}

impl Default for ClientAssertionSpec {
    fn default() -> Self {
        Self {
            claims: BTreeMap::new(),
            ttl_secs: 300,
        }
    }
}

/// Where the login call carries the instance's user name and password.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionCredentials {
    /// As two members of the JSON body the login posts, under the names
    /// `username_field` and `password_field`.
    Body,
    /// As the `Authorization: Basic` header of the login request, which is
    /// what vCenter and Wazuh want.
    Basic,
}

/// A login call that mints a session token.
///
/// The appliances this exists for -- vCenter, F5 BIG-IP, Check Point, Wazuh,
/// Redfish -- answer a login with a vendor-named token rather than an OAuth2
/// token response, put it in a field or a header of their own choosing, and say
/// nothing about how long it lives. So the token is read from where the profile
/// points, its lifetime is what the profile declares, and it is carried in the
/// header the profile names rather than as a bearer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SessionLoginSpec {
    /// The login endpoint, a template (may use `base_url`).
    pub url: String,
    /// Where the credentials go on the login request.
    pub send: SessionCredentials,
    /// The body member carrying the user name under `send: body`.
    pub username_field: String,
    /// The body member carrying the password under `send: body`.
    pub password_field: String,
    /// Further members of the login body, each a template over the instance's
    /// context (F5's `loginProviderName`). A credential never travels here:
    /// these are the profile's own text and carry no secret.
    pub body: BTreeMap<String, String>,
    /// JSON pointer to the token in the login response; the empty pointer is
    /// the whole body, which is what vCenter answers with.
    pub token_at: Option<String>,
    /// The response header carrying the token, for an appliance that answers
    /// with one instead.
    pub token_header: Option<String>,
    /// How long the token is held. The appliance does not say, so this is the
    /// session lifetime it documents.
    pub ttl_secs: u64,
    /// How long before that lifetime is up the token is minted again.
    pub early_refresh_secs: u64,
    /// The request header every later request carries the token in.
    pub header: String,
    /// Text put in front of the token in that header.
    pub prefix: String,
}

impl Default for SessionLoginSpec {
    fn default() -> Self {
        Self {
            url: String::new(),
            send: SessionCredentials::Body,
            username_field: "username".to_owned(),
            password_field: "password".to_owned(),
            body: BTreeMap::new(),
            token_at: None,
            token_header: None,
            ttl_secs: 1800,
            early_refresh_secs: 60,
            header: String::new(),
            prefix: String::new(),
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

/// The digest a signature is made with.
///
/// The same digest hashes the body and the signed headers where the canonical
/// string carries those, because every scheme in the survey uses one hash
/// throughout. `sha1` is here for Duo's legacy v2 scheme alone and is never a
/// default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SignatureDigest {
    /// SHA-1; Duo v2 only.
    Sha1,
    /// SHA-256.
    Sha256,
    /// SHA-512.
    Sha512,
}

/// How the secret enters the digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SignatureKeying {
    /// The secret is the HMAC key over the canonical string (RFC 2104).
    Hmac,
    /// The secret is hashed in front of the canonical string with no HMAC
    /// construction, which is what Cortex XDR's advanced key specifies.
    Prefix,
}

/// How the digest is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SignatureEncoding {
    /// Lowercase hexadecimal.
    Hex,
    /// Standard base64 with padding.
    Base64,
}

/// What the header the digest goes in carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SignatureTarget {
    /// `Basic base64(<key_id>:<digest>)` -- HTTP basic with the key id as the
    /// user name, which is Duo's placement.
    Basic,
    /// The encoded digest on its own, after the spec's `prefix`.
    Digest,
}

/// A shipped signing scheme, named rather than spelled out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SignaturePreset {
    /// Duo's signature version 5: SHA-512 over seven lines, the last two the
    /// hash of the body and the hash of the canonicalised `x-duo-` headers.
    DuoV5,
    /// Duo's signature version 2: SHA-1 over the first five of those lines.
    /// Duo's legacy scheme, for a tenant whose endpoints still verify it.
    DuoV2,
}

/// The header a digest goes in unless the profile names another.
const DEFAULT_SIGNATURE_HEADER: &str = "authorization";

/// The name a signature template reads its request's facts under.
pub const SIGNATURE_FACTS: &str = "signature";

impl SignaturePreset {
    /// The spec this preset stands for.
    #[must_use]
    pub fn spec(self) -> SignatureSpec {
        let (digest, lines, signed_header_prefix) = match self {
            SignaturePreset::DuoV5 => (
                SignatureDigest::Sha512,
                &[
                    "{{ signature.date }}",
                    "{{ signature.method }}",
                    "{{ signature.host }}",
                    "{{ signature.path }}",
                    "{{ signature.query }}",
                    "{{ signature.body_hash }}",
                    "{{ signature.headers_hash }}",
                ][..],
                "x-duo-",
            ),
            SignaturePreset::DuoV2 => (
                SignatureDigest::Sha1,
                &[
                    "{{ signature.date }}",
                    "{{ signature.method }}",
                    "{{ signature.host }}",
                    "{{ signature.path }}",
                    "{{ signature.query }}",
                ][..],
                "",
            ),
        };
        SignatureSpec {
            preset: None,
            digest,
            keying: SignatureKeying::Hmac,
            canonical: lines.iter().map(|line| (*line).to_owned()).collect(),
            encoding: SignatureEncoding::Hex,
            // Duo verifies the date line against the `Date` header it arrived
            // with, so the signer writes the one it signed.
            headers: [("date".to_owned(), "{{ signature.date }}".to_owned())]
                .into_iter()
                .collect(),
            signed_header_prefix: signed_header_prefix.to_owned(),
            place: SignatureTarget::Basic,
            header: DEFAULT_SIGNATURE_HEADER.to_owned(),
            prefix: String::new(),
        }
    }
}

/// A keyed digest over a canonical string of the built request.
///
/// The canonical string is what makes this its own mode rather than a
/// placement: it is COMPUTED per request, out of the method, host, path,
/// query, date, nonce and body the driver just built, so it cannot be a value
/// resolved once per instance. Each `canonical` entry is one LINE, joined by
/// newlines, and each is a template over `signature.*` and nothing else --
/// `signature.date`, `.timestamp`, `.timestamp_ms`, `.nonce`, `.method`,
/// `.host`, `.path`, `.query`, `.body_hash`, `.headers_hash` and `.key_id`.
/// A template that reads `vars`, `unit`, `window` or any other context name is
/// refused at load, so nothing a unit supplies can reach a signature.
///
/// `headers` are set on the request before it is signed, each a template over
/// the same facts less `headers_hash`, which is computed after them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SignatureSpec {
    /// A shipped scheme by name, in place of the fields below.
    pub preset: Option<SignaturePreset>,
    /// The digest of the signature, the body hash and the header hash.
    pub digest: SignatureDigest,
    /// Where the secret goes.
    pub keying: SignatureKeying,
    /// The canonical string, one line per entry.
    pub canonical: Vec<String>,
    /// How the digest is written.
    pub encoding: SignatureEncoding,
    /// Headers set before the request is signed.
    pub headers: BTreeMap<String, String>,
    /// The lowercase name prefix of the headers `signature.headers_hash`
    /// covers; unset means it covers none, so the hash is of the empty string.
    pub signed_header_prefix: String,
    /// What the header carries.
    pub place: SignatureTarget,
    /// The header the digest goes in.
    pub header: String,
    /// Text in front of the digest, under `place: digest` only.
    pub prefix: String,
}

impl Default for SignatureSpec {
    fn default() -> Self {
        Self {
            preset: None,
            digest: SignatureDigest::Sha256,
            keying: SignatureKeying::Hmac,
            canonical: Vec::new(),
            encoding: SignatureEncoding::Hex,
            headers: BTreeMap::new(),
            signed_header_prefix: String::new(),
            place: SignatureTarget::Digest,
            header: DEFAULT_SIGNATURE_HEADER.to_owned(),
            prefix: String::new(),
        }
    }
}

impl SignatureSpec {
    /// Whether the block says nothing but its `preset`, so the preset is the
    /// whole scheme and not half of one.
    #[must_use]
    fn preset_alone(&self) -> bool {
        let bare = Self {
            preset: self.preset,
            ..Self::default()
        };
        *self == bare
    }

    /// Whether the block says nothing at all, so the profile names the mode
    /// and spells no scheme.
    #[must_use]
    fn unspelled(&self) -> bool {
        *self == Self::default()
    }

    /// The scheme in force: the preset's spec when one is named, this block
    /// when it spells a scheme out, and Duo's version 2 when it says nothing.
    ///
    /// The mode was spelled `duo_hmac` before the scheme became config, and
    /// what it signed was Duo's version 2. A profile written against that name
    /// has no block to read -- there was none to write -- so an unspelled
    /// scheme means what the name used to mean, and such a profile binds and
    /// signs as it did. Anything that names a preset or spells a scheme out,
    /// the shipped `duo` profile included, is unaffected.
    #[must_use]
    pub fn effective(&self) -> Cow<'_, Self> {
        match self.preset {
            Some(preset) => Cow::Owned(preset.spec()),
            None if self.unspelled() => Cow::Owned(SignaturePreset::DuoV2.spec()),
            None => Cow::Borrowed(self),
        }
    }

    /// Why this scheme is an exemption from the crypto standard, or `None`
    /// when it takes none.
    ///
    /// SHA-1 is not used for a security purpose in this house and signing a
    /// request is one. Duo's version 2 is the exemption, taken because an
    /// older tenant's endpoints verify nothing else, and every bind that takes
    /// it says so rather than passing in silence. Read the EFFECTIVE scheme,
    /// so the answer is the same whichever route selected it.
    #[must_use]
    pub fn crypto_exemption(&self) -> Option<&'static str> {
        (self.digest == SignatureDigest::Sha1).then_some(
            "signs requests with SHA-1, which is kept out of a security purpose everywhere \
             else; Duo's signature version 2 is the exemption, so move the tenant to version \
             5 wherever its endpoints verify it",
        )
    }
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
    /// Shape of the `signature` mode: the digest, the canonical string and
    /// where the digest goes.
    pub signature: SignatureSpec,
    /// Shape of the `credentials` mode: every place a credential goes.
    pub credentials: Vec<CredentialPlacementSpec>,
    /// Shape of the `session_login` mode: the login call and where its token
    /// is read and carried.
    pub session_login: SessionLoginSpec,
}

impl AuthSpec {
    /// The templates `mode` renders to decide what it mints and where: the
    /// token endpoint, and every claim of a JWT-bearer assertion. Named by
    /// their field, so a refusal points at the profile line.
    ///
    /// These are the only templates that can make a minted credential depend on
    /// a context, so these are the ones binding holds to the instance's.
    #[must_use]
    pub fn credential_templates(&self, mode: AuthKind) -> Vec<(String, &str)> {
        match mode {
            AuthKind::Oauth2ClientCredentials => {
                let mut templates = vec![(
                    "auth.oauth2_client_credentials.token_url".to_owned(),
                    self.oauth2_client_credentials.token_url.as_str(),
                )];
                if let Some(assertion) = &self.oauth2_client_credentials.client_assertion {
                    templates.extend(assertion.claims.iter().map(|(name, value)| {
                        (
                            format!(
                                "auth.oauth2_client_credentials.client_assertion.claims.{name}"
                            ),
                            value.as_str(),
                        )
                    }));
                }
                templates
            }
            AuthKind::SessionLogin => {
                let login = &self.session_login;
                let mut templates = vec![("auth.session_login.url".to_owned(), login.url.as_str())];
                templates.extend(login.body.iter().map(|(name, value)| {
                    (format!("auth.session_login.body.{name}"), value.as_str())
                }));
                templates
            }
            AuthKind::JwtBearer => {
                let mut templates = vec![(
                    "auth.jwt_bearer.token_url".to_owned(),
                    self.jwt_bearer.token_url.as_str(),
                )];
                templates.extend(self.jwt_bearer.claims.iter().map(|(name, value)| {
                    (format!("auth.jwt_bearer.claims.{name}"), value.as_str())
                }));
                templates
            }
            AuthKind::GceMetadata => {
                vec![(
                    "auth.gce_metadata.url".to_owned(),
                    self.gce_metadata.url.as_str(),
                )]
            }
            // `credentials` renders nothing against a context: a composed value
            // substitutes named credentials alone, so it cannot vary by unit.
            // `signature` renders per request against the request's own facts,
            // and the grammar refuses it any other name, so there is no context
            // for binding to hold it to.
            AuthKind::None
            | AuthKind::Bearer
            | AuthKind::ApiKey
            | AuthKind::Basic
            | AuthKind::Signature
            | AuthKind::SigV4
            | AuthKind::Credentials => Vec::new(),
        }
    }

    /// Whether `mode` authenticates its token exchange with a signed assertion
    /// rather than a client secret, which is a choice the INSTANCE makes by
    /// supplying a private key -- so this says only that the profile offers it.
    #[must_use]
    pub const fn offers_client_assertion(&self, mode: AuthKind) -> bool {
        matches!(mode, AuthKind::Oauth2ClientCredentials)
            && self.oauth2_client_credentials.client_assertion.is_some()
    }

    /// The credentials the `credentials` mode's placements read, in the order
    /// the profile writes them, each named once.
    #[must_use]
    pub fn credential_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for placement in &self.credentials {
            for name in placement.reads() {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        names
    }

    /// The `auth.*` names a credential template of `mode` may read: what the
    /// authenticator itself puts there, plus the token-response fields the
    /// profile allow-lists.
    #[must_use]
    pub fn exposed_names(&self, mode: AuthKind) -> Vec<&str> {
        let mut names = Vec::new();
        if mode == AuthKind::JwtBearer {
            names.extend(["client_email", "token_uri", "token_url"]);
        }
        // A client assertion is signed over the endpoint it is posted to, so
        // its claims read the rendered endpoint and the client id.
        if self.offers_client_assertion(mode) {
            names.extend(["token_url", "client_id"]);
        }
        let expose = match mode {
            AuthKind::JwtBearer => &self.jwt_bearer.expose,
            AuthKind::Oauth2ClientCredentials => &self.oauth2_client_credentials.expose,
            _ => return names,
        };
        names.extend(expose.iter().map(String::as_str));
        names
    }
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
            signature: SignatureSpec::default(),
            credentials: Vec::new(),
            session_login: SessionLoginSpec::default(),
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

/// What a provider's refusal looks like when it means "slow down" and the
/// status alone does not say so (AWS answers 400 with a `__type` body).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ThrottleSpec {
    /// The status such a refusal carries.
    pub status: u16,
    /// Text the body must contain for the refusal to be a throttle.
    pub body_contains: String,
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
    /// The refusal that means "slow down" on an API whose status does not
    /// say so; retried and counted a throttle whatever `retry_on` lists.
    pub throttle_when: Option<ThrottleSpec>,
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
            throttle_when: None,
        }
    }
}

impl RetrySpec {
    /// Whether a response status is retried.
    #[must_use]
    pub fn retries(&self, status: u16) -> bool {
        !self.never_retry.contains(&status) && self.retry_on.iter().any(|m| m.matches(status))
    }

    /// Whether this refusal is the declared throttle. A status in
    /// `never_retry` is not one, so a profile cannot turn a 403 into a
    /// retried throttle by naming text a refusal body happens to carry.
    #[must_use]
    pub fn throttled(&self, status: u16, text: &str) -> bool {
        self.throttle_when.as_ref().is_some_and(|spec| {
            spec.status == status
                && !self.never_retry.contains(&status)
                && text.contains(&spec.body_contains)
        })
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

/// The provider's documented rate limit for one unit's requests.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RateSpec {
    /// Requests a second this unit may send; a fraction expresses a limit
    /// slower than one a second.
    pub requests_per_sec: f64,
}

/// Compared by bits so a rate-bearing endpoint stays `Eq` like every other
/// spec of the grammar; a rate is written by hand, never computed.
impl PartialEq for RateSpec {
    fn eq(&self, other: &Self) -> bool {
        self.requests_per_sec.to_bits() == other.requests_per_sec.to_bits()
    }
}

impl Eq for RateSpec {}

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
    /// Rate limit of every endpoint that sets none.
    pub rate: Option<RateSpec>,
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
    /// The provider's rate limit for this unit's requests, held to across
    /// its pages, window steps and ticks; the defaults' when unset, and
    /// unpaced when neither sets one.
    pub rate: Option<RateSpec>,
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
            rate: None,
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
/// themselves, which a template would put in a URL or a log line. The same four
/// scalo drops from a token reading, so a profile is refused here rather than
/// passing validation and failing at render with no hint.
const NEVER_EXPOSED: &[&str] = &["access_token", "refresh_token", "id_token", "client_secret"];

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

/// Why a prefix cannot go with a query parameter, in either mode that places
/// one: nothing writes it, so a configured prefix would be dropped and the bare
/// credential sent in its place.
pub(crate) const QUERY_PREFIX_ISSUE: &str =
    "is set beside `query`; nothing writes a prefix into a query parameter";

/// Problems with the `credentials` mode's placements.
///
/// A malformed placement is a credential that goes nowhere, goes somewhere
/// twice, carries text no header value may carry, or is composed from a name no
/// instance can supply, so each is refused with the index the profile wrote it
/// at. This is the whole of what a placement list may be: binding re-checks it
/// through here, so a mode built without the profile's own validation is refused
/// the same way.
pub(crate) fn credential_placement_issues(placements: &[CredentialPlacementSpec]) -> Vec<Issue> {
    let mut issues = Vec::new();
    if placements.is_empty() {
        issues.push(Issue::new(
            "auth.credentials",
            "must declare at least one placement",
        ));
        return issues;
    }
    let mut targets: Vec<String> = Vec::new();
    for (i, placement) in placements.iter().enumerate() {
        let at = |f: &str| format!("auth.credentials[{i}].{f}");
        match (&placement.header, &placement.query) {
            (Some(name), None) => {
                if reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_err() {
                    issues.push(Issue::new(
                        at("header"),
                        format!("`{name}` is not a header name"),
                    ));
                }
            }
            (None, Some(name)) if name.trim().is_empty() => {
                issues.push(Issue::new(at("query"), "is empty"));
            }
            (None, Some(_)) => {
                if !placement.prefix.is_empty() {
                    issues.push(Issue::new(at("prefix"), QUERY_PREFIX_ISSUE));
                }
            }
            _ => issues.push(Issue::new(
                format!("auth.credentials[{i}]"),
                "needs exactly one of `header` or `query`",
            )),
        }
        if let Some(target) = placement.target() {
            if targets.contains(&target) {
                issues.push(Issue::new(
                    format!("auth.credentials[{i}]"),
                    format!("places a credential in {target}, which an earlier placement already carries"),
                ));
            }
            targets.push(target);
        }
        // The prefix and a value's literal text are static profile content, so
        // text no header value may carry is refused here with the field that
        // carries it, rather than as a signing failure every tick whose words
        // are about the credential.
        if let Some(reason) = header_text_issue(&placement.prefix) {
            issues.push(Issue::new(at("prefix"), reason));
        }
        match (&placement.from, &placement.value) {
            (Some(name), None) if name.trim().is_empty() => {
                issues.push(Issue::new(at("from"), "is empty"));
            }
            (Some(name), None) => {
                if let Some(reason) = credential_name_issue(name) {
                    issues.push(Issue::new(at("from"), reason));
                }
            }
            (None, Some(value)) => {
                // A single credential may still go in a query: what is refused
                // here is several of them in one value that a query would write
                // into every log along the path.
                if placement.query.is_some() {
                    issues.push(Issue::new(
                        at("value"),
                        "composes a value into a query parameter; a composed value carries more \
                         than one secret and a query is logged along the path, so place it in a \
                         header",
                    ));
                }
                if !placement.prefix.is_empty() && placement.header.is_some() {
                    issues.push(Issue::new(
                        at("prefix"),
                        "is set beside `value`; a composed value says the whole header value, so \
                         write the prefix into it",
                    ));
                }
                match credential_value_parts(value) {
                    Err(reason) => issues.push(Issue::new(at("value"), reason)),
                    Ok(parts) => {
                        let text: String = parts
                            .iter()
                            .filter_map(|part| match part {
                                ValuePart::Text(text) => Some(text.as_str()),
                                ValuePart::Credential(_) => None,
                            })
                            .collect();
                        if let Some(reason) = header_text_issue(&text) {
                            issues.push(Issue::new(at("value"), reason));
                        }
                    }
                }
            }
            _ => issues.push(Issue::new(
                format!("auth.credentials[{i}]"),
                "needs exactly one of `from` (one credential as it resolved) or `value` \
                 (a value composed of `{{ credentials.<name> }}` placeholders)",
            )),
        }
    }
    issues
}

/// Every top-level name a template of the grammar reads besides
/// [`SIGNATURE_FACTS`].
///
/// A signature template is held to the request's own facts, so this is the
/// list it is refused. Spelled out rather than derived: the names are set by
/// this crate, and one added without a thought for signing would otherwise
/// widen what can reach a signature.
///
/// A denylist is the weaker of the two shapes -- a twelfth name added here
/// would pass load validation until someone remembered this list -- and an
/// allowlist over [`SIGNATURE_FACTS`] alone would be safer. It is not one
/// because CEL reads a name the grammar never set as an error rather than a
/// blank, so the render is fail-closed and the twelfth name would refuse the
/// request instead of signing over it.
pub(crate) const CONTEXT_NAMES: [&str; 11] = [
    "vars", "base_url", "unit", "window", "page", "key", "item", "ids", "auth", "body", "headers",
];

/// The fact a `headers` template may not read: the hash covering the headers
/// the scheme sets, which is computed once they are all on the request.
const HEADERS_HASH_FACT: &str = "headers_hash";

/// Why `template` may not compute a signature, or `None` when it may.
///
/// The canonical string is computed per REQUEST, and the request is the only
/// thing a signature may be about: a canonical string that could read a unit's
/// `vars` would let one unit's narrowing decide what another unit's request
/// signs. The key and the placement are not templates at all, so what varies
/// per request is the request's own facts and nothing else.
pub(crate) fn signature_template_issue(template: &Template) -> Option<String> {
    CONTEXT_NAMES
        .into_iter()
        .find(|name| template.references(name))
        .map(|name| {
            format!(
                "reads `{name}`; a signature is computed per request, so its canonical string and \
                 its headers read `{SIGNATURE_FACTS}.*` and nothing else"
            )
        })
}

/// Why `source` may not stand as a signature `headers` template, or `None`
/// when it may.
///
/// `signature.headers_hash` covers the headers the scheme sets, so it is
/// computed once they are on the request -- after every one of these templates
/// has rendered. One that reads it is asking for the hash of itself, which the
/// facts do not carry, and CEL would refuse it on the first tick: under a hot
/// reload that takes a running source down rather than refusing the config.
/// The check is over the template TEXT because `references` answers for a
/// top-level name and this is a member of one.
pub(crate) fn signature_header_template_issue(source: &str) -> Option<String> {
    source.contains(HEADERS_HASH_FACT).then(|| {
        format!(
            "reads `{SIGNATURE_FACTS}.{HEADERS_HASH_FACT}`, which covers the headers this \
             template is one of and so is computed after it; a signed header carries a fact, \
             never the hash of itself"
        )
    })
}

/// Every problem with a signing scheme's shape.
///
/// A named `preset` is the whole scheme, so a block that names one and also
/// sets a field is refused rather than half-applied: which half won would
/// otherwise decide what goes on the wire.
pub(crate) fn signature_spec_issues(spec: &SignatureSpec) -> Vec<Issue> {
    let at = |field: &str| format!("auth.signature.{field}");
    let mut issues = Vec::new();
    if spec.preset.is_some() {
        if !spec.preset_alone() {
            issues.push(Issue::new(
                at("preset"),
                "names a shipped scheme, which is the whole of it; drop the other keys of the \
                 block, or drop the preset and spell the scheme out",
            ));
        }
        return issues;
    }
    // A block that says nothing is the scheme the mode's old name carried; see
    // `SignatureSpec::effective`.
    if spec.unspelled() {
        return issues;
    }
    if spec.canonical.is_empty() {
        issues.push(Issue::new(
            at("canonical"),
            "must carry at least one line, or the block must name a `preset`",
        ));
    }
    let check =
        |field: String, source: &str, issues: &mut Vec<Issue>| match Template::compile(source) {
            Err(e) => issues.push(Issue::new(field, e.to_string())),
            Ok(template) => {
                if let Some(reason) = signature_template_issue(&template) {
                    issues.push(Issue::new(field, reason));
                }
            }
        };
    for (i, line) in spec.canonical.iter().enumerate() {
        check(at(&format!("canonical[{i}]")), line, &mut issues);
    }
    for (name, value) in &spec.headers {
        let field = at(&format!("headers.{name}"));
        if reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_err() {
            issues.push(Issue::new(
                field.clone(),
                format!("`{name}` is not a header name"),
            ));
        }
        if let Some(reason) = signature_header_template_issue(value) {
            issues.push(Issue::new(field.clone(), reason));
        }
        check(field, value, &mut issues);
    }
    if reqwest::header::HeaderName::from_bytes(spec.header.as_bytes()).is_err() {
        issues.push(Issue::new(
            at("header"),
            format!("`{}` is not a header name", spec.header),
        ));
    }
    if !spec.prefix.is_empty() {
        if spec.place == SignatureTarget::Basic {
            issues.push(Issue::new(
                at("prefix"),
                "is set under `place: basic`, whose value is `Basic base64(<key_id>:<digest>)` and \
                 carries its own prefix",
            ));
        }
        if let Some(reason) = header_text_issue(&spec.prefix) {
            issues.push(Issue::new(at("prefix"), reason));
        }
    }
    if !spec.signed_header_prefix.is_empty()
        && spec.signed_header_prefix != spec.signed_header_prefix.to_ascii_lowercase()
    {
        issues.push(Issue::new(
            at("signed_header_prefix"),
            "is matched against lowercased header names, so write it in lowercase",
        ));
    }
    issues
}

/// Every problem with a session login's shape.
///
/// The login posts the instance's password and reads a token back, so the two
/// ends that decide where the password goes and where the token comes from are
/// refused here rather than on the first tick: a login that names neither place
/// to read the token from would authenticate and then send nothing.
pub(crate) fn session_login_issues(spec: &SessionLoginSpec) -> Vec<Issue> {
    let at = |field: &str| format!("auth.session_login.{field}");
    let mut issues = Vec::new();
    if spec.url.trim().is_empty() {
        issues.push(Issue::new(at("url"), "is required"));
    } else {
        issues.extend(template_issue(&at("url"), &spec.url));
    }
    match (&spec.token_at, &spec.token_header) {
        (Some(pointer), None) => {
            // The empty pointer is the whole body, which is the answer vCenter
            // gives: a JSON string that IS the token.
            if !pointer.is_empty() {
                issues.extend(pointer_issue(&at("token_at"), pointer));
            }
        }
        (None, Some(name)) => {
            if reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_err() {
                issues.push(Issue::new(
                    at("token_header"),
                    format!("`{name}` is not a header name"),
                ));
            }
        }
        _ => issues.push(Issue::new(
            at("token_at"),
            "needs exactly one of `token_at` or `token_header`; the token is read from the \
             response body or from a header of it",
        )),
    }
    if spec.header.trim().is_empty() {
        issues.push(Issue::new(
            at("header"),
            "is required: a session token is not a bearer, so the profile names the header it \
             goes in",
        ));
    } else if reqwest::header::HeaderName::from_bytes(spec.header.as_bytes()).is_err() {
        issues.push(Issue::new(
            at("header"),
            format!("`{}` is not a header name", spec.header),
        ));
    }
    if let Some(reason) = header_text_issue(&spec.prefix) {
        issues.push(Issue::new(at("prefix"), reason));
    }
    if spec.ttl_secs == 0 {
        issues.push(Issue::new(at("ttl_secs"), "must be at least 1"));
    }
    if spec.send == SessionCredentials::Body {
        for (field, name) in [
            ("username_field", &spec.username_field),
            ("password_field", &spec.password_field),
        ] {
            if name.trim().is_empty() {
                issues.push(Issue::new(at(field), "is required under `send: body`"));
            }
        }
        // The credentials are written last, so a body member of the same name
        // would be replaced by one of them rather than sent.
        for name in spec.body.keys() {
            if name == &spec.username_field || name == &spec.password_field {
                issues.push(Issue::new(
                    at(&format!("body.{name}")),
                    "is the member the login writes the credential into",
                ));
            }
        }
    }
    for (name, value) in &spec.body {
        issues.extend(template_issue(&at(&format!("body.{name}")), value));
    }
    issues
}

/// The query parameters a pager writes: the token's own when it goes into one,
/// and the page or offset parameter.
fn paginate_query_writers(at: &str, paginate: &PaginateSpec) -> Vec<(String, String)> {
    let into = paginate
        .into
        .as_deref()
        .and_then(|into| into.strip_prefix("query:"))
        .map(|name| (format!("{at}.into"), name.to_owned()));
    let param = paginate
        .param
        .as_deref()
        .map(|name| (format!("{at}.param"), name.to_owned()));
    into.into_iter().chain(param).collect()
}

/// Why `text` cannot stand in a header value, or `None` when it can.
///
/// The placement writes it into a header, which refuses a control character --
/// the newline that would otherwise read as the start of a header of its own.
fn header_text_issue(text: &str) -> Option<String> {
    reqwest::header::HeaderValue::from_str(text)
        .err()
        .map(|_| "carries text no header value may carry, such as a newline or a tab".to_owned())
}

impl RestProfile {
    /// The unit names the profile declares, in endpoint order -- what an
    /// instance's `units` keys and a typed block's `services[].name` may be.
    #[must_use]
    pub fn unit_names(&self) -> Vec<&str> {
        self.endpoints.iter().map(|e| e.unit.as_str()).collect()
    }

    /// Every query parameter the profile writes itself, each with the field that
    /// writes it.
    ///
    /// A credential placed in a query parameter is APPENDED to the built URL, so
    /// a name the profile also writes is sent twice rather than once, and the
    /// provider reads whichever it reads first.
    fn query_writers(&self) -> Vec<(String, String)> {
        let mut writers: Vec<(String, String)> = self
            .defaults
            .query
            .keys()
            .map(|name| (format!("defaults.query.{name}"), name.clone()))
            .collect();
        if let Some(paginate) = &self.defaults.paginate {
            writers.extend(paginate_query_writers("defaults.paginate", paginate));
        }
        for (i, endpoint) in self.endpoints.iter().enumerate() {
            writers.extend(
                endpoint
                    .query
                    .keys()
                    .map(|name| (format!("endpoints[{i}].query.{name}"), name.clone())),
            );
            if let Some(paginate) = &endpoint.paginate {
                writers.extend(paginate_query_writers(
                    &format!("endpoints[{i}].paginate"),
                    paginate,
                ));
            }
        }
        writers
    }

    /// Credential placements writing a query parameter the profile also writes.
    fn credential_query_collisions(&self) -> Vec<Issue> {
        let writers = self.query_writers();
        self.auth
            .credentials
            .iter()
            .enumerate()
            .filter_map(|(i, placement)| {
                let name = placement.query.as_deref()?;
                let (field, _) = writers.iter().find(|(_, written)| written == name)?;
                Some(Issue::new(
                    format!("auth.credentials[{i}].query"),
                    format!(
                        "`{name}` is written by {field} as well; a query pair is appended, so the \
                         credential would be sent twice"
                    ),
                ))
            })
            .collect()
    }

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

    /// The effective rate limit of an endpoint, when it or the defaults
    /// declare one.
    #[must_use]
    pub fn rate_of<'a>(&'a self, endpoint: &'a EndpointSpec) -> Option<&'a RateSpec> {
        endpoint.rate.as_ref().or(self.defaults.rate.as_ref())
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
        // A credential mode is built once per instance and per scope, so a
        // template of one that read a name only a unit or a request supplies
        // would be frozen to whichever unit asked first.
        for mode in &self.auth.accepts {
            for (field, source) in self.auth.credential_templates(*mode) {
                if let Ok(template) = Template::compile(source)
                    && let Some(name) = crate::auth::per_unit_name(&template)
                {
                    issues.push(Issue::new(
                        field,
                        format!(
                            "reads `{name}`, which only a unit supplies; a credential is minted \
                             once per instance and per scope, so its endpoint and claims cannot \
                             vary by unit"
                        ),
                    ));
                }
            }
        }
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
            if key.query.is_some() && key.header.is_none() && !key.prefix.is_empty() {
                issues.push(Issue::new("auth.api_key.prefix", QUERY_PREFIX_ISSUE));
            }
        }
        if self.auth.accepts.contains(&AuthKind::Credentials) {
            issues.extend(credential_placement_issues(&self.auth.credentials));
            issues.extend(self.credential_query_collisions());
        }
        if self.auth.accepts.contains(&AuthKind::Signature) {
            issues.extend(signature_spec_issues(&self.auth.signature));
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
            if let Some(assertion) = &oauth.client_assertion {
                let at = |field: &str| {
                    format!("auth.oauth2_client_credentials.client_assertion.{field}")
                };
                for (name, value) in &assertion.claims {
                    issues.extend(template_issue(&at(&format!("claims.{name}")), value));
                }
                for reserved in ["iat", "exp", "jti"] {
                    if assertion.claims.contains_key(reserved) {
                        issues.push(Issue::new(
                            at(&format!("claims.{reserved}")),
                            "is set on every assertion: `iat` and `exp` from `ttl_secs`, and \
                             `jti` fresh so no two assertions carry the same id",
                        ));
                    }
                }
                if assertion.ttl_secs == 0 {
                    issues.push(Issue::new(at("ttl_secs"), "must be at least 1"));
                }
            }
        }
        if self.auth.accepts.contains(&AuthKind::SessionLogin) {
            issues.extend(session_login_issues(&self.auth.session_login));
        }
        if self.retry.max_backoff_ms < self.retry.min_backoff_ms {
            issues.push(Issue::new(
                "retry.max_backoff_ms",
                "must be at least min_backoff_ms",
            ));
        }
        issues.extend(rate_issue("defaults.rate", self.defaults.rate.as_ref()));
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
            issues.extend(rate_issue(&at("rate"), endpoint.rate.as_ref()));
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

/// Problems with a `rate` at `field`: a pace is only expressible from a
/// finite, positive number of requests a second.
fn rate_issue(field: &str, rate: Option<&RateSpec>) -> Vec<Issue> {
    match rate {
        Some(rate) if !(rate.requests_per_sec.is_finite() && rate.requests_per_sec > 0.0) => {
            vec![Issue::new(
                format!("{field}.requests_per_sec"),
                "must be a positive number of requests a second",
            )]
        }
        _ => Vec::new(),
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
/// Secret fields are credential specs (`vault:<mount>/data/<path>:<key>` and
/// its `bao:` and `openbao:` spellings, `env:VAR`, `file:<path>`, or a
/// literal) resolved when first used.
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
    /// `signature`: the public half naming the key -- Duo's integration key
    /// (`ikey`), Cortex XDR's key id -- which a `basic` placement carries as
    /// the user name and a header template reads as `signature.key_id`.
    #[serde(alias = "integration_key")]
    pub key_id: Option<String>,
    /// `signature`: the secret the digest is keyed with (Duo's `skey`).
    pub secret_key: Option<SensitiveString>,
    /// `signature`: a shipped scheme in place of the profile's, for a tenant
    /// whose endpoints verify another version of it.
    pub signature_preset: Option<SignaturePreset>,
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
    /// `credentials`: the credential specs the profile's placements read, by
    /// the names they read them under. Each resolves once however many
    /// placements carry it.
    pub credentials: BTreeMap<String, SensitiveString>,
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

    /// Every credential-spec field that is set, as `(field, spec)`; a named
    /// credential of the `credentials` mode is `credentials.<name>`.
    ///
    /// Destructured exhaustively so a field added to the struct cannot
    /// quietly escape the load-time spec check: the plain identifiers are
    /// named and discarded, the specs are listed.
    #[must_use]
    pub fn credential_specs(&self) -> Vec<(Cow<'static, str>, &SensitiveString)> {
        let Self {
            mode: _,
            username: _,
            client_id: _,
            scope: _,
            key_id: _,
            signature_preset: _,
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
            credentials,
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
        .filter_map(|(field, spec)| spec.as_ref().map(|spec| (Cow::Borrowed(field), spec)))
        .chain(
            credentials
                .iter()
                .map(|(name, spec)| (Cow::Owned(format!("credentials.{name}")), spec)),
        )
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
            key_id: None,
            secret_key: None,
            signature_preset: None,
            service_account_key: None,
            service_account_key_file: None,
            private_key: None,
            access_key_id: None,
            secret_access_key: None,
            credentials_json: None,
            assume_role_arn: None,
            credentials: BTreeMap::new(),
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
    /// The signing scheme in force: the instance's own preset when it names
    /// one, else what the profile's block says.
    ///
    /// A tenant whose endpoints verify an older version of a scheme selects it
    /// here rather than by editing a shipped profile, which is shared by every
    /// instance of it.
    #[must_use]
    pub fn signature<'a>(&self, profile: &'a RestProfile) -> Cow<'a, SignatureSpec> {
        match self.auth.signature_preset {
            Some(preset) => Cow::Owned(preset.spec()),
            None => profile.auth.signature.effective(),
        }
    }

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
                // Which credential the instance carries picks the path: the
                // secret posts `client_secret`, the key signs a client
                // assertion. Both would leave which one authenticated the
                // exchange to the order the arms happen to be written in.
                match (&self.auth.client_secret, &self.auth.private_key) {
                    (Some(_), None) => {}
                    (None, Some(_)) if profile.auth.offers_client_assertion(self.auth.mode) => {}
                    (None, Some(_)) => issues.push(Issue::new(
                        "auth.private_key",
                        "signs a client assertion, and the profile declares no \
                         `auth.oauth2_client_credentials.client_assertion`",
                    )),
                    _ => issues.push(Issue::new(
                        "auth.client_secret",
                        "`oauth2_client_credentials` needs exactly one of `client_secret` or \
                         `private_key`",
                    )),
                }
            }
            AuthKind::SessionLogin => {
                needs("username", self.auth.username.is_some(), &mut issues);
                needs("password", self.auth.password.is_some(), &mut issues);
            }
            AuthKind::Signature => {
                needs("secret_key", self.auth.secret_key.is_some(), &mut issues);
                let scheme = self.signature(profile);
                // A basic placement carries the key id as the user name, so a
                // request without one authenticates as no integration at all.
                if scheme.place == SignatureTarget::Basic {
                    needs("key_id", self.auth.key_id.is_some(), &mut issues);
                }
                // Duo's version 2 puts a POST's BODY parameters on the line
                // this scheme builds from the query, and nothing in the grammar
                // says so. A POST would be signed over a query it does not
                // carry and Duo would refuse every request, so the pairing is
                // refused here instead. Version 5 hashes the body on a line of
                // its own and has no such trap.
                if *scheme == SignaturePreset::DuoV2.spec() {
                    for endpoint in &profile.endpoints {
                        if profile.method_of(endpoint) == Method::Post {
                            issues.push(Issue::new(
                                "auth.signature_preset",
                                format!(
                                    "`duo_v2` signs a POST's body parameters where this grammar \
                                     signs the query, so unit `{}` would be signed over the \
                                     wrong string; that unit needs `duo_v5`",
                                    endpoint.unit
                                ),
                            ));
                        }
                    }
                }
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
            AuthKind::Credentials => {
                // The instance is the only place a placed credential's spec can
                // come from, so a name one side has and the other has not is
                // refused here rather than as a 401 on the first tick.
                let placed = profile.auth.credential_names();
                for name in &placed {
                    if !self.auth.credentials.contains_key(name) {
                        issues.push(Issue::new(
                            format!("auth.credentials.{name}"),
                            "the profile places it and the instance supplies no spec for it",
                        ));
                    }
                }
                for name in self.auth.credentials.keys() {
                    // A key that is not a name matches no placement by
                    // construction, so the grammar is what it is told about.
                    if let Some(reason) = credential_name_issue(name) {
                        issues.push(Issue::new(format!("auth.credentials.{name}"), reason));
                        continue;
                    }
                    if !placed.contains(name) {
                        issues.push(Issue::new(
                            format!("auth.credentials.{name}"),
                            format!(
                                "no placement of the profile reads it; it reads {}",
                                placed.join(", ")
                            ),
                        ));
                    }
                }
                issues.extend(self.unit_query_collisions(profile));
            }
        }
        if self.auth.assume_role_arn.is_some() && self.auth.mode != AuthKind::SigV4 {
            issues.push(Issue::new(
                "auth.assume_role_arn",
                "only the `sigv4` mode assumes a role",
            ));
        }
        if self.auth.signature_preset.is_some() && self.auth.mode != AuthKind::Signature {
            issues.push(Issue::new(
                "auth.signature_preset",
                "only the `signature` mode signs, so nothing else reads a scheme",
            ));
        }
        // Named credentials are a shared field on the instance and only one mode
        // places them, so a map left behind under another mode would read as an
        // authenticated instance while every request went out without a
        // credential.
        if !self.auth.credentials.is_empty() && self.auth.mode != AuthKind::Credentials {
            issues.push(Issue::new(
                "auth.credentials",
                "only the `credentials` mode places named credentials",
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
                    format!(
                        "the profile has no such unit; it declares {}",
                        profile.unit_names().join(", ")
                    ),
                )),
                Some(_) if declared => issues.push(Issue::new(
                    format!("units.{name}.endpoint"),
                    "instantiates an endpoint under a name the profile already declares",
                )),
                Some(endpoint) if !profile.endpoints.iter().any(|e| e.unit == *endpoint) => {
                    issues.push(Issue::new(
                        format!("units.{name}.endpoint"),
                        format!(
                            "the profile has no endpoint `{endpoint}`; it declares {}",
                            profile.unit_names().join(", ")
                        ),
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

    /// Unit narrowing writing a query parameter a credential placement also
    /// writes, which would send the credential twice.
    fn unit_query_collisions(&self, profile: &RestProfile) -> Vec<Issue> {
        let placed: Vec<&str> = profile
            .auth
            .credentials
            .iter()
            .filter_map(|placement| placement.query.as_deref())
            .collect();
        let mut issues = Vec::new();
        for (unit, narrowing) in &self.units {
            for name in narrowing.query.keys() {
                if placed.contains(&name.as_str()) {
                    issues.push(Issue::new(
                        format!("units.{unit}.query.{name}"),
                        "is a query parameter a credential placement writes; a query pair is \
                         appended, so the credential would be sent twice",
                    ));
                }
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

    const DATADOG: &str = r#"
profile: datadog
base_url: "{{ vars.api_url }}"
shape: incremental
auth:
  accepts: [credentials]
  credentials:
    - { header: DD-API-KEY, from: api_key }
    - { header: DD-APPLICATION-KEY, from: application_key }
window: { format: rfc3339_millis, lookback: 1h }
retry: { never_retry: [401, 403] }
endpoints:
  - unit: audit_events
    path: /api/v2/audit/events
    rows: { decoder: json_at, at: "/data" }
    paginate: { strategy: none }
"#;

    const QUERY_KEY: &str = r#"
profile: query_key
base_url: "{{ vars.api_url }}"
shape: incremental
auth:
  accepts: [credentials]
  credentials:
    - { query: api_key, from: api_key }
window: { format: rfc3339_millis, lookback: 1h }
defaults: { query: { api_key: "{{ vars.api_key }}" } }
endpoints:
  - unit: things
    path: /things
    rows: { decoder: json_array }
    paginate: { strategy: none }
"#;

    const QUERY_PREFIX: &str = r#"
profile: query_prefix
base_url: "{{ vars.api_url }}"
shape: incremental
auth:
  accepts: [api_key]
  api_key: { query: token, prefix: "Token " }
window: { format: rfc3339_millis, lookback: 1h }
endpoints:
  - unit: things
    path: /things
    rows: { decoder: json_array }
    paginate: { strategy: none }
"#;

    /// An inline profile as it was written before the signing scheme became
    /// config: the mode under its old name and no block, because there was
    /// none to write.
    const LEGACY_DUO: &str = r#"
base_url: "{{ vars.base_url }}"
shape: incremental
auth: { accepts: [duo_hmac] }
window: { format: epoch_millis, lookback: 1h }
endpoints:
  - unit: authentication_logs
    path: /admin/v2/logs/authentication
    rows: { decoder: json_at, at: "/response/authlogs" }
    paginate: { strategy: none }
"#;

    fn parse(yaml: &str) -> RestProfile {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    /// The mode's old name still binds, and with no block to read it carries
    /// the scheme that name used to mean rather than being refused for a
    /// canonical string it was never able to write.
    #[test]
    fn the_old_mode_name_with_no_block_is_the_scheme_it_used_to_carry() {
        let profile = parse(LEGACY_DUO);
        assert_eq!(profile.auth.accepts, [AuthKind::Signature]);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());

        let instance: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: duo_hmac, integration_key: DI, secret_key: env:SKEY }\n",
        )
        .unwrap();
        assert_eq!(instance.auth.mode, AuthKind::Signature);
        assert_eq!(instance.auth.key_id.as_deref(), Some("DI"));
        let issues = instance.validate(&profile);
        assert!(issues.is_empty(), "{issues:?}");
        assert_eq!(*instance.signature(&profile), SignaturePreset::DuoV2.spec());
        assert!(instance.signature(&profile).crypto_exemption().is_some());
    }

    /// Duo's version 2 puts a POST's body parameters on the line this grammar
    /// builds from the query, so the pairing is refused rather than signing
    /// every POST over a string Duo will not recompute.
    #[test]
    fn duo_v2_is_refused_on_a_unit_that_posts() {
        let mut profile = parse(LEGACY_DUO);
        profile.endpoints[0].method = Some(Method::Post);
        let instance: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: signature, key_id: DI, secret_key: env:SKEY, signature_preset: duo_v2 }\n",
        )
        .unwrap();
        let issues = instance.validate(&profile);
        assert!(
            issues
                .iter()
                .any(|i| i.message.contains("duo_v2") && i.message.contains("authentication_logs")),
            "{issues:?}"
        );

        // Version 5 hashes the body on a line of its own, so the same unit is
        // fine under it.
        let v5: RestInstance = serde_yaml_ng::from_str(
            "profile: x\ntopic: t\nauth: { mode: signature, key_id: DI, secret_key: env:SKEY, signature_preset: duo_v5 }\n",
        )
        .unwrap();
        assert!(
            v5.validate(&profile).is_empty(),
            "{:?}",
            v5.validate(&profile)
        );
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

        for spec in [
            "env:RUNZERO_TOKEN",
            "vault:kv/data/team/x:token",
            "bao:kv/data/team/x:token",
            "openbao:kv/data/team/x:token",
            "file:/run/secrets/token",
        ] {
            assert!(bearer(spec).is_empty(), "{spec}: {:?}", bearer(spec));
        }

        let issues = bearer("aws:prod/runzero:token");
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.token");
        assert!(
            issues[0].message.contains("is not a credential spec")
                && issues[0].message.contains("`secrets-aws`"),
            "{:?}",
            issues[0]
        );
        assert!(
            !issues[0].message.contains("runzero"),
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
            "profile: x\ntopic: t\nauth: { mode: oauth2_client_credentials, client_id: id, client_secret: \"aws:prod/x:k\" }\n",
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
            credentials: BTreeMap::from([
                ("api_key".to_owned(), "env:A".into()),
                ("application_key".to_owned(), "env:A".into()),
            ]),
            ..InstanceAuth::default()
        };
        let fields: Vec<String> = auth
            .credential_specs()
            .into_iter()
            .map(|(field, _)| field.into_owned())
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
                "credentials_json",
                "credentials.api_key",
                "credentials.application_key"
            ]
        );
        auth.token = None;
        assert_eq!(
            auth.credential_specs().len(),
            12,
            "only the fields that are set"
        );
    }

    /// A profile placing more than one credential names each one, and the
    /// instance is the only place their specs come from, so a name one side has
    /// and the other has not is refused at load with its field.
    #[test]
    fn named_credentials_are_matched_between_the_profile_and_the_instance() {
        let profile = parse(DATADOG);
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        assert_eq!(
            profile.auth.credential_names(),
            ["api_key", "application_key"]
        );

        let instance = |auth: &str| {
            let instance: RestInstance = serde_yaml_ng::from_str(&format!(
                "profile: datadog\ntopic: t\nauth: {{ mode: credentials, credentials: {auth} }}\n"
            ))
            .unwrap();
            instance.validate(&profile)
        };

        assert!(
            instance("{ api_key: \"env:DD_API_KEY\", application_key: \"env:DD_APP_KEY\" }")
                .is_empty()
        );

        let issues = instance("{ api_key: \"env:DD_API_KEY\" }");
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.credentials.application_key");
        assert!(
            issues[0].message.contains("supplies no spec for it"),
            "{:?}",
            issues[0]
        );

        // A misspelt name is refused from both ends: the one the profile places
        // is missing and the one the instance wrote is read by nothing.
        let issues = instance("{ api_key: \"env:A\", aplication_key: \"env:B\" }");
        assert_eq!(issues.len(), 2, "{issues:?}");
        assert_eq!(issues[1].field, "auth.credentials.aplication_key");
        assert!(
            issues[1]
                .message
                .contains("no placement of the profile reads it")
                && issues[1].message.contains("application_key"),
            "{:?}",
            issues[1]
        );

        // A named credential's spec goes through the same check as every other.
        let issues = instance("{ api_key: \"aws:prod/dd:key\", application_key: \"env:B\" }");
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.credentials.api_key");
        assert!(
            issues[0].message.contains("is not a credential spec"),
            "{:?}",
            issues[0]
        );
        assert!(!issues[0].message.contains("prod"), "{:?}", issues[0]);
    }

    /// A composed value substitutes named credentials and nothing else: it is
    /// not a template, so no expression can be written over a secret and no
    /// context can reach the value.
    #[test]
    fn a_composed_credential_value_substitutes_named_credentials_alone() {
        assert_eq!(
            credential_value_parts(
                "accessKey={{ credentials.access_key }};secretKey={{ credentials.secret_key }}"
            )
            .unwrap(),
            [
                ValuePart::Text("accessKey=".into()),
                ValuePart::Credential("access_key".into()),
                ValuePart::Text(";secretKey=".into()),
                ValuePart::Credential("secret_key".into()),
            ]
        );

        for (value, wanted) in [
            ("{{ vars.access_key }}", "it is not a template"),
            ("{{ unit.name }}", "it is not a template"),
            (
                "{{ credentials.access_key[0:4] }}",
                "is not a credential name",
            ),
            ("{{ credentials. }}", "is not a credential name"),
            ("accessKey={{ credentials.access_key", "never closed"),
            ("accessKey=plain", "names no credential"),
            (
                "{{ credentials.access_key }}}",
                "outside a `{{ credentials.<name> }}` placeholder",
            ),
        ] {
            let reason = credential_value_parts(value).expect_err(value);
            assert!(reason.contains(wanted), "{value}: {reason}");
        }
    }

    /// Every way a placement can be malformed is refused at load with the index
    /// the profile wrote it at.
    #[test]
    fn a_malformed_credential_placement_is_refused_with_its_index() {
        let placed = |header: &str, from: &str| CredentialPlacementSpec {
            header: Some(header.to_owned()),
            from: Some(from.to_owned()),
            ..CredentialPlacementSpec::default()
        };
        let one = |placement| credential_placement_issues(&[placement]);
        let sole = |placement| {
            let issues = credential_placement_issues(&[placement]);
            assert_eq!(issues.len(), 1, "{issues:?}");
            issues.into_iter().next().expect("one issue")
        };

        assert!(one(placed("DD-API-KEY", "api_key")).is_empty());
        assert_eq!(
            credential_placement_issues(&[])[0].field,
            "auth.credentials",
            "a mode that places nothing authenticates nothing"
        );

        // Neither place, or both.
        for placement in [
            CredentialPlacementSpec {
                from: Some("api_key".into()),
                ..CredentialPlacementSpec::default()
            },
            CredentialPlacementSpec {
                query: Some("api_key".into()),
                ..placed("X-Api-Key", "api_key")
            },
        ] {
            let issue = sole(placement);
            assert_eq!(issue.field, "auth.credentials[0]");
            assert!(issue.message.contains("`header` or `query`"), "{issue:?}");
        }

        // Neither credential, or both.
        for placement in [
            CredentialPlacementSpec {
                header: Some("X-Api-Key".into()),
                ..CredentialPlacementSpec::default()
            },
            CredentialPlacementSpec {
                value: Some("{{ credentials.api_key }}".into()),
                ..placed("X-Api-Key", "api_key")
            },
        ] {
            let issue = sole(placement);
            assert_eq!(issue.field, "auth.credentials[0]");
            assert!(issue.message.contains("`from`"), "{issue:?}");
        }

        let issue = sole(placed("X Api Key", "api_key"));
        assert_eq!(issue.field, "auth.credentials[0].header");
        assert!(issue.message.contains("is not a header name"), "{issue:?}");

        // A composed value carries more than one secret, so it does not go in a
        // query, and it says the whole header value, so no prefix goes with it.
        let issue = sole(CredentialPlacementSpec {
            query: Some("auth".into()),
            value: Some("k={{ credentials.api_key }}".into()),
            ..CredentialPlacementSpec::default()
        });
        assert_eq!(issue.field, "auth.credentials[0].value");
        assert!(issue.message.contains("place it in a header"), "{issue:?}");

        let issue = sole(CredentialPlacementSpec {
            header: Some("Authorization".into()),
            prefix: "Bearer ".into(),
            value: Some("k={{ credentials.api_key }}".into()),
            ..CredentialPlacementSpec::default()
        });
        assert_eq!(issue.field, "auth.credentials[0].prefix");

        let issue = sole(CredentialPlacementSpec {
            header: Some("X-Api-Key".into()),
            value: Some("{{ vars.api_key }}".into()),
            ..CredentialPlacementSpec::default()
        });
        assert_eq!(issue.field, "auth.credentials[0].value");
        assert!(issue.message.contains("it is not a template"), "{issue:?}");

        // The second placement into one header would overwrite the first.
        let issues = credential_placement_issues(&[
            placed("DD-API-KEY", "api_key"),
            placed("dd-api-key", "application_key"),
        ]);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.credentials[1]");
        assert!(
            issues[0].message.contains("already carries"),
            "{:?}",
            issues[0]
        );
    }

    /// A query parameter carries the credential on its own -- nothing writes a
    /// prefix into one -- so a prefix beside a query is refused rather than
    /// dropped, in the placement list and in the `api_key` mode alike.
    #[test]
    fn a_prefix_beside_a_query_parameter_is_refused_in_either_mode() {
        let issues = credential_placement_issues(&[CredentialPlacementSpec {
            query: Some("token".into()),
            from: Some("api_key".into()),
            prefix: "Token ".into(),
            ..CredentialPlacementSpec::default()
        }]);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.credentials[0].prefix");
        assert!(
            issues[0]
                .message
                .contains("nothing writes a prefix into a query parameter"),
            "{:?}",
            issues[0]
        );

        let issues = parse(QUERY_PREFIX).validate();
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.api_key.prefix");
    }

    /// The prefix and a value's literal text are static profile content, so text
    /// no header value may carry is refused at load with the field that carries
    /// it, rather than on every tick in words about the credential.
    #[test]
    fn placement_text_no_header_value_may_carry_is_refused_at_load() {
        let issues = credential_placement_issues(&[CredentialPlacementSpec {
            header: Some("Authorization".into()),
            value: Some("a\r\nX-Injected: 1{{ credentials.api_key }}".into()),
            ..CredentialPlacementSpec::default()
        }]);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.credentials[0].value");
        assert!(
            issues[0].message.contains("no header value may carry"),
            "{:?}",
            issues[0]
        );

        let issues = credential_placement_issues(&[CredentialPlacementSpec {
            header: Some("X-Api-Key".into()),
            from: Some("api_key".into()),
            prefix: "Token\n".into(),
            ..CredentialPlacementSpec::default()
        }]);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.credentials[0].prefix");
    }

    /// The three places a credential is named are matched against each other, so
    /// one grammar serves all three: a name one of them took and another refused
    /// would be a credential that can be placed and never supplied.
    #[test]
    fn one_name_grammar_serves_a_placement_a_placeholder_and_an_instance_key() {
        for name in ["a.b", " api_key ", "api key", "key!"] {
            let issues = credential_placement_issues(&[CredentialPlacementSpec {
                header: Some("X-Api-Key".into()),
                from: Some(name.to_owned()),
                ..CredentialPlacementSpec::default()
            }]);
            assert_eq!(issues.len(), 1, "{name}: {issues:?}");
            assert_eq!(issues[0].field, "auth.credentials[0].from");
            assert!(
                issues[0].message.contains("is not a credential name"),
                "{name}: {:?}",
                issues[0]
            );

            let reason =
                credential_value_parts(&format!("{{{{ credentials.{name} }}}}")).expect_err(name);
            assert!(
                reason.contains("is not a credential name"),
                "{name}: {reason}"
            );
        }

        let instance: RestInstance = serde_yaml_ng::from_str(
            "profile: datadog\ntopic: t\nauth: { mode: credentials, credentials: { api_key: \"env:A\", \"application.key\": \"env:B\" } }\n",
        )
        .unwrap();
        let issues = instance.validate(&parse(DATADOG));
        assert!(
            issues.iter().any(|issue| {
                issue.field == "auth.credentials.application.key"
                    && issue.message.contains("is not a credential name")
            }),
            "{issues:?}"
        );
    }

    /// Named credentials are a shared instance field and one mode places them, so
    /// a map left behind under another mode is refused rather than reading as an
    /// authenticated instance whose requests all go out unauthenticated.
    #[test]
    fn named_credentials_under_another_mode_are_refused() {
        let instance: RestInstance = serde_yaml_ng::from_str(
            "profile: github\ntopic: t\nauth: { mode: bearer, token: \"env:T\", credentials: { api_key: \"env:A\" } }\n",
        )
        .unwrap();
        let issues = instance.validate(&parse(GITHUB));
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.credentials");
        assert!(
            issues[0].message.contains("only the `credentials` mode"),
            "{:?}",
            issues[0]
        );
    }

    /// A credential placed in a query parameter is APPENDED to the built URL, so
    /// a name the profile or a unit also writes sends the credential twice.
    #[test]
    fn a_query_placement_the_profile_or_a_unit_also_writes_is_refused() {
        let mut profile = parse(QUERY_KEY);
        let issues = profile.validate();
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "auth.credentials[0].query");
        assert!(
            issues[0].message.contains("defaults.query.api_key")
                && issues[0].message.contains("sent twice"),
            "{:?}",
            issues[0]
        );

        // The pager writes one of its own, which is no different.
        profile.defaults.query.clear();
        profile.endpoints[0].paginate = Some(PaginateSpec {
            into: Some("query:api_key".into()),
            ..PaginateSpec::default()
        });
        let issues = profile.credential_query_collisions();
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(
            issues[0].message.contains("endpoints[0].paginate.into"),
            "{:?}",
            issues[0]
        );

        let instance: RestInstance = serde_yaml_ng::from_str(
            "profile: query_key\ntopic: t\nauth: { mode: credentials, credentials: { api_key: \"env:A\" } }\nunits: { things: { query: { api_key: \"x\" } } }\n",
        )
        .unwrap();
        let issues = instance.validate(&profile);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "units.things.query.api_key");
        assert!(issues[0].message.contains("sent twice"), "{:?}", issues[0]);
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

    /// A rate that names no interval is a load error, not a unit that
    /// quietly sends as fast as it can.
    #[test]
    fn a_rate_that_is_not_a_positive_number_of_requests_is_refused() {
        for bad in ["0", "-2", ".nan"] {
            let yaml = GITHUB.replace(
                "max_pages: 50",
                &format!("max_pages: 50\n    rate: {{ requests_per_sec: {bad} }}"),
            );
            let issues = parse(&yaml).validate();
            assert!(
                issues
                    .iter()
                    .any(|i| i.field == "endpoints[0].rate.requests_per_sec"),
                "{bad}: {issues:?}"
            );
        }
        let yaml = GITHUB.replace(
            "max_pages: 50",
            "max_pages: 50\n    rate: { requests_per_sec: 0.2 }",
        );
        assert!(
            parse(&yaml).validate().is_empty(),
            "a fraction is how a limit slower than one a second is written"
        );
    }

    /// A unit's own rate wins over the defaults', and a profile that
    /// declares neither is unpaced.
    #[test]
    fn an_endpoint_rate_overrides_the_defaults_rate() {
        let yaml = format!(
            "{}defaults: {{ rate: {{ requests_per_sec: 10 }}, rows: {{ decoder: json_at, at: /items }} }}\nendpoints:\n  - {{ unit: a, path: /a }}\n  - {{ unit: b, path: /b, rate: {{ requests_per_sec: 2 }} }}\n",
            GITHUB.split("endpoints:").next().unwrap()
        );
        let profile = parse(&yaml);
        let [a, b] = profile.endpoints.as_slice() else {
            panic!("two units")
        };
        assert_eq!(
            profile.rate_of(a).map(|r| r.requests_per_sec),
            Some(10.0),
            "the defaults' rate reaches a unit that sets none"
        );
        assert_eq!(profile.rate_of(b).map(|r| r.requests_per_sec), Some(2.0));
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        assert!(
            parse(GITHUB).rate_of(&parse(GITHUB).endpoints[0]).is_none(),
            "no rate anywhere is no pacing"
        );
    }

    /// `retry.throttle_when` round-trips, and a status the profile never
    /// retries is not turned into a throttle by text a refusal carries.
    #[test]
    fn a_declared_throttle_is_recognised_but_never_over_never_retry() {
        let yaml = GITHUB.replace(
            "retry: { never_retry: [401, 403] }",
            "retry: { never_retry: [401, 403], throttle_when: { status: 400, body_contains: ThrottlingException } }",
        );
        let profile = parse(&yaml);
        assert_eq!(
            profile.retry.throttle_when,
            Some(ThrottleSpec {
                status: 400,
                body_contains: "ThrottlingException".into()
            })
        );
        assert!(profile.validate().is_empty(), "{:?}", profile.validate());
        let retry = &profile.retry;
        assert!(retry.throttled(400, r#"{"__type":"ThrottlingException"}"#));
        assert!(
            !retry.throttled(400, r#"{"__type":"ValidationException"}"#),
            "another 400 is the client's bug, not a throttle"
        );
        assert!(!retry.throttled(429, "ThrottlingException"), "wrong status");
        assert!(
            !RetrySpec::default().throttled(400, "ThrottlingException"),
            "a profile that declares none has no throttle to recognise"
        );

        let forbidden = parse(&yaml.replace("status: 400", "status: 403"));
        assert!(
            !forbidden
                .retry
                .throttled(403, "ThrottlingException, slow down"),
            "never_retry wins, so a refusal cannot be retried as a throttle"
        );
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
        // The same four scalo drops from a token reading, so none of them
        // passes validation and then fails at render with no hint.
        for credential in NEVER_EXPOSED {
            let leak =
                parse(&yaml.replace("expose: [instance_url]", &format!("expose: [{credential}]")));
            assert!(
                leak.validate()
                    .iter()
                    .any(|i| i.field == "auth.oauth2_client_credentials.expose"
                        && i.message.contains("credential")),
                "{credential}: {:?}",
                leak.validate()
            );
        }
        assert!(
            NEVER_EXPOSED.contains(&"client_secret"),
            "the field a token endpoint echoes back is a credential too"
        );
    }

    /// A credential is minted once per instance and per scope, so a template
    /// deciding what it mints may not read a name only a unit or a request
    /// supplies: a shipped profile is refused at validation rather than binding
    /// one unit's answer and presenting it as every other unit's credential.
    #[test]
    fn a_credential_template_may_not_read_what_only_a_unit_supplies() {
        let yaml = "profile: dwd\nbase_url: \"{{ vars.api_url }}\"\nauth:\n  accepts: [jwt_bearer]\n  jwt_bearer:\n    token_url: \"{{ auth.token_uri }}\"\n    claims: { iss: \"{{ auth.client_email }}\", sub: \"{{ vars.admin_email }}\" }\nvars: { api_url: \"https://api.example\", admin_email: \"\" }\nendpoints:\n  - { unit: users, path: /users, rows: { decoder: json_array } }\n";
        assert!(
            parse(yaml).validate().is_empty(),
            "{:?}",
            parse(yaml).validate()
        );

        let per_unit = parse(&yaml.replace(
            "sub: \"{{ vars.admin_email }}\"",
            "sub: \"{{ unit.name }}@example.com\"",
        ));
        assert!(
            per_unit
                .validate()
                .iter()
                .any(|i| i.field == "auth.jwt_bearer.claims.sub"
                    && i.message.contains("only a unit supplies")),
            "{:?}",
            per_unit.validate()
        );

        let windowed = parse(&yaml.replace(
            "token_url: \"{{ auth.token_uri }}\"",
            "token_url: \"https://idp.example/token?at={{ window.start }}\"",
        ));
        assert!(
            windowed
                .validate()
                .iter()
                .any(|i| i.field == "auth.jwt_bearer.token_url"),
            "{:?}",
            windowed.validate()
        );

        // The signing scope of `sigv4` is per request BY DESIGN, so it is not
        // held to the rule.
        let scoped = parse(
            "profile: aws\nbase_url: \"{{ vars.endpoint }}\"\nauth:\n  accepts: [sigv4]\n  sigv4: { service: \"{{ vars.service }}\", region: \"{{ vars.region }}\" }\nendpoints:\n  - { unit: trail, path: /, vars: { service: cloudtrail }, rows: { decoder: json_array } }\n",
        );
        assert!(scoped.validate().is_empty(), "{:?}", scoped.validate());
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

    /// A misspelt unit name is refused with the names the profile declares,
    /// so the operator need not open the profile file to find the right one.
    #[test]
    fn an_unknown_unit_names_the_units_the_profile_declares() {
        let profile = parse(GITHUB);
        let instance: RestInstance = serde_yaml_ng::from_str(
            "profile: github\ntopic: t\nauth: { mode: bearer, token: x }\nunits: { audit_logs: {} }\n",
        )
        .unwrap();

        let issues = instance.validate(&profile);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "units.audit_logs");
        assert!(
            issues[0].message.contains("audit_log"),
            "names the unit the profile declares: {}",
            issues[0].message
        );

        let misinstantiated: RestInstance = serde_yaml_ng::from_str(
            "profile: github\ntopic: t\nauth: { mode: bearer, token: x }\nunits:\n  acme:\n    endpoint: audit_logs\n",
        )
        .unwrap();

        let issues = misinstantiated.validate(&profile);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, "units.acme.endpoint");
        assert!(
            issues[0].message.contains("it declares audit_log"),
            "an instantiated endpoint is refused the same way: {}",
            issues[0].message
        );
    }
}
