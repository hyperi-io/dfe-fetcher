// Project:   dfe-fetcher
// File:      crates/rest/src/auth.rs
// Purpose:   The auth axis: credential modes applied to a built request
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The auth axis.
//!
//! An [`AuthMode`] is built once per INSTANCE from the profile's shape and the
//! instance's identity, so two instances of one profile never share a token
//! cache; a unit that needs a token for another scope gets its own mode over
//! the same identity, and units naming the same scope share one. It is applied
//! to the BUILT request, which is what lets a signing mode see the final
//! method, host, path and body.
//!
//! **A credential mode never sees a unit's context.** The token endpoint and
//! the JWT claims are rendered when the mode is built, against the instance's
//! own context, and held as owned values; the templates a profile writes there
//! may read `vars`, `base_url` and the authenticator's `auth.*`, and binding
//! refuses one that reads anything only a unit or a request supplies. A claim
//! set that cannot vary by unit cannot be minted for the wrong identity, which
//! matters because the claim naming a domain-wide-delegation subject decides
//! WHO the token acts as.
//!
//! The modes divide three ways, and each keeps only what scalo cannot own:
//!
//! - **A resolved credential in a place the profile names.** `bearer`,
//!   `api_key` and `basic` are one of scalo's placements over the resolved
//!   credential spec, so the spec resolves on first use and the writing of the
//!   header, the query pair or the basic credential is scalo's. `credentials` is
//!   a LIST of those placements, which scalo makes a signer in itself, for a
//!   provider that authenticates a request with more than one credential; a
//!   placement may compose several into one value.
//! - **A minted token.** `oauth2_client_credentials`, `jwt_bearer` and
//!   `gce_metadata` are scalo exchanges behind [`Cached`], which owns the
//!   caching, the renewal point and the single-flight gate: a cold mode hit by
//!   many units at once mints once. `jwt_bearer` keeps its own key reading and
//!   RS256 assertion and posts the assertion through scalo's [`TokenPost`].
//! - **A signing scheme.** `signature` and `sigv4` have their own crypto, so
//!   each is one more [`RequestSigner`] here rather than a signing dependency
//!   in scalo. `signature` is the generic one: the digest, the canonical string
//!   and where the digest goes all come from the profile, so a vendor's scheme
//!   is config rather than a module.
//!
//! [`AuthMode::signer`] is how the executor reaches a mode: a [`ModeSigner`]
//! is scalo's [`RequestSigner`] over one request's context, and its `sign` is
//! the dispatcher that picks the arm. The context is there for the signing
//! scope of `sigv4`, which is per unit by design.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use reqsign::aws::{
    AssumeRoleCredentialProvider, Credential as AwsCredential, RequestSigner as AwsRequestSigner,
    StaticCredentialProvider,
};
use reqwest::header::{HeaderName, HeaderValue};
use scalo::SensitiveString;
use scalo::auth::{
    AuthError, BasicPlacement, Cached, ClientCredentials, Credential, CredentialSource, Exchange,
    HeaderPlacement, MetadataServer, Placement, QueryPlacement, TokenPost, TokenReading,
};
use scalo::http_client::{RequestSigner, SignError};
use serde_json::Value;
use tokio::sync::OnceCell;

use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::secret::{ResolveSecret, Secret as SecretCell};

use crate::profile::template::{Template, TemplateCtx};
use crate::profile::{
    AuthKind, AuthSpec, InstanceAuth, SIGNATURE_FACTS, SignatureDigest, SignatureEncoding,
    SignatureKeying, SignatureSpec, SignatureTarget, ValuePart, credential_value_parts,
};
use crate::request::ExchangeClient;

/// The form field of the JWT-bearer grant (RFC 7523).
const JWT_BEARER_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

/// A credential acquisition's own result: scalo's error rather than the
/// framework's, so a refusal keeps its status all the way to the executor.
type Acquired<T> = std::result::Result<T, AuthError>;

/// scalo's secret resolver (`vault:...`, `env:VAR`, or a literal) as the
/// core cell's resolver.
#[derive(Debug)]
pub struct ScaloSecrets;

impl ResolveSecret for ScaloSecrets {
    async fn resolve(spec: &str) -> Result<SensitiveString> {
        scalo::secrets::resolve(spec)
            .await
            .map(SensitiveString::from)
            .map_err(|e| Error::Credential(e.to_string()))
    }
}

/// A credential spec resolved on first use.
pub type Secret = SecretCell<ScaloSecrets>;

/// A failure the fetcher raised on its way into a credential acquisition.
///
/// Nothing another attempt would change, which is what
/// [`AuthError::Unavailable`] says. The message is the fetcher's own rather
/// than its variant prefix stacked under scalo's.
fn unavailable(error: Error) -> AuthError {
    AuthError::Unavailable {
        reason: match error {
            Error::Config(reason) | Error::Credential(reason) => reason,
            other => other.to_string(),
        },
    }
}

/// An acquisition failure as the framework error the driver counts.
fn acquisition_error(error: AuthError) -> Error {
    credential_error(SignError::from(error))
}

/// A resolved credential spec as the source a placement reads per request.
///
/// [`Secret`] is already a resolve-once cell with its own single-flight, so
/// nothing is cached twice: this holds only the [`Credential`] that wraps the
/// resolved value, built the first time a placement asks for it. A hit is one
/// pointer clone. A spec that does not resolve is tried again by the next
/// request rather than answered from the last refusal, because the cell holds
/// values and not failures and the resolver is local.
///
/// The resolver stays core's type parameter, defaulted to this crate's, so a
/// test can count how often a spec is read from its store.
#[derive(Debug)]
struct Resolved<R = ScaloSecrets> {
    spec: SecretCell<R>,
    credential: OnceCell<Arc<Credential>>,
}

impl<R: ResolveSecret> Resolved<R> {
    fn new(spec: SecretCell<R>) -> Arc<Self> {
        Arc::new(Self {
            spec,
            credential: OnceCell::new(),
        })
    }
}

impl<R: ResolveSecret> CredentialSource for Resolved<R> {
    async fn credential(&self) -> Acquired<Arc<Credential>> {
        self.credential
            .get_or_try_init(|| async {
                let value = self.spec.value().await.map_err(unavailable)?;
                Ok(Arc::new(Credential::new(
                    SensitiveString::from(value),
                    never_renewed(),
                )))
            })
            .await
            .map(Arc::clone)
    }
}

/// A renewal point no request reaches: a resolved spec has nothing to renew
/// from, because the value lives in its [`Secret`] for the life of the process.
fn never_renewed() -> Instant {
    let now = Instant::now();
    now.checked_add(Duration::from_hours(24 * 365))
        .unwrap_or(now)
}

/// A resolved credential and where the profile puts it.
///
/// The source is held beside the placement -- the same `Arc` -- so a probe can
/// resolve the spec without sending a request.
#[derive(Debug)]
pub struct Placed {
    source: Arc<Resolved>,
    placement: Placement<Arc<Resolved>>,
}

impl Placed {
    /// `Authorization: Bearer <token>`.
    #[must_use]
    pub(crate) fn bearer(token: Secret) -> Self {
        let source = Resolved::new(token);
        Self {
            placement: Placement::Header(HeaderPlacement::bearer(Arc::clone(&source))),
            source,
        }
    }

    /// The key in `name`, after `prefix`.
    fn header(key: Secret, name: HeaderName, prefix: &str) -> Self {
        let source = Resolved::new(key);
        Self {
            placement: Placement::Header(HeaderPlacement::new(name, prefix, Arc::clone(&source))),
            source,
        }
    }

    /// The key as the query parameter `name`.
    fn query(key: Secret, name: &str) -> Self {
        let source = Resolved::new(key);
        Self {
            placement: Placement::Query(QueryPlacement::new(name, Arc::clone(&source))),
            source,
        }
    }

    /// The password of HTTP basic auth under `username`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the username carries a colon: the colon
    /// separates the two halves of a basic credential and RFC 7617 gives the
    /// username no way to escape one, so the request would authenticate as
    /// something other than what was configured. The username is as static as
    /// the `api_key` header name, so it is refused here rather than on the
    /// first request it would have gone out on.
    fn basic(password: Secret, username: &str) -> Result<Self> {
        if username.contains(':') {
            return Err(Error::Config(format!(
                "auth mode `basic` username `{username}` carries a colon, which separates the \
                 username from the password and cannot be escaped"
            )));
        }
        let source = Resolved::new(password);
        Ok(Self {
            placement: Placement::Basic(BasicPlacement::new(username, Arc::clone(&source))),
            source,
        })
    }

    /// Resolve the spec without sending a request.
    async fn resolve(&self) -> Result<()> {
        self.source
            .credential()
            .await
            .map(|_| ())
            .map_err(acquisition_error)
    }
}

/// One piece of a composed credential value: text the profile wrote, or a
/// resolved credential substituted for its name.
#[derive(Debug)]
enum ComposedPart<R = ScaloSecrets> {
    /// Literal text the profile wrote.
    Text(Box<str>),
    /// The credential substituted here.
    Credential(Arc<Resolved<R>>),
}

/// One piece of a composed value with its credential in hand, so the value can
/// be measured before a byte of it is written.
enum Piece<'a> {
    /// The profile's text.
    Text(&'a str),
    /// The resolved credential.
    Secret(Arc<Credential>),
}

impl Piece<'_> {
    /// The text this piece contributes to the value.
    fn text(&self) -> &str {
        match self {
            Piece::Text(text) => text,
            Piece::Secret(credential) => credential.secret.expose(),
        }
    }
}

/// Several resolved credentials as one value, for an API that wants both halves
/// of a key pair inside one header.
///
/// The parts are fixed when the instance binds and the value is composed once,
/// on first use, so the string holding two secrets is built in one place and
/// lives in one [`SensitiveString`]. Nothing is interpolated but a named
/// credential, so no context reaches the value and no expression runs over a
/// secret.
#[derive(Debug)]
struct Composed<R = ScaloSecrets> {
    parts: Vec<ComposedPart<R>>,
    value: OnceCell<Arc<Credential>>,
}

impl<R: ResolveSecret> CredentialSource for Composed<R> {
    async fn credential(&self) -> Acquired<Arc<Credential>> {
        self.value
            .get_or_try_init(|| async {
                // The pieces are acquired before the string is allocated so it is
                // allocated at its final length: a String that grew would leave
                // the secrets it held so far in freed heap.
                let mut pieces = Vec::with_capacity(self.parts.len());
                for part in &self.parts {
                    pieces.push(match part {
                        ComposedPart::Text(text) => Piece::Text(text),
                        ComposedPart::Credential(source) => {
                            Piece::Secret(source.credential().await?)
                        }
                    });
                }
                let mut value =
                    String::with_capacity(pieces.iter().map(|piece| piece.text().len()).sum());
                for piece in &pieces {
                    value.push_str(piece.text());
                }
                Ok(Arc::new(Credential::new(
                    SensitiveString::from(value),
                    never_renewed(),
                )))
            })
            .await
            .map(Arc::clone)
    }
}

/// What one placement of the `credentials` mode puts on the request: a resolved
/// spec, or several composed into one value.
#[derive(Debug)]
enum Sourced<R = ScaloSecrets> {
    /// One named credential, as it resolved.
    One(Arc<Resolved<R>>),
    /// Several, composed into one value by the profile's placement.
    Composed(Arc<Composed<R>>),
}

impl<R: ResolveSecret> CredentialSource for Sourced<R> {
    async fn credential(&self) -> Acquired<Arc<Credential>> {
        match self {
            Sourced::One(source) => source.credential().await,
            Sourced::Composed(source) => source.credential().await,
        }
    }
}

/// Every credential of the instance, each in the place the profile names.
///
/// A list of scalo's placements is itself a [`RequestSigner`], so two headers
/// arrive on one request from one pass and each is marked sensitive by the
/// placement that wrote it. One [`Resolved`] per NAME is shared by every
/// placement that reads it, so a credential carried in two places -- or composed
/// into a value and also placed on its own -- resolves once.
#[derive(Debug)]
pub struct Credentials<R = ScaloSecrets> {
    placements: Vec<Placement<Sourced<R>>>,
    named: Vec<Arc<Resolved<R>>>,
}

impl<R: ResolveSecret> Credentials<R> {
    /// Resolve every named spec without sending a request; a composed value is
    /// composed from these, so the composing itself cannot fail. What no probe
    /// reaches is scalo's own header-value check, which a resolved credential
    /// carrying a control character fails when the placement writes it.
    async fn resolve(&self) -> Result<()> {
        for source in &self.named {
            source
                .credential()
                .await
                .map(|_| ())
                .map_err(acquisition_error)?;
        }
        Ok(())
    }
}

/// How a token response is read: the profile's assumed lifetime, its refresh
/// margin, and the top-level fields it names as readable by templates.
fn token_reading(fallback_secs: u64, early_refresh_secs: u64, expose: &[String]) -> TokenReading {
    expose.iter().fold(
        TokenReading::default()
            .with_expires_in_fallback(Duration::from_secs(fallback_secs))
            .with_renew_margin(Duration::from_secs(early_refresh_secs)),
        |reading, name| reading.expose_field(name.as_str()),
    )
}

/// The bound on one acquisition: the exchange client's own per-request timeout
/// over every attempt it may make, plus the intervals between them. scalo's own
/// exchanges bound themselves the same way.
///
/// The bound covers everything the acquisition does, not the POST alone: a
/// JWT-bearer exchange reads its key, renders its claims and signs RS256 inside
/// this budget before the first byte goes out. One 30-second request's worth is
/// ample for a key read and an RS256 signature, so the mint is not given an
/// allowance of its own.
fn exchange_deadline(http: &ExchangeClient) -> Duration {
    let config = http.config();
    Duration::from_secs(config.timeout_secs)
        .saturating_mul(config.max_retries.saturating_add(1))
        .saturating_add(
            Duration::from_millis(config.max_retry_interval_ms).saturating_mul(config.max_retries),
        )
}

/// A minting mode's cache, holding no failure: a refused or unreachable
/// endpoint is posted to again by the next request, so the next tick reads the
/// endpoint's current answer rather than a held refusal. The callers that
/// waited on one in-flight exchange still share its failure.
fn cached<E: Exchange>(exchange: E) -> Cached<E> {
    Cached::new(exchange).with_failure_backoff(Duration::ZERO)
}

/// An OAuth2 client-credentials exchange behind scalo's cache.
///
/// The token endpoint is rendered when the mode is built and held as text, so
/// one mode has one endpoint however many units carry its token. The exchange
/// itself is built on first use, because the client secret resolves then.
pub struct OAuth2Client {
    token_url: String,
    client_id: String,
    client_secret: Secret,
    /// Unset rather than empty: `scope=` is a request for no scopes, which some
    /// providers refuse.
    scope: Option<String>,
    reading: TokenReading,
    exposes: bool,
    http: Arc<ExchangeClient>,
    source: OnceCell<Cached<ClientCredentials>>,
}

/// Hand-written because the exchange client has no `Debug`, and because the
/// rendered endpoint is named without its query.
impl fmt::Debug for OAuth2Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuth2Client")
            .field("token_url", &endpoint_name(&self.token_url))
            .field("client_id", &self.client_id)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

/// A token endpoint as a render may name it: scheme, host, port and path. The
/// query and any userinfo are dropped, because a provider that wants its
/// credential in the URL puts it in one of them.
fn endpoint_name(url: &str) -> String {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return "an unparseable endpoint".to_owned();
    };
    let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
    format!(
        "{}://{}{port}{}",
        parsed.scheme(),
        parsed.host_str().unwrap_or_default(),
        parsed.path()
    )
}

impl OAuth2Client {
    /// The cached credential, exchanged now when none is held or the held one
    /// has reached its renewal point.
    async fn credential(&self) -> Acquired<Arc<Credential>> {
        self.source
            .get_or_try_init(|| self.exchange())
            .await?
            .credential()
            .await
    }

    /// The exchange, once the client secret has resolved.
    async fn exchange(&self) -> Acquired<Cached<ClientCredentials>> {
        let secret = SensitiveString::from(self.client_secret.value().await.map_err(unavailable)?);
        let mut exchange =
            ClientCredentials::new(&self.http, self.token_url.as_str(), &self.client_id, secret)?
                .with_reading(self.reading.clone());
        if let Some(scope) = &self.scope {
            exchange = exchange.with_scope(scope.as_str());
        }
        Ok(cached(exchange))
    }
}

/// Where a JWT-bearer instance's signing key comes from.
#[derive(Debug)]
enum JwtKeySource {
    /// A Google-style service-account key JSON.
    ServiceAccountKey(Secret),
    /// A credential spec resolving to the path of that JSON.
    ServiceAccountKeyFile(Secret),
    /// A bare RSA private key PEM.
    PrivateKey(Secret),
}

/// The signing key once read, with what the key file says about itself.
struct JwtKey {
    encoding: jsonwebtoken::EncodingKey,
    client_email: String,
    token_uri: String,
}

/// Everything a JWT-bearer assertion is minted from, shared between the mode
/// and the exchange its cache holds.
///
/// `ctx` is the INSTANCE's context, taken when the mode was built, not a
/// unit's: the endpoint and the claims are the same for every unit that carries
/// this mode's token, and binding refuses a template that would make them
/// differ. It is kept rather than rendered outright because the claims also
/// read `auth.*` off the signing key, which is read on the first mint.
struct JwtAssertion {
    token_url: Template,
    claims: Vec<(String, Template)>,
    ttl: Duration,
    source: JwtKeySource,
    key: OnceCell<JwtKey>,
    http: Arc<ExchangeClient>,
    ctx: TemplateCtx,
}

/// Hand-written: the signing key has no `Debug`, neither has the exchange
/// client, and the context carries whatever the profile exposed to its
/// templates.
impl fmt::Debug for JwtAssertion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JwtAssertion")
            .field("token_url", &self.token_url)
            .field("claims", &self.claims)
            .field("ttl", &self.ttl)
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl JwtAssertion {
    /// The signing key, read on first use: a key JSON supplies the PEM, the
    /// client email and the token URI; a bare PEM supplies the PEM alone.
    async fn key(&self) -> Result<&JwtKey> {
        self.key
            .get_or_try_init(|| async {
                let (pem, client_email, token_uri) = match &self.source {
                    JwtKeySource::PrivateKey(secret) => (
                        secret.value().await?.to_owned(),
                        String::new(),
                        String::new(),
                    ),
                    JwtKeySource::ServiceAccountKey(secret) => {
                        parse_service_account_key(secret.value().await?)?
                    }
                    JwtKeySource::ServiceAccountKeyFile(secret) => {
                        let path = secret.value().await?;
                        let json = tokio::fs::read_to_string(path).await.map_err(|e| {
                            Error::Credential(format!("service account key file `{path}`: {e}"))
                        })?;
                        parse_service_account_key(&json)?
                    }
                };
                let encoding = jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes())
                    .map_err(|e| Error::Credential(format!("jwt_bearer private key: {e}")))?;
                Ok(JwtKey {
                    encoding,
                    client_email,
                    token_uri,
                })
            })
            .await
    }

    /// The signed assertion and the URL it is sent to; the templates see the
    /// key's `client_email` and `token_uri` as `auth.*`, and the rendered
    /// `token_url` as `auth.token_url`.
    async fn mint(&self) -> Result<(String, String)> {
        let key = self.key().await?;
        let mut ctx = self.ctx.clone();
        let mut auth = serde_json::Map::new();
        auth.insert(
            "client_email".into(),
            Value::String(key.client_email.clone()),
        );
        auth.insert("token_uri".into(), Value::String(key.token_uri.clone()));
        ctx.set("auth", Value::Object(auth.clone()));
        let url = self.token_url.render(&ctx)?;
        if url.trim().is_empty() {
            return Err(Error::Config(
                "jwt_bearer token_url rendered empty; set the var it reads or use a key with a token_uri".into(),
            ));
        }
        auth.insert("token_url".into(), Value::String(url.clone()));
        ctx.set("auth", Value::Object(auth));
        let now = chrono::Utc::now().timestamp();
        let mut claims = serde_json::Map::new();
        for (name, template) in &self.claims {
            let value = template.render_value(&ctx)?;
            if value.is_null() || value.as_str().is_some_and(str::is_empty) {
                continue;
            }
            claims.insert(name.clone(), value);
        }
        claims.insert("iat".into(), Value::from(now));
        claims.insert(
            "exp".into(),
            Value::from(now + i64::try_from(self.ttl.as_secs()).unwrap_or(i64::MAX)),
        );
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        let jwt = jsonwebtoken::encode(&header, &Value::Object(claims), &key.encoding)
            .map_err(|e| Error::Credential(format!("jwt_bearer signing: {e}")))?;
        Ok((url, jwt))
    }
}

/// One JWT-bearer exchange: a fresh assertion, posted as the grant's form.
///
/// The assertion is minted per acquisition rather than held, so a renewal signs
/// again instead of replaying an expired one.
#[derive(Debug)]
struct JwtExchange {
    assertion: Arc<JwtAssertion>,
    reading: TokenReading,
}

impl Exchange for JwtExchange {
    async fn acquire(&self) -> Acquired<Credential> {
        let (url, assertion) = self.assertion.mint().await.map_err(unavailable)?;
        TokenPost::new(&self.assertion.http, url)?
            .with_form_field("grant_type", JWT_BEARER_GRANT)
            .with_form_field("assertion", assertion)
            .with_reading(self.reading.clone())
            .acquire()
            .await
    }

    fn timeout(&self) -> Duration {
        exchange_deadline(&self.assertion.http)
    }
}

/// The OAuth2 JWT-bearer grant: an RS256 assertion over the profile's claim
/// templates, signed with the instance's key and exchanged for a cached access
/// token.
#[derive(Debug)]
pub struct JwtBearer {
    exposes: bool,
    source: Cached<JwtExchange>,
}

impl JwtBearer {
    /// The cached token, exchanged now when none is held or the held one has
    /// reached its renewal point.
    async fn credential(&self) -> Acquired<Arc<Credential>> {
        self.source.credential().await
    }
}

/// The PEM, client email and token URI of a Google-style service-account
/// key JSON.
fn parse_service_account_key(json: &str) -> Result<(String, String, String)> {
    let key: Value = serde_json::from_str(json)
        .map_err(|e| Error::Credential(format!("service account key is not JSON: {e}")))?;
    let text = |field: &str| {
        key.get(field)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_default()
    };
    let pem = text("private_key");
    if pem.is_empty() {
        return Err(Error::Credential(
            "service account key has no `private_key`".into(),
        ));
    }
    Ok((pem, text("client_email"), text("token_uri")))
}

/// The header a metadata server wants as proof the call was not made by a
/// browser or a confused proxy.
const METADATA_FLAVOR: HeaderName = HeaderName::from_static("metadata-flavor");

/// The GCE metadata server's token for the workload's service account.
///
/// The URL is rendered and checked when the mode is built, so a profile that
/// pointed the metadata read at an arbitrary host is refused at load rather
/// than on the first tick.
#[derive(Debug)]
pub struct GceMetadata {
    source: Cached<MetadataServer>,
}

impl GceMetadata {
    /// The cached token, fetched now when none is held or the held one has
    /// reached its renewal point.
    async fn credential(&self) -> Acquired<Arc<Credential>> {
        self.source.credential().await
    }
}

/// The host names a metadata read may go to besides a link-local or loopback
/// address: Google documents its metadata server by name, and its instances
/// resolve these to the link-local address.
const METADATA_HOSTS: [&str; 2] = ["metadata.google.internal", "metadata.goog"];

/// Why a rendered metadata URL is not usable, or `None` when it is.
///
/// A metadata read is exempt from the https rule the token exchanges are held
/// to -- every cloud serves its metadata over plaintext -- so the host is what
/// keeps the request on the instance. An inline profile could otherwise point
/// the read at any host over plaintext and be handed whatever it answers with.
fn metadata_url_issue(url: &str) -> Option<String> {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return Some("is not a URL".to_owned());
    };
    let Some(host) = parsed.host_str() else {
        return Some("names no host".to_owned());
    };
    if METADATA_HOSTS.contains(&host) {
        return None;
    }
    let address = host.trim_start_matches('[').trim_end_matches(']');
    let on_this_instance = match address.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(v4)) => v4.is_loopback() || v4.is_link_local(),
        // fe80::/10, spelled out because the standard predicate for it is
        // still unstable.
        Ok(std::net::IpAddr::V6(v6)) => v6.is_loopback() || (v6.segments()[0] & 0xffc0) == 0xfe80,
        Err(_) => host == "localhost",
    };
    (!on_this_instance).then(|| {
        format!(
            "host `{host}` is not this instance's metadata server; a metadata read goes to a \
             loopback or link-local address, or to {}",
            METADATA_HOSTS.join(" or ")
        )
    })
}

/// The characters a canonical query leaves bare: the RFC 3986 unreserved set,
/// which is the encoding RFC 5849 3.4.1.3.2 names and Duo's spec repeats.
const CANONICAL_ENCODE: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// The date format an RFC 2822 signature line and its header carry.
const RFC_2822_UTC: &str = "%a, %d %b %Y %H:%M:%S -0000";

/// What separates a header's name from its value, and one header from the next,
/// in the string a header hash covers.
const HEADER_HASH_SEPARATOR: &str = "\x00";

/// The digest of `message`.
fn digest_bytes(algorithm: SignatureDigest, message: &[u8]) -> Vec<u8> {
    use sha2::Digest as _;

    match algorithm {
        SignatureDigest::Sha1 => sha1::Sha1::digest(message).to_vec(),
        SignatureDigest::Sha256 => sha2::Sha256::digest(message).to_vec(),
        SignatureDigest::Sha512 => sha2::Sha512::digest(message).to_vec(),
    }
}

/// The digest of `message` as the lowercase hex a canonical line carries.
fn hex_digest(algorithm: SignatureDigest, message: &[u8]) -> String {
    hex::encode(digest_bytes(algorithm, message))
}

/// `message` keyed with `secret` under RFC 2104.
///
/// [`hmac::SimpleHmac`] takes any [`Digest`], so one body covers the three; it
/// produces the same HMAC as the block-level type, holding two digest states
/// rather than one.
///
/// [`Digest`]: hmac::digest::Digest
fn hmac_bytes(algorithm: SignatureDigest, secret: &[u8], message: &[u8]) -> Result<Vec<u8>> {
    use hmac::Mac as _;

    fn keyed<D>(secret: &[u8], message: &[u8]) -> Result<Vec<u8>>
    where
        D: hmac::digest::Digest + hmac::digest::crypto_common::BlockSizeUser,
    {
        let mut mac = hmac::SimpleHmac::<D>::new_from_slice(secret)
            .map_err(|e| Error::Credential(format!("signature secret key: {e}")))?;
        mac.update(message);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    match algorithm {
        SignatureDigest::Sha1 => keyed::<sha1::Sha1>(secret, message),
        SignatureDigest::Sha256 => keyed::<sha2::Sha256>(secret, message),
        SignatureDigest::Sha512 => keyed::<sha2::Sha512>(secret, message),
    }
}

/// The signed headers as the one string a header hash covers: each name
/// lowercase, then its value trimmed, in name order, everything joined by NUL.
///
/// An empty prefix covers no header, so the string is empty and its hash is the
/// hash of the empty string -- which is what a request carrying none signs.
fn canonical_headers(request: &reqwest::Request, prefix: &str) -> String {
    if prefix.is_empty() {
        return String::new();
    }
    // reqwest lowercases every header name it holds, so the prefix is matched
    // against the name as it will go out.
    let mut fields: Vec<(&str, String)> = request
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str(),
                value.to_str().unwrap_or_default().trim().to_owned(),
            )
        })
        .filter(|(name, _)| name.starts_with(prefix))
        .collect();
    fields.sort();
    fields
        .iter()
        .flat_map(|(name, value)| [*name, value.as_str()])
        .collect::<Vec<_>>()
        .join(HEADER_HASH_SEPARATOR)
}

/// A keyed digest over a canonical string of the built request.
///
/// The canonical string is COMPUTED per request -- the date, a nonce, the
/// method, the host, the path, the sorted query, the body's hash -- which is
/// why signing is its own mode rather than a placement over a resolved value.
/// Nothing is cached: the date is part of what is signed, so each request is
/// signed afresh.
///
/// The templates were compiled when the instance bound and read the request's
/// own facts alone, so what varies per request is the request. The digest, the
/// keying, the encoding, the placement, the key id and the secret are fixed
/// here for the life of the instance.
#[derive(Debug)]
pub struct Signature {
    digest: SignatureDigest,
    keying: SignatureKeying,
    encoding: SignatureEncoding,
    canonical: Vec<Template>,
    headers: Vec<(HeaderName, Template)>,
    signed_header_prefix: Box<str>,
    place: SignatureTarget,
    header: HeaderName,
    prefix: Box<str>,
    key_id: String,
    secret_key: Secret,
}

impl Signature {
    /// Build the mode from the scheme in force and the instance's identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the scheme's templates do not compile,
    /// one of them reads a name only a unit supplies, a header name is not one,
    /// or the identity carries no secret key.
    fn new(scheme: &SignatureSpec, identity: &InstanceAuth) -> Result<Self> {
        let compile = |field: String, source: &str| -> Result<Template> {
            let template =
                Template::compile(source).map_err(|e| Error::Config(format!("{field}: {e}")))?;
            match crate::profile::signature_template_issue(&template) {
                Some(reason) => Err(Error::Config(format!("{field}: {reason}"))),
                None => Ok(template),
            }
        };
        let mut canonical = Vec::with_capacity(scheme.canonical.len());
        for (i, line) in scheme.canonical.iter().enumerate() {
            canonical.push(compile(format!("auth.signature.canonical[{i}]"), line)?);
        }
        if canonical.is_empty() {
            return Err(Error::Config(
                "auth mode `signature` needs `auth.signature.canonical` or a `preset`".into(),
            ));
        }
        let mut headers = Vec::with_capacity(scheme.headers.len());
        for (name, value) in &scheme.headers {
            let field = format!("auth.signature.headers.{name}");
            headers.push((
                HeaderName::from_bytes(name.as_bytes())
                    .map_err(|e| Error::Config(format!("{field}: {e}")))?,
                compile(field, value)?,
            ));
        }
        // A basic placement carries the key id as the user name, so an instance
        // without one would authenticate as no integration at all.
        let key_id = match (scheme.place, &identity.key_id) {
            (SignatureTarget::Basic, None) => {
                return Err(Error::Config(
                    "auth mode `signature` needs `key_id` to place a basic credential".into(),
                ));
            }
            (_, key_id) => key_id.clone().unwrap_or_default(),
        };
        Ok(Self {
            digest: scheme.digest,
            keying: scheme.keying,
            encoding: scheme.encoding,
            canonical,
            headers,
            signed_header_prefix: scheme.signed_header_prefix.as_str().into(),
            place: scheme.place,
            header: HeaderName::from_bytes(scheme.header.as_bytes())
                .map_err(|e| Error::Config(format!("auth.signature.header: {e}")))?,
            prefix: scheme.prefix.as_str().into(),
            key_id,
            secret_key: identity
                .secret_key
                .clone()
                .map(Secret::new)
                .ok_or_else(|| Error::Config("auth mode `signature` needs `secret_key`".into()))?,
        })
    }

    /// The request's own facts as a signature template reads them, less
    /// `headers_hash`, which is computed once the mode's own headers are on.
    fn facts(
        &self,
        request: &reqwest::Request,
        at: chrono::DateTime<chrono::Utc>,
        nonce: &str,
    ) -> serde_json::Map<String, Value> {
        let url = request.url();
        // The port is part of the host when it is explicit, as the `Host`
        // header carries it.
        let mut host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        if let Some(port) = url.port() {
            host = format!("{host}:{port}");
        }
        // Encoded before sorting, because the order is the encoded pairs'.
        let encode =
            |text: &str| percent_encoding::utf8_percent_encode(text, CANONICAL_ENCODE).to_string();
        let mut pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(name, value)| (encode(&name), encode(&value)))
            .collect();
        pairs.sort();
        let query = pairs
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        let body = request
            .body()
            .and_then(reqwest::Body::as_bytes)
            .unwrap_or_default();
        [
            ("date", Value::String(at.format(RFC_2822_UTC).to_string())),
            ("timestamp", Value::from(at.timestamp())),
            ("timestamp_ms", Value::from(at.timestamp_millis())),
            ("nonce", Value::String(nonce.to_owned())),
            (
                "method",
                Value::String(request.method().as_str().to_ascii_uppercase()),
            ),
            ("host", Value::String(host)),
            ("path", Value::String(url.path().to_owned())),
            ("query", Value::String(query)),
            ("body_hash", Value::String(hex_digest(self.digest, body))),
            ("key_id", Value::String(self.key_id.clone())),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
    }

    /// The canonical string over `facts`: one line per template, joined by
    /// newlines.
    fn canonical(&self, facts: &serde_json::Map<String, Value>) -> Result<String> {
        let ctx = facts_ctx(facts);
        let mut lines = Vec::with_capacity(self.canonical.len());
        for line in &self.canonical {
            lines.push(line.render(&ctx)?);
        }
        Ok(lines.join("\n"))
    }

    /// Set the headers the scheme names, so the request carries what it signs.
    fn set_signed_headers(
        &self,
        request: &mut reqwest::Request,
        facts: &serde_json::Map<String, Value>,
    ) -> Result<()> {
        if self.headers.is_empty() {
            return Ok(());
        }
        let ctx = facts_ctx(facts);
        for (name, template) in &self.headers {
            let value = HeaderValue::from_str(&template.render(&ctx)?).map_err(|e| {
                Error::Credential(format!("signature header `{}`: {e}", name.as_str()))
            })?;
            request.headers_mut().insert(name.clone(), value);
        }
        Ok(())
    }

    /// Sign the request and put the digest where the scheme places it.
    async fn apply(&self, request: &mut reqwest::Request) -> Result<()> {
        let mut facts = self.facts(
            request,
            chrono::Utc::now(),
            &uuid::Uuid::new_v4().simple().to_string(),
        );
        // The headers go on before the hash that covers them, so a scheme can
        // both carry a value and sign it.
        self.set_signed_headers(request, &facts)?;
        facts.insert(
            "headers_hash".to_owned(),
            Value::String(hex_digest(
                self.digest,
                canonical_headers(request, &self.signed_header_prefix).as_bytes(),
            )),
        );
        let canonical = self.canonical(&facts)?;
        let secret = self.secret_key.value().await?;
        let raw = match self.keying {
            SignatureKeying::Hmac => {
                hmac_bytes(self.digest, secret.as_bytes(), canonical.as_bytes())?
            }
            // The secret is hashed in front of the canonical string, so the
            // string holding both is allocated at its final length: one that
            // grew would leave the secret in freed heap.
            SignatureKeying::Prefix => {
                let mut message = String::with_capacity(secret.len() + canonical.len());
                message.push_str(secret);
                message.push_str(&canonical);
                digest_bytes(self.digest, message.as_bytes())
            }
        };
        let digest = match self.encoding {
            SignatureEncoding::Hex => hex::encode(&raw),
            SignatureEncoding::Base64 => base64::engine::general_purpose::STANDARD.encode(&raw),
        };
        let value = match self.place {
            SignatureTarget::Basic => format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD
                    .encode(format!("{}:{digest}", self.key_id))
            ),
            SignatureTarget::Digest => format!("{}{digest}", self.prefix),
        };
        set_header(request, self.header.clone(), &value)
    }
}

/// One request's facts as the context its templates render against.
fn facts_ctx(facts: &serde_json::Map<String, Value>) -> TemplateCtx {
    let mut ctx = TemplateCtx::new();
    ctx.set(SIGNATURE_FACTS, Value::Object(facts.clone()));
    ctx
}

impl RequestSigner for Signature {
    async fn sign(&self, request: &mut reqwest::Request) -> std::result::Result<(), SignError> {
        self.apply(request).await.map_err(signing_failure)
    }
}

/// Where a SigV4 instance's keys come from.
#[derive(Debug)]
enum SigV4Keys {
    /// The key id and the secret, each a credential spec.
    Pair {
        access_key_id: Secret,
        secret_access_key: Secret,
    },
    /// One spec resolving to a JSON document carrying both.
    Json(Secret),
}

/// The role an instance's keys assume: one STS `AssumeRole` per instance on
/// the regional endpoint of the first request signed, the session
/// credentials cached by reqsign until shortly before their expiry and
/// minted again then, and every request signed with them.
#[derive(Debug)]
struct AssumeRole {
    role_arn: String,
    context: reqsign::Context,
    session: OnceCell<reqsign::Signer<AwsCredential>>,
}

impl AssumeRole {
    /// The signer over the session, built once from the static keys.
    async fn signer(
        &self,
        region: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> &reqsign::Signer<AwsCredential> {
        self.session
            .get_or_init(|| async {
                let sts = reqsign::Signer::new(
                    self.context.clone(),
                    StaticCredentialProvider::new(access_key_id, secret_access_key),
                    AwsRequestSigner::new("sts", region),
                );
                let provider = AssumeRoleCredentialProvider::new(self.role_arn.clone(), sts)
                    .with_role_session_name("dfe-fetcher".to_owned())
                    .with_region(region.to_owned())
                    .with_regional_sts_endpoint();
                reqsign::Signer::new(
                    self.context.clone(),
                    provider,
                    AwsRequestSigner::new("sts", region),
                )
            })
            .await
    }
}

/// Why `arn` cannot be assumed: reqsign refuses it as an IAM role ARN of
/// the `aws` partition. Another partition (GovCloud, China) is checked
/// against the instance's own region when the role is first assumed.
#[must_use]
pub fn role_arn_issue(arn: &str) -> Option<&'static str> {
    let refused = arn.starts_with("arn:aws:")
        && reqsign::aws::AssumeRoleGrant::new(arn, "dfe-fetcher")
            .validate_for_region("us-east-1")
            .is_err();
    refused.then_some("is not an IAM role ARN (arn:aws:iam::<account>:role/<name>)")
}

/// AWS Signature Version 4 over static keys, or over the session those keys
/// assume.
///
/// The signing scope (service and region) is rendered per request from the
/// unit's context, so the units of one instance sign for the services they
/// are and a region-locked unit for its region; the keys resolve once per
/// instance. The body's SHA-256 travels in `x-amz-content-sha256` because
/// every service but S3 refuses `UNSIGNED-PAYLOAD`. reqsign signs an
/// `http::request::Parts`, so the built request is projected into one and
/// the committed URI and headers copied back.
#[derive(Debug)]
pub struct SigV4 {
    service: Template,
    region: Template,
    keys: SigV4Keys,
    resolved: OnceCell<(String, SensitiveString)>,
    assume_role: Option<AssumeRole>,
}

impl SigV4 {
    /// The key pair, resolved on first use.
    async fn keys(&self) -> Result<&(String, SensitiveString)> {
        self.resolved
            .get_or_try_init(|| async {
                match &self.keys {
                    SigV4Keys::Pair {
                        access_key_id,
                        secret_access_key,
                    } => Ok((
                        access_key_id.value().await?.to_owned(),
                        SensitiveString::from(secret_access_key.value().await?),
                    )),
                    SigV4Keys::Json(secret) => parse_aws_credentials(secret.value().await?),
                }
            })
            .await
    }

    /// This instance's keys signing for the scope `ctx` renders.
    fn scoped<'a>(&'a self, ctx: &'a TemplateCtx) -> ScopedSigV4<'a> {
        ScopedSigV4 { keys: self, ctx }
    }

    /// Sign the request for the scope `ctx` renders.
    async fn apply(&self, request: &mut reqwest::Request, ctx: &TemplateCtx) -> Result<()> {
        let service = self.service.render(ctx)?;
        let region = self.region.render(ctx)?;
        let (access_key_id, secret_access_key) = self.keys().await?;
        let request_signer = AwsRequestSigner::new(&service, &region);
        let signer = match &self.assume_role {
            Some(role) => role
                .signer(&region, access_key_id, secret_access_key.expose())
                .await
                .clone()
                .with_request_signer(request_signer),
            None => reqsign::Signer::new(
                reqsign::default_context(),
                StaticCredentialProvider::new(access_key_id, secret_access_key.expose()),
                request_signer,
            ),
        };
        Self::sign_request(request, &signer).await
    }

    /// Sign a built request in place with `signer`, whose credential and
    /// scope are already chosen.
    async fn sign_request(
        request: &mut reqwest::Request,
        signer: &reqsign::Signer<AwsCredential>,
    ) -> Result<()> {
        let body = request
            .body()
            .and_then(reqwest::Body::as_bytes)
            .unwrap_or_default();
        let hash = reqsign::hash::hex_sha256(body);
        request.headers_mut().insert(
            "x-amz-content-sha256",
            HeaderValue::from_str(&hash)
                .map_err(|e| Error::Source(format!("sigv4 payload hash header: {e}")))?,
        );
        let mut builder = http::Request::builder()
            .method(request.method().clone())
            .uri(request.url().as_str());
        if let Some(headers) = builder.headers_mut() {
            *headers = request.headers().clone();
        }
        let (mut parts, ()) = builder
            .body(())
            .map_err(|e| Error::Source(format!("sigv4 request projection failed: {e}")))?
            .into_parts();
        signer
            .sign(&mut parts, None)
            .await
            .map_err(|e| Error::Source(format!("sigv4 signing failed: {e}")))?;
        *request.headers_mut() = parts.headers;
        *request.url_mut() = url::Url::parse(&parts.uri.to_string())
            .map_err(|e| Error::Source(format!("sigv4 signed URI is not a URL: {e}")))?;
        Ok(())
    }
}

/// One instance's SigV4 keys over one request's context.
///
/// The signing scope is a template, so it renders per request; the context is
/// per request and the keys are per instance, which is why the pair is made
/// here rather than held on the mode.
#[derive(Clone, Copy)]
struct ScopedSigV4<'a> {
    keys: &'a SigV4,
    ctx: &'a TemplateCtx,
}

/// Hand-written: the context carries whatever the profile exposed to its
/// templates, so the mode's kind is all a render says.
impl fmt::Debug for ScopedSigV4<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedSigV4")
            .field("mode", &AuthKind::SigV4.as_str())
            .finish_non_exhaustive()
    }
}

impl RequestSigner for ScopedSigV4<'_> {
    async fn sign(&self, request: &mut reqwest::Request) -> std::result::Result<(), SignError> {
        self.keys
            .apply(request, self.ctx)
            .await
            .map_err(signing_failure)
    }
}

/// The key id and secret of an AWS credentials JSON document, in either
/// the snake-case or the AWS spelling.
fn parse_aws_credentials(json: &str) -> Result<(String, SensitiveString)> {
    let document: Value = serde_json::from_str(json)
        .map_err(|e| Error::Credential(format!("sigv4 credentials_json is not JSON: {e}")))?;
    let field = |snake: &str, aws: &str| {
        document
            .get(snake)
            .or_else(|| document.get(aws))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| Error::Credential(format!("sigv4 credentials_json has no `{snake}`")))
    };
    Ok((
        field("access_key_id", "AccessKeyId")?,
        SensitiveString::from(field("secret_access_key", "SecretAccessKey")?),
    ))
}

/// The top-level template names only a unit or a request supplies, and so the
/// names a credential template may not read.
///
/// A credential mode is built once per instance and per scope; a template of
/// one that read any of these would render against whichever unit's request
/// reached the mode first and hold that answer for every other unit. Where the
/// value decides the identity the token acts as -- a domain-wide-delegation
/// `sub` -- that is one unit's data fetched as another unit's principal, so the
/// template is refused rather than documented.
const PER_UNIT_NAMES: [&str; 5] = ["unit", "window", "page", "key", "item"];

/// Render a credential template against the instance's context, at bind.
///
/// # Errors
///
/// Returns [`Error::Config`] naming the field when the template does not
/// compile, reads a name only a unit supplies, or does not render.
fn render_at_bind(field: &str, template: &str, ctx: &TemplateCtx) -> Result<String> {
    let compiled =
        Template::compile(template).map_err(|e| Error::Config(format!("{field}: {e}")))?;
    if let Some(name) = per_unit_name(&compiled) {
        return Err(Error::Config(format!(
            "{field}: reads `{name}`, which only a unit supplies; a credential is minted once per \
             instance and per scope, so its endpoint and claims cannot vary by unit"
        )));
    }
    compiled
        .render(ctx)
        .map_err(|e| Error::Config(format!("{field}: {e}")))
}

/// The first per-unit name `template` reads, when it reads one.
#[must_use]
pub(crate) fn per_unit_name(template: &Template) -> Option<&'static str> {
    PER_UNIT_NAMES
        .into_iter()
        .find(|name| template.references(name))
}

/// Compile a credential template that is rendered per mint rather than at bind
/// -- a JWT claim, whose `auth.*` comes off the signing key -- and refuse one
/// that reads a name only a unit supplies.
///
/// # Errors
///
/// Returns [`Error::Config`] naming the field.
fn compile_at_bind(field: &str, template: &str) -> Result<Template> {
    let compiled =
        Template::compile(template).map_err(|e| Error::Config(format!("{field}: {e}")))?;
    match per_unit_name(&compiled) {
        Some(name) => Err(Error::Config(format!(
            "{field}: reads `{name}`, which only a unit supplies; a credential is minted once per \
             instance and per scope, so its endpoint and claims cannot vary by unit"
        ))),
        None => Ok(compiled),
    }
}

/// The resolved spec of a named credential, shared with every other placement
/// that reads the same name.
///
/// # Errors
///
/// Returns [`Error::Config`] naming the field when the instance supplies no
/// credential of that name.
fn named_credential<'a, R: ResolveSecret>(
    named: &mut std::collections::BTreeMap<&'a str, Arc<Resolved<R>>>,
    identity: &'a InstanceAuth,
    name: &str,
    field: &str,
) -> Result<Arc<Resolved<R>>> {
    let (key, spec) = identity.credentials.get_key_value(name).ok_or_else(|| {
        Error::Config(format!(
            "{field}: the instance supplies no credential `{name}`"
        ))
    })?;
    Ok(Arc::clone(named.entry(key.as_str()).or_insert_with(|| {
        Resolved::new(SecretCell::new(spec.clone()))
    })))
}

/// Build the `credentials` mode: one resolved spec per named credential, and one
/// of scalo's placements per place the profile names.
///
/// # Errors
///
/// Returns [`Error::Config`] naming the field for every way a placement list can
/// be malformed, through the profile's own validation, and for a placement
/// reading a credential the instance does not supply. A typed block binds a
/// shipped profile without validating it, so this is where that profile's
/// placements are refused.
fn build_credentials<R: ResolveSecret>(
    spec: &AuthSpec,
    identity: &InstanceAuth,
) -> Result<Credentials<R>> {
    if let Some(issue) = crate::profile::credential_placement_issues(&spec.credentials)
        .into_iter()
        .next()
    {
        return Err(Error::Config(issue.to_string()));
    }
    let mut named = std::collections::BTreeMap::new();
    let mut placements = Vec::with_capacity(spec.credentials.len());
    for (i, placement) in spec.credentials.iter().enumerate() {
        let at = |f: &str| format!("auth.credentials[{i}].{f}");
        let source =
            match (&placement.from, &placement.value) {
                (Some(name), None) => {
                    Sourced::One(named_credential(&mut named, identity, name, &at("from"))?)
                }
                (None, Some(value)) => {
                    let parts = credential_value_parts(value)
                        .map_err(|reason| Error::Config(format!("{}: {reason}", at("value"))))?;
                    let mut composed = Vec::with_capacity(parts.len());
                    for part in parts {
                        composed.push(match part {
                            ValuePart::Text(text) => ComposedPart::Text(text.into()),
                            ValuePart::Credential(name) => ComposedPart::Credential(
                                named_credential(&mut named, identity, &name, &at("value"))?,
                            ),
                        });
                    }
                    Sourced::Composed(Arc::new(Composed {
                        parts: composed,
                        value: OnceCell::new(),
                    }))
                }
                _ => {
                    return Err(Error::Config(format!(
                        "auth.credentials[{i}] needs exactly one of `from` or `value`"
                    )));
                }
            };
        placements.push(match (&placement.header, &placement.query) {
            (Some(name), None) => Placement::Header(HeaderPlacement::new(
                HeaderName::from_bytes(name.as_bytes())
                    .map_err(|e| Error::Config(format!("{}: {e}", at("header"))))?,
                placement.prefix.as_str(),
                source,
            )),
            (None, Some(name)) => Placement::Query(QueryPlacement::new(name.as_str(), source)),
            _ => {
                return Err(Error::Config(format!(
                    "auth.credentials[{i}] needs exactly one of `header` or `query`"
                )));
            }
        });
    }
    Ok(Credentials {
        named: named.into_values().collect(),
        placements,
    })
}

/// The credential mode of one instance.
#[derive(Debug)]
pub enum AuthMode {
    /// No credential.
    None,
    /// `Authorization: Bearer <token>`.
    Bearer(Placed),
    /// A key in a header or query parameter.
    ApiKey(Placed),
    /// HTTP Basic.
    Basic(Placed),
    /// OAuth2 client credentials.
    OAuth2ClientCredentials(OAuth2Client),
    /// A keyed digest over a canonical string of the request.
    Signature(Signature),
    /// OAuth2 JWT-bearer grant.
    JwtBearer(JwtBearer),
    /// The GCE metadata server's token.
    GceMetadata(GceMetadata),
    /// AWS SigV4 signing.
    SigV4(SigV4),
    /// More than one credential, each where the profile places it.
    Credentials(Credentials),
}

impl AuthMode {
    /// Build the instance's mode from the profile's shape and its identity.
    /// `http` is the client a credential exchange posts through, shared by
    /// every instance. `ctx` is the INSTANCE's template context, which the
    /// token endpoint and the JWT claims render against, once, here.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the instance's mode is not accepted by the
    /// profile, an identity field for it is missing, or a template of the mode
    /// does not render against the instance's context.
    pub fn build(
        spec: &AuthSpec,
        identity: &InstanceAuth,
        http: Arc<ExchangeClient>,
        ctx: &TemplateCtx,
    ) -> Result<Self> {
        Self::build_scoped(spec, identity, http, ctx, None)
    }

    /// The same identity minting its token for `scope` instead of the
    /// instance's or the profile's; a unit whose API audience differs from
    /// the profile's gets one of these, and units sharing a scope share it.
    /// A mode that mints no scoped token ignores the scope.
    ///
    /// # Errors
    ///
    /// As [`AuthMode::build`].
    pub fn build_scoped(
        spec: &AuthSpec,
        identity: &InstanceAuth,
        http: Arc<ExchangeClient>,
        ctx: &TemplateCtx,
        scope: Option<&str>,
    ) -> Result<Self> {
        if !spec.accepts.contains(&identity.mode) {
            return Err(Error::Config(format!(
                "auth mode `{}` is not accepted by the profile",
                identity.mode.as_str()
            )));
        }
        let need = |field: &str, value: Option<&SensitiveString>| {
            value.cloned().map(Secret::new).ok_or_else(|| {
                Error::Config(format!(
                    "auth mode `{}` needs `{field}`",
                    identity.mode.as_str()
                ))
            })
        };
        Ok(match identity.mode {
            AuthKind::None => AuthMode::None,
            AuthKind::Bearer => {
                AuthMode::Bearer(Placed::bearer(need("token", identity.token.as_ref())?))
            }
            AuthKind::ApiKey => {
                let key = need("key", identity.key.as_ref())?;
                AuthMode::ApiKey(match (&spec.api_key.header, &spec.api_key.query) {
                    (Some(name), _) => {
                        let header = HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                            Error::Config(format!("auth.api_key header `{name}`: {e}"))
                        })?;
                        Placed::header(key, header, &spec.api_key.prefix)
                    }
                    (None, Some(name)) => {
                        if !spec.api_key.prefix.is_empty() {
                            return Err(Error::Config(format!(
                                "auth.api_key.prefix: {}",
                                crate::profile::QUERY_PREFIX_ISSUE
                            )));
                        }
                        Placed::query(key, name)
                    }
                    (None, None) => {
                        return Err(Error::Config(
                            "auth.api_key needs `header` or `query`".into(),
                        ));
                    }
                })
            }
            AuthKind::Basic => AuthMode::Basic(Placed::basic(
                need("password", identity.password.as_ref())?,
                identity
                    .username
                    .as_deref()
                    .ok_or_else(|| Error::Config("auth mode `basic` needs `username`".into()))?,
            )?),
            AuthKind::Oauth2ClientCredentials => {
                let oauth = &spec.oauth2_client_credentials;
                let scope = scope
                    .map(str::to_owned)
                    .or_else(|| identity.scope.clone())
                    .unwrap_or_else(|| oauth.scope.clone());
                AuthMode::OAuth2ClientCredentials(OAuth2Client {
                    token_url: render_at_bind(
                        "auth.oauth2_client_credentials.token_url",
                        &oauth.token_url,
                        ctx,
                    )?,
                    client_id: identity.client_id.clone().ok_or_else(|| {
                        Error::Config(
                            "auth mode `oauth2_client_credentials` needs `client_id`".into(),
                        )
                    })?,
                    client_secret: need("client_secret", identity.client_secret.as_ref())?,
                    scope: (!scope.is_empty()).then_some(scope),
                    reading: token_reading(
                        oauth.expires_in_fallback_secs,
                        oauth.early_refresh_secs,
                        &oauth.expose,
                    ),
                    exposes: !oauth.expose.is_empty(),
                    http,
                    source: OnceCell::new(),
                })
            }
            AuthKind::Signature => {
                // The instance's own preset wins, so a tenant verifying an
                // older version of a scheme picks it without editing a profile
                // every other instance of it also binds.
                let scheme = match identity.signature_preset {
                    Some(preset) => std::borrow::Cow::Owned(preset.spec()),
                    None => spec.signature.effective(),
                };
                AuthMode::Signature(Signature::new(&scheme, identity)?)
            }
            AuthKind::JwtBearer => {
                let jwt = &spec.jwt_bearer;
                let source = match (
                    &identity.service_account_key,
                    &identity.service_account_key_file,
                    &identity.private_key,
                ) {
                    (Some(json), None, None) => {
                        JwtKeySource::ServiceAccountKey(Secret::new(json.clone()))
                    }
                    (None, Some(path), None) => {
                        JwtKeySource::ServiceAccountKeyFile(Secret::new(path.clone()))
                    }
                    (None, None, Some(pem)) => JwtKeySource::PrivateKey(Secret::new(pem.clone())),
                    _ => {
                        return Err(Error::Config(
                            "auth mode `jwt_bearer` needs exactly one of `service_account_key`, \
                             `service_account_key_file` or `private_key`"
                                .into(),
                        ));
                    }
                };
                let mut claims = Vec::with_capacity(jwt.claims.len());
                for (name, value) in &jwt.claims {
                    let field = format!("auth.jwt_bearer.claims.{name}");
                    let template = match (name.as_str(), scope) {
                        ("scope", Some(scope)) => compile_at_bind(&field, scope)?,
                        _ => compile_at_bind(&field, value)?,
                    };
                    claims.push((name.clone(), template));
                }
                if let Some(scope) = scope
                    && !jwt.claims.contains_key("scope")
                {
                    claims.push((
                        "scope".to_owned(),
                        compile_at_bind("auth.jwt_bearer.claims.scope", scope)?,
                    ));
                }
                AuthMode::JwtBearer(JwtBearer {
                    exposes: !jwt.expose.is_empty(),
                    source: cached(JwtExchange {
                        assertion: Arc::new(JwtAssertion {
                            token_url: compile_at_bind(
                                "auth.jwt_bearer.token_url",
                                &jwt.token_url,
                            )?,
                            claims,
                            ttl: Duration::from_secs(jwt.ttl_secs),
                            source,
                            key: OnceCell::new(),
                            http,
                            ctx: ctx.clone(),
                        }),
                        reading: token_reading(
                            jwt.expires_in_fallback_secs,
                            jwt.early_refresh_secs,
                            &jwt.expose,
                        ),
                    }),
                })
            }
            AuthKind::GceMetadata => {
                let gce = &spec.gce_metadata;
                let url = render_at_bind("auth.gce_metadata.url", &gce.url, ctx)?;
                if let Some(issue) = metadata_url_issue(&url) {
                    return Err(Error::Config(format!("auth.gce_metadata.url: {issue}")));
                }
                AuthMode::GceMetadata(GceMetadata {
                    source: cached(
                        MetadataServer::new(&http, url)
                            .map_err(acquisition_error)?
                            .with_header(METADATA_FLAVOR, "Google")
                            .with_reading(token_reading(
                                gce.expires_in_fallback_secs,
                                gce.early_refresh_secs,
                                &[],
                            )),
                    ),
                })
            }
            AuthKind::SigV4 => {
                let keys = match (
                    &identity.credentials_json,
                    &identity.access_key_id,
                    &identity.secret_access_key,
                ) {
                    (Some(json), None, None) => SigV4Keys::Json(Secret::new(json.clone())),
                    (None, Some(id), Some(secret)) => SigV4Keys::Pair {
                        access_key_id: Secret::new(id.clone()),
                        secret_access_key: Secret::new(secret.clone()),
                    },
                    _ => {
                        return Err(Error::Config(
                            "auth mode `sigv4` needs `access_key_id` and `secret_access_key`, \
                             or `credentials_json` alone"
                                .into(),
                        ));
                    }
                };
                AuthMode::SigV4(SigV4 {
                    service: Template::compile(&spec.sigv4.service)?,
                    region: Template::compile(&spec.sigv4.region)?,
                    keys,
                    resolved: OnceCell::new(),
                    assume_role: identity.assume_role_arn.clone().map(|role_arn| AssumeRole {
                        role_arn,
                        context: reqsign::default_context(),
                        session: OnceCell::new(),
                    }),
                })
            }
            AuthKind::Credentials => AuthMode::Credentials(build_credentials(spec, identity)?),
        })
    }

    /// The mode's name for logs.
    #[must_use]
    pub fn kind(&self) -> AuthKind {
        match self {
            AuthMode::None => AuthKind::None,
            AuthMode::Bearer(_) => AuthKind::Bearer,
            AuthMode::ApiKey(_) => AuthKind::ApiKey,
            AuthMode::Basic(_) => AuthKind::Basic,
            AuthMode::OAuth2ClientCredentials(_) => AuthKind::Oauth2ClientCredentials,
            AuthMode::Signature(_) => AuthKind::Signature,
            AuthMode::JwtBearer(_) => AuthKind::JwtBearer,
            AuthMode::GceMetadata(_) => AuthKind::GceMetadata,
            AuthMode::SigV4(_) => AuthKind::SigV4,
            AuthMode::Credentials(_) => AuthKind::Credentials,
        }
    }

    /// The mode as a signer over one request's context.
    ///
    /// The context is the SIGNING scope's, which `sigv4` renders per request;
    /// the credential modes render nothing here, because their templates were
    /// rendered when the mode was built.
    #[must_use]
    pub fn signer<'a>(&'a self, ctx: &'a TemplateCtx) -> ModeSigner<'a> {
        ModeSigner { mode: self, ctx }
    }

    /// The cached-or-fresh credential of a token-minting mode.
    async fn minted(&self) -> Acquired<Arc<Credential>> {
        match self {
            AuthMode::OAuth2ClientCredentials(client) => client.credential().await,
            AuthMode::JwtBearer(client) => client.credential().await,
            AuthMode::GceMetadata(client) => client.credential().await,
            AuthMode::None
            | AuthMode::Bearer(_)
            | AuthMode::ApiKey(_)
            | AuthMode::Basic(_)
            | AuthMode::Signature(_)
            | AuthMode::SigV4(_)
            | AuthMode::Credentials(_) => Err(AuthError::Unavailable {
                reason: format!("auth mode `{}` mints no token", self.kind().as_str()),
            }),
        }
    }

    /// Drop the token a minting mode holds, so the next request mints again.
    ///
    /// A provider that revokes a token before its advertised expiry answers 401
    /// or 403, and the cache would otherwise re-present the same token until its
    /// renewal point -- at least half its lifetime, so half an hour of certain
    /// refusals on an hour-long token. A mode that mints nothing has nothing to
    /// drop: a resolved spec is the operator's own value, and a signing scheme
    /// signs afresh each time.
    pub fn invalidate(&self) {
        match self {
            AuthMode::OAuth2ClientCredentials(client) => {
                if let Some(source) = client.source.get() {
                    source.invalidate();
                }
            }
            AuthMode::JwtBearer(client) => client.source.invalidate(),
            AuthMode::GceMetadata(client) => client.source.invalidate(),
            AuthMode::None
            | AuthMode::Bearer(_)
            | AuthMode::ApiKey(_)
            | AuthMode::Basic(_)
            | AuthMode::Signature(_)
            | AuthMode::SigV4(_)
            | AuthMode::Credentials(_) => {}
        }
    }

    /// The token-response fields the mode exposes to templates as `auth.*`,
    /// minting the token first when none is cached; `None` for a mode that
    /// exposes nothing, so a request render never mints for nothing.
    ///
    /// # Errors
    ///
    /// Returns the token-exchange error.
    pub async fn exposed(&self) -> Result<Option<Value>> {
        match self {
            AuthMode::OAuth2ClientCredentials(client) if client.exposes => self.extra().await,
            AuthMode::JwtBearer(client) if client.exposes => self.extra().await,
            _ => Ok(None),
        }
    }

    /// What the exchange returned beside the token, `None` when the response
    /// carried none of the fields the profile named.
    async fn extra(&self) -> Result<Option<Value>> {
        let credential = self.minted().await.map_err(acquisition_error)?;
        Ok(credential.extra.as_deref().cloned())
    }

    /// Resolve the credential (and mint a token) without sending a data request.
    ///
    /// # Errors
    ///
    /// Returns the credential or token-exchange error.
    pub async fn probe(&self) -> Result<()> {
        match self {
            AuthMode::None => Ok(()),
            AuthMode::Bearer(placed) | AuthMode::ApiKey(placed) | AuthMode::Basic(placed) => {
                placed.resolve().await
            }
            AuthMode::OAuth2ClientCredentials(_)
            | AuthMode::JwtBearer(_)
            | AuthMode::GceMetadata(_) => {
                self.minted().await.map(|_| ()).map_err(acquisition_error)
            }
            AuthMode::Signature(signer) => signer.secret_key.value().await.map(|_| ()),
            AuthMode::SigV4(signer) => signer.keys().await.map(|_| ()),
            AuthMode::Credentials(credentials) => credentials.resolve().await,
        }
    }
}

/// One instance's mode as scalo's signing hook over one request's context.
///
/// Borrowed rather than owned: the mode lives on the shape for the life of the
/// instance and the context for the life of the tick, so a signer is made per
/// request and costs two pointers.
#[derive(Clone, Copy)]
pub struct ModeSigner<'a> {
    mode: &'a AuthMode,
    ctx: &'a TemplateCtx,
}

/// Hand-written: the context carries whatever the profile exposed to its
/// templates, up to a response body, so the mode's kind is all a render says.
impl fmt::Debug for ModeSigner<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModeSigner")
            .field("mode", &self.mode.kind().as_str())
            .finish_non_exhaustive()
    }
}

/// The dispatcher: each arm is itself a [`RequestSigner`], so the placement of
/// a resolved credential, the placement of a minted token and a signing scheme
/// all reach the request the same way.
impl RequestSigner for ModeSigner<'_> {
    async fn sign(&self, request: &mut reqwest::Request) -> std::result::Result<(), SignError> {
        match self.mode {
            AuthMode::None => Ok(()),
            AuthMode::Bearer(placed) | AuthMode::ApiKey(placed) | AuthMode::Basic(placed) => {
                placed.placement.sign(request).await
            }
            AuthMode::OAuth2ClientCredentials(_)
            | AuthMode::JwtBearer(_)
            | AuthMode::GceMetadata(_) => {
                HeaderPlacement::bearer(Minting { mode: self.mode })
                    .sign(request)
                    .await
            }
            AuthMode::Signature(signer) => signer.sign(request).await,
            AuthMode::SigV4(signer) => signer.scoped(self.ctx).sign(request).await,
            AuthMode::Credentials(credentials) => credentials.placements.sign(request).await,
        }
    }
}

/// A token-minting mode as the source its placement reads.
#[derive(Clone, Copy)]
struct Minting<'a> {
    mode: &'a AuthMode,
}

/// Hand-written for the same reason as [`ModeSigner`]'s: the mode's kind and
/// nothing of what it holds.
impl fmt::Debug for Minting<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Minting")
            .field("mode", &self.mode.kind().as_str())
            .finish_non_exhaustive()
    }
}

impl CredentialSource for Minting<'_> {
    async fn credential(&self) -> Acquired<Arc<Credential>> {
        self.mode.minted().await
    }
}

/// A signing scheme's own failure on its way into the signing hook.
///
/// The error travels whole as the cause so [`credential_error`] hands back the
/// same one the scheme raised, rather than a status flattened into a string.
/// Nothing here marks a failure worth signing again: the TICK is the retry unit,
/// because the scheduler runs a failed tick over the same window, and a second
/// backoff inside one request would only lengthen how long one mint holds a
/// mode's renewal gate.
fn signing_failure(error: Error) -> SignError {
    SignError::with_cause(error.to_string(), error)
}

/// The framework error behind a signing failure.
///
/// A signing scheme handed its error through as the cause, so that error comes
/// back unchanged and keeps the status the executor counts and the fixture
/// asserts. A credential source reports [`AuthError`] instead: a refusal keeps
/// its status as an API error; an endpoint that could not be reached, one that
/// ran out of time, and a waiter handed either of those are source failures, so
/// the metric says which; a response that is not a credential -- or one the
/// consumer could not supply at all -- is a credential failure.
#[must_use]
pub fn credential_error(error: SignError) -> Error {
    match error {
        SignError::Failed {
            message,
            cause: Some(cause),
            ..
        } => match cause.downcast::<Error>() {
            Ok(raised) => *raised,
            Err(cause) => Error::Credential(format!("{message}: {cause}")),
        },
        SignError::Failed { message, .. } => Error::Credential(message),
        SignError::Auth(AuthError::Refused { status, detail, .. }) => Error::Api {
            status,
            text: detail,
            throttled: false,
        },
        SignError::Auth(unreachable @ AuthError::Unreachable { .. }) => {
            Error::Source(unreachable.to_string())
        }
        // The message is written here rather than taken from the variant's
        // Display so the metric reads `timeout`: the category of a failure that
        // is not an HTTP answer comes off its text.
        SignError::Auth(AuthError::TimedOut { secs }) => {
            Error::Source(format!("credential exchange timed out after {secs}s"))
        }
        // A waiter on an exchange that could not be reached or ran out of time
        // reports what that one exchange reported, under the same category the
        // caller that ran it got.
        SignError::Auth(AuthError::Shared {
            message,
            transient: true,
        }) => Error::Source(message),
        // A failure the fetcher raised on its way in already says why in its own
        // words; re-rendering it through the variant's Display would stack a
        // third prefix on it.
        SignError::Auth(AuthError::Unavailable { reason }) => Error::Credential(reason),
        SignError::Auth(other) => Error::Credential(other.to_string()),
        other => Error::Credential(other.to_string()),
    }
}

fn set_header(
    request: &mut reqwest::Request,
    name: reqwest::header::HeaderName,
    value: &str,
) -> Result<()> {
    let mut value = HeaderValue::from_str(value)
        .map_err(|e| Error::Credential(format!("credential is not a valid header value: {e}")))?;
    value.set_sensitive(true);
    request.headers_mut().insert(name, value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use reqwest::header::AUTHORIZATION;

    use super::*;
    use crate::profile::{CredentialPlacementSpec, SignaturePreset};

    fn exchange() -> Arc<ExchangeClient> {
        crate::request::exchange_client().unwrap()
    }

    /// The mode over an instance context with nothing in it, which is what a
    /// profile whose credential templates are literals binds against.
    fn build(spec: &AuthSpec, identity: &InstanceAuth) -> Result<AuthMode> {
        AuthMode::build(spec, identity, exchange(), &TemplateCtx::new())
    }

    /// Sign through the hook the executor reaches a mode by, with the framework
    /// error the executor counts.
    async fn authorize(
        mode: &AuthMode,
        request: &mut reqwest::Request,
        ctx: &TemplateCtx,
    ) -> Result<()> {
        RequestSigner::sign(&mode.signer(ctx), request)
            .await
            .map_err(credential_error)
    }

    fn identity(mode: AuthKind) -> InstanceAuth {
        InstanceAuth {
            mode,
            token: Some("tok".into()),
            key: Some("k".into()),
            username: Some("u".into()),
            password: Some("p".into()),
            client_id: Some("id".into()),
            client_secret: Some("s".into()),
            ..InstanceAuth::default()
        }
    }

    fn spec(accepts: &[AuthKind]) -> AuthSpec {
        AuthSpec {
            accepts: accepts.to_vec(),
            ..AuthSpec::default()
        }
    }

    /// An instance of the `credentials` mode carrying these named specs.
    fn credentials(named: &[(&str, &str)]) -> InstanceAuth {
        InstanceAuth {
            mode: AuthKind::Credentials,
            credentials: named
                .iter()
                .map(|(name, spec)| ((*name).to_owned(), SensitiveString::from(*spec)))
                .collect(),
            ..InstanceAuth::default()
        }
    }

    /// Every spec the counting resolver has been asked for.
    static READS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

    /// Resolves a spec to its own text and records the read, so a test can count
    /// what a store is asked for rather than inspect how a source is held.
    struct Counting;

    impl ResolveSecret for Counting {
        fn resolve(spec: &str) -> impl Future<Output = Result<SensitiveString>> + Send {
            READS.lock().expect("the read log").push(spec.to_owned());
            std::future::ready(Ok(SensitiveString::from(spec)))
        }
    }

    /// One credential in a header of its own.
    fn placed_in(header: &str, from: &str) -> CredentialPlacementSpec {
        CredentialPlacementSpec {
            header: Some(header.to_owned()),
            from: Some(from.to_owned()),
            ..CredentialPlacementSpec::default()
        }
    }

    /// One header whose value is composed from named credentials.
    fn composed_in(header: &str, value: &str) -> CredentialPlacementSpec {
        CredentialPlacementSpec {
            header: Some(header.to_owned()),
            value: Some(value.to_owned()),
            ..CredentialPlacementSpec::default()
        }
    }

    fn request() -> reqwest::Request {
        reqwest::Request::new(
            reqwest::Method::GET,
            "https://api.example/x".parse().unwrap(),
        )
    }

    #[tokio::test]
    async fn bearer_sets_the_authorization_header_and_marks_it_sensitive() {
        let mode = build(&spec(&[AuthKind::Bearer]), &identity(AuthKind::Bearer)).unwrap();
        let mut req = request();
        authorize(&mode, &mut req, &TemplateCtx::new())
            .await
            .unwrap();
        let value = req.headers().get(AUTHORIZATION).unwrap();
        assert_eq!(value.to_str().unwrap(), "Bearer tok");
        assert!(value.is_sensitive());
    }

    #[tokio::test]
    async fn api_key_goes_to_a_prefixed_header_or_a_query_parameter() {
        let mut header_spec = spec(&[AuthKind::ApiKey]);
        header_spec.api_key.header = Some("Authorization".into());
        header_spec.api_key.prefix = "SSWS ".into();
        let mode = build(&header_spec, &identity(AuthKind::ApiKey)).unwrap();
        let mut req = request();
        authorize(&mode, &mut req, &TemplateCtx::new())
            .await
            .unwrap();
        assert_eq!(
            req.headers().get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "SSWS k"
        );

        let mut query_spec = spec(&[AuthKind::ApiKey]);
        query_spec.api_key.query = Some("api_key".into());
        let mode = build(&query_spec, &identity(AuthKind::ApiKey)).unwrap();
        let mut req = request();
        authorize(&mode, &mut req, &TemplateCtx::new())
            .await
            .unwrap();
        assert_eq!(req.url().query(), Some("api_key=k"));

        // A header name the profile got wrong is refused when the instance is
        // bound, not on the first request it would have gone out on.
        let mut bad = spec(&[AuthKind::ApiKey]);
        bad.api_key.header = Some("X Api Key".into());
        let err = build(&bad, &identity(AuthKind::ApiKey)).unwrap_err();
        assert!(err.to_string().contains("auth.api_key header"), "{err}");
    }

    /// A key whose own value carries a query separator arrives as one
    /// parameter, which formatting it into the URL would not manage.
    #[tokio::test]
    async fn a_query_key_is_encoded_rather_than_appended_raw() {
        let mut query_spec = spec(&[AuthKind::ApiKey]);
        query_spec.api_key.query = Some("api_key".into());
        let mut id = identity(AuthKind::ApiKey);
        id.key = Some("a&b=c".into());
        let mode = build(&query_spec, &id).unwrap();
        let mut req = reqwest::Request::new(
            reqwest::Method::GET,
            "https://api.example/x?page=2".parse().unwrap(),
        );
        authorize(&mode, &mut req, &TemplateCtx::new())
            .await
            .unwrap();
        assert_eq!(req.url().query(), Some("page=2&api_key=a%26b%3Dc"));
        assert_eq!(req.url().query_pairs().count(), 2);
    }

    #[tokio::test]
    async fn basic_encodes_user_and_password_and_marks_it_sensitive() {
        let mode = build(&spec(&[AuthKind::Basic]), &identity(AuthKind::Basic)).unwrap();
        let mut req = request();
        authorize(&mode, &mut req, &TemplateCtx::new())
            .await
            .unwrap();
        let value = req.headers().get(AUTHORIZATION).unwrap();
        assert_eq!(value.to_str().unwrap(), "Basic dTpw");
        assert!(value.is_sensitive());
    }

    /// The colon separates the two halves of a basic credential and RFC 7617
    /// gives the username no way to escape one, so a username carrying one
    /// would authenticate as something other than what was configured. The
    /// username is as static as the `api_key` header name, so the instance is
    /// refused when it is bound rather than on the first request.
    #[test]
    fn a_colon_in_the_basic_username_is_refused_when_the_instance_is_bound() {
        let mut id = identity(AuthKind::Basic);
        id.username = Some("account:1234".to_owned());
        id.password = Some("s3cr3t-do-not-print".into());
        let err = build(&spec(&[AuthKind::Basic]), &id)
            .expect_err("the colon would move the field separator");
        assert!(matches!(err, Error::Config(_)), "{err:?}");
        assert!(err.to_string().contains("colon"), "{err}");
        assert!(!err.to_string().contains("s3cr3t-do-not-print"), "{err}");
    }

    #[test]
    fn a_mode_the_profile_does_not_accept_or_a_missing_field_is_a_config_error() {
        let err = build(&spec(&[AuthKind::Bearer]), &identity(AuthKind::Basic)).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
        let mut missing = identity(AuthKind::Bearer);
        missing.token = None;
        let err = build(&spec(&[AuthKind::Bearer]), &missing).unwrap_err();
        assert!(err.to_string().contains("token"), "{err}");
    }

    /// A token endpoint over plaintext hands the client secret to anyone on the
    /// path, because the secret is a field of the form that is posted. Loopback
    /// is allowed so a fixture needs no certificate.
    #[tokio::test]
    async fn a_plaintext_token_endpoint_is_refused_and_a_loopback_one_is_not() {
        let mut oauth_spec = spec(&[AuthKind::Oauth2ClientCredentials]);
        oauth_spec.oauth2_client_credentials.token_url = "http://idp.example/token".into();
        let mode = build(&oauth_spec, &identity(AuthKind::Oauth2ClientCredentials)).unwrap();
        let err = mode
            .probe()
            .await
            .expect_err("the client secret would go out in the clear");
        assert!(err.to_string().contains("https"), "{err}");
        assert!(matches!(err, Error::Credential(_)), "{err:?}");

        // A loopback endpoint gets as far as trying to reach it.
        oauth_spec.oauth2_client_credentials.token_url =
            "http://127.0.0.1:1/token".parse().unwrap();
        let mode = build(&oauth_spec, &identity(AuthKind::Oauth2ClientCredentials)).unwrap();
        let err = mode
            .probe()
            .await
            .expect_err("nothing is listening on port 1");
        assert!(matches!(err, Error::Source(_)), "{err:?}");
    }

    /// Nothing a mode holds may reach a log or a report that formats it, so
    /// every mode's own render is checked against the credential it was built
    /// with.
    #[test]
    fn no_mode_renders_its_credential() {
        let mut every = spec(&[
            AuthKind::Bearer,
            AuthKind::ApiKey,
            AuthKind::Basic,
            AuthKind::Oauth2ClientCredentials,
            AuthKind::Signature,
            AuthKind::JwtBearer,
            AuthKind::GceMetadata,
            AuthKind::SigV4,
            AuthKind::Credentials,
        ]);
        every.api_key.header = Some("X-Api-Key".into());
        every.oauth2_client_credentials.token_url = "https://idp.example/token".into();
        every.gce_metadata.url = "http://169.254.169.254/token".into();
        every.signature.preset = Some(SignaturePreset::DuoV5);
        every.credentials = vec![
            placed_in("X-Api-Key", "one"),
            composed_in(
                "Authorization",
                "a={{ credentials.one }};b={{ credentials.two }}",
            ),
        ];
        for kind in every.accepts.clone() {
            let mut id = InstanceAuth {
                mode: kind,
                token: Some("s3cr3t-do-not-print".into()),
                key: Some("s3cr3t-do-not-print".into()),
                username: Some("account".into()),
                password: Some("s3cr3t-do-not-print".into()),
                client_id: Some("client-42".into()),
                client_secret: Some("s3cr3t-do-not-print".into()),
                key_id: Some("DI".into()),
                secret_key: Some("s3cr3t-do-not-print".into()),
                ..InstanceAuth::default()
            };
            id.private_key = Some("s3cr3t-do-not-print".into());
            id.access_key_id = Some("AKIA".into());
            id.secret_access_key = Some("s3cr3t-do-not-print".into());
            id.credentials = ["one", "two"]
                .into_iter()
                .map(|name| (name.to_owned(), "s3cr3t-do-not-print".into()))
                .collect();
            let mode = build(&every, &id).unwrap_or_else(|e| panic!("{}: {e}", kind.as_str()));
            let rendered = format!("{mode:?}");
            assert!(
                !rendered.contains("s3cr3t-do-not-print"),
                "{}: {rendered}",
                kind.as_str()
            );
        }
    }

    /// Duo's own integration key and secret key from its published signing
    /// example, so the vectors below are over the values the spec uses.
    const DUO_IKEY: &str = "DIWJ8X6AEYOR5OMC6TQ1";
    const DUO_SKEY: &str = "Zh5eGmUq9zpfQnyUIu5OL9iWoMMv5ZNmk3zLJ4Ep";

    /// The date of Duo's own example, so a vector is over a fixed instant.
    const VECTOR_DATE: &str = "Tue, 21 Aug 2012 17:29:18 -0000";

    /// The SHA-512 of the empty string, which is what a GET's body hash and an
    /// unsigned header set hash to under Duo's v5 scheme.
    const EMPTY_SHA512: &str = "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce4\
                                7d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e";

    /// A Duo instance on `preset`.
    fn duo(preset: SignaturePreset) -> Result<AuthMode> {
        build(
            &spec(&[AuthKind::Signature]),
            &InstanceAuth {
                mode: AuthKind::Signature,
                key_id: Some(DUO_IKEY.to_owned()),
                secret_key: Some(DUO_SKEY.into()),
                signature_preset: Some(preset),
                ..InstanceAuth::default()
            },
        )
    }

    /// The signer of a signing mode.
    fn signer(mode: &AuthMode) -> &Signature {
        match mode {
            AuthMode::Signature(signer) => signer,
            other => panic!("not a signing mode: {other:?}"),
        }
    }

    /// The log request of Duo's example, whose query carries a comma and a
    /// slash so the canonical encoding is visible.
    fn duo_log_request() -> reqwest::Request {
        reqwest::Request::new(
            reqwest::Method::GET,
            "https://API-Deadbeef.duosecurity.com/admin/v2/logs/authentication?mintime=1&limit=2&next_offset=1532951895000,af0b/a?b"
                .parse()
                .unwrap(),
        )
    }

    /// The canonical string a signer builds for `request` at [`VECTOR_DATE`].
    fn canonical_at_vector_date(signer: &Signature, request: &reqwest::Request) -> String {
        let at = chrono::DateTime::parse_from_rfc2822(VECTOR_DATE)
            .unwrap()
            .with_timezone(&chrono::Utc);
        let mut facts = signer.facts(request, at, "nonce-no-duo-scheme-reads-one");
        facts.insert(
            "headers_hash".to_owned(),
            Value::String(hex_digest(
                signer.digest,
                canonical_headers(request, &signer.signed_header_prefix).as_bytes(),
            )),
        );
        signer.canonical(&facts).unwrap()
    }

    /// Duo's signature version 5, against the canonical string Duo's own
    /// documentation lays out -- date, uppercase method, lowercase host, path,
    /// sorted RFC 3986 query, the body's SHA-512 and the signed headers'
    /// SHA-512, newline-joined -- and against a signature computed from that
    /// string by hand rather than by this implementation.
    #[tokio::test]
    async fn duo_v5_signs_sha512_over_the_seven_line_canonical_string() {
        let mode = duo(SignaturePreset::DuoV5).unwrap();
        assert_eq!(mode.kind(), AuthKind::Signature);
        let request = duo_log_request();
        let canonical = canonical_at_vector_date(signer(&mode), &request);
        assert_eq!(
            canonical,
            format!(
                "{VECTOR_DATE}\n\
                 GET\n\
                 api-deadbeef.duosecurity.com\n\
                 /admin/v2/logs/authentication\n\
                 limit=2&mintime=1&next_offset=1532951895000%2Caf0b%2Fa%3Fb\n\
                 {EMPTY_SHA512}\n\
                 {EMPTY_SHA512}"
            )
        );
        // hmac.new(skey, canonical, hashlib.sha512).hexdigest()
        assert_eq!(
            hex::encode(
                hmac_bytes(
                    SignatureDigest::Sha512,
                    DUO_SKEY.as_bytes(),
                    canonical.as_bytes()
                )
                .unwrap()
            ),
            "44e2bba2b2c84f216e65d32c60d4e32fdbfb47e84432c46f8f1dd6b0c142b86f\
             3232a144d3c3c87bfab13d84fdb76c1f76be45dd02529f54e34c06a08fc43f61"
        );
    }

    /// Duo's signature version 2, the legacy scheme an older tenant selects:
    /// the first five of those lines, HMAC-SHA1.
    #[tokio::test]
    async fn duo_v2_signs_sha1_over_the_five_line_canonical_string() {
        let mode = duo(SignaturePreset::DuoV2).unwrap();
        let request = duo_log_request();
        let canonical = canonical_at_vector_date(signer(&mode), &request);
        assert_eq!(
            canonical,
            format!(
                "{VECTOR_DATE}\n\
                 GET\n\
                 api-deadbeef.duosecurity.com\n\
                 /admin/v2/logs/authentication\n\
                 limit=2&mintime=1&next_offset=1532951895000%2Caf0b%2Fa%3Fb"
            )
        );
        // hmac.new(skey, canonical, hashlib.sha1).hexdigest()
        assert_eq!(
            hex::encode(
                hmac_bytes(
                    SignatureDigest::Sha1,
                    DUO_SKEY.as_bytes(),
                    canonical.as_bytes()
                )
                .unwrap()
            ),
            "4f57a877fdd9f8f60dc14e08802a887098e381b0"
        );
    }

    /// Which digest reaches the `Authorization` header: v5 puts 128 hex
    /// characters of SHA-512 in the Basic password and v2 puts 40 of SHA-1, so
    /// the default is the current scheme and the older tenant's selection is
    /// the only way back to the legacy one.
    #[tokio::test]
    async fn the_preset_decides_the_digest_that_reaches_the_authorization_header() {
        let signed = async |preset| {
            let mode = duo(preset).unwrap();
            let mut request = duo_log_request();
            authorize(&mode, &mut request, &TemplateCtx::new())
                .await
                .unwrap();
            let header = request.headers().get(AUTHORIZATION).unwrap();
            assert!(header.is_sensitive());
            let credential = base64::engine::general_purpose::STANDARD
                .decode(header.to_str().unwrap().strip_prefix("Basic ").unwrap())
                .unwrap();
            let credential = String::from_utf8(credential).unwrap();
            let (ikey, digest) = credential.split_once(':').unwrap();
            assert_eq!(ikey, DUO_IKEY);
            // The date is signed, so the request carries the one it signed.
            let date = request
                .headers()
                .get(reqwest::header::DATE)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            assert!(date.ends_with(" -0000"), "RFC 2822 with -0000: {date}");
            assert!(chrono::DateTime::parse_from_rfc2822(&date).is_ok());
            digest.to_owned()
        };
        assert_eq!(signed(SignaturePreset::DuoV5).await.len(), 128);
        assert_eq!(signed(SignaturePreset::DuoV2).await.len(), 40);
    }

    /// The whole signature of a live request, end to end: the built request is
    /// signed, the date it carries is read back, and the credential is rebuilt
    /// from that date with the block-level HMAC type rather than the one the
    /// mode uses.
    #[tokio::test]
    async fn a_signed_request_carries_the_credential_the_scheme_specifies() {
        use hmac::{Hmac, Mac};

        let mode = duo(SignaturePreset::DuoV5).unwrap();
        // A non-default port is part of the host, as it is in the Host header.
        let mut request = reqwest::Request::new(
            reqwest::Method::GET,
            "http://127.0.0.1:8081/admin/v1/check".parse().unwrap(),
        );
        authorize(&mode, &mut request, &TemplateCtx::new())
            .await
            .unwrap();
        let date = request
            .headers()
            .get(reqwest::header::DATE)
            .unwrap()
            .to_str()
            .unwrap();
        let canonical = format!(
            "{date}\nGET\n127.0.0.1:8081\n/admin/v1/check\n\n{EMPTY_SHA512}\n{EMPTY_SHA512}"
        );
        let mut mac = Hmac::<sha2::Sha512>::new_from_slice(DUO_SKEY.as_bytes()).unwrap();
        mac.update(canonical.as_bytes());
        assert_eq!(
            request
                .headers()
                .get(AUTHORIZATION)
                .unwrap()
                .to_str()
                .unwrap(),
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!(
                    "{DUO_IKEY}:{}",
                    hex::encode(mac.finalize().into_bytes())
                ))
            )
        );
    }

    /// A signed header is both carried and covered: a scheme whose headers set
    /// `x-duo-date` signs the hash of that header, not the hash of nothing.
    #[tokio::test]
    async fn a_signed_header_the_scheme_sets_is_covered_by_the_header_hash() {
        let mut duo_spec = spec(&[AuthKind::Signature]);
        let mut scheme = SignaturePreset::DuoV5.spec();
        scheme
            .headers
            .insert("x-duo-date".to_owned(), "{{ signature.date }}".to_owned());
        duo_spec.signature = scheme;
        let mode = build(
            &duo_spec,
            &InstanceAuth {
                mode: AuthKind::Signature,
                key_id: Some(DUO_IKEY.to_owned()),
                secret_key: Some(DUO_SKEY.into()),
                ..InstanceAuth::default()
            },
        )
        .unwrap();
        let mut request = duo_log_request();
        request
            .headers_mut()
            .insert(reqwest::header::DATE, HeaderValue::from_static(VECTOR_DATE));
        request
            .headers_mut()
            .insert("x-duo-date", HeaderValue::from_static(VECTOR_DATE));
        let canonical = canonical_at_vector_date(signer(&mode), &request);
        // hashlib.sha512(("x-duo-date\x00" + date).encode()).hexdigest()
        assert!(
            canonical.ends_with(
                "379dbaf99303e804a3d9f0a2e4d8c4c99397911addce373e45e217371ebbda6b\
                 283b3d9e81e4938add811b5817928f843c53d07f9d57f190c9795ad259131a41"
            ),
            "{canonical}"
        );
    }

    /// The Cortex XDR advanced shape, which the axis has to be able to say
    /// even with no profile shipping for it: a plain SHA-256 over the api key,
    /// a nonce and a millisecond timestamp, with the key id and both of those
    /// carried in headers of their own and the digest in a bare
    /// `Authorization`.
    #[tokio::test]
    async fn the_cortex_xdr_advanced_shape_is_expressible() {
        let mut xdr = spec(&[AuthKind::Signature]);
        xdr.signature = SignatureSpec {
            digest: SignatureDigest::Sha256,
            keying: SignatureKeying::Prefix,
            canonical: vec!["{{ signature.nonce }}{{ signature.timestamp_ms }}".to_owned()],
            headers: [
                ("x-xdr-nonce", "{{ signature.nonce }}"),
                ("x-xdr-timestamp", "{{ signature.timestamp_ms }}"),
                ("x-xdr-auth-id", "{{ signature.key_id }}"),
            ]
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect(),
            place: SignatureTarget::Digest,
            ..SignatureSpec::default()
        };
        let mode = build(
            &xdr,
            &InstanceAuth {
                mode: AuthKind::Signature,
                key_id: Some("17".to_owned()),
                secret_key: Some("cortex-advanced-key".into()),
                ..InstanceAuth::default()
            },
        )
        .unwrap();
        let mut request = reqwest::Request::new(
            reqwest::Method::POST,
            "https://api-tenant.xdr.au.paloaltonetworks.com/public_api/v1/audits/management_logs"
                .parse()
                .unwrap(),
        );
        authorize(&mode, &mut request, &TemplateCtx::new())
            .await
            .unwrap();
        let header = |name: &str| {
            request
                .headers()
                .get(name)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        };
        assert_eq!(header("x-xdr-auth-id"), "17");
        let nonce = header("x-xdr-nonce");
        let timestamp = header("x-xdr-timestamp");
        // hashlib.sha256((api_key + nonce + timestamp).encode()).hexdigest(),
        // which is the scheme's own recipe rather than an HMAC.
        assert_eq!(
            header("authorization"),
            hex::encode(digest_bytes(
                SignatureDigest::Sha256,
                format!("cortex-advanced-key{nonce}{timestamp}").as_bytes()
            ))
        );
        // That recipe over a vector computed by hand, so the keying and not
        // just the wiring is pinned.
        assert_eq!(
            hex::encode(digest_bytes(
                SignatureDigest::Sha256,
                b"cortex-advanced-key0123456789abcdef0123456789abcdef1345570158000"
            )),
            "5555c6d3b86efcf80dc278336730afc98ff86ef506c6e0b3f215c38cdef0fcf2"
        );
    }

    /// The scheme's refusals: an instance with no secret key, a canonical
    /// string that reads a name only a unit supplies, and a block that names a
    /// preset and also sets a field of its own.
    #[test]
    fn a_signing_scheme_is_refused_where_it_could_sign_the_wrong_thing() {
        let missing = build(
            &spec(&[AuthKind::Signature]),
            &InstanceAuth {
                mode: AuthKind::Signature,
                key_id: Some("DI".to_owned()),
                signature_preset: Some(SignaturePreset::DuoV5),
                ..InstanceAuth::default()
            },
        )
        .unwrap_err();
        assert!(missing.to_string().contains("secret_key"), "{missing}");

        let anonymous = build(
            &spec(&[AuthKind::Signature]),
            &InstanceAuth {
                mode: AuthKind::Signature,
                secret_key: Some("s".into()),
                signature_preset: Some(SignaturePreset::DuoV5),
                ..InstanceAuth::default()
            },
        )
        .unwrap_err();
        assert!(anonymous.to_string().contains("key_id"), "{anonymous}");

        let mut per_unit = spec(&[AuthKind::Signature]);
        per_unit.signature.canonical = vec!["{{ vars.tenant }}".to_owned()];
        let refused = build(
            &per_unit,
            &InstanceAuth {
                mode: AuthKind::Signature,
                secret_key: Some("s".into()),
                ..InstanceAuth::default()
            },
        )
        .unwrap_err();
        assert!(refused.to_string().contains("reads `vars`"), "{refused}");

        let mut half = SignatureSpec {
            preset: Some(SignaturePreset::DuoV5),
            ..SignatureSpec::default()
        };
        half.digest = SignatureDigest::Sha1;
        let issues = crate::profile::signature_spec_issues(&half);
        assert!(
            issues.iter().any(|i| i.field == "auth.signature.preset"),
            "{issues:?}"
        );
    }

    /// SigV4, checked against a signature computed by hand from the spec:
    /// the canonical request over the signed headers with the body's hash
    /// as the payload, the string to sign with the credential scope, and
    /// the AWS4 key derivation. The scope comes from the context, so the
    /// same mode signs a region-locked unit for its own region.
    #[tokio::test]
    async fn sigv4_signs_the_built_request_for_the_scope_the_context_renders() {
        use reqsign::hash::{hex_hmac_sha256, hex_sha256, hmac_sha256};

        let mut sigv4_spec = spec(&[AuthKind::SigV4]);
        sigv4_spec.sigv4.service = "{{ vars.service }}".into();
        sigv4_spec.sigv4.region = "{{ vars.region }}".into();
        let mode = build(
            &sigv4_spec,
            &InstanceAuth {
                mode: AuthKind::SigV4,
                access_key_id: Some("AKIAIOSFODNN7EXAMPLE".into()),
                secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
                ..InstanceAuth::default()
            },
        )
        .unwrap();
        assert_eq!(mode.kind(), AuthKind::SigV4);
        let mut ctx = TemplateCtx::new();
        ctx.set(
            "vars",
            serde_json::json!({"service": "cloudtrail", "region": "us-east-1"}),
        );
        let body = br#"{"MaxResults":50}"#;
        let mut req = reqwest::Client::new()
            .post("https://cloudtrail.us-east-1.amazonaws.com/")
            .header("Content-Type", "application/x-amz-json-1.1")
            .header("X-Amz-Target", "CloudTrail_20131101.LookupEvents")
            .body(body.to_vec())
            .build()
            .unwrap();
        authorize(&mode, &mut req, &ctx).await.unwrap();

        let header = |name: &str| {
            req.headers()
                .get(name)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        };
        assert_eq!(header("x-amz-content-sha256"), hex_sha256(body));
        let amz_date = header("x-amz-date");
        let date = &amz_date[..8];
        let authorization = header("authorization");
        assert!(req.headers().get("authorization").unwrap().is_sensitive());
        let expected_prefix = format!(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/{date}/us-east-1/cloudtrail/aws4_request, SignedHeaders="
        );
        assert!(
            authorization.starts_with(&expected_prefix),
            "{authorization}"
        );
        let rest = &authorization[expected_prefix.len()..];
        let (signed_headers, signature) = rest.split_once(", Signature=").unwrap();
        let names: Vec<&str> = signed_headers.split(';').collect();
        assert!(names.contains(&"host") && names.contains(&"x-amz-target"));
        let mut canonical = String::from("POST\n/\n\n");
        for name in &names {
            canonical.push_str(&format!("{name}:{}\n", header(name).trim()));
        }
        canonical.push_str(&format!("\n{signed_headers}\n{}", hex_sha256(body)));
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{date}/us-east-1/cloudtrail/aws4_request\n{}",
            hex_sha256(canonical.as_bytes())
        );
        let k_date = hmac_sha256(
            b"AWS4wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            date.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, b"us-east-1");
        let k_service = hmac_sha256(&k_region, b"cloudtrail");
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        assert_eq!(
            signature,
            hex_hmac_sha256(&k_signing, string_to_sign.as_bytes()),
            "the signature the spec's derivation yields"
        );

        // A unit whose context names another service and region is signed
        // for that scope by the same mode.
        ctx.set(
            "vars",
            serde_json::json!({"service": "health", "region": "us-east-1"}),
        );
        let mut probe = reqwest::Request::new(
            reqwest::Method::GET,
            "https://health.us-east-1.amazonaws.com/".parse().unwrap(),
        );
        authorize(&mode, &mut probe, &ctx).await.unwrap();
        assert!(
            probe
                .headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("/us-east-1/health/aws4_request, "),
            "the scope follows the context"
        );
        assert_eq!(
            probe
                .headers()
                .get("x-amz-content-sha256")
                .unwrap()
                .to_str()
                .unwrap(),
            hex_sha256(b""),
            "a GET is signed over the empty payload"
        );

        // One JSON document may carry both halves, in either spelling.
        let document = build(
            &sigv4_spec,
            &InstanceAuth {
                mode: AuthKind::SigV4,
                credentials_json: Some(
                    r#"{"AccessKeyId": "AKIAFROMJSONEXAMPLE0", "SecretAccessKey": "s"}"#.into(),
                ),
                ..InstanceAuth::default()
            },
        )
        .unwrap();
        let mut req = reqwest::Request::new(
            reqwest::Method::GET,
            "https://sts.us-east-1.amazonaws.com/".parse().unwrap(),
        );
        authorize(&document, &mut req, &ctx).await.unwrap();
        assert!(
            req.headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIAFROMJSONEXAMPLE0/")
        );
        let half = build(
            &sigv4_spec,
            &InstanceAuth {
                mode: AuthKind::SigV4,
                access_key_id: Some("AKIA".into()),
                ..InstanceAuth::default()
            },
        )
        .unwrap_err();
        assert!(half.to_string().contains("secret_access_key"), "{half}");
    }

    /// An STS that answers every `AssumeRole` with one session and records
    /// the requests it signed and sent.
    #[derive(Debug, Clone, Default)]
    struct FakeSts {
        calls: Arc<std::sync::Mutex<Vec<(String, http::HeaderMap, String)>>>,
    }

    impl reqsign::HttpSend for FakeSts {
        fn http_send(
            &self,
            req: http::Request<bytes::Bytes>,
        ) -> impl Future<Output = reqsign::Result<http::Response<bytes::Bytes>>> + Send {
            let (parts, body) = req.into_parts();
            self.calls.lock().unwrap().push((
                parts.uri.to_string(),
                parts.headers,
                String::from_utf8_lossy(&body).into_owned(),
            ));
            std::future::ready(Ok(http::Response::builder()
                .status(200)
                .body(bytes::Bytes::from_static(
                    br#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>ASIASESSIONKEYEXAMPLE</AccessKeyId><SecretAccessKey>session-secret</SecretAccessKey><SessionToken>session-token-xyz</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleResult></AssumeRoleResponse>"#,
                ))
                .unwrap()))
        }
    }

    /// With `assume_role_arn` the keys sign one STS `AssumeRole` on the
    /// regional endpoint, and every data request is then signed with the
    /// session it returned -- its key id in the credential scope and its
    /// token in `x-amz-security-token` -- from one exchange per instance.
    #[tokio::test]
    async fn an_assumed_role_signs_data_requests_with_the_session_it_minted_once() {
        let sts = FakeSts::default();
        let mut sigv4_spec = spec(&[AuthKind::SigV4]);
        sigv4_spec.sigv4.service = "{{ vars.service }}".into();
        sigv4_spec.sigv4.region = "ap-southeast-2".into();
        let mode = build(
            &sigv4_spec,
            &InstanceAuth {
                mode: AuthKind::SigV4,
                access_key_id: Some("AKIAIOSFODNN7EXAMPLE".into()),
                secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
                assume_role_arn: Some("arn:aws:iam::123456789012:role/dfe-reader".into()),
                ..InstanceAuth::default()
            },
        )
        .unwrap();
        let AuthMode::SigV4(signer) = &mode else {
            panic!("sigv4")
        };
        let role = signer.assume_role.as_ref().expect("a role to assume");
        assert_eq!(role.role_arn, "arn:aws:iam::123456789012:role/dfe-reader");
        let mode = AuthMode::SigV4(SigV4 {
            service: Template::compile("{{ vars.service }}").unwrap(),
            region: Template::compile("ap-southeast-2").unwrap(),
            keys: SigV4Keys::Pair {
                access_key_id: Secret::new("AKIAIOSFODNN7EXAMPLE".into()),
                secret_access_key: Secret::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            },
            resolved: OnceCell::new(),
            assume_role: Some(AssumeRole {
                role_arn: role.role_arn.clone(),
                context: reqsign::Context::new().with_http_send(sts.clone()),
                session: OnceCell::new(),
            }),
        });
        let mut ctx = TemplateCtx::new();
        ctx.set("vars", serde_json::json!({"service": "cloudtrail"}));

        for _ in 0..2 {
            let mut req = reqwest::Request::new(
                reqwest::Method::GET,
                "https://cloudtrail.ap-southeast-2.amazonaws.com/"
                    .parse()
                    .unwrap(),
            );
            authorize(&mode, &mut req, &ctx).await.unwrap();
            let header = |name: &str| {
                req.headers()
                    .get(name)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned()
            };
            assert!(
                header("authorization")
                    .starts_with("AWS4-HMAC-SHA256 Credential=ASIASESSIONKEYEXAMPLE/"),
                "signed with the session key, not the static key: {}",
                header("authorization")
            );
            assert!(header("authorization").contains("/ap-southeast-2/cloudtrail/aws4_request, "));
            assert_eq!(header("x-amz-security-token"), "session-token-xyz");
        }

        let calls = sts.calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            1,
            "one AssumeRole per instance, the session cached"
        );
        let (uri, headers, _) = &calls[0];
        assert!(
            uri.starts_with("https://sts.ap-southeast-2.amazonaws.com/?"),
            "{uri}"
        );
        assert!(
            uri.contains("Action=AssumeRole")
                && uri.contains("RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fdfe-reader")
                && uri.contains("RoleSessionName=dfe-fetcher"),
            "{uri}"
        );
        let sts_auth = headers.get("authorization").unwrap().to_str().unwrap();
        assert!(
            sts_auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/")
                && sts_auth.contains("/ap-southeast-2/sts/aws4_request, "),
            "the exchange itself is signed with the static key: {sts_auth}"
        );
    }

    #[tokio::test]
    async fn an_env_spec_resolves_once_and_a_missing_variable_is_a_credential_error() {
        let secret = Secret::new("env:DFE_FETCHER_TEST_MISSING_SECRET_VAR".into());
        let err = secret.value().await.unwrap_err();
        assert!(matches!(err, Error::Credential(_)), "{err:?}");
        let literal = Secret::new("plain".into());
        assert_eq!(literal.value().await.unwrap(), "plain");
        assert_eq!(literal.value().await.unwrap(), "plain");
    }

    /// A spec that does not resolve is the resolver's own answer on every
    /// request, so an operator reads the variable's name rather than a cache's
    /// account of it.
    #[tokio::test]
    async fn a_spec_that_does_not_resolve_is_a_credential_error_on_every_request() {
        let mode = AuthMode::Bearer(Placed::bearer(Secret::new(
            "env:DFE_FETCHER_TEST_MISSING_SECRET_VAR".into(),
        )));
        for _ in 0..2 {
            let err = mode.probe().await.expect_err("the variable is not set");
            assert!(
                err.to_string()
                    .contains("DFE_FETCHER_TEST_MISSING_SECRET_VAR"),
                "the resolver's own answer, not a held one: {err}"
            );
            assert!(
                !err.to_string().contains("not supplied by the consumer"),
                "the fetcher's own words, one prefix: {err}"
            );
        }
    }

    /// A signing scheme raises a framework error, and the hook's error type
    /// must not flatten it: the executor classifies a refused token exchange by
    /// its STATUS, so a 401 has to come back out as one.
    #[test]
    fn a_signing_scheme_s_own_error_survives_the_signing_hook_whole() {
        let refused = Error::Api {
            status: 401,
            text: "token exchange refused: error=invalid_client".to_owned(),
            throttled: false,
        };
        match credential_error(signing_failure(refused)) {
            Error::Api { status, text, .. } => {
                assert_eq!(status, 401);
                assert!(text.contains("invalid_client"), "{text}");
            }
            other => panic!("expected the API error back, got {other:?}"),
        }

        let unresolved = Error::Credential("`TOKEN` is not set".to_owned());
        let back = credential_error(signing_failure(unresolved));
        assert!(matches!(back, Error::Credential(_)), "{back:?}");
        assert!(back.to_string().contains("`TOKEN` is not set"), "{back}");
    }

    /// A credential source of scalo's own reports `AuthError`, which maps onto
    /// the framework error by what it says rather than by its text -- and so
    /// onto the metrics label the driver counts under.
    #[test]
    fn a_scalo_acquisition_failure_maps_by_what_it_says() {
        let refused = credential_error(SignError::from(AuthError::Refused {
            url: "https://idp.example/token".to_owned(),
            status: 401,
            detail: "error=invalid_client".to_owned(),
        }));
        assert!(
            matches!(&refused, Error::Api { status: 401, text, .. } if text.contains("invalid_client")),
            "{refused:?}"
        );
        assert_eq!(refused.api_error_code(), "4xx");

        let unavailable = credential_error(SignError::from(AuthError::Unavailable {
            reason: "secret spec did not resolve".to_owned(),
        }));
        assert!(
            matches!(unavailable, Error::Credential(_)),
            "{unavailable:?}"
        );

        let malformed = credential_error(SignError::from(AuthError::Malformed {
            url: "https://idp.example/token".to_owned(),
            reason: "no access_token".to_owned(),
        }));
        assert!(matches!(malformed, Error::Credential(_)), "{malformed:?}");

        // A token endpoint that accepts the connection and then says nothing is
        // the USUAL hang, because the acquisition's deadline is the exchange
        // client's own timeout and fires first.
        let timed_out = credential_error(SignError::from(AuthError::TimedOut { secs: 30 }));
        assert!(matches!(timed_out, Error::Source(_)), "{timed_out:?}");
        assert_eq!(timed_out.api_error_code(), "timeout");

        // A caller that waited on one in-flight exchange is counted under the
        // same category as the caller that ran it.
        let waited = credential_error(SignError::from(AuthError::Shared {
            message: "credential exchange at https://idp.example/token failed: connect".to_owned(),
            transient: true,
        }));
        assert!(matches!(waited, Error::Source(_)), "{waited:?}");
        assert_eq!(waited.api_error_code(), "network");

        let waited_on_a_refusal = credential_error(SignError::from(AuthError::Shared {
            message: "credential exchange refused with status 401".to_owned(),
            transient: false,
        }));
        assert!(
            matches!(waited_on_a_refusal, Error::Credential(_)),
            "a refusal a waiter was handed carries no status to classify by: \
             {waited_on_a_refusal:?}"
        );
    }

    /// A template or a key the fetcher could not turn into a credential is the
    /// fetcher's own message, not its error prefix stacked under scalo's.
    #[test]
    fn a_failure_the_fetcher_raised_keeps_its_own_words() {
        let reason = "jwt_bearer token_url rendered empty";
        let error = acquisition_error(unavailable(Error::Config(reason.to_owned())));
        assert!(matches!(error, Error::Credential(_)), "{error:?}");
        assert_eq!(error.to_string(), format!("credential error: {reason}"));
        assert!(
            !error.to_string().contains("configuration error"),
            "one prefix, not three: {error}"
        );
        assert!(
            !error.to_string().contains("not supplied by the consumer"),
            "one prefix, not three: {error}"
        );
    }

    /// One acquisition is bounded by the exchange client's own attempts, so the
    /// cache's renewal gate is never released while an attempt the client
    /// scheduled is still in flight. The shipped client grants no retries, so
    /// its bound is one request's timeout; a client that granted them would
    /// count every attempt and the intervals between them.
    #[test]
    fn an_acquisition_is_bounded_by_the_exchange_clients_attempts() {
        let http = exchange();
        assert_eq!(http.config().max_retries, 0);
        assert_eq!(
            exchange_deadline(&http),
            Duration::from_secs(http.config().timeout_secs)
        );

        let retrying = ExchangeClient::new(scalo::http_client::HttpClientConfig {
            timeout_secs: 5,
            max_retries: 2,
            max_retry_interval_ms: 1_000,
            ..scalo::http_client::HttpClientConfig::default()
        })
        .unwrap();
        assert_eq!(
            exchange_deadline(&retrying),
            Duration::from_secs(5 * 3) + Duration::from_secs(2),
            "three attempts and the two intervals between them"
        );

        // An absurd timeout saturates rather than overflowing the sum: a bad
        // reload must not take the process down.
        let absurd = ExchangeClient::new(scalo::http_client::HttpClientConfig {
            timeout_secs: u64::MAX,
            max_retries: 1,
            max_retry_interval_ms: 1_000,
            ..scalo::http_client::HttpClientConfig::default()
        })
        .unwrap();
        assert_eq!(exchange_deadline(&absurd), Duration::MAX);
    }

    /// A metadata read is exempt from the https rule, so the host is what keeps
    /// it on the instance: the link-local address every cloud serves it on, a
    /// loopback fixture, or the name Google documents.
    #[test]
    fn a_metadata_url_off_this_instance_is_refused() {
        for url in [
            "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
            "http://127.0.0.1:8080/metadata/token",
            "http://localhost:8080/metadata/token",
            "http://[::1]:8080/metadata/token",
            "http://[fe80::1]/metadata/token",
        ] {
            assert!(metadata_url_issue(url).is_none(), "{url}");
        }
        for url in [
            "http://metadata.example/token",
            "http://10.0.0.1/token",
            "https://attacker.example/computeMetadata/v1/instance/service-accounts/default/token",
            "not a url",
        ] {
            let issue = metadata_url_issue(url).unwrap_or_else(|| panic!("{url} is refused"));
            assert!(!issue.is_empty(), "{url}");
        }

        // The URL is rendered and checked when the instance is bound, not on the
        // first tick it would have been read on.
        let mut gce = spec(&[AuthKind::GceMetadata]);
        gce.gce_metadata.url = "http://metadata.example/token".into();
        let err = build(&gce, &identity(AuthKind::GceMetadata))
            .expect_err("a metadata read off this instance");
        assert!(matches!(err, Error::Config(_)), "{err:?}");
        assert!(err.to_string().contains("auth.gce_metadata.url"), "{err}");

        let mut fine = spec(&[AuthKind::GceMetadata]);
        fine.gce_metadata.url = "http://{{ vars.metadata_host }}/token".into();
        let mut ctx = TemplateCtx::new();
        ctx.set(
            "vars",
            serde_json::json!({"metadata_host": "169.254.169.254"}),
        );
        AuthMode::build(&fine, &identity(AuthKind::GceMetadata), exchange(), &ctx)
            .expect("the instance's own var renders the metadata host");
    }

    /// A signer and its credential source are handed the unit's context, which
    /// can hold a response body and whatever the profile exposed as `auth.*`, so
    /// neither may print more than the mode's kind.
    #[test]
    fn a_signer_renders_the_mode_kind_and_nothing_of_the_context() {
        let mode = build(&spec(&[AuthKind::Bearer]), &identity(AuthKind::Bearer)).unwrap();
        let mut ctx = TemplateCtx::new();
        ctx.set(
            "auth",
            serde_json::json!({"instance_url": "s3cr3t-do-not-print"}),
        );
        ctx.set("vars", serde_json::json!({"token": "s3cr3t-do-not-print"}));
        for rendered in [
            format!("{:?}", mode.signer(&ctx)),
            format!("{:?}", Minting { mode: &mode }),
            format!(
                "{:?}",
                ScopedSigV4 {
                    keys: &SigV4 {
                        service: Template::compile("s3").unwrap(),
                        region: Template::compile("ap-southeast-2").unwrap(),
                        keys: SigV4Keys::Json(Secret::new("s3cr3t-do-not-print".into())),
                        resolved: OnceCell::new(),
                        assume_role: None,
                    },
                    ctx: &ctx,
                }
            ),
        ] {
            assert!(!rendered.contains("s3cr3t-do-not-print"), "{rendered}");
            assert!(!rendered.contains("instance_url"), "{rendered}");
        }
    }

    /// A credential is minted once per instance and per scope, so a template
    /// deciding what it mints cannot read a name only a unit supplies: a
    /// domain-wide-delegation `sub` off a per-unit var would run every unit as
    /// whichever unit minted first.
    #[test]
    fn a_credential_template_reading_a_per_unit_name_is_refused_when_the_instance_is_bound() {
        let mut jwt = spec(&[AuthKind::JwtBearer]);
        jwt.jwt_bearer.token_url = "https://idp.example/token".into();
        jwt.jwt_bearer
            .claims
            .insert("sub".into(), "{{ unit.name }}@example.com".into());
        let mut id = identity(AuthKind::JwtBearer);
        id.private_key = Some("not-a-key".into());
        let err = build(&jwt, &id).expect_err("the subject decides who the token acts as");
        assert!(matches!(err, Error::Config(_)), "{err:?}");
        assert!(
            err.to_string().contains("auth.jwt_bearer.claims.sub"),
            "{err}"
        );
        assert!(err.to_string().contains("only a unit supplies"), "{err}");

        // The endpoint of a minting mode is refused the same way.
        let mut oauth = spec(&[AuthKind::Oauth2ClientCredentials]);
        oauth.oauth2_client_credentials.token_url =
            "https://{{ unit.name }}.idp.example/token".into();
        let err = build(&oauth, &identity(AuthKind::Oauth2ClientCredentials))
            .expect_err("one mode, one endpoint");
        assert!(
            err.to_string()
                .contains("auth.oauth2_client_credentials.token_url"),
            "{err}"
        );

        // A claim over the instance's own vars is what the grammar is for.
        let mut fine = spec(&[AuthKind::JwtBearer]);
        fine.jwt_bearer.token_url = "https://idp.example/token".into();
        fine.jwt_bearer
            .claims
            .insert("sub".into(), "{{ vars.admin_email }}".into());
        let mut ctx = TemplateCtx::new();
        ctx.set(
            "vars",
            serde_json::json!({"admin_email": "admin@example.com"}),
        );
        AuthMode::build(&fine, &id, exchange(), &ctx).expect("an instance-level var binds");
    }

    /// Datadog wants `DD-API-KEY` and `DD-APPLICATION-KEY` on the same request:
    /// the api key authenticates it and the application key scopes it to a user,
    /// so neither can be dropped. Both arrive from one resolution of the
    /// instance's specs, and each is marked sensitive by the placement that
    /// wrote it.
    #[tokio::test]
    async fn two_placements_carry_two_credentials_on_one_request() {
        let mut datadog = spec(&[AuthKind::Credentials]);
        datadog.credentials = vec![
            placed_in("DD-API-KEY", "api_key"),
            placed_in("DD-APPLICATION-KEY", "application_key"),
        ];
        let mode = build(
            &datadog,
            &credentials(&[("api_key", "api-s3cr3t"), ("application_key", "app-s3cr3t")]),
        )
        .unwrap();
        assert_eq!(mode.kind(), AuthKind::Credentials);

        let mut req = request();
        authorize(&mode, &mut req, &TemplateCtx::new())
            .await
            .unwrap();

        for (name, value) in [
            ("dd-api-key", "api-s3cr3t"),
            ("dd-application-key", "app-s3cr3t"),
        ] {
            let carried = req
                .headers()
                .get(name)
                .unwrap_or_else(|| panic!("{name} is on the request"));
            assert_eq!(carried.to_str().unwrap(), value);
            assert!(carried.is_sensitive(), "{name}");
        }
        let rendered = format!("{:?}", req.headers());
        assert!(!rendered.contains("s3cr3t"), "{rendered}");
    }

    /// Tenable wants ONE header carrying both halves of the key pair, so the
    /// single-header shape does not save us: the value is composed from two
    /// secrets. Vulnerability Management spells it
    /// `Authorization: accessKey=x;secretKey=y` and Security Center
    /// `x-apikey: accesskey=x; secretkey=y`, which is the same shape with other
    /// names.
    #[tokio::test]
    async fn one_header_carries_a_value_composed_from_two_credentials() {
        for (header, value, expected) in [
            (
                "Authorization",
                "accessKey={{ credentials.access_key }};secretKey={{ credentials.secret_key }}",
                "accessKey=access-s3cr3t;secretKey=secret-s3cr3t",
            ),
            (
                "x-apikey",
                "accesskey={{ credentials.access_key }}; secretkey={{ credentials.secret_key }}",
                "accesskey=access-s3cr3t; secretkey=secret-s3cr3t",
            ),
        ] {
            let mut tenable = spec(&[AuthKind::Credentials]);
            tenable.credentials = vec![composed_in(header, value)];
            let mode = build(
                &tenable,
                &credentials(&[
                    ("access_key", "access-s3cr3t"),
                    ("secret_key", "secret-s3cr3t"),
                ]),
            )
            .unwrap();

            let mut req = request();
            authorize(&mode, &mut req, &TemplateCtx::new())
                .await
                .unwrap();

            let carried = req
                .headers()
                .get(header)
                .unwrap_or_else(|| panic!("{header} is on the request"));
            assert_eq!(carried.to_str().unwrap(), expected);
            assert!(
                carried.is_sensitive(),
                "{header}: a composed value is as secret as its parts"
            );
            for rendered in [format!("{:?}", req.headers()), format!("{mode:?}")] {
                assert!(!rendered.contains("s3cr3t"), "{header}: {rendered}");
            }
            assert!(req.url().query().is_none(), "{header}: not in the URL");
        }
    }

    /// A composed value is composed once and held, so the string carrying two
    /// secrets is built in one place however many requests go out.
    #[tokio::test]
    async fn a_composed_value_is_composed_once() {
        let composed = Composed {
            parts: vec![
                ComposedPart::Text("accessKey=".into()),
                ComposedPart::Credential(Resolved::new(Secret::new("access-s3cr3t".into()))),
                ComposedPart::Text(";secretKey=".into()),
                ComposedPart::Credential(Resolved::new(Secret::new("secret-s3cr3t".into()))),
            ],
            value: OnceCell::new(),
        };

        let first = composed.credential().await.unwrap();
        let again = composed.credential().await.unwrap();

        assert_eq!(
            first.secret.expose(),
            "accessKey=access-s3cr3t;secretKey=secret-s3cr3t"
        );
        assert!(Arc::ptr_eq(&first, &again), "composed once and held");
        let rendered = format!("{composed:?}");
        assert!(!rendered.contains("s3cr3t"), "{rendered}");
    }

    /// One name read by two placements is one resolved spec, so a credential
    /// placed on its own and also composed into a value with another is read
    /// from its store once.
    #[tokio::test]
    async fn a_credential_two_placements_read_is_resolved_once() {
        let mut both = spec(&[AuthKind::Credentials]);
        both.credentials = vec![
            placed_in("X-Api-Key", "api_key"),
            composed_in(
                "Authorization",
                "accessKey={{ credentials.api_key }};secretKey={{ credentials.secret_key }}",
            ),
        ];
        let built: Credentials<Counting> = build_credentials(
            &both,
            &credentials(&[("api_key", "api-s3cr3t"), ("secret_key", "secret-s3cr3t")]),
        )
        .unwrap();

        let mut req = request();
        built.placements.sign(&mut req).await.unwrap();

        assert_eq!(
            req.headers().get("x-api-key").unwrap().to_str().unwrap(),
            "api-s3cr3t"
        );
        assert_eq!(
            req.headers().get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "accessKey=api-s3cr3t;secretKey=secret-s3cr3t"
        );
        let reads = READS.lock().expect("the read log");
        for spec in ["api-s3cr3t", "secret-s3cr3t"] {
            assert_eq!(
                reads.iter().filter(|read| read.as_str() == spec).count(),
                1,
                "read from its store once, however many placements carry it"
            );
        }
    }

    /// A typed block binds a shipped profile without validating it, so the mode
    /// is built straight from the profile's placements: what the profile's own
    /// validation refuses is refused here too.
    #[test]
    fn building_the_mode_refuses_a_placement_the_profile_was_never_validated_for() {
        let id = credentials(&[("api_key", "api-s3cr3t"), ("secret_key", "secret-s3cr3t")]);
        for (placements, wanted) in [
            (
                vec![CredentialPlacementSpec {
                    query: Some("auth".into()),
                    value: Some("k={{ credentials.api_key }}".into()),
                    ..CredentialPlacementSpec::default()
                }],
                "place it in a header",
            ),
            (
                vec![CredentialPlacementSpec {
                    prefix: "Bearer ".into(),
                    ..composed_in("Authorization", "k={{ credentials.api_key }}")
                }],
                "write the prefix into it",
            ),
            (
                vec![
                    placed_in("X-Api-Key", "api_key"),
                    placed_in("x-api-key", "secret_key"),
                ],
                "already carries",
            ),
            (
                vec![CredentialPlacementSpec {
                    prefix: "Token ".into(),
                    query: Some("api_key".into()),
                    from: Some("api_key".into()),
                    ..CredentialPlacementSpec::default()
                }],
                "nothing writes a prefix into a query parameter",
            ),
        ] {
            let mut profile = spec(&[AuthKind::Credentials]);
            profile.credentials = placements;
            let err = build(&profile, &id).expect_err(wanted);
            assert!(err.to_string().contains(wanted), "{err}");
            assert!(!err.to_string().contains("s3cr3t"), "{err}");
        }
    }

    /// Nothing writes a prefix into a query parameter, so a profile that asks
    /// for one is refused rather than sending the bare key under a name the
    /// operator believes carries a prefixed one.
    #[test]
    fn a_query_api_key_with_a_prefix_is_refused_rather_than_dropped() {
        let mut query_spec = spec(&[AuthKind::ApiKey]);
        query_spec.api_key.query = Some("api_key".into());
        query_spec.api_key.prefix = "Token ".into();
        let err = build(&query_spec, &identity(AuthKind::ApiKey))
            .expect_err("the prefix has nowhere to be written");
        assert!(err.to_string().contains("auth.api_key.prefix"), "{err}");
        assert!(
            err.to_string()
                .contains("nothing writes a prefix into a query parameter"),
            "{err}"
        );
    }

    /// A placement reading a credential the instance does not supply is refused
    /// when the instance is bound, naming the field and the credential.
    #[test]
    fn a_placement_whose_credential_the_instance_lacks_is_refused_when_bound() {
        let mut datadog = spec(&[AuthKind::Credentials]);
        datadog.credentials = vec![
            placed_in("DD-API-KEY", "api_key"),
            placed_in("DD-APPLICATION-KEY", "application_key"),
        ];
        let err = build(&datadog, &credentials(&[("api_key", "api-s3cr3t")]))
            .expect_err("the application key has nowhere to come from");
        assert!(matches!(err, Error::Config(_)), "{err:?}");
        assert!(
            err.to_string().contains("auth.credentials[1].from"),
            "{err}"
        );
        assert!(err.to_string().contains("application_key"), "{err}");
        assert!(!err.to_string().contains("api-s3cr3t"), "{err}");

        // A composed value reading a name the instance lacks is refused the
        // same way, and the refusal names the placement rather than the value.
        let mut tenable = spec(&[AuthKind::Credentials]);
        tenable.credentials = vec![composed_in(
            "Authorization",
            "accessKey={{ credentials.access_key }};secretKey={{ credentials.secret_key }}",
        )];
        let err = build(&tenable, &credentials(&[("access_key", "access-s3cr3t")]))
            .expect_err("the secret key has nowhere to come from");
        assert!(
            err.to_string().contains("auth.credentials[0].value"),
            "{err}"
        );
        assert!(err.to_string().contains("secret_key"), "{err}");

        // A profile declaring no placement places nothing.
        let err = build(&spec(&[AuthKind::Credentials]), &credentials(&[]))
            .expect_err("a mode that places nothing authenticates nothing");
        assert!(err.to_string().contains("auth.credentials"), "{err}");
    }

    /// The executor reaches a mode through the hook, so the header a mode puts
    /// on the request has to arrive that way too.
    #[tokio::test]
    async fn signing_through_the_hook_places_the_credential() {
        let mode = build(&spec(&[AuthKind::Bearer]), &identity(AuthKind::Bearer)).unwrap();
        let ctx = TemplateCtx::new();
        let mut req = request();
        RequestSigner::sign(&mode.signer(&ctx), &mut req)
            .await
            .unwrap();
        assert_eq!(
            req.headers().get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "Bearer tok"
        );

        let missing = AuthMode::Bearer(Placed::bearer(Secret::new(
            "env:DFE_FETCHER_TEST_MISSING_SECRET_VAR".into(),
        )));
        let err = RequestSigner::sign(&missing.signer(&ctx), &mut request())
            .await
            .expect_err("the spec does not resolve");
        assert!(matches!(credential_error(err), Error::Credential(_)));
    }
}
