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
//! The modes divide three ways, and each keeps only what scalo cannot own:
//!
//! - **A resolved credential in a place the profile names.** `bearer`,
//!   `api_key` and `basic` are one of scalo's placements over a [`Resolving`]
//!   exchange, so the credential spec resolves on first use and the writing of
//!   the header, the query pair or the basic credential is scalo's.
//! - **A minted token.** `oauth2_client_credentials`, `jwt_bearer` and
//!   `gce_metadata` are scalo exchanges behind [`Cached`], which owns the
//!   caching, the renewal point and the single-flight gate: a cold mode hit by
//!   many units at once mints once. `jwt_bearer` keeps its own key reading and
//!   RS256 assertion and posts the assertion through scalo's [`TokenPost`].
//! - **A signing scheme.** `duo_hmac` and `sigv4` have their own crypto, so
//!   each is one more [`RequestSigner`] here rather than a signing dependency
//!   in scalo.
//!
//! [`AuthMode::signer`] is how the executor reaches a mode: a [`ModeSigner`]
//! is scalo's [`RequestSigner`] over one request's context, and its `sign` is
//! the dispatcher that picks the arm.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use reqsign::aws::{
    AssumeRoleCredentialProvider, Credential as AwsCredential, RequestSigner as AwsRequestSigner,
    StaticCredentialProvider,
};
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use scalo::SensitiveString;
use scalo::auth::{
    AuthError, BasicPlacement, Cached, ClientCredentials, Credential, CredentialSource, Exchange,
    HeaderPlacement, MetadataServer, Placement, QueryPlacement, TokenPost, TokenReading,
};
use scalo::http_client::{RequestSigner, SignError};
use serde_json::Value;
use tokio::sync::OnceCell;

use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::secret::ResolveSecret;

use crate::profile::template::{Template, TemplateCtx};
use crate::profile::{AuthKind, AuthSpec, InstanceAuth};
use crate::request::ExchangeClient;

/// The form field of the JWT-bearer grant (RFC 7523).
const JWT_BEARER_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

/// How long a resolved credential spec is held before the cache reads it
/// again, longer than a process runs. The spec resolves once inside its
/// [`Secret`] and stays resolved there, so a renewal reads that value and
/// reaches no vault.
// `Duration::from_days` is not a const fn on stable, so the seconds are spelled.
#[allow(clippy::duration_suboptimal_units)]
const RESOLVED_HOLD: Duration = Duration::from_secs(29 * 24 * 60 * 60);

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
pub type Secret = dfe_fetcher_core::secret::Secret<ScaloSecrets>;

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

/// A credential spec as the exchange a cache holds.
///
/// The spec resolves once inside its [`Secret`] and is held there for the life
/// of the process, so this is I/O the first time and a read after that.
#[derive(Debug)]
struct Resolving(Secret);

impl Exchange for Resolving {
    async fn acquire(&self) -> Acquired<Credential> {
        let secret = self.0.value().await.map_err(unavailable)?;
        let now = Instant::now();
        Ok(Credential::new(
            SensitiveString::from(secret),
            now.checked_add(RESOLVED_HOLD).unwrap_or(now),
        ))
    }
}

/// A resolved credential spec as the source a placement reads per request: a
/// hit is one atomic load and a pointer clone.
type StaticSource = Arc<Cached<Resolving>>;

/// A resolved credential and where the profile puts it.
///
/// The source is held beside the placement -- the same `Arc` -- so a probe can
/// resolve the spec without sending a request.
#[derive(Debug)]
pub struct Placed {
    source: StaticSource,
    placement: Placement<StaticSource>,
}

impl Placed {
    /// `Authorization: Bearer <token>`.
    #[must_use]
    pub fn bearer(token: Secret) -> Self {
        let source = held(token);
        Self {
            placement: Placement::Header(HeaderPlacement::bearer(Arc::clone(&source))),
            source,
        }
    }

    /// The key in `name`, after `prefix`.
    fn header(key: Secret, name: HeaderName, prefix: &str) -> Self {
        let source = held(key);
        Self {
            placement: Placement::Header(HeaderPlacement::new(name, prefix, Arc::clone(&source))),
            source,
        }
    }

    /// The key as the query parameter `name`.
    fn query(key: Secret, name: &str) -> Self {
        let source = held(key);
        Self {
            placement: Placement::Query(QueryPlacement::new(name, Arc::clone(&source))),
            source,
        }
    }

    /// The password of HTTP basic auth under `username`.
    fn basic(password: Secret, username: &str) -> Self {
        let source = held(password);
        Self {
            placement: Placement::Basic(BasicPlacement::new(username, Arc::clone(&source))),
            source,
        }
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

/// A spec behind a cache that holds no failure: a spec that does not resolve is
/// tried again by the next request rather than answered from the last refusal,
/// because the resolver is local and its answer costs nothing.
fn held(secret: Secret) -> StaticSource {
    Arc::new(Cached::new(Resolving(secret)).with_failure_backoff(Duration::ZERO))
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
/// over every attempt it may make, so the cache's renewal gate is not released
/// while an attempt the client itself scheduled is still in flight. scalo's own
/// exchanges bound themselves the same way.
fn exchange_deadline(http: &ExchangeClient) -> Duration {
    let config = http.config();
    Duration::from_secs(config.timeout_secs).saturating_mul(config.max_retries.saturating_add(1))
        + Duration::from_millis(config.max_retry_interval_ms).saturating_mul(config.max_retries)
}

/// A minting mode's cache, holding no failure: a refused or unreachable
/// endpoint is posted to again by the next request, so a rotated secret is
/// picked up on the next tick rather than after a backoff. The callers that
/// waited on one in-flight exchange still share its failure.
fn cached<E: Exchange>(exchange: E) -> Cached<E> {
    Cached::new(exchange).with_failure_backoff(Duration::ZERO)
}

/// An OAuth2 client-credentials exchange behind scalo's cache.
///
/// The exchange is built on first use, because the token endpoint is a template
/// and renders against a unit's context. One mode holds one cache, so the
/// endpoint the first unit renders is the one this mode exchanges at.
pub struct OAuth2Client {
    token_url: Template,
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

/// Hand-written because the exchange client has no `Debug`.
impl fmt::Debug for OAuth2Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuth2Client")
            .field("token_url", &self.token_url)
            .field("client_id", &self.client_id)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl OAuth2Client {
    /// The cached credential, exchanged now when none is held or the held one
    /// has reached its renewal point.
    async fn credential(&self, ctx: &TemplateCtx) -> Acquired<Arc<Credential>> {
        self.source
            .get_or_try_init(|| self.exchange(ctx))
            .await?
            .credential()
            .await
    }

    /// The exchange, once the context has rendered the token endpoint and the
    /// client secret has resolved.
    async fn exchange(&self, ctx: &TemplateCtx) -> Acquired<Cached<ClientCredentials>> {
        let url = self.token_url.render(ctx).map_err(unavailable)?;
        let secret = SensitiveString::from(self.client_secret.value().await.map_err(unavailable)?);
        let mut exchange = ClientCredentials::new(&self.http, url, &self.client_id, secret)?
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
struct JwtAssertion {
    token_url: Template,
    claims: Vec<(String, Template)>,
    ttl: Duration,
    source: JwtKeySource,
    key: OnceCell<JwtKey>,
    http: Arc<ExchangeClient>,
}

/// Hand-written: the signing key has no `Debug`, and neither has the exchange
/// client.
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
    async fn mint(&self, ctx: &TemplateCtx) -> Result<(String, String)> {
        let key = self.key().await?;
        let mut ctx = ctx.clone();
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
struct JwtExchange {
    assertion: Arc<JwtAssertion>,
    reading: TokenReading,
    /// The context the claims and the endpoint render against, taken at the
    /// first exchange. One mode holds one cache, so this is the context of
    /// whichever of its units asked first.
    ctx: TemplateCtx,
}

/// Hand-written: the context carries whatever a profile exposed to its
/// templates, and a render of the exchange has no business printing it.
impl fmt::Debug for JwtExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JwtExchange")
            .field("assertion", &self.assertion)
            .finish_non_exhaustive()
    }
}

impl Exchange for JwtExchange {
    async fn acquire(&self) -> Acquired<Credential> {
        let (url, assertion) = self.assertion.mint(&self.ctx).await.map_err(unavailable)?;
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
    assertion: Arc<JwtAssertion>,
    reading: TokenReading,
    exposes: bool,
    source: OnceCell<Cached<JwtExchange>>,
}

impl JwtBearer {
    /// The cached token, exchanged now when none is held or the held one has
    /// reached its renewal point.
    async fn credential(&self, ctx: &TemplateCtx) -> Acquired<Arc<Credential>> {
        self.source
            .get_or_init(|| async {
                cached(JwtExchange {
                    assertion: Arc::clone(&self.assertion),
                    reading: self.reading.clone(),
                    ctx: ctx.clone(),
                })
            })
            .await
            .credential()
            .await
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
pub struct GceMetadata {
    url: Template,
    reading: TokenReading,
    http: Arc<ExchangeClient>,
    source: OnceCell<Cached<MetadataServer>>,
}

/// Hand-written because the exchange client has no `Debug`.
impl fmt::Debug for GceMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GceMetadata")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl GceMetadata {
    /// The cached token, fetched now when none is held or the held one has
    /// reached its renewal point.
    async fn credential(&self, ctx: &TemplateCtx) -> Acquired<Arc<Credential>> {
        self.source
            .get_or_try_init(|| std::future::ready(self.exchange(ctx)))
            .await?
            .credential()
            .await
    }

    /// The exchange, once the context has rendered the metadata URL.
    fn exchange(&self, ctx: &TemplateCtx) -> Acquired<Cached<MetadataServer>> {
        let url = self.url.render(ctx).map_err(unavailable)?;
        Ok(cached(
            MetadataServer::new(&self.http, url)?
                .with_header(METADATA_FLAVOR, "Google")
                .with_reading(self.reading.clone()),
        ))
    }
}

/// Duo Admin API request signing.
///
/// Every request is signed over a canonical string of its `Date` header,
/// method, lowercase host, path and RFC 3986-encoded query pairs sorted by
/// name, HMAC-SHA1 with the secret key, and carried as a Basic credential of
/// `integration_key:hex(signature)` with that same `Date`. Nothing is cached:
/// the date is part of the signature, so each request is signed afresh.
#[derive(Debug)]
pub struct DuoHmac {
    integration_key: String,
    secret_key: Secret,
}

/// The characters Duo's canonical query leaves bare: RFC 3986 unreserved.
const DUO_ENCODE: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

impl DuoHmac {
    /// The canonical string of a built request at `date`: the host is the
    /// URL's, lowercase, with the port when one is explicit (as the `Host`
    /// header carries it), and the query is the decoded pairs sorted by name
    /// and re-encoded, so it matches whatever encoding the URL used.
    fn canonical(request: &reqwest::Request, date: &str) -> String {
        let url = request.url();
        let mut host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        if let Some(port) = url.port() {
            host = format!("{host}:{port}");
        }
        let mut pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        pairs.sort();
        let query: Vec<String> = pairs
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    percent_encoding::utf8_percent_encode(k, DUO_ENCODE),
                    percent_encoding::utf8_percent_encode(v, DUO_ENCODE)
                )
            })
            .collect();
        format!(
            "{date}\n{}\n{host}\n{}\n{}",
            request.method().as_str().to_ascii_uppercase(),
            url.path(),
            query.join("&")
        )
    }

    /// Sign the request and carry the signature as the Basic credential,
    /// beside the `Date` the signature covers.
    async fn apply(&self, request: &mut reqwest::Request) -> Result<()> {
        use hmac::{Hmac, Mac};

        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S -0000")
            .to_string();
        let canonical = Self::canonical(request, &date);
        let mut mac = Hmac::<sha1::Sha1>::new_from_slice(self.secret_key.value().await?.as_bytes())
            .map_err(|e| Error::Credential(format!("duo_hmac secret key: {e}")))?;
        mac.update(canonical.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        let credential = base64::engine::general_purpose::STANDARD
            .encode(format!("{}:{signature}", self.integration_key));
        set_header(request, AUTHORIZATION, &format!("Basic {credential}"))?;
        request.headers_mut().insert(
            reqwest::header::DATE,
            HeaderValue::from_str(&date)
                .map_err(|e| Error::Credential(format!("duo_hmac date header: {e}")))?,
        );
        Ok(())
    }
}

impl RequestSigner for DuoHmac {
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
#[derive(Debug, Clone, Copy)]
struct ScopedSigV4<'a> {
    keys: &'a SigV4,
    ctx: &'a TemplateCtx,
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
    /// Duo Admin API signing.
    DuoHmac(DuoHmac),
    /// OAuth2 JWT-bearer grant.
    JwtBearer(JwtBearer),
    /// The GCE metadata server's token.
    GceMetadata(GceMetadata),
    /// AWS SigV4 signing.
    SigV4(SigV4),
}

impl AuthMode {
    /// Build the instance's mode from the profile's shape and its identity.
    /// `http` is the client a credential exchange posts through, shared by
    /// every instance.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the instance's mode is not accepted by the
    /// profile or an identity field for it is missing.
    pub fn build(
        spec: &AuthSpec,
        identity: &InstanceAuth,
        http: Arc<ExchangeClient>,
    ) -> Result<Self> {
        Self::build_scoped(spec, identity, http, None)
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
                    (None, Some(name)) => Placed::query(key, name),
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
            )),
            AuthKind::Oauth2ClientCredentials => {
                let oauth = &spec.oauth2_client_credentials;
                let scope = scope
                    .map(str::to_owned)
                    .or_else(|| identity.scope.clone())
                    .unwrap_or_else(|| oauth.scope.clone());
                AuthMode::OAuth2ClientCredentials(OAuth2Client {
                    token_url: Template::compile(&oauth.token_url)?,
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
            AuthKind::DuoHmac => AuthMode::DuoHmac(DuoHmac {
                integration_key: identity.integration_key.clone().ok_or_else(|| {
                    Error::Config("auth mode `duo_hmac` needs `integration_key`".into())
                })?,
                secret_key: need("secret_key", identity.secret_key.as_ref())?,
            }),
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
                    let template = match (name.as_str(), scope) {
                        ("scope", Some(scope)) => Template::compile(scope)?,
                        _ => Template::compile(value)?,
                    };
                    claims.push((name.clone(), template));
                }
                if let Some(scope) = scope
                    && !jwt.claims.contains_key("scope")
                {
                    claims.push(("scope".to_owned(), Template::compile(scope)?));
                }
                AuthMode::JwtBearer(JwtBearer {
                    assertion: Arc::new(JwtAssertion {
                        token_url: Template::compile(&jwt.token_url)?,
                        claims,
                        ttl: Duration::from_secs(jwt.ttl_secs),
                        source,
                        key: OnceCell::new(),
                        http,
                    }),
                    reading: token_reading(
                        jwt.expires_in_fallback_secs,
                        jwt.early_refresh_secs,
                        &jwt.expose,
                    ),
                    exposes: !jwt.expose.is_empty(),
                    source: OnceCell::new(),
                })
            }
            AuthKind::GceMetadata => {
                let gce = &spec.gce_metadata;
                AuthMode::GceMetadata(GceMetadata {
                    url: Template::compile(&gce.url)?,
                    reading: token_reading(
                        gce.expires_in_fallback_secs,
                        gce.early_refresh_secs,
                        &[],
                    ),
                    http,
                    source: OnceCell::new(),
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
            AuthMode::DuoHmac(_) => AuthKind::DuoHmac,
            AuthMode::JwtBearer(_) => AuthKind::JwtBearer,
            AuthMode::GceMetadata(_) => AuthKind::GceMetadata,
            AuthMode::SigV4(_) => AuthKind::SigV4,
        }
    }

    /// The mode as a signer over one request's context.
    ///
    /// The context is per request and scalo's hook takes the request alone, so
    /// it is carried here rather than held on the mode, which is per instance.
    #[must_use]
    pub fn signer<'a>(&'a self, ctx: &'a TemplateCtx) -> ModeSigner<'a> {
        ModeSigner { mode: self, ctx }
    }

    /// Put the credential on a built request. `ctx` renders the token URL and
    /// the signing scope.
    ///
    /// The framework-typed front door to [`ModeSigner`], which is the
    /// dispatcher.
    ///
    /// # Errors
    ///
    /// Returns the credential or token-exchange error.
    pub async fn authorize(&self, request: &mut reqwest::Request, ctx: &TemplateCtx) -> Result<()> {
        RequestSigner::sign(&self.signer(ctx), request)
            .await
            .map_err(credential_error)
    }

    /// The cached-or-fresh credential of a token-minting mode.
    async fn minted(&self, ctx: &TemplateCtx) -> Acquired<Arc<Credential>> {
        match self {
            AuthMode::OAuth2ClientCredentials(client) => client.credential(ctx).await,
            AuthMode::JwtBearer(client) => client.credential(ctx).await,
            AuthMode::GceMetadata(client) => client.credential(ctx).await,
            AuthMode::None
            | AuthMode::Bearer(_)
            | AuthMode::ApiKey(_)
            | AuthMode::Basic(_)
            | AuthMode::DuoHmac(_)
            | AuthMode::SigV4(_) => Err(AuthError::Unavailable {
                reason: format!("auth mode `{}` mints no token", self.kind().as_str()),
            }),
        }
    }

    /// The token-response fields the mode exposes to templates as `auth.*`,
    /// minting the token first when none is cached; `None` for a mode that
    /// exposes nothing, so a request render never mints for nothing.
    ///
    /// # Errors
    ///
    /// Returns the token-exchange error.
    pub async fn exposed(&self, ctx: &TemplateCtx) -> Result<Option<Value>> {
        match self {
            AuthMode::OAuth2ClientCredentials(client) if client.exposes => self.extra(ctx).await,
            AuthMode::JwtBearer(client) if client.exposes => self.extra(ctx).await,
            _ => Ok(None),
        }
    }

    /// What the exchange returned beside the token, `None` when the response
    /// carried none of the fields the profile named.
    async fn extra(&self, ctx: &TemplateCtx) -> Result<Option<Value>> {
        let credential = self.minted(ctx).await.map_err(acquisition_error)?;
        Ok(credential.extra.as_deref().cloned())
    }

    /// Resolve the credential (and mint a token) without sending a data request.
    ///
    /// # Errors
    ///
    /// Returns the credential or token-exchange error.
    pub async fn probe(&self, ctx: &TemplateCtx) -> Result<()> {
        match self {
            AuthMode::None => Ok(()),
            AuthMode::Bearer(placed) | AuthMode::ApiKey(placed) | AuthMode::Basic(placed) => {
                placed.resolve().await
            }
            AuthMode::OAuth2ClientCredentials(_)
            | AuthMode::JwtBearer(_)
            | AuthMode::GceMetadata(_) => self
                .minted(ctx)
                .await
                .map(|_| ())
                .map_err(acquisition_error),
            AuthMode::DuoHmac(signer) => signer.secret_key.value().await.map(|_| ()),
            AuthMode::SigV4(signer) => signer.keys().await.map(|_| ()),
        }
    }
}

/// One instance's mode as scalo's signing hook over one request's context.
///
/// Borrowed rather than owned: the mode lives on the shape for the life of the
/// instance and the context for the life of the tick, so a signer is made per
/// request and costs two pointers.
#[derive(Debug, Clone, Copy)]
pub struct ModeSigner<'a> {
    mode: &'a AuthMode,
    ctx: &'a TemplateCtx,
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
                HeaderPlacement::bearer(Minting {
                    mode: self.mode,
                    ctx: self.ctx,
                })
                .sign(request)
                .await
            }
            AuthMode::DuoHmac(signer) => signer.sign(request).await,
            AuthMode::SigV4(signer) => signer.scoped(self.ctx).sign(request).await,
        }
    }
}

/// A token-minting mode as the source its placement reads.
///
/// The context renders the token endpoint and the assertion's claims and is per
/// request, so it is carried here rather than on the mode.
#[derive(Debug, Clone, Copy)]
struct Minting<'a> {
    mode: &'a AuthMode,
    ctx: &'a TemplateCtx,
}

impl CredentialSource for Minting<'_> {
    async fn credential(&self) -> Acquired<Arc<Credential>> {
        self.mode.minted(self.ctx).await
    }
}

/// A signing scheme's own failure on its way into the signing hook.
///
/// The error travels whole as the cause so [`credential_error`] hands back the
/// same one the scheme raised, rather than a status flattened into a string.
/// Only a failure to reach the credential endpoint is worth signing again; a
/// refusal, a spec that does not resolve and a key that will not parse are the
/// same answer next attempt.
fn signing_failure(error: Error) -> SignError {
    let transient = matches!(error, Error::Source(_));
    let failure = SignError::with_cause(error.to_string(), error);
    if transient {
        failure.retryable()
    } else {
        failure
    }
}

/// The framework error behind a signing failure.
///
/// A signing scheme handed its error through as the cause, so that error comes
/// back unchanged and keeps the status the executor counts and the fixture
/// asserts. A credential source reports [`AuthError`] instead: a refusal keeps
/// its status as an API error, an endpoint that could not be reached is a
/// source failure, and a response that is not a credential -- or one the
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

    use super::*;

    fn exchange() -> Arc<ExchangeClient> {
        crate::request::exchange_client().unwrap()
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

    fn request() -> reqwest::Request {
        reqwest::Request::new(
            reqwest::Method::GET,
            "https://api.example/x".parse().unwrap(),
        )
    }

    #[tokio::test]
    async fn bearer_sets_the_authorization_header_and_marks_it_sensitive() {
        let mode = AuthMode::build(
            &spec(&[AuthKind::Bearer]),
            &identity(AuthKind::Bearer),
            exchange(),
        )
        .unwrap();
        let mut req = request();
        mode.authorize(&mut req, &TemplateCtx::new()).await.unwrap();
        let value = req.headers().get(AUTHORIZATION).unwrap();
        assert_eq!(value.to_str().unwrap(), "Bearer tok");
        assert!(value.is_sensitive());
    }

    #[tokio::test]
    async fn api_key_goes_to_a_prefixed_header_or_a_query_parameter() {
        let mut header_spec = spec(&[AuthKind::ApiKey]);
        header_spec.api_key.header = Some("Authorization".into());
        header_spec.api_key.prefix = "SSWS ".into();
        let mode = AuthMode::build(&header_spec, &identity(AuthKind::ApiKey), exchange()).unwrap();
        let mut req = request();
        mode.authorize(&mut req, &TemplateCtx::new()).await.unwrap();
        assert_eq!(
            req.headers().get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "SSWS k"
        );

        let mut query_spec = spec(&[AuthKind::ApiKey]);
        query_spec.api_key.query = Some("api_key".into());
        let mode = AuthMode::build(&query_spec, &identity(AuthKind::ApiKey), exchange()).unwrap();
        let mut req = request();
        mode.authorize(&mut req, &TemplateCtx::new()).await.unwrap();
        assert_eq!(req.url().query(), Some("api_key=k"));

        // A header name the profile got wrong is refused when the instance is
        // bound, not on the first request it would have gone out on.
        let mut bad = spec(&[AuthKind::ApiKey]);
        bad.api_key.header = Some("X Api Key".into());
        let err = AuthMode::build(&bad, &identity(AuthKind::ApiKey), exchange()).unwrap_err();
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
        let mode = AuthMode::build(&query_spec, &id, exchange()).unwrap();
        let mut req = reqwest::Request::new(
            reqwest::Method::GET,
            "https://api.example/x?page=2".parse().unwrap(),
        );
        mode.authorize(&mut req, &TemplateCtx::new()).await.unwrap();
        assert_eq!(req.url().query(), Some("page=2&api_key=a%26b%3Dc"));
        assert_eq!(req.url().query_pairs().count(), 2);
    }

    #[tokio::test]
    async fn basic_encodes_user_and_password_and_marks_it_sensitive() {
        let mode = AuthMode::build(
            &spec(&[AuthKind::Basic]),
            &identity(AuthKind::Basic),
            exchange(),
        )
        .unwrap();
        let mut req = request();
        mode.authorize(&mut req, &TemplateCtx::new()).await.unwrap();
        let value = req.headers().get(AUTHORIZATION).unwrap();
        assert_eq!(value.to_str().unwrap(), "Basic dTpw");
        assert!(value.is_sensitive());
    }

    /// The colon separates the two halves of a basic credential and RFC 7617
    /// gives the username no way to escape one, so a username carrying one
    /// would authenticate as something other than what was configured.
    #[tokio::test]
    async fn a_colon_in_the_basic_username_is_refused_rather_than_moving_the_separator() {
        let mut id = identity(AuthKind::Basic);
        id.username = Some("account:1234".to_owned());
        id.password = Some("s3cr3t-do-not-print".into());
        let mode = AuthMode::build(&spec(&[AuthKind::Basic]), &id, exchange()).unwrap();
        let mut req = request();
        let err = mode
            .authorize(&mut req, &TemplateCtx::new())
            .await
            .expect_err("the colon would move the field separator");
        assert!(err.to_string().contains("colon"), "{err}");
        assert!(!err.to_string().contains("s3cr3t-do-not-print"), "{err}");
        assert!(req.headers().get(AUTHORIZATION).is_none());
    }

    #[test]
    fn a_mode_the_profile_does_not_accept_or_a_missing_field_is_a_config_error() {
        let err = AuthMode::build(
            &spec(&[AuthKind::Bearer]),
            &identity(AuthKind::Basic),
            exchange(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
        let mut missing = identity(AuthKind::Bearer);
        missing.token = None;
        let err = AuthMode::build(&spec(&[AuthKind::Bearer]), &missing, exchange()).unwrap_err();
        assert!(err.to_string().contains("token"), "{err}");
    }

    /// A token endpoint over plaintext hands the client secret to anyone on the
    /// path, because the secret is a field of the form that is posted. Loopback
    /// is allowed so a fixture needs no certificate.
    #[tokio::test]
    async fn a_plaintext_token_endpoint_is_refused_and_a_loopback_one_is_not() {
        let mut oauth_spec = spec(&[AuthKind::Oauth2ClientCredentials]);
        oauth_spec.oauth2_client_credentials.token_url = "http://idp.example/token".into();
        let mode = AuthMode::build(
            &oauth_spec,
            &identity(AuthKind::Oauth2ClientCredentials),
            exchange(),
        )
        .unwrap();
        let err = mode
            .probe(&TemplateCtx::new())
            .await
            .expect_err("the client secret would go out in the clear");
        assert!(err.to_string().contains("https"), "{err}");
        assert!(matches!(err, Error::Credential(_)), "{err:?}");

        // A loopback endpoint gets as far as trying to reach it.
        oauth_spec.oauth2_client_credentials.token_url =
            "http://127.0.0.1:1/token".parse().unwrap();
        let mode = AuthMode::build(
            &oauth_spec,
            &identity(AuthKind::Oauth2ClientCredentials),
            exchange(),
        )
        .unwrap();
        let err = mode
            .probe(&TemplateCtx::new())
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
            AuthKind::DuoHmac,
            AuthKind::JwtBearer,
            AuthKind::GceMetadata,
            AuthKind::SigV4,
        ]);
        every.api_key.header = Some("X-Api-Key".into());
        every.oauth2_client_credentials.token_url = "https://idp.example/token".into();
        every.gce_metadata.url = "http://metadata.example/token".into();
        for kind in every.accepts.clone() {
            let mut id = InstanceAuth {
                mode: kind,
                token: Some("s3cr3t-do-not-print".into()),
                key: Some("s3cr3t-do-not-print".into()),
                username: Some("account".into()),
                password: Some("s3cr3t-do-not-print".into()),
                client_id: Some("client-42".into()),
                client_secret: Some("s3cr3t-do-not-print".into()),
                integration_key: Some("DI".into()),
                secret_key: Some("s3cr3t-do-not-print".into()),
                ..InstanceAuth::default()
            };
            id.private_key = Some("s3cr3t-do-not-print".into());
            id.access_key_id = Some("AKIA".into());
            id.secret_access_key = Some("s3cr3t-do-not-print".into());
            let mode = AuthMode::build(&every, &id, exchange())
                .unwrap_or_else(|e| panic!("{}: {e}", kind.as_str()));
            let rendered = format!("{mode:?}");
            assert!(
                !rendered.contains("s3cr3t-do-not-print"),
                "{}: {rendered}",
                kind.as_str()
            );
        }
    }

    /// Duo's scheme, checked against a signature computed by hand from the
    /// canonical string the spec defines: `date \n METHOD \n host \n path \n
    /// sorted RFC 3986 query`, HMAC-SHA1 with the secret key, hex, carried as
    /// `Basic base64(ikey:hex)` with the same `Date` header.
    #[tokio::test]
    async fn duo_hmac_signs_the_built_request_the_way_the_admin_api_verifies_it() {
        use hmac::{Hmac, Mac};

        let mode = AuthMode::build(
            &spec(&[AuthKind::DuoHmac]),
            &InstanceAuth {
                mode: AuthKind::DuoHmac,
                integration_key: Some("DIWJ8X6AEYOR5OMC6TQ1".into()),
                secret_key: Some("Zh5eGmUq9zpfQnyUIu5OL9iWoMMv5ZNmk3zLJ4Ep".into()),
                ..InstanceAuth::default()
            },
            exchange(),
        )
        .unwrap();
        assert_eq!(mode.kind(), AuthKind::DuoHmac);
        let mut req = reqwest::Request::new(
            reqwest::Method::GET,
            "https://API-Deadbeef.duosecurity.com/admin/v2/logs/authentication?mintime=1&limit=2&next_offset=1532951895000,af0b/a?b"
                .parse()
                .unwrap(),
        );
        mode.authorize(&mut req, &TemplateCtx::new()).await.unwrap();

        let date = req
            .headers()
            .get("date")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert!(date.ends_with(" -0000"), "RFC 2822 with -0000: {date}");
        assert!(chrono::DateTime::parse_from_rfc2822(&date).is_ok());
        let canonical = format!(
            "{date}\nGET\napi-deadbeef.duosecurity.com\n/admin/v2/logs/authentication\nlimit=2&mintime=1&next_offset=1532951895000%2Caf0b%2Fa%3Fb"
        );
        let mut mac =
            Hmac::<sha1::Sha1>::new_from_slice(b"Zh5eGmUq9zpfQnyUIu5OL9iWoMMv5ZNmk3zLJ4Ep")
                .unwrap();
        mac.update(canonical.as_bytes());
        let expected = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!(
                "DIWJ8X6AEYOR5OMC6TQ1:{}",
                hex::encode(mac.finalize().into_bytes())
            ))
        );
        let auth = req.headers().get(AUTHORIZATION).unwrap();
        assert_eq!(auth.to_str().unwrap(), expected);
        assert!(auth.is_sensitive());

        // A non-default port is part of the host, as it is in the Host header.
        let mut local = reqwest::Request::new(
            reqwest::Method::GET,
            "http://127.0.0.1:8081/admin/v1/check".parse().unwrap(),
        );
        mode.authorize(&mut local, &TemplateCtx::new())
            .await
            .unwrap();
        let date = local.headers().get("date").unwrap().to_str().unwrap();
        let canonical = format!("{date}\nGET\n127.0.0.1:8081\n/admin/v1/check\n");
        let mut mac =
            Hmac::<sha1::Sha1>::new_from_slice(b"Zh5eGmUq9zpfQnyUIu5OL9iWoMMv5ZNmk3zLJ4Ep")
                .unwrap();
        mac.update(canonical.as_bytes());
        assert!(
            local
                .headers()
                .get(AUTHORIZATION)
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with(&base64::engine::general_purpose::STANDARD.encode(format!(
                    "DIWJ8X6AEYOR5OMC6TQ1:{}",
                    hex::encode(mac.finalize().into_bytes())
                )))
        );

        let missing = AuthMode::build(
            &spec(&[AuthKind::DuoHmac]),
            &InstanceAuth {
                mode: AuthKind::DuoHmac,
                integration_key: Some("DI".into()),
                ..InstanceAuth::default()
            },
            exchange(),
        )
        .unwrap_err();
        assert!(missing.to_string().contains("secret_key"), "{missing}");
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
        let mode = AuthMode::build(
            &sigv4_spec,
            &InstanceAuth {
                mode: AuthKind::SigV4,
                access_key_id: Some("AKIAIOSFODNN7EXAMPLE".into()),
                secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
                ..InstanceAuth::default()
            },
            exchange(),
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
        mode.authorize(&mut req, &ctx).await.unwrap();

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
        mode.authorize(&mut probe, &ctx).await.unwrap();
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
        let document = AuthMode::build(
            &sigv4_spec,
            &InstanceAuth {
                mode: AuthKind::SigV4,
                credentials_json: Some(
                    r#"{"AccessKeyId": "AKIAFROMJSONEXAMPLE0", "SecretAccessKey": "s"}"#.into(),
                ),
                ..InstanceAuth::default()
            },
            exchange(),
        )
        .unwrap();
        let mut req = reqwest::Request::new(
            reqwest::Method::GET,
            "https://sts.us-east-1.amazonaws.com/".parse().unwrap(),
        );
        document.authorize(&mut req, &ctx).await.unwrap();
        assert!(
            req.headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIAFROMJSONEXAMPLE0/")
        );
        let half = AuthMode::build(
            &sigv4_spec,
            &InstanceAuth {
                mode: AuthKind::SigV4,
                access_key_id: Some("AKIA".into()),
                ..InstanceAuth::default()
            },
            exchange(),
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
        let mode = AuthMode::build(
            &sigv4_spec,
            &InstanceAuth {
                mode: AuthKind::SigV4,
                access_key_id: Some("AKIAIOSFODNN7EXAMPLE".into()),
                secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
                assume_role_arn: Some("arn:aws:iam::123456789012:role/dfe-reader".into()),
                ..InstanceAuth::default()
            },
            exchange(),
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
            mode.authorize(&mut req, &ctx).await.unwrap();
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
            let err = mode
                .probe(&TemplateCtx::new())
                .await
                .expect_err("the variable is not set");
            assert!(
                err.to_string()
                    .contains("DFE_FETCHER_TEST_MISSING_SECRET_VAR"),
                "the resolver's own answer, not a held one: {err}"
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

    /// Only a credential endpoint that could not be reached is worth signing
    /// again; every other failure answers the same way next attempt.
    #[test]
    fn only_an_unreachable_endpoint_is_marked_worth_signing_again() {
        assert!(
            signing_failure(Error::Source("token exchange at ...: connect".to_owned()))
                .is_retryable()
        );
        for error in [
            Error::Credential("spec did not resolve".to_owned()),
            Error::Api {
                status: 401,
                text: String::new(),
                throttled: false,
            },
        ] {
            assert!(!signing_failure(error).is_retryable());
        }
    }

    /// A credential source of scalo's own reports `AuthError`, which maps onto
    /// the framework error by what it says rather than by its text.
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
    }

    /// A template or a key the fetcher could not turn into a credential is the
    /// fetcher's own message, not its error prefix stacked under scalo's.
    #[test]
    fn a_failure_the_fetcher_raised_keeps_its_own_words() {
        let reason = "jwt_bearer token_url rendered empty";
        let error = acquisition_error(unavailable(Error::Config(reason.to_owned())));
        assert!(matches!(error, Error::Credential(_)), "{error:?}");
        assert!(error.to_string().contains(reason), "{error}");
        assert!(
            !error.to_string().contains("configuration error"),
            "one prefix, not three: {error}"
        );
    }

    /// One acquisition is bounded by the exchange client's own attempts, so the
    /// cache's renewal gate is never released while an attempt the client
    /// scheduled is still in flight. The client grants no retries, so the bound
    /// is one request's timeout.
    #[test]
    fn an_acquisition_is_bounded_by_the_exchange_clients_attempts() {
        let http = exchange();
        assert_eq!(http.config().max_retries, 0);
        assert_eq!(
            exchange_deadline(&http),
            Duration::from_secs(http.config().timeout_secs)
        );
    }

    /// The executor reaches a mode through the hook, so the header a mode puts
    /// on the request has to arrive that way too.
    #[tokio::test]
    async fn signing_through_the_hook_places_the_credential() {
        let mode = AuthMode::build(
            &spec(&[AuthKind::Bearer]),
            &identity(AuthKind::Bearer),
            exchange(),
        )
        .unwrap();
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
