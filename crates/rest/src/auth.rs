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
//! the same identity. It is applied to the BUILT request, which is what lets a
//! signing mode see the final method, host, path and body. Credential specs
//! resolve on first use and stay resolved for the life of the process; a
//! minted token (OAuth2 client credentials, a JWT-bearer exchange, the GCE
//! metadata server) is cached until shortly before it expires and refreshed
//! on demand.

use std::fmt;
use std::future::Future;
use std::time::Duration;

use base64::Engine as _;
use reqsign::aws::{
    AssumeRoleCredentialProvider, Credential, RequestSigner, StaticCredentialProvider,
};
use reqwest::header::{AUTHORIZATION, HeaderValue};
use scalo::SensitiveString;
use serde_json::Value;
use tokio::sync::{OnceCell, RwLock};
use tokio::time::Instant;

use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::secret::ResolveSecret;

use crate::profile::template::{Template, TemplateCtx};
use crate::profile::{AuthKind, AuthSpec, InstanceAuth};

/// The form field of the JWT-bearer grant (RFC 7523).
const JWT_BEARER_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

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

/// Where an API key is placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiKeyPlace {
    /// A header, with a prefix such as `SSWS `.
    Header {
        /// Header name.
        name: String,
        /// Text before the key.
        prefix: String,
    },
    /// A query parameter.
    Query {
        /// Parameter name.
        name: String,
    },
}

#[derive(Debug, Clone)]
struct CachedToken {
    token: SensitiveString,
    refresh_at: Instant,
    /// The token response's fields the mode exposes to templates as
    /// `auth.*`, an object.
    exposed: Value,
}

/// A minted token held until shortly before it expires; every token-minting
/// mode caches through this one type.
#[derive(Debug, Default)]
struct TokenCache(RwLock<Option<CachedToken>>);

impl TokenCache {
    /// The cached token, or the one `mint` produces when none is held or the
    /// held one is due for refresh.
    async fn get_or_mint<F, Fut>(&self, mint: F) -> Result<CachedToken>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<CachedToken>>,
    {
        if let Some(cached) = self.0.read().await.as_ref()
            && Instant::now() < cached.refresh_at
        {
            return Ok(cached.clone());
        }
        let mut slot = self.0.write().await;
        // Another task may have refreshed while this one waited for the write lock.
        if let Some(cached) = slot.as_ref()
            && Instant::now() < cached.refresh_at
        {
            return Ok(cached.clone());
        }
        let fresh = mint().await?;
        *slot = Some(fresh.clone());
        Ok(fresh)
    }
}

/// The token in a token endpoint's 2xx response, with its refresh point
/// from `expires_in` (a number or a numeric string) or the fallback, and
/// the top-level fields named in `expose` (those present) as the exposed
/// object.
async fn cached_token(
    response: reqwest::Response,
    url: &str,
    expires_in_fallback: Duration,
    early_refresh: Duration,
    expose: &[String],
) -> Result<CachedToken> {
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(Error::Api {
            status: status.as_u16(),
            text: format!(
                "token exchange refused: {}",
                text.chars().take(512).collect::<String>()
            ),
        });
    }
    let body: Value = response
        .json()
        .await
        .map_err(|e| Error::Credential(format!("token response from {url} is not a token: {e}")))?;
    let access_token = body
        .get("access_token")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::Credential(format!(
                "token response from {url} carries no `access_token`"
            ))
        })?;
    let expires_in = body
        .get("expires_in")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .map_or(expires_in_fallback, Duration::from_secs);
    let exposed = expose
        .iter()
        .filter_map(|name| body.get(name).map(|v| (name.clone(), v.clone())))
        .collect();
    Ok(CachedToken {
        token: SensitiveString::from(access_token),
        refresh_at: Instant::now() + expires_in.saturating_sub(early_refresh),
        exposed: Value::Object(exposed),
    })
}

/// An OAuth2 client-credentials exchange with a cached token.
#[derive(Debug)]
pub struct OAuth2Client {
    token_url: Template,
    client_id: String,
    client_secret: Secret,
    scope: String,
    expires_in_fallback: Duration,
    early_refresh: Duration,
    expose: Vec<String>,
    cache: TokenCache,
    http: reqwest::Client,
}

impl OAuth2Client {
    /// A bearer token, exchanged now if none is cached or it is due for refresh.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Credential`] when the secret does not resolve or the
    /// token response is malformed, [`Error::Api`] for a non-2xx token
    /// response, and [`Error::Source`] when the token endpoint is unreachable.
    pub async fn token(&self, ctx: &TemplateCtx) -> Result<SensitiveString> {
        self.cached(ctx).await.map(|c| c.token)
    }

    async fn cached(&self, ctx: &TemplateCtx) -> Result<CachedToken> {
        self.cache.get_or_mint(|| self.exchange(ctx)).await
    }

    async fn exchange(&self, ctx: &TemplateCtx) -> Result<CachedToken> {
        let url = self.token_url.render(ctx)?;
        let secret = self.client_secret.value().await?;
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", "client_credentials"),
            ("client_id", self.client_id.as_str()),
            ("client_secret", secret),
        ];
        if !self.scope.is_empty() {
            form.push(("scope", self.scope.as_str()));
        }
        let response =
            self.http.post(&url).form(&form).send().await.map_err(|e| {
                Error::Source(format!("token exchange at {url}: {}", e.without_url()))
            })?;
        cached_token(
            response,
            &url,
            self.expires_in_fallback,
            self.early_refresh,
            &self.expose,
        )
        .await
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

/// The OAuth2 JWT-bearer grant: an RS256 assertion over the profile's claim
/// templates, signed with the instance's key and exchanged for a cached
/// access token.
pub struct JwtBearer {
    token_url: Template,
    claims: Vec<(String, Template)>,
    ttl: Duration,
    source: JwtKeySource,
    key: OnceCell<JwtKey>,
    expires_in_fallback: Duration,
    early_refresh: Duration,
    expose: Vec<String>,
    cache: TokenCache,
    http: reqwest::Client,
}

impl fmt::Debug for JwtBearer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JwtBearer")
            .field("token_url", &self.token_url)
            .field("claims", &self.claims)
            .field("ttl", &self.ttl)
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl JwtBearer {
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

    /// A bearer token, exchanged now if none is cached or it is due for refresh.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Credential`] when the key does not resolve or parse
    /// or the token response is malformed, [`Error::Config`] when a claim or
    /// the token URL fails to render, [`Error::Api`] for a non-2xx token
    /// response, and [`Error::Source`] when the token endpoint is unreachable.
    pub async fn token(&self, ctx: &TemplateCtx) -> Result<SensitiveString> {
        self.cached(ctx).await.map(|c| c.token)
    }

    async fn cached(&self, ctx: &TemplateCtx) -> Result<CachedToken> {
        self.cache.get_or_mint(|| self.exchange(ctx)).await
    }

    /// The signed assertion and the URL it is sent to; the templates see the
    /// key's `client_email` and `token_uri` as `auth.*`, and the rendered
    /// `token_url` as `auth.token_url`.
    async fn assertion(&self, ctx: &TemplateCtx) -> Result<(String, String)> {
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

    async fn exchange(&self, ctx: &TemplateCtx) -> Result<CachedToken> {
        let (url, jwt) = self.assertion(ctx).await?;
        let response = self
            .http
            .post(&url)
            .form(&[("grant_type", JWT_BEARER_GRANT), ("assertion", &jwt)])
            .send()
            .await
            .map_err(|e| Error::Source(format!("token exchange at {url}: {}", e.without_url())))?;
        cached_token(
            response,
            &url,
            self.expires_in_fallback,
            self.early_refresh,
            &self.expose,
        )
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

/// The GCE metadata server's token for the workload's service account.
#[derive(Debug)]
pub struct GceMetadata {
    url: Template,
    expires_in_fallback: Duration,
    early_refresh: Duration,
    cache: TokenCache,
    http: reqwest::Client,
}

impl GceMetadata {
    /// A bearer token, fetched now if none is cached or it is due for refresh.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Api`] for a non-2xx answer, [`Error::Credential`]
    /// for a body that is not a token, and [`Error::Source`] when the
    /// metadata server is unreachable (not running on GCE or GKE).
    pub async fn token(&self, ctx: &TemplateCtx) -> Result<SensitiveString> {
        self.cache
            .get_or_mint(|| async {
                let url = self.url.render(ctx)?;
                let response = self
                    .http
                    .get(&url)
                    .header("Metadata-Flavor", "Google")
                    .send()
                    .await
                    .map_err(|e| {
                        Error::Source(format!("metadata server at {url}: {}", e.without_url()))
                    })?;
                cached_token(
                    response,
                    &url,
                    self.expires_in_fallback,
                    self.early_refresh,
                    &[],
                )
                .await
            })
            .await
            .map(|c| c.token)
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

    async fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
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
    session: OnceCell<reqsign::Signer<Credential>>,
}

impl AssumeRole {
    /// The signer over the session, built once from the static keys.
    async fn signer(
        &self,
        region: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> &reqsign::Signer<Credential> {
        self.session
            .get_or_init(|| async {
                let sts = reqsign::Signer::new(
                    self.context.clone(),
                    StaticCredentialProvider::new(access_key_id, secret_access_key),
                    RequestSigner::new("sts", region),
                );
                let provider = AssumeRoleCredentialProvider::new(self.role_arn.clone(), sts)
                    .with_role_session_name("dfe-fetcher".to_owned())
                    .with_region(region.to_owned())
                    .with_regional_sts_endpoint();
                reqsign::Signer::new(
                    self.context.clone(),
                    provider,
                    RequestSigner::new("sts", region),
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

    async fn sign(&self, request: &mut reqwest::Request, ctx: &TemplateCtx) -> Result<()> {
        let service = self.service.render(ctx)?;
        let region = self.region.render(ctx)?;
        let (access_key_id, secret_access_key) = self.keys().await?;
        let request_signer = RequestSigner::new(&service, &region);
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
        signer: &reqsign::Signer<Credential>,
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
    Bearer(Secret),
    /// A key in a header or query parameter.
    ApiKey {
        /// The key.
        key: Secret,
        /// Where it goes.
        place: ApiKeyPlace,
    },
    /// HTTP Basic.
    Basic {
        /// User name.
        username: String,
        /// Password.
        password: Secret,
    },
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
    /// `http` is the client the token exchange uses.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the instance's mode is not accepted by the
    /// profile or an identity field for it is missing.
    pub fn build(spec: &AuthSpec, identity: &InstanceAuth, http: reqwest::Client) -> Result<Self> {
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
        http: reqwest::Client,
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
            AuthKind::Bearer => AuthMode::Bearer(need("token", identity.token.as_ref())?),
            AuthKind::ApiKey => AuthMode::ApiKey {
                key: need("key", identity.key.as_ref())?,
                place: match (&spec.api_key.header, &spec.api_key.query) {
                    (Some(name), _) => ApiKeyPlace::Header {
                        name: name.clone(),
                        prefix: spec.api_key.prefix.clone(),
                    },
                    (None, Some(name)) => ApiKeyPlace::Query { name: name.clone() },
                    (None, None) => {
                        return Err(Error::Config(
                            "auth.api_key needs `header` or `query`".into(),
                        ));
                    }
                },
            },
            AuthKind::Basic => AuthMode::Basic {
                username: identity
                    .username
                    .clone()
                    .ok_or_else(|| Error::Config("auth mode `basic` needs `username`".into()))?,
                password: need("password", identity.password.as_ref())?,
            },
            AuthKind::Oauth2ClientCredentials => {
                let oauth = &spec.oauth2_client_credentials;
                AuthMode::OAuth2ClientCredentials(OAuth2Client {
                    token_url: Template::compile(&oauth.token_url)?,
                    client_id: identity.client_id.clone().ok_or_else(|| {
                        Error::Config(
                            "auth mode `oauth2_client_credentials` needs `client_id`".into(),
                        )
                    })?,
                    client_secret: need("client_secret", identity.client_secret.as_ref())?,
                    scope: scope
                        .map(str::to_owned)
                        .or_else(|| identity.scope.clone())
                        .unwrap_or_else(|| oauth.scope.clone()),
                    expires_in_fallback: Duration::from_secs(oauth.expires_in_fallback_secs),
                    early_refresh: Duration::from_secs(oauth.early_refresh_secs),
                    expose: oauth.expose.clone(),
                    cache: TokenCache::default(),
                    http,
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
                    token_url: Template::compile(&jwt.token_url)?,
                    claims,
                    ttl: Duration::from_secs(jwt.ttl_secs),
                    source,
                    key: OnceCell::new(),
                    expires_in_fallback: Duration::from_secs(jwt.expires_in_fallback_secs),
                    early_refresh: Duration::from_secs(jwt.early_refresh_secs),
                    expose: jwt.expose.clone(),
                    cache: TokenCache::default(),
                    http,
                })
            }
            AuthKind::GceMetadata => {
                let gce = &spec.gce_metadata;
                AuthMode::GceMetadata(GceMetadata {
                    url: Template::compile(&gce.url)?,
                    expires_in_fallback: Duration::from_secs(gce.expires_in_fallback_secs),
                    early_refresh: Duration::from_secs(gce.early_refresh_secs),
                    cache: TokenCache::default(),
                    http,
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
            AuthMode::ApiKey { .. } => AuthKind::ApiKey,
            AuthMode::Basic { .. } => AuthKind::Basic,
            AuthMode::OAuth2ClientCredentials(_) => AuthKind::Oauth2ClientCredentials,
            AuthMode::DuoHmac(_) => AuthKind::DuoHmac,
            AuthMode::JwtBearer(_) => AuthKind::JwtBearer,
            AuthMode::GceMetadata(_) => AuthKind::GceMetadata,
            AuthMode::SigV4(_) => AuthKind::SigV4,
        }
    }

    /// Put the credential on a built request. `ctx` renders the token URL.
    ///
    /// # Errors
    ///
    /// Returns the credential or token-exchange error.
    pub async fn authorize(&self, request: &mut reqwest::Request, ctx: &TemplateCtx) -> Result<()> {
        match self {
            AuthMode::None => {}
            AuthMode::Bearer(token) => {
                set_header(
                    request,
                    AUTHORIZATION,
                    &format!("Bearer {}", token.value().await?),
                )?;
            }
            AuthMode::ApiKey { key, place } => match place {
                ApiKeyPlace::Header { name, prefix } => {
                    let header = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                        .map_err(|e| Error::Config(format!("api_key header `{name}`: {e}")))?;
                    set_header(request, header, &format!("{prefix}{}", key.value().await?))?;
                }
                ApiKeyPlace::Query { name } => {
                    let value = key.value().await?;
                    request.url_mut().query_pairs_mut().append_pair(name, value);
                }
            },
            AuthMode::Basic { username, password } => {
                let pair = format!("{username}:{}", password.value().await?);
                let encoded = base64::engine::general_purpose::STANDARD.encode(pair);
                set_header(request, AUTHORIZATION, &format!("Basic {encoded}"))?;
            }
            AuthMode::OAuth2ClientCredentials(_)
            | AuthMode::JwtBearer(_)
            | AuthMode::GceMetadata(_) => {
                let token = self.minted(ctx).await?;
                set_header(
                    request,
                    AUTHORIZATION,
                    &format!("Bearer {}", token.expose()),
                )?;
            }
            AuthMode::DuoHmac(signer) => signer.sign(request).await?,
            AuthMode::SigV4(signer) => signer.sign(request, ctx).await?,
        }
        Ok(())
    }

    /// The cached-or-fresh token of a token-minting mode.
    async fn minted(&self, ctx: &TemplateCtx) -> Result<SensitiveString> {
        match self {
            AuthMode::OAuth2ClientCredentials(client) => client.token(ctx).await,
            AuthMode::JwtBearer(client) => client.token(ctx).await,
            AuthMode::GceMetadata(client) => client.token(ctx).await,
            AuthMode::None
            | AuthMode::Bearer(_)
            | AuthMode::ApiKey { .. }
            | AuthMode::Basic { .. }
            | AuthMode::DuoHmac(_)
            | AuthMode::SigV4(_) => Err(Error::Config(format!(
                "auth mode `{}` mints no token",
                self.kind().as_str()
            ))),
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
            AuthMode::OAuth2ClientCredentials(client) if !client.expose.is_empty() => {
                client.cached(ctx).await.map(|c| Some(c.exposed))
            }
            AuthMode::JwtBearer(client) if !client.expose.is_empty() => {
                client.cached(ctx).await.map(|c| Some(c.exposed))
            }
            _ => Ok(None),
        }
    }

    /// Resolve the credential (and mint a token) without sending a data request.
    ///
    /// # Errors
    ///
    /// Returns the credential or token-exchange error.
    pub async fn probe(&self, ctx: &TemplateCtx) -> Result<()> {
        match self {
            AuthMode::None => Ok(()),
            AuthMode::Bearer(secret) | AuthMode::ApiKey { key: secret, .. } => {
                secret.value().await.map(|_| ())
            }
            AuthMode::Basic { password, .. } => password.value().await.map(|_| ()),
            AuthMode::OAuth2ClientCredentials(_)
            | AuthMode::JwtBearer(_)
            | AuthMode::GceMetadata(_) => self.minted(ctx).await.map(|_| ()),
            AuthMode::DuoHmac(signer) => signer.secret_key.value().await.map(|_| ()),
            AuthMode::SigV4(signer) => signer.keys().await.map(|_| ()),
        }
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
            reqwest::Client::new(),
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
        let mode = AuthMode::build(
            &header_spec,
            &identity(AuthKind::ApiKey),
            reqwest::Client::new(),
        )
        .unwrap();
        let mut req = request();
        mode.authorize(&mut req, &TemplateCtx::new()).await.unwrap();
        assert_eq!(
            req.headers().get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "SSWS k"
        );

        let mut query_spec = spec(&[AuthKind::ApiKey]);
        query_spec.api_key.query = Some("api_key".into());
        let mode = AuthMode::build(
            &query_spec,
            &identity(AuthKind::ApiKey),
            reqwest::Client::new(),
        )
        .unwrap();
        let mut req = request();
        mode.authorize(&mut req, &TemplateCtx::new()).await.unwrap();
        assert_eq!(req.url().query(), Some("api_key=k"));
    }

    #[tokio::test]
    async fn basic_encodes_user_and_password() {
        let mode = AuthMode::build(
            &spec(&[AuthKind::Basic]),
            &identity(AuthKind::Basic),
            reqwest::Client::new(),
        )
        .unwrap();
        let mut req = request();
        mode.authorize(&mut req, &TemplateCtx::new()).await.unwrap();
        assert_eq!(
            req.headers().get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "Basic dTpw"
        );
    }

    #[test]
    fn a_mode_the_profile_does_not_accept_or_a_missing_field_is_a_config_error() {
        let err = AuthMode::build(
            &spec(&[AuthKind::Bearer]),
            &identity(AuthKind::Basic),
            reqwest::Client::new(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
        let mut missing = identity(AuthKind::Bearer);
        missing.token = None;
        let err = AuthMode::build(&spec(&[AuthKind::Bearer]), &missing, reqwest::Client::new())
            .unwrap_err();
        assert!(err.to_string().contains("token"), "{err}");
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
            reqwest::Client::new(),
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
            reqwest::Client::new(),
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
            reqwest::Client::new(),
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
            reqwest::Client::new(),
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
            reqwest::Client::new(),
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
            reqwest::Client::new(),
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
}
