// Project:   dfe-fetcher
// File:      crates/rest/src/shape/mod.rs
// Purpose:   The REST shape: one page sequence per window step per unit, rows pulled lazily
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The REST event-window and dump shape.
//!
//! One [`RestShape`] per instance. A tick of one unit sends its prelude,
//! then walks the window steps and, within each, the pages: render the
//! request from the profile's templates, send it through the executor,
//! frame the body with the decoder, yield rows one at a time, then ask the
//! pager for the next page. A lookup or manifest unit feeds each page's
//! rows into a second request whose response carries the rows; a lister
//! unit takes its one page of items from a listing protocol instead of a
//! page fetch; a fold unit hands each key's rows to a folding builder and
//! yields the one row they become. Pages and items are requested one at a
//! time and only when the driver polls for more, so not polling is what
//! pushes back on the provider. Nothing here knows about transports,
//! cursors or the batcher.

pub mod queue;
pub mod window;

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use reqwest::header::HeaderMap;
use serde_json::Value;

use bytes::Bytes;
use dfe_fetcher_core::checkpoint::CheckpointValue;
use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::metric_names;
use dfe_fetcher_core::rules::splice_fields;
use dfe_fetcher_core::{Mark, Row, RowSource, RowStream, SourceMaturity, TickCtx, UnitSpec};

use crate::auth::AuthMode;
use crate::decode::{ByteStream, Decoder, RowBytes, collect_page};
use crate::hooks::RowBuilder;
use crate::hooks::s3_list::{CONTINUATION_PARAM, ListingPages};
use crate::page::{Inject, PageMeta, PageState, Pager};
use crate::profile::bound::{
    BoundEndpoint, BoundLookup, BoundProfile, BoundRequest, KeySource, bind, render_body,
};
use crate::profile::template::TemplateCtx;
use crate::profile::{AuthKind, Method, RestInstance, RestProfile};
use crate::request::{ExchangeClient, RequestExecutor, Sending};
use window::Step;

/// The REST shape of one instance.
#[derive(Debug)]
pub struct RestShape {
    bound: BoundProfile,
    auth: AuthMode,
    /// The same identity minting a token per scope a unit names, keyed by
    /// scope so units sharing an audience share a token.
    scoped_auth: BTreeMap<String, AuthMode>,
    executor: RequestExecutor,
    maturity: SourceMaturity,
}

impl RestShape {
    /// Bind `profile` to `instance` and build its auth mode over `exchange`,
    /// plus one per scope the units name; data requests go out on `client`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] with the field for anything the profile or the
    /// instance gets wrong.
    pub fn from_instance(
        profile: &RestProfile,
        instance: &RestInstance,
        connection_id: &str,
        client: reqwest::Client,
        exchange: &Arc<ExchangeClient>,
    ) -> Result<Self> {
        let bound = bind(profile, instance, connection_id)?;
        // A signing scheme that is a crypto exemption says so per connection,
        // however it was selected -- the profile's preset, the instance's
        // `signature_preset`, a typed block's own spelling of it, or a profile
        // that names the mode and spells no scheme.
        if instance.auth.mode == AuthKind::Signature
            && let Some(exemption) = instance.signature(profile).crypto_exemption()
        {
            tracing::warn!(source = connection_id, "connection {exemption}");
        }
        // The INSTANCE's context, never a unit's: a credential mode is built
        // once and its endpoint and claims are rendered here, so no unit can
        // change what the mode mints (see `AuthMode`).
        let auth = AuthMode::build(
            &bound.auth,
            &instance.auth,
            Arc::clone(exchange),
            &bound.ctx,
        )?;
        let mut scoped_auth = BTreeMap::new();
        // A profile whose `accepts` merely carries a minting mode lets a unit
        // name a scope under whichever mode the instance picks, and a mode that
        // mints nothing ignores the scope: a second one would read every spec
        // from its store again and sign exactly as this one does.
        if instance.auth.mode.is_scoped() {
            for scope in bound.endpoints.iter().filter_map(|e| e.auth_scope.clone()) {
                if scoped_auth.contains_key(&scope) {
                    continue;
                }
                let mode = AuthMode::build_scoped(
                    &bound.auth,
                    &instance.auth,
                    Arc::clone(exchange),
                    &bound.ctx,
                    Some(&scope),
                )?;
                scoped_auth.insert(scope, mode);
            }
        }
        Ok(Self::with_scoped_auth(
            bound,
            auth,
            scoped_auth,
            client,
            profile.maturity,
        ))
    }

    /// A shape over an already-bound profile.
    #[must_use]
    pub fn new(
        bound: BoundProfile,
        auth: AuthMode,
        client: reqwest::Client,
        maturity: SourceMaturity,
    ) -> Self {
        Self::with_scoped_auth(bound, auth, BTreeMap::new(), client, maturity)
    }

    fn with_scoped_auth(
        bound: BoundProfile,
        auth: AuthMode,
        scoped_auth: BTreeMap<String, AuthMode>,
        client: reqwest::Client,
        maturity: SourceMaturity,
    ) -> Self {
        let quota = bound
            .quota
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let executor =
            RequestExecutor::new(client, bound.retry.clone(), bound.error_at.clone(), quota);
        Self {
            bound,
            auth,
            scoped_auth,
            executor,
            maturity,
        }
    }

    /// The mode a unit's requests carry: the one for its scope when it names
    /// one, else the instance's.
    fn auth_for(&self, endpoint: &BoundEndpoint) -> &AuthMode {
        endpoint
            .auth_scope
            .as_ref()
            .and_then(|scope| self.scoped_auth.get(scope))
            .unwrap_or(&self.auth)
    }

    /// The bound profile.
    #[must_use]
    pub fn bound(&self) -> &BoundProfile {
        &self.bound
    }

    /// The instance's connection id, the `source` label.
    #[must_use]
    pub fn connection_id(&self) -> &str {
        &self.bound.connection_id
    }

    /// `base_url/path` with the rendered query pairs appended; a path that
    /// renders a whole URL (a manifest item's `contentUri`) replaces the base
    /// URL, and the executor refuses the host unless the unit's
    /// [`crate::origin::OriginSet`] names it.
    fn request_url(
        base_url: &str,
        path: &crate::profile::Template,
        query: &crate::profile::TemplateMap,
        ctx: &TemplateCtx,
    ) -> Result<reqwest::Url> {
        let path = path.render(ctx)?;
        let joined = if path.starts_with("http://") || path.starts_with("https://") {
            path
        } else {
            format!("{base_url}/{}", path.trim_start_matches('/'))
        };
        let mut url = reqwest::Url::parse(&joined)
            .map_err(|e| Error::Config(format!("request URL `{joined}`: {e}")))?;
        {
            let mut pairs = url.query_pairs_mut();
            for (name, value) in query.render(ctx)? {
                pairs.append_pair(&name, &value);
            }
        }
        drop_empty_query(&mut url);
        Ok(url)
    }

    /// The profile's headers, then `extra`'s, rendered against `ctx`.
    fn render_headers(
        &self,
        ctx: &TemplateCtx,
        extra: Option<&crate::profile::TemplateMap>,
    ) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        let mut rendered = self.bound.headers.render(ctx)?;
        if let Some(extra) = extra {
            rendered.extend(extra.render(ctx)?);
        }
        for (name, value) in rendered {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| Error::Config(format!("header `{name}`: {e}")))?;
            let value = reqwest::header::HeaderValue::from_str(&value)
                .map_err(|e| Error::Config(format!("header `{name}` value: {e}")))?;
            headers.insert(name, value);
        }
        Ok(headers)
    }

    /// Send the profile's probe request with the instance's credential; when
    /// the probe declares `fail_when`, read the 2xx body and check it.
    async fn send_probe(&self, probe: &crate::profile::bound::BoundProbe) -> Result<()> {
        let ctx = &self.bound.ctx;
        let url = Self::request_url(&self.bound.base_url, &probe.path, &probe.query, ctx)?;
        let headers = self.render_headers(ctx, None)?;
        let method = reqwest_method(probe.method);
        let client = self.executor.client().clone();
        let make = || {
            client
                .request(method.clone(), url.clone())
                .headers(headers.clone())
                .build()
                .map_err(|e| Error::Config(format!("probe request build: {e}")))
        };
        let Some(response) = self
            .executor
            .send(
                Sending {
                    source: &self.bound.connection_id,
                    auth: &self.auth,
                    ctx,
                    origins: &self.bound.origins,
                    idempotent: probe.method == Method::Get,
                    ignore: &[],
                },
                make,
            )
            .await?
        else {
            unreachable!("a probe ignores no status, so the executor never answers None")
        };
        let Some(predicate) = &probe.fail_when else {
            return Ok(());
        };
        let response_headers = response.headers().clone();
        let stream: ByteStream<'static> = response.bytes_stream().map_err(body_error).boxed();
        let page = collect_page(stream, crate::profile::DEFAULT_MAX_PAGE_BYTES).await?;
        let value = serde_json::from_slice::<Value>(&page)
            .map_err(|e| Error::Decode(format!("probe body is not JSON: {e}")))?;
        self.check_failure(predicate, &value, &response_headers)
    }

    /// Evaluate a `fail_when` predicate over a 2xx body; true is the
    /// provider's failure, reported with the text at `error.at`.
    fn check_failure(
        &self,
        predicate: &crate::profile::Predicate,
        value: &Value,
        headers: &HeaderMap,
    ) -> Result<()> {
        let mut check = TemplateCtx::new();
        check.set("body", value.clone());
        check.set("headers", PageMeta::new(headers, None).headers_json());
        if !predicate.eval(&check)? {
            return Ok(());
        }
        let text = self
            .bound
            .error_at
            .as_deref()
            .and_then(|p| value.pointer(p))
            .map_or_else(|| value.to_string(), ToString::to_string);
        Err(Error::Source(format!(
            "provider reported failure in a 2xx page: {}",
            text.chars().take(512).collect::<String>()
        )))
    }

    /// The URL, headers and body of one request of `request` at page
    /// `state`: the pager's parameter and cursor go where the strategy says.
    fn build_parts(
        &self,
        base_url: &str,
        request: &BoundRequest,
        pager: &Pager,
        state: &PageState,
        ctx: &TemplateCtx,
    ) -> Result<(reqwest::Url, HeaderMap, Option<Value>)> {
        let mut url = if let Some(next) = &state.next_url {
            reqwest::Url::parse(next)
                .map_err(|e| Error::Source(format!("next page URL `{next}`: {e}")))?
        } else {
            Self::request_url(base_url, &request.path, &request.query, ctx)?
        };
        if state.next_url.is_none() {
            {
                let mut pairs = url.query_pairs_mut();
                if let Some((name, value)) = pager.query_param(state) {
                    pairs.append_pair(name, &value);
                }
                if let (Some(Inject::Query(name)), Some(token)) = (pager.inject(), &state.token) {
                    pairs.append_pair(name, token);
                }
            }
            drop_empty_query(&mut url);
        }
        let headers = self.render_headers(ctx, Some(&request.headers))?;
        let body = match (request.method, &request.body) {
            (Method::Post, Some(body)) => {
                let mut rendered = match (pager.inject(), &state.token) {
                    (Some(Inject::BodyReplace(_)), Some(_)) => {
                        Value::Object(serde_json::Map::new())
                    }
                    _ => render_body(body, ctx)?,
                };
                if let (Some(Inject::Body(pointer) | Inject::BodyReplace(pointer)), Some(token)) =
                    (pager.inject(), &state.token)
                {
                    set_pointer(&mut rendered, pointer, Value::String(token.clone()))?;
                }
                Some(rendered)
            }
            _ => None,
        };
        Ok((url, headers, body))
    }

    /// Send one page request of a unit and frame its response.
    async fn fetch<'a>(
        &'a self,
        endpoint: &'a BoundEndpoint,
        stage: Stage<'a>,
        ctx: &TemplateCtx,
        page: &PageState,
        bytes: metrics::Counter,
    ) -> Result<Page<'a>> {
        let (url, headers, body) =
            self.build_parts(&endpoint.base_url, stage.request, stage.pager, page, ctx)?;
        // A status the request ignores (a 404 for a key the provider does
        // not know) is an empty page, and the page sequence ends there.
        let Some(response) = self
            .send_request(
                endpoint,
                ctx,
                stage.request,
                url.clone(),
                headers,
                body.as_ref(),
            )
            .await?
        else {
            return Ok(Page::ignored(page));
        };
        let mut framed = self
            .frame_response(endpoint, stage, ctx, page, response, bytes)
            .await?;
        framed.url = Some(url);
        Ok(framed)
    }

    /// The unit's tick context: its own, plus the fields the credential
    /// mode exposes as `auth.*` (minting the token first when the mode
    /// exposes any).
    async fn tick_ctx(&self, endpoint: &BoundEndpoint) -> Result<TemplateCtx> {
        let mut ctx = endpoint.ctx.clone();
        if let Some(exposed) = self.auth_for(endpoint).exposed().await? {
            ctx.set("auth", exposed);
        }
        Ok(ctx)
    }

    /// Send the unit's prelude, one step after another, before its first
    /// page: a 2xx or an ignored status carries on, anything else fails
    /// the tick.
    async fn run_prelude(&self, endpoint: &BoundEndpoint, ctx: &TemplateCtx) -> Result<()> {
        for step in &endpoint.prelude {
            let (url, headers, body) = self.build_parts(
                &endpoint.base_url,
                step,
                &Pager::None,
                &Pager::None.first(),
                ctx,
            )?;
            self.send_request(endpoint, ctx, step, url, headers, body.as_ref())
                .await?;
        }
        Ok(())
    }

    /// One page of a unit's listing for the continuation `token`: the
    /// unit's listing request with the token injected, read whole; `None`
    /// for a status the request ignores.
    async fn list_page(
        &self,
        endpoint: &BoundEndpoint,
        ctx: &TemplateCtx,
        token: Option<String>,
        bytes: metrics::Counter,
    ) -> Result<Option<Bytes>> {
        let request = &endpoint.request;
        let path = request.path.render(ctx)?;
        let joined = if path.starts_with("http://") || path.starts_with("https://") {
            path
        } else {
            format!("{}/{}", endpoint.base_url, path.trim_start_matches('/'))
        };
        let mut url = reqwest::Url::parse(&joined)
            .map_err(|e| Error::Config(format!("listing URL `{joined}`: {e}")))?;
        // S3 signs and decodes the query as RFC 3986 (a space is `%20`), so
        // the pairs are encoded by hand rather than as a form.
        let mut pairs: Vec<(String, String)> = request.query.render(ctx)?;
        if let Some(token) = token {
            pairs.push((CONTINUATION_PARAM.to_owned(), token));
        }
        let query: Vec<String> = pairs
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    percent_encoding::utf8_percent_encode(k, crate::hooks::s3_list::RFC3986),
                    percent_encoding::utf8_percent_encode(v, crate::hooks::s3_list::RFC3986)
                )
            })
            .collect();
        url.set_query((!query.is_empty()).then(|| query.join("&")).as_deref());
        let headers = self.render_headers(ctx, Some(&request.headers))?;
        let Some(response) = self
            .send_request(endpoint, ctx, request, url, headers, None)
            .await?
        else {
            return Ok(None);
        };
        collect_page(body_stream(response, bytes), endpoint.max_page_bytes)
            .await
            .map(Some)
    }

    /// The listing of a lister unit as its one page of items: the objects
    /// modified after `cutoff`, oldest first.
    async fn list<'a>(
        &'a self,
        endpoint: &'a BoundEndpoint,
        ctx: &TemplateCtx,
        cutoff: Option<(chrono::DateTime<chrono::Utc>, String)>,
        bytes: metrics::Counter,
    ) -> Result<Page<'a>> {
        let Some(lister) = endpoint.lister else {
            return Err(Error::Config(format!(
                "unit `{}` names no lister",
                endpoint.unit.name
            )));
        };
        let mut pages = ListingRequests {
            shape: self,
            endpoint,
            ctx,
            bytes,
        };
        // The lister applies the manifest's item cap, so the cut lands on a
        // position boundary.
        let max_items = endpoint.lookup.as_ref().and_then(|l| l.max_items);
        let listing = lister
            .list(cutoff, max_items, endpoint.max_pages, &mut pages)
            .await?;
        if listing.truncated {
            let source = &self.bound.connection_id;
            metrics::counter!(metric_names::PAGES_TRUNCATED_TOTAL, "source" => source.clone(), "unit" => endpoint.unit.name.to_string()).increment(1);
            tracing::warn!(
                source,
                unit = %endpoint.unit.name,
                max_pages = endpoint.max_pages,
                "object listing ended on a continuation token; the keys past it wait for the next tick"
            );
        }
        Ok(Page {
            rows: futures::stream::iter(listing.items.into_iter().map(Ok)).boxed(),
            state: Pager::None.first(),
            headers: HeaderMap::new(),
            count: 0,
            body: None,
            ignored: false,
            url: None,
        })
    }

    /// Frame a response into a page: the body is read whole when the
    /// decoder, the pager or `fail_when` needs it, else streamed; the
    /// builder, when there is one, expands each framed row against `ctx`.
    async fn frame_response<'a>(
        &'a self,
        endpoint: &'a BoundEndpoint,
        stage: Stage<'a>,
        ctx: &TemplateCtx,
        page: &PageState,
        response: reqwest::Response,
        bytes: metrics::Counter,
    ) -> Result<Page<'a>> {
        let response_headers = response.headers().clone();
        let stream = body_stream(response, bytes);

        let needs_tree = stage.pager.reads_body() || stage.fail_when.is_some();
        let (rows, tree): (RowBytes<'a>, Option<Value>) =
            if stage.decoder.is_page_bounded() || needs_tree {
                let body = collect_page(stream, endpoint.max_page_bytes).await?;
                let tree = if needs_tree {
                    Some(
                        serde_json::from_slice::<Value>(&body)
                            .map_err(|e| Error::Decode(format!("page body is not JSON: {e}")))?,
                    )
                } else {
                    None
                };
                if let (Some(predicate), Some(value)) = (stage.fail_when, &tree) {
                    self.check_failure(predicate, value, &response_headers)?;
                }
                let framed = stage.decoder.frame_page(&body)?;
                (
                    futures::stream::iter(framed.into_iter().map(Ok)).boxed(),
                    tree,
                )
            } else {
                (stage.decoder.frame(stream, endpoint.max_page_bytes), None)
            };
        let rows = match stage.builder {
            Some(builder) => builder.expand_stream(rows, ctx.clone()),
            None => rows,
        };

        Ok(Page {
            rows,
            state: page.clone(),
            headers: response_headers,
            count: 0,
            body: tree,
            ignored: false,
            url: None,
        })
    }

    /// Build and send one request of `endpoint` through the executor, with
    /// the credential that unit signs with and after its rate gate hands out
    /// a slot; a `reqwest::Request` is single-use, so the builder runs per
    /// attempt. `ctx` is the request's own context, which a signing mode
    /// reads for its scope. `None` is a status the request ignores.
    async fn send_request(
        &self,
        endpoint: &BoundEndpoint,
        ctx: &TemplateCtx,
        request: &BoundRequest,
        url: reqwest::Url,
        headers: HeaderMap,
        body: Option<&Value>,
    ) -> Result<Option<reqwest::Response>> {
        if let Some(gate) = &endpoint.rate {
            gate.wait().await;
        }
        let client = self.executor.client().clone();
        let wire = reqwest_method(request.method);
        let make = || {
            let mut builder = client
                .request(wire.clone(), url.clone())
                .headers(headers.clone());
            if let Some(body) = body {
                builder = builder.json(body);
            }
            if let Some(timeout) = request.timeout {
                builder = builder.timeout(timeout);
            }
            builder
                .build()
                .map_err(|e| Error::Config(format!("request build: {e}")))
        };
        self.executor
            .send(
                Sending {
                    source: &self.bound.connection_id,
                    auth: self.auth_for(endpoint),
                    ctx,
                    origins: &endpoint.origins,
                    idempotent: request.idempotent(),
                    ignore: &request.ignore_status,
                },
                make,
            )
            .await
    }

    /// The keys of a keyset unit that reads them from a request: the array
    /// at `keys_at` in that response, none for an ignored status.
    async fn fetch_keys(
        &self,
        endpoint: &BoundEndpoint,
        request: &BoundRequest,
        keys_at: &str,
        ctx: &TemplateCtx,
    ) -> Result<Vec<Value>> {
        let (url, headers, body) = self.build_parts(
            &endpoint.base_url,
            request,
            &Pager::None,
            &Pager::None.first(),
            ctx,
        )?;
        let Some(response) = self
            .send_request(endpoint, ctx, request, url, headers, body.as_ref())
            .await?
        else {
            return Ok(Vec::new());
        };
        let stream: ByteStream<'static> = response.bytes_stream().map_err(body_error).boxed();
        let page = collect_page(stream, endpoint.max_page_bytes).await?;
        let value = serde_json::from_slice::<Value>(&page)
            .map_err(|e| Error::Decode(format!("keyset response is not JSON: {e}")))?;
        match value.pointer(keys_at) {
            None | Some(Value::Null) => Ok(Vec::new()),
            Some(Value::Array(keys)) => Ok(keys.clone()),
            Some(other) => Err(Error::Decode(format!(
                "unit `{}`: construct.keyset.keys_at `{keys_at}` holds {other}, not a list",
                endpoint.unit.name
            ))),
        }
    }
}

/// A lister's view of the shape: one signed listing request per
/// continuation token.
struct ListingRequests<'a> {
    shape: &'a RestShape,
    endpoint: &'a BoundEndpoint,
    ctx: &'a TemplateCtx,
    bytes: metrics::Counter,
}

impl ListingPages for ListingRequests<'_> {
    fn page(&mut self, token: Option<String>) -> BoxFuture<'_, Result<Option<Bytes>>> {
        self.shape
            .list_page(self.endpoint, self.ctx, token, self.bytes.clone())
            .boxed()
    }
}

/// The request, framing and paging of one stage of a unit: its page
/// sequence, or a lookup batch's.
#[derive(Clone, Copy)]
struct Stage<'a> {
    request: &'a BoundRequest,
    decoder: &'a Decoder,
    builder: Option<RowBuilder>,
    pager: &'a Pager,
    fail_when: Option<&'a crate::profile::Predicate>,
    max_pages: u32,
}

impl<'a> Stage<'a> {
    fn pages(endpoint: &'a BoundEndpoint) -> Self {
        Self {
            request: &endpoint.request,
            decoder: &endpoint.decoder,
            builder: endpoint.builder,
            pager: &endpoint.pager,
            fail_when: endpoint.fail_when.as_ref(),
            max_pages: endpoint.max_pages,
        }
    }

    fn lookup(lookup: &'a BoundLookup) -> Self {
        Self {
            request: &lookup.request,
            decoder: &lookup.decoder,
            builder: lookup.builder,
            pager: &lookup.pager,
            fail_when: None,
            max_pages: lookup.max_pages,
        }
    }
}

/// A response body as a byte stream, counting the bytes as they arrive.
fn body_stream(response: reqwest::Response, bytes: metrics::Counter) -> ByteStream<'static> {
    response
        .bytes_stream()
        .map_err(body_error)
        .inspect_ok(move |chunk| bytes.increment(chunk.len() as u64))
        .boxed()
}

/// A failure while reading a body; a timeout is named as one, since
/// reqwest's own text for it ("error decoding response body") would
/// classify as a network failure. The URL is stripped: reqwest's Display
/// appends it, and it carries the credential under `auth.api_key.query`.
fn body_error(e: reqwest::Error) -> Error {
    let timed_out = e.is_timeout();
    let e = e.without_url();
    if timed_out {
        Error::Source(format!(
            "response body: timed out waiting for the next bytes: {e}"
        ))
    } else {
        Error::Source(format!("response body: {e}"))
    }
}

/// A URL whose query rendered to nothing carries no `?`.
fn drop_empty_query(url: &mut reqwest::Url) {
    if url.query() == Some("") {
        url.set_query(None);
    }
}

/// The id a first-stage row carries: the row itself when it is a JSON
/// scalar, else the value at `id_at`.
fn row_id(payload: &[u8], id_at: Option<&str>) -> Result<Value> {
    let value: Value = serde_json::from_slice(payload)
        .map_err(|e| Error::Decode(format!("lookup id row is not JSON: {e}")))?;
    match id_at {
        None => Ok(value),
        Some(pointer) => value
            .pointer(pointer)
            .cloned()
            .ok_or_else(|| Error::Decode(format!("lookup id row has nothing at `{pointer}`"))),
    }
}

/// Write `value` at `pointer` in `body`, creating a missing top-level key.
fn set_pointer(body: &mut Value, pointer: &str, value: Value) -> Result<()> {
    if let Some(slot) = body.pointer_mut(pointer) {
        *slot = value;
        return Ok(());
    }
    match (body, pointer.strip_prefix('/')) {
        (Value::Object(map), Some(key)) if !key.contains('/') => {
            map.insert(key.replace("~1", "/").replace("~0", "~"), value);
            Ok(())
        }
        _ => Err(Error::Config(format!(
            "paginate.into `body:{pointer}` points nowhere in the request body"
        ))),
    }
}

/// One in-flight page of a unit's tick.
struct Page<'a> {
    rows: RowBytes<'a>,
    state: PageState,
    headers: HeaderMap,
    count: usize,
    body: Option<Value>,
    /// The provider answered a status the request ignores: no rows, no next page.
    ignored: bool,
    /// The URL the page was fetched from, which a relative next URL is
    /// resolved against.
    url: Option<reqwest::Url>,
}

impl Page<'_> {
    /// The empty page an ignored status stands for.
    fn ignored(state: &PageState) -> Self {
        Self {
            rows: futures::stream::empty().boxed(),
            state: state.clone(),
            headers: HeaderMap::new(),
            count: 0,
            body: None,
            ignored: true,
            url: None,
        }
    }
}

/// A lookup batch in flight: the ids one lookup request carries (one item
/// on a manifest) and the context its pages render from (the key and the
/// window step the batch closed under). Held apart from the tick's page
/// streams so a page of it can be fetched without borrowing a stream
/// across the await.
struct LookupBatch<'a> {
    shape: &'a RestShape,
    endpoint: &'a BoundEndpoint,
    lookup: &'a BoundLookup,
    ctx: TemplateCtx,
    ids: Vec<Value>,
    /// The checkpoint mark every row of a manifest item carries.
    mark: Option<Mark>,
    /// The fields stamped on every row of a manifest item.
    fields: Vec<(String, Value)>,
    bytes: metrics::Counter,
}

impl<'a> LookupBatch<'a> {
    /// A batch of `ids` under `ctx`; on a manifest the one id is the item
    /// the templates read, and its checkpoint mark and added fields are
    /// rendered here.
    fn new(
        shape: &'a RestShape,
        endpoint: &'a BoundEndpoint,
        lookup: &'a BoundLookup,
        mut ctx: TemplateCtx,
        ids: Vec<Value>,
        bytes: metrics::Counter,
    ) -> Result<Self> {
        let mut mark = None;
        let mut fields = Vec::with_capacity(lookup.item_fields.len());
        if lookup.manifest {
            let item = ids.first().cloned().unwrap_or(Value::Null);
            ctx.set("item", item);
            for (name, value) in &lookup.item_fields {
                fields.push((name.clone(), render_body(value, &ctx)?));
            }
            if let Some(templates) = &lookup.item_mark {
                let key = templates.key.render(&ctx)?;
                let position = templates.position.render(&ctx)?;
                let position = chrono::DateTime::parse_from_rfc3339(&position)
                    .map_err(|e| {
                        Error::Decode(format!(
                            "unit `{}`: manifest position `{position}` is not RFC 3339: {e}",
                            endpoint.unit.name
                        ))
                    })?
                    .with_timezone(&chrono::Utc);
                mark = Some(Mark::Item {
                    key: key.into_boxed_str(),
                    position,
                });
            }
        }
        Ok(Self {
            shape,
            endpoint,
            lookup,
            ctx,
            ids,
            mark,
            fields,
            bytes,
        })
    }

    /// One page of the batch: the request renders `ids` as the builder
    /// shapes them, the rows see the ids as they were collected; an
    /// ignored status is an empty page.
    async fn page(&self, page: &PageState) -> Result<Page<'a>> {
        let mut ctx = self.ctx.clone();
        ctx.set("page", page.as_json());
        let mut request_ctx = ctx.clone();
        let request_ids = match self.lookup.builder {
            Some(builder) => builder.request_ids(&self.ids, &ctx)?,
            None => self.ids.clone(),
        };
        request_ctx.set("ids", Value::Array(request_ids));
        ctx.set("ids", Value::Array(self.ids.clone()));
        let (url, headers, body) = self.shape.build_parts(
            &self.endpoint.base_url,
            &self.lookup.request,
            &self.lookup.pager,
            page,
            &request_ctx,
        )?;
        let Some(response) = self
            .shape
            .send_request(
                self.endpoint,
                &request_ctx,
                &self.lookup.request,
                url,
                headers,
                body.as_ref(),
            )
            .await?
        else {
            return Ok(Page::ignored(page));
        };
        self.shape
            .frame_response(
                self.endpoint,
                Stage::lookup(self.lookup),
                &ctx,
                page,
                response,
                self.bytes.clone(),
            )
            .await
    }
}

/// The state the row stream of one unit's tick carries between polls: the
/// keys still to fetch (one `None` when the unit has no keyset, resolved on
/// the first poll when they come from a request), the window steps each key
/// walks, and within a step the page sequence.
struct Tick<'a> {
    shape: &'a RestShape,
    endpoint: &'a BoundEndpoint,
    ctx: TemplateCtx,
    keys: Option<std::vec::IntoIter<Option<Value>>>,
    window_steps: Vec<Step>,
    steps: std::vec::IntoIter<Step>,
    step: Option<Step>,
    /// The fields stamped on every row of the current key.
    fields: Vec<(String, Value)>,
    pending: Option<PageState>,
    current: Option<Page<'a>>,
    /// Lookup units: the ids the pages have yielded and not yet looked up,
    /// the batch in flight (kept for its next page), and the page of the
    /// lookup in flight.
    ids: Vec<Value>,
    batch: Option<LookupBatch<'a>>,
    lookup_page: Option<Page<'a>>,
    /// Whether the prelude has been sent this tick.
    prelude_sent: bool,
    /// Whether the tick's context has been set up (the exposed `auth.*`).
    ctx_ready: bool,
    /// The unit's last committed checkpoint, which a lister narrows by.
    checkpoint: Option<CheckpointValue>,
    /// Manifest items taken for the current key, against `max_items`.
    items_taken: usize,
    /// The rows of the current key, held for the fold.
    folding: Vec<Bytes>,
    finished: bool,
    records: metrics::Counter,
    bytes: metrics::Counter,
}

impl<'a> Tick<'a> {
    /// The keys this tick walks: each element of the keyset list or of the
    /// keyset request's array, or one `None` for a unit without a keyset.
    async fn load_keys(&mut self) -> Result<()> {
        let keys = match &self.endpoint.keyset {
            None => vec![None],
            Some(KeySource::List(template)) => match template.render_value(&self.ctx)? {
                Value::Array(keys) => keys.into_iter().map(Some).collect(),
                other => {
                    return Err(Error::Config(format!(
                        "unit `{}`: construct.keyset.from rendered {other}, not a list",
                        self.endpoint.unit.name
                    )));
                }
            },
            Some(KeySource::Request { request, keys_at }) => self
                .shape
                .fetch_keys(self.endpoint, request, keys_at, &self.ctx)
                .await?
                .into_iter()
                .map(Some)
                .collect(),
        };
        self.keys = Some(keys.into_iter());
        Ok(())
    }

    /// Send the buffered ids as one lookup batch, under the current key and
    /// the window step they were collected in, and take its first page.
    async fn flush_lookup(&mut self) -> Result<()> {
        let Some(lookup) = &self.endpoint.lookup else {
            return Ok(());
        };
        if self.ids.is_empty() {
            return Ok(());
        }
        let mut ctx = self.ctx.clone();
        ctx.set("window", self.step_json());
        let batch = LookupBatch::new(
            self.shape,
            self.endpoint,
            lookup,
            ctx,
            std::mem::take(&mut self.ids),
            self.bytes.clone(),
        )?;
        let page = batch.page(&lookup.pager.first()).await?;
        self.batch = Some(batch);
        self.lookup_page = Some(page);
        Ok(())
    }

    /// A page row of a lookup unit is an id: buffer it, and flush a full
    /// batch. `true` when a lookup is now in flight. A manifest past its
    /// item cap for the key takes no more and asks for no more pages.
    async fn take_id(&mut self, payload: &[u8]) -> Result<bool> {
        let Some(lookup) = &self.endpoint.lookup else {
            return Ok(false);
        };
        if lookup
            .max_items
            .is_some_and(|max| self.items_taken >= max as usize)
        {
            self.current = None;
            self.pending = None;
            return Ok(false);
        }
        self.items_taken += 1;
        self.ids.push(row_id(payload, lookup.id_at.as_deref())?);
        if self.ids.len() >= lookup.batch {
            self.flush_lookup().await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// The row the fold makes of the current key's rows, stamped with the
    /// key's fields; `None` when there is no fold or nothing was held.
    fn fold_key(&mut self) -> Result<Option<Row>> {
        let Some(fold) = self.endpoint.fold else {
            return Ok(None);
        };
        let rows = std::mem::take(&mut self.folding);
        Ok(fold
            .fold(&rows, &self.ctx)?
            .map(|payload| Row::new(splice_fields(payload, &self.fields))))
    }

    /// What templates read as `window` for the current step.
    fn step_json(&self) -> Value {
        self.step.as_ref().map_or(Value::Null, |s| s.json.clone())
    }

    /// Where a lister starts: the unit's committed checkpoint position AND
    /// key, else the start of the tick's window (the profile's lookback when
    /// the scheduler passes none), so a first tick reads the recent past and
    /// every later one reads once from where it left off.
    ///
    /// The key is half the position: the listing's timestamps are
    /// second-resolution, so objects written in the committed second are told
    /// apart by key alone. A window start has no key, and the empty string
    /// sorts before every real one.
    fn lister_cutoff(&self) -> Option<(chrono::DateTime<chrono::Utc>, String)> {
        match &self.checkpoint {
            Some(CheckpointValue::Item { position, key }) => Some((*position, key.clone())),
            _ => self
                .step
                .as_ref()
                .and_then(|s| s.window.as_ref())
                .map(|w| (w.start, String::new())),
        }
    }

    /// Whether every key of the tick has been walked.
    fn keys_done(&self) -> bool {
        self.keys
            .as_ref()
            .is_none_or(|keys| keys.as_slice().is_empty())
    }

    /// End the tick on `e`: the stream yields it once and then nothing.
    fn fail(&mut self, e: Error) -> Result<Row> {
        self.finished = true;
        Err(e)
    }

    /// Move to the next key: set `key` in the context, render its fields and
    /// restart the window steps. `false` when every key is done.
    fn step_key(&mut self) -> Result<bool> {
        let Some(key) = self.keys.as_mut().and_then(Iterator::next) else {
            return Ok(false);
        };
        if let Some(key) = key {
            self.ctx.set("key", key);
        }
        self.fields = self
            .endpoint
            .key_fields
            .iter()
            .map(|(name, value)| render_body(value, &self.ctx).map(|v| (name.clone(), v)))
            .collect::<Result<_>>()?;
        self.steps = self.window_steps.clone().into_iter();
        self.items_taken = 0;
        Ok(true)
    }

    /// The next page of a finished page: the pager's answer under the
    /// stage's ceiling, or `None` when the sequence ends.
    ///
    /// A ceiling with more to fetch is counted. A unit that reads the
    /// window fails on it: the scheduler advances the window only on a
    /// successful tick, and the rows past the ceiling belong to THIS window,
    /// so the next tick reads the same window again rather than skipping
    /// them. A dump or a unit that reads no window is cut short and says so.
    fn advance(&self, stage: Stage<'a>, page: &Page<'a>) -> Result<Option<PageState>> {
        if page.ignored {
            return Ok(None);
        }
        let meta = PageMeta::new(&page.headers, Some(page.count)).at(page.url.as_ref());
        match stage
            .pager
            .advance(&page.state, &meta, page.body.as_ref())?
        {
            Some(next) if next.number < stage.max_pages => Ok(Some(next)),
            Some(_) => {
                let source = &self.shape.bound.connection_id;
                let unit = &self.endpoint.unit.name;
                metrics::counter!(metric_names::PAGES_TRUNCATED_TOTAL, "source" => source.clone(), "unit" => unit.to_string()).increment(1);
                if self.endpoint.windowed && !self.endpoint.unit.is_dump() {
                    tracing::warn!(
                        source,
                        unit = %unit,
                        max_pages = stage.max_pages,
                        "page ceiling reached with rows of the window unfetched; the tick fails so the window is not advanced past them"
                    );
                    return Err(Error::PageCeiling {
                        unit: unit.to_string(),
                        max_pages: stage.max_pages,
                    });
                }
                tracing::debug!(
                    source,
                    unit = %unit,
                    max_pages = stage.max_pages,
                    "page ceiling reached; the rest of the store is cut from this tick"
                );
                Ok(None)
            }
            None => Ok(None),
        }
    }

    /// Produce the next row, moving through pages, steps and keys as they
    /// run out; on a lookup unit the pages feed the id buffer and the rows
    /// come from the lookup in flight, whose own pages are followed too.
    async fn next(&mut self) -> Option<Result<Row>> {
        loop {
            if self.finished {
                return None;
            }
            if !self.ctx_ready {
                match self.shape.tick_ctx(self.endpoint).await {
                    Ok(ctx) => self.ctx = ctx,
                    Err(e) => return Some(self.fail(e)),
                }
                self.ctx_ready = true;
            }
            if !self.prelude_sent {
                if let Err(e) = self.shape.run_prelude(self.endpoint, &self.ctx).await {
                    return Some(self.fail(e));
                }
                self.prelude_sent = true;
            }
            if self.keys.is_none()
                && let Err(e) = self.load_keys().await
            {
                return Some(self.fail(e));
            }
            if let Some(page) = &mut self.lookup_page {
                match page.rows.next().await {
                    Some(Ok(payload)) => {
                        page.count += 1;
                        self.records.increment(1);
                        let (mark, payload) = match &self.batch {
                            Some(batch) => {
                                (batch.mark.clone(), splice_fields(payload, &batch.fields))
                            }
                            None => (None, payload),
                        };
                        if self.endpoint.fold.is_some() {
                            self.folding.push(payload);
                            continue;
                        }
                        return Some(Ok(Row {
                            payload: splice_fields(payload, &self.fields),
                            mark,
                        }));
                    }
                    Some(Err(e)) => return Some(self.fail(e)),
                    None => {
                        let Some(page) = self.lookup_page.take() else {
                            continue;
                        };
                        let Some(batch) = &self.batch else {
                            continue;
                        };
                        match self.advance(Stage::lookup(batch.lookup), &page) {
                            Ok(Some(next)) => match batch.page(&next).await {
                                Ok(page) => self.lookup_page = Some(page),
                                Err(e) => return Some(self.fail(e)),
                            },
                            Ok(None) => self.batch = None,
                            Err(e) => return Some(self.fail(e)),
                        }
                        continue;
                    }
                }
            }
            if let Some(page) = &mut self.current {
                match page.rows.next().await {
                    Some(Ok(payload)) => {
                        page.count += 1;
                        if self.endpoint.lookup.is_some() {
                            match self.take_id(&payload).await {
                                Ok(_) => continue,
                                Err(e) => return Some(self.fail(e)),
                            }
                        }
                        self.records.increment(1);
                        if self.endpoint.fold.is_some() {
                            self.folding.push(payload);
                            continue;
                        }
                        return Some(Ok(Row::new(splice_fields(payload, &self.fields))));
                    }
                    Some(Err(e)) => return Some(self.fail(e)),
                    None => {
                        let Some(page) = self.current.take() else {
                            continue;
                        };
                        match self.advance(Stage::pages(self.endpoint), &page) {
                            Ok(next) => self.pending = next,
                            Err(e) => return Some(self.fail(e)),
                        }
                        continue;
                    }
                }
            }
            let state = if let Some(state) = self.pending.take() {
                state
            } else if let Some(step) = self.steps.next() {
                self.step = Some(step);
                self.endpoint.pager.first()
            } else {
                // The steps of this key are done: a lookup that reads the
                // key sends the ids it yielded before the next key is set,
                // and the last ids of the tick go out once every key is done.
                let per_key = self.endpoint.lookup.as_ref().is_some_and(|l| l.per_key);
                if !self.ids.is_empty() && (per_key || self.keys_done()) {
                    if let Err(e) = self.flush_lookup().await {
                        return Some(self.fail(e));
                    }
                    continue;
                }
                // The key's rows are complete: a fold yields their one row
                // before the next key is set.
                match self.fold_key() {
                    Ok(Some(row)) => return Some(Ok(row)),
                    Ok(None) => {}
                    Err(e) => return Some(self.fail(e)),
                }
                match self.step_key() {
                    Ok(true) => continue,
                    Ok(false) => {
                        self.finished = true;
                        return None;
                    }
                    Err(e) => return Some(self.fail(e)),
                }
            };
            let mut ctx = self.ctx.clone();
            ctx.set("window", self.step_json());
            ctx.set("page", state.as_json());
            let fetched = if self.endpoint.lister.is_some() {
                self.shape
                    .list(
                        self.endpoint,
                        &ctx,
                        self.lister_cutoff(),
                        self.bytes.clone(),
                    )
                    .await
            } else {
                self.shape
                    .fetch(
                        self.endpoint,
                        Stage::pages(self.endpoint),
                        &ctx,
                        &state,
                        self.bytes.clone(),
                    )
                    .await
            };
            match fetched {
                Ok(page) => self.current = Some(page),
                Err(e) => return Some(self.fail(e)),
            }
        }
    }
}

impl RowSource for RestShape {
    fn name(&self) -> &str {
        &self.bound.name
    }

    fn maturity(&self) -> SourceMaturity {
        self.maturity
    }

    fn units(&self) -> &[UnitSpec] {
        &self.bound.units
    }

    // SHORTCUT: pages are requested one at a time per unit and a connection's
    // units run sequentially; fan pages out with `buffer_unordered(n)` for the
    // offset and page-number pagers only when a store's page count x page
    // latency exceeds its interval.
    fn rows<'a>(&'a self, tick: TickCtx<'a>) -> RowStream<'a> {
        let Some(endpoint) = self.bound.endpoint(&tick.unit.name) else {
            let name = tick.unit.name.clone();
            return futures::stream::once(async move {
                Err(Error::Config(format!(
                    "unit `{name}` is not bound to this profile"
                )))
            })
            .boxed();
        };
        let ctx = endpoint.ctx.clone();
        // A unit whose requests never read the window has nothing to chunk:
        // it is the provider's current state and runs once per tick.
        let mut window_spec = endpoint.window.clone();
        if !endpoint.windowed {
            window_spec.step = None;
        }
        let window_steps = window::steps(
            &window_spec,
            endpoint.unit.shape,
            tick.window,
            chrono::Utc::now(),
        );
        let source = self.bound.connection_id.clone();
        let state = Tick {
            shape: self,
            endpoint,
            ctx,
            keys: None,
            window_steps,
            steps: Vec::new().into_iter(),
            step: None,
            fields: Vec::new(),
            pending: None,
            current: None,
            ids: Vec::new(),
            batch: None,
            lookup_page: None,
            prelude_sent: false,
            ctx_ready: false,
            checkpoint: tick.checkpoint.cloned(),
            items_taken: 0,
            folding: Vec::new(),
            finished: false,
            records: metrics::counter!(metric_names::RECORDS_FETCHED_TOTAL, "source" => source.clone()),
            bytes: metrics::counter!(metric_names::BYTES_FETCHED_TOTAL, "source" => source),
        };
        futures::stream::unfold(state, |mut tick| async move {
            tick.next().await.map(|row| (row, tick))
        })
        .boxed()
    }

    /// The profile's probe request when it declares one, which resolves the
    /// credential on the way; otherwise the credential alone.
    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        match &self.bound.probe {
            Some(probe) => self.send_probe(probe).boxed(),
            None => self.auth.probe().boxed(),
        }
    }
}

fn reqwest_method(method: Method) -> reqwest::Method {
    match method {
        Method::Get => reqwest::Method::GET,
        Method::Post => reqwest::Method::POST,
    }
}
