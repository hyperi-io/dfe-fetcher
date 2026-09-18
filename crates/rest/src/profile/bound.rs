// Project:   dfe-fetcher
// File:      crates/rest/src/profile/bound.rs
// Purpose:   A profile bound to one instance: every template compiled, every axis built
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Binding a profile to an instance.
//!
//! `bind` is the load-time step that turns the grammar's strings into compiled
//! templates, predicates, a [`Pager`] and a [`Decoder`] per endpoint, and the
//! [`UnitSpec`]s the driver iterates. Everything that can fail does so here,
//! with the instance's field path, so a bad profile never reaches a tick.

use std::collections::BTreeMap;
use std::sync::Arc;

use dfe_fetcher_core::batch::AccumulateConfig;
use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::{RowContent, UnitShape, UnitSpec};
use serde_json::Value;

use super::template::{Predicate, Template, TemplateCtx, TemplateMap};
use super::{
    AuthKind, AuthSpec, EndpointSpec, LookupRequest, Method, ProfileRef, QuotaSpec, RestInstance,
    RestProfile, RetrySpec, WindowSpec,
};
use crate::decode::Decoder;
use crate::hooks::{Lister, RowBuilder};
use crate::origin::OriginSet;
use crate::page::Pager;
use crate::request::RateGate;

/// One request shape with its templates compiled: a unit's page request,
/// a lookup's batch request, a keyset's key request, a manifest's item
/// request or a prelude step.
#[derive(Debug)]
pub struct BoundRequest {
    /// GET or POST.
    pub method: Method,
    /// Path appended to the base URL, or a whole URL when it renders one.
    pub path: Template,
    /// Query parameters: profile defaults, the endpoint's, then the instance's
    /// unit override, later entries winning.
    pub query: TemplateMap,
    /// Headers in the same precedence.
    pub headers: TemplateMap,
    /// POST body with template leaves.
    pub body: Option<Value>,
    /// Non-2xx statuses that are not failures for this request.
    pub ignore_status: Vec<u16>,
    /// A total bound on the request, body included, when the profile sets
    /// one; the client's idle read timeout applies otherwise.
    pub timeout: Option<std::time::Duration>,
}

impl BoundRequest {
    /// Whether a non-2xx may be retried.
    #[must_use]
    pub fn idempotent(&self) -> bool {
        self.method == Method::Get
    }

    /// Whether the path, query, headers or body read the top-level
    /// variable `var`.
    #[must_use]
    pub fn references(&self, var: &str) -> bool {
        self.path.references(var)
            || self.query.references(var)
            || self.headers.references(var)
            || self.body.as_ref().is_some_and(|b| body_references(b, var))
    }
}

/// Whether any string leaf of a body template reads `var`; a leaf that does
/// not compile reads nothing (validation reports it).
pub(crate) fn body_references(body: &Value, var: &str) -> bool {
    match body {
        Value::String(s) => Template::compile(s).is_ok_and(|t| t.references(var)),
        Value::Object(map) => map.values().any(|v| body_references(v, var)),
        Value::Array(items) => items.iter().any(|v| body_references(v, var)),
        _ => false,
    }
}

/// Where a keyset's keys come from.
#[derive(Debug)]
pub enum KeySource {
    /// A template rendering the list from the instance's vars.
    List(Template),
    /// A request the unit sends first, its keys at a pointer in the body.
    Request {
        /// The request.
        request: BoundRequest,
        /// JSON pointer to the key array.
        keys_at: String,
    },
}

/// One endpoint with every axis built.
#[derive(Debug)]
pub struct BoundEndpoint {
    /// The driver-facing unit.
    pub unit: UnitSpec,
    /// The page request.
    pub request: BoundRequest,
    /// The base URL this unit's requests go to, rendered once against the
    /// unit's context: the unit's own template when it names one, else the
    /// profile's.
    pub base_url: String,
    /// The origins this unit's requests may be sent to: its own base URL, the
    /// instance's, and the `allow_hosts` the profile and the endpoint declare.
    pub origins: OriginSet,
    /// The scope this unit's token is minted for, when it names one.
    pub auth_scope: Option<String>,
    /// Window rendering and chunking, with the unit's own format when it
    /// names one.
    pub window: WindowSpec,
    /// Row framing.
    pub decoder: Decoder,
    /// The transform each framed row goes through, when the unit names one.
    pub builder: Option<RowBuilder>,
    /// The fold the rows of one key go through, when the unit names one.
    pub fold: Option<RowBuilder>,
    /// The listing protocol standing in for the page fetch, when the unit
    /// names one.
    pub lister: Option<Lister>,
    /// Pagination.
    pub pager: Pager,
    /// Fails the tick when true over a 2xx page.
    pub fail_when: Option<Predicate>,
    /// Page ceiling per window step.
    pub max_pages: u32,
    /// The pace this unit's requests are held to, when it declares a rate;
    /// one gate per unit, so its pages, window steps and ticks share the
    /// one sequence of slots.
    pub rate: Option<RateGate>,
    /// Bound on a page-bounded decoder's buffer.
    pub max_page_bytes: usize,
    /// The context every request of this unit renders from: the instance's
    /// `vars` with the unit's own overlaid, `base_url`, and `unit`.
    pub ctx: TemplateCtx,
    /// The keys the unit fetches once each, when it is a keyset.
    pub keyset: Option<KeySource>,
    /// Fields rendered once per key and stamped on every row of that key:
    /// JSON values whose string leaves are templates.
    pub key_fields: Vec<(String, Value)>,
    /// The second stage, when the pages carry ids or items rather than rows.
    pub lookup: Option<BoundLookup>,
    /// The acknowledgement the unit's rows earn, when it is a queue.
    pub queue: Option<BoundQueue>,
    /// The requests sent once per tick before the first page.
    pub prelude: Vec<BoundRequest>,
    /// Whether any request of the unit reads `window`; one that reads none
    /// is the provider's current state and runs once per tick however the
    /// profile chunks the window.
    pub windowed: bool,
}

/// A queue's acknowledgement with its templates compiled.
#[derive(Debug)]
pub struct BoundQueue {
    /// Pointer into each framed row to its ack id.
    pub ack_at: String,
    /// The acknowledgement request; `ids` is the batch.
    pub ack_request: BoundRequest,
    /// Ids per acknowledgement request.
    pub ack_batch: usize,
}

/// The checkpoint mark of a manifest item, as templates over `item`.
#[derive(Debug)]
pub struct ItemMark {
    /// The item's identity.
    pub key: Template,
    /// The item's RFC 3339 position in the listing order.
    pub position: Template,
}

/// A lookup stage with its templates compiled: a lookup proper, or a
/// manifest, which is a lookup of one item per request whose templates
/// read that item as `item`.
#[derive(Debug)]
pub struct BoundLookup {
    /// Pointer into each first-stage row to its id; unset when the row is
    /// the id.
    pub id_at: Option<String>,
    /// Ids per request.
    pub batch: usize,
    /// The batch request; `ids` is the batch, `item` its one element on a
    /// manifest.
    pub request: BoundRequest,
    /// The request reads `key`, so a batch never spans two keys: the ids a
    /// key yielded are looked up before the next key is set.
    pub per_key: bool,
    /// One request per item, exposed to the templates as `item`.
    pub manifest: bool,
    /// The checkpoint mark every row of an item carries, when the manifest
    /// declares one.
    pub item_mark: Option<ItemMark>,
    /// Items fetched per key per tick on a manifest; unbounded when unset.
    pub max_items: Option<u32>,
    /// Fields rendered per item and stamped on every row of it: JSON values
    /// whose string leaves are templates and may read `item`.
    pub item_fields: Vec<(String, Value)>,
    /// Row framing of the response.
    pub decoder: Decoder,
    /// The transform each framed row of the response goes through.
    pub builder: Option<RowBuilder>,
    /// Pagination of one batch's response.
    pub pager: Pager,
    /// Page ceiling per batch.
    pub max_pages: u32,
}

impl BoundEndpoint {
    /// Whether a non-2xx may be retried.
    #[must_use]
    pub fn idempotent(&self) -> bool {
        self.request.idempotent()
    }
}

/// The health-check request with its templates compiled.
#[derive(Debug)]
pub struct BoundProbe {
    /// GET or POST.
    pub method: Method,
    /// Path appended to the base URL.
    pub path: Template,
    /// Query parameters.
    pub query: TemplateMap,
    /// Fails the probe when true over its 2xx body.
    pub fail_when: Option<Predicate>,
}

/// A profile bound to one instance.
#[derive(Debug)]
pub struct BoundProfile {
    /// Profile name, for logs.
    pub name: String,
    /// The instance's connection id.
    pub connection_id: String,
    /// The base URL, rendered once from the instance's vars.
    pub base_url: String,
    /// The origins the probe may be sent to: the instance's base URL and the
    /// profile's `allow_hosts`.
    pub origins: OriginSet,
    /// Headers on every request.
    pub headers: TemplateMap,
    /// Window rendering and chunking.
    pub window: WindowSpec,
    /// Retry policy.
    pub retry: RetrySpec,
    /// Error text pointer.
    pub error_at: Option<String>,
    /// Quota headers.
    pub quota: QuotaSpec,
    /// The auth shape, for building the instance's mode.
    pub auth: AuthSpec,
    /// The health-check request, when the profile declares one.
    pub probe: Option<BoundProbe>,
    /// Enabled endpoints, in profile order.
    pub endpoints: Vec<BoundEndpoint>,
    /// The units of `endpoints`, in the same order.
    pub units: Vec<UnitSpec>,
    /// The instance context every request render starts from: `vars`,
    /// `base_url`, `unit`.
    pub ctx: TemplateCtx,
    /// The instance's batch bounds, when it sets its own.
    pub accumulate: Option<AccumulateConfig>,
    /// The instance's CEL keep-filter.
    pub filter: Option<String>,
    /// The instance's fetch interval.
    pub interval_secs: Option<u64>,
    /// Topic base.
    pub topic: String,
}

fn compile_map(
    field: &str,
    entries: &BTreeMap<String, String>,
    into: &mut TemplateMap,
) -> Result<()> {
    for (name, value) in entries {
        into.insert(name, value)
            .map_err(|e| Error::Config(format!("{field}.{name}: {e}")))?;
    }
    Ok(())
}

/// Render every string leaf of a JSON body against `ctx`. A leaf that is
/// exactly one `{{ expr }}` keeps the expression's type, so a page size
/// stays a number and an id list stays a list; an object member whose leaf
/// renders `null` is left out, which is how a profile writes an optional
/// member (`"{{ vars.pattern != '' ? vars.pattern : null }}"`).
///
/// # Errors
///
/// Returns [`Error::Config`] when a leaf does not compile or render.
pub fn render_body(body: &Value, ctx: &TemplateCtx) -> Result<Value> {
    Ok(match body {
        Value::String(s) => Template::compile(s)?.render_value(ctx)?,
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| render_body(v, ctx).map(|v| (k.clone(), v)))
                .filter(|entry| !matches!(entry, Ok((_, Value::Null))))
                .collect::<Result<_>>()?,
        ),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|v| render_body(v, ctx))
                .collect::<Result<_>>()?,
        ),
        other => other.clone(),
    })
}

/// Permit the origin of each `allow_hosts` entry at `field`.
///
/// An entry is a template rendered like a base URL, so the host comes from the
/// operator's configuration rather than from a provider's answer, and it names
/// one exact origin: a suffix match would let `example.com.evil.net` pass for
/// `example.com`.
fn permit_allow_hosts(
    origins: &mut OriginSet,
    field: &str,
    entries: &[String],
    ctx: &TemplateCtx,
) -> Result<()> {
    for (i, entry) in entries.iter().enumerate() {
        let at = format!("{field}[{i}]");
        let rendered = Template::compile(entry)
            .and_then(|t| t.render(ctx))
            .map_err(|e| Error::Config(format!("{at}: {e}")))?;
        let rendered = rendered.trim();
        // An entry reading an unset operator var renders empty and widens
        // nothing, which is how `TemplateMap::render` already treats its own.
        if rendered.is_empty() {
            continue;
        }
        if rendered.contains('*') {
            return Err(Error::Config(format!(
                "{at}: `{rendered}` is a wildcard; allow_hosts names exact hosts"
            )));
        }
        if !origins.permit(rendered) {
            return Err(Error::Config(format!(
                "{at}: `{rendered}` is not an absolute URL naming a host, \
                 e.g. https://manage.office.com"
            )));
        }
    }
    Ok(())
}

/// Bind one endpoint as the unit `name`: the endpoint's own name, or the
/// name of an instance unit that instantiates it, whose override applies.
fn bind_endpoint(
    profile: &RestProfile,
    instance: &RestInstance,
    endpoint: &EndpointSpec,
    name: &str,
    ctx: &TemplateCtx,
) -> Result<BoundEndpoint> {
    let at = |f: &str| format!("endpoints[{name}].{f}");
    // The context reaching here is the instance's, so its `base_url` is the
    // instance-level render, which the probe addresses.
    let instance_base_url = ctx
        .get("base_url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let shape = endpoint.shape.unwrap_or(profile.shape);
    let over = instance.units.get(name);
    let topic_base = over
        .and_then(|o| o.topic.as_deref())
        .unwrap_or(&instance.topic);
    let topic = match shape {
        UnitShape::Incremental => topic_base.to_owned(),
        UnitShape::Dump => format!("{topic_base}-{name}"),
    };
    let mut ctx = ctx.clone();
    // The unit's own values over the instance's, the instance's per-unit
    // overrides over both.
    let unit_vars = over.map(|o| &o.vars);
    if !endpoint.vars.is_empty() || unit_vars.is_some_and(|v| !v.is_empty()) {
        let mut vars = ctx
            .get("vars")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        vars.extend(endpoint.vars.iter().map(|(k, v)| (k.clone(), v.clone())));
        if let Some(unit_vars) = unit_vars {
            vars.extend(unit_vars.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        ctx.set("vars", Value::Object(vars));
    }
    ctx.set("unit", serde_json::json!({ "name": name }));
    // The effective base URL template is rendered against the unit's own
    // context, so a profile whose hosts differ by unit may read a per-unit
    // var; the instance-level render is the probe's.
    let base_url = {
        let field = if endpoint.base_url.is_some() {
            at("base_url")
        } else {
            "base_url".to_owned()
        };
        let rendered = Template::compile(profile.base_url_of(endpoint))?
            .render(&ctx)
            .map_err(|e| Error::Config(format!("{field}: {e}")))?;
        if rendered.trim().is_empty() {
            return Err(Error::Config(format!(
                "{field} rendered empty; the instance must set the var it reads"
            )));
        }
        let rendered = rendered.trim_end_matches('/').to_owned();
        ctx.set("base_url", Value::String(rendered.clone()));
        rendered
    };
    let ctx = ctx;
    let mut origins = OriginSet::new();
    origins.permit(&base_url);
    origins.permit(&instance_base_url);
    permit_allow_hosts(&mut origins, "allow_hosts", &profile.allow_hosts, &ctx)?;
    permit_allow_hosts(
        &mut origins,
        &at("allow_hosts"),
        &endpoint.allow_hosts,
        &ctx,
    )?;
    let mut unit = UnitSpec::new(name, shape, &topic);
    unit.row_key.clone_from(&endpoint.row_key);
    unit.content = {
        let field = at("rows.content");
        let rendered = Template::compile(&profile.content_of(endpoint))
            .and_then(|t| t.render(&ctx))
            .map_err(|e| Error::Config(format!("{field}: {e}")))?;
        let content = rendered
            .trim()
            .parse()
            .map_err(|e| Error::Config(format!("{field}: {e}")))?;
        if content == RowContent::Binary && shape == UnitShape::Dump {
            return Err(Error::Config(format!(
                "{field}: a dump unit cannot carry binary rows; the snapshot envelope is JSON"
            )));
        }
        content
    };
    let keyset = match profile.keyset_of(endpoint) {
        None => None,
        Some(spec) => {
            let kat = |f: &str| at(&format!("construct.keyset.{f}"));
            Some(match (&spec.from, &spec.request, &spec.keys_at) {
                (Some(from), None, _) => KeySource::List(
                    Template::compile(from)
                        .map_err(|e| Error::Config(format!("{}: {e}", kat("from"))))?,
                ),
                (None, Some(request), Some(keys_at)) => KeySource::Request {
                    request: bind_request(&kat("request"), request, Method::Post)?,
                    keys_at: keys_at.clone(),
                },
                _ => {
                    return Err(Error::Config(format!(
                        "{}: a keyset needs exactly one of `from` or `request` (with `keys_at`)",
                        kat("from")
                    )));
                }
            })
        }
    };
    // A field that reads the key is rendered per key by the shape; every
    // other one renders once here and rides on the unit.
    let mut key_fields = Vec::new();
    for (field_name, value) in &endpoint.add_fields {
        let field = at(&format!("add_fields.{field_name}"));
        // The enricher owns these names on every record; a unit that added one
        // would have its value parked and renamed on the way out.
        if field_name.starts_with("_source") || field_name.starts_with("_timestamp_") {
            return Err(Error::Config(format!(
                "{field}: `_source*` and `_timestamp_*` are the enricher's own names and cannot be added by a unit"
            )));
        }
        for var in ["window", "page", "item"] {
            if body_references(value, var) {
                return Err(Error::Config(format!(
                    "{field}: add_fields renders once per request set and cannot read `{var}`"
                )));
            }
        }
        if body_references(value, "key") {
            if keyset.is_none() {
                return Err(Error::Config(format!(
                    "{field}: reads `key` but the unit declares no `construct.keyset`"
                )));
            }
            key_fields.push((field_name.clone(), value.clone()));
            continue;
        }
        let rendered =
            render_body(value, &ctx).map_err(|e| Error::Config(format!("{field}: {e}")))?;
        unit.add_fields.push((field_name.clone(), rendered));
    }

    let mut query = TemplateMap::new();
    compile_map("defaults.query", &profile.defaults.query, &mut query)?;
    compile_map(&at("query"), &endpoint.query, &mut query)?;
    let mut headers = TemplateMap::new();
    compile_map("defaults.headers", &profile.defaults.headers, &mut headers)?;
    compile_map(&at("headers"), &endpoint.headers, &mut headers)?;
    if let Some(over) = over {
        compile_map(&format!("units.{name}.query"), &over.query, &mut query)?;
        compile_map(
            &format!("units.{name}.headers"),
            &over.headers,
            &mut headers,
        )?;
    }

    let lookup = match (profile.lookup_of(endpoint), profile.manifest_of(endpoint)) {
        (Some(spec), _) => {
            let lat = |f: &str| at(&format!("construct.lookup.{f}"));
            let request = bind_request(&lat("request"), &spec.request, Method::Post)?;
            Some(BoundLookup {
                id_at: spec.id_at.clone(),
                batch: spec.batch.max(1),
                per_key: request.references("key"),
                manifest: false,
                item_mark: None,
                max_items: None,
                item_fields: Vec::new(),
                request,
                decoder: Decoder::build(&spec.rows),
                builder: RowBuilder::build(spec.rows.builder),
                pager: Pager::build(&spec.paginate.clone().unwrap_or_default())
                    .map_err(|e| Error::Config(format!("{}: {e}", lat("paginate"))))?,
                max_pages: spec.max_pages.unwrap_or(super::DEFAULT_MAX_PAGES),
            })
        }
        (None, Some(spec)) => {
            let mat = |f: &str| at(&format!("construct.manifest.{f}"));
            let request = bind_request(&mat("item_request"), &spec.item_request, Method::Get)?;
            let item_mark = match (&spec.key, &spec.position) {
                (Some(key), Some(position)) => Some(ItemMark {
                    key: Template::compile(key)
                        .map_err(|e| Error::Config(format!("{}: {e}", mat("key"))))?,
                    position: Template::compile(position)
                        .map_err(|e| Error::Config(format!("{}: {e}", mat("position"))))?,
                }),
                _ => None,
            };
            Some(BoundLookup {
                id_at: None,
                batch: 1,
                per_key: request.references("key"),
                manifest: true,
                item_mark,
                max_items: spec.max_items,
                item_fields: spec
                    .add_fields
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                request,
                decoder: Decoder::build(&spec.rows),
                builder: RowBuilder::build(spec.rows.builder),
                pager: Pager::None,
                max_pages: 1,
            })
        }
        (None, None) => None,
    };

    let prelude = profile
        .prelude_of(endpoint)
        .iter()
        .enumerate()
        .map(|(i, step)| bind_request(&at(&format!("prelude[{i}]")), step, Method::Post))
        .collect::<Result<Vec<_>>>()?;

    let queue = match profile.queue_of(endpoint) {
        Some(spec) => Some(BoundQueue {
            ack_at: spec.ack_at.clone(),
            ack_request: bind_request(
                &at("construct.queue.ack_request"),
                &spec.ack_request,
                Method::Post,
            )?,
            ack_batch: spec.ack_batch.max(1),
        }),
        None => None,
    };

    let rows = profile.rows_of(endpoint);
    let paginate = profile.paginate_of(endpoint);
    let request = BoundRequest {
        method: profile.method_of(endpoint),
        path: Template::compile(profile.path_of(endpoint))
            .map_err(|e| Error::Config(format!("{}: {e}", at("path"))))?,
        query,
        headers,
        body: profile.body_of(endpoint).cloned(),
        ignore_status: endpoint.ignore_status.clone(),
        timeout: endpoint.timeout_secs.map(std::time::Duration::from_secs),
    };
    let windowed = request.references("window")
        || lookup
            .as_ref()
            .is_some_and(|l| l.request.references("window"));
    Ok(BoundEndpoint {
        unit,
        request,
        base_url,
        origins,
        auth_scope: endpoint.auth_scope().map(str::to_owned),
        window: profile.window_of(endpoint),
        decoder: Decoder::build(&rows),
        builder: RowBuilder::build(rows.builder),
        fold: RowBuilder::build(endpoint.fold),
        lister: Lister::build(endpoint.lister),
        pager: Pager::build(&paginate)
            .map_err(|e| Error::Config(format!("{}: {e}", at("paginate"))))?,
        fail_when: endpoint
            .fail_when
            .as_deref()
            .map(Predicate::compile)
            .transpose()
            .map_err(|e| Error::Config(format!("{}: {e}", at("fail_when"))))?,
        max_pages: profile.max_pages_of(endpoint),
        rate: profile
            .rate_of(endpoint)
            .and_then(|rate| RateGate::new(rate.requests_per_sec)),
        max_page_bytes: profile.max_page_bytes_of(endpoint),
        ctx,
        keyset,
        key_fields,
        lookup,
        queue,
        prelude,
        windowed,
    })
}

/// Refuse a credential template whose render depends on which unit asks.
///
/// A credential mode is built once per instance -- and once per scope a unit
/// names -- so its token endpoint and its claims are rendered once, against the
/// instance's context. A template that rendered differently under a unit's
/// context would hold whichever answer reached the mode first and present it as
/// every other unit's credential. Where the claim decides WHO the token acts as
/// (a domain-wide-delegation `sub`), that is one unit's data fetched as another
/// unit's principal, so the profile is refused at load rather than documented.
///
/// `auth.*` is stood in for rather than resolved: what the authenticator puts
/// there comes off the signing key and the token response, which are the
/// instance's whatever unit asks. A template reading a name no mode exposes
/// fails both renders alike and is left to the mint to report.
fn refuse_per_unit_credentials(
    auth: &AuthSpec,
    mode: AuthKind,
    instance_ctx: &TemplateCtx,
    endpoints: &[BoundEndpoint],
) -> Result<()> {
    let templates = auth.credential_templates(mode);
    if templates.is_empty() {
        return Ok(());
    }
    let stand_in = Value::Object(
        auth.exposed_names(mode)
            .into_iter()
            .map(|name| (name.to_owned(), Value::String(format!("auth.{name}"))))
            .collect(),
    );
    for (field, source) in templates {
        let template =
            Template::compile(source).map_err(|e| Error::Config(format!("{field}: {e}")))?;
        let instance = render_with_auth(&template, instance_ctx, &stand_in);
        for endpoint in endpoints {
            let unit = render_with_auth(&template, &endpoint.ctx, &stand_in);
            let same = match (&instance, &unit) {
                (Ok(instance), Ok(unit)) => instance == unit,
                (Err(_), Err(_)) => true,
                _ => false,
            };
            if !same {
                return Err(Error::Config(format!(
                    "{field}: renders differently for unit `{}` than for the instance; a \
                     credential is minted once per instance and per scope, so a value only a \
                     unit supplies cannot decide what it mints",
                    endpoint.unit.name
                )));
            }
        }
    }
    Ok(())
}

/// Render `template` against `ctx` with `auth` standing in for what a mode
/// exposes.
fn render_with_auth(template: &Template, ctx: &TemplateCtx, auth: &Value) -> Result<String> {
    let mut ctx = ctx.clone();
    ctx.set("auth", auth.clone());
    template.render(&ctx)
}

/// Compile a secondary request's templates; `field` names it in errors and
/// `default` is the method it sends unless it names one.
fn bind_request(field: &str, request: &LookupRequest, default: Method) -> Result<BoundRequest> {
    let mut query = TemplateMap::new();
    compile_map(&format!("{field}.query"), &request.query, &mut query)?;
    let mut headers = TemplateMap::new();
    compile_map(&format!("{field}.headers"), &request.headers, &mut headers)?;
    Ok(BoundRequest {
        method: request.method_or(default),
        path: Template::compile(&request.path)
            .map_err(|e| Error::Config(format!("{field}.path: {e}")))?,
        query,
        headers,
        body: request.body.clone(),
        ignore_status: request.ignore_status.clone(),
        timeout: request.timeout_secs.map(std::time::Duration::from_secs),
    })
}

/// Resolve the instance's profile reference against the shipped profiles.
///
/// # Errors
///
/// Returns [`Error::Config`] when a named profile is unknown or an inline one
/// does not parse.
pub fn resolve_profile<'a>(
    instance: &'a RestInstance,
    shipped: &'a BTreeMap<String, RestProfile>,
) -> Result<std::borrow::Cow<'a, RestProfile>> {
    match &instance.profile {
        ProfileRef::Inline(profile) => Ok(std::borrow::Cow::Borrowed(profile)),
        ProfileRef::Named(name) if name.is_empty() => Err(Error::Config(
            "profile is required: a shipped profile name or an inline profile".into(),
        )),
        ProfileRef::Named(name) => shipped
            .get(name)
            .map(std::borrow::Cow::Borrowed)
            .ok_or_else(|| {
                Error::Config(format!(
                    "profile `{name}` is not a shipped profile (known: {})",
                    shipped.keys().cloned().collect::<Vec<_>>().join(", ")
                ))
            }),
    }
}

/// Bind `profile` to `instance` as connection `connection_id`.
///
/// Runs both validations first, so the error names the field.
///
/// # Errors
///
/// Returns [`Error::Config`] carrying every validation issue, or the first
/// compile or render failure with its field.
pub fn bind(
    profile: &RestProfile,
    instance: &RestInstance,
    connection_id: &str,
) -> Result<BoundProfile> {
    let mut issues = profile.validate();
    issues.extend(instance.validate(profile));
    if !issues.is_empty() {
        return Err(Error::Config(
            issues
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; "),
        ));
    }

    let mut ctx = TemplateCtx::new();
    let mut vars = profile.vars.clone();
    vars.extend(instance.vars.iter().map(|(k, v)| (k.clone(), v.clone())));
    ctx.set("vars", Value::Object(vars.into_iter().collect()));
    let base_url = Template::compile(&profile.base_url)?
        .render(&ctx)
        .map_err(|e| Error::Config(format!("base_url: {e}")))?;
    if base_url.trim().is_empty() {
        return Err(Error::Config(
            "base_url rendered empty; the instance must set the var it reads".into(),
        ));
    }
    let base_url = base_url.trim_end_matches('/').to_owned();
    ctx.set("base_url", Value::String(base_url.clone()));
    let mut origins = OriginSet::new();
    origins.permit(&base_url);
    permit_allow_hosts(&mut origins, "allow_hosts", &profile.allow_hosts, &ctx)?;

    let mut headers = TemplateMap::new();
    compile_map("headers", &profile.headers, &mut headers)?;

    let probe = match &profile.probe {
        Some(spec) => {
            let mut query = TemplateMap::new();
            compile_map("probe.query", &spec.query, &mut query)?;
            Some(BoundProbe {
                method: spec.method,
                path: Template::compile(&spec.path)
                    .map_err(|e| Error::Config(format!("probe.path: {e}")))?,
                query,
                fail_when: spec
                    .fail_when
                    .as_deref()
                    .map(Predicate::compile)
                    .transpose()
                    .map_err(|e| Error::Config(format!("probe.fail_when: {e}")))?,
            })
        }
        None => None,
    };

    let mut endpoints = Vec::with_capacity(profile.endpoints.len());
    for endpoint in &profile.endpoints {
        if instance
            .units
            .get(&endpoint.unit)
            .is_some_and(|over| !over.enabled)
        {
            continue;
        }
        endpoints.push(bind_endpoint(
            profile,
            instance,
            endpoint,
            &endpoint.unit,
            &ctx,
        )?);
    }
    // The instance's own units: each runs a profile endpoint once more
    // under its name, in name order.
    for (name, over) in &instance.units {
        let Some(template) = &over.endpoint else {
            continue;
        };
        if !over.enabled {
            continue;
        }
        let endpoint = profile
            .endpoints
            .iter()
            .find(|e| e.unit == *template)
            .ok_or_else(|| {
                Error::Config(format!(
                    "units.{name}.endpoint: the profile has no endpoint `{template}`"
                ))
            })?;
        endpoints.push(bind_endpoint(profile, instance, endpoint, name, &ctx)?);
    }
    refuse_per_unit_credentials(&profile.auth, instance.auth.mode, &ctx, &endpoints)?;
    let units = endpoints.iter().map(|e| e.unit.clone()).collect();

    Ok(BoundProfile {
        name: if profile.profile.is_empty() {
            connection_id.to_owned()
        } else {
            profile.profile.clone()
        },
        connection_id: connection_id.to_owned(),
        base_url,
        origins,
        headers,
        window: profile.window.clone(),
        retry: profile.retry.clone(),
        error_at: profile.error.at.clone(),
        quota: profile.quota.clone(),
        auth: profile.auth.clone(),
        probe,
        endpoints,
        units,
        ctx,
        accumulate: instance.accumulate,
        filter: instance.filter.clone(),
        interval_secs: instance.interval_secs,
        topic: instance.topic.clone(),
    })
}

/// A tiny convenience for the units the driver iterates.
impl BoundProfile {
    /// The bound endpoint for a unit name.
    #[must_use]
    pub fn endpoint(&self, unit: &str) -> Option<&BoundEndpoint> {
        self.endpoints.iter().find(|e| &*e.unit.name == unit)
    }

    /// The units as shared specs.
    #[must_use]
    pub fn unit_names(&self) -> Vec<Arc<str>> {
        self.units.iter().map(|u| Arc::clone(&u.name)).collect()
    }
}
