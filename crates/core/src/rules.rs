// Project:   dfe-fetcher
// File:      crates/core/src/rules.rs
// Purpose:   Compiled per-row rules: CEL keep-filter, routes, added fields
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Compiled per-row rules.
//!
//! The per-source CEL `filter` needs the row as a tree, so a source with a
//! filter takes ONE `serde_json::Value` parse per row and its routes read that
//! tree. The deployment's `output.routes` alone read the row's bytes lazily
//! through scalo's rule set, and a row with neither is not parsed at all. A
//! unit's `add_fields` are spliced into the bytes before either runs. A
//! dropped row never reaches the batcher, so it never takes a memory lease and
//! never gets a `seq`.
//!
//! Filter semantics are the fetcher's existing contract: fail-OPEN on shape (a
//! payload that is not a JSON object cannot be filtered and passes) and
//! fail-CLOSED on evaluation (a missing field, a type mismatch or a non-boolean
//! result drops the row). Programs and routes compile once, at load and on
//! every reload.

use std::sync::Arc;

use bytes::Bytes;
use cel::Program;
use scalo::rules::{
    CompileOptions, FieldSource, JsonPayload, MatchConfig, RuleConfig, RuleDiagnostic, RuleSet,
    RuleSetConfig, SetAction, TargetId,
};
use serde_json::{Map, Value};

use crate::error::{Error, Result};

/// The `rule_set` label on the routes' `rules_hits_total` series.
pub const ROUTES_RULE_SET: &str = "output.routes";

/// The one target every route competes for, set to the matched route's
/// position, so first match is decided across every route in declared order.
const ROUTE_TARGET: &str = "route";

/// One `output.routes` rule: rows whose `field` equals `value` go to
/// `destinations` instead of the default transports. First match wins.
#[derive(Debug, Clone)]
pub struct Route {
    /// A dotted path: a top-level key, or `a.b` for the key `b` inside `a`.
    pub field: String,
    /// The text the field must equal: a string as written, a number or a
    /// boolean as its JSON text.
    pub value: String,
    /// Named destinations.
    pub destinations: Arc<[Arc<str>]>,
}

impl Route {
    /// A route on `field == value` to the named destinations.
    #[must_use]
    pub fn new<I, S>(field: &str, value: &str, destinations: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            field: field.to_owned(),
            value: value.to_owned(),
            destinations: destinations
                .into_iter()
                .map(|s| Arc::from(s.as_ref()))
                .collect(),
        }
    }
}

/// Where a matched route sends a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routed {
    /// The route's position in `output.routes`.
    pub index: usize,
    /// Named destinations.
    pub destinations: Arc<[Arc<str>]>,
    /// The `_source` the row takes: the destination, when the route names one.
    /// A fan-out names no single source, so its rows keep their own.
    pub source: Option<Arc<str>>,
}

/// What the rules decided for one row.
#[derive(Debug)]
pub enum Verdict {
    /// The filter rejected the row; nothing downstream sees it.
    Drop,
    /// The row goes on, with the route that matched it.
    Keep {
        /// The row's bytes, re-serialised only when fields were added.
        payload: Bytes,
        /// The matched route, or `None` for the default transports.
        route: Option<Routed>,
    },
}

/// The routes compiled into one scalo rule set.
#[derive(Debug)]
struct Routes {
    set: RuleSet,
    target: TargetId,
    /// One per route, in declared order.
    routed: Vec<Routed>,
}

impl Routes {
    /// The first route in declared order whose field equals its value.
    fn pick<R: FieldSource + ?Sized>(&self, record: &R) -> Option<Routed> {
        let outcome = self.set.evaluate(record);
        let index: usize = outcome.value(self.target)?.parse().ok()?;
        self.routed.get(index).cloned()
    }
}

/// What a route's rule id starts with: `route-<n>` is `output.routes[n]`, the
/// `rule` label on its `rules_hits_total` series.
const ROUTE_ID_PREFIX: &str = "route-";

/// The position in `output.routes` of the route a rule id names.
#[must_use]
pub fn route_index(rule_id: &str) -> Option<usize> {
    rule_id.strip_prefix(ROUTE_ID_PREFIX)?.parse().ok()
}

/// The scalo rule set `routes` compile to: one row per route, in declared
/// order, each setting the route target to its own position.
fn route_rule_set(routes: &[Route]) -> RuleSetConfig {
    routes.iter().enumerate().fold(
        RuleSetConfig::new(ROUTES_RULE_SET, [ROUTE_TARGET]),
        |set, (index, route)| {
            set.rule(
                RuleConfig::new(
                    format!("{ROUTE_ID_PREFIX}{index}"),
                    ROUTE_TARGET,
                    SetAction::value(index.to_string()),
                )
                .matching(MatchConfig::equals(&route.field, &route.value)),
            )
        },
    )
}

/// Every finding scalo's rule set check reports for `routes`: an error stops
/// them compiling, a warning names a route an earlier one always shadows.
#[must_use]
pub fn check_routes(routes: &[Route]) -> Vec<RuleDiagnostic> {
    RuleSet::check(&route_rule_set(routes), &CompileOptions::default())
}

/// The compiled rules of one source: keep-filter, routes.
#[derive(Debug)]
pub struct RowRules {
    filter: Option<(String, Program)>,
    routes: Option<Routes>,
}

impl RowRules {
    /// Rules that keep every row unchanged.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            filter: None,
            routes: None,
        }
    }

    /// Compile the filter under scalo's expression profile and the routes into
    /// a scalo rule set.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Filter`] when the expression does not compile or uses a
    /// function the profile blocks, and [`Error::Config`] when a route's field
    /// is not a dotted path.
    pub fn compile(filter: Option<&str>, routes: Vec<Route>) -> Result<Self> {
        let filter = match filter.map(str::trim).filter(|f| !f.is_empty()) {
            Some(expr) => {
                let program = scalo::expression::compile(expr)
                    .map_err(|e| Error::Filter(format!("filter `{expr}` rejected: {e}")))?;
                Some((expr.to_owned(), program))
            }
            None => None,
        };
        let routes = if routes.is_empty() {
            None
        } else {
            let set = RuleSet::compile_with(&route_rule_set(&routes), &CompileOptions::default())
                .map_err(|e| Error::Config(format!("output.routes: {e}")))?;
            let target = set.target(ROUTE_TARGET).ok_or_else(|| {
                Error::Config("output.routes: the compiled rule set has no route target".into())
            })?;
            Some(Routes {
                set,
                target,
                routed: routes
                    .into_iter()
                    .enumerate()
                    .map(|(index, route)| Routed {
                        index,
                        source: match &*route.destinations {
                            [one] => Some(Arc::clone(one)),
                            _ => None,
                        },
                        destinations: route.destinations,
                    })
                    .collect(),
            })
        };
        Ok(Self { filter, routes })
    }

    /// The filter's source text, for the hot-reload comparison.
    #[must_use]
    pub fn filter_expr(&self) -> Option<&str> {
        self.filter.as_ref().map(|(expr, _)| expr.as_str())
    }

    /// Whether these rules never parse a row.
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.filter.is_none() && self.routes.is_none()
    }

    /// Every route, in declared order.
    #[must_use]
    pub fn routes(&self) -> &[Routed] {
        self.routes.as_ref().map_or(&[], |r| r.routed.as_slice())
    }

    /// Decide one row. `add_fields` are the unit's added fields, spliced into
    /// the object's bytes before the filter and the routes see it.
    ///
    /// SHORTCUT: one full `serde_json::Value` parse per row whenever a filter
    /// is configured, lifted to referenced-field extraction
    /// (`program.references().variables()` + a slice getter) when a filtered
    /// unit sustains more than ~20k rows/s.
    #[must_use]
    pub fn apply(&self, payload: Bytes, add_fields: &[(String, Value)]) -> Verdict {
        let payload = splice_fields(payload, add_fields);
        let Some((_, program)) = &self.filter else {
            // A payload past scalo's depth bound is read by no route.
            let route = self.routes.as_ref().and_then(|routes| {
                JsonPayload::new(&payload).and_then(|record| routes.pick(&record))
            });
            return Verdict::Keep { payload, route };
        };
        let parsed = serde_json::from_slice::<Value>(&payload).ok();
        let Some(map) = parsed.as_ref().and_then(Value::as_object) else {
            return Verdict::Keep {
                payload,
                route: None,
            };
        };
        if !keep(program, map) {
            return Verdict::Drop;
        }
        let route = self
            .routes
            .as_ref()
            .zip(parsed.as_ref())
            .and_then(|(routes, record)| routes.pick(record));
        Verdict::Keep { payload, route }
    }
}

/// Append `fields` to a JSON object's bytes, before its closing brace, with
/// no parse: the row's own bytes are copied as they are. A payload that is
/// not an object (an array, a scalar, not JSON) is returned unchanged.
///
/// A key the row ALREADY carries is not appended a second time: two top-level
/// keys of the same name make the loader's `ClickHouse` JSON column reject the
/// whole record, and a rejected record is not dead-lettered -- it is lost. On
/// a possible collision the row is parsed once and the value it arrived with
/// is parked under `<key>_original` (the next free `<key>_original_<n>` when
/// that name is taken too), so the added value takes the name and every key
/// appears exactly once.
#[must_use]
pub fn splice_fields(payload: Bytes, fields: &[(String, Value)]) -> Bytes {
    if fields.is_empty() {
        return payload;
    }
    let Some(open) = payload.iter().position(|b| !b.is_ascii_whitespace()) else {
        return payload;
    };
    let Some(close) = payload.iter().rposition(|b| !b.is_ascii_whitespace()) else {
        return payload;
    };
    if payload[open] != b'{' || payload[close] != b'}' || close <= open {
        return payload;
    }
    if fields.iter().any(|(key, _)| may_carry(&payload, key)) {
        if let Some(rebuilt) = add_without_duplicates(&payload, fields) {
            return rebuilt;
        }
        return payload;
    }
    let empty = payload[open + 1..close].iter().all(u8::is_ascii_whitespace);
    let mut out = Vec::with_capacity(payload.len() + fields.len() * 32);
    out.extend_from_slice(&payload[..close]);
    for (i, (key, value)) in fields.iter().enumerate() {
        if i > 0 || !empty {
            out.push(b',');
        }
        // A string key and a JSON value always serialise.
        let _ = serde_json::to_writer(&mut out, key);
        out.push(b':');
        let _ = serde_json::to_writer(&mut out, value);
    }
    out.extend_from_slice(&payload[close..]);
    Bytes::from(out)
}

/// Whether `key` appears in `payload` as a quoted JSON name anywhere. A nested
/// field or a string value matches too: a false positive costs the parse path,
/// never correctness.
fn may_carry(payload: &[u8], key: &str) -> bool {
    let needle = format!("\"{key}\"");
    memchr::memmem::find(payload, needle.as_bytes()).is_some()
}

/// Add `fields` to a parsed row so no key appears twice, parking each value the
/// row arrived with. `None` when the bytes are not a JSON object after all.
fn add_without_duplicates(payload: &[u8], fields: &[(String, Value)]) -> Option<Bytes> {
    let Value::Object(mut map) = serde_json::from_slice(payload).ok()? else {
        return None;
    };
    for (key, value) in fields {
        if let Some(existing) = map.remove(key) {
            park_original(&mut map, key, existing);
        }
        map.insert(key.clone(), value.clone());
    }
    serde_json::to_vec(&map).ok().map(Bytes::from)
}

/// Park the value the row arrived with beside the added one: `<key>_original`,
/// or the next free `<key>_original_<n>` when that name is taken as well, so a
/// row enriched twice never loses the value it came with.
fn park_original(map: &mut Map<String, Value>, key: &str, value: Value) {
    let preferred = format!("{key}_original");
    if !map.contains_key(&preferred) {
        map.insert(preferred, value);
        return;
    }
    let mut n = 2u32;
    loop {
        let parked = format!("{preferred}_{n}");
        if !map.contains_key(&parked) {
            map.insert(parked, value);
            return;
        }
        n += 1;
    }
}

/// The filter decision over a parsed object: a boolean as itself, a number by
/// non-zero, anything else and every evaluation error as a drop.
fn keep(program: &Program, map: &Map<String, Value>) -> bool {
    let Ok(context) = scalo::expression::build_context(map) else {
        return false;
    };
    match program.execute(&context) {
        Ok(cel::Value::Bool(b)) => b,
        Ok(cel::Value::Int(n)) => n != 0,
        Ok(cel::Value::UInt(n)) => n != 0,
        Ok(cel::Value::Float(f)) => f != 0.0,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn keep(verdict: Verdict) -> (Bytes, Option<Routed>) {
        match verdict {
            Verdict::Keep { payload, route } => (payload, route),
            Verdict::Drop => panic!("expected the row to be kept"),
        }
    }

    fn names(route: &Routed) -> Vec<&str> {
        route.destinations.iter().map(|s| &**s).collect()
    }

    fn rules(filter: &str) -> RowRules {
        RowRules::compile(Some(filter), Vec::new()).unwrap()
    }

    #[test]
    fn a_bad_filter_is_rejected_at_compile_not_per_row() {
        let err = RowRules::compile(Some("severity =="), Vec::new()).unwrap_err();
        assert!(matches!(err, Error::Filter(_)), "got {err:?}");
        let profile = RowRules::compile(Some(r#"name.matches("^a")"#), Vec::new()).unwrap_err();
        assert!(
            profile.to_string().contains("matches"),
            "the scalo profile rejects regex by default: {profile}"
        );
    }

    #[test]
    fn no_rules_keeps_the_bytes_untouched_without_parsing() {
        let rules = RowRules::empty();
        assert!(rules.is_noop());
        let raw = Bytes::from_static(b"this is not even json");
        let (payload, route) = keep(rules.apply(raw.clone(), &[]));
        assert_eq!(payload, raw);
        assert!(route.is_none());
    }

    #[test]
    fn filter_keeps_matching_rows_and_drops_the_rest() {
        let rules = rules(r#"severity == "high""#);
        assert!(matches!(
            rules.apply(Bytes::from_static(br#"{"severity":"high"}"#), &[]),
            Verdict::Keep { .. }
        ));
        assert!(matches!(
            rules.apply(Bytes::from_static(br#"{"severity":"low"}"#), &[]),
            Verdict::Drop
        ));
    }

    #[test]
    fn filter_fails_closed_on_a_missing_field_or_type_mismatch() {
        let rules = rules("count > 5");
        assert!(
            matches!(
                rules.apply(Bytes::from_static(br#"{"other":1}"#), &[]),
                Verdict::Drop
            ),
            "missing field drops"
        );
        assert!(
            matches!(
                rules.apply(Bytes::from_static(br#"{"count":"five"}"#), &[]),
                Verdict::Drop
            ),
            "type mismatch drops"
        );
        assert!(matches!(
            rules.apply(Bytes::from_static(br#"{"count":6}"#), &[]),
            Verdict::Keep { .. }
        ));
    }

    #[test]
    fn filter_fails_open_on_a_payload_that_is_not_an_object() {
        let rules = rules(r#"severity == "high""#);
        let (payload, _) = keep(rules.apply(Bytes::from_static(b"[1,2,3]"), &[]));
        assert_eq!(payload, Bytes::from_static(b"[1,2,3]"));
        let (payload, _) = keep(rules.apply(Bytes::from_static(b"not json"), &[]));
        assert_eq!(payload, Bytes::from_static(b"not json"));
    }

    #[test]
    fn non_boolean_results_coerce_like_the_pipeline_filter() {
        assert!(matches!(
            rules("count").apply(Bytes::from_static(br#"{"count":3}"#), &[]),
            Verdict::Keep { .. }
        ));
        assert!(matches!(
            rules("count").apply(Bytes::from_static(br#"{"count":0}"#), &[]),
            Verdict::Drop
        ));
        assert!(
            matches!(
                rules("name").apply(Bytes::from_static(br#"{"name":"x"}"#), &[]),
                Verdict::Drop
            ),
            "a string result is not truthy"
        );
    }

    #[test]
    fn the_filter_sees_nested_fields_of_an_enveloped_row() {
        let rules = rules("record.alive == true && kind == \"row\"");
        let row = br#"{"kind":"row","seq":3,"record":{"alive":true,"id":"a"}}"#;
        assert!(matches!(
            rules.apply(Bytes::from_static(row), &[]),
            Verdict::Keep { .. }
        ));
        let dead = br#"{"kind":"row","seq":4,"record":{"alive":false}}"#;
        assert!(matches!(
            rules.apply(Bytes::from_static(dead), &[]),
            Verdict::Drop
        ));
    }

    #[test]
    fn a_kept_row_without_added_fields_keeps_the_providers_bytes() {
        let rules = rules("true");
        let raw = Bytes::from_static(br#"{"z":1,  "a":2}"#);
        let (payload, _) = keep(rules.apply(raw.clone(), &[]));
        assert_eq!(
            payload, raw,
            "one parse for the decision, no re-serialisation"
        );
    }

    #[test]
    fn routes_pick_the_first_matching_destination_set() {
        let routes = vec![
            Route::new("severity", "critical", ["pager", "siem"]),
            Route::new("record.kind", "asset", ["inventory"]),
        ];
        let rules = RowRules::compile(None, routes).unwrap();
        let (_, route) = keep(rules.apply(Bytes::from_static(br#"{"severity":"critical"}"#), &[]));
        let route = route.unwrap();
        assert_eq!(names(&route), ["pager", "siem"]);
        assert_eq!(route.index, 0);
        let (_, route) = keep(rules.apply(
            Bytes::from_static(br#"{"severity":"low","record":{"kind":"asset"}}"#),
            &[],
        ));
        assert_eq!(
            names(&route.unwrap()),
            ["inventory"],
            "dotted paths reach into the record"
        );
        let (_, route) = keep(rules.apply(Bytes::from_static(br#"{"severity":"low"}"#), &[]));
        assert!(route.is_none());
    }

    /// A route naming one destination names the source its rows belong to. A
    /// fan-out names several, so its rows keep the source they came with.
    #[test]
    fn a_route_to_one_destination_names_its_source_and_a_fan_out_names_none() {
        let rules = RowRules::compile(
            None,
            vec![
                Route::new("kind", "asset", ["inventory"]),
                Route::new("kind", "alert", ["pager", "siem"]),
            ],
        )
        .unwrap();
        let routes = rules.routes();
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].source.as_deref(), Some("inventory"));
        assert_eq!(routes[1].source, None);
        let (_, route) = keep(rules.apply(Bytes::from_static(br#"{"kind":"alert"}"#), &[]));
        assert_eq!(route.as_ref(), Some(&routes[1]));
        assert_eq!(RowRules::empty().routes(), []);
    }

    /// With a filter the routes read the tree the filter parsed. A dropped row
    /// takes no route, and a kept one takes the first that matches.
    #[test]
    fn routes_behind_a_filter_read_the_filtered_tree() {
        let rules = RowRules::compile(
            Some(r#"severity != "debug""#),
            vec![Route::new("severity", "high", ["siem"])],
        )
        .unwrap();
        assert!(matches!(
            rules.apply(Bytes::from_static(br#"{"severity":"debug"}"#), &[]),
            Verdict::Drop
        ));
        let (_, route) = keep(rules.apply(Bytes::from_static(br#"{"severity":"high"}"#), &[]));
        assert_eq!(names(&route.unwrap()), ["siem"]);
        let (_, route) = keep(rules.apply(Bytes::from_static(br#"{"severity":"low"}"#), &[]));
        assert!(route.is_none());
    }

    /// Routes alone read the bytes without a parse, so a payload that is not a
    /// JSON object, or one nested past scalo's depth bound, takes no route and
    /// keeps its bytes.
    #[test]
    fn routes_alone_leave_a_payload_they_cannot_read_unrouted() {
        let rules =
            RowRules::compile(None, vec![Route::new("kind", "asset", ["inventory"])]).unwrap();
        for raw in [&b"not json"[..], b"[1,2]", b"\"kind\""] {
            let (payload, route) = keep(rules.apply(Bytes::copy_from_slice(raw), &[]));
            assert_eq!(&payload[..], raw);
            assert!(route.is_none(), "{raw:?}");
        }
        let depth = scalo::parse_guard::MAX_PARSE_DEPTH + 1;
        let deep = format!(
            r#"{{"kind":"asset","x":{}{}}}"#,
            "[".repeat(depth),
            "]".repeat(depth)
        );
        let (_, route) = keep(rules.apply(Bytes::from(deep), &[]));
        assert!(route.is_none(), "past the depth bound no route reads it");
    }

    /// A route reads a field as scalo's rule set does: a number or a boolean
    /// as its JSON text, so `5` and `true` match the route values "5" and
    /// "true".
    #[test]
    fn a_route_on_a_number_or_a_boolean_matches_its_json_text() {
        let rules = RowRules::compile(
            None,
            vec![
                Route::new("code", "5", ["five"]),
                Route::new("live", "true", ["live"]),
            ],
        )
        .unwrap();
        assert!(matches!(
            rules.apply(Bytes::from_static(br#"{"code":5}"#), &[]),
            Verdict::Keep { route: Some(_), .. }
        ));
        assert!(matches!(
            rules.apply(Bytes::from_static(br#"{"live":true}"#), &[]),
            Verdict::Keep { route: Some(_), .. }
        ));
        assert!(matches!(
            rules.apply(Bytes::from_static(br#"{"code":6}"#), &[]),
            Verdict::Keep { route: None, .. }
        ));
    }

    /// A route whose field is not a dotted path could never match, so it is
    /// refused when the routes compile rather than matching nothing for ever.
    #[test]
    fn a_route_field_that_is_no_dotted_path_is_refused_at_compile() {
        for field in ["", "kind.", "a..b"] {
            assert!(
                RowRules::compile(None, vec![Route::new(field, "x", ["inventory"])]).is_err(),
                "{field:?}"
            );
        }
    }

    /// A route that shadows an earlier one is a warning, and a field that is no
    /// dotted path is an error, both from scalo's own check.
    #[test]
    fn check_routes_reports_a_shadowed_route_and_a_bad_field() {
        let shadowed = check_routes(&[
            Route::new("kind", "asset", ["inventory"]),
            Route::new("kind", "asset", ["archive"]),
        ]);
        assert_eq!(shadowed.len(), 1, "{shadowed:?}");
        assert!(!shadowed[0].is_error());
        assert_eq!(shadowed[0].rule.as_deref(), Some("route-1"));

        let bad = check_routes(&[Route::new("kind.", "asset", ["inventory"])]);
        assert!(bad.iter().any(RuleDiagnostic::is_error), "{bad:?}");
        assert_eq!(check_routes(&[]), []);
    }

    #[test]
    fn added_fields_are_written_into_the_object_and_visible_to_the_filter() {
        let rules = rules(r#"_dfe_fetcher_package == "requests""#);
        let added = [("_dfe_fetcher_package".to_string(), json!("requests"))];
        let (payload, _) = keep(rules.apply(Bytes::from_static(br#"{"info":{}}"#), &added));
        let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(value["_dfe_fetcher_package"], "requests");
        assert_eq!(value["info"], json!({}));
    }

    /// Fields are spliced into the object's bytes -- no parse, the row's own
    /// bytes untouched -- and an empty object, surrounding whitespace, a
    /// nested closing brace and a non-object payload are all handled.
    #[test]
    fn splice_appends_fields_to_an_object_without_parsing_it() {
        let fields = [
            ("_dfe_fetcher_package".to_string(), json!("requests")),
            ("n".to_string(), json!(7)),
        ];
        let spliced = splice_fields(Bytes::from_static(br#"{"info":{"a":1}}"#), &fields);
        assert_eq!(
            &spliced[..],
            br#"{"info":{"a":1},"_dfe_fetcher_package":"requests","n":7}"#
        );
        assert_eq!(
            &splice_fields(Bytes::from_static(b"{}"), &fields)[..],
            br#"{"_dfe_fetcher_package":"requests","n":7}"#,
            "no leading comma in an empty object"
        );
        assert_eq!(
            &splice_fields(Bytes::from_static(b" { } \n"), &fields)[..],
            b" { \"_dfe_fetcher_package\":\"requests\",\"n\":7} \n",
            "whitespace around and inside is kept in place"
        );
        assert_eq!(
            &splice_fields(Bytes::from_static(br#"{"s":"}"}"#), &fields[..1])[..],
            br#"{"s":"}","_dfe_fetcher_package":"requests"}"#,
            "a brace inside a string is not the closing brace"
        );
        for not_object in [&b"[1,2]"[..], b"\"text\"", b"", b"not json"] {
            assert_eq!(
                &splice_fields(Bytes::copy_from_slice(not_object), &fields)[..],
                not_object,
                "left alone"
            );
        }
        let untouched = Bytes::from_static(br#"{"a":1}"#);
        assert_eq!(splice_fields(untouched.clone(), &[]), untouched);
        let value: serde_json::Value = serde_json::from_slice(&spliced).unwrap();
        assert_eq!(value["n"], 7, "the result is valid JSON");
    }

    /// An added field whose key the row already carries replaces it and parks
    /// the row's own value, rather than appending a second copy of the key:
    /// `ClickHouse` rejects a record with a duplicate top-level key outright.
    #[test]
    fn an_added_field_the_row_already_carries_parks_the_original_instead_of_duplicating() {
        let fields = [("_dfe_fetcher_object".to_string(), json!({"key": "ours"}))];
        let spliced = splice_fields(
            Bytes::from_static(br#"{"id":1,"_dfe_fetcher_object":{"key":"theirs"}}"#),
            &fields,
        );
        let text = String::from_utf8(spliced.to_vec()).unwrap();
        assert_eq!(
            text.matches(r#""_dfe_fetcher_object""#).count(),
            1,
            "one copy of the key: {text}"
        );
        let value: serde_json::Value = serde_json::from_slice(&spliced).unwrap();
        assert_eq!(value["_dfe_fetcher_object"], json!({"key": "ours"}));
        assert_eq!(
            value["_dfe_fetcher_object_original"],
            json!({"key": "theirs"})
        );
        assert_eq!(value["id"], 1, "the rest of the row is untouched");

        // A row that carries the parked name too keeps it and numbers the next.
        let again = splice_fields(
            Bytes::from_static(
                br#"{"_dfe_fetcher_object":"theirs","_dfe_fetcher_object_original":"first"}"#,
            ),
            &fields,
        );
        let value: serde_json::Value = serde_json::from_slice(&again).unwrap();
        assert_eq!(value["_dfe_fetcher_object_original"], "first");
        assert_eq!(value["_dfe_fetcher_object_original_2"], "theirs");

        // A key that only LOOKS present (a nested field, a string value) takes
        // the parse path and still lands exactly once.
        let nested = splice_fields(
            Bytes::from_static(br#"{"outer":{"_dfe_fetcher_object":1}}"#),
            &fields,
        );
        let value: serde_json::Value = serde_json::from_slice(&nested).unwrap();
        assert_eq!(value["_dfe_fetcher_object"], json!({"key": "ours"}));
        assert_eq!(value["outer"]["_dfe_fetcher_object"], 1);
        assert!(value.get("_dfe_fetcher_object_original").is_none());
    }

    #[test]
    fn compiled_filter_reports_its_source_for_hot_reload_comparison() {
        let rules = rules("a == 1");
        assert_eq!(rules.filter_expr(), Some("a == 1"));
        assert!(!rules.is_noop());
        assert!(RowRules::compile(None, Vec::new()).unwrap().is_noop());
    }
}
