// Project:   dfe-fetcher
// File:      crates/core/src/rules.rs
// Purpose:   Compiled per-row rules: CEL keep-filter, routes, added fields
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Compiled per-row rules.
//!
//! The per-source CEL `filter`, the deployment's `output.routes` and a unit's
//! `add_fields` all need the row as a tree, so they share ONE
//! `serde_json::Value` parse per row and none when nothing is configured. A
//! dropped row never reaches the batcher, so it never takes a memory lease and
//! never gets a `seq`.
//!
//! Filter semantics are the fetcher's existing contract: fail-OPEN on shape (a
//! payload that is not a JSON object cannot be filtered and passes) and
//! fail-CLOSED on evaluation (a missing field, a type mismatch or a non-boolean
//! result drops the row). Programs compile once, at load and on every reload.

use std::sync::Arc;

use bytes::Bytes;
use cel::Program;
use serde_json::{Map, Value};

use crate::error::{Error, Result};

/// One `output.routes` rule: rows whose `field` equals `value` go to
/// `destinations` instead of the default transports. First match wins.
#[derive(Debug, Clone)]
pub struct Route {
    /// Top-level field, or a dotted path into nested objects.
    pub field: String,
    /// The string the field must equal.
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

/// What the rules decided for one row.
#[derive(Debug)]
pub enum Verdict {
    /// The filter rejected the row; nothing downstream sees it.
    Drop,
    /// The row goes on, with the destinations a route chose.
    Keep {
        /// The row's bytes, re-serialised only when fields were added.
        payload: Bytes,
        /// Named destinations, or `None` for the default transports.
        route: Option<Arc<[Arc<str>]>>,
    },
}

/// The compiled rules of one source: keep-filter, routes.
#[derive(Debug)]
pub struct RowRules {
    filter: Option<(String, Program)>,
    routes: Vec<Route>,
}

impl RowRules {
    /// Rules that keep every row unchanged.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            filter: None,
            routes: Vec::new(),
        }
    }

    /// Compile the filter under scalo's expression profile and keep the routes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Filter`] when the expression does not compile or uses a
    /// function the profile blocks.
    pub fn compile(filter: Option<&str>, routes: Vec<Route>) -> Result<Self> {
        let filter = match filter.map(str::trim).filter(|f| !f.is_empty()) {
            Some(expr) => {
                let program = scalo::expression::compile(expr)
                    .map_err(|e| Error::Filter(format!("filter `{expr}` rejected: {e}")))?;
                Some((expr.to_owned(), program))
            }
            None => None,
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
        self.filter.is_none() && self.routes.is_empty()
    }

    /// Decide one row. `add_fields` are the unit's added fields, spliced into
    /// the object's bytes before the filter and the routes see it.
    ///
    /// SHORTCUT: one full `serde_json::Value` parse per row whenever a filter
    /// or route is configured; lift to referenced-field extraction
    /// (`program.references().variables()` + a slice getter) when a filtered
    /// unit sustains more than ~20k rows/s.
    #[must_use]
    pub fn apply(&self, payload: Bytes, add_fields: &[(String, Value)]) -> Verdict {
        let payload = splice_fields(payload, add_fields);
        if self.is_noop() {
            return Verdict::Keep {
                payload,
                route: None,
            };
        }
        let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&payload) else {
            return Verdict::Keep {
                payload,
                route: None,
            };
        };
        if let Some((_, program)) = &self.filter
            && !keep(program, &map)
        {
            return Verdict::Drop;
        }
        let route = self
            .routes
            .iter()
            .find(|route| field_at(&map, &route.field).is_some_and(|v| v == route.value))
            .map(|route| Arc::clone(&route.destinations));
        Verdict::Keep { payload, route }
    }
}

/// Append `fields` to a JSON object's bytes, before its closing brace, with
/// no parse: the row's own bytes are copied as they are. A payload that is
/// not an object (an array, a scalar, not JSON) is returned unchanged.
///
/// A key the row ALREADY carries is not appended a second time: two top-level
/// keys of the same name make the loader's ClickHouse JSON column reject the
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

/// The string at `path` (top-level key, or dotted into nested objects).
fn field_at<'a>(map: &'a Map<String, Value>, path: &str) -> Option<&'a str> {
    if let Some(value) = map.get(path) {
        return value.as_str();
    }
    let mut node: &Value = map.get(path.split('.').next()?)?;
    for key in path.split('.').skip(1) {
        node = node.get(key)?;
    }
    node.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn keep(verdict: Verdict) -> (Bytes, Option<Arc<[Arc<str>]>>) {
        match verdict {
            Verdict::Keep { payload, route } => (payload, route),
            Verdict::Drop => panic!("expected the row to be kept"),
        }
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
    fn routes_pick_the_first_matching_destination_set_from_the_same_parse() {
        let routes = vec![
            Route::new("severity", "critical", ["pager", "siem"]),
            Route::new("record.kind", "asset", ["inventory"]),
        ];
        let rules = RowRules::compile(None, routes).unwrap();
        let (_, route) = keep(rules.apply(Bytes::from_static(br#"{"severity":"critical"}"#), &[]));
        let names: Vec<&str> = route.as_deref().unwrap().iter().map(|s| &**s).collect();
        assert_eq!(names, ["pager", "siem"]);
        let (_, route) = keep(rules.apply(
            Bytes::from_static(br#"{"severity":"low","record":{"kind":"asset"}}"#),
            &[],
        ));
        assert_eq!(
            route.as_deref().unwrap().len(),
            1,
            "dotted paths reach into the record"
        );
        let (_, route) = keep(rules.apply(Bytes::from_static(br#"{"severity":"low"}"#), &[]));
        assert!(route.is_none());
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
    /// ClickHouse rejects a record with a duplicate top-level key outright.
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
