// Project:   dfe-fetcher
// File:      crates/rest/src/profile/template.rs
// Purpose:   `{{ cel }}` templates: compiled once, rendered per request
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `{{ cel }}` templates.
//!
//! A profile string is a literal with zero or more `{{ expr }}` holes; each hole
//! is a CEL expression compiled once at load under scalo's expression profile
//! and evaluated per REQUEST (never per row) over a context of `vars`,
//! `window`, `page`, `key`, `item`, `auth`, `unit` and `base_url`. CEL has no
//! time formatting, so the window strings arrive pre-formatted; every profile
//! in the survey needs only string concatenation.

use std::fmt::Write as _;

use cel::{Context, Program};
use serde_json::{Map, Value};

use dfe_fetcher_core::error::{Error, Result};

/// The variables a render sees, as one JSON object per top-level name.
#[derive(Debug, Default, Clone)]
pub struct TemplateCtx {
    vars: Map<String, Value>,
}

impl TemplateCtx {
    /// An empty context.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set (or replace) a top-level variable.
    pub fn set(&mut self, name: &str, value: Value) {
        self.vars.insert(name.to_owned(), value);
    }

    /// Read a top-level variable.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.vars.get(name)
    }

    fn cel(&self) -> Result<Context<'_>> {
        scalo::expression::build_context(&self.vars)
            .map_err(|e| Error::Config(format!("template context: {e}")))
    }
}

/// One compiled `{{ expr }}`, kept with its source for error messages.
#[derive(Debug)]
struct Expr {
    source: String,
    program: Program,
}

impl Expr {
    fn compile(source: &str) -> Result<Self> {
        let source = source.trim();
        if source.is_empty() {
            return Err(Error::Config(
                "template has an empty `{{ }}` expression".into(),
            ));
        }
        let program = scalo::expression::compile(source)
            .map_err(|e| Error::Config(format!("template expression `{source}` rejected: {e}")))?;
        Ok(Self {
            source: source.to_owned(),
            program,
        })
    }

    fn eval(&self, ctx: &TemplateCtx) -> Result<cel::Value> {
        self.program.execute(&ctx.cel()?).map_err(|e| {
            Error::Config(format!(
                "template expression `{}` failed to evaluate: {e}",
                self.source
            ))
        })
    }

    fn references(&self, var: &str) -> bool {
        self.program.references().has_variable(var)
    }
}

#[derive(Debug)]
enum Part {
    Literal(String),
    Expr(Expr),
}

/// A profile string with compiled `{{ }}` holes.
#[derive(Debug)]
pub struct Template {
    parts: Vec<Part>,
}

impl Template {
    /// Compile `text`, splitting literal runs from `{{ expr }}` holes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] for an unterminated `{{`, an empty hole, or an
    /// expression the CEL profile rejects.
    pub fn compile(text: &str) -> Result<Self> {
        let mut parts = Vec::new();
        let mut rest = text;
        while let Some(open) = rest.find("{{") {
            if open > 0 {
                parts.push(Part::Literal(rest[..open].to_owned()));
            }
            let after = &rest[open + 2..];
            let Some(close) = after.find("}}") else {
                return Err(Error::Config(format!(
                    "template `{text}` has an unterminated `{{{{`"
                )));
            };
            parts.push(Part::Expr(Expr::compile(&after[..close])?));
            rest = &after[close + 2..];
        }
        if !rest.is_empty() || parts.is_empty() {
            parts.push(Part::Literal(rest.to_owned()));
        }
        Ok(Self { parts })
    }

    /// Whether the template has no holes.
    #[must_use]
    pub fn is_literal(&self) -> bool {
        self.parts.iter().all(|p| matches!(p, Part::Literal(_)))
    }

    /// The literal text when the template has no holes.
    #[must_use]
    pub fn literal(&self) -> Option<&str> {
        match self.parts.as_slice() {
            [Part::Literal(s)] => Some(s),
            _ => None,
        }
    }

    /// Whether any hole reads the top-level variable `var`.
    #[must_use]
    pub fn references(&self, var: &str) -> bool {
        self.parts.iter().any(|p| match p {
            Part::Expr(e) => e.references(var),
            Part::Literal(_) => false,
        })
    }

    /// Render against a context.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] naming the expression when a hole fails to
    /// evaluate (a missing variable is an error, never a blank).
    pub fn render(&self, ctx: &TemplateCtx) -> Result<String> {
        let mut out = String::new();
        for part in &self.parts {
            match part {
                Part::Literal(s) => out.push_str(s),
                Part::Expr(e) => write_value(&mut out, &e.eval(ctx)?)?,
            }
        }
        Ok(out)
    }

    /// Render as a JSON value: a template that is exactly one `{{ expr }}`
    /// keeps the expression's type (a number, a list), anything else is the
    /// rendered string. This is how a request body leaf stays typed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a hole fails to evaluate or the value
    /// has no JSON form.
    pub fn render_value(&self, ctx: &TemplateCtx) -> Result<Value> {
        match self.parts.as_slice() {
            [Part::Expr(e)] => e
                .eval(ctx)?
                .json()
                .map_err(|e| Error::Config(format!("template value is not renderable: {e}"))),
            _ => self.render(ctx).map(Value::String),
        }
    }
}

/// Append a CEL value as request text: strings verbatim, scalars as written,
/// null as nothing, lists and maps as JSON.
fn write_value(out: &mut String, value: &cel::Value) -> Result<()> {
    match value {
        cel::Value::String(s) => out.push_str(s),
        cel::Value::Null => {}
        cel::Value::Int(n) => {
            let _ = write!(out, "{n}");
        }
        cel::Value::UInt(n) => {
            let _ = write!(out, "{n}");
        }
        cel::Value::Float(f) => {
            let _ = write!(out, "{f}");
        }
        cel::Value::Bool(b) => {
            let _ = write!(out, "{b}");
        }
        other => {
            let json = other
                .json()
                .map_err(|e| Error::Config(format!("template value is not renderable: {e}")))?;
            out.push_str(&json.to_string());
        }
    }
    Ok(())
}

/// A compiled CEL predicate over a response (`stop_when`, `fail_when`).
#[derive(Debug)]
pub struct Predicate {
    expr: Expr,
}

impl Predicate {
    /// Compile a bare CEL expression (no `{{ }}` around it).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the expression does not compile.
    pub fn compile(source: &str) -> Result<Self> {
        let source = source
            .trim()
            .strip_prefix("{{")
            .and_then(|s| s.strip_suffix("}}"))
            .unwrap_or(source);
        Ok(Self {
            expr: Expr::compile(source)?,
        })
    }

    /// Whether the predicate reads the top-level variable `var`.
    #[must_use]
    pub fn references(&self, var: &str) -> bool {
        self.expr.references(var)
    }

    /// Evaluate to a boolean.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the expression fails to evaluate or does
    /// not yield a boolean.
    pub fn eval(&self, ctx: &TemplateCtx) -> Result<bool> {
        match self.expr.eval(ctx)? {
            cel::Value::Bool(b) => Ok(b),
            other => Err(Error::Config(format!(
                "predicate `{}` yielded {other:?}, not a boolean",
                self.expr.source
            ))),
        }
    }
}

/// Ordered `name -> template` pairs (query parameters, headers).
///
/// A value that renders to the empty string is omitted, which is how a profile
/// says "send `_oid` only when the instance sets one".
#[derive(Debug, Default)]
pub struct TemplateMap {
    entries: Vec<(String, Template)>,
}

impl TemplateMap {
    /// An empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Compile and append one entry.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the value does not compile.
    pub fn insert(&mut self, name: &str, value: &str) -> Result<()> {
        self.entries
            .push((name.to_owned(), Template::compile(value)?));
        Ok(())
    }

    /// Whether any entry has a hole.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether any entry reads `var`.
    #[must_use]
    pub fn references(&self, var: &str) -> bool {
        self.entries.iter().any(|(_, t)| t.references(var))
    }

    /// Render every entry, dropping the ones that render empty.
    ///
    /// # Errors
    ///
    /// Returns the first entry's render error.
    pub fn render(&self, ctx: &TemplateCtx) -> Result<Vec<(String, String)>> {
        let mut out = Vec::with_capacity(self.entries.len());
        for (name, template) in &self.entries {
            let value = template.render(ctx)?;
            if !value.is_empty() {
                out.push((name.clone(), value));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx() -> TemplateCtx {
        let mut ctx = TemplateCtx::new();
        ctx.set(
            "vars",
            json!({"org": "acme", "page_size": 100, "empty": ""}),
        );
        ctx.set(
            "window",
            json!({"start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"}),
        );
        ctx.set("page", json!({"token": "abc", "number": 3}));
        ctx
    }

    #[test]
    fn a_literal_stays_literal_and_never_evaluates() {
        let t = Template::compile("/orgs/audit-log").unwrap();
        assert!(t.is_literal());
        assert_eq!(t.literal(), Some("/orgs/audit-log"));
        assert_eq!(t.render(&ctx()).unwrap(), "/orgs/audit-log");
        assert!(!t.references("vars"));
    }

    #[test]
    fn expressions_render_strings_numbers_and_concatenation() {
        let t = Template::compile("/orgs/{{ vars.org }}/audit-log?per_page={{ vars.page_size }}")
            .unwrap();
        assert!(!t.is_literal());
        assert_eq!(
            t.render(&ctx()).unwrap(),
            "/orgs/acme/audit-log?per_page=100"
        );
        assert!(t.references("vars"));
        assert!(!t.references("window"));
    }

    #[test]
    fn the_window_and_page_are_plain_strings_ready_for_concatenation() {
        let t = Template::compile("created:{{ window.start }}..{{ window.end }}").unwrap();
        assert_eq!(
            t.render(&ctx()).unwrap(),
            "created:2026-01-01T00:00:00Z..2026-01-02T00:00:00Z"
        );
        let t = Template::compile("{{ page.token }}").unwrap();
        assert_eq!(t.render(&ctx()).unwrap(), "abc");
        let t = Template::compile("{{ page.number + 1 }}").unwrap();
        assert_eq!(t.render(&ctx()).unwrap(), "4");
    }

    #[test]
    fn null_and_empty_render_to_empty_so_the_caller_can_omit_the_value() {
        let t = Template::compile("{{ vars.empty }}").unwrap();
        assert_eq!(t.render(&ctx()).unwrap(), "");
        let mut ctx = ctx();
        ctx.set("vars", json!({"org_id": null}));
        let t = Template::compile("{{ vars.org_id }}").unwrap();
        assert_eq!(t.render(&ctx).unwrap(), "");
    }

    #[test]
    fn a_missing_variable_is_a_render_error_not_a_silent_blank() {
        let t = Template::compile("{{ vars.nope }}").unwrap();
        let err = t.render(&ctx()).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
        assert!(
            err.to_string().contains("vars.nope"),
            "names the expression: {err}"
        );
    }

    #[test]
    fn unbalanced_braces_and_bad_cel_fail_at_compile() {
        assert!(Template::compile("{{ vars.org").is_err());
        assert!(Template::compile("{{ vars.org ==  }}").is_err());
        let err = Template::compile("{{ }}").unwrap_err();
        assert!(err.to_string().contains("empty"), "{err}");
    }

    /// A body leaf that is exactly one expression keeps the expression's
    /// type -- a number stays a number, a list a list -- while a leaf that
    /// mixes text and holes is a string.
    #[test]
    fn a_single_expression_renders_as_its_typed_value() {
        let mut ctx = ctx();
        ctx.set("ids", json!(["a", "b"]));
        assert_eq!(
            Template::compile("{{ vars.page_size }}")
                .unwrap()
                .render_value(&ctx)
                .unwrap(),
            json!(100)
        );
        assert_eq!(
            Template::compile("{{ ids }}")
                .unwrap()
                .render_value(&ctx)
                .unwrap(),
            json!(["a", "b"])
        );
        assert_eq!(
            Template::compile("{{ vars.org }}")
                .unwrap()
                .render_value(&ctx)
                .unwrap(),
            json!("acme")
        );
        assert_eq!(
            Template::compile(" {{ vars.page_size }}")
                .unwrap()
                .render_value(&ctx)
                .unwrap(),
            json!(" 100"),
            "text around the hole makes it a string"
        );
        assert_eq!(
            Template::compile("literal")
                .unwrap()
                .render_value(&ctx)
                .unwrap(),
            json!("literal")
        );
    }

    #[test]
    fn lists_and_maps_render_as_json() {
        let mut ctx = ctx();
        ctx.set("ids", json!(["a", "b"]));
        let t = Template::compile("{{ ids }}").unwrap();
        assert_eq!(t.render(&ctx).unwrap(), r#"["a","b"]"#);
    }

    #[test]
    fn predicates_evaluate_to_bool_and_reject_non_bool() {
        let p = Predicate::compile("body.next_key == ''").unwrap();
        let mut ctx = TemplateCtx::new();
        ctx.set("body", json!({"next_key": ""}));
        assert!(p.eval(&ctx).unwrap());
        ctx.set("body", json!({"next_key": "abc"}));
        assert!(!p.eval(&ctx).unwrap());
        ctx.set("body", json!({}));
        assert!(
            p.eval(&ctx).is_err(),
            "a missing field is an error the caller surfaces"
        );
        let not_bool = Predicate::compile("body.next_key").unwrap();
        ctx.set("body", json!({"next_key": "abc"}));
        assert!(not_bool.eval(&ctx).is_err());
        assert!(p.references("body"));
        assert!(!p.references("headers"));
    }

    #[test]
    fn a_template_map_omits_keys_that_render_empty() {
        let mut params = TemplateMap::new();
        params.insert("org", "{{ vars.org }}").unwrap();
        params.insert("_oid", "{{ vars.empty }}").unwrap();
        params.insert("fixed", "1").unwrap();
        let rendered = params.render(&ctx()).unwrap();
        assert_eq!(
            rendered,
            vec![
                ("org".to_string(), "acme".to_string()),
                ("fixed".to_string(), "1".to_string()),
            ]
        );
    }
}
