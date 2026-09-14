// Project:   dfe-fetcher
// File:      crates/rest/src/page.rs
// Purpose:   The pager axis: how the next page of a unit is found and requested
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The pager axis.
//!
//! Pagination is data plus one `advance` step the shape runs per page: the
//! [`Pager`] reads the response's headers, its row count, and (for the
//! strategies that need it) the page body as a tree, and answers with the next
//! [`PageState`] or `None`. The next request's templates read the state as
//! `page.token`, `page.number`, `page.page` and `page.offset`; a `request_path`
//! or header cursor replaces the URL outright.

use std::collections::BTreeMap;

use reqwest::header::HeaderMap;
use serde_json::Value;

use dfe_fetcher_core::error::{Error, Result};

use crate::profile::template::{Predicate, TemplateCtx};
use crate::profile::{PagerStrategy, PaginateSpec};

/// Where a cursor or next URL is read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorFrom {
    /// A JSON pointer into the page body.
    Body(String),
    /// A response header.
    Header(String),
}

impl CursorFrom {
    fn parse(text: &str) -> Result<Self> {
        if let Some(pointer) = text.strip_prefix("body:") {
            Ok(CursorFrom::Body(pointer.to_owned()))
        } else if let Some(name) = text.strip_prefix("header:") {
            Ok(CursorFrom::Header(name.to_ascii_lowercase()))
        } else {
            Err(Error::Config(format!(
                "paginate.from `{text}` must be `body:/pointer` or `header:Name`"
            )))
        }
    }

    fn read(&self, meta: &PageMeta<'_>, body: Option<&Value>) -> Result<Option<String>> {
        match self {
            CursorFrom::Body(pointer) => {
                let body = body.ok_or_else(|| {
                    Error::Config(format!(
                        "pager reads `{pointer}` from the body but the page was streamed"
                    ))
                })?;
                Ok(body.pointer(pointer).and_then(scalar_text))
            }
            CursorFrom::Header(name) => Ok(meta
                .headers
                .get(name.as_str())
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)),
        }
    }
}

/// Where the cursor goes on the next request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inject {
    /// A query parameter.
    Query(String),
    /// A JSON pointer into the rendered POST body.
    Body(String),
    /// A JSON pointer into an otherwise EMPTY POST body: the next request
    /// carries the cursor and nothing else.
    BodyReplace(String),
    /// The whole URL.
    Path,
}

impl Inject {
    fn parse(text: &str) -> Result<Self> {
        if let Some(name) = text.strip_prefix("query:") {
            Ok(Inject::Query(name.to_owned()))
        } else if let Some(pointer) = text.strip_prefix("body_replace:/") {
            Ok(Inject::BodyReplace(format!("/{pointer}")))
        } else if let Some(pointer) = text.strip_prefix("body:") {
            Ok(Inject::Body(pointer.to_owned()))
        } else if text == "path" {
            Ok(Inject::Path)
        } else {
            Err(Error::Config(format!(
                "paginate.into `{text}` must be `query:name`, `body:/pointer`, \
                 `body_replace:/pointer` or `path`"
            )))
        }
    }
}

/// The value at a pointer as request text: strings verbatim, numbers as
/// digits, a list of scalars comma-joined (Duo's `[timestamp, id]` offset).
fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Array(items) => {
            let parts: Option<Vec<String>> = items
                .iter()
                .map(|item| match item {
                    Value::Array(_) | Value::Object(_) | Value::Null => None,
                    scalar => scalar_text(scalar),
                })
                .collect();
            parts.filter(|p| !p.is_empty()).map(|p| p.join(","))
        }
        Value::Null | Value::Object(_) => None,
    }
}

/// Where the shape is in a unit's page sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageState {
    /// 0-based index of this page in the sequence.
    pub number: u32,
    /// The cursor token the next request injects, once one has been read.
    pub token: Option<String>,
    /// The next request's full URL, when the pager supplies one.
    pub next_url: Option<String>,
    page: Option<u64>,
    offset: Option<u64>,
}

impl PageState {
    /// The `page_number` value for this page.
    #[must_use]
    pub fn page_number(&self) -> Option<u64> {
        self.page
    }

    /// The `offset` value for this page.
    #[must_use]
    pub fn offset(&self) -> Option<u64> {
        self.offset
    }

    /// What templates read as `page.*`.
    #[must_use]
    pub fn as_json(&self) -> Value {
        let mut map = serde_json::Map::new();
        map.insert("number".into(), Value::from(self.number));
        map.insert(
            "token".into(),
            self.token.clone().map_or(Value::Null, Value::String),
        );
        if let Some(page) = self.page {
            map.insert("page".into(), Value::from(page));
        }
        if let Some(offset) = self.offset {
            map.insert("offset".into(), Value::from(offset));
        }
        if let Some(url) = &self.next_url {
            map.insert("next_url".into(), Value::String(url.clone()));
        }
        Value::Object(map)
    }
}

/// What the pager sees of one response besides the body.
#[derive(Debug, Clone, Copy)]
pub struct PageMeta<'a> {
    headers: &'a HeaderMap,
    /// Rows the decoder framed from this page, when the shape counted them.
    rows: Option<usize>,
    /// The URL the page was fetched from, when the shape kept it.
    url: Option<&'a reqwest::Url>,
}

impl<'a> PageMeta<'a> {
    /// Headers of the response and, when known, its row count.
    #[must_use]
    pub fn new(headers: &'a HeaderMap, rows: Option<usize>) -> Self {
        Self {
            headers,
            rows,
            url: None,
        }
    }

    /// The same, with the URL the page was fetched from, which a relative
    /// `request_path` value is resolved against when no `base` is set.
    #[must_use]
    pub fn at(self, url: Option<&'a reqwest::Url>) -> Self {
        Self { url, ..self }
    }

    /// Headers as a lowercase-name map for CEL predicates.
    #[must_use]
    pub fn headers_json(&self) -> Value {
        let mut map = BTreeMap::new();
        for (name, value) in self.headers {
            if let Ok(text) = value.to_str() {
                map.insert(
                    name.as_str().to_ascii_lowercase(),
                    Value::String(text.to_owned()),
                );
            }
        }
        serde_json::to_value(map).unwrap_or(Value::Null)
    }
}

/// The pagination strategy of one unit, built from its [`PaginateSpec`].
#[derive(Debug)]
pub enum Pager {
    /// One page.
    None,
    /// RFC 5988 `Link: rel="next"`.
    LinkHeader,
    /// A token read from the body or a header and injected into the next request.
    Cursor {
        /// Where the token is read.
        from: CursorFrom,
        /// Where it is written.
        into: Inject,
        /// Stops after this page when true.
        stop_when: Option<Predicate>,
    },
    /// A page-number parameter.
    PageNumber {
        /// The parameter.
        param: String,
        /// The first page's number.
        start: u64,
        /// Pointer to the total page count, when the body carries one.
        total_pages_at: Option<String>,
    },
    /// An offset parameter, advanced by the rows each page carried.
    Offset {
        /// The parameter.
        param: String,
        /// The first offset.
        start: u64,
        /// Rows per page: a shorter page is the last. Required without a
        /// total; with one the total and an empty page end the sequence.
        page_size: Option<u64>,
        /// Pointer to the total row count, when the body carries one.
        total_at: Option<String>,
    },
    /// The next URL read from the body or a header.
    RequestPath {
        /// Where the URL is read.
        from: CursorFrom,
        /// Base a relative URL is resolved against.
        base: Option<String>,
    },
}

impl Pager {
    /// Build from a validated spec.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a required field is missing or malformed.
    pub fn build(spec: &PaginateSpec) -> Result<Self> {
        let missing = |field: &str| {
            Error::Config(format!(
                "paginate strategy needs `{field}`; run the profile through validate()"
            ))
        };
        Ok(match spec.strategy {
            PagerStrategy::None => Pager::None,
            PagerStrategy::LinkHeader => Pager::LinkHeader,
            PagerStrategy::Cursor => Pager::Cursor {
                from: CursorFrom::parse(spec.from.as_deref().ok_or_else(|| missing("from"))?)?,
                into: Inject::parse(spec.into.as_deref().ok_or_else(|| missing("into"))?)?,
                stop_when: spec
                    .stop_when
                    .as_deref()
                    .map(Predicate::compile)
                    .transpose()?,
            },
            PagerStrategy::PageNumber => Pager::PageNumber {
                param: spec.param.clone().ok_or_else(|| missing("param"))?,
                start: spec.start.unwrap_or(0),
                total_pages_at: spec.total_pages_at.clone(),
            },
            PagerStrategy::Offset => {
                if spec.page_size.is_none() && spec.total_at.is_none() {
                    return Err(missing("page_size (or total_at)"));
                }
                Pager::Offset {
                    param: spec.param.clone().ok_or_else(|| missing("param"))?,
                    start: spec.start.unwrap_or(0),
                    page_size: spec.page_size,
                    total_at: spec.total_at.clone(),
                }
            }
            PagerStrategy::RequestPath => Pager::RequestPath {
                from: CursorFrom::parse(spec.from.as_deref().ok_or_else(|| missing("from"))?)?,
                base: spec.base.clone(),
            },
        })
    }

    /// Whether advancing needs the page body as a tree.
    #[must_use]
    pub fn reads_body(&self) -> bool {
        match self {
            Pager::None | Pager::LinkHeader => false,
            Pager::Cursor {
                from, stop_when, ..
            } => {
                matches!(from, CursorFrom::Body(_))
                    || stop_when.as_ref().is_some_and(|p| p.references("body"))
            }
            Pager::PageNumber { total_pages_at, .. } => total_pages_at.is_some(),
            Pager::Offset { total_at, .. } => total_at.is_some(),
            Pager::RequestPath { from, .. } => matches!(from, CursorFrom::Body(_)),
        }
    }

    /// Where the cursor is injected, for the strategies that inject one.
    #[must_use]
    pub fn inject(&self) -> Option<&Inject> {
        match self {
            Pager::Cursor { into, .. } => Some(into),
            _ => None,
        }
    }

    /// The query parameter this page's request carries for a counting pager.
    #[must_use]
    pub fn query_param(&self, page: &PageState) -> Option<(&str, String)> {
        match self {
            Pager::PageNumber { param, .. } => page.page.map(|p| (param.as_str(), p.to_string())),
            Pager::Offset { param, .. } => page.offset.map(|o| (param.as_str(), o.to_string())),
            _ => None,
        }
    }

    /// The first page.
    #[must_use]
    pub fn first(&self) -> PageState {
        PageState {
            number: 0,
            token: None,
            next_url: None,
            page: match self {
                Pager::PageNumber { start, .. } => Some(*start),
                _ => None,
            },
            offset: match self {
                Pager::Offset { start, .. } => Some(*start),
                _ => None,
            },
        }
    }

    /// The page after `page`, or `None` when the sequence ends.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a body-reading strategy is given no body,
    /// a predicate fails, or the provider hands back the token it was just
    /// given (a loop that would otherwise spin to `max_pages`).
    pub fn advance(
        &self,
        page: &PageState,
        meta: &PageMeta<'_>,
        body: Option<&Value>,
    ) -> Result<Option<PageState>> {
        let next_number = page.number + 1;
        match self {
            Pager::None => Ok(None),
            Pager::LinkHeader => Ok(link_next(meta.headers).map(|url| PageState {
                number: next_number,
                token: None,
                next_url: Some(url),
                page: None,
                offset: None,
            })),
            Pager::Cursor {
                from,
                into,
                stop_when,
            } => {
                let Some(token) = from.read(meta, body)?.filter(|t| !t.is_empty()) else {
                    return Ok(None);
                };
                if let Some(stop) = stop_when {
                    let mut ctx = TemplateCtx::new();
                    ctx.set("body", body.cloned().unwrap_or(Value::Null));
                    ctx.set("headers", meta.headers_json());
                    if stop.eval(&ctx)? {
                        return Ok(None);
                    }
                }
                if page.token.as_deref() == Some(token.as_str()) {
                    return Err(Error::Source(format!(
                        "provider returned the cursor it was given (`{token}`); refusing to loop"
                    )));
                }
                Ok(Some(PageState {
                    number: next_number,
                    next_url: matches!(into, Inject::Path).then(|| token.clone()),
                    token: Some(token),
                    page: None,
                    offset: None,
                }))
            }
            Pager::PageNumber {
                total_pages_at,
                start,
                ..
            } => {
                let current = page.page.unwrap_or(*start);
                let next = current + 1;
                let done = match total_pages_at {
                    Some(pointer) => {
                        let total = body
                            .ok_or_else(|| {
                                Error::Config(format!(
                                    "pager reads `{pointer}` from the body but the page was streamed"
                                ))
                            })?
                            .pointer(pointer)
                            .and_then(Value::as_u64);
                        // Pages are numbered from `start`, so the last page is
                        // start + total - 1; a missing total ends the sequence.
                        total.is_none_or(|t| next >= start + t)
                    }
                    None => meta.rows == Some(0),
                };
                Ok((!done).then_some(PageState {
                    number: next_number,
                    token: None,
                    next_url: None,
                    page: Some(next),
                    offset: None,
                }))
            }
            Pager::Offset {
                page_size,
                total_at,
                start,
                ..
            } => {
                let current = page.offset.unwrap_or(*start);
                let received = meta.rows.map(|r| r as u64);
                let next = current
                    + received.or(*page_size).ok_or_else(|| {
                        Error::Config(
                            "offset pager has neither a row count nor a page_size to advance by"
                                .into(),
                        )
                    })?;
                let short = page_size.is_some_and(|size| received.is_some_and(|r| r < size));
                let done = received == Some(0)
                    || short
                    || match total_at {
                        Some(pointer) => {
                            let total = body
                                .ok_or_else(|| {
                                    Error::Config(format!(
                                        "pager reads `{pointer}` from the body but the page was streamed"
                                    ))
                                })?
                                .pointer(pointer)
                                .and_then(Value::as_u64);
                            total.is_none_or(|t| next >= start + t)
                        }
                        None => false,
                    };
                Ok((!done).then_some(PageState {
                    number: next_number,
                    token: None,
                    next_url: None,
                    page: None,
                    offset: Some(next),
                }))
            }
            Pager::RequestPath { from, base } => {
                let Some(url) = from.read(meta, body)?.filter(|u| !u.is_empty()) else {
                    return Ok(None);
                };
                let url = match (
                    url.starts_with("http://") || url.starts_with("https://"),
                    base,
                    meta.url,
                ) {
                    (true, _, _) => url,
                    (false, Some(base), _) => format!(
                        "{}/{}",
                        base.trim_end_matches('/'),
                        url.trim_start_matches('/')
                    ),
                    (false, None, Some(from)) => from
                        .join(&url)
                        .map_err(|e| {
                            Error::Source(format!(
                                "request_path returned `{url}`, which does not resolve against `{from}`: {e}"
                            ))
                        })?
                        .to_string(),
                    (false, None, None) => {
                        return Err(Error::Config(format!(
                            "request_path returned the relative URL `{url}` and no `base` is set"
                        )));
                    }
                };
                if page.next_url.as_deref() == Some(url.as_str()) {
                    return Err(Error::Source(format!(
                        "provider returned the next URL it was given (`{url}`); refusing to loop"
                    )));
                }
                Ok(Some(PageState {
                    number: next_number,
                    token: None,
                    next_url: Some(url),
                    page: None,
                    offset: None,
                }))
            }
        }
    }
}

/// The first `Link` URL whose relation is `next` (RFC 5988), across every
/// `Link` header and both the quoted and unquoted `rel` spellings.
#[must_use]
pub fn link_next(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(reqwest::header::LINK)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .find_map(link_next_in)
}

fn link_next_in(header: &str) -> Option<String> {
    header.split(',').find_map(|part| {
        let (url, params) = part.trim().split_once(';')?;
        let is_next = params.split(';').any(|p| {
            let p = p.trim();
            p == "rel=\"next\"" || p == "rel=next"
        });
        if !is_next {
            return None;
        }
        url.trim()
            .strip_prefix('<')
            .and_then(|s| s.strip_suffix('>'))
            .map(str::to_owned)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Leaked so a `PageMeta` can borrow it for the rest of the test.
    fn headers(pairs: &[(&str, &str)]) -> &'static reqwest::header::HeaderMap {
        let mut h = reqwest::header::HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                reqwest::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                reqwest::header::HeaderValue::from_str(v).unwrap(),
            );
        }
        Box::leak(Box::new(h))
    }

    fn spec(strategy: PagerStrategy) -> PaginateSpec {
        PaginateSpec {
            strategy,
            ..PaginateSpec::default()
        }
    }

    #[test]
    fn none_serves_exactly_one_page() {
        let pager = Pager::build(&spec(PagerStrategy::None)).unwrap();
        let page = pager.first();
        assert_eq!(page.number, 0);
        let meta = PageMeta::new(headers(&[]), None);
        assert!(pager.advance(&page, &meta, None).unwrap().is_none());
        assert!(!pager.reads_body());
    }

    #[test]
    fn link_header_follows_rel_next_and_stops_without_it() {
        let pager = Pager::build(&spec(PagerStrategy::LinkHeader)).unwrap();
        let page = pager.first();
        let meta = PageMeta::new(
            headers(&[(
                "link",
                r#"<https://api.example.com/x?page=2>; rel="next", <https://api.example.com/x?page=9>; rel="last""#,
            )]),
            None,
        );
        let next = pager.advance(&page, &meta, None).unwrap().unwrap();
        assert_eq!(next.number, 1);
        assert_eq!(
            next.next_url.as_deref(),
            Some("https://api.example.com/x?page=2")
        );
        let last = PageMeta::new(
            headers(&[("link", r#"<https://api.example.com/x?page=1>; rel="prev""#)]),
            None,
        );
        assert!(pager.advance(&next, &last, None).unwrap().is_none());
        assert!(
            pager
                .advance(&next, &PageMeta::new(headers(&[]), None), None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn link_header_accepts_unquoted_rel_and_multiple_headers() {
        let pager = Pager::build(&spec(PagerStrategy::LinkHeader)).unwrap();
        let meta = PageMeta::new(
            headers(&[
                ("link", "<https://a.example/prev>; rel=prev"),
                ("link", "<https://a.example/next>; rel=next"),
            ]),
            None,
        );
        let next = pager.advance(&pager.first(), &meta, None).unwrap().unwrap();
        assert_eq!(next.next_url.as_deref(), Some("https://a.example/next"));
    }

    #[test]
    fn link_header_ignores_an_empty_value_and_a_url_without_brackets() {
        let pager = Pager::build(&spec(PagerStrategy::LinkHeader)).unwrap();
        for value in ["", r#"https://api.example.com/x?page=2; rel="next""#] {
            let meta = PageMeta::new(headers(&[("link", value)]), None);
            assert!(
                pager
                    .advance(&pager.first(), &meta, None)
                    .unwrap()
                    .is_none(),
                "{value:?}"
            );
        }
    }

    #[test]
    fn cursor_in_body_stops_on_an_empty_token_even_on_a_full_page() {
        let pager = Pager::build(&PaginateSpec {
            from: Some("body:/next_key".into()),
            into: Some("query:start_key".into()),
            stop_when: Some("body.next_key == ''".into()),
            ..spec(PagerStrategy::Cursor)
        })
        .unwrap();
        assert!(pager.reads_body());
        let page = pager.first();
        let meta = PageMeta::new(headers(&[]), None);
        let full = json!({"assets": [1, 2, 3, 4, 5], "next_key": "k2"});
        let next = pager.advance(&page, &meta, Some(&full)).unwrap().unwrap();
        assert_eq!(next.token.as_deref(), Some("k2"));
        assert_eq!(next.number, 1);
        let terminal = json!({"assets": [1, 2, 3, 4, 5], "next_key": ""});
        assert!(
            pager
                .advance(&next, &meta, Some(&terminal))
                .unwrap()
                .is_none()
        );
        let absent = json!({"assets": []});
        assert!(
            pager
                .advance(&next, &meta, Some(&absent))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn cursor_without_stop_when_stops_only_on_a_missing_or_empty_token() {
        let pager = Pager::build(&PaginateSpec {
            from: Some("body:/response_metadata/next_cursor".into()),
            into: Some("query:cursor".into()),
            ..spec(PagerStrategy::Cursor)
        })
        .unwrap();
        let meta = PageMeta::new(headers(&[]), None);
        let body = json!({"entries": [], "response_metadata": {"next_cursor": "abc"}});
        let next = pager
            .advance(&pager.first(), &meta, Some(&body))
            .unwrap()
            .unwrap();
        assert_eq!(next.token.as_deref(), Some("abc"));
        let done = json!({"entries": [], "response_metadata": {"next_cursor": ""}});
        assert!(pager.advance(&next, &meta, Some(&done)).unwrap().is_none());
    }

    /// Duo's v2 logs answer `next_offset` as a two-element array (a
    /// timestamp and an offset id) and take it back comma-joined; a list of
    /// scalars is the joined token, and an empty list ends the sequence.
    #[test]
    fn a_cursor_that_is_a_list_of_scalars_is_sent_comma_joined() {
        let pager = Pager::build(&PaginateSpec {
            from: Some("body:/response/metadata/next_offset".into()),
            into: Some("query:next_offset".into()),
            ..spec(PagerStrategy::Cursor)
        })
        .unwrap();
        let meta = PageMeta::new(headers(&[]), None);
        let body = json!({"response": {"metadata": {"next_offset": ["1532951895000", "af0ba235-0b33-23c8-bc23-a31aa0231de8"]}}});
        let next = pager
            .advance(&pager.first(), &meta, Some(&body))
            .unwrap()
            .unwrap();
        assert_eq!(
            next.token.as_deref(),
            Some("1532951895000,af0ba235-0b33-23c8-bc23-a31aa0231de8")
        );
        let last = json!({"response": {"metadata": {"next_offset": null}}});
        assert!(pager.advance(&next, &meta, Some(&last)).unwrap().is_none());
        let empty = json!({"response": {"metadata": {"next_offset": []}}});
        assert!(pager.advance(&next, &meta, Some(&empty)).unwrap().is_none());
        let nested = json!({"response": {"metadata": {"next_offset": [{"a": 1}]}}});
        assert!(
            pager
                .advance(&next, &meta, Some(&nested))
                .unwrap()
                .is_none(),
            "a list of objects is no cursor"
        );
    }

    #[test]
    fn a_cursor_from_a_header_needs_no_body() {
        let pager = Pager::build(&PaginateSpec {
            from: Some("header:NextPageUri".into()),
            into: Some("path".into()),
            ..spec(PagerStrategy::Cursor)
        })
        .unwrap();
        assert!(!pager.reads_body());
        let meta = PageMeta::new(headers(&[("nextpageuri", "https://m.example/next")]), None);
        let next = pager.advance(&pager.first(), &meta, None).unwrap().unwrap();
        assert_eq!(next.next_url.as_deref(), Some("https://m.example/next"));
    }

    #[test]
    fn a_numeric_cursor_token_renders_as_its_digits() {
        let pager = Pager::build(&PaginateSpec {
            from: Some("body:/response/metadata/next_offset".into()),
            into: Some("query:next_offset".into()),
            ..spec(PagerStrategy::Cursor)
        })
        .unwrap();
        let meta = PageMeta::new(headers(&[]), None);
        let body = json!({"response": {"metadata": {"next_offset": 1_700_000_000_123u64}}});
        let next = pager
            .advance(&pager.first(), &meta, Some(&body))
            .unwrap()
            .unwrap();
        assert_eq!(next.token.as_deref(), Some("1700000000123"));
    }

    #[test]
    fn page_number_counts_from_start_and_honours_total_pages() {
        let pager = Pager::build(&PaginateSpec {
            param: Some("page".into()),
            start: Some(1),
            total_pages_at: Some("/result_info/total_pages".into()),
            ..spec(PagerStrategy::PageNumber)
        })
        .unwrap();
        let first = pager.first();
        assert_eq!(first.page_number(), Some(1));
        let meta = PageMeta::new(headers(&[]), None);
        let body = json!({"result": [], "result_info": {"total_pages": 2}});
        let second = pager.advance(&first, &meta, Some(&body)).unwrap().unwrap();
        assert_eq!(second.page_number(), Some(2));
        assert!(
            pager
                .advance(&second, &meta, Some(&body))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn page_number_without_a_total_runs_until_max_pages_or_an_empty_page() {
        let pager = Pager::build(&PaginateSpec {
            param: Some("page".into()),
            ..spec(PagerStrategy::PageNumber)
        })
        .unwrap();
        assert!(!pager.reads_body());
        let meta = PageMeta::new(headers(&[]), Some(3));
        let next = pager.advance(&pager.first(), &meta, None).unwrap().unwrap();
        assert_eq!(next.page_number(), Some(1), "start defaults to 0");
        let empty = PageMeta::new(headers(&[]), Some(0));
        assert!(
            pager.advance(&next, &empty, None).unwrap().is_none(),
            "an empty page ends it"
        );
    }

    #[test]
    fn offset_advances_by_page_size_and_stops_at_the_total() {
        let pager = Pager::build(&PaginateSpec {
            param: Some("offset".into()),
            page_size: Some(100),
            total_at: Some("/meta/pagination/total".into()),
            ..spec(PagerStrategy::Offset)
        })
        .unwrap();
        let first = pager.first();
        assert_eq!(first.offset(), Some(0));
        let meta = PageMeta::new(headers(&[]), Some(100));
        let body = json!({"meta": {"pagination": {"total": 250}}});
        let second = pager.advance(&first, &meta, Some(&body)).unwrap().unwrap();
        assert_eq!(second.offset(), Some(100));
        let third = pager.advance(&second, &meta, Some(&body)).unwrap().unwrap();
        assert_eq!(third.offset(), Some(200));
        assert!(pager.advance(&third, &meta, Some(&body)).unwrap().is_none());
    }

    /// The offset advances by the rows the page actually carried, so a
    /// provider that answers fewer than `limit` on a non-final page is not
    /// skipped past; with a total in the body `page_size` is not needed at
    /// all (CrowdStrike: `offset` = ids collected so far, stop at
    /// `meta.pagination.total` or on an empty page).
    #[test]
    fn offset_advances_by_the_rows_received_and_page_size_is_optional_with_a_total() {
        let pager = Pager::build(&PaginateSpec {
            param: Some("offset".into()),
            total_at: Some("/meta/pagination/total".into()),
            ..spec(PagerStrategy::Offset)
        })
        .unwrap();
        let body = json!({"meta": {"pagination": {"total": 5}}});
        let first = pager.first();
        let second = pager
            .advance(&first, &PageMeta::new(headers(&[]), Some(2)), Some(&body))
            .unwrap()
            .unwrap();
        assert_eq!(second.offset(), Some(2));
        let third = pager
            .advance(&second, &PageMeta::new(headers(&[]), Some(1)), Some(&body))
            .unwrap()
            .unwrap();
        assert_eq!(
            third.offset(),
            Some(3),
            "a short page moves by what it held"
        );
        assert!(
            pager
                .advance(&third, &PageMeta::new(headers(&[]), Some(0)), Some(&body))
                .unwrap()
                .is_none(),
            "an empty page ends it before the total"
        );
        let fourth = pager
            .advance(&third, &PageMeta::new(headers(&[]), Some(2)), Some(&body))
            .unwrap();
        assert!(fourth.is_none(), "3 + 2 reaches the total of 5");

        assert!(
            Pager::build(&PaginateSpec {
                param: Some("offset".into()),
                ..spec(PagerStrategy::Offset)
            })
            .is_err(),
            "without a total the page size is what detects the last page"
        );
    }

    #[test]
    fn offset_without_a_total_stops_on_a_short_page() {
        let pager = Pager::build(&PaginateSpec {
            param: Some("offset".into()),
            page_size: Some(100),
            ..spec(PagerStrategy::Offset)
        })
        .unwrap();
        let full = PageMeta::new(headers(&[]), Some(100));
        let next = pager.advance(&pager.first(), &full, None).unwrap().unwrap();
        assert_eq!(next.offset(), Some(100));
        let short = PageMeta::new(headers(&[]), Some(31));
        assert!(pager.advance(&next, &short, None).unwrap().is_none());
    }

    #[test]
    fn request_path_takes_the_next_url_from_the_body_and_resolves_relative_ones() {
        let pager = Pager::build(&PaginateSpec {
            from: Some("body:/nextRecordsUrl".into()),
            base: Some("https://sf.example".into()),
            ..spec(PagerStrategy::RequestPath)
        })
        .unwrap();
        let meta = PageMeta::new(headers(&[]), None);
        let body = json!({"done": false, "nextRecordsUrl": "/services/data/v60.0/query/01g-2000"});
        let next = pager
            .advance(&pager.first(), &meta, Some(&body))
            .unwrap()
            .unwrap();
        assert_eq!(
            next.next_url.as_deref(),
            Some("https://sf.example/services/data/v60.0/query/01g-2000")
        );
        let absolute = json!({"@odata.nextLink": "https://graph.example/v1.0/x?$skiptoken=7"});
        let odata = Pager::build(&PaginateSpec {
            from: Some("body:/@odata.nextLink".into()),
            ..spec(PagerStrategy::RequestPath)
        })
        .unwrap();
        let next = odata
            .advance(&odata.first(), &meta, Some(&absolute))
            .unwrap()
            .unwrap();
        assert_eq!(
            next.next_url.as_deref(),
            Some("https://graph.example/v1.0/x?$skiptoken=7")
        );
        assert!(
            odata
                .advance(&next, &meta, Some(&json!({"value": []})))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn the_page_state_exposes_what_templates_can_read() {
        let pager = Pager::build(&PaginateSpec {
            from: Some("body:/next".into()),
            into: Some("query:cursor".into()),
            ..spec(PagerStrategy::Cursor)
        })
        .unwrap();
        let meta = PageMeta::new(headers(&[]), None);
        let next = pager
            .advance(&pager.first(), &meta, Some(&json!({"next": "t1"})))
            .unwrap()
            .unwrap();
        let value = next.as_json();
        assert_eq!(value["token"], "t1");
        assert_eq!(value["number"], 1);
        assert_eq!(pager.inject(), Some(&Inject::Query("cursor".into())));
    }

    /// A provider whose next request is the cursor ALONE (1Password: the
    /// window body on the first call, `{cursor}` on the rest) declares
    /// `into: body_replace:/cursor`; the shape builds that body from nothing.
    #[test]
    fn a_body_replace_injection_is_its_own_destination() {
        let pager = Pager::build(&PaginateSpec {
            from: Some("body:/cursor".into()),
            into: Some("body_replace:/cursor".into()),
            stop_when: Some("body.has_more == false".into()),
            ..spec(PagerStrategy::Cursor)
        })
        .unwrap();
        assert_eq!(pager.inject(), Some(&Inject::BodyReplace("/cursor".into())));
        let meta = PageMeta::new(headers(&[]), None);
        let more = json!({"items": [], "cursor": "c1", "has_more": true});
        let next = pager
            .advance(&pager.first(), &meta, Some(&more))
            .unwrap()
            .unwrap();
        assert_eq!(next.token.as_deref(), Some("c1"));
        let last = json!({"items": [], "cursor": "c2", "has_more": false});
        assert!(
            pager.advance(&next, &meta, Some(&last)).unwrap().is_none(),
            "a cursor on the last page does not continue when has_more is false"
        );
        assert!(Inject::parse("body_replace:cursor").is_err(), "a pointer");
        assert!(Inject::parse("replace:/cursor").is_err());
    }

    #[test]
    fn a_body_reading_pager_without_a_body_is_an_error_not_a_silent_stop() {
        let pager = Pager::build(&PaginateSpec {
            from: Some("body:/next".into()),
            into: Some("query:cursor".into()),
            ..spec(PagerStrategy::Cursor)
        })
        .unwrap();
        let meta = PageMeta::new(headers(&[]), None);
        assert!(pager.advance(&pager.first(), &meta, None).is_err());
    }

    #[test]
    fn a_page_that_repeats_its_own_token_is_refused() {
        let pager = Pager::build(&PaginateSpec {
            from: Some("body:/next".into()),
            into: Some("query:cursor".into()),
            ..spec(PagerStrategy::Cursor)
        })
        .unwrap();
        let meta = PageMeta::new(headers(&[]), None);
        let next = pager
            .advance(&pager.first(), &meta, Some(&json!({"next": "same"})))
            .unwrap()
            .unwrap();
        let err = pager
            .advance(&next, &meta, Some(&json!({"next": "same"})))
            .unwrap_err();
        assert!(
            err.to_string().contains("same"),
            "a provider loop must not spin until max_pages: {err}"
        );
    }
}
