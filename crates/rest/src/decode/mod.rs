// Project:   dfe-fetcher
// File:      crates/rest/src/decode/mod.rs
// Purpose:   The decoder axis: framing a response body into rows, streaming where the format allows
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The decoder axis.
//!
//! A [`Decoder`] frames a response body into rows. NDJSON, JSON arrays and
//! plain lines stream: the body is pulled chunk by chunk and a row is yielded
//! as soon as its boundary is seen, so an unpolled body stalls the TCP window
//! and memory is bounded by one row. `json_at`, `document` and `csv` are
//! page-bounded: the body is read whole under `max_page_bytes` and rows are
//! slices of that one buffer. `gzip` inflates a member sequence in front of any
//! of them. Rows are the provider's bytes; nothing here parses a row. The
//! framers themselves are the framework's ([`dfe_fetcher_core::frame`]); this
//! module decides which one a profile's `rows` spec selects and feeds it the
//! response body.

use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use serde_json::value::RawValue;

use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::frame::scan::trim_line;
use dfe_fetcher_core::frame::{
    ArrayFramer, Framer, LeasedBlock, LineFramer, csv_rows, framed, split_array, split_lines,
};

use crate::profile::{DecoderKind, RowsSpec};

/// A response body as it arrives.
pub type ByteStream<'a> = BoxStream<'a, Result<Bytes>>;
/// Rows framed from a body.
pub type RowBytes<'a> = BoxStream<'a, Result<Bytes>>;

/// Framing of a response body into rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decoder {
    /// One JSON object per line, streamed.
    Ndjson,
    /// A top-level JSON array, streamed element by element.
    JsonArray,
    /// A wrapped page read whole; rows at this JSON pointer.
    ///
    /// SHORTCUT: the page is buffered up to `max_page_bytes`; a pointer-aware
    /// streaming framer lifts that when a provider's single page exceeds
    /// 16 MiB with no `.jsonl` or array form.
    JsonAt(String),
    /// The whole body is one row.
    Document,
    /// A JSON document read whole: a top-level array is one row per
    /// element, anything else is one row.
    Json,
    /// Plain text lines, each wrapped as `{"line": ...}`.
    Lines,
    /// CSV read whole, one object per record.
    Csv {
        /// The first record names the columns.
        header: bool,
    },
    /// A gzip member sequence inflated in front of the inner decoder.
    Gzip(Box<Decoder>),
    /// The inner decoder's rows are JSON strings whose content is the row
    /// (AWS Config's `Results`); each is unquoted.
    Quoted(Box<Decoder>),
}

impl Decoder {
    /// Build from the grammar's row spec.
    #[must_use]
    pub fn build(spec: &RowsSpec) -> Self {
        let mut decoder = match spec.decoder {
            DecoderKind::JsonArray => Decoder::JsonArray,
            DecoderKind::Ndjson => Decoder::Ndjson,
            DecoderKind::JsonAt => Decoder::JsonAt(spec.at.clone().unwrap_or_default()),
            DecoderKind::Document => Decoder::Document,
            DecoderKind::Json => Decoder::Json,
            DecoderKind::Lines => Decoder::Lines,
            DecoderKind::Csv => Decoder::Csv {
                header: spec.header,
            },
        };
        if spec.gzip {
            decoder = Decoder::Gzip(Box::new(decoder));
        }
        if spec.quoted {
            decoder = Decoder::Quoted(Box::new(decoder));
        }
        decoder
    }

    /// Whether framing needs the whole body in memory.
    #[must_use]
    pub fn is_page_bounded(&self) -> bool {
        match self {
            Decoder::Ndjson | Decoder::JsonArray | Decoder::Lines => false,
            Decoder::JsonAt(_) | Decoder::Document | Decoder::Json | Decoder::Csv { .. } => true,
            Decoder::Gzip(inner) | Decoder::Quoted(inner) => inner.is_page_bounded(),
        }
    }

    /// Frame a body as it arrives. Streaming decoders yield rows per chunk,
    /// with one open row bounded by `max_page_bytes`; page-bounded ones read
    /// the body whole under the same bound first.
    #[must_use]
    pub fn frame<'a>(&'a self, body: ByteStream<'a>, max_page_bytes: usize) -> RowBytes<'a> {
        match self {
            Decoder::Ndjson => stream_framed(body, LineFramer::new(false).bounded(max_page_bytes)),
            Decoder::Lines => stream_framed(body, LineFramer::new(true).bounded(max_page_bytes)),
            Decoder::JsonArray => {
                stream_framed(body, ArrayFramer::default().bounded(max_page_bytes))
            }
            Decoder::Gzip(inner) => inner.frame(gunzip(body), max_page_bytes),
            Decoder::Quoted(inner) => inner
                .frame(body, max_page_bytes)
                .map(|row| row.and_then(|row| unquote(&row)))
                .boxed(),
            Decoder::JsonAt(_) | Decoder::Document | Decoder::Json | Decoder::Csv { .. } => {
                let page = async move {
                    let page = collect_page(body, max_page_bytes).await?;
                    self.frame_page(&page)
                };
                futures::stream::once(page)
                    .map(|rows| match rows {
                        Ok(rows) => futures::stream::iter(rows.into_iter().map(Ok)).boxed(),
                        Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
                    })
                    .flatten()
                    .boxed()
            }
        }
    }

    /// Frame a page the caller already holds; rows are slices of `page`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Decode`] when the page does not have the shape the
    /// decoder expects (not an array at the pointer, malformed CSV, a gzip
    /// decoder, which cannot frame an already-buffered page).
    pub fn frame_page(&self, page: &Bytes) -> Result<Vec<Bytes>> {
        match self {
            Decoder::Ndjson => Ok(split_lines(page, false)),
            Decoder::Lines => Ok(split_lines(page, true)),
            Decoder::JsonArray => split_array(page, 0, page.len()),
            Decoder::JsonAt(pointer) => match raw_at(page, pointer)? {
                Some((start, end)) => split_array(page, start, end),
                None => Ok(Vec::new()),
            },
            Decoder::Document => Ok(if trim_line(page).is_empty() {
                Vec::new()
            } else {
                vec![page.clone()]
            }),
            Decoder::Json => {
                let trimmed = trim_line(page);
                match trimmed.first() {
                    None => Ok(Vec::new()),
                    Some(b'[') => split_array(page, 0, page.len()),
                    Some(_) => Ok(vec![page.clone()]),
                }
            }
            Decoder::Csv { header } => csv_rows(page, *header),
            Decoder::Gzip(_) => Err(Error::Decode(
                "gzip frames a body stream, not a buffered page".into(),
            )),
            Decoder::Quoted(inner) => inner
                .frame_page(page)?
                .iter()
                .map(|row| unquote(row))
                .collect(),
        }
    }
}

/// The content of a framed row that is a JSON string.
fn unquote(row: &[u8]) -> Result<Bytes> {
    serde_json::from_slice::<String>(row)
        .map(Bytes::from)
        .map_err(|e| Error::Decode(format!("quoted row is not a JSON string: {e}")))
}

/// Read a body whole, refusing one larger than `max`.
///
/// # Errors
///
/// Returns [`Error::OversizePage`] once the body passes `max` bytes, or the
/// body stream's own error.
pub async fn collect_page(mut body: ByteStream<'_>, max: usize) -> Result<Bytes> {
    let mut page = BytesMut::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        if page.len() + chunk.len() > max {
            return Err(Error::OversizePage { max });
        }
        page.extend_from_slice(&chunk);
    }
    Ok(page.freeze())
}

/// Inflate a gzip member sequence in front of another decoder.
fn gunzip(body: ByteStream<'_>) -> ByteStream<'_> {
    use async_compression::tokio::bufread::GzipDecoder;
    use tokio_util::io::{ReaderStream, StreamReader};

    let reader = StreamReader::new(body.map_err(std::io::Error::other));
    let mut decoder = GzipDecoder::new(reader);
    decoder.multiple_members(true);
    ReaderStream::new(decoder)
        .map_err(|e| Error::Decode(format!("gzip: {e}")))
        .boxed()
}

/// Drive a framer over a body stream. Body chunks are not leased: reqwest
/// bounds them by the TCP window and the batcher leases the rows.
fn stream_framed<'a, F: Framer + Unpin + 'a>(body: ByteStream<'a>, framer: F) -> RowBytes<'a> {
    framed(body.map_ok(LeasedBlock::unleased).boxed(), framer).boxed()
}

/// Byte range of the array at a JSON pointer, without building a `Value` tree:
/// each level is read as a map or list of raw slices.
///
/// `None` when the pointer leads to nothing or to `null` (an empty page).
fn raw_at(page: &Bytes, pointer: &str) -> Result<Option<(usize, usize)>> {
    let base = page.as_ptr() as usize;
    let mut current: &[u8] = page;
    for segment in pointer.split('/').skip(1) {
        let segment = segment.replace("~1", "/").replace("~0", "~");
        let trimmed = trim_line(current);
        let next: Option<&RawValue> = match trimmed.first() {
            Some(b'{') => {
                let map: std::collections::HashMap<&str, &RawValue> =
                    serde_json::from_slice(trimmed)
                        .map_err(|e| Error::Decode(format!("json_at `{pointer}`: {e}")))?;
                map.get(segment.as_str()).copied()
            }
            Some(b'[') => {
                let list: Vec<&RawValue> = serde_json::from_slice(trimmed)
                    .map_err(|e| Error::Decode(format!("json_at `{pointer}`: {e}")))?;
                segment
                    .parse::<usize>()
                    .ok()
                    .and_then(|i| list.get(i).copied())
            }
            _ => None,
        };
        match next {
            Some(raw) => current = raw.get().as_bytes(),
            None => return Ok(None),
        }
    }
    let trimmed = trim_line(current);
    match trimmed.first() {
        None => Ok(None),
        Some(b'n') if trimmed == b"null" => Ok(None),
        Some(b'[') => {
            let start = trimmed.as_ptr() as usize - base;
            Ok(Some((start, start + trimmed.len())))
        }
        Some(_) => Err(Error::Decode(format!(
            "json_at `{pointer}`: the value there is not an array"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::io::Write as _;

    fn chunks(parts: &[&[u8]]) -> ByteStream<'static> {
        let owned: Vec<Result<Bytes>> = parts
            .iter()
            .map(|p| Ok(Bytes::copy_from_slice(p)))
            .collect();
        futures::stream::iter(owned).boxed()
    }

    async fn rows(decoder: &Decoder, parts: &[&[u8]]) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut stream = decoder.frame(chunks(parts), 1024 * 1024);
        while let Some(row) = stream.next().await {
            out.push(String::from_utf8(row?.to_vec()).unwrap());
        }
        Ok(out)
    }

    #[tokio::test]
    async fn ndjson_with_and_without_a_trailing_newline() {
        let d = Decoder::Ndjson;
        assert_eq!(
            rows(&d, &[b"{\"a\":1}\n{\"a\":2}\n"]).await.unwrap(),
            ["{\"a\":1}", "{\"a\":2}"]
        );
        assert_eq!(
            rows(&d, &[b"{\"a\":1}\n{\"a\":2}"]).await.unwrap(),
            ["{\"a\":1}", "{\"a\":2}"],
            "certificates.jsonl has no trailing newline"
        );
    }

    #[tokio::test]
    async fn ndjson_zero_byte_body_is_an_empty_store_and_a_bare_object_is_one_row() {
        let d = Decoder::Ndjson;
        assert!(rows(&d, &[b""]).await.unwrap().is_empty());
        assert!(rows(&d, &[]).await.unwrap().is_empty());
        assert_eq!(
            rows(&d, &[b"{\"only\":true}"]).await.unwrap(),
            ["{\"only\":true}"]
        );
    }

    #[tokio::test]
    async fn ndjson_tolerates_crlf_blank_lines_and_chunk_splits_mid_line() {
        let d = Decoder::Ndjson;
        assert_eq!(
            rows(&d, &[b"{\"a\":1}\r\n\r\n{\"a\":2}\r\n"])
                .await
                .unwrap(),
            ["{\"a\":1}", "{\"a\":2}"]
        );
        assert_eq!(
            rows(&d, &[b"{\"a\":", b"1}\n{\"a\":2", b"}\n{\"a\"", b":3}"])
                .await
                .unwrap(),
            ["{\"a\":1}", "{\"a\":2}", "{\"a\":3}"]
        );
    }

    #[tokio::test]
    async fn json_array_streams_elements_across_chunk_boundaries() {
        let d = Decoder::JsonArray;
        assert_eq!(
            rows(&d, &[b"[{\"a\":1},", b" {\"a\":", b"2}]"])
                .await
                .unwrap(),
            ["{\"a\":1}", "{\"a\":2}"]
        );
        assert_eq!(
            rows(&d, &[b" \n[\n  {\"a\":1}\n]\n"]).await.unwrap(),
            ["{\"a\":1}"]
        );
        assert!(rows(&d, &[b"[]"]).await.unwrap().is_empty());
        assert!(rows(&d, &[b"[", b"]"]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn json_array_keeps_brackets_inside_strings_and_nested_containers_intact() {
        let d = Decoder::JsonArray;
        let body = br#"[{"s":"a]b}c\"d\\","n":{"l":[1,[2,3]]}},{"t":"[{"}]"#;
        assert_eq!(
            rows(&d, &[body]).await.unwrap(),
            [r#"{"s":"a]b}c\"d\\","n":{"l":[1,[2,3]]}}"#, r#"{"t":"[{"}"#]
        );
        assert_eq!(
            rows(&d, &[b"[1, \"two\", true, null, 4.5]"]).await.unwrap(),
            ["1", "\"two\"", "true", "null", "4.5"]
        );
    }

    #[tokio::test]
    async fn json_array_rejects_garbage_and_truncation() {
        let d = Decoder::JsonArray;
        assert!(rows(&d, &[b"{\"a\":1}"]).await.is_err(), "not an array");
        assert!(rows(&d, &[b"[{\"a\":1}"]).await.is_err(), "unterminated");
        assert!(
            rows(&d, &[b"[{\"a\":1}] x"]).await.is_err(),
            "trailing garbage"
        );
        assert!(rows(&d, &[b"[{\"a\":1},]"]).await.is_err(), "stray comma");
        assert!(
            rows(&d, &[b""]).await.is_err(),
            "an empty body is not an array"
        );
    }

    #[tokio::test]
    async fn json_at_frames_the_array_at_a_pointer_as_slices_of_the_page() {
        let d = Decoder::JsonAt("/assets".into());
        assert!(d.is_page_bounded());
        let page = Bytes::from_static(br#"{"assets": [{"id":"a"}, {"id":"b"}], "next_key": "k"}"#);
        let framed = d.frame_page(&page).unwrap();
        assert_eq!(framed.len(), 2);
        assert_eq!(&framed[0][..], br#"{"id":"a"}"#);
        assert_eq!(&framed[1][..], br#"{"id":"b"}"#);
        assert!(
            std::ptr::eq(page.as_ptr().wrapping_add(12), framed[0].as_ptr()),
            "a slice of the page buffer, not a copy"
        );
        let nested = Decoder::JsonAt("/data/items".into());
        let page = Bytes::from_static(br#"{"data":{"items":[1,2]}}"#);
        assert_eq!(nested.frame_page(&page).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn json_at_with_an_absent_or_null_pointer_is_an_empty_page() {
        let d = Decoder::JsonAt("/users".into());
        assert!(
            d.frame_page(&Bytes::from_static(br#"{"next_key":""}"#))
                .unwrap()
                .is_empty()
        );
        assert!(
            d.frame_page(&Bytes::from_static(br#"{"users":null}"#))
                .unwrap()
                .is_empty()
        );
        assert!(
            d.frame_page(&Bytes::from_static(br#"{"users":{}}"#))
                .is_err(),
            "not an array"
        );
    }

    #[tokio::test]
    async fn document_is_the_whole_body_as_one_row_and_nothing_for_an_empty_body() {
        let d = Decoder::Document;
        assert_eq!(
            rows(&d, &[b"{\"info\":", b"{}}"]).await.unwrap(),
            ["{\"info\":{}}"]
        );
        assert!(rows(&d, &[b""]).await.unwrap().is_empty());
    }

    /// An object-store `.json` export is either a top-level array or one
    /// document; `json` frames both, unlike `json_array` (refuses an object)
    /// and `document` (one row whatever the shape).
    #[tokio::test]
    async fn json_frames_an_array_per_element_and_anything_else_as_one_row() {
        let d = Decoder::Json;
        assert!(d.is_page_bounded());
        assert_eq!(
            rows(&d, &[b"[{\"a\":1},", b"{\"a\":2}]"]).await.unwrap(),
            ["{\"a\":1}", "{\"a\":2}"]
        );
        assert_eq!(
            rows(&d, &[b" {\"Records\": [1, 2]}\n"]).await.unwrap(),
            [" {\"Records\": [1, 2]}\n"],
            "an object is one row, the page as it came"
        );
        assert_eq!(rows(&d, &[b"42"]).await.unwrap(), ["42"]);
        assert!(rows(&d, &[b" \n"]).await.unwrap().is_empty());
        assert!(rows(&d, &[b"[1,"]).await.is_err(), "a truncated array");
    }

    #[tokio::test]
    async fn lines_wraps_each_line_as_a_json_object_with_escaping() {
        let d = Decoder::Lines;
        assert_eq!(
            rows(&d, &[b"v1.0.0\nv1.1.0 \"quoted\"\n"]).await.unwrap(),
            [r#"{"line":"v1.0.0"}"#, r#"{"line":"v1.1.0 \"quoted\""}"#]
        );
    }

    #[tokio::test]
    async fn csv_with_a_header_yields_objects_and_handles_quoted_newlines() {
        let d = Decoder::Csv { header: true };
        let page = Bytes::from_static(b"EVENT_TYPE,MESSAGE\nLogin,\"multi\nline\"\nLogout,ok\n");
        let framed = d.frame_page(&page).unwrap();
        assert_eq!(framed.len(), 2);
        let first: serde_json::Value = serde_json::from_slice(&framed[0]).unwrap();
        assert_eq!(first["EVENT_TYPE"], "Login");
        assert_eq!(first["MESSAGE"], "multi\nline");
        let headless = Decoder::Csv { header: false };
        let framed = headless.frame_page(&Bytes::from_static(b"a,b\n")).unwrap();
        let row: serde_json::Value = serde_json::from_slice(&framed[0]).unwrap();
        assert_eq!(row["col_0"], "a");
        assert_eq!(row["col_1"], "b");
    }

    #[tokio::test]
    async fn gzip_inflates_multiple_members_before_the_inner_decoder() {
        let mut body = Vec::new();
        for member in ["{\"a\":1}\n", "{\"a\":2}\n"] {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            enc.write_all(member.as_bytes()).unwrap();
            body.extend(enc.finish().unwrap());
        }
        let d = Decoder::Gzip(Box::new(Decoder::Ndjson));
        let (head, tail) = body.split_at(body.len() / 2);
        assert_eq!(
            rows(&d, &[head, tail]).await.unwrap(),
            ["{\"a\":1}", "{\"a\":2}"]
        );
        assert!(rows(&d, &[b"not gzip"]).await.is_err());
    }

    /// A gzip member that inflates to one line longer than the bound (a
    /// `.json` export declared `ndjson`) fails at the bound rather than
    /// being carried whole; the rows before it are out.
    #[tokio::test]
    async fn a_streamed_row_over_the_bound_fails_instead_of_being_carried_whole() {
        let mut body = Vec::new();
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(b"{\"a\":1}\n").unwrap();
        enc.write_all(&vec![b'x'; 1024 * 1024]).unwrap();
        body.extend(enc.finish().unwrap());
        let d = Decoder::Gzip(Box::new(Decoder::Ndjson));
        let mut stream = d.frame(chunks(&[&body]), 64 * 1024);
        assert_eq!(&stream.next().await.unwrap().unwrap()[..], b"{\"a\":1}");
        let err = stream.next().await.unwrap().unwrap_err();
        assert!(
            matches!(err, Error::OversizePage { max } if max == 64 * 1024),
            "{err:?}"
        );
        assert!(stream.next().await.is_none());

        let d = Decoder::JsonArray;
        let mut stream = d.frame(chunks(&[b"[{\"a\":1},{\"b\":\"", &[b'x'; 200]]), 64);
        assert!(stream.next().await.unwrap().is_ok());
        assert!(matches!(
            stream.next().await.unwrap().unwrap_err(),
            Error::OversizePage { max: 64 }
        ));
    }

    #[tokio::test]
    async fn a_page_bounded_decoder_refuses_a_page_over_the_bound() {
        let d = Decoder::Document;
        let mut stream = d.frame(chunks(&[b"0123456789", b"0123456789"]), 15);
        let err = stream.next().await.unwrap().unwrap_err();
        assert!(matches!(err, Error::OversizePage { max: 15 }), "{err:?}");
        assert_eq!(err.api_error_code(), "oversize_page");
    }

    #[tokio::test]
    async fn collect_page_bounds_the_buffer_and_returns_the_whole_body() {
        let page = collect_page(chunks(&[b"ab", b"cd"]), 4).await.unwrap();
        assert_eq!(&page[..], b"abcd");
        assert!(collect_page(chunks(&[b"ab", b"cd"]), 3).await.is_err());
    }

    /// AWS Config's `Results`: an array of JSON STRINGS each holding one
    /// resource's document; the row is the document, not the string.
    #[tokio::test]
    async fn quoted_rows_are_unquoted_into_the_documents_they_hold() {
        let d = Decoder::Quoted(Box::new(Decoder::JsonAt("/Results".into())));
        assert!(d.is_page_bounded());
        let page = Bytes::from_static(
            br#"{"Results": ["{\"resourceId\":\"i-1\",\"configuration\":{\"size\":100}}", "{\"resourceId\":\"vol-1\"}"], "NextToken": "t"}"#,
        );
        let framed = d.frame_page(&page).unwrap();
        assert_eq!(
            &framed[0][..],
            br#"{"resourceId":"i-1","configuration":{"size":100}}"#
        );
        assert_eq!(&framed[1][..], br#"{"resourceId":"vol-1"}"#);
        assert_eq!(
            rows(&d, &[page.as_ref()]).await.unwrap().len(),
            2,
            "the streaming path unquotes too"
        );
        let not_strings = Bytes::from_static(br#"{"Results": [{"resourceId":"i-1"}]}"#);
        assert!(
            d.frame_page(&not_strings).is_err(),
            "an object is not a quoted row"
        );
        let spec = RowsSpec {
            decoder: DecoderKind::JsonAt,
            at: Some("/Results".into()),
            quoted: true,
            ..RowsSpec::default()
        };
        assert!(matches!(Decoder::build(&spec), Decoder::Quoted(_)));
    }

    #[test]
    fn decoder_builds_from_the_grammar_and_gzip_wraps_the_inner() {
        use crate::profile::{DecoderKind, RowsSpec};
        let spec = RowsSpec {
            decoder: DecoderKind::Ndjson,
            gzip: true,
            ..RowsSpec::default()
        };
        let d = Decoder::build(&spec);
        assert!(matches!(d, Decoder::Gzip(ref inner) if matches!(**inner, Decoder::Ndjson)));
        assert!(!d.is_page_bounded());
        let at = RowsSpec {
            decoder: DecoderKind::JsonAt,
            at: Some("/value".into()),
            ..RowsSpec::default()
        };
        assert!(matches!(Decoder::build(&at), Decoder::JsonAt(ref p) if p == "/value"));
    }
}
