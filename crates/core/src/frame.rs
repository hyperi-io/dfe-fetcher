// Project:   dfe-fetcher
// File:      crates/core/src/frame.rs
// Purpose:   Framing byte blocks into rows: lines, JSON arrays and CSV, streaming and lease-aware
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Blocks into rows.
//!
//! Every shape hands the framework bytes in blocks: an HTTP body chunk, one
//! `arrow-json` buffer per fetched batch, a slice of a file. A [`Framer`]
//! turns a block sequence into rows without seeing the whole body, and
//! [`framed`] drives one over a block stream, holding each [`LeasedBlock`] --
//! and its memory lease -- only until the rows framed from it are out. The
//! three framers here are the ones every shape needs: NDJSON or plain lines,
//! a top-level JSON array, and CSV records as objects.

pub mod scan;

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures_core::Stream;
use futures_core::stream::BoxStream;

use crate::batch::Lease;
use crate::error::{Error, Result};
use scan::{ArrayScanner, Scan, next_newline, trim_line, trim_range};

/// A block of bytes accounted on the memory guard until dropped.
pub struct LeasedBlock {
    bytes: Bytes,
    lease: Option<Arc<dyn Lease>>,
}

impl LeasedBlock {
    /// Lease `bytes.len()` on `lease` for the life of the block.
    #[must_use]
    pub fn new(bytes: Bytes, lease: Arc<dyn Lease>) -> Self {
        lease.add(bytes.len() as u64);
        Self {
            bytes,
            lease: Some(lease),
        }
    }

    /// A block nothing accounts for: a body chunk the transport already
    /// bounds, or a test fixture.
    #[must_use]
    pub fn unleased(bytes: Bytes) -> Self {
        Self { bytes, lease: None }
    }

    /// The block's bytes.
    #[must_use]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

impl Drop for LeasedBlock {
    fn drop(&mut self) {
        if let Some(lease) = &self.lease {
            lease.release(self.bytes.len() as u64);
        }
    }
}

/// Something that turns blocks into rows, one boundary scan per block.
///
/// A row wholly inside one block may be a slice of it; the driver keeps the
/// block alive until its rows are out, so that is safe.
///
/// # Errors
///
/// Both methods return [`Error::Decode`] when the bytes are not the format
/// the framer expects; the stream ends with that error.
pub trait Framer: Send {
    /// Frame `chunk`, appending each complete row to `out`.
    ///
    /// # Errors
    ///
    /// [`Error::Decode`] when the chunk breaks the format.
    fn feed(&mut self, chunk: &Bytes, out: &mut Vec<Bytes>) -> Result<()>;

    /// The rows still open once the blocks have ended.
    ///
    /// # Errors
    ///
    /// [`Error::Decode`] when the input ended mid-row.
    fn finish(&mut self, out: &mut Vec<Bytes>) -> Result<()>;
}

impl<F: Framer + ?Sized> Framer for Box<F> {
    fn feed(&mut self, chunk: &Bytes, out: &mut Vec<Bytes>) -> Result<()> {
        (**self).feed(chunk, out)
    }

    fn finish(&mut self, out: &mut Vec<Bytes>) -> Result<()> {
        (**self).finish(out)
    }
}

/// Wrap a text line as `{"line": "..."}`.
#[must_use]
pub fn wrap_line(line: &[u8]) -> Bytes {
    let text = String::from_utf8_lossy(line);
    let literal = serde_json::Value::String(text.into_owned()).to_string();
    let mut out = Vec::with_capacity(literal.len() + 10);
    out.extend_from_slice(b"{\"line\":");
    out.extend_from_slice(literal.as_bytes());
    out.push(b'}');
    Bytes::from(out)
}

/// One line as a row: the trimmed slice, or wrapped as `{"line": ...}`; nothing
/// for a blank line.
fn line_row(line: &Bytes, wrap: bool) -> Option<Bytes> {
    let (start, end) = trim_range(line);
    if start == end {
        return None;
    }
    Some(if wrap {
        wrap_line(&line[start..end])
    } else {
        line.slice(start..end)
    })
}

/// The non-blank lines of a complete buffer, as slices of it.
#[must_use]
pub fn split_lines(page: &Bytes, wrap: bool) -> Vec<Bytes> {
    let mut rows = Vec::new();
    let mut pos = 0;
    while pos < page.len() {
        let end = next_newline(&page[pos..]).map_or(page.len(), |n| pos + n + 1);
        rows.extend(line_row(&page.slice(pos..end), wrap));
        pos = end;
    }
    rows
}

/// Streams `\n`-terminated lines: those wholly inside a block are slices of
/// it, a line completed from a carried prefix is one allocation. With `wrap`
/// each line lands as `{"line": ...}`; without it the trimmed line is the row.
/// The open line is bounded by [`LineFramer::bounded`]: a body with no
/// newline (an HTML error page under a 200, a `.json` export declared
/// `ndjson`) fails at the bound instead of being carried whole.
#[derive(Debug)]
pub struct LineFramer {
    carry: BytesMut,
    wrap: bool,
    max: usize,
}

impl Default for LineFramer {
    fn default() -> Self {
        Self::new(false)
    }
}

impl LineFramer {
    /// A framer; `wrap` decides between raw lines and `{"line": ...}` objects.
    #[must_use]
    pub fn new(wrap: bool) -> Self {
        Self {
            carry: BytesMut::new(),
            wrap,
            max: usize::MAX,
        }
    }

    /// Refuse an open line longer than `max` bytes.
    #[must_use]
    pub fn bounded(mut self, max: usize) -> Self {
        self.max = max;
        self
    }

    /// Bytes of the open line held right now.
    #[must_use]
    pub fn carried(&self) -> usize {
        self.carry.len()
    }

    /// Whether the open line can grow by `more` bytes.
    fn fits(&self, more: usize) -> Result<()> {
        if self.carry.len().saturating_add(more) > self.max {
            return Err(Error::OversizePage { max: self.max });
        }
        Ok(())
    }
}

impl Framer for LineFramer {
    fn feed(&mut self, chunk: &Bytes, out: &mut Vec<Bytes>) -> Result<()> {
        let mut start = 0;
        while let Some(pos) = next_newline(&chunk[start..]) {
            let end = start + pos + 1;
            let line = if self.carry.is_empty() {
                chunk.slice(start..end)
            } else {
                self.fits(end - start)?;
                self.carry.extend_from_slice(&chunk[start..end]);
                std::mem::take(&mut self.carry).freeze()
            };
            out.extend(line_row(&line, self.wrap));
            start = end;
        }
        if start < chunk.len() {
            self.fits(chunk.len() - start)?;
            self.carry.extend_from_slice(&chunk[start..]);
        }
        Ok(())
    }

    fn finish(&mut self, out: &mut Vec<Bytes>) -> Result<()> {
        let tail = std::mem::take(&mut self.carry).freeze();
        out.extend(line_row(&tail, self.wrap));
        Ok(())
    }
}

/// Streams the elements of a top-level array, holding at most one open
/// element, which [`ArrayFramer::bounded`] caps: an element longer than the
/// bound fails once the block that crosses it has been scanned.
#[derive(Debug)]
pub struct ArrayFramer {
    buf: BytesMut,
    scanner: ArrayScanner,
    max: usize,
}

impl Default for ArrayFramer {
    fn default() -> Self {
        Self {
            buf: BytesMut::new(),
            scanner: ArrayScanner::default(),
            max: usize::MAX,
        }
    }
}

impl ArrayFramer {
    /// Refuse an open element longer than `max` bytes.
    #[must_use]
    pub fn bounded(mut self, max: usize) -> Self {
        self.max = max;
        self
    }

    /// Bytes of the open element held right now.
    #[must_use]
    pub fn carried(&self) -> usize {
        self.buf.len()
    }

    fn drain(&mut self, out: &mut Vec<Bytes>) -> Result<()> {
        loop {
            match self.scanner.scan(&self.buf)? {
                Scan::Element { start, end } => {
                    let element = self.buf.split_to(end).freeze().slice(start..);
                    out.push(element);
                }
                Scan::NeedMore { keep_from } => {
                    let _ = self.buf.split_to(keep_from);
                    return Ok(());
                }
                Scan::Done { consumed } => {
                    let _ = self.buf.split_to(consumed);
                    return Ok(());
                }
            }
        }
    }
}

impl Framer for ArrayFramer {
    fn feed(&mut self, chunk: &Bytes, out: &mut Vec<Bytes>) -> Result<()> {
        self.buf.extend_from_slice(chunk);
        self.drain(out)?;
        // What the scan could not close is one open element; a block that
        // carries it past the bound fails here rather than growing the buffer.
        if self.buf.len() > self.max {
            return Err(Error::OversizePage { max: self.max });
        }
        Ok(())
    }

    fn finish(&mut self, out: &mut Vec<Bytes>) -> Result<()> {
        self.drain(out)?;
        if !self.scanner.is_done() {
            return Err(Error::Decode(
                "json_array: body ended before the closing `]`".into(),
            ));
        }
        if !trim_line(&self.buf).is_empty() {
            return Err(Error::Decode(
                "json_array: trailing content after `]`".into(),
            ));
        }
        Ok(())
    }
}

/// The elements of the array occupying `page[start..end]`, as slices of `page`.
///
/// # Errors
///
/// Returns [`Error::Decode`] when the range is not a complete JSON array.
pub fn split_array(page: &Bytes, start: usize, end: usize) -> Result<Vec<Bytes>> {
    let mut scanner = ArrayScanner::new();
    let mut rows = Vec::new();
    let mut pos = start;
    loop {
        match scanner.scan(&page[pos..end])? {
            Scan::Element { start: s, end: e } => {
                rows.push(page.slice(pos + s..pos + e));
                pos += e;
            }
            Scan::NeedMore { .. } => {
                return Err(Error::Decode(
                    "json_array: body ended before the closing `]`".into(),
                ));
            }
            Scan::Done { consumed } => {
                pos += consumed;
                if pos < end {
                    scanner.scan(&page[pos..end])?;
                }
                return Ok(rows);
            }
        }
    }
}

/// Streams CSV records as JSON objects keyed by the header row (or `col_<n>`
/// without one). Quoting, escaped quotes and newlines inside quotes follow
/// RFC 4180 as `csv-core` reads them; a record split across blocks is carried
/// by the parser itself, so no input is buffered here.
///
/// SHORTCUT: each record becomes a `serde_json::Map` before it is written,
/// one allocation per field. Lift (write the object bytes straight from the
/// field slices) when a CSV unit sustains more than ~20k rows/s.
pub struct CsvFramer {
    reader: csv_core::Reader,
    header: bool,
    headers: Option<Vec<String>>,
    fields: Vec<u8>,
    fields_len: usize,
    ends: Vec<usize>,
    ends_len: usize,
}

impl std::fmt::Debug for CsvFramer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvFramer")
            .field("header", &self.header)
            .field("headers", &self.headers)
            .finish_non_exhaustive()
    }
}

impl CsvFramer {
    /// A framer; with `header` the first record names the columns.
    #[must_use]
    pub fn new(header: bool) -> Self {
        Self {
            reader: csv_core::Reader::new(),
            header,
            headers: None,
            fields: vec![0; 4096],
            fields_len: 0,
            ends: vec![0; 64],
            ends_len: 0,
        }
    }

    /// Turn the completed record in the field buffers into a row (or the
    /// header) and reset them.
    fn take_record(&mut self, out: &mut Vec<Bytes>) {
        let mut start = 0;
        let mut values = Vec::with_capacity(self.ends_len);
        for &end in &self.ends[..self.ends_len] {
            values.push(String::from_utf8_lossy(&self.fields[start..end]).into_owned());
            start = end;
        }
        self.fields_len = 0;
        self.ends_len = 0;
        if self.header && self.headers.is_none() {
            self.headers = Some(values);
            return;
        }
        let mut map = serde_json::Map::with_capacity(values.len());
        for (i, value) in values.into_iter().enumerate() {
            let key = self
                .headers
                .as_ref()
                .and_then(|h| h.get(i).cloned())
                .unwrap_or_else(|| format!("col_{i}"));
            map.insert(key, serde_json::Value::String(value));
        }
        out.push(Bytes::from(serde_json::Value::Object(map).to_string()));
    }

    /// Run the parser over `input` (empty means end of input) until it asks
    /// for more.
    fn drive(&mut self, input: &[u8], out: &mut Vec<Bytes>) {
        let mut pos = 0;
        loop {
            let (result, read, written, ended) = self.reader.read_record(
                &input[pos..],
                &mut self.fields[self.fields_len..],
                &mut self.ends[self.ends_len..],
            );
            pos += read;
            self.fields_len += written;
            self.ends_len += ended;
            match result {
                csv_core::ReadRecordResult::InputEmpty => return,
                csv_core::ReadRecordResult::OutputFull => {
                    let grown = self.fields.len() * 2;
                    self.fields.resize(grown, 0);
                }
                csv_core::ReadRecordResult::OutputEndsFull => {
                    let grown = self.ends.len() * 2;
                    self.ends.resize(grown, 0);
                }
                csv_core::ReadRecordResult::Record => self.take_record(out),
                csv_core::ReadRecordResult::End => return,
            }
        }
    }
}

impl Framer for CsvFramer {
    fn feed(&mut self, chunk: &Bytes, out: &mut Vec<Bytes>) -> Result<()> {
        if !chunk.is_empty() {
            self.drive(chunk, out);
        }
        Ok(())
    }

    fn finish(&mut self, out: &mut Vec<Bytes>) -> Result<()> {
        self.drive(&[], out);
        Ok(())
    }
}

/// CSV records of a complete buffer as JSON objects keyed by the header (or
/// `col_<n>` without one).
///
/// # Errors
///
/// Returns [`Error::Decode`] when the framer refuses the bytes.
pub fn csv_rows(page: &Bytes, header: bool) -> Result<Vec<Bytes>> {
    let mut framer = CsvFramer::new(header);
    let mut rows = Vec::new();
    framer.feed(page, &mut rows)?;
    framer.finish(&mut rows)?;
    Ok(rows)
}

/// A framer driven over a block stream: rows come out as their boundaries are
/// seen, each block stays leased until the rows framed from it are out, and
/// the first block or framer error ends the stream after the rows before it.
pub struct Framed<'a, F> {
    blocks: BoxStream<'a, Result<LeasedBlock>>,
    framer: F,
    pending: VecDeque<Bytes>,
    current: Option<LeasedBlock>,
    done: bool,
}

/// Drive `framer` over `blocks`.
#[must_use]
pub fn framed<F: Framer>(blocks: BoxStream<'_, Result<LeasedBlock>>, framer: F) -> Framed<'_, F> {
    Framed {
        blocks,
        framer,
        pending: VecDeque::new(),
        current: None,
        done: false,
    }
}

impl<F: Framer + Unpin> Stream for Framed<'_, F> {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(row) = this.pending.pop_front() {
                if this.pending.is_empty() {
                    this.current = None;
                }
                return Poll::Ready(Some(Ok(row)));
            }
            if this.done {
                return Poll::Ready(None);
            }
            let mut out = Vec::new();
            match this.blocks.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(block))) => {
                    if let Err(e) = this.framer.feed(block.bytes(), &mut out) {
                        this.done = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                    this.pending.extend(out);
                    this.current = Some(block);
                }
                Poll::Ready(Some(Err(e))) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    this.done = true;
                    if let Err(e) = this.framer.finish(&mut out) {
                        return Poll::Ready(Some(Err(e)));
                    }
                    this.pending.extend(out);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{Lease, NoLease};
    use futures::StreamExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn blocks(parts: &[&[u8]]) -> futures::stream::BoxStream<'static, Result<LeasedBlock>> {
        let owned: Vec<Result<LeasedBlock>> = parts
            .iter()
            .map(|p| Ok(LeasedBlock::unleased(Bytes::copy_from_slice(p))))
            .collect();
        futures::stream::iter(owned).boxed()
    }

    async fn rows<F: Framer + Unpin + 'static>(framer: F, parts: &[&[u8]]) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut stream = framed(blocks(parts), framer);
        while let Some(row) = stream.next().await {
            out.push(String::from_utf8(row?.to_vec()).unwrap());
        }
        Ok(out)
    }

    #[test]
    fn lines_inside_one_chunk_are_slices_and_blanks_are_skipped() {
        let mut framer = LineFramer::new(false);
        let mut out = Vec::new();
        let chunk = Bytes::from_static(b"{\"a\":1}\n\n{\"a\":2}\r\n  \n");
        framer.feed(&chunk, &mut out).unwrap();
        assert_eq!(
            out,
            vec![
                Bytes::from_static(b"{\"a\":1}"),
                Bytes::from_static(b"{\"a\":2}")
            ]
        );
        assert!(
            std::ptr::eq(chunk.as_ptr(), out[0].as_ptr()),
            "a slice of the chunk, not a copy"
        );
        let mut tail = Vec::new();
        framer.finish(&mut tail).unwrap();
        assert_eq!(tail, [] as [bytes::Bytes; 0]);
    }

    #[test]
    fn a_line_split_across_chunks_is_carried_and_the_tail_is_flushed_at_the_end() {
        let mut framer = LineFramer::new(false);
        let mut out = Vec::new();
        framer
            .feed(&Bytes::from_static(b"{\"a\":"), &mut out)
            .unwrap();
        assert_eq!(out, [] as [bytes::Bytes; 0]);
        framer
            .feed(&Bytes::from_static(b"1}\n{\"b\""), &mut out)
            .unwrap();
        assert_eq!(out, vec![Bytes::from_static(b"{\"a\":1}")]);
        framer.feed(&Bytes::from_static(b":2}"), &mut out).unwrap();
        assert_eq!(out.len(), 1);
        framer.finish(&mut out).unwrap();
        assert_eq!(out[1], Bytes::from_static(b"{\"b\":2}"));
    }

    #[tokio::test]
    async fn ndjson_tolerates_a_missing_trailing_newline_crlf_and_a_zero_byte_body() {
        assert_eq!(
            rows(LineFramer::new(false), &[b"{\"a\":1}\n{\"a\":2}"])
                .await
                .unwrap(),
            ["{\"a\":1}", "{\"a\":2}"]
        );
        assert_eq!(
            rows(LineFramer::new(false), &[b"{\"a\":1}\r\n\r\n{\"a\":2}\r\n"])
                .await
                .unwrap(),
            ["{\"a\":1}", "{\"a\":2}"]
        );
        assert_eq!(
            rows(LineFramer::new(false), &[b""]).await.unwrap(),
            [] as [std::string::String; 0]
        );
        assert_eq!(
            rows(LineFramer::new(false), &[]).await.unwrap(),
            [] as [std::string::String; 0]
        );
    }

    /// An open line past the bound fails after the rows before it, and the
    /// carry never grows past the bound: a body with no newline cannot be
    /// buffered whole.
    #[test]
    fn a_line_longer_than_the_bound_fails_before_it_is_carried() {
        let mut framer = LineFramer::new(false).bounded(64);
        let mut out = Vec::new();
        let no_newline = vec![b'x'; 1024 * 1024];
        let mut chunk = b"{\"a\":1}\n".to_vec();
        chunk.extend_from_slice(&no_newline);
        let err = framer
            .feed(&Bytes::from(chunk), &mut out)
            .expect_err("a megabyte with no newline");
        assert!(matches!(err, Error::OversizePage { max: 64 }), "{err:?}");
        assert_eq!(
            out,
            vec![Bytes::from_static(b"{\"a\":1}")],
            "the row before it is out"
        );
        assert_eq!(
            framer.carried(),
            0,
            "nothing of the oversize line was copied"
        );

        let mut framer = LineFramer::new(false).bounded(64);
        let mut out = Vec::new();
        for _ in 0..3 {
            match framer.feed(&Bytes::from(vec![b'y'; 40]), &mut out) {
                Ok(()) => assert!(framer.carried() <= 64),
                Err(Error::OversizePage { max: 64 }) => {
                    assert!(framer.carried() <= 64, "the carry stayed under the bound");
                    return;
                }
                Err(other) => panic!("{other:?}"),
            }
        }
        panic!("three 40-byte chunks with no newline must trip a 64-byte bound");
    }

    /// An open array element past the bound fails once the block crossing
    /// it is scanned; the elements before it are out.
    #[test]
    fn an_array_element_longer_than_the_bound_fails_after_the_ones_before_it() {
        let mut framer = ArrayFramer::default().bounded(64);
        let mut out = Vec::new();
        framer
            .feed(&Bytes::from_static(b"[{\"a\":1},{\"blob\":\""), &mut out)
            .unwrap();
        assert_eq!(out.len(), 1);
        let err = framer
            .feed(&Bytes::from(vec![b'x'; 200]), &mut out)
            .expect_err("an element of 200 bytes and counting");
        assert!(matches!(err, Error::OversizePage { max: 64 }), "{err:?}");
        assert_eq!(out.len(), 1, "no second element was framed");
        assert!(
            ArrayFramer::default()
                .bounded(64)
                .feed(&Bytes::from_static(b"[1,2,3]"), &mut Vec::new())
                .is_ok(),
            "small elements under a small bound are fine"
        );
    }

    #[tokio::test]
    async fn plain_lines_are_wrapped_as_json_with_escaping() {
        assert_eq!(
            rows(LineFramer::new(true), &[b"v1.0.0\nv1.1.0 \"quoted\"\n"])
                .await
                .unwrap(),
            [r#"{"line":"v1.0.0"}"#, r#"{"line":"v1.1.0 \"quoted\""}"#]
        );
    }

    #[tokio::test]
    async fn json_array_streams_elements_across_chunk_boundaries_and_rejects_truncation() {
        assert_eq!(
            rows(
                ArrayFramer::default(),
                &[b"[{\"a\":1},", b" {\"a\":", b"2}]"]
            )
            .await
            .unwrap(),
            ["{\"a\":1}", "{\"a\":2}"]
        );
        assert_eq!(
            rows(ArrayFramer::default(), &[b"[", b"]"]).await.unwrap(),
            [] as [std::string::String; 0]
        );
        assert!(
            rows(ArrayFramer::default(), &[b"[{\"a\":1}"])
                .await
                .is_err(),
            "unterminated"
        );
        assert!(
            rows(ArrayFramer::default(), &[b"[{\"a\":1}] x"])
                .await
                .is_err(),
            "trailing garbage"
        );
        assert!(
            rows(ArrayFramer::default(), &[b""]).await.is_err(),
            "an empty body is not an array"
        );
    }

    #[tokio::test]
    async fn csv_streams_records_as_objects_across_chunks_with_quoted_newlines() {
        let got = rows(
            CsvFramer::new(true),
            &[
                b"EVENT_TYPE,MESS",
                b"AGE\nLogin,\"multi\nli",
                b"ne\"\nLogout,ok",
            ],
        )
        .await
        .unwrap();
        assert_eq!(got.len(), 2, "{got:?}");
        let first: serde_json::Value = serde_json::from_str(&got[0]).unwrap();
        assert_eq!(first["EVENT_TYPE"], "Login");
        assert_eq!(first["MESSAGE"], "multi\nline");
        let second: serde_json::Value = serde_json::from_str(&got[1]).unwrap();
        assert_eq!(second["EVENT_TYPE"], "Logout");
        assert_eq!(second["MESSAGE"], "ok", "a final record without a newline");
    }

    #[tokio::test]
    async fn csv_without_a_header_names_columns_by_position_and_skips_blank_lines() {
        let got = rows(CsvFramer::new(false), &[b"a,b\n\nc,d\n"])
            .await
            .unwrap();
        assert_eq!(got.len(), 2, "{got:?}");
        let row: serde_json::Value = serde_json::from_str(&got[0]).unwrap();
        assert_eq!(row["col_0"], "a");
        assert_eq!(row["col_1"], "b");
        assert_eq!(
            rows(CsvFramer::new(true), &[b"only,header\n"])
                .await
                .unwrap(),
            [] as [std::string::String; 0]
        );
        assert_eq!(
            rows(CsvFramer::new(true), &[b""]).await.unwrap(),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn page_helpers_frame_a_whole_buffer_the_same_way() {
        let page = Bytes::from_static(b"{\"a\":1}\n{\"a\":2}");
        assert_eq!(split_lines(&page, false).len(), 2);
        let arr = Bytes::from_static(b"[1, 2]");
        assert_eq!(split_array(&arr, 0, arr.len()).unwrap(), ["1", "2"]);
        assert!(split_array(&Bytes::from_static(b"[1,"), 0, 3).is_err());
        let csv = csv_rows(&Bytes::from_static(b"h\nv\n"), true).unwrap();
        assert_eq!(&csv[0][..], br#"{"h":"v"}"#);
    }

    struct Counting(AtomicI64);

    impl Lease for Counting {
        fn add(&self, bytes: u64) {
            self.0.fetch_add(bytes.cast_signed(), Ordering::SeqCst);
        }
        fn release(&self, bytes: u64) {
            self.0.fetch_sub(bytes.cast_signed(), Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn a_block_is_leased_until_its_last_row_is_yielded() {
        let lease = Arc::new(Counting(AtomicI64::new(0)));
        let l: Arc<dyn Lease> = lease.clone();
        let block = LeasedBlock::new(Bytes::from_static(b"{\"id\":1}\n{\"id\":2}\n"), l.clone());
        assert_eq!(lease.0.load(Ordering::SeqCst), 18);
        let blocks = futures::stream::iter(vec![Ok(block)]).boxed();
        let mut rows = framed(blocks, LineFramer::new(false));
        let first = rows.next().await.unwrap().unwrap();
        assert_eq!(&first[..], b"{\"id\":1}");
        assert_eq!(
            lease.0.load(Ordering::SeqCst),
            18,
            "the block stays leased while a row of it is still pending"
        );
        let second = rows.next().await.unwrap().unwrap();
        assert_eq!(&second[..], b"{\"id\":2}");
        assert_eq!(
            lease.0.load(Ordering::SeqCst),
            0,
            "released with the last row"
        );
        assert!(rows.next().await.is_none());
    }

    #[tokio::test]
    async fn a_producer_error_ends_the_stream_after_the_rows_before_it() {
        let l: Arc<dyn Lease> = Arc::new(NoLease);
        let blocks = futures::stream::iter(vec![
            Ok(LeasedBlock::new(
                Bytes::from_static(b"{\"id\":1}\n"),
                l.clone(),
            )),
            Err(Error::Source("cursor broke".into())),
        ])
        .boxed();
        let mut rows = framed(blocks, LineFramer::new(false));
        assert!(rows.next().await.unwrap().is_ok());
        assert!(rows.next().await.unwrap().is_err());
        assert!(rows.next().await.is_none());
    }

    #[tokio::test]
    async fn a_framer_error_ends_the_stream_and_the_block_lease_is_released() {
        let lease = Arc::new(Counting(AtomicI64::new(0)));
        let l: Arc<dyn Lease> = lease.clone();
        let blocks = futures::stream::iter(vec![Ok(LeasedBlock::new(
            Bytes::from_static(b"not an array"),
            l.clone(),
        ))])
        .boxed();
        let mut rows = framed(blocks, ArrayFramer::default());
        assert!(rows.next().await.unwrap().is_err());
        assert!(rows.next().await.is_none());
        drop(rows);
        assert_eq!(lease.0.load(Ordering::SeqCst), 0);
    }
}
