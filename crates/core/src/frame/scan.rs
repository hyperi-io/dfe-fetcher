// Project:   dfe-fetcher
// File:      crates/core/src/frame/scan.rs
// Purpose:   Incremental scanners that find row boundaries in NDJSON and JSON-array bytes
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Boundary scanners shared by the streaming and the page-bounded paths.
//!
//! Both scanners work on whatever bytes the caller holds and say where the next
//! row ends, so the streaming framer can run them over a growing `BytesMut` and
//! the page-bounded framer over a complete buffer, emitting slices of it.

use crate::error::{Error, Result};

/// Outcome of one scan over the caller's buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scan {
    /// A complete element occupies `start..end` of the buffer handed in.
    Element {
        /// First byte of the element.
        start: usize,
        /// One past its last byte.
        end: usize,
    },
    /// No complete element yet; bytes before `keep_from` can be discarded.
    NeedMore {
        /// Offset the next scan must start from (the open element's start, or
        /// the end of the consumed separators).
        keep_from: usize,
    },
    /// The array closed; `consumed` bytes were used and only whitespace may follow.
    Done {
        /// Bytes consumed by the closing bracket and everything before it.
        consumed: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Container,
    Str,
    Scalar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Start,
    BeforeElement { after_comma: bool },
    InElement,
    AfterElement,
    Done,
}

/// Finds element boundaries in a top-level JSON array, resumable across chunks.
#[derive(Debug)]
pub struct ArrayScanner {
    state: State,
    kind: Kind,
    depth: u32,
    in_string: bool,
    escape: bool,
    /// Bytes of the open element already scanned, relative to its start, so a
    /// resumed scan does not walk them again.
    scanned: usize,
}

impl Default for ArrayScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayScanner {
    /// A scanner at the start of a body.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: State::Start,
            kind: Kind::Scalar,
            depth: 0,
            in_string: false,
            escape: false,
            scanned: 0,
        }
    }

    /// Whether the closing bracket has been seen.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }

    /// Scan `bytes` from offset 0. After an `Element`, call again with the
    /// buffer advanced past `end`; after `NeedMore`, with the buffer advanced to
    /// `keep_from` plus whatever arrived since.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Decode`] when the bytes are not a JSON array: no opening
    /// bracket, a stray comma, a missing separator, or content after the close.
    pub fn scan(&mut self, bytes: &[u8]) -> Result<Scan> {
        let mut i = 0usize;
        loop {
            match self.state {
                State::Start => {
                    i = skip_ws(bytes, i);
                    let Some(&b) = bytes.get(i) else {
                        return Ok(Scan::NeedMore { keep_from: i });
                    };
                    if b != b'[' {
                        return Err(Error::Decode(format!(
                            "json_array: body starts with {:?}, not `[`",
                            char::from(b)
                        )));
                    }
                    i += 1;
                    self.state = State::BeforeElement { after_comma: false };
                }
                State::BeforeElement { after_comma } => {
                    i = skip_ws(bytes, i);
                    let Some(&b) = bytes.get(i) else {
                        return Ok(Scan::NeedMore { keep_from: i });
                    };
                    if b == b']' {
                        if after_comma {
                            return Err(Error::Decode(format!(
                                "json_array: stray comma before `]` at byte {i}"
                            )));
                        }
                        self.state = State::Done;
                        return Ok(Scan::Done { consumed: i + 1 });
                    }
                    if b == b',' {
                        return Err(Error::Decode(format!(
                            "json_array: empty element at byte {i}"
                        )));
                    }
                    self.kind = match b {
                        b'{' | b'[' => Kind::Container,
                        b'"' => Kind::Str,
                        _ => Kind::Scalar,
                    };
                    self.depth = 0;
                    self.in_string = false;
                    self.escape = false;
                    self.scanned = 0;
                    self.state = State::InElement;
                    // The element starts at `i`; the caller's buffer may begin
                    // here on the next call, so the element scan is relative.
                    return Ok(self.scan_element(bytes, i));
                }
                State::InElement => return Ok(self.scan_element(bytes, 0)),
                State::AfterElement => {
                    i = skip_ws(bytes, i);
                    let Some(&b) = bytes.get(i) else {
                        return Ok(Scan::NeedMore { keep_from: i });
                    };
                    match b {
                        b',' => {
                            i += 1;
                            self.state = State::BeforeElement { after_comma: true };
                        }
                        b']' => {
                            self.state = State::Done;
                            return Ok(Scan::Done { consumed: i + 1 });
                        }
                        other => {
                            return Err(Error::Decode(format!(
                                "json_array: expected `,` or `]` after element, found {:?} at byte {i}",
                                char::from(other)
                            )));
                        }
                    }
                }
                State::Done => {
                    i = skip_ws(bytes, i);
                    if i < bytes.len() {
                        return Err(Error::Decode(format!(
                            "json_array: trailing content after `]` at byte {i}"
                        )));
                    }
                    return Ok(Scan::Done { consumed: i });
                }
            }
        }
    }

    /// Scan the open element starting at `start`, resuming past bytes already seen.
    fn scan_element(&mut self, bytes: &[u8], start: usize) -> Scan {
        let mut i = start + self.scanned;
        while let Some(&b) = bytes.get(i) {
            match self.kind {
                Kind::Container => {
                    if self.in_string {
                        if self.escape {
                            self.escape = false;
                        } else if b == b'\\' {
                            self.escape = true;
                        } else if b == b'"' {
                            self.in_string = false;
                        }
                    } else {
                        match b {
                            b'"' => self.in_string = true,
                            b'{' | b'[' => self.depth += 1,
                            b'}' | b']' => {
                                self.depth -= 1;
                                if self.depth == 0 {
                                    self.state = State::AfterElement;
                                    return Scan::Element { start, end: i + 1 };
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Kind::Str => {
                    // The opening quote is byte `start`; the string ends at the
                    // first unescaped quote after it.
                    if i > start {
                        if self.escape {
                            self.escape = false;
                        } else if b == b'\\' {
                            self.escape = true;
                        } else if b == b'"' {
                            self.state = State::AfterElement;
                            return Scan::Element { start, end: i + 1 };
                        }
                    }
                }
                Kind::Scalar => {
                    if matches!(b, b',' | b']') || b.is_ascii_whitespace() {
                        self.state = State::AfterElement;
                        return Scan::Element { start, end: i };
                    }
                }
            }
            i += 1;
        }
        self.scanned = i - start;
        Scan::NeedMore { keep_from: start }
    }
}

fn skip_ws(bytes: &[u8], mut i: usize) -> usize {
    while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
        i += 1;
    }
    i
}

/// Where the next line ends in `bytes`: the newline's offset, if one is present.
#[must_use]
pub fn next_newline(bytes: &[u8]) -> Option<usize> {
    memchr::memchr(b'\n', bytes)
}

/// The `start..end` of `line` once surrounding whitespace (including the
/// `\r\n` or `\n` terminator) is dropped; empty when the line is blank.
#[must_use]
pub fn trim_range(line: &[u8]) -> (usize, usize) {
    let mut end = line.len();
    while end > 0 && line[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    let mut start = 0;
    while start < end && line[start].is_ascii_whitespace() {
        start += 1;
    }
    (start, end)
}

/// A line without its terminator and surrounding whitespace.
#[must_use]
pub fn trim_line(line: &[u8]) -> &[u8] {
    let (start, end) = trim_range(line);
    &line[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_elements(body: &[u8]) -> Result<Vec<String>> {
        let mut scanner = ArrayScanner::new();
        let mut out = Vec::new();
        let mut pos = 0;
        loop {
            match scanner.scan(&body[pos..])? {
                Scan::Element { start, end } => {
                    out.push(String::from_utf8(body[pos + start..pos + end].to_vec()).unwrap());
                    pos += end;
                }
                Scan::NeedMore { .. } => {
                    return Err(Error::Decode("unexpected end of input".into()));
                }
                Scan::Done { consumed } => {
                    pos += consumed;
                    if pos >= body.len() {
                        return Ok(out);
                    }
                    scanner.scan(&body[pos..])?;
                    return Ok(out);
                }
            }
        }
    }

    #[test]
    fn scans_whole_arrays() {
        assert_eq!(all_elements(b"[]").unwrap(), Vec::<String>::new());
        assert_eq!(all_elements(b"[{\"a\":1}]").unwrap(), ["{\"a\":1}"]);
        assert_eq!(
            all_elements(br#"[ {"a":[1,2]} , "x]" , 12 ,true]"#).unwrap(),
            ["{\"a\":[1,2]}", "\"x]\"", "12", "true"]
        );
    }

    #[test]
    fn resumes_across_an_element_split_into_single_bytes() {
        let body = br#"[{"k":"v\"]"},{"n":[1]}]"#;
        let mut scanner = ArrayScanner::new();
        let mut held: Vec<u8> = Vec::new();
        let mut out = Vec::new();
        for &b in body {
            held.push(b);
            loop {
                match scanner.scan(&held).unwrap() {
                    Scan::Element { start, end } => {
                        out.push(String::from_utf8(held[start..end].to_vec()).unwrap());
                        held.drain(..end);
                    }
                    Scan::NeedMore { keep_from } => {
                        held.drain(..keep_from);
                        break;
                    }
                    Scan::Done { consumed } => {
                        held.drain(..consumed);
                        break;
                    }
                }
            }
        }
        assert!(scanner.is_done());
        assert_eq!(out, [r#"{"k":"v\"]"}"#, r#"{"n":[1]}"#]);
    }

    #[test]
    fn trims_lines_of_cr_and_blank_space() {
        assert_eq!(trim_line(b"{\"a\":1}\r"), b"{\"a\":1}");
        assert_eq!(trim_line(b"  \r"), b"");
        assert_eq!(next_newline(b"ab\ncd"), Some(2));
        assert_eq!(next_newline(b"abcd"), None);
    }
}
