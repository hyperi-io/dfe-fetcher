// Project:   dfe-fetcher
// File:      crates/fetcher/src/extractor/container/lines.rs
// Purpose:   Newline-delimited reads of a container's output with a bound on one line
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Bounded line reads of a container's stdout and stderr.
//!
//! A container is third-party code, so a line that never ends must not grow
//! the fetcher's memory: tokio-util's delimiter codec refuses a line past the
//! bound and discards it up to its newline. The codec is driven here rather
//! than through `FramedRead`, which ends its stream after a decode error.

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::codec::{AnyDelimiterCodec, AnyDelimiterCodecError, Decoder};

/// The most one read takes from the reader.
const READ_BYTES: usize = 8 * 1024;

/// One line of a container's output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Line {
    /// A line within the bound, without its newline.
    Whole(Bytes),
    /// A line past the bound, refused and discarded to its newline.
    TooLong,
}

/// Lines of `reader`, each at most `max` bytes. Cancel-safe: the open line
/// lives in the buffer here, and `read_buf` loses nothing when dropped.
pub(super) struct BoundedLines<R> {
    reader: R,
    buf: BytesMut,
    codec: AnyDelimiterCodec,
    eof: bool,
}

impl<R: AsyncRead + Unpin> BoundedLines<R> {
    /// Lines of `reader`, bounded at `max` bytes.
    pub(super) fn new(reader: R, max: usize) -> Self {
        Self {
            reader,
            buf: BytesMut::new(),
            codec: AnyDelimiterCodec::new_with_max_length(vec![b'\n'], Vec::new(), max),
            eof: false,
        }
    }

    /// The next line, or `None` at the end of the reader.
    ///
    /// # Errors
    ///
    /// The reader's own error.
    pub(super) async fn next_line(&mut self) -> std::io::Result<Option<Line>> {
        loop {
            let decoded = if self.eof {
                self.codec.decode_eof(&mut self.buf)
            } else {
                self.codec.decode(&mut self.buf)
            };
            match decoded {
                Ok(Some(line)) => return Ok(Some(Line::Whole(line))),
                Err(AnyDelimiterCodecError::MaxChunkLengthExceeded) => {
                    return Ok(Some(Line::TooLong));
                }
                Err(AnyDelimiterCodecError::Io(e)) => return Err(e),
                Ok(None) if self.eof => return Ok(None),
                Ok(None) => {}
            }
            self.buf.reserve(READ_BYTES);
            if self.reader.read_buf(&mut self.buf).await? == 0 {
                self.eof = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lines come out whole up to the bound and refused past it, whether a
    /// line ends inside one read or runs across several, and a last line with
    /// no newline still comes out.
    #[tokio::test]
    async fn lines_are_bounded_and_the_read_carries_on() {
        let long = [b"abc\n".as_slice(), &vec![b'x'; 3 * READ_BYTES], b"\n\nxyz"].concat();
        let mut lines = BoundedLines::new(long.as_slice(), 8);
        let mut got = Vec::new();
        while let Some(line) = lines.next_line().await.unwrap() {
            got.push(line);
        }
        assert_eq!(
            got,
            [
                Line::Whole(Bytes::from_static(b"abc")),
                Line::TooLong,
                Line::Whole(Bytes::new()),
                Line::Whole(Bytes::from_static(b"xyz")),
            ]
        );
    }
}
