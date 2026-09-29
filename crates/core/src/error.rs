// Project:   dfe-fetcher
// File:      crates/core/src/error.rs
// Purpose:   Error type shared by every shape crate and the driver
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The framework's error type.
//!
//! Shapes carry the HTTP status they saw in [`Error::Api`], and whether the
//! profile declared that refusal a throttle, so the metrics label comes from
//! typed fields rather than from matching on the message text.

use thiserror::Error;

/// Error raised by a shape, the batcher, the rules or a checkpoint store.
#[derive(Error, Debug)]
pub enum Error {
    /// A profile, instance or rule was rejected at load.
    #[error("configuration error: {0}")]
    Config(String),

    /// The provider could not be fetched for a reason other than an HTTP status.
    #[error("source error: {0}")]
    Source(String),

    /// A rendered request URL named a host the unit may not address, so the
    /// request was refused before its credential was applied.
    #[error("request refused: {0}")]
    OriginRefused(String),

    /// A credential spec did not resolve or a token could not be minted.
    #[error("credential error: {0}")]
    Credential(String),

    /// The provider answered with a non-success HTTP status.
    #[error("API error: status {status}: {text}")]
    Api {
        /// The HTTP status code the provider returned.
        status: u16,
        /// The provider's error text, read from the profile's `error.at` when set.
        text: String,
        /// Whether the refusal was the profile's declared throttle
        /// (`retry.throttle_when`), which a status alone cannot say: AWS
        /// answers 400 with a `ThrottlingException` body.
        throttled: bool,
    },

    /// A response body could not be framed into rows.
    #[error("decode error: {0}")]
    Decode(String),

    /// A page-bounded decoder was handed a page larger than its bound, or a
    /// streaming framer an open row longer than it; the tick aborts rather
    /// than buffer it.
    #[error("page or open row exceeds max_page_bytes ({max} bytes)")]
    OversizePage {
        /// The bound in force.
        max: usize,
    },

    /// An event-window unit hit its page ceiling with rows of the window
    /// still unfetched; the unit's run fails so the window is not advanced
    /// past them, and the driver reads the window again in narrower halves.
    #[error(
        "unit `{unit}` hit max_pages ({max_pages}) with rows of the window still unfetched; raise max_pages or shorten the window"
    )]
    PageCeiling {
        /// The unit.
        unit: String,
        /// The ceiling in force.
        max_pages: u32,
    },

    /// One item of a per-item unit failed, named so the driver can close the
    /// items before it as complete.
    #[error("{source}")]
    Item {
        /// The item's key.
        key: Box<str>,
        /// What went wrong inside it.
        #[source]
        source: Box<Error>,
    },

    /// A CEL filter or transform failed to compile or evaluate.
    #[error("filter error: {0}")]
    Filter(String),

    /// The emit side rejected a batch for a reason other than backpressure.
    #[error("transport error: {0}")]
    Transport(String),

    /// The destination is full; the tick aborts without a checkpoint.
    #[error("destination backpressured: {0}")]
    Backpressured(String),

    /// A checkpoint could not be read or written.
    #[error("cursor error: {0}")]
    Cursor(String),
}

/// Result alias for framework operations.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// The bounded metrics category for this error: `throttle`, `4xx`, `5xx`,
    /// `timeout`, `network`, `origin_refused`, `oversize_page` or
    /// `page_ceiling`.
    ///
    /// An [`Error::Api`] classifies from its TYPED status (an S3 `SlowDown`
    /// is a 503 that means throttle) or from the profile's declared
    /// throttle; anything else is not an HTTP answer, so it is a timeout or
    /// a network failure by its text.
    #[must_use]
    pub fn api_error_code(&self) -> &'static str {
        match self {
            Error::Api {
                throttled: true, ..
            } => "throttle",
            Error::Api { status, text, .. } => match status {
                429 => "throttle",
                408 => "timeout",
                503 if text.contains("SlowDown") => "throttle",
                400..=499 => "4xx",
                500..=599 => "5xx",
                _ => "network",
            },
            Error::OriginRefused(_) => "origin_refused",
            Error::OversizePage { .. } => "oversize_page",
            Error::PageCeiling { .. } => "page_ceiling",
            Error::Item { source, .. } => source.api_error_code(),
            other => non_http_error_code(&other.to_string()),
        }
    }

    /// Wrap this error as the failure of item `key`.
    #[must_use]
    pub fn in_item(self, key: &str) -> Self {
        Error::Item {
            key: key.into(),
            source: Box::new(self),
        }
    }
}

/// The metrics category of a failure that is not an HTTP answer (a refused
/// connection, a checkpoint write, a transport): `timeout` when its text
/// says so, `network` otherwise.
#[must_use]
pub fn non_http_error_code(msg: &str) -> &'static str {
    let lower = msg.to_ascii_lowercase();
    if lower.contains("timed out") || lower.contains("timeout") {
        "timeout"
    } else {
        "network"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_status_classifies_from_the_typed_code_not_the_text() {
        let throttled = Error::Api {
            status: 429,
            text: "nothing about rates in here".into(),
            throttled: false,
        };
        assert_eq!(throttled.api_error_code(), "throttle");
        let forbidden = Error::Api {
            status: 403,
            text: "500 is mentioned in the body".into(),
            throttled: false,
        };
        assert_eq!(forbidden.api_error_code(), "4xx");
        let upstream = Error::Api {
            status: 502,
            text: String::new(),
            throttled: false,
        };
        assert_eq!(upstream.api_error_code(), "5xx");
        let slow = Error::Api {
            status: 408,
            text: String::new(),
            throttled: false,
        };
        assert_eq!(slow.api_error_code(), "timeout");
    }

    /// A refusal the profile declared a throttle classifies as one whatever
    /// its status: AWS paces CloudTrail with a 400 carrying
    /// `ThrottlingException`, which would otherwise be counted a client bug
    /// and never retried.
    #[test]
    fn a_declared_throttle_classifies_as_throttle_not_by_its_status() {
        assert_eq!(
            Error::Api {
                status: 400,
                text: r#"{"__type":"ThrottlingException"}"#.into(),
                throttled: true,
            }
            .api_error_code(),
            "throttle"
        );
        assert_eq!(
            Error::Api {
                status: 400,
                text: r#"{"__type":"ValidationException"}"#.into(),
                throttled: false,
            }
            .api_error_code(),
            "4xx"
        );
    }

    #[test]
    fn non_api_errors_are_a_timeout_or_a_network_failure() {
        assert_eq!(
            Error::Source("connection refused".into()).api_error_code(),
            "network"
        );
        assert_eq!(
            Error::Source("request timed out".into()).api_error_code(),
            "timeout"
        );
        assert_eq!(
            Error::Source("AWS SlowDown: reduce request rate".into()).api_error_code(),
            "network",
            "a throttle is an HTTP answer, never read off the text of another error"
        );
        assert_eq!(
            Error::Api {
                status: 503,
                text: "<Error><Code>SlowDown</Code></Error>".into(),
                throttled: false,
            }
            .api_error_code(),
            "throttle",
            "S3's SlowDown is a 503 that means throttle"
        );
        assert_eq!(
            Error::Api {
                status: 503,
                text: "Service Unavailable".into(),
                throttled: false,
            }
            .api_error_code(),
            "5xx"
        );
    }

    #[test]
    fn the_typed_ceilings_and_an_item_failure_classify_without_reading_text() {
        let ceiling = Error::PageCeiling {
            unit: "sign_ins".into(),
            max_pages: 10,
        };
        assert_eq!(ceiling.api_error_code(), "page_ceiling");
        assert!(ceiling.to_string().contains("max_pages (10)"), "{ceiling}");
        let inside = Error::Api {
            status: 503,
            text: "SlowDown".into(),
            throttled: false,
        }
        .in_item("logs/a.json");
        assert_eq!(
            inside.api_error_code(),
            "throttle",
            "the item's cause classifies"
        );
        assert!(matches!(&inside, Error::Item { key, .. } if &**key == "logs/a.json"));
        assert_eq!(
            inside.to_string(),
            "API error: status 503: SlowDown",
            "the item wrapper adds no text of its own"
        );
    }

    #[test]
    fn display_carries_status_and_text() {
        let err = Error::Api {
            status: 403,
            text: "the API client grant does not permit this".into(),
            throttled: false,
        };
        assert_eq!(
            err.to_string(),
            "API error: status 403: the API client grant does not permit this"
        );
    }
}
