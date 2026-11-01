// Project:   dfe-fetcher
// File:      crates/rest/src/hooks/s3_uploads.rs
// Purpose:   The S3 listing horizon: ListMultipartUploads walked to the oldest upload in progress
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The S3 listing horizon.
//!
//! S3 lists a multipart upload's object under the time the upload began, so an
//! upload still in progress lands behind objects written while it ran. Before
//! each listing the lister walks `ListMultipartUploads` through the shape's
//! [`ListingPages`] and reads nothing modified at or after the oldest upload's
//! start.

use bytes::Bytes;
use chrono::{DateTime, Utc};

use dfe_fetcher_core::error::{Error, Result};

use super::s3_list::{ListingPages, each_element};

/// The query parameter that makes a bucket GET `ListMultipartUploads`.
pub const UPLOADS_PARAM: &str = "uploads";

/// The statuses that say a store will not list uploads for this credential
/// (`AccessDenied`) or at all (an S3-compatible store without the call).
pub const UPLOADS_UNAVAILABLE: [u16; 5] = [400, 403, 404, 405, 501];

/// Where `ListMultipartUploads` continues: its `NextKeyMarker` and
/// `NextUploadIdMarker`.
pub type UploadsMarker = (String, String);

/// The shape's answer to one `ListMultipartUploads` request.
#[derive(Debug, Clone)]
pub enum UploadsAnswer {
    /// The page, and the store's `Date` for it when it sent one.
    Page {
        /// The XML body.
        body: Bytes,
        /// When the store answered.
        date: Option<DateTime<Utc>>,
    },
    /// A status in [`UPLOADS_UNAVAILABLE`].
    Unavailable,
}

/// The newest `LastModified` a listing may read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Horizon {
    /// Objects modified before `at` are complete: `at` is the start of the
    /// oldest upload in progress, or when the store answered if none was.
    Held {
        /// The bound, exclusive.
        at: DateTime<Utc>,
        /// The key of the upload that set it, if one did.
        upload: Option<String>,
    },
    /// The store would not list its uploads.
    Unavailable,
}

/// One `ListMultipartUploads` page: the oldest upload on it as `(Initiated,
/// Key)`, and where the listing continues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadsPage {
    /// The page's earliest `Initiated`, with its key.
    pub oldest: Option<(DateTime<Utc>, String)>,
    /// The markers for the next page, while `IsTruncated`.
    pub next: Option<UploadsMarker>,
}

/// The bound of the next listing: the oldest `Initiated` across every
/// `ListMultipartUploads` page, else the store's `Date` for the first
/// page (`asked` when it sent none), so an upload begun after this call
/// is held too.
///
/// # Errors
///
/// Returns the request's error, [`Error::Decode`] for a page that is not
/// the documented XML, or [`Error::PageCeiling`] when the uploads still
/// continue after `max_pages`: the oldest may be on a page not read.
pub async fn horizon<P: ListingPages>(
    unit: &str,
    max_pages: u32,
    asked: DateTime<Utc>,
    pages: &mut P,
) -> Result<Horizon> {
    let mut answered: Option<DateTime<Utc>> = None;
    let mut oldest: Option<(DateTime<Utc>, String)> = None;
    let mut marker = None;
    for page in 0..max_pages {
        let UploadsAnswer::Page { body, date } = pages.uploads(marker.take()).await? else {
            return Ok(Horizon::Unavailable);
        };
        answered.get_or_insert(date.unwrap_or(asked));
        let found = parse_list_multipart_uploads(&body)?;
        if let Some(upload) = found.oldest
            && oldest.as_ref().is_none_or(|o| upload < *o)
        {
            oldest = Some(upload);
        }
        match found.next {
            Some(_) if page + 1 == max_pages => {
                return Err(Error::PageCeiling {
                    unit: unit.to_owned(),
                    ceiling: "max_pages",
                    limit: max_pages,
                });
            }
            Some(next) => marker = Some(next),
            None => break,
        }
    }
    let answered = answered.unwrap_or(asked);
    Ok(match oldest {
        Some((at, key)) if at < answered => Horizon::Held {
            at,
            upload: Some(key),
        },
        _ => Horizon::Held {
            at: answered,
            upload: None,
        },
    })
}

/// The oldest upload of a `ListMultipartUploads` page and its continuation
/// markers: only `Key`, `Initiated`, `IsTruncated` and the two `Next*Marker`s
/// are read.
///
/// # Errors
///
/// Returns [`Error::Decode`] when the body is not well-formed XML, or an
/// upload's `Initiated` is not a timestamp: an upload whose start is unknown
/// cannot be held behind.
pub fn parse_list_multipart_uploads(xml: &[u8]) -> Result<UploadsPage> {
    let mut oldest: Option<(DateTime<Utc>, String)> = None;
    let (mut truncated, mut key_marker, mut upload_marker) = (false, None, None);
    let (mut key, mut initiated) = (None, None);
    each_element(xml, "ListMultipartUploads", |name, value| {
        match name {
            "Key" => key = Some(value),
            "Initiated" => {
                let at = value.trim().parse::<DateTime<Utc>>().map_err(|e| {
                    Error::Decode(format!("ListMultipartUploads Initiated `{value}`: {e}"))
                })?;
                initiated = Some(at);
            }
            "IsTruncated" => truncated = value.trim() == "true",
            "NextKeyMarker" => key_marker = Some(value),
            "NextUploadIdMarker" => upload_marker = Some(value),
            "Upload" => {
                let at = initiated.take().ok_or_else(|| {
                    Error::Decode("ListMultipartUploads names an upload with no Initiated".into())
                })?;
                let upload = (at, key.take().unwrap_or_default());
                if oldest.as_ref().is_none_or(|o| upload < *o) {
                    oldest = Some(upload);
                }
            }
            _ => {}
        }
        Ok(())
    })?;
    let next = truncated.then(|| {
        (
            key_marker.unwrap_or_default(),
            upload_marker.unwrap_or_default(),
        )
    });
    Ok(UploadsPage { oldest, next })
}
