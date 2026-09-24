// Project:   dfe-fetcher
// File:      crates/rest/src/hooks/s3_list.rs
// Purpose:   The S3 lister: ListObjectsV2 XML pages walked into one sorted page of items
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The S3 lister.
//!
//! `ListObjectsV2` answers XML in key order, paged by `NextContinuationToken`,
//! and an object-store unit reads objects once in `last_modified` order so
//! the newest one it has read is its checkpoint. Neither fits the JSON
//! pagers, so the lister stands where the page fetch would: it walks the
//! pages through the [`ListingPages`] the shape hands it (each request
//! signed like every other), keeps the keys modified after the cutoff,
//! sorts them, and yields the listing as ONE page of items for the unit's
//! manifest, each `{key, path, last_modified, size}` with `path` the key
//! encoded for a URL path. Nothing here sends: the shape renders and signs
//! the listing request, the lister only decides what to ask for next and
//! what the pages hold.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use quick_xml::Reader;
use quick_xml::events::{BytesRef, Event};

use dfe_fetcher_core::error::{Error, Result};

use crate::profile::ListerKind;

/// RFC 3986 with only the unreserved set bare: what S3 signs and decodes a
/// query value or a key segment as (a space is `%20`, never `+`).
pub(crate) const RFC3986: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// The query parameter the lister injects to continue a listing.
pub const CONTINUATION_PARAM: &str = "continuation-token";

/// A listing protocol standing in for a unit's page fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lister {
    /// S3 `ListObjectsV2`.
    S3,
}

/// What a lister asks the shape for: one listing page per continuation
/// token, `None` when the provider answered a status the request ignores.
pub trait ListingPages: Send {
    /// The page for `token` (the first page for `None`).
    fn page(&mut self, token: Option<String>) -> BoxFuture<'_, Result<Option<Bytes>>>;
}

/// One object of a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    /// The object key as the bucket names it.
    pub key: String,
    /// When the object was last written.
    pub last_modified: DateTime<Utc>,
    /// Object size in bytes, 0 when the listing did not say.
    pub size: u64,
}

impl Listed {
    /// The item a manifest reads, framed as one JSON row: the key, its
    /// URL-path form, the timestamp as RFC 3339 and the size.
    #[must_use]
    pub fn item(&self) -> Bytes {
        let path = self
            .key
            .split('/')
            .map(|segment| utf8_percent_encode(segment, RFC3986).to_string())
            .collect::<Vec<_>>()
            .join("/");
        let item = serde_json::json!({
            "key": self.key,
            "path": path,
            "last_modified": self.last_modified.to_rfc3339(),
            "size": self.size,
        });
        Bytes::from(item.to_string())
    }
}

/// What one listing produced: the item rows, and whether the walk stopped on
/// a continuation token rather than the end of the listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    /// The items, oldest first, ready for the unit's manifest.
    pub items: Vec<Bytes>,
    /// The page ceiling was reached with more keys to list; the rest of the
    /// prefix waits for the next tick.
    pub truncated: bool,
}

/// Whether an object sits past the checkpoint: later than its position, or at
/// the same position under a key sorted after it.
///
/// `LastModified` is second-resolution, so a run of objects shares one
/// position and only the key orders them.
fn past_cutoff(object: &Listed, cutoff: Option<&(DateTime<Utc>, String)>) -> bool {
    match cutoff {
        None => true,
        Some((position, key)) => {
            object.last_modified > *position
                || (object.last_modified == *position && object.key > *key)
        }
    }
}

/// The first `max_items` objects, extended through any run that shares the
/// last one's `last_modified`.
///
/// A cut inside such a run commits a position the objects left behind also
/// carry, and the next tick skips them.
fn cut_at_a_position_boundary(mut objects: Vec<Listed>, max_items: Option<u32>) -> Vec<Listed> {
    let Some(max) = max_items.map(|m| m as usize) else {
        return objects;
    };
    if max == 0 || objects.len() <= max {
        return objects;
    }
    let boundary = objects[max - 1].last_modified;
    let tie = objects[max..]
        .iter()
        .take_while(|o| o.last_modified == boundary)
        .count();
    objects.truncate(max + tie);
    objects
}

impl Lister {
    /// The lister an endpoint names, if any.
    #[must_use]
    pub fn build(kind: Option<ListerKind>) -> Option<Self> {
        kind.map(|kind| match kind {
            ListerKind::S3 => Lister::S3,
        })
    }

    /// The listing as item rows: `pages` fetches one listing page per
    /// continuation token, walked up to `max_pages` times; the keys past
    /// `cutoff` come back sorted by `last_modified` then key, oldest first,
    /// cut to `max_items` at a position boundary.
    ///
    /// # Errors
    ///
    /// Returns the page fetch's error, or [`Error::Decode`] for a page that
    /// is not the XML the protocol documents.
    pub async fn list<P: ListingPages>(
        self,
        cutoff: Option<(DateTime<Utc>, String)>,
        max_items: Option<u32>,
        max_pages: u32,
        pages: &mut P,
    ) -> Result<Listing> {
        let Lister::S3 = self;
        let mut out: Vec<Listed> = Vec::new();
        let mut continuation: Option<String> = None;
        let mut truncated = false;
        for page in 0..max_pages {
            let Some(body) = pages.page(continuation.clone()).await? else {
                break;
            };
            let (objects, next) = parse_list_objects_v2(&body)?;
            out.extend(
                objects
                    .into_iter()
                    .filter(|o| past_cutoff(o, cutoff.as_ref())),
            );
            match next {
                Some(token) => {
                    continuation = Some(token);
                    truncated = page + 1 == max_pages;
                }
                None => break,
            }
        }
        out.sort_by(|a, b| (a.last_modified, &a.key).cmp(&(b.last_modified, &b.key)));
        let out = cut_at_a_position_boundary(out, max_items);
        Ok(Listing {
            items: out.iter().map(Listed::item).collect(),
            truncated,
        })
    }
}

/// The objects of a `ListObjectsV2` page and its `NextContinuationToken`:
/// only `Key`, `LastModified`, `Size` and the token are read, so an element
/// the schema grows is ignored.
///
/// # Errors
///
/// Returns [`Error::Decode`] when the body is not well-formed XML.
pub fn parse_list_objects_v2(xml: &[u8]) -> Result<(Vec<Listed>, Option<String>)> {
    let text = std::str::from_utf8(xml)
        .map_err(|e| Error::Decode(format!("ListObjectsV2 page is not UTF-8: {e}")))?;
    let mut reader = Reader::from_str(text);
    // Text is not trimmed per event: an element's text arrives in one event per
    // run between entity references, so trimming each run would drop a space
    // that sits beside one.
    let mut objects = Vec::new();
    let mut next_token = None;
    let mut key: Option<String> = None;
    let mut modified: Option<DateTime<Utc>> = None;
    let mut size: u64 = 0;
    let mut in_contents = false;
    let mut tag: Option<String> = None;
    // An element's text arrives in one event per run between entity
    // references, so it is accumulated here and read at the closing tag; a key
    // carrying `&` or `<` would otherwise keep only its last fragment and be
    // fetched under a name the bucket does not have.
    let mut value = String::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = e.name().into_inner().to_owned();
                if name == "Contents" {
                    in_contents = true;
                    key = None;
                    modified = None;
                    size = 0;
                }
                tag = Some(name);
                value.clear();
            }
            Ok(Event::End(e)) => {
                if e.name().into_inner() == "Contents" {
                    if let (Some(key), Some(last_modified)) = (key.take(), modified.take()) {
                        objects.push(Listed {
                            key,
                            last_modified,
                            size,
                        });
                    }
                    in_contents = false;
                }
                match tag.as_deref() {
                    Some("Key") if in_contents => key = Some(std::mem::take(&mut value)),
                    Some("LastModified") if in_contents => {
                        modified = value.trim().parse::<DateTime<Utc>>().ok();
                    }
                    Some("Size") if in_contents => size = value.trim().parse().unwrap_or(0),
                    Some("NextContinuationToken") => {
                        next_token = Some(std::mem::take(&mut value));
                    }
                    _ => {}
                }
                tag = None;
                value.clear();
            }
            Ok(Event::Text(t)) => value.push_str(&t),
            Ok(Event::GeneralRef(r)) => value.push(entity_char(&r)?),
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => {
                return Err(Error::Decode(format!(
                    "ListObjectsV2 page is not the XML the protocol documents: {e}"
                )));
            }
        }
    }
    Ok((objects, next_token))
}

/// The character a `&name;` or `&#nn;` reference stands for.
///
/// A listing carries no DTD, so the five predefined names and numeric
/// references are everything that can appear.
fn entity_char(reference: &BytesRef<'_>) -> Result<char> {
    if let Some(c) = reference
        .resolve_char_ref()
        .map_err(|e| Error::Decode(format!("ListObjectsV2 character reference: {e}")))?
    {
        return Ok(c);
    }
    match &**reference {
        "amp" => Ok('&'),
        "lt" => Ok('<'),
        "gt" => Ok('>'),
        "quot" => Ok('"'),
        "apos" => Ok('\''),
        other => Err(Error::Decode(format!(
            "ListObjectsV2 names the undefined entity `&{other};`"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use futures::FutureExt;
    use serde_json::Value;

    /// Scripted pages: the token asked for is recorded, the body answered
    /// comes from the script.
    struct Scripted {
        asked: Vec<Option<String>>,
        answer: fn(Option<&str>) -> Option<&'static [u8]>,
    }

    impl ListingPages for Scripted {
        fn page(&mut self, token: Option<String>) -> BoxFuture<'_, Result<Option<Bytes>>> {
            self.asked.push(token.clone());
            let body = (self.answer)(token.as_deref()).map(Bytes::from_static);
            async move { Ok(body) }.boxed()
        }
    }

    const PAGE_ONE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>my-bucket</Name>
    <Prefix>logs/</Prefix>
    <KeyCount>2</KeyCount>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>true</IsTruncated>
    <NextContinuationToken>abc123</NextContinuationToken>
    <Contents>
        <Key>logs/2026/05/21/file-002.json.gz</Key>
        <LastModified>2026-05-21T11:30:45.500Z</LastModified>
        <ETag>"cafebabe"</ETag>
        <Size>9999</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
    <Contents>
        <Key>logs/2026/05/21/file 001.json.gz</Key>
        <LastModified>2026-05-21T10:00:00.000Z</LastModified>
        <ETag>"deadbeef"</ETag>
        <Size>4321</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
</ListBucketResult>"#;

    const PAGE_TWO: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>my-bucket</Name>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>logs/2026/05/20/old.json.gz</Key>
        <LastModified>2026-05-20T10:00:00.000Z</LastModified>
        <Size>1</Size>
    </Contents>
</ListBucketResult>"#;

    #[test]
    fn a_page_yields_its_keys_and_its_continuation_token() {
        let (objects, token) = parse_list_objects_v2(PAGE_ONE.as_bytes()).unwrap();
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].key, "logs/2026/05/21/file-002.json.gz");
        assert_eq!(objects[0].size, 9999);
        assert_eq!(objects[1].key, "logs/2026/05/21/file 001.json.gz");
        assert_eq!(token.as_deref(), Some("abc123"));
        let (objects, token) = parse_list_objects_v2(PAGE_TWO.as_bytes()).unwrap();
        assert_eq!(objects.len(), 1);
        assert!(token.is_none());
        assert!(
            parse_list_objects_v2(b"<ListBucketResult></Contents>").is_err(),
            "an end tag that closes nothing"
        );
    }

    #[tokio::test]
    async fn the_listing_walks_the_tokens_filters_by_the_cutoff_and_sorts_by_time() {
        let mut pages = Scripted {
            asked: Vec::new(),
            answer: |token| {
                Some(match token {
                    None => PAGE_ONE.as_bytes(),
                    Some("abc123") => PAGE_TWO.as_bytes(),
                    Some(_) => b"",
                })
            },
        };
        let items: Vec<Value> = Lister::S3
            .list(
                Some((
                    Utc.with_ymd_and_hms(2026, 5, 20, 12, 0, 0).unwrap(),
                    String::new(),
                )),
                None,
                10,
                &mut pages,
            )
            .await
            .unwrap()
            .items
            .iter()
            .map(|row| serde_json::from_slice(row).unwrap())
            .collect();
        assert_eq!(pages.asked, [None, Some("abc123".to_owned())]);
        let keys: Vec<&str> = items.iter().map(|i| i["key"].as_str().unwrap()).collect();
        assert_eq!(
            keys,
            [
                "logs/2026/05/21/file 001.json.gz",
                "logs/2026/05/21/file-002.json.gz"
            ],
            "the old object is before the cutoff; the rest oldest first"
        );
        assert_eq!(
            items[0]["path"], "logs/2026/05/21/file%20001.json.gz",
            "the key encoded for a URL path, slashes kept"
        );
        assert_eq!(items[0]["last_modified"], "2026-05-21T10:00:00+00:00");
        assert_eq!(items[1]["last_modified"], "2026-05-21T11:30:45.500+00:00");
        assert_eq!(items[0]["size"], 4321);
    }

    #[tokio::test]
    async fn the_page_ceiling_bounds_the_walk_and_an_ignored_status_ends_it() {
        let mut one = Scripted {
            asked: Vec::new(),
            answer: |_| Some(PAGE_ONE.as_bytes()),
        };
        let listing = Lister::S3.list(None, None, 1, &mut one).await.unwrap();
        assert_eq!(listing.items.len(), 2, "one page, its token not followed");
        assert!(
            listing.truncated,
            "the walk stopped on a token, so the prefix has more keys"
        );
        let mut ignored = Scripted {
            asked: Vec::new(),
            answer: |_| None,
        };
        let listing = Lister::S3.list(None, None, 5, &mut ignored).await.unwrap();
        assert!(listing.items.is_empty());
        assert!(!listing.truncated);
    }

    /// A key carrying XML entities is unescaped to what the bucket really
    /// calls the object, and percent-encoded from THAT for the GET path.
    #[test]
    fn a_key_with_xml_entities_is_unescaped_to_the_objects_real_name() {
        const ESCAPED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>logs/a&amp;b &lt;1&gt;.json</Key>
        <LastModified>2026-05-21T10:00:00.000Z</LastModified>
        <Size>1</Size>
    </Contents>
</ListBucketResult>"#;
        let (objects, _) = parse_list_objects_v2(ESCAPED.as_bytes()).unwrap();
        assert_eq!(objects[0].key, "logs/a&b <1>.json");
        let item: Value = serde_json::from_slice(&objects[0].item()).unwrap();
        assert_eq!(item["key"], "logs/a&b <1>.json");
        assert_eq!(
            item["path"], "logs/a%26b%20%3C1%3E.json",
            "the path is encoded from the real key, not from the escaped one"
        );
    }

    /// Objects sharing one `LastModified` are never split by the item cut:
    /// the checkpoint is a position plus a key, and a cut inside a run would
    /// commit a position the objects left behind also carry.
    #[test]
    fn the_item_cut_never_splits_a_run_of_equal_timestamps() {
        let at = |secs: i64| Utc.timestamp_opt(secs, 0).single().unwrap();
        let listed = |key: &str, secs: i64| Listed {
            key: key.to_owned(),
            last_modified: at(secs),
            size: 1,
        };
        // Three objects at the same second, one later.
        let objects = vec![
            listed("a", 100),
            listed("b", 100),
            listed("c", 100),
            listed("d", 200),
        ];
        let cut = cut_at_a_position_boundary(objects.clone(), Some(2));
        assert_eq!(
            cut.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
            ["a", "b", "c"],
            "the cut extends through the tie rather than stopping inside it"
        );
        assert_eq!(
            cut_at_a_position_boundary(objects.clone(), Some(4)).len(),
            4,
            "a cut at or past the end keeps everything"
        );
        assert_eq!(cut_at_a_position_boundary(objects, None).len(), 4);
    }

    /// The cutoff is a position AND a key: an object at the committed second
    /// whose key sorts after the committed one is still unread, while the
    /// committed object itself is not read twice.
    #[test]
    fn the_cutoff_breaks_a_same_second_tie_by_key() {
        let at = |secs: i64| Utc.timestamp_opt(secs, 0).single().unwrap();
        let object = |key: &str, secs: i64| Listed {
            key: key.to_owned(),
            last_modified: at(secs),
            size: 1,
        };
        let cutoff = Some((at(100), "b".to_owned()));
        assert!(
            !past_cutoff(&object("a", 100), cutoff.as_ref()),
            "sorted before the committed key at the same second: already read"
        );
        assert!(
            !past_cutoff(&object("b", 100), cutoff.as_ref()),
            "the committed object itself is not read again"
        );
        assert!(
            past_cutoff(&object("c", 100), cutoff.as_ref()),
            "the same second, a later key: never read"
        );
        assert!(past_cutoff(&object("a", 200), cutoff.as_ref()));
        assert!(past_cutoff(&object("a", 100), None), "no cutoff, read it");
    }
}
