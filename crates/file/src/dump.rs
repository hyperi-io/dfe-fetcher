// Project:   dfe-fetcher
// File:      crates/file/src/dump.rs
// Purpose:   The file dump shape: every file a glob matches, read once, streamed by chunk into rows
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The file dump shape.
//!
//! A tick lists the files the globs match, drops the ones already read, and
//! streams the rest oldest first: each file is read in chunks of
//! `chunk_bytes` (inflated first when it starts with the gzip magic, member
//! after member), framed by the decoder its extension or the spec names, and
//! every row carries `Mark::Item { key: path, position: ctime }`. The driver
//! wraps each file as its own snapshot (`begin`, rows, `end`), folds the
//! file's marks into the unit's checkpoint once its last batch is
//! acknowledged, and the next tick skips every file whose change time is at
//! or before the committed one. A tick with no new file publishes nothing.
//!
//! The change time (`ctime`), not the modification time, is the done marker:
//! a file published by write-to-temp-then-rename keeps its old `mtime` but
//! gets a fresh `ctime` at the rename, so it is never older than a file
//! already read. A file rewritten in place gets a new `ctime` too and is read
//! again as a new snapshot.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, TimeZone, Utc};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};
use tracing::{debug, warn};

use dfe_fetcher_core::batch::Lease;
use dfe_fetcher_core::checkpoint::CheckpointValue;
use dfe_fetcher_core::error::{Error, Result};
use dfe_fetcher_core::frame::{ArrayFramer, CsvFramer, Framer, LeasedBlock, LineFramer, framed};
use dfe_fetcher_core::{Mark, Row, RowStream};

use crate::FileSource;

/// The gzip member magic; a file starting with it is inflated whatever its name.
const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];

/// How a dump frames each file.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DumpDecoder {
    /// By extension: `.jsonl` / `.ndjson` NDJSON, `.json` array, `.csv` CSV;
    /// gzip by magic.
    #[default]
    Auto,
    /// One JSON object per line.
    Ndjson,
    /// A top-level JSON array.
    JsonArray,
    /// CSV with a header row.
    Csv,
}

impl DumpDecoder {
    /// The decoder for `path`: this one, or for `Auto` the one its extension
    /// (after a trailing `.gz`) names.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when `Auto` cannot tell from the extension.
    pub fn for_path(self, path: &Path) -> Result<Self> {
        if self != DumpDecoder::Auto {
            return Ok(self);
        }
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let stem = name.strip_suffix(".gz").unwrap_or(name);
        let ext = stem.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase());
        match ext.as_deref() {
            Some("jsonl" | "ndjson") => Ok(DumpDecoder::Ndjson),
            Some("json") => Ok(DumpDecoder::JsonArray),
            Some("csv") => Ok(DumpDecoder::Csv),
            _ => Err(Error::Config(format!(
                "{}: cannot pick a decoder from the extension; set `decoder`",
                path.display()
            ))),
        }
    }

    /// The framer for this decoder, its open row bounded by `max_row_bytes`.
    fn framer(self, max_row_bytes: usize) -> Box<dyn Framer + Unpin> {
        match self {
            DumpDecoder::Auto | DumpDecoder::Ndjson => {
                Box::new(LineFramer::new(false).bounded(max_row_bytes))
            }
            DumpDecoder::JsonArray => Box::new(ArrayFramer::default().bounded(max_row_bytes)),
            DumpDecoder::Csv => Box::new(CsvFramer::new(true)),
        }
    }
}

/// A directory dump: every file the globs match, once each.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DumpSpec {
    /// Globs of the files to read.
    pub paths: Vec<String>,
    /// How each file is framed.
    pub decoder: DumpDecoder,
    /// Bytes read from a file per chunk; one chunk plus the open row is what
    /// a reader holds ahead of the driver.
    pub chunk_bytes: usize,
    /// Longest one row may run before its boundary; a file with no boundary
    /// at all (a `.json` document declared `ndjson`) fails here instead of
    /// being read whole into memory.
    pub max_row_bytes: usize,
}

impl Default for DumpSpec {
    fn default() -> Self {
        Self {
            paths: Vec::new(),
            decoder: DumpDecoder::Auto,
            chunk_bytes: 64 * 1024,
            max_row_bytes: 16 * 1024 * 1024,
        }
    }
}

impl DumpSpec {
    /// Reject a dump with nothing to read, a glob that does not parse, or no
    /// room to read.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] naming the field.
    pub fn validate(&self) -> Result<()> {
        if self.paths.iter().all(|p| p.trim().is_empty()) {
            return Err(Error::Config(
                "file dump `paths` must name at least one glob".into(),
            ));
        }
        for pattern in &self.paths {
            glob::Pattern::new(pattern)
                .map_err(|e| Error::Config(format!("file dump `paths` entry `{pattern}`: {e}")))?;
        }
        if self.chunk_bytes == 0 {
            return Err(Error::Config("file dump `chunk_bytes` must be > 0".into()));
        }
        if self.max_row_bytes == 0 {
            return Err(Error::Config(
                "file dump `max_row_bytes` must be > 0".into(),
            ));
        }
        Ok(())
    }
}

/// One file the globs matched, with the change time that is its done marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The path as the glob produced it.
    pub path: PathBuf,
    /// Inode change time (last write or rename into place).
    pub changed_at: DateTime<Utc>,
}

/// The inode change time of `meta`, falling back to the modification time
/// where the platform has no `ctime`.
fn change_time(meta: &std::fs::Metadata) -> DateTime<Utc> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Some(at) = Utc
            .timestamp_opt(meta.ctime(), u32::try_from(meta.ctime_nsec()).unwrap_or(0))
            .single()
        {
            return at;
        }
    }
    meta.modified()
        .map(DateTime::<Utc>::from)
        .unwrap_or_else(|_| Utc::now())
}

/// The files `patterns` match right now, oldest change first, without the
/// ones changed at or before `after`.
///
/// # Errors
///
/// Returns [`Error::Config`] for a glob that does not parse and
/// [`Error::Source`] for a directory that cannot be read.
pub fn list(patterns: &[String], after: Option<DateTime<Utc>>) -> Result<Vec<Candidate>> {
    let mut found = Vec::new();
    for pattern in patterns {
        let entries = glob::glob(pattern)
            .map_err(|e| Error::Config(format!("file dump glob `{pattern}`: {e}")))?;
        for entry in entries {
            let path = entry.map_err(|e| {
                Error::Source(format!(
                    "file dump: cannot read {}: {}",
                    e.path().display(),
                    e.error()
                ))
            })?;
            let meta = std::fs::metadata(&path).map_err(|e| {
                Error::Source(format!("file dump: cannot stat {}: {e}", path.display()))
            })?;
            if !meta.is_file() {
                continue;
            }
            let changed_at = change_time(&meta);
            if after.is_some_and(|done| changed_at <= done) {
                continue;
            }
            found.push(Candidate { path, changed_at });
        }
    }
    found.sort_by(|a, b| (a.changed_at, &a.path).cmp(&(b.changed_at, &b.path)));
    found.dedup();
    Ok(found)
}

/// Open `path` for reading, inflating it when it starts with the gzip magic.
async fn open(path: &Path) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| Error::Source(format!("file dump: cannot open {}: {e}", path.display())))?;
    let mut reader = BufReader::new(file);
    let head = reader
        .fill_buf()
        .await
        .map_err(|e| Error::Source(format!("file dump: cannot read {}: {e}", path.display())))?;
    if head.starts_with(GZIP_MAGIC) {
        let mut decoder = async_compression::tokio::bufread::GzipDecoder::new(reader);
        decoder.multiple_members(true);
        Ok(Box::new(decoder))
    } else {
        Ok(Box::new(reader))
    }
}

/// The bytes of one file as leased chunks of at most `chunk_bytes`.
fn chunks(
    path: PathBuf,
    chunk_bytes: usize,
    lease: Arc<dyn Lease>,
) -> BoxStream<'static, Result<LeasedBlock>> {
    let start = async move {
        let reader = open(&path).await?;
        Ok::<_, Error>(futures::stream::unfold(
            (reader, path, lease, false),
            move |(mut reader, path, lease, done)| async move {
                if done {
                    return None;
                }
                let mut buf = BytesMut::with_capacity(chunk_bytes);
                let read = match reader.read_buf(&mut buf).await {
                    Ok(n) => n,
                    Err(e) => {
                        let problem = Error::Decode(format!("{}: {e}", path.display()));
                        return Some((Err(problem), (reader, path, lease, true)));
                    }
                };
                if read == 0 {
                    return None;
                }
                let block = LeasedBlock::new(buf.freeze(), Arc::clone(&lease));
                Some((Ok(block), (reader, path, lease, false)))
            },
        ))
    };
    futures::stream::once(start).try_flatten().boxed()
}

/// The rows of one file, each marked with the file's path and change time;
/// every failure inside the file, before its first row or after, names the
/// file as its item so the driver knows which snapshot it belongs to.
fn file_rows(
    candidate: Candidate,
    decoder: DumpDecoder,
    chunk_bytes: usize,
    max_row_bytes: usize,
    lease: Arc<dyn Lease>,
) -> RowStream<'static> {
    let key: Box<str> = candidate.path.to_string_lossy().into();
    let framer = match decoder.for_path(&candidate.path) {
        Ok(d) => d.framer(max_row_bytes),
        Err(e) => {
            let e = e.in_item(&key);
            return futures::stream::once(async move { Err(e) }).boxed();
        }
    };
    let position = candidate.changed_at;
    let shown = candidate.path.display().to_string();
    debug!(path = %shown, "file dump: reading");
    framed(chunks(candidate.path, chunk_bytes, lease), framer)
        .map(move |row| {
            let payload: Bytes = row.map_err(|e| {
                match e {
                    Error::Decode(text) if !text.starts_with(&shown) => {
                        Error::Decode(format!("{shown}: {text}"))
                    }
                    other => other,
                }
                .in_item(&key)
            })?;
            Ok(Row {
                payload,
                mark: Some(Mark::Item {
                    key: key.clone(),
                    position,
                }),
            })
        })
        .boxed()
}

/// One dump unit: its globs, decoder and chunk size.
pub struct FileDump {
    spec: DumpSpec,
    lease: Arc<dyn Lease>,
    unit: Arc<str>,
}

impl std::fmt::Debug for FileDump {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileDump")
            .field("unit", &self.unit)
            .field("spec", &self.spec)
            .finish_non_exhaustive()
    }
}

impl FileDump {
    /// A dump over `spec`, leasing every chunk it holds on `lease`.
    #[must_use]
    pub fn new(unit: &str, spec: DumpSpec, lease: Arc<dyn Lease>) -> Self {
        Self {
            spec,
            lease,
            unit: Arc::from(unit),
        }
    }

    /// The committed change time, if the checkpoint is a file dump's.
    fn done_before(checkpoint: Option<&CheckpointValue>) -> Result<Option<DateTime<Utc>>> {
        match checkpoint {
            None => Ok(None),
            Some(CheckpointValue::Item { position, .. }) => Ok(Some(*position)),
            Some(other) => Err(Error::Cursor(format!(
                "file dump has a {} checkpoint where a file marker was expected",
                match other {
                    CheckpointValue::Keyset(_) => "keyset",
                    CheckpointValue::Lines(_) => "file tail",
                    CheckpointValue::Item { .. } => "file marker",
                }
            ))),
        }
    }
}

impl FileSource for FileDump {
    /// SHORTCUT: files are read one at a time per unit, in change-time order,
    /// and units of an instance run sequentially in the driver. Lift (read
    /// files concurrently with `buffer_unordered(n)`) when a unit's file
    /// count x per-file read time exceeds its interval; row order inside one
    /// snapshot is not a contract, so nothing else changes.
    fn rows<'a>(&'a self, checkpoint: Option<&'a CheckpointValue>) -> RowStream<'a> {
        let after = match Self::done_before(checkpoint) {
            Ok(after) => after,
            Err(e) => return futures::stream::once(async move { Err(e) }).boxed(),
        };
        let files = match list(&self.spec.paths, after) {
            Ok(files) => files,
            Err(e) => return futures::stream::once(async move { Err(e) }).boxed(),
        };
        debug!(unit = %self.unit, files = files.len(), "file dump: tick");
        let decoder = self.spec.decoder;
        let chunk_bytes = self.spec.chunk_bytes;
        let max_row_bytes = self.spec.max_row_bytes;
        let lease = Arc::clone(&self.lease);
        futures::stream::iter(files)
            .flat_map(move |candidate| {
                file_rows(
                    candidate,
                    decoder,
                    chunk_bytes,
                    max_row_bytes,
                    Arc::clone(&lease),
                )
            })
            .boxed()
    }

    fn probe(&self) -> BoxFuture<'_, Result<()>> {
        async move {
            for pattern in &self.spec.paths {
                let base = glob_base(pattern);
                if !base.is_dir() {
                    warn!(unit = %self.unit, pattern, base = %base.display(), "file dump: base directory absent");
                    return Err(Error::Source(format!(
                        "file dump: base directory of `{pattern}` does not exist"
                    )));
                }
            }
            Ok(())
        }
        .boxed()
    }
}

/// The literal directory prefix of a glob: everything before the first
/// component that carries a wildcard.
#[must_use]
pub fn glob_base(pattern: &str) -> PathBuf {
    let mut base = PathBuf::new();
    for component in Path::new(pattern).components() {
        let text = component.as_os_str().to_string_lossy();
        if text.contains(['*', '?', '[']) {
            break;
        }
        base.push(component);
    }
    if base.as_os_str().is_empty() {
        base.push(".");
    }
    base
}

#[cfg(test)]
mod tests {
    use super::*;
    use dfe_fetcher_core::batch::NoLease;
    use std::io::Write as _;

    fn no_lease() -> Arc<dyn Lease> {
        Arc::new(NoLease)
    }

    fn gz(data: &[u8]) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    async fn collect(dump: &FileDump, cp: Option<&CheckpointValue>) -> Result<Vec<Row>> {
        dump.rows(cp).try_collect().await
    }

    #[test]
    fn a_dump_needs_a_parseable_glob_and_a_chunk_size() {
        assert!(DumpSpec::default().validate().is_err());
        let spec: DumpSpec = serde_json::from_str(r#"{"paths": ["/data/*.jsonl.gz"]}"#).unwrap();
        spec.validate().unwrap();
        assert_eq!(spec.decoder, DumpDecoder::Auto);
        assert_eq!(spec.chunk_bytes, 64 * 1024);
        assert_eq!(spec.max_row_bytes, 16 * 1024 * 1024);
        let zero: DumpSpec = serde_json::from_str(r#"{"paths": ["x"], "chunk_bytes": 0}"#).unwrap();
        assert!(zero.validate().is_err());
        let unbounded: DumpSpec =
            serde_json::from_str(r#"{"paths": ["x"], "max_row_bytes": 0}"#).unwrap();
        assert!(unbounded.validate().is_err());
        let bad: DumpSpec = serde_json::from_str(r#"{"paths": ["/data/[.jsonl"]}"#).unwrap();
        assert!(bad.validate().unwrap_err().to_string().contains("[.jsonl"));
        assert!(
            serde_json::from_str::<DumpSpec>(r#"{"path": "x"}"#).is_err(),
            "unknown key"
        );
    }

    #[test]
    fn auto_picks_the_decoder_from_the_extension_under_a_gz_suffix() {
        let auto = DumpDecoder::Auto;
        assert_eq!(
            auto.for_path(Path::new("a.jsonl")).unwrap(),
            DumpDecoder::Ndjson
        );
        assert_eq!(
            auto.for_path(Path::new("a.NDJSON.gz")).unwrap(),
            DumpDecoder::Ndjson
        );
        assert_eq!(
            auto.for_path(Path::new("a.json.gz")).unwrap(),
            DumpDecoder::JsonArray
        );
        assert_eq!(auto.for_path(Path::new("a.csv")).unwrap(), DumpDecoder::Csv);
        assert!(auto.for_path(Path::new("a.txt")).is_err());
        assert!(auto.for_path(Path::new("noext")).is_err());
        assert_eq!(
            DumpDecoder::Csv.for_path(Path::new("a.json")).unwrap(),
            DumpDecoder::Csv,
            "an explicit decoder wins"
        );
    }

    #[test]
    fn the_glob_base_is_the_literal_prefix() {
        assert_eq!(
            glob_base("/data/exports/**/*.jsonl"),
            Path::new("/data/exports")
        );
        assert_eq!(
            glob_base("/data/exports/a.jsonl"),
            Path::new("/data/exports/a.jsonl")
        );
        assert_eq!(glob_base("*.csv"), Path::new("."));
    }

    #[tokio::test]
    async fn every_format_and_gzip_variant_frames_one_row_per_record() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.jsonl"), "{\"id\":1}\n{\"id\":2}\n").unwrap();
        std::fs::write(dir.path().join("b.json"), "[{\"id\":3},\n {\"id\":4}]").unwrap();
        std::fs::write(dir.path().join("c.csv"), "id,name\n5,\"x\ny\"\n6,z\n").unwrap();
        let mut two_members = gz(b"{\"id\":7}\n");
        two_members.extend(gz(b"{\"id\":8}\n"));
        std::fs::write(dir.path().join("d.jsonl.gz"), two_members).unwrap();
        std::fs::write(dir.path().join("e.json.gz"), gz(b"[{\"id\":9}]")).unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub").join("f.csv.gz"), gz(b"id\n10\n")).unwrap();
        std::fs::write(dir.path().join("ignored.txt"), "nope").unwrap();

        let spec = DumpSpec {
            paths: vec![
                format!("{}/*.jsonl*", dir.path().display()),
                format!("{}/*.json*", dir.path().display()),
                format!("{}/**/*.csv*", dir.path().display()),
            ],
            decoder: DumpDecoder::Auto,
            chunk_bytes: 7,
            ..DumpSpec::default()
        };
        let dump = FileDump::new("u", spec, no_lease());
        let rows = collect(&dump, None).await.unwrap();
        let mut ids: Vec<i64> = rows
            .iter()
            .map(|r| {
                let v: serde_json::Value = serde_json::from_slice(&r.payload).unwrap();
                match &v["id"] {
                    serde_json::Value::Number(n) => n.as_i64().unwrap(),
                    serde_json::Value::String(s) => s.parse().unwrap(),
                    other => panic!("{other}"),
                }
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let csv_row: serde_json::Value = serde_json::from_slice(
            &rows
                .iter()
                .find(|r| r.payload.starts_with(b"{\"id\":\"5\""))
                .unwrap()
                .payload,
        )
        .unwrap();
        assert_eq!(csv_row["name"], "x\ny");
        for row in &rows {
            match &row.mark {
                Some(Mark::Item { key, .. }) => {
                    assert!(key.starts_with(dir.path().to_str().unwrap()));
                }
                other => panic!("every row carries its file marker, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn a_committed_marker_skips_files_changed_at_or_before_it_and_reads_the_rest() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("old.jsonl"), "{\"id\":1}\n").unwrap();
        let spec = DumpSpec {
            paths: vec![format!("{}/*.jsonl", dir.path().display())],
            ..DumpSpec::default()
        };
        let dump = FileDump::new("u", spec, no_lease());
        let first = collect(&dump, None).await.unwrap();
        assert_eq!(first.len(), 1);
        let Some(Mark::Item { key, position }) = first[0].mark.clone() else {
            panic!("marker");
        };
        let committed = CheckpointValue::Item {
            key: key.into_string(),
            position,
        };
        assert!(
            collect(&dump, Some(&committed)).await.unwrap().is_empty(),
            "the file is never re-read"
        );

        // A file published after the commit, even with an older mtime
        // (write-to-temp then rename keeps the temp file's mtime).
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let tmp = dir.path().join("new.tmp");
        std::fs::write(&tmp, "{\"id\":2}\n").unwrap();
        let old = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        std::fs::File::open(&tmp)
            .unwrap()
            .set_modified(old)
            .unwrap();
        std::fs::rename(&tmp, dir.path().join("new.jsonl")).unwrap();
        let second = collect(&dump, Some(&committed)).await.unwrap();
        assert_eq!(second.len(), 1, "the renamed-in file is new by ctime");
        assert_eq!(&second[0].payload[..], b"{\"id\":2}");
    }

    #[tokio::test]
    async fn files_stream_oldest_change_first_and_a_file_appearing_mid_tick_waits_for_the_next() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("b.jsonl"), "{\"id\":\"b\"}\n").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        std::fs::write(dir.path().join("a.jsonl"), "{\"id\":\"a\"}\n").unwrap();
        let spec = DumpSpec {
            paths: vec![format!("{}/*.jsonl", dir.path().display())],
            ..DumpSpec::default()
        };
        let dump = FileDump::new("u", spec, no_lease());
        let mut stream = dump.rows(None);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(&first.payload[..], b"{\"id\":\"b\"}", "oldest change first");
        // Appears while the tick is streaming: not in this tick's listing.
        // The sleep puts its change time strictly after a.jsonl's, so the next
        // tick's checkpoint cannot filter it out.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        std::fs::write(dir.path().join("c.jsonl"), "{\"id\":\"c\"}\n").unwrap();
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(&second.payload[..], b"{\"id\":\"a\"}");
        assert!(stream.next().await.is_none(), "c.jsonl was not listed");
        let mut cp = dfe_fetcher_core::checkpoint::Checkpoint::new("k");
        cp.fold(first.mark.unwrap());
        cp.fold(second.mark.unwrap());
        let committed = cp.value().cloned().unwrap();
        let next = collect(&dump, Some(&committed)).await.unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(
            &next[0].payload[..],
            b"{\"id\":\"c\"}",
            "picked up next tick"
        );
    }

    #[tokio::test]
    async fn a_truncated_gzip_is_a_decode_error_naming_the_file_after_the_rows_before_it() {
        let dir = tempfile::TempDir::new().unwrap();
        let whole = gz(b"{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n");
        std::fs::write(dir.path().join("cut.jsonl.gz"), &whole[..whole.len() - 6]).unwrap();
        let spec = DumpSpec {
            paths: vec![format!("{}/*.gz", dir.path().display())],
            ..DumpSpec::default()
        };
        let dump = FileDump::new("u", spec, no_lease());
        let err = collect(&dump, None).await.unwrap_err();
        assert!(
            matches!(err, Error::Item { ref key, ref source } if key.ends_with("cut.jsonl.gz") && matches!(**source, Error::Decode(_))),
            "{err:?}"
        );
        assert!(err.to_string().contains("cut.jsonl.gz"), "{err}");
    }

    /// A line longer than `max_row_bytes` (a `.json` document renamed
    /// `.jsonl`, a log line that never ends) fails the file at the bound,
    /// named as that file's failure, rather than being read whole.
    #[tokio::test]
    async fn a_row_longer_than_max_row_bytes_fails_the_file_at_the_bound() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut long = b"{\"id\":1}\n{\"blob\":\"".to_vec();
        long.extend(vec![b'x'; 1024 * 1024]);
        std::fs::write(dir.path().join("long.jsonl"), long).unwrap();
        let dump = FileDump::new(
            "u",
            DumpSpec {
                paths: vec![format!("{}/*.jsonl", dir.path().display())],
                max_row_bytes: 64,
                ..DumpSpec::default()
            },
            no_lease(),
        );
        let err = collect(&dump, None).await.unwrap_err();
        assert!(
            matches!(err, Error::Item { ref key, ref source } if key.ends_with("long.jsonl") && matches!(**source, Error::OversizePage { max: 64 })),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_malformed_array_and_an_unknown_extension_are_errors_and_a_wrong_checkpoint_kind_is_refused()
     {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("bad.json"), "{\"id\":1}").unwrap();
        let dump = FileDump::new(
            "u",
            DumpSpec {
                paths: vec![format!("{}/*.json", dir.path().display())],
                ..DumpSpec::default()
            },
            no_lease(),
        );
        let err = collect(&dump, None).await.unwrap_err();
        assert!(err.to_string().contains("bad.json"), "{err}");

        std::fs::write(dir.path().join("x.dat"), "1\n").unwrap();
        let dump = FileDump::new(
            "u",
            DumpSpec {
                paths: vec![format!("{}/*.dat", dir.path().display())],
                ..DumpSpec::default()
            },
            no_lease(),
        );
        assert!(matches!(
            collect(&dump, None).await.unwrap_err(),
            Error::Item { ref source, .. } if matches!(**source, Error::Config(_))
        ));
        let wrong = CheckpointValue::Keyset(vec![serde_json::json!(1)]);
        assert!(matches!(
            collect(&dump, Some(&wrong)).await.unwrap_err(),
            Error::Cursor(_)
        ));
    }

    #[tokio::test]
    async fn chunks_are_leased_while_held_and_released_by_the_end() {
        use std::sync::atomic::{AtomicI64, Ordering};
        struct Counting {
            current: AtomicI64,
            peak: AtomicI64,
        }
        impl Lease for Counting {
            fn add(&self, bytes: u64) {
                let now = self
                    .current
                    .fetch_add(bytes.cast_signed(), Ordering::SeqCst)
                    + bytes.cast_signed();
                self.peak.fetch_max(now, Ordering::SeqCst);
            }
            fn release(&self, bytes: u64) {
                self.current
                    .fetch_sub(bytes.cast_signed(), Ordering::SeqCst);
            }
        }
        let dir = tempfile::TempDir::new().unwrap();
        let mut body = String::new();
        for i in 0..200 {
            body.push_str(&format!("{{\"id\":{i}}}\n"));
        }
        std::fs::write(dir.path().join("big.jsonl"), &body).unwrap();
        let counting = Arc::new(Counting {
            current: AtomicI64::new(0),
            peak: AtomicI64::new(0),
        });
        let lease: Arc<dyn Lease> = counting.clone();
        let dump = FileDump::new(
            "u",
            DumpSpec {
                paths: vec![format!("{}/*.jsonl", dir.path().display())],
                chunk_bytes: 100,
                ..DumpSpec::default()
            },
            lease,
        );
        let rows = collect(&dump, None).await.unwrap();
        assert_eq!(rows.len(), 200);
        assert_eq!(counting.current.load(Ordering::SeqCst), 0, "all released");
        let peak = counting.peak.load(Ordering::SeqCst);
        assert!(peak > 0 && peak <= 200, "one chunk at a time, peak {peak}");
    }

    #[tokio::test]
    async fn probe_needs_the_glob_base_directory() {
        let dir = tempfile::TempDir::new().unwrap();
        let ok = FileDump::new(
            "u",
            DumpSpec {
                paths: vec![format!("{}/*.jsonl", dir.path().display())],
                ..DumpSpec::default()
            },
            no_lease(),
        );
        ok.probe().await.unwrap();
        let missing = FileDump::new(
            "u",
            DumpSpec {
                paths: vec![format!("{}/absent/*.jsonl", dir.path().display())],
                ..DumpSpec::default()
            },
            no_lease(),
        );
        assert!(missing.probe().await.is_err());
    }
}
