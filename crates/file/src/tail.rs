// Project:   dfe-fetcher
// File:      crates/file/src/tail.rs
// Purpose:   The file tail shape: growing files followed by the vendored tailer, lines committed after ack
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The file tail shape.
//!
//! The tailer's own options are surfaced as-is: fingerprinting decides how a
//! rotated file is recognised, `max_line_bytes` bounds one line, and the
//! checkpoint file lives under `data_dir`. A line is committed there only after
//! the batch carrying it is acknowledged.
//!
//! The tailer (behind the `tail` feature) is Vector's `file-source`, vendored
//! under `third-party/`. It runs as one background task per unit for the life
//! of the shape and hands the tick one read pass at a time over a bounded
//! channel, so an unpolled tick stalls the tailer at its send. A tick yields
//! lines until the tailer reports a pass that read nothing since the tick
//! began, or `max_tick_secs` elapse; each line carries
//! `Mark::Line { file_id, end_offset }`, the driver folds those and commits
//! them after the acks, and the NEXT tick hands the committed positions back
//! to the tailer's checkpoint view -- so `checkpoints.json` only ever holds
//! acknowledged offsets and a restart re-reads at most what was in flight.
//! The tailer's own read position runs ahead of that view, so a tick that
//! fails after lines were handed out would leave the next tick starting past
//! them: the shape remembers what each tick handed out and, when the
//! checkpoint handed back does not cover it, restarts the tailer from the
//! view, which re-reads from the acknowledged offsets.

use serde::{Deserialize, Serialize};

use dfe_fetcher_core::error::{Error, Result};

/// Where a freshly discovered file is read from.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ReadFrom {
    /// The first byte.
    #[default]
    Beginning,
    /// The current end; only new lines are read.
    End,
}

/// How a file is recognised across renames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FingerprintStrategy {
    /// A checksum of the file's first line(s).
    Checksum,
    /// The device and inode.
    DeviceAndInode,
}

/// Fingerprint options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Fingerprint {
    /// The strategy.
    pub strategy: FingerprintStrategy,
    /// Bytes hashed for the checksum strategy.
    pub bytes: usize,
    /// Leading bytes skipped before hashing.
    pub ignored_header_bytes: usize,
    /// Lines hashed for the checksum strategy.
    pub lines: usize,
}

impl Default for Fingerprint {
    fn default() -> Self {
        Self {
            strategy: FingerprintStrategy::Checksum,
            bytes: 256,
            ignored_header_bytes: 0,
            lines: 1,
        }
    }
}

/// How a tailed line becomes a row.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TailDecoder {
    /// Each line is one JSON object, passed through as is.
    #[default]
    Ndjson,
    /// Each line is text, wrapped as `{"line": "..."}`.
    Line,
}

/// A tailed set of files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TailSpec {
    /// Globs of the files to follow.
    pub include: Vec<String>,
    /// Globs excluded from `include`.
    pub exclude: Vec<String>,
    /// Where a new file is read from.
    pub read_from: ReadFrom,
    /// How a line becomes a row.
    pub decoder: TailDecoder,
    /// Longest line accepted; longer lines are dropped with a count.
    pub max_line_bytes: usize,
    /// Minimum interval between glob scans.
    pub glob_minimum_cooldown_ms: u64,
    /// How long a rotated file keeps being read after it disappears from the glob.
    pub rotate_wait_secs: u64,
    /// Longest a tick runs before it ends so its lines can be committed,
    /// even if the files keep growing.
    pub max_tick_secs: u64,
    /// How files are recognised across renames.
    pub fingerprint: Fingerprint,
    /// Directory the checkpoint file lives in; must be on durable storage.
    pub data_dir: String,
}

impl Default for TailSpec {
    fn default() -> Self {
        Self {
            include: Vec::new(),
            exclude: Vec::new(),
            read_from: ReadFrom::Beginning,
            decoder: TailDecoder::Ndjson,
            max_line_bytes: 102_400,
            glob_minimum_cooldown_ms: 1000,
            rotate_wait_secs: 30,
            max_tick_secs: 30,
            fingerprint: Fingerprint::default(),
            data_dir: String::new(),
        }
    }
}

impl TailSpec {
    /// Reject a tail with nothing to follow, nowhere to checkpoint, a glob
    /// that does not parse, or a fingerprint that cannot tell files apart.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] naming the field.
    pub fn validate(&self) -> Result<()> {
        if self.include.iter().all(|p| p.trim().is_empty()) {
            return Err(Error::Config(
                "file tail `include` must name at least one glob".into(),
            ));
        }
        for pattern in self.include.iter().chain(&self.exclude) {
            glob::Pattern::new(pattern)
                .map_err(|e| Error::Config(format!("file tail glob `{pattern}`: {e}")))?;
        }
        if self.data_dir.trim().is_empty() {
            return Err(Error::Config(
                "file tail `data_dir` is required for the checkpoint file".into(),
            ));
        }
        if self.max_line_bytes == 0 {
            return Err(Error::Config(
                "file tail `max_line_bytes` must be > 0".into(),
            ));
        }
        if self.max_tick_secs == 0 {
            return Err(Error::Config(
                "file tail `max_tick_secs` must be > 0".into(),
            ));
        }
        if self.fingerprint.strategy == FingerprintStrategy::Checksum
            && (self.fingerprint.bytes == 0 || self.fingerprint.lines == 0)
        {
            return Err(Error::Config(
                "file tail `fingerprint.bytes` and `fingerprint.lines` must be > 0 for checksum"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[cfg(feature = "tail")]
pub use tailer::FileTail;

#[cfg(feature = "tail")]
mod tailer {
    //! The tailer behind the `tail` feature: Vector's `FileServer` driven as
    //! one background task per unit, its passes handed to the tick.

    use std::collections::{HashMap, VecDeque};
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use bytes::Bytes;
    use futures::channel::mpsc;
    use futures::future::BoxFuture;
    use futures::{FutureExt, Sink, StreamExt};
    use tokio::sync::{Mutex, MutexGuard};
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;
    use tracing::{debug, error, info, warn};

    use file_source::file_server::{FileServer, Line};
    use file_source::paths_provider::{Glob, MatchOptions};
    use file_source_common::checkpointer::{Checkpointer, CheckpointsView};
    use file_source_common::{FileFingerprint, FileSourceInternalEvents, Fingerprinter};

    use dfe_fetcher_core::batch::Lease;
    use dfe_fetcher_core::checkpoint::CheckpointValue;
    use dfe_fetcher_core::error::{Error, Result};
    use dfe_fetcher_core::frame::scan::trim_range;
    use dfe_fetcher_core::frame::wrap_line;
    use dfe_fetcher_core::{Mark, Row, RowStream};

    use super::{FingerprintStrategy, ReadFrom, TailDecoder, TailSpec};
    use crate::FileSource;
    use crate::dump::glob_base;

    /// Bytes the tailer reads from one file per pass (Vector's default).
    ///
    /// SHORTCUT: one pass sits between the tailer and the tick and a pass
    /// carries at most this much per file, so a unit's throughput is bounded
    /// by passes per second x files x 2 KiB. Lift (a larger read per pass and
    /// a deeper channel, both leased the same way) when a tailed file grows
    /// faster than that sustained; nothing in the tick changes.
    const MAX_READ_BYTES: usize = 2048;
    /// Passes the tick may hold ahead of the driver before the tailer parks
    /// at its send.
    const PASS_CAPACITY: usize = 1;

    /// The `u64` a fingerprint travels as in `Mark::Line`: the checksum
    /// itself, or a fixed mix of device and inode. The checksum inverts; a
    /// device/inode id is mapped back only while the process that saw the
    /// file is alive.
    fn id_of(fingerprint: FileFingerprint) -> u64 {
        match fingerprint {
            FileFingerprint::FirstLinesChecksum(sum) => sum,
            FileFingerprint::DevInode(dev, ino) => dev.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ ino,
        }
    }

    /// One read pass as the tailer sent it, its bytes leased until dropped.
    struct Pass {
        at: Instant,
        lines: Vec<Line>,
        bytes: u64,
        lease: Arc<dyn Lease>,
    }

    impl Drop for Pass {
        fn drop(&mut self) {
            self.lease.release(self.bytes);
        }
    }

    /// The tailer's sink: stamps each pass with the time it was sent, leases
    /// its bytes, and forwards it over the bounded channel.
    struct PassSink {
        tx: mpsc::Sender<Pass>,
        lease: Arc<dyn Lease>,
    }

    impl Sink<Vec<Line>> for PassSink {
        type Error = mpsc::SendError;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Pin::new(&mut self.tx).poll_ready(cx)
        }

        fn start_send(
            mut self: Pin<&mut Self>,
            lines: Vec<Line>,
        ) -> std::result::Result<(), Self::Error> {
            let bytes = lines.iter().map(|l| l.text.len() as u64).sum();
            self.lease.add(bytes);
            let pass = Pass {
                at: Instant::now(),
                lines,
                bytes,
                lease: Arc::clone(&self.lease),
            };
            Pin::new(&mut self.tx).start_send(pass)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Pin::new(&mut self.tx).poll_flush(cx)
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Pin::new(&mut self.tx).poll_close(cx)
        }
    }

    /// How many of a repeating tailer event are logged: the first, then every
    /// thousandth. A log file of over-length lines, or a directory the process
    /// cannot read, would otherwise write one warning per line or per pass.
    const LOG_EVERY: u64 = 1000;

    /// The running count when this occurrence should be logged.
    fn sampled(seen: &AtomicU64) -> Option<u64> {
        let n = seen.fetch_add(1, Ordering::Relaxed) + 1;
        (n == 1 || n.is_multiple_of(LOG_EVERY)).then_some(n)
    }

    /// The tailer's event hooks, as log lines tagged with the unit.
    #[derive(Clone)]
    struct Events {
        unit: Arc<str>,
        /// Lines refused for length so far, so the warning is sampled.
        long_lines: Arc<AtomicU64>,
        /// Glob entries that could not be read, likewise.
        unreadable: Arc<AtomicU64>,
    }

    impl FileSourceInternalEvents for Events {
        fn emit_file_added(&self, path: &std::path::Path) {
            info!(unit = %self.unit, path = %path.display(), "file tail: following");
        }
        fn emit_file_resumed(&self, path: &std::path::Path, file_position: u64) {
            info!(unit = %self.unit, path = %path.display(), offset = file_position, "file tail: resumed");
        }
        fn emit_file_watch_error(&self, path: &std::path::Path, error: std::io::Error) {
            warn!(unit = %self.unit, path = %path.display(), %error, "file tail: cannot watch");
        }
        fn emit_file_unwatched(&self, path: &std::path::Path, reached_eof: bool) {
            info!(unit = %self.unit, path = %path.display(), reached_eof, "file tail: stopped following");
        }
        fn emit_file_deleted(&self, path: &std::path::Path) {
            debug!(unit = %self.unit, path = %path.display(), "file tail: deleted");
        }
        fn emit_file_delete_error(&self, path: &std::path::Path, error: std::io::Error) {
            warn!(unit = %self.unit, path = %path.display(), %error, "file tail: cannot delete");
        }
        fn emit_file_fingerprint_read_error(&self, path: &std::path::Path, error: std::io::Error) {
            warn!(unit = %self.unit, path = %path.display(), %error, "file tail: cannot fingerprint");
        }
        fn emit_file_checkpointed(&self, count: usize, duration: Duration) {
            debug!(unit = %self.unit, count, elapsed_ms = duration.as_millis() as u64, "file tail: checkpoints written");
        }
        fn emit_file_checksum_failed(&self, path: &std::path::Path) {
            debug!(unit = %self.unit, path = %path.display(), "file tail: too small to fingerprint yet");
        }
        fn emit_file_checkpoint_write_error(&self, error: std::io::Error) {
            error!(unit = %self.unit, %error, "file tail: checkpoint write failed");
        }
        fn emit_files_open(&self, count: usize) {
            debug!(unit = %self.unit, count, "file tail: files open");
        }
        fn emit_path_globbing_failed(&self, path: &std::path::Path, error: &std::io::Error) {
            if let Some(total) = sampled(&self.unreadable) {
                warn!(unit = %self.unit, path = %path.display(), %error, total, "file tail: glob entry unreadable (1 in 1000 logged)");
            }
        }
        fn emit_file_line_too_long(
            &self,
            _truncated: &bytes::BytesMut,
            configured_limit: usize,
            encountered_size_so_far: usize,
        ) {
            if let Some(total) = sampled(&self.long_lines) {
                warn!(unit = %self.unit, limit = configured_limit, seen = encountered_size_so_far, total, "file tail: line over max_line_bytes dropped (1 in 1000 logged)");
            }
        }
    }

    /// The tailer while it runs.
    struct Running {
        rx: mpsc::Receiver<Pass>,
        view: Arc<CheckpointsView>,
        task: tokio::task::JoinHandle<()>,
        /// Stops this run of the tailer without ending the shape.
        stop: CancellationToken,
        /// Fingerprints seen this process, by the id their marks carry.
        ids: HashMap<u64, FileFingerprint>,
        /// The furthest offset handed out per file since the last tick whose
        /// checkpoint covered everything handed before it.
        handed: HashMap<u64, u64>,
    }

    impl Running {
        /// Whether every offset handed out is covered by `committed`.
        fn covered_by(&self, committed: &[(u64, u64)]) -> bool {
            self.handed.iter().all(|(id, handed)| {
                committed
                    .iter()
                    .any(|(cid, offset)| cid == id && offset >= handed)
            })
        }

        /// Stop this run and wait for its final checkpoint write.
        async fn stop(self) -> std::result::Result<(), tokio::task::JoinError> {
            self.stop.cancel();
            drop(self.rx);
            self.task.await
        }
    }

    /// One tail unit over the vendored tailer.
    pub struct FileTail {
        spec: TailSpec,
        unit: Arc<str>,
        lease: Arc<dyn Lease>,
        running: Mutex<Option<Running>>,
        shutdown: CancellationToken,
    }

    impl std::fmt::Debug for FileTail {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FileTail")
                .field("unit", &self.unit)
                .field("spec", &self.spec)
                .finish_non_exhaustive()
        }
    }

    impl Drop for FileTail {
        fn drop(&mut self) {
            self.shutdown.cancel();
        }
    }

    impl FileTail {
        /// A tail over `spec`, leasing every pass it holds on `lease`. The
        /// tailer starts at the first tick.
        ///
        /// # Errors
        ///
        /// Returns [`Error::Config`] when the spec does not validate.
        pub fn new(unit: &str, spec: TailSpec, lease: Arc<dyn Lease>) -> Result<Self> {
            spec.validate()?;
            Ok(Self {
                spec,
                unit: Arc::from(unit),
                lease,
                running: Mutex::new(None),
                shutdown: CancellationToken::new(),
            })
        }

        /// Stop the tailer and wait for its final checkpoint write. Terminal:
        /// a later tick reports the tailer as stopped.
        pub async fn stop(&self) {
            self.shutdown.cancel();
            if let Some(running) = self.running.lock().await.take()
                && let Err(e) = running.stop().await
            {
                warn!(unit = %self.unit, error = %e, "file tail: tailer task ended badly");
            }
        }

        fn fingerprint_of(
            &self,
            ids: &HashMap<u64, FileFingerprint>,
            id: u64,
        ) -> Option<FileFingerprint> {
            ids.get(&id)
                .copied()
                .or(match self.spec.fingerprint.strategy {
                    FingerprintStrategy::Checksum => Some(FileFingerprint::FirstLinesChecksum(id)),
                    FingerprintStrategy::DeviceAndInode => None,
                })
        }

        /// Hand the driver's committed positions to the tailer's view; only
        /// ever forward, and only for fingerprints that can be named.
        fn apply(
            &self,
            view: &CheckpointsView,
            ids: &HashMap<u64, FileFingerprint>,
            committed: &[(u64, u64)],
        ) {
            for &(id, offset) in committed {
                let Some(fingerprint) = self.fingerprint_of(ids, id) else {
                    debug!(unit = %self.unit, id, offset, "file tail: committed offset for a file this process has not seen");
                    continue;
                };
                if view.get(fingerprint).is_none_or(|have| have < offset) {
                    view.update(fingerprint, offset);
                }
            }
        }

        /// Start the tailer: load its checkpoint file, fold the committed
        /// positions in, persist, then run.
        async fn start(&self, committed: &[(u64, u64)]) -> Result<Running> {
            if self.shutdown.is_cancelled() {
                return Err(Error::Source(format!(
                    "file tail `{}`: the tailer was stopped",
                    self.unit
                )));
            }
            let data_dir = PathBuf::from(&self.spec.data_dir);
            tokio::fs::create_dir_all(&data_dir).await.map_err(|e| {
                Error::Cursor(format!(
                    "file tail: cannot create data_dir {}: {e}",
                    data_dir.display()
                ))
            })?;
            let mut checkpointer = Checkpointer::new(&data_dir);
            checkpointer.read_checkpoints(None).await;
            let view = checkpointer.view();
            let ids = HashMap::new();
            self.apply(&view, &ids, committed);
            checkpointer.write_checkpoints().await.map_err(|e| {
                Error::Cursor(format!(
                    "file tail: cannot write checkpoints under {}: {e}",
                    data_dir.display()
                ))
            })?;

            let emitter = Events {
                unit: Arc::clone(&self.unit),
                long_lines: Arc::new(AtomicU64::new(0)),
                unreadable: Arc::new(AtomicU64::new(0)),
            };
            let include: Vec<PathBuf> = self.spec.include.iter().map(PathBuf::from).collect();
            let exclude: Vec<PathBuf> = self.spec.exclude.iter().map(PathBuf::from).collect();
            let paths = Glob::new(&include, &exclude, MatchOptions::default(), emitter.clone())
                .ok_or_else(|| {
                    Error::Config("file tail: include/exclude globs do not parse".into())
                })?;
            let strategy = match self.spec.fingerprint.strategy {
                FingerprintStrategy::Checksum => {
                    file_source_common::FingerprintStrategy::FirstLinesChecksum {
                        ignored_header_bytes: self.spec.fingerprint.ignored_header_bytes,
                        lines: self.spec.fingerprint.lines,
                    }
                }
                FingerprintStrategy::DeviceAndInode => {
                    file_source_common::FingerprintStrategy::DevInode
                }
            };
            let server = FileServer {
                paths_provider: paths,
                max_read_bytes: MAX_READ_BYTES,
                ignore_checkpoints: false,
                read_from: match self.spec.read_from {
                    ReadFrom::Beginning => file_source_common::ReadFrom::Beginning,
                    ReadFrom::End => file_source_common::ReadFrom::End,
                },
                ignore_before: None,
                max_line_bytes: self.spec.max_line_bytes,
                line_delimiter: Bytes::from_static(b"\n"),
                data_dir,
                glob_minimum_cooldown: Duration::from_millis(self.spec.glob_minimum_cooldown_ms),
                fingerprinter: Fingerprinter::new(strategy, self.spec.fingerprint.bytes, true),
                oldest_first: false,
                remove_after: None,
                emitter,
                rotate_wait: Duration::from_secs(self.spec.rotate_wait_secs),
            };
            let (tx, rx) = mpsc::channel::<Pass>(PASS_CAPACITY);
            let sink = PassSink {
                tx,
                lease: Arc::clone(&self.lease),
            };
            let stop = self.shutdown.child_token();
            let stop_reading = stop.clone().cancelled_owned().boxed();
            let stop_writer = stop.clone().cancelled_owned().boxed();
            let unit = Arc::clone(&self.unit);
            let task = tokio::spawn(async move {
                match server
                    .run(sink, stop_reading, stop_writer, checkpointer)
                    .await
                {
                    Ok(_) => info!(unit = %unit, "file tail: tailer stopped"),
                    Err(e) => {
                        warn!(unit = %unit, error = %e, "file tail: tailer ended; the tick side went away");
                    }
                }
            });
            info!(unit = %self.unit, include = ?self.spec.include, "file tail: tailer started");
            Ok(Running {
                rx,
                view,
                task,
                stop,
                ids,
                handed: HashMap::new(),
            })
        }

        /// The tailer for this tick: the running one when the checkpoint
        /// handed back covers everything it handed out, else a fresh one
        /// started from the committed offsets, so lines a failed tick handed
        /// out are read again.
        async fn resume(
            &self,
            guard: &mut MutexGuard<'_, Option<Running>>,
            committed: &[(u64, u64)],
        ) -> Result<()> {
            if let Some(running) = guard.as_mut() {
                if running.covered_by(committed) {
                    let ids = std::mem::take(&mut running.ids);
                    self.apply(&running.view, &ids, committed);
                    running.ids = ids;
                    running.handed.clear();
                    return Ok(());
                }
                warn!(
                    unit = %self.unit,
                    files = running.handed.len(),
                    "file tail: the last tick handed out lines its checkpoint does not cover; restarting the tailer from the committed offsets"
                );
                if let Some(running) = guard.take()
                    && let Err(e) = running.stop().await
                {
                    warn!(unit = %self.unit, error = %e, "file tail: tailer task ended badly");
                }
            }
            **guard = Some(self.start(committed).await?);
            Ok(())
        }

        /// A line as a row: trimmed NDJSON, or wrapped text; nothing for a
        /// blank line.
        fn decode(&self, text: &Bytes) -> Option<Bytes> {
            let (start, end) = trim_range(text);
            if start == end {
                return None;
            }
            Some(match self.spec.decoder {
                TailDecoder::Ndjson => text.slice(start..end),
                TailDecoder::Line => wrap_line(&text[start..end]),
            })
        }
    }

    /// The committed positions, if the checkpoint is a file tail's.
    fn committed_lines(checkpoint: Option<&CheckpointValue>) -> Result<Vec<(u64, u64)>> {
        match checkpoint {
            None => Ok(Vec::new()),
            Some(CheckpointValue::Lines(files)) => Ok(files.clone()),
            Some(other) => Err(Error::Cursor(format!(
                "file tail has a {} checkpoint where line offsets were expected",
                match other {
                    CheckpointValue::Keyset(_) => "keyset",
                    CheckpointValue::Item { .. } => "file marker",
                    CheckpointValue::Lines(_) => "line",
                }
            ))),
        }
    }

    /// One tick's state: the running tailer (locked for the tick), the pass
    /// being drained, and the clock that bounds the tick.
    struct Tick<'a> {
        tail: &'a FileTail,
        guard: MutexGuard<'a, Option<Running>>,
        started: Instant,
        deadline: Instant,
        pending: VecDeque<Row>,
        current: Option<Pass>,
        done: bool,
    }

    impl Tick<'_> {
        async fn next(&mut self) -> Option<Result<Row>> {
            loop {
                if let Some(row) = self.pending.pop_front() {
                    if self.pending.is_empty() {
                        self.current = None;
                    }
                    return Some(Ok(row));
                }
                if self.done {
                    return None;
                }
                let Some(running) = self.guard.as_mut() else {
                    self.done = true;
                    return Some(Err(Error::Source(
                        "file tail: the tailer is not running".into(),
                    )));
                };
                let next = tokio::select! {
                    biased;
                    pass = running.rx.next() => pass,
                    () = tokio::time::sleep_until(self.deadline) => {
                        debug!(unit = %self.tail.unit, "file tail: tick ended at max_tick_secs");
                        self.done = true;
                        return None;
                    }
                };
                let Some(mut pass) = next else {
                    self.done = true;
                    *self.guard = None;
                    return Some(Err(Error::Source(format!(
                        "file tail `{}`: the tailer stopped; it restarts at the next tick",
                        self.tail.unit
                    ))));
                };
                if pass.lines.is_empty() {
                    if pass.at >= self.started {
                        self.done = true;
                        return None;
                    }
                    continue;
                }
                for line in pass.lines.drain(..) {
                    let id = id_of(line.file_id);
                    running.ids.entry(id).or_insert(line.file_id);
                    if let Some(payload) = self.tail.decode(&line.text) {
                        running
                            .handed
                            .entry(id)
                            .and_modify(|offset| *offset = (*offset).max(line.end_offset))
                            .or_insert(line.end_offset);
                        self.pending.push_back(Row {
                            payload,
                            mark: Some(Mark::Line {
                                file_id: id,
                                end_offset: line.end_offset,
                            }),
                        });
                    }
                }
                if !self.pending.is_empty() {
                    self.current = Some(pass);
                }
            }
        }
    }

    impl FileSource for FileTail {
        fn rows<'a>(&'a self, checkpoint: Option<&'a CheckpointValue>) -> RowStream<'a> {
            let committed = match committed_lines(checkpoint) {
                Ok(c) => c,
                Err(e) => return futures::stream::once(async move { Err(e) }).boxed(),
            };
            let open = async move {
                let mut guard = self.running.lock().await;
                self.resume(&mut guard, &committed).await?;
                let started = Instant::now();
                Ok::<_, Error>(Tick {
                    tail: self,
                    guard,
                    started,
                    deadline: started + Duration::from_secs(self.spec.max_tick_secs),
                    pending: VecDeque::new(),
                    current: None,
                    done: false,
                })
            };
            futures::stream::once(open)
                .map(|tick| match tick {
                    Ok(tick) => futures::stream::unfold(tick, |mut t| async move {
                        t.next().await.map(|row| (row, t))
                    })
                    .boxed(),
                    Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
                })
                .flatten()
                .boxed()
        }

        fn probe(&self) -> BoxFuture<'_, Result<()>> {
            async move {
                let data_dir = PathBuf::from(&self.spec.data_dir);
                tokio::fs::create_dir_all(&data_dir).await.map_err(|e| {
                    Error::Cursor(format!(
                        "file tail: cannot create data_dir {}: {e}",
                        data_dir.display()
                    ))
                })?;
                for pattern in &self.spec.include {
                    let base = glob_base(pattern);
                    if !base.is_dir() {
                        return Err(Error::Source(format!(
                            "file tail: base directory of `{pattern}` does not exist"
                        )));
                    }
                }
                Ok(())
            }
            .boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tail_needs_globs_a_data_dir_and_a_usable_fingerprint() {
        assert!(TailSpec::default().validate().is_err());
        let spec: TailSpec = serde_json::from_str(
            r#"{"include": ["/var/log/app/*.log"], "data_dir": "/var/lib/dfe/tail"}"#,
        )
        .unwrap();
        spec.validate().unwrap();
        assert_eq!(spec.fingerprint.bytes, 256);
        assert_eq!(spec.decoder, TailDecoder::Ndjson);
        assert_eq!(spec.max_tick_secs, 30);
        let mut zero = spec.clone();
        zero.fingerprint.bytes = 0;
        assert!(zero.validate().is_err());
        let mut no_dir = spec.clone();
        no_dir.data_dir.clear();
        assert!(no_dir.validate().is_err());
        let mut bad_glob = spec.clone();
        bad_glob.exclude.push("[".into());
        assert!(
            bad_glob
                .validate()
                .unwrap_err()
                .to_string()
                .contains("glob `[`")
        );
        let mut no_tick = spec;
        no_tick.max_tick_secs = 0;
        assert!(no_tick.validate().is_err());
    }
}

#[cfg(all(test, feature = "tail"))]
mod tailer_tests {
    use super::*;
    use dfe_fetcher_core::batch::{Lease, NoLease};
    use dfe_fetcher_core::checkpoint::{Checkpoint, CheckpointValue};
    use dfe_fetcher_core::{Mark, Row};
    use futures::StreamExt;
    use std::io::Write as _;
    use std::sync::Arc;

    use crate::FileSource;

    fn no_lease() -> Arc<dyn Lease> {
        Arc::new(NoLease)
    }

    fn spec(dir: &tempfile::TempDir) -> TailSpec {
        TailSpec {
            include: vec![format!("{}/logs/*.log", dir.path().display())],
            data_dir: dir.path().join("state").display().to_string(),
            glob_minimum_cooldown_ms: 50,
            rotate_wait_secs: 1,
            max_tick_secs: 5,
            ..TailSpec::default()
        }
    }

    fn append(path: &std::path::Path, lines: &[&str]) {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
        f.flush().unwrap();
    }

    /// One tick: its rows, and the checkpoint the driver would commit.
    async fn tick(
        tail: &FileTail,
        cp: Option<&CheckpointValue>,
    ) -> (Vec<Row>, Option<CheckpointValue>) {
        let rows: Vec<Row> = tail.rows(cp).map(|r| r.expect("tick row")).collect().await;
        let mut folded = Checkpoint::new("k");
        for row in &rows {
            folded.fold(row.mark.clone().expect("a tail row carries its mark"));
        }
        (rows, folded.value().cloned())
    }

    /// Tick until the read from `cp` yields `want` rows.
    ///
    /// The tailer does not re-glob inside `glob_minimum_cooldown_ms`, so the
    /// tick straight after an append can legitimately see nothing. Each tick
    /// is a complete read from `cp` rather than a continuation, so the tick
    /// that sees the appended lines sees all of them and nothing is counted
    /// twice.
    async fn tick_until(
        tail: &FileTail,
        cp: Option<&CheckpointValue>,
        want: usize,
    ) -> (Vec<Row>, Option<CheckpointValue>) {
        for _ in 0..100 {
            let got = tick(tail, cp).await;
            if got.0.len() >= want {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("the tailer never yielded {want} row(s) from this checkpoint");
    }

    fn texts(rows: &[Row]) -> Vec<String> {
        rows.iter()
            .map(|r| String::from_utf8(r.payload.to_vec()).unwrap())
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn appended_lines_arrive_in_order_with_their_offsets_and_a_tick_ends_when_caught_up() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("logs")).unwrap();
        let log = dir.path().join("logs").join("app.log");
        append(&log, &["{\"n\":1}", "{\"n\":2}"]);
        let tail = FileTail::new("logs", spec(&dir), no_lease()).unwrap();

        let (rows, cp) = tick(&tail, None).await;
        assert_eq!(texts(&rows), ["{\"n\":1}", "{\"n\":2}"]);
        let Some(Mark::Line {
            file_id,
            end_offset,
        }) = rows[1].mark.clone()
        else {
            panic!("line mark");
        };
        assert_eq!(end_offset, 16, "byte offset just past the second line");
        assert_eq!(cp, Some(CheckpointValue::Lines(vec![(file_id, 16)])));

        append(&log, &["{\"n\":3}"]);
        let (rows, cp2) = tick(&tail, cp.as_ref()).await;
        assert_eq!(texts(&rows), ["{\"n\":3}"], "only the new line");
        assert_eq!(cp2, Some(CheckpointValue::Lines(vec![(file_id, 24)])));

        let (rows, cp3) = tick(&tail, cp2.as_ref()).await;
        assert!(rows.is_empty(), "nothing new");
        assert!(cp3.is_none());
        tail.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rotated_file_is_read_to_its_end_and_the_new_file_from_its_start() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("logs")).unwrap();
        let log = dir.path().join("logs").join("app.log");
        append(&log, &["{\"n\":1}"]);
        let tail = FileTail::new("logs", spec(&dir), no_lease()).unwrap();
        let (rows, cp) = tick(&tail, None).await;
        assert_eq!(texts(&rows), ["{\"n\":1}"]);

        // Rotate: rename the live file away (still matched by nothing), append
        // to the renamed one (the tailer keeps its handle), start a new file.
        append(&log, &["{\"n\":2}"]);
        std::fs::rename(&log, dir.path().join("logs").join("app.log.1")).unwrap();
        append(&dir.path().join("logs").join("app.log.1"), &["{\"n\":3}"]);
        append(&log, &["{\"m\":1}"]);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let (rows, cp2) = tick(&tail, cp.as_ref()).await;
        let mut got = texts(&rows);
        got.sort();
        assert_eq!(got, ["{\"m\":1}", "{\"n\":2}", "{\"n\":3}"]);
        let Some(CheckpointValue::Lines(files)) = cp2 else {
            panic!("line checkpoint");
        };
        assert_eq!(files.len(), 2, "two fingerprints: {files:?}");
        tail.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_truncated_and_rewritten_file_is_read_from_its_new_start() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("logs")).unwrap();
        let log = dir.path().join("logs").join("app.log");
        append(&log, &["{\"gen\":1,\"n\":1}", "{\"gen\":1,\"n\":2}"]);
        let tail = FileTail::new("logs", spec(&dir), no_lease()).unwrap();
        let (rows, cp) = tick(&tail, None).await;
        assert_eq!(rows.len(), 2);

        std::fs::write(&log, "{\"gen\":2,\"n\":1}\n").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let (rows, cp2) = tick(&tail, cp.as_ref()).await;
        assert_eq!(
            texts(&rows),
            ["{\"gen\":2,\"n\":1}"],
            "a new first line is a new file"
        );
        let Some(CheckpointValue::Lines(files)) = cp2 else {
            panic!("line checkpoint");
        };
        assert_eq!(files[0].1, 16, "offset just past the rewritten first line");
        tail.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restart_resumes_from_the_committed_offset_not_the_read_one() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("logs")).unwrap();
        let log = dir.path().join("logs").join("app.log");
        append(&log, &["{\"n\":1}", "{\"n\":2}"]);
        let tail = FileTail::new("logs", spec(&dir), no_lease()).unwrap();
        let (rows, committed) = tick(&tail, None).await;
        assert_eq!(rows.len(), 2);
        // A second tick reads a line the driver never acknowledges (the tick
        // is lost: no checkpoint handed back).
        append(&log, &["{\"n\":3}"]);
        let (rows, _lost) = tick(&tail, committed.as_ref()).await;
        assert_eq!(texts(&rows), ["{\"n\":3}"]);
        tail.stop().await;

        // Restart: the tailer's own file holds only acknowledged offsets, and
        // the driver hands the last committed checkpoint to the first tick.
        let again = FileTail::new("logs", spec(&dir), no_lease()).unwrap();
        append(&log, &["{\"n\":4}"]);
        let (rows, cp) = tick(&again, committed.as_ref()).await;
        assert_eq!(
            texts(&rows),
            ["{\"n\":3}", "{\"n\":4}"],
            "the unacknowledged line is read again, the acknowledged ones are not"
        );
        let (rows, _) = tick(&again, cp.as_ref()).await;
        assert!(rows.is_empty());
        again.stop().await;

        // The checkpoint file is the tailer's own format.
        let state =
            std::fs::read_to_string(dir.path().join("state").join("checkpoints.json")).unwrap();
        assert!(state.contains("\"version\":\"1\""), "{state}");
    }

    /// A tick whose lines the driver never commits (a failed flush) is
    /// followed, on the SAME tailer, by a tick handed the older checkpoint:
    /// the lines of the failed tick come again, with whatever was appended
    /// since, and nothing is skipped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_tick_is_re_read_by_the_next_tick_on_the_same_tailer() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("logs")).unwrap();
        let log = dir.path().join("logs").join("app.log");
        append(&log, &["{\"n\":1}", "{\"n\":2}"]);
        let tail = FileTail::new("logs", spec(&dir), no_lease()).unwrap();
        let (rows, committed) = tick(&tail, None).await;
        assert_eq!(rows.len(), 2);

        append(&log, &["{\"n\":3}"]);
        let (rows, _lost) = tick_until(&tail, committed.as_ref(), 1).await;
        assert_eq!(texts(&rows), ["{\"n\":3}"], "handed out, never committed");

        append(&log, &["{\"n\":4}"]);
        let (rows, cp) = tick_until(&tail, committed.as_ref(), 2).await;
        assert_eq!(
            texts(&rows),
            ["{\"n\":3}", "{\"n\":4}"],
            "the failed tick's line comes again, then the new one"
        );
        let (rows, _) = tick(&tail, cp.as_ref()).await;
        assert!(rows.is_empty(), "once committed, nothing is re-read");
        tail.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_line_decoder_wraps_text_and_blank_lines_are_skipped() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("logs")).unwrap();
        let log = dir.path().join("logs").join("app.log");
        append(&log, &["hello \"there\"", "", "world"]);
        let mut s = spec(&dir);
        s.decoder = TailDecoder::Line;
        let tail = FileTail::new("logs", s, no_lease()).unwrap();
        let (rows, _) = tick(&tail, None).await;
        assert_eq!(
            texts(&rows),
            [r#"{"line":"hello \"there\""}"#, r#"{"line":"world"}"#]
        );
        tail.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pass_is_leased_while_held_and_a_wrong_checkpoint_kind_is_refused() {
        use std::sync::atomic::{AtomicI64, Ordering};
        struct Counting(AtomicI64, AtomicI64);
        impl Lease for Counting {
            fn add(&self, bytes: u64) {
                let now =
                    self.0.fetch_add(bytes.cast_signed(), Ordering::SeqCst) + bytes.cast_signed();
                self.1.fetch_max(now, Ordering::SeqCst);
            }
            fn release(&self, bytes: u64) {
                self.0.fetch_sub(bytes.cast_signed(), Ordering::SeqCst);
            }
        }
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("logs")).unwrap();
        append(
            &dir.path().join("logs").join("app.log"),
            &["{\"n\":1}", "{\"n\":2}"],
        );
        let counting = Arc::new(Counting(AtomicI64::new(0), AtomicI64::new(0)));
        let lease: Arc<dyn Lease> = counting.clone();
        let tail = FileTail::new("logs", spec(&dir), lease).unwrap();
        let (rows, _) = tick(&tail, None).await;
        assert_eq!(rows.len(), 2);
        assert!(
            counting.1.load(Ordering::SeqCst) >= 14,
            "the pass was leased"
        );
        assert_eq!(
            counting.0.load(Ordering::SeqCst),
            0,
            "released once its lines are out"
        );
        let wrong = CheckpointValue::Keyset(vec![serde_json::json!(1)]);
        let err = tail.rows(Some(&wrong)).next().await.unwrap().unwrap_err();
        assert!(matches!(err, Error::Cursor(_)), "{err}");
        tail.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn probe_creates_the_data_dir_and_needs_the_include_base() {
        let dir = tempfile::TempDir::new().unwrap();
        let tail = FileTail::new("logs", spec(&dir), no_lease()).unwrap();
        assert!(tail.probe().await.is_err(), "logs/ does not exist yet");
        std::fs::create_dir_all(dir.path().join("logs")).unwrap();
        tail.probe().await.unwrap();
        assert!(dir.path().join("state").is_dir());
        tail.stop().await;
    }
}
