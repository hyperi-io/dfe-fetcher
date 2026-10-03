// Project:   dfe-fetcher
// File:      crates/core/src/batch.rs
// Purpose:   The accumulate batcher: fits-before-push, bounded rows/bytes/window
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The accumulate batcher.
//!
//! A plain struct the driver loop drives, not an actor. `fits` is checked
//! BEFORE `push`, so a batch never exceeds `max_bytes`: the row that does not
//! fit closes the batch and opens the next (the one exception is an empty
//! batch, which accepts any single row so an oversize row can still travel
//! alone). The window is a deadline armed at the FIRST row of a batch, so a
//! partial batch waits at most one window. Every buffered byte is leased on the
//! memory guard through [`Lease`] and released when the flushed [`Batch`] is
//! dropped, which is after the emit has a terminal outcome for every row.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::Mark;
use crate::error::{Error, Result};

/// Byte accounting the batcher reports buffered bytes to.
///
/// The app implements it over the memory guard; core never sees the guard.
pub trait Lease: Send + Sync {
    /// Bytes now held in a buffer.
    fn add(&self, bytes: u64);
    /// Bytes no longer held.
    fn release(&self, bytes: u64);
}

/// A lease that counts nothing, for tests and for an emit path with no guard.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoLease;

impl Lease for NoLease {
    fn add(&self, _bytes: u64) {}
    fn release(&self, _bytes: u64) {}
}

/// Batch bounds, from the config cascade under `accumulate` and overridable per
/// source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct AccumulateConfig {
    /// Rows per batch before a flush.
    pub max_rows: usize,
    /// Payload bytes per batch before a flush.
    pub max_bytes: usize,
    /// How long a partial batch waits after its first row.
    pub window_ms: u64,
    /// Concurrent sends per flush on a transport without a native batch call.
    ///
    /// Defaults to the whole batch: scalo's Kafka transport awaits each
    /// produce's delivery report, so a bound below `max_rows` serialises the
    /// flush against the broker's round trip. Memory does not grow with it --
    /// the batch is already in memory and already leased.
    pub in_flight: usize,
}

impl Default for AccumulateConfig {
    fn default() -> Self {
        let max_rows = 1000;
        Self {
            max_rows,
            max_bytes: 8 * 1024 * 1024,
            window_ms: 1000,
            in_flight: max_rows,
        }
    }
}

impl AccumulateConfig {
    /// The batch window as a duration.
    #[must_use]
    pub fn window(&self) -> Duration {
        Duration::from_millis(self.window_ms)
    }

    /// Reject bounds that would flush every row or send nothing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] naming the field that is zero.
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("max_rows", self.max_rows),
            ("max_bytes", self.max_bytes),
            ("in_flight", self.in_flight),
        ] {
            if value == 0 {
                return Err(Error::Config(format!("accumulate.{name} must be > 0")));
            }
        }
        if self.window_ms == 0 {
            return Err(Error::Config("accumulate.window_ms must be > 0".into()));
        }
        Ok(())
    }
}

/// One enriched, routed row ready for the transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outbound {
    /// Topic the row lands on, suffix included.
    pub topic: Arc<str>,
    /// The row's bytes.
    pub payload: Bytes,
    /// Named destinations a route chose, or `None` for the default transports.
    pub route: Option<Arc<[Arc<str>]>>,
}

impl Outbound {
    /// A row for the default transports.
    #[must_use]
    pub fn new(topic: &str, payload: impl Into<Bytes>) -> Self {
        Self {
            topic: Arc::from(topic),
            payload: payload.into(),
            route: None,
        }
    }

    /// Send this row to named destinations instead.
    #[must_use]
    pub fn with_route(mut self, route: Arc<[Arc<str>]>) -> Self {
        self.route = Some(route);
        self
    }
}

/// Why a batch was flushed; the `trigger` label of the flush counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// `max_rows` reached.
    Rows,
    /// The next row would exceed `max_bytes`.
    Bytes,
    /// The window deadline passed.
    Window,
    /// The unit's stream ended.
    End,
    /// The admission gate asked the driver to hold; the buffer drains first.
    Hold,
}

impl Trigger {
    /// Every trigger, in label order.
    pub const ALL: [Trigger; 5] = [
        Trigger::Rows,
        Trigger::Bytes,
        Trigger::Window,
        Trigger::End,
        Trigger::Hold,
    ];

    /// The label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Trigger::Rows => "rows",
            Trigger::Bytes => "bytes",
            Trigger::Window => "window",
            Trigger::End => "end",
            Trigger::Hold => "hold",
        }
    }
}

/// Releases a lease when dropped, so an aborted emit cannot leave the guard high.
struct Leased {
    lease: Arc<dyn Lease>,
    bytes: u64,
}

impl Drop for Leased {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.lease.release(self.bytes);
        }
    }
}

/// A flushed batch: the rows, their marks, and the lease they hold until dropped.
pub struct Batch {
    /// Rows in push order.
    pub rows: Vec<Outbound>,
    /// Marks of the rows that had one and of the rows consumed without being
    /// buffered, in arrival order.
    pub marks: Vec<Mark>,
    /// Payload bytes in the batch.
    pub bytes: usize,
    _leased: Leased,
}

impl std::fmt::Debug for Batch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Batch")
            .field("rows", &self.rows.len())
            .field("marks", &self.marks.len())
            .field("bytes", &self.bytes)
            .field("leased", &self._leased.bytes)
            .finish()
    }
}

/// The accumulate buffer of one unit's tick.
pub struct Batcher {
    rows: Vec<Outbound>,
    marks: Vec<Mark>,
    bytes: usize,
    opened: Option<Instant>,
    cfg: AccumulateConfig,
    lease: Arc<dyn Lease>,
}

impl Batcher {
    /// An empty batcher leasing on `lease`.
    #[must_use]
    pub fn new(cfg: AccumulateConfig, lease: Arc<dyn Lease>) -> Self {
        Self {
            rows: Vec::with_capacity(cfg.max_rows.min(1024)),
            marks: Vec::new(),
            bytes: 0,
            opened: None,
            cfg,
            lease,
        }
    }

    /// The bounds in force.
    #[must_use]
    pub fn config(&self) -> &AccumulateConfig {
        &self.cfg
    }

    /// Whether no row is buffered; marks of consumed rows may still be held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Whether neither a row nor a mark is held.
    #[must_use]
    pub fn is_settled(&self) -> bool {
        self.rows.is_empty() && self.marks.is_empty()
    }

    /// Rows buffered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Payload bytes buffered.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Whether a row of `len` bytes can join the current batch without
    /// exceeding either bound. An empty batch accepts any row.
    #[must_use]
    pub fn fits(&self, len: usize) -> bool {
        self.rows.is_empty()
            || (self.rows.len() < self.cfg.max_rows && self.bytes + len <= self.cfg.max_bytes)
    }

    /// Buffer a row; leases its bytes and arms the window at the first row.
    pub fn push(&mut self, row: Outbound, mark: Option<Mark>) {
        let len = row.payload.len();
        self.lease.add(len as u64);
        self.bytes += len;
        if self.opened.is_none() {
            self.opened = Some(Instant::now());
        }
        self.rows.push(row);
        if let Some(mark) = mark {
            self.marks.push(mark);
        }
    }

    /// Record the mark of a row that was consumed but not buffered (filtered
    /// out, replaced by nothing): it travels with the batch in arrival order,
    /// so the checkpoint passes it after the flush that follows, as it would
    /// a buffered row's.
    pub fn consume(&mut self, mark: Option<Mark>) {
        if let Some(mark) = mark {
            self.marks.push(mark);
        }
    }

    /// Whether the batch has reached a bound and must flush.
    #[must_use]
    pub fn full(&self) -> bool {
        self.rows.len() >= self.cfg.max_rows || self.bytes >= self.cfg.max_bytes
    }

    /// When the window closes on the current batch, if one is open.
    #[must_use]
    pub fn window_deadline(&self) -> Option<Instant> {
        self.opened.map(|t| t + self.cfg.window())
    }

    /// Take the batch, leaving the batcher empty. The lease moves with it.
    pub fn take(&mut self) -> Batch {
        let bytes = std::mem::take(&mut self.bytes);
        self.opened = None;
        // Replaced rather than `mem::take`d: that leaves capacity 0 behind, so
        // every batch after the first regrows from nothing.
        let rows = std::mem::replace(
            &mut self.rows,
            Vec::with_capacity(self.cfg.max_rows.min(1024)),
        );
        Batch {
            rows,
            marks: std::mem::take(&mut self.marks),
            bytes,
            _leased: Leased {
                lease: Arc::clone(&self.lease),
                bytes: bytes as u64,
            },
        }
    }
}

impl Drop for Batcher {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.lease.release(self.bytes as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    /// Counts leased bytes so a test can prove the guard sees the buffer.
    #[derive(Default)]
    struct Counting(AtomicU64);

    impl Lease for Counting {
        fn add(&self, bytes: u64) {
            self.0.fetch_add(bytes, Ordering::SeqCst);
        }
        fn release(&self, bytes: u64) {
            self.0.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    fn cfg(max_rows: usize, max_bytes: usize, window_ms: u64) -> AccumulateConfig {
        AccumulateConfig {
            max_rows,
            max_bytes,
            window_ms,
            in_flight: 4,
        }
    }

    fn row(len: usize) -> Outbound {
        Outbound::new("topic", vec![b'x'; len])
    }

    #[test]
    fn defaults_are_the_agreed_values() {
        let d = AccumulateConfig::default();
        assert_eq!(d.max_rows, 1000);
        assert_eq!(d.max_bytes, 8 * 1024 * 1024);
        assert_eq!(d.window_ms, 1000);
        assert_eq!(
            d.in_flight, d.max_rows,
            "the whole batch goes in flight; a smaller bound serialises the flush"
        );
        assert_eq!(d.window(), Duration::from_secs(1));
        d.validate().unwrap();
    }

    #[test]
    fn zero_bounds_are_rejected_at_load() {
        assert!(cfg(0, 10, 10).validate().is_err());
        assert!(cfg(10, 0, 10).validate().is_err());
        assert!(
            cfg(10, 10, 0).validate().is_err(),
            "a zero window flushes every row"
        );
        let mut no_flight = cfg(10, 10, 10);
        no_flight.in_flight = 0;
        assert!(no_flight.validate().is_err());
    }

    #[test]
    fn config_deserialises_with_defaults_for_absent_keys() {
        let c: AccumulateConfig = serde_json::from_str(r#"{"max_rows": 5}"#).unwrap();
        assert_eq!(c.max_rows, 5);
        assert_eq!(c.max_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn a_batch_flushes_when_max_rows_is_reached() {
        let mut b = Batcher::new(cfg(3, usize::MAX, 1000), Arc::new(NoLease));
        for _ in 0..2 {
            assert!(b.fits(1));
            b.push(row(1), None);
            assert!(!b.full());
        }
        b.push(row(1), None);
        assert!(b.full(), "third row hits max_rows");
        let batch = b.take();
        assert_eq!(batch.rows.len(), 3);
        assert!(b.is_empty());
        assert!(!b.full());
    }

    #[test]
    fn fits_is_checked_before_push_so_a_batch_never_exceeds_max_bytes() {
        let mut b = Batcher::new(cfg(100, 10, 1000), Arc::new(NoLease));
        b.push(row(4), None);
        b.push(row(4), None);
        assert_eq!(b.bytes(), 8);
        assert!(b.fits(2), "8 + 2 == 10 is allowed");
        assert!(
            !b.fits(3),
            "8 + 3 would exceed max_bytes; the caller flushes first"
        );
        assert!(
            !b.full(),
            "not full yet: fits-before-push decides, not a post-push check"
        );
        b.push(row(2), None);
        assert!(b.full());
        assert_eq!(b.take().bytes, 10);
    }

    #[test]
    fn an_empty_batch_accepts_a_row_larger_than_max_bytes() {
        let mut b = Batcher::new(cfg(100, 10, 1000), Arc::new(NoLease));
        assert!(
            b.fits(50),
            "an oversize row must still be able to travel alone"
        );
        b.push(row(50), None);
        assert!(b.full());
        assert!(!b.fits(1));
    }

    #[test]
    fn pushed_bytes_are_leased_and_released_when_the_batch_is_dropped() {
        let lease = Arc::new(Counting::default());
        let mut b = Batcher::new(cfg(100, 100, 1000), Arc::clone(&lease) as Arc<dyn Lease>);
        b.push(row(30), None);
        b.push(row(20), None);
        assert_eq!(
            lease.0.load(Ordering::SeqCst),
            50,
            "buffered bytes count against the guard"
        );
        let batch = b.take();
        assert_eq!(
            lease.0.load(Ordering::SeqCst),
            50,
            "taking hands the lease to the batch; nothing is released until the emit is done"
        );
        assert_eq!(batch.bytes, 50);
        drop(batch);
        assert_eq!(
            lease.0.load(Ordering::SeqCst),
            0,
            "the batch releases on drop"
        );
    }

    #[test]
    fn an_abandoned_batcher_releases_what_it_still_holds() {
        let lease = Arc::new(Counting::default());
        {
            let mut b = Batcher::new(cfg(100, 100, 1000), Arc::clone(&lease) as Arc<dyn Lease>);
            b.push(row(7), None);
        }
        assert_eq!(lease.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn marks_travel_with_the_batch_they_were_pushed_in() {
        let mut b = Batcher::new(cfg(100, 100, 1000), Arc::new(NoLease));
        b.push(row(1), Some(Mark::Ack("a".into())));
        b.push(row(1), None);
        b.push(row(1), Some(Mark::Ack("b".into())));
        let batch = b.take();
        assert_eq!(
            batch.marks,
            vec![Mark::Ack("a".into()), Mark::Ack("b".into())]
        );
        assert_eq!(b.take().marks, [] as [Mark; 0]);
    }

    /// A consumed row's mark keeps its place among the buffered rows' marks,
    /// leases nothing and arms no window; a batch holding only such marks
    /// is empty of rows but not settled.
    #[test]
    fn a_consumed_rows_mark_travels_in_order_without_a_row() {
        let lease = Arc::new(Counting::default());
        let mut b = Batcher::new(cfg(100, 100, 1000), Arc::clone(&lease) as Arc<dyn Lease>);
        b.consume(Some(Mark::Ack("dropped-first".into())));
        assert!(b.is_empty());
        assert!(!b.is_settled());
        assert!(b.window_deadline().is_none(), "no row, no window");
        assert_eq!(lease.0.load(Ordering::SeqCst), 0);
        b.push(row(3), Some(Mark::Ack("kept".into())));
        b.consume(None);
        b.consume(Some(Mark::Ack("dropped-last".into())));
        let batch = b.take();
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(
            batch.marks,
            vec![
                Mark::Ack("dropped-first".into()),
                Mark::Ack("kept".into()),
                Mark::Ack("dropped-last".into())
            ]
        );
        assert!(b.is_settled());
    }

    #[tokio::test(start_paused = true)]
    async fn the_window_is_armed_at_the_first_row_not_at_construction() {
        let mut b = Batcher::new(cfg(100, 100, 1000), Arc::new(NoLease));
        assert!(
            b.window_deadline().is_none(),
            "an empty batch has no deadline"
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        b.push(row(1), None);
        let deadline = b.window_deadline().expect("armed by the first row");
        assert_eq!(
            deadline,
            tokio::time::Instant::now() + Duration::from_secs(1)
        );
        tokio::time::advance(Duration::from_millis(500)).await;
        b.push(row(1), None);
        assert_eq!(
            b.window_deadline(),
            Some(deadline),
            "later rows do not push the deadline out"
        );
        tokio::time::sleep_until(deadline).await;
        assert!(tokio::time::Instant::now() >= deadline);
        let _ = b.take();
        assert!(b.window_deadline().is_none(), "taking disarms the window");
    }

    #[test]
    fn trigger_names_are_the_metric_label_vocabulary() {
        let names: Vec<&str> = Trigger::ALL.iter().map(|t| t.as_str()).collect();
        assert_eq!(names, ["rows", "bytes", "window", "end", "hold"]);
    }

    #[test]
    fn outbound_carries_topic_payload_and_optional_route() {
        let plain = Outbound::new("t", b"{}".to_vec());
        assert_eq!(&*plain.topic, "t");
        assert!(plain.route.is_none());
        let routed = plain.clone().with_route(Arc::from([Arc::from("siem")]));
        assert_eq!(routed.route.as_deref().map(<[Arc<str>]>::len), Some(1));
    }
}
