// Project:   dfe-fetcher
// File:      crates/core/src/metric_names.rs
// Purpose:   The one list of framework metric names every crate emits against
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Metric names emitted by the framework.
//!
//! The shape crates record through the `metrics` facade and the app describes
//! the series at registration; both sides read the names from here so a rename
//! cannot leave one side emitting a series the other never described. Labels
//! are bounded: `source` is a connection id, `store` is `<connection>.<unit>`,
//! `code` and `trigger` are closed vocabularies.

/// Per-API-call latency, labelled `source`.
pub const API_DURATION_SECONDS: &str = "dfe_fetcher_api_duration_seconds";
/// API errors by `source` and `code` (`throttle|4xx|5xx|timeout|network`).
pub const API_ERRORS_TOTAL: &str = "dfe_fetcher_api_errors_total";
/// Pages requested, labelled `source`.
pub const PAGES_FETCHED_TOTAL: &str = "dfe_fetcher_pages_fetched_total";
/// Page sequences cut at `max_pages` with more to fetch, by `source` and
/// `unit`: an event-window unit fails its tick on it, a dump or a listing is
/// cut short.
pub const PAGES_TRUNCATED_TOTAL: &str = "dfe_fetcher_pages_truncated_total";
/// Tail pages that came back full (`limit` rows), by `source` and `unit`;
/// a tick ends after `max_pages_per_tick` of them.
pub const TAIL_PAGES_FULL_TOTAL: &str = "dfe_fetcher_tail_pages_full_total";
/// Rows the shapes yielded before the filter, labelled `source`.
pub const RECORDS_FETCHED_TOTAL: &str = "dfe_fetcher_records_fetched_total";
/// Response body bytes read, labelled `source`.
pub const BYTES_FETCHED_TOTAL: &str = "dfe_fetcher_bytes_fetched_total";
/// Rows the compiled filter dropped, labelled `source`.
pub const RECORDS_FILTERED_TOTAL: &str = "dfe_fetcher_records_filtered_total";
/// Batch flushes by `source` and `trigger` (`rows|bytes|window|end|hold`).
pub const ACCUMULATE_FLUSHES_TOTAL: &str = "dfe_fetcher_accumulate_flushes_total";
/// Rows per flushed batch, labelled `source`.
pub const ACCUMULATE_BATCH_ROWS: &str = "dfe_fetcher_accumulate_batch_rows";
/// Bytes per flushed batch, labelled `source`.
pub const ACCUMULATE_BATCH_BYTES: &str = "dfe_fetcher_accumulate_batch_bytes";
/// Bytes buffered and leased right now, labelled `source`.
pub const ACCUMULATE_PENDING_BYTES: &str = "dfe_fetcher_accumulate_pending_bytes";
/// Dump rows emitted, labelled `store`.
pub const SNAPSHOT_ROWS_TOTAL: &str = "dfe_fetcher_snapshot_rows_total";
/// Dump rows replaced by an oversize stub, labelled `store`.
pub const SNAPSHOT_ROWS_OVERSIZE_TOTAL: &str = "dfe_fetcher_snapshot_rows_oversize_total";
/// Dumps by `store` and `status` (`complete|aborted`).
pub const SNAPSHOTS_TOTAL: &str = "dfe_fetcher_snapshots_total";
/// Provider quota headers surfaced as gauges: `dfe_fetcher_api_quota_<name>`,
/// labelled `source`; `<name>` comes from the profile's bounded `quota.headers`.
pub const API_QUOTA_PREFIX: &str = "dfe_fetcher_api_quota_";

/// Every fixed name above, for the app to describe and for the test below.
pub const ALL: &[&str] = &[
    API_DURATION_SECONDS,
    API_ERRORS_TOTAL,
    PAGES_FETCHED_TOTAL,
    PAGES_TRUNCATED_TOTAL,
    TAIL_PAGES_FULL_TOTAL,
    RECORDS_FETCHED_TOTAL,
    BYTES_FETCHED_TOTAL,
    RECORDS_FILTERED_TOTAL,
    ACCUMULATE_FLUSHES_TOTAL,
    ACCUMULATE_BATCH_ROWS,
    ACCUMULATE_BATCH_BYTES,
    ACCUMULATE_PENDING_BYTES,
    SNAPSHOT_ROWS_TOTAL,
    SNAPSHOT_ROWS_OVERSIZE_TOTAL,
    SNAPSHOTS_TOTAL,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_unique_prefixed_and_prometheus_shaped() {
        let mut seen = std::collections::HashSet::new();
        for name in ALL {
            assert!(seen.insert(*name), "duplicate metric name {name}");
            assert!(
                name.starts_with("dfe_fetcher_"),
                "{name} lacks the app prefix"
            );
            assert!(
                name.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                "{name} is not a Prometheus metric name"
            );
        }
        for counter in ALL.iter().filter(|n| n.ends_with("_total")) {
            assert!(
                !counter.contains("_seconds"),
                "{counter} mixes unit and counter suffix"
            );
        }
    }
}
