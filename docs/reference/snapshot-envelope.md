<!-- Project:   dfe-fetcher                            -->
<!-- File:      docs/reference/snapshot-envelope.md     -->
<!-- Purpose:   Reference for the snapshot envelope dump units travel in -->
<!-- Language:  Markdown                                 -->
<!--                                                     -->
<!-- License:   BUSL-1.1                                 -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED              -->

# The snapshot envelope

A dump unit's rows travel in an envelope so a consumer can rebuild the whole
store by `snapshot_id` and tell a truncated dump from a complete one. A dump
is one `begin` frame, the store's rows, and one `end` frame carrying the row
count, all on the same topic and all stamped with the same `snapshot_id` and
`snapshot_at`. A dump that aborts emits no `end`, which is the consumer-side
incompleteness signal. The writer and a reference reassembler live in
`crates/core/src/envelope.rs`; the driver applies the envelope to every unit
whose shape is `dump` (a REST profile dump, a `sources.db` dump store, a
`sources.file` dump unit).

- [Frames](#frames)
- [Fields](#fields)
- [Sequence numbers](#sequence-numbers)
- [Scope and topics](#scope-and-topics)
- [Oversize rows](#oversize-rows)
- [Consuming a snapshot](#consuming-a-snapshot)
- [Metrics](#metrics)

## Frames

| `kind` | When | Carries |
|--------|------|---------|
| `begin` | First frame of a dump, buffered before the first row is polled | `total`, the store's own count, when the connector knows it (optional) |
| `row` | One provider record | `record`, the record as the provider sent it |
| `oversize` | A record that exceeded the transport limit, replaced by a stub | `bytes`, the size of the record not sent; `row_key`, its identity per the unit's `row_key` when it had one |
| `end` | Last frame of a dump | `row_count`, rows and stubs emitted; `completed_at`, when the dump finished |

## Fields

Every frame carries the head; the kind-specific fields follow it.

| Field | Every frame | Meaning |
|-------|-------------|---------|
| `kind` | yes | `begin`, `row`, `oversize` or `end` |
| `snapshot_id` | yes | UUIDv7 minted at `snapshot_at`; the dump this frame belongs to |
| `snapshot_at` | yes | when the dump started, RFC 3339 with milliseconds |
| `timestamp` | yes | equal to `snapshot_at`, so the loader's `_timestamp` groups the whole dump |
| `store` | yes | `<connection>.<unit>` |
| `seq` | yes | 0-based position: the row's own for `row` and `oversize`, 0 for `begin`, the row count for `end` |
| `total` | `begin`, optional | the store's own count when the connector knows it |
| `record` | `row` | the record as the provider sent it, under its own key so the provider's fields cannot collide with the envelope's |
| `bytes`, `row_key` | `oversize` | the size of the record that was not sent, and its identity per the unit's `row_key` (absent when the unit names none or the row had none) |
| `row_count`, `completed_at` | `end` | rows and oversize stubs emitted -- what a consumer must count -- and when the dump finished |

The pipeline's enrichment adds `_timestamp_fetcher`, `_timestamp_received`,
`_source` and `_source_fetcher` to every frame as it does to every record.

## Sequence numbers

`seq` is monotonic over `row` and `oversize` frames, so `end.row_count`
equals the number of those frames a consumer must see and every `seq` in
`0..row_count` is present exactly once in a complete dump. A row the
per-source filter drops gives its `seq` back, so what lands stays contiguous:
on a dump the filter sees the enveloped row (`record.alive == true` in the
example config), and a dropped row is never a hole.

## Scope and topics

| Shape | One snapshot covers | Opened |
|-------|---------------------|--------|
| REST profile dump (`shape: dump`) | one tick of one unit | before the first row, so an empty store lands as an empty snapshot (`begin` then `end` with `row_count` 0) |
| `sources.db` dump store | one tick of one store | before the first row, likewise |
| `sources.file` dump unit | one file | at the file's first row; closed -- `end`, flush, checkpoint -- when the next file starts, so an idle tick publishes nothing and a bad file leaves the files before it complete and committed |

A dump unit lands on its own topic, `<topic>-<unit>` with the deployment's
topic suffix appended (`_land` by default), so the stores of one connector
never interleave; incremental units land on `<topic>` plus the suffix. Two
instances sharing a `topic` are told apart by `_source_fetcher`
(`<connection>.<unit>`) and by `store`.

## Oversize rows

A row longer than `oversize.max_record_bytes` (the deployment's
`oversize:` block in `config.example.yaml`) becomes an `oversize` stub that
takes the next `seq`, so the count still reconciles, and a copy cut at
`oversize.max_dlq_bytes` goes to the dead-letter queue with the reason. An
oversize row of an incremental unit (no envelope) goes to the DLQ and is
skipped.

## Consuming a snapshot

Frames may arrive in any order and more than once (at-least-once delivery).
A snapshot is complete only when its `end` has arrived and every `seq` in
`0..row_count` is present as a `row` or an `oversize` stub; `end` seen with a
`seq` missing is truncated, and no `end` is open. The `Reassembler` in
`crates/core/src/envelope.rs` is that check as code: feed it frames, ask its
status by `snapshot_id`, and take the rows of a complete snapshot in `seq`
order (stubs counted for completeness and excluded from the rows).

## Metrics

| Metric | Labels | Meaning |
|--------|--------|---------|
| `dfe_fetcher_snapshots_total` | `store`, `status` (`complete`, `aborted`) | snapshots closed with an `end`, and dump ticks that failed before one |
| `dfe_fetcher_snapshot_rows_total` | `store` | `row` frames emitted |
| `dfe_fetcher_snapshot_rows_oversize_total` | `store` | `oversize` stubs emitted |

The names are declared in `crates/core/src/metric_names.rs`.
