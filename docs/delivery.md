<!-- Project:   dfe-fetcher                                                -->
<!-- File:      docs/delivery.md                                           -->
<!-- Purpose:   What happens to a record between its source and the output -->
<!-- Language:  Markdown                                                   -->
<!--                                                                       -->
<!-- License:   BUSL-1.1                                                   -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED                                -->

# Delivery, enrichment and cursors

What happens to a record between its source and the output: the emit path, its dead letters, the guarantee each intake gives, the fields enrichment stamps, and what a tick does with no cursor. The tick itself is in [internals.md](internals.md#one-tick).

## Emit

The emitter issues a flush's sends as concurrent futures (`accumulate.in_flight`
at a time). Every record gets a terminal outcome: sent, dead-lettered (the
transport refused that record and would again, so the record goes to the DLQ
with its payload cut at `dlq.max_payload_bytes`), or backpressured. The
backpressured subset is retried with a bounded backoff and, if still refused,
the whole tick aborts without a checkpoint so the scheduler's stall handling
takes over. A failure every record shares (the transport closed or timed out,
the broker gone) is not a dead letter: the tick aborts and the rows are
fetched again.

## Dead letters

A dead letter does not count until the DLQ confirms a backend holds it (scalo's `Dlq::write_confirmed`: on disk for the file backend, acked by the broker for Kafka). A write the DLQ rejects or cannot confirm aborts the tick like a transport failure, so the checkpoint never passes a record nothing holds and the record is fetched again.

An enabled DLQ with no backend that starts (an unwritable spool directory, a Kafka backend with no client config) logs an ERROR at startup and rejects every dead-letter write, so a refused record stays unacknowledged: its tick aborts and fetches it again rather than counting it held. A DLQ config that does not parse fails startup.

With no DLQ configured, or a disabled one, a record the transport rejects is still refused for good, so fetching it again would hold the source back and gain nothing. It is dropped and counted in `pipeline_dead_letters_dropped_total{reason="transport_refused"}`, an oversize row in `{reason="too_large"}` (with a warn-level log line, debounced to one every 10 seconds a connection) and a row enrichment refused in `{reason="unstampable"}`, and the tick goes on. A dead letter no DLQ backend can ever hold (scalo's `Dlq::refusal`) is left out of the DLQ write and dropped the same way, counted under scalo's reason. A `kafka_only` DLQ has such a ceiling, the producer's `message.max.bytes` once encoded, and the default `dlq.max_payload_bytes` cut keeps an entry under it. Any other DLQ failure can clear, so it still aborts the tick.

## Delivery guarantees

Each intake reports the guarantee it gives in `pipeline_delivery_guarantee{listener, guarantee, reason}`, from scalo's `EffectiveGuarantee::publish_for` against what the outputs confirm. A scheduled source and the ingest listener hold their cursor or answer until delivery, so over Kafka or gRPC they are `at_least_once`. The ingest listener and Vector follow their `acknowledgements` setting, and a container's stdout is `best_effort` (`source_cannot_ack`).

## Enrichment

Five names are reserved on every record: `_timestamp_fetcher`, `_timestamp_received`, `_source` (the topic base, or the destination a single-destination route names), `_source_fetcher` (`<connection>.<unit>`) and `_org_id` (`org_id`, else `default_org_id`). The per-source CEL filter runs on the provider's row before they are added, so a filter cannot reference them. A payload that has one keeps its value under `<key>_original` and the fetcher's value takes the name: the loader routes on `_source` and the record's org is its `_org_id`, so the fetcher's values have to be the ones that survive, and a record carrying the same top-level key twice is rejected outright by ClickHouse rather than dead-lettered.

`<key>_original` is never overwritten. A payload that has both the
reserved name and its `_original` -- a replayed record on a second enrich
pass, say -- keeps the `_original` it arrived with, and the colliding value is
parked under the next free `<key>_original_<n>` counting from 2, with a
warning naming the key.

## Payloads that are not one object

A payload that opens as a JSON object, after whitespace or a UTF-8 byte-order mark, and cannot be stamped is refused, because sent as it came it would keep any `_org_id` its sender gave it. That is an object with no closing brace, or one that may have a reserved key and does not parse. "May have" is a byte scan for the names and for any backslash, since an escape can spell a name, so a payload that fails to parse cannot be shown free of one, and appending the fetcher's stamp could give the record two `_org_id`. A refused row is never sent: it is dead-lettered with the reason, or with no DLQ dropped and counted in `pipeline_dead_letters_dropped_total{reason="unstampable"}`, and its bytes count for no org. A stamped row leaves without the byte-order mark.

A JSON array of objects goes out as one record per element, each stamped, because the loader splits an array into its elements and an element with no `_org_id` lands in its default org. A body of two or more whitespace-separated objects and nothing else, newline-delimited records in one payload, is split the same way, exactly where the loader's split would fan it out: the stamp lands before the last closing brace, so sent whole it would reach the last record and no other. Each record counts for usage at its length before the stamp, and the row's mark rides its last record. `[]` goes out as nothing.

A binary row, and a payload that is neither an object nor such a split body (a scalar, plain text, an array holding a non-object), goes out verbatim with no `_org_id`. The loader rejects it, though the archiver may keep it. Its usage counts under `default_org_id`, because usage measures ingest into the platform, not what the loader stores.

## Cursor cold start

A tick with no window cursor stored, or no cursor store at all, is a cold start. Every one counts in `dfe_fetcher_cursor_cold_start_total{source}`, because an empty store cannot tell a new source from a lost cursor: a cursor directory with no volume behind it loses every cursor on a restart. Under `lookback` the first cold start of a source logs a WARN and later ones log at DEBUG, since with no store at all every tick is one. Under `refuse` the tick is refused and logs an ERROR, at most once every 10 s.

A window cursor read that fails is a fault, not a cold start. The cursor may still be stored further back than the lookback, so the tick is refused under either `on_missing_cursor` setting and the next tick reads again. The window stays where the stored cursor left it, rather than sliding forward and skipping what sat behind it. Each refusal counts in `dfe_fetcher_cursor_read_failures_total{source}` and as a failed fetch, and logs an ERROR at most once every 10 s per source.

A unit checkpoint (a tail's key, a lister's last object, a file's offset) is read differently. A read of it that fails logs `checkpoint read failed; starting the unit over` and the unit runs as if it had none, so it reads again what an empty checkpoint starts from.

`cursor.on_missing_cursor` decides what the tick does. `lookback` (the default) fetches the last `default_window_hours`, so a new source starts unaided and a lost cursor skips anything older. `refuse` fetches nothing and fails the tick until a cursor exists, so a lost cursor never skips data and a new source never starts unaided. Each refused tick counts as a failed fetch, and the refusal logs an ERROR at most once every 10 s per source.
