# dfe-fetcher architecture

The repo is a Cargo workspace: one app crate and a source framework split into
an I/O-free core plus one leaf crate per shape family, with the file tailer
vendored from Vector under `third-party/`. Every crate is `publish = false` and
inherits its version from `[workspace.package]`, so a release bump touches the
root manifest only. Diagrams (system, the tick sequence, the profile grammar,
the crate DAG) live in [DESIGN.md](DESIGN.md); this file is the
codemap, the invariants and the build graph.

## Codemap

| Where | What lives there |
|---|---|
| `crates/core` (`dfe-fetcher-core`) | The row model (`Row`, `Mark`, `RowSource`, `UnitSpec`, `TickCtx`), the batcher, the compiled per-row rules (CEL filter, routes, added fields), the snapshot envelope and its reassembler, the checkpoint contract (`CursorStore`, `CursorValue`, mark folding), the once-resolved secret cell (the resolver is the I/O crate's), the framework's error type and the list of metric names. No HTTP, no transport, no filesystem. |
| `crates/rest` (`dfe-fetcher-rest`) | The declarative REST profile grammar and its validation, `{{ cel }}` templates, the runtime axes (auth modes, pagers, streaming decoders, the request executor with retry and its per-unit origin gate), the hooks (row builders and the S3 lister), the queue shape, and `RestShape`, the `RowSource` for a bound profile. |
| `crates/db` (`dfe-fetcher-db`) | The `Store` contract (dump, keyset tail), the `sources.db` grammar, `DbShape` (the `RowSource` over an instance's stores), the per-dialect keyset predicate, the blocking-cursor pump and the block-to-row framing that leases every buffered block. Engines are features: `odbc` (unixODBC + `arrow-odbc` typing + `arrow-json` writing), `clickhouse` (server-side `JSONEachRow`, `clickhouse-dfe` for the ping and typed exceptions) and `mongodb` (the official driver: `find` to dump, change stream or `_id` to tail). |
| `crates/file` (`dfe-fetcher-file`) | The `FileSource` contract, the `sources.file` grammar, the dump reader (NDJSON, JSON array, CSV, gzip by magic) and the tail spec; the tailer sits behind the `tail` feature over the vendored crates. |
| `third-party/vector-file-source{,-common}` | Vector's file tailer (MIT), vendored and depended on by path from `crates/file` only; provenance in `third-party/README.md`. |
| `crates/fetcher` (`dfe-fetcher`, the binary) | The config cascade and hot reload, the typed source blocks, their registry and their mapping onto profile instances (`config/builtin.rs`), the scheduler, `Driver` (the tick the scheduler runs for every framework shape), the emitter, the pipeline (enrichment, DLQ, readiness), the output transports, the cursor file store, the shipped REST profiles (`profiles/*.yaml`), the deployment contract and capability catalog, the ingest server and the container and Vector extractors. |
| `config.example.yaml`, `docs/` | Operator-facing surface, at the repo root; the app crate's tests reach them through the manifest directory. |

Adding a REST source is a profile (YAML) plus an instance in `sources.rest`;
adding a hook is a variant on an axis enum in `crates/rest`; adding an engine is
a feature-gated module in `crates/db`. A typed source block (`sources.github`,
`sources.aws`) keeps its struct for the operator and maps onto an instance of
the shipped profile of the same name at load; the driver runs it like any
`sources.rest` entry.

## Negative invariants

- `dfe-fetcher-core` depends on no other workspace crate and on scalo only for
  `expression`. It never names an HTTP client, a transport or a file. Async
  appears only as `futures-core` stream and future types; tokio is linked for
  its clock types and its once-cell and never spawns a task there.
- `dfe-fetcher-rest`, `dfe-fetcher-db` and `dfe-fetcher-file` depend on `core`
  and never on each other or on the app. None sends to a transport, writes a
  cursor or reads the config cascade; each receives bound units and yields rows.
- The app never re-implements a shape, a pager, a decoder or an auth mode; it
  owns wiring, transports, checkpoint persistence, config and the deployment
  contract. There is no per-provider Rust in the app: a provider is a profile,
  and what a profile cannot say is a hook on an axis in `crates/rest`.
- No crate depends on a Vector crate from a registry. The file tailer is the
  vendored copy under `third-party/`, depended on path-only by
  `dfe-fetcher-file`.
- Nothing deployment-specific: no hostnames, addresses, vault paths or licence
  detail outside `config.example.yaml` placeholders and tests.

## Build graph

`core` is the root and compiles in seconds (serde, bytes, chrono, uuid,
smallvec, csv-core, cel via scalo `expression`). `rest` (reqwest,
async-compression, backon, reqsign), `db` and `file` are siblings off `core`
and compile in parallel. The cold-compile critical path is unchanged from the single-crate
layout: scalo with `transport-kafka` (librdkafka) and `transport-grpc` (tonic,
prost), then the app's link. A profile or pager change rebuilds `rest` and the
app; an engine bump rebuilds `db` alone. `db` is always linked for its grammar
(a few serde types), so a `sources.db` block naming an engine the binary lacks
is refused at load with the feature to build; the engines themselves and the
file tailer are off the default build.

Feature flags on the app follow scalo's `<capability>-<backend>` naming:
`db-odbc`, `db-clickhouse`, `db-mongodb`, `file`, `file-tail`, plus `jemalloc`
and `full`. `.hyperi-ci.yaml` lists feature sets explicitly so a runner is
never asked for a native driver it does not carry; `db-odbc` needs unixODBC on
the host.
