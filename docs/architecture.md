# dfe-fetcher architecture

The repo is a Cargo workspace: one app crate and a source framework split into
an I/O-free core plus one leaf crate per source family, with Vector's file
tailer copied in under `third-party/`. Every crate is `publish = false` and
inherits its version from `[workspace.package]`, so a release bump touches the
root manifest only. Diagrams (system, the tick sequence, the profile grammar,
the crate DAG) live in [internals.md](internals.md). This file is the
codemap, the invariants and the build graph.

## Codemap

| Where | What lives there |
|---|---|
| `crates/core` (`dfe-fetcher-core`) | The row model (`Row`, `Mark`, `RowSource`, `UnitSpec`, `TickCtx`), the batcher, the compiled per-row rules (CEL filter, routes, added fields), the snapshot envelope and its reassembler, the checkpoint contract (`CursorStore`, `CursorValue`, mark folding), the once-resolved secret cell (the resolver is the I/O crate's), the framework's error type and the list of metric names. No HTTP, no transport, no filesystem. |
| `crates/rest` (`dfe-fetcher-rest`) | The declarative REST profile grammar and its validation, `{{ cel }}` templates, the runtime axes (auth modes, pagers, streaming decoders, the request executor with retry and its per-unit origin gate), the hooks (row builders and the S3 lister), the queue unit kind, and `RestShape`, the `RowSource` for a bound profile. |
| `crates/db` (`dfe-fetcher-db`) | The `Store` contract (dump, keyset tail), the `sources.db` grammar, `DbShape` (the `RowSource` over an instance's stores), the per-dialect keyset predicate, the blocking-cursor pump and the block-to-row framing that leases every buffered block. Engines are features: `odbc` (unixODBC + `arrow-odbc` typing + `arrow-json` writing), `clickhouse` (server-side `JSONEachRow`, `dfe-clickhouse` for the ping and typed exceptions) and `mongodb` (the official driver: `find` to dump, change stream or `_id` to tail). |
| `crates/file` (`dfe-fetcher-file`) | The `FileSource` contract, the `sources.file` grammar, the dump reader (NDJSON, JSON array, CSV, gzip by magic) and the tail spec. The tailer sits behind the `tail` feature, over the copy of Vector's tailer in `third-party/`. |
| `third-party/vector-file-source{,-common}` | Vector's file tailer (MIT), copied from Vector's repository and depended on by path from `crates/file` only. Provenance in `third-party/README.md`. |
| `crates/fetcher` (`dfe-fetcher`, the binary) | The config cascade and hot reload, the typed source blocks, their registry and their mapping onto profile instances (`crates/fetcher/src/config/builtin.rs`), the scheduler, `Driver` (the tick the scheduler runs for every framework source), the emitter, the pipeline (enrichment, DLQ, readiness), the output transports, the cursor file store, the shipped REST profiles (`profiles/*.yaml`), the deployment contract and capability catalog, the ingest server and the container and Vector extractors. |
| `config.example.yaml`, `docs/` | Operator-facing surface, at the repo root. The app crate's tests reach them through the manifest directory. |

Adding a REST source is a profile (YAML) plus an instance in `sources.rest`.
Adding a hook is a variant on an axis enum in `crates/rest`. Adding an engine is
a feature-gated module in `crates/db`. A typed source block (`sources.github`,
`sources.aws`) keeps its struct for the operator and maps onto an instance of
the shipped profile of the same name at load. The driver runs it like any
`sources.rest` entry.

## Negative invariants

- `dfe-fetcher-core` depends on no other workspace crate and on scalo only for
  `expression` and `rules`. It never names an HTTP client, a transport or a
  file. Async appears only as `futures-core` stream and future types and the
  `async-trait` checkpoint store. tokio is linked for its clock types and its
  once-cell and never spawns a task there.
- `dfe-fetcher-rest`, `dfe-fetcher-db` and `dfe-fetcher-file` depend on `core`
  and never on each other or on the app. None sends to a transport, writes a
  cursor or reads the config cascade. Each receives bound units and yields rows.
- The app never re-implements a source, a pager, a decoder or an auth mode. It
  owns wiring, transports, checkpoint persistence, config and the deployment
  contract. There is no per-provider Rust in the app: a provider is a profile,
  and what a profile cannot say is a hook on an axis in `crates/rest`.
- No crate depends on a Vector crate from a registry. The file tailer is the
  copy of Vector's under `third-party/`, depended on path-only by
  `dfe-fetcher-file`.
- Nothing deployment-specific: no hostnames, addresses, vault paths or licence
  detail outside `config.example.yaml` placeholders and tests.

## Build graph

`core` is the root, on serde, bytes, chrono, uuid, smallvec, csv-core, cel and
scalo's `expression` and `rules`. `rest` (reqwest, async-compression,
backon, reqsign), `db` and `file` are siblings off `core` and compile in
parallel, and the app links all four. The shipped profiles are the app's own
(`include_str!` in `crates/fetcher/src/profiles/mod.rs`), so editing one
rebuilds the app alone. A pager or hook change rebuilds `rest` and the app, and
an engine bump rebuilds `db` and the app. `db` is always linked for its grammar,
so a `sources.db` block naming an engine the binary lacks is refused at load
with the feature to build. The engines themselves and the file tailer are off
the default build.

The database and tailer features on the app follow scalo's
`<capability>-<backend>` naming: `db-odbc`, `db-clickhouse`, `db-mongodb` and
`file-tail`. Beside them are `file` (kept as a name, the dump is always built),
`jemalloc`, `full` and `pgo-driver`, which builds the PGO workload binary.
`.hyperi-ci.yaml` lists feature sets explicitly so a runner is never asked for a
native driver it does not carry, and `db-odbc` needs unixODBC on the host. The
release build turns on `jemalloc` and all four engine and tailer features, and
its image carries unixODBC and the open-licence drivers
([reference/db-drivers.md](reference/db-drivers.md)).
