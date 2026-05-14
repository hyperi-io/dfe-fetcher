## CI

CI is live via `hyperi-ci`. Run `hyperi-ci check` (or `make check`) locally before pushing.

---

# Project Context

**Project:** dfe-fetcher
**Purpose:** Data fetcher for external services (AWS, Azure, M365, GCP) with container-based extractor support

> **Note:** The `ai/` submodule provides standards and configuration - not code
> to import. Your project never imports or links to it.

---

## DO NOT ADD TO THIS FILE

**The following belong elsewhere:**

| Data | Correct Location |
|------|------------------|
| Version numbers | `VERSION` file, `git describe --tags` |
| Tasks/Progress | `TODO.md` |
| Session history | Git log (`git log --oneline -10`) |
| Changelog | `CHANGELOG.md` (semantic-release) |
| Dates | Git commit timestamps |

**This file is for static project context only.**

---

## Project Overview

### Architecture

Native Rust fetcher that:

1. Fetches data from cloud services via native Rust sources (AWS, Azure, M365, GCP)
2. Manages container-based extractors for third-party tools (any language)
3. Integrates with Vector.dev via native gRPC protocol
4. Delivers all fetched data to Kafka and/or gRPC for the DFE pipeline
5. Tracks incremental fetch state via file-based cursor store (PVC-backed)
6. Filters records per-source using CEL expressions before delivery

### Three Extraction Modes

1. **Native Sources** (`src/source/`) — Rust-native implementations using crates
2. **Container Extractors** (`src/extractor/container/`) — Docker/podman isolated containers
3. **Vector Extractors** (`src/extractor/vector/`) — Vector.dev instances via gRPC

### Decision Framework

- If a good Rust crate / OSS project exists → build natively in `src/source/`
- If a great OSS tool exists in another language → wrap in container
- One container per source + config (no horizontal scaling)
- N instances of same type with different configs (e.g., 10 M365 orgs)

### Key Components

1. **Scheduler** — Timed fetch with jitter (fastrand), concurrency control via semaphore
2. **Native Sources** — AWS, Azure, M365, GCP source implementations with OAuth2/SigV4 auth
3. **Credential Resolver** — `vault:path:key`, `env:VAR`, literal string resolution via rustlib SecretsManager
4. **Token Manager** — OAuth2 client_credentials with automatic caching (60s before expiry)
5. **Container Manager** — Docker/podman lifecycle with image pull policies, stderr capture, timeouts, restart-on-crash with exponential backoff
6. **Vector Manager** — Native gRPC Vector protocol integration via rustlib GrpcTransport
7. **Pipeline** — Enrichment (`_timestamp_fetcher`, `_source_fetcher`, `_timestamp_received` fields) + CEL filtering + output delivery
8. **Output Transport** — Unified transport layer via rustlib Transport trait (Kafka, gRPC, or both)
9. **Cursor Store** — Incremental fetch state persistence (single JSON file, PVC-backed)
10. **MemoryGuard** — Cgroup-aware memory pressure detection (from rustlib)
11. **Ingest Server** — axum HTTP server for container extractors with bearer token auth
12. **Metrics** — DfeMetrics dual-emit (`dfe_fetcher_*` prefix) + MetricsManager Prometheus endpoint

### Tech Stack

- **Language:** Rust
- **HTTP Client:** reqwest (shared factory in `src/credential.rs`)
- **HTTP Server:** axum 0.8 (ingest endpoint) + rustlib HttpServer (metrics/health)
- **Output Transport:** rustlib Transport trait (Kafka + gRPC, no direct rdkafka dep)
- **Shared Library:** hyperi-rustlib (config, secrets, metrics, tiered-sink, transport, expression)
- **Container Runtime:** Docker or podman (exec via CLI)
- **Deployment:** Kubernetes (one container per source + config)

---

## Key Decisions

### Container Communication

**Decision:** Containers send data to the fetcher via stdout (JSON lines), HTTP POST, or gRPC (Vector protocol).
**Rationale:** Stdout is simplest for one-shot tools, HTTP for continuous processes, gRPC for Vector-native sources.
**Alternatives considered:** Unix sockets (too platform-specific), shared volumes (complex lifecycle).

### No Horizontal Scaling (BY DESIGN)

**Decision:** Each fetcher instance handles one set of sources. Scale by deploying multiple instances with different configs. Never run multiple pods with the same source config.

**Rationale:**
- Cloud API rate limits are per-credential, not per-instance — two pods with the same AWS credentials hit the same rate limit, halving effective throughput while doubling API calls
- Two pods fetching the same source would both fetch the same time window, producing duplicate data
- Cursor contention: concurrent writers to the same cursor key cause lost updates
- True scale-out would require distributed locking, work partitioning, cursor CAS, and leader election — essentially a distributed scheduler for zero throughput gain
- The fetcher is I/O bound (waiting for API responses), not CPU bound — one pod easily saturates a cloud API's rate limit

**Scaling pattern:** 50 M365 tenants → 50 pods, each with unique `instance_id` and config. K8s handles scheduling and restarts. Each pod is fully independent — no coordination needed.

**Alternatives rejected:** Active/passive failover (K8s leader election lease) — adds complexity for marginal availability gain since K8s already restarts crashed pods.

### Native vs Container Decision

**Decision:** Use native Rust when good crates exist; container for everything else.
**Rationale:** Native gives best performance and integration. Containers give language freedom for third-party tools.

### forbid(unsafe_code)

**Decision:** `unsafe_code = "deny"` in Cargo.toml lints. No plugin system — removed in favour of container/sidecar approach.
**Rationale:** Safety guarantee for the entire codebase. Three extraction modes (native, container, vector) cover all use cases.

### Cursor Store (File-Based, Not Kafka)

**Decision:** Single JSON file at a configurable path (`cursor.file_path`), PVC-backed for pod restart persistence. No Kafka cursor backend.
**Rationale:**
- Fetchers are single-writer-per-config by design — no shared state needed between pods
- Kafka cursor topic was over-engineered: each pod writes its own cursors, never reads another pod's
- File-based is simpler, debuggable (kubectl exec + cat), and has no external service dependency
- Config cascade specifies WHERE the file lives; the cursor store writes independently
- Read-only fallback: if PVC is unavailable, runs degraded (re-fetches default window, logs warning)
**Alternatives rejected:** Kafka compacted topic (unnecessary for single-writer), K8s ConfigMap API (extra RBAC), OpenBao KV (adds latency + availability coupling)

### Credential Resolution

**Decision:** Three-prefix format: `vault:path:key`, `env:VAR_NAME`, or literal string.
**Rationale:** Simple, extensible, works for all sources. Vault resolution uses rustlib SecretsManager with OpenBao SecretSource.

---

## External Dependencies

- **hyperi-rustlib** — Config cascade, secrets, logger, metrics, Transport trait (Kafka + gRPC), tiered-sink, config reload, expression (CEL filtering), CLI framework, MemoryGuard, ScalingPressure, DfeMetrics

### CRITICAL: hyperi-rustlib Usage Rules

- **ALWAYS use the crates.io release** — `hyperi-rustlib = { version = ">=X.Y.Z", features = [...] }` in Cargo.toml
- **NEVER add `path = "/projects/hyperi-rustlib"` to Cargo.toml** — local path overrides break CI and other developers
- **Source is at `/projects/hyperi-rustlib`** for reading API docs and checking available features — READ ONLY, never link to it
- **To test unreleased rustlib changes:** publish a new version to crates.io first, then bump the version here
- **AWS APIs** — CloudTrail, GuardDuty, SecurityHub, Config (SigV4 signing via reqsign)
- **Azure APIs** — Activity Log, Defender, Sentinel, Entra ID (Microsoft Graph + Azure Management)
- **M365 APIs** — Office 365 Management Activity API, Microsoft Graph Security
- **GCP APIs** — Cloud Audit Logs, Security Command Center, Cloud Logging (JWT signing via jsonwebtoken)
- **Kafka** — Output delivery via rustlib Transport trait (wraps rdkafka)
- **gRPC** — Optional output delivery to dfe-receiver via rustlib Transport trait

---

## Module Layout

```
src/
├── config/           # 7-layer config cascade, all config structs
│   ├── mod.rs        # Config, validation, all sub-configs
│   └── shared.rs     # SharedConfig (Arc<RwLock<Config>>)
├── credential.rs     # Credential resolver (vault/env/literal), OAuth2 TokenManager, HTTP client factory
├── cursor/           # Incremental fetch state persistence
│   ├── mod.rs        # CursorStore trait, CursorValue types
│   └── file.rs       # File-based cursor store (single JSON file, PVC-backed)
├── error.rs          # Centralised error types (Config, Source, Credential, Output, Extractor)
├── extractor/        # External extractors
│   ├── mod.rs        # Extractor trait
│   ├── container/    # Docker/podman container management (pull, run, stderr, timeout, restart)
│   └── vector/       # Vector.dev gRPC integration via rustlib
├── ingest/           # HTTP ingest server (axum) with bearer token auth
│   └── mod.rs        # POST /ingest/:source, GET /health
├── lib.rs            # Public module exports (unsafe_code = "deny" in Cargo.toml lints)
├── main.rs           # CLI entry point (clap), signal handling (SIGINT+SIGTERM)
├── metrics/          # DfeMetrics dual-emit + MetricsManager (dfe_fetcher_* prefix)
│   └── mod.rs        # Metrics struct, DfeMetrics wiring, hand-rolled render() fallback
├── output.rs         # Output transport layer (Kafka / gRPC / Both via rustlib Transport trait)
├── pipeline/         # Orchestration, enrichment, CEL filtering, output delivery
│   └── mod.rs        # PipelineState, Orchestrator, enrich_record
├── scheduler/        # Fetch timing with fastrand jitter, semaphore concurrency
│   └── mod.rs        # Scheduler with per-source interval overrides
└── source/           # Native source trait + providers
    ├── mod.rs        # Source trait, FetchResult struct
    ├── aws/          # AWS (CloudTrail, GuardDuty, SecurityHub, Config)
    ├── azure/        # Azure (Activity Log, Defender, Sentinel, Entra ID)
    ├── gcp/          # GCP (Audit Logs, SCC, Cloud Logging)
    └── m365/         # M365 (Audit Log, Message Trace, DLP, Alerts)

tests/
├── common/           # Shared test infrastructure (dual-mode Docker/remote)
│   └── mod.rs        # TestMode, KafkaTestConfig, skip_if_no_kafka! macro
├── integration/      # Integration tests (single binary, wiremock + unit-style)
│   ├── main.rs       # Test binary entry point (mod declarations)
│   ├── config.rs     # Config validation, env overrides, filter expressions
│   ├── credentials.rs # Credential resolver tests
│   ├── deployment.rs # DeploymentContract, topic suffix, cursor selection
│   ├── pipeline.rs   # Enrichment, CEL filtering, metrics rendering
│   ├── source_aws.rs # AWS wiremock tests (6 tests)
│   ├── source_azure.rs # Azure wiremock tests (8 tests)
│   ├── source_gcp.rs # GCP wiremock tests (7 tests)
│   └── source_m365.rs # M365 wiremock tests (10 tests)
├── e2e/              # End-to-end tests (requires Docker/Kafka, #[ignore])
│   ├── main.rs       # Test binary entry point
│   ├── container.rs  # Docker container extractor tests (3 tests)
│   ├── kafka.rs      # Kafka produce/consume roundtrip + enrichment (3 tests)
│   ├── kafka_cursor.rs # Kafka transport output tests (1 test)
│   └── smoke_cloud.rs # Live cloud API tests (8 tests, requires credentials)
├── fixtures/         # Test data files (empty — fixtures inline for now)
└── smoke.rs          # Mandatory startup smoke test (config, metrics, pipeline init)

benches/
└── pipeline.rs       # Pipeline enrichment throughput benchmark
```

---

## Resources

**Documentation:**

- [README.md](README.md) — Overview, quick start, configuration reference
- [docs/DESIGN.md](docs/DESIGN.md) — Architecture with mermaid diagrams
- [config.example.yaml](config.example.yaml) — Full annotated configuration

**Related Projects:**

- [dfe-receiver](https://github.com/hyperi-io/dfe-receiver) — HTTP/gRPC data ingest (sibling project)
- [hyperi-rustlib](https://github.com/hyperi-io/hyperi-rustlib) — Shared Rust library

---

## Notes for AI Assistants

This file contains **static project context only**.

**DO NOT add:**

- Version numbers (use `git describe --tags`)
- Progress/tasks (use `TODO.md`)
- Dates or session history (use `git log`)
- "Current Session" or "Last Session" sections

**DO add:**

- Architecture decisions and rationale
- Key component descriptions
- External dependencies
- How things work (not what's happening)

When in doubt, ask: "Will this be true next week?" If no, it doesn't belong here.

---

## Rust Release-Track Optimisation Readiness

**Tier 1 (allocator + fat LTO on beta+):** ✅ **READY**
- `jemalloc` feature only (mimalloc removed per 2026-04-17 policy)
- `#[global_allocator]` wired in `src/main.rs` under `cfg(feature = "jemalloc")`
- `default = []` — clean
- `[profile.release] lto = "thin"` — CI overrides to `fat`

**Tier 2 (PGO + BOLT on release):** ✅ **CONFIGURED**

Opt-in via `build.rust.optimize.pgo` in `.hyperi-ci.yaml` (enabled).
Workload contract:

- `scripts/pgo-workload.sh` — orchestrator: starts mock cloud-API server
  + Kafka container + fetcher with all 4 sources @ 1-second intervals
- `src/bin/pgo-driver.rs` (gated by `pgo-driver` feature) — long-running
  axum mock that serves Azure/M365/AWS/GCP response shapes (paginated,
  500 records/page by default)
- Hot path exercised: HTTP fetch → JSON parse → enrichment → CEL filter
  → output produce → cursor advance
- Duration floor 60s, default 300s
- Linux-only (BOLT requirement)

Reference implementations: dfe-loader v1.17.5, dfe-receiver. Channel
gating: spike/alpha → jemalloc + thin LTO; beta → jemalloc + fat LTO;
release → jemalloc + fat LTO + PGO + BOLT.

---

## POLICY UPDATE 2026-04-17 — jemalloc-only ✅ DONE

DFE allocator policy standardised on jemalloc. Source:
`hyperi-ai/standards/languages/RUST.md` → *Allocator Policy*.

mimalloc feature, dep, and main.rs fallback removed (2026-04-29). No
functional change to the published binary (jemalloc was always
selected; CI only passes `--features jemalloc`).

---

## CI workflow contract (post dfe-loader Canary 2) ✅ READY

`.github/workflows/ci.yml` MUST satisfy these for Tier 2 PGO/BOLT to
actually run on tagged releases:

| Setting | Required | Status |
|---|---|---|
| `uses: hyperi-io/hyperi-ci/.github/workflows/rust-ci.yml@<ref>` | `ba03ff0` (v1.12.1+) or `@main` | ✅ `@main` |
| `with: publish-target` | `both` | ✅ `both` |
| `.hyperi-ci.yaml` `build.rust.targets` | explicit linux targets | ✅ `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` |

Reference implementation: dfe-loader v1.17.5 — full Tier 2 verified
live on R2, build log signature
`channel=release, allocator=jemalloc, lto=fat, pgo=on, bolt=on`.

---

## Operational Status — Live Cloud Test Tenants (2026-04-29)

⚠️ **AWS, Azure, M365 test environments are being rebuilt.**
`tests/e2e/smoke_cloud.rs` cannot be run end-to-end against live
cloud APIs until those tenants are back online. Mock-backed
integration tests (`tests/integration/source_*.rs` via wiremock /
LocalStack) continue to work. Re-check tenant readiness before
attempting any of:

- `cargo test --test integration -- --ignored` (smoke_cloud)
- AWS SigV4 / Azure OAuth / M365 Graph live verification
- Terraform `plan/apply` in `infra/test/`

Ask the user to confirm tenant rebuild status before running these.
GCP smoke is unaffected.


---

## DFE Pipeline Context

**This app:** dfe-fetcher — Pull-mode ingress — polls AWS / Azure / M365 / GCP → Kafka.
**Criticality:** 3/6 (1 = highest)
**Rustlib rebuild wave:** 2

### Data flow

```text
                 ┌────────────────────────────────────────────┐
                 │                INGRESS                      │
                 │  ┌──────────────┐    ┌──────────────┐      │
                 │  │ dfe-receiver │    │ dfe-fetcher  │      │
                 │  │ (push: HTTP/ │    │ (pull: AWS / │      │
                 │  │  syslog/gRPC)│    │ Azure / M365)│      │
                 │  └──────┬───────┘    └──────┬───────┘      │
                 └─────────┼───────────────────┼──────────────┘
                           │                   │
                           └─────────┬─────────┘
                                     ▼
                         ┌─────────────────────┐
                         │  Kafka — ingress    │
                         └──────────┬──────────┘
                                    ▼
                         ┌──────────────────────┐
                         │      dfe-loader      │
                         │ (route, enrich,      │
                         │  parse, fan-out)     │
                         └──────────┬──────────┘
                                    ▼
                         ┌─────────────────────┐
                         │ Kafka — transform   │
                         └──────────┬──────────┘
                           │                  │
                           ▼                  ▼
                  ┌────────────────┐ ┌──────────────────────┐
                  │ dfe-transform- │ │ dfe-transform-vector │
                  │      vrl       │ │ (Vector.dev wrapper) │
                  └────────┬───────┘ └──────────┬──────────┘
                           │                    │
                           └─────────┬──────────┘
                                     ▼
                         ┌─────────────────────┐
                         │  Kafka — archive    │
                         └──────────┬──────────┘
                                    ▼
                         ┌─────────────────────┐
                         │    dfe-archiver     │
                         │ (S3 / Azure / GCS / │
                         │      MinIO)         │
                         └─────────────────────┘
```

### Siblings — the core six DFE Rust apps

| # | App | Role | Wave |
|---|-----|------|------|
| 1 | dfe-loader | Mid-tier routing, enrichment, parsing — most complex, best canary | 1 |
| 2 | dfe-receiver | Push ingress (HTTP / syslog / gRPC) — pipeline entry point | 1 |
| 3 | dfe-fetcher | Pull ingress (AWS / Azure / M365 / GCP) | 2 |
| 4 | dfe-archiver | Sink to object store — bookend of the pipeline | 1 |
| 5 | dfe-transform-vrl | Embedded VRL transform engine | 2 |
| 6 | dfe-transform-vector | Vector.dev subprocess wrapper (owns its own routing config) | 2 |

### Rustlib rebuild waves (ARC = 3 concurrent Rust CI builds)

- **Wave 1 — bookends + ingress:** dfe-loader, dfe-receiver, dfe-archiver.
  Covers the full data path (ingress → mid-tier → sink). If wave 1 is green,
  the pipeline structure is sound.
- **Wave 2 — remaining:** dfe-fetcher, dfe-transform-vrl, dfe-transform-vector.
  Pull ingress + transform layer.

Waves run sequentially; consumers within a wave run in parallel, capped at
the ARC runner's concurrent Rust CI capacity (3).

### Automation — `/rebuild-consumers` (driven from rustlib)

When `hyperi-rustlib` changes and the change needs to flow downstream,
**drive the rebuild from the `hyperi-rustlib` repo, not from this app**.
The rebuild-consumers skill in rustlib owns the wave plan, target version,
ARC capacity, and consumer scope:

```bash
# from the rustlib repo (sibling of this app):
python3 scripts/rebuild_consumers.py check        # surface breakage
python3 scripts/rebuild_consumers.py apply --wave 1
python3 scripts/rebuild_consumers.py apply --wave 2
```

Reference (relative to this app — adjust if your workspace layout differs):

- `../hyperi-rustlib/.claude/skills/rebuild-consumers/SKILL.md`
- `../hyperi-rustlib/.claude/consumers.toml`
- `../hyperi-rustlib/scripts/rebuild_consumers.py`
- `../hyperi-rustlib/STATE.md` → *Core DFE Apps*

### Backburner — not auto-rebuilt

`dfe-transform-elastic` and `dfe-transform-splack` have drifted significantly
against accumulated rustlib changes and need manual remediation before
re-joining the lockstep set. The automation **excludes** them by design.
Promotion requires a deliberate `tier = "core"` flip in
`../hyperi-rustlib/.claude/consumers.toml`.
