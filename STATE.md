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
5. Tracks incremental fetch state via cursor store (file or Kafka)
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
9. **Cursor Store** — Incremental fetch state persistence (file or Kafka backend, auto-selected)
10. **TieredSink** — In-memory buffering with circuit breaker (from rustlib)
11. **Ingest Server** — axum HTTP server for container extractors with bearer token auth
12. **Metrics** — Prometheus-compatible `/metrics` endpoint with fetch/extractor/memory counters

### Tech Stack

- **Language:** Rust
- **HTTP Client:** reqwest (shared factory in `src/credential.rs`)
- **HTTP Server:** axum 0.7 (metrics + ingest endpoint)
- **Output Transport:** rustlib Transport trait (wraps rdkafka + tonic/gRPC)
- **Shared Library:** hyperi-rustlib (config, secrets, metrics, tiered-sink, transport, expression)
- **Container Runtime:** Docker or podman (exec via CLI)
- **Deployment:** Kubernetes (one container per source + config)

---

## Key Decisions

### Container Communication

**Decision:** Containers send data to the fetcher via stdout (JSON lines), HTTP POST, or gRPC (Vector protocol).
**Rationale:** Stdout is simplest for one-shot tools, HTTP for continuous processes, gRPC for Vector-native sources.
**Alternatives considered:** Unix sockets (too platform-specific), shared volumes (complex lifecycle).

### No Horizontal Scaling

**Decision:** Each fetcher instance handles one set of sources. Scale by deploying multiple instances.
**Rationale:** Cloud API rate limits are per-credential, not per-instance. No benefit to fan-out.

### Native vs Container Decision

**Decision:** Use native Rust when good crates exist; container for everything else.
**Rationale:** Native gives best performance and integration. Containers give language freedom for third-party tools.

### forbid(unsafe_code)

**Decision:** `#![forbid(unsafe_code)]` in lib.rs. No plugin system — plugin feature was removed.
**Rationale:** Safety guarantee for the entire codebase. Three extraction modes (native, container, vector) cover all use cases without needing dynamic .so loading.

### Credential Resolution

**Decision:** Three-prefix format: `vault:path:key`, `env:VAR_NAME`, or literal string.
**Rationale:** Simple, extensible, works for all sources. Vault resolution uses rustlib SecretsManager with OpenBao SecretSource.

---

## External Dependencies

- **hyperi-rustlib** — Config cascade, secrets, logger, metrics, Transport trait (Kafka + gRPC), tiered-sink, config reload, expression (CEL filtering), CLI framework
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
├── buffer/           # Memory pressure tracking, TieredSink wrapper
│   ├── mod.rs        # BufferManager with saturating atomic ops
│   └── tiered.rs     # TieredSink<S> with circuit breaker
├── config/           # 7-layer config cascade, all config structs
│   ├── mod.rs        # Config, validation, all sub-configs
│   └── shared.rs     # SharedConfig (Arc<RwLock<Config>>)
├── credential.rs     # Credential resolver (vault/env/literal), OAuth2 TokenManager, HTTP client factory
├── cursor/           # Incremental fetch state persistence
│   ├── mod.rs        # CursorStore trait, CursorValue, auto-select backend
│   ├── file.rs       # File-based cursor backend (one JSON file per key)
│   └── kafka.rs      # Kafka compacted topic cursor backend
├── error.rs          # Centralised error types (Config, Source, Credential, Output, Extractor)
├── extractor/        # External extractors
│   ├── mod.rs        # Extractor trait
│   ├── container/    # Docker/podman container management (pull, run, stderr, timeout, restart)
│   └── vector/       # Vector.dev gRPC integration via rustlib
├── ingest/           # HTTP ingest server (axum) with bearer token auth
│   └── mod.rs        # POST /ingest/:source, GET /health
├── lib.rs            # Public module exports, #![forbid(unsafe_code)]
├── main.rs           # CLI entry point (clap), signal handling (SIGINT+SIGTERM)
├── metrics/          # Prometheus metrics with saturating gauge ops
│   └── mod.rs        # Metrics struct, render() for /metrics
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
├── container_integration.rs  # Docker container extractor tests (3 tests, requires Docker)
├── integration.rs    # Config, enrichment, metrics, credentials, buffer (19 tests)
├── smoke_cloud.rs    # Live cloud API tests (8 tests, requires credentials)
├── source_aws.rs     # AWS wiremock + disabled/health-check tests
├── source_azure.rs   # Azure wiremock + disabled/health-check tests
├── source_gcp.rs     # GCP wiremock + disabled/health-check tests
└── source_m365.rs    # M365 wiremock + disabled/health-check tests

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
