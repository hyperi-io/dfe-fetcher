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
3. Supports dynamically loaded Rust plugin modules (.so)
4. Integrates with Vector.dev via native gRPC protocol
5. Delivers all fetched data to Kafka for the DFE pipeline

### Four Extraction Modes

1. **Native Sources** (`src/source/`) — Rust-native implementations using crates
2. **Plugin Sources** (`src/extractor/plugin/`) — Dynamically loaded .so modules
3. **Container Extractors** (`src/extractor/container/`) — Docker/podman isolated containers
4. **Vector Extractors** (`src/extractor/vector/`) — Vector.dev instances via gRPC

### Decision Framework

- If a good Rust crate / OSS project exists → build natively in `src/source/`
- If a great OSS tool exists in another language → wrap in container
- One container per source + config (no horizontal scaling)
- N instances of same type with different configs (e.g., 10 M365 orgs)

### Key Components

1. **Scheduler** — Timed fetch with jitter, concurrency control
2. **Native Sources** — AWS, Azure, M365, GCP source implementations
3. **Container Manager** — Docker/podman lifecycle for external tools
4. **Plugin Loader** — Dynamic .so loading (same pattern as dfe-receiver)
5. **Vector Manager** — Native gRPC Vector protocol integration
6. **Pipeline** — Enrichment (timestamps, source metadata) + Kafka delivery
7. **TieredSink** — In-memory buffering with circuit breaker (from rustlib)

### Tech Stack

- **Language:** Rust
- **HTTP Client:** reqwest (for API calls)
- **HTTP Server:** axum 0.7 (metrics + ingest endpoint)
- **Kafka:** rdkafka
- **Shared Library:** hyperi-rustlib (config, secrets, metrics, tiered-sink)
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

---

## External Dependencies

- **hyperi-rustlib** — Shared library for config cascade, logging, metrics, secrets, tiered-sink
- **AWS APIs** — CloudTrail, GuardDuty, SecurityHub, Config
- **Azure APIs** — Activity Log, Defender, Sentinel, Entra ID (Microsoft Graph)
- **M365 APIs** — Office 365 Management Activity API, Microsoft Graph
- **GCP APIs** — Cloud Audit Logs, Security Command Center, Cloud Logging
- **Kafka** — Output delivery to DFE pipeline

---

## Module Layout

```
src/
├── buffer/           # Memory pressure, TieredSink re-exports
├── config/           # 7-layer config cascade, all config structs
├── error.rs          # Centralised error types
├── extractor/        # External extractors
│   ├── container/    # Docker/podman container management
│   ├── plugin/       # Dynamic .so plugin loading
│   └── vector/       # Vector.dev gRPC integration
├── lib.rs            # Public module exports
├── main.rs           # CLI entry point
├── metrics/          # Prometheus metrics
├── pipeline/         # Orchestration, enrichment, Kafka delivery
├── scheduler/        # Fetch timing, jitter, concurrency control
├── sink/             # Sink trait + Kafka implementation
│   └── kafka/        # Kafka producer
└── source/           # Native source trait + providers
    ├── aws/          # AWS (CloudTrail, GuardDuty, SecurityHub)
    ├── azure/        # Azure (Activity Log, Defender, Sentinel, Entra ID)
    ├── gcp/          # GCP (Audit Logs, SCC, Cloud Logging)
    └── m365/         # M365 (Audit Log, Message Trace, DLP, Alerts)
```

---

## Resources

**Documentation:**

- [docs/DESIGN.md](docs/DESIGN.md) — Architecture with mermaid diagrams
- [config.example.yaml](config.example.yaml) — Configuration reference

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
