<!-- Project:   dfe-fetcher                          -->
<!-- File:      docs/DESIGN.md                        -->
<!-- Purpose:   Architecture, data flow, and design rationale -->
<!-- Language:  Markdown                               -->
<!--                                                   -->
<!-- License:   BUSL-1.1                               -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED            -->

# dfe-fetcher Design Document

## Overview

dfe-fetcher is a data extraction service that pulls security and operational data from external cloud services and delivers it to the DFE pipeline via Kafka.

## High-Level Architecture

```mermaid
graph TB
    subgraph External Services
        AWS[AWS<br>CloudTrail, GuardDuty,<br>SecurityHub]
        Azure[Azure<br>Activity Log, Defender,<br>Sentinel, Entra ID]
        M365[Microsoft 365<br>Audit Log, Message Trace,<br>DLP, Alerts]
        GCP[GCP<br>Audit Logs, SCC,<br>Cloud Logging]
        Other[Other Services<br>CloudWatch, Okta,<br>CrowdStrike, etc.]
    end

    subgraph dfe-fetcher
        subgraph "Native Sources (Rust)"
            NS_AWS[AWS Source]
            NS_Azure[Azure Source]
            NS_M365[M365 Source]
            NS_GCP[GCP Source]
        end

        subgraph "Container Extractors"
            CE1[Container 1<br>e.g. YACE]
            CE2[Container 2<br>e.g. custom tool]
            CE3[Container N<br>...]
        end

        subgraph "Vector Extractors"
            VE1[Vector Instance 1]
            VE2[Vector Instance N]
        end

        Scheduler[Scheduler<br>Timing + Concurrency]
        CursorStore[Cursor Store<br>File JSON]
        Pipeline[Pipeline<br>Enrich + Filter + Route]
        Ingest[Ingest Server<br>HTTP :8080]
        GRPC[gRPC Receiver<br>Vector Protocol :6000]
        Output[Output Transport<br>Kafka / gRPC / Both]
        Metrics[Metrics Server<br>Prometheus :9090]
    end

    subgraph DFE Pipeline
        Kafka[Kafka Topics<br>*_land]
        Receiver[dfe-receiver]
        Loader[dfe-loader]
    end

    AWS --> NS_AWS
    Azure --> NS_Azure
    M365 --> NS_M365
    GCP --> NS_GCP
    Other --> CE1
    Other --> CE2

    NS_AWS --> Scheduler
    NS_Azure --> Scheduler
    NS_M365 --> Scheduler
    NS_GCP --> Scheduler

    Scheduler --> CursorStore
    CursorStore --> Pipeline
    CE1 -->|stdout / HTTP| Ingest
    CE2 -->|stdout / HTTP| Ingest
    CE3 -->|stdout / HTTP| Ingest
    VE1 -->|gRPC| GRPC
    VE2 -->|gRPC| GRPC
    Ingest --> Pipeline
    GRPC --> Pipeline

    Pipeline --> Output
    Output --> Kafka
    Output -->|gRPC| Receiver
    Kafka --> Loader
```

## Extraction Modes

```mermaid
graph LR
    subgraph "Mode 1: Native Rust"
        A1[Rust Crate Available?] -->|Yes| A2[Build in src/source/]
        A2 --> A3[Direct API Calls]
        A3 --> A4[FetchResult]
    end

    subgraph "Mode 2: Container"
        C1[OSS Tool Exists?] -->|Yes| C2[Wrap in Container]
        C2 --> C3[Docker/Podman Run]
        C3 --> C4[stdout JSON lines<br>OR HTTP POST]
        C4 --> C5[IngestMessage]
    end

    subgraph "Mode 3: Vector.dev"
        D1[Vector Config] --> D2[Container or Sidecar]
        D2 --> D3[gRPC Vector Protocol]
        D3 --> D4[IngestMessage]
    end
```

## Data Flow

```mermaid
sequenceDiagram
    participant S as Source/Extractor
    participant Sch as Scheduler
    participant C as Cursor Store
    participant P as Pipeline
    participant O as Output Transport
    participant T as Kafka / gRPC

    Note over Sch: Timer tick (interval + jitter)
    Sch->>C: Load cursor (last fetch window)
    Sch->>Sch: Acquire concurrency permit
    Sch->>S: fetch(from_cursor)
    S->>S: Authenticate with service
    S->>S: API call (with pagination)
    S-->>Sch: Vec<FetchResult>
    Sch->>P: deliver(results)
    P->>P: Enrich: add _timestamp_fetcher, _timestamp_received, _source (topic base), _source_fetcher
    P->>P: CEL filter: evaluate per-source filter expression
    P->>O: send(topic + suffix, payload)
    O->>T: Produce message (Kafka and/or gRPC)
    Sch->>C: Save cursor (new fetch window)
    Note over T: topic = "{source}{topic_suffix}"<br>e.g. "aws_land"
```

Those four enrichment names are reserved. A payload that already carries one
keeps its own value under `<key>_original` and the fetcher's value takes the
name: the loader routes on `_source`, so the DFE source name has to be the one
that survives, and a record carrying the same top-level key twice is rejected
outright by ClickHouse rather than dead-lettered.

`<key>_original` is never overwritten. A payload that already carries both the
reserved name and its `_original` -- a replayed record on a second enrich pass,
say -- keeps the `_original` it arrived with, and the colliding value is parked
under the next free `<key>_original_<n>` counting from 2, with a warning naming
the key.

## Container Extractor Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Configured: Config loaded
    Configured --> Starting: start()
    Starting --> Running: Container started
    Running --> Running: Health check OK
    Running --> Unhealthy: Health check fail
    Unhealthy --> Running: Recovery
    Unhealthy --> Stopped: Max retries
    Running --> Stopping: shutdown signal
    Stopping --> Stopped: Container stopped
    Stopped --> [*]

    state Running {
        [*] --> Scheduled: mode=scheduled
        [*] --> Continuous: mode=continuous
        Scheduled --> Waiting: Run complete
        Waiting --> Scheduled: Timer tick
        Continuous --> Continuous: Streaming output
    }
```

## Container Communication

```mermaid
graph TB
    subgraph "stdout Mode"
        C1[Container] -->|JSON lines on stdout| F1[Fetcher reads child stdout]
        F1 --> P1[Parse line as JSON]
        P1 --> D1[Deliver to Pipeline]
    end

    subgraph "HTTP Mode"
        C2[Container] -->|POST /ingest/source| F2[Ingest HTTP Server :8080]
        F2 --> P2[Parse JSON body]
        P2 --> D2[Deliver to Pipeline]
    end

    subgraph "gRPC Mode (Vector)"
        C3[Vector Container] -->|Vector gRPC protocol| F3[gRPC Server :6000]
        F3 --> P3[Convert to JSON]
        P3 --> D3[Deliver to Pipeline]
    end
```

## Configuration Cascade

```mermaid
graph TB
    CLI[1. CLI Arguments] --> Merge
    ENV[2. Environment Variables<br>DFE_FETCHER_*] --> Merge
    DOT[3. .env File] --> Merge
    CFG[4. Config File<br>config.yaml] --> Merge
    DEF[5. Hard-coded Defaults] --> Merge
    Merge[Merged Config] --> Validate
    Validate --> SharedConfig
    SharedConfig -->|SIGHUP| Reload[Hot Reload]
    Reload --> Merge
```

## Multi-Tenant Deployment

```mermaid
graph TB
    subgraph "Per-Org Deployment"
        F1[dfe-fetcher<br>config-org-1.yaml]
        F2[dfe-fetcher<br>config-org-2.yaml]
        FN[dfe-fetcher<br>config-org-N.yaml]
    end

    subgraph "Each Instance"
        S1[M365 Source<br>tenant: org-1]
        S2[Container Extractors<br>per org config]
    end

    F1 --> S1
    F1 --> S2

    subgraph "Shared Kafka"
        K[Kafka Cluster]
    end

    S1 --> K
    S2 --> K

    Note1[One container per source + config<br>No horizontal scaling needed<br>Deploy N instances for N orgs]
```

## Module Dependencies

```mermaid
graph TD
    main[main.rs] --> config
    main --> metrics
    main --> pipeline
    main --> scheduler
    main --> source
    main --> extractor
    main --> ingest
    main --> output
    main --> cursor

    pipeline --> config
    pipeline --> metrics
    pipeline --> output
    pipeline --> buffer
    pipeline --> source
    pipeline --> error

    scheduler --> config
    scheduler --> metrics
    scheduler --> source
    scheduler --> cursor

    source --> config
    source --> credential
    source --> error

    extractor --> config
    extractor --> error
    extractor --> pipeline

    ingest[ingest<br>HTTP server] --> pipeline

    output[output.rs<br>Kafka / gRPC / Both] --> config
    output --> error

    cursor[cursor/<br>File JSON] --> config
    cursor --> error

    credential[credential.rs<br>vault/env/literal<br>OAuth2 TokenManager] --> error

    buffer --> config

    config --> error

    subgraph "scalo"
        rustlib_config[config]
        rustlib_logger[logger]
        rustlib_metrics[metrics]
        rustlib_tiered[tiered-sink]
        rustlib_secrets[secrets]
        rustlib_transport[Transport trait<br>Kafka + gRPC]
        rustlib_expression[expression<br>CEL filtering]
    end

    config --> rustlib_config
    main --> rustlib_logger
    metrics --> rustlib_metrics
    buffer --> rustlib_tiered
    credential --> rustlib_secrets
    output --> rustlib_transport
    extractor --> rustlib_transport
    pipeline --> rustlib_expression
```

## Decision Framework: Native vs Container

| Criteria | Native (Rust) | Container |
|----------|--------------|-----------|
| Good Rust crate exists | Yes | - |
| Great OSS tool in another language | - | Yes |
| Need tight integration | Yes | - |
| Need isolation | - | Yes |
| Performance critical | Yes | - |
| Rapid prototyping | - | Yes |
| Third-party maintained | - | Yes |

### Examples

| Source | Mode | Reason |
|--------|------|--------|
| AWS CloudTrail | Native | Direct REST, signed with `reqsign` (SigV4) |
| Azure Activity Log | Native | REST API, `reqwest` sufficient |
| M365 Audit Log | Native | Microsoft Graph REST, OAuth2 |
| GCP Audit Logs | Native | Direct REST, service-account JWT |
| Prometheus exporters | Container | Mature exporter (e.g. a Go tool) run as a sidecar |
| Syslog collection | Vector | Vector has a native syslog source |

### Sidecar Patterns

For log sources that established tools handle well, use container extractors
with Filebeat, Fluentd, or similar collection agents:

| Source Type | Sidecar Tool | Communication |
|-------------|-------------|---------------|
| Syslog | Filebeat (system module) | HTTP POST to `/ingest/syslog` |
| Windows Event Logs | Filebeat (winlogbeat) | HTTP POST to `/ingest/windows_events` |
| Custom app logs | Filebeat (file input) | stdout JSON lines |
| Network captures | Zeek / Suricata | stdout JSON lines |
| Cloud provider logs | Cloud-specific CLI tools | stdout JSON lines |

Each sidecar runs as a container extractor with `mode: continuous` and
communicates via stdout (JSON lines) or HTTP POST to the ingest endpoint.
The fetcher manages the container lifecycle including restart-on-crash with
exponential backoff.

## Performance Sensitivity

> **dfe-fetcher is less hot-path-sensitive than the rest of the DFE stack.**

Typical fetcher volumes are orders of magnitude lower than the loader /
receiver / transform / archiver tier. A single fetcher pod servicing one
tenant's combined AWS / Azure / M365 / GCP / SaaS audit feeds normally
moves tens to a few thousand records per minute. The downstream pipeline
moves PB/hour. The fetcher is also I/O-bound waiting on remote cloud
APIs, not CPU-bound parsing or routing.

What this means in practice:

| Concern | Rest of DFE stack | dfe-fetcher |
|---|---|---|
| Per-record allocation | Zero-allocation hot path; pre-allocated pools, arenas | Allocate freely - `Vec`, `String`, `serde_json::Value` are fine |
| SIMD JSON parse | `sonic-rs` mandatory on hot paths | `serde_json` is sufficient |
| Cloning / `Bytes` reuse | Aggressive `Bytes` reuse, `Cow`, arena lifetimes | `Bytes::from(serde_json::to_vec(&doc)?)` per record is fine |
| Channel sizing | Backpressure-tuned bounded channels per stage | Default Tokio channels; per-source `for` loops are fine |
| `regex` on hot path | Forbidden - use `memchr` / `memmem::Finder` | Acceptable if needed; volumes don't justify rewrite |
| `async fn` granularity | Watch `.await` budget, yield_now hints | `await` per HTTP request is the unit of work |
| Per-source concurrency | Lock-free, sharded, ArcSwap | `Arc<Mutex<>>` for an OAuth token cache is acceptable |

We still care about correctness, cancellation safety, bounded retries,
and not leaking tasks - those are correctness concerns, not perf
concerns. We also still prefer the simple, idiomatic version of any
pattern over the clever one. But the aggressive hot-path discipline
documented in the hyperi-ai Rust standards and applied across
dfe-loader / dfe-receiver / dfe-archiver does **not** need to be
applied symmetrically here.

This trade-off is deliberate. It keeps the per-source code
straightforward (one async function per HTTP endpoint, `Vec` of
records out), keeps the cognitive load low for new sources, and
reserves the harder optimisation work for the pipeline tiers where
volumes actually demand it. When a fetcher source ever becomes a
bottleneck (sustained high-cardinality tenants, very chatty audit
feeds), revisit on a case-by-case basis - the scalo hot-path
patterns are available if needed, just not the default starting
point here.
