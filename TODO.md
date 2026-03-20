# TODO - dfe-fetcher

This is the **single source of truth** for all tasks and progress.

---

## Active Tasks

### rustlib Migration (Eliminate Bespoke Code)

Audit performed against rustlib v1.16.6. These items replace hand-rolled code with rustlib equivalents.

- [ ] **Migrate metrics to MetricsManager** — Replace 450-line hand-rolled `src/metrics/mod.rs` (AtomicU64 + render()) with rustlib `MetricsManager`. Keep `DfeMetrics` dual-emit (already wired). The hand-rolled `/metrics` renderer can be replaced by `MetricsManager::start_server()` Prometheus endpoint.
- [ ] **Migrate HTTP server to rustlib HttpServer** — Replace bespoke axum metrics+health server in `src/main.rs` with rustlib `HttpServer` (built-in `/health/live`, `/health/ready`, `/metrics`). Keep ingest server separate (domain-specific auth).
- [ ] **Wire MemoryGuard** — BufferManager was deleted but `MemoryGuard` from rustlib `memory` feature is not yet integrated. Wire `MemoryGuard::new(config)` into pipeline for cgroup-aware memory pressure detection. Remove any remaining manual memory tracking.
- [ ] **Wire ScalingPressure** — Config `scaling: ScalingPressureConfig` exists but calculator not instantiated. Wire `ScalingPressure::new(&config)` and expose pressure score via `/scaling/pressure` endpoint for KEDA.
- [ ] **Replace RateWindow** — Hand-rolled `RateWindow` in metrics (65 lines) duplicates `scaling::rate_window::RateWindow` in rustlib. Replace.
- [ ] **Deprecate legacy KafkaConfig** — Bespoke `KafkaConfig`/`SaslConfig`/`KafkaTlsConfig`/`ProducerConfig` (120 lines) in config.rs exists only for backward compat with legacy `kafka:` config key. Add migration path: log deprecation, document `output.kafka:` (rustlib `KafkaConfig`) as the replacement. Target removal in next major.
- [ ] **Kafka integration tests** — Existing tests use `KafkaTransport` correctly via `output.rs`. Verify no remaining direct `rdkafka` usage anywhere (should be zero — all via rustlib transport).

---

## Remaining Work

### Production Blockers

1. [x] **AWS SigV4 signing** — Implemented via `reqsign` 0.16 crate with explicit body SHA256 hashing (`sha2`+`hex`). Verified against live CloudTrail API.

2. [x] **GCP JWT signing** — Implemented via `jsonwebtoken` 10 crate (with `aws_lc_rs` feature) with RS256 signing. Verified against live Cloud Logging API.

3. [x] **DLQ support** — Implemented using `hyperi-rustlib` 1.10.0 `Dlq::file_only()`. Failed Kafka sends route to DLQ with metric tracking.

4. [x] **Cloud test infrastructure** — Terraform in `infra/test/` provisions IAM user (AWS), app registration (Azure), service account (GCP). Smoke tests in `tests/smoke_cloud.rs` verify live API calls.

### Hardening

4. [x] **Ingest endpoint authentication** — Bearer token validation via `auth_token` config field. Credential resolution supports vault/env/literal.

5. [x] **Container restart-on-crash** — Exponential backoff restart with configurable `max_restart_attempts`, `max_restart_backoff_secs`, and `stable_after_secs`.

6. [x] **Incremental fetching / cursor state** — Cursor store with file and Kafka backends. Auto-selects backend based on output config. Sources resume from last successful fetch window.

7. [x] **Per-message filtering** — CEL expression filtering via rustlib `expression` feature. Configurable per-source `filter:` field evaluated before output delivery.

8. [x] **`_timestamp_received` enrichment** — All fetched data sets `_timestamp_received` to fetch time (aligns with `common-header/timeseries.yaml` schema).

### Testing

8. [x] **Wiremock source tests** — 31 wiremock tests across all 4 sources (AWS 6, Azure 8, GCP 7, M365 10) covering fetch success, pagination, empty responses, error handling, and health checks. URL override fields added to config structs.

9. [x] **Container extractor integration test** — Docker-based test with `alpine` container producing JSON to stdout. 3 tests in `tests/container_integration.rs`.

10. [x] **Kafka integration test** — Dual-mode (Docker/remote) tests in `tests/kafka_integration.rs` and `tests/e2e_kafka.rs`. 5 tests covering cursor roundtrip, transport send, produce-consume, enrichment verification, and cursor-driven FetchWindow.

11. [x] **Benchmarks** — Pipeline enrichment throughput benchmark in `benches/pipeline.rs`.

### Nice-to-Have

12. [x] **Helm chart** — Generated from `DeploymentContract` via `dfe-fetcher emit-chart <dir>`. Includes Deployment, Service, ConfigMap, Secret, HPA, KEDA, ServiceAccount.
13. [x] **Config --validate flag** — Available as `dfe-fetcher config-check` subcommand (rustlib CLI module).
14. [x] **Adopt rustlib `top` feature** — Live TUI metrics dashboard via `dfe-fetcher top` subcommand.

---

## Completed

### Phase 1: Bug Fixes and Hardening

- [x] Fix buffer underflow in `BufferManager::remove_bytes` — `fetch_update` with `saturating_sub`
- [x] Fix jitter to use actual randomness — replaced `SystemTime` modulo with `fastrand::u64`
- [x] Fix metrics gauge underflow — `fetch_update` with `saturating_sub` in `dec_*` methods
- [x] Strengthen config validation — unique container names, non-empty topics, valid addresses
- [x] Add SIGTERM handler alongside SIGINT for container environments
- [x] Fix `_reloader_handle` drop — persist for application lifetime

### Phase 2: Credential Resolution Framework

- [x] Create `src/credential.rs` — resolve `vault:path:key`, `env:VAR`, literal strings
- [x] Create shared HTTP client factory — timeout, user-agent, connection pool defaults
- [x] Add OAuth2 token manager — `TokenManager` with client_credentials grant and 60s-before-expiry caching

### Phase 3: Native Source Implementations

#### 3a. M365 Source

- [x] OAuth2 auth (Management API scope + Graph scope)
- [x] `fetch_audit_log` — Office 365 Management Activity API with subscription start on 404
- [x] `fetch_message_trace` — Graph API email activity reports
- [x] `fetch_dlp` — Graph Security API filtered by DataLossPrevention
- [x] `fetch_alerts` — Graph Security Alerts v2

#### 3b. Azure Source

- [x] OAuth2 auth (management + graph scopes)
- [x] `fetch_activity_log` — Azure Monitor API with time filter
- [x] `fetch_defender` — Defender for Cloud security alerts
- [x] `fetch_sentinel` — SecurityInsights incidents (resource_group/workspace from config)
- [x] `fetch_entra_id` — Graph auditLogs (signIns + directoryAudits)

#### 3c. AWS Source

- [x] AWS credential loading — static keys, vault JSON, env prefix
- [x] `fetch_cloudtrail` — LookupEvents with time window (SigV4 placeholder)
- [x] `fetch_guardduty` — ListDetectors -> ListFindings -> GetFindings chain
- [x] `fetch_securityhub` — GetFindings with WorkflowStatus filter
- [x] `fetch_config` — SelectAggregateResourceConfig
- [x] `fetch_cloudwatch_logs` — FilterLogEvents with log group filtering and pagination
- [x] `fetch_cloudwatch_metrics` — ListMetrics discovery + GetMetricData with namespace/metric filters
- [x] CloudWatch Metrics OTLP output — optional `output_format: "otlp"` produces `ExportMetricsServiceRequest` protobuf (HyperDX compatible). UCUM unit mapping, Gauge data points with cloud resource attributes.

#### 3d. GCP Source

- [x] GCP auth — service account JWT flow + metadata server fallback (RSA signing placeholder)
- [x] `fetch_audit_logs` — Cloud Logging entries.list with audit filter
- [x] `fetch_scc` — Security Command Center findings (requires organization_id)
- [x] `fetch_cloud_logging` — entries.list with custom filter from config

### Phase 4: Plugin System (Removed)

Plugin system was removed. Three extraction modes remain: native, container, and vector.

### Phase 5: Container Extractor Hardening

- [x] Add container image pull — configurable pull policy (always/if-not-present/never)
- [x] Add container log capture — stderr to tracing via `spawn_stderr_logger`
- [x] Add container timeout — kill after `timeout_secs` for scheduled mode

### Phase 6: Deployment and CI

- [x] Create multi-stage Dockerfile (rust builder + debian-slim runtime)
- [x] Create README.md
- [x] Add LICENSE, CONTRIBUTING.md, SECURITY.md, COMMERCIAL.md
- [x] Create VERSION file (0.1.0)
- [x] Review `.github/workflows/` — ci.yml, publish.yml, semantic-release.yml present and correct
- [x] Add `DeploymentContract` (`src/deployment.rs`) — drives Helm chart, Dockerfile, Docker Compose generation via rustlib
- [x] Adopt rustlib CLI module — `DfeApp` trait, `CommonArgs`, `StandardCommand`, `VersionInfo`
- [x] CLI subcommands: `run` (default), `version`, `config-check`, `emit-dockerfile`, `emit-chart`, `emit-compose`, `emit-contract`

### Phase 7: Testing Infrastructure

- [x] Create `tests/integration.rs` — 19 integration tests (config, enrichment, metrics, credentials, buffer)
- [x] Create source test files — `tests/source_{aws,azure,m365,gcp}.rs` (9 tests: disabled, health check, missing credentials)
- [x] Pipeline enrichment edge-case tests — empty object, nested JSON, non-JSON, large payload

### Earlier Work

- [x] Fix compile blockers (TieredSink, rdkafka 0.39, FutureRecord type, Producer import)
- [x] Build HTTP ingest server (`src/ingest/mod.rs`)
- [x] Make container extractors functional (scheduled + continuous modes)
- [x] Add Vector.dev gRPC receiver using rustlib transport
- [x] Wire ingest/container/vector into main.rs
- [x] Write detailed WBS plan

---

## Build Status

```
cargo fmt --check    — verify before push
cargo clippy -D warn — verify before push
cargo test           — 140 tests (70 unit + 21 integration + 49 source)
cargo test --test smoke_cloud -- --ignored          — 8 smoke tests (live cloud APIs)
cargo test --test container_integration -- --ignored — 3 container tests (Docker)
cargo test --test kafka_integration -- --ignored     — 2 Kafka tests (Docker or remote)
cargo test --test e2e_kafka -- --ignored             — 3 e2e tests (Docker or remote)
```

---

## Notes for AI Assistants

This file is the **single source of truth** for tasks and progress.

**Rules:**

- All tasks go here, nowhere else
- Mark tasks `[x]` when complete, move to Completed section
- Never add tasks to STATE.md or CLAUDE.md

**Status tags:**

- `[ ]` - Not started
- `[BLOCKED]` - Waiting on something (note what)
- `[x]` - Completed
