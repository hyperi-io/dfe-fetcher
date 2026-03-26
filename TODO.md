# TODO - dfe-fetcher

This is the **single source of truth** for all tasks and progress.

---

## Active Tasks

### Metrics Standard Migration (DFE-METRICS-MIGRATION-FETCHER.md)

- [x] Bump rustlib to >=1.18.0
- [x] Fix MetricsManager namespace from `""` to `"dfe_fetcher"`
- [x] Rename all fetcher metrics to `dfe_fetcher_*` prefix (namespace collision fixed)
- [x] Fix counters missing `_total` suffix
- [x] Merge fetches_success/error into `dfe_fetcher_fetches_total{source, status}` labels
- [x] Add `dfe_fetcher_fetch_duration_seconds` histogram (per-source)
- [x] Add `dfe_fetcher_cursor_age_seconds` gauge (data staleness SLO)
- [x] Add `dfe_fetcher_api_errors_total` with `source` + `code` labels
- [x] Add `dfe_fetcher_ingest_requests_total` + `dfe_fetcher_ingest_duration_seconds`
- [x] Add `dfe_fetcher_extractor_runs_total` with `name` + `status` labels
- [x] Wire all DfeMetrics methods (records, transport, pipeline, scaling, auth)
- [ ] **Adopt metrics-dfe groups** (AppMetrics, SinkMetrics, BackpressureMetrics) — BLOCKED: `metrics-dfe` feature not yet published on crates.io. Adopt when rustlib ships it.
- [ ] **Configure histogram buckets** — Using defaults. Will configure tuned buckets when metrics-dfe groups land.

### Remaining rustlib Migration

- [x] **Wire MemoryGuard** — Integrated via rustlib `memory` feature (cgroup-aware)
- [x] **Wire ScalingPressure** — Integrated via rustlib `scaling` feature with KEDA endpoint
- [x] **Replace RateWindow** — Using rustlib `scaling::RateWindow`
- [x] **Kafka integration tests** — Verified zero direct `rdkafka` usage, all via rustlib transport
- [x] **Deprecate legacy KafkaConfig** — Runtime warning logged at startup. `output.topic_suffix` added as forward migration path. Doc comment marks structs deprecated. Target removal in next major.

### Upcoming

- [ ] **Documentation review** — run full doco review skill against codebase, fix stale content
- [ ] **Rebuild CI with updated hyperi-ci** — hyperi-ci has significant updates (prod/test change separation). Re-run full CI pipeline, verify test/build/release workflow still works end-to-end.
- [ ] **Wire FetchWindow into sources** — all 4 sources currently ignore the `_window` parameter. Each source's time-window logic needs updating to use `window.start`/`window.end` instead of hardcoded lookbacks. Cursors don't actually work end-to-end until this is done.
- [ ] **Adopt metrics-dfe groups** — BLOCKED: `metrics-dfe` feature not yet published. Adopt when rustlib ships AppMetrics/SinkMetrics/BackpressureMetrics.
- [ ] **Deprecate legacy KafkaConfig** — target removal in next major version
- [x] **M365 test credentials** — App registration `dfe-fetcher-m365-test` (cb1e86cc) created in HyperSec tenant with ActivityFeed.Read, SecurityAlert.Read.All, Reports.Read.All. Smoke tests passing.
- [ ] **Add M365 to Terraform** — `infra/test/main.tf` has AWS/Azure/GCP but M365 app was created manually. Add azuread_application resource for reproducibility.
- [ ] **Cloud admin setup guides** — Create `docs/{aws,azure,m365,gcp}.md` describing what a cloud admin needs to configure for each service, with links to the Terraform files in `infra/test/`. Covers IAM roles, app registrations, API permissions, service accounts.
- [ ] **Save M365 creds to OpenBao** — `secret/dfe-fetcher/m365` (blocked: need fresh bao token)

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

9. [x] **Container extractor integration test** — Docker-based test with `alpine` container producing JSON to stdout. 3 tests in `tests/e2e/container.rs`.

10. [x] **Kafka integration test** — Dual-mode (Docker/remote) tests in `tests/e2e/kafka.rs`. 5 tests covering cursor roundtrip, transport send, produce-consume, enrichment verification, and cursor-driven FetchWindow.

11. [x] **Benchmarks** — Pipeline enrichment throughput benchmark in `benches/pipeline.rs`.

12. [x] **Test restructuring** — Standard layout: `tests/integration/`, `tests/e2e/`, `tests/smoke.rs`, `tests/common/`. Consolidated source tests into single binary.

13. [x] **Startup smoke test** — `tests/smoke.rs` validates `Config::default()`, `Metrics::new()`, `PipelineState::new()`, enrichment, and `DeploymentContract` don't panic.

14. [x] **Backpressure/hot-reload/backoff tests** — Scheduler stall test, hot-reload interval test, container restart backoff unit tests, deployment contract/topic suffix/cursor selection tests.

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
cargo test           — 141 tests passing (run `cargo test` for count)
cargo test --test smoke -- --ignored                 — startup smoke test
cargo test --test e2e -- --ignored                   — 5 Kafka e2e + 3 container tests (Docker)
cargo test --test integration -- --ignored           — 8 smoke_cloud tests (live cloud APIs)
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
