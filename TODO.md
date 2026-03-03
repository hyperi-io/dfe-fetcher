# TODO - dfe-fetcher

This is the **single source of truth** for all tasks and progress.

---

## Active Tasks

_None currently — MVP complete. See Remaining Work for next steps._

---

## Remaining Work

### Production Blockers

1. [x] **AWS SigV4 signing** — Implemented via `reqsign` 0.16 crate with explicit body SHA256 hashing (`sha2`+`hex`). Verified against live CloudTrail API.

2. [x] **GCP JWT signing** — Implemented via `jsonwebtoken` 10 crate with RS256 signing. Pending live verification (GCP auth needs refresh).

3. [x] **DLQ support** — Implemented using `hyperi-rustlib` 1.10.0 `Dlq::file_only()`. Failed Kafka sends route to DLQ with metric tracking.

4. [x] **Cloud test infrastructure** — Terraform in `infra/test/` provisions IAM user (AWS), app registration (Azure), service account (GCP). Smoke tests in `tests/smoke_cloud.rs` verify live API calls.

### Hardening

4. [ ] **Ingest endpoint authentication** — `/ingest/:source` HTTP endpoint has no auth. Add bearer token or shared secret validation to prevent unauthorised POST.

5. [ ] **Container restart-on-crash** — Continuous containers don't auto-restart with backoff if they exit unexpectedly. Add exponential backoff restart logic in `run_continuous`.

6. [ ] **Incremental fetching / cursor state** — All sources fetch a fixed time window (1-24 hours) on every run. No state tracking between runs, so records are duplicated. Add cursor/checkpoint persistence (file or Kafka offset).

7. [ ] **Plugin .so loading** — Plugin registry exists as a stub. Actual `libloading` calls require `unsafe` code. Either move plugin loader to a separate crate without `#![forbid(unsafe_code)]` or change to `#![deny(unsafe_code)]` with targeted `#[allow]`.

### Testing

8. [ ] **Wiremock source tests** — Current source tests only cover disabled/missing-credential paths. Add wiremock-based tests that mock actual API responses, pagination, error handling, and token refresh.

9. [ ] **Container extractor integration test** — Test with a simple `echo` container that outputs JSON to stdout.

10. [ ] **Kafka sink integration test** — Use testcontainers or mock Kafka to test produce/flush cycle.

11. [ ] **Benchmarks** — Enrichment throughput, Kafka produce rate, buffer pressure. Place in `benches/`.

### Nice-to-Have

12. [ ] **Helm chart** — K8s deployment template for dfe-fetcher.
13. [ ] **Config --validate flag** — Parse and validate config without starting the service.

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

#### 3d. GCP Source

- [x] GCP auth — service account JWT flow + metadata server fallback (RSA signing placeholder)
- [x] `fetch_audit_logs` — Cloud Logging entries.list with audit filter
- [x] `fetch_scc` — Security Command Center findings (requires organization_id)
- [x] `fetch_cloud_logging` — entries.list with custom filter from config

### Phase 4: Plugin System

- [x] Add `libloading` dependency
- [x] Define plugin C ABI types and PluginRegistry structure
- [x] Implement `load_from_config` — directory scanning + explicit entries (stub, no unsafe loading)

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
cargo fmt --check    ✅ Clean
cargo clippy -D warn ✅ Clean
cargo test           ✅ 71/71 passing (43 unit + 19 integration + 9 source)
cargo test --test smoke_cloud -- --ignored  ✅ 6/6 (AWS + Azure live, GCP pending auth)
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
