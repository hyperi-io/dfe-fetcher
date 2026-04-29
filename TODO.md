# TODO - dfe-fetcher

This is the **single source of truth** for all tasks and progress.

---

## Active Tasks

### Dependency Refresh — 2026-04-29

`/deps` Phase 1 run on 2026-04-29. Lockfile bumped, all clippy warnings
cleared, **405 tests passing** (311 unit + 91 integration + 3 smoke).

- [x] `cargo update` — security-relevant lockfile bumps applied:
      `rustls 0.23.40`, `rustls-webpki 0.103.13`, `openssl 0.10.78`,
      `rand 0.8.6`, `tokio 1.52.1`, `metrics 0.24.4`, plus 30+ others
- [x] Clippy `duration_suboptimal_units` (Rust 1.95 lint) — three call
      sites in `src/credential.rs` and `src/metrics/mod.rs` migrated
      from `Duration::from_secs(60)` → `Duration::from_mins(1)`
- [x] `reqwest` direct dep set to `default-features = false` with
      explicit `charset`/`http2`/`system-proxy` to drop our contribution
      to `default-tls`. Net openssl chain remains because hyperi-rustlib
      itself enables reqwest defaults — see upstream item below.
- [x] `hyperi-rustlib >=2.5.4` confirmed as latest stable on crates.io

Deferred bumps (require code review, **not safe for cargo update**):
- [ ] `reqwest 0.12 → 0.13` — blocked by `hyperi-rustlib` pin
      (`>=0.12, <0.13` until vaultrs and opentelemetry-otlp support 0.13).
      Track upstream and coordinate with rustlib bump.
- [ ] `reqsign 0.16 → 0.20` — major API change (signing surface
      reorganised). Migrate when AWS SigV4 callsites in
      `src/source/aws/` are touched next; verify against live
      CloudTrail / SecurityHub before shipping.

Upstream items (file against the right repo):
- [ ] `hyperi-rustlib` Cargo.toml: set `default-features = false` on
      its optional `reqwest` dep + add an explicit feature for
      `default-tls` (or just `rustls-tls`) so consumers can drop the
      `native-tls`/`openssl` chain entirely. As long as rustlib enables
      reqwest defaults, our local `default-features = false` is a no-op
      under feature unification. Worth a small PR upstream — we use
      rustls everywhere.

### Dependabot scope

State as of 2026-04-29 (after `cargo update`):

| # | Severity | Package | Status |
|---|---|---|---|
| 9-13, 15 | high | `openssl 0.10.77 → 0.10.78` | ✅ resolved by `cargo update` |
| 12 | low | `openssl` PEM oversized length | ✅ resolved by `cargo update` |
| 14 | low | `rand 0.8.5 → 0.8.6` | ✅ resolved by `cargo update` |
| 15 | high | `rustls-webpki 0.103.12 → 0.103.13` | ✅ resolved by `cargo update` |

All open Dependabot alerts now have a fix in `Cargo.lock`. Will be
formally closed when next push lands. Renovate: **0 open PRs** as of
2026-04-29.

Structural follow-up: the `openssl` chain is *only* present because
hyper-tls is pulled via reqwest defaults. Once the upstream rustlib
fix above lands, `cargo tree -i openssl` should return empty and these
alerts won't re-appear.

### New source: runzero asset inventory

[runzero API docs](https://help.runzero.com/docs/leveraging-the-api/)

- [ ] Add `src/source/runzero/` native source mirroring the existing
      AWS/Azure/M365/GCP shape (HTTP client + bearer auth + paginated
      fetch + cursor advance)
- [ ] Pull state dumps (full asset/inventory snapshots) **timestamped
      per record** so consumers can detect drift between fetches
- [ ] Cursor key per `instance_id + organization_id` — runzero is
      multi-tenant per account, mirror the M365 multi-tenant pattern
- [ ] Wire into `src/source/mod.rs` source registry + Helm chart
      contract + `config.example.yaml`
- [ ] Wiremock integration tests under `tests/integration/source_runzero.rs`
- [ ] Live smoke test under `tests/e2e/smoke_cloud.rs` (gated on
      `RUNZERO_TOKEN` env var; mark `#[ignore]`)

### Performance Review

Audit applicable optimisations from [dfe-loader/docs/PERFORMANCE.md](/projects/dfe-loader/docs/PERFORMANCE.md).

- [x] Allocator: `jemalloc` feature wired (mimalloc removed per
      2026-04-17 policy). Benchmarks deferred until Tier 2 PGO canary.
- [x] Build profile: `lto = "thin"` (CI overrides to `fat` at beta+),
      `codegen-units = 1`, `panic = "abort"`, `strip = true` confirmed
- [ ] Profile under load (perf, flamegraph, jeprof) — record baseline for regression detection
- [ ] PGO + BOLT: evaluate ROI for production binary (10-20% + 5-15% gain) — see Tier 2 section
- [ ] Batch tuning: validate buffer/flush thresholds align with rustlib Kafka transport (10K recv / 20K prefetch)

### Submodule Update + Code Review + Release

- [x] A. Single versioning migration — COMPLETE (branches: [main], workflow_dispatch with tag, .githooks/commit-msg, release branch deleted)
- [x] B. rustlib >= 2.0.0 migration — COMPLETE (Transport trait split, 84 tests passing, compiles clean)
- [ ] C1. Code review (/review skill)
- [ ] C2. Security review (/security-review skill)
- [ ] D1. Commit submodule update + any review fixes
- [ ] D2. Push via hyperi-ci push
- [ ] D3. Release via hyperi-ci release

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
- [ ] **Adopt metrics-dfe groups** (AppMetrics, SinkMetrics, BackpressureMetrics) — BLOCKED: `metrics-dfe` feature not yet published on crates.io
- [ ] **Configure histogram buckets** — Using defaults. Will configure tuned buckets when metrics-dfe groups land

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
- [x] **Add M365 to Terraform** — azuread_application + service_principal + password in `infra/test/main.tf`. Plan verified clean.
- [x] **Cloud admin setup guides** — `docs/cloud-setup/{aws,azure,m365,gcp}.md` with permissions, TF links, manual CLI, config examples.
- [ ] **Save M365 creds to OpenBao** — `secret/dfe-fetcher/m365` (blocked: need fresh bao token)
- [ ] **Re-auth AWS SSO + GCP** — `aws sso login` and `gcloud auth login` expired. Needed for full `terraform plan/apply`.

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
cargo test           — 179 tests passing (run `cargo nextest run` for count)
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

---

## Rust Release-Track Optimisation (hyperi-ci Tier 1/2)

**Context:** hyperi-ci is shipping channel-gated build optimisations for Rust
binaries (see `hyperi-ai/standards/languages/RUST.md` — *Release-Track Build
Optimisation*). Local `cargo build` is unaffected.

### Tier 1 prep (automatic at beta+/release once hyperi-ci ships)

Current state: **✅ READY — no source changes required.**

- [x] `Cargo.toml` has `[features] jemalloc` + `mimalloc` declared
- [x] `main.rs` wires `#[global_allocator]` under `#[cfg(feature = "jemalloc")]`
- [x] `default = []` — clean
- [x] `[profile.release] lto = "thin"` — CI overrides to `fat` on beta+

No action required. Next release-channel build picks up jemalloc + fat LTO
automatically once hyperi-ci ships the feature.

### Tier 2 opt-in (PGO + BOLT — release channel only) ✅ DONE 2026-04-29

- [x] **Workload script** — [scripts/pgo-workload.sh](scripts/pgo-workload.sh)
      orchestrates: start `pgo-driver` (mock cloud-API server) → start
      single-node Kafka (KRaft) → write ephemeral fetcher config with
      all 4 sources at 1s intervals + URL overrides → start fetcher →
      sleep `duration_secs` → cleanup. Shellcheck-clean. Floor 60s,
      default 300s. Linux only (BOLT requirement).
- [x] **`[[bin]] pgo-driver`** added to [Cargo.toml](Cargo.toml) with
      `required-features = ["pgo-driver"]`. Feature gate adds NO new
      transitive deps (axum, tokio, serde_json already present). Default
      builds skip the binary entirely.
- [x] **Mock cloud-API server** —
      [src/bin/pgo-driver.rs](src/bin/pgo-driver.rs). axum HTTP server
      listening on `127.0.0.1:19090` with realistic shapes for:
      - `POST /oauth/token` → OAuth2 client_credentials (Azure/M365/GCP)
      - `GET /azure/activity/*` → Activity Log (`value` + `@odata.nextLink`)
      - `GET /azure/graph/*` → Graph sign-ins
      - `GET /m365/management/*` → Audit Log
      - `GET /m365/graph/*` → security alerts
      - `POST /aws` → AWS JSON dispatch via `X-Amz-Target` header
        (CloudTrail Events, GuardDuty findings, Config items)
      - `POST /gcp/v2/entries:list` → Cloud Logging entries
      Default page size 500 records; pagination cycles every 3 pages so
      the `@odata.nextLink` follow-on path is also exercised.
- [x] **`.hyperi-ci.yaml`** has `build.rust.optimize.pgo.enabled: true`
      with `workload_cmd` + `duration_secs: 300`, plus
      `build.rust.optimize.bolt.enabled: true`.
- [x] **Hot path coverage** — fetcher polls 4 sources at 1s intervals
      against a 500-records/page mock. Net throughput at steady state:
      ~2000 records/sec through HTTP fetch → JSON parse → enrichment →
      CEL filter → Kafka produce → cursor advance. Substantially exceeds
      the loader v1.17.4 floor that produced negative PGO gains.

### Tier 2 next steps (require live CI run + canary)

- [ ] Trigger first release-channel publish to confirm Tier 2 actually
      runs end-to-end. Verify post-build:
      - Build log shows `channel=release, allocator=jemalloc, lto=fat,
        pgo=on, bolt=on`
      - `strings dfe-fetcher | grep -ciE 'jemalloc|je_mallctl'` non-zero
      - PGO profile artifacts uploaded
- [ ] Compare release vs Tier 1 baseline once both binaries on R2:
      `hyperfine` against the wiremock workload, record delta.
- [ ] Document binary-size delta in `docs/PERFORMANCE.md` (mirroring
      loader/receiver pattern — currently no such doc, create on first
      canary).

---

## POLICY UPDATE 2026-04-17 — Jemalloc at every channel, drop mimalloc ✅ DONE

**Allocator policy changed:** DFE binaries now standardise on jemalloc at
**every** channel. mimalloc is no longer a supported option. See
`hyperi-ai/standards/languages/RUST.md` → *Allocator Policy* and
`hyperi-ci/docs/RUST-RELEASE-TRACK-OPTIMISATION.md`.

### Action items (all done 2026-04-29)

- [x] Remove `mimalloc = ["dep:mimalloc"]` from `[features]` in `Cargo.toml`
- [x] Remove `mimalloc = { version = "0.1", optional = true }` from
      `[dependencies]`
- [x] Remove mimalloc `#[cfg]` fallback block from `src/main.rs`
- [x] `cargo check` clean with `--no-default-features --features jemalloc`

### Verification on next release

```bash
strings target/<target>/release/dfe-fetcher | grep -ciE 'jemalloc|je_mallctl'
```

### CI behaviour change

- Spike/alpha: was system allocator → now jemalloc. +10s compile, cached.
- Beta/release: unchanged.

---

## Lessons from dfe-receiver Tier 2 canary (2026-04-17)

**Context:** dfe-receiver was the first DFE binary to ship the full
hyperi-ci release-track build optimisation feature (Tier 1 jemalloc +
fat LTO on beta+; Tier 2 PGO + BOLT opt-in on release). Findings from
that work are now baked into the shared docs — this section is the
signal to apply the same pattern here.

### Canary findings

- **Binary size impact (jemalloc static link, stripped release)**:
  +491 KB (+3.5%) on a 14 MB baseline. mimalloc was +131 KB (+1.0%)
  but is no longer an allowed allocator per 2026-04-17 policy.
- **Micro-bench allocator delta**: jemalloc wins −7.2% on
  `json_validation/large`, −4.4% on small; within noise elsewhere.
  Micro-benchmarks understate the real production win — Kafka
  producer + async task allocations are where the gains materialise.
- **Detection on stripped binaries**: `nm` won't see symbols because
  release profile has `strip = true`. Use
  `strings <binary> | grep -ciE 'jemalloc|je_mallctl'` — should
  return > 0 on a jemalloc build.
- **PGO workload shape that actually works**: a Rust driver linked to
  the project's own lib (to reuse proto types) plus a bash orchestrator
  that spins up testcontainers dependencies, starts the instrumented
  binary, drives realistic multi-protocol traffic for ≥ 60s
  (300s default), and cleans up on EXIT. See dfe-receiver's
  `scripts/pgo-workload.sh` + `src/bin/pgo-driver.rs` for the
  reference implementation.
- **PGO workload anti-patterns confirmed**: single-request curls,
  `curl /healthz` loops, and port-probe scripts all produce negative
  PGO gains — the compiler mis-optimises startup paths over hot paths.

### Where to read

- hyperi-ci `docs/RUST-RELEASE-TRACK-OPTIMISATION.md` — opt-in guide,
  verification, troubleshooting
- hyperi-ci `docs/PGO-WORKLOAD-GUIDE.md` — four rules, anti-patterns,
  profile quality metrics
- hyperi-ci `templates/pgo-workload/` — five template shapes to copy
  (`http-server.sh`, `grpc-server.sh`, `kafka-producer.sh`,
  `kafka-consumer.sh`, `multi-protocol.sh`)
- dfe-receiver `docs/PERFORMANCE.md` — concrete binary-size numbers,
  bench deltas, reproduction commands
- hyperi-ai standards `rules/rust.md` — the channel matrix +
  jemalloc-only policy

### Applies to this project

(Each consumer project owns the per-project status below — update as
Tier 1 preconditions are met and when Tier 2 opt-in lands.)

- [ ] Tier 1 preconditions met (`jemalloc` feature declared in
      `Cargo.toml`, `#[global_allocator]` wired in `src/main.rs` under
      `#[cfg(feature = "jemalloc")]`, mimalloc removed)
- [ ] Workload script exists and passes local `cargo pgo build →
      workload → cargo pgo optimize` round-trip
- [ ] `.hyperi-ci.yaml` has `build.rust.optimize.pgo.enabled: true`
      with `workload_cmd` configured
- [ ] Next release-channel build verified: `strings <binary> | grep
      jemalloc` non-empty; build log shows cargo pgo invocations

---

## Lessons from dfe-loader Tier 2 canary (2026-04-23)

**Context:** dfe-loader was Canary 2 for hyperi-ci Tier 2. Released
v1.17.5 to R2 with full `channel=release, allocator=jemalloc, lto=fat,
pgo=on, bolt=on` on both archs after two real bugs surfaced and were
fixed mid-canary. These are infrastructure-level gotchas every DFE
Rust project needs to check before triggering its own canary — both
caused dfe-loader v1.17.4 to publish *successfully* but as the wrong
build type (spike-channel = Tier 1 only, no PGO/BOLT).

### Two CI-level gotchas every consumer project must verify

1. **`.github/workflows/ci.yml` `uses:` pin must be hyperi-ci ≥ v1.12.1
   (commit `ba03ff0` or newer).** Older pins (e.g. `1d4fb19d` = v1.8.0)
   predate the `HYPERCI_CHANNEL` resolver, so tagged dispatches resolve
   to `channel=spike` regardless of `.hyperi-ci.yaml` config — meaning
   PGO/BOLT never run even when `optimize.pgo.enabled: true` is set.

   Verify in this repo:
   ```bash
   grep "uses: hyperi-io/hyperi-ci" .github/workflows/ci.yml
   ```
   **This project currently pins
   `1d4fb19d5f16c4c46df84ed7a2f983170fa854b0` = hyperi-ci v1.8.0 —
   MUST bump to `ba03ff0da0dbc4c56f9b06ee3a65a2c5f418e092` (v1.12.1+)
   or `@main` before the canary or release will silently ship as
   spike-channel.**

2. **`ci.yml` `with: publish-target` must be `both` (not `internal`).**
   This workflow input *overrides* `publish.target` from
   `.hyperi-ci.yaml`. `internal` resolves to spike channel = Tier 1
   only. `both` = release channel = Tier 2 unlocked.

   This project: ✅ `publish-target: both` already set in
   [.github/workflows/ci.yml](.github/workflows/ci.yml).

### Workload-shape lesson for fetcher

dfe-loader's workload (Kafka producer driving messages into a running
loader against testcontainers Kafka + ClickHouse) is the closest
*structure* to copy, but the protocol is wrong for fetcher. Fetcher's
hot path is **HTTP fetch → JSON unwrap → cursor advance → forward**,
not Kafka consume.

Adapt the loader pattern as follows:
- Bash orchestrator: spin up wiremock (or a tiny Rust mock-API binary
  built from the lib) responding with realistic JSON payload mixes
  (paginated cursor responses, varied page sizes, occasional 429/500
  for retry-path coverage). Add the destination ingest sink (httpbin
  or a simple sink mock) on a second port.
- Rust pgo-driver bin: drive the fetcher under test via either its
  CLI (point it at the wiremock URL) OR by exercising public lib
  entry points if there's a `run_extractor()`-type API. Either way,
  loop the extract→cursor cycle for the full duration.
- Reference:
  [/projects/dfe-loader/scripts/pgo-workload.sh](/projects/dfe-loader/scripts/pgo-workload.sh)
  + [/projects/dfe-loader/src/bin/pgo-driver.rs](/projects/dfe-loader/src/bin/pgo-driver.rs)
  for the orchestration shape (signal trapping, readiness polling,
  duration floor of 60s, ephemeral config dir).

### Verification artefacts (loader v1.17.5)

For comparison after fetcher canary:
- amd64 binary: 18.2 MB stripped, 39 jemalloc symbol strings, BOLT
  marker present
- Build log signature: `Rust build optimisation: channel=release,
  allocator=jemalloc, lto=fat, pgo=on, bolt=on`
- Build duration jumped from ~4 min (Tier 1) to ~26 min (Tier 2 PGO+BOLT)
- R2: `https://downloads.hyperi.io/dfe-loader/v1.17.5/{dfe-loader-linux-{amd64,arm64},checksums.sha256}`

### Pre-flight checklist for fetcher canary

Before triggering the first release-channel publish:

- [x] **Bump `.github/workflows/ci.yml` pin** from `1d4fb19d` (v1.8.0)
      to `@main` (2026-04-29). Tier 2 will now run on tagged dispatch.
- [x] **Set explicit `build.rust.targets`** in `.hyperi-ci.yaml`
      (`x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`).
- [x] **Verify rustlib is at latest stable on crates.io.** Floor is
      `>=2.5.4`, latest stable is 2.5.4 (verified 2026-04-29 via
      `curl https://crates.io/api/v1/crates/hyperi-rustlib`).
- [ ] **Local hyperi-ci CLI matches PyPI latest** (currently v1.12.1).
      `uv tool upgrade hyperi-ci`.
- [ ] **Run `hyperi-ci check` locally** — must pass clippy + fmt +
      cargo deny. Confirmed clippy clean as of 2026-04-29 (3 Rust
      1.95 `duration_suboptimal_units` lints fixed).
- [ ] **Audit blind sleeps in tests** —
      `grep -rn "tokio::time::sleep(Duration::from_millis" tests/`
      and replace with port-poll / readiness assertions.

### Trigger sequence (verbatim from loader Canary 2)

Branch CI alone is insufficient — publish + release jobs are skipped
on non-main pushes. The path that actually reaches R2:

1. Real `fix:` (or `feat:`/`perf:`) commit on main → semantic-release
   bumps version + creates tag.
2. `git pull --rebase origin main` to pull the version-commit + tag.
3. `hyperi-ci release vX.Y.Z` → dispatches publish workflow (full
   PGO+BOLT build for both archs + R2 upload).
4. `hyperi-ci watch` (warning: 30 min default timeout — Tier 2 builds
   easily exceed that, re-watch as needed).
5. Verify R2 with `curl -I` on each artefact + `strings | grep jemalloc`
   on the downloaded binary.

For a canary commit, something real + small is better than chore-only
noise. Do NOT delete the GH Release first and try to re-publish the
same tag — the release handler refuses.
