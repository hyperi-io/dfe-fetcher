#!/usr/bin/env bash
# Project:   dfe-fetcher
# File:      scripts/pgo-workload.sh
# Purpose:   PGO workload orchestrator — mock cloud APIs + Redpanda + fetcher
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   scripts/pgo-workload.sh <path-to-dfe-fetcher-binary>
#
# Drives the fetcher's hot path (HTTP poll → JSON parse → enrichment → CEL
# filter → output produce → cursor advance) under representative load so a
# PGO-instrumented binary accumulates useful profile data.
#
# Environment variables (all optional):
#   PGO_WORKLOAD_DURATION_SECS   Duration of load (default 300, floor 60)
#   PGO_WORKLOAD_KAFKA_IMAGE     Override the Redpanda image (var keeps the
#                                KAFKA_ prefix because the wire protocol is
#                                still Kafka; downstream config is unchanged)
#   PGO_WORKLOAD_KEEP            Set to 1 to skip cleanup (debug)
#   PGO_DRIVER_PATH              Override pgo-driver binary path
#   PGO_DRIVER_PAGE_SIZE         Records per mock response (default 500)
#   PGO_DRIVER_PAGES_BEFORE_END  Pagination cycle depth (default 3)
#
# Preconditions:
#   - Docker daemon running, user has access
#   - $1 is the fetcher binary built with --features jemalloc
#   - pgo-driver binary built with --features pgo-driver (auto-built if missing)
#
# Behaviour:
#   - Starts mock cloud-API server (pgo-driver) on 127.0.0.1:19090
#   - Starts single-node Redpanda (Kafka-wire-protocol) on 127.0.0.1:19092
#   - Writes ephemeral fetcher config (Azure + M365 + AWS + GCP via overrides)
#   - Starts fetcher binary, waits for /readyz on 127.0.0.1:9090
#   - Lets fetcher poll the mock for $PGO_WORKLOAD_DURATION_SECS
#   - Cleans up (traps EXIT): kills fetcher + driver, removes containers

set -euo pipefail

# ----------------------------------------------------------------------------
# Args + env
# ----------------------------------------------------------------------------

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <path-to-dfe-fetcher-binary>" >&2
    exit 1
fi

FETCHER_BIN="$1"
if [[ ! -x "$FETCHER_BIN" ]]; then
    echo "error: $FETCHER_BIN is not executable" >&2
    exit 1
fi

DURATION="${PGO_WORKLOAD_DURATION_SECS:-300}"
KAFKA_IMAGE="${PGO_WORKLOAD_KAFKA_IMAGE:-docker.redpanda.com/redpandadata/redpanda:v26.1.9}"
KEEP="${PGO_WORKLOAD_KEEP:-0}"

# Floor of 60s — shorter workloads produce bad PGO profiles
if [[ "$DURATION" -lt 60 ]]; then
    echo "error: PGO_WORKLOAD_DURATION_SECS must be >= 60 (got $DURATION)" >&2
    echo "  short workloads produce NEGATIVE PGO gains by biasing the" >&2
    echo "  compiler toward startup paths instead of hot paths" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Locate pgo-driver; build on demand if missing.
PGO_DRIVER_PATH="${PGO_DRIVER_PATH:-}"
if [[ -z "$PGO_DRIVER_PATH" ]]; then
    for candidate in \
        "$PROJECT_ROOT/target/release/pgo-driver" \
        "$PROJECT_ROOT/target/debug/pgo-driver" \
        "/cache/cargo-targets/dfe-fetcher/release/pgo-driver" \
        "/cache/cargo-targets/dfe-fetcher/debug/pgo-driver"; do
        if [[ -x "$candidate" ]]; then
            PGO_DRIVER_PATH="$candidate"
            break
        fi
    done
fi
if [[ -z "$PGO_DRIVER_PATH" || ! -x "$PGO_DRIVER_PATH" ]]; then
    echo "pgo-workload: pgo-driver not found, building..." >&2
    (cd "$PROJECT_ROOT" && cargo build --release --features pgo-driver --bin pgo-driver) \
        || { echo "error: failed to build pgo-driver" >&2; exit 1; }
    PGO_DRIVER_PATH="$PROJECT_ROOT/target/release/pgo-driver"
    if [[ ! -x "$PGO_DRIVER_PATH" ]]; then
        echo "error: pgo-driver still missing after build at $PGO_DRIVER_PATH" >&2
        exit 1
    fi
fi

# ----------------------------------------------------------------------------
# Cleanup
# ----------------------------------------------------------------------------

FETCHER_PID=""
DRIVER_PID=""
KAFKA_CID=""
CONFIG_DIR=""

cleanup() {
    local rc=$?
    if [[ "$KEEP" == "1" ]]; then
        echo "PGO_WORKLOAD_KEEP=1 — skipping cleanup" >&2
        echo "  fetcher PID: $FETCHER_PID" >&2
        echo "  driver PID:  $DRIVER_PID" >&2
        echo "  broker CID:  $KAFKA_CID" >&2
        echo "  config dir:  $CONFIG_DIR" >&2
        return $rc
    fi
    echo "pgo-workload: cleanup" >&2
    if [[ -n "$FETCHER_PID" ]] && kill -0 "$FETCHER_PID" 2>/dev/null; then
        kill -TERM "$FETCHER_PID" 2>/dev/null || true
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            if ! kill -0 "$FETCHER_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        kill -KILL "$FETCHER_PID" 2>/dev/null || true
    fi
    if [[ -n "$DRIVER_PID" ]] && kill -0 "$DRIVER_PID" 2>/dev/null; then
        kill -TERM "$DRIVER_PID" 2>/dev/null || true
        for _ in 1 2 3 4 5; do
            if ! kill -0 "$DRIVER_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        kill -KILL "$DRIVER_PID" 2>/dev/null || true
    fi
    if [[ -n "$KAFKA_CID" ]]; then
        docker rm -f "$KAFKA_CID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$CONFIG_DIR" && -d "$CONFIG_DIR" ]]; then
        rm -rf "$CONFIG_DIR"
    fi
    exit $rc
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------------
# Start mock cloud-API server (pgo-driver)
# ----------------------------------------------------------------------------

echo "pgo-workload: starting pgo-driver: $PGO_DRIVER_PATH"
PGO_DRIVER_BIND="127.0.0.1:19090" \
PGO_DRIVER_PAGE_SIZE="${PGO_DRIVER_PAGE_SIZE:-500}" \
PGO_DRIVER_PAGES_BEFORE_END="${PGO_DRIVER_PAGES_BEFORE_END:-3}" \
    "$PGO_DRIVER_PATH" >/tmp/pgo-driver.log 2>&1 &
DRIVER_PID=$!
echo "pgo-workload: pgo-driver PID: $DRIVER_PID"

for attempt in $(seq 1 30); do
    if ! kill -0 "$DRIVER_PID" 2>/dev/null; then
        echo "error: pgo-driver died during startup" >&2
        tail -50 /tmp/pgo-driver.log >&2
        exit 1
    fi
    if curl -sf -o /dev/null --max-time 1 -X POST \
        "http://127.0.0.1:19090/oauth/token"; then
        echo "pgo-workload: pgo-driver ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 30 ]]; then
        echo "error: pgo-driver did not become ready in 30s" >&2
        tail -50 /tmp/pgo-driver.log >&2
        exit 1
    fi
    sleep 1
done

# ----------------------------------------------------------------------------
# Start Redpanda (Kafka-wire-protocol compatible single-node broker)
#
# Why Redpanda, not Apache Kafka: the Kafka JVM needs 1.5-2 GB heap+metaspace
# and won't co-exist with a PGO-instrumented binary on the 4 GB CI runners
# (see hyperi-io/dfe-fetcher#28). Redpanda is a single C++/Seastar binary,
# boots in ~1s, and fits comfortably under a 512 MiB cap. App config is
# unchanged - same `localhost:19092` broker, same wire protocol.
#
# `--mode dev-container` bundles `--overprovisioned --reserve-memory 0M
# --check=false --unsafe-bypass-fsync` and enables topic auto-create, which
# replaces the Kafka env-var matrix above.
# ----------------------------------------------------------------------------

echo "pgo-workload: starting Redpanda ($KAFKA_IMAGE)"
KAFKA_CID=$(docker run -d --rm \
    -p 19092:9092 \
    "$KAFKA_IMAGE" \
    redpanda start \
        --mode dev-container \
        --smp 1 \
        --memory 512M \
        --kafka-addr PLAINTEXT://0.0.0.0:9092 \
        --advertise-kafka-addr PLAINTEXT://localhost:19092)
echo "pgo-workload: Redpanda CID: $KAFKA_CID"

# Real protocol readiness via the admin API (rpk), not a bare TCP-open probe.
# TCP open != broker accepting Kafka protocol; the old loop slept 2s after
# TCP open to paper over the RAFT bootstrap race. With rpk we wait for the
# cluster to self-report Healthy.
for attempt in $(seq 1 60); do
    if docker exec "$KAFKA_CID" rpk cluster health 2>/dev/null | grep -q "Healthy:.*true"; then
        echo "pgo-workload: Redpanda ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 60 ]]; then
        echo "error: Redpanda did not become ready in 120s" >&2
        docker logs --tail 50 "$KAFKA_CID" >&2
        exit 1
    fi
    sleep 2
done

# ----------------------------------------------------------------------------
# Write ephemeral fetcher config
# ----------------------------------------------------------------------------

CONFIG_DIR=$(mktemp -d -t pgo-workload-XXXXXX)
CONFIG_FILE="$CONFIG_DIR/config.yaml"
CURSOR_DIR="$CONFIG_DIR/cursors"
mkdir -p "$CURSOR_DIR"

# Aggressive 1-second intervals across all four sources push max throughput
# through the hot path: HTTP fetch → parse → enrich → filter → produce.
cat > "$CONFIG_FILE" <<YAML
instance_id: "pgo-workload"

scheduler:
  default_interval_secs: 1
  jitter_percent: 0
  max_concurrent_fetches: 8

cursor:
  directory: "$CURSOR_DIR"
  default_window_hours: 1

output:
  output_type: "kafka"
  topic_suffix: ""
  kafka:
    brokers: ["localhost:19092"]
    client_id: "dfe-fetcher-pgo"
    librdkafka_overrides:
      linger.ms: "10"
      compression.type: "lz4"

sources:
  azure:
    enabled: true
    tenant_id: "00000000-0000-0000-0000-000000000001"
    client_id: "11111111-1111-1111-1111-111111111111"
    client_secret: "pgo-mock-secret"
    subscription_id: "00000000-0000-0000-0000-000000000001"
    interval_secs: 1
    topic: "azure"
    services:
      - name: activity_log
    token_url_override: "http://127.0.0.1:19090/oauth/token"
    management_url_override: "http://127.0.0.1:19090/azure/activity"
    graph_url_override: "http://127.0.0.1:19090/azure/graph"

  m365:
    enabled: true
    tenant_id: "00000000-0000-0000-0000-000000000002"
    client_id: "22222222-2222-2222-2222-222222222222"
    client_secret: "pgo-mock-secret"
    interval_secs: 1
    topic: "m365"
    services:
      - name: alerts
    token_url_override: "http://127.0.0.1:19090/oauth/token"
    management_url_override: "http://127.0.0.1:19090/m365/management"
    graph_url_override: "http://127.0.0.1:19090/m365/graph"

  aws:
    enabled: true
    region: "ap-southeast-2"
    # Placeholder credentials — fetcher SigV4-signs requests but the
    # destination is the local mock, which ignores signatures. Use a
    # non-AKIA prefix so static-analysis secret scanners don't flag it.
    access_key_id: "PGOMOCK000000000000A"
    secret_access_key: "pgo-mock-secret-key-zzzzzzzzzzzzzzzzzzzzzzzz"
    interval_secs: 1
    topic: "aws"
    endpoint_override: "http://127.0.0.1:19090/aws"
    services:
      - name: cloudtrail
      - name: guardduty

  gcp:
    enabled: true
    project_id: "pgo-project"
    interval_secs: 1
    topic: "gcp"
    services:
      - name: audit_logs
        filter: "logName:cloudaudit.googleapis.com"
    api_url_override: "http://127.0.0.1:19090/gcp"
    token_url_override: "http://127.0.0.1:19090/oauth/token"

metrics_address: "127.0.0.1:9090"

dlq:
  enabled: false

config_reload_secs: 0
YAML

# ----------------------------------------------------------------------------
# Start fetcher
# ----------------------------------------------------------------------------

echo "pgo-workload: starting fetcher: $FETCHER_BIN"
echo "pgo-workload: config: $CONFIG_FILE"

# PGO profiles go here by default with cargo-pgo.
export LLVM_PROFILE_FILE="${LLVM_PROFILE_FILE:-$PROJECT_ROOT/target/pgo-profiles/pgo-%p_%m.profraw}"
mkdir -p "$(dirname "$LLVM_PROFILE_FILE")"

"$FETCHER_BIN" --config "$CONFIG_FILE" \
    >"$CONFIG_DIR/fetcher.log" 2>&1 &
FETCHER_PID=$!
echo "pgo-workload: fetcher PID: $FETCHER_PID"

for attempt in $(seq 1 60); do
    if ! kill -0 "$FETCHER_PID" 2>/dev/null; then
        echo "error: fetcher died during startup" >&2
        tail -100 "$CONFIG_DIR/fetcher.log" >&2
        exit 1
    fi
    if curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:9090/readyz" \
        || curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:9090/healthz"; then
        echo "pgo-workload: fetcher ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 60 ]]; then
        echo "error: fetcher did not become ready in 60s" >&2
        tail -100 "$CONFIG_DIR/fetcher.log" >&2
        exit 1
    fi
    sleep 1
done

# Extra settle so the first scheduled fetch round trips before we start timing.
sleep 2

# ----------------------------------------------------------------------------
# Drive load
# ----------------------------------------------------------------------------

echo "pgo-workload: driving load for ${DURATION}s (fetcher polls @ 1s on 4 sources)"

# Fetcher is its own load driver — it polls the mock at the configured
# intervals. We just sit here for the duration and keep an eye on liveness.
START="$(date +%s)"
END=$(( START + DURATION ))

while [[ "$(date +%s)" -lt "$END" ]]; do
    if ! kill -0 "$FETCHER_PID" 2>/dev/null; then
        echo "error: fetcher died during workload" >&2
        tail -100 "$CONFIG_DIR/fetcher.log" >&2
        exit 1
    fi
    if ! kill -0 "$DRIVER_PID" 2>/dev/null; then
        echo "error: pgo-driver died during workload" >&2
        tail -50 /tmp/pgo-driver.log >&2
        exit 1
    fi
    sleep 5
done

echo "pgo-workload: workload complete"

# Give the fetcher a moment to drain buffers + flush PGO profile data
sleep 5

echo "pgo-workload: done (fetcher logs: $CONFIG_DIR/fetcher.log)"
