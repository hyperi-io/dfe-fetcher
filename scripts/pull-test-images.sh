#!/usr/bin/env bash
# Project:   dfe-fetcher
# File:      scripts/pull-test-images.sh
# Purpose:   Pull every container image the tests start, before any test runs
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# nextest runs this as the `pull-test-images` setup script (.config/nextest.toml).
# A failed pull is retried, so a registry 5xx costs a retry rather than the run.
# The pins match crates/fetcher/tests/common/mod.rs, scalo's test_support and
# crates/fetcher/tests/e2e/container.rs, and container_hygiene checks the first two.

set -euo pipefail

IMAGES=(
    # scalo transport::kafka::test_support::KAFKA_IMAGE, also the TLS broker's.
    "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
    # scalo's TLS broker makes its certificates in this image.
    "alpine/openssl:3.5.9@sha256:1c361a204e9791bec6202bccbd1065eafaa65cb5a1a9a06ae000238569fba6a2"
    "openbao/openbao:2.6.1@sha256:5b2486ab0fb90bbc788cc345b0a08616dfb375873ee8be5df3a2fd4d378a67e0"
    "localstack/localstack:4.14@sha256:3ebc37595918b8accb852f8048fef2aff047d465167edd655528065b07bc364a"
    "clickhouse/clickhouse-server:26.3.42.3@sha256:21d572843e59539c7d100286b6f5a6053c341fe4b33c2d7b74b1ff7cb24c5399"
    "mongo:8.3.9@sha256:81a1c8842a09589fc8d5f285266f3340bf4abdf66700ba22988f14cc9b2b3118"
    # The container extractor's e2e tests.
    "alpine:3.24.2@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6"
)

# Seconds to wait before the second, third and fourth attempt.
BACKOFF=(5 15 45)

pull() {
    local image="$1" attempt=1
    until docker pull --quiet "${image}" >/dev/null; do
        if ((attempt > ${#BACKOFF[@]})); then
            echo "pull-test-images: ${image} failed after ${attempt} attempts" >&2
            return 1
        fi
        echo "pull-test-images: ${image} attempt ${attempt} failed, retrying in ${BACKOFF[attempt - 1]}s" >&2
        sleep "${BACKOFF[attempt - 1]}"
        attempt=$((attempt + 1))
    done
}

main() {
    # With no Docker daemon the container tests skip locally, or fail in CI, and say why.
    if ! docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
        echo "pull-test-images: no Docker daemon, nothing pulled" >&2
        return 0
    fi
    local image
    for image in "${IMAGES[@]}"; do
        if docker image inspect "${image}" >/dev/null 2>&1; then
            echo "pull-test-images: ${image} present" >&2
            continue
        fi
        echo "pull-test-images: pulling ${image}" >&2
        pull "${image}"
    done
}

main "$@"
