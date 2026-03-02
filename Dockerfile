# Project:   dfe-fetcher
# File:      Dockerfile
# Purpose:   Multi-stage build for dfe-fetcher
# License:   FSL-1.1-ALv2
# Copyright: (c) 2026 HYPERI PTY LIMITED

# =============================================================================
# Stage 1: Build
# =============================================================================
FROM rust:1.82-bookworm AS builder

# Install build dependencies for rdkafka (librdkafka) and protobuf (tonic)
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependencies: copy manifests first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs \
    && cargo build --release 2>/dev/null || true \
    && rm -rf src

# Build the actual binary
COPY src/ src/
RUN cargo build --release --locked

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd --gid 1000 dfe && \
    useradd --uid 1000 --gid dfe --shell /bin/false --create-home dfe

COPY --from=builder /build/target/release/dfe-fetcher /usr/local/bin/dfe-fetcher
COPY config.example.yaml /etc/dfe-fetcher/config.yaml

# Metrics port
EXPOSE 9090
# Ingest HTTP server port
EXPOSE 8080
# Vector gRPC port
EXPOSE 6000

USER dfe

ENTRYPOINT ["dfe-fetcher"]
CMD ["--config", "/etc/dfe-fetcher/config.yaml"]
