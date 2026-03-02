# dfe-fetcher

Data fetcher component for the HyperI DFE (Data Fusion Engine) platform.

Fetches security and operational data from cloud services and external extractors,
delivering to the DFE pipeline via Kafka.

## Architecture

```text
Native Sources (AWS, Azure, M365, GCP)
           |
Plugin Sources (.so)
           |
Container Extractors (Docker/podman)
           |
Vector.dev Extractors (gRPC)
           |
           v
Pipeline (enrich + route)
           |
           v
Kafka Sink (TieredSink)
```

## Features

- **Native sources**: AWS (CloudTrail, GuardDuty, SecurityHub, Config), Azure
  (Activity Log, Defender, Sentinel, Entra ID), M365 (Audit Log, Message Trace,
  DLP, Alerts), GCP (Audit Logs, SCC, Cloud Logging)
- **Container extractors**: Run any tool in Docker/podman containers with stdout
  or HTTP communication
- **Vector.dev integration**: Receive data via Vector's native gRPC protocol
- **Plugin system**: Dynamic `.so` plugin loading (requires separate unsafe crate)
- **Tiered sink**: In-memory buffering with circuit breaker when Kafka is unavailable
- **Metrics**: Prometheus-compatible `/metrics` endpoint
- **Config reload**: Live config reload via SIGHUP

## Quick Start

```bash
# Build
cargo build --release

# Run with example config
./target/release/dfe-fetcher --config config.example.yaml

# Docker
docker build -t dfe-fetcher .
docker run -v ./config.yaml:/etc/dfe-fetcher/config.yaml dfe-fetcher
```

## Configuration

See [config.example.yaml](config.example.yaml) for a full annotated configuration.

Key environment variable overrides (prefix `DFE_FETCHER_`):

| Variable | Description |
|----------|-------------|
| `DFE_FETCHER_KAFKA__BROKERS` | Kafka broker addresses |
| `DFE_FETCHER_KAFKA__TOPIC_SUFFIX` | Topic suffix (default: `_land`) |
| `DFE_FETCHER_SCHEDULER__DEFAULT_INTERVAL_SECS` | Fetch interval |
| `DFE_FETCHER_METRICS__ADDRESS` | Metrics bind address |
| `DFE_FETCHER_BUFFER__MEMORY_LIMIT` | Memory limit (0 = auto) |

## Credential Resolution

Credentials support three formats:

- `vault:secret/path:key` - Resolve from secrets manager (OpenBao/Vault)
- `env:VARIABLE_NAME` - Read from environment variable
- Literal string - Use as-is

## Known Limitations (MVP)

- **AWS SigV4**: Request signing is a placeholder. AWS API calls will be rejected
  until proper `aws-sigv4` crate signing is implemented.
- **GCP JWT**: Service account JWT signing is a placeholder. GCP token exchange
  will fail until `jsonwebtoken` crate RSA signing is added.
- **Plugin loading**: Plugin registry exists but actual `.so` loading is deferred
  (requires `unsafe` code in a separate crate).
- **No incremental fetching**: Sources fetch a fixed time window on each run.
  Cursor/checkpoint persistence is not yet implemented.
- **No ingest auth**: The `/ingest/:source` HTTP endpoint has no authentication.

## Development

```bash
# Check
cargo fmt --check
cargo clippy -- -D warnings
cargo test  # 71 tests
```

## License

FSL-1.1-ALv2 - See [LICENSE](LICENSE) for details.

Copyright (c) 2026 HYPERI PTY LIMITED
