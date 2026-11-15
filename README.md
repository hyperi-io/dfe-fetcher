# dfe-fetcher

Pulls data from cloud and SaaS APIs, databases and files into a DFE deployment.

[![Build Status](https://github.com/hyperi-io/dfe-fetcher/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/dfe-fetcher/actions)
[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](LICENSE)

## What it does

- A new REST provider is a YAML profile, with no Rust: the base URL, auth modes, endpoints, decoder and pager, checked at load with the line of any unknown key.
- Database sources over ODBC, the ClickHouse HTTP interface and MongoDB, dumped whole or tailed, by key tuple on SQL and by change stream or `_id` on MongoDB.
- Any tool run in a Docker or podman container as a source, its stdout JSON lines or HTTP posts collected, and Vector's native gRPC protocol received.
- A cursor or checkpoint written only after the transport has taken the batch, so a failed tick fetches again rather than losing records. A dump travels as one snapshot, framed `begin`, rows and `end`.
- A per-source CEL filter, re-read every tick, and a memory brake that pauses polling a provider while the service is under memory pressure, for at most `self_regulation.max_hold_secs` at a time.
- Secrets from a secrets manager, an env var or a literal, config reload on SIGHUP or file watch, and a deployment contract that generates its Dockerfile, its Compose file and the released chart.

Every record goes to the DFE pipeline over Kafka or gRPC, the same way dfe-receiver's do.

## Quick start

On Linux the build needs `protoc`, a C toolchain, `pkg-config` and librdkafka. scalo compiles its gRPC `.proto` files, `aws-lc-sys` compiles C, and `rdkafka-sys` links librdkafka through pkg-config. It refuses a librdkafka older than the version after the `+` in its `Cargo.lock` entry, and a Debian or Ubuntu package can be older than that, so there [build librdkafka from source](https://github.com/confluentinc/librdkafka#build-from-source) first.

```bash
sudo apt-get install -y protobuf-compiler build-essential pkg-config
cargo build --release
./target/release/dfe-fetcher --config config.example.yaml config-check
./target/release/dfe-fetcher --config config.example.yaml
```

`--config` goes before the subcommand. [config.example.yaml](config.example.yaml) enables no source, so the fetcher starts idle and logs `no work configured` until a source, an extractor or the ingest listener is switched on. Metrics and health answer on port 9090 (`/metrics`, `/livez`, `/readyz`), and `/readyz` answers `{"status":"ready"}` while it idles. The ingest HTTP endpoint on 8080 and the Vector gRPC receiver on 6000 are off until a deployment turns them on.

A default build carries the REST sources and the file dump. The database engines and the file tailer are cargo features of `crates/fetcher/Cargo.toml` (`db-odbc`, `db-clickhouse`, `db-mongodb`, `file-tail`), and a config naming an engine the binary lacks is refused at load with the feature to build. The release image is built with all four and `jemalloc` (`build.rust.features` in `.hyperi-ci.yaml`).

## Configuration

[config.example.yaml](config.example.yaml) is the annotated reference, [docs/config-schema.yaml](docs/config-schema.yaml) the generated list of every key, and [docs/reference/profile-grammar.md](docs/reference/profile-grammar.md) the profile grammar field by field. Settings load through a cascade: CLI flags, then `DFE_FETCHER_*` environment variables, then the config file, then defaults. The file is the one `--config` names, and there is no `DFE_FETCHER_CONFIG` variable. Without `--config`, scalo's own search for `settings.yaml` and `defaults.yaml` stands in, with `.env` read from the working directory, as [docs/internals.md](docs/internals.md#configuration-cascade) sets out. Each provider has its typed block (`sources.aws`, `sources.okta`, and so on), and `sources.rest.<id>` takes a shipped or inline profile.

## Sources

Each REST source is a shipped profile under `crates/fetcher/profiles/`, and databases and files are source kinds of their own (`sources.db`, `sources.file`). A profile's `maturity` is repeated in [docs/capability-catalog.yaml](docs/capability-catalog.yaml), and the fetcher warns at startup for any enabled source that is not stable. [docs/cloud-setup/README.md](docs/cloud-setup/README.md) indexes the setup guides.

| Source | Block | What it pulls |
|--------|-------|---------------|
| AWS | `sources.aws` | CloudTrail, GuardDuty, Security Hub, Config, CloudWatch Logs and Metrics, Inspector v2, Health |
| Azure | `sources.azure` | Activity Log, Defender alerts, Sentinel incidents, the Entra ID audit feeds, Log Analytics queries |
| Microsoft 365 | `sources.m365` | Management Activity feeds (audit, DLP, Exchange), Graph security alerts |
| Google Cloud | `sources.gcp` | Cloud Audit Logs, VPC flow and DNS query logs, any Cloud Logging filter, Security Command Center findings |
| Google Cloud Pub/Sub, Google Workspace | `sources.gcp_pubsub`, `sources.google_workspace` | Pull subscriptions, Reports API activity |
| Okta, Cisco Duo, CrowdStrike, Cloudflare | `sources.okta`, `sources.duo`, `sources.crowdstrike`, `sources.cloudflare` | System Log, authentication logs, Falcon alerts, account audit logs |
| Datadog, runZero | `sources.rest` with the shipped profile | Audit trail and security signals, asset inventory snapshots |
| Bitwarden, 1Password, Slack, GitHub, Salesforce | `sources.bitwarden`, `sources.onepassword`, `sources.slack`, `sources.github`, `sources.salesforce` | Each product's audit and event logs |
| Object store | `sources.object_store` | S3 and S3-compatible bucket prefixes, each object read at least once |
| PyPI, crates.io, Go modules | `sources.pypi`, `sources.crates_io`, `sources.go_modules` | Package metadata for supply-chain monitoring |
| Databases | `sources.db` | Any ODBC-reachable database, ClickHouse or MongoDB, dumped whole or tailed by key |
| Files | `sources.file` | Directories of NDJSON, JSON or CSV dumps, or log files tailed |

A new API starts as a profile, shipped under `crates/fetcher/profiles/` or inline under `sources.rest.<id>.profile`. Rust is added only for what the grammar cannot express, as a variant on the matching axis in `crates/rest`.

## Documentation

- [docs/internals.md](docs/internals.md): the system, the framework's tick and the configuration cascade, with diagrams.
- [docs/delivery.md](docs/delivery.md) and [docs/extractors.md](docs/extractors.md): what happens to a record on its way out, and the container and Vector extractors.
- [docs/architecture.md](docs/architecture.md): the codemap of the Cargo workspace.
- [docs/reference/](docs/reference/profile-grammar.md): the profile grammar, the snapshot envelope and the database drivers.
- [docs/cloud-setup/](docs/cloud-setup/README.md): one setup guide per provider.
- [docs/testing.md](docs/testing.md): the test tiers and the live-API suites.

The suite's published docs are at <https://dfe.docs.hyperi.io>, and this repository's pages at <https://dfe.docs.hyperi.io/dfe-fetcher/>.

## Help and contributing

How to work in this repo, the commands that prove a change and what tends to bite are in [CONTRIBUTING.md](CONTRIBUTING.md). A question that is not a bug goes in a new issue.

Open bugs: [this repo](https://github.com/hyperi-io/dfe-fetcher/issues?q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor) | [whole DFE suite](https://github.com/search?type=issues&q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor+repo%3Ahyperi-io%2Fdfe-infra+repo%3Ahyperi-io%2Fdfe-docker+repo%3Ahyperi-io%2Fdfe-deploy+repo%3Ahyperi-io%2Fdfe-engine+repo%3Ahyperi-io%2Fdfe-loader+repo%3Ahyperi-io%2Fdfe-receiver+repo%3Ahyperi-io%2Fdfe-fetcher+repo%3Ahyperi-io%2Fdfe-archiver+repo%3Ahyperi-io%2Fdfe-transform-vrl+repo%3Ahyperi-io%2Fdfe-transform-vector+repo%3Ahyperi-io%2Fdfe-transform-elastic+repo%3Ahyperi-io%2Fdfe-ui+repo%3Ahyperi-io%2Fdfe-hyperdx+repo%3Ahyperi-io%2Fscalo-rs+repo%3Ahyperi-io%2Fscalo-py+repo%3Ahyperi-io%2Flogreducer+repo%3Ahyperi-io%2Fdfe-schemas+repo%3Ahyperi-io%2Fvector-vrl+repo%3Ahyperi-io%2Fculvert+repo%3Ahyperi-io%2Fdfe-clickhouse-rs+repo%3Ahyperi-io%2Ffactbook-rs)

## License

BUSL-1.1, see [LICENSE](LICENSE).
