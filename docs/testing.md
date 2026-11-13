<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/testing.md                      -->
<!-- Purpose:   Test levels and the shared test services -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Testing

The general rules are hyperi-ai's testing standard (`standards/universal/testing.md` in hyperi-ai). This page covers what dfe-fetcher does on top of them.

| Level | Runs | Selection |
|---|---|---|
| core | every push and PR | nextest's default selection: every test not marked `#[ignore]` |
| full | nightly (`schedule` in `.github/workflows/ci.yml`) and before every release (`test.full.required_for_release: true`) | core plus the `#[ignore]` tests, less the live-provider suites |

Core holds the unit tests, the fixture and wiremock suites, and every integration test against a real service: Kafka, the Kafka TLS round trip, OpenBao, LocalStack, ClickHouse and MongoDB.

Full adds the container extractor tests, the Kafka produce-and-consume tests in the `e2e` target and the batcher measurement, which moves 100k rows through real Kafka. `test.full.rust.filter` in `.hyperi-ci.yaml` leaves out `smoke_remote` and `runzero`, which need tenant credentials no runner holds. Run those by hand with the credentials their `#[ignore]` reasons name.

The `db-odbc` suites build only with that feature and need the `odbc-postgresql` and `odbc-mariadb` drivers, so CI does not run them. Each starts its own database container through testcontainers.

## Shared services, one instance each per run

| Service | Image | Container ports |
|---|---|---|
| Kafka | `apache/kafka` | 9092 PLAINTEXT |
| OpenBao | `openbao/openbao` | 8200 |
| LocalStack | `localstack/localstack` | 4566 |
| ClickHouse | `clickhouse/clickhouse-server` | 8123 HTTP |
| MongoDB | `mongo` | 27017, standalone and a one-member replica set |

The Kafka pin is scalo's `transport::kafka::test_support::KAFKA_IMAGE`. The rest live in `crates/fetcher/tests/common/mod.rs`.

scalo's `test_support::shared_service` starts them:

- The first test process to ask creates the container, named `<prefix>-integration-<service>-r<run>`. `<prefix>` is `HYPERI_TEST_CONTAINER_PREFIX`, else `dfe-fetcher-test`. Every other process joins it by name.
- A watcher removes it once the run's nextest process exits, whether the run passed, failed or was interrupted.
- Containers carry `io.hyperi.test.suite=dfe-fetcher-integration` and their owner's labels, so a later create sweeps what a dead run left.
- Locally the daemon is the current docker context, and an `ssh://` context gets a tunnel per port. With no daemon a test skips locally and fails in CI.

Tests on a shared service keep to their own names: topics and consumer groups per test, OpenBao paths per test, a ClickHouse table per test and a MongoDB database per test.

The Kafka TLS test starts a broker of its own with scalo's `TlsBroker`, which generates a private CA. It produces through the `kafka:` block with `tls.ca_file` naming that CA, then again with no CA named and `SSL_CERT_FILE` holding it.
