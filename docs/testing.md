<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/testing.md                      -->
<!-- Purpose:   Test levels and the shared test services -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Testing

This page covers the test levels dfe-fetcher runs and the services its tests share.

| Level | Runs | Selection |
|---|---|---|
| core | every pull request to `main`, and every push that changes more than `docs/` and Markdown (`paths-ignore` in `.github/workflows/ci.yml`) | nextest's default selection: every test not marked `#[ignore]` |
| full | nightly (`schedule` in `.github/workflows/ci.yml`) and before every release (`test.full.required_for_release: true`) | core plus the `#[ignore]` tests, less the live-provider suites |

Core holds the unit tests, the fixture and wiremock suites, and every integration test against a running service: Kafka, the Kafka TLS round trip, OpenBao, LocalStack, ClickHouse, MongoDB, and PostgreSQL and MariaDB over ODBC.

Full adds the container extractor tests, the Kafka produce-and-consume tests in the `e2e` target and the batcher measurement, which moves rows through a running Kafka. `test.full.rust.filter` in `.hyperi-ci.yaml` leaves out `smoke_remote`, `runzero` and `hana`, which need tenant credentials or a HANA system no runner holds. Run those by hand with `cargo nextest run -p dfe-fetcher --test e2e --run-ignored only <prefix>` and the variables each [cloud-setup](cloud-setup/README.md) guide names. A missing variable fails its test rather than skipping it.

The `hana` suite (`crates/fetcher/tests/e2e/hana.rs`, feature `db-hana`) reads SAP HANA Cloud with `SAP_HANA_HOST`, `SAP_HANA_PORT`, `SAP_HANA_DB_ADMIN_USERNAME` and `SAP_HANA_DB_ADMIN_PASSWORD`, or any system through `HANA_TEST_URL`, a full `hdbsql://` or `hdbsqls://` URL. HANA Express needs 8 GB and runs on amd64 only, so no runner starts it. Run it on a host with the memory: `docker run -d --name dfe-fetcher-hxe --hostname hxehost -p 127.0.0.1:39041:39041 --ulimit nofile=1048576:1048576 --sysctl kernel.shmmax=1073741824 --sysctl kernel.shmmni=4096 --sysctl net.ipv4.ip_local_port_range='40000 60999' -v <dir>:/hana/hxeinit -v hxe-data:/hana/mounts saplabs/hanaexpress --agree-to-sap-license --passwords-url file:///hana/hxeinit/password.json`, where `<dir>/password.json` is `{"master_password": "<password>"}`. It logs `Startup finished` after a few minutes. Set `HANA_TEST_URL=hdbsql://SYSTEM:<password>@127.0.0.1:39041`, run the suite on that host, then remove the container, its volume and the password file. The suite creates one table of its own and drops it.

The `db-odbc` suites need that feature to build and the `odbc-postgresql` and `odbc-mariadb` drivers, which hyperi-ci installs on the CI runner. Each starts a database container through testcontainers.

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

- The first test process to ask creates the container, called `<prefix>-integration-<service>-r<run>`. `<prefix>` is `HYPERI_TEST_CONTAINER_PREFIX`, else `dfe-fetcher-test`. Every other process joins it by name.
- A watcher removes it once the run's nextest process exits, whether the run succeeded, failed or was interrupted.
- Containers have `io.hyperi.test.suite=dfe-fetcher-integration` and their owner's labels, so a later create sweeps what a dead run left.
- The daemon is the one `DOCKER_HOST` names, else the current docker context's, and a daemon reached over `ssh://` gets a tunnel per port. With no daemon a test skips locally and fails in CI.

Tests on a shared service keep to their names: topics and consumer groups per test, OpenBao paths per test, a ClickHouse table per test and a MongoDB database per test.

The Kafka TLS test starts a dedicated broker with scalo's `TlsBroker`, which generates a private CA. It produces through the `kafka:` block with `tls.ca_paths` naming that CA, then again with no CA set and `SSL_CERT_FILE` holding it.
