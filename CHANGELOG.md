# [1.1.0-dev.2](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.0-dev.1...v1.1.0-dev.2) (2026-03-18)


### Bug Fixes

* add backpressure handling with stall, metrics, and transport health tracking ([bea83a0](https://github.com/hyperi-io/dfe-fetcher/commit/bea83a0f3f8e352755d7a8131f6009989b74485a))
* add container restart-on-crash with exponential backoff ([d6089bd](https://github.com/hyperi-io/dfe-fetcher/commit/d6089bd795a32871d6cf6726d543f0792d772034))
* add cursor store with file and kafka backends ([e51791b](https://github.com/hyperi-io/dfe-fetcher/commit/e51791bd1017f86af874b79178b51e9ea311905c))
* add FetchWindow to Source trait for incremental fetching ([3ce62b9](https://github.com/hyperi-io/dfe-fetcher/commit/3ce62b9442f5b983fa49b5921d29c9c8a60fae8d))
* add output, cursor, instance_id, filter, auth, restart config ([0f87cb0](https://github.com/hyperi-io/dfe-fetcher/commit/0f87cb06ea8e4eb52c353a959d4d292f4c52df6b))
* add OutputTransport using rustlib Transport trait ([67e5c7e](https://github.com/hyperi-io/dfe-fetcher/commit/67e5c7e8ab2de59b7fd6158b9eb53b0426d70e2e))
* add per-message CEL filtering with rustlib expression engine ([a912cdd](https://github.com/hyperi-io/dfe-fetcher/commit/a912cdd155c6f094a594a4d29db8844643dc12e9))
* add transport, cursor, and filter error variants ([24daaf9](https://github.com/hyperi-io/dfe-fetcher/commit/24daaf95e931a59bcbaacb077def4efa8ba9832d))
* address review findings — enrichment, dual-write, stale docs, deprecation warning ([c5c3e19](https://github.com/hyperi-io/dfe-fetcher/commit/c5c3e19ac6797bd6676bfdabb967d45654ed45e0))
* delete custom Sink trait, use rustlib Transport uniformly ([58ae98f](https://github.com/hyperi-io/dfe-fetcher/commit/58ae98f9d8960b45e60df6f2305c52682789aab5))
* format output.rs clone_from line ([42bb1df](https://github.com/hyperi-io/dfe-fetcher/commit/42bb1df3d671268a192073046ba8398bec4cc091))
* integrate cursor store into scheduler for incremental fetching ([c158eb9](https://github.com/hyperi-io/dfe-fetcher/commit/c158eb9606465341bdb3d03e0cad2a06c9be8246))
* remove plugin .so system, use container/sidecar approach ([26a22c9](https://github.com/hyperi-io/dfe-fetcher/commit/26a22c9a7d2a2b2025922747cec8dbbd0be80c92))
* resolve all clippy warnings ([efa28f3](https://github.com/hyperi-io/dfe-fetcher/commit/efa28f3948306ddcbbeaa65aab5cf785cf0253c2))
* rewrite pipeline to use rustlib Transport via OutputManager ([ed2f34e](https://github.com/hyperi-io/dfe-fetcher/commit/ed2f34ea8f08b6c689ec14ba75fb6aee1f242254))
* standardise metric names to dfe_ prefix for cross-service consistency ([5aefbf6](https://github.com/hyperi-io/dfe-fetcher/commit/5aefbf6d70c042de283d4b9d140a102bcc35b271))


### Features

* add TUI dashboard subcommand via rustlib top module ([c316933](https://github.com/hyperi-io/dfe-fetcher/commit/c31693305a6fcaa7719db05416dc4789dcad276e))

# [1.1.0-dev.1](https://github.com/hyperi-io/dfe-fetcher/compare/v1.0.0...v1.1.0-dev.1) (2026-03-16)


### Bug Fixes

* add build.type app, remove legacy publish workflow ([2fdc2db](https://github.com/hyperi-io/dfe-fetcher/commit/2fdc2db7aa096730de6a488f894590f7a1dac41b))
* add chart, infra, schemas to cargo publish exclude list [skip ci] ([644bcdf](https://github.com/hyperi-io/dfe-fetcher/commit/644bcdfc63ec56f09a75dcdfa307363a5ce32e31))
* add native_deps and image_profile to deployment contract ([4922b73](https://github.com/hyperi-io/dfe-fetcher/commit/4922b73a7a00a53b6cbe8676ccd734287a3317fa))
* migrate to hyperi-ci and upgrade to edition 2024 ([e09240a](https://github.com/hyperi-io/dfe-fetcher/commit/e09240a54120eb9de6952e24e0b59305bd039bae))
* update aws-lc-sys to 0.38.0 for security patches ([9e16616](https://github.com/hyperi-io/dfe-fetcher/commit/9e16616a3e9b6086fe5e3aeda2d8a5d118f95cbf))
* use crates.io for hyperi-rustlib instead of jfrog registry ([5337b3e](https://github.com/hyperi-io/dfe-fetcher/commit/5337b3e6e0e841d9efaeb66b239d6a8a063c97f4))


### Features

* add CLI module, deployment contract, and generated artifacts [skip ci] ([b0a8304](https://github.com/hyperi-io/dfe-fetcher/commit/b0a8304f28f8a9d0a72a872dc49fed86ac20b0c3))
* add cloudwatch logs and metrics to aws source ([cf77190](https://github.com/hyperi-io/dfe-fetcher/commit/cf77190800c6d61a2eaf170a290e0a8b10354035))
* add OTLP protobuf output for cloudwatch metrics ([f8ca862](https://github.com/hyperi-io/dfe-fetcher/commit/f8ca86274f3a6582f824a5448d559d4380f90411))

# 1.0.0 (2026-03-03)


### Bug Fixes

* enable aws_lc_rs crypto provider for jsonwebtoken ([c871929](https://github.com/hyperi-io/dfe-fetcher/commit/c871929a81dce276f72aa6221e26efead26e8ee3))
* implement SigV4 signing, GCP JWT auth, and DLQ support ([7a531ca](https://github.com/hyperi-io/dfe-fetcher/commit/7a531ca5413f96391a960901a09bfcd58e4144c9))
* resolve compile blockers and align dependencies ([0c1c7a2](https://github.com/hyperi-io/dfe-fetcher/commit/0c1c7a26c278588f6e81df17359e7067fcdd2609))


### Features

* add ingest HTTP server and functional container extractors ([148f447](https://github.com/hyperi-io/dfe-fetcher/commit/148f447acd348ade008317e7232153e84ac4d514))
* add Vector.dev gRPC receiver using rustlib transport ([c51b2e1](https://github.com/hyperi-io/dfe-fetcher/commit/c51b2e1ccda4f6b42e9a7835dcd1056dd0e442f3))
* complete MVP with native sources, extractors, and test suite [skip ci] ([c1a7d45](https://github.com/hyperi-io/dfe-fetcher/commit/c1a7d453b43cbd570c8a88345c28fcd65f875010))
* initial project scaffold ([4a765de](https://github.com/hyperi-io/dfe-fetcher/commit/4a765dea9321a456ae24c15b5e7a657c906b35b7))
