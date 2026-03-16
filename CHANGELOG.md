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
