# Changelog

Rendered by CI and committed back at the end of a release -- do not edit by
hand. Release notes also appear on the GitHub Releases page, one per tag.

## [1.4.14](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.13...v1.4.14) (2026-09-04)

### Bug Fixes

* **pipeline:** stamp _source with the DFE source name on every delivery path ([#75](https://github.com/hyperi-io/dfe-fetcher/issues/75)) ([ed87b2d](https://github.com/hyperi-io/dfe-fetcher/commit/ed87b2d423e2d49d345d185e3992af8ee97b38cf)), closes [#74](https://github.com/hyperi-io/dfe-fetcher/issues/74)
* **tests:** retry a Kafka testcontainer start and say why when it still fails ([#76](https://github.com/hyperi-io/dfe-fetcher/issues/76)) ([e130a2a](https://github.com/hyperi-io/dfe-fetcher/commit/e130a2a81bbc6a1ebadcf731f444af641d04c185)), closes [#75](https://github.com/hyperi-io/dfe-fetcher/issues/75) [#75](https://github.com/hyperi-io/dfe-fetcher/issues/75)

## [1.4.13](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.12...v1.4.13) (2026-08-28)

### Bug Fixes

* version check on by default via the releases endpoint ([1688ec0](https://github.com/hyperi-io/dfe-fetcher/commit/1688ec0cbaeb481456bdae06f0453d5b16679b15))

## [1.4.12](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.11...v1.4.12) (2026-08-27)

### Bug Fixes

* adopt scalo 2.10.14 ([e524995](https://github.com/hyperi-io/dfe-fetcher/commit/e52499504544b8facb523f247147c121c115840e))
* drop the schemas submodule the fetcher never read ([#51](https://github.com/hyperi-io/dfe-fetcher/issues/51)) ([15ae1eb](https://github.com/hyperi-io/dfe-fetcher/commit/15ae1eb0ec4a969419814cf8e279aaaa4141f8fb))

## [1.4.11](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.10...v1.4.11) (2026-08-23)

### Bug Fixes

* **deps:** adopt scalo 2.10.13 and clear every rustsec advisory ([e4705fc](https://github.com/hyperi-io/dfe-fetcher/commit/e4705fc982a4962b0e012aebdc0beadb1c87d0ac))

## [1.4.10](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.9...v1.4.10) (2026-08-18)

## [1.4.9](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.8...v1.4.9) (2026-08-18)

## [1.4.8](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.7...v1.4.8) (2026-08-17)

## [1.4.6](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.5...v1.4.6) (2026-08-03)

# [1.3.0](https://github.com/hyperi-io/dfe-fetcher/compare/v1.2.3...v1.3.0) (2026-05-27)


### Bug Fixes

* adopt v2.7.1 DLQ API (Dlq::spawn + queue-admission send semantics) ([006960c](https://github.com/hyperi-io/dfe-fetcher/commit/006960ca71c569fb4e46d4cc4389b6eaa941d785))
* avoid needless clone of shutdown token (clippy --all-features) ([c7b7161](https://github.com/hyperi-io/dfe-fetcher/commit/c7b7161863b357807ff0221b4a20e88fa162a2aa))
* complete v2.7.1 DLQ API migration in fetcher test+bench sites ([a8771cc](https://github.com/hyperi-io/dfe-fetcher/commit/a8771cc76da1faf2e8412cb96034c72e944126f5))
* **deps:** bump astral-tokio-tar 0.6.1 -> 0.6.2 + metrics-util 0.20.2 -> 0.20.4 ([b1cae36](https://github.com/hyperi-io/dfe-fetcher/commit/b1cae36ea5a780bdb8c40de6c57ba2157a2a1bfd))
* **release:** force patch bump v1.2.4 ([de5e455](https://github.com/hyperi-io/dfe-fetcher/commit/de5e4559ac83c3af509ccc9c6b052b6e8ea0fe7a))
* **security:** rotate Duo HMAC test fixture to low-entropy placeholder ([f336929](https://github.com/hyperi-io/dfe-fetcher/commit/f33692953dafecc267d06c1956c68f0428c94b2a)), closes [hi#entropy](https://github.com/hi/issues/entropy) [hi#entropy](https://github.com/hi/issues/entropy)


### Features

* expand source coverage to 16 source families + rustlib 2.8.0 ([6af52e4](https://github.com/hyperi-io/dfe-fetcher/commit/6af52e455eb538d436c0b3f2571d5c4e0e561422))

## [1.2.3](https://github.com/hyperi-io/dfe-fetcher/compare/v1.2.2...v1.2.3) (2026-05-07)


### Bug Fixes

* **cli:** flatten StandardCommand for generate-artefacts + metrics-manifest ([02a6c44](https://github.com/hyperi-io/dfe-fetcher/commit/02a6c447787b9e53eccaaeec6eb6a7d45bd96eaa))
* **deploy:** regenerate Dockerfile with Ubuntu 24.04 userdel fix ([0a525c8](https://github.com/hyperi-io/dfe-fetcher/commit/0a525c84f9bd2a34c39395e43c75d217c3023af1))
* **release:** retrigger publish under hyperi-ci v2.1.5 ([5c182cf](https://github.com/hyperi-io/dfe-fetcher/commit/5c182cff4cb9442b980e462764b5ebd248bdff1f))
* **release:** retrigger publish under hyperi-ci v2.1.6 ([ec832ed](https://github.com/hyperi-io/dfe-fetcher/commit/ec832edffe9ba9f2670f79fe1ceeafa2e7b4946a))

## [1.2.2](https://github.com/hyperi-io/dfe-fetcher/compare/v1.2.1...v1.2.2) (2026-05-02)


### Bug Fixes

* **deployment:** wire DfeApp::deployment_contract trait hook + bump rustlib to >=2.7.0 ([a75905a](https://github.com/hyperi-io/dfe-fetcher/commit/a75905a48481210aca50c6ba43d2008e53c9fd9e))
* **deps:** track rustlib 2.6.1 (cli→cli-service, worker→worker-pool) ([f2e38e4](https://github.com/hyperi-io/dfe-fetcher/commit/f2e38e40e7639a35f71daa6cf456b6fa54bab1e2))

## [1.2.1](https://github.com/hyperi-io/dfe-fetcher/compare/v1.2.0...v1.2.1) (2026-04-29)


### Bug Fixes

* add rust-toolchain.toml with llvm-tools-preview for Tier 2 PGO ([1dad301](https://github.com/hyperi-io/dfe-fetcher/commit/1dad301add36102719909565c2748a505b4d9597))

# [1.2.0](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.13...v1.2.0) (2026-04-29)


### Bug Fixes

* drop openssl chain via rustlib 2.5.5 + reqwest defaults ([91d34de](https://github.com/hyperi-io/dfe-fetcher/commit/91d34decea6f3bee343fb021b543296a201ff5a8))
* rustfmt + non-AKIA mock access key for pgo workload ([0a6561a](https://github.com/hyperi-io/dfe-fetcher/commit/0a6561ad952a030cb5c15cf1202947a2c6dcdc36))


### Features

* tier 2 PGO+BOLT workload (pgo-driver + workload script) ([6f1ac1e](https://github.com/hyperi-io/dfe-fetcher/commit/6f1ac1e35894d66df1fc0bca21ded7f4cbd1bdd7))

## [1.1.13](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.12...v1.1.13) (2026-04-17)


### Bug Fixes

* lift test coverage to 82% with testcontainer-backed integration tests ([f4c6735](https://github.com/hyperi-io/dfe-fetcher/commit/f4c6735d4cd7c5a7719410a6b4290079b52f4462)), closes [#18](https://github.com/hyperi-io/dfe-fetcher/issues/18)

## [1.1.12](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.11...v1.1.12) (2026-04-16)


### Bug Fixes

* bump rustlib to >=2.5.4 and add deny.toml ([bd435d7](https://github.com/hyperi-io/dfe-fetcher/commit/bd435d73c3272f831e36650c7b92837f2be287d6))

## [1.1.11](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.10...v1.1.11) (2026-04-15)


### Bug Fixes

* deserialise double-serialised JSON string fields before delivery ([2d75e15](https://github.com/hyperi-io/dfe-fetcher/commit/2d75e153c754e2266a0f43ccabc793fd647a25d8)), closes [#17](https://github.com/hyperi-io/dfe-fetcher/issues/17)

## [1.1.10](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.9...v1.1.10) (2026-04-09)


### Bug Fixes

* standardise config path to /etc/dfe/fetcher.yaml ([e4a1d61](https://github.com/hyperi-io/dfe-fetcher/commit/e4a1d61e3bc8b94e9bf20644909d285d2f249585)), closes [#16](https://github.com/hyperi-io/dfe-fetcher/issues/16)


### Performance Improvements

* zero-copy and parallelism improvements across fetch pipeline ([9b96c86](https://github.com/hyperi-io/dfe-fetcher/commit/9b96c866430743bdbd872d58f7cbd5f53b4afd99))

## [1.1.9](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.8...v1.1.9) (2026-04-03)


### Bug Fixes

* add missing semicolons on warn! macro calls (clippy) ([b6e96e4](https://github.com/hyperi-io/dfe-fetcher/commit/b6e96e4240d82bdc29fca03d1ab97383068621ce))
* cargo fmt — reformat concurrent fetch code ([c25ff0f](https://github.com/hyperi-io/dfe-fetcher/commit/c25ff0fa7b2ef8d632a477cfd14d8dae5a00526a))
* concurrent within-source service fetching via join_all ([d79cde6](https://github.com/hyperi-io/dfe-fetcher/commit/d79cde65cdf990a078f25bf81791a0d3f111da3f))

## [1.1.8](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.7...v1.1.8) (2026-04-02)


### Bug Fixes

* add comprehensive debug/trace logging across scheduler, pipeline, output ([900f944](https://github.com/hyperi-io/dfe-fetcher/commit/900f944266be5fa336ebe1de9eb24058b40466f1))
* bump hyperi-rustlib to 2.4.3, adopt ServiceRuntime, fix SensitiveString API ([1553246](https://github.com/hyperi-io/dfe-fetcher/commit/1553246954164a4c0f48bd2e3bcc5026846d8652))
* cargo fmt — wrap long import line ([144d0f2](https://github.com/hyperi-io/dfe-fetcher/commit/144d0f2aea790c151044b3b96cc36edff51054c4))
* improve AWS service fetch error handling (continue on failure) ([723af3f](https://github.com/hyperi-io/dfe-fetcher/commit/723af3f2b49139c1fd903f06d32371fa97434cad))
* remove tracked target symlink — breaks CI runners ([85bfbcb](https://github.com/hyperi-io/dfe-fetcher/commit/85bfbcba2479dd3b2d88f847a3d5c39361e5d0b5))
* update DfeMetrics::register() to pass &MetricsManager for manifest ([171c4c5](https://github.com/hyperi-io/dfe-fetcher/commit/171c4c54e0872035c986777235cfeecbf3bfb50c))
* update to rustlib v2.x ServiceRuntime + deployment contract fields ([98f4b67](https://github.com/hyperi-io/dfe-fetcher/commit/98f4b67f12ae79940189c57ee9e119aaa54eafb3))

## [1.1.7](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.6...v1.1.7) (2026-03-27)


### Bug Fixes

* correct lib.rs doc comment to mention gRPC output ([28e4f41](https://github.com/hyperi-io/dfe-fetcher/commit/28e4f41ce4bc979d253642d294531c504750175c))
* trigger release after single versioning migration ([007633f](https://github.com/hyperi-io/dfe-fetcher/commit/007633f087045ea42ed1c020c9bf52db587d0a16))

## [1.1.3-dev.6](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.3-dev.5...v1.1.3-dev.6) (2026-03-25)


### Bug Fixes

* align with rustlib 1.19.6 — config registry, SensitiveString, MetricsManager server, version-check ([f2132f3](https://github.com/hyperi-io/dfe-fetcher/commit/f2132f345f29e24fc3ea000ef12bf9e916f720bb))

## [1.1.3-dev.5](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.3-dev.4...v1.1.3-dev.5) (2026-03-24)


### Bug Fixes

* replace invalid Renovate preset :pinActionsToFullSha with helpers:pinGitHubActionDigestsToSemver ([d649acd](https://github.com/hyperi-io/dfe-fetcher/commit/d649acd7c02776f31c21b909791438af49ba2ddc))

## [1.1.3-dev.4](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.3-dev.3...v1.1.3-dev.4) (2026-03-22)


### Bug Fixes

* deprecate legacy kafka: config, add output.topic_suffix ([981038f](https://github.com/hyperi-io/dfe-fetcher/commit/981038ff2399b16495c465ccf7bce1ff3f1bb779))
* inline Renovate config (preset resolution broken) ([0eb0c57](https://github.com/hyperi-io/dfe-fetcher/commit/0eb0c579bdac3aaca8990a621bb5354bb3c2b54e))

## [1.1.3-dev.3](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.3-dev.2...v1.1.3-dev.3) (2026-03-20)


### Bug Fixes

* add ingest request metrics and extractor name labels ([1ed51a4](https://github.com/hyperi-io/dfe-fetcher/commit/1ed51a43d982dae95c4ff66ac5ff7e5ec4c2c76d))
* merge fetch counters into dfe_fetcher_fetches_total with source+status labels, fix MetricsManager namespace ([d5c39f2](https://github.com/hyperi-io/dfe-fetcher/commit/d5c39f212d48be81e258fa162874db64244e93d5))

## [1.1.3-dev.2](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.3-dev.1...v1.1.3-dev.2) (2026-03-20)


### Bug Fixes

* add dfe_fetcher_api_errors_total with source and code labels ([44f3752](https://github.com/hyperi-io/dfe-fetcher/commit/44f37528854ca9520fcbed3d5b83a12be762ba05))
* add fetch_duration_seconds histogram and cursor_age_seconds gauge ([6b9c0b5](https://github.com/hyperi-io/dfe-fetcher/commit/6b9c0b57b8758bc4fa03099646be5a5149043b5b))
* rename fetcher metrics to dfe_fetcher_* prefix, add _total suffix to all counters ([1df0bb0](https://github.com/hyperi-io/dfe-fetcher/commit/1df0bb0674fc80575f131fc59ac37914fdd401b7))

## [1.1.3-dev.1](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.2...v1.1.3-dev.1) (2026-03-20)


### Bug Fixes

* bump rustlib to >=1.16.6, fix clippy, add rustlib migration roadmap ([81ec6c9](https://github.com/hyperi-io/dfe-fetcher/commit/81ec6c962f67b6191c9f1adb02d22769696dc291))
* register all metrics with metrics crate, deprecate legacy KafkaConfig ([012a319](https://github.com/hyperi-io/dfe-fetcher/commit/012a31915e575b42e4be1d92dc706fcbb24778fd))
* replace hand-rolled RateWindow with rustlib scaling::RateWindow ([9602f44](https://github.com/hyperi-io/dfe-fetcher/commit/9602f4411c5e5436a42acab246fa917074396f27))
* standardise test infra with dual-mode common module ([2432195](https://github.com/hyperi-io/dfe-fetcher/commit/243219585108ef79f7ee3a0973c7e8cadbec5b1e))

## [1.1.2](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.1...v1.1.2) (2026-03-20)


### Bug Fixes

* GA blockers — FetchWindow, cursor fallback, integration tests, .env ([eba3026](https://github.com/hyperi-io/dfe-fetcher/commit/eba3026ab78cdcc6d2656fac0322b25e3f31f457))
* wire FetchWindow into all source implementations for cursor-based incremental fetching ([f547795](https://github.com/hyperi-io/dfe-fetcher/commit/f547795dc846bdab883bf3095ee84b26836d006d))

## [1.1.1](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.0...v1.1.1) (2026-03-20)


### Bug Fixes

* replace BufferManager with rustlib MemoryGuard ([27f35fe](https://github.com/hyperi-io/dfe-fetcher/commit/27f35fec69c29b0f4f1a2a062b0c3b2d7ba7afd6))

# [1.1.0](https://github.com/hyperi-io/dfe-fetcher/compare/v1.0.0...v1.1.0) (2026-03-19)


### Bug Fixes

* add backpressure handling with stall, metrics, and transport health tracking ([bea83a0](https://github.com/hyperi-io/dfe-fetcher/commit/bea83a0f3f8e352755d7a8131f6009989b74485a))
* add build.type app, remove legacy publish workflow ([2fdc2db](https://github.com/hyperi-io/dfe-fetcher/commit/2fdc2db7aa096730de6a488f894590f7a1dac41b))
* add chart, infra, schemas to cargo publish exclude list [skip ci] ([644bcdf](https://github.com/hyperi-io/dfe-fetcher/commit/644bcdfc63ec56f09a75dcdfa307363a5ce32e31))
* add container restart-on-crash with exponential backoff ([d6089bd](https://github.com/hyperi-io/dfe-fetcher/commit/d6089bd795a32871d6cf6726d543f0792d772034))
* add cursor store with file and kafka backends ([e51791b](https://github.com/hyperi-io/dfe-fetcher/commit/e51791bd1017f86af874b79178b51e9ea311905c))
* add DfeMetrics dual-emit alongside existing metrics ([5bed1b8](https://github.com/hyperi-io/dfe-fetcher/commit/5bed1b8309a84dd24df56893ce46e55fc0a85b28))
* add FetchWindow to Source trait for incremental fetching ([3ce62b9](https://github.com/hyperi-io/dfe-fetcher/commit/3ce62b9442f5b983fa49b5921d29c9c8a60fae8d))
* add native_deps and image_profile to deployment contract ([4922b73](https://github.com/hyperi-io/dfe-fetcher/commit/4922b73a7a00a53b6cbe8676ccd734287a3317fa))
* add output, cursor, instance_id, filter, auth, restart config ([0f87cb0](https://github.com/hyperi-io/dfe-fetcher/commit/0f87cb06ea8e4eb52c353a959d4d292f4c52df6b))
* add OutputTransport using rustlib Transport trait ([67e5c7e](https://github.com/hyperi-io/dfe-fetcher/commit/67e5c7e8ab2de59b7fd6158b9eb53b0426d70e2e))
* add per-message CEL filtering with rustlib expression engine ([a912cdd](https://github.com/hyperi-io/dfe-fetcher/commit/a912cdd155c6f094a594a4d29db8844643dc12e9))
* add ScalingPressure for KEDA-optimised autoscaling ([604b4c3](https://github.com/hyperi-io/dfe-fetcher/commit/604b4c3de203b72fc950697b78b84f423d0b7c4c))
* add transport, cursor, and filter error variants ([24daaf9](https://github.com/hyperi-io/dfe-fetcher/commit/24daaf95e931a59bcbaacb077def4efa8ba9832d))
* address review findings — enrichment, dual-write, stale docs, deprecation warning ([c5c3e19](https://github.com/hyperi-io/dfe-fetcher/commit/c5c3e19ac6797bd6676bfdabb967d45654ed45e0))
* apply log spam controls at 4 hot spots ([e6e961b](https://github.com/hyperi-io/dfe-fetcher/commit/e6e961b3e93aae1c5ada3e54cf6bd9d814bc608c))
* bump rustlib to >=1.16.3 for observability features ([7161073](https://github.com/hyperi-io/dfe-fetcher/commit/7161073cddc0af4b1edb8339c44bda7ff481acb5))
* correct import ordering after log spam and security changes ([6aa642e](https://github.com/hyperi-io/dfe-fetcher/commit/6aa642eac67ca20878ede98cf4e261f785a48330))
* delete custom Sink trait, use rustlib Transport uniformly ([58ae98f](https://github.com/hyperi-io/dfe-fetcher/commit/58ae98f9d8960b45e60df6f2305c52682789aab5))
* format output.rs clone_from line ([42bb1df](https://github.com/hyperi-io/dfe-fetcher/commit/42bb1df3d671268a192073046ba8398bec4cc091))
* integrate cursor store into scheduler for incremental fetching ([c158eb9](https://github.com/hyperi-io/dfe-fetcher/commit/c158eb9606465341bdb3d03e0cad2a06c9be8246))
* make scheduler interval hot-reloadable, document reload behaviour ([a0be5ca](https://github.com/hyperi-io/dfe-fetcher/commit/a0be5ca5094fbd0ffe5133093c6d18cffabd4c11))
* migrate env overrides to rustlib ApplyFlatEnv trait ([765d12d](https://github.com/hyperi-io/dfe-fetcher/commit/765d12dbf0bdbc35e32f614ebd30b0ef9c75c7c8))
* migrate ingest and metrics servers to rustlib HttpServer ([4480094](https://github.com/hyperi-io/dfe-fetcher/commit/44800946027e6f97129d0217f91aaa7776f28b1c))
* migrate to hyperi-ci and upgrade to edition 2024 ([e09240a](https://github.com/hyperi-io/dfe-fetcher/commit/e09240a54120eb9de6952e24e0b59305bd039bae))
* remove plugin .so system, use container/sidecar approach ([26a22c9](https://github.com/hyperi-io/dfe-fetcher/commit/26a22c9a7d2a2b2025922747cec8dbbd0be80c92))
* resolve all clippy warnings ([efa28f3](https://github.com/hyperi-io/dfe-fetcher/commit/efa28f3948306ddcbbeaa65aab5cf785cf0253c2))
* rewrite pipeline to use rustlib Transport via OutputManager ([ed2f34e](https://github.com/hyperi-io/dfe-fetcher/commit/ed2f34ea8f08b6c689ec14ba75fb6aee1f242254))
* standardise metric names to dfe_ prefix for cross-service consistency ([5aefbf6](https://github.com/hyperi-io/dfe-fetcher/commit/5aefbf6d70c042de283d4b9d140a102bcc35b271))
* update aws-lc-sys to 0.38.0 for security patches ([9e16616](https://github.com/hyperi-io/dfe-fetcher/commit/9e16616a3e9b6086fe5e3aeda2d8a5d118f95cbf))
* use crates.io for hyperi-rustlib instead of jfrog registry ([5337b3e](https://github.com/hyperi-io/dfe-fetcher/commit/5337b3e6e0e841d9efaeb66b239d6a8a063c97f4))
* wire security event logging for auth, config, and DLQ ([c89b430](https://github.com/hyperi-io/dfe-fetcher/commit/c89b430a028ba6da438bfcbf880dd6b63cb09fdd))


### Features

* add CLI module, deployment contract, and generated artifacts [skip ci] ([b0a8304](https://github.com/hyperi-io/dfe-fetcher/commit/b0a8304f28f8a9d0a72a872dc49fed86ac20b0c3))
* add cloudwatch logs and metrics to aws source ([cf77190](https://github.com/hyperi-io/dfe-fetcher/commit/cf77190800c6d61a2eaf170a290e0a8b10354035))
* add OTLP protobuf output for cloudwatch metrics ([f8ca862](https://github.com/hyperi-io/dfe-fetcher/commit/f8ca86274f3a6582f824a5448d559d4380f90411))
* add TUI dashboard subcommand via rustlib top module ([c316933](https://github.com/hyperi-io/dfe-fetcher/commit/c31693305a6fcaa7719db05416dc4789dcad276e))

# [1.1.0-dev.4](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.0-dev.3...v1.1.0-dev.4) (2026-03-19)


### Bug Fixes

* add DfeMetrics dual-emit alongside existing metrics ([5bed1b8](https://github.com/hyperi-io/dfe-fetcher/commit/5bed1b8309a84dd24df56893ce46e55fc0a85b28))
* apply log spam controls at 4 hot spots ([e6e961b](https://github.com/hyperi-io/dfe-fetcher/commit/e6e961b3e93aae1c5ada3e54cf6bd9d814bc608c))
* bump rustlib to >=1.16.3 for observability features ([7161073](https://github.com/hyperi-io/dfe-fetcher/commit/7161073cddc0af4b1edb8339c44bda7ff481acb5))
* correct import ordering after log spam and security changes ([6aa642e](https://github.com/hyperi-io/dfe-fetcher/commit/6aa642eac67ca20878ede98cf4e261f785a48330))
* migrate env overrides to rustlib ApplyFlatEnv trait ([765d12d](https://github.com/hyperi-io/dfe-fetcher/commit/765d12dbf0bdbc35e32f614ebd30b0ef9c75c7c8))
* wire security event logging for auth, config, and DLQ ([c89b430](https://github.com/hyperi-io/dfe-fetcher/commit/c89b430a028ba6da438bfcbf880dd6b63cb09fdd))

# [1.1.0-dev.3](https://github.com/hyperi-io/dfe-fetcher/compare/v1.1.0-dev.2...v1.1.0-dev.3) (2026-03-19)


### Bug Fixes

* add ScalingPressure for KEDA-optimised autoscaling ([604b4c3](https://github.com/hyperi-io/dfe-fetcher/commit/604b4c3de203b72fc950697b78b84f423d0b7c4c))
* make scheduler interval hot-reloadable, document reload behaviour ([a0be5ca](https://github.com/hyperi-io/dfe-fetcher/commit/a0be5ca5094fbd0ffe5133093c6d18cffabd4c11))
* migrate ingest and metrics servers to rustlib HttpServer ([4480094](https://github.com/hyperi-io/dfe-fetcher/commit/44800946027e6f97129d0217f91aaa7776f28b1c))

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
