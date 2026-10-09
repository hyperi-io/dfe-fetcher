# Changelog

Rendered by CI and committed back at the end of a release -- do not edit by
hand. Release notes also appear on the GitHub Releases page, one per tag.

## [1.4.33](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.32...v1.4.33) (2026-10-09)

### Bug Fixes

* allow container env specs in credential gate ([#224](https://github.com/hyperi-io/dfe-fetcher/issues/224)) ([5305270](https://github.com/hyperi-io/dfe-fetcher/commit/530527000511fd810c7fd9945a6beea935b3df8f))
* emit deployment contract v4 ([#223](https://github.com/hyperi-io/dfe-fetcher/issues/223)) ([b7c1865](https://github.com/hyperi-io/dfe-fetcher/commit/b7c18657b82d605b2fbf1fc62210e80cdc58f73a))

## [1.4.32](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.31...v1.4.32) (2026-10-06)

### Bug Fixes

* **ci:** make markdownlint blocking ([#211](https://github.com/hyperi-io/dfe-fetcher/issues/211)) ([8ab9126](https://github.com/hyperi-io/dfe-fetcher/commit/8ab9126cd1fe1327e333f5e3b789eedd4e47cc0f)), closes [#210](https://github.com/hyperi-io/dfe-fetcher/issues/210)
* clear clippy 1.99 lints and rename fetch_update ([#217](https://github.com/hyperi-io/dfe-fetcher/issues/217)) ([762f6e5](https://github.com/hyperi-io/dfe-fetcher/commit/762f6e5a295d29e2fd341af8713ad9fae0f7d3f3))
* **docs:** clear the markdownlint warnings ([#210](https://github.com/hyperi-io/dfe-fetcher/issues/210)) ([0a742d7](https://github.com/hyperi-io/dfe-fetcher/commit/0a742d765634028729e1c77f376868bb23a08bc6))
* GA public tree, secret types, dump tie, one API-error count ([#207](https://github.com/hyperi-io/dfe-fetcher/issues/207)) ([ae9046d](https://github.com/hyperi-io/dfe-fetcher/commit/ae9046ddfc53deed74a38cdd097357dc0e50ce18))
* **gcp:** take the key JSON the chart ships ([#209](https://github.com/hyperi-io/dfe-fetcher/issues/209)) ([782053b](https://github.com/hyperi-io/dfe-fetcher/commit/782053b90c714c3d8ebc2c0051a89bfc74a3aa41))
* honour the version-check opt-out ([#201](https://github.com/hyperi-io/dfe-fetcher/issues/201)) ([9a8d98a](https://github.com/hyperi-io/dfe-fetcher/commit/9a8d98a66a271adc35bca43735cbb285fa804f4c)), closes [#200](https://github.com/hyperi-io/dfe-fetcher/issues/200)
* move to scalo 2.14.0 and turn PGO and BOLT on for GA ([#220](https://github.com/hyperi-io/dfe-fetcher/issues/220)) ([0ae4a16](https://github.com/hyperi-io/dfe-fetcher/commit/0ae4a163be2449ccaf148e1c666bf2df5d693624)), closes [#138](https://github.com/hyperi-io/dfe-fetcher/issues/138) [#218](https://github.com/hyperi-io/dfe-fetcher/issues/218) [#183](https://github.com/hyperi-io/dfe-fetcher/issues/183) [hyperi-io/scalo-rs#281](https://github.com/hyperi-io/scalo-rs/issues/281)
* name config keys the fetcher ignores ([#216](https://github.com/hyperi-io/dfe-fetcher/issues/216)) ([3f1b5ce](https://github.com/hyperi-io/dfe-fetcher/commit/3f1b5ce6a8c26495a36eec86549131089fca503c)), closes [#179](https://github.com/hyperi-io/dfe-fetcher/issues/179) [#168](https://github.com/hyperi-io/dfe-fetcher/issues/168)
* refuse container env names that reach the CLI itself ([#213](https://github.com/hyperi-io/dfe-fetcher/issues/213)) ([656bf80](https://github.com/hyperi-io/dfe-fetcher/commit/656bf800ea8a3c8b0500bd0ca53d0224382e8d87))
* refuse off-host request URLs, fetch every Security Hub status ([#203](https://github.com/hyperi-io/dfe-fetcher/issues/203)) ([1bd8026](https://github.com/hyperi-io/dfe-fetcher/commit/1bd8026911630956d1717b71c8ce4193efec0de9))
* refuse to run the ingest listener with no token ([#215](https://github.com/hyperi-io/dfe-fetcher/issues/215)) ([b0a4822](https://github.com/hyperi-io/dfe-fetcher/commit/b0a482225913211e901ac27704fdaea42ff78f15))
* replace private issue refs with setup requirements ([#195](https://github.com/hyperi-io/dfe-fetcher/issues/195)) ([f18d6e9](https://github.com/hyperi-io/dfe-fetcher/commit/f18d6e91b5adc1ed9aa7bf05fcdaaef467a24cf5))
* route topics, unstall windows, fsync cursors ([#212](https://github.com/hyperi-io/dfe-fetcher/issues/212)) ([3245fb9](https://github.com/hyperi-io/dfe-fetcher/commit/3245fb9d0adb4e51053b86aab3cdac48a289c216))
* tell chart users to set image.tag ([#214](https://github.com/hyperi-io/dfe-fetcher/issues/214)) ([84fc1b3](https://github.com/hyperi-io/dfe-fetcher/commit/84fc1b32fb17c3840ecc65e05de3f4db6c0cee7c)), closes [#174](https://github.com/hyperi-io/dfe-fetcher/issues/174)
* **test:** run the Kafka test broker on the JVM image ([#202](https://github.com/hyperi-io/dfe-fetcher/issues/202)) ([a7e34d7](https://github.com/hyperi-io/dfe-fetcher/commit/a7e34d7a8084a454ab4aa02637ef23c99e791e21))
* **test:** wait for the group join, then frames ([#208](https://github.com/hyperi-io/dfe-fetcher/issues/208)) ([ee35787](https://github.com/hyperi-io/dfe-fetcher/commit/ee35787d6e27f1197c04411771d69d708a46e99c))

## [1.4.31](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.30...v1.4.31) (2026-09-27)

### Bug Fixes

* zstd producer default, steadier tail test, regenerated compose ([#194](https://github.com/hyperi-io/dfe-fetcher/issues/194)) ([0e44c7b](https://github.com/hyperi-io/dfe-fetcher/commit/0e44c7bafaa142c60d5c84c8356f1afbf13ea2ae))

## [1.4.30](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.29...v1.4.30) (2026-09-27)

### Bug Fixes

* **fetcher:** hold Vector and container intake until delivery ([#192](https://github.com/hyperi-io/dfe-fetcher/issues/192)) ([26c0437](https://github.com/hyperi-io/dfe-fetcher/commit/26c04379a5e10fd3bb7caa460227afdd00460c7f))
* **metrics:** count each failed send once ([#191](https://github.com/hyperi-io/dfe-fetcher/issues/191)) ([ff200e3](https://github.com/hyperi-io/dfe-fetcher/commit/ff200e314917d9f6e70c9916fd47fce6692bdefc))

## [1.4.29](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.28...v1.4.29) (2026-09-24)

### Bug Fixes

* **ci:** ignore unreachable otel advisory in osv ([#187](https://github.com/hyperi-io/dfe-fetcher/issues/187)) ([10efe44](https://github.com/hyperi-io/dfe-fetcher/commit/10efe440d84e74e5730650a85862321358b626da))
* **deps:** hmac 0.13, quick-xml 0.42, rust deps ([#186](https://github.com/hyperi-io/dfe-fetcher/issues/186)) ([9144f52](https://github.com/hyperi-io/dfe-fetcher/commit/9144f526d1ad01587140c2864b16798396e3040d))
* **output:** dead-letter refused fan-out records ([#189](https://github.com/hyperi-io/dfe-fetcher/issues/189)) ([ff58f8f](https://github.com/hyperi-io/dfe-fetcher/commit/ff58f8f4c800e38679a72d44572dc317ca2ed664))
* rebuild on scalo 2.12.9 ([ea91eb6](https://github.com/hyperi-io/dfe-fetcher/commit/ea91eb6b6bca05935d6bdf0ff84f55be01b8bf23))

## [1.4.28](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.27...v1.4.28) (2026-09-24)

### Bug Fixes

* rebuild on scalo 2.12.7, S3 source stable ([#185](https://github.com/hyperi-io/dfe-fetcher/issues/185)) ([878614b](https://github.com/hyperi-io/dfe-fetcher/commit/878614bc1e2e88587ab3f4f299d7743d848de31d))

## [1.4.27](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.26...v1.4.27) (2026-09-24)

### Bug Fixes

* **auth:** authenticate with a key pair, and mint a session token ([4b480e7](https://github.com/hyperi-io/dfe-fetcher/commit/4b480e78aa0ee5a0f31e38b448eaef8de4a57694))
* **auth:** mint and place credentials through scalo's sources ([c96aa60](https://github.com/hyperi-io/dfe-fetcher/commit/c96aa60ec76b9c8ecf0ffc19ebbaa08755419ef3)), closes [#118](https://github.com/hyperi-io/dfe-fetcher/issues/118) [#110](https://github.com/hyperi-io/dfe-fetcher/issues/110) [#112](https://github.com/hyperi-io/dfe-fetcher/issues/112) [#114](https://github.com/hyperi-io/dfe-fetcher/issues/114)
* **auth:** sign a request with the digest the profile names ([8119301](https://github.com/hyperi-io/dfe-fetcher/commit/81193012a3716cc9df7bd8772a66a100f89bf03a)), closes [#153](https://github.com/hyperi-io/dfe-fetcher/issues/153)
* **auth:** sign requests through scalo and resolve every prefix it serves ([61d3a3d](https://github.com/hyperi-io/dfe-fetcher/commit/61d3a3d21bf48f4820a2656cbf1115cd30b5499b)), closes [#72](https://github.com/hyperi-io/dfe-fetcher/issues/72)
* **ci:** skip PGO and BOLT for the rc.14 workstream ([#182](https://github.com/hyperi-io/dfe-fetcher/issues/182)) ([ff253e3](https://github.com/hyperi-io/dfe-fetcher/commit/ff253e3d1bbad9b953bcd4e0cee7378ac8a93e90))
* **config:** count the vector receiver as work, and say so when idle ([97657cc](https://github.com/hyperi-io/dfe-fetcher/commit/97657cc3ca66d2ead16875a38a07484c4f13adcb))
* **config:** refuse a shared instance id when more than one source runs ([8a37e11](https://github.com/hyperi-io/dfe-fetcher/commit/8a37e11cb7f6152bad83313d907e6508c49eea33))
* **config:** resolve every plain identity field at load ([abdca8b](https://github.com/hyperi-io/dfe-fetcher/commit/abdca8be302602c920da32a471cfaeb1c6d9a8a8)), closes [#21](https://github.com/hyperi-io/dfe-fetcher/issues/21)
* **datadog:** add the setup guide, and name the permission Datadog actually has ([7f1e580](https://github.com/hyperi-io/dfe-fetcher/commit/7f1e5802c00aeacf39b1ca1a14bc9e9aaa02efd0))
* **deps:** drop four dependencies nothing calls, and demote two to dev ([d2a8521](https://github.com/hyperi-io/dfe-fetcher/commit/d2a8521b162447039a8944e20464692051d7a4d6)), closes [#63](https://github.com/hyperi-io/dfe-fetcher/issues/63)
* **deps:** move the REST path to reqwest 0.13 ([a3df3f9](https://github.com/hyperi-io/dfe-fetcher/commit/a3df3f995989c0b790306cfe8f5d8ada5ea187d0)), closes [#39](https://github.com/hyperi-io/dfe-fetcher/issues/39) [#39](https://github.com/hyperi-io/dfe-fetcher/issues/39)
* **deps:** unyank chacha20, record the otel advisory's reachability, say release ([6b805fd](https://github.com/hyperi-io/dfe-fetcher/commit/6b805fd686cdf246a04b189d1dc56479ba1fad58)), closes [#64](https://github.com/hyperi-io/dfe-fetcher/issues/64)
* **docs:** move the architecture doc under docs/ and add the README Context section ([#177](https://github.com/hyperi-io/dfe-fetcher/issues/177)) ([e7f1f44](https://github.com/hyperi-io/dfe-fetcher/commit/e7f1f447eb8258062c79de42da548a917dcd2978))
* **docs:** name scalo, not rustlib or pylib ([#181](https://github.com/hyperi-io/dfe-fetcher/issues/181)) ([d46c840](https://github.com/hyperi-io/dfe-fetcher/commit/d46c840e99734865fd4605269f6a87e5ef51f931))
* **gates:** match a sigv4 signer, not the word in a comment ([741e28c](https://github.com/hyperi-io/dfe-fetcher/commit/741e28c6c7d7af4299beec9103bfb71c446218c1))
* rebuild on scalo 2.12.6 with release consent ([e14db5d](https://github.com/hyperi-io/dfe-fetcher/commit/e14db5dbda96c357f30225624956f0bcd2a18470))
* **rest:** name the units a profile declares when refusing an unknown one ([28f59d6](https://github.com/hyperi-io/dfe-fetcher/commit/28f59d69098b48f2f02b712a4813e3d6c9ba50d4)), closes [#98](https://github.com/hyperi-io/dfe-fetcher/issues/98)
* **rest:** pace units to a declared rate and retry declared throttles ([45b27b3](https://github.com/hyperi-io/dfe-fetcher/commit/45b27b3962f269e50c9bce2b904f077ae1dea66c)), closes [#120](https://github.com/hyperi-io/dfe-fetcher/issues/120)
* **rest:** place more than one credential on a request ([6badc72](https://github.com/hyperi-io/dfe-fetcher/commit/6badc72f37abd9f46104fa8d609e737e945cbaed)), closes [#148](https://github.com/hyperi-io/dfe-fetcher/issues/148) [#149](https://github.com/hyperi-io/dfe-fetcher/issues/149)
* **scheduler:** retry a failed tick over the same window and bound the catch-up span ([edfbf1e](https://github.com/hyperi-io/dfe-fetcher/commit/edfbf1eee5983b3f08ddf72478b7698c961ac10f)), closes [#133](https://github.com/hyperi-io/dfe-fetcher/issues/133)
* **scheduler:** stop a source's fetch task when a reload removes it ([f677c33](https://github.com/hyperi-io/dfe-fetcher/commit/f677c33d5b54dec87919f2327dfe2bc93cb9ec10)), closes [#119](https://github.com/hyperi-io/dfe-fetcher/issues/119)
* **test:** bound the file-tail and dump listing races instead of sleeping ([8e205c5](https://github.com/hyperi-io/dfe-fetcher/commit/8e205c585eb24509878a483e626fec8a0861c966)), closes [#117](https://github.com/hyperi-io/dfe-fetcher/issues/117) [#117](https://github.com/hyperi-io/dfe-fetcher/issues/117)
* **test:** cross a second boundary instead of polling for a change time ([33b53aa](https://github.com/hyperi-io/dfe-fetcher/commit/33b53aa9cffec3eeca8739966c178ed5ea1e5f2f)), closes [#171](https://github.com/hyperi-io/dfe-fetcher/issues/171)
* **test:** pin Kafka to the version the fleet can actually run ([62a7354](https://github.com/hyperi-io/dfe-fetcher/commit/62a7354efa0f17bba45602fbfc7f22b514fc6c4b))
* **test:** stop the re-read test asserting which tick the line lands on ([687f569](https://github.com/hyperi-io/dfe-fetcher/commit/687f569401b41edcae8e9d415d9bbbfcbb4dbca1)), closes [#116](https://github.com/hyperi-io/dfe-fetcher/issues/116)
* **test:** use a port nothing can bind, not one just handed back ([9dfb307](https://github.com/hyperi-io/dfe-fetcher/commit/9dfb3074c9c4cf2cf594507d4432b8cca7f7d1ba))

## [1.4.26](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.25...v1.4.26) (2026-09-15)

### Bug Fixes

* **config:** refuse a container env spec the resolver cannot read ([0cde15c](https://github.com/hyperi-io/dfe-fetcher/commit/0cde15ca515af73d88149e8a335b513a9ed13b64)), closes [#125](https://github.com/hyperi-io/dfe-fetcher/issues/125)

## [1.4.25](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.24...v1.4.25) (2026-09-15)

### Bug Fixes

* **auth:** refuse unreadable credential specs and resolve the identity fields ([610aa89](https://github.com/hyperi-io/dfe-fetcher/commit/610aa893f3c02013ff762ccce98f6be3031ee631)), closes [#111](https://github.com/hyperi-io/dfe-fetcher/issues/111) [#121](https://github.com/hyperi-io/dfe-fetcher/issues/121) [#122](https://github.com/hyperi-io/dfe-fetcher/issues/122)
* **ci:** ship the database engines and the file tail in the release image ([e6dba41](https://github.com/hyperi-io/dfe-fetcher/commit/e6dba411ac5774293f96790c03dbb6a9f98c6e41)), closes [hyperi-ci#130](https://github.com/hyperi-io/hyperi-ci/issues/130)
* **db:** one config grammar for every database engine ([1eb2542](https://github.com/hyperi-io/dfe-fetcher/commit/1eb25429698752f6fb69e263d2119a3fa9f498ed)), closes [dfe-infra#304](https://github.com/hyperi-io/dfe-infra/issues/304)
* **test:** prove the api-key redaction rather than trust the call sites ([057a094](https://github.com/hyperi-io/dfe-fetcher/commit/057a0947b8fe11581c0d2806dae9d079bc7324fa)), closes [#102](https://github.com/hyperi-io/dfe-fetcher/issues/102)
* **test:** spell the mongodb store fixtures the way the grammar does ([ae9bdf5](https://github.com/hyperi-io/dfe-fetcher/commit/ae9bdf5ce465882cbc14877789262ea1c4a875e7))

## [1.4.24](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.23...v1.4.24) (2026-09-15)

### Bug Fixes

* **chart:** restore the generated chart and guard it against drift ([2c7bb95](https://github.com/hyperi-io/dfe-fetcher/commit/2c7bb95a935afe1ea9b7c1ad6594f6706bd674bf))

## [1.4.23](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.22...v1.4.23) (2026-09-15)

### Bug Fixes

* **pgo:** name the gcp service the profile actually declares ([ee3c246](https://github.com/hyperi-io/dfe-fetcher/commit/ee3c246415930721f22249cae6aaec24ef421f3e))
* **sources:** run every source on one generic framework ([47deaa5](https://github.com/hyperi-io/dfe-fetcher/commit/47deaa55614dba1bae084d8b0915c949f373605f))

## [1.4.22](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.21...v1.4.22) (2026-09-12)

### Bug Fixes

* rebuild on scalo 2.12.2 ([21976d5](https://github.com/hyperi-io/dfe-fetcher/commit/21976d509a17a42e253952b10f361edf910d30e0))

## [1.4.21](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.20...v1.4.21) (2026-09-10)

### Bug Fixes

* **config:** make the settings a deployment writes actually reach something ([c146d64](https://github.com/hyperi-io/dfe-fetcher/commit/c146d641aaaab27fe74960dd023642646f913aa3))
* **deps:** raise the scalo floor to 2.12.1 ([0d4a2ab](https://github.com/hyperi-io/dfe-fetcher/commit/0d4a2ab768e1d47d8c9dfdc50428a7f2c661bd8a)), closes [#54](https://github.com/hyperi-io/dfe-fetcher/issues/54) [#67](https://github.com/hyperi-io/dfe-fetcher/issues/67) [#68](https://github.com/hyperi-io/dfe-fetcher/issues/68) [#69](https://github.com/hyperi-io/dfe-fetcher/issues/69)
* **docs:** make the config reference describe what the code does ([83f6b4f](https://github.com/hyperi-io/dfe-fetcher/commit/83f6b4f650b946f29726f2c48a0abaed1aca9c22))
* **health:** keep an output outage out of the readiness probe ([db1645b](https://github.com/hyperi-io/dfe-fetcher/commit/db1645bbeb0a9631832d978b10a6a2e4a04437dd))
* **ingest:** resolve the ingest auth token and pin the contract default to the code ([22db218](https://github.com/hyperi-io/dfe-fetcher/commit/22db2181bbdd1eeaf43e82b280353742ce3daa65)), closes [#85](https://github.com/hyperi-io/dfe-fetcher/issues/85)
* **pipeline:** release tracked bytes when a send is cancelled ([861038b](https://github.com/hyperi-io/dfe-fetcher/commit/861038ba714753edc7a0f7bc4190192ffa12def7))

## [1.4.20](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.19...v1.4.20) (2026-09-10)

### Bug Fixes

* idle a fetcher with no work instead of refusing its own default config ([73859d2](https://github.com/hyperi-io/dfe-fetcher/commit/73859d2fa947622ef879064dca421ffe94312796)), closes [#269](https://github.com/hyperi-io/dfe-fetcher/issues/269) [#84](https://github.com/hyperi-io/dfe-fetcher/issues/84)
* prove the first enabled source starts fetching without a restart ([c968727](https://github.com/hyperi-io/dfe-fetcher/commit/c96872749cde135939e1fe9461af6524c04a291b))

## [1.4.19](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.18...v1.4.19) (2026-09-09)

### Bug Fixes

* output destinations with data-match routes and hold on backpressure ([73828ec](https://github.com/hyperi-io/dfe-fetcher/commit/73828ec442717aeeb187df08dd4d865091e269df))
* rebuild on scalo 2.12.1 ([e19bfad](https://github.com/hyperi-io/dfe-fetcher/commit/e19bfad634e680ca57a3d467ff63f951ee2f7bbf))

## [1.4.18](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.17...v1.4.18) (2026-09-08)

### Bug Fixes

* **config:** container and vector paths honour output.topic_suffix ([015a9e5](https://github.com/hyperi-io/dfe-fetcher/commit/015a9e5d3ca8dd323e82a027d46a81abc2125ff4))

## [1.4.17](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.16...v1.4.17) (2026-09-05)

### Bug Fixes

* **pipeline:** the append path escapes source names once per batch, not per record ([#79](https://github.com/hyperi-io/dfe-fetcher/issues/79)) ([b0858bc](https://github.com/hyperi-io/dfe-fetcher/commit/b0858bc42080d08c3317fd89aa495255cd02acfa)), closes [#78](https://github.com/hyperi-io/dfe-fetcher/issues/78)

## [1.4.16](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.15...v1.4.16) (2026-09-04)

### Bug Fixes

* **pipeline:** a reserved-key rename never clobbers an existing <key>_original ([#78](https://github.com/hyperi-io/dfe-fetcher/issues/78)) ([de5506c](https://github.com/hyperi-io/dfe-fetcher/commit/de5506cc0535a1dedff7e38dd9a3c06e7b261055)), closes [#77](https://github.com/hyperi-io/dfe-fetcher/issues/77)

## [1.4.15](https://github.com/hyperi-io/dfe-fetcher/compare/v1.4.14...v1.4.15) (2026-09-04)

### Bug Fixes

* **ingest:** a payload that already carries _source no longer reaches the loader with two of them ([#77](https://github.com/hyperi-io/dfe-fetcher/issues/77)) ([751f6e5](https://github.com/hyperi-io/dfe-fetcher/commit/751f6e5d92ad94468b9743c25bd0a733c40cb09e)), closes [#75](https://github.com/hyperi-io/dfe-fetcher/issues/75)

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
