<!-- Project:   dfe-fetcher                            -->
<!-- File:      docs/reference/profile-grammar.md       -->
<!-- Purpose:   Reference for the declarative REST profile grammar -->
<!-- Language:  Markdown                                 -->
<!--                                                     -->
<!-- License:   BUSL-1.1                                 -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED              -->

# The REST profile grammar

A profile is the shape of an API and carries no identity: a base URL template,
the auth modes the API accepts, headers, the retry policy, where errors live in
a non-2xx body, the window format, and the endpoints with their decoder and
pager. An instance is one deployment of that shape: its credential kind and
refs, `vars`, topic and interval, with per-unit narrowing. Every struct rejects
unknown keys and every enum is a closed vocabulary, so a typo or an
unsupported mode is a load error carrying the YAML line, never a setting that
parses and does nothing.

The grammar as built is `crates/rest/src/profile/mod.rs`, and the generated
JSON Schema of the whole config surface (this grammar included) is
`docs/config-schema.yaml`. The shipped profiles under
`crates/fetcher/profiles/` are the worked examples of every entry below;
`config.example.yaml` shows an instance of one and an inline profile. Defaults
are not repeated here: read them off the `Default` impls in the grammar.

- [Vocabulary](#vocabulary)
- [Profile](#profile)
- [Auth](#auth)
- [Retry, errors and quota](#retry-errors-and-quota)
- [Window](#window)
- [Endpoints](#endpoints)
- [Rows](#rows)
- [Pagination](#pagination)
- [Constructs and prelude](#constructs-and-prelude)
- [Probe](#probe)
- [Templates](#templates)
- [Instance](#instance)
- [Validation](#validation)

## Vocabulary

Every closed enum of the grammar, with the shipped profiles that use each
entry.

| Axis | Vocabulary | Shipped profiles that use it |
|------|------------|------------------------------|
| Auth mode (`auth.accepts`, instance `auth.mode`) | `none` | pypi, crates_io, go_modules |
| | `bearer` (static token) | github, cloudflare, onepassword, slack, okta, runzero, gcp |
| | `api_key` (named header with a prefix, or a query parameter) | okta (`SSWS`) |
| | `basic` | none shipped |
| | `oauth2_client_credentials` (cached until shortly before expiry; per-unit `auth.scope`; `expose` allow-lists token-response fields for templates) | azure, m365, bitwarden, crowdstrike, salesforce, runzero |
| | `jwt_bearer` (RFC 7523 RS256 assertion from a service-account key, a key file or a bare PEM; claim templates) | gcp, gcp_pubsub, google_workspace, salesforce |
| | `gce_metadata` (the workload's token from the metadata server) | gcp, gcp_pubsub |
| | `duo_hmac` (Duo's HMAC-SHA1 request signing) | duo |
| | `sigv4` (AWS Signature v4, service and region rendered per request) | aws, object_store |
| Pager (`paginate.strategy`) | `none` | runzero, azure `log_analytics` |
| | `link_header` (RFC 5988 `rel="next"`) | github, okta |
| | `cursor` (a token from the body or a header, injected into `query:`, `body:`, `body_replace:` or the path; a list cursor is comma-joined) | aws, bitwarden, crowdstrike, duo, gcp, google_workspace, onepassword, slack |
| | `page_number` (bounded by `total_pages_at`) | cloudflare |
| | `offset` (advanced by rows received, bounded by `total_at` or a short page) | crowdstrike |
| | `request_path` (the next URL from the body or a header, relative resolved against the page) | azure, m365, salesforce |
| Decoder (`rows.decoder`) | `json_array` (streamed) | github, okta, m365 |
| | `ndjson` (streamed) | runzero, object_store |
| | `json_at` (a wrapped page, rows at a JSON pointer) | aws, azure, bitwarden, cloudflare, crowdstrike, duo, gcp, gcp_pubsub, google_workspace, onepassword, salesforce, slack |
| | `document` (the whole body is one row) | pypi, crates_io, go_modules, aws `cloudwatch_metrics` |
| | `json` (an array is its elements, anything else one row) | object_store |
| | `lines` (plain text, one row per line) | go_modules, object_store |
| | `csv` (one object per record, optional header) | salesforce `event_log_file` |
| Row flags (`rows.*`) | `gzip` (inflate a gzip member sequence before framing), `quoted` (each row is a JSON string holding the row), `header` (CSV) | object_store, aws `config`, salesforce |
| Construct (`construct.*`, at most one of the last three per unit) | `keyset` (one page sequence per key of a list, from a template or a first request) | aws, azure, pypi, crates_io, go_modules |
| | `lookup` (a second request per batch of ids the pages yield, with its own rows and pager) | aws `guardduty`, aws `cloudwatch_metrics`, crowdstrike |
| | `manifest` (a second request per item the pages point at; `key` and `position` give each row an item checkpoint) | m365, object_store, salesforce, go_modules |
| | `queue` (rows carry an ack id; the ack request goes out after delivery) | gcp_pubsub |
| `prelude` | idempotent requests sent once per tick before the first page | m365 (the subscription start) |
| Row builder (`rows.builder`, or `fold` for a folding one) | `columnar_table` | azure `log_analytics` |
| | `cloudwatch_metrics` | aws `cloudwatch_metrics` |
| | `go_module_aggregate` (a fold) | go_modules |
| | `wrap_non_object` | object_store |
| | `pubsub_message` | gcp_pubsub |
| Lister (`lister`) | `s3` (`ListObjectsV2`, the keys modified after the unit's checkpoint) | object_store |
| Window (`window.format`) | `rfc3339_secs`, `rfc3339_millis`, `epoch_secs`, `epoch_millis`, `strftime:<pattern>`; `step` chunks the window, `lookback` is the window when the scheduler passes none; a unit may override the format | every incremental profile |
| Method | `GET` (retried by default), `POST` (retried only with `retry.retry_non_idempotent`) | every profile |
| Shape (`shape`, per profile or per endpoint) | `incremental` (rows are events in the window), `dump` (rows are the whole store, enveloped) | runzero is the shipped dump |
| Probe | the cheapest authenticated call, with an optional `fail_when`; without one the health check resolves the credential (and mints a token) | github, okta, cloudflare, duo, slack, onepassword, aws, pypi, crates_io, go_modules |

## Profile

| Field | Meaning |
|-------|---------|
| `profile` | Registry key of a shipped profile; an inline profile may leave it empty. |
| `maturity` | `alpha`, `beta` or `stable`; the fetcher warns at startup for an enabled non-stable source. |
| `base_url` | Template; must reference a variable, never a literal URL, because a profile carries no identity. Rendered per unit, so a unit's `vars` can pick the host. |
| `shape` | Default shape of every endpoint (`incremental` or `dump`). |
| `auth` | Accepted modes and the shape of each ([Auth](#auth)). |
| `headers` | Header templates on every request. |
| `retry`, `error`, `quota` | [Retry, errors and quota](#retry-errors-and-quota). |
| `window` | [Window](#window). |
| `probe` | The health-check request ([Probe](#probe)). |
| `defaults` | Values every endpoint inherits unless it sets its own ([Endpoints](#endpoints)). |
| `endpoints` | The units. |
| `vars` | Default `vars` an instance may override. |

## Auth

`auth.accepts` lists the modes an instance may pick; the sibling keys carry
the shape of each mode. Identity (tokens, keys, secrets) lives on the
instance, never here.

| Mode shape | Fields |
|------------|--------|
| `api_key` | `header` (the header carrying the key), `query` (the query parameter carrying it), `prefix` (text put in front of the key in a header, e.g. `SSWS `). |
| `oauth2_client_credentials` | `token_url` (template), `scope` (omitted from the form when empty), `expires_in_fallback_secs` (lifetime assumed when the response carries no `expires_in`), `early_refresh_secs`, `expose` (top-level token-response fields the templates read as `auth.<name>`; `access_token`, `refresh_token` and `id_token` are refused). The exchange posts `grant_type=client_credentials`, `client_id`, `client_secret` and the scope as a form. |
| `jwt_bearer` | `token_url` (template; may read `auth.token_uri` from a service-account key), `claims` (templates for `iss`, `scope`, `aud` and an optional `sub`; a claim that renders empty is left out), `ttl_secs` (`exp - iat`), `expires_in_fallback_secs`, `early_refresh_secs`, `expose`. The authenticator exposes `client_email` and `token_uri` from a service-account key and `token_url` once rendered. |
| `gce_metadata` | `url` (the service account's token URL on the metadata server, a template), `expires_in_fallback_secs`, `early_refresh_secs`. |
| `sigv4` | `service` and `region` (both templates, rendered per request from the unit's context; the body's SHA-256 is the payload hash). |
| `bearer`, `basic`, `duo_hmac`, `none` | No profile-side shape. |

A mode that mints a token for a scope (`oauth2_client_credentials`,
`jwt_bearer`) lets a unit ask for its own with `endpoints[].auth.scope`; units
with the same scope share one token.

## Retry, errors and quota

| Field | Meaning |
|-------|---------|
| `retry.max_retries` | Attempts after the first. |
| `retry.min_backoff_ms`, `retry.max_backoff_ms` | First backoff and the ceiling. |
| `retry.retry_on` | Statuses that are retried: a code, or a class such as `5xx`. |
| `retry.never_retry` | Statuses never retried whatever `retry_on` says (every shipped profile lists 401 and 403). |
| `retry.retry_after_header` | Honour `Retry-After` on a retried response. |
| `retry.retry_non_idempotent` | Retry POST as well as GET; off by default, set where every POST of the profile is a read. |
| `retry.throttle_when` | What a refusal meaning "slow down" looks like where the status does not say so (`status` and `body_contains`, as AWS answers 400 with a `ThrottlingException` body): retried whatever `retry_on` lists, and counted `throttle` rather than a client error. A status in `never_retry` is never one. |
| `error.at` | JSON pointer to the error text in a non-2xx body; the whole body is used when unset. |
| `quota.headers` | Gauge-name suffix -> response header, surfaced as `dfe_fetcher_api_quota_<suffix>` labelled by source. |

A final retry failure fails the tick, and a failed tick does not advance the
window. A status listed in an endpoint's `ignore_status` is neither retried nor
counted as an API error.

## Window

| Field | Meaning |
|-------|---------|
| `window.format` | How `window.start` and `window.end` render: `rfc3339_secs`, `rfc3339_millis`, `epoch_secs`, `epoch_millis` or `strftime:<pattern>`. |
| `window.step` | Split the window into steps of at most this length (`<n>s|m|h|d`), one page sequence each. |
| `window.lookback` | Window used when the scheduler passes none. |
| `endpoints[].window.format` | A unit's own rendering when its API differs from the profile's. |

A unit whose templates read no `window` runs once per tick, whatever the
step. A typed epoch number comes from the CEL cast: `"{{ int(window.start) }}"`
renders a JSON number.

## Endpoints

Each entry of `endpoints` is one unit of the source. `defaults` carries the
same keys (except `unit`, `shape`, `base_url`, `auth`, `window`, `vars`,
`row_key`, `fail_when`, `add_fields`, `fold`, `lister`, `ignore_status`,
`timeout_secs`) and
every endpoint inherits them unless it sets its own; a `defaults.body` reaches
POST endpoints only, and an endpoint's own `prelude: []` or `construct: {}`
opts out of the defaults'.

| Field | Meaning |
|-------|---------|
| `unit` | Unit name: the `_source_fetcher` suffix, the dump `store` suffix, the topic suffix. |
| `shape` | Overrides the profile's shape for this unit. |
| `base_url` | This unit's base URL template when its host differs from the profile's. |
| `auth.scope` | The scope (OAuth2) or `scope` claim (JWT bearer) this unit's token is minted for. |
| `window.format` | This unit's window rendering. |
| `vars` | Values this unit's templates read as `vars.*`, over the instance's and under the instance's `units.<name>.vars`. |
| `method` | `GET` or `POST`; the defaults' when unset, else GET. |
| `path` | Path template appended to `base_url`; `{{ unit.name }}` lets one default path serve many units. |
| `query`, `headers` | Templates; an empty query rendering is omitted. |
| `body` | JSON body for POST; string leaves are templates, a leaf that is one expression keeps its type, a member that renders `null` is left out. |
| `rows` | Row framing ([Rows](#rows)). |
| `row_key` | JSON pointer to the row's identity, for the oversize stub and logs. |
| `paginate` | Pagination ([Pagination](#pagination)). |
| `fail_when` | CEL over a 2xx body; true fails the tick with the text at `error.at`. |
| `max_pages` | Page ceiling per tick. |
| `rate` | The provider's rate limit for this unit's requests (`requests_per_sec`, a fraction for a limit slower than one a second), held to across its pages, window steps and ticks; the defaults' when unset, and unpaced when neither sets one. |
| `timeout_secs` | Total bound on one request of this endpoint, its body included. Refused on a unit whose decoder streams: a streamed store takes as long as it takes and the client's idle read timeout bounds each read instead. |
| `max_page_bytes` | Bytes a page-bounded decoder may buffer. |
| `add_fields` | Fields added to every row: a template, or a JSON object or array whose string leaves are templates; one that reads `key` renders per key of the unit's keyset. |
| `fold` | A folding row builder: the rows of one key become the one row it builds. |
| `lister` | A listing protocol standing in for the page fetch. |
| `construct` | How the unit's requests are multiplied ([Constructs and prelude](#constructs-and-prelude)). |
| `prelude` | Requests sent once per tick before the first page. |
| `ignore_status` | Non-2xx statuses answered as an empty page instead of a failure. |

## Rows

| Field | Meaning |
|-------|---------|
| `rows.decoder` | The framing: `json_array`, `ndjson`, `json_at`, `document`, `json`, `lines`, `csv`. `json_at`, `document`, `json` and `csv` read the whole page into memory, bounded by `max_page_bytes`; the others stream. |
| `rows.at` | JSON pointer to the row array (`json_at` only); an absent pointer is an empty page. |
| `rows.gzip` | The body is a gzip member sequence to inflate before framing (a `Content-Encoding: gzip` response is inflated by the client regardless). |
| `rows.header` | The first CSV record names the columns. |
| `rows.quoted` | Each framed row is a JSON string whose content is the row; it is unquoted first. |
| `rows.builder` | A row builder applied to each framed row: `columnar_table`, `cloudwatch_metrics`, `wrap_non_object`, `pubsub_message`. The folding builder `go_module_aggregate` is named under `fold`, never here. |

## Pagination

| Field | Meaning |
|-------|---------|
| `paginate.strategy` | `none`, `link_header`, `cursor`, `page_number`, `offset`, `request_path`. |
| `paginate.from` | Where the token or URL comes from: `body:/pointer` or `header:Name`. |
| `paginate.into` | Where the token goes on the next request: `query:name`, `body:/pointer`, `body_replace:/pointer` (the next body is the token and nothing else) or `path`. |
| `paginate.stop_when` | CEL over `body` and `headers`; true stops after this page. |
| `paginate.total_pages_at` | JSON pointer to the total page count (`page_number`). |
| `paginate.total_at` | JSON pointer to the total row count (`offset`). |
| `paginate.page_size` | Rows per page (`offset`); a shorter page is the last; optional when `total_at` is set. |
| `paginate.param` | Query parameter carrying the page number or offset. |
| `paginate.start` | First page number or first offset. |
| `paginate.base` | Base URL a relative `request_path` value is resolved against; without one it resolves against the page's own URL. |

A cursor that is a list of scalars is sent comma-joined; an empty list, null
or a list of objects ends the sequence.

## Constructs and prelude

A unit takes at most one of `lookup`, `manifest` and `queue`; `keyset` can sit
beside any of them. A secondary request (a keyset request, a lookup batch, a
manifest item, a prelude step) is a `LookupRequest`: `method` (unset takes the
construct's default: POST for a lookup batch, a keyset request and a prelude
step, GET for a manifest item), `path` (appended to `base_url`, or a whole URL
when it renders one), `query`, `headers`, `body` and `ignore_status`.

| Construct | Fields | Context |
|-----------|--------|---------|
| `construct.keyset` | `from` (a template rendering the list of keys) or `request` plus `keys_at` (the keys come from one request the unit sends first); exactly one. One page sequence per key. | `{{ key }}` in the unit's templates and `add_fields`. |
| `construct.lookup` | `id_at` (pointer into each first-stage row to its id; unset when the row is the id), `batch`, `request`, `rows`, `paginate`, `max_pages`. A second request per batch of ids; a lookup whose request reads `key` flushes its ids before the next key. | `{{ ids }}` in the batch request. |
| `construct.manifest` | `item_request`, `rows`, `key` and `position` (templates of the item's identity and RFC 3339 position, set together, giving every row an item checkpoint mark), `max_items` (items per key per tick; the rest wait), `add_fields` (rendered per item). The pages' rows are pointers; the unit's rows are what each item answers. | `{{ item }}` in the item request and fields. |
| `construct.queue` | `ack_at` (pointer into each framed row to its ack id), `ack_request` (POST by default), `ack_batch` (ids per acknowledgement). The ack request is sent for each batch of ids the driver hands back after the batch is delivered. | `{{ ids }}` in the ack request. |
| `prelude` | A list of `LookupRequest`s sent once per tick before the unit's first page, rendered from the unit's vars alone (never the window, a key or an item). | -- |

## Probe

`probe` names the API's cheapest authenticated call: `method`, `path`,
`query` and an optional `fail_when` (CEL over the 2xx body, for an API that
answers a bad credential inside a 200). Without a probe the health check
resolves the credential and, for a minting mode, exchanges it for a token.

## Templates

Templates are `{{ cel }}` expressions over:

| Name | Available |
|------|-----------|
| `vars.*` | The profile's defaults, overlaid by the instance's `vars`, the endpoint's `vars`, then the instance's `units.<name>.vars`. |
| `base_url` | The rendered base URL (in `token_url` templates). |
| `window.start`, `window.end` | The step's bounds, rendered per `window.format`; `int(...)` gives a number. |
| `unit.name` | The unit's name. |
| `key` | The current keyset key. |
| `item` | The current manifest item. |
| `ids` | The current lookup or ack batch, a typed list. |
| `auth.*` | What the mode exposes: `client_email`, `token_uri`, `token_url` for a JWT bearer, plus the token-response fields listed under `expose`. |

A body leaf that is exactly one expression keeps its JSON type; a mixed leaf
is a string. A rendered path that is a whole URL is used as is.

## Instance

An instance (`sources.rest.<id>`, or a typed block mapped at load) carries:

| Field | Meaning |
|-------|---------|
| `enabled` | Whether the instance runs. |
| `profile` | A shipped profile by name, or the profile itself inline. |
| `interval_secs` | Fetch interval; the scheduler default when unset. |
| `topic` | Topic base; dump units land on `<topic>-<unit>`. |
| `filter` | CEL keep-filter over each row, hot-reloaded. |
| `auth` | The mode and its credentials (below). |
| `vars` | Values the profile's templates read as `vars.*`. |
| `units` | Per-unit narrowing, keyed by unit name (below). |
| `accumulate` | Batch bounds for this instance; the deployment's when unset. |

Every secret in `auth` is a credential spec (`vault:<mount>/data/<path>:<key>`,
`env:VAR`, or a literal) resolved when first used:

| Mode | Instance fields |
|------|-----------------|
| `bearer` | `token` |
| `api_key` | `key` |
| `basic` | `username`, `password` |
| `oauth2_client_credentials` | `client_id` (the literal id), `client_secret`, optional `scope` overriding the profile's |
| `duo_hmac` | `integration_key`, `secret_key` |
| `jwt_bearer` | exactly one of `service_account_key` (a Google-style key JSON as a spec), `service_account_key_file` (a spec resolving to the path of such a file), `private_key` (a bare RSA PEM for an API whose issuer and audience come from `vars`) |
| `gce_metadata`, `none` | nothing |
| `sigv4` | `access_key_id` and `secret_access_key`, or `credentials_json` alone (a document carrying both, in either the snake_case or the AWS `AccessKeyId` / `SecretAccessKey` spelling) |

`vault:`, `bao:`, `openbao:`, `env:` and `file:` resolve, and the prefix is
matched exactly: an `aws:` spec, which needs a secrets feature the fetcher does
not build, or a near miss such as `Vault:`, is refused at load naming the field
and the prefix rather than reaching the provider as literal text.

`units.<name>` narrows one unit: `enabled`, `endpoint` (instantiate a
profile endpoint under this name with these overrides, so one endpoint serves
many buckets or subscriptions, tagged `<connection>.<name>`), `topic` (in
place of the instance's), `query` and `headers` (merged over the endpoint's),
`vars` (over the instance's for this unit).

## Validation

Binding an instance to its profile reports every problem with its field path
rather than the first: a mode the profile does not accept, a missing
credential field for the mode, a `units.<name>` the profile has no unit for
-- the refusal names the units the profile declares -- or one that
instantiates an endpoint under a name the profile already declares,
an empty `topic`, a filter or template that does not compile, a `base_url`
that is a literal, a pointer that does not start with `/`, an `offset` pager
without `page_size` or `total_at`, a keyset with both or neither of `from` and
`request`, `key` in `add_fields` without a keyset, a queue unit that also
declares a lookup or manifest, a `defaults.body` on a GET, and an `expose`
entry naming a credential.
