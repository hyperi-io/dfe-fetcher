<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/runzero.md           -->
<!-- Purpose:   runZero admin setup guide             -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                              -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# runZero Setup for dfe-fetcher

What a runZero administrator configures so dfe-fetcher can pull an
organisation's asset inventory from a runZero console (read-only, pull-mode).

## Overview

dfe-fetcher reads the runZero export API: one `GET /export/org/<store>.jsonl`
per store, streamed one JSON object per line, gzip on the wire. Nothing is
incremental. Every tick fetches each store whole and lands it as a snapshot:
`begin`, one `row` frame per object, `end` with the row count, all stamped with
one `snapshot_id`, on the store's own topic `<topic>-<store>` (the frames and
the consumer rules are in
[../reference/snapshot-envelope.md](../reference/snapshot-envelope.md)). A
consumer rebuilds a complete inventory by snapshot id and can tell a truncated
export from a whole one.

The source is the shipped `runzero` profile
(`crates/fetcher/profiles/runzero.yaml`), configured as an instance under
`sources.rest`. A self-hosted console and the cloud console are two instances
of that one profile with different identities:

- **Self-hosted, export token.** The organisation's export token as a static
  bearer. Every store accepts it. This is the data path.
- **Cloud (or self-hosted), OAuth2 API client.** A client id and secret
  exchanged at `<console>/api/v1.0/account/api/token`, with the organisation
  id sent as `_oid` on every call. The console refuses four stores to an API
  client whatever its grants, and the cloud console refuses every export
  unless the client carries an inventory grant. Use this path for the account
  API and for stores an API client may read; leave the refused stores off.

A refused export (401 or 403) ends the tick and is never retried: the console
doubles a server-side throttle delay on every refusal, so repeating the call
lengthens the penalty.

| Store | Row key | Export token | OAuth2 client with Inventory read |
|-------|---------|--------------|-----------------------------------|
| `assets` | `/id` | yes | yes |
| `sites` | `/id` | yes | yes |
| `certificates` | `/id` | yes | yes |
| `services` | `/service_id` | yes | yes |
| `software` | `/software_id` | yes | yes |
| `vulnerabilities` | `/vulnerability_id` | yes | yes |
| `wireless` | `/id` | yes | yes |
| `findings` | `/finding_code` | yes | refused |
| `tasks` | `/id` | yes | refused |
| `users` | `/id` | yes | refused |
| `groups` | `/id` | yes | refused |

The row key names the object's identity for the oversize stub and the logs.
On the asset joins (`services`, `software`, `vulnerabilities`) the parent
asset is denormalised onto every row and `id` is the asset's, so the row key
is the store's own id.

## Prerequisites

- A runZero console: self-hosted, or the runZero cloud platform.
- An organisation in that console whose inventory you want to export, and a
  console administrator who can edit the organisation and create API clients.
- Network egress from the fetcher host to the console's API.

## Required Permissions

| Path | Credential | Where it is created | Grants |
|------|------------|---------------------|--------|
| Export token | The organisation's export token, sent as `Authorization: Bearer` | Organizations -> the organisation -> Export token | Read-only access to every export of that one organisation; bound to it, so no `_oid` is needed |
| OAuth2 API client | A client id and secret exchanged for a bearer at `/account/api/token` | Account -> API clients | The client's own permission scope; `Inventory: read` is what the exports check. Bind it to the organisation or send `_oid`. |

Both paths are read-only. dfe-fetcher never scans, creates, modifies or deletes
anything on the console. The export token cannot reach the account API and
the API client cannot reach the four refused stores, which is why the two
paths coexist.

## Source-Side Setup

1. **Create or pick the organisation.** In the console, open **Organizations**
   and note the organisation's id (the UUID in its URL). The OAuth2 path sends
   it as `_oid`.

2. **Generate the export token.** Edit the organisation and, under **Export
   token**, generate one. Copy it now and store it in your secrets manager;
   the console shows it on that page and regenerating it invalidates the old
   one.

3. **Create the API client** (OAuth2 path only). Under **Account -> API
   clients**, create a client named `dfe-fetcher` with the **Inventory: read**
   permission and no write permission. Copy the client id and the client
   secret; the secret is shown once.

4. **Smoke-test the export token** against the self-hosted console:

   ```bash
   printf 'header = "Authorization: Bearer %s"\n' "$RUNZERO_EXPORT_TOKEN" |
     curl -sS --compressed --config - \
       "https://runzero.example.internal/api/v1.0/export/org/sites.jsonl" |
     head -c 400
   ```

   The token reaches curl through `--config -` rather than an argument: a
   command line is readable by any user through `ps` and is kept in shell
   history, and `printf` is a shell builtin, so it starts no process of its own.

5. **Smoke-test the API client** (OAuth2 path):

   ```bash
   printf 'data-urlencode = "client_secret=%s"\n' "$RUNZERO_CLIENT_SECRET" |
     curl -sS -X POST --config - \
       "https://console.runzero.example/api/v1.0/account/api/token" \
       -d grant_type=client_credentials \
       -d "client_id=$RUNZERO_CLIENT_ID"
   ```

   The client secret takes the same `--config -` route, for the same reason;
   the client id is not a secret and stays on the command line.

   A success response carries `access_token` and `expires_in`. Use the token
   against `/export/org/sites.jsonl?_oid=<organisation id>` to confirm the
   inventory grant.

## dfe-fetcher Configuration

The instance lives under `sources.rest` and is keyed by its connection id
(the cursor key, the metric and log label, and the `_source_fetcher` prefix:
`runzero_self_hosted.assets`). `vars.base_url` has no default -- every
instance names its console. `vars.org_id` defaults to empty, so an export-token
instance leaves it unset and `_oid` is omitted; an OAuth2 instance must set
it, or the console answers 400.

### Config File

The stanza below is the one in
[config.example.yaml](../../config.example.yaml), which stays the annotated
reference:

```yaml
sources:
  rest:
    runzero_self_hosted:
      enabled: true
      topic: "runzero"                  # stores land on runzero-assets, runzero-services, ...
      interval_secs: 3600
      # filter: 'record.alive == true'  # CEL over the enveloped row; hot-reloaded
      auth:
        mode: bearer
        token: "vault:kv/data/runzero/self_hosted:export_token"   # the organisation's export token
      vars:
        base_url: "https://runzero.example.internal/api/v1.0"     # required, no default
      # units:                           # optional per-store narrowing
      #   assets: { query: { fields: "id,alive,addresses,names,os,type" } }
      #   services: { query: { search: "alive:t" } }
      #   wireless: { enabled: false }
      profile: runzero
    runzero_cloud:
      enabled: true
      topic: "runzero-cloud"            # or "runzero" to land in the same tables, told apart by _source_fetcher
      interval_secs: 86400
      auth:
        mode: oauth2_client_credentials
        client_id: "00000000-0000-0000-0000-000000000000"          # the API client's id (not a secret)
        client_secret: "vault:kv/data/runzero/cloud:client_secret"
      vars:
        base_url: "https://console.runzero.example/api/v1.0"
        org_id: "00000000-0000-0000-0000-000000000000"             # required with OAuth2: 400 without it
      units:
        findings: { enabled: false }    # refused to an API client; a refusal ends the tick
        tasks: { enabled: false }
        users: { enabled: false }
        groups: { enabled: false }
      profile: runzero
```

`units.<store>.query` merges query parameters over the export request, so the
console's own `fields` and `search` narrowing applies server-side; a store
with `enabled: false` is not fetched. An instance-level `accumulate` block
overrides the deployment's batch bounds.

### Environment Variables

Scalar fields follow the `DFE_FETCHER_SOURCES__REST__<ID>__<FIELD>` pattern
(double underscores, nested keys joined the same way):

```bash
DFE_FETCHER_SOURCES__REST__RUNZERO_SELF_HOSTED__ENABLED="true"
DFE_FETCHER_SOURCES__REST__RUNZERO_SELF_HOSTED__AUTH__TOKEN="env:RUNZERO_EXPORT_TOKEN"
DFE_FETCHER_SOURCES__REST__RUNZERO_SELF_HOSTED__VARS__BASE_URL="https://runzero.example.internal/api/v1.0"
```

Define the instance in the config file and inject only the credential through
the environment; a credential field takes an `env:VAR` spec, so the secret
stays out of the YAML.

### Secrets Manager

Every credential field is a spec: `token` and `client_secret` above are
`vault:<mount>/data/<path>:<key>` references resolved when first used.
`client_id` is the API client's id and not a secret; write it as a literal.

### Topics

Each store lands on `<topic>-<store>` with the deployment's topic suffix
appended, so the self-hosted instance above produces `runzero-assets_land`,
`runzero-services_land` and so on, and the cloud instance
`runzero-cloud-assets_land`. Two instances on one `topic` share tables and
are told apart by `_source_fetcher` (`<connection id>.<store>`) and by
`store` in the envelope.

## Verification

1. **Health check.** With the export token the health check resolves the
   credential; with an OAuth2 client it also mints a token at
   `/account/api/token`, so a healthy result proves the client id and secret.
   Neither proves a store is readable -- the first tick does.

2. **Live tests.** `crates/fetcher/tests/e2e/runzero.rs` binds the shipped
   profile to a self-hosted and a cloud console, streams a store through the
   driver into a Kafka broker and rebuilds the snapshot to the export's row
   count. The tests are `#[ignore]` by default and read
   `RUNZERO_SELFHOSTED_CONSOLE_URL`, `RUNZERO_SELFHOSTED_EXPORT_TOKEN`,
   `RUNZERO_SELFHOSTED_CLIENT_ID`, `RUNZERO_SELFHOSTED_CLIENT_SECRET`,
   `RUNZERO_SELFHOSTED_ORG_ID`, `RUNZERO_CLOUD_CONSOLE_URL`,
   `RUNZERO_CLOUD_CLIENT_ID`, `RUNZERO_CLOUD_CLIENT_SECRET` and, optionally,
   `RUNZERO_CLOUD_TOKEN_ENDPOINT` from `.env-cloud`:

   ```bash
   cargo test -p dfe-fetcher --test e2e runzero -- --ignored
   ```

3. **Watch the first tick.** The driver logs `unit tick complete` per store
   with `rows`, `filtered`, `oversize` and `flushes`; the
   `dfe_fetcher_snapshots_total{store,status}` counter increments `complete`
   per store per tick, and the console's usage headers surface as the
   `dfe_fetcher_api_quota_usage_today` and `dfe_fetcher_api_quota_usage_total`
   gauges.

4. **Common failures.**
   - `401` on every store: the export token is wrong or was regenerated, or
     the OAuth2 exchange used the wrong client secret.
   - `403` on `findings`, `tasks`, `users` or `groups` with an OAuth2 client:
     those stores take an export token only; disable them on that instance.
   - `403` on every store on the cloud console: the API client has no
     inventory grant.
   - `400` with an OAuth2 client: `vars.org_id` is unset.
   - A store lands as an empty snapshot (`begin` then `end` with `row_count`
     0): the export answered no rows, which is the store's state, not a
     failure.

## Cost

dfe-fetcher only reads. Exports carry no charge of their own, but the API
usage counters the console reports (`x-api-usage-today`,
`x-api-usage-total`) count every call, each store is fetched whole every
tick, and what your plan allows depends on your subscription. Size the
interval to how often the inventory changes rather than polling aggressively.
Confirm any cost implications against your own runZero agreement.

## References

- runZero API specification (the export endpoints, `_oid`, the token exchange): <https://app.swaggerhub.com/apis/runZero/runZero>
- runZero documentation (export tokens, API clients, organisations): <https://www.runzero.com/docs/>
