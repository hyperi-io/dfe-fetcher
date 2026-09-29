<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/bitwarden.md         -->
<!-- Purpose:   Bitwarden cloud admin setup guide      -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Bitwarden Setup for dfe-fetcher

What a Bitwarden organization owner needs to configure so dfe-fetcher can
read organization event logs from the Bitwarden Public API.

## Overview

dfe-fetcher pulls organization event logs from the Bitwarden Public API at
`/public/events` - member, collection, group, vault, and policy actions
across the organization. The endpoint is poll-based (Bitwarden offers no
outbound webhooks). dfe-fetcher authenticates with the organization API key
using the OAuth2 client_credentials flow: it exchanges `client_id` +
`client_secret` (scope `api.organization`) at `/identity/connect/token` for a
bearer token, then GETs `/public/events` with a `start`/`end` window and
follows the `continuationToken` for pagination.

The source is the shipped `bitwarden` profile
(`crates/fetcher/profiles/bitwarden.yaml`); the `sources.bitwarden` block
below maps onto an instance of it at load. The token is cached per instance
and refreshed shortly before it expires; a 429 or 5xx is retried with backoff
(honouring `Retry-After`), a refused exchange or a 401 or 403 ends the tick,
and a tick that fails does not advance the fetch window. A missing client id
or secret is refused at load, naming `sources.bitwarden`.

## Prerequisites

- A **Bitwarden Teams or Enterprise** organization (the Public API is
  available to Teams and Enterprise plans, not Free or Families).
- The setup must be done by an organization **Owner** - only an owner can
  view or rotate the organization API key.
- Works on Bitwarden Cloud (US or EU) and self-hosted servers.
- No paid add-on beyond the Teams/Enterprise subscription.

## Required Permissions

| Service | Object/Endpoint | Permission or Scope | Notes |
|---------|-----------------|---------------------|-------|
| Identity | `/identity/connect/token` | grant_type `client_credentials`, scope `api.organization` | Token lives ~60 min; dfe-fetcher caches and refreshes it |
| Public API | `/public/events` | Organization API key | Read-only event log; GET with `start`/`end`/`continuationToken` |

The credential is the **organization** API key, whose `client_id` has the
form `organization.<uuid>`. A personal API key (`user.<uuid>`) will not work
against the Public API. Access is read-only.

## Source-Side Setup

1. **Retrieve the organization API key**

   In the Bitwarden web vault, open the **Admin Console** for the
   organization. Go to **Settings** -> **Organization info** and scroll to
   the **API key** section, then select **View API Key** (you may be prompted
   to re-enter your master password). Copy the **client_id** (format
   `organization.<uuid>`) and **client_secret**. To rotate, use **Rotate API
   Key** on the same screen - this invalidates the old secret.

2. **Confirm event logging is on**

   Event logs are an organization feature; ensure events are enabled for the
   organization (Admin Console -> Settings). If `/public/events` returns 403,
   verify the account permissions and that the Events feature is enabled.

3. **Identify your region / host**

   | Deployment | API base (`api_url_override`) | Identity/token (`identity_url_override`) |
   |------------|-------------------------------|------------------------------------------|
   | Cloud US (default) | `https://api.bitwarden.com` | `https://identity.bitwarden.com/connect/token` |
   | Cloud EU | `https://api.bitwarden.eu` | `https://identity.bitwarden.eu/connect/token` |
   | Self-hosted | `https://your-host/api` | `https://your-host/identity/connect/token` |

   US cloud uses the built-in defaults, so no overrides are needed there.

4. **Smoke-test the token exchange**

   ```bash
   curl -s https://identity.bitwarden.com/connect/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d grant_type=client_credentials \
     -d "client_id=organization.<uuid>" \
     -d "client_secret=<secret>" \
     -d scope=api.organization
   ```

   A successful response contains `access_token` and `expires_in` (typically
   3600). Use that bearer token against
   `https://api.bitwarden.com/public/events` to confirm event access.

## dfe-fetcher Configuration

### Config File

```yaml
sources:
  bitwarden:
    enabled: true
    client_id: "organization.<uuid>"
    client_secret: "your-organization-api-client-secret"
    # Cloud EU or self-hosted only:
    # api_url_override: "https://api.bitwarden.eu"
    # identity_url_override: "https://identity.bitwarden.eu/connect/token"
    services:
      - name: events
    topic: "bitwarden"
    # filter: 'type < 1500'   # optional, hot-reloaded
```

### Environment Variables

```bash
DFE_FETCHER_SOURCES__BITWARDEN__ENABLED="true"
DFE_FETCHER_SOURCES__BITWARDEN__CLIENT_ID="organization.<uuid>"
DFE_FETCHER_SOURCES__BITWARDEN__CLIENT_SECRET="your-organization-api-client-secret"
# Cloud EU or self-hosted only:
DFE_FETCHER_SOURCES__BITWARDEN__API_URL_OVERRIDE="https://api.bitwarden.eu"
DFE_FETCHER_SOURCES__BITWARDEN__IDENTITY_URL_OVERRIDE="https://identity.bitwarden.eu/connect/token"
```

### Secrets Manager

Keep the client secret out of the config file:

```yaml
sources:
  bitwarden:
    enabled: true
    client_id: "organization.<uuid>"
    credential_secret: "vault:kv/data/bitwarden:client_secret"
    services:
      - name: events
    topic: "bitwarden"
```

`credential_secret` takes precedence over a literal `client_secret` and
resolves to the secret string. `client_id` is always supplied in clear (it is
not sensitive). The only service is `events`; it has no service-side config
keys.

## Verification

The health check is the OAuth2 token exchange: a minted token proves the
client, and no data endpoint is touched. A refused exchange is a health error
carrying the response.

End-to-end smoke tests live in `crates/fetcher/tests/e2e/smoke_remote.rs`
and are `#[ignore]`d by default. They read `BITWARDEN_CLIENT_ID` and
`BITWARDEN_CLIENT_SECRET` (required), plus `BITWARDEN_API_BASE` and
`BITWARDEN_IDENTITY_URL` (optional region/self-host overrides), from
`.env-cloud` (or `.env`):

```bash
cargo test -p dfe-fetcher --test e2e bitwarden_ -- --ignored
```

Common failure modes:

- **401 on token exchange / health check fails** - wrong `client_id` or
  `client_secret`, a rotated secret, or a personal (`user.<uuid>`) key.
- **403 on `/public/events`** - account lacks permission or event logging is
  not enabled for the organization.
- **Wrong region** - EU tenant left on the US default; set both
  `api_url_override` and `identity_url_override`.
- **HTTP 429** - rate limited; dfe-fetcher retries with backoff and
  `Retry-After` up to the profile's policy, then fails the tick without
  advancing; the page ceiling is 50 per fetch.

## Cost

dfe-fetcher only reads. The Public API and `/public/events` reads add no
charge of their own, but access may require a paid Bitwarden plan (the
Events/Public API is an organization feature). Fair-use server-side rate
limits apply. Confirm any cost implications against your own Bitwarden
plan.

## References

- Bitwarden Public API (auth, regional endpoints): <https://bitwarden.com/help/public-api/>
- Public API overview (contributing docs): <https://contributing.bitwarden.com/getting-started/server/public-api/>
- Password Manager APIs index: <https://bitwarden.com/help/bitwarden-apis/>
- Bitwarden Labs events Public API client (reference tool): <https://github.com/bitwarden-labs/events-public-api-client>
