<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/crowdstrike.md       -->
<!-- Purpose:   CrowdStrike Falcon admin setup guide  -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   FSL-1.1-ALv2                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# CrowdStrike Setup for dfe-fetcher

What a CrowdStrike Falcon administrator needs to configure so dfe-fetcher
can read alert data from a Falcon tenant.

> Status: alpha - code-complete, not production-validated; behaviour and config may change.

## Overview

dfe-fetcher reads CrowdStrike alerts via the Falcon public API. It
authenticates with an **OAuth2 API client** (client ID + secret) using the
`client_credentials` grant against `/oauth2/token`, then sends the returned
bearer token to the data endpoints. Tokens are valid for 30 minutes and the
fetcher refreshes them automatically.

The `alerts` service is a **two-stage** call against the Alerts API:

1. `GET /alerts/queries/alerts/v2` - returns alert composite IDs for the time
   window (Falcon Query Language `created_timestamp` filter).
2. `POST /alerts/entities/alerts/v2` with `{"composite_ids": [...]}` - returns
   full alert entities for those IDs, in batches of up to 1000.

Falcon tenants live on different cloud regions, each with its own API host.
The admin's only job is to create an OAuth2 API client with the right scope
and tell dfe-fetcher which region to use.

> **History (2026).** CrowdStrike renamed "Detections" to "Alerts" and
> introduced the Alerts API. The legacy `/detects/*` endpoints were
> deprecated 2024-10-01 and decommissioned 2025-09-30. dfe-fetcher uses the
> Alerts API (`/alerts/queries/alerts/v2` + `/alerts/entities/alerts/v2`),
> which needs the **Alerts: Read** scope.

## Prerequisites

- A CrowdStrike Falcon subscription with the Falcon console.
- The **Falcon Administrator** role (required to view, create, or modify
  OAuth2 API clients and keys).
- Knowledge of which Falcon cloud region your tenant lives on (shown as the
  Base URL on the API clients page).

## Required Permissions

| Service | Object/Endpoint | Permission or Scope | Notes |
|---------|-----------------|---------------------|-------|
| Alerts | `GET /alerts/queries/alerts/v2` (stage 1) | **Alerts: Read** | Read-only. Returns alert composite IDs. |
| Alerts | `POST /alerts/entities/alerts/v2` (stage 2) | **Alerts: Read** | Read-only. Returns full alert entities. |
| OAuth2 token | `POST /oauth2/token` | (no scope - the API client itself) | `client_credentials` grant. Always required. |

Select only the scope your flow needs (least privilege). No write scope is
required.

## Source-Side Setup

1. **Sign in to the Falcon console** with a Falcon Administrator account, in
   the cloud region where your tenant lives.

2. **Open the API clients page.** Navigate to **Support and resources >
   Resources and tools > API clients and keys**. (Older consoles list this as
   **Support > API clients and keys**.)

3. **Create the API client.** Under **OAuth2 API clients**, click **Create
   API client**. Enter:
   - **Client name**: `dfe-fetcher`
   - **Description**: e.g. "dfe-fetcher alert pull (read-only)"

4. **Select scopes.** In the scopes table, tick **Read** for **Alerts**.
   Leave all **Write** boxes unchecked.

5. **Save and capture credentials.** Click **Create**. Falcon shows the
   **Client ID**, **Client Secret**, and **Base URL** once only. Copy all
   three now - the secret cannot be retrieved later (a reset is required if
   lost).

6. **Note the Base URL** for your region:

   | Region | Base URL |
   |--------|----------|
   | US-1 (default) | `https://api.crowdstrike.com` |
   | US-2 | `https://api.us-2.crowdstrike.com` |
   | EU-1 | `https://api.eu-1.crowdstrike.com` |
   | US-GOV-1 | `https://api.laggar.gcw.crowdstrike.com` |

7. **(Optional) Verify the token exchange** from a shell:

   ```bash
   curl -s -X POST "https://api.crowdstrike.com/oauth2/token" \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "client_id=<CLIENT_ID>" \
     -d "client_secret=<CLIENT_SECRET>"
   ```

   A successful response includes `access_token` and `expires_in` (1800).
   Replace the host with your region's Base URL.

## dfe-fetcher Configuration

The CrowdStrike source config fields are: `enabled`, `api_url_override`
(region base URL), `client_id`, `client_secret`, `credential_secret`,
`services`, `topic`. The only service is `alerts`.

### Config File

```yaml
sources:
  crowdstrike:
    enabled: true
    # Region base URL. Omit for US-1 default (https://api.crowdstrike.com).
    # api_url_override: "https://api.eu-1.crowdstrike.com"
    client_id: "your-oauth2-client-id"
    client_secret: "your-oauth2-client-secret"
    services:
      - name: alerts
        # config:
        #   limit: 100               # query page size, max 1000
        #   filter: "severity:>=70"  # extra Falcon Query Language clause
    topic: "crowdstrike"
    # filter: 'severity >= 70'  # hot-reloaded CEL expression
```

### Environment Variables

```bash
DFE_FETCHER_SOURCES__CROWDSTRIKE__ENABLED="true"
DFE_FETCHER_SOURCES__CROWDSTRIKE__API_URL_OVERRIDE="https://api.eu-1.crowdstrike.com"
DFE_FETCHER_SOURCES__CROWDSTRIKE__CLIENT_ID="your-oauth2-client-id"
DFE_FETCHER_SOURCES__CROWDSTRIKE__CLIENT_SECRET="your-oauth2-client-secret"
```

### Secrets Manager (Production)

Keep the client secret out of the config file with `credential_secret`:

```yaml
sources:
  crowdstrike:
    enabled: true
    # api_url_override: "https://api.us-2.crowdstrike.com"
    client_id: "your-oauth2-client-id"
    credential_secret: "vault:secret/dfe/crowdstrike:client_secret"
    services:
      - name: alerts
    topic: "crowdstrike"
```

`credential_secret` resolves to the OAuth2 client secret. When set, it takes
precedence over an inline `client_secret`.

## Verification

**Health check.** The source's `health_check` performs a token exchange only
(the cheapest valid auth probe). A healthy result means the client ID/secret
and region base URL are correct - it does NOT confirm the Alerts: Read scope.

**Live smoke tests.** `tests/e2e/smoke_remote.rs` has env-gated, `#[ignore]`d
tests that hit a real tenant. They read these variables (canonical
`.env-cloud`, fallback `.env`):

- `CROWDSTRIKE_CLIENT_ID`
- `CROWDSTRIKE_CLIENT_SECRET`
- `CROWDSTRIKE_API_BASE` (optional; maps to `api_url_override`)

Run them:

```bash
# Auth probe only (token exchange)
cargo nextest run --test e2e -- --ignored crowdstrike_health_check

# Full two-stage alert pull
cargo nextest run --test e2e -- --ignored crowdstrike_fetch_alerts
```

`crowdstrike_fetch_alerts` asserts every result is tagged
`crowdstrike.alerts`. Zero records is normal on a quiet tenant or short
lookback window (default 1 hour).

**Common failures.**

- `401`/token exchange fails - wrong client ID/secret, or the secret was
  rotated. Reset the secret in the console and update config.
- `403 Forbidden` on the alert query - the API client lacks the read scope
  (grant Alerts: Read).
- Token works but zero records every cycle - check the region base URL and
  that alerts exist in the lookback window; widen with a longer interval or a
  `filter` adjustment.

## Cost

dfe-fetcher only reads. OAuth2 API clients and Alerts API reads are
generally included with a Falcon subscription and not expected to carry an
additional charge, though this can depend on your CrowdStrike entitlements.
Confirm any cost implications against your own CrowdStrike agreement.

## References

- CrowdStrike - Falcon console API clients and keys (Support and resources):
  https://www.crowdstrike.com/blog/tech-center/get-access-falcon-apis/
- CrowdStrike Developer Center - OpenAPI docs and scopes:
  https://developer.crowdstrike.com/docs/openapi/
- CrowdStrike Detects API decommission and migration to Alerts API:
  https://cloud.google.com/chronicle/docs/detection/migrate-detects-api-to-alerts-api
- FalconPy - environment configuration / regional base URLs:
  https://www.falconpy.io/Usage/Environment-Configuration.html
