<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/okta.md              -->
<!-- Purpose:   Okta cloud admin setup guide          -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   FSL-1.1-ALv2                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Okta Setup for dfe-fetcher

What an Okta administrator needs to configure so dfe-fetcher can read System
Log events from an Okta org.

> Status: alpha - code-complete, not production-validated; behaviour and config may change.

## Overview

dfe-fetcher pulls events from the Okta System Log API at
`{tenant_url}/api/v1/logs`, one Okta org per fetcher instance. It requests a
time-bounded, ascending window (`since` / `until` / `limit` /
`sortOrder=ASCENDING`) and pages through the RFC 5988 `Link: rel="next"`
header. An optional service-config `filter` (Okta OData syntax) narrows
results server-side. Authentication is one of two schemes, selected by the
`use_ssws_header` config toggle:

- `use_ssws_header: true` (default) - sends `Authorization: SSWS <token>`
  using a legacy Okta API token.
- `use_ssws_header: false` - sends `Authorization: Bearer <token>` using an
  OAuth 2.0 access token minted with the `okta.logs.read` scope.

Okta recommends OAuth 2.0 over SSWS for management APIs. SSWS remains
supported and is the simplest path; OAuth is the better long-term choice.

## Prerequisites

- Any Okta org (Workforce or Customer Identity). The System Log API is
  available on all tiers; no add-on is required.
- An administrator account to create the credential:
  - SSWS path: an admin who can mint API tokens. The token inherits that
    admin's privileges, so use a dedicated service account whose role is
    stable (a deactivated creator deprovisions the token).
  - OAuth path: an admin who can create an API Service / OAuth service app
    and grant it the `okta.logs.read` scope.
- Least-privilege option: the standard **Read-only Administrator** role can
  view System Log data and manage its own API token; or use a custom admin
  role carrying the **System Log query** permission.

## Required Permissions

| Service | Endpoint | Permission or Scope | Notes |
|---------|----------|---------------------|-------|
| `system_log` | `GET {tenant_url}/api/v1/logs` | SSWS token from a service account with read access to System Log | Token inherits the creating admin's privileges; Read-only Admin or System Log query custom role is sufficient. |
| `system_log` | `GET {tenant_url}/api/v1/logs` | OAuth scope `okta.logs.read` | Granted to an API Service app using the client-credentials flow. |

All access is read-only.

## Source-Side Setup

### Option A - SSWS API token (simplest)

1. Sign in to the **Admin Console** as the service-account admin.
2. Go to **Security -> API**, open the **Tokens** tab, click **Create
   token**.
3. Name it `dfe-fetcher`. Optionally restrict the origin: pick **Any IP**,
   any defined network zone, or specific zones (recommend pinning the
   fetcher's egress IP/zone).
4. Click **Copy to clipboard** - the token value is shown only once. Store it
   in your secrets manager.
5. Note: SSWS tokens auto-renew on each use but are revoked after 30 days of
   inactivity, and are deprovisioned if the creating user is deactivated.
   Schedule the fetcher so the token is exercised regularly.

   ```bash
   # Smoke-test (SSWS):
   curl -sS "https://your-tenant.okta.com/api/v1/logs?limit=1" \
     -H "Authorization: SSWS $OKTA_TOKEN" \
     -H "Accept: application/json"
   ```

### Option B - OAuth 2.0 service app (recommended long term)

1. **Admin Console -> Applications -> Applications -> Create App
   Integration -> API Services**. Name it `dfe-fetcher`.
2. On the app's **General** tab, configure the client-credentials flow with a
   public/private key pair (JWT client assertion). Save the public key in
   Okta and keep the private key for the token caller.
3. **Okta API Scopes** tab -> grant **`okta.logs.read`**.
4. Assign the app an admin role with System Log access (Super Admin, Read-only
   Admin, or a custom role with **System Log query**).
5. The caller mints a one-hour access token via the client-credentials grant
   (POST to `{tenant_url}/oauth2/v1/token` with a signed `client_assertion`)
   and supplies it to dfe-fetcher as `token`, with `use_ssws_header: false`.
   Because OAuth access tokens expire hourly, drive this through
   `credential_secret` plus your secrets manager's rotation, not a static
   config value.

## dfe-fetcher Configuration

### Config File

```yaml
sources:
  okta:
    enabled: true
    tenant_url: "https://your-tenant.okta.com"   # no trailing slash
    token: "00your_ssws_token_here"
    use_ssws_header: true            # true = SSWS (default); false = Bearer/OAuth
    services:
      - name: system_log
        config:
          filter: 'eventType eq "user.session.start"'   # optional OData filter
          limit: 100                                    # per-page, max 1000
    topic: "okta"
    # filter: 'eventType != "user.session.access_token"'   # CEL, hot-reloaded
```

For OAuth, set `use_ssws_header: false` and supply an OAuth access token as
`token` (preferably via `credential_secret`).

### Environment Variables

Pattern is `DFE_FETCHER_SOURCES__OKTA__<FIELD>` (double underscores).

```bash
DFE_FETCHER_SOURCES__OKTA__ENABLED="true"
DFE_FETCHER_SOURCES__OKTA__TENANT_URL="https://your-tenant.okta.com"
DFE_FETCHER_SOURCES__OKTA__TOKEN="00your_ssws_token_here"
DFE_FETCHER_SOURCES__OKTA__USE_SSWS_HEADER="true"
```

### Secrets Manager

Keep the token out of the config file with `credential_secret`, which
resolves to the literal token value (SSWS token or OAuth access token):

```yaml
sources:
  okta:
    enabled: true
    tenant_url: "https://your-tenant.okta.com"
    credential_secret: "vault:secret/okta:token"
    use_ssws_header: true
    services:
      - name: system_log
    topic: "okta"
```

## Verification

- **Health check.** The source's `health_check()` calls
  `GET {tenant_url}/api/v1/users/me` with the configured auth header; a 2xx
  confirms the token + scheme + tenant URL are correct and reachable.
- **e2e smoke test.** `tests/e2e/smoke_remote.rs` has `#[ignore]`-gated live
  tests. Export credentials, then run:

  ```bash
  export OKTA_TENANT_URL="https://your-tenant.okta.com"
  export OKTA_TOKEN="00..."
  export OKTA_USE_SSWS="true"     # or false for OAuth bearer
  cargo nextest run --test e2e -- --ignored okta_
  ```

- **Common failure modes.**
  - `401 Unauthorized`: wrong scheme - SSWS token sent as Bearer (or vice
    versa). Flip `use_ssws_header` to match the token type.
  - `403 Forbidden`: the token's admin role lacks System Log access, or the
    OAuth app was not granted `okta.logs.read`.
  - `404` / connection error: `tenant_url` typo, trailing slash, or wrong
    domain (note `oktapreview.com` for preview orgs).
  - Token silently stops working after a quiet period: SSWS tokens are
    revoked after 30 days of inactivity.

## Cost

dfe-fetcher only reads. The System Log API is not expected to carry an
additional charge, though rate limits and any plan-specific terms depend on
your Okta org. Confirm any cost implications against your own Okta plan.

## References

- Create an API token (Okta Developer):
  https://developer.okta.com/docs/guides/create-an-api-token/main/
- Manage Okta API tokens (Help Center):
  https://help.okta.com/oie/en-us/content/topics/security/api.htm
- System Log API reference:
  https://developer.okta.com/docs/api/openapi/okta-management/management/tags/systemlog
- System Log query parameters:
  https://developer.okta.com/docs/reference/system-log-query/
- OAuth 2.0 scopes (incl. okta.logs.read):
  https://developer.okta.com/docs/api/oauth2
- Implement OAuth for Okta with a service app:
  https://developer.okta.com/docs/guides/implement-oauth-for-okta-serviceapp/main/
- Read-only administrators role:
  https://help.okta.com/en-us/content/topics/security/administrators-read-only-admin.htm
