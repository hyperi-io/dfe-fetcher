<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/okta.md              -->
<!-- Purpose:   Okta cloud admin setup guide          -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Okta Setup for dfe-fetcher

What an Okta administrator needs to configure so dfe-fetcher can read System
Log events from an Okta org.

## Overview

dfe-fetcher pulls events from the Okta System Log API at
`{tenant_url}/api/v1/logs`, one Okta org per fetcher instance. It requests a
time-bounded, ascending window (`since` / `until` / `limit` /
`sortOrder=ASCENDING`) and pages through the RFC 5988 `Link: rel="next"`
header. An optional service-config `filter` (Okta OData syntax) narrows
results server-side. Authentication is one of three schemes:

- `use_ssws_header: true` (default on the `sources.okta` block) - sends
  `Authorization: SSWS <token>` using a legacy Okta API token.
- `use_ssws_header: false` - sends `Authorization: Bearer <token>` using an
  OAuth 2.0 access token someone else minted with the `okta.logs.read` scope.
- An API Services app and its key pair, configured as a `sources.rest`
  instance of the same profile: the fetcher mints its own access token, signing
  a private-key JWT for the client-credentials exchange.

Okta recommends OAuth 2.0 over SSWS for management APIs, and the service-app
key pair over both: an SSWS token is static and carries the permissions of the
admin who minted it, where a service app is granted `okta.logs.read` alone and
the private half of its key never leaves the deployment. SSWS remains supported
and is the simplest path.

The source is the shipped `okta` REST profile
(`crates/fetcher/profiles/okta.yaml`); the `sources.okta` block below maps
onto an instance of it at load, so the profile's retry policy applies: a 429
or 5xx is retried with backoff (honouring `Retry-After`), a 401 or 403 ends
the tick, and a tick that fails does not advance the fetch window.

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

### Option B - OAuth 2.0 service app (recommended)

1. **Admin Console -> Applications -> Applications -> Create App
   Integration -> API Services**. Name it `dfe-fetcher`.
2. On the app's **General** tab, configure the client-credentials flow with a
   public/private key pair (JWT client assertion). The assertion is signed
   RS256, so generate an **RSA** key pair - an EC key is refused when the
   fetcher reads it. Save the public key in Okta and keep the private key for
   the fetcher. Note the key id (`kid`) Okta shows beside the public key.
3. **Okta API Scopes** tab -> grant **`okta.logs.read`**.
4. Assign the app an admin role with System Log access (Super Admin, Read-only
   Admin, or a custom role with **System Log query**).
5. Note the app's client id. The fetcher mints and renews the access token
   itself: it signs a short-lived assertion with the private key and posts it to
   `{tenant_url}/oauth2/v1/token`, so nothing in the config expires hourly and
   no rotation job is needed. Keep the private key in your secrets manager and
   reference it as a spec.

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

For an access token someone else mints, set `use_ssws_header: false` and supply
it as `token` (preferably via `credential_secret`).

For the service app, configure the same profile as a `sources.rest` instance so
the fetcher can hold the key and mint its own token:

```yaml
sources:
  rest:
    okta:
      profile: okta
      topic: "okta"
      auth:
        mode: oauth2_client_credentials
        client_id: "0oa1example"
        private_key: "vault:kv/data/okta:private_key"
        private_key_id: "the kid Okta shows beside the public key"
      vars:
        base_url: "https://your-tenant.okta.com"
```

The private key is the app's own RSA PEM, the one whose public half was uploaded
to Okta. Scope and assertion lifetime come from the profile, so nothing about the
exchange has to be restated here.

`private_key_id` is the `kid` the assertion names itself by, and it is not a
secret. An app with ONE registered key pair is resolved without it. An app with
two is not: Okta reads the `kid` and answers
`The client_assertion JWT kid is invalid.` to an assertion that names none. Two
key pairs is what a rotation looks like, so **set it before you upload a second
public key**, not after - roll it forward as: add the new key pair in Okta, set
`private_key` and `private_key_id` to the new pair, confirm a tick, then remove
the old public key from the app.

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
    credential_secret: "vault:kv/data/okta:token"
    use_ssws_header: true
    services:
      - name: system_log
    topic: "okta"
```

## Verification

- **Health check.** The profile's probe calls
  `GET {tenant_url}/api/v1/users/me` with the configured auth header; a 2xx
  confirms the token + scheme + tenant URL are correct and reachable.
- **e2e smoke test.** `crates/fetcher/tests/e2e/smoke_remote.rs` has
  `#[ignore]`-gated live tests. Export credentials (or put them in
  `.env-cloud`), then run:

  ```bash
  export OKTA_TENANT_URL="https://your-tenant.okta.com"
  export OKTA_TOKEN="00..."
  export OKTA_USE_SSWS="true"     # or false for OAuth bearer
  cargo test -p dfe-fetcher --test e2e okta_ -- --ignored
  ```

- **Common failure modes.**
  - `401 Unauthorized`: wrong scheme - SSWS token sent as Bearer (or vice
    versa). Flip `use_ssws_header` to match the token type.
  - `401` on the token exchange with `invalid_client`: the public key Okta
    holds does not pair with the configured `private_key`, or the `client_id`
    is another app's.
  - `401` on the token exchange naming the `kid`: the app has more than one key
    pair registered and the instance sets no `private_key_id`, or sets one the
    app does not hold. This is what a half-finished key rotation looks like.
  - `403 Forbidden`: the token's admin role lacks System Log access, or the
    OAuth app was not granted `okta.logs.read`.
  - `404` / connection error: `tenant_url` typo, trailing slash, or wrong
    domain (note `oktapreview.com` for preview orgs). An empty or missing
    `tenant_url` is refused at load, naming `sources.okta`.
  - Token silently stops working after a quiet period: SSWS tokens are
    revoked after 30 days of inactivity.

## Cost

dfe-fetcher only reads. The System Log API is not expected to carry an
additional charge, though rate limits and any plan-specific terms depend on
your Okta org. Confirm any cost implications against your own Okta plan.

## References

- Create an API token (Okta Developer):
  <https://developer.okta.com/docs/guides/create-an-api-token/main/>
- Manage Okta API tokens (Help Center):
  <https://help.okta.com/oie/en-us/content/topics/security/api.htm>
- System Log API reference:
  <https://developer.okta.com/docs/api/openapi/okta-management/management/tags/systemlog>
- System Log query parameters:
  <https://developer.okta.com/docs/reference/system-log-query/>
- OAuth 2.0 scopes (incl. okta.logs.read):
  <https://developer.okta.com/docs/api/oauth2>
- Implement OAuth for Okta with a service app:
  <https://developer.okta.com/docs/guides/implement-oauth-for-okta-serviceapp/main/>
- Read-only administrators role:
  <https://help.okta.com/en-us/content/topics/security/administrators-read-only-admin.htm>
