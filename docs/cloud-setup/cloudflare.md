<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/cloudflare.md        -->
<!-- Purpose:   Cloudflare cloud admin setup guide    -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Cloudflare Setup for dfe-fetcher

What a Cloudflare administrator needs to configure so dfe-fetcher can read
account-level audit logs.

> Status: alpha - code-complete, not production-validated; behaviour and config may change.

## Overview

dfe-fetcher pulls account audit-log entries from the v1 REST endpoint
`https://api.cloudflare.com/client/v4/accounts/{account_id}/audit_logs`, one
Cloudflare account per fetcher instance. It requests a time window
(`since` / `before` RFC 3339) with `per_page`, optional `actor.email` and
`action.type` filters, and pages by number using `result_info.total_pages`
from Cloudflare's `{"success":..., "result":[...], "result_info":{...}}`
envelope. Authentication is a scoped API token in
`Authorization: Bearer <token>`.

Note on API versions: Cloudflare made **Audit Logs v2** (the newer
`/accounts/{account_id}/logs/audit` endpoint) generally available in early
2026. dfe-fetcher currently targets the **v1** `audit_logs` endpoint, which
is still live - Cloudflare has not published a firm retirement date for v1.
If/when v1 is sunset, this source will need to move to v2; track the
references below.

## Prerequisites

- Any Cloudflare account. Account audit logs are available on all plans;
  no paid add-on is required to read them via the API.
- An account role that can create API tokens and read account settings
  (Super Administrator, or Administrator). User API tokens are created from
  your user profile, scoped to the chosen account.
- Your 32-character hex **Account ID** (see below).

## Required Permissions

| Service | Endpoint | Permission or Scope | Notes |
|---------|----------|---------------------|-------|
| `audit_logs` | `GET /accounts/{account_id}/audit_logs` (v1) | **Account Settings: Read** (`Account` -> `Account Settings` -> `Read`) | This is the permission that actually works for the v1 audit_logs endpoint. There is NO permission literally named "Account Audit Logs Read" for v1. |
| `audit_logs` (v2, future) | `GET /accounts/{account_id}/logs/audit` | **Account Audit Logs Read** (a.k.a. "Get account audit logs (Version 2)") or Account Settings Read/Write | Only relevant once dfe-fetcher moves to the v2 endpoint. |

Scope the token to the single target account (not "all accounts") and keep it
read-only.

## Source-Side Setup

### 1. Find your Account ID

In the Cloudflare dashboard (`dash.cloudflare.com`), open the account, then
either:

- Click the menu button (three dots) next to the account name on the Account
  Home page and choose **Copy account ID**; or
- On the account **Overview** page, scroll to the **API** section at the
  bottom and copy the **Account ID**.

It is a 32-character hex string. This is the `account_id` config field.

### 2. Create a scoped API token

1. Top-right profile -> **My Profile** -> **API Tokens** -> **Create Token**.
2. Scroll to **Create Custom Token** -> **Get started**.
3. Name it `dfe-fetcher-audit`.
4. Under **Permissions**, select **Account** / **Account Settings** /
   **Read**.
5. Under **Account Resources**, select **Include -> <your account>** (do not
   grant all accounts).
6. (Recommended) Under **Client IP Address Filtering**, restrict to the
   fetcher's egress IP, and set a **TTL** to bound the token's lifetime.
7. **Continue to summary** -> **Create Token**. Copy the token now - it is
   shown once. New tokens use the scannable `cfut_`-prefixed format.

   ```bash
   # Smoke-test the v1 endpoint:
   curl -sS \
     "https://api.cloudflare.com/client/v4/accounts/$ACCOUNT_ID/audit_logs?per_page=1" \
     -H "Authorization: Bearer $CLOUDFLARE_TOKEN"
   # Verify the token itself:
   curl -sS "https://api.cloudflare.com/client/v4/user/tokens/verify" \
     -H "Authorization: Bearer $CLOUDFLARE_TOKEN"
   ```

## dfe-fetcher Configuration

### Config File

```yaml
sources:
  cloudflare:
    enabled: true
    account_id: "0123456789abcdef0123456789abcdef"   # 32-char hex
    token: "cfut_your_api_token_here"
    services:
      - name: audit_logs
        config:
          actor_email: "ops@example.com"   # optional, filter by acting user
          action_type: "login"             # optional, filter by action category
          per_page: 100                     # default 100, max 1000
    topic: "cloudflare"
    # filter: 'action.type != "view"'   # CEL, hot-reloaded
```

### Environment Variables

Pattern is `DFE_FETCHER_SOURCES__CLOUDFLARE__<FIELD>` (double underscores).

```bash
DFE_FETCHER_SOURCES__CLOUDFLARE__ENABLED="true"
DFE_FETCHER_SOURCES__CLOUDFLARE__ACCOUNT_ID="0123456789abcdef0123456789abcdef"
DFE_FETCHER_SOURCES__CLOUDFLARE__TOKEN="cfut_your_api_token_here"
```

### Secrets Manager

Keep the token out of the config file with `credential_secret`, which
resolves to the literal token value:

```yaml
sources:
  cloudflare:
    enabled: true
    account_id: "0123456789abcdef0123456789abcdef"
    credential_secret: "vault:secret/cloudflare:token"
    services:
      - name: audit_logs
    topic: "cloudflare"
```

## Verification

- **Health check.** The source's `health_check()` calls
  `GET /user/tokens/verify`; a 2xx confirms the token is valid and active.
  Note this proves the token is good but not that it carries Account Settings
  Read - exercise the smoke test for that.
- **e2e smoke test.** `tests/e2e/smoke_remote.rs` has `#[ignore]`-gated live
  tests. Export credentials, then run:

  ```bash
  export CLOUDFLARE_ACCOUNT_ID="0123456789abcdef0123456789abcdef"
  export CLOUDFLARE_TOKEN="cfut_..."
  cargo nextest run --test e2e -- --ignored cloudflare_
  ```

- **Common failure modes.**
  - `403 Forbidden` / `success:false` with an authentication error: token
    lacks **Account Settings: Read**, or is scoped to the wrong account.
    Adding Account Settings Read is the usual fix - do NOT go hunting for an
    "Audit Logs" permission for the v1 endpoint.
  - `400` / `404`: malformed or wrong `account_id` (must be the 32-char hex
    account ID, not a zone ID).
  - Body `success:false` on a 2xx: dfe-fetcher treats this as an error and
    surfaces the `errors` array.
  - Empty result every tick: no audit events in the lookback window
    (default 1h).

## Cost

dfe-fetcher only reads. Reading account audit logs through the API is not
expected to carry an additional charge, but this can depend on your
Cloudflare plan. Confirm any cost implications against your own Cloudflare
plan.

## References

- Cloudflare API - Get Account Audit Logs (v1 list method):
  https://developers.cloudflare.com/api/resources/audit_logs/methods/list/
- Review audit logs - v1 (Fundamentals):
  https://developers.cloudflare.com/fundamentals/account/account-security/review-audit-logs/
- Audit Logs - version 2 (Fundamentals):
  https://developers.cloudflare.com/fundamentals/account/account-security/audit-logs/
- Audit logs (version 2) - General Availability changelog:
  https://developers.cloudflare.com/changelog/post/2026-03-10-audit-logs-v2-ga/
- API token permissions reference:
  https://developers.cloudflare.com/fundamentals/api/reference/permissions/
- Create API token:
  https://developers.cloudflare.com/fundamentals/api/get-started/create-token/
- Find account and zone IDs:
  https://developers.cloudflare.com/fundamentals/account/find-account-and-zone-ids/
