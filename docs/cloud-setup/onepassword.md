<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/onepassword.md       -->
<!-- Purpose:   1Password cloud admin setup guide      -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   FSL-1.1-ALv2                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# 1Password Setup for dfe-fetcher

What a 1Password account owner or administrator needs to configure so
dfe-fetcher can read activity from the 1Password Events Reporting API.

> Status: alpha - code-complete, not production-validated; behaviour and config may change.

## Overview

dfe-fetcher pulls three event streams from the 1Password Events Reporting
(Events) API: sign-in attempts, item usages, and audit events (changes to
vaults, groups, users, and similar account-management actions). The API is
poll-only - there are no outbound webhooks. dfe-fetcher authenticates with a
single bearer JSON Web Token (JWT) issued from the 1Password admin console
and POSTs a time-window / cursor body to each `/api/v2/*` endpoint.

## Prerequisites

- A **1Password Business** account (Events Reporting is a Business-tier
  feature; it is not available on Teams Starter, Families, or Individual).
  1Password Enterprise also qualifies.
- The person doing this setup must be an account **owner or administrator**.
- No paid add-on beyond the Business subscription; Events Reporting and
  Events API access are included.

## Required Permissions

The token is scoped per integration to a set of event types. dfe-fetcher
needs a single token that can read all three streams it polls.

| Service | Object/Endpoint | Permission or Scope | Notes |
|---------|-----------------|---------------------|-------|
| Events Reporting | `/api/v2/signinattempts` | Sign-in attempts event type | Failed and successful sign-ins, with location/device detail |
| Events Reporting | `/api/v2/itemusages` | Item usages event type | Item view/copy/edit events in shared vaults |
| Events Reporting | `/api/v2/auditevents` | Audit events event type | Vault/group/user/account administration changes |

The token is **read-only** by design - the Events API exposes no write
operations. When issuing the token, select (do not deselect) all three event
features above. To change which event types a token can read, you must issue
a new token; you cannot edit an existing one.

## Source-Side Setup

1. **Confirm Events Reporting is available**

   Sign in at your account URL (for example `https://my.1password.com`) as an
   owner or administrator. The host in that URL is your region - note it now,
   you need it for the base URL in step 4 (`1password.com` = US,
   `1password.ca` = Canada, `1password.eu` / `ent.1password.eu` = EU).

2. **Create the Events Reporting integration**

   Select **Integrations** in the sidebar. Under **Events Reporting**, pick
   your SIEM if one is listed, or choose **Other** to build a custom client
   (dfe-fetcher is a custom client). Enter a name such as `dfe-fetcher`, then
   select **Add Integration**.

3. **Issue a bearer token**

   Enter a token name (for example `dfe-fetcher`), choose an expiry, and make
   sure **Sign-in attempts**, **Item usages**, and **Audit events** are all
   selected. Select **Issue Token**, then copy the JWT (it is shown once -
   save it to a vault). This is the value for `token` below.

   CLI alternative (requires the 1Password CLI signed in to a Business
   account):

   ```bash
   op events-api create --name dfe-fetcher \
     --features signinattempts,itemusages,auditevents \
     --expires-in 0   # 0 = never expires; set a duration like 8760h otherwise
   ```

4. **Note your regional base URL**

   The default is the US host. Override only if your tenant is not US:

   | Region | Base URL |
   |--------|----------|
   | US (default) | `https://events.1password.com` |
   | US Enterprise | `https://events.ent.1password.com` |
   | Canada | `https://events.1password.ca` |
   | EU (standard) | `https://events.1password.eu` |
   | EU (enterprise) | `https://events.ent.1password.eu` |

## dfe-fetcher Configuration

### Config File

```yaml
sources:
  onepassword:
    enabled: true
    token: "your-events-reporting-bearer-jwt"
    # api_url_override: "https://events.1password.eu"   # only if not US
    services:
      - name: signin_attempts     # /api/v2/signinattempts
      - name: item_usages         # /api/v2/itemusages
      - name: audit_events        # /api/v2/auditevents
    topic: "onepassword"
    # filter: 'category != "miss"'   # optional, hot-reloaded
```

### Environment Variables

```bash
DFE_FETCHER_SOURCES__ONEPASSWORD__ENABLED="true"
DFE_FETCHER_SOURCES__ONEPASSWORD__TOKEN="your-events-reporting-bearer-jwt"
# Non-US tenants only:
DFE_FETCHER_SOURCES__ONEPASSWORD__API_URL_OVERRIDE="https://events.1password.eu"
```

### Secrets Manager

Keep the token out of the config file by resolving it from a secret store:

```yaml
sources:
  onepassword:
    enabled: true
    credential_secret: "vault:secret/onepassword:events_token"
    services:
      - name: signin_attempts
      - name: item_usages
      - name: audit_events
    topic: "onepassword"
```

`credential_secret` takes precedence over a literal `token`; it resolves to
the bearer JWT string. The only configurable service-side key is `limit`
(page size, default 100, max 1000).

## Verification

The source implements `health_check`, which calls `/api/auth/introspect`
with the token (this does not consume any per-endpoint event budget) and
returns healthy on a 2xx.

End-to-end smoke tests live in `tests/e2e/smoke_remote.rs` and are
`#[ignore]`d by default. They read `ONEPASSWORD_EVENTS_TOKEN` (required) and
`ONEPASSWORD_API_BASE` (optional regional override) from `.env-cloud` (or
`.env`):

```bash
ONEPASSWORD_EVENTS_TOKEN="your-jwt" \
  cargo nextest run --test e2e -- --ignored onepassword_
```

Tests cover `onepassword_health_check`, `onepassword_fetch_signin_attempts`,
`onepassword_fetch_item_usages`, and `onepassword_fetch_audit_events`.

Common failure modes:

- **401 / health check fails** - token revoked, expired, or wrong region
  base URL.
- **Empty results** - the time window had no matching events; sign-in
  attempts and item usages are sparse on small tenants.
- **403 on one endpoint only** - the token was issued without that event
  feature. Issue a new token with all three selected.

## Cost

dfe-fetcher only reads. Events Reporting and Events API reads add no charge
of their own, but access may require a paid 1Password plan (Business or
equivalent with Events Reporting). Fair-use rate limits apply. Confirm any
cost implications against your own 1Password plan.

## References

- 1Password Events Reporting setup: https://support.1password.com/events-reporting/
- About the Events API: https://developer.1password.com/docs/events-api/introduction/
- Servers and base URLs (regional hosts): https://developer.1password.com/docs/events-api/servers/
- Events API reference (endpoints, rate limits): https://developer.1password.com/docs/events-api/reference/
- 1Password CLI events-api command: https://developer.1password.com/docs/cli/reference/management-commands/events-api/
