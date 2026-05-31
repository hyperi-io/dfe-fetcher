<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/duo.md               -->
<!-- Purpose:   Cisco Duo admin setup guide           -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Duo Setup for dfe-fetcher

What a Cisco Duo administrator needs to configure so dfe-fetcher can read
authentication logs from a Duo tenant via the Admin API.

## Overview

dfe-fetcher reads Duo authentication events from the **Duo Admin API**
endpoint `/admin/v2/logs/authentication`. Duo does NOT use a bearer token:
every request is signed with Duo's proprietary HMAC-SHA1 scheme. The fetcher
computes a signature over a canonical request string and sends it in a Basic
auth header, with a fresh `Date` header per request (no token is cached):

```text
canonical = HTTP_DATE \n METHOD \n LOWER(HOST) \n PATH \n SORTED_QUERY
sig       = hex(HMAC-SHA1(skey, canonical))
Auth      = Basic base64(ikey:sig)
```

The admin provisions an **Admin API application** in the Duo Admin Panel,
grants it log-read permission, and hands dfe-fetcher three values: the
**integration key** (ikey), **secret key** (skey), and **API hostname**.

The v2 endpoint takes `mintime`/`maxtime` as **millisecond** Unix timestamps,
returns up to `limit` records (default 100, max 1000), and paginates with an
opaque `next_offset`. Note Duo enforces a deliberate **two-minute delay**:
authentications less than two minutes old are not yet returned.

## Prerequisites

- A Duo subscription whose edition includes Admin API access.
- A Duo administrator with the **Owner** (or equivalent) role - required to
  create applications and grant Admin API permissions.
- Access to the Duo Admin Panel for your tenant
  (`https://admin.duosecurity.com`).

## Required Permissions

| Service | Object/Endpoint | Permission or Scope | Notes |
|---------|-----------------|---------------------|-------|
| Authentication logs | `GET /admin/v2/logs/authentication` | **Grant read log** | Read-only. The only permission dfe-fetcher needs. |
| Health probe | `GET /admin/v1/check` | (covered by the same application keys) | Cheap auth check; no extra permission. |

Grant **only** "Grant read log". Leave all other Admin API permission toggles
unchecked (least privilege).

## Source-Side Setup

1. **Sign in to the Duo Admin Panel** at `https://admin.duosecurity.com` as an
   administrator with rights to create applications.

2. **Start a new Admin API application.** Either path works depending on your
   panel version:
   - **Newer panel:** **Applications > Application Catalog**, find **Admin
     API**, click **+ Add**.
   - **Older panel:** **Applications > Protect an Application**, search for
     `Admin API`, then click **Protect** next to it.

3. **Grant the log-read permission.** On the new application's page, in the
   **Permissions** section, tick **Grant read log**. Untick everything else.
   Click **Save Changes**.

4. **Copy the credentials.** In the application's **Details** section, copy:
   - **Integration key** (the `ikey`)
   - **Secret key** (the `skey`) - treat as a password; shown here so capture
     it now
   - **API hostname** (the `api_host`, format `api-XXXXXXXX.duosecurity.com`)

5. **(Optional) Name the application** something recognisable, e.g.
   `dfe-fetcher-authlogs`, and **Save Changes** again.

## dfe-fetcher Configuration

The Duo source config fields are: `enabled`, `api_host`, `integration_key`
(ikey), `secret_key` (skey), `credential_secret`, `api_url_override` (test
mock only), `services`, `topic`. The only service is `authentication_logs`.

### Config File

```yaml
sources:
  duo:
    enabled: true
    api_host: "api-XXXXXXXX.duosecurity.com"   # API hostname from Admin Panel
    integration_key: "your-ikey"
    secret_key: "your-skey"
    services:
      - name: authentication_logs
        # config:
        #   limit: 100   # per-page size, max 1000
    topic: "duo"
    # filter: 'result == "FRAUD"'  # hot-reloaded CEL expression
```

### Environment Variables

```bash
DFE_FETCHER_SOURCES__DUO__ENABLED="true"
DFE_FETCHER_SOURCES__DUO__API_HOST="api-XXXXXXXX.duosecurity.com"
DFE_FETCHER_SOURCES__DUO__INTEGRATION_KEY="your-ikey"
DFE_FETCHER_SOURCES__DUO__SECRET_KEY="your-skey"
```

### Secrets Manager (Production)

Keep the secret key out of the config file with `credential_secret`:

```yaml
sources:
  duo:
    enabled: true
    api_host: "api-XXXXXXXX.duosecurity.com"
    integration_key: "your-ikey"
    credential_secret: "vault:secret/dfe/duo:skey"
    services:
      - name: authentication_logs
    topic: "duo"
```

`credential_secret` resolves to the secret key (skey). When set, it takes
precedence over an inline `secret_key`. The ikey and api_host are not secret
and can stay inline.

## Verification

**Health check.** The source's `health_check` signs and calls
`GET /admin/v1/check`, which returns `{"response": "valid", "stat": "OK"}` for
working credentials. A healthy result confirms the ikey, skey, and api_host
are correct and the application is active.

**Live smoke tests.** `tests/e2e/smoke_remote.rs` has env-gated, `#[ignore]`d
tests that hit a real tenant. They read these variables (canonical
`.env-cloud`, fallback `.env`):

- `DUO_API_HOST`
- `DUO_INTEGRATION_KEY`
- `DUO_SECRET_KEY`

Run them:

```bash
cargo nextest run --test e2e -- --ignored duo_
```

Results are tagged `duo.authentication_logs`. Zero records is normal on a
quiet tenant, a short lookback, or if all events fall inside the two-minute
delay window.

**Common failures.**

- `40103 Invalid signature` / `stat != OK` - the skey is wrong, or clock skew
  between the host and Duo broke the signed `Date` header. Verify the skey and
  ensure NTP is in sync.
- `40101 Missing request credentials` - the ikey or api_host is wrong.
- `40301 Access denied` - the Admin API application lacks **Grant read log**;
  re-check the Permissions section and save.
- Health passes but zero records every cycle - widen the window or confirm
  authentications exist; remember events under two minutes old are withheld.

## Cost

dfe-fetcher only reads. The Admin API and authentication-log reads add no
charge of their own, but access may require a Duo plan/edition that includes
Admin API access. Confirm any cost implications against your own Duo plan.

## References

- Cisco Duo - Admin API (application creation, signing, permissions):
  https://duo.com/docs/adminapi
- Cisco Duo - Admin API v2 authentication logs
  (`/admin/v2/logs/authentication`, mintime/maxtime in ms, next_offset):
  https://duo.com/docs/adminapi#authentication-logs
- Cisco Duo - Protecting applications (Application Catalog / Protect an
  Application): https://duo.com/docs/protecting-applications
