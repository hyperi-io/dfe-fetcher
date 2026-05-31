<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/slack.md             -->
<!-- Purpose:   Slack cloud admin setup guide          -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Slack Setup for dfe-fetcher

What a Slack Enterprise Grid Organization Owner needs to configure so
dfe-fetcher can read audit-log events from the Slack Audit Logs API.

## Overview

dfe-fetcher pulls organization-wide audit events from the Slack Audit Logs
API at `https://api.slack.com/audit/v1/logs`. Each entry describes an actor,
an action, an entity, and a context (the API is read-only and does not expose
message content). dfe-fetcher authenticates with an org-level user bearer
token (`xoxp-...`) carrying the `auditlogs:read` scope and pages through
results via the response `next_cursor`.

## Prerequisites

- A **Slack Enterprise Grid** organization (also marketed as Enterprise+).
  The Audit Logs API is not available on Free, Pro, or Business+ plans.
- The setup must be performed by an **Organization Owner** - the app must be
  installed at the org level, and only an Org Owner sees the org install
  option.
- No paid add-on beyond the Enterprise Grid plan.

## Required Permissions

| Service | Object/Endpoint | Permission or Scope | Notes |
|---------|-----------------|---------------------|-------|
| Audit Logs | `audit/v1/logs` | `auditlogs:read` (User Token Scope) | The single scope that enables Audit Logs API access; org-wide |
| Org install helper | n/a | `users:read` (Bot Token Scope) | Not used for reads, but org-level install is often blocked without it |

The token produced is a **user token** (`xoxp-...`), not a bot token. It must
come from installing the app on the **Enterprise organization**, not on an
individual workspace - a workspace-scoped token will not authorize Audit Logs
API calls.

## Source-Side Setup

1. **Create the app**

   Sign in as an Org Owner and open https://api.slack.com/apps. Select
   **Create New App** -> **From scratch**. Name it `dfe-fetcher`, pick any
   workspace in the org as the development workspace, and select **Create
   App**.

2. **Add the OAuth scopes**

   In the app settings, go to **OAuth & Permissions**. Under **Scopes** ->
   **User Token Scopes**, add **`auditlogs:read`**. Under **Bot Token
   Scopes**, add **`users:read`** (needed so org-level install is permitted).

3. **Add a redirect URL**

   Still under **OAuth & Permissions**, add a redirect URL under **Redirect
   URLs** and save. The org install runs a standard OAuth2 flow, which
   requires at least one redirect URL.

4. **Activate public distribution**

   Go to **Manage Distribution**. Confirm all four checklist sections show
   green checkmarks, tick **I've reviewed and removed any hard-coded
   information**, and select **Activate Public Distribution**. This does not
   list the app publicly - it is the technical prerequisite that unlocks
   org-wide (rather than single-workspace) installation.

5. **Install at the organization level**

   Start the install/OAuth flow. On the install screen, use the dropdown in
   the upper right to select the **Enterprise organization**, not a single
   workspace. If the org option is missing, confirm you are signed in as an
   Org Owner.

6. **Copy the token**

   After authorization you return to **OAuth & Permissions**. Under **OAuth
   Tokens**, copy the **User OAuth Token** (starts with `xoxp-`). This is the
   value for `token` below.

   Verify the token from a shell:

   ```bash
   curl -s "https://api.slack.com/audit/v1/logs?limit=1" \
     -H "Authorization: Bearer xoxp-..."
   ```

## dfe-fetcher Configuration

### Config File

```yaml
sources:
  slack:
    enabled: true
    token: "xoxp-your-org-user-token"
    services:
      - name: audit_logs
        # config:
        #   action: "user_login"   # optional: filter to one action
        #   entity: "user"         # optional: filter to one entity type
        #   limit: 200             # per-page, default 200, max 1000
    topic: "slack"
    # filter: 'action != "user_login"'   # optional, hot-reloaded
```

### Environment Variables

```bash
DFE_FETCHER_SOURCES__SLACK__ENABLED="true"
DFE_FETCHER_SOURCES__SLACK__TOKEN="xoxp-your-org-user-token"
```

### Secrets Manager

```yaml
sources:
  slack:
    enabled: true
    credential_secret: "vault:secret/slack:audit_token"
    services:
      - name: audit_logs
    topic: "slack"
```

`credential_secret` takes precedence over a literal `token` and resolves to
the `xoxp-...` string. The only audit service dfe-fetcher implements is
`audit_logs`.

## Verification

The source implements `health_check`, which calls `/api/auth.test` with the
token and treats `{"ok": true}` as healthy.

End-to-end smoke tests live in `tests/e2e/smoke_remote.rs` and are
`#[ignore]`d by default. They read `SLACK_AUDIT_TOKEN` (required) from
`.env-cloud` (or `.env`):

```bash
SLACK_AUDIT_TOKEN="xoxp-..." \
  cargo nextest run --test e2e -- --ignored slack_
```

Tests cover `slack_health_check` and `slack_fetch_audit_logs`.

Common failure modes:

- **`not_authed` / `invalid_auth`** - token is wrong, revoked, or expired.
- **`feature_not_enabled` or empty data** - app was installed on a workspace
  rather than the org; reinstall at the org level.
- **`not_allowed_token_type`** - a bot token was supplied; the Audit Logs API
  requires the `xoxp-` user token.
- **HTTP 429** - rate limited. The endpoint is Tier 3 (about 50 calls/min);
  dfe-fetcher caps at 50 pages per fetch and polls on the configured
  interval.

## Cost

dfe-fetcher only reads. The Audit Logs API adds no charge of its own, but it
is available only on Slack Enterprise Grid, which is a paid plan. Standard
API rate limits apply. Confirm any cost implications against your own Slack
plan.

## References

- Using the Audit Logs API: https://docs.slack.dev/admins/audit-logs-api/
- `auditlogs:read` scope: https://api.slack.com/scopes/auditlogs:read
- Audit logs overview (admin help): https://slack.com/help/articles/360000394286-Audit-logs-in-Slack
- Slack rate limits: https://docs.slack.dev/apis/web-api/rate-limits/
