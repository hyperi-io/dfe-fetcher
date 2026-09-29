<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/github.md            -->
<!-- Purpose:   GitHub cloud admin setup guide        -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# GitHub Setup for dfe-fetcher

What a GitHub administrator needs to configure so dfe-fetcher can read
audit-log data from a GitHub organisation or enterprise account.

## Overview

dfe-fetcher pulls events from the GitHub audit log over the REST API at
`api.github.com`, paging through the `Link: rel="next"` header. It reads
exactly one of two endpoints per fetcher instance:

- Organisation scope: `GET /orgs/{org}/audit-log`
- Enterprise scope: `GET /enterprises/{enterprise}/audit-log`

A service-config `include` selects `web`, `git`, or `all` event classes.
Every request carries `Accept: application/vnd.github+json` and the pinned
`X-GitHub-Api-Version: 2022-11-28` header. Authentication is a bearer token
(classic Personal Access Token, fine-grained PAT, or a GitHub App
installation token). The audit-log API is available only on GitHub
Enterprise Cloud - both the org-level and enterprise-level endpoints require
your organisation to be part of an Enterprise Cloud plan.

The source is the shipped `github` REST profile
(`crates/fetcher/profiles/github.yaml`); the `sources.github` block below
maps onto an instance of it at load, so the profile's retry policy applies:
a 429 or 5xx is retried with backoff (honouring `Retry-After`), a 401 or 403
ends the tick, and a tick that fails does not advance the fetch window.

## Prerequisites

- GitHub Enterprise Cloud. The audit-log REST API is not available on free,
  Team, or standalone organisations - the org must belong to an enterprise.
- For org scope: organisation owner (or a member who can read the org audit
  log). For enterprise scope: enterprise owner / admin.
- Ability to create a token (or register a GitHub App) for a dedicated
  service identity.
- A note on token types: the classic PAT path (`read:audit_log`) is the
  reliable, documented path for both endpoints today. Fine-grained PAT and
  GitHub App support for the audit-log list endpoint exists but has been
  inconsistent in practice - prefer a classic PAT unless your org policy
  blocks them. Fine-grained PATs are NOT supported for the audit-log
  streaming endpoints (dfe-fetcher does not use those; it uses the list
  endpoint).

## Required Permissions

| Service | Endpoint | Permission or Scope | Notes |
|---------|----------|---------------------|-------|
| `audit_log` (org) | `GET /orgs/{org}/audit-log` | Classic PAT scope `read:audit_log` | Reliable path. Token owner must be able to read the org audit log. |
| `audit_log` (org) | `GET /orgs/{org}/audit-log` | Fine-grained / GitHub App: org `Administration: Read` | Supported but historically flaky; verify with `X-Accepted-GitHub-Permissions` response header. |
| `audit_log` (enterprise) | `GET /enterprises/{enterprise}/audit-log` | Classic PAT scope `read:audit_log` | Token owner must be an enterprise admin. |
| `audit_log` (enterprise) | `GET /enterprises/{enterprise}/audit-log` | Fine-grained / GitHub App: enterprise `Enterprise administration: Read` | Alternative to classic PAT. |

All access is read-only. There is no GitHub OAuth scope literally named
`read:enterprise` for this endpoint - enterprise access uses the same
`read:audit_log` scope (classic) or the `Enterprise administration: Read`
permission (fine-grained / App).

## Source-Side Setup

### Option A - Classic Personal Access Token (recommended)

1. Sign in as a user with audit-log access (org owner for org scope, or
   enterprise admin for enterprise scope). Use a dedicated service account
   where possible.
2. Top-right profile picture -> **Settings** -> left sidebar bottom
   -> **Developer settings** -> **Personal access tokens** -> **Tokens
   (classic)** -> **Generate new token (classic)**.
3. Name it `dfe-fetcher-audit`, set an expiration (rotate within that
   window), and select the **`read:audit_log`** scope. If that scope is not
   offered, select `admin:org` -> `read:org` as a fallback.
4. **Generate token** and copy it immediately - GitHub shows it once.

   ```bash
   # Smoke-test the org endpoint (replace ORG and TOKEN):
   curl -sS "https://api.github.com/orgs/ORG/audit-log?per_page=1&include=all" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Accept: application/vnd.github+json" \
     -H "X-GitHub-Api-Version: 2022-11-28"
   ```

### Option B - Fine-grained Personal Access Token

1. **Settings** -> **Developer settings** -> **Personal access tokens**
   -> **Fine-grained tokens** -> **Generate new token**.
2. Set **Resource owner** to the target organisation (fine-grained tokens
   max out at 366 days; non-expiring is not allowed).
3. Under **Organization permissions**, set **Administration** to
   **Read-only** (enterprise endpoint: set **Enterprise administration** to
   Read). Grant nothing else.
4. **Generate token**. If the org requires approval, the token stays pending
   (and can read only public data) until an org owner approves it under the
   org's **Settings -> Personal access tokens -> Pending requests**.
5. Confirm the endpoint accepts it by inspecting the
   `X-Accepted-GitHub-Permissions` response header on a test call.

### Option C - GitHub App installation token

Register an App (org or enterprise **Settings -> Developer settings ->
GitHub Apps**) with the `Administration: Read` (org) or `Enterprise
administration: Read` permission, install it on the org/enterprise, and have
dfe-fetcher's caller supply the short-lived installation token as `token`.

## dfe-fetcher Configuration

### Config File

Set exactly one of `org` or `enterprise`.

```yaml
sources:
  github:
    enabled: true
    org: "your-github-org"          # OR: enterprise: "your-enterprise-slug"
    token: "ghp_your_token_here"
    services:
      - name: audit_log
        config:
          include: "all"            # all | web | git
    topic: "github"
    # filter: 'action != "git.clone"'   # CEL, hot-reloaded
```

### Environment Variables

Pattern is `DFE_FETCHER_SOURCES__GITHUB__<FIELD>` (double underscores).

```bash
DFE_FETCHER_SOURCES__GITHUB__ENABLED="true"
DFE_FETCHER_SOURCES__GITHUB__ORG="your-github-org"
# or: DFE_FETCHER_SOURCES__GITHUB__ENTERPRISE="your-enterprise-slug"
DFE_FETCHER_SOURCES__GITHUB__TOKEN="ghp_your_token_here"
```

### Secrets Manager

Keep the token out of the config file with `credential_secret`, which
resolves to the literal token value:

```yaml
sources:
  github:
    enabled: true
    org: "your-github-org"
    credential_secret: "vault:kv/data/github:token"
    services:
      - name: audit_log
        config:
          include: "all"
    topic: "github"
```

## Verification

- **Health check.** The profile's probe calls `GET /user` with the token; a
  2xx means the token authenticates and `api.github.com` is reachable. Note
  this only proves the token is valid, not that it carries audit-log
  permission.
- **e2e smoke test.** `crates/fetcher/tests/e2e/smoke_remote.rs` has
  `#[ignore]`-gated live tests. Export credentials (or put them in
  `.env-cloud`), then run:

  ```bash
  export GITHUB_AUDIT_TOKEN="ghp_..."
  export GITHUB_AUDIT_ORG="your-github-org"   # OR GITHUB_AUDIT_ENTERPRISE
  cargo test -p dfe-fetcher --test e2e github_ -- --ignored
  ```

  Covers the health check, audit-log fetch, and git-only include variant.
- **Common failure modes.**
  - `403 Forbidden` with a valid token: token lacks `read:audit_log` (or the
    fine-grained permission), or the org is not on Enterprise Cloud.
  - `404 Not Found`: wrong `org`/`enterprise` slug, or the audit-log API is
    not available for that account tier.
  - Empty result every tick: no events in the lookback window; git events are
    retained only ~7 days, so a stale window returns nothing.
  - Setting both `org` and `enterprise`, or neither, is a config error: the
    fetcher refuses the config at load, naming `sources.github`.

## Cost

dfe-fetcher only reads. The audit-log REST API adds no charge of its own,
but it requires GitHub Enterprise Cloud, which is a paid plan. Standard API
rate limits apply. Confirm any cost implications against your own GitHub
plan.

## References

- Reviewing the audit log for your organization (Enterprise Cloud):
  <https://docs.github.com/en/enterprise-cloud@latest/organizations/keeping-your-organization-secure/managing-security-settings-for-your-organization/reviewing-the-audit-log-for-your-organization>
- Using the audit log API for your enterprise:
  <https://docs.github.com/en/enterprise-cloud@latest/admin/monitoring-activity-in-your-enterprise/reviewing-audit-logs-for-your-enterprise/using-the-audit-log-api-for-your-enterprise>
- REST API endpoints for enterprise audit logs:
  <https://docs.github.com/en/enterprise-cloud@latest/rest/enterprise-admin/audit-log>
- Access the Audit Log REST API using scoped tokens (read:audit_log):
  <https://github.blog/changelog/2022-12-19-access-the-audit-log-rest-api-using-scoped-tokens/>
- Managing your personal access tokens:
  <https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens>
- Permissions required for fine-grained personal access tokens:
  <https://docs.github.com/en/rest/authentication/permissions-required-for-fine-grained-personal-access-tokens>
