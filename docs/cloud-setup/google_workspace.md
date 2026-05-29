<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/google_workspace.md  -->
<!-- Purpose:   Google Workspace cloud admin setup guide -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Google Workspace Setup for dfe-fetcher

What a Google Workspace / GCP administrator needs to configure so dfe-fetcher
can read audit and activity data from the Workspace Reports API.

> Status: alpha - code-complete, not production-validated; additionally
> pending hyperi-infra#5 (domain-wide-delegation service account) before it
> can be exercised against a live tenant.

## Overview

dfe-fetcher reads the Google Workspace **Reports API** (part of the Admin SDK
API). For each configured Workspace application it calls
`GET admin.googleapis.com/admin/reports/v1/activity/users/all/applications/<app>`
with a `startTime`/`endTime` window, follows `nextPageToken` until the window is
drained, and emits one record per activity tagged `google_workspace.<app>`.

Authentication is an OAuth2 **service account with domain-wide delegation**,
using the JWT-with-subject variant. The service account signs an RS256 JWT whose
`sub` claim impersonates a designated Workspace admin email, then exchanges it
for an access token scoped to that admin's tenant. The only scope requested is
read-only:

- `https://www.googleapis.com/auth/admin.reports.audit.readonly`

Two trust steps are required and they live in different consoles: the service
account and the Admin SDK API enablement are in **Google Cloud**, while the
domain-wide-delegation scope grant is in the **Workspace Admin console**. The
Admin console step is human-only - there is no IaC path for it.

dfe-fetcher never writes, modifies, or deletes anything.

## Prerequisites

- A Google Cloud project (any project; it only hosts the service account).
- A **super administrator** account in the target Workspace tenant - required
  to authorise domain-wide delegation in the Admin console.
- The Workspace admin email that the service account will impersonate. It must
  hold the **Reports** administrator privilege (Alert Center view access is
  also useful if you later add alert feeds).
- `gcloud` CLI authenticated against the host project
  (`gcloud auth login`).

## Required Permissions

| Service | Object/Endpoint/Scope | IAM role or grant | Notes |
|---------|-----------------------|-------------------|-------|
| Admin SDK API (Reports) | `admin/reports/v1/activity/users/all/applications/<app>` | n/a (API enabled on the GCP project) | Must be enabled on the host project or every call 403s |
| Domain-wide delegation | `https://www.googleapis.com/auth/admin.reports.audit.readonly` | SA numeric client ID authorised in Admin console | Scope string must match EXACTLY |
| Impersonated admin | `admin_email` (the JWT `sub`) | Workspace **Reports** administrator role | Logs belong to the domain; the SA cannot read them without impersonating an admin |

All access is **read-only**. No write or admin scope is required.

## Source-Side Setup

### 1. Enable the Admin SDK API on the GCP project

Console path: Google Cloud console -> APIs and Services -> Library -> search
"Admin SDK API" -> Enable. Or via CLI:

```bash
gcloud services enable admin.googleapis.com --project=your-host-project
```

If this API is not enabled, every Reports call fails even when delegation looks
correct.

### 2. Create the service account

```bash
gcloud iam service-accounts create dfe-fetcher-workspace \
  --display-name="dfe-fetcher Workspace Reports" \
  --project=your-host-project
```

### 3. Generate a JSON key

```bash
gcloud iam service-accounts keys create workspace-sa.json \
  --iam-account=dfe-fetcher-workspace@your-host-project.iam.gserviceaccount.com
```

Store `workspace-sa.json` securely - it contains the RSA private key dfe-fetcher
signs JWTs with.

### 4. Find the service account's numeric client ID

The Admin console only accepts the **numeric** client ID (the unique ID), not
the service-account email. Console path: Google Cloud console -> IAM and Admin
-> Service Accounts -> select the account -> Advanced settings -> copy the
Client ID. Or via CLI:

```bash
gcloud iam service-accounts describe \
  dfe-fetcher-workspace@your-host-project.iam.gserviceaccount.com \
  --format='value(uniqueId)'
```

The same numeric ID is the `client_id` field in `workspace-sa.json`.

### 5. Authorise domain-wide delegation in the Admin console (human-only)

Sign in to the Workspace Admin console as a **super administrator**. Path:
Menu -> Security -> Access and data control -> API controls -> Domain-wide
delegation -> Manage Domain Wide Delegation -> Add new.

- **Client ID**: the numeric client ID from step 4.
- **OAuth scopes** (comma-delimited, no spaces):
  `https://www.googleapis.com/auth/admin.reports.audit.readonly`
- Click Authorize.

Notes:
- The scope string must match what dfe-fetcher requests EXACTLY, or token
  exchange fails with "Client is unauthorized" / "not authorized for any of the
  scopes requested".
- If Multi-party approval is enabled, a second super admin must approve.
- Propagation usually takes a few minutes but can take up to 24 hours.

### 6. Confirm the impersonated admin has Reports access

The `admin_email` you set must be a real Workspace admin with the **Reports**
administrator privilege. Console path: Admin console -> Account -> Admin roles.

## dfe-fetcher Configuration

Service names map directly to Reports API `applicationName` values. Current
values include: `login`, `admin`, `drive`, `token`, `mobile`, `groups`,
`groups_enterprise`, `calendar`, `chat`, `meet`, `chrome`, `keep`,
`access_transparency`, `context_aware_access`. Each entry produces records
tagged `google_workspace.<name>`. Per-service config supports an optional
`event_name` to filter to a single event.

### Config File

```yaml
sources:
  google_workspace:
    enabled: true
    service_account_key: "/etc/workspace/workspace-sa.json"
    admin_email: "audit-admin@example.com"      # MUST be a Workspace admin
    customer_id: "my_customer"                   # default; tenant of admin_email
    # api_url_override: "https://admin.googleapis.com"            # default
    # token_url_override: "https://oauth2.googleapis.com/token"   # default
    services:
      - name: login
      - name: admin
      - name: drive
      - name: token
      - name: groups
      - name: meet
        config:
          event_name: "call_ended"   # optional single-event filter
    topic: "google_workspace"
    # filter: 'id.applicationName == "login"'   # hot-reloaded
```

`customer_id` defaults to `my_customer`, which resolves to the tenant the
impersonated admin belongs to - correct for single-tenant deployments. Set an
explicit C-prefixed ID only for reseller scenarios.

### Environment Variables

```bash
DFE_FETCHER_SOURCES__GOOGLE_WORKSPACE__ENABLED="true"
DFE_FETCHER_SOURCES__GOOGLE_WORKSPACE__SERVICE_ACCOUNT_KEY="/etc/workspace/workspace-sa.json"
DFE_FETCHER_SOURCES__GOOGLE_WORKSPACE__ADMIN_EMAIL="audit-admin@example.com"
DFE_FETCHER_SOURCES__GOOGLE_WORKSPACE__CUSTOMER_ID="my_customer"
```

### Secrets Manager

Keep the SA key out of the config file with a vault spec:

```yaml
sources:
  google_workspace:
    enabled: true
    credential_secret: "vault:secret/google_workspace:sa_key"
    admin_email: "audit-admin@example.com"
    services:
      - name: login
      - name: admin
    topic: "google_workspace"
```

`credential_secret` resolves to the full service account JSON key as a string.
Provide exactly one of `credential_secret` or `service_account_key`.

## Verification

dfe-fetcher's `health_check` for this source signs a JWT and performs the token
exchange; it returns `true` only when delegation and impersonation are wired
correctly. The env-gated e2e tests in
[`tests/e2e/smoke_remote.rs`](../../tests/e2e/smoke_remote.rs) exercise the live
path. They are `#[ignore]`'d until the tenant side is provisioned.

```bash
export GOOGLE_WORKSPACE_SA_KEY="/etc/workspace/workspace-sa.json"
# or GOOGLE_WORKSPACE_CREDENTIAL_SECRET="vault:secret/google_workspace:sa_key"
export GOOGLE_WORKSPACE_ADMIN_EMAIL="audit-admin@example.com"

# Token exchange + scope grant only:
cargo test --test smoke_remote google_workspace_health_check -- --ignored --nocapture

# Real login activity fetch (empty window is a valid pass):
cargo test --test smoke_remote google_workspace_login_activity_fetch -- --ignored --nocapture
```

Common failures:

- `401/403 unauthorized_client` on token exchange - scope in the Admin console
  does not match `admin.reports.audit.readonly` exactly, or delegation has not
  propagated yet (wait up to 24h).
- `403` on the Reports call - `admin_email` is not a Workspace admin, or lacks
  the Reports administrator privilege.
- `missing client_email / private_key` - the SA key JSON is malformed or the
  wrong file.
- Empty results - a quiet window is normal; widen the window or pick a busier
  application such as `login`.

## Cost

dfe-fetcher only reads. Service accounts, the Admin SDK / Reports API, and
audit activity reads are not expected to carry an additional charge, though
this can depend on your Google Workspace plan. Confirm any cost implications
against your own Google Workspace and Google Cloud agreements.

## References

- Reports API overview: https://developers.google.com/workspace/admin/reports/v1/overview
- activities.list (applicationName values): https://developers.google.com/workspace/admin/reports/reference/rest/v1/activities/list
- Control API access with domain-wide delegation: https://knowledge.workspace.google.com/admin/apps/control-api-access-with-domain-wide-delegation
- Perform Google Workspace domain-wide delegation of authority: https://developers.google.com/workspace/cloud-search/docs/guides/delegation
- Create access credentials (service account): https://developers.google.com/workspace/guides/create-credentials
