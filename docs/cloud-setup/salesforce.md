<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/salesforce.md       -->
<!-- Purpose:   Salesforce admin setup guide          -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Salesforce Setup for dfe-fetcher

What a Salesforce administrator configures so dfe-fetcher can read security and
audit data from a Salesforce org (read-only, pull-mode).

## Overview

dfe-fetcher authenticates server-to-server against
`<login_url>/services/oauth2/token` via one of two OAuth2 flows, selected by
which config fields are set:

- **JWT bearer** (recommended): an RS256-signed JWT with claims
  `{iss: client_id, sub: username, aud: login_url, exp: now+5m}`, signed with
  the app's RSA private key. No secret stored in dfe-fetcher, only the private
  key. Chosen when `private_key` or `private_key_secret` is set.
- **Client credentials**: `client_id` + `client_secret` form post. Chosen when
  a secret is set and no private key is.

The token response returns an `instance_url` (e.g.
`https://yourco.my.salesforce.com`) that dfe-fetcher uses for all subsequent
REST calls (you can pin it with `instance_url_override`). The JWT `aud` follows
`login_url` (`https://login.salesforce.com` for production,
`https://test.salesforce.com` for sandboxes).

Services (each emits source tag `salesforce.<service>`):

- `setup_audit_trail` - SOQL against `SetupAuditTrail`; admin config changes.
- `login_history` - SOQL against `LoginHistory`; login events.
- `event_log_file` - two-stage: SOQL lists `EventLogFile` rows in the window,
  then each row's `LogFile` body is downloaded and parsed from CSV to one
  record per line, with `_dfe_fetcher_event_type` / `_dfe_fetcher_log_date`
  provenance fields added.

## Prerequisites

- A Salesforce org (production or sandbox) and an admin who can create a
  connected app / external client app and a permission set.
- For JWT bearer: `openssl` to generate an RSA keypair and self-signed cert.
- For `event_log_file`: at least the free event types (EE/UE/Performance
  editions), or the Event Monitoring / Shield add-on for the full event-type
  set and longer retention.
- The REST API version is selected with `api_version`.

## Required Permissions

All permissions are read-only. The OAuth scope only needs **`api`** (Access the
Salesforce API) - no `full` or write scope.

| Service | sObject / endpoint | Integration-user permission | Add-on |
|---------|--------------------|-----------------------------|--------|
| `setup_audit_trail` | `SetupAuditTrail` (SOQL) | API Enabled + View Setup and Configuration | None (every org) |
| `login_history` | `LoginHistory` (SOQL) | API Enabled + Manage Users (or View All Users) | None (every org) |
| `event_log_file` | `EventLogFile` (SOQL + LogFile download) | API Enabled + View Event Log Files | Free event types with limited retention; the full event-type set and longer retention require Event Monitoring / Shield |

## Source-Side Setup

There is no Terraform path for this source. Connected-app / external-client-app
creation, certificate upload, and the pre-authorise / run-as policy are
UI / CLI steps in Salesforce Setup; the provider does not cover digital-signature
config cleanly. Follow the manual steps below.

> Spring '26 change: new orgs can no longer create classic **Connected Apps**
> without requesting the capability from Salesforce Support. The successor is the
> **External Client App (ECA)**, which supports the same JWT-bearer and
> client-credentials flows. Steps below use Connected App terminology; the ECA
> equivalents live under Setup -> App Manager -> New External Client App, with
> JWT-bearer configured under Flow Enablement and policies under the app's
> Policies tab. Existing connected apps keep working.

### 1. (JWT only) Generate an RSA keypair + self-signed certificate

```bash
# RSA private key - dfe-fetcher signs JWTs with this
openssl genrsa -out salesforce_dfe.key 2048

# Self-signed X.509 cert - upload this to the connected app
openssl req -x509 -new -nodes -key salesforce_dfe.key \
  -subj "/CN=dfe-fetcher" -days 730 -out salesforce_dfe.crt
```

Keep `salesforce_dfe.key` secret (config or vault). Upload `salesforce_dfe.crt`
in step 2.

### 2. Create the Connected App (or External Client App)

Setup -> App Manager -> New Connected App -> Create a Connected App.

- Connected App Name: `dfe-fetcher`; set a contact email.
- Tick **Enable OAuth Settings**.
- Callback URL: `https://login.salesforce.com/services/oauth2/callback`
  (unused server-to-server, but the form requires a value).
- Selected OAuth Scopes: add **Access the Salesforce API (api)**.
- JWT bearer only: tick **Use digital signatures** and upload
  `salesforce_dfe.crt`.
- Save (provisioning takes a few minutes).

Copy the **Consumer Key** (this is `client_id`) and, for client credentials, the
**Consumer Secret** (this is `client_secret`) from Manage Consumer Details.

### 3. Create / choose an integration user

Use a dedicated least-privilege user (a Salesforce Integration license is ideal).
Assign a permission set / profile granting the permissions from the table above:
API Enabled; View Setup and Configuration; Manage Users or View All Users; View
Event Log Files.

### 4. Wire the chosen auth flow

JWT bearer - Setup -> App Manager -> dfe-fetcher -> Manage -> Edit Policies:

- Permitted Users: **Admin approved users are pre-authorized**.
- Assign the integration user's permission set / profile to the app
  (Manage -> Profiles / Permission Sets). This pre-authorisation lets the JWT
  `sub` obtain a token without interactive consent.

Client credentials - Manage -> Edit Policies -> OAuth Policies:

- Tick **Enable Client Credentials Flow**.
- Set **Run As** to the integration user. Save and acknowledge the warning.

### 5. Smoke-test the token exchange

JWT bearer (via the `sf` CLI):

```bash
sf org login jwt \
  --client-id <CONSUMER_KEY> \
  --jwt-key-file salesforce_dfe.key \
  --username integration@yourco.com \
  --instance-url https://yourco.my.salesforce.com
```

Client credentials (raw token call):

```bash
curl -s https://login.salesforce.com/services/oauth2/token \
  -d grant_type=client_credentials \
  -d client_id=<CONSUMER_KEY> \
  -d client_secret=<CONSUMER_SECRET>
```

A success response carries `access_token` and `instance_url`. `invalid_grant`
on JWT usually means: the user is not pre-authorised (step 4), the cert does not
match the key, or `aud` is wrong (must be `https://login.salesforce.com`, or
`https://test.salesforce.com` for sandboxes).

## dfe-fetcher Configuration

### Config File

JWT bearer (recommended):

```yaml
sources:
  salesforce:
    enabled: true
    # login_url: "https://login.salesforce.com"   # sandbox: https://test.salesforce.com
    # api_version: "v60.0"
    client_id: "your-connected-app-consumer-key"
    username: "integration@yourco.com"
    private_key: "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
    services:
      - name: setup_audit_trail
      - name: login_history
      - name: event_log_file
        config:
          interval: "Daily"          # or "Hourly"
          event_types: ["Login", "Logout", "ApiTotalUsage", "ReportExport"]
    topic: "salesforce"
    # filter: 'EventType == "Login"'   # CEL, hot-reloaded
```

Client credentials: replace `username` + `private_key` with `client_secret`.

Recognised top-level fields: `enabled`, `login_url`, `api_version`, `client_id`,
`username`, `private_key`, `private_key_secret`, `client_secret`,
`credential_secret`, `instance_url_override`, `interval_secs`, `services`,
`topic`, `filter`. The `event_log_file` service config accepts `interval`
(`Daily`/`Hourly`) and `event_types` (allow-list; omit to pull every available
type).

### Environment Variables

Pattern: `DFE_FETCHER_SOURCES__SALESFORCE__<FIELD>` (double underscores).

```bash
DFE_FETCHER_SOURCES__SALESFORCE__ENABLED="true"
DFE_FETCHER_SOURCES__SALESFORCE__CLIENT_ID="your-consumer-key"
# JWT bearer:
DFE_FETCHER_SOURCES__SALESFORCE__USERNAME="integration@yourco.com"
DFE_FETCHER_SOURCES__SALESFORCE__PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
# or client credentials:
DFE_FETCHER_SOURCES__SALESFORCE__CLIENT_SECRET="your-consumer-secret"
```

### Secrets Manager

Keep the private key / consumer secret out of plain config. Both flows accept a
secret-store spec, which takes precedence over the inline field:

```yaml
sources:
  salesforce:
    enabled: true
    client_id: "your-consumer-key"
    username: "integration@yourco.com"
    private_key_secret: "vault:secret/dfe/salesforce:private_key"    # JWT bearer
    # credential_secret: "vault:secret/dfe/salesforce:client_secret" # client credentials
    services:
      - name: setup_audit_trail
      - name: login_history
      - name: event_log_file
    topic: "salesforce"
```

- `private_key_secret` resolves to the full RSA private-key PEM.
- `credential_secret` resolves to the consumer secret.

Sandboxes: set `login_url: "https://test.salesforce.com"`. My Domain: token
exchange still goes through `login_url`; the returned `instance_url` is your My
Domain host. For multiple orgs, run one dfe-fetcher instance per org, each with
its own app (and cert for JWT), integration user, and `instance_id`.

## Verification

1. **Health check.** `SalesforceSource::health_check` performs the token
   exchange and confirms an `instance_url` came back. It does not exercise
   per-sObject read permissions.

2. **Env-gated smoke tests.** Live tests live in
   [`tests/e2e/smoke_remote.rs`](../../tests/e2e/smoke_remote.rs), all
   `#[ignore]`'d. They read these from `.env-cloud`: `SALESFORCE_CLIENT_ID`
   (required) plus one of `SALESFORCE_PRIVATE_KEY` /
   `SALESFORCE_PRIVATE_KEY_SECRET` (with `SALESFORCE_USERNAME`) or
   `SALESFORCE_CLIENT_SECRET` / `SALESFORCE_CREDENTIAL_SECRET`. Optional:
   `SALESFORCE_LOGIN_URL`, `SALESFORCE_API_VERSION`,
   `SALESFORCE_INSTANCE_URL`.

   ```bash
   cargo test --test e2e salesforce_ -- --ignored --nocapture
   ```

3. **Common failures.**
   - Auth error / missing auth fields: supply `client_id` plus a private key
     (JWT) or `client_secret` (client credentials).
   - `invalid_grant` (JWT): user not pre-authorised, cert/key mismatch, or wrong
     `aud`/`login_url` (sandbox vs production).
   - SOQL `403`/`INVALID_TYPE`: integration user lacks the relevant
     read permission (e.g. View Event Log Files).
   - `event_log_file` empty: log files are generated asynchronously - up to ~24h
     for `Daily`, ~1h for `Hourly`. Tight windows see empty results.

## Cost

dfe-fetcher only reads. The SOQL surfaces (`setup_audit_trail`,
`login_history`) and a small built-in set of `event_log_file` event types
are available on standard orgs. The full EventLogFile event-type set and
longer retention may require a paid add-on (Event Monitoring or Salesforce
Shield), so there could be a cost depending on what you need. API calls
count against the org's API request allowance. Confirm any cost implications
against your own Salesforce agreement.

## References

- OAuth 2.0 JWT bearer flow (server-to-server): <https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5>
- Configure JWT bearer flow for External Client Apps: <https://help.salesforce.com/s/articleView?id=xcloud.configure_oauth_jwt_flow_external_client_apps.htm&type=5>
- EventLogFile object reference: <https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_eventlogfile.htm>
- EventLogFile supported event types: <https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_eventlogfile_supportedeventtypes.htm>
- Using Event Monitoring (REST): <https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/using_resources_event_log_files.htm>
- SOQL query REST resource: <https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_query.htm>
