<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/datadog.md           -->
<!-- Purpose:   Datadog cloud admin setup guide       -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Datadog Setup for dfe-fetcher

What a Datadog administrator configures so dfe-fetcher can read an
organisation's audit trail and security signals.

## Overview

dfe-fetcher reads two Datadog v2 endpoints, one Datadog organisation per
fetcher instance:

- `GET /api/v2/audit/events` -- the Audit Trail, as the `audit_events` unit.
- `GET /api/v2/security_monitoring/signals` -- security signals, as the
  `security_signals` unit.

Each tick asks for the scheduler's window as `filter[from]` and `filter[to]`
in RFC 3339 with millisecond precision, sorted `timestamp` so the oldest event
comes first, `page[limit]` rows to a page. It follows `meta.page.after` into
`page[cursor]` until Datadog leaves the cursor out, which is the last page. An
optional `filter[query]` in Datadog's own search syntax narrows either unit.

Datadog authenticates a call with two credentials in two headers.
`DD-API-KEY` names the organisation and grants nothing on its own.
`DD-APPLICATION-KEY` carries the permissions of the user who created it. The
call is refused without both. These are separate objects created in separate
places, which is the part operators get wrong most often.

The source is the shipped `datadog` profile
(`crates/fetcher/profiles/datadog.yaml`), configured as an instance under
`sources.rest`. There is no typed `sources.datadog` block.

A 408, 429 or 5xx is retried with backoff, honouring `Retry-After`. A 401 or
403 ends the tick and is never retried, because a refusal will not resolve
itself. A tick that fails does not advance the fetch window, so the next tick
re-fetches. Datadog returns its reason as `{"errors": ["..."]}` and the fetcher
reports that as the tick's failure. The site's rate-limit headers
`x-ratelimit-limit` and `x-ratelimit-remaining` surface as the
`dfe_fetcher_api_quota_api_rate_limit` and
`dfe_fetcher_api_quota_api_rate_remaining` gauges.

`vars.api_url` is the API host of the organisation's site and defaults to US1.
An organisation on any other site needs its own -- see the site table below.

## Prerequisites

- A Datadog organisation, and knowing which site it is on. A key pair issued
  on one site is not valid on another.
- **Audit Trail enabled** on that organisation. Datadog records nothing until
  an administrator turns it on, so `audit_events` returns empty pages on an
  organisation that never has.
- A product that writes security signals. Cloud SIEM analyses incoming logs
  and generates them. Without one, `security_signals` returns empty pages.
- An administrator who can create an API key and an application key in
  Organization Settings.
- A Datadog user whose roles carry the two read permissions below. The
  application key inherits that user's permissions, so its owner decides what
  the fetcher can read.
- Network egress from the fetcher host to the site's API host.

## Required Permissions

| Unit | Endpoint | Permission | In by default |
|------|----------|------------|---------------|
| `audit_events` | `GET /api/v2/audit/events` | **Audit Trail Read** (`audit_logs_read`), under Compliance | Datadog Admin Role |
| `security_signals` | `GET /api/v2/security_monitoring/signals` | **Security Signals Read** (`security_monitoring_signals_read`), under Cloud Security Platform | Datadog Read Only Role |

Both are read permissions. dfe-fetcher never writes, modifies or deletes
anything in Datadog.

Grant them to the user who will own the application key, through a role under
**Organization Settings** -> **Roles**. A custom role does not pick up newly
released permissions unless it is set to take automatic updates, so add each
one explicitly. Turning Audit Trail on is a different permission again,
**Audit Trail Write**, needed once at setup and never by the fetcher.

An application key can also be scoped, which narrows it and never widens it.
Datadog's API specification lists `security_monitoring_signals_read` among the
authorization scopes a key can carry and lists no scope for the audit
endpoint. Leave the key unscoped, or confirm `audit_events` still answers
before relying on a scoped one.

## Source-Side Setup

### 1. Enable Audit Trail

**Organization Settings** -> **Audit Trail Settings**, under **COMPLIANCE**,
then **Enable**. This needs the Audit Trail Write permission. Retention is
chosen on the same page, and a fetch window reaching further back than the
retention period returns nothing for the expired part of it.

### 2. Grant the read permissions

**Organization Settings** -> **Roles**. Give the user who will create the
application key a role carrying Audit Trail Read and Security Signals Read.
The two are granted separately, so a user can hold one and not the other.

### 3. Create the API key

**Organization Settings** -> **API Keys** -> **New Key**. Name it
`dfe-fetcher` and copy the value. The api key names the organisation and
carries no permissions of its own.

### 4. Create the application key

Signed in as the user from step 2: **Organization Settings** -> **Application
Keys** -> **New Key**. Copy the value. The key carries that user's permissions
as they stand at each call, and Datadog revokes a user's application keys when
their account is disabled -- an api key the same user created stays valid.
Pick an owner who will not be offboarded out from under the fetcher.

Key names are unique across the organisation, so name the pair for the
deployment rather than for the product.

### 5. Find the site's API host

The API host is the `api` subdomain of the site's parameter. That is
`vars.api_url`.

| Site | Site URL | `vars.api_url` | Location |
|------|----------|----------------|----------|
| US1 | `https://app.datadoghq.com` | `https://api.datadoghq.com` | US |
| US3 | `https://us3.datadoghq.com` | `https://api.us3.datadoghq.com` | US |
| US5 | `https://us5.datadoghq.com` | `https://api.us5.datadoghq.com` | US |
| EU1 | `https://app.datadoghq.eu` | `https://api.datadoghq.eu` | EU (Germany) |
| AP1 | `https://ap1.datadoghq.com` | `https://api.ap1.datadoghq.com` | Japan |
| AP2 | `https://ap2.datadoghq.com` | `https://api.ap2.datadoghq.com` | Australia |
| UK1 | `https://uk1.datadoghq.com` | `https://api.uk1.datadoghq.com` | UK |
| US1-FED | `https://app.ddog-gov.com` | `https://api.ddog-gov.com` | US |
| US2-FED | `https://us2.ddog-gov.com` | `https://api.us2.ddog-gov.com` | US |

The site is whichever URL the organisation's dashboard is on. Getting this
wrong is the most likely first failure, and it reads as a credential problem
rather than a hostname one.

## dfe-fetcher Configuration

The instance lives under `sources.rest` and is keyed by its connection id,
which is the cursor key, the metric and log label, and the `_source_fetcher`
prefix (`datadog.audit_events`). Both units are incremental, so their rows
land on the instance's `topic` and are told apart by `_source_fetcher`.

### Config File

The stanza below is the one in
[config.example.yaml](../../config.example.yaml), which stays the annotated
reference:

```yaml
sources:
  rest:
    datadog:
      enabled: true
      topic: "datadog"
      interval_secs: 300
      auth:
        mode: credentials
        credentials:
          api_key: "vault:kv/data/datadog/prod:api_key"
          application_key: "vault:kv/data/datadog/prod:application_key"
      vars:
        api_url: "https://api.datadoghq.com"   # the API host of this org's site
      # units:                                  # optional per-unit narrowing
      #   audit_events: { vars: { filter_query: "@evt.name:Dashboard" } }
      #   security_signals: { enabled: false }
      profile: datadog
```

`vars.api_url` overrides the profile's US1 default.
`units.<name>.vars.filter_query` narrows one unit with Datadog's own search
syntax, and a value that renders empty is left off the request, which is why
the profile's default sends no `filter[query]` at all.
`units.<name>.enabled: false` leaves a unit out -- set it on
`security_signals` on an organisation that runs nothing producing signals.
`vars.page_limit` sets `page[limit]`, which Datadog caps at 1000.

### Environment Variables

Scalar fields follow the `DFE_FETCHER_SOURCES__REST__<ID>__<FIELD>` pattern
(double underscores, nested keys joined the same way):

```bash
DFE_FETCHER_SOURCES__REST__DATADOG__ENABLED="true"
DFE_FETCHER_SOURCES__REST__DATADOG__VARS__API_URL="https://api.datadoghq.eu"
```

Define the instance in the config file and inject the credentials through the
environment, which a credential spec reads directly:

```yaml
        credentials:
          api_key: "env:DD_API_KEY"
          application_key: "env:DD_APP_KEY"
```

### Secrets Manager

Both credential fields are specs: `vault:<mount>/data/<path>:<key>` resolved
when first used, `env:VAR`, or a literal. Neither key belongs in a committed
config as a literal.

## Verification

1. **No health probe, deliberately.** Datadog's cheap validation
   endpoint checks the api key alone, and a health check that passed on half
   the credential is worse than none, so the `datadog` profile declares no
   probe. The health check resolves both credential specs instead and sends
   nothing to Datadog. A passing check proves the secrets resolve, not that
   Datadog accepts them. The first tick is what proves that.

2. **Validate the config.** `dfe-fetcher config-check --config <file>` loads
   the config without running. An instance supplying only one of `api_key` and
   `application_key` is refused at load, naming the field the profile could
   not read.

3. **Smoke-test the key pair** against the site's API host:

   ```bash
   printf 'header = "DD-API-KEY: %s"\nheader = "DD-APPLICATION-KEY: %s"\n' \
     "$DD_API_KEY" "$DD_APP_KEY" |
     curl -sS --globoff --config - \
       "https://api.datadoghq.com/api/v2/audit/events?page[limit]=1"
   ```

   Both keys reach curl through `--config -` rather than an argument: a
   command line is readable by any user through `ps` and is kept in shell
   history, and `printf` is a shell builtin, so it starts no process of its
   own. `--globoff` stops curl reading the brackets in `page[limit]` as a
   range. Swap the path for `/api/v2/security_monitoring/signals` to test the
   other unit -- the two permissions are granted separately, so one endpoint
   can answer while the other refuses.

4. **Watch the first tick.** The driver logs `unit tick complete` per unit
   with `rows`, `filtered`, `oversize` and `flushes`. Datadog's rate-limit
   headers arrive as the `dfe_fetcher_api_quota_api_rate_limit` and
   `dfe_fetcher_api_quota_api_rate_remaining` gauges, labelled by source.

5. **Common failure modes.**
   - `401` with `{"errors":["Unauthorized"]}`: one of the two headers never
     arrived. A `credentials` instance sends both or fails at load, so on a
     running fetcher this points at something between the fetcher and Datadog
     stripping a header.
   - `403` with `{"errors":["Forbidden"]}`: both headers arrived and the pair
     was refused. The body does not say which half. Check that the two keys
     belong to the same organisation and that `vars.api_url` is that
     organisation's site, then check the application key owner's roles.
   - Either ends the tick, neither is retried, and the fetch window does not
     advance.
   - `429`: the site's rate limit. Retried with backoff. Lengthen
     `interval_secs` if it recurs.
   - `audit_events` returns an empty page every tick: Audit Trail is not
     enabled, the window is outside the retention period, or a `filter_query`
     matches nothing.
   - `security_signals` returns an empty page every tick: no detection rules
     are producing signals.
   - Everything refuses after a period of working: the application key's owner
     was disabled, which revokes their application keys.

## Cost

dfe-fetcher only reads. Datadog's Audit Trail documentation does not state a
plan requirement or a charge for reading audit events through the API, and
Audit Trail does not appear on Datadog's pricing documentation. It is off
until an administrator turns it on, so treat it as a product to enable rather
than a feature already included, and confirm it against your own Datadog
contract. Cloud SIEM, which produces the security signals, is billed on the
volume of logs it analyses -- reading the signals back adds nothing to that.

Each tick is one request per unit plus one per further page. Size
`interval_secs` and `vars.page_limit` to the organisation's event volume
rather than polling aggressively, and watch the quota gauges.

## References

- API and Application Keys (creating each, scopes, revocation): https://docs.datadoghq.com/account_management/api-app-keys/
- Datadog sites (the site table): https://docs.datadoghq.com/getting_started/site/
- Datadog Role Permissions: https://docs.datadoghq.com/account_management/rbac/permissions/
- Datadog Audit Trail (enabling it, retention, permissions): https://docs.datadoghq.com/account_management/audit_trail/
- Audit API: https://docs.datadoghq.com/api/latest/audit/
- Security Monitoring API: https://docs.datadoghq.com/api/latest/security-monitoring/
- API rate limits and their headers: https://docs.datadoghq.com/api/latest/rate-limits/
- Cloud SIEM: https://docs.datadoghq.com/security/cloud_siem/
- Datadog pricing (products and billing units): https://docs.datadoghq.com/account_management/billing/pricing/
- Datadog v2 OpenAPI specification (the permission each endpoint requires, its parameters and its paging): https://raw.githubusercontent.com/DataDog/datadog-api-client-go/master/.generator/schemas/v2/openapi.yaml
