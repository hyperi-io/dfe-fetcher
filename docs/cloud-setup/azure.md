<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/azure.md            -->
<!-- Purpose:   Azure cloud admin setup guide         -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   FSL-1.1-ALv2                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Azure Setup for dfe-fetcher

What a cloud administrator configures so dfe-fetcher can pull security and
operational data from Azure and Microsoft Entra ID.

## Overview

dfe-fetcher polls Azure APIs on an interval and ships each record to Kafka.
It supports these services: `activity_log` (subscription Activity Log),
`defender` (Microsoft Defender for Cloud alerts), `sentinel` (Microsoft
Sentinel incidents), the three split Entra ID Graph audit feeds
(`entra_signins`, `entra_directory_audits`, `entra_provisioning`), and
`log_analytics` (arbitrary KQL queries against a Log Analytics workspace).
All access is read-only. dfe-fetcher authenticates with the OAuth2
`client_credentials` grant using a single Entra ID app registration and
client secret, requesting one of three audiences per call:

- `https://management.azure.com/.default` - Activity Log, Defender, Sentinel.
- `https://graph.microsoft.com/.default` - the Entra ID audit feeds.
- `https://api.loganalytics.io/.default` - Log Analytics queries.

## Prerequisites

- An Azure subscription and an Entra ID tenant.
- Permission to create an app registration and to assign subscription RBAC,
  plus a Global Administrator (or Privileged Role Administrator) to grant
  admin consent for the Graph application permission.
- Azure CLI authenticated (`az login`) for the manual or Terraform paths.
- For `sentinel`: a Log Analytics workspace with Microsoft Sentinel enabled,
  and its resource group + workspace name.
- For `log_analytics`: the target workspace GUID and the `Log Analytics
  Reader` role for the service principal on that workspace.
- Cost flags: dfe-fetcher only reads, but some services here (e.g. Sentinel,
  Defender for Cloud, Log Analytics) may carry a cost depending on your Azure
  plan and what is enabled. See the Cost section.

## Required Permissions

| Service | API / Audience | Role / Permission | Type |
|---------|----------------|-------------------|------|
| `activity_log` | Azure Management | `Reader` on subscription | RBAC |
| `defender` | Azure Management (`Microsoft.Security/alerts`) | `Reader` on subscription | RBAC |
| `sentinel` | Azure Management (`Microsoft.SecurityInsights/incidents`) | `Reader` on subscription | RBAC |
| `entra_signins` | Microsoft Graph (`auditLogs/signIns`) | `AuditLog.Read.All` | Application |
| `entra_directory_audits` | Microsoft Graph (`auditLogs/directoryAudits`) | `AuditLog.Read.All` | Application |
| `entra_provisioning` | Microsoft Graph (`auditLogs/provisioning`) | `AuditLog.Read.All` (or scoped `ProvisioningLog.Read.All`) | Application |
| `log_analytics` | Log Analytics (`api.loganalytics.io`) | `Log Analytics Reader` on the workspace | RBAC |

Notes:

- The `Reader` RBAC role on the subscription grants `*/read`, which covers
  `Microsoft.Security/*/read` (Defender) and the Activity Log and Sentinel
  management reads. Defender alerts and Sentinel incidents do not need a
  separate `Security Reader` role.
- `AuditLog.Read.All` (application permission ID
  `df021288-bdef-4463-88db-98f22de89214`) covers all three Entra audit
  endpoints. For least privilege on provisioning only, the more scoped
  `ProvisioningLog.Read.All` also works for `entra_provisioning`.
- The Microsoft Graph resource app ID is always
  `00000003-0000-0000-c000-000000000000`.
- All permissions are read-only.

## Source-Side Setup

### Terraform (Automated)

The test infrastructure is defined in
[`infra/test/main.tf`](../../infra/test/main.tf) (Azure section). It creates
the app registration `dfe-fetcher-test`, a service principal, a client
secret (1-year expiry), and assigns the `Reader` role on the subscription.

```bash
cd infra/test
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars - set azure_subscription_id
az login
terraform init
terraform apply
terraform output -json | python3 gen-env.py > ../../.env
```

The Terraform provisions only the subscription `Reader` role, which covers
`activity_log`, `defender`, and `sentinel`. For the Entra audit feeds you
must add the Graph application permission and grant admin consent (Terraform
does not do this; run the manual step below or extend the Terraform):

```bash
# AuditLog.Read.All (application / Role) on Microsoft Graph
az ad app permission add \
  --id <azure_client_id> \
  --api 00000003-0000-0000-c000-000000000000 \
  --api-permissions df021288-bdef-4463-88db-98f22de89214=Role

az ad app permission admin-consent --id <azure_client_id>
```

### Manual Setup

1. Create the app registration.

   ```bash
   az ad app create --display-name "dfe-fetcher"
   ```

2. Create the service principal.

   ```bash
   az ad sp create --id <app_id>
   ```

3. Add a client secret. Save the `password` from the output - it is the
   client secret.

   ```bash
   az ad app credential reset --id <app_id> --years 1
   ```

4. Assign the `Reader` role on the subscription (covers `activity_log`,
   `defender`, `sentinel`).

   ```bash
   az role assignment create \
     --assignee <service_principal_object_id> \
     --role Reader \
     --scope /subscriptions/<subscription_id>
   ```

5. Add the Graph application permission for the Entra audit feeds, then
   grant admin consent. `admin-consent` is required for application (Role)
   permissions; ignore any CLI hint suggesting `az ad app permission grant`.

   ```bash
   az ad app permission add \
     --id <app_id> \
     --api 00000003-0000-0000-c000-000000000000 \
     --api-permissions df021288-bdef-4463-88db-98f22de89214=Role

   az ad app permission admin-consent --id <app_id>
   ```

6. (Optional) For `log_analytics`, assign `Log Analytics Reader` on the
   workspace.

   ```bash
   az role assignment create \
     --assignee <service_principal_object_id> \
     --role "Log Analytics Reader" \
     --scope <workspace_resource_id>
   ```

7. Configure dfe-fetcher (see below).

## dfe-fetcher Configuration

The Azure source config fields are: `enabled`, `tenant_id`, `client_id`,
`client_secret`, `credential_secret`, `subscription_id`, `interval_secs`,
`services`, `topic`, `management_url_override`, `graph_url_override`,
`token_url_override`, `filter`. Each service entry is `{ name, config }`.

The Entra feeds are split into three distinct services, each with its own
source tag and cursor. There is intentionally no combined `entra_id`
service - configure each feed you want.

### Config File

```yaml
sources:
  azure:
    enabled: true
    tenant_id: "your-tenant-uuid"
    client_id: "your-client-uuid"
    client_secret: "your-client-secret"
    subscription_id: "your-subscription-uuid"
    # interval_secs: 300            # override scheduler default
    services:
      - name: activity_log
      - name: defender
      - name: sentinel
        config:
          resource_group: "my-rg"           # required for real data
          workspace_name: "my-sentinel-ws"  # required for real data
      - name: entra_signins             # graph v1.0 auditLogs/signIns
      - name: entra_directory_audits    # graph v1.0 auditLogs/directoryAudits
      - name: entra_provisioning        # graph v1.0 auditLogs/provisioning
      - name: log_analytics
        config:
          workspace_id: "00000000-0000-0000-0000-000000000000"  # GUID, not name
          kql: |
            SecurityEvent
            | where EventID == 4625
            | project TimeGenerated, Account, IpAddress, FailureReason
    topic: "azure"
    # filter: 'level == "Critical" || level == "Error"'   # CEL, hot-reloaded
```

Notes: `sentinel` falls back to `default` for `resource_group` /
`workspace_name` if unset, which will not return data - set both. For
`log_analytics`, `workspace_id` is the workspace GUID (not the display
name), and the fetcher injects the time window as the server-side
`timespan`, so the KQL should NOT include a `where TimeGenerated ...`
clause.

### Environment Variables

Pattern: `DFE_FETCHER_SOURCES__AZURE__<FIELD>` (double underscores).

```bash
DFE_FETCHER_SOURCES__AZURE__ENABLED="true"
DFE_FETCHER_SOURCES__AZURE__TENANT_ID="your-tenant-uuid"
DFE_FETCHER_SOURCES__AZURE__CLIENT_ID="your-client-uuid"
DFE_FETCHER_SOURCES__AZURE__CLIENT_SECRET="your-client-secret"
DFE_FETCHER_SOURCES__AZURE__SUBSCRIPTION_ID="your-subscription-uuid"
```

### Secrets Manager

`credential_secret` resolves from OpenBao/Vault using the `provider:path:key`
format. When set, it supplies both the client ID and client secret
(the resolved value is used for each), so omit inline `client_id` /
`client_secret`.

```yaml
sources:
  azure:
    enabled: true
    tenant_id: "your-tenant-uuid"
    subscription_id: "your-subscription-uuid"
    credential_secret: "vault:secret/dfe/azure:credentials"
    services:
      - name: activity_log
      - name: entra_signins
    topic: "azure"
```

## Verification

1. Credential check. `AzureSource::health_check()` returns `true` only when
   the Management API token is obtained. Exercise it via the env-gated
   smoke test:

   ```bash
   cargo test --test e2e azure_health_check -- --ignored
   ```

2. Live fetch tests. The env-gated tests in
   [`tests/e2e/smoke_remote.rs`](../../tests/e2e/smoke_remote.rs) hit real
   Azure APIs. They are `#[ignore]` by default and read credentials from
   `.env-cloud` (preferred) or `.env`:
   `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`,
   `AZURE_SUBSCRIPTION_ID`, plus `AZURE_SENTINEL_WORKSPACE_NAME` /
   `AZURE_SENTINEL_RESOURCE_GROUP` for Sentinel and
   `AZURE_LOG_ANALYTICS_WORKSPACE_ID` / `AZURE_LOG_ANALYTICS_KQL` for Log
   Analytics.

   ```bash
   cargo test --test e2e -- --ignored        # all live tests
   cargo test --test e2e azure_fetch_entra_signins -- --ignored
   ```

   Available Azure tests: `azure_health_check`, `azure_fetch_activity_log`,
   `azure_fetch_entra_signins`, `azure_fetch_entra_directory_audits`,
   `azure_fetch_entra_provisioning` (needs provisioning scope),
   `azure_fetch_log_analytics`, `azure_fetch_sentinel`.

3. Common failures.
   - `401 / AADSTS700016` or invalid client: wrong `tenant_id` /
     `client_id` / `client_secret`, or the secret expired.
   - `403` on an Entra feed: admin consent was not granted, or the SP lacks
     the provisioning scope (`entra_provisioning` is the usual culprit).
   - Empty Activity Log / Defender / Sentinel: `Reader` role not yet
     propagated, or no events in the window.
   - `azure.log_analytics requires service config workspace_id (GUID)` /
     `requires service config kql`: the per-service `config` block is
     missing a required key, or `workspace_id` is a name instead of a GUID.

## Cost

dfe-fetcher only reads; it never enables services. The read paths generally
carry no additional charge, but some capabilities here may have a cost
depending on your Azure plan and what is enabled - for example Microsoft
Sentinel, Microsoft Defender for Cloud, and Log Analytics may carry their own
cost when used. Confirm any cost implications against your own Azure
agreement.

## References

- Microsoft Graph permissions reference (AuditLog.Read.All): https://learn.microsoft.com/en-us/graph/permissions-reference
- Microsoft Entra audit logs API overview: https://learn.microsoft.com/en-us/graph/api/resources/azure-ad-auditlog-overview?view=graph-rest-1.0
- List provisioningObjectSummary (v1.0): https://learn.microsoft.com/en-us/graph/api/provisioningobjectsummary-list?view=graph-rest-1.0
- Defender for Cloud Alerts - List (api-version 2022-01-01): https://learn.microsoft.com/en-us/rest/api/defenderforcloud/alerts/list
- Azure built-in roles for Security (Reader / Security Reader): https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/security
- Azure built-in roles for Monitor (Log Analytics Reader): https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/monitor
- az ad app permission CLI reference: https://learn.microsoft.com/en-us/cli/azure/ad/app/permission?view=azure-cli-latest
