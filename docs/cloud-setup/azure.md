<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/azure.md            -->
<!-- Purpose:   Azure cloud admin setup guide         -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   FSL-1.1-ALv2                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Azure Setup for dfe-fetcher

What a cloud administrator needs to configure so dfe-fetcher can read
security and operational data from Azure.

## What dfe-fetcher Needs

An **Entra ID (Azure AD) app registration** with a client secret.
The service principal is assigned **Reader** role on the subscription
for Azure Management API access, and Graph API permissions for Entra ID
audit/sign-in logs.

dfe-fetcher authenticates via OAuth2 client_credentials flow using two
scopes:

- `https://management.azure.com/.default` (Activity Log, Defender, Sentinel)
- `https://graph.microsoft.com/.default` (Entra ID audit and sign-in logs)

## Required Permissions

| Service | API / Scope | Role / Permission | Type |
|---------|------------|-------------------|------|
| **Activity Log** | Azure Management | `Reader` role on subscription | RBAC |
| **Defender for Cloud** | Azure Management | `Reader` role on subscription | RBAC |
| **Sentinel** | Azure Management | `Reader` role on subscription | RBAC |
| **Entra ID (sign-ins)** | Microsoft Graph | `AuditLog.Read.All` | Application |
| **Entra ID (directory audits)** | Microsoft Graph | `AuditLog.Read.All` | Application |

**Notes:**

- The `Reader` RBAC role on the subscription covers Activity Log,
  Defender alerts, and Sentinel incidents via the Azure Management API.
- Graph `AuditLog.Read.All` covers both `auditLogs/signIns` and
  `auditLogs/directoryAudits` endpoints.
- All permissions are **read-only**. dfe-fetcher never modifies resources.

## Terraform (Automated)

The test infrastructure is defined in
[`infra/test/main.tf`](../../infra/test/main.tf) (Azure section). It
creates an app registration, service principal, client secret, and
assigns `Reader` role on the subscription.

```bash
cd infra/test
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set azure_subscription_id
# Authenticate: az login
terraform init
terraform apply
terraform output -json | python3 gen-env.py > ../../.env
```

**After apply**, grant admin consent for Graph API permissions if you
also need Entra ID logs:

```bash
az ad app permission add \
  --id <azure_client_id> \
  --api 00000003-0000-0000-c000-000000000000 \
  --api-permissions df021288-bdef-4463-88db-98f22de89214=Role

az ad app permission admin-consent --id <azure_client_id>
```

The Terraform provisions the app registration with subscription Reader
role only. Graph API permissions for Entra ID audit logs require the
manual step above (or extend the Terraform).

## Manual Setup

1. **Create app registration**

   ```bash
   az ad app create --display-name "dfe-fetcher"
   ```

2. **Create service principal**

   ```bash
   az ad sp create --id <app_id>
   ```

3. **Add client secret**

   ```bash
   az ad app credential reset --id <app_id> --years 1
   ```

   Save the `password` from the output — it is the client secret.

4. **Assign Reader role on subscription**

   ```bash
   az role assignment create \
     --assignee <service_principal_object_id> \
     --role Reader \
     --scope /subscriptions/<subscription_id>
   ```

5. **Add Graph permissions** (for Entra ID logs)

   ```bash
   # AuditLog.Read.All (application)
   az ad app permission add \
     --id <app_id> \
     --api 00000003-0000-0000-c000-000000000000 \
     --api-permissions df021288-bdef-4463-88db-98f22de89214=Role

   az ad app permission admin-consent --id <app_id>
   ```

6. **Configure dfe-fetcher**

### Config File

```yaml
sources:
  azure:
    enabled: true
    tenant_id: "your-tenant-uuid"
    client_id: "your-client-uuid"
    client_secret: "your-client-secret"
    subscription_id: "your-subscription-uuid"
    services:
      - name: activity_log
      - name: defender
      - name: sentinel
      - name: entra_id
    topic: "azure"
```

### Environment Variables

```bash
DFE_FETCHER_SOURCES__AZURE__ENABLED="true"
DFE_FETCHER_SOURCES__AZURE__TENANT_ID="your-tenant-uuid"
DFE_FETCHER_SOURCES__AZURE__CLIENT_ID="your-client-uuid"
DFE_FETCHER_SOURCES__AZURE__CLIENT_SECRET="your-client-secret"
DFE_FETCHER_SOURCES__AZURE__SUBSCRIPTION_ID="your-subscription-uuid"
```

### Secrets Manager (Production)

```yaml
sources:
  azure:
    enabled: true
    tenant_id: "your-tenant-uuid"
    subscription_id: "your-subscription-uuid"
    credential_secret: "vault:secret/dfe/azure:credentials"
    services:
      - name: activity_log
      - name: entra_id
    topic: "azure"
```

The vault secret should contain JSON:

```json
{
  "client_id": "your-client-uuid",
  "client_secret": "your-client-secret"
}
```

## Multi-Tenant Access

To fetch from multiple Azure tenants, define separate source
configurations with distinct `tenant_id` values. Each tenant needs its
own app registration.

```yaml
sources:
  azure:
    enabled: true
    tenant_id: "tenant-alpha-uuid"
    client_id: "client-alpha-uuid"
    client_secret: "secret-alpha"
    subscription_id: "sub-alpha-uuid"
    services:
      - name: activity_log
    topic: "azure_alpha"
```

Run a separate dfe-fetcher instance per tenant, each with its own config
file and `instance_id`.

## Cost

**Free.** Entra ID app registrations, Activity Log read API calls, and
Defender/Sentinel read operations have no additional charges. Graph API
reads for audit logs are included in the Entra ID licence.
