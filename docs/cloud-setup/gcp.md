<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/gcp.md              -->
<!-- Purpose:   GCP cloud admin setup guide           -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   FSL-1.1-ALv2                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# GCP Setup for dfe-fetcher

What a cloud administrator needs to configure so dfe-fetcher can read
security and audit data from Google Cloud Platform.

## What dfe-fetcher Needs

A **GCP service account** with read-only roles. dfe-fetcher authenticates
using a service account JSON key file, generating short-lived OAuth2
tokens via JWT signing.

dfe-fetcher never writes, modifies, or deletes resources.

## Required Permissions

Enable only the services you configure. Not all are needed.

| Service | IAM Role | Scope | Notes |
|---------|---------|-------|-------|
| **Cloud Audit Logs** | `roles/logging.viewer` | Project | Admin Activity and Data Access logs |
| **Cloud Logging** | `roles/logging.viewer` | Project | All log entries in the project |
| **Security Command Center** | `roles/securitycenter.findingsViewer` | Organisation | Requires SCC enabled; scoped to org |

**Notes:**

- `roles/logging.viewer` covers both Cloud Audit Logs and general Cloud
  Logging. A single role grant is sufficient for both services.
- SCC findings require **organisation-level** access and an
  `organization_id` in the service config. SCC must be activated on the
  organisation before use.

### Least-Privilege Custom Role (Alternative)

If `roles/logging.viewer` is too broad, create a custom role with:

```
logging.logEntries.list
logging.logs.list
logging.logServiceIndexes.list
logging.logServices.list
```

## Terraform (Automated)

The test infrastructure is defined in
[`infra/test/main.tf`](../../infra/test/main.tf) (GCP section). It
creates a service account, assigns `roles/logging.viewer` on the
project, and generates a JSON key.

```bash
cd infra/test
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set gcp_project_id
# Authenticate: gcloud auth application-default login
terraform init
terraform apply
terraform output -json | python3 gen-env.py > ../../.env
```

The Terraform provisions `roles/logging.viewer` only. For SCC access,
add an additional IAM binding at the organisation level:

```hcl
resource "google_organization_iam_member" "scc_viewer" {
  org_id = "123456789"
  role   = "roles/securitycenter.findingsViewer"
  member = "serviceAccount:${google_service_account.fetcher_test.email}"
}
```

## Manual Setup

1. **Create service account**

   ```bash
   gcloud iam service-accounts create dfe-fetcher \
     --display-name="dfe-fetcher" \
     --project=your-project-id
   ```

2. **Assign Logs Viewer role**

   ```bash
   gcloud projects add-iam-policy-binding your-project-id \
     --member="serviceAccount:dfe-fetcher@your-project-id.iam.gserviceaccount.com" \
     --role="roles/logging.viewer"
   ```

3. **Assign SCC role** (optional, organisation level)

   ```bash
   gcloud organizations add-iam-policy-binding 123456789 \
     --member="serviceAccount:dfe-fetcher@your-project-id.iam.gserviceaccount.com" \
     --role="roles/securitycenter.findingsViewer"
   ```

4. **Generate JSON key**

   ```bash
   gcloud iam service-accounts keys create sa-key.json \
     --iam-account=dfe-fetcher@your-project-id.iam.gserviceaccount.com
   ```

   Store this file securely. It contains the private key used for
   authentication.

5. **Configure dfe-fetcher**

### Config File

```yaml
sources:
  gcp:
    enabled: true
    project_id: "your-project-id"
    service_account_key: "/etc/gcp/sa-key.json"
    services:
      - name: audit_logs
      - name: cloud_logging
      - name: scc
        config:
          organization_id: "123456789"
    topic: "gcp"
```

### Environment Variables

```bash
DFE_FETCHER_SOURCES__GCP__ENABLED="true"
DFE_FETCHER_SOURCES__GCP__PROJECT_ID="your-project-id"
DFE_FETCHER_SOURCES__GCP__SERVICE_ACCOUNT_KEY="/etc/gcp/sa-key.json"
```

### Secrets Manager (Production)

```yaml
sources:
  gcp:
    enabled: true
    project_id: "your-project-id"
    credential_secret: "vault:secret/dfe/gcp:credentials"
    services:
      - name: audit_logs
      - name: cloud_logging
    topic: "gcp"
```

The vault secret should contain the full service account JSON key as a
string.

### Kubernetes Deployment

Mount the service account key as a Kubernetes secret:

```bash
kubectl create secret generic dfe-fetcher-gcp \
  --from-file=sa-key.json=./sa-key.json
```

Reference in the pod spec:

```yaml
volumes:
  - name: gcp-key
    secret:
      secretName: dfe-fetcher-gcp
containers:
  - name: dfe-fetcher
    volumeMounts:
      - name: gcp-key
        mountPath: /etc/gcp
        readOnly: true
```

## Multi-Project Access

To fetch from multiple GCP projects, run a separate dfe-fetcher instance
per project. Each needs:

- A service account in (or with access to) the target project
- `roles/logging.viewer` on each target project
- A distinct `instance_id` in the fetcher config

Alternatively, a single service account can be granted
`roles/logging.viewer` on multiple projects — configure one dfe-fetcher
instance per project with the same key file but different `project_id`.

## SCC (Security Command Center) Setup

SCC is an organisation-level service. Additional requirements:

1. SCC must be activated on the GCP organisation (Standard or Premium tier)
2. The service account needs `roles/securitycenter.findingsViewer` at
   the **organisation** level
3. The `organization_id` must be provided in the service config (see
   config example above)

Without an `organization_id`, the SCC service logs a warning and returns
no data.

## Cost

**Free.** GCP service accounts and Cloud Logging read API calls are
included. Admin Activity audit logs are always-on and free. Data Access
audit logs may incur Cloud Logging ingestion charges if enabled, but
dfe-fetcher only reads — it does not enable logging.

SCC findings API reads are included with the SCC tier (Standard or
Premium). No additional per-read charges.
