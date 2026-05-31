<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/gcp.md              -->
<!-- Purpose:   GCP cloud admin setup guide           -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# GCP Setup for dfe-fetcher

What a cloud administrator configures so dfe-fetcher can read security and
audit data from Google Cloud Platform (read-only, pull-mode).

## Overview

dfe-fetcher polls Google Cloud REST APIs on a schedule and ships records to
Kafka. It authenticates with a service account: it signs an RS256 JWT with the
service account's private key and exchanges it for a short-lived OAuth2 access
token (scope `https://www.googleapis.com/auth/cloud-platform`), or - when
running on GCE/GKE - it reads a token from the metadata server (workload
identity). dfe-fetcher never writes, modifies, or deletes resources.

The GCP source exposes these services (each emits its own source tag
`gcp.<service>` and tracks its own cursor):

- Cloud Audit Logs, split per subtype: `admin_activity`, `data_access`,
  `system_event`, `policy_denied`. There is no combined `audit_logs` service -
  configure each subtype you want.
- Additional Cloud Logging streams: `vpc_flow_logs`, `dns_queries`,
  `storage_access` (a `gcs_bucket`-scoped subset of `data_access`).
- `cloud_logging` - an arbitrary Cloud Logging filter (default
  `severity >= WARNING`, override with service config `filter`).
- `scc` - Security Command Center findings (organisation-scoped).

All audit/logging services read via `POST logging.googleapis.com/v2/entries:list`
with a Logging Query Language filter; `scc` reads
`securitycenter.googleapis.com/v1/organizations/<org>/sources/-/findings`.

## Prerequisites

- A GCP project (`project_id`) whose logs you want to read.
- `gcloud` CLI authenticated as a project owner/editor, or Terraform with
  application-default credentials (`gcloud auth application-default login`).
- For `scc`: Security Command Center activated on the organisation (Standard or
  Premium/Enterprise tier) and the numeric `organization_id`.
- For `data_access` / `storage_access`: Data Access audit logs explicitly
  enabled (see Source-Side Setup) - they are OFF by default.
- For `vpc_flow_logs`: VPC Flow Logs enabled per subnet. For `dns_queries`: a
  Cloud DNS server policy with logging enabled.

## Required Permissions

Grant only what you need. All roles are read-only.

| Service(s) | IAM Role | Scope | Notes |
|------------|----------|-------|-------|
| `admin_activity`, `system_event`, `policy_denied`, `vpc_flow_logs`, `dns_queries`, `cloud_logging` | `roles/logging.viewer` | Project | Reads the `_Required`/`_Default` buckets, excluding Data Access logs |
| `data_access`, `storage_access` | `roles/logging.privateLogViewer` | Project | REQUIRED for Data Access audit logs - `roles/logging.viewer` alone CANNOT read them |
| `scc` | `roles/securitycenter.findingsViewer` | Organisation | Requires SCC activated; needs `organization_id` in service config |

**Notes:**

- `roles/logging.privateLogViewer` is a superset of `roles/logging.viewer`, so
  granting it alone covers every logging-backed service. If you do not fetch
  `data_access` or `storage_access`, `roles/logging.viewer` is sufficient.
- SCC findings require **organisation-level** access. Without an
  `organization_id` in the `scc` service config, the source logs a warning and
  returns no data.

### Least-Privilege Custom Role (Alternative)

If the predefined logging roles are too broad, a custom role needs at least:

```
logging.logEntries.list
logging.logs.list
logging.views.access      # required to read Data Access (private) logs
```

## Source-Side Setup

### Terraform (Automated)

The test infrastructure lives in
[`infra/test/main.tf`](../../infra/test/main.tf) (GCP section). It creates a
service account, binds `roles/logging.viewer` on the project, and emits a JSON
key.

```bash
cd infra/test
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars - set gcp_project_id
gcloud auth application-default login
terraform init
terraform apply
terraform output -json | python3 gen-env.py > ../../.env
```

The provided Terraform grants `roles/logging.viewer` only. To read Data Access
logs, raise the binding to `roles/logging.privateLogViewer`:

```hcl
resource "google_project_iam_member" "private_logs_viewer" {
  project = var.gcp_project_id
  role    = "roles/logging.privateLogViewer"
  member  = "serviceAccount:${google_service_account.fetcher_test.email}"
}
```

For SCC, add an organisation-level binding:

```hcl
resource "google_organization_iam_member" "scc_viewer" {
  org_id = "123456789"
  role   = "roles/securitycenter.findingsViewer"
  member = "serviceAccount:${google_service_account.fetcher_test.email}"
}
```

### Manual Setup (gcloud)

1. **Create the service account**

   ```bash
   gcloud iam service-accounts create dfe-fetcher \
     --display-name="dfe-fetcher" \
     --project=your-project-id
   ```

2. **Grant a logging role**

   ```bash
   # Covers admin_activity / system_event / policy_denied / vpc_flow_logs /
   # dns_queries / cloud_logging:
   gcloud projects add-iam-policy-binding your-project-id \
     --member="serviceAccount:dfe-fetcher@your-project-id.iam.gserviceaccount.com" \
     --role="roles/logging.viewer"

   # Also needed for data_access / storage_access:
   gcloud projects add-iam-policy-binding your-project-id \
     --member="serviceAccount:dfe-fetcher@your-project-id.iam.gserviceaccount.com" \
     --role="roles/logging.privateLogViewer"
   ```

3. **Enable Data Access audit logs** (only if you fetch `data_access` /
   `storage_access`; they are OFF by default)

   In the console: IAM and Admin -> Audit Logs -> select the service(s) ->
   enable `DATA_READ` / `DATA_WRITE`. Or apply an IAM policy with an
   `auditConfigs` block, for example for all services:

   ```yaml
   auditConfigs:
   - service: allServices
     auditLogConfigs:
     - logType: DATA_READ
     - logType: DATA_WRITE
   ```

   ```bash
   gcloud projects set-iam-policy your-project-id policy.yaml
   ```

   dfe-fetcher only reads logs; it never enables logging.

4. **Grant the SCC role** (optional, organisation level)

   ```bash
   gcloud organizations add-iam-policy-binding 123456789 \
     --member="serviceAccount:dfe-fetcher@your-project-id.iam.gserviceaccount.com" \
     --role="roles/securitycenter.findingsViewer"
   ```

5. **Generate a JSON key**

   ```bash
   gcloud iam service-accounts keys create sa-key.json \
     --iam-account=dfe-fetcher@your-project-id.iam.gserviceaccount.com
   ```

   Store it securely - it holds the private key dfe-fetcher signs with. On
   GKE/GCE you can skip the key and use workload identity (dfe-fetcher falls
   back to the metadata server when no key or secret is configured).

## dfe-fetcher Configuration

### Config File

```yaml
sources:
  gcp:
    enabled: true
    project_id: "your-project-id"
    service_account_key: "/etc/gcp/sa-key.json"
    services:
      - name: admin_activity      # log_id("cloudaudit.googleapis.com/activity")
      - name: data_access         # log_id("cloudaudit.googleapis.com/data_access") - needs enablement
      - name: system_event        # log_id("cloudaudit.googleapis.com/system_event")
      - name: policy_denied       # log_id("cloudaudit.googleapis.com/policy")
      - name: vpc_flow_logs       # log_id("compute.googleapis.com/vpc_flows")
      - name: dns_queries         # log_id("dns.googleapis.com/dns_queries")
      - name: storage_access      # data_access narrowed to resource.type="gcs_bucket"
      - name: cloud_logging
        config:
          filter: "severity >= WARNING"   # any Logging Query Language clause
      - name: scc
        config:
          organization_id: "123456789"
    topic: "gcp"
    # filter: 'severity != "INFO"'   # CEL, hot-reloaded
```

Recognised top-level fields: `enabled`, `project_id`, `service_account_key`,
`credential_secret`, `interval_secs`, `services`, `topic`, `filter`,
`api_url_override`, `token_url_override`. No others exist.

### Environment Variables

Pattern: `DFE_FETCHER_SOURCES__GCP__<FIELD>` (double underscores).

```bash
DFE_FETCHER_SOURCES__GCP__ENABLED="true"
DFE_FETCHER_SOURCES__GCP__PROJECT_ID="your-project-id"
DFE_FETCHER_SOURCES__GCP__SERVICE_ACCOUNT_KEY="/etc/gcp/sa-key.json"
```

### Secrets Manager

Resolve the whole service account JSON key from a secret store via
`credential_secret`. The resolved value is the full JSON key as a string.

```yaml
sources:
  gcp:
    enabled: true
    project_id: "your-project-id"
    credential_secret: "vault:secret/dfe/gcp:credentials"
    services:
      - name: admin_activity
      - name: cloud_logging
    topic: "gcp"
```

When mounting the key as a Kubernetes secret, point `service_account_key` at
the mounted path:

```bash
kubectl create secret generic dfe-fetcher-gcp --from-file=sa-key.json=./sa-key.json
```

```yaml
volumes:
  - name: gcp-key
    secret: { secretName: dfe-fetcher-gcp }
containers:
  - name: dfe-fetcher
    volumeMounts:
      - { name: gcp-key, mountPath: /etc/gcp, readOnly: true }
```

For multiple projects, run one dfe-fetcher instance per project (each with a
distinct `instance_id`); a single service account can hold the logging role on
several projects.

## Verification

1. **Health check.** `GcpSource::health_check` simply acquires an access token,
   so a healthy result confirms the credential and token exchange only - not
   per-service IAM grants.

2. **Env-gated smoke tests.** Live tests live in
   [`tests/e2e/smoke_remote.rs`](../../tests/e2e/smoke_remote.rs), all
   `#[ignore]`'d. They read `GCP_PROJECT_ID` and `GCP_SERVICE_ACCOUNT_KEY` (a
   file path) from `.env-cloud`.

   ```bash
   cargo test --test e2e gcp_ -- --ignored --nocapture
   ```

3. **Common failures.**
   - `403`/empty on `data_access` or `storage_access`: missing
     `roles/logging.privateLogViewer`, or Data Access audit logs not enabled.
   - SCC warning "requires organization_id": add `organization_id` to the `scc`
     service config.
   - Token exchange error: bad key file path, malformed JSON key, or clock skew
     (the JWT `exp` is `now + 3600s`).
   - `metadata server unavailable`: no key/secret configured and not running on
     GCE/GKE.

## Cost

dfe-fetcher only reads; it never enables logging or services. The read calls
and the always-on audit logs (admin_activity, system_event, policy_denied)
are not expected to carry an additional charge. Some capabilities here may
have a cost depending on what you enable - for example turning on Data Access
audit logs (to populate `data_access`/`storage_access`) can generate logging
charges, and SCC findings depend on your activated SCC tier. Confirm any cost
implications against your own GCP agreement.

## References

- Cloud Logging IAM roles and permissions: <https://docs.cloud.google.com/iam/docs/roles-permissions/logging>
- Cloud Logging access control (Logs Viewer vs Private Logs Viewer): <https://docs.cloud.google.com/logging/docs/access-control>
- Cloud Audit Logs overview: <https://docs.cloud.google.com/logging/docs/audit>
- Enable Data Access audit logs: <https://docs.cloud.google.com/logging/docs/audit/configure-data-access>
- Logging query language (`log_id`, filters): <https://docs.cloud.google.com/logging/docs/view/logging-query-language>
- `entries.list` API method: <https://docs.cloud.google.com/logging/docs/reference/v2/rest/v2/entries/list>
- Security Command Center findings API: <https://cloud.google.com/security-command-center/docs/reference/rest/v1/organizations.sources.findings>
