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
service account's private key and exchanges it once per tick for a short-lived
OAuth2 access token (scope `https://www.googleapis.com/auth/cloud-platform`),
or - when running on GCE/GKE with no key configured - it reads a token from the
metadata server (workload identity), or it sends a pre-minted access token
supplied as `credential_secret`. dfe-fetcher never writes, modifies, or
deletes resources.

The source is the shipped `gcp` profile (`crates/fetcher/profiles/gcp.yaml`);
the `sources.gcp` block below maps onto an instance of it at load. Each
Cloud Logging unit sends its documented filter clause and pages on
`nextPageToken`; a 429 or 5xx is retried with backoff (honouring
`Retry-After`), a 401 or 403 ends the tick with the API's `error.message`,
and a tick that fails does not advance the fetch window.

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
- `gcloud` CLI authenticated as a project owner/editor, or OpenTofu with
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
- SCC findings require **organisation-level** access. An `scc` service
  without `organization_id` in its config is refused at load, naming
  `sources.gcp`.

### Least-Privilege Custom Role (Alternative)

If the predefined logging roles are too broad, a custom role needs at least:

```text
logging.logEntries.list
logging.logs.list
logging.views.access      # required to read Data Access (private) logs
```

## Source-Side Setup

### OpenTofu (Automated)

The test infrastructure lives in
[`infra/test/main.tf`](../../infra/test/main.tf) (GCP section). It creates a
service account, binds `roles/logging.viewer` on the project, and emits a JSON
key.

```bash
cd infra/test
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars - set gcp_project_id
gcloud auth application-default login
tofu init
tofu apply
tofu output -json | python3 gen-env.py > ../../.env
```

The provided module grants `roles/logging.viewer` only. To read Data Access
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
`credential_secret`, `interval_secs`, `services`, `connections`, `topic`,
`filter`, `api_url_override`, `token_url_override`. No others exist. A Cloud
Logging unit without `project_id`, `scc` without `organization_id`, or an
unknown service name is refused at load. `api_url_override` points both the
Logging and the Security Command Center hosts at one URL.

### Environment Variables

Pattern: `DFE_FETCHER_SOURCES__GCP__<FIELD>` (double underscores).

```bash
DFE_FETCHER_SOURCES__GCP__ENABLED="true"
DFE_FETCHER_SOURCES__GCP__PROJECT_ID="your-project-id"
DFE_FETCHER_SOURCES__GCP__SERVICE_ACCOUNT_KEY="/etc/gcp/sa-key.json"
```

`service_account_key` takes the key JSON itself as well as the path of its file: a value that opens with `{` is the key. So a Kubernetes Secret entry holding the contents of `sa-key.json`, set as `DFE_FETCHER_SOURCES__GCP__SERVICE_ACCOUNT_KEY` through the chart's `extraEnv`, works without a mounted file. [README.md](README.md#credentials-in-kubernetes) has the chart side.

### Secrets Manager

On this block `credential_secret` resolves to an OAuth2 ACCESS TOKEN that is
sent as the bearer on every call -- not the service-account key. Use it when
something outside the fetcher mints and rotates the token; a static value
expires within the hour.

```yaml
sources:
  gcp:
    enabled: true
    project_id: "your-project-id"
    credential_secret: "vault:kv/data/dfe/gcp:access_token"
    services:
      - name: admin_activity
      - name: cloud_logging
    topic: "gcp"
```

To keep the service-account key itself in the secrets manager, run the `gcp`
profile as a `sources.rest` instance with `auth.mode: jwt_bearer` and
`auth.service_account_key: "vault:..."` (the JSON as a spec), which is the
same shape the block maps onto. When mounting the key as a Kubernetes secret,
point `service_account_key` at the mounted path:

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

1. **Health check.** The health check is the token exchange (or, with
   `credential_secret`, the resolution of the token), so a healthy result
   confirms the credential only - not per-service IAM grants.

2. **Env-gated smoke tests.** Live tests live in
   [`crates/fetcher/tests/e2e/smoke_remote.rs`](../../crates/fetcher/tests/e2e/smoke_remote.rs),
   all `#[ignore]`'d. They read `GCP_PROJECT_ID` and `GCP_SERVICE_ACCOUNT_KEY`
   (a file path) from `.env-cloud`, and `GCP_ORGANIZATION_ID` for the SCC
   test. A refusal fails its test rather than passing with zero records.

   ```bash
   cargo test -p dfe-fetcher --test e2e gcp_ -- --ignored
   ```

3. **Common failures.**
   - `403`/empty on `data_access` or `storage_access`: missing
     `roles/logging.privateLogViewer`, or Data Access audit logs not enabled.
   - The config is refused naming `scc` and `organization_id`: add
     `organization_id` to the `scc` service config.
   - Token exchange error: bad key file path, malformed JSON key, or clock skew
     (the JWT `exp` is `now + 3600s`).
   - `metadata server unavailable`: no key/secret configured and not running on
     GCE/GKE.
   - SCC answers `400` naming the legacy tier: Security Command Center is not
     activated on a Standard or Premium tier for the organisation.

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
