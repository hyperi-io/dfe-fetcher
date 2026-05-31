<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/object_store.md     -->
<!-- Purpose:   Object-store source family setup guide -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# Object Store Setup for dfe-fetcher

What a cloud administrator needs to configure so dfe-fetcher can read log
objects (gzipped JSON-lines and similar) that cloud services drop into a
storage bucket.

## Overview

Many cloud services do not expose an API for their logs - they only
deliver them as objects into a bucket. The `object_store` source family
polls bucket prefixes for new objects, decodes them (gzip aware), and
emits one record per line. Typical feeds:

- AWS CloudTrail -> S3
- AWS VPC Flow Logs -> S3
- AWS WAF logs -> S3
- GCP Cloud Logging -> GCS storage sink (Phase 2)
- Azure diagnostic settings -> Blob container (Phase 2)

The source is a family of backends selected per entry by a `provider`
discriminator:

- `s3` - FULLY IMPLEMENTED and tested live. SigV4-signed ListObjectsV2 +
  GetObject. Also works against S3-compatible stores (MinIO, Cloudflare
  R2, Backblaze B2) via `endpoint_override`.
- `gcs` - PHASE 2 STUB. The config parses and the enum variant exists,
  but listing and fetching return a "Phase 2" error and the driver
  logs-and-skips the backend. NOT USABLE YET.
- `azure_blob` - PHASE 2 STUB. Same status as `gcs`. NOT USABLE YET.

Supported object formats: `json_gz`, `jsonl`, `json`, `text_gz`, `text`.
Per tick, each (bucket, prefix) is capped at 1000 objects and 10 list
pages; the remainder rolls into the next tick.

dfe-fetcher only reads. It never writes, modifies, deletes, or lifecycles
objects, buckets, or containers.

## Prerequisites

For the live S3 backend:

- An S3 bucket (or S3-compatible store) that already receives log objects
  from the producing service. dfe-fetcher does not provision delivery -
  see Source-Side Setup for links on configuring each producer.
- An IAM identity (user or assumable role) with read-only access to the
  bucket and prefixes you intend to poll.
- Static AWS access keys for that identity (or a vault secret holding
  them). The S3 backend authenticates with SigV4 static credentials.
- The bucket region (used for SigV4 signing and endpoint construction).

For GCS / Azure Blob: nothing to do yet. Those backends are Phase 2 stubs
and cannot fetch data. The permission notes below are documented now so
the eventual rollout is unblocked, but leave those backends out of your
config today.

## Required Permissions

### S3 (live)

Minimal read-only access. `s3:ListBucket` is a bucket-level action and
targets the bare bucket ARN; `s3:GetObject` is an object-level action and
targets the `/*` object ARN. Pointing both at `/*` is the most common
mistake - `ListBucket` then silently fails.

| Action | Resource ARN | Why |
|--------|--------------|-----|
| `s3:ListBucket` | `arn:aws:s3:::my-log-bucket` | List object keys + last-modified under each prefix |
| `s3:GetObject` | `arn:aws:s3:::my-log-bucket/*` | Download each object body |

All actions are read-only. No write, delete, or bucket-config actions are
needed. For cross-account delivery buckets, the bucket policy on the
target bucket must also allow the fetcher principal.

### GCS (Phase 2 - not yet implemented)

When the GCS backend ships, the service account needs
`roles/storage.objectViewer` (read plus list) on the bucket or project.
`roles/storage.legacyObjectReader` is the read-without-list variant if
listing is not wanted. Do not configure a `gcs` backend today.

### Azure Blob (Phase 2 - not yet implemented)

When the Azure Blob backend ships, the principal needs the data-plane
role `Storage Blob Data Reader` on the storage account or container.
Note that Azure control-plane roles (Owner, Contributor, Reader) do NOT
grant blob data access on their own - a data-plane role is required, and
reading via Entra credentials without it returns
`403 AuthorizationPermissionMismatch`. Role assignments can take up to 10
minutes to propagate. Do not configure an `azure_blob` backend today.

## Source-Side Setup

dfe-fetcher reads whatever a producing service has already delivered. You
must configure the producer to deliver into the bucket, then point a
prefix at it.

1. Confirm (or enable) log delivery to the bucket. Common producers:

   - CloudTrail to S3: create a trail with an S3 destination. See
     https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-create-a-trail-using-the-console-first-time.html
     Objects land under
     `AWSLogs/<account-id>/CloudTrail/<region>/...` as gzipped JSON.
   - VPC Flow Logs to S3: publish flow logs to an S3 bucket. See
     https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-s3.html
   - AWS WAF logs to S3: enable logging on the web ACL with an S3
     destination. See
     https://docs.aws.amazon.com/waf/latest/developerguide/logging-s3.html
   - (Phase 2) GCP Cloud Logging to GCS: create a log sink with a Cloud
     Storage bucket destination. See
     https://cloud.google.com/logging/docs/export/configure_export_v2
   - (Phase 2) Azure diagnostic settings to Blob: route resource logs to
     a storage account. See
     https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/diagnostic-settings

2. Note the exact key prefix each producer writes under. The prefix is
   what dfe-fetcher polls; getting it wrong yields zero records.

3. Note the object format. CloudTrail, VPC Flow Logs (parquet excluded),
   and most native AWS feeds are gzipped JSON-lines -> use `json_gz`.

4. Create the read-only IAM identity (see S3 manual setup below) and
   confirm it can list and read the prefix.

### S3 manual IAM setup

1. Create the IAM user:

   ```bash
   aws iam create-user --user-name dfe-fetcher-s3-reader
   ```

2. Attach the least-privilege read-only policy (save as `policy.json`):

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "ListLogBucket",
         "Effect": "Allow",
         "Action": ["s3:ListBucket"],
         "Resource": ["arn:aws:s3:::my-log-bucket"]
       },
       {
         "Sid": "GetLogObjects",
         "Effect": "Allow",
         "Action": ["s3:GetObject"],
         "Resource": ["arn:aws:s3:::my-log-bucket/*"]
       }
     ]
   }
   ```

   ```bash
   aws iam put-user-policy \
     --user-name dfe-fetcher-s3-reader \
     --policy-name dfe-fetcher-s3-readonly \
     --policy-document file://policy.json
   ```

   To restrict to a single prefix, narrow the `GetObject` resource to
   `arn:aws:s3:::my-log-bucket/AWSLogs/123456789012/CloudTrail/*` and add
   an `s3:prefix` condition to the `ListBucket` statement.

3. Generate an access key:

   ```bash
   aws iam create-access-key --user-name dfe-fetcher-s3-reader
   ```

## dfe-fetcher Configuration

### Config File

S3 backend (live). Each backend lists buckets; each bucket lists prefixes
with a `format` and a `source_tag`. Records are tagged
`object_store.<source_tag>`.

```yaml
sources:
  object_store:
    enabled: true
    topic: "object_store"
    # filter: '_dfe_fetcher_object.bucket == "my-log-bucket"'  # hot-reloaded
    backends:
      - provider: s3
        region: "ap-southeast-2"
        access_key_id: "AKIA..."
        secret_access_key: "wJalrXUtnFEMI..."
        # endpoint_override: "http://localhost:9000"  # MinIO / R2 / B2
        buckets:
          - bucket: "my-log-bucket"
            prefixes:
              - prefix: "AWSLogs/123456789012/CloudTrail/ap-southeast-2/"
                format: json_gz
                source_tag: "aws_cloudtrail"
                # topic: "cloudtrail"   # optional per-prefix topic override
              - prefix: "AWSLogs/123456789012/vpcflowlogs/"
                format: json_gz
                source_tag: "aws_vpc_flow"
```

Field reference (S3 backend):

- `provider` - `s3` (live), `gcs` / `azure_blob` (Phase 2 stubs only).
- `region` - AWS region for SigV4 signing and endpoint construction.
- `access_key_id`, `secret_access_key` - static credentials. Each may be
  an inline value, an `env:VAR` spec, or a `vault:` spec.
- `endpoint_override` - optional; for MinIO, R2, B2, or VPC endpoints.
- `credential_secret` - optional; vault spec holding both keys as JSON
  (see Secrets Manager below). Wins over the inline key fields.
- `buckets[].bucket` - bucket name (container name for Azure Blob).
- `buckets[].prefixes[].prefix` - key prefix to poll (may be empty to
  scan the whole bucket).
- `buckets[].prefixes[].format` - one of `json_gz`, `jsonl`, `json`,
  `text_gz`, `text`.
- `buckets[].prefixes[].source_tag` - record source tag
  (`object_store.<source_tag>`).
- `buckets[].prefixes[].topic` - optional per-prefix topic override.

The example config also documents `gcs` and `azure_blob` backend shapes,
but they are PHASE 2 STUBS - if configured, the driver logs a Phase 2
warning and skips them. Do not enable them today.

### Environment Variables

Top-level source fields follow the
`DFE_FETCHER_SOURCES__OBJECT_STORE__<FIELD>` pattern (double underscores):

```bash
DFE_FETCHER_SOURCES__OBJECT_STORE__ENABLED="true"
DFE_FETCHER_SOURCES__OBJECT_STORE__TOPIC="object_store"
```

The `backends` list (with its nested buckets and prefixes) is awkward to
express purely as environment variables. Define backends in the config
file and inject only the credentials via env. The credential fields
accept an `env:` indirection so the secret value itself stays in the
process environment, not in the YAML:

```yaml
backends:
  - provider: s3
    region: "ap-southeast-2"
    access_key_id: "env:AWS_ACCESS_KEY_ID"
    secret_access_key: "env:AWS_SECRET_ACCESS_KEY"
```

```bash
AWS_ACCESS_KEY_ID="AKIA..."
AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI..."
```

### Secrets Manager

Keep static keys out of the config file with `credential_secret`. It
resolves to a JSON object containing both keys and takes precedence over
the inline `access_key_id` / `secret_access_key` fields:

```yaml
sources:
  object_store:
    enabled: true
    topic: "object_store"
    backends:
      - provider: s3
        region: "ap-southeast-2"
        credential_secret: "vault:secret/dfe/aws-s3-reader:credentials"
        buckets:
          - bucket: "my-log-bucket"
            prefixes:
              - prefix: "AWSLogs/123456789012/CloudTrail/ap-southeast-2/"
                format: json_gz
                source_tag: "aws_cloudtrail"
```

The vault secret should contain JSON:

```json
{
  "access_key_id": "AKIA...",
  "secret_access_key": "wJalrXUtnFEMI..."
}
```

The keys `AccessKeyId` / `SecretAccessKey` (AWS casing) are also accepted.

## Verification

1. Confirm the IAM identity can list and read the prefix directly:

   ```bash
   aws s3 ls s3://my-log-bucket/AWSLogs/123456789012/CloudTrail/ \
     --profile dfe-fetcher-s3-reader
   ```

   A non-empty listing confirms `s3:ListBucket`. Then download one object
   to confirm `s3:GetObject`:

   ```bash
   aws s3 cp s3://my-log-bucket/<one-key-from-above> /tmp/probe.gz \
     --profile dfe-fetcher-s3-reader
   ```

2. Start dfe-fetcher with the object_store source enabled and watch the
   logs. A working backend logs `Polling object-store backends` then, per
   drained prefix, `object_store: prefix drained` with object and record
   counts.

3. The source `health_check` reports healthy when at least one S3
   backend's credentials resolve. GCS / Azure stub backends never
   participate in the health check.

4. If you (incorrectly) configure a `gcs` or `azure_blob` backend, you
   will see a warning like
   `object_store: GCS backend is Phase 2 stub, skipping` - remove that
   backend.

5. Emitted records carry a `_dfe_fetcher_object` envelope with
   `provider`, `bucket`, `key`, `last_modified`, and `size`, so you can
   filter and route downstream.

## Cost

dfe-fetcher only reads objects; it never creates trails, flow logs, sinks,
or diagnostic settings. The object LIST/GET calls it makes may generate
usage charges (API requests and any cross-region or egress data transfer),
so there could be a cost depending on bucket traffic and where the fetcher
runs - polling is bounded per tick, and tuning the fetch interval limits
request volume. The producing service's own logging/storage is billed
separately by the cloud provider. Confirm any cost implications against
your own cloud agreement.

## References

- S3 read-only IAM policy (two-ARN pattern):
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/example-bucket-policies.html
- AWS IAM example - read access to an S3 bucket:
  https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html
- CloudTrail trail to S3:
  https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-create-a-trail-using-the-console-first-time.html
- VPC Flow Logs to S3:
  https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-s3.html
- AWS WAF logging to S3:
  https://docs.aws.amazon.com/waf/latest/developerguide/logging-s3.html
- GCP Cloud Storage IAM roles (Phase 2):
  https://cloud.google.com/storage/docs/access-control/iam-roles
- GCP log sink to Cloud Storage (Phase 2):
  https://cloud.google.com/logging/docs/export/configure_export_v2
- Azure built-in Storage roles, Storage Blob Data Reader (Phase 2):
  https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/storage
- Azure assign a role for blob data access (Phase 2):
  https://learn.microsoft.com/en-us/azure/storage/blobs/assign-azure-role-data-access
- Azure diagnostic settings (Phase 2):
  https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/diagnostic-settings
