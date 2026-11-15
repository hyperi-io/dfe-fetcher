<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/object-store.md     -->
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

The source is a family of backends selected per entry by a `provider`
discriminator:

- `s3` - SigV4-signed ListObjectsV2 + GetObject, tested live. Also works
  against S3-compatible stores (MinIO, Cloudflare R2, Backblaze B2) via
  `endpoint_override`.
- `gcs` and `azure_blob` - NOT IMPLEMENTED. The config parses, but each is
  skipped at load with a warning naming it, and the block's S3 backend still
  runs. They read nothing, so leave them out of your config.

The S3 backend runs on the shipped `object_store` profile (`crates/fetcher/profiles/object_store.yaml`), and the `sources.object_store` block below maps onto one instance of it at load, one unit per prefix. The highest `last_modified` (and key) a tick delivered is the prefix's checkpoint, committed once the prefix's whole tick is delivered, and the next listing starts there. So an object is read once while ticks succeed. A tick that fails partway, or a crash before the commit, commits nothing, and the next tick reads again the objects it had already delivered: delivery is at least once, so a consumer that counts records dedupes on `_dfe_fetcher_object.key`. A first tick, with no checkpoint, starts at the start of the scheduler's window: `cursor.default_window_hours` back (one hour by default, or one fetch interval if that is longer), so objects already older than that are never read. A refused listing (401 or 403) fails that prefix's tick and the other prefixes still run. An object that keeps failing is retried per the profile's policy and then fails the prefix's tick, so the checkpoint never advances past it.

Supported object formats: `json_gz`, `jsonl`, `json`, `text_gz`, `text`. Each tick walks the prefix's whole listing (1000 keys a page, at most 1000 pages, past which the unit fails and wants a narrower prefix) and reads at most 1000 objects, oldest first. The remainder waits for the next tick. One S3 backend per block: a second bucket account runs as a `sources.rest` instance of the `object_store` profile with its own key pair.

dfe-fetcher only reads. It never writes, modifies, deletes, or lifecycles
objects, buckets, or containers.

## Prerequisites

For the live S3 backend:

- An S3 bucket (or S3-compatible store) that already receives log objects
  from the producing service. dfe-fetcher does not provision delivery -
  see Source-Side Setup for links on configuring each producer.
- An IAM user with read-only access to the bucket and prefixes you intend
  to poll.
- Static AWS access keys for that user (or a vault secret holding them).
  The S3 backend authenticates with SigV4 static credentials, resolved once
  per instance.
- The bucket region (used for SigV4 signing and endpoint construction).

## Required Permissions

### S3

Minimal read-only access. `s3:ListBucket` is a bucket-level action and
targets the bare bucket ARN, and `s3:GetObject` is an object-level action and
targets the `/*` object ARN. Pointing both at `/*` is the most common
mistake - `ListBucket` then silently fails.

| Action | Resource ARN | Why |
|--------|--------------|-----|
| `s3:ListBucket` | `arn:aws:s3:::my-log-bucket` | List object keys + last-modified under each prefix |
| `s3:ListBucketMultipartUploads` | `arn:aws:s3:::my-log-bucket` | Hold the listing behind uploads still in progress (see Multipart Uploads) |
| `s3:GetObject` | `arn:aws:s3:::my-log-bucket/*` | Download each object body |

All actions are read-only. No write, delete, or bucket-config actions are
needed. For cross-account delivery buckets, the bucket policy on the
target bucket must also allow the fetcher principal.

### Multipart Uploads

S3 lists an object written by multipart upload under the time the upload BEGAN, not when it completed. A large object can land behind newer ones the fetcher has already read, and a checkpoint alone would skip it. So before each listing the fetcher calls `ListMultipartUploads` on the prefix and reads nothing modified at or after the start of the oldest upload still in progress.

Without `s3:ListBucketMultipartUploads` (a 403), or on a store that does not support the call, the fetcher logs one warning per prefix naming the permission and falls back to `settle_secs`: only objects older than that many seconds are read. The default is 300, one default fetch interval. An upload that takes longer than `settle_secs` can still be skipped on that path, so grant the permission or raise `settle_secs` past your longest upload.

An upload that is never completed or aborted holds its prefix until it goes, and the fetcher warns once it has held for a day. Add an `AbortIncompleteMultipartUpload` lifecycle rule to the bucket so abandoned uploads are removed.

MinIO lists a completed multipart object under its completion time, so this case does not arise there, and the hold only delays objects written while an upload runs.

## Source-Side Setup

dfe-fetcher reads whatever a producing service has already delivered. You
must configure the producer to deliver into the bucket, then point a
prefix at it.

1. Confirm (or enable) log delivery to the bucket. Common producers:

   - CloudTrail to S3: create a trail with an S3 destination. See
     <https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-create-a-trail-using-the-console-first-time.html>
     Objects land under
     `AWSLogs/<account-id>/CloudTrail/<region>/...` as gzipped JSON.
   - VPC Flow Logs to S3: publish flow logs to an S3 bucket. See
     <https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-s3.html>
   - AWS WAF logs to S3: enable logging on the web ACL with an S3
     destination. See
     <https://docs.aws.amazon.com/waf/latest/developerguide/logging-s3.html>

2. Note the exact key prefix each producer writes under. The prefix is
   what dfe-fetcher polls, and getting it wrong yields zero records.

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
         "Action": ["s3:ListBucket", "s3:ListBucketMultipartUploads"],
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

S3 backend. Each backend lists buckets, and each bucket lists prefixes
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

| Field | Meaning |
|-------|---------|
| `provider` | `s3`. `gcs` and `azure_blob` parse but are skipped (Overview) |
| `region` | AWS region for SigV4 signing and endpoint construction |
| `access_key_id`, `secret_access_key` | Static credentials, each an inline value, an `env:VAR` spec, or a `vault:` spec |
| `endpoint_override` | Optional, for MinIO, R2, B2, or VPC endpoints |
| `credential_secret` | Optional vault spec holding both keys as JSON (see Secrets Manager below). Wins over the inline key fields |
| `settle_secs` | Optional, default 300. How old an object must be before it is read, used only where `ListMultipartUploads` is refused (see Multipart Uploads) |
| `buckets[].bucket` | Bucket name |
| `buckets[].prefixes[].prefix` | Key prefix to poll (may be empty to scan the whole bucket) |
| `buckets[].prefixes[].format` | One of `json_gz`, `jsonl`, `json`, `text_gz`, `text` |
| `buckets[].prefixes[].source_tag` | Record source tag (`object_store.<source_tag>`). A tag that names a format or repeats is refused at load |
| `buckets[].prefixes[].topic` | Optional per-prefix topic override |

Prefixes run in tag order, not config order.

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
        credential_secret: "vault:kv/data/dfe/aws-s3-reader:credentials"
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
   logs. A working prefix logs `unit tick complete` with its `rows`,
   `filtered`, `oversize` and `flushes` counts on every tick. A refused
   listing logs `unit tick failed` with the API's text.

3. The health check is the key pair resolving, with no probe request.

4. A `gcs` or `azure_blob` backend in the config logs a warning at load that
   the backend is not implemented and is skipped. Remove it.

5. Emitted records carry a `_dfe_fetcher_object` envelope with
   `provider`, `bucket`, `key`, `last_modified`, and `size`, so you can
   filter and route downstream.

## Cost

dfe-fetcher only reads objects. It never creates trails, flow logs, sinks,
or diagnostic settings. The object LIST/GET calls it makes may generate
usage charges (API requests and any cross-region or egress data transfer),
so there could be a cost depending on bucket traffic and where the fetcher
runs - polling is bounded per tick, and tuning the fetch interval limits
request volume. The producing service's own logging/storage is billed
separately by the cloud provider. Confirm any cost implications against
your own cloud agreement.

## References

- S3 object metadata, multipart `Last-Modified` is the initiation time:
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html>
- S3 ListMultipartUploads:
  <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html>
- S3 lifecycle rule to abort incomplete multipart uploads:
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html>
- S3 read-only IAM policy (two-ARN pattern):
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/example-bucket-policies.html>
- AWS IAM example - read access to an S3 bucket:
  <https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html>
- CloudTrail trail to S3:
  <https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-create-a-trail-using-the-console-first-time.html>
- VPC Flow Logs to S3:
  <https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-s3.html>
- AWS WAF logging to S3:
  <https://docs.aws.amazon.com/waf/latest/developerguide/logging-s3.html>
