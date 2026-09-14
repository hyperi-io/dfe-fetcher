<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/aws.md              -->
<!-- Purpose:   AWS cloud admin setup guide           -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   BUSL-1.1                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# AWS Setup for dfe-fetcher

What a cloud administrator configures so dfe-fetcher can pull security and
operational data from AWS.

## Overview

dfe-fetcher polls AWS service APIs on an interval and ships each record to
Kafka. It supports these AWS services: CloudTrail (management-event audit
trail), GuardDuty (threat-detection findings), SecurityHub (aggregated
security findings), Config (resource configuration through
`SelectResourceConfig`), CloudWatch Logs (log events), CloudWatch Metrics
(monitoring data, emitted as JSON or OTLP), Inspector (Inspector v2
vulnerability findings), and Health (AWS Health events). All access is
read-only - dfe-fetcher never writes, modifies, or deletes resources.
Requests are direct AWS REST/JSON API calls signed with SigV4, not the AWS
SDK. Authentication is a static access key pair, inline or from a secrets
manager.

The source is the shipped `aws` profile (`crates/fetcher/profiles/aws.yaml`);
the `sources.aws` block below maps onto an instance of it at load. Every unit
is its own AWS service, signed for that service in the configured region
(Health is region-locked to `us-east-1`), and every list pages to the end on
the token the API documents. A 429 or 5xx is retried with backoff (honouring
`Retry-After`), a 401 or 403 ends the tick, and a tick that fails does not
advance the fetch window. The health check is STS `GetCallerIdentity`, the one
call every key may make.

## Prerequisites

- An AWS account with the services you intend to fetch already enabled
  (GuardDuty, SecurityHub, Config recording, and Inspector v2 must be turned
  on tenant-side or their APIs return empty or refuse).
- An IAM user (or role) for dfe-fetcher, plus permission to create it.
- AWS CLI v2 authenticated (SSO or a profile) if using the manual or OpenTofu
  paths.
- Cost flags: dfe-fetcher only reads, but some capabilities here may carry a
  cost depending on your AWS plan. The Health API requires a paid support
  plan (lesser tiers answer `SubscriptionRequiredException`), and services
  such as Inspector v2 may carry their own cost when enabled. See the Cost
  section.

## Required Permissions

Grant only the actions for the services you configure. None support
resource-level ARN scoping for these read operations, so use
`"Resource": "*"`. The health check's `sts:GetCallerIdentity` needs no
permission.

| Service | IAM Actions | Notes |
|---------|-------------|-------|
| CloudTrail | `cloudtrail:LookupEvents` (or managed policy `AWSCloudTrail_ReadOnlyAccess`) | `LookupEvents` covers the last 90 days of management events |
| GuardDuty | `guardduty:ListDetectors`, `guardduty:ListFindings`, `guardduty:GetFindings` | One detector per region; fetcher walks all detectors and looks findings up under the detector that listed them |
| SecurityHub | `securityhub:GetFindings` | Requires SecurityHub enabled in the account; only `NEW` workflow-status findings are fetched |
| Config | `config:SelectResourceConfig` | Requires AWS Config recording in the account; the `expression` knob replaces the default query |
| CloudWatch Logs | `logs:FilterLogEvents`, `logs:DescribeLogGroups`, `logs:DescribeLogStreams`, `logs:GetLogEvents` | Per-service config must set `log_group_name` |
| CloudWatch Metrics | `cloudwatch:ListMetrics`, `cloudwatch:GetMetricData`, `cloudwatch:GetMetricStatistics`, `cloudwatch:DescribeAlarms` | Per-service config must set `namespaces` |
| Inspector | `inspector2:ListFindings` (or managed policy `AmazonInspector2ReadOnlyAccess`) | Inspector v2; returns empty unless enabled tenant-side |
| Health | `health:DescribeEvents` | Requires Business / Enterprise On-Ramp / Enterprise Support tier |

### Least-Privilege Policy (All Services)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudTrailRead",
      "Effect": "Allow",
      "Action": [
        "cloudtrail:LookupEvents",
        "cloudtrail:GetTrailStatus",
        "cloudtrail:DescribeTrails"
      ],
      "Resource": "*"
    },
    {
      "Sid": "GuardDutyRead",
      "Effect": "Allow",
      "Action": [
        "guardduty:ListDetectors",
        "guardduty:ListFindings",
        "guardduty:GetFindings"
      ],
      "Resource": "*"
    },
    {
      "Sid": "SecurityHubRead",
      "Effect": "Allow",
      "Action": [
        "securityhub:GetFindings"
      ],
      "Resource": "*"
    },
    {
      "Sid": "ConfigRead",
      "Effect": "Allow",
      "Action": [
        "config:SelectResourceConfig"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchLogsRead",
      "Effect": "Allow",
      "Action": [
        "logs:FilterLogEvents",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams",
        "logs:GetLogEvents"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchMetricsRead",
      "Effect": "Allow",
      "Action": [
        "cloudwatch:ListMetrics",
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:DescribeAlarms"
      ],
      "Resource": "*"
    },
    {
      "Sid": "InspectorRead",
      "Effect": "Allow",
      "Action": [
        "inspector2:ListFindings"
      ],
      "Resource": "*"
    },
    {
      "Sid": "HealthRead",
      "Effect": "Allow",
      "Action": [
        "health:DescribeEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

## Source-Side Setup

### OpenTofu (Automated)

The test infrastructure is defined in
[`infra/test/main.tf`](../../infra/test/main.tf) (AWS section). It
provisions an IAM user `dfe-fetcher-test`, attaches the
`AWSCloudTrail_ReadOnlyAccess` managed policy, adds an inline policy with
CloudWatch Logs and Metrics read actions, and creates an access key.

```bash
cd infra/test
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars - set aws_profile and aws_region
tofu init
tofu apply
tofu output -json | python3 gen-env.py > ../../.env
```

The module provisions CloudTrail and CloudWatch permissions only. For
GuardDuty, SecurityHub, Config, Inspector, or Health, attach additional
statements to the inline policy (see the Required Permissions table) or
extend the module.

### Manual Setup

If you prefer not to use OpenTofu:

1. Create the IAM user.

   ```bash
   aws iam create-user --user-name dfe-fetcher
   ```

2. Attach the managed policy for CloudTrail.

   ```bash
   aws iam attach-user-policy \
     --user-name dfe-fetcher \
     --policy-arn arn:aws:iam::aws:policy/AWSCloudTrail_ReadOnlyAccess
   ```

3. Create an inline policy for the other services. Save the JSON above (or
   a subset) to `policy.json`, removing statements for services you do not
   need.

   ```bash
   aws iam put-user-policy \
     --user-name dfe-fetcher \
     --policy-name dfe-fetcher-readonly \
     --policy-document file://policy.json
   ```

4. Generate an access key.

   ```bash
   aws iam create-access-key --user-name dfe-fetcher
   ```

5. Configure dfe-fetcher (see below).

### Cross-Account Access

Set `assume_role_arn` to an IAM role ARN and the fetcher signs with that role
instead of the key it is given: it calls STS `AssumeRole` once per connection
(on the regional STS endpoint of the connection's region, session name
`dfe-fetcher`), keeps the session credentials until shortly before they
expire, then assumes the role again, and every signed request carries the
session token in `x-amz-security-token`. The key's own principal needs
`sts:AssumeRole` on the role, and the role's trust policy must name that
principal; the role then needs the read permissions listed above, not the
key. A role in another partition (GovCloud, China) is checked against the
connection's region when it is first assumed.

Without `assume_role_arn` the fetcher signs with the static key pair. A
second account can be read either way: a role in that account assumed by
this key, or a read-only user and key in that account added as a second
entry under `connections` (below) or as a second `sources.rest` instance of
the `aws` profile.

## dfe-fetcher Configuration

The AWS source config fields are: `enabled`, `region`, `access_key_id`,
`secret_access_key`, `assume_role_arn`, `credential_secret`,
`interval_secs`, `services`, `connections`, `topic`, `endpoint_override`,
`filter`. Each service entry is `{ name, config }`.
Valid `name` values are `cloudtrail`, `guardduty`, `securityhub`, `config`,
`cloudwatch_logs`, `cloudwatch_metrics`, `inspector`, `health`. A missing
key, an unknown service, `cloudwatch_logs` without `log_group_name` or
`cloudwatch_metrics` without `namespaces` is refused at load, naming
`sources.aws`.

### Config File

```yaml
sources:
  aws:
    enabled: true
    region: "ap-southeast-2"
    access_key_id: "AKIA..."
    secret_access_key: "wJalrXUtnFEMI..."
    # interval_secs: 300            # override scheduler default
    services:
      - name: cloudtrail
      - name: guardduty
      - name: securityhub
      - name: config
        # config:
        #   expression: "SELECT resourceId, resourceType, configuration"   # replaces the default SELECT
      - name: cloudwatch_logs
        config:
          log_group_name: "/aws/vpc/flowlogs"   # required
          # filter_pattern: ""                   # optional
      - name: cloudwatch_metrics
        config:
          namespaces: ["AWS/EC2"]                # required
          # metric_names: ["CPUUtilization"]     # optional; narrows ListMetrics on the API
          # period_secs: 300
          # stat: "Average"
          # output_format: "json"                # "json" (default) or "otlp"
      - name: inspector
        config:
          max_results: 100                       # per page, max 100
      - name: health
        config:
          max_results: 100                       # per page, max 100
    topic: "aws"
    # filter: 'eventName != "ConsoleLogin"'      # CEL, hot-reloaded
    # connections:                               # several accounts from one block
    #   - id: aws-acct-123
    #     region: us-east-1
    #     credential_secret: "vault:kv/data/aws-acct-123:credentials"
```

Notes: `inspector` calls the Inspector v2 REST API (`inspector2`), and
`health` is region-locked to `us-east-1` regardless of the configured
`region`. `config` runs `SelectResourceConfig` with a default expression that
names the configuration item's top-level properties (`SELECT *` would not
return `configuration`); the `expression` knob replaces it. Each `Results`
string lands as the document it holds.

### Environment Variables

Pattern: `DFE_FETCHER_SOURCES__AWS__<FIELD>` (double underscores).

```bash
DFE_FETCHER_SOURCES__AWS__ENABLED="true"
DFE_FETCHER_SOURCES__AWS__REGION="ap-southeast-2"
DFE_FETCHER_SOURCES__AWS__ACCESS_KEY_ID="AKIA..."
DFE_FETCHER_SOURCES__AWS__SECRET_ACCESS_KEY="wJalrXUtnFEMI..."
```

### Secrets Manager

`credential_secret` resolves from the secrets manager. Format is
`vault:<mount>/data/<path>:<key>`, e.g. `vault:kv/data/aws:credentials` -- the
KV v2 mount followed by a literal `data` segment, without which the whole path
is read under the default `secret` mount. Individual `access_key_id` /
`secret_access_key` values may also be `env:` or `vault:` prefixed. Credentials
are resolved once per instance, not per API call.

```yaml
sources:
  aws:
    enabled: true
    region: "ap-southeast-2"
    credential_secret: "vault:kv/data/dfe/aws:credentials"
    services:
      - name: cloudtrail
      - name: guardduty
    topic: "aws"
```

The vault secret must be JSON with these keys (PascalCase `AccessKeyId` /
`SecretAccessKey` are also accepted):

```json
{
  "access_key_id": "AKIA...",
  "secret_access_key": "wJalrXUtnFEMI..."
}
```

## Verification

1. Credential check. The profile's probe calls STS `GetCallerIdentity` with
   the signed key, so a healthy result proves the key pair is valid and the
   signature is accepted, not that any service permission is granted.
   Exercise it via the env-gated smoke test:

   ```bash
   cargo test -p dfe-fetcher --test e2e aws_health_check -- --ignored
   ```

2. Live fetch tests. The env-gated tests in
   [`crates/fetcher/tests/e2e/smoke_remote.rs`](../../crates/fetcher/tests/e2e/smoke_remote.rs)
   hit real AWS APIs. They are `#[ignore]` by default and read credentials
   from `.env-cloud` (preferred) or `.env`: `AWS_ACCESS_KEY_ID`,
   `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_CLOUDWATCH_LOG_GROUP`
   (optional). A refusal fails its test rather than passing with zero records.

   ```bash
   cargo test -p dfe-fetcher --test e2e aws_ -- --ignored
   ```

3. Common failures.
   - `403 / SignatureDoesNotMatch`: clock skew or wrong secret key.
   - Empty results from GuardDuty / SecurityHub / Inspector: the service is
     not enabled, or there is no detector / scan target. This is normal on a
     fresh account.
   - `AccessDeniedException` on Config: `config:SelectResourceConfig` is not
     granted, or AWS Config is not recording in the account.
   - `SubscriptionRequiredException` on Health: account is below Business
     support tier.
   - `cloudwatch_logs requires log_group_name` / `cloudwatch_metrics
     requires namespaces`: the per-service `config` block is missing a
     required key; the config is refused at load.

## Cost

dfe-fetcher only reads; it never enables services or creates trails, log
groups, detectors, or recorders. The core read paths generally carry no
additional charge, but some capabilities here may have a cost depending on
your AWS plan and what is enabled - for example the Health programmatic API
requires a paid support plan, and services such as Inspector or Config are
billed when enabled. Confirm any cost implications against your own AWS
agreement.

## References

- AWS managed policy AWSCloudTrail_ReadOnlyAccess: https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AWSCloudTrail_ReadOnlyAccess.html
- CloudTrail LookupEvents (90-day window): https://docs.aws.amazon.com/awscloudtrail/latest/APIReference/API_LookupEvents.html
- AWS Config SelectResourceConfig: https://docs.aws.amazon.com/config/latest/APIReference/API_SelectResourceConfig.html
- AWS Config query limitations (`SELECT *` returns scalar properties only): https://docs.aws.amazon.com/config/latest/developerguide/querying-AWS-resources.html
- GuardDuty ListFindings / GetFindings: https://docs.aws.amazon.com/guardduty/latest/APIReference/API_ListFindings.html
- Amazon Inspector2 IAM actions: https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazoninspector2.html
- Amazon Inspector managed policies: https://docs.aws.amazon.com/inspector/latest/user/security-iam-awsmanpol.html
- AWS Health IAM policy examples: https://docs.aws.amazon.com/health/latest/ug/security_iam_id-based-policy-examples.html
- AWS Health DescribeEvents API: https://docs.aws.amazon.com/health/latest/APIReference/API_DescribeEvents.html
