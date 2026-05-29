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
Kafka. It supports seven AWS services: CloudTrail (management-event audit
trail), GuardDuty (threat-detection findings), SecurityHub (aggregated
security findings), Config (resource-configuration snapshots via an
aggregator), CloudWatch Logs (log events), CloudWatch Metrics (monitoring
data, emitted as JSON or OTLP), Inspector (Inspector v2 vulnerability
findings), and Health (AWS Health events). All access is read-only -
dfe-fetcher never writes, modifies, or deletes resources. Requests are
direct AWS REST/JSON API calls signed with SigV4 (via the `reqsign` crate),
not the AWS SDK. Authentication is static access keys, cross-account
`AssumeRole`, or a secrets manager reference.

## Prerequisites

- An AWS account with the services you intend to fetch already enabled
  (GuardDuty, SecurityHub, Config aggregator, and Inspector v2 must be
  turned on tenant-side or their APIs return empty).
- An IAM user (or role) for dfe-fetcher, plus permission to create it.
- AWS CLI v2 authenticated (SSO or a profile) if using the manual or
  Terraform paths.
- Cost flags: dfe-fetcher only reads, but some capabilities here may carry a
  cost depending on your AWS plan. The Health API requires a paid support
  plan (lesser tiers return `AccessDeniedException`), and services such as
  Inspector v2 may carry their own cost when enabled. See the Cost section.

## Required Permissions

Grant only the actions for the services you configure. None support
resource-level ARN scoping for these read operations, so use
`"Resource": "*"`.

| Service | IAM Actions | Notes |
|---------|-------------|-------|
| CloudTrail | `cloudtrail:LookupEvents` (or managed policy `AWSCloudTrail_ReadOnlyAccess`) | `LookupEvents` covers the last 90 days of management events |
| GuardDuty | `guardduty:ListDetectors`, `guardduty:ListFindings`, `guardduty:GetFindings` | One detector per region; fetcher walks all detectors |
| SecurityHub | `securityhub:GetFindings` | Requires SecurityHub enabled in the account |
| Config | `config:SelectAggregateResourceConfig` | Requires a configuration aggregator |
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
        "config:SelectAggregateResourceConfig"
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

### Terraform (Automated)

The test infrastructure is defined in
[`infra/test/main.tf`](../../infra/test/main.tf) (AWS section). It
provisions an IAM user `dfe-fetcher-test`, attaches the
`AWSCloudTrail_ReadOnlyAccess` managed policy, adds an inline policy with
CloudWatch Logs and Metrics read actions, and creates an access key.

```bash
cd infra/test
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars - set aws_profile and aws_region
terraform init
terraform apply
terraform output -json | python3 gen-env.py > ../../.env
```

The Terraform provisions CloudTrail and CloudWatch permissions only. For
GuardDuty, SecurityHub, Config, Inspector, or Health, attach additional
statements to the inline policy (see the Required Permissions table) or
extend the Terraform.

### Manual Setup

If you prefer not to use Terraform:

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

To fetch from a different AWS account without sharing long-lived
credentials, use `assume_role_arn`. The fetcher uses the temporary
credentials from `sts:AssumeRole`.

1. In the target account, create a role with the read-only policy above and
   a trust policy allowing the fetcher's account to assume it.

2. Grant the source IAM user `sts:AssumeRole` on that target role ARN.

3. Set `assume_role_arn` in the AWS source config (see below).

## dfe-fetcher Configuration

The AWS source config fields are: `enabled`, `region`, `access_key_id`,
`secret_access_key`, `assume_role_arn`, `credential_secret`,
`interval_secs`, `services`, `topic`, `endpoint_override`, `filter`. Each
service entry is `{ name, config }`. Valid `name` values are `cloudtrail`,
`guardduty`, `securityhub`, `config`, `cloudwatch_logs`,
`cloudwatch_metrics`, `inspector`, `health`.

### Config File

```yaml
sources:
  aws:
    enabled: true
    region: "ap-southeast-2"
    access_key_id: "AKIA..."
    secret_access_key: "wJalrXUtnFEMI..."
    # assume_role_arn: "arn:aws:iam::123456789012:role/dfe-fetcher-readonly"
    # interval_secs: 300            # override scheduler default
    services:
      - name: cloudtrail
      - name: guardduty
      - name: securityhub
      - name: config
      - name: cloudwatch_logs
        config:
          log_group_name: "/aws/vpc/flowlogs"   # required
          # filter_pattern: ""                   # optional
      - name: cloudwatch_metrics
        config:
          namespaces: ["AWS/EC2"]                # required
          # metric_names: ["CPUUtilization"]     # optional whitelist
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
```

Note: `inspector` calls the Inspector v2 REST API (`inspector2`), and
`health` is region-locked to `us-east-1` regardless of the configured
`region`. Both return empty if not enabled / not entitled.

### Environment Variables

Pattern: `DFE_FETCHER_SOURCES__AWS__<FIELD>` (double underscores).

```bash
DFE_FETCHER_SOURCES__AWS__ENABLED="true"
DFE_FETCHER_SOURCES__AWS__REGION="ap-southeast-2"
DFE_FETCHER_SOURCES__AWS__ACCESS_KEY_ID="AKIA..."
DFE_FETCHER_SOURCES__AWS__SECRET_ACCESS_KEY="wJalrXUtnFEMI..."
# DFE_FETCHER_SOURCES__AWS__ASSUME_ROLE_ARN="arn:aws:iam::123456789012:role/..."
```

### Secrets Manager

`credential_secret` resolves from OpenBao/Vault. Format is
`provider:path:key`, e.g. `vault:secret/aws:credentials`. Individual
`access_key_id` / `secret_access_key` values may also be `env:` or `vault:`
prefixed and are resolved at runtime.

```yaml
sources:
  aws:
    enabled: true
    region: "ap-southeast-2"
    credential_secret: "vault:secret/dfe/aws:credentials"
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

1. Credential check. `AwsSource::health_check()` returns `true` only when
   credentials resolve. Exercise it via the env-gated smoke test:

   ```bash
   cargo test --test e2e aws_health_check -- --ignored
   ```

2. Live fetch tests. The env-gated tests in
   [`tests/e2e/smoke_remote.rs`](../../tests/e2e/smoke_remote.rs) hit real
   AWS APIs. They are `#[ignore]` by default and read credentials from
   `.env-cloud` (preferred) or `.env`:
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
   `AWS_CLOUDWATCH_LOG_GROUP` (optional).

   ```bash
   cargo test --test e2e -- --ignored        # all live tests
   cargo test --test e2e aws_fetch_cloudtrail -- --ignored
   ```

   Available AWS tests: `aws_health_check`, `aws_fetch_cloudtrail`,
   `aws_fetch_cloudwatch_logs`, `aws_fetch_cloudwatch_metrics`,
   `aws_fetch_inspector` (needs Inspector v2 enabled), `aws_fetch_health`
   (needs Business+ support tier).

3. Common failures.
   - `403 / SignatureDoesNotMatch`: clock skew or wrong secret key.
   - Empty results from GuardDuty / SecurityHub / Config / Inspector: the
     service is not enabled, or there is no aggregator / detector / scan
     target. This is normal on a fresh account.
   - `AccessDeniedException` on Health: account is below Business support
     tier.
   - `cloudwatch_logs requires log_group_name` / `cloudwatch_metrics
     requires namespaces`: the per-service `config` block is missing a
     required key.

## Cost

dfe-fetcher only reads; it never enables services or creates trails, log
groups, detectors, or aggregators. The core read paths generally carry no
additional charge, but some capabilities here may have a cost depending on
your AWS plan and what is enabled - for example the Health programmatic API
requires a paid support plan, and services such as Inspector or Config are
billed when enabled. Confirm any cost implications against your own AWS
agreement.

## References

- AWS managed policy AWSCloudTrail_ReadOnlyAccess: https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AWSCloudTrail_ReadOnlyAccess.html
- CloudTrail LookupEvents (90-day window): https://docs.aws.amazon.com/awscloudtrail/latest/APIReference/API_LookupEvents.html
- Amazon Inspector2 IAM actions: https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazoninspector2.html
- Amazon Inspector managed policies: https://docs.aws.amazon.com/inspector/latest/user/security-iam-awsmanpol.html
- AWS Health IAM policy examples: https://docs.aws.amazon.com/health/latest/ug/security_iam_id-based-policy-examples.html
- AWS Health DescribeEvents API: https://docs.aws.amazon.com/health/latest/APIReference/API_DescribeEvents.html
