<!-- Project:   dfe-fetcher                         -->
<!-- File:      docs/cloud-setup/aws.md              -->
<!-- Purpose:   AWS cloud admin setup guide           -->
<!-- Language:  Markdown                              -->
<!--                                                  -->
<!-- License:   FSL-1.1-ALv2                          -->
<!-- Copyright: (c) 2026 HYPERI PTY LIMITED           -->

# AWS Setup for dfe-fetcher

What a cloud administrator needs to configure so dfe-fetcher can read
security and operational data from AWS.

## What dfe-fetcher Needs

An IAM user (or role) with **read-only** access to the services you want
to fetch. dfe-fetcher never writes, modifies, or deletes resources.

Authentication uses static access keys or cross-account `AssumeRole`.
Credentials can be provided inline, via environment variables, or from
a secrets manager (OpenBao/Vault).

## Required Permissions

Enable only the services you configure. Not all are needed.

| Service | IAM Policy / Actions | Notes |
|---------|---------------------|-------|
| **CloudTrail** | `AWSCloudTrail_ReadOnlyAccess` (managed policy) | `LookupEvents` on management trail |
| **GuardDuty** | `guardduty:ListDetectors`, `guardduty:ListFindings`, `guardduty:GetFindings` | One detector per region |
| **SecurityHub** | `securityhub:GetFindings` | Requires SecurityHub enabled in account |
| **Config** | `config:SelectAggregateResourceConfig` | Requires an aggregator |
| **CloudWatch Logs** | `logs:FilterLogEvents`, `logs:DescribeLogGroups`, `logs:DescribeLogStreams`, `logs:GetLogEvents` | Read-only log access |
| **CloudWatch Metrics** | `cloudwatch:ListMetrics`, `cloudwatch:GetMetricData`, `cloudwatch:GetMetricStatistics`, `cloudwatch:DescribeAlarms` | Read-only metric access |

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
    }
  ]
}
```

## Terraform (Automated)

The test infrastructure is defined in
[`infra/test/main.tf`](../../infra/test/main.tf) (AWS section). It
provisions an IAM user with CloudTrail read-only and CloudWatch read
permissions, plus an access key.

```bash
cd infra/test
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set aws_profile and aws_region
terraform init
terraform apply
terraform output -json | python3 gen-env.py > ../../.env
```

The Terraform provisions CloudTrail and CloudWatch permissions only. For
GuardDuty, SecurityHub, or Config, attach additional policies to the IAM
user or add statements to the inline policy.

## Manual Setup

If you prefer not to use Terraform:

1. **Create IAM user**

   ```bash
   aws iam create-user --user-name dfe-fetcher
   ```

2. **Attach managed policies** (for CloudTrail)

   ```bash
   aws iam attach-user-policy \
     --user-name dfe-fetcher \
     --policy-arn arn:aws:iam::aws:policy/AWSCloudTrail_ReadOnlyAccess
   ```

3. **Create inline policy** for other services — use the JSON above,
   removing any statements for services you do not need.

   ```bash
   aws iam put-user-policy \
     --user-name dfe-fetcher \
     --policy-name dfe-fetcher-readonly \
     --policy-document file://policy.json
   ```

4. **Generate access key**

   ```bash
   aws iam create-access-key --user-name dfe-fetcher
   ```

5. **Configure dfe-fetcher**

### Config File

```yaml
sources:
  aws:
    enabled: true
    region: "ap-southeast-2"
    access_key_id: "AKIA..."
    secret_access_key: "wJalrXUtnFEMI..."
    services:
      - name: cloudtrail
      - name: guardduty
      - name: securityhub
    topic: "aws"
```

### Environment Variables

```bash
DFE_FETCHER_SOURCES__AWS__ENABLED="true"
DFE_FETCHER_SOURCES__AWS__REGION="ap-southeast-2"
DFE_FETCHER_SOURCES__AWS__ACCESS_KEY_ID="AKIA..."
DFE_FETCHER_SOURCES__AWS__SECRET_ACCESS_KEY="wJalrXUtnFEMI..."
```

### Secrets Manager (Production)

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

The vault secret should contain JSON:

```json
{
  "access_key_id": "AKIA...",
  "secret_access_key": "wJalrXUtnFEMI..."
}
```

## Cross-Account Access

To fetch from a different AWS account without sharing long-lived
credentials, use `assume_role_arn`. The fetcher calls `sts:AssumeRole`
and uses temporary credentials.

1. In the **target account**, create a role with the read-only policy
   above and a trust policy allowing the fetcher account to assume it.

2. In dfe-fetcher config:

   ```yaml
   sources:
     aws:
       enabled: true
       region: "us-east-1"
       access_key_id: "AKIA..."
       secret_access_key: "wJalrXUtnFEMI..."
       assume_role_arn: "arn:aws:iam::123456789012:role/dfe-fetcher-readonly"
       services:
         - name: cloudtrail
       topic: "aws"
   ```

The IAM user in the source account needs `sts:AssumeRole` permission
for the target role ARN.

## Cost

**Free.** IAM users, CloudTrail management event lookups, and CloudWatch
read API calls are free tier. No additional charges for read-only access.

CloudTrail data events and CloudWatch Logs storage are billed separately
by AWS, but dfe-fetcher only reads — it does not enable or create trails
or log groups.
