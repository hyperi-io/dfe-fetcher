# dfe-fetcher Test Infrastructure

OpenTofu configuration that provisions the credentials dfe-fetcher's live
tests use against real AWS, Azure, Microsoft 365 and GCP APIs.

**Cost:** All resources are free tier -- IAM users, app registrations, service
accounts, CloudTrail management events, Activity Log reads, and Cloud Logging
reads cost nothing.

## What Gets Created

| Cloud | Resource | Purpose |
|-------|----------|---------|
| AWS | IAM user `dfe-fetcher-test` + access key | SigV4-signed CloudTrail and CloudWatch reads |
| Azure | App registration `dfe-fetcher-test` + client secret + Reader role | OAuth2 Activity Log reads |
| Microsoft 365 | A second app registration + client secret | The M365 live tests |
| GCP | Service account `dfe-fetcher-test` + JSON key + Logs Viewer role | JWT-signed Cloud Logging reads |

## Prerequisites

1. AWS signed in through SSO (`aws sso login --profile <profile>`)
2. Azure signed in with `az login`
3. GCP signed in with `gcloud auth application-default login`
4. OpenTofu's `tofu` binary on the path

## Setup

```bash
cd infra/test

# Configure (copy example and fill in your account/project IDs)
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values

# Provision
tofu init
tofu apply

# Generate .env with credentials
tofu output -json | python3 gen-env.py > ../../.env
```

The `.env` file and `.tmp/gcp-sa-key.json` are gitignored.

## Run the Live Tests

```bash
cd ../..
cargo nextest run -p dfe-fetcher --test e2e --run-ignored only aws_ azure_ m365_ gcp_health_check gcp_fetch_
```

## Teardown

```bash
cd infra/test
tofu destroy
```

## Azure Account Migration

When migrating to a new Azure tenant/subscription:

1. `tofu destroy` (removes old resources)
2. `az login` to the new tenant
3. Update `azure_subscription_id` in `terraform.tfvars`
4. `tofu apply` (creates new resources)
5. Regenerate `.env`: `tofu output -json | python3 gen-env.py > ../../.env`

The OpenTofu state tracks exactly what was created, so there's nothing to
manually chase down in the old account.
