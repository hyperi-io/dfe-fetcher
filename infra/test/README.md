# dfe-fetcher Test Infrastructure

Terraform configuration to provision lightweight cloud credentials for testing
dfe-fetcher against real AWS, Azure, and GCP APIs.

**Cost:** All resources are free tier — IAM users, app registrations, service
accounts, CloudTrail management events, Activity Log reads, and Cloud Logging
reads cost nothing.

## What Gets Created

| Cloud | Resource | Purpose |
|-------|----------|---------|
| AWS | IAM user `dfe-fetcher-test` + access key | SigV4-signed CloudTrail API calls |
| Azure | App registration `dfe-fetcher-test` + client secret + Reader role | OAuth2 Activity Log reads |
| GCP | Service account `dfe-fetcher-test` + JSON key + Logs Viewer role | JWT-signed Cloud Logging reads |

## Prerequisites

1. **AWS:** Authenticated via SSO (`aws sso login --profile <profile>`)
2. **Azure:** Authenticated via `az login`
3. **GCP:** Authenticated via `gcloud auth application-default login`
4. **Terraform:** v1.5+

## Setup

```bash
cd infra/test

# Configure (copy example and fill in your account/project IDs)
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values

# Provision
terraform init
terraform apply

# Generate .env with credentials
terraform output -json | python3 gen-env.py > ../../.env
```

The `.env` file and `.tmp/gcp-sa-key.json` are gitignored.

## Run Smoke Tests

```bash
cd ../..
cargo test --test smoke_cloud -- --ignored
```

## Teardown

```bash
cd infra/test
terraform destroy
```

## Azure Account Migration

When migrating to a new Azure tenant/subscription:

1. `terraform destroy` (removes old resources)
2. `az login` to the new tenant
3. Update `azure_subscription_id` in `terraform.tfvars`
4. `terraform apply` (creates new resources)
5. Regenerate `.env`: `terraform output -json | python3 gen-env.py > ../../.env`

The Terraform state tracks exactly what was created, so there's nothing to
manually chase down in the old account.
