# Project:   dfe-fetcher
# File:      infra/test/main.tf
# Purpose:   Provision lightweight test credentials for AWS, Azure, GCP
# Language:  HCL (Terraform)
#
# License:   FSL-1.1-ALv2
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   cd infra/test
#   cp terraform.tfvars.example terraform.tfvars  # fill in values
#   terraform init
#   terraform apply
#   terraform output -json | python3 gen-env.py > ../../.env
#
# Teardown:
#   terraform destroy

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

# =============================================================================
# Variables — actual values in terraform.tfvars (gitignored) or .env
# =============================================================================

variable "aws_profile" {
  description = "AWS CLI profile name"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-southeast-2"
}

variable "azure_subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
}

# =============================================================================
# Providers
# =============================================================================

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

provider "azuread" {}

provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
}

provider "google" {
  project = var.gcp_project_id
}

# =============================================================================
# AWS — IAM user with read-only access to fetched services
# =============================================================================
# Cost: Free (IAM users, CloudTrail management events, CloudWatch reads are free)

resource "aws_iam_user" "fetcher_test" {
  name = "dfe-fetcher-test"
  tags = {
    Purpose   = "dfe-fetcher smoke test"
    ManagedBy = "terraform"
  }
}

resource "aws_iam_user_policy_attachment" "cloudtrail_readonly" {
  user       = aws_iam_user.fetcher_test.name
  policy_arn = "arn:aws:iam::aws:policy/AWSCloudTrail_ReadOnlyAccess"
}

resource "aws_iam_user_policy" "cloudwatch_readonly" {
  name = "dfe-fetcher-cloudwatch-readonly"
  user = aws_iam_user.fetcher_test.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogsRead"
        Effect = "Allow"
        Action = [
          "logs:FilterLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
          "logs:GetLogEvents",
        ]
        Resource = "*"
      },
      {
        Sid    = "CloudWatchMetricsRead"
        Effect = "Allow"
        Action = [
          "cloudwatch:ListMetrics",
          "cloudwatch:GetMetricData",
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:DescribeAlarms",
        ]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_access_key" "fetcher_test" {
  user = aws_iam_user.fetcher_test.name
}

# =============================================================================
# Azure — App registration with Reader role on subscription
# =============================================================================
# Cost: Free (app registrations and Activity Log reads are free)
#
# To recreate after tenant migration:
#   1. Update azure_subscription_id in terraform.tfvars
#   2. Re-authenticate az CLI to the new tenant
#   3. terraform apply

data "azuread_client_config" "current" {}

resource "azuread_application" "fetcher_test" {
  display_name = "dfe-fetcher-test"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "fetcher_test" {
  client_id = azuread_application.fetcher_test.client_id
  owners    = [data.azuread_client_config.current.object_id]
}

resource "azuread_application_password" "fetcher_test" {
  application_id = azuread_application.fetcher_test.id
  display_name   = "dfe-fetcher-test-secret"
  end_date       = timeadd(timestamp(), "8760h") # 1 year
  lifecycle {
    ignore_changes = [end_date]
  }
}

resource "azurerm_role_assignment" "fetcher_reader" {
  scope                = "/subscriptions/${var.azure_subscription_id}"
  role_definition_name = "Reader"
  principal_id         = azuread_service_principal.fetcher_test.object_id
}

# =============================================================================
# M365 — App registration for Office 365 Management Activity API + Graph Security
# =============================================================================
# Cost: Free (app registrations and API reads are free)
#
# Permissions (application, not delegated):
#   Office 365 Management API (c5393580-f805-4401-95e8-94b7a6ef2fc2):
#     - ActivityFeed.Read (594c1fb6-4f81-4475-ae41-0c394909246c)
#   Microsoft Graph (00000003-0000-0000-c000-000000000000):
#     - SecurityEvents.Read.All (bf394140-e372-4bf9-a898-299cfc7564e5)
#     - SecurityAlert.Read.All  (472e4a4d-bb4a-4026-98d1-0b0d74cb74a5)
#     - Reports.Read.All        (230c1aed-a721-4c5d-9cb4-a90514e508ef)
#
# Admin consent is required after apply:
#   az ad app permission admin-consent --id <m365_client_id>

resource "azuread_application" "m365_fetcher_test" {
  display_name = "dfe-fetcher-m365-test"
  owners       = [data.azuread_client_config.current.object_id]

  # Office 365 Management API — ActivityFeed.Read
  required_resource_access {
    resource_app_id = "c5393580-f805-4401-95e8-94b7a6ef2fc2"
    resource_access {
      id   = "594c1fb6-4f81-4475-ae41-0c394909246c"
      type = "Role"
    }
  }

  # Microsoft Graph — SecurityEvents, SecurityAlert, Reports
  required_resource_access {
    resource_app_id = "00000003-0000-0000-c000-000000000000"
    resource_access {
      id   = "bf394140-e372-4bf9-a898-299cfc7564e5" # SecurityEvents.Read.All
      type = "Role"
    }
    resource_access {
      id   = "472e4a4d-bb4a-4026-98d1-0b0d74cb74a5" # SecurityAlert.Read.All
      type = "Role"
    }
    resource_access {
      id   = "230c1aed-a721-4c5d-9cb4-a90514e508ef" # Reports.Read.All
      type = "Role"
    }
  }
}

resource "azuread_service_principal" "m365_fetcher_test" {
  client_id = azuread_application.m365_fetcher_test.client_id
  owners    = [data.azuread_client_config.current.object_id]
}

resource "azuread_application_password" "m365_fetcher_test" {
  application_id = azuread_application.m365_fetcher_test.id
  display_name   = "dfe-fetcher-m365-test-secret"
  end_date       = timeadd(timestamp(), "8760h") # 1 year
  lifecycle {
    ignore_changes = [end_date]
  }
}

# =============================================================================
# GCP — Service account with Logs Viewer role
# =============================================================================
# Cost: Free (service accounts and Cloud Logging reads are free)

resource "google_service_account" "fetcher_test" {
  account_id   = "dfe-fetcher-test"
  display_name = "dfe-fetcher test"
  project      = var.gcp_project_id
}

resource "google_project_iam_member" "logs_viewer" {
  project = var.gcp_project_id
  role    = "roles/logging.viewer"
  member  = "serviceAccount:${google_service_account.fetcher_test.email}"
}

resource "google_service_account_key" "fetcher_test" {
  service_account_id = google_service_account.fetcher_test.name
}

# =============================================================================
# Outputs — used by gen-env.py to create .env
# =============================================================================

output "aws_access_key_id" {
  value     = aws_iam_access_key.fetcher_test.id
  sensitive = true
}

output "aws_secret_access_key" {
  value     = aws_iam_access_key.fetcher_test.secret
  sensitive = true
}

output "aws_region" {
  value = var.aws_region
}

output "azure_tenant_id" {
  value = data.azuread_client_config.current.tenant_id
}

output "azure_client_id" {
  value     = azuread_application.fetcher_test.client_id
  sensitive = true
}

output "azure_client_secret" {
  value     = azuread_application_password.fetcher_test.value
  sensitive = true
}

output "azure_subscription_id" {
  value = var.azure_subscription_id
}

output "m365_tenant_id" {
  value = data.azuread_client_config.current.tenant_id
}

output "m365_client_id" {
  value     = azuread_application.m365_fetcher_test.client_id
  sensitive = true
}

output "m365_client_secret" {
  value     = azuread_application_password.m365_fetcher_test.value
  sensitive = true
}

output "gcp_project_id" {
  value = var.gcp_project_id
}

output "gcp_service_account_key" {
  value     = google_service_account_key.fetcher_test.private_key
  sensitive = true
}
