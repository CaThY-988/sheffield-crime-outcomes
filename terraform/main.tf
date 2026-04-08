terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source = "databricks/databricks"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

provider "databricks" {
  host = var.databricks_host
}

resource "aws_s3_bucket" "raw_data" {
  bucket = var.s3_bucket_name
}

# Bring the existing Databricks IAM role under Terraform control by reference
data "aws_iam_role" "existing_databricks_role" {
  name = var.existing_databricks_iam_role_name
}

# Databricks requires the IAM role for storage credentials to be self-assuming.
# The current docs show the Databricks Unity Catalog principal ARN below.
data "aws_iam_policy_document" "databricks_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type = "AWS"
      identifiers = [
        "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
        data.aws_iam_role.existing_databricks_role.arn
      ]
    }

    actions = ["sts:AssumeRole"]

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_external_id]
    }
  }
}

# Update the trust policy on the existing role
resource "aws_iam_role" "databricks_role_trust_update" {
  name               = data.aws_iam_role.existing_databricks_role.name
  assume_role_policy = data.aws_iam_policy_document.databricks_assume_role.json

  lifecycle {
    ignore_changes = [
      description,
      force_detach_policies,
      max_session_duration,
      path,
      permissions_boundary,
      tags
    ]
  }
}

# Grant the existing Databricks IAM role access to the new bucket
data "aws_iam_policy_document" "databricks_s3_access" {
  statement {
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]

    resources = [
      aws_s3_bucket.raw_data.arn
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [
      "${aws_s3_bucket.raw_data.arn}/*"
    ]
  }
}

resource "aws_iam_role_policy" "databricks_s3_access" {
  name   = "databricks-sheffield-new-bucket-access"
  role   = data.aws_iam_role.existing_databricks_role.name
  policy = data.aws_iam_policy_document.databricks_s3_access.json
}

resource "databricks_external_location" "police_raw" {
  name            = var.databricks_external_location_name
  url             = "s3://${aws_s3_bucket.raw_data.bucket}/police/"
  credential_name = var.databricks_storage_credential_name
  comment         = "External location for Sheffield crime raw data"

  depends_on = [aws_iam_role_policy.databricks_s3_access]
}