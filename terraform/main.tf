########################################
# Terraform + Providers
########################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.100"
    }
  }
}

# AWS provider uses region from terraform.tfvars
provider "aws" {
  region = var.aws_region
}

# Databricks provider reads credentials from environment variables:
# - DATABRICKS_HOST
# - DATABRICKS_TOKEN
provider "databricks" {}

########################################
# Local values (helper variables)
########################################

locals {
  # Full S3 path used by Databricks external location
  s3_url = "s3://${aws_s3_bucket.raw_data.bucket}/${var.bucket_prefix}/"

  # ARN for objects inside the prefix (used in IAM policy)
  object_arn = "${aws_s3_bucket.raw_data.arn}/${var.bucket_prefix}/*"

  # Use real Databricks external ID if available, otherwise fallback to bootstrap value
  # This is what enables the 2-stage deployment
  resolved_external_id = var.databricks_external_id != "" ? var.databricks_external_id : var.bootstrap_external_id
}

########################################
# S3 Bucket (data storage)
########################################

# Bucket where raw data will be stored
resource "aws_s3_bucket" "raw_data" {
  bucket = var.s3_bucket_name

  tags = {
    Project     = "sheffield-crime-outcomes"
  }
}

# Block all public access (best practice for data pipelines)
resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

########################################
# IAM Policy (what Databricks can do in S3)
########################################

# Define permissions for accessing S3
data "aws_iam_policy_document" "s3_access" {

  # Allow listing the bucket (restricted to the prefix)
  statement {
    sid = "ListBucket"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket"
    ]

    resources = [aws_s3_bucket.raw_data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [var.bucket_prefix, "${var.bucket_prefix}/*"]
    }
  }

  # Allow read/write/delete on objects inside the prefix
  statement {
    sid = "ObjectAccess"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [local.object_arn]
  }
}

# Create IAM policy from the document above
resource "aws_iam_policy" "databricks_s3" {
  name   = "${var.iam_role_name}-s3-policy"
  policy = data.aws_iam_policy_document.s3_access.json
}

########################################
# IAM Role (identity Databricks assumes)
########################################

# Trust policy: defines WHO can assume this role
data "aws_iam_policy_document" "assume_role" {

  # Allow Databricks Unity Catalog to assume the role
  statement {
    sid     = "DatabricksAssumeRole"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [var.databricks_uc_master_role_arn]
    }

    # External ID condition (security requirement)
    # This is the tricky part requiring 2-stage deployment
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [local.resolved_external_id]
    }
  }

  # Allow the role to assume itself (Databricks requirement)
  statement {
    sid     = "SelfAssumeRole"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${var.aws_account_id}:role/${var.iam_role_name}"]
    }
  }
}

# Create the IAM role
resource "aws_iam_role" "databricks_role" {
  name               = var.iam_role_name
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

# Attach S3 access policy to the role
resource "aws_iam_role_policy_attachment" "attach_s3" {
  role       = aws_iam_role.databricks_role.name
  policy_arn = aws_iam_policy.databricks_s3.arn
}

########################################
# Databricks Storage Credential
########################################

# This tells Databricks:
# "Use this IAM role to access cloud storage"
resource "databricks_storage_credential" "raw" {
  name = var.databricks_storage_credential_name

  aws_iam_role {
    role_arn = aws_iam_role.databricks_role.arn
  }

  comment   = "Managed by Terraform for Sheffield crime outcomes project"
  read_only = false
}

########################################
# Databricks External Location
########################################

# Only created in stage 2 (after IAM trust is fixed)
resource "databricks_external_location" "police_raw" {
  count = var.create_external_location ? 1 : 0

  # Name of external location in Unity Catalog
  name = var.databricks_external_location_name

  # S3 path Databricks will access
  url = local.s3_url

  # Link to the storage credential created above
  credential_name = databricks_storage_credential.raw.id

  comment = "External location for Sheffield crime raw data"

  # Ensure dependencies are created first
  depends_on = [
    databricks_storage_credential.raw,
    aws_iam_role.databricks_role
  ]
}

########################################
# Databricks Grants (permissions)
########################################

# Give a user/group permission to use the external location
resource "databricks_grants" "police_raw" {
  count = var.create_external_location ? 1 : 0

  external_location = databricks_external_location.police_raw[0].id

  grant {
    principal  = var.databricks_principal
    privileges = [
      "READ_FILES",
      "WRITE_FILES",
      "CREATE_EXTERNAL_TABLE"
    ]
  }
}