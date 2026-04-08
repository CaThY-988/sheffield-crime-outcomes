terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "raw_data" {
  bucket = var.s3_bucket_name
}

provider "databricks" {
  host  = var.databricks_host
}

resource "databricks_external_location" "police_raw" {
  name            = var.databricks_external_location_name
  url             = "s3://${aws_s3_bucket.raw_data.bucket}/police/"
  credential_name = var.databricks_storage_credential_name
  comment         = "External location for Sheffield crime raw data"
}