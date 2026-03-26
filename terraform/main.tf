terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-2"
}

resource "aws_s3_bucket" "raw_data" {
  bucket = "zoomcamp-2026-sheffield-crime-outcomes-submission-eu-west-2-an"
}