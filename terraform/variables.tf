variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket"
  type        = string
}

variable "databricks_host" {
  description = "Databricks workspace host"
  type        = string
}

variable "databricks_storage_credential_name" {
  type = string
}

variable "databricks_external_location_name" {
  type    = string
  default = "sheffield_crime_external_location"
}