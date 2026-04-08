variable "aws_region" {
  type    = string
  default = "eu-west-2"
}

variable "s3_bucket_name" {
  type = string
}

variable "databricks_host" {
  type = string
}

variable "databricks_external_location_name" {
  type    = string
  default = "sheffield_crime_external_location"
}

variable "databricks_storage_credential_name" {
  type = string
}

variable "existing_databricks_iam_role_name" {
  type = string
}

variable "databricks_external_id" {
  type = string
}