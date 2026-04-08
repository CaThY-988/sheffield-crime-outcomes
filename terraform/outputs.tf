output "raw_data_bucket_name" {
  description = "Name of the raw data S3 bucket."
  value       = aws_s3_bucket.raw_data.bucket
}

output "iam_role_arn" {
  description = "ARN of the IAM role Databricks assumes."
  value       = aws_iam_role.databricks_role.arn
}

output "databricks_external_id" {
  description = "Databricks-generated external ID for the IAM trust policy."
  value       = databricks_storage_credential.raw.aws_iam_role[0].external_id
}