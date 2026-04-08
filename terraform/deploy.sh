#!/usr/bin/env bash
set -euo pipefail

echo "==> Checking required tools"
command -v terraform >/dev/null 2>&1 || { echo "terraform is not installed"; exit 1; }

echo "==> Checking required environment variables"
: "${DATABRICKS_HOST:?DATABRICKS_HOST is not set}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN is not set}"

echo "==> Initialising Terraform"
terraform init

echo "==> Stage 1: bootstrap"
echo "    - create S3 bucket"
echo "    - create IAM policy + role with bootstrap external ID"
echo "    - create Databricks storage credential"

terraform apply -auto-approve \
  -var="create_external_location=false" \
  -var="databricks_external_id="

echo "==> Reading Databricks-generated external ID"
EXTERNAL_ID="$(terraform output -raw databricks_external_id)"

if [ -z "$EXTERNAL_ID" ]; then
  echo "ERROR: databricks_external_id output is empty"
  exit 1
fi

echo "==> External ID found"

echo "==> Stage 2: finalize"
echo "    - update IAM trust policy with real external ID"
echo "    - create Databricks external location"
echo "    - apply Databricks grants"

terraform apply -auto-approve \
  -var="databricks_external_id=${EXTERNAL_ID}" \
  -var="create_external_location=true"

echo "==> Deployment complete"
echo "IAM trust policy updated with Databricks external ID: ${EXTERNAL_ID}"