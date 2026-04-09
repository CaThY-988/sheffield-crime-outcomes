#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "==> Checking required tools"
command -v terraform >/dev/null 2>&1 || { echo "terraform is not installed"; exit 1; }

echo "==> Checking required environment variables"
: "${DATABRICKS_HOST:?DATABRICKS_HOST is not set}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN is not set}"

echo "==> Initialising Terraform"
terraform init

echo "==> Stage 1: bootstrap"
terraform apply -auto-approve \
  -var="create_external_location=false" \
  -var="databricks_external_id="

EXTERNAL_ID="$(terraform output -raw databricks_external_id)"

if [ -z "$EXTERNAL_ID" ]; then
  echo "ERROR: databricks_external_id output is empty"
  exit 1
fi

echo "==> Stage 2: update IAM trust policy only"
terraform apply -auto-approve \
  -var="databricks_external_id=${EXTERNAL_ID}" \
  -var="create_external_location=false"

echo "==> Waiting for IAM changes to propagate"
sleep 30

echo "==> Stage 3: create external location"
terraform apply -auto-approve \
  -var="databricks_external_id=${EXTERNAL_ID}" \
  -var="create_external_location=true"

echo "==> Deployment complete"

###############################################################################
# OPTIONAL: VERIFY THE FINAL STATE SAFELY
#
# A plain `terraform plan` may show a rollback toward bootstrap if your defaults
# are:
#   - databricks_external_id = ""
#   - create_external_location = false
#
# So if you want to inspect the FINAL state, do it with the same final inputs:
#
# terraform plan \
#   -var="databricks_external_id=${EXTERNAL_ID}" \
#   -var="create_external_location=true"
#
# If everything is correct, Terraform should report no changes needed.
###############################################################################