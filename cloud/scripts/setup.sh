#!/bin/bash
set -e

# Check if a project ID is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <project_id>"
  exit 1
fi

# Variables
PROJECT_ID="$1"
SERVICE_ACCOUNT_NAME="gcp-infra-deployer-sa"
DEFAULT_PROJECT_ID=$(gcloud config get-value project)
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${DEFAULT_PROJECT_ID}.iam.gserviceaccount.com"
KEY_FILE_PATH="~/terraform-key.json"

# Check if the user is already authenticated
if gcloud auth application-default print-access-token &>/dev/null; then
  echo "Already authenticated with Google Cloud."
else
  echo "Authenticating with Google Cloud..."
  gcloud auth application-default login
fi

# Check if the service account already exists in the default project
EXISTING_ACCOUNT=$(gcloud iam service-accounts list --project=${DEFAULT_PROJECT_ID} --filter="email:${SERVICE_ACCOUNT_EMAIL}" --format="value(email)")

if [ -z "${EXISTING_ACCOUNT}" ]; then
  echo "Service account does not exist. Creating service account..."
  gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} --project=${DEFAULT_PROJECT_ID} --display-name "Terraform Service Account"
  gcloud projects add-iam-policy-binding ${DEFAULT_PROJECT_ID} --member "serviceAccount:${SERVICE_ACCOUNT_EMAIL}" --role "roles/owner"
  gcloud iam service-accounts keys create ${KEY_FILE_PATH} --iam-account ${SERVICE_ACCOUNT_EMAIL}
  export GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE_PATH}
else
  echo "Service account already exists: ${EXISTING_ACCOUNT}"
fi

# Grant roles to the service account in the target project if it's different from the default project
if [ "${PROJECT_ID}" != "${DEFAULT_PROJECT_ID}" ]; then
  echo "Granting roles to the service account in the target project..."
  gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member "serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role "roles/owner"
else
  echo "Target project ID is the same as the default project ID. Skipping role granting."
fi

# Export the service account key file path
export GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE_PATH}

echo "Setup completed successfully."