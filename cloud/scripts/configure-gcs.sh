#!/bin/bash
set -e

# Check if a project ID is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <project_id>"
  exit 1
fi

# gcloud Variables
export PROJECT_ID="$1"  # Export for envsubst
SERVICE_ACCOUNT_NAME="spark-gcs-sa"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
SECRETS_DIR="cloud/k8s/secrets"
KEY_FILE="${SECRETS_DIR}/spark-sa-key.json"

# Ensure secrets directory exists
mkdir -p ${SECRETS_DIR}

echo "Configuring service account for GCS access..."

# Check if the key file already exists and is valid
if [ -f "${KEY_FILE}" ]; then
    echo "Key file already exists: ${KEY_FILE}"
else
    # Check if the service account already exists
    EXISTING_ACCOUNT=$(gcloud iam service-accounts list --project=${PROJECT_ID} --filter="email:${SERVICE_ACCOUNT_EMAIL}" --format="value(email)")

    if [ -z "${EXISTING_ACCOUNT}" ]; then
        # Create a service account for Spark GCS access
        gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
                --description="Service account for Spark GCS access" \
                --display-name="Spark GCS SA" \
                --project=${PROJECT_ID}

        # Grant the necessary roles to the service account
        gcloud projects add-iam-policy-binding $PROJECT_ID \
                --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
                --role="roles/storage.objectViewer" \
                --role="roles/storage.objectCreator" \
                --project=${PROJECT_ID}
    else
        echo "Service account already exists: ${EXISTING_ACCOUNT}"
    fi

    # Create a key for the service account
    gcloud iam service-accounts keys create ${KEY_FILE} \
            --iam-account=${SERVICE_ACCOUNT_EMAIL} \
            --project=${PROJECT_ID}
fi