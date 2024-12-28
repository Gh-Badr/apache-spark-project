#!/bin/bash
set -e

# Check if a project ID is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <project_id>"
  exit 1
fi

# Variables
PROJECT_ID="$1"
SERVICE_ACCOUNT_NAME="spark-gcs-sa"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
VALUES_FILE="cloud/k8s/values/spark-values.yml"
SECRETS_DIR="cloud/k8s/secrets"
KEY_FILE="${SECRETS_DIR}/spark-sa-key.json"

# Ensure secrets directory exists
mkdir -p ${SECRETS_DIR}

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
        --project=${PROJECT_ID}
else
  echo "Service account already exists: ${EXISTING_ACCOUNT}"
fi

# Create a key for the service account
gcloud iam service-accounts keys create ${KEY_FILE} \
    --iam-account=${SERVICE_ACCOUNT_EMAIL} \
    --project=${PROJECT_ID}

# Create a Kubernetes secret for the service account key
kubectl create secret generic spark-gcs-secret \
    --from-file=key.json=${KEY_FILE} \
    --dry-run=client -o yaml | kubectl apply -f -

# Install Spark
helm upgrade spark bitnami/spark \
    -f ${VALUES_FILE} \
    --wait

# Download the GCS connector JAR
wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -O cloud/k8s/jars/gcs-connector.jar

# Copy the GCS connector JAR to the Spark worker
kubectl cp cloud/k8s/jars/gcs-connector.jar spark-worker-0:/tmp/gcs-connector.jar

# Clean up
rm -f ${KEY_FILE}
rm cloud/k8s/jars/gcs-connector.jar

echo "Spark has been successfuly configured to use GCS."