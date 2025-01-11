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
VALUES_FILE="cloud/k8s/values/spark-values-gcs.yml"
SECRETS_DIR="cloud/k8s/secrets"
KEY_FILE="${SECRETS_DIR}/spark-sa-key.json"

# Check if the key file already exists and is valid
if [ -f "${KEY_FILE}" ]; then
    echo "Key file already exists: ${KEY_FILE}"
else
    echo "Key file does not exist. Run the following command to create it:"
    echo "./cloud/scripts/configure-gcs.sh ${PROJECT_ID}"
fi

# Create a Kubernetes secret for the service account key
kubectl create secret generic spark-gcs-secret \
    --from-file=key.json=${KEY_FILE} \
    --dry-run=client -o yaml | kubectl apply -f -

# Install Spark
helm upgrade spark bitnami/spark \
    -f ${VALUES_FILE} \
    --wait

# Create jars directory
mkdir -p cloud/k8s/jars

# Download the GCS connector JAR
wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -O cloud/k8s/jars/gcs-connector.jar

# Copy the GCS connector JAR to the Spark worker
kubectl cp cloud/k8s/jars/gcs-connector.jar spark-master-0:/tmp/gcs-connector.jar
kubectl cp cloud/k8s/jars/gcs-connector.jar spark-worker-0:/tmp/gcs-connector.jar
kubectl cp cloud/k8s/jars/gcs-connector.jar spark-worker-1:/tmp/gcs-connector.jar

# Clean up
rm cloud/k8s/jars/gcs-connector.jar
rm -rf cloud/k8s/jars

echo "Spark has been successfuly configured to use GCS."