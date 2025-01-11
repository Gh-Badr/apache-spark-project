#!/bin/bash
set -e

# Check if a project ID is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <project_id>"
  exit 1
fi

# gcloud Variables
export PROJECT_ID="$1"  # Export for envsubst
SECRETS_DIR="cloud/k8s/secrets"
KEY_FILE="${SECRETS_DIR}/spark-sa-key.json"

# Kubernetes Variables
CONFIG_MAP_NAME="producer-config"
NAMESPACE="kafka"
GCS_SECRET_NAME="spark-gcs-secret"
PRODUCER_FILE_PATH="src/stream-processing/producer.py"
DEPLOYMENT_TEMPLATE_PATH="cloud/k8s/kafka/producer-deployment.yaml"
DEPLOYMENT_FILE_PATH="cloud/k8s/kafka/producer-deployment-rendered.yaml"

# Check if the key file already exists and is valid
if [ -f "${KEY_FILE}" ]; then
    echo "Key file already exists: ${KEY_FILE}"
else
    echo "Key file does not exist. Run the following command to create it:"
    echo "./cloud/scripts/configure-gcs.sh ${PROJECT_ID}"
fi

echo "Creating Kubernetes secret and config map..."
# Create a Kubernetes secret for the service account key
kubectl create secret generic $GCS_SECRET_NAME --from-file=key.json=${KEY_FILE} --namespace=$NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# Create or update the config map with the producer.py file
kubectl create configmap $CONFIG_MAP_NAME --from-file=producer.py=$PRODUCER_FILE_PATH --namespace=$NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

echo "Applying deployment..."
export START_FILE="0"
export NUM_FILES="20"
export SPEED_FACTOR=432000 # 5 days = 1 sec
# export SPEED_FACTOR="86400" # 1 day = 1 sec
# export SPEED_FACTOR=3600 # 1 hour = 1 sec

# Render the deployment template with the project ID
envsubst '$PROJECT_ID,$START_FILE,$NUM_FILES,$SPEED_FACTOR' < ${DEPLOYMENT_TEMPLATE_PATH} > ${DEPLOYMENT_FILE_PATH}

# Apply the rendered deployment
kubectl apply -f ${DEPLOYMENT_FILE_PATH} --namespace=$NAMESPACE

# Clean up
rm ${DEPLOYMENT_FILE_PATH}

echo "Config map and deployment applied successfully."

# Command to change the environment variables when the deployment is already running
# kubectl set env deployment/event-producer START_FILE=1 NUM_FILES=5 --namespace=kafka