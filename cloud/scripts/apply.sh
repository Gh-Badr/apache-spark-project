#!/bin/bash
set -e

# Check if a project ID is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <project_id>"
  exit 1
fi

# Variables
PROJECT_ID="$1"

# Change to the terraform directory
cd cloud/terraform

# Retrieve the cluster name and zone from Terraform outputs
CLUSTER_NAME=$(terraform output -raw cluster_name)
CLUSTER_ZONE=$(terraform output -raw cluster_location)

# Change to the root directory
cd ../..

# Get credentials for the GKE cluster
gcloud container clusters get-credentials ${CLUSTER_NAME} --zone ${CLUSTER_ZONE} --project ${PROJECT_ID}

# Add necessary Helm repository
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

# Install Spark
helm upgrade --install spark bitnami/spark \
    --wait

echo "Spark has been successfully installed on the GKE cluster."