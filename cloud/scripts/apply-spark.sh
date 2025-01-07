#!/bin/bash
set -e

# Add necessary Helm repository
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

# Install Spark
helm upgrade --install spark bitnami/spark -f cloud/k8s/values/spark-values-2.yaml

echo "Spark has been successfully installed on the GKE cluster."