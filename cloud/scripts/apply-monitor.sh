#!/bin/bash
set -e

# Add Prometheus repo
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Install Prometheus Stack (includes Grafana)
helm install monitoring prometheus-community/kube-prometheus-stack \
    --namespace monitoring \
    --create-namespace \
    -f cloud/k8s/values/prom-grafana.yaml

# Wait for the Grafana service to get an external IP
kubectl wait --for=condition=available --timeout=600s deployment/monitoring-grafana -n monitoring

# Grafana URL
GRAFANA_URL=$(kubectl get svc -n monitoring monitoring-grafana -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "Grafana URL: http://${GRAFANA_URL}"
  
