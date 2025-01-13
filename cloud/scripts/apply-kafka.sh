#!/bin/bash

# Check if the namespace kafka exists
if kubectl get namespace kafka &>/dev/null; then
  echo "Namespace kafka already exists."
else
  # Create namespace kafka
  kubectl create namespace kafka
fi

# Deploy Kafka operator
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Wait for Kafka operator
kubectl wait --for=condition=available --timeout=300s deployment/strimzi-cluster-operator -n kafka

# Apply kafka-cluster.yaml
kubectl apply -f cloud/k8s/kafka/kafka-cluster.yaml -n kafka

# Wait for Kafka cluster
kubectl wait --for=condition=Ready --timeout=600s kafka/kafka-cluster -n kafka

# Apply kafka-topic.yaml
kubectl apply -f cloud/k8s/kafka/kafka-topic.yaml -n kafka

# Apply kafdrop.yaml
kubectl apply -f cloud/k8s/kafka/kafdrop.yaml -n kafka

# Wait for Kafdrop
kubectl wait --for=condition=available --timeout=300s deployment/kafdrop -n kafka

# Sleep for 10 seconds
sleep 10

# Get the external IP of the svc (LoadBalancer)
kaftrop_ip=$(kubectl get svc kafdrop -n kafka -o=jsonpath='{.status.loadBalancer.ingress[0].ip}')
while [ -z "$kaftrop_ip" ]; do
  echo "Waiting for Kafdrop IP..."
  sleep 5
  kaftrop_ip=$(kubectl get svc kafdrop -n kafka -o=jsonpath='{.status.loadBalancer.ingress[0].ip}')
done

echo "Kafdrop is available at http://${kaftrop_ip}:9000"
