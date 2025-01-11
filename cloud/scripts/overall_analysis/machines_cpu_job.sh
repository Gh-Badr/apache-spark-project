#!/bin/bash
set -e

# Check if a project ID is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <project_id>"
  exit 1
fi

# Variables
PROJECT_ID="$1"

#Create the /tmp/.ivy2 if it does not exist
echo "Creating /tmp/.ivy2 directory on worker..."
kubectl exec -it spark-master-0 -- mkdir -p /tmp/.ivy2

echo "Submitting Spark job..."
# Copy the file to master pod instead
kubectl cp src/overall_analysis/machines_by_cpu.py spark-master-0:/opt/bitnami/spark/work/machines_by_cpu.py

kubectl exec -it spark-master-0 -- \
    spark-submit \
    --master spark://spark-master-svc:7077 \
    --deploy-mode client \
    --name google-cluster-analysis \
    --packages com.google.cloud:google-cloud-storage:2.22.4 \
    --conf "spark.jars.ivy=/tmp/.ivy2" \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    --conf spark.driver.memory=1g \
    --jars /tmp/gcs-connector.jar \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
    --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
    --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/etc/gcs/key.json \
    /opt/bitnami/spark/work/machines_by_cpu.py \
    --bucket ${PROJECT_ID}-cluster-analysis \