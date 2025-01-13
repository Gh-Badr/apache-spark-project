#!/bin/bash
set -e

# Check if a project ID is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <job_file.py>"
  exit 1
fi

# Variables
JOB_FILE=$1

#Create the /tmp/.ivy2 if it does not exist
echo "Creating /tmp/.ivy2 directory on worker..."
kubectl exec -it spark-master-0 -- mkdir -p /tmp/.ivy2

echo "Checking if the file is under src/overall_analysis/ directory"
if [ ! -f src/overall_analysis/$JOB_FILE ]; then
  echo "File src/overall_analysis/$JOB_FILE does not exist. Make sure the file is under src/overall_analysis/ directory."
  exit 1
fi

echo "Copying the file src/overall_analysis/$JOB_FILE to master pod..."
# Copy the file to master pod instead
kubectl cp src/overall_analysis/$JOB_FILE spark-master-0:/opt/bitnami/spark/work/$JOB_FILE

# Install matplotlib if not already installed
echo "Checking if matplotlib is installed..."
if ! kubectl exec -it spark-master-0 -- pip3 list | grep matplotlib; then
  echo "Installing matplotlib..."
  kubectl exec -it spark-master-0 -- pip3 install matplotlib
fi

echo "Running the job..."
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
    /opt/bitnami/spark/work/$JOB_FILE \