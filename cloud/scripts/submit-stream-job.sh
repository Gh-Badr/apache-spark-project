#!/bin/bash
set -e

#Create the /tmp/.ivy2 if it does not exist
echo "Creating /tmp/.ivy2 directory on worker..."
kubectl exec -it spark-master-0 -- mkdir -p /tmp/.ivy2

echo "Submitting Spark streaming job..."
# Copy the file to master pod instead
kubectl cp src/stream-processing/spark_streaming_job.py spark-master-0:/opt/bitnami/spark/work/spark_streaming_job.py

# Submit from master pod
kubectl exec -it spark-master-0 -- \
    spark-submit \
    --master spark://spark-master-svc:7077 \
    --deploy-mode client \
    --name cluster-events-analysis \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0 \
    --conf "spark.jars.ivy=/tmp/.ivy2" \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=512m \
    --conf spark.driver.memory=512m \
    --conf spark.sql.streaming.schemaInference=true \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.default.parallelism=4 \
    --conf spark.streaming.kafka.consumer.cache.enabled=false \
    --conf spark.streaming.kafka.maxRatePerPartition=50 \
    /opt/bitnami/spark/work/spark_streaming_job.py

