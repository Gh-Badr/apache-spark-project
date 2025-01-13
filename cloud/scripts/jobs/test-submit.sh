#!/bin/bash
set -e

# Copy the test script to the Spark worker
kubectl cp src/test_gcs.py spark-worker-0:/opt/bitnami/spark/work/test_gcs.py

# Submit the test script to the Spark cluster
kubectl exec -it spark-worker-0 -- \
    spark-submit \
    --master spark://spark-master-svc:7077 \
    --jars /tmp/gcs-connector.jar \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
    --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
    --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/etc/gcs/key.json \
    /opt/bitnami/spark/work/test_gcs.py
