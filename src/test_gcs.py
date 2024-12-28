from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GCS Test") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/etc/gcs/key.json")
spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

df = spark.read.csv("gs://clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz")
print(f"Number of rows: {df.count()}")
spark.stop()