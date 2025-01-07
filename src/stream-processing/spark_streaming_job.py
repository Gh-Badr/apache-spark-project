from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

def create_spark_session():
    return SparkSession.builder \
        .appName("AnalysisWithStreamProcessing") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()

def define_schemas():
    job_schema = StructType([
        StructField("timestamp", LongType(), True),
        StructField("missing_type", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user", StringType(), True),
        StructField("scheduling_class", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("logical_job_name", StringType(), True)
    ])
    
    task_schema = StructType([
        StructField("timestamp", LongType(), True),
        StructField("missing_type", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("task_index", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user", StringType(), True),
        StructField("scheduling_class", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("cpu_request", StringType(), True),
        StructField("memory_request", StringType(), True),
        StructField("disk_space_request", StringType(), True)
    ])
    
    return job_schema, task_schema

def process_streams():
    print("Creating Spark session...")
    spark = create_spark_session()
    print("Defining schemas...")
    job_schema, task_schema = define_schemas()
    
    print("Setting up Kafka streams...")
    kafka_bootstrap = "kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
    
    # Read job events stream
    job_events = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", "job-events") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)")
    
    # Read task events stream
    task_events = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", "task-events") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)")
    
    print("Parsing JSON data...")
    # Parse JSON data with simplified schema
    parsed_jobs = job_events \
        .select(from_json(col("value"), job_schema).alias("data")) \
        .select(
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_timestamp"),
            "data.event_type",
            "data.job_id"
        ).repartition(4)
    
    parsed_tasks = task_events \
        .select(from_json(col("value"), task_schema).alias("data")) \
        .select(
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_timestamp"),
            "data.event_type",
            "data.scheduling_class",
            "data.job_id"
        ).repartition(4)

    print("Setting up streaming queries...")
    # Simple aggregations without watermark
    job_counts = parsed_jobs \
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            "event_type"
        ) \
        .count() \
        .orderBy("window")

    task_distribution = parsed_tasks \
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            "scheduling_class",
            "event_type"
        ) \
        .count() \
        .orderBy("window")
    
    print("Starting streaming queries...")
    # Start the streaming queries with smaller trigger intervals
    queries = [
        job_counts.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 10) \
            .trigger(processingTime="10 seconds") \
            .start(),
            
        task_distribution.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 10) \
            .trigger(processingTime="10 seconds") \
            .start()
    ]
    
    print("Streaming queries started with the following analyses:")
    print("1. Job events count by type")
    print("2. Task distribution by scheduling class")
    print("\nAwaiting termination...")
    
    # Wait for any query to terminate
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    print("Starting Spark streaming job...")
    process_streams()