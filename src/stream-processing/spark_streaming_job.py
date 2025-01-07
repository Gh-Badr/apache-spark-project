from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

metrics_data = []

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics/":
            print(f'GET request received for {self.path}')
            print(f'Responding with {len(metrics_data)} metrics')
            self.send_response(200)
            self.send_header("Content-type", "text/plain; charset=utf-8")
            self.end_headers()
            metrics = "\n".join(metrics_data)
            self.wfile.write(metrics.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

def start_metrics_server():
    server_address = ("0.0.0.0", 8181)  # Bind to all interfaces
    httpd = HTTPServer(server_address, MetricsHandler)
    print("Starting metrics server on port 8081...")
    httpd.serve_forever()

def create_spark_session():
    return SparkSession.builder \
        .appName("stream-process") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .config("spark.metrics.conf.*.sink.prometheusServlet.class", "org.apache.spark.metrics.sink.PrometheusServlet") \
        .config("spark.metrics.conf.*.sink.prometheusServlet.path", "/metrics/") \
        .config("spark.sql.streaming.metricsEnabled", "true") \
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

# This function is called for each batch of the stream
# It collects the results of the batch and creates the metrics
def foreach_batch_function(df, epoch_id, metric_name):
    global metrics_data
    results = df.collect()
    temp_metrics = []

    # Get the current time (now) in seconds
    current_time = float(time.time())

    for row in results:
        # Calculate the relative timestamp (current_time - event_timestamp)
        event_timestamp = row['event_timestamp'].timestamp() / 1000.0  # Convert to seconds
        relative_timestamp = int(current_time - event_timestamp)  # This is the difference in seconds

        if 'event_type' in row:
            metric_value = row['count']
            event_type = row['event_type']
            temp_metrics.append(f'# TYPE spark_streaming_{metric_name}_count gauge')
            # Use relative timestamp for the metric
            temp_metrics.append(f'spark_streaming_{metric_name}_count{{event_type="{event_type}"}} {metric_value} {relative_timestamp}')
        
        if 'scheduling_class' in row:
            metric_value = row['count']
            scheduling_class = str(row['scheduling_class'])
            temp_metrics.append(f'# TYPE spark_streaming_{metric_name}_count gauge')
            # Use relative timestamp for the metric
            temp_metrics.append(f'spark_streaming_{metric_name}_count{{scheduling_class="{scheduling_class}"}} {metric_value} {relative_timestamp}')

    metrics_data.extend(temp_metrics)


def monitor_query_progress(query, query_name):
    global metrics_data
    while query.isActive:
        progress = query.lastProgress
        if progress:
            metrics = [
                f'# TYPE spark_streaming_{query_name}_input_rate gauge',
                f'spark_streaming_{query_name}_input_rate {progress["inputRowsPerSecond"]}',
                f'# TYPE spark_streaming_{query_name}_processing_rate gauge',
                f'spark_streaming_{query_name}_processing_rate {progress["processedRowsPerSecond"]}',
                f'# TYPE spark_streaming_{query_name}_batch_duration gauge',
                f'spark_streaming_{query_name}_batch_duration {progress["durationMs"]["triggerExecution"]}',
                f'# TYPE spark_streaming_{query_name}_num_input_rows gauge',
                f'spark_streaming_{query_name}_num_input_rows {progress["numInputRows"]}'
            ]
            metrics_data.extend(metrics)
        time.sleep(5)

def process_streams():
    print("Starting stream processing...")
    threading.Thread(target=start_metrics_server, daemon=True).start()
    spark = create_spark_session()
    job_schema, task_schema = define_schemas()
    kafka_bootstrap = "kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"

    job_events = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", "job-events") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    task_events = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", "task-events") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    print("Parsing job and task events...")
    parsed_jobs = job_events \
        .select(from_json(col("value"), job_schema).alias("data")) \
        .select(
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_timestamp"),
            "data.event_type",
            "data.scheduling_class",
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
    # Job events count by event type
    job_counts_by_event = parsed_jobs \
        .groupBy(
            "event_timestamp",
            "event_type"
        ) \
        .count() \
        .orderBy("event_timestamp")

    # Task distribution by event type 
    task_count_by_event = parsed_tasks \
        .groupBy(
            "event_timestamp",
            "event_type"
        ) \
        .count() \
        .orderBy("event_timestamp")
    
    # Job distribution by scheduling class
    job_counts_by_scheduling_class = parsed_jobs \
        .groupBy(
            "event_timestamp",
            "scheduling_class"
        ) \
        .count() \
        .orderBy("event_timestamp")
    
    # Task distribution by scheduling class
    task_counts_by_scheduling_class = parsed_tasks \
        .groupBy(
            "event_timestamp",
            "scheduling_class"
        ) \
        .count() \
        .orderBy("event_timestamp")
    
    print("Starting streaming queries...")
    queries = [
        job_counts_by_event.writeStream \
            .outputMode("complete") \
            .foreachBatch(lambda df, epochId: foreach_batch_function(df, epochId, "job_event_type")) \
            .trigger(processingTime="10 seconds") \
            .start(),

        task_count_by_event.writeStream \
            .outputMode("complete") \
            .foreachBatch(lambda df, epochId: foreach_batch_function(df, epochId, "task_event_type")) \
            .trigger(processingTime="10 seconds") \
            .start(),

        job_counts_by_scheduling_class.writeStream \
            .outputMode("complete") \
            .foreachBatch(lambda df, epochId: foreach_batch_function(df, epochId, "job_scheduling_class")) \
            .trigger(processingTime="10 seconds") \
            .start(),

        task_counts_by_scheduling_class.writeStream \
            .outputMode("complete") \
            .foreachBatch(lambda df, epochId: foreach_batch_function(df, epochId, "task_scheduling_class")) \
            .trigger(processingTime="10 seconds") \
            .start()
    ]

    monitors = [
        threading.Thread(target=monitor_query_progress, args=(queries[0], "job_events"), daemon=True),
        threading.Thread(target=monitor_query_progress, args=(queries[1], "task_events"), daemon=True)
    ]

    for monitor in monitors:
        monitor.start()

    print("Streaming queries started with the following analyses:")
    print("1. Job events count by type")
    print("2. Task distribution by scheduling class")
    print("\nAwaiting termination...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    print("Starting Spark streaming job...")
    process_streams()
