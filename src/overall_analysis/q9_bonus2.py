from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, expr
import matplotlib.pyplot as plt
import argparse
import os
import csv
import time

class LocalResultsHandler:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        # Create output directories
        os.makedirs(os.path.join(output_dir, "plots"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "data"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "performance"), exist_ok=True)

    def save_plot(self, plt, filename):
        """Save matplotlib plot locally"""
        filepath = os.path.join(self.output_dir, "plots", filename)
        plt.savefig(filepath)
        print(f"Plot saved to {filepath}")
    
    def save_csv(self, data, columns, filename):
        """Save data as CSV locally"""
        filepath = os.path.join(self.output_dir, "data", filename)
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        print(f"Data saved to {filepath}")

    def save_performance(self, data, columns, filename):
        """Save performance metrics as CSV locally"""
        filepath = os.path.join(self.output_dir, "performance", filename)
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        print(f"Performance metrics saved to {filepath}")

def create_spark_session():
    return SparkSession.builder \
        .appName("GoogleClusterAnalysis_Q9_Bonus2") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def analyze_hourly_resources(spark, results_handler):
    """Analyze hourly patterns in resource consumption"""
    print("Analyzing hourly resource consumption patterns...")

    reading_start = time.time()
    
    # Read files
    task_usage_df = spark.read.csv("gs://clusterdata-2011-2/task_usage/*.csv.gz")

    reading_end = time.time()

    total_records = task_usage_df.count()

    processing_start = time.time()
    
    # Process task usage data with hour extraction
    hourly_data = task_usage_df.select(
        (expr("cast(_c0 as bigint) % 86400 / 3600").cast("int")).alias("hour"),
        col("_c5").cast("double").alias("cpu_usage"),
        col("_c6").cast("double").alias("memory_usage")
    ).filter(
        col("cpu_usage").isNotNull() & 
        col("memory_usage").isNotNull()
    )
    
    # Calculate hourly averages
    hourly_averages = hourly_data.groupBy("hour").agg(
        avg("cpu_usage").alias("avg_cpu"),
        avg("memory_usage").alias("avg_memory")
    ).orderBy("hour")
    
    # Convert to format for plotting
    results = hourly_averages.collect()
    hours = [int(r["hour"]) for r in results]
    avg_cpu = [float(r["avg_cpu"]) for r in results]
    avg_memory = [float(r["avg_memory"]) for r in results]

    processing_end = time.time()
    
    # Create visualization
    plt.figure(figsize=(12, 5))
    
    # CPU Usage plot
    plt.subplot(1, 2, 1)
    plt.plot(hours, avg_cpu, marker='o')
    plt.title('Average CPU Usage by Hour')
    plt.xlabel('Hour of Day')
    plt.ylabel('Average CPU Usage')
    plt.grid(True)
    
    # Memory Usage plot
    plt.subplot(1, 2, 2)
    plt.plot(hours, avg_memory, marker='o', color='orange')
    plt.title('Average Memory Usage by Hour')
    plt.xlabel('Hour of Day')
    plt.ylabel('Average Memory Usage')
    plt.grid(True)
    
    plt.tight_layout()
    
    # Save results
    timestamp = int(time.time())
    results_handler.save_plot(plt, f"q9_bonus2_hourly_resources_{timestamp}.png")
    
    # Save hourly averages
    results_handler.save_csv(
        data=[{
            "hour": hour,
            "avg_cpu": cpu,
            "avg_memory": mem
        } for hour, cpu, mem in zip(hours, avg_cpu, avg_memory)],
        columns=["hour", "avg_cpu", "avg_memory"],
        filename=f"q9_bonus2_hourly_resources_{timestamp}.csv"
    )

    # Save performance metrics
    results_handler.save_performance(
        data=[{"metric": "reading_time", "value": reading_end - reading_start},
              {"metric": "processing_time", "value": processing_end - processing_start},
              {"metric": "total_records", "value": total_records}],
        columns=["metric", "value"],
        filename=f"q9_bonus2_performance_metrics_{timestamp}.csv"
    )
    
    # Print results
    print("\nHourly Resource Usage Summary:")
    print(f"{'Hour':<6}{'Avg CPU':<15}{'Avg Memory':<15}")
    for hour, cpu, mem in zip(hours, avg_cpu, avg_memory):
        print(f"{hour:<6}{cpu:<15.3f}{mem:<15.3f}")

    # Print performance metrics
    print("\nPerformance Metrics:")
    print(f"Reading Time: {reading_end - reading_start:.2f} seconds")
    print(f"Processing Time: {processing_end - processing_start:.2f} seconds")
    print(f"Total Records: {total_records}")
    
    return results

def parse_arguments():
    parser = argparse.ArgumentParser(description='Google Cluster Analysis')
    parser.add_argument('--output-dir', 
                      default='/opt/bitnami/spark/results',
                      help='Output directory for results')
    return parser.parse_args()

def main():
    args = parse_arguments()
    results_handler = LocalResultsHandler(args.output_dir)
    spark = create_spark_session()
    
    try:
        results = analyze_hourly_resources(spark, results_handler)
        print(f"\nAnalysis complete. Results have been saved to {args.output_dir}")
        print("\nTo copy results to your local machine, use:")
        print(f"kubectl cp spark-master-0:{args.output_dir} ./local_results")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()