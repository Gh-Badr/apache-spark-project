from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as sql_max, corr, count, when
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
        .appName("GoogleClusterAnalysis_Q8") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def analyze_peaks_evictions(spark, results_handler):
    """Analyze correlation between resource consumption peaks and evictions"""
    print("Analyzing correlation between resource peaks and evictions...")

    reading_start = time.time()
    
    # Read files
    task_usage_df = spark.read.csv("gs://clusterdata-2011-2/task_usage/*.csv.gz")
    task_events_df = spark.read.csv("gs://clusterdata-2011-2/task_events/*.csv.gz")

    reading_end = time.time()

    total_records = task_usage_df.count() + task_events_df.count()

    processing_start = time.time()
    
    # Process task usage data (resource consumption)
    task_usage_peaks = task_usage_df.select(
        col("_c4").alias("machine_id"),
        col("_c5").cast("double").alias("cpu_rate"),
        col("_c6").cast("double").alias("memory_usage")
    ).filter(
        col("cpu_rate").isNotNull() & 
        col("memory_usage").isNotNull()
    ).groupBy("machine_id").agg(
        sql_max("cpu_rate").alias("peak_cpu"),
        sql_max("memory_usage").alias("peak_memory")
    )
    
    # Process task events data (evictions)
    evictions_count = task_events_df.select(
        col("_c4").alias("machine_id"),
        when(col("_c5") == "2", 1).otherwise(0).alias("is_evicted")
    ).filter(
        col("machine_id").isNotNull() & 
        (col("machine_id") != "")
    ).groupBy("machine_id").agg(
        count("is_evicted").alias("eviction_count")
    )
    
    # Join peaks with evictions
    joined_df = task_usage_peaks.join(evictions_count, "machine_id")
    
    # Calculate correlations
    correlations = joined_df.select(
        corr("peak_cpu", "eviction_count").alias("cpu_correlation"),
        corr("peak_memory", "eviction_count").alias("memory_correlation")
    ).collect()[0]
    
    # Convert to format for plotting
    results = joined_df.collect()
    peak_cpus = [float(r["peak_cpu"]) for r in results]
    peak_memories = [float(r["peak_memory"]) for r in results]
    eviction_counts = [int(r["eviction_count"]) for r in results]

    processing_end = time.time()
    
    # Create visualization
    plt.figure(figsize=(12, 5))
    
    # CPU peaks vs evictions
    plt.subplot(1, 2, 1)
    plt.scatter(peak_cpus, eviction_counts, alpha=0.5)
    plt.xlabel('Peak CPU Usage')
    plt.ylabel('Number of Eviction Events')
    plt.title('Peak CPU Usage vs Eviction Events')
    
    # Memory peaks vs evictions
    plt.subplot(1, 2, 2)
    plt.scatter(peak_memories, eviction_counts, alpha=0.5)
    plt.xlabel('Peak Memory Usage')
    plt.ylabel('Number of Eviction Events')
    plt.title('Peak Memory Usage vs Eviction Events')
    
    plt.tight_layout()
    
    # Save results
    timestamp = int(time.time())
    results_handler.save_plot(plt, f"q8_peaks_evictions_{timestamp}.png")
    
    # Save correlations
    results_handler.save_csv(
        data=[{
            "resource": "CPU peaks",
            "correlation": float(correlations["cpu_correlation"])
        }, {
            "resource": "Memory peaks",
            "correlation": float(correlations["memory_correlation"])
        }],
        columns=["resource", "correlation"],
        filename=f"q8_peaks_evictions_{timestamp}.csv"
    )

    # Save sample of raw data
    results_handler.save_csv(
        data=[{
            "machine_id": r["machine_id"],
            "peak_cpu": float(r["peak_cpu"]),
            "peak_memory": float(r["peak_memory"]),
            "eviction_count": int(r["eviction_count"])
        } for r in results[:20]],  # First 20 records
        columns=["machine_id", "peak_cpu", "peak_memory", "eviction_count"],
        filename=f"q8_peaks_evictions_sample_{timestamp}.csv"
    )

    # Save performance metrics
    results_handler.save_performance(
        data=[{"metric": "reading_time", "value": reading_end - reading_start},
              {"metric": "processing_time", "value": processing_end - processing_start},
              {"metric": "total_records", "value": total_records}],
        columns=["metric", "value"],
        filename=f"q8_performance_metrics_{timestamp}.csv"
    )
    
    # Print results
    print("\nCorrelation Results:")
    print(f"Peak CPU usage vs Eviction events: {correlations['cpu_correlation']:.3f}")
    print(f"Peak Memory usage vs Eviction events: {correlations['memory_correlation']:.3f}")

    # Print performance metrics
    print("\nPerformance Metrics:")
    print(f"Reading Time: {reading_end - reading_start:.2f} seconds")
    print(f"Processing Time: {processing_end - processing_start:.2f} seconds")
    print(f"Total Records: {total_records}")
    
    return correlations

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
        results = analyze_peaks_evictions(spark, results_handler)
        print(f"\nAnalysis complete. Results have been saved to {args.output_dir}")
        print("\nTo copy results to your local machine, use:")
        print(f"kubectl cp spark-master-0:{args.output_dir} ./local_results")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()