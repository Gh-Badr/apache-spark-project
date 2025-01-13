from pyspark.sql import SparkSession
from pyspark.sql.functions import col, corr, count
import matplotlib.pyplot as plt
import argparse
import os
import csv
import time

class LocalResultsHandler:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        os.makedirs(os.path.join(output_dir, "plots"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "data"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "performance"), exist_ok=True)

    def save_plot(self, plt, filename):
        filepath = os.path.join(self.output_dir, "plots", filename)
        plt.savefig(filepath)
        print(f"Plot saved to {filepath}")
    
    def save_csv(self, data, columns, filename):
        filepath = os.path.join(self.output_dir, "data", filename)
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        print(f"Data saved to {filepath}")

    def save_performance(self, data, columns, filename):
        filepath = os.path.join(self.output_dir, "performance", filename)
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        print(f"Performance metrics saved to {filepath}")

def create_spark_session():
    return SparkSession.builder \
        .appName("GoogleClusterAnalysis_Q7") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def analyze_resource_usage(spark, results_handler):
    """Analyze relationship between requested and consumed resources"""
    print("Analyzing resource request vs consumption correlation...")

    reading_start = time.time()
    
    # Read files
    task_events_df = spark.read.csv("gs://clusterdata-2011-2/task_events/*.csv.gz")
    task_usage_df = spark.read.csv("gs://clusterdata-2011-2/task_usage/*.csv.gz")

    reading_end = time.time()

    total_records = task_events_df.count() + task_usage_df.count()

    processing_start = time.time()
    
    # Process task events data (requests)
    task_events_df = task_events_df.select(
        col("_c2").alias("job_id"),
        col("_c3").alias("task_index"),
        col("_c9").cast("double").alias("cpu_request"),
        col("_c10").cast("double").alias("memory_request")
    ).filter(
        col("cpu_request").isNotNull() & 
        col("memory_request").isNotNull()
    )
    
    # Process task usage data (consumption)
    task_usage_df = task_usage_df.select(
        col("_c2").alias("job_id"),
        col("_c3").alias("task_index"),
        col("_c5").cast("double").alias("cpu_rate"),
        col("_c6").cast("double").alias("memory_usage")
    ).filter(
        col("cpu_rate").isNotNull() & 
        col("memory_usage").isNotNull()
    )
    
    # Join the dataframes
    joined_df = task_events_df.join(
        task_usage_df,
        ["job_id", "task_index"]
    )
    
    # Calculate correlations on full dataset
    correlations = joined_df.select(
        corr("cpu_request", "cpu_rate").alias("cpu_correlation"),
        corr("memory_request", "memory_usage").alias("memory_correlation")
    ).collect()[0]
    
    # Take a sample for visualization
    sample_size = 10000  # Adjust based on your needs
    sample_df = joined_df.sample(False, fraction=0.01, seed=42).limit(sample_size)
    
    # Convert sample to lists for plotting
    sample_data = sample_df.collect()
    cpu_requests = [float(r["cpu_request"]) for r in sample_data]
    cpu_rates = [float(r["cpu_rate"]) for r in sample_data]
    memory_requests = [float(r["memory_request"]) for r in sample_data]
    memory_usages = [float(r["memory_usage"]) for r in sample_data]

    processing_end = time.time()
    
    # Create scatter plots
    plt.figure(figsize=(12, 5))
    
    # CPU plot
    plt.subplot(1, 2, 1)
    plt.scatter(cpu_requests, cpu_rates, alpha=0.5)
    plt.xlabel('CPU Request')
    plt.ylabel('CPU Rate')
    plt.title('CPU Request vs CPU Rate\n(Sample of Data)')
    
    # Memory plot
    plt.subplot(1, 2, 2)
    plt.scatter(memory_requests, memory_usages, alpha=0.5)
    plt.xlabel('Memory Request')
    plt.ylabel('Memory Usage')
    plt.title('Memory Request vs Memory Usage\n(Sample of Data)')
    
    plt.tight_layout()
    
    # Save results
    timestamp = int(time.time())
    results_handler.save_plot(plt, f"q7_resource_correlation_{timestamp}.png")
    
    # Save correlations
    results_handler.save_csv(
        data=[{
            "resource": "CPU",
            "correlation": float(correlations["cpu_correlation"])
        }, {
            "resource": "Memory",
            "correlation": float(correlations["memory_correlation"])
        }],
        columns=["resource", "correlation"],
        filename=f"q7_resource_correlation_{timestamp}.csv"
    )

    # Save performance metrics
    results_handler.save_performance(
        data=[{"metric": "reading_time", "value": reading_end - reading_start},
              {"metric": "processing_time", "value": processing_end - processing_start},
              {"metric": "total_records", "value": total_records}],
        columns=["metric", "value"],
        filename=f"q7_performance_metrics_{timestamp}.csv"
    )
    
    # Print results
    print("\nCorrelation Results (computed on full dataset):")
    print(f"CPU Request vs Rate correlation: {correlations['cpu_correlation']:.3f}")
    print(f"Memory Request vs Usage correlation: {correlations['memory_correlation']:.3f}")

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
        results = analyze_resource_usage(spark, results_handler)
        print(f"\nAnalysis complete. Results have been saved to {args.output_dir}")
        print("\nTo copy results to your local machine, use:")
        print(f"kubectl cp spark-master-0:{args.output_dir} ./local_results")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()