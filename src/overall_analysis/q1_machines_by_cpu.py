from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
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
        .appName("GoogleClusterAnalysis_Q1") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def analyze_machine_distribution(spark, results_handler):
    """Analysis 1: Distribution of machines by CPU capacity"""
    print("Analyzing machine distribution...")

    reading_start = time.time()
    
    # Read all machine events files
    df = spark.read.csv("gs://clusterdata-2011-2/machine_events/*.csv.gz")

    reading_end = time.time()
    df_count = df.count()

    processing_start = time.time()

    # Get latest CPU capacity for each machine
    machine_dist = df.select(col("_c1").alias("machine_id"), 
                col("_c4").cast("double").alias("cpu_capacity")) \
        .filter(col("cpu_capacity").isNotNull())
    
    # Create capacity ranges
    ranges = [(0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.0)]
    distribution = []
    
    for low, high in ranges:
        count = machine_dist.filter(
            (col("cpu_capacity") > low) & 
            (col("cpu_capacity") <= high)
        ).count()
        
        distribution.append({
            "range": f"{int(low*100)}-{int(high*100)}%", 
            "count": count
        })

    processing_end = time.time()
    
    # Create visualization
    plt.figure(figsize=(10, 6))
    plt.bar([d["range"] for d in distribution], [d["count"] for d in distribution])
    plt.title("Distribution of Machines by CPU Capacity")
    plt.xlabel("CPU Capacity Range")
    plt.ylabel("Number of Machines")
    plt.xticks(rotation=45)
    plt.tight_layout()  # Added to prevent label cutoff
    
    # Save results
    timestamp = int(time.time())
    results_handler.save_plot(plt, f"q1_machine_distribution_{timestamp}.png")
    results_handler.save_csv(
        data=distribution,
        columns=["range", "count"],
        filename=f"q1_machine_distribution_{timestamp}.csv"
    )

    # Save performance metrics
    results_handler.save_performance(
        data=[{"metric": "reading_time", "value": reading_end - reading_start},
                {"metric": "processing_time", "value": processing_end - processing_start},
                {"metric": "total_records", "value": df_count}],
        columns=["metric", "value"],
        filename=f"q1_performance_metrics_{timestamp}.csv"
    )
    
    # Print results
    print("\nDistribution Results:")
    for d in distribution:
        print(f"Range {d['range']}: {d['count']} machines")

    # Print performance metrics
    print(f"\nReading time: {reading_end - reading_start:.2f} seconds")
    print(f"Processing time: {processing_end - processing_start:.2f} seconds")
    print(f"Total records: {df_count}")
    
    return distribution

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
        machine_dist_results = analyze_machine_distribution(spark, results_handler)
        print(f"\nAnalysis complete. Results have been saved to {args.output_dir}")
        print("\nTo copy results to your local machine, use:")
        print(f"kubectl cp spark-master-0:{args.output_dir} ./local_results")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()