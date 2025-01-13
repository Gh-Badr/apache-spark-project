from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum, corr, avg, round
from pyspark.sql.window import Window
import pyspark.sql.functions as F
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
        .appName("GoogleClusterAnalysis_Q5_Bonus1") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def analyze_capacity_evictions(spark, results_handler):
    """Analyze relationship between machine capacity and evictions"""
    print("Analyzing relationship between machine capacity and evictions...")

    reading_start = time.time()
    
    # Read data files
    machines_df = spark.read.csv("gs://clusterdata-2011-2/machine_events/*.csv.gz")
    tasks_df = spark.read.csv("gs://clusterdata-2011-2/task_events/*.csv.gz")

    reading_end = time.time()
    
    total_records = machines_df.count() + tasks_df.count()

    processing_start = time.time()
    
    # Process machine capacities
    machines_df = machines_df.select(
        col("_c1").alias("machine_id"),
        col("_c4").cast("double").alias("cpu_capacity"),
        col("_c5").cast("double").alias("memory_capacity")
    ).na.fill(0.0)
    
    # Get latest capacity values for each machine
    window_spec = Window.partitionBy("machine_id").orderBy(F.lit('A'))
    machines_df = machines_df.withColumn(
        "rn", F.row_number().over(window_spec)
    ).filter(F.col("rn") == 1).drop("rn")
    
    # Count evictions per machine
    evictions_df = tasks_df.select(
        col("_c4").alias("machine_id"),
        when(col("_c5") == "2", 1).otherwise(0).alias("is_evicted")
    ).filter(col("machine_id") != "")
    
    evictions_per_machine = evictions_df.groupBy("machine_id").agg(
        sum("is_evicted").alias("eviction_count")
    )
    
    # Join capacities with evictions
    joined_df = machines_df.join(evictions_per_machine, "machine_id")
    
    # Calculate correlations
    correlations = joined_df.select(
        corr("cpu_capacity", "eviction_count").alias("cpu_correlation"),
        corr("memory_capacity", "eviction_count").alias("memory_correlation")
    ).collect()[0]
    
    # Convert to lists for plotting
    results = joined_df.collect()
    cpu_capacities = [float(r["cpu_capacity"]) for r in results]
    mem_capacities = [float(r["memory_capacity"]) for r in results]
    eviction_counts = [int(r["eviction_count"]) for r in results]

    # Calculate average evictions by capacity range
    def get_range_column(col_name):
        return when(col(col_name) < 0.25, 0) \
               .when(col(col_name) < 0.5, 1) \
               .when(col(col_name) < 0.75, 2) \
               .otherwise(3) \
               .alias(f"{col_name}_range")

    cpu_ranges_df = joined_df \
        .withColumn("range", get_range_column("cpu_capacity")) \
        .groupBy("range") \
        .agg(avg("eviction_count").alias("avg_evictions")) \
        .orderBy("range")

    mem_ranges_df = joined_df \
        .withColumn("range", get_range_column("memory_capacity")) \
        .groupBy("range") \
        .agg(avg("eviction_count").alias("avg_evictions")) \
        .orderBy("range")

    cpu_range_results = [{
        "range": r["range"],
        "avg_evictions": float(r["avg_evictions"])
    } for r in cpu_ranges_df.collect()]

    mem_range_results = [{
        "range": r["range"],
        "avg_evictions": float(r["avg_evictions"])
    } for r in mem_ranges_df.collect()]

    processing_end = time.time()
    
    # Create scatter plots
    plt.figure(figsize=(12, 5))
    
    plt.subplot(1, 2, 1)
    plt.scatter(cpu_capacities, eviction_counts, alpha=0.5)
    plt.xlabel('CPU Capacity (normalized)')
    plt.ylabel('Number of Evictions')
    plt.title('CPU Capacity vs Eviction Frequency')
    
    plt.subplot(1, 2, 2)
    plt.scatter(mem_capacities, eviction_counts, alpha=0.5)
    plt.xlabel('Memory Capacity (normalized)')
    plt.ylabel('Number of Evictions')
    plt.title('Memory Capacity vs Eviction Frequency')
    
    plt.tight_layout()
    
    # Save scatter plots
    timestamp = int(time.time())
    results_handler.save_plot(plt, f"q5_bonus1_capacity_evictions_scatter_{timestamp}.png")
    
    # Create bar plots for average evictions
    plt.figure(figsize=(12, 5))
    
    plt.subplot(1, 2, 1)
    plt.bar([r["range"] for r in cpu_range_results], 
            [r["avg_evictions"] for r in cpu_range_results])
    plt.xlabel('CPU Capacity Range\n(0: 0-0.25, 1: 0.25-0.5, 2: 0.5-0.75, 3: 0.75-1.0)')
    plt.ylabel('Average Evictions')
    plt.title('Average Evictions by CPU Capacity Range')
    
    plt.subplot(1, 2, 2)
    plt.bar([r["range"] for r in mem_range_results], 
            [r["avg_evictions"] for r in mem_range_results])
    plt.xlabel('Memory Capacity Range\n(0: 0-0.25, 1: 0.25-0.5, 2: 0.5-0.75, 3: 0.75-1.0)')
    plt.ylabel('Average Evictions')
    plt.title('Average Evictions by Memory Capacity Range')
    
    plt.tight_layout()
    
    # Save bar plots
    results_handler.save_plot(plt, f"q5_bonus1_capacity_evictions_bars_{timestamp}.png")
    
    # Save detailed results
    results_handler.save_csv(
        data=[{
            "machine_id": r["machine_id"],
            "cpu_capacity": float(r["cpu_capacity"]),
            "memory_capacity": float(r["memory_capacity"]),
            "eviction_count": int(r["eviction_count"])
        } for r in results[:20]],  # Save first 20 records
        columns=["machine_id", "cpu_capacity", "memory_capacity", "eviction_count"],
        filename=f"q5_bonus1_detailed_results_{timestamp}.csv"
    )
    
    # Save correlations
    results_handler.save_csv(
        data=[{
            "metric": "CPU Correlation",
            "value": float(correlations["cpu_correlation"])
        }, {
            "metric": "Memory Correlation",
            "value": float(correlations["memory_correlation"])
        }],
        columns=["metric", "value"],
        filename=f"q5_bonus1_correlations_{timestamp}.csv"
    )
    
    # Save average evictions by range
    results_handler.save_csv(
        data=cpu_range_results,
        columns=["range", "avg_evictions"],
        filename=f"q5_bonus1_cpu_range_averages_{timestamp}.csv"
    )
    results_handler.save_csv(
        data=mem_range_results,
        columns=["range", "avg_evictions"],
        filename=f"q5_bonus1_memory_range_averages_{timestamp}.csv"
    )

    # Save performance metrics
    results_handler.save_performance(
        data=[{"metric": "reading_time", "value": reading_end - reading_start},
              {"metric": "processing_time", "value": processing_end - processing_start},
              {"metric": "total_records", "value": total_records}],
        columns=["metric", "value"],
        filename=f"q5_bonus1_performance_metrics_{timestamp}.csv"
    )
    
    # Print results
    print("\nResults Sample (first 20 machines):")
    print(f"{'Machine ID':<15} {'CPU Capacity':<15} {'Memory Capacity':<15} {'Evictions':<15}")
    for r in results[:20]:
        print(f"{r['machine_id']:<15} {r['cpu_capacity']:<15.2f} "
              f"{r['memory_capacity']:<15.2f} {r['eviction_count']:<15}")
    
    print("\nCorrelation Results:")
    print(f"CPU Correlation: {correlations['cpu_correlation']:.3f}")
    print(f"Memory Correlation: {correlations['memory_correlation']:.3f}")
    
    # Print performance metrics
    print("\nPerformance Metrics:")
    print(f"Reading Time: {reading_end - reading_start:.2f} seconds")
    print(f"Processing Time: {processing_end - processing_start:.2f} seconds")
    print(f"Total Records: {total_records}")
    
    return {
        "correlations": correlations,
        "cpu_ranges": cpu_range_results,
        "memory_ranges": mem_range_results
    }

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
        results = analyze_capacity_evictions(spark, results_handler)
        print(f"\nAnalysis complete. Results have been saved to {args.output_dir}")
        print("\nTo copy results to your local machine, use:")
        print(f"kubectl cp spark-master-0:{args.output_dir} ./local_results")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()