from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum, round
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
        .appName("GoogleClusterAnalysis_Q4") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def analyze_eviction_probability(spark, results_handler):
    """Analyze eviction probability by scheduling class"""
    print("Analyzing eviction probability by scheduling class...")

    reading_start = time.time()
    
    # Read task events files
    tasks_df = spark.read.csv("gs://clusterdata-2011-2/task_events/*.csv.gz")

    reading_end = time.time()

    total_records = tasks_df.count()

    processing_start = time.time()
    
    # Process tasks data
    tasks_df = tasks_df.select(
        col("_c7").cast("int").alias("scheduling_class"),
        col("_c5").cast("int").alias("event_type")
    ).filter(col("scheduling_class").isNotNull())
    
    # Count total tasks and evicted tasks per scheduling class
    probabilities_df = tasks_df.groupBy("scheduling_class").agg(
        count("*").alias("total_tasks"),
        sum(when(col("event_type") == 2, 1).otherwise(0)).alias("evicted_tasks")
    )
    
    # Calculate probability
    probabilities_df = probabilities_df.withColumn(
        "probability",
        round(col("evicted_tasks") / col("total_tasks"), 4)
    ).orderBy("scheduling_class")
    
    # Convert to format for plotting and saving
    results = [{
        "scheduling_class": row["scheduling_class"],
        "total_tasks": row["total_tasks"],
        "evicted_tasks": row["evicted_tasks"],
        "probability": float(row["probability"])
    } for row in probabilities_df.collect()]

    processing_end = time.time()
    
    # Create visualization
    plt.figure(figsize=(10, 6))
    scheduling_classes = [r["scheduling_class"] for r in results]
    probabilities = [r["probability"] for r in results]
    colors = ['red', 'blue', 'green', 'orange'][:len(scheduling_classes)]
    
    plt.bar(scheduling_classes, probabilities, color=colors)
    plt.xticks(scheduling_classes)
    plt.xlabel('Scheduling Class')
    plt.ylabel('Eviction Probability')
    plt.title('Eviction Probability by Scheduling Class')
    plt.tight_layout()
    
    # Save results
    timestamp = int(time.time())
    results_handler.save_plot(plt, f"q4_eviction_probability_{timestamp}.png")
    
    # Save probabilities
    results_handler.save_csv(
        data=results,
        columns=["scheduling_class", "total_tasks", "evicted_tasks", "probability"],
        filename=f"q4_eviction_probability_{timestamp}.csv"
    )

    # Save performance metrics
    results_handler.save_performance(
        data=[{"metric": "reading_time", "value": reading_end - reading_start},
              {"metric": "processing_time", "value": processing_end - processing_start},
              {"metric": "total_records", "value": total_records}],
        columns=["metric", "value"],
        filename=f"q4_performance_metrics_{timestamp}.csv"
    )
    
    # Print results
    print("\nEviction Probability by Scheduling Class:")
    print(f"{'Scheduling Class':<20}{'Total Tasks':<15}{'Evicted Tasks':<15}{'Probability':<15}")
    for result in results:
        print(f"{result['scheduling_class']:<20}"
              f"{result['total_tasks']:<15}"
              f"{result['evicted_tasks']:<15}"
              f"{result['probability']:.4f}")

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
        results = analyze_eviction_probability(spark, results_handler)
        print(f"\nAnalysis complete. Results have been saved to {args.output_dir}")
        print("\nTo copy results to your local machine, use:")
        print(f"kubectl cp spark-master-0:{args.output_dir} ./local_results")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()