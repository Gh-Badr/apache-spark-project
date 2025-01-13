from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum, min as spark_min, max as spark_max
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
        .appName("GoogleClusterAnalysis_Q2") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def analyze_computational_power_loss(spark, results_handler):
    """Analyze computational power lost due to maintenance"""
    print("Analyzing computational power lost due to maintenance...")

    reading_start = time.time()
    
    # Read all machine events files
    df = spark.read.csv("gs://clusterdata-2011-2/machine_events/*.csv.gz")

    reading_end = time.time()
    df_count = df.count()

    processing_start = time.time()
    
    # Convert columns to appropriate types
    df = df.select(
        col("_c0").cast("long").alias("timestamp"),
        col("_c1").cast("long").alias("machine_id"),
        col("_c2").cast("int").alias("event_type"),
        col("_c4").cast("double").alias("cpu_capacity")
    ).na.fill(0, ["cpu_capacity"])  # Fill missing CPU values with 0
    
    # Create a window spec for each machine
    window_spec = Window.partitionBy("machine_id").orderBy("timestamp")
    
    # Add lag information to identify offline periods
    df_with_lag = df.withColumn("prev_event_type", F.lag("event_type").over(window_spec))
    df_with_lag = df_with_lag.withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))
    df_with_lag = df_with_lag.withColumn("prev_cpu_capacity", F.lag("cpu_capacity").over(window_spec))
    
    # Calculate offline periods and power loss
    offline_periods = df_with_lag.filter(
        (col("event_type") == 0) &  # Current event is ADD
        (col("prev_event_type") == 1)  # Previous event was REMOVE
    ).select(
        "machine_id",
        (col("timestamp") - col("prev_timestamp")).alias("offline_time"),
        ((col("timestamp") - col("prev_timestamp")) * col("prev_cpu_capacity")).alias("lost_power")
    )
    
    # Calculate total lost power
    total_lost_power = offline_periods.agg(sum("lost_power")).collect()[0][0]
    
    # Calculate total available power
    time_range = df.agg(
        spark_min("timestamp").alias("min_time"),
        spark_max("timestamp").alias("max_time")
    ).collect()[0]
    
    total_cpu_capacity = df.filter(col("event_type") == 0).agg(sum("cpu_capacity")).collect()[0][0]
    total_time = time_range['max_time'] - time_range['min_time']
    total_available_power = total_time * total_cpu_capacity
    
    # Calculate percentage
    percentage_lost = (total_lost_power / total_available_power) * 100 if total_available_power > 0 else 0
    
    # Prepare results
    results = {
        "total_lost_power": total_lost_power,
        "total_available_power": total_available_power,
        "percentage_lost": percentage_lost
    }

    processing_end = time.time()
    
    # Create visualization
    plt.figure(figsize=(10, 10))
    plt.pie([total_lost_power, total_available_power - total_lost_power],
            labels=['Lost Power Due to Maintenance', 'Available Power'],
            colors=['red', 'green'],
            autopct='%1.2f%%',
            explode=(0.1, 0),
            startangle=140)
    plt.title("Proportion of Computational Power Lost Due to Maintenance")
    
    # Save results
    timestamp = int(time.time())
    results_handler.save_plot(plt, f"q2_power_loss_{timestamp}.png")
    results_handler.save_csv(
        data=[results],
        columns=["total_lost_power", "total_available_power", "percentage_lost"],
        filename=f"q2_power_loss_{timestamp}.csv"
    )

    # Save performance metrics
    results_handler.save_performance(
        data=[{"metric": "reading_time", "value": reading_end - reading_start},
              {"metric": "processing_time", "value": processing_end - processing_start},
              {"metric": "total_records", "value": df_count}],
        columns=["metric", "value"],
        filename=f"q2_performance_metrics_{timestamp}.csv"
    )
    
    # Print results
    print(f"\nResults:")
    print(f"Total lost computational power: {total_lost_power:.2f}")
    print(f"Total available computational power: {total_available_power:.2f}")
    print(f"Percentage of computational power lost due to maintenance: {percentage_lost:.2f}%")

    # Print performance metrics
    print(f"\nReading time: {reading_end - reading_start:.2f} seconds")
    print(f"Processing time: {processing_end - processing_start:.2f} seconds")
    print(f"Total records: {df_count}")
    
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
        results = analyze_computational_power_loss(spark, results_handler)
        print(f"\nAnalysis complete. Results have been saved to {args.output_dir}")
        print("\nTo copy results to your local machine, use:")
        print(f"kubectl cp spark-master-0:{args.output_dir} ./local_results")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()