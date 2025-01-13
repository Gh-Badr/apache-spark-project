from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when
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
        .appName("GoogleClusterAnalysis_Q6") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def analyze_job_machine_distribution(spark, results_handler):
    """Analyze if tasks from the same job run on the same machine"""
    print("Analyzing job-machine distribution...")

    reading_start = time.time()
    
    # Read task events files
    tasks_df = spark.read.csv("gs://clusterdata-2011-2/task_events/*.csv.gz")

    reading_end = time.time()

    total_records = tasks_df.count()

    processing_start = time.time()
    
    # Extract relevant columns and remove empty machine_ids
    tasks_df = tasks_df.select(
        col("_c2").alias("job_id"),
        col("_c3").alias("task_index"),
        col("_c4").alias("machine_id")
    ).filter(col("machine_id") != "")

    # Count distinct machines per job
    machine_counts = tasks_df.groupBy("job_id") \
        .agg(countDistinct("machine_id").alias("machine_count"))
    
    # Count jobs with single machine vs multiple machines
    same_machine_count = machine_counts.filter(col("machine_count") == 1).count()
    total_jobs = machine_counts.count()
    different_machine_count = total_jobs - same_machine_count

    processing_end = time.time()
    
    # Prepare results
    results = {
        "same_machine_count": same_machine_count,
        "different_machine_count": different_machine_count,
        "total_jobs": total_jobs
    }
    
    # Create pie chart
    plt.figure(figsize=(8, 8))
    plt.pie([same_machine_count, different_machine_count],
            explode=(0.1, 0),
            labels=['Same Machine', 'Different Machines'],
            colors=['#ff9999', '#66b3ff'],
            autopct='%1.1f%%',
            shadow=True,
            startangle=140)
    plt.title('Distribution of Jobs with Tasks Running on\nSame Machine vs Different Machines')
    plt.axis('equal')
    
    # Save results
    timestamp = int(time.time())
    results_handler.save_plot(plt, f"q6_job_machine_distribution_{timestamp}.png")
    
    results_handler.save_csv(
        data=[{
            "category": "Same Machine",
            "count": same_machine_count,
            "percentage": (same_machine_count/total_jobs)*100
        }, {
            "category": "Different Machines",
            "count": different_machine_count,
            "percentage": (different_machine_count/total_jobs)*100
        }],
        columns=["category", "count", "percentage"],
        filename=f"q6_job_machine_distribution_{timestamp}.csv"
    )

    # Save performance metrics
    results_handler.save_performance(
        data=[{"metric": "reading_time", "value": reading_end - reading_start},
              {"metric": "processing_time", "value": processing_end - processing_start},
              {"metric": "total_records", "value": total_records}],
        columns=["metric", "value"],
        filename=f"q6_performance_metrics_{timestamp}.csv"
    )
    
    # Print results
    print("\nJob-Machine Distribution Results:")
    print(f"Jobs with tasks running on the same machine: {same_machine_count}")
    print(f"Jobs with tasks running on different machines: {different_machine_count}")
    print(f"Total jobs: {total_jobs}")
    print(f"Percentage on same machine: {(same_machine_count/total_jobs)*100:.1f}%")
    print(f"Percentage on different machines: {(different_machine_count/total_jobs)*100:.1f}%")

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
        results = analyze_job_machine_distribution(spark, results_handler)
        print(f"\nAnalysis complete. Results have been saved to {args.output_dir}")
        print("\nTo copy results to your local machine, use:")
        print(f"kubectl cp spark-master-0:{args.output_dir} ./local_results")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()