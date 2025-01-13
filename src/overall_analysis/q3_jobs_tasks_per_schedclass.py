from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct
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
        .appName("GoogleClusterAnalysis_Q3") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def analyze_scheduling_distribution(spark, results_handler):
    """Analyze distribution of jobs/tasks per scheduling class"""
    print("Analyzing scheduling class distribution...")

    reading_start = time.time()
    
    # Read job events files
    jobs_df = spark.read.csv("gs://clusterdata-2011-2/job_events/*.csv.gz")
    
    # Read task events files
    tasks_df = spark.read.csv("gs://clusterdata-2011-2/task_events/*.csv.gz")

    reading_end = time.time()

    tasks_records = tasks_df.count()
    jobs_records = jobs_df.count()

    processing_start = time.time()
    
    # Process jobs distribution
    jobs_df = jobs_df.select(
        col("_c2").alias("job_id"),
        col("_c5").cast("int").alias("scheduling_class")
    ).filter(col("scheduling_class").isNotNull())
    
    # Process tasks distribution
    tasks_df = tasks_df.select(
        col("_c2").alias("job_id"),
        col("_c7").cast("int").alias("scheduling_class")
    ).filter(col("scheduling_class").isNotNull())
    
    # Count unique jobs per scheduling class
    jobs_distribution = jobs_df.groupBy("scheduling_class") \
        .agg(countDistinct("job_id").alias("count")) \
        .orderBy("scheduling_class")
    
    # Count unique tasks per scheduling class
    tasks_distribution = tasks_df.groupBy("scheduling_class") \
        .agg(countDistinct("job_id").alias("count")) \
        .orderBy("scheduling_class")
    
    # Convert to format for plotting and saving
    jobs_results = [{"scheduling_class": row["scheduling_class"], "count": row["count"]} 
                   for row in jobs_distribution.collect()]
    tasks_results = [{"scheduling_class": row["scheduling_class"], "count": row["count"]} 
                    for row in tasks_distribution.collect()]

    processing_end = time.time()
    
    # Create visualization
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(16, 6))
    
    # Plot jobs distribution
    jobs_classes = [d["scheduling_class"] for d in jobs_results]
    jobs_counts = [d["count"] for d in jobs_results]
    axes[0].bar(jobs_classes, jobs_counts, color='skyblue')
    axes[0].set_title("Distribution of Jobs per Scheduling Class")
    axes[0].set_xlabel("Scheduling Class")
    axes[0].set_ylabel("Number of Jobs")
    axes[0].set_xticks(jobs_classes)
    
    # Plot tasks distribution
    tasks_classes = [d["scheduling_class"] for d in tasks_results]
    tasks_counts = [d["count"] for d in tasks_results]
    axes[1].bar(tasks_classes, tasks_counts, color='lightgreen')
    axes[1].set_title("Distribution of Tasks per Scheduling Class")
    axes[1].set_xlabel("Scheduling Class")
    axes[1].set_ylabel("Number of Tasks")
    axes[1].set_xticks(tasks_classes)
    
    plt.tight_layout()
    
    # Save results
    timestamp = int(time.time())
    results_handler.save_plot(plt, f"q3_scheduling_distribution_{timestamp}.png")
    
    # Save jobs distribution
    results_handler.save_csv(
        data=jobs_results,
        columns=["scheduling_class", "count"],
        filename=f"q3_jobs_distribution_{timestamp}.csv"
    )
    
    # Save tasks distribution
    results_handler.save_csv(
        data=tasks_results,
        columns=["scheduling_class", "count"],
        filename=f"q3_tasks_distribution_{timestamp}.csv"
    )

    # save performance metrics
    results_handler.save_performance(
        data=[{"metric": "reading_time", "value": reading_end - reading_start},
              {"metric": "processing_time", "value": processing_end - processing_start},
              {"metric": "jobs_records", "value": jobs_records},
              {"metric": "tasks_records", "value": tasks_records},
              {"metric": "total_records", "value": jobs_records + tasks_records}],
        columns=["metric", "value"],
        filename=f"q3_performance_metrics_{timestamp}.csv"
    )
    
    # Print results
    print("\nJobs Distribution:")
    print(f"{'Scheduling Class':<20}{'Number of Jobs':<20}")
    for result in jobs_results:
        print(f"{result['scheduling_class']:<20}{result['count']:<20}")
    
    print("\nTasks Distribution:")
    print(f"{'Scheduling Class':<20}{'Number of Tasks':<20}")
    for result in tasks_results:
        print(f"{result['scheduling_class']:<20}{result['count']:<20}")

    # Print performance metrics
    print("\nPerformance Metrics:")
    print(f"Reading Time: {reading_end - reading_start:.2f} seconds")
    print(f"Processing Time: {processing_end - processing_start:.2f} seconds")
    print(f"Total Jobs Records: {jobs_records}")
    print(f"Total Tasks Records: {tasks_records}")
    print(f"Total Records: {jobs_records + tasks_records}")
    
    return {"jobs": jobs_results, "tasks": tasks_results}

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
        results = analyze_scheduling_distribution(spark, results_handler)
        print(f"\nAnalysis complete. Results have been saved to {args.output_dir}")
        print("\nTo copy results to your local machine, use:")
        print(f"kubectl cp spark-master-0:{args.output_dir} ./local_results")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()