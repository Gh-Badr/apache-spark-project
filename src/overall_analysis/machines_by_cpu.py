from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
import argparse
import time

class GCSResultsHandler:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
    
    def save_dataframe(self, df, filename):
        """Save Spark DataFrame to GCS as CSV"""
        output_path = f"gs://{self.bucket_name}/data/{filename}"
        # Use these options to make the write operation more robust
        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .option("fs.gs.outputstream.upload.chunk.size", 1024*1024) \
            .option("fs.gs.outputstream.upload.cache.size", 0) \
            .option("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
            .format("csv") \
            .save(output_path)
        print(f"Data saved to {output_path}")

def create_spark_session():
    return SparkSession.builder \
        .appName("GoogleClusterAnalysis") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()

def analyze_machine_distribution(spark, results_handler):
    """Analysis 1: Distribution of machines by CPU capacity"""
    print("Analyzing machine distribution...")
    
    # Read all machine events files
    df = spark.read.csv("gs://clusterdata-2011-2/machine_events/*.csv.gz")
    
    # Filter ADD events and get latest CPU capacity for each machine
    machine_dist = df.filter(col("_c2") == "0") \
        .select(col("_c1").alias("machine_id"), 
                col("_c4").cast("double").alias("cpu_capacity")) \
        .filter(col("cpu_capacity").isNotNull())
    
    # Create ranges using Spark SQL functions
    distribution = []
    ranges = [(0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.0)]
    
    # Create one DataFrame for range statistics
    for low, high in ranges:
        range_name = f"{int(low*100)}-{int(high*100)}%"
        count_value = machine_dist.filter(
            (col("cpu_capacity") >= low) & (col("cpu_capacity") < high)
        ).count()
        
        # Create a DataFrame for this range
        range_df = spark.createDataFrame(
            [(range_name, count_value)],
            ["range", "count"]
        )
        distribution.append(range_df)
    
    # Union all range DataFrames
    result_df = distribution[0]
    for df in distribution[1:]:
        result_df = result_df.union(df)
    
    # Save results
    results_handler.save_dataframe(result_df, f"machine_distribution_{int(time.time())}")
    
    # Print results
    print("\nResults:")
    result_df.show(truncate=False)
    
    return result_df

def parse_arguments():
    parser = argparse.ArgumentParser(description='Google Cluster Analysis')
    parser.add_argument('--bucket', 
                      required=True,
                      help='GCS bucket name for storing results')
    return parser.parse_args()

def main():
    args = parse_arguments()
    results_handler = GCSResultsHandler(args.bucket)
    spark = create_spark_session()
    
    try:
        machine_dist_results = analyze_machine_distribution(spark, results_handler)
        print("\nAnalysis complete. Results have been saved to GCS.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()