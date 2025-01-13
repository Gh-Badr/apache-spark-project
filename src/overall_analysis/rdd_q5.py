from pyspark import SparkContext, SparkConf
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

def create_spark_context():
    conf = SparkConf().setAppName("GoogleClusterAnalysis_Q5_Bonus1_RDD") \
        .set("spark.driver.memory", "1g") \
        .set("spark.executor.memory", "1g") \
        .set("spark.executor.cores", "1")
    return SparkContext.getOrCreate(conf=conf)

def safe_float(value):
    try:
        return float(value) if value.strip() != '' else 0.0
    except (ValueError, AttributeError):
        return 0.0

def calculate_correlation(pairs):
    n = len(pairs)
    if n == 0:
        return 0.0
        
    sum_x = sum(p[0] for p in pairs)
    sum_y = sum(p[1] for p in pairs)
    sum_xy = sum(p[0] * p[1] for p in pairs)
    sum_x2 = sum(p[0] * p[0] for p in pairs)
    sum_y2 = sum(p[1] * p[1] for p in pairs)
    
    numerator = n * sum_xy - sum_x * sum_y
    denominator = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)) ** 0.5
    
    return numerator / denominator if denominator != 0 else 0.0

def get_capacity_range(capacity):
    if capacity < 0.25: return 0
    elif capacity < 0.5: return 1
    elif capacity < 0.75: return 2
    else: return 3

def analyze_capacity_evictions(sc, results_handler):
    """Analyze relationship between machine capacity and evictions"""
    print("Analyzing relationship between machine capacity and evictions...")

    reading_start = time.time()
    
    # Read files
    machines_rdd = sc.textFile("gs://clusterdata-2011-2/machine_events/*.csv.gz")
    tasks_rdd = sc.textFile("gs://clusterdata-2011-2/task_events/*.csv.gz")

    reading_end = time.time()
    
    total_records = machines_rdd.count() + tasks_rdd.count()

    processing_start = time.time()
    
    # Process machine capacities
    machines_parsed = machines_rdd.map(lambda line: line.split(',')) \
        .map(lambda x: (x[1], (safe_float(x[4]), safe_float(x[5]))))  # (machine_id, (cpu, memory))
    
    # Get latest capacities for each machine
    machines_latest = machines_parsed.groupByKey() \
        .mapValues(list) \
        .mapValues(lambda x: x[-1])  # Take last value for each machine
    
    # Process task events for evictions
    evictions = tasks_rdd.map(lambda line: line.split(',')) \
        .filter(lambda x: x[4] != '') \
        .map(lambda x: (x[4], 1 if x[5] == '2' else 0)) \
        .reduceByKey(lambda a, b: a + b)  # Count evictions per machine
    
    # Join capacities with evictions
    joined_data = machines_latest.join(evictions)
    
    # Convert to format for analysis
    results = joined_data.collect()
    cpu_capacities = [(cap[0], evict) for _, (cap, evict) in results]
    mem_capacities = [(cap[1], evict) for _, (cap, evict) in results]
    
    # Calculate correlations
    cpu_correlation = calculate_correlation(cpu_capacities)
    mem_correlation = calculate_correlation(mem_capacities)
    
    # Prepare data for plotting
    capacities_evictions = [(float(cap[0]), float(cap[1]), int(evict)) 
                           for _, (cap, evict) in results]
    
    cpu_caps = [x[0] for x in capacities_evictions]
    mem_caps = [x[1] for x in capacities_evictions]
    evict_counts = [x[2] for x in capacities_evictions]
    
    # Calculate average evictions by range
    cpu_ranges = [(get_capacity_range(cpu), evict) 
                 for cpu, _, evict in capacities_evictions]
    mem_ranges = [(get_capacity_range(mem), evict) 
                 for _, mem, evict in capacities_evictions]
    
    cpu_range_avgs = sc.parallelize(cpu_ranges) \
        .groupByKey() \
        .mapValues(lambda x: sum(x) / len(x)) \
        .sortByKey() \
        .collect()
        
    mem_range_avgs = sc.parallelize(mem_ranges) \
        .groupByKey() \
        .mapValues(lambda x: sum(x) / len(x)) \
        .sortByKey() \
        .collect()

    processing_end = time.time()
    
    # Create scatter plots
    plt.figure(figsize=(12, 5))
    
    plt.subplot(1, 2, 1)
    plt.scatter(cpu_caps, evict_counts, alpha=0.5)
    plt.xlabel('CPU Capacity (normalized)')
    plt.ylabel('Number of Evictions')
    plt.title('CPU Capacity vs Eviction Frequency')
    
    plt.subplot(1, 2, 2)
    plt.scatter(mem_caps, evict_counts, alpha=0.5)
    plt.xlabel('Memory Capacity (normalized)')
    plt.ylabel('Number of Evictions')
    plt.title('Memory Capacity vs Eviction Frequency')
    
    plt.tight_layout()
    
    # Save scatter plots
    timestamp = int(time.time())
    results_handler.save_plot(plt, f"rdd_q5_bonus1_capacity_evictions_scatter_{timestamp}.png")
    
    # Create bar plots
    plt.figure(figsize=(12, 5))
    
    plt.subplot(1, 2, 1)
    plt.bar([r[0] for r in cpu_range_avgs], [r[1] for r in cpu_range_avgs])
    plt.xlabel('CPU Capacity Range\n(0: 0-0.25, 1: 0.25-0.5, 2: 0.5-0.75, 3: 0.75-1.0)')
    plt.ylabel('Average Evictions')
    plt.title('Average Evictions by CPU Capacity Range')
    
    plt.subplot(1, 2, 2)
    plt.bar([r[0] for r in mem_range_avgs], [r[1] for r in mem_range_avgs])
    plt.xlabel('Memory Capacity Range\n(0: 0-0.25, 1: 0.25-0.5, 2: 0.5-0.75, 3: 0.75-1.0)')
    plt.ylabel('Average Evictions')
    plt.title('Average Evictions by Memory Capacity Range')
    
    plt.tight_layout()
    
    # Save bar plots
    results_handler.save_plot(plt, f"rdd_q5_bonus1_capacity_evictions_bars_{timestamp}.png")
    
    # Save detailed results
    results_handler.save_csv(
        data=[{
            "machine_id": r[0],
            "cpu_capacity": float(r[1][0][0]),
            "memory_capacity": float(r[1][0][1]),
            "eviction_count": int(r[1][1])
        } for r in results[:20]],
        columns=["machine_id", "cpu_capacity", "memory_capacity", "eviction_count"],
        filename=f"rdd_q5_bonus1_detailed_results_{timestamp}.csv"
    )
    
    # Save correlations
    results_handler.save_csv(
        data=[{
            "metric": "CPU Correlation",
            "value": float(cpu_correlation)
        }, {
            "metric": "Memory Correlation",
            "value": float(mem_correlation)
        }],
        columns=["metric", "value"],
        filename=f"rdd_q5_bonus1_correlations_{timestamp}.csv"
    )
    
    # Save average evictions by range
    results_handler.save_csv(
        data=[{"range": r[0], "avg_evictions": float(r[1])} for r in cpu_range_avgs],
        columns=["range", "avg_evictions"],
        filename=f"q5_bonus1_cpu_range_averages_{timestamp}.csv"
    )
    
    results_handler.save_csv(
        data=[{"range": r[0], "avg_evictions": float(r[1])} for r in mem_range_avgs],
        columns=["range", "avg_evictions"],
        filename=f"rdd_q5_bonus1_memory_range_averages_{timestamp}.csv"
    )

    # Save performance metrics
    results_handler.save_performance(
        data=[{"metric": "reading_time", "value": reading_end - reading_start},
              {"metric": "processing_time", "value": processing_end - processing_start},
              {"metric": "total_records", "value": total_records}],
        columns=["metric", "value"],
        filename=f"rdd_q5_bonus1_performance_metrics_{timestamp}.csv"
    )
    
    # Print results
    print("\nResults Sample (first 20 machines):")
    print(f"{'Machine ID':<15} {'CPU Capacity':<15} {'Memory Capacity':<15} {'Evictions':<15}")
    for r in results[:20]:
        print(f"{r[0]:<15} {r[1][0][0]:<15.2f} {r[1][0][1]:<15.2f} {r[1][1]:<15}")
    
    print("\nCorrelation Results:")
    print(f"CPU Correlation: {cpu_correlation:.3f}")
    print(f"Memory Correlation: {mem_correlation:.3f}")
    
    # Print performance metrics
    print("\nPerformance Metrics:")
    print(f"Reading Time: {reading_end - reading_start:.2f} seconds")
    print(f"Processing Time: {processing_end - processing_start:.2f} seconds")
    print(f"Total Records: {total_records}")
    
    return {
        "cpu_correlation": cpu_correlation,
        "memory_correlation": mem_correlation,
        "cpu_ranges": cpu_range_avgs,
        "memory_ranges": mem_range_avgs
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
    sc = create_spark_context()
    
    try:
        results = analyze_capacity_evictions(sc, results_handler)
        print(f"\nAnalysis complete. Results have been saved to {args.output_dir}")
        print("\nTo copy results to your local machine, use:")
        print(f"kubectl cp spark-master-0:{args.output_dir} ./local_results")
    finally:
        sc.stop()

if __name__ == "__main__":
    main()