import csv
import json
import time
import gzip
import io
from datetime import datetime
from kafka import KafkaProducer
from google.cloud import storage
import threading

class ClusterEventProducer:
    def __init__(self, project_id):
        print("Initializing ClusterEventProducer...", flush=True)
        self.producer = KafkaProducer(
            bootstrap_servers='kafka-cluster-kafka-bootstrap:9092',
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=100,
            buffer_memory=33554432,
            compression_type='gzip'
        )
        print("Kafka producer initialized.", flush=True)
        print("Initializing GCS client...", flush=True)
        self.storage_client = storage.Client(project=project_id)
        self.bucket = self.storage_client.bucket('clusterdata-2011-2')
        print("GCS client initialized.", flush=True)
    
    def list_available_files(self, prefix):
        """List all available files in the GCS bucket with given prefix"""
        return [blob.name for blob in self.bucket.list_blobs(prefix=prefix)]

    def stream_gcs_file(self, blob_path):
        """Stream a gzipped CSV file from GCS"""
        print(f"Starting to process file: {blob_path}", flush=True)
        blob = self.bucket.blob(blob_path)
        
        buffer = io.BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        
        with gzip.GzipFile(fileobj=buffer, mode='rb') as gz:
            text_wrapper = io.TextIOWrapper(gz, encoding='utf-8')
            reader = csv.reader(text_wrapper)
            for row in reader:
                yield row

    def process_file(self, event_type, blob_path, speed_factor=60):
        """Process a single file of events"""
        print(f"Processing {event_type} file: {blob_path}", flush=True)
        topic = f"{event_type}-events"
        
        current_timestamp = None
        current_events = []
        start_time = None
        first_event_time = None
        event_count = 0
        batch_size = 100  # Process events in batches
        
        try:
            print(f"Starting to read rows from {blob_path}", flush=True)
            for row in self.stream_gcs_file(blob_path):
                try:
                    timestamp = int(float(row[0]))
                    event = self.parse_event(event_type, row)
                    
                    if first_event_time is None:
                        first_event_time = timestamp
                        start_time = time.time()
                        print(f"First event time set to {timestamp}", flush=True)
                    
                    if current_timestamp != timestamp and current_events:
                        # Send events for previous timestamp
                        event_delay = (current_timestamp - first_event_time) / speed_factor
                        current_delay = time.time() - start_time
                        sleep_time = event_delay - current_delay
                        
                        if sleep_time > 0:
                            print(f"Sleeping for {sleep_time:.2f} seconds", flush=True)
                            time.sleep(sleep_time)
                        
                        # Send events in batches
                        for i in range(0, len(current_events), batch_size):
                            batch = current_events[i:i + batch_size]
                            futures = [self.producer.send(topic, value=e) for e in batch]
                            # Wait for the batch to be sent
                            for future in futures:
                                future.get(timeout=10)
                            event_count += len(batch)
                            print(f"Sent batch of {len(batch)} events. Total events sent: {event_count}", flush=True)
                        
                        current_events = []
                    
                    current_timestamp = timestamp
                    current_events.append(event)
                    
                    # Also process events in batches even if timestamp hasn't changed
                    if len(current_events) >= batch_size:
                        futures = [self.producer.send(topic, value=e) for e in current_events]
                        # Wait for the batch to be sent
                        for future in futures:
                            future.get(timeout=10)
                        event_count += len(current_events)
                        print(f"Sent batch of {len(current_events)} events. Total events sent: {event_count}", flush=True)
                        current_events = []
                    
                except (ValueError, IndexError) as e:
                    print(f"Skipping malformed row in {blob_path}. Error: {str(e)}", flush=True)
                    continue
            
            # Send remaining events
            if current_events:
                futures = [self.producer.send(topic, value=e) for e in current_events]
                # Wait for the batch to be sent
                for future in futures:
                    future.get(timeout=10)
                event_count += len(current_events)
                print(f"Sent final batch of {len(current_events)} events. Total events sent: {event_count}", flush=True)
            
            print(f"Completed processing {blob_path}. Sent {event_count} events.", flush=True)
            
        except Exception as e:
            print(f"Error processing file {blob_path}: {str(e)}", flush=True)
            import traceback
            traceback.print_exc()
            raise

        return event_count

    def process_events_incrementally(self, event_type, start_file_index=0, num_files=1, speed_factor=60):
        """Process multiple files incrementally"""
        prefix = f"{event_type}_events/"
        available_files = sorted(self.list_available_files(prefix))
        
        if not available_files:
            print(f"No files found with prefix {prefix}", flush=True)
            return
        
        print(f"Found {len(available_files)} files. Processing {num_files} files starting from index {start_file_index}", flush=True)
        
        end_index = min(start_file_index + num_files, len(available_files))
        files_to_process = available_files[start_file_index:end_index]
        
        for file_path in files_to_process:
            try:
                self.process_file(event_type, file_path, speed_factor)
                print(f"Successfully processed {file_path}", flush=True)
            except Exception as e:
                print(f"Failed to process {file_path}: {str(e)}", flush=True)
                continue

    def process_events_concurrent(self, start_file_index=0, num_files=1, speed_factor=60):
        """Process job and task events concurrently"""
        # Create threads for job and task processing
        job_thread = threading.Thread(
            target=self.process_events_incrementally,
            args=('job', start_file_index, num_files, speed_factor)
        )
        
        task_thread = threading.Thread(
            target=self.process_events_incrementally,
            args=('task', start_file_index, num_files, speed_factor)
        )
        
        print("Starting concurrent processing of job and task events...", flush=True)
        
        # Start both threads
        job_thread.start()
        task_thread.start()
        
        # Wait for both threads to complete
        job_thread.join()
        task_thread.join()
        
        print("Completed concurrent processing of job and task events.", flush=True)

    def parse_event(self, event_type, row):
        """Parse different event types"""
        if event_type == 'job':
            return {
                'timestamp': int(float(row[0])),
                'missing_type': row[1],
                'job_id': row[2],
                'event_type': row[3],
                'user': row[4],
                'scheduling_class': row[5],
                'job_name': row[6],
                'logical_job_name': row[7]
            }
        else:  # task events
            return {
                'timestamp': int(float(row[0])),
                'missing_type': row[1],
                'job_id': row[2],
                'task_index': row[3],
                'machine_id': row[4],
                'event_type': row[5],
                'user': row[6],
                'scheduling_class': row[7],
                'priority': row[8],
                'cpu_request': row[9],
                'memory_request': row[10],
                'disk_space_request': row[11]
            }

def main():
    import os
    import argparse
    
    parser = argparse.ArgumentParser(description='Process cluster events')
    parser.add_argument('--start-file', type=int, default=0, help='Starting file index')
    parser.add_argument('--num-files', type=int, default=1, help='Number of files to process')
    parser.add_argument('--speed-factor', type=int, default=60, help='Speed factor for event replay')
    args = parser.parse_args()

    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    if not project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT environment variable must be set")
    
    print("Starting producer with configuration:", flush=True)
    print(f"  Start File Index: {args.start_file}", flush=True)
    print(f"  Number of Files: {args.num_files}", flush=True)
    print(f"  Speed Factor: {args.speed_factor}", flush=True)
    print(f"  Project ID: {project_id}", flush=True)
    
    producer = ClusterEventProducer(project_id)
    
    # Process files concurrently
    print(f"Starting concurrent processing with speed_factor={args.speed_factor}...", flush=True)
    producer.process_events_concurrent(args.start_file, args.num_files, args.speed_factor)

if __name__ == "__main__":
    print("Starting producer...", flush=True)
    main()