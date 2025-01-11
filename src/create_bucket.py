from google.cloud import storage
import sys

def create_bucket(bucket_name, project_id, location="US"):
    """Create a new bucket in Google Cloud Storage"""
    storage_client = storage.Client(project=project_id)
    
    try:
        bucket = storage_client.create_bucket(
            bucket_name,
            project=project_id,
            location=location
        )
        print(f"Bucket {bucket.name} created successfully in {bucket.location}")
        return bucket
    except Exception as e:
        print(f"Error creating bucket: {e}")
        return None

if __name__ == "__main__":
    # Read the project ID from the command-line arguments
    if len(sys.argv) != 2:
        print("Usage: python create_bucket.py PROJECT_ID")
        sys.exit(1)

    PROJECT_ID = sys.argv[1]
    
    # Create a unique bucket name using project ID and purpose
    BUCKET_NAME = f"{PROJECT_ID}-cluster-analysis"
    
    bucket = create_bucket(BUCKET_NAME, PROJECT_ID)
    if bucket:
        print(f"\nYou can now use this bucket name in your analysis script:")
        print(f"BUCKET_NAME = \"{BUCKET_NAME}\"")