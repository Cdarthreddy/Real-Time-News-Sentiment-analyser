import pandas as pd
import boto3
import time
import uuid
import json
from datetime import datetime

# AWS S3 bucket details
bucket_name = 'news-sentiment-data-scp'
s3_prefix = 'stream/'  # This is the folder in your bucket

# Load CSV file (modify path if needed)
df = pd.read_csv('data/news_data/True.csv').head(100)

# Initialize S3 client
s3 = boto3.client('s3')

for _, row in df.iterrows():
    # Construct record
    record = {
        "news_id": str(uuid.uuid4()),
        "headline": row['title'],
        "sentiment": "positive",  # Optional: run sentiment analysis if needed
        "timestamp": datetime.utcnow().isoformat()
    }

    # Convert to JSON
    json_data = json.dumps(record)
    
    # File path in S3
    s3_key = f"{s3_prefix}{record['news_id']}.json"
    
    # Upload to S3
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=json_data)
    print(f"Uploaded {s3_key} to {bucket_name}")
    
    # Wait 1 second to simulate real-time data
    time.sleep(1)
