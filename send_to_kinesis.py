import boto3
import json
import time
import random

# Initialize Kinesis client
kinesis = boto3.client("kinesis", region_name="us-east-1")  # Replace with your region

stream_name = "news-stream"

# Simulate streaming 100 JSON records
for i in range(100):
    record = {
        "news_id": f"id_{i}",
        "headline": random.choice(["AI is booming", "Market crash", "Elections soon"]),
        "sentiment": random.choice(["positive", "negative", "neutral"]),
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S')
    }
    print(f"Sending: {record}")
    kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(record),
        PartitionKey="partition-key"
    )
    time.sleep(0.5)  # Simulate live feed
