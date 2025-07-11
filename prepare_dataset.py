import pandas as pd
import uuid
import random
from datetime import datetime, timedelta

# Load both datasets
fake_df = pd.read_csv("data/news_data/Fake.csv")
true_df = pd.read_csv("data/news_data/True.csv")

# Add labels
fake_df['sentiment'] = 'fake'
true_df['sentiment'] = 'real'

# Combine and shuffle
df = pd.concat([fake_df, true_df]).sample(frac=1).reset_index(drop=True)

# Add required fields
df['news_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
df['headline'] = df['title']
start_time = datetime.now() - timedelta(minutes=len(df))

# Assign synthetic timestamps spaced 10 seconds apart
df['timestamp'] = [start_time + timedelta(seconds=i*10) for i in range(len(df))]

# Keep only needed columns
df = df[['news_id', 'headline', 'sentiment', 'timestamp']]

# Save as JSON lines (newline-delimited JSON)
df.to_json("data/news_stream.json", orient="records", lines=True, date_format="iso")
print("✅ Saved to data/news_stream.json")
