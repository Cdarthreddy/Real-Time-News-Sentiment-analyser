import pandas as pd
import matplotlib.pyplot as plt
import glob

def load_word_counts(local_dir="word_data"):
    files = glob.glob(f"{local_dir}/*.parquet")
    if not files:
        raise FileNotFoundError("No Parquet files found in word_data/")
    all_data = [pd.read_parquet(file) for file in files]
    df = pd.concat(all_data, ignore_index=True)
    return df

# Load and process
df = load_word_counts("word_data")
df['timestamp'] = pd.to_datetime(df['window'].apply(lambda x: x['start']))

# Filter top words manually or use .groupby("word").sum().nlargest()
top_words = df[df['word'].isin(['AI', 'finance', 'election', 'stocks', 'crypto'])]

# Plot
plt.figure(figsize=(12, 6))
for word in top_words['word'].unique():
    subset = top_words[top_words['word'] == word]
    plt.plot(subset['timestamp'], subset['count'], label=word)

plt.legend()
plt.title("Top Trending Words Over Time")
plt.xlabel("Time")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.grid(True)

# SAVE the plot instead of showing it
plt.savefig("top_trending_words.png")
print("✅ Plot saved as top_trending_words.png")

