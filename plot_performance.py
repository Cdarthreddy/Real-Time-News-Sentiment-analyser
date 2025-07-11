import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load benchmark results
df = pd.read_csv("benchmark_results.csv")

# Clean up column names
df.columns = df.columns.str.strip().str.lower()

# Set up plot style
sns.set(style="whitegrid")

# Plot 1: Articles Processed vs Time (Line Plot)
plt.figure(figsize=(10, 6))
plt.plot(df["size"], df["sequential_time(s)"], marker='o', label="Sequential Time")
plt.plot(df["size"], df["parallel_time(s)"], marker='s', label="Parallel Time")
plt.title("Articles Processed: Sequential vs Parallel Execution Time")
plt.xlabel("Dataset Size")
plt.ylabel("Time (seconds)")
plt.legend()
plt.tight_layout()
plt.savefig("articles_vs_time.png")
plt.show()

# Plot 2: Throughput vs Input Size (Bar Chart)
plt.figure(figsize=(8, 6))
sns.barplot(x="size", y="throughput_parallel", hue="size", data=df, palette="viridis", legend=False)
plt.title("Throughput vs Input Size")
plt.xlabel("Dataset Size")
plt.ylabel("Throughput (articles/sec)")
plt.tight_layout()
plt.savefig("throughput_vs_input.png")
plt.show()

# Plot 3: Speedup vs Input Size
plt.figure(figsize=(8, 6))
sns.barplot(x="size", y="speedup", hue="size", data=df, palette="magma", legend=False)
plt.title("Speedup of Parallel Processing")
plt.xlabel("Dataset Size")
plt.ylabel("Speedup (Sequential Time / Parallel Time)")
plt.tight_layout()
plt.savefig("speedup_plot.png")
plt.show()

