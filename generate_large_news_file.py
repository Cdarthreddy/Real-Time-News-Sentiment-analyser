import pandas as pd
import json

df_fake = pd.read_csv("data/news_data/Fake.csv")
df_true = pd.read_csv("data/news_data/True.csv")
texts = pd.concat([df_fake[["title", "text"]], df_true[["title", "text"]]], ignore_index=True)
texts = texts.dropna()

target_size_gb = 1.5
target_size_bytes = int(target_size_gb * 1024**3)

output_path = "data/part3_1_5gb.txt"
with open(output_path, "w", encoding="utf-8") as f:
    total = 0
    while total < target_size_bytes:
        for _, row in texts.iterrows():
            article_json = {
                "title": row["title"],
                "description": row["text"]
            }
            line = json.dumps(article_json) + "\n"
            f.write(line)
            total += len(line.encode("utf-8"))
            if total >= target_size_bytes:
                break

print(f"✅ Done: {output_path} (~{target_size_gb}GB)")

