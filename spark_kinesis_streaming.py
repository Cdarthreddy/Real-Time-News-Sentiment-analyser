from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import from_json, col, window, explode, split

def main():
    spark = SparkSession.builder \
        .appName("NewsFromKinesis") \
        .getOrCreate()

    # Schema of the incoming JSON records
    schema = StructType() \
        .add("news_id", StringType()) \
        .add("headline", StringType()) \
        .add("sentiment", StringType()) \
        .add("timestamp", StringType())  # We'll parse it later if needed

    # Read from Kinesis
    raw_df = spark.readStream \
        .format("kinesis") \
        .option("streamName", "news-stream") \
        .option("region", "us-east-1") \
        .option("initialPosition", "LATEST") \
        .load()

    # Convert binary to JSON string
    json_df = raw_df.selectExpr("CAST(data AS STRING) AS json_string")
    parsed_df = json_df.select(from_json("json_string", schema).alias("data")).select("data.*")

    # Word count with sliding window
    words = parsed_df \
        .withColumn("word", explode(split(col("headline"), r"\s+"))) \
        .withColumn("timestamp", col("timestamp").cast("timestamp"))

    windowed_counts = words \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("word")) \
        .count()

    # Output to console (or to S3 or OpenSearch if needed)
    query = windowed_counts.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
