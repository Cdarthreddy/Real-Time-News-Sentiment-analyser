from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import explode, split, col, window

def main():
    spark = SparkSession.builder \
        .appName("NewsSentimentStreamFromS3") \
        .getOrCreate()

    # Configure S3
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

    # Define schema
    schema = StructType([
        StructField("news_id", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    # Input S3 path
    input_path = "s3a://news-sentiment-data-scp/"

    # Read stream from S3
    df = spark.readStream \
        .format("json") \
        .schema(schema) \
        .load(input_path)

    # ✅ Write sentiment data to S3 (append mode)
    sentiment_query = df.select("news_id", "headline", "sentiment", "timestamp") \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", "s3a://news-sentiment-data-scp/checkpoints/sentiment/") \
        .option("path", "s3a://news-sentiment-data-scp/results/sentiment/") \
        .start()

    # ✅ Sliding Window Word Count with Watermark
    words_df = df \
        .withWatermark("timestamp", "10 minutes") \
        .select(explode(split(col("headline"), r"\s+")).alias("word"), col("timestamp"))

    windowed_counts = words_df.groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("word")
    ).count()

    # ✅ Write word count results to S3
    wordcount_query = windowed_counts \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", "s3a://news-sentiment-data-scp/checkpoints/words/") \
        .option("path", "s3a://news-sentiment-data-scp/results/words/") \
        .start()

    # ✅ Await both streaming queries
    sentiment_query.awaitTermination()
    wordcount_query.awaitTermination()

if __name__ == "__main__":
    main()

