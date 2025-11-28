import sys
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, current_timestamp, struct, to_date, to_timestamp, year, month, concat_ws, lpad, avg, count, window, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "yelp-reviews"
CHECKPOINT_LOCATION = "./checkpoint_sentiment_full"

# S3 Paths
S3_BUCKET_BASE = os.getenv("S3_BUCKET_BASE", "s3a://cmpt732-pluto-project")
AIRBNB_BASE_PATH = f"{S3_BUCKET_BASE}/Airbnb_by_city/Nashville_Tennessee_United_States"
YELP_BUSINESS_PATH = os.getenv("S3_BUSINESS_PATH", f"{S3_BUCKET_BASE}/yelp_parquet/Nashville/business.parquet")

# UDFs
analyzer = SentimentIntensityAnalyzer()

def clean_text_py(s):
    if s is None: return None
    s = str(s)
    s = re.sub(r'<[^>]+>', ' ', s)
    s = re.sub(r'http\S+|www\.\S+', ' ', s)
    s = re.sub(r'[\r\n\t]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def get_sentiment_score(text):
    if not text: return 0.0
    try:
        return analyzer.polarity_scores(str(text)).get("compound", 0.0)
    except:
        return 0.0

clean_text_udf = udf(clean_text_py, StringType())
sentiment_udf = udf(get_sentiment_score, DoubleType())

def main():
    spark = SparkSession.builder \
        .appName("YelpAirbnb_StreamStatic_Join") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # --- 1. Static Source: Airbnb (Pre-Compute) ---
    print("Pre-computing Airbnb Static Stats...")
    try:
        listings_path = f"{AIRBNB_BASE_PATH}/listings.csv"
        reviews_path = f"{AIRBNB_BASE_PATH}/reviews.csv.gz"

        df_listings = spark.read.csv(listings_path, header=True, inferSchema=True, multiLine=True, escape='"')
        df_reviews = spark.read.csv(reviews_path, header=True, inferSchema=True, multiLine=True, escape='"')

        if "comments" not in df_reviews.columns:
            print("WARNING: 'comments' column missing in Airbnb reviews. Sentiment will be 0.")
            df_reviews = df_reviews.withColumn("comments", lit(""))
        
        # Sample for speed
        df_reviews = df_reviews.sample(fraction=0.1, seed=42)

        airbnb_joined = df_reviews.join(df_listings, df_reviews.listing_id == df_listings.id) \
            .select(
                to_date(df_reviews.date).alias("date"),
                col("comments").alias("text")
            )
        
        airbnb_scored = airbnb_joined \
            .withColumn("clean_text", clean_text_udf(col("text"))) \
            .withColumn("sentiment", sentiment_udf(col("clean_text")))
        
        airbnb_stats = airbnb_scored \
            .withColumn("year", year(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("year_month", concat_ws("-", col("year"), lpad(col("month"), 2, "0"))) \
            .groupBy("year_month") \
            .agg(
                avg("sentiment").alias("airbnb_avg_sentiment"),
                count("sentiment").alias("airbnb_review_count")
            ) \
            .cache()
        
        print(f"Airbnb Stats Cached. Count: {airbnb_stats.count()}")
        
    except Exception as e:
        print(f"ERROR loading Airbnb data: {e}")
        airbnb_stats = spark.createDataFrame([], schema=StructType([
            StructField("year_month", StringType()),
            StructField("airbnb_avg_sentiment", DoubleType()),
            StructField("airbnb_review_count", IntegerType())
        ]))

    # --- 2. Streaming Source: Yelp (Kafka) ---
    print("Starting Yelp Stream...")
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    schema = StructType([
        StructField("business_id", StringType()),
        StructField("user_id", StringType()),
        StructField("stars", DoubleType()),
        StructField("text", StringType()),
        StructField("date", StringType()),
        StructField("stream_timestamp", StringType())
    ])

    yelp_stream = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*") \
        .withColumn("timestamp", to_timestamp(col("date"))) \
        .withColumn("clean_text", clean_text_udf(col("text"))) \
        .withColumn("sentiment", sentiment_udf(col("clean_text")))

    # --- 3. Aggregate Stream (Windowing) ---
    yelp_windowed = yelp_stream \
        .withWatermark("timestamp", "2 hours") \
        .groupBy(window(col("timestamp"), "30 days", "1 day")) \
        .agg(
            avg("sentiment").alias("yelp_avg_sentiment"),
            count("sentiment").alias("yelp_review_count")
        ) \
        .withColumn("year_month", concat_ws("-", year(col("window.start")), lpad(month(col("window.start")), 2, "0")))

    # --- 4. Stream-Static Join ---
    comparison_stream = yelp_windowed.join(airbnb_stats, "year_month", "left_outer") \
        .select(
            col("year_month"),
            col("yelp_avg_sentiment"),
            col("airbnb_avg_sentiment"),
            col("yelp_review_count"),
            col("airbnb_review_count"),
            (col("airbnb_avg_sentiment") - col("yelp_avg_sentiment")).alias("sentiment_gap")
        )

    # --- 5. Tourism Alignment Logic ---
    final_stream = comparison_stream.withColumn(
        "tourism_status",
        when((col("airbnb_avg_sentiment") >= 0.6) & (col("yelp_avg_sentiment") >= 0.6), "Tourism Booming (Both High) 🚀")
        .when((col("airbnb_avg_sentiment") < 0.5) & (col("yelp_avg_sentiment") >= 0.6), "Great Food, Poor Stay (Yelp > Airbnb) 🍔")
        .when((col("airbnb_avg_sentiment") >= 0.6) & (col("yelp_avg_sentiment") < 0.5), "Great Stay, Bad Food (Airbnb > Yelp) 🛌")
        .otherwise("Mixed / Neutral 😐")
    )

    # --- 6. Output to Local CSV ---
    def write_to_dashboard_csv(df, epoch_id):
        output_path = "/home/ubuntu/dashboard_data"
        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        print(f"Batch {epoch_id} written to {output_path}")

    print("Starting Stream... Writing to /home/ubuntu/dashboard_data")
    query = final_stream.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_to_dashboard_csv) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
