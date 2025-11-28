import sys
import os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import (
    col, split, size, regexp_replace, avg, sum, count, countDistinct,
    lit, when, coalesce, round as spark_round
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import GBTRegressor
from pyspark.sql.types import FloatType, DoubleType, StructType, StructField, IntegerType

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def load_data(spark, inputs):
    """
    Loads Business, Checkin, and Listings data from Parquet files.
    Assumes 'inputs' is the base project directory.
    """
    # Adjusted to match your specific folder structure relative to the project root
    business_path = inputs + "/Nashville/business.parquet"
    checkin_path = inputs + "/Nashville/checkin.parquet"
    listings_path = inputs + "/parquet_data/listings"

    print(f"Loading data from base directory: {inputs}")
    print(f"Business Path: {business_path}")
    print(f"Listings Path: {listings_path}")

    return (
        spark.read.parquet(business_path),
        spark.read.parquet(checkin_path),
        spark.read.parquet(listings_path)
    )

def clean_data(business_df, checkin_df, listings_df):
    """
    Cleans Airbnb price/revenue and aggregates Yelp checkins.
    Returns (business_joined, listings_cleaned)
    """
    # --- Clean Airbnb Listings ---
    listings_cleaned = listings_df \
        .withColumn("latitude", col("latitude").cast(DoubleType())) \
        .withColumn("longitude", col("longitude").cast(DoubleType())) \
        .filter(col("latitude").isNotNull() & col("longitude").isNotNull()) \
        .withColumn(
            "price_clean", 
            regexp_replace(col("price"), "[$,]", "").cast(FloatType())
        ).na.fill(0, subset=["reviews_per_month"])

    # Calculate Revenue Proxy
    listings_cleaned = listings_cleaned.withColumn(
        "est_monthly_revenue", 
        col("price_clean") * col("reviews_per_month")
    )

    # --- Clean Yelp Checkins ---
    checkin_cleaned = checkin_df.withColumn(
        "checkin_count", 
        size(split(col("date"), ","))
    )

    # Join Checkin counts to Business Data
    business_joined = business_df.join(
        checkin_cleaned.select("business_id", "checkin_count"), 
        on="business_id", 
        how="left"
    ).na.fill(0, subset=["checkin_count"])

    return business_joined, listings_cleaned

def perform_spatial_clustering(business_joined, listings_cleaned, k=35):
    """
    Unifies spatial data using K-Means clustering.
    Returns (business_zoned, listings_zoned, zone_centers_df)
    """
    # Extract Coordinates
    coords_business = business_joined.select(
        col("latitude").cast(DoubleType()), 
        col("longitude").cast(DoubleType())
    ).filter(col("latitude").isNotNull() & col("longitude").isNotNull())

    coords_listings = listings_cleaned.select(
        col("latitude"), 
        col("longitude")
    )

    # Union and Vectorize
    all_coords = coords_business.union(coords_listings)
    assembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
    vectorized_coords = assembler.transform(all_coords)

    # Train K-Means
    kmeans = KMeans(k=k, seed=42)
    kmeans_model = kmeans.fit(vectorized_coords)

    # Helper to apply model
    def assign_zone(df):
        df_valid = df.filter(col("latitude").isNotNull() & col("longitude").isNotNull())
        vec_df = assembler.transform(df_valid)
        return kmeans_model.transform(vec_df).withColumnRenamed("prediction", "zone_id")

    # Assign Zones
    business_zoned = assign_zone(business_joined)
    listings_zoned = assign_zone(listings_cleaned)

    # Extract Centers
    centers = kmeans_model.clusterCenters()
    centers_data = [(int(i), float(c[0]), float(c[1])) for i, c in enumerate(centers)]
    schema_centers = StructType([
        StructField("zone_id", IntegerType(), False),
        StructField("center_lat", FloatType(), False),
        StructField("center_lon", FloatType(), False)
    ])
    
    # We need the spark session to createDataFrame, assuming it's available in context or passed
    # Since we are inside a function without 'spark', we can get it from the dataframe
    spark = business_joined.sparkSession
    zone_centers_df = spark.createDataFrame(centers_data, schema=schema_centers)

    return business_zoned, listings_zoned, zone_centers_df

def engineer_features(business_zoned, listings_zoned, zone_centers_df):
    """
    Aggregates supply and demand metrics by zone.
    Returns master_df
    """
    # Aggregate Yelp (Demand)
    yelp_features = business_zoned.groupBy("zone_id").agg(
        count("business_id").alias("total_businesses"),
        avg("stars").alias("avg_biz_stars"),
        sum("review_count").alias("total_biz_reviews"),
        sum("checkin_count").alias("total_checkins")
    )

    # Aggregate Airbnb (Supply)
    airbnb_features = listings_zoned.groupBy("zone_id").agg(
        countDistinct("id").alias("airbnb_listing_count"),
        avg("price_clean").alias("avg_nightly_price"),
        avg("est_monthly_revenue").alias("avg_actual_revenue")
    )

    # Merge
    master_df = yelp_features.join(airbnb_features, on="zone_id", how="inner") \
                             .join(zone_centers_df, on="zone_id", how="inner") \
                             .na.fill(0)
    return master_df

def train_model(master_df):
    """
    Trains GBT Regressor and predicts revenue.
    Returns predictions dataframe
    """
    feature_cols = ["total_businesses", "avg_biz_stars", "total_biz_reviews", "total_checkins"]
    assembler_ml = VectorAssembler(inputCols=feature_cols, outputCol="features")
    ml_data = assembler_ml.transform(master_df)

    # Train GBT Regressor
    gbt = GBTRegressor(featuresCol="features", labelCol="avg_actual_revenue", maxIter=50, seed=42)
    gbt_model = gbt.fit(ml_data)

    # Optional: Print Feature Importance
    print("\n--- Feature Importance ---")
    importances = gbt_model.featureImportances
    for i, feature in enumerate(feature_cols):
        print(f"{feature}: {importances[i]:.4f}")
    print("--------------------------\n")

    return gbt_model.transform(ml_data)

def save_output(predictions, output_path):
    """
    Calculates final scores and saves to CSV.
    """
    final_output = predictions.withColumn(
        "investment_potential_score", 
        col("prediction") - col("avg_actual_revenue")
    ).withColumn(
        "market_status",
        when(col("investment_potential_score") > 100, "Hidden Gem (Invest)")
        .when(col("investment_potential_score") < -100, "Saturated (Avoid)")
        .otherwise("Balanced Market")
    )

    power_bi_df = final_output.select(
        "zone_id", "center_lat", "center_lon", "market_status",
        spark_round("investment_potential_score", 2).alias("investment_potential_score"),
        spark_round("prediction", 2).alias("predicted_revenue_potential"),
        spark_round("avg_actual_revenue", 2).alias("avg_actual_revenue"),
        spark_round("avg_nightly_price", 2).alias("avg_nightly_price"),
        "airbnb_listing_count", "total_businesses", "total_checkins",
        spark_round("avg_biz_stars", 1).alias("avg_biz_rating")
    )

    power_bi_df.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(output_path)

# ==========================================
# MAIN LOGIC
# ==========================================

def main(inputs, output):
    # Retrieve the Spark session
    spark = SparkSession.builder.getOrCreate()

    # 1. Load
    business_df, checkin_df, listings_df = load_data(spark, inputs)

    # 2. Clean
    business_joined, listings_cleaned = clean_data(business_df, checkin_df, listings_df)

    # 3. Cluster
    business_zoned, listings_zoned, zone_centers_df = perform_spatial_clustering(business_joined, listings_cleaned, k=35)

    # 4. Feature Engineering
    master_df = engineer_features(business_zoned, listings_zoned, zone_centers_df)

    # 5. Train
    predictions = train_model(master_df)

    # 6. Save
    save_output(predictions, output)

if __name__ == '__main__':
    # INPUTS (From Env Var or Default S3)
    # Default: s3a://cmpt732-pluto-project/yelp_parquet
    inputs = os.getenv("S3_INPUT_BASE_PATH", "s3a://cmpt732-pluto-project/yelp_parquet")
    
    # OUTPUT (From Env Var or Default S3)
    # Default: s3a://cmpt732-pluto-project/nashville_investment_zones_gbt_35
    output = os.getenv("S3_OUTPUT_MODEL_PATH", "s3a://cmpt732-pluto-project/nashville_investment_zones_gbt_35")
    
    spark = SparkSession.builder \
        .appName('Nashville GBT Analysis') \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
        
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)