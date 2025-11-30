import sys
import os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import DoubleType

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def load_data(spark, inputs):
    """
    Loads Business and Listings data from Parquet files.
    Assumes 'inputs' is the base project directory.
    """
    business_path = inputs + "/Nashville/business.parquet"
    listings_path = inputs + "/parquet_data/listings"

    print(f"Loading data from base directory: {inputs}")
    print(f"Business Path: {business_path}")
    print(f"Listings Path: {listings_path}")

    return (
        spark.read.parquet(business_path),
        spark.read.parquet(listings_path)
    )

def prepare_coordinates(business_df, listings_df):
    """
    Extracts latitude/longitude, unions them, and vectorizes.
    Returns the vectorized dataframe ready for KMeans.
    """
    print("Preparing and vectorizing coordinates...")

    coords_business = business_df.select(
        col("latitude").cast(DoubleType()), 
        col("longitude").cast(DoubleType())
    ).filter(col("latitude").isNotNull() & col("longitude").isNotNull())

    # Select and Cast Listings Coords
    coords_listings = listings_df.select(
        col("latitude").cast(DoubleType()), 
        col("longitude").cast(DoubleType())
    ).filter(col("latitude").isNotNull() & col("longitude").isNotNull())

    # Union
    all_coords = coords_business.union(coords_listings)

    # Vectorize
    assembler = VectorAssembler(
        inputCols=["latitude", "longitude"], 
        outputCol="features"
    )

    vectorized_coords = assembler.transform(all_coords).select("features")
    
    # Cache since we will iterate over this many times
    vectorized_coords.cache()
    
    print(f"Total points to cluster: {vectorized_coords.count()}")
    return vectorized_coords

def run_elbow_analysis(vectorized_coords, start_k=10, end_k=110, step=5):
    """
    Iterates through k values and calculates WCSS (Cost).
    Returns (k_values, costs).
    """
    print("\nStarting Elbow Method Calculation...")
    print("This may take a few minutes depending on your machine speed.\n")
    print("------------------------------------------------")
    print(f"{'Number of Zones (k)':<20} | {'Cost (WCSS)':<20}")
    print("------------------------------------------------")

    k_values = range(start_k, end_k, step)
    costs = []

    for k in k_values:
        kmeans = KMeans(featuresCol="features", k=k, seed=42)
        model = kmeans.fit(vectorized_coords)
        
        cost = model.summary.trainingCost
        costs.append(cost)
        
        print(f"{k:<20} | {cost:,.2f}")

    print("------------------------------------------------")
    print("Calculation Complete.")
    
    return k_values, costs

def plot_results(k_values, costs, output_image_name='nashville_elbow_plot_new.png'):
    """
    Generates and saves the Elbow Curve plot.
    """
    try:
        import matplotlib
        matplotlib.use('Agg') 
        import matplotlib.pyplot as plt
        
        plt.figure(figsize=(10, 6))
        plt.plot(k_values, costs, marker='o', linestyle='-', color='b')
        plt.title('The Elbow Method: Optimal k for Nashville Zones')
        plt.xlabel('Number of Zones (k)')
        plt.ylabel('Cost (Within-Cluster Sum of Squares)')
        plt.grid(True)
        
        plt.savefig(output_image_name)
        print(f"\nPlot saved as '{output_image_name}'. Open this image to see the curve.")
        
    except ImportError:
        print("\nMatplotlib not found. Please check the printed table above.")
        print("Look for the k value where the Cost decrease starts to slow down (flatten out).")

# ==========================================
# MAIN LOGIC
# ==========================================

def main(inputs):
    # Retrieve the Spark session
    spark = SparkSession.builder.getOrCreate()

    # 1. Load Data
    business_df, listings_df = load_data(spark, inputs)

    # 2. Prepare Vectors (Clean & Vectorize)
    vectorized_coords = prepare_coordinates(business_df, listings_df)

    # 3. Run Analysis
    k_values, costs = run_elbow_analysis(vectorized_coords)

    # 4. Save Plot
    plot_results(k_values, costs)

if __name__ == '__main__':
    # INPUTS (From Env Var or Default S3)
    # Default: s3a://cmpt732-pluto-project/yelp_parquet
    inputs = os.getenv("S3_INPUT_BASE_PATH", "s3a://cmpt732-pluto-project/yelp_parquet")
    
    spark = SparkSession.builder \
        .appName('Nashville Elbow Method Analysis - For clusterings into optimal Zones') \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
        
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    main(inputs)
