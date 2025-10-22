from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os


def create_spark_session():
    """Create a Spark session with appropriate configuration."""
    return (
        SparkSession.builder.appName("Yelp JSON to Parquet Converter")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )


def convert_business_to_parquet(spark, input_path, output_path):
    """Convert business JSON to Parquet with proper schema."""
    print("Converting business.json to Parquet...")

    # Read JSON
    df = spark.read.json(input_path)

    # Write as Parquet partitioned by state (for Canadian data, by province)
    df.write.mode("overwrite").partitionBy("state").parquet(output_path)

    print(f"  Saved to {output_path}")
    print(f"  Total records: {df.count()}")


def convert_review_to_parquet(spark, input_path, output_path):
    """Convert review JSON to Parquet."""
    print("Converting review.json to Parquet...")

    df = spark.read.json(input_path)

    # Partition by year (extract from date field)
    df_with_year = df.withColumn("year", df.date.substr(1, 4))

    df_with_year.write.mode("overwrite").partitionBy("year").parquet(output_path)

    print(f"  Saved to {output_path}")
    print(f"  Total records: {df.count()}")


def convert_user_to_parquet(spark, input_path, output_path):
    """Convert user JSON to Parquet."""
    print("Converting user.json to Parquet...")

    df = spark.read.json(input_path)

    df.write.mode("overwrite").parquet(output_path)

    print(f"  Saved to {output_path}")
    print(f"  Total records: {df.count()}")


def convert_tip_to_parquet(spark, input_path, output_path):
    """Convert tip JSON to Parquet."""
    print("Converting tip.json to Parquet...")

    df = spark.read.json(input_path)

    df.write.mode("overwrite").parquet(output_path)

    print(f"  Saved to {output_path}")
    print(f"  Total records: {df.count()}")


def convert_checkin_to_parquet(spark, input_path, output_path):
    """Convert checkin JSON to Parquet."""
    print("Converting checkin.json to Parquet...")

    df = spark.read.json(input_path)

    df.write.mode("overwrite").parquet(output_path)

    print(f"  Saved to {output_path}")
    print(f"  Total records: {df.count()}")


def convert_photo_to_parquet(spark, input_path, output_path):
    """Convert photo JSON to Parquet."""
    print("Converting photo.json to Parquet...")

    df = spark.read.json(input_path)

    df.write.mode("overwrite").partitionBy("label").parquet(output_path)

    print(f"  Saved to {output_path}")
    print(f"  Total records: {df.count()}")


def convert_city_to_parquet(spark, city_dir, output_base_dir):
    """Convert all JSON files in a city directory to Parquet."""
    city_name = os.path.basename(city_dir)
    print(f"\n{'=' * 60}")
    print(f"Processing city: {city_name}")
    print(f"{'=' * 60}")

    output_city_dir = os.path.join(output_base_dir, city_name)

    # Define file mappings
    files_to_convert = {
        "business.json": ("business.parquet", convert_business_to_parquet),
        "review.json": ("review.parquet", convert_review_to_parquet),
        "user.json": ("user.parquet", convert_user_to_parquet),
        "tip.json": ("tip.parquet", convert_tip_to_parquet),
        "checkin.json": ("checkin.parquet", convert_checkin_to_parquet),
        "photo.json": ("photo.parquet", convert_photo_to_parquet),
    }

    for json_file, (parquet_name, converter_func) in files_to_convert.items():
        input_path = os.path.join(city_dir, json_file)
        output_path = os.path.join(output_city_dir, parquet_name)

        if os.path.exists(input_path):
            try:
                converter_func(spark, input_path, output_path)
            except Exception as e:
                print(f"  ERROR converting {json_file}: {e}")
        else:
            print(f"  Skipping {json_file} (not found)")


def main():
    # Configuration
    input_base_dir = "/home/aarish/Documents/University/CMPT732/Project/yelp_by_city"
    output_base_dir = "/home/aarish/Documents/University/CMPT732/Project/yelp_parquet"

    # Set to True to only process Canadian cities, False for all cities
    CANADIAN_ONLY = True

    # Canadian province codes
    canadian_provinces = {
        "AB",
        "BC",
        "MB",
        "NB",
        "NL",
        "NS",
        "NT",
        "NU",
        "ON",
        "PE",
        "QC",
        "SK",
        "YT",
    }

    # Create Spark session
    print("Starting Spark session...")
    spark = create_spark_session()

    # Get all city directories
    city_dirs = [
        os.path.join(input_base_dir, d)
        for d in os.listdir(input_base_dir)
        if os.path.isdir(os.path.join(input_base_dir, d))
    ]

    # Filter for Canadian cities if needed
    if CANADIAN_ONLY:
        print("\nFiltering for Canadian cities only...")
        filtered_dirs = []
        for city_dir in city_dirs:
            # Check if this city has Canadian businesses by reading a sample
            business_file = os.path.join(city_dir, "business.json")
            if os.path.exists(business_file):
                try:
                    # Read just first business to check state
                    df_sample = spark.read.json(business_file).limit(1)
                    state = df_sample.select("state").first()
                    if state and state["state"] in canadian_provinces:
                        filtered_dirs.append(city_dir)
                except Exception as e:
                    print(
                        f"  Warning: Could not check {os.path.basename(city_dir)}: {e}"
                    )
        city_dirs = filtered_dirs
        print(f"Found {len(city_dirs)} Canadian cities")
    else:
        print(f"\nFound {len(city_dirs)} cities to process")

    if not city_dirs:
        print("\n⚠️  No cities found to process!")
        spark.stop()
        return

    # Process each city
    for city_dir in sorted(city_dirs):
        convert_city_to_parquet(spark, city_dir, output_base_dir)

    # Stop Spark
    spark.stop()

    print(f"\n{'=' * 60}")
    print("✅ Conversion complete!")
    print(f"Parquet files saved to: {output_base_dir}")
    print(f"{'=' * 60}")

    # Print summary of output structure
    print("\nOutput structure:")
    print(f"{output_base_dir}/")
    print("  ├── Toronto/")
    print("  │   ├── business.parquet/")
    print("  │   ├── review.parquet/")
    print("  │   ├── user.parquet/")
    print("  │   └── ...")
    print("  ├── Montreal/")
    print("  └── ...")


if __name__ == "__main__":
    main()
