from pyspark.sql import SparkSession
import os


def create_spark_session():
    return (
        SparkSession.builder.appName("Yelp JSON to Parquet")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )


from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def convert_to_parquet(spark, input_path, output_path, partition_col=None, schema=None):
    if not os.path.exists(input_path):
        print(f"Skipping {os.path.basename(input_path)} (not found)")
        return

    print(f"Converting {os.path.basename(input_path)}...")
    try:
        if schema:
            df = spark.read.schema(schema).json(input_path)
        else:
            df = spark.read.json(input_path)
        
        if partition_col:
            if partition_col == "year" and "date" in df.columns:
                df = df.withColumn("year", df.date.substr(1, 4))
        
        writer = df.write.mode("overwrite")
        
        if partition_col:
            writer = writer.partitionBy(partition_col)
            
        writer.parquet(output_path)
        print(f"Saved {output_path} ({df.count()} records)")
    except Exception as e:
        print(f"Error converting {os.path.basename(input_path)}: {e}")


def process_city(spark, city_dir, output_base_dir):
    city_name = os.path.basename(city_dir)
    print(f"\nProcessing city: {city_name}")
    
    output_city_dir = os.path.join(output_base_dir, city_name)
    
    # Define explicit schema for reviews to avoid costly inference
    review_schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("stars", DoubleType(), True),
        StructField("useful", DoubleType(), True),
        StructField("funny", DoubleType(), True),
        StructField("cool", DoubleType(), True),
        StructField("text", StringType(), True),
        StructField("date", StringType(), True)
    ])
    
    # File mapping: (json_file, parquet_name, partition_column, schema)
    files = [
        ("business.json", "business.parquet", "state", None),
        ("review.json", "review.parquet", "year", review_schema),
        ("user.json", "user.parquet", None, None),
        ("tip.json", "tip.parquet", None, None),
        ("checkin.json", "checkin.parquet", None, None),
        ("photo.json", "photo.parquet", "label", None),
    ]

    for json_file, parquet_name, part_col, schema in files:
        convert_to_parquet(
            spark, 
            os.path.join(city_dir, json_file),
            os.path.join(output_city_dir, parquet_name),
            part_col,
            schema
        )


def main():
    base_dir = "/Users/aarish/Documents/MSCompSci/CMPT732/project/test"
    input_base_dir = os.path.join(base_dir, "yelp_by_city")
    output_base_dir = os.path.join(base_dir, "yelp_parquet")
    
    us_states = {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    }

    print("Starting Spark session...")
    spark = create_spark_session()

    # Find US cities
    print("Scanning for US cities...")
    city_dirs = []
    for d in os.listdir(input_base_dir):
        path = os.path.join(input_base_dir, d)
        if not os.path.isdir(path):
            continue
            
        biz_file = os.path.join(path, "business.json")
        if os.path.exists(biz_file):
            try:
                # Check first record for state
                row = spark.read.json(biz_file).limit(1).collect()
                if row and row[0]["state"] in us_states:
                    city_dirs.append(path)
            except Exception:
                pass

    print(f"Found {len(city_dirs)} US cities")

    for city_dir in sorted(city_dirs):
        process_city(spark, city_dir, output_base_dir)

    spark.stop()
    print("\nConversion complete.")


if __name__ == "__main__":
    main()
