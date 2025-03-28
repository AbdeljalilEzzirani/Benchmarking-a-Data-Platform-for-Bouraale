import polars as pl
from pyspark.sql import SparkSession
import os

# Input files
files = {
    "SF10": "data_sets/measurements_sf10.txt",
    "SF50": "data_sets/measurements_sf50.txt",
    "SF100": "data_sets/measurements_sf100.txt"
}

# Output directories
output_dir = "data_sets/formats"
os.makedirs(output_dir, exist_ok=True)

# Spark session
spark = SparkSession.builder \
    .appName("ConvertFormats") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

for sf, file in files.items():
    # Read raw data
    df = spark.read.option("delimiter", ";").csv(file, schema="station STRING, temp FLOAT")
    
    # Parquet
    df.write.mode("overwrite").parquet(f"{output_dir}/{sf}_parquet")
    print(f"Converted {sf} to Parquet")
    
    # Delta Lake
    df.write.format("delta").mode("overwrite").save(f"{output_dir}/{sf}_delta")
    print(f"Converted {sf} to Delta Lake")
    
    # Iceberg (requires a catalog setup, simplified here)
    df.write.format("iceberg").mode("overwrite").save(f"{output_dir}/{sf}_iceberg")
    print(f"Converted {sf} to Iceberg")
    
    # ORC
    df.write.mode("overwrite").orc(f"{output_dir}/{sf}_orc")
    print(f"Converted {sf} to ORC")

spark.stop()