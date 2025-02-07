# src/spark_transform.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col
import os

def spark_transform(input_path, output_path):
    """
    Reads merged CSV from input_path (S3), applies transformations, writes Parquet to output_path.
    """
    # Create Spark session
    spark = SparkSession.builder.appName("NDIS_Spark_Transform").getOrCreate()

    # 1. Read
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # 2. Basic transformations: trim spaces, drop duplicates
    for c in df.columns:
        df = df.withColumn(c, trim(col(c)))
    df = df.dropDuplicates()

    # (Optional) show first few rows for debugging
    df.show(5)

    # 3. Write as Parquet
    df.write.mode("overwrite").parquet(output_path)

    print(f"Spark transform complete! Data written to: {output_path}")
    spark.stop()

if __name__ == "__main__":
    # Example usage from environment variables (or replace directly)
    processed_bucket = os.getenv("PROCESSED_BUCKET", "<YOUR_PROCESSED_BUCKET>")

    input_s3_uri = f"s3a://{processed_bucket}/ndis/processed_data/merged_ndis_data.csv"
    output_s3_uri = f"s3a://{processed_bucket}/ndis/transformed_data/parquet/"

    spark_transform(input_s3_uri, output_s3_uri)
