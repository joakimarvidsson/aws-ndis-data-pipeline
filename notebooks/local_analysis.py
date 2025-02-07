# local_analysis.py

from pyspark.sql import SparkSession

def main():
    # 1. Create Spark session
    spark = (SparkSession.builder
             .appName("NDISLocalAnalysis")
             .getOrCreate())
    
    # 2. Read Parquet from S3
    #    EXACT path - we assume your bucket is called ndis-pipeline-processed-bucket
    #    and you have "ndis/transformed_data/parquet/" prefix containing .parquet files
    input_path = "s3a://ndis-pipeline-processed-bucket/ndis/transformed_data/parquet/"
    print(f"Reading Parquet from {input_path} ...")
    
    df = spark.read.parquet(input_path)
    print("Schema:")
    df.printSchema()

    print("Showing first 5 rows:")
    df.show(5)

    # Simple transformation: count total rows
    total_rows = df.count()
    print(f"Total rows in the dataset: {total_rows}")

    spark.stop()

if __name__ == "__main__":
    main()
