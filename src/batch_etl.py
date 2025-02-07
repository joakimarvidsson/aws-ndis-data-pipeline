import requests
import boto3
import pandas as pd
from io import StringIO
import os

# -----------------------------------------------------------------------------
# 1) Three CSV links from dataresearch.ndis.gov.au (30 June 2024 data)
# -----------------------------------------------------------------------------
DATA_URLS = {
    # A6: Participant characteristics data at 30 June 2024
    "participants_june_2024": (
        "https://dataresearch.ndis.gov.au/media/4144/download?attachment"
    ),
    # D1: Active providers at 30 June 2024
    "providers_june_2024": (
        "https://dataresearch.ndis.gov.au/media/4147/download?attachment"
    ),
    # E1: Total payments June 2024
    "payments_june_2024": (
        "https://dataresearch.ndis.gov.au/media/4021/download?attachment"
    )
}

def download_csv_from_url(url):
    """
    Download CSV from a public URL and return its content as bytes.
    Raise an exception if the request fails.
    """
    print(f"Downloading dataset from: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def upload_to_s3(content_bytes, bucket_name, object_key):
    """
    Upload bytes to S3 as an object with the given key.
    """
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=content_bytes
    )
    print(f"Uploaded {object_key} to s3://{bucket_name}")

def run_batch_etl():
    # -----------------------------------------------------------------------------
    # 2) Set your S3 bucket names via environment variables or replace directly here.
    # -----------------------------------------------------------------------------
    raw_bucket = os.getenv("RAW_BUCKET", "<YOUR_RAW_BUCKET>")
    processed_bucket = os.getenv("PROCESSED_BUCKET", "<YOUR_PROCESSED_BUCKET>")

    # Folders in S3 where data will be stored
    raw_folder = "ndis/raw_data"          # e.g. s3://my-raw-bucket/ndis/raw_data/
    processed_folder = "ndis/processed_data"

    # Merged CSV filename
    merged_filename = "merged_ndis_data.csv"

    # Container for the DataFrames we’ll merge
    dataframes = []

    # -----------------------------------------------------------------------------
    # 3) Download each CSV, upload it to raw S3, then load into Pandas
    # -----------------------------------------------------------------------------
    for dataset_name, url in DATA_URLS.items():
        try:
            csv_bytes = download_csv_from_url(url)
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {dataset_name}: {e}")
            continue

        # Construct the S3 key for raw CSV (e.g., "ndis/raw_data/participants_june_2023.csv")
        raw_key = f"{raw_folder}/{dataset_name}.csv"

        # Upload raw CSV to S3
        upload_to_s3(csv_bytes, raw_bucket, raw_key)

        # Read raw CSV from S3 into Pandas
        s3 = boto3.client('s3')
        raw_obj = s3.get_object(Bucket=raw_bucket, Key=raw_key)
        raw_data_str = raw_obj['Body'].read().decode('utf-8')

        df = pd.read_csv(StringIO(raw_data_str))

        # Basic cleanup: rename columns, drop empty rows
        df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]
        df.dropna(how="all", inplace=True)

        # Tag the dataset source
        df["source_dataset"] = dataset_name

        dataframes.append(df)

    # -----------------------------------------------------------------------------
    # 4) Merge all DataFrames into one (if any were downloaded)
    # -----------------------------------------------------------------------------
    if not dataframes:
        print("No dataframes were downloaded. Exiting.")
        return

    merged_df = pd.concat(dataframes, ignore_index=True, sort=False)

    # Example transformation: fill missing numeric fields with 0
    for col in merged_df.select_dtypes(["float", "int"]).columns:
        merged_df[col].fillna(0, inplace=True)

    # -----------------------------------------------------------------------------
    # 5) Upload the merged CSV to processed S3
    # -----------------------------------------------------------------------------
    output_buffer = StringIO()
    merged_df.to_csv(output_buffer, index=False)
    merged_csv_bytes = output_buffer.getvalue().encode('utf-8')

    merged_key = f"{processed_folder}/{merged_filename}"
    upload_to_s3(merged_csv_bytes, processed_bucket, merged_key)

    print(f"Batch ETL complete! Merged CSV at s3://{processed_bucket}/{merged_key}")

if __name__ == "__main__":
    run_batch_etl()
