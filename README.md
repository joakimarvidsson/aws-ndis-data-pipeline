# AWS NDIS Data Pipeline

This repository showcases a very simple end-to-end data engineering project using AWS, Spark, Databricks, and Tableau to process and analyze open data related to the National Disability Insurance Scheme (NDIS). The project demonstrates:

1. **Infrastructure as Code** (Terraform or manual AWS setup) to create Amazon S3 buckets for raw and processed data.  
2. **Batch ETL** scripts in Python to ingest, clean, and transform NDIS-related CSVs from data.gov.au into a unified dataset.  
3. **Spark & Databricks** transformations for large-scale analytics, including a small predictive model using Spark MLlib.  
4. **Dashboards** in both Databricks built-in visualizations and Tableau for final data presentation.

---

## Architecture Overview

```text
             +---------------+
             |   data.gov.au  |
             | (NDIS Datasets)|
             +-------+-------+
                     |
                     v (CSV)
+-------------------+   +--------------+   +---------------------+   +---------------------+
| Terraform/Manual |   | AWS S3 (Raw)  |-->| Python ETL / Spark  |-->| AWS S3 (Processed)  |
|   AWS Setup      |   | e.g. raw_...  |   | Transformations     |   | e.g. processed_...  |
+-------------------+   +--------------+   +---------------------+   +---------------------+
      | (Parquet)
      v
Databricks or Local
Spark ML + Dashboard
(Predictive Model)


1. Data is downloaded from public NDIS data sources on data.gov.au.  
2. Batch ETL Python scripts ingest data into a raw S3 bucket.  
3. Transformation jobs (Python or Spark) clean & unify data in a processed S3 bucket.  
4. Optionally, a predictive model is built with Spark ML, and dashboards are generated in Databricks or Tableau.
```
---

## Prerequisites

1. **AWS Account** with permission to create S3 buckets and IAM roles/policies.  
2. **Terraform** (optional) for automated infrastructure. Alternatively, you can manually create the S3 buckets.  
3. **Python 3.8+**  
4. **PySpark** (if running transformations locally) or a **Databricks** workspace.  
5. **Tableau** (optional) if you want to build dashboards externally.  
6. **Git** to clone and push this repository.

---

## Infrastructure Deployment

1. **Clone** this repository:
   ```bash
   git clone https://github.com/<YourUsername>/aws-ndis-data-pipeline.git
   cd aws-ndis-data-pipeline
   ```

2.	If using Terraform, navigate to the infrastructure folder:
   ```bash
  cd infrastructure
  terraform init
  terraform plan
  terraform apply
```

## Data Ingestion & ETL

1.	Install Python dependencies:
  ```bash
  pip install -r requirements.txt
```

2.	Run the batch ETL script:
```bash
cd src
python batch_etl.py
```
This script downloads CSV from [data.gov.au](https://dataresearch.ndis.gov.au/), uploads raw data to the raw data bucket, cleans/transforms with Pandas, then writes processed data to the processed bucket.

## Spark & Predictive Modeling

Local (PySpark)

1.	Ensure PySpark is installed:
  ```bash
  pip install pyspark
```
2.	Run the Spark transform:
  ```bash
  spark-submit spark_transform.py
```
Reads raw/processed S3 buckets, does large-scale transformations, and writes Parquet back to S3.

3.	Predictive Model
	•	For example, local_analysis.py or ndis_dashboard_and_prediction.ipynb demonstrates how to train a simple Spark ML regression (using columns with zero missing data).

