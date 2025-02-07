# aws-ndis-data-pipeline
Data engineering pipeline demo with Terraform, AWS, Databricks, and NDIS data.

# AWS NDIS Data Pipeline

This repository demonstrates a simple data engineering pipeline using:
- **Terraform** for AWS infrastructure
- **Databricks** for big-data processing
- A real open dataset (from [data.gov.au](https://data.gov.au/)) related to the NDIS

## Folder Structure
- `infrastructure/` - Terraform `.tf` files
- `data/` - Sample CSV files (optional local storage)
- `src/` - Python scripts for ETL, streaming, transformations
- `notebooks/` - Jupyter/Databricks notebooks
