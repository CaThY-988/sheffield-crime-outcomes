# sheffield-crime-outcomes
Data Zoomcamp Final Project Space (Batch)

Dataset can be found at: https://data.police.uk/docs/

## Reproducibility 

### Prerequisites

- AWS Account
- Databricks Account

### .env file
AWS_ACCESS_KEY_ID={your access key}
AWS_SECRET_ACCESS_KEY={your secret}
AWS_DEFAULT_REGION={your region e.g. eu-west-2}

DATABRICKS_HOST={your databricks host}
DATABRICKS_HTTP_PATH ={your http path}
DATABRICKS_TOKEN={your access token}

### Clone repo

Contents: 
- ingest.py - takes data from https://data.police.uk/ API and loads to S3 bucket with simple partitions
- load_to_databricks.py - moves data from S3 to databricks


