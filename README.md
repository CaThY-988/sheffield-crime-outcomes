# sheffield-crime-outcomes
Data Zoomcamp Final Project Space (Batch)

Dataset can be found at: https://data.police.uk/docs


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

# Steps to reproduce

## Prerequisites
- Terraform installed
- Docker and Docker Compose installed
- Git installed
- AWS Account
- Databricks Account

1. Project Preparation
Clone the Project Repository

git clone https://github.com/CaThY-988/sheffield-crime-outcomes.git 

2. AWS Set UP
...

3. Databricks Set UP
...

4. Terraform Deployment

Configure your local terraform.tfvars (see terraform.tfvars.example)

cd sheffield-crime-outcomes/terraform
chmod +x deploy.sh
./deploy.sh

4. Start Kestra

