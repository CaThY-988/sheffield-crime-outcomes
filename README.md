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



## 🚀 Reproducibility Guide

This project is fully reproducible end-to-end using Terraform, Docker, Airflow, dbt, and Streamlit.

---

### ✅ Prerequisites

Make sure you have the following installed:

- Docker (with Docker Compose)
- Terraform
- Make (pre-installed on most macOS/Linux systems)
- An AWS account with credentials
- A Databricks workspace with:
  - SQL Warehouse
  - Access token

---

### 🔐 Environment Setup

Clone the repository:

```bash
git clone https://github.com/CaThY-988/sheffield-crime-outcomes.git
cd sheffield-crime-outcomes
```

Create your environment variables file:

```bash
cp .env.example .env
```

Update `.env` with your credentials:

```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=eu-west-2
AWS_BUCKET_NAME=...

DATABRICKS_HOST=...
DATABRICKS_HTTP_PATH=...
DATABRICKS_TOKEN=...
```

---

### 🏗️ Provision Infrastructure (Terraform)

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# fill in required values
cd ..

bash scripts/terraform_deploy.sh
```

This will:
- Create an S3 bucket
- Configure Databricks external location and permissions

---

### ▶️ Run the Pipeline

Start all services and trigger the pipeline:

```bash
make run
```

This will:
- Start Airflow and Streamlit via Docker Compose
- Create an Airflow admin user (`admin / admin`)
- Unpause the DAG
- Trigger the pipeline

---

### 📊 Access the Interfaces

- **Airflow UI**: http://localhost:8080  
  Username: `admin`  
  Password: `admin`

- **Streamlit App**: http://localhost:8501  

---

### 🔍 Monitoring the Pipeline

**Airflow UI (recommended)**  
1. Open the DAG: `sheffield_crime_pipeline`  
2. Click on a task  
3. View logs to see detailed progress  

**Terminal logs**

```bash
make logs
```

---

### 🔄 Re-running the Pipeline

To trigger the pipeline again:

```bash
make trigger
```

---

### 🧹 Resetting the Environment (optional)

Stop all services:

```bash
make down
```

Full reset (including Airflow state):

```bash
docker compose down -v
```

---

### 🧠 Notes

- The pipeline is scheduled to run **monthly** via Airflow  
- `make run` performs an initial trigger for convenience  
- Future runs are handled automatically by Airflow  
- Task-level logs are available in the Airflow UI  
