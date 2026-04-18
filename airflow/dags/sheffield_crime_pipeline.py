from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="sheffield_crime_pipeline",
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2027, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["crime", "databricks", "dbt"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_data",
        bash_command="""
            set -e
            echo "🚀 Starting ingest..."
            cd /opt/airflow
            python app/ingest.py
            echo "✅ Ingest complete"
        """,
    )

    load_to_databricks = BashOperator(
        task_id="load_to_databricks",
        bash_command="""
            set -e
            echo "🚀 ]Starting Databricks load..."
            cd /opt/airflow
            python app/load_to_databricks.py
            echo "✅ Databricks load complete"
        """,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="""
            set -e
            echo "🚀 Starting dbt build..."
            cd /opt/airflow
            dbt build --project-dir /opt/airflow/dbt_police_data --profiles-dir /opt/airflow/dbt_police_data
            echo "✅ dbt build complete"
        """,
    )

    done = BashOperator(
        task_id="done",
        bash_command='echo "🎉 Pipeline finished successfully."',
    )

    ingest >> load_to_databricks >> dbt_build >> done