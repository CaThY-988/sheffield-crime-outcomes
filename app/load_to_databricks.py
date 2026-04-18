import json
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv
from databricks import sql

from date_utils import iter_complete_months

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

months = iter_complete_months("2025-01-01")
bucket = os.getenv("AWS_BUCKET_NAME")

if not bucket:
    raise ValueError("AWS_BUCKET_NAME is not set")

s3 = boto3.client("s3")

datasets = [
    {
        "name": "crime_data",
        "schema": {
            "category": "STRING",
            "context": "STRING",
            "id": "BIGINT",
            "location": "STRING",
            "location_subtype": "STRING",
            "location_type": "STRING",
            "month": "STRING",
            "outcome_status": "STRING",
            "persistent_id": "STRING",
            "ingest_year_month": "STRING",
            "loaded_at": "TIMESTAMP",
        },
        "transforms": {
            "location": "to_json(location)",
            "outcome_status": "to_json(outcome_status)",
        },
    },
    {
        "name": "outcome_data",
        "schema": {
            "category": "STRING",
            "crime": "STRING",
            "date": "STRING",
            "person_id": "STRING",
            "ingest_year_month": "STRING",
            "loaded_at": "TIMESTAMP",
        },
        "transforms": {
            "category": "to_json(category)",
            "crime": "to_json(crime)",
        },
    },
]


def s3_json_has_rows(bucket: str, key: str) -> bool:
    """
    Return True if the S3 JSON object exists and contains at least one record.
    Assumes the file content is a JSON array, e.g. [] or [{...}, {...}].
    """
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        return False

    body = response["Body"].read().decode("utf-8").strip()
    if not body:
        return False

    data = json.loads(body)
    return isinstance(data, list) and len(data) > 0


ddl_statements = [
    """
    CREATE SCHEMA IF NOT EXISTS workspace.src_police
    """
]

for dataset in datasets:
    schema = dataset["schema"]

    columns_ddl = ",\n    ".join(
        f"{col} {dtype}" for col, dtype in schema.items()
    )

    ddl_statements.append(
        f"""
        CREATE TABLE IF NOT EXISTS workspace.src_police.{dataset['name']} (
            {columns_ddl}
        )
        USING DELTA
        CLUSTER BY AUTO
        """
    )

for ingest_year_month in months:
    for dataset in datasets:
        dataset_name = dataset["name"]
        schema = dataset["schema"]
        transforms = dataset.get("transforms", {})

        s3_key = (
            f"police/raw/{dataset_name}/"
            f"date={ingest_year_month}/{dataset_name}_{ingest_year_month}.json"
        )

        if not s3_json_has_rows(bucket, s3_key):
            print(
                f"Skipping {dataset_name} for {ingest_year_month}: "
                f"source file missing or empty at s3://{bucket}/{s3_key}"
            )
            continue

        select_columns = []
        for col in schema.keys():
            if col == "ingest_year_month":
                select_columns.append(f"'{ingest_year_month}' AS ingest_year_month")
            elif col == "loaded_at":
                select_columns.append("current_timestamp() AS loaded_at")
            elif col in transforms:
                select_columns.append(f"{transforms[col]} AS {col}")
            else:
                select_columns.append(col)

        columns_sql = ",\n    ".join(select_columns)

        source_path = f"s3://{bucket}/{s3_key}"

        ddl_statements.extend([
            f"""
            DELETE FROM workspace.src_police.{dataset_name}
            WHERE ingest_year_month = '{ingest_year_month}'
            """,
            f"""
            INSERT INTO workspace.src_police.{dataset_name}
            SELECT
                {columns_sql}
            FROM json.`{source_path}`
            """
        ])


def main() -> None:
    conn = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )

    try:
        with conn.cursor() as cursor:
            for statement in ddl_statements:
                cursor.execute(statement)
                print(f"Executed: {statement}")
    finally:
        conn.close()

    print("Databricks raw tables refreshed successfully.")


if __name__ == "__main__":
    main()