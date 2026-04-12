import os
from pathlib import Path

from dotenv import load_dotenv
from databricks import sql

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

years = ["2025"]
months = ["01", "02", "03"]
bucket = os.getenv("AWS_BUCKET_NAME")

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

ddl_statements = []

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

for year in years:
    for month in months:
        ingest_year_month = f"{year}-{month}"

        for dataset in datasets:
            dataset_name = dataset["name"]
            schema = dataset["schema"]
            transforms = dataset.get("transforms", {})

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

            source_path = (
                f"s3://{bucket}/police/raw/{dataset_name}/"
                f"date={ingest_year_month}/{dataset_name}_{ingest_year_month}.json"
            )

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