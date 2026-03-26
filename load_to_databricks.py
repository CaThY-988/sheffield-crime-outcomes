import os
from pathlib import Path

from dotenv import load_dotenv
from databricks import sql

load_dotenv(Path(__file__).resolve().parent / ".env")

year = "2025"
month = "01"
datasets = ["crime", "outcome"]

ddl_statements = []

for dataset in datasets:
    ddl_statements.extend([
        f"DROP TABLE IF EXISTS workspace.src_police.{dataset}_data_{year}_{month}",
        f"""
        CREATE TABLE workspace.src_police.{dataset}_data_{year}_{month}
        USING JSON
        LOCATION 's3://zoomcamp--497675597195-eu-west-2-an/police/raw/{dataset}_data/date={year}-{month}/{dataset}_data_{year}-{month}.json'
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