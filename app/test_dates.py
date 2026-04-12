import pandas as pd


# Add to ingest.py
dates = pd.date_range(start="2025-01-01", end=pd.Timestamp.today(), freq="MS") \
           .strftime("%Y-%m") \
           .tolist()

# Add to load_to_databricks.py
year_months = [
    (str(d.year), f"{d.month:02d}")
    for d in pd.date_range("2025-01-01", pd.Timestamp.today(), freq="MS")
]

ddl_statements = []

for year, month in year_months:
    ddl_statements.extend([f"dataset_data_{year}_{month}"])

print(ddl_statements)