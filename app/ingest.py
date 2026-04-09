import requests
import json
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

# Sheffield city centre
LAT = 53.3811
LNG = -1.4701

# AWS Vars
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# 
datasets = [
    {
        "name": "crime_data",
        "endpoint": "crimes-street/all-crime"
    },
    {
        "name": "outcome_data",
        "endpoint": "outcomes-at-location"
    }
]

dates = ["2025-01", "2025-02"]

s3 = boto3.client("s3")

for date in dates:
    for dataset in datasets:
        url = f"https://data.police.uk/api/{dataset['endpoint']}"
        s3_key = f"police/raw/{dataset['name']}/date={date}/{dataset['name']}_{date}.json"

        params = {
            "lat": LAT,
            "lng": LNG,
            "date": date
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        json_content = json.dumps(data)

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json_content,
            ContentType="application/json"
        )

        print(f"Uploaded {dataset['name']} to s3://{BUCKET_NAME}/{s3_key}")