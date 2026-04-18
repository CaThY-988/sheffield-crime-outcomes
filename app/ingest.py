import requests
import time
import json
import os
import boto3
from dotenv import load_dotenv
from date_utils import iter_complete_months

load_dotenv()

# Sheffield
LAT = 53.3811
LNG = -1.4701
LAT_RANGE = 0.03
LNG_RANGE = 0.05

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

dates = iter_complete_months("2025-01-01")

def make_rect_poly(lat: float, lng: float, lat_range: float, lng_range: float) -> str:
    lat_min = lat - lat_range
    lat_max = lat + lat_range
    lng_min = lng - lng_range
    lng_max = lng + lng_range

    return (
        f"{lat_min},{lng_min}:"
        f"{lat_max},{lng_min}:"
        f"{lat_max},{lng_max}:"
        f"{lat_min},{lng_max}"
    )

def main() -> None:
    if not BUCKET_NAME:
        raise ValueError("AWS_BUCKET_NAME is not set")
    s3 = boto3.client("s3")

    poly = make_rect_poly(LAT, LNG, LAT_RANGE, LNG_RANGE)

    for date in dates:
        for dataset in datasets:
            url = f"https://data.police.uk/api/{dataset['endpoint']}"
            s3_key = f"police/raw/{dataset['name']}/date={date}/{dataset['name']}_{date}.json"

            params = {
                "poly": poly,
                "date": date
            }

            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 429:
                print("Hit rate limit, sleeping for 10 seconds and retrying once...")
                time.sleep(10)
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

if __name__ == "__main__":
    main()