import os
from datetime import datetime, timedelta, timezone

import requests
from pymongo import MongoClient, UpdateOne

try:                                   
    from airflow.sdk import dag, task
except ImportError:                 
    from airflow.decorators import dag, task

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY", "PKR", "AUD", "CAD"]

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongodb:27017/")
MONGO_DB = "currency_db"
MONGO_COLLECTION = "exchange_rates"

# XCom keys (the "names" in named XCom)
XCOM_RAW = "raw_exchange_rates"
XCOM_TRANSFORMED = "transformed_exchange_rates"


@dag(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates from Frankfurter API into MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    @task
    def extract_exchange_rates(**context):
        """Call the API and push the raw response under a named XCom key."""
        params = {"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)}
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()

        payload = {
            "retrieved_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "data": response.json(),
        }
        context["ti"].xcom_push(key=XCOM_RAW, value=payload)
        print(f"Extracted {len(payload['data'])} rows from {response.url}")

    @task
    def transform_exchange_rates(**context):
        """Pull raw data, reshape each row into the required document format."""
        ti = context["ti"]
        raw = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RAW)
        if not raw:
            raise ValueError("No raw data found in XCom")

        rows = raw["data"]
        # v1-style response safety net: {"base":..,"date":..,"rates":{..}}
        if isinstance(rows, dict) and "rates" in rows:
            rows = [{"base": rows["base"], "date": rows["date"],
                     "quote": q, "rate": r} for q, r in rows["rates"].items()]

        transformed = []
        for row in rows:
            rate = row.get("rate")
            if rate is None or float(rate) <= 0:      # basic data-quality check
                continue
            transformed.append({
                "base_currency": row["base"].upper(),
                "target_currency": row["quote"].upper(),
                "exchange_rate": round(float(rate), 4),
                "rate_date": row["date"],
                "source": "Frankfurter API",
                "retrieved_at": raw["retrieved_at"],
            })

        ti.xcom_push(key=XCOM_TRANSFORMED, value=transformed)
        print(f"Transformed {len(transformed)} records, e.g. {transformed[:1]}")

    @task
    def load_to_mongodb(**context):
        """Pull transformed records and upsert them into MongoDB."""
        records = context["ti"].xcom_pull(
            task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED
        )
        if not records:
            raise ValueError("No transformed data found in XCom")

        client = MongoClient(MONGO_URI)
        try:
            collection = client[MONGO_DB][MONGO_COLLECTION]
            # One document per currency pair per day -> re-runs don't duplicate
            collection.create_index(
                [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
                unique=True,
            )
            ops = [
                UpdateOne(
                    {"base_currency": r["base_currency"],
                     "target_currency": r["target_currency"],
                     "rate_date": r["rate_date"]},
                    {"$set": r},
                    upsert=True,
                )
                for r in records
            ]
            result = collection.bulk_write(ops)
            print(f"Inserted {result.upserted_count}, updated {result.modified_count} "
                  f"in {MONGO_DB}.{MONGO_COLLECTION}")
        finally:
            client.close()

    extract_exchange_rates() >> transform_exchange_rates() >> load_to_mongodb()


currency_exchange_etl()