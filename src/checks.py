import os
import tempfile
from pathlib import Path

import boto3
import psycopg
import pandas as pd

from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

OUTPUTS_DIR = PROJECT_ROOT / "outputs"
OUTPUTS_DIR.mkdir(exist_ok=True)

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "toko"),
    "user": os.getenv("POSTGRES_USER", "toko"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9007")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio"))
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", ""))

CURATED_BUCKET = os.getenv("MINIO_CURATED_BUCKET", "toko-curated")


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )


def get_source_metrics():
    query = """
        SELECT
            COUNT(*) FILTER (
                WHERE UPPER(TRIM(status)) = 'PAID'
                  AND order_id IS NOT NULL
                  AND TRIM(order_id) <> ''
                  AND product_id IS NOT NULL
                  AND TRIM(product_id) <> ''
                  AND qty > 0
                  AND unit_price IN (25000, 50000, 75000, 120000, 250000)
                  AND order_ts IS NOT NULL
            ) AS total_orders,

            COALESCE(SUM(qty) FILTER (
                WHERE UPPER(TRIM(status)) = 'PAID'
                  AND order_id IS NOT NULL
                  AND TRIM(order_id) <> ''
                  AND product_id IS NOT NULL
                  AND TRIM(product_id) <> ''
                  AND qty > 0
                  AND unit_price IN (25000, 50000, 75000, 120000, 250000)
                  AND order_ts IS NOT NULL
            ), 0) AS total_items,

            COALESCE(SUM(qty * unit_price) FILTER (
                WHERE UPPER(TRIM(status)) = 'PAID'
                  AND order_id IS NOT NULL
                  AND TRIM(order_id) <> ''
                  AND product_id IS NOT NULL
                  AND TRIM(product_id) <> ''
                  AND qty > 0
                  AND unit_price IN (25000, 50000, 75000, 120000, 250000)
                  AND order_ts IS NOT NULL
            ), 0) AS total_revenue,

            COUNT(*) FILTER (
                WHERE NOT (
                    order_id IS NOT NULL
                    AND TRIM(order_id) <> ''
                    AND product_id IS NOT NULL
                    AND TRIM(product_id) <> ''
                    AND qty > 0
                    AND unit_price IN (25000, 50000, 75000, 120000, 250000)
                    AND UPPER(TRIM(status)) IN ('PAID', 'CANCELLED')
                    AND order_ts IS NOT NULL
                )
            ) AS rejected_rows
        FROM orders
        WHERE order_id <> 'MP7_INC_TEST_001'
    """

    with psycopg.connect(**PG_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()

    return {
        "total_orders": int(row[0]),
        "total_items": int(row[1]),
        "total_revenue": float(row[2]),
        "rejected_rows": int(row[3]),
    }


def download_curated(s3, prefix, local_dir):
    paginator = s3.get_paginator("list_objects_v2")

    count = 0

    for page in paginator.paginate(Bucket=CURATED_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".parquet"):
                continue

            filename = f"{count}.parquet"
            s3.download_file(CURATED_BUCKET, key, str(local_dir / filename))
            count += 1

    if count == 0:
        raise RuntimeError(f"Tidak ada Parquet pada {prefix}")

    return count


def get_lake_metrics(spark, s3, temp_dir):

    daily_dir = temp_dir / "daily"
    product_dir = temp_dir / "product"

    daily_dir.mkdir()
    product_dir.mkdir()

    download_curated(s3, "daily_sales/", daily_dir)
    download_curated(s3, "product_sales/", product_dir)

    daily_df = spark.read.parquet(str(daily_dir))
    product_df = spark.read.parquet(str(product_dir))

    daily_metrics = daily_df.agg(
        F.sum("order_count").alias("orders"),
        F.sum("item_count").alias("items"),
        F.sum("revenue").alias("revenue"),
    ).first()

    product_metrics = product_df.agg(
        F.sum("order_count").alias("orders"),
        F.sum("item_count").alias("items"),
        F.sum("revenue").alias("revenue"),
    ).first()

    # Pastikan kedua tabel Curated konsisten
    if (
        daily_metrics["orders"] != product_metrics["orders"]
        or daily_metrics["items"] != product_metrics["items"]
        or daily_metrics["revenue"] != product_metrics["revenue"]
    ):
        raise RuntimeError("Daily sales dan product sales tidak konsisten.")

    return {
        "total_orders": int(daily_metrics["orders"]),
        "total_items": int(daily_metrics["items"]),
        "total_revenue": float(daily_metrics["revenue"]),
    }


def run_checks():

    print("Starting reconciliation...")

    source = get_source_metrics()

    s3 = get_s3()
    spark = (
        SparkSession.builder.appName("TokoBandung-Reconciliation")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        with tempfile.TemporaryDirectory() as temp:
            lake = get_lake_metrics(spark, s3, Path(temp))

        lake["rejected_rows"] = int(
            pd.read_csv(OUTPUTS_DIR / "quality_report.csv")["rejected_rows"].iloc[0]
        )

        rows = []

        for metric in ["total_orders", "total_items", "total_revenue", "rejected_rows"]:
            source_value = source[metric]
            lake_value = lake[metric]
            diff = source_value - lake_value

            rows.append(
                {
                    "metric": metric,
                    "source_value": source_value,
                    "lake_value": lake_value,
                    "diff": diff,
                    "status": "PASS" if abs(diff) < 0.000001 else "FAIL",
                }
            )

        report = pd.DataFrame(rows)

        report.to_csv(OUTPUTS_DIR / "reconciliation.csv", index=False)

        print("\nRECONCILIATION RESULT")
        print(report.to_string(index=False))

        if not (report["status"] == "PASS").all():
            raise RuntimeError("Reconciliation gagal!")

        print("\nAll reconciliation checks PASSED.")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_checks()
