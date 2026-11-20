import os
import tempfile
from pathlib import Path
from datetime import datetime

import boto3
import pandas as pd

from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# =====================================================
# 1. CONFIGURATION
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9007")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio"))
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", ""))

RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "toko-raw")
PROCESSED_BUCKET = os.getenv("MINIO_PROCESSED_BUCKET", "toko-processed")

RAW_KEY = "orders/initial_load/orders.csv"

ALLOWED_PRICES = [25000, 50000, 75000, 120000, 250000]

OUTPUTS_DIR = PROJECT_ROOT / "outputs"
OUTPUTS_DIR.mkdir(exist_ok=True)


# =====================================================
# 2. MINIO CONNECTION
# =====================================================


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )


def upload_directory(s3, local_dir, bucket, prefix):
    local_dir = Path(local_dir)

    for file_path in local_dir.rglob("*"):
        if not file_path.is_file():
            continue

        # Skip hidden files and Spark checksum files
        if file_path.name.startswith(".") or file_path.name.endswith(".crc"):
            continue

        relative_path = file_path.relative_to(local_dir)
        object_key = f"{prefix}/{relative_path.as_posix()}"

        s3.upload_file(str(file_path), bucket, object_key)

        print(f"Uploaded: {object_key}")


# =====================================================
# 3. SPARK SESSION
# =====================================================


def create_spark_session():
    return (
        SparkSession.builder.appName("TokoBandung-Transform")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# =====================================================
# 4. MAIN TRANSFORMATION
# =====================================================


def run_transform():

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("Starting transformation...")
    print(f"Run ID: {run_id}")

    s3 = get_s3_client()

    spark = create_spark_session()

    try:
        with tempfile.TemporaryDirectory() as temp_dir:

            temp_dir = Path(temp_dir)

            raw_file = temp_dir / "orders.csv"

            # Download raw CSV from MinIO
            s3.download_file(RAW_BUCKET, RAW_KEY, str(raw_file))

            print("Raw data downloaded from MinIO.")

            # Read CSV with Spark
            orders_df = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(str(raw_file))
            )

            total_rows = orders_df.count()

            print(f"Input rows: {total_rows}")

            # Standardize status and numeric fields
            standardized_df = (
                orders_df.withColumn("status", F.upper(F.trim(F.col("status"))))
                .withColumn("qty", F.col("qty").cast("integer"))
                .withColumn("unit_price", F.col("unit_price").cast("double"))
            )

            # Deduplication: newest updated_at first
            window_spec = Window.partitionBy("order_id").orderBy(
                F.col("updated_at").desc(), F.col("order_ts").desc()
            )

            checked_df = standardized_df.withColumn(
                "_row_number", F.row_number().over(window_spec)
            ).withColumn(
                "reject_reason",
                F.concat_ws(
                    "; ",
                    F.when(
                        F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""),
                        "Missing order_id",
                    ),
                    F.when(
                        F.col("product_id").isNull()
                        | (F.trim(F.col("product_id")) == ""),
                        "Missing product_id",
                    ),
                    F.when(
                        F.col("qty").isNull() | (F.col("qty") <= 0), "Invalid quantity"
                    ),
                    F.when(
                        F.col("unit_price").isNull()
                        | (~F.col("unit_price").isin(ALLOWED_PRICES)),
                        "Invalid unit_price",
                    ),
                    F.when(
                        F.col("status").isNull()
                        | (~F.col("status").isin(["PAID", "CANCELLED"])),
                        "Unsupported status",
                    ),
                    F.when(F.col("order_ts").isNull(), "Missing order_ts"),
                    F.when(F.col("_row_number") > 1, "Duplicate order_id"),
                ),
            )

            valid_df = checked_df.filter(F.col("reject_reason") == "").drop(
                "_row_number", "reject_reason"
            )

            rejected_df = checked_df.filter(F.col("reject_reason") != "").drop(
                "_row_number"
            )

            valid_count = valid_df.count()
            rejected_count = rejected_df.count()

            print(f"Valid rows: {valid_count}")
            print(f"Rejected rows: {rejected_count}")

            # Quality report
            unsupported_status = rejected_df.filter(
                F.col("reject_reason").contains("Unsupported status")
            ).count()

            invalid_quantity = rejected_df.filter(
                F.col("reject_reason").contains("Invalid quantity")
            ).count()

            invalid_price = rejected_df.filter(
                F.col("reject_reason").contains("Invalid unit_price")
            ).count()

            duplicate_rows = rejected_df.filter(
                F.col("reject_reason").contains("Duplicate order_id")
            ).count()

            quality_report = pd.DataFrame(
                [
                    {
                        "total_input_rows": total_rows,
                        "valid_rows": valid_count,
                        "rejected_rows": rejected_count,
                        "unsupported_status": unsupported_status,
                        "invalid_quantity": invalid_quantity,
                        "invalid_unit_price": invalid_price,
                        "duplicate_rows": duplicate_rows,
                        "quarantine_location": (
                            f"s3://{PROCESSED_BUCKET}/quarantine/" f"run_id={run_id}/"
                        ),
                    }
                ]
            )

            quality_report.to_csv(OUTPUTS_DIR / "quality_report.csv", index=False)

            # Write Parquet locally
            processed_local = temp_dir / "processed"
            quarantine_local = temp_dir / "quarantine"

            valid_df.write.mode("overwrite").parquet(str(processed_local))

            rejected_df.write.mode("overwrite").parquet(str(quarantine_local))

            # Upload to MinIO
            processed_prefix = f"orders/run_id={run_id}"
            quarantine_prefix = f"quarantine/run_id={run_id}"

            upload_directory(s3, processed_local, PROCESSED_BUCKET, processed_prefix)

            upload_directory(s3, quarantine_local, PROCESSED_BUCKET, quarantine_prefix)

            print("\nTransformation completed!")
            print(quality_report.to_string(index=False))

    finally:
        spark.stop()


# =====================================================
# 5. COMMAND LINE
# =====================================================

if __name__ == "__main__":
    run_transform()
