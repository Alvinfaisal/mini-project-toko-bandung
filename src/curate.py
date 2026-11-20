
import os
import tempfile
from pathlib import Path

import boto3

from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F


# =====================================================
# 1. CONFIGURATION
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9007")
ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio")
)
SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "")
)

PROCESSED_BUCKET = os.getenv(
    "MINIO_PROCESSED_BUCKET", "toko-processed"
)
CURATED_BUCKET = os.getenv(
    "MINIO_CURATED_BUCKET", "toko-curated"
)


# =====================================================
# 2. CONNECTION
# =====================================================

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1"
    )


def create_spark():
    return (
        SparkSession.builder
        .appName("TokoBandung-Curated")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# =====================================================
# 3. FIND LATEST PROCESSED RUN
# =====================================================

def find_latest_run(s3):

    response = s3.list_objects_v2(
        Bucket=PROCESSED_BUCKET,
        Prefix="orders/"
    )

    run_ids = set()

    for obj in response.get("Contents", []):
        key = obj["Key"]

        if "/run_id=" in key:
            run_id = key.split("/run_id=")[1].split("/")[0]
            run_ids.add(run_id)

    if not run_ids:
        raise RuntimeError("Tidak ditemukan data processed.")

    latest_run = sorted(run_ids)[-1]

    print(f"Latest processed run: {latest_run}")

    return f"orders/run_id={latest_run}/"


# =====================================================
# 4. DOWNLOAD PROCESSED PARQUET
# =====================================================

def download_processed(s3, prefix, local_dir):

    response = s3.list_objects_v2(
        Bucket=PROCESSED_BUCKET,
        Prefix=prefix
    )

    parquet_files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
        and not Path(obj["Key"]).name.startswith(".")
    ]

    if not parquet_files:
        raise RuntimeError("File Parquet processed tidak ditemukan.")

    for key in parquet_files:
        filename = Path(key).name

        s3.download_file(
            PROCESSED_BUCKET,
            key,
            str(local_dir / filename)
        )

    print(f"Downloaded {len(parquet_files)} Parquet file(s).")


# =====================================================
# 5. UPLOAD CURATED IDEMPOTENTLY
# =====================================================

def upload_curated(s3, local_dir, prefix):

    expected_objects = {}

    for file_path in Path(local_dir).rglob("*"):

        if not file_path.is_file():
            continue

        # Abaikan file hidden dan checksum Hadoop
        if file_path.name.startswith("."):
            continue

        if file_path.name.endswith(".crc"):
            continue

        relative_path = file_path.relative_to(local_dir)
        key = f"{prefix}/{relative_path.as_posix()}"

        expected_objects[key] = file_path

    # Hapus object lama hanya pada prefix tujuan ini
    paginator = s3.get_paginator("list_objects_v2")

    old_keys = []

    for page in paginator.paginate(
        Bucket=CURATED_BUCKET,
        Prefix=f"{prefix}/"
    ):
        for obj in page.get("Contents", []):
            if obj["Key"] not in expected_objects:
                old_keys.append(obj["Key"])

    # Upload object baru dengan key yang konsisten
    for key, file_path in expected_objects.items():
        s3.upload_file(
            str(file_path),
            CURATED_BUCKET,
            key
        )
        print(f"Uploaded: {key}")

    # Hapus object lama yang tidak lagi diperlukan
    for i in range(0, len(old_keys), 1000):
        batch = old_keys[i:i + 1000]

        s3.delete_objects(
            Bucket=CURATED_BUCKET,
            Delete={
                "Objects": [{"Key": key} for key in batch],
                "Quiet": True
            }
        )

    print(f"Total objects uploaded: {len(expected_objects)}")


# =====================================================
# 6. MAIN CURATION
# =====================================================

def run_curate():

    print("Starting curation...")

    s3 = get_s3_client()

    if not s3.list_buckets():
        raise RuntimeError("MinIO tidak dapat diakses.")

    spark = create_spark()

    try:
        with tempfile.TemporaryDirectory() as temp:

            temp_dir = Path(temp)

            source_dir = temp_dir / "processed"
            daily_dir = temp_dir / "daily_sales"
            product_dir = temp_dir / "product_sales"

            source_dir.mkdir()

            # Cari dan download processed terbaru
            processed_prefix = find_latest_run(s3)

            download_processed(
                s3,
                processed_prefix,
                source_dir
            )

            # Baca processed Parquet
            processed_df = spark.read.parquet(
                str(source_dir)
            )

            print(f"Processed rows: {processed_df.count()}")

            # Hanya transaksi PAID yang dihitung
            sales_df = (
                processed_df
                .filter(F.col("status") == "PAID")
                .withColumn(
                    "dt",
                    F.to_date(F.col("order_ts"))
                )
                .withColumn(
                    "revenue",
                    F.col("qty") * F.col("unit_price")
                )
            )

            # Daily sales
            daily_sales = (
                sales_df
                .groupBy("dt")
                .agg(
                    F.countDistinct("order_id").alias("order_count"),
                    F.sum("qty").alias("item_count"),
                    F.sum("revenue").alias("revenue")
                )
            )

            # Product sales
            product_sales = (
                sales_df
                .groupBy("product_id")
                .agg(
                    F.countDistinct("order_id").alias("order_count"),
                    F.sum("qty").alias("item_count"),
                    F.sum("revenue").alias("revenue")
                )
            )

            # Simpan Parquet lokal
            daily_sales.write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .parquet(str(daily_dir))

            product_sales.write \
                .mode("overwrite") \
                .parquet(str(product_dir))

            # Upload dengan object key konsisten
            upload_curated(
                s3,
                daily_dir,
                "daily_sales"
            )

            upload_curated(
                s3,
                product_dir,
                "product_sales"
            )

            print("\nCURATION COMPLETED")
            print(f"Daily sales rows: {daily_sales.count()}")
            print(f"Product sales rows: {product_sales.count()}")

            daily_sales.show(truncate=False)
            product_sales.show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_curate()
