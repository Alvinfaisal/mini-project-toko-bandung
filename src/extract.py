import os
import io
import csv
import json
import hashlib
import argparse

from datetime import datetime
from pathlib import Path

import boto3
import psycopg
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# =========================================================
# 1. LOAD CONFIGURATION
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

# PostgreSQL
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DATABASE = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9007")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD"))

RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "toko-raw")

WATERMARK_KEY = "checkpoints/orders_watermark.json"


# =========================================================
# 2. CREATE CONNECTIONS
# =========================================================


def get_postgres_connection():
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


# =========================================================
# 3. ENSURE RAW BUCKET EXISTS
# =========================================================


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=RAW_BUCKET)
        print(f"Bucket tersedia: {RAW_BUCKET}")

    except ClientError:
        s3.create_bucket(Bucket=RAW_BUCKET)
        print(f"Bucket dibuat: {RAW_BUCKET}")


# =========================================================
# 4. READ WATERMARK
# =========================================================


def read_watermark(s3):
    try:
        response = s3.get_object(Bucket=RAW_BUCKET, Key=WATERMARK_KEY)

        watermark = json.loads(response["Body"].read().decode("utf-8"))

        return watermark

    except ClientError as error:
        error_code = error.response["Error"].get("Code")

        if error_code in ("NoSuchKey", "404"):
            return None

        raise


# =========================================================
# 5. EXTRACT DATA FROM POSTGRES
# =========================================================


def extract_orders(mode, last_updated_at=None):

    query = """
        SELECT *
        FROM orders
    """

    params = ()

    if mode == "incremental":
        if last_updated_at is None:
            raise ValueError("Watermark belum tersedia. Jalankan initial load dahulu.")

        query += """
            WHERE updated_at > %s
        """

        params = (last_updated_at,)

    query += """
        ORDER BY updated_at, order_id
    """

    with get_postgres_connection() as conn:
        with conn.cursor() as cursor:

            cursor.execute(query, params)

            columns = [description.name for description in cursor.description]

            rows = cursor.fetchall()

            cursor.execute("SELECT COUNT(*) FROM orders")
            total_source_rows = cursor.fetchone()[0]

    return columns, rows, total_source_rows


# =========================================================
# 6. CONVERT DATA TO CSV
# =========================================================


def convert_to_csv(columns, rows):

    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)

    writer.writerow(columns)

    for row in rows:
        writer.writerow(
            [
                value.isoformat(sep=" ") if isinstance(value, datetime) else value
                for value in row
            ]
        )

    return buffer.getvalue().encode("utf-8")


# =========================================================
# 7. UPLOAD AND VERIFY RAW DATA
# =========================================================


def upload_and_verify(s3, object_key, csv_bytes, expected_rows):

    expected_checksum = hashlib.sha256(csv_bytes).hexdigest()

    s3.put_object(
        Bucket=RAW_BUCKET, Key=object_key, Body=csv_bytes, ContentType="text/csv"
    )

    print(f"Raw uploaded: {object_key}")

    # Download kembali object untuk verifikasi
    response = s3.get_object(Bucket=RAW_BUCKET, Key=object_key)

    downloaded_bytes = response["Body"].read()

    actual_checksum = hashlib.sha256(downloaded_bytes).hexdigest()

    # Hitung jumlah baris CSV, tidak termasuk header
    actual_rows = (
        sum(1 for _ in csv.reader(io.StringIO(downloaded_bytes.decode("utf-8")))) - 1
    )

    if actual_rows != expected_rows:
        raise RuntimeError(
            f"Row count mismatch: expected={expected_rows}, " f"actual={actual_rows}"
        )

    if actual_checksum != expected_checksum:
        raise RuntimeError("Checksum verification failed!")

    print(f"Row count verified: {actual_rows}")
    print("Checksum verified: MATCH")

    return actual_checksum


# =========================================================
# 8. UPDATE WATERMARK
# =========================================================


def update_watermark(s3, last_updated_at, total_source_rows, raw_key, checksum):

    watermark = {
        "last_updated_at": last_updated_at,
        "row_count": total_source_rows,
        "raw_key": raw_key,
        "sha256": checksum,
        "updated_at": datetime.now().isoformat(),
    }

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=WATERMARK_KEY,
        Body=json.dumps(watermark, indent=4).encode("utf-8"),
        ContentType="application/json",
    )

    print("Watermark berhasil diperbarui.")
    print(f"Last updated at: {last_updated_at}")


# =========================================================
# 9. MAIN PIPELINE
# =========================================================


def run_extract(mode):

    print(f"\nStarting extraction mode: {mode}")

    s3 = get_minio_client()
    ensure_bucket(s3)

    watermark = read_watermark(s3)

    last_updated_at = None

    if mode == "incremental":

        if watermark is None:
            raise RuntimeError(
                "Checkpoint belum tersedia. " "Jalankan initial load terlebih dahulu."
            )

        last_updated_at = datetime.fromisoformat(watermark["last_updated_at"])

        print(f"Previous watermark: {last_updated_at}")

    columns, rows, total_source_rows = extract_orders(mode, last_updated_at)

    print(f"Extracted rows: {len(rows)}")
    print(f"Total source rows: {total_source_rows}")

    if not rows:
        print("Tidak ada data baru. Watermark tidak diubah.")
        return

    csv_bytes = convert_to_csv(columns, rows)

    # Menentukan object key
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    if mode == "initial":
        object_key = "orders/initial_load/orders.csv"
    else:
        object_key = f"orders/incremental/batch_{batch_id}.csv"

    # Upload dan verifikasi dilakukan sebelum watermark
    checksum = upload_and_verify(s3, object_key, csv_bytes, len(rows))

    # Ambil updated_at terbaru dari batch
    updated_at_index = columns.index("updated_at")

    latest_updated_at = max(row[updated_at_index] for row in rows)

    update_watermark(
        s3,
        latest_updated_at.isoformat(sep=" "),
        total_source_rows,
        object_key,
        checksum,
    )

    print("\nExtraction completed successfully!")


# =========================================================
# 10. COMMAND LINE INTERFACE
# =========================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="PostgreSQL to MinIO Raw Extraction")

    parser.add_argument(
        "--mode", choices=["initial", "incremental"], default="incremental"
    )

    args = parser.parse_args()

    run_extract(args.mode)
