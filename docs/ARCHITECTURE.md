# Architecture - Toko Bandung

## 1. Overview
Pipeline menggunakan PostgreSQL sebagai source database,
MinIO sebagai data lake, dan PySpark untuk transformasi.

## 2. Data Flow

```text
PostgreSQL (orders)
       |
       | Initial / Incremental Extraction
       | updated_at watermark
       v
MinIO - toko-raw
       |
       | Download raw object to local temporary file
       v
PySpark - Standardization & Quality Gate
       |
       +--------------------+
       |                    |
       v                    v
MinIO - toko-processed   MinIO - quarantine
       |
       | Aggregation
       v
MinIO - toko-curated
       |
       +-----------------------+
       |                       |
       v                       v
   daily_sales             product_sales
```

## 3. Data Lake Zones

### Raw
Menyimpan data sumber dari PostgreSQL, file awal, metadata, dan checkpoint watermark.

### Processed
Menyimpan data yang telah distandardisasi dan lolos quality gate dalam format Parquet.

### Quarantine
Menyimpan data yang gagal validasi beserta alasan penolakannya.

### Curated
Menyimpan hasil agregasi untuk kebutuhan analisis:
- daily_sales
- product_sales

## 4. Incremental Processing
Data baru diambil berdasarkan kolom updated_at.
Watermark diperbarui setelah data berhasil diunggah dan diverifikasi.

## 5. Catatan Implementasi
Spark membaca file yang diunduh sementara dari MinIO menggunakan boto3.
File hasil transformasi kemudian diunggah kembali ke MinIO.
