# Pipeline Toko Bandung — Retail Source-to-Lake

## 1. Project Overview

Pipeline Toko Bandung merupakan mini project Data Engineering yang membangun proses pemindahan dan pengolahan data transaksi retail dari PostgreSQL ke MinIO menggunakan Apache Spark.

Pipeline ini menerapkan konsep **Source-to-Lake Architecture** dengan tiga zona penyimpanan:

- **Raw:** Menyimpan data mentah hasil ekstraksi dari PostgreSQL.
- **Processed:** Menyimpan data yang telah distandardisasi, divalidasi, dideduplikasi, dan dipisahkan berdasarkan kualitas data.
- **Curated:** Menyimpan data yang telah diolah menjadi dataset analitik untuk kebutuhan reporting.

### Architecture

```text
PostgreSQL
    |
    v
Extract (Python)
    |
    v
MinIO - Raw Zone
    |
    v
Transform (Apache Spark)
    |
    +--------------------+
    |                    |
    v                    v
Processed Zone      Quarantine
    |
    v
Curate (Apache Spark)
    |
    v
MinIO - Curated Zone
    |
    +--------------------+
    |                    |
    v                    v
daily_sales        product_sales
```

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline orchestration and extraction |
| PostgreSQL | Source database |
| MinIO | Object storage / Data Lake |
| Apache Spark | Data transformation and aggregation |
| Docker Compose | Running supporting services |
| Pandas | Report generation and data inspection |
| Boto3 | MinIO integration |
| Psycopg | PostgreSQL connection |

## 3. Repository Structure

```text
mp7_pipeline_toko_bandung/
│
├── src/
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   ├── curate.py
│   ├── checks.py
│   ├── benchmark.py
│   └── pipeline.py
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── DATA_CONTRACT.md
│   ├── RUNBOOK.md
│   └── SPARK_BENCHMARK.md
│
├── notebooks/
├── outputs/
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## 4. Prerequisites

Pastikan perangkat sudah memiliki:

- Python 3.10 atau versi yang kompatibel dengan dependencies project.
- Docker Desktop.
- Git.
- Java yang kompatibel dengan versi PySpark yang digunakan.

Pastikan Docker Desktop sudah berjalan sebelum memulai pipeline.

## 5. Installation and Setup

### Step 1 — Clone Repository

```bash
git clone <repository-url>
cd mp7_pipeline_toko_bandung
```

### Step 2 — Create Virtual Environment

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Linux / macOS:

```bash
python -m venv .venv
source .venv/bin/activate
```

### Step 3 — Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4 — Configure Environment Variables

Salin file `.env.example` menjadi `.env`.

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Linux / macOS:

```bash
cp .env.example .env
```

Sesuaikan konfigurasi koneksi PostgreSQL dan MinIO di dalam file `.env`.

Contoh variabel yang perlu dikonfigurasi:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=your_database
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password

MINIO_ENDPOINT=http://localhost:9007
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key

MINIO_RAW_BUCKET=toko-raw
MINIO_PROCESSED_BUCKET=toko-processed
MINIO_CURATED_BUCKET=toko-curated
```

**Important:** Jangan mengunggah file `.env` ke repository Git karena berisi informasi kredensial.

### Step 5 — Start Docker Services

Jalankan PostgreSQL dan MinIO menggunakan Docker Compose:

```bash
docker compose up -d
```

Periksa status container:

```bash
docker compose ps
```

Pastikan seluruh service berjalan dengan baik sebelum melanjutkan.

## 6. Running the Pipeline

Pipeline menyediakan beberapa command untuk menjalankan setiap tahapan.

### A. Initialize

```bash
python -m src.pipeline init
```

Memeriksa kesiapan konfigurasi environment sebelum pipeline dijalankan.

### B. Initial Data Extraction

Untuk pengambilan data awal dari PostgreSQL ke MinIO Raw Zone, jalankan:

```bash
python -m src.extract --mode initial
```

Tahapan ini mengambil data awal dari tabel sumber dan menyimpannya di MinIO.

**Catatan:** Initial extraction digunakan untuk membangun snapshot awal. Jangan menjalankan ulang initial load secara sembarangan karena dapat menggantikan objek raw awal.

### C. Process Data

```bash
python -m src.pipeline process
```

Tahapan transformasi menggunakan Apache Spark meliputi:

- Standardisasi kolom dan nilai status transaksi.
- Konversi tipe data.
- Deduplication berdasarkan `order_id`.
- Validasi kualitas data.
- Memisahkan data valid dan invalid.
- Menyimpan hasil ke Processed Zone dan Quarantine.

### D. Curate Data

```bash
python -m src.pipeline curate
```

Tahapan ini menghasilkan dua dataset analitik:

- `daily_sales`: Ringkasan penjualan berdasarkan tanggal.
- `product_sales`: Ringkasan penjualan berdasarkan produk.

### E. Data Quality and Reconciliation Check

```bash
python -m src.pipeline check
```

Tahapan ini membandingkan hasil curated dengan data sumber untuk memastikan konsistensi jumlah transaksi, jumlah item, dan revenue, serta memeriksa jumlah data yang ditolak.

### F. Incremental Extraction

Untuk mengambil data baru berdasarkan kolom `updated_at`, jalankan:

```bash
python -m src.pipeline ingest
```

Proses incremental menggunakan watermark untuk mengambil record yang lebih baru dari watermark terakhir.

Hasil ekstraksi diunggah ke Raw Zone dan diverifikasi menggunakan jumlah record serta checksum sebelum watermark diperbarui.

**Current implementation note:** Incremental extraction sudah diuji secara terpisah. Namun, proses transformasi dan curated pada implementasi ini masih membaca initial raw snapshot, sehingga batch incremental belum otomatis masuk ke dataset curated.

## 7. Pipeline Execution Summary

Untuk menjalankan pipeline dari awal pada environment yang sudah dikonfigurasi:

```bash
python -m src.pipeline init

# Initial extraction hanya dilakukan untuk snapshot awal
python -m src.extract --mode initial

# Transform raw data
python -m src.pipeline process

# Generate curated datasets
python -m src.pipeline curate

# Run quality and reconciliation checks
python -m src.pipeline check
```

Untuk pengujian incremental berikutnya:

```bash
python -m src.pipeline ingest
```

Kemudian periksa hasil ekstraksi dan watermark.

## 8. Data Lake Storage Structure

Data disimpan dalam tiga bucket MinIO.

```text
MinIO
│
├── toko-raw/
│   └── orders/
│       ├── initial_load/
│       └── incremental/
│
├── toko-processed/
│   ├── valid/
│   └── quarantine/
│
└── toko-curated/
    ├── daily_sales/
    │   └── dt=YYYY-MM-DD/
    │
    └── product_sales/
```

Raw Zone menyimpan data sumber, Processed Zone menyimpan data hasil transformasi dan data quarantine, sedangkan Curated Zone menyimpan dataset siap analisis.

## 9. Output and Reports

Hasil pengujian dan laporan tersedia di folder `outputs/`.

| File | Description |
|---|---|
| `quality_report.csv` | Ringkasan hasil validasi kualitas data |
| `reconciliation.csv` | Perbandingan hasil sumber dan curated |
| `partition_pruning_benchmark.csv` | Perbandingan performa partition pruning |
| `partition_benchmark.csv` | Hasil eksperimen Spark shuffle partitions |
| `partition_benchmark_median.csv` | Ringkasan median benchmark |

Folder `outputs/curated/` dan `outputs/processed/` juga dapat digunakan untuk memeriksa hasil lokal yang diunduh dari MinIO atau hasil proses Spark.

## 10. Spark Optimization Benchmark

Project ini melakukan dua eksperimen optimasi Spark:

1. **Shuffle Partitions Benchmark:** Membandingkan konfigurasi shuffle partitions yang berbeda.
2. **Partition Pruning Benchmark:** Membandingkan query pada dataset tanpa partition dan dataset yang dipartisi berdasarkan tanggal (`dt`).

Untuk menjalankan partition pruning benchmark:

```bash
python -m src.benchmark
```

Hasil pengujian disimpan dalam:

```text
outputs/partition_pruning_benchmark.csv
```

## 11. Important Notes

- Pastikan PostgreSQL dan MinIO berjalan sebelum menjalankan pipeline.
- Pastikan konfigurasi `.env` sesuai dengan environment masing-masing.
- Jangan menjalankan initial extraction berulang kali tanpa memahami dampaknya.
- Proses curated menggunakan hasil processed terbaru.
- Incremental extraction dan transformasi incremental end-to-end belum sepenuhnya terintegrasi.
- File kredensial `.env` tidak boleh di-commit ke Git.

## 12. Documentation

Dokumentasi tambahan tersedia di folder `docs/`:

- `ARCHITECTURE.md`: Penjelasan arsitektur pipeline.
- `DATA_CONTRACT.md`: Definisi dan aturan data.
- `RUNBOOK.md`: Panduan operasional pipeline.
- `SPARK_BENCHMARK.md`: Dokumentasi eksperimen performa Spark.

---

**Project:** Pipeline Toko Bandung — Mini Project 7  
**Focus:** Retail Source-to-Lake Data Engineering Pipeline  
**Storage:** MinIO  
**Processing Engine:** Apache Spark  
**Source Database:** PostgreSQL
