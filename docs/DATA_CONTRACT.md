# Data Contract - Toko Bandung

## 1. Informasi Sumber
- Source system: PostgreSQL
- Database: toko
- Source table: orders
- Data owner: Toko Bandung (simulasi)
- Ingestion method: Initial load dan incremental extraction
- Watermark column: updated_at

## 2. Schema dan Aturan Kolom

| Kolom | Arti | Aturan |
|---|---|---|
| order_id | ID transaksi | Tidak boleh kosong, primary key |
| customer_id | ID pelanggan | Identitas pelanggan |
| product_id | ID produk | Tidak boleh kosong |
| qty | Jumlah barang | Harus lebih besar dari 0 |
| unit_price | Harga satuan | 25000, 50000, 75000, 120000, 250000 |
| status | Status transaksi | PAID atau CANCELLED |
| order_ts | Waktu transaksi | Tidak boleh kosong |
| updated_at | Waktu perubahan data | Digunakan sebagai watermark |

## 3. Primary Key dan Deduplication
- Primary key: order_id
- Duplikasi ditangani menggunakan Window Function Spark.
- Record dengan updated_at terbaru diprioritaskan.

## 4. Aturan Status
Status distandardisasi dengan TRIM dan UPPER.

Status yang diterima:
- PAID
- CANCELLED

Status lain, misalnya SHIPPED dan PAYED, masuk quarantine.

## 5. Aturan Bisnis
- qty harus lebih besar dari 0.
- unit_price harus termasuk daftar harga yang diizinkan.
- order_id dan product_id tidak boleh kosong.
- order_ts tidak boleh kosong.
- Transaksi CANCELLED tidak dihitung dalam agregasi penjualan.

## 6. Penanganan Data Tidak Valid
Baris yang gagal validasi tidak masuk ke processed valid data dan curated.
Baris tersebut disimpan di quarantine beserta reject_reason.

## 7. Contoh
Valid:
order_id=ORD001, product_id=P001, qty=2, unit_price=25000, status=PAID

Tidak valid:
order_id=ORD002, product_id=P001, qty=0, unit_price=25000, status=PAID

Alasan: Invalid quantity.
