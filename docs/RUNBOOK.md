# Runbook - Toko Bandung

## 1. Prasyarat
- Docker Desktop
- Python
- Java
- PostgreSQL
- MinIO
- Dependencies dari requirements.txt

## 2. Menjalankan Infrastruktur
Jalankan container menggunakan Docker Compose:

```bash
docker compose up -d
```

Pastikan PostgreSQL dan MinIO berjalan.

## 3. Menjalankan Pipeline
1. Aktifkan virtual environment.
2. Pastikan konfigurasi environment tersedia di file .env.
3. Buka notebook notebooks/minio_exploration.ipynb.
4. Jalankan cell secara berurutan.
5. Periksa hasil pada bucket MinIO.

## 4. Pemeriksaan Hasil
Periksa keberadaan data pada:
- toko-raw
- toko-processed
- toko-curated

Pastikan hasil processed dan curated memiliki file _SUCCESS.

## 5. Incremental Load
1. Baca watermark terakhir dari checkpoint.
2. Ambil data PostgreSQL dengan updated_at lebih besar dari watermark.
3. Unggah hasil extract ke MinIO Raw.
4. Verifikasi jumlah baris dan checksum.
5. Perbarui watermark setelah verifikasi berhasil.

## 6. Recovery dan Failure Handling

### Raw berhasil diunggah tetapi watermark belum diperbarui
Jalankan kembali proses verifikasi raw.
Jika data valid, lanjutkan pembaruan watermark.
Jangan langsung menghapus raw object.

### Incremental dijalankan ulang
Gunakan watermark terakhir agar data yang sama tidak diekstrak kembali.

### Curated diunggah ulang
Gunakan object key deterministik agar hasil rerun tidak menghasilkan file duplikat.

## 7. Quality Gate
Data yang tidak memenuhi Data Contract diarahkan ke quarantine.
Periksa reject_reason untuk mengetahui penyebab kegagalan.
