# Failure Test - Toko Bandung

## Scenario 1: Process stops after raw upload

### Gejala
File raw sudah tersimpan di MinIO, tetapi watermark belum diperbarui.

### Penyebab
Proses gagal sebelum checkpoint berhasil ditulis.

### Recovery
1. Baca watermark terakhir dari MinIO.
2. Jalankan kembali incremental extraction.
3. Upload batch dengan object key yang konsisten.
4. Verifikasi jumlah baris dan checksum.
5. Update watermark setelah raw berhasil diverifikasi.

### Pencegahan
Watermark hanya diperbarui setelah raw berhasil ditulis dan diverifikasi.

Status: Recovery procedure documented, not physically simulated.

## Scenario 2: Incremental rerun

### Gejala
Pipeline dijalankan ulang setelah watermark diperbarui.

### Hasil pengujian
- Transaksi baru berhasil diambil: 1 baris.
- Raw berhasil di-upload dan diverifikasi.
- Watermark berhasil diperbarui.
- Incremental rows pada rerun: 0.

### Pencegahan
Menggunakan updated_at sebagai watermark dan order_id sebagai kunci deduplikasi.

Status: Tested.

## Scenario 3: Duplicate Curated upload

### Gejala
Pipeline dijalankan ulang dan berpotensi menghasilkan file Parquet baru.

### Recovery
Gunakan object key konsisten untuk menggantikan file sebelumnya.

### Hasil pengujian
- daily_sales: 8 objects.
- product_sales: 2 objects.
- Object key menggunakan nama deterministik.

### Pencegahan
Upload menggunakan deterministic object key.

Status: Object structure verified. Confirm object counts remain unchanged after a second upload.