# Spark Optimization Experiment

## 1. Tujuan
Membandingkan performa Spark menggunakan konfigurasi
spark.sql.shuffle.partitions yang berbeda.

## 2. Metodologi
- Dataset: Processed orders
- Input rows: 1.269
- Operasi: Group By product_id dengan agregasi total quantity dan revenue
- Konfigurasi: 2, 8, dan 32 shuffle partitions
- Pengukuran: 3 kali untuk setiap konfigurasi
- Warm-up dilakukan sebelum pengukuran.
- Perbandingan menggunakan median elapsed time.

## 3. Hasil Benchmark

| Scenario | Median (seconds) | Input Rows | Output Rows |
|---|---:|---:|---:|
| shuffle_2 | 0.1853 | 1269 | 40 |
| shuffle_8 | 0.1871 | 1269 | 40 |
| shuffle_32 | 0.2198 | 1269 | 40 |

## 4. Analisis
Konfigurasi shuffle_2 menghasilkan median waktu tercepat,
yaitu 0.1853 detik.

Konfigurasi shuffle_8 memiliki performa yang hampir sama,
dengan median 0.1871 detik.

Konfigurasi shuffle_32 menghasilkan waktu paling lambat,
yaitu 0.2198 detik.

Hal ini menunjukkan bahwa penambahan jumlah shuffle partitions
tidak selalu meningkatkan performa. Pada dataset yang relatif
kecil, overhead pengelolaan partisi tambahan dapat mengurangi
keuntungan paralelisme.

## 5. Kesimpulan
Berdasarkan eksperimen ini, shuffle_2 menjadi konfigurasi
tercepat untuk dataset dan lingkungan pengujian yang digunakan.

Hasil ini tidak dapat digeneralisasi untuk semua ukuran dataset.
Pengujian tambahan diperlukan untuk menentukan konfigurasi
optimal pada volume data yang lebih besar.

## 6. Catatan
Eksperimen mengukur waktu eksekusi agregasi tanpa menulis
hasil ke Parquet. Karena itu, output_bytes dicatat sebagai 0
dan bukan ukuran file hasil.
