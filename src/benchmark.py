
from pathlib import Path
import shutil
import time
import csv

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum as spark_sum, count


def main():
    root = Path("outputs")
    processed_root = root / "processed"
    benchmark_root = root / "benchmark_tmp"

    runs = [
        p for p in processed_root.iterdir()
        if p.is_dir() and any(p.glob("*.parquet"))
    ]

    if not runs:
        raise FileNotFoundError("Processed Parquet tidak ditemukan.")

    latest = max(runs, key=lambda p: p.stat().st_mtime)

    spark = (
        SparkSession.builder
        .appName("PartitionPruningBenchmark")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Reading: {latest}")

    df = spark.read.parquet(str(latest))
    df = df.withColumn("dt", to_date(col("order_ts")))

    unpartitioned = benchmark_root / "unpartitioned"
    partitioned = benchmark_root / "partitioned"

    if benchmark_root.exists():
        shutil.rmtree(benchmark_root)

    df.write.mode("overwrite").parquet(str(unpartitioned))
    df.write.mode("overwrite").partitionBy("dt").parquet(str(partitioned))

    target_date = "2026-10-20"

    plain_df = spark.read.parquet(str(unpartitioned))
    partition_df = spark.read.parquet(str(partitioned))

    def run_query(data):
        start = time.perf_counter()

        result = (
            data.filter(col("dt") == target_date)
            .agg(
                count("*").alias("order_count"),
                spark_sum("qty").alias("item_count"),
                spark_sum(col("qty") * col("unit_price")).alias("revenue")
            )
            .collect()
        )

        elapsed = time.perf_counter() - start
        return result[0], elapsed

    # Warm-up
    run_query(plain_df)
    run_query(partition_df)

    results = []

    for scenario, data in [
        ("without_partition", plain_df),
        ("with_partition", partition_df)
    ]:
        for run_no in range(1, 4):
            result, elapsed = run_query(data)

            results.append({
                "scenario": scenario,
                "run_no": run_no,
                "elapsed_sec": round(elapsed, 6),
                "order_count": result["order_count"],
                "item_count": result["item_count"],
                "revenue": float(result["revenue"] or 0)
            })

    output = root / "partition_pruning_benchmark.csv"

    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print("\nBENCHMARK RESULTS")
    for row in results:
        print(row)

    print(f"\nSaved: {output}")
    print("\nWithout partition execution plan:")
    plain_df.filter(col("dt") == target_date).explain("formatted")

    print("\nWith partition execution plan:")
    partition_df.filter(col("dt") == target_date).explain("formatted")

    spark.stop()


if __name__ == "__main__":
    main()
