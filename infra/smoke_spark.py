"""Small cloud Spark verification: compute, write Parquet, and read it back."""

import argparse
import json


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    parser = argparse.ArgumentParser()
    parser.add_argument("output_uri")
    args = parser.parse_args()
    if not args.output_uri.startswith("gs://") or "/checks/s02/" not in args.output_uri:
        parser.error("Output must be a GCS path under checks/s02/.")
    spark = SparkSession.builder.appName("air-quality-s02-check").getOrCreate()
    try:
        rows = spark.range(1000).repartition(2)
        rows.write.mode("overwrite").parquet(args.output_uri)
        result = (
            spark.read.parquet(args.output_uri)
            .agg(F.count("*").alias("row_count"), F.sum("id").alias("id_sum"))
            .first()
        )
        if result.row_count != 1000 or result.id_sum != 499500:
            raise RuntimeError(f"Parquet round trip failed: {result}")
        print(json.dumps({"status": "passed", **result.asDict()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
