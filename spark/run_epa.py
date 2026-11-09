"""Dataproc entry point: verify, normalize, dedupe, write, and reconcile a year."""

import argparse
import json
import platform
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.normalize_epa import deduplicate, normalize, read_sources
from spark.schemas import CURATED_SCHEMA

RESOURCE_PROPERTIES = {
    "spark.driver.cores",
    "spark.driver.memory",
    "spark.driver.memoryOverhead",
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.dynamicAllocation.enabled",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.session.timeZone",
    "spark.sql.parquet.outputTimestampType",
}


def resource_properties(properties):
    """Never export Spark's generated authentication secret into run evidence."""
    return {key: value for key, value in properties if key in RESOURCE_PROPERTIES}


def write_json(spark, uri, value):
    payload = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if not uri.startswith("gs://"):
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        Path(uri).write_text(payload)
        return
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.create(path, True)
    try:
        stream.write(bytearray(payload.encode("utf-8")))
    finally:
        stream.close()


def parquet_files(spark, uri):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    entries = fs.listFiles(path, True)
    files = []
    while entries.hasNext():
        item = entries.next()
        name = item.getPath().toString()
        if name.endswith(".parquet"):
            files.append({"uri": name, "bytes": item.getLen()})
    return sorted(files, key=lambda r: r["uri"])


def execute(spark, manifest, output):
    if sys.version_info[:2] != (3, 15):
        raise RuntimeError("This project requires Python 3.15")
    start = time.monotonic()
    sources = manifest["sources"]
    years = {r["year"] for r in sources}
    if len(years) != 1:
        raise ValueError("Run one complete archive year per batch")
    raw = read_sources(spark, sources, manifest["ingested_at_utc"]).persist(
        StorageLevel.DISK_ONLY
    )
    counts = {
        r.source_uri: r["count"] for r in raw.groupBy("source_uri").count().collect()
    }
    for source in sources:
        if counts.get(source["stage_uri"], 0) != source["staged_rows"]:
            raise ValueError(f"Staged row count mismatch: {source['stage_uri']}")
    print(f"EPA_PROGRESS input_verified rows={sum(counts.values())}", flush=True)
    valid, exceptions = normalize(raw)
    valid = valid.persist(StorageLevel.DISK_ONLY)
    candidates = {
        r.source_uri: r["count"] for r in valid.groupBy("source_uri").count().collect()
    }
    rejected = {
        r.source_uri: r["count"]
        for r in exceptions.groupBy("source_uri").count().collect()
    }
    reasons = [
        r.asDict()
        for r in exceptions.select(F.explode("error_reasons").alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    ]
    if sum(rejected.values()):
        exceptions.write.mode("errorifexists").parquet(output + "/exceptions")
    print(
        f"EPA_PROGRESS normalized candidates={sum(candidates.values())} "
        f"exceptions={sum(rejected.values())}",
        flush=True,
    )
    clean = deduplicate(valid)
    clean.repartition(
        "pollutant", F.pmod(F.xxhash64("monitor_key"), F.lit(8))
    ).write.mode("errorifexists").option("maxRecordsPerFile", 1000000).partitionBy(
        "year", "pollutant"
    ).parquet(output + "/data")
    restored = spark.read.schema(CURATED_SCHEMA).parquet(output + "/data")
    print("EPA_PROGRESS parquet_written checking_readback", flush=True)
    written = {
        r.source_uri: r["count"]
        for r in restored.groupBy("source_uri").count().collect()
    }
    by_pollutant = [
        r.asDict()
        for r in restored.groupBy("year", "pollutant")
        .agg(
            F.count("*").alias("rows"),
            F.sum((~F.col("is_hour_aligned")).cast("long")).alias(
                "non_hour_aligned_rows"
            ),
            F.sum(F.col("pm25_advisory_superseded").cast("long")).alias(
                "advisory_rows"
            ),
            F.sum(F.col("sample_measurement").isNull().cast("long")).alias(
                "null_measurement_rows"
            ),
            F.sum((F.col("sample_measurement") < 0).cast("long")).alias(
                "negative_measurement_rows"
            ),
        )
        .collect()
    ]
    records = []
    for source in sources:
        uri = source["stage_uri"]
        record = {
            **source,
            "input_rows": counts[uri],
            "rejected_rows": rejected.get(uri, 0),
            "output_rows": written.get(uri, 0),
            "duplicate_rows": candidates.get(uri, 0) - written.get(uri, 0),
        }
        if record["input_rows"] != record["rejected_rows"] + candidates.get(uri, 0):
            raise RuntimeError(f"Candidate reconciliation failed: {uri}")
        if record["duplicate_rows"] < 0:
            raise RuntimeError(f"Output exceeds candidates: {uri}")
        records.append(record)
    files = parquet_files(spark, output + "/data")
    result = {
        "status": "verified",
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "year": next(iter(years)),
        "elapsed_seconds": round(time.monotonic() - start, 3),
        "python_version": platform.python_version(),
        "spark_version": spark.version,
        "input_fingerprint": manifest["input_fingerprint"],
        "sources": records,
        "pollutants": by_pollutant,
        "exception_reasons": reasons,
        "parquet_files": files,
        "output_bytes": sum(r["bytes"] for r in files),
        "spark_properties": resource_properties(spark.sparkContext.getConf().getAll()),
    }
    write_json(spark, output + "/metrics.json", result)
    print(
        "EPA_NORMALIZATION_RESULT "
        + json.dumps(
            {
                k: v
                for k, v in result.items()
                if k not in {"spark_properties", "parquet_files"}
            },
            sort_keys=True,
        ),
        flush=True,
    )
    valid.unpersist()
    raw.unpersist()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    spark = SparkSession.builder.appName("epa-s12-normalize").getOrCreate()
    try:
        execute(spark, manifest, args.output.rstrip("/"))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
