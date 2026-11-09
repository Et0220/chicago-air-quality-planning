"""S11: native Spark SQL transforms; no Python workers or UDFs required."""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark.schemas import CURATED_SCHEMA, EPA_COLUMNS, EPA_CSV_SCHEMA

GASES = ("44201", "42401", "42101", "42602")
PARTICLES = ("88101", "88502", "81102", "14129", "12128", "85129")


def configure(spark: SparkSession) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")


def read_sources(
    spark: SparkSession, sources: list[dict], ingested_at: str
) -> DataFrame:
    """Read a full archive-year scope with explicit immutable input lineage."""
    configure(spark)
    frames = []
    for source in sources:
        frame = (
            spark.read.schema(EPA_CSV_SCHEMA)
            .options(
                header=True,
                enforceSchema=False,
                mode="FAILFAST",
                multiLine=True,
                escape='"',
                encoding="UTF-8",
            )
            .csv(source.get("input_uri", source["stage_uri"]))
        )
        for key, value in {
            "source_uri": source["stage_uri"],
            "source_generation": source["stage_generation"],
            "raw_source_uri": source["raw_uri"],
            "raw_source_generation": source["raw_generation"],
            "source_file_group": source["pollutant"],
            "source_year": source["year"],
            "ingested_at_utc": ingested_at,
        }.items():
            frame = frame.withColumn(key, F.lit(value))
        frames.append(frame)
    if not frames:
        raise ValueError("At least one staged source is required")
    return reduce(DataFrame.unionByName, frames)


def normalize(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return valid candidates and exceptions; do not silently discard bad rows."""
    configure(raw.sparkSession)
    names = [c.lower().replace(" ", "_") for c in EPA_COLUMNS]
    df = raw.select(
        *[
            F.when(F.trim(F.col(c)) != "", F.col(c)).alias(n)
            for c, n in zip(EPA_COLUMNS, names, strict=True)
        ],
        *[F.col(c) for c in raw.columns if c not in EPA_COLUMNS],
    )
    df = df.withColumn(
        "raw_record", F.to_json(F.struct(*names), {"ignoreNullFields": "false"})
    )
    df = df.withColumn("row_hash", F.sha2("raw_record", 256))
    errors = []
    for name, width in [
        ("state_code", 2),
        ("county_code", 3),
        ("site_num", 4),
        ("parameter_code", 5),
        ("poc", 2),
    ]:
        col = F.trim(F.col(name))
        valid = col.rlike(r"^\d{1," + str(width) + r"}$")
        if name == "state_code":
            valid = valid | (col == "CC")
        errors.append(
            F.when(~F.coalesce(valid, F.lit(False)), F.lit("invalid_" + name))
        )
        df = df.withColumn(name, F.when(valid, F.lpad(col, width, "0")))
    df = df.withColumn(
        "site_key", F.concat_ws("-", "state_code", "county_code", "site_num")
    )
    df = df.withColumn(
        "monitor_key", F.concat_ws("-", "site_key", "parameter_code", "poc")
    )
    # EPA hourly-duration samples may start at :26, etc. Preserve exact time
    # and expose a separate UTC hour bucket for the project's curated grain.
    gmt_text = F.concat_ws(" ", F.col("date_gmt"), F.col("time_gmt"))
    df = df.withColumn("observed_at_utc", F.to_timestamp(gmt_text, "yyyy-MM-dd HH:mm"))
    errors.append(
        F.when(
            F.col("observed_at_utc").isNull()
            | ~F.coalesce(
                F.col("time_gmt").rlike(r"^(?:[01]\d|2[0-3]):[0-5]\d$"), F.lit(False)
            ),
            F.lit("invalid_gmt_time"),
        )
    )
    df = df.withColumn("date_local_standard", F.to_date("date_local", "yyyy-MM-dd"))
    df = df.withColumn("time_local_standard", F.col("time_local"))
    errors.append(
        F.when(
            F.col("date_local_standard").isNull()
            | ~F.coalesce(
                F.col("time_local").rlike(r"^(?:[01]\d|2[0-3]):[0-5]\d$"), F.lit(False)
            ),
            F.lit("invalid_local_standard_time"),
        )
    )
    # Archive year is local standard date year; UTC New Year spillover stays
    # in its source partition, allowing complete year replacement on replay.
    df = df.withColumn("year", F.col("source_year").cast("int"))
    df = df.withColumn("observed_hour_utc", F.date_trunc("hour", "observed_at_utc"))
    df = df.withColumn(
        "is_hour_aligned", F.col("observed_at_utc") == F.col("observed_hour_utc")
    )
    errors.append(
        F.when(
            F.year("date_local_standard") != F.col("year"),
            F.lit("source_year_mismatch"),
        )
    )
    df = df.withColumn("revision_date", F.to_date("date_of_last_change", "yyyy-MM-dd"))
    errors.append(
        F.when(
            F.col("date_of_last_change").isNotNull() & F.col("revision_date").isNull(),
            F.lit("invalid_revision_date"),
        )
    )
    for name in ["sample_measurement", "latitude", "longitude", "mdl", "uncertainty"]:
        value = F.col(name).cast("double")
        bad = F.col(name).isNotNull() & (
            value.isNull() | F.isnan(value) | (F.abs(value) == F.lit(float("inf")))
        )
        errors.append(F.when(bad, F.lit("invalid_" + name)))
        df = df.withColumn(name + "_value", value)
    unit = F.lower(F.trim(F.col("units_of_measure")))
    gas = F.col("parameter_code").isin(*GASES)
    particle = F.col("parameter_code").isin(*PARTICLES)
    ppm = unit.isin("parts per million", "ppm")
    ppb = unit.isin("parts per billion", "ppb")
    ug = unit.isin(
        "micrograms/cubic meter (lc)",
        "micrograms/cubic meter (25 c)",
        "micrograms/cubic meter",
        "µg/m³",
        "ug/m3",
    )
    known = (gas & (ppm | ppb)) | (particle & ug)
    errors.append(
        F.when(~F.coalesce(known, F.lit(False)), F.lit("unsupported_parameter_unit"))
    )
    df = df.withColumn(
        "unit_standard", F.when(gas & (ppm | ppb), "ppb").when(particle & ug, "µg/m³")
    )
    df = df.withColumn(
        "measurement_standard",
        F.when(gas & ppm, F.col("sample_measurement_value") * 1000).when(
            known, F.col("sample_measurement_value")
        ),
    )
    df = df.withColumn(
        "pm25_advisory_superseded",
        F.coalesce(
            (F.col("parameter_code") == "88101")
            & F.lpad(F.trim(F.col("method_code")), 3, "0").isin("236", "238"),
            F.lit(False),
        ),
    )
    chicago = (
        (F.col("state_code") == "17")
        & F.col("county_code").isin("031", "043", "097", "197", "089", "111")
    ) | ((F.col("state_code") == "18") & (F.col("county_code") == "089"))
    local = F.from_utc_timestamp("observed_at_utc", "America/Chicago")
    df = df.withColumn("chicago_local_date", F.when(chicago, F.to_date(local)))
    df = df.withColumn(
        "chicago_local_hour", F.when(chicago, F.hour(local)).cast("short")
    )
    df = df.withColumn(
        "chicago_utc_offset_minutes",
        F.when(
            chicago, (local.cast("long") - F.col("observed_at_utc").cast("long")) / 60
        ).cast("short"),
    )
    df = df.withColumn("ingested_at_utc", F.to_timestamp("ingested_at_utc"))
    errors.append(
        F.when(F.col("ingested_at_utc").isNull(), F.lit("invalid_ingestion_time"))
    )
    for name in ("source_generation", "raw_source_generation"):
        errors.append(
            F.when(
                ~F.coalesce(F.col(name).rlike(r"^\d{1,38}$"), F.lit(False)),
                F.lit("invalid_" + name),
            )
        )
    df = df.withColumn(
        "error_reasons", F.filter(F.array(*errors), lambda x: x.isNotNull())
    )
    exceptions = df.filter(F.size("error_reasons") > 0).select(
        "source_uri",
        "source_generation",
        "raw_source_uri",
        "raw_source_generation",
        "source_file_group",
        "source_year",
        "raw_record",
        "row_hash",
        "error_reasons",
    )
    valid = df.filter(F.size("error_reasons") == 0)
    for name in ["sample_measurement", "latitude", "longitude", "mdl", "uncertainty"]:
        valid = valid.withColumn(name, F.col(name + "_value"))
    valid = valid.withColumn("date_of_last_change", F.col("revision_date"))
    valid = valid.withColumn("pollutant", F.col("parameter_code"))
    return valid.select(
        *[F.col(f.name).cast(f.dataType) for f in CURATED_SCHEMA]
    ), exceptions


def deduplicate(valid: DataFrame) -> DataFrame:
    """Advisory eligibility, revision, numeric generation, stable hash ordering."""
    window = Window.partitionBy("monitor_key", "observed_hour_utc").orderBy(
        F.col("pm25_advisory_superseded").asc(),
        F.col("date_of_last_change").desc_nulls_last(),
        F.col("source_generation").cast("decimal(38,0)").desc(),
        F.col("row_hash").desc(),
        F.col("source_uri").desc(),
    )
    return (
        valid.withColumn("_rank", F.row_number().over(window))
        .filter("_rank = 1")
        .drop("_rank")
    )
