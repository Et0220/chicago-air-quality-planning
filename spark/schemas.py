"""Explicit EPA CSV and curated Parquet contracts (no schema inference)."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EPA_COLUMNS = (
    "State Code",
    "County Code",
    "Site Num",
    "Parameter Code",
    "POC",
    "Latitude",
    "Longitude",
    "Datum",
    "Parameter Name",
    "Date Local",
    "Time Local",
    "Date GMT",
    "Time GMT",
    "Sample Measurement",
    "Units of Measure",
    "MDL",
    "Uncertainty",
    "Qualifier",
    "Method Type",
    "Method Code",
    "Method Name",
    "State Name",
    "County Name",
    "Date of Last Change",
)
# Parse strings first so malformed numbers/dates remain available in exceptions.
EPA_CSV_SCHEMA = StructType([StructField(c, StringType()) for c in EPA_COLUMNS])
CURATED_SCHEMA = StructType(
    [
        StructField(c, StringType())
        for c in (
            "state_code",
            "county_code",
            "site_num",
            "parameter_code",
            "poc",
            "site_key",
            "monitor_key",
        )
    ]
    + [
        StructField("observed_at_utc", TimestampType()),
        StructField("observed_hour_utc", TimestampType()),
        StructField("is_hour_aligned", BooleanType()),
        StructField("date_local_standard", DateType()),
        StructField("time_local_standard", StringType()),
    ]
    + [
        StructField(c, DoubleType())
        for c in (
            "sample_measurement",
            "measurement_standard",
            "latitude",
            "longitude",
            "mdl",
            "uncertainty",
        )
    ]
    + [
        StructField(c, StringType())
        for c in (
            "units_of_measure",
            "unit_standard",
            "datum",
            "parameter_name",
            "qualifier",
            "method_type",
            "method_code",
            "method_name",
            "state_name",
            "county_name",
        )
    ]
    + [StructField("date_of_last_change", DateType())]
    + [
        StructField(c, StringType())
        for c in (
            "source_uri",
            "source_generation",
            "raw_source_uri",
            "raw_source_generation",
            "source_file_group",
            "row_hash",
        )
    ]
    + [
        StructField("ingested_at_utc", TimestampType()),
        StructField("pm25_advisory_superseded", BooleanType()),
        StructField("chicago_local_date", DateType()),
        StructField("chicago_local_hour", ShortType()),
        StructField("chicago_utc_offset_minutes", ShortType()),
        StructField("year", IntegerType()),
        StructField("pollutant", StringType()),
    ]
)
