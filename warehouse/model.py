"""Explicit Snowflake types; no dependency on a local Spark JVM."""

STRINGS = (
    "state_code county_code site_num parameter_code poc site_key monitor_key "
    "time_local_standard units_of_measure unit_standard datum parameter_name "
    "qualifier method_type method_code method_name state_name county_name "
    "source_uri source_generation raw_source_uri raw_source_generation "
    "source_file_group row_hash"
).split()
FLOATS = (
    "sample_measurement measurement_standard latitude longitude mdl uncertainty"
).split()
TIMESTAMPS = "observed_at_utc observed_hour_utc ingested_at_utc".split()
DATES = "date_local_standard date_of_last_change chicago_local_date".split()
BOOLEANS = "is_hour_aligned pm25_advisory_superseded".split()
SMALLINTS = "chicago_local_hour chicago_utc_offset_minutes".split()
EPA_TYPES = {
    **dict.fromkeys(STRINGS, "VARCHAR"),
    **dict.fromkeys(FLOATS, "DOUBLE"),
    **dict.fromkeys(TIMESTAMPS, "TIMESTAMP_NTZ(6)"),
    **dict.fromkeys(DATES, "DATE"),
    **dict.fromkeys(BOOLEANS, "BOOLEAN"),
    **dict.fromkeys(SMALLINTS, "SMALLINT"),
    "year": "INTEGER",
    "pollutant": "VARCHAR",
}
DIMENSIONS = {
    "site": {
        "latitude": "DOUBLE",
        "longitude": "DOUBLE",
        "address": "VARCHAR",
        "county_code": "VARCHAR",
        "state_code": "VARCHAR",
        "established_date": "DATE",
        "closed_date": "DATE",
    },
    "monitor": {
        "site_key": "VARCHAR",
        "latitude": "DOUBLE",
        "longitude": "DOUBLE",
        "method_code": "VARCHAR",
        "status": "VARCHAR",
        "monitoring_objective": "VARCHAR",
        "activity_start": "DATE",
        "activity_end": "DATE",
    },
    "sensor": {
        "latitude": "DOUBLE",
        "longitude": "DOUBLE",
        "is_outdoor": "BOOLEAN",
        "device_model": "VARCHAR",
    },
    "venue": {
        "latitude": "DOUBLE",
        "longitude": "DOUBLE",
        "venue_name": "VARCHAR",
        "monitor_key": "VARCHAR",
        "distance_km": "DOUBLE",
    },
}
