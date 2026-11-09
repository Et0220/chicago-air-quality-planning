USE DATABASE {{database}};
USE SCHEMA {{schema}};

CREATE TABLE IF NOT EXISTS dimension_event (
    kind VARCHAR NOT NULL, natural_key VARCHAR NOT NULL,
    effective_at_utc TIMESTAMP_NTZ(6) NOT NULL, row_hash VARCHAR NOT NULL,
    attributes VARIANT, source_uri VARCHAR, source_generation VARCHAR,
    effective_basis VARCHAR,
    PRIMARY KEY (kind, natural_key, effective_at_utc)
);
{{dimension_ddl}}

CREATE TABLE IF NOT EXISTS dim_pollutant (
    pollutant VARCHAR NOT NULL PRIMARY KEY,
    parameter_name VARCHAR, unit_standard VARCHAR
);
CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE NOT NULL PRIMARY KEY,
    year INTEGER, month INTEGER, day INTEGER, day_of_week_iso INTEGER
);
CREATE TABLE IF NOT EXISTS dim_hour (
    observed_hour_utc TIMESTAMP_NTZ(6) NOT NULL PRIMARY KEY,
    utc_date DATE, utc_hour SMALLINT, chicago_local_date DATE,
    chicago_local_hour SMALLINT, chicago_utc_offset_minutes SMALLINT
);
CREATE TABLE IF NOT EXISTS fact_epa_hourly (
    {{epa_columns}},
    site_sk VARCHAR NOT NULL, monitor_sk VARCHAR NOT NULL,
    s12_batch VARCHAR NOT NULL, commit_generation VARCHAR NOT NULL,
    parquet_uri VARCHAR NOT NULL, parquet_generation VARCHAR NOT NULL,
    loaded_at_utc TIMESTAMP_NTZ(6) NOT NULL,
    PRIMARY KEY (monitor_key, observed_hour_utc)
) CLUSTER BY (TO_DATE(observed_hour_utc), pollutant);
-- A clustering key is recorded, but automatic reclustering is not needed for this pilot.
ALTER TABLE fact_epa_hourly SUSPEND RECLUSTER;
CREATE TABLE IF NOT EXISTS warehouse_commit (
    year INTEGER NOT NULL PRIMARY KEY, s12_batch VARCHAR NOT NULL,
    commit_generation VARCHAR NOT NULL, commit_sha256 VARCHAR NOT NULL,
    reference_sha256 VARCHAR NOT NULL, row_count NUMBER NOT NULL,
    loaded_at_utc TIMESTAMP_NTZ(6) NOT NULL
);
CREATE TABLE IF NOT EXISTS warehouse_load_file (
    year INTEGER, s12_batch VARCHAR, commit_generation VARCHAR,
    uri VARCHAR, generation VARCHAR, bytes NUMBER, md5 VARCHAR,
    PRIMARY KEY (year, uri)
);

-- S14 landing contracts: raw measurement grain; no QC or hourly aggregation here.
CREATE TABLE IF NOT EXISTS landing_pa_reading (
    sensor_id NUMBER, report_ts_utc TIMESTAMP_NTZ(6), arrival_ts_utc TIMESTAMP_NTZ(6),
    pm25_cf1_a DOUBLE, pm25_cf1_b DOUBLE, humidity_pct DOUBLE,
    latitude DOUBLE, longitude DOUBLE, is_outdoor BOOLEAN,
    raw_uri VARCHAR, source_request_id VARCHAR, payload VARIANT,
    PRIMARY KEY (sensor_id, report_ts_utc)
);
CREATE TABLE IF NOT EXISTS landing_pa_sensor_history (
    sensor_id NUMBER, valid_from_utc TIMESTAMP_NTZ(6), valid_to_utc TIMESTAMP_NTZ(6),
    attributes VARIANT, source_uri VARCHAR, source_generation VARCHAR,
    PRIMARY KEY (sensor_id, valid_from_utc)
);
CREATE TABLE IF NOT EXISTS landing_airnow_obs (
    airnow_site_id VARCHAR, pollutant VARCHAR, observed_at_utc TIMESTAMP_NTZ(6),
    concentration DOUBLE, units VARCHAR, aqi NUMBER, arrival_ts_utc TIMESTAMP_NTZ(6),
    raw_uri VARCHAR, source_revision_utc TIMESTAMP_NTZ(6), payload VARIANT,
    PRIMARY KEY (airnow_site_id, pollutant, observed_at_utc)
);
CREATE TABLE IF NOT EXISTS airnow_obs_history (
    airnow_site_id VARCHAR, pollutant VARCHAR, observed_at_utc TIMESTAMP_NTZ(6),
    source_revision_utc TIMESTAMP_NTZ(6), payload VARIANT,
    PRIMARY KEY (airnow_site_id, pollutant, observed_at_utc, source_revision_utc)
);
CREATE TABLE IF NOT EXISTS fact_pa_hourly (
    sensor_id NUMBER, observed_hour_utc TIMESTAMP_NTZ(6), sensor_sk VARCHAR,
    pollutant VARCHAR, measurement_qc DOUBLE, unit_standard VARCHAR,
    sample_count NUMBER, qc_version VARCHAR, loaded_at_utc TIMESTAMP_NTZ(6),
    PRIMARY KEY (sensor_id, observed_hour_utc)
);
CREATE TABLE IF NOT EXISTS fact_airnow_hourly (
    airnow_site_id VARCHAR, pollutant VARCHAR, observed_hour_utc TIMESTAMP_NTZ(6),
    concentration DOUBLE, units VARCHAR, aqi NUMBER,
    source_revision_utc TIMESTAMP_NTZ(6), loaded_at_utc TIMESTAMP_NTZ(6),
    PRIMARY KEY (airnow_site_id, pollutant, observed_hour_utc)
);

-- The caller creates session-local event_input/version_* tables before BEGIN.
-- Only DML runs here: no implicit DDL commit can expose half a load.
CREATE OR REPLACE PROCEDURE apply_dimension_events()
RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER AS $$
BEGIN
{{scd_merge_body}}
RETURN 'applied';
END;
$$;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{database}}.{{schema}}
    TO ROLE AQ_LOADER;
GRANT USAGE ON PROCEDURE apply_dimension_events() TO ROLE AQ_LOADER;
