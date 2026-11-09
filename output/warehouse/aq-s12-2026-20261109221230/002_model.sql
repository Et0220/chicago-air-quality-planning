USE DATABASE AIR_QUALITY;
USE SCHEMA ANALYTICS;

CREATE TABLE IF NOT EXISTS dimension_event (
    kind VARCHAR NOT NULL, natural_key VARCHAR NOT NULL,
    effective_at_utc TIMESTAMP_NTZ(6) NOT NULL, row_hash VARCHAR NOT NULL,
    attributes VARIANT, source_uri VARCHAR, source_generation VARCHAR,
    effective_basis VARCHAR,
    PRIMARY KEY (kind, natural_key, effective_at_utc)
);

CREATE TABLE IF NOT EXISTS dim_site (
    site_sk VARCHAR NOT NULL PRIMARY KEY,
    site_key VARCHAR NOT NULL,
    valid_from_utc TIMESTAMP_NTZ(6) NOT NULL,
    valid_to_utc TIMESTAMP_NTZ(6), is_current BOOLEAN NOT NULL,
    row_hash VARCHAR NOT NULL, attributes VARIANT,
    source_uri VARCHAR, source_generation VARCHAR, effective_basis VARCHAR,
    latitude DOUBLE,
longitude DOUBLE,
address VARCHAR,
county_code VARCHAR,
state_code VARCHAR,
established_date DATE,
closed_date DATE
);
MERGE INTO dim_site d USING (SELECT '0' AS sk) s ON d.site_sk = s.sk
WHEN NOT MATCHED THEN INSERT (
    site_sk, site_key, valid_from_utc, is_current, row_hash, effective_basis
) VALUES ('0', '_UNKNOWN', '0001-01-01'::TIMESTAMP_NTZ, TRUE, 'unknown', 'unknown');


CREATE TABLE IF NOT EXISTS dim_monitor (
    monitor_sk VARCHAR NOT NULL PRIMARY KEY,
    monitor_key VARCHAR NOT NULL,
    valid_from_utc TIMESTAMP_NTZ(6) NOT NULL,
    valid_to_utc TIMESTAMP_NTZ(6), is_current BOOLEAN NOT NULL,
    row_hash VARCHAR NOT NULL, attributes VARIANT,
    source_uri VARCHAR, source_generation VARCHAR, effective_basis VARCHAR,
    site_key VARCHAR,
latitude DOUBLE,
longitude DOUBLE,
method_code VARCHAR,
status VARCHAR,
monitoring_objective VARCHAR,
activity_start DATE,
activity_end DATE
);
MERGE INTO dim_monitor d USING (SELECT '0' AS sk) s ON d.monitor_sk = s.sk
WHEN NOT MATCHED THEN INSERT (
    monitor_sk, monitor_key, valid_from_utc, is_current, row_hash, effective_basis
) VALUES ('0', '_UNKNOWN', '0001-01-01'::TIMESTAMP_NTZ, TRUE, 'unknown', 'unknown');


CREATE TABLE IF NOT EXISTS dim_sensor (
    sensor_sk VARCHAR NOT NULL PRIMARY KEY,
    sensor_key VARCHAR NOT NULL,
    valid_from_utc TIMESTAMP_NTZ(6) NOT NULL,
    valid_to_utc TIMESTAMP_NTZ(6), is_current BOOLEAN NOT NULL,
    row_hash VARCHAR NOT NULL, attributes VARIANT,
    source_uri VARCHAR, source_generation VARCHAR, effective_basis VARCHAR,
    latitude DOUBLE,
longitude DOUBLE,
is_outdoor BOOLEAN,
device_model VARCHAR
);
MERGE INTO dim_sensor d USING (SELECT '0' AS sk) s ON d.sensor_sk = s.sk
WHEN NOT MATCHED THEN INSERT (
    sensor_sk, sensor_key, valid_from_utc, is_current, row_hash, effective_basis
) VALUES ('0', '_UNKNOWN', '0001-01-01'::TIMESTAMP_NTZ, TRUE, 'unknown', 'unknown');


CREATE TABLE IF NOT EXISTS dim_venue (
    venue_sk VARCHAR NOT NULL PRIMARY KEY,
    venue_key VARCHAR NOT NULL,
    valid_from_utc TIMESTAMP_NTZ(6) NOT NULL,
    valid_to_utc TIMESTAMP_NTZ(6), is_current BOOLEAN NOT NULL,
    row_hash VARCHAR NOT NULL, attributes VARIANT,
    source_uri VARCHAR, source_generation VARCHAR, effective_basis VARCHAR,
    latitude DOUBLE,
longitude DOUBLE,
venue_name VARCHAR,
monitor_key VARCHAR,
distance_km DOUBLE
);
MERGE INTO dim_venue d USING (SELECT '0' AS sk) s ON d.venue_sk = s.sk
WHEN NOT MATCHED THEN INSERT (
    venue_sk, venue_key, valid_from_utc, is_current, row_hash, effective_basis
) VALUES ('0', '_UNKNOWN', '0001-01-01'::TIMESTAMP_NTZ, TRUE, 'unknown', 'unknown');


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
    state_code VARCHAR,
county_code VARCHAR,
site_num VARCHAR,
parameter_code VARCHAR,
poc VARCHAR,
site_key VARCHAR,
monitor_key VARCHAR,
time_local_standard VARCHAR,
units_of_measure VARCHAR,
unit_standard VARCHAR,
datum VARCHAR,
parameter_name VARCHAR,
qualifier VARCHAR,
method_type VARCHAR,
method_code VARCHAR,
method_name VARCHAR,
state_name VARCHAR,
county_name VARCHAR,
source_uri VARCHAR,
source_generation VARCHAR,
raw_source_uri VARCHAR,
raw_source_generation VARCHAR,
source_file_group VARCHAR,
row_hash VARCHAR,
sample_measurement DOUBLE,
measurement_standard DOUBLE,
latitude DOUBLE,
longitude DOUBLE,
mdl DOUBLE,
uncertainty DOUBLE,
observed_at_utc TIMESTAMP_NTZ(6),
observed_hour_utc TIMESTAMP_NTZ(6),
ingested_at_utc TIMESTAMP_NTZ(6),
date_local_standard DATE,
date_of_last_change DATE,
chicago_local_date DATE,
is_hour_aligned BOOLEAN,
pm25_advisory_superseded BOOLEAN,
chicago_local_hour SMALLINT,
chicago_utc_offset_minutes SMALLINT,
year INTEGER,
pollutant VARCHAR,
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

MERGE INTO dimension_event d USING event_input s
ON d.kind = s.kind AND d.natural_key = s.natural_key
    AND d.effective_at_utc = s.effective_at_utc
WHEN NOT MATCHED THEN INSERT (
    kind, natural_key, effective_at_utc, row_hash, attributes,
    source_uri, source_generation, effective_basis
) VALUES (s.kind, s.natural_key, s.effective_at_utc, s.row_hash, s.attributes,
    s.source_uri, s.source_generation, s.effective_basis);


MERGE INTO dim_site d USING version_site s ON d.site_sk = s.surrogate_key
WHEN MATCHED AND (d.valid_to_utc IS DISTINCT FROM s.valid_to_utc) THEN
    UPDATE SET valid_to_utc = s.valid_to_utc, is_current = (s.valid_to_utc IS NULL)
WHEN NOT MATCHED THEN INSERT (
    site_sk, site_key, valid_from_utc, valid_to_utc, is_current,
    row_hash, attributes, source_uri, source_generation, effective_basis, latitude, longitude, address, county_code, state_code, established_date, closed_date
) VALUES (
    s.surrogate_key, s.natural_key, s.valid_from_utc, s.valid_to_utc,
    s.valid_to_utc IS NULL, s.row_hash, s.attributes, s.source_uri,
    s.source_generation, s.effective_basis, s.attributes:"latitude"::DOUBLE, s.attributes:"longitude"::DOUBLE, s.attributes:"address"::VARCHAR, s.attributes:"county_code"::VARCHAR, s.attributes:"state_code"::VARCHAR, s.attributes:"established_date"::DATE, s.attributes:"closed_date"::DATE
);
-- Late documented events can make a previously inferred change boundary redundant.
-- The source event remains in the ledger; facts are re-keyed in the same transaction.
DELETE FROM dim_site WHERE site_sk <> '0' AND site_sk NOT IN (
    SELECT surrogate_key FROM version_site
);


MERGE INTO dim_monitor d USING version_monitor s ON d.monitor_sk = s.surrogate_key
WHEN MATCHED AND (d.valid_to_utc IS DISTINCT FROM s.valid_to_utc) THEN
    UPDATE SET valid_to_utc = s.valid_to_utc, is_current = (s.valid_to_utc IS NULL)
WHEN NOT MATCHED THEN INSERT (
    monitor_sk, monitor_key, valid_from_utc, valid_to_utc, is_current,
    row_hash, attributes, source_uri, source_generation, effective_basis, site_key, latitude, longitude, method_code, status, monitoring_objective, activity_start, activity_end
) VALUES (
    s.surrogate_key, s.natural_key, s.valid_from_utc, s.valid_to_utc,
    s.valid_to_utc IS NULL, s.row_hash, s.attributes, s.source_uri,
    s.source_generation, s.effective_basis, s.attributes:"site_key"::VARCHAR, s.attributes:"latitude"::DOUBLE, s.attributes:"longitude"::DOUBLE, s.attributes:"method_code"::VARCHAR, s.attributes:"status"::VARCHAR, s.attributes:"monitoring_objective"::VARCHAR, s.attributes:"activity_start"::DATE, s.attributes:"activity_end"::DATE
);
-- Late documented events can make a previously inferred change boundary redundant.
-- The source event remains in the ledger; facts are re-keyed in the same transaction.
DELETE FROM dim_monitor WHERE monitor_sk <> '0' AND monitor_sk NOT IN (
    SELECT surrogate_key FROM version_monitor
);


MERGE INTO dim_sensor d USING version_sensor s ON d.sensor_sk = s.surrogate_key
WHEN MATCHED AND (d.valid_to_utc IS DISTINCT FROM s.valid_to_utc) THEN
    UPDATE SET valid_to_utc = s.valid_to_utc, is_current = (s.valid_to_utc IS NULL)
WHEN NOT MATCHED THEN INSERT (
    sensor_sk, sensor_key, valid_from_utc, valid_to_utc, is_current,
    row_hash, attributes, source_uri, source_generation, effective_basis, latitude, longitude, is_outdoor, device_model
) VALUES (
    s.surrogate_key, s.natural_key, s.valid_from_utc, s.valid_to_utc,
    s.valid_to_utc IS NULL, s.row_hash, s.attributes, s.source_uri,
    s.source_generation, s.effective_basis, s.attributes:"latitude"::DOUBLE, s.attributes:"longitude"::DOUBLE, s.attributes:"is_outdoor"::BOOLEAN, s.attributes:"device_model"::VARCHAR
);
-- Late documented events can make a previously inferred change boundary redundant.
-- The source event remains in the ledger; facts are re-keyed in the same transaction.
DELETE FROM dim_sensor WHERE sensor_sk <> '0' AND sensor_sk NOT IN (
    SELECT surrogate_key FROM version_sensor
);


MERGE INTO dim_venue d USING version_venue s ON d.venue_sk = s.surrogate_key
WHEN MATCHED AND (d.valid_to_utc IS DISTINCT FROM s.valid_to_utc) THEN
    UPDATE SET valid_to_utc = s.valid_to_utc, is_current = (s.valid_to_utc IS NULL)
WHEN NOT MATCHED THEN INSERT (
    venue_sk, venue_key, valid_from_utc, valid_to_utc, is_current,
    row_hash, attributes, source_uri, source_generation, effective_basis, latitude, longitude, venue_name, monitor_key, distance_km
) VALUES (
    s.surrogate_key, s.natural_key, s.valid_from_utc, s.valid_to_utc,
    s.valid_to_utc IS NULL, s.row_hash, s.attributes, s.source_uri,
    s.source_generation, s.effective_basis, s.attributes:"latitude"::DOUBLE, s.attributes:"longitude"::DOUBLE, s.attributes:"venue_name"::VARCHAR, s.attributes:"monitor_key"::VARCHAR, s.attributes:"distance_km"::DOUBLE
);
-- Late documented events can make a previously inferred change boundary redundant.
-- The source event remains in the ledger; facts are re-keyed in the same transaction.
DELETE FROM dim_venue WHERE venue_sk <> '0' AND venue_sk NOT IN (
    SELECT surrogate_key FROM version_venue
);

RETURN 'applied';
END;
$$;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA AIR_QUALITY.ANALYTICS
    TO ROLE AQ_LOADER;
GRANT USAGE ON PROCEDURE apply_dimension_events() TO ROLE AQ_LOADER;
