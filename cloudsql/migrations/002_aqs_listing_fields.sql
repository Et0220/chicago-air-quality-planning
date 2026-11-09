-- AQS listings provide monitor coordinates, first data year, last sample date,
-- and extraction date. Do not infer exact monitor start/closure dates.
-- EPA also includes a small number of Canadian site rows with state code CC.
ALTER TABLE aqs_site DROP CONSTRAINT aqs_site_state_code_check;
ALTER TABLE aqs_site ADD CONSTRAINT aqs_site_state_code_check
    CHECK (state_code ~ '^[0-9]{2}$' OR state_code = 'CC');

ALTER TABLE aqs_site
    ADD COLUMN established_date DATE,
    ADD COLUMN closed_date DATE,
    ADD COLUMN extraction_date DATE;

ALTER TABLE aqs_monitor
    ADD COLUMN latitude DOUBLE PRECISION,
    ADD COLUMN longitude DOUBLE PRECISION,
    ADD COLUMN datum TEXT,
    ADD COLUMN first_year_of_data INTEGER,
    ADD COLUMN last_sample_date DATE,
    ADD COLUMN extraction_date DATE;

CREATE INDEX aqs_monitor_last_sample_idx
    ON aqs_monitor (parameter_code, last_sample_date DESC);
