USE DATABASE AIR_QUALITY;
USE SCHEMA ANALYTICS;
CREATE OR REPLACE VIEW chicago_epa_hourly AS
SELECT * FROM fact_epa_hourly
WHERE (state_code = '17' AND county_code IN ('031','043','097','197','089','111'))
   OR (state_code = '18' AND county_code = '089');
-- An analytical mean, not a regulatory statistic. Preserve exclusions as counts.
CREATE OR REPLACE VIEW chicago_pm25_monthly AS
SELECT DATE_TRUNC('MONTH', chicago_local_date) AS month,
    parameter_code, units_of_measure,
    AVG(IFF(NOT pm25_advisory_superseded AND measurement_standard >= 0
        AND is_hour_aligned, measurement_standard, NULL)) AS mean_pm25,
    COUNT(*) AS total_rows,
    COUNT_IF(pm25_advisory_superseded) AS advisory_rows,
    COUNT_IF(measurement_standard < 0) AS negative_rows,
    COUNT_IF(NOT is_hour_aligned) AS non_hour_aligned_rows
FROM chicago_epa_hourly WHERE parameter_code IN ('88101','88502')
GROUP BY 1,2,3;
GRANT SELECT ON ALL VIEWS IN SCHEMA AIR_QUALITY.ANALYTICS TO ROLE AQ_LOADER;
