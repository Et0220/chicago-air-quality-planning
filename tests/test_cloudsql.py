"""S05 PostgreSQL integration checks; opt in with a disposable test DSN."""

import os
import uuid

import pytest

from ingestion import db


@pytest.mark.skipif(
    not os.environ.get("AIR_QUALITY_TEST_POSTGRES_DSN"),
    reason="Set AIR_QUALITY_TEST_POSTGRES_DSN for local PostgreSQL integration",
)
def test_migrations_keys_duplicates_and_revision(monkeypatch) -> None:
    monkeypatch.setenv(
        "AIR_QUALITY_CLOUDSQL_DSN", os.environ["AIR_QUALITY_TEST_POSTGRES_DSN"]
    )
    schema = "s05_test_" + uuid.uuid4().hex
    with db.connect() as conn:
        cur = conn.cursor()
        try:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}"')
            expected = sorted(
                path.name for path in db.MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql")
            )
            assert expected[0] == "001_operational.sql"
            assert db.migrate(conn) == expected  # applies every numbered file
            assert db.migrate(conn) == []  # rerun is a no-op

            run_id = uuid.uuid4()
            cur.execute(
                """INSERT INTO ingest_run
                   (run_id, job_name, status, started_at_utc)
                   VALUES (%s, 'epa_download', 'running', now())""",
                (str(run_id),),
            )
            for _ in range(2):
                cur.execute(
                    """INSERT INTO ingest_file
                       (pollutant, year, file_name, catalog_status)
                       VALUES ('pm25_frm_fem', 2024, 'hourly_88101_2024.zip',
                               'listed_nonempty')
                       ON CONFLICT (pollutant, year) DO NOTHING"""
                )
            cur.execute("SELECT count(*) FROM ingest_file")
            assert cur.fetchone()[0] == 1

            cur.execute(
                """INSERT INTO pa_sensor
                   (sensor_id, latitude, longitude, is_outdoor,
                    first_seen_at_utc, last_seen_at_utc)
                   VALUES (42, 41.9, -87.6, TRUE, now(), now())"""
            )
            for _ in range(2):
                cur.execute(
                    """INSERT INTO pa_reading
                       (sensor_id, report_ts_utc, arrival_ts_utc, pm25_cf1_a,
                        pm25_cf1_b, humidity_pct, raw_uri, source_request_id)
                       VALUES (42, '2026-11-08 12:00+00', now(), 10, 11, 50,
                               'gs://test/pa.json', 'request-1')
                       ON CONFLICT (sensor_id, report_ts_utc) DO NOTHING"""
                )
            cur.execute("SELECT count(*) FROM pa_reading")
            assert cur.fetchone()[0] == 1

            for _ in range(2):
                cur.execute(
                    """INSERT INTO airnow_obs
                       (airnow_site_id, pollutant, observed_at_utc,
                        concentration, aqi, retrieved_at_utc, raw_uri,
                        source_generation)
                       VALUES ('170310001', 'PM25', '2026-11-08 12:00+00',
                               12.5, 51, '2026-11-08 13:00+00',
                               'gs://test/airnow.dat', '1')
                       ON CONFLICT (airnow_site_id, pollutant, observed_at_utc)
                       DO NOTHING"""
                )
            cur.execute("SELECT count(*) FROM airnow_obs")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT count(*) FROM airnow_obs_revision")
            assert cur.fetchone()[0] == 0
            cur.execute(
                """UPDATE airnow_obs SET concentration = 13.5, aqi = 53,
                   retrieved_at_utc = '2026-11-08 14:00+00',
                   source_generation = '2'
                   WHERE airnow_site_id = '170310001' AND pollutant = 'PM25'"""
            )
            cur.execute("SELECT concentration, aqi FROM airnow_obs_revision")
            assert tuple(cur.fetchone()) == (12.5, 51)
            cur.execute("SELECT concentration, aqi FROM airnow_obs")
            assert tuple(cur.fetchone()) == (13.5, 53)

            cur.execute(
                """INSERT INTO aqs_site
                   (site_key, state_code, county_code, site_num,
                    source_uri, source_generation)
                   VALUES ('17-031-0001', '17', '031', '0001',
                           'gs://test/sites', '1')"""
            )
            cur.execute(
                """INSERT INTO aqs_monitor
                   (monitor_key, site_key, parameter_code, poc,
                    source_uri, source_generation)
                   VALUES ('17-031-0001-88101-01', '17-031-0001',
                           '88101', '01', 'gs://test/monitors', '1')"""
            )
            cur.execute("SELECT count(*) FROM aqs_monitor")
            assert cur.fetchone()[0] == 1
            conn.commit()
        finally:
            conn.rollback()
            cur.execute("SET search_path TO public")
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()
            cur.close()
