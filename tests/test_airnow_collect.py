"""S09 source parsing, immutable storage, retries, and PostgreSQL revision tests."""

import base64
import csv
import hashlib
import io
import os
import uuid
from contextlib import closing
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest
import requests
from google.api_core.exceptions import NotFound, PreconditionFailed

from ingestion import airnow_collect as airnow
from ingestion import airnow_verify, db

HOUR = datetime(2026, 11, 1, 6, tzinfo=UTC)
FIELDS = (
    "AQSID,SiteName,Status,Latitude,Longitude,CountryCode,ValidDate,ValidTime,"
    "DataSource,PM25,PM25_Unit,OZONE,OZONE_Unit,NO2,NO2_Unit,CO,CO_Unit,"
    "SO2,SO2_Unit,PM10,PM10_Unit,PM25_AQI,OZONE_AQI,NO2_AQI,PM10_AQI"
).split(",")


def source(*changes):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=FIELDS)
    writer.writeheader()
    for change in changes or ({},):
        row = dict.fromkeys(FIELDS, "")
        row.update(
            AQSID="170310001",
            SiteName="Test, Chicago",
            Status="Active",
            Latitude="41.9",
            Longitude="-87.6",
            CountryCode="US",
            ValidDate="11/01/2026",
            ValidTime="06:00",
            DataSource="Test agency",
            PM25="-0.7",
            PM25_Unit="UG/M3",
            PM25_AQI="3.0",
            OZONE_AQI="9",
            CO="0.1",
            CO_Unit="PPM",
        )
        row.update(change)
        writer.writerow(row)
    return output.getvalue().encode()


def snapshot(**kwargs):
    return replace(airnow.Snapshot(HOUR, source(), HOUR + timedelta(hours=1)), **kwargs)


def test_parser_preserves_ids_negative_values_aqi_only_and_units():
    rows = airnow.parse_file(
        source({}, {"AQSID": "840170310052"}), HOUR, airnow.Bounds()
    )
    assert len(rows) == 6
    assert {r.airnow_site_id for r in rows} == {"170310001", "840170310052"}
    pm, ozone, co = rows[:3]
    assert (pm.concentration, pm.aqi, pm.concentration_unit) == (-0.7, 3, "UG/M3")
    assert (ozone.concentration, ozone.aqi) == (None, 9)
    assert (co.concentration, co.aqi, co.concentration_unit) == (0.1, None, "PPM")
    assert pm.site_name == "Test, Chicago"
    assert pm.observed_at_utc == HOUR  # fall-back day remains unambiguous UTC


def test_region_and_missing_values_do_not_fabricate_observations():
    content = source(
        {},
        {"AQSID": "060250005", "Latitude": "32.6"},
        {"AQSID": "170310002", "PM25": "", "PM25_AQI": "", "OZONE_AQI": "", "CO": ""},
    )
    assert len(airnow.parse_file(content, HOUR, airnow.Bounds())) == 3


@pytest.mark.parametrize(
    "changes,match",
    [
        ({"PM25": "NaN"}, "Non-finite"),
        ({"PM25_AQI": "5.4"}, "Invalid AQI"),
        ({"PM25_AQI": "-999"}, "Invalid AQI"),
        ({"PM25_Unit": ""}, "Missing concentration unit"),
        ({"ValidTime": "07:00"}, "UTC hour mismatch"),
        ({"AQSID": "1"}, "Invalid AQSID"),
    ],
)
def test_bad_values_fail_the_hour(changes, match):
    with pytest.raises(ValueError, match=match):
        airnow.parse_file(source(changes), HOUR, airnow.Bounds())


def test_bad_or_incomplete_files_cannot_withdraw_data():
    for content in (
        b"",
        b"<html>error</html>",
        source({}, {}),
        source({"Latitude": "0"}),
        source() + b"truncated,row\n",
    ):
        with pytest.raises(ValueError):
            airnow.parse_file(content, HOUR, airnow.Bounds())
    old_date = source({"ValidDate": "11/01/26"})
    assert airnow.parse_file(old_date, HOUR, airnow.Bounds())[0].observed_at_utc == HOUR


def test_window_crosses_midnight_and_dst_without_local_conversion():
    end = HOUR + timedelta(hours=1)
    hours = airnow.hour_window(end, 72)
    assert len(hours) == len(set(hours)) == 72
    assert hours[-1] == end
    assert hours[0] == end - timedelta(hours=71)
    assert all(b - a == timedelta(hours=1) for a, b in zip(hours, hours[1:]))
    local = [hour.astimezone(ZoneInfo("America/Chicago")) for hour in hours[-2:]]
    assert [(hour.hour, hour.fold) for hour in local] == [(1, 0), (1, 1)]
    for text in (
        "2026-11-01T01:00:00",
        "2026-11-01T01:00:00-05:00",
        "2026-11-01T06:30:00Z",
    ):
        with pytest.raises(ValueError):
            airnow.utc_hour(text)
    for count in (0, 73):
        with pytest.raises(ValueError):
            airnow.hour_window(HOUR, count)


class MemoryBucket:
    name = "test-bucket"

    def __init__(self, race=False):
        self.objects = {}
        self.race = race

    def blob(self, name, generation=None):
        bucket = self

        class Blob:
            metadata = {}

            def reload(self):
                if name not in bucket.objects:
                    raise NotFound("not found")
                content, metadata, generation = bucket.objects[name]
                self.size = len(content)
                self.md5_hash = base64.b64encode(hashlib.md5(content).digest()).decode()
                self.metadata = metadata
                self.generation = generation

            def download_as_bytes(self, **kwargs):
                assert kwargs["checksum"] == "crc32c"
                return bucket.objects[name][0]

            def upload_from_string(self, content, **kwargs):
                assert kwargs["if_generation_match"] == 0
                assert kwargs["checksum"] == "crc32c"
                assert name not in bucket.objects
                bucket.objects[name] = (content, self.metadata, len(bucket.objects) + 1)
                if bucket.race:
                    raise PreconditionFailed("concurrent identical create")

        blob = Blob()
        blob.name = name
        return blob


def test_verifier_reconciles_exact_source_generation_and_detects_drift():
    bucket = MemoryBucket()
    snap = snapshot()
    artifact = airnow.archive_snapshot(bucket, snap, "raw/airnow")
    observations = airnow.parse_file(snap.content, HOUR, airnow.Bounds())
    rows = [
        (
            *asdict(obs).values(),
            snap.retrieved_at_utc,
            artifact["raw_uri"],
            artifact["source_generation"],
            True,
        )
        for obs in observations
    ]

    def verify(test_rows):
        conn = MagicMock()
        cur = conn.cursor.return_value
        cur.fetchall.side_effect = [test_rows, []]
        cur.fetchone.return_value = (0,)
        return airnow_verify.verify(conn, bucket, [HOUR])

    assert verify(rows)["status"] == "passed"
    wrong = [list(row) for row in rows]
    wrong[0][4] = 999
    assert "SQL/source mismatch" in verify(wrong)["mismatches"][0]
    assert "Missing SQL key" in verify(rows[:-1])["mismatches"][0]
    assert verify([])["missing_hours"] == [HOUR.isoformat()]
    wrong = [list(row) for row in rows]
    wrong[0][-1] = False
    assert verify(wrong)["non_preliminary_count"] == 1


@pytest.mark.parametrize("race", [False, True])
def test_archive_is_immutable_and_content_deduplicated(race):
    bucket = MemoryBucket(race)
    first = airnow.archive_snapshot(bucket, snapshot(), "raw/airnow")
    again = airnow.archive_snapshot(bucket, snapshot(), "raw/airnow")
    assert not again["raw_created"]
    assert again["source_generation"] == first["source_generation"]
    revised = airnow.archive_snapshot(
        bucket, snapshot(content=source({"PM25": "4"})), "raw/airnow"
    )
    assert len(bucket.objects) == 2
    assert first["raw_uri"] != revised["raw_uri"]
    name = first["raw_uri"].split("/", 3)[3]
    bucket.objects[name] = (b"corrupted", {}, 1)
    with pytest.raises(ValueError, match="checksum mismatch"):
        airnow.archive_snapshot(bucket, snapshot(), "raw/airnow")


def test_fetch_raises_missing_hour_and_retries_transient_errors():
    with airnow.http_session() as session:
        retry = session.get_adapter("https://").max_retries
        assert retry.total == 3 and 503 in retry.status_forcelist
    session = MagicMock()
    response = session.get.return_value.__enter__.return_value
    response.raise_for_status.side_effect = requests.HTTPError("404 missing")
    with pytest.raises(requests.HTTPError):
        airnow.fetch_hour(session, HOUR)
    response.raise_for_status.side_effect = None
    response.content = source()
    fetched = airnow.fetch_hour(session, HOUR)
    assert fetched.url.endswith("2026/20261101/HourlyAQObs_2026110106.dat")
    assert fetched.content == source()


@pytest.mark.skipif(
    not os.environ.get("AIR_QUALITY_TEST_CLOUDSQL"),
    reason="Opt in to disposable Cloud SQL schema integration",
)
def test_real_sql_idempotency_revision_withdrawal_stale_and_failure(monkeypatch):
    schema = "s09_test_" + uuid.uuid4().hex
    artifact = {"raw_uri": "gs://test/raw/first.dat", "source_generation": "1"}
    initial = snapshot()
    rows = airnow.parse_file(initial.content, HOUR, airnow.Bounds())
    with db.connect() as conn, closing(conn.cursor()) as cur:
        try:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}"')
            db.migrate(conn)
            with airnow.collector_lock(conn):
                first = airnow.save_hour(conn, rows, initial, artifact)
                assert first["inserted"] == 3
                newer = replace(
                    initial,
                    retrieved_at_utc=initial.retrieved_at_utc + timedelta(minutes=1),
                )
                assert airnow.save_hour(conn, rows, newer, artifact)["unchanged"] == 3
                changed = [replace(rows[0], concentration=12.5, aqi=50), *rows[1:]]
                newest = replace(
                    newer,
                    retrieved_at_utc=newer.retrieved_at_utc + timedelta(minutes=1),
                )
                second = {**artifact, "source_generation": "2"}
                assert airnow.save_hour(conn, changed, newest, second)["revised"] == 1
                assert airnow.save_hour(conn, rows, initial, artifact)["stale"] == 3
                cur.execute(
                    "SELECT concentration,aqi,source_generation "
                    "FROM airnow_obs_revision"
                )
                assert tuple(cur.fetchone()) == (-0.7, 3, "1")
                later = replace(
                    newest,
                    retrieved_at_utc=newest.retrieved_at_utc + timedelta(minutes=1),
                )
                result = airnow.save_hour(conn, changed[1:], later, second)
                assert result["withdrawn"] == result["revised"] == 1
                cur.execute("SELECT count(*),count(concentration) FROM airnow_obs")
                assert tuple(cur.fetchone()) == (3, 1)
                cur.execute("SELECT count(*) FROM airnow_obs_revision")
                assert cur.fetchone()[0] == 2
                # Failure in a later row rolls back an earlier row's revision.
                bad = [
                    replace(changed[0], concentration=20),
                    replace(changed[1], pollutant="INVALID"),
                ]
                with pytest.raises(Exception):
                    airnow.save_hour(
                        conn,
                        bad,
                        replace(
                            later,
                            retrieved_at_utc=later.retrieved_at_utc
                            + timedelta(minutes=1),
                        ),
                        second,
                    )
                cur.execute("SELECT count(*) FROM airnow_obs_revision")
                assert cur.fetchone()[0] == 2
                conn.commit()

            # Missing source hours are failed runs with no fabricated data.
            def missing(*args):
                raise requests.HTTPError("404 missing hour")

            monkeypatch.setattr(airnow, "fetch_hour", missing)
            report = airnow.collect(
                conn, MemoryBucket(), None, [HOUR], airnow.Bounds(), "raw/airnow"
            )
            assert report["status"] == "failed" and len(report["errors"]) == 1
            cur.execute(
                "SELECT status FROM ingest_run WHERE run_id=%s", (report["run_id"],)
            )
            assert cur.fetchone()[0] == "failed"
        finally:
            conn.rollback()
            cur.execute("SET search_path TO public")
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()
