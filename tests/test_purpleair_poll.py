"""S08 poller tests: offline logic always; PostgreSQL scenarios are opt-in."""

import json
import os
import urllib.error
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from ingestion import db
from ingestion import purpleair_check as pa
from ingestion import purpleair_poll as pp

FIELDS = [
    "sensor_index",
    "last_seen",
    "location_type",
    "latitude",
    "longitude",
    "humidity",
    "pm2.5_cf_1_a",
    "pm2.5_cf_1_b",
]
SNAPSHOT = 1_800_000_000  # seconds


def payload(rows, snapshot=SNAPSHOT):
    return {
        "api_version": "V1.test",
        "time_stamp": snapshot,
        "data_time_stamp": snapshot,
        "max_age": 900,
        "fields": FIELDS,
        "data": rows,
    }


def row(sensor, age=60, lat=41.9, lon=-87.6, humidity=50.0, a=10.0, b=11.0, loc=0):
    return [sensor, SNAPSHOT - age, loc, lat, lon, humidity, a, b]


def body(rows, snapshot=SNAPSHOT):
    return json.dumps(payload(rows, snapshot)).encode()


def fetcher(data):
    return lambda: (200, 123, data)


def make_now():
    state = {"n": 0}
    base = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)

    def now():
        state["n"] += 1
        return base + timedelta(seconds=state["n"])

    return now


def kwargs(tmp_path):
    return {
        "raw_prefix": "raw/purpleair",
        "bucket": None,
        "local_dir": tmp_path,
        "stale_after": 600,
        "details": {"test": True},
        "now": make_now(),
    }


# --- offline logic ----------------------------------------------------------


def test_readings_flag_stale_skip_unusable_and_convert_time():
    rows = [
        row(1, age=60),
        row(2, age=601),  # stale
        row(3, age=600),  # exactly at the limit: not stale
        [None, SNAPSHOT, 0, 41.9, -87.6, 50.0, 1.0, 1.0],  # no sensor id
        [5, None, 0, 41.9, -87.6, 50.0, 1.0, 1.0],  # no report time
        row(6, humidity=None, a=None),
    ]
    readings, unusable = pp.readings_from(payload(rows), stale_after=600)
    assert unusable == 2
    assert [r.sensor_id for r in readings] == [1, 2, 3, 6]
    assert [r.stale for r in readings] == [False, True, False, False]
    assert readings[0].report_ts == datetime.fromtimestamp(SNAPSHOT - 60, UTC)
    assert readings[3].humidity is None and readings[3].pm25_a is None
    assert all(r.is_outdoor for r in readings)


def test_parse_payload_rejects_bad_shapes():
    with pytest.raises(ValueError):
        pp.parse_payload(b"not json")
    with pytest.raises(ValueError, match="shape"):
        pp.parse_payload(b'{"foo": 1}')
    bad = payload([])
    bad["fields"] = ["sensor_index"]
    with pytest.raises(ValueError, match="last_seen"):
        pp.parse_payload(json.dumps(bad).encode())


def test_error_classification():
    http = urllib.error.HTTPError("u", 403, "Forbidden", {}, None)
    assert pp.classify_error(http) == ("http_error", 403)
    assert pp.classify_error(TimeoutError()) == ("timeout", None)
    assert pp.classify_error(urllib.error.URLError(TimeoutError())) == (
        "timeout",
        None,
    )
    assert pp.classify_error(urllib.error.URLError("dns")) == ("other_error", None)
    assert pp.classify_error(json.JSONDecodeError("x", "y", 0)) == (
        "invalid_response",
        None,
    )
    assert pp.classify_error(RuntimeError("boom")) == ("other_error", None)


def test_raw_key_matches_the_gcs_layout():
    when = datetime(2026, 11, 9, 7, 5, 3, tzinfo=UTC)
    key = pp.raw_key("raw/purpleair", when, "abcdef12-0000")
    assert key == (
        "raw/purpleair/dt=2026-11-09/hh=07/purpleair_20261109T070503Z_abcdef12.json"
    )


def test_local_poll_saves_raw_bytes_unchanged_and_logs(tmp_path):
    data = body([row(1), row(2, age=700)])
    stats = pp.poll(None, fetcher(data), **kwargs(tmp_path))
    assert stats.outcome == "success"
    assert (stats.sensors_seen, stats.stale_reports) == (2, 1)
    assert stats.duplicate_reports is None  # unknown without the database
    saved = [p for p in tmp_path.rglob("*.json")]
    assert len(saved) == 1 and saved[0].read_bytes() == data
    assert "dt=2026-11-09/hh=12" in saved[0].as_posix()
    lines = (tmp_path / "poll_log.jsonl").read_text().splitlines()
    assert json.loads(lines[0])["outcome"] == "success"


def test_local_failure_is_logged_without_raw_file(tmp_path):
    def broken():
        raise urllib.error.HTTPError("u", 403, "Forbidden", {}, None)

    stats = pp.poll(None, broken, **kwargs(tmp_path))
    assert (stats.outcome, stats.http_status) == ("http_error", 403)
    assert not list(tmp_path.rglob("*.json"))
    record = json.loads((tmp_path / "poll_log.jsonl").read_text().splitlines()[0])
    assert record["outcome"] == "http_error" and record["http_status"] == 403


def test_main_never_writes_the_key_anywhere(tmp_path, monkeypatch, capsys):
    secret = "SECRET-KEY-DO-NOT-LEAK"
    monkeypatch.setattr(pa, "load_env", lambda: None)
    monkeypatch.setenv(pa.KEY_VAR, secret)
    fixture = tmp_path / "fixture.json"
    fixture.write_bytes(body([row(1)]))
    code = pp.main(["--fixture", str(fixture), "--raw-dir", str(tmp_path / "out")])
    captured = capsys.readouterr()
    assert code == 0
    assert secret not in captured.out + captured.err
    for path in (tmp_path / "out").rglob("*"):
        if path.is_file():
            assert secret not in path.read_text()


def test_main_requires_a_key_for_live_calls(monkeypatch, capsys):
    monkeypatch.setattr(pa, "load_env", lambda: None)
    monkeypatch.delenv(pa.KEY_VAR, raising=False)
    assert pp.main([]) == 2
    assert "not set" in capsys.readouterr().err


# --- PostgreSQL scenarios (opt in with a disposable DSN) ---------------------

needs_postgres = pytest.mark.skipif(
    not os.environ.get("AIR_QUALITY_TEST_POSTGRES_DSN"),
    reason="Set AIR_QUALITY_TEST_POSTGRES_DSN for local PostgreSQL integration",
)


@pytest.fixture
def pg(monkeypatch):
    monkeypatch.setenv(
        "AIR_QUALITY_CLOUDSQL_DSN", os.environ["AIR_QUALITY_TEST_POSTGRES_DSN"]
    )
    schema = "s08_test_" + uuid.uuid4().hex
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(f'CREATE SCHEMA "{schema}"')
        cur.execute(f'SET search_path TO "{schema}"')
        db.migrate(conn)
        try:
            yield conn
        finally:
            conn.rollback()
            cur.execute("SET search_path TO public")
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()
            cur.close()


def one(conn, sql, args=()):
    cur = conn.cursor()
    cur.execute(sql, args)
    result = cur.fetchall()
    cur.close()
    return result


@needs_postgres
def test_poll_dedupes_flags_late_tracks_history_and_recovers(pg, tmp_path):
    kw = kwargs(tmp_path)

    # 1. First poll: three sensors, one stale, one missing humidity.
    first = body([row(1), row(2, age=700), row(3, humidity=None)])
    s1 = pp.poll(pg, fetcher(first), **kw)
    assert (s1.outcome, s1.rows_inserted, s1.duplicate_reports) == ("success", 3, 0)
    assert (s1.sensors_seen, s1.stale_reports, s1.late_reports) == (3, 1, 0)
    assert one(pg, "SELECT count(*) FROM pa_reading")[0][0] == 3
    assert (
        one(pg, "SELECT count(*) FROM pa_sensor_history WHERE valid_to_utc IS NULL")[0][
            0
        ]
        == 3
    )
    # Humidity stays NULL (no silent exclusion) and the poll id is the request id.
    assert (
        one(pg, "SELECT humidity_pct FROM pa_reading WHERE sensor_id = 3")[0][0] is None
    )

    # 2. Identical response: every reading is a duplicate, nothing new.
    s2 = pp.poll(pg, fetcher(first), **kw)
    assert (s2.rows_inserted, s2.duplicate_reports) == (0, 3)
    assert one(pg, "SELECT count(*) FROM pa_reading")[0][0] == 3
    assert one(pg, "SELECT count(*) FROM pa_sensor_history")[0][0] == 3

    # 3. A newer report from sensor 1 that moved: history interval closes + opens.
    moved = pp.poll(pg, fetcher(body([row(1, age=10, lat=42.0)])), **kw)
    assert moved.rows_inserted == 1
    history = one(
        pg,
        "SELECT latitude, valid_to_utc IS NULL FROM pa_sensor_history "
        "WHERE sensor_id = 1 ORDER BY valid_from_utc",
    )
    assert [tuple(h) for h in history] == [(41.9, False), (42.0, True)]
    assert one(pg, "SELECT latitude FROM pa_sensor WHERE sensor_id = 1")[0][0] == 42.0

    # 4. A late arrival: a report older than the last one we saw for sensor 1.
    late = pp.poll(pg, fetcher(body([row(1, age=300)])), **kw)
    assert (late.rows_inserted, late.late_reports) == (1, 1)
    # The late report must not rewind the sensor's last-seen or coordinates.
    assert one(pg, "SELECT latitude FROM pa_sensor WHERE sensor_id = 1")[0][0] == 42.0

    # 5. Outage: HTTP 403 is logged, nothing inserted; the next poll recovers.
    def forbidden():
        raise urllib.error.HTTPError("u", 403, "Forbidden", {}, None)

    before = one(pg, "SELECT count(*) FROM pa_reading")[0][0]
    down = pp.poll(pg, forbidden, **kw)
    assert (down.outcome, down.http_status, down.rows_inserted) == (
        "http_error",
        403,
        0,
    )
    assert one(pg, "SELECT count(*) FROM pa_reading")[0][0] == before
    recovered = pp.poll(pg, fetcher(body([row(4)])), **kw)
    assert (recovered.outcome, recovered.rows_inserted) == ("success", 1)

    # 6. Garbage response is recorded as invalid_response.
    bad = pp.poll(pg, fetcher(b"<html>oops</html>"), **kw)
    assert bad.outcome == "invalid_response"

    # Every poll is logged exactly once; run state matches the outcome.
    outcomes = [
        r[0]
        for r in one(pg, "SELECT outcome FROM pa_poll_log ORDER BY requested_at_utc")
    ]
    assert outcomes == [
        "success",
        "success",
        "success",
        "success",
        "http_error",
        "success",
        "invalid_response",
    ]
    statuses = [
        r[0] for r in one(pg, "SELECT status FROM ingest_run ORDER BY started_at_utc")
    ]
    assert statuses.count("failed") == 2 and statuses.count("succeeded") == 5
    # Counters on the failed poll are zero, and 403 is stored.
    assert one(
        pg,
        "SELECT http_status, rows_inserted FROM pa_poll_log "
        "WHERE outcome = 'http_error'",
    )[0] == [403, 0]


# --- test-window cutoff ------------------------------------------------------


def test_window_closed_boundaries():
    cutoff = "2026-11-12T06:00:00Z"
    before = datetime(2026, 11, 12, 5, 59, 59, tzinfo=UTC)
    at = datetime(2026, 11, 12, 6, 0, 0, tzinfo=UTC)
    assert pp.window_closed(None, at) is False
    assert pp.window_closed("", at) is False
    assert pp.window_closed(cutoff, before) is False
    assert pp.window_closed(cutoff, at) is True
    assert pp.window_closed("2026-11-12T01:00:00-05:00", at) is True  # = 06:00 UTC


def test_window_cutoff_requires_timezone():
    with pytest.raises(ValueError, match="UTC offset"):
        pp.window_closed("2026-11-12T06:00:00", datetime.now(UTC))


def test_main_exits_without_request_after_cutoff(monkeypatch, capsys):
    monkeypatch.setenv(pp.STOP_AFTER_VAR, "2000-01-01T00:00:00Z")
    monkeypatch.delenv(pa.KEY_VAR, raising=False)  # would fail if it got further
    assert pp.main([]) == 0
    assert "no request made" in capsys.readouterr().out
