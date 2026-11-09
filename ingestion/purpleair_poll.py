"""S08: PurpleAir poller (provisional: hourly, whole Chicago bounding box).

One run is one ``/v1/sensors`` call. The response is saved unchanged, readings
are upserted (with ``--db``), and one poll record is written. Schedule runs
externally (Cloud Scheduler, launchd); this script does not loop.

Safe by default: without flags it only writes the raw JSON under
``tmp/gcs_mirror/`` (same layout as the bucket) and a local JSONL poll log.

    python -m ingestion.purpleair_poll                  # local raw files only
    python -m ingestion.purpleair_poll --db --raw-store gcs   # Cloud Run job

The API key comes from ``AIR_QUALITY_PURPLEAIR_READ_KEY`` (environment or an
ignored ``.env``) and is never logged or stored. ``--fixture`` replays a saved
response offline.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import yaml

from ingestion import db
from ingestion import purpleair_check as pa

ROOT = Path(__file__).resolve().parents[1]
JOB_NAME = "purpleair_poll"
REQUIRED_COLUMNS = ("sensor_index", "last_seen")
Fetch = Callable[[], "tuple[int, int, bytes]"]  # (http_status, latency_ms, body)


@dataclass(frozen=True)
class Reading:
    sensor_id: int
    report_ts: datetime
    latitude: float | None
    longitude: float | None
    is_outdoor: bool
    pm25_a: float | None
    pm25_b: float | None
    humidity: float | None
    stale: bool


@dataclass
class Stats:
    outcome: str = "success"
    http_status: int | None = None
    latency_ms: int | None = None
    sensors_seen: int = 0
    rows_inserted: int = 0
    duplicate_reports: int | None = 0
    late_reports: int | None = 0
    stale_reports: int = 0
    raw_uri: str | None = None
    error_message: str | None = None


# --- request / parse --------------------------------------------------------


def make_fetch(key: str, fields: list[str], max_age: int, timeout: int = 60) -> Fetch:
    def fetch() -> tuple[int, int, bytes]:
        params = {
            "fields": ",".join(fields),
            "location_type": 0,  # outdoor only
            "max_age": max_age,
            **pa.BBOX,
        }
        url = f"{pa.API}/sensors?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url, headers={"X-API-Key": key})
        started = time.monotonic()
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            status = response.status
        return status, round((time.monotonic() - started) * 1000), body

    return fetch


def classify_error(exc: BaseException) -> tuple[str, int | None]:
    """Map an exception to a pa_poll_log outcome and optional HTTP status."""
    if isinstance(exc, urllib.error.HTTPError):  # subclass of URLError: check first
        return "http_error", exc.code
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return "timeout", None
    if isinstance(exc, urllib.error.URLError):
        if isinstance(exc.reason, (TimeoutError, socket.timeout)):
            return "timeout", None
        return "other_error", None
    if isinstance(exc, (ValueError, KeyError)):  # includes JSON decode errors
        return "invalid_response", None
    return "other_error", None


def parse_payload(body: bytes) -> dict:
    payload = json.loads(body)
    if not isinstance(payload, dict) or not {"fields", "data"} <= payload.keys():
        raise ValueError("Unexpected PurpleAir response shape")
    missing = [c for c in REQUIRED_COLUMNS if c not in payload["fields"]]
    if missing:
        raise ValueError(f"Response lacks required columns: {', '.join(missing)}")
    return payload


def _num(value: object) -> float | None:
    return None if value is None else float(value)


def readings_from(payload: dict, stale_after: int) -> tuple[list[Reading], int]:
    """Return usable readings and the count of rows without sensor id/report time.

    A reading is *stale* when the snapshot is more than ``stale_after`` seconds
    newer than its ``last_seen``. Stale readings are kept; S16 decides on QC.
    """
    columns = payload["fields"]
    snapshot = payload.get("data_time_stamp") or payload.get("time_stamp")
    readings: list[Reading] = []
    unusable = 0
    for raw in payload["data"]:
        row = dict(zip(columns, raw))
        if len(raw) < len(columns) or None in (
            row.get("sensor_index"),
            row.get("last_seen"),
        ):
            unusable += 1
            continue
        last_seen = int(row["last_seen"])
        location_type = row.get("location_type")
        readings.append(
            Reading(
                sensor_id=int(row["sensor_index"]),
                report_ts=datetime.fromtimestamp(last_seen, UTC),
                latitude=_num(row.get("latitude")),
                longitude=_num(row.get("longitude")),
                # The request filters to outdoor sensors, so absent means outdoor.
                is_outdoor=True if location_type is None else location_type == 0,
                pm25_a=_num(row.get("pm2.5_cf_1_a")),
                pm25_b=_num(row.get("pm2.5_cf_1_b")),
                humidity=_num(row.get("humidity")),
                stale=bool(snapshot) and snapshot - last_seen > stale_after,
            )
        )
    return readings, unusable


# --- raw storage ------------------------------------------------------------


def raw_key(prefix: str, requested_at: datetime, poll_id: str) -> str:
    return (
        f"{prefix}/dt={requested_at:%Y-%m-%d}/hh={requested_at:%H}/"
        f"purpleair_{requested_at:%Y%m%dT%H%M%SZ}_{poll_id[:8]}.json"
    )


def write_raw(key: str, body: bytes, bucket: str | None, local_dir: Path) -> str:
    """Store the response bytes unchanged; return its URI (gs:// or local path)."""
    if bucket:
        try:
            from google.cloud import storage
        except ImportError as exc:
            raise RuntimeError("Install the project gcp extra for GCS") from exc
        blob = storage.Client().bucket(bucket).blob(key)
        blob.upload_from_string(
            body, content_type="application/json", if_generation_match=0
        )
        return f"gs://{bucket}/{key}"
    path = local_dir / key
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(body)
    temporary.replace(path)
    return str(path)


# --- database ---------------------------------------------------------------

UPSERT_SENSOR = """
INSERT INTO pa_sensor
    (sensor_id, latitude, longitude, is_outdoor, first_seen_at_utc, last_seen_at_utc)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (sensor_id) DO UPDATE SET
    latitude = CASE WHEN EXCLUDED.last_seen_at_utc >= pa_sensor.last_seen_at_utc
        THEN COALESCE(EXCLUDED.latitude, pa_sensor.latitude)
        ELSE pa_sensor.latitude END,
    longitude = CASE WHEN EXCLUDED.last_seen_at_utc >= pa_sensor.last_seen_at_utc
        THEN COALESCE(EXCLUDED.longitude, pa_sensor.longitude)
        ELSE pa_sensor.longitude END,
    is_outdoor = CASE WHEN EXCLUDED.last_seen_at_utc >= pa_sensor.last_seen_at_utc
        THEN EXCLUDED.is_outdoor ELSE pa_sensor.is_outdoor END,
    first_seen_at_utc = LEAST(pa_sensor.first_seen_at_utc, EXCLUDED.first_seen_at_utc),
    last_seen_at_utc = GREATEST(pa_sensor.last_seen_at_utc, EXCLUDED.last_seen_at_utc)
"""
INSERT_READING = """
INSERT INTO pa_reading
    (sensor_id, report_ts_utc, arrival_ts_utc, pm25_cf1_a, pm25_cf1_b,
     humidity_pct, raw_uri, source_request_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (sensor_id, report_ts_utc) DO NOTHING
"""
CLOSE_HISTORY = (
    "UPDATE pa_sensor_history SET valid_to_utc = %s "
    "WHERE sensor_id = %s AND valid_to_utc IS NULL"
)
OPEN_HISTORY = """
INSERT INTO pa_sensor_history
    (sensor_id, valid_from_utc, valid_to_utc, latitude, longitude, is_outdoor)
VALUES (%s, %s, NULL, %s, %s, %s)
"""


def _changed(old: tuple, reading: Reading) -> bool:
    """True if reported coordinates or outdoor flag differ from the open interval."""

    def differs(was: float | None, now: float | None) -> bool:
        return now is not None and (was is None or round(was, 5) != round(now, 5))

    lat, lon, outdoor, _ = old
    return (
        differs(lat, reading.latitude)
        or differs(lon, reading.longitude)
        or outdoor != reading.is_outdoor
    )


def store_readings(
    cur, readings: list[Reading], arrival: datetime, raw_uri: str, poll_id: str
) -> tuple[int, int, int]:
    """Upsert sensors, history intervals and readings; return (new, dup, late)."""
    ids = sorted({r.sensor_id for r in readings})
    cur.execute(
        "SELECT sensor_id, last_seen_at_utc FROM pa_sensor WHERE sensor_id = ANY(%s)",
        (ids,),
    )
    previous = {row[0]: row[1] for row in cur.fetchall()}
    cur.execute(
        "SELECT sensor_id, latitude, longitude, is_outdoor, valid_from_utc "
        "FROM pa_sensor_history WHERE valid_to_utc IS NULL AND sensor_id = ANY(%s)",
        (ids,),
    )
    current = {row[0]: tuple(row[1:]) for row in cur.fetchall()}

    inserted = duplicates = late = 0
    for r in sorted(readings, key=lambda x: (x.sensor_id, x.report_ts)):
        cur.execute(
            UPSERT_SENSOR,
            (
                r.sensor_id,
                r.latitude,
                r.longitude,
                r.is_outdoor,
                r.report_ts,
                r.report_ts,
            ),
        )
        old = current.get(r.sensor_id)
        if old is None:
            # First observation: never invent an earlier effective date.
            cur.execute(
                OPEN_HISTORY,
                (r.sensor_id, r.report_ts, r.latitude, r.longitude, r.is_outdoor),
            )
            current[r.sensor_id] = (r.latitude, r.longitude, r.is_outdoor, r.report_ts)
        elif _changed(old, r) and r.report_ts > old[3]:
            lat = r.latitude if r.latitude is not None else old[0]
            lon = r.longitude if r.longitude is not None else old[1]
            cur.execute(CLOSE_HISTORY, (r.report_ts, r.sensor_id))
            cur.execute(
                OPEN_HISTORY, (r.sensor_id, r.report_ts, lat, lon, r.is_outdoor)
            )
            current[r.sensor_id] = (lat, lon, r.is_outdoor, r.report_ts)
        cur.execute(
            INSERT_READING,
            (
                r.sensor_id,
                r.report_ts,
                arrival,
                r.pm25_a,
                r.pm25_b,
                r.humidity,
                raw_uri,
                poll_id,
            ),
        )
        if cur.rowcount == 0:
            duplicates += 1
        else:
            inserted += 1
            last = previous.get(r.sensor_id)
            if last is not None and r.report_ts < last:
                late += 1
    return inserted, duplicates, late


def _log_poll(cur, poll_id, run_id, requested_at, completed_at, stats: Stats) -> None:
    cur.execute(
        """INSERT INTO pa_poll_log
           (poll_id, run_id, requested_at_utc, completed_at_utc, outcome,
            http_status, latency_ms, sensors_seen, rows_inserted,
            duplicate_reports, late_reports, stale_reports, raw_uri, error_message)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            poll_id,
            run_id,
            requested_at,
            completed_at,
            stats.outcome,
            stats.http_status,
            stats.latency_ms,
            stats.sensors_seen,
            stats.rows_inserted,
            stats.duplicate_reports or 0,
            stats.late_reports or 0,
            stats.stale_reports,
            stats.raw_uri,
            stats.error_message,
        ),
    )


def _finish_run(cur, run_id, finished_at, stats: Stats) -> None:
    cur.execute(
        """UPDATE ingest_run SET status = %s, finished_at_utc = %s,
           input_count = %s, output_count = %s, error_message = %s
           WHERE run_id = %s""",
        (
            "succeeded" if stats.outcome == "success" else "failed",
            finished_at,
            stats.sensors_seen,
            stats.rows_inserted,
            stats.error_message,
            run_id,
        ),
    )


# --- one poll ---------------------------------------------------------------


def poll(
    conn,
    fetch: Fetch,
    *,
    raw_prefix: str,
    bucket: str | None,
    local_dir: Path,
    stale_after: int,
    details: dict,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Stats:
    """Run one poll. ``conn`` is a DB-API connection or None (raw files only)."""
    stats = Stats()
    poll_id, run_id = str(uuid.uuid4()), str(uuid.uuid4())
    requested_at = now()
    if conn is not None:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO ingest_run
               (run_id, job_name, status, started_at_utc, details)
               VALUES (%s, %s, 'running', %s, CAST(%s AS jsonb))""",
            (run_id, JOB_NAME, requested_at, json.dumps(details)),
        )
        conn.commit()
        cur.close()
    completed_at = requested_at
    try:
        status, latency_ms, body = fetch()
        stats.http_status, stats.latency_ms = status, latency_ms
        completed_at = now()
        payload = parse_payload(body)
        readings, unusable = readings_from(payload, stale_after)
        stats.sensors_seen = len(payload["data"])
        stats.stale_reports = unusable + sum(r.stale for r in readings)
        stats.raw_uri = write_raw(
            raw_key(raw_prefix, requested_at, poll_id), body, bucket, local_dir
        )
        if conn is not None:
            cur = conn.cursor()
            try:
                inserted, duplicates, late = store_readings(
                    cur, readings, completed_at, stats.raw_uri, poll_id
                )
                stats.rows_inserted = inserted
                stats.duplicate_reports, stats.late_reports = duplicates, late
                _log_poll(cur, poll_id, run_id, requested_at, completed_at, stats)
                _finish_run(cur, run_id, completed_at, stats)
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                cur.close()
        else:
            stats.duplicate_reports = stats.late_reports = None  # needs the database
    except Exception as exc:
        stats.outcome, status = classify_error(exc)
        stats.http_status = status or stats.http_status
        stats.error_message = f"{type(exc).__name__}: {exc}"[:500]
        stats.rows_inserted = 0
        if conn is not None:
            conn.rollback()
            cur = conn.cursor()
            _log_poll(cur, poll_id, run_id, requested_at, now(), stats)
            _finish_run(cur, run_id, now(), stats)
            conn.commit()
            cur.close()
    if conn is None:
        record = {"poll_id": poll_id, "requested_at_utc": requested_at.isoformat()}
        record.update(asdict(stats))
        log = local_dir / "poll_log.jsonl"
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
    return stats


STOP_AFTER_VAR = "AIR_QUALITY_PURPLEAIR_STOP_AFTER_UTC"


def window_closed(stop_after: str | None, now: datetime) -> bool:
    """True once ``now`` reaches the optional cutoff (ISO 8601 with offset or Z).

    A closed window makes the job exit before any API call, so a forgotten
    schedule spends no PurpleAir points after the test period.
    """
    if not stop_after:
        return False
    cutoff = datetime.fromisoformat(stop_after.strip().replace("Z", "+00:00"))
    if cutoff.tzinfo is None:
        raise ValueError(f"{STOP_AFTER_VAR} must include a UTC offset or 'Z'")
    return now >= cutoff


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", action="store_true", help="write to Cloud SQL/Postgres")
    parser.add_argument("--raw-store", choices=("local", "gcs"), default="local")
    parser.add_argument("--raw-dir", type=Path, default=ROOT / "tmp" / "gcs_mirror")
    parser.add_argument("--fixture", type=Path, help="saved response; no network")
    parser.add_argument("--max-age", type=int)
    parser.add_argument("--stale-after", type=int)
    args = parser.parse_args(argv)

    settings = yaml.safe_load((ROOT / "config/settings.yaml").read_text())
    section = settings["purpleair"]
    fields = ["sensor_index", *section["fields"]]
    max_age = args.max_age or int(section["max_age_seconds"])
    stale_after = args.stale_after or int(section["stale_after_seconds"])
    pa.load_env()
    if window_closed(os.environ.get(STOP_AFTER_VAR), datetime.now(UTC)):
        print(f"poll skipped: {STOP_AFTER_VAR} has passed; no request made")
        return 0
    if args.fixture:
        body = args.fixture.read_bytes()
        fetch: Fetch = lambda: (200, 0, body)  # noqa: E731
    else:
        key = os.environ.get(pa.KEY_VAR)
        if not key:
            print(f"{pa.KEY_VAR} is not set (put it in .env).", file=sys.stderr)
            return 2
        fetch = make_fetch(key, section["fields"], max_age)
    bucket = None
    if args.raw_store == "gcs":
        bucket = os.environ.get("AIR_QUALITY_GCS_BUCKET")
        if not bucket:
            print("AIR_QUALITY_GCS_BUCKET is not set.", file=sys.stderr)
            return 2
    details = {
        "fields": fields,
        "max_age_seconds": max_age,
        "stale_after_seconds": stale_after,
        "poll_seconds": section["target_poll_seconds"],
        "bbox": pa.BBOX,
    }
    with db.connect() if args.db else nullcontext(None) as conn:
        stats = poll(
            conn,
            fetch,
            raw_prefix=settings["gcs"]["raw_purpleair_prefix"],
            bucket=bucket,
            local_dir=args.raw_dir,
            stale_after=stale_after,
            details=details,
        )
    print(
        f"poll outcome={stats.outcome} http={stats.http_status} "
        f"latency_ms={stats.latency_ms} sensors={stats.sensors_seen} "
        f"inserted={stats.rows_inserted} duplicates={stats.duplicate_reports} "
        f"late={stats.late_reports} stale={stats.stale_reports} raw={stats.raw_uri}"
    )
    if stats.error_message:
        print(f"error: {stats.error_message}", file=sys.stderr)
    return 0 if stats.outcome == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
