"""S08: PurpleAir poller with optional fixed-cohort collection.

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
import math
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
from dataclasses import asdict, dataclass, field
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
    is_outdoor: bool | None
    pm25_a: float | None
    pm25_b: float | None
    humidity: float | None
    stale: bool
    quality_flags: tuple[str, ...] = ()
    response_age_seconds: float | None = None


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
    missing_sensor_ids: list[int] = field(default_factory=list)
    unexpected_sensor_ids: list[int] = field(default_factory=list)
    valid_sensor_ids: list[int] = field(default_factory=list)
    quality_flags: dict[int, list[str]] = field(default_factory=dict)
    raw_only_reports: int = 0


# --- request / parse --------------------------------------------------------


def sensor_allowlist(value: str | list[int] | None) -> list[int] | None:
    """An omitted roster means discovery; an empty/invalid roster never does."""
    if value is None:
        return None
    parts = value.split(",") if isinstance(value, str) else value
    if not parts or any(not str(v).strip().isdigit() for v in parts):
        raise ValueError("Sensor allowlist must contain positive integer IDs")
    ids = [int(v) for v in parts]
    if min(ids) <= 0 or len(ids) != len(set(ids)) or len(ids) > 20:
        raise ValueError("Sensor allowlist must contain 1–20 distinct positive IDs")
    return ids


def make_fetch(
    key: str,
    fields: list[str],
    max_age: int,
    timeout: int = 60,
    *,
    sensor_ids: list[int] | None = None,
) -> Fetch:
    sensor_ids = sensor_allowlist(sensor_ids)

    def fetch() -> tuple[int, int, bytes]:
        params = {
            "fields": ",".join(fields),
            "location_type": 0,  # outdoor only
            "max_age": max_age,
        }
        if sensor_ids is None:
            params.update(pa.BBOX)
        else:
            params["show_only"] = ",".join(map(str, sensor_ids))
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

    Freshness uses response ``time_stamp``, never the older cached snapshot.
    Invalid/future ages and missing QC fields fail closed. All raw readings
    remain stored; only unflagged readings count toward analytical coverage.
    """
    columns = payload["fields"]
    response_time = payload.get("time_stamp")
    if (
        not isinstance(response_time, (int, float))
        or isinstance(response_time, bool)
        or not math.isfinite(response_time)
        or response_time <= 0
    ):
        raise ValueError("Response lacks a valid time_stamp for freshness checks")
    readings: list[Reading] = []
    unusable = 0
    for raw in payload["data"]:
        row = dict(zip(columns, raw))
        if len(raw) != len(columns) or None in (
            row.get("sensor_index"),
            row.get("last_seen"),
        ):
            unusable += 1
            continue
        last_seen = int(row["last_seen"])
        location_type = row.get("location_type")
        age = response_time - last_seen
        stale = not 0 <= age <= stale_after
        a, b, humidity = (
            _num(row.get(k)) for k in ("pm2.5_cf_1_a", "pm2.5_cf_1_b", "humidity")
        )
        flags = []
        if stale:
            flags.append("future_report" if age < 0 else "stale_response")
        if location_type != 0:
            flags.append("not_confirmed_outdoor")
        if a is None or b is None:
            flags.append("missing_pm_channel")
        elif not all(math.isfinite(v) and v >= 0 for v in (a, b)):
            flags.append("invalid_pm_channel")
        elif abs(a - b) > max(5, 0.2 * ((a + b) / 2)):
            flags.append("ab_disagreement")
        if humidity is None or not math.isfinite(humidity) or not 0 <= humidity <= 100:
            flags.append("invalid_humidity")
        readings.append(
            Reading(
                sensor_id=int(row["sensor_index"]),
                report_ts=datetime.fromtimestamp(last_seen, UTC),
                latitude=_num(row.get("latitude")),
                longitude=_num(row.get("longitude")),
                is_outdoor=location_type == 0 if location_type in (0, 1) else None,
                pm25_a=a,
                pm25_b=b,
                humidity=humidity,
                stale=stale,
                quality_flags=tuple(flags),
                response_age_seconds=age,
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
        or (reading.is_outdoor is not None and outdoor != reading.is_outdoor)
    )


def store_readings(
    cur, readings: list[Reading], arrival: datetime, raw_uri: str, poll_id: str
) -> tuple[int, int, int]:
    """Upsert sensors, history intervals and readings; return (new, dup, late)."""
    # Unknown status cannot safely populate the NOT NULL metadata/history fields.
    # Future reports must not advance last_seen and poison subsequent history.
    # Both remain in the immutable raw response and quality audit.
    readings = [
        r
        for r in readings
        if r.is_outdoor is not None and "future_report" not in r.quality_flags
    ]
    if not readings:
        return 0, 0, 0
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

    # Newest report already stored per sensor. A location change may only be
    # applied by a report newer than this; a late report must not rewrite the
    # interval that newer reports already established.
    newest = dict(previous)
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
        elif _changed(old, r) and r.report_ts > max(
            old[3], newest.get(r.sensor_id, old[3])
        ):
            lat = r.latitude if r.latitude is not None else old[0]
            lon = r.longitude if r.longitude is not None else old[1]
            cur.execute(CLOSE_HISTORY, (r.report_ts, r.sensor_id))
            cur.execute(
                OPEN_HISTORY, (r.sensor_id, r.report_ts, lat, lon, r.is_outdoor)
            )
            current[r.sensor_id] = (lat, lon, r.is_outdoor, r.report_ts)
        newest[r.sensor_id] = max(newest.get(r.sensor_id, r.report_ts), r.report_ts)
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
           input_count = %s, output_count = %s, error_message = %s,
           details = details || CAST(%s AS jsonb)
           WHERE run_id = %s""",
        (
            "succeeded" if stats.outcome == "success" else "failed",
            finished_at,
            stats.sensors_seen,
            stats.rows_inserted,
            stats.error_message,
            json.dumps({"quality": quality_summary(stats)}),
            run_id,
        ),
    )


def quality_summary(stats: Stats) -> dict:
    return {
        key: getattr(stats, key)
        for key in (
            "missing_sensor_ids",
            "unexpected_sensor_ids",
            "valid_sensor_ids",
            "quality_flags",
            "raw_only_reports",
        )
    }


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
    expected = sensor_allowlist(details.get("sensor_ids"))
    if expected is not None:
        stale_after = min(stale_after, 180)
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
        # Retain the exact response even if its shape, timestamps or IDs fail QC.
        stats.raw_uri = write_raw(
            raw_key(raw_prefix, requested_at, poll_id), body, bucket, local_dir
        )
        payload = parse_payload(body)
        readings, unusable = readings_from(payload, stale_after)
        stats.sensors_seen = len(payload["data"])
        stats.stale_reports = unusable + sum(r.stale for r in readings)
        seen = {r.sensor_id for r in readings}
        stats.raw_only_reports = unusable + sum(
            r.is_outdoor is None or "future_report" in r.quality_flags for r in readings
        )
        if expected is not None:
            stats.missing_sensor_ids = sorted(set(expected) - seen)
            stats.unexpected_sensor_ids = sorted(seen - set(expected))
        stats.valid_sensor_ids = sorted(
            {r.sensor_id for r in readings if not r.quality_flags}
        )
        stats.quality_flags = {
            r.sensor_id: list(r.quality_flags) for r in readings if r.quality_flags
        }
        if stats.unexpected_sensor_ids:
            raise ValueError(
                "Response contains sensors outside the configured allowlist"
            )
        if len(seen) != len(readings):
            raise ValueError("Response contains duplicate sensor IDs")
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
    record = {
        "event": "purpleair_poll",
        "poll_id": poll_id,
        "requested_at_utc": requested_at.isoformat(),
        "completed_at_utc": completed_at.isoformat(),
        "details": details,
        **asdict(stats),
    }
    # Cloud Logging ingests this as jsonPayload; offline review uses the same record.
    print(json.dumps(record))
    if conn is None:
        log = local_dir / "poll_log.jsonl"
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
    return stats


STOP_AFTER_VAR = "AIR_QUALITY_PURPLEAIR_STOP_AFTER_UTC"
NOT_BEFORE_VAR = "AIR_QUALITY_PURPLEAIR_NOT_BEFORE_UTC"
SENSOR_IDS_VAR = "AIR_QUALITY_PURPLEAIR_SENSOR_IDS"


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


def _setting(cli: int | None, env_suffix: str, section: dict, key: str) -> int:
    """Resolve CLI flag > AIR_QUALITY_* environment > settings.yaml (positive int)."""
    name = f"AIR_QUALITY_{env_suffix}"
    raw = cli if cli is not None else os.environ.get(name) or section[key]
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name if cli is None else key} must be positive")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", action="store_true", help="write to Cloud SQL/Postgres")
    parser.add_argument("--raw-store", choices=("local", "gcs"), default="local")
    parser.add_argument("--raw-dir", type=Path, default=ROOT / "tmp" / "gcs_mirror")
    parser.add_argument("--fixture", type=Path, help="saved response; no network")
    parser.add_argument("--max-age", type=int)
    parser.add_argument("--stale-after", type=int)
    parser.add_argument("--sensor-ids", help="comma-separated fixed roster; max 20")
    parser.add_argument("--run-kind", choices=("scheduled", "preflight"))
    args = parser.parse_args(argv)

    pa.load_env()
    settings = yaml.safe_load((ROOT / "config/settings.yaml").read_text())
    section = settings["purpleair"]
    fields = ["sensor_index", *section["fields"]]
    max_age = _setting(
        args.max_age, "PURPLEAIR_MAX_AGE_SECONDS", section, "max_age_seconds"
    )
    stale_after = _setting(
        args.stale_after,
        "PURPLEAIR_STALE_AFTER_SECONDS",
        section,
        "stale_after_seconds",
    )
    poll_seconds = _setting(
        None, "PURPLEAIR_TARGET_POLL_SECONDS", section, "target_poll_seconds"
    )
    try:
        sensor_ids = sensor_allowlist(
            args.sensor_ids
            if args.sensor_ids is not None
            else os.environ.get(SENSOR_IDS_VAR)
        )
    except ValueError as exc:
        parser.error(str(exc))
    if sensor_ids is not None:
        stale_after = min(stale_after, 180)
    run_kind = args.run_kind or os.environ.get(
        "AIR_QUALITY_PURPLEAIR_RUN_KIND", "scheduled"
    )
    if run_kind not in ("scheduled", "preflight"):
        parser.error("Run kind must be scheduled or preflight")
    raw_prefix = (
        os.environ.get("AIR_QUALITY_GCS_RAW_PURPLEAIR_PREFIX")
        or settings["gcs"]["raw_purpleair_prefix"]
    ).strip("/")
    if window_closed(os.environ.get(STOP_AFTER_VAR), datetime.now(UTC)):
        print(f"poll skipped: {STOP_AFTER_VAR} has passed; no request made")
        return 0
    not_before = os.environ.get(NOT_BEFORE_VAR)
    if not_before and not window_closed(not_before, datetime.now(UTC)):
        print(f"poll skipped: {NOT_BEFORE_VAR} has not arrived; no request made")
        return 0
    if args.fixture:
        body = args.fixture.read_bytes()
        fetch: Fetch = lambda: (200, 0, body)  # noqa: E731
    else:
        key = os.environ.get(pa.KEY_VAR)
        if not key:
            print(f"{pa.KEY_VAR} is not set (put it in .env).", file=sys.stderr)
            return 2
        fetch = make_fetch(key, section["fields"], max_age, sensor_ids=sensor_ids)
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
        "poll_seconds": poll_seconds,
        "sensor_ids": sensor_ids,
        "run_kind": run_kind,
        "freshness_clock": "response_time_stamp",
        "bbox": pa.BBOX if sensor_ids is None else None,
    }
    with db.connect() if args.db else nullcontext(None) as conn:
        stats = poll(
            conn,
            fetch,
            raw_prefix=raw_prefix,
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
