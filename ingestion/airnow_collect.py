"""S09: archive public HourlyAQObs snapshots and refresh preliminary observations."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import math
import os
import re
import uuid
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import requests
import yaml
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion import db

ROOT = Path(__file__).resolve().parents[1]
POLLUTANTS = ("PM25", "OZONE", "NO2", "CO", "SO2", "PM10")
AQI_POLLUTANTS = ("PM25", "OZONE", "NO2", "PM10")
LOCK_ID = 2050909


@dataclass(frozen=True)
class Bounds:
    south: float = 41.3
    north: float = 42.5
    west: float = -88.5
    east: float = -87.0

    def __post_init__(self):
        if not (-90 <= self.south < self.north <= 90):
            raise ValueError("Invalid latitude bounds")
        if not (-180 <= self.west < self.east <= 180):
            raise ValueError("Invalid longitude bounds")


@dataclass(frozen=True)
class Observation:
    airnow_site_id: str
    pollutant: str
    observed_at_utc: datetime
    site_name: str | None
    concentration: float | None
    concentration_unit: str | None
    aqi: int | None
    latitude: float | None
    longitude: float | None
    site_status: str | None
    data_source: str | None

    @property
    def key(self):
        return (self.airnow_site_id, self.pollutant, self.observed_at_utc)

    @property
    def value(self):
        return (self.concentration, self.concentration_unit, self.aqi)


@dataclass(frozen=True)
class Snapshot:
    hour: datetime
    content: bytes
    retrieved_at_utc: datetime
    last_modified: str | None = None
    etag: str | None = None

    @property
    def url(self):
        return (
            f"https://files.airnowtech.org/airnow/{self.hour:%Y}/{self.hour:%Y%m%d}/"
            f"HourlyAQObs_{self.hour:%Y%m%d%H}.dat"
        )


def utc_hour(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("Supply a UTC hour with Z or +00:00")
    if result.minute or result.second or result.microsecond:
        raise ValueError("The end hour must begin exactly on the hour")
    return result.astimezone(UTC)


def hour_window(end: datetime, hours: int) -> list[datetime]:
    utc_hour(end.isoformat())
    if not 1 <= hours <= 72:
        raise ValueError("hours must be between 1 and 72")
    if end - timedelta(hours=hours - 1) < datetime(2019, 5, 28, tzinfo=UTC):
        raise ValueError("HourlyAQObs starts on 2019-05-28")
    return [end - timedelta(hours=i) for i in reversed(range(hours))]


def number(value: str, field: str) -> float | None:
    if not value:
        return None
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"Non-finite {field}: {value!r}")
    return result


def parse_file(content: bytes, hour: datetime, bounds: Bounds) -> list[Observation]:
    """Unpivot values only; no AQI calculation, unit conversion, or ID rewriting."""
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")), strict=True)
    if not reader.fieldnames:
        raise ValueError("Empty HourlyAQObs file")
    reader.fieldnames = [name.strip().upper() for name in reader.fieldnames]
    required = {
        "AQSID",
        "SITENAME",
        "STATUS",
        "LATITUDE",
        "LONGITUDE",
        "COUNTRYCODE",
        "VALIDDATE",
        "VALIDTIME",
        "DATASOURCE",
        *POLLUTANTS,
        *(f"{p}_UNIT" for p in POLLUTANTS),
        *(f"{p}_AQI" for p in AQI_POLLUTANTS),
    }
    if len(reader.fieldnames) != len(set(reader.fieldnames)):
        raise ValueError("Duplicate CSV headers")
    if missing := required - set(reader.fieldnames):
        raise ValueError(f"Missing HourlyAQObs columns: {sorted(missing)}")
    result, sites = [], set()
    row_count = 0
    for raw in reader:
        row_count += 1
        if None in raw or any(value is None for value in raw.values()):
            raise ValueError(f"Malformed CSV row {reader.line_num}")
        row = {key: value.strip() for key, value in raw.items()}
        lat = number(row["LATITUDE"], "latitude")
        lon = number(row["LONGITUDE"], "longitude")
        if (
            row["COUNTRYCODE"] != "US"
            or lat is None
            or lon is None
            or not bounds.south <= lat <= bounds.north
            or not bounds.west <= lon <= bounds.east
        ):
            continue
        site_id = row["AQSID"]
        if not re.fullmatch(r"(?:\d{9}|\d{12})", site_id):
            raise ValueError(f"Invalid AQSID: {site_id!r}")
        date_format = "%m/%d/%Y" if len(row["VALIDDATE"]) == 10 else "%m/%d/%y"
        observed = datetime.strptime(
            f"{row['VALIDDATE']} {row['VALIDTIME']}", f"{date_format} %H:%M"
        ).replace(tzinfo=UTC)
        if observed != hour:
            raise ValueError(f"File/record UTC hour mismatch: {observed} != {hour}")
        if site_id in sites:
            raise ValueError(f"Duplicate AQSID in hourly file: {site_id}")
        sites.add(site_id)
        for pollutant in POLLUTANTS:
            value = number(row[pollutant], pollutant)
            aqi = number(row.get(f"{pollutant}_AQI", ""), f"{pollutant} AQI")
            if aqi is not None and (not aqi.is_integer() or not 0 <= aqi <= 32767):
                raise ValueError(f"Invalid AQI: {aqi}")
            if value is None and aqi is None:
                continue
            unit = row[f"{pollutant}_UNIT"] or None
            if value is not None and unit is None:
                raise ValueError(
                    f"Missing concentration unit for {site_id}/{pollutant}"
                )
            result.append(
                Observation(
                    site_id,
                    pollutant,
                    observed,
                    row["SITENAME"] or None,
                    value,
                    unit,
                    int(aqi) if aqi is not None else None,
                    lat,
                    lon,
                    row["STATUS"] or None,
                    row["DATASOURCE"] or None,
                )
            )
    if not row_count or not sites:
        # An empty/truncated response must never withdraw the whole area's data.
        raise ValueError("HourlyAQObs has no source rows or no Chicago-area sites")
    return result


def http_session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = "ChicagoAirQualityCourse/0.1 HourlyAQObs"
    session.mount(
        "https://",
        HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=("GET",),
                respect_retry_after_header=True,
            )
        ),
    )
    return session


def fetch_hour(session, hour) -> Snapshot:
    snapshot = Snapshot(hour, b"", datetime.now(UTC))
    with session.get(snapshot.url, timeout=(10, 45)) as response:
        response.raise_for_status()
        content = response.content
        if not content or len(content) > 20_000_000:
            raise ValueError("Empty or unexpectedly large HourlyAQObs response")
        return replace(
            snapshot,
            content=content,
            retrieved_at_utc=datetime.now(UTC),
            last_modified=response.headers.get("Last-Modified"),
            etag=response.headers.get("ETag"),
        )


def archive_snapshot(bucket, snapshot: Snapshot, prefix: str) -> dict:
    digest = hashlib.sha256(snapshot.content).hexdigest()
    name = (
        f"{prefix}/dt={snapshot.hour:%Y-%m-%d}/hh={snapshot.hour:%H}/"
        f"HourlyAQObs_{snapshot.hour:%Y%m%d%H}_{digest}.dat"
    )
    blob = bucket.blob(name)
    created = False
    try:
        blob.reload()
    except NotFound:
        blob.metadata = {
            "sha256": digest,
            "source_url": snapshot.url,
            "first_retrieved_at_utc": snapshot.retrieved_at_utc.isoformat(),
            "source_last_modified": snapshot.last_modified or "",
            "source_etag": snapshot.etag or "",
        }
        try:
            blob.upload_from_string(
                snapshot.content,
                content_type="text/csv",
                if_generation_match=0,
                checksum="crc32c",
            )
            created = True
        except PreconditionFailed:
            pass  # Another attempt already published the identical content name.
        blob.reload()
    md5 = base64.b64encode(hashlib.md5(snapshot.content).digest()).decode()
    if (
        blob.size != len(snapshot.content)
        or blob.md5_hash != md5
        or (blob.metadata or {}).get("sha256") != digest
    ):
        raise ValueError(f"Archived snapshot checksum mismatch: {name}")
    return {
        "raw_uri": f"gs://{bucket.name}/{name}",
        "source_generation": str(blob.generation),
        "sha256": digest,
        "bytes": len(snapshot.content),
        "raw_created": created,
    }


@contextmanager
def collector_lock(conn):
    with closing(conn.cursor()) as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_ID,))
        acquired = cur.fetchone()[0]
    conn.commit()
    if not acquired:
        raise RuntimeError("Another AirNow collector is running; retry later")
    try:
        yield
    finally:
        conn.rollback()
        with closing(conn.cursor()) as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))
        conn.commit()


def save_hour(
    conn, observations, snapshot: Snapshot, artifact: dict, bounds: Bounds = Bounds()
) -> dict:
    """Atomically refresh an hour, archiving changed values with the S05 trigger."""
    counts = {"inserted": 0, "revised": 0, "unchanged": 0, "stale": 0, "withdrawn": 0}
    fields = tuple(Observation.__dataclass_fields__)
    keys = fields[:3]
    columns = (*fields, "retrieved_at_utc", "raw_uri", "source_generation")
    try:
        with closing(conn.cursor()) as cur:
            cur.execute(
                f"SELECT {','.join(fields)},retrieved_at_utc FROM airnow_obs "
                "WHERE observed_at_utc = %s AND latitude BETWEEN %s AND %s "
                "AND longitude BETWEEN %s AND %s FOR UPDATE",
                (snapshot.hour, bounds.south, bounds.north, bounds.west, bounds.east),
            )
            previous = {
                tuple(row[:3]): (Observation(*row[:-1]), row[-1])
                for row in cur.fetchall()
            }
            incoming = {obs.key: obs for obs in observations}
            if len(incoming) != len(observations):
                raise ValueError("Duplicate observation keys")
            if any(obs.observed_at_utc != snapshot.hour for obs in observations):
                raise ValueError("Observation outside requested hour")
            # A removed pollutant/site is a withdrawal, not a stale current value.
            for key, (old, _) in previous.items():
                if key not in incoming:
                    incoming[key] = replace(
                        old, concentration=None, concentration_unit=None, aqi=None
                    )
            for key, obs in incoming.items():
                old, old_time = previous.get(key, (None, None))
                if old_time and old_time >= snapshot.retrieved_at_utc:
                    counts["stale"] += 1
                    continue
                cur.execute(
                    f"INSERT INTO airnow_obs AS current ({','.join(columns)}) "
                    f"VALUES ({','.join(['%s'] * len(columns))}) "
                    f"ON CONFLICT ({','.join(keys)}) DO UPDATE SET "
                    + ",".join(f"{c}=EXCLUDED.{c}" for c in columns if c not in keys)
                    + " WHERE current.retrieved_at_utc < EXCLUDED.retrieved_at_utc "
                    "RETURNING airnow_site_id",
                    (
                        *asdict(obs).values(),
                        snapshot.retrieved_at_utc,
                        artifact["raw_uri"],
                        artifact["source_generation"],
                    ),
                )
                if not cur.fetchone():
                    counts["stale"] += 1
                elif old is None:
                    counts["inserted"] += 1
                elif old.value != obs.value:
                    counts["revised"] += 1
                    if obs.concentration is None and obs.aqi is None:
                        counts["withdrawn"] += 1
                else:
                    counts["unchanged"] += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return counts


def collect(conn, bucket, session, hours, bounds, prefix):
    report = {
        "run_id": str(uuid.uuid4()),
        "job_name": "airnow_collect",
        "start_hour_utc": hours[0].isoformat(),
        "end_hour_utc": hours[-1].isoformat(),
        "bounds": asdict(bounds),
        "is_preliminary": True,
        "files": [],
        "errors": [],
    }
    with collector_lock(conn):
        with closing(conn.cursor()) as cur:
            cur.execute(
                "INSERT INTO ingest_run (run_id,job_name,status,started_at_utc,"
                "input_count,details) VALUES (%s,'airnow_collect','running',"
                "now(),%s,%s::jsonb)",
                (report["run_id"], len(hours), json.dumps(report)),
            )
        conn.commit()
        for hour in hours:
            try:
                snapshot = fetch_hour(session, hour)
                artifact = archive_snapshot(bucket, snapshot, prefix)
                observations = parse_file(snapshot.content, hour, bounds)
                counts = save_hour(conn, observations, snapshot, artifact, bounds)
                item = {
                    "hour": hour.isoformat(),
                    **artifact,
                    **counts,
                    "observations": len(observations),
                    "retrieved_at_utc": snapshot.retrieved_at_utc.isoformat(),
                    "source_last_modified": snapshot.last_modified,
                }
                report["files"].append(item)
                print(json.dumps({"airnow_hour": item}), flush=True)
            except Exception as exc:
                conn.rollback()
                error = {"hour": hour.isoformat(), "error": str(exc)}
                report["errors"].append(error)
                print(json.dumps({"airnow_error": error}), flush=True)
        report["totals"] = {
            key: sum(item[key] for item in report["files"])
            for key in (
                "inserted",
                "revised",
                "unchanged",
                "stale",
                "withdrawn",
                "raw_created",
                "observations",
            )
        }
        report["status"] = "failed" if report["errors"] else "succeeded"
        with closing(conn.cursor()) as cur:
            cur.execute(
                "UPDATE ingest_run SET status=%s,finished_at_utc=now(),"
                "output_count=%s,error_message=%s,details=%s::jsonb WHERE run_id=%s",
                (
                    report["status"],
                    report["totals"]["observations"],
                    f"{len(report['errors'])} hour(s) failed"
                    if report["errors"]
                    else None,
                    json.dumps(report),
                    report["run_id"],
                ),
            )
        conn.commit()
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings", type=Path, default=ROOT / "config/settings.yaml")
    parser.add_argument("--end-hour", type=utc_hour, help="Inclusive UTC begin-hour")
    parser.add_argument("--hours", type=int, help="1–72 completed hours (default 72)")
    parser.add_argument("--report", type=Path, help="Save the complete JSON run report")
    args = parser.parse_args(argv)
    config = yaml.safe_load(args.settings.read_text())

    def setting(section, key, default):
        return os.environ.get(
            f"AIR_QUALITY_{section}_{key}".upper(),
            config.get(section, {}).get(key, default),
        )

    hours = (
        args.hours
        if args.hours is not None
        else int(setting("airnow", "revision_lookback_hours", 72))
    )
    current_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    end = args.end_hour or current_hour - timedelta(hours=1)
    if end >= current_hour:
        parser.error("Only completed UTC hours can be collected")
    bounds = Bounds(
        **{
            key: float(setting("airnow", key, default))
            for key, default in asdict(Bounds()).items()
        }
    )
    prefix = str(setting("gcs", "raw_airnow_prefix", "raw/airnow")).strip("/")
    if not prefix or ".." in prefix.split("/"):
        parser.error("Invalid raw AirNow prefix")
    bucket_name = os.environ.get("AIR_QUALITY_GCS_BUCKET")
    if not bucket_name:
        parser.error("Set AIR_QUALITY_GCS_BUCKET")
    window = hour_window(end, hours)
    with (
        storage.Client(project=os.environ.get("AIR_QUALITY_GCP_PROJECT_ID")) as client,
        http_session() as session,
        db.connect() as conn,
    ):
        report = collect(
            conn, client.bucket(bucket_name), session, window, bounds, prefix
        )
    print(json.dumps({"airnow_summary": report}), flush=True)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
