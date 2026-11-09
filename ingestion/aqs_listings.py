"""Load the EPA AQS site and monitor listings into GCS and Cloud SQL."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import tempfile
import urllib.request
import zipfile
from collections import Counter
from collections.abc import Iterator
from datetime import date
from itertools import chain
from pathlib import Path

from ingestion import db

BASE_URL = "https://aqs.epa.gov/aqsweb/airdata/"
CHICAGO_COUNTIES = {
    ("17", "031"): "Cook, IL",
    ("17", "043"): "DuPage, IL",
    ("17", "097"): "Lake, IL",
    ("17", "197"): "Will, IL",
    ("17", "089"): "Kane, IL",
    ("17", "111"): "McHenry, IL",
    ("18", "089"): "Lake, IN",
}
PM25_CODES = {"88101", "88502"}
SUPERSEDED_METHODS = {"236", "238"}
SITE_FIELDS = {
    "State Code",
    "County Code",
    "Site Number",
    "Latitude",
    "Longitude",
    "Datum",
    "Local Site Name",
    "City Name",
    "County Name",
    "State Name",
    "Site Established Date",
    "Site Closed Date",
    "Extraction Date",
}
MONITOR_FIELDS = {
    "State Code",
    "County Code",
    "Site Number",
    "Parameter Code",
    "POC",
    "Latitude",
    "Longitude",
    "Datum",
    "Last Method Code",
    "Last Method",
    "First Year of Data",
    "Last Sample Date",
    "Extraction Date",
    "Local Site Name",
}
REPORT_COLUMNS = (
    "county",
    "monitor_key",
    "parameter_code",
    "latitude",
    "longitude",
    "first_year_of_data",
    "last_sample_date",
    "last_method_code",
    "pm25_advisory_superseded",
    "site_name",
)

SITE_UPSERT = """
INSERT INTO aqs_site (
    site_key, state_code, county_code, site_num, latitude, longitude, datum,
    site_name, city_name, county_name, state_name, first_year, last_year,
    established_date, closed_date, extraction_date, source_uri,
    source_generation, metadata
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s)
ON CONFLICT (site_key) DO UPDATE SET
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
    datum = EXCLUDED.datum, site_name = EXCLUDED.site_name,
    city_name = EXCLUDED.city_name, county_name = EXCLUDED.county_name,
    state_name = EXCLUDED.state_name, first_year = EXCLUDED.first_year,
    last_year = EXCLUDED.last_year,
    established_date = EXCLUDED.established_date,
    closed_date = EXCLUDED.closed_date,
    extraction_date = EXCLUDED.extraction_date,
    source_uri = EXCLUDED.source_uri,
    source_generation = EXCLUDED.source_generation,
    metadata = EXCLUDED.metadata
WHERE aqs_site.source_generation IS DISTINCT FROM EXCLUDED.source_generation
"""

MONITOR_UPSERT = """
INSERT INTO aqs_monitor (
    monitor_key, site_key, parameter_code, poc, method_code, method_name,
    latitude, longitude, datum, first_year_of_data, last_sample_date,
    extraction_date, pm25_advisory_superseded, source_uri, source_generation,
    metadata
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s)
ON CONFLICT (monitor_key) DO UPDATE SET
    method_code = EXCLUDED.method_code, method_name = EXCLUDED.method_name,
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
    datum = EXCLUDED.datum,
    first_year_of_data = EXCLUDED.first_year_of_data,
    last_sample_date = EXCLUDED.last_sample_date,
    extraction_date = EXCLUDED.extraction_date,
    pm25_advisory_superseded = EXCLUDED.pm25_advisory_superseded,
    source_uri = EXCLUDED.source_uri,
    source_generation = EXCLUDED.source_generation,
    metadata = EXCLUDED.metadata
WHERE aqs_monitor.source_generation IS DISTINCT FROM EXCLUDED.source_generation
"""


def _value(row: dict[str, str], name: str) -> str | None:
    value = row[name].strip()
    return value or None


def _code(row: dict[str, str], name: str, width: int) -> str:
    value = row[name].strip()
    if name == "State Code" and value == "CC":
        return value
    if not value.isascii() or not value.isdigit() or len(value) > width:
        raise ValueError(f"Invalid {name}: {value!r}")
    return value.zfill(width)


def _date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _float(value: str | None) -> float | None:
    return float(value) if value else None


def _extra(row: dict[str, str], mapped: set[str]) -> str:
    return json.dumps(
        {key: value for key, value in row.items() if key not in mapped and value},
        sort_keys=True,
    )


def _site_key(row: dict[str, str]) -> tuple[str, str, str, str]:
    state = _code(row, "State Code", 2)
    county = _code(row, "County Code", 3)
    site = _code(row, "Site Number", 4)
    return state, county, site, f"{state}-{county}-{site}"


def site_record(row: dict[str, str], uri: str, generation: str) -> tuple:
    state, county, site, key = _site_key(row)
    established = _date(_value(row, "Site Established Date"))
    closed = _date(_value(row, "Site Closed Date"))
    return (
        key,
        state,
        county,
        site,
        _float(_value(row, "Latitude")),
        _float(_value(row, "Longitude")),
        _value(row, "Datum"),
        _value(row, "Local Site Name"),
        _value(row, "City Name"),
        _value(row, "County Name"),
        _value(row, "State Name"),
        established.year if established else None,
        closed.year if closed else None,
        established,
        closed,
        _date(_value(row, "Extraction Date")),
        uri,
        generation,
        _extra(row, SITE_FIELDS),
    )


def monitor_record(row: dict[str, str], uri: str, generation: str) -> tuple:
    _, _, _, site_key = _site_key(row)
    parameter = _code(row, "Parameter Code", 5)
    poc = _code(row, "POC", 2)
    method = _value(row, "Last Method Code")
    first_year = _value(row, "First Year of Data")
    return (
        f"{site_key}-{parameter}-{poc}",
        site_key,
        parameter,
        poc,
        method,
        _value(row, "Last Method"),
        _float(_value(row, "Latitude")),
        _float(_value(row, "Longitude")),
        _value(row, "Datum"),
        int(first_year) if first_year else None,
        _date(_value(row, "Last Sample Date")),
        _date(_value(row, "Extraction Date")),
        parameter == "88101" and method in SUPERSEDED_METHODS,
        uri,
        generation,
        _extra(row, MONITOR_FIELDS),
    )


def iter_listing(path: Path, stem: str) -> Iterator[dict[str, str]]:
    with zipfile.ZipFile(path) as archive:
        members = [member for member in archive.namelist() if not member.endswith("/")]
        if members != [f"{stem}.csv"]:
            raise ValueError(f"Unexpected ZIP contents for {stem}: {members}")
        with archive.open(members[0]) as binary:
            reader = csv.DictReader(io.TextIOWrapper(binary, encoding="utf-8-sig"))
            required = SITE_FIELDS if stem == "aqs_sites" else MONITOR_FIELDS
            if not required.issubset(reader.fieldnames or []):
                raise ValueError(f"Missing required {stem} columns")
            for row in reader:
                if None in row:
                    raise ValueError(f"Extra CSV fields in {stem}")
                yield row


def inspect(site_zip: Path, monitor_zip: Path) -> tuple[dict[str, int], list[dict]]:
    site_keys: set[str] = set()
    monitor_keys: set[str] = set()
    counts: Counter[str] = Counter()
    chicago: list[dict] = []
    for row in iter_listing(site_zip, "aqs_sites"):
        key = _site_key(row)[3]
        if key in site_keys:
            raise ValueError(f"Duplicate AQS site: {key}")
        site_keys.add(key)
        site_record(row, "source", "version")
        counts["sites"] += 1
    for row in iter_listing(monitor_zip, "aqs_monitors"):
        record = monitor_record(row, "source", "version")
        key, site_key, parameter = record[:3]
        if key in monitor_keys:
            raise ValueError(f"Duplicate AQS monitor: {key}")
        if site_key not in site_keys:
            raise ValueError(f"Monitor has no site listing: {key}")
        monitor_keys.add(key)
        counts["monitors"] += 1
        county = CHICAGO_COUNTIES.get((row["State Code"], row["County Code"]))
        if county and parameter in PM25_CODES:
            chicago.append(
                dict(
                    zip(
                        REPORT_COLUMNS,
                        (
                            county,
                            key,
                            parameter,
                            record[6],
                            record[7],
                            record[9],
                            record[10],
                            record[4],
                            record[12],
                            row["Local Site Name"],
                        ),
                    )
                )
            )
    chicago.sort(key=lambda row: (row["county"], row["monitor_key"]))
    counts["chicago_pm25"] = len(chicago)
    return dict(counts), chicago


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def upload(path: Path, stem: str, bucket_name: str, row_count: int) -> tuple[str, str]:
    from google.api_core.exceptions import PreconditionFailed
    from google.cloud import storage

    digest = sha256(path)
    object_name = f"raw/aqs/{stem}/{digest}.zip"
    blob = (
        storage.Client(project=os.getenv("AIR_QUALITY_GCP_PROJECT_ID"))
        .bucket(bucket_name)
        .blob(object_name)
    )
    try:
        blob.metadata = {
            "sha256": digest,
            "source_url": BASE_URL + stem + ".zip",
            "row_count": str(row_count),
        }
        blob.upload_from_filename(
            str(path), content_type="application/zip", if_generation_match=0
        )
    except PreconditionFailed:
        blob.reload()
        if blob.metadata.get("sha256") != digest:
            raise ValueError(
                f"Existing GCS object has a different checksum: {object_name}"
            )
    return f"gs://{bucket_name}/{object_name}", str(blob.generation)


def load_table(
    conn, path: Path, stem: str, uri: str, generation: str, batch_size: int = 500
) -> int:
    converter = site_record if stem == "aqs_sites" else monitor_record
    sql = SITE_UPSERT if stem == "aqs_sites" else MONITOR_UPSERT
    count = 0
    batch: list[tuple] = []
    cur = conn.cursor()
    try:
        for row in iter_listing(path, stem):
            batch.append(converter(row, uri, generation))
            if len(batch) >= batch_size:
                cur.execute(*_batch_statement(sql, batch))
                conn.commit()
                count += len(batch)
                batch.clear()
        if batch:
            cur.execute(*_batch_statement(sql, batch))
            conn.commit()
            count += len(batch)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    return count


def _batch_statement(sql: str, rows: list[tuple]) -> tuple[str, tuple]:
    """Send a batch in one PostgreSQL round trip instead of executemany calls."""
    before_conflict, separator, conflict = sql.partition("ON CONFLICT")
    before_values, _, values = before_conflict.partition("VALUES ")
    if not separator or not values or values.count("%s") != len(rows[0]):
        raise ValueError("Unexpected AQS INSERT template")
    statement = (
        before_values
        + "VALUES "
        + ", ".join([values.strip()] * len(rows))
        + "\nON CONFLICT"
        + conflict
    )
    return statement, tuple(chain.from_iterable(rows))


def write_report(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _download(stem: str, directory: Path) -> Path:
    path = directory / f"{stem}.zip"
    with urllib.request.urlopen(BASE_URL + stem + ".zip", timeout=120) as response:
        with path.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-zip", type=Path)
    parser.add_argument("--monitor-zip", type=Path)
    parser.add_argument("--bucket", default=os.getenv("AIR_QUALITY_GCS_BUCKET"))
    parser.add_argument(
        "--report", type=Path, default=Path("output/aqs/chicago_pm25_monitors.csv")
    )
    parser.add_argument("--no-gcs", action="store_true")
    parser.add_argument("--no-db", action="store_true")
    args = parser.parse_args()
    if not args.no_gcs and not args.bucket:
        parser.error("--bucket or AIR_QUALITY_GCS_BUCKET is required")
    with tempfile.TemporaryDirectory(prefix="aqs-listings-") as work:
        directory = Path(work)
        sites = args.site_zip or _download("aqs_sites", directory)
        monitors = args.monitor_zip or _download("aqs_monitors", directory)
        counts, chicago = inspect(sites, monitors)
        write_report(chicago, args.report)
        print(f"AQS sites: {counts['sites']}; monitors: {counts['monitors']}")
        print(f"Chicago-area PM2.5 monitors: {counts['chicago_pm25']}")
        print(f"Chicago monitor list: {args.report}")
        sources = {}
        for stem, path, count in (
            ("aqs_sites", sites, counts["sites"]),
            ("aqs_monitors", monitors, counts["monitors"]),
        ):
            if args.no_gcs:
                sources[stem] = (BASE_URL + stem + ".zip", sha256(path))
            else:
                sources[stem] = upload(path, stem, args.bucket, count)
                print(f"Uploaded {stem}: {sources[stem][0]}")
        if not args.no_db:
            with db.connect() as conn:
                for stem, path in (("aqs_sites", sites), ("aqs_monitors", monitors)):
                    uri, generation = sources[stem]
                    count = load_table(conn, path, stem, uri, generation)
                    print(f"Loaded {stem} rows: {count}")
        for row in chicago:
            print(
                f"{row['county']} {row['monitor_key']} "
                f"({row['latitude']}, {row['longitude']}) "
                f"{row['first_year_of_data']}–{row['last_sample_date']} "
                f"method={row['last_method_code']} "
                f"superseded={row['pm25_advisory_superseded']}"
            )


if __name__ == "__main__":
    main()
