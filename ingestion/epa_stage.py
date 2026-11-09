"""S10: validate EPA ZIPs and stream their CSV members into GCS staging."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import os
import re
import tempfile
import uuid
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import yaml
from google.cloud import storage

from ingestion import db
from ingestion.epa_download import parse_years
from ingestion.epa_manifest import ROOT
from ingestion.epa_state import State

CHUNK_SIZE = 4 * 1024 * 1024
REQUIRED_COLUMNS = {
    "State Code",
    "County Code",
    "Site Num",
    "Parameter Code",
    "POC",
    "Date GMT",
    "Time GMT",
    "Sample Measurement",
    "Units of Measure",
}


@dataclass(frozen=True)
class StageEntry:
    ingest_file_id: int
    pollutant: str
    year: int
    file_name: str
    listed_rows: int
    raw_uri: str
    raw_generation: str
    raw_md5: str
    raw_size_bytes: int
    stage_state: str
    staged_rows: int | None

    @property
    def parameter(self) -> str:
        match = re.fullmatch(r"hourly_([0-9A-Z]+)_(\d{4})\.zip", self.file_name)
        if not match or int(match[2]) != self.year:
            raise ValueError(f"Unexpected EPA filename: {self.file_name}")
        return match[1]

    @property
    def csv_name(self) -> str:
        return self.file_name[:-4] + ".csv"


class CopyingReader(io.RawIOBase):
    """Copy decompressed bytes to a GCS writer while csv.reader counts records."""

    def __init__(self, source, target):
        self.source = source
        self.target = target
        self.digest = hashlib.md5(usedforsecurity=False)
        self.byte_count = 0

    def readable(self):
        return True

    def readinto(self, buffer):
        chunk = self.source.read(len(buffer))
        if not chunk:
            return 0
        self.target.write(chunk)
        self.digest.update(chunk)
        self.byte_count += len(chunk)
        buffer[: len(chunk)] = chunk
        return len(chunk)


def extract_csv(zip_path: Path, expected_member: str, target):
    """Stream one member; zipfile checks its CRC when the reader reaches EOF."""
    with zipfile.ZipFile(zip_path) as archive:
        members = archive.infolist()
        if len(members) != 1 or members[0].filename != expected_member:
            raise ValueError(f"Unexpected ZIP members in {zip_path.name}")
        info = members[0]
        if info.is_dir() or info.flag_bits & 1:
            raise ValueError("EPA ZIP contains a directory or encrypted member")
        with archive.open(info) as source:
            copying = CopyingReader(source, target)
            buffered = io.BufferedReader(copying, buffer_size=256 * 1024)
            with io.TextIOWrapper(
                buffered, encoding="utf-8-sig", newline="", errors="strict"
            ) as decoded:
                reader = csv.reader(decoded, strict=True)
                header = next(reader, None)
                if header is None or not REQUIRED_COLUMNS.issubset(header):
                    raise ValueError("EPA CSV has no valid hourly header")
                rows = 0
                for row in reader:
                    if len(row) != len(header):
                        raise ValueError("EPA CSV row width differs from its header")
                    rows += 1
            if copying.byte_count != info.file_size:
                raise ValueError("Uncompressed CSV size differs from ZIP metadata")
            return rows, copying.byte_count, copying.digest.hexdigest(), header


def stage_name(entry: StageEntry, prefix: str) -> str:
    return (
        f"{prefix}/{entry.parameter}/{entry.year}/"
        f"{entry.file_name[:-4]}__g{entry.raw_generation}.csv"
    )


def _blob_md5_hex(blob) -> str | None:
    return base64.b64decode(blob.md5_hash).hex() if blob.md5_hash else None


def reusable(blob, entry: StageEntry) -> bool:
    if not blob or not blob.size or not blob.generation:
        return False
    meta = blob.metadata or {}
    return (
        meta.get("source_generation") == entry.raw_generation
        and meta.get("source_uri") == entry.raw_uri
        and meta.get("csv_rows", "").isdigit()
        and meta.get("csv_md5_hex") == _blob_md5_hex(blob)
    )


def load_entries(conn, years=None, pollutants=None) -> list[StageEntry]:
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            """SELECT ingest_file_id, pollutant, year, file_name, listed_rows,
                      raw_uri, raw_generation, raw_md5, raw_size_bytes,
                      stage_state, staged_rows
               FROM ingest_file
               WHERE catalog_status = 'listed_nonempty'
                 AND download_state = 'downloaded'
               ORDER BY year, pollutant"""
        )
        entries = [StageEntry(*row) for row in cursor.fetchall()]
    entries = [
        row
        for row in entries
        if (years is None or row.year in years)
        and (pollutants is None or row.pollutant in pollutants)
    ]
    if not entries:
        raise ValueError("No downloaded EPA files match the requested filters")
    for entry in entries:
        if not re.fullmatch(r"[0-9a-f]{32}", entry.raw_md5):
            raise ValueError(f"Missing or invalid raw MD5: {entry.file_name}")
        if not entry.raw_generation.isdigit() or entry.raw_size_bytes <= 0:
            raise ValueError(f"Missing raw generation or size: {entry.file_name}")
        entry.parameter
    return entries


def current_entry(conn, file_id: int) -> StageEntry:
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            """SELECT ingest_file_id, pollutant, year, file_name, listed_rows,
                      raw_uri, raw_generation, raw_md5, raw_size_bytes,
                      stage_state, staged_rows
               FROM ingest_file WHERE ingest_file_id = %s
                 AND catalog_status = 'listed_nonempty'
                 AND download_state = 'downloaded'""",
            (file_id,),
        )
        row = cursor.fetchone()
    if row is None:
        raise ValueError(f"Downloaded source changed or disappeared: {file_id}")
    return StageEntry(*row)


def _raw_blob(bucket, entry: StageEntry, raw_prefix: str):
    prefix = f"gs://{bucket.name}/"
    if not entry.raw_uri.startswith(prefix):
        raise ValueError(f"Raw URI points outside data bucket: {entry.file_name}")
    name = entry.raw_uri[len(prefix) :]
    expected = f"{raw_prefix}/{entry.year}/{entry.file_name}"
    if name != expected:
        raise ValueError(f"Unexpected raw EPA object path: {entry.file_name}")
    blob = bucket.blob(name, generation=int(entry.raw_generation))
    blob.reload()
    if int(blob.size) != entry.raw_size_bytes or _blob_md5_hex(blob) != entry.raw_md5:
        raise ValueError(f"Raw GCS metadata differs from SQL: {entry.file_name}")
    return blob


def stage_object(
    bucket, entry: StageEntry, prefix: str, run_id: uuid.UUID, raw_prefix: str
):
    name = stage_name(entry, prefix)
    existing = bucket.get_blob(name)
    if reusable(existing, entry):
        return int(existing.metadata["csv_rows"]), "reused", existing
    raw = _raw_blob(bucket, entry, raw_prefix)
    with tempfile.TemporaryDirectory(prefix="epa-stage-") as directory:
        zip_path = Path(directory) / entry.file_name
        raw.download_to_filename(str(zip_path), timeout=120)
        if zip_path.stat().st_size != entry.raw_size_bytes:
            raise ValueError(f"Downloaded ZIP size differs from SQL: {entry.file_name}")
        digest = hashlib.md5(usedforsecurity=False)
        with zip_path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != entry.raw_md5:
            raise ValueError(f"Downloaded ZIP MD5 differs from SQL: {entry.file_name}")
        blob = bucket.blob(name)
        blob.metadata = {
            "source_uri": entry.raw_uri,
            "source_generation": entry.raw_generation,
            "source_md5_hex": entry.raw_md5,
            "producer_run_id": str(run_id),
            "ingested_at_utc": datetime.now(UTC).isoformat(),
        }
        with blob.open(
            "wb",
            chunk_size=CHUNK_SIZE,
            content_type="text/csv",
            if_generation_match=int(existing.generation) if existing else 0,
            checksum="crc32c",
            timeout=120,
        ) as target:
            rows, size, md5, header = extract_csv(zip_path, entry.csv_name, target)
        blob.reload()
        if int(blob.size) != size or _blob_md5_hex(blob) != md5:
            raise ValueError(f"Staged CSV differs from local stream: {entry.file_name}")
        blob.metadata = {
            **(blob.metadata or {}),
            "csv_rows": str(rows),
            "csv_md5_hex": md5,
            "csv_columns": str(len(header)),
        }
        blob.patch(
            if_generation_match=int(blob.generation),
            if_metageneration_match=blob.metageneration,
        )
        return rows, "created", blob


def update_stage(conn, entry: StageEntry, rows: int, run_id: uuid.UUID) -> None:
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            """UPDATE ingest_file SET stage_state='staged', staged_rows=%s,
                      last_run_id=%s, updated_at_utc=now()
               WHERE ingest_file_id=%s AND raw_generation=%s
                 AND download_state='downloaded'""",
            (rows, str(run_id), entry.ingest_file_id, entry.raw_generation),
        )
        if cursor.rowcount != 1:
            conn.rollback()
            raise ValueError(f"Raw generation changed during stage: {entry.file_name}")
    conn.commit()


def mark_failed(conn, entry: StageEntry, run_id: uuid.UUID) -> None:
    conn.rollback()
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            """UPDATE ingest_file SET stage_state='failed', staged_rows=NULL,
                      last_run_id=%s, updated_at_utc=now()
               WHERE ingest_file_id=%s AND raw_generation=%s
                 AND stage_state <> 'staged'""",
            (str(run_id), entry.ingest_file_id, entry.raw_generation),
        )
    conn.commit()


def stage_one(
    conn, bucket, entry: StageEntry, prefix: str, run_id: uuid.UUID, raw_prefix: str
):
    with State(conn).lock(entry):
        current = current_entry(conn, entry.ingest_file_id)
        if current.raw_generation != entry.raw_generation:
            raise ValueError(f"Source version changed: {entry.file_name}")
        try:
            rows, action, blob = stage_object(
                bucket, current, prefix, run_id, raw_prefix
            )
            update_stage(conn, current, rows, run_id)
        except Exception:
            mark_failed(conn, current, run_id)
            raise
        return {
            "pollutant": current.pollutant,
            "year": current.year,
            "outcome": (
                "mismatch"
                if rows != current.listed_rows
                else "unchanged"
                if action == "reused"
                and current.stage_state == "staged"
                and current.staged_rows == rows
                else "reconciled"
                if action == "reused"
                else "staged"
            ),
            "listed_rows": current.listed_rows,
            "staged_rows": rows,
            "staged_uri": f"gs://{bucket.name}/{blob.name}",
            "staged_generation": str(blob.generation),
            "raw_generation": current.raw_generation,
        }


def _work(entries, bucket_name, project, prefix, run_id, raw_prefix):
    results = []
    with db.connect() as conn:
        bucket = storage.Client(project=project).bucket(bucket_name)
        for entry in entries:
            try:
                result = stage_one(conn, bucket, entry, prefix, run_id, raw_prefix)
            except Exception as exc:
                conn.rollback()
                result = {
                    "pollutant": entry.pollutant,
                    "year": entry.year,
                    "outcome": "failed",
                    "error": f"{type(exc).__name__}: {exc}"[:500],
                }
            print(json.dumps({"run_id": str(run_id), **result}), flush=True)
            results.append(result)
    return results


def _start_run(conn, run_id, count, details):
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            """INSERT INTO ingest_run
               (run_id, job_name, status, started_at_utc, input_count, details)
               VALUES (%s,'epa_stage','running',now(),%s,%s::jsonb)""",
            (str(run_id), count, json.dumps(details)),
        )
    conn.commit()


def _finish_run(conn, run_id, results):
    counts = Counter(row["outcome"] for row in results)
    problems = [row for row in results if row["outcome"] in {"failed", "mismatch"}]
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            """UPDATE ingest_run SET status=%s, finished_at_utc=now(),
                      output_count=%s, error_message=%s,
                      details=details || %s::jsonb WHERE run_id=%s""",
            (
                "failed" if problems else "succeeded",
                len(results) - counts["failed"],
                f"{len(problems)} file(s) failed or mismatched" if problems else None,
                json.dumps({"outcomes": counts, "problems": problems}),
                str(run_id),
            ),
        )
    conn.commit()
    return counts, problems


def verify(entries, client, bucket_name, prefix):
    blobs = {
        blob.name: blob for blob in client.list_blobs(bucket_name, prefix=prefix + "/")
    }
    mismatches = []
    matches = []
    row_total = 0
    for entry in entries:
        blob = blobs.get(stage_name(entry, prefix))
        if entry.stage_state != "staged" or entry.staged_rows is None:
            mismatches.append(f"SQL not staged: {entry.pollutant}/{entry.year}")
        elif not reusable(blob, entry):
            mismatches.append(
                f"GCS stage missing or invalid: {entry.pollutant}/{entry.year}"
            )
        elif int(blob.metadata["csv_rows"]) != entry.staged_rows:
            mismatches.append(
                f"SQL/GCS row count differs: {entry.pollutant}/{entry.year}"
            )
        else:
            matches.append(entry)
            row_total += entry.staged_rows
            if entry.staged_rows != entry.listed_rows:
                mismatches.append(
                    f"Catalog/staged row count differs: {entry.pollutant}/{entry.year} "
                    f"({entry.listed_rows} vs {entry.staged_rows})"
                )
    return {
        "verified_at_utc": datetime.now(UTC).isoformat(),
        "selected": len(entries),
        "matched": len(matches),
        "staged_rows": row_total,
        "mismatches": mismatches,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings", type=Path, default=ROOT / "config/settings.yaml")
    parser.add_argument("--years", type=parse_years)
    parser.add_argument("--pollutants", help="Comma-separated configured group names")
    parser.add_argument("--concurrency", type=int, choices=range(1, 5), default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)
    try:
        settings = yaml.safe_load(args.settings.read_text())
        groups = settings["epa"]["hourly_groups"]
        pollutants = set(args.pollutants.split(",")) if args.pollutants else None
        if pollutants and pollutants - groups.keys():
            raise ValueError(
                f"Unknown pollutants: {sorted(pollutants - groups.keys())}"
            )
        prefix = (
            os.getenv("AIR_QUALITY_GCS_STAGING_EPA_PREFIX")
            or settings["gcs"]["staging_epa_prefix"]
        ).strip("/")
        if not prefix or ".." in prefix.split("/"):
            raise ValueError("Invalid staging EPA prefix")
        raw_prefix = (
            os.getenv("AIR_QUALITY_GCS_RAW_EPA_PREFIX")
            or settings["gcs"]["raw_epa_prefix"]
        ).strip("/")
        if not raw_prefix or ".." in raw_prefix.split("/"):
            raise ValueError("Invalid raw EPA object prefix")
        with db.connect() as conn:
            entries = load_entries(conn, args.years, pollutants)
            for entry in entries:
                if entry.parameter != groups[entry.pollutant]:
                    raise ValueError(f"Parameter/manifest mismatch: {entry.file_name}")
            if args.dry_run:
                print(
                    json.dumps(
                        {
                            "selected": len(entries),
                            "zip_bytes": sum(row.raw_size_bytes for row in entries),
                        }
                    )
                )
                return 0
            bucket_name = os.environ["AIR_QUALITY_GCS_BUCKET"]
            project = os.environ["AIR_QUALITY_GCP_PROJECT_ID"]
            if args.verify_only:
                result = verify(
                    entries, storage.Client(project=project), bucket_name, prefix
                )
                print(json.dumps(result), flush=True)
                return 1 if result["mismatches"] else 0
            run_id = uuid.uuid4()
            _start_run(
                conn,
                run_id,
                len(entries),
                {
                    "bucket": bucket_name,
                    "prefix": prefix,
                    "raw_prefix": raw_prefix,
                    "cloud_run_execution": os.getenv("CLOUD_RUN_EXECUTION"),
                    "concurrency": args.concurrency,
                },
            )
            results = []
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                futures = [
                    pool.submit(
                        _work,
                        entries[index :: args.concurrency],
                        bucket_name,
                        project,
                        prefix,
                        run_id,
                        raw_prefix,
                    )
                    for index in range(min(args.concurrency, len(entries)))
                ]
                for future in as_completed(futures):
                    results.extend(future.result())
            counts, problems = _finish_run(conn, run_id, results)
        print(
            json.dumps(
                {"run_id": str(run_id), "outcomes": counts, "problems": problems}
            ),
            flush=True,
        )
        return 1 if problems else 0
    except (OSError, ValueError, KeyError, yaml.YAMLError) as exc:
        print(f"EPA stage failed: {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
