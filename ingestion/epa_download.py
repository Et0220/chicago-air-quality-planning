"""S06: stream EPA hourly ZIPs to versioned GCS and reconcile Cloud SQL state."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import os
import re
import struct
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import requests
import yaml
from google.api_core import exceptions as gcs_errors
from google.cloud import storage

from ingestion import db
from ingestion.epa_manifest import FIELDS, ROOT, ManifestEntry, load_settings
from ingestion.epa_state import State

HTTP_TIMEOUT = (15, 90)
CHUNK_SIZE = 4 * 1024 * 1024
ATTEMPTS = 3


@dataclass(frozen=True)
class Artifact:
    uri: str
    generation: str
    md5: str  # Lowercase hexadecimal; GCS represents the same digest in base64.
    size: int
    created_at: datetime


def load_manifest(path, groups, years=None, pollutants=None):
    entries = []
    seen = set()
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != list(FIELDS):
            raise ValueError("Manifest columns do not match the S01 contract")
        for raw in reader:
            if None in raw or any(value is None for value in raw.values()):
                raise ValueError("Malformed CSV record")
            row = {key: value or None for key, value in raw.items()}
            row["year"] = int(row["year"])
            for key in ("listed_rows", "listed_bytes"):
                row[key] = int(row[key]) if row[key] is not None else None
                if row[key] is not None and row[key] < 0:
                    raise ValueError(f"Negative {key}")
            entry = ManifestEntry(**row)
            key = (entry.pollutant, entry.year)
            if key in seen or entry.pollutant not in groups:
                raise ValueError(f"Duplicate or unknown manifest key: {key}")
            seen.add(key)
            if not 1990 <= entry.year <= 2100:
                raise ValueError(f"Invalid year: {entry.year}")
            if entry.status == "missing":
                if any(
                    row[k] is not None
                    for k in (
                        "file_name",
                        "url",
                        "listed_rows",
                        "listed_bytes",
                        "last_modified",
                    )
                ):
                    raise ValueError(
                        f"Missing entry must have null source fields: {key}"
                    )
            else:
                expected = f"hourly_{groups[entry.pollutant]}_{entry.year}.zip"
                if entry.file_name != expected or entry.url != (
                    f"https://aqs.epa.gov/aqsweb/airdata/{expected}"
                ):
                    raise ValueError(f"Unexpected EPA filename or URL: {key}")
                if entry.listed_rows is None or entry.listed_bytes is None:
                    raise ValueError(f"Listed entry lacks counts: {key}")
                date.fromisoformat(entry.last_modified or "")
                expected_status = (
                    "listed_nonempty" if entry.listed_rows else "listed_empty"
                )
                if entry.status != expected_status:
                    raise ValueError(f"Inconsistent catalog status: {key}")
            if (years is None or entry.year in years) and (
                pollutants is None or entry.pollutant in pollutants
            ):
                entries.append(entry)
    if not entries:
        raise ValueError("No manifest entries match the requested filters")
    if years and years - {row.year for row in entries}:
        raise ValueError("Requested years are absent from the manifest")
    return entries


def parse_years(value):
    result = set()
    for part in value.split(","):
        match = re.fullmatch(r"(\d{4})(?:-(\d{4}))?", part.strip())
        if not match:
            raise argparse.ArgumentTypeError("Use years such as 2025,2026 or 1990-2026")
        first, last = int(match[1]), int(match[2] or match[1])
        if not 1990 <= first <= last <= 2100:
            raise argparse.ArgumentTypeError("Year ranges must be ascending, 1990–2100")
        result.update(range(first, last + 1))
    return result


def fingerprint(entry):
    return hashlib.sha256(
        json.dumps(asdict(entry), sort_keys=True).encode()
    ).hexdigest()


def source_headers(response):
    return {
        "source_etag": response.headers.get("ETag", ""),
        "source_last_modified": response.headers.get("Last-Modified", ""),
        "source_content_length": response.headers.get("Content-Length", ""),
    }


def reusable(blob, entry, headers):
    """Require source validators AND an intact completed object, not SQL alone."""
    if blob is None or not blob.md5_hash or not blob.size:
        return False
    metadata = blob.metadata or {}
    if metadata.get("download_md5_hex") != base64.b64decode(blob.md5_hash).hex():
        return False
    if metadata.get("catalog_sha256") != fingerprint(entry):
        return False
    if metadata.get("source_url") != entry.url:
        return False
    length = headers["source_content_length"]
    if length and int(length) != blob.size:
        return False
    etag = headers["source_etag"]
    if etag and not etag.startswith("W/"):
        return etag == metadata.get("source_etag")
    modified = headers["source_last_modified"]
    return bool(
        length and modified and modified == metadata.get("source_last_modified")
    )


def artifact_from_blob(blob, bucket_name):
    if not blob.md5_hash or not blob.generation or not blob.size:
        raise ValueError("GCS object lacks checksum, generation, or size")
    return Artifact(
        f"gs://{bucket_name}/{blob.name}",
        str(blob.generation),
        base64.b64decode(blob.md5_hash, validate=True).hex(),
        int(blob.size),
        blob.time_created,
    )


def validate_zip_end(tail, size):
    # Validate the ZIP envelope while streaming; S10 checks each member's CRC
    # during decompression. ZIP64 archives also retain this standard end record.
    index = tail.rfind(b"PK\x05\x06")
    if index < 0 or len(tail) - index < 22:
        raise ValueError("Truncated ZIP: end-of-central-directory record missing")
    _, disk, central_disk, _, entries, central_size, offset, comment = struct.unpack(
        "<4s4H2LH", tail[index : index + 22]
    )
    if disk or central_disk or not entries or len(tail) - index != 22 + comment:
        raise ValueError("Invalid or multipart ZIP")
    if offset != 0xFFFFFFFF and central_size != 0xFFFFFFFF:
        if offset + central_size > size - (22 + comment):
            raise ValueError("ZIP central directory extends beyond the archive")


def stream_zip(session, bucket, entry, name, previous, headers, run_id):
    request_headers = {"Accept-Encoding": "identity"}
    if headers["source_etag"] and not headers["source_etag"].startswith("W/"):
        request_headers["If-Match"] = headers["source_etag"]
    with session.get(
        entry.url, stream=True, timeout=HTTP_TIMEOUT, headers=request_headers
    ) as response:
        response.raise_for_status()
        actual_headers = source_headers(response)
        if (
            headers["source_etag"]
            and actual_headers["source_etag"] != headers["source_etag"]
        ):
            raise ValueError("Source changed between HEAD and GET; retry")
        expected_size = int(actual_headers["source_content_length"] or 0)
        blob = bucket.blob(name)
        blob.metadata = {
            **actual_headers,
            "source_url": entry.url,
            "catalog_sha256": fingerprint(entry),
            "producer_run_id": str(run_id),
            "ingested_at_utc": datetime.now(UTC).isoformat(),
            "listed_rows": str(entry.listed_rows),
        }
        digest = hashlib.md5(usedforsecurity=False)
        size, tail, start = 0, b"", b""
        # Exceptions terminate the resumable upload; a partial object is never
        # published. Preconditions protect against writers outside our SQL lock.
        with blob.open(
            "wb",
            chunk_size=CHUNK_SIZE,
            content_type="application/zip",
            if_generation_match=int(previous.generation) if previous else 0,
            checksum="crc32c",
            timeout=90,
        ) as target:
            for chunk in response.iter_content(chunk_size=256 * 1024):
                if not chunk:
                    continue
                start = (start + chunk)[:4]
                if len(start) == 4 and start != b"PK\x03\x04":
                    raise ValueError("Response is not a ZIP file")
                size += len(chunk)
                digest.update(chunk)
                tail = (tail + chunk)[-65557:]
                target.write(chunk)
            if expected_size and size != expected_size:
                raise ValueError(f"Incomplete source: {size} of {expected_size} bytes")
            validate_zip_end(tail, size)
        blob.reload()
        artifact = artifact_from_blob(blob, bucket.name)
        if artifact.size != size or artifact.md5 != digest.hexdigest():
            # Never mark unverified data downloaded. A subsequent run can repair it.
            raise ValueError("GCS size/MD5 differs from the downloaded byte stream")
        blob.metadata = {**blob.metadata, "download_md5_hex": artifact.md5}
        blob.patch(
            if_generation_match=int(artifact.generation),
            if_metageneration_match=blob.metageneration,
        )
        return artifact


def retryable(exc):
    if isinstance(exc, requests.HTTPError):
        return exc.response.status_code in {408, 412, 429, 500, 502, 503, 504}
    return isinstance(
        exc,
        (
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,
            gcs_errors.TooManyRequests,
            gcs_errors.ServiceUnavailable,
            gcs_errors.InternalServerError,
            gcs_errors.GatewayTimeout,
            gcs_errors.PreconditionFailed,
            ValueError,
        ),
    )


def download_one(entry, state, bucket, session, run_id, prefix, sleep=time.sleep):
    with state.lock(entry):
        file_id, old_state, old_uri, old_generation = state.register(entry, run_id)
        if entry.status != "listed_nonempty":
            return (
                "skipped_empty" if entry.status == "listed_empty" else "skipped_missing"
            )
        try:
            for attempt in range(ATTEMPTS):
                try:
                    with session.head(
                        entry.url,
                        timeout=HTTP_TIMEOUT,
                        allow_redirects=True,
                        headers={"Accept-Encoding": "identity"},
                    ) as response:
                        response.raise_for_status()
                        headers = source_headers(response)
                    name = f"{prefix}/{entry.year}/{entry.file_name}"
                    previous = bucket.get_blob(name)
                    if reusable(previous, entry, headers):
                        artifact = artifact_from_blob(previous, bucket.name)
                        outcome = (
                            "unchanged"
                            if (
                                old_state == "downloaded"
                                and old_uri == artifact.uri
                                and old_generation == artifact.generation
                            )
                            else "reconciled"
                        )
                    else:
                        artifact = stream_zip(
                            session, bucket, entry, name, previous, headers, run_id
                        )
                        outcome = "downloaded"
                    state.downloaded(file_id, artifact, run_id)
                    return outcome
                except Exception as exc:
                    if attempt + 1 == ATTEMPTS or not retryable(exc):
                        raise
                    sleep(2**attempt)
        except Exception:
            state.failed(file_id, run_id)
            raise


def work_batch(entries, bucket_name, project, run_id, prefix):
    results = []
    with db.connect() as conn, requests.Session() as session:
        state = State(conn)
        bucket = storage.Client(project=project).bucket(bucket_name)
        session.headers["User-Agent"] = "ChicagoAirQualityDownload/0.1"
        for entry in entries:
            try:
                outcome = download_one(entry, state, bucket, session, run_id, prefix)
                result = {
                    "pollutant": entry.pollutant,
                    "year": entry.year,
                    "outcome": outcome,
                }
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


def verify_state(entries, conn, client, bucket_name, prefix):
    """Read-only reconciliation against a fresh GCS listing and SQL history."""
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            "SELECT pollutant, year, catalog_status, download_state, raw_uri, "
            "raw_generation, raw_md5, raw_size_bytes FROM ingest_file"
        )
        rows = {(row[0], row[1]): row for row in cursor.fetchall()}
        cursor.execute(
            "SELECT f.pollutant, f.year, v.raw_generation, v.raw_uri, v.raw_md5, "
            "v.raw_size_bytes FROM ingest_file_version v "
            "JOIN ingest_file f USING (ingest_file_id)"
        )
        versions = {tuple(row) for row in cursor.fetchall()}
    blobs = {
        blob.name: blob for blob in client.list_blobs(bucket_name, prefix=prefix + "/")
    }
    mismatches, verified = [], []
    skipped = 0
    for entry in entries:
        key = (entry.pollutant, entry.year)
        row = rows.get(key)
        if row is None or row[2] != entry.status:
            mismatches.append(f"Missing/mismatched catalog state: {key}")
            continue
        if entry.status != "listed_nonempty":
            if row[3] != "skipped":
                mismatches.append(f"Empty/missing cell is not skipped: {key}")
            else:
                skipped += 1
            continue
        blob = blobs.get(f"{prefix}/{entry.year}/{entry.file_name}")
        if blob is None:
            mismatches.append(f"GCS object missing: {key}")
            continue
        artifact = artifact_from_blob(blob, bucket_name)
        expected = (artifact.uri, artifact.generation, artifact.md5, artifact.size)
        version = (*key, artifact.generation, artifact.uri, artifact.md5, artifact.size)
        if (
            row[3] != "downloaded"
            or tuple(row[4:]) != expected
            or version not in versions
        ):
            mismatches.append(f"SQL state/history differs from GCS: {key}")
        elif (blob.metadata or {}).get("download_md5_hex") != artifact.md5:
            mismatches.append(f"Verified stream checksum differs from GCS: {key}")
        else:
            verified.append(
                {"pollutant": entry.pollutant, "year": entry.year, **asdict(artifact)}
            )
    tracked = {row[4] for row in rows.values()}
    for name in blobs:
        if f"gs://{bucket_name}/{name}" not in tracked:
            mismatches.append(f"GCS object absent from SQL state: {name}")
    return {
        "verified_at_utc": datetime.now(UTC).isoformat(),
        "selected": len(entries),
        "matched_nonempty": len(verified),
        "skipped": skipped,
        "listed_gcs_objects": len(blobs),
        "version_rows": len(versions),
        "actual_zip_bytes": sum(item["size"] for item in verified),
        "mismatches": mismatches,
        "objects": verified,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", type=Path, default=ROOT / "output/epa/manifest.csv"
    )
    parser.add_argument("--settings", type=Path, default=ROOT / "config/settings.yaml")
    parser.add_argument("--years", type=parse_years)
    parser.add_argument("--pollutants", help="Comma-separated configured group names")
    parser.add_argument("--concurrency", type=int, default=2, choices=range(1, 5))
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate and list; no I/O"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Read-only check of SQL state/history against GCS listing",
    )
    args = parser.parse_args(argv)
    try:
        _, _, groups = load_settings(args.settings)
        pollutants = set(args.pollutants.split(",")) if args.pollutants else None
        if pollutants and pollutants - groups.keys():
            raise ValueError(
                f"Unknown pollutants: {sorted(pollutants - groups.keys())}"
            )
        entries = load_manifest(args.manifest, groups, args.years, pollutants)
        if args.dry_run:
            print(
                json.dumps(
                    {
                        "selected": len(entries),
                        "statuses": dict(Counter(row.status for row in entries)),
                    }
                )
            )
            return 0
        settings = yaml.safe_load(args.settings.read_text())
        prefix = (
            os.environ.get("AIR_QUALITY_GCS_RAW_EPA_PREFIX")
            or settings["gcs"]["raw_epa_prefix"]
        ).strip("/")
        if not prefix or ".." in prefix.split("/"):
            raise ValueError("Invalid raw EPA object prefix")
        bucket_name = os.environ["AIR_QUALITY_GCS_BUCKET"]
        project = os.environ["AIR_QUALITY_GCP_PROJECT_ID"]
        if args.verify_only:
            with db.connect() as conn:
                report = verify_state(
                    entries, conn, storage.Client(project=project), bucket_name, prefix
                )
            print(json.dumps(report, default=str), flush=True)
            return 1 if report["mismatches"] else 0
        run_id = uuid.uuid4()
        with db.connect() as conn:
            state = State(conn)
            state.start_run(
                run_id,
                len(entries),
                {
                    "manifest_sha256": hashlib.sha256(
                        args.manifest.read_bytes()
                    ).hexdigest(),
                    "bucket": bucket_name,
                    "prefix": prefix,
                    "cloud_run_execution": os.environ.get("CLOUD_RUN_EXECUTION"),
                    "concurrency": args.concurrency,
                },
            )
            counts, errors = Counter(), []
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                futures = [
                    pool.submit(
                        work_batch,
                        entries[index :: args.concurrency],
                        bucket_name,
                        project,
                        run_id,
                        prefix,
                    )
                    for index in range(min(args.concurrency, len(entries)))
                ]
                for future in as_completed(futures):
                    try:
                        results = future.result()
                        counts.update(item["outcome"] for item in results)
                        errors.extend(
                            item for item in results if item["outcome"] == "failed"
                        )
                    except Exception as exc:
                        errors.append(
                            {"worker_error": f"{type(exc).__name__}: {exc}"[:500]}
                        )
            state.finish_run(run_id, counts, errors)
        print(
            json.dumps(
                {"run_id": str(run_id), "outcomes": dict(counts), "errors": errors}
            ),
            flush=True,
        )
        return 1 if errors else 0
    except (OSError, ValueError, KeyError, yaml.YAMLError) as exc:
        print(f"EPA download failed: {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
