"""Read one published commit; never discover data with a directory listing."""

import hashlib
import json
import re
from contextlib import contextmanager
from uuid import uuid4

from spark.cloud_epa import blob_for


def validate_commit(commit, bucket, prefix, year):
    batch = commit.get("batch", "")
    if not re.fullmatch(rf"aq-s12-{year}-\d{{14}}", batch):
        raise ValueError("Commit batch/year mismatch")
    if commit.get("status") != "verified" or commit.get("year") != year:
        raise ValueError("Only a verified commit for the requested year is accepted")
    files = commit.get("files", [])
    if not files or len({f["uri"] for f in files}) != len(files):
        raise ValueError("Commit files must be nonempty and unique")
    pattern = re.escape(f"gs://{bucket}/{prefix}/year={year}/pollutant=")
    pattern += r"(\d{5})/" + re.escape(batch) + r"-[A-Za-z0-9._-]+\.parquet"
    selected = []
    for item in files:
        match = re.fullmatch(pattern, item["uri"])
        if not match or not re.fullmatch(r"\d+", str(item["generation"])):
            raise ValueError("Invalid committed path or generation")
        if item["bytes"] <= 0 or not item.get("md5"):
            raise ValueError("Missing committed size/checksum")
        selected.append(
            {
                **item,
                "relative_path": item["uri"].removeprefix(f"gs://{bucket}/{prefix}/"),
                "year": year,
                "pollutant": match[1],
            }
        )
    expected = {}
    for row in commit["pollutants"]:
        code = row["pollutant"]
        if row["year"] != year or code in expected or row["rows"] <= 0:
            raise ValueError("Invalid or duplicate pollutant reconciliation cell")
        expected[code] = row["rows"]
    if {f["pollutant"] for f in selected} != set(expected):
        raise ValueError("File partitions disagree with committed pollutant counts")
    if sum(expected.values()) != sum(s["output_rows"] for s in commit["sources"]):
        raise ValueError("Committed source/pollutant totals disagree")
    if sum(f["bytes"] for f in selected) != commit["output_bytes"]:
        raise ValueError("Committed byte totals disagree")
    return selected, expected


def read_commit(client, bucket, prefix, year):
    blob = client.bucket(bucket).get_blob(f"{prefix}/_commits/{year}.json")
    if blob is None:
        raise ValueError("No published commit for requested year")
    raw = blob.download_as_bytes(if_generation_match=int(blob.generation))
    commit = json.loads(raw)
    files, expected = validate_commit(commit, bucket, prefix, year)
    return {
        "commit": commit,
        "commit_uri": f"gs://{bucket}/{blob.name}",
        "commit_generation": str(blob.generation),
        "commit_sha256": hashlib.sha256(raw).hexdigest(),
        "files": files,
        "expected": expected,
    }


def verify_snapshot(client, snapshot):
    commit = blob_for(client, snapshot["commit_uri"])
    commit.reload()
    if str(commit.generation) != snapshot["commit_generation"]:
        raise ValueError("Published commit changed; restart from the new commit")
    for item in snapshot["files"]:
        # Check the LIVE generation: Snowflake COPY reads live external-stage URLs.
        blob = blob_for(client, item["uri"])
        blob.reload()
        if (
            str(blob.generation) != str(item["generation"])
            or blob.size != item["bytes"]
            or blob.md5_hash != item["md5"]
        ):
            raise ValueError(f"Committed object is missing or changed: {item['uri']}")


@contextmanager
def year_lock(client, bucket, prefix, year):
    lock = client.bucket(bucket).blob(f"{prefix}/_locks/{year}.json")
    lock.upload_from_string(
        json.dumps({"batch": f"s13-{uuid4()}", "year": year}),
        content_type="application/json",
        if_generation_match=0,
    )
    generation = int(lock.generation)
    try:
        yield
    finally:
        lock.delete(if_generation_match=generation)


def reconcile(expected, actual):
    actual = {str(k): int(v) for k, v in actual.items()}
    if expected != actual:
        raise ValueError(
            f"Row reconciliation failed: expected={expected}, actual={actual}"
        )
