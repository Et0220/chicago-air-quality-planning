"""Read-only reconciliation of AirNow SQL rows with immutable raw generations."""

import argparse
import hashlib
import json
import os
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from google.cloud import storage

from ingestion import db
from ingestion.airnow_collect import (
    Bounds,
    Observation,
    hour_window,
    parse_file,
    utc_hour,
)


def verify(conn, bucket, hours, bounds=Bounds()):
    fields = tuple(Observation.__dataclass_fields__)
    with closing(conn.cursor()) as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cur.execute(
            f"SELECT {','.join(fields)},retrieved_at_utc,raw_uri,source_generation,"
            "is_preliminary FROM airnow_obs WHERE observed_at_utc BETWEEN %s AND %s "
            "AND latitude BETWEEN %s AND %s AND longitude BETWEEN %s AND %s",
            (hours[0], hours[-1], bounds.south, bounds.north, bounds.west, bounds.east),
        )
        rows = cur.fetchall()
        cur.execute(
            "SELECT count(*) FROM airnow_obs_revision r JOIN airnow_obs o USING "
            "(airnow_site_id,pollutant,observed_at_utc) "
            "WHERE r.observed_at_utc BETWEEN %s AND %s "
            "AND o.latitude BETWEEN %s AND %s AND o.longitude BETWEEN %s AND %s",
            (hours[0], hours[-1], bounds.south, bounds.north, bounds.west, bounds.east),
        )
        revisions = cur.fetchone()[0]
        cur.execute(
            "SELECT run_id,status,started_at_utc,finished_at_utc,details->'totals' "
            "FROM ingest_run WHERE job_name='airnow_collect' "
            "AND details->>'start_hour_utc'=%s AND details->>'end_hour_utc'=%s "
            "ORDER BY started_at_utc",
            (hours[0].isoformat(), hours[-1].isoformat()),
        )
        runs = [
            dict(
                zip(
                    ("run_id", "status", "started_at_utc", "finished_at_utc", "totals"),
                    row,
                )
            )
            for row in cur.fetchall()
        ]
    conn.commit()
    report = {
        "verified_at_utc": datetime.now(UTC).isoformat(),
        "start_hour_utc": hours[0].isoformat(),
        "end_hour_utc": hours[-1].isoformat(),
        "bounds": asdict(bounds),
        "row_count": len(rows),
        "revision_count": revisions,
        "site_count": len({row[0] for row in rows}),
        "duplicate_key_count": len(rows) - len({tuple(row[:3]) for row in rows}),
        "non_preliminary_count": sum(not row[-1] for row in rows),
        "hour_count": len({row[2] for row in rows}),
        "missing_hours": [
            h.isoformat() for h in hours if h not in {r[2] for r in rows}
        ],
        "pollutants": {},
        "raw_snapshots": [],
        "mismatches": [],
        "runs": runs,
    }
    groups, pollutant_counts = defaultdict(list), defaultdict(Counter)
    for row in rows:
        obs = Observation(*row[: len(fields)])
        retrieved, uri, generation, _ = row[len(fields) :]
        groups[(obs.observed_at_utc, uri, generation)].append(obs)
        counts = pollutant_counts[obs.pollutant]
        counts["rows"] += 1
        counts["concentrations"] += obs.concentration is not None
        counts["aqi"] += obs.aqi is not None
        if retrieved < obs.observed_at_utc:
            report["mismatches"].append(f"Retrieval precedes observation: {obs.key}")
    report["pollutants"] = dict(pollutant_counts)
    for (hour, uri, generation), observations in sorted(groups.items()):
        prefix = f"gs://{bucket.name}/"
        try:
            if not uri.startswith(prefix):
                raise ValueError("Unexpected raw bucket")
            blob = bucket.blob(uri.removeprefix(prefix), generation=int(generation))
            blob.reload()
            content = blob.download_as_bytes(checksum="crc32c")
            digest = hashlib.sha256(content).hexdigest()
            if digest != (blob.metadata or {}).get("sha256"):
                raise ValueError("Raw SHA-256 metadata mismatch")
            if not blob.name.endswith(f"_{digest}.dat"):
                raise ValueError("Raw content-address mismatch")
            expected = {obs.key: obs for obs in parse_file(content, hour, bounds)}
            actual = {obs.key: obs for obs in observations}
            for key, obs in actual.items():
                if key in expected and obs != expected[key]:
                    report["mismatches"].append(f"SQL/source mismatch: {key}")
                elif key not in expected and obs.value != (None, None, None):
                    report["mismatches"].append(f"Missing withdrawal: {key}")
            for key in expected.keys() - actual.keys():
                report["mismatches"].append(f"Missing SQL key: {key}")
            report["raw_snapshots"].append(
                {
                    "hour": hour.isoformat(),
                    "raw_uri": uri,
                    "source_generation": generation,
                    "sha256": digest,
                    "bytes": len(content),
                    "source_observations": len(expected),
                }
            )
        except Exception as exc:
            report["mismatches"].append(f"{uri}#{generation}: {exc}")
    report["status"] = (
        "failed"
        if any(
            (
                report["missing_hours"],
                report["duplicate_key_count"],
                report["non_preliminary_count"],
                report["mismatches"],
            )
        )
        else "passed"
    )
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--end-hour", required=True, type=utc_hour)
    parser.add_argument("--hours", default=72, type=int)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    with (
        db.connect() as conn,
        storage.Client(project=os.environ.get("AIR_QUALITY_GCP_PROJECT_ID")) as client,
    ):
        report = verify(
            conn,
            client.bucket(os.environ["AIR_QUALITY_GCS_BUCKET"]),
            hour_window(args.end_hour, args.hours),
        )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(
        json.dumps(
            {
                key: value
                for key, value in report.items()
                if key not in {"raw_snapshots", "runs"}
            },
            default=str,
        )
    )
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
