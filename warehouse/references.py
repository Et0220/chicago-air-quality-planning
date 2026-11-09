"""Use the exact S07 snapshots already normalized into Cloud SQL."""

import gzip
import hashlib
import json
from collections import Counter
from pathlib import Path

from ingestion.aqs_listings import iter_listing, monitor_record, sha256, site_record
from warehouse.scd import event
from warehouse.sql import ROOT


def listing_event(stem, row, uri, generation):
    if stem == "aqs_sites":
        data = site_record(row, uri, generation)
        key, extracted = data[0], data[15]
        attrs = dict(
            zip(
                (
                    "state_code",
                    "county_code",
                    "site_num",
                    "latitude",
                    "longitude",
                    "datum",
                    "site_name",
                    "city_name",
                    "county_name",
                    "state_name",
                ),
                data[1:11],
                strict=True,
            )
        )
        attrs.update(
            address=row.get("Address") or None,
            established_date=data[13].isoformat() if data[13] else None,
            closed_date=data[14].isoformat() if data[14] else None,
        )
        kind = "site"
    else:
        data = monitor_record(row, uri, generation)
        key, extracted = data[0], data[11]
        attrs = dict(
            zip(
                (
                    "site_key",
                    "parameter_code",
                    "poc",
                    "method_code",
                    "method_name",
                    "latitude",
                    "longitude",
                    "datum",
                    "first_year_of_data",
                ),
                data[1:10],
                strict=True,
            )
        )
        attrs.update(
            last_sample_date=data[10].isoformat() if data[10] else None,
            status="unknown",
            activity_start=None,
            activity_end=None,
            monitoring_objective=row.get("Monitoring Objective") or None,
            pm25_advisory_superseded=data[12],
        )
        kind = "monitor"
    if extracted is None:
        raise ValueError("No extraction date: cannot assign dimension validity")
    return event(
        kind,
        key,
        extracted.isoformat() + "T00:00:00+00:00",
        attrs,
        uri,
        generation,
        "source extraction date; day precision, no earlier history",
    )


def prepare_references(client, bucket, directory):
    spec = json.loads((ROOT / "config/warehouse_references.json").read_text())
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "dimension_events.jsonl.gz"
    digest = hashlib.sha256()
    counts = Counter()
    with (
        target.open("wb") as output,
        gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as compressed,
    ):
        for stem, source in spec.items():
            path = directory / (stem + ".zip")
            if not path.exists() or sha256(path) != source["sha256"]:
                blob = client.bucket(bucket).blob(
                    source["name"], generation=int(source["generation"])
                )
                blob.download_to_filename(
                    str(path), if_generation_match=int(source["generation"])
                )
            if (
                path.stat().st_size != source["bytes"]
                or sha256(path) != source["sha256"]
            ):
                raise ValueError(f"S07 snapshot content mismatch: {stem}")
            seen = set()
            for row in iter_listing(path, stem):
                item = listing_event(
                    stem, row, f"gs://{bucket}/{source['name']}", source["generation"]
                )
                if item["natural_key"] in seen:
                    raise ValueError("Duplicate S07 reference key")
                seen.add(item["natural_key"])
                line = (
                    json.dumps(item, sort_keys=True, separators=(",", ":")) + "\n"
                ).encode()
                compressed.write(line)
                digest.update(line)
                counts[item["kind"]] += 1
            if len(seen) != source["rows"]:
                raise ValueError("S07 snapshot row count mismatch")
    return {
        "path": str(target.resolve()),
        "sha256": digest.hexdigest(),
        "gzip_sha256": sha256(target),
        "counts": dict(counts),
        "sources": spec,
    }
