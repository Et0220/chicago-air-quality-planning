"""Preview or retire one superseded batch's live Parquet objects safely."""

import argparse
import json
import re
from datetime import UTC, datetime
from uuid import uuid4

from spark.cloud_epa import ROOT, blob_for, save


def read_json(blob):
    blob.reload()
    return json.loads(blob.download_as_text(if_generation_match=int(blob.generation)))


def candidates(current, previous, bucket, prefix, year, batch):
    if current["batch"] == batch or previous["batch"] != batch:
        raise ValueError("Only a superseded published batch can be retired")
    protected = {f["uri"] for f in current["files"]}
    pattern = (
        re.escape(f"gs://{bucket}/{prefix}/year={year}/pollutant=")
        + r"\d{5}/"
        + re.escape(batch)
        + r"-[^/]+\.parquet"
    )
    files = previous["files"]
    if not files or len({f["uri"] for f in files}) != len(files):
        raise ValueError("Missing or duplicate cleanup candidates")
    for item in files:
        if item["uri"] in protected or not re.fullmatch(pattern, item["uri"]):
            raise ValueError("Cleanup candidate is protected or outside batch scope")
        if not str(item["generation"]).isdigit() or not item["md5"]:
            raise ValueError("Cleanup requires generation and checksum evidence")
    return files


def verify_live(blob, item):
    blob.reload()
    if (
        str(blob.generation) != str(item["generation"])
        or blob.size != item["bytes"]
        or blob.md5_hash != item["md5"]
    ):
        raise ValueError(f"Live object differs from published evidence: {item['uri']}")


def cleanup(args, client):
    bucket = client.bucket(args.bucket)
    commit_blob = bucket.blob(f"{args.prefix}/_commits/{args.year}.json")
    lock = bucket.blob(f"{args.prefix}/_locks/{args.year}.json")
    lock_generation = None
    if args.apply:
        bucket.reload()
        if not bucket.versioning_enabled:
            raise ValueError("Cleanup requires bucket versioning for recoverability")
        lock.upload_from_string(
            json.dumps({"batch": f"cleanup-{uuid4()}", "year": args.year}),
            content_type="application/json",
            if_generation_match=0,
        )
        lock_generation = int(lock.generation)
    try:
        current = read_json(commit_blob)
        commit_generation = int(commit_blob.generation)
        previous = read_json(
            bucket.blob(f"{args.prefix}/_runs/{args.batch}/metrics.json")
        )
        files = candidates(
            current, previous, args.bucket, args.prefix, args.year, args.batch
        )
        # Check ALL candidates and current files before the first deletion.
        for item in current["files"]:
            verify_live(blob_for(client, item["uri"]), item)
        pending, absent = [], []
        for item in files:
            blob = blob_for(client, item["uri"])
            if not blob.exists():
                absent.append(item)
                continue
            verify_live(blob, item)
            pending.append(item)
        latest_commit = bucket.get_blob(commit_blob.name)
        if latest_commit is None or int(latest_commit.generation) != commit_generation:
            raise ValueError("Current commit changed during cleanup preflight")
        report = {
            "checked_at_utc": datetime.now(UTC).isoformat(),
            "status": "planned",
            "batch": args.batch,
            "year": args.year,
            "current_batch": current["batch"],
            "current_commit_generation": str(commit_generation),
            "candidate_files": len(files),
            "candidate_bytes": sum(f["bytes"] for f in files),
            "pending": pending,
            "already_absent": absent,
            "retired": [],
        }
        folder = ROOT / "output/spark" / args.batch
        save(folder / "cleanup_plan.json", report)
        if args.apply:
            for item in pending:
                # No generation in the resource URL: archive the live version.
                # The precondition prevents deleting a replacement generation.
                blob_for(client, item["uri"]).delete(
                    if_generation_match=int(item["generation"])
                )
                report["retired"].append(item)
            for item in current["files"]:
                verify_live(blob_for(client, item["uri"]), item)
            live = [
                f
                for f in client.list_blobs(
                    bucket, prefix=f"{args.prefix}/year={args.year}/"
                )
                if f.name.endswith(".parquet")
            ]
            latest_commit = bucket.get_blob(commit_blob.name)
            if (
                latest_commit is None
                or int(latest_commit.generation) != commit_generation
            ):
                raise ValueError("Current commit changed during cleanup")
            report.update(
                status="retired",
                live_files=len(live),
                live_bytes=sum(f.size for f in live),
                current_commit_unchanged=True,
                recovery="Retired generations remain noncurrent in versioned storage",
            )
            save(folder / "cleanup.json", report)
            bucket.blob(
                f"{args.prefix}/_runs/{args.batch}/cleanup.json"
            ).upload_from_string(json.dumps(report), content_type="application/json")
        return report
    finally:
        if lock_generation is not None:
            lock.delete(if_generation_match=lock_generation)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--batch", required=True)
    parser.add_argument("--prefix", default="curated/epa_hourly")
    parser.add_argument("--apply", action="store_true", help="Retire live objects")
    args = parser.parse_args()
    if not re.fullmatch(r"aq-s12-" + str(args.year) + r"-\d{14}", args.batch):
        parser.error("--batch must be an S12 batch ID for --year")
    from google.cloud import storage

    report = cleanup(args, storage.Client(project=args.project))
    print(json.dumps({k: v for k, v in report.items() if not isinstance(v, list)}))


if __name__ == "__main__":
    main()
