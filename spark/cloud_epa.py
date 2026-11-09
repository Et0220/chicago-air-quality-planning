"""S12 CLI: immutable inputs, asynchronous submission, verified publication."""

import argparse
import base64
import csv
import hashlib
import json
import os
import re
import subprocess
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CODE_FILES = (
    "spark/__init__.py",
    "spark/schemas.py",
    "spark/normalize_epa.py",
    "spark/run_epa.py",
)


def selected_sources(evidence, catalog, year):
    expected = {
        r["pollutant"]: r
        for r in catalog
        if int(r["year"]) == year and r["status"] == "listed_nonempty"
    }
    sources = [r for r in evidence["objects"] if r["year"] == year]
    if not expected or len(sources) != len(expected):
        raise ValueError("A complete staged archive year is required")
    if len({r["pollutant"] for r in sources}) != len(sources):
        raise ValueError("Duplicate staged source group")
    for row in sources:
        if row["pollutant"] not in expected:
            raise ValueError("Unexpected staged source group")
        if int(expected[row["pollutant"]]["listed_rows"]) != row["staged_rows"]:
            raise ValueError("Catalog/staged row count mismatch")
        if row["listed_rows"] != row["staged_rows"]:
            raise ValueError("Staging evidence does not reconcile")
    return sorted(sources, key=lambda r: r["pollutant"])


def fingerprint(sources, image, runtime):
    payload = {
        "sources": sources,
        "image": image,
        "runtime": runtime,
        "code": {
            p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in CODE_FILES
        },
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def gcloud(*arguments):
    result = subprocess.run(
        ["gcloud", "--format=json", *arguments],
        check=True,
        text=True,
        capture_output=True,
    )
    return json.loads(result.stdout)


def blob_for(client, uri, generation=None):
    if not uri.startswith("gs://"):
        raise ValueError("Expected GCS URI")
    bucket, name = uri[5:].split("/", 1)
    return client.bucket(bucket).blob(name, generation=generation)


def copy_verified(source, target):
    """Server-side generation-pinned copy, never silently replace an object."""
    source.reload()
    if target.exists():
        target.reload()
    else:
        token = None
        while True:
            token, _, _ = target.rewrite(
                source,
                token=token,
                if_generation_match=0,
                if_source_generation_match=int(source.generation),
            )
            if token is None:
                break
        target.reload()
    if target.size != source.size or target.md5_hash != source.md5_hash:
        raise ValueError(f"Copy checksum mismatch: {target.name}")
    return target


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def check_stage(source, row):
    source.reload()
    metadata = source.metadata or {}
    checks = [
        str(source.generation) == row["stage_generation"],
        source.size == row["csv_bytes"],
        base64.b64decode(source.md5_hash).hex() == row["csv_md5"],
        metadata.get("csv_rows") == str(row["staged_rows"]),
        metadata.get("source_generation") == row["raw_generation"],
        metadata.get("source_uri") == row["raw_uri"],
    ]
    if not all(checks):
        raise ValueError(f"Live staging validation failed: {row['stage_uri']}")


def submit(args, client, sources):
    if not re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", args.image or ""):
        raise ValueError("--image must be an immutable Artifact Registry digest")
    identity = fingerprint(sources, args.image, args.runtime)
    target_bucket = client.bucket(args.bucket)
    commit = target_bucket.blob(f"{args.curated_prefix}/_commits/{args.year}.json")
    prior = None
    if commit.exists():
        commit.reload()
        prior = json.loads(commit.download_as_text())
        if prior["input_fingerprint"] == identity:
            for item in prior["files"]:
                blob = blob_for(client, item["uri"], int(item["generation"]))
                blob.reload()
                if blob.size != item["bytes"] or blob.md5_hash != item["md5"]:
                    raise ValueError("Previously published output failed verification")
            print(
                json.dumps(
                    {
                        "status": "unchanged",
                        "year": args.year,
                        "input_fingerprint": identity,
                    }
                )
            )
            return
    batch = f"aq-s12-{args.year}-{datetime.now(UTC):%Y%m%d%H%M%S}"
    folder = ROOT / "output/spark" / batch
    lock = target_bucket.blob(f"{args.curated_prefix}/_locks/{args.year}.json")
    # A failed/aborted run retains its lock. Only explicit safe resume releases it.
    lock.upload_from_string(
        json.dumps({"batch": batch, "year": args.year}),
        content_type="application/json",
        if_generation_match=0,
    )
    lock.reload()
    state = {
        "batch": batch,
        "project": args.project,
        "region": args.region,
        "scratch": args.scratch,
        "bucket": args.bucket,
        "curated_prefix": args.curated_prefix,
        "lock_generation": str(lock.generation),
        "year": args.year,
        "prior_commit_generation": str(commit.generation) if prior else "0",
        "input_fingerprint": identity,
        "image": args.image,
        "runtime": args.runtime,
        "ttl_minutes": args.ttl_minutes,
        "status": "preparing",
    }
    save(folder / "submission.json", state)
    prepared = []
    scratch_bucket = client.bucket(args.scratch)
    for row in sources:
        source = blob_for(client, row["stage_uri"], int(row["stage_generation"]))
        check_stage(source, row)
        name = (
            f"s12/inputs/{args.year}/{row['pollutant']}/g{row['stage_generation']}.csv"
        )
        snapshot = copy_verified(source, scratch_bucket.blob(name))
        prepared.append(
            {
                **row,
                "input_uri": f"gs://{args.scratch}/{name}",
                "input_generation": str(snapshot.generation),
            }
        )
    manifest = {
        "sources": prepared,
        "input_fingerprint": identity,
        "ingested_at_utc": datetime.now(UTC).isoformat(),
    }
    save(folder / "manifest.json", manifest)
    package = folder / "epa_spark.zip"
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as archive:
        for file in CODE_FILES:
            archive.write(ROOT / file, file)
    uris = {}
    for label, path in [
        ("package", package),
        ("manifest", folder / "manifest.json"),
        ("main", ROOT / "spark/run_epa.py"),
    ]:
        blob = scratch_bucket.blob(f"s12/runs/{batch}/{path.name}")
        blob.upload_from_filename(str(path), if_generation_match=0)
        uris[label] = f"gs://{args.scratch}/{blob.name}"
    output = f"gs://{args.scratch}/s12/runs/{batch}/result"
    state.update({"output": output, "status": "ready"})
    save(folder / "submission.json", state)
    properties = (
        "spark.dynamicAllocation.enabled=false,spark.executor.instances=2,"
        "spark.executor.cores=4,spark.driver.cores=4,spark.sql.shuffle.partitions=64,"
        "spark.sql.adaptive.enabled=true,spark.executorEnv.TZ=UTC,"
        "spark.driverEnv.TZ=UTC"
    )
    result = gcloud(
        "dataproc",
        "batches",
        "submit",
        "pyspark",
        uris["main"],
        f"--project={args.project}",
        f"--region={args.region}",
        f"--batch={batch}",
        f"--version={args.runtime}",
        f"--container-image={args.image}",
        f"--service-account=aq-spark@{args.project}.iam.gserviceaccount.com",
        f"--subnet=projects/{args.project}/regions/{args.region}/subnetworks/aq-spark",
        "--tags=aq-spark",
        f"--staging-bucket={args.scratch}",
        f"--deps-bucket=gs://{args.scratch}",
        f"--ttl={args.ttl_minutes}m",
        f"--py-files={uris['package']}",
        f"--files={uris['manifest']}",
        f"--properties={properties}",
        "--async",
        "--",
        "--manifest=manifest.json",
        f"--output={output}",
    )
    state["status"] = "submitted"
    save(folder / "submission.json", state)
    save(folder / "batch.json", result)
    print(json.dumps({"batch": batch, "folder": str(folder), "status": "submitted"}))


def finalize(args, client):
    folder = ROOT / "output/spark" / args.batch
    state = json.loads((folder / "submission.json").read_text())
    batch = gcloud(
        "dataproc",
        "batches",
        "describe",
        args.batch,
        f"--project={state['project']}",
        f"--region={state['region']}",
    )
    save(folder / "batch.json", batch)
    if batch["state"] != "SUCCEEDED":
        raise ValueError(f"Batch is {batch['state']}; no output was published")
    usage = batch.get("runtimeInfo", {}).get("approximateUsage", {})
    if "milliDcuSeconds" not in usage or "shuffleStorageGbSeconds" not in usage:
        raise ValueError("Final usage is not available yet; retry finalize shortly")
    metrics = json.loads(
        blob_for(client, state["output"] + "/metrics.json").download_as_text()
    )
    if (
        metrics["status"] != "verified"
        or metrics["input_fingerprint"] != state["input_fingerprint"]
    ):
        raise ValueError("Unexpected batch result")
    manifest = json.loads((folder / "manifest.json").read_text())
    if len(metrics["sources"]) != len(manifest["sources"]):
        raise ValueError("Incomplete batch metrics")
    for row in metrics["sources"]:
        if (
            row["input_rows"]
            != row["output_rows"] + row["duplicate_rows"] + row["rejected_rows"]
        ):
            raise ValueError("Batch row reconciliation failed")
    prefix = state["curated_prefix"]
    bucket = client.bucket(state["bucket"])
    commit = bucket.blob(f"{prefix}/_commits/{state['year']}.json")
    # Finalize is itself replayable after successful publication.
    if commit.exists():
        current = json.loads(commit.download_as_text())
        if current.get("batch") == args.batch:
            save(folder / "commit.json", current)
            old_lock = bucket.blob(f"{prefix}/_locks/{state['year']}.json")
            if old_lock.exists():
                old_lock.reload()
                if str(old_lock.generation) == state["lock_generation"]:
                    old_lock.delete(if_generation_match=int(state["lock_generation"]))
            print(json.dumps({"status": "already_published", "batch": args.batch}))
            return
    lock = bucket.blob(f"{prefix}/_locks/{state['year']}.json")
    lock.reload()
    if str(lock.generation) != state["lock_generation"]:
        raise ValueError("Year lock changed; refusing publication")
    files = []
    for item in metrics["parquet_files"]:
        if not item["uri"].startswith(state["output"] + "/data/"):
            raise ValueError("Output file escaped its batch path")
        relative = item["uri"].split("/data/", 1)[1]
        if not re.fullmatch(
            r"year=" + str(state["year"]) + r"/pollutant=\d{5}/[^/]+\.parquet", relative
        ):
            raise ValueError("Unexpected output partition")
        parent, filename = relative.rsplit("/", 1)
        destination = bucket.blob(f"{prefix}/{parent}/{args.batch}-{filename}")
        source = blob_for(client, item["uri"])
        source.reload()
        if source.size != item["bytes"]:
            raise ValueError("Output byte count changed")
        copied = copy_verified(source, destination)
        files.append(
            {
                "uri": f"gs://{state['bucket']}/{copied.name}",
                "generation": str(copied.generation),
                "bytes": copied.size,
                "md5": copied.md5_hash,
            }
        )
    # Durable evidence lives outside the seven-day scratch lifecycle.
    for name in ("manifest.json", "batch.json"):
        bucket.blob(f"{prefix}/_runs/{args.batch}/{name}").upload_from_filename(
            str(folder / name)
        )
    rejection_files = []
    scratch_prefix = state["output"][5:].split("/", 1)[1] + "/exceptions/"
    for source in client.list_blobs(state["scratch"], prefix=scratch_prefix):
        if source.name.endswith(".parquet"):
            target = bucket.blob(
                f"{prefix}/_exceptions/{args.batch}/{source.name.rsplit('/', 1)[1]}"
            )
            copied = copy_verified(source, target)
            rejection_files.append(f"gs://{bucket.name}/{copied.name}")
    usage = batch.get("runtimeInfo", {}).get("approximateUsage", {})
    dcu_hours = int(usage.get("milliDcuSeconds", 0)) / 3600000
    shuffle_cost = int(usage.get("shuffleStorageGbSeconds", 0)) / 3600 * 0.000054795
    result = {
        **metrics,
        "batch": args.batch,
        "files": files,
        "exception_files": rejection_files,
        "usage": usage,
        "compute_cost_usd_estimate": round(dcu_hours * 0.06, 6),
        "shuffle_cost_usd_estimate": round(shuffle_cost, 6),
        "compute_and_shuffle_usd_estimate": round(dcu_hours * 0.06 + shuffle_cost, 6),
        "cost_note": (
            "Standard DCU and reported shuffle-unit estimates; excludes GCS storage, "
            "build, operations, and credits."
        ),
        "published_at_utc": datetime.now(UTC).isoformat(),
    }
    bucket.blob(f"{prefix}/_runs/{args.batch}/metrics.json").upload_from_string(
        json.dumps(result), content_type="application/json"
    )
    commit.upload_from_string(
        json.dumps(result),
        content_type="application/json",
        if_generation_match=int(state["prior_commit_generation"]),
    )
    save(folder / "commit.json", result)
    lock.delete(if_generation_match=int(state["lock_generation"]))
    state["status"] = "published"
    save(folder / "submission.json", state)
    print(
        json.dumps(
            {
                "status": "published",
                "batch": args.batch,
                "rows": sum(r["output_rows"] for r in result["sources"]),
                "bytes": sum(r["bytes"] for r in files),
                "compute_cost_usd_estimate": result["compute_cost_usd_estimate"],
            }
        )
    )


def release_failed(args, client):
    """Release only the matching lock of a confirmed failed/cancelled batch."""
    folder = ROOT / "output/spark" / args.batch
    state = json.loads((folder / "submission.json").read_text())
    batch = gcloud(
        "dataproc",
        "batches",
        "describe",
        args.batch,
        f"--project={state['project']}",
        f"--region={state['region']}",
    )
    save(folder / "batch.json", batch)
    retire = getattr(args, "action", None) == "retire-unpublished"
    if retire and not args.reason:
        raise ValueError("Retiring an unpublished diagnostic run requires --reason")
    allowed = {"SUCCEEDED"} if retire else {"FAILED", "CANCELLED"}
    if batch["state"] not in allowed:
        raise ValueError("Only FAILED/CANCELLED batches can release their lock")
    if retire:
        commit = client.bucket(state["bucket"]).blob(
            f"{state['curated_prefix']}/_commits/{state['year']}.json"
        )
        if (
            commit.exists()
            and json.loads(commit.download_as_text()).get("batch") == args.batch
        ):
            raise ValueError("Cannot retire the currently published batch")
        metrics = json.loads(
            blob_for(client, state["output"] + "/metrics.json").download_as_text()
        )
        metrics["retirement_reason"] = args.reason
        save(folder / "diagnostic_metrics.json", metrics)
        client.bucket(state["bucket"]).blob(
            f"{state['curated_prefix']}/_runs/{args.batch}/diagnostic_metrics.json"
        ).upload_from_string(json.dumps(metrics), content_type="application/json")
    lock = client.bucket(state["bucket"]).blob(
        f"{state['curated_prefix']}/_locks/{state['year']}.json"
    )
    lock.reload()
    if str(lock.generation) != state["lock_generation"]:
        raise ValueError("Lock generation changed; refusing to release it")
    if json.loads(lock.download_as_text())["batch"] != args.batch:
        raise ValueError("Lock belongs to another batch")
    lock.delete(if_generation_match=int(state["lock_generation"]))
    state["status"] = "diagnostic_retired" if retire else "failed_lock_released"
    if retire:
        state["retirement_reason"] = args.reason
    save(folder / "submission.json", state)
    print(json.dumps({"status": state["status"], "batch": args.batch}))


def main():
    settings = yaml.safe_load((ROOT / "config/settings.yaml").read_text())
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=["plan", "submit", "finalize", "release-failed", "retire-unpublished"],
    )
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--batch")
    parser.add_argument("--reason")
    parser.add_argument("--project", default=os.getenv("AIR_QUALITY_GCP_PROJECT_ID"))
    parser.add_argument("--bucket", default=os.getenv("AIR_QUALITY_GCS_BUCKET"))
    parser.add_argument("--scratch")
    parser.add_argument("--region", default="us-central1")
    parser.add_argument("--runtime", default="2.2")
    parser.add_argument("--ttl-minutes", type=int, default=30)
    parser.add_argument("--image", default=os.getenv("AIR_QUALITY_SPARK_IMAGE"))
    parser.add_argument(
        "--curated-prefix",
        default=os.getenv(
            "AIR_QUALITY_GCS_CURATED_EPA_PREFIX", settings["gcs"]["curated_epa_prefix"]
        ),
    )
    args = parser.parse_args()
    if not 10 <= args.ttl_minutes <= 120:
        parser.error("--ttl-minutes must be between 10 and 120")
    evidence = json.loads((ROOT / "output/epa/stage_verification.json").read_text())
    with (ROOT / "output/epa/manifest.csv").open() as stream:
        catalog = list(csv.DictReader(stream))
    sources = selected_sources(evidence, catalog, args.year)
    if args.action == "plan":
        print(
            json.dumps(
                {
                    "year": args.year,
                    "files": len(sources),
                    "input_rows": sum(r["staged_rows"] for r in sources),
                    "input_bytes": sum(r["csv_bytes"] for r in sources),
                    "executors": 2,
                    "cores_each": 4,
                    "driver_cores": 4,
                    "ttl_minutes": args.ttl_minutes,
                    "sources": sources,
                },
                indent=2,
            )
        )
        return
    if not args.project or not args.bucket:
        parser.error(
            "--project and --bucket (or their environment variables) are required"
        )
    if not args.scratch:
        args.scratch = args.project + "-aq-spark-scratch"
    from google.cloud import storage

    client = storage.Client(project=args.project)
    if args.action == "submit":
        submit(args, client, sources)
    else:
        if not args.batch or not re.fullmatch(r"aq-s12-\d{4}-\d{14}", args.batch):
            parser.error("--batch must be an S12 batch ID")
        if args.action in {"release-failed", "retire-unpublished"}:
            release_failed(args, client)
        else:
            finalize(args, client)


if __name__ == "__main__":
    main()
