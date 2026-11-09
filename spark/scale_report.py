"""Generate the S12 report from verified commits; keep all unrun cells visible."""

import csv
import json
from datetime import UTC, datetime

from spark.cloud_epa import ROOT


def build_report(root=ROOT):
    with (root / "output/epa/manifest.csv").open() as stream:
        catalog = list(csv.DictReader(stream))
    commits = {}
    for path in sorted((root / "output/spark").glob("*/commit.json")):
        record = json.loads(path.read_text())
        year = record["year"]
        if (
            year not in commits
            or record["published_at_utc"] > commits[year]["published_at_utc"]
        ):
            commits[year] = record
    stages = json.loads((root / "output/epa/stage_verification.json").read_text())
    staged = {(r["year"], r["pollutant"]): r for r in stages["objects"]}
    processed = {
        (r["year"], r["pollutant"]): r
        for commit in commits.values()
        for r in commit["sources"]
    }
    columns = [
        "year",
        "file_group",
        "catalog_status",
        "listed_rows",
        "staged_rows",
        "input_bytes",
        "input_rows",
        "output_rows",
        "duplicate_rows",
        "rejected_rows",
        "status",
        "batch",
    ]
    records = []
    for row in catalog:
        key = (int(row["year"]), row["pollutant"])
        stage = staged.get(key, {})
        result = processed.get(key, {})
        status = "not_processed"
        if row["status"] == "listed_empty":
            status = "empty_hourly_catalog_cell"
        elif row["status"] == "missing":
            status = "missing_catalog_cell"
        elif result:
            status = (
                "reconciled_with_exceptions"
                if result["rejected_rows"]
                else "reconciled"
            )
        records.append(
            {
                "year": key[0],
                "file_group": key[1],
                "catalog_status": row["status"],
                "listed_rows": row["listed_rows"],
                "staged_rows": stage.get("staged_rows"),
                "input_bytes": stage.get("csv_bytes"),
                **{
                    k: result.get(k)
                    for k in [
                        "input_rows",
                        "output_rows",
                        "duplicate_rows",
                        "rejected_rows",
                    ]
                },
                "status": status,
                "batch": commits[key[0]]["batch"] if result else "",
            }
        )
    destination = root / "output/spark/reconciliation.csv"
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)
    total_input = sum(r["input_rows"] or 0 for r in records)
    total_output = sum(r["output_rows"] or 0 for r in records)
    removed = sum(r["duplicate_rows"] or 0 for r in records)
    rejected = sum(r["rejected_rows"] or 0 for r in records)
    total_catalog = sum(int(r["listed_rows"] or 0) for r in catalog)
    lines = [
        "# S12: Cloud PySpark scale report",
        "",
        f"Updated {datetime.now(UTC).isoformat()} from verified batch results.",
        "",
        f"**Processed {total_input:,} of {total_catalog:,} catalog rows "
        f"({total_input / total_catalog:.2%}).** Curated rows: **{total_output:,}**; "
        f"duplicates removed: **{removed:,}**; exception rows: **{rejected:,}**.",
        "",
        f"**Remaining shortfall: {total_catalog - total_input:,} rows.** "
        "Unprocessed inputs are not counted as normalized output. "
        "Full historical S12 acceptance remains open. This delivery intentionally "
        "covers the 2026 pilot; the 1990–2025 backfill is deferred.",
        "",
        "The saved EPA catalog covers 1990–2026 and was published in June 2026; "
        "2026 is a partial year. There are 296 year × file-group cells: "
        "233 nonempty and 63 empty. Empty hourly cells do not imply no "
        "longer-duration observations exist.",
        "",
        "[All manifest cells](../output/spark/reconciliation.csv). "
        "Identity: input = curated + removed duplicates + exceptions. "
        "Each batch first checks actual CSV record counts against S10 and the catalog, "
        "then reads the written Parquet back. Input bytes are staged CSV sizes; "
        "output bytes are actual Parquet object lengths.",
        "",
        "## Verified batches",
        "",
        "| Archive year | Input rows | Input bytes | Curated rows | Duplicates | "
        "Exceptions | Output bytes | Transform seconds | DCU hours | "
        "Compute estimate USD |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for year, commit in sorted(commits.items()):
        sources = commit["sources"]
        usage = commit.get("usage", {})
        lines.append(
            f"| {year} | {sum(r['input_rows'] for r in sources):,} | "
            f"{sum(r['csv_bytes'] for r in sources):,} | "
            f"{sum(r['output_rows'] for r in sources):,} | "
            f"{sum(r['duplicate_rows'] for r in sources):,} | "
            f"{sum(r['rejected_rows'] for r in sources):,} | "
            f"{commit['output_bytes']:,} | "
            f"{commit['elapsed_seconds']:.1f} | "
            f"{int(usage.get('milliDcuSeconds', 0)) / 3600000:.4f} | "
            f"{commit['compute_cost_usd_estimate']:.4f} |"
        )
    lines += [
        "",
        "## Output by observed parameter",
        "",
        "| Archive year | Parameter code | Curated rows | Parquet bytes | "
        "Advisory flagged | Null measurements | Negative measurements | Nonaligned |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for year, commit in sorted(commits.items()):
        for row in sorted(commit["pollutants"], key=lambda r: r["pollutant"]):
            size = sum(
                f["bytes"]
                for f in commit["files"]
                if f"/pollutant={row['pollutant']}/" in f["uri"]
            )
            lines.append(
                f"| {year} | {row['pollutant']} | {row['rows']:,} | {size:,} | "
                f"{row['advisory_rows']:,} | {row['null_measurement_rows']:,} | "
                f"{row['negative_measurement_rows']:,} | "
                f"{row.get('non_hour_aligned_rows', 0):,} |"
            )
    lines += [
        "",
        "## Runtime, cost, and evidence",
        "",
        "Each batch uses two fixed executors with four cores each, a four-core driver, "
        "64 shuffle partitions, adaptive SQL execution, and a 30-minute TTL. "
        "Python 3.15 is mandatory. The Docker image is pinned by digest; "
        "actual Python/Spark versions and properties are preserved in each commit.",
        "",
        "Transform duration excludes cloud startup and publication. DCU usage comes "
        "from Dataproc approximateUsage; estimated compute cost is milliDcuSeconds / "
        "3,600,000 × $0.06. Shuffle estimate is reported shuffleStorageGbSeconds / "
        "3,600 × $0.000054795; both components are retained in commit evidence. "
        "This is a compute estimate, not a billing invoice or total cost. "
        "Build time, retained GCS data, operations, and credits are separate. "
        "[Google Cloud pricing](https://cloud.google.com/products/managed-service-for-apache-spark/pricing).",
        "",
    ]
    for year, commit in sorted(commits.items()):
        batch = commit["batch"]
        lines.append(
            f"- {year}: `{batch}`; Python {commit['python_version']}, "
            f"Spark {commit['spark_version']}; "
            f"[metrics](../output/spark/{batch}/commit.json)."
        )
    lines += [
        "",
        "## All attempts, including failures",
        "",
        "| Batch | State | Compute USD | Shuffle USD |",
        "| --- | --- | ---: | ---: |",
    ]
    attempt_cost = 0
    for path in sorted((root / "output/spark").glob("*/batch.json")):
        batch = json.loads(path.read_text())
        submission = json.loads((path.parent / "submission.json").read_text())
        usage = batch.get("runtimeInfo", {}).get("approximateUsage", {})
        cost = int(usage.get("milliDcuSeconds", 0)) / 3600000 * 0.06
        shuffle = int(usage.get("shuffleStorageGbSeconds", 0)) / 3600 * 0.000054795
        attempt_cost += cost + shuffle
        cost_text = f"{cost:.4f}" if usage else "not yet reported"
        shuffle_text = f"{shuffle:.4f}" if usage else "not yet reported"
        lines.append(
            f"| {path.parent.name} | {batch.get('state', 'SUBMITTED')} "
            f"({submission['status']}) | "
            f"{cost_text} | {shuffle_text} |"
        )
    lines += [
        "",
        f"All-attempt compute + shuffle estimate: ${attempt_cost:.4f}.",
        "Missing usage is not counted as zero-cost evidence. The first pilot "
        "failed while pulling the private image (403); the repository-only "
        "Spark read grant was then added through Terraform before retry.",
        "A successful diagnostic run then exposed an overly strict zero-minute "
        "timestamp check. Its output was not published: 3,579 valid `:26` samples "
        "would have been quarantined. The corrected job preserves exact sampling "
        "time, adds an explicit UTC-hour key/alignment flag, and successfully "
        "reran the year.",
    ]
    lines += [
        "",
        "## Interpretation and handoff",
        "",
        "Partition `year` is the EPA archive/local-standard calendar year. "
        "New Year UTC spillover remains in the same archive partition. "
        "Use observed_at_utc for event-time filtering. "
        "Input files preserve parameter codes, units/bases, and method metadata. "
        "Superseded PM2.5 methods are flagged and lose same-hour ties, but otherwise "
        "remain visible. Null/negative concentrations remain in curated data.",
        "",
        "S13 must consume exact file URIs/generations from "
        "`curated/epa_hourly/_commits/{year}.json`; recursive wildcard loading can "
        "include old or interrupted attempts. Only a verified successful batch "
        "can publish a commit. Raw inputs and prior generations remain recoverable. "
        "Cloud SQL process_state has not been updated: the Spark identity has no "
        "SQL write grants. These commit records are the S12 processing ledger.",
        "",
        "S15 owns national cross-year key validation and warehouse reconciliation. "
        "[Run and recovery instructions](spark_normalization.md).",
        "",
    ]
    (root / "docs/scale_report.md").write_text("\n".join(lines))
    return records


if __name__ == "__main__":
    build_report()
