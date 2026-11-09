# Air Quality Planning for Outdoor Events

This project helps a Chicago-area outdoor fitness operator compare venues and
recurring class times using historical PM2.5, then review recent conditions
before a class. The planned pipeline lands national EPA AirData in Google Cloud
Storage, processes it with PySpark, keeps operational state in Cloud SQL, and
serves Chicago study tables from Snowflake to Streamlit and a read-only MCP
server. PurpleAir and AirNow provide recent context. See
[`plan/PROJECT_SEGMENTS.md`](plan/PROJECT_SEGMENTS.md) for scope and ownership.

## S01: scaffold and contracts

The shared schemas, keys, time rules, source mappings, and GCS object layout are
in [`docs/data_contracts.md`](docs/data_contracts.md). Non-secret defaults live
in [`config/settings.yaml`](config/settings.yaml). Copy `.env.example` to `.env`
for local development and fill values only when a later segment needs them.
Do not commit `.env` or credentials.

Use Python 3.15. Set up the development tools and run the checks:

```sh
python3.15 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
make lint test
```

Measurement files are not checked into this repository. S03 retains a small
public catalog snapshot and its manifest for reproducibility. EPA AirData comes
from its public [download catalog](https://aqs.epa.gov/aqsweb/airdata/download_files.html);
live sources and access requirements are recorded in the contracts. S01 is a
draft handoff until Yujie, Breanna, Bravo, and Tongyan review the contracts.
Changes to keys, grains, or units should be recorded there before dependent
segments implement them.

## Repository map

| Path | Purpose |
| --- | --- |
| `infra/` | Reproducible Google Cloud resources |
| `ingestion/` | Manifests, downloaders, and live collectors |
| `spark/` | Explicit schemas and distributed transformations |
| `cloudsql/migrations/` | Operational database schema |
| `snowflake/ddl/`, `snowflake/views/` | Warehouse model and serving layer |
| `analysis/eda/` | Exploratory work and Chicago scheduling analysis |
| `app/`, `mcp/` | Streamlit and read-only Claude access |
| `tests/fixtures/` | Small deterministic examples |
| `docs/`, `config/` | Contracts, reports, and non-secret configuration |

## S02: Google Cloud infrastructure

`infra/` contains Terraform for versioned GCS, Cloud SQL, job identities,
secret containers, Artifact Registry, paused Run job schedules, Spark networking,
and a project budget. See [`docs/infra.md`](docs/infra.md) for resource names,
cost assumptions, trial requirements, deployment, and cloud verification.

```sh
terraform -chdir=infra init -backend=false
make infra-check
```

These tests use a simulated Google provider and do not provision resources.
S02 is deployed in `final-team-514205`: the final real plan reports no changes,
all five placeholder jobs passed, and the cloud Spark Parquet check passed.
That S02 snapshot predates the enabled PurpleAir and AirNow collectors;
only live-sync remains paused in the November 9 validation. See
[`docs/infra_evidence.json`](docs/infra_evidence.json) for recorded cloud results.
`make infra-smoke` requires authentication and runs billable verification jobs
against the confirmed trial project.
It now requires `AIR_QUALITY_SPARK_IMAGE` to identify a digest-pinned
Python 3.15 Spark image; see [Spark setup](docs/spark_normalization.md).

## S03: EPA AirData manifest

`ingestion/epa_manifest.py` reads the hourly catalog and emits every configured
pollutant × year cell from 1990 through the latest catalog year, preserving
empty files and marking absent entries as `missing`. It prints row/size totals
and saves the HTML, source metadata, CSV, and per-pollutant JSON summary.

```sh
python -m ingestion.epa_manifest
# Reproduce the saved snapshot without network access:
python -m ingestion.epa_manifest --catalog-file output/epa/catalog.html \
  --output-dir tmp/epa-replay
```

The completed snapshot covers **296 cells (1990–2026): 233 nonempty, 63 empty,
0 missing**, totaling **907,024,502 listed rows** and **6.26 GiB** of estimated
compressed ZIP bytes. The catalog entries are dated June 25, 2026; latest-year
counts are partial. The scraper downloads only the catalog. See
[`output/epa/manifest.csv`](output/epa/manifest.csv),
[`output/epa/summary.json`](output/epa/summary.json), and
[`docs/epa_manifest.md`](docs/epa_manifest.md) for provenance, empty-file flags,
validation, and the S06 handoff. `make lint test` includes the saved-fixture tests.

## S05: Cloud SQL operational schema

`cloudsql/migrations/001_operational.sql` defines ingestion state, PurpleAir
sensor and poll data, AirNow current observations and revisions, and AQS site
and monitor listings. `ingestion/db.py` applies numbered SQL migrations in a
transaction, checks their hashes on rerun, and connects either to local
PostgreSQL or Cloud SQL through the Python connector. The migration and local
integration instructions are in [`docs/cloudsql.md`](docs/cloudsql.md).

```sh
docker compose -f cloudsql/compose.yaml up -d --wait
export AIR_QUALITY_CLOUDSQL_DSN='postgresql://air_quality_dev@127.0.0.1:5433/air_quality_test'
python -m ingestion.db migrate
export AIR_QUALITY_TEST_POSTGRES_DSN="$AIR_QUALITY_CLOUDSQL_DSN"
make lint test
```

The compose password is for the disposable loopback-only test database. Cloud
SQL requires an administrator connection for the first migration and SQL
grants; the S02 IAM roles alone do not have table privileges.

## S06: EPA ZIP downloader

`ingestion/epa_download.py` streams nonempty EPA ZIPs into versioned
`raw/epa/{year}/{file}.zip` objects, verifies their checksums and actual sizes,
and records current and historical generations in Cloud SQL. Empty entries
are recorded as skipped. It supports bounded concurrency, retries, selective
reruns, and a read-only SQL/GCS reconciliation command.

The full manifest is landed: **233 ZIPs (6.26 GiB), 63 empty entries skipped**.
The deployed Python 3.15 job's full rerun reused all 233 files without errors
or new generations. Final SQL/GCS reconciliation found **zero mismatches**.

```sh
python -m pip install -e '.[dev,epa]'
python -m ingestion.epa_download --dry-run
# With the GCS/project and Cloud SQL connection environment configured:
python -m ingestion.epa_download --years 2026 --pollutants carbon_monoxide,pm25_non_frm_fem,lead
python -m ingestion.epa_download --verify-only
```

The Cloud Run image uses `ingestion/Dockerfile.epa`; deployment, verification,
and S10 handoff details are in [`docs/epa_download.md`](docs/epa_download.md).

## S10: EPA CSV staging

`ingestion/epa_stage.py` validates each downloaded EPA ZIP at its recorded GCS
generation, streams its CSV member into `staging/epa/{param}/{year}/`, and
records the verified CSV row count in Cloud SQL. Staged object names include
the raw generation, so a source replacement cannot be confused with its
previous CSV. A read-only check compares SQL, GCS metadata, and manifest row
counts; differences are listed by pollutant and year.

The deployed job has staged **233/233 files**: **907,024,502 rows** and
**218,661,382,349 CSV bytes**. A full rerun reported 233 unchanged objects;
the final SQL/GCS/catalog verification found zero mismatches.

```sh
python -m ingestion.epa_stage --dry-run
# With the GCS/project and Cloud SQL connection environment configured:
python -m ingestion.epa_stage --years 2026 --pollutants carbon_monoxide
python -m ingestion.epa_stage --verify-only
```

Deployment, commands, and full-run evidence are in
[`docs/epa_stage.md`](docs/epa_stage.md).

## S07: AQS site and monitor listings

`ingestion/aqs_listings.py` validates the published site and monitor ZIPs,
uploads immutable SHA-256-named copies to GCS, and upserts their rows into
Cloud SQL after migration 002. It prints the Chicago-area PM2.5 monitor list
and writes [`output/aqs/chicago_pm25_monitors.csv`](output/aqs/chicago_pm25_monitors.csv).
See [`docs/aqs_listings.md`](docs/aqs_listings.md) for source counts,
reproduction, and the Cloud SQL load evidence.

## S09: AirNow hourly collector

`ingestion/airnow_collect.py` refetches the preceding 72 completed UTC hours,
archives immutable public HourlyAQObs files, and upserts Chicago-area
concentrations and AQI into Cloud SQL. All observations are preliminary;
changed or withdrawn values retain their prior versions.

The verified backfill contains **2,359 observations across 26 sites and 72
hours**, from November 6, 2026 at 07:00 through November 9 at 06:00 UTC.
The identical-window rerun inserted **zero rows** and reused all **72 raw
snapshots**. SQL-to-source reconciliation found **zero mismatches**. There
were zero live revisions in these runs; revision behavior passed the isolated
PostgreSQL integration test.

```sh
python -m pip install -e '.[dev,airnow]'
gcloud run jobs execute aq-airnow-collect \
  --project=final-team-514205 --region=us-central1 --wait
# With the standard Cloud SQL/GCS connection environment configured:
python -m ingestion.airnow_verify --end-hour 2026-11-09T06:00:00Z \
  --hours 72 --report output/airnow/verification.json
```

The hourly scheduler runs at minute 40 UTC. See
[`docs/airnow_collect.md`](docs/airnow_collect.md) for configuration, recovery,
pausing, source semantics, and S14/S17 handoff details.

## S11/S12: EPA normalization and cloud scale

`spark/normalize_epa.py` implements explicit schemas, UTC parsing, Chicago DST
display fields, pollutant unit conversion, PM2.5 advisory preference, and
deterministic monitor-hour revision deduplication. Exceptions retain source
lineage and reasons; null and negative concentrations remain visible.

All project execution uses **Python 3.15**, including the custom Dataproc
image. Local Spark also needs Java 17:

```sh
.venv/bin/python -m pip install -e '.[dev,spark,gcp]'
# Set JAVA_HOME to Java 17, then:
make lint test
.venv/bin/python -m spark.cloud_epa plan --year 2026
```

The follow-up verification passed **95 tests**, with four opt-in database
tests skipped. Twenty tests cover S11/S12 and cleanup, including actual local
Spark execution, lead partition typing, Parquet round trips, and cleanup
safety. Without a usable Java 17 runtime, JVM tests skip with a reason
(88 passed, 11 skipped in the simulated missing-Java check). Current processing scope, actual rows/bytes, runtime,
and compute estimates are in [the scale report](docs/scale_report.md).

The **2026 pilot is published and verified**: 6,988,009 input/output rows,
zero rejected or removed rows, and 514,136,327 Parquet bytes across 50 files.
Cloud execution used Python **3.15.2** and Spark **3.5.3**. It preserved 3,579
valid samples beginning at `:26`, with exact sampling timestamps, a separate
UTC-hour key, and an alignment flag. The 1990–2025 backfill is deferred;
full S12 historical acceptance is not claimed.

[The runbook](docs/spark_normalization.md) provides step-by-step setup,
submission, publication, replay, and recovery. S13 must load exact committed
file lists, and `year` partitions use the EPA archive/local-standard year.

## S13: Snowflake warehouse pilot

The Python 3.15 loader and star schema are prepared for the committed 2026
batch: 50 Parquet files and 6,988,009 expected rows. The GCS preflight and S07
reference checks have passed. Snowflake execution still requires the trial
account identifier, username, and browser authentication; warehouse loading
is not yet claimed. [The warehouse runbook](docs/warehouse_model.md) covers
account setup, read-only GCS integration, history tests, loading, and replay.

```sh
.venv/bin/python -m pip install -e '.[dev,gcp,warehouse]'
.venv/bin/python -m warehouse.load prepare --year 2026
# After account bootstrap and the documented GCS grant:
.venv/bin/python -m warehouse.check_history
.venv/bin/python -m warehouse.load load --year 2026
```

The loader uses an explicit COPY `FILES` list, verifies object generations
before and after loading, reconciles each pollutant, and promotes facts and
dimension versions in one transaction. Replaying the same input verifies the
warehouse ledger and returns `unchanged`. Live landing tables are defined for
S14; this pilot does not start live sync or the 1990–2025 backfill.
