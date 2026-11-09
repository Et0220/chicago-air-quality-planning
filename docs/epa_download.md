# S06: EPA hourly ZIP ingestion

**Status: complete and verified on November 9, 2026 UTC.**

The downloader in `ingestion/epa_download.py` lands the S03 manifest's nonempty
ZIPs in versioned GCS and records their exact size, MD5, and generation in the
S05 state tables. Deployment and cloud verification results are recorded below.

## Deployed results

The full 1990–2026 manifest has landed in
`gs://final-team-514205-air-quality/raw/epa/`: **233 ZIPs, 233 GCS generations,
and 6,716,684,039 actual bytes (6.2554 GiB)**. All **63 listed empty entries**
were skipped; the manifest has no missing cells. The catalog estimate exceeds
measured bytes by only 3,321 bytes, but exact transfer sizes are still recorded
separately from that estimate.

| Check | Cloud Run execution | Result |
| --- | --- | --- |
| Sample | `aq-epa-download-mkczs` | 2 downloads, 1 empty lead skip |
| Sample rerun | `aq-epa-download-pldjf` | 2 unchanged, 1 empty skip; identical generations |
| Full backfill | `aq-epa-download-b6wbv` | 231 downloads, 2 unchanged samples, 63 empty skips; no errors |
| Full rerun | `aq-epa-download-m5jwh` | 233 unchanged, 63 empty skips; no added generations |
| Python 3.15 rerun | `aq-epa-download-89w4z` | 233 unchanged, 63 empty skips; no errors |
| Final reconciliation | `aq-epa-download-ddr2t` | 296 entries checked; 233 objects and version records, 63 skips, zero mismatches |

The final read-only reconciliation completed at **07:16:08 UTC** on the
deployed Python 3.15 image. It matched each nonempty entry's URI, generation,
size, and MD5 across SQL current state, SQL version history, and a fresh GCS
listing. A separate listing including noncurrent versions confirmed that
the complete reruns created **no additional object generations**. Per-object
results are saved in
[`output/epa/download_verification.json`](../output/epa/download_verification.json).

The sample generations were independently read back, fully decompressed, and
checked for ZIP CRC integrity. Their 24-column CSVs contained exactly 368,027
CO rows and 335,702 PM2.5 non-FRM/FEM rows, matching the S03 listing. The full
backfill completed on November 9, 2026 UTC in approximately 55 minutes.

The workspace moved to Python 3.15 while the initial Python 3.11 image was
downloading. S06 was rebuilt and redeployed on **Python 3.15**, and the full
no-op check was repeated on that runtime. The current image is:

```text
us-central1-docker.pkg.dev/final-team-514205/air-quality/epa-download@sha256:e45561bc0f73a51f3ee83abbfbf5e0bd300685dbedd860f2fd7bf06a8d39c0cd
```

The current build is `d3dd9ca9-d557-40e9-8250-c4b79b6618a0`. Source hashes,
original and current image digests, execution times, and run IDs are retained
in [the machine-readable evidence](epa_download_evidence.json). All 13 S06
tests, including the disposable SQL schema integration check, passed on
Python 3.15. Repository checks passed with 50 tests and three opt-in tests
skipped; six Terraform tests passed. The final real Terraform plan exited 0
with no changes. Collector schedules remain paused.

## Transfer and recovery behavior

- Objects use `gs://<bucket>/raw/epa/{year}/{file_name}`. Two workers run by
  default, with a configurable limit of one to four. Each worker has its own
  HTTP session and SQL connection. Memory is bounded by a 4 MiB upload buffer
  per worker; no complete ZIP or extracted CSV is held in memory or local disk.
- The manifest is validated before cloud writes. `--years` accepts lists and
  ranges; `--pollutants` accepts configured group names. `listed_empty` and
  `missing` entries are persisted as skipped without requesting a ZIP.
- Each nonempty entry first checks EPA's HTTP headers and the current GCS
  object. Reuse requires matching catalog/source metadata, a strong ETag or
  last-modified plus size, and a verified stream MD5 matching GCS's MD5.
  A missing GCS object is downloaded even if SQL previously said downloaded.
- Downloads stream directly into a resumable GCS upload. Source length, ZIP
  signature/end record, transport CRC32C, and final stream MD5/size are checked.
  An interrupted or invalid stream aborts before publication. S10 must still
  decompress and verify each member's CRC and row counts.
- Network errors, throttling, selected server errors, and integrity failures
  use up to three whole-file attempts with exponential backoff. A 404 is a failure, not
  an empty catalog cell. Independent files continue; any failure makes the
  overall run fail and is recorded in `ingest_run.details`.
- A PostgreSQL session lock serializes the same pollutant/year across job
  executions. GCS generation preconditions protect against other writers.
  Each verified generation and its current state are committed together in
  SQL. A successful upload followed by a SQL failure is reconciled on rerun
  without transferring the ZIP again. A crash before the final checksum
  metadata is saved can require a replacement generation; raw versions remain
  retained rather than deleted.
- Same-generation reruns preserve downstream staging/processing progress.
  A new generation resets those states and row counts. `ingest_file_version`
  preserves earlier generations. `raw_md5` is lowercase hexadecimal; GCS's
  base64 MD5 is decoded before comparison.

The object metadata field `catalog_sha256` is the fingerprint of that file's
manifest entry. `ingest_run.details.manifest_sha256` separately identifies the
complete input CSV; the S03 HTML snapshot hash remains in its source metadata.

## Local and Cloud Run commands

Use Python 3.15 and the existing project environment:

```sh
python -m pip install -e '.[dev,epa]'
python -m ingestion.epa_download --dry-run
python -m ingestion.epa_download --years 2026 \
  --pollutants carbon_monoxide,pm25_non_frm_fem,lead
python -m ingestion.epa_download --verify-only
```

Export `AIR_QUALITY_GCP_PROJECT_ID`, `AIR_QUALITY_GCS_BUCKET`, and the Cloud SQL
connection settings documented in [cloudsql.md](cloudsql.md). Cloud Run uses
`AIR_QUALITY_CLOUDSQL_IAM_USER=aq-epa@final-team-514205.iam`; it has no database
password or service-account key. Local execution needs matching IAM ADC or a
local PostgreSQL DSN. The downloader does not automatically read `.env`.
`AIR_QUALITY_GCS_RAW_EPA_PREFIX` overrides the YAML prefix. The manifest is
bundled into the image; refresh S03 and rebuild to deploy a newer snapshot.

Build with `ingestion/Dockerfile.epa` and the source allowlist:

```sh
gcloud builds submit . --project=final-team-514205 --region=us-central1 \
  --config=ingestion/cloudbuild-epa.yaml --ignore-file=.gcloudignore.epa \
  --substitutions=_IMAGE=us-central1-docker.pkg.dev/final-team-514205/air-quality/epa-download:YOUR_TAG \
  --gcs-source-staging-dir=gs://final-team-514205-aq-spark-scratch/build-source \
  --service-account=projects/final-team-514205/serviceAccounts/aq-build@final-team-514205.iam.gserviceaccount.com
```

Set `epa_download_image` in ignored Terraform variables to the returned image
**digest** (`.../epa-download@sha256:...`), then inspect and apply a Terraform
plan. The image runs as an unprivileged OS user. The builder can read only the
staged source prefix, push to the project image repository, and write logs.
It has no ingestion-data or SQL permissions. `.env`, credentials, Terraform
state, unrelated files, and source measurements are excluded from the build.

```sh
# Sample, followed by the same command again to verify a no-op:
gcloud run jobs execute aq-epa-download --project=final-team-514205 \
  --region=us-central1 \
  '--args=^@^--years@2026@--pollutants@carbon_monoxide,pm25_non_frm_fem,lead' --wait

# Full manifest (one task, two transfer workers by default):
gcloud run jobs execute aq-epa-download --project=final-team-514205 \
  --region=us-central1 --wait

# Read-only state/history versus fresh GCS listing:
gcloud run jobs execute aq-epa-download --project=final-team-514205 \
  --region=us-central1 --args=--verify-only --wait
```

The job has 1 vCPU, 1 GiB RAM, a two-hour task timeout, and one task retry.
It is invoked manually; S06 does not enable the paused collector schedules.
Job retries are safe because every file is checked independently. After a
forced termination, the old run can remain `running`; a fresh run reconciles
its completed objects and releases no stale locks (SQL closes those sessions).

## Verification and handoff

Tests in `tests/test_epa_download.py` cover selection, empty/missing skips,
checksum verification, truncation and interruption, retry backoff, unchanged
reruns, database-failure recovery, and detection of SQL/GCS disagreement.
Set `AIR_QUALITY_EPA_SQL_TEST=1` only with a configured test connection to run
the integration case in a disposable schema. It checks generation history,
cross-connection locks, idempotency, and downstream state resets. It removes
the schema afterward. The integration case passed against the project's
PostgreSQL 16 instance using a temporary administrator that was then deleted.

S10 should read only `catalog_status='listed_nonempty' AND
download_state='downloaded'`, and open `raw_uri` at the recorded
`raw_generation`. It must retain version lineage and independently validate
ZIP contents while staging. The catalog's approximate bytes and listed rows
are separate from measured transfer size and subsequent validated row counts.
When updating staging state, S10 must condition its update on the same
`raw_generation` it read, so completion of an old version cannot mark a newly
downloaded version as staged.

GCS resumable writes and generation conditions follow the
[BlobWriter reference](https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.fileio.BlobWriter).
The build identity follows Google's
[user-specified Cloud Build account guidance](https://docs.cloud.google.com/build/docs/securing-builds/configure-user-specified-service-accounts).
