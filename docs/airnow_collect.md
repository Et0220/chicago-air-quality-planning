# S09 — AirNow hourly collector

`ingestion/airnow_collect.py` loads the public HourlyAQObs archive into
`raw/airnow/` and the existing `airnow_obs` / `airnow_obs_revision` tables.
The Cloud Run job is `aq-airnow-collect` in `us-central1`. Its dedicated
`aq-airnow` service account uses Cloud SQL IAM authentication; no AirNow API
key or database password is required.

## Source and scope

The [AirNow Hourly AQ Obs fact sheet](https://docs.airnowapi.org/docs/HourlyAQObsFactSheet.pdf)
defines the CSV fields and UTC begin-hour timestamps. AirNow republishes the
preceding 72 hours approximately 35 minutes after each hour. These data are
preliminary. PM2.5, ozone, and PM10 AQI are NowCast indices; NO2 AQI is hourly.
An AQI is never calculated from the raw concentration by this collector.

Files come from
`https://files.airnowtech.org/airnow/YYYY/YYYYMMDD/HourlyAQObs_YYYYMMDDHH.dat`.
The area is the same WGS84 bounding box used for PurpleAir discovery:
41.3–42.5° N, 88.5–87.0° W, restricted to `CountryCode=US`. This geographic
selection includes northwest Indiana. It is independent of S07's county
selection and is not an AirNow-to-AQS monitor crosswalk.

The parser preserves nine- and twelve-digit source IDs, negative concentrations,
reported units, status, responsible agency, and AQI-only records. Blank values
remain null. A station/pollutant with neither concentration nor AQI does not
create a new observation. Both the historical two-digit year and current
four-digit year formats are supported. Local time and `GMTOffset` are not
used to interpret the UTC hour.

## Reruns and revisions

Each invocation refetches all 72 completed UTC hours by default. Source bytes
are stored unchanged at:

```text
raw/airnow/dt=YYYY-MM-DD/hh=HH/HourlyAQObs_YYYYMMDDHH_<sha256>.dat
```

An atomic create with `if_generation_match=0` protects each content-addressed
object. Identical bytes reuse the existing object and generation; changed
bytes create a new immutable snapshot. GCS size, MD5, and SHA-256 metadata
are checked before the SQL update. Each current row records the exact GCS
URI/generation and the time that the HTTP retrieval completed. The original
source URL and HTTP Last-Modified/ETag are in object metadata; the run report
also records retrieval time and Last-Modified per hour.

The primary key is `(airnow_site_id, pollutant, observed_at_utc)`. Each hour is
upserted in one transaction. The existing S05 trigger archives the previous
concentration, unit, AQI, retrieval time, and raw provenance whenever the
concentration, unit, or AQI changes. A later snapshot that omits a previously observed key
within the configured area clears its current concentration/unit/AQI to null
and retains the old value in revisions. It does not delete the key. Older or
equal retrieval timestamps cannot replace newer state. Metadata-only refreshes
do not create revisions. All current rows have `is_preliminary=true`.

A PostgreSQL session advisory lock serializes collectors, including overlapping
Cloud Run executions. SQL commits release neither that lock nor the connection.
The connection closing releases it after a crash. A failed hour does not roll
back already completed hours; rerunning safely resumes the full window.
Invalid/truncated files and missing source hours fail explicitly. No synthetic
observations fill gaps. HTTP 429 and transient server/network failures receive
bounded retries. A job with any failed hour exits nonzero and records a failed
`ingest_run` with per-hour errors; Cloud Run permits one retry. A killed process
can leave a `running` ingest record for S23 to reconcile.

## Run and operate

```sh
python -m pip install -e '.[dev,airnow]'
export AIR_QUALITY_GCP_PROJECT_ID=final-team-514205
export AIR_QUALITY_GCS_BUCKET=final-team-514205-air-quality
export AIR_QUALITY_CLOUDSQL_CONNECTION_NAME=final-team-514205:us-central1:aq-operational
export AIR_QUALITY_CLOUDSQL_DB=air_quality
# In Cloud Run, Terraform supplies this worker identity automatically:
export AIR_QUALITY_CLOUDSQL_IAM_USER=aq-airnow@final-team-514205.iam
python -m ingestion.airnow_collect --hours 72 --report /tmp/airnow-run.json
```

Local IAM execution requires credentials for the named database identity;
setting its username alone does not impersonate it. Use the deployed job for
normal operations:

```sh
gcloud run jobs execute aq-airnow-collect \
  --project=final-team-514205 --region=us-central1 --wait

# Reproduce an exact window; end-hour is inclusive and must be completed UTC.
gcloud run jobs execute aq-airnow-collect \
  --project=final-team-514205 --region=us-central1 --wait \
  --args='^@^--hours@72@--end-hour@2026-11-09T06:00:00Z'
```

Configuration lives in `config/settings.yaml`. Scalar overrides use
`AIR_QUALITY_AIRNOW_REVISION_LOOKBACK_HOURS`, `AIR_QUALITY_AIRNOW_SOUTH`,
`..._NORTH`, `..._WEST`, `..._EAST`, and `AIR_QUALITY_GCS_RAW_AIRNOW_PREFIX`.
Keep the configured area consistent across scheduled runs. `--hours` and
`--end-hour` are explicit CLI overrides; the maximum window is 72 hours.
For older recovery windows, invoke successive explicit windows.

Build with `ingestion/cloudbuild-airnow.yaml` and `.gcloudignore.airnow`;
`ingestion/Dockerfile.airnow.dockerignore` restricts the container context.
The image runs Python 3.15 as an unprivileged user. Terraform requires an
immutable digest in `airnow_collect_image`. Set `airnow_schedule_enabled=true`
after backfill verification. The scheduler invokes the job at `40 * * * *`
in UTC, after the documented source update time. To pause collection, set
that variable to false and apply the AirNow scheduler change. For an immediate
operational pause:

```sh
gcloud scheduler jobs pause aq-airnow-collect \
  --project=final-team-514205 --location=us-central1
```

This creates Terraform drift until its variable is updated. The schedule
remains active until paused; the project's trial expiry is January 6, 2027.

## Verification and downstream handoff

`tests/test_airnow_collect.py` covers source parsing, missing values, UTC/DST,
malformed responses, immutable snapshot reuse, retries, and real PostgreSQL
idempotency, revisions, withdrawal, stale-update rejection, and transaction
rollback. The SQL test opts in with `AIR_QUALITY_TEST_CLOUDSQL=1`, uses the
normal connection configuration, creates a random temporary schema, and drops
only that schema. It requires schema-creation rights and writes no test
observations to the production schema.

```sh
make lint test
AIR_QUALITY_TEST_CLOUDSQL=1 python -m pytest -q tests/test_airnow_collect.py
terraform -chdir=infra test
# Read-only reconciliation with the standard Cloud SQL/GCS environment:
python -m ingestion.airnow_verify --end-hour 2026-11-09T06:00:00Z \
  --hours 72 --report output/airnow/verification.json
```

S14 can incrementally read `airnow_obs` using
`(retrieved_at_utc, airnow_site_id, pollutant, observed_at_utc)` as the ordered
watermark, including null withdrawals. S17 should join on overlapping UTC
hours and distinguish raw concentration from NowCast AQI. Read the recorded
GCS generation when reproducing any row. Revisions contain prior values;
`superseded_at_utc` is the retrieval time of the replacement snapshot.

## Live acceptance — November 9, 2026

The backfill and identical-window rerun both succeeded in the deployed
Python 3.15 image. The window is **November 6 at 07:00 through November 9 at
06:00 UTC**, inclusive: 72 consecutive begin-hours.

| Check | Verified result |
| --- | --- |
| Backfill execution | `aq-airnow-collect-b6xbk` |
| Rerun execution | `aq-airnow-collect-slprp` |
| Current observations / sites | 2,359 / 26 |
| Raw files / bytes | 72 / 71,757,311 |
| Rerun inserted / unchanged rows | 0 / 2,359 |
| Rerun new raw snapshots | 0; all URI/generation pairs identical |
| Missing hours / duplicate keys / non-preliminary rows | 0 / 0 / 0 |
| SQL-to-raw mismatches | 0; every referenced generation downloaded and parsed |
| Live revisions | 0 |
| Synthetic revision test | Passed in a disposable SQL schema: value update, old-value archive, withdrawal, stale rejection, rollback |

| Pollutant | Rows | Non-null concentration | Non-null AQI |
| --- | ---: | ---: | ---: |
| PM2.5 | 1,511 | 1,502 | 1,510 |
| Ozone | 316 | 294 | 316 |
| PM10 | 252 | 244 | 252 |
| NO2 | 70 | 70 | 70 |
| SO2 | 210 | 210 | 0 |
| CO | 0 | 0 | 0 |

CO parsing is supported, but the selected source files contained no CO
values in this area. Missing concentrations and AQI were not manufactured.
The synthetic SQL fixture revised PM2.5 from -0.7 / AQI 3 to 12.5 / AQI 50,
verified the archived prior value and generation, then tested a withdrawal.
These synthetic rows never entered the production tables. Temporary test
schemas and database users were removed after verification.

The deployed image is
`us-central1-docker.pkg.dev/final-team-514205/air-quality/airnow-collect@sha256:f84e200ca2663eb3710406e841851ecf00ef32673552601d8cc9ed48d555d785`.
All seven files in its uploaded build context matched the workspace hashes
at verification. The read-only verifier is a separate local utility and is
not required in the collector image.

Machine-readable results: [`airnow_collect_evidence.json`](airnow_collect_evidence.json)
and [`../output/airnow/verification.json`](../output/airnow/verification.json).
These counts describe the acceptance snapshot; subsequent scheduled runs can
add observations and revisions.
